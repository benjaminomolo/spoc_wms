# app/services/post_to_ledger_reversal.py

import logging
from datetime import datetime, timezone

from flask import jsonify
from sqlalchemy import and_, or_
from sqlalchemy.orm import joinedload

from ai import resolve_exchange_rate_for_transaction
from models import Journal, PayrollPayment, PayrollTransaction, \
    ChartOfAccounts, DeductionPayment, Deduction, \
    InventoryTransactionDetail, JournalEntry, \
    CurrencyExchangeTransaction, GoodsReceipt  # adjust import path to your actual models location
from services.inventory_helpers import update_inventory_summary
from services.post_to_ledger import create_inventory_ledger_entry
from utils import generate_unique_journal_number

logger = logging.getLogger(__name__)


def repost_transaction(
        db_session,
        ledger_filters,
        post_function=None,  # Make optional
        *args,
        inventory_reversal_function=None,
        inventory_source_type=None,
        inventory_source_id=None,
        repost=True,
        **kwargs
):
    """
    Repost a transaction:
      1. Delete old ledger entries for multiple (source_type, source_id) pairs
      2. Optionally reverse inventory with its own source_type/source_id
      3. Call the post function to create new ledger entries

    :param ledger_filters: List of dicts with exact match filters for ledger deletion
                           Example: [{"source_type": "purchase_header", "source_id": 123},
                                     {"source_type": "purchase_detail", "source_id": 123}]
    :param post_function: Function that will post ledger entries
    :param inventory_reversal_function: Optional function to reverse inventory
    :param inventory_source_type: Source type for inventory reversal
    :param inventory_source_id: Source ID for inventory reversal
    """

    logger.info(f'ledger filters are {ledger_filters}')

    # 1️⃣ Delete old ledger entries (JournalEntries with source_type/source_id)
    for f in ledger_filters:
        journal_entries = db_session.query(JournalEntry).filter_by(
            source_type=f["source_type"],
            source_id=f["source_id"]
        ).all()

        if journal_entries:
            logger.info(f"Deleting {len(journal_entries)} ledger entries for "
                        f"{f['source_type']} #{f['source_id']}")

            # Get unique journal IDs to check if they need to be deleted
            journal_ids = {entry.journal_id for entry in journal_entries}

            # Delete the journal entries
            for entry in journal_entries:
                db_session.delete(entry)

            # Check if the parent journals are now empty and delete them if so
            for journal_id in journal_ids:
                journal = db_session.query(Journal).get(journal_id)
                if journal and not journal.entries:  # If journal has no entries left
                    logger.info(f"Deleting empty journal #{journal.id} ({journal.journal_number})")
                    db_session.delete(journal)

    # 2️⃣ Reverse inventory if applicable
    if inventory_reversal_function and inventory_source_type and inventory_source_id:
        try:
            inventory_reversal_function(
                db_session,
                inventory_source_type,
                inventory_source_id
            )
        except Exception as e:
            logger.warning(f"Inventory reversal failed for "
                           f"{inventory_source_type} #{inventory_source_id}: {e}")

    db_session.flush()

    # 3️⃣ Conditionally repost new ledger entries
    if repost:
        if post_function is None:
            raise ValueError("post_function must be provided when repost=True")
        logger.info("Reposting document...")
        post_function(db_session, *args, **kwargs)

    db_session.commit()


def update_transaction(
        db_session,
        ledger_filters,
        update_function=None,
        repost=True,
        *args,
        **kwargs
):
    """
    Generic function to update existing ledger entries for any payment type.

    :param db_session: Database session
    :param ledger_filters: List of dicts with exact match filters for ledger entries to update
    :param update_function: Function to update ledger entries for this payment type
    :param repost: Whether to perform the ledger update
    :param args: Additional positional args to pass to update_function
    :param kwargs: Additional keyword args to pass to update_function
    """

    # 1️⃣ Find existing journal entries to update
    entries_to_update = []
    for f in ledger_filters:
        journal_entries = db_session.query(JournalEntry).filter_by(
            source_type=f["source_type"],
            source_id=f["source_id"]
        ).all()

        if journal_entries:
            entries_to_update.extend(journal_entries)

    db_session.flush()

    # 2️⃣ Conditionally update ledger entries
    if repost:
        if update_function is None:
            raise ValueError("update_function must be provided when repost=True")

        update_function(db_session, entries_to_update, *args, **kwargs)

    db_session.commit()


def update_payroll_payment_to_ledger(db_session, transactions_to_update, payment_id=None,
                                     current_user=None, data=None, exchange_rate_id=None):
    """
    Update existing payroll payment journal entries with exchange rate support
    """
    app_id = current_user.app_id

    # Load payment with related data
    payment = db_session.query(PayrollPayment).options(
        joinedload(PayrollPayment.payroll_transactions)
        .joinedload(PayrollTransaction.employees)
    ).filter_by(id=payment_id).first()

    if not payment or not payment.payroll_transactions:
        raise ValueError("Payroll payment not found")

    payroll_transaction = payment.payroll_transactions
    employee = payroll_transaction.employees
    employee_name = f"{employee.first_name} {employee.last_name}"

    # ✅ ADDED: Resolve exchange rate for foreign currency
    exchange_rate_id = exchange_rate_id

    # Get required accounts
    payable_account = None
    if data and data.get('creditSubCategory'):
        payable_account = db_session.query(ChartOfAccounts).filter_by(
            id=data['creditSubCategory'],
            app_id=app_id
        ).first()
        if not payable_account:
            raise ValueError("Specified payable account not found")

    if not payable_account:
        # Get payable account from employee
        payable_account_id = employee.payable_account_id
        payable_account = db_session.query(ChartOfAccounts).filter_by(
            id=payable_account_id,
            app_id=app_id
        ).first()
        if not payable_account:
            raise ValueError("No payable account configured for this employee")

    funding_account_id = payment.payment_account
    funding_account = db_session.query(ChartOfAccounts).filter_by(
        id=funding_account_id,
        app_id=app_id
    ).first()
    if not funding_account:
        raise ValueError("No funding account configured for this payment")

    # Update journal entries
    for journal_entry in transactions_to_update:
        if journal_entry.dr_cr == "C":  # Credit entry (cash/bank going out)
            journal_entry.date = payment.payment_date
            journal_entry.subcategory_id = funding_account_id
            journal_entry.amount = float(payment.amount)
            journal_entry.description = f"Salary payment to {employee_name}"
            journal_entry.updated_at = datetime.now(timezone.utc)

        elif journal_entry.dr_cr == "D":  # Debit entry (reducing payable liability)
            journal_entry.date = payment.payment_date
            journal_entry.subcategory_id = payable_account.id
            journal_entry.amount = float(payment.amount)
            journal_entry.description = f"Salary payment - {employee_name}"
            journal_entry.updated_at = datetime.now(timezone.utc)

    # ✅ Update the journal header totals and status
    # Group journal entries by journal_id and update each journal
    journal_ids = {entry.journal_id for entry in transactions_to_update}

    for journal_id in journal_ids:
        journal = db_session.query(Journal).filter_by(id=journal_id).first()
        if journal:
            # Update journal details including exchange rate
            journal.date = payment.payment_date
            journal.narration = f"Payroll Payment: {employee_name} - {payroll_transaction.payroll_period.payroll_period_name}"
            journal.vendor_id = employee.payee_id
            journal.payment_mode_id = payment.payment_method
            journal.currency_id = payment.currency_id  # ✅ Ensure currency is set
            journal.exchange_rate_id = exchange_rate_id  # ✅ ADDED: Update exchange rate
            journal.updated_by = current_user.id
            journal.updated_at = datetime.now(timezone.utc)
            journal.journal_ref_no = payment.reference if payment.reference else payroll_transaction.payroll_period.payroll_period_name

            # ✅ CRITICAL: Update journal totals
            journal.update_totals()

            # Keep status as "Unposted" since we're just updating, not posting
            journal.status = "Unposted"

    # Reset posting status since we've modified the entries
    payment.is_posted_to_ledger = False

    db_session.commit()
    logger.info(
        f"✅ Updated payroll payment journal entries for payment {payment_id} with exchange rate {exchange_rate_id}")


def update_deduction_payment_to_ledger(db_session, transactions_to_update, deduction_payment_id=None,
                                       current_user=None, data=None, exchange_rate_id=None):
    """
    Update existing deduction payment journal entries with exchange rate support
    """
    app_id = current_user.app_id

    # Load deduction payment with related data
    deduction_payment = db_session.query(DeductionPayment).options(
        joinedload(DeductionPayment.deductions)
        .joinedload(Deduction.payroll_transactions)
        .joinedload(PayrollTransaction.employees)
    ).filter_by(id=deduction_payment_id).first()

    if not deduction_payment or not deduction_payment.deductions:
        raise ValueError("Deduction payment not found")

    deduction = deduction_payment.deductions
    payroll_transaction = deduction.payroll_transactions
    employee = payroll_transaction.employees
    employee_name = f"{employee.first_name} {employee.last_name}"

    # Resolve exchange rate for foreign currency
    exchange_rate_id = exchange_rate_id

    # Get required accounts
    deduction_payable_account = db_session.query(ChartOfAccounts).filter_by(
        id=employee.deduction_payable_account_id,
        app_id=app_id
    ).first()
    if not deduction_payable_account:
        raise ValueError("No deduction payable account configured for this employee")

    funding_account_id = deduction_payment.payment_account
    funding_account = db_session.query(ChartOfAccounts).filter_by(
        id=funding_account_id,
        app_id=app_id
    ).first()
    if not funding_account:
        raise ValueError("No funding account configured for this deduction payment")

    # Update journal entries
    for journal_entry in transactions_to_update:
        if journal_entry.dr_cr == "C":  # Credit entry (cash/bank going out to third party)
            journal_entry.date = deduction_payment.payment_date
            journal_entry.subcategory_id = funding_account_id
            journal_entry.amount = float(deduction_payment.amount)
            journal_entry.description = f"Deduction payment for {employee_name} - {deduction.deduction_type}"
            journal_entry.updated_at = datetime.now(timezone.utc)

        elif journal_entry.dr_cr == "D":  # Debit entry (reducing deduction payable liability)
            journal_entry.date = deduction_payment.payment_date
            journal_entry.subcategory_id = deduction_payable_account.id
            journal_entry.amount = float(deduction_payment.amount)
            journal_entry.description = f"Deduction payment - {employee_name} ({deduction.deduction_type})"
            journal_entry.updated_at = datetime.now(timezone.utc)

    # Update the journal header totals and status
    journal_ids = {entry.journal_id for entry in transactions_to_update}

    for journal_id in journal_ids:
        journal = db_session.query(Journal).filter_by(id=journal_id).first()
        if journal:
            # Update journal details including exchange rate
            journal.date = deduction_payment.payment_date
            journal.narration = f"Deduction Payment: {employee_name} - {deduction.deduction_type.name} - {deduction_payment.amount}"
            journal.vendor_id = deduction.payroll_transactions.employees.payee_id
            journal.payment_mode_id = deduction_payment.payment_method
            journal.currency_id = deduction_payment.currency_id
            journal.exchange_rate_id = exchange_rate_id
            journal.updated_by = current_user.id
            journal.updated_at = datetime.now(timezone.utc)

            # Update journal totals
            journal.update_totals()

            # Keep status as "Unposted"
            journal.status = "Unposted"

    # Reset posting status since we've modified the entries
    deduction_payment.is_posted_to_ledger = False

    db_session.commit()


def reverse_inventory_posting(db_session, transaction_detail_ids, current_user):
    """
    Reverse previously posted inventory transactions to allow corrections and reposting.

    Key Functionality:
    ------------------
    - Finds and reverses ledger entries for specified inventory transactions
    - Updates posting status flags to allow reprocessing
    - Handles both individual transactions and parent inventory entries
    - Maintains audit trail of reversal operations

    Workflow:
    ---------
    1. Validates transactions exist and are posted
    2. Creates reversal ledger entries with opposite DR/CR
    3. Resets posting flags for selected transactions
    4. Updates parent entry status if all child transactions reversed

    Typical Use Cases:
    ------------------
    - Correcting data entry errors in inventory transactions
    - Re-processing after account configuration changes
    - Handling system errors during initial posting
    - Period-end adjustments and corrections

    Returns comprehensive status with success/failure details for each transaction.

    Args:
        db_session:
        transaction_detail_ids:
        current_user:

    Returns:

    """
    app_id = current_user.app_id

    try:
        # Get the transaction details
        transaction_details = db_session.query(InventoryTransactionDetail).filter(
            InventoryTransactionDetail.id.in_(transaction_detail_ids),
            InventoryTransactionDetail.app_id == app_id,
            InventoryTransactionDetail.is_posted_to_ledger == True
        ).all()

        if not transaction_details:
            return {'success': True, 'message': 'No posted transaction details found'}

        reversed_details = []
        failed_details = []

        for detail in transaction_details:
            try:
                # Find and reverse the ledger transactions
                success = _reverse_ledger_transactions(db_session, detail, current_user)

                if success:
                    # Mark as not posted
                    detail.is_posted_to_ledger = False

                    # Also update parent inventory entry if all its transactions are reversed
                    inventory_entry = detail.inventory_entry_line_item.inventory_entry
                    if all(not td.is_posted_to_ledger for td in
                           inventory_entry.line_items[0].inventory_transaction_details):
                        inventory_entry.is_posted_to_ledger = False

                    reversed_details.append(detail.id)
                else:
                    failed_details.append({'id': detail.id, 'error': 'Failed to reverse ledger transactions'})

            except Exception as e:
                logger.error(f"Error reversing transaction detail {detail.id}: {str(e)}")
                failed_details.append({'id': detail.id, 'error': str(e)})

        db_session.commit()

        return {
            'success': True,
            'message': f'Reversed {len(reversed_details)} transaction details successfully',
            'reversed_details': reversed_details,
            'failed_details': failed_details
        }

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in reverse posting: {str(e)}")
        return {'success': False, 'message': f'Error reversing transactions: {str(e)}'}


def _reverse_ledger_transactions(db_session, transaction_detail, current_user):
    """
    Delete ledger transactions linked to inventory transactions instead of creating reversal entries.

    Core Functionality:
    -------------------
    - Finds all general ledger entries created from a specific inventory transaction
    - Permanently deletes the ledger entries to clean up accounting records
    - Provides complete removal of accounting impact for the inventory transaction

    Technical Process:
    ------------------
    1. Queries ledger transactions by source inventory transaction ID
    2. Deletes all found ledger entries from the database
    3. Returns success status based on deletion outcome

    Audit Considerations:
    ---------------------
    - Permanent deletion of accounting entries (use with caution)
    - Appropriate for complete transaction removal during corrections
    - Maintains inventory transaction record while removing accounting impact
    - Should be used only when complete reversal/removal is intended

    Args:
        db_session: Database session
        transaction_detail: The inventory transaction detail to reverse
        current_user: User initiating the reversal

    Returns:
        bool: True if deletion successful, False if error occurred
    """
    try:
        app_id = current_user.app_id

        # Find all journal entries linked to this inventory transaction
        journal_entries = db_session.query(JournalEntry).filter(
            JournalEntry.source_type == "inventory_transaction",
            JournalEntry.source_id == transaction_detail.id,
            JournalEntry.app_id == app_id
        ).all()

        if not journal_entries:
            logger.warning(f"No ledger entries found for inventory transaction {transaction_detail.id}")
            return True

        # Get unique journal IDs to check if they need to be deleted
        journal_ids = {entry.journal_id for entry in journal_entries}

        # Delete all journal entries instead of creating reversal entries
        for journal_entry in journal_entries:
            db_session.delete(journal_entry)

        # Check if the parent journals are now empty and delete them if so
        for journal_id in journal_ids:
            journal = db_session.query(Journal).get(journal_id)
            if journal and not journal.entries:  # If journal has no entries left
                logger.info(f"Deleting empty journal #{journal.id} ({journal.journal_number})")
                db_session.delete(journal)

        logger.info(
            f"Deleted {len(journal_entries)} ledger entries for inventory transaction {transaction_detail.id}")
        return True

    except Exception as e:
        logger.error(f"Error deleting ledger entries for {transaction_detail.id}: {str(e)}")
        return False


def repost_inventory_transactions(db_session, transaction_detail_ids, base_currency_id, current_user, posted_status):
    """    Complete reprocessing workflow for previously reversed inventory transactions.

    Two-Step Process:
    -----------------
    1. REVERSE: First clears any existing ledger entries using reverse_inventory_posting()
    2. REPOST: Re-creates accounting entries using post_inventory_to_ledger()

    Primary Use Cases:
    ------------------
    - Correcting errors in original transaction data
    - Updating account mappings or currency rates
    - Re-processing after system configuration changes
    - Handling partial or failed initial postings

    Key Benefits:
    -------------
    - Atomic operation: Either completely succeeds or fully rolls back
    - Comprehensive status reporting for both reversal and reposting phases
    - Maintains data integrity through proper transaction management
    - Preserves audit trail with clear reversal and repost tracking

    Returns combined results showing reversal and reposting outcomes."""
    try:
        if not posted_status:
            posted_status = "Posted"
        # First reverse any existing postings
        reverse_result = reverse_inventory_posting(db_session, transaction_detail_ids, current_user)

        if not reverse_result['success']:
            return reverse_result

        # Now repost the transactions
        app_id = current_user.app_id

        # Get the transaction details
        transaction_details = db_session.query(InventoryTransactionDetail).filter(
            InventoryTransactionDetail.id.in_(transaction_detail_ids),
            InventoryTransactionDetail.app_id == app_id
        ).all()

        successful_reposts = []
        failed_reposts = []

        for detail in transaction_details:
            try:
                # Repost using your existing function
                success = create_inventory_ledger_entry(db_session, base_currency_id, detail, current_user, posted_status)

                if success:
                    successful_reposts.append(detail.id)
                else:
                    failed_reposts.append({'id': detail.id, 'error': 'Failed to repost'})

            except Exception as e:
                logger.error(f"Error reposting transaction detail {detail.id}: {str(e)}")
                failed_reposts.append({'id': detail.id, 'error': str(e)})

        db_session.commit()

        return {
            'success': True,
            'message': f'Reposted {len(successful_reposts)} transaction details successfully',
            'reposted_details': successful_reposts,
            'failed_details': failed_reposts,
            'reversal_details': reverse_result['reversed_details']
        }

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in reposting: {str(e)}")
        return {'success': False, 'message': f'Error reposting transactions: {str(e)}'}


def reverse_sales_invoice_posting(db_session, invoice, user=None):
    """
    ===========================================================================
    REVERSE SALES INVOICE LEDGER POSTINGS (DELETE METHOD)
    ===========================================================================

    WHAT THIS FUNCTION DOES:
    ------------------------
    Deletes ALL journal entries related to a sales invoice and removes empty
    parent journals. This effectively "unposts" the invoice from the ledger.

    WHEN TO USE:
    ------------
    - Invoice was created in error and needs to be completely removed
    - User wants to delete an invoice and start over
    - Correcting a mistake where invoice was posted with wrong amounts

    WHAT GETS DELETED:
    ------------------
    1. INVOICE-LEVEL ENTRIES (source_type, source_id = invoice.id):
       - 'sales_invoice_receivable' - Dr Accounts Receivable
       - 'sales_invoice_revenue'     - Cr Revenue/Income
       - 'sales_invoice_tax'         - Cr Tax Payable

    2. LINE-ITEM-LEVEL ENTRIES (source_type, source_id = item.id):
       - 'sales_invoice_cogs' - Dr COGS, Cr Inventory (only for inventory items)

    3. EMPTY JOURNALS:
       - Any journal that has no remaining entries after deletion

    4. INVOICE STATUS:
       - Sets invoice.is_posted_to_ledger = False

    WHAT DOES NOT GET DELETED:
    --------------------------
    - The invoice record itself
    - Customer information
    - Line items (they remain for reference)
    - Payment transactions (these are separate!)

    IMPORTANT NOTES:
    ----------------
    1. DELETES, NOT REVERSES: This function permanently removes entries.
       No audit trail is created. Use only when users need to completely
       remove incorrect invoices.

    2. PAYMENTS MUST BE HANDLED SEPARATELY: If this invoice has been paid,
       you must reverse/cancel payments FIRST using cancel_sales_transaction()
       before reversing the invoice.

    3. COGS HANDLING: Only reverses COGS for inventory items. Service items
       have no COGS entries.

    4. JOURNAL CLEANUP: Automatically deletes parent journals when they
       become empty to prevent orphaned records.

    PARAMETERS:
    -----------
    db_session : SQLAlchemy session
    invoice    : SalesInvoice object to reverse
    user       : Current user (for app_id, optional)

    RETURNS:
    --------
    bool : True if reversal succeeded, False on error

    EXAMPLE USAGE:
    --------------
    # In your delete invoice route:
    success = reverse_sales_invoice_posting(db_session, invoice, current_user)
    if success:
        db_session.delete(invoice)  # Now safe to delete invoice
        db_session.commit()

    ===========================================================================
    """
    try:
        app_id = getattr(user, "app_id", getattr(invoice, "app_id", None))
        if not app_id:
            raise ValueError("Cannot determine app_id for reversal.")

        # --- Determine eligible source IDs and types in memory ---
        invoice_level_types = ["sales_invoice_receivable", "sales_invoice_revenue", "sales_invoice_tax"]

        line_item_ids = [
            item.id
            for item in getattr(invoice, "invoice_items", [])
            if getattr(item, "item_type", None) == "inventory"
        ]

        # Build filter criteria for a single query
        filter_criteria = []
        if invoice_level_types:
            filter_criteria.append(
                and_(
                    JournalEntry.source_type.in_(invoice_level_types),
                    JournalEntry.source_id == invoice.id
                )
            )
        if line_item_ids:
            filter_criteria.append(
                and_(
                    JournalEntry.source_type == "sales_invoice_cogs",
                    JournalEntry.source_id.in_(line_item_ids)
                )
            )

        # Nothing to reverse
        if not filter_criteria:
            logger.info(f"No ledger entries to reverse for invoice {invoice.id}")
            return True

        # --- Single query to fetch all journal entries ---
        journal_entries = db_session.query(JournalEntry).filter(
            JournalEntry.app_id == app_id,
            or_(*filter_criteria)
        ).all()

        if not journal_entries:
            logger.warning(f"No ledger entries found for invoice {invoice.id}")
            return True

        # Get unique journal IDs to check if they need to be deleted
        journal_ids = {entry.journal_id for entry in journal_entries}

        # --- Delete all journal entries in a loop ---
        for entry in journal_entries:
            db_session.delete(entry)

        # --- Check if parent journals are now empty and delete them ---
        for journal_id in journal_ids:
            journal = db_session.query(Journal).get(journal_id)
            if journal and not journal.entries:  # If journal has no entries left
                logger.info(f"Deleting empty journal #{journal.id} ({journal.journal_number})")
                db_session.delete(journal)

        # --- Mark invoice as unposted ---
        if hasattr(invoice, "is_posted_to_ledger"):
            invoice.is_posted_to_ledger = False

        db_session.flush()
        logger.info(f"Reversed {len(journal_entries)} ledger entries for invoice {invoice.id}")
        return True

    except Exception as e:
        logger.error(f"Error reversing invoice {getattr(invoice, 'id', 'unknown')}: {str(e)}", exc_info=True)
        db_session.rollback()
        return False


def reverse_direct_sales_posting(db_session, direct_sale, user=None):
    """
    Reverse ledger postings for a direct sales in a single optimized query.

    Reversal rules:
    - Direct Sale-level: direct_sale_payment, direct_sale_item
    - Line-item-level: COGS (only for inventory items)

    Args:
        db_session: SQLAlchemy session
        direct_sale: DirectSale object
        user: current_user object (optional, for app_id)

    Returns:
        bool: True if reversal succeeded, False on error
    """
    try:
        app_id = getattr(user, "app_id", getattr(direct_sale, "app_id", None))
        if not app_id:
            raise ValueError("Cannot determine app_id for reversal.")
        # --- Determine eligible source IDs and types in memory ---
        direct_sale_level_types = ["direct_sale_payment", "direct_sale_item", "direct_sale_cogs"]

        # Get all payment allocation IDs
        # Get all payment allocation IDs (before we delete them)
        payment_allocations = list(getattr(direct_sale, "payment_allocations", []))
        payment_allocation_ids = [pa.id for pa in payment_allocations]

        # Get line item IDs for inventory items
        line_item_ids = [
            item.id
            for item in getattr(direct_sale, "direct_sale_items", [])
            if getattr(item, "item_type", None) == "inventory"
        ]

        # Build filter criteria for a single query
        filter_criteria = []

        # Add payment allocation transactions
        if payment_allocation_ids:
            filter_criteria.append(
                and_(
                    JournalEntry.source_type == "direct_sale_payment",
                    JournalEntry.source_id.in_(payment_allocation_ids)
                )
            )

        # Add line item transactions (including COGS)
        if line_item_ids:
            filter_criteria.append(
                and_(
                    JournalEntry.source_type.in_(["direct_sale_item", "direct_sale_cogs"]),
                    JournalEntry.source_id.in_(line_item_ids)
                )
            )

        if direct_sale.sale_reference.startswith("pos_"):
            filter_criteria.append(
                and_(
                    JournalEntry.source_type == "pos_sale",
                    JournalEntry.source_id == direct_sale.id
                )
            )

        # Nothing to reverse
        if not filter_criteria:
            logger.info(f"No ledger entries to reverse for direct sale {direct_sale.id}")
            return True

        # --- Single query to fetch all journal entries ---
        journal_entries = db_session.query(JournalEntry).filter(
            JournalEntry.app_id == app_id,
            or_(*filter_criteria)
        ).all()

        if not journal_entries:
            logger.warning(f"No ledger entries found for direct sale {direct_sale.id}")
            return True

        # Get unique journal IDs to check if they need to be deleted
        journal_ids = {entry.journal_id for entry in journal_entries}

        # --- Delete all journal entries in a loop ---
        for entry in journal_entries:
            db_session.delete(entry)

        # --- Check if parent journals are now empty and delete them ---
        for journal_id in journal_ids:
            journal = db_session.query(Journal).get(journal_id)
            if journal and not journal.entries:  # If journal has no entries left
                logger.info(f"Deleting empty journal #{journal.id} ({journal.journal_number})")
                db_session.delete(journal)

        # --- Mark direct sale as unposted ---
        if hasattr(direct_sale, "is_posted_to_ledger"):
            direct_sale.is_posted_to_ledger = False

        # ✅ UPDATED: Delete payment allocations instead of marking as unposted
        for allocation in payment_allocations:
            db_session.delete(allocation)

        db_session.flush()
        logger.info(f"Reversed {len(journal_entries)} ledger entries for direct sale {direct_sale.id}")
        return True

    except Exception as e:
        logger.error(f"Error reversing direct sale {getattr(direct_sale, 'id', 'unknown')}: {str(e)}", exc_info=True)
        db_session.rollback()
        return False


def reverse_sales_transaction_posting(db_session, transaction, user=None):
    """
    Reverse ledger postings for a direct sales in a single optimized query.

    Reversal rules:
    - Direct Sale-level: direct_sale_payment, direct_sale_item
    - Line-item-level: COGS (only for inventory items)

    Args:
        db_session: SQLAlchemy session
        transaction: SalesTransaction object
        user: current_user object (optional, for app_id)

    Returns:
        bool: True if reversal succeeded, False on error
    """
    try:
        app_id = getattr(user, "app_id", getattr(transaction, "app_id", None))
        if not app_id:
            raise ValueError("Cannot determine app_id for reversal.")

        # Get all payment allocation IDs
        payment_allocation_ids = [pa.id for pa in getattr(transaction, "payment_allocations", [])]

        # Build filter criteria for a single query
        filter_criteria = []

        # Add payment allocation transactions
        if payment_allocation_ids:
            filter_criteria.append(
                and_(
                    JournalEntry.source_type.in_([
                        "invoice_payment",
                        "overpayment_write_off",
                        "customer_credit"
                    ]),
                    JournalEntry.source_id.in_(payment_allocation_ids)
                )
            )

        # Nothing to reverse
        if not filter_criteria:
            logger.info(f"No ledger entries to reverse for direct sale {transaction.id}")
            return True

        # --- Single query to fetch all journal entries ---
        journal_entries = db_session.query(JournalEntry).filter(
            JournalEntry.app_id == app_id,
            or_(*filter_criteria)
        ).all()

        if not journal_entries:
            logger.warning(f"No ledger entries found for direct sale {transaction.id}")
            return True

        # Get unique journal IDs to check if they need to be deleted
        journal_ids = {entry.journal_id for entry in journal_entries}

        # --- Delete all journal entries in a loop ---
        for entry in journal_entries:
            db_session.delete(entry)

        # --- Check if parent journals are now empty and delete them ---
        for journal_id in journal_ids:
            journal = db_session.query(Journal).get(journal_id)
            if journal and not journal.entries:  # If journal has no entries left
                logger.info(f"Deleting empty journal #{journal.id} ({journal.journal_number})")
                db_session.delete(journal)

        # --- Mark direct sale as unposted ---
        if hasattr(transaction, "is_posted_to_ledger"):
            transaction.is_posted_to_ledger = False

        # --- Mark payment allocations as unposted ---
        for allocation in getattr(transaction, "payment_allocations", []):
            if hasattr(allocation, "is_posted_to_ledger"):
                allocation.is_posted_to_ledger = False

        db_session.flush()
        logger.info(f"Reversed {len(journal_entries)} ledger entries for direct sale {transaction.id}")
        return True

    except Exception as e:
        logger.error(f"Error reversing direct sale {getattr(transaction, 'id', 'unknown')}: {str(e)}", exc_info=True)
        db_session.rollback()
        return False


def remove_expense_journal_entries(db_session, expense_transaction_id):
    """
    Remove all journal entries associated with an expense transaction.
    """
    try:
        # Find journals that have entries linked to this expense
        journals = db_session.query(Journal).join(JournalEntry).filter(
            JournalEntry.source_type == 'expense_transaction',
            JournalEntry.source_id == expense_transaction_id
        ).all()

        if not journals:
            return True, "No journal records found to remove."

        deleted_count = 0
        for journal in journals:
            db_session.delete(journal)
            deleted_count += 1

        return True, f"Removed {deleted_count} journal records successfully."

    except Exception as e:
        db_session.rollback()
        logging.error(f"Error removing journal entries for expense {expense_transaction_id}: {str(e)}")
        return False, f"Error removing journal entries: {str(e)}"


from sqlalchemy import func

def delete_journal_entries_by_source(db_session, source_type, source_id, app_id):
    """
    Optimized delete of journal entries by source.
    Supports single source_type or list of source_types.
    """

    try:
        # Base query
        query = db_session.query(JournalEntry).filter(
            JournalEntry.source_id == source_id,
            JournalEntry.app_id == app_id
        )

        # Allow string or list
        if isinstance(source_type, (list, tuple, set)):
            query = query.filter(JournalEntry.source_type.in_(source_type))
            source_label = ", ".join(source_type)
        else:
            query = query.filter(JournalEntry.source_type == source_type)
            source_label = source_type

        # 🔹 Get affected journal IDs BEFORE deletion
        journal_ids = [
            j_id for (j_id,) in query.with_entities(JournalEntry.journal_id).distinct().all()
        ]

        if not journal_ids:
            return True, f"No journal entries found for {source_label} with ID {source_id}."

        # 🔹 Bulk delete journal entries
        deleted_count = query.delete(synchronize_session=False)

        # 🔹 Delete journals that now have zero entries
        for journal_id in journal_ids:
            remaining = db_session.query(func.count(JournalEntry.id)).filter(
                JournalEntry.journal_id == journal_id
            ).scalar()

            if remaining == 0:
                db_session.query(Journal).filter_by(
                    id=journal_id,
                    app_id=app_id
                ).delete(synchronize_session=False)

        return True, f"Removed {deleted_count} journal entries for {source_label}."

    except Exception as e:
        db_session.rollback()
        logging.error(f"Error removing journal entries for {source_type} {source_id}: {str(e)}")
        return False, f"Error removing journal entries: {str(e)}"


def delete_posted_fund_transfer_journals(db_session, app_id, transaction_id=None):
    """
    Find and delete all posted journals for fund transfers.
    """
    try:
        # Build the base query - INCLUDING STATUS FILTER
        query = db_session.query(Journal).filter(
            Journal.app_id == app_id,
            Journal.entries.any(JournalEntry.source_type == 'fund_transfer')
        )

        # Filter by specific transaction if provided
        if transaction_id:
            query = query.filter(
                Journal.entries.any(JournalEntry.source_id == transaction_id)
            )

        # Get the journals to be deleted for logging
        journals_to_delete = query.all()

        if not journals_to_delete:
            return {
                'status': 'success',
                'message': 'No posted fund transfer journals found to delete',
                'journals_deleted': 0,
                'journal_numbers': []
            }

        journal_numbers = [journal.journal_number for journal in journals_to_delete]
        journal_ids = [journal.id for journal in journals_to_delete]

        # Delete journal entries first (due to foreign key constraints)
        db_session.query(JournalEntry).filter(
            JournalEntry.journal_id.in_(journal_ids)
        ).delete(synchronize_session=False)

        # Now delete the actual Journal records
        delete_count = query.delete(synchronize_session=False)

        db_session.commit()

        logger.info(f"Deleted {delete_count} posted fund transfer journals: {journal_numbers}")

        return {
            'status': 'success',
            'message': f'Successfully deleted {delete_count} posted fund transfer journal(s)',
            'journals_deleted': delete_count,
            'journal_numbers': journal_numbers,
            'journal_ids': journal_ids
        }

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error deleting posted fund transfer journals: {str(e)}")
        raise ValueError(f"Failed to delete posted journals: {str(e)}")


def reverse_purchase_inventory_entries(db_session, inventory_entry):
    """
    Reverse inventory entries specifically for purchases
    """
    try:
        total_reversed_cost = 0
        items_to_reverse = []  # Store items for batch reversal

        # First pass: collect all reversal data
        for line_item in inventory_entry.line_items:
            # Get all transaction details for this line item
            transaction_details = db_session.query(InventoryTransactionDetail).filter_by(
                inventory_entry_line_item_id=line_item.id
            ).all()

            # Collect reversal data for each transaction
            for transaction in transaction_details:
                total_reversed_cost += transaction.total_cost
                items_to_reverse.append({
                    'app_id': transaction.app_id,
                    'item_id': transaction.item_id,
                    'location_id': transaction.location_id,
                    'quantity': -transaction.quantity,  # Reverse quantity
                    'total_cost': -transaction.total_cost  # Reverse value
                })

            # Delete all transaction details for this line item
            db_session.query(InventoryTransactionDetail).filter(
                InventoryTransactionDetail.inventory_entry_line_item_id == line_item.id
            ).delete()

            # Delete the line item itself
            db_session.delete(line_item)

            db_session.flush()

            db_session.delete(inventory_entry)

        # Second pass: execute all reversals in batch
        for item in items_to_reverse:
            update_inventory_summary(
                db_session,
                item['app_id'],
                item['item_id'],
                item['location_id'],
                item['quantity'],
                item['total_cost']
            )

        # Commit the deletions
        db_session.flush()
        return True

    except Exception as e:
        logger.error(f"Error reversing inventory entry {inventory_entry.id}: {str(e)}")
        db_session.rollback()
        raise


def reverse_direct_purchase_posting(db_session, direct_purchase, user=None):
    """
    Reverse ledger postings for a direct purchase in a single optimized query.

    Reversal rules:
    - Direct Purchase-level: direct_purchase_payment, direct_purchase_inventory, direct_purchase_expense
    - Shipping and handling: direct_purchase_shipping

    Args:
        db_session: SQLAlchemy session
        direct_purchase: DirectPurchaseTransaction object
        user: current_user object (optional, for app_id)

    Returns:
        bool: True if reversal succeeded, False on error
    """
    try:
        app_id = getattr(user, "app_id", getattr(direct_purchase, "app_id", None))
        if not app_id:
            raise ValueError("Cannot determine app_id for reversal.")

        # --- Determine eligible source IDs and types in memory ---
        direct_purchase_level_types = ["direct_purchase_payment", "direct_purchase_inventory",
                                       "direct_purchase_expense", "direct_purchase_shipping"]

        # Get all payment allocation IDs
        payment_allocation_ids = [pa.id for pa in getattr(direct_purchase, "payment_allocations", [])]

        # Get line item IDs for inventory and expense items
        line_item_ids = [
            item.id for item in getattr(direct_purchase, "direct_purchase_items", [])
        ]

        # Build filter criteria for a single query
        filter_criteria = []

        # Add payment allocation transactions
        if payment_allocation_ids:
            filter_criteria.append(
                and_(
                    JournalEntry.source_type == "direct_purchase_payment",
                    JournalEntry.source_id.in_(payment_allocation_ids)
                )
            )

        # Add line item transactions (inventory and expense)
        if line_item_ids:
            filter_criteria.append(
                and_(
                    JournalEntry.source_type.in_(["direct_purchase_inventory", "direct_purchase_expense"]),
                    JournalEntry.source_id.in_(line_item_ids)
                )
            )

        # Add shipping and handling transactions
        filter_criteria.append(
            and_(
                JournalEntry.source_type == "direct_purchase_shipping",
                JournalEntry.source_id == direct_purchase.id
            )
        )

        # Nothing to reverse
        if not filter_criteria:
            logger.info(f"No ledger entries to reverse for direct purchase {direct_purchase.id}")
            return True

        # --- Single query to fetch all journal entries ---
        journal_entries = db_session.query(JournalEntry).filter(
            JournalEntry.app_id == app_id,
            or_(*filter_criteria)
        ).all()

        if not journal_entries:
            logger.warning(f"No ledger entries found for direct purchase {direct_purchase.id}")
            return True

        # Get unique journal IDs to check if they need to be deleted
        journal_ids = {entry.journal_id for entry in journal_entries}

        # --- Delete all journal entries in a loop ---
        for entry in journal_entries:
            db_session.delete(entry)

        # --- Check if parent journals are now empty and delete them ---
        for journal_id in journal_ids:
            journal = db_session.query(Journal).get(journal_id)
            if journal and not journal.entries:  # If journal has no entries left
                logger.info(f"Deleting empty journal #{journal.id} ({journal.journal_number})")
                db_session.delete(journal)

        # --- Mark direct purchase as unposted ---
        if hasattr(direct_purchase, "is_posted_to_ledger"):
            direct_purchase.is_posted_to_ledger = False

        # --- Mark shipping as unposted ---
        if hasattr(direct_purchase, "shipping_handling_posted"):
            direct_purchase.shipping_handling_posted = False

        # --- Mark payment allocations as unposted ---
        for allocation in getattr(direct_purchase, "payment_allocations", []):
            if hasattr(allocation, "is_posted_to_ledger"):
                allocation.is_posted_to_ledger = False

        # --- Mark goods receipts as unposted ---
        for receipt in getattr(direct_purchase, "goods_receipts", []):
            if hasattr(receipt, "is_posted_to_ledger"):
                receipt.is_posted_to_ledger = False
            if hasattr(receipt, "inventory_posted"):
                receipt.inventory_posted = False

        db_session.flush()
        logger.info(f"Reversed {len(journal_entries)} ledger entries for direct purchase {direct_purchase.id}")
        return True

    except Exception as e:
        logger.error(f"Error reversing direct purchase {getattr(direct_purchase, 'id', 'unknown')}: {str(e)}",
                     exc_info=True)
        db_session.rollback()
        return False


def reverse_po_transaction_posting(db_session, transaction, user=None):
    """
    Reverse ledger postings for a PO transaction in a single optimized query.

    Reversal rules:
    - PO Payment-level: purchase_order_payment
    - Goods receipt: goods_receipt

    Args:
        db_session: SQLAlchemy session
        transaction: PurchaseTransaction object
        user: current_user object (optional, for app_id)

    Returns:
        bool: True if reversal succeeded, False on error
    """
    try:
        app_id = getattr(user, "app_id", getattr(transaction, "app_id", None))
        if not app_id:
            raise ValueError("Cannot determine app_id for reversal.")

        # Get all payment allocation IDs
        payment_allocation_ids = [pa.id for pa in getattr(transaction, "payment_allocations", [])]

        # Build filter criteria for a single query
        filter_criteria = []

        # Add payment allocation transactions
        if payment_allocation_ids:
            filter_criteria.append(
                and_(
                    JournalEntry.source_type == "purchase_order_payment",
                    JournalEntry.source_id.in_(payment_allocation_ids)
                )
            )

        # Add goods receipt transactions if this transaction is linked to a PO
        if transaction.purchase_order_id:
            goods_receipts = db_session.query(GoodsReceipt).filter(
                GoodsReceipt.purchase_order_id == transaction.purchase_order_id,
                GoodsReceipt.app_id == app_id
            ).all()

            for receipt in goods_receipts:
                filter_criteria.append(
                    and_(
                        JournalEntry.source_type == "goods_receipt",
                        JournalEntry.source_id == receipt.id
                    )
                )

        # Nothing to reverse
        if not filter_criteria:
            logger.info(f"No ledger entries to reverse for PO transaction {transaction.id}")
            return True

        # --- Single query to fetch all journal entries ---
        journal_entries = db_session.query(JournalEntry).filter(
            JournalEntry.app_id == app_id,
            or_(*filter_criteria)
        ).all()

        if not journal_entries:
            logger.warning(f"No ledger entries found for PO transaction {transaction.id}")
            return True

        # Get unique journal IDs to check if they need to be deleted
        journal_ids = {entry.journal_id for entry in journal_entries}

        # --- Delete all journal entries in a loop ---
        for entry in journal_entries:
            db_session.delete(entry)

        # --- Check if parent journals are now empty and delete them ---
        for journal_id in journal_ids:
            journal = db_session.query(Journal).get(journal_id)
            if journal and not journal.entries:  # If journal has no entries left
                logger.info(f"Deleting empty journal #{journal.id} ({journal.journal_number})")
                db_session.delete(journal)

        # --- Mark PO transaction as unposted ---
        if hasattr(transaction, "is_posted_to_ledger"):
            transaction.is_posted_to_ledger = False

        # --- Mark payment allocations as unposted ---
        for allocation in getattr(transaction, "payment_allocations", []):
            if hasattr(allocation, "is_posted_to_ledger"):
                allocation.is_posted_to_ledger = False

        # --- Mark goods receipts as unposted ---
        if transaction.purchase_order_id:
            goods_receipts = db_session.query(GoodsReceipt).filter(
                GoodsReceipt.purchase_order_id == transaction.purchase_order_id,
                GoodsReceipt.app_id == app_id
            ).all()

            for receipt in goods_receipts:
                if hasattr(receipt, "is_posted_to_ledger"):
                    receipt.is_posted_to_ledger = False
                if hasattr(receipt, "inventory_posted"):
                    receipt.inventory_posted = False

        db_session.flush()
        logger.info(f"Reversed {len(journal_entries)} ledger entries for PO transaction {transaction.id}")
        return True

    except Exception as e:
        logger.error(f"Error reversing PO transaction {getattr(transaction, 'id', 'unknown')}: {str(e)}", exc_info=True)
        db_session.rollback()
        return False


def _delete_asset_movement_ledger_entries(db_session, asset_movement, current_user):
    """
    Delete ledger transactions linked to asset movements - OPTIMIZED with bulk delete
    """
    try:
        app_id = current_user.app_id

        # Get all journal IDs before deletion to clean up empty journals
        journal_entries = db_session.query(JournalEntry).filter(
            JournalEntry.source_type.in_(['asset_movement', 'asset', 'assets']),
            JournalEntry.source_id == asset_movement.id,
            JournalEntry.app_id == app_id
        ).all()

        if not journal_entries:
            # Try without source_type filter as fallback
            journal_entries = db_session.query(JournalEntry).filter(
                JournalEntry.source_id == asset_movement.id,
                JournalEntry.app_id == app_id
            ).all()

        if not journal_entries:
            logger.warning(f"No ledger entries found for asset movement {asset_movement.id}")
            return True, "No ledger entries found to delete"

        # Get unique journal IDs
        journal_ids = {entry.journal_id for entry in journal_entries if entry.journal_id}
        entry_count = len(journal_entries)

        # BULK DELETE: Delete all journal entries in one query
        deleted_count = db_session.query(JournalEntry).filter(
            JournalEntry.id.in_([entry.id for entry in journal_entries])
        ).delete(synchronize_session=False)

        # Clean up empty journals
        deleted_journals = 0
        for journal_id in journal_ids:
            # Check if journal has any entries left
            remaining_entries = db_session.query(JournalEntry).filter(
                JournalEntry.journal_id == journal_id
            ).count()

            if remaining_entries == 0:
                db_session.query(Journal).filter(Journal.id == journal_id).delete(synchronize_session=False)
                deleted_journals += 1

        logger.info(
            f"Deleted {deleted_count} ledger entries and {deleted_journals} empty journals "
            f"for asset movement {asset_movement.id}"
        )
        return True, f"Deleted {deleted_count} ledger entries successfully"

    except Exception as e:
        logger.error(f"Error deleting ledger entries for asset movement {asset_movement.id}: {str(e)}")
        return False, str(e)

