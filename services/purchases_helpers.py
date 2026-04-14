# app/services/sales_helpers.py

import logging
import math
import re
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from flask_login import current_user
from sqlalchemy import and_, or_
from sqlalchemy.orm import joinedload

from ai import get_base_currency, get_exchange_rate
from db import Session
from models import PayrollPeriod, Employee, PayrollTransaction, PayRollStatusEnum, AdvancePayment, ChartOfAccounts, \
    Category, OpeningBalance, DirectSalesTransaction, PaymentAllocation, DirectSaleItem, SalesInvoice, InventoryEntry, \
    InventoryEntryLineItem, InventoryTransactionDetail, SalesTransaction, Quotation, SalesOrder, \
    DirectPurchaseTransaction, PurchasePaymentAllocation, DirectPurchaseItem, JournalEntry, Journal, PurchaseOrder, \
    PurchaseTransaction
from services.inventory_helpers import update_inventory_summary, safe_clear_stock_history_cache
from utils import convert_to_base_currency
from utils_and_helpers.numbers import generate_sequence_number

logger = logging.getLogger(__name__)


def generate_direct_purchase_number(db_session=None, app_id=None):
    """Generate direct purchase number"""
    return generate_sequence_number(
        prefix="PUR",
        table_model=DirectPurchaseTransaction,
        number_field="direct_purchase_number",
        db_session=db_session,
        app_id=app_id
    )


def allocate_purchase_payment(
        payment_date: datetime.date,
        payment_amount: Decimal,
        db_session,
        payment_mode_id: int,
        payment_type: str,
        payment_account_id: int | None,
        prepaid_account_id: int | None,  # Moved prepaid up since it's commonly used
        purchase_transaction: PurchaseTransaction = None,
        purchase_transaction_id: int = None,
        inventory_account_id: int | None = None,
        non_inventory_account_id: int | None = None,
        other_expense_account_id: int | None = None,
        other_expense_service_id: int | None = None,
        tax_payable_id: int | None = None,
        tax_receivable_id: int | None = None,
        credit_purchase_account_id: int | None = None,
        reference: str = None,
        exchange_rate_id: int = None,
) -> PurchasePaymentAllocation:
    """
    Allocates a purchase payment proportionally between inventory, non-inventory, and expenses.
    Uses the pre-calculated totals from PurchaseOrder, following the exact same logic as direct purchase allocation.

    Args:
        purchase_transaction: Obj
        purchase_transaction_id: ID of the purchase transaction
        payment_date: Date of payment
        payment_amount: Amount being paid (Decimal)
        db_session: Database session
        payment_mode_id: Payment mode ID
        payment_type: advance_payment or regular_payment
        payment_account_id: Payment account ID
        inventory_account_id: Inventory asset account ID
        non_inventory_account_id: Non-inventory account ID
        other_expense_account_id: Other expenses account ID
        other_expense_service_id: Other expense service account ID
        tax_payable_id: Tax payable account ID
        tax_receivable_id: Tax receivable account ID
        credit_purchase_account_id: Credit purchase account ID
        prepaid_account_id: Advance account id
        reference: Optional payment reference

    Returns:
        PurchasePaymentAllocation: The created allocation record
    """

    # Fetch the purchase transaction and related purchase order
    transaction = purchase_transaction
    if not transaction:
        transaction = db_session.get(PurchaseTransaction, purchase_transaction_id)
        if not transaction:
            raise ValueError("Purchase transaction not found")

    po = transaction.purchase_orders
    if not po:
        raise ValueError("Purchase order not found for transaction")

    # Validate amounts
    payment_amount = Decimal(payment_amount)
    if payment_amount < 0:
        raise ValueError("Payment amount must be greater than 0")

    total_amount = Decimal(po.total_amount or 0)
    total_line_subtotal = Decimal(po.total_line_subtotal or 0)
    if total_line_subtotal == 0:
        raise ValueError("Total line subtotal cannot be zero when calculating proportions.")

    if total_amount == 0:
        raise ValueError("Purchase order total amount cannot be zero")

    # Calculate category totals from line items (to match direct purchase logic)
    inventory_total = Decimal(0)
    non_inventory_total = Decimal(0)
    service_expense_total = Decimal(0)

    for item in po.purchase_order_items:
        if item.item_type == "inventory":
            inventory_total += Decimal(item.total_price or 0)
        elif item.item_type in ("non_inventory", "non-inventory"):
            non_inventory_total += Decimal(item.total_price or 0)
        else:
            service_expense_total += Decimal(item.total_price or 0)

    # Calculate taxes
    total_tax_amount = Decimal(po.total_tax_amount or 0)
    total_line_item_tax_amount = sum(
        Decimal(item.tax_amount or 0)
        for item in po.purchase_order_items
        if item.item_type in ['inventory', 'non_inventory', 'non-inventory', 'service'] and item.tax_amount is not None
    )
    overall_purchase_tax = total_tax_amount - total_line_item_tax_amount

    # Handle discounts
    discount_amount = Decimal(po.calculated_discount_amount or 0)

    # Get the proportions of the categories
    inventory_proportion = (inventory_total / total_line_subtotal) if total_line_subtotal > 0 else Decimal(0)
    non_inventory_proportion = (non_inventory_total / total_line_subtotal) if total_line_subtotal > 0 else Decimal(0)
    service_expense_proportion = (service_expense_total / total_line_subtotal) if total_line_subtotal > 0 else Decimal(
        0)

    # Apply discount (subtracting from totals)
    if discount_amount > 0:
        inventory_total -= (inventory_proportion * discount_amount)
        non_inventory_total -= (non_inventory_proportion * discount_amount)
        service_expense_total -= (service_expense_proportion * discount_amount)

    # Apply tax (adding to totals)
    if overall_purchase_tax > 0:
        inventory_total += (inventory_proportion * overall_purchase_tax)
        non_inventory_total += (non_inventory_proportion * overall_purchase_tax)
        service_expense_total += (service_expense_proportion * overall_purchase_tax)

    # Prevent negative values
    inventory_total = max(inventory_total, Decimal(0))
    non_inventory_total = max(non_inventory_total, Decimal(0))
    service_expense_total = max(service_expense_total, Decimal(0))

    shipping_cost = Decimal(po.shipping_cost or 0)
    handling_cost = Decimal(po.handling_cost or 0)
    other_expenses = shipping_cost + handling_cost

    # Total base for allocation should exclude other expenses (matching direct purchase logic)
    allocatable_base = total_amount - other_expenses

    # Allocation function (matches direct purchase exactly)
    def allocate_category(category_total, category_name=""):
        # Use allocatable_base for all but 'Other Expenses'
        base = allocatable_base if category_name != "Other Expenses" else total_amount
        if base == 0:
            return Decimal(0)

        category_ratio = category_total / base
        allocated = (payment_amount * category_ratio).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
        return allocated

    # Perform allocations (same order as direct purchase)
    allocated_inventory = allocate_category(inventory_total, "Inventory")
    allocated_non_inventory = allocate_category(non_inventory_total, "Non-Inventory")
    allocated_services = allocate_category(service_expense_total, "Services")
    allocated_other_expenses = allocate_category(other_expenses, "Other Expenses")

    # Prevent negative allocations
    allocated_inventory = max(allocated_inventory, Decimal(0))
    allocated_non_inventory = max(allocated_non_inventory, Decimal(0))
    allocated_services = max(allocated_services, Decimal(0))
    allocated_other_expenses = max(allocated_other_expenses, Decimal(0))

    # Handle rounding (identical to direct purchase)
    total_allocated = (
            allocated_inventory + allocated_non_inventory +
            allocated_other_expenses + allocated_services
    )
    rounding_diff = (payment_amount - total_allocated).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)

    if rounding_diff != 0:
        allocations = {
            'inventory': allocated_inventory,
            'non_inventory': allocated_non_inventory,
            'services': allocated_services,
            'other_expenses': allocated_other_expenses
        }
        largest_category = max(allocations.items(), key=lambda x: x[1])[0]
        allocations[largest_category] += rounding_diff

        allocated_inventory = allocations['inventory']
        allocated_non_inventory = allocations['non_inventory']
        allocated_services = allocations['services']
        allocated_other_expenses = allocations['other_expenses']


    # Create allocation record (same structure as direct purchase)
    allocation = PurchasePaymentAllocation(
        payment_id=transaction.id,
        payment_date=payment_date,
        allocated_inventory=allocated_inventory,
        allocated_non_inventory=allocated_non_inventory,
        allocated_services=allocated_services,
        allocated_other_expenses=allocated_other_expenses,
        allocated_tax_receivable=Decimal(0),
        allocated_tax_payable=Decimal(0),
        inventory_account_id=inventory_account_id,
        non_inventory_account_id=non_inventory_account_id,
        other_expense_account_id=other_expense_account_id,
        other_expense_service_id=other_expense_service_id,
        tax_payable_account_id=tax_payable_id,
        tax_receivable_account_id=tax_receivable_id,
        payment_account_id=payment_account_id,
        credit_purchase_account=credit_purchase_account_id,
        payment_mode=payment_mode_id,
        payment_type=payment_type,
        reference=reference,
        app_id=po.app_id,
        prepaid_account_id=prepaid_account_id,
        exchange_rate_id=exchange_rate_id,
        is_posted_to_ledger=True
    )

    db_session.add(allocation)
    return allocation


def allocate_direct_purchase_payment(
        db_session,
        direct_purchase_id: int,
        payment_date: datetime.date,
        payment_amount: Decimal,
        payment_mode_id: int,
        payment_account_id: int,
        inventory_account_id: None,  # Will be handled during ledger posting
        non_inventory_account_id: int | None,
        other_expense_account_id: int | None,
        other_expense_service_id: int | None,
        credit_purchase_account_id: int | None,
        reference: str = None,
        is_posted_to_ledger: Optional[bool] = False,
        exchange_rate_id: int = None

) -> PurchasePaymentAllocation:
    """
    Allocates a direct purchase payment across inventory, non-inventory items, services, other expenses,
    and applicable taxes based on the proportional contribution of each category to the total purchase amount.

    This function:
    - Retrieves and validates the direct purchase transaction and its associated line items.
    - Proportionally allocates the payment amount to inventory, non-inventory, service expenses,
      shipping/handling costs (as other expenses), and taxes (receivable and payable).
    - Adjusts allocations for discounts and ensures rounding differences are handled.
    - Creates and returns a PurchasePaymentAllocation record for persistence in the database.

    Parameters:
    - direct_purchase_id: ID of the direct purchase transaction being paid for.
    - payment_date: Date the payment is made.
    - payment_amount: Total amount paid.
    - db_session: SQLAlchemy database session.
    - payment_mode_id: ID representing the mode of payment.
    - payment_account_id: Account ID from which the payment is made.
    - inventory_account_id, non_inventory_account_id: Account IDs for recording inventory and non-inventory costs.
    - other_expense_account_id, other_expense_service_id: Account IDs for other costs like shipping, handling, and services.
    - tax_payable_id, tax_receivable_id: Account IDs for tax-related entries.
    - credit_purchase_account_id: Account for credit purchase balancing.
    - reference: Optional payment reference or memo.

    Returns:
    - A populated PurchasePaymentAllocation object (not yet committed to the DB).
    """
    from ai import resolve_exchange_rate_for_transaction

    # Retrieve direct purchase record
    direct_purchase = db_session.query(DirectPurchaseTransaction).filter_by(id=direct_purchase_id).first()
    if not direct_purchase:
        raise ValueError("Direct purchase transaction not found")

    # Fetch all line items
    line_items = db_session.query(DirectPurchaseItem).filter_by(transaction_id=direct_purchase_id).all()

    # Validate amounts
    payment_amount = Decimal(payment_amount)
    if payment_amount < 0:
        raise ValueError("Payment amount must be greater than 0")

    total_amount = Decimal(direct_purchase.total_amount or 0)
    total_line_subtotal = Decimal(direct_purchase.total_line_subtotal or 0)
    if total_line_subtotal == 0:
        raise ValueError("Total line subtotal cannot be zero when calculating proportions.")

    if total_amount == 0:
        raise ValueError("Purchase order total amount cannot be zero")

    # Calculate category totals
    inventory_total = Decimal(0)
    non_inventory_total = Decimal(0)
    service_expense_total = Decimal(0)

    # Calculate taxes
    # Line-item tax is directly attached to items. Overall purchase tax includes any additional tax charges.

    total_tax_amount = Decimal(direct_purchase.total_tax_amount or 0)
    total_line_item_tax_amount = sum(
        Decimal(item.tax_amount or 0)
        for item in direct_purchase.direct_purchase_items
        if item.item_type in ['inventory', 'non_inventory', 'non-inventory', 'service'] and item.tax_amount is not None
    )
    overall_purchase_tax = total_tax_amount - total_line_item_tax_amount

    # Checking if there was a discount applied
    discount_amount = Decimal(direct_purchase.calculated_discount_amount or 0)
    for idx, item in enumerate(line_items, 1):
        if item.item_type == "inventory":
            inventory_total += Decimal(item.total_price or 0)
        # Adjust this check to match how item types are stored
        elif item.item_type in ("non_inventory", "non-inventory"):
            non_inventory_total += Decimal(item.total_price or 0)
        else:
            service_expense_total += Decimal(item.total_price or 0)

    # Get the propotions of the 3
    inventory_proportion = (inventory_total / total_line_subtotal)
    non_inventory_proportion = (non_inventory_total / total_line_subtotal)
    service_expense_proportion = (service_expense_total / total_line_subtotal)
    # Apply discount
    if discount_amount > 0:
        inventory_total -= (inventory_proportion * discount_amount)
        non_inventory_total -= (non_inventory_proportion * discount_amount)
        service_expense_total -= (service_expense_proportion * discount_amount)

    # Apply tax
    if overall_purchase_tax > 0:
        inventory_total += (inventory_proportion * overall_purchase_tax)
        non_inventory_total += (non_inventory_proportion * overall_purchase_tax)
        service_expense_total += (service_expense_proportion * overall_purchase_tax)

    # 🛡️ Prevent negatives
    inventory_total = max(inventory_total, Decimal(0))
    non_inventory_total = max(non_inventory_total, Decimal(0))
    service_expense_total = max(service_expense_total, Decimal(0))

    shipping_cost = Decimal(direct_purchase.shipping_cost or 0)
    handling_cost = Decimal(direct_purchase.handling_cost or 0)
    other_expenses = shipping_cost + handling_cost

    # Total base for allocation should exclude other expenses
    allocatable_base = total_amount - other_expenses

    # Allocation function
    def allocate_category(category_total, category_name=""):
        # Use allocatable_base for all but 'Other Expenses'
        base = allocatable_base if category_name != "Other Expenses" else total_amount

        if base == 0:
            return Decimal(0)

        category_ratio = category_total / base
        allocated = (payment_amount * category_ratio).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)

        return allocated

    # Perform allocations

    allocated_inventory = allocate_category(inventory_total, "Inventory")
    allocated_non_inventory = allocate_category(non_inventory_total, "Non-Inventory")
    allocated_services = allocate_category(service_expense_total, "Services")
    allocated_other_expenses = allocate_category(other_expenses, "Other Expenses")

    # 🛡️ Prevent negative allocations
    allocated_inventory = max(allocated_inventory, Decimal(0))
    allocated_non_inventory = max(allocated_non_inventory, Decimal(0))
    allocated_services = max(allocated_services, Decimal(0))
    allocated_other_expenses = max(allocated_other_expenses, Decimal(0))

    # Handle rounding
    total_allocated = (
            allocated_inventory + allocated_non_inventory +
            allocated_other_expenses + allocated_services
    )
    rounding_diff = (payment_amount - total_allocated).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)

    if rounding_diff != 0:
        allocations = {
            'inventory': allocated_inventory,
            'non_inventory': allocated_non_inventory,
            'services': allocated_services,
            'other_expenses': allocated_other_expenses
        }
        largest_category = max(allocations.items(), key=lambda x: x[1])[0]
        allocations[largest_category] += rounding_diff

        allocated_inventory = allocations['inventory']
        allocated_non_inventory = allocations['non_inventory']
        allocated_services = allocations['services']
        allocated_other_expenses = allocations['other_expenses']

    currency_id = direct_purchase.currency_id


    # Create allocation record
    allocation = PurchasePaymentAllocation(
        payment_id=None,
        direct_purchase_id=direct_purchase_id,
        payment_date=payment_date,
        payment_type='direct_purchase',
        allocated_inventory=allocated_inventory,
        allocated_non_inventory=allocated_non_inventory,
        allocated_services=allocated_services,
        allocated_other_expenses=allocated_other_expenses,
        allocated_tax_receivable=Decimal(0),
        allocated_tax_payable=Decimal(0),
        inventory_account_id=inventory_account_id,  # No need to save it here since the inventory item already has it
        non_inventory_account_id=non_inventory_account_id,
        other_expense_account_id=other_expense_account_id,
        other_expense_service_id=other_expense_service_id,
        tax_payable_account_id=None,
        tax_receivable_account_id=None,
        payment_account_id=payment_account_id,
        credit_purchase_account=credit_purchase_account_id,
        payment_mode=payment_mode_id,
        reference=reference,
        app_id=direct_purchase.app_id,
        exchange_rate_id=exchange_rate_id,
        is_posted_to_ledger=is_posted_to_ledger
    )

    db_session.add(allocation)
    return allocation


def suggest_next_direct_purchase_reference(db_session: Session):
    """
    Suggest the next direct sale reference based on the most recent reference entered by the user.
    - If there is no previous reference, return format: SALE-{month_year}-00001
    - If the last reference ends with a number, increment it while preserving leading zeros.
    - Otherwise, return the last reference with '-1' appended.
    """
    app_id = current_user.app_id  # Assumes current_user is available

    # Get the last entered direct sale reference
    last_entry = db_session.query(DirectPurchaseTransaction).filter(
        DirectPurchaseTransaction.app_id == app_id,
        DirectPurchaseTransaction.purchase_reference is not None
    ).order_by(DirectPurchaseTransaction.id.desc()).first()

    # If no previous reference exists, use the special format
    if not last_entry or not last_entry.purchase_reference:
        now = datetime.now()
        month_year = now.strftime("%m%y")  # e.g., "0325" for March 2025
        return f"PUR-REF-1"

    last_ref = last_entry.purchase_reference.strip()

    # Check if it ends with a number
    match = re.search(r'(\d+)$', last_ref)
    if match:
        number_str = match.group(1)
        number_len = len(number_str)  # Preserve digit length
        number = int(number_str) + 1
        new_ref = last_ref[:match.start(1)] + str(number).zfill(number_len)
    else:
        new_ref = f"{last_ref}-1"

    return new_ref


def suggest_next_purchase_order_reference(db_session: Session):
    """
    Suggest the next direct sale reference based on the most recent reference entered by the user.
    - If there is no previous reference, return format: SALE-{month_year}-00001
    - If the last reference ends with a number, increment it while preserving leading zeros.
    - Otherwise, return the last reference with '-1' appended.
    """
    app_id = current_user.app_id  # Assumes current_user is available

    # Get the last entered direct sale reference
    last_entry = db_session.query(PurchaseOrder).filter(
        PurchaseOrder.app_id == app_id,
        PurchaseOrder.purchase_order_reference is not None
    ).order_by(PurchaseOrder.id.desc()).first()

    # If no previous reference exists, use the special format
    if not last_entry or not last_entry.purchase_order_reference:
        now = datetime.now()
        month_year = now.strftime("%m%y")  # e.g., "0325" for March 2025
        return f"PUR-REF-1"

    last_ref = last_entry.purchase_order_reference.strip()

    # Check if it ends with a number
    match = re.search(r'(\d+)$', last_ref)
    if match:
        number_str = match.group(1)
        number_len = len(number_str)  # Preserve digit length
        number = int(number_str) + 1
        new_ref = last_ref[:match.start(1)] + str(number).zfill(number_len)
    else:
        new_ref = f"{last_ref}-1"

    return new_ref


def get_inventory_entries_for_direct_purchase(db_session, direct_purchase_id, app_id):
    """
    Fetch all inventory entries related to a specific purchase transaction
    """
    return db_session.query(InventoryEntry).filter(
        InventoryEntry.source_type == "purchase",
        InventoryEntry.source_id == direct_purchase_id,
        InventoryEntry.app_id == app_id,
        InventoryEntry.inventory_source == "direct_purchase"
    ).options(
        joinedload(InventoryEntry.line_items).joinedload(InventoryEntryLineItem.inventory_transaction_details)
    ).all()


def reverse_purchase_inventory_entries(db_session, inventory_entry):
    """
    Reverse inventory entries specifically for purchase transactions
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
                    'quantity': -transaction.quantity,  # Reverse quantity (decrease stock)
                    'total_cost': -transaction.total_cost  # Reverse value
                })

            # Delete all transaction details for this line item
            db_session.query(InventoryTransactionDetail).filter(
                InventoryTransactionDetail.inventory_entry_line_item_id == line_item.id
            ).delete()

            # Delete the line item itself
            db_session.delete(line_item)

        # Delete the main inventory entry
        db_session.delete(inventory_entry)
        db_session.flush()

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

        db_session.flush()
        safe_clear_stock_history_cache(logger)
        return True

    except Exception as e:
        logger.error(f"Error reversing purchase inventory entry {inventory_entry.id}: {str(e)}")
        db_session.rollback()
        raise


def reverse_direct_purchase_posting(db_session, direct_purchase, user=None):
    """
    Reverse ledger postings for a direct purchase in a single optimized query.
    """
    try:
        app_id = getattr(user, "app_id", getattr(direct_purchase, "app_id", None))
        if not app_id:
            raise ValueError("Cannot determine app_id for reversal.")

        # Get all payment allocation IDs
        payment_allocation_ids = [pa.id for pa in getattr(direct_purchase, "payment_allocations", [])]

        # Get line item IDs for inventory items
        line_item_ids = [
            item.id
            for item in getattr(direct_purchase, "direct_purchase_items", [])
            if getattr(item, "item_type", None) == "inventory"
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

        # Add inventory item transactions
        if line_item_ids:
            filter_criteria.append(
                and_(
                    JournalEntry.source_type == "direct_purchase_inventory",
                    JournalEntry.source_id.in_(line_item_ids)
                )
            )

        # Nothing to reverse
        if not filter_criteria:
            logger.info(f"No ledger entries to reverse for direct purchase {direct_purchase.id}")
            return True

        # Single query to fetch all journal entries
        journal_entries = db_session.query(JournalEntry).filter(
            JournalEntry.app_id == app_id,
            or_(*filter_criteria)
        ).all()

        if not journal_entries:
            logger.warning(f"No ledger entries found for direct purchase {direct_purchase.id}")
            return True

        # Get unique journal IDs to check if they need to be deleted
        journal_ids = {entry.journal_id for entry in journal_entries}

        # Delete all journal entries
        for entry in journal_entries:
            db_session.delete(entry)

        # Check if parent journals are now empty and delete them
        for journal_id in journal_ids:
            journal = db_session.query(Journal).get(journal_id)
            if journal and not journal.entries:
                logger.info(f"Deleting empty journal #{journal.id} ({journal.journal_number})")
                db_session.delete(journal)

        db_session.flush()
        logger.info(f"Reversed {len(journal_entries)} ledger entries for direct purchase {direct_purchase.id}")
        return True

    except Exception as e:
        logger.error(f"Error reversing direct purchase {getattr(direct_purchase, 'id', 'unknown')}: {str(e)}",
                     exc_info=True)
        db_session.rollback()
        return False


def generate_next_purchase_order_number(db_session=None, app_id=None):
    """Generate purchase order number: PO-MMYY-XXXXX"""
    return generate_sequence_number(
        prefix="PO",
        table_model=PurchaseOrder,
        number_field="purchase_order_number",
        db_session=db_session,
        app_id=app_id
    )


def calculate_direct_purchase_landed_costs(db_session, direct_purchase, purchase_items, app_id):
    """
    Calculate true landed costs for direct purchase items using the same logic as goods receipts
    Includes proper allocation of discounts, taxes, shipping, and handling
    """
    from decimal import Decimal, ROUND_HALF_UP

    # Get overall purchase values
    total_line_subtotal = Decimal(str(direct_purchase.total_line_subtotal or 0))
    calculated_discount_amount = Decimal(str(direct_purchase.calculated_discount_amount or 0))
    total_tax_amount = Decimal(str(direct_purchase.total_tax_amount or 0))
    shipping_cost = Decimal(str(getattr(direct_purchase, 'shipping_cost', 0) or 0))
    handling_cost = Decimal(str(getattr(direct_purchase, 'handling_cost', 0) or 0))

    # Calculate line item tax total
    total_line_item_tax = sum(
        Decimal(str(item.tax_amount or 0))
        for item in purchase_items
        if item.tax_amount is not None
    )

    # Calculate purchase level tax (tax applied at purchase level, not line level)
    overall_purchase_tax = total_tax_amount - total_line_item_tax

    # Calculate category totals
    inventory_items = [item for item in purchase_items if item.item_type == "inventory"]
    non_inventory_items = [item for item in purchase_items if item.item_type in ("non_inventory", "non-inventory")]
    service_items = [item for item in purchase_items if item.item_type == "service"]

    inventory_total = sum(Decimal(str(item.total_price or 0)) for item in inventory_items)
    non_inventory_total = sum(Decimal(str(item.total_price or 0)) for item in non_inventory_items)
    service_total = sum(Decimal(str(item.total_price or 0)) for item in service_items)

    # Calculate proportions for ALL categories
    total_all_items_value = total_line_subtotal

    inventory_proportion = inventory_total / total_all_items_value if total_all_items_value > 0 else Decimal(0)
    non_inventory_proportion = non_inventory_total / total_all_items_value if total_all_items_value > 0 else Decimal(0)
    service_proportion = service_total / total_all_items_value if total_all_items_value > 0 else Decimal(0)

    # Calculate category shares of discount and tax for ALL categories
    inventory_discount_share = inventory_proportion * calculated_discount_amount
    inventory_tax_share = inventory_proportion * overall_purchase_tax

    non_inventory_discount_share = non_inventory_proportion * calculated_discount_amount
    non_inventory_tax_share = non_inventory_proportion * overall_purchase_tax

    service_discount_share = service_proportion * calculated_discount_amount
    service_tax_share = service_proportion * overall_purchase_tax

    # Calculate category shares of shipping and handling for tracking (but not for landed cost)
    inventory_shipping_handling_share = inventory_proportion * (shipping_cost + handling_cost)
    non_inventory_shipping_handling_share = non_inventory_proportion * (shipping_cost + handling_cost)
    service_shipping_handling_share = service_proportion * (shipping_cost + handling_cost)

    allocations = []

    for item in purchase_items:
        # Calculate this item's proportion of total purchase value
        item_proportion_total = Decimal(
            str(item.total_price)) / total_all_items_value if total_all_items_value > 0 else Decimal(0)

        if item.item_type == "inventory" and inventory_total > 0:
            # Calculate this item's proportion of total inventory
            item_proportion_category = Decimal(str(item.total_price)) / inventory_total

            # Calculate allocated shipping + handling for tracking
            allocated_shipping_handling = item_proportion_total * (shipping_cost + handling_cost)

            # Store combined shipping and handling
            item.allocated_shipping_handling = float(
                allocated_shipping_handling.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate adjusted total cost (EXCLUDING shipping and handling from landed cost)
            item_adjusted_total = (
                    Decimal(str(item.total_price)) -
                    (item_proportion_total * calculated_discount_amount) +
                    (item_proportion_total * overall_purchase_tax)
            )

            # Calculate unit cost (excluding shipping and handling)
            item_quantity = Decimal(str(item.quantity or 1))
            if item_quantity > 0:
                unit_cost = item_adjusted_total / item_quantity
            else:
                unit_cost = Decimal(str(item.unit_price or 0))

            # Update the item with calculated costs
            item.unit_cost = float(unit_cost.quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP))
            item.total_cost = float(item_adjusted_total.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate allocated tax (line item tax + allocated purchase level tax)
            line_item_tax = Decimal(str(item.tax_amount or 0))
            po_level_tax = item_proportion_total * overall_purchase_tax
            total_allocated_tax = po_level_tax

            allocations.append({
                'item_id': item.id,
                'allocated_amount': float(item_adjusted_total),
                'allocated_tax': float(total_allocated_tax),
                'allocated_discount': 0.0,
                'allocated_shipping_handling': float(allocated_shipping_handling)
            })

        elif item.item_type in ("non_inventory", "non-inventory") and non_inventory_total > 0:
            # Calculate this item's proportion of total non-inventory
            item_proportion_category = Decimal(str(item.total_price)) / non_inventory_total

            # Calculate allocated shipping + handling for tracking
            allocated_shipping_handling = item_proportion_total * (shipping_cost + handling_cost)

            item.allocated_shipping_handling = float(
                allocated_shipping_handling.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate adjusted total cost (EXCLUDING shipping and handling)
            item_adjusted_total = (
                    Decimal(str(item.total_price)) -
                    (item_proportion_total * calculated_discount_amount) +
                    (item_proportion_total * overall_purchase_tax)
            )

            # Calculate unit cost
            item_quantity = Decimal(str(item.quantity or 1))
            if item_quantity > 0:
                unit_cost = item_adjusted_total / item_quantity
            else:
                unit_cost = Decimal(str(item.unit_price or 0))

            # Update the item with calculated costs
            item.unit_cost = float(unit_cost.quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP))
            item.total_cost = float(item_adjusted_total.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate allocated tax
            line_item_tax = Decimal(str(item.tax_amount or 0))
            po_level_tax = item_proportion_total * overall_purchase_tax
            total_allocated_tax = po_level_tax

            allocations.append({
                'item_id': item.id,
                'allocated_amount': float(item_adjusted_total),
                'allocated_tax': float(total_allocated_tax),
                'allocated_discount': 0.0,
                'allocated_shipping_handling': float(allocated_shipping_handling)
            })

        elif item.item_type == "service" and service_total > 0:
            # Calculate this item's proportion of total services
            item_proportion_category = Decimal(str(item.total_price)) / service_total

            # Calculate allocated shipping + handling for tracking
            allocated_shipping_handling = item_proportion_total * (shipping_cost + handling_cost)

            item.allocated_shipping_handling = float(
                allocated_shipping_handling.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate adjusted total cost (EXCLUDING shipping and handling)
            item_adjusted_total = (
                    Decimal(str(item.total_price)) -
                    (item_proportion_total * calculated_discount_amount) +
                    (item_proportion_total * overall_purchase_tax)
            )

            # For services, quantity is typically 1, but handle anyway
            item_quantity = Decimal(str(item.quantity or 1))
            if item_quantity > 0:
                unit_cost = item_adjusted_total / item_quantity
            else:
                unit_cost = Decimal(str(item.unit_price or 0))

            # Update the item with calculated costs
            item.unit_cost = float(unit_cost.quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP))
            item.total_cost = float(item_adjusted_total.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate allocated tax
            line_item_tax = Decimal(str(item.tax_amount or 0))
            po_level_tax = item_proportion_total * overall_purchase_tax
            total_allocated_tax = po_level_tax

            allocations.append({
                'item_id': item.id,
                'allocated_amount': float(item_adjusted_total),
                'allocated_tax': float(total_allocated_tax),
                'allocated_discount': 0.0,
                'allocated_shipping_handling': float(allocated_shipping_handling)
            })

        else:
            # Fallback for items that don't fit any category
            allocated_amount = Decimal(str(item.total_price or 0))

            # Calculate allocated tax for fallback items
            line_item_tax = Decimal(str(item.tax_amount or 0))
            if total_all_items_value > 0:
                item_proportion = Decimal(str(item.total_price or 0)) / total_all_items_value
                po_level_tax = item_proportion * overall_purchase_tax
                allocated_shipping_handling = item_proportion * (shipping_cost + handling_cost)
            else:
                po_level_tax = Decimal(0)
                allocated_shipping_handling = Decimal(0)

            total_allocated_tax = po_level_tax

            item.allocated_shipping_handling = float(
                allocated_shipping_handling.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))
            item.unit_cost = float(Decimal(str(item.unit_price or 0)))
            item.total_cost = float(allocated_amount)

            allocations.append({
                'item_id': item.id,
                'allocated_amount': float(allocated_amount),
                'allocated_tax': float(total_allocated_tax),
                'allocated_discount': 0.0,
                'allocated_shipping_handling': float(allocated_shipping_handling)
            })

    db_session.flush()
    return allocations


def calculate_purchase_order_landed_costs(db_session, purchase_order, app_id):
    """
    Calculate true landed costs for purchase order items including overall discounts and taxes
    This can be called when the PO is converted to an actual purchase
    """
    from decimal import Decimal, ROUND_HALF_UP

    # Get overall purchase order values
    total_line_subtotal = Decimal(str(purchase_order.total_line_subtotal or 0))
    calculated_discount_amount = Decimal(str(purchase_order.calculated_discount_amount or 0))
    total_tax_amount = Decimal(str(purchase_order.total_tax_amount or 0))

    # Calculate line item tax total
    total_line_item_tax = sum(
        Decimal(str(item.tax_amount or 0))
        for item in purchase_order.purchase_order_items
        if item.tax_amount is not None
    )

    # Calculate overall purchase tax (tax not allocated to specific items)
    overall_purchase_tax = total_tax_amount - total_line_item_tax

    # Separate inventory items from others
    inventory_items = [item for item in purchase_order.purchase_order_items if item.item_type == "inventory"]
    non_inventory_items = [item for item in purchase_order.purchase_order_items if
                           item.item_type in ("non_inventory", "non-inventory")]
    service_items = [item for item in purchase_order.purchase_order_items if item.item_type == "service"]

    # Calculate category totals
    inventory_total = sum(Decimal(str(item.total_price or 0)) for item in inventory_items)
    non_inventory_total = sum(Decimal(str(item.total_price or 0)) for item in non_inventory_items)
    service_total = sum(Decimal(str(item.total_price or 0)) for item in service_items)

    # Calculate proportions
    inventory_proportion = inventory_total / total_line_subtotal if total_line_subtotal > 0 else Decimal(0)
    non_inventory_proportion = non_inventory_total / total_line_subtotal if total_line_subtotal > 0 else Decimal(0)
    service_proportion = service_total / total_line_subtotal if total_line_subtotal > 0 else Decimal(0)

    # Calculate category shares of discount, tax
    inventory_discount_share = inventory_proportion * calculated_discount_amount
    inventory_tax_share = inventory_proportion * overall_purchase_tax
    # inventory_shipping_share = inventory_proportion * shipping_cost
    # inventory_handling_share = inventory_proportion * handling_cost

    # Calculate adjusted inventory total
    adjusted_inventory_total = (
            inventory_total -
            inventory_discount_share +
            inventory_tax_share
    )

    # Distribute adjusted costs to individual inventory items
    for item in inventory_items:
        if Decimal(str(item.total_price or 0)) > 0 and inventory_total > 0:
            # Calculate this item's proportion of total inventory
            item_proportion = Decimal(str(item.total_price)) / inventory_total

            # Calculate adjusted total cost for this item
            item_adjusted_total = (
                    Decimal(str(item.total_price)) -
                    (item_proportion * inventory_discount_share) +
                    (item_proportion * inventory_tax_share)
            )

            # Calculate true unit cost
            item_quantity = Decimal(str(item.quantity or 1))
            if item_quantity > 0:
                item_unit_cost = item_adjusted_total / item_quantity
            else:
                item_unit_cost = Decimal(str(item.unit_price or 0))

            # Update the item with calculated costs
            item.unit_cost = float(item_unit_cost.quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP))
            item.total_cost = float(item_adjusted_total.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            logger.info(f"PO Landed cost calculated for item {item.id}: "
                        f"Unit Cost: {item.unit_cost}, Total Cost: {item.total_cost}")

    # For non-inventory and service items, set base costs
    for item in non_inventory_items + service_items:
        item.unit_cost = float(item.unit_price or 0)
        item.total_cost = float(item.total_price or 0)

    db_session.flush()
    return True


# Replace the simple calculate_allocations call with proper landed cost calculation
def calculate_goods_receipt_landed_costs(db_session, goods_receipt, purchase_record, receipt_items, app_id):
    """
    Calculate true landed costs for goods receipt items based on received quantities
    Separate shipping and handling costs for tracking while excluding them from landed cost
    """
    from decimal import Decimal, ROUND_HALF_UP

    # Get overall purchase values
    total_line_subtotal = Decimal(str(purchase_record.total_line_subtotal or 0))
    calculated_discount_amount = Decimal(str(purchase_record.calculated_discount_amount or 0))
    total_tax_amount = Decimal(str(purchase_record.total_tax_amount or 0))
    shipping_cost = Decimal(str(getattr(purchase_record, 'shipping_cost', 0) or 0))
    handling_cost = Decimal(str(getattr(purchase_record, 'handling_cost', 0) or 0))

    # Get all items from the purchase record
    if hasattr(purchase_record, 'purchase_order_items'):
        all_items = purchase_record.purchase_order_items
    else:
        all_items = purchase_record.direct_purchase_items

    # Calculate line item tax total
    total_line_item_tax = sum(
        Decimal(str(item.tax_amount or 0))
        for item in all_items
        if item.tax_amount is not None
    )

    # Calculate purchase order level tax (tax applied at PO level, not line level)
    overall_purchase_tax = total_tax_amount - total_line_item_tax

    # Calculate proportions based on ALL items (not just inventory)
    total_all_items_value = total_line_subtotal

    # Calculate category totals from original purchase
    inventory_items = [item for item in all_items if item.item_type == "inventory"]
    non_inventory_items = [item for item in all_items if item.item_type in ("non_inventory", "non-inventory")]
    service_items = [item for item in all_items if item.item_type == "service"]

    inventory_total = sum(Decimal(str(item.total_price or 0)) for item in inventory_items)
    non_inventory_total = sum(Decimal(str(item.total_price or 0)) for item in non_inventory_items)
    service_total = sum(Decimal(str(item.total_price or 0)) for item in service_items)

    # Calculate proportions for ALL categories
    inventory_proportion = inventory_total / total_all_items_value if total_all_items_value > 0 else Decimal(0)
    non_inventory_proportion = non_inventory_total / total_all_items_value if total_all_items_value > 0 else Decimal(0)
    service_proportion = service_total / total_all_items_value if total_all_items_value > 0 else Decimal(0)

    # Calculate category shares of discount and tax for ALL categories
    inventory_discount_share = inventory_proportion * calculated_discount_amount
    inventory_tax_share = inventory_proportion * overall_purchase_tax

    non_inventory_discount_share = non_inventory_proportion * calculated_discount_amount
    non_inventory_tax_share = non_inventory_proportion * overall_purchase_tax

    service_discount_share = service_proportion * calculated_discount_amount
    service_tax_share = service_proportion * overall_purchase_tax

    # Calculate category shares of shipping and handling for tracking (but not for landed cost)
    inventory_shipping_handling_share = inventory_proportion * (shipping_cost + handling_cost)
    non_inventory_shipping_handling_share = non_inventory_proportion * (shipping_cost + handling_cost)
    service_shipping_handling_share = service_proportion * (shipping_cost + handling_cost)

    # Now calculate for received quantities only
    allocations = []

    for receipt_item in receipt_items:
        # Find the corresponding original item
        original_item = None
        for item in all_items:
            if (hasattr(purchase_record, 'purchase_order_items') and receipt_item.purchase_order_item_id == item.id) or \
                    (hasattr(purchase_record,
                             'direct_purchase_items') and receipt_item.direct_purchase_item_id == item.id):
                original_item = item
                break

        if not original_item:
            continue

        # Calculate received quantity proportion
        received_proportion = Decimal(str(receipt_item.quantity_received)) / Decimal(str(original_item.quantity))

        # Calculate this item's proportion of total purchase value
        item_proportion_total = Decimal(
            str(original_item.total_price)) / total_all_items_value if total_all_items_value > 0 else Decimal(0)

        if original_item.item_type == "inventory" and inventory_total > 0:
            # Calculate this item's proportion of total inventory
            item_proportion_category = Decimal(str(original_item.total_price)) / inventory_total

            # Calculate allocated shipping + handling for tracking
            allocated_shipping_handling = item_proportion_total * (shipping_cost + handling_cost) * received_proportion

            # Store combined shipping and handling
            receipt_item.allocated_shipping_handling = float(
                allocated_shipping_handling.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate adjusted total cost for RECEIVED quantity (EXCLUDING shipping and handling)
            item_adjusted_total_received = (
                    (Decimal(str(original_item.total_price)) * received_proportion) -
                    (item_proportion_total * calculated_discount_amount * received_proportion) +
                    (item_proportion_total * overall_purchase_tax * received_proportion)
            )

            # Calculate unit cost (excluding shipping and handling)
            if receipt_item.quantity_received > 0:
                unit_cost = item_adjusted_total_received / Decimal(str(receipt_item.quantity_received))
            else:
                unit_cost = Decimal(str(original_item.unit_price or 0))

            # Update the receipt item with calculated costs
            receipt_item.unit_cost = float(unit_cost.quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP))
            receipt_item.total_cost = float(
                item_adjusted_total_received.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate allocated tax (line item tax + allocated PO level tax)
            line_item_tax = Decimal(str(original_item.tax_amount or 0)) * received_proportion
            po_level_tax = item_proportion_total * overall_purchase_tax * received_proportion
            total_allocated_tax = po_level_tax

            allocations.append({
                'item_id': original_item.id,
                'allocated_amount': float(item_adjusted_total_received),
                'allocated_tax': float(total_allocated_tax),
                'allocated_discount': 0.0,
                'allocated_shipping_handling': float(allocated_shipping_handling)
            })

        elif original_item.item_type in ("non_inventory", "non-inventory") and non_inventory_total > 0:
            # Calculate this item's proportion of total non-inventory
            item_proportion_category = Decimal(str(original_item.total_price)) / non_inventory_total

            # Calculate allocated shipping + handling for tracking
            allocated_shipping_handling = item_proportion_total * (shipping_cost + handling_cost) * received_proportion

            receipt_item.allocated_shipping_handling = float(
                allocated_shipping_handling.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate adjusted total cost for RECEIVED quantity (EXCLUDING shipping and handling)
            item_adjusted_total_received = (
                    (Decimal(str(original_item.total_price)) * received_proportion) -
                    (item_proportion_total * calculated_discount_amount * received_proportion) +
                    (item_proportion_total * overall_purchase_tax * received_proportion)
            )

            # Calculate unit cost
            if receipt_item.quantity_received > 0:
                unit_cost = item_adjusted_total_received / Decimal(str(receipt_item.quantity_received))
            else:
                unit_cost = Decimal(str(original_item.unit_price or 0))

            # Update the receipt item with calculated costs
            receipt_item.unit_cost = float(unit_cost.quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP))
            receipt_item.total_cost = float(
                item_adjusted_total_received.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate allocated tax
            line_item_tax = Decimal(str(original_item.tax_amount or 0)) * received_proportion
            po_level_tax = item_proportion_total * overall_purchase_tax * received_proportion
            total_allocated_tax = po_level_tax

            allocations.append({
                'item_id': original_item.id,
                'allocated_amount': float(item_adjusted_total_received),
                'allocated_tax': float(total_allocated_tax),
                'allocated_discount': 0.0,
                'allocated_shipping_handling': float(allocated_shipping_handling)
            })

        elif original_item.item_type == "service" and service_total > 0:
            # Calculate this item's proportion of total services
            item_proportion_category = Decimal(str(original_item.total_price)) / service_total

            # Calculate allocated shipping + handling for tracking
            allocated_shipping_handling = item_proportion_total * (shipping_cost + handling_cost) * received_proportion

            receipt_item.allocated_shipping_handling = float(
                allocated_shipping_handling.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate adjusted total cost for RECEIVED quantity (EXCLUDING shipping and handling)
            item_adjusted_total_received = (
                    (Decimal(str(original_item.total_price)) * received_proportion) -
                    (item_proportion_total * calculated_discount_amount * received_proportion) +
                    (item_proportion_total * overall_purchase_tax * received_proportion)
            )

            # For services, quantity is typically 1, but handle anyway
            if receipt_item.quantity_received > 0:
                unit_cost = item_adjusted_total_received / Decimal(str(receipt_item.quantity_received))
            else:
                unit_cost = Decimal(str(original_item.unit_price or 0))

            # Update the receipt item with calculated costs
            receipt_item.unit_cost = float(unit_cost.quantize(Decimal('0.0000'), rounding=ROUND_HALF_UP))
            receipt_item.total_cost = float(
                item_adjusted_total_received.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))

            # Calculate allocated tax
            line_item_tax = Decimal(str(original_item.tax_amount or 0)) * received_proportion
            po_level_tax = item_proportion_total * overall_purchase_tax * received_proportion
            total_allocated_tax = po_level_tax

            allocations.append({
                'item_id': original_item.id,
                'allocated_amount': float(item_adjusted_total_received),
                'allocated_tax': float(total_allocated_tax),
                'allocated_discount': 0.0,
                'allocated_shipping_handling': float(allocated_shipping_handling)
            })

        else:
            # Fallback for items that don't fit any category
            received_proportion = Decimal(str(receipt_item.quantity_received)) / Decimal(str(original_item.quantity))
            allocated_amount = Decimal(str(original_item.total_price or 0)) * received_proportion

            # Calculate allocated tax for fallback items
            line_item_tax = Decimal(str(original_item.tax_amount or 0)) * received_proportion
            if total_all_items_value > 0:
                item_proportion = Decimal(str(original_item.total_price or 0)) / total_all_items_value
                po_level_tax = item_proportion * overall_purchase_tax * received_proportion
                allocated_shipping_handling = item_proportion * (shipping_cost + handling_cost) * received_proportion
            else:
                po_level_tax = Decimal(0)
                allocated_shipping_handling = Decimal(0)

            total_allocated_tax = po_level_tax

            receipt_item.allocated_shipping_handling = float(
                allocated_shipping_handling.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP))
            receipt_item.unit_cost = float(Decimal(str(original_item.unit_price or 0)))
            receipt_item.total_cost = float(allocated_amount)

            allocations.append({
                'item_id': original_item.id,
                'allocated_amount': float(allocated_amount),
                'allocated_tax': float(total_allocated_tax),
                'allocated_discount': 0.0,
                'allocated_shipping_handling': float(allocated_shipping_handling)
            })

    return allocations


def get_inventory_entries_for_purchase_order(db_session, purchase_order_id, app_id):
    """
    Fetch all inventory entries related to a specific purchase order
    """
    return db_session.query(InventoryEntry).filter_by(
        source_type="purchase",
        inventory_source="purchase_order",
        source_id=purchase_order_id,
        app_id=app_id
    ).options(
        joinedload(InventoryEntry.line_items).joinedload(InventoryEntryLineItem.inventory_transaction_details)
    ).all()


def validate_prepaid_account_consistency(purchase_order_id, new_prepaid_account_id):
    """Check if previous advance payments used different prepaid accounts"""
    existing_advance_payments = session.query(PurchaseTransaction) \
        .join(PurchasePaymentAllocation) \
        .filter(
        PurchaseTransaction.purchase_order_id == purchase_order_id,
        PurchaseTransaction.payment_type == 'advance_payment',
        PurchasePaymentAllocation.prepaid_account_id != new_prepaid_account_id
    ) \
        .all()

    if existing_advance_payments:
        used_accounts = set([alloc.prepaid_account_id for payment in existing_advance_payments
                             for alloc in payment.payment_allocations])
        warning_message = f"Warning: Previous advance payments used different prepaid accounts: {used_accounts}"
        return False, warning_message

    return True, "Account consistent with previous payments"


def check_prepaid_account_consistency(db_session, purchase_order_id, new_prepaid_account_id):
    """
    Enhanced consistency check that includes journal entry information
    """
    # Check existing payment allocations
    existing_allocations = db_session.query(PurchasePaymentAllocation) \
        .join(PurchaseTransaction) \
        .filter(
        PurchaseTransaction.purchase_order_id == purchase_order_id,
        PurchasePaymentAllocation.payment_type == 'advance_payment',
        PurchasePaymentAllocation.prepaid_account_id is not None,
        PurchasePaymentAllocation.prepaid_account_id != new_prepaid_account_id
    ) \
        .all()

    source_ids = [alloc.id for alloc in existing_allocations]
    logger.info(f'Source IDs are {source_ids}')

    if not source_ids:
        logger.info("No existing allocations found, skipping journal update check.")
        journals_to_update = []
    else:
        journals_to_update = (
            db_session.query(JournalEntry)
            .join(Journal, Journal.id == JournalEntry.journal_id)
            .join(PurchasePaymentAllocation, PurchasePaymentAllocation.id == JournalEntry.source_id)
            .join(PurchaseTransaction, PurchaseTransaction.id == PurchasePaymentAllocation.payment_id)
            .filter(
                JournalEntry.source_type == 'purchase_order_payment',
                PurchaseTransaction.purchase_order_id == purchase_order_id,
                PurchasePaymentAllocation.payment_type == 'advance_payment',
                JournalEntry.dr_cr == 'D',
                JournalEntry.subcategory_id != new_prepaid_account_id,
                JournalEntry.source_id.in_(source_ids)
            )
            .all()
        )

    # Get account names for better user information
    existing_accounts_info = []
    account_ids = list(set([alloc.prepaid_account_id for alloc in existing_allocations]))

    for account_id in account_ids:
        account = db_session.query(ChartOfAccounts).filter_by(id=account_id).first()
        payment_count = len([alloc for alloc in existing_allocations if alloc.prepaid_account_id == account_id])

        existing_accounts_info.append({
            'account_id': account_id,
            'account_name': account.sub_category if account else f"Account {account_id}",
            'payment_count': payment_count
        })
    logger.info(f'Journals to update are {journals_to_update}')

    return {
        'warning_required': len(existing_allocations) > 0,
        'existing_payments_count': len(existing_allocations),
        'journals_to_update_count': len(journals_to_update),
        'existing_accounts_info': existing_accounts_info,
        'new_account_id': new_prepaid_account_id
    }
