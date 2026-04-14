# app/services/sales_helpers.py

import logging
import math
import re
import traceback
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from flask import jsonify
from flask_login import current_user
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from ai import get_base_currency, get_exchange_rate
from db import Session
from models import PayrollPeriod, Employee, PayrollTransaction, PayRollStatusEnum, AdvancePayment, ChartOfAccounts, \
    Category, OpeningBalance, DirectSalesTransaction, PaymentAllocation, DirectSaleItem, SalesInvoice, InventoryEntry, \
    InventoryEntryLineItem, InventoryTransactionDetail, SalesTransaction, Quotation, SalesOrder, DeliveryNote, \
    DeliveryReferenceNumber, ExchangeRate, InvoiceStatus, PaymentReceipt, SalesPaymentStatus, Currency, CustomerCredit, \
    BulkPayment
from services.inventory_helpers import update_inventory_summary
from services.post_to_ledger_reversal import reverse_sales_transaction_posting
from services.vendors_and_customers import get_or_create_customer_credit_account
from utils import convert_to_base_currency, create_transaction, generate_unique_journal_number
from utils_and_helpers.exchange_rates import get_or_create_exchange_rate_for_transaction
from utils_and_helpers.numbers import generate_sequence_number

logger = logging.getLogger(__name__)


# Define your wrapper functions
def generate_direct_sale_number(db_session=None, app_id=None):
    """Generate direct sale number"""
    return generate_sequence_number(
        prefix="SALE",
        table_model=DirectSalesTransaction,
        number_field="direct_sale_number",
        db_session=db_session,
        app_id=app_id
    )


def suggest_next_direct_sale_reference(db_session: Session):
    """
    Suggest the next direct sale reference based on the most recent reference entered by the user.
    Excludes POS-generated references (those starting with 'pos_').

    Rules:
    - If there is no previous reference, return format: SALE-REF-1
    - If the last reference ends with a number, increment it while preserving leading zeros.
    - Otherwise, append '-1' to the last reference.
    """
    app_id = current_user.app_id  # Assumes current_user is available

    # Get the last entered direct sale reference (excluding POS)
    last_entry = (
        db_session.query(DirectSalesTransaction)
        .filter(
            DirectSalesTransaction.app_id == app_id,
            DirectSalesTransaction.sale_reference.isnot(None),
            ~DirectSalesTransaction.sale_reference.ilike("pos_%")  # Exclude POS references
        )
        .order_by(DirectSalesTransaction.id.desc())
        .first()
    )

    # If no previous reference exists, use base format
    if not last_entry or not last_entry.sale_reference:
        return "SALE-REF-1"

    last_ref = last_entry.sale_reference.strip()

    # Check if the reference ends with a number
    match = re.search(r'(\d+)$', last_ref)
    if match:
        number_str = match.group(1)
        number_len = len(number_str)
        number = int(number_str) + 1
        new_ref = last_ref[:match.start(1)] + str(number).zfill(number_len)
    else:
        new_ref = f"{last_ref}-1"

    return new_ref


def allocate_direct_sale_payment(
        direct_sale_id: int,
        payment_amount: Decimal,
        db_session: Session,
        payment_mode: int,
        payment_date,
        total_tax_amount: Decimal,
        payment_account: int,
        tax_payable_account_id: int,
        credit_sale_account: int,
        reference: Optional[str] = None,
        is_pos: Optional[bool] = False,
        is_posted_to_ledger: Optional[bool] = False,
        exchange_rate_id: int = None

) -> PaymentAllocation:
    """
    Allocates a payment of a direct sale transaction proportionally between the base amount and tax amount.
    Considers both line-item taxes and the overall transaction tax.

    Args:

        payment_amount (Decimal): The amount being paid.
        db_session (Session): The database session.
        payment_mode (int): The payment mode ID.
        total_tax_amount (Decimal): The total tax amount for the transaction.
        payment_account (int): The payment account ID.
        tax_payable_account_id (int): The tax payable account ID.
        credit_sale_account (int): The credit sale account ID.
        reference (str, optional): A reference for the payment. Defaults to None.
        is_pos (bool, optional): Used for POS transactions
        is_posted_to_ledger (bool, optional): Ledger status
        exchange_rate_id (int): Exchange rate id

    Returns:
        PaymentAllocation: The newly created payment allocation record.
    """
    from ai import resolve_exchange_rate_for_transaction

    direct_sale = db_session.query(DirectSalesTransaction).filter_by(id=direct_sale_id).first()
    # Fetch all line items for the transaction
    line_items = db_session.query(DirectSaleItem).filter_by(transaction_id=direct_sale_id).all()

    # Convert payment amount to Decimal (if not already)
    payment_amount = Decimal(payment_amount)

    total_tax_amount = Decimal(total_tax_amount)

    # Calculate proportional allocation
    allocated_tax_amount = (total_tax_amount / Decimal(direct_sale.total_amount)) * payment_amount
    allocated_base_amount = payment_amount - allocated_tax_amount  # The rest goes to the base

    # Round to 2 decimal places
    allocated_tax_amount = allocated_tax_amount.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
    allocated_base_amount = allocated_base_amount.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)

    currency_id = direct_sale.currency_id

    # exchange_rate_id, notification = resolve_exchange_rate_for_transaction(session=db_session,
    #                                                                        currency_id=currency_id,
    #                                                                        transaction_date=payment_date,
    #                                                                        app_id=current_user.app_id)

    # Create payment allocation entry
    payment_allocation = PaymentAllocation(
        payment_id=None,  # Assuming payment is linked to invoice
        direct_sale_id=direct_sale_id,
        payment_date=payment_date,
        invoice_id=None,
        allocated_base_amount=allocated_base_amount,
        allocated_tax_amount=allocated_tax_amount,
        payment_account=payment_account,
        tax_payable_account_id=tax_payable_account_id,
        credit_sale_account=credit_sale_account,
        created_at=datetime.now(),
        updated_at=datetime.now(),
        payment_mode=payment_mode,
        app_id=direct_sale.app_id,
        exchange_rate_id=exchange_rate_id,
        reference=reference,  # Use the reference parameter (defaults to None if not provided)
        is_posted_to_ledger=is_pos or is_posted_to_ledger
    )

    db_session.add(payment_allocation)
    db_session.commit()

    return payment_allocation


def suggest_next_invoice_reference(db_session: Session):
    """
    Suggest the next invoice reference based on the most recent reference entered by the user.
    - If there is no previous reference, return format: INV-{month_year}-00001
    - If the last reference ends with a number, increment it while preserving leading zeros.
    - Otherwise, return the last reference with '-1' appended.
    """
    app_id = current_user.app_id  # Assumes current_user is available

    # Get the last entered invoice reference
    last_entry = db_session.query(SalesInvoice.invoice_reference).filter(
        SalesInvoice.app_id == app_id,
        SalesInvoice.invoice_reference.isnot(None)
    ).order_by(SalesInvoice.id.desc()).first()

    # If no previous reference exists, use the special format
    if not last_entry or not last_entry.invoice_reference:
        now = datetime.now()
        month_year = now.strftime("%m%y")  # e.g., "0925" for Sept 2025
        return f"INV-REF-1"

    last_ref = last_entry.invoice_reference.strip()

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


def generate_next_invoice_number(db_session=None, app_id=None):
    """Fixed: Generate invoice number"""
    return generate_sequence_number(
        prefix="INV",
        table_model=SalesInvoice,
        number_field="invoice_number",
        db_session=db_session,  # ← ADDED
        app_id=app_id  # ← ADDED
    )


def get_inventory_entries_for_invoice(db_session, invoice_id, app_id):
    """
    Fetch all inventory entries related to a specific sales invoice
    """
    return db_session.query(InventoryEntry).filter_by(
        source_type="sale",
        inventory_source="sales_invoice",
        source_id=invoice_id,
        app_id=app_id
    ).options(
        joinedload(InventoryEntry.line_items).joinedload(InventoryEntryLineItem.inventory_transaction_details)
    ).all()


def get_inventory_entries_for_direct_sale(db_session, direct_sale_id, app_id):
    """
    Fetch all inventory entries related to a specific sales transaction (both direct_sale and pos_sale)
    """
    return db_session.query(InventoryEntry).filter(
        InventoryEntry.source_type == "sale",
        InventoryEntry.source_id == direct_sale_id,
        InventoryEntry.app_id == app_id,
        InventoryEntry.inventory_source.in_(["direct_sale", "pos_sale"])  # Look for both types
    ).options(
        joinedload(InventoryEntry.line_items).joinedload(InventoryEntryLineItem.inventory_transaction_details)
    ).all()


def reverse_sales_inventory_entries(db_session, inventory_entry):
    try:
        total_reversed_cost = 0
        items_to_reverse = []

        # Process in smaller batches to avoid session overload
        batch_size = 20
        line_items = list(inventory_entry.line_items)

        for i in range(0, len(line_items), batch_size):
            batch = line_items[i:i + batch_size]

            for line_item in batch:
                transaction_details = db_session.query(InventoryTransactionDetail).filter_by(
                    inventory_entry_line_item_id=line_item.id
                ).all()

                for transaction in transaction_details:
                    total_reversed_cost += transaction.total_cost
                    items_to_reverse.append({
                        'app_id': transaction.app_id,
                        'item_id': transaction.item_id,
                        'item_name': line_item.inventory_item_variation_link.inventory_item.item_name,
                        'location_id': transaction.location_id,
                        'quantity': -transaction.quantity,
                        'total_cost': -transaction.total_cost
                    })

                    # Delete each transaction individually
                    db_session.delete(transaction)

                # Delete line item
                db_session.delete(line_item)

            # Flush after each batch
            db_session.flush()

        # Delete main inventory entry
        db_session.delete(inventory_entry)
        db_session.flush()

        # Process reversals
        for item in items_to_reverse:
            update_inventory_summary(
                db_session,
                item['app_id'],
                item['item_id'],
                item['location_id'],
                item['quantity'],
                item['total_cost'],
                item_name=item.get('item_name')
            )

        return True

    except Exception as e:
        logger.error(f"Error reversing inventory entry {inventory_entry.id}: {str(e)}")
        db_session.rollback()
        raise


def allocate_payment(
        sale_transaction_id: int,
        invoice_id: int,
        payment_date,
        payment_amount: Decimal,
        remaining_balance: Decimal,  # NEW: Receive remaining balance to avoid duplicate calculation
        db_session: Session,
        payment_mode: int,
        total_tax_amount: Decimal,
        payment_account: int,
        tax_payable_account_id: int,
        credit_sale_account: None,
        reference: Optional[str] = None,
        write_off_account_id: Optional[int] = None,
        overpayment_amount: Decimal = Decimal('0.00'),
        payment_type=None,
        is_posted_to_ledger=None,
        exchange_rate_id=None,
        base_currency_id=None,
        invoice=None
) -> PaymentAllocation:
    """
    Allocates a payment to an invoice with proper tax splitting.

    ===== CRITICAL: PAYMENT AMOUNT VS OVERPAYMENT =====
    This function expects:

    payment_amount = The ACTUAL amount received in PAYMENT CURRENCY
                     (BEFORE any overpayment deduction)

    overpayment_amount = The portion of payment_amount that EXCEEDS the invoice
                        (will be subtracted internally)

    EFFECTIVE PAYMENT = payment_amount - overpayment_amount
                      (This is what actually gets applied to the invoice)

    ===== EXAMPLE =====
    Invoice total: 19,800 (tax-inclusive)
    Customer pays: 20,000

    CORRECT call:
        payment_amount = 20000     # Actual amount received
        overpayment_amount = 200   # Excess amount

    Function calculates:
        effective_payment = 20000 - 200 = 19800
        tax_ratio = invoice.total_tax_amount / invoice.total_amount
        allocated_tax = 19800 * tax_ratio      # 1800
        allocated_base = 19800 * (1 - tax_ratio)  # 18000

    ===== WRONG USAGE =====
    payment_amount = 19800     # ❌ Already reduced amount
    overpayment_amount = 200   # This would give: 19800 - 200 = 19600 applied

    ===== TAX HANDLING =====
    For tax-inclusive invoices (where total_amount includes tax):
        - tax_ratio = invoice.total_tax_amount / invoice.total_amount
        - Both allocated_base and allocated_tax come from the same payment

    For tax-exclusive invoices (where tax is added on top):
        - This function assumes tax-inclusive. For exclusive, modify calculation.

    ===== ROUNDING =====
    Uses ROUND_HALF_UP for tax allocation, then adjusts base amount
    to ensure total matches effective_payment exactly.
    """

    from ai import resolve_exchange_rate_for_transaction

    # Validate payment amount
    if payment_amount <= 0:
        raise ValueError("Payment amount must be greater than 0.")

    # Validate invoice total amount
    if invoice.total_amount == 0:
        raise ValueError("Invoice total amount cannot be zero.")

    # Convert payment amount to Decimal
    payment_amount = Decimal(payment_amount)
    effective_payment_amount = payment_amount - overpayment_amount

    # Calculate allocation based on invoice proportions
    tax_ratio = invoice.total_tax_amount / invoice.total_amount if invoice.total_amount > 0 else Decimal('0.00')
    base_ratio = Decimal('1.00') - tax_ratio

    # Allocate the effective payment amount
    allocated_tax_amount = (effective_payment_amount * tax_ratio).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
    allocated_base_amount = (effective_payment_amount * base_ratio).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)

    # Handle rounding discrepancies
    total_allocated = allocated_tax_amount + allocated_base_amount
    if total_allocated != effective_payment_amount:
        # Adjust base amount to account for rounding difference
        allocated_base_amount = effective_payment_amount - allocated_tax_amount

    currency_id = invoice.currency

    # Create payment allocation entry
    payment_allocation = PaymentAllocation(
        payment_id=sale_transaction_id,
        payment_date=payment_date,
        invoice_id=invoice_id,
        allocated_base_amount=allocated_base_amount,
        allocated_tax_amount=allocated_tax_amount,
        overpayment_amount=overpayment_amount,
        payment_account=payment_account,
        tax_payable_account_id=tax_payable_account_id,
        credit_sale_account=credit_sale_account,
        write_off_account_id=write_off_account_id,
        created_at=datetime.now(),
        updated_at=datetime.now(),
        payment_mode=payment_mode,
        app_id=invoice.app_id,
        exchange_rate_id=exchange_rate_id,
        reference=reference,
        payment_type=payment_type,
        is_posted_to_ledger=bool(is_posted_to_ledger)
    )

    db_session.add(payment_allocation)

    return payment_allocation


def suggest_next_quotation_reference(db_session: Session):
    """
    Suggest the next quotation reference based on the most recent reference entered by the user.
    - If there is no previous reference, return format: QUOTE-REF-1
    - If the last reference ends with a number, increment it while preserving leading zeros.
    - Otherwise, return the last reference with '-1' appended.
    """
    app_id = current_user.app_id  # Assumes current_user is available

    # Get the last entered quotation reference
    last_entry = db_session.query(Quotation.quotation_reference).filter(
        Quotation.app_id == app_id,
        Quotation.quotation_reference.isnot(None)
    ).order_by(Quotation.id.desc()).first()

    # If no previous reference exists, use the special format
    if not last_entry or not last_entry.quotation_reference:
        return f"QUOTE-REF-1"

    last_ref = last_entry.quotation_reference.strip()

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


def generate_next_quotation_number():
    """Generate quotation number with company initial"""
    with Session() as db_session:
        app_id = current_user.app_id
        company_name = current_user.company.name if current_user.company else ""

        # Get company initial
        if company_name and company_name[0].isalpha():
            company_initial = company_name[0].upper()
        else:
            company_initial = "X"

        prefix = f"{company_initial}Q"
        month_year = datetime.now().strftime("%m%y")

        # Get maximum number for current month
        max_number = db_session.query(
            func.max(Quotation.quotation_number)
        ).filter(
            Quotation.app_id == app_id,
            Quotation.quotation_number.isnot(None),
            Quotation.quotation_number.like(f"{prefix}-{month_year}-%")
        ).scalar()

        if max_number:
            next_number = int(max_number.split('-')[-1]) + 1
        else:
            next_number = 1

        return f"{prefix}-{month_year}-{str(next_number).zfill(5)}"


def generate_next_payment_receipt_number(db_session=None, app_id=None):
    return generate_sequence_number(
        prefix="REC",
        table_model=PaymentReceipt,
        number_field="payment_receipt_number",
        db_session=db_session,  # ← ADD THIS
        app_id=app_id  # ← ADD THIS
    )


def generate_next_sales_order_number(db_session=None, app_id=None):
    """Generate sales order number: SO-MMYY-XXXXX"""
    return generate_sequence_number(
        prefix="SO",
        table_model=SalesOrder,
        number_field="sales_order_number",
        db_session=db_session,  # ← ADD THIS
        app_id=app_id  # ← ADD THIS
    )


def suggest_next_sales_order_reference(db_session: Session):
    """
    Suggest the next sales order reference based on the most recent reference entered by the user.
    - If there is no previous reference, return format: SO-REF-1
    - If the last reference ends with a number, increment it while preserving leading zeros.
    - Otherwise, return the last reference with '-1' appended.
    """
    app_id = current_user.app_id  # Assumes current_user is available

    # Get the last entered sales order reference
    last_entry = db_session.query(SalesOrder.sales_order_reference).filter(
        SalesOrder.app_id == app_id,
        SalesOrder.sales_order_reference.isnot(None)
    ).order_by(SalesOrder.id.desc()).first()

    # If no previous reference exists, use the special format
    if not last_entry or not last_entry.sales_order_reference:
        return f"SO-REF-1"

    last_ref = last_entry.sales_order_reference.strip()

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


def generate_next_delivery_note_number(db_session, app_id):
    """Generate Delivery Note number: DN-MMYY-XXXXX"""
    return generate_sequence_number(
        prefix="DN",
        table_model=DeliveryNote,
        number_field="delivery_number",
        db_session=db_session,  # ← ADD THIS
        app_id=app_id  # ← ADD THIS
    )


def generate_next_delivery_reference_number(db_session, app_id):
    """Generate Delivery Reference number: DR-MMYY-XXXXX"""
    return generate_sequence_number(
        prefix="DR",
        table_model=DeliveryReferenceNumber,
        number_field="delivery_reference_number",
        db_session=db_session,  # ← ADD THIS
        app_id=app_id  # ← ADD THIS
    )


def update_transaction_exchange_rate(
        db_session,
        transaction,
        currency_id,
        base_currency_id,
        exchange_rate,
        transaction_date,
        app_id,
        user_id,
        source_type
):
    """
    Handle exchange rate updates for existing transactions

    Args:
        db_session: Database session
        transaction: The transaction object being updated (can be DirectSalesTransaction, SalesInvoice, etc.)
        currency_id: Transaction currency ID
        base_currency_id: Base currency ID
        exchange_rate: Exchange rate value from form (string)
        transaction_date: Date of transaction
        app_id: Application/Company ID
        user_id: Current user ID
        source_type: Type of source ('sale', 'invoice', 'purchase', 'journal', 'expense')

    Returns:
        tuple: (exchange_rate_id, error_response)
        error_response is None if successful, otherwise a JSON response dict
    """
    # If currencies are the same, no rate needed

    if currency_id == base_currency_id:
        return None, None

    # Validate exchange rate
    if not exchange_rate or exchange_rate.strip() == '':
        return None, jsonify({
            'success': False,
            'message': 'Exchange rate is required for foreign currency transactions'
        })

    try:
        exchange_rate_value = float(exchange_rate)
        if exchange_rate_value <= 0:
            return None, jsonify({
                'success': False,
                'message': 'Exchange rate must be greater than 0'
            })
    except ValueError:

        return None, jsonify({
            'success': False,
            'message': 'Exchange rate is required for foreign currency transactions'
        })

    # Find existing exchange rate
    existing_rate = None

    # Check if transaction has direct exchange_rate attribute (like SalesInvoice)
    if hasattr(transaction, 'exchange_rate') and transaction.exchange_rate:
        existing_rate = transaction.exchange_rate
    # Check through payment allocations (for DirectSalesTransaction)
    elif hasattr(transaction, 'payment_allocations') and transaction.payment_allocations:
        allocation = transaction.payment_allocations[0]
        if allocation.exchange_rate_id:
            existing_rate = allocation.exchange_rate

    # If not found, try direct query as fallback
    if not existing_rate:
        existing_rate = db_session.query(ExchangeRate).filter(
            ExchangeRate.source_type == source_type,
            ExchangeRate.source_id == transaction.id
        ).first()

    if existing_rate:
        # Update existing rate
        existing_rate.from_currency_id = currency_id
        existing_rate.to_currency_id = base_currency_id
        existing_rate.rate = exchange_rate_value
        existing_rate.date = transaction_date
        existing_rate.created_by = user_id
        db_session.add(existing_rate)
        return existing_rate.id, None
    else:
        # Create new rate
        rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
            session=db_session,
            action='create',
            from_currency_id=currency_id,
            to_currency_id=base_currency_id,
            rate_value=exchange_rate_value,
            rate_date=transaction_date,
            app_id=app_id,
            created_by=user_id,
            source_type=source_type,
            source_id=transaction.id,
            currency_exchange_transaction_id=None
        )
        return rate_id, None


def update_invoice_status(db_session, invoice_input):
    """
    Helper function to update invoice status after credit reversal

    Args:
        db_session: Database session
        invoice_input: Can be either invoice_id (int) or SalesInvoice object

    Returns:
        Updated invoice object
    """
    # Handle both invoice_id and invoice object
    if isinstance(invoice_input, int):
        invoice = db_session.query(SalesInvoice).get(invoice_input)
    else:
        invoice = invoice_input

    if not invoice:
        logger.error(f"Invoice not found for status update: {invoice_input}")
        return None

    # Calculate total paid in INVOICE CURRENCY from transactions
    total_paid_in_invoice_currency = db_session.query(func.sum(SalesTransaction.amount_paid)).filter(
        SalesTransaction.invoice_id == invoice.id,
        SalesTransaction.payment_status != SalesPaymentStatus.cancelled
    ).scalar() or Decimal('0.00')

    # Calculate percentage paid with tolerance
    if invoice.total_amount == 0:
        paid_percentage = 100
    else:
        paid_percentage = (total_paid_in_invoice_currency / invoice.total_amount) * 100

    # Status with tolerance (99.99% paid = fully paid)
    if paid_percentage >= 99.98:
        invoice.status = InvoiceStatus.paid
    elif total_paid_in_invoice_currency > 0:
        invoice.status = InvoiceStatus.partially_paid
    else:
        invoice.status = InvoiceStatus.unpaid
    # After calculating total_paid_in_invoice_currency
    logger.info(f"Invoice {invoice.id} - total_paid: {total_paid_in_invoice_currency}")
    logger.info(f"Invoice {invoice.id} - total_amount: {invoice.total_amount}")
    logger.info(f"Invoice {invoice.id} - paid_percentage: {paid_percentage}%")
    db_session.add(invoice)

    return invoice


def update_invoice_payment_status(db_session, invoice_id):
    """
    Update invoice payment status based on remaining payments after deletion
    Logic mirrors the payment recording function
    """
    invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id).first()
    if not invoice:
        return

    # Calculate total paid from remaining payments (excluding deleted ones)
    total_paid = db_session.query(func.sum(SalesTransaction.amount_paid)).filter(
        SalesTransaction.invoice_id == invoice_id,
        SalesTransaction.payment_status.in_(
            [SalesPaymentStatus.full, SalesPaymentStatus.partial, SalesPaymentStatus.paid]),
        SalesTransaction.payment_status != SalesPaymentStatus.refund,
        SalesTransaction.payment_status != SalesPaymentStatus.cancelled
    ).scalar() or Decimal('0.00')

    # Determine the payment status based on your payment recording logic
    if total_paid >= invoice.total_amount:
        payment_status = SalesPaymentStatus.paid
        invoice_status = InvoiceStatus.paid
    elif total_paid > 0:
        payment_status = SalesPaymentStatus.partial
        invoice_status = InvoiceStatus.partially_paid
    else:
        payment_status = SalesPaymentStatus.unpaid
        invoice_status = InvoiceStatus.unpaid

    # Update the invoice status
    invoice.status = invoice_status

    # Update payment status for all remaining transactions for this invoice
    remaining_transactions = db_session.query(SalesTransaction).filter(
        SalesTransaction.invoice_id == invoice_id,
        SalesTransaction.payment_status.in_(
            [SalesPaymentStatus.full, SalesPaymentStatus.partial, SalesPaymentStatus.paid]),
        SalesTransaction.payment_status != SalesPaymentStatus.refund,
        SalesTransaction.payment_status != SalesPaymentStatus.cancelled
    ).all()

    for transaction in remaining_transactions:
        transaction.payment_status = payment_status


def get_customer_invoices_with_balances(db_session, customer_id, app_id, base_currency_id, exclude_receipt_id=None):
    """
    Get all invoices for a customer with calculated balances.

    Args:
        db_session: Database session
        customer_id: Customer ID
        app_id: App ID
        base_currency_id: Base currency ID
        exclude_receipt_id: Optional receipt ID to exclude from paid calculations

    Returns:
        List of invoice dictionaries with calculated fields
    """
    # Base query for all customer invoices
    query = db_session.query(
        SalesInvoice.id,
        SalesInvoice.invoice_number,
        SalesInvoice.invoice_date,
        SalesInvoice.due_date,
        SalesInvoice.total_amount,
        SalesInvoice.currency,
        Currency.user_currency.label('currency_code'),
        ExchangeRate.rate.label('exchange_rate_value'),  # Get rate directly
        func.coalesce(func.sum(SalesTransaction.amount_paid), 0).label('total_paid')
    ).outerjoin(
        SalesTransaction,
        SalesTransaction.invoice_id == SalesInvoice.id
    ).join(
        Currency,
        Currency.id == SalesInvoice.currency
    ).outerjoin(  # Left join to exchange rate
        ExchangeRate,
        ExchangeRate.id == SalesInvoice.exchange_rate_id
    ).filter(
        SalesInvoice.app_id == app_id,
        SalesInvoice.customer_id == customer_id
    )

    # Exclude transactions from a specific receipt if provided
    if exclude_receipt_id:
        query = query.filter(
            (SalesTransaction.bulk_payment_id != exclude_receipt_id) |
            (SalesTransaction.bulk_payment_id.is_(None))
        )

    # Group by invoice fields
    invoices_data = query.group_by(
        SalesInvoice.id,
        SalesInvoice.invoice_number,
        SalesInvoice.invoice_date,
        SalesInvoice.due_date,
        SalesInvoice.total_amount,
        SalesInvoice.currency,
        Currency.user_currency,
        ExchangeRate.rate  # Include rate in group by
    ).all()

    # Process invoices to add calculated fields
    all_invoices = []
    for inv in invoices_data:
        # Calculate balance due (total - paid from ALL receipts except excluded)
        balance_due = float(inv.total_amount - (inv.total_paid or 0))

        # Calculate base currency equivalent using invoice's own exchange rate
        if inv.currency != base_currency_id:
            rate = float(inv.exchange_rate_value) if inv.exchange_rate_value else 1
            balance_due_base = balance_due * rate
        else:
            balance_due_base = balance_due

        all_invoices.append({
            'id': inv.id,
            'invoice_number': inv.invoice_number,
            'invoice_date': inv.invoice_date,
            'due_date': inv.due_date,
            'total_amount': float(inv.total_amount),
            'currency': inv.currency_code,
            'currency_id': inv.currency,
            'balance_due': balance_due,
            'balance_due_base': balance_due_base
        })

    # Sort by date (newest first)
    all_invoices.sort(key=lambda x: x['invoice_date'], reverse=True)

    return all_invoices


def manage_bulk_payment(db_session, source, action='edit', current_user=None, **kwargs):
    """
    ===========================================================================
    MANAGE BULK PAYMENT OPERATIONS
    ===========================================================================

    This function handles adjustments to EXISTING bulk payments where credits
    need to be created. For complete deletion of bulk payments, use
    bulk_delete_payments() instead.

    ACTIONS:
    ---------

    1. remove_transaction:
       - Removes a SINGLE SALES TRANSACTION from a bulk payment
       - The transaction was originally allocated from this bulk payment
       - Converts the removed amount to a customer credit
       - Uses suspense account for multi-currency bridging
       - Journal 1 (Invoice Currency): Dr AR, Cr Suspense
       - Journal 2 (Payment Currency): Dr Suspense, Cr Customer Credit
       - Updates invoice status (now partially/unpaid)
       - BULK PAYMENT TOTAL REMAINS UNCHANGED (money already received)

       Use cases:
       - Cancelling a specific transaction within a bulk payment
       - Removing an invoice from a payment during editing
       - Correcting allocation errors

    2. create_credit_from_unallocated:
       - Creates a credit from UNALLOCATED funds in a bulk payment
       - No transaction is removed (funds were never allocated)
       - Converts unallocated/excess funds to customer credit
       - Journal 1 (Payment Currency): Dr Cash, Cr Suspense
       - Journal 2 (Payment Currency): Dr Suspense, Cr Customer Credit
       - BULK PAYMENT TOTAL REMAINS UNCHANGED

       Use cases:
       - Converting overpayment to credit
       - Handling prepayments (no invoices yet)
       - Creating credit from unallocated amount during editing

    KEY PARAMETERS:
    ---------------
    - suspense_account_id: Required for both actions (multi-currency bridging)
    - transaction_id: Required for remove_transaction
    - amount: Required for create_credit_from_unallocated
    - reuse_existing_credit: If True, adds to existing credit instead of creating new

    IMPORTANT NOTES:
    ----------------
    - BULK PAYMENT TOTAL NEVER CHANGES in this function!
      The total represents money received and stays constant.
    - For complete deletion of a bulk payment, use bulk_delete_payments()
    - All journals have single currency (no mixing!)
    - Credit is always created in bulk payment currency
    """

    app_id = kwargs.get('app_id', current_user.app_id)
    reuse_existing = kwargs.get('reuse_existing_credit', False)

    try:
        logger.info(f'Re use is {reuse_existing}')

        # Helper function to find or create credit
        def get_or_create_credit(amount, reason, transaction_id=None):
            if reuse_existing:
                # Try to find existing active credit from this bulk payment
                existing = db_session.query(CustomerCredit).filter(
                    CustomerCredit.bulk_payment_id == source.id,
                    CustomerCredit.currency_id == source.currency_id,
                    CustomerCredit.customer_id == source.customer_id
                ).order_by(CustomerCredit.created_date.asc()).first()  # Oldest first

                if existing:
                    existing.original_amount += Decimal(str(amount))
                    existing.available_amount += Decimal(str(amount))

                    # Update status based on new available amount
                    if existing.available_amount <= 0:
                        existing.status = 'used'
                    else:
                        existing.status = 'active'

                    logger.info(f"Updated existing credit #{existing.id} (+{amount}, status: {existing.status})")
                    return existing, True
            # Create new credit
            credit = CustomerCredit(
                app_id=app_id,
                customer_id=source.customer_id,
                bulk_payment_id=source.id,
                original_amount=Decimal(str(amount)),
                available_amount=Decimal(str(amount)),
                currency_id=source.currency_id,
                exchange_rate_id=source.exchange_rate_id,
                created_date=datetime.now(),
                issued_date=datetime.now().date(),
                status='active',
                credit_reason=reason,
                reference_number=f"CREDIT-{transaction_id}" if transaction_id else None,
                created_by=current_user.id if current_user else None
            )
            db_session.add(credit)
            db_session.flush()
            logger.info(f"Created new credit #{credit.id} for {amount}")
            return credit, False  # False means created new

        # ===== REMOVE A SINGLE TRANSACTION FROM A BULK PAYMENT =====
        # Used when editing a receipt and removing an invoice from the payment
        # The removed amount becomes a customer credit that can be used later
        if action == 'remove_transaction':
            """
            ===========================================================================
            REMOVE TRANSACTION FROM BULK PAYMENT
            ===========================================================================
            
            WHAT THIS ACTION DOES:
            -----------------------
            Removes a single transaction from a bulk payment and converts it to a 
            customer credit. Used when editing a receipt and removing an invoice 
            from the payment allocation.
            
            WHEN TO USE:
            ------------
            - Editing a receipt and removing an invoice from payment
            - Customer requests to remove an invoice from a bulk payment
            - Correcting allocation errors
            - Cancelling a specific transaction within a bulk payment
            
            REQUIRED PARAMETERS:
            --------------------
            transaction_id       : ID of transaction to remove
            suspense_account_id  : ID of suspense account for multi-currency bridging
            
            OPTIONAL PARAMETERS:
            --------------------
            reason               : Reason for removal (default: 'transaction_removal')
            notes                : Additional notes for audit trail
            
            WHAT HAPPENS:
            -------------
            1. REVERSES LEDGER: reverse_sales_transaction_posting() deletes journal entries
            2. DELETES RECORDS: Removes PaymentAllocation and SalesTransaction
            3. UPDATES BULK PAYMENT: Reduces bulk_payment.total_amount
            4. CREATES CREDIT: CustomerCredit record in payment currency
            5. ADJUSTMENT ENTRIES: create_removal_adjustment_entry() creates TWO journals:
               
               Journal 1 (Invoice Currency):
                   Dr: Accounts Receivable (increase AR)
                   Cr: Suspense Account (move to suspense)
               
               Journal 2 (Payment Currency):
                   Dr: Suspense Account (take from suspense)
                   Cr: Customer Credit Account (create liability)
            
            6. UPDATES INVOICE: Recalculates invoice status (now partially/unpaid)
            
            WHY THIS APPROACH:
            ------------------
            - Uses suspense to bridge between invoice and payment currencies
            - Maintains single currency per journal
            - Preserves value across currency conversions
            - Provides complete audit trail
            
            EXAMPLE:
            --------
            Bulk payment: 500 EUR received
            Transaction removed: 50 USD invoice (originally allocated)
            
            Journal 1 (USD):
                Dr Accounts Receivable           50 USD
                   Cr Suspense                      50 USD
            
            Journal 2 (EUR):
                Dr Suspense                      41.67 EUR (at 1.2 USD/EUR)
                   Cr Customer Credit                 41.67 EUR
            
            Result: Customer gets 41.67 EUR credit, invoice becomes unpaid
            
            NOTES:
            ------
            - Credit created in bulk payment currency (EUR)
            - Invoice status reverts to unpaid/partial
            - Bulk payment total decreases
            - Can be used with post_customer_credit_to_invoice() later
            
            ===========================================================================
            """
            bulk_payment = source
            transaction_id = kwargs.get('transaction_id')
            suspense_account_id = kwargs.get('suspense_account_id')
            reason = kwargs.get('reason', 'transaction_removal')
            notes = kwargs.get('notes', '')

            if not transaction_id:
                raise ValueError("Transaction ID required")

            # Find the transaction
            transaction = db_session.query(SalesTransaction).filter_by(
                id=transaction_id,
                bulk_payment_id=bulk_payment.id
            ).first()

            if not transaction:
                raise ValueError(f"Transaction {transaction_id} not found in bulk payment")

            logger.info(f'Payment status is {transaction.payment_status}')
            if transaction.payment_status == SalesPaymentStatus.cancelled:
                raise ValueError("Transaction is already cancelled")

            stats = {
                'transaction_removed': transaction_id,
                'credit_created': None,
                'affected_invoices': {transaction.invoice_id}
            }

            # Get the amount in payment currency (this is what will become a credit)
            # Calculate amount in BULK PAYMENT CURRENCY (EUR)
            if transaction.currency_id == bulk_payment.currency_id:
                # Same currency - simple
                amount_in_payment_currency = float(transaction.amount_paid)
                amount_in_invoice_curr = amount_in_payment_currency
            else:
                # Different currencies - convert via base currency (UGX)

                # 1. Get rates (already available)
                invoice_rate = float(
                    transaction.invoice.exchange_rate.rate) if transaction.invoice and transaction.invoice.exchange_rate else 1
                bulk_rate = float(bulk_payment.exchange_rate.rate) if bulk_payment.exchange_rate else 1

                # 2. Convert: invoice amount → base currency (UGX) → payment currency (EUR)
                amount_in_base = float(transaction.amount_paid) * invoice_rate
                amount_in_invoice_curr = float(transaction.amount_paid)
                amount_in_payment_currency = amount_in_base / bulk_rate

                logger.info(f"Converted {transaction.amount_paid} {transaction.currency_id} → "
                            f"{amount_in_base} UGX → {amount_in_payment_currency} {bulk_payment.currency_id}")
            # Reverse ledger posting for this transaction
            try:
                reverse_sales_transaction_posting(db_session, transaction)
                logger.info(f"Reversed ledger for transaction {transaction_id}")
            except Exception as e:
                raise ValueError(f"Failed to reverse transaction ledger: {str(e)}")

            # Get the allocation BEFORE deleting transaction
            allocation = transaction.payment_allocations[0] if transaction.payment_allocations else None

            # In remove_transaction action, use the helper:
            credit, was_updated = get_or_create_credit(amount_in_payment_currency, reason, transaction_id)

            # Create adjustment journal entry (DR: AR, CR: Customer Credit)
            create_removal_adjustment_entry(
                db_session=db_session,
                bulk_payment=bulk_payment,
                transaction=transaction,
                allocation=allocation,
                amount_in_invoice=amount_in_invoice_curr,
                amount_in_payment=amount_in_payment_currency,
                credit=credit,
                current_user=current_user,
                suspense_account_id=suspense_account_id
            )

            # Update invoice status
            update_invoice_status(db_session, transaction.invoice_id)
            logger.info(f"Updated invoice {transaction.invoice_id} status")

            return True, f"Transaction removed and credit #{credit.id} created", stats

        # ===== CREATE CREDIT FROM UNALLOCATED AMOUNT =====
        # Used when editing a receipt and there's leftover unallocated amount
        # This converts the unallocated portion to a customer credit
        # ===== CREATE CREDIT FROM UNALLOCATED AMOUNT =====
        # ===== CREATE CREDIT FROM UNALLOCATED AMOUNT =====
        elif action == 'create_credit_from_unallocated':
            """
            ===========================================================================
            CREATE CREDIT FROM UNALLOCATED AMOUNT
            ===========================================================================
            
            WHAT THIS ACTION DOES:
            -----------------------
            Creates a customer credit from unallocated/excess funds in a bulk payment.
            Used when there's leftover amount that wasn't allocated to any invoice.
            
            WHEN TO USE:
            ------------
            - Editing a receipt and there's unallocated amount
            - Converting overpayment to customer credit
            - Handling prepayments (customer pays before invoices are created)
            - Cancelling a transaction (via cancel_sales_transaction route)
            
            REQUIRED PARAMETERS:
            --------------------
            amount              : Amount to convert to credit (in payment currency)
            suspense_account_id : ID of suspense account for multi-currency bridging
            
            OPTIONAL PARAMETERS:
            --------------------
            reason              : Reason for credit (default: 'adjustment')
            notes               : Additional notes for audit trail
            
            WHAT HAPPENS:
            -------------
            1. Creates a CustomerCredit record in payment currency
            2. Creates TWO journal entries through suspense:
            
               JOURNAL 1 (Payment Currency):
                   Dr: Cash/Payment Account     (decrease asset)
                   Cr: Suspense Account         (increase suspense)
                   → Moves funds from cash to suspense
               
               JOURNAL 2 (Payment Currency):
                   Dr: Suspense Account          (decrease suspense)
                   Cr: Customer Credit Account   (increase liability)
                   → Creates customer credit liability
            
            3. Bulk payment total remains UNCHANGED
            4. Credit is available for future invoice payments
            
            WHY TWO JOURNALS:
            -----------------
            - Maintains single currency per journal
            - Provides clear audit trail through suspense
            - Consistent with other multi-currency operations
            - Prevents value loss in currency conversions
            
            EXAMPLE:
            --------
            Bulk payment: 500 EUR received, only 400 EUR allocated
            Unallocated: 100 EUR
            
            Journal 1:
                Dr Cash (EUR)                   100 EUR
                   Cr Suspense (EUR)                100 EUR
            
            Journal 2:
                Dr Suspense (EUR)                100 EUR
                   Cr Customer Credit (EUR)          100 EUR
            
            Result: 100 EUR credit available to customer
            
            NOTES:
            ------
            - Credit is ALWAYS created in bulk payment currency
            - Uses suspense account for consistency
            - No invoices are affected
            - Can be used later via post_customer_credit_to_invoice()
            
            ===========================================================================
            """
            bulk_payment = source
            amount = kwargs.get('amount')
            reason = kwargs.get('reason', 'adjustment')
            suspense_account_id = kwargs.get('suspense_account_id')  # ← Get it from kwargs

            if not amount:
                raise ValueError("Amount required")
            if not suspense_account_id:
                raise ValueError("suspense_account_id required")

            # Convert to Decimal
            amount = Decimal(str(amount))

            # Create customer credit
            credit = CustomerCredit(
                app_id=app_id,
                customer_id=bulk_payment.customer_id,
                bulk_payment_id=bulk_payment.id,
                original_amount=amount,
                available_amount=amount,
                currency_id=bulk_payment.currency_id,
                exchange_rate_id=bulk_payment.exchange_rate_id,
                created_date=datetime.now(),
                issued_date=datetime.now().date(),
                status='active',
                credit_reason=reason,
                created_by=current_user.id if current_user else None
            )
            db_session.add(credit)
            db_session.flush()

            # Get customer credit account
            customer_credit_account_id = get_or_create_customer_credit_account(db_session, app_id)

            # Create TWO journals through suspense

            # Journal 1: Move from Cash to Suspense
            journal1_number = generate_unique_journal_number(db_session, app_id)
            lines1 = [
                {
                    "subcategory_id": bulk_payment.payment_account_id,
                    "amount": float(amount),
                    "dr_cr": "D",
                    "description": f"Reclassification to customer credit - move to suspense",
                    "source_type": "receipt_adjustment",
                    "source_id": bulk_payment.id
                },
                {
                    "subcategory_id": suspense_account_id,
                    "amount": float(amount),
                    "dr_cr": "C",
                    "description": f"Funds in suspense for credit creation",
                    "source_type": "bulk_payment_adjustment",
                    "source_id": bulk_payment.id
                }
            ]

            create_transaction(
                db_session=db_session,
                date=datetime.now().date(),
                currency=bulk_payment.currency_id,
                created_by=current_user.id,
                app_id=app_id,
                journal_number=journal1_number,
                journal_ref_no=f"CREDIT-{credit.id}-STEP1",
                narration=f"Move to suspense for credit #{credit.id}",
                payment_mode_id=None,
                project_id=bulk_payment.project_id,
                vendor_id=bulk_payment.customer_id,
                exchange_rate_id=bulk_payment.exchange_rate_id,
                status='Posted',
                lines=lines1
            )

            # Journal 2: Move from Suspense to Customer Credit
            journal2_number = generate_unique_journal_number(db_session, app_id)
            lines2 = [
                {
                    "subcategory_id": suspense_account_id,
                    "amount": float(amount),
                    "dr_cr": "D",
                    "description": f"Funds from suspense for credit creation",
                    "source_type": "bulk_payment_adjustment",
                    "source_id": bulk_payment.id
                },
                {
                    "subcategory_id": customer_credit_account_id,
                    "amount": float(amount),
                    "dr_cr": "C",
                    "description": f"Customer credit #{credit.id} created from unallocated amount",
                    "source_type": "bulk_payment_adjustment",
                    "source_id": bulk_payment.id
                }
            ]

            create_transaction(
                db_session=db_session,
                date=datetime.now().date(),
                currency=bulk_payment.currency_id,
                created_by=current_user.id,
                app_id=app_id,
                journal_number=journal2_number,
                journal_ref_no=f"CREDIT-{credit.id}-STEP2",
                narration=f"Created credit #{credit.id} from suspense",
                payment_mode_id=None,
                project_id=bulk_payment.project_id,
                vendor_id=bulk_payment.customer_id,
                exchange_rate_id=bulk_payment.exchange_rate_id,
                status='Posted',
                lines=lines2
            )

            return True, f"Credit #{credit.id} created from unallocated amount", {'credit_id': credit.id}
        else:
            raise ValueError(
                f"Unknown action: {action}. Supported actions: remove_transaction, create_credit_from_unallocated")

    except Exception as e:
        logger.error(f"Error in manage_bulk_payment ({action}): {str(e)}\n{traceback.format_exc()}")
        db_session.rollback()
        return False, str(e), None


def create_removal_adjustment_entry(db_session, bulk_payment, transaction, amount_in_invoice, amount_in_payment,  allocation, credit, current_user,
                                    suspense_account_id):
    """
    Create journal entries when removing a transaction from a bulk payment.
    Uses suspense account to bridge between invoice currency and payment currency.

    Two journals:

    Journal 1 (Invoice Currency):
        Dr: Accounts Receivable (reverse the payment)
        Cr: Suspense Account (move to suspense)

    Journal 2 (Payment Currency):
        Dr: Suspense Account (take from suspense)
        Cr: Customer Credit Account (create liability)

    This keeps cash account unchanged and creates a customer credit liability.
    """
    try:
        app_id = current_user.app_id
        invoice = transaction.invoice

        # Get customer credit account
        customer_credit_account_id = get_or_create_customer_credit_account(db_session, app_id)

        # ===== JOURNAL 1: Reverse AR and move to suspense (INVOICE CURRENCY) =====
        journal1_number = generate_unique_journal_number(db_session, app_id)

        lines_journal1 = [
            {
                "subcategory_id": invoice.account_receivable_id,
                "amount": amount_in_invoice,
                "dr_cr": "D",  # Debit AR to reverse the original credit
                "description": f"Reversal of payment for Invoice {invoice.invoice_number}",
                "source_type": "receipt_adjustment",
                "source_id": bulk_payment.id
            },
            {
                "subcategory_id": suspense_account_id,
                "amount": amount_in_invoice,
                "dr_cr": "C",  # Credit suspense to move funds
                "description": f"Funds moved to suspense from cancelled transaction",
                "source_type": "receipt_adjustment",
                "source_id": bulk_payment.id
            }
        ]

        create_transaction(
            db_session=db_session,
            date=bulk_payment.payment_date,
            currency=invoice.currency,  # Journal in invoice currency
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal1_number,
            journal_ref_no=f"ADJ-{bulk_payment.id}",
            narration=f"Reversed payment for Invoice {invoice.invoice_number}",
            payment_mode_id=None,
            project_id=bulk_payment.project_id,
            vendor_id=bulk_payment.customer_id,
            exchange_rate_id=invoice.exchange_rate_id,  # Use invoice rate
            status='Posted',
            lines=lines_journal1
        )

        logger.info(f"Created Journal 1 (invoice currency) for removed transaction {transaction.id}")

        # ===== JOURNAL 2: Move from suspense to customer credit (PAYMENT CURRENCY) =====
        journal2_number = generate_unique_journal_number(db_session, app_id)

        lines_journal2 = [
            {
                "subcategory_id": suspense_account_id,
                "amount": amount_in_payment,
                "dr_cr": "D",  # Debit suspense to take funds
                "description": f"Funds from suspense for cancelled transaction",
                "source_type": "receipt_adjustment",
                "source_id": bulk_payment.id
            },
            {
                "subcategory_id": customer_credit_account_id,
                "amount": amount_in_payment,
                "dr_cr": "C",  # Credit customer credit account (liability)
                "description": f"Credit #{credit.id} created from cancelled transaction",
                "source_type": "receipt_adjustment",
                "source_id": bulk_payment.id
            }
        ]

        create_transaction(
            db_session=db_session,
            date=bulk_payment.payment_date,
            currency=bulk_payment.currency_id,  # Journal in payment currency (EUR)
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal2_number,
            journal_ref_no=f"ADJ-{bulk_payment.id}",
            narration=f"Created credit #{credit.id} for {bulk_payment.customer.vendor_name}",
            payment_mode_id=None,
            project_id=bulk_payment.project_id,
            vendor_id=bulk_payment.customer_id,
            exchange_rate_id=bulk_payment.exchange_rate_id,  # Use bulk payment rate
            status='Posted',
            lines=lines_journal2
        )

        logger.info(f"Created Journal 2 (payment currency) for removed transaction {transaction.id}")

        return True, "Adjustment entries created successfully"

    except Exception as e:
        logger.error(f"Error creating removal adjustment: {str(e)}")
        return False, str(e)


def generate_payment_receipt_number(db_session=None, app_id=None):
    """Generate a unique payment receipt number."""
    return generate_sequence_number(
        prefix="RCPT",
        table_model=BulkPayment,  # the table storing the payment
        number_field="bulk_payment_number",  # could rename to payment_receipt_number later
        db_session=db_session,
        app_id=app_id
    )
