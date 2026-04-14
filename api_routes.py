# api_routes.py
import traceback
from collections import defaultdict
from datetime import datetime, date, timedelta
from decimal import Decimal
import logging

from flask import Blueprint, request, jsonify, abort, current_app
from flask_login import current_user, login_required
from sqlalchemy import func, case, not_
from sqlalchemy.orm import joinedload, aliased, load_only
from sqlalchemy.sql.elements import and_, or_

from ai import get_base_currency, get_exchange_rate
from db import Session
from decorators import require_permission, cached_route
from exceptions import DatabaseError
from models import Company, SalesTransaction, DirectSalesTransaction, Currency, OrderStatus, PurchaseTransaction, \
    DirectPurchaseTransaction, SalesPaymentStatus, PurchasePaymentStatus, \
    Category, PayrollTransaction, Deduction, Benefit, AdvanceRepayment, Employee, AdvancePayment, \
    ChartOfAccounts, InventoryItemVariationLink, InventoryEntry, InventoryItem, InventoryLocation, \
    InventoryItemAttribute, PayrollPeriod, UserModuleAccess, ExchangeRate, PaymentAllocation, SalesInvoice, \
    PurchaseOrder, InventoryEntryLineItem, InventoryTransactionDetail, InventorySummary, Vendor, Project, \
    InventoryCategory, JournalEntry, Journal, PurchasePaymentAllocation, InventorySubCategory, Brand, \
    InventoryItemVariation, Asset, AssetMovementLineItem, AssetMovement, AssetItem, Department
from utils import get_converted_cost, get_cash_balances, get_cash_balances_with_base, is_cash_related, \
    calculate_batch_available_quantity, calculate_available_quantity
from utils_and_helpers.cache_keys import inventory_dashboard_cache_key
from utils_and_helpers.cache_utils import clear_inventory_dashboard_cache_key
from utils_and_helpers.date_time_utils import get_date_range_from_filter

api_routes = Blueprint('api_routes', __name__)

# Set up logging
logger = logging.getLogger(__name__)


@api_routes.route('/api/sales/transactions', methods=["GET"])
def api_sales_transactions():
    api_key = request.headers.get("X-API-Key")
    currency_filter = request.args.get("currency")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    time_filter = request.args.get("time_filter")
    payment_mode_id = request.args.get("payment_mode")
    vendor_id = request.args.get("vendor")
    project_id = request.args.get("project")
    selected_value = request.args.get('invoice')
    reference = request.args.get("reference")
    status_filter = request.args.get("status")  # ✅ NEW: Get status filter

    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    db_session = Session()
    try:
        app_id = current_user.app_id

        # Get base currency first
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency = base_currency_info["base_currency"]

        # Get currency ID if currency filter is provided
        currency_id = None
        if currency_filter:
            currency = db_session.query(Currency.id).filter_by(
                app_id=app_id,
                user_currency=currency_filter
            ).first()
            if currency:
                currency_id = currency.id

        # OPTIMIZATION: Pre-calculate date ranges
        date_filters = _build_date_filters(start_date, end_date, time_filter)
        if isinstance(date_filters, dict) and 'error' in date_filters:
            return jsonify(date_filters), 400

        # OPTIMIZATION: Build base queries with eager loading to avoid N+1
        invoice_based_query, direct_sales_query = _build_base_queries(
            db_session, app_id, currency_id, reference, selected_value,
            payment_mode_id, vendor_id, project_id, date_filters
        )

        # OPTIMIZATION: Pre-calculate totals in batch to avoid N+1 queries
        combined_transactions = []

        # Process invoice transactions with batch optimization
        if not selected_value or (selected_value and selected_value.startswith('invoice')):
            invoice_transactions = _process_invoice_transactions_batch(
                db_session, app_id, invoice_based_query,
                currency_filter, base_currency_id, base_currency
            )
            combined_transactions.extend(invoice_transactions)

        # Process direct sales with batch optimization
        if not selected_value or (selected_value and selected_value.startswith('direct')):
            direct_transactions = _process_direct_transactions_batch(
                db_session, app_id, direct_sales_query,
                currency_filter, base_currency_id, base_currency
            )
            combined_transactions.extend(direct_transactions)

        # After combining invoice_transactions and direct_transactions
        # Sort by payment_date descending
        combined_transactions.sort(
            key=lambda x: x['payment_date'] or "",  # handle possible None
            reverse=True
        )

        return jsonify(combined_transactions)

    except Exception as e:
        logger.error(f"An error occured: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"error": "Something went wrong", "details": str(e)}), 500
    finally:
        db_session.close()


def _build_date_filters(start_date, end_date, time_filter):
    """Build date filters efficiently using the improved date range function"""
    if start_date and end_date:
        try:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            return {'start': start_date_obj, 'end': end_date_obj}
        except ValueError:
            return {"error": "Invalid date format. Use YYYY-MM-DD"}

    elif time_filter:
        # Use your improved function to get the date range
        start_date_obj, end_date_obj = get_date_range_from_filter(time_filter, start_date, end_date)
        return {'start': start_date_obj, 'end': end_date_obj}

    # ✅ ALWAYS return a default date range instead of None
    start_date_obj, end_date_obj = get_date_range_from_filter(None, start_date, end_date)
    return {'start': start_date_obj, 'end': end_date_obj}


def _build_base_queries(db_session, app_id, currency_id, reference, selected_value,
                        payment_mode_id, vendor_id, project_id, date_filters):
    """Build optimized base queries with proper joins"""

    # OPTIMIZATION: Use select() for specific columns and eager loading
    from sqlalchemy.orm import joinedload, selectinload

    # FIXED: Use actual relationship attributes, not strings
    # Invoice-based query with eager loading
    invoice_based_query = db_session.query(SalesTransaction).options(
        joinedload(SalesTransaction.invoice).joinedload(SalesInvoice.invoice_items),
        joinedload(SalesTransaction.customer),
        joinedload(SalesTransaction.currency),
        selectinload(SalesTransaction.payment_allocations).joinedload(PaymentAllocation.payment_modes),
        selectinload(SalesTransaction.payment_allocations).joinedload(PaymentAllocation.exchange_rate)
    ).filter_by(app_id=app_id).filter(
        SalesTransaction.payment_status.notin_([SalesPaymentStatus.cancelled]),
        SalesTransaction.is_posted_to_ledger == True  # ✅ only include posted
    )

    # Direct sales query with eager loading
    direct_sales_query = db_session.query(DirectSalesTransaction).options(
        joinedload(DirectSalesTransaction.direct_sale_items),
        joinedload(DirectSalesTransaction.customer),
        joinedload(DirectSalesTransaction.currency),
        selectinload(DirectSalesTransaction.payment_allocations).joinedload(PaymentAllocation.payment_modes),
        selectinload(DirectSalesTransaction.payment_allocations).joinedload(PaymentAllocation.exchange_rate)
    ).filter_by(app_id=app_id).filter(
        DirectSalesTransaction.status.notin_([OrderStatus.canceled, OrderStatus.draft]),
        DirectSalesTransaction.is_posted_to_ledger == True

    )

    # Apply filters
    if currency_id:
        invoice_based_query = invoice_based_query.filter(SalesTransaction.currency_id == currency_id)
        direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.currency_id == currency_id)

    if reference:
        invoice_based_query = invoice_based_query.filter(SalesTransaction.reference_number == reference)
        direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.sale_reference == reference)

    if selected_value:
        parts = selected_value.split('|', 1)
        if len(parts) == 2:
            invoice_type, invoice_number = parts
            invoice_number = invoice_number.strip()

            if invoice_type == 'invoice':
                invoice_based_query = invoice_based_query.join(
                    SalesInvoice, SalesTransaction.invoice_id == SalesInvoice.id
                ).filter(SalesInvoice.invoice_number == invoice_number)
            else:
                direct_sales_query = direct_sales_query.filter(
                    DirectSalesTransaction.direct_sale_number == invoice_number
                )

    if payment_mode_id:
        invoice_based_query = invoice_based_query.filter(
            SalesTransaction.payment_allocations.any(payment_mode=payment_mode_id)
        )
        direct_sales_query = direct_sales_query.filter(
            DirectSalesTransaction.payment_allocations.any(payment_mode=payment_mode_id)
        )

    if vendor_id:
        invoice_based_query = invoice_based_query.filter(SalesTransaction.customer_id == vendor_id)
        direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.customer_id == vendor_id)

    if project_id:
        invoice_based_query = invoice_based_query.filter(SalesTransaction.invoice.has(project_id=project_id))
        direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.project_id == project_id)

    # Apply date filters - SIMPLIFIED
    if date_filters:
        invoice_based_query = invoice_based_query.filter(
            SalesTransaction.payment_date.between(date_filters['start'], date_filters['end'])
        )
        direct_sales_query = direct_sales_query.filter(
            DirectSalesTransaction.payment_date.between(date_filters['start'], date_filters['end'])
        )

    return invoice_based_query, direct_sales_query


def _process_invoice_transactions_batch(db_session, app_id, invoice_query, currency_filter, base_currency_id,
                                        base_currency):
    """Process invoice transactions with batch optimizations"""

    # OPTIMIZATION: Get all invoice IDs first for batch processing
    invoice_transactions = list(invoice_query)
    invoice_ids = [tx.invoice_id for tx in invoice_transactions if tx.invoice_id]

    # OPTIMIZATION: Pre-calculate totals in batch WITH DATE FILTERS
    invoice_totals = {}
    if invoice_ids:
        # Get the date filters from the main query to ensure consistency
        date_filters_applied = False
        totals_query = db_session.query(
            SalesTransaction.invoice_id,
            func.sum(SalesTransaction.amount_paid).label('total_paid')
        ).filter(
            SalesTransaction.invoice_id.in_(invoice_ids),
            SalesTransaction.app_id == app_id,
            SalesTransaction.payment_status.notin_([SalesPaymentStatus.cancelled])
        )

        # Apply the same date filters as the main query
        if hasattr(invoice_query, '_where_criteria') and invoice_query._where_criteria:
            # Copy all WHERE conditions from the main query
            for condition in invoice_query._where_criteria:
                totals_query = totals_query.filter(condition)
            date_filters_applied = True

        # If no date filters were copied, apply default date range logic
        if not date_filters_applied:
            # Apply a safe default date range to prevent including all historical payments
            from datetime import date, timedelta
            default_end_date = date.today()
            default_start_date = default_end_date - timedelta(days=365)  # 1 year back
            totals_query = totals_query.filter(
                SalesTransaction.payment_date.between(default_start_date, default_end_date)
            )

        totals_query = totals_query.group_by(SalesTransaction.invoice_id)

        invoice_totals = {row.invoice_id: row.total_paid or 0 for row in totals_query.all()}

    # Process transactions
    transactions = []
    for tx in invoice_transactions:
        if not tx.invoice:
            continue

        invoice_id = tx.invoice_id
        total_invoice = tx.invoice.total_amount or 0
        total_paid = invoice_totals.get(invoice_id, 0)
        remaining = total_invoice - total_paid

        # Currency conversion
        amount_paid = _convert_amount(
            tx.amount_paid, tx, currency_filter, base_currency_id
        )
        if not currency_filter and tx.currency_id != base_currency_id:
            rate = _get_exchange_rate(tx)
            total_invoice = _convert_amount(total_invoice, tx, currency_filter, base_currency_id, rate)
            total_paid = _convert_amount(total_paid, tx, currency_filter, base_currency_id, rate)
            remaining = _convert_amount(remaining, tx, currency_filter, base_currency_id, rate)

        # FIXED: Correct status logic
        if remaining == 0:
            payment_progress = "Completed"
        elif remaining > 0:
            payment_progress = "Partial"
        else:  # remaining < 0 (overpaid)
            payment_progress = "Overpaid"

        transaction_data = {
            'id': tx.id,
            'type': 'invoice',
            'invoice_number': tx.invoice.invoice_number,
            'customer_name': tx.customer.vendor_name if tx.customer else None,
            'status': tx.invoice.status.name if tx.invoice and tx.invoice.status else None,
            'payment_date': tx.payment_date.isoformat() if tx.payment_date else None,
            'amount_paid': amount_paid,
            'currency': base_currency if not currency_filter else (
                tx.currency.user_currency if tx.currency else base_currency),
            'reference_number': tx.reference_number,
            'total_invoice_amount': total_invoice,
            'total_paid': total_paid,
            'remaining_balance': remaining,
            'payment_mode': tx.payment_allocations[0].payment_modes.payment_mode if tx.payment_allocations and
                                                                                    tx.payment_allocations[
                                                                                        0].payment_modes else None,
            'posted_to_ledger': tx.is_posted_to_ledger,
            'payment_progress': payment_progress,  # Use corrected status
            'items': _process_invoice_items(tx, currency_filter, base_currency_id)
        }
        transactions.append(transaction_data)

    return transactions


def _process_direct_transactions_batch(db_session, app_id, direct_query, currency_filter, base_currency_id,
                                       base_currency):
    """Process direct transactions with batch optimizations"""

    # OPTIMIZATION: Get all transactions first
    direct_transactions = list(direct_query)
    sale_numbers = [tx.direct_sale_number for tx in direct_transactions if tx.direct_sale_number]

    # OPTIMIZATION: Pre-calculate totals in batch WITH DATE FILTERS
    direct_totals = {}
    if sale_numbers:
        # Get the date filters from the main query to ensure consistency
        date_filters_applied = False
        totals_query = db_session.query(
            DirectSalesTransaction.direct_sale_number,
            func.sum(DirectSalesTransaction.amount_paid).label('total_paid')
        ).filter(
            DirectSalesTransaction.direct_sale_number.in_(sale_numbers),
            DirectSalesTransaction.app_id == app_id,
            DirectSalesTransaction.status.notin_([OrderStatus.canceled, OrderStatus.draft])
        )

        # Apply the same date filters as the main query
        if hasattr(direct_query, '_where_criteria') and direct_query._where_criteria:
            # Copy all WHERE conditions from the main query
            for condition in direct_query._where_criteria:
                totals_query = totals_query.filter(condition)
            date_filters_applied = True

        # If no date filters were copied, apply default date range logic
        if not date_filters_applied:
            # Apply a safe default date range to prevent including all historical payments
            from datetime import date, timedelta
            default_end_date = date.today()
            default_start_date = default_end_date - timedelta(days=365)  # 1 year back
            totals_query = totals_query.filter(
                DirectSalesTransaction.payment_date.between(default_start_date, default_end_date)
            )

        totals_query = totals_query.group_by(DirectSalesTransaction.direct_sale_number)

        direct_totals = {row.direct_sale_number: row.total_paid or 0 for row in totals_query.all()}

    # Process transactions
    transactions = []
    for tx in direct_transactions:
        total_sale = tx.total_amount or 0
        total_paid = direct_totals.get(tx.direct_sale_number, 0)
        remaining = total_sale - total_paid

        # Currency conversion
        amount_paid = _convert_amount(
            tx.amount_paid, tx, currency_filter, base_currency_id
        )
        if not currency_filter and tx.currency_id != base_currency_id:
            rate = _get_exchange_rate(tx)
            total_sale = _convert_amount(total_sale, tx, currency_filter, base_currency_id, rate)
            total_paid = _convert_amount(total_paid, tx, currency_filter, base_currency_id, rate)
            remaining = _convert_amount(remaining, tx, currency_filter, base_currency_id, rate)

        # FIXED: Correct sale type logic
        if remaining == 0:
            sale_type = "Full"
        elif remaining > 0:
            sale_type = "Installment"
        else:  # remaining < 0 (overpaid)
            sale_type = "Overpaid"

        transaction_data = {
            'id': tx.id,
            'type': 'direct',
            'direct_sale_number': tx.direct_sale_number,
            'customer_name': tx.customer.vendor_name if tx.customer else None,
            'status': tx.status.name if tx.status else None,
            'payment_date': tx.payment_date.isoformat() if tx.payment_date else None,
            'amount_paid': amount_paid,
            'currency': base_currency if not currency_filter else (
                tx.currency.user_currency if tx.currency else base_currency),
            'reference_number': tx.sale_reference,
            'total_sale_amount': total_sale,
            'total_paid': total_paid,
            'remaining_balance': remaining,
            'payment_mode': tx.payment_allocations[0].payment_modes.payment_mode if tx.payment_allocations and
                                                                                    tx.payment_allocations[
                                                                                        0].payment_modes else None,
            'posted_to_ledger': tx.is_posted_to_ledger,
            'sale_type': sale_type,  # Use corrected sale type
            'items': _process_direct_items(tx, currency_filter, base_currency_id)
        }
        transactions.append(transaction_data)

    return transactions


def _convert_amount(amount, transaction, currency_filter, base_currency_id, rate=None):
    """Convert amount to base currency if needed"""
    if currency_filter or (transaction and transaction.currency_id == base_currency_id):
        return float(amount or 0)

    if rate is None and transaction:
        rate = _get_exchange_rate(transaction)

    return float(Decimal(amount or 0) * rate) if rate else float(amount or 0)


def _get_exchange_rate(transaction):
    """Get exchange rate from payment allocations"""
    if transaction.payment_allocations:
        for allocation in transaction.payment_allocations:
            if allocation.exchange_rate and allocation.exchange_rate.rate:
                return allocation.exchange_rate.rate
    return None


def _process_invoice_items(transaction, currency_filter, base_currency_id):
    """Process invoice items efficiently"""
    if not transaction.invoice or not transaction.invoice.invoice_items:
        return []

    rate = None
    if not currency_filter and transaction.currency_id != base_currency_id:
        rate = _get_exchange_rate(transaction)

    return [_process_item(item, rate, currency_filter, transaction.currency_id, base_currency_id)
            for item in transaction.invoice.invoice_items]


def _process_direct_items(transaction, currency_filter, base_currency_id):
    """Process direct sale items efficiently"""
    if not transaction.direct_sale_items:
        return []

    rate = None
    if not currency_filter and transaction.currency_id != base_currency_id:
        rate = _get_exchange_rate(transaction)

    return [_process_item(item, rate, currency_filter, transaction.currency_id, base_currency_id)
            for item in transaction.direct_sale_items]


def _process_item(item, rate, currency_filter, currency_id, base_currency_id):
    """Process individual item data"""
    # OPTIMIZATION: Cache item name calculation
    item_name = item.item_name
    if not item_name and item.inventory_item_variation_link:
        inv_item = item.inventory_item_variation_link.inventory_item
        inv_variation = item.inventory_item_variation_link.inventory_item_variation
        if inv_item and inv_variation:
            item_name = f"{inv_item.item_name} ({inv_variation.variation_name})"

    description = item.description
    if not description and item.inventory_item_variation_link and item.inventory_item_variation_link.inventory_item:
        description = item.inventory_item_variation_link.inventory_item.item_description

    # Apply currency conversion if needed
    needs_conversion = not currency_filter and currency_id != base_currency_id

    return {
        'item_type': item.item_type,
        'item_name': item_name or "-",
        'description': description,
        'quantity': float(item.quantity or 0),
        'unit_price': _convert_amount(item.unit_price, None, currency_filter, base_currency_id,
                                      rate) if needs_conversion else float(item.unit_price or 0),
        'total_price': _convert_amount(item.total_price, None, currency_filter, base_currency_id,
                                       rate) if needs_conversion else float(item.total_price or 0),
        'tax_amount': _convert_amount(item.tax_amount, None, currency_filter, base_currency_id,
                                      rate) if needs_conversion else float(item.tax_amount or 0),
        'discount_amount': _convert_amount(item.discount_amount, None, currency_filter, base_currency_id,
                                           rate) if needs_conversion else float(item.discount_amount or 0),
        'tax_rate': float(item.tax_rate or 0),
        'discount_rate': float(item.discount_rate or 0),
        'uom': item.unit_of_measurement.abbreviation if item.unit_of_measurement else None,
        'currency': item.currencies.user_currency if item.currencies else None
    }


@api_routes.route('/api/purchases/transactions', methods=["GET"])
def api_purchases_transactions():
    api_key = request.headers.get("X-API-Key")
    currency_filter = request.args.get("currency")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    time_filter = request.args.get("time_filter")
    status_filter = request.args.get("status")
    payment_mode_id = request.args.get("payment_mode")
    vendor_id = request.args.get("vendor")
    project_id = request.args.get("project")
    selected_value = request.args.get('purchase')
    reference = request.args.get("reference")

    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    db_session = Session()
    try:
        company = db_session.query(Company).filter_by(api_key=api_key).first()
        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        app_id = company.id

        # Get currency ID if currency filter is provided
        currency_id = None
        if currency_filter:
            currency = db_session.query(Currency).filter_by(
                app_id=app_id,
                user_currency=currency_filter
            ).first()
            if currency:
                currency_id = currency.id

        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency = base_currency_info["base_currency"]

        # OPTIMIZATION: Pre-calculate date ranges using the same function as sales
        date_filters = _build_date_filters(start_date, end_date, time_filter)
        if isinstance(date_filters, dict) and 'error' in date_filters:
            return jsonify(date_filters), 400

        # OPTIMIZATION: Build optimized base queries with eager loading
        po_based_query, direct_purchase_query = _build_optimized_purchase_queries(
            db_session, app_id, currency_id, reference, selected_value,
            payment_mode_id, vendor_id, project_id, date_filters, status_filter
        )

        combined_transactions = []

        # OPTIMIZATION: Process PO transactions with batch totals calculation
        if not selected_value or (selected_value and selected_value.startswith('purchase')):
            po_transactions = _process_po_transactions_optimized(
                db_session, app_id, po_based_query, currency_filter,
                base_currency_id, base_currency
            )
            combined_transactions.extend(po_transactions)

        # OPTIMIZATION: Process direct purchases with batch totals calculation
        if not selected_value or (selected_value and selected_value.startswith('direct_purchase')):
            direct_transactions = _process_direct_purchases_optimized(
                db_session, app_id, direct_purchase_query, currency_filter,
                base_currency_id, base_currency
            )
            combined_transactions.extend(direct_transactions)

        # Sort by payment_date descending
        combined_transactions.sort(
            key=lambda x: x['payment_date'] or "",
            reverse=True
        )

        return jsonify(combined_transactions)

    except Exception as e:
        logger.error(f"Error: {e}")
        return jsonify({"error": "Something went wrong", "details": str(e)}), 500
    finally:
        db_session.close()


def _build_optimized_purchase_queries(db_session, app_id, currency_id, reference, selected_value,
                                      payment_mode_id, vendor_id, project_id, date_filters, status_filter):
    """Build optimized queries with eager loading"""
    from sqlalchemy.orm import joinedload, selectinload

    # PO-based query with eager loading - OPTIMIZATION
    po_based_query = db_session.query(PurchaseTransaction).options(
        joinedload(PurchaseTransaction.purchase_orders).joinedload(PurchaseOrder.purchase_order_items),
        joinedload(PurchaseTransaction.vendor),
        joinedload(PurchaseTransaction.currency),
        selectinload(PurchaseTransaction.payment_allocations).joinedload(PurchasePaymentAllocation.payment_modes),
        selectinload(PurchaseTransaction.payment_allocations).joinedload(PurchasePaymentAllocation.exchange_rate)
    ).filter_by(app_id=app_id, is_posted_to_ledger=True)

    # Direct purchase query with eager loading - OPTIMIZATION
    direct_purchase_query = db_session.query(DirectPurchaseTransaction).options(
        joinedload(DirectPurchaseTransaction.direct_purchase_items),
        joinedload(DirectPurchaseTransaction.vendor),
        joinedload(DirectPurchaseTransaction.currency),
        selectinload(DirectPurchaseTransaction.payment_allocations).joinedload(PurchasePaymentAllocation.payment_modes),
        selectinload(DirectPurchaseTransaction.payment_allocations).joinedload(PurchasePaymentAllocation.exchange_rate)
    ).filter(
        DirectPurchaseTransaction.app_id == app_id,
        DirectPurchaseTransaction.is_posted_to_ledger == True,
        DirectPurchaseTransaction.status.notin_([OrderStatus.draft, OrderStatus.canceled])
    )

    # Apply currency filter if specified
    if currency_id:
        po_based_query = po_based_query.filter(PurchaseTransaction.currency_id == currency_id)
        direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.currency_id == currency_id)

    if reference:
        po_based_query = po_based_query.filter(PurchaseTransaction.reference_number == reference)
        direct_purchase_query = direct_purchase_query.filter(
            DirectPurchaseTransaction.purchase_reference == reference)

    if selected_value:
        purchase_type, purchase_number = selected_value.split('|', 1)
        purchase_number = purchase_number.strip()
        purchase_type = purchase_type.strip()

        if purchase_type == 'purchase':
            po_based_query = po_based_query.join(
                PurchaseOrder, PurchaseTransaction.purchase_order_id == PurchaseOrder.id
            ).filter(
                PurchaseOrder.purchase_order_number == purchase_number
            )
        else:
            direct_purchase_query = direct_purchase_query.filter(
                DirectPurchaseTransaction.direct_purchase_number == purchase_number
            )

    # Apply payment_mode filter
    if payment_mode_id and payment_mode_id != 'None':
        po_based_query = po_based_query.join(PurchaseTransaction.payment_allocations).filter(
            PurchasePaymentAllocation.payment_mode == payment_mode_id
        )
        direct_purchase_query = direct_purchase_query.join(DirectPurchaseTransaction.payment_allocations).filter(
            PurchasePaymentAllocation.payment_mode == payment_mode_id
        )

    # Apply vendor filter
    if vendor_id and vendor_id != 'None':
        po_based_query = po_based_query.filter(PurchaseTransaction.vendor_id == vendor_id)
        direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.vendor_id == vendor_id)

    # Apply project filter
    if project_id and project_id != 'None':
        po_based_query = po_based_query.filter(PurchaseTransaction.purchase_orders.has(project_id=project_id))
        direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.project_id == project_id)

    # Apply date filters - USING THE SAME LOGIC AS SALES
    if date_filters:
        po_based_query = po_based_query.filter(
            PurchaseTransaction.payment_date.between(date_filters['start'], date_filters['end'])
        )
        direct_purchase_query = direct_purchase_query.filter(
            DirectPurchaseTransaction.payment_date.between(date_filters['start'], date_filters['end'])
        )

    # Apply status filter
    if status_filter:
        if status_filter == 'posted':
            po_based_query = po_based_query.filter(PurchaseTransaction.is_posted_to_ledger == True)
            direct_purchase_query = direct_purchase_query.filter(
                DirectPurchaseTransaction.is_posted_to_ledger == True)
        elif status_filter == 'not_posted':
            po_based_query = po_based_query.filter(PurchaseTransaction.is_posted_to_ledger == False)
            direct_purchase_query = direct_purchase_query.filter(
                DirectPurchaseTransaction.is_posted_to_ledger == False)

    return po_based_query, direct_purchase_query


def _process_po_transactions_optimized(db_session, app_id, po_query, currency_filter, base_currency_id, base_currency):
    """Process PO transactions with optimized batch processing"""

    # OPTIMIZATION: Get all transactions at once with eager loading
    po_transactions = list(po_query)

    # OPTIMIZATION: Batch calculate totals for all POs
    po_ids = [tx.purchase_order_id for tx in po_transactions if tx.purchase_order_id]
    po_totals = {}

    if po_ids:
        # PRESERVE ORIGINAL LOGIC: Calculate total paid without date filters for outstanding balance
        totals_result = db_session.query(
            PurchaseTransaction.purchase_order_id,
            func.sum(PurchaseTransaction.amount_paid).label('total_paid')
        ).filter(
            PurchaseTransaction.purchase_order_id.in_(po_ids),
            PurchaseTransaction.app_id == app_id,
            PurchaseTransaction.payment_status != PurchasePaymentStatus.cancelled
        ).group_by(PurchaseTransaction.purchase_order_id).all()

        po_totals = {row.purchase_order_id: row.total_paid or 0 for row in totals_result}

    transactions = []
    for tx in po_transactions:
        if not tx.purchase_orders:
            continue

        po_id = tx.purchase_order_id
        total_po_amount = tx.purchase_orders.total_amount if tx.purchase_orders else 0

        # Use batch calculated total - PRESERVE ORIGINAL LOGIC
        total_paid = po_totals.get(po_id, 0)
        remaining = total_po_amount - total_paid

        # Currency conversion - PRESERVE ORIGINAL LOGIC
        amount_paid = tx.amount_paid
        if not currency_filter and tx.currency_id != base_currency_id:
            rate = tx.payment_allocations[0].exchange_rate.rate if tx.payment_allocations and tx.payment_allocations[
                0].exchange_rate else None
            if rate:
                def convert(amount, rate):
                    return float(Decimal(amount) * rate) if rate else float(amount)

                amount_paid = convert(tx.amount_paid, rate)
                total_po_amount = convert(total_po_amount, rate)
                total_paid = convert(total_paid, rate)
                remaining = convert(remaining, rate)

        # Payment progress logic - PRESERVE ORIGINAL LOGIC
        payment_progress = "Final" if remaining == 0 else "Ongoing" if total_paid > tx.amount_paid else "Initial"

        # OPTIMIZATION: Process items efficiently
        items = _process_po_items_optimized(tx, currency_filter, base_currency_id,
                                            rate if not currency_filter and tx.currency_id != base_currency_id else None)

        # FIX: Return date exactly as in original code - let Flask jsonify handle serialization
        transaction_data = {
            'id': tx.id,
            'type': 'purchase_order',
            'po_number': tx.purchase_orders.purchase_order_number if tx.purchase_orders else None,
            'vendor_name': tx.vendor.vendor_name if tx.vendor else None,
            'payment_date': tx.payment_date,  # FIXED: Return date object directly like original code
            'amount_paid': amount_paid,
            'currency': base_currency if not currency_filter else (
                tx.currency.user_currency if tx.currency else base_currency),
            'reference_number': tx.reference_number,
            'total_po_amount': total_po_amount,
            'total_paid': total_paid,
            'remaining_balance': remaining,
            'payment_mode': tx.payment_allocations[0].payment_modes.payment_mode if tx.payment_allocations and
                                                                                    tx.payment_allocations[
                                                                                        0].payment_modes else None,
            'posted_to_ledger': tx.is_posted_to_ledger,
            'status': tx.payment_status.name,
            'payment_progress': payment_progress,
            'items': items
        }
        transactions.append(transaction_data)

    return transactions


def _process_direct_purchases_optimized(db_session, app_id, direct_query, currency_filter, base_currency_id,
                                        base_currency):
    """Process direct purchases with optimized batch processing"""

    # OPTIMIZATION: Get all transactions at once with eager loading
    direct_transactions = list(direct_query)

    # OPTIMIZATION: Batch calculate totals for all direct purchases
    purchase_numbers = [tx.direct_purchase_number for tx in direct_transactions if tx.direct_purchase_number]
    direct_totals = {}

    if purchase_numbers:
        # PRESERVE ORIGINAL LOGIC: Calculate total paid without date filters for outstanding balance
        totals_result = db_session.query(
            DirectPurchaseTransaction.direct_purchase_number,
            func.sum(DirectPurchaseTransaction.amount_paid).label('total_paid')
        ).filter(
            DirectPurchaseTransaction.direct_purchase_number.in_(purchase_numbers),
            DirectPurchaseTransaction.app_id == app_id,
            DirectPurchaseTransaction.status.name != "draft"
        ).group_by(DirectPurchaseTransaction.direct_purchase_number).all()

        direct_totals = {row.direct_purchase_number: row.total_paid or 0 for row in totals_result}

    transactions = []
    for tx in direct_transactions:
        total_purchase_amount = tx.total_amount

        # Use batch calculated total - PRESERVE ORIGINAL LOGIC
        total_paid = direct_totals.get(tx.direct_purchase_number, 0)
        remaining = total_purchase_amount - total_paid

        # Currency conversion - PRESERVE ORIGINAL LOGIC
        amount_paid = tx.amount_paid
        if not currency_filter and tx.currency_id != base_currency_id:
            rate = tx.payment_allocations[0].exchange_rate.rate if tx.payment_allocations and tx.payment_allocations[
                0].exchange_rate else None
            if rate:
                def convert(amount, rate):
                    return float(Decimal(amount) * rate) if rate else float(amount)

                amount_paid = convert(tx.amount_paid, rate)
                total_purchase_amount = convert(total_purchase_amount, rate)
                total_paid = convert(total_paid, rate)
                remaining = convert(remaining, rate)

        # OPTIMIZATION: Process items efficiently
        items = _process_direct_purchase_items_optimized(tx, currency_filter, base_currency_id,
                                                         rate if not currency_filter and tx.currency_id != base_currency_id else None)

        # FIX: Return date exactly as in original code - let Flask jsonify handle serialization
        transaction_data = {
            'id': tx.id,
            'type': 'direct',
            'direct_purchase_number': tx.direct_purchase_number,
            'vendor_name': tx.vendor.vendor_name if tx.vendor else None,
            'payment_date': tx.payment_date,  # FIXED: Return date object directly like original code
            'amount_paid': amount_paid,
            'currency': base_currency if not currency_filter else (
                tx.currency.user_currency if tx.currency else base_currency),
            'reference_number': tx.purchase_reference,
            'total_purchase_amount': total_purchase_amount,
            'total_paid': total_paid,
            'remaining_balance': remaining,
            'payment_mode': tx.payment_allocations[0].payment_modes.payment_mode if tx.payment_allocations and
                                                                                    tx.payment_allocations[
                                                                                        0].payment_modes else None,
            'posted_to_ledger': tx.is_posted_to_ledger,
            'status': tx.status.name,
            'purchase_type': "Installment" if remaining > 0 else "Full",
            'items': items
        }
        transactions.append(transaction_data)

    return transactions


def _process_po_items_optimized(transaction, currency_filter, base_currency_id, rate=None):
    """Process PO items efficiently"""
    if not transaction.purchase_orders or not transaction.purchase_orders.purchase_order_items:
        return []

    def convert(amount, rate):
        return float(Decimal(amount) * rate) if rate else float(amount)

    items = []
    for item in transaction.purchase_orders.purchase_order_items:
        # Item name logic - PRESERVE ORIGINAL
        item_name = item.item_name
        if not item_name and item.inventory_item_variation_link:
            inv_item = item.inventory_item_variation_link.inventory_item
            inv_variation = item.inventory_item_variation_link.inventory_item_variation
            if inv_item and inv_variation:
                item_name = f"{inv_item.item_name} ({inv_variation.variation_name})"

        # Description logic - PRESERVE ORIGINAL
        description = item.description
        if not description and item.inventory_item_variation_link and item.inventory_item_variation_link.inventory_item:
            description = item.inventory_item_variation_link.inventory_item.item_description

        # Currency conversion for items - PRESERVE ORIGINAL LOGIC
        needs_conversion = not currency_filter and transaction.currency_id != base_currency_id

        item_data = {
            'item_type': item.item_type,
            'item_name': item_name or "-",
            'description': description,
            'quantity': float(item.quantity),
            'unit_price': convert(item.unit_price, rate) if needs_conversion else float(item.unit_price),
            'total_price': convert(item.total_price, rate) if needs_conversion else float(item.total_price),
            'tax_amount': convert(item.tax_amount, rate) if item.tax_amount and needs_conversion else float(
                item.tax_amount or 0),
            'discount_amount': convert(item.discount_amount,
                                       rate) if item.discount_amount and needs_conversion else float(
                item.discount_amount or 0),
            'tax_rate': float(item.tax_rate) if item.tax_rate else 0,
            'discount_rate': float(item.discount_rate) if item.discount_rate else 0,
            'uom': item.unit_of_measurement.abbreviation if item.unit_of_measurement else None,
            'currency': item.currencies.user_currency if item.currencies else None
        }
        items.append(item_data)

    return items


def _process_direct_purchase_items_optimized(transaction, currency_filter, base_currency_id, rate=None):
    """Process direct purchase items efficiently"""
    if not transaction.direct_purchase_items:
        return []

    def convert(amount, rate):
        return float(Decimal(amount) * rate) if rate else float(amount)

    items = []
    for item in transaction.direct_purchase_items:
        # Item name logic - PRESERVE ORIGINAL
        item_name = item.item_name
        if not item_name and item.inventory_item_variation_link:
            inv_item = item.inventory_item_variation_link.inventory_item
            inv_variation = item.inventory_item_variation_link.inventory_item_variation
            if inv_item and inv_variation:
                item_name = f"{inv_item.item_name} ({inv_variation.variation_name})"

        # Description logic - PRESERVE ORIGINAL
        description = item.description
        if not description and item.inventory_item_variation_link and item.inventory_item_variation_link.inventory_item:
            description = item.inventory_item_variation_link.inventory_item.item_description

        # Currency conversion for items - PRESERVE ORIGINAL LOGIC
        needs_conversion = not currency_filter and transaction.currency_id != base_currency_id

        item_data = {
            'item_type': item.item_type,
            'item_name': item_name or "-",
            'description': description,
            'quantity': float(item.quantity),
            'unit_price': convert(item.unit_price, rate) if needs_conversion else float(item.unit_price),
            'total_price': convert(item.total_price, rate) if needs_conversion else float(item.total_price),
            'tax_amount': convert(item.tax_amount, rate) if item.tax_amount and needs_conversion else float(
                item.tax_amount or 0),
            'discount_amount': convert(item.discount_amount,
                                       rate) if item.discount_amount and needs_conversion else float(
                item.discount_amount or 0),
            'tax_rate': float(item.tax_rate) if item.tax_rate else 0,
            'discount_rate': float(item.discount_rate) if item.discount_rate else 0,
            'uom': item.unit_of_measurement.abbreviation if item.unit_of_measurement else None,
            'currency': item.currencies.user_currency if item.currencies else None
        }
        items.append(item_data)

    return items


def get_filter_date(time_filter):
    """Helper to get filter date based on time filter string"""
    today = date.today()
    if time_filter == 'today':
        return today, None
    elif time_filter == 'yesterday':
        return today - timedelta(days=1), None
    elif time_filter == 'week':
        return today - timedelta(days=today.weekday()), None
    elif time_filter == 'month':
        return today.replace(day=1), None
    elif time_filter == 'last_month':
        first_day_current = today.replace(day=1)
        last_day_previous = first_day_current - timedelta(days=1)
        return last_day_previous.replace(day=1), last_day_previous
    elif time_filter == 'quarter':
        quarter_month = ((today.month - 1) // 3) * 3 + 1
        return today.replace(month=quarter_month, day=1), None
    elif time_filter == 'year':
        return today.replace(month=1, day=1), None
    return None, None  # For custom or invalid filters


# Inital Purchase api routes
# @api_routes.route('/api/purchases/transactions', methods=["GET"])
# def api_purchases_transactions():
#     api_key = request.headers.get("X-API-Key")
#     currency_filter = request.args.get("currency")
#     start_date = request.args.get("start_date")
#     end_date = request.args.get("end_date")
#     time_filter = request.args.get("time_filter")  # today, week, month, quarter, year
#     status_filter = request.args.get("status")  # posted, not_posted
#
#     payment_mode_id = request.args.get("payment_mode")
#     vendor_id = request.args.get("vendor")
#     project_id = request.args.get("project")
#
#     selected_value = request.args.get('purchase')  # e.g. "purchase|PO-001"
#     reference = request.args.get("reference")
#     logger.info(f'Data is {request.args}')
#     if not api_key:
#         return jsonify({"error": "API key is missing"}), 401
#
#     db_session = Session()
#     try:
#         company = db_session.query(Company).filter_by(api_key=api_key).first()
#         if not company:
#             return jsonify({"error": "Invalid API key"}), 403
#
#         app_id = company.id
#
#         # Get currency ID if currency filter is provided
#         currency_id = None
#         if currency_filter:
#             currency = db_session.query(Currency).filter_by(
#                 app_id=app_id,
#                 user_currency=currency_filter
#             ).first()
#             if currency:
#                 currency_id = currency.id
#
#         base_currency_info = get_base_currency(db_session, app_id)
#         if not base_currency_info:
#             return jsonify({"error": "Base currency not defined for this company"}), 400
#
#         base_currency_id = base_currency_info["base_currency_id"]
#         base_currency = base_currency_info["base_currency"]
#
#         # Helper function to convert to base currency
#         def convert(amount, rate):
#             return float(Decimal(amount) * rate) if rate else float(amount)
#
#         # Initialize base queries
#         po_based_query = db_session.query(PurchaseTransaction).filter_by(app_id=app_id)
#         direct_purchase_query = db_session.query(DirectPurchaseTransaction).filter(
#             DirectPurchaseTransaction.app_id == app_id,
#             DirectPurchaseTransaction.status.notin_([OrderStatus.draft, OrderStatus.canceled])
#             # Exclude drafts and canceled
#         )
#
#         # Apply currency filter if specified
#         if currency_id:
#             po_based_query = po_based_query.filter(PurchaseTransaction.currency_id == currency_id)
#             direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.currency_id == currency_id)
#
#         if reference:
#             po_based_query = po_based_query.filter(PurchaseTransaction.reference_number == reference)
#             direct_purchase_query = direct_purchase_query.filter(
#                 DirectPurchaseTransaction.purchase_reference == reference)
#
#         if selected_value:
#             purchase_type, purchase_number = selected_value.split('|', 1)
#             purchase_number = purchase_number.strip()
#             purchase_type = purchase_type.strip()
#
#             if purchase_type == 'purchase':
#
#                 po_based_query = po_based_query.join(
#                     PurchaseOrder, PurchaseTransaction.purchase_order_id == PurchaseOrder.id
#                 ).filter(
#                     PurchaseOrder.purchase_number == purchase_number
#                 )
#
#             else:
#                 direct_purchase_query = direct_purchase_query.filter(
#                     DirectPurchaseTransaction.direct_purchase_number == purchase_number
#                 )
#
#         # Apply payment_mode filter
#         if payment_mode_id and payment_mode_id != 'None':
#             po_based_query = po_based_query.join(PurchaseTransaction.payment_allocations).filter(
#                 PaymentAllocation.payment_mode == payment_mode_id
#             )
#             direct_purchase_query = direct_purchase_query.join(DirectPurchaseTransaction.payment_allocations).filter(
#                 PaymentAllocation.payment_mode == payment_mode_id
#             )
#
#         # Apply vendor filter
#         if vendor_id and vendor_id != 'None':
#             po_based_query = po_based_query.filter(PurchaseTransaction.vendor_id == vendor_id)
#             direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.vendor_id == vendor_id)
#
#         # Apply project filter
#         if project_id and project_id != 'None':
#             po_based_query = po_based_query.filter(PurchaseTransaction.purchase_orders.has(project_id == project_id))
#
#             direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.project_id == project_id)
#
#         # Apply date filters
#         if start_date and end_date:
#             try:
#                 start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
#                 end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
#
#                 po_based_query = po_based_query.filter(
#                     PurchaseTransaction.payment_date.between(start_date_obj, end_date_obj)
#                 )
#                 direct_purchase_query = direct_purchase_query.filter(
#                     DirectPurchaseTransaction.payment_date.between(start_date_obj, end_date_obj)
#                 )
#             except ValueError:
#                 return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
#         elif time_filter:
#             today = date.today()
#             if time_filter == 'today':
#                 filter_date = today
#             elif time_filter == 'yesterday':
#                 filter_date = today - timedelta(days=1)
#             elif time_filter == 'week':
#                 filter_date = today - timedelta(days=today.weekday())
#             elif time_filter == 'month':
#                 filter_date = today.replace(day=1)
#             elif time_filter == 'last_month':
#                 first_day_current_month = today.replace(day=1)
#                 filter_date = (first_day_current_month - timedelta(days=1)).replace(day=1)
#                 end_date = first_day_current_month - timedelta(days=1)
#             elif time_filter == 'quarter':
#                 quarter_month = ((today.month - 1) // 3) * 3 + 1
#                 filter_date = today.replace(month=quarter_month, day=1)
#             elif time_filter == 'year':
#                 filter_date = today.replace(month=1, day=1)
#             elif time_filter == 'custom':
#                 # Custom range will be handled by start_date/end_date parameters
#                 pass
#             else:
#                 return jsonify({"error": "Invalid time filter"}), 400
#
#             if time_filter != 'custom':
#                 if time_filter == 'last_month':
#                     # Special case for last month which needs both start and end
#                     po_based_query = po_based_query.filter(
#                         PurchaseTransaction.payment_date.between(filter_date, end_date)
#                     )
#                     direct_purchase_query = direct_purchase_query.filter(
#                         DirectPurchaseTransaction.payment_date.between(filter_date, end_date)
#                     )
#                 else:
#                     # All other cases use >= filter_date
#                     po_based_query = po_based_query.filter(
#                         PurchaseTransaction.payment_date >= filter_date
#                     )
#                     direct_purchase_query = direct_purchase_query.filter(
#                         DirectPurchaseTransaction.payment_date >= filter_date
#                     )
#
#         # Apply status filter
#         if status_filter:
#             if status_filter == 'posted':
#                 po_based_query = po_based_query.filter(PurchaseTransaction.is_posted_to_ledger == True)
#                 direct_purchase_query = direct_purchase_query.filter(
#                     DirectPurchaseTransaction.is_posted_to_ledger == True)
#             elif status_filter == 'not_posted':
#                 po_based_query = po_based_query.filter(PurchaseTransaction.is_posted_to_ledger == False)
#                 direct_purchase_query = direct_purchase_query.filter(
#                     DirectPurchaseTransaction.is_posted_to_ledger == False)
#
#         combined_transactions = []
#
#         # Process PO-based purchases
#         # Only process invoice-based sales if:
#         # 1. No selected_value filter is applied, OR
#         # 2. We're specifically filtering for invoices
#         if not selected_value or (selected_value and purchase_type == 'purchase'):
#             for tx in po_based_query.all():
#                 po_id = tx.purchase_order_id
#                 total_po_amount = tx.purchase_orders.total_amount if tx.purchase_orders else 0
#
#                 # Calculate total paid for this PO
#                 total_paid = db_session.query(func.sum(PurchaseTransaction.amount_paid)) \
#                                  .filter(PurchaseTransaction.purchase_order_id == po_id,
#                                          PurchaseTransaction.app_id == app_id,
#                                          PurchaseTransaction.payment_status != PurchasePaymentStatus.cancelled) \
#                                  .scalar() or 0
#
#                 remaining = total_po_amount - total_paid
#
#                 amount_paid = tx.amount_paid
#                 if not currency_filter and tx.currency_id != base_currency_id:
#                     rate = tx.payment_allocations[0].exchange_rate.rate if tx.payment_allocations else None
#                     amount_paid = convert(tx.amount_paid, rate)
#                     total_po_amount = convert(total_po_amount, rate)
#                     total_paid = convert(total_paid, rate)
#                     remaining = convert(remaining, rate)
#
#                 combined_transactions.append({
#                     'id': tx.id,
#                     'type': 'purchase_order',
#                     'po_number': tx.purchase_orders.purchase_order_number if tx.purchase_orders else None,
#                     'vendor_name': tx.vendor.vendor_name,
#                     'payment_date': tx.payment_date,
#                     'amount_paid': amount_paid,
#                     'currency': base_currency if not currency_filter else tx.currency.user_currency,
#                     'reference_number': tx.reference_number,
#                     'total_po_amount': total_po_amount,
#                     'total_paid': total_paid,
#                     'remaining_balance': remaining,
#                     'payment_mode': tx.payment_allocations[0].payment_modes.payment_mode
#                     if tx.payment_allocations and tx.payment_allocations[0].payment_modes else None,
#                     'posted_to_ledger': tx.is_posted_to_ledger,
#                     'status': tx.payment_status.name,
#                     'payment_progress': "Final" if remaining == 0 else "Ongoing" if total_paid > amount_paid else "Initial",
#                     'items': [
#                         {
#                             'item_type': item.item_type,
#                             'item_name': (
#                                     item.item_name
#                                     or (
#                                         f"{item.inventory_item_variation_link.inventory_item.item_name} "
#                                         f"({item.inventory_item_variation_link.inventory_item_variation.variation_name})"
#                                         if item.inventory_item_variation_link and item.inventory_item_variation_link.inventory_item
#                                            and item.inventory_item_variation_link.inventory_item_variation else None
#                                     )
#                                     or "-"
#                             ),
#                             'description': item.description if item.item_name else item.inventory_item_variation_link.inventory_item.item_description,
#                             'quantity': float(item.quantity),
#                             'unit_price': convert(item.unit_price,
#                                                   rate) if not currency_filter and tx.currency_id != base_currency_id else float(
#                                 item.unit_price),
#                             'total_price': convert(item.total_price,
#                                                    rate) if not currency_filter and tx.currency_id != base_currency_id else float(
#                                 item.total_price),
#                             'tax_amount': convert(item.tax_amount,
#                                                   rate) if item.tax_amount and not currency_filter and tx.currency_id != base_currency_id else float(
#                                 item.tax_amount or 0),
#                             'discount_amount': convert(item.discount_amount,
#                                                        rate) if item.discount_amount and not currency_filter and tx.currency_id != base_currency_id else float(
#                                 item.discount_amount or 0),
#
#                             'tax_rate': float(item.tax_rate) if item.tax_rate else 0,
#
#                             'discount_rate': float(item.discount_rate) if item.discount_rate else 0,
#
#                             'uom': item.unit_of_measurement.abbreviation if item.unit_of_measurement else None,
#                             'currency': item.currencies.user_currency if item.currencies else None
#                         } for item in tx.purchase_orders.purchase_order_items if tx.purchase_orders
#                     ]
#                 })
#
#         # Process direct purchases
#         # Only process direct purchases if:
#         # 1. No selected_value filter is applied, OR
#         # 2. We're specifically filtering for direct sales
#         if not selected_value or (selected_value and purchase_type == 'direct_purchase'):
#             for tx in direct_purchase_query.all():
#                 total_purchase_amount = tx.total_amount
#                 total_paid = db_session.query(func.sum(DirectPurchaseTransaction.amount_paid)) \
#                                  .filter(
#                     DirectPurchaseTransaction.direct_purchase_number == tx.direct_purchase_number,
#                     DirectPurchaseTransaction.app_id == app_id,
#                     DirectPurchaseTransaction.status.name != "draft"
#                 ).scalar() or 0
#
#                 remaining = total_purchase_amount - total_paid
#                 print(f"tx.status.name {tx.status.name}")
#                 amount_paid = tx.amount_paid
#                 if not currency_filter and tx.currency_id != base_currency_id:
#                     rate = tx.payment_allocations[0].exchange_rate.rate if tx.payment_allocations else None
#                     amount_paid = convert(tx.amount_paid, rate)
#                     total_purchase_amount = convert(total_purchase_amount, rate)
#                     total_paid = convert(total_paid, rate)
#                     remaining = convert(remaining, rate)
#
#                 combined_transactions.append({
#                     'id': tx.id,
#                     'type': 'direct',
#                     'direct_purchase_number': tx.direct_purchase_number,
#                     'vendor_name': tx.vendor.vendor_name,
#                     'payment_date': tx.payment_date,
#                     'amount_paid': amount_paid,
#                     'currency': base_currency if not currency_filter else tx.currency.user_currency,
#                     'reference_number': tx.purchase_reference,
#                     'total_purchase_amount': total_purchase_amount,
#                     'total_paid': total_paid,
#                     'remaining_balance': remaining,
#                     'payment_mode': tx.payment_allocations[0].payment_modes.payment_mode
#                     if tx.payment_allocations and tx.payment_allocations[0].payment_modes else None,
#                     'posted_to_ledger': tx.is_posted_to_ledger,
#                     'status': tx.status.name,
#                     'purchase_type': "Installment" if remaining > 0 else "Full",
#                     'items': [
#                         {
#                             'item_type': item.item_type,
#                             'item_name': (
#                                     item.item_name
#                                     or (
#                                         f"{item.inventory_item_variation_link.inventory_item.item_name} "
#                                         f"({item.inventory_item_variation_link.inventory_item_variation.variation_name})"
#                                         if item.inventory_item_variation_link and item.inventory_item_variation_link.inventory_item
#                                            and item.inventory_item_variation_link.inventory_item_variation else None
#                                     )
#                                     or "-"
#                             ),
#                             'description': item.description if item.item_name else item.inventory_item_variation_link.inventory_item.item_description,
#                             'quantity': float(item.quantity),
#                             'unit_price': convert(item.unit_price,
#                                                   rate) if not currency_filter and tx.currency_id != base_currency_id else float(
#                                 item.unit_price),
#                             'total_price': convert(item.total_price,
#                                                    rate) if not currency_filter and tx.currency_id != base_currency_id else float(
#                                 item.total_price),
#                             'tax_amount': convert(item.tax_amount,
#                                                   rate) if item.tax_amount and not currency_filter and tx.currency_id != base_currency_id else float(
#                                 item.tax_amount or 0),
#                             'discount_amount': convert(item.discount_amount,
#                                                        rate) if item.discount_amount and not currency_filter and tx.currency_id != base_currency_id else float(
#                                 item.discount_amount or 0),
#
#                             'tax_rate': float(item.tax_rate) if item.tax_rate else 0,
#
#                             'discount_rate': float(item.discount_rate) if item.discount_rate else 0,
#
#                             'uom': item.unit_of_measurement.abbreviation if item.unit_of_measurement else None,
#                             'currency': item.currencies.user_currency if item.currencies else None
#                         } for item in tx.direct_purchase_items
#                     ]
#                 })
#
#
#         return jsonify(combined_transactions)
#
#     except Exception as e:
#         print(f"Error: {e}")
#         return jsonify({"error": "Something went wrong", "details": str(e)}), 500
#
#     finally:
#         db_session.close()


@api_routes.route('/api/payroll/payruns', methods=["GET"])
def get_payrun_data():
    # Authentication and validation
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    # Get filter parameters
    payroll_period_id = request.args.get("payroll_period_id")
    employee_id = request.args.get("employee_id")
    department_id = request.args.get("department_id")
    project_id = request.args.get("project_id")
    payment_status = request.args.get("payment_status")
    currency_id = request.args.get("currency")

    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    time_filter = request.args.get("time_filter")  # today, week, month, quarter, year

    with Session() as db_session:
        try:
            app_id = current_user.app_id

            if not currency_id:
                base_currency_info = get_base_currency(db_session, app_id)
                if not base_currency_info:
                    return jsonify({"error": "Base currency not defined for this company"}), 400

                currency_id = base_currency_info["base_currency_id"]
                base_currency = base_currency_info["base_currency"]

            # Base query
            # Base query - fixed version
            query = db_session.query(PayrollTransaction).filter(
                PayrollTransaction.app_id == app_id,
                PayrollTransaction.currency_id == currency_id
            ).join(
                PayrollTransaction.payroll_period
            )

            if payroll_period_id:
                query = query.filter(PayrollTransaction.payroll_period_id == payroll_period_id)
                # Don't apply date filters if a specific payroll period is selected
                start_date = None
                end_date = None
                time_filter = None
            if employee_id:
                query = query.filter(PayrollTransaction.employee_id == employee_id)
            if department_id:
                query = query.join(Employee).filter(Employee.department_id == department_id)
            if project_id:
                query = query.join(Employee).filter(Employee.project_id == project_id)
            if payment_status:
                query = query.filter(PayrollTransaction.payment_status == payment_status)

            # Apply date filters
            #  - fixed
            if start_date and end_date:
                try:
                    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                    query = query.filter(
                        PayrollPeriod.start_date >= start_date_obj,
                        PayrollPeriod.end_date <= end_date_obj
                    )
                except ValueError:
                    return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
            elif time_filter:
                today = date.today()
                if time_filter == 'today':
                    query = query.filter(PayrollPeriod.start_date == today)
                elif time_filter == 'week':
                    start_of_week = today - timedelta(days=today.weekday())
                    query = query.filter(
                        PayrollPeriod.start_date >= start_of_week,
                        PayrollPeriod.end_date <= today
                    )
                elif time_filter == 'month':
                    first_day = today.replace(day=1)
                    query = query.filter(
                        PayrollPeriod.start_date >= first_day,
                        PayrollPeriod.end_date <= today
                    )

                elif time_filter == 'last_month':
                    # Calculate last month's first and last day
                    first_day_this_month = today.replace(day=1)
                    last_day_last_month = first_day_this_month - timedelta(days=1)
                    first_day_last_month = last_day_last_month.replace(day=1)
                    query = query.filter(
                        PayrollPeriod.start_date >= first_day_last_month,
                        PayrollPeriod.end_date <= last_day_last_month
                    )

                elif time_filter == 'quarter':
                    quarter_month = ((today.month - 1) // 3) * 3 + 1
                    quarter_start = date(today.year, quarter_month, 1)
                    if quarter_month == 10:
                        quarter_end = date(today.year, 12, 31)
                    else:
                        next_quarter_month = quarter_month + 3
                        quarter_end = date(today.year, next_quarter_month, 1) - timedelta(days=1)

                    query = query.filter(
                        PayrollPeriod.start_date >= quarter_start,
                        PayrollPeriod.end_date <= quarter_end
                    )

                elif time_filter == 'year':
                    year_start = date(today.year, 1, 1)
                    year_end = date(today.year, 12, 31)
                    query = query.filter(
                        PayrollPeriod.start_date >= year_start,
                        PayrollPeriod.end_date <= year_end
                    )

                else:
                    return jsonify({"error": "Invalid time filter"}), 400

            # Execute query and process results
            payrun_data = []
            for transaction in query.all():
                employee = transaction.employees
                currency = transaction.currency

                # Get deductions
                deductions = db_session.query(Deduction).filter_by(
                    payroll_transaction_id=transaction.id
                ).all()
                total_deductions = sum(d.amount for d in deductions)

                # Get benefits
                benefits = db_session.query(Benefit).filter_by(
                    payroll_transaction_id=transaction.id
                ).all()
                total_benefits = sum(b.amount for b in benefits)

                # Get advance payments

                advance_payments = db_session.query(AdvanceRepayment) \
                    .filter(AdvanceRepayment.payroll_id == transaction.payroll_period_id) \
                    .join(AdvancePayment) \
                    .filter(AdvancePayment.employee_id == employee.id) \
                    .all()
                total_advance = sum(a.payment_amount for a in advance_payments)

                net_salary = transaction.gross_salary - total_deductions - total_advance + total_benefits

                payrun_data.append({
                    "payroll_period_id": transaction.payroll_period_id,
                    "payroll_period_name": transaction.payroll_period.payroll_period_name,
                    "period_start_date": transaction.payroll_period.start_date.strftime('%Y-%m-%d'),
                    "period_end_date": transaction.payroll_period.end_date.strftime('%Y-%m-%d'),
                    "employee": {
                        "id": employee.employee_id,
                        "name": f"{employee.first_name} {employee.last_name}",
                        "department": employee.department.department_name if employee.department else None
                    },
                    "currency": currency.user_currency,
                    "gross_salary": float(transaction.gross_salary),
                    "deductions": {
                        "total": float(total_deductions),
                        "items": [{
                            "name": d.deduction_type.name if d.deduction_type else "Other",
                            "amount": float(d.amount)
                        } for d in deductions]
                    },
                    "benefits": {
                        "total": float(total_benefits),
                        "items": [{
                            "name": b.benefit_type.name if b.benefit_type else "Other",
                            "amount": float(b.amount)
                        } for b in benefits]
                    },
                    "advance_payments": {
                        "total": float(total_advance),
                        "items": [{
                            "date": a.payment_date.strftime('%Y-%m-%d'),
                            "amount": float(a.payment_amount),

                        } for a in advance_payments]
                    },
                    "net_salary": float(net_salary),
                    "payment_status": transaction.payment_status.value,

                })

            # Group by currency for summary
            currency_totals = {}
            for item in payrun_data:
                curr = item['currency']
                if curr not in currency_totals:
                    currency_totals[curr] = {
                        "gross_salary": 0.0,
                        "total_deductions": 0.0,
                        "total_benefits": 0.0,
                        "total_advance": 0.0,
                        "net_salary": 0.0,
                        "employee_count": 0
                    }
                currency_totals[curr]["gross_salary"] += item["gross_salary"]
                currency_totals[curr]["total_deductions"] += item["deductions"]["total"]
                currency_totals[curr]["total_benefits"] += item["benefits"]["total"]
                currency_totals[curr]["total_advance"] += item["advance_payments"]["total"]
                currency_totals[curr]["net_salary"] += item["net_salary"]
                currency_totals[curr]["employee_count"] += 1

            return jsonify({

                "payrun_data": payrun_data,
                "currency_summary": currency_totals,
                "record_count": len(payrun_data)
            })

        except Exception as e:
            print(f'Error retrieving payrun data: {str(e)}')
            return jsonify({"error": str(e)}), 500


@api_routes.route('/api/inventory', methods=["GET"])
def get_inventory_data():
    # Authentication and validation
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as db_session:
        try:
            # Validate company
            company = db_session.query(Company).filter_by(api_key=api_key).first()
            if not company:
                return jsonify({"error": "Invalid API key"}), 403

            app_id = company.id

            # Get filter parameters
            category_id = request.args.get("category")
            subcategory_id = request.args.get("subcategory")
            location_id = request.args.get("location")
            brand_id = request.args.get("brand")
            status = request.args.get("status", "active")  # Default to active items
            low_stock = request.args.get("low_stock", "false").lower() == "true"
            start_date = request.args.get("start_date")
            end_date = request.args.get("end_date")
            time_filter = request.args.get("time_filter")  # today, week, month, quarter, year
            currency_id = request.args.get("currency")

            # Get base currency info if needed for valuation
            if not currency_id:
                base_currency_info = get_base_currency(db_session, app_id)
                if not base_currency_info:
                    return jsonify({"error": "Base currency not defined for this company"}), 400
                currency_id = base_currency_info["base_currency_id"]
                base_currency = base_currency_info["base_currency"]

            # Process date filters
            if start_date and end_date:
                try:
                    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                except ValueError:
                    return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
            elif time_filter:
                today = date.today()
                if time_filter == 'today':
                    start_date_obj = today
                    end_date_obj = today
                elif time_filter == 'week':
                    start_date_obj = today - timedelta(days=today.weekday())
                    end_date_obj = today
                elif time_filter == 'month':
                    start_date_obj = today.replace(day=1)
                    end_date_obj = today
                elif time_filter == 'quarter':
                    quarter_month = ((today.month - 1) // 3) * 3 + 1
                    start_date_obj = today.replace(month=quarter_month, day=1)
                    end_date_obj = today
                elif time_filter == 'year':
                    start_date_obj = today.replace(month=1, day=1)
                    end_date_obj = today
                else:
                    return jsonify({"error": "Invalid time filter"}), 400
            else:
                start_date_obj = None
                end_date_obj = None

            # Base query for inventory items with their variations and current stock
            # Now we need to join with InventoryEntryLineItem instead of InventoryEntry
            query = db_session.query(
                InventoryItem,
                InventoryItemVariationLink,
                func.coalesce(
                    func.sum(
                        case(
                            (
                                and_(
                                    InventoryEntry.stock_movement.in_(['in', 'transfer']),
                                    or_(
                                        InventoryEntry.to_location.isnot(None),
                                        # For transfers, only count if to_location exists
                                        InventoryEntry.stock_movement == 'in'  # For regular in movements
                                    ),
                                    or_(
                                        not_(start_date_obj and end_date_obj),
                                        and_(
                                            InventoryEntry.transaction_date >= start_date_obj,
                                            InventoryEntry.transaction_date <= end_date_obj
                                        )
                                    )
                                ),
                                InventoryEntryLineItem.quantity
                            ),
                            (
                                and_(
                                    InventoryEntry.stock_movement.in_(['out', 'missing', 'transfer']),
                                    or_(
                                        and_(
                                            InventoryEntry.stock_movement == 'transfer',
                                            InventoryEntry.from_location.isnot(None)  # For transfers out
                                        ),
                                        InventoryEntry.stock_movement.in_(['out', 'missing'])
                                        # For regular out movements
                                    ),
                                    or_(
                                        not_(start_date_obj and end_date_obj),
                                        and_(
                                            InventoryEntry.transaction_date >= start_date_obj,
                                            InventoryEntry.transaction_date <= end_date_obj
                                        )
                                    )
                                ),
                                -InventoryEntryLineItem.quantity
                            ),
                            else_=0
                        )
                    ), 0
                ).label('current_stock')
            ).join(
                InventoryItemVariationLink,
                InventoryItemVariationLink.inventory_item_id == InventoryItem.id
            ).outerjoin(
                InventoryEntryLineItem,
                InventoryEntryLineItem.item_id == InventoryItemVariationLink.id
            ).outerjoin(
                InventoryEntry,
                InventoryEntry.id == InventoryEntryLineItem.inventory_entry_id
            ).filter(
                InventoryItem.app_id == app_id,
                InventoryItem.status == status
            ).group_by(
                InventoryItem.id,
                InventoryItemVariationLink.id
            )

            # Apply category filter
            if category_id:
                query = query.filter(InventoryItem.item_category_id == category_id)

            # Apply subcategory filter
            if subcategory_id:
                query = query.filter(InventoryItem.item_subcategory_id == subcategory_id)

            # Apply brand filter
            if brand_id:
                query = query.filter(InventoryItem.brand_id == brand_id)

            # Apply location filter - this is complex due to the many-to-many relationship
            if location_id:
                # Subquery to find variation links with stock in the specified location
                location_subquery = db_session.query(
                    BatchVariationLink.item_id
                ).filter(
                    BatchVariationLink.location_id == location_id,
                    BatchVariationLink.quantity > 0
                ).distinct().subquery()

                query = query.join(
                    location_subquery,
                    location_subquery.c.item_id == InventoryItemVariationLink.id
                )

            # Execute the query and process results
            inventory_data = []
            for item, variation_link, current_stock in query.all():
                # Skip low stock items if not requested
                if low_stock and not (current_stock < item.reorder_point):
                    continue

                # Get batch information for this variation
                batches = db_session.query(
                    BatchVariationLink.quantity,
                    BatchVariationLink.unit_cost,
                    BatchVariationLink.currency_id,
                    BatchVariationLink.transaction_date,
                    Batch.batch_number,
                    InventoryLocation.location.label('location_name')
                ).join(
                    Batch,
                    Batch.id == BatchVariationLink.batch_id
                ).outerjoin(
                    InventoryLocation,
                    InventoryLocation.id == BatchVariationLink.location_id
                ).filter(
                    BatchVariationLink.item_id == variation_link.id,
                    BatchVariationLink.quantity > 0,
                    BatchVariationLink.app_id == app_id
                ).order_by(
                    BatchVariationLink.transaction_date.asc()
                ).all()

                # Calculate FIFO value for the item variation
                item_value = Decimal('0.0')
                remaining_qty = Decimal(current_stock)

                for batch in batches:
                    if remaining_qty <= 0:
                        break

                    qty_to_use = min(remaining_qty, Decimal(str(batch.quantity)))

                    # Convert to requested currency if needed
                    if batch.currency_id == currency_id:
                        cost = Decimal(str(batch.unit_cost))
                    else:
                        rate = get_exchange_rate(db_session, batch.currency_id, currency_id, app_id)
                        if not rate:
                            continue
                        cost = Decimal(str(batch.unit_cost)) * Decimal(str(rate))

                    item_value += qty_to_use * cost
                    remaining_qty -= qty_to_use

                # Get item variations and attributes
                variations = []
                if variation_link.inventory_item_variation:
                    attribute = db_session.query(InventoryItemAttribute).get(
                        variation_link.inventory_item_variation.attribute_id)
                    variations.append({
                        "attribute_name": attribute.attribute_name if attribute else None,
                        "variation_name": variation_link.inventory_item_variation.variation_name
                    })

                # Get locations where this variation has stock
                locations = list({batch.location_name for batch in batches if batch.location_name})

                inventory_data.append({
                    "item_id": item.id,
                    "variation_id": variation_link.id,
                    "item_name": item.item_name,
                    "item_code": item.item_code,
                    "description": item.item_description,
                    "category": item.inventory_category.category_name if item.inventory_category else None,
                    "subcategory": item.inventory_subcategory.subcategory_name if item.inventory_subcategory else None,
                    "brand": item.brand.name if item.brand else None,
                    "locations": locations,
                    "current_stock": float(current_stock),
                    "reorder_point": float(item.reorder_point),
                    "stock_value": float(item_value),
                    "currency_id": currency_id,
                    "status": item.status,
                    "variations": variations,
                    "batches": [{
                        "batch_number": batch.batch_number,
                        "quantity": float(batch.quantity),
                        "unit_cost": float(batch.unit_cost),
                        "currency_id": batch.currency_id,
                        "location": batch.location_name,
                        "transaction_date": batch.transaction_date.strftime(
                            '%Y-%m-%d') if batch.transaction_date else None
                    } for batch in batches]
                })

            # Calculate summary metrics
            total_items = len(inventory_data)
            total_stock_value = sum(item["stock_value"] for item in inventory_data)
            low_stock_count = sum(1 for item in inventory_data if item["current_stock"] < item["reorder_point"])
            out_of_stock_count = sum(1 for item in inventory_data if item["current_stock"] <= 0)

            print(
                f'Inventory data is {inventory_data} and total_stock_value is {total_stock_value} and low_stock_count{low_stock_count}')

            return jsonify({
                "company": company.name,
                "inventory_data": inventory_data,
                "summary": {
                    "total_items": total_items,
                    "total_stock_value": float(total_stock_value),
                    "low_stock_items": low_stock_count,
                    "out_of_stock_items": out_of_stock_count,
                    "currency_id": currency_id
                },
                "record_count": total_items
            })

        except Exception as e:
            print(f'Error retrieving inventory data: {str(e)}')
            return jsonify({"error": str(e)}), 500


# @api_routes.route('/api/inventory/transactions', methods=["GET"])
# @cached_route(timeout=300, key_func=inventory_dashboard_cache_key)
# def get_inventory_transactions():
#     # Authentication and validation
#     api_key = request.headers.get("X-API-Key")
#     if not api_key:
#         return jsonify({"error": "API key is missing"}), 401
#
#     # Get filter parameters
#     start_date = request.args.get("start_date")
#     end_date = request.args.get("end_date")
#     time_filter = request.args.get("time_filter")
#     category_id = request.args.get("category")
#     subcategory_id = request.args.get("subcategory")
#     location_id = request.args.get("location")
#     brand_id = request.args.get("brand")
#     status = request.args.get("status", "active")
#     item_id = request.args.get("item")
#     variation_id = request.args.get("variation_id")
#     movement_type = request.args.get("movement_type")
#     currency_filter = request.args.get("currency")
#     posted_status = request.args.get("posted")
#
#
#     with Session() as db_session:
#         try:
#             # Validate company
#             company = db_session.query(Company).filter_by(api_key=api_key).first()
#             if not company:
#                 return jsonify({"error": "Invalid API key"}), 403
#
#             app_id = company.id
#
#             base_currency_info = get_base_currency(db_session, app_id)
#             if not base_currency_info:
#                 return jsonify({"error": "Base currency not defined for this company"}), 400
#
#             base_currency_id = base_currency_info["base_currency_id"]
#             base_currency = base_currency_info["base_currency"]
#
#             inventory_data = []
#             # Determine the effective end date for stock calculation
#             if time_filter:
#                 # Use the function for time-based filters
#                 start_date_obj, end_date_obj = get_date_range_from_filter(
#                     time_filter,
#                     custom_start=start_date,
#                     custom_end=end_date
#                 )
#                 logger.info(f"DATE RANGE: Using time_filter '{time_filter}' -> {start_date_obj} to {end_date_obj}")
#             elif start_date and end_date:
#                 # Use explicit dates if provided
#                 try:
#                     start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
#                     end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
#                 except ValueError:
#                     return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
#             else:
#                 # Default case - use last 30 days
#                 start_date_obj, end_date_obj = get_date_range_from_filter('default')
#
#             # SINGLE OPTIMIZED QUERY: Get ALL transactions up to end date
#             base_query = db_session.query(
#                 InventoryTransactionDetail,
#                 InventoryLocation.location.label('location_name'),
#                 InventoryItem.item_name,
#                 InventoryItem.item_code,
#                 InventoryItem.reorder_point,
#                 InventoryItem.item_category_id,
#                 InventoryCategory.category_name.label('category_name'),
#                 InventoryItem.item_subcategory_id,
#                 InventorySubCategory.subcategory_name.label('subcategory_name'),
#                 InventoryItem.brand_id,
#                 Brand.name.label('brand_name'),
#                 InventoryItemVariation.variation_name,
#                 InventoryEntry.inventory_source,
#                 InventoryEntry.source_type,
#                 InventoryEntry.source_id
#             ).join(
#                 InventoryLocation,
#                 InventoryTransactionDetail.location_id == InventoryLocation.id
#             ).join(
#                 InventoryItemVariationLink,
#                 InventoryTransactionDetail.item_id == InventoryItemVariationLink.id
#             ).join(
#                 InventoryItem,
#                 InventoryItemVariationLink.inventory_item_id == InventoryItem.id
#             ).outerjoin(
#                 InventoryItemVariation,
#                 InventoryItemVariationLink.inventory_item_variation_id == InventoryItemVariation.id
#             ).outerjoin(
#                 InventoryCategory,
#                 InventoryItem.item_category_id == InventoryCategory.id
#             ).outerjoin(
#                 InventorySubCategory,
#                 InventoryItem.item_subcategory_id == InventorySubCategory.id
#             ).outerjoin(
#                 Brand,
#                 InventoryItem.brand_id == Brand.id
#             ).outerjoin(
#                 InventoryEntryLineItem,
#                 InventoryTransactionDetail.inventory_entry_line_item_id == InventoryEntryLineItem.id
#             ).outerjoin(
#                 InventoryEntry,
#                 InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
#             ).filter(
#                 InventoryTransactionDetail.app_id == app_id,
#                 InventoryTransactionDetail.transaction_date <= end_date_obj  # ALL transactions up to end date
#             )
#
#             # Apply basic filters
#             if movement_type:
#                 base_query = base_query.filter(InventoryTransactionDetail.movement_type == movement_type)
#
#             if currency_filter:
#                 base_query = base_query.filter(InventoryTransactionDetail.currency_id == currency_filter)
#
#             if posted_status:
#                 if posted_status.lower() == 'true':
#                     base_query = base_query.filter(InventoryTransactionDetail.is_posted_to_ledger == True)
#                 elif posted_status.lower() == 'false':
#                     base_query = base_query.filter(InventoryTransactionDetail.is_posted_to_ledger == False)
#
#             if location_id:
#                 base_query = base_query.filter(InventoryTransactionDetail.location_id == location_id)
#
#             # Apply complex filters
#             if status and status != 'all':
#                 base_query = base_query.filter(InventoryItem.status == status)
#
#             if item_id:
#                 base_query = base_query.filter(InventoryTransactionDetail.item_id == int(item_id))
#
#             if variation_id:
#                 base_query = base_query.filter(InventoryItemVariationLink.id == variation_id)
#
#             if category_id:
#                 base_query = base_query.filter(InventoryItem.item_category_id == category_id)
#
#             if subcategory_id:
#                 base_query = base_query.filter(InventoryItem.item_subcategory_id == subcategory_id)
#
#             if brand_id:
#                 base_query = base_query.filter(InventoryItem.brand_id == brand_id)
#
#             # Execute single optimized query
#             logger.info("EXECUTING SINGLE OPTIMIZED QUERY - All transactions up to end date")
#             all_results = base_query.all()
#             logger.info(f"QUERY RESULTS: Found {len(all_results)} total transactions up to {end_date_obj}")
#
#             # STEP 1: Calculate ACTUAL stock as of end date from ALL results
#             actual_stock_quantities = {}
#             item_total_stock_qty = {}
#             item_variation_ids = set()
#             location_ids_from_transactions = set()
#             out_of_stock_items = 0
#             for result in all_results:
#                 transaction_detail = result[0]
#                 if transaction_detail.item_id and transaction_detail.location_id:
#                     item_loc = (transaction_detail.item_id, transaction_detail.location_id)
#
#                     item_var = transaction_detail.item_id
#
#                     if item_loc not in actual_stock_quantities:
#                         actual_stock_quantities[item_loc] = 0
#                     if item_var not in item_total_stock_qty:
#                         item_total_stock_qty[item_var] = 0
#
#                     actual_stock_quantities[item_loc] += transaction_detail.quantity
#                     item_total_stock_qty[item_var] += transaction_detail.quantity
#
#                     item_variation_ids.add(transaction_detail.item_id)
#                     location_ids_from_transactions.add(transaction_detail.location_id)
#             logger.info(f"STOCK CALCULATION: Processed {len(actual_stock_quantities)} item-location combinations")
#             logger.info(f"STOCK CALCULATION: Sample stock values: {dict(list(actual_stock_quantities.items())[:3])}")
#             logger.info(f'Actual stock qties is {item_total_stock_qty}')
#
#             inventory_data.append({"total_items": len(item_variation_ids)})
#
#             # Get average costs from InventorySummary
#             average_costs = {}
#             if item_variation_ids:
#                 avg_cost_query = db_session.query(
#                     InventorySummary.item_id,
#                     InventorySummary.location_id,
#                     InventorySummary.average_cost
#                 ).filter(
#                     InventorySummary.app_id == app_id,
#                     InventorySummary.item_id.in_(list(item_variation_ids))
#                 )
#
#                 if location_id:
#                     avg_cost_query = avg_cost_query.filter(InventorySummary.location_id == int(location_id))
#                 elif location_ids_from_transactions:
#                     avg_cost_query = avg_cost_query.filter(
#                         InventorySummary.location_id.in_(list(location_ids_from_transactions)))
#
#                 for cost_item in avg_cost_query.all():
#                     cost_key = (cost_item.item_id, cost_item.location_id)
#                     average_costs[cost_key] = float(cost_item.average_cost or 0)
#             total_stock_value = 0
#             cost = 0
#             for var_loc in actual_stock_quantities:
#
#                 for cost_price in average_costs:
#                     if var_loc == cost_price:
#                         cost = float(average_costs[cost_price])
#                 total_stock_value += float(actual_stock_quantities[var_loc]) * cost
#
#             for var_id in item_total_stock_qty:
#                 if item_total_stock_qty[var_id] == 0:
#                     out_of_stock_items+=1
#
#             inventory_data.append({"out_of_stock_items": out_of_stock_items})
#             inventory_data.append({"total_stock_value": float(total_stock_value)})
#             # STEP 2: Filter results for movement charts based on date range
#             movement_transactions = []
#             if start_date and end_date:
#                 try:
#                     start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
#                     end_date_obj_movement = datetime.strptime(end_date, '%Y-%m-%d').date()
#
#                     for result in all_results:
#                         transaction_detail = result[0]
#                         if start_date_obj <= transaction_detail.transaction_date <= end_date_obj_movement:
#                             movement_transactions.append(result)
#
#                     logger.info(
#                         f"MOVEMENT FILTER: {len(movement_transactions)} transactions within date range {start_date} to {end_date}")
#
#                 except ValueError:
#                     return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
#             elif time_filter:
#                 today = date.today()
#                 movement_transactions = []
#
#                 if time_filter == 'today':
#                     filter_date = today
#                     for result in all_results:
#                         if result[0].transaction_date == filter_date:
#                             movement_transactions.append(result)
#                 elif time_filter == 'yesterday':
#                     filter_date = today - timedelta(days=1)
#                     for result in all_results:
#                         if result[0].transaction_date == filter_date:
#                             movement_transactions.append(result)
#                 elif time_filter == 'week':
#                     filter_date = today - timedelta(days=today.weekday())
#                     for result in all_results:
#                         if result[0].transaction_date >= filter_date:
#                             movement_transactions.append(result)
#                 elif time_filter == 'month':
#                     filter_date = today.replace(day=1)
#                     for result in all_results:
#                         if result[0].transaction_date >= filter_date:
#                             movement_transactions.append(result)
#                 elif time_filter == 'last_month':
#                     first_day_current_month = today.replace(day=1)
#                     filter_date = (first_day_current_month - timedelta(days=1)).replace(day=1)
#                     end_date_movement = first_day_current_month - timedelta(days=1)
#                     for result in all_results:
#                         if filter_date <= result[0].transaction_date <= end_date_movement:
#                             movement_transactions.append(result)
#                 elif time_filter == 'quarter':
#                     quarter_month = ((today.month - 1) // 3) * 3 + 1
#                     filter_date = today.replace(month=quarter_month, day=1)
#                     for result in all_results:
#                         if result[0].transaction_date >= filter_date:
#                             movement_transactions.append(result)
#                 elif time_filter == 'year':
#                     filter_date = today.replace(month=1, day=1)
#                     for result in all_results:
#                         if result[0].transaction_date >= filter_date:
#                             movement_transactions.append(result)
#
#                 logger.info(
#                     f"MOVEMENT FILTER: {len(movement_transactions)} transactions for time filter '{time_filter}'")
#             else:
#                 # If no date filter, use all transactions for movement
#                 movement_transactions = all_results
#                 logger.info(f"MOVEMENT FILTER: Using all {len(movement_transactions)} transactions (no date filter)")
#
#             # ✅ FIXED: Format results - include stock positions even when no transactions in period
#             transactions = []
#
#             # Track which item-location pairs we've already processed
#             processed_item_location_pairs = set()
#
#             # First, process movement transactions (if any)
#             for result in movement_transactions:
#                 try:
#                     (transaction_detail, location_name, item_name, item_code, reorder_point,
#                      category_id, category_name, subcategory_id, subcategory_name, brand_id,
#                      brand_name, variation_name, inventory_source, source_type, source_id) = result
#
#                     item_variation_id = transaction_detail.item_id
#                     loc_id = transaction_detail.location_id
#
#                     if not item_variation_id or not loc_id:
#                         continue
#
#                     # Get ACTUAL stock as of end date (from our stock calculation)
#                     stock_key = (item_variation_id, loc_id)
#                     current_stock = actual_stock_quantities.get(stock_key, 0)
#                     average_cost = average_costs.get(stock_key, 0)
#
#                     # Calculate value for THIS SPECIFIC TRANSACTION
#                     transaction_quantity = float(transaction_detail.quantity or 0)
#                     transaction_value = transaction_quantity * average_cost
#
#                     # Currency conversion
#                     use_converted = not currency_filter and transaction_detail.currency_id != base_currency_id
#                     unit_cost = float(transaction_detail.unit_cost or 0)
#                     total_cost = float(transaction_detail.total_cost or 0)
#
#                     if use_converted and transaction_detail.currency_id:
#                         unit_cost = get_converted_cost(db_session, unit_cost, transaction_detail.currency_id,
#                                                        base_currency_id, app_id)
#                         total_cost = get_converted_cost(db_session, total_cost, transaction_detail.currency_id,
#                                                         base_currency_id, app_id)
#
#                     transaction_data = {
#                         "id": transaction_detail.id,
#                         "inventory_entry_line_item_id": transaction_detail.inventory_entry_line_item_id,
#                         "transaction_date": transaction_detail.transaction_date.strftime(
#                             '%Y-%m-%d') if transaction_detail.transaction_date else None,
#                         "movement_type": transaction_detail.movement_type,
#                         "quantity": transaction_quantity,
#                         "unit_cost": unit_cost,
#                         "total_cost": total_cost,
#                         "current_stock": current_stock,  # ✅ ACTUAL stock as of end date
#                         "average_cost": average_cost,
#                         "total_value": transaction_value,
#                         "currency_id": base_currency_id if not currency_filter else transaction_detail.currency_id,
#                         "currency": base_currency,
#                         "is_posted_to_ledger": transaction_detail.is_posted_to_ledger,
#                         "created_at": transaction_detail.created_at.strftime(
#                             '%Y-%m-%d %H:%M:%S') if transaction_detail.created_at else None,
#                         "location_id": loc_id,
#                         "location_name": location_name,
#                         "inventory_source": inventory_source,
#                         "source_type": source_type,
#                         "source_id": source_id,
#                         "item_id": item_variation_id,
#                         "item_name": item_name,
#                         "item_code": item_code,
#                         "reorder": reorder_point,
#                         "category_id": category_id,
#                         "category": category_name,
#                         "subcategory_id": subcategory_id,
#                         "subcategory": subcategory_name,
#                         "brand_id": brand_id,
#                         "brand": brand_name,
#                         "variation_id": item_variation_id,
#                         "variation_name": variation_name,
#                     }
#
#                     transactions.append(transaction_data)
#                     processed_item_location_pairs.add((item_variation_id, loc_id))
#
#                 except Exception as e:
#                     logger.error(f"Error processing transaction {transaction_detail.id}: {str(e)}")
#                     continue
#
#             # ✅ NEW: Add stock positions for items that have stock but no transactions in the period
#             if actual_stock_quantities:
#                 logger.info(f"CHECKING FOR STOCK POSITIONS: {len(actual_stock_quantities)} item-locations with stock")
#
#                 # Get item details for all items that have stock but weren't included in movement transactions
#                 stock_items_to_process = []
#                 for stock_key in actual_stock_quantities.keys():
#                     if stock_key not in processed_item_location_pairs:
#                         stock_items_to_process.append(stock_key)
#
#                 if stock_items_to_process:
#                     logger.info(f"ADDING {len(stock_items_to_process)} STOCK POSITIONS (no transactions in period)")
#
#                     # Use a simpler approach - query item/location details separately
#                     item_ids = list(set([key[0] for key in stock_items_to_process]))
#                     location_ids = list(set([key[1] for key in stock_items_to_process]))
#
#                     # Query item details
#                     items_query = db_session.query(
#                         InventoryItemVariationLink.id,
#                         InventoryItem.item_name,
#                         InventoryItem.item_code,
#                         InventoryItem.reorder_point,
#                         InventoryItem.item_category_id,
#                         InventoryCategory.category_name.label('category_name'),
#                         InventoryItem.item_subcategory_id,
#                         InventorySubCategory.subcategory_name.label('subcategory_name'),
#                         InventoryItem.brand_id,
#                         Brand.name.label('brand_name'),
#                         InventoryItemVariation.variation_name
#                     ).join(
#                         InventoryItem,
#                         InventoryItemVariationLink.inventory_item_id == InventoryItem.id
#                     ).outerjoin(
#                         InventoryItemVariation,
#                         InventoryItemVariationLink.inventory_item_variation_id == InventoryItemVariation.id
#                     ).outerjoin(
#                         InventoryCategory,
#                         InventoryItem.item_category_id == InventoryCategory.id
#                     ).outerjoin(
#                         InventorySubCategory,
#                         InventoryItem.item_subcategory_id == InventorySubCategory.id
#                     ).outerjoin(
#                         Brand,
#                         InventoryItem.brand_id == Brand.id
#                     ).filter(
#                         InventoryItemVariationLink.id.in_(item_ids)
#                     )
#
#                     # Apply the same filters as the main query
#                     if status and status != 'all':
#                         items_query = items_query.filter(InventoryItem.status == status)
#                     if category_id:
#                         items_query = items_query.filter(InventoryItem.item_category_id == category_id)
#                     if subcategory_id:
#                         items_query = items_query.filter(InventoryItem.item_subcategory_id == subcategory_id)
#                     if brand_id:
#                         items_query = items_query.filter(InventoryItem.brand_id == brand_id)
#
#                     item_details = {item.id: item for item in items_query.all()}
#
#                     # Query location details
#                     locations_query = db_session.query(
#                         InventoryLocation.id,
#                         InventoryLocation.location.label('location_name')
#                     ).filter(
#                         InventoryLocation.id.in_(location_ids)
#                     )
#                     location_details = {loc.id: loc for loc in locations_query.all()}
#
#                     # Create stock position records
#                     for stock_key in stock_items_to_process:
#                         item_variation_id, loc_id = stock_key
#
#                         item_detail = item_details.get(item_variation_id)
#                         location_detail = location_details.get(loc_id)
#
#                         if not item_detail or not location_detail:
#                             continue
#
#                         current_stock = actual_stock_quantities.get(stock_key, 0)
#                         average_cost = average_costs.get(stock_key, 0)
#
#                         # Only include items with actual stock
#                         if current_stock != 0:
#                             stock_data = {
#                                 "id": None,
#                                 "inventory_entry_line_item_id": None,
#                                 "transaction_date": end_date_obj.strftime('%Y-%m-%d'),
#                                 "movement_type": "stock_position",
#                                 "quantity": 0,
#                                 "unit_cost": 0,
#                                 "total_cost": 0,
#                                 "current_stock": current_stock,
#                                 "average_cost": average_cost,
#                                 "total_value": current_stock * average_cost,
#                                 "currency_id": base_currency_id,
#                                 "currency": base_currency,
#                                 "is_posted_to_ledger": True,
#                                 "created_at": None,
#                                 "location_id": loc_id,
#                                 "location_name": location_detail.location_name,
#                                 "inventory_source": None,
#                                 "source_type": None,
#                                 "source_id": None,
#                                 "item_id": item_variation_id,
#                                 "item_name": item_detail.item_name,
#                                 "item_code": item_detail.item_code,
#                                 "reorder": item_detail.reorder_point,
#                                 "category_id": item_detail.item_category_id,
#                                 "category": item_detail.category_name,
#                                 "subcategory_id": item_detail.item_subcategory_id,
#                                 "subcategory": item_detail.subcategory_name,
#                                 "brand_id": item_detail.brand_id,
#                                 "brand": item_detail.brand_name,
#                                 "variation_id": item_variation_id,
#                                 "variation_name": item_detail.variation_name,
#                             }
#                             transactions.append(stock_data)
#
#             logger.info(
#                 f"FINAL RESULT: {len(transactions)} records (transactions + stock positions) with CORRECT as-of-date stock values")
#             if transactions:
#                 logger.info(
#                     f"SAMPLE STOCK: First 3 records stock values: {[t['current_stock'] for t in transactions[:3]]}")
#             inventory_data.append({"total_transactions": len(transactions)})
#             inventory_data.append({"transactions": transactions})
#             response = jsonify(inventory_data)
#             response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
#             response.headers['Pragma'] = 'no-cache'
#             response.headers['Expires'] = '0'
#             return response
#
#         except Exception as e:
#             logger.error(f'Error retrieving inventory transactions: {str(e)}\n{traceback.format_exc()}')
#             return jsonify({"error": str(e)}), 500


@api_routes.route('/api/inventory/transactions', methods=["GET"])
def get_inventory_transactions():
    # Authentication and validation
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    # Get filter parameters (remove date-related ones)
    category_id = request.args.get("category")
    subcategory_id = request.args.get("subcategory")
    location_id = request.args.get("location")
    brand_id = request.args.get("brand")
    status = request.args.get("status", "active")
    item_id = request.args.get("item")
    variation_id = request.args.get("variation_id")

    with Session() as db_session:
        try:
            # Validate company
            company = db_session.query(Company).filter_by(api_key=api_key).first()
            if not company:
                return jsonify({"error": "Invalid API key"}), 403

            app_id = company.id

            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Get ALL transactions - no date filtering for current stock
            base_query = db_session.query(
                InventoryTransactionDetail,
                InventoryLocation.location.label('location_name'),
                InventoryItem.item_name,
                InventoryItem.item_code,
                InventoryItem.reorder_point,
                InventoryItem.item_category_id,
                InventoryCategory.category_name.label('category_name'),
                InventoryItem.item_subcategory_id,
                InventorySubCategory.subcategory_name.label('subcategory_name'),
                InventoryItem.brand_id,
                Brand.name.label('brand_name'),
                InventoryItemVariation.variation_name,
                InventoryEntry.inventory_source,
                InventoryEntry.source_type,
                InventoryEntry.source_id
            ).join(
                InventoryLocation,
                InventoryTransactionDetail.location_id == InventoryLocation.id
            ).join(
                InventoryItemVariationLink,
                InventoryTransactionDetail.item_id == InventoryItemVariationLink.id
            ).join(
                InventoryItem,
                InventoryItemVariationLink.inventory_item_id == InventoryItem.id
            ).outerjoin(
                InventoryItemVariation,
                InventoryItemVariationLink.inventory_item_variation_id == InventoryItemVariation.id
            ).outerjoin(
                InventoryCategory,
                InventoryItem.item_category_id == InventoryCategory.id
            ).outerjoin(
                InventorySubCategory,
                InventoryItem.item_subcategory_id == InventorySubCategory.id
            ).outerjoin(
                Brand,
                InventoryItem.brand_id == Brand.id
            ).outerjoin(
                InventoryEntryLineItem,
                InventoryTransactionDetail.inventory_entry_line_item_id == InventoryEntryLineItem.id
            ).outerjoin(
                InventoryEntry,
                InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
            ).filter(
                InventoryTransactionDetail.app_id == app_id
            )

            # Apply filters
            if status and status != 'all':
                base_query = base_query.filter(InventoryItem.status == status)

            if item_id:
                base_query = base_query.filter(InventoryTransactionDetail.item_id == int(item_id))

            if variation_id:
                base_query = base_query.filter(InventoryItemVariationLink.id == variation_id)

            if category_id:
                base_query = base_query.filter(InventoryItem.item_category_id == category_id)

            if subcategory_id:
                base_query = base_query.filter(InventoryItem.item_subcategory_id == subcategory_id)

            if brand_id:
                base_query = base_query.filter(InventoryItem.brand_id == brand_id)

            if location_id:
                base_query = base_query.filter(InventoryTransactionDetail.location_id == location_id)

            # Execute query
            all_results = base_query.all()

            # Calculate ACTUAL stock from ALL transactions
            actual_stock_quantities = {}
            item_variation_ids = set()
            out_of_stock_items = 0

            for result in all_results:
                transaction_detail = result[0]
                if transaction_detail.item_id and transaction_detail.location_id:
                    item_loc = (transaction_detail.item_id, transaction_detail.location_id)

                    if item_loc not in actual_stock_quantities:
                        actual_stock_quantities[item_loc] = 0

                    actual_stock_quantities[item_loc] += transaction_detail.quantity
                    item_variation_ids.add(transaction_detail.item_id)

            total_unique_items = len(item_variation_ids)

            # Count out of stock items
            for item_loc, quantity in actual_stock_quantities.items():
                item_id, loc_id = item_loc
                if location_id and int(loc_id) != int(location_id):
                    continue
                if quantity <= 0:
                    out_of_stock_items += 1

            # Adjust total_items based on location filter
            if location_id:
                items_in_location = set()
                for item_loc in actual_stock_quantities.keys():
                    item_id, loc_id = item_loc
                    if int(loc_id) == int(location_id):
                        items_in_location.add(item_id)
                total_unique_items = len(items_in_location)

            # Build response - simplified for warehouse
            transactions = []
            processed_item_location_pairs = set()

            for result in all_results:
                try:
                    (transaction_detail, location_name, item_name, item_code, reorder_point,
                     category_id, category_name, subcategory_id, subcategory_name, brand_id,
                     brand_name, variation_name, inventory_source, source_type, source_id) = result

                    item_variation_id = transaction_detail.item_id
                    loc_id = transaction_detail.location_id

                    if not item_variation_id or not loc_id:
                        continue

                    stock_key = (item_variation_id, loc_id)
                    current_stock = actual_stock_quantities.get(stock_key, 0)

                    transaction_data = {
                        "id": transaction_detail.id,
                        "transaction_date": transaction_detail.transaction_date.strftime('%Y-%m-%d') if transaction_detail.transaction_date else None,
                        "movement_type": transaction_detail.movement_type,
                        "quantity": float(transaction_detail.quantity or 0),
                        "current_stock": current_stock,
                        "location_id": loc_id,
                        "location_name": location_name,
                        "inventory_source": inventory_source,
                        "source_type": source_type,
                        "source_id": source_id,
                        "item_id": item_variation_id,
                        "item_name": item_name,
                        "item_code": item_code,
                        "category": category_name,
                        "subcategory": subcategory_name,
                        "brand": brand_name,
                        "variation_name": variation_name,
                        "reference_number": None  # Add if needed from InventoryEntry.reference
                    }

                    transactions.append(transaction_data)
                    processed_item_location_pairs.add((item_variation_id, loc_id))

                except Exception as e:
                    logger.error(f"Error processing transaction: {str(e)}")
                    continue

            # Add stock positions for items with no transactions
            if actual_stock_quantities:
                stock_items_to_process = []
                for stock_key in actual_stock_quantities.keys():
                    item_id, loc_id = stock_key
                    if location_id and int(loc_id) != int(location_id):
                        continue
                    if stock_key not in processed_item_location_pairs:
                        stock_items_to_process.append(stock_key)

                if stock_items_to_process:
                    item_ids = list(set([key[0] for key in stock_items_to_process]))
                    location_ids = list(set([key[1] for key in stock_items_to_process]))

                    items_query = db_session.query(
                        InventoryItemVariationLink.id,
                        InventoryItem.item_name,
                        InventoryItem.item_code,
                        InventoryItem.item_category_id,
                        InventoryCategory.category_name.label('category_name'),
                        InventoryItem.item_subcategory_id,
                        InventorySubCategory.subcategory_name.label('subcategory_name'),
                        InventoryItem.brand_id,
                        Brand.name.label('brand_name'),
                        InventoryItemVariation.variation_name
                    ).join(
                        InventoryItem,
                        InventoryItemVariationLink.inventory_item_id == InventoryItem.id
                    ).outerjoin(
                        InventoryItemVariation,
                        InventoryItemVariationLink.inventory_item_variation_id == InventoryItemVariation.id
                    ).outerjoin(
                        InventoryCategory,
                        InventoryItem.item_category_id == InventoryCategory.id
                    ).outerjoin(
                        InventorySubCategory,
                        InventoryItem.item_subcategory_id == InventorySubCategory.id
                    ).outerjoin(
                        Brand,
                        InventoryItem.brand_id == Brand.id
                    ).filter(
                        InventoryItemVariationLink.id.in_(item_ids)
                    )

                    if status and status != 'all':
                        items_query = items_query.filter(InventoryItem.status == status)
                    if category_id:
                        items_query = items_query.filter(InventoryItem.item_category_id == category_id)
                    if subcategory_id:
                        items_query = items_query.filter(InventoryItem.item_subcategory_id == subcategory_id)
                    if brand_id:
                        items_query = items_query.filter(InventoryItem.brand_id == brand_id)

                    item_details = {item.id: item for item in items_query.all()}

                    locations_query = db_session.query(
                        InventoryLocation.id,
                        InventoryLocation.location.label('location_name')
                    ).filter(InventoryLocation.id.in_(location_ids))
                    location_details = {loc.id: loc for loc in locations_query.all()}

                    for stock_key in stock_items_to_process:
                        item_variation_id, loc_id = stock_key
                        item_detail = item_details.get(item_variation_id)
                        location_detail = location_details.get(loc_id)

                        if not item_detail or not location_detail:
                            continue

                        current_stock = actual_stock_quantities.get(stock_key, 0)

                        stock_data = {
                            "id": None,
                            "transaction_date": None,
                            "movement_type": "stock_position",
                            "quantity": 0,
                            "current_stock": current_stock,
                            "location_id": loc_id,
                            "location_name": location_detail.location_name,
                            "inventory_source": None,
                            "source_type": None,
                            "source_id": None,
                            "item_id": item_variation_id,
                            "item_name": item_detail.item_name,
                            "item_code": item_detail.item_code,
                            "category": item_detail.category_name,
                            "subcategory": item_detail.subcategory_name,
                            "brand": item_detail.brand_name,
                            "variation_name": item_detail.variation_name,
                            "reference_number": None
                        }
                        transactions.append(stock_data)

            inventory_data = [
                {"total_items": total_unique_items},
                {"out_of_stock_items": out_of_stock_items},
                {"total_stock_value": 0},  # Removed for WMS
                {"total_transactions": len(transactions)},
                {"transactions": transactions}
            ]

            response = jsonify(inventory_data)
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            return response

        except Exception as e:
            logger.error(f'Error retrieving inventory transactions: {str(e)}\n{traceback.format_exc()}')
            return jsonify({"error": str(e)}), 500

@api_routes.route('/api/user-modules/<int:user_id>')
@login_required
def get_user_modules(user_id):
    db_session = Session()

    if current_user.role != 'Admin':
        db_session.close()
        abort(403)

    try:
        modules = {}
        accesses = db_session.query(UserModuleAccess).filter_by(user_id=user_id).all()

        for access in accesses:
            modules[str(access.module_id)] = {
                'can_view': access.can_view,
                'can_edit': access.can_edit,
                'can_approve': access.can_approve,
                'can_administer': access.can_administer
            }

        return jsonify(modules)

    except Exception as e:
        logger.debug(f"Error while retrieving user modules for user_id {user_id}: {e}")
        return jsonify({'error': 'Failed to retrieve user permissions'}), 500

    finally:
        db_session.close()


@api_routes.route('/api/update-permission', methods=['POST'])
@login_required
def update_user_permission():
    db_session = Session()
    try:
        if current_user.role != 'Admin':
            abort(403)

        data = request.get_json()
        user_id = data.get('user_id')
        module_id = data.get('module_id')
        permission = data.get('permission')
        value = data.get('value')

        if not all([user_id, module_id, permission, value is not None]):
            return jsonify({'error': 'Missing required fields'}), 400

        valid_permissions = {'can_view', 'can_edit', 'can_approve', 'can_administer'}
        if permission not in valid_permissions:
            return jsonify({'error': 'Invalid permission'}), 400

        # Get or create the permission record
        access = db_session.query(UserModuleAccess).filter_by(
            user_id=user_id,
            module_id=module_id
        ).first()

        if not access:
            access = UserModuleAccess(
                user_id=user_id,
                module_id=module_id,
                **{permission: value}
            )
            db_session.add(access)
        else:
            setattr(access, permission, value)

        db_session.commit()
        return jsonify({'success': True, 'message': 'Permission updated'})

    except Exception as e:
        db_session.rollback()
        logger.debug(f'An error occurred {str(e)}')
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()


@api_routes.route('/api/expenses/transactions', methods=["GET"])
def api_expense_transactions():
    # Authentication and validation
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        logger.warning("API key is missing")
        return jsonify({"error": "API key is missing"}), 401

    # Get all possible filter parameters
    currency_filter = request.args.get("currency")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    time_filter = request.args.get("time_filter")
    category_id = request.args.get("category_id")
    subcategory_id = request.args.get("subcategory_id")
    payment_mode_id = request.args.get("payment_mode_id")
    vendor_id = request.args.get("vendor_id")
    project_id = request.args.get("project_id")

    db_session = Session()
    try:
        # Validate company
        company = db_session.query(Company).filter_by(api_key=api_key).first()
        if not company:
            logger.warning(f"Invalid API key: {api_key}")
            return jsonify({"error": "Invalid API key"}), 403

        app_id = company.id

        # Get base currency info
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            logger.error("Base currency not defined for company")
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency = base_currency_info["base_currency"]

        # Build base query with eager loading - now using JournalEntry
        query = db_session.query(JournalEntry).options(
            joinedload(JournalEntry.chart_of_accounts).joinedload(ChartOfAccounts.categories),
            joinedload(JournalEntry.journal).joinedload(Journal.currency),
            joinedload(JournalEntry.journal).joinedload(Journal.payment_mode),
            joinedload(JournalEntry.journal).joinedload(Journal.vendor),
            joinedload(JournalEntry.journal).joinedload(Journal.project),
            joinedload(JournalEntry.journal).joinedload(Journal.exchange_rate)
        ).filter(
            JournalEntry.app_id == app_id,
            Journal.status == "Posted",
            JournalEntry.chart_of_accounts.has(parent_account_type='Expense'),  # Use .has() method
            JournalEntry.dr_cr == 'D'  # Expense transactions are debits
        )

        # Apply currency filter
        if currency_filter:
            currency = db_session.query(Currency).filter_by(
                app_id=app_id,
                user_currency=currency_filter
            ).first()
            if currency:
                query = query.join(JournalEntry.journal).filter(Journal.currency_id == currency.id)

        # Apply category filter
        if category_id:
            query = query.join(JournalEntry.chart_of_accounts).filter(
                ChartOfAccounts.category_fk == category_id
            )

        # Apply subcategory filter
        if subcategory_id:
            query = query.filter(
                JournalEntry.subcategory_id == subcategory_id
            )

        # Apply payment mode filter
        if payment_mode_id:
            query = query.join(JournalEntry.journal).filter(
                Journal.payment_mode_id == payment_mode_id
            )

        # Apply vendor filter
        if vendor_id:
            query = query.join(JournalEntry.journal).filter(
                Journal.vendor_id == vendor_id
            )

        # Apply project filter
        if project_id:
            query = query.join(JournalEntry.journal).filter(
                Journal.project_id == project_id
            )

        # Apply date filters
        if start_date and end_date:
            try:
                start = datetime.strptime(start_date, '%Y-%m-%d').date()
                end = datetime.strptime(end_date, '%Y-%m-%d').date()
                query = query.join(JournalEntry.journal).filter(Journal.date.between(start, end))
            except ValueError:
                logger.error("Invalid date format provided")
                return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
        elif time_filter:
            filter_date, end_date_filter = get_filter_date(time_filter)
            if filter_date is None:
                logger.error(f"Invalid time filter: {time_filter}")
                return jsonify({"error": "Invalid time filter"}), 400

            if end_date_filter:  # For range filters like last_month
                query = query.join(JournalEntry.journal).filter(Journal.date.between(filter_date, end_date_filter))
            else:  # For single date filters
                query = query.join(JournalEntry.journal).filter(Journal.date >= filter_date)

        # Execute query and format results
        entries = query.order_by(Journal.date.desc()).all()
        results = []

        for entry in entries:
            try:
                journal = entry.journal
                original_amount = entry.amount
                tx_currency_id = journal.currency_id if journal else base_currency_id
                tx_currency = journal.currency.user_currency if journal and journal.currency else base_currency

                rate = journal.exchange_rate.rate if journal and journal.exchange_rate else None
                amount_in_base = float(
                    Decimal(original_amount) * rate) if rate and tx_currency_id != base_currency_id else float(
                    original_amount)

                results.append({
                    "id": entry.id,
                    "date": journal.date.strftime('%Y-%m-%d') if journal else None,
                    "category_id": entry.chart_of_accounts.categories.id if entry.chart_of_accounts and entry.chart_of_accounts.categories else None,
                    "category": entry.chart_of_accounts.categories.category if entry.chart_of_accounts and entry.chart_of_accounts.categories else None,
                    "subcategory": entry.chart_of_accounts.sub_category if entry.chart_of_accounts else None,
                    "currency": tx_currency,
                    "amount": float(original_amount),
                    "amount_in_base_currency": amount_in_base,
                    "base_currency": base_currency,
                    "description": entry.description,
                    "payment_mode": journal.payment_mode.payment_mode if journal and journal.payment_mode else None,
                    "vendor": journal.vendor.vendor_name if journal and journal.vendor else None,
                    "project": journal.project.name if journal and journal.project else None,
                    "date_added": journal.date_added.strftime('%Y-%m-%d') if journal and journal.date_added else None,
                    "journal_number": journal.journal_number if journal else None
                })
            except Exception as e:
                logger.error(f"Error processing journal entry {entry.id}: {str(e)}")
                continue

        logger.info(f"Returning {len(results)} expense transactions")
        return jsonify(results)

    except Exception as e:
        logger.error(f"Error in expense transactions endpoint: {str(e)}", exc_info=True)
        return jsonify({"error": "Something went wrong", "details": str(e)}), 500
    finally:
        db_session.close()


@api_routes.route('/api/chart_of_accounts/cash_and_bank', methods=["GET"])
@login_required
def get_cash_and_bank_accounts():
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Filter only cash and bank accounts
        accounts = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            (ChartOfAccounts.is_cash == True) | (ChartOfAccounts.is_bank == True)
        ).all()

        # Get unique categories associated with these accounts
        categories_map = {}
        for acc in accounts:
            if acc.category_fk and acc.categories:
                categories_map[acc.category_fk] = {
                    "id": acc.categories.id,
                    "category": acc.categories.category,
                    "category_code": acc.categories.category_id
                }

        return jsonify({
            "categories": list(categories_map.values()),
            "chart_of_accounts": [{
                "account_type": acc.parent_account_type,
                "category": acc.categories.category,
                "category_id": acc.category_fk,
                "category_code": acc.category_id,
                "sub_category_id": acc.id,
                "sub_category_code": acc.sub_category_id,
                "sub_category": acc.sub_category,
                "is_cash": acc.is_cash,
                "is_bank": acc.is_bank
            } for acc in accounts]
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error occurred {e}')
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()


@api_routes.route('/get_exchange_rates', methods=["GET"])
def get_exchange_rates():
    api_key = request.headers.get("X-API-Key")
    end_date = request.args.get("end_date")

    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        company = user_session.query(Company).filter_by(api_key=api_key).first()

        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        try:
            app_id = company.id
            base_currency_info = get_base_currency(user_session, app_id)

            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            base_currency_id = base_currency_info["base_currency_id"]

            # Get all currencies used by the company
            currencies = user_session.query(Currency).filter(Currency.app_id == app_id).all()

            exchange_rates = {}
            for currency in currencies:
                if currency.id == base_currency_id:
                    continue

                # Get the most recent exchange rate before or on end_date
                rate_query = user_session.query(ExchangeRate).filter(
                    ExchangeRate.from_currency == currency.id,
                    ExchangeRate.to_currency == base_currency_id,
                    ExchangeRate.date <= end_date
                ).order_by(ExchangeRate.date.desc()).first()

                if rate_query:
                    exchange_rates[currency.user_currency] = float(rate_query.rate)

            return jsonify({
                "base_currency": base_currency_info["base_currency"],
                "exchange_rates": exchange_rates
            })

        except Exception as e:
            print(f'Error is {e}')
            return jsonify({"error": str(e)}), 500


@api_routes.route('/api/fx_gain_loss_summary')
@login_required
def fx_gain_loss_summary():
    from sqlalchemy import and_, or_
    from decimal import Decimal
    from datetime import datetime, time
    import collections

    db_session = Session()
    try:
        app_id = current_user.app_id
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({'status': 'error', 'message': 'Base currency not set'}), 400

        base_currency_id = base_currency_info["base_currency_id"]

        start_date_str = request.args.get("start_date")
        end_date_str = request.args.get("end_date")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else datetime.today()

        # Step 1: Get foreign cash asset balances as of end_date
        # Query journal entries for asset accounts with foreign currency
        entries = db_session.query(JournalEntry).options(
            joinedload(JournalEntry.journal).joinedload(Journal.currency),
            joinedload(JournalEntry.journal).joinedload(Journal.exchange_rate),
            joinedload(JournalEntry.chart_of_accounts)
        ).filter(
            JournalEntry.app_id == app_id,
            JournalEntry.dr_cr == 'D',  # Debits for asset increases
            Journal.date <= end_date.date()
        ).join(
            JournalEntry.journal
        ).join(
            JournalEntry.chart_of_accounts
        ).filter(
            ChartOfAccounts.is_cash == True,  # Only cash accounts
            Journal.currency_id != base_currency_id  # Foreign currency only
        ).all()

        balances = collections.defaultdict(lambda: Decimal("0.0"))  # key: (account_id, currency_id)
        book_value_map = collections.defaultdict(lambda: Decimal("0.0"))
        latest_rate_map = {}

        for entry in entries:
            journal = entry.journal
            if not journal:
                continue

            key = (entry.subcategory_id, journal.currency_id)
            direction = Decimal("1.0") if entry.dr_cr == 'D' else Decimal("-1.0")
            amount = Decimal(str(entry.amount)) * direction

            # Get original book value
            rate = Decimal(journal.exchange_rate.rate) if journal.exchange_rate else Decimal("1.0")
            base_equivalent = amount * rate

            balances[key] += amount
            book_value_map[key] += base_equivalent

        # Step 2: Get latest exchange rates as of end_date
        for (account_id, currency_id) in balances.keys():
            latest_rate = db_session.query(ExchangeRate).filter_by(
                app_id=app_id,
                from_currency_id=currency_id,
                to_currency_id=base_currency_id
            ).filter(
                ExchangeRate.date <= end_date.date()
            ).order_by(ExchangeRate.date.desc()).first()

            if latest_rate:
                latest_rate_map[(account_id, currency_id)] = Decimal(latest_rate.rate)

        # Step 3: Calculate gain/loss
        result = []
        for (account_id, currency_id), foreign_qty in balances.items():
            if foreign_qty == 0:
                continue

            book_value = book_value_map[(account_id, currency_id)]
            latest_rate = latest_rate_map.get((account_id, currency_id))
            if not latest_rate:
                continue

            current_base_value = foreign_qty * latest_rate
            gain_loss = (current_base_value - book_value).quantize(Decimal("0.01"))

            account = db_session.query(ChartOfAccounts).filter_by(id=account_id).first()
            currency = db_session.query(Currency).filter_by(id=currency_id).first()
            label = f"{account.sub_category} ({currency.user_currency})" if account and currency else str(account_id)

            result.append({
                "account_id": account_id,
                "subcategory": label,
                "fx_gain_loss": float(gain_loss)
            })

        return jsonify(result)

    except Exception as e:
        logger.error(f"FX Gain/Loss summary error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        db_session.close()


@api_routes.route('/api/inventory/available')
def get_available_quantity():
    db_session = Session()
    item_id = request.args.get('item_id', type=int)
    location_id = request.args.get('location_id', type=int)
    logger.info(f'Front end data is Item ID {item_id} and lcoation id {location_id}')
    if not item_id or not location_id:
        return jsonify({'error': 'Missing item_id or location_id'}), 400

    try:
        quantity = calculate_available_quantity(
            db_session=db_session,
            app_id=current_user.app_id,
            item_id=item_id,
            location_id=location_id
        )

        logger.info(f'Qty is {quantity}')
        return jsonify({'available_quantity': quantity})

    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except DatabaseError as e:
        return jsonify({'error': 'Inventory check failed'}), 500


# @api_routes.route('/api/my_balances', methods=['GET'])
# @login_required
# def get_cash_balances_api():
#     """API endpoint to fetch current balances in base currency"""
#     try:
#         with Session() as db_session:
#             app_id = current_user.app_id
#
#             # Get base currency from request parameters (if provided)
#             base_currency_id = request.args.get('base_currency_id', type=int)
#             base_currency_code = request.args.get('base_currency_code', type=str)
#
#             if not base_currency_id or not base_currency_code:
#                 # 1. Get company's base currency
#                 base_currency_info = get_base_currency(db_session, app_id)
#                 if not base_currency_info:
#                     return jsonify({"error": "Base currency not defined for this company"}), 400
#
#                 base_currency_id = base_currency_info["base_currency_id"]
#                 base_currency_code = base_currency_info["base_currency"]
#
#             # 2. Get all relevant accounts
#             accounts = db_session.query(ChartOfAccounts).filter(
#                 ChartOfAccounts.app_id == app_id,
#                 or_(
#                     ChartOfAccounts.is_cash == True,
#                     ChartOfAccounts.is_bank == True,
#                     ChartOfAccounts.is_payable == True,
#                     ChartOfAccounts.is_receivable == True
#                 )
#             ).all()
#
#             # Get all journal entries for these accounts
#             account_ids = [account.id for account in accounts]
#             # Get all journal entries for these accounts, only from Posted journals
#             journal_entries = (
#                 db_session.query(JournalEntry)
#                 .join(Journal, Journal.id == JournalEntry.journal_id)
#                 .options(
#                     load_only(JournalEntry.subcategory_id, JournalEntry.amount, JournalEntry.dr_cr),
#                     joinedload(JournalEntry.journal).load_only(Journal.currency_id),
#                     joinedload(JournalEntry.journal).joinedload(Journal.currency).load_only(Currency.user_currency)
#                 )
#                 .filter(
#                     JournalEntry.subcategory_id.in_(account_ids),
#                     JournalEntry.app_id == app_id,
#                     Journal.status == 'Posted'
#                 )
#                 .all()
#             )
#
#             # Group journal entries by account
#             entries_by_account = {}
#             for entry in journal_entries:
#                 if entry.subcategory_id not in entries_by_account:
#                     entries_by_account[entry.subcategory_id] = []
#                 entries_by_account[entry.subcategory_id].append(entry)
#
#             # 3. Calculate balances
#             results = []
#             totals = {
#                 'cash': Decimal('0'),
#                 'bank': Decimal('0'),
#                 'payable': Decimal('0'),
#                 'receivable': Decimal('0')
#             }
#
#             for account in accounts:
#                 # Track balances by currency
#                 currency_balances = {}  # {currency_id: {'original': Decimal, 'converted': Decimal}}
#
#                 # Get entries for this account
#                 entries = entries_by_account.get(account.id, [])
#
#                 for entry in entries:
#                     amount = Decimal(str(entry.amount))
#                     currency_id = entry.journal.currency_id if entry.journal else base_currency_id
#
#                     # Normalize dr_cr and normal_balance to first letter uppercase
#                     txn_dc = entry.dr_cr.upper()[0]  # 'D' or 'C'
#                     normal_bal = account.normal_balance.upper()[0]  # 'D' or 'C'
#
#                     # Determine if we add or subtract
#                     adjusted_amount = amount if txn_dc == normal_bal else -amount
#
#                     # Initialize currency tracking if needed
#                     if currency_id not in currency_balances:
#                         currency_balances[currency_id] = {
#                             'original': Decimal('0'),
#                             'converted': Decimal('0'),
#                             'currency_code': entry.journal.currency.user_currency if entry.journal and entry.journal.currency else base_currency_code
#                         }
#
#                     # Update original balance
#                     currency_balances[currency_id]['original'] += adjusted_amount
#
#                 # Convert all currency balances to base currency
#                 account_balance = Decimal('0')
#                 currency_details = []
#
#                 for currency_id, balance_info in currency_balances.items():
#                     original_balance = balance_info['original']
#
#                     if currency_id == base_currency_id:
#                         # No conversion needed for base currency
#                         converted_balance = original_balance
#                         rate = Decimal('1')
#                     else:
#                         # Convert foreign currency balance using latest rate
#                         try:
#                             rate = get_exchange_rate(
#                                 db_session,
#                                 currency_id,
#                                 base_currency_id,
#                                 app_id
#                             )
#                             converted_balance = original_balance * Decimal(str(rate))
#                         except Exception as e:
#                             logger.error(
#                                 f"Currency conversion error for account {account.id}: {str(e)}\n{traceback.format_exc()}")
#                             converted_balance = Decimal('0')
#                             rate = Decimal('0')
#
#                     account_balance += converted_balance
#
#                     currency_details.append({
#                         'original_balance': float(original_balance),
#                         'currency_code': balance_info['currency_code'],
#                         'converted_balance': float(converted_balance),
#                         'exchange_rate': float(rate),
#                         'is_base_currency': currency_id == base_currency_id
#                     })
#
#                 account_type = get_account_type(account)
#                 totals[account_type] += account_balance
#
#                 account_data = {
#                     "account_id": account.id,
#                     "account_name": account.sub_category,
#                     "account_code": account.sub_category_id,
#                     "account_type": account_type,
#                     "balance": float(account_balance),
#                     "currency_details": currency_details,
#                     "base_currency_code": base_currency_code
#                 }
#
#                 # Only include bank_name if account is bank and field exists
#                 if account_type == "bank":
#                     account_data['bank_name'] = account.bank_name if hasattr(account, 'bank_name') else None
#
#                 results.append(account_data)
#
#             return jsonify({
#                 "success": True,
#                 "data": {
#                     "accounts": results,
#                     "summary": {
#                         "cash_total": float(totals['cash']),
#                         "bank_total": float(totals['bank']),
#                         "payable_total": float(totals['payable']),
#                         "receivable_total": float(totals['receivable']),
#                         "base_currency_id": base_currency_id,
#                         "base_currency_code": base_currency_code,
#                         "timestamp": datetime.now().isoformat()
#                     }
#                 }
#             })
#
#     except Exception as e:
#         logger.error(f'Error getting cash balances: {str(e)}')
#         return jsonify({
#             "success": False,
#             "error": "Failed to load balances",
#             "details": str(e)
#         }), 500


@api_routes.route('/api/my_balances', methods=['GET'])
@login_required
def get_cash_balances_api():
    """API endpoint to fetch current balances in base currency"""
    try:
        with Session() as db_session:
            app_id = current_user.app_id

            # Get base currency from frontend first, fallback to company default
            base_currency_id = request.args.get('base_currency_id', type=int)
            base_currency_code = request.args.get('base_currency_code', type=str)

            if not base_currency_id or not base_currency_code:
                base_currency_info = get_base_currency(db_session, app_id)
                if not base_currency_info:
                    return jsonify({"error": "Base currency not defined for this company"}), 400
                base_currency_id = base_currency_info["base_currency_id"]
                base_currency_code = base_currency_info["base_currency"]

            # Get all relevant accounts
            accounts = db_session.query(ChartOfAccounts).filter(
                ChartOfAccounts.app_id == app_id,
                or_(
                    ChartOfAccounts.is_cash == True,
                    ChartOfAccounts.is_bank == True,
                    ChartOfAccounts.is_payable == True,
                    ChartOfAccounts.is_receivable == True
                )
            ).all()

            account_ids = [account.id for account in accounts]

            if not account_ids:
                return jsonify({
                    "success": True,
                    "data": {
                        "accounts": [],
                        "summary": {
                            "cash_total": 0,
                            "bank_total": 0,
                            "payable_total": 0,
                            "receivable_total": 0,
                            "base_currency_id": base_currency_id,
                            "base_currency_code": base_currency_code,
                            "timestamp": datetime.now().isoformat()
                        }
                    }
                }), 200

            # Get journal entries with their amounts (historical)
            journal_entries = (
                db_session.query(JournalEntry)
                .join(Journal, Journal.id == JournalEntry.journal_id)
                .options(
                    load_only(JournalEntry.subcategory_id, JournalEntry.amount, JournalEntry.dr_cr),
                    joinedload(JournalEntry.journal).load_only(Journal.currency_id),
                    joinedload(JournalEntry.journal).joinedload(Journal.currency).load_only(Currency.user_currency)
                )
                .filter(
                    JournalEntry.subcategory_id.in_(account_ids),
                    JournalEntry.app_id == app_id,
                    Journal.status == 'Posted'
                )
                .all()
            )

            # Group by account and currency
            entries_by_account = {}
            for entry in journal_entries:
                if entry.subcategory_id not in entries_by_account:
                    entries_by_account[entry.subcategory_id] = []
                entries_by_account[entry.subcategory_id].append(entry)

            # Calculate balances
            results = []
            totals = {
                'cash': Decimal('0'),
                'bank': Decimal('0'),
                'payable': Decimal('0'),
                'receivable': Decimal('0')
            }

            for account in accounts:
                entries = entries_by_account.get(account.id, [])

                # Group by currency
                currency_balances = {}
                for entry in entries:
                    amount = Decimal(str(entry.amount))
                    currency_id = entry.journal.currency_id if entry.journal else base_currency_id
                    currency_code = entry.journal.currency.user_currency if entry.journal and entry.journal.currency else base_currency_code

                    # Sign logic: Add if dr_cr matches normal_balance, else subtract
                    txn_dc = entry.dr_cr.upper()[0]
                    normal_bal = account.normal_balance.upper()[0]
                    adjusted_amount = amount if txn_dc == normal_bal else -amount

                    if currency_id not in currency_balances:
                        currency_balances[currency_id] = {
                            'original': Decimal('0'),
                            'currency_code': currency_code
                        }
                    currency_balances[currency_id]['original'] += adjusted_amount

                # Convert each currency to base using LATEST exchange rate (your original logic)
                account_balance = Decimal('0')
                currency_details = []

                for currency_id, balance_info in currency_balances.items():
                    original_balance = balance_info['original']

                    if currency_id == base_currency_id:
                        converted_balance = original_balance
                        rate = Decimal('1')
                    else:
                        # Use your get_exchange_rate function to get latest rate
                        try:
                            rate = get_exchange_rate(
                                db_session,
                                currency_id,
                                base_currency_id,
                                app_id
                            )
                            converted_balance = original_balance * Decimal(str(rate))
                        except Exception as e:
                            logger.error(f"Currency conversion error: {e}")
                            converted_balance = Decimal('0')
                            rate = Decimal('0')

                    account_balance += converted_balance

                    currency_details.append({
                        'original_balance': float(original_balance),
                        'currency_code': balance_info['currency_code'],
                        'converted_balance': float(converted_balance),
                        'exchange_rate': float(rate),
                        'is_base_currency': currency_id == base_currency_id
                    })

                account_type = get_account_type(account)
                totals[account_type] += account_balance

                account_data = {
                    "account_id": account.id,
                    "account_name": account.sub_category,
                    "account_code": account.sub_category_id,
                    "account_type": account_type,
                    "balance": float(account_balance),
                    "currency_details": currency_details,
                    "base_currency_code": base_currency_code
                }

                if account_type == "bank":
                    account_data['bank_name'] = account.bank_name if hasattr(account, 'bank_name') else None

                results.append(account_data)

            return jsonify({
                "success": True,
                "data": {
                    "accounts": results,
                    "summary": {
                        "cash_total": float(totals['cash']),
                        "bank_total": float(totals['bank']),
                        "payable_total": float(totals['payable']),
                        "receivable_total": float(totals['receivable']),
                        "base_currency_id": base_currency_id,
                        "base_currency_code": base_currency_code,
                        "timestamp": datetime.now().isoformat()
                    }
                }
            })

    except Exception as e:
        logger.error(f'Error getting balances: {str(e)}\n{traceback.format_exc()}')
        return jsonify({
            "success": False,
            "error": "Failed to load balances",
            "details": str(e)
        }), 500

def get_account_type(account):
    """Determine account type based on boolean flags"""
    if account.is_cash:
        return "cash"
    if account.is_bank:
        return "bank"
    if account.is_payable:
        return "payable"
    if account.is_receivable:
        return "receivable"
    return "other"


@api_routes.route('/api/old_version/get_transactions_base_currency', methods=["GET"])
def get_transactions_base_currency_old_version():
    api_key = request.headers.get("X-API-Key")

    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        company = user_session.query(Company).filter_by(api_key=api_key).first()

        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        try:
            app_id = company.id
            base_currency_info = get_base_currency(user_session, app_id)

            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Get end_date from query params or default to today
            end_date_str = request.args.get('end_date')
            if end_date_str:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            else:
                end_date = datetime.today().date()

            # OPTIMIZATION 1: Use eager loading to reduce N+1 queries
            from sqlalchemy.orm import joinedload

            # Load all related data in one query
            entries = user_session.query(JournalEntry).options(
                joinedload(JournalEntry.journal).joinedload(Journal.currency),
                joinedload(JournalEntry.journal).joinedload(Journal.exchange_rate),
                joinedload(JournalEntry.journal).joinedload(Journal.payment_mode),
                joinedload(JournalEntry.journal).joinedload(Journal.vendor),
                joinedload(JournalEntry.journal).joinedload(Journal.project),
                joinedload(JournalEntry.chart_of_accounts).joinedload(ChartOfAccounts.categories),
                joinedload(JournalEntry.chart_of_accounts).joinedload(ChartOfAccounts.report_section)
            ).join(
                Journal, Journal.id == JournalEntry.journal_id
            ).filter(
                Journal.app_id == app_id,
                Journal.status == 'Posted',
                Journal.date <= end_date  # Add date filter to reduce data
            ).order_by(
                Journal.date.desc(),  # Optional: most recent first
                JournalEntry.id
            ).all()

            # OPTIMIZATION 2: Pre-fetch exchange rates in batch
            # Collect distinct foreign currency IDs and exchange rate IDs
            foreign_currency_ids = set()
            exchange_rate_ids = set()

            for entry in entries:
                journal = entry.journal
                if journal.currency_id != base_currency_id:
                    foreign_currency_ids.add(journal.currency_id)
                    if journal.exchange_rate_id:
                        exchange_rate_ids.add(journal.exchange_rate_id)

            # Load all exchange rates at once
            exchange_rates_cache = {}
            if exchange_rate_ids:
                rates = user_session.query(
                    ExchangeRate.id,
                    ExchangeRate.rate
                ).filter(
                    ExchangeRate.id.in_(list(exchange_rate_ids))
                ).all()
                exchange_rates_cache = {rate.id: Decimal(str(rate.rate)) for rate in rates}

            # OPTIMIZATION 3: Also get latest rates for currencies without specific rates
            latest_rates_cache = {}
            if foreign_currency_ids:
                # Get the most recent rate for each foreign currency to base currency
                # Using a subquery to get the latest rate per currency
                from sqlalchemy import desc

                subquery = user_session.query(
                    ExchangeRate.from_currency_id,
                    ExchangeRate.rate,
                    func.row_number().over(
                        partition_by=ExchangeRate.from_currency_id,
                        order_by=desc(ExchangeRate.date)
                    ).label('row_num')
                ).filter(
                    ExchangeRate.from_currency_id.in_(list(foreign_currency_ids)),
                    ExchangeRate.to_currency_id == base_currency_id,
                    ExchangeRate.app_id == app_id,
                    ExchangeRate.date <= end_date
                ).subquery()

                latest_rates = user_session.query(
                    subquery.c.from_currency_id,
                    subquery.c.rate
                ).filter(
                    subquery.c.row_num == 1
                ).all()

                latest_rates_cache = {rate.from_currency_id: Decimal(str(rate.rate)) for rate in latest_rates}

            transactions_list = []

            # OPTIMIZATION 4: Use local variables and pre-compute for speed
            base_currency_decimal = Decimal('1.0')

            for entry in entries:
                journal = entry.journal
                transaction_currency_id = journal.currency_id
                original_amount = entry.amount

                # Convert amount to Decimal once
                amount_decimal = Decimal(str(original_amount)) if not isinstance(original_amount,
                                                                                 Decimal) else original_amount

                # OPTIMIZATION 5: Fast path for base currency
                if transaction_currency_id == base_currency_id:
                    amount_in_base = amount_decimal
                    rate = base_currency_decimal
                else:
                    # Try to get rate from journal's exchange_rate
                    if journal.exchange_rate_id and journal.exchange_rate_id in exchange_rates_cache:
                        rate = exchange_rates_cache[journal.exchange_rate_id]
                    elif journal.exchange_rate and journal.exchange_rate.rate:
                        rate = Decimal(str(journal.exchange_rate.rate))
                    else:
                        # Fallback to latest rate for this currency
                        rate = latest_rates_cache.get(transaction_currency_id, base_currency_decimal)

                    amount_in_base = round(amount_decimal * rate, 2)

                # OPTIMIZATION 6: Safe attribute access with caching
                chart_accounts = entry.chart_of_accounts
                categories = chart_accounts.categories if chart_accounts else None
                report_section = chart_accounts.report_section if chart_accounts else None

                transaction_currency = journal.currency.user_currency if journal.currency else None

                transactions_list.append({
                    "id": entry.id,
                    "journal_id": journal.id,
                    "journal_number": journal.journal_number,
                    "transaction_type": chart_accounts.parent_account_type if chart_accounts else None,
                    "date": journal.date.strftime('%Y-%m-%d'),
                    "category": categories.category if categories else None,
                    "subcategory": chart_accounts.sub_category if chart_accounts else None,
                    "currency": transaction_currency,
                    "amount": float(original_amount),
                    "amount_in_base_currency": float(amount_in_base),
                    "base_currency": base_currency,
                    "dr_cr": entry.dr_cr,
                    "description": entry.description or journal.narration,
                    "payment_mode": journal.payment_mode.payment_mode if journal.payment_mode else None,
                    "payment_to_vendor": journal.vendor.vendor_name if journal.vendor else None,
                    "project_name": journal.project.name if journal.project else None,
                    "date_added": journal.date_added.strftime('%Y-%m-%d') if journal.date_added else None,
                    "source_type": entry.source_type,
                    "normal_balance": chart_accounts.normal_balance if chart_accounts else None,
                    "is_cash_equivalent": bool(
                        (chart_accounts.is_cash if chart_accounts else False) or
                        (chart_accounts.is_bank if chart_accounts else False)
                    ),
                    "report_section": report_section.name if report_section else None,
                    "line_number": entry.line_number
                })

            return jsonify(transactions_list)

        except Exception as e:
            logger.error(f'Error is {e}')
            return jsonify({"error": str(e)}), 500


@api_routes.route('/api/get_transactions_base_currency', methods=["GET"])
def get_transactions_base_currency():
    api_key = request.headers.get("X-API-Key")

    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        company = user_session.query(Company).filter_by(api_key=api_key).first()

        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        try:
            app_id = company.id
            base_currency_info = get_base_currency(user_session, app_id)

            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Get filters from query params
            start_date_str = request.args.get('start_date')
            end_date_str = request.args.get('end_date')
            currency_filter = request.args.get('currency')

            # Build base query with eager loading
            from sqlalchemy.orm import joinedload

            query = user_session.query(JournalEntry).options(
                joinedload(JournalEntry.journal).joinedload(Journal.currency),
                joinedload(JournalEntry.journal).joinedload(Journal.exchange_rate),
                joinedload(JournalEntry.chart_of_accounts).joinedload(ChartOfAccounts.categories),
                joinedload(JournalEntry.chart_of_accounts).joinedload(ChartOfAccounts.report_section)
            ).join(
                Journal, Journal.id == JournalEntry.journal_id
            ).join(
                ChartOfAccounts, ChartOfAccounts.id == JournalEntry.subcategory_id
            ).filter(
                Journal.app_id == app_id,
                Journal.status == 'Posted',
                ChartOfAccounts.parent_account_type.in_(['Income', 'Expense'])
            )

            # Apply date filters if provided
            if start_date_str:
                try:
                    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                    query = query.filter(Journal.date >= start_date)
                except ValueError:
                    return jsonify({"error": "Invalid start_date format. Use YYYY-MM-DD"}), 400

            if end_date_str:
                try:
                    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                    query = query.filter(Journal.date <= end_date)
                except ValueError:
                    return jsonify({"error": "Invalid end_date format. Use YYYY-MM-DD"}), 400

            # Apply currency filter if provided and not 'All'
            if currency_filter and currency_filter != 'All':
                query = query.filter(Journal.currency.has(user_currency=currency_filter))

            # Order by date (most recent first)
            entries = query.order_by(
                Journal.date.desc(),
                JournalEntry.id
            ).all()

            # If no date filters provided, limit to last 365 days by default
            if not start_date_str and not end_date_str:
                # We'll still fetch all but this helps when there's too much data
                # Consider adding a default date range limit
                one_year_ago = datetime.today().date() - timedelta(days=365)
                entries = [e for e in entries if e.journal.date >= one_year_ago]
                print(f"Limited to transactions from last year: {len(entries)} entries")

            print(f"Processing {len(entries)} transactions")

            # Pre-fetch exchange rates in batch
            foreign_currency_ids = set()
            exchange_rate_ids = set()

            for entry in entries:
                journal = entry.journal
                if journal.currency_id != base_currency_id:
                    foreign_currency_ids.add(journal.currency_id)
                    if journal.exchange_rate_id:
                        exchange_rate_ids.add(journal.exchange_rate_id)

            # Load all exchange rates at once
            exchange_rates_cache = {}
            if exchange_rate_ids:
                rates = user_session.query(
                    ExchangeRate.id,
                    ExchangeRate.rate
                ).filter(
                    ExchangeRate.id.in_(list(exchange_rate_ids))
                ).all()
                exchange_rates_cache = {rate.id: Decimal(str(rate.rate)) for rate in rates}

            # Get latest rates for currencies without specific rates
            latest_rates_cache = {}
            if foreign_currency_ids:
                from sqlalchemy import desc

                # Get the end_date for rate lookup
                rate_lookup_date = end_date if end_date_str else datetime.today().date()

                subquery = user_session.query(
                    ExchangeRate.from_currency_id,
                    ExchangeRate.rate,
                    func.row_number().over(
                        partition_by=ExchangeRate.from_currency_id,
                        order_by=desc(ExchangeRate.date)
                    ).label('row_num')
                ).filter(
                    ExchangeRate.from_currency_id.in_(list(foreign_currency_ids)),
                    ExchangeRate.to_currency_id == base_currency_id,
                    ExchangeRate.app_id == app_id,
                    ExchangeRate.date <= rate_lookup_date
                ).subquery()

                latest_rates = user_session.query(
                    subquery.c.from_currency_id,
                    subquery.c.rate
                ).filter(
                    subquery.c.row_num == 1
                ).all()

                latest_rates_cache = {rate.from_currency_id: Decimal(str(rate.rate)) for rate in latest_rates}

            transactions_list = []
            base_currency_decimal = Decimal('1.0')

            for entry in entries:
                journal = entry.journal
                transaction_currency_id = journal.currency_id
                original_amount = entry.amount

                # Convert amount to Decimal once
                amount_decimal = Decimal(str(original_amount)) if not isinstance(original_amount,
                                                                                 Decimal) else original_amount

                # Fast path for base currency
                if transaction_currency_id == base_currency_id:
                    amount_in_base = amount_decimal
                    rate = base_currency_decimal
                else:
                    # Try to get rate from journal's exchange_rate
                    if journal.exchange_rate_id and journal.exchange_rate_id in exchange_rates_cache:
                        rate = exchange_rates_cache[journal.exchange_rate_id]
                    elif journal.exchange_rate and journal.exchange_rate.rate:
                        rate = Decimal(str(journal.exchange_rate.rate))
                    else:
                        # Fallback to latest rate for this currency
                        rate = latest_rates_cache.get(transaction_currency_id, base_currency_decimal)

                    amount_in_base = round(amount_decimal * rate, 2)

                # Safe attribute access
                chart_accounts = entry.chart_of_accounts
                categories = chart_accounts.categories if chart_accounts else None
                report_section = chart_accounts.report_section if chart_accounts else None

                transaction_currency = journal.currency.user_currency if journal.currency else None

                transactions_list.append({
                    "id": entry.id,
                    "journal_id": journal.id,
                    "journal_number": journal.journal_number,
                    "transaction_type": chart_accounts.parent_account_type if chart_accounts else None,
                    "date": journal.date.strftime('%Y-%m-%d'),
                    "category": categories.category if categories else None,
                    "subcategory": chart_accounts.sub_category if chart_accounts else None,
                    "currency": transaction_currency,
                    "amount": float(original_amount),
                    "amount_in_base_currency": float(amount_in_base),
                    "base_currency": base_currency,
                    "dr_cr": entry.dr_cr,
                    "description": entry.description or journal.narration,
                    "payment_mode": journal.payment_mode.payment_mode if journal.payment_mode else None,
                    "payment_to_vendor": journal.vendor.vendor_name if journal.vendor else None,
                    "project_name": journal.project.name if journal.project else None,
                    "date_added": journal.date_added.strftime('%Y-%m-%d') if journal.date_added else None,
                    "source_type": entry.source_type,
                    "normal_balance": chart_accounts.normal_balance if chart_accounts else None,
                    "is_cash_equivalent": bool(
                        (chart_accounts.is_cash if chart_accounts else False) or
                        (chart_accounts.is_bank if chart_accounts else False)
                    ),
                    "report_section": report_section.name if report_section else None,
                    "line_number": entry.line_number
                })

            return jsonify(transactions_list)

        except Exception as e:
            logger.error(f'Error fetching transactions: {e} \n{traceback.format_exc()}')
            return jsonify({"error": str(e)}), 500


@api_routes.route('/api/income_expense_summary', methods=["GET"])
def get_income_expense_summary():
    """Simple and efficient income/expense report"""
    api_key = request.headers.get("X-API-Key")

    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        company = user_session.query(Company).filter_by(api_key=api_key).first()

        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        try:
            import time
            from datetime import timedelta
            from sqlalchemy import func, case, and_
            from decimal import Decimal

            start_time = time.time()

            app_id = company.id
            base_currency_info = get_base_currency(user_session, app_id)

            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Get filters from query params
            start_date_str = request.args.get('start_date') or None
            end_date_str = request.args.get('end_date') or None
            hide_zero = request.args.get('hide_zero', 'false').lower() == 'true'

            # DEFAULT TO CURRENT MONTH
            today = datetime.today().date()

            if not start_date_str and not end_date_str:
                # Default to current month
                first_day = today.replace(day=1)
                last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                start_date = first_day
                end_date = last_day
            else:
                # Use provided dates
                if start_date_str:
                    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                else:
                    # If no start date, default to 1 year ago
                    start_date = today - timedelta(days=365)

                if end_date_str:
                    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                else:
                    # If no end date, default to today
                    end_date = today

            # CORRECT CASE STATEMENT SYNTAX - NO LISTS, JUST POSITIONAL ARGUMENTS
            # Build the signed amount calculation
            signed_amount_case = case(
                (and_(ChartOfAccounts.parent_account_type == 'Income', JournalEntry.dr_cr == 'C'), JournalEntry.amount),
                (
                    and_(ChartOfAccounts.parent_account_type == 'Income', JournalEntry.dr_cr == 'D'),
                    -JournalEntry.amount),
                (
                    and_(ChartOfAccounts.parent_account_type == 'Expense', JournalEntry.dr_cr == 'D'),
                    JournalEntry.amount),
                (and_(ChartOfAccounts.parent_account_type == 'Expense', JournalEntry.dr_cr == 'C'),
                 -JournalEntry.amount),
                else_=0
            )

            # Build the exchange rate conversion
            exchange_rate_case = case(
                (Journal.currency_id == base_currency_id, 1.0),
                (Journal.exchange_rate_id.isnot(None), func.coalesce(ExchangeRate.rate, 1.0)),
                else_=1.0
            )

            # ONE SINGLE QUERY
            query = user_session.query(
                ChartOfAccounts.parent_account_type,
                func.coalesce(Category.category, 'Uncategorized').label('category'),
                func.coalesce(ChartOfAccounts.sub_category, 'Uncategorized').label('subcategory'),
                func.sum(signed_amount_case * exchange_rate_case).label('amount_in_base')
            ).join(
                Journal, Journal.id == JournalEntry.journal_id
            ).join(
                ChartOfAccounts, ChartOfAccounts.id == JournalEntry.subcategory_id
            ).outerjoin(
                Category, Category.id == ChartOfAccounts.category_fk
            ).outerjoin(
                ExchangeRate, ExchangeRate.id == Journal.exchange_rate_id
            ).filter(
                and_(
                    Journal.app_id == app_id,
                    Journal.status == 'Posted',
                    ChartOfAccounts.parent_account_type.in_(['Income', 'Expense']),
                    Journal.date >= start_date,
                    Journal.date <= end_date
                )
            ).group_by(
                ChartOfAccounts.parent_account_type,
                Category.category,
                ChartOfAccounts.sub_category
            ).order_by(
                ChartOfAccounts.parent_account_type.desc(),
                Category.category,
                ChartOfAccounts.sub_category
            )

            results = query.all()

            # Process results
            income_data = []
            expense_data = []
            total_income = Decimal('0')
            total_expense = Decimal('0')

            for parent_type, category, subcategory, amount in results:
                if amount is None:
                    continue

                amount_decimal = Decimal(str(amount))

                if hide_zero and abs(amount_decimal) < Decimal('0.01'):
                    continue

                item = {
                    "category": category,
                    "subcategory": subcategory,
                    "amount": float(amount_decimal)
                }

                if parent_type == 'Income':
                    income_data.append(item)
                    total_income += amount_decimal
                else:
                    expense_data.append(item)
                    total_expense += amount_decimal

            net_income = total_income - total_expense

            end_time = time.time()
            logger.info(f"Income/Expense report generated in {end_time - start_time:.2f} seconds")

            return jsonify({
                "income": income_data,
                "expense": expense_data,
                "total_income": float(total_income),
                "total_expense": float(total_expense),
                "net_income": float(net_income),
                "base_currency": base_currency,
                "report_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

        except Exception as e:
            logger.error(f'Error in income/expense summary: {e}\n{traceback.format_exc()}')
            return jsonify({"error": str(e)}), 500


@api_routes.route('/api/trial_balance_summary', methods=["GET"])
def get_trial_balance_summary():
    """Optimized trial balance report"""
    api_key = request.headers.get("X-API-Key")

    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        company = user_session.query(Company).filter_by(api_key=api_key).first()

        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        try:
            import time
            from datetime import timedelta
            from sqlalchemy import func, case, and_
            from decimal import Decimal

            start_time = time.time()

            app_id = company.id
            base_currency_info = get_base_currency(user_session, app_id)

            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Get filters from query params
            start_date_str = request.args.get('start_date')
            end_date_str = request.args.get('end_date')

            # DEFAULT TO ALL TIME (no date restriction unless specified)
            start_date = None
            end_date = None

            if start_date_str:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()

            if end_date_str:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

            # Build the exchange rate conversion
            exchange_rate_case = case(
                (Journal.currency_id == base_currency_id, 1.0),
                (Journal.exchange_rate_id.isnot(None), func.coalesce(ExchangeRate.rate, 1.0)),
                else_=1.0
            )

            # Query for debit amounts
            debit_query = user_session.query(
                ChartOfAccounts.parent_account_type,
                func.coalesce(Category.category, 'Uncategorized').label('category'),
                func.coalesce(ChartOfAccounts.sub_category, 'Uncategorized').label('subcategory'),
                func.sum(
                    case(
                        (JournalEntry.dr_cr == 'D', JournalEntry.amount),
                        else_=0
                    ) * exchange_rate_case
                ).label('debit_amount')
            ).join(
                Journal, Journal.id == JournalEntry.journal_id
            ).join(
                ChartOfAccounts, ChartOfAccounts.id == JournalEntry.subcategory_id
            ).outerjoin(
                Category, Category.id == ChartOfAccounts.category_fk
            ).outerjoin(
                ExchangeRate, ExchangeRate.id == Journal.exchange_rate_id
            ).filter(
                Journal.app_id == app_id,
                Journal.status == 'Posted'
            )

            # Query for credit amounts
            credit_query = user_session.query(
                ChartOfAccounts.parent_account_type,
                func.coalesce(Category.category, 'Uncategorized').label('category'),
                func.coalesce(ChartOfAccounts.sub_category, 'Uncategorized').label('subcategory'),
                func.sum(
                    case(
                        (JournalEntry.dr_cr == 'C', JournalEntry.amount),
                        else_=0
                    ) * exchange_rate_case
                ).label('credit_amount')
            ).join(
                Journal, Journal.id == JournalEntry.journal_id
            ).join(
                ChartOfAccounts, ChartOfAccounts.id == JournalEntry.subcategory_id
            ).outerjoin(
                Category, Category.id == ChartOfAccounts.category_fk
            ).outerjoin(
                ExchangeRate, ExchangeRate.id == Journal.exchange_rate_id
            ).filter(
                Journal.app_id == app_id,
                Journal.status == 'Posted'
            )

            # Apply date filters if provided
            if start_date:
                debit_query = debit_query.filter(Journal.date >= start_date)
                credit_query = credit_query.filter(Journal.date >= start_date)

            if end_date:
                debit_query = debit_query.filter(Journal.date <= end_date)
                credit_query = credit_query.filter(Journal.date <= end_date)

            # Group and execute both queries
            debit_results = debit_query.group_by(
                ChartOfAccounts.parent_account_type,
                Category.category,
                ChartOfAccounts.sub_category
            ).all()

            credit_results = credit_query.group_by(
                ChartOfAccounts.parent_account_type,
                Category.category,
                ChartOfAccounts.sub_category
            ).all()

            # Combine results
            trial_balance_data = {}
            total_debit = Decimal('0')
            total_credit = Decimal('0')

            # Process debit results
            for parent_type, category, subcategory, debit_amount in debit_results:
                if debit_amount is None or debit_amount == 0:
                    continue

                key = (parent_type, category, subcategory)
                if key not in trial_balance_data:
                    trial_balance_data[key] = {
                        'parent_type': parent_type,
                        'category': category,
                        'subcategory': subcategory,
                        'debit': Decimal('0'),
                        'credit': Decimal('0')
                    }

                trial_balance_data[key]['debit'] = Decimal(str(debit_amount))
                total_debit += Decimal(str(debit_amount))

            # Process credit results
            for parent_type, category, subcategory, credit_amount in credit_results:
                if credit_amount is None or credit_amount == 0:
                    continue

                key = (parent_type, category, subcategory)
                if key not in trial_balance_data:
                    trial_balance_data[key] = {
                        'parent_type': parent_type,
                        'category': category,
                        'subcategory': subcategory,
                        'debit': Decimal('0'),
                        'credit': Decimal('0')
                    }

                trial_balance_data[key]['credit'] = Decimal(str(credit_amount))
                total_credit += Decimal(str(credit_amount))

            # Format response
            accounts_data = []
            for key, data in trial_balance_data.items():
                # Skip accounts with zero balances
                if data['debit'] == 0 and data['credit'] == 0:
                    continue

                accounts_data.append({
                    "parent_type": data['parent_type'],
                    "category": data['category'],
                    "subcategory": data['subcategory'],
                    "debit": float(data['debit']),
                    "credit": float(data['credit'])
                })

            # Sort by category, then subcategory
            accounts_data.sort(key=lambda x: (x['category'], x['subcategory']))

            end_time = time.time()
            logger.info(f"Trial Balance report generated in {end_time - start_time:.2f} seconds")

            return jsonify({
                "accounts": accounts_data,
                "total_debit": float(total_debit),
                "total_credit": float(total_credit),
                "balance_check": float(total_debit - total_credit),
                "is_balanced": abs(total_debit - total_credit) < Decimal('0.01'),
                "base_currency": base_currency,
                "report_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "date_range": {
                    "start_date": start_date_str,
                    "end_date": end_date_str
                }
            })

        except Exception as e:
            logger.error(f'Error in trial balance summary: {e}\n{traceback.format_exc()}')
            return jsonify({"error": str(e)}), 500


@api_routes.route('/api/assets/transactions', methods=["GET"])
def get_asset_transactions():
    """
    API endpoint for asset dashboard data
    """
    # Authentication
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    # Get filter parameters
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    time_filter = request.args.get("time_filter")
    asset_type_id = request.args.get("asset_type_id")
    location_id = request.args.get("location_id")
    department_id = request.args.get("department_id")
    assigned_to_id = request.args.get("assigned_to_id")
    supplier_id = request.args.get("supplier_id")
    category_id = request.args.get("category_id")
    brand_id = request.args.get("brand_id")
    project_id = request.args.get("project_id")
    status = request.args.get("status")
    condition = request.args.get("condition")
    movement_type = request.args.get("movement_type")

    with Session() as db_session:
        try:
            # Validate company
            company = db_session.query(Company).filter_by(api_key=api_key).first()
            if not company:
                return jsonify({"error": "Invalid API key"}), 403

            app_id = company.id

            # Get base currency
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            base_currency = base_currency_info["base_currency"]

            # Determine date range
            if time_filter and time_filter != 'custom':
                start_date_obj, end_date_obj = get_date_range_from_filter(
                    time_filter,
                    custom_start=start_date,
                    custom_end=end_date
                )
            elif start_date and end_date:
                try:
                    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                except ValueError:
                    return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
            else:
                # Default to last 30 days
                start_date_obj, end_date_obj = get_date_range_from_filter('default')

            logger.info(f"Asset Dashboard - Date range: {start_date_obj} to {end_date_obj}")

            # ============================================
            # QUERY 1: ASSET SUMMARY STATISTICS - AS OF END DATE
            # ============================================

            # Get all assets that existed as of end_date
            asset_query = db_session.query(Asset).filter(
                Asset.app_id == app_id,
                Asset.purchase_date <= end_date_obj  # ✅ Show assets purchased up to end date
            )

            # Apply asset filters
            if asset_type_id:
                asset_query = asset_query.filter(Asset.asset_item_id == asset_type_id)
            if location_id:
                asset_query = asset_query.filter(Asset.location_id == location_id)
            if department_id:
                asset_query = asset_query.filter(Asset.department_id == department_id)
            if assigned_to_id:
                asset_query = asset_query.filter(Asset.assigned_to_id == assigned_to_id)
            if supplier_id:
                asset_query = asset_query.filter(Asset.supplier_id == supplier_id)
            if project_id:
                asset_query = asset_query.filter(Asset.project_id == project_id)
            if status:
                asset_query = asset_query.filter(Asset.status == status)
            if condition:
                asset_query = asset_query.filter(Asset.condition == condition)

            # Apply category and brand filters
            if category_id or brand_id:
                asset_query = asset_query.join(AssetItem, Asset.asset_item_id == AssetItem.id)
                if category_id:
                    asset_query = asset_query.filter(AssetItem.item_category_id == category_id)
                if brand_id:
                    asset_query = asset_query.filter(AssetItem.brand_id == brand_id)

            # Get all assets as of end date
            all_assets = asset_query.all()

            total_assets = len(all_assets)
            total_purchase_value = sum(float(a.purchase_price or 0) for a in all_assets)
            total_current_value = sum(float(a.current_value or 0) for a in all_assets)

            # Depreciation = Purchase - Current
            total_depreciation = total_purchase_value - total_current_value
            depreciation_rate = (total_depreciation / total_purchase_value * 100) if total_purchase_value > 0 else 0

            # Status distribution as of end date
            status_counts = {}
            for asset in all_assets:
                status_counts[asset.status] = status_counts.get(asset.status, 0) + 1

            # Condition distribution as of end date
            condition_counts = {}
            for asset in all_assets:
                condition_counts[asset.condition] = condition_counts.get(asset.condition, 0) + 1

            # ============================================
            # QUERY 2: MOVEMENT TRANSACTIONS (WITHIN DATE RANGE)
            # ============================================

            movement_query = db_session.query(
                AssetMovementLineItem,
                AssetMovement,
                Asset,
                AssetItem,
                InventoryLocation,
                Department,
                Employee,
                Vendor
            ).join(
                AssetMovement,
                AssetMovementLineItem.asset_movement_id == AssetMovement.id
            ).outerjoin(
                Asset,
                AssetMovementLineItem.asset_id == Asset.id
            ).outerjoin(
                AssetItem,
                Asset.asset_item_id == AssetItem.id
            ).outerjoin(
                InventoryLocation,
                AssetMovementLineItem.to_location_id == InventoryLocation.id
            ).outerjoin(
                Department,
                AssetMovementLineItem.to_department_id == Department.id
            ).outerjoin(
                Employee,
                AssetMovementLineItem.assigned_to_id == Employee.id
            ).outerjoin(
                Vendor,
                AssetMovementLineItem.party_id == Vendor.id
            ).filter(
                AssetMovement.app_id == app_id,
                AssetMovement.transaction_date >= start_date_obj,
                AssetMovement.transaction_date <= end_date_obj
            )

            # Apply movement filters
            if movement_type:
                movement_query = movement_query.filter(AssetMovement.movement_type == movement_type)
            if asset_type_id:
                movement_query = movement_query.filter(Asset.asset_item_id == asset_type_id)
            if location_id:
                movement_query = movement_query.filter(
                    (AssetMovementLineItem.from_location_id == location_id) |
                    (AssetMovementLineItem.to_location_id == location_id)
                )
            if department_id:
                movement_query = movement_query.filter(
                    (AssetMovementLineItem.from_department_id == department_id) |
                    (AssetMovementLineItem.to_department_id == department_id)
                )
            if assigned_to_id:
                movement_query = movement_query.filter(AssetMovementLineItem.assigned_to_id == assigned_to_id)
            if supplier_id:
                movement_query = movement_query.filter(AssetMovementLineItem.party_id == supplier_id)
            if project_id:
                movement_query = movement_query.filter(AssetMovement.project_id == project_id)

            # Apply category and brand filters to movement query
            if category_id or brand_id:
                movement_query = movement_query.join(AssetItem, Asset.asset_item_id == AssetItem.id)
                if category_id:
                    movement_query = movement_query.filter(AssetItem.item_category_id == category_id)
                if brand_id:
                    movement_query = movement_query.filter(AssetItem.brand_id == brand_id)

            movements = movement_query.all()

            # ============================================
            # QUERY 3: ASSET TYPE DISTRIBUTION - AS OF END DATE
            # ============================================

            asset_type_query = db_session.query(
                AssetItem.id,
                AssetItem.asset_name,
                AssetItem.asset_code,
                func.count(Asset.id).label('asset_count'),
                func.sum(Asset.purchase_price).label('total_purchase'),
                func.sum(Asset.current_value).label('total_current')
            ).outerjoin(
                Asset,
                Asset.asset_item_id == AssetItem.id
            ).filter(
                AssetItem.app_id == app_id,
                AssetItem.status == 'active',
                Asset.purchase_date <= end_date_obj  # ✅ Assets purchased up to end date
            )

            # Apply all asset filters to type distribution
            if asset_type_id:
                asset_type_query = asset_type_query.filter(AssetItem.id == asset_type_id)
            if location_id:
                asset_type_query = asset_type_query.filter(Asset.location_id == location_id)
            if department_id:
                asset_type_query = asset_type_query.filter(Asset.department_id == department_id)
            if assigned_to_id:
                asset_type_query = asset_type_query.filter(Asset.assigned_to_id == assigned_to_id)
            if supplier_id:
                asset_type_query = asset_type_query.filter(Asset.supplier_id == supplier_id)
            if project_id:
                asset_type_query = asset_type_query.filter(Asset.project_id == project_id)
            if status:
                asset_type_query = asset_type_query.filter(Asset.status == status)
            if condition:
                asset_type_query = asset_type_query.filter(Asset.condition == condition)
            if category_id:
                asset_type_query = asset_type_query.filter(AssetItem.item_category_id == category_id)
            if brand_id:
                asset_type_query = asset_type_query.filter(AssetItem.brand_id == brand_id)

            asset_type_query = asset_type_query.group_by(
                AssetItem.id,
                AssetItem.asset_name,
                AssetItem.asset_code
            ).order_by(
                func.count(Asset.id).desc()
            ).limit(10).all()

            # ============================================
            # QUERY 4: LOCATION DISTRIBUTION - AS OF END DATE
            # ============================================

            location_query = db_session.query(
                InventoryLocation.id,
                InventoryLocation.location,
                func.count(Asset.id).label('asset_count'),
                func.sum(Asset.current_value).label('total_value')
            ).outerjoin(
                Asset,
                Asset.location_id == InventoryLocation.id
            ).filter(
                InventoryLocation.app_id == app_id,
                Asset.purchase_date <= end_date_obj  # ✅ Assets purchased up to end date
            )

            # Apply all asset filters to location distribution
            if asset_type_id:
                location_query = location_query.filter(Asset.asset_item_id == asset_type_id)
            if department_id:
                location_query = location_query.filter(Asset.department_id == department_id)
            if assigned_to_id:
                location_query = location_query.filter(Asset.assigned_to_id == assigned_to_id)
            if supplier_id:
                location_query = location_query.filter(Asset.supplier_id == supplier_id)
            if project_id:
                location_query = location_query.filter(Asset.project_id == project_id)
            if status:
                location_query = location_query.filter(Asset.status == status)
            if condition:
                location_query = location_query.filter(Asset.condition == condition)

            # Apply category and brand filters
            if category_id or brand_id:
                location_query = location_query.join(AssetItem, Asset.asset_item_id == AssetItem.id)
                if category_id:
                    location_query = location_query.filter(AssetItem.item_category_id == category_id)
                if brand_id:
                    location_query = location_query.filter(AssetItem.brand_id == brand_id)

            location_distribution = location_query.group_by(
                InventoryLocation.id,
                InventoryLocation.location
            ).order_by(
                func.count(Asset.id).desc()
            ).all()

            # ============================================
            # QUERY 5: ASSETS BY ACQUISITION DATE (WITHIN DATE RANGE)
            # ============================================

            timeline_query = db_session.query(
                func.strftime('%Y-%m', Asset.purchase_date).label('month'),
                func.count(Asset.id).label('asset_count'),
                func.sum(Asset.purchase_price).label('total_value')
            ).filter(
                Asset.app_id == app_id,
                Asset.purchase_date.isnot(None),
                Asset.purchase_date >= start_date_obj,  # ✅ Start date filter
                Asset.purchase_date <= end_date_obj     # ✅ End date filter
            )

            # Apply filters to timeline
            if asset_type_id:
                timeline_query = timeline_query.filter(Asset.asset_item_id == asset_type_id)
            if location_id:
                timeline_query = timeline_query.filter(Asset.location_id == location_id)
            if department_id:
                timeline_query = timeline_query.filter(Asset.department_id == department_id)
            if assigned_to_id:
                timeline_query = timeline_query.filter(Asset.assigned_to_id == assigned_to_id)
            if supplier_id:
                timeline_query = timeline_query.filter(Asset.supplier_id == supplier_id)
            if project_id:
                timeline_query = timeline_query.filter(Asset.project_id == project_id)
            if status:
                timeline_query = timeline_query.filter(Asset.status == status)
            if condition:
                timeline_query = timeline_query.filter(Asset.condition == condition)

            if category_id or brand_id:
                timeline_query = timeline_query.join(AssetItem, Asset.asset_item_id == AssetItem.id)
                if category_id:
                    timeline_query = timeline_query.filter(AssetItem.item_category_id == category_id)
                if brand_id:
                    timeline_query = timeline_query.filter(AssetItem.brand_id == brand_id)

            acquisition_timeline = timeline_query.group_by(
                func.strftime('%Y-%m', Asset.purchase_date)
            ).order_by(
                func.strftime('%Y-%m', Asset.purchase_date).desc()
            ).limit(12).all()

            # ============================================
            # QUERY 6: WARRANTY EXPIRING SOON - AS OF END DATE
            # ============================================

            today = date.today()
            ninety_days = today + timedelta(days=90)

            warranty_query = db_session.query(Asset).filter(
                Asset.app_id == app_id,
                Asset.warranty_expiry.isnot(None),
                Asset.warranty_expiry >= today,
                Asset.warranty_expiry <= ninety_days,
                Asset.status.in_(['in_stock', 'assigned']),
                Asset.purchase_date <= end_date_obj  # ✅ Assets purchased up to end date
            )

            # Apply filters to warranty query
            if asset_type_id:
                warranty_query = warranty_query.filter(Asset.asset_item_id == asset_type_id)
            if location_id:
                warranty_query = warranty_query.filter(Asset.location_id == location_id)
            if department_id:
                warranty_query = warranty_query.filter(Asset.department_id == department_id)
            if assigned_to_id:
                warranty_query = warranty_query.filter(Asset.assigned_to_id == assigned_to_id)
            if supplier_id:
                warranty_query = warranty_query.filter(Asset.supplier_id == supplier_id)
            if project_id:
                warranty_query = warranty_query.filter(Asset.project_id == project_id)
            if status:
                warranty_query = warranty_query.filter(Asset.status == status)
            if condition:
                warranty_query = warranty_query.filter(Asset.condition == condition)

            if category_id or brand_id:
                warranty_query = warranty_query.join(AssetItem, Asset.asset_item_id == AssetItem.id)
                if category_id:
                    warranty_query = warranty_query.filter(AssetItem.item_category_id == category_id)
                if brand_id:
                    warranty_query = warranty_query.filter(AssetItem.brand_id == brand_id)

            warranty_expiring_soon = warranty_query.order_by(
                Asset.warranty_expiry.asc()
            ).limit(10).all()

            # ============================================
            # QUERY 7: TOP ASSETS BY VALUE - AS OF END DATE
            # ============================================

            top_assets_query = db_session.query(
                Asset,
                AssetItem.asset_name,
                InventoryLocation.location.label('location_name'),
                Department.department_name,
                Employee,
                Vendor.vendor_name.label('supplier_name')
            ).outerjoin(
                AssetItem, Asset.asset_item_id == AssetItem.id
            ).outerjoin(
                InventoryLocation, Asset.location_id == InventoryLocation.id
            ).outerjoin(
                Department, Asset.department_id == Department.id
            ).outerjoin(
                Employee, Asset.assigned_to_id == Employee.id
            ).outerjoin(
                Vendor, Asset.supplier_id == Vendor.id
            ).filter(
                Asset.app_id == app_id,
                Asset.status.in_(['in_stock', 'assigned', 'maintenance']),
                Asset.purchase_date <= end_date_obj  # ✅ Assets purchased up to end date
            )

            # Apply all filters to top assets query
            if asset_type_id:
                top_assets_query = top_assets_query.filter(Asset.asset_item_id == asset_type_id)
            if location_id:
                top_assets_query = top_assets_query.filter(Asset.location_id == location_id)
            if department_id:
                top_assets_query = top_assets_query.filter(Asset.department_id == department_id)
            if assigned_to_id:
                top_assets_query = top_assets_query.filter(Asset.assigned_to_id == assigned_to_id)
            if supplier_id:
                top_assets_query = top_assets_query.filter(Asset.supplier_id == supplier_id)
            if project_id:
                top_assets_query = top_assets_query.filter(Asset.project_id == project_id)
            if status:
                top_assets_query = top_assets_query.filter(Asset.status == status)
            if condition:
                top_assets_query = top_assets_query.filter(Asset.condition == condition)

            # Apply category and brand filters
            if category_id or brand_id:
                if category_id:
                    top_assets_query = top_assets_query.filter(AssetItem.item_category_id == category_id)
                if brand_id:
                    top_assets_query = top_assets_query.filter(AssetItem.brand_id == brand_id)

            top_assets_query = top_assets_query.order_by(
                Asset.current_value.desc()
            ).limit(10).all()

            # Format top assets data
            top_assets_data = []
            for asset, asset_type_name, location_name, department_name, employee, supplier_name in top_assets_query:
                # Calculate depreciation
                purchase_price = float(asset.purchase_price or 0)
                current_value = float(asset.current_value or 0)
                depreciation = purchase_price - current_value
                dep_pct = (depreciation / purchase_price * 100) if purchase_price > 0 else 0

                # Format assigned to
                assigned_to_name = None
                if employee:
                    assigned_to_name = f"{employee.first_name} {employee.last_name}".strip()

                # Determine location display
                location_display = location_name or department_name or assigned_to_name or '—'

                top_assets_data.append({
                    'id': asset.id,
                    'asset_tag': asset.asset_tag,
                    'asset_type': asset_type_name or '—',
                    'serial_number': asset.serial_number or '—',
                    'status': asset.status,
                    'status_label': asset.status.replace('_', ' ').title(),
                    'location': location_display,
                    'purchase_date': asset.purchase_date.strftime('%Y-%m-%d') if asset.purchase_date else '—',
                    'purchase_price': round(purchase_price, 2),
                    'current_value': round(current_value, 2),
                    'depreciation': round(depreciation, 2),
                    'depreciation_pct': round(dep_pct, 1)
                })

            # ============================================
            # FORMAT RESPONSE DATA
            # ============================================

            # Summary metrics
            summary_metrics = [
                {"total_assets": total_assets},
                {"total_current_value": round(total_current_value, 2)},
                {"total_depreciation": round(total_depreciation, 2)},
                {"depreciation_rate": round(depreciation_rate, 1)},
                {"total_movements": len(movements)}
            ]

            # Status distribution
            status_data = []
            for status_key, count in status_counts.items():
                if status_key:
                    status_data.append({
                        "status": status_key.replace('_', ' ').title(),
                        "count": count,
                        "value": status_key
                    })

            # Condition distribution
            condition_data = []
            for condition_key, count in condition_counts.items():
                if condition_key:
                    condition_data.append({
                        "condition": condition_key.title(),
                        "count": count,
                        "value": condition_key
                    })

            # Movement transactions for charts
            movement_data = []
            for movement in movements:
                line_item, header, asset, asset_item, location, department, employee, vendor = movement

                # Determine from/to display
                from_display = None
                to_display = None

                if line_item.from_location_id:
                    from_loc = db_session.query(InventoryLocation).filter_by(id=line_item.from_location_id).first()
                    from_display = from_loc.location if from_loc else None
                elif line_item.from_department_id:
                    from_dept = db_session.query(Department).filter_by(id=line_item.from_department_id).first()
                    from_display = from_dept.department_name if from_dept else None

                if line_item.to_location_id:
                    to_loc = db_session.query(InventoryLocation).filter_by(id=line_item.to_location_id).first()
                    to_display = to_loc.location if to_loc else None
                elif line_item.to_department_id:
                    to_dept = db_session.query(Department).filter_by(id=line_item.to_department_id).first()
                    to_display = to_dept.department_name if to_dept else None
                elif line_item.assigned_to_id:
                    emp = db_session.query(Employee).filter_by(id=line_item.assigned_to_id).first()
                    to_display = f"{emp.first_name} {emp.last_name}".strip() if emp else None

                movement_data.append({
                    "id": line_item.id,
                    "movement_id": header.id,
                    "movement_type": header.movement_type,
                    "movement_type_label": header.movement_type.replace('_', ' ').title(),
                    "transaction_date": header.transaction_date.strftime('%Y-%m-%d'),
                    "reference": header.reference,
                    "asset_id": asset.id if asset else None,
                    "asset_tag": asset.asset_tag if asset else None,
                    "asset_type": asset_item.asset_name if asset_item else None,
                    "from_location": from_display,
                    "to_location": to_display,
                    "assigned_to": f"{employee.first_name} {employee.last_name}".strip() if employee else None,
                    "party_name": vendor.vendor_name if vendor else None,
                    "transaction_value": float(line_item.transaction_value or 0),
                    "line_notes": line_item.line_notes
                })

            # Asset type distribution
            asset_type_data = []
            for at in asset_type_query:
                asset_type_data.append({
                    "id": at.id,
                    "name": at.asset_name,
                    "code": at.asset_code,
                    "asset_count": at.asset_count or 0,
                    "total_purchase": float(at.total_purchase or 0),
                    "total_current": float(at.total_current or 0)
                })

            # Location distribution
            location_data = []
            for loc in location_distribution:
                location_data.append({
                    "id": loc.id,
                    "name": loc.location,
                    "asset_count": loc.asset_count or 0,
                    "total_value": float(loc.total_value or 0)
                })

            # Acquisition timeline
            timeline_data = []
            for tl in acquisition_timeline:
                timeline_data.append({
                    "month": tl.month,
                    "asset_count": tl.asset_count or 0,
                    "total_value": float(tl.total_value or 0)
                })

            # Warranty expiring soon
            warranty_data = []
            for asset in warranty_expiring_soon:
                warranty_data.append({
                    "id": asset.id,
                    "asset_tag": asset.asset_tag,
                    "asset_type": asset.asset_item.asset_name if asset.asset_item else None,
                    "serial_number": asset.serial_number,
                    "warranty_expiry": asset.warranty_expiry.strftime('%Y-%m-%d') if asset.warranty_expiry else None,
                    "days_until": (asset.warranty_expiry - today).days if asset.warranty_expiry else 0,
                    "location": asset.location.location if asset.location else None,
                    "assigned_to": f"{asset.assigned_to.first_name} {asset.assigned_to.last_name}".strip() if asset.assigned_to else None
                })

            response_data = [
                summary_metrics,           # [0] Summary cards - as of end date
                status_data,              # [1] Status distribution - as of end date
                condition_data,           # [2] Condition distribution - as of end date
                movement_data,            # [3] Movement transactions - within date range
                asset_type_data,          # [4] Asset type distribution - as of end date
                location_data,            # [5] Location distribution - as of end date
                timeline_data,            # [6] Acquisition timeline - within date range
                warranty_data,            # [7] Warranty expiring soon - as of end date
                top_assets_data           # [8] Top assets by value - as of end date
            ]

            return jsonify(response_data)

        except Exception as e:
            logger.error(f'Error retrieving asset transactions: {str(e)}\n{traceback.format_exc()}')
            return jsonify({"error": str(e)}), 500


@api_routes.route('/api/wms/dashboard', methods=["GET"])
def get_wms_dashboard_data():
    """
    API endpoint for WMS dashboard data (no finance/accounting)
    """
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as db_session:
        try:
            company = db_session.query(Company).filter_by(api_key=api_key).first()
            if not company:
                return jsonify({"error": "Invalid API key"}), 403

            app_id = company.id
            location_id = request.args.get("location")
            category_id = request.args.get("category")
            status_filter = request.args.get("status")

            # ========== INVENTORY DATA (WMS Focused) ==========
            inventory_query = db_session.query(
                InventoryTransactionDetail,
                InventoryLocation.location.label('location_name'),
                InventoryItem.item_name,
                InventoryItem.item_code,
                InventoryItem.reorder_point,
                InventoryCategory.category_name.label('category_name'),
                InventoryItemVariation.variation_name
            ).join(
                InventoryLocation, InventoryTransactionDetail.location_id == InventoryLocation.id
            ).join(
                InventoryItemVariationLink, InventoryTransactionDetail.item_id == InventoryItemVariationLink.id
            ).join(
                InventoryItem, InventoryItemVariationLink.inventory_item_id == InventoryItem.id
            ).outerjoin(
                InventoryItemVariation, InventoryItemVariationLink.inventory_item_variation_id == InventoryItemVariation.id
            ).outerjoin(
                InventoryCategory, InventoryItem.item_category_id == InventoryCategory.id
            ).filter(
                InventoryTransactionDetail.app_id == app_id
            )

            if location_id:
                inventory_query = inventory_query.filter(InventoryTransactionDetail.location_id == location_id)
            if category_id:
                inventory_query = inventory_query.filter(InventoryItem.item_category_id == category_id)

            results = inventory_query.all()

            # Calculate current stock
            stock_map = {}
            item_details = {}
            category_stock = {}
            low_stock_count = 0
            out_of_stock_count = 0
            in_stock_count = 0
            total_quantity = 0

            for result in results:
                trans_detail = result[0]
                location_name = result[1]
                item_name = result[2]
                item_code = result[3]
                reorder_point = result[4] or 0
                category_name = result[5] or 'Uncategorized'
                variation_name = result[6]

                key = (trans_detail.item_id, trans_detail.location_id)
                stock_map[key] = stock_map.get(key, 0) + (trans_detail.quantity or 0)

                if key not in item_details:
                    item_details[key] = {
                        'name': item_name,
                        'code': item_code,
                        'category': category_name,
                        'variation': variation_name,
                        'location': location_name,
                        'reorder_point': reorder_point
                    }

            # Process stock levels
            top_items = []
            for key, quantity in stock_map.items():
                total_quantity += quantity
                details = item_details.get(key, {})
                reorder_point = details.get('reorder_point', 0)

                if quantity <= 0:
                    out_of_stock_count += 1
                    status = 'out_of_stock'
                elif quantity <= reorder_point:
                    low_stock_count += 1
                    status = 'low_stock'
                else:
                    in_stock_count += 1
                    status = 'in_stock'

                top_items.append({
                    'name': details.get('name', 'Unknown'),
                    'sku': details.get('code', '-'),
                    'location': details.get('location', '-'),
                    'quantity': quantity,
                    'status': status
                })

                # Category stock
                cat = details.get('category', 'Uncategorized')
                category_stock[cat] = category_stock.get(cat, 0) + quantity

            top_items.sort(key=lambda x: x['quantity'], reverse=True)

            # Get recent movements
            recent_movements = []
            for result in results[:50]:
                trans_detail = result[0]
                movement_type = 'stock_in' if trans_detail.quantity > 0 else 'stock_out'
                recent_movements.append({
                    'date': trans_detail.transaction_date.strftime('%Y-%m-%d'),
                    'name': result[2],
                    'type': movement_type,
                    'type_label': 'Stock In' if movement_type == 'stock_in' else 'Stock Out',
                    'quantity': abs(trans_detail.quantity or 0),
                    'location': result[1],
                    'reference': trans_detail.reference_number
                })

            # ========== ASSET DATA (WMS Focused) ==========
            asset_query = db_session.query(
                Asset,
                AssetItem.asset_name.label('asset_type'),
                InventoryLocation.location.label('location_name')
            ).outerjoin(
                AssetItem, Asset.asset_item_id == AssetItem.id
            ).outerjoin(
                InventoryLocation, Asset.location_id == InventoryLocation.id
            ).filter(
                Asset.app_id == app_id
            )

            if location_id:
                asset_query = asset_query.filter(Asset.location_id == location_id)
            if status_filter:
                asset_query = asset_query.filter(Asset.status == status_filter)

            assets = asset_query.all()

            total_assets = 0
            assets_in_stock = 0
            assets_assigned = 0
            assets_maintenance = 0
            condition_counts = {'excellent': 0, 'good': 0, 'fair': 0, 'poor': 0}
            asset_type_counts = {}
            asset_location_counts = {}
            warranty_expiring = []
            assets_list = []

            today = date.today()
            ninety_days = today + timedelta(days=90)

            for asset, asset_type, location_name in assets:
                total_assets += 1

                # Status counts
                if asset.status == 'in_stock':
                    assets_in_stock += 1
                elif asset.status == 'assigned':
                    assets_assigned += 1
                elif asset.status == 'maintenance':
                    assets_maintenance += 1

                # Condition counts
                if asset.condition:
                    condition_counts[asset.condition] = condition_counts.get(asset.condition, 0) + 1

                # Asset type counts
                type_name = asset_type or 'Other'
                asset_type_counts[type_name] = asset_type_counts.get(type_name, 0) + 1

                # Location counts
                loc_name = location_name or 'Unknown'
                asset_location_counts[loc_name] = asset_location_counts.get(loc_name, 0) + 1

                # Assets list for table
                assets_list.append({
                    'asset_tag': asset.asset_tag,
                    'asset_type': type_name,
                    'serial_number': asset.serial_number,
                    'location': location_name or '-',
                    'condition': asset.condition or 'good',
                    'status': asset.status
                })

                # Warranty expiring soon
                if asset.warranty_expiry and asset.warranty_expiry >= today and asset.warranty_expiry <= ninety_days:
                    days_left = (asset.warranty_expiry - today).days
                    warranty_expiring.append({
                        'asset_tag': asset.asset_tag,
                        'serial_number': asset.serial_number,
                        'expiry_date': asset.warranty_expiry.strftime('%Y-%m-%d'),
                        'days_left': days_left,
                        'location': location_name or '-'
                    })

            warranty_expiring.sort(key=lambda x: x['days_left'])

            # Monthly movements data
            monthly_movements = len([m for m in recent_movements if '202' in m['date']])  # Simplified

            response = {
                "total_skus": len(stock_map),
                "total_assets": total_assets,
                "total_quantity": total_quantity,
                "total_locations": len(set([d.get('location', '-') for d in item_details.values()])),
                "in_stock_count": in_stock_count,
                "low_stock_count": low_stock_count,
                "out_of_stock_count": out_of_stock_count,
                "monthly_movements": monthly_movements,
                "assets_in_stock": assets_in_stock,
                "assets_assigned": assets_assigned,
                "assets_maintenance": assets_maintenance,
                "warranty_expiring": len(warranty_expiring),
                "stock_by_category": [{"name": k, "quantity": v} for k, v in category_stock.items()],
                "asset_conditions": [{"condition": k, "count": v} for k, v in condition_counts.items() if v > 0],
                "asset_types": [{"name": k, "count": v} for k, v in sorted(asset_type_counts.items(), key=lambda x: x[1], reverse=True)[:10]],
                "asset_locations": [{"name": k, "count": v} for k, v in sorted(asset_location_counts.items(), key=lambda x: x[1], reverse=True)[:6]],
                "top_items": top_items[:10],
                "assets": assets_list[:20],
                "warranties": warranty_expiring[:10],
                "movements": recent_movements,
                "recent_movements": recent_movements[:20]
            }

            return jsonify(response)

        except Exception as e:
            logger.error(f"Error in WMS dashboard API: {str(e)}\n{traceback.format_exc()}")
            return jsonify({"error": str(e)}), 500

@api_routes.route('/api/assets/export_dashboard', methods=["POST"])
def export_asset_dashboard():
    """
    Export asset dashboard data to CSV
    """
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    data = request.get_json()
    report_type = data.get('report_type', 'assets')

    with Session() as db_session:
        try:
            company = db_session.query(Company).filter_by(api_key=api_key).first()
            if not company:
                return jsonify({"error": "Invalid API key"}), 403

            app_id = company.id

            import csv
            from io import StringIO
            from flask import make_response

            output = StringIO()
            writer = csv.writer(output)

            if report_type == 'assets':
                # Export all assets
                writer.writerow([
                    'Asset Tag', 'Serial Number', 'Asset Type', 'Status', 'Condition',
                    'Location', 'Department', 'Assigned To', 'Purchase Date',
                    'Purchase Price', 'Current Value', 'Depreciation', 'Depreciation %',
                    'Supplier', 'Project', 'Warranty Expiry'
                ])

                assets = db_session.query(Asset).filter(Asset.app_id == app_id).all()
                for asset in assets:
                    depreciation = (asset.purchase_price or 0) - (asset.current_value or 0)
                    dep_pct = (depreciation / (asset.purchase_price or 1) * 100) if asset.purchase_price else 0

                    writer.writerow([
                        asset.asset_tag,
                        asset.serial_number or '',
                        asset.asset_item.asset_name if asset.asset_item else '',
                        asset.status.replace('_', ' ').title(),
                        asset.condition.title() if asset.condition else '',
                        asset.location.location if asset.location else '',
                        asset.department.department_name if asset.department else '',
                        f"{asset.assigned_to.first_name} {asset.assigned_to.last_name}".strip() if asset.assigned_to else '',
                        asset.purchase_date.strftime('%Y-%m-%d') if asset.purchase_date else '',
                        round(float(asset.purchase_price or 0), 2),
                        round(float(asset.current_value or 0), 2),
                        round(depreciation, 2),
                        f"{round(dep_pct, 1)}%",
                        asset.supplier.vendor_name if asset.supplier else '',
                        asset.project.name if asset.project else '',
                        asset.warranty_expiry.strftime('%Y-%m-%d') if asset.warranty_expiry else ''
                    ])

            elif report_type == 'movements':
                # Export asset movements
                writer.writerow([
                    'Date', 'Reference', 'Movement Type', 'Asset Tag', 'Asset Type',
                    'From Location/Dept', 'To Location/Dept/Employee', 'Party', 'Transaction Value'
                ])

                movements = db_session.query(
                    AssetMovement, AssetMovementLineItem, Asset, AssetItem
                ).join(
                    AssetMovementLineItem,
                    AssetMovement.id == AssetMovementLineItem.asset_movement_id
                ).outerjoin(
                    Asset,
                    AssetMovementLineItem.asset_id == Asset.id
                ).outerjoin(
                    AssetItem,
                    Asset.asset_item_id == AssetItem.id
                ).filter(
                    AssetMovement.app_id == app_id
                ).order_by(
                    AssetMovement.transaction_date.desc()
                ).limit(1000).all()

                for movement, line_item, asset, asset_item in movements:
                    writer.writerow([
                        movement.transaction_date.strftime('%Y-%m-%d'),
                        movement.reference or '',
                        movement.movement_type.replace('_', ' ').title(),
                        asset.asset_tag if asset else '',
                        asset_item.asset_name if asset_item else '',
                        line_item.from_location_id or '',
                        line_item.to_location_id or line_item.assigned_to_id or '',
                        line_item.party_id or '',
                        round(float(line_item.transaction_value or 0), 2)
                    ])

            elif report_type == 'warranty':
                # Export warranty report
                writer.writerow([
                    'Asset Tag', 'Serial Number', 'Asset Type', 'Warranty Expiry',
                    'Days Remaining', 'Location', 'Assigned To', 'Status'
                ])

                today = date.today()
                assets = db_session.query(Asset).filter(
                    Asset.app_id == app_id,
                    Asset.warranty_expiry.isnot(None)
                ).order_by(Asset.warranty_expiry).all()

                for asset in assets:
                    days_remaining = (asset.warranty_expiry - today).days if asset.warranty_expiry else 0
                    writer.writerow([
                        asset.asset_tag,
                        asset.serial_number or '',
                        asset.asset_item.asset_name if asset.asset_item else '',
                        asset.warranty_expiry.strftime('%Y-%m-%d') if asset.warranty_expiry else '',
                        days_remaining,
                        asset.location.location if asset.location else '',
                        f"{asset.assigned_to.first_name} {asset.assigned_to.last_name}".strip() if asset.assigned_to else '',
                        asset.status.replace('_', ' ').title()
                    ])

            output.seek(0)
            filename = f"asset_{report_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            response = make_response(output.getvalue())
            response.headers["Content-Disposition"] = f"attachment; filename={filename}"
            response.headers["Content-type"] = "text/csv"

            return response

        except Exception as e:
            logger.error(f'Error exporting asset data: {str(e)}\n{traceback.format_exc()}')
            return jsonify({"error": str(e)}), 500
