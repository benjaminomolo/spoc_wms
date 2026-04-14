import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

from flask import request, jsonify, render_template, flash, redirect, url_for, abort
from flask_login import login_required, current_user
from sqlalchemy import or_, func, literal, desc, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from ai import get_base_currency
from db import Session
from models import Company, Module, InventoryLocation, ChartOfAccounts, Vendor, DirectSalesTransaction, \
    InventoryItemVariationLink, DirectSaleItem, Project, PaymentMode, UnitOfMeasurement, Currency, PaymentAllocation, \
    SalesTransaction, SalesPaymentStatus, SalesInvoice, InvoiceStatus, OrderStatus, CustomerCredit, \
    DirectPurchaseTransaction, InventoryItem, DirectPurchaseItem, PurchasePaymentAllocation, GoodsReceipt, \
    GoodsReceiptItem, PurchaseReturn, PurchaseOrder, PurchaseOrderItem, PurchaseOrderStatusLog, PurchaseOrderHistory, \
    PurchaseOrderNote, PurchaseTransaction, JournalEntry, PurchasePaymentStatus, Journal
from services.inventory_helpers import safe_clear_stock_history_cache
from services.post_to_ledger import post_direct_sale_cogs_to_ledger, post_sales_transaction_to_ledger, \
    bulk_post_sales_transactions, post_customer_credit_to_ledger, \
    post_purchase_transaction_to_ledger, bulk_post_purchase_transactions, bulk_post_goods_receipts
from services.post_to_ledger_reversal import reverse_direct_sales_posting, reverse_sales_invoice_posting, \
    delete_journal_entries_by_source, reverse_po_transaction_posting
from services.purchases_helpers import generate_direct_purchase_number, allocate_direct_purchase_payment, \
    get_inventory_entries_for_direct_purchase, reverse_purchase_inventory_entries, reverse_direct_purchase_posting, \
    generate_next_purchase_order_number
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    get_inventory_entries_for_direct_sale, reverse_sales_inventory_entries, allocate_payment
from services.vendors_and_customers import get_or_create_customer_credit_account
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj
from utils_and_helpers.lists import check_list_not_empty
from . import purchases_bp

import logging

logger = logging.getLogger(__name__)


@purchases_bp.route('/transactions', methods=["GET"])
@login_required
def view_purchase_transactions():
    app_id = current_user.app_id
    db_session = Session()

    try:
        # Default parameters for initial page load
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        start_date = request.args.get('start_date', None)
        end_date = request.args.get('end_date', None)
        filter_type = request.args.get('filter_type', 'payment_date')
        status_filter = request.args.get('status', None)
        vendor_id = request.args.get('vendor', None)
        payment_mode_id = request.args.get('payment_mode', None)
        purchase_order_number = request.args.get('purchase_order_number', None)
        direct_purchase_number = request.args.get('direct_purchase_number', None)
        payment_status = request.args.get('payment_status', None)
        reference = request.args.get('reference', None)
        transaction_type = request.args.get('transaction_type', None)  # ADD: Transaction type filter
        filter_applied = bool(start_date or end_date or status_filter or vendor_id or
                              payment_mode_id or purchase_order_number or direct_purchase_number or
                              payment_status or reference or transaction_type)

        # Fetch company details
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Fetch filter dropdown data
        vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers', 'partner', 'other']

        # Normalize to lowercase for consistency
        vendor_types = [v.lower() for v in vendor_types]

        # Query vendors whose vendor_type matches any in the list
        vendors = (
            db_session.query(Vendor)
            .filter(
                Vendor.app_id == app_id,
                func.lower(Vendor.vendor_type).in_(vendor_types)
            )
            .all()
        )

        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id, is_active=True).all()


        # Get filtered transactions data
        combined_transactions, pagination_data = _get_purchase_transactions_data(
            db_session, page, per_page, start_date, end_date, filter_type,
            status_filter, vendor_id, payment_mode_id, purchase_order_number,
            direct_purchase_number, payment_status, reference, transaction_type=transaction_type
        )

        # In your view_purchase_transactions route, add:
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        # Render template for GET
        return render_template(
            '/purchases/purchase_transactions.html',
            purchase_transactions=combined_transactions,
            vendors=vendors,
            pagination=pagination_data,
            currencies=currencies,
            filters={
                'start_date': start_date,
                'end_date': end_date,
                'filter_type': filter_type,
                'status': status_filter,
                'vendor': vendor_id,
                'payment_mode': payment_mode_id,
                'purchase_order_number': purchase_order_number,
                'direct_purchase_number': direct_purchase_number,
                'payment_status': payment_status,
                'reference': reference
            },
            company=company,
            role=role,
            module_name="Purchase",
            modules=modules_data,
            filter_applied=filter_applied
        )

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error in view_purchase_transactions: {str(e)}")
        flash('An error occurred while retrieving purchase transactions.', 'error')
        return render_template('error.html', message='Database error occurred'), 500

    except Exception as e:
        logger.error(f'Unexpected error in view_purchase_transactions: {str(e)}\n{traceback.format_exc()}')
        flash('An unexpected error occurred.', 'error')
        return render_template('error.html', message='Unexpected error occurred'), 500

    finally:
        db_session.close()


@purchases_bp.route('/transactions/filter', methods=["GET", "POST"])
@login_required
def purchase_transactions_filter():
    """
    Return filtered JSON data for AJAX requests (GET or POST).
    """
    db_session = Session()

    try:
        # Handle both GET and POST requests
        if request.method == 'GET':
            # Get parameters from query string for GET requests
            page = request.args.get('page', 1, type=int)
            per_page = request.args.get('per_page', 20, type=int)
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            filter_type = request.args.get('filter_type', 'payment_date')
            status_filter = request.args.get('status')
            vendor_id = request.args.get('vendor')
            payment_mode_id = request.args.get('payment_mode')
            purchase_order_number = request.args.get('purchase_order_number')
            direct_purchase_number = request.args.get('direct_purchase_number')
            payment_status = request.args.get('payment_status')
            reference = request.args.get('reference')
            currency = request.args.get('currency')
            transaction_type = request.args.get('transaction_type')
        else:
            # Get JSON data from POST request
            data = request.get_json() or {}
            page = int(data.get('page', 1))
            per_page = int(data.get('per_page', 20))
            start_date = data.get('start_date')
            end_date = data.get('end_date')
            filter_type = data.get('filter_type', 'payment_date')
            status_filter = data.get('status')
            vendor_id = data.get('vendor')
            payment_mode_id = data.get('payment_mode')
            purchase_order_number = data.get('purchase_order_number')
            direct_purchase_number = data.get('direct_purchase_number')
            payment_status = data.get('payment_status')
            reference = data.get('reference')
            currency = data.get('currency')
            transaction_type = data.get('transaction_type')

        # Get filtered data
        combined_transactions, pagination_data = _get_purchase_transactions_data(
            db_session, page, per_page, start_date, end_date, filter_type,
            status_filter, vendor_id, payment_mode_id, purchase_order_number,
            direct_purchase_number, payment_status, reference, currency, transaction_type=transaction_type
        )

        # Return JSON for AJAX filtering
        return jsonify({
            'success': True,
            'purchase_transactions': combined_transactions,
            'pagination': pagination_data,
            'filters': {
                'start_date': start_date,
                'end_date': end_date,
                'filter_type': filter_type,
                'status': status_filter,
                'vendor': vendor_id,
                'payment_mode': payment_mode_id,
                'purchase_order_number': purchase_order_number,
                'direct_purchase_number': direct_purchase_number,
                'payment_status': payment_status,
                'reference': reference,
                'currency': currency,
                'transaction_type': transaction_type
            }
        }), 200

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error in purchase_transactions_filter: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred while filtering purchase transactions.'}), 500

    except Exception as e:
        logger.error(f'Unexpected error in purchase_transactions_filter: {str(e)}\n{traceback.format_exc()}')
        return jsonify({'success': False, 'message': 'An unexpected error occurred.'}), 500

    finally:
        db_session.close()


def _get_purchase_transactions_data(db_session, page, per_page, start_date, end_date, filter_type,
                                    status_filter, vendor_id, payment_mode_id, purchase_order_number,
                                    direct_purchase_number, payment_status, reference, currency_id=None,
                                    transaction_type=None):
    """
    Helper function to get filtered purchase transactions data with pagination.
    """
    app_id = current_user.app_id

    # Base queries for PO-based and direct purchase transactions
    po_based_query = db_session.query(PurchaseTransaction).filter(
        PurchaseTransaction.app_id == app_id
    )

    direct_purchase_query = db_session.query(DirectPurchaseTransaction).filter_by(app_id=app_id)

    # Apply transaction type filter
    if transaction_type:
        if transaction_type == 'purchase_order':
            # Only show purchase order transactions
            direct_purchase_query = direct_purchase_query.filter(literal(False))  # Exclude direct purchases
        elif transaction_type == 'direct':
            # Only show direct purchase transactions
            po_based_query = po_based_query.filter(literal(False))  # Exclude purchase orders
    # Apply date filters based on filter type
    if start_date or end_date:
        try:
            if start_date:
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if end_date:
                end_date = datetime.strptime(end_date, '%Y-%m-%d')

            if filter_type == 'payment_date':
                if start_date:
                    po_based_query = po_based_query.filter(PurchaseTransaction.payment_date >= start_date)
                    direct_purchase_query = direct_purchase_query.filter(
                        DirectPurchaseTransaction.payment_date >= start_date)
                if end_date:
                    po_based_query = po_based_query.filter(PurchaseTransaction.payment_date <= end_date)
                    direct_purchase_query = direct_purchase_query.filter(
                        DirectPurchaseTransaction.payment_date <= end_date)
            elif filter_type == 'order_date':
                if start_date:
                    po_based_query = po_based_query.join(PurchaseOrder).filter(
                        PurchaseOrder.purchase_order_date >= start_date)
                if end_date:
                    po_based_query = po_based_query.join(PurchaseOrder).filter(
                        PurchaseOrder.purchase_order_date <= end_date)
        except ValueError:
            raise ValueError('Invalid date format. Please use YYYY-MM-DD.')

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
        elif status_filter == 'draft':
            po_based_query = po_based_query.filter(PurchaseTransaction.payment_status == 'draft')
            direct_purchase_query = direct_purchase_query.filter(
                DirectPurchaseTransaction.status == 'draft')
        elif status_filter == 'cancelled':
            po_based_query = po_based_query.filter(PurchaseTransaction.payment_status in ['canceled', 'cancelled'])
            direct_purchase_query = direct_purchase_query.filter(
                DirectPurchaseTransaction.status in ['canceled', 'cancelled'])

    # Apply currency filter
    if currency_id:
        po_based_query = po_based_query.filter(PurchaseTransaction.currency_id == currency_id)
        direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.currency_id == currency_id)

    # Apply vendor filter
    if vendor_id:
        po_based_query = po_based_query.filter(PurchaseTransaction.vendor_id == vendor_id)
        direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.vendor_id == vendor_id)

    # Apply payment mode filter
    if payment_mode_id:
        po_based_query = po_based_query.join(PurchasePaymentAllocation).filter(
            PurchasePaymentAllocation.payment_mode == payment_mode_id)
        direct_purchase_query = direct_purchase_query.join(PurchasePaymentAllocation).filter(
            PurchasePaymentAllocation.payment_mode == payment_mode_id)

    # Apply purchase order number filter
    # Apply purchase order number filter
    if purchase_order_number:
        po_based_query = po_based_query.join(PurchaseOrder).filter(
            PurchaseOrder.purchase_order_number.ilike(f'%{purchase_order_number}%')
        )
        # Exclude direct purchases when filtering by PO number
        direct_purchase_query = direct_purchase_query.filter(literal(False))

    # Apply direct purchase number filter
    # Apply direct purchase number filter
    if direct_purchase_number:
        direct_purchase_query = direct_purchase_query.filter(
            DirectPurchaseTransaction.direct_purchase_number.ilike(f'%{direct_purchase_number}%')
        )
        # Exclude purchase orders when filtering by direct purchase number
        po_based_query = po_based_query.filter(literal(False))

    # Apply payment status filter
    if payment_status:
        po_based_query = po_based_query.filter(PurchaseTransaction.payment_status == payment_status)
        direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.payment_status == payment_status)

    # Apply reference filter
    if reference:
        po_based_query = po_based_query.filter(
            or_(
                PurchaseTransaction.reference_number.ilike(f'%{reference}%'),
                PurchaseOrder.purchase_order_reference.ilike(f'%{reference}%')
            )
        )
        direct_purchase_query = direct_purchase_query.filter(
            DirectPurchaseTransaction.purchase_reference.ilike(f'%{reference}%')
        )

    # Get total counts for pagination
    po_total = po_based_query.count()
    direct_total = direct_purchase_query.count()
    total_transactions = po_total + direct_total

    # Calculate pagination
    total_pages = (total_transactions + per_page - 1) // per_page
    offset = (page - 1) * per_page

    # Apply pagination - we'll handle this manually since we have two different queries
    po_transactions = po_based_query.order_by(PurchaseTransaction.created_at.desc()).offset(offset).limit(
        per_page).all()
    direct_transactions = direct_purchase_query.order_by(DirectPurchaseTransaction.created_at.desc()).offset(
        offset).limit(per_page).all()

    # Combine and process transactions
    combined_transactions = []

    # Process PO-based transactions
    for transaction in po_transactions:
        transaction_data = _process_po_transaction(db_session, transaction, app_id)
        combined_transactions.append(transaction_data)

    # Process direct purchase transactions
    for transaction in direct_transactions:
        transaction_data = _process_direct_purchase_transaction(db_session, transaction, app_id)
        combined_transactions.append(transaction_data)

    # Sort combined transactions by created_at descending and apply pagination
    combined_transactions.sort(key=lambda x: x.get('created_at', datetime.min), reverse=True)
    paginated_transactions = combined_transactions[:per_page]

    pagination_data = {
        'page': page,
        'per_page': per_page,
        'total': total_transactions,
        'pages': total_pages,
        'has_next': page < total_pages,
        'has_prev': page > 1
    }

    return paginated_transactions, pagination_data


def _process_po_transaction(db_session, transaction, app_id):
    """Process a single PO-based transaction."""
    po_id = transaction.purchase_order_id
    total_po_amount = transaction.purchase_orders.total_amount if transaction.purchase_orders else 0

    # Calculate total paid for this PO, excluding cancelled transactions
    total_paid = db_session.query(func.sum(PurchaseTransaction.amount_paid)) \
                     .filter(PurchaseTransaction.purchase_order_id == po_id,
                             PurchaseTransaction.app_id == app_id,
                             PurchaseTransaction.payment_status != PurchasePaymentStatus.cancelled) \
                     .scalar() or 0

    # Calculate remaining balance
    remaining_balance = total_po_amount - total_paid

    # Calculate unposted transactions count
    unposted_goods_receipt_count = db_session.query(GoodsReceiptItem). \
        join(GoodsReceipt, GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id). \
        join(PurchaseOrderItem, GoodsReceiptItem.purchase_order_item_id == PurchaseOrderItem.id). \
        filter(
        GoodsReceipt.purchase_order_id == po_id,
        PurchaseOrderItem.item_type == "inventory",
        GoodsReceiptItem.is_posted_to_ledger == False
    ).count()

    unposted_purchase_returns_count = db_session.query(PurchaseReturn).filter_by(
        purchase_order_id=po_id, is_posted_to_ledger=False).count()
    unposted_payment_allocations_count = db_session.query(PurchasePaymentAllocation).filter_by(
        payment_id=transaction.id, is_posted_to_ledger=False).count()
    unposted_total = unposted_goods_receipt_count + unposted_purchase_returns_count + unposted_payment_allocations_count

    # Get account name from payment allocation
    account_name = None
    if transaction.payment_allocations and transaction.payment_allocations[0].chart_of_accounts_asset:
        account_name = transaction.payment_allocations[0].chart_of_accounts_asset.sub_category

    return {
        'id': transaction.id,
        'po_id': transaction.purchase_order_id,
        'type': 'purchase_order',
        'po_number': transaction.purchase_orders.purchase_order_number if transaction.purchase_orders else None,
        'vendor_name': transaction.vendor.vendor_name,
        'payment_date': transaction.payment_date.strftime('%Y-%m-%d') if transaction.payment_date else None,
        'amount_paid': float(transaction.amount_paid),
        'currency': transaction.currency.user_currency if transaction.currency else None,
        'reference_number': transaction.reference_number,
        'account_name': account_name,
        'total_po_amount': float(total_po_amount),
        'total_paid': float(total_paid),
        'remaining_balance': float(remaining_balance),
        'payment_mode': transaction.payment_allocations[0].payment_modes.payment_mode
        if transaction.payment_allocations and transaction.payment_allocations[0].payment_modes
        else None,
        'posted_to_ledger': transaction.is_posted_to_ledger,
        'status': transaction.payment_status.name if transaction.payment_status else None,
        'payment_progress': "Final" if remaining_balance == 0 else "Ongoing" if total_paid > transaction.amount_paid else "Initial",
        'unposted_total': unposted_total,
        'created_at': transaction.created_at
    }


def _process_direct_purchase_transaction(db_session, transaction, app_id):
    """Process a single direct purchase transaction."""
    total_purchase_amount = transaction.total_amount
    total_paid = db_session.query(func.sum(DirectPurchaseTransaction.amount_paid)) \
                     .filter_by(direct_purchase_number=transaction.direct_purchase_number,
                                app_id=app_id).scalar() or 0

    remaining_balance = total_purchase_amount - total_paid

    # Calculate unposted transactions count
    unposted_goods_receipt_count = db_session.query(GoodsReceiptItem). \
        join(GoodsReceipt, GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id). \
        join(DirectPurchaseItem, GoodsReceiptItem.direct_purchase_item_id == DirectPurchaseItem.id). \
        filter(
        GoodsReceipt.direct_purchase_id == transaction.id,
        DirectPurchaseItem.item_type == "inventory",
        GoodsReceiptItem.is_posted_to_ledger == False
    ).count()
    unposted_purchase_returns_count = db_session.query(PurchaseReturn).filter_by(
        direct_purchase_id=transaction.id, is_posted_to_ledger=False).count()
    unposted_payment_allocations_count = db_session.query(PurchasePaymentAllocation).filter_by(
        direct_purchase_id=transaction.id, is_posted_to_ledger=False).count()
    unposted_total = unposted_goods_receipt_count + unposted_purchase_returns_count + unposted_payment_allocations_count
    account_name = None
    if transaction.payment_allocations and transaction.payment_allocations[0].chart_of_accounts_asset:
        account_name = transaction.payment_allocations[0].chart_of_accounts_asset.sub_category

    return {
        'id': transaction.id,
        'type': 'direct',
        'po_number': None,
        'direct_purchase_number': transaction.direct_purchase_number,
        'vendor_name': transaction.vendor.vendor_name,
        'payment_date': transaction.payment_date.strftime('%Y-%m-%d') if transaction.payment_date else None,
        'amount_paid': float(transaction.amount_paid),
        'currency': transaction.currency.user_currency if transaction.currency else None,
        'reference_number': transaction.purchase_reference,
        'account_name': account_name,
        'total_purchase_amount': float(total_purchase_amount),
        'total_paid': float(total_paid),
        'remaining_balance': float(remaining_balance),
        'payment_mode': transaction.payment_allocations[0].payment_modes.payment_mode
        if transaction.payment_allocations and transaction.payment_allocations[0].payment_modes
        else None,
        'posted_to_ledger': transaction.is_posted_to_ledger,
        'status': transaction.status.name if transaction.status else None,
        'purchase_type': "Installment" if remaining_balance > 0 else "Full",
        'unposted_total': unposted_total,
        'created_at': transaction.created_at
    }


@purchases_bp.route('/transactions/bulk_approve', methods=['POST'])
@login_required
def bulk_approve_purchase_transactions():
    db_session = Session()
    try:
        data = request.get_json()
        direct_purchase_ids = data.get('direct_purchase_ids', [])
        po_transaction_ids = data.get('po_transaction_ids', [])

        approved_direct_purchases = 0
        approved_po_transactions = 0

        # Approve direct purchases
        for direct_purchase_id in direct_purchase_ids:
            direct_purchase = db_session.query(DirectPurchaseTransaction).filter_by(id=direct_purchase_id).first()
            if direct_purchase and direct_purchase.status == OrderStatus.draft:
                # Check if this is a POS purchase (cash purchase)
                is_pos_purchase = (
                        direct_purchase.purchase_reference and
                        direct_purchase.purchase_reference.startswith('pos_') and
                        direct_purchase.payment_status == 'pending'
                )

                if is_pos_purchase:
                    # Post to ledger for POS purchases
                    success, message = bulk_post_purchase_transactions(
                        db_session=db_session,
                        direct_purchase_ids=[direct_purchase.id],
                        po_transaction_ids=[],
                        current_user=current_user,
                        is_pos_purchase=True
                    )

                    if not success:
                        logger.warning(
                            f"Warning: Failed to post POS purchase {direct_purchase.id} to ledger: {message}")
                        raise Exception(f'Failed to post POS purchase {direct_purchase.id} to ledger: {message}')
                    else:
                        # POS purchase specific handling
                        direct_purchase.status = OrderStatus.paid
                        direct_purchase.payment_status = 'full'

                else:
                    # Regular direct purchase approval logic
                    if direct_purchase.amount_paid == 0:
                        direct_purchase.status = OrderStatus.unpaid
                    elif direct_purchase.amount_paid < direct_purchase.total_amount:
                        direct_purchase.status = OrderStatus.partially_paid
                    else:
                        direct_purchase.status = OrderStatus.paid

                db_session.add(direct_purchase)
                approved_direct_purchases += 1

        # Approve PO transactions (payments against purchase orders)
        for transaction_id in po_transaction_ids:
            transaction = db_session.query(PurchaseTransaction).filter_by(id=transaction_id).first()
            if transaction and transaction.payment_status == PurchasePaymentStatus.unpaid:
                # For individual payment approval against purchase orders
                purchase_order = transaction.purchase_orders
                if purchase_order:
                    transaction.payment_status = PurchasePaymentStatus.paid

                    # Update purchase order payment status if needed
                    total_paid = sum(
                        pt.amount_paid for pt in purchase_order.purchase_transactions
                        if pt.payment_status == PurchasePaymentStatus.paid
                    )

                    if total_paid >= purchase_order.total_amount:
                        purchase_order.status = OrderStatus.paid
                    elif total_paid > 0:
                        purchase_order.status = OrderStatus.partially_paid

                    db_session.add(transaction)
                    db_session.add(purchase_order)
                    approved_po_transactions += 1

        db_session.commit()

        return jsonify({
            'success': True,
            'message': f'Approved {approved_direct_purchases} direct purchases and {approved_po_transactions} PO payments'
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error approving purchase transactions: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error approving transactions: {str(e)}'
        }), 500
    finally:
        db_session.close()


@purchases_bp.route('/transactions/bulk_post_to_ledger', methods=['POST'])
@login_required
def bulk_post_purchase_transactions_route():
    """
    Bulk mark purchase transactions as posted in ledger
    """
    db_session = Session()
    try:
        data = request.get_json()
        if not data:
            return jsonify(success=False, message="No data provided"), 400

        direct_purchase_ids = data.get('direct_purchase_ids', [])
        po_transaction_ids = data.get('po_transaction_ids', [])

        success, message = bulk_post_purchase_transactions(
            db_session=db_session,
            direct_purchase_ids=direct_purchase_ids,
            po_transaction_ids=po_transaction_ids,
            current_user=current_user
        )

        if success:
            return jsonify(success=True, message=message)
        else:
            return jsonify(success=False, message=message), 400

    except Exception as e:
        logger.error(f"Bulk purchase posting route error: {str(e)}")
        return jsonify(success=False, message=f"Internal server error: {str(e)}"), 500
    finally:
        db_session.close()


@purchases_bp.route('/goods_receipts/bulk_post_to_ledger', methods=['POST'])
@login_required
def bulk_post_goods_receipts_route():
    """
    Bulk mark goods receipts as posted in ledger
    """
    db_session = Session()
    try:
        data = request.get_json()
        if not data:
            return jsonify(success=False, message="No data provided"), 400

        goods_receipt_ids = data.get('goods_receipt_ids', [])

        if not goods_receipt_ids:
            return jsonify(success=False, message="No goods receipt IDs provided"), 400

        success, message = bulk_post_goods_receipts(
            db_session=db_session,
            goods_receipt_ids=goods_receipt_ids,
            current_user=current_user
        )

        if success:
            return jsonify(success=True, message=message)
        else:
            return jsonify(success=False, message=message), 400

    except Exception as e:
        logger.error(f"Bulk goods receipt posting route error: {str(e)}")
        return jsonify(success=False, message=f"Internal server error: {str(e)}"), 500
    finally:
        db_session.close()


@purchases_bp.route('/transactions/bulk_delete', methods=['POST'])
@login_required
def bulk_delete_purchase_transactions():
    db_session = Session()
    try:
        data = request.get_json()
        direct_purchase_ids = data.get('direct_purchase_ids', [])
        po_transaction_ids = data.get('po_transaction_ids', [])

        deleted_direct_purchases = 0
        deleted_po_transactions = 0
        error_messages = []

        # Delete direct purchases
        for direct_purchase_id in direct_purchase_ids:
            try:
                direct_purchase = db_session.query(DirectPurchaseTransaction).filter_by(id=direct_purchase_id).first()
                if not direct_purchase:
                    error_messages.append(f"Direct purchase {direct_purchase_id} not found")
                    continue

                # Always attempt reversal regardless of posting status
                try:
                    # Reverse inventory entries first
                    inventory_entries = get_inventory_entries_for_direct_purchase(db_session, direct_purchase.id,
                                                                                  current_user.app_id)
                    for entry in inventory_entries:
                        reverse_purchase_inventory_entries(db_session, entry)

                    # Reverse ledger postings
                    reverse_direct_purchase_posting(db_session, direct_purchase, current_user)

                    # Clear stock history cache
                    try:
                        clear_stock_history_cache()
                        logger.info("Successfully cleared stock history cache after inventory reversal")
                    except Exception as e:
                        logger.error(f"Cache clearing failed: {e}")
                        # Continue anyway - cache clearing failure shouldn't break the operation

                except Exception as reversal_error:
                    error_messages.append(
                        f"Failed to reverse direct purchase {direct_purchase.direct_purchase_number}: {str(reversal_error)}")
                    continue

                # Now delete the direct purchase and its related records
                # First delete goods receipts
                db_session.query(GoodsReceipt).filter_by(direct_purchase_id=direct_purchase_id).delete()

                # Delete payment allocations
                db_session.query(PurchasePaymentAllocation).filter_by(direct_purchase_id=direct_purchase_id).delete()

                # Then delete line items
                db_session.query(DirectPurchaseItem).filter_by(transaction_id=direct_purchase_id).delete()

                # Finally delete the main transaction
                db_session.delete(direct_purchase)
                deleted_direct_purchases += 1

            except Exception as e:
                error_messages.append(f"Error deleting direct purchase {direct_purchase_id}: {str(e)}")
                continue

        # Delete PO transactions
        for transaction_id in po_transaction_ids:
            try:
                transaction = db_session.query(PurchaseTransaction).filter_by(id=transaction_id).first()
                if not transaction:
                    error_messages.append(f"PO transaction {transaction_id} not found")
                    continue

                # Always attempt reversal regardless of posting status
                try:
                    reverse_po_transaction_posting(db_session, transaction, current_user)
                except Exception as reversal_error:
                    error_messages.append(
                        f"Failed to reverse PO transaction {transaction_id}: {str(reversal_error)}")
                    continue

                # Delete payment allocations
                db_session.query(PurchasePaymentAllocation).filter_by(payment_id=transaction_id).delete()

                # Delete the main transaction
                db_session.delete(transaction)
                deleted_po_transactions += 1

            except Exception as e:
                error_messages.append(f"Error deleting PO transaction {transaction_id}: {str(e)}")
                continue

        db_session.commit()

        if deleted_direct_purchases > 0 or deleted_po_transactions > 0:
            message = f"Successfully deleted {deleted_direct_purchases} direct purchases and {deleted_po_transactions} PO transactions"
            if error_messages:
                message += f". {len(error_messages)} errors occurred"
            return jsonify({
                'success': True,
                'message': message,
                'deleted_direct_purchases': deleted_direct_purchases,
                'deleted_po_transactions': deleted_po_transactions,
                'error_count': len(error_messages)
            })
        else:
            return jsonify({
                'success': False,
                'message': "No transactions were deleted. Errors: " + "; ".join(error_messages[:5]),
                'error_count': len(error_messages)
            }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk_delete_purchase_transactions: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'Error deleting transactions: {str(e)}'
        }), 500
    finally:
        db_session.close()


@purchases_bp.route('/transactions/bulk_cancel', methods=['POST'])
@login_required
def bulk_cancel_purchase_transactions():
    db_session = Session()
    try:
        data = request.get_json()
        direct_purchase_ids = data.get('direct_purchase_ids', [])
        po_transaction_ids = data.get('po_transaction_ids', [])

        canceled_direct_purchases = 0
        canceled_po_transactions = 0
        error_messages = []

        # Cancel direct purchases
        for direct_purchase_id in direct_purchase_ids:
            try:
                direct_purchase = db_session.query(DirectPurchaseTransaction).filter_by(id=direct_purchase_id).first()
                if not direct_purchase:
                    error_messages.append(f"Direct purchase {direct_purchase_id} not found")
                    continue

                # Check if already cancelled
                if direct_purchase.status == 'cancelled':
                    error_messages.append(
                        f"Direct purchase {direct_purchase.direct_purchase_number} is already cancelled")
                    continue

                # Check if can be cancelled (not fully received/invoiced if applicable)
                if hasattr(direct_purchase, 'is_fully_received') and direct_purchase.is_fully_received:
                    error_messages.append(
                        f"Cannot cancel direct purchase {direct_purchase.direct_purchase_number} - already fully received")
                    continue

                if hasattr(direct_purchase, 'is_fully_invoiced') and direct_purchase.is_fully_invoiced:
                    error_messages.append(
                        f"Cannot cancel direct purchase {direct_purchase.direct_purchase_number} - already fully invoiced")
                    continue

                # Reverse inventory entries
                try:
                    inventory_entries = get_inventory_entries_for_direct_purchase(db_session, direct_purchase.id,
                                                                                  current_user.app_id)
                    for entry in inventory_entries:
                        reverse_purchase_inventory_entries(db_session, entry)
                except Exception as inv_error:
                    error_messages.append(
                        f"Failed to reverse inventory for direct purchase {direct_purchase.direct_purchase_number}: {str(inv_error)}")
                    continue

                # Reverse ledger postings
                try:
                    reverse_direct_purchase_posting(db_session, direct_purchase, current_user)
                except Exception as ledger_error:
                    error_messages.append(
                        f"Failed to reverse ledger entries for direct purchase {direct_purchase.direct_purchase_number}: {str(ledger_error)}")
                    continue

                # Update status to cancelled
                direct_purchase.status = 'cancelled'
                direct_purchase.cancelled_by = current_user.id
                direct_purchase.cancelled_at = datetime.utcnow()
                direct_purchase.cancellation_reason = 'Bulk cancellation'

                # Clear stock history cache
                try:
                    clear_stock_history_cache()
                    logger.info("Successfully cleared stock history cache after cancellation")
                except Exception as cache_error:
                    logger.error(f"Cache clearing failed during cancellation: {cache_error}")
                    # Continue anyway - cache clearing failure shouldn't break the operation

                canceled_direct_purchases += 1

            except Exception as e:
                error_messages.append(f"Error cancelling direct purchase {direct_purchase_id}: {str(e)}")
                continue

        # Cancel PO transactions
        for transaction_id in po_transaction_ids:
            try:
                transaction = db_session.query(PurchaseTransaction).filter_by(id=transaction_id).first()
                if not transaction:
                    error_messages.append(f"PO transaction {transaction_id} not found")
                    continue

                # Check if already cancelled
                if transaction.status == 'cancelled':
                    error_messages.append(f"PO transaction {transaction.purchase_order_number} is already cancelled")
                    continue

                # Check if can be cancelled (not fully received/invoiced)
                if hasattr(transaction, 'receiving_status') and transaction.receiving_status == 'fully_received':
                    error_messages.append(
                        f"Cannot cancel PO transaction {transaction.purchase_order_number} - already fully received")
                    continue

                if hasattr(transaction, 'invoicing_status') and transaction.invoicing_status == 'fully_invoiced':
                    error_messages.append(
                        f"Cannot cancel PO transaction {transaction.purchase_order_number} - already fully invoiced")
                    continue

                # Reverse ledger postings
                try:
                    reverse_po_transaction_posting(db_session, transaction, current_user)
                except Exception as reversal_error:
                    error_messages.append(
                        f"Failed to reverse PO transaction {transaction.purchase_order_number}: {str(reversal_error)}")
                    continue

                # Update status to cancelled
                transaction.status = 'cancelled'
                transaction.cancelled_by = current_user.id
                transaction.cancelled_at = datetime.utcnow()
                transaction.cancellation_reason = 'Bulk cancellation'

                canceled_po_transactions += 1

            except Exception as e:
                error_messages.append(f"Error cancelling PO transaction {transaction_id}: {str(e)}")
                continue

        db_session.commit()

        if canceled_direct_purchases > 0 or canceled_po_transactions > 0:
            message = f"Successfully cancelled {canceled_direct_purchases} direct purchases and {canceled_po_transactions} PO transactions"
            if error_messages:
                message += f". {len(error_messages)} errors occurred"

            # Log successful cancellation
            logger.info(f"Bulk cancellation completed by user {current_user.id}: {message}")

            return jsonify({
                'success': True,
                'message': message,
                'cancelled_direct_purchases': canceled_direct_purchases,
                'cancelled_po_transactions': canceled_po_transactions,
                'error_count': len(error_messages)
            })
        else:
            return jsonify({
                'success': False,
                'message': "No transactions were cancelled. Errors: " + "; ".join(error_messages[:5]),
                'error_count': len(error_messages)
            }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk_cancel_purchase_transactions: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'Error cancelling transactions: {str(e)}'
        }), 500
    finally:
        db_session.close()
