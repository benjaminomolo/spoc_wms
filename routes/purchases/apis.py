import logging
import traceback
from datetime import datetime
from decimal import Decimal, InvalidOperation

from flask import jsonify, request, flash, redirect, url_for
from flask_login import login_required, current_user
from sqlalchemy import func

from ai import get_base_currency
from db import Session
from models import DirectSalesTransaction, SalesInvoice, SalesTransaction, SalesPaymentStatus, InvoiceStatus, \
    OrderStatus, CustomerCredit, CreditApplication, Currency, Quotation, DirectPurchaseTransaction, PurchaseOrder, \
    GoodsReceipt
from services.post_to_ledger import post_sales_transaction_to_ledger, post_customer_credit_to_ledger, \
    post_credit_application_to_ledger
from services.purchases_helpers import suggest_next_direct_purchase_reference, check_prepaid_account_consistency, \
    suggest_next_purchase_order_reference
from services.sales_helpers import suggest_next_direct_sale_reference, suggest_next_invoice_reference, \
    allocate_direct_sale_payment, allocate_payment, suggest_next_quotation_reference, suggest_next_sales_order_reference
from services.vendors_and_customers import get_or_create_customer_credit_account
from utils_and_helpers.amounts_utils import format_currency
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj

from . import purchases_bp

logger = logging.getLogger(__name__)


@purchases_bp.route('/api/suggest_direct_purchase_reference')
@login_required
def suggest_direct_purchase_reference():
    """
    API endpoint to suggest the next direct sale reference based on the most recent reference.
    """
    with Session() as db_session:
        suggested_ref = suggest_next_direct_purchase_reference(db_session)

    return jsonify({'suggested_reference': suggested_ref})


@purchases_bp.route('/api/check_direct_purchase_reference')
@login_required
def check_direct_purchase_reference():
    """
    API endpoint to check if a direct sale reference already exists.
    """
    reference = request.args.get('reference', type=str)
    exists = False

    if reference:
        with Session() as db_session:
            exists = db_session.query(DirectPurchaseTransaction).filter(
                DirectPurchaseTransaction.app_id == current_user.app_id,
                func.lower(DirectPurchaseTransaction.purchase_reference) == reference.lower()
            ).count() > 0

    return jsonify({'exists': exists})


@purchases_bp.route('/api/suggest_purchase_order_reference')
@login_required
def suggest_purchase_order_reference():
    """
    API endpoint to suggest the next direct sale reference based on the most recent reference.
    """
    with Session() as db_session:
        suggested_ref = suggest_next_purchase_order_reference(db_session)

    return jsonify({'suggested_reference': suggested_ref})


@purchases_bp.route('/api/check_purchase_order_reference')
@login_required
def check_purchase_order_reference():
    """
    API endpoint to check if a direct sale reference already exists.
    """
    reference = request.args.get('reference', type=str)
    exists = False

    if reference:
        with Session() as db_session:
            exists = db_session.query(PurchaseOrder).filter(
                PurchaseOrder.app_id == current_user.app_id,
                func.lower(PurchaseOrder.purchase_order_reference) == reference.lower()
            ).count() > 0

    return jsonify({'exists': exists})


@purchases_bp.route('/api/check_prepaid_consistency', methods=['POST'])
def api_check_prepaid_consistency():
    """API endpoint for frontend to check prepaid account consistency"""
    try:
        data = request.get_json()
        purchase_order_id = data.get('purchase_order_id')
        new_prepaid_account_id = data.get('prepaid_account_id')

        db_session = Session()

        try:
            result = check_prepaid_account_consistency(
                db_session, purchase_order_id, new_prepaid_account_id
            )

            return jsonify(result)

        finally:
            db_session.close()

    except Exception as e:
        logger.error(f'An error occured {e}\n{traceback.format_exc()}')
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


# In your purchases blueprint (purchases_routes.py or similar)

from functools import lru_cache

# Add a simple cache for the redirect lookup (optional)
@lru_cache(maxsize=100)
@purchases_bp.route('/redirect_goods_receipt_to_po/<int:receipt_id>')
@login_required
def redirect_goods_receipt_to_po(receipt_id):
    """
    Redirect from a goods receipt to its associated purchase order.
    This allows users to view the purchase order when clicking on a goods receipt entry.
    """
    db_session = Session()
    try:
        # Query the goods receipt
        receipt = db_session.query(GoodsReceipt).filter_by(id=receipt_id).first()

        if not receipt:
            flash('Goods receipt not found.', 'error')
            return redirect(url_for('inventory.stock_movement_history'))

        # Check if receipt has a purchase order
        if not receipt.purchase_order_id:
            flash('This goods receipt is not associated with a purchase order.', 'error')
            return redirect(url_for('inventory.stock_movement_history'))

        # Get the purchase order to ensure it exists
        purchase_order = db_session.query(PurchaseOrder).filter_by(id=receipt.purchase_order_id).first()

        if not purchase_order:
            flash('Associated purchase order not found.', 'error')
            return redirect(url_for('inventory.stock_movement_history'))

        # Redirect to the purchase order details page
        return redirect(url_for('purchases.purchase_order_details',
                               purchase_order_id=receipt.purchase_order_id))

    except Exception as e:
        logger.error(f"Error redirecting goods receipt {receipt_id} to PO: {e}")
        flash('An error occurred while redirecting.', 'error')
        return redirect(url_for('inventory.stock_movement_history'))
    finally:
        db_session.close()
