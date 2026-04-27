import traceback
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import BytesIO
import io
from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT, TA_LEFT, TA_CENTER
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from sqlalchemy import func, or_, case, and_, literal, desc, asc
from sqlalchemy.orm import joinedload

from io import BytesIO

import pandas as pd
from flask import request, jsonify, render_template, flash, redirect, url_for, abort, send_file
from flask_login import login_required, current_user
from sqlalchemy import or_, func, literal, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from ai import get_base_currency
from db import Session
from models import Company, Module, InventoryLocation, ChartOfAccounts, Vendor, DirectSalesTransaction, \
    InventoryItemVariationLink, DirectSaleItem, Project, PaymentMode, UnitOfMeasurement, Currency, PaymentAllocation, \
    SalesTransaction, SalesPaymentStatus, SalesInvoice, InvoiceStatus, OrderStatus, CustomerCredit, \
    DirectPurchaseTransaction, InventoryItem, DirectPurchaseItem, PurchasePaymentAllocation, GoodsReceipt, \
    GoodsReceiptItem, PurchaseReturn, PurchaseOrder, InventoryEntry, PurchaseOrderItem, InventoryCategory, \
    InventoryItemVariation, ExchangeRate, SalesInvoiceItem, InventoryEntryLineItem
from services.inventory_helpers import safe_clear_stock_history_cache, get_user_accessible_locations
from services.post_to_ledger import post_direct_sale_cogs_to_ledger, post_sales_transaction_to_ledger, \
    bulk_post_sales_transactions, post_customer_credit_to_ledger, \
    post_purchase_transaction_to_ledger, post_goods_receipt_to_ledger, bulk_post_goods_receipts
from services.post_to_ledger_reversal import reverse_direct_sales_posting, reverse_sales_invoice_posting, \
    reverse_sales_transaction_posting, delete_journal_entries_by_source
from services.purchases_helpers import generate_direct_purchase_number, allocate_direct_purchase_payment, \
    get_inventory_entries_for_direct_purchase, reverse_purchase_inventory_entries, reverse_direct_purchase_posting, \
    calculate_direct_purchase_landed_costs, calculate_goods_receipt_landed_costs
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    get_inventory_entries_for_direct_sale, reverse_sales_inventory_entries, allocate_payment
from services.vendors_and_customers import get_or_create_customer_credit_account
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction, \
    generate_next_goods_receipt_number, get_total_received_for_purchase, get_item_details
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj
from utils_and_helpers.lists import check_list_not_empty
from . import sales_bp

import logging

logger = logging.getLogger(__name__)


@sales_bp.route('/reports/sales_by_item', methods=['GET'])
@login_required
def sales_by_item_page():
    """Main page for Sales by Item report"""
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Get company and base currency
        company = db_session.query(Company).filter_by(id=app_id).first()

        # Get filter dropdown data - customers
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()

        # Get all inventory items with variations
        items = db_session.query(InventoryItemVariationLink).filter_by(
            app_id=app_id,
            status="active"
        ).all()

        # Get categories
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()

        # Get currencies
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id if base_currency else None

        # Get locations and projects
        locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
        projects = db_session.query(Project).filter_by(app_id=app_id, is_active=True).all()

        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        return render_template(
            '/sales/sales_by_item.html',
            customers=customers,
            items=items,
            categories=categories,
            currencies=currencies,
            base_currency=base_currency,
            base_currency_code=base_currency_code,
            base_currency_id=base_currency_id,
            company=company,
            locations=locations,
            projects=projects,
            role=role,
            module_name="Sales",
            modules=modules_data
        )

    except Exception as e:
        logger.error(f"Error loading sales by item page: {e}\n{traceback.format_exc()}")
        flash('Error loading report page', 'error')
        return render_template('error.html', message=str(e)), 500
    finally:
        db_session.close()


@sales_bp.route('/reports/sales_by_item/summary', methods=['GET'])
@login_required
def sales_by_item_summary():
    db_session = Session()
    try:
        # Get filter parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        customer_id = request.args.get('customer_id')
        item_id = request.args.get('item_id')
        category_id = request.args.get('category_id')
        transaction_type = request.args.get('transaction_type')
        currency_id = request.args.get('currency_id')
        location_id = request.args.get('location_id')
        project_id = request.args.get('project_id')
        sort_field = request.args.get('sort_field', 'total_revenue')
        sort_order = request.args.get('sort_order', 'desc')
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)

        app_id = current_user.app_id
        user_id = current_user.id

        # ===== GET USER'S ACCESSIBLE LOCATIONS =====
        user_locations = get_user_accessible_locations(user_id, app_id)
        user_location_ids = [loc.id for loc in user_locations] if user_locations else []

        # Get base currency
        base_currency = db_session.query(Currency).filter_by(app_id=app_id, currency_index=1).first()
        if not base_currency:
            return jsonify({'success': False, 'message': 'Base currency not configured'}), 400

        base_currency_id = base_currency.id
        base_currency_code = base_currency.user_currency

        # If user has no location access, return empty
        if not user_location_ids:
            return jsonify({
                'success': True,
                'data': [],
                'pagination': {
                    'page': 1,
                    'per_page': per_page,
                    'total_pages': 0,
                    'total_items': 0,
                    'has_next': False,
                    'has_prev': False
                },
                'totals': {
                    'total_quantity': 0,
                    'total_revenue': 0,
                    'currency': base_currency_code
                }
            }), 200

        # If user selects a specific location, verify they have access
        # If user selects a specific location, verify they have access - FIX TYPE CONVERSION
        if location_id and location_id != 'None' and location_id != '':
            try:
                location_id_int = int(location_id)
                if location_id_int not in user_location_ids:
                    return jsonify({'success': False, 'message': 'You do not have permission to access this location'}), 403
            except ValueError:
                pass

        # USE HELPER FUNCTION with user_location_ids
        all_items = _get_sales_by_item_summary_data(
            db_session, app_id, base_currency_id, base_currency_code,
            start_date, end_date, customer_id, item_id, category_id,
            transaction_type, currency_id, project_id, location_id,
            sort_field, sort_order, user_location_ids  # ADD THIS PARAMETER
        )

        # Apply pagination
        total_items = len(all_items)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_items = all_items[start_idx:end_idx]

        # Calculate totals
        total_qty = sum(item['total_quantity'] for item in all_items)
        total_revenue = sum(item['total_revenue'] for item in all_items)

        pagination_data = {
            'page': page,
            'per_page': per_page,
            'total_pages': (total_items + per_page - 1) // per_page if total_items > 0 else 1,
            'total_items': total_items,
            'has_next': page < ((total_items + per_page - 1) // per_page) if total_items > 0 else False,
            'has_prev': page > 1
        }

        return jsonify({
            'success': True,
            'data': paginated_items,
            'pagination': pagination_data,
            'totals': {
                'total_quantity': round(total_qty, 2),
                'total_revenue': round(total_revenue, 2),
                'currency': base_currency_code
            }
        }), 200

    except Exception as e:
        logger.error(f"Error in sales_by_item_summary: {e}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


@sales_bp.route('/reports/sales_by_item/detail', methods=['GET'])
@login_required
def sales_by_item_detail():
    db_session = Session()
    try:
        # Get filter parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        customer_id = request.args.get('customer_id')
        item_id = request.args.get('item_id')
        category_id = request.args.get('category_id')
        transaction_type = request.args.get('transaction_type')
        currency_id = request.args.get('currency_id')
        location_id = request.args.get('location_id')
        project_id = request.args.get('project_id')
        sort_field = request.args.get('sort_field', 'transaction_date')
        sort_order = request.args.get('sort_order', 'desc')
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)

        app_id = current_user.app_id
        user_id = current_user.id

        # ===== GET USER'S ACCESSIBLE LOCATIONS =====
        user_locations = get_user_accessible_locations(user_id, app_id)
        user_location_ids = [loc.id for loc in user_locations] if user_locations else []

        # Get base currency
        base_currency = db_session.query(Currency).filter_by(app_id=app_id, currency_index=1).first()
        if not base_currency:
            return jsonify({'success': False, 'message': 'Base currency not configured'}), 400

        base_currency_id = base_currency.id
        base_currency_code = base_currency.user_currency

        # If user has no location access, return empty
        if not user_location_ids:
            return jsonify({
                'success': True,
                'data': [],
                'pagination': {
                    'page': 1,
                    'per_page': per_page,
                    'total_pages': 0,
                    'total_items': 0,
                    'has_next': False,
                    'has_prev': False
                }
            }), 200

        # If user selects a specific location, verify they have access
        # If user selects a specific location, verify they have access - FIX TYPE CONVERSION
        if location_id and location_id != 'None' and location_id != '':
            try:
                location_id_int = int(location_id)
                if location_id_int not in user_location_ids:
                    return jsonify({'success': False, 'message': 'You do not have permission to access this location'}), 403
            except ValueError:
                pass

        # USE HELPER FUNCTION with user_location_ids
        all_transactions = _get_sales_by_item_detail_data(
            db_session, app_id, base_currency_id, base_currency_code,
            start_date, end_date, customer_id, item_id, category_id,
            transaction_type, currency_id, project_id, location_id,
            sort_field, sort_order, user_location_ids  # ADD THIS PARAMETER
        )

        # Apply pagination
        total_items = len(all_transactions)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_transactions = all_transactions[start_idx:end_idx]

        pagination_data = {
            'page': page,
            'per_page': per_page,
            'total_pages': (total_items + per_page - 1) // per_page if total_items > 0 else 1,
            'total_items': total_items,
            'has_next': page < ((total_items + per_page - 1) // per_page) if total_items > 0 else False,
            'has_prev': page > 1
        }

        return jsonify({
            'success': True,
            'data': paginated_transactions,
            'pagination': pagination_data
        }), 200

    except Exception as e:
        logger.error(f"Error in sales_by_item_detail: {e}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


def _get_sales_by_item_summary_data(db_session, app_id, base_currency_id, base_currency_code,
                                    start_date, end_date, customer_id, item_id, category_id,
                                    transaction_type, currency_id, project_id, location_id,
                                    sort_field='total_revenue', sort_order='desc', user_location_ids=None):
    """Helper to get summary data for sales by item from Inventory Entry only"""
    try:
        logger.info(f"Sales Summary - Start: {start_date}, End: {end_date}")

        # If user_location_ids is None or empty, return empty
        if not user_location_ids:
            return []

        # Build base query for Inventory Entry (stock out for sales)
        query = db_session.query(
            InventoryItemVariationLink.id.label('variation_link_id'),
            InventoryItem.id.label('item_id'),
            InventoryItem.item_name.label('base_item_name'),
            InventoryItemVariation.variation_name.label('variation_name'),
            InventoryItem.item_code.label('item_code'),
            InventoryCategory.category_name.label('category_name'),
            UnitOfMeasurement.abbreviation.label('uom'),
            InventoryEntryLineItem.quantity.label('quantity'),
            (InventoryEntryLineItem.quantity * InventoryEntryLineItem.selling_price).label('total_price_original'),
            InventoryEntryLineItem.selling_price.label('unit_price'),
            InventoryEntry.transaction_date.label('sale_date'),
            InventoryEntry.currency_id.label('currency_id'),
            InventoryEntry.supplier_id.label('customer_id'),
            Vendor.vendor_name.label('customer_name'),
            ExchangeRate.rate.label('exchange_rate'),
            InventoryLocation.location.label('location_name')
        ).join(
            InventoryEntryLineItem, InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
        ).join(
            InventoryItemVariationLink, InventoryItemVariationLink.id == InventoryEntryLineItem.item_id
        ).join(
            InventoryItem, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).join(
            UnitOfMeasurement, UnitOfMeasurement.id == InventoryItem.uom_id
        ).outerjoin(
            Vendor, Vendor.id == InventoryEntry.supplier_id
        ).outerjoin(
            InventoryLocation, InventoryLocation.id == InventoryEntry.from_location
        ).outerjoin(
            Project, Project.id == InventoryEntry.project_id
        ).outerjoin(
            InventoryItemVariation, InventoryItemVariation.id == InventoryItemVariationLink.inventory_item_variation_id
        ).outerjoin(
            InventoryCategory, InventoryCategory.id == InventoryItem.item_category_id
        ).outerjoin(
            ExchangeRate, ExchangeRate.id == InventoryEntry.exchange_rate_id
        ).filter(
            InventoryEntry.app_id == app_id,
            InventoryEntry.stock_movement == 'out',
            InventoryEntry.source_id.is_(None),
            InventoryEntry.inventory_source.in_(['sale', 'adjustment_out', 'write_off']),
            InventoryLocation.id.in_(user_location_ids)  # Location permission filter
        )

        # Apply date filters
        if start_date:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            query = query.filter(InventoryEntry.transaction_date >= start_date_obj)

        if end_date:
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            query = query.filter(InventoryEntry.transaction_date <= end_date_obj)

        # Apply customer filter
        if customer_id and customer_id != 'None' and customer_id != '':
            query = query.filter(InventoryEntry.supplier_id == customer_id)

        # Apply item filter
        if item_id and item_id != 'None' and item_id != '':
            query = query.filter(InventoryItemVariationLink.id == item_id)

        # Apply category filter
        if category_id and category_id != 'None' and category_id != '':
            query = query.filter(InventoryItem.item_category_id == category_id)

        # Apply currency filter
        if currency_id and currency_id != 'None' and currency_id != '':
            query = query.filter(InventoryEntry.currency_id == currency_id)

        # Apply location filter
        if location_id and location_id != 'None' and location_id != '':
            query = query.filter(InventoryEntry.from_location == location_id)

        # Apply project filter
        if project_id and project_id != 'None' and project_id != '':
            query = query.filter(InventoryEntry.project_id == project_id)

        # Get all results
        results = query.all()

        logger.info(f"Inventory sales results: {len(results)}")

        if not results:
            return []

        # Process results
        all_sales = []

        def convert_to_base(amount, currency_id_val, exchange_rate):
            if not amount:
                return 0.0
            if currency_id_val == base_currency_id:
                return float(amount)
            if exchange_rate:
                return float(amount) * float(exchange_rate)
            return float(amount)

        for item in results:
            converted_revenue = convert_to_base(item.total_price_original, item.currency_id, item.exchange_rate)
            all_sales.append({
                'variation_link_id': item.variation_link_id,
                'item_id': item.item_id,
                'item_name': item.base_item_name,
                'variation_name': item.variation_name or '',
                'item_code': item.item_code,
                'category_name': item.category_name,
                'uom': item.uom or 'pcs',
                'quantity': float(item.quantity),
                'total_revenue_base': converted_revenue,
                'sale_date': item.sale_date
            })

        # Group by variation_link_id
        grouped_data = {}
        for sale in all_sales:
            key = sale['variation_link_id']
            if key not in grouped_data:
                grouped_data[key] = {
                    'variation_link_id': key,
                    'item_id': sale['item_id'],
                    'item_name': sale['item_name'],
                    'variation_name': sale['variation_name'],
                    'item_code': sale['item_code'],
                    'category_name': sale['category_name'],
                    'uom': sale['uom'],
                    'total_quantity': 0,
                    'total_revenue': 0,
                    'last_sale_date': None
                }

            grouped_data[key]['total_quantity'] += sale['quantity']
            grouped_data[key]['total_revenue'] += sale['total_revenue_base']

            if sale['sale_date']:
                if not grouped_data[key]['last_sale_date'] or sale['sale_date'] > grouped_data[key]['last_sale_date']:
                    grouped_data[key]['last_sale_date'] = sale['sale_date']

        # Convert to list
        items_list = []
        for key, data in grouped_data.items():
            avg_price = data['total_revenue'] / data['total_quantity'] if data['total_quantity'] > 0 else 0

            if data['variation_name']:
                item_name = f"{data['item_name']} ({data['variation_name']})"
            else:
                item_name = data['item_name']

            items_list.append({
                'item_name': item_name,
                'item_code': data['item_code'] or '-',
                'category_name': data['category_name'] or '-',
                'uom': data['uom'] or 'pcs',
                'total_quantity': round(data['total_quantity'], 2),
                'total_revenue': round(data['total_revenue'], 2),
                'average_price': round(avg_price, 2),
                'last_sale_date': data['last_sale_date'].isoformat() if data['last_sale_date'] else None
            })

        # Sort
        sort_field_mapping = {
            'total_revenue': 'total_revenue',
            'total_quantity': 'total_quantity',
            'item_name': 'item_name',
            'last_sale_date': 'last_sale_date'
        }
        sort_by = sort_field_mapping.get(sort_field, 'total_revenue')
        reverse_order = sort_order == 'desc'
        items_list.sort(key=lambda x: x[sort_by], reverse=reverse_order)

        return items_list

    except Exception as e:
        logger.error(f"Error in _get_sales_by_item_summary_data: {e}\n{traceback.format_exc()}")
        return []


def _get_sales_by_item_detail_data(db_session, app_id, base_currency_id, base_currency_code,
                                   start_date, end_date, customer_id, item_id, category_id,
                                   transaction_type, currency_id, project_id, location_id,
                                   sort_field='transaction_date', sort_order='desc', user_location_ids=None):
    """Helper to get detail data for sales by item from Inventory Entry only (no pagination)"""
    try:
        # If user_location_ids is None or empty, return empty
        if not user_location_ids:
            return []

        all_transactions = []

        def convert_to_base(amount, currency_id_val, exchange_rate):
            if not amount:
                return 0.0
            if currency_id_val == base_currency_id:
                return float(amount)
            if exchange_rate:
                return float(amount) * float(exchange_rate)
            return float(amount)

        # Parse date objects
        start_date_obj = None
        end_date_obj = None
        if start_date:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()

        # ========== GET INVENTORY ENTRY ITEMS (SALES) ONLY ==========
        inventory_sale_query = db_session.query(
            InventoryEntry.transaction_date.label('transaction_date'),
            literal('Inventory Entry').label('transaction_type'),
            InventoryEntry.reference.label('transaction_number'),
            Vendor.vendor_name.label('customer_name'),
            InventoryItemVariationLink.id.label('variation_link_id'),
            InventoryItem.item_name.label('base_item_name'),
            InventoryItemVariation.variation_name.label('variation_name'),
            InventoryItem.item_code.label('item_code'),
            InventoryCategory.category_name.label('category_name'),
            UnitOfMeasurement.abbreviation.label('uom'),
            InventoryEntryLineItem.quantity.label('quantity'),
            InventoryEntryLineItem.selling_price.label('unit_price'),
            (InventoryEntryLineItem.quantity * InventoryEntryLineItem.selling_price).label('total_revenue_original'),
            InventoryEntry.currency_id.label('currency_id'),
            InventoryEntry.id.label('transaction_id'),
            ExchangeRate.rate.label('exchange_rate'),
            InventoryLocation.location.label('location_name')
        ).join(
            InventoryEntryLineItem, InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
        ).join(
            InventoryItemVariationLink, InventoryItemVariationLink.id == InventoryEntryLineItem.item_id
        ).join(
            InventoryItem, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).join(
            UnitOfMeasurement, UnitOfMeasurement.id == InventoryItem.uom_id
        ).outerjoin(
            InventoryItemVariation, InventoryItemVariation.id == InventoryItemVariationLink.inventory_item_variation_id
        ).outerjoin(
            InventoryCategory, InventoryCategory.id == InventoryItem.item_category_id
        ).outerjoin(
            Vendor, Vendor.id == InventoryEntry.supplier_id
        ).outerjoin(
            InventoryLocation, InventoryLocation.id == InventoryEntry.from_location
        ).outerjoin(
            ExchangeRate, ExchangeRate.id == InventoryEntry.exchange_rate_id
        ).filter(
            InventoryEntry.app_id == app_id,
            InventoryEntry.stock_movement == 'out',
            InventoryEntry.source_id.is_(None),
            InventoryEntry.inventory_source.in_(['sale', 'adjustment_out', 'write_off']),
            InventoryLocation.id.in_(user_location_ids)  # Location permission filter
        )

        # Apply date filters
        if start_date_obj:
            inventory_sale_query = inventory_sale_query.filter(InventoryEntry.transaction_date >= start_date_obj)
        if end_date_obj:
            inventory_sale_query = inventory_sale_query.filter(InventoryEntry.transaction_date <= end_date_obj)

        # Apply customer filter
        if customer_id and customer_id != 'None' and customer_id != '':
            inventory_sale_query = inventory_sale_query.filter(InventoryEntry.supplier_id == customer_id)

        # Apply item filter
        if item_id and item_id != 'None' and item_id != '':
            inventory_sale_query = inventory_sale_query.filter(InventoryItemVariationLink.id == item_id)

        # Apply category filter
        if category_id and category_id != 'None' and category_id != '':
            inventory_sale_query = inventory_sale_query.filter(InventoryItem.item_category_id == category_id)

        # Apply currency filter
        if currency_id and currency_id != 'None' and currency_id != '':
            inventory_sale_query = inventory_sale_query.filter(InventoryEntry.currency_id == currency_id)

        # Apply location filter
        if location_id and location_id != 'None' and location_id != '':
            inventory_sale_query = inventory_sale_query.filter(InventoryEntry.from_location == location_id)

        # Apply project filter
        if project_id and project_id != 'None' and project_id != '':
            inventory_sale_query = inventory_sale_query.filter(InventoryEntry.project_id == project_id)

        inventory_results = inventory_sale_query.all()
        logger.info(f"Inventory sales results: {len(inventory_results)}")

        if not inventory_results:
            return []

        for item in inventory_results:
            converted_unit_price = convert_to_base(item.unit_price, item.currency_id, item.exchange_rate)
            converted_revenue = convert_to_base(item.total_revenue_original, item.currency_id, item.exchange_rate)

            all_transactions.append({
                'transaction_date': item.transaction_date,
                'transaction_type': item.transaction_type,
                'transaction_number': item.transaction_number,
                'transaction_id': item.transaction_id,
                'customer_name': item.customer_name or '-',
                'base_item_name': item.base_item_name,
                'variation_name': item.variation_name or '',
                'item_code': item.item_code,
                'category_name': item.category_name,
                'uom': item.uom or 'pcs',
                'quantity': float(item.quantity),
                'unit_price': converted_unit_price,
                'total_revenue': converted_revenue,
                'currency_id': item.currency_id,
                'exchange_rate': item.exchange_rate
            })

        if not all_transactions:
            return []

        # ========== SORT ==========
        sort_reverse = sort_order == 'desc'

        if sort_field == 'transaction_date':
            all_transactions.sort(key=lambda x: (x['transaction_date'] or date.min, x['transaction_id']),
                                  reverse=sort_reverse)
        elif sort_field == 'transaction_type':
            all_transactions.sort(key=lambda x: (x['transaction_type'], x['transaction_id']), reverse=sort_reverse)
        elif sort_field == 'transaction_number':
            all_transactions.sort(key=lambda x: (x['transaction_number'] or '', x['transaction_id']),
                                  reverse=sort_reverse)
        elif sort_field == 'customer_name':
            all_transactions.sort(key=lambda x: (x['customer_name'] or '', x['transaction_id']), reverse=sort_reverse)
        elif sort_field == 'item_name':
            all_transactions.sort(key=lambda x: (x['base_item_name'] or '', x['transaction_id']), reverse=sort_reverse)
        elif sort_field == 'quantity':
            all_transactions.sort(key=lambda x: (x['quantity'], x['transaction_id']), reverse=sort_reverse)
        elif sort_field == 'unit_price':
            all_transactions.sort(key=lambda x: (x['unit_price'], x['transaction_id']), reverse=sort_reverse)
        elif sort_field == 'total_revenue':
            all_transactions.sort(key=lambda x: (x['total_revenue'], x['transaction_id']), reverse=sort_reverse)
        else:
            all_transactions.sort(key=lambda x: (x['transaction_date'] or date.min, x['transaction_id']),
                                  reverse=sort_reverse)

        # ========== FORMAT RESULTS FOR FRONTEND ==========
        transactions = []
        for tx in all_transactions:
            if tx.get('variation_name'):
                item_name = f"{tx['base_item_name']} ({tx['variation_name']})"
            else:
                item_name = tx['base_item_name']

            # URL for inventory entry
            transaction_url = f"/inventory/entry/{tx['transaction_id']}"

            transactions.append({
                'transaction_date': tx['transaction_date'].isoformat() if tx['transaction_date'] else None,
                'transaction_type': tx['transaction_type'],
                'transaction_number': tx['transaction_number'],
                'transaction_id': tx['transaction_id'],
                'customer_name': tx['customer_name'],
                'item_name': item_name,
                'item_code': tx.get('item_code', '-'),
                'category_name': tx.get('category_name', '-'),
                'uom': tx.get('uom', 'pcs'),
                'quantity': round(tx['quantity'], 2),
                'unit_price': round(tx['unit_price'], 2),
                'total_revenue': round(tx['total_revenue'], 2)
            })

        return transactions

    except Exception as e:
        logger.error(f"Error in _get_sales_by_item_detail_data: {e}\n{traceback.format_exc()}")
        return []


@sales_bp.route('/reports/sales_by_item/export/pdf', methods=['GET'])
@login_required
def export_sales_by_item_pdf():
    """Export Sales by Item report to PDF"""
    db_session = Session()
    try:
        # Get filter parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        customer_id = request.args.get('customer_id')
        item_id = request.args.get('item_id')
        category_id = request.args.get('category_id')
        transaction_type = request.args.get('transaction_type')
        currency_id = request.args.get('currency_id')
        location_id = request.args.get('location_id')
        project_id = request.args.get('project_id')
        sort_field = request.args.get('sort_field', 'total_revenue')
        sort_order = request.args.get('sort_order', 'desc')
        view_type = request.args.get('view', 'summary')

        app_id = current_user.app_id
        user_id = current_user.id

        # ===== GET USER'S ACCESSIBLE LOCATIONS =====
        user_locations = get_user_accessible_locations(user_id, app_id)
        user_location_ids = [loc.id for loc in user_locations] if user_locations else []

        # Get base currency
        base_currency = db_session.query(Currency).filter_by(app_id=app_id, currency_index=1).first()
        if not base_currency:
            return jsonify({'success': False, 'message': 'Base currency not configured'}), 400

        base_currency_id = base_currency.id
        base_currency_code = base_currency.user_currency

        # Get company name
        company = db_session.query(Company).filter_by(id=app_id).first()
        company_name = company.name if company else "Company"

        # If user has no location access, return empty report
        if not user_location_ids:
            data = []
        else:
            # Get data based on view type with location permissions
            if view_type == 'summary':
                data = _get_sales_by_item_summary_data(
                    db_session, app_id, base_currency_id, base_currency_code,
                    start_date, end_date, customer_id, item_id, category_id,
                    transaction_type, currency_id, project_id, location_id,
                    sort_field, sort_order, user_location_ids
                )
            else:
                data = _get_sales_by_item_detail_data(
                    db_session, app_id, base_currency_id, base_currency_code,
                    start_date, end_date, customer_id, item_id, category_id,
                    transaction_type, currency_id, project_id, location_id,
                    sort_field, sort_order, user_location_ids
                )

        if not data:
            data = []

        # Prepare PDF with LANDSCAPE orientation
        from reportlab.lib.pagesizes import letter, landscape
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=landscape(letter),
                                rightMargin=36, leftMargin=36,
                                topMargin=36, bottomMargin=36)

        # Register fonts
        try:
            pdfmetrics.registerFont(TTFont('Roboto', 'Roboto-Regular.ttf'))
            pdfmetrics.registerFont(TTFont('Roboto-Bold', 'Roboto-Bold.ttf'))
            font_name_regular = 'Roboto'
            font_name_bold = 'Roboto-Bold'
        except:
            font_name_regular = 'Helvetica'
            font_name_bold = 'Helvetica-Bold'

        # Define styles
        styles = getSampleStyleSheet()
        styles['Normal'].fontName = font_name_regular
        styles['Normal'].fontSize = 9
        styles['Normal'].leading = 12
        styles.add(ParagraphStyle(
            name='ReportTitle',
            fontName=font_name_bold,
            fontSize=16,
            alignment=TA_CENTER
        ))
        styles.add(ParagraphStyle(
            name='SubHeader',
            fontName=font_name_bold,
            fontSize=10,
            spaceAfter=6
        ))
        styles.add(ParagraphStyle(
            name='SmallText',
            fontName=font_name_regular,
            fontSize=7,
            textColor=colors.gray,
            leading=9
        ))
        styles.add(ParagraphStyle(
            name='TableCell',
            fontName=font_name_regular,
            fontSize=7,
            leading=9
        ))
        styles.add(ParagraphStyle(
            name='AmountCell',
            fontName=font_name_regular,
            fontSize=7,
            alignment=TA_RIGHT,
            leading=9
        ))

        elements = []

        # Report Header
        elements.append(Paragraph(company_name, styles['ReportTitle']))
        elements.append(Spacer(1, 4))
        elements.append(Paragraph("Sales by Item Report", styles['SubHeader']))

        # Date range
        report_period = ""
        if start_date and end_date:
            report_period = f"{start_date} to {end_date}"
        elif start_date:
            report_period = f"From {start_date}"
        elif end_date:
            report_period = f"Until {end_date}"
        else:
            report_period = "All Time"

        elements.append(Paragraph(f"Period: {report_period}", styles['Normal']))

        # Filters applied
        filters_text = []
        if customer_id and customer_id != 'None' and customer_id != '':
            customer = db_session.query(Vendor).filter_by(id=customer_id).first()
            if customer:
                filters_text.append(f"Customer: {customer.vendor_name}")
        if category_id and category_id != 'None' and category_id != '':
            category = db_session.query(InventoryCategory).filter_by(id=category_id).first()
            if category:
                filters_text.append(f"Category: {category.category_name}")
        if transaction_type and transaction_type != 'None' and transaction_type != '':
            type_display = {
                'invoice': 'Sales Invoice',
                'direct_sale': 'Direct Sale',
                'pos': 'POS Sale',
                'inventory_entry': 'Inventory Entry'
            }.get(transaction_type, transaction_type.replace('_', ' ').title())
            filters_text.append(f"Type: {type_display}")
        if location_id and location_id != 'None' and location_id != '':
            location = db_session.query(InventoryLocation).filter_by(id=location_id).first()
            if location:
                filters_text.append(f"Location: {location.location_name}")
        if project_id and project_id != 'None' and project_id != '':
            project = db_session.query(Project).filter_by(id=project_id).first()
            if project:
                filters_text.append(f"Project: {project.name}")

        if filters_text:
            elements.append(Paragraph(f"Filters: {', '.join(filters_text)}", styles['SmallText']))

        elements.append(Paragraph(f"Currency: {base_currency_code}", styles['SmallText']))
        elements.append(
            Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", styles['SmallText']))
        elements.append(Spacer(1, 12))

        # Data Table
        if view_type == 'summary':
            # Summary table headers
            table_data = [["Item Name", "Item Code", "Category", "UOM", "Quantity Sold", "Total Revenue", "Avg Price",
                           "Last Sale Date"]]

            for item in data:
                table_data.append([
                    Paragraph(str(item.get('item_name', '-')), styles['TableCell']),
                    Paragraph(str(item.get('item_code', '-')), styles['TableCell']),
                    Paragraph(str(item.get('category_name', '-')), styles['TableCell']),
                    Paragraph(str(item.get('uom', 'pcs')), styles['TableCell']),
                    Paragraph(f"{item.get('total_quantity', 0):,.2f}", styles['AmountCell']),
                    Paragraph(f"{item.get('total_revenue', 0):,.2f}", styles['AmountCell']),
                    Paragraph(f"{item.get('average_price', 0):,.2f}", styles['AmountCell']),
                    Paragraph(str(item.get('last_sale_date', '-')), styles['TableCell'])
                ])

            # Add totals row
            total_qty = sum(item.get('total_quantity', 0) for item in data)
            total_revenue = sum(item.get('total_revenue', 0) for item in data)

            table_data.append([
                "", "", "", "TOTALS:",
                Paragraph(f"{total_qty:,.2f}", styles['AmountCell']),
                Paragraph(f"{total_revenue:,.2f}", styles['AmountCell']),
                "", ""
            ])

            col_widths = [150, 70, 80, 50, 70, 100, 90, 90]

        else:  # detail view
            # Detail table headers
            table_data = [
                ["Date", "Type", "Transaction #", "Customer", "Item Name", "Item Code", "Category", "UOM", "Quantity",
                 "Unit Price", "Total Revenue"]]

            for tx in data:
                # Format date safely
                date_str = tx.get('transaction_date', '-')
                if date_str and date_str != '-':
                    date_str = date_str[:10] if len(date_str) > 10 else date_str

                table_data.append([
                    Paragraph(str(date_str), styles['TableCell']),
                    Paragraph(str(tx.get('transaction_type', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('transaction_number', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('customer_name', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('item_name', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('item_code', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('category_name', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('uom', 'pcs')), styles['TableCell']),
                    Paragraph(f"{tx.get('quantity', 0):,.2f}", styles['AmountCell']),
                    Paragraph(f"{tx.get('unit_price', 0):,.2f}", styles['AmountCell']),
                    Paragraph(f"{tx.get('total_revenue', 0):,.2f}", styles['AmountCell'])
                ])

            # Add totals row
            total_qty = sum(tx.get('quantity', 0) for tx in data)
            total_revenue = sum(tx.get('total_revenue', 0) for tx in data)

            table_data.append([
                "", "", "", "", "", "", "TOTALS:", "",
                Paragraph(f"{total_qty:,.2f}", styles['AmountCell']),
                "",
                Paragraph(f"{total_revenue:,.2f}", styles['AmountCell'])
            ])

            col_widths = [50, 60, 70, 80, 80, 50, 60, 40, 50, 80, 100]

        # Create table with REPEAT HEADER ON EVERY PAGE
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), font_name_bold),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#343a40')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),

            ('FONTNAME', (0, 1), (-1, -2), font_name_regular),
            ('FONTSIZE', (0, 1), (-1, -2), 7),
            ('GRID', (0, 0), (-1, -2), 0.5, colors.HexColor('#dee2e6')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),

            ('ALIGN', (4, 1), (5, -2), 'RIGHT') if view_type == 'summary' else ('ALIGN', (8, 1), (10, -2), 'RIGHT'),

            ('FONTNAME', (0, -1), (-1, -1), font_name_bold),
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e9ecef')),
            ('LINEABOVE', (0, -1), (-1, -1), 1, colors.black),

            ('ROWBACKGROUNDS', (0, 1), (-1, -2), [colors.white, colors.HexColor('#f8f9fa')]),
        ]))

        elements.append(table)

        # Build PDF
        doc.build(elements)
        buffer.seek(0)

        filename = f"Sales_by_Item_{view_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        return send_file(buffer, as_attachment=True, download_name=filename, mimetype='application/pdf')

    except Exception as e:
        logger.error(f"Error exporting sales by item PDF: {e}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


@sales_bp.route('/reports/sales_by_item/export/excel', methods=['GET'])
@login_required
def export_sales_by_item_excel():
    """Export Sales by Item report to Excel"""
    db_session = Session()
    try:
        # Get filter parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        customer_id = request.args.get('customer_id')
        item_id = request.args.get('item_id')
        category_id = request.args.get('category_id')
        transaction_type = request.args.get('transaction_type')
        currency_id = request.args.get('currency_id')
        location_id = request.args.get('location_id')
        project_id = request.args.get('project_id')
        sort_field = request.args.get('sort_field', 'total_revenue')
        sort_order = request.args.get('sort_order', 'desc')
        view_type = request.args.get('view', 'summary')

        app_id = current_user.app_id

        # Get base currency
        base_currency = db_session.query(Currency).filter_by(app_id=app_id, currency_index=1).first()
        base_currency_code = base_currency.user_currency if base_currency else "USD"
        base_currency_id = base_currency.id if base_currency else None

        # Get data based on view type
        if view_type == 'summary':
            data = _get_sales_by_item_summary_data(
                db_session, app_id, base_currency_id, base_currency_code,
                start_date, end_date, customer_id, item_id, category_id,
                transaction_type, currency_id, project_id, location_id,
                sort_field, sort_order
            )

            df = pd.DataFrame(data)
            if not df.empty:
                df = df.rename(columns={
                    'item_name': 'Item Name',
                    'item_code': 'Item Code',
                    'category_name': 'Category',
                    'uom': 'UOM',
                    'total_quantity': 'Quantity Sold',
                    'total_revenue': 'Total Revenue',
                    'average_price': 'Average Price',
                    'last_sale_date': 'Last Sale Date'
                })

                totals = {
                    'Item Name': 'TOTALS',
                    'Quantity Sold': df['Quantity Sold'].sum(),
                    'Total Revenue': df['Total Revenue'].sum()
                }
                df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)

        else:  # detail view
            data = _get_sales_by_item_detail_data(
                db_session, app_id, base_currency_id, base_currency_code,
                start_date, end_date, customer_id, item_id, category_id,
                transaction_type, currency_id, project_id, location_id,
                sort_field, sort_order
            )

            df = pd.DataFrame(data)
            if not df.empty:
                df = df.rename(columns={
                    'transaction_date': 'Transaction Date',
                    'transaction_type': 'Transaction Type',
                    'transaction_number': 'Transaction #',
                    'customer_name': 'Customer',
                    'item_name': 'Item Name',
                    'item_code': 'Item Code',
                    'category_name': 'Category',
                    'uom': 'UOM',
                    'quantity': 'Quantity',
                    'unit_price': 'Unit Price',
                    'total_revenue': 'Total Revenue'
                })

                totals = {
                    'Transaction #': 'TOTALS',
                    'Quantity': df['Quantity'].sum(),
                    'Total Revenue': df['Total Revenue'].sum()
                }
                df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)

        # Create Excel file
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            sheet_name = 'Sales by Item - Summary' if view_type == 'summary' else 'Sales by Item - Detail'
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            worksheet = writer.sheets[sheet_name]
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        output.seek(0)

        filename = f"Sales_by_Item_{view_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(output, download_name=filename, as_attachment=True,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except Exception as e:
        logger.error(f"Error exporting sales by item Excel: {e}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()
