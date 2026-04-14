# app/routes/inventory/inventory_reports.py
import traceback
from datetime import datetime, timedelta
from decimal import Decimal

from flask import Blueprint, jsonify, render_template, flash, redirect, url_for, request, current_app
from flask_login import login_required, current_user
from sqlalchemy import func, case, or_, and_
from sqlalchemy.exc import SQLAlchemyError

from configs import cache
from decorators import role_required, require_permission, cached_route
from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Project, ChartOfAccounts, PaymentMode, Category, Department, Employee, \
    PayrollPeriod, InventoryCategory, InventorySubCategory, InventoryLocation, InventoryItem, InventoryItemVariation, \
    InventoryItemAttribute, Brand, InventoryItemVariationLink, UnitOfMeasurement, Vendor, InventoryEntry, \
    InventoryEntryLineItem, InventoryTransactionDetail, InventorySummary
import logging

from utils import ensure_default_location, generate_unique_lot, create_notification, apply_date_filters
from utils_and_helpers.cache_keys import stock_list_list_cache_key, stock_list_grid_cache_key
from utils_and_helpers.cache_utils import clear_stock_list_grid_cache
from utils_and_helpers.file_utils import allowed_file, is_file_size_valid, file_exists, generate_unique_filename
from . import inventory_bp

logger = logging.getLogger(__name__)


@inventory_bp.route('/stock_list_list', methods=["GET"])
@login_required
def stock_list_list():
    """
    Render stock list page (GET).
    """
    db_session = Session()

    try:
        # Default parameters for initial page load
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        hide_zero_items = request.args.get('hide_zero_items', default=False, type=lambda x: x.lower() == 'true')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        category_id = request.args.get('category')
        subcategory_id = request.args.get('subcategory')
        brand_id = request.args.get('brand')
        attribute_id = request.args.get('attribute')
        variation_id = request.args.get('variation')
        location_id = request.args.get('location')
        status_filter = request.args.get('status')  # ✅ ADD THIS
        item_id = request.args.get('item')
        sort_by = request.args.get('sort_by', 'item_name')      # ADD THIS
        sort_order = request.args.get('sort_order', 'asc')      # ADD THIS



        # Ensure proper boolean value
        hide_zero_items = bool(hide_zero_items)

        filter_applied = bool(start_date or end_date or category_id or subcategory_id or
                              brand_id or attribute_id or variation_id or location_id or status_filter or item_id)

        # Use InventorySummary for initial load (fastest)
        stock_data, pagination_data = _get_stock_list_data(
            db_session, page, per_page, hide_zero_items, start_date, end_date,
            category_id, subcategory_id, brand_id, attribute_id, variation_id,
            location_id, status_filter, item_id, sort_by, sort_order
        )

        # Fetch filter dropdown data
        categories = db_session.query(InventoryCategory).filter_by(app_id=current_user.app_id).all()
        subcategories = db_session.query(InventorySubCategory).filter_by(app_id=current_user.app_id).all()
        brands = db_session.query(Brand).filter_by(app_id=current_user.app_id).all()
        attributes = db_session.query(InventoryItemAttribute).filter_by(app_id=current_user.app_id).all()
        variations = db_session.query(InventoryItemVariation).filter_by(app_id=current_user.app_id).all()
        locations = db_session.query(InventoryLocation).filter_by(app_id=current_user.app_id).all()
        inventory_items = db_session.query(InventoryItemVariationLink).filter_by(app_id=current_user.app_id, status="active").all()

        # Fetch company details
        company = db_session.query(Company).filter_by(id=current_user.app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=current_user.app_id, included='yes').all()]

        return render_template(
            '/inventory/stock_list_list.html',
            stock_by_category=stock_data or {},
            modules=modules_data or [],
            company=company,
            role=role or 'Viewer',
            filter_applied=filter_applied,
            module_name="Inventory",
            items=inventory_items,
            pagination=pagination_data or {
                'page': page,
                'per_page': per_page,
                'total_pages': 0,
                'total_items': 0,
                'has_next': False,
                'has_prev': False
            },
            hide_zero_items=hide_zero_items,
            filters={
                'start_date': start_date or '',
                'end_date': end_date or '',
                'category': category_id or '',
                'subcategory': subcategory_id or '',
                'brand': brand_id or '',
                'attribute': attribute_id or '',
                'variation': variation_id or '',
                'location': location_id or '',
                'status': status_filter or '',
                'item_id': item_id or '',
                'sort_by': sort_by,        # ADD THIS
                'sort_order': sort_order
            },
            filter_options={
                'categories': categories or [],
                'subcategories': subcategories or [],
                'brands': brands or [],
                'attributes': attributes or [],
                'variations': variations or [],
                'locations': locations or [],
                'items': inventory_items or []
            }
        )

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error in stock_list_list: {str(e)}")
        flash('An error occurred while retrieving stock list.', 'error')
        return render_template('error.html', message='Database error occurred'), 500

    except Exception as e:
        logger.error(f'Unexpected error in stock_list_list: {str(e)} \n{traceback.format_exc()}')
        flash('An unexpected error occurred.', 'error')
        return redirect(request.referrer)

    finally:
        db_session.close()


@inventory_bp.route('/stock_list_list/filter', methods=["POST"])
@login_required
# @cached_route(timeout=300, key_func=stock_list_list_cache_key)
def stock_list_filter():
    """
    Return filtered JSON data for AJAX requests (POST).
    """
    db_session = Session()

    try:
        # Get JSON data from POST request
        data = request.get_json() or {}
        page = int(data.get('page', 1))
        per_page = int(data.get('per_page', 20))
        hide_zero_items = data.get('hide_zero_items', False)
        start_date = data.get('start_date')
        end_date_str = data.get('end_date')
        category_id = data.get('category')
        subcategory_id = data.get('subcategory')
        brand_id = data.get('brand')
        attribute_id = data.get('attribute')
        variation_id = data.get('variation')
        location_id = data.get('location')
        status_filter = data.get('status')
        item_id = data.get('item')
        sort_by = data.get('sort_by', 'item_name')      # ADD THIS
        sort_order = data.get('sort_order', 'asc')

        # SMART SWITCHING: Use summary for current stock, transaction history for dates
        if start_date or end_date_str:
            # Use transaction-based calculation when dates are provided
            stock_data, pagination_data = _get_stock_list_data_with_dates(
                db_session, page, per_page, hide_zero_items, start_date, end_date_str,
                category_id, subcategory_id, brand_id, attribute_id, variation_id,
                location_id, status_filter, item_id, sort_by, sort_order
            )
        else:
            # Use InventorySummary for current stock (no dates)
            stock_data, pagination_data = _get_stock_list_data(
                db_session, page, per_page, hide_zero_items, start_date, end_date_str,
                category_id, subcategory_id, brand_id, attribute_id, variation_id,
                location_id, status_filter, item_id, sort_by, sort_order
            )

        return jsonify({
            'success': True,
            'stock_data': stock_data,
            'pagination': pagination_data,
            'filters': {
                'start_date': start_date,
                'end_date': end_date_str,
                'category': category_id,
                'subcategory': subcategory_id,
                'brand': brand_id,
                'attribute': attribute_id,
                'variation': variation_id,
                'location': location_id,
                'status': status_filter,
                'item': item_id
            }
        }), 200

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error in stock_list_filter: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred while filtering stock list.'}), 500

    except Exception as e:
        logger.error(f'Unexpected error in stock_list_filter: {str(e)}')
        return jsonify({'success': False, 'message': 'An unexpected error occurred.'}), 500

    finally:
        db_session.close()


def _get_stock_list_data(db_session, page, per_page, hide_zero_items=False, start_date=None,
                         end_date=None, category_id=None, subcategory_id=None, brand_id=None,
                         attribute_id=None, variation_id=None, location_id=None,
                         status_filter=None, item_id=None, sort_by='item_name', sort_order='asc'):  # 🔴 ADD item_id
    """
    Retrieve stock list data using InventorySummary for current quantities.
    """
    app_id = current_user.app_id

    # Build query
    query = db_session.query(
        InventoryItem.id,
        InventoryItem.item_name,
        InventoryItem.image_filename,
        InventoryCategory.category_name,
        InventoryCategory.id.label('category_id'),
        InventorySubCategory.subcategory_name,
        InventorySubCategory.id.label('subcategory_id'),
        Brand.name.label('brand'),
        Brand.id.label('brand_id'),
        InventoryItemAttribute.attribute_name.label('attribute'),
        InventoryItemAttribute.id.label('attribute_id'),
        InventoryItemVariation.variation_name.label('variation'),
        InventoryItemVariation.id.label('variation_id'),
        InventoryLocation.location.label('location'),
        InventoryLocation.id.label('location_id'),
        InventoryItem.reorder_point,
        InventorySummary.total_quantity.label('total_quantity'),
        UnitOfMeasurement.abbreviation.label('uom'),
        InventoryItemVariationLink.id.label('variation_link_id')  # Add this for item filtering
    ).select_from(InventorySummary).join(
        InventoryItemVariationLink, InventorySummary.item_id == InventoryItemVariationLink.id
    ).join(
        InventoryItem, InventoryItemVariationLink.inventory_item_id == InventoryItem.id
    ).join(
        InventoryCategory, InventoryItem.item_category_id == InventoryCategory.id
    ).join(
        InventorySubCategory, InventoryItem.item_subcategory_id == InventorySubCategory.id
    ).join(
        InventoryLocation, InventorySummary.location_id == InventoryLocation.id
    ).outerjoin(
        Brand, InventoryItem.brand_id == Brand.id
    ).outerjoin(
        InventoryItemAttribute, InventoryItemVariationLink.attribute_id == InventoryItemAttribute.id
    ).outerjoin(
        InventoryItemVariation, InventoryItemVariationLink.inventory_item_variation_id == InventoryItemVariation.id
    ).outerjoin(
        UnitOfMeasurement, InventoryItem.uom_id == UnitOfMeasurement.id
    ).filter(
        InventorySummary.app_id == app_id,
        InventoryItem.app_id == app_id,
        InventoryItem.status == 'active',
        InventoryCategory.app_id == app_id,
        InventorySubCategory.app_id == app_id,
        InventoryLocation.app_id == app_id
    )

    # 🔴 ADD item filter
    if item_id:
        query = query.filter(InventoryItemVariationLink.id == item_id)

    # Apply other filters (same as before)
    if category_id:
        query = query.filter(InventoryCategory.id == category_id)
    if subcategory_id:
        query = query.filter(InventorySubCategory.id == subcategory_id)
    if brand_id:
        query = query.filter(Brand.id == brand_id)
    if attribute_id:
        query = query.filter(InventoryItemAttribute.id == attribute_id)
    if variation_id:
        query = query.filter(InventoryItemVariation.id == variation_id)
    if location_id:
        query = query.filter(InventoryLocation.id == location_id)
    if hide_zero_items:
        query = query.filter(InventorySummary.total_quantity != 0)

    # Get ALL results
    all_results = query.all()

    # Process and filter items (same as before, but now with item filter already applied in SQL)
    all_processed_items = []
    for item in all_results:
        quantity = item.total_quantity or 0

        # Calculate status
        if quantity == 0:
            status = 'Out of Stock'
        elif 0 < quantity <= (item.reorder_point or 0) + 2:
            status = 'Low Stock'
        elif quantity < 0:
            status = 'Negative Stock'
        else:
            status = 'In Stock'

        # Apply status filter (if provided)
        if status_filter and status != status_filter:
            continue

        # Create item dictionary (same as before)
        item_dict = {
            'id': item.id,
            'item_name': item.item_name,
            'uom': item.uom or '-',
            'quantity': quantity,
            'image_filename': item.image_filename,
            'subcategory_name': item.subcategory_name,
            'brand': item.brand,
            'attribute': item.attribute,
            'variation': item.variation,
            'variation_id': item.variation_id,
            'location': item.location,
            'location_id': item.location_id,
            'status': status,
            'category_id': item.category_id,
            'subcategory_id': item.subcategory_id,
            'brand_id': item.brand_id,
            'attribute_id': item.attribute_id,
            'category_name': item.category_name
        }
        all_processed_items.append(item_dict)

    # Sort, paginate, and return (same as before)
    # Define sort key functions
    def get_sort_key(item):
        if sort_by == 'item_name':
            return item['item_name']
        elif sort_by == 'category':
            return item['category_name']
        elif sort_by == 'subcategory':
            return item['subcategory_name']
        elif sort_by == 'quantity':
            return item['quantity']
        elif sort_by == 'location':
            return item['location']
        elif sort_by == 'status':
            return item['status']
        else:
            return item['item_name']

    # Sort all items
    all_processed_items.sort(key=get_sort_key, reverse=(sort_order == 'desc'))

    total_items = len(all_processed_items)
    total_pages = (total_items + per_page - 1) // per_page if total_items > 0 else 1

    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_items = all_processed_items[start_idx:end_idx]

    # Group by category
    stock_by_category = {}
    for item in paginated_items:
        category = item['category_name']
        if category not in stock_by_category:
            stock_by_category[category] = []
        stock_by_category[category].append(item)

    pagination_data = {
        'page': page,
        'per_page': per_page,
        'total_pages': total_pages,
        'total_items': total_items,
        'has_next': page < total_pages,
        'has_prev': page > 1
    }

    return stock_by_category, pagination_data

def _get_stock_list_data_with_dates(db_session, page, per_page, hide_zero_items=False, start_date=None,
                                    end_date=None, category_id=None, subcategory_id=None, brand_id=None,
                                    attribute_id=None, variation_id=None, location_id=None,
                                    status_filter=None, item_id=None, sort_by='item_name', sort_order='asc'):  # 🔴 ADD item_id parameter
    """
    SIMPLIFIED: Uses a cleaner approach with separate queries to avoid JOIN complexity.
    """
    app_id = current_user.app_id

    # First get all base items
    base_query = db_session.query(
        InventoryItem.id,
        InventoryItem.item_name,
        InventoryItem.image_filename,
        InventoryCategory.category_name,
        InventoryCategory.id.label('category_id'),
        InventorySubCategory.subcategory_name,
        InventorySubCategory.id.label('subcategory_id'),
        Brand.name.label('brand'),
        Brand.id.label('brand_id'),
        InventoryItemAttribute.attribute_name.label('attribute'),
        InventoryItemAttribute.id.label('attribute_id'),
        InventoryItemVariation.variation_name.label('variation'),
        InventoryItemVariation.id.label('variation_id'),
        InventoryItem.reorder_point,
        UnitOfMeasurement.abbreviation.label('uom'),
        InventoryItemVariationLink.id.label('variation_link_id')
    ).join(
        InventoryCategory, InventoryItem.item_category_id == InventoryCategory.id
    ).join(
        InventorySubCategory, InventoryItem.item_subcategory_id == InventorySubCategory.id
    ).outerjoin(
        InventoryItemVariationLink, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
    ).outerjoin(
        Brand, InventoryItem.brand_id == Brand.id
    ).outerjoin(
        InventoryItemAttribute, InventoryItemVariationLink.attribute_id == InventoryItemAttribute.id
    ).outerjoin(
        InventoryItemVariation, InventoryItemVariationLink.inventory_item_variation_id == InventoryItemVariation.id
    ).outerjoin(
        UnitOfMeasurement, InventoryItem.uom_id == UnitOfMeasurement.id
    ).filter(
        InventoryItem.app_id == app_id,
        InventoryItem.status == 'active',
        InventoryCategory.app_id == app_id,
        InventorySubCategory.app_id == app_id
    )

    # Apply basic filters
    if category_id:
        base_query = base_query.filter(InventoryCategory.id == category_id)
    if subcategory_id:
        base_query = base_query.filter(InventorySubCategory.id == subcategory_id)
    if brand_id:
        base_query = base_query.filter(Brand.id == brand_id)
    if attribute_id:
        base_query = base_query.filter(InventoryItemAttribute.id == attribute_id)
    if variation_id:
        base_query = base_query.filter(InventoryItemVariation.id == variation_id)

    # 🔴 ADD item filter
    if item_id:
        base_query = base_query.filter(InventoryItemVariationLink.id == item_id)

    base_query = base_query.order_by(
        InventoryCategory.category_name,
        InventorySubCategory.subcategory_name,
        InventoryItem.item_name
    )

    # Get locations
    locations_query = db_session.query(InventoryLocation).filter(InventoryLocation.app_id == app_id)
    if location_id:
        locations_query = locations_query.filter(InventoryLocation.id == location_id)
    locations = locations_query.order_by(InventoryLocation.location).all()

    base_results = base_query.all()

    if not base_results:
        return {}, {
            'page': page,
            'per_page': per_page,
            'total_pages': 0,
            'total_items': 0,
            'has_next': False,
            'has_prev': False
        }

    # Get quantities in batch
    variation_link_ids = [r.variation_link_id for r in base_results if r.variation_link_id]
    location_ids = [loc.id for loc in locations]

    if not variation_link_ids or not location_ids:
        return {}, {
            'page': page,
            'per_page': per_page,
            'total_pages': 0,
            'total_items': 0,
            'has_next': False,
            'has_prev': False
        }

    quantity_query = db_session.query(
        InventoryTransactionDetail.item_id,
        InventoryTransactionDetail.location_id,
        func.sum(InventoryTransactionDetail.quantity).label('total_quantity')
    ).filter(
        InventoryTransactionDetail.app_id == app_id,
        InventoryTransactionDetail.item_id.in_(variation_link_ids),
        InventoryTransactionDetail.location_id.in_(location_ids)
    )

    if start_date:
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            quantity_query = quantity_query.filter(InventoryTransactionDetail.transaction_date >= start_date_obj)
        except ValueError:
            pass

    if end_date:
        try:
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
            quantity_query = quantity_query.filter(InventoryTransactionDetail.transaction_date <= end_date_obj)
        except ValueError:
            pass

    quantity_results = quantity_query.group_by(
        InventoryTransactionDetail.item_id,
        InventoryTransactionDetail.location_id
    ).all()

    quantity_dict = {(q.item_id, q.location_id): (q.total_quantity or 0) for q in quantity_results}

    # Build ALL items first (no pagination yet)
    all_items = []

    for base_item in base_results:
        if not base_item.variation_link_id:
            continue

        for location in locations:
            quantity = quantity_dict.get((base_item.variation_link_id, location.id), 0)

            if hide_zero_items and quantity == 0:
                continue

            # Determine status
            if quantity == 0:
                status = 'Out of Stock'
            elif 0 < quantity <= (base_item.reorder_point or 0) + 2:
                status = 'Low Stock'
            elif quantity < 0:
                status = 'Negative Stock'
            else:
                status = 'In Stock'

            # Apply status filter
            if status_filter and status != status_filter:
                continue

            item_data = {
                'id': base_item.id,
                'item_name': base_item.item_name,
                'uom': base_item.uom or '-',
                'quantity': quantity,
                'image_filename': base_item.image_filename,
                'subcategory_name': base_item.subcategory_name,
                'brand': base_item.brand,
                'attribute': base_item.attribute,
                'variation': base_item.variation,
                'variation_id': base_item.variation_id,
                'location': location.location,
                'location_id': location.id,
                'status': status,
                'category_id': base_item.category_id,
                'subcategory_id': base_item.subcategory_id,
                'brand_id': base_item.brand_id,
                'attribute_id': base_item.attribute_id,
                'category_name': base_item.category_name
            }

            all_items.append(item_data)

    # Sort all items
    # Define sort key functions
    def get_sort_key(item):
        if sort_by == 'item_name':
            return item['item_name']
        elif sort_by == 'category':
            return item['category_name']
        elif sort_by == 'subcategory':
            return item['subcategory_name']
        elif sort_by == 'quantity':
            return item['quantity']
        elif sort_by == 'location':
            return item['location']
        elif sort_by == 'status':
            return item['status']
        else:
            return item['item_name']

    # Sort all items
    all_items.sort(key=get_sort_key, reverse=(sort_order == 'desc'))

    total_items = len(all_items)
    total_pages = (total_items + per_page - 1) // per_page if total_items > 0 else 1

    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_items = all_items[start_idx:end_idx]

    # Rebuild paginated structure
    paginated_stock_by_category = {}
    for item in paginated_items:
        category = item['category_name']
        if category not in paginated_stock_by_category:
            paginated_stock_by_category[category] = []
        paginated_stock_by_category[category].append(item)

    pagination_data = {
        'page': page,
        'per_page': per_page,
        'total_pages': total_pages,
        'total_items': total_items,
        'has_next': page < total_pages,
        'has_prev': page > 1
    }

    return paginated_stock_by_category, pagination_data


# Inventory Detail version
# @inventory_bp.route('/stock_list_grid')
# @login_required
# @cached_route(timeout=300, key_func=stock_list_grid_cache_key)
# def stock_list_grid():
#     db_session = Session()
#     try:
#         app_id = current_user.app_id
#         company = db_session.query(Company).filter_by(id=app_id).first()
#         role = current_user.role
#         modules_data = [mod.module_name for mod in
#                         db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
#
#         # Query to get CURRENT stock values grouped by category - USING InventorySummary
#         results = db_session.query(
#             InventoryItem.id,
#             InventoryItem.item_name,
#             InventoryItem.image_filename,
#             InventoryCategory.category_name,
#             InventorySubCategory.subcategory_name,
#             func.coalesce(func.sum(InventorySummary.total_quantity), 0).label('total_quantity')
#         ).join(
#             InventoryCategory, InventoryItem.item_category_id == InventoryCategory.id
#         ).join(
#             InventorySubCategory, InventoryItem.item_subcategory_id == InventorySubCategory.id
#         ).outerjoin(
#             InventoryItemVariationLink, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
#         ).outerjoin(
#             InventorySummary, InventorySummary.item_id == InventoryItemVariationLink.id
#         ).filter(
#             InventoryItem.app_id == app_id,
#             InventoryItem.status == 'active',
#             # app_id filters
#             InventoryCategory.app_id == app_id,
#             InventorySubCategory.app_id == app_id,
#             or_(
#                 InventoryItemVariationLink.app_id == app_id,
#                 InventoryItemVariationLink.id.is_(None)
#             ),
#             InventorySummary.app_id == app_id
#         ).group_by(
#             InventoryItem.id,
#             InventoryCategory.category_name,
#             InventorySubCategory.subcategory_name
#         ).all()
#
#         # Print results for debugging
#         for item in results:
#             print(
#                 f"ItemID: {item.id} Item: {item.item_name}, Quantity: {item.total_quantity}, Image name: {item.image_filename}")
#
#         # Organize data by category
#         stock_by_category = {}
#         for item in results:
#             category = item.category_name
#             if category not in stock_by_category:
#                 stock_by_category[category] = []
#
#             # Determine stock status based on quantity
#             if item.total_quantity == 0:
#                 status = 'Out of Stock'
#             elif item.total_quantity < 0:
#                 status = 'Negative Stock'
#             elif item.total_quantity <= 5:  # You can adjust this threshold
#                 status = 'Low Stock'
#             else:
#                 status = 'In Stock'
#
#             stock_by_category[category].append({
#                 'id': item.id,
#                 'item_name': item.item_name,
#                 'quantity': item.total_quantity,
#                 'image_filename': item.image_filename,
#                 'subcategory_name': item.subcategory_name,
#                 'status': status
#             })
#
#         return render_template(
#             'stock_list_grid.html',
#             stock_by_category=stock_by_category,
#             modules=modules_data,
#             company=company,
#             role=role,
#             module_name="Inventory"
#         )
#
#     except Exception as e:
#         # Log the error and show a flash message to the user
#         logger.error(f"Error retrieving stock list: {e}")
#         flash("An error occurred while retrieving the stock list. Please try again later.", "error")
#         return redirect(url_for('dashboard'))
#
#     finally:
#         db_session.close()

# Inventory Summary Version


@inventory_bp.route('/stock_list_grid')
@login_required
@cached_route(timeout=300, key_func=stock_list_grid_cache_key)
def stock_list_grid():
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Get hide_zero_items parameter from request
        hide_zero_items = request.args.get('hide_zero_items', 'false').lower() == 'true'

        # Query to get CURRENT stock values grouped by category - USING InventorySummary
        query = db_session.query(
            InventoryItem.id,
            InventoryItem.item_name,
            InventoryItem.image_filename,
            InventoryCategory.category_name,
            InventorySubCategory.subcategory_name,
            func.coalesce(func.sum(InventorySummary.total_quantity), 0).label('total_quantity')
        ).join(
            InventoryCategory, InventoryItem.item_category_id == InventoryCategory.id
        ).join(
            InventorySubCategory, InventoryItem.item_subcategory_id == InventorySubCategory.id
        ).outerjoin(
            InventoryItemVariationLink, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).outerjoin(
            InventorySummary, and_(
                InventorySummary.item_id == InventoryItemVariationLink.id,
                InventorySummary.app_id == app_id  # ✅ Add this
            )
        ).filter(
            InventoryItem.app_id == app_id,
            InventoryItem.status == 'active',
            # app_id filters
            InventoryCategory.app_id == app_id,
            InventorySubCategory.app_id == app_id,
            or_(
                InventoryItemVariationLink.app_id == app_id,
                InventoryItemVariationLink.id.is_(None)
            ),
            or_(
                InventorySummary.app_id == app_id,
                InventorySummary.id.is_(None)
            )
        ).group_by(
            InventoryItem.id,
            InventoryCategory.category_name,
            InventorySubCategory.subcategory_name
        )

        # Filter out zero quantity items if hide_zero_items is True
        if hide_zero_items:
            query = query.having(func.coalesce(func.sum(InventorySummary.total_quantity), 0) != 0)

        results = query.all()

        # Print results for debugging
        for item in results:
            print(
                f"ItemID: {item.id} Item: {item.item_name}, Quantity: {item.total_quantity}, Image name: {item.image_filename}")

        # Organize data by category
        stock_by_category = {}
        for item in results:
            category = item.category_name
            if category not in stock_by_category:
                stock_by_category[category] = []

            # Determine stock status based on quantity
            if item.total_quantity == 0:
                status = 'Out of Stock'
            elif item.total_quantity < 0:
                status = 'Negative Stock'
            elif item.total_quantity <= 5:  # You can adjust this threshold
                status = 'Low Stock'
            else:
                status = 'In Stock'

            stock_by_category[category].append({
                'id': item.id,
                'item_name': item.item_name,
                'quantity': item.total_quantity,
                'image_filename': item.image_filename,
                'subcategory_name': item.subcategory_name,
                'status': status
            })

        return render_template(
            'stock_list_grid.html',
            stock_by_category=stock_by_category,
            modules=modules_data,
            company=company,
            role=role,
            module_name="Inventory",
            hide_zero_items=hide_zero_items  # Pass to template
        )

    except Exception as e:
        # Log the error and show a flash message to the user
        logger.error(f"Error retrieving stock list: {e}")
        flash("An error occurred while retrieving the stock list. Please try again later.", "error")
        return redirect(url_for('dashboard'))

    finally:
        db_session.close()
