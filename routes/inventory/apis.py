# all inventory-related API endpoints


# app/routes/inventory/apis.py

import logging
import os
import traceback
from datetime import datetime

from flask import Blueprint, jsonify, render_template, flash, redirect, url_for, request, abort, send_from_directory
from flask_login import login_required, current_user
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from api_routes import api_routes
from decorators import role_required, require_permission
from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Project, ChartOfAccounts, PaymentMode, Category, Department, Employee, \
    PayrollPeriod, InventoryCategory, InventorySubCategory, InventoryLocation, InventoryItem, InventoryItemVariation, \
    InventoryItemAttribute, Brand, InventoryItemVariationLink, InventoryEntry, InventoryEntryLineItem, \
    InventoryTransactionDetail, User, UserLocationAssignment, Attachment
from services.inventory_helpers import suggest_next_inventory_reference, calculate_inventory_quantities, \
    reverse_inventory_entry, reverse_single_transaction_detail, suggest_next_pos_order_reference, \
    update_user_location_role, get_location_users, deactivate_user_location_assignment, assign_user_to_location, \
    get_inventory_quantity_and_cost, get_stock_by_department_filtered, get_stock_by_department_breakdown
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.date_time_utils import get_date_range_from_filter
from utils_and_helpers.numbers import safe_int_conversion

from . import inventory_bp

# Set up logging
logger = logging.getLogger(__name__)


@inventory_bp.route('/api/check_inventory_reference')
@login_required
def check_inventory_reference():
    reference = request.args.get('reference', type=str)
    exists = False

    if reference:
        with Session() as db_session:
            exists = db_session.query(InventoryEntry).filter(
                InventoryEntry.app_id == current_user.app_id,
                func.lower(InventoryEntry.reference) == reference.lower()
            ).count() > 0

    return jsonify({'exists': exists})


@inventory_bp.route('/api/suggest_inventory_reference')
@login_required
def suggest_inventory_reference():
    """
    API endpoint to suggest the next inventory reference based on the most recent reference.
    """
    with Session() as db_session:
        suggested_ref = suggest_next_inventory_reference(db_session)

    return jsonify({'suggested_reference': suggested_ref})


@api_routes.route('/api/inventory_stock', methods=['GET'])
@login_required
def api_inventory_stock():
    """
    Flexible API for inventory stock information with various filtering options
    Supports department filtering for transfers
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get parameters
        location_id = request.args.get('location_id', type=int)
        item_ids = request.args.getlist('item_ids[]', type=int)
        project_id = request.args.get('project_id', type=int)
        status = request.args.get('status', 'active')
        include_negative = request.args.get('include_negative', 'false').lower() == 'true'
        group_by_location = request.args.get('group_by_location', 'false').lower() == 'true'
        use_variation_ids = request.args.get('use_variation_ids', 'false').lower() == 'true'

        # If project_id is provided, get stock for that specific department
        if project_id:
            stock_data = get_stock_by_department_filtered(
                db_session=db_session,
                app_id=app_id,
                item_ids=item_ids,
                location_id=location_id,
                project_id=project_id,
                use_variation_ids=use_variation_ids
            )
            departments_data = None
        else:
            # Get total stock (across all departments)
            stock_data = calculate_inventory_quantities(
                db_session=db_session,
                app_id=app_id,
                item_ids=item_ids,
                use_variation_ids=use_variation_ids,
                location_id=location_id,
                include_negative=include_negative,
                group_by_location=group_by_location
            )

            # Get department breakdown for transfer form
            departments_data = get_stock_by_department_breakdown(
                db_session=db_session,
                app_id=app_id,
                item_ids=item_ids,
                location_id=location_id,
                use_variation_ids=use_variation_ids
            )

        return jsonify({
            'success': True,
            'stock_data': stock_data,
            'departments': departments_data,
            'parameters': {
                'location_id': location_id,
                'project_id': project_id,
                'include_negative': include_negative,
                'group_by_location': group_by_location
            }
        })

    except Exception as e:
        logger.error(f"Error in api_inventory_stock: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching inventory stock'
        }), 500

    finally:
        db_session.close()



@api_routes.route('/api/inventory_stock_with_cost', methods=['GET'])
@login_required
def api_inventory_stock_with_cost():
    """
    Flexible API for inventory stock information including average cost
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get parameters
        location_id = request.args.get('location_id', type=int)
        item_ids = request.args.getlist('item_ids[]', type=int)
        status = request.args.get('status', 'active')
        include_negative = request.args.get('include_negative', 'false').lower() == 'true'
        group_by_location = request.args.get('group_by_location', 'false').lower() == 'true'
        use_variation_ids = request.args.get('use_variation_ids', 'false').lower() == 'true'

        # Build base query
        query = db_session.query(InventoryItem).filter_by(app_id=app_id)
        if status != 'all':
            query = query.filter_by(status=status)

        # Calculate quantities with cost
        stock_data = get_inventory_quantity_and_cost(
            db_session=db_session,
            app_id=app_id,
            item_ids=item_ids,
            use_variation_ids=use_variation_ids,
            location_id=location_id,
            include_negative=include_negative,
            group_by_location=group_by_location
        )

        return jsonify({
            'success': True,
            'stock_data': stock_data,
            'parameters': {
                'location_id': location_id,
                'include_negative': include_negative,
                'group_by_location': group_by_location
            }
        })

    except Exception as e:
        logger.error(f"Error in api_inventory_stock_with_cost: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching inventory stock with cost'
        }), 500

    finally:
        db_session.close()

@inventory_bp.route('/api/bulk_delete_entries', methods=['POST'])
@login_required
@role_required(['Admin', 'Supervisor'])
def bulk_delete_entries():
    """Bulk delete specific transaction details and clean up empty entries"""

    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()

        # Handle various input formats for entry_ids (which are transaction detail IDs)
        raw_entry_ids = data.get('entry_ids', [])
        transaction_detail_ids = safe_int_conversion(raw_entry_ids)

        if not transaction_detail_ids:
            db_session.close()
            return jsonify({
                'success': False,
                'message': 'No transaction details selected for deletion'
            }), 400

        # Get the transaction details with their related line items and entries
        transaction_details = db_session.query(InventoryTransactionDetail).options(
            joinedload(InventoryTransactionDetail.inventory_entry_line_item).joinedload(
                InventoryEntryLineItem.inventory_entry)
        ).filter(
            InventoryTransactionDetail.id.in_(transaction_detail_ids),
            InventoryTransactionDetail.app_id == app_id
        ).all()

        if not transaction_details:
            db_session.close()
            return jsonify({
                'success': False,
                'message': 'No valid transaction details found'
            }), 404

        successful_details = []
        failed_details = []
        entries_to_check = set()  # Track entries that might need deletion

        # Process each transaction detail individually
        for detail in transaction_details:
            try:
                # First, reverse this specific transaction detail
                reverse_single_transaction_detail(db_session, detail)

                # Store the entry for later cleanup check
                inventory_entry = detail.inventory_entry_line_item.inventory_entry
                entries_to_check.add(inventory_entry.id)

                # Delete the transaction detail
                db_session.delete(detail)

                # Check if the parent line item has any other transaction details
                line_item = detail.inventory_entry_line_item
                remaining_details = db_session.query(InventoryTransactionDetail).filter(
                    InventoryTransactionDetail.inventory_entry_line_item_id == line_item.id
                ).count()

                # If this was the last transaction detail for this line item, delete the line item too
                if remaining_details == 0:
                    db_session.delete(line_item)
                    logger.info(f"Deleted line item {line_item.id} as it had no more transaction details")

                successful_details.append(detail.id)
                logger.info(f"Successfully deleted transaction detail {detail.id}")

            except Exception as e:
                logger.error(f"Error processing transaction detail {detail.id}: {str(e)}\n{traceback.format_exc()}")
                failed_details.append({'id': detail.id, 'error': str(e)})
                db_session.rollback()  # Rollback this detail's changes

        # Check if any entries are now empty and should be deleted
        entries_deleted = []
        for entry_id in entries_to_check:
            try:
                # Check if this entry has any remaining line items
                remaining_line_items = db_session.query(InventoryEntryLineItem).filter(
                    InventoryEntryLineItem.inventory_entry_id == entry_id
                ).count()

                if remaining_line_items == 0:
                    # Entry has no more line items, delete it
                    entry = db_session.query(InventoryEntry).get(entry_id)
                    if entry:
                        db_session.delete(entry)
                        entries_deleted.append(entry_id)
                        logger.info(f"Deleted inventory entry {entry_id} as it had no more line items")

            except Exception as e:
                logger.error(f"Error checking entry {entry_id} for deletion: {str(e)}")
                # Don't fail the whole operation if entry cleanup fails

        # Final commit for all successful deletions
        try:
            db_session.commit()
            # Clear cache after deletion
            clear_stock_history_cache()
            logger.info(
                f"Successfully cleared stock history cache after deletion of {len(successful_details)} transaction details and {len(entries_deleted)} entries")
        except Exception as e:
            logger.error(f"Error committing final changes: {str(e)}")
            db_session.rollback()
            # If commit fails, mark all as failed
            failed_details.extend([{'id': detail_id, 'error': 'Commit failed'} for detail_id in successful_details])
            successful_details = []
            entries_deleted = []

        response_data = {
            'success': True if successful_details else False,
            'message': f'Deleted {len(successful_details)} transaction details successfully',
            'deleted_details': successful_details,
            'deleted_entries': entries_deleted,
            'failed_details': failed_details
        }

        if entries_deleted:
            response_data['message'] += f' and {len(entries_deleted)} empty entries'

        if failed_details:
            response_data['message'] += f', {len(failed_details)} failed'

        db_session.close()
        return jsonify(response_data)

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk delete entries: {str(e)}\n{traceback.format_exc()}")
        db_session.close()
        return jsonify({
            'success': False,
            'message': f'Internal server error: {str(e)}'
        }), 500


@inventory_bp.route('/pos/api/suggest_order_reference')
@login_required
def suggest_pos_order_reference():
    """
    API endpoint to suggest the next POS order reference based on the most recent reference.
    """
    with Session() as db_session:
        suggested_ref = suggest_next_pos_order_reference(db_session)

    return jsonify({'suggested_reference': suggested_ref})

@inventory_bp.route('/inventory_location/assign_user', methods=['POST'])
@role_required(['Admin', 'Contributor'])
def assign_user_to_location_route():
    """Assign a user to a location"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        location_id = data.get('location_id')
        role = data.get('role', 'staff')

        if not user_id or not location_id:
            return jsonify({'error': 'User ID and Location ID are required'}), 400

        # Validate that the location belongs to current user's app
        with Session() as db_session:
            location = db_session.query(InventoryLocation).filter_by(
                id=location_id,
                app_id=current_user.app_id
            ).first()
            if not location:
                return jsonify({'error': 'Location not found'}), 404

        # Assign or update user to location (function handles both)
        assignment = assign_user_to_location(
            user_id=user_id,
            location_id=location_id,
            app_id=current_user.app_id,
            assigned_by=current_user.id,
            role=role
        )

        return jsonify({
            'success': 'User assigned to location successfully',
            'assignment_id': assignment.id
        }), 200

    except Exception as e:
        logger.error(f"Error assigning user to location: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred while assigning user to location'}), 500

@inventory_bp.route('/inventory_location/remove_user', methods=['POST'])
@role_required(['Admin', 'Contributor'])
def remove_user_from_location_route():
    """Remove a user from a location"""
    try:
        data = request.get_json()
        assignment_id = data.get('assignment_id')

        if not assignment_id:
            return jsonify({'error': 'Assignment ID is required'}), 400

        with Session() as db_session:
            # Verify the assignment belongs to current user's app
            assignment = db_session.query(UserLocationAssignment).join(
                InventoryLocation
            ).filter(
                UserLocationAssignment.id == assignment_id,
                InventoryLocation.app_id == current_user.app_id
            ).first()

            if not assignment:
                return jsonify({'error': 'Assignment not found'}), 404

        # Deactivate the assignment
        success = deactivate_user_location_assignment(
            user_id=assignment.user_id,
            location_id=assignment.location_id,
            app_id=current_user.app_id
        )

        if success:
            return jsonify({'success': 'User removed from location successfully'}), 200
        else:
            return jsonify({'error': 'Failed to remove user from location'}), 400

    except Exception as e:
        logger.error(f"Error removing user from location: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred while removing user from location'}), 500


@inventory_bp.route('/api/users')
@role_required(['Admin', 'Supervisor'])
def get_users_route():
    """Return list of users for dropdown"""
    try:
        with Session() as db_session:
            # Get all active users in the current app
            users = db_session.query(User).filter_by(
                app_id=current_user.app_id
            ).all()

            users_data = []
            for user in users:
                users_data.append({
                    'id': user.id,
                    'name': user.name or '',
                    'email': user.email,
                    'username': user.name or '',
                    'role': user.role,
                    'display_name': user.name or user.username  # Use name field directly
                })

            return jsonify({'users': users_data}), 200

    except Exception as e:
        logger.error(f"Error fetching users: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred while fetching users'}), 500


@inventory_bp.route('/api/location/<int:location_id>/users')
@role_required(['Admin', 'Contributor'])
def get_location_users_route(location_id):
    """Get all users assigned to a specific location"""
    try:
        # Verify location belongs to current app
        with Session() as db_session:
            location = db_session.query(InventoryLocation).filter_by(
                id=location_id,
                app_id=current_user.app_id
            ).first()
            if not location:
                return jsonify({'error': 'Location not found'}), 404

        users = get_location_users(location_id, current_user.app_id)

        users_data = []
        for user in users:
            users_data.append({
                'id': user.id,
                'first_name': user.first_name,
                'last_name': user.last_name,
                'email': user.email,
                'username': user.username
            })

        return jsonify({'users': users_data}), 200

    except Exception as e:
        logger.error(f"Error fetching location users: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred while fetching location users'}), 500


@inventory_bp.route('/inventory_location/update_user_role', methods=['POST'])
@role_required(['Admin', 'Contributor'])
def update_user_location_role_route():
    """Update a user's role at a location"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        location_id = data.get('location_id')
        new_role = data.get('role')

        if not all([user_id, location_id, new_role]):
            return jsonify({'error': 'User ID, Location ID, and Role are required'}), 400

        success = update_user_location_role(
            user_id=user_id,
            location_id=location_id,
            app_id=current_user.app_id,
            new_role=new_role
        )

        if success:
            return jsonify({'success': 'User role updated successfully'}), 200
        else:
            return jsonify({'error': 'Failed to update user role'}), 400

    except Exception as e:
        logger.error(f"Error updating user role: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred while updating user role'}), 500


@inventory_bp.route('/api/projects', methods=['GET'])
@login_required
def api_get_projects():
    """Return list of active projects for dropdowns."""
    db_session = Session()
    app_id = current_user.app_id

    try:
        projects = (
            db_session.query(Project)
            .filter(Project.app_id == app_id)
            .order_by(Project.name.asc())
            .all()
        )

        data = [
            {
                "id": project.id,
                "name": project.name,
                "description": project.description or ""
            }
            for project in projects
        ]

        return jsonify({"projects": data}), 200
    except Exception as e:
        db_session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        db_session.close()


@inventory_bp.route('/api/locations/<int:location_id>')
@login_required
def get_location_details(location_id):
    """
    API endpoint to get details for a specific inventory location
    """
    try:
        db_session = Session()
        # Query the location with all related data
        location = db_session.query(InventoryLocation).options(
            joinedload(InventoryLocation.project),
            joinedload(InventoryLocation.discount_account),
            joinedload(InventoryLocation.payment_account),
            joinedload(InventoryLocation.card_payment_account),
            joinedload(InventoryLocation.mobile_money_account),
            joinedload(InventoryLocation.user_location_assignments).joinedload(UserLocationAssignment.user)
        ).filter_by(id=location_id, app_id=current_user.app_id).first()

        if not location:
            return jsonify({'error': 'Location not found'}), 404

        # Return the location data in the format expected by the frontend
        return jsonify({
            'location': {
                'id': location.id,
                'location': location.location,
                'description': location.description,
                'project_id': location.project_id,
                'discount_account_id': location.discount_account_id,
                'payment_account_id': location.payment_account_id,
                'card_payment_account_id': location.card_payment_account_id,
                'mobile_money_account_id': location.mobile_money_account_id,
                'workflow_type': location.workflow_type,
                # Include additional info if needed
                'project_name': location.project.name if location.project else None,
                'discount_account_name': location.discount_account.sub_category if location.discount_account else None,
                'payment_account_name': location.payment_account.sub_category if location.payment_account else None,
                'card_payment_account_name': location.card_payment_account.sub_category if location.card_payment_account else None,
                'mobile_money_account_name': location.mobile_money_account.sub_category if location.mobile_money_account else None
            }
        })

    except Exception as e:
        logger.error(f"Error fetching location details: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


@inventory_bp.route('/api/inventory/markup-analysis', methods=["GET"])
def get_markup_analysis():
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    # Get ALL the same filters as main API
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    time_filter = request.args.get("time_filter")
    category_id = request.args.get("category")
    subcategory_id = request.args.get("subcategory")
    location_id = request.args.get("location")
    brand_id = request.args.get("brand")
    status = request.args.get("status", "active")
    item_id = request.args.get("item")
    variation_id = request.args.get("variation_id")
    currency_filter = request.args.get("currency")

    with Session() as db_session:
        try:
            # Validate company
            company = db_session.query(Company).filter_by(api_key=api_key).first()
            if not company:
                return jsonify({"error": "Invalid API key"}), 403

            app_id = company.id

            # Get date range
            if time_filter:
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
                start_date_obj, end_date_obj = get_date_range_from_filter('month')

            # Base query - get all sales transactions with their details
            query = db_session.query(
                InventoryTransactionDetail,
                InventoryEntryLineItem.unit_price,
                InventoryEntryLineItem.selling_price,
                InventoryItem.id.label('inventory_item_id'),  # ✅ Add this
                InventoryItem.item_name,
                InventoryItem.item_code,
                InventoryItem.item_category_id,
                InventoryCategory.category_name,
                InventoryItem.item_subcategory_id,
                InventorySubCategory.subcategory_name,
                InventoryItem.brand_id,

                Brand.name.label('brand_name'),
                InventoryItemVariation.variation_name,
                InventoryLocation.location,
                InventoryEntry.inventory_source,
                InventoryEntry.source_id,
                InventoryEntry.id.label('entry_id')  # Add this for the inventory entry ID
            ).join(
                InventoryEntryLineItem,
                InventoryTransactionDetail.inventory_entry_line_item_id == InventoryEntryLineItem.id
            ).join(
                InventoryEntry,
                InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
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
            ).join(
                InventoryLocation,
                InventoryTransactionDetail.location_id == InventoryLocation.id
            ).filter(
                InventoryTransactionDetail.app_id == app_id,
                # Filter for sales transactions only
                InventoryEntry.inventory_source.in_(['sale', 'direct_sale', 'sales_invoice', 'pos_sale']),
                InventoryTransactionDetail.transaction_date.between(start_date_obj, end_date_obj),
                InventoryEntryLineItem.unit_price > 0,
                InventoryEntryLineItem.selling_price > 0
            )

            # Add this before executing the query
            query = query.order_by(InventoryTransactionDetail.transaction_date.desc())

            # Apply ALL the same filters as main API
            if status and status != 'all':
                query = query.filter(InventoryItem.status == status)

            if item_id:
                query = query.filter(InventoryTransactionDetail.item_id == int(item_id))

            if variation_id:
                query = query.filter(InventoryItemVariationLink.id == variation_id)

            if category_id:
                query = query.filter(InventoryItem.item_category_id == category_id)

            if subcategory_id:
                query = query.filter(InventoryItem.item_subcategory_id == subcategory_id)

            if brand_id:
                query = query.filter(InventoryItem.brand_id == brand_id)

            if location_id:
                query = query.filter(InventoryTransactionDetail.location_id == location_id)

            if currency_filter:
                query = query.filter(InventoryTransactionDetail.currency_id == currency_filter)


            # Execute query
            results = query.all()

            # Process results
            markup_values = []
            category_stats = {}
            location_stats = {}
            sales_transactions = []

            # For item aggregation (Items Needing Review)
            items_data = {}

            # For trend analysis
            trend_data = {}

            for (trans, cost, price, inventory_item_id, item_name, item_code, cat_id, cat_name,
                 subcat_id, subcat_name, brand_id, brand_name, var_name, loc_name, inv_source, source_id, entry_id) in results:

                if cost and price:
                    markup = ((float(price) - float(cost)) / float(cost)) * 100
                    markup_values.append(markup)

                    # Add to sales transactions list (individual sales)
                    sales_transactions.append({
                        'transaction_id': trans.id,
                        'date': trans.transaction_date.strftime('%Y-%m-%d'),
                        'item_id': trans.item_id,
                        'inventory_item_id': inventory_item_id,
                        'item_name': item_name,
                        'item_code': item_code,
                        'variation': var_name,
                        'category': cat_name or 'Uncategorized',
                        'subcategory': subcat_name,
                        'brand': brand_name,
                        'location': loc_name,
                        'cost': round(float(cost), 2),
                        'selling_price': round(float(price), 2),
                        'markup': round(markup, 1),
                        'quantity': abs(float(trans.quantity or 0)),  # ✅ Convert -5 to 5
                        'total': round(float(price) * abs(float(trans.quantity or 0)), 2),  # ✅ Also use abs for total
                        'source': inv_source,  # inventory_source
                        'source_id': source_id,  # Add this if available
                        'entry_id': entry_id  # Add this if available
                    })

                    # Track by item for "Items Needing Review"
                    item_key = f"{trans.item_id}_{trans.location_id}"
                    if item_key not in items_data:
                        items_data[item_key] = {
                            'item_id': trans.item_id,
                            'inventory_item_id': inventory_item_id,  # ✅ Add this
                            'item_name': item_name,
                            'item_code': item_code,
                            'variation': var_name,
                            'category': cat_name or 'Uncategorized',
                            'subcategory': subcat_name,
                            'brand': brand_name,
                            'location': loc_name,
                            'costs': [],
                            'prices': [],
                            'markups': [],
                            'sale_dates': [],
                            'sale_count': 0
                        }

                    items_data[item_key]['costs'].append(float(cost))
                    items_data[item_key]['prices'].append(float(price))
                    items_data[item_key]['markups'].append(markup)
                    items_data[item_key]['sale_dates'].append(trans.transaction_date.strftime('%Y-%m-%d'))
                    items_data[item_key]['sale_count'] += 1

                    # Track by category for averages
                    cat = cat_name or 'Uncategorized'
                    if cat not in category_stats:
                        category_stats[cat] = {'markups': [], 'count': 0}
                    category_stats[cat]['markups'].append(markup)
                    category_stats[cat]['count'] += 1

                    # Track by location for averages
                    if loc_name not in location_stats:
                        location_stats[loc_name] = {'markups': [], 'count': 0}
                    location_stats[loc_name]['markups'].append(markup)
                    location_stats[loc_name]['count'] += 1

                    # Track for trend (monthly)
                    period = trans.transaction_date.strftime('%Y-%m')
                    if period not in trend_data:
                        trend_data[period] = []
                    trend_data[period].append(markup)

            # Calculate distribution
            total = len(markup_values) or 1

            distribution = {
                'negative': len([m for m in markup_values if m < 0]),
                'zero_to_ten': len([m for m in markup_values if 0 <= m < 10]),
                'ten_to_twenty': len([m for m in markup_values if 10 <= m < 20]),
                'twenty_to_fifty': len([m for m in markup_values if 20 <= m < 50]),
                'fifty_to_hundred': len([m for m in markup_values if 50 <= m < 100]),
                'over_hundred': len([m for m in markup_values if m >= 100])
            }

            distribution_percent = {
                k: round((v / total) * 100, 1) for k, v in distribution.items()
            }

            # Build "Items Needing Review" list (aggregated by item)
            # In your backend, remove the filter and include ALL items
            items_needing_review = []
            for item_key, data in items_data.items():
                avg_markup = sum(data['markups']) / len(data['markups'])

                # Include ALL items, filtering happens in frontend
                items_needing_review.append({
                    'item_id': data['item_id'],
                    'inventory_item_id': data['inventory_item_id'],  # ✅ actual item ID
                    'item_name': data['item_name'],
                    'item_code': data['item_code'],
                    'variation': data['variation'],
                    'category': data['category'] or 'Uncategorized',
                    'subcategory': data['subcategory'],
                    'brand': data['brand'],
                    'location': data['location'],
                    'avg_cost': round(sum(data['costs']) / len(data['costs']), 2),
                    'avg_price': round(sum(data['prices']) / len(data['prices']), 2),
                    'avg_markup': round(avg_markup, 1),
                    'sale_count': data['sale_count'],
                    'last_sale': max(data['sale_dates']) if data['sale_dates'] else None
                })

            # Sort by markup (worst first)
            items_needing_review.sort(key=lambda x: x['avg_markup'])

            # Calculate category averages
            category_averages = []
            for cat, data in category_stats.items():
                category_averages.append({
                    'category': cat,
                    'avg_markup': round(sum(data['markups']) / len(data['markups']), 1),
                    'transaction_count': data['count']
                })
            category_averages.sort(key=lambda x: x['avg_markup'])

            # Calculate location averages
            location_averages = []
            for loc, data in location_stats.items():
                location_averages.append({
                    'location': loc,
                    'avg_markup': round(sum(data['markups']) / len(data['markups']), 1),
                    'transaction_count': data['count']
                })
            location_averages.sort(key=lambda x: x['avg_markup'])

            # Calculate trend
            trend = {
                'labels': sorted(trend_data.keys())[-6:],
                'values': [round(sum(trend_data[p]) / len(trend_data[p]), 1)
                           for p in sorted(trend_data.keys())[-6:]]
            }

            return jsonify({
                'filters_applied': {
                    'date_range': f"{start_date_obj} to {end_date_obj}",
                    'time_filter': time_filter,
                    'category': category_id,
                    'subcategory': subcategory_id,
                    'location': location_id,
                    'brand': brand_id,
                    'status': status,
                    'item': item_id,
                    'variation': variation_id,
                    'currency': currency_filter
                },
                'summary': {
                    'total_transactions': len(markup_values),
                    'avg_markup': round(sum(markup_values) / total, 1) if markup_values else 0,
                    'min_markup': round(min(markup_values), 1) if markup_values else 0,
                    'max_markup': round(max(markup_values), 1) if markup_values else 0
                },
                'distribution': distribution,
                'distribution_percent': distribution_percent,
                # Individual sales (for detailed view) - limit to last 100 for performance
                'recent_sales': sales_transactions[:100],
                'total_sales_count': len(sales_transactions),
                # Aggregated items (for "Items Needing Review")
                'items_needing_review': items_needing_review[:50],  # Top 50 problematic items
                'items_needing_review_count': len(items_needing_review),
                'category_averages': category_averages[:10],
                'location_averages': location_averages,
                'trend': trend
            })

        except Exception as e:
            print(f'Error in markup analysis: {str(e)}')
            import traceback
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500



@inventory_bp.route('/attachment/<int:attachment_id>')
@login_required
def serve_attachment(attachment_id):
    """Serve attachment files with access control"""
    db_session = None
    try:
        db_session = Session()
        app_id = current_user.app_id

        # Get the attachment
        attachment = db_session.query(Attachment).filter_by(id=attachment_id).first()

        if not attachment:
            abort(404)

        # Check if user has access to the parent record
        if attachment.record_type == 'inventory_entry':
            # Check if inventory entry belongs to user's company
            entry = db_session.query(InventoryEntry).filter_by(
                id=attachment.record_id,
                app_id=app_id
            ).first()
            if not entry:
                abort(403)

        elif attachment.record_type == 'asset_movement':
            # Check if asset movement belongs to user's company
            movement = db_session.query(AssetMovement).filter_by(
                id=attachment.record_id,
                app_id=app_id
            ).first()
            if not movement:
                abort(403)

        # Check if file exists
        if not os.path.exists(attachment.file_path):
            abort(404)

        # Serve the file
        directory = os.path.dirname(attachment.file_path)
        filename = os.path.basename(attachment.file_path)

        return send_from_directory(directory, filename, as_attachment=True)

    except Exception as e:
        logger.error(f"Error serving attachment: {str(e)}")
        abort(500)
    finally:
        if db_session:
            db_session.close()


@inventory_bp.route('/api/available_stock_by_department', methods=['GET'])
@login_required
def api_available_stock_by_department():
    """
    Get available stock grouped by department for a specific item and location
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        item_id = request.args.get('item_id', type=int)
        location_id = request.args.get('location_id', type=int)

        if not item_id or not location_id:
            return jsonify({'success': False, 'message': 'Item and location are required'}), 400

        # Query transaction details grouped by project
        results = db_session.query(
            Project.id.label('project_id'),
            Project.name.label('project_name'),
            func.sum(
                case(
                    (InventoryTransactionDetail.movement_type == 'in',
                     InventoryTransactionDetail.quantity),
                    (InventoryTransactionDetail.movement_type == 'out',
                     -InventoryTransactionDetail.quantity),
                    else_=0
                )
            ).label('available_quantity')
        ).join(
            InventoryTransactionDetail.project
        ).filter(
            InventoryTransactionDetail.item_id == item_id,
            InventoryTransactionDetail.location_id == location_id,
            InventoryTransactionDetail.app_id == app_id,
            Project.is_active == True
        ).group_by(
            Project.id, Project.name
        ).having(
            func.sum(
                case(
                    (InventoryTransactionDetail.movement_type == 'in',
                     InventoryTransactionDetail.quantity),
                    (InventoryTransactionDetail.movement_type == 'out',
                     -InventoryTransactionDetail.quantity),
                    else_=0
                )
            ) > 0
        ).all()

        departments = []
        for row in results:
            departments.append({
                'project_id': row.project_id,
                'project_name': row.project_name,
                'available_quantity': float(row.available_quantity)
            })

        return jsonify({
            'success': True,
            'departments': departments
        })

    except Exception as e:
        logger.error(f"Error in api_available_stock_by_department: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()
