# app/routes/inventory/inventory_items.py
import os
import traceback
import uuid
from datetime import datetime
from decimal import Decimal

import sqlalchemy
from flask import Blueprint, jsonify, render_template, flash, redirect, url_for, request, current_app
from flask_login import login_required, current_user
from sqlalchemy import func, case
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from sqlalchemy import or_
from werkzeug.utils import secure_filename

from decorators import role_required, require_permission, cached_route
from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Project, ChartOfAccounts, PaymentMode, Category, Department, Employee, \
    PayrollPeriod, InventoryCategory, InventorySubCategory, InventoryLocation, InventoryItem, InventoryItemVariation, \
    InventoryItemAttribute, Brand, InventoryItemVariationLink, UnitOfMeasurement, Vendor, InventoryEntry, \
    InventoryEntryLineItem, InventorySummary, ItemSellingPrice, AssetItem, Asset
import logging

from services.inventory_helpers import calculate_inventory_quantities, get_last_purchase_price, \
    get_weighted_average_cost, calculate_inventory_valuation, safe_clear_stock_history_cache
from utils import empty_to_none, calculate_net_quantity
from utils_and_helpers.amounts_utils import format_amount
from utils_and_helpers.cache_keys import inventory_items_cache_key
from utils_and_helpers.file_utils import allowed_file, is_file_size_valid, file_exists, generate_unique_filename
from . import asset_bp

logger = logging.getLogger(__name__)


@asset_bp.route('/add_new_asset_type', methods=['GET', 'POST'])
@role_required(['Admin', 'Supervisor', 'Contributor'])
@login_required
def add_new_asset_type():
    db_session = Session()
    app_id = current_user.app_id

    try:
        if request.method == 'POST':
            try:
                # Retrieve form data
                asset_name = request.form.get('asset_name', '').strip()
                if not asset_name:
                    return jsonify({'success': False, 'message': 'Asset name is required'}), 400

                asset_code = request.form.get('asset_code', '').strip() or None
                item_category_id = request.form.get('category')
                item_subcategory_id = request.form.get('subcategory')
                brand_id = request.form.get('brand_id') or None
                description = request.form.get('description') or None

                # Asset-specific fields with defaults
                expected_useful_life_years = request.form.get('expected_useful_life_years', '5')
                depreciation_method = request.form.get('depreciation_method', 'straight_line')
                salvage_value_percentage = request.form.get('salvage_value_percentage', '10.00')
                maintenance_interval_days = request.form.get('maintenance_interval_days', '365')

                # Accounting fields
                fixed_asset_account_id = request.form.get('fixed_asset_account_id')
                depreciation_expense_account_id = request.form.get('depreciation_expense_account_id')
                accumulated_depreciation_account_id = request.form.get('accumulated_depreciation_account_id')


                # Handle file upload (optional)
                image_filename = None
                file = request.files.get('asset_image')

                if file and file.filename:
                    if allowed_file(file.filename):
                        if is_file_size_valid(file):
                            filename, file_extension = os.path.splitext(file.filename)
                            unique_filename = f"asset_{app_id}_{uuid.uuid4()}{file_extension}"
                            image_filename = secure_filename(unique_filename)
                            file_path = os.path.join(current_app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)

                            if file_exists(file_path):
                                image_filename = generate_unique_filename(
                                    current_app.config['UPLOAD_FOLDER_INVENTORY'],
                                    image_filename
                                )
                                file_path = os.path.join(current_app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)

                            try:
                                file.save(file_path)
                            except Exception as e:
                                logger.warning(f"Error saving asset image: {e}")
                                # Continue without image - it's optional
                        else:
                            # File too large, but continue without image
                            logger.warning("Asset image file size too large, skipping")
                    else:
                        # Invalid file type, but continue without image
                        logger.warning("Invalid asset image file type, skipping")

                # Create new asset item
                new_asset_item = AssetItem(
                    app_id=app_id,
                    asset_name=asset_name,
                    asset_code=asset_code,
                    brand_id=brand_id,
                    item_category_id=item_category_id,
                    item_subcategory_id=item_subcategory_id,
                    asset_description=description,
                    image_filename=image_filename,

                    # Asset-specific fields
                    expected_useful_life_years=int(expected_useful_life_years),
                    depreciation_method=depreciation_method,
                    salvage_value_percentage=float(salvage_value_percentage),
                    maintenance_interval_days=int(maintenance_interval_days),

                    # Accounting fields
                    fixed_asset_account_id=fixed_asset_account_id,
                    depreciation_expense_account_id=depreciation_expense_account_id,
                    accumulated_depreciation_account_id=accumulated_depreciation_account_id
                )

                db_session.add(new_asset_item)
                db_session.commit()

                return jsonify({
                    'success': True,
                    'message': 'Asset item created successfully',
                    'asset_item_id': new_asset_item.id
                })

            except IntegrityError as e:
                db_session.rollback()
                logger.error(f"Integrity error creating asset item: {str(e)}")
                return jsonify({
                    'success': False,
                    'message': f'Asset Code "{asset_code}" already exists. Use a unique code.'
                }), 400

            except ValueError as e:
                db_session.rollback()
                logger.error(f"Value error creating asset item: {str(e)}")
                return jsonify({
                    'success': False,
                    'message': 'Invalid numeric value provided'
                }), 400

            except Exception as e:
                db_session.rollback()
                logger.error(f"Error creating asset item: {str(e)}\n{traceback.format_exc()}")
                return jsonify({
                    'success': False,
                    'message': f'Error creating asset item: {str(e)}'
                }), 500

        else:
            # GET request - render the form template
            company = db_session.query(Company).filter_by(id=app_id).first()
            role = current_user.role
            modules_data = [mod.module_name for mod in
                            db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

            # Get data for dropdowns
            categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
            subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
            brands = db_session.query(Brand).filter_by(app_id=app_id).all()

            return render_template(
                'assets/new_asset_item.html',  # This is your template file
                company=company,
                role=role,
                modules=modules_data,
                categories=categories,
                subcategories=subcategories,
                brands=brands,
                title="Add New Asset Item"
            )

    except Exception as e:
        logger.error(f"Unexpected error in add_new_asset_item: {str(e)}\n{traceback.format_exc()}")

        if request.method == 'POST':
            return jsonify({
                'success': False,
                'message': f'Unexpected error: {str(e)}'
            }), 500
        else:
            flash(f'Error loading form: {str(e)}', 'error')
            return redirect(request.referrer)

    finally:
        db_session.close()


@asset_bp.route('/asset_type/<int:item_id>', methods=['GET', 'POST'])
@login_required
def asset_type_details(item_id):
    db_session = Session()
    app_id = current_user.app_id

    try:
        asset_item = db_session.query(AssetItem).filter_by(id=item_id, app_id=app_id).first()
        if not asset_item:
            flash('Asset item not found.', 'error')
            return redirect(request.referrer)

        if request.method == 'POST':
            # Handle basic item info
            asset_name = request.form['asset_name']
            asset_code = request.form.get('asset_code', '').strip() or None
            item_category_id = request.form.get('category')
            item_subcategory_id = request.form.get('subcategory')
            brand_id = request.form.get('brand_id') or None
            description = request.form.get('description') or None

            # Asset-specific fields
            expected_useful_life_years = request.form.get('expected_useful_life_years', '5')
            depreciation_method = request.form.get('depreciation_method', 'straight_line')
            salvage_value_percentage = request.form.get('salvage_value_percentage', '10.00')
            maintenance_interval_days = request.form.get('maintenance_interval_days', '365')

            # Accounting fields
            fixed_asset_account_id = request.form.get('fixed_asset_account_id')
            depreciation_expense_account_id = request.form.get('depreciation_expense_account_id')
            accumulated_depreciation_account_id = request.form.get('accumulated_depreciation_account_id')

            # Validate required accounting fields
            if not all([fixed_asset_account_id, depreciation_expense_account_id, accumulated_depreciation_account_id]):
                flash('All accounting fields are required.', 'error')
                return redirect(url_for('assets.asset_item_details', item_id=item_id))

            # Handle file upload
            file = request.files.get('asset_image')
            image_filename = asset_item.image_filename

            if file and allowed_file(file.filename):
                if is_file_size_valid(file):
                    filename, file_extension = os.path.splitext(file.filename)
                    unique_filename = f"asset_{app_id}_{uuid.uuid4()}{file_extension}"
                    image_filename = secure_filename(unique_filename)
                    file_path = os.path.join(current_app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)

                    if file_exists(file_path):
                        image_filename = generate_unique_filename(
                            current_app.config['UPLOAD_FOLDER_INVENTORY'],
                            image_filename
                        )
                        file_path = os.path.join(current_app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)

                    try:
                        file.save(file_path)
                    except Exception as e:
                        logger.warning(f"Error saving asset image: {e}")
                        # Keep old image if new one fails

            # Update asset item
            asset_item.asset_name = asset_name
            asset_item.asset_code = asset_code
            asset_item.item_category_id = item_category_id
            asset_item.item_subcategory_id = item_subcategory_id
            asset_item.brand_id = brand_id
            asset_item.asset_description = description
            asset_item.image_filename = image_filename

            # Asset-specific fields
            asset_item.expected_useful_life_years = int(expected_useful_life_years)
            asset_item.depreciation_method = depreciation_method
            asset_item.salvage_value_percentage = float(salvage_value_percentage)
            asset_item.maintenance_interval_days = int(maintenance_interval_days)

            # Accounting fields
            asset_item.fixed_asset_account_id = fixed_asset_account_id
            asset_item.depreciation_expense_account_id = depreciation_expense_account_id
            asset_item.accumulated_depreciation_account_id = accumulated_depreciation_account_id

            try:
                db_session.commit()
                flash('Asset item updated successfully!', 'success')
                return redirect(url_for('assets.asset_item_details', item_id=item_id))

            except IntegrityError as e:
                db_session.rollback()
                flash(f'Asset Code "{asset_code}" already exists. Use a unique code.', 'error')
                return redirect(url_for('assets.asset_item_details', item_id=item_id))

            except Exception as e:
                db_session.rollback()
                logger.error(f"Error updating asset item: {str(e)}\n{traceback.format_exc()}")
                flash('Error updating asset item', 'error')
                return redirect(url_for('assets.asset_item_details', item_id=item_id))

        # GET REQUEST - Render details page with location tracking
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Get dropdown data
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
        subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
        brands = db_session.query(Brand).filter_by(app_id=app_id).all()

        # Get accounting accounts for dropdowns
        chart_of_accounts = db_session.query(ChartOfAccounts).filter_by(app_id=app_id).all()

        # Get all locations for dropdowns
        all_locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

        # Get all departments for dropdowns
        departments = db_session.query(Department).filter_by(app_id=app_id).all() if hasattr(
            db_session.query(Department), 'first') else []

        # Get all employees for dropdowns
        employees = db_session.query(Employee).filter_by(app_id=app_id).all() if hasattr(db_session.query(Employee),
                                                                                         'first') else []

        # Get all assets of this type with their locations and assignments
        assets = db_session.query(Asset).filter_by(
            asset_item_id=item_id,
            app_id=app_id
        ).options(
            joinedload(Asset.assigned_to),
            joinedload(Asset.location),
            joinedload(Asset.department),
            joinedload(Asset.supplier)
        ).order_by(Asset.asset_tag).all()

        # Calculate comprehensive statistics
        total_assets = len(assets)
        active_assets = len([a for a in assets if a.status in ['assigned', 'in_stock']])
        under_maintenance = len([a for a in assets if a.status == 'maintenance'])
        disposed_assets = len([a for a in assets if a.status == 'disposed'])

        total_value = sum(a.current_value for a in assets if a.current_value)
        total_purchase_value = sum(a.purchase_price for a in assets if a.purchase_price)

        # Location distribution analysis
        location_distribution = {}
        status_distribution = {}
        condition_distribution = {}
        department_distribution = {}

        asset_details = []

        for asset in assets:
            # Asset details for the table
            asset_details.append({
                'id': asset.id,
                'asset_tag': asset.asset_tag,
                'serial_number': asset.serial_number,
                'status': asset.status,
                'condition': asset.condition,
                'location_name': asset.location.location if asset.location else "Not Assigned",
                'location_id': asset.location_id,
                'assigned_to_name': f"{asset.assigned_to.first_name} {asset.assigned_to.last_name}" if asset.assigned_to else "Unassigned",
                'assigned_to_id': asset.assigned_to_id,
                'department_name': asset.department.name if asset.department else "Not Assigned",
                'department_id': asset.department_id,
                'purchase_price': asset.purchase_price,
                'current_value': asset.current_value,
                'purchase_date': asset.purchase_date.strftime('%Y-%m-%d') if asset.purchase_date else None,
                'warranty_expiry': asset.warranty_expiry.strftime('%Y-%m-%d') if asset.warranty_expiry else None

            })

            # Location distribution
            location_name = asset.location.location if asset.location else "Unassigned"
            location_distribution[location_name] = location_distribution.get(location_name, 0) + 1

            # Status distribution
            status = asset.status or "unknown"
            status_distribution[status] = status_distribution.get(status, 0) + 1

            # Condition distribution
            condition = asset.condition or "unknown"
            condition_distribution[condition] = condition_distribution.get(condition, 0) + 1

            # Department distribution
            department_name = asset.department.name if asset.department else "Unassigned"
            department_distribution[department_name] = department_distribution.get(department_name, 0) + 1

        # Sort distributions
        sorted_locations = sorted(location_distribution.items(), key=lambda x: x[1], reverse=True)
        sorted_statuses = sorted(status_distribution.items(), key=lambda x: x[1], reverse=True)
        sorted_conditions = sorted(condition_distribution.items(), key=lambda x: x[1], reverse=True)
        sorted_departments = sorted(department_distribution.items(), key=lambda x: x[1], reverse=True)

        # Calculate location percentages
        location_data = []
        for location_name, count in sorted_locations:
            percentage = (count / total_assets * 100) if total_assets > 0 else 0
            location_data.append({
                'name': location_name,
                'count': count,
                'percentage': round(percentage, 1)
            })

        return render_template(
            'assets/asset_item_details.html',
            asset_item=asset_item,
            categories=categories,
            subcategories=subcategories,
            brands=brands,
            chart_of_accounts=chart_of_accounts,
            all_locations=all_locations,
            departments=departments,
            employees=employees,
            assets=assets,
            asset_details=asset_details,
            total_assets=total_assets,
            active_assets=active_assets,
            under_maintenance=under_maintenance,
            disposed_assets=disposed_assets,
            total_value=total_value,
            total_purchase_value=total_purchase_value,
            location_data=location_data,
            status_distribution=sorted_statuses,
            condition_distribution=sorted_conditions,
            department_distribution=sorted_departments,
            company=company,
            role=role,
            modules=modules_data,
            title=f"Asset Item: {asset_item.asset_name}"
        )

    except Exception as e:
        logger.error(f"Error in asset_item_details route: {e}\n{traceback.format_exc()}")
        flash('An error occurred while processing your request.', 'error')
        return redirect(request.referrer)
    finally:
        db_session.close()


@asset_bp.route('/deactivate_asset_type', methods=['POST'])
@login_required
def deactivate_asset_type():
    db_session = Session()
    try:
        data = request.get_json()
        item_id = data.get('item_id')
        app_id = current_user.app_id

        # Validate item_id
        if not item_id:
            return jsonify({'success': False, 'error': 'Asset Item ID is required.'}), 400

        # Fetch the asset item from the database
        asset_item = db_session.query(AssetItem).filter_by(id=item_id, app_id=app_id).first()
        if not asset_item:
            return jsonify({'success': False, 'error': 'Asset item not found.'}), 404

        # Check if the asset item is already inactive
        if asset_item.status == 'inactive':
            return jsonify({'success': False, 'error': 'Asset item is already inactive.'}), 400

        # Check if there are active assets of this type
        # CORRECTED: Use filter() instead of filter_by() for .in_()
        active_assets = db_session.query(Asset).filter(
            Asset.asset_item_id == item_id,
            Asset.app_id == app_id,
            Asset.status.in_(['assigned', 'in_stock', 'maintenance'])
        ).count()

        if active_assets > 0:
            return jsonify({
                'success': False,
                'error': f'Cannot deactivate asset item. There are {active_assets} active assets of this type.'
            }), 400

        # Update the asset item status to "inactive"
        asset_item.status = 'inactive'
        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Asset item deactivated successfully.',
            'active_assets': active_assets
        })

    except IntegrityError as e:
        db_session.rollback()
        logger.error(f"Integrity error while deactivating asset item {item_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Cannot deactivate asset item. It is referenced by other records.'
        }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error deactivating asset item {item_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)}), 500

    finally:
        db_session.close()


@asset_bp.route('/reactivate_asset_type', methods=['POST'])
@login_required
def reactivate_asset_type():
    db_session = Session()
    try:
        data = request.get_json()
        item_id = data.get('item_id')
        app_id = current_user.app_id

        # Validate item_id
        if not item_id:
            return jsonify({'success': False, 'error': 'Asset Item ID is required.'}), 400

        # Fetch the asset item from the database
        asset_item = db_session.query(AssetItem).filter_by(id=item_id, app_id=app_id).first()
        if not asset_item:
            return jsonify({'success': False, 'error': 'Asset item not found.'}), 404

        # Check if the asset item is already active
        if asset_item.status == 'active':
            return jsonify({'success': False, 'error': 'Asset item is already active.'}), 400

        # Update the asset item status to "active"
        asset_item.status = 'active'
        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Asset item reactivated successfully.'
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error reactivating asset item {item_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)}), 500

    finally:
        db_session.close()


@asset_bp.route('/asset_types')
@login_required
def asset_types():
    """
    Render the asset types management page
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Get filter data
        brands = db_session.query(Brand).filter_by(app_id=app_id).order_by(Brand.name).all()
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).order_by(
            InventoryCategory.category_name).all()
        subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).order_by(
            InventorySubCategory.subcategory_name).all()

        # Get chart of accounts for dropdowns
        chart_of_accounts = db_session.query(ChartOfAccounts).filter_by(
            app_id=app_id, is_active=True
        ).order_by(ChartOfAccounts.sub_category).all()

        # Filter by account_type - simple and works for all clients
        fixed_asset_accounts = [acc for acc in chart_of_accounts if acc.parent_account_type == 'Asset']
        depreciation_expense_accounts = [acc for acc in chart_of_accounts if acc.parent_account_type == 'Expense']
        accumulated_dep_accounts = [acc for acc in chart_of_accounts if acc.parent_account_type == 'Asset']

        return render_template(
            'assets/asset_types.html',
            company=company,
            modules=modules_data,
            role=role,
            brands=brands,
            categories=categories,
            subcategories=subcategories,
            fixed_asset_accounts=fixed_asset_accounts,
            depreciation_expense_accounts=depreciation_expense_accounts,
            accumulated_dep_accounts=accumulated_dep_accounts,
            now=datetime.now()
        )

    except Exception as e:
        logger.error(f"Error rendering asset types page: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the asset types page', 'error')
        return redirect(url_for('main.dashboard'))

    finally:
        db_session.close()


@asset_bp.route('/api/asset_types', methods=['GET'])
@login_required
def api_asset_types():
    """
    Return JSON data of asset types with filtering and pagination
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get pagination and filter parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        search = request.args.get('search', '')
        status = request.args.get('status', '')
        brand_id = request.args.get('brand_id', type=int)
        category_id = request.args.get('category_id', type=int)

        # Build query
        query = db_session.query(AssetItem).filter(
            AssetItem.app_id == app_id
        )

        # Apply filters
        if search:
            search_term = f'%{search}%'
            query = query.filter(
                or_(
                    AssetItem.asset_name.ilike(search_term),
                    AssetItem.asset_code.ilike(search_term),
                    AssetItem.asset_description.ilike(search_term)
                )
            )

        if status:
            query = query.filter(AssetItem.status == status)

        if brand_id:
            query = query.filter(AssetItem.brand_id == brand_id)

        if category_id:
            query = query.filter(AssetItem.item_category_id == category_id)

        # Get total count
        total_items = query.count()

        # Calculate pagination
        total_pages = (total_items + per_page - 1) // per_page

        # Get paginated results
        asset_types = query.order_by(
            AssetItem.created_at.desc()
        ).offset((page - 1) * per_page).limit(per_page).all()

        # Serialize data
        types_data = []
        for item in asset_types:
            types_data.append({
                'id': item.id,
                'asset_name': item.asset_name,
                'asset_code': item.asset_code,
                'brand_name': item.brand.name if item.brand else None,
                'category_name': item.inventory_category.category_name if item.inventory_category else None,
                'subcategory_name': item.inventory_subcategory.subcategory_name if item.inventory_subcategory else None,
                'expected_useful_life_years': item.expected_useful_life_years,
                'depreciation_method': item.depreciation_method,
                'depreciation_method_label': item.depreciation_method.replace('_',
                                                                              ' ').title() if item.depreciation_method else 'Straight Line',
                'salvage_value_percentage': float(
                    item.salvage_value_percentage) if item.salvage_value_percentage else 0,
                'status': item.status,
                'asset_count': len(item.assets) if item.assets else 0,
                'fixed_asset_account': item.fixed_asset_account.sub_category if item.fixed_asset_account else None,
                'depreciation_account': item.depreciation_expense_account.sub_category if item.depreciation_expense_account else None,
                'accumulated_dep_account': item.accumulated_depreciation_account.sub_category if item.accumulated_depreciation_account else None,
                'maintenance_interval_days': item.maintenance_interval_days,
                'created_at': item.created_at.strftime('%Y-%m-%d %H:%M:%S') if item.created_at else None
            })

        return jsonify({
            'success': True,
            'asset_types': types_data,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_items': total_items,
                'has_next': page < total_pages,
                'has_prev': page > 1
            }
        })

    except Exception as e:
        logger.error(f"Error in api_asset_types: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching asset types'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/api/asset_types/<int:type_id>', methods=['GET'])
@login_required
def api_asset_type_detail(type_id):
    """
    Return JSON data for a single asset type
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        asset_type = db_session.query(AssetItem).filter_by(
            id=type_id,
            app_id=app_id
        ).first()

        if not asset_type:
            return jsonify({
                'success': False,
                'message': 'Asset type not found'
            }), 404

        data = {
            'id': asset_type.id,
            'asset_name': asset_type.asset_name,
            'asset_code': asset_type.asset_code,
            'asset_description': asset_type.asset_description,
            'brand_id': asset_type.brand_id,
            'brand_name': asset_type.brand.brand_name if asset_type.brand else None,
            'item_category_id': asset_type.item_category_id,
            'category_name': asset_type.inventory_category.category_name if asset_type.inventory_category else None,
            'item_subcategory_id': asset_type.item_subcategory_id,
            'subcategory_name': asset_type.inventory_subcategory.subcategory_name if asset_type.inventory_subcategory else None,
            'expected_useful_life_years': asset_type.expected_useful_life_years,
            'depreciation_method': asset_type.depreciation_method,
            'salvage_value_percentage': float(
                asset_type.salvage_value_percentage) if asset_type.salvage_value_percentage else 0,
            'fixed_asset_account_id': asset_type.fixed_asset_account_id,
            'depreciation_expense_account_id': asset_type.depreciation_expense_account_id,
            'accumulated_depreciation_account_id': asset_type.accumulated_depreciation_account_id,
            'maintenance_interval_days': asset_type.maintenance_interval_days,
            'status': asset_type.status,
            'image_filename': asset_type.image_filename
        }

        return jsonify({
            'success': True,
            'asset_type': data
        })

    except Exception as e:
        logger.error(f"Error fetching asset type {type_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching asset type details'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/api/asset_types/create', methods=['POST'])
@login_required
@role_required(['Admin', 'Supervisor', 'Contributor'])
def api_asset_type_create():
    """
    Create a new asset type
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ['asset_name', 'fixed_asset_account_id', 'depreciation_expense_account_id',
                           'accumulated_depreciation_account_id']
        for field in required_fields:
            if not data.get(field):
                return jsonify({
                    'success': False,
                    'message': f'{field.replace("_", " ").title()} is required'
                }), 400

        # Check for duplicate asset code
        if data.get('asset_code'):
            existing = db_session.query(AssetItem).filter_by(
                app_id=app_id,
                asset_code=data['asset_code']
            ).first()
            if existing:
                return jsonify({
                    'success': False,
                    'message': f'Asset code "{data["asset_code"]}" is already in use'
                }), 400

        # Create new asset type
        asset_type = AssetItem(
            app_id=app_id,
            asset_name=data['asset_name'],
            asset_code=data.get('asset_code'),
            asset_description=data.get('asset_description'),
            brand_id=data.get('brand_id'),
            item_category_id=data.get('item_category_id'),
            item_subcategory_id=data.get('item_subcategory_id'),
            expected_useful_life_years=data.get('expected_useful_life_years', 5),
            depreciation_method=data.get('depreciation_method', 'straight_line'),
            salvage_value_percentage=data.get('salvage_value_percentage', 10.00),
            fixed_asset_account_id=data['fixed_asset_account_id'],
            depreciation_expense_account_id=data['depreciation_expense_account_id'],
            accumulated_depreciation_account_id=data['accumulated_depreciation_account_id'],
            maintenance_interval_days=data.get('maintenance_interval_days', 365),
            status=data.get('status', 'active'),
            created_at=func.now()
        )

        db_session.add(asset_type)
        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Asset type created successfully',
            'asset_type_id': asset_type.id
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error creating asset type: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while creating asset type'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/api/asset_types/<int:type_id>/update', methods=['PUT'])
@login_required
@role_required(['Admin', 'Supervisor'])
def api_asset_type_update(type_id):
    """
    Update an existing asset type
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        asset_type = db_session.query(AssetItem).filter_by(
            id=type_id,
            app_id=app_id
        ).first()

        if not asset_type:
            return jsonify({
                'success': False,
                'message': 'Asset type not found'
            }), 404

        data = request.get_json()

        # Check for duplicate asset code (excluding current)
        if data.get('asset_code') and data['asset_code'] != asset_type.asset_code:
            existing = db_session.query(AssetItem).filter(
                AssetItem.app_id == app_id,
                AssetItem.asset_code == data['asset_code'],
                AssetItem.id != type_id
            ).first()
            if existing:
                return jsonify({
                    'success': False,
                    'message': f'Asset code "{data["asset_code"]}" is already in use'
                }), 400

        # Update fields
        asset_type.asset_name = data.get('asset_name', asset_type.asset_name)
        asset_type.asset_code = data.get('asset_code', asset_type.asset_code)
        asset_type.asset_description = data.get('asset_description', asset_type.asset_description)
        asset_type.brand_id = data.get('brand_id', asset_type.brand_id)
        asset_type.item_category_id = data.get('item_category_id', asset_type.item_category_id)
        asset_type.item_subcategory_id = data.get('item_subcategory_id', asset_type.item_subcategory_id)
        asset_type.expected_useful_life_years = data.get('expected_useful_life_years',
                                                         asset_type.expected_useful_life_years)
        asset_type.depreciation_method = data.get('depreciation_method', asset_type.depreciation_method)
        asset_type.salvage_value_percentage = data.get('salvage_value_percentage', asset_type.salvage_value_percentage)
        asset_type.fixed_asset_account_id = data.get('fixed_asset_account_id', asset_type.fixed_asset_account_id)
        asset_type.depreciation_expense_account_id = data.get('depreciation_expense_account_id',
                                                              asset_type.depreciation_expense_account_id)
        asset_type.accumulated_depreciation_account_id = data.get('accumulated_depreciation_account_id',
                                                                  asset_type.accumulated_depreciation_account_id)
        asset_type.maintenance_interval_days = data.get('maintenance_interval_days',
                                                        asset_type.maintenance_interval_days)
        asset_type.status = data.get('status', asset_type.status)
        asset_type.updated_at = func.now()

        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Asset type updated successfully'
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error updating asset type {type_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while updating asset type'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/api/asset_types/<int:type_id>/delete', methods=['DELETE'])
@login_required
@role_required(['Admin', 'Supervisor'])
def api_asset_type_delete(type_id):
    """
    Delete an asset type (only if no assets are linked)
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        asset_type = db_session.query(AssetItem).filter_by(
            id=type_id,
            app_id=app_id
        ).first()

        if not asset_type:
            return jsonify({
                'success': False,
                'message': 'Asset type not found'
            }), 404

        # Check if any assets are linked to this type
        if asset_type.assets and len(asset_type.assets) > 0:
            return jsonify({
                'success': False,
                'message': f'Cannot delete asset type because it has {len(asset_type.assets)} asset(s) linked to it'
            }), 400

        db_session.delete(asset_type)
        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Asset type deleted successfully'
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error deleting asset type {type_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while deleting asset type'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/api/asset_types/bulk_delete', methods=['POST'])
@login_required
@role_required(['Admin', 'Supervisor'])
def api_asset_type_bulk_delete():
    """
    Bulk delete asset types
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()
        type_ids = data.get('type_ids', [])

        if not type_ids:
            return jsonify({
                'success': False,
                'message': 'No asset types selected'
            }), 400

        deleted_count = 0
        failed_ids = []

        for type_id in type_ids:
            asset_type = db_session.query(AssetItem).filter_by(
                id=type_id,
                app_id=app_id
            ).first()

            if asset_type:
                # Check if any assets are linked
                if asset_type.assets and len(asset_type.assets) > 0:
                    failed_ids.append(type_id)
                else:
                    db_session.delete(asset_type)
                    deleted_count += 1

        db_session.commit()

        message = f'Successfully deleted {deleted_count} asset type(s)'
        if failed_ids:
            message += f'. {len(failed_ids)} type(s) skipped because they have linked assets'

        return jsonify({
            'success': True,
            'message': message,
            'deleted_count': deleted_count,
            'failed_ids': failed_ids
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error bulk deleting asset types: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while deleting asset types'
        }), 500

    finally:
        db_session.close()
