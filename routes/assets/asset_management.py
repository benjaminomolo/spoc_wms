# app/routes/asset/asset_management.py
import decimal
import traceback
from datetime import datetime, timedelta
from decimal import Decimal

from flask import Blueprint, jsonify, render_template, flash, redirect, url_for, request, current_app, g
from flask_login import login_required, current_user
from sqlalchemy import func, or_, and_
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import joinedload

from decorators import role_required, require_permission, cached_route
from ai import get_base_currency, get_or_create_exchange_rate_id
from db import Session
from models import Company, Module, Currency, Project, ChartOfAccounts, PaymentMode, Category, Department, Employee, \
    PayrollPeriod, InventoryCategory, InventorySubCategory, InventoryLocation, InventoryItem, InventoryItemVariation, \
    InventoryItemAttribute, Brand, InventoryItemVariationLink, UnitOfMeasurement, Vendor, InventoryEntry, \
    InventoryEntryLineItem, ExchangeRate, InventoryTransactionDetail, CustomerGroup, ItemSellingPrice, DirectSaleItem, \
    SalesOrderItem, QuotationItem, SalesInvoiceItem, JournalEntry, UserLocationAssignment, Asset, AssetItem, \
    AssetMovement, AssetMovementLineItem, DepreciationRecord
import logging

from services.assets_helpers import process_asset_entries, process_edit_asset_entries
from services.inventory_helpers import handle_supplier_logic, \
    render_inventory_entry_form, \
    process_inventory_entries, reverse_inventory_entry, get_inventory_entry_with_details, \
    render_edit_inventory_entry_form, process_inventory_entries_for_edit, remove_inventory_journal_entries
from services.post_to_ledger_reversal import _delete_asset_movement_ledger_entries
from services.vendors_and_customers import handle_party_logic
from utils import ensure_default_location, generate_unique_lot, create_notification, empty_to_none, \
    validate_quantity_and_selling_price, validate_quantity_and_price, handle_batch_variation_update
from utils_and_helpers.amounts_utils import format_amount
from utils_and_helpers.cache_keys import stock_history_cache_key
from utils_and_helpers.cache_utils import on_inventory_data_changed, clear_stock_history_cache
from utils_and_helpers.date_time_utils import parse_date
from utils_and_helpers.numbers import _get_int_or_none
from . import asset_bp
from .post_to_ledger import post_asset_movement_to_ledger

logger = logging.getLogger(__name__)


@asset_bp.route('/asset_entry', methods=['GET', 'POST'])
@login_required
@role_required(['Admin', 'Supervisor', 'Contributor'])
def asset_entry():
    db_session = None
    try:
        db_session = Session()
        app_id = current_user.app_id

        # Get base currency FIRST
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            flash("Base currency not configured for this company", "error")
            return redirect(request.referrer)
        base_currency_id = base_currency_info["base_currency_id"]

        # Get company info
        company = db_session.query(Company).filter_by(id=app_id).scalar()
        if not company:
            flash("Company not found", "error")
            return redirect(request.referrer)

        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        if request.method == 'POST':
            try:

                # Extract form data
                movement_type = request.form.get('movement_type', '').strip()
                if not movement_type:
                    return jsonify({'status': 'error', 'message': 'Movement type is required'}), 400

                transaction_date_str = request.form.get('transaction_date')
                if not transaction_date_str:
                    return jsonify({'status': 'error', 'message': 'Transaction date is required'}), 400

                transaction_date = parse_date(transaction_date_str)

                # Get reference (can be auto-generated if not provided)
                reference = request.form.get('reference', '').strip()

                # Get project (optional)
                project_id = request.form.get('project_id')
                project_id = int(project_id) if project_id and project_id.isdigit() else None

                # Get accounting information
                payable_account_id = request.form.get('payable_account')
                payable_account_id = int(
                    payable_account_id) if payable_account_id and payable_account_id.isdigit() else None

                adjustment_account_id = request.form.get('adjustment_account')
                adjustment_account_id = int(
                    adjustment_account_id) if adjustment_account_id and adjustment_account_id.isdigit() else None

                sales_account_id = request.form.get('sales_account')
                sales_account_id = int(sales_account_id) if sales_account_id and sales_account_id.isdigit() else None

                # Get currency (default to base currency)
                currency_id = request.form.get('currency', base_currency_id)
                currency_id = int(currency_id) if currency_id and currency_id.isdigit() else base_currency_id

                # Process asset line items
                asset_item_ids = request.form.getlist('asset_item_id[]')
                if not asset_item_ids:
                    return jsonify({'status': 'error', 'message': 'At least one asset is required'}), 400

                # Get all arrays from form data
                asset_ids = request.form.getlist('asset_id[]')  # For existing assets
                asset_tags = request.form.getlist('asset_tag[]')  # For new assets
                serial_numbers = request.form.getlist('serial_number[]')
                purchase_prices = request.form.getlist('purchase_price[]')
                sale_notes = request.form.getlist('sale_notes[]')  # ✅ ADD THIS LINE

                # Only get current_values for donation_in, otherwise use purchase_price
                if movement_type == 'donation_in':
                    current_values = request.form.getlist('current_value[]')
                    # Validate donation_in has current values
                    for i, current_value in enumerate(current_values):
                        if not current_value or float(current_value) <= 0:
                            return jsonify({
                                'status': 'error',
                                'message': f'Fair market value is required for donation in line item {i + 1}'
                            }), 400
                else:
                    # For acquisition and opening_balance, purchase_price IS the current_value
                    # For other movements, current_value comes from the existing asset
                    current_values = purchase_prices.copy()

                sale_prices = request.form.getlist('sale_price[]')
                disposal_values = request.form.getlist('disposal_value[]')

                # ✅ CORRECT: Get party IDs as lists
                supplier_ids = request.form.getlist('supplier_id[]')  # For acquisitions
                customer_ids = request.form.getlist('customer_id[]')  # For sales

                # Also get the names if needed
                supplier_names = request.form.getlist('supplier_name[]')
                customer_names = request.form.getlist('customer_name[]')

                # ✅ ADDED: Get depreciation data from form
                depreciation_amounts = request.form.getlist('depreciation_amount[]')
                depreciation_notes = request.form.getlist('depreciation_notes[]')

                transfer_reasons = request.form.getlist('transfer_reason[]')
                # Process asset entries - get back Asset objects
                processed_assets = process_asset_entries(
                    db_session=db_session,
                    app_id=app_id,
                    movement_type=movement_type,
                    asset_item_ids=asset_item_ids,
                    asset_ids=asset_ids,
                    asset_tags=asset_tags,
                    serial_numbers=serial_numbers,
                    purchase_prices=purchase_prices,
                    current_values=current_values,
                    sale_prices=sale_prices,
                    disposal_values=disposal_values,
                    # ✅ DEPRECIATION PARAMETERS
                    depreciation_amounts=depreciation_amounts,
                    depreciation_notes=depreciation_notes,
                    sale_notes=sale_notes,
                    transfer_reasons=transfer_reasons,
                    transaction_date=transaction_date,
                    supplier_ids=supplier_ids,
                    customer_ids=customer_ids,
                    supplier_names=supplier_names,
                    customer_names=customer_names,
                    currency_id=currency_id,
                    base_currency_id=base_currency_id,
                    reference=reference,
                    project_id=project_id,
                    current_user_id=current_user.id,
                    payable_account_id=payable_account_id,
                    adjustment_account_id=adjustment_account_id,
                    sales_account_id=sales_account_id,
                    additional_data=request.form
                )

                # Get the asset_movement object
                asset_movement = processed_assets['asset_movement']


                db_session.commit()

                # Create notification
                create_notification(
                    db_session=db_session,
                    company_id=app_id,
                    user_id=current_user.id,
                    message=f"Asset {movement_type.replace('_', ' ')} recorded successfully",
                    type='success',
                    is_popup=False,
                    url=url_for('assets.movement_history')
                )
                return jsonify({
                    'status': 'success',
                    'message': f'Asset {movement_type.replace("_", " ")} recorded successfully!',
                    'reference': reference,
                    'created_count': processed_assets['created_count'],
                    'updated_count': processed_assets['updated_count']
                }), 200

            except ValueError as e:
                db_session.rollback()
                logger.error(f"Validation error in asset_entry: {str(e)}\n{traceback.format_exc()}")
                return jsonify({'status': 'error', 'message': f'Validation error: {str(e)}'}), 400

            except Exception as e:
                db_session.rollback()
                logger.error(f"Error processing asset entry: {str(e)}\n{traceback.format_exc()}")
                return jsonify({'status': 'error', 'message': f'Error processing asset entry: {str(e)}'}), 500
        else:
            # GET request - get movement_type from URL parameter
            movement_type = request.args.get('movement_type', '')

            # Validate movement_type is allowed
            allowed_movement_types = ['acquisition', 'donation_in', 'opening_balance', 'sale',
                                      'donation_out', 'disposal', 'transfer', 'assignment',
                                      'return', 'depreciation']

            if movement_type not in allowed_movement_types:
                movement_type = ''

            # Render form with movement_type
            return render_asset_entry_form(db_session, app_id, company, role, modules_data, movement_type=movement_type)

    except Exception as e:
        if db_session:
            db_session.rollback()
        logger.error(f"Unexpected error in asset_entry: {str(e)}\n{traceback.format_exc()}")
        flash(f'An unexpected error occurred: {str(e)}', 'error')
        return redirect(request.referrer)
    finally:
        if db_session:
            db_session.close()


def render_asset_entry_form(db_session, app_id, company, role, modules_data, movement_type=''):
    """Render the asset entry form with required data"""

    # Get asset items
    asset_items = db_session.query(AssetItem).filter_by(
        app_id=app_id,
        status='active'
    ).order_by(AssetItem.asset_name).all()

    # Get locations
    locations = db_session.query(InventoryLocation).filter_by(
        app_id=app_id
    ).order_by(InventoryLocation.location).all()

    # Get employees
    employees = db_session.query(Employee).filter_by(
        app_id=app_id,
        employment_status='active'
    ).order_by(Employee.first_name).all()

    # Get departments
    departments = db_session.query(Department).filter_by(
        app_id=app_id
    ).order_by(Department.department_name).all()

    # Get projects
    projects = db_session.query(Project).filter_by(
        app_id=app_id,
        is_active=True
    ).order_by(Project.name).all()

    # Get currencies
    currencies = db_session.query(Currency).filter_by(
        app_id=app_id
    ).order_by(Currency.currency_index).all()

    # Get base currency
    base_currency = db_session.query(Currency).filter_by(
        app_id=app_id,
        currency_index=1
    ).first()

    vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers']
    vendor_types = [v.lower() for v in vendor_types]

    suppliers = (
        db_session.query(Vendor)
        .filter(
            Vendor.app_id == app_id,
            func.lower(Vendor.vendor_type).in_(vendor_types)
        )
        .all()
    )

    customers = db_session.query(Vendor).filter(
        Vendor.app_id == app_id,
        Vendor.is_active == True,
        Vendor.is_one_time == False,
        func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
    ).all()

    suppliers = (
        db_session.query(Vendor)
        .filter(
            Vendor.app_id == app_id,
            func.lower(Vendor.vendor_type).in_(vendor_types)
        )
        .all()
    )

    return render_template(
        'assets/asset_entry.html',
        company=company,
        role=role,
        modules=modules_data,
        asset_items=asset_items,
        locations=locations,
        employees=employees,
        departments=departments,
        projects=projects,
        currencies=currencies,
        base_currency=base_currency,
        suppliers=suppliers,
        customers=customers,
        movement_type=movement_type
    )


def _load_edit_form_data(db_session, app_id, movement_type):
    """
    Load ALL dropdown data for edit form - matching the structure of render_asset_entry_form
    """
    data = {}

    # Get asset items
    data['asset_items'] = db_session.query(AssetItem).filter_by(
        app_id=app_id,
        status='active'
    ).order_by(AssetItem.asset_name).all()

    # Get locations
    data['locations'] = db_session.query(InventoryLocation).filter_by(
        app_id=app_id
    ).order_by(InventoryLocation.location).all()

    # Get employees
    data['employees'] = db_session.query(Employee).filter_by(
        app_id=app_id,
        employment_status='active'
    ).order_by(Employee.first_name).all()

    # Get departments
    data['departments'] = db_session.query(Department).filter_by(
        app_id=app_id
    ).order_by(Department.department_name).all()

    # Get projects
    data['projects'] = db_session.query(Project).filter_by(
        app_id=app_id,
        is_active=True
    ).order_by(Project.name).all()

    # Get currencies
    data['currencies'] = db_session.query(Currency).filter_by(
        app_id=app_id
    ).order_by(Currency.currency_index).all()

    # Get base currency
    data['base_currency'] = db_session.query(Currency).filter_by(
        app_id=app_id,
        currency_index=1
    ).first()

    # Get suppliers (Vendors with vendor/supplier type)
    vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers']
    data['suppliers'] = db_session.query(Vendor).filter(
        Vendor.app_id == app_id,
        func.lower(Vendor.vendor_type).in_(vendor_types)
    ).order_by(Vendor.vendor_name).all()

    # Get customers (Vendors with customer/client type)
    data['customers'] = db_session.query(Vendor).filter(
        Vendor.app_id == app_id,
        Vendor.is_active == True,
        Vendor.is_one_time == False,
        func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
    ).order_by(Vendor.vendor_name).all()

    return data


@asset_bp.route('/asset_edit/<int:movement_id>', methods=['GET', 'POST'])
@login_required
@role_required(['Admin', 'Supervisor', 'Contributor'])
def asset_edit(movement_id):
    """
    Edit an existing asset movement/transaction
    Only Admin and Supervisor and Contributor roles can edit
    """
    db_session = None
    try:
        db_session = Session()
        app_id = current_user.app_id

        # SINGLE QUERY: Get asset movement with all related data in one go
        asset_movement = db_session.query(AssetMovement).filter_by(
            id=movement_id,
            app_id=app_id
        ).first()

        if not asset_movement:
            flash("Asset movement not found", "error")
            return redirect(url_for('assets.movement_history'))

        # Get line items with asset data in ONE JOINED query
        # Get line items with asset data AND party name in ONE JOINED query
        line_items_with_assets = db_session.query(
            AssetMovementLineItem,
            Asset,
            Vendor.vendor_name.label('party_name')  # ADD THIS
        ).outerjoin(
            Asset, AssetMovementLineItem.asset_id == Asset.id
        ).outerjoin(
            Vendor, AssetMovementLineItem.party_id == Vendor.id  # ADD THIS
        ).filter(
            AssetMovementLineItem.asset_movement_id == movement_id,
            AssetMovementLineItem.app_id == app_id
        ).all()

        # Separate line items and assets for easier access
        line_items = []
        for item in line_items_with_assets:
            line_item = item[0]
            # Add party_name attribute to line_item
            line_item.party_name = item[2] if len(item) > 2 else None
            line_items.append(line_item)
        assets_dict = {item[0].asset_id: item[1] for item in line_items_with_assets if item[1]}

        # Get company info
        company = db_session.query(Company).filter_by(id=app_id).first()
        if not company:
            flash("Company not found", "error")
            return redirect(request.referrer)

        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        if request.method == 'POST':
            try:

                # Extract form data
                movement_type = asset_movement.movement_type
                transaction_date_str = request.form.get('transaction_date')
                if not transaction_date_str:
                    return jsonify({'status': 'error', 'message': 'Transaction date is required'}), 400

                transaction_date = parse_date(transaction_date_str)
                reference = request.form.get('reference', '').strip() or asset_movement.reference

                project_id = request.form.get('project_id')
                project_id = int(project_id) if project_id and project_id.isdigit() else asset_movement.project_id

                # Get accounting information
                payable_account_id = _get_int_or_none(request.form.get('payable_account'),
                                                      asset_movement.payable_account_id)
                adjustment_account_id = _get_int_or_none(request.form.get('adjustment_account'),
                                                         asset_movement.adjustment_account_id)
                sales_account_id = _get_int_or_none(request.form.get('sales_account'), asset_movement.sales_account_id)

                # Get base currency
                base_currency_info = get_base_currency(db_session, app_id)
                if not base_currency_info:
                    return jsonify({'status': 'error', 'message': 'Base currency not configured'}), 400
                base_currency_id = base_currency_info["base_currency_id"]

                currency_id = _get_int_or_none(request.form.get('currency'),
                                               asset_movement.currency_id or base_currency_id)

                # Process asset line items
                asset_item_ids = request.form.getlist('asset_item_id[]')
                if not asset_item_ids:
                    return jsonify({'status': 'error', 'message': 'At least one asset is required'}), 400

                # Get form data in bulk
                form_data = {
                    'asset_ids': request.form.getlist('asset_id[]'),
                    'asset_tags': empty_to_none(request.form.getlist('asset_tag[]')),
                    'serial_numbers': empty_to_none(request.form.getlist('serial_number[]')),
                    'purchase_prices': request.form.getlist('purchase_price[]'),
                    'sale_prices': request.form.getlist('sale_price[]'),
                    'disposal_values': request.form.getlist('disposal_value[]'),
                    'supplier_ids': request.form.getlist('supplier_id[]'),
                    'customer_ids': request.form.getlist('customer_id[]'),
                    'supplier_names': request.form.getlist('supplier_name[]'),
                    'customer_names': request.form.getlist('customer_name[]'),
                    'depreciation_amounts': request.form.getlist('depreciation_amount[]'),
                    'depreciation_notes': request.form.getlist('depreciation_notes[]'),
                    'sale_notes': request.form.getlist('sale_notes[]'),
                    'transfer_reasons': request.form.getlist('transfer_reason[]')
                }

                # Handle current values based on movement type
                if movement_type == 'donation_in':
                    current_values = request.form.getlist('current_value[]')
                    # Validate donation_in has current values
                    for i, current_value in enumerate(current_values):
                        if not current_value or float(current_value) <= 0:
                            return jsonify({
                                'status': 'error',
                                'message': f'Fair market value is required for donation in line item {i + 1}'
                            }), 400
                else:
                    current_values = form_data['purchase_prices'].copy()

                # BEGIN TRANSACTION
                try:
                    # 1. Delete all ledger entries linked to this asset movement
                    if movement_type in ['acquisition', 'sale', 'donation_in', 'donation_out', 'disposal',
                                         'opening_balance', 'depreciation']:
                        delete_success, delete_message = _delete_asset_movement_ledger_entries(
                            db_session=db_session,
                            asset_movement=asset_movement,
                            current_user=current_user
                        )

                        if not delete_success:
                            db_session.rollback()
                            return jsonify({'status': 'error',
                                            'message': f'Failed to delete ledger entries: {delete_message}'}), 400

                    # 2. Bulk delete existing line items
                    db_session.query(AssetMovementLineItem).filter(
                        AssetMovementLineItem.asset_movement_id == movement_id,
                        AssetMovementLineItem.app_id == app_id
                    ).delete(synchronize_session=False)

                    # ✅ ADD THIS: Bulk delete related depreciation records
                    if movement_type == 'depreciation':
                        deleted_count = db_session.query(DepreciationRecord).filter(
                            DepreciationRecord.asset_movement_line_item_id.in_(
                                db_session.query(AssetMovementLineItem.id).filter(
                                    AssetMovementLineItem.asset_movement_id == movement_id,
                                    AssetMovementLineItem.app_id == app_id
                                )
                            )
                        ).delete(synchronize_session=False)


                    db_session.flush()

                    db_session.flush()

                    # 3. Update the asset movement header
                    asset_movement.transaction_date = transaction_date
                    asset_movement.reference = reference
                    asset_movement.project_id = project_id
                    asset_movement.currency_id = currency_id
                    asset_movement.payable_account_id = payable_account_id
                    asset_movement.adjustment_account_id = adjustment_account_id
                    asset_movement.sales_account_id = sales_account_id
                    asset_movement.updated_by = current_user.id
                    asset_movement.updated_at = func.now()

                    db_session.flush()

                    # 4. Reprocess all asset entries
                    # 4. Reprocess all asset entries - USE EDIT-SPECIFIC FUNCTION
                    processed_assets = process_edit_asset_entries(
                        db_session=db_session,
                        app_id=app_id,
                        asset_movement=asset_movement,  # Pass the existing movement
                        movement_type=movement_type,
                        asset_item_ids=asset_item_ids,
                        asset_ids=form_data['asset_ids'],
                        asset_tags=form_data['asset_tags'],
                        serial_numbers=form_data['serial_numbers'],
                        purchase_prices=form_data['purchase_prices'],
                        current_values=current_values,
                        sale_prices=form_data['sale_prices'],
                        disposal_values=form_data['disposal_values'],
                        depreciation_amounts=form_data['depreciation_amounts'],
                        depreciation_notes=form_data['depreciation_notes'],
                        sale_notes=form_data['sale_notes'],
                        transfer_reasons=form_data['transfer_reasons'],
                        transaction_date=transaction_date,
                        supplier_ids=form_data['supplier_ids'],
                        customer_ids=form_data['customer_ids'],
                        supplier_names=form_data['supplier_names'],
                        customer_names=form_data['customer_names'],
                        currency_id=currency_id,
                        base_currency_id=base_currency_id,
                        reference=reference,
                        project_id=project_id,
                        current_user_id=current_user.id,
                        payable_account_id=payable_account_id,
                        adjustment_account_id=adjustment_account_id,
                        sales_account_id=sales_account_id,
                        additional_data=request.form,
                        original_line_items=line_items  # ADD THIS
                    )


                    db_session.commit()

                    # Create notification
                    create_notification(
                        db_session=db_session,
                        company_id=app_id,
                        user_id=current_user.id,
                        message=f"Asset {movement_type.replace('_', ' ')} updated successfully",
                        type='success',
                        is_popup=False,
                        url=url_for('assets.view_movement', movement_id=movement_id)
                    )

                    return jsonify({
                        'status': 'success',
                        'message': f'Asset {movement_type.replace("_", " ")} updated successfully!',
                        'reference': reference,
                        'created_count': processed_assets['created_count'],
                        'updated_count': processed_assets['updated_count'],
                        'redirect_url': url_for('assets.view_movement', movement_id=movement_id)
                    }), 200

                except Exception as e:
                    db_session.rollback()
                    logger.error(f"Error during edit reprocessing: {str(e)}\n{traceback.format_exc()}")
                    raise

            except ValueError as e:
                db_session.rollback()
                logger.error(f"Validation error in asset_edit: {str(e)}\n{traceback.format_exc()}")
                return jsonify({'status': 'error', 'message': f'Validation error: {str(e)}'}), 400

            except Exception as e:
                db_session.rollback()
                logger.error(f"Error editing asset movement: {str(e)}\n{traceback.format_exc()}")
                return jsonify({'status': 'error', 'message': f'Error updating asset movement: {str(e)}'}), 500

        else:
            # GET request - Load ALL dropdown data matching create form structure
            dropdown_data = _load_edit_form_data(db_session, app_id, asset_movement.movement_type)

            return render_template(
                'assets/asset_edit.html',  # Note: using same template structure
                company=company,
                role=role,
                modules=modules_data,
                asset_movement=asset_movement,
                line_items=line_items,
                assets_dict=assets_dict,
                **dropdown_data  # Unpack all dropdown data
            )

    except Exception as e:
        if db_session:
            db_session.rollback()
        logger.error(f"Unexpected error in asset_edit: {str(e)}\n{traceback.format_exc()}")
        flash(f'An unexpected error occurred: {str(e)}', 'error')
        return redirect(url_for('assets.movement_history'))
    finally:
        if db_session:
            db_session.close()


@asset_bp.route('/movement_history')
@login_required
def movement_history():
    """
    Render the asset movement history page
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()

        # Get base currency
        base_currency = db_session.query(Currency).filter_by(
            app_id=app_id,
            currency_index=1
        ).first()

        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Fetch filter data
        asset_items = db_session.query(AssetItem).filter_by(
            app_id=app_id,
            status='active'
        ).order_by(AssetItem.asset_name).all()

        projects = db_session.query(Project).filter_by(app_id=app_id).order_by(Project.name).all()

        # Get distinct references
        references = (
            db_session.query(AssetMovement.reference)
            .filter(AssetMovement.app_id == app_id)
            .filter(AssetMovement.reference.isnot(None))
            .distinct()
            .all()
        )
        references = [ref[0] for ref in references if ref[0]]

        # Get movement types for filter
        movement_types = [
            {'value': 'acquisition', 'label': 'Purchase/Acquisition'},
            {'value': 'donation_in', 'label': 'Donation Received'},
            {'value': 'opening_balance', 'label': 'Opening Balance'},
            {'value': 'sale', 'label': 'Sale'},
            {'value': 'donation_out', 'label': 'Donation Given'},
            {'value': 'disposal', 'label': 'Disposal/Write-off'},
            {'value': 'transfer', 'label': 'Transfer'},
            {'value': 'assignment', 'label': 'Assignment'},
            {'value': 'return', 'label': 'Return'},
            {'value': 'depreciation', 'label': 'Depreciation'}
        ]

        return render_template(
            'assets/asset_movement_history.html',
            company=company,
            modules=modules_data,
            role=role,
            base_currency=base_currency,
            asset_items=asset_items,
            references=references,
            projects=projects,
            movement_types=movement_types
        )

    except Exception as e:
        logger.error(f"Error rendering asset movement history page: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the movement history page', 'error')
        return redirect(url_for('main.dashboard'))

    finally:
        db_session.close()


@asset_bp.route('/api/movement_history', methods=['GET'])
@login_required
def api_movement_history():
    """
    Return JSON data of asset movement history with filtering and pagination
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get pagination and filter parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        asset_item_id = request.args.get('asset_item_id', type=int)
        reference = request.args.get('reference', '')
        movement_type = request.args.get('movement_type', '')
        project_id = request.args.get('project_id', type=int)
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        search = request.args.get('search', '')

        # Get base currency information
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({
                'success': False,
                'message': 'Base currency not configured for this company'
            }), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency_code = base_currency_info["base_currency"]

        # Build base query
        query = db_session.query(AssetMovement).filter(
            AssetMovement.app_id == app_id
        )

        # Apply filters
        if asset_item_id:
            # Filter by asset item through line items and assets
            query = query.join(
                AssetMovementLineItem,
                AssetMovement.id == AssetMovementLineItem.asset_movement_id
            ).join(
                Asset,
                AssetMovementLineItem.asset_id == Asset.id
            ).filter(
                Asset.asset_item_id == asset_item_id
            ).distinct()

        if reference:
            query = query.filter(AssetMovement.reference.ilike(f'%{reference}%'))

        if movement_type:
            query = query.filter(AssetMovement.movement_type == movement_type)

        if project_id:
            query = query.filter(AssetMovement.project_id == project_id)

        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
                query = query.filter(func.date(AssetMovement.transaction_date) >= start_date_obj)
            except ValueError:
                pass

        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
                query = query.filter(func.date(AssetMovement.transaction_date) <= end_date_obj)
            except ValueError:
                pass

        if search:
            search_term = f'%{search}%'
            query = query.join(
                AssetMovementLineItem,
                AssetMovement.id == AssetMovementLineItem.asset_movement_id
            ).outerjoin(
                Asset,
                AssetMovementLineItem.asset_id == Asset.id
            ).outerjoin(
                Vendor,  # Your party table is Vendor
                AssetMovementLineItem.party_id == Vendor.id
            ).filter(
                or_(
                    AssetMovement.reference.ilike(search_term),
                    Asset.asset_tag.ilike(search_term),
                    Asset.serial_number.ilike(search_term),
                    Vendor.vendor_name.ilike(search_term)
                )
            ).distinct()

        # Get total count
        total_items = query.count()

        # Calculate pagination
        total_pages = (total_items + per_page - 1) // per_page

        # Get paginated results - newest first
        movements = query.order_by(
            AssetMovement.transaction_date.desc(),
            AssetMovement.id.desc()
        ).offset((page - 1) * per_page).limit(per_page).all()

        # Get line items for each movement with all necessary joins
        movement_ids = [m.id for m in movements]

        # Get all locations and departments for from fields in a separate query to avoid too many joins
        locations_dict = {}
        departments_dict = {}
        if movement_ids:
            all_location_ids = set()
            all_department_ids = set()

            # First get all line items to collect location/department IDs
            line_items_for_locations = db_session.query(AssetMovementLineItem).filter(
                AssetMovementLineItem.asset_movement_id.in_(movement_ids),
                AssetMovementLineItem.app_id == app_id
            ).all()

            for li in line_items_for_locations:
                if li.from_location_id:
                    all_location_ids.add(li.from_location_id)
                if li.to_location_id:
                    all_location_ids.add(li.to_location_id)
                if li.from_department_id:
                    all_department_ids.add(li.from_department_id)
                if li.to_department_id:
                    all_department_ids.add(li.to_department_id)

            # Fetch all locations and departments in bulk
            if all_location_ids:
                locations = db_session.query(InventoryLocation).filter(
                    InventoryLocation.id.in_(all_location_ids)
                ).all()
                locations_dict = {loc.id: loc.location for loc in locations}

            if all_department_ids:
                departments = db_session.query(Department).filter(
                    Department.id.in_(all_department_ids)
                ).all()
                departments_dict = {dept.id: dept.department_name for dept in departments}

        # Main line items query
        line_items_query = db_session.query(
            AssetMovementLineItem,
            Asset,
            AssetItem,
            Vendor,
            Employee
        ).outerjoin(
            Asset, AssetMovementLineItem.asset_id == Asset.id
        ).outerjoin(
            AssetItem, Asset.asset_item_id == AssetItem.id
        ).outerjoin(
            Vendor, AssetMovementLineItem.party_id == Vendor.id
        ).outerjoin(
            Employee, AssetMovementLineItem.assigned_to_id == Employee.id
        ).filter(
            AssetMovementLineItem.asset_movement_id.in_(movement_ids),
            AssetMovementLineItem.app_id == app_id
        ).all()

        # Group line items by movement_id
        line_items_by_movement = {}
        for line_item, asset, asset_item, vendor, employee in line_items_query:
            movement_id = line_item.asset_movement_id
            if movement_id not in line_items_by_movement:
                line_items_by_movement[movement_id] = []

            line_items_by_movement[movement_id].append({
                'id': line_item.id,
                'asset_id': line_item.asset_id,
                'asset_tag': asset.asset_tag if asset else None,
                'serial_number': asset.serial_number if asset else None,
                'asset_item_id': asset.asset_item_id if asset else None,
                'asset_item_name': asset_item.asset_name if asset_item else None,
                'asset_item_code': asset_item.asset_code if asset_item else None,
                'party_id': line_item.party_id,
                'party_name': vendor.vendor_name if vendor else None,
                'transaction_value': float(line_item.transaction_value) if line_item.transaction_value else 0,
                'from_location_id': line_item.from_location_id,
                'from_location_name': locations_dict.get(
                    line_item.from_location_id) if line_item.from_location_id else None,
                'from_department_id': line_item.from_department_id,
                'from_department_name': departments_dict.get(
                    line_item.from_department_id) if line_item.from_department_id else None,
                'to_location_id': line_item.to_location_id,
                'to_location_name': locations_dict.get(line_item.to_location_id) if line_item.to_location_id else None,
                'to_department_id': line_item.to_department_id,
                'to_department_name': departments_dict.get(
                    line_item.to_department_id) if line_item.to_department_id else None,
                'assigned_to_id': line_item.assigned_to_id,
                'assigned_to_name': f"{employee.first_name} {employee.last_name}".strip() if employee else None,
                'line_notes': line_item.line_notes
            })

        # Serialize movements
        movements_data = []
        total_value_base = Decimal('0.00')

        for movement in movements:
            # Get line items for this movement
            line_items = line_items_by_movement.get(movement.id, [])

            # Calculate total transaction value
            total_value = Decimal('0.00')
            for item in line_items:
                total_value += Decimal(str(item['transaction_value']))

            # Get currency info
            currency = movement.currency
            currency_code = currency.user_currency if currency else base_currency_code

            # Convert to base currency if needed
            if movement.currency_id and movement.currency_id != base_currency_id:
                # You can implement exchange rate lookup here if needed
                exchange_rate = 1.0
                total_value_base_converted = total_value * Decimal(str(exchange_rate))
            else:
                total_value_base_converted = total_value
                exchange_rate = 1.0

            total_value_base += total_value_base_converted

            # Get creator name - using 'creator' relationship from your model
            creator_name = movement.creator.name if movement.creator else 'Unknown'

            # Your AssetMovement model doesn't have updated_at or updated_by fields
            # So removing those entirely

            movements_data.append({
                'id': movement.id,
                'movement_type': movement.movement_type,
                'movement_type_label': movement.movement_type.replace('_', ' ').title(),
                'transaction_date': movement.transaction_date.strftime('%Y-%m-%d'),
                'reference': movement.reference or '-',
                'project_id': movement.project_id,
                'project_name': movement.project.name if movement.project else None,
                'currency_id': movement.currency_id,
                'currency_code': currency_code,
                'exchange_rate': float(exchange_rate),
                'total_value': float(total_value),
                'total_value_base': float(total_value_base_converted),
                'status': movement.status,
                'line_items': line_items,
                'line_items_count': len(line_items),
                'created_by': creator_name,
                'created_at': movement.created_at.strftime('%Y-%m-%d %H:%M:%S') if movement.created_at else None,
                # Removed updated_by and updated_at since they don't exist in your AssetMovement model
            })

        return jsonify({
            'success': True,
            'movements': movements_data,
            'summary': {
                'total_value_base': float(total_value_base),
                'base_currency_id': base_currency_id,
                'base_currency_code': base_currency_code,
                'total_movements': total_items
            },
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
        logger.error(f"Error in api_movement_history: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching asset movement history'
        }), 500

    finally:
        db_session.close()

@asset_bp.route('/view_movement/<int:movement_id>')
@login_required
def view_movement(movement_id):
    """
    View a single asset movement with all details
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get the asset movement
        movement = db_session.query(AssetMovement).filter_by(
            id=movement_id,
            app_id=app_id
        ).first()

        if not movement:
            flash("Asset movement not found", "error")
            return redirect(url_for('assets.movement_history'))

        # Get company info
        company = db_session.query(Company).filter_by(id=app_id).first()

        # Get base currency
        base_currency = db_session.query(Currency).filter_by(
            app_id=app_id,
            currency_index=1
        ).first()

        role = current_user.role
        modules_data = [mod.module_name for mod in
                       db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        return render_template(
            'assets/view_movement.html',
            company=company,
            modules=modules_data,
            role=role,
            base_currency=base_currency,
            movement=movement,
            now=datetime.now()
        )

    except Exception as e:
        logger.error(f"Error viewing movement {movement_id}: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the movement details', 'error')
        return redirect(url_for('assets.movement_history'))

    finally:
        db_session.close()


@asset_bp.route('/api/movement/<int:movement_id>/details', methods=['GET'])
@login_required
def api_movement_details(movement_id):
    """
    Return JSON data for a single asset movement with all related details
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get movement with all relationships
        movement = db_session.query(AssetMovement).filter_by(
            id=movement_id,
            app_id=app_id
        ).first()

        if not movement:
            return jsonify({
                'success': False,
                'message': 'Asset movement not found'
            }), 404

        # Get base currency
        base_currency_info = get_base_currency(db_session, app_id)
        base_currency_code = base_currency_info["base_currency"] if base_currency_info else None

        # Get all locations and departments for lookup
        locations = {loc.id: loc.location for loc in db_session.query(InventoryLocation).filter_by(app_id=app_id).all()}
        departments = {dept.id: dept.department_name for dept in db_session.query(Department).filter_by(app_id=app_id).all()}

        # Get line items with all related data
        line_items = db_session.query(
            AssetMovementLineItem,
            Asset,
            AssetItem,
            Vendor,
            Employee,
            InventoryLocation,
            Department
        ).outerjoin(
            Asset, AssetMovementLineItem.asset_id == Asset.id
        ).outerjoin(
            AssetItem, Asset.asset_item_id == AssetItem.id
        ).outerjoin(
            Vendor, AssetMovementLineItem.party_id == Vendor.id
        ).outerjoin(
            Employee, AssetMovementLineItem.assigned_to_id == Employee.id
        ).outerjoin(
            InventoryLocation, AssetMovementLineItem.to_location_id == InventoryLocation.id
        ).outerjoin(
            Department, AssetMovementLineItem.to_department_id == Department.id
        ).filter(
            AssetMovementLineItem.asset_movement_id == movement_id,
            AssetMovementLineItem.app_id == app_id
        ).all()

        # Format line items
        line_items_data = []
        total_value = Decimal('0.00')

        for line_item, asset, asset_item, vendor, employee, to_location, to_department in line_items:
            # Get from location/department names
            from_location_name = locations.get(line_item.from_location_id) if line_item.from_location_id else None
            from_department_name = departments.get(line_item.from_department_id) if line_item.from_department_id else None

            # Get to location/department names (already joined)
            to_location_name = to_location.location if to_location else None
            to_department_name = to_department.department_name if to_department else None

            # Format employee name
            employee_name = None
            if employee:
                employee_name = f"{employee.first_name} {employee.last_name}".strip()

            line_total = line_item.transaction_value or Decimal('0.00')
            total_value += line_total

            line_items_data.append({
                'id': line_item.id,
                'asset_id': line_item.asset_id,
                'asset_tag': asset.asset_tag if asset else None,
                'serial_number': asset.serial_number if asset else None,
                'asset_item_id': asset.asset_item_id if asset else None,
                'asset_item_name': asset_item.asset_name if asset_item else None,
                'asset_item_code': asset_item.asset_code if asset_item else None,
                'asset_description': asset_item.asset_description if asset_item else None,
                'asset_status': asset.status if asset else None,
                'asset_condition': asset.condition if asset else None,
                'purchase_date': asset.purchase_date.strftime('%Y-%m-%d') if asset and asset.purchase_date else None,
                'purchase_price': float(asset.purchase_price) if asset and asset.purchase_price else 0,
                'current_value': float(asset.current_value) if asset and asset.current_value else 0,
                'party_id': line_item.party_id,
                'party_name': vendor.vendor_name if vendor else None,
                'party_type': vendor.vendor_type if vendor else None,
                'transaction_value': float(line_item.transaction_value) if line_item.transaction_value else 0,
                'from_location_id': line_item.from_location_id,
                'from_location_name': from_location_name,
                'from_department_id': line_item.from_department_id,
                'from_department_name': from_department_name,
                'to_location_id': line_item.to_location_id,
                'to_location_name': to_location_name,
                'to_department_id': line_item.to_department_id,
                'to_department_name': to_department_name,
                'assigned_to_id': line_item.assigned_to_id,
                'assigned_to_name': employee_name,
                'line_notes': line_item.line_notes
            })

        # Get currency info
        currency = movement.currency
        currency_code = currency.user_currency if currency else base_currency_code

        # Convert total to base currency if needed
        total_value_base = total_value
        exchange_rate = 1.0
        if movement.currency_id and movement.currency_id != base_currency_info.get("base_currency_id"):
            # You can implement exchange rate lookup here
            pass

        # Format movement data
        movement_data = {
            'id': movement.id,
            'movement_type': movement.movement_type,
            'movement_type_label': movement.movement_type.replace('_', ' ').title(),
            'transaction_date': movement.transaction_date.strftime('%Y-%m-%d'),
            'reference': movement.reference or '-',
            'project_id': movement.project_id,
            'project_name': movement.project.name if movement.project else None,
            'currency_id': movement.currency_id,
            'currency_code': currency_code,
            'exchange_rate': exchange_rate,
            'total_value': float(total_value),
            'total_value_base': float(total_value_base),
            'status': movement.status,
            'line_items': line_items_data,
            'line_items_count': len(line_items_data),
            'created_by': movement.creator.name if movement.creator else 'Unknown',
            'created_at': movement.created_at.strftime('%Y-%m-%d %H:%M:%S') if movement.created_at else None
        }

        return jsonify({
            'success': True,
            'movement': movement_data
        })

    except Exception as e:
        logger.error(f"Error in api_movement_details: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching movement details'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/api/bulk_delete_movements', methods=['POST'])
@login_required
@role_required(['Admin', 'Supervisor'])
def bulk_delete_movements():
    """
    Bulk delete asset movements and their line items
    Also updates asset status back to previous state
    Only Admin and Supervisor can delete
    """
    db_session = None
    try:
        data = request.get_json()
        movement_ids = data.get('movement_ids', [])

        if not movement_ids:
            return jsonify({'success': False, 'message': 'No movements selected for deletion'}), 400

        db_session = Session()
        app_id = current_user.app_id

        # Check which movements can be deleted
        disallowed_movements = []
        valid_movements = []

        for movement_id in movement_ids:
            asset_movement = db_session.query(AssetMovement).filter_by(
                id=movement_id,
                app_id=app_id
            ).first()

            if not asset_movement:
                continue

            # Check if movement is posted to ledger
            if asset_movement.is_posted_to_ledger:
                disallowed_movements.append({
                    'id': movement_id,
                    'reference': asset_movement.reference or f'ID: {movement_id}',
                    'reason': 'Already posted to ledger'
                })
            else:
                valid_movements.append(movement_id)

        if not valid_movements:
            return jsonify({
                'success': False,
                'message': 'No valid movements selected for deletion',
                'disallowed': disallowed_movements
            }), 400

        # Delete valid movements and update asset statuses
        deleted_count = 0
        updated_assets = []

        for movement_id in valid_movements:
            asset_movement = db_session.query(AssetMovement).filter_by(
                id=movement_id,
                app_id=app_id
            ).first()

            if asset_movement:
                # Get all line items before deleting
                line_items = db_session.query(AssetMovementLineItem).filter(
                    AssetMovementLineItem.asset_movement_id == movement_id,
                    AssetMovementLineItem.app_id == app_id
                ).all()

                # Update asset statuses based on movement type
                for line_item in line_items:
                    asset = db_session.query(Asset).filter_by(
                        id=line_item.asset_id,
                        app_id=app_id
                    ).first()

                    if asset:
                        # Reverse the movement based on type
                        if asset_movement.movement_type == 'assignment':
                            # Reverse assignment - set back to in_stock
                            asset.status = 'in_stock'
                            asset.assigned_to_id = None
                            updated_assets.append({
                                'asset_id': asset.id,
                                'asset_tag': asset.asset_tag,
                                'new_status': 'in_stock'
                            })

                        elif asset_movement.movement_type == 'return':
                            # Reverse return - set back to assigned (but we don't know who)
                            # Better to just mark as in_stock
                            asset.status = 'in_stock'
                            asset.assigned_to_id = None
                            updated_assets.append({
                                'asset_id': asset.id,
                                'asset_tag': asset.asset_tag,
                                'new_status': 'in_stock'
                            })

                        elif asset_movement.movement_type == 'transfer':
                            # Reverse transfer - send back to original location
                            if line_item.from_location_id:
                                asset.location_id = line_item.from_location_id
                                updated_assets.append({
                                    'asset_id': asset.id,
                                    'asset_tag': asset.asset_tag,
                                    'new_location': line_item.from_location_id
                                })

                        elif asset_movement.movement_type == 'acquisition':
                            # Deleting acquisition - asset should be deactivated or removed
                            asset.status = 'deleted'
                            updated_assets.append({
                                'asset_id': asset.id,
                                'asset_tag': asset.asset_tag,
                                'new_status': 'deleted'
                            })

                        elif asset_movement.movement_type == 'sale' or asset_movement.movement_type == 'disposal':
                            # Deleting sale/disposal - bring asset back
                            asset.status = 'in_stock'
                            updated_assets.append({
                                'asset_id': asset.id,
                                'asset_tag': asset.asset_tag,
                                'new_status': 'in_stock'
                            })

                        # For depreciation, opening_balance, donation_in/out - just keep as is

                        db_session.flush()

                # Delete line items
                db_session.query(AssetMovementLineItem).filter(
                    AssetMovementLineItem.asset_movement_id == movement_id,
                    AssetMovementLineItem.app_id == app_id
                ).delete(synchronize_session=False)

                # Delete depreciation records if any
                db_session.query(DepreciationRecord).filter(
                    DepreciationRecord.asset_movement_line_item_id.in_(
                        db_session.query(AssetMovementLineItem.id).filter(
                            AssetMovementLineItem.asset_movement_id == movement_id,
                            AssetMovementLineItem.app_id == app_id
                        )
                    )
                ).delete(synchronize_session=False)

                # Delete the movement header
                db_session.delete(asset_movement)
                deleted_count += 1

        db_session.commit()

        message = f'Successfully deleted {deleted_count} movement(s)'
        if updated_assets:
            message += f' and updated {len(updated_assets)} asset(s)'
        if disallowed_movements:
            message += f'. {len(disallowed_movements)} movement(s) could not be deleted (posted to ledger)'

        return jsonify({
            'success': True,
            'message': message,
            'deleted_count': deleted_count,
            'updated_assets': len(updated_assets),
            'disallowed_count': len(disallowed_movements),
            'disallowed': disallowed_movements
        }), 200

    except Exception as e:
        if db_session:
            db_session.rollback()
        logger.error(f"Error in bulk_delete_movements: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': f'Error deleting movements: {str(e)}'}), 500
    finally:
        if db_session:
            db_session.close()
