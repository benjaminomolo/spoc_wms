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
    render_edit_inventory_entry_form, process_inventory_entries_for_edit, remove_inventory_journal_entries, \
    get_user_accessible_locations
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

                handled_by = request.form.get('handled_by')

                # Get location
                #  - ONLY for receiving and internal movements
                out_movements = ['sale', 'donation_out', 'disposal']
                if movement_type in out_movements:
                    # Out movements don't use header location
                    location_id = None
                else:
                    location_id = request.form.get('location_id')
                    if location_id and location_id.isdigit():
                        location_id = int(location_id)
                    else:
                        location_id = None

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
                project_ids = request.form.getlist('project_id[]')

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

                print(f"DEBUG process_asset_entries: movement_type={movement_type}, location_id={location_id}")

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
                    project_ids=project_ids,
                    location_id=location_id,
                    current_user_id=current_user.id,
                    payable_account_id=payable_account_id,
                    adjustment_account_id=adjustment_account_id,
                    sales_account_id=sales_account_id,
                    handled_by=handled_by,
                    additional_data=request.form
                )

                # Get the asset_movement object
                asset_movement = processed_assets['asset_movement']

                # ===== PROCESS ATTACHMENTS =====
                from services.inventory_helpers import process_attachments
                process_attachments(request, 'asset_movement', asset_movement.id, current_user.id, db_session)
                # ===== END ATTACHMENTS PROCESSING =====
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

    locations = get_user_accessible_locations(current_user.id, app_id)

    # Get employees
    employees = db_session.query(Employee).filter_by(
        app_id=app_id,
        is_active=True
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

    vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers', 'customer', 'customers']
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
        movement_type=movement_type,

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
    data['locations'] = get_user_accessible_locations(current_user.id, app_id)

    # Get employees
    data['employees'] = db_session.query(Employee).filter_by(
        app_id=app_id,
        is_active=True
    ).order_by(Employee.first_name).all()

    # Get departments
    data['departments'] = db_session.query(Department).filter_by(
        app_id=app_id
    ).order_by(Department.department_name).all()

    data['employees'] = db_session.query(Employee).filter_by(app_id=app_id).all()

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
                handled_by = request.form.get('handled_by')
                # Get location
                location_id = request.form.get('location_id')
                if location_id and location_id.isdigit():
                    location_id = int(location_id)
                else:
                    location_id = None
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

                project_ids = request.form.getlist('project_id[]')

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
                    'transfer_reasons': request.form.getlist('transfer_reason[]'),
                    'project_ids': project_ids
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
                    asset_movement.handled_by = handled_by
                    asset_movement.location_id = location_id

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
                        project_ids=form_data['project_ids'],
                        location_id=location_id,
                        current_user_id=current_user.id,
                        payable_account_id=payable_account_id,
                        adjustment_account_id=adjustment_account_id,
                        sales_account_id=sales_account_id,
                        additional_data=request.form,
                        original_line_items=line_items  # ADD THIS
                    )

                    # Process attachments
                    from services.inventory_helpers import process_attachments
                    process_attachments(request, 'asset_movement', movement_id, current_user.id, db_session)

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
            # Get existing attachments for this asset movement
            from models import Attachment
            attachments = db_session.query(Attachment).filter_by(
                record_type='asset_movement',
                record_id=movement_id
            ).all()
            return render_template(
                'assets/asset_edit.html',  # Note: using same template structure
                company=company,
                role=role,
                modules=modules_data,
                asset_movement=asset_movement,
                line_items=line_items,
                assets_dict=assets_dict,
                attachments=attachments,
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

        # Get user's accessible locations (for filter dropdown)
        user_locations = get_user_accessible_locations(current_user.id, app_id)

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
            {'value': 'sale', 'label': 'Dispatch'},
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
            movement_types=movement_types,
            locations=user_locations  # Pass user's accessible locations
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
    Returns ONLY line items that match the filters, not entire movements
    """
    db_session = Session()
    app_id = current_user.app_id
    user_id = current_user.id

    try:
        # Get pagination and filter parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        asset_item_id = request.args.get('asset_item_id', type=int)
        reference = request.args.get('reference', '')
        movement_type = request.args.get('movement_type', '')
        project_id = request.args.get('project_id', type=int)
        location_id = request.args.get('location_id', type=int)
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')
        search = request.args.get('search', '')

        # Get user's accessible locations
        user_locations = get_user_accessible_locations(user_id, app_id)
        user_location_ids = [loc.id for loc in user_locations] if user_locations else []

        # Get base currency information
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({
                'success': False,
                'message': 'Base currency not configured for this company'
            }), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency_code = base_currency_info["base_currency"]

        # ===== FIX: Query LINE ITEMS directly, not movements =====
        query = db_session.query(AssetMovementLineItem).join(
            AssetMovement, AssetMovementLineItem.asset_movement_id == AssetMovement.id
        ).filter(
            AssetMovement.app_id == app_id
        )

        # ===== LOCATION PERMISSION FILTER =====
        if user_location_ids:
            location_filter = or_(
                AssetMovement.location_id.in_(user_location_ids),
                AssetMovementLineItem.from_location_id.in_(user_location_ids),
                AssetMovementLineItem.to_location_id.in_(user_location_ids)
            )
            query = query.filter(location_filter)
        else:
            return jsonify({
                'success': True,
                'movements': [],
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_pages': 0,
                    'total_items': 0,
                    'has_next': False,
                    'has_prev': False
                }
            })

        # Apply location filter if specified
        if location_id:
            if location_id not in user_location_ids:
                return jsonify({
                    'success': False,
                    'message': 'You do not have permission to access this location'
                }), 403
            query = query.filter(
                or_(
                    AssetMovement.location_id == location_id,
                    AssetMovementLineItem.from_location_id == location_id,
                    AssetMovementLineItem.to_location_id == location_id
                )
            )

        # ===== PROJECT FILTER - FILTERS LINE ITEMS DIRECTLY =====
        if project_id:
            query = query.filter(AssetMovementLineItem.project_id == project_id)

        # Apply other filters
        if asset_item_id:
            query = query.join(Asset, AssetMovementLineItem.asset_id == Asset.id).filter(
                Asset.asset_item_id == asset_item_id
            )

        if reference:
            query = query.filter(AssetMovement.reference.ilike(f'%{reference}%'))

        if movement_type:
            query = query.filter(AssetMovement.movement_type == movement_type)

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
            query = query.outerjoin(Asset, AssetMovementLineItem.asset_id == Asset.id).outerjoin(
                Vendor, AssetMovementLineItem.party_id == Vendor.id
            ).filter(
                or_(
                    AssetMovement.reference.ilike(search_term),
                    Asset.asset_tag.ilike(search_term),
                    Asset.serial_number.ilike(search_term),
                    Vendor.vendor_name.ilike(search_term)
                )
            )

        # Get total count of LINE ITEMS (not movements)
        total_items = query.count()

        # Calculate pagination
        if total_items == 0:
            total_pages = 0
        else:
            total_pages = (total_items + per_page - 1) // per_page

        # Get paginated line items
        line_items = query.order_by(
            AssetMovement.transaction_date.desc(),
            AssetMovement.id.desc(),
            AssetMovementLineItem.id
        ).offset((page - 1) * per_page).limit(per_page).all()

        if not line_items:
            return jsonify({
                'success': True,
                'movements': [],
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_pages': 0,
                    'total_items': 0,
                    'has_next': False,
                    'has_prev': False
                }
            })

        # Get unique movement IDs from the filtered line items
        movement_ids = list(set([li.asset_movement_id for li in line_items]))

        # Fetch all movements in bulk
        movements_dict = {}
        for movement in db_session.query(AssetMovement).filter(AssetMovement.id.in_(movement_ids)).all():
            movements_dict[movement.id] = movement

        # Collect all location IDs
        all_location_ids = set()
        for movement in movements_dict.values():
            if movement.location_id:
                all_location_ids.add(movement.location_id)
        for li in line_items:
            if li.from_location_id:
                all_location_ids.add(li.from_location_id)
            if li.to_location_id:
                all_location_ids.add(li.to_location_id)

        # Fetch locations
        locations_dict = {}
        if all_location_ids:
            locations = db_session.query(InventoryLocation).filter(
                InventoryLocation.id.in_(all_location_ids)
            ).all()
            locations_dict = {loc.id: loc.location for loc in locations}

        # Collect all project/department IDs
        all_project_ids = set([li.project_id for li in line_items if li.project_id])

        # Fetch projects
        projects_dict = {}
        if all_project_ids:
            projects = db_session.query(Project).filter(Project.id.in_(all_project_ids)).all()
            projects_dict = {p.id: p.name for p in projects}

        # Get asset IDs
        asset_ids = list(set([li.asset_id for li in line_items if li.asset_id]))
        assets_dict = {}
        if asset_ids:
            assets = db_session.query(Asset).filter(Asset.id.in_(asset_ids)).all()
            assets_dict = {a.id: a for a in assets}

        # Get asset item IDs
        asset_item_ids = list(set([a.asset_item_id for a in assets_dict.values() if a.asset_item_id]))
        asset_items_dict = {}
        if asset_item_ids:
            asset_items = db_session.query(AssetItem).filter(AssetItem.id.in_(asset_item_ids)).all()
            asset_items_dict = {ai.id: ai for ai in asset_items}

        # Get party IDs
        party_ids = list(set([li.party_id for li in line_items if li.party_id]))
        parties_dict = {}
        if party_ids:
            parties = db_session.query(Vendor).filter(Vendor.id.in_(party_ids)).all()
            parties_dict = {p.id: p for p in parties}

        # Get employee IDs
        employee_ids = list(set([li.assigned_to_id for li in line_items if li.assigned_to_id]))
        employees_dict = {}
        if employee_ids:
            employees = db_session.query(Employee).filter(Employee.id.in_(employee_ids)).all()
            employees_dict = {e.id: e for e in employees}

        # Build response - Group by movement for frontend structure
        movements_by_id = {}
        for line_item in line_items:
            movement = movements_dict.get(line_item.asset_movement_id)
            if not movement:
                continue

            if movement.id not in movements_by_id:
                movements_by_id[movement.id] = {
                    'id': movement.id,
                    'movement_type': movement.movement_type,
                    'transaction_date': movement.transaction_date.strftime('%Y-%m-%d'),
                    'reference': movement.reference or '-',
                    'handled_by': movement.handled_by_employee.first_name + ' ' + movement.handled_by_employee.last_name if movement.handled_by_employee else None,
                    'movement_location': locations_dict.get(movement.location_id),
                    'line_items': []
                }

            asset = assets_dict.get(line_item.asset_id)
            asset_item = asset_items_dict.get(asset.asset_item_id) if asset else None
            party = parties_dict.get(line_item.party_id)
            employee = employees_dict.get(line_item.assigned_to_id)

            movements_by_id[movement.id]['line_items'].append({
                'id': line_item.id,
                'asset_tag': asset.asset_tag if asset else None,
                'serial_number': asset.serial_number if asset else None,
                'asset_item_name': asset_item.asset_name if asset_item else None,
                'party_name': party.vendor_name if party else None,
                'from_location_name': locations_dict.get(line_item.from_location_id),
                'to_location_name': locations_dict.get(line_item.to_location_id),
                'assigned_to_name': f"{employee.first_name} {employee.last_name}".strip() if employee else None,
                'project_name': projects_dict.get(line_item.project_id),  # Department name from project
                'project_id': line_item.project_id,
            })

        movements_data = list(movements_by_id.values())

        return jsonify({
            'success': True,
            'movements': movements_data,
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
        departments = {dept.id: dept.department_name for dept in
                       db_session.query(Department).filter_by(app_id=app_id).all()}

        # Get line items with all related data - ADDED Project join
        line_items = db_session.query(
            AssetMovementLineItem,
            Asset,
            AssetItem,
            Vendor,
            Employee,
            InventoryLocation,
            Department,
            Project  # ADDED
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
        ).outerjoin(
            Project, AssetMovementLineItem.project_id == Project.id  # ADDED
        ).filter(
            AssetMovementLineItem.asset_movement_id == movement_id,
            AssetMovementLineItem.app_id == app_id
        ).all()

        # Format line items
        line_items_data = []
        total_value = Decimal('0.00')

        for line_item, asset, asset_item, vendor, employee, to_location, to_department, project in line_items:
            # Get from location/department names
            from_location_name = locations.get(line_item.from_location_id) if line_item.from_location_id else None
            from_department_name = departments.get(
                line_item.from_department_id) if line_item.from_department_id else None

            # Get to location/department names (already joined)
            to_location_name = to_location.location if to_location else None
            to_department_name = to_department.department_name if to_department else None

            # Format employee name
            employee_name = None
            if employee:
                employee_name = f"{employee.first_name} {employee.last_name}".strip()

            # Get project/department name
            project_name = project.name if project else None

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
                'project_id': line_item.project_id,  # ADDED
                'project_name': project_name,  # ADDED
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
    Bulk delete asset movement line items (not entire movements)
    Only deletes selected line items, keeps the movement if other line items exist
    """
    db_session = None
    try:
        data = request.get_json()
        movement_ids = data.get('movement_ids', [])
        line_item_ids = data.get('line_item_ids', [])

        print(f"DEBUG: Received movement_ids: {movement_ids}")
        print(f"DEBUG: Received line_item_ids: {line_item_ids}")

        if not line_item_ids:
            return jsonify({'success': False, 'message': 'No line items selected for deletion'}), 400

        db_session = Session()
        app_id = current_user.app_id

        # Track what was deleted
        deleted_line_items = []
        deleted_movements = []
        disallowed_items = []

        # Store movement info for later empty check
        movement_info = {}  # movement_id -> list of line_item_ids

        # Process each selected line item
        for line_item_id in line_item_ids:
            line_item = db_session.query(AssetMovementLineItem).filter_by(
                id=line_item_id,
                app_id=app_id
            ).first()

            if not line_item:
                print(f"DEBUG: Line item {line_item_id} not found")
                continue

            # Get the movement for this line item
            movement = db_session.query(AssetMovement).filter_by(
                id=line_item.asset_movement_id,
                app_id=app_id
            ).first()

            if not movement:
                print(f"DEBUG: Movement for line item {line_item_id} not found")
                continue

            print(
                f"DEBUG: Processing line item {line_item_id} for movement {movement.id} (type: {movement.movement_type})")

            # Check if movement is posted to ledger
            if movement.is_posted_to_ledger:
                disallowed_items.append({
                    'line_item_id': line_item_id,
                    'reference': movement.reference or f'Movement ID: {movement.id}',
                    'reason': 'Movement already posted to ledger'
                })
                continue

            # Store movement info for later
            if movement.id not in movement_info:
                movement_info[movement.id] = {
                    'movement': movement,
                    'line_item_ids': []
                }
            movement_info[movement.id]['line_item_ids'].append(line_item_id)

            # Get asset before deleting line item
            asset = db_session.query(Asset).filter_by(
                id=line_item.asset_id,
                app_id=app_id
            ).first()

            # Update asset status based on movement type
            if asset:
                print(f"DEBUG: Updating asset {asset.id} ({asset.asset_tag}) - Current status: {asset.status}")

                if movement.movement_type == 'assignment':
                    # Reverse assignment - set back to in_stock
                    asset.status = 'in_stock'
                    asset.assigned_to_id = None
                    print(f"DEBUG: Asset {asset.id} status changed to 'in_stock' (reversed assignment)")

                elif movement.movement_type == 'return':
                    # Reverse return - set back to in_stock
                    asset.status = 'in_stock'
                    asset.assigned_to_id = None
                    print(f"DEBUG: Asset {asset.id} status changed to 'in_stock' (reversed return)")

                elif movement.movement_type == 'transfer':
                    # Reverse transfer - send back to original location
                    if line_item.from_location_id:
                        asset.location_id = line_item.from_location_id
                        print(f"DEBUG: Asset {asset.id} location reverted to {line_item.from_location_id}")

                elif movement.movement_type == 'acquisition':
                    # Deleting acquisition - asset should be deactivated
                    asset.status = 'deleted'
                    print(f"DEBUG: Asset {asset.id} status changed to 'deleted' (acquisition reversed)")

                elif movement.movement_type == 'opening_balance':
                    # Deleting opening balance - asset should be deleted
                    asset.status = 'deleted'
                    print(f"DEBUG: Asset {asset.id} status changed to 'deleted' (opening balance reversed)")

                elif movement.movement_type == 'sale' or movement.movement_type == 'disposal':
                    # Deleting sale/disposal - bring asset back
                    asset.status = 'in_stock'
                    print(f"DEBUG: Asset {asset.id} status changed to 'in_stock' (reversed {movement.movement_type})")

                elif movement.movement_type == 'donation_out':
                    # Deleting donation out - bring asset back
                    asset.status = 'in_stock'
                    print(f"DEBUG: Asset {asset.id} status changed to 'in_stock' (reversed donation out)")

                elif movement.movement_type == 'donation_in':
                    # Deleting donation in - asset should be deleted
                    asset.status = 'deleted'
                    print(f"DEBUG: Asset {asset.id} status changed to 'deleted' (donation in reversed)")

                elif movement.movement_type == 'depreciation':
                    # For depreciation, just keep asset as is
                    # Depreciation doesn't change asset status
                    print(f"DEBUG: Asset {asset.id} - depreciation reversal not needed")

                else:
                    print(f"DEBUG: Unknown movement type {movement.movement_type} - no status change")

                db_session.flush()

            # Delete depreciation records for this line item
            db_session.query(DepreciationRecord).filter(
                DepreciationRecord.asset_movement_line_item_id == line_item_id
            ).delete(synchronize_session=False)

            # Delete the line item
            db_session.delete(line_item)
            deleted_line_items.append(line_item_id)
            print(f"DEBUG: Deleted line item {line_item_id}")

        # Check each movement for remaining line items
        for movement_id, info in movement_info.items():
            movement = info['movement']

            # Count remaining line items for this movement
            remaining_line_items = db_session.query(AssetMovementLineItem).filter(
                AssetMovementLineItem.asset_movement_id == movement_id,
                AssetMovementLineItem.app_id == app_id
            ).count()

            print(f"DEBUG: Movement {movement_id} has {remaining_line_items} remaining line items")

            # If no remaining line items, delete the movement header
            if remaining_line_items == 0:
                db_session.delete(movement)
                deleted_movements.append(movement_id)
                print(f"DEBUG: Deleted empty movement {movement_id}")

        db_session.commit()

        # Build response message
        message_parts = []
        if deleted_line_items:
            message_parts.append(f'Deleted {len(deleted_line_items)} asset(s)')
        if deleted_movements:
            message_parts.append(f'Deleted {len(deleted_movements)} empty movement(s)')
        if disallowed_items:
            message_parts.append(f'{len(disallowed_items)} item(s) could not be deleted (posted to ledger)')

        message = ', '.join(message_parts) if message_parts else 'No items were deleted'

        print(f"DEBUG: Success - {message}")

        return jsonify({
            'success': True,
            'message': message,
            'deleted_line_items': len(deleted_line_items),
            'deleted_movements': len(deleted_movements),
            'disallowed_count': len(disallowed_items),
            'disallowed': disallowed_items
        }), 200

    except Exception as e:
        if db_session:
            db_session.rollback()
        logger.error(f"Error in bulk_delete_movements: {str(e)}\n{traceback.format_exc()}")
        print(f"DEBUG: Error - {str(e)}")
        return jsonify({'success': False, 'message': f'Error deleting items: {str(e)}'}), 500
    finally:
        if db_session:
            db_session.close()
