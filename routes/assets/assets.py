# app/routes/asset/asset_management.py
import decimal
import traceback
from datetime import datetime, timedelta, date
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
    render_edit_inventory_entry_form, remove_inventory_journal_entries
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


@asset_bp.route('/asset_list')
@login_required
def asset_list():
    """
    Render the asset list page (individual assets)
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Get filter data
        asset_types = db_session.query(AssetItem).filter_by(
            app_id=app_id, status='active'
        ).order_by(AssetItem.asset_name).all()

        locations = db_session.query(InventoryLocation).filter_by(
            app_id=app_id
        ).order_by(InventoryLocation.location).all()

        departments = db_session.query(Department).filter_by(
            app_id=app_id
        ).order_by(Department.department_name).all()

        employees = db_session.query(Employee).filter_by(
            app_id=app_id, employment_status='active'
        ).order_by(Employee.first_name).all()

        suppliers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True
        ).order_by(Vendor.vendor_name).all()

        projects = db_session.query(Project).filter_by(
            app_id=app_id, is_active=True
        ).order_by(Project.name).all()

        # Status options
        status_options = ['in_stock', 'assigned', 'maintenance', 'disposed', 'sold']

        # Condition options
        condition_options = ['excellent', 'good', 'fair', 'poor']

        return render_template(
            'assets/asset_list.html',
            company=company,
            modules=modules_data,
            role=role,
            asset_types=asset_types,
            locations=locations,
            departments=departments,
            employees=employees,
            suppliers=suppliers,
            projects=projects,
            status_options=status_options,
            condition_options=condition_options,
            now=datetime.now()
        )

    except Exception as e:
        logger.error(f"Error rendering asset list page: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the asset list page', 'error')
        return redirect(url_for('main.dashboard'))

    finally:
        db_session.close()


@asset_bp.route('/api/assets', methods=['GET'])
@login_required
def api_assets():
    """
    Return JSON data of individual assets with filtering and pagination
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get pagination and filter parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        search = request.args.get('search', '')
        asset_type_id = request.args.get('asset_type_id', type=int)
        status = request.args.get('status', '')
        condition = request.args.get('condition', '')
        location_id = request.args.get('location_id', type=int)
        department_id = request.args.get('department_id', type=int)
        assigned_to_id = request.args.get('assigned_to_id', type=int)
        supplier_id = request.args.get('supplier_id', type=int)
        project_id = request.args.get('project_id', type=int)
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')

        # Build query
        query = db_session.query(Asset).filter(
            Asset.app_id == app_id
        )

        # Apply joins for related data
        query = query.outerjoin(AssetItem, Asset.asset_item_id == AssetItem.id)
        query = query.outerjoin(InventoryLocation, Asset.location_id == InventoryLocation.id)
        query = query.outerjoin(Department, Asset.department_id == Department.id)
        query = query.outerjoin(Employee, Asset.assigned_to_id == Employee.id)
        query = query.outerjoin(Vendor, Asset.supplier_id == Vendor.id)
        query = query.outerjoin(Project, Asset.project_id == Project.id)

        # Apply filters
        if search:
            search_term = f'%{search}%'
            query = query.filter(
                or_(
                    Asset.asset_tag.ilike(search_term),
                    Asset.serial_number.ilike(search_term),
                    AssetItem.asset_name.ilike(search_term),
                    AssetItem.asset_code.ilike(search_term)
                )
            )

        if asset_type_id:
            query = query.filter(Asset.asset_item_id == asset_type_id)

        if status:
            query = query.filter(Asset.status == status)

        if condition:
            query = query.filter(Asset.condition == condition)

        if location_id:
            query = query.filter(Asset.location_id == location_id)

        if department_id:
            query = query.filter(Asset.department_id == department_id)

        if assigned_to_id:
            query = query.filter(Asset.assigned_to_id == assigned_to_id)

        if supplier_id:
            query = query.filter(Asset.supplier_id == supplier_id)

        if project_id:
            query = query.filter(Asset.project_id == project_id)

        if date_from:
            try:
                date_from_obj = datetime.strptime(date_from, "%Y-%m-%d").date()
                query = query.filter(Asset.purchase_date >= date_from_obj)
            except ValueError:
                pass

        if date_to:
            try:
                date_to_obj = datetime.strptime(date_to, "%Y-%m-%d").date()
                query = query.filter(Asset.purchase_date <= date_to_obj)
            except ValueError:
                pass

        # Get total count
        total_items = query.count()

        # Calculate pagination
        total_pages = (total_items + per_page - 1) // per_page

        # Get paginated results - newest first
        assets = query.order_by(
            Asset.created_at.desc()
        ).offset((page - 1) * per_page).limit(per_page).all()

        # Serialize data
        assets_data = []
        for asset in assets:
            # Calculate depreciation percentage
            depreciation_pct = 0
            if asset.purchase_price and asset.purchase_price > 0:
                depreciation_pct = round(((asset.purchase_price - asset.current_value) / asset.purchase_price) * 100, 1)

            assets_data.append({
                'id': asset.id,
                'asset_tag': asset.asset_tag,
                'serial_number': asset.serial_number,
                'asset_type_id': asset.asset_item_id,
                'asset_type_name': asset.asset_item.asset_name if asset.asset_item else None,
                'asset_type_code': asset.asset_item.asset_code if asset.asset_item else None,
                'status': asset.status,
                'status_label': asset.status.replace('_', ' ').title(),
                'condition': asset.condition,
                'condition_label': asset.condition.title() if asset.condition else None,
                'location_id': asset.location_id,
                'location_name': asset.location.location if asset.location else None,
                'department_id': asset.department_id,
                'department_name': asset.department.department_name if asset.department else None,
                'assigned_to_id': asset.assigned_to_id,
                'assigned_to_name': f"{asset.assigned_to.first_name} {asset.assigned_to.last_name}".strip() if asset.assigned_to else None,
                'purchase_date': asset.purchase_date.strftime('%Y-%m-%d') if asset.purchase_date else None,
                'purchase_price': float(asset.purchase_price) if asset.purchase_price else 0,
                'current_value': float(asset.current_value) if asset.current_value else 0,
                'depreciation_pct': depreciation_pct,
                'supplier_id': asset.supplier_id,
                'supplier_name': asset.supplier.vendor_name if asset.supplier else None,
                'project_id': asset.project_id,
                'project_name': asset.project.name if asset.project else None,
                'useful_life_years': asset.useful_life_years,
                'depreciation_method': asset.depreciation_method,
                'warranty_expiry': asset.warranty_expiry.strftime('%Y-%m-%d') if asset.warranty_expiry else None,
                'created_at': asset.created_at.strftime('%Y-%m-%d') if asset.created_at else None
            })

        return jsonify({
            'success': True,
            'assets': assets_data,
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
        logger.error(f"Error in api_assets: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching assets'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/api/assets/<int:asset_id>', methods=['GET'])
@login_required
def api_asset_detail(asset_id):
    """
    Return JSON data for a single asset
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        asset = db_session.query(Asset).filter_by(
            id=asset_id,
            app_id=app_id
        ).first()

        if not asset:
            return jsonify({
                'success': False,
                'message': 'Asset not found'
            }), 404

        # Get movement history for this asset
        movements = db_session.query(AssetMovementLineItem).filter_by(
            asset_id=asset_id,
            app_id=app_id
        ).order_by(
            AssetMovementLineItem.asset_movement_id.desc()
        ).limit(10).all()

        movement_history = []
        for movement in movements:
            movement_history.append({
                'id': movement.id,
                'movement_id': movement.asset_movement_id,
                'movement_type': movement.asset_movement.movement_type if movement.asset_movement else None,
                'transaction_date': movement.asset_movement.transaction_date.strftime(
                    '%Y-%m-%d') if movement.asset_movement and movement.asset_movement.transaction_date else None,
                'reference': movement.asset_movement.reference if movement.asset_movement else None,
                'transaction_value': float(movement.transaction_value) if movement.transaction_value else 0,
                'from_location': movement.from_location.location if movement.from_location else None,
                'to_location': movement.to_location.location if movement.to_location else None,
                'party_name': movement.party.vendor_name if movement.party else None
            })

        data = {
            'id': asset.id,
            'asset_tag': asset.asset_tag,
            'serial_number': asset.serial_number,
            'asset_type_id': asset.asset_item_id,
            'asset_type_name': asset.asset_item.asset_name if asset.asset_item else None,
            'asset_type_code': asset.asset_item.asset_code if asset.asset_item else None,
            'status': asset.status,
            'condition': asset.condition,
            'location_id': asset.location_id,
            'location_name': asset.location.location if asset.location else None,
            'department_id': asset.department_id,
            'department_name': asset.department.department_name if asset.department else None,
            'assigned_to_id': asset.assigned_to_id,
            'assigned_to_name': f"{asset.assigned_to.first_name} {asset.assigned_to.last_name}".strip() if asset.assigned_to else None,
            'purchase_date': asset.purchase_date.strftime('%Y-%m-%d') if asset.purchase_date else None,
            'purchase_price': float(asset.purchase_price) if asset.purchase_price else 0,
            'current_value': float(asset.current_value) if asset.current_value else 0,
            'supplier_id': asset.supplier_id,
            'supplier_name': asset.supplier.vendor_name if asset.supplier else None,
            'project_id': asset.project_id,
            'project_name': asset.project.name if asset.project else None,
            'useful_life_years': asset.useful_life_years,
            'depreciation_method': asset.depreciation_method,
            'capitalization_date': asset.capitalization_date.strftime(
                '%Y-%m-%d') if asset.capitalization_date else None,
            'last_depreciation_date': asset.last_depreciation_date.strftime(
                '%Y-%m-%d') if asset.last_depreciation_date else None,
            'warranty_expiry': asset.warranty_expiry.strftime('%Y-%m-%d') if asset.warranty_expiry else None,
            'created_at': asset.created_at.strftime('%Y-%m-%d %H:%M:%S') if asset.created_at else None,
            'movement_history': movement_history
        }

        return jsonify({
            'success': True,
            'asset': data
        })

    except Exception as e:
        logger.error(f"Error fetching asset {asset_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching asset details'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/api/assets/bulk_delete', methods=['POST'])
@login_required
@role_required(['Admin', 'Supervisor'])
def api_assets_bulk_delete():
    """
    Bulk delete assets (only if no movements linked)
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()
        asset_ids = data.get('asset_ids', [])

        if not asset_ids:
            return jsonify({
                'success': False,
                'message': 'No assets selected'
            }), 400

        deleted_count = 0
        failed_ids = []
        failed_messages = []

        for asset_id in asset_ids:
            asset = db_session.query(Asset).filter_by(
                id=asset_id,
                app_id=app_id
            ).first()

            if asset:
                # Check if asset has any movement history
                movements = db_session.query(AssetMovementLineItem).filter_by(
                    asset_id=asset_id,
                    app_id=app_id
                ).count()

                if movements > 0:
                    failed_ids.append(asset_id)
                    failed_messages.append(f"{asset.asset_tag} has {movements} movement(s)")
                else:
                    db_session.delete(asset)
                    deleted_count += 1

        db_session.commit()

        message = f'Successfully deleted {deleted_count} asset(s)'
        if failed_ids:
            message += f'. {len(failed_ids)} asset(s) skipped: ' + '; '.join(failed_messages[:3])
            if len(failed_messages) > 3:
                message += f' and {len(failed_messages) - 3} more'

        return jsonify({
            'success': True,
            'message': message,
            'deleted_count': deleted_count,
            'failed_ids': failed_ids
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error bulk deleting assets: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while deleting assets'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/api/assets/export', methods=['POST'])
@login_required
def api_assets_export():
    """
    Export assets to CSV
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()

        # Build query with same filters as list
        query = db_session.query(Asset).filter(
            Asset.app_id == app_id
        )

        # Apply filters (same as api_assets)
        if data.get('asset_type_id'):
            query = query.filter(Asset.asset_item_id == data['asset_type_id'])
        if data.get('status'):
            query = query.filter(Asset.status == data['status'])
        if data.get('condition'):
            query = query.filter(Asset.condition == data['condition'])
        if data.get('location_id'):
            query = query.filter(Asset.location_id == data['location_id'])
        if data.get('department_id'):
            query = query.filter(Asset.department_id == data['department_id'])

        assets = query.order_by(Asset.asset_tag).all()

        # Generate CSV
        import csv
        from io import StringIO

        output = StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow([
            'Asset Tag',
            'Serial Number',
            'Asset Type',
            'Asset Code',
            'Status',
            'Condition',
            'Location',
            'Department',
            'Assigned To',
            'Purchase Date',
            'Purchase Price',
            'Current Value',
            'Depreciation %',
            'Supplier',
            'Project',
            'Warranty Expiry'
        ])

        # Write data
        for asset in assets:
            depreciation_pct = 0
            if asset.purchase_price and asset.purchase_price > 0:
                depreciation_pct = round(((asset.purchase_price - asset.current_value) / asset.purchase_price) * 100, 1)

            writer.writerow([
                asset.asset_tag,
                asset.serial_number or '',
                asset.asset_item.asset_name if asset.asset_item else '',
                asset.asset_item.asset_code if asset.asset_item else '',
                asset.status.replace('_', ' ').title(),
                asset.condition.title() if asset.condition else '',
                asset.location.location if asset.location else '',
                asset.department.department_name if asset.department else '',
                f"{asset.assigned_to.first_name} {asset.assigned_to.last_name}".strip() if asset.assigned_to else '',
                asset.purchase_date.strftime('%Y-%m-%d') if asset.purchase_date else '',
                float(asset.purchase_price) if asset.purchase_price else 0,
                float(asset.current_value) if asset.current_value else 0,
                f"{depreciation_pct}%",
                asset.supplier.vendor_name if asset.supplier else '',
                asset.project.name if asset.project else '',
                asset.warranty_expiry.strftime('%Y-%m-%d') if asset.warranty_expiry else ''
            ])

        output.seek(0)

        from flask import make_response
        response = make_response(output.getvalue())
        response.headers[
            "Content-Disposition"] = f"attachment; filename=assets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        response.headers["Content-type"] = "text/csv"

        return response

    except Exception as e:
        logger.error(f"Error exporting assets: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while exporting assets'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/asset/<int:asset_id>/view')
@login_required
def asset_view(asset_id):
    """
    Render the individual asset view page
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get the asset
        asset = db_session.query(Asset).filter_by(
            id=asset_id,
            app_id=app_id
        ).first()

        if not asset:
            flash("Asset not found", "error")
            return redirect(url_for('assets.asset_list'))

        # Get company info
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                       db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Get base currency
        base_currency_info = get_base_currency(db_session, app_id)
        base_currency = base_currency_info["base_currency"] if base_currency_info else "USD"

        return render_template(
            'assets/asset_view.html',
            company=company,
            modules=modules_data,
            role=role,
            base_currency=base_currency,
            asset=asset,
            now=datetime.now()
        )

    except Exception as e:
        logger.error(f"Error rendering asset view for {asset_id}: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the asset view', 'error')
        return redirect(url_for('assets.asset_list'))

    finally:
        db_session.close()


@asset_bp.route('/api/<int:asset_id>/details', methods=['GET'])
@login_required
def api_asset_details(asset_id):
    """
    Return JSON data for a single asset with all related details and movement history
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get asset with all related data
        asset = db_session.query(
            Asset,
            AssetItem,
            InventoryLocation,
            Department,
            Employee,
            Vendor,
            Project
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
        ).outerjoin(
            Project, Asset.project_id == Project.id
        ).filter(
            Asset.id == asset_id,
            Asset.app_id == app_id
        ).first()

        if not asset:
            return jsonify({
                'success': False,
                'message': 'Asset not found'
            }), 404

        asset_record, asset_type, location, department, employee, supplier, project = asset

        # Calculate depreciation
        purchase_price = float(asset_record.purchase_price or 0)
        current_value = float(asset_record.current_value or 0)
        depreciation = purchase_price - current_value
        depreciation_pct = (depreciation / purchase_price * 100) if purchase_price > 0 else 0

        # Get movement history
        movements = db_session.query(
            AssetMovement,
            AssetMovementLineItem,
            Vendor,
            InventoryLocation,
            Department,
            Employee
        ).join(
            AssetMovementLineItem,
            AssetMovement.id == AssetMovementLineItem.asset_movement_id
        ).outerjoin(
            Vendor,
            AssetMovementLineItem.party_id == Vendor.id
        ).outerjoin(
            InventoryLocation,
            AssetMovementLineItem.to_location_id == InventoryLocation.id
        ).outerjoin(
            Department,
            AssetMovementLineItem.to_department_id == Department.id
        ).outerjoin(
            Employee,
            AssetMovementLineItem.assigned_to_id == Employee.id
        ).filter(
            AssetMovementLineItem.asset_id == asset_id,
            AssetMovement.app_id == app_id
        ).order_by(
            AssetMovement.transaction_date.desc()
        ).all()

        movement_history = []
        for movement, line_item, party, to_location, to_department, to_employee in movements:
            # Get from location/department
            from_location_name = None
            if line_item.from_location_id:
                from_loc = db_session.query(InventoryLocation).filter_by(id=line_item.from_location_id).first()
                from_location_name = from_loc.location if from_loc else None

            from_department_name = None
            if line_item.from_department_id:
                from_dept = db_session.query(Department).filter_by(id=line_item.from_department_id).first()
                from_department_name = from_dept.department_name if from_dept else None

            # Format from/to display
            from_display = from_location_name or from_department_name or '—'
            to_display = to_location.location if to_location else to_department.department_name if to_department else f"{to_employee.first_name} {to_employee.last_name}".strip() if to_employee else '—'

            movement_history.append({
                'id': movement.id,
                'movement_type': movement.movement_type,
                'movement_type_label': movement.movement_type.replace('_', ' ').title(),
                'transaction_date': movement.transaction_date.strftime('%Y-%m-%d'),
                'reference': movement.reference or '—',
                'from': from_display,
                'to': to_display,
                'party': party.vendor_name if party else '—',
                'transaction_value': float(line_item.transaction_value or 0),
                'notes': line_item.line_notes or '—'
            })

        # Get depreciation history
        depreciation_history = db_session.query(DepreciationRecord).filter_by(
            asset_id=asset_id,
            app_id=app_id
        ).order_by(
            DepreciationRecord.depreciation_date.desc()
        ).all()

        dep_history = []
        for dep in depreciation_history:
            dep_history.append({
                'id': dep.id,
                'date': dep.depreciation_date.strftime('%Y-%m-%d'),
                'amount': float(dep.depreciation_amount or 0),
                'previous_value': float(dep.previous_value or 0),
                'new_value': float(dep.new_value or 0),
                'notes': dep.notes or '—'
            })

        # Format response
        asset_data = {
            'id': asset_record.id,
            'asset_tag': asset_record.asset_tag,
            'serial_number': asset_record.serial_number or '—',
            'asset_type_id': asset_record.asset_item_id,
            'asset_type_name': asset_type.asset_name if asset_type else '—',
            'asset_type_code': asset_type.asset_code if asset_type else '—',
            'asset_description': asset_type.asset_description if asset_type else '—',
            'status': asset_record.status,
            'status_label': asset_record.status.replace('_', ' ').title(),
            'condition': asset_record.condition or '—',
            'condition_label': asset_record.condition.title() if asset_record.condition else '—',
            'location_id': asset_record.location_id,
            'location_name': location.location if location else '—',
            'department_id': asset_record.department_id,
            'department_name': department.department_name if department else '—',
            'assigned_to_id': asset_record.assigned_to_id,
            'assigned_to_name': f"{employee.first_name} {employee.last_name}".strip() if employee else '—',
            'purchase_date': asset_record.purchase_date.strftime('%Y-%m-%d') if asset_record.purchase_date else '—',
            'purchase_price': purchase_price,
            'current_value': current_value,
            'depreciation': depreciation,
            'depreciation_pct': round(depreciation_pct, 1),
            'supplier_id': asset_record.supplier_id,
            'supplier_name': supplier.vendor_name if supplier else '—',
            'project_id': asset_record.project_id,
            'project_name': project.name if project else '—',
            'useful_life_years': asset_record.useful_life_years or asset_type.expected_useful_life_years if asset_type else '—',
            'depreciation_method': asset_record.depreciation_method or asset_type.depreciation_method if asset_type else '—',
            'depreciation_method_label': (asset_record.depreciation_method or asset_type.depreciation_method or '').replace('_', ' ').title() if (asset_record.depreciation_method or asset_type.depreciation_method) else 'Straight Line',
            'capitalization_date': asset_record.capitalization_date.strftime('%Y-%m-%d') if asset_record.capitalization_date else '—',
            'last_depreciation_date': asset_record.last_depreciation_date.strftime('%Y-%m-%d') if asset_record.last_depreciation_date else '—',
            'warranty_expiry': asset_record.warranty_expiry.strftime('%Y-%m-%d') if asset_record.warranty_expiry else '—',
            'warranty_days_left': (asset_record.warranty_expiry - date.today()).days if asset_record.warranty_expiry else None,
            'created_at': asset_record.created_at.strftime('%Y-%m-%d %H:%M:%S') if asset_record.created_at else '—',
            'movement_history': movement_history,
            'depreciation_history': dep_history
        }

        return jsonify({
            'success': True,
            'asset': asset_data
        })

    except Exception as e:
        logger.error(f"Error fetching asset details for {asset_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching asset details'
        }), 500

    finally:
        db_session.close()


@asset_bp.route('/api/<int:asset_id>/depreciation', methods=['POST'])
@login_required
@role_required(['Admin', 'Supervisor', 'Contributor'])
def api_asset_record_depreciation(asset_id):
    """
    Record a manual depreciation entry for an asset
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()
        amount = Decimal(str(data.get('amount', 0)))
        notes = data.get('notes', '')
        depreciation_date = parse_date(data.get('date'))
        movement_line_item_id = data.get('movement_line_item_id')  # ✅ Get from request if provided

        if not depreciation_date:
            depreciation_date = date.today()

        if amount <= 0:
            return jsonify({
                'success': False,
                'message': 'Depreciation amount must be greater than zero'
            }), 400

        asset = db_session.query(Asset).filter_by(
            id=asset_id,
            app_id=app_id
        ).first()

        if not asset:
            return jsonify({
                'success': False,
                'message': 'Asset not found'
            }), 404

        if asset.current_value <= 0:
            return jsonify({
                'success': False,
                'message': 'Asset already has zero or negative value'
            }), 400

        if amount > asset.current_value:
            return jsonify({
                'success': False,
                'message': f'Depreciation amount ({amount}) cannot exceed current value ({asset.current_value})'
            }), 400

        # Create depreciation record
        previous_value = asset.current_value
        new_value = asset.current_value - amount

        depreciation_record = DepreciationRecord(
            app_id=app_id,
            asset_id=asset_id,
            asset_movement_line_item_id=movement_line_item_id,  # ✅ Link to movement line item
            depreciation_date=depreciation_date,
            depreciation_amount=amount,
            previous_value=previous_value,
            new_value=new_value,
            notes=notes,
            created_by=current_user.id,
            created_at=func.now()
        )

        # Update asset
        asset.current_value = new_value
        asset.last_depreciation_date = depreciation_date
        asset.updated_at = func.now()

        db_session.add(depreciation_record)
        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Depreciation recorded successfully',
            'new_value': float(new_value),
            'depreciation_record_id': depreciation_record.id
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error recording depreciation for asset {asset_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while recording depreciation'
        }), 500

    finally:
        db_session.close()
