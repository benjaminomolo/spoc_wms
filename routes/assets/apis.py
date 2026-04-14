# app/routes/assets/apis.py

import logging
import traceback

from flask import Blueprint, jsonify, render_template, flash, redirect, url_for, request
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
    InventoryTransactionDetail, User, UserLocationAssignment, Asset, AssetItem
from services.assets_helpers import suggest_next_asset_reference
from services.inventory_helpers import suggest_next_inventory_reference, calculate_inventory_quantities, \
    reverse_inventory_entry, reverse_single_transaction_detail, suggest_next_pos_order_reference, \
    update_user_location_role, get_location_users, deactivate_user_location_assignment, assign_user_to_location
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.numbers import safe_int_conversion

from . import asset_bp

logger = logging.getLogger(__name__)


@asset_bp.route('/api/suggest_asset_reference')
@login_required
def suggest_asset_reference():
    """
    API endpoint to suggest the next inventory reference based on the most recent reference.
    """
    with Session() as db_session:
        suggested_ref = suggest_next_asset_reference(db_session)

        print(f'suggested_reference is {suggested_ref}')

    return jsonify({'suggested_reference': suggested_ref})


@asset_bp.route('/api/asset_movement/get_available_assets', methods=['GET'])
def get_available_assets():
    """Get available assets for asset movement forms"""
    try:
        app_id = current_user.app_id
        db_session = Session()
        # Get query parameters for filtering
        asset_item_id = request.args.get('asset_item_id', type=int)
        movement_type = request.args.get('movement_type', '')
        status_filter = request.args.get('status', '')
        current_asset_id = request.args.get('current_asset_id', type=int)  # ADD THIS

        # Base query
        query = db_session.query(
            Asset.id,
            Asset.asset_tag,
            Asset.serial_number,
            Asset.asset_item_id,
            AssetItem.asset_name.label('asset_item_name'),
            AssetItem.asset_code.label('asset_item_code'),
            Asset.current_value,
            Asset.purchase_price,
            Asset.status,
            Asset.condition,
            Asset.useful_life_years,
            Asset.depreciation_method,
            InventoryLocation.location.label('current_location'),
            Department.department_name.label('current_department'),
            Employee.first_name.label('assigned_to_name')
        ).join(
            AssetItem, Asset.asset_item_id == AssetItem.id
        ).outerjoin(
            InventoryLocation, Asset.location_id == InventoryLocation.id
        ).outerjoin(
            Department, Asset.department_id == Department.id
        ).outerjoin(
            Employee, Asset.assigned_to_id == Employee.id
        ).filter(
            Asset.app_id == app_id,
            AssetItem.status == 'active'
        )

        # Apply filters
        if asset_item_id:
            query = query.filter(Asset.asset_item_id == asset_item_id)

        # Apply status filter based on movement type
        if movement_type:
            if movement_type in ['sale', 'donation_out', 'disposal', 'transfer', 'assignment']:
                # For outgoing movements, show assets that are in stock
                query = query.filter(Asset.status == 'in_stock')
            elif movement_type == 'return':
                # For returns, show assets that are assigned
                query = query.filter(Asset.status == 'assigned')
            elif movement_type in ['acquisition', 'donation_in', 'opening_balance']:
                # For incoming movements, return empty (we're creating new assets)
                return jsonify({'success': True, 'assets': []})

        # Additional status filter if provided
        if status_filter:
            query = query.filter(Asset.status == status_filter)

        # Execute query
        assets = query.order_by(Asset.asset_tag).all()

        # Format response
        assets_list = []
        asset_ids_in_results = set()

        for asset in assets:
            asset_ids_in_results.add(asset.id)
            assets_list.append({
                'id': asset.id,
                'asset_tag': asset.asset_tag,
                'serial_number': asset.serial_number or '',
                'asset_item_id': asset.asset_item_id,
                'asset_item_name': asset.asset_item_name,
                'asset_item_code': asset.asset_item_code,
                'current_value': float(asset.current_value) if asset.current_value else 0.0,
                'purchase_price': float(asset.purchase_price) if asset.purchase_price else 0.0,
                'status': asset.status,
                'condition': asset.condition or '',
                'useful_life_years': asset.useful_life_years,
                'depreciation_method': asset.depreciation_method or '',
                'current_location': asset.current_location or '',
                'current_department': asset.current_department or '',
                'assigned_to_name': asset.assigned_to_name or '',
                'display_text': f"{asset.asset_tag} - {asset.asset_item_name}"
            })

        # ADD THIS: Include the currently selected asset even if it doesn't meet filters
        if current_asset_id and current_asset_id not in asset_ids_in_results:
            current_asset = db_session.query(
                Asset.id,
                Asset.asset_tag,
                Asset.serial_number,
                Asset.asset_item_id,
                AssetItem.asset_name.label('asset_item_name'),
                AssetItem.asset_code.label('asset_item_code'),
                Asset.current_value,
                Asset.purchase_price,
                Asset.status,
                Asset.condition,
                Asset.useful_life_years,
                Asset.depreciation_method,
                InventoryLocation.location.label('current_location'),
                Department.department_name.label('current_department'),
                Employee.first_name.label('assigned_to_name')
            ).join(
                AssetItem, Asset.asset_item_id == AssetItem.id
            ).outerjoin(
                InventoryLocation, Asset.location_id == InventoryLocation.id
            ).outerjoin(
                Department, Asset.department_id == Department.id
            ).outerjoin(
                Employee, Asset.assigned_to_id == Employee.id
            ).filter(
                Asset.id == current_asset_id,
                Asset.app_id == app_id
            ).first()

            if current_asset:
                assets_list.append({
                    'id': current_asset.id,
                    'asset_tag': current_asset.asset_tag,
                    'serial_number': current_asset.serial_number or '',
                    'asset_item_id': current_asset.asset_item_id,
                    'asset_item_name': current_asset.asset_item_name,
                    'asset_item_code': current_asset.asset_item_code,
                    'current_value': float(current_asset.current_value) if current_asset.current_value else 0.0,
                    'purchase_price': float(current_asset.purchase_price) if current_asset.purchase_price else 0.0,
                    'status': current_asset.status,
                    'condition': current_asset.condition or '',
                    'useful_life_years': current_asset.useful_life_years,
                    'depreciation_method': current_asset.depreciation_method or '',
                    'current_location': current_asset.current_location or '',
                    'current_department': current_asset.current_department or '',
                    'assigned_to_name': current_asset.assigned_to_name or '',
                    'display_text': f"{current_asset.asset_tag} - {current_asset.asset_item_name}"
                })

        logger.info(f'Here is the asset list {assets_list}')

        return jsonify({
            'success': True,
            'assets': assets_list,
            'count': len(assets_list)
        })

    except Exception as e:
        logger.error(f"Error fetching available assets: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500


@asset_bp.route('/api/asset_movement/search_available_assets', methods=['GET'])
def search_available_assets():
    """Search available assets with multiple criteria"""
    try:
        app_id = current_user.app_id
        db_session = Session()

        # Get search parameters
        search_term = request.args.get('q', '')
        asset_item_id = request.args.get('asset_item_id', type=int)
        location_id = request.args.get('location_id', type=int)
        department_id = request.args.get('department_id', type=int)
        status = request.args.get('status', '')
        condition = request.args.get('condition', '')
        movement_type = request.args.get('movement_type', '')

        # Build query
        query = db_session.query(
            Asset.id,
            Asset.asset_tag,
            Asset.serial_number,
            Asset.asset_item_id,
            AssetItem.asset_name.label('asset_item_name'),
            Asset.current_value,
            Asset.status,
            Asset.condition,
            InventoryLocation.location_name.label('location_name')
        ).join(
            AssetItem, Asset.asset_item_id == AssetItem.id
        ).outerjoin(
            InventoryLocation, Asset.location_id == InventoryLocation.id
        ).filter(
            Asset.app_id == app_id,
            AssetItem.status == 'active'
        )

        # Apply movement type filters
        if movement_type:
            if movement_type in ['sale', 'donation_out', 'disposal', 'assignment', 'transfer']:
                query = query.filter(Asset.status == 'in_stock')
            elif movement_type == 'return':
                query = query.filter(Asset.status == 'assigned')

        # Apply search term
        if search_term:
            search_term = f"%{search_term}%"
            query = query.filter(
                or_(
                    Asset.asset_tag.ilike(search_term),
                    Asset.serial_number.ilike(search_term),
                    AssetItem.asset_name.ilike(search_term)
                )
            )

        # Apply other filters
        if asset_item_id:
            query = query.filter(Asset.asset_item_id == asset_item_id)
        if location_id:
            query = query.filter(Asset.location_id == location_id)
        if department_id:
            query = query.filter(Asset.department_id == department_id)
        if status:
            query = query.filter(Asset.status == status)
        if condition:
            query = query.filter(Asset.condition == condition)

        # Limit results for performance
        assets = query.limit(50).all()

        # Format results for Select2
        results = []
        for asset in assets:
            results.append({
                'id': asset.id,
                'text': f"{asset.asset_tag} - {asset.asset_item_name}",
                'asset_tag': asset.asset_tag,
                'serial_number': asset.serial_number or '',
                'current_value': float(asset.current_value) if asset.current_value else 0.0,
                'status': asset.status,
                'condition': asset.condition or '',
                'location': asset.location.location or ''
            })

        return jsonify({
            'results': results,
            'pagination': {'more': False}
        })

    except Exception as e:
        logger.error(f"Error searching assets: {str(e)}")
        return jsonify({'results': [], 'error': str(e)})
