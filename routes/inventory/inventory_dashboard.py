# app/routes/inventory/inventory_dashboard.py

import logging
import traceback

from flask import Blueprint, jsonify, render_template, flash, redirect, url_for, request
from flask_login import login_required, current_user
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from decorators import role_required, require_permission
from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Project, ChartOfAccounts, PaymentMode, Category, Department, Employee, \
    PayrollPeriod, InventoryCategory, InventorySubCategory, InventoryLocation, InventoryItem, InventoryItemVariation, \
    InventoryItemAttribute, Brand, InventoryItemVariationLink, AssetItem, Vendor
from services.inventory_helpers import get_user_accessible_locations

from . import inventory_bp

# Set up logging
logger = logging.getLogger(__name__)


@inventory_bp.route('/dashboard', methods=["GET"])
@login_required
@require_permission('Inventory', 'view')  # Requires view access
def inventory_dashboard():
    db_session = Session()
    try:
        app_id = current_user.app_id
        role = current_user.role
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        module_name = "Inventory"
        api_key = company.api_key

        # Fetch necessary data
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
        subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
        locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
        # Fetch items - using working query without the problematic outerjoin
        items = db_session.query(InventoryItemVariationLink). \
            join(InventoryItemVariationLink.inventory_item). \
            filter(InventoryItemVariationLink.app_id == app_id). \
            order_by(
            InventoryItem.item_name.asc()
        ). \
            all()

        variations = db_session.query(InventoryItemVariation).filter_by(app_id=app_id).all()
        attributes = db_session.query(InventoryItemAttribute).filter_by(app_id=app_id).all()
        brands = db_session.query(Brand).filter_by(app_id=app_id).all()

        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency = base_currency_info["base_currency"]

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        return render_template(
            '/inventory/inventory_dashboard.html',
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name,
            api_key=api_key,
            base_currency=base_currency,
            currencies=currencies,
            categories=categories,
            subcategories=subcategories,
            locations=locations,
            variations=variations,
            attributes=attributes,
            brands=brands,
            items=items
        )

    finally:
        db_session.close()


@inventory_bp.route('/wms_dashboard', methods=["GET"])
@login_required
@require_permission('Inventory', 'view')
def wms_dashboard():
    db_session = Session()
    try:
        app_id = current_user.app_id
        role = current_user.role
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        api_key = company.api_key

        # Get user's accessible locations
        user_locations = get_user_accessible_locations(current_user.id, app_id)
        user_location_ids = [loc.id for loc in user_locations]

        # Inventory filters data - FILTER LOCATIONS BY USER ACCESS
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
        subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()

        # Only show locations user has access to
        if user_location_ids:
            locations = db_session.query(InventoryLocation).filter(
                InventoryLocation.app_id == app_id,
                InventoryLocation.id.in_(user_location_ids)
            ).all()
        else:
            locations = []

        items = db_session.query(InventoryItemVariationLink).join(InventoryItemVariationLink.inventory_item).filter(
            InventoryItemVariationLink.app_id == app_id
        ).order_by(InventoryItem.item_name.asc()).all()
        variations = db_session.query(InventoryItemVariation).filter_by(app_id=app_id).all()
        brands = db_session.query(Brand).filter_by(app_id=app_id).all()

        # Asset filters data
        asset_types = db_session.query(AssetItem).filter_by(app_id=app_id, status='active').all()
        departments = db_session.query(Department).filter_by(app_id=app_id).all()
        employees = db_session.query(Employee).filter_by(app_id=app_id, is_active=True).all()
        suppliers = db_session.query(Vendor).filter(Vendor.app_id == app_id, Vendor.is_active == True).all()
        projects = db_session.query(Project).filter_by(app_id=app_id, is_active=True).all()
        status_options = ['in_stock', 'assigned', 'maintenance', 'disposed', 'sold']

        base_currency_info = get_base_currency(db_session, app_id)
        base_currency = base_currency_info["base_currency"] if base_currency_info else "USD"

        return render_template(
            'inventory/wms_dashboard.html',
            company=company,
            role=role,
            modules=modules_data,
            module_name="WMS Dashboard",
            api_key=api_key,
            base_currency=base_currency,
            categories=categories,
            subcategories=subcategories,
            locations=locations,
            items=items,
            variations=variations,
            brands=brands,
            asset_types=asset_types,
            departments=departments,
            employees=employees,
            suppliers=suppliers,
            projects=projects,
            status_options=status_options
        )
    except Exception as e:
        logger.error(f"Error rendering WMS dashboard: {str(e)}")
        flash('An error occurred while loading the WMS dashboard', 'error')
        return redirect(url_for('main.dashboard'))
    finally:
        db_session.close()
