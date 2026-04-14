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



@asset_bp.route('/dashboard', methods=["GET"])
@login_required
@require_permission('Assets', 'view')
def asset_dashboard():
    """
    Render the asset dashboard page
    """
    db_session = Session()
    try:
        app_id = current_user.app_id
        role = current_user.role
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        module_name = "Assets"
        api_key = company.api_key

        # Fetch filter data
        asset_types = db_session.query(AssetItem).filter_by(
            app_id=app_id, status='active'
        ).order_by(AssetItem.asset_name).all()

        locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).order_by(InventoryLocation.location).all()
        departments = db_session.query(Department).filter_by(app_id=app_id).order_by(Department.department_name).all()
        employees = db_session.query(Employee).filter_by(app_id=app_id, employment_status='active').order_by(Employee.first_name).all()
        suppliers = db_session.query(Vendor).filter(Vendor.app_id == app_id, Vendor.is_active == True).order_by(Vendor.vendor_name).all()
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).order_by(InventoryCategory.category_name).all()
        brands = db_session.query(Brand).filter_by(app_id=app_id).order_by(Brand.name).all()
        projects = db_session.query(Project).filter_by(app_id=app_id, is_active=True).order_by(Project.name).all()

        # Status options
        status_options = ['in_stock', 'assigned', 'maintenance', 'disposed', 'sold']
        condition_options = ['excellent', 'good', 'fair', 'poor']

        # Get base currency
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency = base_currency_info["base_currency"]
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        return render_template(
            '/assets/asset_dashboard.html',
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name,
            api_key=api_key,
            base_currency=base_currency,
            currencies=currencies,
            asset_types=asset_types,
            locations=locations,
            departments=departments,
            employees=employees,
            suppliers=suppliers,
            categories=categories,
            brands=brands,
            projects=projects,
            status_options=status_options,
            condition_options=condition_options,
            now=datetime.now()
        )

    except Exception as e:
        logger.error(f"Error rendering asset dashboard: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the asset dashboard', 'error')
        return redirect(url_for('main.dashboard'))
    finally:
        db_session.close()


