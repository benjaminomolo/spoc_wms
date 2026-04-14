# app/routes/inventory/backorders.py
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
    SalesOrderItem, QuotationItem, SalesInvoiceItem, JournalEntry, UserLocationAssignment, InventorySummary, Backorder, \
    BackorderItem, BackorderFulfillment
import logging

from services.inventory_helpers import handle_supplier_logic, \
    render_inventory_entry_form, \
    process_inventory_entries, reverse_inventory_entry, get_inventory_entry_with_details, \
    render_edit_inventory_entry_form, process_inventory_entries_for_edit, remove_inventory_journal_entries
from utils import ensure_default_location, generate_unique_lot, create_notification, empty_to_none, \
    validate_quantity_and_selling_price, validate_quantity_and_price, handle_batch_variation_update, \
    generate_next_backorder_number
from utils_and_helpers.amounts_utils import format_amount
from utils_and_helpers.cache_keys import stock_history_cache_key
from utils_and_helpers.cache_utils import on_inventory_data_changed, clear_stock_history_cache
from utils_and_helpers.forms import get_locked_record
from . import inventory_bp
from .post_to_ledger import post_inventory_entry_to_ledger

logger = logging.getLogger(__name__)


@inventory_bp.route('/add_backorder', methods=['GET', 'POST'])
@login_required
def add_backorder():
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

    if request.method == 'POST':

        try:
            # Extract backorder source information

            # Extract customer data
            customer_id = request.form.get('customer_id')

            if customer_id and customer_id.isdigit():
                customer_id = int(customer_id)
            else:
                # Handle one-time customer if needed
                customer_id = None

            backorder_number = generate_next_backorder_number(db_session, app_id)

            # Extract backorder details
            backorder_date = datetime.strptime(request.form['backorder_date'], '%Y-%m-%d').date()

            expected_fulfillment_date = request.form.get('expected_fulfillment_date')
            if expected_fulfillment_date:
                expected_fulfillment_date = datetime.strptime(expected_fulfillment_date, '%Y-%m-%d').date()

            else:
                expected_fulfillment_date = None

            priority = int(request.form.get('priority_level', 3))

            source_type = None
            source_id = None

            # Create new Backorder object
            new_backorder = Backorder(
                backorder_number=backorder_number,
                source_type=source_type,
                source_id=source_id,
                customer_id=customer_id,
                backorder_date=backorder_date,
                expected_fulfillment_date=expected_fulfillment_date,
                status='pending',
                priority=priority,
                created_by=current_user.id,
                app_id=app_id
            )

            db_session.add(new_backorder)
            db_session.flush()

            # Handle Backorder Items
            item_ids = request.form.getlist('inventory_item[]')
            quantities = request.form.getlist('quantity_ordered[]')
            uom_ids = request.form.getlist('uom[]')
            item_expected_dates = request.form.getlist('item_expected_fulfillment_date[]')

            for idx, (item_id, quantity, uom_id, item_expected_date) in enumerate(
                    zip(item_ids, quantities, uom_ids, item_expected_dates), start=1):
                item_expected_date = datetime.strptime(item_expected_date,
                                                       '%Y-%m-%d').date() if item_expected_date else None

                # Create BackorderItem
                new_item = BackorderItem(
                    backorder_id=new_backorder.id,
                    item_id=item_id,
                    original_quantity=float(quantity),
                    fulfilled_quantity=0,
                    remaining_quantity=float(quantity),
                    uom_id=int(uom_id),
                    status='pending',
                    expected_fulfillment_date=item_expected_date,
                    app_id=app_id
                )

                db_session.add(new_item)

            # Commit all items to the database
            db_session.commit()

            # Return JSON success response
            flash("Backorder added successfully!", "success")
            return jsonify({
                'success': True,
                'message': 'Backorder added successfully!',
                'backorder_id': new_backorder.id
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f"An error occurred: {e}")
            flash(f"An error occurred: {str(e)}", "error")
            return jsonify({
                'success': False,
                'message': f'An error occurred: {str(e)}'
            })

        finally:
            db_session.close()

    else:

        # Fetch necessary data for the form
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()
        currencies = db_session.query(Currency).filter_by(app_id=app_id)
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id

        inventory_items = db_session.query(InventoryItemVariationLink).join(InventoryItem).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.status == "active"
        ).order_by(InventoryItem.item_name.asc()).all()

        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()

        # Default backorder date is today
        backorder_date = date.today().strftime('%Y-%m-%d')

        return render_template('/inventory/new_backorder.html',
                               customers=customers,
                               inventory_items=inventory_items,
                               currencies=currencies,
                               base_currency_id=base_currency_id,
                               base_currency_code=base_currency_code,

                               uoms=uoms,
                               backorder_date=backorder_date,
                               modules=modules_data,
                               company=company,
                               role=role)


@inventory_bp.route('/backorders')
@login_required
def backorders():
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get company and modules data (consistent with inventory_entries approach)
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = (
            db_session.query(Module.module_name)
            .filter_by(app_id=app_id, included='yes')
            .all()
        )
        modules_data = [mod.module_name for mod in modules_data]

        # Get filter parameters with more robust defaults
        status_filter = request.args.get('status', '').strip()
        start_date = request.args.get('start_date', '').strip()
        end_date = request.args.get('end_date', '').strip()
        filter_type = request.args.get('filter_type', 'backorder_date')  # Default to backorder_date

        # Base query with app_id filtering for security
        query = (
            db_session.query(Backorder)
            .join(BackorderItem)
            .join(InventoryItemVariationLink)
            .filter(Backorder.app_id == app_id)
        )

        # Apply status filter
        if status_filter:
            query = query.filter(BackorderItem.status == status_filter)

        # Apply date filters with improved error handling
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')

                if filter_type == 'backorder_date':
                    if start_date:
                        query = query.filter(Backorder.backorder_date >= start_date)
                    if end_date:
                        # Add 1 day to include the entire end date
                        query = query.filter(Backorder.backorder_date < end_date + timedelta(days=1))
                elif filter_type == 'date_modified':
                    if start_date:
                        query = query.filter(Backorder.date_modified >= start_date)
                    if end_date:
                        query = query.filter(Backorder.date_modified < end_date + timedelta(days=1))
            except ValueError as e:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')
                logger.error(f"Date parsing error: {str(e)}")
                return redirect(url_for('backorders'))

        # Order by most recent first
        query = query.order_by(Backorder.backorder_date.desc())

        # Execute query
        backorders_data = query.all()

        return render_template(
            'backorders.html',
            backorders=backorders_data,
            modules=modules_data,
            company=company,
            role=current_user.role,
            module_name="Inventory"  # Consistent with inventory_entries pattern
        )
    except Exception as e:
        flash('An error occurred while fetching backorders', 'error')
        logger.error(f"Error in backorders route: {str(e)}")
        return redirect(url_for('inventory.inventory_dashboard'))
    finally:
        db_session.close()



@inventory_bp.route('/fulfill_backorder_item', methods=['POST'])
def fulfill_backorder_item():
    if request.method == 'POST':
        db_session = Session()
        try:
            # Get form data
            item_id = request.form.get('item_id')
            fulfilled_quantity = Decimal(request.form.get('fulfilled_quantity'))
            fulfillment_date = request.form.get('fulfillment_date')

            if fulfillment_date:
                fulfillment_date = datetime.strptime(fulfillment_date, '%Y-%m-%d').date()
            else:
                fulfillment_date = datetime.now()

            # Validate inputs
            if not item_id or not fulfilled_quantity:
                return jsonify({'message': 'Missing required fields', 'status': 'error', 'success': False})

            if fulfilled_quantity <= 0:
                return jsonify({
                    'success': False,
                    'message': 'Quantity must be greater than 0',
                    'status': 'error'
                })

            # Get the backorder item
            item = db_session.query(BackorderItem).filter_by(id=item_id).first()
            if not item:
                return jsonify({
                    'success': False,
                    'message': 'Backorder item not found',
                    'status': 'error'
                })

            # Validate quantity
            if fulfilled_quantity > item.remaining_quantity:
                return jsonify({
                    'success': False,
                    'message': f'Quantity cannot exceed remaining quantity ({item.remaining_quantity})',
                    'status': 'error'
                })

            # Update quantities
            item.fulfilled_quantity += fulfilled_quantity
            item.remaining_quantity -= fulfilled_quantity

            # Update status based on remaining quantity
            if item.remaining_quantity == 0:
                item.status = 'fulfilled'
                item.actual_fulfillment_date = datetime.now()
                flash_message = 'Backorder item fully fulfilled'
            else:
                item.status = 'partially_fulfilled'
                flash_message = 'Backorder item partially fulfilled'

            # Create fulfillment record
            fulfillment = BackorderFulfillment(
                backorder_item_id=item.id,
                fulfillment_quantity=fulfilled_quantity,
                fulfillment_date=fulfillment_date,
                app_id=item.app_id
            )
            db_session.add(fulfillment)

            # Update parent backorder status if needed
            backorder = item.backorders
            if backorder:
                # Check if all items are fulfilled
                all_fulfilled = all(
                    i.status == 'fulfilled' for i in backorder.backorder_items
                )
                if all_fulfilled:
                    backorder.status = 'fulfilled'
                    backorder.actual_fulfillment_date = datetime.now()
                    flash_message = 'Backorder fully fulfilled'
                else:
                    # Check if any items are fulfilled or partially fulfilled
                    has_fulfillment = any(
                        i.status in ('fulfilled', 'partially_fulfilled')
                        for i in backorder.backorder_items
                    )
                    if has_fulfillment:
                        backorder.status = 'partially_fulfilled'

            db_session.commit()
            return jsonify({
                'success': True,
                'message': f'{flash_message}',
                'status': 'success'
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f"Error fulfilling backorder item: {str(e)}")
            return jsonify({'success': False,
                            'message': 'An error occurred while fulfilling the backorder item',
                            'status': 'error'
                            })


@inventory_bp.route('/discontinue_backorder_item/<int:item_id>', methods=['POST'])
def discontinue_backorder_item(item_id):
    db_session = Session()
    try:
        # Get the backorder item
        item = db_session.query(BackorderItem).filter_by(id=item_id).first()
        if not item:
            return jsonify({'success': False, 'message': 'Backorder item not found'}), 404

        # Update status to discontinued
        item.status = 'discontinued'
        item.remaining_quantity = 0  # Set remaining quantity to 0

        # Update parent backorder status if needed
        backorder = item.backorders
        if backorder:
            # Check if all items are discontinued or fulfilled
            all_completed = all(
                i.status in ('discontinued', 'fulfilled')
                for i in backorder.backorder_items
            )
            if all_completed:
                backorder.status = 'fulfilled'
                backorder.actual_fulfillment_date = datetime.datetime.now()

        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Backorder item discontinued successfully',
            'data': {
                'item_id': item.id,
                'status': item.status
            }
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error discontinuing backorder item: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while discontinuing the backorder item'
        }), 500


@inventory_bp.route('/cancel_backorder_item/<int:item_id>', methods=['POST'])
def cancel_backorder_item(item_id):
    db_session = Session()
    try:

        # Get the backorder item
        item = db_session.query(BackorderItem).filter_by(id=item_id).first()
        if not item:
            return jsonify({'success': False, 'message': 'Backorder item not found'}), 404

        # Update status to canceled
        item.status = 'canceled'

        # Update parent backorder status if needed
        backorder = item.backorders
        if backorder:
            # Check if all items are canceled or fulfilled
            all_completed = all(
                i.status in ('canceled', 'fulfilled')
                for i in backorder.backorder_items
            )
            if all_completed:
                backorder.status = 'canceled'

        db_session.commit()

        flash('Backorder item canceled successfully', 'success')
        return jsonify({'success': True, 'message': 'Backorder item canceled successfully'})

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error canceling backorder item: {str(e)}")
        flash('An error occurred while canceling the backorder item', 'error')
        return jsonify({'success': False, 'message': 'An error occurred while canceling the backorder item'}), 500
