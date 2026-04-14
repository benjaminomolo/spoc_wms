# app/routes/inventory/inventory_management.py
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
    SalesOrderItem, QuotationItem, SalesInvoiceItem, JournalEntry, UserLocationAssignment, InventorySummary
import logging

from services.inventory_helpers import handle_supplier_logic, \
    render_inventory_entry_form, \
    process_inventory_entries, reverse_inventory_entry, get_inventory_entry_with_details, \
    render_edit_inventory_entry_form, process_inventory_entries_for_edit, remove_inventory_journal_entries
from utils import ensure_default_location, generate_unique_lot, create_notification, empty_to_none, \
    validate_quantity_and_selling_price, validate_quantity_and_price, handle_batch_variation_update
from utils_and_helpers.amounts_utils import format_amount
from utils_and_helpers.cache_keys import stock_history_cache_key
from utils_and_helpers.cache_utils import on_inventory_data_changed, clear_stock_history_cache
from utils_and_helpers.forms import get_locked_record
from . import inventory_bp
from .post_to_ledger import post_inventory_entry_to_ledger

logger = logging.getLogger(__name__)


@inventory_bp.route('/inventory_entry', methods=['GET', 'POST'])
@login_required
@role_required(['Admin', 'Supervisor', 'Contributor'])
def inventory_entry():
    db_session = None
    rate_id = None
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
                movement_type = request.form.get('movement_type', 'in')
                source_type = request.form.get('source_type')
                transaction_date = datetime.strptime(request.form['transaction_date'], '%Y-%m-%d').date()

                # Handle location based on movement type
                location = None
                from_location = None
                to_location = None

                if movement_type == 'transfer':
                    # For transfers, we have separate from and to locations
                    from_location = request.form.get('location')
                    to_location = request.form.get('to_location')
                    source_type = "transfer"
                    if not from_location or not to_location:
                        flash("Both from and to locations are required for transfers", "error")
                        return redirect(request.referrer)
                elif movement_type in ['adjustment', 'missing', 'expired', 'damaged', 'opening_balance']:
                    # For adjustments and missing, we only need the location being adjusted
                    location = request.form.get('location')

                    if movement_type == "missing":
                        source_type = "missing"

                    elif movement_type == 'expired':
                        source_type = 'expired'

                    elif movement_type == 'damaged':
                        source_type = 'damaged'

                    elif movement_type == 'opening_balance':
                        source_type = 'opening_balance'

                    else:
                        source_type = 'adjustment'

                    if not location:
                        location = ensure_default_location(db_session=db_session, app_id=app_id)
                else:
                    # For in/out movements, use the single location field
                    location = request.form.get('location')

                    if movement_type == 'stock_out_sale':
                        source_type = 'sale'

                    if movement_type == 'stock_out_write_off':
                        source_type = 'write_off'

                    if not location:
                        location = ensure_default_location(db_session=db_session, app_id=app_id)

                # Handle supplier logic (not needed for adjustments/missing)
                supplier_id = None
                if movement_type not in ['adjustment', 'missing', 'expired', 'damaged', 'opening_balance']:
                    supplier_id = handle_supplier_logic(request.form, app_id, db_session)

                project_id = request.form.get('project_id') or None
                expiration_date_str = request.form.get('expiration_date')
                expiration_date = datetime.strptime(expiration_date_str,
                                                    '%Y-%m-%d').date() if expiration_date_str else None

                # Get currency
                currency_val = request.form.get('currency')

                if currency_val and currency_val.strip() != "":
                    form_currency_id = int(currency_val)

                else:
                    form_currency_id = base_currency_id

                reference_str = request.form.get('reference')
                reference = empty_to_none(reference_str)

                write_off_reason_str = request.form.get('write_off_reason')
                write_off_reason = empty_to_none(write_off_reason_str)

                # Soft uniqueness check
                if reference:
                    existing_ref = db_session.query(InventoryEntry).filter_by(
                        app_id=app_id,
                        reference=reference
                    ).first()
                    if existing_ref:
                        # Warn the user but allow override
                        flash(
                            f"A previous inventory entry already used reference '{reference}'. You can still proceed.",
                            "warning")

                # get Accounting Information
                payable_account_str = request.form.get('payable_account')
                payable_account_id = empty_to_none(payable_account_str)
                write_off_account_str = request.form.get('adjustment_account')
                write_off_account_id = empty_to_none(write_off_account_str)
                adjustment_account_str = request.form.get('adjustment_account')
                adjustment_account_id = empty_to_none(adjustment_account_str)
                sales_account_str = request.form.get('sales_account')
                sales_account_id = empty_to_none(sales_account_str)

                # Process line items
                inventory_items = request.form.getlist('inventory_item[]')
                quantities = request.form.getlist('quantity[]')
                unit_prices = request.form.getlist('unit_price[]')
                selling_prices = request.form.getlist(
                    'selling_price[]') if movement_type != 'stock_out_sale' else unit_prices

                # Get system quantities and adjustments for adjustment movements
                system_quantities = request.form.getlist('system_quantity[]') if movement_type == 'adjustment' else None
                adjustments = request.form.getlist('adjustment[]') if movement_type == 'adjustment' else None

                if not inventory_items:
                    raise ValueError("At least one inventory item is required")

                # Process inventory entries
                inventory_ent = process_inventory_entries(
                    db_session=db_session,
                    app_id=app_id,
                    inventory_items=inventory_items,
                    quantities=quantities,
                    unit_prices=unit_prices,
                    selling_prices=selling_prices,
                    location=location,
                    from_location=from_location,
                    to_location=to_location,
                    transaction_date=transaction_date,
                    supplier_id=supplier_id,
                    form_currency_id=form_currency_id,
                    base_currency_id=base_currency_id,
                    expiration_date=expiration_date,
                    reference=reference,
                    write_off_reason=write_off_reason,
                    project_id=project_id,
                    movement_type=movement_type,
                    current_user_id=current_user.id,
                    source_type=source_type,
                    system_quantities=system_quantities,
                    adjustments=adjustments,
                    payable_account_id=payable_account_id,
                    write_off_account_id=write_off_account_id,
                    adjustment_account_id=adjustment_account_id,
                    sales_account_id=sales_account_id,
                    is_posted_to_ledger=True

                )

                db_session.commit()

                # Invalidate cache after modification
                # In your inventory_entry route after successful processing:
                try:
                    # Clear all stock history cache - no need for complex logic
                    clear_stock_history_cache()
                    current_app.logger.info("Successfully cleared stock history cache after inventory entry")
                except Exception as e:
                    current_app.logger.error(f"Cache clearing failed: {e}")
                    # Continue anyway - cache clearing failure shouldn't break the operation

                return jsonify({'status': 'success', 'message': 'Inventory added successfully!'}), 200

            except ValueError as e:
                db_session.rollback()
                logger.error(f"Validation error: {str(e)}\n{traceback.format_exc()}")
                return jsonify({'status': 'error', 'message': f'A validation error has occurred! {str(e)}'}), 400

            except Exception as e:
                db_session.rollback()
                logger.error(f"Error processing inventory entry: {str(e)}\n{traceback.format_exc()}")
                return jsonify({'status': 'error', 'message': f'Error processing inventory entry {str(e)}'}), 500

        else:
            # GET request - render form
            # GET request - Get movement_type from URL parameter
            movement_type = request.args.get('movement_type', 'in')

            allowed_movement_types = ['in', 'stock_out_sale', 'transfer', 'adjustment',
                                      'missing', 'expired', 'damaged', 'opening_balance',
                                      'stock_out_write_off']

            logger.info(f'Movement types are {movement_type}')

            if movement_type not in allowed_movement_types:
                movement_type = 'in'

            # Pass movement_type to the form template
            return render_inventory_entry_form(
                db_session, app_id, company, role, modules_data,
                movement_type=movement_type  # Add this parameter
            )

    except Exception as e:
        if db_session:
            db_session.rollback()
        logger.error(f"Unexpected error in inventory_entry: {str(e)}\n{traceback.format_exc()}")
        flash(f'An unexpected error occurred: {str(e)}', 'error')
        return redirect(request.referrer)
    finally:
        if db_session:
            db_session.close()


@inventory_bp.route('/stock_movement_history')
@login_required
def stock_movement_history():
    """
    Render the stock movement history page
    """
    db_session = Session()
    app_id = current_user.app_id

    company = db_session.query(Company).filter_by(id=app_id).first()
    # Get base currency PROPERLY - query it directly
    base_currency = db_session.query(Currency).filter_by(
        app_id=app_id,
        currency_index=1
    ).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
    try:
        # Fetch all necessary data for the template filters

        inventory_items = db_session.query(InventoryItemVariationLink).join(InventoryItem).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.status == "active"
        ).order_by(InventoryItem.item_name.asc()).all()

        locations = db_session.query(InventoryLocation).filter_by(app_id=current_user.app_id).all()

        return render_template(
            'inventory/stock_movement_history.html',
            items=inventory_items,
            locations=locations,
            company=company,
            modules=modules_data,
            role=role,
            base_currency=base_currency
        )

    except Exception as e:
        logger.error(f"Error rendering stock movement history page: {str(e)}\n{traceback.format_exc()}")
        return "An error occurred while loading the stock movement history page", 500

    finally:
        db_session.close()


@inventory_bp.route('/api/stock_movement_history', methods=['GET'])
@login_required
@cached_route(timeout=300, key_func=stock_history_cache_key)
def api_stock_movement_history():
    """
    Return JSON data of stock movement history with filtering, running totals, and base currency conversion
    Using the new InventoryTransactionDetail model
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get pagination and filter parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        item_id = request.args.get('item_id', type=int)
        reference = request.args.get('reference', '')
        ledger_status = request.args.get('ledger_status', '')
        location_id = request.args.get('location_id', type=int)
        movement_type = request.args.get('movement_type', '')
        source_type = request.args.get('source_type', '')
        start_date = request.args.get('start_date', '')
        end_date = request.args.get('end_date', '')

        # Get base currency information
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({
                'success': False,
                'message': 'Base currency not configured for this company'
            }), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency_code = base_currency_info["base_currency"]

        # Build base query with joins - use InventoryTransactionDetail instead of BatchVariationLink
        query = db_session.query(InventoryTransactionDetail) \
            .join(
            InventoryEntryLineItem,
            InventoryTransactionDetail.inventory_entry_line_item_id == InventoryEntryLineItem.id
        ).join(
            InventoryEntry,
            InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
        ).join(
            InventoryItemVariationLink,
            InventoryTransactionDetail.item_id == InventoryItemVariationLink.id
        ).outerjoin(
            ExchangeRate,
            InventoryTransactionDetail.exchange_rate_id == ExchangeRate.id
        ).options(
            joinedload(InventoryTransactionDetail.inventory_entry_line_item)
            .joinedload(InventoryEntryLineItem.inventory_entry)
            .joinedload(InventoryEntry.created_user),
            joinedload(InventoryTransactionDetail.inventory_entry_line_item)
            .joinedload(InventoryEntryLineItem.inventory_entry)
            .joinedload(InventoryEntry.currency),
            joinedload(InventoryTransactionDetail.currency),
            joinedload(InventoryTransactionDetail.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item),
            joinedload(InventoryTransactionDetail.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item_attributes),
            joinedload(InventoryTransactionDetail.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item_variation),
            joinedload(InventoryTransactionDetail.location)
        ).filter(InventoryTransactionDetail.app_id == app_id)

        # Apply filters
        if item_id:
            query = query.filter(InventoryTransactionDetail.item_id == item_id)

        if location_id:
            query = query.filter(InventoryTransactionDetail.location_id == location_id)

        if movement_type:
            query = query.filter(InventoryTransactionDetail.movement_type == movement_type)

        # Apply reference filter - change from exact match to partial search
        if reference:
            query = query.filter(InventoryEntry.reference.ilike(f'%{reference}%'))

        # Apply ledger status filter - exclude transfers entirely from this filter
        if ledger_status == 'posted':
            query = query.filter(
                InventoryTransactionDetail.is_posted_to_ledger == True,
                InventoryEntry.inventory_source != 'transfer'
            )
        elif ledger_status == 'not_posted':
            query = query.filter(
                InventoryTransactionDetail.is_posted_to_ledger == False,
                InventoryEntry.inventory_source != 'transfer'
            )

        if source_type:
            query = query.filter(InventoryEntry.source_type == source_type)

        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
                query = query.filter(func.date(InventoryTransactionDetail.transaction_date) >= start_date_obj)
            except ValueError:
                pass

        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
                query = query.filter(func.date(InventoryTransactionDetail.transaction_date) <= end_date_obj)
            except ValueError:
                pass

        # Get total count
        total_items = query.count()

        # Apply pagination and ordering - NEWEST FIRST for display
        total_pages = (total_items + per_page - 1) // per_page

        # Get all transactions for running totals calculation (in chronological order)
        all_transactions_query = query.order_by(
            InventoryTransactionDetail.transaction_date.asc(),
            InventoryTransactionDetail.id.asc()
        )
        all_transactions = all_transactions_query.all()

        # Calculate running totals for each item variation (in chronological order)
        running_totals = {}
        running_values_base = {}
        transaction_running_totals = {}
        transaction_running_values_base = {}

        for transaction in all_transactions:
            item_variation_key = transaction.item_id

            # Initialize running totals for this item variation if not exists
            if item_variation_key not in running_totals:
                running_totals[item_variation_key] = 0
                running_values_base[item_variation_key] = Decimal('0.00')

            # Calculate quantity impact
            quantity_impact = transaction.quantity

            # Calculate value impact in base currency
            unit_price = Decimal(str(transaction.unit_cost)) if transaction.unit_cost else Decimal('0.00')
            value_impact = Decimal('0.00')

            if quantity_impact != 0 and unit_price > 0:
                # Use the exchange rate from the transaction detail
                if transaction.currency_id != base_currency_id and transaction.exchange_rate:
                    exchange_rate = Decimal(str(transaction.exchange_rate.rate))
                    unit_price_base = unit_price * exchange_rate
                else:
                    unit_price_base = unit_price  # Same currency or no exchange rate

                value_impact = Decimal(str(abs(quantity_impact))) * unit_price_base

                # For out movements, value impact is negative
                if quantity_impact < 0:
                    value_impact = -value_impact

            # Update running totals
            running_totals[item_variation_key] += quantity_impact
            running_values_base[item_variation_key] += value_impact

            # Store the running totals at the time of this transaction
            transaction_running_totals[transaction.id] = running_totals[item_variation_key]
            transaction_running_values_base[transaction.id] = running_values_base[item_variation_key]

        # Now get the paginated results in DESCENDING order (newest first)
        transaction_details = query.order_by(
            InventoryTransactionDetail.transaction_date.desc(),  # CHANGED TO DESC
            InventoryTransactionDetail.id.desc()  # CHANGED TO DESC
        ).offset((page - 1) * per_page).limit(per_page).all()

        # Serialize transaction details for the current page
        entries_data = []
        total_value_base = Decimal('0.00')

        for transaction in transaction_details:
            line_item = transaction.inventory_entry_line_item
            entry = line_item.inventory_entry if line_item else None

            # Get location name
            location_name = transaction.location.location if transaction.location else None

            # Get item details
            variation_link = transaction.inventory_item_variation_link
            item = variation_link.inventory_item if variation_link else None
            attribute = variation_link.inventory_item_attributes if variation_link else None
            variation = variation_link.inventory_item_variation if variation_link else None

            # Get the pre-calculated running totals for this transaction
            running_total = transaction_running_totals.get(transaction.id, 0)
            running_value_base = float(transaction_running_values_base.get(transaction.id, Decimal('0.00')))

            # Calculate current transaction value in base currency
            unit_price = Decimal(str(transaction.unit_cost)) if transaction.unit_cost else Decimal('0.00')
            line_total = Decimal(str(abs(transaction.quantity))) * unit_price if transaction.quantity else Decimal(
                '0.00')

            # Use the exchange rate from the transaction
            if transaction.currency_id != base_currency_id and transaction.exchange_rate:
                exchange_rate = Decimal(str(transaction.exchange_rate.rate))
                line_total_base = line_total * exchange_rate
            else:
                line_total_base = line_total  # Same currency or no exchange rate

            total_value_base += line_total_base

            entries_data.append({
                'id': transaction.id,
                'entry_id': entry.id if entry else None,
                'inventory_source': entry.inventory_source,
                'line_item_id': line_item.id if line_item else None,
                'item_name': item.item_name if item else 'Unknown Item',
                'item_code': item.item_code if item else '',
                'uom': item.unit_of_measurement.abbreviation,
                'attribute': attribute.attribute_name if attribute else '',
                'variation': variation.variation_name if variation else '',
                'movement_type': transaction.movement_type,
                'source_type': entry.source_type if entry else 'Manual',
                'source_id': entry.source_id if entry else 'None',
                'reference_number': entry.reference if entry else '',
                'quantity': transaction.quantity,
                'running_total': running_total,
                'running_value_base': running_value_base,
                'unit_price': float(unit_price),
                'unit_price_base': float(line_total_base / Decimal(
                    str(abs(transaction.quantity)))) if transaction.quantity and line_total_base else 0.0,
                'total_amount': float(line_total),
                'total_amount_base': format_amount(float(line_total_base)),
                'currency_id': transaction.currency_id,
                'currency_code': transaction.currency.user_currency if transaction.currency else '',
                'exchange_rate_id': transaction.exchange_rate_id,
                'exchange_rate': float(transaction.exchange_rate.rate) if transaction.exchange_rate else 1.0,
                'base_currency_id': base_currency_id,
                'base_currency_code': base_currency_code,
                'location': location_name,
                'location_id': transaction.location_id,
                'from_location': entry.from_inventory_location.location if entry.from_inventory_location else '',
                'to_location': entry.to_inventory_location.location if entry.to_inventory_location else '',
                'transaction_date': transaction.transaction_date.strftime('%Y-%m-%d'),
                'created_at': transaction.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                'created_by': entry.created_user.name if entry and entry.created_user else 'Unknown',
                'vendor_id': transaction.vendor_id,
                'project_id': transaction.project_id,
                'is_posted_to_ledger': transaction.is_posted_to_ledger
            })

        # No need to sort again since we already queried in descending order
        # entries_data is already in the correct order (newest first)

        return jsonify({
            'success': True,
            'entries': entries_data,
            'summary': {
                'total_value_base': float(total_value_base),
                'base_currency_id': base_currency_id,
                'base_currency_code': base_currency_code,
                'total_entries': total_items
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
        logger.error(f"Error in api_stock_movement_history: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching stock movement history'
        }), 500

    finally:
        db_session.close()


@inventory_bp.route('/inventory_entry/<int:entry_id>', methods=['GET'])
@login_required
def inventory_entry_detail(entry_id):
    """
    View inventory entry details (read-only view)
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get the inventory entry with all related data
        entry = db_session.query(InventoryEntry).filter_by(
            id=entry_id,
            app_id=app_id
        ).options(
            joinedload(InventoryEntry.line_items)
            .joinedload(InventoryEntryLineItem.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item)
            .joinedload(InventoryItem.unit_of_measurement),
            joinedload(InventoryEntry.line_items)
            .joinedload(InventoryEntryLineItem.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item_attributes),
            joinedload(InventoryEntry.line_items)
            .joinedload(InventoryEntryLineItem.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item_variation),
            joinedload(InventoryEntry.created_user),
            joinedload(InventoryEntry.updated_user),
            joinedload(InventoryEntry.from_inventory_location),
            joinedload(InventoryEntry.to_inventory_location),
            joinedload(InventoryEntry.vendor),
            joinedload(InventoryEntry.project),
            joinedload(InventoryEntry.currency),
            joinedload(InventoryEntry.exchange_rate),
            joinedload(InventoryEntry.payable_account),
            joinedload(InventoryEntry.write_off_account),
            joinedload(InventoryEntry.adjustment_account),
            joinedload(InventoryEntry.sales_account)
        ).first()

        if not entry:
            flash('Inventory entry not found', 'error')
            return redirect(url_for('inventory.stock_movement_history'))

        # Get transaction details for this entry
        transaction_details = db_session.query(InventoryTransactionDetail).filter(
            InventoryTransactionDetail.inventory_entry_line_item_id.in_(
                [item.id for item in entry.line_items]
            )
        ).options(
            joinedload(InventoryTransactionDetail.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item),
            joinedload(InventoryTransactionDetail.location)
        ).all()

        # Get company and other context data
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        return render_template(
            'inventory/inventory_entry_details.html',
            entry=entry,
            company=company,
            modules=modules_data,
            role=current_user.role,
            transaction_details=transaction_details,
            now=datetime.now()
        )

    except Exception as e:
        logger.error(f"Error viewing inventory entry {entry_id}: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the inventory entry', 'error')
        return redirect(url_for('inventory.stock_movement_history'))

    finally:
        db_session.close()


@inventory_bp.route('/inventory_entry/<int:entry_id>/edit', methods=['GET', 'POST'])
@login_required
@role_required(['Admin', 'Supervisor', 'Contributor'])
def edit_inventory_entry(entry_id):
    db_session = None
    try:
        db_session = Session()
        app_id = current_user.app_id

        # Get base currency
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

        # Get the existing inventory entry with details
        inventory_entry = get_inventory_entry_with_details(db_session, entry_id, app_id)

        if not inventory_entry:
            flash("Inventory entry not found", "error")
            return redirect(url_for('inventory.stock_movement_history'))

        if request.method == 'POST':
            try:
                logger.info(f'Front end form data is {request.form}')
                form_version = request.form.get('version', type=int)
                inventory_entry = get_inventory_entry_with_details(db_session, entry_id, app_id, lock=True)
                # ✅ OPTIMISTIC LOCKING: Check version before proceeding
                if form_version is None or form_version != inventory_entry.version:
                    return jsonify({
                        'status': 'error',
                        'message': 'This record has been modified by another user. Please refresh and try again.',
                        'code': 'VERSION_CONFLICT'
                    }), 409

                # Extract form data (same as your original inventory_entry function)
                movement_type = request.form.get('movement_type', inventory_entry.stock_movement)
                source_type = request.form.get('source_type', inventory_entry.source_type)
                transaction_date = datetime.strptime(request.form['transaction_date'], '%Y-%m-%d').date()

                # Handle location based on movement type
                location = None
                from_location = None
                to_location = None

                if movement_type == 'transfer':
                    from_location = request.form.get('location')
                    to_location = request.form.get('to_location')
                    source_type = "transfer"
                    if not from_location or not to_location:
                        flash("Both from and to locations are required for transfers", "error")
                        return redirect(request.referrer)
                elif movement_type in ['adjustment', 'missing', 'expired', 'damaged', 'opening_balance']:
                    location = request.form.get('location')
                    if movement_type == "missing":
                        source_type = "missing"
                    elif movement_type == 'expired':
                        source_type = 'expired'
                    elif movement_type == 'damaged':
                        source_type = 'damaged'
                    elif movement_type == 'opening_balance':
                        source_type = 'opening_balance'
                    else:
                        source_type = 'adjustment'
                    if not location:
                        location = ensure_default_location(db_session=db_session, app_id=app_id)
                else:
                    location = request.form.get('location')
                    if movement_type == 'stock_out_sale':
                        source_type = 'sale'

                    if movement_type == 'stock_out_write_off':
                        source_type = 'write_off'

                    if not location:
                        location = ensure_default_location(db_session=db_session, app_id=app_id)

                # Handle supplier logic
                supplier_id = None
                if movement_type not in ['adjustment', 'missing', 'expired', 'damaged', 'opening_balance']:
                    supplier_id = handle_supplier_logic(request.form, app_id, db_session)

                project_id = request.form.get('project_id') or None
                expiration_date_str = request.form.get('expiration_date')
                expiration_date = datetime.strptime(expiration_date_str,
                                                    '%Y-%m-%d').date() if expiration_date_str else None

                # Get currency
                currency_val = request.form.get('currency')
                if currency_val and currency_val.strip() != "":
                    form_currency_id = int(currency_val)
                else:
                    form_currency_id = base_currency_id

                reference_str = request.form.get('reference')
                reference = empty_to_none(reference_str)

                write_off_reason_str = request.form.get('write_off_reason')
                write_off_reason = empty_to_none(write_off_reason_str)

                # Check reference uniqueness (excluding current entry)
                if reference and reference != inventory_entry.reference:
                    existing_ref = db_session.query(InventoryEntry).filter(
                        InventoryEntry.app_id == app_id,
                        InventoryEntry.reference == reference,
                        InventoryEntry.id != entry_id
                    ).first()
                    if existing_ref:
                        flash(f"Reference '{reference}' is already used by another entry", "warning")

                # Get Accounting Information
                payable_account_str = request.form.get('payable_account')
                payable_account_id = empty_to_none(payable_account_str)
                write_off_account_str = request.form.get('adjustment_account')
                write_off_account_id = empty_to_none(write_off_account_str)
                adjustment_account_str = request.form.get('adjustment_account')
                adjustment_account_id = empty_to_none(adjustment_account_str)
                sales_account_str = request.form.get('sales_account')
                sales_account_id = empty_to_none(sales_account_str)

                # Process line items
                inventory_items = request.form.getlist('inventory_item[]')
                quantities = request.form.getlist('quantity[]')
                unit_prices = request.form.getlist('unit_price[]')
                selling_prices = request.form.getlist('selling_price[]') if movement_type != 'out' else unit_prices

                # Get system quantities and adjustments for adjustment movements
                system_quantities = request.form.getlist('system_quantity[]') if movement_type == 'adjustment' else None
                adjustments = request.form.getlist('adjustment[]') if movement_type == 'adjustment' else None

                if not inventory_items:
                    raise ValueError("At least one inventory item is required")

                # First, reverse the original transaction
                reverse_inventory_entry(db_session, inventory_entry)

                # Update the inventory entry header with new values
                inventory_entry.stock_movement = movement_type
                inventory_entry.source_type = source_type
                inventory_entry.transaction_date = transaction_date
                inventory_entry.supplier_id = supplier_id
                inventory_entry.project_id = project_id
                inventory_entry.expiration_date = expiration_date
                inventory_entry.currency_id = form_currency_id
                inventory_entry.reference = reference
                inventory_entry.notes = write_off_reason
                inventory_entry.payable_account_id = payable_account_id
                inventory_entry.write_off_account_id = write_off_account_id
                inventory_entry.adjustment_account_id = adjustment_account_id
                inventory_entry.sales_account_id = sales_account_id
                inventory_entry.updated_by = current_user.id
                inventory_entry.updated_at = datetime.now()

                # Then process the new entries
                updated_inventory_entry = process_inventory_entries_for_edit(
                    db_session=db_session,
                    app_id=app_id,
                    inventory_items=inventory_items,
                    quantities=quantities,
                    unit_prices=unit_prices,
                    selling_prices=selling_prices,
                    location=location,
                    from_location=from_location,
                    to_location=to_location,
                    transaction_date=transaction_date,
                    supplier_id=supplier_id,
                    form_currency_id=form_currency_id,
                    base_currency_id=base_currency_id,
                    expiration_date=expiration_date,
                    reference=reference,
                    project_id=project_id,
                    movement_type=movement_type,
                    current_user_id=current_user.id,
                    source_type=source_type,
                    system_quantities=system_quantities,
                    adjustments=adjustments,
                    payable_account_id=payable_account_id,
                    write_off_account_id=write_off_account_id,
                    adjustment_account_id=adjustment_account_id,
                    sales_account_id=sales_account_id,
                    existing_entry=inventory_entry,
                    posted_status=True
                )

                db_session.flush()
                db_session.refresh(inventory_entry)  # reload from DB
                updated_inventory_entry = inventory_entry

                # === CRITICAL: Add this after updating inventory entry header ===
                inventory_entry.status = 'draft'  # Reset status
                inventory_entry.is_posted_to_ledger = True

                inventory_entry.version += 1
                db_session.commit()

                # Clear cache after modification
                try:
                    clear_stock_history_cache()
                    current_app.logger.info("Successfully cleared stock history cache after inventory entry edit")
                except Exception as e:
                    current_app.logger.error(f"Cache clearing failed: {e}")

                return jsonify({'status': 'success', 'message': 'Inventory entry updated successfully!'}), 200

            except ValueError as e:
                db_session.rollback()
                logger.error(f"Validation error: {str(e)}\n{traceback.format_exc()}")
                return jsonify({'status': 'error', 'message': f'A validation error has occurred! {str(e)}'}), 400

            except Exception as e:
                db_session.rollback()
                logger.error(f"Error processing inventory entry edit: {str(e)}\n{traceback.format_exc()}")
                return jsonify({'status': 'error', 'message': f'Error processing inventory entry edit {str(e)}'}), 500

        else:
            # GET request - render edit form with existing data
            return render_edit_inventory_entry_form(
                db_session, app_id, company, role, modules_data, inventory_entry
            )


    except Exception as e:
        if db_session:
            db_session.rollback()
        logger.error(f"Unexpected error in edit_inventory_entry: {str(e)}\n{traceback.format_exc()}")
        flash(f'An unexpected error occurred: {str(e)}', 'error')
        return redirect(request.referrer)
    finally:
        if db_session:
            db_session.close()


@inventory_bp.route('/manage_attributes_variations')
@role_required(['Admin', 'Supervisor'])
def manage_attributes_variations():
    try:
        app_id = current_user.app_id

        with Session() as db_session:
            role = current_user.role
            company = db_session.query(Company).filter_by(id=app_id).first()
            modules_data = [mod.module_name for mod in
                            db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
            module_name = "Inventory"
            attributes = db_session.query(InventoryItemAttribute).options(
                joinedload(InventoryItemAttribute.inventory_item_variation_link)
            ).filter_by(app_id=app_id).all()
            variations = db_session.query(InventoryItemVariation).options(
                joinedload(InventoryItemVariation.inventory_item_variation_link)
                .joinedload(InventoryItemVariationLink.inventory_item_attributes),
                joinedload(InventoryItemVariation.inventory_item_attributes)
            ).filter_by(app_id=app_id).all()

        return render_template(
            'inventory/manage_attributes_variations.html',
            attributes=attributes,
            variations=variations,
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name
        )

    except Exception as e:
        logger.error(f"Error loading attributes/variations: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the inventory attributes.', 'danger')

        # Use referrer to redirect back if available, otherwise fallback
        return redirect(request.referrer or url_for('inventory.inventory_dashboard'))


@inventory_bp.route('/manage_categories_subcategories')
@role_required(['Admin', 'Contributor'])
def manage_categories_subcategories():
    try:
        app_id = current_user.app_id

        with Session() as db_session:
            role = current_user.role
            company = db_session.query(Company).filter_by(id=app_id).first()

            # Fetch modules
            modules_data = [mod.module_name for mod in
                            db_session.query(Module)
                            .filter_by(app_id=app_id)
                            .filter_by(included='yes')
                            .all()]

            module_name = "Inventory"

            # ✅ Fetch all categories
            categories = db_session.query(InventoryCategory).options(
                joinedload(InventoryCategory.inventory_subcategory)
            ).filter_by(app_id=app_id).all()

            # ✅ Fetch all subcategories with their linked category
            subcategories = db_session.query(InventorySubCategory).options(
                joinedload(InventorySubCategory.inventory_category)
            ).filter_by(app_id=app_id).all()

        return render_template(
            'inventory/manage_categories_subcategories.html',
            categories=categories,
            subcategories=subcategories,
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name
        )

    except Exception as e:
        logger.error(f"Error loading categories/subcategories: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the inventory categories.', 'danger')

        # Use referrer to redirect back if available, otherwise fallback
        return redirect(request.referrer or url_for('inventory.inventory_dashboard'))


@inventory_bp.route('/add_inventory_category', methods=['POST'])
@login_required
def add_inventory_category():
    db_session = Session()
    try:
        app_id = current_user.app_id
        category_name = request.form.get('category_name').strip()

        new_inventory_category = InventoryCategory(
            category_name=category_name,
            app_id=app_id
        )
        db_session.add(new_inventory_category)
        db_session.commit()

        return jsonify({
            'id': new_inventory_category.id,
            'category_name': category_name
        })
    finally:
        db_session.close()  # Ensure that the session is closed


@inventory_bp.route('/add_inventory_subcategory', methods=['POST'])
@login_required
def add_inventory_subcategory():
    db_session = Session()
    try:
        app_id = current_user.app_id
        subcategory_name = request.form.get('subcategory_name').strip()
        category_id = request.form.get('category_id')

        new_inventory_subcategory = InventorySubCategory(
            item_category_id=category_id,
            subcategory_name=subcategory_name,
            app_id=app_id
        )
        db_session.add(new_inventory_subcategory)
        db_session.commit()

        return jsonify({
            'id': new_inventory_subcategory.id,
            'subcategory_name': subcategory_name,
            'category_id': category_id
        })
    finally:
        db_session.close()  # Ensure that the session is closed


@inventory_bp.route('/edit_inventory_category/<int:category_id>', methods=['POST'])
@login_required
def edit_inventory_category(category_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        category_name = request.form.get('category_name').strip()

        category = db_session.query(InventoryCategory).filter_by(
            id=category_id,
            app_id=app_id
        ).first()

        if not category:
            return jsonify({'error': 'Category not found'}), 404

        category.category_name = category_name
        db_session.commit()

        return jsonify({
            'id': category.id,
            'category_name': category.category_name
        })
    finally:
        db_session.close()


@inventory_bp.route('/edit_inventory_subcategory/<int:subcategory_id>', methods=['POST'])
@login_required
def edit_inventory_subcategory(subcategory_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        subcategory_name = request.form.get('subcategory_name').strip()
        category_id = request.form.get('category_id')

        subcategory = db_session.query(InventorySubCategory).filter_by(
            id=subcategory_id,
            app_id=app_id
        ).first()

        if not subcategory:
            return jsonify({'error': 'Subcategory not found'}), 404

        subcategory.subcategory_name = subcategory_name
        subcategory.item_category_id = category_id
        db_session.commit()

        return jsonify({
            'id': subcategory.id,
            'subcategory_name': subcategory.subcategory_name,
            'category_id': subcategory.item_category_id
        })
    finally:
        db_session.close()


@inventory_bp.route('/delete_inventory_category/<int:category_id>', methods=['POST'])
@login_required
def delete_inventory_category(category_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        category = db_session.query(InventoryCategory).filter_by(
            id=category_id,
            app_id=app_id
        ).first()

        if not category:
            return jsonify({'error': 'Category not found'}), 404

        # ✅ Check if category has linked subcategories
        subcategory_count = db_session.query(InventorySubCategory).filter_by(
            item_category_id=category.id,
            app_id=app_id
        ).count()

        if subcategory_count > 0:
            return jsonify({
                'error': f'Cannot delete. This category has {subcategory_count} linked subcategory(ies).'
            }), 400

        # ✅ Check if category has directly linked items
        item_count = db_session.query(InventoryItem).filter_by(
            item_category_id=category.id,
            app_id=app_id
        ).count()

        if item_count > 0:
            return jsonify({
                'error': f'Cannot delete. This category is linked to {item_count} inventory item(s).'
            }), 400

        db_session.delete(category)
        db_session.commit()

        return jsonify({'success': True, 'message': 'Category deleted successfully.'})
    finally:
        db_session.close()


@inventory_bp.route('/delete_inventory_subcategory/<int:subcategory_id>', methods=['POST'])
@login_required
def delete_inventory_subcategory(subcategory_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        subcategory = db_session.query(InventorySubCategory).filter_by(
            id=subcategory_id,
            app_id=app_id
        ).first()

        if not subcategory:
            return jsonify({'error': 'Subcategory not found'}), 404

        # ✅ Check if any items are linked to this subcategory
        linked_items_count = db_session.query(InventoryItem).filter_by(
            item_subcategory_id=subcategory.id,
            app_id=app_id
        ).count()

        if linked_items_count > 0:
            return jsonify({
                'error': f'Cannot delete. This subcategory is linked to {linked_items_count} inventory item(s).'
            }), 400

        db_session.delete(subcategory)
        db_session.commit()

        return jsonify({'success': True, 'message': 'Subcategory deleted successfully.'})
    finally:
        db_session.close()


@inventory_bp.route('/manage_brands')
@role_required(['Admin', 'Contributor'])
def manage_brands():
    try:
        app_id = current_user.app_id

        with Session() as db_session:
            role = current_user.role
            company = db_session.query(Company).filter_by(id=app_id).first()

            # Fetch modules
            modules_data = [mod.module_name for mod in
                            db_session.query(Module)
                            .filter_by(app_id=app_id, included='yes')
                            .all()]

            module_name = "Inventory"

            # Fetch all brands for the company
            brands = db_session.query(Brand).filter_by(app_id=app_id).all()

        return render_template(
            'inventory/manage_brands.html',
            brands=brands,
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name
        )

    except Exception as e:
        logger.error(f"Error loading brands: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading brands.', 'danger')
        return redirect(request.referrer or url_for('inventory.inventory_dashboard'))


# ----------------------
# Add new brand
# ----------------------
@inventory_bp.route('/brand/add', methods=['POST'])
@login_required
def add_brand():
    db_session = Session()
    try:
        app_id = current_user.app_id
        data = request.get_json()
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()

        if not name:
            return jsonify({'error': 'Brand name is required'}), 400

        # Check if brand already exists
        existing = db_session.query(Brand).filter_by(app_id=app_id, name=name).first()
        if existing:
            return jsonify({'error': 'Brand with this name already exists'}), 400

        brand = Brand(app_id=app_id, name=name, description=description)
        db_session.add(brand)
        db_session.commit()

        return jsonify({'success': 'Brand added successfully'})
    except Exception as e:
        db_session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()


# ----------------------
# Edit brand
# ----------------------
@inventory_bp.route('/brand/<int:brand_id>/edit', methods=['POST'])
@login_required
def edit_brand(brand_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        brand = db_session.query(Brand).filter_by(id=brand_id, app_id=app_id).first()
        if not brand:
            return jsonify({'error': 'Brand not found'}), 404

        data = request.get_json()
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()

        if not name:
            return jsonify({'error': 'Brand name is required'}), 400

        # Optional: check if another brand has the same name
        existing = db_session.query(Brand).filter(
            Brand.app_id == app_id,
            Brand.name == name,
            Brand.id != brand_id
        ).first()
        if existing:
            return jsonify({'error': 'Another brand with this name already exists'}), 400

        brand.name = name
        brand.description = description
        db_session.commit()

        return jsonify({'success': 'Brand updated successfully'})
    except Exception as e:
        db_session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()


# ----------------------
# Delete brand
# ----------------------
@inventory_bp.route('/brand/<int:brand_id>/delete', methods=['POST'])
@login_required
def delete_brand(brand_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        brand = db_session.query(Brand).filter_by(id=brand_id, app_id=app_id).first()
        if not brand:
            return jsonify({'error': 'Brand not found'}), 404

        # Check if brand is linked to any inventory items
        linked_items = db_session.query(InventoryItem).filter_by(brand_id=brand.id).count()
        if linked_items > 0:
            return jsonify({'error': 'Cannot delete brand; it is linked to existing inventory items'}), 400

        db_session.delete(brand)
        db_session.commit()

        return jsonify({'success': 'Brand deleted successfully'})
    except Exception as e:
        db_session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()


@inventory_bp.route('/manage_uoms')
@role_required(['Admin', 'Contributor'])
def manage_uoms():
    try:
        app_id = current_user.app_id

        with Session() as db_session:
            role = current_user.role
            company = db_session.query(Company).filter_by(id=app_id).first()

            # Fetch modules
            modules_data = [
                mod.module_name for mod in
                db_session.query(Module)
                .filter_by(app_id=app_id, included='yes')
                .all()
            ]

            module_name = "Inventory"

            # Fetch all UOMs for the company
            uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()

        return render_template(
            'inventory/manage_uoms.html',
            uoms=uoms,
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name
        )

    except Exception as e:
        logger.error(f"Error loading UOMs: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading units of measurement.', 'danger')
        return redirect(request.referrer or url_for('inventory.inventory_dashboard'))


# -----------------------------
# Add UOM
# -----------------------------
@inventory_bp.route('/uom/add', methods=['POST'])
@role_required(['Admin', 'Contributor'])
@login_required
def add_uom():
    try:
        data = request.get_json()
        name = data.get('name', '').strip()
        abbreviation = data.get('abbreviation', '').strip()
        app_id = current_user.app_id

        if not name:
            return jsonify({'error': 'UOM name is required'}), 400

        with Session() as db_session:
            new_uom = UnitOfMeasurement(
                app_id=app_id,
                full_name=name,
                abbreviation=abbreviation
            )
            db_session.add(new_uom)
            db_session.commit()

        return jsonify({'success': 'UOM added successfully'}), 200

    except SQLAlchemyError as e:
        logger.error(f"DB error adding UOM: {str(e)}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Error adding UOM: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred'}), 500


# -----------------------------
# Edit UOM
# -----------------------------
@inventory_bp.route('/uom/<int:uom_id>/edit', methods=['POST'])
@role_required(['Admin', 'Contributor'])
@login_required
def edit_uom(uom_id):
    try:
        data = request.get_json()
        name = data.get('name', '').strip()
        abbreviation = data.get('abbreviation', '').strip()

        if not name:
            return jsonify({'error': 'UOM name is required'}), 400

        with Session() as db_session:
            uom = db_session.query(UnitOfMeasurement).filter_by(id=uom_id, app_id=current_user.app_id).first()
            if not uom:
                return jsonify({'error': 'UOM not found'}), 404

            uom.name = name
            uom.abbreviation = abbreviation
            db_session.commit()

        return jsonify({'success': 'UOM updated successfully'}), 200

    except SQLAlchemyError as e:
        logger.error(f"DB error editing UOM: {str(e)}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Error editing UOM: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred'}), 500


# -----------------------------
# Delete UOM
# -----------------------------
# -----------------------------
# Delete UOM
# -----------------------------
@inventory_bp.route('/uom/<int:uom_id>/delete', methods=['POST'])
@role_required(['Admin', 'Contributor'])
@login_required
def delete_uom(uom_id):
    try:
        with Session() as db_session:
            uom = db_session.query(UnitOfMeasurement).filter_by(id=uom_id, app_id=current_user.app_id).first()
            if not uom:
                return jsonify({'error': 'UOM not found'}), 404

            # Check if UOM is being used in any inventory items
            used_count = db_session.query(InventoryItem).filter_by(uom_id=uom.id, app_id=current_user.app_id).count()
            if used_count > 0:
                return jsonify({'error': 'Cannot delete UOM. It is already assigned to inventory items.'}), 400

            db_session.delete(uom)
            db_session.commit()

        return jsonify({'success': 'UOM deleted successfully'}), 200

    except SQLAlchemyError as e:
        logger.error(f"DB error deleting UOM: {str(e)}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Error deleting UOM: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred'}), 500


# ------------------------------------
# Selling Prices
# -----------------------------------
@inventory_bp.route('/manage_selling_prices')
@role_required(['Admin', 'Supervisor', 'Sales Manager'])
def manage_selling_prices():
    """Manage selling prices for inventory items with all prices preloaded"""
    try:
        app_id = current_user.app_id

        # Get filter parameters from request
        category_filter = request.args.get('categoryFilter', '')
        subcategory_filter = request.args.get('subCategoryFilter', '')
        brand_filter = request.args.get('brandFilter', '')
        status_filter = request.args.get('statusFilter', 'active')
        search_term = request.args.get('search', '')
        zero_prices_only = request.args.get('zeroPricesOnly', 'false') == 'true'  # ✅ New filter

        with Session() as db_session:
            role = current_user.role
            company = db_session.query(Company).filter_by(id=app_id).first()
            modules_data = [mod.module_name for mod in
                            db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
            module_name = "Inventory"

            # Build base query with filters
            query = (
                db_session.query(InventoryItem)
                .options(
                    joinedload(InventoryItem.unit_of_measurement),
                    joinedload(InventoryItem.inventory_item_variation_link)
                    .joinedload(InventoryItemVariationLink.inventory_item_variation),
                    joinedload(InventoryItem.inventory_item_variation_link)
                    .joinedload(InventoryItemVariationLink.inventory_item_attributes),
                    joinedload(InventoryItem.inventory_item_variation_link)
                    .joinedload(InventoryItemVariationLink.selling_prices)
                    .joinedload(ItemSellingPrice.currency),
                    joinedload(InventoryItem.inventory_item_variation_link)
                    .joinedload(InventoryItemVariationLink.selling_prices)
                    .joinedload(ItemSellingPrice.customer_group),
                )
                .filter_by(app_id=app_id)
            )

            # Apply filters
            if status_filter == 'active':
                query = query.filter(InventoryItem.status == 'active')
            elif status_filter == 'all':
                pass

            if category_filter:
                query = query.filter(InventoryItem.item_category_id == category_filter)

            if subcategory_filter:
                query = query.filter(InventoryItem.item_subcategory_id == subcategory_filter)

            if brand_filter:
                query = query.filter(InventoryItem.brand_id == brand_filter)

            if search_term:
                query = query.filter(
                    or_(
                        InventoryItem.item_name.ilike(f'%{search_term}%'),
                        InventoryItem.item_code.ilike(f'%{search_term}%')
                    )
                )

            inventory_items = query.all()

            # ✅ Get stock information for all variations in one query
            variation_ids = []
            for item in inventory_items:
                for variation in item.inventory_item_variation_link:
                    variation_ids.append(variation.id)

            stock_summaries = {}
            if variation_ids:
                # Get all stock summaries for these variations
                summaries = db_session.query(InventorySummary).filter(
                    InventorySummary.item_id.in_(variation_ids),
                    InventorySummary.app_id == app_id
                ).all()

                # Group by variation_id and sum quantities across locations
                for summary in summaries:
                    if summary.item_id not in stock_summaries:
                        stock_summaries[summary.item_id] = {
                            'total_quantity': 0,
                            'total_value': 0
                        }
                    stock_summaries[summary.item_id]['total_quantity'] += summary.total_quantity
                    stock_summaries[summary.item_id]['total_value'] += float(summary.total_value or 0)

            # Prepare JSON data for frontend
            prices_data = {}
            for item in inventory_items:
                for variation in item.inventory_item_variation_link:
                    # ✅ Add stock information
                    stock_info = stock_summaries.get(variation.id, {'total_quantity': 0, 'total_value': 0})

                    prices_data[str(variation.id)] = {
                        'selling_prices': [
                            {
                                'id': price.id,
                                'selling_price': float(price.selling_price),
                                'currency_id': price.currency_id,
                                'currency_code': price.currency.user_currency if price.currency else '',
                                'min_quantity': float(price.min_quantity),
                                'max_quantity': float(price.max_quantity) if price.max_quantity else None,
                                'customer_group_id': price.customer_group_id,
                                'customer_group_name': price.customer_group.name if price.customer_group else 'All Customers',
                                'effective_from': price.effective_from.isoformat() if price.effective_from else None,
                                'effective_to': price.effective_to.isoformat() if price.effective_to else None,
                                'price_type': price.price_type,
                                'is_active': price.is_active,
                                'markup_percentage': float(
                                    price.markup_percentage) if price.markup_percentage else None,
                                'margin_percentage': float(price.margin_percentage) if price.margin_percentage else None
                            }
                            for price in variation.selling_prices
                        ],
                        'variation_info': {
                            'id': variation.id,
                            'item_name': item.item_name,
                            'variation_name': variation.inventory_item_variation.variation_name if variation.inventory_item_variation else None,
                            'attribute_name': variation.inventory_item_attributes.attribute_name if variation.inventory_item_attributes else None,
                            'uom': item.unit_of_measurement.full_name,
                        },
                        'stock_info': {  # ✅ Add stock info
                            'total_quantity': float(stock_info['total_quantity']),
                            'total_value': float(stock_info['total_value'])
                        }
                    }

            # Get filter options
            categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
            subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
            brands = db_session.query(Brand).filter_by(app_id=app_id).all()
            currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
            customer_groups = db_session.query(CustomerGroup).filter_by(app_id=app_id, is_active=True).all()

        return render_template(
            'inventory/manage_selling_prices.html',
            inventory_items=inventory_items,
            currencies=currencies,
            customer_groups=customer_groups,
            categories=categories,
            subcategories=subcategories,
            brands=brands,
            prices_data_json=prices_data,
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name,
            filter_applied=any([category_filter, subcategory_filter, brand_filter, search_term]),
            zero_prices_only=zero_prices_only  # ✅ Pass to template
        )

    except Exception as e:
        logger.error(f"Error loading selling prices management: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading selling prices management.', 'danger')
        return redirect(request.referrer or url_for('inventory.inventory_dashboard'))


@inventory_bp.route('/api/selling_prices/variation/<int:variation_link_id>', methods=['GET'])
@role_required(['Admin', 'Supervisor', 'Sales Manager', 'Sales'])
def get_variation_selling_prices(variation_link_id):
    """Get selling prices for a specific inventory item variation"""
    db_session = Session()
    app_id = current_user.app_id
    logger.info('Route ahs been called')
    try:
        # Verify the variation belongs to the user's app
        variation = db_session.query(InventoryItemVariationLink).filter_by(
            id=variation_link_id,
            app_id=app_id
        ).first()

        if not variation:
            return jsonify({
                'success': False,
                'message': 'Variation not found'
            }), 404

        selling_prices = db_session.query(ItemSellingPrice).options(
            joinedload(ItemSellingPrice.currency),
            joinedload(ItemSellingPrice.customer_group),
            joinedload(ItemSellingPrice.created_user),
            joinedload(ItemSellingPrice.updated_user)
        ).filter_by(
            inventory_item_variation_link_id=variation_link_id,
            app_id=app_id
        ).order_by(
            ItemSellingPrice.customer_group_id,
            ItemSellingPrice.min_quantity,
            ItemSellingPrice.effective_from.desc()
        ).all()

        prices_data = []
        for price in selling_prices:
            prices_data.append({
                'id': price.id,
                'selling_price': float(price.selling_price),
                'currency_id': price.currency_id,
                'currency_code': price.currency.user_currency if price.currency else '',
                'min_quantity': float(price.min_quantity),
                'max_quantity': float(price.max_quantity) if price.max_quantity else None,
                'customer_group_id': price.customer_group_id,
                'customer_group_name': price.customer_group.name if price.customer_group else 'All Customers',
                'effective_from': price.effective_from.isoformat() if price.effective_from else None,
                'effective_to': price.effective_to.isoformat() if price.effective_to else None,
                'price_type': price.price_type,
                'is_active': price.is_active,
                'markup_percentage': float(price.markup_percentage) if price.markup_percentage else None,
                'margin_percentage': float(price.margin_percentage) if price.margin_percentage else None,
                'created_by': f"{price.created_user.name}" if price.created_user else 'System',
                'updated_by': f"{price.updated_user.name}" if price.updated_user else None,
                'created_at': price.created_at.isoformat() if price.created_at else None,
                'updated_at': price.updated_at.isoformat() if price.updated_at else None
            })

        return jsonify({
            'success': True,
            'selling_prices': prices_data,
            'variation_info': {
                'id': variation.id,
                'item_name': variation.inventory_item.item_name,
                'variation_name': variation.inventory_item_variation.variation_name if variation.inventory_item_variation else None,
                'attribute_name': variation.inventory_item_attributes.attribute_name if variation.inventory_item_attributes else None
            }
        })

    except Exception as e:
        logger.error(f"Error getting selling prices: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'Error getting selling prices: {str(e)}'
        }), 500
    finally:
        db_session.close()


@inventory_bp.route('/api/selling_prices', methods=['POST'])
@role_required(['Admin', 'Supervisor', 'Sales Manager'])
def create_selling_price():
    """Create a new selling price"""
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ['inventory_item_variation_link_id', 'selling_price', 'currency_id']
        for field in required_fields:
            if not data.get(field):
                return jsonify({
                    'success': False,
                    'message': f'{field.replace("_", " ").title()} is required'
                }), 400

        # Verify the variation belongs to the user's app
        variation = db_session.query(InventoryItemVariationLink).filter_by(
            id=data['inventory_item_variation_link_id'],
            app_id=app_id
        ).first()

        if not variation:
            return jsonify({
                'success': False,
                'message': 'Inventory item variation not found'
            }), 404

        # Check for duplicate price configuration
        existing_price = db_session.query(ItemSellingPrice).filter_by(
            app_id=app_id,
            inventory_item_variation_link_id=data['inventory_item_variation_link_id'],
            currency_id=data['currency_id'],
            min_quantity=Decimal(str(data.get('min_quantity', 1))),
            customer_group_id=data.get('customer_group_id'),
            price_type=data.get('price_type', 'standard')
        ).first()

        if existing_price:
            return jsonify({
                'success': False,
                'message': 'A selling price already exists for this combination'
            }), 400

        # Validate effective dates
        effective_from = datetime.fromisoformat(data['effective_from']) if data.get(
            'effective_from') else datetime.now()
        effective_to = datetime.fromisoformat(data['effective_to']) if data.get('effective_to') else None

        if effective_to and effective_from > effective_to:
            return jsonify({
                'success': False,
                'message': 'Effective from date cannot be after effective to date'
            }), 400

        # Validate quantity ranges
        min_quantity = Decimal(str(data.get('min_quantity', 1)))
        max_quantity = Decimal(str(data['max_quantity'])) if data.get('max_quantity') else None

        if max_quantity and min_quantity > max_quantity:
            return jsonify({
                'success': False,
                'message': 'Minimum quantity cannot be greater than maximum quantity'
            }), 400

        # Create new selling price
        selling_price = ItemSellingPrice(
            app_id=app_id,
            inventory_item_variation_link_id=data['inventory_item_variation_link_id'],
            selling_price=Decimal(str(data['selling_price'])),
            currency_id=data['currency_id'],
            min_quantity=min_quantity,
            max_quantity=max_quantity,
            customer_group_id=data.get('customer_group_id'),
            effective_from=effective_from,
            effective_to=effective_to,
            price_type=data.get('price_type', 'standard'),
            markup_percentage=Decimal(str(data['markup_percentage'])) if data.get('markup_percentage') else None,
            margin_percentage=Decimal(str(data['margin_percentage'])) if data.get('margin_percentage') else None,
            is_active=data.get('is_active', True),
            created_by=current_user.id
        )

        db_session.add(selling_price)
        db_session.commit()

        # Log the activity
        logger.info(
            f"User {current_user.id} created selling price {selling_price.id} for variation {data['inventory_item_variation_link_id']}")

        return jsonify({
            'success': True,
            'message': 'Selling price created successfully',
            'price_id': selling_price.id
        })

    except ValueError as e:
        db_session.rollback()
        logger.error(f"Value error creating selling price: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Invalid data format. Please check your inputs.'
        }), 400
    except Exception as e:
        db_session.rollback()
        logger.error(f"Error creating selling price: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'Error creating selling price: {str(e)}'
        }), 500
    finally:
        db_session.close()


@inventory_bp.route('/api/selling_prices/<int:price_id>', methods=['PUT'])
@role_required(['Admin', 'Supervisor', 'Sales Manager'])
def update_selling_price(price_id):
    """Update an existing selling price"""
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()
        selling_price = db_session.query(ItemSellingPrice).filter_by(
            id=price_id,
            app_id=app_id
        ).first()

        if not selling_price:
            return jsonify({
                'success': False,
                'message': 'Selling price not found'
            }), 404

        # Check for duplicate price configuration (excluding current price)
        if any(key in data for key in ['currency_id', 'min_quantity', 'customer_group_id', 'price_type']):
            existing_price = db_session.query(ItemSellingPrice).filter(
                ItemSellingPrice.id != price_id,
                ItemSellingPrice.app_id == app_id,
                ItemSellingPrice.inventory_item_variation_link_id == selling_price.inventory_item_variation_link_id,
                ItemSellingPrice.currency_id == data.get('currency_id', selling_price.currency_id),
                ItemSellingPrice.min_quantity == Decimal(str(data.get('min_quantity', selling_price.min_quantity))),
                ItemSellingPrice.customer_group_id == data.get('customer_group_id', selling_price.customer_group_id),
                ItemSellingPrice.price_type == data.get('price_type', selling_price.price_type)
            ).first()

            if existing_price:
                return jsonify({
                    'success': False,
                    'message': 'Another selling price already exists for this combination'
                }), 400

        # Update fields
        if 'selling_price' in data:
            selling_price.selling_price = Decimal(str(data['selling_price']))
        if 'currency_id' in data:
            selling_price.currency_id = data['currency_id']
        if 'min_quantity' in data:
            selling_price.min_quantity = Decimal(str(data['min_quantity']))
        if 'max_quantity' in data:
            selling_price.max_quantity = Decimal(str(data['max_quantity'])) if data.get('max_quantity') else None
        if 'customer_group_id' in data:
            selling_price.customer_group_id = data['customer_group_id']
        if 'effective_from' in data:
            selling_price.effective_from = datetime.fromisoformat(data['effective_from'])
        if 'effective_to' in data:
            selling_price.effective_to = datetime.fromisoformat(data['effective_to']) if data.get(
                'effective_to') else None
        if 'price_type' in data:
            selling_price.price_type = data['price_type']
        if 'markup_percentage' in data:
            selling_price.markup_percentage = Decimal(str(data['markup_percentage'])) if data.get(
                'markup_percentage') else None
        if 'margin_percentage' in data:
            selling_price.margin_percentage = Decimal(str(data['margin_percentage'])) if data.get(
                'margin_percentage') else None
        if 'is_active' in data:
            selling_price.is_active = data['is_active']

        selling_price.updated_by = current_user.id

        db_session.commit()

        logger.info(f"User {current_user.id} updated selling price {price_id}")

        return jsonify({
            'success': True,
            'message': 'Selling price updated successfully'
        })

    except ValueError as e:
        db_session.rollback()
        logger.error(f"Value error updating selling price: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Invalid data format. Please check your inputs.'
        }), 400
    except Exception as e:
        db_session.rollback()
        logger.error(f"Error updating selling price: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'Error updating selling price: {str(e)}'
        }), 500
    finally:
        db_session.close()


@inventory_bp.route('/api/selling_prices/<int:price_id>', methods=['DELETE'])
@role_required(['Admin', 'Supervisor'])
def delete_selling_price(price_id):
    """Delete a selling price"""
    db_session = Session()
    app_id = current_user.app_id

    try:
        selling_price = db_session.query(ItemSellingPrice).filter_by(
            id=price_id,
            app_id=app_id
        ).first()

        if not selling_price:
            return jsonify({
                'success': False,
                'message': 'Selling price not found'
            }), 404

        # Store variation ID for logging before deletion
        variation_id = selling_price.inventory_item_variation_link_id

        db_session.delete(selling_price)
        db_session.commit()

        logger.info(f"User {current_user.id} deleted selling price {price_id} for variation {variation_id}")

        return jsonify({
            'success': True,
            'message': 'Selling price deleted successfully'
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error deleting selling price: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'Error deleting selling price: {str(e)}'
        }), 500
    finally:
        db_session.close()


@inventory_bp.route('/api/selling_prices/bulk', methods=['POST'])
@role_required(['Admin', 'Supervisor', 'Sales Manager'])
def bulk_update_selling_prices():
    """Bulk update selling prices (activate/deactivate)"""
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()
        price_ids = data.get('price_ids', [])
        action = data.get('action')  # 'activate' or 'deactivate'

        if not price_ids or action not in ['activate', 'deactivate']:
            return jsonify({
                'success': False,
                'message': 'Invalid request. Please provide price IDs and valid action.'
            }), 400

        # Update prices
        updated_count = db_session.query(ItemSellingPrice).filter(
            ItemSellingPrice.id.in_(price_ids),
            ItemSellingPrice.app_id == app_id
        ).update({
            ItemSellingPrice.is_active: (action == 'activate'),
            ItemSellingPrice.updated_by: current_user.id,
            ItemSellingPrice.updated_at: datetime.now()
        }, synchronize_session=False)

        db_session.commit()

        logger.info(f"User {current_user.id} {action}d {updated_count} selling prices")

        return jsonify({
            'success': True,
            'message': f'Successfully {action}d {updated_count} selling price(s)',
            'updated_count': updated_count
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk update selling prices: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'Error updating selling prices: {str(e)}'
        }), 500
    finally:
        db_session.close()


@inventory_bp.route('/api/selling_prices/<int:variation_link_id>', methods=['GET'])
@role_required(['Admin', 'Supervisor', 'Sales Manager', 'Sales'])
def get_selling_prices(variation_link_id):
    """Get selling prices for a specific inventory item variation"""
    db_session = Session()
    app_id = current_user.app_id

    try:
        selling_prices = db_session.query(ItemSellingPrice).options(
            joinedload(ItemSellingPrice.currency),
            joinedload(ItemSellingPrice.customer_group)
        ).filter_by(
            inventory_item_variation_link_id=variation_link_id,
            app_id=app_id,
            is_active=True
        ).order_by(
            ItemSellingPrice.customer_group_id,  # Group by customer group
            ItemSellingPrice.min_quantity,  # Then by quantity
            ItemSellingPrice.effective_from.desc()  # Then by effective date
        ).all()

        prices_data = []
        for price in selling_prices:
            prices_data.append({
                'id': price.id,
                'selling_price': float(price.selling_price),
                'currency_id': price.currency_id,
                'currency_code': price.currency.currency_code if price.currency else '',
                'min_quantity': float(price.min_quantity),
                'max_quantity': float(price.max_quantity) if price.max_quantity else None,
                'customer_group_id': price.customer_group_id,
                'customer_group_name': price.customer_group.name if price.customer_group else 'All Customers',
                'effective_from': price.effective_from.isoformat() if price.effective_from else None,
                'effective_to': price.effective_to.isoformat() if price.effective_to else None,
                'price_type': price.price_type,
                'is_active': price.is_active,
                'markup_percentage': float(price.markup_percentage) if price.markup_percentage else None,
                'margin_percentage': float(price.margin_percentage) if price.margin_percentage else None
            })

        return jsonify({
            'success': True,
            'selling_prices': prices_data
        })

    except Exception as e:
        logger.error(f"Error getting selling prices: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'Error getting selling prices: {str(e)}'
        }), 500
    finally:
        db_session.close()


@inventory_bp.route('/api/selling_prices/<int:price_id>/toggle', methods=['POST'])
@role_required(['Admin', 'Supervisor', 'Sales Manager'])
def toggle_selling_price(price_id):
    """Toggle the active status of a selling price"""
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get the selling price
        selling_price = db_session.query(ItemSellingPrice).filter(
            ItemSellingPrice.id == price_id,
            ItemSellingPrice.app_id == app_id
        ).first()

        if not selling_price:
            return jsonify({
                'success': False,
                'message': 'Selling price not found or you do not have permission to modify it'
            }), 404

        # Toggle the active status
        new_status = not selling_price.is_active
        selling_price.is_active = new_status
        selling_price.updated_by = current_user.id
        selling_price.updated_at = func.now()

        # Commit the changes
        db_session.commit()

        return jsonify({
            'success': True,
            'message': f'Price successfully {"activated" if new_status else "deactivated"}',
            'is_active': new_status
        })

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error while toggling selling price {price_id}: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Database error occurred while updating price status'
        }), 500

    except Exception as e:
        db_session.rollback()
        logger.error(f"Unexpected error while toggling selling price {price_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An unexpected error occurred'
        }), 500

    finally:
        db_session.close()


# ------------------------------
# Customer Groups
# -------------------------------

@inventory_bp.route('/manage_customer_groups')
@role_required(['Admin', 'Contributor'])
def manage_customer_groups():
    try:
        app_id = current_user.app_id

        with Session() as db_session:
            role = current_user.role
            company = db_session.query(Company).filter_by(id=app_id).first()

            # Fetch modules
            modules_data = [
                mod.module_name for mod in
                db_session.query(Module)
                .filter_by(app_id=app_id, included='yes')
                .all()
            ]

            module_name = "Inventory"

            # Fetch all customer groups for the company
            customer_groups = (
                db_session.query(CustomerGroup)
                .filter_by(app_id=app_id)
                .all()
            )

        return render_template(
            'inventory/manage_customer_groups.html',
            customer_groups=customer_groups,
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name
        )

    except Exception as e:
        logger.error(f"Error loading Customer Groups: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading customer groups.', 'danger')
        return redirect(request.referrer or url_for('inventory.inventory_dashboard'))


@inventory_bp.route('/customer_group/add', methods=['POST'])
@role_required(['Admin', 'Contributor'])
def create_customer_group():
    """Create a new customer group"""
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()

        # Validate required fields
        if not data or not data.get('name'):
            return jsonify({'success': False, 'message': 'Customer group name is required'}), 400

        # Check if group already exists
        existing_group = db_session.query(CustomerGroup).filter(
            CustomerGroup.name == data['name'],
            CustomerGroup.app_id == app_id
        ).first()

        if existing_group:
            return jsonify({'success': False, 'message': 'Customer group with this name already exists'}), 400

        # Create new customer group
        new_group = CustomerGroup(
            app_id=app_id,
            name=data['name'],
            description=data.get('description', ''),
            default_discount_percentage=data.get('default_discount_percentage', 0.0),
            default_tax_percentage=data.get('default_tax_percentage', 0.0),
            is_active=data.get('is_active', True),
            is_default=data.get('is_default', True),
            created_by=current_user.id
        )

        db_session.add(new_group)
        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Customer group created successfully',
            'group_id': new_group.id
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error creating customer group: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': 'Error creating customer group'}), 500
    finally:
        db_session.close()


# -------------------------------
# Edit Customer Group
# -------------------------------
@inventory_bp.route('/customer_group/<int:group_id>/edit', methods=['POST'])
@login_required
def edit_customer_group(group_id):
    try:
        app_id = current_user.app_id
        data = request.get_json()

        name = data.get('name')
        description = data.get('description')
        discount = data.get('default_discount_percentage', 0.0)
        tax = data.get('default_tax_percentage', 0.0)
        is_active = data.get('is_active', True)
        is_default = data.get('is_default', True)

        if not name:
            return jsonify({'error': 'Customer group name is required'}), 400

        with Session() as db_session:
            group = db_session.query(CustomerGroup).filter_by(id=group_id, app_id=app_id).first()
            if not group:
                return jsonify({'error': 'Customer group not found'}), 404

            group.name = name.strip()
            group.description = description.strip() if description else None
            group.default_discount_percentage = discount
            group.default_tax_percentage = tax
            group.is_active = is_active
            group.is_default = is_default
            group.updated_by = current_user.id

            db_session.commit()

        return jsonify({'success': 'Customer group updated successfully'}), 200

    except IntegrityError:
        return jsonify({'error': 'Customer group with this name already exists'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# -------------------------------
# Delete Customer Group
# -------------------------------
@inventory_bp.route('/customer_group/<int:group_id>/delete', methods=['POST'])
@login_required
def delete_customer_group(group_id):
    try:
        app_id = current_user.app_id
        with Session() as db_session:
            group = db_session.query(CustomerGroup).filter_by(id=group_id, app_id=app_id).first()
            if not group:
                return jsonify({'error': 'Customer group not found'}), 404

            # ⚠️ Instead of hard delete, mark inactive (safer for audit + linked records)
            group.is_active = False
            group.updated_by = current_user.id
            db_session.commit()

        return jsonify({'success': 'Customer group deactivated successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# -----------------------------
# Manage Inventory Locations
# -----------------------------
@inventory_bp.route('/manage_inventory_locations')
@role_required(['Admin', 'Supervisor'])
def manage_inventory_locations():
    try:
        app_id = current_user.app_id

        with Session() as db_session:
            role = current_user.role
            company = db_session.query(Company).filter_by(id=app_id).first()
            locations = (
                db_session.query(InventoryLocation)
                .filter_by(app_id=current_user.app_id)
                .options(
                    joinedload(InventoryLocation.discount_account),
                    joinedload(InventoryLocation.payment_account),
                    joinedload(InventoryLocation.card_payment_account),
                    joinedload(InventoryLocation.mobile_money_account),
                    joinedload(InventoryLocation.project),  # ✅ Added this line
                    joinedload(InventoryLocation.user_location_assignments)
                    .joinedload(UserLocationAssignment.user)
                )
                .all()
            )

            accounts = db_session.query(ChartOfAccounts).filter_by(app_id=app_id).all()

            # Fetch modules
            modules_data = [
                mod.module_name for mod in
                db_session.query(Module)
                .filter_by(app_id=app_id, included='yes')
                .all()
            ]

            module_name = "Inventory"

        return render_template(
            'inventory/manage_inventory_locations.html',
            locations=locations,
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name,
            accounts=accounts
        )

    except Exception as e:
        logger.error(f"Error loading Inventory Locations: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading inventory locations.', 'danger')
        return redirect(request.referrer or url_for('inventory.inventory_dashboard'))


# -----------------------------
# Add Inventory Location
# -----------------------------
# Add Location
@inventory_bp.route('/inventory_location/add', methods=['POST'])
@role_required(['Admin', 'Contributor'])
@login_required
def add_inventory_location():
    try:
        data = request.get_json() or {}
        loc_name = (data.get('location') or '').strip()
        description = data.get('description') or None
        discount_account_id = data.get('discount_account_id') or None
        payment_account_id = data.get('payment_account_id') or None
        # New columns
        card_payment_account_id = data.get('card_payment_account_id') or None
        mobile_money_account_id = data.get('mobile_money_account_id') or None
        workflow_type = data.get('workflow_type') or 'process_payment'
        project_id = data.get('project_id') or None
        if not loc_name:
            return jsonify({'error': 'Location name is required'}), 400

        with Session() as db_session:
            new_loc = InventoryLocation(
                app_id=current_user.app_id,
                location=loc_name,
                description=description,
                discount_account_id=discount_account_id,
                payment_account_id=payment_account_id,
                # New columns
                card_payment_account_id=card_payment_account_id,
                mobile_money_account_id=mobile_money_account_id,
                workflow_type=workflow_type,
                project_id=project_id
            )
            db_session.add(new_loc)
            db_session.commit()

            return jsonify({'success': 'Location added successfully', 'id': new_loc.id}), 200
    except SQLAlchemyError as e:
        logger.error(f"DB error adding location: {e}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Error adding location: {e}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred'}), 500


# Edit Location
@inventory_bp.route('/inventory_location/<int:loc_id>/edit', methods=['POST'])
@role_required(['Admin', 'Contributor'])
@login_required
def edit_inventory_location(loc_id):
    try:
        data = request.get_json() or {}
        loc_name = (data.get('location') or '').strip()
        description = data.get('description') or None
        discount_account_id = data.get('discount_account_id') or None
        payment_account_id = data.get('payment_account_id') or None
        # New columns
        card_payment_account_id = data.get('card_payment_account_id') or None
        mobile_money_account_id = data.get('mobile_money_account_id') or None
        workflow_type = data.get('workflow_type') or 'process_payment'
        project = data.get('project_id') or None
        if not loc_name:
            return jsonify({'error': 'Location name is required'}), 400

        with Session() as db_session:
            location = db_session.query(InventoryLocation).filter(
                InventoryLocation.id == loc_id,
                InventoryLocation.app_id == current_user.app_id
            ).first()

            if not location:
                return jsonify({'error': 'Location not found'}), 404

            location.location = loc_name
            location.description = description
            location.discount_account_id = discount_account_id
            location.payment_account_id = payment_account_id
            # New columns
            location.card_payment_account_id = card_payment_account_id
            location.mobile_money_account_id = mobile_money_account_id
            location.workflow_type = workflow_type
            location.project_id = project

            db_session.commit()

            return jsonify({'success': 'Location updated successfully'}), 200
    except SQLAlchemyError as e:
        logger.error(f"DB error updating location: {e}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Error updating location: {e}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred'}), 500


# Delete Location (soft delete or hard delete - here we'll hard delete if safe)
@inventory_bp.route('/inventory_location/<int:loc_id>/delete', methods=['POST'])
@role_required(['Admin', 'Contributor'])
@login_required
def delete_inventory_location(loc_id):
    try:
        with Session() as db_session:
            loc = db_session.query(InventoryLocation).filter_by(id=loc_id, app_id=current_user.app_id).first()
            if not loc:
                return jsonify({'error': 'Location not found'}), 404

            # Prevent delete if used in inventory summary/entries/sales - optional safety
            # Example: check InventorySummary or InventoryTransactionDetail usage
            from models import InventorySummary, InventoryTransactionDetail, DirectSaleItem
            used_count = 0
            used_count += db_session.query(InventorySummary).filter_by(location_id=loc_id).count()
            used_count += db_session.query(InventoryTransactionDetail).filter_by(location_id=loc_id).count()
            used_count += db_session.query(DirectSaleItem).filter_by(location_id=loc_id).count()
            if used_count > 0:
                return jsonify({'error': 'Cannot delete location. It is referenced by inventory records.'}), 400

            db_session.delete(loc)
            db_session.commit()
            return jsonify({'success': 'Location deleted successfully'}), 200
    except SQLAlchemyError as e:
        logger.error(f"DB error deleting location: {e}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Error deleting location: {e}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred'}), 500


# Small helper: fetch single location via JSON (optional)
@inventory_bp.route('/inventory_location/<int:loc_id>', methods=['GET'])
@role_required(['Admin', 'Contributor', 'Viewer'])
@login_required
def get_inventory_location(loc_id):
    try:
        with Session() as db_session:
            loc = db_session.query(InventoryLocation).filter_by(id=loc_id, app_id=current_user.app_id).first()
            if not loc:
                return jsonify({'error': 'Location not found'}), 404
            return jsonify({
                'id': loc.id,
                'location': loc.location,
                'description': loc.description,
                'discount_account_id': loc.discount_account_id,
                'payment_account_id': loc.payment_account_id
            }), 200
    except Exception as e:
        logger.error(f"Error fetching location: {e}\n{traceback.format_exc()}")
        return jsonify({'error': 'An error occurred'}), 500
