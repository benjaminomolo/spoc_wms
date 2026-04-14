# app/routes/inventory/pos/pos.py
import traceback
from datetime import datetime, timedelta
from decimal import Decimal

from flask import Blueprint, jsonify, render_template, flash, redirect, url_for, request, current_app, g
from flask_login import login_required, current_user
from sqlalchemy import func, or_, and_
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import joinedload

from decorators import role_required, require_permission, cached_route
from ai import get_base_currency, get_or_create_exchange_rate_id, get_exchange_rate
from db import Session
from models import Company, Module, Currency, Project, ChartOfAccounts, PaymentMode, Category, Department, Employee, \
    PayrollPeriod, InventoryCategory, InventorySubCategory, InventoryLocation, InventoryItem, InventoryItemVariation, \
    InventoryItemAttribute, Brand, InventoryItemVariationLink, UnitOfMeasurement, Vendor, InventoryEntry, \
    InventoryEntryLineItem, ExchangeRate, InventoryTransactionDetail, CustomerGroup, ItemSellingPrice, InventorySummary, \
    DirectSalesTransaction, DirectSaleItem
import logging

from services.inventory_helpers import handle_supplier_logic, \
    render_inventory_entry_form, \
    process_inventory_entries, reverse_inventory_entry, get_inventory_entry_with_details, \
    render_edit_inventory_entry_form, process_inventory_entries_for_edit, update_inventory_summary
from services.post_to_ledger import post_pos_transaction_to_ledger
from services.vendors_and_customers import get_or_create_vendor
from utils import ensure_default_location, generate_unique_lot, create_notification, empty_to_none, \
    validate_quantity_and_selling_price, validate_quantity_and_price, handle_batch_variation_update, get_converted_cost, \
    get_or_create_default_location, generate_direct_sale_number
from utils_and_helpers.amounts_utils import format_amount
from utils_and_helpers.cache_keys import stock_history_cache_key
from utils_and_helpers.cache_utils import on_inventory_data_changed, clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj
from . import pos_bp

logger = logging.getLogger(__name__)


# -------------------- POS PAGE --------------------
@pos_bp.route('')
@login_required
def pos_page():
    """Render the POS system page"""
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get company and base currency
        company = db_session.query(Company).filter_by(id=app_id).first()
        base_currency = db_session.query(Currency).filter_by(
            app_id=app_id, currency_index=1
        ).first()

        # Get all currencies for selection
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        # Get categories for filtering
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()

        # Get or create default location
        default_location = get_or_create_default_location(db_session, app_id)

        # Get locations for filtering
        locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

        # Get customer groups for pricing
        customer_groups = db_session.query(CustomerGroup).filter_by(app_id=app_id, is_active=True).all()

        # Get tax rate from the default customer group, or first available, or use default
        default_customer_group = db_session.query(CustomerGroup).filter_by(
            app_id=app_id,
            is_active=True,
            is_default=True
        ).first()

        if default_customer_group:
            tax_rate = float(default_customer_group.default_tax_percentage)
        elif customer_groups:
            tax_rate = float(customer_groups[0].default_tax_percentage)
        else:
            tax_rate = 0.0  # Default tax rate if no customer groups exist

        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        return render_template(
            'inventory/pos.html',
            categories=categories,
            locations=locations,
            default_location_id=default_location,  # Pass the default location ID
            currencies=currencies,
            base_currency=base_currency,
            customer_groups=customer_groups,
            company=company,
            modules=modules_data,
            role=role,
            tax_rate=tax_rate
        )

    except Exception as e:
        logger.error(f"Error rendering POS page: {str(e)}\n{traceback.format_exc()}")
        return "An error occurred while loading the POS page", 500
    finally:
        db_session.close()


@pos_bp.route('/api/products')
@role_required(['Admin', 'Contributor'])
def get_pos_products():
    """Get products for POS with inventory and pricing"""
    try:
        app_id = current_user.app_id
        category_id = request.args.get('category_id', type=int)
        location_id = request.args.get('location_id', type=int)
        customer_group_id = request.args.get('customer_group_id', type=int)
        currency_id = request.args.get('currency_id', type=int)
        search_term = request.args.get('search', '')
        logger.info(f'Filtered data is {request.args}')
        with Session() as db_session:

            # Get customer group tax percentage if customer_group_id is provided
            customer_group_tax_percentage = 0
            if customer_group_id:
                customer_group = db_session.query(CustomerGroup).filter_by(
                    id=customer_group_id, app_id=app_id
                ).first()
                if customer_group:
                    customer_group_tax_percentage = float(customer_group.default_tax_percentage)

            # Get location workflow type if location_id is provided
            location_workflow_type = 'process_payment'  # default
            if location_id:
                location = db_session.query(InventoryLocation).filter_by(
                    id=location_id, app_id=app_id
                ).first()
                if location and location.workflow_type:
                    location_workflow_type = location.workflow_type

            currencies = db_session.query(Currency).filter_by(app_id=app_id)
            # Get base currency
            # Find base currency (currency_index = 1)
            base_currency = next((c for c in currencies if c.currency_index == 1), None)

            if not base_currency:
                return jsonify({'success': False, 'message': 'Base currency not configured'}), 500

            if not currency_id:
                currency_id = base_currency.id

            # Resolve the selected currency object
            selected_currency = next((c for c in currencies if c.id == currency_id), base_currency)

            # Get the currency code/symbol (user_currency field)
            user_currency = selected_currency.user_currency

            # Base query with eager loads
            query = db_session.query(InventoryItem).options(
                joinedload(InventoryItem.inventory_item_variation_link)
                .joinedload(InventoryItemVariationLink.selling_prices)
                .joinedload(ItemSellingPrice.currency),
                joinedload(InventoryItem.inventory_item_variation_link)
                .joinedload(InventoryItemVariationLink.inventory_summary)
                .joinedload(InventorySummary.location),
                joinedload(InventoryItem.inventory_category)
            ).filter(
                InventoryItem.app_id == app_id,
                InventoryItem.status == 'active'
            )

            # Apply filters early
            if category_id:
                query = query.filter(InventoryItem.item_category_id == category_id)

            if search_term:
                query = query.filter(InventoryItem.item_name.ilike(f'%{search_term}%'))

            products = query.all()

            pos_products = []
            for item in products:
                for variation in item.inventory_item_variation_link:
                    if variation.status != 'active':
                        continue

                    # Get customer group price
                    selling_price = get_customer_group_price(
                        db_session, variation.id, customer_group_id, currency_id, app_id, base_currency
                    )
                    # Stock per location
                    stock_quantity = 0
                    if variation.inventory_summary:
                        if location_id:
                            location_summary = next(
                                (s for s in variation.inventory_summary if s.location_id == location_id), None
                            )
                            stock_quantity = location_summary.total_quantity if location_summary else 0
                        else:
                            stock_quantity = sum(s.total_quantity for s in variation.inventory_summary)

                    # Optional: only include if stock available
                    # if stock_quantity <= 0:
                    #     continue

                    pos_products.append({
                        'item_id': item.id,
                        'variation_id': variation.id,
                        'variation_name': variation.inventory_item_variation.variation_name if variation.inventory_item_variation else None,
                        'name': item.item_name,
                        'description': item.item_description,
                        'price': float(selling_price),
                        'user_currency': user_currency,
                        'stock': stock_quantity,
                        'image': item.image_filename,
                        'category_id': item.item_category_id,
                        'category_name': item.inventory_category.category_name if item.inventory_category else '',
                        'uom_id': item.uom_id,
                        'uom': item.unit_of_measurement.abbreviation if item.unit_of_measurement else None
                    })
            # Sort products alphabetically by name and variation
            pos_products.sort(key=lambda x: (x['name'].lower(), (x['variation_name'] or '').lower()))

            return jsonify({
                'success': True,
                'products': pos_products,
                'selected_currency': user_currency,
                'customer_group_tax_percentage': customer_group_tax_percentage,
                'location_workflow_type': location_workflow_type
            })

    except Exception as e:
        logger.error(f"Error fetching POS products: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': 'Error fetching products'}), 500


def get_customer_group_price(db_session, variation_id, customer_group_id, currency_id, app_id, base_currency):
    """Get the price for a variation, using base currency as primary reference and converting if needed."""
    try:
        now = datetime.now()
        logger.info(
            f'Getting price for variation {variation_id} | requested currency: {currency_id} | base currency: {base_currency.id}')

        # Build the base query conditions
        base_conditions = [
            ItemSellingPrice.inventory_item_variation_link_id == variation_id,
            ItemSellingPrice.is_active == True,
            ItemSellingPrice.currency_id == base_currency.id,
            or_(ItemSellingPrice.effective_to.is_(None), ItemSellingPrice.effective_to >= now)
        ]

        # --- 1. First priority: Customer group specific price ---
        if customer_group_id:
            customer_group_price = db_session.query(ItemSellingPrice).filter(
                *base_conditions,
                ItemSellingPrice.customer_group_id == customer_group_id
            ).first()

            if customer_group_price:
                price = customer_group_price.selling_price
                logger.info(f'Using customer group specific price: {price}')
                # Convert to requested currency if different
                if currency_id != base_currency.id:
                    price = get_converted_cost(db_session, price, base_currency.id, currency_id, app_id, now)
                    logger.info(f'Converted customer group price to requested currency: {price}')
                return price

        # --- 2. Second priority: Standard override (no customer group) ---
        standard_price = db_session.query(ItemSellingPrice).filter(
            *base_conditions,
            ItemSellingPrice.customer_group_id.is_(None)
        ).first()

        if standard_price:
            price = standard_price.selling_price
            logger.info(f'Using standard price override: {price}')
            # Convert to requested currency if different
            if currency_id != base_currency.id:
                price = get_converted_cost(db_session, price, base_currency.id, currency_id, app_id, now)
                logger.info(f'Converted standard price to requested currency: {price}')
            return price

        # --- 3. Fallback: Base currency price ---
        base_price = db_session.query(ItemSellingPrice).filter(
            *base_conditions
        ).first()

        if base_price:
            price = base_price.selling_price
            logger.info(f'Using base currency price: {price}')
            # Convert to requested currency if different
            if currency_id != base_currency.id:
                price = get_converted_cost(db_session, price, base_currency.id, currency_id, app_id, now)
                logger.info(f'Converted base price to requested currency: {price}')
            return price

        logger.info('No price found for variation')
        return 0

    except Exception as e:
        logger.error(f"Error getting customer group price: {str(e)}")
        return 0


@pos_bp.route('/api/checkout', methods=['POST'])
@role_required(['Admin', 'Contributor'])
def pos_checkout():
    """Process POS checkout with currency conversion"""
    try:
        app_id = current_user.app_id
        data = request.get_json()
        if not data or 'items' not in data or not data['items']:
            return jsonify({'success': False, 'message': 'No items in cart'}), 400

        with Session() as db_session:
            # Get base currency for conversion
            base_currency_id = int(data.get('base_currency_id'))
            sale_currency_id = int(data.get('currency_id'))

            # Get workflow type from frontend
            workflow_type = data.get('workflow_type', 'process_payment')
            order_reference = data.get('order_reference')
            payment_method = data.get('payment_method')

            # Determine status based on workflow type
            if workflow_type == 'order_slip':
                status = 'draft'
                payment_status = 'pending'
            else:
                status = 'paid'
                payment_status = 'full' if Decimal(str(data.get('amount_paid', 0))) >= Decimal(
                    str(data.get('total_amount', 0))) else 'partial'

            # Get customer ID
            customer_id = None
            if data.get('customer_id'):
                customer_id = data.get('customer_id')
            else:
                # Get or create customer
                customer_data = data.get('customer_data', {})
                customer_group_id = data.get('customer_group_id')
                customer = get_or_create_vendor(
                    db_session=db_session,
                    app_id=app_id,
                    customer_data=customer_data,
                    customer_group_id=customer_group_id
                )
                customer_id = customer.id

            # Get exchange rate for conversion
            exchange_rate_obj, exchange_rate_value = get_exchange_rate_and_obj(db_session, sale_currency_id,
                                                                               base_currency_id, app_id)
            if not exchange_rate_obj:
                return jsonify({'success': False, 'message': 'Exchange rate not found'}), 400

            # Calculate totals in sale currency
            subtotal = Decimal('0')

            # Get location from frontend and query the location object
            location_id = int(data.get('location_id'))
            if not location_id:
                return jsonify({'success': False, 'message': 'Location is required'}), 400

            # QUERY THE LOCATION OBJECT
            location = db_session.query(InventoryLocation).filter_by(
                id=location_id,
                app_id=app_id
            ).first()
            if not location:
                return jsonify({'success': False, 'message': 'Invalid location'}), 400
            else:
                if location.payment_account:
                    payment_account_id = location.payment_account_id
                else:
                    return jsonify(
                        {'success': False, 'message': f'Please configure Payment Account for {location.location}'}), 400

            # Pre-fetch all inventory items in a single query
            item_ids = [item['item_id'] for item in data['items']]
            inventory_items_map = {}
            if item_ids:
                inventory_items = db_session.query(InventoryItem).filter(
                    InventoryItem.id.in_(item_ids),
                    InventoryItem.app_id == app_id
                ).all()
                inventory_items_map = {item.id: item for item in inventory_items}

            for item in data['items']:
                item_subtotal = Decimal(str(item['quantity'])) * Decimal(str(item['unit_price']))
                subtotal += item_subtotal

            discount_amount = Decimal(str(data.get('discount_amount', 0)))
            tax_rate = Decimal(str(data.get('tax_rate', 0)))
            tax_amount = (subtotal - discount_amount) * (tax_rate / Decimal('100'))
            total_amount = subtotal - discount_amount + tax_amount

            # Generate direct sale number
            direct_sale_number = generate_direct_sale_number()

            # Create direct sale transaction
            sale = DirectSalesTransaction(
                app_id=app_id,
                customer_id=customer_id,
                direct_sale_number=direct_sale_number,
                payment_date=datetime.now(),
                total_amount=float(total_amount),
                amount_paid=float(Decimal(str(data.get('amount_paid', total_amount)))),
                total_line_subtotal=float(subtotal),
                currency_id=sale_currency_id,
                sales_tax_rate=float(tax_rate),
                sales_discount_type=data.get('discount_type', 'amount'),
                sales_discount_value=float(discount_amount),
                calculated_discount_amount=float(discount_amount),
                total_tax_amount=float(tax_amount),
                payment_status=payment_status,
                status=status,
                created_by=current_user.id,
                sale_reference=order_reference
            )

            db_session.add(sale)
            db_session.flush()  # Get the sale ID

            # Prepare data for processing
            inventory_items = []
            quantities = []
            unit_prices_base = []
            selling_prices_base = []

            # Add sale items and prepare inventory data
            for item_data in data['items']:
                inventory_item = inventory_items_map.get(item_data['item_id'])

                unit_price_sale_currency = Decimal(str(item_data['unit_price']))
                unit_price_base_currency = unit_price_sale_currency * exchange_rate_value

                # Create sale item
                sale_item = DirectSaleItem(
                    transaction_id=sale.id,
                    app_id=app_id,
                    item_type='inventory',
                    item_id=item_data['variation_id'],
                    item_name=item_data['name'],
                    quantity=item_data['quantity'],
                    uom=inventory_item.uom_id if inventory_item else 1,
                    unit_price=float(unit_price_sale_currency),
                    total_price=float(unit_price_sale_currency * Decimal(str(item_data['quantity']))),
                    tax_rate=0,
                    tax_amount=0,
                    location_id=location_id,
                    discount_amount=0,
                    discount_rate=0
                )
                db_session.add(sale_item)

                # Prepare inventory data
                inventory_items.append(item_data['variation_id'])
                quantities.append(item_data['quantity'])
                unit_prices_base.append(float(unit_price_base_currency))
                selling_prices_base.append(float(unit_price_base_currency))

            # Process inventory
            process_inventory_entries(
                db_session=db_session,
                app_id=app_id,
                inventory_items=inventory_items,
                quantities=quantities,
                unit_prices=unit_prices_base,
                selling_prices=selling_prices_base,
                location=location_id,
                from_location=location_id,
                to_location=None,
                transaction_date=datetime.now(),
                supplier_id=customer_id,
                form_currency_id=base_currency_id,
                base_currency_id=base_currency_id,
                expiration_date=None,
                reference=direct_sale_number,
                write_off_reason=None,
                project_id=None,
                movement_type='stock_out_sale',
                current_user_id=current_user.id,
                source_type='pos_sale',
                source_id=sale.id,
                sales_account_id=payment_account_id,
                exchange_rate_id=exchange_rate_obj.id if hasattr(exchange_rate_obj, 'id') else None,
                is_posted_to_ledger=True
            )

            # Post to ledger - if this fails, ROLL BACK EVERYTHING
            try:
                post_pos_transaction_to_ledger(
                    db_session=db_session,
                    base_currency_id=base_currency_id,
                    direct_sale=sale,
                    sale_items=[],  # We don't have the sale items as objects, but we have the data
                    current_user=current_user,
                    items_data=data['items'],
                    exchange_rate=exchange_rate_obj,  # Pass the exchange rate object
                    inventory_items_map=inventory_items_map,
                    payment_method=payment_method,
                    location=location  # Pass the location OBJECT, not just the ID
                )
            except Exception as ledger_error:
                logger.error(f"Ledger posting failed: {ledger_error}\n{traceback.format_exc()}")
                # ROLL BACK THE ENTIRE TRANSACTION
                db_session.rollback()
                return jsonify({
                    'success': False,
                    'message': f'Failed to post transaction to ledger due to error {ledger_error}. Sale has been cancelled.'
                }), 500

            # If we get here, ledger posting was successful - commit everything
            db_session.commit()

            return jsonify({
                'success': True,
                'sale_id': sale.id,
                'sale_number': direct_sale_number,
                'total_amount': float(total_amount),
                'currency_id': sale_currency_id,
                'workflow_type': workflow_type,
                'status': status
            })

    except Exception as e:
        # This will catch any other exceptions and roll back
        if 'db_session' in locals():
            db_session.rollback()
        logger.error(f"Error processing checkout: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': 'Error processing checkout'}), 500


@pos_bp.route('/api/customers/search')
@role_required(['Admin', 'Sales'])
def search_pos_customers():
    """Search customers for POS"""
    try:
        app_id = current_user.app_id
        search_term = request.args.get('q', '')
        logger.info(f'Data is {request.args}')
        with Session() as db_session:
            customers = db_session.query(Vendor).options(
                joinedload(Vendor.customer_group)
            ).filter(
                Vendor.app_id == app_id,
                Vendor.vendor_type == 'Customer',
                or_(
                    Vendor.vendor_name.ilike(f'%{search_term}%'),
                    Vendor.email.ilike(f'%{search_term}%'),
                    Vendor.tel_contact.ilike(f'%{search_term}%')
                )
            ).limit(10).all()

            customer_list = [{
                'id': c.id,
                'name': c.vendor_name,
                'email': c.email,
                'phone': c.tel_contact,
                'address': c.address,
                'customer_group_id': c.customer_group_id,
                'default_discount_percentage': float(
                    c.customer_group.default_discount_percentage) if c.customer_group else 0
            } for c in customers]

            return jsonify({'success': True, 'customers': customer_list})

    except Exception as e:
        logger.error(f"Error searching customers: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': 'Error searching customers'}), 500
