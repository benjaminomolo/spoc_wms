import traceback
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from flask import request, jsonify, render_template, flash, redirect, url_for, abort
from flask_login import login_required, current_user
from sqlalchemy import or_, func, literal, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from ai import get_base_currency
from db import Session
from models import Company, Module, InventoryLocation, ChartOfAccounts, Vendor, DirectSalesTransaction, \
    InventoryItemVariationLink, DirectSaleItem, Project, PaymentMode, UnitOfMeasurement, Currency, PaymentAllocation, \
    SalesTransaction, SalesPaymentStatus, SalesInvoice, InvoiceStatus, OrderStatus, CustomerCredit, \
    DirectPurchaseTransaction, InventoryItem, DirectPurchaseItem, PurchasePaymentAllocation, GoodsReceipt, \
    GoodsReceiptItem, PurchaseReturn, ExchangeRate
from services.chart_of_accounts_helpers import group_accounts_by_category
from services.inventory_helpers import safe_clear_stock_history_cache
from services.post_to_ledger import post_direct_sale_cogs_to_ledger, post_sales_transaction_to_ledger, \
    bulk_post_sales_transactions, post_customer_credit_to_ledger, \
    post_purchase_transaction_to_ledger
from services.post_to_ledger_reversal import reverse_direct_sales_posting, reverse_sales_invoice_posting, \
    reverse_sales_transaction_posting, delete_journal_entries_by_source
from services.purchases_helpers import generate_direct_purchase_number, allocate_direct_purchase_payment, \
    get_inventory_entries_for_direct_purchase, reverse_purchase_inventory_entries, reverse_direct_purchase_posting, \
    calculate_direct_purchase_landed_costs
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    get_inventory_entries_for_direct_sale, reverse_sales_inventory_entries, allocate_payment
from services.vendors_and_customers import get_or_create_customer_credit_account
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj, get_or_create_exchange_rate_for_transaction
from utils_and_helpers.forms import get_locked_record
from utils_and_helpers.lists import check_list_not_empty
from . import purchases_bp

import logging

logger = logging.getLogger(__name__)


@purchases_bp.route('/add_purchase_transaction', methods=['GET', 'POST'])
@login_required
def add_purchase_transaction():
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

    warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

    funding_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            or_(
                ChartOfAccounts.is_cash.is_(True),
                ChartOfAccounts.is_bank.is_(True)
            )
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    expense_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.parent_account_type == 'Expense'
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )
    if request.method == 'POST':

        try:
            # Get the action parameter
            action = request.form.get('action', 'save_and_exit')
            # Validate that at least one item is added
            check_list_not_empty(lst=request.form.getlist('item_type[]'))

            # Extract vendor data
            vendor_id = request.form.get('vendor_id')
            if vendor_id.isdigit():
                # Existing vendor (convert ID to integer)
                vendor_id = int(vendor_id)

            else:
                # One-time vendor (vendor_id is actually the name)
                vendor_name = vendor_id

                # Check if a one-time vendor with the same name exists
                existing_vendor = db_session.query(Vendor).filter_by(
                    vendor_name=vendor_name,
                    is_one_time=True,
                    app_id=app_id
                ).first()

                if existing_vendor:
                    vendor_id = existing_vendor.id  # Use existing one-time vendor
                else:
                    # Create a new one-time vendor
                    new_vendor = Vendor(
                        vendor_name=vendor_name,
                        is_one_time=True,
                        vendor_type="Supplier",
                        app_id=app_id
                    )
                    db_session.add(new_vendor)
                    db_session.flush()
                    vendor_id = new_vendor.id  # Use new vendor's ID

            direct_purchase_number = generate_direct_purchase_number(db_session, app_id)

            # Extract payment details
            payment_date = datetime.strptime(request.form['purchase_transaction_date'], '%Y-%m-%d').date()

            amount_paid = round(float(request.form['amount_paid']), 2)

            total_line_subtotal = round(float(request.form['overall_line_subtotal']), 2)
            total_amount = round(float(request.form['total_amount']), 2)
            currency_id = int(request.form['currency'])

            payment_mode = int(request.form['payment_mode']) if request.form['payment_mode'] else None

            payment_account = int(request.form['fundingAccount'])

            # Extract expense accounts
            expense_account_service = int(request.form['expenseAccount']) if request.form.get(
                'expenseAccount') else None
            shipping_handling_account = int(request.form['shippingHandlingAccount']) if request.form.get(
                'shippingHandlingAccount') else None
            expense_account_non_inventory = int(request.form['nonInventoryExpenseAccount']) if request.form.get(
                'nonInventoryExpenseAccount') else None
            purchase_reference = request.form['purchase_reference'] or None

            terms_and_conditions = request.form['terms_and_conditions'] or None

            shipping_cost = float(request.form['shipping_cost'])

            handling_cost = float(request.form['handling_cost'])

            purchase_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0

            purchase_discount_type = request.form['overall_discount_type'] or None
            purchase_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0

            total_tax_amount = request.form['total_tax'] or None
            project_id = request.form['project'] or None

            if purchase_discount_type == "amount":
                calculated_discount_amount = purchase_discount_value
            else:
                calculated_discount_amount = (total_line_subtotal * purchase_discount_value) / 100

            net_inventory_total = request.form.get('net_inventory_total') or None
            net_non_inventory_total = request.form.get('net_non_inventory_total') or None
            total_expenses = request.form.get('total_expenses') or None

            # Extract exchange rate

            base_currency_id = int(request.form.get('base_currency_id')) if request.form.get('base_currency_id') else None
            exchange_rate = request.form.get('exchange_rate')

            # Handle exchange rate for the purchase
            exchange_rate_id = None
            exchange_rate_value = None
            rate_obj = None

            if base_currency_id and currency_id != base_currency_id:
                # Validate exchange rate
                if not exchange_rate or exchange_rate.strip() == '':
                    return jsonify({
                        'success': False,
                        'message': 'Exchange rate is required for foreign currency transactions'
                    }), 400

                try:
                    exchange_rate_value = float(exchange_rate)
                    if exchange_rate_value <= 0:
                        return jsonify({
                            'success': False,
                            'message': 'Exchange rate must be greater than 0'
                        }), 400
                except ValueError:
                    return jsonify({
                        'success': False,
                        'message': 'Invalid exchange rate format'
                    }), 400

                # Create exchange rate record
                rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=db_session,
                    action='create',
                    from_currency_id=currency_id,
                    to_currency_id=base_currency_id,
                    rate_value=exchange_rate_value,
                    rate_date=payment_date,
                    app_id=app_id,
                    created_by=current_user.id,
                    source_type='purchase',
                    source_id=None,  # Will update after purchase is created
                    currency_exchange_transaction_id=None
                )

                exchange_rate_id = rate_id

            # Create new direct purchase object
            new_direct_purchase = DirectPurchaseTransaction(
                vendor_id=vendor_id,
                payment_date=payment_date,
                amount_paid=amount_paid,
                total_amount=total_amount,
                total_line_subtotal=total_line_subtotal,
                currency_id=currency_id,

                inventory_total=net_inventory_total,
                non_inventory_total=net_non_inventory_total,
                expenses_total=total_expenses,

                purchase_reference=purchase_reference,
                terms_and_conditions=terms_and_conditions,
                direct_purchase_number=direct_purchase_number,
                calculated_discount_amount=calculated_discount_amount,
                total_tax_amount=total_tax_amount,
                purchase_discount_type=purchase_discount_type,
                purchase_discount_value=purchase_discount_value,
                shipping_cost=shipping_cost,
                handling_cost=handling_cost,
                purchase_tax_rate=purchase_tax_rate,
                created_by=current_user.id,
                app_id=app_id,
                project_id=project_id,
            )
            db_session.add(new_direct_purchase)
            db_session.flush()

            if rate_obj:
                rate_obj.source_id = new_direct_purchase.id
                db_session.add(rate_obj)

            # Handle direct purchase items
            item_type_list = request.form.getlist('item_type[]')
            item_name_list = request.form.getlist('item_name[]')
            inventory_item_list = request.form.getlist('inventory_item[]')
            description_list = request.form.getlist('item_description[]')
            qty_list = request.form.getlist('quantity[]')
            uom_list = request.form.getlist('uom[]')
            unit_price_list = request.form.getlist('unit_price[]')
            warehouse_list = request.form.getlist('warehouse[]')
            discount_list = request.form.getlist('discount_value[]')
            tax_list = request.form.getlist('tax_value[]')
            subtotal_list = request.form.getlist('total_price[]')

            # Store items temporarily for landed cost calculation
            purchase_items = []
            for idx, (item_type, non_inventory_item, inventory_item, description, qty, warehouse, uom, unit_price,
                      discount_amount,
                      tax_amount, subtotal) in enumerate(zip(
                item_type_list, item_name_list, inventory_item_list, description_list, qty_list, warehouse_list,
                uom_list,
                unit_price_list, discount_list, tax_list, subtotal_list),
                start=1):

                # Handling item ID based on type
                valid_item_id = int(inventory_item) if item_type == "inventory" and inventory_item else None
                item_id = None if item_type != "inventory" else valid_item_id

                # If not an inventory item, store item name instead
                item_name = None if item_type == "inventory" else non_inventory_item

                # Fetch description for inventory items
                if item_type == "inventory":
                    inventory_item = db_session.query(InventoryItem).filter_by(id=item_id,
                                                                               app_id=app_id).first()
                    description = inventory_item.item_description if inventory_item else None

                # Convert values safely
                discount_amount = float(discount_amount) if discount_amount else 0
                tax_amount = float(tax_amount) if tax_amount else 0

                # Prevent division by zero
                subtotal_after_tax = float(subtotal) - tax_amount if float(subtotal) != 0 else 1
                subtotal_after_discount = float(subtotal) - tax_amount + discount_amount if float(subtotal) != 0 else 1

                discount_rate = round((discount_amount / subtotal_after_discount) * 100, 2)
                tax_rate = round((tax_amount / subtotal_after_tax) * 100, 2)

                # Create direct purchase item
                # Create direct purchase item (without landed cost initially)
                new_item = DirectPurchaseItem(
                    transaction_id=new_direct_purchase.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    description=description,
                    quantity=qty,
                    unit_price=unit_price,
                    total_price=subtotal,
                    currency=currency_id,
                    uom=uom,
                    discount_amount=discount_amount,
                    discount_rate=discount_rate,
                    tax_amount=tax_amount,
                    tax_rate=tax_rate,
                    location_id=warehouse,
                    app_id=app_id,
                    unit_cost=None,  # Will be calculated later
                    total_cost=None  # Will be calculated later
                )
                db_session.add(new_item)
                db_session.flush()

                # After creating new_direct_purchase and flushing
                if exchange_rate_id and rate_obj:
                    rate_obj.source_id = new_direct_purchase.id
                    db_session.add(rate_obj)

                # Store item for landed cost calculation
                purchase_items.append(new_item)

            # NEW: Calculate and update landed costs for inventory items
            calculate_direct_purchase_landed_costs(
                db_session=db_session,
                direct_purchase=new_direct_purchase,
                purchase_items=purchase_items,
                app_id=app_id
            )

            allocate_direct_purchase_payment(
                db_session=db_session,
                payment_amount=Decimal(amount_paid),
                direct_purchase_id=new_direct_purchase.id,
                payment_mode_id=payment_mode,
                payment_account_id=payment_account,
                credit_purchase_account_id=None,
                payment_date=payment_date,
                inventory_account_id=None,
                non_inventory_account_id=expense_account_non_inventory,
                other_expense_account_id=shipping_handling_account,
                other_expense_service_id=expense_account_service,
                reference=purchase_reference,
                exchange_rate_id=exchange_rate_id,
                is_posted_to_ledger=True
            )

            db_session.flush()
            # ✅ POST TO LEDGER WITH UNPOSTED STATUS
            ledger_success, ledger_message = post_purchase_transaction_to_ledger(
                db_session=db_session,
                purchase_transaction=new_direct_purchase,
                current_user=current_user,
                status='Posted',
                base_currency_id=base_currency_id
            )

            if not ledger_success:
                raise Exception(f"Ledger posting failed: {ledger_message}")
            # After successfully processing inventory entry
            safe_clear_stock_history_cache(logger)
            # Now commit everything
            db_session.commit()

            # Return JSON success response
            # Return JSON success response with action info
            return jsonify({
                'success': True,
                'message': 'Direct purchase added successfully!',
                'direct_purchase_id': direct_purchase_number,
                'action': action
            })

        except ValueError as err:
            return jsonify({
                'success': False,
                'message': f'An error occurred: {err}'
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f"An error occurred: {str(e)}\n{traceback.format_exc()}")
            return jsonify({
                'success': False,
                'message': f'An error occurred: {str(e)}'
            })

        finally:
            db_session.close()

    else:
        # List of acceptable vendor types
        vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers']

        # Normalize to lowercase for consistency
        vendor_types = [v.lower() for v in vendor_types]

        # Query vendors whose vendor_type matches any in the list
        vendors = (
            db_session.query(Vendor)
            .filter(
                Vendor.app_id == app_id,
                Vendor.is_one_time == False,
                func.lower(Vendor.vendor_type).in_(vendor_types)
            )
            .all()
        )
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id
        inventory_items = db_session.query(InventoryItemVariationLink).join(InventoryItem).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.status == "active"
        ).order_by(InventoryItem.item_name.asc()).all()

        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
        projects = db_session.query(Project).filter_by(app_id=app_id).all()
        # Render the template with the next direct purchase number and vendors
        return render_template('/purchases/direct_purchase.html',
                               currencies=currencies,
                               base_currency=base_currency,
                               base_currency_code=base_currency_code,
                               base_currency_id=base_currency_id,
                               inventory_items=inventory_items,
                               uoms=uoms,
                               vendors=vendors,
                               modules=modules_data,
                               payment_modes=payment_modes,
                               company=company,
                               role=role,
                               warehouses=warehouses,
                               grouped_funding_accounts=group_accounts_by_category(funding_accounts),
                               grouped_expense_accounts=group_accounts_by_category(expense_accounts),
                               projects=projects)


@purchases_bp.route('/edit_direct_purchase_transaction/<int:direct_purchase_id>', methods=['GET', 'POST'])
@login_required
def edit_direct_purchase_transaction(direct_purchase_id):
    app_id = current_user.app_id
    db_session = Session()

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

        funding_accounts = (
            db_session.query(ChartOfAccounts)
            .filter(
                ChartOfAccounts.app_id == app_id,
                or_(
                    ChartOfAccounts.is_cash.is_(True),
                    ChartOfAccounts.is_bank.is_(True)
                )
            )
            .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
            .all()
        )

        expense_accounts = (
            db_session.query(ChartOfAccounts)
            .filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.parent_account_type == 'Expense'
            )
            .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
            .all()
        )

        grouped_funding_accounts = group_accounts_by_category(funding_accounts)
        grouped_expense_accounts = group_accounts_by_category(expense_accounts)

        # Fetch the existing direct purchase transaction
        # In your edit route, when loading the purchase
        direct_purchase = db_session.query(DirectPurchaseTransaction).options(
            joinedload(DirectPurchaseTransaction.payment_allocations)  # Load payment allocations
        ).filter_by(id=direct_purchase_id, app_id=app_id).first()


        if not direct_purchase:
            return jsonify({
                'success': False,
                'message': 'Direct purchase not found!'
            }), 404

        if request.method == 'POST':
            # Get the action parameter
            action = request.form.get('action', 'save_and_exit')

            form_version = request.form.get('version', type=int)

            locked_direct_purchase = get_locked_record(db_session, DirectPurchaseTransaction, direct_purchase_id, form_version)

            if not locked_direct_purchase:
                return jsonify({
                    'success': False,
                    'message': 'This Purchase Transaction was modified by another user. Please refresh and try again.',
                    'error_type': 'optimistic_lock_failed'
                }), 409  # 409 Conflict is appropriate for version conflicts

            # Now work with the locked record
            direct_purchase = locked_direct_purchase

            # Validate that at least one item is added
            check_list_not_empty(lst=request.form.getlist('item_type[]'))

            # ✅ REVERSAL: Reverse inventory and ledger entries before editing
            try:
                inventory_entries = get_inventory_entries_for_direct_purchase(db_session, direct_purchase.id, app_id)
                for entry in inventory_entries:
                    reverse_purchase_inventory_entries(db_session, entry)

                reverse_direct_purchase_posting(db_session, direct_purchase, current_user)

            except Exception as inv_error:
                # Stop everything here if reversal fails
                db_session.rollback()
                logger.error(f"Failed to reverse inventory/ledger: {str(inv_error)}")
                return jsonify({
                    'success': False,
                    'message': f'Cannot edit direct purchase: {str(inv_error)}'
                })

            # Extract vendor data
            vendor_id = request.form.get('vendor_id')
            if vendor_id.isdigit():
                # Existing vendor (convert ID to integer)
                vendor_id = int(vendor_id)
            else:
                # One-time vendor (vendor_id is actually the name)
                vendor_name = vendor_id

                # Check if a one-time vendor with the same name exists
                existing_vendor = db_session.query(Vendor).filter_by(
                    vendor_name=vendor_name,
                    is_one_time=True,
                    app_id=app_id
                ).first()

                if existing_vendor:
                    vendor_id = existing_vendor.id
                else:
                    # Create a new one-time vendor
                    new_vendor = Vendor(
                        vendor_name=vendor_name,
                        is_one_time=True,
                        vendor_type="Supplier",
                        app_id=app_id
                    )
                    db_session.add(new_vendor)
                    db_session.flush()
                    vendor_id = new_vendor.id



            # Extract payment details
            payment_date = datetime.strptime(request.form['purchase_transaction_date'], '%Y-%m-%d').date()
            amount_paid = round(float(request.form['amount_paid']), 2)
            total_line_subtotal = round(float(request.form['overall_line_subtotal']), 2)
            total_amount = round(float(request.form['total_amount']), 2)
            currency_id = int(request.form['currency'])

            # Extract exchange rate
            exchange_rate = request.form.get('exchange_rate')
            base_currency_id = int(request.form.get('base_currency_id')) if request.form.get('base_currency_id') else None

            # Handle exchange rate for the purchase
            exchange_rate_id = None
            exchange_rate_value = None

            if base_currency_id and currency_id != base_currency_id:
                # Validate exchange rate
                if not exchange_rate or exchange_rate.strip() == '':
                    return jsonify({
                        'success': False,
                        'message': 'Exchange rate is required for foreign currency transactions'
                    }), 400

                try:
                    exchange_rate_value = float(exchange_rate)
                    if exchange_rate_value <= 0:
                        return jsonify({
                            'success': False,
                            'message': 'Exchange rate must be greater than 0'
                        }), 400
                except ValueError:
                    return jsonify({
                        'success': False,
                        'message': 'Invalid exchange rate format'
                    }), 400

                # Check if exchange rate already exists
                # Get the first payment allocation for this purchase
                existing_allocation = db_session.query(PurchasePaymentAllocation).filter_by(
                    direct_purchase_id=direct_purchase.id
                ).first()

                if existing_allocation and existing_allocation.exchange_rate_id:
                    # Update existing exchange rate
                    existing_rate = db_session.query(ExchangeRate).get(existing_allocation.exchange_rate_id)
                    if existing_rate:
                        existing_rate.rate = Decimal(str(exchange_rate_value))
                        existing_rate.date = payment_date
                        db_session.add(existing_rate)
                        exchange_rate_id = existing_rate.id
                else:
                    # Create new exchange rate
                    rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                        session=db_session,
                        action='create',
                        from_currency_id=currency_id,
                        to_currency_id=base_currency_id,
                        rate_value=exchange_rate_value,
                        rate_date=payment_date,
                        app_id=app_id,
                        created_by=current_user.id,
                        source_type='purchase',
                        source_id=direct_purchase.id,
                        currency_exchange_transaction_id=None
                    )
                    exchange_rate_id = rate_id

            payment_mode = int(request.form['payment_mode']) if request.form['payment_mode'] else None
            payment_account = int(request.form['fundingAccount'])

            # Extract expense accounts
            expense_account_service = int(request.form['expenseAccount']) if request.form.get(
                'expenseAccount') else None
            shipping_handling_account = int(request.form['shippingHandlingAccount']) if request.form.get(
                'shippingHandlingAccount') else None
            expense_account_non_inventory = int(request.form['nonInventoryExpenseAccount']) if request.form.get(
                'nonInventoryExpenseAccount') else None

            purchase_reference = request.form['purchase_reference'] or None
            terms_and_conditions = request.form['terms_and_conditions'] or None
            shipping_cost = float(request.form['shipping_cost'])
            handling_cost = float(request.form['handling_cost'])
            purchase_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0
            purchase_discount_type = request.form['overall_discount_type'] or None
            purchase_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0
            total_tax_amount = request.form['total_tax'] or None
            project_id_str = request.form['project'] or None
            project_id = empty_to_none(project_id_str)

            if purchase_discount_type == "amount":
                calculated_discount_amount = purchase_discount_value
            else:
                calculated_discount_amount = (total_line_subtotal * purchase_discount_value) / 100

            net_inventory_total = request.form.get('net_inventory_total') or None
            net_non_inventory_total = request.form.get('net_non_inventory_total') or None
            total_expenses = request.form.get('total_expenses') or None

            # Update direct purchase object
            direct_purchase.vendor_id = vendor_id
            direct_purchase.payment_date = payment_date
            direct_purchase.amount_paid = amount_paid
            direct_purchase.total_amount = total_amount
            direct_purchase.total_line_subtotal = total_line_subtotal
            direct_purchase.currency_id = currency_id
            direct_purchase.inventory_total = net_inventory_total
            direct_purchase.non_inventory_total = net_non_inventory_total
            direct_purchase.expenses_total = total_expenses
            direct_purchase.purchase_reference = purchase_reference
            direct_purchase.terms_and_conditions = terms_and_conditions
            direct_purchase.calculated_discount_amount = calculated_discount_amount
            direct_purchase.total_tax_amount = total_tax_amount
            direct_purchase.purchase_discount_type = purchase_discount_type
            direct_purchase.purchase_discount_value = purchase_discount_value
            direct_purchase.shipping_cost = shipping_cost
            direct_purchase.handling_cost = handling_cost
            direct_purchase.purchase_tax_rate = purchase_tax_rate
            direct_purchase.project_id = project_id
            direct_purchase.is_complete_receipt = True

            # Delete existing items and allocations (after reversal)
            db_session.query(DirectPurchaseItem).filter_by(transaction_id=direct_purchase.id).delete()
            db_session.query(PurchasePaymentAllocation).filter_by(direct_purchase_id=direct_purchase.id).delete()

            # Handle direct purchase items
            item_type_list = request.form.getlist('item_type[]')
            item_name_list = request.form.getlist('item_name[]')
            inventory_item_list = request.form.getlist('inventory_item[]')
            description_list = request.form.getlist('item_description[]')
            qty_list = request.form.getlist('quantity[]')
            uom_list = request.form.getlist('uom[]')
            unit_price_list = request.form.getlist('unit_price[]')
            warehouse_list = request.form.getlist('warehouse[]')
            discount_list = request.form.getlist('discount_value[]')
            tax_list = request.form.getlist('tax_value[]')
            subtotal_list = request.form.getlist('total_price[]')

            # Store items temporarily for landed cost calculation
            purchase_items = []

            for idx, (item_type, non_inventory_item, inventory_item, description, qty, warehouse, uom, unit_price,
                      discount_amount, tax_amount, subtotal) in enumerate(zip(
                item_type_list, item_name_list, inventory_item_list, description_list, qty_list, warehouse_list,
                uom_list, unit_price_list, discount_list, tax_list, subtotal_list), start=1):

                # Handling item ID based on type
                valid_item_id = int(inventory_item) if item_type == "inventory" and inventory_item else None
                item_id = None if item_type != "inventory" else valid_item_id

                # If not an inventory item, store item name instead
                item_name = None if item_type == "inventory" else non_inventory_item

                # Fetch description for inventory items
                if item_type == "inventory":
                    inventory_item_obj = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()
                    description = inventory_item_obj.item_description if inventory_item_obj else None

                # Convert values safely
                discount_amount = float(discount_amount) if discount_amount else 0
                tax_amount = float(tax_amount) if tax_amount else 0

                # Prevent division by zero
                subtotal_after_tax = float(subtotal) - tax_amount if float(subtotal) != 0 else 1
                subtotal_after_discount = float(subtotal) - tax_amount + discount_amount if float(subtotal) != 0 else 1

                discount_rate = round((discount_amount / subtotal_after_discount) * 100, 2)
                tax_rate = round((tax_amount / subtotal_after_tax) * 100, 2)

                # Create direct purchase item
                new_item = DirectPurchaseItem(
                    transaction_id=direct_purchase.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    description=description,
                    quantity=qty,
                    unit_price=unit_price,
                    total_price=subtotal,
                    currency=currency_id,
                    uom=uom,
                    discount_amount=discount_amount,
                    discount_rate=discount_rate,
                    tax_amount=tax_amount,
                    tax_rate=tax_rate,
                    location_id=warehouse,
                    app_id=app_id
                )
                db_session.add(new_item)

                db_session.flush()

                # Store item for landed cost calculation - ADD THIS LINE
                purchase_items.append(new_item)

            # NEW: Calculate and update landed costs for inventory items - ADD THIS SECTION
            calculate_direct_purchase_landed_costs(
                db_session=db_session,
                direct_purchase=direct_purchase,
                purchase_items=purchase_items,
                app_id=app_id
            )
            # Allocate payment
            allocate_direct_purchase_payment(
                db_session=db_session,
                payment_amount=Decimal(amount_paid),
                direct_purchase_id=direct_purchase.id,
                payment_mode_id=payment_mode,
                payment_account_id=payment_account,
                credit_purchase_account_id=None,
                payment_date=payment_date,
                inventory_account_id=None,
                non_inventory_account_id=expense_account_non_inventory,
                other_expense_account_id=shipping_handling_account,
                other_expense_service_id=expense_account_service,
                reference=purchase_reference,
                is_posted_to_ledger=True,
                exchange_rate_id=exchange_rate_id
            )

            direct_purchase.status = "draft"
            db_session.flush()  # Ensure all operations are executed
            db_session.refresh(direct_purchase)



            # ✅ POST TO LEDGER WITH UNPOSTED STATUS
            ledger_success, ledger_message = post_purchase_transaction_to_ledger(
                db_session=db_session,
                purchase_transaction=direct_purchase,
                current_user=current_user,
                status='Posted',
                base_currency_id=base_currency_id
            )

            if not ledger_success:
                raise Exception(f"Ledger posting failed: {ledger_message}")
            direct_purchase.version+=1
            # After successfully processing inventory entry
            safe_clear_stock_history_cache(logger)
            # Now commit everything
            db_session.commit()

            return jsonify({
                'success': True,
                'message': 'Direct purchase updated successfully!',
                'direct_purchase_id': direct_purchase.id,
                'action': action
            })

        else:
            # GET request - render edit form
            vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers']
            vendor_types = [v.lower() for v in vendor_types]

            vendors = (
                db_session.query(Vendor)
                .filter(
                    Vendor.app_id == app_id,
                    func.lower(Vendor.vendor_type).in_(vendor_types)
                )
                .all()
            )
            currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
            base_currency = next((c for c in currencies if c.currency_index == 1), None)
            base_currency_code = base_currency.user_currency if base_currency else ''
            base_currency_id = base_currency.id


            inventory_items = db_session.query(InventoryItemVariationLink).join(InventoryItem).filter(
                InventoryItemVariationLink.app_id == app_id,
                InventoryItemVariationLink.status == "active"
            ).order_by(InventoryItem.item_name.asc()).all()


            uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()
            payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
            projects = db_session.query(Project).filter_by(app_id=app_id).all()
            # Get exchange rate for this purchase if it exists

            # Fetch existing direct purchase items
            direct_purchase_items = db_session.query(DirectPurchaseItem).filter_by(
                transaction_id=direct_purchase.id).all()

            return render_template('/purchases/edit_direct_purchase.html',
                                   currencies=currencies,
                                   base_currency=base_currency,
                                   base_currency_id=base_currency_id,
                                   base_currency_code=base_currency_code,
                                   inventory_items=inventory_items,
                                   uoms=uoms,
                                   vendors=vendors,
                                   modules=modules_data,
                                   payment_modes=payment_modes,
                                   company=company,
                                   role=role,
                                   direct_purchase=direct_purchase,
                                   direct_purchase_items=direct_purchase_items,
                                   warehouses=warehouses,
                                   grouped_expense_accounts=grouped_expense_accounts,
                                   grouped_funding_accounts=grouped_funding_accounts,
                                   projects=projects)

    except Exception as e:
        db_session.rollback()
        logger.error(f"An error occurred: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'An error occurred: {str(e)}'
        })
    finally:
        db_session.close()


@purchases_bp.route('/direct_purchase/<int:direct_purchase_id>', methods=['GET'])
@login_required
def direct_purchase_details(direct_purchase_id):
    app_id = current_user.app_id
    with Session() as db_session:
        # Get company and user data
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()

        # Load direct purchase with all relationships
        direct_purchase = db_session.query(DirectPurchaseTransaction).options(
            joinedload(DirectPurchaseTransaction.vendor),
            joinedload(DirectPurchaseTransaction.currency),
            joinedload(DirectPurchaseTransaction.user),
            joinedload(DirectPurchaseTransaction.direct_purchase_items)
            .joinedload(DirectPurchaseItem.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item),
            joinedload(DirectPurchaseTransaction.direct_purchase_items)
            .joinedload(DirectPurchaseItem.unit_of_measurement),
            joinedload(DirectPurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.payment_modes),
            joinedload(DirectPurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.inventory_account),
            joinedload(DirectPurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.non_inventory_account),
            joinedload(DirectPurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.chart_of_accounts_asset),
            joinedload(DirectPurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.chart_of_accounts_tax_receivable),
            joinedload(DirectPurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.chart_of_accounts_tax_payable),
            joinedload(DirectPurchaseTransaction.goods_receipts)
            .joinedload(GoodsReceipt.receipt_items)
            .joinedload(GoodsReceiptItem.direct_purchase_item)
        ).filter_by(id=direct_purchase_id, app_id=app_id).first()

        if not direct_purchase:
            abort(404, "Direct Purchase not found")

        # Check tax and inventory status

        has_credit_purchase = (any(allocation.credit_purchase_account is not None
                                   for allocation in direct_purchase.payment_allocations)
                               )

        has_inventory = any(
            item.item_type == "inventory" for item in direct_purchase.direct_purchase_items
        )

        has_non_inventory = any(
            item.item_type == "non-inventory" for item in direct_purchase.direct_purchase_items
        )

        has_service = any(
            item.item_type == "service" for item in direct_purchase.direct_purchase_items
        )

        has_other_expenses = (
                float(direct_purchase.shipping_cost) > 0 or
                float(direct_purchase.handling_cost) > 0
        )

        discount_exists = (
                float(direct_purchase.calculated_discount_amount) > 0
        )

        is_first_payment = not db_session.query(PurchasePaymentAllocation).join(DirectPurchaseTransaction) \
            .filter(
            PurchasePaymentAllocation.is_posted_to_ledger == True,
            DirectPurchaseTransaction.id == direct_purchase_id
        ).first()

        # 1. Fetch the very first allocation by id
        # Get the first allocation ID
        first_alloc = (
            db_session.query(PurchasePaymentAllocation)
            .filter_by(direct_purchase_id=direct_purchase_id, app_id=app_id)
            .order_by(PurchasePaymentAllocation.id.asc())
            .first()
        )
        first_alloc_id = first_alloc.id if first_alloc else None

        # Check if first payment is posted
        first_payment_posted = db_session.query(PurchasePaymentAllocation).filter(
            PurchasePaymentAllocation.direct_purchase_id == direct_purchase_id,
            PurchasePaymentAllocation.is_posted_to_ledger == True
        ).order_by(PurchasePaymentAllocation.id.asc()).first()

        # Calculate remaining balance
        remaining_balance = direct_purchase.total_amount - direct_purchase.amount_paid

        # Prepare payment allocations with account details
        payment_allocations = []
        for allocation in direct_purchase.payment_allocations:
            # Determine if the first allocation has been posted to ledger
            is_first_allocation = (allocation.id == first_alloc_id)
            can_post = (
                    (is_first_allocation and not allocation.is_posted_to_ledger) or
                    (not is_first_allocation and first_payment_posted is not None)
            )

            payment_allocations.append({
                "id": allocation.id,
                "payment_date": allocation.payment_date.strftime("%Y-%m-%d"),
                "amount_paid": float(allocation.allocated_inventory) + float(allocation.allocated_non_inventory) +
                               float(allocation.allocated_services) + float(allocation.allocated_other_expenses) +
                               float(allocation.allocated_tax_receivable) + float(allocation.allocated_tax_payable),
                "allocated_inventory": float(allocation.allocated_inventory),
                "allocated_non_inventory": float(allocation.allocated_non_inventory),
                "allocated_services": float(allocation.allocated_services),
                "allocated_other_expenses": float(allocation.allocated_other_expenses),
                "allocated_tax_receivable": float(allocation.allocated_tax_receivable),
                "allocated_tax_payable": float(allocation.allocated_tax_payable),
                "payment_mode": allocation.payment_modes.payment_mode if allocation.payment_modes else None,

                "payment_mode_id": allocation.payment_mode if allocation.payment_mode else None,
                "is_posted_to_ledger": allocation.is_posted_to_ledger,
                "can_post": can_post,  # <-- Add this flag
                "reference": allocation.reference,
                "payment_type": allocation.payment_type,
                "accounts": {
                    "inventory": {
                        "id": allocation.inventory_account_id,
                        "name": allocation.inventory_account.sub_category if allocation.inventory_account else None
                    },
                    "non_inventory": {
                        "id": allocation.non_inventory_account_id,
                        "name": allocation.non_inventory_account.sub_category if allocation.non_inventory_account else None
                    },
                    "payment": {
                        "id": allocation.payment_account_id,
                        "name": allocation.chart_of_accounts_asset.sub_category if allocation.chart_of_accounts_asset else None
                    },
                    "tax_receivable": {
                        "id": allocation.tax_receivable_account_id,
                        "name": allocation.chart_of_accounts_tax_receivable.sub_category if allocation.chart_of_accounts_tax_receivable else None
                    },
                    "tax_payable": {
                        "id": allocation.tax_payable_account_id,
                        "name": allocation.chart_of_accounts_tax_payable.sub_category if allocation.chart_of_accounts_tax_payable else None
                    }
                }
            })

        # Prepare goods receipts data
        goods_receipts_data = []
        total_received_quantities = defaultdict(float)

        for receipt in direct_purchase.goods_receipts:
            receipt_items = []
            for item in receipt.receipt_items:
                if item.direct_purchase_item_id:
                    total_received_quantities[item.direct_purchase_item_id] += float(item.quantity_received)

                po_item = next(
                    (i for i in direct_purchase.direct_purchase_items
                     if i.id == item.direct_purchase_item_id), None
                )

                receipt_items.append({
                    "id": item.id,
                    "direct_purchase_item_id": item.direct_purchase_item_id,
                    "quantity_received": float(item.quantity_received),
                    "received_condition": item.received_condition,
                    "notes": item.notes,
                    "inventory_adjusted": item.inventory_adjusted,
                    "is_posted_to_ledger": item.is_posted_to_ledger,
                    "item_type": po_item.item_type if po_item else None,
                    "item_name": po_item.item_name if po_item.item_name else po_item.inventory_item_variation_link.inventory_item.item_name
                })

            goods_receipts_data.append({
                "id": receipt.id,
                "receipt_number": receipt.receipt_number,
                "receipt_date": receipt.receipt_date.strftime("%Y-%m-%d"),
                "received_by": receipt.received_by,
                "is_complete_receipt": receipt.direct_purchase.is_complete_receipt,
                "is_posted_to_ledger": receipt.is_posted_to_ledger,
                "created_at": receipt.created_at.strftime("%Y-%m-%d %H:%M"),
                "receipt_items": receipt_items
            })

        # Prepare direct purchase items with received quantities
        purchase_items = []
        for item in direct_purchase.direct_purchase_items:
            total_received = total_received_quantities.get(item.id, 0.0)
            pending_quantity = float(item.quantity) - total_received

            purchase_items.append({
                "id": item.id,
                "item_type": item.item_type,
                "item_id": item.item_id,
                "item_name": item.item_name if item.item_name else item.inventory_item_variation_link.inventory_item.item_name,
                "description": item.description if item.item_name else item.inventory_item_variation_link.inventory_item.item_description,
                "quantity": float(item.quantity),
                "quantity_received": total_received,
                "quantity_pending": pending_quantity,
                "uom": item.unit_of_measurement.full_name if item.unit_of_measurement else None,
                "unit_price": float(item.unit_price),
                "unit_cost": float(item.unit_cost),
                "total_cost": float(item.total_cost),
                "tax_rate": float(item.tax_rate),
                "tax_amount": float(item.tax_amount),
                "discount_amount": float(item.discount_amount),
                "discount_rate": float(item.discount_rate),
                "total_price": float(item.total_price)
            })

        # Add this query to get purchase returns
        purchase_returns = db_session.query(PurchaseReturn).options(
            joinedload(PurchaseReturn.receipt_items)
            .joinedload(GoodsReceiptItem.direct_purchase_item)
        ).filter(
            PurchaseReturn.direct_purchase_id == direct_purchase_id,
            PurchaseReturn.app_id == app_id
        ).all()

        # Calculate total returned amounts
        total_returned_amount = sum(float(r.allocated_amount) for r in purchase_returns)
        total_returned_tax = sum(float(r.allocated_tax_amount) for r in purchase_returns)

        # Build the complete response
        direct_purchase_data = {
            "id": direct_purchase.id,
            "direct_purchase_number": direct_purchase.direct_purchase_number,
            "purchase_date": direct_purchase.payment_date.strftime("%Y-%m-%d"),
            "expected_delivery_date": direct_purchase.expected_delivery_date.strftime(
                "%Y-%m-%d") if direct_purchase.expected_delivery_date else None,
            "shipping_address": direct_purchase.shipping_address,
            "delivery_method": direct_purchase.delivery_method,
            "vendor": {
                "id": direct_purchase.vendor.id,
                "name": direct_purchase.vendor.vendor_name,
                "contact": f"{direct_purchase.vendor.tel_contact}{' | ' + direct_purchase.vendor.email if direct_purchase.vendor.email else ''}",
                "address": direct_purchase.vendor.address or None,
                "city_country": f"{direct_purchase.vendor.city or ''}{', ' + direct_purchase.vendor.country if direct_purchase.vendor.country else ''}".strip() or None
            },
            "currency": direct_purchase.currency.user_currency if direct_purchase.currency else None,
            "total_amount": float(direct_purchase.total_amount),
            "amount_paid": float(direct_purchase.amount_paid),
            "remaining_balance": float(remaining_balance),
            "status": direct_purchase.status,
            "subtotal": float(direct_purchase.total_line_subtotal),
            "payment_status": direct_purchase.payment_status,
            "payment_allocations": payment_allocations,
            "direct_purchase_items": purchase_items,
            "terms_and_conditions": direct_purchase.terms_and_conditions,
            "direct_purchase_discount": float(direct_purchase.calculated_discount_amount),
            "direct_purchase_discount_type": direct_purchase.purchase_discount_type,
            "direct_purchase_tax_rate": float(direct_purchase.purchase_tax_rate),
            "total_tax_amount": float(direct_purchase.total_tax_amount),
            "shipping_cost": float(direct_purchase.shipping_cost),
            "handling_cost": float(direct_purchase.handling_cost),
            "inventory_total": float(direct_purchase.payment_allocations[0].allocated_inventory),
            "non_inventory_total": float(direct_purchase.payment_allocations[0].allocated_non_inventory),
            "services_total": float(direct_purchase.payment_allocations[0].allocated_services),
            "expenses_total": float(direct_purchase.payment_allocations[0].allocated_other_expenses),
            "is_complete_receipt": direct_purchase.is_complete_receipt,
            "shipping_handling_posted": direct_purchase.shipping_handling_posted,

            "users": [{
                "id": user.id,
                "name": user.name,
                "email": user.email
            } for user in company.users],
            "created_by": {
                "id": direct_purchase.user.id,
                "name": direct_purchase.user.name,
                "email": direct_purchase.user.email
            },
            "created_at": direct_purchase.created_at.strftime("%Y-%m-%d %H:%M") if direct_purchase.created_at else None,

            "updated_at": direct_purchase.updated_at.strftime("%Y-%m-%d %H:%M") if direct_purchase.updated_at else None,
            "is_posted_to_ledger": direct_purchase.is_posted_to_ledger,
            "flags": {
                "has_inventory": has_inventory,
                "has_non_inventory": has_non_inventory,
                "has_service": has_service,
                "has_other_expenses": has_other_expenses,

                "has_credit_purchase": has_credit_purchase,
                "is_first_payment": is_first_payment,
                "discount_exists": False
                # Am not considering Discounts for now as each discount will be treated as Trade discount
            },
            "goods_receipts": goods_receipts_data,
            "can_create_receipt": any(
                item['quantity_pending'] > 0 for item in purchase_items) if purchase_items else False,
            "purchase_returns": [
                {
                    "id": r.id,
                    "return_number": r.return_number,
                    "item_name": r.receipt_items.direct_purchase_item.item_name if r.receipt_items.direct_purchase_item.item_name else r.receipt_items.direct_purchase_item.inventory_item_variation_link.inventory_item.item_name,
                    "return_date": r.return_date.strftime("%Y-%m-%d"),
                    "quantity": float(r.quantity),
                    "amount": float(r.allocated_amount),
                    "tax_amount": float(r.allocated_tax_amount),
                    "total_amount": float(r.allocated_amount + r.allocated_tax_amount),
                    "reason": r.reason,
                    "status": r.status,
                    "is_posted_to_ledger": r.is_posted_to_ledger
                } for r in purchase_returns
            ],
            "total_returned_amount": total_returned_amount,
            "total_returned_tax": total_returned_tax

        }

        if request.args.get('format') == 'json':
            return jsonify(direct_purchase_data)

        return render_template(
            '/purchases/direct_purchase_details.html',
            direct_purchase=direct_purchase_data,
            company=company,
            modules=modules_data,
            payment_modes=payment_modes,
            role=role,
            module_name="Purchase"
        )


@purchases_bp.route('/cancel_direct_purchase', methods=['POST'])
@login_required
def cancel_direct_purchase():
    db_session = Session()
    try:
        data = request.get_json()
        direct_purchase_id = data.get('direct_purchase_id')

        if not direct_purchase_id:
            return jsonify({'success': False, 'message': 'No purchase ID provided'}), 400

        direct_purchase = db_session.query(DirectPurchaseTransaction).filter_by(id=direct_purchase_id).first()

        if not direct_purchase:
            return jsonify({'success': False, 'message': 'Purchase transaction not found'}), 404

        # Check if already cancelled
        if direct_purchase.status in ['cancelled', 'canceled']:
            return jsonify({'success': False, 'message': 'Purchase transaction is already cancelled'}), 400

        # Reverse inventory entries
        try:
            inventory_entries = get_inventory_entries_for_direct_purchase(db_session, direct_purchase.id,
                                                                          current_user.app_id)
            for entry in inventory_entries:
                reverse_purchase_inventory_entries(db_session, entry)
        except Exception as inv_error:
            return jsonify({'success': False, 'message': f'Failed to reverse inventory: {str(inv_error)}'}), 400

        # Reverse ledger postings
        try:
            reverse_direct_purchase_posting(db_session, direct_purchase, current_user)
        except Exception as ledger_error:
            return jsonify({'success': False, 'message': f'Failed to reverse ledger entries: {str(ledger_error)}'}), 400

        # Update status to cancelled
        direct_purchase.status = OrderStatus.canceled

        # Clear stock history cache
        try:
            clear_stock_history_cache()
            logger.info("Successfully cleared stock history cache after cancellation")
        except Exception as cache_error:
            logger.error(f"Cache clearing failed during cancellation: {cache_error}")

        db_session.commit()

        return jsonify({
            'success': True,
            'message': f'Purchase transaction {direct_purchase.direct_purchase_number} cancelled successfully'
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in cancel_direct_purchase: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'Error cancelling purchase transaction: {str(e)}'
        }), 500
    finally:
        db_session.close()
