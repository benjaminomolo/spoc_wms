import json
import traceback
from datetime import datetime
from decimal import Decimal

from flask import request, jsonify, render_template, flash, redirect, url_for
from flask_login import login_required, current_user
from sqlalchemy import or_, func, literal, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from ai import get_base_currency
from db import Session
from models import Company, Module, InventoryLocation, ChartOfAccounts, Vendor, DirectSalesTransaction, \
    InventoryItemVariationLink, DirectSaleItem, Project, PaymentMode, UnitOfMeasurement, Currency, PaymentAllocation, \
    SalesTransaction, SalesPaymentStatus, SalesInvoice, InvoiceStatus, OrderStatus, CustomerCredit, CreditApplication, \
    InventoryItem
from services.chart_of_accounts_helpers import group_accounts_by_category, get_all_system_accounts
from services.post_to_ledger import post_direct_sale_cogs_to_ledger, post_sales_transaction_to_ledger, \
    bulk_post_sales_transactions, post_overpayment_write_off_to_ledger, post_customer_credit_to_ledger, \
    post_payment_receipt_to_ledger
from services.post_to_ledger_reversal import reverse_direct_sales_posting, reverse_sales_invoice_posting, \
    reverse_sales_transaction_posting, delete_journal_entries_by_source
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    get_inventory_entries_for_direct_sale, reverse_sales_inventory_entries, allocate_payment, \
    update_transaction_exchange_rate, update_invoice_status, manage_bulk_payment
from services.vendors_and_customers import get_or_create_customer_credit_account, delete_credit_applications, \
    cancel_all_credit_relationships
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj, get_or_create_exchange_rate_for_transaction
from utils_and_helpers.forms import get_int_or_none, get_locked_record
from utils_and_helpers.lists import check_list_not_empty
from . import sales_bp

import logging

from .customer_credits import manage_customer_credits, reverse_credit_application, update_credit_status

logger = logging.getLogger(__name__)


@sales_bp.route('/add_sales_transaction', methods=['GET', 'POST'])
@login_required
def add_sales_transaction():
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
    warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
    # Query to get funding accounts (cash or bank) for the current app
    funding_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.is_system_account.is_(False),
            or_(
                ChartOfAccounts.is_cash.is_(True),
                ChartOfAccounts.is_bank.is_(True)
            )
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    # Receivable accounts (is_receivable == True)
    receivable_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.is_receivable.is_(True),
            ChartOfAccounts.is_system_account.is_(False)
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    # Receivable accounts (is_receivable == True)
    tax_payable_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.is_payable.is_(True),
            ChartOfAccounts.is_system_account.is_(False)
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    revenue_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.parent_account_type == 'Income',
            ChartOfAccounts.is_system_account.is_(False)
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    grouped_funding_accounts = group_accounts_by_category(funding_accounts)
    grouped_receivable_accounts = group_accounts_by_category(receivable_accounts)
    grouped_tax_payable_accounts = group_accounts_by_category(tax_payable_accounts)
    grouped_revenue_accounts = group_accounts_by_category(revenue_accounts)

    if request.method == 'POST':

        try:

            # Validate that at least one item is added
            check_list_not_empty(lst=request.form.getlist('item_type[]'))

            # Extract customer data
            customer_id = request.form.get('customer_id')

            if customer_id.isdigit():
                # Existing customer (convert ID to integer)
                customer_id = int(customer_id)

            else:
                # One-time customer (customer_id is actually the name)
                customer_name = customer_id

                # Check if a one-time customer with the same name exists
                existing_customer = db_session.query(Vendor).filter_by(
                    vendor_name=customer_name,
                    is_one_time=True,
                    app_id=app_id
                ).first()

                if existing_customer:
                    customer_id = existing_customer.id  # Use existing one-time customer
                else:
                    # Create a new one-time customer

                    new_customer = Vendor(
                        vendor_name=customer_name,
                        is_one_time=True,
                        vendor_type="Customer",
                        app_id=app_id
                    )
                    db_session.add(new_customer)
                    db_session.flush()
                    customer_id = new_customer.id  # Use new customer's ID

            # Generate a unique direct sale number
            direct_sale_number = generate_direct_sale_number(db_session=db_session, app_id=app_id)
            # Extract payment details
            payment_date = datetime.strptime(request.form['sales_transaction_date'], '%Y-%m-%d').date()

            amount_paid = round(float(request.form['amount_paid']), 2)

            total_line_subtotal = round(float(request.form['overall_line_subtotal']), 2)

            total_amount = round(float(request.form['total_amount']), 2)

            if amount_paid > total_amount:
                raise Exception('Amount Paid cannot be higher than Total amount')

            currency_id = int(request.form['currency'])
            base_currency_id = int(request.form['base_currency_id']) if request.form.get('base_currency_id') else None
            exchange_rate = request.form.get('exchange_rate')
            payment_mode = int(request.form['payment_mode']) if request.form['payment_mode'] else None
            payment_account = int(request.form['fundingAccount']) if request.form['fundingAccount'] else None
            tax_payable_account_id = int(request.form['taxAccount']) if request.form[
                'taxAccount'] else None

            sale_reference = request.form['sales_reference'] or None
            terms_and_conditions = request.form['terms_conditions'] or None
            shipping_cost = float(request.form['shipping_cost'])
            handling_cost = float(request.form['handling_cost'])
            sales_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0
            sales_discount_type = request.form['overall_discount_type'] or None
            sales_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0
            invoice_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0
            total_tax_amount = request.form['total_tax'] or None
            project_id = request.form['project'] or None

            # Handle exchange rate for the sale
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

                # Create exchange rate record (source_id will be None initially, updated after sale creation)
                rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=db_session,
                    action='create',
                    from_currency_id=currency_id,
                    to_currency_id=base_currency_id,
                    rate_value=exchange_rate_value,
                    rate_date=payment_date,
                    app_id=app_id,
                    created_by=current_user.id,
                    source_type='sale',
                    source_id=None,  # Will update after sale is created
                    currency_exchange_transaction_id=None
                )

                exchange_rate_id = rate_id
            if sales_discount_type == "amount":
                calculated_discount_amount = sales_discount_value
            else:
                calculated_discount_amount = (total_line_subtotal * sales_discount_value) / 100

            revenue_account = int(request.form['revenueAccount']) if request.form['revenueAccount'] else None

            # Create new direct sale object
            new_direct_sale = DirectSalesTransaction(
                customer_id=customer_id,
                payment_date=payment_date,
                amount_paid=amount_paid,
                total_amount=total_amount,
                total_line_subtotal=total_line_subtotal,
                currency_id=currency_id,
                sale_reference=sale_reference,
                terms_and_conditions=terms_and_conditions,
                direct_sale_number=direct_sale_number,
                calculated_discount_amount=calculated_discount_amount,
                total_tax_amount=total_tax_amount,
                sales_discount_type=sales_discount_type,
                sales_discount_value=sales_discount_value,
                shipping_cost=shipping_cost,
                handling_cost=handling_cost,
                sales_tax_rate=sales_tax_rate,
                created_by=current_user.id,
                app_id=app_id,
                project_id=project_id,
                revenue_account_id=revenue_account,
                is_posted_to_ledger=True
            )

            db_session.add(new_direct_sale)
            db_session.flush()  # Need ID for allocate_direct_sale_payment

            # Update exchange rate with sale ID if one was created
            if exchange_rate_id and rate_obj:
                rate_obj.source_id = new_direct_sale.id
                db_session.add(rate_obj)

            # Handle direct sale items

            item_type_list = request.form.getlist('item_type[]')
            item_name_list = request.form.getlist('item_name[]')
            inventory_item_list = request.form.getlist('inventory_item[]')
            description_list = request.form.getlist('item_description[]')
            qty_list = request.form.getlist('quantity[]')
            warehouse_list = request.form.getlist('warehouse[]')
            uom_list = request.form.getlist('uom[]')
            unit_price_list = request.form.getlist('unit_price[]')
            discount_list = request.form.getlist('discount_value[]')
            tax_list = request.form.getlist('tax_value[]')
            subtotal_list = request.form.getlist('total_price[]')

            for idx, (item_type, non_inventory_item, inventory_item, warehouse, description, qty, uom, unit_price,
                      discount_amount,
                      tax_amount, subtotal) in enumerate(zip(
                item_type_list, item_name_list, inventory_item_list, warehouse_list, description_list, qty_list,
                uom_list,
                unit_price_list, discount_list, tax_list, subtotal_list),
                start=1):

                # Handling item ID based on type
                valid_item_id = int(inventory_item) if item_type == "inventory" and inventory_item else None
                item_id = None if item_type != "inventory" else valid_item_id  # Ensure only inventory items have an item_id

                # If not an inventory item, store item name instead
                item_name = None if item_type == "inventory" else non_inventory_item  # If not inventory, store item name instead
                warehouse = empty_to_none(warehouse)
                description = empty_to_none(description)
                # Fetch description for inventory items
                if item_type == "inventory":
                    inventory_item_variation = db_session.query(InventoryItemVariationLink).filter_by(
                        inventory_item_id=item_id,
                        app_id=app_id
                    ).first()
                    description = inventory_item_variation.inventory_item.item_description if inventory_item_variation else None

                # Convert values safely
                discount_amount = float(discount_amount) if discount_amount else 0
                tax_amount = float(tax_amount) if tax_amount else 0

                # Prevent division by zero
                subtotal_after_tax = float(subtotal) - tax_amount if float(subtotal) != 0 else 1
                subtotal_after_discount = float(subtotal) - tax_amount + discount_amount if float(subtotal) != 0 else 1

                discount_rate = round((discount_amount / subtotal_after_discount) * 100, 2)
                tax_rate = round((tax_amount / subtotal_after_tax) * 100, 2)

                # Create direct sale item
                new_item = DirectSaleItem(
                    transaction_id=new_direct_sale.id,
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

            allocate_direct_sale_payment(db_session=db_session, payment_amount=amount_paid,
                                         direct_sale_id=new_direct_sale.id, payment_mode=payment_mode,
                                         total_tax_amount=Decimal(total_tax_amount), payment_account=payment_account,
                                         tax_payable_account_id=tax_payable_account_id,
                                         credit_sale_account=None, payment_date=payment_date, reference=sale_reference,
                                         is_posted_to_ledger=True,
                                         exchange_rate_id=exchange_rate_id)

            db_session.flush()
            # ✅ POST TO LEDGER WITH UNPOSTED STATUS
            ledger_success, ledger_message = post_sales_transaction_to_ledger(
                db_session=db_session,
                sales_transaction=new_direct_sale,
                current_user=current_user,
                status='Posted',
                exchange_rate_id=exchange_rate_id,
                exchange_rate_value=exchange_rate_value,
                base_currency_id=base_currency_id
            )

            if not ledger_success:
                raise Exception(f"Ledger posting failed: {ledger_message}")

            # Now commit everything
            db_session.commit()

            # Return JSON success response
            return jsonify({
                'success': True,
                'message': 'Direct sale added successfully!',
                'direct_sale_id': direct_sale_number
            })

        except ValueError:
            return jsonify({
                'success': False,
                'message': f'An error occurred: Please place at least more than one line item'
            })

        except Exception as e:
            db_session.rollback()
            logger.debug(f"An error occurred: {str(e)}\n{traceback.format_exc()}")  # Print the error for debugging
            # Return JSON error response
            return jsonify({
                'success': False,
                'message': f'An error occurred: {str(e)}'
            })

        finally:
            db_session.close()

    else:
        # Fetch customers, currencies, inventory items, and UOMs for the dropdowns

        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            Vendor.is_one_time == False,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id

        # In your Flask route
        # inventory_items = db_session.query(InventoryItemVariationLink).options(
        #     joinedload(InventoryItemVariationLink.inventory_summary)
        # ).filter_by(app_id=current_user.app_id).all()
        #
        inventory_items = db_session.query(InventoryItemVariationLink).join(InventoryItem).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.status == "active"
        ).order_by(InventoryItem.item_name.asc()).all()


        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()

        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
        projects = db_session.query(Project).filter_by(app_id=app_id).all()
        # Render the template with the next direct sale number and customers
        return render_template('/sales/new_sales_transaction.html',
                               currencies=currencies,
                               inventory_items=inventory_items, uoms=uoms,
                               customers=customers, modules=modules_data, payment_modes=payment_modes, company=company,
                               role=role, projects=projects, funding_accounts=grouped_funding_accounts,
                               receivable_accounts=grouped_receivable_accounts,
                               tax_accounts=grouped_tax_payable_accounts,
                               revenue_accounts=grouped_revenue_accounts, warehouses=warehouses,
                               base_currency=base_currency, base_currency_code=base_currency_code,
                               base_currency_id=base_currency_id)


@sales_bp.route('/edit_direct_sales_transaction/<int:direct_sale_id>', methods=['GET', 'POST'])
@login_required
def edit_direct_sales_transaction(direct_sale_id):
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
    warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
    # Query to get funding accounts (cash or bank) for the current app
    funding_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            or_(
                ChartOfAccounts.is_cash.is_(True),
                ChartOfAccounts.is_bank.is_(True),
                ChartOfAccounts.is_system_account.is_(False)
            )
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    # Receivable accounts (is_receivable == True)
    receivable_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.is_receivable.is_(True),
            ChartOfAccounts.is_system_account.is_(False)
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    tax_payable_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.is_payable.is_(True),
            ChartOfAccounts.is_system_account.is_(False)
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    revenue_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.parent_account_type == 'Income',
            ChartOfAccounts.is_system_account.is_(False)
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    if request.method == 'POST':

        try:

            form_version = request.form.get('version', type=int)

            locked_direct_sale = get_locked_record(db_session, DirectSalesTransaction, direct_sale_id, form_version)

            if not locked_direct_sale:
                return jsonify({
                    'success': False,
                    'message': 'This Sales was modified by another user. Please refresh and try again.',
                    'error_type': 'optimistic_lock_failed'
                }), 409  # 409 Conflict is appropriate for version conflicts

            # Fetch the existing direct sale transaction
            direct_sale = locked_direct_sale

            try:
                inventory_entries = get_inventory_entries_for_direct_sale(db_session, direct_sale.id, app_id)
                for entry in inventory_entries:
                    reverse_sales_inventory_entries(db_session, entry)

                reverse_direct_sales_posting(db_session, direct_sale)

            except Exception as inv_error:
                # Stop everything here if reversal fails
                db_session.rollback()
                logger.error(f"Failed to reverse inventory/ledger: {str(inv_error)}")
                return jsonify({
                    'success': False,
                    'message': f'Cannot edit direct sale: {str(inv_error)}'
                })

            if not direct_sale:
                return jsonify({
                    'success': False,
                    'message': 'Direct sale not found!'
                }), 404

            # Extract customer data
            customer_id = request.form.get('customer_id')

            if customer_id.isdigit():
                # Existing customer (convert ID to integer)
                customer_id = int(customer_id)
            else:
                # One-time customer (customer_id is actually the name)
                customer_name = customer_id

                # Check if a one-time customer with the same name exists
                existing_customer = db_session.query(Vendor).filter_by(
                    vendor_name=customer_name,
                    is_one_time=True,
                    app_id=app_id
                ).first()

                if existing_customer:
                    customer_id = existing_customer.id  # Use existing one-time customer
                else:
                    # Create a new one-time customer
                    new_customer = Vendor(
                        vendor_name=customer_name,
                        is_one_time=True,
                        vendor_type="Customer",
                        app_id=app_id
                    )
                    db_session.add(new_customer)
                    db_session.commit()
                    customer_id = new_customer.id  # Use new customer's ID

            # Update payment details
            direct_sale.payment_date = datetime.strptime(request.form['sales_transaction_date'],
                                                         '%Y-%m-%d').date()

            payment_date = datetime.strptime(request.form['sales_transaction_date'],
                                             '%Y-%m-%d').date()

            amount_paid = (round(float(request.form['amount_paid']), 2))
            total_amount = round(float(request.form['total_amount']), 2)

            if amount_paid > total_amount:
                raise Exception('Amount Paid cannot be higher than Total amount')

            direct_sale.amount_paid = amount_paid
            direct_sale.total_line_subtotal = round(float(request.form['overall_line_subtotal']), 2)
            direct_sale.total_amount = total_amount

            direct_sale.currency_id = int(request.form['currency'])
            # === NEW: Get base currency and exchange rate ===
            base_currency_id = int(request.form.get('base_currency_id')) if request.form.get(
                'base_currency_id') else None
            exchange_rate = request.form.get('exchange_rate')

            # Handle exchange rate for the sale
            exchange_rate_id, error_response = update_transaction_exchange_rate(
                db_session=db_session,
                transaction=direct_sale,
                currency_id=direct_sale.currency_id,
                base_currency_id=base_currency_id,
                exchange_rate=exchange_rate,
                transaction_date=payment_date,
                app_id=app_id,
                user_id=current_user.id,
                source_type='sale'
            )

            # If there was an error, return it
            if error_response:
                return error_response
            sale_reference = request.form['sales_reference'] or None
            direct_sale.sale_reference = sale_reference
            direct_sale.terms_and_conditions = request.form['terms_and_conditions'] or None

            direct_sale.shipping_cost = float(request.form['shipping_cost'])
            direct_sale.handling_cost = float(request.form['handling_cost'])

            direct_sale.sales_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0
            direct_sale.sales_discount_type = request.form['overall_discount_type'] or None

            direct_sale.sales_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0
            direct_sale.created_by = direct_sale.created_by

            total_tax_amount = request.form['total_tax'] or None

            direct_sale.total_tax_amount = total_tax_amount
            direct_sale.project_id = normalize_form_value(request.form.get('project'))

            revenue_account = int(request.form['revenueAccount']) if request.form['revenueAccount'] else None
            direct_sale.revenue_account_id = revenue_account
            if direct_sale.sales_discount_type == "amount":
                direct_sale.calculated_discount_amount = direct_sale.sales_discount_value
            else:
                direct_sale.calculated_discount_amount = (
                                                                 direct_sale.total_line_subtotal * direct_sale.sales_discount_value) / 100

            # Update direct sale items
            item_type_list = request.form.getlist('item_type[]')
            item_name_list = request.form.getlist('item_name[]')
            inventory_item_list = request.form.getlist('inventory_item[]')
            warehouse_list = request.form.getlist('warehouse[]')
            description_list = request.form.getlist('item_description[]')
            qty_list = request.form.getlist('quantity[]')
            uom_list = request.form.getlist('uom[]')
            unit_price_list = request.form.getlist('unit_price[]')
            discount_list = request.form.getlist('discount_value[]')
            tax_list = request.form.getlist('tax_value[]')
            subtotal_list = request.form.getlist('total_price[]')

            # Delete existing items
            db_session.query(DirectSaleItem).filter_by(transaction_id=direct_sale.id).delete()

            for idx, (item_type, non_inventory_item, inventory_item, warehouse, description, qty, uom, unit_price,
                      discount_amount,
                      tax_amount, subtotal) in enumerate(zip(
                item_type_list, item_name_list, inventory_item_list, warehouse_list, description_list, qty_list,
                uom_list,
                unit_price_list, discount_list, tax_list, subtotal_list),
                start=1):

                # Handling item ID based on type
                valid_item_id = int(inventory_item) if item_type == "inventory" and inventory_item else None
                item_id = None if item_type != "inventory" else valid_item_id  # Ensure only inventory items have an item_id

                # If not an inventory item, store item name instead
                item_name = None if item_type == "inventory" else non_inventory_item  # If not inventory, store item name instead
                warehouse = empty_to_none(warehouse)
                description = empty_to_none(description)
                # Fetch description for inventory items
                if item_type == "inventory":
                    inventory_item_variation = db_session.query(InventoryItemVariationLink).filter_by(
                        inventory_item_id=item_id,
                        app_id=app_id
                    ).first()
                    description = inventory_item_variation.inventory_item.item_description if inventory_item_variation else None

                # Convert values safely
                discount_amount = float(discount_amount) if discount_amount else 0
                tax_amount = float(tax_amount) if tax_amount else 0

                # Prevent division by zero
                subtotal_after_tax = float(subtotal) - tax_amount if float(subtotal) != 0 else 1
                subtotal_after_discount = float(subtotal) - tax_amount + discount_amount if float(subtotal) != 0 else 1

                discount_rate = round((discount_amount / subtotal_after_discount) * 100, 2)
                tax_rate = round((tax_amount / subtotal_after_tax) * 100, 2)

                # Create direct sale item
                new_item = DirectSaleItem(
                    transaction_id=direct_sale.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    location_id=warehouse,
                    description=description,
                    quantity=qty,
                    unit_price=unit_price,
                    total_price=subtotal,
                    uom=uom,
                    discount_amount=discount_amount,
                    discount_rate=discount_rate,
                    tax_amount=tax_amount,
                    tax_rate=tax_rate,
                    app_id=app_id

                )

                db_session.add(new_item)

            # Allocate payment

            payment_mode = int(request.form['payment_mode']) if request.form['payment_mode'] else None
            payment_account = int(request.form['fundingAccount']) if request.form['fundingAccount'] else None
            tax_payable_account_id = int(request.form['taxAccount']) if request.form['taxAccount'] else None

            db_session.query(PaymentAllocation).filter_by(direct_sale_id=direct_sale_id).delete()

            allocate_direct_sale_payment(
                db_session=db_session,
                payment_amount=direct_sale.amount_paid,
                direct_sale_id=direct_sale_id,
                payment_mode=payment_mode,
                total_tax_amount=total_tax_amount,
                payment_account=payment_account,
                tax_payable_account_id=tax_payable_account_id,
                credit_sale_account=None,
                payment_date=payment_date,
                reference=sale_reference,
                is_posted_to_ledger=True,
                exchange_rate_id=exchange_rate_id
            )

            # ✅ CRITICAL: Refresh the session state after all changes
            direct_sale.status = "draft"
            direct_sale.is_posted_to_ledger = True

            db_session.flush()  # Ensure all operations are executed
            db_session.refresh(direct_sale)  # Reload the direct_sale object with fresh relationships

            # ✅ CORRECT - use the updated direct_sale object
            ledger_success, ledger_message = post_sales_transaction_to_ledger(
                db_session=db_session,
                sales_transaction=direct_sale,  # Use the existing updated object
                current_user=current_user,
                transaction_type='direct_sale',  # Add this parameter
                status='Posted',
                base_currency_id=base_currency_id,
                exchange_rate_id=exchange_rate_id,
                exchange_rate_value=exchange_rate

            )

            if not ledger_success:
                raise Exception(f"Ledger posting failed: {ledger_message}")
            # Commit all changes to the database
            direct_sale.version+=1
            db_session.commit()
            # Return JSON success response
            return jsonify({
                'success': True,
                'message': 'Direct sale updated successfully!',
                'direct_sale_id': direct_sale.id
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f"An error occurred: {str(e)}\n{traceback.format_exc()}")  # Print the error for debugging
            # Return JSON error response
            return jsonify({
                'success': False,
                'message': f'An error occurred: {str(e)}'
            })

        finally:
            db_session.close()

    else:

        # Fetch the existing direct sale transaction
        direct_sale = db_session.query(DirectSalesTransaction).filter_by(id=direct_sale_id, app_id=app_id).first()
        if not direct_sale:
            return "Direct sale not found", 404
        exchange_rate_value = None
        # === GET EXCHANGE RATE VALUE FOR DISPLAY ===
        if direct_sale.payment_allocations[0].exchange_rate:
            exchange_rate_value = direct_sale.payment_allocations[0].exchange_rate.rate

        # Fetch customers, currencies, inventory items, and UOMs for the dropdowns
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''

        inventory_items = db_session.query(InventoryItemVariationLink).join(InventoryItem).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.status == "active"
        ).order_by(InventoryItem.item_name.asc()).all()


        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()

        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
        projects = db_session.query(Project).filter_by(app_id=app_id).all()
        # Fetch existing direct sale items
        direct_sale_items = db_session.query(DirectSaleItem).filter_by(transaction_id=direct_sale.id).all()

        # Render the template with the existing direct sale data
        return render_template('/sales/edit_direct_sales_transaction.html',
                               currencies=currencies, inventory_items=inventory_items, uoms=uoms,
                               customers=customers, modules=modules_data, payment_modes=payment_modes, company=company,
                               role=role, direct_sale=direct_sale, direct_sale_items=direct_sale_items,
                               projects=projects, funding_accounts=funding_accounts,
                               receivable_accounts=receivable_accounts, tax_accounts=tax_payable_accounts,
                               warehouses=warehouses, revenue_accounts=revenue_accounts,
                               base_currency=base_currency, base_currency_code=base_currency_code,
                               exchange_rate_value=exchange_rate_value)


@sales_bp.route('/transactions', methods=["GET"])
@login_required
def view_sales_transactions():
    app_id = current_user.app_id
    db_session = Session()

    try:
        # Default parameters for initial page load
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        start_date = request.args.get('start_date', None)
        end_date = request.args.get('end_date', None)
        filter_type = request.args.get('filter_type', 'payment_date')
        status_filter = request.args.get('status', None)
        customer_id = request.args.get('customer', None)
        payment_mode_id = request.args.get('payment_mode', None)
        invoice_number = request.args.get('invoice_number', None)
        direct_sale_number = request.args.get('direct_sale_number', None)
        payment_status = request.args.get('payment_status', None)  # NEW
        reference = request.args.get('reference', None)  # NEW
        # In view_sales_transactions function, add this parameter:
        transaction_type = request.args.get('transaction_type', None)

        filter_applied = bool(start_date or end_date or status_filter or customer_id or
                              payment_mode_id or invoice_number or direct_sale_number or
                              payment_status or reference or transaction_type)  # UPDATED: Added new filters

        # Fetch company details
        company = db_session.query(Company).filter_by(id=app_id).first()
        base_currency = company.base_currency
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Fetch filter dropdown data
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id, is_active=True).all()


        # Get filtered transactions data
        combined_transactions, pagination_data = _get_sales_transactions_data(
            db_session, page, per_page, start_date, end_date, filter_type,
            status_filter, customer_id, payment_mode_id, invoice_number,
            direct_sale_number, payment_status, reference, transaction_type=transaction_type
            # NEW: Pass the new parameters
        )

        # In your view_purchase_transactions route, add:
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        # Render template for GET
        return render_template(
            '/sales/sales_transactions.html',
            sales_transactions=combined_transactions,
            customers=customers,
            payment_modes=payment_modes,
            pagination=pagination_data,
            filters={
                'start_date': start_date,
                'end_date': end_date,
                'filter_type': filter_type,
                'status': status_filter,
                'customer': customer_id,
                'payment_mode': payment_mode_id,
                'invoice_number': invoice_number,
                'direct_sale_number': direct_sale_number,
                'payment_status': payment_status,  # NEW
                'reference': reference  # NEW
            },
            company=company,
            base_currency=base_currency,
            role=role,
            module_name="Sales",
            modules=modules_data,
            filter_applied=filter_applied,
            currencies=currencies
        )

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error in view_sales_transactions: {str(e)}")
        flash('An error occurred while retrieving sales transactions.', 'error')
        return render_template('error.html', message='Database error occurred'), 500

    except Exception as e:
        logger.error(f'Unexpected error in view_sales_transactions: {str(e)}\n{traceback.format_exc()}')
        flash('An unexpected error occurred.', 'error')
        return render_template('error.html', message='Unexpected error occurred'), 500

    finally:
        db_session.close()


@sales_bp.route('/transactions/filter', methods=["GET", "POST"])
@login_required
def sales_transactions_filter():
    """
    Return filtered JSON data for AJAX requests (GET or POST).
    """
    db_session = Session()

    try:
        # Handle both GET and POST requests
        if request.method == 'GET':
            # Get parameters from query string for GET requests
            page = request.args.get('page', 1, type=int)
            per_page = request.args.get('per_page', 20, type=int)
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            filter_type = request.args.get('filter_type', 'payment_date')
            status_filter = request.args.get('status')
            customer_id = request.args.get('customer')
            payment_mode_id = request.args.get('payment_mode')
            invoice_number = request.args.get('invoice_number')
            direct_sale_number = request.args.get('direct_sale_number')
            payment_status = request.args.get('payment_status')  # NEW: Payment status filter
            reference = request.args.get('reference')  # NEW: Reference filter
            currency = request.args.get('currency')
            transaction_type = request.args.get('transaction_type')  # For GET
        else:
            # Get JSON data from POST request
            data = request.get_json() or {}
            page = int(data.get('page', 1))
            per_page = int(data.get('per_page', 20))
            start_date = data.get('start_date')
            end_date = data.get('end_date')
            filter_type = data.get('filter_type', 'payment_date')
            status_filter = data.get('status')
            customer_id = data.get('customer')
            payment_mode_id = data.get('payment_mode')
            invoice_number = data.get('invoice_number')
            direct_sale_number = data.get('direct_sale_number')
            payment_status = data.get('payment_status')  # NEW: Payment status filter
            reference = data.get('reference')  # NEW: Reference filter
            currency = data.get('currency')
            transaction_type = data.get('transaction_type')  # For POST

        # Get filtered data
        combined_transactions, pagination_data = _get_sales_transactions_data(
            db_session, page, per_page, start_date, end_date, filter_type,
            status_filter, customer_id, payment_mode_id, invoice_number,
            direct_sale_number, payment_status, reference, currency, transaction_type=transaction_type
            # NEW: Pass the new parameters
        )

        # Return JSON for AJAX filtering
        return jsonify({
            'success': True,
            'sales_transactions': combined_transactions,
            'pagination': pagination_data,
            'filters': {
                'start_date': start_date,
                'end_date': end_date,
                'filter_type': filter_type,
                'status': status_filter,
                'customer': customer_id,
                'payment_mode': payment_mode_id,
                'invoice_number': invoice_number,
                'direct_sale_number': direct_sale_number,
                'payment_status': payment_status,  # NEW: Include in response
                'reference': reference,
                'currency': currency,
                'transaction_type': transaction_type
            }
        }), 200

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error in sales_transactions_filter: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred while filtering sales transactions.'}), 500

    except Exception as e:
        logger.error(f'Unexpected error in sales_transactions_filter: {str(e)}\n{traceback.format_exc()}')
        return jsonify({'success': False, 'message': 'An unexpected error occurred.'}), 500

    finally:
        db_session.close()


def _get_sales_transactions_data(db_session, page, per_page, start_date, end_date, filter_type='payment_date',
                                 status_filter=None, customer_id=None, payment_mode_id=None,
                                 invoice_number=None, direct_sale_number=None, payment_status=None, reference=None,
                                 currency_id=None, transaction_type=None):
    """
    Helper function to retrieve sales transactions data with given filters.
    """
    app_id = current_user.app_id

    # Define common columns for union
    invoice_columns = [
        SalesTransaction.id.label('id'),
        literal('invoice').label('type'),
        SalesInvoice.invoice_number.label('document_number'),
        Vendor.vendor_name.label('customer_name'),
        SalesTransaction.payment_date.label('payment_date'),
        SalesInvoice.invoice_date.label('invoice_date'),
        SalesTransaction.amount_paid.label('amount_paid'),
        Currency.user_currency.label('currency'),
        PaymentMode.payment_mode.label('payment_mode'),
        ChartOfAccounts.sub_category.label('account_name'),
        SalesTransaction.reference_number.label('reference'),
        SalesTransaction.is_posted_to_ledger.label('is_posted_to_ledger'),
        SalesInvoice.status.label('invoice_status'),
        SalesTransaction.payment_status.label('payment_status'),
        SalesInvoice.total_amount.label('total_amount'),
        literal(False).label('is_pos'),  # ADD: Invoices are never POS
        func.coalesce(func.sum(SalesTransaction.amount_paid).over(
            partition_by=SalesTransaction.invoice_id
        ), 0).label('total_paid_to_date')
    ]

    direct_sales_columns = [
        DirectSalesTransaction.id.label('id'),
        literal('direct').label('type'),
        DirectSalesTransaction.direct_sale_number.label('document_number'),
        Vendor.vendor_name.label('customer_name'),
        DirectSalesTransaction.payment_date.label('payment_date'),
        DirectSalesTransaction.created_at.label('invoice_date'),
        DirectSalesTransaction.amount_paid.label('amount_paid'),
        Currency.user_currency.label('currency'),
        PaymentMode.payment_mode.label('payment_mode'),
        ChartOfAccounts.sub_category.label('account_name'),
        DirectSalesTransaction.sale_reference.label('reference'),
        DirectSalesTransaction.is_posted_to_ledger.label('is_posted_to_ledger'),
        DirectSalesTransaction.status.label('invoice_status'),
        literal('full').label('payment_status'),  # Default for direct sales
        DirectSalesTransaction.total_amount.label('total_amount'),
        DirectSalesTransaction.is_pos.label('is_pos'),  # ADD: POS flag for direct sales
        func.coalesce(func.sum(DirectSalesTransaction.amount_paid).over(
            partition_by=DirectSalesTransaction.direct_sale_number
        ), 0).label('total_paid_to_date')
    ]

    # Base queries with joins
    invoice_query = db_session.query(*invoice_columns).join(
        SalesInvoice, SalesTransaction.invoice_id == SalesInvoice.id
    ).join(
        Vendor, SalesTransaction.customer_id == Vendor.id
    ).join(
        Currency, SalesTransaction.currency_id == Currency.id
    ).outerjoin(
        PaymentAllocation, PaymentAllocation.payment_id == SalesTransaction.id
    ).outerjoin(
        PaymentMode, PaymentAllocation.payment_mode == PaymentMode.id
    ).outerjoin(
        ChartOfAccounts, PaymentAllocation.payment_account == ChartOfAccounts.id
    ).filter(
        SalesTransaction.app_id == app_id
    )

    direct_sales_query = db_session.query(*direct_sales_columns).join(
        Vendor, DirectSalesTransaction.customer_id == Vendor.id
    ).join(
        Currency, DirectSalesTransaction.currency_id == Currency.id
    ).outerjoin(
        PaymentAllocation, PaymentAllocation.direct_sale_id == DirectSalesTransaction.id
    ).outerjoin(
        PaymentMode, PaymentAllocation.payment_mode == PaymentMode.id
    ).outerjoin(
        ChartOfAccounts, PaymentAllocation.payment_account == ChartOfAccounts.id
    ).filter(
        DirectSalesTransaction.app_id == app_id
    )

    # Apply transaction type filter
    if transaction_type:
        if transaction_type == 'invoice':
            # Only show invoice transactions
            direct_sales_query = direct_sales_query.filter(literal(False))  # Exclude direct sales
        elif transaction_type == 'direct':
            # Only show direct sales (non-POS)
            invoice_query = invoice_query.filter(literal(False))  # Exclude invoices
            direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.is_pos == False)
        elif transaction_type == 'pos':
            # Only show POS transactions
            invoice_query = invoice_query.filter(literal(False))  # Exclude invoices
            direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.is_pos == True)

    # Apply date filters
    if start_date:
        try:
            start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            if filter_type == 'payment_date':
                invoice_query = invoice_query.filter(SalesTransaction.payment_date >= start_date_dt)
                direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.payment_date >= start_date_dt)
            elif filter_type == 'invoice_date':
                invoice_query = invoice_query.filter(SalesInvoice.invoice_date >= start_date_dt)
                direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.created_at >= start_date_dt)
        except ValueError:
            raise ValueError('Invalid start date format. Use YYYY-MM-DD.')

    if end_date:
        try:
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            if filter_type == 'payment_date':
                invoice_query = invoice_query.filter(SalesTransaction.payment_date <= end_date_dt)
                direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.payment_date <= end_date_dt)
            elif filter_type == 'invoice_date':
                invoice_query = invoice_query.filter(SalesInvoice.invoice_date <= end_date_dt)
                direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.created_at <= end_date_dt)
        except ValueError:
            raise ValueError('Invalid end date format. Use YYYY-MM-DD.')

    # Apply status filter
    if status_filter:
        if status_filter == 'posted':
            invoice_query = invoice_query.filter(SalesTransaction.is_posted_to_ledger == True)
            direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.is_posted_to_ledger == True)
        elif status_filter == 'not_posted':
            invoice_query = invoice_query.filter(SalesTransaction.is_posted_to_ledger == False)
            direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.is_posted_to_ledger == False)
        elif status_filter == 'draft':
            # NEW: Filter for draft status
            # For invoices: check SalesInvoice.status
            # For direct sales: check DirectSalesTransaction.status
            invoice_query = invoice_query.filter(SalesInvoice.status == 'draft')
            direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.status == 'draft')
        elif status_filter == 'approved':
            # NEW: Filter for approved status
            invoice_query = invoice_query.filter(SalesInvoice.status == 'approved')
            direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.status == 'approved')
        elif status_filter == 'cancelled':
            # NEW: Filter for cancelled status
            invoice_query = invoice_query.filter(
                (SalesInvoice.status == 'cancelled') |
                (SalesTransaction.payment_status == 'cancelled')
            )
            direct_sales_query = direct_sales_query.filter(
                (DirectSalesTransaction.status == 'cancelled') |
                (DirectSalesTransaction.payment_status == 'cancelled')
            )

    # Apply customer filter
    if customer_id:
        invoice_query = invoice_query.filter(SalesTransaction.customer_id == customer_id)
        direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.customer_id == customer_id)

    # Apply currency filter
    if currency_id:
        invoice_query = invoice_query.filter(SalesTransaction.currency_id == currency_id)
        direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.currency_id == currency_id)

    # Apply payment mode filter
    if payment_mode_id:
        invoice_query = invoice_query.filter(PaymentAllocation.payment_mode == payment_mode_id)
        direct_sales_query = direct_sales_query.filter(PaymentAllocation.payment_mode == payment_mode_id)

    # Apply invoice number filter
    # Apply invoice number filter - FIXED with ilike
    if invoice_number:
        # Only show invoices that match, exclude all direct sales
        invoice_query = invoice_query.filter(SalesInvoice.invoice_number.ilike(f'%{invoice_number}%'))
        # Exclude all direct sales by adding a false condition
        direct_sales_query = direct_sales_query.filter(literal(False))

    # Apply direct sale number filter - FIXED with ilike
    if direct_sale_number:
        # Only show direct sales that match, exclude all invoices
        direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.direct_sale_number.ilike(f'%{direct_sale_number}%'))
        # Exclude all invoices by adding a false condition
        invoice_query = invoice_query.filter(literal(False))

    # Apply reference filter
    if reference:
        invoice_query = invoice_query.filter(SalesTransaction.reference_number.ilike(f'%{reference}%'))
        direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.sale_reference.ilike(f'%{reference}%'))

    # Apply payment status filter
    if payment_status:
        if payment_status == 'paid':
            # Fully paid: total_paid_to_date >= total_amount
            invoice_query = invoice_query.filter(SalesInvoice.status == 'paid')
            direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.status == 'paid')
        elif payment_status == 'inprogress':
            # Partially paid: 0 < total_paid_to_date < total_amount
            invoice_query = invoice_query.filter(SalesInvoice.status == 'partially_paid')
            direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.status == 'partially_paid')

        elif payment_status == 'unpaid':
            # Unpaid: total_paid_to_date == 0
            invoice_query = invoice_query.filter(SalesInvoice.status == 'unpaid')
            direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.status == 'unpaid')

    # Determine order by clause based on filter type
    if filter_type == 'invoice_date':
        order_by_clause = [desc('invoice_date'), desc('id')]
    else:  # payment_date is default
        order_by_clause = [desc('payment_date'), desc('id')]

    # Create union query
    union_query = invoice_query.union_all(direct_sales_query).subquery()

    # Main query from union with ordering
    main_query = db_session.query(
        union_query.c.id,
        union_query.c.type,
        union_query.c.document_number,
        union_query.c.customer_name,
        union_query.c.payment_date,
        union_query.c.amount_paid,
        union_query.c.currency,
        union_query.c.payment_mode,
        union_query.c.account_name,
        union_query.c.reference,
        union_query.c.is_posted_to_ledger,
        union_query.c.invoice_status,
        union_query.c.payment_status,
        union_query.c.total_amount,
        union_query.c.is_pos,  # ADD: Include is_pos in main query
        union_query.c.total_paid_to_date
    ).order_by(*order_by_clause)

    # Get total count
    total_items = main_query.count()

    # Apply pagination
    paginated_query = main_query.offset((page - 1) * per_page).limit(per_page)
    paginated_results = paginated_query.all()

    # Process results
    combined_transactions = []
    for result in paginated_results:
        # Calculate payment progress
        remaining_balance = float(result.total_amount or 0) - float(result.total_paid_to_date or 0)

        if result.type == 'invoice':
            # Convert enum status to string
            status_value = result.payment_status
            if status_value and hasattr(status_value, 'value'):
                status_value = status_value.value
            elif status_value and hasattr(status_value, 'name'):
                status_value = status_value.name

            payment_progress = (
                "Cancelled" if result.payment_status.value == "cancelled"
                else "Final" if remaining_balance == 0
                else "Ongoing" if result.total_paid_to_date > result.amount_paid
                else "Initial"
            )

            combined_transactions.append({
                'id': result.id,
                'type': 'invoice',
                'invoice_number': result.document_number,
                'customer_name': result.customer_name,
                'status': status_value,
                'payment_date': result.payment_date.isoformat() if result.payment_date else None,
                'amount_paid': float(result.amount_paid or 0),
                'currency': result.currency,
                'reference_number': result.reference,
                'total_invoice_amount': float(result.total_amount or 0),
                'total_paid': float(result.total_paid_to_date or 0),
                'remaining_balance': float(remaining_balance or 0),
                'payment_mode': result.payment_mode,
                'account_name': result.account_name,
                'posted_to_ledger': result.is_posted_to_ledger,
                'payment_progress': payment_progress,
                'is_pos': False  # ADD: Invoices are never POS
            })

        else:  # direct sale
            # Convert enum status to string
            status_value = result.invoice_status
            if status_value and hasattr(status_value, 'value'):
                status_value = status_value.value
            elif status_value and hasattr(status_value, 'name'):
                status_value = status_value.name

            sale_type = (
                "Cancelled" if result.payment_status and result.payment_status == "cancelled"
                else "Installment" if remaining_balance > 0
                else "Full"
            )

            combined_transactions.append({
                'id': result.id,
                'type': 'direct',
                'direct_sale_number': result.document_number,
                'customer_name': result.customer_name,
                'status': status_value,
                'payment_date': result.payment_date.isoformat() if result.payment_date else None,
                'amount_paid': float(result.amount_paid or 0),
                'currency': result.currency,
                'reference_number': result.reference,
                'total_sale_amount': float(result.total_amount or 0),
                'total_paid': float(result.total_paid_to_date or 0),
                'remaining_balance': float(remaining_balance or 0),
                'payment_mode': result.payment_mode,
                'account_name': result.account_name,
                'posted_to_ledger': result.is_posted_to_ledger,
                'sale_type': sale_type,
                'is_pos': bool(result.is_pos)  # ADD: Include POS flag
            })

    pagination_data = {
        'page': page,
        'per_page': per_page,
        'total_pages': (total_items + per_page - 1) // per_page,
        'total_items': total_items,
        'has_next': page < ((total_items + per_page - 1) // per_page),
        'has_prev': page > 1,
        'filter_type': filter_type
    }

    return combined_transactions, pagination_data


@sales_bp.route('/transactions/references')
@login_required
def get_transaction_references():
    """
    Return unique reference numbers for filter dropdown.
    """
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Get unique references from both SalesTransaction and DirectSalesTransaction
        sales_references = db_session.query(
            SalesTransaction.reference_number
        ).filter(
            SalesTransaction.app_id == app_id,
            SalesTransaction.reference_number.isnot(None)
        ).distinct().all()

        direct_sales_references = db_session.query(
            DirectSalesTransaction.sale_reference
        ).filter(
            DirectSalesTransaction.app_id == app_id,
            DirectSalesTransaction.sale_reference.isnot(None)
        ).distinct().all()

        # Combine and deduplicate references
        references = set()
        for ref in sales_references:
            if ref[0]:  # Check if not None or empty
                references.add(ref[0])
        for ref in direct_sales_references:
            if ref[0]:  # Check if not None or empty
                references.add(ref[0])

        return jsonify({
            'success': True,
            'references': sorted(list(references))
        }), 200

    except Exception as e:
        logger.error(f'Error fetching references: {str(e)}')
        return jsonify({'success': False, 'message': 'Error fetching references'}), 500
    finally:
        db_session.close()


@sales_bp.route('/transactions/bulk_approve', methods=['POST'])
@login_required
def bulk_approve_sales_transactions():
    db_session = Session()
    try:
        data = request.get_json()
        direct_sale_ids = data.get('direct_sale_ids', [])
        invoice_transaction_ids = data.get('invoice_transaction_ids', [])

        approved_direct_sales = 0
        approved_invoice_transactions = 0

        # Approve direct sales
        for direct_sale_id in direct_sale_ids:
            direct_sale = db_session.query(DirectSalesTransaction).filter_by(id=direct_sale_id).first()
            if direct_sale and direct_sale.status == OrderStatus.draft:
                # Check if this is a POS order slip first
                is_pos_order = (
                        direct_sale.sale_reference and
                        direct_sale.sale_reference.startswith('pos_') and
                        direct_sale.payment_status == 'pending'
                )

                if is_pos_order:

                    # Post to ledger
                    success, message = bulk_post_sales_transactions(
                        db_session=db_session,
                        direct_sale_ids=[direct_sale.id],
                        invoice_transaction_ids=[],
                        current_user=current_user,
                        is_pos_order=True
                    )

                    if not success:
                        logger.warning(f"Warning: Failed to post POS order {direct_sale.id} to ledger: {message}")
                        raise Exception(f'Failed to post POS order {direct_sale.id} to ledger: {message}')
                    else:
                        # POS order slip specific handling
                        direct_sale.status = OrderStatus.paid
                        direct_sale.payment_status = 'full'  # Fixed: single = for assignment


                else:
                    # Regular direct sale approval logic
                    if direct_sale.amount_paid == 0:
                        direct_sale.status = OrderStatus.unpaid
                    elif direct_sale.amount_paid < direct_sale.total_amount:
                        direct_sale.status = OrderStatus.partially_paid
                    else:
                        direct_sale.status = OrderStatus.paid

                db_session.add(direct_sale)
                approved_direct_sales += 1

        # Approve invoice transactions
        for transaction_id in invoice_transaction_ids:
            transaction = db_session.query(SalesTransaction).filter_by(id=transaction_id).first()
            if transaction and transaction.payment_status == SalesPaymentStatus.unpaid:
                # For individual payment approval, set status based on this payment amount
                # vs invoice total (this might need refinement based on your business logic)
                invoice = transaction.invoice
                if invoice:
                    transaction.payment_status = SalesPaymentStatus.paid

                    db_session.add(transaction)
                    approved_invoice_transactions += 1

        db_session.commit()

        return jsonify({
            'success': True,
            'message': f'Approved {approved_direct_sales} direct sales and {approved_invoice_transactions} invoice payments'
        })

    except Exception as e:
        db_session.rollback()
        return jsonify({
            'success': False,
            'message': f'Error approving transactions: {str(e)}'
        }), 500
    finally:
        db_session.close()


# @sales_bp.route('/transactions/bulk_post_to_ledger', methods=['POST'])
# @login_required
# def bulk_post_sales_transaction_to_ledger():
#     db_session = Session()
#     try:
#         # Get JSON data from request
#         data = request.get_json()
#         if not data:
#             return jsonify(success=False, message="No data provided"), 400
#
#         base_currency_id = data.get('base_currency_id')
#         direct_sale_ids = data.get('direct_sale_ids', [])
#         invoice_transaction_ids = data.get('invoice_transaction_ids', [])
#         logger.info(f'Bulk post data is {data}')
#         success_count = 0
#         error_messages = []
#
#         # Process direct sales
#         for transaction_id in direct_sale_ids:
#             try:
#
#                 direct_sale = db_session.query(DirectSalesTransaction).get(transaction_id)
#                 currency_id = direct_sale.currency_id
#                 if not direct_sale:
#                     error_messages.append(f"Direct sale {transaction_id} not found")
#                     continue
#
#                 if direct_sale.is_posted_to_ledger:
#                     error_messages.append(f"Direct sale {direct_sale.direct_sale_number} already posted to ledger")
#                     continue
#
#                 exchange_rate_id = None
#                 exchange_rate_value = 1
#                 if int(base_currency_id) != int(currency_id):
#                     # Get exchange rate for conversion
#                     exchange_rate_obj, exchange_rate_value = get_exchange_rate_and_obj(db_session, currency_id,
#                                                                                        base_currency_id,
#                                                                                        current_user.app_id,
#                                                                                        direct_sale.payment_date)
#                     exchange_rate_id = exchange_rate_obj.id
#                     exchange_rate_value = exchange_rate_value
#
#                 # Check for unposted payment allocations
#                 unposted_allocations = [pa for pa in direct_sale.payment_allocations if not pa.is_posted_to_ledger]
#
#                 if not unposted_allocations:
#                     error_messages.append(
#                         f"No unposted payment allocations found for direct sale {direct_sale.direct_sale_number}")
#                     continue
#
#                 # Post payment allocations to ledger
#                 for allocation in unposted_allocations:
#                     if not allocation.payment_account:
#                         error_messages.append(
#                             f"Payment account not configured for allocation in direct sale {direct_sale.direct_sale_number}")
#                         continue
#
#                     journal_number = generate_unique_journal_number(db_session, current_user.app_id)
#
#                     if allocation.payment_account:
#                         # Debit Cash/Bank account
#                         create_transaction(
#                             db_session=db_session,
#                             transaction_type=allocation.chart_of_accounts_asset.parent_account_type,
#                             date=allocation.payment_date,
#                             category_id=allocation.chart_of_accounts_asset.category_fk,
#                             subcategory_id=allocation.payment_account,
#                             currency=direct_sale.currency_id,
#                             amount=float(allocation.allocated_base_amount + allocation.allocated_tax_amount),
#                             dr_cr="D",
#                             description=f"Payment received for direct sale {direct_sale.direct_sale_number}",
#                             payment_mode_id=allocation.payment_mode,
#                             vendor_id=direct_sale.customer_id,
#                             created_by=current_user.id,
#                             project_id=direct_sale.project_id,
#                             source_type="direct_sale_payment",
#                             source_id=allocation.id,
#                             app_id=direct_sale.app_id,
#                             exchange_rate_id=exchange_rate_id,
#                             journal_number=journal_number
#                         )
#
#                     # Credit Revenue account
#                     revenue_account_id = direct_sale.revenue_account_id
#                     if not revenue_account_id:
#                         error_messages.append(
#                             f"Revenue account not configured for direct sale {direct_sale.direct_sale_number}")
#                         raise Exception(
#                             f'Revenue account not configured for direct sale {direct_sale.direct_sale_number}')
#
#                     create_transaction(
#                         db_session=db_session,
#                         transaction_type=direct_sale.revenue_account.parent_account_type,
#                         date=allocation.payment_date,
#                         category_id=direct_sale.revenue_account.category_fk if direct_sale.revenue_account else None,
#                         subcategory_id=revenue_account_id,
#                         currency=direct_sale.currency_id,
#                         amount=float(allocation.allocated_base_amount),
#                         dr_cr="C",
#                         description=f"Revenue from direct sale {direct_sale.direct_sale_number}",
#                         vendor_id=direct_sale.customer_id,
#                         payment_mode_id=allocation.payment_mode,
#                         created_by=direct_sale.created_by,
#                         source_type="direct_sale_payment",
#                         source_id=allocation.id,
#                         app_id=direct_sale.app_id,
#                         project_id=direct_sale.project_id,
#                         exchange_rate_id=exchange_rate_id,
#                         journal_number=journal_number
#                     )
#
#                     # Credit Tax Payable account if tax exists
#                     if allocation.allocated_tax_amount > 0 and allocation.tax_payable_account_id:
#                         create_transaction(
#                             db_session=db_session,
#                             transaction_type=allocation.chart_of_accounts_tax.parent_account_type,
#                             date=allocation.payment_date,
#                             category_id=allocation.chart_of_accounts_tax.category_fk,
#                             subcategory_id=allocation.tax_payable_account_id,
#                             currency=direct_sale.currency_id,
#                             amount=float(allocation.allocated_tax_amount),
#                             dr_cr="C",
#                             description=f"Tax collected for direct sale {direct_sale.direct_sale_number}",
#                             vendor_id=direct_sale.customer_id,
#                             payment_mode_id=allocation.payment_mode,
#                             project_id=direct_sale.project_id,
#                             created_by=direct_sale.created_by,
#                             source_type="direct_sale_payment",
#                             source_id=allocation.id,
#                             app_id=direct_sale.app_id,
#                             exchange_rate_id=exchange_rate_id,
#                             journal_number=journal_number
#                         )
#                     elif allocation.allocated_tax_amount > 0 and not allocation.tax_payable_account_id:
#                         create_transaction(
#                             db_session=db_session,
#                             transaction_type=direct_sale.revenue_account.parent_account_type,
#                             date=allocation.payment_date,
#                             category_id=direct_sale.revenue_account.category_fk if direct_sale.revenue_account else None,
#                             subcategory_id=revenue_account_id,
#                             currency=direct_sale.currency_id,
#                             amount=float(allocation.allocated_tax_amount),
#                             dr_cr="C",
#                             description=f"Revenue from direct sale {direct_sale.direct_sale_number}",
#                             vendor_id=direct_sale.customer_id,
#                             payment_mode_id=allocation.payment_mode,
#                             created_by=direct_sale.created_by,
#                             source_type="direct_sale_payment",
#                             source_id=allocation.id,
#                             app_id=direct_sale.app_id,
#                             project_id=direct_sale.project_id,
#                             exchange_rate_id=exchange_rate_id,
#                             journal_number=journal_number
#                         )
#
#                     # Process COGS for inventory items
#                     inventory_items = [item for item in direct_sale.direct_sale_items if item.item_type == "inventory"]
#
#                     if inventory_items:
#                         post_direct_sale_cogs_to_ledger(
#                             db_session=db_session,
#                             direct_sale=direct_sale,
#                             exchange_rate_id=exchange_rate_id,
#                             exchange_rate_value=exchange_rate_value,
#                             current_user=current_user,
#                             base_currency_id=base_currency_id
#
#                         )
#                         pass
#                     allocation.is_posted_to_ledger = False
#
#                 direct_sale.is_posted_to_ledger = False
#                 success_count += 1
#
#             except Exception as e:
#                 error_messages.append(f"Error processing direct sale {transaction_id}: {str(e)}")
#                 continue
#
#         # Process invoice transactions
#         for transaction_id in invoice_transaction_ids:
#             try:
#                 sales_transaction = db_session.query(SalesTransaction).get(transaction_id)
#                 if not sales_transaction:
#                     error_messages.append(f"Sales transaction {transaction_id} not found")
#                     continue
#
#                 if sales_transaction.is_posted_to_ledger:
#                     error_messages.append(
#                         f"Transaction {sales_transaction.invoice.invoice_number} already posted to ledger")
#                     continue
#
#                 unposted_allocations = [pa for pa in sales_transaction.payment_allocations if
#                                         not pa.is_posted_to_ledger]
#
#                 if not unposted_allocations:
#                     error_messages.append(
#                         f"No unposted payment allocations found for invoice {sales_transaction.invoice.invoice_number}")
#                     continue
#
#                 currency_id = sales_transaction.currency_id
#
#                 exchange_rate_id = None
#                 exchange_rate_value = 1
#                 if int(base_currency_id) != int(currency_id):
#                     # Get exchange rate for conversion
#                     exchange_rate_obj, exchange_rate_value = get_exchange_rate_and_obj(db_session, currency_id,
#                                                                                        base_currency_id,
#                                                                                        current_user.app_id,
#                                                                                        sales_transaction.payment_date)
#                     exchange_rate_id = exchange_rate_obj.id
#                     exchange_rate_value = exchange_rate_value
#
#                 for allocation in unposted_allocations:
#                     if not allocation.payment_account:
#                         error_messages.append(
#                             f"Payment account not configured for allocation in invoice {sales_transaction.invoice.invoice_number}")
#                         continue
#
#                     invoice_journal_number = generate_unique_journal_number(db_session, current_user.app_id)
#
#                     # Debit Cash/Bank account
#                     create_transaction(
#                         db_session=db_session,
#                         transaction_type=allocation.chart_of_accounts_asset.parent_account_type,
#                         date=allocation.payment_date,
#                         category_id=allocation.chart_of_accounts_asset.category_fk,
#                         subcategory_id=allocation.payment_account,
#                         currency=sales_transaction.currency_id,
#                         amount=float(allocation.allocated_base_amount + allocation.allocated_tax_amount),
#                         dr_cr="D",
#                         description=f"Payment received for invoice {sales_transaction.invoice.invoice_number}",
#                         payment_mode_id=allocation.payment_mode,
#                         vendor_id=sales_transaction.customer_id,
#                         created_by=sales_transaction.created_by,
#                         source_type="invoice_payment",
#                         source_id=allocation.id,
#                         project_id=sales_transaction.invoice.project_id,
#                         app_id=sales_transaction.app_id,
#                         exchange_rate_id=exchange_rate_id,
#                         journal_number=invoice_journal_number
#                     )
#
#                     # Find the original receivable entry
#
#                     # Credit Account Receivable
#                     create_transaction(
#                         db_session=db_session,
#                         transaction_type=sales_transaction.invoice.account_receivable.parent_account_type,
#                         date=allocation.payment_date,
#                         category_id=sales_transaction.invoice.account_receivable.category_fk,
#                         subcategory_id=sales_transaction.invoice.account_receivable_id,
#                         currency=sales_transaction.currency_id,
#                         amount=float(allocation.allocated_base_amount + allocation.allocated_tax_amount),
#                         dr_cr="C",
#                         description=f"Payment applied to invoice {sales_transaction.invoice.invoice_number}",
#                         vendor_id=sales_transaction.customer_id,
#                         created_by=current_user.id,
#                         source_type="invoice_payment",
#                         source_id=allocation.id,
#                         payment_mode_id=allocation.payment_mode,
#                         app_id=sales_transaction.app_id,
#                         project_id=sales_transaction.invoice.project_id,
#                         exchange_rate_id=exchange_rate_id,
#                         journal_number=invoice_journal_number
#                     )
#
#                     allocation.is_posted_to_ledger = True
#
#                 sales_transaction.is_posted_to_ledger = True
#                 success_count += 1
#
#             except Exception as e:
#                 error_messages.append(f"Error processing invoice transaction {transaction_id}: {str(e)}")
#                 continue
#
#         db_session.commit()
#
#         if success_count > 0:
#             message = f"Successfully posted {success_count} transactions to ledger"
#             if error_messages:
#                 message += f". Errors: {', '.join(error_messages[:3])}"  # Show first 3 errors
#             return jsonify(success=True, message=message)
#         else:
#             return jsonify(success=False,
#                            message="No transactions were posted. Errors: " + ", ".join(error_messages)), 400
#
#     except Exception as e:
#         logger.debug(f"An error occurred: {str(e)}\n{traceback.format_exc()}")  # Print the error for debugging
#         db_session.rollback()
#         return jsonify(success=False, message=f"Error posting to ledger: {str(e)}"), 500
#     finally:
#         db_session.close()


@sales_bp.route('/transactions/bulk_post_to_ledger', methods=['POST'])
@login_required
def bulk_post_sales_transactions_route():
    """
    Bulk mark sales transactions as posted in ledger
    """
    db_session = Session()
    try:
        data = request.get_json()
        if not data:
            return jsonify(success=False, message="No data provided"), 400

        direct_sale_ids = data.get('direct_sale_ids', [])
        invoice_transaction_ids = data.get('invoice_transaction_ids', [])

        success, message = bulk_post_sales_transactions(
            db_session=db_session,
            direct_sale_ids=direct_sale_ids,
            invoice_transaction_ids=invoice_transaction_ids,
            current_user=current_user
        )

        if success:
            return jsonify(success=True, message=message)
        else:
            return jsonify(success=False, message=message), 400

    except Exception as e:
        logger.error(f"Bulk sales posting route error: {str(e)}")
        return jsonify(success=False, message=f"Internal server error: {str(e)}"), 500
    finally:
        db_session.close()


@sales_bp.route('/cancel_direct_sales_transaction/<int:transaction_id>', methods=['POST'])
@login_required
def cancel_direct_sales_transaction(transaction_id):
    db_session = Session()

    try:
        # Fetch the transaction by ID
        transaction = db_session.query(DirectSalesTransaction).filter_by(id=transaction_id).first()

        if not transaction:
            flash("Transaction not found.", "error")
            return redirect(url_for('sales.view_sales_transactions'))

        # Always attempt reversal regardless of posting status
        try:
            inventory_entries = get_inventory_entries_for_direct_sale(db_session, transaction.id,
                                                                      current_user.app_id)

            for entry in inventory_entries:
                reverse_sales_inventory_entries(db_session, entry)

            reverse_direct_sales_posting(db_session, transaction)

            try:
                # Clear all stock history cache - no need for complex logic
                clear_stock_history_cache()
                logger.info("Successfully cleared stock history cache after inventory entry")
            except Exception as e:
                logger.error(f"Cache clearing failed: {e}")
                # Continue anyway - cache clearing failure shouldn't break the operation

        except Exception as inv_error:
            # Stop everything here if reversal fails
            db_session.rollback()
            logger.error(f"Failed to reverse inventory/ledger: {str(inv_error)}")
            return jsonify({
                'success': False,
                'message': f'Cannot edit direct sale: {str(inv_error)}'
            })

        # Update the transaction status to "cancelled"
        transaction.payment_status = "cancelled"
        transaction.status = OrderStatus.canceled.name
        transaction.is_posted_to_ledger = False
        db_session.commit()

        flash("Transaction cancelled successfully with reversal entries.", "success")
        return redirect(url_for('sales.view_sales_transactions'))

    except Exception as e:
        logger.error(f"\n[ERROR] Exception occurred: {str(e)}")

        db_session.rollback()

        flash("An error occurred while cancelling the transaction.", "error")
        return redirect(url_for('sales.view_sales_transactions'))

    finally:

        db_session.close()


@sales_bp.route('/transactions/bulk_delete', methods=['POST'])
@login_required
def bulk_delete_sales_transactions():
    db_session = Session()

    try:
        data = request.get_json(silent=True) or {}

        direct_sale_ids = data.get('direct_sale_ids', [])
        invoice_transaction_ids = data.get('invoice_transaction_ids', [])

        deleted_direct_sales = 0
        deleted_invoice_transactions = 0

        invoices_to_update = set()

        # =========================================
        # DELETE DIRECT SALES
        # =========================================
        for direct_sale_id in direct_sale_ids:
            direct_sale = db_session.query(DirectSalesTransaction).filter_by(
                id=direct_sale_id
            ).first()

            if not direct_sale:
                raise Exception(f"Direct sale {direct_sale_id} not found")

            payment_allocations = db_session.query(PaymentAllocation).filter_by(
                direct_sale_id=direct_sale_id
            ).all()

            # Delete credit applications
            for allocation in payment_allocations:
                success, msg, count = delete_credit_applications(
                    db_session=db_session,
                    payment_allocation_id=allocation.id,
                    current_user=current_user
                )

                if not success:
                    raise Exception(f"Failed deleting credits for allocation {allocation.id}: {msg}")

            # Reverse inventory
            inventory_entries = get_inventory_entries_for_direct_sale(
                db_session,
                direct_sale.id,
                current_user.app_id
            )

            for entry in inventory_entries:
                reverse_sales_inventory_entries(db_session, entry)

            clear_stock_history_cache()

            # Reverse ledger
            reverse_direct_sales_posting(db_session, direct_sale)

            # Delete allocations
            for allocation in payment_allocations:
                db_session.delete(allocation)

            # Delete line items
            line_items = db_session.query(DirectSaleItem).filter_by(
                transaction_id=direct_sale_id
            ).all()

            for item in line_items:
                db_session.delete(item)

            db_session.delete(direct_sale)
            deleted_direct_sales += 1

        # =========================================
        # DELETE INVOICE TRANSACTIONS
        # =========================================
        for transaction_id in invoice_transaction_ids:
            transaction_id = int(transaction_id)

            transaction = db_session.query(SalesTransaction).filter_by(
                id=transaction_id
            ).first()

            if not transaction:
                raise Exception(f"Invoice transaction {transaction_id} not found")

            invoice_id = transaction.invoice_id
            invoices_to_update.add(invoice_id)

            payment_allocations = db_session.query(PaymentAllocation).filter_by(
                payment_id=transaction_id
            ).all()

            # Check credit application
            is_credit_application = any(
                allocation.payment_type == 'credit'
                for allocation in payment_allocations
            )

            if is_credit_application:
                success, msg, credit_stats = reverse_credit_application(
                    db_session=db_session,
                    payment_allocation=payment_allocations[0],
                    transaction=transaction,
                    current_user=current_user,
                    action="delete"
                )

                if not success:
                    raise Exception(
                        f"Failed reversing credit application for transaction {transaction_id}: {msg}"
                    )

                deleted_invoice_transactions += 1
                continue

            # Handle overpayment credits
            for allocation in payment_allocations:
                created_credits = db_session.query(CustomerCredit).filter_by(
                    payment_allocation_id=allocation.id
                ).all()

                for credit in created_credits:
                    success, msg, credit_stats = manage_customer_credits(
                        db_session=db_session,
                        source=allocation,
                        action='delete',
                        credit=credit,
                        force=True,
                        current_user=current_user
                    )

                    if not success:
                        raise Exception(f"Failed deleting credit {credit.id}: {msg}")

            # Delete credit applications
            for allocation in payment_allocations:
                success, msg, count = delete_credit_applications(
                    db_session=db_session,
                    payment_allocation_id=allocation.id,
                    current_user=current_user
                )

                if not success:
                    raise Exception(
                        f"Credit delete error for allocation {allocation.id}: {msg}"
                    )

            # Reverse ledger
            reverse_sales_transaction_posting(db_session, transaction)

            # ===== HANDLE BULK PAYMENT CREDIT =====
            if transaction.bulk_payment_id:
                bulk_payment = transaction.bulk_payment

                # Calculate amount in BULK PAYMENT CURRENCY
                amount_paid = float(transaction.amount_paid or 0)

                if transaction.currency_id == bulk_payment.currency_id:
                    amount_in_bulk_currency = amount_paid
                else:
                    invoice_rate = (
                        float(transaction.invoice.exchange_rate.rate)
                        if transaction.invoice and transaction.invoice.exchange_rate
                        else 1
                    )
                    bulk_rate = (
                        float(bulk_payment.exchange_rate.rate)
                        if bulk_payment.exchange_rate
                        else 1
                    )
                    amount_in_base = amount_paid * invoice_rate
                    amount_in_bulk_currency = amount_in_base / bulk_rate

                # Get system accounts
                system_accounts = get_all_system_accounts(
                    db_session=db_session,
                    app_id=current_user.app_id,
                    created_by_user_id=current_user.id
                )
                suspense_account_id = system_accounts['suspense']

                # ALWAYS call manage_bulk_payment - it handles both new and existing credits
                success, msg, data = manage_bulk_payment(
                    db_session=db_session,
                    source=bulk_payment,
                    action='remove_transaction',
                    amount=amount_in_bulk_currency,
                    transaction_id=transaction_id,
                    reason='transaction_deletion',
                    notes=f"Credit from deleted transaction #{transaction.id}",
                    current_user=current_user,
                    suspense_account_id=suspense_account_id,
                    reuse_existing_credit=True  # This makes it update existing credits
                )

                if not success:
                    raise Exception(f"Bulk credit management failed: {msg}")

            # Delete allocations and transaction
            for allocation in payment_allocations:
                db_session.delete(allocation)

            db_session.delete(transaction)
            deleted_invoice_transactions += 1

        # =========================================
        # UPDATE INVOICE STATUS
        # =========================================
        for invoice_id in invoices_to_update:
            update_invoice_status(db_session, invoice_id)

        db_session.commit()

        return jsonify({
            "success": True,
            "message": f"Deleted {deleted_direct_sales} direct sales and {deleted_invoice_transactions} invoice transactions",
            "deleted_direct_sales": deleted_direct_sales,
            "deleted_invoice_transactions": deleted_invoice_transactions
        })

    except Exception as e:
        db_session.rollback()
        logger.error(
            f"Error in bulk_delete_sales_transactions: {str(e)}",
            exc_info=True
        )
        return jsonify({
            "success": False,
            "message": str(e)
        }), 400

    finally:
        db_session.close()


@sales_bp.route('/direct_sales_transaction_details/<int:transaction_id>', methods=['GET'])
@login_required
def direct_sales_transaction_details(transaction_id):
    app_id = current_user.app_id
    with Session() as db_session:
        # Fetch company, role, and modules data
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch payment modes
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
        cash_accounts = (
            db_session.query(ChartOfAccounts)
            .filter(
                ChartOfAccounts.app_id == app_id,
                (ChartOfAccounts.is_cash == True) | (ChartOfAccounts.is_bank == True)
            )
            .all()
        )
        # Fetch the direct sales transaction
        direct_sale = db_session.query(DirectSalesTransaction).filter_by(id=transaction_id, app_id=app_id).first()
        if not direct_sale:
            return jsonify({"error": "Direct sale transaction not found"}), 404

        # Calculate total payments made (excluding refunded payments)
        total_paid = db_session.query(
            func.sum(PaymentAllocation.allocated_base_amount + PaymentAllocation.allocated_tax_amount)).filter(
            PaymentAllocation.direct_sale_id == transaction_id
        ).scalar() or Decimal('0.00')

        # Calculate the remaining balance
        remaining_balance = Decimal(direct_sale.total_amount) - total_paid

        # Prepare direct sale data
        direct_sale_data = {
            "id": direct_sale.id,
            "direct_sale_number": direct_sale.direct_sale_number,
            "payment_date": direct_sale.payment_date.strftime("%Y-%m-%d"),
            "sale_reference": direct_sale.sale_reference,
            "terms_and_conditions": direct_sale.terms_and_conditions,
            "customer": {
                "name": direct_sale.customer.vendor_name,
                "contact": f"{direct_sale.customer.tel_contact}{' | ' + direct_sale.customer.email if direct_sale.customer.email else ''}",
                "address": direct_sale.customer.address or None,
                "city_country": f"{direct_sale.customer.city or ''} {direct_sale.customer.country or ''}".strip() or None
            } if direct_sale.customer else None,
            "currency": direct_sale.currency.user_currency if direct_sale.currency else None,
            "total_amount": float(direct_sale.total_amount),
            "amount_paid": float(total_paid),
            "remaining_balance": float(remaining_balance),
            "payment_status": direct_sale.payment_status,
            "status": direct_sale.status,
            "total_line_subtotal": float(direct_sale.total_line_subtotal),
            "calculated_discount_amount": float(direct_sale.calculated_discount_amount),
            "sales_tax_rate": float(direct_sale.sales_tax_rate),
            "shipping_cost": float(direct_sale.shipping_cost),
            "handling_cost": float(direct_sale.handling_cost),
            "direct_sale_items": [
                {
                    "id": item.id,
                    "item_name": item.item_name if item.item_name else item.inventory_item_variation_link.inventory_item.item_name,
                    "description": item.description if item.item_name else item.inventory_item_variation_link.inventory_item.item_description,
                    "quantity": item.quantity,
                    "uom": item.unit_of_measurement.full_name,
                    "unit_price": float(item.unit_price),
                    "tax_rate": float(item.tax_rate),
                    "tax_amount": float(item.tax_amount),
                    "discount_amount": float(item.discount_amount),
                    "discount_rate": float(item.discount_rate),
                    "total_price": float(item.total_price)
                } for item in direct_sale.direct_sale_items
            ],
            "payment_allocations": [
                {
                    "id": allocation.id,
                    "allocated_base_amount": float(allocation.allocated_base_amount),
                    "allocated_tax_amount": float(allocation.allocated_tax_amount),
                    "payment_mode": allocation.payment_modes.payment_mode if allocation.payment_modes else None,
                    "is_posted_to_ledger": allocation.is_posted_to_ledger,
                    "created_at": allocation.created_at.strftime("%Y-%m-%d %H:%M:%S")
                } for allocation in direct_sale.payment_allocations
            ],
            "created_by": direct_sale.user.name,
            "created_at": direct_sale.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": direct_sale.updated_at.strftime("%Y-%m-%d %H:%M:%S")
        }

    # Return JSON or render template
    if request.args.get('format') == 'json':
        return jsonify(direct_sale_data)
    else:
        return render_template('/sales/direct_sale_transaction_details.html', direct_sale=direct_sale_data,
                               company=company, modules=modules_data, payment_modes=payment_modes, role=role,
                               cash_accounts=cash_accounts,
                               module_name="Sales")


@sales_bp.route('/transaction/<int:transaction_id>', methods=['GET'])
@login_required
def sales_transaction_details(transaction_id):
    app_id = current_user.app_id

    with Session() as db_session:
        # Single query to get company, modules, and payment modes
        company = db_session.query(Company).filter_by(id=app_id).first()
        if not company:
            return jsonify({"error": "Company not found"}), 404

        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()

        currencies = db_session.query(Currency).filter_by(app_id=app_id)
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''

        # Fetch transaction with relationships eagerly loaded to avoid N+1 queries
        transaction = db_session.query(SalesTransaction) \
            .options(
            joinedload(SalesTransaction.invoice),
            joinedload(SalesTransaction.customer),
            joinedload(SalesTransaction.currency),
            joinedload(SalesTransaction.user),
            joinedload(SalesTransaction.payment_allocations)
            .joinedload(PaymentAllocation.payment_modes),
            joinedload(SalesTransaction.payment_allocations)
            .joinedload(PaymentAllocation.chart_of_accounts_asset),
            joinedload(SalesTransaction.payment_allocations)
            .joinedload(PaymentAllocation.chart_of_accounts_tax)
        ) \
            .filter_by(id=transaction_id, app_id=app_id) \
            .first()

        if not transaction:
            return jsonify({"error": "Sales transaction not found"}), 404

        # Calculate total paid for the INVOICE, not the transaction
        # Assuming multiple transactions can be linked to one invoice
        if transaction.invoice:
            total_paid = db_session.query(
                func.sum(SalesTransaction.amount_paid)
            ).filter(
                SalesTransaction.invoice_id == transaction.invoice.id,
                SalesTransaction.app_id == app_id,
                SalesTransaction.payment_status != SalesPaymentStatus.cancelled
            ).scalar() or Decimal('0.00')
        else:
            total_paid = Decimal('0.00')

        # Build transaction data
        transaction_data = build_transaction_data(transaction, total_paid)

    # Return response
    if request.args.get('format') == 'json':
        return jsonify(transaction_data)
    else:
        return render_template(
            '/sales/sales_transaction_details.html',
            transaction=transaction_data,
            company=company,
            modules=modules_data,
            payment_modes=payment_modes,
            role=current_user.role,
            module_name="Sales",
            currencies=currencies,
            base_currency=base_currency,
            base_currency_code=base_currency_code
        )


def build_transaction_data(transaction, total_paid):
    """Helper function to build transaction data structure"""

    # Get exchange rate information
    exchange_rate_info = None
    if transaction.payment_allocations and transaction.payment_allocations[0].exchange_rate:
        exchange_rate = transaction.payment_allocations[0].exchange_rate
        # Calculate rate in correct direction (1 transaction currency = X base currency)
        if exchange_rate.from_currency_id == transaction.currency_id:
            rate_value = float(exchange_rate.rate)
        else:
            rate_value = float(1 / exchange_rate.rate)

        exchange_rate_info = {
            'id': exchange_rate.id,
            'rate': rate_value,
            'date': exchange_rate.date.strftime("%Y-%m-%d") if exchange_rate.date else None,
            'from_currency': transaction.currency.user_currency if transaction.currency else None,
            'to_currency': 'Base Currency'  # You might want to pass this from base_currency
        }

    data = {
        "id": transaction.id,
        "invoice": build_invoice_data(transaction, total_paid),
        "customer": build_customer_data(transaction),
        "payment_date": transaction.payment_date.strftime("%Y-%m-%d"),
        "amount_paid": float(transaction.amount_paid),
        "currency": transaction.currency.user_currency if transaction.currency else None,
        "currency_id": transaction.currency.id if transaction.currency else None,
        "payment_mode": build_payment_mode_data(transaction),
        "reference_number": transaction.reference_number,
        "payment_status": transaction.payment_status.value,
        "is_posted_to_ledger": transaction.is_posted_to_ledger,
        "created_by": transaction.user.name if transaction.user else None,
        "created_at": transaction.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": transaction.updated_at.strftime("%Y-%m-%d %H:%M:%S") if transaction.updated_at else None,
        "payment_allocations": build_payment_allocations_data(transaction),
        "bulk_payment_id": transaction.bulk_payment_id,
        "bulk_payment_number": transaction.bulk_payment.bulk_payment_number if transaction.bulk_payment else None,
        "exchange_rate": exchange_rate_info  # Add this line
    }

    # Add allocation amounts if allocations exist
    if transaction.payment_allocations:
        data["allocated_base_amount"] = float(transaction.payment_allocations[0].allocated_base_amount)
        data["allocated_tax_amount"] = float(transaction.payment_allocations[0].allocated_tax_amount)

    return data


def build_invoice_data(transaction, total_paid):
    """Build invoice data structure"""
    if not transaction.invoice:
        return None

    # Get invoice exchange rate information
    invoice_exchange_rate = None
    if transaction.invoice.exchange_rate:
        # Calculate rate in correct direction
        if transaction.invoice.exchange_rate.from_currency_id == transaction.invoice.currency:
            rate_value = float(transaction.invoice.exchange_rate.rate)
        else:
            rate_value = float(1 / transaction.invoice.exchange_rate.rate)

        invoice_exchange_rate = {
            "rate": rate_value,
            "date": transaction.invoice.exchange_rate.date.strftime(
                "%Y-%m-%d") if transaction.invoice.exchange_rate.date else None
        }

    return {
        "id": transaction.invoice.id,
        "invoice_number": transaction.invoice.invoice_number,
        "invoice_date": transaction.invoice.invoice_date.strftime("%Y-%m-%d"),
        "due_date": transaction.invoice.due_date.strftime("%Y-%m-%d") if transaction.invoice.due_date else None,
        "total_amount": float(transaction.invoice.total_amount),
        "total_paid": float(total_paid),
        "status": transaction.invoice.status,
        "currency_id": transaction.invoice.currency,  # Add this
        "exchange_rate": invoice_exchange_rate  # Add this
    }


def build_customer_data(transaction):
    """Build customer data structure"""
    if not transaction.customer:
        return None

    contact_parts = []
    if transaction.customer.tel_contact:
        contact_parts.append(transaction.customer.tel_contact)
    if transaction.customer.email:
        contact_parts.append(transaction.customer.email)

    city_country = " ".join(filter(None, [
        transaction.customer.city or "",
        transaction.customer.country or ""
    ])).strip() or None

    return {
        "id": transaction.customer_id,
        "name": transaction.customer.vendor_name,
        "contact": " | ".join(contact_parts) if contact_parts else None,
        "address": transaction.customer.address or None,
        "city_country": city_country
    }


def build_payment_mode_data(transaction):
    """Build payment mode data - simplified structure"""
    if not transaction.payment_allocations:
        return None

    allocation = transaction.payment_allocations[0]
    return {
        "id": allocation.payment_mode,
        "payment_mode": allocation.payment_modes.payment_mode if allocation.payment_modes else None
    }


def build_payment_allocations_data(transaction):
    """Build payment allocations data"""
    return [
        {
            "id": allocation.id,
            "allocated_base_amount": float(allocation.allocated_base_amount),
            "allocated_tax_amount": float(allocation.allocated_tax_amount),
            "payment_mode": allocation.payment_modes.payment_mode if allocation.payment_modes else None,
            "payment_account_id": allocation.payment_account,
            "payment_account_name": allocation.chart_of_accounts_asset.sub_category if allocation.chart_of_accounts_asset else None,
            "payment_account_category": allocation.chart_of_accounts_asset.categories.category if allocation.chart_of_accounts_asset else None,
            "tax_payable_account_id": allocation.tax_payable_account_id,
            "tax_payable_account_name": allocation.chart_of_accounts_tax.sub_category if allocation.chart_of_accounts_tax else None,
            "tax_payable_account_category": allocation.chart_of_accounts_tax.categories.category if allocation.chart_of_accounts_tax else None,
            "created_at": allocation.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "payment_type": allocation.payment_type,
            "exchange_rate_id": allocation.exchange_rate_id
        } for allocation in transaction.payment_allocations
    ]


@sales_bp.route('/edit_sales_transaction/<int:transaction_id>', methods=['POST'])
@login_required
def edit_sales_transaction(transaction_id):
    app_id = current_user.app_id
    db_session = Session()

    try:
        # ===== GET FORM DATA (same as record payment) =====
        amount = Decimal(str(request.form.get('amount', 0)))
        payment_date = datetime.strptime(request.form.get('paymentDate'), '%Y-%m-%d')
        reference = request.form.get('reference') or None
        payment_method = request.form.get('paymentMethod')
        payment_method = int(payment_method) if payment_method and payment_method.strip().isdigit() else None

        # Payment currency
        payment_currency = request.form.get('payment_currency')
        base_currency_id = get_int_or_none(request.form.get('base_currency_id'))

        # Exchange rate if payment currency != base currency
        exchange_rate = request.form.get('exchange_rate')
        exchange_rate_value = float(exchange_rate) if exchange_rate else None

        # Payment type and accounts
        payment_type = request.form.get('paymentTypeRadio', 'cash')
        funding_account_id = get_int_or_none(request.form.get('fundingAccount'))
        credit_settlement_account_id = get_int_or_none(request.form.get('creditSettlementAccount'))
        tax_account_id = get_int_or_none(request.form.get('taxAccount'))

        # Overpayment handling
        overpayment_handling = request.form.get('overpaymentHandling', 'credit')
        write_off_account_id = get_int_or_none(request.form.get('writeOffAccount'))

        # Selected credits (for credit payments)
        selected_credits_json = request.form.get('selectedCredits')
        selected_credits = json.loads(selected_credits_json) if selected_credits_json else []

        # ===== FETCH EXISTING TRANSACTION =====
        transaction = db_session.query(SalesTransaction).filter_by(id=transaction_id, app_id=app_id).first()
        if not transaction:
            flash('Transaction not found.', 'error')
            return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

        invoice = transaction.invoice
        if not invoice:
            flash('Associated invoice not found.', 'error')
            return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

        # ===== DETERMINE PAYMENT CURRENCY =====
        if payment_type == 'credit':
            # For credit payments, always use invoice currency
            payment_currency_id = invoice.currency
        else:
            # For cash payments, use selected currency
            payment_currency_id = int(payment_currency) if payment_currency else invoice.currency

        # ===== GET SYSTEM ACCOUNTS =====
        system_accounts = get_all_system_accounts(
            db_session=db_session,
            app_id=app_id,
            created_by_user_id=current_user.id
        )
        suspense_account_id = system_accounts['suspense']
        fx_gain_loss_account_id = system_accounts['fx_gain_loss']
        customer_credit_system_account_id = system_accounts.get('customer_credit')
        write_off_system_account_id = system_accounts.get('write_off')

        # ===== CREATE/UPDATE EXCHANGE RATE IF NEEDED =====
        exchange_rate_id = None
        if payment_currency_id != base_currency_id and exchange_rate_value:
            rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                session=db_session,
                action='create',
                from_currency_id=payment_currency_id,
                to_currency_id=base_currency_id,
                rate_value=exchange_rate_value,
                rate_date=payment_date.date(),
                app_id=app_id,
                created_by=current_user.id,
                source_type='invoice_payment',
                source_id=invoice.id
            )
            exchange_rate_id = rate_id

        # ===== CALCULATE AMOUNTS IN INVOICE CURRENCY =====
        if payment_currency_id == invoice.currency:
            # Same currency - payment amount = invoice amount
            amount_in_invoice = amount
            if payment_currency_id != base_currency_id and exchange_rate_value:
                amount_in_base = float(amount) * exchange_rate_value
            else:
                amount_in_base = float(amount)
        else:
            # Different currencies - calculate using rates
            if not exchange_rate_value:
                raise Exception("Exchange rate is required when payment currency differs from invoice currency")

            payment_in_base = float(amount) * exchange_rate_value
            invoice_rate = float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1
            amount_in_invoice = Decimal(str(payment_in_base / invoice_rate)).quantize(Decimal('0.01'))
            amount_in_base = payment_in_base

        # ===== CALCULATE OTHER PAYMENTS (excluding current) =====
        other_payments_total = db_session.query(func.sum(SalesTransaction.amount_paid)).filter(
            SalesTransaction.invoice_id == invoice.id,
            SalesTransaction.id != transaction_id,
            SalesTransaction.payment_status.in_(
                [SalesPaymentStatus.full, SalesPaymentStatus.partial, SalesPaymentStatus.paid]
            ),
            SalesTransaction.payment_status != SalesPaymentStatus.cancelled
        ).scalar() or Decimal('0.00')

        remaining_without_current = invoice.total_amount - other_payments_total

        # ===== HANDLE OVERPAYMENT (only for cash) =====
        overpayment_amount = Decimal('0.00')
        allocated_amount = amount_in_invoice

        if payment_type == 'cash' and amount_in_invoice > remaining_without_current:
            overpayment_amount = amount_in_invoice - remaining_without_current
            allocated_amount = remaining_without_current
            overpayment_amount = overpayment_amount.quantize(Decimal('0.01'))

        # ===== VALIDATION =====
        if payment_type == 'cash' and not funding_account_id:
            flash('Payment receiving account is required for cash payments.', 'error')
            return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

        if payment_type == 'credit' and not credit_settlement_account_id:
            flash('Customer credit account is required for credit payments.', 'error')
            return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

        # ===== DELETE EXISTING JOURNAL ENTRIES =====
        # Delete journal entries for all related source types
        for source_type in ['invoice_payment', 'customer_credit', 'overpayment_write_off']:
            delete_journal_entries_by_source(db_session, source_type, transaction_id, app_id)

        # ===== REVERSE ALL EXISTING CREDITS FROM THIS TRANSACTION =====
        # Get all payment allocations for this transaction
        old_allocations = db_session.query(PaymentAllocation).filter(
            PaymentAllocation.payment_id == transaction_id
        ).all()

        for allocation in old_allocations:
            # Find credits created from this allocation (overpayment credits)
            customer_credits = db_session.query(CustomerCredit).filter_by(
                payment_allocation_id=allocation.id,
                app_id=app_id
            ).all()

            for credit in customer_credits:
                success, msg, stats = manage_customer_credits(
                    db_session=db_session,
                    source=allocation,
                    action='reverse',
                    credit=credit,
                    current_user=current_user
                )
                if not success:
                    flash(f'Failed to reverse credit: {msg}', 'error')
                    return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

            # Also handle credit applications (credits USED in this payment)
            credit_applications = db_session.query(CreditApplication).filter_by(
                payment_allocation_id=allocation.id
            ).all()

            for app in credit_applications:
                credit = app.credit
                credit.available_amount += app.applied_amount
                update_credit_status(credit)
                db_session.delete(app)
        # ===== DELETE EXISTING PAYMENT ALLOCATIONS =====
        db_session.query(PaymentAllocation).filter(
            PaymentAllocation.payment_id == transaction_id
        ).delete()

        # ===== DETERMINE PAYMENT STATUS =====
        if other_payments_total + allocated_amount >= invoice.total_amount - Decimal('0.01'):
            payment_status = SalesPaymentStatus.paid
            invoice.status = InvoiceStatus.paid
        else:
            payment_status = SalesPaymentStatus.partial
            invoice.status = InvoiceStatus.partially_paid

        # ===== UPDATE TRANSACTION =====
        transaction.amount_paid = amount_in_invoice
        transaction.payment_date = payment_date
        transaction.reference_number = reference
        transaction.payment_status = payment_status
        transaction.currency_id = invoice.currency  # Always store in invoice currency
        transaction.is_posted_to_ledger = False

        # Set payment account based on type
        if payment_type == 'credit':
            payment_account = credit_settlement_account_id
            payment_method = None
        else:
            payment_account = funding_account_id

        # ===== CREATE NEW PAYMENT ALLOCATION =====
        payment_allocation = allocate_payment(
            sale_transaction_id=transaction_id,
            invoice_id=invoice.id,
            payment_date=payment_date,
            payment_amount=amount_in_invoice,
            remaining_balance=remaining_without_current,
            db_session=db_session,
            payment_mode=payment_method,
            total_tax_amount=invoice.total_tax_amount,
            payment_account=payment_account,
            tax_payable_account_id=tax_account_id,
            credit_sale_account=None,
            reference=reference,
            overpayment_amount=overpayment_amount,
            write_off_account_id=write_off_system_account_id if overpayment_handling == 'write_off' else None,
            payment_type=payment_type,
            is_posted_to_ledger=True,
            exchange_rate_id=exchange_rate_id,
            base_currency_id=base_currency_id,
            invoice=invoice
        )
        db_session.flush()

        # ===== HANDLE OVERPAYMENT (cash only) =====
        if payment_type == 'cash' and overpayment_amount > 0:
            if overpayment_handling == 'write_off' and write_off_system_account_id:
                success, message = post_overpayment_write_off_to_ledger(
                    db_session=db_session,
                    payment_account_id=payment_account,
                    write_off_account_id=write_off_system_account_id,
                    write_off_amount=overpayment_amount,
                    current_user=current_user,
                    payment_allocation_id=payment_allocation.id,
                    invoice=invoice,
                    status='Posted',
                    project_id=invoice.project_id,
                    exchange_rate_id=exchange_rate_id,
                    exchange_rate_value=exchange_rate_value
                )
                if not success:
                    flash(f'Error posting write-off: {message}', 'error')
                    return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))
                flash_message = f'Transaction updated! {overpayment_amount:.2f} written off as income.'
            else:
                success, msg, credit = manage_customer_credits(
                    db_session=db_session,
                    source=payment_allocation,
                    action='create',
                    amount=overpayment_amount,
                    currency_id=invoice.currency,
                    reason='overpayment',
                    exchange_rate_id=exchange_rate_id,
                    reference=reference or payment_allocation.reference,
                    customer_id=invoice.customer_id,
                    funding_account=payment_account,
                    payment_allocation=payment_allocation,
                    current_user=current_user,
                    project_id=invoice.project_id
                )
                if not success:
                    flash(f'Error creating credit: {msg}', 'error')
                    return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))
                flash_message = f'Transaction updated! {overpayment_amount:.2f} overpayment - Credit #{credit.id} created.'
        else:
            flash_message = 'Transaction updated successfully!'

        # ===== POST TO LEDGER (using same function as record payment) =====
        # Build allocation details for ledger
        allocation_details = [{
            'allocation': payment_allocation,
            'invoice': invoice,
            'amount': float(allocated_amount),
            'amount_base': amount_in_base if payment_currency_id != invoice.currency else float(allocated_amount),
            'amount_payment': float(amount),
            'invoice_rate': float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1
        }]

        # Use post_payment_receipt_to_ledger (works for single invoice too)
        success, message = post_payment_receipt_to_ledger(
            db_session=db_session,
            receipt=None,  # No bulk payment
            allocations=allocation_details,
            payment_account_id=payment_account,
            suspense_account_id=suspense_account_id,
            fx_gain_loss_account_id=fx_gain_loss_account_id,
            current_user=current_user,
            overpayment_action=overpayment_handling if payment_type == 'cash' else None,
            customer_credit_account_id=customer_credit_system_account_id,
            write_off_account_id=write_off_system_account_id if overpayment_handling == 'write_off' and payment_type == 'cash' else None,
            status='Posted',
            project_id=invoice.project_id,
            exchange_rate_id=exchange_rate_id,
            base_currency_id=base_currency_id,
            payment_currency_id=payment_currency_id,
            overpayment_amount=overpayment_amount
        )

        if not success:
            flash(f'Error posting to ledger: {message}', 'error')
            return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

        # ===== COMMIT =====
        db_session.commit()
        flash(flash_message, 'success')
        return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error editing sales transaction: {e}\n{traceback.format_exc()}')
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))
    finally:
        db_session.close()




@sales_bp.route('/cancel_sales_transaction/<int:transaction_id>', methods=['POST'])
@login_required
def cancel_sales_transaction(transaction_id):
    db_session = Session()

    try:
        # Fetch transaction
        transaction = db_session.query(SalesTransaction).filter_by(id=transaction_id).first()

        if not transaction:
            flash("Transaction not found.", "error")
            return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

        if transaction.payment_status == SalesPaymentStatus.cancelled:
            flash("Transaction is already cancelled.", "warning")
            return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

        # Get payment allocation
        payment_allocation = db_session.query(PaymentAllocation) \
            .filter_by(payment_id=transaction_id) \
            .first()

        stats = {
            'applications_reversed': 0,
            'source_credits_restored': 0,
            'created_credits_deleted': 0,
            'downstream_transactions_cancelled': 0,
            'credit_application_reversed': None
        }

        # ===== CHECK IF THIS IS A CREDIT APPLICATION =====
        if payment_allocation and payment_allocation.payment_type == 'credit':
            # Handle credit application reversal
            success, msg, credit_stats = reverse_credit_application(
                db_session=db_session,
                payment_allocation=payment_allocation,
                transaction=transaction,
                current_user=current_user,
                action="cancel"
            )

            if not success:
                db_session.rollback()
                flash(f"Error reversing credit application: {msg}", "error")
                return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

            if credit_stats.get('credit_application_reversed'):
                stats['credit_application_reversed'] = credit_stats['credit_application_reversed']
                stats['applications_reversed'] += 1

        # ===== HANDLE CREDITS CREATED FROM THIS PAYMENT (overpayments) =====
        elif payment_allocation:
            # Find credits created from this allocation
            created_credits = db_session.query(CustomerCredit).filter_by(
                payment_allocation_id=payment_allocation.id
            ).all()

            for credit in created_credits:
                # Reverse the credit (this will handle all applications)
                success, msg, credit_stats = manage_customer_credits(
                    db_session=db_session,
                    source=payment_allocation,
                    action='reverse',
                    credit=credit,
                    current_user=current_user
                )

                if not success:
                    db_session.rollback()
                    flash(f"Error reversing credit: {msg}", "error")
                    return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

                stats['created_credits_deleted'] += 1
                if credit_stats:
                    stats['applications_reversed'] += credit_stats.get('applications_reversed', 0)

        # Store invoice ID
        invoice_id = transaction.invoice_id

        # Reverse ledger postings (if not already done by credit reversal)
        if not stats.get('credit_application_reversed'):
            try:
                reverse_sales_transaction_posting(db_session, transaction)
            except Exception as reversal_error:
                db_session.rollback()
                logger.error(f"Failed to reverse ledger entries: {str(reversal_error)}")
                flash(f"Cannot cancel transaction: {str(reversal_error)}", "error")
                return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

        # ===== HANDLE BULK PAYMENT (if applicable) =====
        if transaction.bulk_payment_id and not stats.get('credit_application_reversed'):
            bulk_payment = transaction.bulk_payment

            # DELETE the allocation journal for this transaction
            if payment_allocation:
                delete_journal_entries_by_source(
                    db_session=db_session,
                    source_type='receipt_allocation',  # Match your new source_type
                    source_id=payment_allocation.id,
                    app_id=current_user.app_id
                )

            # Calculate amount in BULK PAYMENT CURRENCY
            if transaction.currency_id == bulk_payment.currency_id:
                amount_in_bulk_currency = float(transaction.amount_paid)
            else:
                # Get invoice exchange rate
                if transaction.invoice and transaction.invoice.exchange_rate:
                    invoice_rate = float(transaction.invoice.exchange_rate.rate)
                else:
                    invoice_rate = 1

                # Get bulk payment exchange rate
                if bulk_payment.exchange_rate:
                    bulk_rate = float(bulk_payment.exchange_rate.rate)
                else:
                    bulk_rate = 1

                # Convert: invoice amount → base → bulk currency
                amount_in_base = float(transaction.amount_paid) * invoice_rate
                amount_in_bulk_currency = amount_in_base / bulk_rate

            # ===== GET SYSTEM ACCOUNTS =====
            system_accounts = get_all_system_accounts(
                db_session=db_session,
                app_id=current_user.app_id,
                created_by_user_id=current_user.id
            )

            suspense_account_id = system_accounts['suspense']

            # ✅ CREATE A NEW CREDIT for this cancelled transaction
            success, msg, data = manage_bulk_payment(
                db_session=db_session,
                source=bulk_payment,
                action='remove_transaction',
                amount=amount_in_bulk_currency,
                transaction_id=transaction_id,
                reason='transaction_cancellation',
                notes=f"Credit created from cancelled transaction #{transaction.id} for Invoice {transaction.invoice.invoice_number}",
                current_user=current_user,
                suspense_account_id=suspense_account_id,
                reuse_existing_credit=True
            )

            if not success:
                logger.error(f"Failed to create credit for cancelled transaction: {msg}")
                db_session.rollback()
                flash(f"Error creating credit for cancelled transaction: {msg}", "error")
                return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

            stats['credit_created'] = data.get('credit_id')


        # Update invoice status
        # Update transaction status FIRST
        transaction.payment_status = SalesPaymentStatus.cancelled
        transaction.is_posted_to_ledger = False

        # THEN update invoice status (now sees transaction as cancelled)
        invoice_status_updated = False
        try:
            update_invoice_status(db_session, invoice_id)
            invoice_status_updated = True
        except Exception as status_error:
            logger.error(f"Failed to update invoice status: {str(status_error)}")


        # Update transaction status
        transaction.payment_status = SalesPaymentStatus.cancelled
        transaction.is_posted_to_ledger = False

        # Commit
        db_session.commit()

        # Build flash message
        messages = []
        if stats['applications_reversed']:
            messages.append(f"{stats['applications_reversed']} credit application(s) reversed")
        if stats['created_credits_deleted']:
            messages.append(f"{stats['created_credits_deleted']} overpayment credit(s) deleted")
        if stats.get('credit_created'):
            messages.append(f"credit #{stats['credit_created']} created")
        if stats.get('credit_application_reversed'):
            messages.append(f"credit application #{stats['credit_application_reversed']} reversed")

        credit_message = f" ({', '.join(messages)})" if messages else ""

        if not invoice_status_updated:
            flash(f"Transaction cancelled successfully{credit_message}, but failed to update invoice status.",
                  "warning")
        else:
            flash(f"Transaction cancelled successfully{credit_message}.", "success")

        return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))

    except Exception as e:
        logger.error(f"Error cancelling sales transaction: {str(e)}\n{traceback.format_exc()}")
        db_session.rollback()
        flash("An error occurred while cancelling the transaction.", "error")
        return redirect(url_for('sales.sales_transaction_details', transaction_id=transaction_id))
    finally:
        db_session.close()
