import traceback
from datetime import datetime
from decimal import Decimal

from flask import request, jsonify, render_template, flash, redirect, url_for
from flask_login import login_required, current_user
from sqlalchemy import or_, func, literal, desc, UniqueConstraint
from sqlalchemy.exc import SQLAlchemyError

from ai import resolve_exchange_rate_for_transaction
from db import Session
from models import Company, Module, InventoryLocation, ChartOfAccounts, Vendor, DirectSalesTransaction, \
    InventoryItemVariationLink, DirectSaleItem, Project, PaymentMode, UnitOfMeasurement, Currency, PaymentAllocation, \
    SalesTransaction, SalesPaymentStatus, SalesInvoice, SalesInvoiceHistory, SalesInvoiceStatusLog, SalesInvoiceItem, \
    InvoiceStatus, JournalEntry, CustomerCredit, InventoryItem
from services.chart_of_accounts_helpers import group_accounts_by_category
from services.inventory_helpers import reverse_inventory_entry
from services.post_to_ledger import post_invoice_cogs_to_ledger, post_invoice_to_ledger
from services.post_to_ledger_reversal import reverse_sales_invoice_posting, reverse_sales_transaction_posting
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    generate_next_invoice_number, get_inventory_entries_for_invoice, reverse_sales_inventory_entries, \
    update_transaction_exchange_rate, manage_bulk_payment
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction
from utils_and_helpers.amounts_utils import parse_number
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj, get_or_create_exchange_rate_for_transaction
from utils_and_helpers.lists import check_list_not_empty
from . import sales_bp

import logging

from .customer_credits import manage_customer_credits, reverse_credit_application

logger = logging.getLogger(__name__)


@sales_bp.route('/add_sales_invoice', methods=['GET', 'POST'])
@login_required
def add_sales_invoice():
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
    warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

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

    # Payable accounts (is_payable == True)
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

    # Revenue accounts (account_type == 'Income')
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

    grouped_receivable_accounts = group_accounts_by_category(receivable_accounts)
    grouped_tax_payable_accounts = group_accounts_by_category(tax_payable_accounts)
    grouped_revenue_accounts = group_accounts_by_category(revenue_accounts)

    if request.method == 'POST':
        try:
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

            # Extract sales_invoice details
            # Generate the next invoice number
            next_invoice_number = generate_next_invoice_number()
            invoice_number = next_invoice_number

            invoice_date = datetime.strptime(request.form['invoice_date'], '%Y-%m-%d').date()
            invoice_reference = request.form['invoice_reference'] or None
            account_receivable = request.form['accountsReceivableAccount'] or None
            tax_account = request.form['taxAccount'] or None
            revenue_account = request.form['revenueAccount'] or None

            due_date = datetime.strptime(request.form['due_date'], '%Y-%m-%d').date() if request.form[
                'due_date'] else None

            overall_line_subtotal = float(request.form['overall_line_subtotal'])
            total_amount = float(request.form['total_amount']) or None
            total_tax_amount = float(request.form['total_tax']) or None
            shipping_cost = parse_number(request.form['shipping_cost'])
            handling_cost = parse_number(request.form['handling_cost'])
            terms_and_conditions = request.form['terms_and_conditions'] or None
            currency_id = request.form['currency']
            base_currency_id = int(request.form.get('base_currency_id')) if request.form.get(
                'base_currency_id') else None
            exchange_rate = request.form.get('exchange_rate')

            project_id = request.form['project'] or None

            # Handle invoice general discount and tax
            invoice_discount_type = request.form['overall_discount_type'] or None
            invoice_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0

            invoice_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0

            # Handle exchange rate for the invoice
            exchange_rate_id = None
            rate_obj = None
            exchange_rate_value = None
            if int(currency_id) != base_currency_id:
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

                # Create exchange rate record (source_id will be None initially, updated after invoice creation)
                rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=db_session,
                    action='create',
                    from_currency_id=currency_id,
                    to_currency_id=base_currency_id,
                    rate_value=exchange_rate_value,
                    rate_date=invoice_date,
                    app_id=app_id,
                    created_by=current_user.id,
                    source_type='invoice',
                    source_id=None,  # Will update after invoice is created
                    currency_exchange_transaction_id=None
                )

                exchange_rate_id = rate_id

            if invoice_discount_type == "amount":
                calculated_discount_amount = invoice_discount_value
            else:
                calculated_discount_amount = (overall_line_subtotal * invoice_discount_value) / 100

            # Create new invoice object
            new_invoice = SalesInvoice(
                invoice_number=invoice_number,
                customer_id=customer_id,
                invoice_date=invoice_date,
                due_date=due_date,
                currency=currency_id,
                total_line_subtotal=overall_line_subtotal,
                total_amount=total_amount,
                invoice_discount_type=invoice_discount_type,
                invoice_discount_value=invoice_discount_value,
                calculated_discount_amount=calculated_discount_amount,
                invoice_tax_rate=invoice_tax_rate,
                shipping_cost=shipping_cost,
                handling_cost=handling_cost,
                total_tax_amount=total_tax_amount,
                status="draft",
                project_id=project_id,
                invoice_reference=invoice_reference,
                account_receivable_id=account_receivable,
                tax_account_id=tax_account,
                revenue_account_id=revenue_account,
                terms_and_conditions=terms_and_conditions,
                created_by=current_user.id,
                app_id=app_id,
                exchange_rate_id=exchange_rate_id
            )
            db_session.add(new_invoice)
            db_session.flush()

            # Update exchange rate with invoice ID if one was created
            if exchange_rate_id and rate_obj:
                rate_obj.source_id = new_invoice.id
                db_session.add(rate_obj)

            # Handle invoice Items
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

            for idx, (item_type, non_inventory_item, inventory_item, warehouse, description, qty, uom, unit_price,
                      discount_amount,
                      tax_amount,
                      subtotal) in enumerate(zip(
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

                # Create invoice item
                new_item = SalesInvoiceItem(
                    invoice_id=new_invoice.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    location_id=warehouse,
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
                    app_id=app_id
                )

                db_session.add(new_item)

            # ✅ POST TO LEDGER WITH UNPOSTED STATUS
            ledger_success, ledger_message = post_invoice_to_ledger(
                db_session=db_session,
                invoice=new_invoice,
                current_user=current_user,
                status='Posted',
                exchange_rate_id=exchange_rate_id,
                exchange_rate_value=exchange_rate_value,
                base_currency_id=base_currency_id
            )

            if not ledger_success:
                raise Exception(f"Ledger posting failed: {ledger_message}")

            # Log history entry for invoice creation
            history_entry = SalesInvoiceHistory(
                invoice_id=new_invoice.id,
                changed_by=current_user.id,  # Store user ID
                change_description="Invoice Created",  # Description of the change
                app_id=app_id  # Associated company
            )
            db_session.add(history_entry)

            # Add status log entry during invoice creation
            status_log_entry = SalesInvoiceStatusLog(
                invoice_id=new_invoice.id,
                status="draft",  # Initial status of the invoice
                changed_by=current_user.id,  # User who created the invoice
                app_id=app_id  # Associated company
            )
            db_session.add(status_log_entry)
            db_session.commit()

            # Return JSON success response
            return jsonify({
                'success': True,
                'message': 'Invoice added successfully!',
                'invoice_id': new_invoice.id
            })

        except ValueError:
            db_session.rollback()
            logger.error(f"An error occurred: {str(ValueError)}\n{traceback.format_exc()}")
            return jsonify({
                'success': False,
                'message': f'An error occurred: Please place at least more than one line item'
            })

        except UniqueConstraint:
            logger.error(f"An error occurred: {str(UniqueConstraint)}\n{traceback.format_exc()}")
            return jsonify({
                'success': False,
                'message': f'The Invoice number is already in the database. Please reload the page'
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

        inventory_items = db_session.query(InventoryItemVariationLink).join(InventoryItem).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.status == "active"
        ).order_by(InventoryItem.item_name.asc()).all()

        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()
        projects = db_session.query(Project).filter_by(app_id=app_id).all()
        # Render the template with the next invoice number and customers
        return render_template('/sales/new_invoice.html',
                               currencies=currencies, inventory_items=inventory_items, uoms=uoms,
                               customers=customers, modules=modules_data, company=company, role=role, projects=projects,
                               warehouses=warehouses, receivable_accounts=grouped_receivable_accounts,
                               tax_accounts=grouped_tax_payable_accounts, revenue_accounts=grouped_revenue_accounts,
                               base_currency=base_currency, base_currency_code=base_currency_code)


@sales_bp.route('/edit_sales_invoice/<int:invoice_id>', methods=['GET', 'POST'])
@login_required
def edit_sales_invoice(invoice_id):
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
    warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
    invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id, app_id=app_id).first()

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

    # Payable accounts (is_payable == True)
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

    # Revenue accounts (account_type == 'Income')
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

            # ✅ Reverse existing inventory entries before modifying invoice
            inventory_entries = get_inventory_entries_for_invoice(db_session, invoice.id, app_id)
            for entry in inventory_entries:
                reverse_sales_inventory_entries(db_session, entry)

            reverse_sales_invoice_posting(db_session, invoice)

            check_list_not_empty(request.form.getlist('item_type[]'))
            # Extract customer data
            customer_id = request.form.get('customer_id', '').strip()  # Get customer_id (may be empty)
            customer_name = request.form.get('customer_name', '').strip()  # Get customer_name

            if customer_id and customer_id.isdigit():
                # Existing customer (convert ID to integer)
                customer_id = int(customer_id)

            else:
                # New customer (customer_id is empty or invalid)

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

            # Updating general invoice details
            # Extract account IDs
            account_receivable_id = request.form['accountsReceivableAccount'] or None
            tax_account_id = request.form['taxAccount'] or None
            revenue_account_id = request.form['revenueAccount'] or None
            # Convert to integers if they exist
            if account_receivable_id:
                account_receivable_id = int(account_receivable_id)
            if tax_account_id:
                tax_account_id = int(tax_account_id)
            if revenue_account_id:
                revenue_account_id = int(revenue_account_id)

            # Assign the IDs to the invoice
            invoice.account_receivable_id = account_receivable_id
            invoice.tax_account_id = tax_account_id
            invoice.revenue_account_id = revenue_account_id

            invoice.invoice_number = request.form['invoice_number']

            invoice.customer_id = customer_id

            invoice.invoice_date = datetime.strptime(request.form['invoice_date'], '%Y-%m-%d').date()

            invoice.due_date = datetime.strptime(request.form['due_date'], '%Y-%m-%d').date() if request.form[
                'due_date'] else None

            invoice.total_amount = float(request.form['total_amount'])

            invoice.invoice_reference = request.form['invoice_reference'] or None

            invoice.account_receivable_id = request.form['accountsReceivableAccount'] or None
            invoice.tax_account_id = request.form['taxAccount'] or None

            invoice_discount_type = request.form['overall_discount_type'] or None
            invoice_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0.0

            invoice_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0

            total_tax_amount = float(request.form['total_tax'])
            invoice.total_tax_amount = total_tax_amount

            overall_line_subtotal = float(request.form['overall_line_subtotal'])

            if invoice_discount_type == "amount":
                calculated_discount_amount = invoice_discount_value
            else:
                calculated_discount_amount = (overall_line_subtotal * invoice_discount_value) / 100

            invoice.calculated_discount_amount = calculated_discount_amount
            invoice.total_line_subtotal = overall_line_subtotal
            project_id = request.form.get('project')
            project_id = int(project_id) if project_id and project_id != 'None' else None
            invoice.project_id = project_id

            invoice.invoice_tax_rate = invoice_tax_rate

            invoice.shipping_cost = float(request.form['shipping_cost']) if request.form[
                'shipping_cost'] else 0

            invoice.handling_cost = float(request.form['handling_cost']) if request.form[
                'handling_cost'] else 0

            invoice.currency = request.form['currency']
            base_currency_id = request.form.get('base_currency_id')
            exchange_rate = request.form.get('exchange_rate')

            exchange_rate_id, error_response = update_transaction_exchange_rate(
                db_session=db_session,
                transaction=invoice,
                currency_id=int(invoice.currency),
                base_currency_id=int(base_currency_id) if base_currency_id else None,
                exchange_rate=exchange_rate,
                transaction_date=invoice.invoice_date,
                app_id=app_id,
                user_id=current_user.id,
                source_type='invoice'  # Use 'invoice' as the source type
            )

            if error_response:
                return error_response

            # Store the exchange_rate_id (already set by the function, but just to be safe)
            invoice.exchange_rate_id = exchange_rate_id

            invoice.terms_and_conditions = request.form['terms_and_conditions'] or None
            invoice.status = invoice.status
            invoice.updated_at = datetime.now()

            # Handle invoice Items

            # Fetch lists from request form and ensure they are not None
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

            # Ensure all lists have the same length before zipping
            list_lengths = [
                len(item_type_list), len(item_name_list), len(inventory_item_list),
                len(description_list), len(qty_list), len(uom_list),
                len(unit_price_list), len(discount_list), len(tax_list), len(subtotal_list)
            ]

            # Remove old items and re-add updated items
            db_session.query(SalesInvoiceItem).filter_by(invoice_id=invoice.id).delete()

            for idx, (item_type, non_inventory_item, inventory_item, warehouse, description, qty, uom, unit_price,
                      discount_amount,
                      tax_amount, subtotal) in enumerate(zip(
                item_type_list, item_name_list, inventory_item_list, warehouse_list, description_list, qty_list,
                uom_list,
                unit_price_list, discount_list, tax_list, subtotal_list), start=1):

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

                # Create updated SalesInvoiceItem
                updated_item = SalesInvoiceItem(
                    invoice_id=invoice.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    location_id=warehouse,
                    description=description,
                    quantity=qty,
                    currency=invoice.currency,
                    unit_price=unit_price,
                    total_price=subtotal,
                    uom=uom,
                    discount_amount=discount_amount,
                    discount_rate=discount_rate,
                    tax_amount=tax_amount,
                    tax_rate=tax_rate,
                    app_id=app_id
                )

                db_session.add(updated_item)
                db_session.flush()
                db_session.refresh(invoice)  # ← This reloads the invoice from database

            ledger_success, ledger_message = post_invoice_to_ledger(
                db_session=db_session,
                invoice=invoice,
                current_user=current_user,
                status='Posted',
                exchange_rate_id=exchange_rate_id,
                exchange_rate_value=exchange_rate,
                base_currency_id=base_currency_id
            )

            if not ledger_success:
                raise Exception(f"Ledger reposting failed: {ledger_message}")
            invoice.is_posted_to_ledger = True

            # Log history entry for invoice update
            history_entry = SalesInvoiceHistory(
                invoice_id=invoice.id,
                changed_by=current_user.id,  # Store user ID
                change_description="Invoice Updated",  # Description of the change
                app_id=app_id  # Associated company
            )
            db_session.add(history_entry)
            db_session.commit()

            # Add status log entry during invoice update
            status_log_entry = SalesInvoiceStatusLog(
                invoice_id=invoice.id,
                status="updated",  # New status after update
                changed_by=current_user.id,  # User who updated the invoice
                app_id=app_id  # Associated company
            )
            db_session.add(status_log_entry)
            db_session.commit()

            return jsonify({
                'success': True,
                'message': 'Invoice updated successfully!',
                'invoice_id': invoice.id
            })

        except ValueError:
            return jsonify({
                'success': False,
                'message': f'An error occurred: Please place at least more than one line item'
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f"An error occurred: {str(e)}")  # Print the error for debugging
            # Return JSON error response
            return jsonify({
                'success': False,
                'message': f'An error occurred: {str(e)}'
            })

        finally:
            db_session.close()

    else:
        # Fetch existing invoice details
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''

        exchange_rate_value = invoice.exchange_rate.rate if invoice.exchange_rate_id else None

        inventory_items = db_session.query(InventoryItemVariationLink).join(InventoryItem).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.status == "active"
        ).order_by(InventoryItem.item_name.asc()).all()

        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()
        projects = db_session.query(Project).filter_by(app_id=app_id)
        # Render the template with invoice details
        return render_template('/sales/edit_invoice.html', invoice=invoice, currencies=currencies,
                               base_currency=base_currency, base_currency_code=base_currency_code,
                               exchange_rate_value=exchange_rate_value,
                               inventory_items=inventory_items, uoms=uoms, customers=customers, company=company,
                               role=role, modules=modules_data, projects=projects, warehouses=warehouses,
                               receivable_accounts=receivable_accounts,
                               tax_accounts=tax_payable_accounts, revenue_accounts=revenue_accounts,
                               module_name="Sales")


@sales_bp.route('/invoices', methods=['GET', 'POST'])
@login_required
def invoice_management():
    app_id = current_user.app_id
    db_session = Session()

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        # Get base currency (currency_index = 1)
        base_currency = db_session.query(Currency).filter_by(
            app_id=app_id,
            currency_index=1
        ).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        currency = db_session.query(Currency).filter_by(app_id=app_id)
        # Base query
        query = db_session.query(SalesInvoice).filter_by(app_id=app_id)

        # Process filter options
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        filter_type = request.args.get('filter_type', 'invoice_date')  # Default: invoice_date
        status_filter = request.args.get('status')
        filter_applied = bool(start_date or end_date or status_filter)
        # Apply date filters based on filter type
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')

                if filter_type == 'invoice_date':
                    if start_date:
                        query = query.filter(SalesInvoice.invoice_date >= start_date)
                    if end_date:
                        query = query.filter(SalesInvoice.invoice_date <= end_date)
                elif filter_type == 'due_date':
                    if start_date:
                        query = query.filter(SalesInvoice.due_date >= start_date)
                    if end_date:
                        query = query.filter(SalesInvoice.due_date <= end_date)
            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')

        # Apply status filter
        if status_filter:
            query = query.filter(SalesInvoice.status == status_filter)

        # Order by latest created_at
        invoices = query.order_by(SalesInvoice.created_at.desc()).all()
        # Get unposted payments count in one query
        if invoices:
            invoice_ids = [inv.id for inv in invoices]
            unposted_counts = db_session.query(
                SalesTransaction.invoice_id,
                func.count(SalesTransaction.id).label('unposted_count')
            ).filter(
                SalesTransaction.is_posted_to_ledger == False,
                SalesTransaction.invoice_id.in_(invoice_ids)
            ).group_by(SalesTransaction.invoice_id).all()

            unposted_dict = {invoice_id: count for invoice_id, count in unposted_counts}
            for invoice in invoices:
                invoice.unposted_payments_count = unposted_dict.get(invoice.id, 0)
        else:
            for invoice in invoices:
                invoice.unposted_payments_count = 0

        # If no invoices exist, return a message
        if not invoices:
            flash('No invoices found. Add a new invoice to get started.', 'info')

        # Calculate dashboard metrics
        total_invoices = query.count()
        draft_invoices = query.filter(SalesInvoice.status == InvoiceStatus.draft).count()
        unpaid_invoices = query.filter(SalesInvoice.status == InvoiceStatus.unpaid).count()
        partially_paid_invoices = query.filter(SalesInvoice.status == InvoiceStatus.partially_paid).count()
        paid_invoices = query.filter(SalesInvoice.status == InvoiceStatus.paid).count()
        canceled_invoices = query.filter(SalesInvoice.status == InvoiceStatus.canceled).count()

        return render_template(
            '/sales/invoices.html',
            invoices=invoices,
            total_invoices=total_invoices,
            draft_invoices=draft_invoices,
            unpaid_invoices=unpaid_invoices,
            partially_paid_invoices=partially_paid_invoices,
            paid_invoices=paid_invoices,
            canceled_invoices=canceled_invoices,
            company=company,
            currency=base_currency,
            role=role,
            filter_applied=filter_applied,
            modules=modules_data
        )
    except Exception as e:
        flash(f'An error occurred: {str(e)}', 'error')
        logger.error(f'Error is: {e}')
        return redirect(url_for('sales.invoice_management'))
    finally:
        db_session.close()  # Close the session


@sales_bp.route('/bulk_approve_invoices', methods=['POST'])
@login_required
def bulk_approve_invoices():
    """
    Approves multiple invoices by their IDs.
    """
    db_session = Session()
    try:
        # Get data from the request
        data = request.get_json()
        invoice_ids = data.get('invoice_ids', [])

        if not invoice_ids:
            return jsonify({"status": "error", "message": "Invoice IDs are required"}), 400

        # Fetch the invoices from the database
        invoices = db_session.query(SalesInvoice).filter(SalesInvoice.id.in_(invoice_ids)).all()

        if not invoices:
            return jsonify({"status": "error", "message": "No invoices found"}), 404

        # Check if all invoices were found
        found_ids = [invoice.id for invoice in invoices]
        not_found_ids = set(invoice_ids) - set(found_ids)

        if not_found_ids:
            return jsonify({
                "status": "error",
                "message": f"Some invoices not found: {not_found_ids}"
            }), 404

        # ✅ NEW: Check account configuration for all invoices
        invoices_with_missing_accounts = []
        for invoice in invoices:
            if not invoice.account_receivable_id and not invoice.revenue_account_id:
                invoices_with_missing_accounts.append(invoice.invoice_number)

        if invoices_with_missing_accounts:
            return jsonify({
                "status": "error",
                "message": "Account configuration required",
                "details": f"Invoices missing account configuration: {', '.join(invoices_with_missing_accounts)}",
                "action_required": "update_invoice_details",
                "invoices_affected": invoices_with_missing_accounts
            }), 400

        # Update the invoice status to "unpaid" for all invoices
        approved_count = 0
        invoice_numbers = []

        for invoice in invoices:
            # Only approve invoices that are in draft status (or whatever your business logic requires)
            if invoice.status == InvoiceStatus.draft:
                invoice.status = InvoiceStatus.unpaid
                invoice_numbers.append(invoice.invoice_number)
                approved_count += 1

                # Log status change
                status_log = SalesInvoiceStatusLog(
                    invoice_id=invoice.id,
                    status="unpaid",
                    changed_by=current_user.id,
                    app_id=current_user.app_id
                )
                db_session.add(status_log)

        db_session.commit()

        # Return a success response
        if approved_count > 0:
            return jsonify({
                "status": "success",
                "message": f"{approved_count} invoice(s) approved successfully.",
                "approved_invoices": invoice_numbers,
                "approved_count": approved_count
            }), 200
        else:
            return jsonify({
                "status": "warning",
                "message": "No invoices were approved. They may already be in a non-draft status."
            }), 200

    except Exception as e:
        # Handle any errors
        db_session.rollback()
        return jsonify({
            "status": "error",
            "message": f"An error occurred: {str(e)}"
        }), 500
    finally:
        db_session.close()


@sales_bp.route('/approve_sales_invoice', methods=['POST'])
@login_required
def approve_sales_invoice():
    """
    Approves an invoice by its ID.
    """
    db_session = Session()
    try:
        # Get data from the request
        data = request.get_json()
        invoice_id = data.get('invoice_id')

        if not invoice_id:
            return jsonify({"status": "error", "message": "Invoice ID is required"}), 400

        # Fetch the invoice from the database
        invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id).first()
        if not invoice:
            return jsonify({"status": "error", "message": "Invoice not found"}), 404

        # ✅ NEW: Check account configuration
        if not invoice.account_receivable_id and not invoice.revenue_account_id:
            return jsonify({
                "status": "error",
                "message": "Account configuration required",
                "details": f"Invoice {invoice.invoice_number} is missing account configuration",
                "action_required": "update_invoice_details",
                "invoice_affected": invoice.invoice_number
            }), 400

        # Update the invoice status to "approved"
        invoice.status = InvoiceStatus.unpaid  # Assuming "paid" is the status for approved invoices
        db_session.commit()

        # Return a success response
        return jsonify({
            "status": "success",
            "message": f"Invoice {invoice.invoice_number} approved successfully."
        }), 200

    except Exception as e:
        # Handle any errors
        db_session.rollback()
        return jsonify({
            "status": "error",
            "message": f"An error occurred: {str(e)}"
        }), 500
    finally:
        db_session.close()


@sales_bp.route('/bulk_post_invoices_to_ledger', methods=['POST'])
@login_required
def bulk_post_invoices_to_ledger():
    """
    Post multiple invoices to ledger using their pre-configured account information
    """
    db_session = Session()
    try:
        # Get data from the request
        data = request.get_json()
        invoice_ids = data.get('invoice_ids', [])
        base_currency_obj = data.get('currency_obj', {})
        base_currency_id = int(base_currency_obj)
        if not invoice_ids:
            return jsonify({"status": "error", "message": "Invoice IDs are required"}), 400

        # Fetch the invoices from the database
        invoices = db_session.query(SalesInvoice).filter(SalesInvoice.id.in_(invoice_ids)).all()

        if not invoices:
            return jsonify({"status": "error", "message": "No invoices found"}), 404

        # Check if all invoices were found
        found_ids = [invoice.id for invoice in invoices]
        not_found_ids = set(invoice_ids) - set(found_ids)

        if not_found_ids:
            return jsonify({
                "status": "error",
                "message": f"Some invoices not found: {not_found_ids}"
            }), 404

        # Check for invoices already posted to ledger
        already_posted_invoices = []
        valid_invoices = []

        for invoice in invoices:
            if invoice.is_posted_to_ledger:
                already_posted_invoices.append(invoice.invoice_number)
            else:
                # Validate that required accounts are configured
                if not invoice.account_receivable_id or not invoice.revenue_account_id:
                    return jsonify({
                        "status": "error",
                        "message": f"Invoice {invoice.invoice_number} is missing required account configuration"
                    }), 400
                valid_invoices.append(invoice)

        if already_posted_invoices:
            return jsonify({
                "status": "error",
                "message": f"Some invoices are already posted to ledger: {', '.join(already_posted_invoices)}"
            }), 400

        if not valid_invoices:
            return jsonify({
                "status": "error",
                "message": "No valid invoices to post to ledger"
            }), 400

        # Process each invoice
        posted_count = 0
        posted_invoice_numbers = []

        for invoice in valid_invoices:
            try:

                # Compute amounts safely
                taxable_amount = Decimal(str(invoice.total_tax_amount or 0))
                non_taxable_amount = invoice.total_amount - taxable_amount

                if taxable_amount < 0 or non_taxable_amount < 0:
                    continue  # Skip invalid amounts

                currency_id = invoice.currency

                exchange_rate_id = None
                exchange_rate_value = 1
                if int(base_currency_id) != int(currency_id):
                    # Get exchange rate for conversion
                    exchange_rate_obj, exchange_rate_value = get_exchange_rate_and_obj(db_session, currency_id,
                                                                                       base_currency_id,
                                                                                       current_user.app_id,
                                                                                       invoice.invoice_date)
                    exchange_rate_id = exchange_rate_obj.id
                    exchange_rate_value = exchange_rate_value

                # Create journal with all entries

                # Add entries to the journal
                lines = [
                    # Record Accounts Receivable (debit)
                    {
                        "subcategory_id": invoice.account_receivable_id,
                        "amount": float(invoice.total_amount),
                        "dr_cr": "D",
                        "description": f"Invoice {invoice.invoice_number}",
                        "source_type": "sales_invoice_receivable",
                        "source_id": invoice.id
                    },
                    # Record Sales Revenue (credit)
                    {
                        "subcategory_id": invoice.revenue_account_id,
                        "amount": float(non_taxable_amount + (taxable_amount if not invoice.tax_account_id else 0)),
                        "dr_cr": "C",
                        "description": f"Invoice {invoice.invoice_number}" + (
                            " (including tax)" if taxable_amount > 0 and not invoice.tax_account_id else ""),
                        "source_type": "sales_invoice_revenue",
                        "source_id": invoice.id
                    }
                ]

                # Add tax entry if tax account is configured
                if taxable_amount > 0 and invoice.tax_account_id:
                    lines.append({
                        "subcategory_id": invoice.tax_account_id,
                        "amount": float(taxable_amount),
                        "dr_cr": "C",
                        "description": f"Tax for Invoice {invoice.invoice_number}",
                        "source_type": "sales_invoice_tax",
                        "source_id": invoice.id
                    })

                # Create the journal with all entries
                journal, entries = create_transaction(
                    db_session=db_session,
                    date=invoice.invoice_date,
                    currency=currency_id,
                    created_by=current_user.id,
                    app_id=invoice.app_id,
                    narration=f"Invoice {invoice.invoice_number}",
                    project_id=invoice.project_id,
                    vendor_id=invoice.customer_id,
                    exchange_rate_id=exchange_rate_id,
                    lines=lines
                )

                # Process COGS for inventory items
                inventory_items = [item for item in invoice.invoice_items if item.item_type == "inventory"]

                if inventory_items:
                    post_invoice_cogs_to_ledger(
                        db_session=db_session,
                        invoice=invoice,
                        exchange_rate_id=exchange_rate_id,
                        exchange_rate_value=exchange_rate_value,
                        current_user=current_user,
                        base_currency_id=base_currency_id
                    )

                # Mark invoice as posted to ledger
                invoice.is_posted_to_ledger = True

                # Create status log entry
                status_log = SalesInvoiceStatusLog(
                    invoice_id=invoice.id,
                    status="posted_to_ledger",
                    changed_by=current_user.id,
                    app_id=current_user.app_id
                )
                db_session.add(status_log)

                posted_count += 1
                posted_invoice_numbers.append(invoice.invoice_number)

            except Exception as e:
                db_session.rollback()
                logger.error(
                    f"Error posting invoice {invoice.invoice_number} to ledger: {str(e)}\n{traceback.format_exc()}")
                return jsonify({
                    "status": "error",
                    "message": f"Failed to post invoice {invoice.invoice_number}: {str(e)}"
                }), 400

        db_session.commit()
        # ✅ ADD THIS RETURN STATEMENT FOR SUCCESS CASE
        return jsonify({
            "status": "success",
            "message": f"Successfully posted {posted_count} invoices to ledger",
            "posted_count": posted_count,
            "posted_invoices": posted_invoice_numbers
        }), 200

    except Exception as e:
        # Handle any errors
        db_session.rollback()
        logger.error(f"Error in bulk post to ledger: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": f"An error occurred: {str(e)}"
        }), 500
    finally:
        db_session.close()


@sales_bp.route('/bulk_cancel_invoices', methods=['POST'])
@login_required
def bulk_cancel_invoices():
    """
    Cancel multiple invoices with strict failure handling - ANY issue fails the entire operation
    """
    app_id = current_user.app_id
    db_session = Session()

    try:
        data = request.get_json()
        invoice_ids = data.get('invoice_ids', [])

        if not invoice_ids:
            return jsonify({"status": "error", "message": "Invoice IDs are required"}), 400

        # Fetch the invoices from the database
        invoices = db_session.query(SalesInvoice).filter(
            SalesInvoice.id.in_(invoice_ids),
            SalesInvoice.app_id == app_id
        ).all()

        if not invoices:
            return jsonify({"status": "error", "message": "No invoices found"}), 404

        canceled_count = 0
        canceled_invoice_numbers = []

        for invoice in invoices:
            # Check for already canceled invoices
            if invoice.status == InvoiceStatus.canceled:
                raise Exception(f"Invoice {invoice.invoice_number} is already canceled")

            # Check if invoice can be canceled
            if invoice.status not in [InvoiceStatus.draft, InvoiceStatus.unpaid]:
                raise Exception(
                    f"Invoice {invoice.invoice_number} cannot be canceled (current status: {invoice.status.value})")

            # ===== HANDLE PAYMENT TRANSACTIONS =====
            payment_transactions = db_session.query(SalesTransaction).filter(
                SalesTransaction.invoice_id == invoice.id,
                SalesTransaction.app_id == app_id
            ).all()

            for payment in payment_transactions:
                # Get payment allocations for this transaction
                payment_allocations = db_session.query(PaymentAllocation).filter(
                    PaymentAllocation.payment_id == payment.id
                ).all()

                # ===== CHECK IF THIS IS A CREDIT APPLICATION =====
                is_credit_application = False
                for allocation in payment_allocations:
                    if allocation.payment_type == 'credit':
                        is_credit_application = True
                        break

                if is_credit_application:
                    # Handle credit application reversal
                    success, msg, credit_stats = reverse_credit_application(
                        db_session=db_session,
                        payment_allocation=payment_allocations[0],
                        transaction=payment,
                        current_user=current_user,
                        action="cancel"
                    )

                    if not success:
                        raise Exception(f"Failed to reverse credit application: {msg}")

                    logger.info(f"Reversed credit application for transaction {payment.id}")
                    # Skip further processing for this transaction
                    continue

                # ===== HANDLE REGULAR PAYMENTS =====
                for allocation in payment_allocations:
                    # Handle credits created FROM this payment (overpayments)
                    created_credits = db_session.query(CustomerCredit).filter_by(
                        payment_allocation_id=allocation.id
                    ).all()

                    for credit in created_credits:
                        # Delete credits properly
                        success, msg, stats = manage_customer_credits(
                            db_session=db_session,
                            source=allocation,
                            action='delete',
                            credit=credit,
                            force=True,
                            current_user=current_user
                        )
                        if not success:
                            raise Exception(f"Failed to delete credit {credit.id}: {msg}")

                    # Delete the allocation
                    db_session.delete(allocation)

                # Handle bulk payment credit creation
                if payment.bulk_payment_id:
                    bulk_payment = payment.bulk_payment

                    # Calculate amount in bulk payment currency
                    if payment.currency_id == bulk_payment.currency_id:
                        amount_in_bulk_currency = float(payment.amount_paid)
                    else:
                        # Different currency - convert via base
                        invoice_rate = float(
                            payment.invoice.exchange_rate.rate) if payment.invoice and payment.invoice.exchange_rate else 1
                        bulk_rate = float(bulk_payment.exchange_rate.rate) if bulk_payment.exchange_rate else 1
                        amount_in_base = float(payment.amount_paid) * invoice_rate
                        amount_in_bulk_currency = amount_in_base / bulk_rate if bulk_rate > 0 else amount_in_base

                    # Check if there's already a credit for this transaction
                    existing_credit = db_session.query(CustomerCredit).filter(
                        CustomerCredit.bulk_payment_id == bulk_payment.id,
                        CustomerCredit.notes.like(f"%transaction #{payment.id}%")
                    ).first()

                    if existing_credit:
                        # Update existing credit
                        existing_credit.original_amount += amount_in_bulk_currency
                        existing_credit.available_amount += amount_in_bulk_currency
                        logger.info(f"Updated existing credit #{existing_credit.id} for transaction {payment.id}")
                    else:
                        # Create new credit using manage_bulk_payment
                        success, msg, data = manage_bulk_payment(
                            db_session=db_session,
                            source=bulk_payment,
                            action='create_credit_from_unallocated',
                            amount=amount_in_bulk_currency,
                            transaction_id=payment.id,
                            reason='invoice_cancellation',
                            notes=f"Credit created from cancelled invoice #{invoice.invoice_number}, transaction #{payment.id}",
                            current_user=current_user
                        )

                        if not success:
                            raise Exception(f"Failed to create credit for transaction {payment.id}: {msg}")

            # ===== REVERSE INVENTORY ENTRIES =====
            inventory_entries = get_inventory_entries_for_invoice(db_session, invoice.id, app_id)
            for entry in inventory_entries:
                reverse_sales_inventory_entries(db_session, entry)
                db_session.delete(entry)

            # ===== REVERSE LEDGER POSTINGS =====
            reverse_sales_invoice_posting(db_session, invoice, current_user)

            # ===== UPDATE INVOICE STATUS =====
            invoice.status = InvoiceStatus.canceled
            invoice.is_posted_to_ledger = False
            invoice.updated_at = datetime.now()

            # Create status log
            status_log = SalesInvoiceStatusLog(
                invoice_id=invoice.id,
                status="canceled",
                changed_by=current_user.id,
                app_id=app_id,
                notes="Bulk cancellation with payment allocation cleanup"
            )
            db_session.add(status_log)

            canceled_count += 1
            canceled_invoice_numbers.append(invoice.invoice_number)

        # Clear cache
        clear_stock_history_cache()

        # Commit only if everything succeeded
        db_session.commit()

        return jsonify({
            "status": "success",
            "message": f"{canceled_count} invoice(s) canceled successfully.",
            "canceled_invoices": canceled_invoice_numbers,
            "canceled_count": canceled_count
        }), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f"Bulk cancel failed - transaction rolled back: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Bulk cancellation failed: {str(e)}"
        }), 500
    finally:
        db_session.close()


@sales_bp.route('/cancel_sales_invoice', methods=['POST'])
@login_required
def cancel_sales_invoice():
    """
    Cancels an invoice by its ID. Any payments made are converted to customer credits.
    """
    db_session = Session()

    try:
        data = request.get_json(force=True)
        invoice_id = data.get("invoice_id")

        if not invoice_id:
            return jsonify({'status': 'error', 'message': 'Invoice ID is required'}), 400

        invoice_id = int(invoice_id)

        invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id).first()

        if not invoice:
            return jsonify({'status': 'error', 'message': 'Invoice not found'}), 404

        if invoice.status == InvoiceStatus.canceled:
            return jsonify({'status': 'error', 'message': 'Invoice already canceled'}), 400

        # ===== HANDLE PAYMENTS AND CONVERT TO CREDITS =====
        payment_transactions = db_session.query(SalesTransaction).filter(
            SalesTransaction.invoice_id == invoice.id,
            SalesTransaction.app_id == current_user.app_id
        ).all()

        for transaction in payment_transactions:
            # Skip already cancelled transactions
            if transaction.payment_status == SalesPaymentStatus.cancelled:
                continue

            # Get payment allocations
            payment_allocations = db_session.query(PaymentAllocation).filter(
                PaymentAllocation.payment_id == transaction.id
            ).all()

            # ===== CHECK IF THIS IS A CREDIT APPLICATION =====
            is_credit_application = False
            for allocation in payment_allocations:
                if allocation.payment_type == 'credit':
                    is_credit_application = True
                    break

            if is_credit_application:
                # Handle credit application reversal
                success, msg, credit_stats = reverse_credit_application(
                    db_session=db_session,
                    payment_allocation=payment_allocations[0],
                    transaction=transaction,
                    current_user=current_user,
                    action="cancel"
                )

                if not success:
                    logger.error(
                        f"Failed to reverse credit application for transaction {transaction.id}: {msg}")
                    # Continue with other transactions even if one fails
                else:
                    logger.info(
                        f"Reversed credit application for transaction {transaction.id}")
                    # Skip the rest of the processing for this transaction
                    # Mark as cancelled and continue to next transaction
                    transaction.payment_status = SalesPaymentStatus.cancelled
                    transaction.is_posted_to_ledger = False
                    continue

            # Handle credits created FROM this payment (regular overpayments)
            for allocation in payment_allocations:
                created_credits = db_session.query(CustomerCredit).filter_by(
                    payment_allocation_id=allocation.id
                ).all()

                for credit in created_credits:
                    success, msg, stats = manage_customer_credits(
                        db_session=db_session,
                        source=allocation,
                        action='delete',
                        credit=credit,
                        force=True,
                        current_user=current_user
                    )
                    if not success:
                        logger.error(f"Failed to delete credit {credit.id}: {msg}")

            # Handle bulk payment credit creation
            if transaction.bulk_payment_id:
                bulk_payment = transaction.bulk_payment

                # Calculate amount in bulk payment currency
                if transaction.currency_id == bulk_payment.currency_id:
                    amount_in_bulk_currency = float(transaction.amount_paid)
                else:
                    # Different currency - convert via base
                    invoice_rate = float(
                        transaction.invoice.exchange_rate.rate) if transaction.invoice and transaction.invoice.exchange_rate else 1
                    bulk_rate = float(bulk_payment.exchange_rate.rate) if bulk_payment.exchange_rate else 1
                    amount_in_base = float(transaction.amount_paid) * invoice_rate
                    amount_in_bulk_currency = amount_in_base / bulk_rate if bulk_rate > 0 else amount_in_base

                # Check if there's already a credit for this transaction
                existing_credit = db_session.query(CustomerCredit).filter(
                    CustomerCredit.bulk_payment_id == bulk_payment.id,
                    CustomerCredit.notes.like(f"%transaction #{transaction.id}%")
                ).first()

                if existing_credit:
                    # Update existing credit
                    existing_credit.original_amount += amount_in_bulk_currency
                    existing_credit.available_amount += amount_in_bulk_currency
                    logger.info(
                        f"Updated existing credit #{existing_credit.id} for transaction {transaction.id}")
                else:
                    # Create new credit using manage_bulk_payment
                    success, msg, data = manage_bulk_payment(
                        db_session=db_session,
                        source=bulk_payment,
                        action='create_credit_from_unallocated',
                        amount=amount_in_bulk_currency,
                        transaction_id=transaction.id,
                        reason='invoice_cancellation',
                        notes=f"Credit created from cancelled invoice #{invoice.invoice_number}, transaction #{transaction.id}",
                        current_user=current_user
                    )

                    if not success:
                        logger.error(
                            f"Failed to create credit for transaction {transaction.id}: {msg}")
                    else:
                        logger.info(
                            f"Created credit for transaction {transaction.id} from invoice cancellation")

            # Mark transaction as cancelled
            transaction.payment_status = SalesPaymentStatus.cancelled
            transaction.is_posted_to_ledger = False

            # Delete payment allocations (they're no longer needed)
            for allocation in payment_allocations:
                db_session.delete(allocation)

        # ===== REVERSE INVENTORY ENTRIES =====
        inventory_entries = get_inventory_entries_for_invoice(
            db_session, invoice.id, current_user.app_id)
        for entry in inventory_entries:
            reverse_sales_inventory_entries(db_session, entry)
            db_session.delete(entry)

        # ===== REVERSE LEDGER POSTINGS =====
        reverse_sales_invoice_posting(db_session, invoice, current_user)

        # ===== UPDATE INVOICE STATUS =====
        invoice.status = InvoiceStatus.canceled
        invoice.is_posted_to_ledger = False
        invoice.updated_at = datetime.now()

        # ===== ADD STATUS LOG =====
        status_log = SalesInvoiceStatusLog(
            invoice_id=invoice.id,
            status="canceled",
            changed_by=current_user.id,
            app_id=current_user.app_id
        )
        db_session.add(status_log)

        # ===== CLEAR CACHE =====
        clear_stock_history_cache()

        db_session.commit()

        return jsonify({
            'status': 'success',
            'message': f'Invoice {invoice.invoice_number} canceled successfully. Payments converted to customer credits.'
        })

    except Exception as e:
        db_session.rollback()
        logger.exception("Error cancelling invoice")
        return jsonify({
            'status': 'error',
            'message': f'Invoice cancellation failed: {str(e)}'
        }), 500
    finally:
        db_session.close()


@sales_bp.route('/bulk_delete_invoices', methods=['POST'])
@login_required
def bulk_delete_invoices():
    """
    Permanently delete multiple invoices and all related records
    Even if invoices have payments, they can be deleted - payments are converted to customer credits
    All transactions (including cancelled) are deleted
    """
    app_id = current_user.app_id
    db_session = Session()

    try:
        data = request.get_json()
        invoice_ids = data.get('invoice_ids', [])

        if not invoice_ids:
            return jsonify({"status": "error", "message": "Invoice IDs are required"}), 400

        # Fetch the invoices from the database
        invoices = db_session.query(SalesInvoice).filter(
            SalesInvoice.id.in_(invoice_ids),
            SalesInvoice.app_id == app_id
        ).all()

        if not invoices:
            return jsonify({"status": "error", "message": "No invoices found"}), 404

        deleted_count = 0
        deleted_invoice_numbers = []
        errors = []
        warnings = []

        for invoice in invoices:
            try:
                # Allow deletion regardless of status, but warn
                if invoice.status not in [InvoiceStatus.draft, InvoiceStatus.unpaid]:
                    warnings.append(
                        f"Invoice {invoice.invoice_number} has status {invoice.status.value} but will be deleted anyway."
                    )

                # ===== HANDLE PAYMENTS AND CONVERT TO CREDITS =====
                # Find ALL transactions for this invoice (including cancelled)
                transactions = db_session.query(SalesTransaction).filter(
                    SalesTransaction.invoice_id == invoice.id,
                    SalesTransaction.app_id == app_id
                ).all()

                for transaction in transactions:
                    # Get payment allocations for this transaction
                    allocations = db_session.query(PaymentAllocation).filter_by(
                        payment_id=transaction.id
                    ).all()

                    # ===== CHECK IF THIS IS A CREDIT APPLICATION =====
                    is_credit_application = False
                    for allocation in allocations:
                        if allocation.payment_type == 'credit':
                            is_credit_application = True
                            break

                    if is_credit_application:
                        # Handle credit application reversal with action='delete'
                        success, msg, credit_stats = reverse_credit_application(
                            db_session=db_session,
                            payment_allocation=allocations[0],
                            transaction=transaction,
                            current_user=current_user,
                            action='delete'  # Permanently delete payment allocation
                        )

                        if not success:
                            errors.append(
                                f"Failed to reverse credit application for transaction {transaction.id}: {msg}")
                            db_session.rollback()
                            continue
                        else:
                            logger.info(f"Reversed credit application for transaction {transaction.id}")
                            # Skip the rest of the processing for this transaction
                            continue

                    # Handle credits created FROM this payment (regular overpayments)
                    for allocation in allocations:
                        created_credits = db_session.query(CustomerCredit).filter_by(
                            payment_allocation_id=allocation.id
                        ).all()

                        for credit in created_credits:
                            # Delete any existing credits
                            success, msg, stats = manage_customer_credits(
                                db_session=db_session,
                                source=allocation,
                                action='delete',
                                credit=credit,
                                force=True,
                                current_user=current_user
                            )
                            if not success:
                                errors.append(f"Failed to delete credit {credit.id}: {msg}")
                            else:
                                logger.info(f"Deleted credit #{credit.id} from payment allocation {allocation.id}")

                    # Only create credits for non-cancelled transactions from bulk payments
                    if (transaction.payment_status != SalesPaymentStatus.cancelled and
                            transaction.bulk_payment_id and
                            not is_credit_application):  # Skip credit applications

                        bulk_payment = transaction.bulk_payment

                        # Calculate amount in bulk payment currency
                        if transaction.currency_id == bulk_payment.currency_id:
                            amount_in_bulk_currency = float(transaction.amount_paid)
                        else:
                            # Different currency - convert via base
                            invoice_rate = float(
                                transaction.invoice.exchange_rate.rate) if transaction.invoice and transaction.invoice.exchange_rate else 1
                            bulk_rate = float(bulk_payment.exchange_rate.rate) if bulk_payment.exchange_rate else 1
                            amount_in_base = float(transaction.amount_paid) * invoice_rate
                            amount_in_bulk_currency = amount_in_base / bulk_rate if bulk_rate > 0 else amount_in_base

                        # Check if there's already a credit for this transaction
                        existing_credit = db_session.query(CustomerCredit).filter(
                            CustomerCredit.bulk_payment_id == bulk_payment.id,
                            CustomerCredit.notes.like(f"%transaction #{transaction.id}%")
                        ).first()

                        if existing_credit:
                            # Update existing credit
                            existing_credit.original_amount += amount_in_bulk_currency
                            existing_credit.available_amount += amount_in_bulk_currency
                            logger.info(
                                f"Updated existing credit #{existing_credit.id} for transaction {transaction.id}")
                        else:
                            # Create new credit using manage_bulk_payment
                            success, msg, data = manage_bulk_payment(
                                db_session=db_session,
                                source=bulk_payment,
                                action='create_credit_from_unallocated',
                                amount=amount_in_bulk_currency,
                                transaction_id=transaction.id,
                                reason='invoice_deletion',
                                notes=f"Credit created from deleted invoice #{invoice.invoice_number}, transaction #{transaction.id}",
                                current_user=current_user
                            )

                            if not success:
                                errors.append(f"Failed to create credit for transaction {transaction.id}: {msg}")
                            else:
                                logger.info(f"Created credit for transaction {transaction.id} from invoice deletion")

                    # Delete payment allocations (only if not already handled by reverse_credit_application)
                    if not is_credit_application:
                        for allocation in allocations:
                            db_session.delete(allocation)

                    # Reverse ledger postings for this transaction (if not cancelled and not credit application)
                    if (transaction.payment_status != SalesPaymentStatus.cancelled and
                            not is_credit_application):
                        try:
                            reverse_sales_transaction_posting(db_session, transaction)
                        except Exception as reversal_error:
                            logger.error(f"Ledger reversal error for transaction {transaction.id}: {reversal_error}")
                            errors.append(f"Ledger reversal error for transaction {transaction.id}: {reversal_error}")

                    # Delete the transaction (YES, even cancelled ones)
                    db_session.delete(transaction)
                    logger.info(f"Deleted transaction {transaction.id} for invoice {invoice.invoice_number}")

                # ===== REVERSE INVENTORY ENTRIES =====
                try:
                    inventory_entries = get_inventory_entries_for_invoice(db_session, invoice.id, app_id)
                    for entry in inventory_entries:
                        reverse_sales_inventory_entries(db_session, entry)
                        db_session.delete(entry)

                    try:
                        clear_stock_history_cache()
                    except Exception as e:
                        logger.error(f"Cache clearing failed: {e}")

                except Exception as reversal_error:
                    error_msg = f"Failed to reverse inventory for invoice {invoice.invoice_number}: {str(reversal_error)}"
                    logger.error(error_msg)
                    errors.append(error_msg)
                    # Continue with deletion even if inventory reversal fails

                # ===== REVERSE LEDGER POSTINGS FOR INVOICE =====
                if invoice.is_posted_to_ledger:
                    try:
                        reverse_sales_invoice_posting(db_session, invoice, current_user)
                    except Exception as ledger_error:
                        error_msg = f"Failed to reverse ledger postings for invoice {invoice.invoice_number}: {str(ledger_error)}"
                        logger.error(error_msg)
                        errors.append(error_msg)
                        # Continue with deletion even if ledger reversal fails

                # ===== DELETE INVOICE RELATED RECORDS =====
                invoice_number = invoice.invoice_number

                # Delete invoice items
                db_session.query(SalesInvoiceItem).filter(
                    SalesInvoiceItem.invoice_id == invoice.id,
                    SalesInvoiceItem.app_id == app_id
                ).delete()

                # Delete status logs
                db_session.query(SalesInvoiceStatusLog).filter(
                    SalesInvoiceStatusLog.invoice_id == invoice.id,
                    SalesInvoiceStatusLog.app_id == app_id
                ).delete()

                # Delete history entries
                db_session.query(SalesInvoiceHistory).filter(
                    SalesInvoiceHistory.invoice_id == invoice.id,
                    SalesInvoiceHistory.app_id == app_id
                ).delete()

                # Delete the invoice itself
                db_session.delete(invoice)

                deleted_count += 1
                deleted_invoice_numbers.append(invoice_number)
                logger.info(f"Successfully deleted invoice {invoice_number}")

            except Exception as invoice_error:
                error_msg = f"Error deleting invoice {invoice.invoice_number}: {str(invoice_error)}"
                logger.error(error_msg)
                errors.append(error_msg)
                continue

        # ===== COMMIT ALL CHANGES =====
        db_session.commit()

        # Prepare response
        if deleted_count > 0:
            response = {
                "status": "success",
                "message": f"{deleted_count} invoice(s) deleted successfully. Active payments converted to customer credits.",
                "deleted_invoices": deleted_invoice_numbers,
                "deleted_count": deleted_count
            }
            if warnings:
                response["warnings"] = warnings
            if errors:
                response["errors"] = errors
            return jsonify(response), 200
        else:
            return jsonify({
                "status": "error",
                "message": "No invoices were deleted.",
                "errors": errors,
                "warnings": warnings
            }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk delete: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"An error occurred: {str(e)}"
        }), 500
    finally:
        db_session.close()


@sales_bp.route('/invoice/<int:invoice_id>', methods=['GET'])
@login_required
def invoice_details(invoice_id):
    app_id = current_user.app_id
    with Session() as db_session:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()

        # Fetch the invoice by ID and app_id
        invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id, app_id=app_id).first()

        # Get base currency for reference
        currencies = db_session.query(Currency).filter_by(app_id=app_id)
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id
        tax_account_query = db_session.query(JournalEntry).filter_by(source_type="sales_invoice_tax",
                                                                     source_id=invoice_id, app_id=app_id).first()

        if not invoice:
            return jsonify({"error": "Invoice not found"}), 404

        # Calculate the total payments made, filtering by payment_status and excluding refunded and cancelled payments
        total_paid = db_session.query(func.sum(SalesTransaction.amount_paid)).filter(
            SalesTransaction.invoice_id == invoice_id,
            SalesTransaction.payment_status.in_(
                [SalesPaymentStatus.full, SalesPaymentStatus.partial, SalesPaymentStatus.paid]),
            # Use enum values
            SalesTransaction.payment_status != SalesPaymentStatus.refund,  # Exclude 'refund' payments
            SalesTransaction.payment_status != SalesPaymentStatus.cancelled  # Exclude 'cancelled' payments
        ).scalar() or Decimal('0.00')

        # Calculate the invoice balance
        invoice_balance = Decimal(invoice.total_amount) - total_paid

        # Get exchange rate information
        exchange_rate_info = None
        if invoice.exchange_rate:
            # Calculate rate in correct direction (1 invoice currency = X base currency)
            if invoice.exchange_rate.from_currency_id == invoice.currency:
                rate_value = float(invoice.exchange_rate.rate)
            else:
                rate_value = float(1 / invoice.exchange_rate.rate)

            exchange_rate_info = {
                'id': invoice.exchange_rate.id,
                'rate': rate_value,
                'date': invoice.exchange_rate.date.strftime("%Y-%m-%d") if invoice.exchange_rate.date else None,
                'from_currency': invoice.currencies.user_currency if invoice.currencies else None,
                'to_currency': base_currency_code,
                'is_foreign': invoice.currency != base_currency.id
            }

        # Prepare invoice data
        invoice_data = {
            "id": invoice.id,
            "invoice_number": invoice.invoice_number,
            "invoice_date": invoice.invoice_date.strftime("%Y-%m-%d"),
            "due_date": invoice.due_date.strftime("%Y-%m-%d") if invoice.due_date else None,
            "customer_id": invoice.customer_id,
            "customer": {
                "name": invoice.customer.vendor_name,
                "contact": f"{invoice.customer.tel_contact}{' | ' + invoice.customer.email if invoice.customer.email else ''}",
                "address": invoice.customer.address or None,
                "city_country": f"{invoice.customer.city or ''} {invoice.customer.country or ''}".strip() or None
            } if invoice.customer else None,
            "currency": invoice.currencies.user_currency if invoice.currency else None,
            "currency_id": invoice.currency if invoice.currency else None,
            "base_currency": base_currency_code,
            "base_currency_id": base_currency_id,
            "total_amount": invoice.total_amount,
            "total_paid": float(total_paid),  # Convert to float
            "remaining_balance": float(invoice_balance),  # Convert to float
            "status": invoice.status,
            "subtotal": invoice.total_line_subtotal,
            "exchange_rate": exchange_rate_info,  # Add exchange rate information
            "invoice_notes": [
                {"id": note.id, "content": note.note_content, "type": note.note_type, "created_by": note.user.name,
                 "recipient": note.user_recipient.name if note.user_recipient else "",
                 "created_at": note.created_at} for note in invoice.invoice_notes
            ],

            "approvals": [
                {"id": app.id, "approver": app.approver_id, "status": app.approval_status} for app in invoice.approvals
            ],
            "invoice_items": [
                {
                    "id": item.id,
                    "item_name": item.item_name if item.item_name else item.inventory_item_variation_link.inventory_item.item_name,
                    "description": item.description if item.item_name else item.inventory_item_variation_link.inventory_item.item_description,
                    "quantity": item.quantity,
                    "uom": item.unit_of_measurement.full_name,
                    "unit_price": item.unit_price,
                    "tax_rate": item.tax_rate,
                    "tax_amount": item.tax_amount,
                    "discount_amount": item.discount_amount,
                    "discount_rate": item.discount_rate,
                    "total_price": item.total_price
                } for item in invoice.invoice_items
            ],
            "terms_and_conditions": invoice.terms_and_conditions,
            "invoice_discount": invoice.calculated_discount_amount,
            "invoice_tax_rate": invoice.invoice_tax_rate,
            "shipping_cost": invoice.shipping_cost,
            "handling_cost": invoice.handling_cost,
            "users": [{
                "id": user.id,
                "name": user.name
            } for user in company.users
            ],
            "sales_order": [
                {
                    'id': invoice.sales_orders.id,
                    'sales_order_number': invoice.sales_orders.sales_order_number
                } if invoice.sales_orders else None
            ],

            "quotation": [
                {
                    'id': invoice.quotation.id,
                    'quotation_number': invoice.quotation.quotation_number
                } if invoice.quotation else None
            ],

            "tax_accounts": [
                {
                    'tax_account_id': tax_account_query.subcategory_id,
                    'tax_account': tax_account_query.chart_of_accounts.sub_category,
                    'tax_account_category_id': tax_account_query.chart_of_accounts.categories.id,
                    'tax_account_category': tax_account_query.chart_of_accounts.categories.category,
                }
            ] if tax_account_query else [],

            "delivery_notes": [
                {
                    'id': delivery_note.id if delivery_note else None,
                    'delivery_note_number': delivery_note.delivery_number if delivery_note else None
                } for delivery_note in invoice.delivery_notes
            ] if invoice.delivery_notes else [],

            "sales_transactions": [
                {
                    'id': transaction.id,
                    'sales_txn_id': transaction.payment_id,
                    'payment_date': transaction.sales_transaction.payment_date.strftime("%Y-%m-%d")
                    if transaction.sales_transaction and transaction.sales_transaction.payment_date
                    else None,
                    'amount_paid': float(transaction.sales_transaction.amount_paid),
                    'payment_mode': transaction.payment_modes.payment_mode if transaction.payment_modes else None,
                    'payment_status': transaction.sales_transaction.payment_status,
                    'reference_number': transaction.reference,
                    "allocated_base_amount": float(transaction.allocated_base_amount),
                    "allocated_tax_amount": float(transaction.allocated_tax_amount),
                    'is_posted_to_ledger': transaction.is_posted_to_ledger,

                    # 👇 Add payment receipts under each transaction
                    'payment_receipts': [
                        {
                            'id': receipt.id,
                            'payment_receipt_number': receipt.payment_receipt_number,
                            'reference_number': receipt.reference_number if receipt.reference_number else None,
                            'payment_date': receipt.payment_date.strftime("%Y-%m-%d") if receipt.payment_date else None,
                            'amount_received': float(receipt.amount_received)
                        } for receipt in transaction.sales_transaction.payment_receipt
                    ] if transaction.sales_transaction else []
                } for transaction in invoice.payment_allocations
            ],
            # Add the balance to the response
            "invoice_balance": round(float(invoice_balance), 2),  # Round the balance to 2 decimal places
            "created_at": invoice.created_at,
            "updated_at": invoice.updated_at,
            "created_by": invoice.user.name

        }
    # Return JSON or render template
    if request.args.get('format') == 'json':
        return jsonify(invoice_data)
    else:
        return render_template('/sales/invoice_details.html', invoice=invoice_data, company=company,
                               modules=modules_data, payment_modes=payment_modes, base_currency=base_currency,
                               base_currency_code=base_currency_code, base_currency_id=base_currency.id,
                               currencies=currencies,
                               role=role, module_name="Sales")
