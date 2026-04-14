import os
import traceback
from datetime import datetime, date, timedelta
from decimal import Decimal

from flask import request, jsonify, render_template, flash, redirect, url_for
from flask_login import login_required, current_user
from sqlalchemy import or_, func, literal, desc, UniqueConstraint, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload
from werkzeug.utils import secure_filename

from ai import resolve_exchange_rate_for_transaction

from db import Session
from models import Company, Module, InventoryLocation, ChartOfAccounts, Vendor, DirectSalesTransaction, \
    InventoryItemVariationLink, DirectSaleItem, Project, PaymentMode, UnitOfMeasurement, Currency, PaymentAllocation, \
    SalesTransaction, SalesPaymentStatus, SalesInvoice, SalesInvoiceHistory, SalesInvoiceStatusLog, SalesInvoiceItem, \
    InvoiceStatus, JournalEntry, Quotation, QuotationHistory, QuotationStatusLog, QuotationAttachment, QuotationItem, \
    QuotationStatus, SalesOrder, SalesOrderItem, SalesOrderHistory, SalesOrderAttachment, SalesOrderStatusLog, \
    OrderStatus, InventoryItem
from services.inventory_helpers import reverse_inventory_entry
from services.post_to_ledger import post_invoice_cogs_to_ledger, post_invoice_to_ledger
from services.post_to_ledger_reversal import reverse_sales_invoice_posting
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    generate_next_invoice_number, get_inventory_entries_for_invoice, reverse_sales_inventory_entries, \
    generate_next_quotation_number, generate_next_sales_order_number, update_transaction_exchange_rate
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj, get_or_create_exchange_rate_for_transaction, \
    handle_exchange_rate_for_edit
from utils_and_helpers.file_utils import allowed_file
from utils_and_helpers.forms import get_locked_record
from utils_and_helpers.lists import check_list_not_empty
from . import sales_bp

import logging

logger = logging.getLogger(__name__)


@sales_bp.route('/add_sales_order', methods=['GET', 'POST'])
@login_required
def add_sales_order():
    from app import UPLOAD_FOLDER_SALES_ORDER

    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
    warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
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

            # Extract sales_order details

            # Generate the next sales_order number
            next_sales_order_number = generate_next_sales_order_number()

            # Add these after extracting customer data
            base_currency_id = int(request.form.get('base_currency_id')) if request.form.get(
                'base_currency_id') else None
            exchange_rate = request.form.get('exchange_rate')
            project_id = request.form.get('project') or None

            # Extract sales_order details
            sales_order_number = next_sales_order_number
            sales_order_reference = request.form['sales_order_reference'] or None
            sales_order_date = datetime.strptime(request.form['sales_order_date'], '%Y-%m-%d').date()

            expiry_date = datetime.strptime(request.form['expiry_date'], '%Y-%m-%d').date() if request.form[
                'expiry_date'] else None

            total_line_subtotal = round(float(request.form['overall_line_subtotal']), 2)

            total_amount = float(request.form['total_amount'])

            shipping_cost = float(request.form['shipping_cost'])

            handling_cost = float(request.form['handling_cost'])

            total_tax_amount = float(request.form['total_tax'])

            terms_and_conditions = request.form['terms_and_conditions'] or None

            currency = request.form['currency']

            # Handle sales_order general discount and tax

            sales_order_discount_type = request.form['overall_discount_type'] or None
            sales_order_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0

            sales_order_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0

            if sales_order_discount_type == "amount":
                calculated_discount_amount = sales_order_discount_value
            else:
                calculated_discount_amount = (total_line_subtotal * sales_order_discount_value) / 100

            # Handle exchange rate for the sales order
            exchange_rate_id = None
            rate_obj = None
            exchange_rate_value = None

            if int(currency) != base_currency_id:
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

                # Create or get exchange rate record
                rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=db_session,
                    action='create',
                    from_currency_id=currency,
                    to_currency_id=base_currency_id,
                    rate_value=exchange_rate_value,
                    rate_date=sales_order_date,
                    app_id=app_id,
                    created_by=current_user.id,
                    source_type='sales_order',
                    source_id=None,  # Will update after sales order is created
                    currency_exchange_transaction_id=None
                )

                exchange_rate_id = rate_id
            # Create new sales_order object
            new_sales_order = SalesOrder(
                sales_order_number=sales_order_number,
                sales_order_reference=sales_order_reference,
                customer_id=customer_id,
                sales_order_date=sales_order_date,
                expiry_date=expiry_date,
                currency=currency,
                total_line_subtotal=total_line_subtotal,
                total_amount=total_amount,
                sales_order_discount_type=sales_order_discount_type,
                sales_order_discount_value=sales_order_discount_value,
                calculated_discount_amount=calculated_discount_amount,
                sales_order_tax_rate=sales_order_tax_rate,
                total_tax_amount=total_tax_amount,
                shipping_cost=shipping_cost,
                handling_cost=handling_cost,
                status="draft",
                terms_and_conditions=terms_and_conditions,
                app_id=app_id,
                exchange_rate_id=exchange_rate_id,
                project_id=project_id
            )
            db_session.add(new_sales_order)
            db_session.flush()

            # ADD THIS after flush
            if exchange_rate_id and rate_obj:
                rate_obj.source_id = new_sales_order.id
                db_session.add(rate_obj)

            # Handle sales_order Items
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

                # Create sales_order
                new_item = SalesOrderItem(
                    sales_order_id=new_sales_order.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    location_id=warehouse,
                    description=description,
                    quantity=qty,
                    currency=currency,
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

            # Commit all items to the database
            db_session.flush()

            # Log history entry for sales_order creation
            history_entry = SalesOrderHistory(
                sales_order_id=new_sales_order.id,
                changed_by=current_user.id,  # Store user ID
                change_description="Sales Order Created",  # Description of the change
                app_id=app_id  # Associated company
            )
            db_session.add(history_entry)
            db_session.flush()

            # Add status log entry during sales_order creation
            status_log_entry = SalesOrderStatusLog(
                sales_order_id=new_sales_order.id,
                status="draft",  # Initial status of the sales_order
                changed_by=current_user.id,  # User who created the sales_order
                app_id=app_id  # Associated company
            )
            db_session.add(status_log_entry)
            db_session.flush()

            # Handle file attachment (if any)
            attachments = request.files.getlist('attachment')
            for attachment in attachments:
                if attachment and allowed_file(attachment.filename):
                    filename = secure_filename(attachment.filename)
                    file_path = os.path.join(UPLOAD_FOLDER_SALES_ORDER, filename)
                    attachment.save(file_path)

                    # Create and save each attachment record
                    new_attachment = SalesOrderAttachment(
                        sales_order_id=new_sales_order.id,
                        file_name=filename,
                        file_location=file_path,
                        app_id=app_id
                    )

                    db_session.add(new_attachment)

            db_session.commit()
            # Return JSON success response
            return jsonify({
                'success': True,
                'message': 'Sales Order added successfully!',
                'sales_order_id': sales_order_number
            })

        except ValueError:
            return jsonify({
                'success': False,
                'message': f'An error occurred: Please place at least more than one line item'
            })

        except UniqueConstraint:
            return jsonify({
                'success': False,
                'message': f'The Sale Order number is already in the database. Please reload the page'
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

        # Fetch customers, currencies, inventory items, and UOMs for the dropdowns
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            Vendor.is_one_time == False,
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
        projects = db_session.query(Project).filter_by(app_id=app_id).all()

        # Render the template with the next sales_order number and customers
        return render_template('/sales/new_sales_order.html',
                               currencies=currencies, base_currency_code=base_currency_code,
                               base_currency_id=base_currency_id,
                               base_currency=base_currency, inventory_items=inventory_items, uoms=uoms,
                               customers=customers, modules=modules_data, company=company, role=role,
                               warehouses=warehouses, projects=projects)


@sales_bp.route('/edit_sales_order/<int:sales_order_id>', methods=['GET', 'POST'])
@login_required
def edit_sales_order(sales_order_id):
    from app import UPLOAD_FOLDER_SALES_ORDER

    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
    warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
    sales_order = db_session.query(SalesOrder).options(
        joinedload(SalesOrder.exchange_rate)
    ).filter_by(id=sales_order_id, app_id=app_id).first()

    if request.method == 'POST':

        try:

            form_version = request.form.get('version', type=int)

            locked_sales_order = get_locked_record(db_session, SalesOrder, sales_order_id, form_version)

            if not locked_sales_order:
                return jsonify({
                    'success': False,
                    'message': 'This Sales Order was modified by another user. Please refresh and try again.',
                    'error_type': 'optimistic_lock_failed'
                }), 409  # 409 Conflict is appropriate for version conflicts

            # Now work with the locked record
            sales_order = locked_sales_order

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
                    db_session.flush()
                    customer_id = new_customer.id  # Use new customer's ID
            # Add these lines after extracting customer data
            base_currency_id = int(request.form.get('base_currency_id')) if request.form.get(
                'base_currency_id') else None
            exchange_rate = request.form.get('exchange_rate')

            # Updating general sales_order details
            sales_order.sales_order_number = request.form['sales_order_number']
            sales_order_reference = request.form['sales_order_reference'] or None
            sales_order.sales_order_reference = sales_order_reference
            sales_order.customer_id = customer_id
            sales_order.sales_order_date = datetime.strptime(request.form['sales_order_date'],
                                                             '%Y-%m-%d').date()

            sales_order.expiry_date = datetime.strptime(request.form['expiry_date'],
                                                        '%Y-%m-%d').date() if request.form[
                'expiry_date'] else None

            sales_order.total_amount = float(request.form['total_amount'])

            sales_order_discount_type = request.form['overall_discount_type'] or None
            sales_order_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0.0

            total_tax_amount = request.form['total_tax']

            sales_order_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0

            overall_line_subtotal = float(request.form['overall_line_subtotal'])

            if sales_order_discount_type == "amount":
                calculated_discount_amount = sales_order_discount_value
            else:
                calculated_discount_amount = (overall_line_subtotal * sales_order_discount_value) / 100

            sales_order.calculated_discount_amount = calculated_discount_amount
            sales_order.total_line_subtotal = overall_line_subtotal

            sales_order.total_tax_amount = total_tax_amount

            sales_order.sales_order_tax_rate = sales_order_tax_rate

            sales_order.shipping_cost = float(request.form['shipping_cost']) if request.form[
                'shipping_cost'] else 0

            sales_order.handling_cost = float(request.form['handling_cost']) if request.form[
                'handling_cost'] else 0

            sales_order.currency = request.form['currency']

            # Handle exchange rate for the sales order

            if int(sales_order.currency) != base_currency_id:
                # In edit_sales_order route, replace with:
                exchange_rate_id, error_response = update_transaction_exchange_rate(
                    db_session=db_session,
                    transaction=sales_order,
                    currency_id=int(sales_order.currency),
                    base_currency_id=base_currency_id,
                    exchange_rate=exchange_rate,
                    transaction_date=sales_order.sales_order_date,
                    app_id=app_id,
                    user_id=current_user.id,
                    source_type='sales_order'
                )

                if error_response:
                    return error_response

                sales_order.exchange_rate_id = exchange_rate_id

            sales_order.terms_and_conditions = request.form['terms_and_conditions'] or None
            sales_order.status = OrderStatus.draft
            sales_order.updated_at = datetime.now()

            # Commit updated sales_order
            db_session.flush()

            # Handle sales_order Items

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
            db_session.query(SalesOrderItem).filter_by(sales_order_id=sales_order.id).delete()

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

                # Fetch description for inventory items
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

                # Create updated SalesOrderIte,
                updated_item = SalesOrderItem(
                    sales_order_id=sales_order.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    location_id=warehouse,
                    description=description,
                    quantity=qty,
                    currency=sales_order.currency,
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

            # Commit all updated items to the database
            db_session.flush()

            # Log history entry for sales_order update
            history_entry = SalesOrderHistory(
                sales_order_id=sales_order.id,
                changed_by=current_user.id,  # Store user ID
                change_description="Sales Order Updated",  # Description of the change
                app_id=app_id  # Associated company
            )
            db_session.add(history_entry)
            db_session.flush()

            # Add status log entry during sales_order update
            status_log_entry = SalesOrderStatusLog(
                sales_order_id=sales_order.id,
                status="updated",  # New status after update
                changed_by=current_user.id,  # User who updated the sales_order
                app_id=app_id  # Associated company
            )
            db_session.add(status_log_entry)
            db_session.flush()

            # Handle file attachment (if any)
            attachments = request.files.getlist('attachment')
            for attachment in attachments:
                if attachment and allowed_file(attachment.filename):
                    filename = secure_filename(attachment.filename)
                    file_path = os.path.join(UPLOAD_FOLDER_SALES_ORDER, filename)
                    attachment.save(file_path)

                    # Create and save each attachment record
                    updated_attachment = SalesOrderAttachment(
                        sales_order_id=sales_order.id,
                        file_name=filename,
                        file_location=file_path,
                        app_id=app_id
                    )

                    db_session.add(updated_attachment)
                    db_session.flush()
            # Add this before the final commit
            sales_order.version += 1
            db_session.commit()
            # Return JSON success response

            return jsonify({
                'success': True,
                'message': 'Sales Order updated successfully!',
                'sales_order_id': sales_order.id
            })

        except ValueError:
            return jsonify({
                'success': False,
                'message': f'An error occurred: Please place at least more than one line item'
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

        # Fetch existing sales_order details
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
        projects = db_session.query(Project).filter_by(app_id=app_id).all()

        # Render the template with sales_order details
        return render_template('/sales/edit_sales_order.html', sales_order=sales_order, currencies=currencies,
                               base_currency_id=base_currency_id, base_currency=base_currency,
                               base_currency_code=base_currency_code,
                               inventory_items=inventory_items, uoms=uoms, customers=customers, company=company,
                               role=role, modules=modules_data, module_name="Sales", warehouses=warehouses,
                               projects=projects)


@sales_bp.route('/sales_orders', methods=['GET', 'POST'])
@login_required
def sales_order_management():
    app_id = current_user.app_id
    db_session = Session()

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Base query
        query = db_session.query(SalesOrder).filter_by(app_id=app_id)

        # Process filter options
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        filter_type = request.args.get('filter_type', 'sales_order_date')  # Default: sales_order_date
        status_filter = request.args.get('status')
        filter_applied = bool(start_date or end_date or status_filter)
        # Apply date filters based on filter type
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')

                if filter_type == 'sales_order_date':
                    if start_date:
                        query = query.filter(SalesOrder.sales_order_date >= start_date)
                    if end_date:
                        query = query.filter(SalesOrder.sales_order_date <= end_date)
                elif filter_type == 'expiry_date':
                    if start_date:
                        query = query.filter(SalesOrder.expiry_date >= start_date)
                    if end_date:
                        query = query.filter(SalesOrder.expiry_date <= end_date)
            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')

        # Apply status filter
        if status_filter:
            query = query.filter(SalesOrder.status == status_filter)

        # Check for expired sales orders
        current_date = datetime.now()
        expired_sales_orders = query.filter(
            and_(
                SalesOrder.expiry_date < current_date,
                SalesOrder.status == OrderStatus.approved
            )
        ).all()

        # Mark expired sales orders as expired
        for sales_order in expired_sales_orders:
            sales_order.status = OrderStatus.expired
            db_session.commit()

        # Order by latest created_at
        sales_orders = query.order_by(SalesOrder.created_at.desc()).all()

        # If no sales orders exist, return a message
        if not sales_orders:
            flash('No Sales Orders found. Add a new sales order to get started.', 'info')

        # Calculate dashboard metrics
        total_sales_orders = query.count()
        invoiced_sales_orders = query.filter(SalesOrder.status == OrderStatus.invoiced).count()
        approved_sales_orders = query.filter(SalesOrder.status == OrderStatus.approved).count()
        accepted_sales_orders = query.filter(SalesOrder.status == OrderStatus.received).count()
        rejected_sales_orders = query.filter(SalesOrder.status == OrderStatus.rejected).count()
        expired_sales_orders = query.filter(SalesOrder.status == OrderStatus.expired).count()

        return render_template(
            '/sales/sales_orders.html',
            sales_orders=sales_orders,
            total_sales_orders=total_sales_orders,
            invoiced_sales_orders=invoiced_sales_orders,
            approved_sales_orders=approved_sales_orders,
            accepted_sales_orders=accepted_sales_orders,
            rejected_sales_orders=rejected_sales_orders,
            expired_sales_orders=expired_sales_orders,
            company=company,
            role=role,
            filter_applied=filter_applied,
            modules=modules_data
        )
    except Exception as e:
        flash(f'An error occurred: {str(e)}', 'error')
        logger.error(f'Error is: {e}')
        return redirect(url_for('sales.sales_order_management'))
    finally:
        db_session.close()  # Close the session


@sales_bp.route('/sales_order/<int:sales_order_id>', methods=['GET'])
@login_required
def sales_order_details(sales_order_id):
    app_id = current_user.app_id
    with Session() as db_session:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        sales_order = db_session.query(SalesOrder).filter_by(id=sales_order_id, app_id=app_id).first()

        if not sales_order:
            return jsonify({"error": "Sales Order not found"}), 404

        sales_order_data = {
            "id": sales_order.id,
            "sales_order_number": sales_order.sales_order_number,
            "sales_order_reference": sales_order.sales_order_reference if sales_order.sales_order_reference else None,
            "sales_order_date": sales_order.sales_order_date.strftime("%Y-%m-%d"),
            "expiry_date": sales_order.expiry_date.strftime("%Y-%m-%d") if sales_order.expiry_date else None,
            "customer": {
                "name": sales_order.customer.vendor_name,
                "contact": f"{sales_order.customer.tel_contact}{' | ' + sales_order.customer.email if sales_order.customer.email else ''}",
                "address": sales_order.customer.address or None,
                "city_country": f"{sales_order.customer.city or ''} {sales_order.customer.country or ''}".strip() or None
            } if sales_order.customer else None,
            "currency": sales_order.currencies.user_currency if sales_order.currency else None,
            "total_amount": sales_order.total_amount,
            "status": sales_order.status,
            "subtotal": sales_order.total_line_subtotal,
            "sales_order_notes": [
                {"id": note.id, "content": note.note_content, "type": note.note_type, "created_by": note.user.name,
                 "recipient": note.user_recipient.name if note.user_recipient else "",
                 "created_at": note.created_at} for note in
                sales_order.sales_order_notes],
            "attachments": [{"id": att.id, "file_name": att.file_name, "file_location": att.file_location} for att in
                            sales_order.sales_order_attachments],
            "approvals": [{"id": app.id, "approver": app.approver_id, "status": app.approval_status} for app in
                          sales_order.sales_order_approvals],
            "sales_order_items": [
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
                } for item in sales_order.sales_order_items
            ],
            "terms_and_conditions": sales_order.terms_and_conditions,
            "sales_order_discount": sales_order.calculated_discount_amount,
            "sales_order_tax_rate": sales_order.sales_order_tax_rate,
            "shipping_cost": sales_order.shipping_cost,
            "handling_cost": sales_order.handling_cost,
            "users": [{
                "id": user.id,
                "name": user.name
            } for user in company.users
            ],
            "quotation": [
                {
                    'id': sales_order.quotation.id if sales_order.quotation else None,
                    'quotation_number': sales_order.quotation.quotation_number if sales_order.quotation else None
                }
            ] if sales_order.quotation else [],

            "delivery_notes": [
                {
                    'id': delivery_note.id if delivery_note else None,
                    'delivery_note_number': delivery_note.delivery_number if delivery_note else None
                } for delivery_note in sales_order.delivery_notes
            ] if sales_order.delivery_notes else [],

            "sales_invoices": [
                {
                    'id': sales_invoice.id if sales_invoice else None,
                    'invoice_number': sales_invoice.invoice_number if sales_invoice else None
                } for sales_invoice in sales_order.invoices
            ] if sales_order.invoices else []}

        print(f'data is {sales_order_data}')

    # Return JSON or render template
    if request.args.get('format') == 'json':
        return jsonify(sales_order_data)
    else:
        return render_template('/sales/sales_order_details.html', sales_order=sales_order_data, company=company,
                               modules=modules_data, role=role, module_name="Sales")


@sales_bp.route('/generate_invoice', methods=['POST'])
@login_required
def generate_invoice():
    app_id = current_user.app_id
    db_session = Session()

    try:
        # Extract sales_order_id from the form
        sales_order_id = request.form.get('sales_order_id')

        # Fetch the sales order from the database
        sales_order = db_session.query(SalesOrder).filter_by(id=sales_order_id, app_id=app_id).first()
        if not sales_order:
            return jsonify({
                'success': False,
                'message': 'Sales Order not found.'
            })

        # Fetch the customer associated with the sales order
        customer = db_session.query(Vendor).filter_by(id=sales_order.customer_id, app_id=app_id).first()
        if not customer:
            return jsonify({
                'success': False,
                'message': 'Customer not found.'
            })

        # Generate the next invoice number
        next_invoice_number = generate_next_invoice_number()

        # Create a new sales invoice based on the sales order
        new_invoice = SalesInvoice(
            invoice_number=next_invoice_number,
            invoice_reference=sales_order.sales_order_reference if sales_order.sales_order_reference else sales_order.sales_order_number,
            customer_id=sales_order.customer_id,
            sales_order_id=sales_order_id,
            invoice_date=date.today(),
            due_date=date.today() + timedelta(days=30),  # Example: 30 days from today
            currency=sales_order.currency,
            total_line_subtotal=sales_order.total_line_subtotal,
            total_amount=sales_order.total_amount,
            invoice_discount_type=sales_order.sales_order_discount_type,
            invoice_discount_value=sales_order.sales_order_discount_value,
            calculated_discount_amount=sales_order.calculated_discount_amount,
            total_tax_amount=sales_order.total_tax_amount,
            invoice_tax_rate=sales_order.sales_order_tax_rate,
            shipping_cost=sales_order.shipping_cost,
            handling_cost=sales_order.handling_cost,
            status=InvoiceStatus.draft,
            terms_and_conditions=None,
            created_by=current_user.id,
            app_id=app_id,
            exchange_rate_id=sales_order.exchange_rate_id
        )
        db_session.add(new_invoice)
        db_session.commit()

        # Fetch the line items from the sales order
        sales_order_items = db_session.query(SalesOrderItem).filter_by(sales_order_id=sales_order.id).all()

        # Create invoice items based on the sales order items
        for item in sales_order_items:
            new_invoice_item = SalesInvoiceItem(
                invoice_id=new_invoice.id,
                item_type=item.item_type,
                item_id=item.item_id,
                location_id=item.location_id,
                item_name=item.item_name,
                description=item.description,
                quantity=item.quantity,
                unit_price=item.unit_price,
                total_price=item.total_price,
                uom=item.uom,
                discount_amount=item.discount_amount,
                discount_rate=item.discount_rate,
                tax_amount=item.tax_amount,
                tax_rate=item.tax_rate,
                app_id=app_id
            )
            db_session.add(new_invoice_item)

        sales_order.status = OrderStatus.invoiced
        # Commit all invoice items to the database
        db_session.commit()

        # Log history entry for invoice creation
        history_entry = SalesInvoiceHistory(
            invoice_id=new_invoice.id,
            changed_by=current_user.id,
            change_description="Invoice Generated from Sales Order",
            app_id=app_id
        )
        db_session.add(history_entry)

        # Add status log entry for the new invoice
        status_log_entry = SalesInvoiceStatusLog(
            invoice_id=new_invoice.id,
            status="draft",
            changed_by=current_user.id,
            app_id=app_id
        )
        db_session.add(status_log_entry)

        # Commit history and status log entries
        db_session.commit()

        flash("Invoice generated successfully!", "success")
        # Replace the JSON response with a redirect
        return redirect(url_for('sales.edit_sales_invoice', invoice_id=new_invoice.id))

    except Exception as e:
        db_session.rollback()
        logger.error(f"An error occurred: {str(e)}")
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(url_for('sales_order_details', sales_order_id=sales_order_id))

    finally:
        db_session.close()
