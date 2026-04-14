import os
import traceback
from datetime import datetime, timedelta
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
    QuotationStatus, SalesOrder, OrderStatus, SalesOrderItem, InventoryItem
from services.inventory_helpers import reverse_inventory_entry
from services.post_to_ledger import post_invoice_cogs_to_ledger, post_invoice_to_ledger
from services.post_to_ledger_reversal import reverse_sales_invoice_posting
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    generate_next_invoice_number, get_inventory_entries_for_invoice, reverse_sales_inventory_entries, \
    generate_next_quotation_number, generate_next_sales_order_number, update_transaction_exchange_rate
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj, get_or_create_exchange_rate_for_transaction
from utils_and_helpers.file_utils import allowed_file
from utils_and_helpers.forms import get_locked_record
from utils_and_helpers.lists import check_list_not_empty
from . import sales_bp

import logging

logger = logging.getLogger(__name__)


@sales_bp.route('/add_quotation', methods=['GET', 'POST'])
@login_required
def add_quotation():
    from app import UPLOAD_FOLDER_QUOTATION

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

            # Generate the next quotation number
            next_quotation_number = generate_next_quotation_number()
            quotation_number = next_quotation_number

            # Extract quotation details
            quotation_reference = request.form['quotation_reference'] or None

            project_id = request.form['project'] or None

            quotation_date = datetime.strptime(request.form['quotation_date'], '%Y-%m-%d').date()

            expiry_date = datetime.strptime(request.form['expiry_date'], '%Y-%m-%d').date() if request.form[
                'expiry_date'] else None

            overall_line_subtotal = round(float(request.form['overall_line_subtotal']), 2)

            total_amount = round(float(request.form['total_amount']), 2)

            shipping_cost = round(float(request.form['shipping_cost']), 2)

            handling_cost = round(float(request.form['handling_cost']), 2)

            total_tax_amount = round(float(request.form['total_tax']), 2)

            terms_and_conditions = request.form['terms_and_conditions'] or None

            currency = request.form['currency']

            # Add these lines where you extract other form data
            base_currency_id = int(request.form.get('base_currency_id')) if request.form.get(
                'base_currency_id') else None
            exchange_rate = request.form.get('exchange_rate')

            # Handle quotation general discount and tax

            quotation_discount_type = request.form['overall_discount_type'] or None
            quotation_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0

            quotation_tax_rate = round(float(request.form['overall_tax']), 2) if request.form['overall_tax'] else 0

            if quotation_discount_type == "amount":
                calculated_discount_amount = quotation_discount_value
            else:
                calculated_discount_amount = (overall_line_subtotal * quotation_discount_value) / 100

            # Handle exchange rate for the quotation
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

                # Create exchange rate record
                rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=db_session,
                    action='create',
                    from_currency_id=currency,
                    to_currency_id=base_currency_id,
                    rate_value=exchange_rate_value,
                    rate_date=quotation_date,
                    app_id=app_id,
                    created_by=current_user.id,
                    source_type='quotation',
                    source_id=None,  # Will update after quotation is created
                    currency_exchange_transaction_id=None
                )

                exchange_rate_id = rate_id

            # Create new Quotation object
            new_quotation = Quotation(
                quotation_number=quotation_number,
                quotation_reference=quotation_reference,
                customer_id=customer_id,
                quotation_date=quotation_date,
                expiry_date=expiry_date,
                currency=currency,
                total_line_subtotal=overall_line_subtotal,
                total_amount=total_amount,
                quotation_discount_type=quotation_discount_type,
                quotation_discount_value=quotation_discount_value,
                calculated_discount_amount=calculated_discount_amount,
                total_tax_amount=total_tax_amount,
                quotation_tax_rate=quotation_tax_rate,
                shipping_cost=shipping_cost,
                handling_cost=handling_cost,
                status="draft",
                terms_and_conditions=terms_and_conditions,
                app_id=app_id,
                exchange_rate_id=exchange_rate_id,
                project_id=project_id
            )
            db_session.add(new_quotation)
            db_session.flush()

            # ADD THIS AFTER flush()
            if exchange_rate_id and rate_obj:
                rate_obj.source_id = new_quotation.id
                db_session.add(rate_obj)

            # Handle Quotation Items
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

                # Create QuotationItem
                new_item = QuotationItem(
                    quotation_id=new_quotation.id,
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

            # Log history entry for quotation creation
            history_entry = QuotationHistory(
                quotation_id=new_quotation.id,
                changed_by=current_user.id,  # Store user ID
                change_description="Quotation Created",  # Description of the change
                app_id=app_id  # Associated company
            )
            db_session.add(history_entry)

            # Add status log entry during quotation creation
            status_log_entry = QuotationStatusLog(
                quotation_id=new_quotation.id,
                status="draft",  # Initial status of the quotation
                changed_by=current_user.id,  # User who created the quotation
                app_id=app_id  # Associated company
            )
            db_session.add(status_log_entry)

            # Handle file attachment (if any)
            attachments = request.files.getlist('attachment')
            for attachment in attachments:
                if attachment and allowed_file(attachment.filename):
                    filename = secure_filename(attachment.filename)
                    file_path = os.path.join(UPLOAD_FOLDER_QUOTATION, filename)
                    attachment.save(file_path)

                    # Create and save each attachment record
                    new_attachment = QuotationAttachment(
                        quotation_id=new_quotation.id,
                        file_name=filename,
                        file_location=file_path,
                        app_id=app_id
                    )

                    db_session.add(new_attachment)

            db_session.commit()

            # Return JSON success response
            return jsonify({
                'success': True,
                'message': 'Quotation added successfully!',
                'quotation_number': quotation_number
            })

        except ValueError:
            return jsonify({
                'success': False,
                'message': f'An error occurred: Please place at least more than one line item'
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f"An error occurred: {str(e)}")  # Print the error for debugging
            logger.error(traceback.format_exc())  # This will show you the exact line
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

        # Render the template with the next quotation number and customers
        return render_template('/sales/new_quotation.html',
                               currencies=currencies, base_currency_id=base_currency_id,
                               base_currency_code=base_currency_code, projects=projects,
                               base_currency=base_currency, inventory_items=inventory_items, uoms=uoms,
                               customers=customers, modules=modules_data, company=company, role=role,
                               warehouses=warehouses)


# Edit Quotation Route

@sales_bp.route('/edit_quotation/<int:quotation_id>', methods=['GET', 'POST'])
@login_required
def edit_quotation(quotation_id):
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
    warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

    # Load quotation with exchange_rate relationship
    quotation = db_session.query(Quotation).options(
        joinedload(Quotation.exchange_rate)
    ).filter_by(id=quotation_id, app_id=app_id).first()

    from app import UPLOAD_FOLDER_QUOTATION

    if request.method == 'POST':

        try:

            form_version = request.form.get('version', type=int)

            locked_quotation = get_locked_record(db_session, Quotation, quotation_id, form_version)

            if not locked_quotation:
                return jsonify({
                    'success': False,
                    'message': 'This Quotation was modified by another user. Please refresh and try again.',
                    'error_type': 'optimistic_lock_failed'
                }), 409

            # Now work with the locked record
            quotation = locked_quotation

            # Extract customer data
            base_currency_id = int(request.form.get('base_currency_id')) if request.form.get('base_currency_id') else None
            exchange_rate = request.form.get('exchange_rate')

            check_list_not_empty(request.form.getlist('item_type[]'))

            customer_id = request.form.get('customer_id', '').strip()
            customer_name = request.form.get('customer_name', '').strip()

            if customer_id and customer_id.isdigit():
                customer_id = int(customer_id)
            else:
                existing_customer = db_session.query(Vendor).filter_by(
                    vendor_name=customer_name,
                    is_one_time=True,
                    app_id=app_id
                ).first()

                if existing_customer:
                    customer_id = existing_customer.id
                else:
                    new_customer = Vendor(
                        vendor_name=customer_name,
                        is_one_time=True,
                        vendor_type="Customer",
                        app_id=app_id
                    )
                    db_session.add(new_customer)
                    db_session.flush()  # Changed from commit to flush
                    customer_id = new_customer.id

            # Updating general quotation details
            quotation.quotation_number = request.form['quotation_number']
            quotation_reference = request.form['quotation_reference'] or None
            project_id = request.form.get('project') or None
            if project_id:
                project_id = int(project_id)
            quotation.quotation_reference = quotation_reference
            quotation.project_id = project_id
            quotation.customer_id = customer_id
            quotation.quotation_date = datetime.strptime(request.form['quotation_date'], '%Y-%m-%d').date()
            quotation.expiry_date = datetime.strptime(request.form['expiry_date'], '%Y-%m-%d').date() if request.form['expiry_date'] else None
            quotation.total_amount = float(request.form['total_amount'])
            quotation_discount_type = request.form['overall_discount_type'] or None
            quotation_discount_value = round(float(request.form['overall_discount_value']), 2) if request.form['overall_discount_value'] else 0.0
            quotation.quotation_discount_value = quotation_discount_value
            quotation_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0
            total_tax_amount = float(request.form['total_tax'])
            overall_line_subtotal = float(request.form['overall_line_subtotal'])

            if quotation_discount_type == "amount":
                calculated_discount_amount = quotation_discount_value
            else:
                calculated_discount_amount = (overall_line_subtotal * quotation_discount_value) / 100

            quotation.calculated_discount_amount = calculated_discount_amount
            quotation.total_line_subtotal = overall_line_subtotal
            quotation.quotation_tax_rate = quotation_tax_rate
            quotation.total_tax_amount = total_tax_amount
            quotation.shipping_cost = float(request.form['shipping_cost']) if request.form['shipping_cost'] else 0
            quotation.handling_cost = float(request.form['handling_cost']) if request.form['handling_cost'] else 0
            quotation.currency = request.form['currency']
            quotation.terms_and_conditions = request.form['terms_and_conditions'] or None
            quotation.status = QuotationStatus.draft
            quotation.updated_at = datetime.now()

            # ✅ USE update_transaction_exchange_rate instead of manual handling
            exchange_rate_id, error_response = update_transaction_exchange_rate(
                db_session=db_session,
                transaction=quotation,
                currency_id=int(quotation.currency),
                base_currency_id=base_currency_id,
                exchange_rate=exchange_rate,
                transaction_date=quotation.quotation_date,
                app_id=app_id,
                user_id=current_user.id,
                source_type='quotation'
            )

            if error_response:
                return error_response

            quotation.exchange_rate_id = exchange_rate_id

            db_session.flush()

            # Handle Quotation Items
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

            # Remove old items and re-add updated items
            for item in quotation.quotation_items:
                db_session.delete(item)

            for idx, (item_type, non_inventory_item, inventory_item, warehouse, description, qty, uom, unit_price,
                      discount_amount, tax_amount, subtotal) in enumerate(zip(
                item_type_list, item_name_list, inventory_item_list, warehouse_list, description_list, qty_list,
                uom_list, unit_price_list, discount_list, tax_list, subtotal_list), start=1):

                valid_item_id = int(inventory_item) if item_type == "inventory" and inventory_item else None
                item_id = None if item_type != "inventory" else valid_item_id
                item_name = None if item_type == "inventory" else non_inventory_item

                warehouse = empty_to_none(warehouse)
                description = empty_to_none(description)

                if item_type == "inventory":
                    inventory_item_variation = db_session.query(InventoryItemVariationLink).filter_by(
                        inventory_item_id=item_id,
                        app_id=app_id
                    ).first()
                    description = inventory_item_variation.inventory_item.item_description if inventory_item_variation else None

                discount_amount = float(discount_amount) if discount_amount else 0
                tax_amount = float(tax_amount) if tax_amount else 0

                subtotal_after_tax = float(subtotal) - tax_amount if float(subtotal) != 0 else 1
                subtotal_after_discount = float(subtotal) - tax_amount + discount_amount if float(subtotal) != 0 else 1

                discount_rate = round((discount_amount / subtotal_after_discount) * 100, 2)
                tax_rate = round((tax_amount / subtotal_after_tax) * 100, 2)

                updated_item = QuotationItem(
                    quotation_id=quotation.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    location_id=warehouse,
                    description=description,
                    quantity=qty,
                    currency=quotation.currency,
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

            # Log history entry
            history_entry = QuotationHistory(
                quotation_id=quotation.id,
                changed_by=current_user.id,
                change_description="Quotation Updated",
                app_id=app_id
            )
            db_session.add(history_entry)
            db_session.flush()

            # Add status log entry
            status_log_entry = QuotationStatusLog(
                quotation_id=quotation.id,
                status="updated",
                changed_by=current_user.id,
                app_id=app_id
            )
            db_session.add(status_log_entry)
            db_session.flush()

            # Handle file attachments
            attachments = request.files.getlist('attachment')
            for attachment in attachments:
                if attachment and allowed_file(attachment.filename):
                    filename = secure_filename(attachment.filename)
                    file_path = os.path.join(UPLOAD_FOLDER_QUOTATION, filename)
                    attachment.save(file_path)

                    updated_attachment = QuotationAttachment(
                        quotation_id=quotation.id,
                        file_name=filename,
                        file_location=file_path,
                        app_id=app_id
                    )
                    db_session.add(updated_attachment)
                    db_session.flush()

            # Return JSON success response
            quotation.version += 1
            db_session.commit()

            return jsonify({
                'success': True,
                'message': 'Quotation updated successfully!',
                'quotation_id': quotation.id
            })

        except ValueError as ve:
            db_session.rollback()
            return jsonify({
                'success': False,
                'message': str(ve)
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

        # Fetch existing quotation details
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

        return render_template('/sales/edit_quotation.html',
                               quotation=quotation,
                               currencies=currencies,
                               base_currency_code=base_currency_code,
                               base_currency_id=base_currency_id,
                               base_currency=base_currency,
                               projects=projects,
                               inventory_items=inventory_items,
                               uoms=uoms,
                               customers=customers,
                               company=company,
                               role=role,
                               modules=modules_data,
                               warehouses=warehouses)

@sales_bp.route('/quotation/<int:quotation_id>', methods=['GET'])
@login_required
def quotation_details(quotation_id):
    app_id = current_user.app_id
    with Session() as db_session:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        quotation = db_session.query(Quotation).filter_by(id=quotation_id, app_id=app_id).first()

        if not quotation:
            return jsonify({"error": "Quotation not found"}), 404

        quotation_data = {
            "id": quotation.id,
            "quotation_number": quotation.quotation_number,
            "quotation_date": quotation.quotation_date.strftime("%Y-%m-%d"),
            "expiry_date": quotation.expiry_date.strftime("%Y-%m-%d") if quotation.expiry_date else None,
            "quotation_reference": quotation.quotation_reference if quotation.quotation_reference else None,
            "customer": {
                "name": quotation.customer.vendor_name,
                "contact": f"{quotation.customer.tel_contact}{' | ' + quotation.customer.email if quotation.customer.email else ''}",
                "address": quotation.customer.address or None,
                "city_country": f"{quotation.customer.city or ''} {quotation.customer.country or ''}".strip() or None
            } if quotation.customer else None,
            "currency": quotation.currencies.user_currency if quotation.currency else None,
            "total_amount": quotation.total_amount,
            "status": quotation.status,
            "subtotal": quotation.total_line_subtotal,
            "quotation_notes": [
                {"id": note.id, "content": note.note_content, "type": note.note_type, "created_by": note.user.name,
                 "created_at": note.created_at, "recipient": note.user_recipient.name if note.user_recipient else ""}
                for note in
                quotation.quotation_notes],
            "attachments": [{"id": att.id, "file_name": att.file_name, "file_location": att.file_location} for att in
                            quotation.quotation_attachments],
            "approvals": [{"id": app.id, "approver": app.approver_id, "status": app.approval_status} for app in
                          quotation.approvals],
            "quotation_items": [
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
                } for item in quotation.quotation_items
            ],
            "terms_and_conditions": quotation.terms_and_conditions,
            "quotation_discount": quotation.calculated_discount_amount,
            "quotation_tax_rate": quotation.quotation_tax_rate,
            "shipping_cost": quotation.shipping_cost,
            "handling_cost": quotation.handling_cost,
            "users": [{
                "id": user.id,
                "name": user.name
            } for user in company.users
            ],
            "sales_order": [
                {
                    'id': sales_order.id,
                    'sales_order_number': sales_order.sales_order_number
                } for sales_order in quotation.sales_orders
            ],
            "delivery_notes": [
                {
                    'id': delivery_note.id,
                    'delivery_note_number': delivery_note.delivery_number
                } for delivery_note in quotation.delivery_notes
            ],
            "sales_invoices": [
                {
                    'id': sales_invoice.id,
                    'invoice_number': sales_invoice.invoice_number
                } for sales_invoice in quotation.invoices
            ]
        }

    # Return JSON or render template
    if request.args.get('format') == 'json':
        return jsonify(quotation_data)
    else:
        return render_template('/sales/quotation_details.html', quotation=quotation_data, company=company,
                               modules=modules_data, role=role, module_name="Sales")


@sales_bp.route('/convert_to_sales_order', methods=['POST'])
@login_required
def convert_to_sales_order():
    try:
        quotation_id = request.form.get('quotation_id')

        if not quotation_id:
            flash("Quotation ID is required", "error")
            return redirect(url_for('quotation_details'))

        with Session() as db_session:
            # Fetch the quotation
            quotation = db_session.query(Quotation).filter_by(id=quotation_id).first()
            if not quotation:
                flash("Quotation not found", "error")
                return redirect(url_for('quotation_details'))

            # Change the status of the quotation to "accepted"
            quotation.status = "accepted"
            db_session.commit()

            # Generate a new sales order number
            new_sales_order_number = generate_next_sales_order_number()

            # Calculate the expiry date (e.g., 30 days after the quotation date)
            expiry_date = quotation.quotation_date + timedelta(days=30)

            # Create a new sales order
            sales_order = SalesOrder(
                sales_order_number=new_sales_order_number,
                sales_order_reference=quotation.quotation_reference if quotation.quotation_reference else quotation.quotation_number,
                sales_order_date=datetime.now().date(),
                expiry_date=expiry_date,
                customer_id=quotation.customer_id,
                total_line_subtotal=quotation.total_line_subtotal,
                total_amount=quotation.total_amount,
                sales_order_discount_type=quotation.quotation_discount_type,
                sales_order_discount_value=quotation.quotation_discount_value,
                calculated_discount_amount=quotation.calculated_discount_amount,
                sales_order_tax_rate=quotation.quotation_tax_rate,
                total_tax_amount=quotation.total_tax_amount,
                shipping_cost=quotation.shipping_cost,
                handling_cost=quotation.handling_cost,
                currency=quotation.currency,
                status=OrderStatus.approved,  # Set status to "approved"
                terms_and_conditions=quotation.terms_and_conditions,
                app_id=quotation.app_id,
                quotation_id=quotation.id,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                exchange_rate_id=quotation.exchange_rate_id
            )
            db_session.add(sales_order)

            db_session.commit()

            # Copy items from quotation_items to sales_order_items
            for quotation_item in quotation.quotation_items:
                sales_order_item = SalesOrderItem(
                    sales_order_id=sales_order.id,
                    item_type=quotation_item.item_type,
                    item_id=quotation_item.item_id,
                    item_name=quotation_item.item_name,
                    location_id=quotation_item.location_id,
                    description=quotation_item.description,
                    quantity=quotation_item.quantity,
                    currency=quotation_item.currency,
                    unit_price=quotation_item.unit_price,
                    total_price=quotation_item.total_price,
                    uom=quotation_item.uom,
                    tax_rate=quotation_item.tax_rate,
                    tax_amount=quotation_item.tax_amount,
                    discount_amount=quotation_item.discount_amount,
                    discount_rate=quotation_item.discount_rate,
                    app_id=quotation_item.app_id,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                db_session.add(sales_order_item)

            db_session.commit()

            flash("Quotation converted to Sales Order successfully!", "success")
            return redirect(
                url_for('sales_order_details', sales_order_id=sales_order.id))  # Change to actual page after conversion

    except Exception as e:
        db_session.rollback()
        flash(f"An error occurred: {str(e)}", "error")
        return redirect(url_for('quotation_details'))  # Change to actual error page


@sales_bp.route('/quotations', methods=['GET', 'POST'])
@login_required
def quotation_management():
    app_id = current_user.app_id
    db_session = Session()

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Base query
        query = db_session.query(Quotation).filter_by(app_id=app_id)

        # Process filter options
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        filter_type = request.args.get('filter_type', 'quotation_date')  # Default: quotation_date
        status_filter = request.args.get('status')
        filter_applied = bool(start_date or end_date or status_filter)
        # Apply date filters based on filter type
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')

                if filter_type == 'quotation_date':
                    if start_date:
                        query = query.filter(Quotation.quotation_date >= start_date)
                    if end_date:
                        query = query.filter(Quotation.quotation_date <= end_date)
                elif filter_type == 'expiry_date':
                    if start_date:
                        query = query.filter(Quotation.expiry_date >= start_date)
                    if end_date:
                        query = query.filter(Quotation.expiry_date <= end_date)
            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')

        # Apply status filter
        if status_filter:
            query = query.filter(Quotation.status == status_filter)

        # Check for expired quotations
        current_date = datetime.now()
        expired_quotations = query.filter(
            and_(
                Quotation.expiry_date < current_date,
                Quotation.status == QuotationStatus.approved
            )
        ).all()

        # Mark expired quotations as expired
        for quotation in expired_quotations:
            quotation.status = QuotationStatus.expired
            db_session.commit()

        # Order by latest created_at
        quotations = query.order_by(Quotation.quotation_date.desc()).all()

        # If no quotations exist, return a message
        if not quotations:
            flash('No quotations found. Add a new quotation to get started.', 'info')

        # Calculate dashboard metrics
        total_quotations = query.count()
        approved_quotations = query.filter(Quotation.status == QuotationStatus.approved).count()
        accepted_quotations = query.filter(Quotation.status == QuotationStatus.accepted).count()
        rejected_quotations = query.filter(Quotation.status == QuotationStatus.rejected).count()
        expired_quotations = query.filter(Quotation.status == QuotationStatus.expired).count()

        return render_template(
            '/sales/quotations.html',
            quotations=quotations,
            total_quotations=total_quotations,
            approved_quotations=approved_quotations,
            accepted_quotations=accepted_quotations,
            rejected_quotations=rejected_quotations,
            expired_quotations=expired_quotations,
            company=company,
            role=role,
            filter_applied=filter_applied,
            modules=modules_data
        )
    except Exception as e:
        flash(f'An error occurred: {str(e)}', 'error')
        logger.error(f'Error is: {e}\n{traceback.format_exc()}')
        return redirect(url_for('sales.quotation_management'))
    finally:
        db_session.close()  # Close the session
