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
    SalesTransaction, SalesPaymentStatus, SalesInvoice, InvoiceStatus, OrderStatus, CustomerCredit, DeliveryNote, \
    DeliveryNoteItem, InventoryItem, SalesOrder, DeliveryReferenceNumber, DeliveryStatus
from services.chart_of_accounts_helpers import group_accounts_by_category
from services.post_to_ledger import post_direct_sale_cogs_to_ledger, post_sales_transaction_to_ledger, \
    bulk_post_sales_transactions, post_customer_credit_to_ledger
from services.post_to_ledger_reversal import reverse_direct_sales_posting, reverse_sales_invoice_posting, \
    reverse_sales_transaction_posting, delete_journal_entries_by_source
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    get_inventory_entries_for_direct_sale, reverse_sales_inventory_entries, allocate_payment, \
    generate_next_delivery_note_number, generate_next_delivery_reference_number
from services.vendors_and_customers import get_or_create_customer_credit_account
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj
from utils_and_helpers.lists import check_list_not_empty
from . import sales_bp

import logging

logger = logging.getLogger(__name__)


@sales_bp.route('/add_delivery_note', methods=['GET', 'POST'])
@login_required
def add_delivery_note():
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

    if request.method == 'POST':

        try:
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
            # Generate the next delivery number
            next_delivery_number = generate_next_delivery_note_number(db_session, app_id)
            # Extract delivery note details
            delivery_number = next_delivery_number

            delivery_date = datetime.strptime(request.form['delivery_date'], '%Y-%m-%d').date()

            shipping_address = request.form['shipping_address']

            delivery_method = request.form['delivery_method'] or None

            additional_notes = request.form.get('additional_notes', '').strip()

            delivered_by_name = request.form.get('delivered_by_name', '').strip()
            received_by_name = request.form.get('received_by_name', '').strip()

            # Extract time inputs
            delivered_by_time = request.form.get('delivered_by_time', '').strip()
            received_by_time = request.form.get('received_by_time', '').strip()

            # Ensure delivered_by_time and received_by_time are properly formatted
            if delivered_by_time:
                try:
                    # Parse time only (no date part)
                    delivered_by_time = datetime.strptime(delivered_by_time, "%H:%M").time()
                except ValueError as e:
                    delivered_by_time = None
            else:
                delivered_by_time = None

            if received_by_time:
                try:
                    # Parse time only (no date part)
                    received_by_time = datetime.strptime(received_by_time, "%H:%M").time()
                except ValueError as e:
                    received_by_time = None
            else:
                received_by_time = None
            # Create new DeliveryNote object

            new_delivery_note = DeliveryNote(
                delivery_number=delivery_number,
                customer_id=customer_id,
                delivery_reference_number=None,
                delivery_date=delivery_date,
                shipping_address=shipping_address,
                delivery_method=delivery_method,
                additional_notes=additional_notes,
                delivered_by_name=delivered_by_name,
                delivered_by_time=delivered_by_time,
                received_by_name=received_by_name,
                received_by_time=received_by_time,
                created_by=current_user.id,
                app_id=app_id
            )
            db_session.add(new_delivery_note)
            db_session.flush()

            # Handle Delivery Note Items
            item_type_list = request.form.getlist('item_type[]')

            item_name_list = request.form.getlist('item_name[]')

            inventory_item_list = request.form.getlist('inventory_item[]')

            description_list = request.form.getlist('item_description[]')

            quantity_delivered_list = request.form.getlist('quantity_delivered[]')

            uom_list = request.form.getlist('uom[]')

            for idx, (
                    item_type, item_name, inventory_item, description, quantity_delivered, uom) in enumerate(
                zip(
                    item_type_list, item_name_list, inventory_item_list, description_list,
                    quantity_delivered_list, uom_list),
                start=1):

                # Handling item ID based on type
                item_id = int(inventory_item) if item_type == "inventory" and inventory_item else None
                item_name = item_name if item_type != "inventory" else None  # If not inventory, store item name instead

                # Fetch description for inventory items
                if item_type == "inventory":
                    inventory_item = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()
                    description = inventory_item.item_description if inventory_item else None

                # Create DeliveryNoteItem
                new_item = DeliveryNoteItem(
                    delivery_note_id=new_delivery_note.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    description=description,
                    quantity_delivered=quantity_delivered,
                    uom=uom,
                    app_id=app_id
                )

                db_session.add(new_item)

            # Commit all items to the database
            db_session.commit()

            return jsonify({
                'success': True,
                'message': 'Delivery Note added successfully!',
                'delivery_note_id': delivery_number
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f"An error occurred: {str(e)}")  # Print the error for debugging
            # Return JSON error response
            flash(f"An error occurred: {str(e)}", "error")
            return jsonify({
                'success': False,
                'message': f'An error occurred: {str(e)}'
            })

        finally:
            db_session.close()

    else:
        # Fetch customers, sales orders, and UOMs for the dropdowns
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            Vendor.is_one_time == False,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()

        sales_orders = db_session.query(SalesOrder).filter_by(app_id=app_id).all()

        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()

        inventory_items = db_session.query(InventoryItemVariationLink).filter_by(app_id=app_id, status="active").all()

        # Render the template with the next delivery number and customers
        return render_template('/sales/new_delivery_note.html',
                               customers=customers, sales_orders=sales_orders, inventory_items=inventory_items,
                               uoms=uoms,
                               modules=modules_data, company=company, role=role)


@sales_bp.route('/generate_delivery_note', methods=['GET', 'POST'])
@login_required
def generate_delivery_note():
    try:
        app_id = current_user.app_id
        sales_order_id = request.form.get('sales_order_id')

        if not sales_order_id:
            flash("Sales Order ID is required", "error")
            return redirect(url_for('sales_order_details'))

        with Session() as db_session:

            sales_order = db_session.query(SalesOrder).filter_by(id=sales_order_id, app_id=app_id).first()
            if not sales_order:
                flash("Sales Order not found!", "error")
                return jsonify({'success': False, 'message': 'Sales Order not found!'})

            customer = db_session.query(Vendor).filter_by(id=sales_order.customer_id, app_id=app_id).first()
            if not customer:
                flash("Customer not found!", "error")
                return jsonify({'success': False, 'message': 'Customer not found!'})
            next_delivery_number = generate_next_delivery_note_number(db_session, app_id)
            existing_reference = db_session.query(DeliveryNote).filter_by(sales_order_id=sales_order_id,
                                                                          app_id=app_id).first()
            delivery_reference_number = existing_reference.delivery_reference_number if existing_reference else generate_next_delivery_reference_number(
                db_session, app_id)

            if not existing_reference:
                new_delivery_reference_number = DeliveryReferenceNumber(
                    delivery_reference_number=delivery_reference_number,
                    app_id=app_id,
                    sales_order_id=sales_order_id,
                    quotation_id=sales_order.quotation.id if sales_order.quotation else None
                )
                db_session.add(new_delivery_reference_number)
                db_session.flush()
                delivery_reference_number = new_delivery_reference_number.id

            new_delivery_note = DeliveryNote(
                delivery_number=next_delivery_number,
                delivery_reference_number=delivery_reference_number,
                customer_id=customer.id,
                delivery_date=datetime.now().date(),
                shipping_address=None,
                delivery_method=None,
                additional_notes=None,
                created_by=current_user.id,
                app_id=app_id,
                status=DeliveryStatus.draft,
                sales_order_id=sales_order.id,
                quotation_id=sales_order.quotation.id if sales_order.quotation else None,
                invoice_id=sales_order.invoices.id if sales_order.invoices else None
            )
            db_session.add(new_delivery_note)
            db_session.flush()

            for sales_order_item in sales_order.sales_order_items:

                if existing_reference:
                    # Start building the query
                    query = db_session.query(func.sum(DeliveryNoteItem.quantity_delivered)).join(DeliveryNote).filter(
                        DeliveryNote.sales_order_id == sales_order_id,
                        DeliveryNote.app_id == app_id
                    )

                    # Check if item_id is available (for inventory items)
                    if sales_order_item.item_id:
                        query = query.filter(DeliveryNoteItem.item_id == sales_order_item.item_id)
                    # If item_id is not available, use item_name (for non-inventory items)
                    else:
                        query = query.filter(DeliveryNoteItem.item_name == sales_order_item.item_name)

                    # Execute the query to get the total delivered
                    total_delivered = query.scalar() or 0

                    remaining_quantity = sales_order_item.quantity - Decimal(str(total_delivered))

                    if remaining_quantity <= 0:
                        continue

                    quantity_to_deliver = remaining_quantity
                else:
                    quantity_to_deliver = sales_order_item.quantity

                new_delivery_note_item = DeliveryNoteItem(
                    delivery_note_id=new_delivery_note.id,
                    item_type=sales_order_item.item_type,
                    item_id=sales_order_item.item_id,
                    item_name=sales_order_item.item_name,
                    description=sales_order_item.description,
                    quantity_delivered=quantity_to_deliver,
                    uom=sales_order_item.uom,
                    app_id=app_id
                )
                db_session.add(new_delivery_note_item)

            db_session.commit()

        flash("Delivery Note created successfully!", "success")
        return redirect(url_for('edit_delivery_note', delivery_note_id=new_delivery_note.id))

    except Exception as e:
        logger.error(f"Error occurred: {e}\n{traceback.format_exc()}")
        flash("An error occurred while generating the delivery note.", "error")
        return jsonify({'success': False, 'message': 'An error occurred while generating the delivery note.'})


@sales_bp.route('/delivery_notes', methods=['GET', 'POST'])
@login_required
def delivery_note_management():
    app_id = current_user.app_id
    db_session = Session()

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Base query
        query = db_session.query(DeliveryNote).filter_by(app_id=app_id)

        # Process filter options
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        filter_type = request.args.get('filter_type', 'delivery_date')  # Default: delivery date
        status_filter = request.args.get('status')
        filter_applied = bool(start_date or end_date or status_filter)
        # Apply date filters based on filter type
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')

                if filter_type == 'delivery_date':
                    if start_date:
                        query = query.filter(DeliveryNote.delivery_date >= start_date)
                    if end_date:
                        query = query.filter(DeliveryNote.delivery_date <= end_date)
                elif filter_type == 'received_date':
                    if start_date:
                        query = query.filter(DeliveryNote.received_by_time >= start_date)
                    if end_date:
                        query = query.filter(DeliveryNote.received_by_time <= end_date)
            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')

        # Apply status filter
        if status_filter:
            query = query.filter(DeliveryNote.status == status_filter)

        # Order by latest delivery date
        delivery_notes = query.order_by(DeliveryNote.delivery_date.desc()).all()

        # If no delivery notes exist, return a message
        if not delivery_notes:
            flash('No Delivery Notes found. Add a new delivery note to get started.', 'info')

        # Calculate dashboard metrics
        total_delivery_notes = query.count()
        draft_delivery_notes = query.filter(DeliveryNote.status == DeliveryStatus.draft).count()
        delivered_delivery_notes = query.filter(DeliveryNote.status == DeliveryStatus.delivered).count()
        canceled_delivery_notes = query.filter(DeliveryNote.status == DeliveryStatus.canceled).count()

        return render_template(
            '/sales/delivery_notes.html',
            delivery_notes=delivery_notes,
            total_delivery_notes=total_delivery_notes,
            draft_delivery_notes=draft_delivery_notes,
            delivered_delivery_notes=delivered_delivery_notes,
            canceled_delivery_notes=canceled_delivery_notes,
            company=company,
            role=role,
            filter_applied=filter_applied,
            modules=modules_data
        )
    except Exception as e:
        flash(f'An error occurred: {str(e)}', 'error')
        logger.error(f'Error is: {e}')
        return redirect(url_for('sales.delivery_note_management'))
    finally:
        db_session.close()  # Close the session


@sales_bp.route('/delivery_note/<int:delivery_note_id>', methods=['GET'])
@login_required
def view_delivery_note(delivery_note_id):
    app_id = current_user.app_id
    with Session() as db_session:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch the delivery note
        delivery_note = db_session.query(DeliveryNote).filter_by(id=delivery_note_id, app_id=app_id).first()

        if not delivery_note:
            return jsonify({"error": "Delivery Note not found"}), 404

        # Prepare delivery note data
        delivery_note_data = {
            "id": delivery_note.id,
            "delivery_number": delivery_note.delivery_number,
            "delivery_date": delivery_note.delivery_date.strftime("%Y-%m-%d"),
            "delivery_reference_number": delivery_note.delivery_reference.delivery_reference_number if delivery_note.delivery_reference else None,
            "sales_order_number": delivery_note.sales_order.sales_order_number if delivery_note.sales_order else None,
            "sales_order_id": delivery_note.sales_order.id if delivery_note.sales_order else None,
            "quotation_number": delivery_note.quotation.quotation_number if delivery_note.quotation else None,
            "quotation_id": delivery_note.quotation.id if delivery_note.quotation else None,
            "customer": {
                "name": delivery_note.customer.vendor_name,
                "contact": f"{delivery_note.customer.tel_contact}{' | ' + delivery_note.customer.email if delivery_note.customer.email else ''}",
                "address": delivery_note.customer.address or None,
                "city_country": f"{delivery_note.customer.city or ''} {delivery_note.customer.country or ''}".strip() or None
            } if delivery_note.customer else None,
            "shipping_address": delivery_note.shipping_address,
            "delivery_method": delivery_note.delivery_method,
            "additional_notes": delivery_note.additional_notes,
            "status": delivery_note.status,
            "delivered_by": {
                "name": delivery_note.delivered_by_name,
                "time": delivery_note.delivered_by_time.strftime(
                    "%Y-%m-%d %H:%M") if delivery_note.delivered_by_time else None

            },
            "received_by": {
                "name": delivery_note.received_by_name,
                "time": delivery_note.received_by_time.strftime(
                    "%Y-%m-%d %H:%M") if delivery_note.received_by_time else None,

            },

            "delivery_items": [
                {
                    "id": item.id,
                    "item_name": item.item_name if item.item_name else item.inventory_item_variation_link.inventory_item.item_name,
                    "description": item.description if item.item_name else item.inventory_item_variation_link.inventory_item.item_description,
                    "quantity": item.quantity_delivered,
                    "uom": item.unit_of_measurement.full_name,
                } for item in delivery_note.delivery_items
            ],
            "created_by": {
                "id": delivery_note.user.id,
                "name": delivery_note.user.name
            },
            "created_at": delivery_note.created_at.strftime("%Y-%m-%d %H:%M"),
            "users": [{
                "id": user.id,
                "name": user.name
            } for user in company.users
            ],
            "delivery_note_notes": [
                {"id": note.id, "content": note.note_content, "type": note.note_type, "created_by": note.user.name,
                 "recipient": note.user_recipient.name if note.user_recipient else "",
                 "created_at": note.created_at} for note in
                delivery_note.delivery_note_notes]

        }
    # Return JSON or render template
    if request.args.get('format') == 'json':
        return jsonify(delivery_note_data)
    else:
        return render_template('/sales/delivery_note_details.html', delivery_note=delivery_note_data, company=company,
                               modules=modules_data, role=role, module_name="Sales")


@sales_bp.route('/cancel_delivery_note', methods=['POST'])
@login_required
def cancel_delivery_note():
    """
    Cancels a delivery note by its ID.
    """
    data = request.get_json(force=True)
    delivery_note_id = data.get("delivery_note_id")
    try:
        if not delivery_note_id:
            flash("Delivery Note ID is required", "error")
            return redirect(url_for('sales.delivery_note_management'))

        with Session() as db_session:
            # Fetch the delivery note from the database
            delivery_note = db_session.query(DeliveryNote).filter_by(id=delivery_note_id).first()
            if not delivery_note:
                flash("Delivery Note not found", "error")
                return redirect(url_for('sales.delivery_note_management'))

            # Update the delivery note status to "cancelled"
            delivery_note.status = DeliveryStatus.canceled  # Assuming "cancelled" is a valid status
            db_session.commit()

            flash(f"Delivery Note {delivery_note.delivery_number} cancelled successfully.", "success")
            return redirect(url_for('sales.delivery_note_management'))  # Replace with actual page

    except Exception as e:
        db_session.rollback()
        flash(f"An error occurred: {str(e)}", "error")
        return redirect(url_for('sales.delivery_note_details', delivery_note_id=delivery_note_id))
