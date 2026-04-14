import traceback
from datetime import datetime, date
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
    InvoiceStatus, JournalEntry, PaymentReceipt
from services.chart_of_accounts_helpers import group_accounts_by_category
from services.inventory_helpers import reverse_inventory_entry
from services.post_to_ledger import post_invoice_cogs_to_ledger, post_invoice_to_ledger
from services.post_to_ledger_reversal import reverse_sales_invoice_posting
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    generate_next_invoice_number, get_inventory_entries_for_invoice, reverse_sales_inventory_entries, \
    update_transaction_exchange_rate, generate_next_payment_receipt_number
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj, get_or_create_exchange_rate_for_transaction
from utils_and_helpers.lists import check_list_not_empty
from . import sales_bp

import logging

logger = logging.getLogger(__name__)


@sales_bp.route('/add_payment_receipt', methods=['GET', 'POST'])
@login_required
def add_payment_receipt():
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
                    db_session.commit()
                    customer_id = new_customer.id  # Use new customer's ID

            # Extract sales_invoice details

            payment_receipt_number = request.form['payment_receipt_number']

            payment_date = datetime.strptime(request.form['receipt_date'], '%Y-%m-%d').date()

            currency = request.form['currency']

            received_by_name = request.form['received_by'] or None

            amount_received_str = request.form['amount_received']
            amount_received_str = amount_received_str.replace(',', '')  # Remove commas
            amount_received = round(float(amount_received_str), 2)

            balance_due_str = request.form['balance_due']
            balance_due_str = balance_due_str.replace(',', '')  # Remove co
            balance_due = round(float(balance_due_str), 2)

            payment_mode = request.form['payment_mode'] or None

            reference_number = request.form['reference_number'] or None

            payment_description = request.form['payment_description'] or None

            notes = request.form.get('notes', '').strip() or None

            issue_by_name = request.form['issued_by'] or None

            # Create new invoice object

            new_payment_receipt = PaymentReceipt(
                payment_receipt_number=payment_receipt_number,
                sales_transaction_id=None,
                sales_order_id=None,
                quotation_id=None,
                customer_id=customer_id,
                received_by_name=received_by_name,
                amount_received=amount_received,
                balance_due=balance_due,
                payment_date=payment_date,
                payment_mode=payment_mode,
                reference_number=reference_number,
                currency_id=currency,
                description=payment_description,
                notes=notes,
                status="draft",
                created_by=current_user.id,
                issued_by_name=issue_by_name,
                app_id=app_id
            )
            db_session.add(new_payment_receipt)
            db_session.commit()

            # Return JSON success response
            return jsonify({
                'success': True,
                'message': 'Payment Receipt added successfully!',
                'invoice_id': new_payment_receipt.id
            })



        except Exception as e:
            logger.error(f'Error occured: {e}\n{traceback.format_exc()}')
            db_session.rollback()
            # Return JSON error response
            return jsonify({
                'success': False,
                'message': f'An error occurred: {str(e)}'
            })

        finally:
            db_session.close()

    else:

        # Generate the next invoice number
        next_payment_receipt_number = generate_next_payment_receipt_number()

        # Fetch customers, currencies, inventory items, and UOMs for the dropdowns
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()

        # Render the template with the next invoice number and customers
        return render_template('/sales/new_payment_receipt.html', payment_receipt_number=next_payment_receipt_number,
                               currencies=currencies, customers=customers, payment_modes=payment_modes,
                               modules=modules_data, company=company, role=role)


@sales_bp.route('/edit_payment_receipt/<int:receipt_id>', methods=['GET', 'POST'])
@login_required
def edit_payment_receipt(receipt_id):
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

    # Fetch the payment receipt by ID and app_id
    receipt = db_session.query(PaymentReceipt).filter_by(id=receipt_id, app_id=app_id).first()

    if not receipt:
        flash("Payment receipt not found.", "error")
        db_session.close()  # Close the session before redirecting
        return redirect(url_for('payment_receipts'))

    if request.method == 'POST':

        try:
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

            # Update payment receipt fields
            receipt.payment_receipt_number = request.form['payment_receipt_number']
            receipt.customer_id = customer_id
            receipt.payment_date = datetime.strptime(request.form['receipt_date'], '%Y-%m-%d').date()
            receipt.currency_id = request.form['currency']

            receipt.received_by_name = request.form['received_by'] or None
            receipt.amount_received = round(float(request.form['amount_received'].replace(',', '')), 2)

            balance_due_str = request.form['balance_due']
            balance_due_str = balance_due_str.replace(',', '')  # Remove co
            balance_due = round(float(balance_due_str), 2)
            receipt.balance_due = balance_due
            receipt.payment_mode = request.form['payment_mode'] or None
            receipt.reference_number = request.form['reference_number'] or None
            receipt.description = request.form['payment_description'] or None
            receipt.issued_by_name = request.form['issued_by'] or None
            notes = request.form.get('notes', '').strip() or None
            receipt.notes = notes
            # Commit updated payment receipt
            db_session.commit()
            return jsonify({
                'success': True,
                'message': 'Payment receipt updated successfully!',
                'receipt_id': receipt.id
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

        # Fetch existing payment receipt details
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()

        return render_template(
            '/sales/edit_payment_receipt.html',
            receipt=receipt,
            customers=customers,
            currencies=currencies,
            payment_modes=payment_modes,
            modules=modules_data,
            company=company,
            role=role
        )


@sales_bp.route('/approve_payment_receipt/<int:receipt_id>', methods=['POST'])
@login_required
def approve_payment_receipt(receipt_id):
    """
    Approves a payment receipt by its ID and updates its status to "paid".
    """
    db_session = Session()
    try:
        # Fetch the payment receipt from the database
        receipt = db_session.query(PaymentReceipt).filter_by(id=receipt_id).first()
        if not receipt:
            flash('Payment receipt not found.', 'error')
            return redirect(url_for('payment_receipt_details', receipt_id=receipt_id))

        # Update the payment receipt status to "paid"
        receipt.status = "paid"  # Assuming "status" is a field in the PaymentReceipt model
        db_session.commit()

        flash('Payment receipt approved successfully!', 'success')
    except Exception as e:
        # Handle any errors
        db_session.rollback()
        flash(f'An error occurred: {str(e)}', 'error')
    finally:
        db_session.close()

    return redirect(url_for('sales.payment_receipt_details', receipt_id=receipt_id))


@sales_bp.route('/payment_receipts')
@login_required
def payment_receipts():
    app_id = current_user.app_id
    db_session = Session()

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Base query
        query = db_session.query(PaymentReceipt).filter_by(app_id=app_id)

        # Process filter options
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        status_filter = request.args.get('status')
        filter_applied = bool(start_date or end_date or status_filter)
        # Apply date filters
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')

                if start_date:
                    query = query.filter(PaymentReceipt.payment_date >= start_date)
                if end_date:
                    query = query.filter(PaymentReceipt.payment_date <= end_date)
            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')

        # Apply status filter
        if status_filter:
            query = query.filter(PaymentReceipt.status == status_filter)

        # Order by latest created_at
        payment_receipts = query.order_by(PaymentReceipt.created_at.desc()).all()

        # If no payment receipts exist, return a message
        if not payment_receipts:
            flash('No payment receipts found. Add a new receipt to get started.', 'info')

        # Calculate dashboard metrics
        total_payment_receipts = query.count()
        draft_payment_receipts = query.filter(PaymentReceipt.status == 'draft').count()
        completed_payment_receipts = query.filter(PaymentReceipt.status == 'paid').count()
        canceled_payment_receipts = query.filter(PaymentReceipt.status.in_(['cancelled', 'refunded', 'failed'])).count()

        return render_template(
            '/sales/payment_receipts.html',
            payment_receipts=payment_receipts,
            total_payment_receipts=total_payment_receipts,
            draft_payment_receipts=draft_payment_receipts,
            completed_payment_receipts=completed_payment_receipts,
            canceled_payment_receipts=canceled_payment_receipts,
            company=company,
            role=role,
            filter_applied=filter_applied,
            modules=modules_data
        )
    except Exception as e:
        flash(f'An error occurred: {str(e)}', 'error')
        print(f'Error is: {e}')
        return redirect(url_for('sales.payment_receipts'))
    finally:
        db_session.close()  # Close the session


@sales_bp.route('/payment_receipt/<int:receipt_id>', methods=['GET'])
@login_required
def payment_receipt_details(receipt_id):
    app_id = current_user.app_id
    with Session() as db_session:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Fetch the payment receipt by ID and app_id
        receipt = db_session.query(PaymentReceipt).filter_by(id=receipt_id, app_id=app_id).first()

        if not receipt:
            return jsonify({"error": "Payment receipt not found"}), 404

        # Prepare payment receipt data
        receipt_data = {
            "id": receipt.id,
            "receipt_number": receipt.payment_receipt_number,
            "payment_date": receipt.payment_date.strftime("%Y-%m-%d"),
            "customer": {
                "name": receipt.customer.vendor_name,
                "contact": f"{receipt.customer.tel_contact}{' | ' + receipt.customer.email if receipt.customer.email else ''}",
                "address": receipt.customer.address or None,
                "city_country": f"{receipt.customer.city or ''} {receipt.customer.country or ''}".strip() or None
            } if receipt.customer else None,
            "currency": receipt.currency.user_currency if receipt.currency else None,
            "amount_received": float(receipt.amount_received),
            "balance_due": float(receipt.balance_due),
            "payment_mode": receipt.payment_modes.payment_mode if receipt.payment_modes else None,
            "reference_number": receipt.reference_number,
            "description": receipt.description,
            "notes": receipt.notes,
            "status": receipt.status,
            "created_by": receipt.user.name if receipt.user else None,
            "issued_by_name": receipt.issued_by_name,
            "received_by_name": receipt.received_by_name,
            "sales_transaction": {
                "id": receipt.sales_transaction.id,
                "invoice_number": receipt.sales_transaction.invoice.invoice_number if receipt.sales_transaction.invoice else None,
                "amount_paid": float(receipt.sales_transaction.amount_paid) if receipt.sales_transaction else None,
                "payment_status": receipt.sales_transaction.payment_status if receipt.sales_transaction else None
            } if receipt.sales_transaction else None,
            "users": [{
                "id": user.id,
                "name": user.name
            } for user in company.users
            ],
            "sales_order": [
                {
                    'id': receipt.sales_order.id,
                    'sales_order_number': receipt.sales_order.sales_order_number
                } if receipt.sales_order else None
            ],

            "quotation": [
                {
                    'id': receipt.quotation.id,
                    'quotation_number': receipt.quotation.quotation_number
                } if receipt.quotation else None
            ],
            "invoice": [
                {
                    'id': receipt.sales_transaction.invoice.id,
                    'invoice_number': receipt.sales_transaction.invoice.invoice_number
                } if receipt.sales_transaction else None
            ],

            "created_at": receipt.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": receipt.updated_at.strftime("%Y-%m-%d %H:%M:%S") if receipt.updated_at else None
        }

        print(f'Payment receipt data is {receipt_data}')

    # Return JSON or render template
    if request.args.get('format') == 'json':
        return jsonify(receipt_data)
    else:
        return render_template('/sales/payment_receipt_details.html', receipt=receipt_data, company=company,
                               modules=modules_data, role=role, module_name="Sales")


@sales_bp.route('/delete_payment_receipt/<int:receipt_id>', methods=['POST'])
@login_required
def delete_payment_receipt(receipt_id):
    app_id = current_user.app_id
    db_session = Session()
    receipt = db_session.query(PaymentReceipt).filter_by(id=receipt_id, app_id=app_id).first()

    try:
        # Delete the receipt
        db_session.delete(receipt)
        db_session.commit()
        flash('Receipt deleted successfully!', 'success')
    except Exception as e:
        db_session.rollback()
        flash(f'An error occurred: {str(e)}', 'error')
    finally:
        db_session.close()

    return redirect(url_for('sales.receipt_management'))


@sales_bp.route('/cancel_payment_receipt/<int:receipt_id>', methods=['POST'])
@login_required
def cancel_payment_receipt(receipt_id):
    """
    Cancels a payment receipt by its ID.
    """

    try:
        if not receipt_id:
            flash("Receipt ID is required", "error")
            return redirect(url_for('sales.payment_receipts'))

        with Session() as db_session:
            # Fetch the payment receipt from the database
            receipt = db_session.query(PaymentReceipt).filter_by(id=receipt_id).first()
            if not receipt:
                flash("Payment Receipt not found", "error")
                return redirect(url_for('sales.payment_receipts'))

            # Update the receipt status to "canceled"
            receipt.status = 'cancelled'
            db_session.commit()

            flash(f"Payment Receipt {receipt.payment_receipt_number} canceled successfully.", "success")
            return redirect(url_for('sales.payment_receipts'))  # Redirect to the list of receipts

    except Exception as e:
        db_session.rollback()
        flash(f"An error occurred: {str(e)}", "error")
        return redirect(url_for('sales.receipt_details', receipt_id=receipt_id))  # Redirect to receipt details


@sales_bp.route('/generate_payment_receipt', methods=['POST'])
@login_required
def generate_payment_receipt():
    app_id = current_user.app_id
    db_session = Session()

    try:
        # Extract transaction_id, quotation_id, and sales_order_id from the form
        transaction_id = request.form.get('transaction_id')
        quotation_id = request.form.get('quotation_id') or None
        sales_order_id = request.form.get('sales_order_id') or None
        transaction_type = request.form.get('transaction_type') or None

        if transaction_type == "direct_sale":
            transaction = db_session.query(DirectSalesTransaction).filter_by(id=transaction_id, app_id=app_id).first()
            description = f'Payment for {transaction.direct_sale_number} {"Sale Reference: " + transaction.sale_reference if transaction.sale_reference else ""}'
            balance_due = Decimal(transaction.total_amount) - Decimal(transaction.amount_paid)
        else:
            # Fetch the transaction from the database
            transaction = db_session.query(SalesTransaction).filter_by(id=transaction_id, app_id=app_id).first()
            description = None
            balance_due = Decimal(transaction.invoice.total_amount) - Decimal(transaction.amount_paid)

        if not transaction:
            return jsonify({
                'success': False,
                'message': 'Transaction not found.'
            })

        # Fetch the customer associated with the transaction
        customer = db_session.query(Vendor).filter_by(id=transaction.customer_id, app_id=app_id).first()
        if not customer:
            return jsonify({
                'success': False,
                'message': 'Customer not found.'
            })

        # Generate the next payment receipt number
        next_payment_receipt_number = generate_next_payment_receipt_number(db_session, app_id)

        # Create a new payment receipt based on the transaction
        new_payment_receipt = PaymentReceipt(
            payment_receipt_number=next_payment_receipt_number,
            sales_transaction_id=transaction_id,
            sales_order_id=sales_order_id,
            quotation_id=quotation_id,
            customer_id=transaction.customer_id,
            received_by_name=None,
            amount_received=transaction.amount_paid,
            balance_due=balance_due,
            payment_date=date.today(),
            payment_mode=None,
            reference_number=None,
            currency_id=transaction.currency_id,
            description=description,
            status="draft",
            created_by=current_user.id,
            issued_by_name=None,
            app_id=app_id
        )
        db_session.add(new_payment_receipt)
        db_session.commit()

        # Redirect to the edit_payment_receipt page with the new payment receipt ID
        return redirect(url_for('sales.edit_payment_receipt', receipt_id=new_payment_receipt.id))

    except Exception as e:
        db_session.rollback()
        logger.error(f"An error occurred: {str(e)}\n{traceback.format_exc()}")
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(request.referrer)

    finally:
        db_session.close()
