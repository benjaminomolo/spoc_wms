import json
import logging
import traceback
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from flask import jsonify, request, flash, redirect, url_for, render_template
from flask_login import login_required, current_user
from sqlalchemy import func, or_, literal, and_

from ai import get_base_currency
from db import Session
from models import DirectSalesTransaction, SalesInvoice, SalesTransaction, SalesPaymentStatus, InvoiceStatus, \
    OrderStatus, CustomerCredit, CreditApplication, Currency, Quotation, Company, Vendor, PaymentMode, ChartOfAccounts, \
    Module, PaymentAllocation, BulkPayment, Project, ExchangeRate
from services.chart_of_accounts_helpers import group_accounts_by_category, get_system_account, get_all_system_accounts
from services.post_to_ledger import post_sales_transaction_to_ledger, post_customer_credit_to_ledger, \
    post_credit_application_to_ledger, post_overpayment_write_off_to_ledger, post_payment_receipt_to_ledger, \
    post_customer_credit_to_invoice
from services.post_to_ledger_reversal import delete_journal_entries_by_source
from services.sales_helpers import suggest_next_direct_sale_reference, suggest_next_invoice_reference, \
    allocate_direct_sale_payment, allocate_payment, suggest_next_quotation_reference, \
    suggest_next_sales_order_reference, update_invoice_status, get_customer_invoices_with_balances, \
    generate_payment_receipt_number
from services.vendors_and_customers import get_or_create_customer_credit_account, get_or_create_write_off_account, \
    apply_existing_credits_to_invoice, create_credit_from_overpayment, delete_credit_applications
from utils_and_helpers.amounts_utils import format_currency
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj, get_or_create_exchange_rate_for_transaction
from utils_and_helpers.forms import validate_required_fields, get_int_or_none

from . import sales_bp
from .customer_credits import manage_customer_credits, update_credit_status

logger = logging.getLogger(__name__)


@sales_bp.route('/api/suggest_direct_sale_reference')
@login_required
def suggest_direct_sale_reference():
    """
    API endpoint to suggest the next direct sale reference based on the most recent reference.
    """
    with Session() as db_session:
        suggested_ref = suggest_next_direct_sale_reference(db_session)

    return jsonify({'suggested_reference': suggested_ref})


@sales_bp.route('/api/check_direct_sale_reference')
@login_required
def check_direct_sale_reference():
    """
    API endpoint to check if a direct sale reference already exists.
    """
    reference = request.args.get('reference', type=str)
    exists = False

    if reference:
        with Session() as db_session:
            exists = db_session.query(DirectSalesTransaction).filter(
                DirectSalesTransaction.app_id == current_user.app_id,
                func.lower(DirectSalesTransaction.sale_reference) == reference.lower()
            ).count() > 0

    return jsonify({'exists': exists})


@sales_bp.route('/api/suggest_invoice_reference')
@login_required
def suggest_invoice_reference():
    """
    API endpoint to suggest the next invoice reference based on the most recent reference.
    """
    with Session() as db_session:
        suggested_ref = suggest_next_invoice_reference(db_session)

    return jsonify({'suggested_reference': suggested_ref})


@sales_bp.route('/api/check_invoice_reference')
@login_required
def check_invoice_reference():
    """
    API endpoint to check if an invoice reference already exists.
    """
    reference = request.args.get('reference', type=str)
    exists = False

    if reference:
        with Session() as db_session:
            exists = db_session.query(SalesInvoice).filter(
                SalesInvoice.app_id == current_user.app_id,
                func.lower(SalesInvoice.invoice_reference) == reference.lower()
            ).count() > 0

    return jsonify({'exists': exists})



@sales_bp.route('/api/record_payment', methods=['POST'])
@login_required
def record_payment():
    """
    Record a payment against a single invoice.
    Handles both cash payments and credit applications.
    """
    db_session = Session()
    try:
        app_id = current_user.app_id
        transaction_type = request.form.get('transaction_type')

        if transaction_type == "direct_sale":
            # Handle direct sale payments separately
            pass
        else:
            # ===== GET FORM DATA =====
            invoice_id = get_int_or_none(request.form.get('invoiceId'))
            amount = Decimal(str(request.form.get('amount', 0)))
            payment_date = datetime.strptime(request.form.get('paymentDate'), '%Y-%m-%d')
            payment_type = request.form.get('paymentType')  # 'cash' or 'credit'
            reference = request.form.get('reference') or None
            payment_method = request.form.get('paymentMethod')
            payment_method = int(payment_method) if payment_method and payment_method.strip().isdigit() else None
            base_currency_id = get_int_or_none(request.form.get('base_currency_id'))

            # Payment currency (may differ from invoice currency)
            payment_currency = request.form.get('payment_currency')

            # Handle currency based on payment type
            if payment_type == 'credit':
                # For credit payments, always use invoice currency
                payment_currency_id = int(request.form.get('invoice_currency_id'))
            else:
                # For cash payments, use selected currency (required)
                payment_currency_id = int(payment_currency)

            # Exchange rate if payment currency != base currency
            exchange_rate = request.form.get('exchange_rate')
            exchange_rate_value = float(exchange_rate) if exchange_rate else None

            # Overpayment handling (for cash payments only)
            overpayment_handling = request.form.get('overpaymentHandling', 'credit')

            # Get funding account for cash payments
            asset_account_id = request.form.get('fundingAccount')

            # Get customer credit account for credit payments
            customer_credit_account_str = request.form.get('creditSettlementAccount')
            customer_credit_account = int(
                customer_credit_account_str) if customer_credit_account_str and customer_credit_account_str.strip() else None

            # Get selected credits data (for credit payments)
            selected_credits_json = request.form.get('selectedCredits')
            selected_credits = json.loads(selected_credits_json) if selected_credits_json else []

            logger.info(f'Record payment form data: {request.form}')

            # ===== VALIDATION =====
            # Fetch the invoice
            invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id).first()
            if not invoice:
                flash('Invoice not found.', 'error')
                return redirect(url_for('sales.invoice_details', invoice_id=invoice_id))

            # Validate based on payment type
            if payment_type == 'cash':
                if not asset_account_id or asset_account_id.strip() == '':
                    flash('Payment receiving account is required for cash payments.', 'error')
                    return redirect(url_for('sales.invoice_details', invoice_id=invoice_id))
                payment_account = int(asset_account_id)
            else:  # credit payment
                if not customer_credit_account:
                    flash('Customer credit account is required for credit payments.', 'error')
                    return redirect(url_for('sales.invoice_details', invoice_id=invoice_id))
                payment_account = customer_credit_account

            # Validate amount
            if amount <= 0:
                flash('Payment amount must be greater than 0.', 'error')
                return redirect(url_for('sales.invoice_details', invoice_id=invoice_id))

            # Calculate remaining balance
            total_paid = db_session.query(func.sum(SalesTransaction.amount_paid)).filter(
                SalesTransaction.invoice_id == invoice_id,
                SalesTransaction.payment_status != SalesPaymentStatus.cancelled
            ).scalar() or Decimal('0.00')
            remaining_balance = invoice.total_amount - total_paid

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

            # ===== CREATE EXCHANGE RATE IF NEEDED =====
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

            # ===== HANDLE BASED ON PAYMENT TYPE =====
            if payment_type == 'cash':
                # ===== CASH PAYMENT =====

                # Calculate amounts in invoice currency
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

                # Handle overpayment
                overpayment_amount = Decimal('0.00')
                allocated_amount = amount_in_invoice

                if amount_in_invoice > remaining_balance:
                    overpayment_amount = amount_in_invoice - remaining_balance
                    allocated_amount = remaining_balance
                    overpayment_amount = overpayment_amount.quantize(Decimal('0.01'))

                # Create sales transaction (in invoice currency)
                new_payment = SalesTransaction(
                    invoice_id=invoice_id,
                    customer_id=invoice.customer_id,
                    payment_date=payment_date,
                    amount_paid=amount_in_invoice,
                    currency_id=invoice.currency,
                    reference_number=reference,
                    is_posted_to_ledger=True,
                    payment_status=SalesPaymentStatus.partial,
                    created_by=current_user.id,
                    app_id=app_id,
                    bulk_payment_id=None  # No bulk payment for single invoice
                )
                db_session.add(new_payment)
                db_session.flush()

                # Create payment allocation
                payment_allocation = allocate_payment(
                    sale_transaction_id=new_payment.id,
                    invoice_id=invoice_id,
                    payment_date=payment_date,
                    payment_amount=amount_in_invoice,
                    remaining_balance=remaining_balance,
                    db_session=db_session,
                    payment_mode=payment_method,
                    total_tax_amount=invoice.total_tax_amount,
                    payment_account=payment_account,
                    tax_payable_account_id=None,
                    credit_sale_account=None,
                    reference=reference,
                    overpayment_amount=overpayment_amount,
                    write_off_account_id=write_off_system_account_id if overpayment_handling == 'write_off' else None,
                    payment_type='cash',
                    is_posted_to_ledger=True,
                    exchange_rate_id=exchange_rate_id,
                    base_currency_id=base_currency_id,
                    invoice=invoice
                )
                db_session.add(payment_allocation)
                db_session.flush()

                # Update payment status
                new_total_paid = total_paid + allocated_amount
                if new_total_paid >= invoice.total_amount - Decimal('0.01'):
                    new_payment.payment_status = SalesPaymentStatus.paid
                    invoice.status = InvoiceStatus.paid
                else:
                    new_payment.payment_status = SalesPaymentStatus.partial
                    invoice.status = InvoiceStatus.partially_paid

                # Handle overpayment (create credit or write-off)
                if overpayment_amount > 0:
                    if overpayment_handling == 'write_off':
                        # Handle as write-off
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
                            return redirect(url_for('sales.invoice_details', invoice_id=invoice_id))
                        flash_message = f'Payment recorded successfully! {overpayment_amount:.2f} written off as income.'
                    else:
                        # Create customer credit
                        success, msg, credit = manage_customer_credits(
                            db_session=db_session,
                            source=payment_allocation,
                            action='create',
                            amount=overpayment_amount,
                            currency_id=invoice.currency,
                            reason='overpayment',
                            exchange_rate_id=exchange_rate_id,
                            reference=reference,
                            customer_id=invoice.customer_id,
                            funding_account=payment_account,
                            payment_allocation=payment_allocation,
                            current_user=current_user,
                            project_id=invoice.project_id
                        )
                        if not success:
                            flash(f'Error creating credit: {msg}', 'error')
                            return redirect(url_for('sales.invoice_details', invoice_id=invoice_id))
                        flash_message = f'Payment recorded successfully! {overpayment_amount:.2f} overpayment - Credit #{credit.id} created.'
                else:
                    flash_message = 'Payment recorded successfully!'

                # Build allocation details for ledger
                allocation_details = [{
                    'allocation': payment_allocation,
                    'invoice': invoice,
                    'amount': float(allocated_amount),
                    'amount_base': amount_in_base,
                    'amount_payment': float(amount),
                    'invoice_rate': float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1
                }]

                # Post to ledger using post_payment_receipt_to_ledger (works without bulk payment)
                success, message = post_payment_receipt_to_ledger(
                    db_session=db_session,
                    receipt=None,  # No bulk payment for single invoice
                    allocations=allocation_details,
                    payment_account_id=payment_account,
                    suspense_account_id=suspense_account_id,
                    fx_gain_loss_account_id=fx_gain_loss_account_id,
                    current_user=current_user,
                    overpayment_action=overpayment_handling,
                    customer_credit_account_id=customer_credit_system_account_id,
                    write_off_account_id=write_off_system_account_id if overpayment_handling == 'write_off' else None,
                    status='Posted',
                    project_id=invoice.project_id,
                    exchange_rate_id=exchange_rate_id,
                    base_currency_id=base_currency_id,
                    payment_currency_id=payment_currency_id,
                    overpayment_amount=overpayment_amount
                )

                if not success:
                    raise Exception(f"Failed to post to ledger: {message}")

            else:  # payment_type == 'credit'
                # ===== CREDIT PAYMENT (Apply existing credits) =====
                if not selected_credits:
                    flash('Please select credits to apply', 'error')
                    return redirect(url_for('sales.invoice_details', invoice_id=invoice_id))

                total_applied = Decimal('0.00')
                transactions = []
                allocation_details = []

                for credit_data in selected_credits:
                    credit_id = credit_data.get('id')
                    amount_in_credit = Decimal(str(credit_data.get('amount', 0)))
                    converted_amount = Decimal(str(credit_data.get('converted_amount', 0)))

                    if amount_in_credit <= 0:
                        continue

                    # Get the credit
                    credit = db_session.query(CustomerCredit).get(credit_id)
                    if not credit or credit.available_amount < amount_in_credit:
                        continue

                    # Create sales transaction for this credit application
                    transaction = SalesTransaction(
                        invoice_id=invoice_id,
                        customer_id=invoice.customer_id,
                        payment_date=payment_date,
                        amount_paid=converted_amount,  # Amount in invoice currency
                        currency_id=invoice.currency,
                        reference_number=reference or f"Credit #{credit.id}",
                        is_posted_to_ledger=True,
                        payment_status=SalesPaymentStatus.partial,
                        created_by=current_user.id,
                        app_id=app_id,
                        bulk_payment_id=None
                    )
                    db_session.add(transaction)
                    db_session.flush()
                    transactions.append(transaction)

                    # Create payment allocation
                    payment_allocation = allocate_payment(
                        sale_transaction_id=transaction.id,
                        invoice_id=invoice_id,
                        payment_date=payment_date,
                        payment_amount=converted_amount,
                        remaining_balance=remaining_balance - total_applied,
                        db_session=db_session,
                        payment_mode=None,
                        total_tax_amount=invoice.total_tax_amount,
                        payment_account=customer_credit_account,
                        tax_payable_account_id=None,
                        credit_sale_account=None,
                        reference=reference,
                        overpayment_amount=Decimal('0.00'),
                        write_off_account_id=None,
                        payment_type='credit',
                        is_posted_to_ledger=True,
                        exchange_rate_id=credit.exchange_rate_id,
                        base_currency_id=base_currency_id,
                        invoice=invoice
                    )
                    db_session.add(payment_allocation)
                    db_session.flush()

                    # Create credit application record
                    credit_application = CreditApplication(
                        credit_id=credit.id,
                        payment_allocation_id=payment_allocation.id,
                        target_type='sales_invoice',
                        target_id=invoice.id,
                        applied_amount=amount_in_credit,
                        applied_amount_invoice_currency=converted_amount,
                        application_date=payment_date,
                        status='applied',
                        applied_by=current_user.id,
                        app_id=app_id
                    )
                    db_session.add(credit_application)

                    # Update credit available amount
                    credit.available_amount -= amount_in_credit
                    update_credit_status(credit)

                    # Add to allocation details for ledger
                    allocation_details.append({
                        'allocation': payment_allocation,
                        'invoice': invoice,
                        'amount': float(converted_amount),
                        'amount_payment': float(amount_in_credit),
                        'invoice_rate': float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1,
                        'credit': credit
                    })

                    total_applied += converted_amount

                if not transactions:
                    flash('Could not apply any credits to this invoice', 'error')
                    return redirect(url_for('sales.invoice_details', invoice_id=invoice_id))

                # Update invoice status
                new_total_paid = total_paid + total_applied
                if new_total_paid >= invoice.total_amount - Decimal('0.01'):
                    invoice.status = InvoiceStatus.paid
                else:
                    invoice.status = InvoiceStatus.partially_paid

                # Post each credit application to ledger
                for alloc in allocation_details:
                    success, msg = post_customer_credit_to_invoice(
                        db_session=db_session,
                        payment_allocation=alloc['allocation'],
                        credit=alloc['credit'],
                        invoice=invoice,
                        amount_in_credit=alloc['amount_payment'],
                        amount_in_invoice=alloc['amount'],
                        current_user=current_user,
                        suspense_account_id=suspense_account_id,
                        customer_credit_account_id=customer_credit_system_account_id,
                        fx_gain_loss_account_id=fx_gain_loss_account_id,
                        status='Posted',
                        project_id=invoice.project_id
                    )
                    if not success:
                        raise Exception(f"Failed to post credit to ledger: {msg}")

                flash_message = f'Payment recorded successfully using {len(transactions)} credit(s).'

            db_session.commit()
            flash(flash_message, 'success')
            return redirect(url_for('sales.invoice_details', invoice_id=invoice_id))

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error recording payment: {e}\n{traceback.format_exc()}')
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(url_for('sales.invoice_details', invoice_id=invoice_id))
    finally:
        db_session.close()


@sales_bp.route('/api/customer_credits', methods=['GET'])
@login_required
def get_customer_credits():
    db_session = None
    try:
        customer_id = request.args.get('customer_id')
        invoice_currency_id = request.args.get('invoice_currency_id', type=int)
        exclude_transaction_id = request.args.get('exclude_transaction_id', type=int)  # For edits

        if not customer_id:
            return jsonify({'status': 'error', 'message': 'Customer ID is required'}), 400
        if not invoice_currency_id:
            return jsonify({'status': 'error', 'message': 'Invoice currency ID is required'}), 400

        db_session = Session()

        base_currency_info = get_base_currency(db_session, current_user.app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_id = base_currency_info["base_currency_id"]

        # Base query for active credits
        query = db_session.query(CustomerCredit).filter(
            CustomerCredit.customer_id == customer_id,
            CustomerCredit.app_id == current_user.app_id,
            CustomerCredit.status == 'active',
            CustomerCredit.available_amount > 0
        )

        # If we're editing a transaction, exclude credits already used in this transaction
        if exclude_transaction_id:
            # Find credits used in this transaction through payment allocations
            used_credits_subquery = db_session.query(
                CreditApplication.credit_id
            ).join(
                PaymentAllocation,
                PaymentAllocation.id == CreditApplication.payment_allocation_id
            ).filter(
                PaymentAllocation.payment_id == exclude_transaction_id,  # Link to sales transaction
                CreditApplication.app_id == current_user.app_id,
                CreditApplication.status == 'applied'
            ).subquery()

            # Exclude those credits from available list
            query = query.filter(~CustomerCredit.id.in_(used_credits_subquery))

        # Get available credits
        cust_credits = query.order_by(CustomerCredit.issued_date.desc()).all()

        credits_data = []
        for credit in cust_credits:
            # Currency info
            currency = db_session.query(Currency).filter_by(id=credit.currency_id).first()

            # Compute converted amount in invoice currency
            if credit.currency_id == invoice_currency_id:
                converted_amount = float(credit.available_amount)
                exchange_rate = 1
                exchange_rate_id = None
            else:
                # Fetch exchange rate from credit currency -> invoice currency
                obj, rate = get_exchange_rate_and_obj(
                    db_session,
                    credit.currency_id,
                    invoice_currency_id,
                    app_id=current_user.app_id,
                    as_of_date=credit.issued_date_formatted
                )
                converted_amount = Decimal(str(credit.available_amount)) * rate
                exchange_rate = rate
                exchange_rate_id = obj.id if obj else None

            credits_data.append({
                'id': credit.id,
                'reference_number': credit.reference_number,
                'original_amount': float(credit.original_amount),
                'available_amount': float(credit.available_amount),
                'currency_id': credit.currency_id,
                'currency_code': currency.user_currency if currency else '',
                'currency_symbol': currency.user_currency if currency else '',
                'exchange_rate': float(exchange_rate) if exchange_rate else 1,
                'exchange_rate_id': exchange_rate_id,
                'converted_amount': float(converted_amount),
                'issued_date': credit.issued_date_formatted,
                'expires_date': credit.expires_date.isoformat() if credit.expires_date else None,
                'status': credit.status,
                'credit_reason': credit.credit_reason
            })

        return jsonify({
            'status': 'success',
            'credits': credits_data
        })

    except Exception as e:
        logger.error(f'Error fetching customer credits: {str(e)}\n{traceback.format_exc()}')
        return jsonify({'status': 'error', 'message': f'An error occurred: {str(e)}'}), 500

    finally:
        if db_session:
            db_session.close()


@sales_bp.route('/api/transaction_credits/<int:transaction_id>', methods=['GET'])
@login_required
def get_transaction_credits(transaction_id):
    db_session = None
    try:
        db_session = Session()

        # Get credit applications for this transaction through payment allocations
        applications = db_session.query(CreditApplication).join(
            PaymentAllocation,
            PaymentAllocation.id == CreditApplication.payment_allocation_id
        ).filter(
            PaymentAllocation.payment_id == transaction_id,
            CreditApplication.app_id == current_user.app_id,
            CreditApplication.status == 'applied'
        ).all()

        applications_data = []
        for app in applications:
            # Get credit details
            credit = db_session.query(CustomerCredit).filter_by(
                id=app.credit_id,
                app_id=current_user.app_id
            ).first()

            if credit:
                currency = db_session.query(Currency).filter_by(id=credit.currency_id).first()

                # Get the payment allocation to access amounts
                payment_allocation = app.payment_allocation

                applications_data.append({
                    'credit_id': app.credit_id,
                    'amount_applied': float(app.applied_amount),
                    'amount_in_invoice_currency': float(
                        app.applied_amount_invoice_currency) if app.applied_amount_invoice_currency else float(
                        app.applied_amount),
                    'original_amount': float(credit.original_amount),
                    # Show what would be available if this credit wasn't used here
                    'available_if_removed': float(credit.available_amount + app.applied_amount),
                    'currency_code': currency.user_currency if currency else '',
                    'exchange_rate': float(credit.exchange_rate.rate) if credit.exchange_rate else 1,
                    'issued_date': credit.issued_date_formatted,
                    'credit_reason': credit.credit_reason,
                    'reference_number': credit.reference_number,
                    'payment_allocation_id': payment_allocation.id if payment_allocation else None
                })

        return jsonify({
            'status': 'success',
            'applications': applications_data
        })

    except Exception as e:
        logger.error(f'Error fetching transaction credits: {str(e)}\n{traceback.format_exc()}')
        return jsonify({'status': 'error', 'message': f'An error occurred: {str(e)}'}), 500

    finally:
        if db_session:
            db_session.close()


@sales_bp.route('/api/apply_customer_credit', methods=['POST'])
@login_required
def apply_customer_credit():
    db_session = None
    try:
        data = request.get_json()
        invoice_id = data.get('invoice_id')
        credit_details = data.get('credit_details', [])
        amount = Decimal(data.get('amount', 0))

        logger.info(f'Applying customer credit - Invoice: {invoice_id}, Amount: {amount} and data {credit_details}')

        if not invoice_id or not credit_details or amount <= 0:
            return jsonify({'status': 'error', 'message': 'Invalid request parameters'}), 400

        db_session = Session()
        credit_application_id = None

        # Get the invoice with proper error handling
        invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id).first()
        if not invoice:
            logger.error(f'Invoice not found: {invoice_id}')
            return jsonify({'status': 'error', 'message': 'Invoice not found'}), 404

        # Use the invoice date for all credit application dating
        invoice_date = invoice.invoice_date

        # Calculate remaining balance
        total_paid = db_session.query(func.sum(SalesTransaction.amount_paid)).filter(
            SalesTransaction.invoice_id == invoice_id,
            SalesTransaction.payment_status.in_([
                SalesPaymentStatus.full,
                SalesPaymentStatus.partial,
                SalesPaymentStatus.paid
            ])
        ).scalar() or Decimal('0.00')

        remaining_balance = invoice.total_amount - total_paid
        logger.info(f'Remaining balance: {remaining_balance}, Amount to apply: {amount}')

        # Validate credit amount doesn't exceed remaining balance
        if amount > remaining_balance:
            logger.info(f'Reducing credit amount from {amount} to remaining balance {remaining_balance}')
            amount = remaining_balance  # Only apply up to the remaining balance

        # Get base currency info for ledger posting
        base_currency_info = get_base_currency(db_session, current_user.app_id)
        if not base_currency_info:
            return jsonify({'status': 'error', 'message': 'Base currency not configured'}), 400

        # Get customer credit account
        customer_credit_account_id = get_or_create_customer_credit_account(db_session, current_user.app_id)

        # Apply credits with currency conversion
        total_applied = Decimal('0.00')
        applied_credits = []

        for credit_info in credit_details:
            if total_applied >= amount:
                logger.info('Credit application complete - reached target amount')
                break

            credit_id = credit_info.get('id')
            credit_amount_applied = credit_info.get('original_amount')
            credit = db_session.query(CustomerCredit).filter_by(id=credit_id).first()

            if not credit:
                logger.warning(f'Credit not found: {credit_id}')
                continue

            if credit.customer_id != invoice.customer_id:
                logger.warning(f'Credit {credit_id} does not belong to invoice customer {invoice.customer_id}')
                continue

            # Convert credit amount to invoice currency
            exchange_rate = Decimal(credit_info.get('exchange_rate', 1))
            available_in_invoice_currency = credit.available_amount * exchange_rate

            # Calculate how much to apply from this credit
            remaining_needed = amount - total_applied
            credit_apply_amount = min(available_in_invoice_currency, remaining_needed)

            # Convert back to credit currency for deduction
            credit_deduction_amount = credit_apply_amount / exchange_rate

            logger.info(
                f'Applying credit {credit_id}: {credit_deduction_amount} ({credit.currency_id}) -> {credit_apply_amount} ({invoice.currency})')

            # Create credit application - use invoice date for proper accounting
            credit_application = CreditApplication(
                app_id=current_user.app_id,
                credit_id=credit.id,
                target_type='sales_invoice',
                target_id=invoice_id,
                applied_amount=credit_deduction_amount.quantize(Decimal('0.01')),  # ✅ Round
                applied_amount_invoice_currency=credit_apply_amount.quantize(Decimal('0.01')),  # ✅ Round
                applied_by=current_user.id,
                status='applied',
                application_date=invoice_date
            )
            db_session.add(credit_application)
            db_session.flush()
            credit_application_id = credit_application.id

            # Update credit available amount (in original currency)
            credit.available_amount -= credit_deduction_amount
            if credit.available_amount == 0:
                credit.status = 'used'

            total_applied += credit_apply_amount
            applied_credits.append({
                'credit_id': credit.id,
                'applied_amount': float(credit_deduction_amount),
                'currency': credit.currency_id,
                'exchange_rate': float(exchange_rate)
            })

        # Create a sales transaction for the credit application (in invoice currency)
        if total_applied > 0:
            sales_transaction = SalesTransaction(
                invoice_id=invoice_id,
                customer_id=invoice.customer_id,
                payment_date=invoice_date,  # Use invoice date for proper accounting
                amount_paid=total_applied,
                currency_id=invoice.currency,  # Use invoice currency
                reference_number=f"CREDIT-APP-{invoice_date.strftime('%Y%m%d')}",
                is_posted_to_ledger=False,
                payment_status=SalesPaymentStatus.paid,
                created_by=current_user.id,
                app_id=current_user.app_id
            )
            db_session.add(sales_transaction)
            db_session.flush()  # Flush to get the ID without committing

            # Allocate the payment - use invoice date
            payment_allocation = allocate_payment(
                sale_transaction_id=sales_transaction.id,
                invoice_id=invoice_id,
                payment_date=invoice_date,  # Use invoice date
                payment_amount=total_applied,
                remaining_balance=remaining_balance,
                db_session=db_session,
                payment_mode=None,
                total_tax_amount=invoice.total_tax_amount,
                payment_account=None,
                tax_payable_account_id=None,
                credit_sale_account=None,
                credit_application_id=credit_application_id,
                reference=f"Credit Application {invoice_date.strftime('%Y-%m-%d')}"
            )

            # ✅ POST TO LEDGER - Credit Application
            success, ledger_message = post_credit_application_to_ledger(
                db_session=db_session,
                sales_transaction=sales_transaction,
                payment_allocation=payment_allocation,
                customer_credit_account_id=customer_credit_account_id,
                current_user=current_user,
                base_currency_info=base_currency_info,
                status='Unposted'
            )

            if not success:
                logger.error(f'Failed to post credit application to ledger: {ledger_message}')
                raise Exception(f'Failed to post credit application to ledger: {ledger_message}')

            # Update invoice status
            if total_paid + total_applied >= invoice.total_amount:
                invoice.status = InvoiceStatus.paid
            else:
                invoice.status = InvoiceStatus.partially_paid

            db_session.commit()

        return jsonify({
            'status': 'success',
            'message': f'Successfully applied {format_currency(total_applied)} of customer credit as of {invoice_date.strftime("%Y-%m-%d")}',
            'applied_credits': applied_credits
        })

    except Exception as e:
        if db_session:
            db_session.rollback()
        logger.error(f'Error applying customer credit: {str(e)}', exc_info=True)
        return jsonify({'status': 'error', 'message': f'An error occurred: {str(e)}'}), 500

    finally:
        if db_session:
            db_session.close()


@sales_bp.route('/api/system_accounts', methods=['GET'])
@login_required
def get_system_accounts():
    app_id = current_user.app_id
    account_type = request.args.get('type')

    db_session = Session()
    try:
        if account_type == 'customer_credit':
            # Use your existing function to get the customer credit account
            customer_credit_account_id = get_or_create_customer_credit_account(
                db_session,
                app_id,
                current_user.id  # Pass the current user as creator if needed
            )

            return jsonify({
                'account_id': customer_credit_account_id,
                'account_type': 'customer_credit'
            })
        else:
            return jsonify({'error': 'Invalid account type'}), 400

    except Exception as e:
        logger.error(f'Error getting system accounts: {e}')
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()


@sales_bp.route('/api/suggest_quotation_reference')
@login_required
def suggest_quotation_reference():
    """
    API endpoint to suggest the next quotation reference based on the most recent reference.
    """
    with Session() as db_session:
        suggested_ref = suggest_next_quotation_reference(db_session)

    return jsonify({'suggested_reference': suggested_ref})


@sales_bp.route('/api/check_quotation_reference')
@login_required
def check_quotation_reference():
    """
    API endpoint to check if a quotation reference already exists.
    """
    reference = request.args.get('reference', type=str)
    exists = False

    if reference:
        with Session() as db_session:
            exists = db_session.query(Quotation).filter(
                Quotation.app_id == current_user.app_id,
                func.lower(Quotation.quotation_reference) == reference.lower()
            ).count() > 0

    return jsonify({'exists': exists})


@sales_bp.route('/api/suggest_sales_order_reference')
@login_required
def suggest_sales_order_reference():
    """
    API endpoint to suggest the next sales order reference based on the most recent reference.
    """
    with Session() as db_session:
        suggested_ref = suggest_next_sales_order_reference(db_session)

    return jsonify({'suggested_reference': suggested_ref})


@sales_bp.route('/api/process_payment', methods=['POST'])
@login_required
def process_payment():
    """
    Process a customer payment that can be applied to multiple invoices.

    Uses suspense account for multi-currency bridging and proper FX gain/loss tracking.
    """
    db_session = Session()
    try:
        data = request.get_json()

        # Get all data from frontend
        customer_id = data.get('customer_id')
        payment_date = datetime.strptime(data.get('payment_date'), '%Y-%m-%d')
        payment_method = data.get('payment_method')
        payment_account = data.get('payment_account')
        project = data.get('project')
        project_id = get_int_or_none(project)
        reference = data.get('reference')
        amount = Decimal(str(data.get('amount', 0)))

        currency_id = data.get('currency_id')  # Selected payment currency
        exchange_rate = Decimal(str(data.get('exchange_rate', 1))) if data.get('exchange_rate') else Decimal('1')
        overpayment_action = data.get('overpayment_action', 'credit')
        write_off_account = data.get('write_off_account')
        base_currency_id = int(data.get('base_currency_id'))

        # Get allocations from frontend
        allocations = data.get('allocations', [])  # Array of {invoice_id, amount, amount_base}

        # ===== GET SYSTEM ACCOUNTS =====

        # Usage in process_payment:
        system_accounts = get_all_system_accounts(
            db_session=db_session,
            app_id=current_user.app_id,
            created_by_user_id=current_user.id
        )
        suspense_account_id = system_accounts['suspense']
        fx_gain_loss_account_id = system_accounts['fx_gain_loss']
        customer_credit_account_id = system_accounts.get('customer_credit')  # May be None
        write_off_account_id = system_accounts.get('write_off')  # May be None

        # Initialize tracking variables
        total_allocated_in_payment_currency = Decimal('0.00')
        allocation_details = []
        transactions_created = []
        last_transaction = None
        last_invoice = None

        bulk_payment_number = generate_payment_receipt_number(db_session, current_user.app_id)

        # Create bulk payment record (groups all transactions together)
        bulk_payment = BulkPayment(
            bulk_payment_number=bulk_payment_number,
            customer_id=customer_id,
            total_amount=amount,
            currency_id=currency_id,
            payment_date=payment_date,
            payment_method=payment_method,
            payment_account_id=payment_account,
            reference=reference,
            project_id=project_id,
            created_by=current_user.id,
            app_id=current_user.app_id
        )
        db_session.add(bulk_payment)
        db_session.flush()

        # Create exchange rate record for this payment if foreign currency
        exchange_rate_id = None
        if int(currency_id) != base_currency_id:
            rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                session=db_session,
                action='create',
                from_currency_id=currency_id,
                to_currency_id=base_currency_id,
                rate_value=float(exchange_rate),
                rate_date=payment_date.date(),
                app_id=current_user.app_id,
                created_by=current_user.id,
                source_type='bulk_payment',
                source_id=bulk_payment.id
            )
            exchange_rate_id = rate_id
            bulk_payment.exchange_rate_id = exchange_rate_id

        # Process allocations ONLY if they exist
        if allocations and len(allocations) > 0:
            # Validate that allocations have the required structure
            valid_allocations = []
            for alloc in allocations:
                if 'invoice_id' in alloc and 'amount' in alloc:
                    valid_allocations.append(alloc)
                else:
                    logger.warning(f"Skipping invalid allocation: {alloc}")

            if not valid_allocations:
                logger.warning("No valid allocations found, treating as no-allocation case")
                allocations = []  # Reset to empty to trigger no-allocation handling
            else:
                # Get all invoice IDs from valid allocations
                invoice_ids = [alloc['invoice_id'] for alloc in valid_allocations]

                # Get current paid amounts for all relevant invoices
                invoice_paid_amounts = {}
                paid_results = db_session.query(
                    SalesTransaction.invoice_id,
                    func.coalesce(func.sum(SalesTransaction.amount_paid), 0).label('total_paid')
                ).filter(
                    SalesTransaction.invoice_id.in_(invoice_ids),
                    SalesTransaction.payment_status.in_(['paid', 'partial'])
                ).group_by(SalesTransaction.invoice_id).all()

                for row in paid_results:
                    invoice_paid_amounts[row.invoice_id] = row.total_paid or Decimal('0.00')

                # Create individual sales transactions for each valid allocation
                for alloc in valid_allocations:
                    invoice_id = alloc['invoice_id']
                    payment_amount = Decimal(str(alloc['amount_payment']))  # Amount in PAYMENT currency
                    payment_in_base = Decimal(str(alloc['amount_base']))  # Amount in PAYMENT currency
                    invoice = db_session.query(SalesInvoice).get(invoice_id)

                    if not invoice:
                        continue

                    # ===== CALCULATE CORRECT AMOUNT IN INVOICE CURRENCY =====
                    # For all invoices, use the amount from frontend directly
                    allocated_amount = Decimal(str(alloc['amount']))  # 15000 ARS
                    actual_payment = Decimal(str(alloc['amount_payment']))  # 9.74 USD


                    # Get invoice
                    invoice = db_session.query(SalesInvoice).get(invoice_id)
                    if not invoice:
                        continue

                    # Track total allocated in payment currency
                    total_allocated_in_payment_currency += payment_amount

                    # Calculate current balance for this invoice
                    total_paid_already = invoice_paid_amounts.get(invoice_id, Decimal('0.00'))
                    new_total_paid = total_paid_already + allocated_amount  # Still in invoice currency
                    is_fully_paid = new_total_paid >= invoice.total_amount - Decimal('0.01')

                    # Create sales transaction - amount in INVOICE currency
                    transaction = SalesTransaction(
                        invoice_id=invoice_id,
                        customer_id=customer_id,
                        payment_date=payment_date,
                        amount_paid=allocated_amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP),
                        currency_id=invoice.currency,
                        reference_number=reference,
                        is_posted_to_ledger=True,
                        payment_status='paid' if is_fully_paid else 'partial',
                        created_by=current_user.id,
                        app_id=current_user.app_id,
                        bulk_payment_id=bulk_payment.id
                    )
                    db_session.add(transaction)
                    db_session.flush()
                    transactions_created.append(transaction)

                    # Update the paid amount for next iterations
                    invoice_paid_amounts[invoice_id] = new_total_paid
                    last_transaction = transaction
                    last_invoice = invoice

                    # Create payment allocation
                    payment_allocation = allocate_payment(
                        sale_transaction_id=transaction.id,
                        invoice_id=invoice_id,
                        payment_date=payment_date,
                        payment_amount=allocated_amount,
                        remaining_balance=invoice.total_amount - total_paid_already,
                        db_session=db_session,
                        payment_mode=payment_method,
                        total_tax_amount=invoice.total_tax_amount,
                        payment_account=payment_account,
                        tax_payable_account_id=None,
                        credit_sale_account=None,
                        reference=reference,
                        overpayment_amount=Decimal('0.00'),
                        write_off_account_id=None,
                        payment_type=payment_method,
                        is_posted_to_ledger=True,
                        exchange_rate_id=exchange_rate_id,
                        base_currency_id=base_currency_id,
                        invoice=invoice
                    )
                    db_session.add(payment_allocation)

                    # Convert to payment currency (USD)
                    amount_in_payment_currency = payment_in_base / exchange_rate  # 37,500 / 3,850 = 9.74 USD


                    # Add to allocation details for ledger posting
                    allocation_details.append({
                        'allocation': payment_allocation,
                        'invoice': invoice,
                        'amount': float(allocated_amount),  # Invoice currency
                        'amount_base': float(payment_in_base),  # Base currency
                        'amount_payment': float(actual_payment),  # Payment currency
                        'invoice_rate': float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1
                    })

                    # Update invoice status directly
                    if is_fully_paid:
                        invoice.status = 'paid'
                    else:
                        invoice.status = 'partially_paid'


        # Calculate overpayment in PAYMENT CURRENCY
        # ===== HANDLE OVERPAYMENT =====
        # Calculate total allocated in BASE CURRENCY from allocation_details
        total_allocated_base = Decimal('0.00')
        for alloc in allocation_details:
            invoice = alloc['invoice']
            invoice_rate = float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1
            amount_in_base = float(alloc['amount']) * invoice_rate
            total_allocated_base += Decimal(str(amount_in_base))


        # Convert payment to base currency
        if int(currency_id) != base_currency_id:
            payment_in_base = amount * exchange_rate
        else:
            payment_in_base = amount

        # Overpayment in base currency
        overpayment_in_base = payment_in_base - total_allocated_base

        # Handle overpayment or no-allocation case
        if overpayment_in_base > 0 or not allocations or len(allocations) == 0:
            logger.info("✅ CONDITION PASSED - Entering credit/write-off block")

            if overpayment_action == 'write_off' and write_off_account:
                pass
            else:
                # Convert overpayment to payment currency for credit
                if int(currency_id) != base_currency_id:
                    credit_amount = (overpayment_in_base / exchange_rate).quantize(Decimal('0.01'))
                else:
                    credit_amount = overpayment_in_base.quantize(Decimal('0.01'))

                # Use amount if no allocations
                if not allocations or len(allocations) == 0:
                    credit_amount = amount.quantize(Decimal('0.01'))

                credit = CustomerCredit(
                    app_id=current_user.app_id,
                    customer_id=customer_id,
                    bulk_payment_id=bulk_payment.id,
                    original_amount=credit_amount,
                    available_amount=credit_amount,
                    currency_id=currency_id,
                    exchange_rate_id=exchange_rate_id,
                    created_date=datetime.now(),
                    issued_date=payment_date,
                    status='active',
                    credit_reason='overpayment' if allocations and len(allocations) > 0 else 'prepayment',
                    reference_number=reference,
                    created_by=current_user.id
                )
                db_session.add(credit)
                db_session.flush()
                logger.info(f"Created customer credit of {credit_amount} {currency_id}")

        # POST TO LEDGER using the new multi-currency method
        success, message = post_payment_receipt_to_ledger(
            db_session=db_session,
            receipt=bulk_payment,
            allocations=allocation_details,
            payment_account_id=payment_account,
            suspense_account_id=suspense_account_id,  # 👈 NEW
            fx_gain_loss_account_id=fx_gain_loss_account_id,
            current_user=current_user,
            overpayment_action=overpayment_action,
            customer_credit_account_id=customer_credit_account_id,
            write_off_account_id=write_off_account_id,
            status='Posted',
            project_id=last_invoice.project_id if last_invoice else None,
            exchange_rate_id=exchange_rate_id
        )

        if not success:
            raise Exception(f"Failed to post bulk payment to ledger: {message}")

        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Payment processed successfully',
            'bulk_payment_id': bulk_payment.id,
            'transaction_count': len(allocations) if allocations else 0
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error processing payment: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        db_session.close()


@sales_bp.route('/receive_payment', methods=['GET', 'POST'])
@login_required
def receive_payment():
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()

        # Get customers with outstanding balances
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()

        # Get payment methods
        payment_methods = db_session.query(PaymentMode).filter_by(
            app_id=app_id,
            is_active=True
        ).all()

        projects = db_session.query(Project).filter_by(
            app_id=app_id,
            is_active=True
        ).all()

        # Get funding accounts (cash/bank accounts)
        payment_accounts = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            or_(
                ChartOfAccounts.is_cash.is_(True),
                ChartOfAccounts.is_bank.is_(True)
            ),
            and_(ChartOfAccounts.is_system_account.is_(False))
        ).order_by(ChartOfAccounts.sub_category).all()

        grouped_payment_accounts = group_accounts_by_category(payment_accounts)

        # Get or create customer credit account (for overpayments)
        customer_credit_account_id = get_or_create_customer_credit_account(
            db_session=db_session,
            app_id=app_id,
            created_by_user_id=current_user.id
        )

        # Fetch the full account object for the template
        customer_credit_account = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.id == customer_credit_account_id
        ).first()

        # Get or create write-off account (miscellaneous income)
        write_off_account_id = get_or_create_write_off_account(
            db_session=db_session,
            app_id=app_id,
            created_by_user_id=current_user.id
        )

        # Fetch the full account object for the template
        write_off_account = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.id == write_off_account_id
        ).first()

        currencies = db_session.query(Currency).filter_by(app_id=app_id)
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''

        return render_template(
            'sales/receive_payments.html',
            customers=customers,
            payment_methods=payment_methods,
            projects=projects,
            payment_accounts=grouped_payment_accounts,
            customer_credit_account=customer_credit_account,
            write_off_account=write_off_account,
            company=company,
            role=current_user.role,
            currencies=currencies,
            base_currency=base_currency,
            base_currency_code=base_currency_code,
            modules=[mod.module_name for mod in db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        )
    except Exception as e:
        logger.error(f"Error in receive_payment: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the payment page.', 'error')
        return redirect(request.referrer)
    finally:
        db_session.close()


@sales_bp.route('/edit_receipt/<int:receipt_id>', methods=['GET', 'POST'])
@login_required
def edit_receipt(receipt_id):
    """
    Edit an existing payment receipt (BulkPayment) with selective updates.
    Only reverses and recreates what actually changed.
    """
    db_session = Session()
    try:
        app_id = current_user.app_id
        receipt = db_session.query(BulkPayment).filter(
            BulkPayment.id == receipt_id,
            BulkPayment.app_id == app_id
        ).first()

        if not receipt:
            flash('Payment receipt not found.', 'error')
            return redirect(url_for('sales.payment_history'))

        # Get selected customer (for POST or GET param)
        selected_customer_id = None
        if request.method == 'POST':

            selected_customer_id = request.form.get('customer')
        else:
            selected_customer_id = request.args.get('customer_id', receipt.customer_id)

        if request.method == 'POST':

            # Get version from form
            form_version = request.form.get('version', type=int)

            # Lock and check version in one query
            receipt = db_session.query(BulkPayment).filter(
                BulkPayment.id == receipt_id,
                BulkPayment.version == form_version
            ).with_for_update().first()

            if not receipt:
                flash('This receipt was modified by another user. Please refresh and try again.', 'error')
                return redirect(url_for('sales.edit_receipt', receipt_id=receipt_id))

            payment_date = datetime.strptime(request.form.get('payment_date'), '%Y-%m-%d')
            payment_method = request.form.get('payment_method')
            payment_account = request.form.get('payment_account')
            project = request.form.get('project')
            reference = request.form.get('reference') or None
            amount = Decimal(str(request.form.get('amount', 0)))
            currency_id = request.form.get('currency')
            exchange_rate = Decimal(str(request.form.get('exchange_rate', 1))) if request.form.get(
                'exchange_rate') else Decimal('1')
            overpayment_action = request.form.get('overpayment_action', 'credit')
            write_off_account = request.form.get('write_off_account')
            base_currency_id = int(request.form.get('base_currency_id'))

            # Get allocations from form (calculated by frontend)
            allocations_json = request.form.get('allocations', '[]')
            new_allocations = json.loads(allocations_json)
            logger.info(f'Edit form data is {request.form}')

            # ===== GET ALL SYSTEM ACCOUNTS IN ONE CALL =====
            system_accounts = get_all_system_accounts(
                db_session=db_session,
                app_id=app_id,
                created_by_user_id=current_user.id
            )
            suspense_account_id = system_accounts['suspense']
            fx_gain_loss_account_id = system_accounts['fx_gain_loss']
            customer_credit_account_id = system_accounts['customer_credit']
            write_off_system_account_id = system_accounts.get('write_off')

            # ===== REVERSE EXISTING TRANSACTIONS =====
            # First, delete allocation journals for each transaction
            for transaction in receipt.transactions:
                for allocation in transaction.payment_allocations:
                    delete_journal_entries_by_source(
                        db_session=db_session,
                        source_type='receipt_allocation',  # ✅ Delete allocation journals
                        source_id=allocation.id,
                        app_id=app_id
                    )
                    # ALSO delete credit application journals if this was a credit
                    if allocation.payment_type == 'credit':
                        delete_journal_entries_by_source(
                            db_session=db_session,
                            source_type='credit_application',
                            source_id=allocation.id,
                            app_id=app_id
                        )

            # Then delete payment allocations and transactions
            for transaction in receipt.transactions:
                for allocation in transaction.payment_allocations:
                    db_session.delete(allocation)
                db_session.delete(transaction)

            # Delete main receipt journals
            delete_journal_entries_by_source(
                db_session=db_session,
                source_type=['payment_receipt', 'receipt_adjustment'],  # ✅ Delete main receipt journals
                source_id=receipt.id,
                app_id=app_id
            )

            # Delete existing credits
            for credit in receipt.customer_credits:
                success, msg, stats = manage_customer_credits(
                    db_session=db_session,
                    source=receipt,
                    action='delete',
                    credit=credit,
                    force=True,
                    current_user=current_user
                )
                if not success:
                    raise Exception(f"Failed to delete credit: {msg}")
            # Delete old journal entries
            delete_journal_entries_by_source(
                db_session=db_session,
                source_type='payment_receipt',
                source_id=receipt.id,
                app_id=app_id
            )

            # Update receipt header
            receipt.payment_date = payment_date
            receipt.payment_method = payment_method
            receipt.payment_account_id = int(payment_account) if payment_account else None
            receipt.project_id = int(project) if project else None
            receipt.reference = reference
            receipt.total_amount = amount
            receipt.currency_id = int(currency_id)

            # Update exchange rate if needed
            if int(currency_id) != base_currency_id:
                rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=db_session,
                    action='create',
                    from_currency_id=currency_id,
                    to_currency_id=base_currency_id,
                    rate_value=float(exchange_rate),
                    rate_date=payment_date,
                    app_id=app_id,
                    created_by=current_user.id,
                    source_type='bulk_payment',
                    source_id=receipt.id
                )
                receipt.exchange_rate_id = rate_id
            else:
                receipt.exchange_rate_id = None

            db_session.flush()

            # ===== CREATE NEW TRANSACTIONS =====
            allocation_details = []

            for alloc in new_allocations:
                invoice_id = alloc['invoice_id']
                invoice = db_session.query(SalesInvoice).get(invoice_id)
                # Define invoice_rate at the beginning
                invoice_rate = float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1

                if not invoice:
                    continue

                # Get values from frontend
                frontend_amount = Decimal(str(alloc['amount']))  # 50
                frontend_payment = Decimal(str(alloc.get('amount_payment', frontend_amount)))  # 100
                frontend_amount_base = Decimal(str(alloc.get('amount_base', 0)))  # 250000
                payment_amount = frontend_payment  # Keep original payment amount
                allocated_amount = frontend_amount
                payment_in_base = frontend_amount_base

                # Get total paid from OTHER receipts
                total_paid_already = db_session.query(func.coalesce(func.sum(SalesTransaction.amount_paid), 0)) \
                                         .filter(
                    SalesTransaction.invoice_id == invoice_id,
                    SalesTransaction.bulk_payment_id != receipt.id,
                    SalesTransaction.payment_status != SalesPaymentStatus.cancelled  # ← Add this
                ).scalar() or Decimal('0.00')


                new_total_paid = total_paid_already + allocated_amount
                is_fully_paid = new_total_paid >= invoice.total_amount - Decimal('0.01')

                # Create new transaction
                transaction = SalesTransaction(
                    invoice_id=invoice_id,
                    customer_id=receipt.customer_id,
                    payment_date=payment_date,
                    amount_paid=allocated_amount,
                    currency_id=invoice.currency,
                    reference_number=reference,
                    is_posted_to_ledger=True,
                    payment_status='paid' if is_fully_paid else 'partial',
                    created_by=current_user.id,
                    app_id=app_id,
                    bulk_payment_id=receipt.id
                )
                db_session.add(transaction)

                db_session.flush()

                # Create payment allocation
                payment_allocation = allocate_payment(
                    sale_transaction_id=transaction.id,
                    invoice_id=invoice_id,
                    payment_date=payment_date,
                    payment_amount=allocated_amount,
                    remaining_balance=invoice.total_amount - total_paid_already,
                    db_session=db_session,
                    payment_mode=payment_method,
                    total_tax_amount=invoice.total_tax_amount,
                    payment_account=payment_account,
                    tax_payable_account_id=None,
                    credit_sale_account=None,
                    reference=reference,
                    overpayment_amount=Decimal('0.00'),
                    write_off_account_id=None,
                    payment_type=payment_method,
                    is_posted_to_ledger=True,
                    exchange_rate_id=receipt.exchange_rate_id,
                    base_currency_id=base_currency_id,
                    invoice=invoice
                )
                db_session.add(payment_allocation)
                db_session.flush()

                # ===== UPDATE INVOICE STATUSES =====
                update_invoice_status(db_session, invoice_input=invoice)


                allocation_details.append({
                        'allocation': payment_allocation,
                        'invoice': invoice,
                        'amount': float(allocated_amount),  # Invoice currency
                        'amount_base': float(payment_in_base),  # Base currency
                        'amount_payment': float(payment_amount),  # Total Payment currency
                        'invoice_rate': float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1
                    })

            # ===== HANDLE OVERPAYMENT =====
            # Calculate total allocated in BASE CURRENCY from allocation_details
            total_allocated_base = Decimal('0.00')
            for alloc in allocation_details:
                invoice = alloc['invoice']
                invoice_rate = float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1
                amount_in_base = float(alloc['amount']) * invoice_rate
                total_allocated_base += Decimal(str(amount_in_base))

            # Convert payment amount to base currency for comparison
            if int(currency_id) != base_currency_id:
                payment_in_base = amount * exchange_rate
            else:
                payment_in_base = amount

            # Overpayment in base currency
            overpayment_in_base = payment_in_base - total_allocated_base

            # For credit creation, store in payment currency
            if overpayment_in_base > 0 and overpayment_action != 'write_off':
                if int(currency_id) != base_currency_id:
                    overpayment_in_payment_currency = overpayment_in_base / exchange_rate
                else:
                    overpayment_in_payment_currency = overpayment_in_base

                credit = CustomerCredit(
                    app_id=app_id,
                    customer_id=receipt.customer_id,
                    bulk_payment_id=receipt.id,
                    original_amount=overpayment_in_payment_currency.quantize(Decimal('0.01')),
                    available_amount=overpayment_in_payment_currency.quantize(Decimal('0.01')),
                    currency_id=currency_id,
                    exchange_rate_id=receipt.exchange_rate_id,
                    created_date=datetime.now(),
                    issued_date=payment_date,
                    status='active',
                    credit_reason='overpayment',
                    reference_number=reference,
                    created_by=current_user.id
                )
                db_session.add(credit)

            # ===== POST TO LEDGER =====
            success, message = post_payment_receipt_to_ledger(
                db_session=db_session,
                receipt=receipt,
                allocations=allocation_details,
                payment_account_id=payment_account,
                suspense_account_id=suspense_account_id,
                fx_gain_loss_account_id=fx_gain_loss_account_id,
                current_user=current_user,
                overpayment_action=overpayment_action,
                customer_credit_account_id=customer_credit_account_id if overpayment_in_base > 0 and overpayment_action != 'write_off' else None,
                write_off_account_id=write_off_system_account_id if overpayment_action == 'write_off' else None,
                status='Posted',
                project_id=project,
                exchange_rate_id=receipt.exchange_rate_id
            )


            if not success:
                raise Exception(f"Failed to post to ledger: {message}")


            receipt.version += 1  # Increment version

            db_session.commit()
            flash('Payment receipt updated successfully.', 'success')
            return redirect(url_for('sales.view_bulk_receipt', receipt_id=receipt.id))
        # GET request - show edit form
        # Get related data for the form
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()

        payment_methods = db_session.query(PaymentMode).filter_by(app_id=app_id, is_active=True).all()
        projects = db_session.query(Project).filter_by(app_id=app_id, is_active=True).all()

        payment_accounts = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            or_(
                ChartOfAccounts.is_cash.is_(True),
                ChartOfAccounts.is_bank.is_(True)
            )
        ).order_by(ChartOfAccounts.sub_category).all()

        grouped_payment_accounts = group_accounts_by_category(payment_accounts)

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)

        # Get or create write-off account (miscellaneous income)
        write_off_account_id = get_or_create_write_off_account(
            db_session=db_session,
            app_id=app_id,
            created_by_user_id=current_user.id
        )

        # Fetch the full account object for the template
        write_off_account = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.id == write_off_account_id
        ).first()

        # Get existing allocations from this receipt

        # Get existing allocations from this receipt
        existing_allocations = []
        for transaction in receipt.transactions:
            existing_allocations.append({
                'invoice_id': transaction.invoice_id,
                'allocated_amount': float(transaction.amount_paid)
            })

        # Then use selected_customer_id in your invoice query
        all_invoices = get_invoices_for_edit_receipt(
            db_session=db_session,
            customer_id=selected_customer_id,
            receipt=receipt,  # Pass the receipt object, not receipt_id
            base_currency_id=base_currency.id
        )

        allocated_total = sum(float(t.amount_paid) for t in receipt.transactions)
        receipt_total_float = float(receipt.total_amount)

        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        module_name = "Sales"
        return render_template(
            'sales/edit_receipt.html',
            receipt=receipt,
            customers=customers,
            selected_customer_id=int(selected_customer_id),
            payment_methods=payment_methods,
            projects=projects,
            payment_accounts=grouped_payment_accounts,
            write_off_account=write_off_account,
            currencies=currencies,
            base_currency=base_currency,
            all_invoices=all_invoices,
            allocated_total=allocated_total,
            receipt_total_float=receipt_total_float,
            company=receipt.company,
            role=current_user.role,
            modules=modules_data,
            module_name=module_name,
            version=receipt.version,
        )

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error editing receipt {receipt_id}: {str(e)}\n{traceback.format_exc()}")
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(url_for('sales.payment_history'))
    finally:
        db_session.close()


def get_invoices_for_edit_receipt(db_session, customer_id, receipt, base_currency_id):
    """
    Get invoices for edit receipt, TEMPORARILY REVERSING this receipt's payments.
    This makes the receipt's payments "disappear" for calculation purposes.
    """
    # Get IDs of invoices in this receipt that have ACTIVE (non-cancelled) transactions
    active_receipt_invoice_ids = []
    for t in receipt.transactions:
        if t.payment_status != SalesPaymentStatus.cancelled:
            active_receipt_invoice_ids.append(t.invoice_id)

    # Get all invoices for this customer
    all_invoices = db_session.query(SalesInvoice).filter(
        SalesInvoice.app_id == receipt.app_id,
        SalesInvoice.customer_id == customer_id
    ).all()

    result = []
    for invoice in all_invoices:
        # Get payments from THIS receipt (to reverse) - EXCLUDE cancelled
        this_receipt_payment = db_session.query(func.coalesce(func.sum(SalesTransaction.amount_paid), 0)) \
                                   .filter(
            SalesTransaction.invoice_id == invoice.id,
            SalesTransaction.bulk_payment_id == receipt.id,
            SalesTransaction.payment_status != SalesPaymentStatus.cancelled
        ).scalar() or Decimal('0.00')

        # Get payments from OTHER receipts - EXCLUDE cancelled
        other_payments = db_session.query(func.coalesce(func.sum(SalesTransaction.amount_paid), 0)) \
                             .filter(
            SalesTransaction.invoice_id == invoice.id,
            SalesTransaction.bulk_payment_id != receipt.id,
            SalesTransaction.payment_status != SalesPaymentStatus.cancelled
        ).scalar() or Decimal('0.00')

        # Calculate balance due: total - other_payments (temporarily ignoring this receipt)
        # Calculate balance due: (current actual balance) + (this receipt's payment)
        current_actual_balance = float(invoice.total_amount - (other_payments + this_receipt_payment))
        balance_due = current_actual_balance + float(this_receipt_payment)

        # Check if this invoice has ACTIVE transactions from this receipt
        has_active_from_this_receipt = invoice.id in active_receipt_invoice_ids

        # Skip only if balance_due is 0 AND no active transactions from this receipt
        if balance_due <= 0.01 and not has_active_from_this_receipt:
            continue

        # Multi-currency calculation
        if invoice.currency != base_currency_id:
            if invoice.exchange_rate:
                rate = float(invoice.exchange_rate.rate)
                balance_due_base = balance_due * rate
                invoice_rate = rate
            else:
                balance_due_base = balance_due
                invoice_rate = 1
        else:
            balance_due_base = balance_due
            invoice_rate = 1

        # Get allocated amount from this receipt (only if not cancelled)
        if has_active_from_this_receipt:
            transaction = next(
                (t for t in receipt.transactions
                 if t.invoice_id == invoice.id and t.payment_status != SalesPaymentStatus.cancelled),
                None
            )
            if transaction:
                allocated_amount = float(transaction.amount_paid)
                if invoice.currency != receipt.currency_id and receipt.exchange_rate:
                    payment_amount = allocated_amount / float(receipt.exchange_rate.rate)
                else:
                    payment_amount = allocated_amount
            else:
                allocated_amount = 0
                payment_amount = 0
        else:
            allocated_amount = 0
            payment_amount = 0

        result.append({
            'id': invoice.id,
            'invoice_number': invoice.invoice_number,
            'invoice_date': invoice.invoice_date,
            'due_date': invoice.due_date,
            'total_amount': float(invoice.total_amount),
            'currency': invoice.currencies.user_currency,
            'currency_id': invoice.currency,
            'balance_due': balance_due,
            'balance_due_base': balance_due_base,
            'exchange_rate': invoice_rate,
            'allocated_amount': allocated_amount,
            'payment_amount': payment_amount,
            'is_from_receipt': has_active_from_this_receipt,  # Only true if has ACTIVE transactions
            'was_paid_by_this_receipt': this_receipt_payment > 0
        })
        # Calculate balance due: total - other_payments (temporarily ignoring this receipt)
        balance_due = float(invoice.total_amount - other_payments)

        # DEBUG LOG - Add this line
        logger.info(f"EDIT RECEIPT DEBUG - Invoice: {invoice.invoice_number}, "
                    f"Total: {invoice.total_amount}, "
                    f"This Receipt Paid: {this_receipt_payment}, "
                    f"Other Payments: {other_payments}, "
                    f"Balance Due (for editing): {balance_due}, "
                    f"Has Active from Receipt: {has_active_from_this_receipt}")
    # Sort by is_from_receipt first, then date, then invoice number
    result.sort(key=lambda x: (
        not x['is_from_receipt'],
        x['invoice_date'],
        x['invoice_number']
    ))

    return result


@sales_bp.route('/api/preview_receipt_edit/<int:receipt_id>', methods=['POST'])
@login_required
def preview_receipt_edit(receipt_id):
    """
    Preview how changes to a receipt would affect invoice allocations.
    Does NOT save anything - just returns what WOULD happen.
    """
    db_session = Session()
    try:
        app_id = current_user.app_id
        receipt = db_session.query(BulkPayment).filter(
            BulkPayment.id == receipt_id,
            BulkPayment.app_id == app_id
        ).first()

        if not receipt:
            return jsonify({'success': False, 'message': 'Receipt not found'})

        data = request.get_json()
        new_amount = Decimal(str(data.get('amount', 0)))
        new_currency_id = data.get('currency_id')
        new_exchange_rate = Decimal(str(data.get('exchange_rate', 1)))

        base_currency_id = int(data.get('base_currency_id'))

        # Get ALL outstanding invoices for this customer (including those already paid by this receipt)
        outstanding = get_customer_invoices_with_balances(
            db_session=db_session,
            customer_id=receipt.customer_id,
            app_id=app_id,
            base_currency_id=base_currency_id,
            exclude_receipt_id=receipt.id  # Exclude THIS receipt's payments
        )

        # Get invoices currently paid by this receipt
        currently_paid = []
        for transaction in receipt.transactions:
            invoice = transaction.invoice
            currently_paid.append({
                'id': invoice.id,
                'invoice_number': invoice.invoice_number,
                'amount': float(transaction.amount_paid),
                'currency': invoice.currencies.user_currency,
                'total_amount': float(invoice.total_amount)
            })

        # Calculate how new amount would be allocated
        # Convert new amount to base currency for comparison
        if new_currency_id != base_currency_id:
            new_amount_base = new_amount * new_exchange_rate
        else:
            new_amount_base = new_amount
            new_exchange_rate = Decimal('1')

        # Simulate allocation
        proposed_allocations = simulate_allocation(
            outstanding_invoices=outstanding,
            amount_base=new_amount_base,
            payment_currency_id=new_currency_id,
            exchange_rate=new_exchange_rate,
            base_currency_id=base_currency_id
        )

        # Compare with current state
        comparison = {
            'currently_paid': currently_paid,
            'proposed_allocations': proposed_allocations,
            'invoices_to_remove': [],  # Invoices that would lose allocation
            'invoices_to_add': [],  # Invoices that would gain allocation
            'invoices_to_change': []  # Invoices with amount changes
        }

        # Find differences
        current_map = {inv['id']: inv for inv in currently_paid}
        proposed_map = {inv['invoice_id']: inv for inv in proposed_allocations}

        # Invoices that would be removed
        for inv_id, inv in current_map.items():
            if inv_id not in proposed_map:
                comparison['invoices_to_remove'].append(inv)

        # Invoices that would be added
        for inv_id, inv in proposed_map.items():
            if inv_id not in current_map:
                comparison['invoices_to_add'].append(inv)

        # Invoices with amount changes
        for inv_id, proposed in proposed_map.items():
            if inv_id in current_map:
                current_amount = current_map[inv_id]['amount']
                if abs(current_amount - proposed['amount']) > 0.01:
                    comparison['invoices_to_change'].append({
                        'invoice_id': inv_id,
                        'invoice_number': proposed['invoice_number'],
                        'current_amount': current_amount,
                        'proposed_amount': proposed['amount'],
                        'currency': proposed['currency']
                    })

        return jsonify({
            'success': True,
            'comparison': comparison,
            'current_total': float(receipt.total_amount),
            'proposed_total': float(new_amount),
            'has_changes': len(comparison['invoices_to_remove']) > 0 or
                           len(comparison['invoices_to_add']) > 0 or
                           len(comparison['invoices_to_change']) > 0
        })

    except Exception as e:
        logger.error(f"Preview error: {str(e)}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        db_session.close()


def simulate_allocation(outstanding_invoices, amount_base, payment_currency_id, exchange_rate, base_currency_id):
    """
    Simulate how amount would be allocated to invoices.
    Returns list of proposed allocations.
    """
    proposed = []
    remaining = amount_base

    # Sort by oldest first (typical allocation logic)
    sorted_invoices = sorted(outstanding_invoices, key=lambda x: x['invoice_date'])

    for inv in sorted_invoices:
        if remaining <= 0:
            break

        # Get invoice balance in base currency
        if inv['currency_id'] != base_currency_id:
            inv_balance_base = inv['balance_due'] * inv.get('exchange_rate', 1)
        else:
            inv_balance_base = inv['balance_due']

        # Calculate amount to allocate
        alloc_base = min(remaining, inv_balance_base)

        # Convert back to invoice currency
        if inv['currency_id'] != base_currency_id:
            alloc_invoice = alloc_base / inv.get('exchange_rate', 1)
        else:
            alloc_invoice = alloc_base

        # Convert to payment currency if different
        if payment_currency_id != base_currency_id:
            alloc_payment = alloc_base / float(exchange_rate)
        else:
            alloc_payment = alloc_base

        proposed.append({
            'invoice_id': inv['id'],
            'invoice_number': inv['invoice_number'],
            'amount': round(alloc_invoice, 2),
            'amount_base': round(alloc_base, 2),
            'amount_payment': round(alloc_payment, 2),
            'currency': inv['currency']
        })

        remaining -= alloc_base

    return proposed


@sales_bp.route('/api/customer_outstanding_invoices/<int:customer_id>', methods=['GET'])
@login_required
def customer_outstanding_invoices(customer_id):
    db_session = Session()
    try:
        # Get base currency ID from query parameters
        base_currency_id = request.args.get('base_currency_id', type=int)

        # Get company info for multi-currency setting
        company = db_session.query(Company).filter(Company.id == current_user.app_id).first()
        is_multi_currency = company.has_multiple_currencies if company else False

        # First, get all invoices for this customer
        invoices = db_session.query(
            SalesInvoice.id,
            SalesInvoice.invoice_number,
            SalesInvoice.invoice_date,
            SalesInvoice.due_date,
            SalesInvoice.total_amount,
            SalesInvoice.currency,
            Currency.user_currency.label('currency_code'),
            ExchangeRate.rate.label('exchange_rate_value')
        ).join(
            Currency,
            Currency.id == SalesInvoice.currency
        ).outerjoin(
            ExchangeRate,
            ExchangeRate.id == SalesInvoice.exchange_rate_id
        ).filter(
            SalesInvoice.app_id == current_user.app_id,
            SalesInvoice.customer_id == customer_id,
            SalesInvoice.status.in_(['sent', 'overdue', 'partially_paid', 'unpaid'])
        ).order_by(SalesInvoice.invoice_date.asc()).all()

        result = []

        for inv in invoices:
            # Calculate total paid from NON-CANCELLED transactions only
            total_paid = db_session.query(
                func.coalesce(func.sum(SalesTransaction.amount_paid), 0)
            ).filter(
                SalesTransaction.invoice_id == inv.id,
                SalesTransaction.payment_status != 'cancelled'  # Exclude cancelled
            ).scalar() or 0

            # Calculate balance due
            balance = float(inv.total_amount - total_paid)

            # Only include invoices with balance > 0
            if balance <= 0.01:
                continue

            # Format dates safely
            invoice_date = inv.invoice_date.strftime('%Y-%m-%d') if inv.invoice_date else None
            due_date = inv.due_date.strftime('%Y-%m-%d') if inv.due_date else None

            invoice_data = {
                'id': inv.id,
                'invoice_number': inv.invoice_number,
                'invoice_date': invoice_date,
                'due_date': due_date,
                'total_amount': float(inv.total_amount),
                'balance_due': balance,
                'currency': inv.currency_code,
                'currency_id': inv.currency
            }

            # DEBUG LOG - REMOVE AFTER FIXING
            logger.info(f"INVOICE DEBUG - ID: {inv.id}, Number: {inv.invoice_number}, "
                        f"Total: {inv.total_amount}, Paid: {total_paid}, "
                        f"Balance: {balance}")

            # Add base currency equivalent if multi-currency
            if is_multi_currency and base_currency_id:
                if inv.currency == base_currency_id:
                    invoice_data['balance_due_base'] = balance
                elif inv.exchange_rate_value:
                    invoice_data['balance_due_base'] = balance * float(inv.exchange_rate_value)
                else:
                    logger.warning(f"No exchange rate for invoice {inv.id}")
                    invoice_data['balance_due_base'] = balance

            result.append(invoice_data)

        return jsonify({
            'success': True,
            'invoices': result,
            'is_multi_currency': is_multi_currency
        })

    except Exception as e:
        logger.error(f"Error fetching customer invoices: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        db_session.close()


@sales_bp.route('/payment-history', methods=['GET'])
@login_required
def payment_history():
    """
    Dedicated page for viewing all customer payments
    (Bulk payments, invoice payments, direct sales)
    """
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Get filter parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        customer_id = request.args.get('customer', type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        payment_mode = request.args.get('payment_mode', type=int)
        reference = request.args.get('reference')
        project = request.args.get('project')
        payment_account = request.args.get('payment_account')
        currency_id = request.args.get('currency', type=int)
        source_type = request.args.get('source_type')

        # Get filter dropdown data
        customers = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.is_active == True,
            Vendor.is_one_time == False,
            func.lower(Vendor.vendor_type).in_(['customer', 'customers', 'client', 'clients', 'buyer', 'buyers'])
        ).all()

        payment_modes = db_session.query(PaymentMode).filter_by(
            app_id=app_id,
            is_active=True
        ).all()

        projects = db_session.query(Project).filter_by(
            app_id=app_id,
            is_active=True
        ).all()
        payment_accounts = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            or_(
                ChartOfAccounts.is_cash.is_(True),
                ChartOfAccounts.is_bank.is_(True)
            )
        ).order_by(ChartOfAccounts.sub_category).all()

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        # Get company details
        company = db_session.query(Company).filter_by(id=app_id).first()

        base_currency = company.base_currency
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        return render_template(
            'sales/payment_history.html',

            customers=customers,
            payment_modes=payment_modes,
            payment_accounts=payment_accounts,
            projects=projects,
            currencies=currencies,
            company=company,
            modules=modules_data,
            role=current_user.role,
            filters={
                'customer': customer_id,
                'start_date': start_date,
                'end_date': end_date,
                'payment_mode': payment_mode,
                'reference': reference,
                'project': project,
                'currency': currency_id,
                'source_type': source_type
            }
        )

    except Exception as e:
        logger.error(f"Error in payment_history: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading payment history.', 'error')
        return redirect(url_for('sales.view_sales_transactions'))
    finally:
        db_session.close()


@sales_bp.route('/api/payment-history', methods=['GET'])
@login_required
def get_payment_history():
    """
    API endpoint for retrieving customer payment history
    """
    db_session = Session()
    try:
        # Pagination
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)

        # Filters
        customer_id = request.args.get('customer', type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        payment_mode = request.args.get('payment_mode', type=int)
        reference = request.args.get('reference')
        project_id = request.args.get('project', type=int)
        payment_account_id = request.args.get('payment_account', type=int)
        source_type = request.args.get('source_type')

        currency_id = request.args.get('currency', type=int)

        # Use SAME function as page view
        payments, pagination = _get_payment_history_data(
            db_session,
            page,
            per_page,
            customer_id,
            start_date,
            end_date,
            payment_mode,
            reference,
            currency_id,
            project_id,
            payment_account_id,
            source_type
        )

        return jsonify({
            "success": True,
            "payments": payments,
            "pagination": pagination
        })

    except Exception as e:
        logger.error(f"Error fetching payment history: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500

    finally:
        db_session.close()


@sales_bp.route('/api/payments/bulk-delete', methods=['POST'])
@login_required
def bulk_delete_payments():
    db_session = Session()
    try:
        data = request.get_json()
        payment_ids = data.get('ids', [])

        if not payment_ids:
            return jsonify({'success': False, 'message': 'No payments selected'})

        # Fetch all bulk payments to be deleted
        bulk_payments = db_session.query(BulkPayment).filter(
            BulkPayment.id.in_(payment_ids),
            BulkPayment.app_id == current_user.app_id
        ).all()

        if not bulk_payments:
            return jsonify({'success': False, 'message': 'No valid payments found'})

        error_messages = []
        invoices_to_update = set()
        deleted_count = 0
        failed_ids = []

        for receipt in bulk_payments:
            try:
                logger.info(f"Processing bulk delete for receipt {receipt.id}")

                # ===== 1. DELETE ALLOCATION JOURNALS FIRST =====
                for transaction in receipt.transactions:
                    for allocation in transaction.payment_allocations:
                        # Delete allocation-specific journals
                        success, msg = delete_journal_entries_by_source(
                            db_session=db_session,
                            source_type=['receipt_allocation'],  # Match your new source_type
                            source_id=allocation.id,
                            app_id=current_user.app_id
                        )
                        if not success:
                            error_messages.append(f"Receipt {receipt.id}: Failed to delete allocation journal: {msg}")

                # ===== 2. DELETE MAIN RECEIPT JOURNALS =====
                delete_journal_entries_by_source(
                    db_session=db_session,
                    source_type=['payment_receipt', 'receipt_adjustment'],
                    source_id=receipt.id,
                    app_id=current_user.app_id
                )
                # ===== 3. HANDLE CREDITS AND THEIR APPLICATIONS =====
                for credit in receipt.customer_credits:
                    # Use manage_customer_credits to delete the credit (force delete)
                    success, msg, stats = manage_customer_credits(
                        db_session=db_session,
                        source=receipt,
                        action='delete',
                        credit=credit,
                        force=True,
                        current_user=current_user
                    )

                    if not success:
                        error_messages.append(f"Receipt {receipt.id}: Failed to delete credit {credit.id}: {msg}")
                    else:
                        # Add affected invoices to update set
                        if stats and 'applications_deleted' in stats and stats['applications_deleted'] > 0:
                            for app in credit.credit_applications:
                                if app.payment_allocation and app.payment_allocation.sales_transaction:
                                    invoice_id = app.payment_allocation.sales_transaction.invoice_id
                                    if invoice_id:
                                        invoices_to_update.add(invoice_id)

                        logger.info(
                            f"Deleted credit #{credit.id} with {stats.get('applications_deleted', 0)} applications")

                # ===== 4. DELETE TRANSACTIONS AND UPDATE INVOICES =====
                for transaction in receipt.transactions:
                    if transaction.invoice_id:
                        invoices_to_update.add(transaction.invoice_id)

                    # Delete payment allocations
                    for allocation in transaction.payment_allocations:
                        logger.info(f"Deleting payment allocation {allocation.id}")
                        db_session.delete(allocation)

                    # Delete the transaction
                    logger.info(f"Deleting transaction {transaction.id}")
                    db_session.delete(transaction)

                # ===== 5. DELETE THE RECEIPT =====
                logger.info(f"Deleting receipt {receipt.id}")
                db_session.delete(receipt)
                deleted_count += 1

            except Exception as e:
                error_msg = f"Error processing receipt {receipt.id}: {str(e)}"
                logger.error(error_msg)
                error_messages.append(error_msg)
                failed_ids.append(receipt.id)
                continue

        # ===== 6. UPDATE INVOICE STATUSES =====
        for invoice_id in invoices_to_update:
            try:
                update_invoice_status(db_session, invoice_id)
                logger.info(f"Updated invoice {invoice_id} status")
            except Exception as e:
                error_messages.append(f"Error updating invoice {invoice_id}: {str(e)}")

        # ===== 7. COMMIT ALL CHANGES =====
        db_session.commit()

        response = {
            'success': True,
            'message': f'{deleted_count} payments processed',
            'deleted_count': deleted_count
        }

        if failed_ids:
            response['failed_ids'] = failed_ids
            response['message'] += f', {len(failed_ids)} failed'

        if error_messages:
            response['warnings'] = error_messages[:5]
            response['warning_count'] = len(error_messages)

        return jsonify(response)

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk_delete_payments: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        db_session.close()


@sales_bp.route('/delete-receipt/<int:receipt_id>', methods=['POST'])
@login_required
def delete_receipt(receipt_id):
    """
    Delete a bulk payment receipt and reverse all associated records.
    """
    db_session = Session()
    try:
        receipt = db_session.query(BulkPayment).filter(
            BulkPayment.id == receipt_id,
            BulkPayment.app_id == current_user.app_id
        ).first()

        if not receipt:
            flash('Receipt not found.', 'error')
            return redirect(url_for('sales.payment_history'))

        logger.info(f"Starting deletion of receipt {receipt_id}")

        error_messages = []
        invoices_to_update = set()

        # ===== 1. DELETE ALLOCATION JOURNALS FIRST =====
        for transaction in receipt.transactions:
            for allocation in transaction.payment_allocations:
                success, msg = delete_journal_entries_by_source(
                    db_session=db_session,
                    source_type='receipt_allocation',  # Match your new source_type
                    source_id=allocation.id,
                    app_id=current_user.app_id
                )
                if not success:
                    error_messages.append(f"Allocation {allocation.id}: Failed to delete journal - {msg}")

        # ===== 2. DELETE MAIN RECEIPT JOURNALS =====
        success, msg = delete_journal_entries_by_source(
            db_session=db_session,
            source_type=['payment_receipt', 'receipt_adjustment'],  # Match your new source_type
            source_id=receipt.id,
            app_id=current_user.app_id
        )
        if not success:
            error_messages.append(f"Failed to reverse receipt ledger: {msg}")

        # ===== 3. HANDLE CREDITS AND THEIR APPLICATIONS =====
        for credit in receipt.customer_credits:
            # Use manage_customer_credits to delete the credit (force delete)
            success, msg, stats = manage_customer_credits(
                db_session=db_session,
                source=receipt,
                action='delete',
                credit=credit,
                force=True,
                current_user=current_user
            )

            if not success:
                error_messages.append(f"Credit {credit.id}: {msg}")
            else:
                # Add affected invoices to update set
                if stats and 'applications_deleted' in stats and stats['applications_deleted'] > 0:
                    for app in credit.credit_applications:
                        if app.payment_allocation and app.payment_allocation.sales_transaction:
                            invoice_id = app.payment_allocation.sales_transaction.invoice_id
                            if invoice_id:
                                invoices_to_update.add(invoice_id)

                logger.info(f"Deleted credit #{credit.id} with {stats.get('applications_deleted', 0)} applications")

        # ===== 4. DELETE TRANSACTIONS AND PAYMENT ALLOCATIONS =====
        for transaction in receipt.transactions:
            if transaction.invoice_id:
                invoices_to_update.add(transaction.invoice_id)

            # Delete payment allocations (journals already deleted)
            for allocation in transaction.payment_allocations:
                logger.info(f"Deleting payment allocation {allocation.id}")
                db_session.delete(allocation)

            # Delete the transaction
            logger.info(f"Deleting transaction {transaction.id}")
            db_session.delete(transaction)

        # ===== 5. DELETE THE RECEIPT =====
        logger.info(f"Deleting receipt {receipt_id}")
        db_session.delete(receipt)

        # ===== 6. UPDATE INVOICE STATUSES =====
        for invoice_id in invoices_to_update:
            try:
                update_invoice_status(db_session, invoice_id)
                logger.info(f"Updated invoice {invoice_id} status")
            except Exception as e:
                error_messages.append(f"Error updating invoice {invoice_id}: {str(e)}")

        db_session.commit()

        if error_messages:
            flash(f'Receipt deleted with {len(error_messages)} warnings: ' + '; '.join(error_messages[:3]), 'warning')
        else:
            flash('Payment receipt deleted successfully.', 'success')

        return redirect(url_for('sales.payment_history'))

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error deleting receipt {receipt_id}: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while deleting the receipt.', 'error')
        return redirect(url_for('sales.payment_history'))
    finally:
        db_session.close()


def _get_payment_history_data(db_session, page, per_page, customer_id=None,
                              start_date=None, end_date=None, payment_mode=None,
                              reference=None, currency_id=None, project_id=None,
                              payment_account_id=None, source_type=None):
    """
    Helper function to retrieve payment history data with given filters.
    """
    app_id = current_user.app_id

    # Parse dates if provided
    start_date_obj = None
    end_date_obj = None
    if start_date:
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()

    # Collect all payments
    all_payments = []

    # ===== 1. Get ALL Bulk Payments =====
    bulk_query = db_session.query(
        BulkPayment.id,
        BulkPayment.bulk_payment_number,
        BulkPayment.payment_date,
        BulkPayment.total_amount,
        BulkPayment.currency_id,
        BulkPayment.reference,
        BulkPayment.payment_method,
        BulkPayment.payment_account_id,
        BulkPayment.project_id,
        BulkPayment.customer_id,
        Vendor.vendor_name.label('customer_name'),
        Currency.user_currency.label('currency'),
        PaymentMode.payment_mode.label('payment_mode'),
        ChartOfAccounts.sub_category.label('deposited_to'),
        Project.name.label('project_name'),
        literal('bulk').label('source_type'),
        literal('Payment Receipt').label('display_name'),
        literal(None).label('document_number')
    ).join(
        Vendor, BulkPayment.customer_id == Vendor.id
    ).join(
        Currency, BulkPayment.currency_id == Currency.id
    ).outerjoin(
        PaymentMode, BulkPayment.payment_method == PaymentMode.id
    ).outerjoin(
        ChartOfAccounts, BulkPayment.payment_account_id == ChartOfAccounts.id
    ).outerjoin(
        Project, BulkPayment.project_id == Project.id  # 👈 Add project join
    ).filter(
        BulkPayment.app_id == app_id
    )

    # Apply filters to bulk query
    if customer_id:
        bulk_query = bulk_query.filter(BulkPayment.customer_id == customer_id)
    if start_date_obj:
        bulk_query = bulk_query.filter(BulkPayment.payment_date >= start_date_obj)
    if end_date_obj:
        bulk_query = bulk_query.filter(BulkPayment.payment_date <= end_date_obj)
    if payment_mode:
        bulk_query = bulk_query.filter(BulkPayment.payment_method == payment_mode)
    if reference:
        bulk_query = bulk_query.filter(BulkPayment.reference.ilike(f'%{reference}%'))
    if currency_id:
        bulk_query = bulk_query.filter(BulkPayment.currency_id == currency_id)
    if project_id:  # 👈 Add project filter
        bulk_query = bulk_query.filter(BulkPayment.project_id == project_id)
    if payment_account_id:  # 👈 Add payment account filter
        bulk_query = bulk_query.filter(BulkPayment.payment_account_id == payment_account_id)


    for row in bulk_query.all():
        # Calculate allocation status
        bulk = db_session.query(BulkPayment).get(row.id)
        total_allocated = sum(float(t.amount_paid) for t in bulk.transactions) if bulk else 0

        if total_allocated >= float(row.total_amount):
            status = 'fully_allocated'
        elif total_allocated > 0:
            status = 'partially_allocated'
        else:
            status = 'unallocated'

        all_payments.append({
            'id': row.id,
            'date': row.payment_date.strftime('%Y-%m-%d') if row.payment_date else None,
            'customer_name': row.customer_name,
            'amount': float(row.total_amount),
            'currency': row.currency,
            'reference': row.reference,
            'payment_mode': row.payment_mode,
            'deposited_to': row.deposited_to,
            'project_name': row.project_name,  # 👈 Add project name
            'project_id': row.project_id,  # 👈 Add project ID
            'payment_account_id': row.payment_account_id,  # 👈 Add payment account ID
            'document_number': row.bulk_payment_number,
            'source_type': 'bulk',
            'display_name': 'Payment Receipt',
            'status': status
        })

    # ===== 2. Get ALL Direct Sales =====
    direct_query = db_session.query(
        DirectSalesTransaction.id,
        DirectSalesTransaction.payment_date,
        DirectSalesTransaction.amount_paid,
        DirectSalesTransaction.currency_id,
        DirectSalesTransaction.sale_reference,
        DirectSalesTransaction.project_id,
        PaymentAllocation.payment_mode,
        PaymentAllocation.payment_account,
        DirectSalesTransaction.customer_id,
        Vendor.vendor_name.label('customer_name'),
        Currency.user_currency.label('currency'),
        PaymentMode.payment_mode.label('payment_mode'),
        ChartOfAccounts.sub_category.label('deposited_to'),
        Project.name.label('project_name'),
        DirectSalesTransaction.direct_sale_number.label('document_number'),
        literal('direct_sale').label('source_type'),
        literal('Direct Sale').label('display_name')
    ).join(
        Vendor, DirectSalesTransaction.customer_id == Vendor.id
    ).join(
        Currency, DirectSalesTransaction.currency_id == Currency.id
    ).join(
        PaymentAllocation, PaymentAllocation.direct_sale_id == DirectSalesTransaction.id
    ).outerjoin(
        PaymentMode, PaymentAllocation.payment_mode == PaymentMode.id
    ).outerjoin(
        ChartOfAccounts, PaymentAllocation.payment_account == ChartOfAccounts.id
    ).outerjoin(
        Project, DirectSalesTransaction.project_id == Project.id  # 👈 Add project join
    ).filter(
        DirectSalesTransaction.app_id == app_id
    )

    # Apply filters to direct query
    if customer_id:
        direct_query = direct_query.filter(DirectSalesTransaction.customer_id == customer_id)
    if start_date_obj:
        direct_query = direct_query.filter(DirectSalesTransaction.payment_date >= start_date_obj)
    if end_date_obj:
        direct_query = direct_query.filter(DirectSalesTransaction.payment_date <= end_date_obj)
    if payment_mode:
        direct_query = direct_query.filter(PaymentAllocation.payment_mode == payment_mode)
    if reference:
        direct_query = direct_query.filter(DirectSalesTransaction.sale_reference.ilike(f'%{reference}%'))
    if currency_id:
        direct_query = direct_query.filter(DirectSalesTransaction.currency_id == currency_id)
    if project_id:  # 👈 Add project filter
        direct_query = direct_query.filter(DirectSalesTransaction.project_id == project_id)
    if payment_account_id:  # 👈 Add payment account filter
        direct_query = direct_query.filter(PaymentAllocation.payment_account == payment_account_id)

    direct_count = direct_query.count()
    logger.info(f"DIRECT QUERY: Found {direct_count} direct sales")

    for row in direct_query.all():
        all_payments.append({
            'id': row.id,
            'date': row.payment_date.strftime('%Y-%m-%d') if row.payment_date else None,
            'customer_name': row.customer_name,
            'amount': float(row.amount_paid),
            'currency': row.currency,
            'reference': row.sale_reference,
            'payment_mode': row.payment_mode,
            'deposited_to': row.deposited_to,
            'project_name': row.project_name,  # 👈 Add project name
            'project_id': row.project_id,  # 👈 Add project ID
            'payment_account_id': row.payment_account,  # 👈 Add payment account ID
            'document_number': row.document_number,
            'source_type': 'direct_sale',
            'display_name': 'Direct Sale',
            'status': 'Paid'
        })



    # ===== 3. Get ALL Invoice Payments (from record_payment) =====
    invoice_query = db_session.query(
        SalesTransaction.id,
        SalesTransaction.payment_date,
        SalesTransaction.amount_paid,
        SalesTransaction.currency_id,
        SalesTransaction.reference_number,
        PaymentAllocation.payment_mode.label('payment_mode_id'),
        PaymentAllocation.payment_account.label('payment_account_id'),
        SalesInvoice.project_id,  # ✅ Get project from invoice, not transaction
        SalesTransaction.customer_id,
        Vendor.vendor_name.label('customer_name'),
        Currency.user_currency.label('currency'),
        PaymentMode.payment_mode.label('payment_mode'),
        ChartOfAccounts.sub_category.label('deposited_to'),
        Project.name.label('project_name'),
        SalesInvoice.invoice_number.label('document_number'),
        literal('invoice_payment').label('source_type'),
        literal('Invoice Payment').label('display_name'),
        SalesTransaction.payment_status.label('status')
    ).join(
        Vendor, SalesTransaction.customer_id == Vendor.id
    ).join(
        Currency, SalesTransaction.currency_id == Currency.id
    ).join(
        SalesInvoice, SalesTransaction.invoice_id == SalesInvoice.id  # Join to invoice
    ).join(
        PaymentAllocation, PaymentAllocation.payment_id == SalesTransaction.id
    ).outerjoin(
        PaymentMode, PaymentAllocation.payment_mode == PaymentMode.id
    ).outerjoin(
        ChartOfAccounts, PaymentAllocation.payment_account == ChartOfAccounts.id
    ).outerjoin(
        Project, SalesInvoice.project_id == Project.id  # ✅ Join project through invoice
    ).filter(
        SalesTransaction.app_id == app_id,
        SalesTransaction.invoice_id.isnot(None),
        SalesTransaction.payment_status != SalesPaymentStatus.cancelled,
        SalesTransaction.bulk_payment_id.is_(None)
    )

    # Apply filters to invoice query
    if customer_id:
        invoice_query = invoice_query.filter(SalesTransaction.customer_id == customer_id)
    if start_date_obj:
        invoice_query = invoice_query.filter(SalesTransaction.payment_date >= start_date_obj)
    if end_date_obj:
        invoice_query = invoice_query.filter(SalesTransaction.payment_date <= end_date_obj)
    if payment_mode:
        invoice_query = invoice_query.filter(PaymentAllocation.payment_mode == payment_mode)
    if reference:
        invoice_query = invoice_query.filter(SalesTransaction.reference_number.ilike(f'%{reference}%'))
    if currency_id:
        invoice_query = invoice_query.filter(SalesTransaction.currency_id == currency_id)
    if project_id:
        invoice_query = invoice_query.filter(SalesTransaction.project_id == project_id)
    if payment_account_id:
        invoice_query = invoice_query.filter(PaymentAllocation.payment_account == payment_account_id)

    invoice_count = invoice_query.count()


    for row in invoice_query.all():
        all_payments.append({
            'id': row.id,
            'date': row.payment_date.strftime('%Y-%m-%d') if row.payment_date else None,
            'customer_name': row.customer_name,
            'amount': float(row.amount_paid),
            'currency': row.currency,
            'reference': row.reference_number,
            'payment_mode': row.payment_mode,
            'deposited_to': row.deposited_to,
            'project_name': row.project_name,
            'project_id': row.project_id,
            'payment_account_id': row.payment_account_id,
            'document_number': row.document_number or f"PMT-{row.id}",
            'source_type': 'invoice_payment',
            'display_name': 'Invoice Payment',
            'status': row.status.value if hasattr(row.status, 'value') else row.status
        })

    # Log each payment's source_type
    source_types = {}
    for p in all_payments:
        st = p['source_type']
        source_types[st] = source_types.get(st, 0) + 1

    if source_type:
        all_payments = [p for p in all_payments if p['source_type'] == source_type]

    # Sort by date (newest first)
    all_payments.sort(key=lambda x: x['date'] or '', reverse=True)

    # Paginate
    total = len(all_payments)
    start = (page - 1) * per_page
    end = start + per_page
    paginated_payments = all_payments[start:end]

    pagination_data = {
        'page': page,
        'per_page': per_page,
        'total_pages': (total + per_page - 1) // per_page,
        'total_items': total,
        'has_next': page < ((total + per_page - 1) // per_page),
        'has_prev': page > 1
    }

    return paginated_payments, pagination_data


# Make sure these routes exist


@sales_bp.route('/receipt/<int:receipt_id>')
@login_required
def view_bulk_receipt(receipt_id):
    """View details of a bulk payment receipt"""
    db_session = Session()
    app_id = current_user.app_id
    try:
        receipt = db_session.query(BulkPayment).filter(
            BulkPayment.id == receipt_id,
            BulkPayment.app_id == current_user.app_id
        ).first()

        if not receipt:
            flash('Receipt not found.', 'error')
            return redirect(url_for('sales.payment_history'))

        # Get all transactions (invoice payments) from this receipt
        transactions = receipt.transactions

        # Get all credits created from this receipt
        credits = receipt.customer_credits

        # ===== REPLACE THIS SECTION =====
        # Calculate total allocated in PAYMENT CURRENCY (not sum of original amounts)
        total_allocated = 0
        payment_rate = float(receipt.exchange_rate.rate) if receipt.exchange_rate else 1

        for tx in transactions:
            # Skip cancelled transactions
            if tx.payment_status == SalesPaymentStatus.cancelled:
                continue

            if tx.currency_id == receipt.currency_id:
                # Same as receipt currency - use directly
                amount_in_payment = float(tx.amount_paid)
            else:
                # Different currency - need to convert to receipt currency
                # First, get amount in base currency
                if tx.invoice and tx.invoice.exchange_rate:
                    invoice_rate = float(tx.invoice.exchange_rate.rate)
                    amount_in_base = float(tx.amount_paid) * invoice_rate
                else:
                    amount_in_base = float(tx.amount_paid)

                # Convert from base to receipt currency
                amount_in_payment = amount_in_base / payment_rate

            total_allocated += amount_in_payment
        # ===== END REPLACEMENT =====

        total_amount = float(receipt.total_amount)
        # Use a percentage-based tolerance (0.1% of payment amount)
        tolerance = total_amount * 0.001  # 0.1% tolerance

        if abs(total_allocated - total_amount) <= tolerance:
            allocation_status = 'fully_allocated'
        elif total_allocated > 0:
            allocation_status = 'partially_allocated'
        else:
            allocation_status = 'unallocated'

        # Get base currency
        company = db_session.query(Company).filter_by(id=app_id).first()
        base_currency = company.base_currency
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Calculate cross rates for display
        cross_rates = []
        payment_currency = receipt.currency
        payment_exchange_rate = receipt.exchange_rate

        if payment_exchange_rate and payment_currency.id != base_currency.id:
            for tx in transactions:
                # Get exchange rate from payment allocation
                if hasattr(tx, 'payment_allocations') and tx.payment_allocations:
                    allocation = tx.payment_allocations[0]
                    if allocation.exchange_rate and allocation.exchange_rate.id != payment_exchange_rate.id:
                        cross_rate_value = float(payment_exchange_rate.rate) * float(allocation.exchange_rate.rate)
                        cross_rates.append({
                            'from_currency': payment_currency.user_currency,
                            'to_currency': tx.currency.user_currency,
                            'rate': cross_rate_value,
                            'invoice_number': tx.invoice.invoice_number
                        })

        return render_template(
            'sales/receipt_detail.html',
            receipt=receipt,
            transactions=transactions,
            credits=credits,
            total_amount=total_amount,
            total_allocated=total_allocated,  # Now correctly in payment currency
            payment_rate=payment_rate,
            allocation_status=allocation_status,
            base_currency=base_currency,
            company=company,
            modules=modules_data,
            role=role,
            cross_rates=cross_rates
        )

    except Exception as e:
        logger.error(f"Error viewing receipt: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading receipt details.', 'error')
        return redirect(url_for('sales.payment_history'))
    finally:
        db_session.close()


@sales_bp.route('/allocate-receipt/<int:id>')
def allocate_receipt(id):
    # Page to allocate bulk receipt to invoices
    pass


@sales_bp.route('/transaction/<int:id>')
def view_transaction(id):
    # View invoice payment details
    pass


@sales_bp.route('/direct-sale/<int:id>')
def view_direct_sale(id):
    # View direct sale details
    pass
