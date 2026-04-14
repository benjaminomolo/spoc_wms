import json
import logging
import traceback
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from flask import jsonify, request, flash, redirect, url_for, render_template
from flask_login import login_required, current_user
from sqlalchemy import func, or_, literal
from sqlalchemy.orm import joinedload

from ai import get_base_currency
from db import Session
from models import DirectSalesTransaction, SalesInvoice, SalesTransaction, SalesPaymentStatus, InvoiceStatus, \
    OrderStatus, CustomerCredit, CreditApplication, Currency, Quotation, Company, Vendor, PaymentMode, ChartOfAccounts, \
    Module, PaymentAllocation, BulkPayment, Project
from services.chart_of_accounts_helpers import group_accounts_by_category, get_all_system_accounts
from services.post_to_ledger import post_sales_transaction_to_ledger, post_customer_credit_to_ledger, \
    post_credit_application_to_ledger, post_overpayment_write_off_to_ledger, post_payment_receipt_to_ledger, \
    post_credit_write_off_to_ledger, post_customer_credit_to_invoice
from services.post_to_ledger_reversal import delete_journal_entries_by_source
from services.sales_helpers import suggest_next_direct_sale_reference, suggest_next_invoice_reference, \
    allocate_direct_sale_payment, allocate_payment, suggest_next_quotation_reference, \
    suggest_next_sales_order_reference, update_invoice_status
from services.vendors_and_customers import get_or_create_customer_credit_account, get_or_create_write_off_account, \
    apply_existing_credits_to_invoice, create_credit_from_overpayment
from utils import create_transaction, generate_unique_journal_number
from utils_and_helpers.amounts_utils import format_currency
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj, get_or_create_exchange_rate_for_transaction
from utils_and_helpers.forms import validate_required_fields

from . import sales_bp

logger = logging.getLogger(__name__)


@sales_bp.route('/customer-credits', methods=['GET'])
@login_required
def view_customer_credits():
    """
    Dedicated page for viewing customer credits
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
        currency_id = request.args.get('currency', type=int)
        status = request.args.get('status')

        filter_applied = bool(customer_id or start_date or end_date or currency_id or status)

        # Fetch filter dropdown data
        customers = db_session.query(Vendor).filter_by(
            app_id=app_id,
            is_active=True,
            vendor_type='Customer'
        ).all()

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        # Get company details
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Get credits data
        credits, pagination = _get_customer_credits_data(
            db_session, page, per_page, customer_id, start_date, end_date, currency_id, status
        )

        return render_template(
            'sales/customer_credits.html',
            credits=credits,
            pagination=pagination,
            customers=customers,
            currencies=currencies,
            company=company,
            role=role,
            module_name="Sales",
            modules=modules_data,
            filter_applied=filter_applied,
            filters={
                'customer': customer_id,
                'start_date': start_date,
                'end_date': end_date,
                'currency': currency_id,
                'status': status
            }
        )

    except Exception as e:
        logger.error(f"Error in view_customer_credits: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading customer credits.', 'error')
        return redirect(url_for('sales.view_sales_transactions'))
    finally:
        db_session.close()


@sales_bp.route('/api/customer-credits', methods=['GET'])
@login_required
def get_customer_credits_api():
    """
    Return filtered customer credits data for AJAX requests
    """
    db_session = Session()
    try:
        # Get parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        customer_id = request.args.get('customer', type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        currency_id = request.args.get('currency', type=int)
        status = request.args.get('status')

        # Get filtered data
        credits, pagination = _get_customer_credits_data(
            db_session, page, per_page, customer_id, start_date, end_date, currency_id, status
        )

        return jsonify({
            'success': True,
            'credits': credits,
            'pagination': pagination
        })

    except Exception as e:
        logger.error(f"Error in get_customer_credits_api: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        db_session.close()


@sales_bp.route('/api/credits/bulk-delete', methods=['POST'])
@login_required
def bulk_delete_credits():
    db_session = Session()
    try:
        data = request.get_json()
        credit_ids = data.get('ids', [])

        if not credit_ids:
            return jsonify({'success': False, 'message': 'No credits selected'}), 400

        # Fetch credits to be deleted
        credits = db_session.query(CustomerCredit).filter(
            CustomerCredit.id.in_(credit_ids),
            CustomerCredit.app_id == current_user.app_id
        ).all()

        if not credits:
            return jsonify({'success': False, 'message': 'No valid credits found'}), 404

        deleted_count = 0
        failed_ids = []
        bulk_payment_credits = []

        for credit in credits:
            try:
                # Check if credit is linked to a bulk payment
                if credit.bulk_payment_id:
                    bulk_payment_credits.append({
                        'id': credit.id,
                        'bulk_payment_id': credit.bulk_payment_id
                    })
                    continue

                # Check if credit has been used
                if credit.available_amount < credit.original_amount:
                    # Credit has been partially used - use manage_customer_credits with force=True
                    success, msg, stats = manage_customer_credits(
                        db_session=db_session,
                        source=credit,
                        action='delete',
                        credit=credit,
                        force=True,
                        current_user=current_user
                    )
                else:
                    # Credit is unused - safe to delete directly
                    # Delete credit applications first
                    db_session.query(CreditApplication).filter(
                        CreditApplication.credit_id == credit.id
                    ).delete()

                    # Delete journal entries
                    delete_journal_entries_by_source(
                        db_session=db_session,
                        source_type='customer_credit',
                        source_id=credit.id,
                        app_id=current_user.app_id
                    )

                    # Delete the credit
                    db_session.delete(credit)

                deleted_count += 1

            except Exception as e:
                logger.error(f"Error deleting credit {credit.id}: {str(e)}")
                failed_ids.append(credit.id)
                continue

        # Prepare response message
        message = f"{deleted_count} credit(s) deleted successfully."

        if bulk_payment_credits:
            message += f" {len(bulk_payment_credits)} credit(s) from bulk payments cannot be deleted (they must be reversed via the original receipt)."

        if failed_ids:
            message += f" {len(failed_ids)} credit(s) failed to delete."

        db_session.commit()

        return jsonify({
            'success': True,
            'message': message,
            'deleted_count': deleted_count,
            'failed_ids': failed_ids,
            'bulk_payment_credits': bulk_payment_credits
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk delete credits: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


def _get_customer_credits_data(db_session, page, per_page, customer_id=None,
                               start_date=None, end_date=None, currency_id=None,
                               status=None):
    """
    Helper function to retrieve customer credits data with given filters.
    """
    app_id = current_user.app_id

    # Parse dates if provided
    start_date_obj = None
    end_date_obj = None
    if start_date:
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()

    # Base query
    query = db_session.query(
        CustomerCredit.id,
        CustomerCredit.original_amount,
        CustomerCredit.available_amount,
        CustomerCredit.currency_id,
        CustomerCredit.issued_date,
        CustomerCredit.created_date,
        CustomerCredit.status,
        CustomerCredit.credit_reason,
        CustomerCredit.reference_number,
        CustomerCredit.customer_id,
        Vendor.vendor_name.label('customer_name'),
        Currency.user_currency.label('currency_code'),
        BulkPayment.reference.label('bulk_reference'),
        PaymentAllocation.reference.label('payment_reference')
    ).join(
        Vendor, CustomerCredit.customer_id == Vendor.id
    ).join(
        Currency, CustomerCredit.currency_id == Currency.id
    ).outerjoin(
        BulkPayment, CustomerCredit.bulk_payment_id == BulkPayment.id
    ).outerjoin(
        PaymentAllocation, CustomerCredit.payment_allocation_id == PaymentAllocation.id
    ).filter(
        CustomerCredit.app_id == app_id
    )

    # Apply filters
    if customer_id:
        query = query.filter(CustomerCredit.customer_id == customer_id)

    if start_date_obj:
        query = query.filter(CustomerCredit.issued_date >= start_date_obj)

    if end_date_obj:
        query = query.filter(CustomerCredit.issued_date <= end_date_obj)

    if currency_id:
        query = query.filter(CustomerCredit.currency_id == currency_id)

    if status:
        query = query.filter(CustomerCredit.status == status)

    # Get total count before pagination
    total_items = query.count()

    # Apply pagination and ordering
    credits = query.order_by(
        CustomerCredit.issued_date.desc(),
        CustomerCredit.id.desc()
    ).offset((page - 1) * per_page).limit(per_page).all()

    # Build response
    result = []
    for row in credits:
        # Determine source reference
        source_ref = row.bulk_reference or row.payment_reference or f"Credit #{row.id}"

        # Calculate usage if needed (original - available = used)
        used_amount = float(row.original_amount) - float(row.available_amount)

        result.append({
            'id': row.id,
            'issued_date': row.issued_date.strftime('%Y-%m-%d') if row.issued_date else None,
            'created_date': row.created_date.strftime('%Y-%m-%d') if row.created_date else None,
            'customer_id': row.customer_id,
            'customer_name': row.customer_name,
            'original_amount': float(row.original_amount),
            'available_amount': float(row.available_amount),
            'used_amount': used_amount,
            'currency': row.currency_code,
            'currency_id': row.currency_id,
            'status': row.status,
            'credit_reason': row.credit_reason,
            'reference_number': row.reference_number,
            'source_reference': source_ref,
            'display_name': 'Customer Credit'
        })

    pagination_data = {
        'page': page,
        'per_page': per_page,
        'total_pages': (total_items + per_page - 1) // per_page,
        'total_items': total_items,
        'has_next': page < ((total_items + per_page - 1) // per_page),
        'has_prev': page > 1
    }

    return result, pagination_data


def manage_customer_credits(db_session, source, action='create', current_user=None, **kwargs):
    """
    Unified function to create, update, or reverse customer credits.
    If any step fails, the entire operation is rolled back.

    Args:
        db_session: Database session
        source: Source object (BulkPayment, PaymentAllocation, SalesTransaction, etc.)
        action: 'create', 'reverse', 'update', or 'delete'
        **kwargs: Additional parameters

    Returns:
        tuple: (success, message, credit_object or stats)

    Raises:
        Exception: On failure - caller should handle rollback
    """
    app_id = kwargs.get('app_id', current_user.app_id)

    try:
        # ===== CREATE CREDIT =====
        if action == 'create':
            amount = kwargs.get('amount')
            currency_id = kwargs.get('currency_id')
            reason = kwargs.get('reason', 'overpayment')
            exchange_rate_id = kwargs.get('exchange_rate_id')
            reference = kwargs.get('reference')
            customer_id = kwargs.get('customer_id')
            funding_account = kwargs.get('funding_account')
            payment_allocation = kwargs.get('payment_allocation')
            project_id = kwargs.get('project_id')

            # Validate required fields
            if not amount:
                raise ValueError("Amount is required for credit creation")
            if not currency_id:
                raise ValueError("Currency ID is required for credit creation")
            if not customer_id and not hasattr(source, 'customer_id') and not hasattr(source, 'customer'):
                raise ValueError("Customer ID is required for credit creation")

            # Determine source type and get customer
            if hasattr(source, 'customer_id'):
                customer_id = source.customer_id
            elif hasattr(source, 'customer'):
                customer_id = source.customer.id

            # Create credit record
            credit = CustomerCredit(
                app_id=app_id,
                customer_id=customer_id,
                original_amount=amount,
                available_amount=amount,
                currency_id=currency_id,
                exchange_rate_id=exchange_rate_id,
                created_date=datetime.now(),
                issued_date=kwargs.get('issued_date', datetime.now().date()),
                status='active',
                credit_reason=reason,
                reference_number=reference,
                created_by=current_user.id if current_user else None
            )

            # Link to source based on type
            if isinstance(source, BulkPayment):
                credit.bulk_payment_id = source.id
            elif isinstance(source, PaymentAllocation):
                credit.payment_allocation_id = source.id
                payment_allocation = source  # Use this for ledger posting
            elif isinstance(source, SalesTransaction):
                allocation = source.payment_allocations[0] if source.payment_allocations else None
                if allocation:
                    credit.payment_allocation_id = allocation.id
                    payment_allocation = allocation
                else:
                    raise ValueError(f"SalesTransaction {source.id} has no payment allocation")

            db_session.add(credit)
            db_session.flush()

            # ===== POST TO LEDGER USING EXISTING FUNCTION =====
            if payment_allocation and reason == 'overpayment':
                # Get or create customer credit account
                customer_credit_account_id = get_or_create_customer_credit_account(
                    db_session=db_session,
                    app_id=app_id,
                    created_by_user_id=current_user.id if current_user else None
                )

                if not customer_credit_account_id:
                    raise ValueError("Failed to get/create customer credit account")

                # Set overpayment amount on the allocation if not already set
                if not hasattr(payment_allocation, 'overpayment_amount') or not payment_allocation.overpayment_amount:
                    payment_allocation.overpayment_amount = amount

                # Post to ledger - this will raise exception on failure
                success, msg = post_customer_credit_to_ledger(
                    db_session=db_session,
                    payment_account_id=funding_account,
                    currency_id=currency_id,
                    customer_credit_account_id=customer_credit_account_id,
                    current_user=current_user,
                    payment_allocation=payment_allocation,
                    status='Posted',
                    project_id=project_id,
                    exchange_rate_id=exchange_rate_id
                )

                if not success:
                    raise ValueError(f"Failed to post credit to ledger: {msg}")

            logger.info(f"Created credit #{credit.id} for {amount} {currency_id} ({reason})")
            return True, "Credit created successfully", credit

        # ===== REVERSE CREDIT (when payment is cancelled/deleted) =====
        elif action == 'reverse':
            credit = kwargs.get('credit')
            if not credit:
                raise ValueError("No credit specified for reversal")

            stats = {
                'applications_reversed': 0,
                'affected_invoices': set()
            }

            # Delete journal entries for the credit
            success, msg = delete_journal_entries_by_source(
                db_session=db_session,
                source_type='customer_credit',
                source_id=credit.payment_allocation_id or credit.bulk_payment_id,
                app_id=app_id
            )
            if not success:
                raise ValueError(f"Failed to delete journal entries: {msg}")

            # Check if credit was partially used
            if credit.available_amount < credit.original_amount:
                # Find all applications
                applications = db_session.query(CreditApplication).filter_by(
                    credit_id=credit.id,
                    status='applied'
                ).all()

                for application in applications:
                    # Get the invoice where credit was applied
                    if application.payment_allocation and application.payment_allocation.sales_transaction:
                        invoice_id = application.payment_allocation.sales_transaction.invoice_id
                        stats['affected_invoices'].add(invoice_id)

                    # Delete journal entries for this application
                    if application.payment_allocation_id:
                        success, msg = delete_journal_entries_by_source(
                            db_session=db_session,
                            source_type='customer_credit',
                            source_id=application.payment_allocation_id,
                            app_id=app_id
                        )
                        if not success:
                            raise ValueError(f"Failed to delete application journal entries: {msg}")

                    # Delete the application
                    db_session.delete(application)
                    stats['applications_reversed'] += 1

            # Delete the credit
            db_session.delete(credit)

            logger.info(f"Reversed credit #{credit.id}")
            return True, "Credit reversed successfully", stats

        # ===== UPDATE CREDIT (when payment amount changes) =====
        elif action == 'update':
            credit = kwargs.get('credit')
            new_amount = kwargs.get('new_amount')
            old_amount = kwargs.get('old_amount')
            payment_allocation = kwargs.get('payment_allocation')
            project_id = kwargs.get('project_id')

            if not credit:
                raise ValueError("No credit specified for update")
            if new_amount is None:
                raise ValueError("New amount is required for update")
            if old_amount is None:
                raise ValueError("Old amount is required for update")

            # Calculate difference
            difference = new_amount - old_amount

            if abs(difference) < 0.01:  # No meaningful change
                return True, "No change in credit amount", credit

            if difference > 0:
                # Credit increased
                credit.original_amount += difference
                credit.available_amount += difference

                # Post adjustment to ledger
                if payment_allocation:
                    customer_credit_account_id = get_or_create_customer_credit_account(
                        db_session=db_session,
                        app_id=app_id,
                        created_by_user_id=current_user.id if current_user else None
                    )

                    if not customer_credit_account_id:
                        raise ValueError("Failed to get/create customer credit account")

                    # Create adjustment journal entry
                    journal_number = generate_unique_journal_number(db_session, app_id)

                    lines = [{
                        "subcategory_id": payment_allocation.payment_account,
                        "amount": float(difference),
                        "dr_cr": "D",
                        "description": f"Credit increase adjustment",
                        "source_type": "credit_adjustment",
                        "source_id": credit.id
                    }, {
                        "subcategory_id": customer_credit_account_id,
                        "amount": float(difference),
                        "dr_cr": "C",
                        "description": f"Credit increase adjustment",
                        "source_type": "credit_adjustment",
                        "source_id": credit.id
                    }]

                    create_transaction(
                        db_session=db_session,
                        date=datetime.now().date(),
                        currency=credit.currency_id,
                        created_by=current_user.id if current_user else None,
                        app_id=app_id,
                        journal_number=journal_number,
                        journal_ref_no=f"ADJ-{credit.id}",
                        narration=f"Credit adjustment - increase by {difference}",
                        payment_mode_id=None,
                        project_id=project_id,
                        vendor_id=credit.customer_id,
                        exchange_rate_id=credit.exchange_rate_id,
                        status='Posted',
                        lines=lines
                    )

            elif difference < 0:
                # Credit decreased
                abs_diff = abs(difference)
                if credit.available_amount < abs_diff:
                    raise ValueError(
                        f"Cannot reduce credit below used amount. Available: {credit.available_amount}, Reduction: {abs_diff}")

                credit.original_amount -= abs_diff
                credit.available_amount -= abs_diff

                # Reverse the difference
                if payment_allocation:
                    customer_credit_account_id = get_or_create_customer_credit_account(
                        db_session=db_session,
                        app_id=app_id,
                        created_by_user_id=current_user.id if current_user else None
                    )

                    if not customer_credit_account_id:
                        raise ValueError("Failed to get/create customer credit account")

                    journal_number = generate_unique_journal_number(db_session, app_id)

                    lines = [{
                        "subcategory_id": customer_credit_account_id,
                        "amount": float(abs_diff),
                        "dr_cr": "D",
                        "description": f"Credit decrease adjustment",
                        "source_type": "credit_adjustment",
                        "source_id": credit.id
                    }, {
                        "subcategory_id": payment_allocation.payment_account,
                        "amount": float(abs_diff),
                        "dr_cr": "C",
                        "description": f"Credit decrease adjustment",
                        "source_type": "credit_adjustment",
                        "source_id": credit.id
                    }]

                    create_transaction(
                        db_session=db_session,
                        date=datetime.now().date(),
                        currency=credit.currency_id,
                        created_by=current_user.id if current_user else None,
                        app_id=app_id,
                        journal_number=journal_number,
                        journal_ref_no=f"ADJ-{credit.id}",
                        narration=f"Credit adjustment - decrease by {abs_diff}",
                        payment_mode_id=None,
                        project_id=project_id,
                        vendor_id=credit.customer_id,
                        exchange_rate_id=credit.exchange_rate_id,
                        status='Posted',
                        lines=lines
                    )

            logger.info(f"Updated credit #{credit.id}: {old_amount} -> {new_amount}")
            return True, "Credit updated successfully", credit

        # ===== DELETE CREDIT (force delete even if used) =====
        # ===== DELETE CREDIT (force delete even if used) =====
        # ===== DELETE CREDIT (force delete even if used) =====
        elif action == 'delete':
            credit = kwargs.get('credit')
            force = kwargs.get('force', False)

            if not credit:
                raise ValueError("No credit specified for deletion")

            stats = {
                'applications_deleted': 0,
                'transactions_deleted': 0,
                'allocations_deleted': 0
            }

            # Check if credit was used
            if credit.available_amount < credit.original_amount and not force:
                raise ValueError(
                    f"Credit has been partially used ({credit.available_amount}/{credit.original_amount}). "
                    f"Use force=True to delete anyway."
                )

            # Delete journal entries for the credit itself
            delete_journal_entries_by_source(
                db_session=db_session,
                source_type='customer_credit',
                source_id=credit.payment_allocation_id or credit.bulk_payment_id,
                app_id=app_id
            )

            # Find and delete all applications
            applications = db_session.query(CreditApplication).filter_by(
                credit_id=credit.id
            ).all()

            for application in applications:
                # Delete journal entries for this application
                if application.payment_allocation_id:
                    delete_journal_entries_by_source(
                        db_session=db_session,
                        source_type='credit_application',
                        source_id=application.payment_allocation_id,
                        app_id=app_id
                    )

                # ✅ DELETE THE PAYMENT ALLOCATION AND TRANSACTION
                if application.payment_allocation:
                    payment_allocation = application.payment_allocation

                    # Get the transaction before deleting allocation
                    if payment_allocation.sales_transaction:
                        transaction = payment_allocation.sales_transaction

                        # Delete the transaction
                        db_session.delete(transaction)
                        stats['transactions_deleted'] += 1
                        logger.info(f"Deleted transaction #{transaction.id}")

                    # Delete the allocation
                    db_session.delete(payment_allocation)
                    stats['allocations_deleted'] += 1
                    logger.info(f"Deleted payment allocation #{payment_allocation.id}")

                # Delete the application
                db_session.delete(application)
                stats['applications_deleted'] += 1

            # Delete the credit
            db_session.delete(credit)

            logger.info(f"Deleted credit #{credit.id} with {stats['applications_deleted']} applications, "
                        f"{stats['transactions_deleted']} transactions, {stats['allocations_deleted']} allocations")
            return True, "Credit deleted successfully", stats
        # Add this to your manage_customer_credits function
        elif action == 'write_off':
            """
            Write off a portion of a customer credit.
            
            Required kwargs:
                credit: CustomerCredit object
                amount: Amount to write off
                write_off_account_id: ID of write-off income account
                reason: Reason for write-off
                notes: Additional notes
            """
            credit = kwargs.get('credit')
            amount = kwargs.get('amount')
            write_off_account_id = kwargs.get('write_off_account_id')
            reason = kwargs.get('reason', 'write_off')
            notes = kwargs.get('notes', '')

            if not credit or not amount:
                raise ValueError("Credit and amount required for write-off")

            if credit.available_amount < amount:
                raise ValueError(f"Insufficient available amount. Available: {credit.available_amount}")

            stats = {
                'credit_id': credit.id,
                'amount_written_off': amount,
                'new_available': credit.available_amount - amount
            }

            # Get customer credit account for ledger posting
            customer_credit_account_id = get_or_create_customer_credit_account(
                db_session=db_session,
                app_id=app_id,
                created_by_user_id=current_user.id if current_user else None
            )

            # Post write-off to ledger
            journal_number = generate_unique_journal_number(db_session, app_id)

            lines = [
                {
                    "subcategory_id": customer_credit_account_id,
                    "amount": float(amount),
                    "dr_cr": "D",  # Debit the liability (reduce it)
                    "description": f"Credit write-off",
                    "source_type": "credit_write_off",
                    "source_id": credit.id
                },
                {
                    "subcategory_id": write_off_account_id,
                    "amount": float(amount),
                    "dr_cr": "C",  # Credit income account
                    "description": f"Credit write-off income",
                    "source_type": "credit_write_off",
                    "source_id": credit.id
                }
            ]

            create_transaction(
                db_session=db_session,
                date=datetime.now().date(),
                currency=credit.currency_id,
                created_by=current_user.id if current_user else None,
                app_id=app_id,
                journal_number=journal_number,
                journal_ref_no=f"WO-{credit.id}",
                narration=f"Credit #{credit.id} write-off",
                payment_mode_id=None,
                project_id=kwargs.get('project_id'),
                vendor_id=credit.customer_id,
                exchange_rate_id=credit.exchange_rate_id,
                status='Posted',
                lines=lines
            )

            # Update credit
            credit.available_amount -= amount
            if credit.available_amount <= 0:
                credit.status = 'used'
            elif credit.available_amount < credit.original_amount:
                credit.status = 'partially_used'

            # Add note
            if not credit.notes:
                credit.notes = ""
            credit.notes += f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Written off {amount} {credit.currency.user_currency}"

            logger.info(f"Written off {amount} from credit #{credit.id}")
            return True, f"Written off {amount} from credit", stats

        else:
            raise ValueError(f"Unknown action: {action}")

    except Exception as e:
        logger.error(f"Error in manage_customer_credits ({action}): {str(e)}\n{traceback.format_exc()}")
        db_session.rollback()
        return False, str(e), None


@sales_bp.route('/api/credit/write-off', methods=['POST'])
@login_required
def write_off_credit():
    """
    Write off a customer credit (partial or full)
    Uses manage_customer_credits with 'write_off' action
    """
    db_session = Session()
    try:
        data = request.get_json()
        credit_id = data.get('credit_id')
        amount = Decimal(str(data.get('amount', 0)))
        reason = data.get('reason', 'write_off')
        notes = data.get('notes', '')

        if not credit_id or not amount or amount <= 0:
            return jsonify({'success': False, 'message': 'Credit ID and amount are required'}), 400

        # Fetch the credit
        credit = db_session.query(CustomerCredit).filter_by(
            id=credit_id,
            app_id=current_user.app_id
        ).first()

        if not credit:
            return jsonify({'success': False, 'message': 'Credit not found'}), 404

        if credit.available_amount < amount:
            return jsonify({'success': False,
                            'message': f'Insufficient available amount. Available: {credit.available_amount}'}), 400

        if credit.status not in ['active', 'partially_used']:
            return jsonify({'success': False, 'message': f'Cannot write off credit with status: {credit.status}'}), 400

        # Get or create write-off account
        write_off_account_id = get_or_create_write_off_account(
            db_session=db_session,
            app_id=current_user.app_id,
            created_by_user_id=current_user.id
        )

        # ===== USE MANAGE_CUSTOMER_CREDITS WITH WRITE_OFF ACTION =====
        success, msg, stats = manage_customer_credits(
            db_session=db_session,
            source=credit,  # Source is the credit itself
            action='write_off',
            credit=credit,
            amount=amount,
            write_off_account_id=write_off_account_id,
            reason=reason,
            notes=notes,
            current_user=current_user
        )

        if not success:
            db_session.rollback()
            return jsonify({'success': False, 'message': msg}), 400

        db_session.commit()

        return jsonify({
            'success': True,
            'message': f'Successfully wrote off {amount} {credit.currency.user_currency}',
            'stats': stats
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error writing off credit: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


@sales_bp.route('/credit/<int:credit_id>')
@login_required
def credit_detail(credit_id):
    """View details of a customer credit"""
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Fetch credit with ALL related data eagerly loaded
        credit = db_session.query(CustomerCredit).options(
            # Eager load credit applications
            joinedload(CustomerCredit.credit_applications)
            .joinedload(CreditApplication.payment_allocation)
            .joinedload(PaymentAllocation.sales_transaction)
            .joinedload(SalesTransaction.invoice),
            # Also load the customer
            joinedload(CustomerCredit.customer),
            # Load currency and exchange rate
            joinedload(CustomerCredit.currency),
            joinedload(CustomerCredit.exchange_rate),
            # Load creator
            joinedload(CustomerCredit.creator)
        ).filter(
            CustomerCredit.id == credit_id,
            CustomerCredit.app_id == app_id
        ).first()

        if not credit:
            flash('Credit not found.', 'error')
            return redirect(url_for('sales.view_customer_credits'))

        # Get company info
        company = db_session.query(Company).filter_by(id=app_id).first()
        base_currency = company.base_currency
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        return render_template(
            'sales/customer_credit_detail.html',
            credit=credit,
            company=company,
            base_currency=base_currency,
            modules=modules_data,
            role=role
        )

    except Exception as e:
        logger.error(f"Error viewing credit {credit_id}: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading credit details.', 'error')
        return redirect(url_for('sales.view_customer_credits'))
    finally:
        db_session.close()


# @sales_bp.route('/api/credit/apply-auto', methods=['POST'])
# @login_required
# def apply_credit_auto():
#     """
#     Automatically apply a customer credit to outstanding invoices (oldest first)
#     Journal entries are posted in the credit's original currency
#     """
#     db_session = Session()
#     try:
#         data = request.get_json()
#
#         credit_id = data.get('credit_id')
#         application_date = datetime.strptime(data.get('application_date', datetime.now().strftime('%Y-%m-%d')),
#                                              '%Y-%m-%d')
#         reference = data.get('reference', '')
#         project_id = data.get('project_id')
#         base_currency_id = int(data.get('base_currency_id'))
#
#         # Fetch the credit
#         credit = db_session.query(CustomerCredit).filter_by(
#             id=credit_id,
#             app_id=current_user.app_id
#         ).first()
#
#         if not credit:
#             return jsonify({'success': False, 'message': 'Credit not found'}), 404
#
#         if credit.available_amount <= 0:
#             return jsonify({'success': False, 'message': 'Credit has no available amount'}), 400
#
#         if credit.status not in ['active', 'partially_used']:
#             return jsonify({'success': False, 'message': f'Cannot apply credit with status: {credit.status}'}), 400
#
#         # ===== GET OUTSTANDING INVOICES =====
#         outstanding_invoices = db_session.query(SalesInvoice).filter(
#             SalesInvoice.customer_id == credit.customer_id,
#             SalesInvoice.app_id == current_user.app_id,
#             SalesInvoice.status.in_(['sent', 'overdue', 'partially_paid', 'unpaid'])
#         ).order_by(SalesInvoice.invoice_date.asc()).all()
#
#         if not outstanding_invoices:
#             return jsonify({'success': False, 'message': 'No outstanding invoices found for this customer'}), 400
#
#         # ===== PREPARE ALLOCATIONS =====
#         remaining_credit = credit.available_amount
#         remaining_in_base = float(remaining_credit * (credit.exchange_rate.rate if credit.exchange_rate else 1))
#
#         allocations = []
#         affected_invoices = set()
#         total_applied = Decimal('0.00')
#         total_applied_base = Decimal('0.00')
#
#         for invoice in outstanding_invoices:
#             if remaining_credit <= 0:
#                 break
#
#             # Calculate invoice balance in base currency
#             total_paid = db_session.query(func.coalesce(func.sum(SalesTransaction.amount_paid), 0)) \
#                              .filter(SalesTransaction.invoice_id == invoice.id) \
#                              .scalar() or Decimal('0.00')
#
#             balance_in_invoice_currency = invoice.total_amount - total_paid
#
#             if balance_in_invoice_currency <= 0:
#                 continue
#
#             # Convert invoice balance to base currency
#             if invoice.currency == base_currency_id:
#                 balance_in_base = float(balance_in_invoice_currency)
#                 invoice_rate = 1
#             elif invoice.exchange_rate:
#                 invoice_rate = float(invoice.exchange_rate.rate)
#                 balance_in_base = float(balance_in_invoice_currency) * invoice_rate
#             else:
#                 rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
#                     session=db_session,
#                     action='create',
#                     from_currency_id=invoice.currency,
#                     to_currency_id=base_currency_id,
#                     rate_date=application_date,
#                     app_id=current_user.app_id,
#                     source_type='credit_application',
#                     created_by=current_user.id
#                 )
#                 invoice_rate = float(rate_obj.rate) if rate_obj else 1
#                 balance_in_base = float(balance_in_invoice_currency) * invoice_rate
#
#             # Calculate amount to apply from credit
#             apply_amount_in_base = min(remaining_in_base, balance_in_base)
#
#             # Convert back to credit currency (this is what will be in journal)
#             if credit.currency_id == base_currency_id:
#                 apply_amount_in_credit = Decimal(str(apply_amount_in_base))
#             elif credit.exchange_rate:
#                 credit_rate = float(credit.exchange_rate.rate)
#                 apply_amount_in_credit = Decimal(str(apply_amount_in_base / credit_rate))
#             else:
#                 apply_amount_in_credit = Decimal(str(apply_amount_in_base))
#
#             # Convert to invoice currency for display
#             if invoice.currency == credit.currency_id:
#                 apply_amount_in_invoice = apply_amount_in_credit
#             else:
#                 apply_amount_in_invoice = Decimal(
#                     str(apply_amount_in_base / invoice_rate)) if invoice_rate else apply_amount_in_credit
#
#             # Round to 2 decimals
#             apply_amount_in_credit = apply_amount_in_credit.quantize(Decimal('0.01'))
#             apply_amount_in_base = Decimal(str(round(apply_amount_in_base, 2)))
#             apply_amount_in_invoice = apply_amount_in_invoice.quantize(Decimal('0.01'))
#
#             if apply_amount_in_credit <= 0:
#                 continue
#
#             allocations.append({
#                 'invoice': invoice,
#                 'amount_credit': apply_amount_in_credit,  # Amount in credit currency (for journal)
#                 'amount_base': apply_amount_in_base,  # Amount in base (for reference)
#                 'amount_invoice': apply_amount_in_invoice  # Amount in invoice currency (for tracking)
#             })
#
#             total_applied += apply_amount_in_credit
#             total_applied_base += apply_amount_in_base
#             remaining_credit -= apply_amount_in_credit
#             remaining_in_base -= float(apply_amount_in_base)
#             affected_invoices.add(invoice.id)
#
#         if not allocations:
#             return jsonify({'success': False, 'message': 'Could not allocate credit to any invoices'}), 400
#
#         # ===== CREATE CREDIT APPLICATIONS =====
#         credit_applications = []
#
#         for alloc in allocations:
#             invoice = alloc['invoice']
#
#             # Create a sales transaction for tracking
#             transaction = SalesTransaction(
#                 invoice_id=invoice.id,
#                 customer_id=credit.customer_id,
#                 payment_date=application_date,
#                 amount_paid=alloc['amount_invoice'],  # Amount in invoice currency
#                 currency_id=invoice.currency,
#                 reference_number=reference or f"Credit #{credit.id}",
#                 is_posted_to_ledger=True,
#                 payment_status='paid' if alloc['amount_invoice'] >= invoice.total_amount else 'partial',
#                 created_by=current_user.id,
#                 app_id=current_user.app_id,
#                 bulk_payment_id=None
#             )
#             db_session.add(transaction)
#             db_session.flush()
#
#             # Create payment allocation
#             payment_allocation = PaymentAllocation(
#                 payment_id=transaction.id,
#                 payment_date=application_date,
#                 invoice_id=invoice.id,
#                 allocated_base_amount=alloc['amount_base'],
#                 allocated_tax_amount=0,
#                 payment_account=None,
#                 credit_sale_account=None,
#                 payment_type='credit',
#                 is_posted_to_ledger=True,
#                 reference=reference or f"Credit #{credit.id} applied",
#                 exchange_rate_id=credit.exchange_rate_id,  # Store credit's rate
#                 app_id=current_user.app_id
#             )
#             db_session.add(payment_allocation)
#             db_session.flush()
#
#             # Create credit application record
#             credit_application = CreditApplication(
#                 credit_id=credit.id,
#                 payment_allocation_id=payment_allocation.id,
#                 target_type='sales_invoice',
#                 target_id=invoice.id,
#                 applied_amount=alloc['amount_credit'],  # Amount in credit currency
#                 applied_amount_invoice_currency=alloc['amount_invoice'],
#                 application_date=application_date,
#                 status='applied',
#                 applied_by=current_user.id,
#                 app_id=current_user.app_id
#             )
#             db_session.add(credit_application)
#             db_session.flush()
#             credit_applications.append(credit_application)
#
#         # ===== UPDATE CREDIT =====
#         credit.available_amount -= total_applied
#         update_credit_status(credit)
#
#         # Add note
#         if not credit.notes:
#             credit.notes = ""
#         credit.notes += f"\n[{application_date.strftime('%Y-%m-%d %H:%M')}] Automatically applied {total_applied} {credit.currency.user_currency} to {len(allocations)} invoice(s)"
#
#         # ===== POST TO LEDGER (IN CREDIT'S ORIGINAL CURRENCY) =====
#         customer_credit_account_id = get_or_create_customer_credit_account(
#             db_session=db_session,
#             app_id=current_user.app_id,
#             created_by_user_id=current_user.id
#         )
#
#         journal_number = generate_unique_journal_number(db_session, current_user.app_id)
#
#         lines = []
#
#         # Credit side - reduce AR for each invoice
#         for i, alloc in enumerate(allocations):
#             # Get the AR account for this invoice
#             ar_account_id = alloc['invoice'].account_receivable_id
#
#             lines.append({
#                 "subcategory_id": ar_account_id,
#                 "amount": float(alloc['amount_credit']),  # ✅ Amount in credit currency
#                 "dr_cr": "C",
#                 "description": f"Credit applied to Invoice #{alloc['invoice'].invoice_number}",
#                 "source_type": "credit_application",
#                 "source_id": credit_applications[i].id
#             })
#
#         # Debit side - reduce credit liability
#         lines.append({
#             "subcategory_id": customer_credit_account_id,
#             "amount": float(total_applied),  # ✅ Total in credit currency
#             "dr_cr": "D",
#             "description": f"Credit #{credit.id} applied to invoices",
#             "source_type": "credit_application",
#             "source_id": credit.id
#         })
#
#         # Create the journal entry in CREDIT currency
#         create_transaction(
#             db_session=db_session,
#             date=application_date,
#             currency=credit.currency_id,  # ✅ Journal in credit's original currency
#             created_by=current_user.id,
#             app_id=current_user.app_id,
#             journal_number=journal_number,
#             journal_ref_no=f"CA-{credit.id}-{application_date.strftime('%Y%m%d')}",
#             narration=f"Credit #{credit.id} automatically applied to {len(allocations)} invoice(s)",
#             payment_mode_id=None,
#             project_id=project_id,
#             vendor_id=credit.customer_id,
#             exchange_rate_id=credit.exchange_rate_id,  # ✅ Store rate for base conversion
#             status='Posted',
#             lines=lines
#         )
#
#         # Update invoice statuses
#         for invoice_id in affected_invoices:
#             update_invoice_status(db_session, invoice_id)
#
#         db_session.commit()
#
#         return jsonify({
#             'success': True,
#             'message': f'Successfully applied {total_applied} {credit.currency.user_currency} to {len(allocations)} invoice(s)',
#             'credit_id': credit.id,
#             'new_available': float(credit.available_amount),
#             'credit_status': credit.status,
#             'applications_count': len(credit_applications),
#             'allocations': [{
#                 'invoice_number': a['invoice'].invoice_number,
#                 'amount': float(a['amount_credit']),
#                 'currency': credit.currency.user_currency
#             } for a in allocations]
#         })
#
#     except Exception as e:
#         db_session.rollback()
#         logger.error(f"Error auto-applying credit: {str(e)}\n{traceback.format_exc()}")
#         return jsonify({'success': False, 'message': str(e)}), 500
#     finally:
#         db_session.close()


@sales_bp.route('/api/credit/apply-auto', methods=['POST'])
@login_required
def apply_credit_auto():
    """
    Automatically apply a customer credit to outstanding invoices (oldest first)
    Journal entries are posted in the credit's original currency
    """
    db_session = Session()
    try:
        data = request.get_json()
        logger.info(f"Front end data sent is {data}")

        credit_id = data.get('credit_id')
        application_date = datetime.strptime(data.get('application_date', datetime.now().strftime('%Y-%m-%d')),
                                             '%Y-%m-%d')
        reference = data.get('reference', '')
        project_id = data.get('project_id')
        base_currency_id = int(data.get('base_currency_id'))
        allocations_data = data.get('allocations', [])  # Array of {invoice_id, amount_credit}

        # Fetch the credit
        credit = db_session.query(CustomerCredit).filter_by(
            id=credit_id,
            app_id=current_user.app_id
        ).first()

        if not credit:
            return jsonify({'success': False, 'message': 'Credit not found'}), 404

        if credit.available_amount <= 0:
            return jsonify({'success': False, 'message': 'Credit has no available amount'}), 400

        if credit.status not in ['active', 'partially_used']:
            return jsonify({'success': False, 'message': f'Cannot apply credit with status: {credit.status}'}), 400

        # ===== GET SYSTEM ACCOUNTS =====
        system_accounts = get_all_system_accounts(
            db_session=db_session,
            app_id=current_user.app_id,
            created_by_user_id=current_user.id
        )

        suspense_account_id = system_accounts['suspense']
        fx_gain_loss_account_id = system_accounts['fx_gain_loss']
        customer_credit_account_id = system_accounts.get('customer_credit')

        # Get credit rate
        credit_rate = float(credit.exchange_rate.rate) if credit.exchange_rate else 1

        # ===== PROCESS ALLOCATIONS =====
        credit_applications = []
        affected_invoices = set()
        total_applied = Decimal('0.00')

        post_data = []  # Store data for bulk posting

        for alloc_data in allocations_data:
            invoice_id = alloc_data['invoice_id']
            amount_credit = float(alloc_data['amount'])

            # Get the invoice
            invoice = db_session.query(SalesInvoice).get(invoice_id)
            if not invoice:
                continue

            # Calculate remaining balance
            total_paid = db_session.query(func.coalesce(func.sum(SalesTransaction.amount_paid), 0)) \
                             .filter(SalesTransaction.invoice_id == invoice.id) \
                             .scalar() or Decimal('0.00')
            remaining_balance = invoice.total_amount - total_paid

            # Convert credit amount to invoice currency
            invoice_rate = float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1

            # Calculate maximum credit that can be applied to this invoice
            invoice_balance_in_base = float(remaining_balance) * invoice_rate  # 15,000 UGX
            max_credit_in_base = min(invoice_balance_in_base, float(credit.available_amount) * credit_rate)
            amount_credit = max_credit_in_base / credit_rate  # 4.2857 USD
            amount_in_invoice = max_credit_in_base / invoice_rate  # 15,000 UGX
            amount_in_invoice = round(amount_in_invoice, 2)
            # Create sales transaction
            transaction = SalesTransaction(
                invoice_id=invoice.id,
                customer_id=credit.customer_id,
                payment_date=application_date,
                amount_paid=Decimal(str(amount_in_invoice)),
                currency_id=invoice.currency,
                reference_number=reference or f"Credit #{credit.id}",
                is_posted_to_ledger=True,
                payment_status='paid' if amount_in_invoice >= float(invoice.total_amount - Decimal('0.01')) else 'partial',
                created_by=current_user.id,
                app_id=current_user.app_id,
                bulk_payment_id=None
            )
            db_session.add(transaction)
            db_session.flush()

            # USE THE ALLOCATE_PAYMENT FUNCTION
            payment_allocation = allocate_payment(
                sale_transaction_id=transaction.id,
                invoice_id=invoice.id,
                payment_date=application_date,
                payment_amount=Decimal(str(amount_in_invoice)),
                remaining_balance=remaining_balance,
                db_session=db_session,
                payment_mode=None,  # No payment mode for credit
                total_tax_amount=invoice.total_tax_amount,
                payment_account=None,  # No payment account for credit
                tax_payable_account_id=None,  # Set this if you have a tax payable account
                credit_sale_account=None,
                reference=reference or f"Credit #{credit.id} applied",
                overpayment_amount=Decimal('0.00'),
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
                applied_amount=Decimal(str(amount_credit)),
                applied_amount_invoice_currency=Decimal(str(amount_in_invoice)),
                application_date=application_date,
                status='applied',
                applied_by=current_user.id,
                app_id=current_user.app_id
            )
            db_session.add(credit_application)
            db_session.flush()
            post_data.append({
                'payment_allocation': payment_allocation,
                'invoice': invoice,
                'amount_in_credit': amount_credit,
                'amount_in_invoice': amount_in_invoice
            })
            credit_applications.append(credit_application)
            total_applied += Decimal(str(amount_credit))
            affected_invoices.add(invoice.id)

            if not credit_applications:
                return jsonify({'success': False, 'message': 'Could not allocate credit to any invoices'}), 400


        # Post all at once after the loop
        for data in post_data:
            success, msg = post_customer_credit_to_invoice(
                db_session=db_session,
                payment_allocation=data['payment_allocation'],
                credit=credit,
                invoice=data['invoice'],
                amount_in_credit=data['amount_in_credit'],
                amount_in_invoice=data['amount_in_invoice'],
                current_user=current_user,
                suspense_account_id=suspense_account_id,
                customer_credit_account_id=customer_credit_account_id,
                fx_gain_loss_account_id=fx_gain_loss_account_id,
                status='Posted',
                project_id=project_id
            )


            if not success:
                logger.error(f"Failed to post credit application to ledger: {msg}")
                db_session.rollback()
                return jsonify({'success': False, 'message': msg}), 500


        # ===== UPDATE CREDIT =====
        credit.available_amount -= total_applied
        update_credit_status(credit)

        # Add note
        if not credit.notes:
            credit.notes = ""
        credit.notes += f"\n[{application_date.strftime('%Y-%m-%d %H:%M')}] Automatically applied {total_applied} {credit.currency.user_currency} to {len(credit_applications)} invoice(s)"

        # Update invoice statuses
        for invoice_id in affected_invoices:
            update_invoice_status(db_session, invoice_id)

        db_session.commit()

        return jsonify({
            'success': True,
            'message': f'Successfully applied {total_applied} {credit.currency.user_currency} to {len(credit_applications)} invoice(s)',
            'credit_id': credit.id,
            'new_available': float(credit.available_amount),
            'credit_status': credit.status,
            'applications_count': len(credit_applications)
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error auto-applying credit: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


@sales_bp.route('/apply-credit/<int:credit_id>')
@login_required
def apply_credit_page(credit_id):
    """
    Render page for applying a credit to outstanding invoices
    """
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Fetch the credit with all related data
        credit = db_session.query(CustomerCredit).filter(
            CustomerCredit.id == credit_id,
            CustomerCredit.app_id == app_id
        ).first()

        if not credit:
            flash('Credit not found.', 'error')
            return redirect(url_for('sales.view_customer_credits'))

        # Check if credit can be applied
        if credit.available_amount <= 0:
            flash('This credit has no available amount to apply.', 'warning')
            return redirect(url_for('sales.view_customer_credits'))

        if credit.status not in ['active', 'partially_used']:
            flash(f'Cannot apply credit with status: {credit.status}', 'warning')
            return redirect(url_for('sales.view_customer_credits'))

        # Get company info
        company = db_session.query(Company).filter_by(id=app_id).first()
        base_currency = company.base_currency

        # Get projects for dropdown
        projects = db_session.query(Project).filter_by(app_id=app_id, is_active=True).all()

        # Get today's date for default
        today = datetime.now().strftime('%Y-%m-%d')

        # Get modules and role for template
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        role = current_user.role
        # 👇 ADD THIS - Get the credit exchange rate
        credit_rate = credit.exchange_rate.rate if credit.exchange_rate else 1

        return render_template(
            'sales/apply_credit.html',
            credit=credit,
            company=company,
            base_currency=base_currency,
            projects=projects,
            today=today,
            credit_rate=credit_rate,
            modules=modules_data,
            role=role
        )

    except Exception as e:
        logger.error(f"Error loading apply credit page for credit {credit_id}: {str(e)}\n{traceback.format_exc()}")
        flash('An error occurred while loading the page.', 'error')
        return redirect(url_for('sales.view_customer_credits'))
    finally:
        db_session.close()


def reverse_credit_application(db_session, transaction, current_user, action='cancel', payment_allocation=None):
    """
    Reverse a credit application when the associated sales transaction is cancelled or deleted.

    This function:
    1. Finds the credit application linked to this transaction
    2. Reverses the journal entries
    3. Restores the credit available amount
    4. Updates invoice status

    Args:
        db_session: Database session
        payment_allocation: Obj
        transaction: SalesTransaction object
        current_user: Current user object
        action: 'cancel' (keep payment allocation) or 'delete' (remove payment allocation)

    Returns:
        tuple: (success, message, stats)
    """
    try:
        stats = {
            'credit_application_reversed': None,
            'credit_restored': None,
            'amount_reversed': 0,
            'invoice_id': transaction.invoice_id,
            'action': action
        }

        # Find payment allocation for this transaction
        if not payment_allocation:
            payment_allocation = db_session.query(PaymentAllocation).filter_by(
                payment_id=transaction.id
            ).first()

            if not payment_allocation:
                return True, "No payment allocation found", stats

        # Find credit application linked to this payment allocation
        credit_application = db_session.query(CreditApplication).filter_by(
            payment_allocation_id=payment_allocation.id
        ).first()

        if not credit_application:
            return True, "No credit application found", stats

        # Get the associated credit
        credit = credit_application.customer_credit
        if not credit:
            return True, "No credit found", stats

        stats['credit_application_reversed'] = credit_application.id
        stats['amount_reversed'] = float(credit_application.applied_amount)

        # ===== REVERSE JOURNAL ENTRIES =====
        # Delete journal entries for this credit application
        delete_journal_entries_by_source(
            db_session=db_session,
            source_type='credit_application',
            source_id=credit_application.payment_allocation_id,
            app_id=current_user.app_id
        )

        # Also delete via sales_transaction if needed
        delete_journal_entries_by_source(
            db_session=db_session,
            source_type='credit_application',
            source_id=transaction.id,
            app_id=current_user.app_id
        )

        # ===== RESTORE CREDIT AVAILABLE AMOUNT =====
        credit.available_amount += credit_application.applied_amount

        # Update credit status
        update_credit_status(credit)

        # Add note to credit
        if not credit.notes:
            credit.notes = ""
        credit.notes += f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Reversed credit application #{credit_application.id} due to transaction {action}"

        stats['credit_restored'] = credit.id

        # ===== DELETE CREDIT APPLICATION (always remove) =====
        db_session.delete(credit_application)

        # ===== HANDLE PAYMENT ALLOCATION BASED ON ACTION =====
        if action == 'delete':
            # Permanently delete payment allocation
            db_session.delete(payment_allocation)
            logger.info(f"Deleted payment allocation #{payment_allocation.id}")
        else:  # action == 'cancel'
            # Keep payment allocation but mark it as cancelled/reversed
            payment_allocation.is_posted_to_ledger = False
            # Optionally add a note

        logger.info(
            f"Reversed credit application #{credit_application.id}, restored {credit_application.applied_amount} to credit #{credit.id}")

        return True, "Credit application reversed successfully", stats

    except Exception as e:
        logger.error(f"Error reversing credit application: {str(e)}\n{traceback.format_exc()}")
        return False, str(e), stats


def update_credit_status(credit):
    """
    Update credit status based on available amount using Decimal comparison

    Args:
        credit: CustomerCredit object with original_amount and available_amount as Decimal fields

    Returns:
        str: New status ('active', 'partially_used', or 'used')
    """
    from decimal import Decimal

    if credit.available_amount >= credit.original_amount - Decimal('0.01'):
        credit.status = 'active'
    elif credit.available_amount > Decimal('0'):
        credit.status = 'partially_used'
    else:
        credit.status = 'used'

    return credit.status
