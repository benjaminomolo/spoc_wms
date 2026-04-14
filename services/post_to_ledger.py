# app/services/post_to_ledger
import logging
import math
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from ai import resolve_exchange_rate_for_transaction, get_base_currency
from models import PayrollPayment, ChartOfAccounts, AdvanceRepayment, PayrollTransaction, AdvancePayment, \
    DeductionPayment, Deduction, InventoryItem, InventoryItemVariationLink, JournalEntry, Journal, Vendor, \
    DirectSalesTransaction, SalesTransaction, SalesInvoiceStatusLog, ExchangeRate, Currency, Employee, \
    DirectPurchaseTransaction, PurchaseTransaction, GoodsReceipt, PurchaseOrder, PurchasePaymentAllocation

from services.chart_of_accounts_helpers import get_retained_earnings_account_id, get_all_system_accounts
from services.vendors_and_customers import get_or_create_customer_credit_account

from utils import create_transaction, generate_unique_journal_number
import traceback

from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj, get_or_create_fx_clearing_account

logger = logging.getLogger(__name__)


def post_payroll_to_ledger(db_session, transaction_id, current_user, data):
    """
    Post payroll transaction entries to ledger.
    Designed to be called inside repost_transaction, no HTTP/Flask specifics here.

    :param db_session: SQLAlchemy session
    :param transaction_id: PayrollTransaction ID
    :param current_user: current user object with id and app_id
    :param data: dict containing, debitSubCategory, creditSubCategory
    """
    app_id = current_user.app_id

    # Load payroll transaction with relationships in one query
    payroll_transaction = (
        db_session.query(PayrollTransaction)
        .options(
            joinedload(PayrollTransaction.employees),
            joinedload(PayrollTransaction.payroll_period),
            joinedload(PayrollTransaction.payable_account)
        )
        .filter_by(id=transaction_id)
        .first()
    )

    if not payroll_transaction:
        raise ValueError("Payroll transaction not found")

    vendor_id = payroll_transaction.employees.payee_id if payroll_transaction.employees.payee_id else None
    payroll_period = payroll_transaction.payroll_period
    payroll_period_name = payroll_period.payroll_period_name
    end_date = payroll_period.end_date
    currency_id = payroll_transaction.currency_id
    employee_name = payroll_transaction.employees.first_name
    project_id = payroll_transaction.employees.project_id

    # Handle advance repayment deduction if any
    total_advance_deducted = 0.0
    advance_repayments = db_session.query(AdvanceRepayment).filter_by(
        payroll_transaction_id=transaction_id,
        app_id=app_id
    ).all()

    for repayment in advance_repayments:
        if not repayment.is_posted_to_ledger:
            advance_payment = repayment.advance_payments

            # Find the original advance payment journal entry
            prepaid_entry = db_session.query(JournalEntry).filter_by(
                app_id=app_id,
                source_type="advance_payment",
                source_id=advance_payment.id,
                dr_cr="D"
            ).first()

            if not prepaid_entry:
                raise ValueError(
                    f"Advance payment ledger entry not found for advance ID {advance_payment.id} to {employee_name}"
                )

            journal_number = generate_unique_journal_number(db_session, app_id)

            exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
                session=db_session,
                currency_id=currency_id,
                transaction_date=end_date,
                app_id=app_id
            )

            # Create journal with both entries for advance recovery
            journal, entries = create_transaction(
                db_session=db_session,
                date=end_date,
                currency=currency_id,
                created_by=current_user.id,
                app_id=app_id,
                journal_number=journal_number,
                project_id=project_id,
                narration=f"Advance recovery for {employee_name} - {payroll_period_name}",
                vendor_id=vendor_id,
                exchange_rate_id=exchange_rate_id,
                lines=[
                    # Credit prepaid salaries (reversing the original debit)
                    {
                        "subcategory_id": prepaid_entry.subcategory_id,
                        "amount": repayment.payment_amount,
                        "dr_cr": "C",
                        "description": f"Advance recovery for {employee_name} - {payroll_period_name}",
                        "source_type": "advance_repayment",
                        "source_id": repayment.id
                    },
                    # Debit expense account
                    {
                        "subcategory_id": int(data['debitSubCategory']),
                        "amount": repayment.payment_amount,
                        "dr_cr": "D",
                        "description": f"Salary for Payroll Period: {payroll_period_name} for {employee_name}",
                        "source_type": "payroll_transaction",
                        "source_id": transaction_id
                    }
                ]
            )

            repayment.is_posted_to_ledger = True
            total_advance_deducted += float(repayment.payment_amount)

    # Unposted payments for this payroll transaction
    unposted_payments = db_session.query(PayrollPayment).filter_by(
        payroll_transaction_id=transaction_id,
        is_posted_to_ledger=False
    ).all()

    coa = db_session.query(ChartOfAccounts).filter_by(app_id=app_id).all()
    coa_dict = {account.id: account for account in coa}

    credit_account_id = int(data['creditSubCategory']) if data.get('creditSubCategory') else None
    debit_account_id = int(data['debitSubCategory']) if data.get('debitSubCategory') else None

    # Get the category_fk for the selected accounts
    credit_category_account = coa_dict.get(credit_account_id).category_fk if credit_account_id and coa_dict.get(
        credit_account_id) else None
    debit_category_account = coa_dict.get(debit_account_id).category_fk if debit_account_id and coa_dict.get(
        debit_account_id) else None
    payable_account_id = None
    payable_category_id = None

    # 1 Handling cases where payable has not been recorded
    if credit_account_id and debit_account_id:
        accrual_journal = generate_unique_journal_number(db_session, app_id)
        amount_payable = payroll_transaction.net_salary

        exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
            session=db_session,
            currency_id=currency_id,
            transaction_date=end_date,
            app_id=app_id
        )

        # Create accrual journal for salary expense and liability
        journal, entries = create_transaction(
            db_session=db_session,
            date=end_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=accrual_journal,
            narration=f"Salary accrual for {employee_name} - {payroll_period_name}",
            vendor_id=vendor_id,
            project_id=project_id,
            exchange_rate_id=exchange_rate_id,
            lines=[
                # Debit Expense account
                {
                    "subcategory_id": debit_account_id,
                    "amount": amount_payable,
                    "dr_cr": "D",
                    "description": f"Salaries for Payroll Period: {payroll_period_name} for {employee_name}",
                    "source_type": "payroll_transaction",
                    "source_id": transaction_id
                },
                # Credit Liability account
                {
                    "subcategory_id": credit_account_id,
                    "amount": amount_payable,
                    "dr_cr": "C",
                    "description": f"Unpaid Salaries for Payroll Period: {payroll_period_name} for {employee_name}",
                    "source_type": "payroll_transaction",
                    "source_id": transaction_id
                }
            ]
        )

        payroll_transaction.payable_account_id = credit_account_id

    elif credit_account_id and not debit_account_id:
        payable_account_id = credit_account_id
        payable_category_id = coa_dict.get(credit_account_id).category_fk if credit_account_id and coa_dict.get(
            credit_account_id) else None
        payroll_transaction.payable_account_id = payable_account_id

    else:
        payable_account = payroll_transaction.payable_account or coa_dict.get(credit_account_id)
        payable_account_id = payable_account.id
        payable_category_id = payable_account.category_fk

    # 2. Handle actual cash/bank payments
    for payroll_payment in unposted_payments:
        journal_number = generate_unique_journal_number(db_session, app_id)
        amount = float(payroll_payment.amount)
        payment_date = payroll_payment.payment_date

        exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
            session=db_session,
            currency_id=currency_id,
            transaction_date=payment_date,
            app_id=app_id
        )

        if not payable_account_id:
            raise ValueError("No payable account configured for this transaction")

        # Determine description
        user_description_parts = []
        if payroll_payment.reference:
            user_description_parts.append(payroll_payment.reference)
        if payroll_payment.notes:
            user_description_parts.append(payroll_payment.notes)

        # If user provided reference/notes, use them; else generate description
        description = " ".join(user_description_parts) if user_description_parts else \
            f"Payroll payment for {payroll_period_name} for {employee_name}"

        # Create payment journal
        journal, entries = create_transaction(
            db_session=db_session,
            date=payment_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal_number,
            narration=description,
            payment_mode_id=payroll_payment.payment_method,
            project_id=project_id,
            vendor_id=vendor_id,
            exchange_rate_id=exchange_rate_id,
            lines=[
                # Credit cash/bank (Asset)
                {
                    "subcategory_id": payroll_payment.payment_account,
                    "amount": amount,
                    "dr_cr": "C",
                    "description": description,
                    "source_type": "payroll_payment",
                    "source_id": payroll_payment.id
                },
                # Debit liability
                {
                    "subcategory_id": payable_account_id,
                    "amount": amount,
                    "dr_cr": "D",
                    "description": f"Reducing unpaid salaries for {payroll_period_name} for {employee_name}",
                    "source_type": "payroll_payment",
                    "source_id": payroll_payment.id
                }
            ]
        )

        payroll_payment.is_posted_to_ledger = True

    # Mark transaction posted if no unposted payments remain
    remaining = db_session.query(PayrollPayment).filter_by(
        payroll_transaction_id=transaction_id,
        is_posted_to_ledger=False
    ).count()

    if remaining == 0:
        payroll_transaction.is_posted_to_ledger = True


def post_payroll_payment_to_ledger(db_session, payment_id=None, current_user=None, data=None, transaction_id=None):
    """
    Post a single payroll payment to ledger
    """
    app_id = current_user.app_id

    # Load payment with related data
    payment = db_session.query(PayrollPayment).options(
        joinedload(PayrollPayment.payroll_transactions)  # Load the transaction
        .joinedload(PayrollTransaction.employees),  # Load employees from transaction
        joinedload(PayrollPayment.payroll_transactions)  # Load transaction again
        .joinedload(PayrollTransaction.payroll_period),  # Load period from transaction
        joinedload(PayrollPayment.payroll_transactions)  # Load transaction again
        .joinedload(PayrollTransaction.payable_account)  # Load account from transaction
    ).filter_by(id=payment_id).first()

    if not payment or not payment.payroll_transactions:
        raise ValueError("Payroll payment not found")

    # Get required accounts
    payable_account = None
    if data and data.get('creditSubCategory'):
        payable_account = db_session.query(ChartOfAccounts).filter_by(
            id=data['creditSubCategory'],
            app_id=app_id
        ).first()
        if not payable_account:
            raise ValueError("Specified payable account not found")

    if not payable_account:
        payable_account_id = payment.payroll_transactions.payable_account_id
        payable_account = db_session.query(ChartOfAccounts).filter_by(
            id=payable_account_id,
            app_id=app_id
        ).first()
        if not payable_account:
            raise ValueError("No payable account configured for this transaction")

    funding_account_id = payment.payment_account
    funding_account = db_session.query(ChartOfAccounts).filter_by(
        id=funding_account_id,
        app_id=app_id
    ).first()
    if not funding_account:
        raise ValueError("No funding account configured for this payment")

    journal_number = generate_unique_journal_number(db_session, app_id)

    # Determine description
    user_description_parts = []
    if payment.reference:
        user_description_parts.append(payment.reference)
    if payment.notes:
        user_description_parts.append(payment.notes)

    # If user provided reference/notes, use them; else generate description
    description = " ".join(user_description_parts) if user_description_parts else \
        f"Payroll payment for {payment.payroll_transactions.employees.first_name}"

    # Get vendor ID
    vendor_id = payment.payroll_transactions.employees.payee_id if payment.payroll_transactions.employees.payee_id else None

    # Create journal with both entries
    journal, entries = create_transaction(
        db_session=db_session,
        date=payment.payment_date,
        currency=payment.payroll_transactions.currency_id,
        created_by=current_user.id,
        app_id=app_id,
        journal_number=journal_number,
        narration=description,
        payment_mode_id=payment.payment_method,
        vendor_id=vendor_id,
        project_id=project_id,
        lines=[
            # CREDIT cash/bank (Asset)
            {
                "subcategory_id": funding_account.id,
                "amount": float(payment.amount),
                "dr_cr": "C",
                "description": description,
                "source_type": "payroll_payment",
                "source_id": payment.id
            },
            # DEBIT liability
            {
                "subcategory_id": payable_account.id,
                "amount": float(payment.amount),
                "dr_cr": "D",
                "description": f"Salary payment for {payment.payroll_transactions.employees.first_name}",
                "source_type": "payroll_payment",
                "source_id": payment.id
            }
        ]
    )

    payment.is_posted_to_ledger = True
    db_session.commit()


def post_bulk_payroll_to_ledger(db_session, transaction, current_user, data):
    """
    Post payroll transactions to ledger with guaranteed debit/credit accounts.
    Uses payable_account_id to determine if this is the first posting.

    :param db_session: SQLAlchemy session
    :param current_user: current user object with id and app_id
    :param data: dict containing debitSubCategory, creditSubCategory (both required)
    :return: Tuple of (status: str, message: str)
             status can be: 'posted', 'payments_posted', 'skipped', or 'failed'

    Args:
        transaction: Query of payroll transaction
    """
    app_id = current_user.app_id
    try:

        vendor_id = transaction.employees.payee_id if transaction.employees.payee_id else None
        payroll_period = transaction.payroll_period
        payroll_period_name = payroll_period.payroll_period_name
        end_date = payroll_period.end_date
        currency_id = transaction.currency_id
        employee_name = transaction.employees.first_name
        project_id = transaction.employees.project_id

        # Get account info
        credit_account_id = int(data['creditSubCategory'])
        debit_account_id = int(data['debitSubCategory'])

        # FIRST-TIME POSTING (no payable_account_id set)
        if not transaction.payable_account_id:
            # Handle advance repayments first
            total_advance_deducted = handle_advance_repayments(
                db_session=db_session,
                transaction_id=transaction.id,
                app_id=app_id,
                employee_name=employee_name,
                payroll_period_name=payroll_period_name,
                end_date=end_date,
                currency_id=currency_id,
                current_user=current_user,
                debit_account_id=debit_account_id,
                vendor_id=vendor_id,
                project_id=project_id
            )

            # Create accrual entries (debit expense, credit liability)
            amount_payable = transaction.net_salary

            exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
                session=db_session,
                currency_id=currency_id,
                transaction_date=end_date,
                app_id=app_id
            )

            # Create journal with both accrual entries - let create_transaction generate the journal number
            journal, entries = create_transaction(
                db_session=db_session,
                date=end_date,
                currency=currency_id,
                created_by=current_user.id,
                app_id=app_id,
                narration=f"Salary accrual for {payroll_period_name} - {employee_name}",
                vendor_id=vendor_id,
                project_id=project_id,
                exchange_rate_id=exchange_rate_id,
                lines=[
                    # Debit Expense
                    {
                        "subcategory_id": debit_account_id,
                        "amount": amount_payable,
                        "dr_cr": "D",
                        "description": f"Salaries for {payroll_period_name} - {employee_name}",
                        "source_type": "payroll_transaction",
                        "source_id": transaction.id
                    },
                    # Credit Liability
                    {
                        "subcategory_id": credit_account_id,
                        "amount": amount_payable,
                        "dr_cr": "C",
                        "description": f"Unpaid Salaries for {payroll_period_name} - {employee_name}",
                        "source_type": "payroll_transaction",
                        "source_id": transaction.id
                    }
                ]
            )

            transaction.payable_account_id = credit_account_id
            transaction.is_posted_to_ledger = True
        return True, "Successfully posted to ledger"

    except Exception as e:
        logger.error(f'An error occurred {e} \n{traceback.format_exc()}')
        db_session.rollback()
        # Re-raise the exception with a more descriptive message
        raise Exception(f"Failed to post transaction #{transaction.id} for {employee_name}: {str(e)}")


def create_payroll_journal_entries(db_session, payroll_period, payroll_transactions, app_id, current_user_id, status,
                                   base_currency_id):
    journals_created = 0

    try:
        if not payroll_transactions:
            logger.warning(f"No payroll transactions found for period {payroll_period.id}")
            return 0

        # Get system accounts
        system_accounts = get_all_system_accounts(
            db_session=db_session,
            app_id=app_id,
            created_by_user_id=current_user_id
        )
        fx_gain_loss_account_id = system_accounts.get('fx_gain_loss')

        # Validation phase (unchanged)
        validation_errors = []
        for transaction in payroll_transactions:
            employee = transaction.employees
            employee_name = f"{employee.first_name} {employee.last_name}"
            missing_accounts = []

            if not employee.payroll_expense_account_id:
                missing_accounts.append("payroll_expense_account")
            if not employee.payable_account_id:
                missing_accounts.append("payable_account")

            total_advance_repayments = db_session.query(func.sum(AdvanceRepayment.payment_amount)).filter_by(
                payroll_transaction_id=transaction.id
            ).scalar() or Decimal('0')

            if total_advance_repayments > 0 and not employee.advance_account_id:
                missing_accounts.append("advance_account")

            total_deductions = db_session.query(func.sum(Deduction.amount)).filter_by(
                payroll_transaction_id=transaction.id
            ).scalar() or Decimal('0')

            if total_deductions > 0 and not employee.deduction_payable_account_id:
                missing_accounts.append("deduction_payable_account")

            if missing_accounts:
                validation_errors.append({
                    'employee_id': employee.id,
                    'employee_name': employee_name,
                    'missing_accounts': missing_accounts
                })

        if validation_errors:
            error_msg = "Cannot create payroll journals. Missing accounts:\n"
            for err in validation_errors:
                error_msg += f"- {err['employee_name']}: Missing {', '.join(err['missing_accounts'])}\n"
            logger.error(error_msg)
            raise Exception(error_msg)

        # ===== JOURNAL CREATION PHASE =====
        for transaction in payroll_transactions:
            employee = transaction.employees
            employee_name = f"{employee.first_name} {employee.last_name}"
            project_id = employee.project_id
            exchange_rate_id = transaction.exchange_rate_id

            total_deductions = db_session.query(func.sum(Deduction.amount)).filter_by(
                payroll_transaction_id=transaction.id
            ).scalar() or Decimal('0')

            # Get advance repayments with FX calculation
            advance_repayments = db_session.query(AdvanceRepayment).filter(
                AdvanceRepayment.payroll_transaction_id == transaction.id
            ).all()

            total_advance_repayments = Decimal('0')
            fx_advance_adjustment = Decimal('0')

            for repayment in advance_repayments:
                advance = repayment.advance_payments
                if advance:
                    advance_amount = repayment.payment_amount
                    original_rate = Decimal(str(advance.exchange_rate.rate)) if advance.exchange_rate else Decimal('1')
                    current_rate = Decimal(
                        str(transaction.exchange_rate.rate)) if transaction.exchange_rate else Decimal('1')

                    original_base_amount = advance_amount * original_rate
                    current_base_amount = advance_amount * current_rate
                    fx_diff = current_base_amount - original_base_amount

                    total_advance_repayments += advance_amount
                    fx_advance_adjustment += fx_diff

            # ===== JOURNAL 1: Payroll entry (employee's currency) =====
            journal_lines = []

            total_payroll_expense = Decimal(str(transaction.net_salary)) + total_advance_repayments
            journal_lines.append({
                "subcategory_id": employee.payroll_expense_account_id,
                "amount": float(total_payroll_expense),
                "dr_cr": "D",
                "description": f"Total payroll expense - {employee_name}",
                "source_type": "payroll_transaction",
                "source_id": transaction.id
            })

            journal_lines.append({
                "subcategory_id": employee.payable_account_id,
                "amount": float(transaction.net_salary),
                "dr_cr": "C",
                "description": f"Net salary payable - {employee_name}",
                "source_type": "payroll_transaction",
                "source_id": transaction.id
            })

            if total_advance_repayments > 0:
                journal_lines.append({
                    "subcategory_id": employee.advance_account_id,
                    "amount": float(total_advance_repayments),
                    "dr_cr": "C",
                    "description": f"Employee advance recovery - {employee_name}",
                    "source_type": "payroll_transaction",
                    "source_id": transaction.id
                })

            if total_deductions > 0:
                journal_lines.extend([
                    {
                        "subcategory_id": employee.payroll_expense_account_id,
                        "amount": float(total_deductions),
                        "dr_cr": "D",
                        "description": f"Employer deductions - {employee_name}",
                        "source_type": "payroll_transaction",
                        "source_id": transaction.id
                    },
                    {
                        "subcategory_id": employee.deduction_payable_account_id,
                        "amount": float(total_deductions),
                        "dr_cr": "C",
                        "description": f"Deductions payable - {employee_name}",
                        "source_type": "payroll_transaction",
                        "source_id": transaction.id
                    }
                ])

            # Create Journal 1
            journal1, entries1 = create_transaction(
                db_session=db_session,
                date=payroll_period.end_date,
                currency=transaction.currency_id,
                created_by=current_user_id,
                app_id=app_id,
                narration=f"Payroll: {employee_name} (Gross Expense: {total_payroll_expense})",
                vendor_id=employee.payee_id,
                project_id=project_id,
                status=status,
                journal_ref_no=payroll_period.payroll_period_name,
                exchange_rate_id=exchange_rate_id,
                lines=journal_lines
            )
            journals_created += 1  # Increment count

            # ===== JOURNAL 2: FX adjustment (base currency) =====
            if abs(fx_advance_adjustment) > Decimal('0.01') and fx_gain_loss_account_id:
                if fx_advance_adjustment > 0:  # LOSS
                    journal2_lines = [
                        {
                            "subcategory_id": fx_gain_loss_account_id,
                            "amount": float(abs(fx_advance_adjustment)),
                            "dr_cr": "D",
                            "description": f"FX Loss on advance repayment - {employee_name}",
                            "source_type": "payroll_transaction",
                            "source_id": transaction.id
                        },
                        {
                            "subcategory_id": employee.advance_account_id,
                            "amount": float(abs(fx_advance_adjustment)),
                            "dr_cr": "C",
                            "description": f"Adjust advance account for FX loss - {employee_name}",
                            "source_type": "payroll_transaction",
                            "source_id": transaction.id
                        }
                    ]
                    narration_fx = f"FX adjustment - {employee_name} (FX Loss: {abs(fx_advance_adjustment):,.2f})"
                else:  # GAIN
                    journal2_lines = [
                        {
                            "subcategory_id": employee.advance_account_id,
                            "amount": float(abs(fx_advance_adjustment)),
                            "dr_cr": "D",
                            "description": f"Adjust advance account for FX gain - {employee_name}",
                            "source_type": "payroll_transaction",
                            "source_id": transaction.id
                        },
                        {
                            "subcategory_id": fx_gain_loss_account_id,
                            "amount": float(abs(fx_advance_adjustment)),
                            "dr_cr": "C",
                            "description": f"FX Gain on advance repayment - {employee_name}",
                            "source_type": "payroll_transaction",
                            "source_id": transaction.id
                        }
                    ]
                    narration_fx = f"FX adjustment - {employee_name} (FX Gain: {abs(fx_advance_adjustment):,.2f})"

                journal2, entries2 = create_transaction(
                    db_session=db_session,
                    date=payroll_period.end_date,
                    currency=base_currency_id,
                    created_by=current_user_id,
                    app_id=app_id,
                    narration=narration_fx,
                    vendor_id=employee.payee_id,
                    project_id=project_id,
                    status=status,
                    journal_ref_no=f"{payroll_period.payroll_period_name}-FX-{employee_name}",
                    exchange_rate_id=None,
                    lines=journal2_lines
                )
                journals_created += 1  # Increment for FX journal too

        return journals_created  # Return the count, not entries

    except Exception as e:
        logger.error(f"Payroll journal creation failed: {str(e)}\n{traceback.format_exc()}")
        raise


def post_payroll_payments_only(db_session, payment, current_user, ledger_data):
    """Handle posting only the unposted payments for an already-posted transaction"""
    app_id = current_user.app_id

    try:
        # Get account info from COA
        credit_account_id = int(ledger_data['creditSubCategory'])

        amount = float(payment.amount)

        exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
            session=db_session,
            currency_id=payment.payroll_transactions.currency_id,
            transaction_date=payment.payment_date,
            app_id=app_id
        )

        # Determine description
        user_description_parts = []
        if payment.reference:
            user_description_parts.append(payment.reference)
        if payment.notes:
            user_description_parts.append(payment.notes)

        # If user provided reference/notes, use them; else generate description
        description = " ".join(user_description_parts) if user_description_parts else \
            f"Payroll payment for {payment.payroll_transactions.employees.first_name}"

        # Get vendor ID
        vendor_id = payment.payroll_transactions.employees.payee_id if payment.payroll_transactions.employees.payee_id else None
        project_id = payment.payroll_transactions.employees.project_id if payment.payroll_transactions.employees.project_id else None
        # Create journal with both payment entries - let create_transaction generate the journal number
        journal, entries = create_transaction(
            db_session=db_session,
            date=payment.payment_date,
            currency=payment.payroll_transactions.currency_id,
            created_by=current_user.id,
            app_id=app_id,
            narration=description,
            payment_mode_id=payment.payment_method,
            vendor_id=vendor_id,
            project_id=project_id,
            exchange_rate_id=exchange_rate_id,
            lines=[
                # CREDIT cash/bank (Asset)
                {
                    "subcategory_id": payment.payment_account,
                    "amount": amount,
                    "dr_cr": "C",
                    "description": description,
                    "source_type": "payroll_payment",
                    "source_id": payment.id
                },
                # DEBIT liability
                {
                    "subcategory_id": credit_account_id,
                    "amount": amount,
                    "dr_cr": "D",
                    "description": description,
                    "source_type": "payroll_payment",
                    "source_id": payment.id
                }
            ]
        )

        payment.is_posted_to_ledger = True
        return True

    except Exception as e:
        logger.error(f'An error occurred while posting payment txn {e} \n{traceback.format_exc()}')
        db_session.rollback()
        raise Exception(f"Failed to post payment {payment.id}: {str(e)}")


def handle_advance_repayments(db_session, transaction_id, app_id, employee_name,
                              payroll_period_name, end_date, currency_id,
                              current_user, debit_account_id, vendor_id, project_id):
    """
    Handle posting of advance repayments for a payroll transaction

    :return: Total amount of advance repayments processed
    """
    total_advance_deducted = 0.0
    advance_repayments = db_session.query(AdvanceRepayment).filter_by(
        payroll_transaction_id=transaction_id,
        app_id=app_id,
        is_posted_to_ledger=False
    ).all()

    for repayment in advance_repayments:
        advance_payment = repayment.advance_payments

        # Find the original advance payment journal entry
        prepaid_entry = db_session.query(JournalEntry).filter_by(
            app_id=app_id,
            source_type="advance_payment",
            source_id=advance_payment.id,
            dr_cr="D"
        ).first()

        if not prepaid_entry:
            raise ValueError(
                f"Advance payment ledger entry not found for advance ID to {employee_name}. If this was not posted to ledger, please ensure it is posted before proceeding"
            )

        # Create journal with both advance repayment entries - let create_transaction generate the journal number
        journal, entries = create_transaction(
            db_session=db_session,
            date=end_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            narration=f"Advance recovery for {employee_name} - {payroll_period_name}",
            vendor_id=vendor_id,
            project_id=project_id,
            exchange_rate_id=repayment.exchange_rate_id,
            lines=[
                # 1. Credit prepaid salaries (reversing the original advance)
                {
                    "subcategory_id": prepaid_entry.subcategory_id,
                    "amount": repayment.payment_amount,
                    "dr_cr": "C",
                    "description": f"Advance recovery for {employee_name} - {payroll_period_name}",
                    "source_type": "advance_repayment",
                    "source_id": repayment.id
                },
                # 2. Debit salary expense (applying the repayment to salary expense)
                {
                    "subcategory_id": debit_account_id,
                    "amount": repayment.payment_amount,
                    "dr_cr": "D",
                    "description": f"Salary advance repayment for {payroll_period_name} - {employee_name}",
                    "source_type": "payroll_transaction",
                    "source_id": transaction_id
                }
            ]
        )

        repayment.is_posted_to_ledger = True
        total_advance_deducted += float(repayment.payment_amount)

    return total_advance_deducted


def post_advance_payment_to_ledger_internal(db_session, app_id, advance_payment_id,
                                            asset_subcategory_id, current_user_id):
    """
    Internal function to post advance payment to ledger
    Returns: (success: bool, result: str/None)
    """
    try:
        # Get COA mapping
        coa = db_session.query(ChartOfAccounts).filter_by(app_id=app_id).all()
        coa_dict = {account.id: account for account in coa}

        # Validate asset subcategory exists
        if asset_subcategory_id not in coa_dict:
            return False, "Invalid asset account selected"

        # Fetch the advance payment record
        advance_payment = db_session.query(AdvancePayment).filter_by(
            id=advance_payment_id,
            app_id=app_id
        ).first()

        if not advance_payment:
            return False, "Advance payment not found"

        # Check if already posted
        if advance_payment.is_posted_to_ledger:
            return False, "Advance payment already posted to ledger"

        currency_id = advance_payment.currency_id
        payment_date = advance_payment.payment_date
        amount = float(advance_payment.advance_amount)

        # Resolve exchange rate
        exchange_rate_id, notification = resolve_exchange_rate_for_transaction(
            session=db_session,
            currency_id=currency_id,
            transaction_date=payment_date,
            app_id=app_id
        )

        # Create description
        description = advance_payment.notes or f"Advance to {advance_payment.employees.first_name}"
        employee = advance_payment.employees.id
        project_id = advance_payment.employees.project_id

        # Get payment account category
        payment_account = db_session.query(ChartOfAccounts).filter_by(
            app_id=app_id,
            id=advance_payment.payment_account
        ).first()

        if not payment_account:
            return False, "Payment account not found"

        # Create journal with both entries
        journal, entries = create_transaction(
            db_session=db_session,
            date=payment_date,
            currency=currency_id,
            created_by=current_user_id,
            app_id=app_id,
            narration=description,
            vendor_id=employee,
            project_id=project_id,
            exchange_rate_id=exchange_rate_id,
            lines=[
                # Debit Advance Account (asset)
                {
                    "subcategory_id": asset_subcategory_id,
                    "amount": amount,
                    "dr_cr": "D",
                    "description": description,
                    "source_type": "advance_payment",
                    "source_id": advance_payment.id
                },
                # Credit Cash / Bank (asset)
                {
                    "subcategory_id": advance_payment.payment_account,
                    "amount": amount,
                    "dr_cr": "C",
                    "description": description,
                    "source_type": "advance_payment",
                    "source_id": advance_payment.id
                }
            ]
        )

        # Mark as posted
        advance_payment.is_posted_to_ledger = True
        db_session.commit()

        return True, None

    except ValueError as e:
        return False, f"Invalid data format: {str(e)}"
    except SQLAlchemyError as e:
        db_session.rollback()
        return False, f"Database error: {str(e)}"
    except Exception as e:
        logger.error(f'An error occurred \n{traceback.format_exc()}')
        db_session.rollback()
        return False, f"Unexpected error: {str(e)}"


def create_advance_payment_journal_entries(db_session, advance_payment, app_id, current_user_id, status,
                                           exchange_rate_id=None):
    """
    Create advance payment journal entries with 'Posted' status
    Uses the same accounting logic as post_advance_payment_to_ledger_internal
    """
    try:
        # Get employee with accounting info
        employee = db_session.query(Employee).filter_by(
            id=advance_payment.employee_id,
            app_id=app_id
        ).first()

        if not employee:
            logger.error(f"Employee {advance_payment.employee_id} not found")
            return None

        # Use employee's advance account if configured, otherwise use the provided asset account
        advance_account_id = employee.advance_account_id
        if not advance_account_id:
            logger.error(f"Employee {employee.first_name} has no advance account configured")
            return None
        project_id = employee.project_id
        currency_id = advance_payment.currency_id
        payment_date = advance_payment.payment_date
        amount = float(advance_payment.advance_amount)

        # Create description
        description = advance_payment.notes or f"Advance to {employee.first_name} {employee.last_name}"

        # Create journal with both entries - SAME LOGIC as your working function
        journal, entries = create_transaction(
            db_session=db_session,
            date=payment_date,
            currency=currency_id,
            created_by=current_user_id,
            app_id=app_id,
            narration=description,
            vendor_id=employee.payee_id,  # Use employee's vendor record
            project_id=project_id,
            exchange_rate_id=exchange_rate_id,
            status=status,
            lines=[
                # Debit Advance Account (asset) - using employee's configured account
                {
                    "subcategory_id": advance_account_id,
                    "amount": amount,
                    "dr_cr": "D",
                    "description": description,
                    "source_type": "advance_payment",
                    "source_id": advance_payment.id
                },
                # Credit Cash / Bank (asset) - same as your working function
                {
                    "subcategory_id": advance_payment.payment_account,
                    "amount": amount,
                    "dr_cr": "C",
                    "description": description,
                    "source_type": "advance_payment",
                    "source_id": advance_payment.id
                }
            ]
        )

        # Update advance payment with journal reference (but don't mark as posted yet)
        advance_payment.journal_id = journal.id

        return journal

    except Exception as e:
        logger.error(f"Error creating advance payment journal: {str(e)}")
        return None


def create_payroll_payment_journal_entries(db_session, payroll_transaction, payroll_payment, payment_account_id,
                                           current_user_id, app_id, exchange_rate_id=None, base_currency_id=None,
                                           status=None):
    """
    ===========================================================================
    PAYROLL PAYMENT JOURNAL CREATION - MULTI-CURRENCY WITH FX HANDLING
    ===========================================================================

    ACCOUNTING LOGIC:
    -----------------
    TWO SEPARATE JOURNALS are created:

    JOURNAL 1 (Employee's Currency - e.g., USD):
        Dr Payable Account (Reduce liability)
           Cr Cash/Bank Account (Money going out)

    JOURNAL 2 (Base Currency - e.g., UGX) - Only if FX difference exists:
        For LOSS:
            Dr FX Loss Account
               Cr Payable Account
        For GAIN:
            Dr Payable Account
               Cr FX Gain Account
    ===========================================================================
    """
    try:
        # Get the payroll transaction and employee details
        if not payroll_transaction:
            payroll_transaction = db_session.query(PayrollTransaction).options(
                joinedload(PayrollTransaction.employees)
            ).filter_by(id=payroll_payment.payroll_transaction_id).first()

        if not payroll_transaction:
            logger.error(f"Payroll transaction not found for payment {payroll_payment.id}")
            return None

        employee = payroll_transaction.employees
        employee_name = f"{employee.first_name} {employee.last_name}"
        project_id = payroll_transaction.employees.project_id

        # Get system accounts for FX handling
        system_accounts = get_all_system_accounts(
            db_session=db_session,
            app_id=app_id,
            created_by_user_id=current_user_id
        )
        fx_gain_loss_account_id = system_accounts.get('fx_gain_loss')

        # Get base currency ID if not provided
        if not base_currency_id:
            base_currency = db_session.query(Currency).filter_by(
                app_id=app_id, currency_index=1
            ).first()
            base_currency_id = base_currency.id if base_currency else None

        # Get payroll transaction's original exchange rate
        payroll_exchange_rate = None
        if payroll_transaction.exchange_rate_id:
            payroll_exchange_rate = db_session.query(ExchangeRate).filter_by(
                id=payroll_transaction.exchange_rate_id
            ).first()

        original_rate = Decimal(str(payroll_exchange_rate.rate)) if payroll_exchange_rate else Decimal('1')
        current_rate = Decimal(str(payroll_payment.exchange_rate.rate)) if payroll_payment.exchange_rate else Decimal(
            '1')

        # Calculate FX gain/loss in base currency
        payment_amount = Decimal(str(payroll_payment.amount))
        original_base_amount = payment_amount * original_rate
        current_base_amount = payment_amount * current_rate
        fx_gain_loss = current_base_amount - original_base_amount

        # ===== JOURNAL 1: Payment in Employee's Currency =====
        journal1_lines = [
            {
                "subcategory_id": employee.payable_account_id,
                "amount": float(payment_amount),
                "dr_cr": "D",
                "description": f"Salary payment - {employee_name}",
                "source_type": "payroll_payment",
                "source_id": payroll_payment.id
            },
            {
                "subcategory_id": payment_account_id,
                "amount": float(payment_amount),
                "dr_cr": "C",
                "description": f"Salary payment to {employee_name}",
                "source_type": "payroll_payment",
                "source_id": payroll_payment.id
            }
        ]

        # Create Journal 1 (Employee's Currency)
        journal1, entries1 = create_transaction(
            db_session=db_session,
            date=payroll_payment.payment_date,
            currency=payroll_payment.currency_id,
            created_by=current_user_id,
            app_id=app_id,
            narration=payroll_payment.notes if payroll_payment.notes else f"Payroll Payment: {employee_name} - {payroll_transaction.payroll_period.payroll_period_name}",
            vendor_id=employee.payee_id,
            project_id=project_id,
            status=status,
            journal_ref_no=payroll_payment.reference or payroll_transaction.payroll_period.payroll_period_name,
            exchange_rate_id=exchange_rate_id,
            lines=journal1_lines
        )

        # ADD THIS: Validate journal1 is balanced using the model's method
        if not journal1 or not journal1.is_balanced():
            logger.error(f"Journal 1 is not balanced for payroll payment {payroll_payment.id}")
            return None

        journals_created = [journal1]

        # ===== JOURNAL 2: FX Adjustment in Base Currency =====
        if abs(fx_gain_loss) > Decimal('0.01') and fx_gain_loss_account_id and base_currency_id:
            if fx_gain_loss > 0:  # Loss - paid more in base currency than expected
                journal2_lines = [
                    {
                        "subcategory_id": fx_gain_loss_account_id,
                        "amount": float(abs(fx_gain_loss)),
                        "dr_cr": "D",  # Loss is debit
                        "description": f"FX Loss on payroll payment - {employee_name}",
                        "source_type": "payroll_payment",
                        "source_id": payroll_payment.id
                    },
                    {
                        "subcategory_id": employee.payable_account_id,
                        "amount": float(abs(fx_gain_loss)),
                        "dr_cr": "C",  # Credit to increase payable in base currency
                        "description": f"Adjust payable for FX loss - {employee_name}",
                        "source_type": "payroll_payment",
                        "source_id": payroll_payment.id
                    }
                ]
                narration_fx = f"FX Loss on payroll payment - {employee_name} (Loss: {abs(fx_gain_loss):,.2f})"
            else:  # Gain - paid less in base currency than expected
                journal2_lines = [
                    {
                        "subcategory_id": employee.payable_account_id,
                        "amount": float(abs(fx_gain_loss)),
                        "dr_cr": "D",  # Debit to reduce payable in base currency
                        "description": f"Adjust payable for FX gain - {employee_name}",
                        "source_type": "payroll_payment",
                        "source_id": payroll_payment.id
                    },
                    {
                        "subcategory_id": fx_gain_loss_account_id,
                        "amount": float(abs(fx_gain_loss)),
                        "dr_cr": "C",  # Gain is credit
                        "description": f"FX Gain on payroll payment - {employee_name}",
                        "source_type": "payroll_payment",
                        "source_id": payroll_payment.id
                    }
                ]
                narration_fx = f"FX Gain on payroll payment - {employee_name} (Gain: {abs(fx_gain_loss):,.2f})"

            # Create Journal 2 in Base Currency
            journal2, entries2 = create_transaction(
                db_session=db_session,
                date=payroll_payment.payment_date,
                currency=base_currency_id,
                created_by=current_user_id,
                app_id=app_id,
                narration=narration_fx,
                vendor_id=employee.payee_id,
                project_id=project_id,
                status=status,
                journal_ref_no=f"FX-{payroll_payment.reference or payroll_transaction.payroll_period.payroll_period_name}",
                exchange_rate_id=None,  # Base currency journal has no exchange rate
                lines=journal2_lines
            )

            # ADD THIS: Validate journal2 is balanced using the model's method
            if not journal2 or not journal2.is_balanced():
                logger.error(f"Journal 2 (FX adjustment) is not balanced for payroll payment {payroll_payment.id}")
                return None

            journals_created.append(journal2)

        return journals_created

    except Exception as e:
        logger.error(f"Error in create_payroll_payment_journal_entries: {str(e)}")
        return None


def create_deduction_payment_journal_entries(db_session, deduction_payment, payment_account_id, deduction,
                                             current_user_id, app_id, exchange_rate_id=None,
                                             base_currency_id=None, fx_gain_loss=None, status=None):
    """
    ===========================================================================
    DEDUCTION PAYMENT JOURNAL CREATION - MULTI-CURRENCY WITH FX HANDLING
    ===========================================================================
    """
    try:
        # Input validation
        if not deduction_payment:
            logger.error("Deduction payment object is required")
            return None

        if not payment_account_id:
            logger.error("Payment account ID is required")
            return None

        # Get employee details with error handling
        if not deduction.payroll_transactions:
            logger.error(f"Deduction {deduction.id} has no associated payroll transaction")
            return None

        payroll_transaction = deduction.payroll_transactions
        employee = payroll_transaction.employees

        if not employee:
            logger.error(f"No employee found for deduction {deduction.id}")
            return None

        employee_name = f"{employee.first_name} {employee.last_name}"
        project_id = employee.project_id

        # Validate required accounts
        if not employee.deduction_payable_account_id:
            logger.error(f"Employee {employee_name} has no deduction payable account configured")
            return None

        # Get system accounts for FX handling
        system_accounts = get_all_system_accounts(
            db_session=db_session,
            app_id=app_id,
            created_by_user_id=current_user_id
        )
        fx_gain_loss_account_id = system_accounts.get('fx_gain_loss')

        # Get base currency ID if not provided
        if not base_currency_id:
            base_currency = db_session.query(Currency).filter_by(
                app_id=app_id, currency_index=1
            ).first()
            if not base_currency:
                logger.error(f"No base currency found for app {app_id}")
                return None
            base_currency_id = base_currency.id

        # Get deduction's original exchange rate - SAME AS PAYROLL
        deduction_exchange_rate = None
        if deduction.exchange_rate_id:
            deduction_exchange_rate = db_session.query(ExchangeRate).filter_by(
                id=deduction.exchange_rate_id
            ).first()

        original_rate = Decimal(str(deduction_exchange_rate.rate)) if deduction_exchange_rate else Decimal('1')

        # Get current exchange rate from payment - SAME AS PAYROLL (direct relationship)
        current_rate = Decimal(
            str(deduction_payment.exchange_rate.rate)) if deduction_payment.exchange_rate else Decimal('1')

        # Calculate FX gain/loss in base currency - SAME AS PAYROLL
        payment_amount = Decimal(str(deduction_payment.amount))
        original_base_amount = payment_amount * original_rate
        current_base_amount = payment_amount * current_rate
        fx_gain_loss_calc = current_base_amount - original_base_amount

        # Use passed fx_gain_loss if provided (for frontend preview)
        if fx_gain_loss is not None and fx_gain_loss != 0:
            fx_gain_loss_calc = Decimal(str(fx_gain_loss))

        # ===== JOURNAL 1: Payment in Deduction's Currency =====
        journal1_lines = [
            {
                "subcategory_id": employee.deduction_payable_account_id,
                "amount": float(payment_amount),
                "dr_cr": "D",
                "description": f"Deduction payment - {employee_name} ({deduction.deduction_type.name})",
                "source_type": "deduction_payment",
                "source_id": deduction_payment.id
            },
            {
                "subcategory_id": payment_account_id,
                "amount": float(payment_amount),
                "dr_cr": "C",
                "description": f"Deduction payment to third party for {employee_name}",
                "source_type": "deduction_payment",
                "source_id": deduction_payment.id
            }
        ]

        # Create Journal 1
        journal1, entries1 = create_transaction(
            db_session=db_session,
            date=deduction_payment.payment_date,
            currency=deduction_payment.currency_id,
            created_by=current_user_id,
            app_id=app_id,
            narration=deduction_payment.notes if deduction_payment.notes else
            f"Deduction Payment: {employee_name} - {deduction.deduction_type.name}",
            vendor_id=employee.payee_id,
            project_id=project_id,
            status=status or "Unposted",
            journal_ref_no=deduction_payment.reference or f"DED-{deduction.id}",
            exchange_rate_id=exchange_rate_id,
            lines=journal1_lines
        )

        if not journal1 or not journal1.is_balanced():
            logger.error(f"Journal 1 is not balanced for deduction payment {deduction_payment.id}")
            return None

        journals_created = [journal1]

        # ===== JOURNAL 2: FX Adjustment in Base Currency =====
        # SAME CONDITION AS PAYROLL
        if abs(fx_gain_loss_calc) > Decimal('0.01') and fx_gain_loss_account_id and base_currency_id:
            if fx_gain_loss_calc > 0:  # Loss
                journal2_lines = [
                    {
                        "subcategory_id": fx_gain_loss_account_id,
                        "amount": float(abs(fx_gain_loss_calc)),
                        "dr_cr": "D",
                        "description": f"FX Loss on deduction payment - {employee_name} ({deduction.deduction_type.name})",
                        "source_type": "deduction_payment",
                        "source_id": deduction_payment.id
                    },
                    {
                        "subcategory_id": employee.deduction_payable_account_id,
                        "amount": float(abs(fx_gain_loss_calc)),
                        "dr_cr": "C",
                        "description": f"Adjust deduction payable for FX loss - {employee_name}",
                        "source_type": "deduction_payment",
                        "source_id": deduction_payment.id
                    }
                ]
                narration_fx = f"FX Loss on deduction payment - {employee_name} (Loss: {abs(fx_gain_loss_calc):,.2f})"
            else:  # Gain
                journal2_lines = [
                    {
                        "subcategory_id": employee.deduction_payable_account_id,
                        "amount": float(abs(fx_gain_loss_calc)),
                        "dr_cr": "D",
                        "description": f"Adjust deduction payable for FX gain - {employee_name}",
                        "source_type": "deduction_payment",
                        "source_id": deduction_payment.id
                    },
                    {
                        "subcategory_id": fx_gain_loss_account_id,
                        "amount": float(abs(fx_gain_loss_calc)),
                        "dr_cr": "C",
                        "description": f"FX Gain on deduction payment - {employee_name} ({deduction.deduction_type.name})",
                        "source_type": "deduction_payment",
                        "source_id": deduction_payment.id
                    }
                ]
                narration_fx = f"FX Gain on deduction payment - {employee_name} (Gain: {abs(fx_gain_loss_calc):,.2f})"

            # Create Journal 2
            journal2, entries2 = create_transaction(
                db_session=db_session,
                date=deduction_payment.payment_date,
                currency=base_currency_id,
                created_by=current_user_id,
                app_id=app_id,
                narration=narration_fx,
                vendor_id=employee.payee_id,
                project_id=project_id,
                status=status or "Unposted",
                journal_ref_no=f"FX-{deduction_payment.reference or f'DED-{deduction.id}'}",
                exchange_rate_id=None,
                lines=journal2_lines
            )

            if not journal2 or not journal2.is_balanced():
                logger.error(f"Journal 2 (FX adjustment) is not balanced for deduction payment {deduction_payment.id}")
                return None

            journals_created.append(journal2)

        # Link deduction payment to primary journal
        deduction_payment.journal_id = journal1.id
        db_session.add(deduction_payment)

        logger.info(f"✅ Created {len(journals_created)} journal(s) for deduction payment {deduction_payment.id}")
        return journals_created

    except Exception as e:
        logger.error(f"Error in create_deduction_payment_journal_entries: {str(e)}\n{traceback.format_exc()}")
        return None


def post_deduction_to_ledger(db_session, transaction_id, current_user, data):
    """
    Post deduction transaction entries to ledger using payroll-style logic.

    :param db_session: SQLAlchemy session
    :param transaction_id: Deduction ID
    :param current_user: current user object with id and app_id
    :param data: dict containing debitSubCategory, creditSubCategory, ledgerBalanceDue
    """
    app_id = current_user.app_id

    # Load deduction transaction with relationships in one query
    deduction_transaction = (
        db_session.query(Deduction)
        .options(
            joinedload(Deduction.employees),
            joinedload(Deduction.payroll_periods)
        )
        .filter_by(id=transaction_id)
        .first()
    )

    if not deduction_transaction:
        raise ValueError("Deduction transaction not found")

    vendor_id = deduction_transaction.employees.payee_id if deduction_transaction.employees.payee_id else None
    payroll_period = deduction_transaction.payroll_periods
    payroll_period_name = payroll_period.payroll_period_name if payroll_period else "Unknown"
    end_date = payroll_period.end_date if payroll_period else date.today()
    currency_id = deduction_transaction.currency_id
    employee_name = deduction_transaction.employees.first_name
    project_id = deduction_transaction.employees.project_id
    # Unposted payments for this deduction transaction
    unposted_payments = db_session.query(DeductionPayment).filter_by(
        deduction_id=transaction_id,
        is_posted_to_ledger=False
    ).all()

    # Load Chart of Accounts
    coa = db_session.query(ChartOfAccounts).filter_by(app_id=app_id).all()
    coa_dict = {account.id: account for account in coa}

    credit_account_id = int(data['creditSubCategory']) if data.get('creditSubCategory') else None
    debit_account_id = int(data['debitSubCategory']) if data.get('debitSubCategory') else None
    ledger_balance_due = float(data.get('ledgerBalanceDue', 0))

    payable_account_id = None

    # 1. Handle initial deduction posting (accrual-like logic)
    if debit_account_id and credit_account_id:
        # Full posting: Expense (Debit) + Liability (Credit)
        exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
            session=db_session,
            currency_id=currency_id,
            transaction_date=end_date,
            app_id=app_id
        )

        # Create journal with both accrual entries
        journal, entries = create_transaction(
            db_session=db_session,
            date=end_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            narration=f"Unpaid deductions for Payroll Period: {payroll_period_name} for {employee_name}",
            vendor_id=vendor_id,
            project_id=project_id,
            exchange_rate_id=exchange_rate_id,
            lines=[
                # Debit Expense
                {
                    "subcategory_id": debit_account_id,
                    "amount": ledger_balance_due,
                    "dr_cr": "D",
                    "description": f"Unpaid deductions for Payroll Period: {payroll_period_name} for {employee_name}",
                    "source_type": "deduction_payment",
                    "source_id": transaction_id
                },
                # Credit Liability
                {
                    "subcategory_id": credit_account_id,
                    "amount": ledger_balance_due,
                    "dr_cr": "C",
                    "description": f"Unpaid deductions for Payroll Period: {payroll_period_name} for {employee_name}",
                    "source_type": "deduction_payment",
                    "source_id": transaction_id
                }
            ]
        )

        deduction_transaction.payable_account_id = credit_account_id

    elif credit_account_id and not debit_account_id:
        # Only credit account defined (no expense yet)
        payable_account_id = credit_account_id
        deduction_transaction.payable_account_id = payable_account_id

    # 2. Handle actual payments
    for deduction_payment in unposted_payments:
        amount = float(deduction_payment.amount)
        payment_date = deduction_payment.payment_date

        exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
            session=db_session,
            currency_id=currency_id,
            transaction_date=payment_date,
            app_id=app_id
        )

        # Determine payable account if not set
        if not payable_account_id:
            payable_account_id = deduction_transaction.payable_account_id

        if not payable_account_id:
            raise ValueError("No payable account configured for this deduction transaction")

        # Validate payment account
        payment_account = db_session.query(ChartOfAccounts).filter_by(id=deduction_payment.payment_account).first()
        if not payment_account:
            raise ValueError(f"Payment account {deduction_payment.payment_account} not found")

        # Create description
        description = f"{deduction_payment.reference + ' ' if deduction_payment.reference else ''}Deduction payment for {payroll_period_name} for {employee_name}"

        # Create journal with both payment entries
        journal, entries = create_transaction(
            db_session=db_session,
            date=payment_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            narration=description,
            payment_mode_id=deduction_payment.payment_method,
            vendor_id=vendor_id,
            project_id=project_id,
            exchange_rate_id=exchange_rate_id,
            lines=[
                # CREDIT cash/bank (Asset)
                {
                    "subcategory_id": deduction_payment.payment_account,
                    "amount": amount,
                    "dr_cr": "C",
                    "description": description,
                    "source_type": "deduction_payment",
                    "source_id": deduction_payment.id
                },
                # DEBIT liability (reducing the payable)
                {
                    "subcategory_id": payable_account_id,
                    "amount": amount,
                    "dr_cr": "D",
                    "description": f"Reducing unpaid deductions for {payroll_period_name} for {employee_name}",
                    "source_type": "deduction_payment",
                    "source_id": deduction_payment.id
                }
            ]
        )

        deduction_payment.is_posted_to_ledger = True

    # 3. Mark deduction transaction posted if no unposted payments remain
    remaining = db_session.query(DeductionPayment).filter_by(
        deduction_id=transaction_id,
        is_posted_to_ledger=False
    ).count()

    if remaining == 0:
        deduction_transaction.is_posted_to_ledger = True


def post_bulk_deduction_to_ledger(db_session, deduction, current_user, data):
    """
    Post deduction transactions to ledger with guaranteed debit/credit accounts.
    """
    app_id = current_user.app_id

    try:
        employee = deduction.employees
        payroll_period = deduction.payroll_periods
        payroll_period_name = payroll_period.payroll_period_name if payroll_period else "Unknown"
        end_date = payroll_period.end_date if payroll_period else date.today()
        currency_id = deduction.currency_id
        employee_name = employee.first_name
        vendor_id = employee.payee_id
        project_id = employee.project_id
        # Get account info
        credit_account_id = int(data['creditSubCategory'])
        debit_account_id = int(data['debitSubCategory'])
        balance_due = float(data['ledgerBalanceDue'])

        # Check if accrual was already posted - now checking JournalEntry instead of Transaction
        accrual_already_posted = db_session.query(JournalEntry).filter_by(
            source_id=deduction.id,
            source_type="deduction_payment",
            subcategory_id=credit_account_id
        ).first()

        # Handle unposted payments first
        unposted_payments = [p for p in deduction.deduction_payments
                             if not p.is_posted_to_ledger]

        # Process payments if any exist
        for payment in unposted_payments:
            success = post_deduction_payments_only(
                db_session=db_session,
                payment=payment,
                current_user=current_user,
                ledger_data=data  # Pass the full data including account info
            )
            if not success:
                raise Exception(f"Failed to post payment {payment.id}")

        # Post accrual if needed (no payments or leftover balance)
        if not accrual_already_posted and balance_due > 0:
            exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
                session=db_session,
                currency_id=currency_id,
                transaction_date=end_date,
                app_id=app_id
            )

            description = f"Unpaid deductions for {payroll_period_name} - {employee_name}"

            # Create journal with both accrual entries
            journal, entries = create_transaction(
                db_session=db_session,
                date=end_date,
                currency=currency_id,
                created_by=current_user.id,
                app_id=app_id,
                narration=description,
                vendor_id=vendor_id,
                project_id=project_id,
                exchange_rate_id=exchange_rate_id,
                lines=[
                    # Debit Expense
                    {
                        "subcategory_id": debit_account_id,
                        "amount": balance_due,
                        "dr_cr": "D",
                        "description": description,
                        "source_type": "deduction_payment",
                        "source_id": deduction.id
                    },
                    # Credit Liability
                    {
                        "subcategory_id": credit_account_id,
                        "amount": balance_due,
                        "dr_cr": "C",
                        "description": description,
                        "source_type": "deduction_payment",
                        "source_id": deduction.id
                    }
                ]
            )

        deduction.is_posted_to_ledger = True
        deduction.payable_account_id = credit_account_id
        return True, "Successfully posted to ledger"

    except Exception as e:
        logger.error(f'Error posting deduction {deduction.id}: {e} \n{traceback.format_exc()}')
        db_session.rollback()
        raise Exception(f"Failed to post deduction #{deduction.id} for {employee_name}: {str(e)}")


def post_deduction_payments_only(db_session, payment, current_user, ledger_data):
    """Handle posting only the unposted payments for an already-posted deduction"""
    app_id = current_user.app_id

    try:
        deduction = payment.deductions
        employee = deduction.employees
        payroll_period = deduction.payroll_periods
        payroll_period_name = payroll_period.payroll_period_name if payroll_period else "Unknown"
        project_id = employee.project_id
        # Get account info
        credit_account_id = int(ledger_data['creditSubCategory'])

        amount = float(payment.amount)

        exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
            session=db_session,
            currency_id=deduction.currency_id,
            transaction_date=payment.payment_date,
            app_id=app_id
        )

        # Validate payment account
        payment_account = db_session.query(ChartOfAccounts).filter_by(id=payment.payment_account).first()
        if not payment_account:
            raise ValueError(f"Payment account {payment.payment_account} not found")

        # Check if accrual was already posted - now checking JournalEntry instead of Transaction
        accrual_already_posted = db_session.query(JournalEntry).filter_by(
            source_id=deduction.id,
            source_type="deduction_payment",
            subcategory_id=credit_account_id
        ).first()

        # Get vendor ID
        vendor_id = employee.payee_id if employee.payee_id else None

        # Create description
        description = f"Deduction payment for {payroll_period_name} - {employee.first_name}"

        if accrual_already_posted:
            # Create journal with both entries for liability settlement
            journal, entries = create_transaction(
                db_session=db_session,
                date=payment.payment_date,
                currency=deduction.currency_id,
                created_by=current_user.id,
                app_id=app_id,
                narration=description,
                payment_mode_id=payment.payment_method,
                vendor_id=vendor_id,
                project_id=project_id,
                exchange_rate_id=exchange_rate_id,
                lines=[
                    # CREDIT cash/bank (Asset)
                    {
                        "subcategory_id": payment.payment_account,
                        "amount": amount,
                        "dr_cr": "C",
                        "description": description,
                        "source_type": "deduction_payment",
                        "source_id": payment.id
                    },
                    # DEBIT liability (settle existing accrual)
                    {
                        "subcategory_id": credit_account_id,
                        "amount": amount,
                        "dr_cr": "D",
                        "description": f"Payment for {payroll_period_name} - {employee.first_name}",
                        "source_type": "deduction_payment",
                        "source_id": payment.id
                    }
                ]
            )
        else:
            # Get debit account info
            debit_account_id = int(ledger_data['debitSubCategory'])

            # Create journal with both entries for direct expense payment
            journal, entries = create_transaction(
                db_session=db_session,
                date=payment.payment_date,
                currency=deduction.currency_id,
                created_by=current_user.id,
                app_id=app_id,
                narration=description,
                payment_mode_id=payment.payment_method,
                vendor_id=vendor_id,
                project_id=project_id,
                exchange_rate_id=exchange_rate_id,
                lines=[
                    # CREDIT cash/bank (Asset)
                    {
                        "subcategory_id": payment.payment_account,
                        "amount": amount,
                        "dr_cr": "C",
                        "description": description,
                        "source_type": "deduction_payment",
                        "source_id": payment.id
                    },
                    # DEBIT expense (direct payment)
                    {
                        "subcategory_id": debit_account_id,
                        "amount": amount,
                        "dr_cr": "D",
                        "description": f"Deduction paid for {payroll_period_name} - {employee.first_name}",
                        "source_type": "deduction_payment",
                        "source_id": payment.id
                    }
                ]
            )

        payment.is_posted_to_ledger = True
        return True

    except Exception as e:
        logger.error(f'Error posting deduction payment {payment.id}: {e} \n{traceback.format_exc()}')
        db_session.rollback()
        raise Exception(f"Failed to post payment {payment.id}: {str(e)}")


# --- LEDGER POSTING FUNCTION ---

def post_opening_balances_to_ledger(
        db_session,
        conversion_date,
        current_user,
        base_currency_id,
        balances_data,
        retained_earnings_account_id
):
    """
    Post opening balances to the general ledger with account + vendor/customer narrations.
    """

    app_id = current_user.app_id

    try:
        # 1. Delete existing opening balance journals
        journal_entries = db_session.query(JournalEntry).filter_by(
            source_type="opening_balance",
            app_id=app_id
        ).all()

        journal_ids = {entry.journal_id for entry in journal_entries}
        for entry in journal_entries:
            db_session.delete(entry)
        for journal_id in journal_ids:
            journal = db_session.query(Journal).get(journal_id)
            if journal:
                db_session.delete(journal)
        db_session.flush()

        # 2. Load accounts
        coa_accounts = db_session.query(ChartOfAccounts).filter_by(app_id=app_id).all()
        coa_dict = {a.id: a for a in coa_accounts}

        retained_earnings_account = coa_dict.get(retained_earnings_account_id)
        if not retained_earnings_account:
            raise ValueError("Retained Earnings account not found")

        posted_count = 0

        # 3. Post balances
        for balance_data in balances_data:
            try:
                account_id = int(balance_data["account_id"])
                total_balance = float(balance_data.get("total_balance", 0.0))
                vendor_balances = balance_data.get("vendor_balances", [])

                account = coa_dict.get(account_id)
                if not account:
                    continue
                account_name = account.sub_category or account.account_name

                if vendor_balances:
                    # Split per vendor/customer
                    for vb in vendor_balances:
                        amount = float(vb["amount"])
                        if math.isclose(amount, 0.0, abs_tol=0.001):
                            continue

                        vendor_id = vb.get("vendor_id")
                        vendor = db_session.query(Vendor).get(vendor_id)
                        vendor_name = vendor.vendor_name if vendor else "Unknown Party"

                        narration = f"Opening Balance – {account_name} ({vendor_name})"
                        source_id = f"account_{account_id}_vendor_{vendor_id}_{conversion_date.strftime('%Y%m%d')}"

                        account_dr_cr, re_dr_cr = _get_dr_cr(account.normal_balance, amount)

                        create_transaction(
                            db_session=db_session,
                            date=conversion_date,
                            currency=base_currency_id,
                            created_by=current_user.id,
                            app_id=app_id,
                            narration=narration,
                            vendor_id=vendor_id,
                            lines=[
                                {
                                    "subcategory_id": account_id,
                                    "amount": abs(amount),
                                    "dr_cr": account_dr_cr,
                                    "description": narration,
                                    "source_type": "opening_balance",
                                    "source_id": source_id,
                                    "vendor_id": vendor_id,
                                },
                                {
                                    "subcategory_id": retained_earnings_account_id,
                                    "amount": abs(amount),
                                    "dr_cr": re_dr_cr,
                                    "description": narration,
                                    "source_type": "opening_balance",
                                    "source_id": source_id,
                                },
                            ],
                        )
                        posted_count += 1

                else:
                    # General balance
                    if math.isclose(total_balance, 0.0, abs_tol=0.001):
                        continue

                    narration = f"Opening Balance – {account_name}"
                    source_id = f"account_{account_id}_{conversion_date.strftime('%Y%m%d')}"
                    account_dr_cr, re_dr_cr = _get_dr_cr(account.normal_balance, total_balance)

                    create_transaction(
                        db_session=db_session,
                        date=conversion_date,
                        currency=base_currency_id,
                        created_by=current_user.id,
                        app_id=app_id,
                        narration=narration,
                        lines=[
                            {
                                "subcategory_id": account_id,
                                "amount": abs(total_balance),
                                "dr_cr": account_dr_cr,
                                "description": narration,
                                "source_type": "opening_balance",
                                "source_id": source_id,
                            },
                            {
                                "subcategory_id": retained_earnings_account_id,
                                "amount": abs(total_balance),
                                "dr_cr": re_dr_cr,
                                "description": narration,
                                "source_type": "opening_balance",
                                "source_id": source_id,
                            },
                        ],
                    )
                    posted_count += 1

            except Exception as e:
                logger.error(f"Error posting opening balance: {str(e)}")
                logger.error(traceback.format_exc())
                continue

        return posted_count

    except Exception as e:
        logger.error(f"Critical error in post_opening_balances_to_ledger: {str(e)}")
        logger.error(traceback.format_exc())
        raise


# --- HELPER FUNCTION FOR DR/CR ---

def _get_dr_cr(normal_balance, amount):
    """Return debit/credit side based on account normal balance and sign of amount."""
    if normal_balance.lower() == "debit":
        return ("D", "C") if amount >= 0 else ("C", "D")
    else:
        return ("C", "D") if amount >= 0 else ("D", "C")


def create_inventory_ledger_entry(db_session, base_currency_id, transaction_detail, current_user, posted_status):
    """
    Post individual inventory transaction details to the general ledger.

    This function handles the accounting entries for inventory transactions by creating
    appropriate debit and credit entries in the general ledger based on the transaction type.

    Parameters:
    -----------
    db_session: SQLAlchemy Session
        Database session for executing queries and commits
    base_currency_id: int
        ID of the company's base currency (passed from frontend template)
    transaction_detail: InventoryTransactionDetail
        The specific inventory transaction detail to post to ledger
    current_user: User
        The user initiating the posting operation

    Returns:
    --------
    bool: True if posting successful, raises Exception if failed

    Workflow:
    ---------
    1. Validation checks (already posted, transfer entries)
    2. Currency conversion setup (if needed)
    3. Determine transaction type and prepare description
    4. Route to appropriate handler based on movement type
    5. Mark transaction as posted on success

    Movement Types Handled:
    -----------------------
    - 'in' → Stock In (Purchase/Production)
        Debit: Inventory Asset, Credit: Payable
    - 'opening_balance' → Opening Balance
        Debit: Inventory Asset, Credit: Retained Earnings
    - 'stock_out_sale' → Stock Out Sale
        Debit: Payment Account & COGS, Credit: Sales Revenue & Inventory Asset
    - 'stock_out_write_off' → Stock Write-Off
        Debit: Adjustment/COGS Account, Credit: Inventory Asset
    - 'adjustment'/'missing'/'damaged'/'expired' → Adjustments
        Debit/Credit: Adjustment Account, Opposite: Inventory Asset

    Notes:
    ------
    - Uses absolute value of total_cost to ensure positive amounts
    - Skips transfer entries (no ledger impact)
    - Generates unique journal number for audit trail
    - Handles foreign currency exchange rates when applicable
    - Updates is_posted_to_ledger flag on successful posting
    """

    try:

        app_id = current_user.app_id

        # CRITICAL: Check if the relationship exists
        if not transaction_detail.inventory_entry_line_item:
            logger.error(f"TRANSACTION DETAIL {transaction_detail.id} HAS NO INVENTORY_ENTRY_LINE_ITEM!")
            logger.error(f"Foreign key value is: {transaction_detail.inventory_entry_line_item_id}")
            return False

        # Get the inventory entry for account information
        inventory_entry = transaction_detail.inventory_entry_line_item.inventory_entry

        # Skip transfer entries
        if inventory_entry.inventory_source == 'transfer':
            logger.info(f"Skipping transfer transaction {transaction_detail.id} - no ledger posting required")
            transaction_detail.is_posted_to_ledger = True
            return True

        # Only get exchange rate if currency is not base currency
        exchange_rate_id = None
        if int(transaction_detail.currency_id) != int(base_currency_id):
            exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
                session=db_session,
                currency_id=transaction_detail.currency_id,
                transaction_date=transaction_detail.transaction_date,
                app_id=app_id
            )

        # Get movement type and determine accounting treatment
        movement_type = inventory_entry.inventory_source
        description = f"Inventory {movement_type} - {inventory_entry.reference or 'No Reference'}"
        line_value = abs(transaction_detail.total_cost)

        item_variation = transaction_detail.inventory_item_variation_link
        inventory_item = item_variation.inventory_item if item_variation else None

        if not inventory_item:
            raise ValueError(f"Inventory item not found for line item")

        # Handle different movement types
        if movement_type in ['purchase', 'manufacture', 'return', 'manual']:  # Stock In
            _handle_stock_in_transaction(db_session, transaction_detail, inventory_entry,
                                         inventory_item, line_value, exchange_rate_id,
                                         description, current_user, app_id, posted_status)

        elif movement_type == 'opening_balance':
            _handle_opening_balance_transaction(db_session, transaction_detail, inventory_entry,
                                                inventory_item, line_value, exchange_rate_id,
                                                description, current_user, app_id, posted_status)

        elif movement_type == 'sale':  # Stock Out Sale
            _handle_stock_out_sale_transaction(db_session, transaction_detail, inventory_entry,
                                               inventory_item, line_value, exchange_rate_id,
                                               description, current_user, app_id, posted_status)

        elif movement_type == 'write_off':  # Stock Out Write-Off
            _handle_stock_out_write_off_transaction(db_session, transaction_detail, inventory_entry,
                                                    inventory_item, line_value, exchange_rate_id,
                                                    description, current_user, app_id, posted_status)

        elif movement_type in ['adjustment', 'missing', 'damaged', 'expired']:
            _handle_adjustment_transaction(db_session, transaction_detail, inventory_entry,
                                           inventory_item, line_value, exchange_rate_id,
                                           description, current_user, app_id, posted_status)

        return True

    except Exception as e:
        logger.error(f'Error posting transaction detail {transaction_detail.id}: {e} \n{traceback.format_exc()}')
        raise Exception(f"Failed to post transaction detail {transaction_detail.id}: {str(e)}")


def _handle_stock_in_transaction(db_session, transaction_detail, inventory_entry, inventory_item,
                                 line_value, exchange_rate_id, description, current_user, app_id, posted_status):
    """
    Handle accounting entries for stock-in (purchase/production) transactions.

    Creates double-entry bookkeeping entries for inventory receipts:
    - CREDIT: Payable account (liability for amount owed to supplier)
    - DEBIT: Inventory asset account (increase in inventory assets)
    """

    # Use entry-level payable account if available, otherwise use item's default
    payable_account = inventory_entry.payable_account
    if not payable_account:
        raise ValueError("Payable account not configured")

    # Create journal with both entries
    journal, entries = create_transaction(
        db_session=db_session,
        date=transaction_detail.transaction_date,
        currency=transaction_detail.currency_id,
        created_by=current_user.id,
        app_id=app_id,
        narration=f"{description} - {inventory_item.item_name}",
        project_id=inventory_entry.project_id,
        vendor_id=inventory_entry.supplier_id,
        exchange_rate_id=exchange_rate_id,
        status=posted_status,
        journal_ref_no=inventory_entry.reference,
        lines=[
            # CREDIT Payable
            {
                "subcategory_id": payable_account.id,
                "amount": line_value,
                "dr_cr": "C",
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            },
            # DEBIT Inventory Asset
            {
                "subcategory_id": inventory_item.asset_account.id,
                "amount": line_value,
                "dr_cr": "D",
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            }
        ]
    )


def _handle_stock_out_sale_transaction(db_session, transaction_detail, inventory_entry, inventory_item,
                                       line_value, exchange_rate_id, description, current_user, app_id, posted_status):
    """
    Handle accounting entries for inventory sales transactions.

    Creates four complementary entries for sales:
    1. CREDIT Sales Revenue: Records income from sale
    2. DEBIT Payment Account: Records receivable/cash receipt
    3. DEBIT COGS: Records cost of goods sold expense
    4. CREDIT Inventory Asset: Reduces inventory assets
    """
    # Get payment account from inventory_entry.sales_account_id
    if not inventory_entry.sales_account_id:
        raise ValueError("Payment account (sales_account_id) not configured for sale")

    # Use item's sales account for revenue recognition
    sales_account = inventory_item.sales_account
    if not sales_account:
        raise ValueError("Sales revenue account not configured for inventory item")

    selling_price = transaction_detail.inventory_entry_line_item.selling_price

    # Create journal with all four entries
    journal, entries = create_transaction(
        db_session=db_session,
        date=transaction_detail.transaction_date,
        currency=transaction_detail.currency_id,
        created_by=current_user.id,
        app_id=app_id,
        narration=f"{description} - {inventory_item.item_name}",
        project_id=inventory_entry.project_id,
        vendor_id=inventory_entry.supplier_id,
        exchange_rate_id=exchange_rate_id,
        status=posted_status,
        journal_ref_no=inventory_entry.reference,
        lines=[
            # CREDIT Sales Revenue
            {
                "subcategory_id": sales_account.id,
                "amount": selling_price,
                "dr_cr": "C",
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            },
            # DEBIT Payment Account
            {
                "subcategory_id": inventory_entry.sales_account_id,
                "amount": selling_price,
                "dr_cr": "D",
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            },
            # DEBIT COGS
            {
                "subcategory_id": inventory_item.cogs_account.id,
                "amount": line_value,
                "dr_cr": "D",
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            },
            # CREDIT Inventory Asset
            {
                "subcategory_id": inventory_item.asset_account.id,
                "amount": line_value,
                "dr_cr": "C",
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            }
        ]
    )


def _handle_stock_out_write_off_transaction(db_session, transaction_detail, inventory_entry, inventory_item,
                                            line_value, exchange_rate_id, description, current_user, app_id,
                                            posted_status):
    """Handle stock out write-off transaction accounting"""
    # Use entry-level adjustment account if available, otherwise use item's COGS account
    adjustment_account = inventory_item.cogs_account
    if not adjustment_account:
        raise ValueError("COGS account not configured for write-off")

    # Create journal with both entries
    journal, entries = create_transaction(
        db_session=db_session,
        date=transaction_detail.transaction_date,
        currency=transaction_detail.currency_id,
        created_by=current_user.id,
        app_id=app_id,
        narration=f"{description} - {inventory_item.item_name}",
        project_id=inventory_entry.project_id,
        exchange_rate_id=exchange_rate_id,
        status=posted_status,
        journal_ref_no=inventory_entry.reference,
        lines=[
            # DEBIT COGS/Adjustment Account
            {
                "subcategory_id": adjustment_account.id,
                "amount": line_value,
                "dr_cr": "D",
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            },
            # CREDIT Inventory Asset
            {
                "subcategory_id": inventory_item.asset_account.id,
                "amount": line_value,
                "dr_cr": "C",
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            }
        ]
    )


def _handle_adjustment_transaction(db_session, transaction_detail, inventory_entry, inventory_item,
                                   line_value, exchange_rate_id, description, current_user, app_id, posted_status):
    """
    Handle accounting entries for inventory adjustment transactions.
    """
    # Use entry-level adjustment account if available
    adjustment_account = inventory_entry.adjustment_account
    if not adjustment_account:
        raise ValueError("Adjustment account not configured")

    # Determine adjustment type (positive = gain, negative = loss)
    adjustment_type = "D" if line_value >= 0 else "C"  # Debit for gains, Credit for losses
    asset_adjustment_type = "C" if adjustment_type == "D" else "D"

    # Create journal with both entries
    journal, entries = create_transaction(
        db_session=db_session,
        date=transaction_detail.transaction_date,
        currency=transaction_detail.currency_id,
        created_by=current_user.id,
        app_id=app_id,
        narration=f"{description} - {inventory_item.item_name}",
        project_id=inventory_entry.project_id,
        exchange_rate_id=exchange_rate_id,
        status=posted_status,
        journal_ref_no=inventory_entry.reference,
        lines=[
            # Adjustment account entry
            {
                "subcategory_id": adjustment_account.id,
                "amount": abs(line_value),
                "dr_cr": adjustment_type,
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            },
            # Opposite entry for inventory asset
            {
                "subcategory_id": inventory_item.asset_account.id,
                "amount": abs(line_value),
                "dr_cr": asset_adjustment_type,
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            }
        ]
    )


def _handle_opening_balance_transaction(db_session, transaction_detail, inventory_entry, inventory_item,
                                        line_value, exchange_rate_id, description, current_user, app_id, posted_status):
    """
    Handle accounting entries for inventory opening balance transactions.
    """
    # Get retained earnings account ID
    retained_earnings_account_id = get_retained_earnings_account_id(db_session, app_id, current_user.id)

    # Fetch the retained earnings account
    retained_earnings_account = db_session.query(ChartOfAccounts).filter(
        ChartOfAccounts.id == retained_earnings_account_id,
        ChartOfAccounts.app_id == app_id
    ).first()

    if not retained_earnings_account:
        raise ValueError("Retained earnings account not found")

    # Create journal with both entries
    journal, entries = create_transaction(
        db_session=db_session,
        date=transaction_detail.transaction_date,
        currency=transaction_detail.currency_id,
        created_by=current_user.id,
        app_id=app_id,
        narration=f"{description} - {inventory_item.item_name}",
        project_id=inventory_entry.project_id,
        exchange_rate_id=exchange_rate_id,
        status=posted_status,
        journal_ref_no=inventory_entry.reference,
        lines=[
            # DEBIT Inventory Asset
            {
                "subcategory_id": inventory_item.asset_account.id,
                "amount": line_value,
                "dr_cr": "D",
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            },
            # CREDIT Retained Earnings
            {
                "subcategory_id": retained_earnings_account.id,
                "amount": line_value,
                "dr_cr": "C",
                "description": f"{description} - {inventory_item.item_name}",
                "source_type": "inventory_transaction",
                "source_id": transaction_detail.id
            }
        ]
    )


def post_pos_transaction_to_ledger(db_session, base_currency_id, direct_sale, sale_items, current_user,
                                   items_data=None, exchange_rate=None, inventory_items_map=None, payment_method=None,
                                   payment_mode_id=None,
                                   location=None):
    """
    POS transaction posting with proper double-entry accounting.
    Each journal is perfectly balanced.
    """

    from services.inventory_helpers import get_retained_earnings_account, get_current_average_cost

    try:

        app_id = current_user.app_id

        # Skip if already posted
        if hasattr(direct_sale, 'is_posted_to_ledger') and direct_sale.is_posted_to_ledger:
            logger.info(f"POS transaction {direct_sale.id} is already posted to ledger")
            return True

        exchange_rate_id = exchange_rate.id if exchange_rate and hasattr(exchange_rate, 'id') else None
        description = f"POS Sale - {direct_sale.direct_sale_number}"
        narration = f"POS Sale Reference - {direct_sale.sale_reference}"

        # 1. GET ALL ACCOUNTS
        accounts = {}
        retained_earnings = get_retained_earnings_account(db_session, app_id)

        # Payment account - based on payment method and location configuration

        payment_method_desc = "Cash"  # Default description
        if payment_method and location:
            payment_method_lower = payment_method.lower()

            if payment_method_lower == 'cash':
                accounts['payment'] = location.payment_account if location.payment_account else retained_earnings
                payment_method_desc = "Cash"
            elif payment_method_lower == 'card':
                if not location.card_payment_account:
                    raise Exception(f"Card payment account not configured for location: {location.location}")
                accounts['payment'] = location.card_payment_account
                payment_method_desc = "Card"
            elif payment_method_lower in ['mobile', 'mobile_money', 'mobile money']:
                if not location.mobile_money_account:
                    raise Exception(f"Mobile money account not configured for location: {location.location}")
                accounts['payment'] = location.mobile_money_account
                payment_method_desc = "Mobile Money"
            else:
                # Default to cash account for unknown payment methods
                accounts['payment'] = location.payment_account if location.payment_account else retained_earnings
                payment_method_desc = payment_method.capitalize()
        else:
            # Fallback if payment_method or location is not provided
            accounts[
                'payment'] = location.payment_account if location and location.payment_account else retained_earnings

        # Sales account - from inventory items or retained earnings
        sales_account_found = False
        if items_data and inventory_items_map:
            for item_data in items_data:
                inventory_item = inventory_items_map.get(item_data['item_id'])
                if inventory_item and inventory_item.sales_account:
                    accounts['sales'] = inventory_item.sales_account
                    sales_account_found = True
                    break

        if not sales_account_found:
            accounts['sales'] = retained_earnings

        # Tax account - from inventory items or sales account
        if direct_sale.total_tax_amount > 0:
            tax_account_found = False
            if items_data and inventory_items_map:
                for item_data in items_data:
                    inventory_item = inventory_items_map.get(item_data['item_id'])
                    if inventory_item and inventory_item.tax_account:
                        accounts['tax'] = inventory_item.tax_account
                        tax_account_found = True
                        break

            if not tax_account_found:
                accounts['tax'] = accounts['sales']

        # Discount account - from location or retained earnings
        if direct_sale.calculated_discount_amount > 0:
            accounts[
                'discount'] = location.discount_account if location and location.discount_account else retained_earnings

        # 2. MAIN SALES TRANSACTION (BALANCED JOURNAL)
        # Calculate amounts
        sales_revenue_amount = float(direct_sale.total_line_subtotal)
        tax_amount = float(direct_sale.total_tax_amount)
        discount_amount = float(direct_sale.calculated_discount_amount)
        payment_received = float(direct_sale.amount_paid)

        # Get vendor ID
        vendor_id = direct_sale.customer_id if direct_sale.customer_id else None

        # Create lines for main sales journal
        sales_lines = [
            # Debit: Payment account (Cash/Bank/Receivable)
            {
                "subcategory_id": accounts['payment'].id,
                "amount": payment_received,
                "dr_cr": "D",
                "description": f"{description} - {payment_method_desc} Payment Received",
                "source_type": "pos_sale",
                "source_id": direct_sale.id
            },
            # Credit: Sales revenue
            {
                "subcategory_id": accounts['sales'].id,
                "amount": sales_revenue_amount,
                "dr_cr": "C",
                "description": f"{description} - Sales Revenue",
                "source_type": "pos_sale",
                "source_id": direct_sale.id
            }
        ]

        # Add discount entry if any
        if discount_amount > 0:
            sales_lines.append({
                "subcategory_id": accounts['discount'].id,
                "amount": discount_amount,
                "dr_cr": "D",
                "description": f"{description} - Sales Discount",
                "source_type": "pos_sale",
                "source_id": direct_sale.id
            })

        # Add tax entry if any
        if tax_amount > 0:
            sales_lines.append({
                "subcategory_id": accounts['tax'].id,
                "amount": tax_amount,
                "dr_cr": "C",
                "description": f"{description} - Tax Payable",
                "source_type": "pos_sale",
                "source_id": direct_sale.id
            })

        # Create main sales journal
        sales_journal, sales_entries = create_transaction(
            db_session=db_session,
            date=direct_sale.payment_date,
            currency=direct_sale.currency_id,
            created_by=current_user.id,
            app_id=app_id,
            narration=narration,
            payment_mode_id=payment_mode_id,
            vendor_id=vendor_id,
            project_id=location.project_id,
            exchange_rate_id=exchange_rate_id,
            status='Unposted' if location.workflow_type == 'order_slip' else 'Posted',
            lines=sales_lines
        )

        # 3. COGS ENTRIES (SEPARATE BALANCED JOURNALS FOR EACH PRODUCT)
        if items_data and inventory_items_map:
            for item_data in items_data:
                inventory_item = inventory_items_map.get(item_data['item_id'])

                if not inventory_item or not inventory_item.cogs_account or not inventory_item.asset_account:
                    continue

                # FIX: Use variation_id instead of item_id for average cost calculation
                avg_cost = get_current_average_cost(db_session, app_id, item_data['variation_id'],
                                                    location.id)
                total_cogs = avg_cost * item_data['quantity']

                if total_cogs <= 0:
                    continue

                # Create COGS journal
                cogs_journal, cogs_entries = create_transaction(
                    db_session=db_session,
                    date=direct_sale.payment_date,
                    currency=base_currency_id,  # Use base currency for COGS
                    created_by=current_user.id,
                    app_id=app_id,
                    payment_mode_id=payment_mode_id,
                    narration=f"{description} - {item_data['name']} COGS",
                    vendor_id=vendor_id,
                    lines=[
                        # DEBIT COGS
                        {
                            "subcategory_id": inventory_item.cogs_account.id,
                            "amount": float(total_cogs),
                            "dr_cr": "D",
                            "description": f"{description} - {item_data['name']} COGS",
                            "source_type": "pos_sale",
                            "source_id": direct_sale.id
                        },
                        # CREDIT Inventory Asset
                        {
                            "subcategory_id": inventory_item.asset_account.id,
                            "amount": float(total_cogs),
                            "dr_cr": "C",
                            "description": f"{description} - {item_data['name']} Inventory Reduction",
                            "source_type": "pos_sale",
                            "source_id": direct_sale.id
                        }
                    ]
                )

        # Mark as posted
        if hasattr(direct_sale, 'is_posted_to_ledger'):
            direct_sale.is_posted_to_ledger = False if location.workflow_type == 'order_slip' else True

        return True

    except Exception as e:
        logger.error(f'Error posting POS transaction {direct_sale.id}: {e} \n{traceback.format_exc()}')
        raise Exception(f"Failed to post POS transaction {direct_sale.id}: {str(e)}")


def post_invoice_to_ledger(db_session, invoice, current_user, base_currency_id, status,
                           exchange_rate_id=None, exchange_rate_value=None):
    """
    Post invoice to ledger with specified status (Unposted/Posted)
    Returns: (success, message)
    """
    try:
        app_id = current_user.app_id

        # Validate required accounts
        if not invoice.account_receivable_id or not invoice.revenue_account_id:
            return False, "Missing required account configuration"

        currency_id = invoice.currency

        # Compute amounts safely
        taxable_amount = Decimal(str(invoice.total_tax_amount or 0))
        total_amount = Decimal(str(invoice.total_amount or 0))
        non_taxable_amount = total_amount - taxable_amount

        if taxable_amount < 0 or non_taxable_amount < 0:
            return False, "Invalid invoice amounts"

        # Create journal with Unposted status
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

        # Create the journal with Unposted status
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
            status=status,  # ✅ This is the key - set to Unposted
            lines=lines
        )

        # Process COGS for inventory items with Unposted status
        inventory_items = [item for item in invoice.invoice_items if item.item_type == "inventory"]
        if inventory_items:
            cogs_success = post_invoice_cogs_to_ledger(
                db_session=db_session,
                invoice=invoice,
                exchange_rate_id=exchange_rate_id,
                exchange_rate_value=exchange_rate_value,
                current_user=current_user,
                base_currency_id=base_currency_id
            )
            if not cogs_success:
                logger.warning(f"COGS posting failed for invoice {invoice.invoice_number}")

        # Mark invoice as posted to ledger but with Unposted status
        invoice.is_posted_to_ledger = True

        # Create status log entry
        status_log = SalesInvoiceStatusLog(
            invoice_id=invoice.id,
            status="posted_to_ledger",
            changed_by=current_user.id,
            app_id=current_user.app_id
        )
        db_session.add(status_log)

        return True, f"Invoice posted to ledger with status: {status}"

    except Exception as e:
        logger.error(f"Error posting invoice {invoice.invoice_number} to ledger: {str(e)}")
        return False, str(e)


def post_invoice_cogs_to_ledger(db_session, invoice, exchange_rate_id, exchange_rate_value, current_user,
                                base_currency_id):
    """
    Post COGS entries for an invoice's inventory items with proper currency conversion
    """

    from services.inventory_helpers import process_inventory_entries
    from services.inventory_helpers import get_current_average_cost

    try:
        app_id = current_user.app_id

        # Get inventory items from the invoice
        inventory_items = [item for item in invoice.invoice_items if item.item_type == "inventory"]

        if not inventory_items:
            return True  # No inventory items to process

        # Pre-fetch all inventory item variation links in a single query
        variation_link_ids = [item.item_id for item in inventory_items if item.item_id]
        variation_links_map = {}

        if variation_link_ids:
            # Query InventoryItemVariationLink with joins to get the actual InventoryItem
            variation_links = db_session.query(InventoryItemVariationLink).filter(
                InventoryItemVariationLink.id.in_(variation_link_ids),
                InventoryItemVariationLink.app_id == app_id
            ).options(
                joinedload(InventoryItemVariationLink.inventory_item).joinedload(InventoryItem.cogs_account),
                joinedload(InventoryItemVariationLink.inventory_item).joinedload(InventoryItem.asset_account)
            ).all()

            variation_links_map = {link.id: link for link in variation_links}

        # Group items by location to process them separately
        items_by_location = {}
        for item in inventory_items:
            if not item.item_id or not item.location_id:
                continue

            if item.location_id not in items_by_location:
                items_by_location[item.location_id] = []
            items_by_location[item.location_id].append(item)
        logger.info(f'Items by location {items_by_location}')

        # Process each location separately
        for location_id, location_items in items_by_location.items():
            # Prepare data for process_inventory_entries for this location
            inventory_items_list = []
            quantities = []
            unit_prices_base = []
            selling_prices_base = []

            # Process each inventory item in this location
            for item in location_items:
                variation_link = variation_links_map.get(item.item_id)
                if not variation_link or not variation_link.inventory_item:
                    continue

                inventory_item = variation_link.inventory_item

                # Skip if missing required accounts
                if not inventory_item.cogs_account or not inventory_item.asset_account:
                    continue

                # Get average cost for this inventory item at the specific location
                avg_cost = get_current_average_cost(
                    db_session, app_id, variation_link.id, item.location_id
                )

                if avg_cost <= 0:
                    logger.warning(
                        f"Invoice {invoice.invoice_number} - Item {inventory_item.item_name} "
                        f"at location {item.location_id} has avg_cost=0. Posting COGS=0."
                    )
                    avg_cost = Decimal("0.00")

                # Convert unit price to base currency if needed

                unit_price_in_invoice_currency = Decimal(str(item.unit_price or 0))

                if exchange_rate_value:
                    unit_price_in_base_currency = unit_price_in_invoice_currency * Decimal(str(exchange_rate_value))
                else:
                    unit_price_in_base_currency = unit_price_in_invoice_currency  # No conversion needed

                # Check if quantity is a string or needs conversion
                if isinstance(item.quantity, str):
                    quantity = Decimal(item.quantity)
                else:
                    quantity = Decimal(str(item.quantity))

                total_cogs = avg_cost * quantity

                # Prepare data for process_inventory_entries
                inventory_items_list.append(variation_link.id)
                quantities.append(item.quantity)
                unit_prices_base.append(float(avg_cost))
                selling_prices_base.append(float(unit_price_in_base_currency))  # Use converted price

                # Get vendor ID
                vendor_id = invoice.customer_id if invoice.customer_id else None

                # Create COGS journal with both entries
                cogs_journal, cogs_entries = create_transaction(
                    db_session=db_session,
                    date=invoice.invoice_date,
                    currency=base_currency_id,
                    created_by=current_user.id,
                    app_id=app_id,
                    narration=f"Invoice {invoice.invoice_number} - {inventory_item.item_name}",
                    project_id=invoice.project_id,
                    vendor_id=vendor_id,
                    lines=[
                        # DEBIT COGS
                        {
                            "subcategory_id": inventory_item.cogs_account.id,
                            "amount": float(total_cogs),
                            "dr_cr": "D",
                            "description": f"Invoice {invoice.invoice_number} - {inventory_item.item_name} COGS",
                            "source_type": "sales_invoice_cogs",
                            "source_id": item.id
                        },
                        # CREDIT Inventory Asset
                        {
                            "subcategory_id": inventory_item.asset_account.id,
                            "amount": float(total_cogs),
                            "dr_cr": "C",
                            "description": f"Invoice {invoice.invoice_number} - {inventory_item.item_name} Inventory Reduction",
                            "source_type": "sales_invoice_cogs",
                            "source_id": item.id
                        }
                    ]
                )

            # Call process_inventory_entries for this location
            if inventory_items_list:
                try:
                    process_inventory_entries(
                        db_session=db_session,
                        app_id=app_id,
                        inventory_items=inventory_items_list,
                        quantities=quantities,
                        unit_prices=unit_prices_base,
                        selling_prices=selling_prices_base,
                        location=location_id,
                        from_location=location_id,
                        to_location=None,
                        transaction_date=invoice.invoice_date,
                        supplier_id=invoice.customer_id,
                        form_currency_id=base_currency_id,
                        base_currency_id=base_currency_id,
                        expiration_date=None,
                        reference=invoice.invoice_number,
                        write_off_reason=None,
                        project_id=invoice.project_id,
                        movement_type='stock_out_sale',
                        current_user_id=current_user.id,
                        source_type='sales_invoice',
                        sales_account_id=invoice.revenue_account_id,
                        exchange_rate_id=exchange_rate_id,
                        is_posted_to_ledger=True,
                        source_id=invoice.id
                    )
                except Exception as e:
                    logger.error(
                        f"Error in process_inventory_entries for invoice {invoice.invoice_number}, location {location_id}: {str(e)}")
                    raise

        return True

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error posting COGS for invoice {invoice.invoice_number}: {str(e)}", exc_info=True)
        raise Exception(f"Inventory processing failed for invoice {invoice.invoice_number}: {str(e)}")


def post_sales_transaction_to_ledger(db_session, sales_transaction, current_user, transaction_type='direct_sale',
                                     status=None, base_currency_id=None, exchange_rate_id=None,
                                     exchange_rate_value=None):
    """
    Post sales transaction (direct sale or invoice) to ledger
    using allocation-based logic and the new create_transaction function.

    Args:
        db_session: Database session
        sales_transaction: Sales transaction object
        current_user: Current user object
        transaction_type: Type of transaction ('direct_sale' or 'invoice')
        status: Ledger status (default: 'Unposted')
        base_currency_id: Fetched from the form
        base_currency_info: Optional pre-fetched base currency info
        exchange_rate_id: Optional pre-fetched from route
        exchange_rate_value: Optional pre-fetched from form

    Returns: (success, message)
    """
    try:
        app_id = current_user.app_id

        # Get base currency if not provided
        currency_id = sales_transaction.currency_id

        success_allocations = 0
        error_messages = []

        # 🔹 Direct Sale Logic (allocation based)
        if transaction_type == 'direct_sale':
            allocations = [pa for pa in sales_transaction.payment_allocations]

            if not allocations:
                return False, f"No unposted payment allocations found for direct sale {sales_transaction.direct_sale_number}"

            for allocation in allocations:
                if not allocation.payment_account:
                    error_messages.append(
                        f"Payment account not configured for allocation in direct sale {sales_transaction.direct_sale_number}"
                    )
                    continue

                journal_number = generate_unique_journal_number(db_session, app_id)
                journal_ref_no = allocation.reference

                lines = []

                # Debit Cash/Bank
                lines.append({
                    "subcategory_id": allocation.payment_account,
                    "amount": float(allocation.allocated_base_amount + allocation.allocated_tax_amount),
                    "dr_cr": "D",
                    "description": f"Payment received for direct sale {sales_transaction.direct_sale_number}",
                    "source_type": "direct_sale_payment",
                    "source_id": allocation.id
                })

                # Credit Revenue
                if not sales_transaction.revenue_account_id:
                    return False, f"Revenue account not configured for direct sale {sales_transaction.direct_sale_number}"

                lines.append({
                    "subcategory_id": sales_transaction.revenue_account_id,
                    "amount": float(allocation.allocated_base_amount),
                    "dr_cr": "C",
                    "description": f"Revenue from direct sale {sales_transaction.direct_sale_number}",
                    "source_type": "direct_sale_payment",
                    "source_id": allocation.id
                })

                # Credit Tax Payable (if any)
                if allocation.allocated_tax_amount > 0:
                    if allocation.tax_account_id:
                        lines.append({
                            "subcategory_id": allocation.tax_account_id,
                            "amount": float(allocation.allocated_tax_amount),
                            "dr_cr": "C",
                            "description": f"Tax collected for direct sale {sales_transaction.direct_sale_number}",
                            "source_type": "direct_sale_payment",
                            "source_id": allocation.id
                        })
                    else:
                        # Fallback: credit revenue if no tax payable account
                        lines.append({
                            "subcategory_id": sales_transaction.revenue_account_id,
                            "amount": float(allocation.allocated_tax_amount),
                            "dr_cr": "C",
                            "description": f"Revenue (tax fallback) for direct sale {sales_transaction.direct_sale_number}",
                            "source_type": "direct_sale_payment",
                            "source_id": allocation.id
                        })

                # Create single journal for this allocation
                create_transaction(
                    db_session=db_session,
                    date=allocation.payment_date,
                    currency=currency_id,
                    created_by=current_user.id,
                    app_id=app_id,
                    journal_number=journal_number,
                    journal_ref_no=journal_ref_no,
                    narration=f"Direct sale payment {sales_transaction.direct_sale_number}",
                    payment_mode_id=allocation.payment_mode,
                    project_id=sales_transaction.project_id,
                    vendor_id=sales_transaction.customer_id,
                    exchange_rate_id=exchange_rate_id,
                    status=status,
                    lines=lines
                )

                allocation.is_posted_to_ledger = True
                success_allocations += 1

            # Post COGS if inventory involved
            inventory_items = [item for item in sales_transaction.direct_sale_items if item.item_type == "inventory"]
            if inventory_items:
                post_direct_sale_cogs_to_ledger(
                    db_session=db_session,
                    direct_sale=sales_transaction,
                    exchange_rate_id=exchange_rate_id,
                    exchange_rate_value=exchange_rate_value,
                    current_user=current_user,
                    base_currency_id=base_currency_id
                )

        # 🔹 Invoice Logic (payment allocations clear A/R only)
        else:
            unposted_allocations = [pa for pa in sales_transaction.payment_allocations if
                                    pa.payment_id == sales_transaction.id]

            if not unposted_allocations:
                return False, f"No unposted payment allocations found for invoice {sales_transaction.invoice.invoice_number}"

            for allocation in unposted_allocations:
                if not allocation.payment_account:
                    error_messages.append(
                        f"Payment account not configured for allocation in invoice {sales_transaction.invoice.invoice_number}"
                    )
                    continue

                journal_number = generate_unique_journal_number(db_session, app_id)
                journal_ref_no = allocation.reference

                lines = []

                # Debit Cash/Bank
                lines.append({
                    "subcategory_id": allocation.payment_account,
                    "amount": float(allocation.allocated_base_amount + allocation.allocated_tax_amount),
                    "dr_cr": "D",
                    "description": f"Payment received for invoice {sales_transaction.invoice.invoice_number}",
                    "source_type": "invoice_payment",
                    "source_id": allocation.id
                })

                # Credit Accounts Receivable
                if not sales_transaction.invoice.account_receivable_id:
                    return False, f"A/R account not configured for invoice {sales_transaction.invoice.invoice_number}"

                lines.append({
                    "subcategory_id": sales_transaction.invoice.account_receivable_id,
                    "amount": float(allocation.allocated_base_amount + allocation.allocated_tax_amount),
                    "dr_cr": "C",
                    "description": f"Payment applied to invoice {sales_transaction.invoice.invoice_number}",
                    "source_type": "invoice_payment",
                    "source_id": allocation.id
                })

                # Create single journal for this allocation
                create_transaction(
                    db_session=db_session,
                    date=allocation.payment_date,
                    currency=currency_id,
                    created_by=current_user.id,
                    app_id=app_id,
                    journal_number=journal_number,
                    journal_ref_no=journal_ref_no,
                    narration=f"Invoice payment {sales_transaction.invoice.invoice_number}",
                    payment_mode_id=allocation.payment_mode,
                    project_id=sales_transaction.invoice.project_id,
                    vendor_id=sales_transaction.customer_id,
                    exchange_rate_id=exchange_rate_id,
                    status=status,
                    lines=lines
                )

                allocation.is_posted_to_ledger = True
                success_allocations += 1

        # ✅ Mark transaction header as posted
        sales_transaction.is_posted_to_ledger = True

        return True, f"{transaction_type.replace('_', ' ').title()} posted to ledger with {success_allocations} allocations"

    except Exception as e:
        logger.error(f"Error posting {transaction_type} to ledger: {str(e)}")
        return False, str(e)


def post_customer_credit_to_ledger(db_session, payment_account_id, currency_id,
                                   customer_credit_account_id, current_user,
                                   payment_allocation,  # ✅ NEW: Accept payment allocation ID
                                   status=None, project_id=None, exchange_rate_id=None):
    """
    Post customer credit (overpayment) to ledger

    Args:
        db_session: Database session
        currency_id: Currency ID of Invoice
        payment_account_id: ID of the payment account (cash/bank)
        customer_credit_account_id: ID of the customer credit liability account
        current_user: Current user object
        payment_allocationd: ORM of the payment allocation that created this credit
        base_currency_info: Dictionary with base currency info (optional - will query if not provided)
        status: Ledger status (default: 'Unposted')
    """
    try:
        app_id = current_user.app_id

        journal_number = generate_unique_journal_number(db_session, app_id)
        credit_amount = float(payment_allocation.overpayment_amount)

        lines = [{
            "subcategory_id": payment_account_id,
            "amount": credit_amount,
            "dr_cr": "D",
            "description": f"Overpayment received from {payment_allocation.invoices.customer.vendor_name}",
            "source_type": "customer_credit",
            "source_id": payment_allocation.id  # ✅ Link to payment allocation
        }, {
            "subcategory_id": customer_credit_account_id,
            "amount": credit_amount,
            "dr_cr": "C",
            "description": f"Customer credit created - Ref: {payment_allocation.reference}",
            "source_type": "customer_credit",
            "source_id": payment_allocation.id  # ✅ Link to payment allocation
        }]

        # Debit Cash/Bank - ✅ Use payment_allocation_id as source_id

        # Credit Customer Credit Account - ✅ Use payment_allocation_id as source_id

        # Create journal entry
        create_transaction(
            db_session=db_session,
            date=payment_allocation.payment_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal_number,
            journal_ref_no=payment_allocation.reference,
            narration=f"Customer credit for overpayment - {payment_allocation.invoices.customer.vendor_name}",
            payment_mode_id=None,
            project_id=project_id,
            vendor_id=payment_allocation.invoices.customer_id,
            exchange_rate_id=exchange_rate_id,
            status=status,
            lines=lines
        )

        return True, "Customer credit posted to ledger successfully"

    except Exception as e:
        logger.error(f"Error posting customer credit to ledger: {str(e)}\n{traceback.format_exc()}")
        return False, str(e)


def post_overpayment_write_off_to_ledger(db_session, payment_account_id,
                                         write_off_account_id, write_off_amount,
                                         current_user, payment_allocation_id,
                                         invoice, status=None, project_id=None,
                                         exchange_rate_id=None, exchange_rate_value=None):
    """
    Post overpayment write-off to ledger - Simple balanced entry
    Debit Payment Account, Credit Write-off Account

    Args:
        db_session: Database session
        payment_account_id: ID of the payment account (cash/bank)
        write_off_account_id: ID of the write-off income account
        write_off_amount: Amount to write off
        current_user: Current user object
        payment_allocation_id: ID of the payment allocation
        invoice: Invoice object for reference
        base_currency_info: Base currency info
        status: Ledger status
        project_id: project id
    """
    try:
        app_id = current_user.app_id

        currency_id = invoice.currency

        journal_number = generate_unique_journal_number(db_session, app_id)
        amount = float(write_off_amount)

        lines = [{
            "subcategory_id": payment_account_id,
            "amount": amount,
            "dr_cr": "D",
            "description": f"Overpayment received from {invoice.customer.vendor_name}",
            "source_type": "overpayment_write_off",
            "source_id": payment_allocation_id
        }, {
            "subcategory_id": write_off_account_id,
            "amount": amount,
            "dr_cr": "C",
            "description": f"Overpayment write-off income for invoice {invoice.invoice_number}",
            "source_type": "overpayment_write_off",
            "source_id": payment_allocation_id
        }]

        # Debit Payment Account (Cash/Bank)

        # Credit Write-off Income Account

        # Create journal entry
        create_transaction(
            db_session=db_session,
            date=invoice.invoice_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal_number,
            journal_ref_no=f"WO-{invoice.invoice_number}",
            narration=f"Overpayment write-off - {invoice.customer.vendor_name}",
            payment_mode_id=None,
            project_id=project_id,
            vendor_id=invoice.customer_id,
            exchange_rate_id=exchange_rate_id,
            status=status,
            lines=lines
        )

        return True, "Overpayment write-off posted to ledger successfully"

    except Exception as e:
        logger.error(f"Error posting overpayment write-off to ledger: {str(e)}\n{traceback.format_exc()}")
        return False, str(e)


def post_credit_write_off_to_ledger(db_session, credit, write_off_amount, current_user, source_id, invoice, status=None,
                                    project_id=None):
    """
    Post credit write-off to ledger using system accounts
    Debit Customer Credit Account, Credit Write-off Account

    Args:
        db_session: Database session
        credit: CustomerCredit object being written off
        write_off_amount: Amount to write off
        current_user: Current user object
        source_id: ID of the source (payment allocation or bulk payment)
        invoice: Invoice object for reference
        status: Ledger status
        project_id: project id
    """
    try:
        app_id = current_user.app_id
        journal_number = generate_unique_journal_number(db_session, app_id)
        amount = float(write_off_amount)

        # ===== GET SYSTEM ACCOUNTS =====

        system_accounts = get_all_system_accounts(
            db_session=db_session,
            app_id=app_id,
            created_by_user_id=current_user.id
        )

        suspense_account_id = system_accounts['suspense']
        fx_gain_loss_account_id = system_accounts['fx_gain_loss']
        customer_credit_account_id = system_accounts.get('customer_credit')  # May be None
        write_off_account_id = system_accounts.get('write_off')  # May be None



        if not customer_credit_account_id or not write_off_account_id:
            raise ValueError("Failed to get system accounts for write-off")

        # Get currency from credit
        currency_id = credit.currency_id

        lines = [
            {
                "subcategory_id": customer_credit_account_id,  # Debit the liability account
                "amount": amount,
                "dr_cr": "D",
                "description": f"Credit write-off - reducing customer credit liability",
                "source_type": "credit_write_off",
                "source_id": source_id
            },
            {
                "subcategory_id": write_off_account_id,  # Credit the income account
                "amount": amount,
                "dr_cr": "C",
                "description": f"Credit write-off income",
                "source_type": "credit_write_off",
                "source_id": source_id
            }
        ]

        # Create journal entry
        create_transaction(
            db_session=db_session,
            date=datetime.now().date(),
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal_number,
            journal_ref_no=f"WO-{credit.id}",
            narration=f"Credit #{credit.id} write-off - {invoice.customer.vendor_name if invoice else 'Customer credit'}",
            payment_mode_id=None,
            project_id=project_id,
            vendor_id=invoice.customer_id if invoice else None,
            exchange_rate_id=credit.exchange_rate_id,
            status=status,
            lines=lines
        )

        return True, "Credit write-off posted to ledger successfully"

    except Exception as e:
        logger.error(f"Error posting credit write-off to ledger: {str(e)}\n{traceback.format_exc()}")
        return False, str(e)


def post_credit_application_to_ledger(db_session, sales_transaction, payment_allocation,
                                      customer_credit_account_id, current_user,
                                      base_currency_info=None, status=None):
    """
    Post credit application to ledger - Debit A/R, Credit Customer Credit account

    Args:
        db_session: Database session
        sales_transaction: SalesTransaction object
        payment_allocation: PaymentAllocation object
        customer_credit_account_id: ID of customer credit liability account
        current_user: Current user object
        base_currency_info: Base currency info
        status: Ledger status
    """
    try:
        app_id = current_user.app_id

        # Get base currency if not provided
        if base_currency_info is None:
            base_currency_info = get_base_currency(db_session, app_id)

        if not base_currency_info:
            return False, "Base currency not configured"

        base_currency_id = base_currency_info["base_currency_id"]
        currency_id = sales_transaction.currency_id

        # Exchange rate if foreign currency
        exchange_rate_id = None
        if int(base_currency_id) != int(currency_id):
            exchange_rate_obj, exchange_rate_value = get_exchange_rate_and_obj(
                db_session, currency_id, base_currency_id, app_id, sales_transaction.payment_date
            )
            exchange_rate_id = exchange_rate_obj.id if exchange_rate_obj else None

        journal_number = generate_unique_journal_number(db_session, app_id)

        # Get invoice details
        invoice = sales_transaction.invoice
        if not invoice:
            return False, "Invoice not found for sales transaction"

        # Get A/R account from invoice
        if not invoice.account_receivable_id:
            return False, "A/R account not configured for invoice"

        amount = float(sales_transaction.amount_paid)

        lines = []

        # Debit Accounts Receivable (A/R increases - customer owes less)
        lines.append({
            "subcategory_id": invoice.account_receivable_id,
            "amount": amount,
            "dr_cr": "D",
            "description": f"Credit application to invoice {invoice.invoice_number}",
            "source_type": "credit_application",
            "source_id": payment_allocation.id if payment_allocation else sales_transaction.id
        })

        # Credit Customer Credit Account (liability decreases - credit used up)
        lines.append({
            "subcategory_id": customer_credit_account_id,
            "amount": amount,
            "dr_cr": "C",
            "description": f"Customer credit applied to invoice {invoice.invoice_number}",
            "source_type": "credit_application",
            "source_id": payment_allocation.id if payment_allocation else sales_transaction.id
        })

        # Create journal entry
        create_transaction(
            db_session=db_session,
            date=sales_transaction.payment_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal_number,
            journal_ref_no=sales_transaction.reference_number,
            narration=f"Customer credit application - {invoice.customer.vendor_name}",
            payment_mode_id=None,
            project_id=invoice.project_id,
            vendor_id=invoice.customer_id,
            exchange_rate_id=exchange_rate_id,
            status=status,
            lines=lines
        )

        # Mark sales transaction as posted to ledger
        sales_transaction.is_posted_to_ledger = False

        logger.info(f"Credit application posted to ledger for invoice {invoice.invoice_number}, amount: {amount}")
        return True, "Credit application posted to ledger successfully"

    except Exception as e:
        logger.error(f"Error posting credit application to ledger: {str(e)}")
        return False, str(e)


def post_payment_receipt_to_ledger(db_session, receipt, allocations,
                                   payment_account_id, current_user,
                                   suspense_account_id,
                                   overpayment_action=None,
                                   customer_credit_account_id=None,
                                   write_off_account_id=None,
                                   fx_gain_loss_account_id=None,
                                   status=None, project_id=None,
                                   exchange_rate_id=None,
                                   base_currency_id=None,
                                   payment_currency_id=None,
                                   overpayment_amount=None):
    """
    ===========================================================================
    POST PAYMENT RECEIPT TO LEDGER (MULTI-CURRENCY WITH SUSPENSE)
    ===========================================================================

     HANDLES TWO SCENARIOS:
    ----------------------
    1. BULK PAYMENT (receipt exists)
       - Multiple invoices, possibly different currencies
       - Payment currency is from receipt (e.g., SSP)
       - Credits created in PAYMENT CURRENCY
       - Called from process_payment()

    2. SINGLE INVOICE PAYMENT (receipt is None)
       - One invoice, payment may be in different currency
       - Payment currency MUST be passed via payment_currency_id parameter
       - Credits created in INVOICE CURRENCY
       - Called from record_payment() and edit_sales_transaction()

    CRITICAL: payment_currency_id MUST be provided when receipt is None!
              Otherwise journal entries will incorrectly use invoice currency.
    ===========================================================================

    WHAT THIS FUNCTION DOES:
    ------------------------
    Posts a customer payment to the general ledger using a suspense account as
    a bridge for multi-currency transactions. Handles:
    - Payment receipt in any currency
    - Allocation to multiple invoices in different currencies
    - Tax splitting (base amount vs tax amount)
    - FX gain/loss calculation and recording
    - Overpayment handling (customer credit or write-off)

    HOW IT WORKS:
    -------------
    JOURNAL 1 (Payment Currency):
        Dr Cash/Bank Account          (Payment amount)
           Cr Suspense Account            (Payment amount)
        → Records money received, held in suspense

    JOURNAL 2-n (Invoice Currency - for each invoice):
        Dr Suspense Account            (Full invoice amount)
           Cr Accounts Receivable        (Net amount - excluding tax)
           Cr Tax Payable Account        (Tax amount - if applicable)
        → Applies payment to invoice, splits tax if needed

    JOURNAL 3 (Base Currency - if FX difference exists):
        For GAIN (payment_base > invoice_base):
            Dr Suspense Account          (FX gain amount)
               Cr FX Gain Account           (FX gain amount)

        For LOSS (payment_base < invoice_base):
            Dr FX Loss Account           (FX loss amount)
               Cr Suspense Account          (FX loss amount)
        → Records realized FX gain/loss from allocation

    JOURNAL 4 (Overpayment handling):
        💡 CURRENCY VARIES BY SCENARIO:
           - Bulk payment: PAYMENT CURRENCY (e.g., SSP)
           - Single invoice: INVOICE CURRENCY (e.g., USD)

        Dr/Cr Suspense Account        (Overpayment amount)
           Cr Customer Credit Account    (Overpayment amount)
        → Moves overpayment to customer credit (or write-off)
        
    KEY PARAMETERS:
    ---------------
    db_session                      : Database session
    receipt                         : BulkPayment object being processed
    allocations                     : List of allocation details containing:
        - invoice                    : SalesInvoice object
        - amount_payment             : Amount in payment currency
        - amount                     : Amount in invoice currency
        - allocation                 : PaymentAllocation object (with tax split)

    payment_account_id               : Account where money is deposited (Cash/Bank)
    suspense_account_id              : Temporary holding account for bridging
    customer_credit_account_id       : Account for customer credits (liability)
    write_off_account_id             : Account for write-offs (expense)
    fx_gain_loss_account_id          : Account for FX gains/losses
    overpayment_amount               : Passing this for single invoice record payment

    IMPORTANT NOTES:
    ----------------
    1. SINGLE CURRENCY PER JOURNAL : Each journal uses only one currency
    2. SUSPENSE IS MULTI-CURRENCY   : Tracks amounts in all currencies
    3. TAX HANDLING                 : Uses allocated_base/allocated_tax from PaymentAllocation
    4. FX SIGN RULE (for sales)     : Positive fx_diff = GAIN (Cr), Negative = LOSS (Dr)
    5. SUSPENSE ALWAYS ZEROS OUT    : After all journals, suspense balance = 0

    EXAMPLE SCENARIO:
    -----------------
    Payment: 100 EUR (rate 4000 = 400,000 UGX)
    Invoice: 100 USD (rate 3800 = 380,000 UGX) with tax 10%

    Journal 1 (EUR):
        Dr Cash                    100 EUR
           Cr Suspense                100 EUR

    Journal 2 (USD):
        Dr Suspense                 100 USD
           Cr AR (net)                 90 USD
           Cr Tax Payable               10 USD

    Journal 3 (UGX):
        Dr Suspense              20,000 UGX  (Gain: 400,000 - 380,000)
           Cr FX Gain                20,000 UGX


    ===========================================================================
    """
    try:
        app_id = current_user.app_id

        if receipt:
            # Handle case with receipt
            payment_amount = float(receipt.total_amount)
            payment_currency = receipt.currency_id
            receipt_ref = receipt.bulk_payment_number or f"RCPT-{receipt.reference}"
            payment_rate = float(receipt.exchange_rate.rate) if receipt.exchange_rate else 1
            customer_name = receipt.customer.vendor_name
            payment_date = receipt.payment_date
            customer_id = receipt.customer_id
            payment_method = receipt.payment_method
            receipt_id_for_source = receipt.id
            receipt_source_type = "payment_receipt"
            receipt_source_type_alloc = "receipt_allocation"
            is_bulk_payment = True
        else:
            # Handle case without receipt (single invoice payments)
            # Calculate payment amount from allocations
            payment_amount = sum(float(alloc.get('amount_payment', 0)) for alloc in allocations)

            # Get other values from first allocation
            first_alloc = allocations[0]
            invoice = first_alloc['invoice']
            allocation_obj = first_alloc['allocation']

            payment_currency = payment_currency_id
            payment_date = allocation_obj.payment_date
            customer_id = invoice.customer_id
            customer_name = invoice.customer.vendor_name
            payment_method = allocation_obj.payment_mode
            receipt_ref = allocation_obj.reference or f"PMT-{allocation_obj.id}"
            receipt_source_type = "invoice_payment"
            receipt_source_type_alloc = "invoice_payment"
            receipt_id_for_source = allocation_obj.id
            payment_rate = 1  # Default, will be overridden if exchange_rate_id provided
            is_bulk_payment = False
            # Get exchange rate if provided
            if exchange_rate_id:
                exchange_rate = db_session.query(ExchangeRate).get(exchange_rate_id)
                payment_rate = float(exchange_rate.rate) if exchange_rate else 1

        # Get base currency for calculations
        if not base_currency_id:
            base_currency_info = get_base_currency(db_session, app_id)
            base_currency_id = base_currency_info["base_currency_id"]

        # ===== JOURNAL 1: Record payment receipt (in payment currency - EUR) =====
        journal1_number = generate_unique_journal_number(db_session, app_id)

        lines_journal1 = [{
            "subcategory_id": payment_account_id,
            "amount": payment_amount,
            "dr_cr": "D",
            "description": f'Payment received from {customer_name} " " {receipt_ref}',
            "source_type": receipt_source_type,
            "source_id": receipt_id_for_source
        }, {
            "subcategory_id": suspense_account_id,
            "amount": payment_amount,
            "dr_cr": "C",
            "description": f"Funds held in suspense for allocation for {receipt_ref}",
            "source_type": receipt_source_type,
            "source_id": receipt_id_for_source
        }]

        create_transaction(
            db_session=db_session,
            date=payment_date,
            currency=payment_currency,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal1_number,
            journal_ref_no=f"{receipt_ref}",
            narration=f"Payment received - {customer_name} for {receipt_ref}",
            payment_mode_id=payment_method,
            project_id=project_id,
            vendor_id=customer_id,
            exchange_rate_id=exchange_rate_id,
            status=status,
            lines=lines_journal1
        )

        # ===== JOURNAL 2-n: Apply to each invoice (in invoice currency) =====
        # ===== JOURNAL 2-n: Apply to each invoice (in invoice currency) =====
        total_allocated_in_payment = 0
        total_fx_loss_base = 0  # Track total FX loss in base currency

        for alloc in allocations:
            invoice = alloc['invoice']
            amount_in_payment = float(alloc.get('amount_payment', 0))
            amount_in_invoice = float(alloc.get('amount', 0))
            payment_allocation_id = alloc.get('allocation').id

            # Get tax allocation from the payment_allocation object
            payment_allocation = alloc.get('allocation')
            allocated_base = float(
                payment_allocation.allocated_base_amount) if payment_allocation else amount_in_invoice
            allocated_tax = float(payment_allocation.allocated_tax_amount) if payment_allocation else 0

            total_allocated_in_payment += amount_in_payment

            # Calculate amount in base at invoice's historical rate
            if invoice.exchange_rate:
                invoice_rate = float(invoice.exchange_rate.rate)
                amount_at_invoice_rate = amount_in_invoice * invoice_rate
                invoice_exchange_rate_id = invoice.exchange_rate_id
            else:
                invoice_rate = 1
                amount_at_invoice_rate = amount_in_invoice
                invoice_exchange_rate_id = None

            # Calculate FX difference for this invoice in BASE CURRENCY
            if invoice.currency != base_currency_id:
                # Get the actual allocated amount in invoice currency
                actual_allocated_in_invoice_currency = allocated_base + allocated_tax  # Without over payment

                # Convert to base currency using invoice rate
                payment_base = actual_allocated_in_invoice_currency * payment_rate

                fx_diff = payment_base - amount_at_invoice_rate
                total_fx_loss_base += fx_diff

                logger.info(f"Creating allocation \n amount_in_payment (total): "
                            f"{amount_in_payment}\n payment_portion: {actual_allocated_in_invoice_currency}\n payment_rate: "
                            f"{payment_rate}\n amount_at_invoice_rate: {amount_at_invoice_rate}\n Invoice rate: "
                            f"{invoice_rate}\n fx_diff: {fx_diff}\n total_fx_loss_base: {total_fx_loss_base}")

            # Create journal in invoice currency with tax split
            journal_number = generate_unique_journal_number(db_session, app_id)

            lines = [{
                "subcategory_id": suspense_account_id,
                "amount": amount_in_invoice,  # Full amount in invoice currency
                "dr_cr": "D",
                "description": f"Funds from suspense applied to Invoice {invoice.invoice_number}",
                "source_type": receipt_source_type_alloc,
                "source_id": payment_allocation_id
            }]

            # Split between AR and tax payable if tax exists
            if allocated_tax > 0 and invoice.tax_account_id:
                # Credit AR with base amount (excluding tax)
                lines.append({
                    "subcategory_id": invoice.account_receivable_id,
                    "amount": allocated_base,
                    "dr_cr": "C",
                    "description": f"Payment applied to Invoice {invoice.invoice_number} (net)",
                    "source_type": receipt_source_type_alloc,
                    "source_id": payment_allocation_id
                })
                # Credit tax payable with tax amount
                lines.append({
                    "subcategory_id": invoice.tax_account_id,
                    "amount": allocated_tax,
                    "dr_cr": "C",
                    "description": f"Tax portion of payment for Invoice {invoice.invoice_number}",
                    "source_type": receipt_source_type_alloc,
                    "source_id": payment_allocation_id
                })
            else:
                # No tax - credit full amount to AR
                lines.append({
                    "subcategory_id": invoice.account_receivable_id,
                    "amount": amount_in_invoice,
                    "dr_cr": "C",
                    "description": f"Payment applied to Invoice {invoice.invoice_number}",
                    "source_type": receipt_source_type_alloc,
                    "source_id": payment_allocation_id
                })

            create_transaction(
                db_session=db_session,
                date=payment_date,
                currency=invoice.currency,
                created_by=current_user.id,
                app_id=app_id,
                journal_number=journal_number,
                journal_ref_no=f"{receipt_ref}",
                narration=f"Payment applied to Invoice {invoice.invoice_number}",
                payment_mode_id=None,
                project_id=project_id,
                vendor_id=customer_id,
                exchange_rate_id=invoice_exchange_rate_id,
                status=status,
                lines=lines
            )
        # ===== JOURNAL 3: Record FX loss on allocations (in BASE CURRENCY) =====
        if abs(total_fx_loss_base) > 0.01 and fx_gain_loss_account_id:
            journal3_number = generate_unique_journal_number(db_session, app_id)

            # FX loss journal in base currency against suspense
            # For SALES (receivables): positive fx_diff = GAIN, negative fx_diff = LOSS
            if total_fx_loss_base > 0:  # GAIN
                lines_journal3 = [{
                    "subcategory_id": suspense_account_id,
                    "amount": abs(total_fx_loss_base),
                    "dr_cr": "D",  # Increase suspense
                    "description": "Adjust suspense for FX gain on allocations",
                    "source_type": receipt_source_type,
                    "source_id": receipt_id_for_source
                }, {
                    "subcategory_id": fx_gain_loss_account_id,
                    "amount": abs(total_fx_loss_base),
                    "dr_cr": "C",  # Gain is credit
                    "description": "Foreign exchange gain on invoice allocations",
                    "source_type": receipt_source_type,
                    "source_id": receipt_id_for_source
                }]
            else:  # LOSS (total_fx_loss_base < 0)
                lines_journal3 = [{
                    "subcategory_id": fx_gain_loss_account_id,
                    "amount": abs(total_fx_loss_base),
                    "dr_cr": "D",  # Loss is debit
                    "description": "Foreign exchange loss on invoice allocations",
                    "source_type": receipt_source_type,
                    "source_id": receipt_id_for_source
                }, {
                    "subcategory_id": suspense_account_id,
                    "amount": abs(total_fx_loss_base),
                    "dr_cr": "C",  # Decrease suspense
                    "description": "Adjust suspense for FX loss on allocations",
                    "source_type": receipt_source_type,
                    "source_id": receipt_id_for_source
                }]

            create_transaction(
                db_session=db_session,
                date=payment_date,
                currency=base_currency_id,  # Base currency journal
                created_by=current_user.id,
                app_id=app_id,
                journal_number=journal3_number,
                journal_ref_no=f"{receipt_ref}-FX",
                narration=f"FX adjustment on allocations - {customer_name} - {receipt_ref}",
                payment_mode_id=None,
                project_id=project_id,
                vendor_id=customer_id,
                exchange_rate_id=None,  # No exchange rate for base currency
                status=status,
                lines=lines_journal3
            )

        # ===== JOURNAL 4: Close suspense to customer credit (in payment currency) =====
        # 💡 CURRENCY VARIES BY SCENARIO:
        #    - Bulk payment: PAYMENT CURRENCY (SSP)
        #    - Single invoice: INVOICE CURRENCY (USD)
        if is_bulk_payment:
            remaining = payment_amount - total_allocated_in_payment
        else:
            remaining = float(overpayment_amount)

        if abs(remaining) > 0.01:
            journal4_number = generate_unique_journal_number(db_session, app_id)
            lines_journal4 = []

            if is_bulk_payment:
                # SCENARIO 1: Bulk payment - credit in PAYMENT CURRENCY (SSP)
                credit_currency = payment_currency
                credit_exchange_rate_id = exchange_rate_id
            else:
                # SCENARIO 2: Single invoice - credit in INVOICE CURRENCY (USD)
                credit_currency = allocations[0]['invoice'].currency
                credit_exchange_rate_id = None  # No exchange rate for base currency credit

            # Close suspense account
            lines_journal4.append({
                "subcategory_id": suspense_account_id,
                "amount": abs(remaining),
                "dr_cr": "D" if remaining > 0 else "C",
                "description": "Closing suspense account",
                "source_type": receipt_source_type,
                "source_id": receipt_id_for_source
            })

            # Handle overpayment disposition
            if overpayment_action == 'write_off' and write_off_account_id:
                lines_journal4.append({
                    "subcategory_id": write_off_account_id,
                    "amount": abs(remaining),
                    "dr_cr": "C",
                    "description": "Payment written off as income",
                    "source_type": receipt_source_type,
                    "source_id": receipt_id_for_source
                })
            else:
                lines_journal4.append({
                    "subcategory_id": customer_credit_account_id,
                    "amount": abs(remaining),
                    "dr_cr": "C",
                    "description": "Customer credit for overpayment",
                    "source_type": receipt_source_type,
                    "source_id": receipt_id_for_source
                })

            create_transaction(
                db_session=db_session,
                date=payment_date,
                currency=credit_currency,
                created_by=current_user.id,
                app_id=app_id,
                journal_number=journal4_number,
                journal_ref_no=f"{receipt_ref}-SUS",
                narration=f"Close suspense - {customer_name}",
                payment_mode_id=None,
                project_id=project_id,
                vendor_id=customer_id,
                exchange_rate_id=credit_exchange_rate_id,
                status=status,
                lines=lines_journal4
            )

        return True, "Payment receipt posted to ledger successfully"

    except Exception as e:
        logger.error(f"Error posting payment receipt to ledger: {str(e)}\n{traceback.format_exc()}")
        return False, str(e)


def post_customer_credit_to_invoice(db_session, payment_allocation, credit, invoice,
                                    amount_in_credit,  # Amount in credit currency (EUR)
                                    amount_in_invoice,  # Full amount in invoice currency (USD)
                                    current_user, suspense_account_id,
                                    customer_credit_account_id=None,
                                    fx_gain_loss_account_id=None,
                                    status=None, project_id=None):
    """
    Apply customer credit to an invoice using suspense account for multi-currency bridging

    This function handles:
    - Moving customer credit to suspense (in credit currency)
    - Applying suspense to invoice with tax split (in invoice currency)
    - Recording any FX gain/loss if currencies differ

    Args:
        db_session: Database session
        payment_allocation: PaymentAllocation object (with tax split data)
        credit: CustomerCredit object being applied
        invoice: Invoice object being paid
        amount_in_credit: Amount in credit currency to apply (e.g., 86.67 EUR)
        amount_in_invoice: Full amount in invoice currency (e.g., 86.67 USD)
        current_user: Current user object
        suspense_account_id: ID of suspense account
        customer_credit_account_id: ID of customer credit account
        fx_gain_loss_account_id: ID of FX gain/loss account
        status: Journal status (default: 'Posted')
        project_id: Optional project ID

    Returns:
        (success, message) tuple
    """
    try:
        app_id = current_user.app_id
        credit_amount = float(amount_in_credit)
        invoice_amount = float(amount_in_invoice)
        credit_currency = credit.currency_id
        invoice_currency = invoice.currency
        credit_ref = f"CR-{credit.id}"
        invoice_ref = invoice.invoice_number

        # Get tax allocation from payment_allocation
        allocated_base = float(payment_allocation.allocated_base_amount) if payment_allocation else invoice_amount
        allocated_tax = float(payment_allocation.allocated_tax_amount) if payment_allocation else 0
        payment_allocation_id = payment_allocation.id

        # Get base currency for calculations
        base_currency_info = get_base_currency(db_session, app_id)
        base_currency_id = base_currency_info["base_currency_id"]

        # Get rates
        credit_rate = float(credit.exchange_rate.rate) if credit.exchange_rate else 1
        invoice_rate = float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1

        # Calculate base equivalents for FX
        credit_base = credit_amount * credit_rate
        invoice_base = invoice_amount * invoice_rate
        fx_difference = invoice_base - credit_base

        logger.info(f"=== APPLYING CREDIT TO INVOICE ===")
        logger.info(f"Credit: {credit_amount} {credit_currency} @ {credit_rate} = {credit_base} UGX")
        logger.info(f"Invoice: {invoice_amount} {invoice_currency} @ {invoice_rate} = {invoice_base} UGX")
        logger.info(f"Tax Split: Base={allocated_base}, Tax={allocated_tax}")
        logger.info(
            f"FX Difference: {fx_difference} {'GAIN' if fx_difference > 0 else 'LOSS' if fx_difference < 0 else 'NONE'}")

        # ===== JOURNAL 1: Move credit to suspense (in credit currency) =====
        journal1_number = generate_unique_journal_number(db_session, app_id)

        lines_journal1 = [{
            "subcategory_id": customer_credit_account_id,
            "amount": credit_amount,
            "dr_cr": "D",  # Decreasing liability (Customer Credit)
            "description": f"Move credit to suspense for Invoice {invoice_ref}",
            "source_type": "credit_application",
            "source_id": payment_allocation_id
        }, {
            "subcategory_id": suspense_account_id,
            "amount": credit_amount,
            "dr_cr": "C",  # Increasing suspense
            "description": f"Credit funds in suspense for Invoice {invoice_ref}",
            "source_type": "credit_application",
            "source_id": payment_allocation_id
        }]

        create_transaction(
            db_session=db_session,
            date=datetime.now().date(),
            currency=credit_currency,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal1_number,
            journal_ref_no=f"{credit_ref}-TO-SUSPENSE",
            narration=f"Move credit to suspense - {invoice_ref}",
            payment_mode_id=None,
            project_id=project_id,
            vendor_id=invoice.customer_id,
            exchange_rate_id=credit.exchange_rate_id,
            status=status,
            lines=lines_journal1
        )

        # ===== JOURNAL 2: Apply suspense to invoice with tax split (in invoice currency) =====
        journal2_number = generate_unique_journal_number(db_session, app_id)

        # Start with suspense debit
        lines_journal2 = [{
            "subcategory_id": suspense_account_id,
            "amount": invoice_amount,  # Full amount in invoice currency
            "dr_cr": "D",  # Decreasing suspense
            "description": f"Apply suspense to Invoice {invoice_ref}",
            "source_type": "credit_application",
            "source_id": payment_allocation_id
        }]

        # Split between AR and tax payable if tax exists
        if allocated_tax > 0 and invoice.tax_account_id:
            # Credit AR with base amount (excluding tax)
            lines_journal2.append({
                "subcategory_id": invoice.account_receivable_id,
                "amount": allocated_base,
                "dr_cr": "C",
                "description": f"Credit applied to Invoice {invoice_ref} (net)",
                "source_type": "credit_application",
                "source_id": payment_allocation_id
            })
            # Credit tax payable with tax amount
            lines_journal2.append({
                "subcategory_id": invoice.tax_account_id,
                "amount": allocated_tax,
                "dr_cr": "C",
                "description": f"Tax portion of credit for Invoice {invoice_ref}",
                "source_type": "credit_application",
                "source_id": payment_allocation_id
            })
        else:
            # No tax - credit full amount to AR
            lines_journal2.append({
                "subcategory_id": invoice.account_receivable_id,
                "amount": invoice_amount,
                "dr_cr": "C",
                "description": f"Credit applied to Invoice {invoice_ref}",
                "source_type": "credit_application",
                "source_id": payment_allocation_id
            })

        create_transaction(
            db_session=db_session,
            date=datetime.now().date(),
            currency=invoice_currency,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal2_number,
            journal_ref_no=f"{credit_ref}-TO-INVOICE",
            narration=f"Apply credit to Invoice {invoice_ref}",
            payment_mode_id=None,
            project_id=project_id,
            vendor_id=invoice.customer_id,
            exchange_rate_id=invoice.exchange_rate_id,
            status=status,
            lines=lines_journal2
        )

        # ===== JOURNAL 3: Handle FX gain/loss in base currency =====
        if abs(fx_difference) > 0.01 and fx_gain_loss_account_id:
            journal3_number = generate_unique_journal_number(db_session, app_id)

            if fx_difference > 0:  # GAIN
                lines_journal3 = [{
                    "subcategory_id": suspense_account_id,
                    "amount": abs(fx_difference),
                    "dr_cr": "D",  # Increase suspense
                    "description": f"FX gain on credit application",
                    "source_type": "credit_application",
                    "source_id": payment_allocation_id
                }, {
                    "subcategory_id": fx_gain_loss_account_id,
                    "amount": abs(fx_difference),
                    "dr_cr": "C",  # Gain is credit
                    "description": f"Foreign exchange gain on credit application",
                    "source_type": "credit_application",
                    "source_id": payment_allocation_id
                }]
            else:  # LOSS
                lines_journal3 = [{
                    "subcategory_id": fx_gain_loss_account_id,
                    "amount": abs(fx_difference),
                    "dr_cr": "D",  # Loss is debit
                    "description": f"Foreign exchange loss on credit application",
                    "source_type": "credit_application",
                    "source_id": payment_allocation_id
                }, {
                    "subcategory_id": suspense_account_id,
                    "amount": abs(fx_difference),
                    "dr_cr": "C",  # Decrease suspense
                    "description": f"FX loss on credit application",
                    "source_type": "credit_application",
                    "source_id": payment_allocation_id
                }]

            create_transaction(
                db_session=db_session,
                date=datetime.now().date(),
                currency=base_currency_id,
                created_by=current_user.id,
                app_id=app_id,
                journal_number=journal3_number,
                journal_ref_no=f"{credit_ref}-FX",
                narration=f"FX adjustment - credit to Invoice {invoice_ref}",
                payment_mode_id=None,
                project_id=project_id,
                vendor_id=invoice.customer_id,
                exchange_rate_id=None,
                status=status,
                lines=lines_journal3
            )

        return True, f"Credit successfully applied to Invoice {invoice_ref}"

    except Exception as e:
        logger.error(f"Error applying credit to invoice: {str(e)}\n{traceback.format_exc()}")
        return False, str(e)


def post_single_invoice_payment(db_session, sales_transaction, current_user,
                                payment_account_id,
                                fx_gain_loss_account_id=None,
                                status=None, project_id=None,
                                exchange_rate_id=None, exchange_rate_value=None):
    """
    ===========================================================================
    POST SINGLE INVOICE PAYMENT TO LEDGER
    ===========================================================================

    Handles two scenarios:
    1. SAME CURRENCY: Direct posting with FX gain/loss calculation
    2. DIFFERENT CURRENCY: Uses suspense account for bridging + FX gain/loss

    FX GAIN/LOSS IS ALWAYS CALCULATED when:
    - Invoice is in foreign currency relative to base currency
    - Exchange rate changed between invoice date and payment date

    JOURNAL STRUCTURE:
    -----------------
    Scenario 1 (Same Currency):
        Dr Cash/Bank                    (Payment amount in invoice currency)
           Cr Accounts Receivable           (Net amount)
           Cr Tax Payable                   (Tax amount - if applicable)
        THEN in BASE currency (if FX difference):
        Dr/Cr Accounts Receivable        (FX adjustment)
           Cr/Dr FX Gain/Loss               (FX adjustment)

    Scenario 2 (Different Currency):
        [Same as before with suspense bridge]
    ===========================================================================
    """
    try:
        app_id = current_user.app_id

        # Get invoice from transaction
        invoice = sales_transaction.invoice
        if not invoice:
            return False, "Invoice not found"

        payment_amount = float(sales_transaction.amount_paid)
        payment_currency = sales_transaction.currency_id
        payment_ref = sales_transaction.reference_number or f"PMT-{sales_transaction.id}"

        # Get payment allocation for tax split
        payment_allocation = sales_transaction.payment_allocations[0] if sales_transaction.payment_allocations else None
        if not payment_allocation:
            return False, "No payment allocation found"

        allocated_base = float(payment_allocation.allocated_base_amount) if payment_allocation else payment_amount
        allocated_tax = float(payment_allocation.allocated_tax_amount) if payment_allocation else 0

        # Get base currency for FX calculations
        base_currency_info = get_base_currency(db_session, app_id)
        base_currency_id = base_currency_info["base_currency_id"]

        # Get payment rate (if foreign currency payment)
        payment_rate = exchange_rate_value if exchange_rate_value else 1

        # ===== JOURNAL 1: Record payment (in payment currency) =====
        journal1_number = generate_unique_journal_number(db_session, app_id)

        lines_journal1 = [{
            "subcategory_id": payment_account_id,
            "amount": payment_amount,
            "dr_cr": "D",
            "description": f'Payment received for Invoice {invoice.invoice_number}',
            "source_type": "invoice_payment",
            "source_id": sales_transaction.id
        }]

        # Credit AR and Tax (in invoice currency)
        if allocated_tax > 0 and invoice.tax_account_id:
            lines_journal1.append({
                "subcategory_id": invoice.account_receivable_id,
                "amount": allocated_base,
                "dr_cr": "C",
                "description": f"Payment applied to Invoice {invoice.invoice_number} (net)",
                "source_type": "invoice_payment",
                "source_id": sales_transaction.id
            })
            lines_journal1.append({
                "subcategory_id": invoice.tax_account_id,
                "amount": allocated_tax,
                "dr_cr": "C",
                "description": f"Tax portion of payment",
                "source_type": "invoice_payment",
                "source_id": sales_transaction.id
            })
        else:
            lines_journal1.append({
                "subcategory_id": invoice.account_receivable_id,
                "amount": payment_amount,
                "dr_cr": "C",
                "description": f"Payment applied to Invoice {invoice.invoice_number}",
                "source_type": "invoice_payment",
                "source_id": sales_transaction.id
            })

        create_transaction(
            db_session=db_session,
            date=sales_transaction.payment_date,
            currency=payment_currency,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal1_number,
            journal_ref_no=payment_ref,
            narration=f"Payment for Invoice {invoice.invoice_number}",
            payment_mode_id=sales_transaction.payment_mode,
            project_id=project_id or invoice.project_id,
            vendor_id=invoice.customer_id,
            exchange_rate_id=exchange_rate_id if payment_currency != base_currency_id else None,
            status=status,
            lines=lines_journal1
        )

        # ===== JOURNAL 2: FX gain/loss (ALWAYS calculate if invoice in foreign currency) =====
        if invoice.currency != base_currency_id and fx_gain_loss_account_id:
            # Calculate values in base currency
            invoice_base_at_invoice_rate = float(invoice.total_amount) * (
                float(invoice.exchange_rate.rate) if invoice.exchange_rate else 1)
            payment_base_at_payment_rate = payment_amount * payment_rate

            # Calculate FX on the portion paid
            paid_ratio = payment_amount / float(invoice.total_amount)
            fx_diff = (payment_base_at_payment_rate - invoice_base_at_invoice_rate) * paid_ratio

            if abs(fx_diff) > 0.01:
                journal2_number = generate_unique_journal_number(db_session, app_id)

                if fx_diff > 0:  # GAIN
                    lines_journal2 = [{
                        "subcategory_id": invoice.account_receivable_id,
                        "amount": abs(fx_diff),
                        "dr_cr": "D",  # Decrease AR (gain)
                        "description": f"FX gain on payment",
                        "source_type": "invoice_payment",
                        "source_id": sales_transaction.id
                    }, {
                        "subcategory_id": fx_gain_loss_account_id,
                        "amount": abs(fx_diff),
                        "dr_cr": "C",  # Gain is credit
                        "description": f"Foreign exchange gain",
                        "source_type": "invoice_payment",
                        "source_id": sales_transaction.id
                    }]
                else:  # LOSS
                    lines_journal2 = [{
                        "subcategory_id": fx_gain_loss_account_id,
                        "amount": abs(fx_diff),
                        "dr_cr": "D",  # Loss is debit
                        "description": f"Foreign exchange loss",
                        "source_type": "invoice_payment",
                        "source_id": sales_transaction.id
                    }, {
                        "subcategory_id": invoice.account_receivable_id,
                        "amount": abs(fx_diff),
                        "dr_cr": "C",  # Increase AR (loss)
                        "description": f"FX loss on payment",
                        "source_type": "invoice_payment",
                        "source_id": sales_transaction.id
                    }]

                create_transaction(
                    db_session=db_session,
                    date=sales_transaction.payment_date,
                    currency=base_currency_id,
                    created_by=current_user.id,
                    app_id=app_id,
                    journal_number=journal2_number,
                    journal_ref_no=f"{payment_ref}-FX",
                    narration=f"FX adjustment - Invoice {invoice.invoice_number}",
                    payment_mode_id=None,
                    project_id=project_id or invoice.project_id,
                    vendor_id=invoice.customer_id,
                    exchange_rate_id=None,
                    status=status,
                    lines=lines_journal2
                )

        sales_transaction.is_posted_to_ledger = True
        return True, f"Payment posted for Invoice {invoice.invoice_number}"

    except Exception as e:
        logger.error(f"Error posting single invoice payment: {str(e)}\n{traceback.format_exc()}")
        return False, str(e)


def bulk_post_sales_transactions(db_session, direct_sale_ids, invoice_transaction_ids, current_user,
                                 is_pos_order=False):
    """
    Bulk update sales transaction journals from 'Unposted' to 'Posted'
    and mark transactions as fully posted to ledger
    """
    try:
        app_id = current_user.app_id
        success_count = 0
        failed_transactions = []

        # Process direct sales
        for sale_id in direct_sale_ids:
            try:
                # Find the direct sale transaction
                direct_sale = db_session.query(DirectSalesTransaction).filter(
                    DirectSalesTransaction.id == sale_id,
                    DirectSalesTransaction.app_id == app_id
                ).first()

                if not direct_sale:
                    failed_transactions.append(f"Direct sale {sale_id} not found")
                    continue

                # Skip if already fully posted
                if direct_sale.is_posted_to_ledger:
                    success_count += 1
                    continue

                if is_pos_order:
                    logger.info(f'Is pos sale')
                    # Find journal entries linked to these allocations
                    journal_entries = db_session.query(JournalEntry).filter(
                        JournalEntry.source_type == "pos_sale",
                        JournalEntry.source_id == direct_sale.id,
                        JournalEntry.app_id == app_id
                    ).all()
                    for entry in journal_entries:
                        if entry.journal and entry.journal.status == 'Unposted':
                            entry.journal.status = "Posted"
                            direct_sale.is_posted_to_ledger = True
                    continue

                # Find all payment allocations for this direct sale
                payment_allocations = direct_sale.payment_allocations
                allocation_ids = [pa.id for pa in payment_allocations]

                if allocation_ids:
                    # Find journal entries linked to these allocations
                    journal_entries = db_session.query(JournalEntry).filter(
                        JournalEntry.source_type == "direct_sale_payment",
                        JournalEntry.source_id.in_(allocation_ids),
                        JournalEntry.app_id == app_id
                    ).all()

                    # Update all related journals to Posted status
                    journals_updated = set()
                    for entry in journal_entries:
                        if entry.journal and entry.journal.status == 'Unposted':
                            entry.journal.status = "Posted"
                            journals_updated.add(entry.journal.id)

                    # Also update COGS journals if any
                    cogs_journal_entries = db_session.query(JournalEntry).filter(
                        JournalEntry.source_type == "direct_sale_cogs",
                        JournalEntry.source_id.in_([item.id for item in direct_sale.direct_sale_items]),
                        JournalEntry.app_id == app_id
                    ).all()

                    for entry in cogs_journal_entries:
                        if entry.journal and entry.journal.status == 'Unposted':
                            entry.journal.status = "Posted"
                            journals_updated.add(entry.journal.id)

                    # ✅ UPDATE PAYMENT ALLOCATIONS TO POSTED
                    for allocation in payment_allocations:
                        allocation.is_posted_to_ledger = True

                # Mark direct sale as fully posted
                direct_sale.is_posted_to_ledger = True
                success_count += 1

            except Exception as e:
                failed_transactions.append(f"Direct sale {sale_id}: {str(e)}")
                continue

        # Process invoice transactions
        for invoice_id in invoice_transaction_ids:
            try:
                # Find the invoice transaction
                invoice_transaction = db_session.query(SalesTransaction).filter(
                    SalesTransaction.id == invoice_id,
                    SalesTransaction.app_id == app_id
                ).first()

                if not invoice_transaction:
                    failed_transactions.append(f"Invoice transaction {invoice_id} not found")
                    continue

                # Skip if already fully posted
                if invoice_transaction.is_posted_to_ledger:
                    success_count += 1
                    continue

                # Find all payment allocations for this invoice
                payment_allocations = invoice_transaction.payment_allocations
                allocation_ids = [pa.id for pa in payment_allocations]
                source_types = ['invoice_payment', 'overpayment_write_off', 'credit_application', 'customer_credit']

                if allocation_ids:
                    # Find journal entries linked to these allocations
                    journal_entries = db_session.query(JournalEntry).filter(
                        JournalEntry.source_type.in_(source_types),
                        JournalEntry.source_id.in_(allocation_ids),
                        JournalEntry.app_id == app_id
                    ).all()

                    # Update all related journals to Posted status
                    journals_updated = set()
                    for entry in journal_entries:
                        if entry.journal and entry.journal.status == 'Unposted':
                            entry.journal.status = "Posted"
                            journals_updated.add(entry.journal.id)

                    # ✅ UPDATE PAYMENT ALLOCATIONS TO POSTED
                    for allocation in payment_allocations:
                        allocation.is_posted_to_ledger = True

                # Mark invoice transaction as fully posted
                invoice_transaction.is_posted_to_ledger = True
                success_count += 1

            except Exception as e:
                failed_transactions.append(f"Invoice transaction {invoice_id}: {str(e)}")
                continue

        # Commit all changes
        db_session.commit()

        if failed_transactions:
            return False, f"Posted {success_count} transactions, failed {len(failed_transactions)}: {', '.join(failed_transactions[:3])}"

        return True, f"Successfully posted {success_count} sales transactions to ledger"

    except Exception as e:
        db_session.rollback()
        logger.error(f"Bulk sales posting error: {str(e)}")
        return False, str(e)


def post_direct_sale_cogs_to_ledger(db_session, direct_sale, exchange_rate_id, exchange_rate_value, current_user,
                                    base_currency_id):
    """
    Post COGS entries for direct_sale's inventory items with proper currency conversion
    """

    from services.inventory_helpers import process_inventory_entries
    from services.inventory_helpers import get_current_average_cost

    try:
        app_id = current_user.app_id

        # Get inventory items from the invoice
        inventory_items = [item for item in direct_sale.direct_sale_items if item.item_type == "inventory"]

        if not inventory_items:
            return True  # No inventory items to process

        # Pre-fetch all inventory item variation links in a single query
        variation_link_ids = [item.item_id for item in inventory_items if item.item_id]
        variation_links_map = {}

        if variation_link_ids:
            # Query InventoryItemVariationLink with joins to get the actual InventoryItem
            variation_links = db_session.query(InventoryItemVariationLink).filter(
                InventoryItemVariationLink.id.in_(variation_link_ids),
                InventoryItemVariationLink.app_id == app_id
            ).options(
                joinedload(InventoryItemVariationLink.inventory_item).joinedload(InventoryItem.cogs_account),
                joinedload(InventoryItemVariationLink.inventory_item).joinedload(InventoryItem.asset_account)
            ).all()

            variation_links_map = {link.id: link for link in variation_links}

        # Group items by location to process them separately
        items_by_location = {}
        for item in inventory_items:
            if not item.item_id or not item.location_id:
                continue

            if item.location_id not in items_by_location:
                items_by_location[item.location_id] = []
            items_by_location[item.location_id].append(item)

        # Process each location separately
        for location_id, location_items in items_by_location.items():
            # Prepare data for process_inventory_entries for this location
            inventory_items_list = []
            quantities = []
            unit_prices_base = []
            selling_prices_base = []

            # Process each inventory item in this location
            for item in location_items:
                variation_link = variation_links_map.get(item.item_id)
                if not variation_link or not variation_link.inventory_item:
                    continue

                inventory_item = variation_link.inventory_item

                # Skip if missing required accounts
                if not inventory_item.cogs_account or not inventory_item.asset_account:
                    continue

                # Get average cost for this inventory item at the specific location
                avg_cost = get_current_average_cost(
                    db_session, app_id, variation_link.id, item.location_id
                )

                if avg_cost <= 0:
                    logger.warning(
                        f"Direct Sale {direct_sale.direct_sale_number} - Item {inventory_item.item_name} "
                        f"at location {item.location_id} has avg_cost=0. Posting COGS=0."
                    )
                    avg_cost = Decimal("0.00")

                # Convert unit price to base currency if needed

                unit_price_in_sale_currency = Decimal(str(item.unit_price or 0))
                # After (handles None):
                if exchange_rate_value:
                    unit_price_in_base_currency = unit_price_in_sale_currency * Decimal(str(exchange_rate_value))
                else:
                    unit_price_in_base_currency = unit_price_in_sale_currency

                # Check if quantity is a string or needs conversion
                if isinstance(item.quantity, str):
                    quantity = Decimal(item.quantity)
                else:
                    quantity = Decimal(str(item.quantity))

                total_cogs = avg_cost * quantity

                # Prepare data for process_inventory_entries
                inventory_items_list.append(variation_link.id)
                quantities.append(item.quantity)
                unit_prices_base.append(float(avg_cost))
                selling_prices_base.append(float(unit_price_in_base_currency))  # Use converted price

                # Get vendor ID
                vendor_id = direct_sale.customer_id if direct_sale.customer_id else None

                # Create COGS journal with both entries
                cogs_journal, cogs_entries = create_transaction(
                    db_session=db_session,
                    date=direct_sale.payment_date,
                    currency=base_currency_id,
                    created_by=current_user.id,
                    app_id=app_id,
                    narration=f"Direct Sale {direct_sale.direct_sale_number} - {inventory_item.item_name}",
                    project_id=direct_sale.project_id,
                    vendor_id=vendor_id,
                    lines=[
                        # DEBIT COGS
                        {
                            "subcategory_id": inventory_item.cogs_account.id,
                            "amount": float(total_cogs),
                            "dr_cr": "D",
                            "description": f"Direct Sale {direct_sale.direct_sale_number} - {inventory_item.item_name} COGS",
                            "source_type": "direct_sale_cogs",
                            "source_id": item.id
                        },
                        # CREDIT Inventory Asset
                        {
                            "subcategory_id": inventory_item.asset_account.id,
                            "amount": float(total_cogs),
                            "dr_cr": "C",
                            "description": f"Direct Sale {direct_sale.direct_sale_number} - {inventory_item.item_name} Inventory Reduction",
                            "source_type": "direct_sale_cogs",
                            "source_id": item.id
                        }
                    ]
                )

            # Call process_inventory_entries for this location
            if inventory_items_list:
                try:
                    process_inventory_entries(
                        db_session=db_session,
                        app_id=app_id,
                        inventory_items=inventory_items_list,
                        quantities=quantities,
                        unit_prices=unit_prices_base,
                        selling_prices=selling_prices_base,
                        location=location_id,
                        from_location=location_id,
                        to_location=None,
                        transaction_date=direct_sale.payment_date,
                        supplier_id=direct_sale.customer_id,
                        form_currency_id=base_currency_id,
                        base_currency_id=base_currency_id,
                        expiration_date=None,
                        reference=direct_sale.direct_sale_number,
                        write_off_reason=None,
                        project_id=direct_sale.project_id,
                        movement_type='stock_out_sale',
                        current_user_id=current_user.id,
                        source_type='direct_sale',
                        sales_account_id=direct_sale.revenue_account_id,
                        exchange_rate_id=exchange_rate_id,
                        is_posted_to_ledger=True,
                        source_id=direct_sale.id
                    )
                except Exception as e:
                    logger.error(
                        f"Error in process_inventory_entries for direct sale {direct_sale.direct_sale_number}, location {location_id}: {str(e)}")
                    raise Exception(f'An error occurred')

        return True

    except Exception as e:
        logger.error(f"Error posting COGS for direct sale {direct_sale.direct_sale_number}: {str(e)}", exc_info=True)
        return Exception(f'An error occurred when proccessing COGS')


def create_expense_journal_entry(db_session, expense_transaction, current_user, status='Unposted', is_update=False,
                                 original_journal_number=None, exchange_rate_id=None):
    """
    Create a journal entry for an expense transaction with specified status.

    Args:
        db_session: Database session
        expense_transaction: ExpenseTransaction object
        current_user: Current user object
        status: Journal entry status ('Unposted' or 'Posted')
        is_update: Boolean indicating if this is an update operation
        original_journal_number: Original journal number to reuse for updates
        exchange_rate_id: Exhange rate id

    Returns:
        tuple: (success: bool, message: str, journal_entry: JournalEntry or None)
    """
    try:
        # Validate transaction exists
        if not expense_transaction:
            return False, "Transaction not found.", None

        # === Extract fields ===
        payment_date = expense_transaction.payment_date or datetime.now().date()
        currency_id = expense_transaction.currency_id
        payment_account_id = expense_transaction.payment_account_id
        project_id = expense_transaction.project_id
        vendor_id = expense_transaction.vendor_id
        amount = expense_transaction.total_amount
        payment_mode_id = expense_transaction.payment_mode_id
        app_id = current_user.app_id
        expense_ref_no = expense_transaction.expense_ref_no or expense_transaction.expense_entry_number

        # === Create journal lines ===
        lines = []

        # 1. Debit entries for each expense item
        for item in expense_transaction.expense_items:
            subcategory_id = item.subcategory_id

            lines.append({
                "subcategory_id": subcategory_id,
                "amount": item.amount,
                "dr_cr": "D",  # Debit
                "description": item.description or f"Expense item for {expense_ref_no}",
                "source_type": "expense_transaction_item",
                "source_id": item.id
            })

        # 2. Credit entry for payment account
        lines.append({
            "subcategory_id": payment_account_id,
            "amount": amount,
            "dr_cr": "C",  # Credit
            "description": f"Payment for {expense_ref_no} ({len(expense_transaction.expense_items)} items)",
            "source_type": "expense_transaction",
            "source_id": expense_transaction.id
        })

        # === Determine journal number ===
        journal_number = None
        if is_update and original_journal_number:
            # Reuse the original journal number for updates
            journal_number = original_journal_number
        else:
            # Generate new journal number for new entries
            journal_number = generate_unique_journal_number(db_session, app_id)  # You'll need this function

        # === Create journal with specified status ===
        journal, entries = create_transaction(
            db_session=db_session,
            date=payment_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            payment_mode_id=payment_mode_id,
            project_id=project_id,
            vendor_id=vendor_id,
            exchange_rate_id=exchange_rate_id,
            status=status,
            lines=lines,
            journal_ref_no=expense_ref_no,
            narration=expense_transaction.narration,
            journal_number=journal_number  # Pass the journal number
        )

        success_message = f"Journal entry created with status: {status}."
        if is_update:
            success_message = f"Journal entry updated with status: {status}."

        return True, success_message, journal

    except Exception as e:
        logging.error(f"Error creating expense journal entry: {str(e)}")
        return False, f"Error creating journal entry: {str(e)}", None


def post_expense_journal_to_ledger(db_session, expense_transaction, current_user):
    """
    Update the journal entry status from 'Unposted' to 'Posted' for an expense transaction.
    """
    try:
        # Find the journal associated with this expense
        journal = db_session.query(Journal).join(JournalEntry).filter(
            JournalEntry.source_type == 'expense_transaction',
            JournalEntry.source_id == expense_transaction.id
        ).first()

        if not journal:
            return False, "No journal entry found for this expense.", None

        if journal.status == 'Posted':
            return False, "Journal entry is already posted to ledger.", None

        # Update journal status to Posted
        journal.status = 'Posted'
        journal.updated_at = datetime.now()
        journal.updated_by = current_user.id

        # Update expense transaction status
        expense_transaction.is_posted_to_ledger = True
        expense_transaction.updated_at = datetime.now()

        db_session.commit()

        return True, f"Expense {expense_transaction.expense_entry_number} posted to ledger successfully.", journal

    except Exception as e:
        db_session.rollback()
        logging.error(f"Error posting expense journal to ledger: {str(e)}\n{traceback.format_exc()}")
        return False, f"Error posting journal to ledger: {str(e)}", None


def create_fund_transfer_journal(db_session, transaction_data, created_by_user_id, app_id, base_currency_id=None):
    """
    Create fund transfer journals with Posted status that handles all currency scenarios
    For inter-currency transfers, creates separate journals for each currency side
    """
    try:
        # Validate required fields
        required_fields = ['from_account_id', 'to_account_id', 'from_currency_id',
                           'to_currency_id', 'from_amount', 'to_amount', 'exchange_date']
        for field in required_fields:
            if field not in transaction_data:
                raise ValueError(f"Missing required field: {field}")

        if not base_currency_id:
            # Get base currency info
            base_currency_info = get_base_currency(db_session, app_id)
            base_currency_id = int(base_currency_info["base_currency_id"]) if base_currency_info else None

        # Get account and currency details
        from_account = db_session.query(ChartOfAccounts).filter_by(
            id=transaction_data['from_account_id'], app_id=app_id
        ).first()
        to_account = db_session.query(ChartOfAccounts).filter_by(
            id=transaction_data['to_account_id'], app_id=app_id
        ).first()
        from_currency = db_session.query(Currency).filter_by(
            id=transaction_data['from_currency_id']
        ).first()
        to_currency = db_session.query(Currency).filter_by(
            id=transaction_data['to_currency_id']
        ).first()

        if not all([from_account, to_account, from_currency, to_currency]):
            raise ValueError("Invalid account or currency data")

        # Create exchange rate record if currencies are different
        exchange_rate_obj = None
        if transaction_data['from_currency_id'] != transaction_data['to_currency_id']:
            # Ensure to_currency is always base currency
            from_cur = transaction_data['from_currency_id']
            to_cur = transaction_data['to_currency_id']
            rate_value = Decimal(str(transaction_data.get('exchange_rate', 1.0)))

            # If to_currency is not base currency, swap and invert rate
            if to_cur != base_currency_id:
                # Swap currencies so to_currency becomes base currency
                from_cur, to_cur = to_cur, from_cur

            exchange_rate_obj = ExchangeRate(
                app_id=app_id,
                from_currency_id=from_cur,  # This will be the foreign currency
                to_currency_id=to_cur,  # This will always be base currency
                rate=rate_value,
                date=transaction_data['exchange_date'],
                currency_exchange_transaction_id=transaction_data.get('source_id')
            )
            db_session.add(exchange_rate_obj)
            db_session.flush()

        # Create description
        if transaction_data.get('description'):
            description = transaction_data['description']
        elif transaction_data['from_currency_id'] == transaction_data['to_currency_id']:
            description = (
                f"Fund Transfer: {transaction_data['from_amount']:,.2f} {from_currency.user_currency} "
                f"from {from_account.sub_category} to {to_account.sub_category}"
            )
        else:
            description = (
                f"Currency Conversion: {transaction_data['from_amount']:,.2f} {from_currency.user_currency} "
                f"to {transaction_data['to_amount']:,.2f} {to_currency.user_currency} "
                f"@ {transaction_data.get('exchange_rate', 1.0):,.4f}"
            )

        journals_created = []

        if transaction_data['from_currency_id'] == transaction_data['to_currency_id']:
            # Same currency transfer - ONE journal with 2 entries
            lines = [
                {
                    "subcategory_id": transaction_data['to_account_id'],
                    "amount": float(transaction_data['to_amount']),
                    "dr_cr": 'D',
                    "description": description,
                    "source_type": 'fund_transfer',
                    "source_id": transaction_data.get('source_id')
                },
                {
                    "subcategory_id": transaction_data['from_account_id'],
                    "amount": float(transaction_data['from_amount']),
                    "dr_cr": 'C',
                    "description": description,
                    "source_type": 'fund_transfer',
                    "source_id": transaction_data.get('source_id')
                }
            ]

            journal, entries = create_transaction(
                db_session=db_session,
                date=transaction_data['exchange_date'],
                currency=transaction_data['from_currency_id'],  # Use the actual currency
                created_by=created_by_user_id,
                app_id=app_id,
                journal_ref_no=f"FT{transaction_data.get('source_id', '')}",
                narration=description,
                payment_mode_id=transaction_data.get('payment_mode_id'),
                project_id=transaction_data.get('project_id'),
                vendor_id=transaction_data.get('vendor_id'),
                exchange_rate_id=None,  # No exchange rate for same currency
                status='Posted',
                lines=lines
            )
            journals_created.append(journal)

        else:
            # Different currencies - TWO SEPARATE JOURNALS with FX clearing

            # Get FX clearing account
            # Get system accounts
            system_accounts = get_all_system_accounts(
                db_session=db_session,
                app_id=app_id,
                created_by_user_id=created_by_user_id
            )
            suspense_account_id = system_accounts['suspense']
            fx_clearing_account_id = suspense_account_id

            # Use the original logic for exchange rate assignment
            # Only assign exchange_rate_id to entries that are NOT in base currency
            from_journal_exchange_rate_id = exchange_rate_obj.id if transaction_data[
                                                                        'from_currency_id'] != base_currency_id else None
            to_journal_exchange_rate_id = exchange_rate_obj.id if transaction_data[
                                                                      'to_currency_id'] != base_currency_id else None

            # JOURNAL 1: From Currency Side
            from_currency_lines = [
                # Debit FX Clearing (from currency)
                {
                    "subcategory_id": fx_clearing_account_id,
                    "amount": float(transaction_data['from_amount']),
                    "dr_cr": 'D',
                    "description": f"FT: Receive {transaction_data['from_amount']:,.2f} {from_currency.user_currency}",
                    "source_type": 'fund_transfer',
                    "source_id": transaction_data.get('source_id')
                },
                # Credit From Account
                {
                    "subcategory_id": transaction_data['from_account_id'],
                    "amount": float(transaction_data['from_amount']),
                    "dr_cr": 'C',
                    "description": f"Currency conversion to {to_currency.user_currency}",
                    "source_type": 'fund_transfer',
                    "source_id": transaction_data.get('source_id')
                }
            ]

            journal_from, entries_from = create_transaction(
                db_session=db_session,
                date=transaction_data['exchange_date'],
                currency=transaction_data['from_currency_id'],  # Use FROM currency
                created_by=created_by_user_id,
                app_id=app_id,
                journal_ref_no=f"FT-FROM-{from_currency.user_currency}",
                narration=f"Fund Transfer Out: {transaction_data['from_amount']:,.2f} {from_currency.user_currency}",
                payment_mode_id=transaction_data.get('payment_mode_id'),
                project_id=transaction_data.get('project_id'),
                vendor_id=transaction_data.get('vendor_id'),
                exchange_rate_id=from_journal_exchange_rate_id,
                status='Posted',
                lines=from_currency_lines
            )
            journals_created.append(journal_from)

            # JOURNAL 2: To Currency Side
            to_currency_lines = [
                # Debit To Account
                {
                    "subcategory_id": transaction_data['to_account_id'],
                    "amount": float(transaction_data['to_amount']),
                    "dr_cr": 'D',
                    "description": f"Currency conversion from {from_currency.user_currency}",
                    "source_type": 'fund_transfer',
                    "source_id": transaction_data.get('source_id')
                },
                # Credit FX Clearing (to currency)
                {
                    "subcategory_id": fx_clearing_account_id,
                    "amount": float(transaction_data['to_amount']),
                    "dr_cr": 'C',
                    "description": f"FT: Pay {transaction_data['to_amount']:,.2f} {to_currency.user_currency}",
                    "source_type": 'fund_transfer',
                    "source_id": transaction_data.get('source_id')
                }
            ]

            journal_to, entries_to = create_transaction(
                db_session=db_session,
                date=transaction_data['exchange_date'],
                currency=transaction_data['to_currency_id'],  # Use TO currency
                created_by=created_by_user_id,
                app_id=app_id,
                journal_ref_no=f"FT-TO-{to_currency.user_currency}",
                narration=f"Fund Transfer In: {transaction_data['to_amount']:,.2f} {to_currency.user_currency}",
                payment_mode_id=transaction_data.get('payment_mode_id'),
                project_id=transaction_data.get('project_id'),
                vendor_id=transaction_data.get('vendor_id'),
                exchange_rate_id=to_journal_exchange_rate_id,
                status='Posted',
                lines=to_currency_lines
            )
            journals_created.append(journal_to)

        # Prepare response
        response = {
            'status': 'success',
            'journals_created': len(journals_created),
            'message': 'Fund transfer journal(s) created'
        }

        # Add journal details to response
        for i, journal in enumerate(journals_created):
            response[f'journal_{i + 1}_id'] = journal.id
            response[f'journal_{i + 1}_number'] = journal.journal_number
            response[f'journal_{i + 1}_currency'] = journal.currency_id
            response[f'journal_{i + 1}_balanced'] = journal.is_balanced()

        return response

    except Exception as e:
        logger.error(f"Error creating fund transfer journal: {str(e)}")
        raise


def find_journals_by_source(db_session, source_type, source_id, app_id, additional_filters=None):
    """
    Universal function to find journals by source with flexible filtering

    Args:
        db_session: Database session
        source_type: Type of source (payroll_transaction, expense_transaction, inventory, advance_payment, etc.)
        source_id: ID of the source record
        app_id: Application ID for tenant isolation
        additional_filters: Optional additional filters for journal entries

    Returns:
        List of Journal objects
    """
    try:
        query = db_session.query(Journal).join(JournalEntry).filter(
            Journal.app_id == app_id,
            JournalEntry.source_type == source_type,
            JournalEntry.source_id == source_id
        )

        # Apply additional filters if provided
        if additional_filters:
            for key, value in additional_filters.items():
                if hasattr(JournalEntry, key):
                    query = query.filter(getattr(JournalEntry, key) == value)
                elif hasattr(Journal, key):
                    query = query.filter(getattr(Journal, key) == value)

        return query.options(joinedload(Journal.entries)).all()

    except Exception as e:
        logger.error(f"Error finding journals for {source_type} {source_id}: {str(e)}")
        return []


def find_journals_by_multiple_sources(db_session, source_pairs, app_id, additional_filters=None):
    """
    Find journals for multiple source types and IDs

    Args:
        db_session: Database session
        source_pairs: List of tuples [(source_type, source_id), ...]
        app_id: Application ID
        additional_filters: Optional additional filters

    Returns:
        Dictionary with source pairs as keys and journal lists as values
    """
    results = {}
    for source_type, source_id in source_pairs:
        journals = find_journals_by_source(db_session, source_type, source_id, app_id, additional_filters)
        results[(source_type, source_id)] = journals
    return results


def post_purchase_transaction_to_ledger(db_session, purchase_transaction, current_user, status="Posted",
                                        base_currency_info=None, base_currency_id=None):
    """
    Post purchase transaction to ledger using individual inventory item costs.
    ALL entries go into ONE journal header.
    """
    try:
        app_id = current_user.app_id

        if not base_currency_id:

            # Get base currency if not provided
            if base_currency_info is None:
                base_currency_info = get_base_currency(db_session, app_id)

            if not base_currency_info:
                raise Exception("Base currency not configured")

            base_currency_id = base_currency_info["base_currency_id"]
        currency_id = purchase_transaction.currency_id

        # Exchange rate if foreign currency
        exchange_rate_id = None
        exchange_rate_value = 1
        if int(base_currency_id) != int(currency_id):
            exchange_rate_obj, exchange_rate_value = get_exchange_rate_and_obj(
                db_session, currency_id, base_currency_id, app_id, purchase_transaction.payment_date
            )
            if not exchange_rate_obj:
                raise Exception(
                    f"Exchange rate not found for currency {currency_id} to base currency {base_currency_id}")
            exchange_rate_id = exchange_rate_obj.id

        # Get unposted payment allocations
        unposted_allocations = [pa for pa in purchase_transaction.payment_allocations]

        if not unposted_allocations:
            raise Exception(
                f"No unposted payment allocations found for purchase {purchase_transaction.direct_purchase_number}")

        # ✅ Generate ONE journal number for the entire purchase
        journal_number = generate_unique_journal_number(db_session, app_id)

        # ✅ Collect ALL lines in one list
        all_lines = []

        # Process payment allocations
        for allocation in unposted_allocations:
            if not allocation.payment_account_id:
                raise Exception(
                    f"Payment account not configured for allocation in purchase {purchase_transaction.direct_purchase_number}")

            # Calculate total allocated amount INCLUDING inventory
            total_allocated = (
                    (allocation.allocated_inventory or 0) +
                    (allocation.allocated_non_inventory or 0) +
                    (allocation.allocated_services or 0) +
                    (allocation.allocated_other_expenses or 0) +
                    (allocation.allocated_tax_payable or 0) +
                    (allocation.allocated_tax_receivable or 0)
            )

            # Validate we have something to post
            if total_allocated <= 0:
                continue

            # ✅ Credit Cash/Bank (payment account) - SINGLE CREDIT FOR EVERYTHING
            all_lines.append({
                "subcategory_id": allocation.payment_account_id,
                "amount": float(total_allocated),
                "dr_cr": "C",
                "description": f"Payment for purchase {purchase_transaction.direct_purchase_number}",
                "source_type": "direct_purchase_payment",
                "source_id": allocation.id
            })

            # ✅ Debit INDIVIDUAL Inventory Accounts for each inventory item
            if allocation.allocated_inventory and allocation.allocated_inventory > 0:
                inventory_allocations = get_inventory_allocations_by_item(db_session, purchase_transaction, allocation,
                                                                          app_id)

                total_inventory_debits = 0
                for inv_allocation in inventory_allocations:
                    if inv_allocation['amount'] > 0:
                        total_inventory_debits += inv_allocation['amount']
                        all_lines.append({
                            "subcategory_id": inv_allocation['asset_account_id'],
                            "amount": inv_allocation['amount'],
                            "dr_cr": "D",
                            "description": f"{inv_allocation['item_name']} - {purchase_transaction.direct_purchase_number}",
                            "source_type": "direct_purchase_payment",
                            "source_id": allocation.id
                        })

                # Verify the total matches (should be exact with landed costs)
                if abs(total_inventory_debits - float(allocation.allocated_inventory or 0)) > 0.01:
                    logger.warning(
                        f"Inventory debit total {total_inventory_debits} doesn't match allocation {allocation.allocated_inventory}")

            # Debit Non-Inventory
            if allocation.allocated_non_inventory and allocation.allocated_non_inventory > 0:

                if not allocation.non_inventory_account_id:
                    raise Exception("Non-inventory account not configured")

                all_lines.append({
                    "subcategory_id": allocation.non_inventory_account_id,
                    "amount": float(allocation.allocated_non_inventory),
                    "dr_cr": "D",
                    "description": f"Non-inventory purchase {purchase_transaction.direct_purchase_number}",
                    "source_type": "direct_purchase_payment",
                    "source_id": allocation.id
                })

            # Debit Services
            if allocation.allocated_services and allocation.allocated_services > 0:
                if not allocation.other_expense_service_id:
                    raise Exception("Service expense account not configured")

                all_lines.append({
                    "subcategory_id": allocation.other_expense_service_id,
                    "amount": float(allocation.allocated_services),
                    "dr_cr": "D",
                    "description": f"Service expense {purchase_transaction.direct_purchase_number}",
                    "source_type": "direct_purchase_payment",
                    "source_id": allocation.id
                })

            # Debit Other Expenses
            if allocation.allocated_other_expenses and allocation.allocated_other_expenses > 0:
                if not allocation.other_expense_account_id:
                    raise Exception("Other expense account not configured")

                all_lines.append({
                    "subcategory_id": allocation.other_expense_account_id,
                    "amount": float(allocation.allocated_other_expenses),
                    "dr_cr": "D",
                    "description": f"Shipping/Handling {purchase_transaction.direct_purchase_number}",
                    "source_type": "direct_purchase_payment",
                    "source_id": allocation.id
                })

            allocation.is_posted_to_ledger = True

        # ✅ Validate we have balanced journal entries
        if len(all_lines) < 2:
            raise Exception(f"Insufficient journal lines for purchase {purchase_transaction.direct_purchase_number}")

        # ✅ Create ONE journal entry for ALL transactions
        journal_success = create_transaction(
            db_session=db_session,
            date=purchase_transaction.payment_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal_number,
            journal_ref_no=purchase_transaction.direct_purchase_number,
            narration=f"Purchase {purchase_transaction.direct_purchase_number}",
            payment_mode_id=allocation.payment_mode if unposted_allocations else None,
            project_id=purchase_transaction.project_id,
            vendor_id=purchase_transaction.vendor_id,
            exchange_rate_id=exchange_rate_id,
            status=status,
            lines=all_lines
        )

        if not journal_success:
            raise Exception(
                f"Failed to create journal entry for purchase {purchase_transaction.direct_purchase_number}")

        # ✅ Use YOUR ACTUAL function for inventory stock updates - NOW WITH LANDED COSTS
        inventory_items = [item for item in purchase_transaction.direct_purchase_items if item.item_type == "inventory"]
        if inventory_items:
            from services.inventory_helpers import process_inventory_entries

            # Prepare data for process_inventory_entries
            inventory_items_list = []
            quantities = []
            unit_prices_base = []
            selling_prices_base = []
            locations = []

            for item in inventory_items:
                if item.item_id:
                    inventory_items_list.append(item.item_id)
                    quantities.append(item.quantity)

                    # ✅ USE THE CALCULATED UNIT COST (landed cost) instead of base unit_price
                    unit_cost_to_use = item.unit_cost if item.unit_cost is not None else item.unit_price

                    # Convert to base currency if needed
                    unit_price_base = float(Decimal(str(unit_cost_to_use or 0)) * Decimal(str(exchange_rate_value)))
                    unit_prices_base.append(unit_price_base)
                    selling_prices_base.append(unit_price_base)  # Using same as cost price for now
                    locations.append(item.location_id)

            if inventory_items_list:
                process_inventory_entries(
                    db_session=db_session,
                    app_id=app_id,
                    inventory_items=inventory_items_list,
                    quantities=quantities,
                    unit_prices=unit_prices_base,
                    selling_prices=selling_prices_base,
                    location=locations[0] if locations else None,
                    from_location=None,
                    to_location=locations[0] if locations else None,
                    transaction_date=purchase_transaction.payment_date,
                    supplier_id=purchase_transaction.vendor_id,
                    form_currency_id=base_currency_id,
                    base_currency_id=base_currency_id,
                    reference=purchase_transaction.direct_purchase_number,
                    project_id=purchase_transaction.project_id,
                    movement_type='in',
                    current_user_id=current_user.id,
                    source_type='direct_purchase',
                    source_id=purchase_transaction.id,
                    write_off_account_id=None,
                    write_off_reason=None,
                    expiration_date=None,
                    is_posted_to_ledger=True
                )

        # Only mark as posted if everything succeeded
        purchase_transaction.is_posted_to_ledger = True

        return True, f"Purchase successfully posted to ledger"

    except Exception as e:
        logger.error(f"CRITICAL ERROR posting purchase to ledger: {str(e)}")
        raise Exception(f"Ledger posting failed: {str(e)}")


def post_goods_receipt_to_ledger(db_session, goods_receipt, current_user, status=None, base_currency_info=None):
    """
    Post goods receipt to ledger for ALL item types.
    Inventory: Debit Inventory Asset Account
    Non-Inventory: Debit Non-Inventory Expense Account
    Services: Debit Service Expense Account
    Shipping/Handling: Debit Shipping/Handling Expense Account
    Credit: Prepaid Account (for advance payments) AND Accounts Payable (for remaining balance)
    """
    try:
        app_id = current_user.app_id

        # Get base currency if not provided
        if base_currency_info is None:
            base_currency_info = get_base_currency(db_session, app_id)

        if not base_currency_info:
            raise Exception("Base currency not configured")

        base_currency_id = base_currency_info["base_currency_id"]

        # Get purchase order and currency
        purchase_order = goods_receipt.purchase_order
        if not purchase_order:
            raise Exception("Purchase order not found for goods receipt")

        currency_id = purchase_order.currency

        # Exchange rate if foreign currency
        exchange_rate_id = None
        exchange_rate_value = 1
        if int(base_currency_id) != int(currency_id):
            # Use the PO's exchange rate directly - NO EXTRA QUERY
            if purchase_order.exchange_rate:
                exchange_rate_id = purchase_order.exchange_rate.id
                exchange_rate_value = purchase_order.exchange_rate.rate
            else:
                # Fallback to getting rate at receipt date if PO doesn't have exchange rate
                exchange_rate_obj, exchange_rate_value = get_exchange_rate_and_obj(
                    db_session, currency_id, base_currency_id, app_id, goods_receipt.receipt_date
                )
                exchange_rate_id = exchange_rate_obj.id if exchange_rate_obj else None

        # Categorize receipt items by type
        inventory_receipt_items = []
        non_inventory_receipt_items = []
        service_receipt_items = []

        for item in goods_receipt.receipt_items:
            if not item.purchase_order_item:
                continue

            if item.purchase_order_item.item_type == "inventory":
                inventory_receipt_items.append(item)
            elif item.purchase_order_item.item_type in ("non_inventory", "non-inventory"):
                non_inventory_receipt_items.append(item)
            elif item.purchase_order_item.item_type == "service":
                service_receipt_items.append(item)

        # Check if we have any items to post
        if not any([inventory_receipt_items, non_inventory_receipt_items, service_receipt_items]):
            raise Exception("No valid items found in goods receipt")

        # Calculate total values for each category
        total_inventory_value = Decimal('0.00')
        total_non_inventory_value = Decimal('0.00')
        total_service_value = Decimal('0.00')
        total_shipping_handling_value = Decimal('0.00')

        # Calculate inventory value
        for item in inventory_receipt_items:
            if item.total_cost is not None:
                total_inventory_value += Decimal(str(item.total_cost))
            elif item.purchase_order_item and item.purchase_order_item.total_cost is not None:
                total_inventory_value += Decimal(str(item.purchase_order_item.total_cost))
            else:
                total_inventory_value += Decimal(str(item.allocated_amount or 0))

        # Calculate non-inventory value
        for item in non_inventory_receipt_items:
            if item.total_cost is not None:
                total_non_inventory_value += Decimal(str(item.total_cost))
            elif item.purchase_order_item and item.purchase_order_item.total_cost is not None:
                total_non_inventory_value += Decimal(str(item.purchase_order_item.total_cost))
            else:
                total_non_inventory_value += Decimal(str(item.allocated_amount or 0))

        # Calculate service value
        for item in service_receipt_items:
            if item.total_cost is not None:
                total_service_value += Decimal(str(item.total_cost))
            elif item.purchase_order_item and item.purchase_order_item.total_cost is not None:
                total_service_value += Decimal(str(item.purchase_order_item.total_cost))
            else:
                total_service_value += Decimal(str(item.allocated_amount or 0))

        # Calculate shipping and handling value (from allocated_shipping_handling field)
        for item in goods_receipt.receipt_items:
            if hasattr(item, 'allocated_shipping_handling') and item.allocated_shipping_handling:
                total_shipping_handling_value += Decimal(str(item.allocated_shipping_handling))

        # Calculate total debit amount
        total_debit_amount = (total_inventory_value +
                              total_non_inventory_value +
                              total_service_value +
                              total_shipping_handling_value)

        if total_debit_amount <= 0:
            raise Exception("Total goods receipt value must be greater than 0")

        # Check advance payments for this PO
        advance_payments = db_session.query(PurchaseTransaction) \
            .join(PurchasePaymentAllocation) \
            .filter(
            PurchaseTransaction.purchase_order_id == purchase_order.id,
            PurchasePaymentAllocation.payment_type == 'advance_payment',
            PurchaseTransaction.payment_status.in_(['paid', 'partial'])
        ) \
            .all()

        total_advance_paid = sum(Decimal(str(txn.amount_paid)) for txn in advance_payments)

        # Collect all journal lines
        all_lines = []
        total_credit_amount = Decimal('0.00')

        # DEBIT SIDE: Handle different account types

        # 1. Debit Inventory Asset Accounts (individual items)
        for item in inventory_receipt_items:
            if item.purchase_order_item and item.purchase_order_item.item_type == "inventory":
                inventory_item = item.purchase_order_item.inventory_item_variation_link
                if inventory_item and inventory_item.inventory_item and inventory_item.inventory_item.asset_account_id:

                    # Calculate item value
                    if item.total_cost is not None:
                        item_value = Decimal(str(item.total_cost))
                    elif item.purchase_order_item.total_cost is not None:
                        item_value = Decimal(str(item.purchase_order_item.total_cost))
                    else:
                        item_value = Decimal(str(item.allocated_amount or 0))

                    if item_value > 0:
                        all_lines.append({
                            "subcategory_id": inventory_item.inventory_item.asset_account_id,
                            "amount": float(item_value),
                            "dr_cr": "D",
                            "description": f"{inventory_item.inventory_item.item_name} - GR #{goods_receipt.receipt_number}",
                            "source_type": "goods_receipt",
                            "source_id": goods_receipt.id
                        })

        # 2. Debit Non-Inventory Expense Account (consolidated)
        if total_non_inventory_value > 0:
            if not purchase_order.non_inventory_expense_account_id:
                raise Exception("Non-inventory expense account not configured for purchase order")

            all_lines.append({
                "subcategory_id": purchase_order.non_inventory_expense_account_id,
                "amount": float(total_non_inventory_value),
                "dr_cr": "D",
                "description": f"Non-inventory items - GR #{goods_receipt.receipt_number}",
                "source_type": "goods_receipt",
                "source_id": goods_receipt.id
            })

        # 3. Debit Service Expense Account (consolidated)
        if total_service_value > 0:
            if not purchase_order.service_expense_account_id:
                raise Exception("Service expense account not configured for purchase order")

            all_lines.append({
                "subcategory_id": purchase_order.service_expense_account_id,
                "amount": float(total_service_value),
                "dr_cr": "D",
                "description": f"Service items - GR #{goods_receipt.receipt_number}",
                "source_type": "goods_receipt",
                "source_id": goods_receipt.id
            })

        # 4. Debit Shipping and Handling Expense Account
        if total_shipping_handling_value > 0:
            if not purchase_order.shipping_handling_account_id:
                raise Exception("Shipping and handling expense account not configured for purchase order")

            all_lines.append({
                "subcategory_id": purchase_order.shipping_handling_account_id,
                "amount": float(total_shipping_handling_value),
                "dr_cr": "D",
                "description": f"Shipping & Handling - GR #{goods_receipt.receipt_number}",
                "source_type": "goods_receipt",
                "source_id": goods_receipt.id
            })

        # CREDIT SIDE: Handle prepaid and accounts payable
        if total_advance_paid > 0:
            # Determine how much to credit to prepaid account
            credit_to_prepaid = min(total_advance_paid, total_debit_amount)
            remaining_credit = total_debit_amount - credit_to_prepaid

            # Credit prepaid account (for advance payments used)
            if credit_to_prepaid > 0:
                if not purchase_order.preferred_prepaid_account_id:
                    raise Exception("Prepaid account not configured for purchase order")

                all_lines.append({
                    "subcategory_id": purchase_order.preferred_prepaid_account_id,
                    "amount": float(credit_to_prepaid),
                    "dr_cr": "C",
                    "description": f"Advance payment utilization for PO #{purchase_order.purchase_order_number}",
                    "source_type": "goods_receipt",
                    "source_id": goods_receipt.id
                })
                total_credit_amount += credit_to_prepaid

            # Credit accounts payable (for remaining balance)
            if remaining_credit > 0:
                if not purchase_order.accounts_payable_id:
                    raise Exception("Accounts payable account not configured for purchase order")

                all_lines.append({
                    "subcategory_id": purchase_order.accounts_payable_id,
                    "amount": float(remaining_credit),
                    "dr_cr": "C",
                    "description": f"Accounts payable for received goods/services PO #{purchase_order.purchase_order_number}",
                    "source_type": "goods_receipt",
                    "source_id": goods_receipt.id
                })
                total_credit_amount += remaining_credit

        else:
            # No advance payments - credit entire amount to accounts payable
            if not purchase_order.accounts_payable_id:
                raise Exception("Accounts payable account not configured for purchase order")

            all_lines.append({
                "subcategory_id": purchase_order.accounts_payable_id,
                "amount": float(total_debit_amount),
                "dr_cr": "C",
                "description": f"Accounts payable for received goods/services PO #{purchase_order.purchase_order_number}",
                "source_type": "goods_receipt",
                "source_id": goods_receipt.id
            })
            total_credit_amount = total_debit_amount

        # Validate balanced entries
        if abs(total_debit_amount - total_credit_amount) > 0.01:
            raise Exception(f"Debits ({total_debit_amount}) and credits ({total_credit_amount}) don't balance")

        if len(all_lines) < 2:
            raise Exception(f"Insufficient journal lines for goods receipt {goods_receipt.receipt_number}")

        # Generate journal number
        journal_number = generate_unique_journal_number(db_session, app_id)

        # Create journal entry
        journal_success = create_transaction(
            db_session=db_session,
            date=goods_receipt.receipt_date,
            currency=currency_id,
            created_by=current_user.id,
            app_id=app_id,
            journal_number=journal_number,
            journal_ref_no=goods_receipt.receipt_number,
            narration=f"Goods Receipt #{goods_receipt.receipt_number} for PO #{purchase_order.purchase_order_number}",
            vendor_id=purchase_order.vendor_id,
            exchange_rate_id=exchange_rate_id,
            status=status,
            lines=all_lines
        )

        if not journal_success:
            raise Exception(f"Failed to create journal entry for goods receipt {goods_receipt.receipt_number}")

        db_session.flush()

        # Update inventory stock only for inventory items
        inventory_items_to_update = []
        quantities = []
        unit_prices_base = []
        selling_prices_base = []
        locations = []

        for item in inventory_receipt_items:
            if (item.purchase_order_item and
                    item.purchase_order_item.item_type == "inventory" and
                    item.purchase_order_item.inventory_item_variation_link):

                inventory_item = item.purchase_order_item.inventory_item_variation_link
                inventory_items_to_update.append(inventory_item.id)
                quantities.append(item.quantity_received)

                # Use landed cost if available, otherwise fallback
                if item.unit_cost is not None:
                    unit_cost = Decimal(str(item.unit_cost))
                elif item.purchase_order_item.unit_cost is not None:
                    unit_cost = Decimal(str(item.purchase_order_item.unit_cost))
                else:
                    unit_cost = Decimal(str(item.purchase_order_item.unit_price))

                # Convert to base currency if needed
                unit_price_base = float(unit_cost * Decimal(str(exchange_rate_value)))
                unit_prices_base.append(unit_price_base)
                selling_prices_base.append(unit_price_base)
                locations.append(item.purchase_order_item.location_id)

        if inventory_items_to_update:
            from services.inventory_helpers import process_inventory_entries

            process_inventory_entries(
                db_session=db_session,
                app_id=app_id,
                inventory_items=inventory_items_to_update,
                quantities=quantities,
                unit_prices=unit_prices_base,
                selling_prices=selling_prices_base,
                location=locations[0] if locations else None,
                from_location=None,
                to_location=locations[0] if locations else None,
                transaction_date=goods_receipt.receipt_date,
                supplier_id=purchase_order.vendor_id,
                form_currency_id=currency_id,
                base_currency_id=base_currency_id,
                reference=goods_receipt.receipt_number,
                project_id=purchase_order.project_id,
                movement_type='in',
                current_user_id=current_user.id,
                source_type='goods_receipt',
                source_id=goods_receipt.id,
                write_off_account_id=None,
                write_off_reason=None,
                expiration_date=None,
                is_posted_to_ledger=True
            )

        return True, f"Goods receipt successfully posted to ledger"

    except Exception as e:
        logger.error(f"CRITICAL ERROR posting goods receipt to ledger: {str(e)}\n{traceback.format_exc()}")
        raise Exception(f"Goods receipt ledger posting failed: {str(e)}")


def get_inventory_allocations_by_item(db_session, purchase_transaction, allocation, app_id):
    """
    Get individual inventory allocations using the calculated landed costs from DirectPurchaseItem
    """
    inventory_allocations = []

    # Get inventory items with their CALCULATED landed costs
    inventory_items = [
        item for item in purchase_transaction.direct_purchase_items
        if item.item_type == "inventory" and item.item_id
    ]

    total_calculated_inventory = sum(item.total_cost or 0 for item in inventory_items if item.total_cost is not None)

    # If we have calculated costs, use them; otherwise fall back to base costs
    if total_calculated_inventory > 0:
        # Use calculated landed costs
        for item in inventory_items:
            if item.total_cost and float(item.total_cost) > 0:
                # Get the inventory item with its asset account
                inventory_item = db_session.query(InventoryItemVariationLink).filter_by(
                    id=item.item_id, app_id=app_id
                ).options(
                    joinedload(InventoryItemVariationLink.inventory_item)
                ).first()

                if (inventory_item and inventory_item.inventory_item and
                        inventory_item.inventory_item.asset_account_id):
                    item_name = inventory_item.inventory_item.item_name or item.item_name or "Inventory Item"

                    inventory_allocations.append({
                        'asset_account_id': inventory_item.inventory_item.asset_account_id,
                        'amount': float(item.total_cost),  # Use calculated total cost
                        'item_name': item_name,
                        'item_id': item.item_id,
                        'direct_purchase_item_id': item.id
                    })
    else:
        # Fallback: use base costs (proportional allocation)
        total_inventory_value = sum(float(item.total_price or 0) for item in inventory_items)

        if total_inventory_value > 0:
            for item in inventory_items:
                if item.total_price and float(item.total_price) > 0:
                    # Calculate proportion
                    item_proportion = float(item.total_price) / total_inventory_value
                    allocated_amount = float(allocation.allocated_inventory or 0) * item_proportion

                    # Get the inventory item with its asset account
                    inventory_item = db_session.query(InventoryItemVariationLink).filter_by(
                        id=item.item_id, app_id=app_id
                    ).options(
                        joinedload(InventoryItemVariationLink.inventory_item)
                    ).first()

                    if (inventory_item and inventory_item.inventory_item and
                            inventory_item.inventory_item.asset_account_id):
                        item_name = inventory_item.inventory_item.item_name or item.item_name or "Inventory Item"

                        inventory_allocations.append({
                            'asset_account_id': inventory_item.inventory_item.asset_account_id,
                            'amount': allocated_amount,
                            'item_name': item_name,
                            'item_id': item.item_id,
                            'direct_purchase_item_id': item.id
                        })

    return inventory_allocations


def bulk_post_purchase_transactions(db_session, direct_purchase_ids, po_transaction_ids, current_user):
    """
    Bulk update purchase transaction journals from 'Unposted' to 'Posted'
    and mark transactions as fully posted to ledger
    """
    try:
        app_id = current_user.app_id
        success_count = 0
        failed_transactions = []

        # Process direct purchases
        for purchase_id in direct_purchase_ids:
            try:
                # Find the direct purchase transaction
                direct_purchase = db_session.query(DirectPurchaseTransaction).filter(
                    DirectPurchaseTransaction.id == purchase_id,
                    DirectPurchaseTransaction.app_id == app_id
                ).first()

                if not direct_purchase:
                    failed_transactions.append(f"Direct purchase {purchase_id} not found")
                    continue

                # Skip if already fully posted
                if direct_purchase.is_posted_to_ledger:
                    success_count += 1
                    continue

                # Find all payment allocations for this direct purchase
                payment_allocations = direct_purchase.payment_allocations
                allocation_ids = [pa.id for pa in payment_allocations]

                journals_updated = set()

                if allocation_ids:
                    # Find journal entries linked to these allocations
                    journal_entries = db_session.query(JournalEntry).filter(
                        JournalEntry.source_type == "direct_purchase_payment",
                        JournalEntry.source_id.in_(allocation_ids),
                        JournalEntry.app_id == app_id
                    ).all()

                    # Update all related journals to Posted status
                    for entry in journal_entries:
                        if entry.journal and entry.journal.status == 'Unposted':
                            entry.journal.status = "Posted"
                            journals_updated.add(entry.journal.id)

                    # Update payment allocations to posted
                    for allocation in payment_allocations:
                        allocation.is_posted_to_ledger = True

                # Handle shipping and handling costs if not posted
                if not direct_purchase.shipping_handling_posted:
                    shipping_journal_entries = db_session.query(JournalEntry).filter(
                        JournalEntry.source_type == "direct_purchase_shipping",
                        JournalEntry.source_id == direct_purchase.id,
                        JournalEntry.app_id == app_id
                    ).all()

                    for entry in shipping_journal_entries:
                        if entry.journal and entry.journal.status == 'Unposted':
                            entry.journal.status = "Posted"
                            journals_updated.add(entry.journal.id)

                    direct_purchase.shipping_handling_posted = True

                # Mark direct purchase as fully posted
                direct_purchase.is_posted_to_ledger = True
                success_count += 1


            except Exception as e:
                failed_transactions.append(f"Direct purchase {purchase_id}: {str(e)}")
                logger.error(f"Error posting direct purchase {purchase_id}: {str(e)}")
                continue

        # Process PO transactions (payments against purchase orders)
        for transaction_id in po_transaction_ids:
            try:
                # Find the PO transaction
                po_transaction = db_session.query(PurchaseTransaction).filter(
                    PurchaseTransaction.id == transaction_id,
                    PurchaseTransaction.app_id == app_id
                ).first()

                if not po_transaction:
                    failed_transactions.append(f"PO transaction {transaction_id} not found")
                    continue

                # Skip if already fully posted
                if po_transaction.is_posted_to_ledger:
                    success_count += 1
                    continue

                # Find all payment allocations for this PO transaction
                payment_allocations = po_transaction.payment_allocations
                allocation_ids = [pa.id for pa in payment_allocations]
                source_types = ['purchase_order_payment', 'vendor_credit_application', 'vendor_overpayment']

                journals_updated = set()

                if allocation_ids:
                    # Find journal entries linked to these allocations
                    journal_entries = db_session.query(JournalEntry).filter(
                        JournalEntry.source_type.in_(source_types),
                        JournalEntry.source_id.in_(allocation_ids),
                        JournalEntry.app_id == app_id
                    ).all()

                    # Update all related journals to Posted status
                    for entry in journal_entries:
                        if entry.journal and entry.journal.status == 'Unposted':
                            entry.journal.status = "Posted"
                            journals_updated.add(entry.journal.id)

                    # Update payment allocations to posted
                    for allocation in payment_allocations:
                        allocation.is_posted_to_ledger = True

                po_transaction.is_posted_to_ledger = True
                success_count += 1

            except Exception as e:
                failed_transactions.append(f"PO transaction {transaction_id}: {str(e)}")
                logger.error(f"Error posting PO transaction {transaction_id}: {str(e)}")
                continue

        # Commit all changes
        db_session.commit()

        if failed_transactions:
            # Truncate error message if too long
            error_details = ', '.join(failed_transactions[:5])
            if len(failed_transactions) > 5:
                error_details += f" ... and {len(failed_transactions) - 5} more"

            return False, f"Posted {success_count} transactions, failed {len(failed_transactions)}: {error_details}"

        return True, f"Successfully posted {success_count} purchase transactions to ledger"

    except Exception as e:
        db_session.rollback()
        logger.error(f"Bulk purchase posting error: {str(e)}", exc_info=True)
        return False, f"Database error: {str(e)}"


def bulk_post_goods_receipts(db_session, goods_receipt_ids, current_user):
    """
    Bulk update goods receipt journals from 'Unposted' to 'Posted'
    and mark goods receipts as fully posted to ledger
    """
    try:
        app_id = current_user.app_id
        success_count = 0
        failed_receipts = []

        for receipt_id in goods_receipt_ids:
            try:
                # Find the goods receipt with related data
                goods_receipt = db_session.query(GoodsReceipt).options(
                    joinedload(GoodsReceipt.receipt_items)
                ).filter(
                    GoodsReceipt.id == receipt_id,
                    GoodsReceipt.app_id == app_id
                ).first()

                if not goods_receipt:
                    failed_receipts.append(f"Goods receipt {receipt_id} not found")
                    continue

                # Skip if already fully posted
                if goods_receipt.is_posted_to_ledger:
                    success_count += 1
                    continue

                # Find all journal entries for this goods receipt
                journal_entries = db_session.query(JournalEntry).options(
                    joinedload(JournalEntry.journal)
                ).filter(
                    JournalEntry.source_type == "goods_receipt",
                    JournalEntry.source_id == goods_receipt.id,
                    JournalEntry.app_id == app_id
                ).all()

                if not journal_entries:
                    failed_receipts.append(f"Goods receipt {receipt_id} has no journal entries")
                    continue

                # Update all related journals to Posted status
                journals_updated = set()
                for entry in journal_entries:
                    if entry.journal and entry.journal.status == 'Unposted':
                        entry.journal.status = "Posted"
                        entry.journal.updated_at = datetime.now(timezone.utc)
                        journals_updated.add(entry.journal.id)
                        logger.debug(f"Updated journal {entry.journal.id} to Posted status")

                # Mark goods receipt and its items as posted to ledger
                goods_receipt.is_posted_to_ledger = True
                goods_receipt.updated_at = datetime.now(timezone.utc)

                for item in goods_receipt.receipt_items:
                    item.is_posted_to_ledger = True
                    item.updated_at = datetime.now(timezone.utc)

                success_count += 1

            except Exception as e:
                error_msg = f"Goods receipt {receipt_id}: {str(e)}"
                failed_receipts.append(error_msg)
                logger.error(f"Error posting goods receipt {receipt_id}: {str(e)}", exc_info=True)
                continue

        # Commit all changes
        db_session.commit()

        if failed_receipts:
            # Truncate error message if too long
            error_details = ', '.join(failed_receipts[:5])  # Show first 5 errors
            if len(failed_receipts) > 5:
                error_details += f" ... and {len(failed_receipts) - 5} more"

            return False, f"Posted {success_count} goods receipts, failed {len(failed_receipts)}: {error_details}"

        return True, f"Successfully posted {success_count} goods receipts to ledger"

    except Exception as e:
        db_session.rollback()
        logger.error(f"Bulk goods receipt posting error: {str(e)}", exc_info=True)
        return False, f"Database error: {str(e)}"


def create_purchase_payment_journal_entries(
        db_session,
        purchase_transaction,
        payment_amount,
        payment_date,
        payment_type,
        asset_account_id,
        status,
        base_currency_id,
        prepaid_account_id=None,
        payment_method_id=None,
        reference=None,
        created_by=None,
        app_id=None,
        allocation_id=None,
        exchange_rate_id=None,
        exchange_rate_value=None,
):
    """
    Create journal entries for purchase payments.

    JOURNAL 1: Record payment in transaction currency
        Dr Accounts Payable/Prepaid (payment currency)
        Cr Cash/Bank (payment currency)

    JOURNAL 2: FX gain/loss adjustment in base currency
        If GAIN (paid less than expected):
            Dr Accounts Payable (gain amount)
            Cr FX Gain Account (gain amount)

        If LOSS (paid more than expected):
            Dr FX Loss Account (loss amount)
            Cr Accounts Payable (loss amount)
    """

    po = purchase_transaction.purchase_orders
    if not po:
        raise ValueError("Purchase order not found")

    # Get system accounts
    system_accounts = get_all_system_accounts(
        db_session=db_session,
        app_id=app_id,
        created_by_user_id=created_by
    )

    fx_gain_loss_account_id = system_accounts.get('fx_gain_loss')

    # Get exchange rates
    po_exchange_rate = po.exchange_rate.rate if po.exchange_rate else Decimal(1)
    payment_exchange_rate = Decimal(exchange_rate_value) if exchange_rate_value else po_exchange_rate

    # Round payment amount
    payment_amount = Decimal(payment_amount).quantize(Decimal('0.01'))

    # Determine the debit account based on payment type
    if payment_type == 'advance_payment':
        debit_account_id = prepaid_account_id
        debit_description = f"Advance payment to vendor for PO #{po.purchase_order_number}"
    else:
        debit_account_id = po.accounts_payable_id
        debit_description = f"Payment to vendor for PO #{po.purchase_order_number}"

    if not debit_account_id:
        raise ValueError(f"No debit account configured for {payment_type} payment")

    # Calculate expected and actual base amounts
    expected_base_amount = (payment_amount * po_exchange_rate).quantize(Decimal('0.01'))
    actual_base_amount = (payment_amount * payment_exchange_rate).quantize(Decimal('0.01'))

    # Calculate FX difference
    fx_diff = actual_base_amount - expected_base_amount

    # Generate reference
    journal_ref_no = reference or purchase_transaction.reference_number or po.purchase_order_number

    # ===== JOURNAL 1: Payment in transaction currency =====
    # Dr Accounts Payable/Prepaid, Cr Cash/Bank
    journal1_lines = [
        {
            "subcategory_id": debit_account_id,
            "amount": payment_amount,
            "dr_cr": "D",
            "description": debit_description,
            "source_type": "purchase_order_payment",
            "source_id": allocation_id
        },
        {
            "subcategory_id": asset_account_id,
            "amount": payment_amount,
            "dr_cr": "C",
            "description": f"Payment for PO #{po.purchase_order_number} - {reference or 'No reference'}",
            "source_type": "purchase_order_payment",
            "source_id": allocation_id
        }
    ]

    journal1, entries1 = create_transaction(
        db_session=db_session,
        date=payment_date,
        currency=purchase_transaction.currency_id,  # Payment currency
        created_by=created_by,
        app_id=app_id,
        journal_ref_no=journal_ref_no,
        narration=f"Purchase Payment - PO #{po.purchase_order_number} - {reference or 'No reference'}",
        payment_mode_id=payment_method_id,
        vendor_id=po.vendor_id,
        exchange_rate_id=exchange_rate_id,
        status=status,
        lines=journal1_lines
    )

    # ===== JOURNAL 2: FX gain/loss adjustment (in base currency) =====
    if abs(fx_diff) > Decimal('0.01') and fx_gain_loss_account_id:
        if fx_diff > 0:  # LOSS - You paid MORE than expected
            # Dr FX Loss, Cr Accounts Payable
            journal2_lines = [
                {
                    "subcategory_id": fx_gain_loss_account_id,
                    "amount": abs(fx_diff),
                    "dr_cr": "D",
                    "description": f"Foreign exchange loss on PO #{po.purchase_order_number}",
                    "source_type": "purchase_order_payment",
                    "source_id": allocation_id
                },
                {
                    "subcategory_id": debit_account_id,
                    "amount": abs(fx_diff),
                    "dr_cr": "C",
                    "description": f"Adjust payable for FX loss on PO #{po.purchase_order_number}",
                    "source_type": "purchase_order_payment",
                    "source_id": allocation_id
                }
            ]
            narration_fx = f"FX adjustment - PO #{po.purchase_order_number} (FX Loss: {abs(fx_diff):,.2f})"
        else:  # GAIN - You paid LESS than expected
            # Dr Accounts Payable, Cr FX Gain
            journal2_lines = [
                {
                    "subcategory_id": debit_account_id,
                    "amount": abs(fx_diff),
                    "dr_cr": "D",
                    "description": f"Adjust payable for FX gain on PO #{po.purchase_order_number}",
                    "source_type": "purchase_order_payment",
                    "source_id": allocation_id
                },
                {
                    "subcategory_id": fx_gain_loss_account_id,
                    "amount": abs(fx_diff),
                    "dr_cr": "C",
                    "description": f"Foreign exchange gain on PO #{po.purchase_order_number}",
                    "source_type": "purchase_order_payment",
                    "source_id": allocation_id
                }
            ]
            narration_fx = f"FX adjustment - PO #{po.purchase_order_number} (FX Gain: {abs(fx_diff):,.2f})"

        journal2, entries2 = create_transaction(
            db_session=db_session,
            date=payment_date,
            currency=base_currency_id,  # Base currency for FX journal
            created_by=created_by,
            app_id=app_id,
            journal_ref_no=f"{journal_ref_no}-FX",
            narration=narration_fx,
            payment_mode_id=None,
            vendor_id=po.vendor_id,
            exchange_rate_id=None,
            status=status,
            lines=journal2_lines
        )

        # Return entries from both journals
        return entries1 + entries2

    # No FX - return only journal 1
    return entries1


def update_prepaid_journals_for_po(db_session, purchase_order_id, new_prepaid_account_id, user_id):
    """
    Update all journal entries for advance payments to use the new prepaid account.
    Directly updates the account IDs in existing entries for simplicity.
    """
    try:
        # Find all journal entries for advance payments on this PO that need updating
        journal_entries_to_update = db_session.query(JournalEntry) \
            .join(PurchasePaymentAllocation, JournalEntry.source_id == PurchasePaymentAllocation.id) \
            .join(PurchaseTransaction, PurchasePaymentAllocation.payment_id == PurchaseTransaction.id) \
            .filter(
            PurchaseTransaction.purchase_order_id == purchase_order_id,
            PurchasePaymentAllocation.payment_type == 'advance_payment',
            JournalEntry.source_type == 'purchase_order_payment',
            JournalEntry.dr_cr == 'D',  # Debit entries (prepaid account side)
            JournalEntry.subcategory_id != new_prepaid_account_id
        ) \
            .all()

        journals_updated = 0

        for journal_entry in journal_entries_to_update:
            # Simply update the subcategory_id to the new prepaid account
            old_account_id = journal_entry.subcategory_id
            journal_entry.subcategory_id = new_prepaid_account_id
            journal_entry.description = f"Updated prepaid account - {journal_entry.description}"

            journals_updated += 1

        # Update payment allocations to use the new prepaid account
        advance_allocations = db_session.query(PurchasePaymentAllocation) \
            .join(PurchaseTransaction) \
            .filter(
            PurchaseTransaction.purchase_order_id == purchase_order_id,
            PurchasePaymentAllocation.payment_type == 'advance_payment',
            PurchasePaymentAllocation.prepaid_account_id != None
        ) \
            .all()

        for allocation in advance_allocations:
            old_account_id = allocation.prepaid_account_id
            allocation.prepaid_account_id = new_prepaid_account_id
        # Update PO's preferred account
        po = db_session.query(PurchaseOrder).get(purchase_order_id)
        if po.preferred_prepaid_account_id != new_prepaid_account_id:
            old_po_account = po.preferred_prepaid_account_id
            po.preferred_prepaid_account_id = new_prepaid_account_id

        return {
            'success': True,
            'journals_updated': journals_updated,
            'allocations_updated': len(advance_allocations),
            'message': f'Updated {journals_updated} journal entries and {len(advance_allocations)} allocations to use new prepaid account'
        }

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error updating prepaid journals: {str(e)}\n{traceback.format_exc()}")
        return {
            'success': False,
            'error': str(e)
        }
