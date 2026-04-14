# app/routes/payroll/payrun.py
import decimal
import logging
import traceback
from collections import defaultdict
from datetime import datetime, date

from flask import Blueprint, jsonify, render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from sqlalchemy import func, distinct, or_, exists
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import joinedload

from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Department, Employee, \
    PayrollPeriod, PayrollTransaction, Deduction, Benefit, BenefitType, \
    DeductionType, PayRollStatusEnum, AdvanceRepayment, AdvancePayment, PaymentMode, ChartOfAccounts, PayrollPayment, \
    DeductionPayment, PaymentStatusEnum, PayrollPeriodStatusEnum, UserPreference, ExchangeRate
from services.payroll_helpers import validate_payroll_period, generate_next_pay_run_number
from services.post_to_ledger import create_payroll_journal_entries
from services.post_to_ledger_reversal import repost_transaction
from utils_and_helpers.exchange_rates import get_or_create_exchange_rate_for_transaction

# Import the same blueprint
from . import payroll_bp

from utils import empty_to_none, create_notification
from utils_and_helpers.date_time_utils import parse_date

logger = logging.getLogger()


@payroll_bp.route('/create_pay_run', methods=['POST'])
@login_required
def create_pay_run():
    db_session = Session()
    app_id = current_user.app_id

    from payroll import get_payroll_frequency
    try:
        pay_run_data = request.get_json()

        if not pay_run_data:
            return jsonify({'success': False, 'message': 'Invalid or missing payload'}), 400

        start_date = parse_date(pay_run_data["start_date"])
        end_date = parse_date(pay_run_data["end_date"])

        # Ensure date-only and prevent future dates
        today = date.today()
        start_date = start_date.date() if isinstance(start_date, datetime) else start_date
        end_date = end_date.date() if isinstance(end_date, datetime) else end_date

        if start_date > today or end_date > today:
            return jsonify({'success': False, 'message': 'Start and end dates cannot be in the future.'}), 400
        if end_date < start_date:
            return jsonify({'success': False, 'message': 'End date cannot be before start date.'}), 400

        # After getting exchange_rates from payload
        exchange_rates = pay_run_data.get("exchange_rates", {})
        base_currency_id = pay_run_data.get("base_currency_id")

        if not base_currency_id:
            # Get base currency
            base_currency_info = get_base_currency(db_session, app_id)
            base_currency_id = base_currency_info["base_currency_id"]
        base_currency_id = int(base_currency_id)

        # ===== ATOMIC OPERATION START =====
        # Start a transaction
        db_session.begin_nested()

        try:
            # Lock the necessary records to prevent concurrent modifications
            # Get all employee IDs from the pay run
            employee_ids = [int(emp["id"]) for emp in pay_run_data["employee_details"]]

            # Lock employees to prevent changes during pay run creation
            employees = db_session.query(Employee).filter(
                Employee.id.in_(employee_ids),
                Employee.app_id == app_id
            ).with_for_update().all()

            if len(employees) != len(employee_ids):
                return jsonify({'success': False, 'message': 'One or more employees not found'}), 404

            # Lock advance payments that will be affected
            all_advance_ids = []
            for emp in pay_run_data["employee_details"]:
                if "advance_payments" in emp:
                    all_advance_ids += [int(r["advanceId"]) for r in emp["advance_payments"]]

            advance_records = {}
            if all_advance_ids:
                # Lock advances to prevent double-deduction
                advances = db_session.query(AdvancePayment).filter(
                    AdvancePayment.id.in_(all_advance_ids),
                    AdvancePayment.app_id == app_id
                ).with_for_update().all()
                advance_records = {a.id: a for a in advances}

                # Verify all advances exist
                if len(advances) != len(set(all_advance_ids)):
                    return jsonify({'success': False, 'message': 'One or more advance payments not found'}), 404

            payroll_frequency = get_payroll_frequency(start_date, end_date)

            # Create PayrollPeriod
            payroll_period = PayrollPeriod(
                payroll_period_name=pay_run_data["payroll_period_name"],
                start_date=start_date,
                end_date=end_date,
                payroll_status=pay_run_data["status"],
                payment_status=PayRollStatusEnum.PENDING,
                app_id=app_id,
                payroll_frequency=payroll_frequency,
                created_by=current_user.id,
            )
            db_session.add(payroll_period)
            db_session.flush()

            # Cache: preload currencies
            user_currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
            currency_map = {c.user_currency: c.id for c in user_currencies}

            # Store all created transactions in a list
            created_transactions = []  # ← Track them

            # Process employees
            for employee in pay_run_data["employee_details"]:
                # Prefer provided currency_id, fallback to cached lookup
                currency_id = employee.get("currency_id") or currency_map.get(employee["currency"])

                # Handle exchange rate for foreign currency
                # Handle exchange rate for foreign currency
                exchange_rate_id = None
                rate_obj = None
                if currency_id != base_currency_id:
                    # Get the rate from frontend - use currency_id as key, not currency code
                    rate_value = exchange_rates.get(str(currency_id))

                    if rate_value:
                        # Use your existing helper function
                        exchange_rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                            session=db_session,
                            action='create',
                            from_currency_id=currency_id,
                            to_currency_id=base_currency_id,
                            rate_value=rate_value,
                            rate_date=end_date,
                            app_id=app_id,
                            created_by=current_user.id,
                            source_type='payroll',
                            source_id=None,
                            currency_exchange_transaction_id=None
                        )

                new_payroll_transaction = PayrollTransaction(
                    employee_id=int(employee["id"]),
                    payroll_period_id=payroll_period.id,
                    gross_salary=employee["gross_salary"],
                    net_salary=employee["net_salary"],
                    currency_id=currency_id,
                    exchange_rate_id=exchange_rate_id,  # ADD THIS
                    app_id=app_id,
                    balance_due=employee["net_salary"],
                    created_by=current_user.id,
                    is_posted_to_ledger=True
                )
                db_session.add(new_payroll_transaction)
                db_session.flush()
                created_transactions.append(new_payroll_transaction)  # ← Store

                # If we created exchange rates for this payroll, update their source_id
                if rate_obj:
                    rate_obj.source_id = payroll_period.id
                    db_session.add(rate_obj)

                # Deductions
                for deduction in employee.get("deductions", []):
                    db_session.add(Deduction(
                        employee_id=employee["id"],
                        payroll_transaction_id=new_payroll_transaction.id,
                        deduction_type_id=deduction.get("deductionId"),
                        amount=deduction.get("calculated_amount"),
                        currency_id=currency_id,
                        exchange_rate_id=exchange_rate_id,
                        payroll_period_id=payroll_period.id,
                        paid_amount=0,
                        balance_due=deduction.get("calculated_amount"),
                        is_posted_to_ledger=True,
                        app_id=app_id
                    ))

                # Benefits
                for benefit in employee.get("benefits", []):
                    db_session.add(Benefit(
                        employee_id=employee["id"],
                        payroll_transaction_id=new_payroll_transaction.id,
                        benefit_type_id=benefit.get("benefitId"),
                        amount=benefit.get("calculated_amount"),
                        currency_id=currency_id,
                        exchange_rate_id=exchange_rate_id,
                        payroll_period_id=payroll_period.id,
                        app_id=app_id
                    ))

                # Advance repayments
                for repayment in employee.get("advance_payments", []):
                    advance_id = int(repayment["advanceId"])
                    deduction_amount = decimal.Decimal(str(repayment["deduction_per_payroll"]))

                    db_advance_repayment = AdvanceRepayment(
                        advance_payment_id=advance_id,
                        payment_date=end_date,
                        payment_amount=deduction_amount,
                        payroll_id=payroll_period.id,
                        exchange_rate_id=exchange_rate_id,
                        payroll_transaction_id=new_payroll_transaction.id,
                        app_id=app_id,
                        is_posted_to_ledger=True
                    )
                    db_session.add(db_advance_repayment)
                    db_session.flush()

                    # Update advance balance
                    advance_transaction = advance_records.get(advance_id)
                    if advance_transaction:
                        advance_transaction.amount_paid += deduction_amount
                        advance_transaction.update_balances()

                        # Check if advance is fully paid
                        if advance_transaction.remaining_balance <= 0:
                            advance_transaction.status = 'paid'

            # Create journal entries (this also happens within the transaction)
            journals_created = create_payroll_journal_entries(
                db_session=db_session,
                payroll_period=payroll_period,
                payroll_transactions=created_transactions,
                app_id=app_id,
                current_user_id=current_user.id,
                status="Posted",
                base_currency_id=base_currency_id
            )

            if not journals_created:
                db_session.rollback()
                e = "Journal Posting Failed"
                return jsonify({'success': False, 'message': str(e)}), 500

            # Commit the entire transaction atomically
            db_session.commit()

            return jsonify({
                'success': True,
                'message': 'Pay run created successfully',
                'pay_run_id': payroll_period.id,
                'journals_created': journals_created  # Just return the count
            })

        except Exception as e:
            # Rollback the entire transaction if anything fails
            db_session.rollback()
            raise e

    except Exception as e:
        logger.error(f"Error creating pay run: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500

    finally:
        db_session.close()


@payroll_bp.route('/generate_pay_run', methods=['GET'])
@login_required
def generate_pay_run():
    db_session = Session()
    if current_user.role not in ['Admin', 'Contributor']:
        flash('You do not have permission to access this page.', 'warning')
        return redirect(url_for('payroll_dashboard'))

    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch all active employees
        employees = db_session.query(Employee).filter_by(app_id=app_id, is_active=True).all()
        benefit_types = db_session.query(BenefitType).filter_by(app_id=app_id).all()
        deduction_types = db_session.query(DeductionType).filter_by(app_id=app_id).all()

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id

        # Group employees by department
        grouped_employees = defaultdict(list)
        for employee in employees:
            department_name = employee.department.department_name if employee.department else "Unassigned"
            grouped_employees[department_name].append(employee)

        return render_template(
            'payroll/create_pay_run.html',
            deduction_types=deduction_types,
            benefit_types=benefit_types,
            grouped_employees=grouped_employees,
            company=company,
            modules=modules_data,
            role=role,
            base_currency=base_currency,
            base_currency_code=base_currency_code,
            base_currency_id=base_currency_id,
            module_name="Payroll"
        )

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error creating pay run: {str(e)}")  # Debug: Print the full error
        return jsonify({'success': False, 'message': str(e)}), 500

    finally:
        db_session.close()  # Ensures session is closed even if an error occurs


@payroll_bp.route('/pay_runs', methods=['GET', 'POST'])
@login_required
def get_pay_runs():
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Get filter parameters
        if request.method == 'POST':
            currency_id = request.form.get("currency")
            filter_by = request.form.get("filter_by", "date_generated")
            start_date = request.form.get("start_date")
            end_date = request.form.get("end_date")
        else:  # GET
            currency_id = None
            filter_by = "date_generated"
            start_date = None
            end_date = None

        filter_applied = bool(start_date or end_date or currency_id)

        # Default dates if not provided
        if not start_date or not end_date:
            earliest_date = db_session.query(func.min(PayrollPeriod.creation_date)).filter_by(app_id=app_id).scalar()
            latest_date = db_session.query(func.max(PayrollPeriod.creation_date)).filter_by(app_id=app_id).scalar()
            start_date = earliest_date if not start_date else start_date
            end_date = latest_date if not end_date else end_date

        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        # User currencies
        user_currency = db_session.query(Currency).filter_by(app_id=app_id).order_by(Currency.currency_index).all()

        # Determine selected currency
        if not currency_id:
            main_currency = db_session.query(Currency).filter_by(currency_index=1, app_id=app_id).first()
            currency_id = main_currency.id
            selected_currency = main_currency.user_currency
        else:
            selected_currency = db_session.query(Currency.user_currency).filter_by(id=currency_id).scalar()

        # Base query
        pay_runs_query = (
            db_session.query(
                PayrollPeriod,
                func.sum(PayrollTransaction.gross_salary).label('total_gross_salary'),
                func.sum(PayrollTransaction.net_salary).label('total_net_salary'),
                func.sum(PayrollTransaction.paid_amount).label('total_paid_amount'),
                func.sum(PayrollTransaction.balance_due).label('total_balance_due'),
                func.count(distinct(PayrollTransaction.employee_id)).label('employee_count'),
                PayrollTransaction.currency_id,
                func.max(PayrollTransaction.is_posted_to_ledger).label('is_posted_to_ledger')
            )
            .join(PayrollTransaction, PayrollPeriod.id == PayrollTransaction.payroll_period_id)
            .filter(PayrollPeriod.app_id == app_id)
        )

        # Apply filters
        if currency_id:
            pay_runs_query = pay_runs_query.filter(PayrollTransaction.currency_id == currency_id)

        if filter_by == "date_generated":
            if start_date:
                pay_runs_query = pay_runs_query.filter(PayrollPeriod.creation_date >= start_date)
            if end_date:
                pay_runs_query = pay_runs_query.filter(PayrollPeriod.creation_date <= end_date)
        elif filter_by == "start_end_date":
            if start_date:
                pay_runs_query = pay_runs_query.filter(PayrollPeriod.start_date >= start_date)
            if end_date:
                pay_runs_query = pay_runs_query.filter(PayrollPeriod.end_date <= end_date)

        pay_runs_data = pay_runs_query.group_by(PayrollPeriod.id, PayrollTransaction.currency_id).all()

        return render_template(
            "payroll/pay_runs.html",
            company=company,
            modules=modules_data,
            role=role,
            payruns=pay_runs_data,
            user_currency=user_currency,
            selected_currency=selected_currency,
            module_name="Payroll",
            filter_applied=filter_applied
        )

    except Exception as e:
        logger.error(f"[ERROR] An error occurred while fetching pay runs: {e}")
        return jsonify({"success": False, "message": "An error occurred while processing your request."}), 500
    finally:
        db_session.close()


@payroll_bp.route('/generate_next_pay_run', methods=['POST'])
@login_required
def get_next_pay_run():
    db_session = None  # Initialize db_session to ensure it's defined for the finally block
    logger.info(f'This has been called')
    try:
        # Parse JSON data
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided in the request"}), 400

        start_date_str = data.get('startDate')
        end_date_str = data.get('endDate')

        # Validate required fields
        if not start_date_str or not end_date_str:
            return jsonify({"error": "Start date and end date are required"}), 400

        # Parse dates
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

        except ValueError as e:
            logger.error(f'Error ahs been gotten {e}')
            return jsonify({
                'success': False,
                'error': f"Invalid date format. Use YYYY-MM-DD. Error: {str(e)}"
            }), 400  # Use 400 for client errors, 500 is for server errors
        # Validate start_date <= end_date
        if start_date > end_date:
            return jsonify(
                {'success': False, 'message': '"error": "Start date must be before or equal to end date"}'}), 400

        db_session = Session()
        app_id = current_user.app_id  # Ensure the user is authenticated

        # Validate the payroll period
        validation_result = validate_payroll_period(db_session, app_id, start_date, end_date)

        # If validation returns a frequency (weekly/bi-weekly/monthly), proceed
        if validation_result.lower() in ['weekly', 'bi-weekly', 'monthly', 'daily']:
            pay_run_period = generate_next_pay_run_number(db_session, app_id)
            return jsonify({
                "pay_run_period": pay_run_period,
                "frequency": validation_result
            })
        else:  # Otherwise treat as error
            return jsonify({"error": validation_result}), 400

    except SQLAlchemyError as e:
        if db_session:
            db_session.rollback()
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

    finally:
        # Ensure the database session is closed
        if db_session:
            db_session.close()


@payroll_bp.route('/payrun_summary/<int:payroll_period_id>', methods=['GET'])
@login_required
def get_payroll_summary(payroll_period_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id

        do_not_show_again = db_session.query(UserPreference.do_not_show_again).filter_by(
            preference_type="payroll_make_full_payment", user_id=current_user.id).scalar()
        # Get funding accounts (Cash or Bank accounts)
        funding_accounts = (
            db_session.query(ChartOfAccounts)
            .filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_system_account.is_(False),
                or_(ChartOfAccounts.is_cash == True, ChartOfAccounts.is_bank == True)
            )
            .all()
        )

        # Get filter parameters from request
        department_filter = request.args.get("department")
        salary_type_filter = request.args.get("salary_type")
        currency_filter = request.args.get("currency")
        filter_applied = bool(currency_filter or department_filter or salary_type_filter)
        # Fetch payroll period name
        payroll_period = db_session.query(PayrollPeriod).filter_by(id=payroll_period_id, app_id=app_id).first()
        if not payroll_period:
            return "Payroll period not found", 404

        # Subquery for total benefits
        benefits_subquery = (
            db_session.query(
                Benefit.payroll_transaction_id,
                func.sum(Benefit.amount).label('total_benefits')
            )
            .filter(Benefit.payroll_period_id == payroll_period_id)
            .group_by(Benefit.payroll_transaction_id)
            .subquery()
        )

        # Subquery for total deductions
        deductions_subquery = (
            db_session.query(
                Deduction.payroll_transaction_id,
                func.sum(Deduction.amount).label('total_deductions')
            )
            .filter(Deduction.payroll_period_id == payroll_period_id)
            .group_by(Deduction.payroll_transaction_id)
            .subquery()
        )

        # Subquery for unposted payments - properly filtered by app_id
        unposted_payments_subquery = (
            db_session.query(
                PayrollPayment.payroll_transaction_id,
                func.count(PayrollPayment.id).label('unposted_payments_count')
            )
            .join(PayrollTransaction, PayrollPayment.payroll_transaction_id == PayrollTransaction.id)
            .filter(PayrollPayment.is_posted_to_ledger == False)
            .filter(PayrollTransaction.app_id == app_id)
            .filter(PayrollTransaction.payroll_period_id == payroll_period_id)
            .group_by(PayrollPayment.payroll_transaction_id)
            .subquery()
        )

        # Base query for payroll transactions
        # Base query for payroll transactions
        query = (
            db_session.query(
                Department.department_name,
                Employee.salary_type,
                (Employee.first_name + ' ' + Employee.last_name).label('employee_name'),
                Currency.user_currency,
                Currency.id.label('currency_id'),  # ✅ Add currency_id
                PayrollTransaction.id.label('payroll_transaction_id'),
                PayrollTransaction.gross_salary,
                PayrollTransaction.net_salary,
                func.coalesce(benefits_subquery.c.total_benefits, 0).label('total_benefits'),
                func.coalesce(deductions_subquery.c.total_deductions, 0).label('total_deductions'),
                PayrollTransaction.paid_amount,
                PayrollTransaction.balance_due,
                PayrollTransaction.payment_status,
                PayrollTransaction.is_posted_to_ledger,
                func.coalesce(unposted_payments_subquery.c.unposted_payments_count, 0).label('unposted_payments_count'),
                PayrollTransaction.payable_account_id,
                PayrollTransaction.exchange_rate_id,
                func.coalesce(ExchangeRate.rate, 1).label('exchange_rate')  # ✅ Add exchange rate value
            )
            .join(Employee, PayrollTransaction.employee_id == Employee.id)
            .join(Department, Employee.department_id == Department.id)
            .join(Currency, Employee.base_currency == Currency.id)
            .outerjoin(ExchangeRate, PayrollTransaction.exchange_rate_id == ExchangeRate.id)  # ✅ Join exchange_rate
            .outerjoin(benefits_subquery, benefits_subquery.c.payroll_transaction_id == PayrollTransaction.id)
            .outerjoin(deductions_subquery, deductions_subquery.c.payroll_transaction_id == PayrollTransaction.id)
            .outerjoin(unposted_payments_subquery,
                       unposted_payments_subquery.c.payroll_transaction_id == PayrollTransaction.id)
            .filter(PayrollTransaction.app_id == app_id)
            .filter(PayrollTransaction.payroll_period_id == payroll_period_id)
        )

        # Apply filters
        if department_filter:
            query = query.filter(Department.id == department_filter)
        if salary_type_filter:
            query = query.filter(Employee.salary_type == salary_type_filter)
        if currency_filter:
            query = query.filter(Currency.id == currency_filter)

        # Count of payroll transactions
        payroll_count = db_session.query(func.count(PayrollTransaction.id)).filter(
            PayrollTransaction.app_id == app_id,
            PayrollTransaction.payroll_period_id == payroll_period_id
        ).scalar()

        # Execute the query
        payroll_summary = query.all()

        # Calculate totals with None handling
        totals = {
            "gross_salary": sum(payroll.gross_salary or 0 for payroll in payroll_summary),
            "net_salary": sum(payroll.net_salary or 0 for payroll in payroll_summary),
            "total_benefits": sum(payroll.total_benefits or 0 for payroll in payroll_summary),
            "total_deductions": sum(payroll.total_deductions or 0 for payroll in payroll_summary),
            "paid_amount": sum(payroll.paid_amount or 0 for payroll in payroll_summary),
            "balance_due": sum(payroll.balance_due or 0 for payroll in payroll_summary),
        }

        return render_template(
            "payroll/payrun_summary.html",
            payroll_summary=payroll_summary,
            totals=totals,
            departments=db_session.query(Department).filter_by(app_id=app_id).all(),
            currencies=currencies,
            base_currency=base_currency,
            base_currency_code=base_currency_code,
            base_currency_id=base_currency_id,
            payroll_period_name=payroll_period.payroll_period_name,
            company=company,
            modules=modules_data,
            funding_accounts=funding_accounts,
            role=role,
            payroll_period_id=payroll_period_id,
            payment_modes=payment_modes,
            payroll_count=payroll_count,
            filter_applied=filter_applied,
            do_not_show_again=do_not_show_again,
            module_name="Payroll",
            app_id=app_id  # ✅ Add app_id for exchange rate API calls
        )
    finally:
        db_session.close()  # Ensure the session is closed


@payroll_bp.route("/payrun_transaction_details/<int:transaction_id>")
@login_required
def payrun_transaction_details(transaction_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id

        # Get funding accounts (Cash or Bank accounts)
        funding_accounts = (
            db_session.query(ChartOfAccounts)
            .filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_system_account == False,
                or_(ChartOfAccounts.is_cash == True, ChartOfAccounts.is_bank == True)
            )
            .all()
        )

        # Fetch payroll transaction and ensure payroll_period is loaded
        transaction = db_session.query(PayrollTransaction).options(
            joinedload(PayrollTransaction.payroll_payments),  # Eager load payroll payments
            joinedload(PayrollTransaction.payroll_period),  # Eager load payroll_period
            joinedload(PayrollTransaction.deductions).joinedload(Deduction.deduction_type),  # Eager load deductions
            joinedload(PayrollTransaction.benefits).joinedload(Benefit.benefit_type)  # Eager load benefits
        ).filter_by(id=transaction_id).first()

        if not transaction:
            # Handle case where transaction is not found
            return redirect(url_for('payroll_transaction_not_found'))

        # Subquery for total benefits
        benefits_subquery = (
            db_session.query(
                Benefit.payroll_transaction_id,
                func.sum(Benefit.amount).label('total_benefits')
            )
            .filter(Benefit.payroll_transaction_id == transaction_id)
            .group_by(Benefit.payroll_transaction_id)
            .subquery()
        )

        # Subquery for total deductions
        deductions_subquery = (
            db_session.query(
                Deduction.payroll_transaction_id,
                func.sum(Deduction.amount).label('total_deductions')
            )
            .filter(Deduction.payroll_transaction_id == transaction_id)
            .group_by(Deduction.payroll_transaction_id)
            .subquery()
        )

        # Subquery for total advance repayments
        advance_repayment_subquery = (
            db_session.query(
                AdvanceRepayment.payroll_id.label('payroll_period_id'),
                AdvancePayment.employee_id.label('employee_id'),
                func.sum(AdvanceRepayment.payment_amount).label('total_advance_deductions')
            )
            .join(AdvancePayment, AdvanceRepayment.advance_payment_id == AdvancePayment.id)
            .filter(AdvanceRepayment.app_id == app_id)
            .group_by(AdvanceRepayment.payroll_id, AdvancePayment.employee_id)
            .subquery()
        )

        # Base query for the payroll transaction details
        query = (
            db_session.query(
                Employee.first_name,
                Employee.middle_name,
                Employee.last_name,
                PayrollTransaction.gross_salary,
                PayrollTransaction.net_salary,
                func.coalesce(benefits_subquery.c.total_benefits, 0).label('total_benefits'),
                func.coalesce(deductions_subquery.c.total_deductions, 0).label('total_deductions'),
                func.coalesce(advance_repayment_subquery.c.total_advance_deductions, 0).label(
                    'total_advance_deductions'),
                PayrollTransaction.paid_amount,
                PayrollTransaction.balance_due,
                PayrollTransaction.payment_status,
                Currency.id.label('currency_id'),
                Currency.user_currency,
                PayrollPeriod.start_date,
                PayrollPeriod.end_date,
                PayrollTransaction.exchange_rate_id,  # ✅ Add this
                ExchangeRate.rate.label('exchange_rate')  # ✅ Add this to get the actual rate
            )
            .join(Employee, PayrollTransaction.employee_id == Employee.id)
            .join(Currency, Employee.base_currency == Currency.id)
            .outerjoin(ExchangeRate, PayrollTransaction.exchange_rate_id == ExchangeRate.id)  # ✅ Add this join
            .outerjoin(benefits_subquery, benefits_subquery.c.payroll_transaction_id == PayrollTransaction.id)
            .outerjoin(deductions_subquery, deductions_subquery.c.payroll_transaction_id == PayrollTransaction.id)
            .outerjoin(
                advance_repayment_subquery,
                (advance_repayment_subquery.c.payroll_period_id == PayrollTransaction.payroll_period_id) &
                (advance_repayment_subquery.c.employee_id == PayrollTransaction.employee_id)
            )
            .join(PayrollPeriod, PayrollTransaction.payroll_period_id == PayrollPeriod.id)
            .filter(PayrollTransaction.app_id == app_id)
            .filter(PayrollTransaction.id == transaction_id)
        )

        # Execute the query
        transaction_details = query.first()

        if not transaction_details:
            return redirect(url_for('payroll_transaction_not_found'))

        # Calculate totals with None handling (if needed for summary)
        totals = {
            "gross_salary": transaction_details.gross_salary or 0,
            "net_salary": transaction_details.net_salary or 0,
            "total_benefits": transaction_details.total_benefits or 0,
            "total_deductions": transaction_details.total_deductions or 0,
            "total_advance_deductions": transaction_details.total_advance_deductions or 0,
            "paid_amount": transaction_details.paid_amount or 0,
            "balance_due": transaction_details.balance_due or 0,
        }

        # Fetch detailed deductions and benefits
        deductions = db_session.query(Deduction, DeductionType).join(DeductionType).filter(
            Deduction.payroll_transaction_id == transaction_id).all()
        benefits = db_session.query(Benefit, BenefitType).join(BenefitType).filter(
            Benefit.payroll_transaction_id == transaction_id).all()

        now = datetime.today()
        formatted_date = now.strftime('%Y-%m-%d')
        return render_template(
            "payroll/payrun_transaction_details.html",
            payroll_transaction=transaction_details,
            payroll_payments=transaction,
            company=company,
            modules=modules_data,
            role=role,
            totals=totals,
            deductions=deductions,
            benefits=benefits,
            transaction_id=transaction_id,
            payment_modes=payment_modes,
            funding_accounts=funding_accounts,
            now=formatted_date,
            base_currency=base_currency,
            base_currency_id=base_currency_id,
            base_currency_code=base_currency_code
        )

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error has been encountered: {str(e)}")
        logger.error(f"Error has been encountered: {str(e)}", "danger")
        return redirect(request.referrer)

    finally:
        db_session.close()  # Ensure the session is closed


@payroll_bp.route('/delete_payrun/<int:payrun_id>', methods=['DELETE'])
@login_required
def delete_payrun(payrun_id):
    db_session = Session()
    try:
        # 1) Fetch all transactions for this payroll period
        transactions = db_session.query(PayrollTransaction).filter(
            PayrollTransaction.payroll_period_id == payrun_id
        ).all()

        for txn in transactions:
            # Prepare ledger filters to clean all ledger entries at once (txn, payments, deductions, advance)
            ledger_filters = [{"source_type": "payroll_transaction", "source_id": txn.id}]

            for p in txn.payroll_payments or []:
                ledger_filters.append({"source_type": "payroll_payment", "source_id": p.id})

            for d in txn.deductions or []:
                ledger_filters.append({"source_type": "deduction", "source_id": d.id})
                for dp in d.deduction_payments or []:
                    ledger_filters.append({"source_type": "deduction_payment", "source_id": dp.id})

            if txn.advance_repayment:
                for ap in txn.advance_repayment:
                    ledger_filters.append({"source_type": "advance_repayment", "source_id": ap.id})

            # Clear ledger entries without repost
            repost_transaction(db_session=db_session, ledger_filters=ledger_filters, repost=False)

            # Delete all related child records
            for b in txn.benefits or []:
                db_session.delete(b)

            for d in txn.deductions or []:
                for dp in d.deduction_payments or []:
                    db_session.delete(dp)
                db_session.delete(d)

            for p in txn.payroll_payments or []:
                db_session.delete(p)

            for ap in txn.advance_repayment or []:
                # Update original advance payment balances
                advance_payment = ap.advance_payments
                if advance_payment:
                    advance_payment.remaining_balance += ap.payment_amount
                    advance_payment.amount_paid -= ap.payment_amount
                    advance_payment.update_repayment_status()
                db_session.delete(ap)

            db_session.delete(txn)

        # 2) Delete the PayrollPeriod itself
        payrun = db_session.query(PayrollPeriod).filter_by(id=payrun_id).first()
        if payrun:
            db_session.delete(payrun)

        db_session.commit()
        return jsonify({'success': True, 'message': 'Payrun deleted successfully.'}), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error deleting payrun {payrun_id}: {str(e)}", exc_info=True)
        return jsonify({'success': False, 'message': f'Error deleting payrun: {str(e)}'}), 500
    finally:
        db_session.close()


@payroll_bp.route('/cancel_payrun/<int:payrun_id>', methods=['POST'])
@login_required
def cancel_payrun(payrun_id):
    if current_user.role != 'Admin':
        return jsonify({
            "success": False,
            "message": "Only administrators can cancel payruns"
        }), 403

    db_session = Session()
    try:
        # Verify payrun exists and belongs to company
        payrun = db_session.query(PayrollPeriod).filter_by(
            id=payrun_id,
            app_id=current_user.app_id
        ).first()

        if not payrun:
            db_session.close()
            return jsonify({
                "success": False,
                "message": "Payrun not found"
            }), 404

        # Get all related transactions
        transactions = db_session.query(PayrollTransaction).filter_by(
            payroll_period_id=payrun_id
        ).all()

        # Check if any payments were made
        payments_exist = db_session.query(exists().where(
            PayrollPayment.payroll_transaction_id.in_(
                db_session.query(PayrollTransaction.id)
                .filter_by(payroll_period_id=payrun_id)
            ))).scalar()

        # Check if any deduction payments were made
        deduction_payments_exist = db_session.query(exists().where(
            DeductionPayment.deduction_id.in_(
                db_session.query(Deduction.id)
                .join(PayrollTransaction)
                .filter(PayrollTransaction.payroll_period_id == payrun_id)
            ))).scalar()

        if payments_exist or deduction_payments_exist:
            db_session.close()
            return jsonify({
                "success": False,
                "message": "Cannot cancel - payments already exist"
            }), 400

        # Prepare ledger filters for posted transactions
        ledger_filters = []
        for txn in transactions:
            if txn.is_posted_to_ledger:
                ledger_filters.append({
                    "source_type": "payroll_transaction",
                    "source_id": txn.id
                })

        # Delete ledger entries for posted transactions if any exist
        if ledger_filters:
            repost_transaction(
                db_session=db_session,
                ledger_filters=ledger_filters,
                post_function=None,
                repost=False
            )

        # Handle advance repayments before deleting other records
        # Get all advance repayments for this payrun
        advance_repayments = db_session.query(AdvanceRepayment).filter(
            AdvanceRepayment.payroll_id == payrun_id
        ).all()

        # Also get advance repayments linked to transactions in this payrun
        transaction_advance_repayments = db_session.query(AdvanceRepayment).filter(
            AdvanceRepayment.payroll_transaction_id.in_(
                db_session.query(PayrollTransaction.id)
                .filter_by(payroll_period_id=payrun_id)
            )
        ).all()

        all_advance_repayments = advance_repayments + transaction_advance_repayments

        # Update original advance payment balances
        for advance_repayment in all_advance_repayments:
            advance_payment = advance_repayment.advance_payments
            if advance_payment:
                advance_payment.remaining_balance += advance_repayment.payment_amount
                advance_payment.amount_paid -= advance_repayment.payment_amount
                advance_payment.update_repayment_status()

        # Delete all related records
        db_session.query(PayrollPayment).filter(
            PayrollPayment.payroll_transaction_id.in_(
                db_session.query(PayrollTransaction.id)
                .filter_by(payroll_period_id=payrun_id)
            )).delete(synchronize_session=False)

        for transaction in transactions:
            # Delete related benefits
            db_session.query(Benefit).filter(
                Benefit.payroll_transaction_id == transaction.id
            ).delete()

            # Delete related deductions
            deductions = db_session.query(Deduction).filter(
                Deduction.payroll_transaction_id == transaction.id
            ).all()
            for deduction in deductions:
                db_session.query(DeductionPayment).filter(
                    DeductionPayment.deduction_id == deduction.id
                ).delete()
                db_session.delete(deduction)

            # change status to cancelled
            transaction.payment_status = PaymentStatusEnum.CANCELLED

        # Delete advance payments
        db_session.query(AdvanceRepayment).filter(
            AdvanceRepayment.payroll_id == payrun_id
        ).delete(synchronize_session=False)

        # Mark as canceled instead of deleting
        payrun.payroll_status = PayrollPeriodStatusEnum.CANCELLED
        payrun.payment_status = PayRollStatusEnum.CANCELLED

        db_session.commit()

        return jsonify({
            "success": True,
            "message": f"Payrun {payrun.payroll_period_name} cancelled successfully"
        })

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error during payrun cancellation: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Database error: {str(e)}"
        }), 500
    except Exception as e:
        db_session.rollback()
        logger.error(f"Unexpected error during payrun cancellation: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Unexpected error: {str(e)}"
        }), 500
    finally:
        db_session.close()


from sqlalchemy.orm import joinedload


@payroll_bp.route('/delete_transaction/<int:transaction_id>', methods=['DELETE', 'GET'])
@login_required
def delete_payrun_transaction(transaction_id):
    db_session = Session()
    try:
        # 0) AuthZ: Admin only
        user_role = getattr(current_user, "role", None)
        if user_role != "Admin":
            return jsonify({"success": False, "message": "Only Admins can delete transactions."}), 403

        # 1) Load transaction + relations we need
        txn = (
            db_session.query(PayrollTransaction)
            .options(
                joinedload(PayrollTransaction.payroll_payments),
                joinedload(PayrollTransaction.benefits),
                joinedload(PayrollTransaction.deductions),
            )
            .filter(PayrollTransaction.id == transaction_id)
            .first()
        )

        if not txn:
            return jsonify({"success": False, "message": "Transaction not found."}), 404

        # Store advance repayment info before deletion
        # Store advance repayment info before deletion
        advance_repayments = txn.advance_repayment  # This is a list
        advance_repayment = advance_repayments[0] if advance_repayments else None
        repayment_amount = advance_repayment.payment_amount if advance_repayment else 0
        advance_payment = advance_repayment.advance_payments if advance_repayment else None

        # 2) If posted (txn or any payment), remove ledger entries FIRST using your repost_transaction
        payments_posted = [p for p in (txn.payroll_payments or []) if p.is_posted_to_ledger]
        needs_ledger_cleanup = bool(txn.is_posted_to_ledger or payments_posted)

        if needs_ledger_cleanup:
            ledger_filters = [{"source_type": "payroll_transaction", "source_id": txn.id}]

            # include posted payments
            for p in payments_posted:
                ledger_filters.append({"source_type": "payroll_payment", "source_id": p.id})

            # ADDED: Include advance repayment if it exists and is posted
            if advance_repayment and advance_repayment.is_posted_to_ledger:
                ledger_filters.append({"source_type": "advance_repayment", "source_id": advance_repayment.id})

            # Delete ledger entries only; do NOT repost
            repost_transaction(
                db_session=db_session,
                ledger_filters=ledger_filters,
                repost=False
            )

            # Reflect that ledger is now cleared
            txn.is_posted_to_ledger = False
            for p in payments_posted:
                p.is_posted_to_ledger = False
            # ADDED: Clear advance repayment posted status if exists
            if advance_repayment:
                advance_repayment.is_posted_to_ledger = False

        # 3) Modify Payroll period payment status
        # Now, check all transactions for the same payroll_period
        payroll_period = db_session.query(PayrollPeriod).filter_by(id=txn.payroll_period_id).first()

        # Print to verify the payroll period fetched
        if payroll_period:
            # Print to show we have found the payroll period
            # Check if all transactions for this payroll_period are fully paid
            all_paid = db_session.query(PayrollTransaction).filter_by(payroll_period_id=payroll_period.id).all()

            # Print to see all transactions associated with the payroll period

            # If all transactions have a zero balance_due, mark the period as PAID
            if all(p.balance_due == 0 for p in all_paid):
                payroll_period.payment_status = PayRollStatusEnum.PAID
            else:

                payroll_period.payment_status = PayRollStatusEnum.PARTIAL

        # 4) Delete related objects in safe order
        # 4a) Benefits
        for b in list(txn.benefits or []):
            db_session.delete(b)

        # 4b) Deductions + their payments
        # 4b) Deductions + their payments
        deductions = list(txn.deductions or [])
        for d in deductions:
            # If deduction was posted, cleanup ledger entries
            if d.is_posted_to_ledger:
                ledger_filters = [{"source_type": "deduction_payment", "source_id": d.id}]

                # Include deduction payments if they were posted
                dpay_posted = db_session.query(DeductionPayment).filter(
                    DeductionPayment.deduction_id == d.id,
                    DeductionPayment.is_posted_to_ledger == True
                ).all()
                for dp in dpay_posted:
                    ledger_filters.append({"source_type": "deduction_payment", "source_id": dp.id})
                    dp.is_posted_to_ledger = False

                repost_transaction(
                    db_session=db_session,
                    ledger_filters=ledger_filters,
                    repost=False
                )

                d.is_posted_to_ledger = False

            # Delete deduction payments
            dpay = db_session.query(DeductionPayment).filter(
                DeductionPayment.deduction_id == d.id
            ).all()
            for dp in dpay:
                db_session.delete(dp)

            # Delete deduction record itself
            db_session.delete(d)

        # 4c) Payroll payments
        for p in list(txn.payroll_payments or []):
            db_session.delete(p)

        # 4d) ADDED: Advance repayment handling
        if txn.advance_repayment:
            # If advance repayment was posted to ledger, we need to handle ledger cleanup
            if advance_repayment.is_posted_to_ledger:
                # Create ledger filters for advance repayment
                advance_ledger_filters = [{"source_type": "advance_repayment", "source_id": txn.advance_repayment.id}]
                repost_transaction(
                    db_session=db_session,
                    ledger_filters=advance_ledger_filters,
                    repost=False
                )

            # Delete the advance repayment record
            db_session.delete(advance_repayment)

            # Update the original advance payment's remaining balance
            if advance_payment:
                advance_payment.remaining_balance += repayment_amount
                advance_payment.amount_paid -= repayment_amount
                advance_payment.update_repayment_status()

        # 5) Finally, delete the transaction
        db_session.delete(txn)

        db_session.commit()
        return jsonify({"success": True, "message": "Transaction deleted successfully."}), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error deleting transaction {transaction_id}: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"Error deleting transaction: {str(e)}"}), 500
    finally:
        db_session.close()
