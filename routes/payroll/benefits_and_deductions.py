# app/routes/payroll/benefits_and_deductions.py


import logging
import traceback

from flask import jsonify, request, redirect, url_for, render_template, flash
from flask_login import login_required, current_user
from sqlalchemy import func, case, distinct, or_
from sqlalchemy.orm import joinedload

from db import Session
from models import Company, Module, Currency, Department, Employee, \
    PayrollPeriod, Deduction, BenefitType, \
    DeductionType, PaymentMode, DeductionPayment, ChartOfAccounts, Category, UserPreference, PaymentStatusEnum, Journal, \
    JournalEntry, ExchangeRate
from services.post_to_ledger_reversal import repost_transaction

# Import the same blueprint
from . import payroll_bp

logger = logging.getLogger()


@payroll_bp.route('/add_benefit', methods=['POST'])
def add_benefit():
    db_session = Session()
    try:
        # Get the JSON data from the request body
        data = request.get_json()

        # Extract data from the JSON payload
        name = data.get('name')
        description = data.get('description')
        benefit_type = data.get('benefitType')
        rate = data.get('rate', None)
        currency_id = int(data.get('currency')) if data.get('currency') else None
        fixed_amount = data.get('fixed_amount', None)
        app_id = data.get('app_id', None)

        # Validate the benefit type and create the appropriate BenefitType object
        if benefit_type == 'rate':
            if rate is None:
                return jsonify({"success": False, "message": "Rate is required for Benefit Type 'rate'"}), 400
            # Create BenefitType for 'rate'
            benefit = BenefitType(name=name, description=description, rate=rate, is_rate=True, currency=None,
                                  app_id=app_id)
        elif benefit_type == 'fixed_amount':
            if fixed_amount is None or currency_id is None:
                return jsonify({"success": False,
                                "message": "Fixed Amount and Currency are required for Benefit Type 'fixed_amount'"}), 400

            # Fetch the Currency object using the ID
            currency = db_session.query(Currency).filter(Currency.id == currency_id).first()
            if not currency:
                return jsonify({"success": False, "message": f"Currency with ID {currency_id} not found"}), 400

            # Create BenefitType for 'fixed_amount'
            benefit = BenefitType(
                name=name,
                description=description,
                fixed_amount=fixed_amount,
                is_rate=False,
                currency=currency,  # Pass the actual Currency model instance
                app_id=app_id
            )
        else:
            return jsonify({"success": False, "message": "Invalid benefit type selected"}), 400

        # Save the benefit to the database
        db_session.add(benefit)
        db_session.commit()

        # Return the new benefit's ID in the response
        return jsonify({
            "success": True,
            "message": "Benefit saved successfully",
            "benefit_id": benefit.id  # Returning the newly created benefit's ID
        })
    except Exception as e:
        db_session.rollback()
        logger.error(f'The error is {e}')
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_session.close()


@payroll_bp.route('/add_deduction', methods=['POST'])
def add_deduction():
    db_session = Session()
    try:
        # Get the JSON data from the request body
        data = request.get_json()
        print(f'data is {data}')
        if not data:
            return jsonify({"success": False, "message": "No data provided"}), 400

        # Get form data
        name = data.get('name')
        description = data.get('description') or None
        deduction_type = data.get('deductionType')  # This comes from the dropdown now
        app_id = data.get('app_id', None)

        # Validate required fields
        if not name:
            return jsonify({"success": False, "message": "Name is required"}), 400
        if not deduction_type:
            return jsonify({"success": False, "message": "Deduction type is required"}), 400

        # Initialize the deduction variable
        deduction = None

        if deduction_type == "rate":
            rate = data.get('rate', None)
            if rate is None:  # Validation to ensure rate is provided
                return jsonify({"success": False, "message": "Rate is required for the selected deduction type"}), 400
            deduction = DeductionType(name=name, description=description, rate=rate, is_rate=True, currency=None,
                                      app_id=app_id)

        elif deduction_type == "fixed_amount":
            currency_id = int(data.get('currency')) if data.get('currency') else None  # Convert currency to int
            fixed_amount_str = data.get('fixed_amount', None)
            if fixed_amount_str is None or currency_id is None:  # Validation to ensure fixed_amount and currency are provided
                return jsonify({"success": False,
                                "message": "Fixed amount and Currency are required for the selected deduction type"}), 400

            # Convert fixed_amount to a numeric type
            try:
                fixed_amount = float(fixed_amount_str)  # Convert to float or Decimal as needed
            except ValueError:
                return jsonify({"success": False, "message": "Fixed amount must be a valid number"}), 400

            # Fetch the Currency object using the ID
            currency = db_session.query(Currency).filter(Currency.id == currency_id).first()
            if not currency:
                return jsonify({"success": False, "message": f"Currency with ID {currency_id} not found"}), 400

            # Create DeductionType for 'fixed_amount'
            deduction = DeductionType(
                name=name,
                description=description,
                fixed_amount=fixed_amount,
                is_rate=False,
                currency=currency,  # Pass the actual Currency model instance
                app_id=app_id
            )

        else:
            return jsonify({"success": False, "message": "Invalid deduction type selected"}), 400

        # Save the deduction type to the database
        db_session.add(deduction)
        db_session.commit()

        # Return the new deduction's ID in the response
        return jsonify({
            "success": True,
            "message": "Deduction saved successfully",
            "deduction_id": deduction.id  # Returning the newly created deduction's ID
        })

    except Exception as e:
        db_session.rollback()
        print(f'Error occurred: {str(e)}')  # Log the error for debugging
        return jsonify({"success": False, "message": "An error occurred while saving the deduction."}), 500

    finally:
        db_session.close()


@payroll_bp.route('/get_deduction_details/<int:deduction_id>', methods=['GET'])
@login_required
def get_deduction_details(deduction_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        deduction = db_session.query(DeductionType).filter_by(id=deduction_id, app_id=app_id).first()

        if deduction:
            return jsonify({
                "success": True,
                "deduction": {
                    "deduction_type": deduction.name,  # Assuming the deduction has a name attribute
                    "rate": deduction.rate if deduction.rate is not None else None,
                    "fixed_amount": deduction.fixed_amount if deduction.fixed_amount is not None else None
                }
            })
        else:
            return jsonify({
                "success": False,
                "message": "Deduction not found"
            }), 404
    finally:
        db_session.close()  # Ensures session is closed even if an error occurs


# Endpoint to get benefit details
@payroll_bp.route('/get_benefit_details/<int:benefit_id>', methods=['GET'])
@login_required
def get_benefit_details(benefit_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        benefit = db_session.query(BenefitType).filter_by(id=benefit_id, app_id=app_id).first()

        if benefit:
            return jsonify({
                "success": True,
                "benefit": {
                    "benefit_type": benefit.name,
                    "rate": benefit.rate if benefit.rate is not None else None,
                    "fixed_amount": benefit.fixed_amount if benefit.fixed_amount is not None else None
                }
            })
        else:
            return jsonify({
                "success": False,
                "message": "Benefit not found"
            }), 404
    finally:
        db_session.close()  # Ensures session is closed even if an error occurs


@payroll_bp.route('/employee_deductions', methods=['GET', 'POST'])
@login_required
def get_employee_deductions():
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Get filter parameters from request
        currency_filter = request.args.get("currency")
        filter_by = request.args.get("filter_by", "date_generated")  # Default to date_generated
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        filter_applied = bool(start_date or end_date or currency_filter)
        # Base query
        deductions_query = (
            db_session.query(
                PayrollPeriod,
                func.sum(Deduction.amount).label('total_deduction_amount'),
                func.sum(Deduction.paid_amount).label('total_paid_amount'),
                func.sum(Deduction.balance_due).label('total_balance'),
                func.count(distinct(Deduction.employee_id)).label('employee_count'),
                Deduction.currency_id
            )
            .join(PayrollPeriod, Deduction.payroll_period_id == PayrollPeriod.id)
            .filter(PayrollPeriod.app_id == app_id)  # Ensure data belongs to the current company
        )

        # Apply currency filter if selected
        if currency_filter:
            deductions_query = deductions_query.filter(Deduction.currency_id == currency_filter)

        else:
            main_currency = db_session.query(Currency).filter_by(currency_index=1, app_id=app_id).first()
            currency_filter = main_currency.id
            deductions_query = deductions_query.filter(Deduction.currency_id == currency_filter)

        # Apply date filtering
        if filter_by == "date_generated":
            if start_date:
                deductions_query = deductions_query.filter(PayrollPeriod.creation_date >= start_date)
            if end_date:
                deductions_query = deductions_query.filter(PayrollPeriod.creation_date <= end_date)

        elif filter_by == "start_end_date":
            if start_date:
                deductions_query = deductions_query.filter(PayrollPeriod.start_date >= start_date)
            if end_date:
                deductions_query = deductions_query.filter(PayrollPeriod.end_date <= end_date)

        # Group by and execute the query
        deductions_data = deductions_query.group_by(PayrollPeriod.id, Deduction.currency_id).all()

        return render_template(
            "payroll/employee_deductions.html",
            company=company,
            modules=modules_data,
            role=role,
            deductions=deductions_data,
            filter_applied=filter_applied,
            module_name="Payroll"
        )
    finally:
        db_session.close()  # Ensure the session is closed


@payroll_bp.route('/deduction_summary/<int:payroll_period_id>', methods=['GET'])
@login_required
def get_deduction_summary(payroll_period_id):
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
            preference_type="deduction_make_full_payment", user_id=current_user.id).scalar()

        # Get funding accounts (Cash or Bank accounts) with categories
        funding_accounts = (
            db_session.query(
                ChartOfAccounts,
                Category.category.label('category_name')
            )
            .join(Category, ChartOfAccounts.category_fk == Category.id)
            .filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_system_account.is_(False),
                or_(ChartOfAccounts.is_cash == True, ChartOfAccounts.is_bank == True)
            )
            .order_by(
                func.lower(Category.category),  # Case-insensitive category sorting
                func.lower(ChartOfAccounts.sub_category)  # Case-insensitive sub-category sorting
            )
            .all()
        )

        # Group accounts by category
        grouped_accounts = {}
        for account, category_name in funding_accounts:
            if category_name not in grouped_accounts:
                grouped_accounts[category_name] = []
            grouped_accounts[category_name].append(account)

        # Get filter parameters from request
        department_filter = request.args.get("department")
        currency_filter = request.args.get("currency")
        filter_applied = bool(department_filter or currency_filter)
        # Fetch payroll period name
        payroll_period = db_session.query(PayrollPeriod).filter_by(id=payroll_period_id, app_id=app_id).first()
        if not payroll_period:
            return "Payroll period not found", 404

        # Subquery for total deduction payments
        deduction_payments_subquery = (
            db_session.query(
                DeductionPayment.deduction_id,
                func.sum(DeductionPayment.amount).label('total_paid_amount'),
                func.sum(case((DeductionPayment.is_posted_to_ledger == False, 1), else_=0)).label('unposted_count')
            )
            .group_by(DeductionPayment.deduction_id)
            .subquery()
        )

        # Base query for deductions
        # Base query for deductions
        query = (
            db_session.query(
                Department.department_name,
                (Employee.first_name + ' ' + Employee.last_name).label('employee_name'),
                DeductionType.name.label('deduction_type'),
                Currency.user_currency,
                Currency.id.label('currency_id'),  # ✅ ADD THIS
                Deduction.id.label('deduction_id'),
                Deduction.amount.label('deduction_amount'),
                func.coalesce(deduction_payments_subquery.c.total_paid_amount, 0).label('total_paid_amount'),
                (Deduction.amount - func.coalesce(deduction_payments_subquery.c.total_paid_amount, 0)).label('balance_due'),
                Deduction.is_posted_to_ledger,
                Deduction.payment_status,
                func.coalesce(deduction_payments_subquery.c.unposted_count, 0).label('unposted_total'),
                Deduction.payable_account_id,
                Deduction.exchange_rate_id,  # ✅ ADD THIS
                ExchangeRate.rate.label('exchange_rate')  # ✅ ADD THIS
            )
            .join(Employee, Deduction.employee_id == Employee.id)
            .join(Department, Employee.department_id == Department.id)
            .join(DeductionType, Deduction.deduction_type_id == DeductionType.id)
            .join(Currency, Deduction.currency_id == Currency.id)
            .outerjoin(deduction_payments_subquery, deduction_payments_subquery.c.deduction_id == Deduction.id)
            .outerjoin(ExchangeRate, Deduction.exchange_rate_id == ExchangeRate.id)  # ✅ ADD THIS JOIN
            .filter(Deduction.app_id == app_id)
            .filter(Deduction.payroll_period_id == payroll_period_id)
        )

        # Apply filters
        if department_filter:
            query = query.filter(Department.id == department_filter)
        if currency_filter:
            query = query.filter(Currency.id == currency_filter)

        # Count of deductions
        deduction_count = db_session.query(func.count(Deduction.id)).filter(
            Deduction.app_id == app_id,
            Deduction.payroll_period_id == payroll_period_id
        ).scalar()

        # Execute the query
        deduction_summary = query.all()

        # Calculate totals with None handling
        totals = {
            "deduction_amount": sum(deduction.deduction_amount or 0 for deduction in deduction_summary),
            "total_paid_amount": sum(deduction.total_paid_amount or 0 for deduction in deduction_summary),
            "balance_due": sum(deduction.balance_due or 0 for deduction in deduction_summary),
            "unposted_total": sum(deduction.unposted_total or 0 for deduction in deduction_summary),
        }

        return render_template(
            "payroll/deduction_summary.html",
            deduction_summary=deduction_summary,
            totals=totals,
            departments=db_session.query(Department).filter_by(app_id=app_id).all(),
            currencies=currencies,
            base_currency=base_currency,
            base_currency_code=base_currency_code,
            base_currency_id=base_currency_id,
            payroll_period_name=payroll_period.payroll_period_name,
            company=company,
            modules=modules_data,
            role=role,
            payroll_period_id=payroll_period_id,
            payment_modes=payment_modes,
            deduction_count=deduction_count,
            funding_accounts=grouped_accounts,
            filter_applied=filter_applied,
            do_not_show_again=do_not_show_again,
            module_name="Payroll"
        )

    except Exception as e:
        db_session.rollback()
        logger.error(f'An error occured {e}\n{traceback.format_exc()}')
        flash(f'An error occured {e}')
        return redirect(request.referrer)

    finally:
        db_session.close()  # Ensure the session is closed


@payroll_bp.route("/deduction_details/<int:transaction_id>")
@login_required
def deduction_details(transaction_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id


        # Get funding accounts (Cash or Bank accounts) with categories
        funding_accounts = (
            db_session.query(
                ChartOfAccounts,
                Category.category.label('category_name')
            )
            .join(Category, ChartOfAccounts.category_fk == Category.id)
            .filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_system_account == False,
                or_(ChartOfAccounts.is_cash == True, ChartOfAccounts.is_bank == True)
            )
            .order_by(
                func.lower(Category.category),  # Case-insensitive category sorting
                func.lower(ChartOfAccounts.sub_category)  # Case-insensitive sub-category sorting
            )
            .all()
        )

        # Group accounts by category
        grouped_accounts = {}
        for account, category_name in funding_accounts:
            if category_name not in grouped_accounts:
                grouped_accounts[category_name] = []
            grouped_accounts[category_name].append(account)

        # Fetch deduction transaction and ensure related data is loaded
        deduction_transaction = db_session.query(Deduction).options(
            joinedload(Deduction.deduction_payments),  # Eager load deduction payments
            joinedload(Deduction.deduction_type),  # Eager load deduction type
            joinedload(Deduction.employees),  # Eager load employee details
            joinedload(Deduction.payroll_periods),  # Eager load payroll period
            joinedload(Deduction.currency),  # Eager load currency
            joinedload(Deduction.payroll_transactions)
        ).filter_by(id=transaction_id).first()

        if not deduction_transaction:
            return redirect(url_for('deduction_transaction_not_found'))

        # Calculate totals
        totals = {
            "total_deductions": deduction_transaction.amount or 0,
            "paid_amount": deduction_transaction.paid_amount or 0,
            "balance_due": deduction_transaction.balance_due or 0,
        }

        # Fetch detailed deductions
        deductions = db_session.query(Deduction, DeductionType).join(DeductionType).filter(
            Deduction.id == transaction_id).all()
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()

        return render_template(
            "payroll/deduction_details.html",
            deduction_transaction=deduction_transaction,
            company=company,
            currencies=currencies,
            base_currency=base_currency,
            base_currency_code=base_currency_code,
            base_currency_id=base_currency_id,
            modules=modules_data,
            role=role,
            totals=totals,
            deductions=deductions,
            funding_accounts=grouped_accounts,
            payment_modes=payment_modes,
            transaction_id=transaction_id
        )
    finally:
        db_session.close()  # Ensure the session is closed


@payroll_bp.route('/delete_deduction_transaction/<int:deduction_id>', methods=['DELETE'])
@login_required
def delete_deduction_transaction(deduction_id):
    db_session = Session()
    try:
        # 0) AuthZ: Admin only
        user_role = getattr(current_user, "role", None)
        if user_role != "Admin":
            return jsonify({"success": False, "message": "Only Admins can delete deduction transactions."}), 403

        # 1) Load deduction transaction + relations we need
        deduction = (
            db_session.query(Deduction)
            .options(
                joinedload(Deduction.deduction_payments),
                joinedload(Deduction.payroll_periods),
                joinedload(Deduction.employees),
                joinedload(Deduction.payroll_transactions)  # Add this to load the related payroll transaction
            )
            .filter(Deduction.id == deduction_id)
            .first()
        )

        if not deduction:
            return jsonify({"success": False, "message": "Deduction transaction not found."}), 404

        # 2) If posted (deduction or any payment), remove ledger entries FIRST
        payments_posted = [p for p in (deduction.deduction_payments or []) if p.is_posted_to_ledger]
        needs_ledger_cleanup = bool(deduction.is_posted_to_ledger or payments_posted)

        if needs_ledger_cleanup:
            ledger_filters = [{"source_type": "deduction_payment", "source_id": deduction.id}]

            # Include posted payments
            for p in payments_posted:
                ledger_filters.append({"source_type": "deduction_payment", "source_id": p.id})

            # Delete ledger entries only; do NOT repost
            repost_transaction(
                db_session=db_session,
                ledger_filters=ledger_filters,
                repost=False
            )

            # Reflect that ledger is now cleared
            deduction.is_posted_to_ledger = False
            for p in payments_posted:
                p.is_posted_to_ledger = False

        # 3) Update related deduction transaction if this deduction is linked to one
        payroll_transaction = deduction.payroll_transactions
        if payroll_transaction:
            # Remove this deduction amount from payroll transaction totals
            payroll_transaction.balance_due += deduction.amount
            payroll_transaction.net_salary += deduction.amount

            # Recalculate payroll transaction status if needed
            if payroll_transaction.balance_due > 0:
                payroll_transaction.payment_status = (
                    PaymentStatusEnum.PAID if payroll_transaction.balance_due == 0
                    else PaymentStatusEnum.PARTLY_PAID
                )
            if payroll_transaction.is_posted_to_ledger:
                # Find journal entries for this payroll transaction and update amounts
                journal_entries = db_session.query(JournalEntry).filter_by(
                    source_type="payroll_transaction",
                    source_id=payroll_transaction.id
                ).all()

                if journal_entries:
                    for entry in journal_entries:
                        # Update the entry amount by adding the deduction amount
                        # Note: This assumes the entry is for the net salary amount
                        entry.amount += deduction.amount

                    # Also update the journal totals if the journal exists
                    for entry in journal_entries:
                        if entry.journal_id:
                            journal = db_session.query(Journal).get(entry.journal_id)
                            if journal:
                                journal.update_totals()

        # 4) Delete deduction payments
        for payment in list(deduction.deduction_payments or []):
            db_session.delete(payment)

        # 5) Finally, delete the deduction transaction itself
        db_session.delete(deduction)

        db_session.commit()

        return jsonify({
            "success": True,
            "message": "Deduction transaction deleted successfully."
        }), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error deleting deduction transaction {deduction_id}: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "message": f"Error deleting deduction transaction: {str(e)}"
        }), 500
    finally:
        db_session.close()
