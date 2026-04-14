# app/routes/payroll/advances.py


import logging
import traceback
from datetime import datetime, timezone

from flask import jsonify, request, redirect, url_for, render_template, flash
from flask_login import login_required, current_user
from sqlalchemy import func, case, distinct, or_, and_
from sqlalchemy.orm import joinedload

from ai import resolve_exchange_rate_for_transaction
from db import Session
from models import Company, Module, Currency, Department, Employee, \
    PayrollPeriod, Deduction, BenefitType, \
    DeductionType, PaymentMode, DeductionPayment, AdvancePayment, User, AdvanceRepayment, ChartOfAccounts, \
    Category, Journal, JournalEntry
from services.post_to_ledger import create_advance_payment_journal_entries
from services.post_to_ledger_reversal import repost_transaction, delete_journal_entries_by_source
from utils import create_notification
from utils_and_helpers.exchange_rates import get_or_create_exchange_rate_for_transaction

# Import the same blueprint
from . import payroll_bp

logger = logging.getLogger()


@payroll_bp.route('/create_advance_payment', methods=['GET', 'POST'])
@login_required
def create_advance_payment():
    db_session = Session()

    if request.method == 'GET':
        # Fetch necessary data for the form (e.g., employees, currencies)
        app_id = current_user.app_id
        role = db_session.query(User.role).filter_by(id=current_user.id).scalar()
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

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

        employees = db_session.query(Employee).filter_by(app_id=app_id).all()  # Fetch all employees
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()  # Fetch all currencies
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id
        # Render the template with the data
        return render_template(
            '/payroll/advance_payment.html',  # Template file name
            employees=employees,  # Pass employees to the template
            currencies=currencies,  # Pass currencies to the template
            base_currency=base_currency,
            base_currency_id=base_currency_id,
            base_currency_code=base_currency_code,
            company=company,
            role=role,
            modules=modules_data,
            funding_accounts=grouped_accounts,
            module_name="Payroll"
        )


    elif request.method == 'POST':
        try:
            data = request.get_json()
            # Start transaction
            db_session.begin_nested()

            # Check if the employee has an existing advance with a remaining balance
            existing_advance = db_session.query(AdvancePayment).filter(
                AdvancePayment.employee_id == data['employee_id'],
                AdvancePayment.remaining_balance > 0
            ).first()

            if existing_advance:
                return jsonify({
                    'success': False,
                    'message': 'Cannot create a new advance payment. The employee already has an outstanding balance.'
                }), 400

            advance_date = datetime.strptime(data['payment_date'], '%Y-%m-%d')

            # Handle exchange rate for the advance payment
            exchange_rate_id = None
            exchange_rate_value = None
            base_currency_id = int(data.get('base_currency_id'))
            currency_id = int(data.get('currency_id'))
            payment_date = datetime.strptime(data['payment_date'], '%Y-%m-%d').date()

            if base_currency_id and currency_id != base_currency_id:
                # Validate exchange rate
                exchange_rate = data.get('exchange_rate')
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
                    from_currency_id=currency_id,
                    to_currency_id=base_currency_id,
                    rate_value=exchange_rate_value,
                    rate_date=payment_date,
                    app_id=current_user.app_id,
                    created_by=current_user.id,
                    source_type='advance_payment',
                    source_id=None,  # Will update after advance payment is created
                    currency_exchange_transaction_id=None
                )
                exchange_rate_id = rate_id

            # Create a new AdvancePayment record
            advance = AdvancePayment(
                employee_id=data['employee_id'],
                advance_amount=data['advance_amount'],
                remaining_balance=data['advance_amount'],
                deduction_per_payroll=data['deduction_per_payroll'],
                payment_date=advance_date,
                payment_method=data.get('payment_method') or None,
                payment_account=data.get('accountSubCategory'),
                currency_id=currency_id,
                notes=data.get('notes', ''),
                app_id=current_user.app_id,
                exchange_rate_id=exchange_rate_id,
                is_posted_to_ledger=True,
                created_by=current_user.id,
                version=1
            )
            advance.update_repayment_status()

            db_session.add(advance)
            db_session.flush()  # Get the advance ID

            # 4. Update exchange rate with advance ID if needed
            if exchange_rate_id and 'rate_obj' in locals():
                rate_obj.source_id = advance.id
                db_session.add(rate_obj)

            # ✅ NEW: Create posted journal entries immediately
            journal = create_advance_payment_journal_entries(
                db_session=db_session,
                advance_payment=advance,
                app_id=current_user.app_id,
                current_user_id=current_user.id,
                status="Posted",
                exchange_rate_id=exchange_rate_id
            )

            if not journal:
                raise Exception("Failed to create journal entries")

            db_session.commit()

            return jsonify({
                'success': True,
                'message': 'Advance payment created successfully!'
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f"Error occurred: {str(e)}")
            return jsonify({
                'success': False,
                'message': str(e)
            }), 500
        finally:
            db_session.close()


@payroll_bp.route('/advance_payments', methods=['GET'])
@login_required
def get_advance_payments():
    db_session = Session()
    try:
        app_id = current_user.app_id
        role = db_session.query(User.role).filter_by(id=current_user.id).scalar()
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch all advance payments for the current company
        advance_payments = db_session.query(AdvancePayment).filter_by(app_id=app_id).all()

        return render_template(
            "/payroll/advance_payments.html",
            company=company,
            role=role,
            modules=modules_data,
            module_name="Payroll",
            advance_payments=advance_payments  # Pass serialized data to the template
        )
    except Exception as e:
        db_session.rollback()
        flash(f"An error has occurred: {e}", "error")
        return render_template(
            "/payroll/advance_payments.html",
            company=None,
            role=None,
            modules=[],
            module_name="Payroll",
            advance_payments=[]  # Return empty list in case of error
        )
    finally:
        db_session.close()


@payroll_bp.route('/edit_advance_payment/<int:advance_payment_id>', methods=['GET'])
@login_required
def edit_advance_payment(advance_payment_id):
    db_session = Session()

    try:
        # Fetch the advance payment record
        advance_payment = db_session.query(AdvancePayment).filter_by(id=advance_payment_id).first()
        if not advance_payment:
            return "Advance payment not found.", 404

        # Fetch necessary data for the form (e.g., employees, currencies)
        app_id = current_user.app_id
        role = db_session.query(User.role).filter_by(id=current_user.id).scalar()
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        employees = db_session.query(Employee).filter_by(app_id=app_id).all()  # Fetch all employees

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

        employees = db_session.query(Employee).filter_by(app_id=app_id).all()  # Fetch all employees
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()  # Fetch all currencies
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id
        # Render the edit template with the data
        return render_template(
            '/payroll/edit_advance_payment.html',  # Template file name
            advance_payment=advance_payment,  # Pass the advance payment data to the template
            employees=employees,  # Pass employees to the template
            currencies=currencies,  # Pass currencies to the template
            base_currency_code=base_currency_code,
            base_currency_id=base_currency_id,
            company=company,
            funding_accounts=grouped_accounts,
            base_currency=base_currency,
            role=role,
            modules=modules_data,
            module_name="Payroll"
        )

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}\n{traceback.format_exc()}")  # Log the error
        return "An error occurred while fetching the advance payment.", 500
    finally:
        db_session.close()


@payroll_bp.route('/delete_advance_payment/<int:payment_id>', methods=['DELETE'])
@login_required
def delete_advance_payment(payment_id):
    db_session = Session()

    try:

        # Fetch the advance payment record by ID
        payment = db_session.query(AdvancePayment).filter_by(id=payment_id).first()

        if not payment:
            return jsonify({'success': False, 'message': 'Advance payment not found!'}), 404

        # Check if there are related records in the AdvanceRepayment table
        related_repayment = db_session.query(AdvanceRepayment).filter_by(advance_payment_id=payment_id).first()
        if related_repayment:
            return jsonify({'success': False,
                            'message': 'Cannot delete advance payment because there is a related advance repayment.'}), 400

        # Call repost_transaction with repost=False to just delete the ledger entries
        repost_transaction(
            db_session=db_session,
            ledger_filters=[{"source_type": "advance_payment", "source_id": payment_id}],
            post_function=None,  # No reposting needed for delete
            repost=False  # This will only delete, not repost
        )
        # Delete the advance payment
        db_session.delete(payment)
        db_session.commit()

        return jsonify({'success': True, 'message': 'Advance payment deleted successfully!'})

    except Exception as e:
        logger.error(f'error is ss {e}')
        db_session.rollback()
        return jsonify({'success': False, 'message': f'Error occurred: {str(e)}'}), 500

    finally:
        db_session.close()


@payroll_bp.route('/advance_repayments', methods=['GET'])
@login_required
def get_advance_repayments():
    db_session = Session()
    try:
        app_id = current_user.app_id
        role = db_session.query(User.role).filter_by(id=current_user.id).scalar()
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch all advance payments for the current company
        advance_repayments = db_session.query(AdvanceRepayment).filter_by(app_id=app_id).all()

        return render_template(
            "/payroll/advance_repayments.html",
            company=company,
            role=role,
            modules=modules_data,
            module_name="Payroll",
            advance_repayments=advance_repayments  # Pass serialized data to the template
        )
    except Exception as e:
        db_session.rollback()
        flash(f"An error has occurred: {e}", "error")
        return render_template(
            "/payroll/advance_payments.html",
            company=None,
            role=None,
            modules=[],
            module_name="Payroll",
            advance_payments=[]  # Return empty list in case of error
        )
    finally:
        db_session.close()


@payroll_bp.route('/update_advance_payment/<int:advance_payment_id>', methods=['POST'])
@login_required
def update_advance_payment(advance_payment_id):
    db_session = Session()

    try:

        # Check if request is JSON (from AJAX) or form data
        if request.is_json:
            data = request.get_json()
        else:
            data = request.form

        client_version = data.get('version')

        advance_payment = db_session.query(AdvancePayment).filter_by(id=advance_payment_id).first()
        if not advance_payment:
            return jsonify({'success': False, 'message': 'Advance payment not found.'}), 404

        # Version check - optimistic locking
        if client_version is not None and int(client_version) != advance_payment.version:
            return jsonify({
                'success': False,
                'message': 'This record has been modified by another user. Please refresh and try again.',
                'code': 'VERSION_CONFLICT'
            }), 409

        advance_date = datetime.strptime(data['payment_date'], '%Y-%m-%d')
        advance_payment.payment_account = data.get('accountSubCategory') or advance_payment.payment_account

        # Update basic fields
        advance_payment.employee_id = data['employee_id']
        advance_payment.advance_amount = float(data['advance_amount'])
        advance_payment.remaining_balance = float(data['advance_amount'])
        advance_payment.deduction_per_payroll = float(data['deduction_per_payroll'])
        advance_payment.payment_date = advance_date
        advance_payment.payment_method = data.get('payment_method') or None
        advance_payment.notes = data.get('notes', '')

        # ✅ Mark as Posted
        advance_payment.is_posted_to_ledger = True

        advance_payment.update_repayment_status()

        # Handle exchange rate
        exchange_rate_id = None
        currency_id = advance_payment.currency_id
        base_currency_id = data.get('base_currency_id')
        payment_date = advance_date.date()

        if base_currency_id and str(currency_id) != str(base_currency_id):
            exchange_rate = data.get('exchange_rate')
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
                from_currency_id=currency_id,
                to_currency_id=base_currency_id,
                rate_value=exchange_rate_value,
                rate_date=payment_date,
                app_id=current_user.app_id,
                created_by=current_user.id,
                source_type='advance_payment',
                source_id=advance_payment_id,
                currency_exchange_transaction_id=None
            )
            exchange_rate_id = rate_id

        advance_payment.exchange_rate_id = exchange_rate_id

        # ✅ Find and update any existing journal entries for this advance payment
        # Since you don't have journal_id in AdvancePayment, we need to find journals by source
        existing_journals = db_session.query(Journal).filter(
            Journal.app_id == current_user.app_id,
            Journal.entries.any(
                and_(
                    JournalEntry.source_type == "advance_payment",
                    JournalEntry.source_id == advance_payment_id
                )
            )
        ).options(joinedload(Journal.entries)).all()


        # ✅ DELETE existing journal entries for this advance payment
        delete_success, delete_message = delete_journal_entries_by_source(
            db_session=db_session,
            source_type='advance_payment',
            source_id=advance_payment_id,
            app_id=current_user.app_id
        )

        if not delete_success:
            return jsonify({
                'success': False,
                'message': f'Failed to delete existing journal entries: {delete_message}'
            }), 400

        # ✅ CREATE fresh journal entries with updated values
        journal = create_advance_payment_journal_entries(
            db_session=db_session,
            advance_payment=advance_payment,
            app_id=current_user.app_id,
            current_user_id=current_user.id,
            status="Posted",
            exchange_rate_id=exchange_rate_id
        )

        if not journal:
            return jsonify({
                'success': False,
                'message': 'Failed to create journal entries for advance payment'
            }), 500

        advance_payment.version += 1
        db_session.commit()

        if request.is_json:
            return jsonify({
                'success': True,
                'message': 'Advance payment updated successfully!'
            })
        else:
            flash('Advance payment updated successfully!', 'success')
            return redirect('/payroll/advance_payments')

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error occurred while updating advance payment: {str(e)}")
        if request.is_json:
            return jsonify({
                'success': False,
                'message': str(e)
            }), 500
        else:
            flash('An error occurred while updating the advance payment.', 'error')
            return "An error occurred while updating the advance payment.", 500
    finally:
        db_session.close()


@payroll_bp.route('/payroll/check_advance_payment_posted/<int:repayment_id>', methods=['GET'])
@login_required
def check_advance_payment_posted(repayment_id):
    db_session = Session()
    try:
        # Fetch the advance repayment record
        advance_repayment = db_session.query(AdvanceRepayment).filter_by(id=repayment_id).first()
        if not advance_repayment:
            return jsonify({'success': False, 'message': 'Advance repayment not found'}), 404

        # Fetch the associated advance payment
        advance_payment = db_session.query(AdvancePayment).filter_by(id=advance_repayment.advance_payment_id).first()
        if not advance_payment:
            return jsonify({'success': False, 'message': 'Associated advance payment not found'}), 404

        # Check if the advance payment is posted to the ledger
        if advance_payment.is_posted_to_ledger != 1:
            return jsonify({
                'success': False,
                'message': 'The associated advance payment is not posted to the ledger. Please post it first.',
                'redirect_url': '/payroll/advance_payments'  # URL to redirect the user
            }), 200

        return jsonify({'success': True}), 200

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()
