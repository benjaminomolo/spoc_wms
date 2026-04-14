import logging
import traceback
from datetime import datetime

from flask import Blueprint, jsonify, render_template, flash, redirect, request, url_for
from flask_login import login_required, current_user
from sqlalchemy import func

from ai import get_base_currency, get_or_create_exchange_rate_id
from db import Session
from models import Company, Module, Currency, Project, ChartOfAccounts, PaymentMode, Category, Vendor
from utils import get_cash_balances, create_notification

general_ledger_routes = Blueprint('general_ledger_routes', __name__)
logger = logging.getLogger(__name__)


@general_ledger_routes.route('/general_ledger/dashboard', methods=["GET"])
@login_required
def general_ledger_dashboard():
    db_session = Session()
    try:
        app_id = current_user.app_id
        role = current_user.role
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        module_name = "General Ledger"
        api_key = company.api_key

        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency_code = base_currency_info["base_currency"]

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)

        projects = db_session.query(Project).filter_by(app_id=app_id).all()
        categories = db_session.query(Category).filter_by(app_id=app_id).all()
        subcategories = db_session.query(ChartOfAccounts).filter_by(app_id=app_id).all()
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
        vendors = db_session.query(Vendor).filter_by(app_id=app_id).all()

        return render_template(
            '/general-ledger/dashboard.html',
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name,
            api_key=api_key,
            base_currency=base_currency,
            base_currency_code=base_currency_code,
            base_currency_id=base_currency_id,
            currencies=currencies,
            projects=projects,
            categories=categories,
            subcategories=subcategories,
            payment_modes=payment_modes,
            vendors=vendors
        )
    except Exception as e:
        db_session.rollback()
        error_msg = f'An error has occurred while loading: {str(e)}\n{traceback.format_exc()}'
        logger.error(error_msg)
        flash(f'{error_msg}', "error")
        return redirect(request.referrer)

    finally:
        db_session.close()


@general_ledger_routes.route('/general_ledger/journal_adjustment', methods=["POST", "GET"])
@login_required
def journal_adjustment():
    try:
        with Session() as db_session:

            app_id = current_user.app_id

            # Fetch all required data
            transaction_categories = db_session.query(Category).filter_by(app_id=app_id).all()
            payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).order_by(
                PaymentMode.payment_mode).all()
            vendors = db_session.query(Vendor).filter_by(app_id=app_id).order_by(Vendor.vendor_name.asc()).all()
            projects = db_session.query(Project).filter_by(app_id=app_id).all()
            currencies = db_session.query(Currency).filter_by(app_id=app_id).order_by(Currency.currency_index).all()

            # Get cash/bank accounts
            cash_accounts = (
                db_session.query(ChartOfAccounts.id)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    (ChartOfAccounts.is_cash == True) | (ChartOfAccounts.is_bank == True)
                )
                .all()
            )
            cash_account_ids = [acc.id for acc in cash_accounts]

            # Modules
            modules_data = [
                mod.module_name for mod in
                db_session.query(Module).filter_by(app_id=app_id, included='yes').all()
            ]

            # Company details
            company = db_session.query(Company).filter_by(id=app_id).scalar()
            journals = db_session.query(Transaction.journal_number).filter_by(app_id=app_id).distinct().all()
            print(f'Journals are {journals}')
            return render_template(
                'general-ledger/journal_adjustment.html',
                transaction_categories=transaction_categories,
                payment_modes=payment_modes,
                vendors=vendors,
                project_names=projects,
                currencies=currencies,
                company=company,
                role=current_user.role,
                modules=modules_data,
                module_name="General Ledger",
                journals=journals,
                cash_account_ids=cash_account_ids
            )

    except Exception as e:
        logger.error(f'An error occurred while fetching data: {str(e)}')
        flash(f'An error occurred while fetching data: {str(e)}', 'danger')
        return redirect(request.referrer)


@general_ledger_routes.route('/api/journal_entries/<journal_number>', methods=['GET'])
@login_required
def get_journal_entries(journal_number):
    """
    API endpoint to fetch transactions for a specific journal number (read-only)
    """
    try:
        db_session = Session()
        app_id = current_user.app_id

        # Get all transactions for this journal number
        transactions = db_session.query(Transaction).filter(
            Transaction.journal_number == journal_number,
            Transaction.app_id == app_id
        ).order_by(Transaction.id).all()

        # Prepare the response data
        entries_data = []
        for txn in transactions:
            entries_data.append({
                'id': txn.id,
                'date': txn.date.strftime('%Y-%m-%d'),
                'project': txn.project.name if txn.project else '',
                'payment_to_from': txn.vendor.vendor_name if txn.vendor else '',
                'payment_mode': txn.payment_mode.payment_mode if txn.payment_mode else '',
                'account_type': txn.chart_of_accounts.parent_account_type if txn.chart_of_accounts else '',
                'category': txn.category.category if txn.category else '',
                'subcategory': txn.chart_of_accounts.sub_category if txn.chart_of_accounts else '',
                'debit': float(txn.amount) if txn.dr_cr == 'D' else 0.00,
                'credit': float(txn.amount) if txn.dr_cr == 'C' else 0.00,
                'description': txn.description,
                'currency': txn.currencies.user_currency if txn.currencies else ''
            })


        # Get some journal metadata from the first transaction
        first_txn = transactions[0] if transactions else None
        journal_data = {
            'journal_number': journal_number,
            'date': first_txn.date.strftime('%Y-%m-%d') if first_txn else '',
            'description': first_txn.description if first_txn else '',
            'total_entries': len(transactions)
        }


        return jsonify({
            'status': 'success',
            'journal': journal_data,
            'entries': entries_data
        })

    except Exception as e:

        logger.error(f'Error is {e}')
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500


@general_ledger_routes.route('/submit_journal_adjustment', methods=['POST'])
@login_required
def submit_journal_adjustment():
    """
    Endpoint to handle journal adjustment submissions (modifies existing journal)
    """
    user_session = Session()
    app_id = current_user.app_id
    user_id = current_user.id

    try:
        # Get base currency info
        base_currency_info = get_base_currency(user_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400
        base_currency_id = base_currency_info["base_currency_id"]
        # Get the journal number being adjusted
        journal_number = request.form.get('original_journal_number')
        if not journal_number:
            return jsonify({
                'status': 'error',
                'message': 'Journal number is required'
            }), 400

        # Verify journal exists
        existing_journal = user_session.query(Transaction).filter(
            Transaction.journal_number == journal_number,
            Transaction.app_id == app_id
        ).first()

        if not existing_journal:
            return jsonify({
                'status': 'error',
                'message': 'Journal not found'
            }), 404

        # Parse form data
        adjustment_date = datetime.strptime(request.form['adjustment_date'], '%Y-%m-%d').date()
        adjustment_reason = request.form.get('adjustment_reason', '')
        account_types = request.form.getlist('account_type')
        categories = request.form.getlist('category')
        subcategories = request.form.getlist('subcategory')
        debit_amounts = request.form.getlist('debit_amount')
        credit_amounts = request.form.getlist('credit_amount')
        descriptions = request.form.getlist('description')

        # Validate we have at least one valid entry
        valid_entries = 0
        for i in range(len(account_types)):
            if (account_types[i] and categories[i] and subcategories[i] and
                (debit_amounts[i] or credit_amounts[i]) and descriptions[i]):
                valid_entries += 1

        if valid_entries == 0:
            return jsonify({
                'status': 'error',
                'message': 'At least one valid adjustment entry is required'
            }), 400

        batch_notifications = set()

        # Process adjustment entries
        for i in range(len(account_types)):
            if not (account_types[i] and categories[i] and subcategories[i] and
                   (debit_amounts[i] or credit_amounts[i]) and descriptions[i]):
                continue  # Skip incomplete rows



            # Determine debit/credit
            if debit_amounts[i]:
                dr_cr = 'D'
                amount = float(debit_amounts[i])
            else:
                dr_cr = 'C'
                amount = float(credit_amounts[i])

            # Create adjustment transaction (using same journal number)
            txn = Transaction(
                journal_number=journal_number,  # Same journal number
                transaction_type=account_types[i],
                date=existing_journal.date,  # Original journal date
                category_id=categories[i],
                subcategory_id=subcategories[i],
                amount=amount,
                dr_cr=dr_cr,
                description=f"Adjustment: {descriptions[i]}",
                payment_mode_id=existing_journal.payment_mode_id,
                project_id=existing_journal.project_id,
                vendor_id=existing_journal.vendor_id,
                currency=existing_journal.currency,
                created_by=user_id,
                app_id=app_id,
                exchange_rate_id=existing_journal.exchange_rate_id,
                date_added=datetime.today(),
                adjustment_date=adjustment_date,
                adjustment_reason=adjustment_reason
            )
            user_session.add(txn)

        # Update journal's adjustment info
        existing_journal.adjusted_at = datetime.now()
        existing_journal.adjusted_by = user_id
        existing_journal.adjustment_reason = adjustment_reason

        # Commit all changes
        user_session.commit()

        # Create notifications if needed
        if batch_notifications:
            message = "Multiple auto-generated rates applied: " + "; ".join(batch_notifications) \
                      if len(batch_notifications) > 1 else next(iter(batch_notifications))

            create_notification(
                db_session=user_session,
                user_id=None,
                company_id=app_id,
                message=message,
                type='info',
                is_popup=True,
                url=url_for('view_exchange_rates')
            )

        return jsonify({
            'status': 'success',
            'message': 'Journal adjustment added successfully',
            'journal_number': journal_number,
            'rate_notices': list(batch_notifications) if batch_notifications else None
        })

    except Exception as e:
        user_session.rollback()
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

    finally:
        user_session.close()


