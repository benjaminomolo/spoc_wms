#app/routes/financial_reports/balance_sheet.py
import json
import logging
import math
import traceback
from datetime import datetime, timedelta
from decimal import Decimal

from flask import flash, redirect, request, render_template, jsonify, url_for
from flask_login import login_required, current_user
from sqlalchemy import func, case, desc, or_
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from sqlalchemy.orm import joinedload

from ai import get_base_currency, get_exchange_rate, get_or_create_exchange_rate_id
from db import Session
from decorators import role_required
from models import ChartOfAccounts, Category, Company, Module, OpeningBalance, ActivityLog, Project, \
    Vendor, PaymentMode, Currency, JournalEntry, Journal
from services.chart_of_accounts_helpers import calculate_account_balance, has_open_transactions
from services.post_to_ledger import post_opening_balances_to_ledger
from utils import apply_date_filters, format_amount, create_notification, generate_unique_journal_number, empty_to_none, \
    get_cash_balances_with_base
from utils_and_helpers.date_time_utils import get_filter_date
from . import financial_reports_bp

logger = logging.getLogger()


@financial_reports_bp.route('/balance_sheet', methods=['GET', 'POST'])
@login_required
def balance_sheet():
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Fetch company details
        company = db_session.query(Company).filter_by(id=app_id).first()
        if not company:
            return jsonify({'success': False, 'message': 'Company not found'}), 404

        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        api_key = company.api_key

        # Fetch base currency info
        base_currency_info = get_base_currency(db_session, app_id)

        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency_code = base_currency_info["base_currency"]

        # Fetch currency data
        # Fetch ALL currency data ONCE
        currencies = db_session.query(
            Currency.id,
            Currency.user_currency
        ).filter(
            Currency.app_id == app_id
        ).all()

        # Create currency mapping
        currency_map = {str(c.id): c.user_currency for c in currencies}
        foreign_currency_ids = [c.id for c in currencies if c.id != base_currency_id]

        # Initialize filter variables
        currency = None
        start_date = None
        end_date = None

        if request.method == 'POST':
            # Handle JSON Data with error checking
            try:
                filters = request.get_json()
                if not filters:
                    return jsonify({'success': False, 'message': 'No filter data provided'}), 400

                currency = filters.get('currency')
                start_date_str = filters.get('startDate')
                end_date_str = filters.get('endDate')

                # Date parsing with validation
                if start_date_str:
                    try:
                        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                    except ValueError:
                        return jsonify({'success': False, 'message': 'Invalid start date format'}), 400

                if end_date_str:
                    try:
                        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                    except ValueError:
                        return jsonify({'success': False, 'message': 'Invalid end date format'}), 400

                # Handle Default Currency Selection
                if not currency:
                    default_currency = db_session.query(Currency.id).filter(
                        Currency.app_id == app_id,
                        Currency.currency_index == 1
                    ).first()
                    currency = default_currency.id if default_currency else None

                # Validate date range
                if start_date and end_date and start_date > end_date:
                    return jsonify({'success': False, 'message': 'Start date must be before or equal to end date'}), 400

                # Set Default Date Range - Updated to use Journal table
                if not start_date:
                    start_date = db_session.query(func.min(Journal.date)).filter(
                        Journal.app_id == app_id, Journal.status == 'Posted').scalar()
                if not end_date:
                    end_date = db_session.query(func.max(Journal.date)).filter(
                        Journal.app_id == app_id, Journal.status == 'Posted').scalar()

                return jsonify({
                    'currency': currency,
                    'start_date': start_date.strftime('%Y-%m-%d') if start_date else None,
                    'end_date': end_date.strftime('%Y-%m-%d') if end_date else None
                })

            except Exception as e:
                db_session.rollback()
                return jsonify({'success': False, 'message': str(e)}), 500

        else:  # GET Request
            try:
                currency = request.args.get('currency')
                start_date_str = request.args.get('startDate')
                end_date_str = request.args.get('endDate')

                # Date parsing with validation
                if start_date_str:
                    try:
                        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                    except ValueError:
                        return render_template('error.html', error='Invalid start date format'), 400

                if end_date_str:
                    try:
                        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                    except ValueError:
                        return render_template('error.html', error='Invalid end date format'), 400

                # Handle Default Currency Selection
                if not currency:
                    default_currency = db_session.query(Currency.id).filter(
                        Currency.app_id == app_id,
                        Currency.currency_index == 1
                    ).first()
                    currency = default_currency.id if default_currency else None

                # Set Default Date Range - Updated to use Journal table
                if not start_date:
                    start_date = db_session.query(func.min(Journal.date)).filter(
                        Journal.app_id == app_id, Journal.status == 'Posted').scalar()
                if not end_date:
                    end_date = db_session.query(func.max(Journal.date)).filter(
                        Journal.app_id == app_id, Journal.status == 'Posted').scalar()

                return render_template(
                    'base_currency_balance_sheet.html',
                    company=company,
                    role=role,
                    modules=modules_data,
                    api_key=api_key,
                    currency=currency,
                    start_date=start_date.strftime('%Y-%m-%d') if start_date else None,
                    end_date=end_date.strftime('%Y-%m-%d') if end_date else None,
                    base_currency=base_currency_code,
                    base_currency_id=base_currency_id,
                    currency_map=currency_map,  # Pass the dict, not JSON string
                    foreign_currency_ids=foreign_currency_ids
                )

            except Exception as e:
                db_session.rollback()
                return render_template('error.html', error=str(e)), 500

    except Exception as e:
        # Handle any exceptions that occur before the try block
        logger.error(f'An error occurred {str(e)}\n{traceback.format_exc()}')
        return jsonify({'success': False, 'message': str(e)}), 500

    finally:
        # Ensure session is always closed
        db_session.close()
