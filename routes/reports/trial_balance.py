#app/routes/financial_reports/balance_sheet.py

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
    Vendor, PaymentMode, Currency, JournalEntry, Journal, User
from services.chart_of_accounts_helpers import calculate_account_balance, has_open_transactions
from services.post_to_ledger import post_opening_balances_to_ledger
from utils import apply_date_filters, format_amount, create_notification, generate_unique_journal_number, empty_to_none, \
    get_cash_balances_with_base
from utils_and_helpers.date_time_utils import get_filter_date
from . import financial_reports_bp

logger = logging.getLogger()


@financial_reports_bp.route('/trial_balance', methods=['GET'])
@login_required
def trial_balance():
    db_session = Session()  # Open a new session

    try:
        app_id = current_user.app_id

        # Fetch the company name and role
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = db_session.query(User.role).filter_by(app_id=app_id).first()[0]
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        api_key = company.api_key
        # Fetch base currency info
        base_currency_info = get_base_currency(db_session, app_id)

        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_name = base_currency_info["base_currency"]

        return render_template(
            'base_currency_trial_balance.html',
            company=company,
            role=role,
            modules=modules_data,
            api_key=api_key,
            base_currency=base_currency_name
        )

    except Exception as e:
        logger.error(f"An error occurred while fetching trial balance data: {e}")  # Log the error for debugging
        return "An error occurred while processing your request.", 500  # Return a 500 status code for server errors

    finally:
        db_session.close()  # Ensure the session is closed
