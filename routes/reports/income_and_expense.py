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
    Vendor, PaymentMode, Currency, JournalEntry, Journal
from services.chart_of_accounts_helpers import calculate_account_balance, has_open_transactions
from services.post_to_ledger import post_opening_balances_to_ledger
from utils import apply_date_filters, format_amount, create_notification, generate_unique_journal_number, empty_to_none, \
    get_cash_balances_with_base
from utils_and_helpers.date_time_utils import get_filter_date
from . import financial_reports_bp

logger = logging.getLogger()


@financial_reports_bp.route('/income_expense_report', methods=['GET'])
@login_required
def income_expense_report():
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Fetch the company name, role, and modules data
        company = db_session.query(Company).filter_by(id=app_id).first()
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

        return render_template(
            'base_currency_income_and_expense_report.html',
            company=company,
            role=role,
            modules=modules_data,
            base_currency=base_currency_code,
            base_currency_id=base_currency_id,
            api_key=api_key  # Pass the api_key data to the template
        )
    finally:
        db_session.close()
