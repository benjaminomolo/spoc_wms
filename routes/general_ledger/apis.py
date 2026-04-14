#app/routes/general_ledger/apis.py

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
    Vendor, PaymentMode, Currency, JournalEntry, Journal, ExchangeRate
from services.chart_of_accounts_helpers import calculate_account_balance, has_open_transactions
from services.post_to_ledger import post_opening_balances_to_ledger
from utils import apply_date_filters, format_amount, create_notification, generate_unique_journal_number, empty_to_none, \
    get_cash_balances_with_base
from utils_and_helpers.date_time_utils import get_filter_date
from . import general_ledger_bp

logger = logging.getLogger()


@general_ledger_bp.route('/api/transactions', methods=["GET"])
def get_general_ledger_transactions():
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        try:
            app_id = current_user.app_id
            currency_filter = request.args.get("currency")
            with_base_currency = not currency_filter or not currency_filter.strip()
            include_cash_balances = request.args.get("include_cash_balances", "true").lower() == "true"

            # Get base currency info
            base_currency_info = get_base_currency(user_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # SINGLE OPTIMIZED QUERY - get all data in one go
            transactions_list = get_optimized_transactions(
                user_session,
                app_id,
                base_currency_id,
                base_currency,
                with_base_currency,
                request.args
            )

            # Only calculate cash balances if requested
            if include_cash_balances:
                cash_balances, total_cash_balance = get_cash_balances_with_base(
                    user_session,
                    app_id,
                    currency_filter if not with_base_currency else None,
                    request.args.get("start_date"),
                    request.args.get("end_date"),
                    with_base_currency,
                    base_currency_id,
                    base_currency
                )
            else:
                cash_balances, total_cash_balance = [], 0

            return jsonify(transactions_list, cash_balances, total_cash_balance)

        except Exception as e:
            logger.error(f"Error processing request: {str(e)}", exc_info=True)
            return jsonify({"error": str(e)}), 500


def get_optimized_transactions(session, app_id, base_currency_id, base_currency, with_base_currency, request_args):
    """Single optimized query to get all transaction data"""

    # Build the base query with all joins EXCEPT Currency
    query = (
        session.query(
            JournalEntry.id,
            JournalEntry.amount,
            JournalEntry.dr_cr,
            JournalEntry.description,
            Journal.date,
            Journal.date_added,
            Journal.journal_number,
            ChartOfAccounts.sub_category.label('subcategory_name'),
            Category.category.label('category_name'),
            ChartOfAccounts.parent_account_type,
            ChartOfAccounts.is_cash,  # ADD THIS
            ChartOfAccounts.is_bank,   # ADD THIS
            ChartOfAccounts.is_payable,
            ChartOfAccounts.is_receivable,
            Currency.user_currency,
            Journal.currency_id,  # ADD THIS - crucial for currency comparison
            PaymentMode.payment_mode,
            Vendor.vendor_name,
            Project.name.label('project_name'),
            ExchangeRate.rate.label('exchange_rate')
        )
        .select_from(JournalEntry)
        .join(Journal, JournalEntry.journal_id == Journal.id)
        .join(ChartOfAccounts, JournalEntry.subcategory_id == ChartOfAccounts.id)
        .join(Category, ChartOfAccounts.category_fk == Category.id)
        .join(Currency, Journal.currency_id == Currency.id)
        .outerjoin(PaymentMode, Journal.payment_mode_id == PaymentMode.id)
        .outerjoin(Vendor, Journal.vendor_id == Vendor.id)
        .outerjoin(Project, Journal.project_id == Project.id)
        .outerjoin(ExchangeRate, Journal.exchange_rate_id == ExchangeRate.id)
        .filter(Journal.app_id == app_id)
        .filter(Journal.status == 'Posted')
    )

    # Apply filters
    currency_filter = request_args.get("currency")
    if currency_filter:
        query = query.filter(Journal.currency_id == currency_filter)

    payment_mode_filter = request_args.get("payment_mode")
    if payment_mode_filter:
        query = query.filter(Journal.payment_mode_id == payment_mode_filter)

    vendor_filter = request_args.get("vendor")
    if vendor_filter:
        query = query.filter(Journal.vendor_id == vendor_filter)

    project_filter = request_args.get("project")
    if project_filter:
        query = query.filter(Journal.project_id == project_filter)

    category_filter = request_args.get("category")
    if category_filter:
        query = query.filter(ChartOfAccounts.category_fk == category_filter)

    subcategory_filter = request_args.get("subcategory")
    if subcategory_filter:
        query = query.filter(JournalEntry.subcategory_id == subcategory_filter)

    # FIXED: Date filtering - handle directly in this function
    start_date = request_args.get("start_date")
    end_date = request_args.get("end_date")
    time_filter = request_args.get("time_filter")

    if start_date and end_date:
        try:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            query = query.filter(Journal.date.between(start_date_obj, end_date_obj))
        except ValueError:
            logger.error("Invalid date format provided")

    elif time_filter:
        start_date, end_date = get_filter_date(time_filter)
        if start_date and end_date:
            query = query.filter(Journal.date.between(start_date, end_date))

    # Order results
    query = query.order_by(Journal.date.desc(), Journal.id.desc(), JournalEntry.line_number)

    # Execute single query
    results = query.all()

    # Process results in Python (much faster than multiple DB queries)
    transactions_list = []
    for row in results:
        original_amount = float(row.amount)
        amount_in_base = original_amount
        # Calculate base currency amount if needed
        if (with_base_currency and
                int(base_currency_id) != int(row.currency_id) and  # Always use base_currency now
                row.exchange_rate):
            amount_in_base = round(original_amount * float(row.exchange_rate), 2)

        transaction_data = {
            "id": row.id,
            "transaction_type": row.parent_account_type,
            "date": row.date.strftime('%Y-%m-%d') if row.date else None,
            "category": row.category_name,
            "subcategory": row.subcategory_name,
            "currency": row.user_currency,  # Always use base_currency instead of row.user_currency
            "amount": original_amount,
            "amount_in_base_currency": amount_in_base,
            "base_currency": base_currency,
            "dr_cr": row.dr_cr,
            "description": row.description,
            "payment_mode": row.payment_mode,
            "payment_to_vendor": row.vendor_name,
            "project_name": row.project_name,
            "date_added": row.date_added.strftime('%Y-%m-%d') if row.date_added else None,
            "journal_number": row.journal_number,
            "is_payable": bool(row.is_payable),
            "is_receivable": bool(row.is_receivable),
            "is_cash": bool(row.is_cash),
            "is_bank": bool(row.is_bank)
        }

        transactions_list.append(transaction_data)

    return transactions_list
