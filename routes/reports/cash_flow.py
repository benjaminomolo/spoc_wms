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


@financial_reports_bp.route('/cash_flow', methods=["GET"])
@login_required
def cash_flow():
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        if not company:
            return jsonify({'success': False, 'message': 'Company not found'}), 404

        # Get company data
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        api_key = company.api_key

        # Get currency info
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400
        base_currency_name = base_currency_info["base_currency"]
        base_currency_id = base_currency_info['base_currency_id']

        # Initialize filter variables with safe defaults
        currency = None
        start_date = None
        end_date = None

        try:
            # Get and validate parameters
            currency = request.args.get('currency')
            start_date_str = request.args.get('startDate')
            end_date_str = request.args.get('endDate')

            # Parse dates safely
            if start_date_str and start_date_str.lower() != 'null':
                try:
                    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                except ValueError:
                    return jsonify({"error": "Invalid start date format. Use YYYY-MM-DD"}), 400

            if end_date_str and end_date_str.lower() != 'null':
                try:
                    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                except ValueError:
                    return jsonify({"error": "Invalid end date format. Use YYYY-MM-DD"}), 400

            # Set default currency if none provided
            if not currency or currency.lower() == 'null':
                default_currency = db_session.query(Currency.id).filter(
                    Currency.app_id == app_id,
                    Currency.currency_index == 1
                ).first()
                currency = default_currency.id if default_currency else None

            # Set default dates if none provided - now querying Journal table instead of Transaction
            if not start_date:
                start_date = db_session.query(func.min(Journal.date)).filter(
                    Journal.app_id == app_id).scalar()
            if not end_date:
                end_date = db_session.query(func.max(Journal.date)).filter(
                    Journal.app_id == app_id).scalar()

            return render_template(
                'reports/cash_flow_statement.html',
                company=company,
                role=role,
                modules=modules_data,
                api_key=api_key,
                currency=currency,
                start_date=start_date.strftime('%Y-%m-%d') if start_date else None,
                end_date=end_date.strftime('%Y-%m-%d') if end_date else None,
                base_currency=base_currency_name,
                base_currency_id=base_currency_id
            )

        except Exception as e:
            db_session.rollback()
            logger.error(f'Error in cash flow report: {str(e)}', exc_info=True)
            flash("An error occurred while generating the report", "error"), 500
            return redirect(request.referrer)

    except Exception as e:
        logger.error(f'Unexpected error in cash flow route: {str(e)}', exc_info=True)
        flash("An unexpected error occurred", "error"), 500
        return redirect(request.referrer)
    finally:
        db_session.close()


@financial_reports_bp.route('/get_cash_flow_transactions_base_currency', methods=["GET"])
def get_cash_flow_transactions_base_currency():
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        company = user_session.query(Company).filter_by(api_key=api_key).first()
        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        try:
            app_id = company.id
            base_currency_info = get_base_currency(user_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Get cash/bank accounts for THIS company only
            cash_account_ids = {
                a.id for a in user_session.query(ChartOfAccounts.id)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    or_(ChartOfAccounts.is_cash == True,
                        ChartOfAccounts.is_bank == True)
                ).all()
            }

            # Load journal entries with related data
            entries = (
                user_session.query(JournalEntry)
                .join(Journal)
                .filter(Journal.app_id == app_id, Journal.status == 'Posted')
                .options(
                    joinedload(JournalEntry.journal)
                    .joinedload(Journal.currency),
                    joinedload(JournalEntry.journal)
                    .joinedload(Journal.exchange_rate),
                    joinedload(JournalEntry.chart_of_accounts)
                    .joinedload(ChartOfAccounts.report_section),
                    joinedload(JournalEntry.journal)
                    .joinedload(Journal.payment_mode),
                    joinedload(JournalEntry.journal)
                    .joinedload(Journal.vendor),
                    joinedload(JournalEntry.journal)
                    .joinedload(Journal.project)
                ).all()
            )

            # Map journal numbers → affects_cash
            journal_cash_status = {}
            for entry in entries:
                journal = entry.journal
                if journal.journal_number not in journal_cash_status:
                    # Get all entries for this journal
                    journal_entries = [e for e in entries if e.journal_id == journal.id]
                    journal_account_ids = {e.subcategory_id for e in journal_entries}
                    all_accounts_are_cash = journal_account_ids.issubset(cash_account_ids)
                    affects_cash = (
                            any(e.subcategory_id in cash_account_ids for e in journal_entries)
                            and not all_accounts_are_cash
                    )
                    journal_cash_status[journal.journal_number] = affects_cash

            transactions_list = []

            for entry in entries:
                journal = entry.journal
                transaction_currency_id = journal.currency_id
                original_amount = entry.amount
                amount_in_base = original_amount

                if transaction_currency_id != base_currency_id:
                    exchange_rate = journal.exchange_rate.rate if journal.exchange_rate else None
                    if exchange_rate:
                        amount_in_base = round(
                            Decimal(str(original_amount)) * Decimal(str(exchange_rate)),
                            2
                        )

                affects_cash = journal_cash_status.get(journal.journal_number, False)
                is_cash_entry = entry.subcategory_id in cash_account_ids if affects_cash else False

                cash_flow_direction = None
                if affects_cash:
                    if is_cash_entry:
                        cash_flow_direction = (
                            "inflow" if entry.dr_cr == "C" else "outflow"  # Credit = inflow, Debit = outflow
                        )
                    else:
                        cash_flow_direction = (
                            "inflow" if entry.dr_cr == "D" else "outflow"  # Debit = inflow, Credit = outflow
                        )

                    transactions_list.append({
                        "id": entry.id,
                        "journal_id": journal.id,
                        "journal_number": journal.journal_number,
                        "transaction_type": entry.chart_of_accounts.parent_account_type if entry.chart_of_accounts else None,
                        "date": journal.date.strftime('%Y-%m-%d'),
                        "category": entry.chart_of_accounts.categories.category if entry.chart_of_accounts and entry.chart_of_accounts.categories else None,
                        "subcategory": entry.chart_of_accounts.sub_category if entry.chart_of_accounts else None,
                        "currency": journal.currency.user_currency if journal.currency else None,
                        "amount": float(original_amount),
                        "amount_in_base_currency": float(amount_in_base),
                        "base_currency": base_currency,
                        "dr_cr": entry.dr_cr,
                        "description": entry.description or journal.narration,
                        "payment_mode": journal.payment_mode.payment_mode if journal.payment_mode else None,
                        "payment_to_vendor": journal.vendor.vendor_name if journal.vendor else None,
                        "project_name": journal.project.name if journal.project else None,
                        "date_added": journal.date_added.strftime('%Y-%m-%d') if journal.date_added else None,
                        "normal_balance": entry.chart_of_accounts.normal_balance if entry.chart_of_accounts else None,
                        "report_section": entry.chart_of_accounts.report_section.name if entry.chart_of_accounts and entry.chart_of_accounts.report_section else None,
                        "is_cash_flow": affects_cash,
                        "is_cash_entry": is_cash_entry,
                        "cash_flow_direction": cash_flow_direction,
                        "line_number": entry.line_number
                    })
            return jsonify(transactions_list)

        except Exception as e:
            logger.error(f'Error: {str(e)}')
            return jsonify({"error": str(e)}), 500
