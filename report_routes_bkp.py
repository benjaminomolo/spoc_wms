import calendar
import datetime
import logging
from collections import defaultdict
from decimal import Decimal
from typing import List

import traceback

from flask import Blueprint, jsonify, render_template, flash, redirect, url_for, request
from flask_login import login_required, current_user
from sqlalchemy import func, case, and_
from sqlalchemy.orm import joinedload

from ai import get_base_currency
from decorators import role_required, require_permission

from db import Session
from models import Company, Module, Currency, ChartOfAccounts, ExchangeRate, Vendor, Project, PaymentMode, \
    Journal, JournalEntry
from utils import get_cash_balances_with_base, get_monetary_accounts, calculate_fx_revaluation, get_converted_cost, \
    empty_to_none

report_routes = Blueprint('report_routes', __name__)
# Set up logging
logger = logging.getLogger(__name__)


@report_routes.route('/api/cash_balances', methods=['GET'])
def api_cash_balances():
    api_key = request.headers.get("X-API-Key")
    base_currency_id_param = request.args.get('base_currency_id')
    base_currency_code_param = request.args.get('base_currency_code')
    logger.info(f'bases are {base_currency_code_param} and {base_currency_id_param} and API {api_key}')
    if not api_key:
        return jsonify({"error": "API key missing"}), 401

    with Session() as session:
        try:
            company = session.query(Company).filter_by(api_key=api_key).first()
            if not company:
                return jsonify({"error": "Invalid API key"}), 403

            base_currency_info = get_base_currency(session, company.id)
            if not base_currency_info:
                return jsonify({'error': 'Base currency not defined'}), 400

            # Safely handle parameters that might be "null" strings
            currency = request.args.get('currency')
            with_base_currency = False
            if not currency:
                with_base_currency = True
            if currency == 'null':
                currency = None

            start_date_param = request.args.get('start_date')
            if start_date_param == 'null':
                start_date_param = None

            end_date_param = request.args.get('end_date')
            if end_date_param == 'null':
                end_date_param = datetime.datetime.today()

            # Parse dates only if they exist and aren't empty/null strings
            start_date = None
            if start_date_param and start_date_param.strip():
                try:
                    start_date = datetime.datetime.strptime(start_date_param.strip(), "%Y-%m-%d").date()
                except ValueError:
                    return jsonify({"error": "Invalid start_date format (YYYY-MM-DD required)"}), 400

            end_date = None
            if end_date_param and end_date_param.strip():
                try:
                    end_date = datetime.datetime.strptime(end_date_param.strip(), "%Y-%m-%d").date()
                except ValueError:
                    return jsonify({"error": "Invalid end_date format (YYYY-MM-DD required)"}), 400

            # Get currency ID if specified
            currency_id = None
            if currency and currency.strip():
                currency_id = session.query(Currency.id).filter_by(
                    user_currency=currency.strip(),
                    app_id=company.id
                ).scalar()
                if not currency_id:
                    return jsonify({"error": f"Currency {currency} not found"}), 400

            cash_balances, total_cash = get_cash_balances_with_base(
                db_session=session,
                app_id=company.id,
                currency=currency_id,
                start_date=start_date,
                end_date=end_date,
                with_base_currency=with_base_currency,
                base_currency=int(base_currency_id_param)
            )

            return jsonify({
                "cash_balances": cash_balances,
                "total_cash": str(total_cash)
            })

        except Exception as e:
            session.rollback()
            return jsonify({"error": str(e)}), 500


@report_routes.route('/api/fx_revaluation', methods=['GET'])
@login_required
def get_fx_revaluation():
    db_session = Session()

    try:
        app_id = current_user.app_id
        date_str = request.args.get('date')
        start_date_str = request.args.get('start_date')
        # ✅ NEW: Optional base currency parameters
        base_currency_id = request.args.get('base_currency_id', type=int)
        base_currency_code = request.args.get('base_currency_code')

        if not date_str:
            return jsonify({'error': 'End date (date parameter) is required'}), 400

        try:
            as_of_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
        except ValueError as e:
            return jsonify({'error': f'Invalid date format: {str(e)}. Use YYYY-MM-DD'}), 400

        fx_result = calculate_fx_revaluation(
            db_session=db_session,
            app_id=app_id,
            as_of_date=as_of_date,
            start_date=start_date,
            base_currency_id=base_currency_id,
            base_currency_code=base_currency_code  # <- add this
        )

        logger.info(f'FX Result {fx_result}')

        return jsonify({
            'status': 'success',
            'data': {
                'total_fx_gain_loss': fx_result['total_fx_gain_loss'],
                'details': fx_result['details'],
                'as_of_date': date_str,
                'start_date': start_date_str
            }
        })

    except Exception as e:
        logger.error(f'Error calculating FX revaluation: {str(e)}')
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()


@report_routes.route('/report/bank_reconciliation', methods=["GET"])
@login_required
def bank_reconciliation():
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        api_key = company.api_key

        # Get bank accounts
        bank_accounts = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.is_bank == True
        ).all()

        # Get currencies
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = db_session.query(Currency).filter_by(
            app_id=app_id,
            currency_index=1
        ).first()

        return render_template(
            'reports/bank_reconciliation.html',
            company=company,
            bank_accounts=bank_accounts,
            currencies=currencies,
            base_currency=base_currency.user_currency if base_currency else '',
            api_key=api_key,
            role=role,
            modules=modules_data
        )

    except Exception as e:
        logger.error(f'Error occured while rendering bank recon {e}')
    finally:
        db_session.close()


def get_bank_transactions(
        db: Session,
        app_id: int,
        account_id: int = None,
        currency_id: int = None,
        status: str = "all",  # "all", "reconciled", "unreconciled"
        start_date: datetime.date = None,
        end_date: datetime.date = None
):
    # Query JournalEntry and join with Journal and ChartOfAccounts
    query = db.query(JournalEntry).join(
        Journal, Journal.id == JournalEntry.journal_id
    ).join(
        ChartOfAccounts, ChartOfAccounts.id == JournalEntry.subcategory_id
    ).filter(
        ChartOfAccounts.is_bank == True,
        Journal.app_id == app_id,
        Journal.status == 'Posted'  # Only include posted journals
    )

    # Apply filters
    if account_id:
        query = query.filter(JournalEntry.subcategory_id == account_id)
    if currency_id:
        query = query.filter(Journal.currency_id == currency_id)
    if status != "all":
        query = query.filter(JournalEntry.reconciled == (status == "reconciled"))
    if start_date:
        query = query.filter(Journal.date >= start_date)
    if end_date:
        query = query.filter(Journal.date <= end_date)

    return query.all()


# Alternative approach returning both entry and journal
def get_bank_transactions_with_journal(
        db: Session,
        app_id: int,
        account_id: int = None,
        currency_id: int = None,
        status: str = "all",
        start_date: datetime.date = None,
        end_date: datetime.date = None
):
    query = db.query(JournalEntry, Journal).join(
        Journal, Journal.id == JournalEntry.journal_id
    ).join(
        ChartOfAccounts, ChartOfAccounts.id == JournalEntry.subcategory_id
    ).filter(
        ChartOfAccounts.is_bank == True,
        Journal.app_id == app_id,
        Journal.status == 'Posted'
    )

    # Apply filters (same as above)
    if account_id:
        query = query.filter(JournalEntry.subcategory_id == account_id)
    if currency_id:
        query = query.filter(Journal.currency_id == currency_id)
    if status != "all":
        query = query.filter(JournalEntry.reconciled == (status == "reconciled"))
    if start_date:
        query = query.filter(Journal.date >= start_date)
    if end_date:
        query = query.filter(Journal.date <= end_date)

    return query.all()


def calculate_account_summary(journal_entries: List[JournalEntry], opening_balance: float = 0):
    """Calculate summary from journal entry list with proper decimal handling"""
    inflows = Decimal('0')
    outflows = Decimal('0')
    reconciled = Decimal('0')

    for entry in journal_entries:
        amount = Decimal(str(entry.amount))  # Ensure proper decimal conversion
        if entry.dr_cr == "D":
            inflows += amount
            if entry.reconciled:
                reconciled += amount
        else:
            outflows += amount
            if entry.reconciled:
                reconciled -= amount

    net_flow = inflows - outflows
    closing_balance = Decimal(str(opening_balance)) + net_flow
    reconciled_balance = Decimal(str(opening_balance)) + reconciled

    return {
        "opening_balance": float(opening_balance),
        "total_inflows": float(inflows),
        "total_outflows": float(outflows),
        "net_flow": float(net_flow),
        "closing_balance": float(closing_balance),
        "reconciled_balance": float(reconciled_balance),
        "unreconciled_balance": float(closing_balance - reconciled_balance)
    }


@report_routes.route('/api/bank_reconciliation/summary', methods=["GET"])
@login_required
def get_reconciliation_summary():
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Get filters
        currency_id = request.args.get('currency_id')  # Optional currency filter
        status = request.args.get('status', 'all')
        account_id = request.args.get('account_id', 'all')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # Parse dates
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None

        if not currency_id:
            # Get currency info
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400
            base_currency_id = base_currency_info["base_currency_id"]
            currency_id = base_currency_id

        # Get all bank accounts
        bank_accounts = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.is_bank == True,
            ChartOfAccounts.app_id == app_id
        ).all()

        results = []
        for account in bank_accounts:
            # Skip if account filter is specified and doesn't match
            if account_id != 'all' and str(account.id) != str(account_id):
                continue

            # Get ALL journal entries for this account
            all_entries_query = db_session.query(JournalEntry).join(
                Journal, Journal.id == JournalEntry.journal_id
            ).filter(
                JournalEntry.subcategory_id == account.id,
                Journal.app_id == app_id,
                Journal.status == 'Posted'
            )

            # Filter by currency if specified
            if currency_id:
                all_entries_query = all_entries_query.filter(Journal.currency_id == int(currency_id))

            all_entries = all_entries_query.all()

            # Calculate opening balance (ALL entries before start date)
            opening_balance = Decimal('0')
            if start_date:
                opening_balance = sum(
                    Decimal(str(entry.amount)) if entry.dr_cr == "D" else -Decimal(str(entry.amount))
                    for entry in all_entries
                    if entry.journal.date < start_date
                )

            # Filter period entries by date range
            period_entries = [
                entry for entry in all_entries
                if (not start_date or entry.journal.date >= start_date) and
                   (not end_date or entry.journal.date <= end_date)
            ]

            # Apply status filter ONLY to period entries
            if status != 'all':
                if status == 'true':
                    period_entries = [entry for entry in period_entries if entry.reconciled is True]
                elif status == 'false':
                    period_entries = [entry for entry in period_entries if entry.reconciled is False]

            # Skip if no entries in period
            if not period_entries and not start_date:
                continue

            # Calculate summary
            summary = calculate_account_summary(period_entries, float(opening_balance))

            # Get currency info
            currency = db_session.query(Currency).get(currency_id) if currency_id else None

            results.append({
                "account_id": account.id,
                "account_name": account.sub_category,
                "currency": currency.user_currency if currency else "Unknown",
                "currency_id": currency_id,
                **summary
            })

        return jsonify(results)

    except Exception as e:
        logger.error(f"Error in reconciliation summary: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        db_session.close()


@report_routes.route('/api/bank_reconciliation/transactions', methods=["GET"])
@login_required
def get_reconciliation_transactions():
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Get all filter parameters
        account_id = request.args.get('account_id')
        currency_id = request.args.get('currency_id')
        status = request.args.get('status', 'all')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        # Parse dates
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None

        # Build base query
        query = db_session.query(
            JournalEntry,
            Currency.user_currency.label('currency_code'),
            ChartOfAccounts.sub_category.label('account_name'),
            Journal  # Include journal for header information
        ).join(
            Journal, Journal.id == JournalEntry.journal_id
        ).join(
            ChartOfAccounts,
            JournalEntry.subcategory_id == ChartOfAccounts.id
        ).outerjoin(
            Currency,
            Journal.currency_id == Currency.id
        ).filter(
            ChartOfAccounts.is_bank == True,
            Journal.app_id == app_id,
            Journal.status == 'Posted'
        )

        # Apply filters
        if account_id:
            query = query.filter(JournalEntry.subcategory_id == account_id)

        if currency_id:
            query = query.filter(Journal.currency_id == currency_id)

        if status == 'true':
            query = query.filter(JournalEntry.reconciled == True)
        elif status == 'false':
            query = query.filter(JournalEntry.reconciled == False)

        if start_date:
            query = query.filter(Journal.date >= start_date)
        if end_date:
            query = query.filter(Journal.date <= end_date)

        # Execute query and format results
        results = query.order_by(Journal.date, JournalEntry.line_number).all()

        formatted_results = []
        for entry, currency_code, account_name, journal in results:
            amount = float(entry.amount) if entry.dr_cr == 'D' else -float(entry.amount)
            formatted_results.append({
                'id': entry.id,
                'journal_id': journal.id,
                'journal_number': journal.journal_number,
                'date': journal.date.strftime('%Y-%m-%d'),
                'description': entry.description or journal.narration,
                'amount': amount,
                'absolute_amount': float(entry.amount),
                'dr_cr': entry.dr_cr,
                'currency': currency_code if currency_code else 'Unknown',
                'account_name': account_name,
                'reconciled': entry.reconciled,
                'reconciliation_date': entry.reconciliation_date.strftime(
                    '%Y-%m-%d') if entry.reconciliation_date else None,
                'payment_mode': journal.payment_mode.payment_mode if journal.payment_mode else None,
                'vendor_name': journal.vendor.vendor_name if journal.vendor else None,
                'line_number': entry.line_number
            })

        return jsonify(formatted_results)

    except Exception as e:
        logger.error(f"Error fetching reconciliation transactions: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        db_session.close()


@report_routes.route('/api/bank_reconciliation/mark', methods=["POST"])
@login_required
def mark_reconciliation_status():
    db_session = Session()
    try:
        # Validate input
        data = request.get_json()

        print(f'data is {data}')
        if not data:
            return jsonify({'error': 'No data provided'}), 400

        entry_ids = data.get('transaction_ids', [])
        reconciled = data.get('reconciled', True)
        notes = data.get('notes', '')

        # Validate entry IDs
        if not entry_ids or not isinstance(entry_ids, list):
            return jsonify({'error': 'Invalid journal entry IDs'}), 400

        # Convert all IDs to integers to prevent SQL injection
        try:
            entry_ids = [int(entry_id) for entry_id in entry_ids]
        except (ValueError, TypeError):
            return jsonify({'error': 'Invalid journal entry ID format'}), 400

        # Get current user's app_id for security
        app_id = current_user.app_id

        # First verify all journal entries belong to this app and are bank transactions
        valid_entries = db_session.query(JournalEntry.id) \
            .join(Journal, Journal.id == JournalEntry.journal_id) \
            .join(ChartOfAccounts, JournalEntry.subcategory_id == ChartOfAccounts.id) \
            .filter(
            JournalEntry.id.in_(entry_ids),
            Journal.app_id == app_id,
            ChartOfAccounts.is_bank == True
        ).all()

        valid_ids = {entry.id for entry in valid_entries}
        invalid_ids = set(entry_ids) - valid_ids

        if invalid_ids:
            return jsonify({
                'error': f'{len(invalid_ids)} journal entries not found or not bank transactions',
                'invalid_ids': list(invalid_ids)
            }), 400

        # Update only valid journal entries
        update_time = datetime.datetime.now()

        db_session.query(JournalEntry) \
            .filter(JournalEntry.id.in_(valid_ids)) \
            .update({
            'reconciled': reconciled,
            'reconciliation_date': update_time if reconciled else None,
            'updated_at': update_time
        }, synchronize_session=False)

        db_session.commit()

        # Return the updated entry count
        return jsonify({
            'success': True,
            'updated_count': len(valid_ids),
            'reconciliation_date': update_time.isoformat() if reconciled else None
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error marking reconciliation status: {str(e)}", exc_info=True)
        return jsonify({
            'error': 'Failed to update journal entries',
            'details': str(e)
        }), 500
    finally:
        db_session.close()


@report_routes.route('/api/balance_sheet', methods=['GET'])
@login_required
def get_balance_sheet():
    db_session = Session()
    try:
        app_id = current_user.app_id
        date_str = request.args.get('date')
        start_date_str = request.args.get('start_date')

        as_of_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None

        if start_date and start_date > as_of_date:
            return jsonify({'error': 'Start date cannot be after end date'}), 400

        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({'error': 'Base currency not defined'}), 400

        base_currency_id = base_currency_info['base_currency_id']
        base_currency_code = base_currency_info['base_currency']

        accounts = db_session.query(ChartOfAccounts).filter_by(app_id=app_id, is_active=True).all()
        monetary_accounts = get_monetary_accounts(db_session, app_id)
        monetary_account_ids = [acct.id for acct in monetary_accounts]

        account_info = {
            a.id: {
                'type': a.parent_account_type,
                'parent': a.categories.category,
                'name': a.sub_category,
                'is_monetary': a.id in monetary_account_ids,
                'normal_balance': a.normal_balance
            }
            for a in accounts
        }

        currencies = db_session.query(Currency).filter(Currency.app_id == app_id).all()
        exchange_rates = {}
        for currency in currencies:
            if currency.id != base_currency_id:
                rate = get_latest_exchange_rate(
                    db_session, app_id, currency.id, base_currency_id, as_of_date
                )
                exchange_rates[currency.id] = rate if rate else 1.0

        balances = {}
        for acc_id, acc_data in account_info.items():
            balances[acc_id] = {
                'type': acc_data['type'],
                'parent': acc_data['parent'],
                'name': acc_data['name'],
                'is_monetary': acc_data['is_monetary'],
                'normal_balance': acc_data['normal_balance'],
                'currency_balances': defaultdict(lambda: {'debit': 0, 'credit': 0}),
                'fx_gain_loss': 0.0
            }

        def accumulate_entries(entries, is_opening):
            for entry in entries:
                acc_id = entry.subcategory_id
                if acc_id not in balances:
                    continue
                journal = entry.journal  # Get the journal header
                entry_data = balances[acc_id]
                is_monetary = entry_data['is_monetary']
                currency = journal.currency_id

                amount = convert_amount(
                    entry.amount,
                    currency,
                    base_currency_id,
                    exchange_rates,
                    is_monetary,
                    journal.date if is_opening else journal.date,
                    db_session,
                    txn_obj=journal
                )

                if entry.dr_cr == 'D':
                    entry_data['currency_balances'][base_currency_id]['debit'] += amount
                else:
                    entry_data['currency_balances'][base_currency_id]['credit'] += amount

                if is_monetary and currency != base_currency_id:
                    current_rate = exchange_rates.get(currency, 1.0)
                    historical_rate = get_historical_exchange_rate(
                        db_session,
                        journal
                    ) or current_rate
                    fx_diff = (current_rate - historical_rate) * float(entry.amount)
                    if entry.dr_cr == 'C':
                        fx_diff *= -1
                    entry_data['fx_gain_loss'] += fx_diff

        # Get opening entries (before start_date)
        if start_date:
            opening_entries_query = db_session.query(JournalEntry).join(
                Journal, Journal.id == JournalEntry.journal_id
            ).filter(
                Journal.app_id == app_id,
                Journal.status == 'Posted',
                Journal.date < start_date
            )
            opening_entries = opening_entries_query.all()
            accumulate_entries(opening_entries, is_opening=True)

        # Get period entries
        period_entries_query = db_session.query(JournalEntry).join(
            Journal, Journal.id == JournalEntry.journal_id
        ).filter(
            Journal.app_id == app_id,
            Journal.status == 'Posted',
            Journal.date <= as_of_date
        )
        if start_date:
            period_entries_query = period_entries_query.filter(Journal.date >= start_date)

        period_entries = period_entries_query.all()
        accumulate_entries(period_entries, is_opening=False)

        def get_net_balance(amounts, normal_balance):
            if normal_balance == 'Debit':
                return amounts['debit'] - amounts['credit']
            else:
                return amounts['credit'] - amounts['debit']

        formatted = defaultdict(list)
        summary = {'assets': 0, 'liabilities': 0, 'equity': 0}
        category_totals = defaultdict(float)

        def process_section(section):
            total = 0
            accounts_by_category = defaultdict(list)
            for acc_id, data in balances.items():
                if data['type'] != section:
                    continue
                amounts = data['currency_balances'][base_currency_id]
                net_base = get_net_balance(amounts, data['normal_balance'])
                accounts_by_category[data['parent']].append({
                    'type': section,
                    'category': data['parent'],
                    'subcategory': data['name'],
                    'balance': net_base,
                    'currency': base_currency_code,
                    'is_monetary': data['is_monetary'],
                })
                total += net_base
                category_totals[data['parent']] += net_base

            for category, accounts in accounts_by_category.items():
                for account in accounts:
                    formatted[section].append(account)

                formatted[section].append({
                    'type': section,
                    'category': category,
                    'subcategory': f'{category} Subtotal',
                    'balance': category_totals[category],
                    'currency': base_currency_code,
                    'is_monetary': False,
                    'is_subtotal': True
                })

            return total

        summary['assets'] = process_section('Asset')
        summary['liabilities'] = process_section('Liability')

        equity_total = 0
        retained_earnings_account = None
        for acc_id, data in balances.items():
            if (data['type'] == 'Equity' and
                    'retained earnings' in data['name'].lower()):
                retained_earnings_account = acc_id
                break

        net_income = calculate_net_income(
            db_session,
            app_id,
            base_currency_id,
            exchange_rates,
            as_of_date
        )

        total_fx_gain_loss = sum(
            data['fx_gain_loss']
            for data in balances.values()
            if data['is_monetary']
        )

        equity_accounts_by_category = defaultdict(list)
        for acc_id, data in balances.items():
            if data['type'] != 'Equity':
                continue

            amounts = data['currency_balances'][base_currency_id]
            net_base = get_net_balance(amounts, data['normal_balance'])

            if acc_id == retained_earnings_account:
                net_base += net_income

            if abs(net_base) < 0.01:
                continue

            equity_accounts_by_category[data['parent']].append({
                'type': 'Equity',
                'category': data['parent'],
                'subcategory': data['name'],
                'balance': net_base,
                'currency': base_currency_code,
                'is_monetary': True,
            })
            equity_total += net_base
            category_totals[data['parent']] += net_base

        if not retained_earnings_account and abs(net_income) > 0.01:
            equity_accounts_by_category['Equity'].append({
                'type': 'Equity',
                'category': 'Equity',
                'subcategory': 'Retained Earnings',
                'balance': net_income,
                'currency': base_currency_code,
                'is_monetary': True,
            })
            equity_total += net_income
            category_totals['Equity'] += net_income

        if abs(total_fx_gain_loss) > 0.01:
            equity_accounts_by_category['FX Revaluation'].append({
                'type': 'Equity',
                'category': 'FX Revaluation',
                'subcategory': 'Unrealized FX Gain/Loss',
                'balance': total_fx_gain_loss,
                'currency': base_currency_code,
                'is_monetary': False,
                'is_fx_component': True
            })
            equity_total += total_fx_gain_loss
            category_totals['FX Revaluation'] += total_fx_gain_loss

        for category, accounts in equity_accounts_by_category.items():
            for account in accounts:
                formatted['Equity'].append(account)

            formatted['Equity'].append({
                'type': 'Equity',
                'category': category,
                'subcategory': f'{category} Subtotal',
                'balance': category_totals[category],
                'currency': base_currency_code,
                'is_monetary': False,
                'is_subtotal': True
            })

        summary['equity'] = equity_total

        summary['components'] = {
            'net_income': net_income,
            'fx_gain_loss': total_fx_gain_loss
        }

        result = {
            'as_of': date_str,
            'start_date': start_date_str,
            'base_currency': base_currency_code,
            'categories': formatted,
            'totals': summary,
            'category_totals': dict(category_totals),
            'balanced': abs(summary['assets'] - (summary['liabilities'] + summary['equity'])) < 0.01
        }

        logger.info(f'Balance sheet result: {result}')
        return jsonify(result)

    except ValueError:
        return jsonify({'error': 'Invalid date or app_id format'}), 400
    except Exception as e:
        logger.error(f'Error generating balance sheet: {e}\n{traceback.format_exc()}')
        db_session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()


def convert_amount(amount, from_currency_id, to_currency_id, exchange_rates, is_monetary, txn_date, session,
                   txn_obj=None):
    """Convert amount to base currency using appropriate rate"""
    if from_currency_id == to_currency_id:
        return float(amount)

    if is_monetary:
        # For monetary items, use latest exchange rate as of balance sheet date
        rate = exchange_rates.get(from_currency_id, 1.0)
    else:
        # For non-monetary items, use historical rate at transaction date
        rate = get_historical_exchange_rate(
            session,
            txn_obj
        ) or 1.0
    logger.info(f'Amount is {amount} and rate {rate} and txn id {txn_obj.id if txn_obj else None}')

    return float(amount) * rate


def calculate_net_income(session, app_id, base_currency_id, exchange_rates, end_date):
    # Get all income journal entries up to end_date
    income_entries = session.query(JournalEntry).join(
        Journal, Journal.id == JournalEntry.journal_id
    ).join(
        ChartOfAccounts, ChartOfAccounts.id == JournalEntry.subcategory_id
    ).filter(
        Journal.app_id == app_id,
        Journal.status == 'Posted',  # Only include posted journals
        Journal.date <= end_date,
        ChartOfAccounts.parent_account_type == 'Income'
    ).all()

    # Get all expense journal entries up to end_date
    expense_entries = session.query(JournalEntry).join(
        Journal, Journal.id == JournalEntry.journal_id
    ).join(
        ChartOfAccounts, ChartOfAccounts.id == JournalEntry.subcategory_id
    ).filter(
        Journal.app_id == app_id,
        Journal.status == 'Posted',  # Only include posted journals
        Journal.date <= end_date,
        ChartOfAccounts.parent_account_type == 'Expense'
    ).all()

    net_income = 0

    # Sum income entries
    for entry in income_entries:
        journal = entry.journal  # Get the journal header
        is_monetary = False  # Income/Expense accounts are typically non-monetary
        amount = convert_amount(
            entry.amount,
            journal.currency_id,  # Currency from journal header
            base_currency_id,
            exchange_rates,
            is_monetary,
            journal.date,  # Date from journal header
            session,
            txn_obj=journal
        )
        # For income accounts: credit increases income, debit decreases income
        net_income += amount if entry.dr_cr == 'C' else -amount

    # Sum expense entries
    for entry in expense_entries:
        journal = entry.journal  # Get the journal header
        is_monetary = False  # Income/Expense accounts are typically non-monetary
        amount = convert_amount(
            entry.amount,
            journal.currency_id,  # Currency from journal header
            base_currency_id,
            exchange_rates,
            is_monetary,
            journal.date,  # Date from journal header
            session,
            txn_obj=journal
        )
        # For expense accounts: debit increases expense, credit decreases expense
        net_income -= amount if entry.dr_cr == 'D' else -amount

    return net_income


def get_latest_exchange_rate(session, app_id, from_currency_id, to_currency_id, as_of_date):
    """Get most recent exchange rate before as_of_date"""
    rate = session.query(ExchangeRate).filter(
        ExchangeRate.app_id == app_id,
        ExchangeRate.from_currency_id == from_currency_id,
        ExchangeRate.to_currency_id == to_currency_id,
        ExchangeRate.date <= as_of_date
    ).order_by(ExchangeRate.date.desc()).first()

    return float(rate.rate) if rate else None


def get_historical_exchange_rate(session, journal, from_currency_id=None, to_currency_id=None, txn_date=None):
    """
    Get exchange rate that was used for a journal transaction.
    Accepts either a Journal object or journal_id.
    """
    # If journal is an ID, get the journal object
    if isinstance(journal, int):
        journal_obj = session.query(Journal).options(
            joinedload(Journal.exchange_rate)
        ).filter(Journal.id == journal).first()
    else:
        # journal is already an object
        journal_obj = journal

    if not journal_obj:
        return None

    # First priority: Use the journal's exchange_rate relationship
    if journal_obj.exchange_rate:
        return float(journal_obj.exchange_rate.rate)

    # Second priority: Use the journal's exchange_rate_id (if relationship not loaded)
    if journal_obj.exchange_rate_id:
        exchange_rate = session.query(ExchangeRate).filter(
            ExchangeRate.id == journal_obj.exchange_rate_id
        ).first()
        if exchange_rate:
            return float(exchange_rate.rate)

    # Fallback: Historical lookup (only if parameters provided)
    if all([from_currency_id, to_currency_id, txn_date]):
        rate = session.query(ExchangeRate).filter(
            ExchangeRate.from_currency_id == from_currency_id,
            ExchangeRate.to_currency_id == to_currency_id,
            ExchangeRate.date == txn_date
        ).first()
        return float(rate.rate) if rate else None

    return None


@report_routes.route('/report/payable_accounts_report', methods=["GET"])
@login_required
def payable_accounts_report():
    try:
        with Session() as db_session:
            app_id = current_user.app_id
            company = db_session.query(Company).filter_by(id=app_id).first()
            role = current_user.role
            modules_data = [mod.module_name for mod in
                            db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
            currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

            # Get base currency information
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                flash('Base currency not defined for this company', 'danger')
                return redirect(request.referrer)

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency_code = base_currency_info["base_currency"]

            # Get filter parameters from request
            vendor_id = request.args.get('vendor_id', type=int)
            account_id = request.args.get('account_id', type=int)
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            currency_id_raw = request.args.get('currency', type=int)
            currency_id = empty_to_none(currency_id_raw)

            logger.info(f'Data information is {request.args}')

            # Get all payable accounts with optional filtering
            payable_query = db_session.query(ChartOfAccounts).filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_payable == True
            )

            if account_id:
                payable_query = payable_query.filter(ChartOfAccounts.id == account_id)

            payable_accounts = payable_query.all()

            payable_report = []
            total_company_payable = Decimal('0')

            for account in payable_accounts:
                # Build journal entry query with joins and filters
                entry_query = db_session.query(JournalEntry).join(
                    Journal, Journal.id == JournalEntry.journal_id
                ).filter(
                    JournalEntry.subcategory_id == account.id,
                    Journal.app_id == app_id,
                    Journal.status == 'Posted'  # Only include posted journals
                )

                # Apply filters if provided
                if vendor_id:
                    entry_query = entry_query.filter(Journal.vendor_id == vendor_id)

                if start_date:
                    entry_query = entry_query.filter(Journal.date >= start_date)

                if end_date:
                    entry_query = entry_query.filter(Journal.date <= end_date)

                if currency_id:
                    entry_query = entry_query.filter(Journal.currency_id == currency_id)

                entries = entry_query.order_by(
                    Journal.date.asc()
                ).all()

                # Calculate total balance for this payable account
                total_balance = Decimal('0')
                vendor_balances = {}
                display_currency = base_currency_code if not currency_id else None

                for entry in entries:
                    journal = entry.journal  # Get the journal header

                    if currency_id:
                        # Show amounts and balances directly in filtered currency without conversion
                        amount = Decimal(str(entry.amount))
                        balance_amount = amount
                        display_currency = journal.currency.user_currency if journal.currency else None
                    else:
                        # Convert amounts to base currency using historical rate by transaction date
                        amount = Decimal(str(entry.amount))
                        if journal.currency_id != base_currency_id:
                            amount = get_converted_cost(
                                db_session,
                                amount,
                                journal.currency_id,
                                base_currency_id,
                                app_id,
                                journal.date.strftime('%Y-%m-%d')
                            )
                        display_currency = base_currency_code

                        # For balances, convert using LATEST rate (at end_date or today)
                        balance_amount = Decimal(str(entry.amount))
                        if journal.currency_id != base_currency_id:
                            balance_amount = get_converted_cost(
                                db_session,
                                balance_amount,
                                journal.currency_id,
                                base_currency_id,
                                app_id,
                                end_date or datetime.date.today().strftime('%Y-%m-%d')
                            )

                    # Apply debit/credit signs
                    amount = amount if entry.dr_cr == 'C' else -amount
                    balance_amount = balance_amount if entry.dr_cr == 'C' else -balance_amount

                    # Initialize vendor data if not exists
                    vendor_id_key = journal.vendor_id
                    if vendor_id_key not in vendor_balances:
                        vendor_balances[vendor_id_key] = {
                            'vendor_info': {
                                'id': journal.vendor.id if journal.vendor else None,
                                'name': journal.vendor.vendor_name if journal.vendor else None,
                                'vendor_id': journal.vendor.vendor_id if journal.vendor else None,
                                'contact': journal.vendor.tel_contact if journal.vendor else None,
                                'email': journal.vendor.email if journal.vendor else None
                            },
                            'balance': Decimal('0'),
                            'transactions': []
                        }

                    # Update vendor balance — use balance_amount, which respects filtered currency logic now
                    vendor_balances[vendor_id_key]['balance'] += balance_amount

                    # Add transaction details
                    vendor_balances[vendor_id_key]['transactions'].append({
                        'journal_number': journal.journal_number,
                        'date': journal.date.strftime('%Y-%m-%d'),
                        'original_amount': float(-1 * entry.amount) if entry.dr_cr == "D" else float(entry.amount),
                        'original_currency': journal.currency.user_currency if journal.currency else None,
                        'converted_amount': float(amount),
                        'display_currency': display_currency,
                        'dr_cr': entry.dr_cr,
                        'description': entry.description or journal.narration,
                        'project_name': journal.project.name if journal.project else None,
                        'payment_mode': journal.payment_mode.payment_mode if journal.payment_mode else None,
                        'transaction_id': entry.id,
                        'reconciled': entry.reconciled,
                        'reconciliation_date': entry.reconciliation_date.strftime(
                            '%Y-%m-%d') if entry.reconciliation_date else None
                    })

                for vendor_id_key, vendor_data in vendor_balances.items():
                    vendor_data['transaction_count'] = len(vendor_data['transactions'])

                # Calculate account total balance
                account_total = sum(v['balance'] for v in vendor_balances.values()) if vendor_balances else Decimal('0')
                total_company_payable += account_total

                # Add account to report only if there are transactions or we're not filtering
                if entries or not (vendor_id or account_id or start_date or end_date or currency_id):
                    payable_report.append({
                        'account_id': account.id,
                        'account_name': account.sub_category,
                        'account_code': account.sub_category_id,
                        'total_balance': float(account_total),
                        'vendors': list(vendor_balances.values()) if vendor_balances else [],
                        'currency': display_currency
                    })

            payable_report.sort(key=lambda x: (
                # Primary key - determines order of groups (0=positive, 1=negative, 2=zero)
                0 if x['total_balance'] > 0 else (1 if x['total_balance'] < 0 else 2),

                # Secondary key - sorts within each group:
                -x['total_balance'] if x['total_balance'] > 0 else  # Positive: descending
                x['total_balance'] if x['total_balance'] < 0 else  # Negative: ascending
                0  # Zero: no sort
            ), reverse=False)  # Important: reverse=False maintains our custom ordering

            # Get all vendors and payable accounts for dropdown filters
            vendors = db_session.query(Vendor).filter_by(app_id=app_id).order_by(Vendor.vendor_name).all()
            all_payable_accounts = db_session.query(ChartOfAccounts).filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_payable == True
            ).order_by(ChartOfAccounts.sub_category).all()

            return render_template(
                'reports/payables_report.html',
                payable_report=payable_report,
                vendors=vendors,
                payable_accounts=all_payable_accounts,
                module_name="General Ledger",
                role=role,
                modules=modules_data,
                company=company,
                current_date=datetime.datetime.now().strftime('%Y-%m-%d'),
                base_currency=base_currency_code,
                total_company_payable=float(total_company_payable),
                filter_vendor_id=vendor_id,
                filter_account_id=account_id,
                filter_start_date=start_date,
                filter_end_date=end_date,
                filter_currency=currency_id,
                currencies=currencies
            )

    except Exception as e:
        logger.error(f'Error generating payable accounts report: {str(e)}', exc_info=True)
        flash(f'An error occurred while generating the report: {str(e)}', 'danger')
        return redirect(request.referrer)


@report_routes.route('/report/receivable_accounts_report', methods=["GET"])
@login_required
def receivable_accounts_report():
    try:
        with Session() as db_session:
            app_id = current_user.app_id
            company = db_session.query(Company).filter_by(id=app_id).first()
            role = current_user.role
            modules_data = [mod.module_name for mod in
                            db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
            currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

            # Get base currency information
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                flash('Base currency not defined for this company', 'danger')
                return redirect(request.referrer)

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency_code = base_currency_info["base_currency"]

            # Get filter parameters from request
            customer_id = request.args.get('customer_id', type=int)
            account_id = request.args.get('account_id', type=int)
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            currency_id_raw = request.args.get('currency', type=int)
            currency_id = empty_to_none(currency_id_raw)

            logger.info(f'Data information is {request.args}')

            # Get all receivable accounts with optional filtering
            receivable_query = db_session.query(ChartOfAccounts).filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_receivable == True
            )

            if account_id:
                receivable_query = receivable_query.filter(ChartOfAccounts.id == account_id)

            receivable_accounts = receivable_query.all()

            receivable_report = []
            total_company_receivable = Decimal('0')

            for account in receivable_accounts:
                # Build journal entry query with joins and filters
                entry_query = db_session.query(JournalEntry).join(
                    Journal, Journal.id == JournalEntry.journal_id
                ).filter(
                    JournalEntry.subcategory_id == account.id,
                    Journal.app_id == app_id,
                    Journal.status == 'Posted'  # Only include posted journals
                )

                # Apply filters if provided
                if customer_id:
                    entry_query = entry_query.filter(Journal.vendor_id == customer_id)

                if start_date:
                    entry_query = entry_query.filter(Journal.date >= start_date)

                if end_date:
                    entry_query = entry_query.filter(Journal.date <= end_date)

                if currency_id:
                    entry_query = entry_query.filter(Journal.currency_id == currency_id)

                entries = entry_query.order_by(
                    Journal.date.asc()
                ).all()

                # Calculate total balance for this receivable account
                total_balance = Decimal('0')
                customer_balances = {}
                display_currency = base_currency_code if not currency_id else None

                for entry in entries:
                    journal = entry.journal  # Get the journal header

                    if currency_id:
                        # Show amounts and balances directly in filtered currency without conversion
                        amount = Decimal(str(entry.amount))
                        balance_amount = amount
                        display_currency = journal.currency.user_currency if journal.currency else None
                    else:
                        # Convert amounts to base currency using historical rate by transaction date
                        amount = Decimal(str(entry.amount))
                        if journal.currency_id != base_currency_id:
                            amount = get_converted_cost(
                                db_session,
                                amount,
                                journal.currency_id,
                                base_currency_id,
                                app_id,
                                journal.date.strftime('%Y-%m-%d')
                            )
                        display_currency = base_currency_code

                        # For balances, convert using LATEST rate (at end_date or today)
                        balance_amount = Decimal(str(entry.amount))
                        if journal.currency_id != base_currency_id:
                            balance_amount = get_converted_cost(
                                db_session,
                                balance_amount,
                                journal.currency_id,
                                base_currency_id,
                                app_id,
                                end_date or datetime.date.today().strftime('%Y-%m-%d')
                            )

                    # Apply debit/credit signs (opposite of payables)
                    amount = amount if entry.dr_cr == 'D' else -amount
                    balance_amount = balance_amount if entry.dr_cr == 'D' else -balance_amount

                    # Initialize customer data if not exists
                    customer_id_key = journal.vendor_id
                    if customer_id_key not in customer_balances:
                        customer_balances[customer_id_key] = {
                            'customer_info': {
                                'id': journal.vendor.id if journal.vendor else None,
                                'name': journal.vendor.vendor_name if journal.vendor else None,
                                'customer_id': journal.vendor.vendor_id if journal.vendor else None,
                                'contact': journal.vendor.tel_contact if journal.vendor else None,
                                'email': journal.vendor.email if journal.vendor else None
                            },
                            'balance': Decimal('0'),
                            'transactions': []
                        }

                    # Update customer balance
                    customer_balances[customer_id_key]['balance'] += balance_amount

                    # Add transaction details
                    customer_balances[customer_id_key]['transactions'].append({
                        'journal_number': journal.journal_number,
                        'date': journal.date.strftime('%Y-%m-%d'),
                        'original_amount': float(entry.amount) if entry.dr_cr == "D" else float(-1 * entry.amount),
                        'original_currency': journal.currency.user_currency if journal.currency else None,
                        'converted_amount': float(amount),
                        'display_currency': display_currency,
                        'dr_cr': entry.dr_cr,
                        'description': entry.description or journal.narration,
                        'project_name': journal.project.name if journal.project else None,
                        'payment_mode': journal.payment_mode.payment_mode if journal.payment_mode else None,
                        'transaction_id': entry.id,
                        'reconciled': entry.reconciled,
                        'reconciliation_date': entry.reconciliation_date.strftime(
                            '%Y-%m-%d') if entry.reconciliation_date else None
                    })

                for customer_id_key, customer_data in customer_balances.items():
                    customer_data['transaction_count'] = len(customer_data['transactions'])

                # Calculate account total balance
                account_total = sum(v['balance'] for v in customer_balances.values()) if customer_balances else Decimal(
                    '0')
                total_company_receivable += account_total

                # Add account to report only if there are transactions or we're not filtering
                if entries or not (customer_id or account_id or start_date or end_date or currency_id):
                    receivable_report.append({
                        'account_id': account.id,
                        'account_name': account.sub_category,
                        'account_code': account.sub_category_id,
                        'total_balance': float(account_total),
                        'customers': list(customer_balances.values()) if customer_balances else [],
                        'currency': display_currency
                    })

            receivable_report.sort(key=lambda x: (
                # Primary key - determines order of groups (0=positive, 1=negative, 2=zero)
                0 if x['total_balance'] > 0 else (1 if x['total_balance'] < 0 else 2),

                # Secondary key - sorts within each group:
                -x['total_balance'] if x['total_balance'] > 0 else  # Positive: descending
                x['total_balance'] if x['total_balance'] < 0 else  # Negative: ascending
                0  # Zero: no sort
            ), reverse=False)

            # Get all customers and receivable accounts for dropdown filters
            customers = db_session.query(Vendor).filter_by(app_id=app_id).order_by(Vendor.vendor_name).all()
            all_receivable_accounts = db_session.query(ChartOfAccounts).filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_receivable == True
            ).order_by(ChartOfAccounts.sub_category).all()

            return render_template(
                'reports/receivables_report.html',
                receivable_report=receivable_report,
                customers=customers,
                receivable_accounts=all_receivable_accounts,
                module_name="General Ledger",
                role=role,
                modules=modules_data,
                company=company,
                current_date=datetime.datetime.now().strftime('%Y-%m-%d'),
                base_currency=base_currency_code,
                total_company_receivable=float(total_company_receivable),
                filter_customer_id=customer_id,
                filter_account_id=account_id,
                filter_start_date=start_date,
                filter_end_date=end_date,
                filter_currency=currency_id,
                currencies=currencies
            )

    except Exception as e:
        logger.error(f'Error generating receivable accounts report: {str(e)}', exc_info=True)
        flash(f'An error occurred while generating the report: {str(e)}', 'danger')
        return redirect(request.referrer)


@report_routes.route('/account_details/<int:account_id>', methods=['GET'])
@login_required
def account_details(account_id):
    try:
        with Session() as db_session:
            app_id = current_user.app_id
            company = db_session.query(Company).filter_by(id=app_id).first()
            role = current_user.role
            modules_data = [
                mod.module_name
                for mod in db_session.query(Module)
                .filter_by(app_id=app_id, included='yes')
                .all()
            ]
            currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
            # Fetch accounts list first
            accounts = db_session.query(ChartOfAccounts).filter_by(app_id=app_id).all()

            grouped_accounts = {}
            for acct in accounts:
                parent_type = acct.parent_account_type or 'Others'
                category = acct.category or 'Uncategorized'

                if parent_type not in grouped_accounts:
                    grouped_accounts[parent_type] = {}

                if category not in grouped_accounts[parent_type]:
                    grouped_accounts[parent_type][category] = []

                grouped_accounts[parent_type][category].append(acct)

            # Get base currency information
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                flash('Base currency not defined for this company', 'danger')
                return redirect(request.referrer)

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency_code = base_currency_info["base_currency"]

            # Get filter parameters
            start_date_str = request.args.get('start_date')
            end_date_str = request.args.get('end_date')
            currency_filter = request.args.get('currency_id', type=int)

            query_account_id = request.args.get('account_id', type=int)
            if query_account_id:
                account_id = query_account_id

            currency_filter_applied = True
            if not currency_filter:
                currency_filter_applied = False
                currency_filter = base_currency_id  # Default to base currency

            start_date = None
            end_date = None
            if start_date_str:
                start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
            if end_date_str:
                end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()

            # **Set default to current month if no dates provided**
            if not start_date and not end_date:
                today = datetime.date.today()
                start_date = today.replace(day=1)
                last_day = calendar.monthrange(today.year, today.month)[1]
                end_date = today.replace(day=last_day)

            # Get account details
            account = db_session.query(ChartOfAccounts).filter_by(id=account_id, app_id=app_id).first()
            if not account:
                flash('Account not found', 'danger')
                return redirect(request.referrer)

            # Build journal entry query with currency filter
            entry_query = db_session.query(JournalEntry).join(
                Journal, Journal.id == JournalEntry.journal_id
            ).filter(
                JournalEntry.subcategory_id == account_id,
                Journal.app_id == app_id,
                Journal.status == 'Posted',  # Only include posted journals
                Journal.currency_id == currency_filter
            )

            if start_date:
                entry_query = entry_query.filter(Journal.date >= start_date)
            if end_date:
                entry_query = entry_query.filter(Journal.date <= end_date)

            entries = entry_query.order_by(Journal.date.asc()).all()

            # Calculate starting balance before filter period
            starting_balance_result = Decimal('0')  # Default to zero

            if start_date:
                starting_balance_query = db_session.query(
                    func.sum(
                        case(
                            (and_(JournalEntry.subcategory_id == account_id, JournalEntry.dr_cr == 'D'),
                             JournalEntry.amount),
                            (and_(JournalEntry.subcategory_id == account_id, JournalEntry.dr_cr == 'C'),
                             -JournalEntry.amount),
                            else_=0
                        )
                    )
                ).join(
                    Journal, Journal.id == JournalEntry.journal_id
                ).filter(
                    JournalEntry.subcategory_id == account_id,
                    Journal.app_id == app_id,
                    Journal.status == 'Posted',
                    Journal.currency_id == currency_filter,
                    Journal.date < start_date
                )

                starting_balance_result = starting_balance_query.scalar() or Decimal('0')

            # Adjust for account's normal balance
            if not start_date:
                starting_balance = Decimal('0')  # Explicitly zero if no start_date filter
            else:
                if account.normal_balance == 'Credit':
                    starting_balance = -Decimal(str(starting_balance_result))
                else:
                    starting_balance = Decimal(str(starting_balance_result))

            # Running balance calculation
            running_balance = starting_balance
            transaction_data = []

            for entry in entries:
                journal = entry.journal  # Get the journal header
                amount = Decimal(str(entry.amount))
                if account.normal_balance == 'Debit':
                    amount = amount if entry.dr_cr == 'D' else -amount
                else:
                    amount = -amount if entry.dr_cr == 'D' else amount

                running_balance += amount

                transaction_data.append({
                    'id': entry.id,
                    'date': journal.date.strftime('%Y-%m-%d'),
                    'journal_number': journal.journal_number,
                    'description': entry.description or journal.narration,
                    'original_amount': float(entry.amount),
                    'original_currency': journal.currency.user_currency if journal.currency else None,
                    'dr_cr': entry.dr_cr,
                    'running_balance': float(running_balance),
                    'project': journal.project.name if journal.project else None,
                    'payment_mode': journal.payment_mode.payment_mode if journal.payment_mode else None,
                    'vendor': journal.vendor.vendor_name if journal.vendor else None,
                    'customer': journal.vendor.vendor_name if journal.vendor else None,
                    'reconciled': entry.reconciled,
                    'reconciliation_date': entry.reconciliation_date.strftime(
                        '%Y-%m-%d') if entry.reconciliation_date else None
                })

            # Get min/max dates
            min_max_dates = db_session.query(
                func.min(Journal.date),
                func.max(Journal.date)
            ).join(
                JournalEntry, Journal.id == JournalEntry.journal_id
            ).filter(
                JournalEntry.subcategory_id == account_id,
                Journal.app_id == app_id,
                Journal.status == 'Posted',
                Journal.currency_id == currency_filter
            ).first()

            display_currency = db_session.query(Currency.user_currency).filter_by(app_id=app_id,
                                                                                  id=currency_filter).scalar()

            return render_template(
                'reports/account_details.html',
                account=account,
                transactions=transaction_data,
                starting_balance=float(starting_balance),
                ending_balance=float(running_balance),
                base_currency=base_currency_code,
                role=role,
                modules=modules_data,
                company=company,
                module_name="General Ledger",
                filter_start_date=start_date.strftime('%Y-%m-%d') if start_date else None,
                filter_end_date=end_date.strftime('%Y-%m-%d') if end_date else None,
                min_date=min_max_dates[0].strftime('%Y-%m-%d') if min_max_dates[0] else None,
                max_date=min_max_dates[1].strftime('%Y-%m-%d') if min_max_dates[1] else None,
                display_currency=display_currency,
                currency_filter_applied=currency_filter_applied,
                account_filter=query_account_id,
                currencies=currencies,
                grouped_accounts=grouped_accounts
            )

    except Exception as e:
        logger.error(f'Error generating account details for account {account_id}: {str(e)}', exc_info=True)
        flash(f'An error occurred while generating the report: {str(e)}', 'danger')
        return redirect(request.referrer)
