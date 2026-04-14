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
from services.chart_of_accounts_helpers import get_retained_earnings_account_id
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
        as_of_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()

        # Get currency data from query parameters (passed from template)
        base_currency_id = request.args.get('base_currency_id')
        base_currency_code = request.args.get('base_currency')
        currency_map_json = request.args.get('currency_map')
        foreign_currency_ids_json = request.args.get('foreign_currency_ids')

        # Parse the JSON data
        import json

        # Parse currency map
        if currency_map_json:
            try:
                currency_map = json.loads(currency_map_json)
            except json.JSONDecodeError:
                return jsonify({'error': 'Invalid currency_map JSON'}), 400
        else:
            return jsonify({'error': 'Missing currency_map parameter'}), 400

        # Parse foreign currency IDs
        if foreign_currency_ids_json:
            try:
                foreign_currency_ids = json.loads(foreign_currency_ids_json)
            except json.JSONDecodeError:
                return jsonify({'error': 'Invalid foreign_currency_ids JSON'}), 400
        else:
            return jsonify({'error': 'Missing foreign_currency_ids parameter'}), 400

        # Validate base currency
        if not base_currency_id or not base_currency_code:
            return jsonify({'error': 'Missing base currency parameters'}), 400

        base_currency_id = int(base_currency_id)

        # Verify base currency exists in the currency map
        if str(base_currency_id) not in currency_map:
            return jsonify({'error': f'Base currency ID {base_currency_id} not found in currency map'}), 400

        # OPTIMIZATION: Cache frequently used values
        base_currency_id_int = base_currency_id
        base_currency_code_str = base_currency_code

        # OPTIMIZATION 1: Load accounts with specific fields only
        accounts = db_session.query(
            ChartOfAccounts.id,
            ChartOfAccounts.parent_account_type,
            ChartOfAccounts.normal_balance,
            ChartOfAccounts.category,
            ChartOfAccounts.sub_category,
            ChartOfAccounts.is_cash,  # Add this
            ChartOfAccounts.is_bank,  # Add this
            ChartOfAccounts.is_receivable,  # Add this
            ChartOfAccounts.is_payable,  # Add this
            ChartOfAccounts.is_system_account
        ).filter_by(
            app_id=app_id,
            is_active=True
        ).all()

        # Create account info with optimized structure
        account_info = {}
        monetary_account_ids = set()
        account_types = {}

        # When loading accounts, determine is_monetary based on account flags
        for a in accounts:
            acc_id = a.id
            # Monetary if it's cash, bank, receivable, or payable
            is_monetary = a.is_cash or a.is_bank or a.is_receivable or a.is_payable

            account_info[acc_id] = {
                'type': a.parent_account_type,
                'parent': a.category,
                'name': a.sub_category,
                'is_monetary': is_monetary,  # Use the derived value
                'is_system_account': a.is_system_account,
                'normal_balance': a.normal_balance
            }
            if is_monetary:
                monetary_account_ids.add(acc_id)


        # Get exchange rates using passed foreign_currency_ids
        exchange_rates = {}
        if foreign_currency_ids:
            # Convert string IDs to integers
            currency_ids = [int(cid) for cid in foreign_currency_ids]
            batch_rates = get_latest_exchange_rates_batch(
                db_session, app_id, currency_ids, base_currency_id_int, as_of_date
            )
            exchange_rates.update(batch_rates)
        logger.info(f'Exchange rates are {exchange_rates}')
        # Add base currency (rate = 1.0)
        exchange_rates[base_currency_id_int] = 1.0

        # Initialize balances with pre-computed fields
        balances = {
            acc_id: {
                'type': data['type'],
                'parent': data['parent'],
                'name': data['name'],
                'is_monetary': data['is_monetary'],
                'is_system_account': data['is_system_account'],
                'normal_balance': data['normal_balance'],
                'currency_balances': {base_currency_id_int: {'debit': 0.0, 'credit': 0.0}},
                'fx_gain_loss': 0.0,
                'net_balance': 0.0,  # Pre-computed net balance
                'debit_total': 0.0,  # Track separately for faster access
                'credit_total': 0.0,
            }
            for acc_id, data in account_info.items()
        }

        # === OPTIMIZATION: SINGLE QUERY FOR ALL DATA ===
        from sqlalchemy import case

        # OPTIMIZATION: Use exists() for monetary check if supported by database
        monetary_accounts_list = list(monetary_account_ids)

        combined_query = db_session.query(
            JournalEntry.subcategory_id,
            Journal.currency_id,
            JournalEntry.dr_cr,
            Journal.exchange_rate_id,
            func.sum(JournalEntry.amount).label('total_amount'),
            func.max(
                case(
                    (JournalEntry.subcategory_id.in_(monetary_accounts_list), 1),
                    else_=0
                )
            ).label('is_monetary_flag')
        ).join(
            Journal, Journal.id == JournalEntry.journal_id
        ).filter(
            Journal.app_id == app_id,
            Journal.status == 'Posted',
            Journal.date <= as_of_date
        ).group_by(
            JournalEntry.subcategory_id,
            Journal.currency_id,
            JournalEntry.dr_cr,
            Journal.exchange_rate_id
        ).all()

        # OPTIMIZATION: Collect and pre-load ALL exchange_rate_ids in one pass
        all_exchange_rate_ids = set()
        exchange_rate_mapping = {}

        for row in combined_query:
            subcategory_id, currency_id, dr_cr, exchange_rate_id, total_amount, is_monetary_flag = row
            if currency_id != base_currency_id_int and exchange_rate_id:
                all_exchange_rate_ids.add(exchange_rate_id)
            # Store mapping for quick access
            exchange_rate_mapping[(subcategory_id, currency_id, dr_cr, exchange_rate_id)] = (
                total_amount, is_monetary_flag)

        # Load ALL exchange rates at once with batch optimization
        exchange_rate_cache = {}
        if all_exchange_rate_ids:
            rate_ids_list = list(all_exchange_rate_ids)
            # Process in batches for very large datasets
            batch_size = 1000
            for i in range(0, len(rate_ids_list), batch_size):
                batch_ids = rate_ids_list[i:i + batch_size]
                rate_objs = db_session.query(
                    ExchangeRate.id,
                    ExchangeRate.rate
                ).filter(
                    ExchangeRate.id.in_(batch_ids)
                ).all()
                exchange_rate_cache.update({rate_id: float(rate) for rate_id, rate in rate_objs})

        # OPTIMIZATION: Process ALL results in one loop with minimal lookups
        for (subcategory_id, currency_id, dr_cr, exchange_rate_id), (
                total_amount, is_monetary_flag) in exchange_rate_mapping.items():
            if subcategory_id not in balances:
                continue

            acc_data = balances[subcategory_id]
            is_monetary = bool(is_monetary_flag)
            total_amount_float = float(total_amount)

            # Convert to base currency with minimal branching
            if currency_id == base_currency_id_int:
                converted_amount = total_amount_float
            else:
                if is_monetary:
                    # Monetary: use latest rate
                    rate = exchange_rates.get(currency_id, 1.0)
                else:
                    # Non-monetary: use journal's historical rate from cache
                    rate = exchange_rate_cache.get(exchange_rate_id, exchange_rates.get(currency_id, 1.0))

                converted_amount = total_amount_float * rate

            # Add to appropriate balance with direct field access
            if dr_cr == 'D':
                acc_data['currency_balances'][base_currency_id_int]['debit'] += converted_amount
                acc_data['debit_total'] += converted_amount
            else:
                acc_data['currency_balances'][base_currency_id_int]['credit'] += converted_amount
                acc_data['credit_total'] += converted_amount

            # Calculate FX gain/loss for monetary foreign currency entries
            if is_monetary and currency_id != base_currency_id_int:
                current_rate = exchange_rates.get(currency_id, 1.0)
                historical_rate = exchange_rate_cache.get(exchange_rate_id, current_rate)

                # ADD THIS - Log every foreign currency entry regardless of rate difference
                logger.info(f"=== FOREIGN CURRENCY ENTRY ===")
                logger.info(f"Account: {acc_data['name']} (ID: {subcategory_id})")
                logger.info(f"Original Amount: {total_amount_float} {currency_map.get(str(currency_id), 'Unknown')}")
                logger.info(f"DR/CR: {dr_cr}")
                logger.info(f"Current Rate: {current_rate}")
                logger.info(f"Historical Rate: {historical_rate}")
                logger.info(f"Rate Difference: {current_rate - historical_rate}")


                if current_rate != historical_rate:
                    fx_diff = (current_rate - historical_rate) * total_amount_float
                    if dr_cr == 'C':
                        fx_diff *= -1
                    acc_data['fx_gain_loss'] += fx_diff



        # OPTIMIZATION: Compute net balances once for all accounts
        # OPTIMIZATION: Compute net balances once for all accounts
        for acc_id, data in balances.items():
            account_type = data['type']

            # FUCK normal_balance, use account type rules
            if account_type == 'Asset':
                # Assets: Debits increase, Credits decrease
                data['net_balance'] = data['debit_total'] - data['credit_total']
            elif account_type == 'Liability':
                # Liabilities: Credits increase, Debits decrease
                data['net_balance'] = data['credit_total'] - data['debit_total']
            elif account_type == 'Equity':
                # Equity: Credits increase, Debits decrease
                data['net_balance'] = data['credit_total'] - data['debit_total']
            elif account_type == 'Income':
                # Income: Credits increase, Debits decrease
                data['net_balance'] = data['credit_total'] - data['debit_total']
            elif account_type == 'Expense':
                # Expenses: Debits increase, Credits decrease
                data['net_balance'] = data['debit_total'] - data['credit_total']

        # OPTIMIZATION: Use list comprehensions and generators for faster processing
        formatted = defaultdict(list)
        summary = {'assets': 0.0, 'liabilities': 0.0, 'equity': 0.0,
                   'components': {'net_income': 0.0, 'fx_gain_loss': 0.0}}

        # Handle suspense account - move its balance to FX Gain/Loss
        for acc_id, data in balances.items():
            if 'Suspense' in data['name']:
                suspense_balance = data['net_balance']
                if abs(suspense_balance) > 0.01:
                    # Add to FX Gain/Loss
                    summary['components']['fx_gain_loss'] -= suspense_balance
                    # Zero out suspense
                    data['net_balance'] = 0
                    data['debit_total'] = 0
                    data['credit_total'] = 0
                    logger.info(f"Moved suspense balance {suspense_balance} to FX Gain/Loss")
                break
        category_totals = defaultdict(float)

        # OPTIMIZATION: Single-pass through all accounts with minimal conditionals
        system_retained_earnings_account = None
        accounts_by_type_category = defaultdict(lambda: defaultdict(list))

        # First pass: collect all data and calculate totals
        for acc_id, data in balances.items():
            account_type = data['type']
            net_balance = data['net_balance']
            is_system = data['is_system_account']

            # Update summary totals
            if account_type == 'Asset':
                summary['assets'] += net_balance
            elif account_type == 'Liability':
                summary['liabilities'] += net_balance
            elif account_type == 'Equity':
                summary['equity'] += net_balance
            elif account_type == 'Income':
                summary['components']['net_income'] += net_balance
            elif account_type == 'Expense':
                summary['components']['net_income'] -= net_balance

            # Track FX gain/loss
            if data['is_monetary']:
                summary['components']['fx_gain_loss'] += data['fx_gain_loss']

            # Find ONLY SYSTEM retained earnings account
            # Ignore user-created accounts even if named "retained earnings"
            if account_type == 'Equity' and 'retained earnings' in data['name'].lower() and is_system:
                system_retained_earnings_account = acc_id
                # Store the net balance for later adjustment
                retained_earnings_balance = net_balance

            # Skip formatting for income/expense accounts
            if account_type in ['Income', 'Expense']:
                continue

            # Prepare account entry
            category = data['parent']
            account_entry = {
                'type': account_type,
                'category': category,
                'subcategory': data['name'],
                'balance': net_balance,
                'currency': base_currency_code_str,
                'is_monetary': data['is_monetary'],
                'is_system_account': is_system,  # Add this flag
                'account_id': acc_id  # Add account ID for reference
            }

            # Group by type and category for efficient processing
            accounts_by_type_category[account_type][category].append(account_entry)
            category_totals[category] += net_balance

        # OPTIMIZATION: Build formatted structure efficiently
        for account_type in ['Asset', 'Liability', 'Equity']:
            if account_type not in accounts_by_type_category:
                continue

            formatted_section = []
            type_categories = accounts_by_type_category[account_type]

            for category, accounts in type_categories.items():
                if accounts:  # Only process if there are accounts
                    formatted_section.extend(accounts)

                    # Add category subtotal
                    category_total = sum(acc['balance'] for acc in accounts)
                    formatted_section.append({
                        'type': account_type,
                        'category': category,
                        'subcategory': f'{category} Subtotal',
                        'balance': category_total,
                        'currency': base_currency_code_str,
                        'is_monetary': False,
                        'is_subtotal': True
                    })

            # Add section total if there are accounts
            if formatted_section:
                section_total = sum(acc['balance'] for acc in formatted_section if not acc.get('is_subtotal', False))
                formatted_section.append({
                    'type': account_type,
                    'category': account_type,
                    'subcategory': f'{account_type} Total',
                    'balance': section_total,
                    'currency': base_currency_code_str,
                    'is_monetary': False,
                    'is_subtotal': True
                })

                formatted[account_type] = formatted_section

        # OPTIMIZATION: Handle equity adjustments efficiently
        net_income = summary['components']['net_income']
        total_fx_gain_loss = summary['components']['fx_gain_loss']

        # Handle net income - ONLY use SYSTEM retained earnings account
        if abs(net_income) > 0.01:
            if system_retained_earnings_account:
                # Update SYSTEM retained earnings account in formatted data
                for i, item in enumerate(formatted.get('Equity', [])):
                    if item.get('account_id') == system_retained_earnings_account:
                        formatted['Equity'][i]['balance'] += net_income
                        summary['equity'] += net_income
                        break
            else:
                # No system retained earnings exists - create one
                new_retained_earnings_account_id = get_retained_earnings_account_id(
                    db_session, app_id, current_user.id
                )
                db_session.commit()
                # Create account entry for the new SYSTEM retained earnings
                retained_earnings_entry = {
                    'type': 'Equity',
                    'category': 'System Equity',
                    'subcategory': 'Retained Earnings',
                    'balance': net_income,
                    'currency': base_currency_code_str,
                    'is_monetary': True,
                    'is_system_account': True,
                    'account_id': new_retained_earnings_account_id
                }

                # Add to formatted equity section
                if 'Equity' not in formatted:
                    formatted['Equity'] = []

                # Find position to insert (before subtotals)
                insert_index = None
                for i, item in enumerate(formatted['Equity']):
                    if item.get('is_subtotal', False):
                        insert_index = i
                        break

                if insert_index is not None:
                    formatted['Equity'].insert(insert_index, retained_earnings_entry)
                else:
                    formatted['Equity'].append(retained_earnings_entry)

                summary['equity'] += net_income
                category_totals['System Equity'] = category_totals.get('System Equity', 0) + net_income

                # Also add to balances dict for consistency
                balances[new_retained_earnings_account_id] = {
                    'type': 'Equity',
                    'parent': 'System Equity',
                    'name': 'Retained Earnings',
                    'is_monetary': True,
                    'is_system_account': True,
                    'normal_balance': 'Credit',
                    'currency_balances': {base_currency_id_int: {'debit': 0.0, 'credit': 0.0}},
                    'fx_gain_loss': 0.0,
                    'net_balance': net_income,
                    'debit_total': 0.0,
                    'credit_total': 0.0
                }

        # Add FX gain/loss
        if abs(total_fx_gain_loss) > 0.01:
            if 'Equity' in formatted:
                # Insert FX gain/loss before equity subtotals
                insert_index = None
                for i, item in enumerate(formatted['Equity']):
                    if item.get('is_subtotal', False):
                        insert_index = i
                        break

                fx_entry = {
                    'type': 'Equity',
                    'category': 'System Equity',  # Changed from 'FX Revaluation'
                    'subcategory': 'Unrealized FX Gain/Loss',
                    'balance': total_fx_gain_loss,
                    'currency': base_currency_code_str,
                    'is_monetary': False,
                    'is_fx_component': True
                }

                if insert_index is not None:
                    formatted['Equity'].insert(insert_index, fx_entry)
                else:
                    formatted['Equity'].append(fx_entry)

                summary['equity'] += total_fx_gain_loss
                category_totals['System Equity'] = category_totals.get('System Equity', 0) + total_fx_gain_loss

        # OPTIMIZATION: Recalculate equity total after adjustments
        if 'Equity' in formatted:
            # Remove old subtotals
            equity_items = [item for item in formatted['Equity'] if not item.get('is_subtotal', False)]

            # Group by category
            equity_by_category = defaultdict(list)
            for item in equity_items:
                equity_by_category[item['category']].append(item)

            # Rebuild equity section with proper subtotals
            new_equity_section = []
            equity_total = 0.0

            for category, accounts in equity_by_category.items():
                # Add accounts for this category
                new_equity_section.extend(accounts)

                # Add category subtotal
                category_total = sum(acc['balance'] for acc in accounts)
                new_equity_section.append({
                    'type': 'Equity',
                    'category': category,
                    'subcategory': f'{category} Subtotal',
                    'balance': category_total,
                    'currency': base_currency_code_str,
                    'is_monetary': False,
                    'is_subtotal': True
                })

                equity_total += category_total

            # Add equity total
            new_equity_section.append({
                'type': 'Equity',
                'category': 'Equity',
                'subcategory': 'Equity Total',
                'balance': equity_total,
                'currency': base_currency_code_str,
                'is_monetary': False,
                'is_subtotal': True
            })

            formatted['Equity'] = new_equity_section

        # Final result with optimized structure
        result = {
            'as_of': date_str,
            'base_currency': base_currency_code_str,
            'categories': dict(formatted),  # Convert defaultdict to regular dict
            'totals': summary,
            'category_totals': dict(category_totals),
            'balanced': abs(summary['assets'] - (summary['liabilities'] + summary['equity'])) < 0.01
        }

        logger.info(f'Balance sheet generated successfully for date: {date_str}')
        return jsonify(result)

    except ValueError as e:
        logger.error(f'Value error in balance sheet: {e}')
        return jsonify({'error': 'Invalid date or parameter format'}), 400
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


def get_latest_exchange_rates_batch(session, app_id, from_currency_ids, to_currency_id, as_of_date):
    """Get latest exchange rates for multiple currencies at once"""
    if not from_currency_ids:
        return {}

    # First, get the latest date for each currency
    from sqlalchemy import func

    # Subquery to find the latest date for each currency
    latest_dates = session.query(
        ExchangeRate.from_currency_id,
        func.max(ExchangeRate.date).label('max_date')
    ).filter(
        ExchangeRate.app_id == app_id,
        ExchangeRate.from_currency_id.in_(from_currency_ids),
        ExchangeRate.to_currency_id == to_currency_id,
        ExchangeRate.date <= as_of_date
    ).group_by(
        ExchangeRate.from_currency_id
    ).subquery()

    # Now get the actual rates for those dates
    rates = session.query(
        ExchangeRate.from_currency_id,
        ExchangeRate.rate
    ).join(
        latest_dates,
        (ExchangeRate.from_currency_id == latest_dates.c.from_currency_id) &
        (ExchangeRate.date == latest_dates.c.max_date)
    ).filter(
        ExchangeRate.app_id == app_id,
        ExchangeRate.to_currency_id == to_currency_id
    ).all()

    # Convert to dictionary: {currency_id: rate}
    return {rate.from_currency_id: float(rate.rate) for rate in rates}


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

            # For display purposes - whether currency filter is applied
            currency_filter_applied = True if currency_filter else False

            start_date = None
            end_date = None
            if start_date_str:
                start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
            if end_date_str:
                end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()

            # Set default to current month if no dates provided
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

            # Get all journal entries for this account
            entry_query = db_session.query(JournalEntry).join(
                Journal, Journal.id == JournalEntry.journal_id
            ).filter(
                JournalEntry.subcategory_id == account_id,
                Journal.app_id == app_id,
                Journal.status == 'Posted'
            )

            if start_date:
                entry_query = entry_query.filter(Journal.date >= start_date)
            if end_date:
                entry_query = entry_query.filter(Journal.date <= end_date)

            entries = entry_query.order_by(Journal.date.asc()).all()

            # Calculate starting balance before filter period
            starting_balance_result = Decimal('0')

            if start_date:
                starting_balance_query = db_session.query(
                    JournalEntry
                ).join(
                    Journal, Journal.id == JournalEntry.journal_id
                ).filter(
                    JournalEntry.subcategory_id == account_id,
                    Journal.app_id == app_id,
                    Journal.status == 'Posted',
                    Journal.date < start_date
                ).all()

                for entry in starting_balance_query:
                    journal = entry.journal
                    amount = Decimal(str(entry.amount))

                    # Convert to base currency using journal's historical rate
                    if journal.currency_id != base_currency_id:
                        if journal.exchange_rate:
                            rate = Decimal(str(journal.exchange_rate.rate))
                            amount = amount * rate
                        else:
                            # Fallback - should not happen for foreign currency
                            amount = amount

                    if entry.dr_cr == 'D':
                        starting_balance_result += amount
                    else:
                        starting_balance_result -= amount

            # Adjust for account's normal balance
            if not start_date:
                starting_balance = Decimal('0')
            else:
                if account.normal_balance == 'Credit':
                    starting_balance = -starting_balance_result
                else:
                    starting_balance = starting_balance_result

            # Running balance calculation in base currency
            running_balance = starting_balance
            transaction_data = []

            for entry in entries:
                journal = entry.journal
                original_amount = Decimal(str(entry.amount))

                # Convert to base currency using journal's historical rate
                converted_amount = original_amount
                if journal.currency_id != base_currency_id:
                    if journal.exchange_rate:
                        rate = Decimal(str(journal.exchange_rate.rate))
                        converted_amount = original_amount * rate
                    else:
                        # Fallback - should not happen for foreign currency
                        converted_amount = original_amount

                # Calculate running balance in base currency
                if account.normal_balance == 'Debit':
                    balance_change = converted_amount if entry.dr_cr == 'D' else -converted_amount
                else:
                    balance_change = -converted_amount if entry.dr_cr == 'D' else converted_amount

                running_balance += balance_change

                # Determine debit/credit in base currency
                if entry.dr_cr == 'D':
                    debit_amount = float(converted_amount)
                    credit_amount = 0.0
                else:
                    debit_amount = 0.0
                    credit_amount = float(converted_amount)

                transaction_data.append({
                    'id': entry.id,
                    'date': journal.date.strftime('%Y-%m-%d'),
                    'journal_number': journal.journal_number,
                    'description': entry.description or journal.narration,
                    'original_amount': float(original_amount),
                    'original_currency': journal.currency.user_currency if journal.currency else None,
                    'dr_cr': entry.dr_cr,
                    'debit': debit_amount,
                    'credit': credit_amount,
                    'running_balance': float(running_balance),
                    'project': journal.project.name if journal.project else None,
                    'payment_mode': journal.payment_mode.payment_mode if journal.payment_mode else None,
                    'vendor': journal.vendor.vendor_name if journal.vendor else None,
                    'customer': journal.vendor.vendor_name if journal.vendor else None,
                    'reconciled': entry.reconciled,
                    'reconciliation_date': entry.reconciliation_date.strftime(
                        '%Y-%m-%d') if entry.reconciliation_date else None
                })

            # Get min/max dates for the account
            min_max_dates = db_session.query(
                func.min(Journal.date),
                func.max(Journal.date)
            ).join(
                JournalEntry, Journal.id == JournalEntry.journal_id
            ).filter(
                JournalEntry.subcategory_id == account_id,
                Journal.app_id == app_id,
                Journal.status == 'Posted'
            ).first()

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
                display_currency=base_currency_code,
                currency_filter_applied=currency_filter_applied,
                account_filter=query_account_id,
                currencies=currencies,
                grouped_accounts=grouped_accounts
            )

    except Exception as e:
        logger.error(f'Error generating account details for account {account_id}: {str(e)}', exc_info=True)
        flash(f'An error occurred while generating the report: {str(e)}', 'danger')
        return redirect(request.referrer)
