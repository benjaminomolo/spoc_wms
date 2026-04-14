#app/routes/general_ledger/journals.py

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
from models import ChartOfAccounts, Category, Company, Module, ActivityLog, Project, \
    Vendor, PaymentMode, Currency, JournalEntry, Journal
from services.chart_of_accounts_helpers import calculate_account_balance, has_open_transactions
from services.post_to_ledger import post_opening_balances_to_ledger
from utils import apply_date_filters, format_amount, create_notification, generate_unique_journal_number, empty_to_none
from utils_and_helpers.exchange_rates import get_or_create_exchange_rate_for_transaction
from . import general_ledger_bp

logger = logging.getLogger()


@general_ledger_bp.route('/add_transaction', methods=["POST", "GET"])
@role_required(['Admin', 'Contributor'])
@login_required
def add_transaction():
    try:
        with Session() as db_session:

            app_id = current_user.app_id

            # Fetch all required data
            transaction_categories = db_session.query(Category).filter_by(app_id=app_id).all()
            payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).order_by(
                PaymentMode.payment_mode).all()
            vendors = db_session.query(Vendor).filter_by(app_id=app_id).order_by(Vendor.vendor_name.asc()).all()
            projects = db_session.query(Project).filter_by(app_id=app_id).all()
            currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
            base_currency = next((c for c in currencies if c.currency_index == 1), None)
            base_currency_code = base_currency.user_currency if base_currency else ''
            base_currency_id = base_currency.id

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

            # Get payable/receivable accounts
            payable_receivable_accounts = (
                db_session.query(ChartOfAccounts.id)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    (ChartOfAccounts.is_payable == True) | (ChartOfAccounts.is_receivable == True)
                ).all()
            )
            payable_receivable_ids = [acc.id for acc in payable_receivable_accounts]

            # Modules
            modules_data = [
                mod.module_name for mod in
                db_session.query(Module).filter_by(app_id=app_id, included='yes').all()
            ]

            # Company details
            company = db_session.query(Company).filter_by(id=app_id).scalar()

            return render_template(
                'add_transaction.html',
                transaction_categories=transaction_categories,
                payment_modes=payment_modes,
                vendors=vendors,
                project_names=projects,
                currencies=currencies,
                company=company,
                role=current_user.role,
                modules=modules_data,
                module_name="General Ledger",
                cash_account_ids=cash_account_ids,
                payable_receivable_ids=payable_receivable_ids,
                base_currency=base_currency,
                base_currency_code=base_currency_code,
                base_currency_id=base_currency_id
            )

    except Exception as e:
        logger.error(f'Error has occured {e}')
        traceback(f'Error has occured {e}')
        flash(f'An error occurred while fetching data: {str(e)}', 'danger')
        return redirect(request.referrer)


@general_ledger_bp.route('/journal_entries', methods=["GET"])
@login_required
def journal_entries():
    """
    Render journal entries page (GET).
    """
    db_session = Session()

    try:
        # Default parameters for initial page load
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        journal_number = request.args.get('journal_number', None)
        reference_number = request.args.get('reference', None)
        start_date = request.args.get('start_date', None)
        account_id = request.args.get('account_id')
        end_date = request.args.get('end_date', None)
        project_id = request.args.get('project', None)
        vendor_id = request.args.get('vendor', None)
        payment_mode_id = request.args.get('payment_mode', None)
        description_search = request.args.get('description_search', None)
        source_type = request.args.get('source_type', None)  # ADD THIS LINE

        filter_type = request.args.get('filter_type', 'transaction_date')

        filter_applied = bool(start_date or end_date or journal_number or project_id or
                              vendor_id or payment_mode_id or description_search or source_type or account_id)

        # Fetch initial data for template rendering
        journal_entries_data, pagination_data = _get_journal_entries_data(
            db_session, page, per_page, journal_number, start_date, end_date,
            project_id, vendor_id, payment_mode_id, description_search, filter_type,
            account_id, reference_number, source_type
        )

        # Fetch unique journal numbers for the filter dropdown
        journal_numbers = db_session.query(
            Journal.journal_number
        ).filter_by(
            app_id=current_user.app_id
        ).distinct().order_by(
            Journal.journal_number.desc()
        ).all()

        # Fetch filter dropdown data
        projects = db_session.query(Project).filter_by(app_id=current_user.app_id, is_active=True).all()
        vendors = db_session.query(Vendor).filter_by(app_id=current_user.app_id, is_active=True).all()
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=current_user.app_id, is_active=True).all()
        # Fetch unique source types - ORDER ALPHABETICALLY
        source_types = db_session.query(JournalEntry.source_type) \
            .filter(JournalEntry.source_type.isnot(None)) \
            .distinct() \
            .order_by(JournalEntry.source_type.asc()) \
            .all()

        # Convert to list of strings (already sorted alphabetically by the query)
        source_types = [st[0] for st in source_types if st[0] is not None]

        # Fetch company details
        company = db_session.query(Company).filter_by(id=current_user.app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=current_user.app_id, included='yes').all()]

        # Fetch accounts list first
        accounts = db_session.query(ChartOfAccounts).filter_by(app_id=current_user.app_id).all()

        grouped_accounts = {}
        for acct in accounts:
            parent_type = acct.parent_account_type or 'Others'
            category = acct.category or 'Uncategorized'

            if parent_type not in grouped_accounts:
                grouped_accounts[parent_type] = {}

            if category not in grouped_accounts[parent_type]:
                grouped_accounts[parent_type][category] = []

            grouped_accounts[parent_type][category].append(acct)

        currencies = db_session.query(Currency).filter_by(app_id=current_user.app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id

        # Render template for GET
        return render_template(
            'general-ledger/journal_entries.html',
            journal_entries=journal_entries_data,
            journal_numbers=journal_numbers,
            grouped_accounts=grouped_accounts,
            projects=projects,
            vendors=vendors,
            source_types=source_types,
            payment_modes=payment_modes,
            pagination=pagination_data,
            currencies=currencies,
            base_currency_id=base_currency_id,
            base_currency_code=base_currency_code,
            base_currency=base_currency,
            filters={
                'journal_number': journal_number,
                'reference_number': reference_number,
                'start_date': start_date,
                'end_date': end_date,
                'project': project_id,
                'vendor': vendor_id,
                'payment_mode': payment_mode_id,
                'description_search': description_search,
                'source_type': source_type,
                'filter_type': filter_type
            },
            company=company,
            role=role,
            module_name="General Ledger",
            modules=modules_data,
            filter_applied=filter_applied
        )

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error in journal_entries: {str(e)}")
        flash('An error occurred while retrieving journal entries.', 'error')
        return render_template('error.html', message='Database error occurred'), 500

    except Exception as e:
        logger.error(f'Unexpected error in journal_entries: {str(e)}\n{traceback.format_exc()}')
        flash('An unexpected error occurred.', 'error')
        return render_template('error.html', message='Unexpected error occurred'), 500

    finally:
        db_session.close()


@general_ledger_bp.route('/journal_entries/filter', methods=["POST"])
@login_required
def journal_entries_filter():
    """
    Return filtered JSON data for AJAX requests (POST).
    """
    db_session = Session()

    try:
        # Get JSON data from POST request
        data = request.get_json() or {}
        page = int(data.get('page', 1))
        per_page = int(data.get('per_page', 20))
        account_id = data.get('account_id')
        journal_number = data.get('journal_number')
        reference_number = data.get('reference')
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        project_id = data.get('project')
        vendor_id = data.get('vendor')
        payment_mode_id = data.get('payment_mode')
        description_search = data.get('description_search')
        filter_type = data.get('filter_type', 'transaction_date')

        source_type = data.get('source_type')

        # Get filtered data
        journal_entries_data, pagination_data = _get_journal_entries_data(
            db_session, page, per_page, journal_number, start_date, end_date,
            project_id, vendor_id, payment_mode_id, description_search, filter_type, account_id,
            reference_number, source_type
        )

        # Return JSON for AJAX filtering
        return jsonify({
            'success': True,
            'journal_entries': journal_entries_data,
            'pagination': pagination_data,
            'filters': {
                'journal_number': journal_number,
                'reference_number': reference_number,
                'start_date': start_date,
                'end_date': end_date,
                'project': project_id,
                'vendor': vendor_id,
                'account_id': account_id,
                'payment_mode': payment_mode_id,
                'description_search': description_search,
                'filter_type': filter_type,
                'source_type': source_type
            }
        }), 200

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error in journal_entries_filter: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred while filtering journal entries.'}), 500

    except Exception as e:
        logger.error(f'Unexpected error in journal_entries_filter: {str(e)}\n{traceback.format_exc()}')
        return jsonify({'success': False, 'message': 'An unexpected error occurred.'}), 500

    finally:
        db_session.close()


def _get_journal_entries_data(db_session, page, per_page, journal_number, start_date, end_date,
                              project_id=None, vendor_id=None, payment_mode_id=None,
                              description_search=None, filter_type='transaction_date',
                              account_id=None, reference_number=None, source_type=None):
    """
    Helper function to retrieve journal entries data with given filters.
    """
    # Base query for journals
    query = db_session.query(Journal).filter_by(app_id=current_user.app_id, status='Posted')

    # Track if we've joined with entries to avoid multiple joins
    joined_entries = False

    # Apply filters
    if journal_number:
        query = query.filter(Journal.journal_number == journal_number)

    if reference_number:
        query = query.filter(Journal.journal_ref_no.ilike(f'%{reference_number}%'))

    if start_date:
        try:
            start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            if filter_type == 'transaction_date':
                query = query.filter(Journal.date >= start_date_dt)
            else:  # date_added
                query = query.filter(Journal.date_added >= start_date_dt)
        except ValueError:
            raise ValueError('Invalid start date format. Use YYYY-MM-DD.')

    if end_date:
        try:
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            if filter_type == 'transaction_date':
                query = query.filter(Journal.date <= end_date_dt)
            else:  # date_added
                query = query.filter(Journal.date_added <= end_date_dt)
        except ValueError:
            raise ValueError('Invalid end date format. Use YYYY-MM-DD.')

    if project_id:
        query = query.filter(Journal.project_id == project_id)

    if vendor_id:
        query = query.filter(Journal.vendor_id == vendor_id)

    if payment_mode_id:
        query = query.filter(Journal.payment_mode_id == payment_mode_id)

    # Apply source_type filter
    if source_type:
        if not joined_entries:
            query = query.join(Journal.entries)
            joined_entries = True

        if source_type == 'general_ledger':
            # Show ONLY manual entries (source_type IS NULL)
            query = query.filter(JournalEntry.source_type.is_(None))
        else:
            # Show ONLY entries from a specific module
            query = query.filter(JournalEntry.source_type == source_type)

    # Apply account filter
    if account_id:
        if not joined_entries:
            query = query.join(Journal.entries)
            joined_entries = True
        query = query.filter(JournalEntry.subcategory_id == account_id)

    # Apply description search filter
    if description_search:
        if not joined_entries:
            query = query.join(Journal.entries)
            joined_entries = True
        query = query.filter(
            or_(
                Journal.narration.ilike(f'%{description_search}%'),
                JournalEntry.description.ilike(f'%{description_search}%')
            )
        )

    # Remove duplicates if we joined with entries
    if joined_entries:
        query = query.distinct()

    # Determine which date field to use for ordering
    if filter_type == 'transaction_date':
        date_field = Journal.date
    else:  # date_added
        date_field = Journal.date_added

    # Order by date descending, then journal number descending
    query = query.order_by(desc(date_field), desc(Journal.journal_number))

    # Pagination
    total_items = query.count()
    total_pages = (total_items + per_page - 1) // per_page
    journals_paginated = query.offset((page - 1) * per_page).limit(per_page).all()

    if not journals_paginated:
        return [], {
            'page': page,
            'per_page': per_page,
            'total_pages': 0,
            'total_items': 0,
            'has_next': False,
            'has_prev': False,
            'filter_type': filter_type
        }

    # Preload relationships for efficiency
    journal_ids = [journal.id for journal in journals_paginated]

    # Fetch all entries for these journals with relationships
    entries_query = db_session.query(JournalEntry).filter(
        JournalEntry.journal_id.in_(journal_ids)
    ).options(
        joinedload(JournalEntry.chart_of_accounts),
        joinedload(JournalEntry.company)
    ).order_by(
        JournalEntry.journal_id,
        JournalEntry.line_number
    ).all()

    # Group entries by journal_id
    entries_by_journal = {}
    for entry in entries_query:
        if entry.journal_id not in entries_by_journal:
            entries_by_journal[entry.journal_id] = []
        entries_by_journal[entry.journal_id].append(entry)

    journal_entries_data = []
    for journal in journals_paginated:
        journal_entries = entries_by_journal.get(journal.id, [])

        transactions_list = []
        total_debit_currency = {}
        total_credit_currency = {}

        for entry in journal_entries:
            currency = journal.currency.user_currency if journal.currency else None

            if entry.dr_cr == 'D':
                total_debit_currency[currency] = total_debit_currency.get(currency, 0) + float(entry.amount)
            elif entry.dr_cr == 'C':
                total_credit_currency[currency] = total_credit_currency.get(currency, 0) + float(entry.amount)

            transactions_list.append({
                'id': entry.id,
                'date': journal.date.strftime('%Y-%m-%d') if journal.date else None,
                'date_added': journal.date_added.isoformat() if journal.date_added else None,
                'account': entry.chart_of_accounts.sub_category if entry.chart_of_accounts else None,
                'account_code': entry.chart_of_accounts.sub_category_id if entry.chart_of_accounts else None,
                'transaction_type': entry.chart_of_accounts.parent_account_type,
                'description': entry.description,
                'debit': format_amount(entry.amount) if entry.dr_cr == 'D' else format_amount(0),
                'credit': format_amount(entry.amount) if entry.dr_cr == 'C' else format_amount(0),
                'currency': currency,
                'reconciled': entry.reconciled,
                'created_by': journal.created_user.name if journal.created_user else None,
                'created_at': journal.date_added.isoformat() if journal.date_added else None
            })

        # Format currency amounts
        formatted_total_debit_currency = {c: format_amount(a) for c, a in total_debit_currency.items()}
        formatted_total_credit_currency = {c: format_amount(a) for c, a in total_credit_currency.items()}

        # Get the appropriate entry date based on filter type
        if filter_type == 'transaction_date':
            entry_date = journal.date
        else:
            entry_date = journal.date_added

        journal_entries_data.append({
            'journal_number': journal.journal_number,
            'source_type': journal_entries[0].source_type if journal_entries else None,
            'entry_date': entry_date.strftime('%Y-%m-%d') if entry_date else None,
            'filter_type': filter_type,
            'transaction_count': len(journal_entries),
            'reference': journal.journal_ref_no,
            'total_debit': float(journal.total_debit or 0),
            'total_credit': float(journal.total_credit or 0),
            'balance': float(journal.balance or 0),
            'vendor': journal.vendor.vendor_name if journal.vendor else None,
            'project': journal.project.name if journal.project else None,
            'payment_mode': journal.payment_mode.payment_mode if journal.payment_mode else None,
            'journal_description': journal.narration or (journal.entries[0].description if journal.entries else None),
            'transactions': transactions_list,
            'journal_exchange_rate': journal.exchange_rate.rate if journal.exchange_rate else None,
            'total_debit_currency': formatted_total_debit_currency,
            'total_credit_currency': formatted_total_credit_currency
        })

    pagination_data = {
        'page': page,
        'per_page': per_page,
        'total_pages': total_pages,
        'total_items': total_items,
        'has_next': page < total_pages,
        'has_prev': page > 1,
        'filter_type': filter_type
    }

    return journal_entries_data, pagination_data


@general_ledger_bp.route('/journal_entries/edit/<journal_number>', methods=['GET', 'POST'])
@login_required
def edit_journal_entry(journal_number):
    user_session = Session()
    try:
        app_id = current_user.app_id

        if request.method == 'GET':
            # Fetch the journal data
            journal = user_session.query(Journal).filter(
                Journal.journal_number == journal_number,
                Journal.app_id == app_id
            ).first()

            if not journal:
                flash('Journal entry not found', 'error')
                return redirect(url_for('general_ledger.journal_entries'))

            # Fetch all journal entries for this journal with relationships
            journal_entries = user_session.query(JournalEntry).options(
                joinedload(JournalEntry.chart_of_accounts)
            ).filter(
                JournalEntry.journal_number == journal_number,
                JournalEntry.app_id == app_id
            ).order_by(JournalEntry.line_number).all()

            # Fetch dropdown options
            currencies = user_session.query(Currency).filter_by(app_id=app_id).order_by(Currency.currency_index).all()
            project_names = user_session.query(Project).filter_by(app_id=app_id).all()
            vendors = user_session.query(Vendor).filter_by(app_id=app_id).all()
            payment_modes = user_session.query(PaymentMode).filter_by(app_id=app_id).all()

            # Fetch base currency info
            base_currency_info = get_base_currency(user_session, app_id)
            base_currency = None
            if base_currency_info:
                base_currency = user_session.query(Currency).get(base_currency_info["base_currency_id"])

            # Get exchange rate value if exists
            exchange_rate_value = None
            if journal.exchange_rate_id:
                rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=user_session,
                    action='get',
                    source_type='journal',
                    source_id=journal.id
                )
                if rate_obj:
                    # Calculate the rate in the correct direction
                    if rate_obj.from_currency_id == journal.currency_id:
                        exchange_rate_value = float(rate_obj.rate)
                    else:
                        exchange_rate_value = float(1 / rate_obj.rate)

            # Convert journal data to dict for easier template access
            journal_data_dict = {
                'id': journal.id,
                'date': journal.date.strftime('%Y-%m-%d') if journal.date else '',
                'currency_id': journal.currency_id,
                'project_id': journal.project_id,
                'vendor_id': journal.vendor_id,
                'payment_mode_id': journal.payment_mode_id,
                'journal_number': journal.journal_number,
                'journal_ref_no': journal.journal_ref_no,
                'narration': journal.narration,
                'exchange_rate_id': journal.exchange_rate_id
            }

            # Convert journal_entries to list of dicts
            entries_list = []
            for entry in journal_entries:
                entries_list.append({
                    'id': entry.id,
                    'subcategory_id': entry.subcategory_id,
                    'category_id': entry.chart_of_accounts.category_fk if entry.chart_of_accounts else None,
                    'account_type': entry.chart_of_accounts.parent_account_type if entry.chart_of_accounts else None,
                    'debit_amount': entry.amount if entry.dr_cr == "D" else None,
                    'credit_amount': entry.amount if entry.dr_cr == "C" else None,
                    'description': entry.description
                })

            # Fetch company details
            company = user_session.query(Company).filter_by(id=current_user.app_id).first()
            role = current_user.role
            modules_data = [mod.module_name for mod in
                            user_session.query(Module).filter_by(app_id=current_user.app_id, included='yes').all()]

            # Get cash/bank accounts
            cash_accounts = (
                user_session.query(ChartOfAccounts.id)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    (ChartOfAccounts.is_cash == True) | (ChartOfAccounts.is_bank == True)
                )
                .all()
            )
            cash_account_ids = [acc.id for acc in cash_accounts]

            # Get payable/receivable accounts
            payable_receivable_accounts = (
                user_session.query(ChartOfAccounts.id)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    (ChartOfAccounts.is_payable == True) | (ChartOfAccounts.is_receivable == True)
                ).all()
            )
            payable_receivable_ids = [acc.id for acc in payable_receivable_accounts]

            return render_template(
                'general-ledger/edit_journal_entry.html',
                journal_data=journal_data_dict,
                journal_entries=entries_list,
                currencies=currencies,
                project_names=project_names,
                payable_receivable_ids=payable_receivable_ids,
                cash_account_ids=cash_account_ids,
                company=company,
                role=role,
                modules=modules_data,
                module_name="General Ledger",
                vendors=vendors,
                payment_modes=payment_modes,
                journal_number=journal_number,
                base_currency=base_currency,
                exchange_rate_value=exchange_rate_value
            )

        elif request.method == 'POST':
            # Handle the update request
            batch_notifications = set()

            # Parse form data
            date = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
            currency = request.form.get('form_currency')
            base_currency_id = request.form.get('base_currency_id')
            exchange_rate = request.form.get('exchange_rate')
            subcategory_list = request.form.getlist('subcategory')
            dr_list = request.form.getlist('debit_amount')
            cr_list = request.form.getlist('credit_amount')
            description_list = [desc if desc.strip() else None for desc in request.form.getlist('description')]
            payment_mode_id = request.form.get('payment_mode') if request.form.get('payment_mode') else None
            project_id = request.form.get('project_name') if request.form.get('project_name') else None
            vendor_id = request.form.get('payment_to_vendor') if request.form.get('payment_to_vendor') else None
            journal_ref_no = request.form.get('journal_ref_no') if request.form.get('journal_ref_no') else None
            narration = request.form.get('narration') if request.form.get('narration') else None

            # Get the journal
            journal = user_session.query(Journal).filter(
                Journal.journal_number == journal_number,
                Journal.app_id == app_id
            ).first()

            if not journal:
                return jsonify({"error": "Journal not found"}), 404

            # Handle exchange rate for the journal
            exchange_rate_id = None
            if int(currency) != int(base_currency_id):
                # Validate exchange rate
                if not exchange_rate or exchange_rate.strip() == '':
                    return jsonify({
                        'status': 'error',
                        'message': 'Exchange rate is required for foreign currency transactions',
                        'notification_type': 'danger'
                    }), 400

                try:
                    exchange_rate_value = float(exchange_rate)
                    if exchange_rate_value <= 0:
                        return jsonify({
                            'status': 'error',
                            'message': 'Exchange rate must be greater than 0',
                            'notification_type': 'danger'
                        }), 400
                except ValueError:
                    return jsonify({
                        'status': 'error',
                        'message': 'Invalid exchange rate format',
                        'notification_type': 'danger'
                    }), 400

                # Check if there's an existing exchange rate for this journal
                existing_rate_id, existing_rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=user_session,
                    action='get',
                    source_type='journal',
                    source_id=journal.id
                )

                if existing_rate_id:
                    # Update existing rate
                    existing_rate_obj.rate = exchange_rate_value
                    existing_rate_obj.date = date
                    existing_rate_obj.created_by = current_user.id
                    user_session.add(existing_rate_obj)
                    exchange_rate_id = existing_rate_id

                    notification = f"Exchange rate updated to {exchange_rate_value}"
                    batch_notifications.add(notification)
                else:
                    # Create new rate
                    rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                        session=user_session,
                        action='create',
                        from_currency_id=int(currency),
                        to_currency_id=int(base_currency_id),
                        rate_value=exchange_rate_value,
                        rate_date=date,
                        app_id=app_id,
                        created_by=current_user.id,
                        source_type='journal',
                        source_id=journal.id,
                        currency_exchange_transaction_id=None
                    )
                    exchange_rate_id = rate_id

                    if rate_id and rate_obj:
                        notification = f"Exchange rate {rate_obj.rate} applied"
                        batch_notifications.add(notification)

            # Update journal header
            journal.date = date
            journal.currency_id = currency
            journal.payment_mode_id = payment_mode_id
            journal.project_id = project_id
            journal.vendor_id = vendor_id
            journal.journal_ref_no = journal_ref_no
            journal.narration = narration
            journal.exchange_rate_id = exchange_rate_id
            journal.updated_by = current_user.id

            # Get existing journal entry IDs
            existing_entry_ids = [e.id for e in user_session.query(JournalEntry.id)
            .filter(JournalEntry.journal_number == journal_number, JournalEntry.app_id == app_id)
            .order_by(JournalEntry.line_number).all()]

            # Calculate new totals
            total_debit = 0
            total_credit = 0

            # Process each journal entry
            for i, (subcategory_id, db_amount, cr_amount, description) in enumerate(zip(
                    subcategory_list, dr_list, cr_list, description_list), 1):

                # Determine amount and dr_cr
                amount = float(db_amount) if db_amount else float(cr_amount)
                dr_cr = "D" if db_amount else "C"

                if dr_cr == 'D':
                    total_debit += amount
                else:
                    total_credit += amount

                # Determine if this is an update or new entry
                if i <= len(existing_entry_ids):
                    entry_id = existing_entry_ids[i - 1]
                    # Update existing entry
                    entry = user_session.query(JournalEntry).get(entry_id)
                    if entry:
                        entry.date = date
                        entry.subcategory_id = subcategory_id
                        entry.amount = amount
                        entry.dr_cr = dr_cr
                        entry.description = empty_to_none(description)
                        entry.line_number = i
                else:
                    # Create new journal entry
                    journal_entry = JournalEntry(
                        journal_id=journal.id,
                        line_number=i,
                        journal_number=journal_number,
                        app_id=app_id,
                        date=date,
                        subcategory_id=subcategory_id,
                        amount=amount,
                        dr_cr=dr_cr,
                        description=empty_to_none(description),
                        source_type=None
                    )
                    user_session.add(journal_entry)

            # Update journal totals
            journal.total_debit = total_debit
            journal.total_credit = total_credit
            journal.balance = total_debit - total_credit

            # Delete any extra entries that were removed
            if len(existing_entry_ids) > len(subcategory_list):
                entries_to_delete = existing_entry_ids[len(subcategory_list):]
                user_session.query(JournalEntry).filter(JournalEntry.id.in_(entries_to_delete)).delete(
                    synchronize_session=False)

            # Handle InternetPayment records if needed (for app_id == 1)
            if app_id == 1:
                # Remove existing internet payments for this journal
                user_session.query(InternetPayment).filter(
                    InternetPayment.journal_fk == journal.id
                ).delete(synchronize_session=False)

                # Create new internet payments if applicable
                if project_id:
                    # Check if any entry has subcategory_id 15
                    entries_with_subcat_15 = any(entry.subcategory_id == 15 for entry in journal.entries)
                    if entries_with_subcat_15:
                        internet_payment = InternetPayment(
                            journal_fk=journal.id,
                            date_of_expiry=date + timedelta(days=30),
                            site_id=project_id,
                            app_id=app_id
                        )
                        user_session.add(internet_payment)

            user_session.commit()

            # Create notification for exchange rates if needed
            if batch_notifications:
                if len(batch_notifications) == 1:
                    message = next(iter(batch_notifications))
                else:
                    message = f"Multiple auto-generated rates applied: " + "; ".join(batch_notifications)

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
                'message': 'Journal entry updated successfully',
                'notification_type': 'success',
                'rate_notices': list(batch_notifications) if batch_notifications else None
            }), 200

    except Exception as e:
        user_session.rollback()
        logger.error(f"Error updating journal entry {journal_number}: {str(e)}")
        traceback.print_exc()

        if request.method == 'POST':
            return jsonify({
                'status': 'error',
                'message': f'Error updating journal entry: {str(e)}',
                'notification_type': 'danger'
            }), 400
        else:
            flash(f'Error loading journal entry: {str(e)}', 'error')
            return redirect(url_for('general_ledger.journal_entries'))

    finally:
        user_session.close()


@general_ledger_bp.route('/submit_transaction', methods=['POST'])
@login_required
def submit_transaction():
    if request.method == 'POST':
        user_session = Session()
        app_id = current_user.app_id

        batch_notifications = set()

        try:
            # Generate journal number first (thread-safe)
            journal_number = generate_unique_journal_number(user_session, current_user.app_id)

            # Parse form data
            date = datetime.strptime(request.form['date'], '%Y-%m-%d').date()
            account_type_list = request.form.getlist('account_type')
            currency = request.form.get('form_currency')
            category = request.form.getlist('category')
            subcategory = request.form.getlist('subcategory')
            dr_list = request.form.getlist('debit_amount')
            cr_list = request.form.getlist('credit_amount')
            description_list = [desc if desc.strip() else None for desc in
                                request.form.getlist('description')]
            payment_mode_id = request.form.get('payment_mode') if request.form.get('payment_mode') else None
            project_id = request.form.get('project_name') if request.form.get('project_name') else None
            vendor_id = request.form.get('payment_to_vendor') if request.form.get('payment_to_vendor') else None
            journal_ref_no = request.form.get('journal_ref_no') if request.form.get('journal_ref_no') else None
            narration = request.form.get('narration') if request.form.get('narration') else None
            exchange_rate = request.form.get('exchange_rate')
            base_currency_id = request.form.get('base_currency_id')

            # Calculate totals for the journal
            total_debit = sum(float(amount) for amount in dr_list if amount)
            total_credit = sum(float(amount) for amount in cr_list if amount)
            balance = total_debit - total_credit

            # Create the Journal first (we need its ID for source_id)
            journal = Journal(
                journal_number=journal_number,
                journal_ref_no=journal_ref_no,
                narration=narration,
                date=date,
                payment_mode_id=payment_mode_id,
                project_id=project_id,
                vendor_id=vendor_id,
                currency_id=currency,
                created_by=current_user.id,
                app_id=app_id,
                total_debit=total_debit,
                total_credit=total_credit,
                balance=balance,
                status='Posted',
                exchange_rate_id=None  # Will update after rate is created
            )
            user_session.add(journal)
            user_session.flush()  # Get the journal ID

            # Handle exchange rate if currencies are different
            exchange_rate_id = None
            if int(currency) != int(base_currency_id):
                # Validate exchange rate
                if not exchange_rate or exchange_rate.strip() == '':
                    return jsonify({
                        'status': 'error',
                        'message': 'Exchange rate is required for foreign currency transactions',
                        'notification_type': 'danger'
                    }), 400

                try:
                    exchange_rate_value = float(exchange_rate)
                    if exchange_rate_value <= 0:
                        return jsonify({
                            'status': 'error',
                            'message': 'Exchange rate must be greater than 0',
                            'notification_type': 'danger'
                        }), 400
                except ValueError:
                    return jsonify({
                        'status': 'error',
                        'message': 'Invalid exchange rate format',
                        'notification_type': 'danger'
                    }), 400

                # Create exchange rate record with journal ID as source_id
                rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=user_session,
                    action='create',
                    from_currency_id=int(currency),
                    to_currency_id=int(base_currency_id),
                    rate_value=exchange_rate_value,
                    rate_date=date,
                    app_id=app_id,
                    created_by=current_user.id,
                    source_type='journal',
                    source_id=journal.id,  # Now we have the journal ID
                    currency_exchange_transaction_id=None
                )

                exchange_rate_id = rate_id

                # Update journal with exchange_rate_id
                journal.exchange_rate_id = exchange_rate_id

            journal_entries = []
            internet_payments = []

            # Process journal entries
            for i, (account_type, cat, subcat, db_amount, cr_amount, description) in enumerate(
                    zip(account_type_list, category, subcategory, dr_list, cr_list, description_list), 1):
                # Determine amount and dr_cr
                amount = float(db_amount) if db_amount else float(cr_amount)
                dr_cr = "D" if db_amount else "C"

                journal_entry = JournalEntry(
                    journal_id=journal.id,
                    line_number=i,
                    journal_number=journal_number,
                    app_id=app_id,
                    date=date,
                    subcategory_id=subcat,
                    amount=amount,
                    dr_cr=dr_cr,
                    description=description,
                    source_type=None
                )
                user_session.add(journal_entry)
                journal_entries.append(journal_entry)

            # Commit all data
            user_session.commit()

            # Create single notification after all entries are processed
            if batch_notifications:
                if len(batch_notifications) == 1:
                    message = next(iter(batch_notifications))
                else:
                    message = f"Multiple auto-generated rates applied: " + "; ".join(batch_notifications)

                create_notification(
                    db_session=user_session,
                    user_id=None,
                    company_id=app_id,
                    message=message,
                    type='info',
                    is_popup=True,
                    url=url_for('view_exchange_rates')
                )

            # Handle InternetPayment records if needed
            if app_id == 1:
                for entry in journal_entries:
                    if project_id and entry.subcategory_id == 15:
                        internet_payment = InternetPayment(
                            journal_fk=journal.id,
                            date_of_expiry=date + timedelta(days=30),
                            site_id=project_id,
                            app_id=app_id
                        )
                        user_session.add(internet_payment)

                user_session.commit()

            return jsonify({
                'status': 'success',
                'message': 'Transaction submitted successfully',
                'notification_type': 'success',
                'rate_notices': list(batch_notifications) if batch_notifications else None
            }), 200

        except Exception as e:
            user_session.rollback()
            traceback.print_exc()
            return jsonify({
                'status': 'error',
                'message': f'Error submitting transaction: {str(e)}',
                'notification_type': 'danger'
            }), 400

        finally:
            user_session.close()

    return redirect(url_for('add_transaction'))
