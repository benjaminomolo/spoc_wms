import logging
import traceback
from datetime import datetime
from decimal import Decimal

from flask import Blueprint, jsonify, render_template, flash, redirect, request, url_for
from flask_login import login_required, current_user
from sqlalchemy import or_
from sqlalchemy.orm import joinedload

from ai import get_base_currency, get_or_create_exchange_rate_id, resolve_exchange_rate_for_transaction
from db import Session
from models import Company, Module, Currency, Project, ChartOfAccounts, PaymentMode, Category, Vendor, \
    ExpenseItem, ExpenseTransaction, UserPreference, ExpenseStatusEnum, Journal, JournalEntry
from services.post_to_ledger import create_expense_journal_entry, post_expense_journal_to_ledger
from services.post_to_ledger_reversal import remove_expense_journal_entries
from utils import get_cash_balances, create_notification, generate_unique_expense_number, apply_date_filters, \
    generate_unique_journal_number, create_transaction
from utils_and_helpers.exchange_rates import get_or_create_exchange_rate_for_transaction

expense_routes = Blueprint('expense_routes', __name__)
logger = logging.getLogger(__name__)


@expense_routes.route('/expenses/add_expense', methods=["POST", "GET"])
@login_required
def add_expense():
    try:
        with Session() as db_session:

            app_id = current_user.app_id

            # Fetch all required data
            transaction_categories = db_session.query(Category).filter_by(app_id=app_id, account_type="Expense").all()
            payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).order_by(
                PaymentMode.payment_mode).all()
            vendors = db_session.query(Vendor).filter_by(app_id=app_id).order_by(Vendor.vendor_name.asc()).all()
            projects = db_session.query(Project).filter_by(app_id=app_id).all()
            currencies = db_session.query(Currency).filter_by(app_id=app_id).order_by(Currency.currency_index).all()
            base_currency = next((c for c in currencies if c.currency_index == 1), None)
            base_currency_code = base_currency.user_currency if base_currency else ''
            # Get accounts
            accounts = (
                db_session.query(ChartOfAccounts)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    (ChartOfAccounts.is_cash == True) |
                    (ChartOfAccounts.is_bank == True) |
                    (ChartOfAccounts.is_payable == True)
                )
                .all()
            )

            payable_account_ids = [account.id for account in accounts if account.is_payable]

            # Modules
            modules_data = [
                mod.module_name for mod in
                db_session.query(Module).filter_by(app_id=app_id, included='yes').all()
            ]

            # Company details
            company = db_session.query(Company).filter_by(id=app_id).scalar()
            journals = db_session.query(Journal.journal_number).filter_by(app_id=app_id).distinct().all()

            return render_template(
                'expenses/new_expense.html',
                expense_categories=transaction_categories,
                payment_modes=payment_modes,
                vendors=vendors,
                project_names=projects,
                currencies=currencies,
                company=company,
                role=current_user.role,
                modules=modules_data,
                module_name="Expenses",
                journals=journals,
                payable_account_ids=payable_account_ids,
                accounts=accounts,
                base_currency=base_currency,
                base_currency_code=base_currency_code
            )

    except Exception as e:
        logger.error(f'An error occurred while fetching data: {str(e)}')
        flash(f'An error occurred while fetching data: {str(e)}', 'danger')
        return redirect(request.referrer)


@expense_routes.route('/expenses/submit', methods=['POST'])
@login_required
def submit_expense():
    db_session = Session()
    app_id = current_user.app_id

    try:
        # --- Generate unique entry number ---
        expense_entry_number = generate_unique_expense_number(db_session, app_id)

        # --- Extract and validate form data ---
        expense_ref_no_raw = request.form.get("expense_ref_no")
        expense_ref_no = expense_ref_no_raw if expense_ref_no_raw else None

        narration_raw = request.form.get("narration")
        narration = narration_raw if narration_raw else None

        vendor_id_raw = request.form.get("vendor")
        vendor_id = int(vendor_id_raw) if vendor_id_raw else None

        project_id_raw = request.form.get("project_name")
        project_id = int(project_id_raw) if project_id_raw else None
        currency_id = int(request.form.get('form_currency'))
        exchange_rate = request.form.get('exchange_rate')
        base_currency_id = int(request.form.get('base_currency_id'))
        payment_mode_id = request.form.get('payment_mode') or None
        payment_account_id = int(request.form.get('payment_account'))
        payment_date = datetime.strptime(request.form.get('date'), '%Y-%m-%d').date()

        # --- Process each expense item and calculate total ---
        subcategory_ids = request.form.getlist('subcategory[]')
        amounts = request.form.getlist('amount[]')
        descriptions = request.form.getlist('description[]')

        # Handle exchange rate if currencies are different
        exchange_rate_id = None
        if currency_id != base_currency_id:
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

            # Create exchange rate record (source_id will be None initially, updated after expense creation)
            rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                session=db_session,
                action='create',
                from_currency_id=currency_id,
                to_currency_id=base_currency_id,
                rate_value=exchange_rate_value,
                rate_date=payment_date,
                app_id=app_id,
                created_by=current_user.id,
                source_type='expense',
                source_id=None,  # Will update after expense is created
                currency_exchange_transaction_id=None
            )

            exchange_rate_id = rate_id

        # Calculate total from individual amounts
        total_amount = 0.0
        expense_items = []
        for subcat_id, amt, desc in zip(subcategory_ids, amounts, descriptions):
            amount = round(float(amt), 2)
            total_amount += amount

            expense_items.append(ExpenseItem(
                subcategory_id=int(subcat_id),
                amount=amount,
                description=desc.strip() if desc else None
            ))

        # Create ExpenseTransaction instance with calculated total
        expense_tx = ExpenseTransaction(
            expense_entry_number=expense_entry_number,
            expense_ref_no=expense_ref_no,
            narration=narration,
            vendor_id=vendor_id,
            project_id=project_id,
            currency_id=currency_id,
            payment_mode_id=payment_mode_id,
            payment_account_id=payment_account_id,
            payment_date=payment_date,
            total_amount=round(total_amount, 2),
            created_by=current_user.id,
            app_id=app_id,
            is_posted_to_ledger=True,
            status='draft',
            exchange_rate_id=exchange_rate_id
        )

        db_session.add(expense_tx)
        db_session.flush()  # Get the ID before commit

        # Set transaction ID for all items
        for item in expense_items:
            item.expense_transaction_id = expense_tx.id
            db_session.add(item)

        # Create journal entry with "Unposted" status
        success, message, journal_entry = create_expense_journal_entry(
            db_session=db_session,
            expense_transaction=expense_tx,
            current_user=current_user,
            status='Posted',
            exchange_rate_id=exchange_rate_id
        )

        if not success:
            # If journal creation fails, we can still save the expense but warn the user
            db_session.rollback()
            return jsonify({
                'status': 'warning',
                'message': f'Expense creation failed',
                'expense_id': expense_tx.id,
                'journal_created': False
            })

        # Link the journal entry to the expense transaction if needed
        # expense_tx.journal_entry_id = journal_entry.id  # If you have this relationship

        db_session.commit()

        return jsonify({
            'status': 'success',
            'message': f'Expense submitted successfully.',
            'expense_id': expense_tx.id,
            'journal_created': True,
            'journal_status': 'Unposted'
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Submit Expense Error: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f"Failed to submit expense: {str(e)}"
        })

    finally:
        db_session.close()


@expense_routes.route('/expenses/transactions', methods=['GET'])
@login_required
def view_expense_transactions():
    db_session = Session()
    try:
        # Fetch company and module data
        company = db_session.query(Company).filter_by(id=current_user.app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=current_user.app_id).filter_by(included='yes').all()]

        # Fetch user preferences for modals
        user_preference_bulk = db_session.query(UserPreference).filter(
            UserPreference.user_id == current_user.id,
            UserPreference.preference_type == "bulk_post_expense_to_ledger_modal"
        ).first()
        do_not_show_again_bulk_post_expenses_to_ledger = user_preference_bulk.do_not_show_again if user_preference_bulk else False

        user_preference_single = db_session.query(UserPreference).filter(
            UserPreference.user_id == current_user.id,
            UserPreference.preference_type == "single_post_expense_to_ledger_modal"
        ).first()
        do_not_show_again_single_post_expense_to_ledger = user_preference_single.do_not_show_again if user_preference_single else False

        # Fetch filter dropdown data
        vendors = db_session.query(Vendor).filter_by(app_id=current_user.app_id, is_active=True).all()
        projects = db_session.query(Project).filter_by(app_id=current_user.app_id).all()

        # Expense categories (parent accounts)
        expense_categories = db_session.query(Category).filter_by(
            account_type='Expense',
            app_id=current_user.app_id
        ).order_by(Category.category).all()

        expense_subcategories = db_session.query(ChartOfAccounts).filter_by(
            parent_account_type='Expense',
            app_id=current_user.app_id
        ).order_by(ChartOfAccounts.sub_category).all()

        payment_modes = db_session.query(PaymentMode).filter_by(app_id=current_user.app_id).all()

        # Payment accounts (bank/cash accounts)
        # Funding accounts (using is_cash/is_bank flags - more efficient)
        funding_accounts = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == current_user.app_id,
            or_(
                ChartOfAccounts.is_cash.is_(True),
                ChartOfAccounts.is_bank.is_(True)
            )
        ).order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc()).all()

        currencies = db_session.query(Currency).filter_by(app_id=current_user.app_id).all()

        # Get initial transactions (first page)
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)

        # Base query with joins
        query = db_session.query(ExpenseTransaction).options(
            joinedload(ExpenseTransaction.vendor),
            joinedload(ExpenseTransaction.project),
            joinedload(ExpenseTransaction.currency),
            joinedload(ExpenseTransaction.payment_mode),
            joinedload(ExpenseTransaction.payment_account)
        ).filter(ExpenseTransaction.app_id == current_user.app_id)

        # Get total count
        total = query.count()

        # Apply pagination
        transactions = query.order_by(
            ExpenseTransaction.payment_date.desc(),
            ExpenseTransaction.id.desc()
        ).offset((page - 1) * per_page).limit(per_page).all()

        pagination_data = {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page
        }

        return render_template(
            'expenses/expense_transactions.html',
            transactions=transactions,
            vendors=vendors,
            projects=projects,
            expense_categories=expense_categories,
            expense_subcategories=expense_subcategories,
            payment_modes=payment_modes,
            funding_accounts=funding_accounts,
            currencies=currencies,
            company=company,
            role=role,
            modules=modules_data,
            module_name='Expenses',
            filter_applied=False,
            do_not_show_again_bulk_post_expenses_to_ledger=do_not_show_again_bulk_post_expenses_to_ledger,
            do_not_show_again_single_post_expense_to_ledger=do_not_show_again_single_post_expense_to_ledger,
            pagination=pagination_data
        )

    except Exception as e:
        logger.error(f"Error in view_expense_transactions: {str(e)}\n{traceback.format_exc()}")
        flash("An error occurred while loading transactions", "danger")
        return redirect(url_for('expense_routes.view_expense_transactions'))
    finally:
        db_session.close()


@expense_routes.route('/expenses/transactions/filter', methods=['GET', 'POST'])
@login_required
def expense_transactions_filter():
    """Return filtered JSON data for AJAX requests"""
    db_session = Session()

    try:
        # Get user permissions once at the beginning

        # Get filter parameters
        if request.method == 'GET':
            page = request.args.get('page', 1, type=int)
            per_page = request.args.get('per_page', 20, type=int)
            expense_number = request.args.get('expense_number', '')
            reference = request.args.get('reference', '')
            narration = request.args.get('narration', '')
            vendor_id = request.args.get('vendor', '')
            project_id = request.args.get('project', '')
            expense_category_id = request.args.get('expense_category', '')
            expense_subcategory_id = request.args.get('expense_subcategory', '')  # For GET
            payment_mode_id = request.args.get('payment_mode', '')
            payment_account_id = request.args.get('payment_account', '')
            currency_id = request.args.get('currency', '')
            start_date = request.args.get('start_date', '')
            end_date = request.args.get('end_date', '')
            status = request.args.get('status', '')
            posted_status = request.args.get('posted_status', '')
        else:
            data = request.get_json() or {}
            page = int(data.get('page', 1))
            per_page = int(data.get('per_page', 20))
            expense_number = data.get('expense_number', '')
            reference = data.get('reference', '')
            narration = data.get('narration', '')
            vendor_id = data.get('vendor', '')
            project_id = data.get('project', '')
            expense_category_id = data.get('expense_category', '')
            expense_subcategory_id = data.get('expense_subcategory', '')  # For POST

            payment_mode_id = data.get('payment_mode', '')
            payment_account_id = data.get('payment_account', '')
            currency_id = data.get('currency', '')
            start_date = data.get('start_date', '')
            end_date = data.get('end_date', '')
            status = data.get('status', '')
            posted_status = data.get('posted_status', '')

        # Base query
        query = db_session.query(ExpenseTransaction).options(
            joinedload(ExpenseTransaction.vendor),
            joinedload(ExpenseTransaction.project),
            joinedload(ExpenseTransaction.currency),
            joinedload(ExpenseTransaction.payment_mode),
            joinedload(ExpenseTransaction.payment_account)
        ).filter(ExpenseTransaction.app_id == current_user.app_id)

        # Apply filters
        if expense_number:
            query = query.filter(ExpenseTransaction.expense_entry_number.ilike(f'%{expense_number}%'))

        if reference:
            query = query.filter(ExpenseTransaction.expense_ref_no.ilike(f'%{reference}%'))

        if narration:
            query = query.filter(ExpenseTransaction.narration.ilike(f'%{narration}%'))

        if vendor_id:
            query = query.filter(ExpenseTransaction.vendor_id == vendor_id)

        if project_id:
            query = query.filter(ExpenseTransaction.project_id == project_id)

        if payment_mode_id:
            query = query.filter(ExpenseTransaction.payment_mode_id == payment_mode_id)

        if payment_account_id:
            query = query.filter(ExpenseTransaction.payment_account_id == payment_account_id)

        if currency_id:
            query = query.filter(ExpenseTransaction.currency_id == currency_id)

        # Date filters
        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
                query = query.filter(ExpenseTransaction.payment_date >= start_date_obj)
            except ValueError:
                pass

        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
                query = query.filter(ExpenseTransaction.payment_date <= end_date_obj)
            except ValueError:
                pass

        # Status filter (draft/approved)
        if status:
            if status == 'draft':
                query = query.filter(ExpenseTransaction.status == 'draft')
            elif status == 'approved':
                query = query.filter(ExpenseTransaction.status == 'approved')

        # Posted status filter
        if posted_status:
            if posted_status == 'posted':
                query = query.filter(ExpenseTransaction.is_posted_to_ledger == True)
            elif posted_status == 'not_posted':
                query = query.filter(ExpenseTransaction.is_posted_to_ledger == False)

        # Expense category and subcategory filters

        # Expense category and subcategory filters
        if expense_category_id or expense_subcategory_id:
            # Start with base join to expense_items
            query = query.join(ExpenseItem)

            if expense_category_id and expense_subcategory_id:
                # When both filters are present, we need one join to expense_items
                # and one join to chart_of_accounts for the category
                query = query.join(ExpenseItem.subcategory).filter(
                    ChartOfAccounts.category_fk == expense_category_id,
                    ExpenseItem.subcategory_id == expense_subcategory_id
                )
            elif expense_category_id:
                # Only category filter
                query = query.join(ExpenseItem.subcategory).filter(
                    ChartOfAccounts.category_fk == expense_category_id
                )
            elif expense_subcategory_id:
                # Only subcategory filter
                query = query.filter(ExpenseItem.subcategory_id == expense_subcategory_id)

        # Get total count
        total = query.count()

        # Apply pagination
        transactions = query.order_by(
            ExpenseTransaction.payment_date.desc(),
            ExpenseTransaction.id.desc()
        ).distinct().offset((page - 1) * per_page).limit(per_page).all()

        # Format transactions for JSON response
        formatted_transactions = []
        for transaction in transactions:
            formatted_transactions.append({
                'id': transaction.id,
                'expense_number': transaction.expense_entry_number or '-',
                'reference': transaction.expense_ref_no or '-',
                'narration': transaction.narration or '-',
                'payee': transaction.vendor.vendor_name if transaction.vendor else '-',
                'project': transaction.project.name if transaction.project else '-',
                'payment_date': transaction.payment_date.strftime('%Y-%m-%d') if transaction.payment_date else '-',
                'total_amount': float(transaction.total_amount),
                'currency': transaction.currency.user_currency if transaction.currency else '-',
                'payment_mode': transaction.payment_mode.payment_mode if transaction.payment_mode else '-',
                'payment_account': transaction.payment_account.sub_category if transaction.payment_account else '-',
                'status': transaction.status.name if transaction.status else 'draft',
                'status_display': 'Approved' if transaction.status and transaction.status.name == 'approved' else 'Draft',
                'is_posted_to_ledger': transaction.is_posted_to_ledger,

            })

        pagination_data = {
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page,
            'has_next': page < ((total + per_page - 1) // per_page),
            'has_prev': page > 1
        }

        return jsonify({
            'success': True,
            'transactions': formatted_transactions,
            'pagination': pagination_data
        })

    except Exception as e:
        logger.error(f"Error in expense_transactions_filter: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while filtering transactions'
        }), 500
    finally:
        db_session.close()


@expense_routes.route('/expenses/post_bulk_to_ledger', methods=['POST'])
@login_required
def post_bulk_expense_to_ledger():
    db_session = Session()

    try:
        # Get data from form instead of JSON
        transaction_ids_str = request.form.get('transaction_ids')
        if not transaction_ids_str:
            flash('No transaction IDs provided.', 'error')
            return redirect(url_for('expense_routes.view_expense_transactions'))

        transaction_ids = [int(id.strip()) for id in transaction_ids_str.split(',') if id.strip()]

        if not transaction_ids:
            flash('No transaction IDs provided.', 'error')
            return redirect(url_for('expense_routes.view_expense_transactions'))

        # Fetch transactions by IDs
        transactions = db_session.query(ExpenseTransaction).filter(
            ExpenseTransaction.id.in_(transaction_ids)
        ).all()

        if not transactions:
            flash('No matching transactions found.', 'error')
            return redirect(url_for('expense_routes.view_expense_transactions'))

        # Check for drafts
        drafts = [tx for tx in transactions if tx.status == 'draft']
        if drafts:
            draft_refs = [tx.expense_entry_number for tx in drafts]
            flash(
                f"One or more selected transactions are still in draft status. Please approve them first: {', '.join(draft_refs)}",
                'error')
            return redirect(url_for('expense_routes.view_expense_transactions'))

        # Check for already posted transactions
        already_posted = [tx for tx in transactions if tx.is_posted_to_ledger]
        if already_posted:
            posted_refs = [tx.expense_entry_number for tx in already_posted]
            flash(f"One or more transactions are already posted to ledger: {', '.join(posted_refs)}", 'error')
            return redirect(url_for('expense_routes.view_expense_transactions'))

        successful_postings = []
        failed_postings = []

        for tx in transactions:
            if tx.is_posted_to_ledger:
                continue  # Should not happen due to above check, but just in case

            # Use the reusable function to post to ledger
            success, message, journal = post_expense_journal_to_ledger(db_session, tx, current_user)

            if success:
                successful_postings.append(tx.expense_entry_number)
            else:
                failed_postings.append({
                    'reference': tx.expense_entry_number,
                    'error': message
                })

        # Commit all changes at once
        db_session.commit()

        # Prepare flash messages based on results
        if successful_postings and not failed_postings:
            flash(f"All {len(successful_postings)} transactions posted successfully to ledger.", 'success')
        elif successful_postings and failed_postings:
            flash(f"{len(successful_postings)} transactions posted successfully. {len(failed_postings)} failed.",
                  'warning')
            # Store failed postings in session for display if needed
            if failed_postings:
                flash(f"Failed transactions: {', '.join([fp['reference'] for fp in failed_postings])}", 'warning')
        else:
            flash(f"Failed to post {len(failed_postings)} transactions to ledger.", 'error')
            if failed_postings:
                flash(f"Failed transactions: {', '.join([fp['reference'] for fp in failed_postings])}", 'error')

        return redirect(url_for('expense_routes.view_expense_transactions'))

    except Exception as e:
        db_session.rollback()
        logger.error(f'Bulk post to ledger error: {e}\n{traceback.format_exc()}')
        flash(f"System error: {str(e)}", 'error')
        return redirect(url_for('expense_routes.view_expense_transactions'))

    finally:
        db_session.close()


@expense_routes.route('/expenses/post_single_to_ledger', methods=['POST'])
@login_required
def post_single_expense_to_ledger():
    db_session = Session()

    try:
        data = request.get_json()
        transaction_id = data.get('transaction_id')

        if not transaction_id:
            return jsonify(success=False, message="Transaction ID is required."), 400

        tx = db_session.query(ExpenseTransaction).filter_by(
            id=transaction_id,
            app_id=current_user.app_id
        ).first()

        if not tx:
            return jsonify(success=False, message="Transaction not found."), 404

        if tx.status == 'draft':
            return jsonify(
                success=False,
                message=f"Transaction {tx.expense_entry_number} is still in draft status. Approve it first."
            ), 400

        if tx.is_posted_to_ledger:
            return jsonify(
                success=False,
                message=f"Transaction {tx.expense_entry_number} is already posted to the ledger."
            ), 400

        # Use the reusable function
        success, message, journal = post_expense_journal_to_ledger(db_session, tx, current_user)

        if success:
            return jsonify(
                success=True,
                message=message,
                transaction_ref=tx.expense_entry_number
            ), 200
        else:
            return jsonify(
                success=False,
                message=message,
                transaction_ref=tx.expense_entry_number
            ), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f'Single post to ledger error for transaction {transaction_id}: {str(e)}')
        return jsonify(success=False, message=f"System error: {str(e)}"), 500
    finally:
        db_session.close()


@expense_routes.route('/expenses/transactions/<int:transaction_id>/journal-entries-json')
def get_journal_entries_json(transaction_id):
    db_session = Session()
    app_id = current_user.app_id

    # Fetch the transaction for the current app
    transaction = db_session.query(ExpenseTransaction).filter_by(id=transaction_id, app_id=app_id).first()

    if not transaction:
        return jsonify({"success": False, "message": "Transaction not found."}), 404

    # Construct journal-style entries (debit expense accounts, credit payment account)
    entries = []

    # Debit entries from expense items
    for item in transaction.expense_items:
        entries.append({
            "account": item.subcategory.sub_category,
            "description": item.description or '',
            "debit": float(item.amount),
            "credit": 0.00,
            "entry_date": transaction.payment_date.strftime('%Y-%m-%d')
        })

    # Credit entry for payment account
    entries.append({
        "account": transaction.payment_account.sub_category,
        "description": f"Disbursement for {transaction.expense_entry_number}",
        "debit": 0.00,
        "credit": float(transaction.total_amount),
        "entry_date": transaction.payment_date.strftime('%Y-%m-%d')
    })

    return jsonify({
        "success": True,
        "expense_number": transaction.expense_entry_number,
        "reference_number": transaction.expense_ref_no,
        "entries": entries
    })


@expense_routes.route('/expenses/approve_transaction', methods=['POST'])
@login_required
def approve_transaction():
    try:
        data = request.get_json()
        transaction_id = data.get("transaction_id")
        app_id = current_user.app_id

        if not transaction_id:
            return jsonify(success=False, error="Transaction ID is required.")

        db_session = Session()

        transaction = db_session.query(ExpenseTransaction).filter_by(
            id=transaction_id,
            app_id=app_id
        ).first()

        if not transaction:
            return jsonify(success=False, error="Transaction not found.")

        if transaction.status != ExpenseStatusEnum.draft:
            return jsonify(success=False, error="Only draft transactions can be approved.")

        # Mark as approved
        transaction.status = ExpenseStatusEnum.approved
        db_session.commit()

        return jsonify(success=True, message="Transaction approved.")

    except Exception as e:
        db_session.rollback()
        return jsonify(success=False, error=f"An error occurred: {str(e)}")

    finally:
        db_session.close()


@expense_routes.route('/expenses/approve_bulk_transactions', methods=['POST'])
@login_required
def approve_bulk_transactions():
    db_session = Session()
    try:
        data = request.get_json()
        transaction_ids = data.get("transaction_ids")
        app_id = current_user.app_id

        if not transaction_ids:
            return jsonify(success=False, error="No transaction IDs provided.")

        # Fetch all matching transactions for the current app
        transactions = db_session.query(ExpenseTransaction).filter(
            ExpenseTransaction.id.in_(transaction_ids),
            ExpenseTransaction.app_id == app_id
        ).all()

        if not transactions:
            return jsonify(success=False, error="No matching transactions found.")

        approved_count = 0
        for transaction in transactions:
            if transaction.status == ExpenseStatusEnum.draft:
                transaction.status = ExpenseStatusEnum.approved
                approved_count += 1

        db_session.commit()

        return jsonify(success=True, message=f"{approved_count} transaction(s) approved.")

    except Exception as e:
        db_session.rollback()
        logger.error("Bulk approval error:", str(e))
        return jsonify(success=False, error="An unexpected error occurred while approving transactions.")


@expense_routes.route('/expenses/delete_bulk_transactions', methods=['POST'])
@login_required
def delete_bulk_transactions():
    db_session = None
    try:
        data = request.get_json()
        transaction_ids = data.get("transaction_ids")
        app_id = current_user.app_id

        if not transaction_ids:
            return jsonify(success=False, error="No transaction IDs provided.")

        db_session = Session()

        # Fetch transactions that match the IDs and belong to this app
        transactions = db_session.query(ExpenseTransaction).filter(
            ExpenseTransaction.id.in_(transaction_ids),
            ExpenseTransaction.app_id == app_id
        ).all()

        if not transactions:
            return jsonify(success=False, error="No matching transactions found.")

        deleted_count = 0
        errors = []

        for transaction in transactions:
            try:
                # Remove journal entries if they exist (for any transaction with journal entries)
                remove_success, remove_message = remove_expense_journal_entries(db_session, transaction.id)
                if not remove_success:
                    errors.append(
                        f"Failed to remove journal entries for {transaction.expense_entry_number}: {remove_message}")
                    continue

                # Delete the transaction (regardless of status, since we removed journal entries)
                db_session.delete(transaction)
                deleted_count += 1

            except Exception as e:
                errors.append(f"Error deleting transaction {transaction.expense_entry_number}: {str(e)}")
                continue

        db_session.commit()

        # Prepare response message
        if deleted_count > 0 and not errors:
            return jsonify(success=True, message=f"{deleted_count} transaction(s) deleted successfully.")
        elif deleted_count > 0 and errors:
            return jsonify(
                success=True,
                message=f"{deleted_count} transaction(s) deleted successfully. {len(errors)} errors occurred.",
                warnings=errors
            )
        else:
            return jsonify(success=False, error=f"No transactions deleted. {len(errors)} errors occurred.",
                           errors=errors)

    except Exception as e:
        if db_session:
            db_session.rollback()
        logger.error(f"Bulk delete error: {str(e)}")
        return jsonify(success=False, error="An unexpected error occurred while deleting transactions.")
    finally:
        if db_session:
            db_session.close()


@expense_routes.route('/expenses/delete_transaction', methods=['POST'])
@login_required
def delete_transaction():
    db_session = Session()

    try:
        data = request.get_json()
        transaction_id = data.get('transaction_id')
        app_id = current_user.app_id

        if not transaction_id:
            return jsonify(success=False, error="Transaction ID is required."), 400

        # Fetch the transaction with app_id filter for security
        transaction = db_session.query(ExpenseTransaction).filter_by(
            id=transaction_id,
            app_id=app_id
        ).first()

        if not transaction:
            return jsonify(success=False, error="Transaction not found."), 404

        remove_success, remove_message = remove_expense_journal_entries(db_session, transaction.id)
        if not remove_success:
            return jsonify(
                success=False,
                error=f"Failed to remove journal entries: {remove_message}"
            ), 400

        # Store reference for success message
        transaction_ref = transaction.expense_entry_number

        # Delete the transaction (regardless of status)
        db_session.delete(transaction)
        db_session.commit()

        return jsonify(
            success=True,
            message=f"Transaction {transaction_ref} deleted successfully."
        ), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f"Delete transaction error: {str(e)}")
        return jsonify(success=False, error=f"An error occurred while deleting the transaction: {str(e)}"), 500

    finally:
        db_session.close()


@expense_routes.route('/expenses/edit_expense/<int:expense_id>', methods=["GET"])
@login_required
def edit_expense(expense_id):
    try:
        with Session() as db_session:
            app_id = current_user.app_id

            # Fetch the existing expense with its items - FIXED THE QUERY HERE
            expense = db_session.query(ExpenseTransaction).options(
                joinedload(ExpenseTransaction.expense_items)
            ).filter_by(
                id=expense_id,
                app_id=app_id
            ).first()

            if not expense:
                flash('Expense not found', 'danger')
                return redirect(url_for('expense_routes.view_expense_transactions'))

            # Fetch all required data
            expense_categories = db_session.query(Category).filter_by(
                app_id=app_id,
                account_type="Expense"
            ).all()

            payment_modes = db_session.query(PaymentMode).filter_by(
                app_id=app_id
            ).order_by(PaymentMode.payment_mode).all()

            vendors = db_session.query(Vendor).filter_by(
                app_id=app_id
            ).order_by(Vendor.vendor_name.asc()).all()

            projects = db_session.query(Project).filter_by(
                app_id=app_id
            ).all()

            currencies = db_session.query(Currency).filter_by(
                app_id=app_id
            ).order_by(Currency.currency_index).all()

            base_currency = next((c for c in currencies if c.currency_index == 1), None)
            base_currency_code = base_currency.user_currency if base_currency else ''

            # Get all expense accounts (subcategories)
            expense_accounts = db_session.query(ChartOfAccounts).filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.category_fk.in_([cat.id for cat in expense_categories])
            ).all()

            modules_data = [
                mod.module_name for mod in
                db_session.query(Module).filter_by(app_id=app_id, included='yes').all()
            ]

            # Group accounts by category for the dropdowns
            accounts_by_category = {}
            for account in expense_accounts:
                if account.category_fk not in accounts_by_category:
                    accounts_by_category[account.category_fk] = []
                accounts_by_category[account.category_fk].append(account)

            # Convert accounts_by_category to a serializable format
            serializable_accounts = {}
            for category_id, accounts in accounts_by_category.items():
                serializable_accounts[category_id] = [
                    {
                        'id': account.id,
                        'category': account.category,
                        'sub_category': account.sub_category
                    }
                    for account in accounts
                ]

            accounts = (
                db_session.query(ChartOfAccounts)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    (ChartOfAccounts.is_cash == True) |
                    (ChartOfAccounts.is_bank == True) |
                    (ChartOfAccounts.is_payable == True)
                )
                .all()
            )

            # === GET EXCHANGE RATE VALUE ===
            exchange_rate_value = None
            if expense.exchange_rate_id:
                # Get the exchange rate record
                from app import ExchangeRate
                rate_record = db_session.query(ExchangeRate).get(expense.exchange_rate_id)

                if rate_record:
                    # Calculate the rate in the correct direction
                    if rate_record.from_currency_id == expense.currency_id:
                        # Rate is stored as FROM expense currency TO base currency
                        exchange_rate_value = Decimal(rate_record.rate)
                    else:
                        # Rate is stored in reverse, so invert it
                        exchange_rate_value = Decimal(1 / rate_record.rate)

            return render_template(
                'expenses/edit_expense_journal.html',
                expense=expense,
                expense_categories=expense_categories,
                payment_modes=payment_modes,
                vendors=vendors,
                project_names=projects,
                currencies=currencies,
                accounts=accounts,
                accounts_by_category=serializable_accounts,
                company=db_session.query(Company).filter_by(id=app_id).first(),
                role=current_user.role,
                module_name="Expenses",
                modules=modules_data,
                base_currency=base_currency,
                base_currency_code=base_currency_code,
                exchange_rate_value=exchange_rate_value  # Pass to template
            )

    except Exception as e:
        logger.error(f'An error occurred while fetching expense data: {str(e)}\n{traceback.format_exc()}')
        flash(f'An error occurred while fetching expense data: {str(e)}', 'danger')
        return redirect(url_for('expense_routes.view_expense_transactions'))


@expense_routes.route('/expenses/update_expense/<int:expense_id>', methods=['POST'])
@login_required
def update_expense(expense_id):
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Fetch the existing expense
        expense = db_session.query(ExpenseTransaction).filter_by(
            id=expense_id,
            app_id=app_id
        ).first()

        if not expense:
            return jsonify({
                'success': False,
                'message': 'Expense not found'
            })

        # First, get the original journal number before removing the entries
        original_journal = db_session.query(Journal).join(JournalEntry).filter(
            JournalEntry.source_type == 'expense_transaction',
            JournalEntry.source_id == expense_id
        ).first()

        original_journal_number = original_journal.journal_number if original_journal else None

        # === Remove existing journal entries first ===
        success, message = remove_expense_journal_entries(db_session, expense_id)
        if not success:
            return jsonify({'success': False, 'message': f"Journal removal failed: {message}"})

        vendor_id_raw = request.form.get("vendor")
        vendor_id = int(vendor_id_raw) if vendor_id_raw else None

        project_id_raw = request.form.get("project_name")
        project_id = int(project_id_raw) if project_id_raw else None
        currency_id = int(request.form.get('form_currency'))
        base_currency_id = int(request.form.get('base_currency_id'))
        exchange_rate = request.form.get('exchange_rate')
        payment_mode_id = request.form.get('payment_mode') or None
        payment_account_id = int(request.form.get('payment_account'))
        payment_date = datetime.strptime(request.form.get('date'), '%Y-%m-%d').date()

        # --- Handle exchange rate for the expense ---
        exchange_rate_id = None
        if currency_id != base_currency_id:
            # Validate exchange rate
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

            # Check if there's an existing exchange rate for this expense
            from app import ExchangeRate
            existing_rate = db_session.query(ExchangeRate).filter(
                ExchangeRate.source_type == 'expense',
                ExchangeRate.source_id == expense_id
            ).first()

            if existing_rate:
                # Update existing rate
                existing_rate.from_currency_id = currency_id
                existing_rate.to_currency_id = base_currency_id
                existing_rate.rate = exchange_rate_value
                existing_rate.date = payment_date
                existing_rate.created_by = current_user.id
                db_session.add(existing_rate)
                exchange_rate_id = existing_rate.id
            else:
                # Create new rate
                rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=db_session,
                    action='create',
                    from_currency_id=currency_id,
                    to_currency_id=base_currency_id,
                    rate_value=exchange_rate_value,
                    rate_date=payment_date,
                    app_id=app_id,
                    created_by=current_user.id,
                    source_type='expense',
                    source_id=expense_id,
                    currency_exchange_transaction_id=None
                )
                exchange_rate_id = rate_id

        # --- Update expense header ---
        expense.vendor_id = vendor_id
        expense.project_id = project_id
        expense.currency_id = currency_id
        expense.payment_mode_id = payment_mode_id
        expense.payment_account_id = payment_account_id
        expense.payment_date = payment_date
        expense.updated_at = datetime.now()
        expense.updated_by = current_user.id
        expense.exchange_rate_id = exchange_rate_id  # Store the exchange rate ID

        # === CRITICAL: Set status back to draft when editing ===
        expense.status = 'draft'

        # Optional: Update reference and narration if needed
        expense_ref_no_raw = request.form.get("expense_ref_no")
        expense.expense_ref_no = expense_ref_no_raw if expense_ref_no_raw else None

        narration_raw = request.form.get("narration")
        expense.narration = narration_raw if narration_raw else None

        # --- Process line items ---
        # First delete existing items
        db_session.query(ExpenseItem).filter_by(
            expense_transaction_id=expense_id
        ).delete()

        # Then add updated items
        account_ids = request.form.getlist('subcategory[]')
        amounts = request.form.getlist('amount[]')
        descriptions = request.form.getlist('description[]')

        total_amount = 0.0
        for account_id, amt, desc in zip(account_ids, amounts, descriptions):
            if not account_id or not amt:
                continue

            amount = round(float(amt), 2)
            total_amount += amount

            item = ExpenseItem(
                expense_transaction_id=expense.id,
                subcategory_id=int(account_id),
                amount=amount,
                description=desc.strip() if desc else None
            )
            db_session.add(item)

        # Update total amount
        expense.total_amount = round(total_amount, 2)

        # === Create new journal entry with updated data ===
        # Then create the new journal entry with the original number
        journal_success, journal_message, new_journal = create_expense_journal_entry(
            db_session=db_session,
            expense_transaction=expense,
            current_user=current_user,
            status='Posted',
            is_update=True,
            original_journal_number=original_journal_number,
            exchange_rate_id=exchange_rate_id  # Pass the exchange rate ID
        )

        # === CRITICAL: Check if journal creation succeeded ===
        if not journal_success:
            db_session.rollback()  # Rollback everything if journal creation fails
            return jsonify({
                'success': False,
                'message': f"Expense update failed: {journal_message}"
            })

        # === Only commit if ALL operations succeeded ===
        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Expense updated successfully and set back to draft status.',
            'expense_id': expense.id
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Update Expense Error: {str(e)}")
        return jsonify({
            'success': False,
            'message': f"Failed to update expense: {str(e)}"
        })

    finally:
        db_session.close()


@expense_routes.route('/expenses/dashboard', methods=["GET"])
@login_required
def expenses_dashboard():
    db_session = Session()
    try:
        app_id = current_user.app_id
        role = current_user.role
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        module_name = "Expenses"
        api_key = company.api_key

        # Get base currency information
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency = base_currency_info["base_currency"]

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        # Get all expense categories (filtered by parent_account_type = 'Expense')
        expense_categories = db_session.query(Category).filter_by(
            app_id=app_id,
            account_type='Expense'
        ).all()

        # Get all expense subcategories
        expense_subcategories = db_session.query(ChartOfAccounts).join(Category).filter(
            ChartOfAccounts.app_id == app_id,
            Category.account_type == 'Expense'
        ).all()

        # Get payment modes used in expense journals
        payment_modes = db_session.query(PaymentMode).join(Journal).filter(
            PaymentMode.app_id == app_id,
            Journal.status == 'Posted'  # Only consider posted journals
        ).distinct().all()

        # Get vendors from expense journals
        vendors = db_session.query(Vendor).join(Journal).filter(
            Vendor.app_id == app_id,
            Journal.status == 'Posted'  # Only consider posted journals
        ).distinct().all()

        # Get projects from expense journals
        projects = db_session.query(Project).join(Journal).filter(
            Project.app_id == app_id,
            Journal.status == 'Posted'  # Only consider posted journals
        ).distinct().all()

        return render_template(
            '/expenses/expense_dashboard.html',
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name,
            api_key=api_key,
            base_currency=base_currency,
            categories=expense_categories,
            subcategories=expense_subcategories,
            payment_modes=payment_modes,
            vendors=vendors,
            projects=projects,
            currencies=currencies
        )

    finally:
        db_session.close()


@expense_routes.route('/expenses/get_expense_payment_categories/<account_type>')
@login_required
def get_expense_payment_categories(account_type):
    db_session = Session()
    try:
        # Base query with app_id filter
        query = db_session.query(Category).filter(
            Category.account_type == account_type,
            Category.app_id == current_user.app_id
        )

        # For Assets, only show cash or bank accounts
        if account_type == 'Asset':
            query = query.join(ChartOfAccounts, Category.id == ChartOfAccounts.category_fk).filter(
                ChartOfAccounts.app_id == current_user.app_id,
                or_(
                    ChartOfAccounts.is_cash == True,
                    ChartOfAccounts.is_bank == True
                )
            )

        # Execute query
        categories = query.distinct().all()  # Added distinct() to avoid duplicates

        # Prepare JSON response
        category_list = [{
            'id': category.id,
            'category_id': category.category_id,
            'category_name': category.category
        } for category in categories]

        # Add logging to verify results
        logger.info(f"Found {len(category_list)} {account_type} categories for app_id {current_user.app_id}")

        return jsonify(category_list)

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error fetching categories: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

    finally:
        db_session.close()
