# app/routes/fund_transfers/fund_transfer.py


import logging
import traceback
from datetime import datetime, time
from decimal import Decimal, ROUND_HALF_UP

from flask import jsonify, request, redirect, url_for, render_template, flash
from flask_login import login_required, current_user
from sqlalchemy import func, case, distinct, or_, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from ai import resolve_exchange_rate_for_transaction, get_base_currency
from db import Session
from models import Company, Module, Currency, Department, Employee, \
    PayrollPeriod, Deduction, BenefitType, \
    DeductionType, PaymentMode, DeductionPayment, AdvancePayment, User, AdvanceRepayment, ChartOfAccounts, \
    Category, UserPreference, CurrencyExchangeTransaction, ExchangeRate, JournalEntry, Journal
from services.post_to_ledger import create_fund_transfer_journal
from services.post_to_ledger_reversal import repost_transaction, delete_posted_fund_transfer_journals, \
    delete_journal_entries_by_source
from utils import create_notification, apply_date_filters
from utils_and_helpers.exchange_rates import get_or_create_fx_clearing_account

# Import the same blueprint
from . import fund_transfers_bp

logger = logging.getLogger()


@fund_transfers_bp.route('/currency_exchange_transactions', methods=['GET'])
@login_required
def view_exchange_transactions():
    with Session() as db_session:
        try:
            app_id = current_user.app_id
            db_session = Session()

            # Fetch necessary data
            company = db_session.query(Company).filter_by(id=app_id).first()
            role = current_user.role
            modules_data = [mod.module_name for mod in
                            db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

            do_not_show_again = db_session.query(UserPreference.do_not_show_again).filter_by(
                preference_type="exchange_transaction_confirmation", user_id=current_user.id).scalar()
            # Get filter parameters (all optional)
            filters = {
                'from_currency': request.args.get('from_currency'),
                'to_currency': request.args.get('to_currency'),
                'start_date': request.args.get('start_date'),
                'end_date': request.args.get('end_date'),
                'filter_type': request.args.get('filter_type', 'date')
            }

            query = db_session.query(CurrencyExchangeTransaction).filter_by(app_id=app_id).order_by(
                CurrencyExchangeTransaction.exchange_date.desc())

            # Apply currency filters if specified
            if filters['from_currency']:
                query = query.filter(CurrencyExchangeTransaction.from_currency_id == filters['from_currency'])
            if filters['to_currency']:
                query = query.filter(CurrencyExchangeTransaction.to_currency_id == filters['to_currency'])

            # Apply date filters if specified
            query = apply_date_filters(
                query=query,
                start_date=filters['start_date'],
                end_date=filters['end_date'],
                filter_type=filters['filter_type'],
                model=CurrencyExchangeTransaction,
                date_field='exchange_date',
                date_added_field='date'
            )

            # Execute and format results
            exchange_transactions = query.all()
            # Get all currencies for dropdowns
            currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

            return render_template(
                'currency_exchange_transactions.html',
                transactions=exchange_transactions,
                currencies=currencies,
                company=company,
                role=role,
                modules=modules_data,
                current_filters=filters,
                do_not_show_again=do_not_show_again,
                module_name="General Ledger"
            )

        except Exception as e:
            db_session.rollback()
            flash(f'Error fetching exchange rates: {str(e)}', 'danger')
            logger.error(f'error is {e}')
            return redirect(request.referrer)


@fund_transfers_bp.route('/submit_exchange_transaction', methods=['POST'])
@login_required
def submit_exchange_transaction():
    if request.method == 'POST':
        with Session() as db_session:
            try:
                # Get form data
                from_account_id = request.form.get('from_account_id')
                to_account_id = request.form.get('to_account_id')
                from_amount = request.form.get('from_amount')
                to_amount = request.form.get('to_amount')
                from_currency_id = request.form.get('from_currency')
                to_currency_id = request.form.get('currency')
                exchange_date = request.form.get('exchange_date')
                exchange_time_str = request.form.get('exchange_time')
                description = request.form.get('description', '')
                payment_mode_id = request.form.get('payment_mode_id')
                project_id = request.form.get('project_id')
                vendor_id = request.form.get('vendor_id')

                # Check if same currency
                if from_currency_id == to_currency_id:
                    if from_amount != to_amount:
                        return jsonify({
                            'status': 'error',
                            'message': 'For same currency transfers, the amounts must be equal'
                        }), 400

                # Basic validation
                if not all([from_account_id, to_account_id, from_amount, to_amount, exchange_date]):
                    return jsonify({
                        'status': 'error',
                        'message': 'All required fields must be filled'
                    }), 400

                # Convert to proper types
                try:
                    from_account_id = int(from_account_id)
                    to_account_id = int(to_account_id)
                    from_amount = float(from_amount)
                    to_amount = float(to_amount)
                    from_currency_id = int(from_currency_id)
                    to_currency_id = int(to_currency_id)
                    exchange_date = datetime.strptime(exchange_date, '%Y-%m-%d').date()

                    # Handle optional fields
                    payment_mode_id = int(payment_mode_id) if payment_mode_id else None
                    project_id = int(project_id) if project_id else None
                    vendor_id = int(vendor_id) if vendor_id else None

                    # Time parsing
                    if exchange_time_str:
                        try:
                            if len(exchange_time_str.split(':')) == 2:
                                exchange_time = datetime.strptime(exchange_time_str, '%H:%M').time()
                            else:
                                exchange_time = datetime.strptime(exchange_time_str, '%H:%M:%S').time()
                        except ValueError as e:
                            logger.error(f"Error parsing time: {e}")
                            exchange_time = None
                    else:
                        exchange_time = None
                except (ValueError, TypeError) as e:
                    return jsonify({
                        'status': 'error',
                        'message': 'Invalid data format'
                    }), 400

                # Get base currency info for the company
                base_currency_info = get_base_currency(db_session, current_user.app_id)
                if not base_currency_info:
                    return jsonify({'status': 'error', 'message': 'Base currency not defined for this company'}), 400

                base_currency_id = base_currency_info["base_currency_id"]
                base_currency_name = base_currency_info["base_currency"]

                # Calculate exchange rate
                if int(to_currency_id) == int(base_currency_id):
                    exchange_rate = (
                            Decimal(str(to_amount)) / Decimal(str(from_amount))
                    ).quantize(Decimal('0.0000000001'), rounding=ROUND_HALF_UP)
                else:
                    exchange_rate = (
                            Decimal(str(from_amount)) / Decimal(str(to_amount))
                    ).quantize(Decimal('0.0000000001'), rounding=ROUND_HALF_UP)

                # Create the CurrencyExchangeTransaction record
                new_transaction = CurrencyExchangeTransaction(
                    from_account_id=from_account_id,
                    to_account_id=to_account_id,
                    from_currency_id=from_currency_id,
                    to_currency_id=to_currency_id,
                    from_amount=from_amount,
                    to_amount=to_amount,
                    exchange_rate=exchange_rate,
                    exchange_date=exchange_date,
                    exchange_time=exchange_time,
                    description=description,
                    datetime_added=datetime.now(),
                    app_id=current_user.app_id,
                    is_posted_to_ledger=True,  # Not posted yet
                    status="complete"  # Initial status
                )

                db_session.add(new_transaction)
                db_session.flush()  # Get the transaction ID

                # Prepare data for journal creation
                transaction_data = {
                    'from_account_id': from_account_id,
                    'to_account_id': to_account_id,
                    'from_currency_id': from_currency_id,
                    'to_currency_id': to_currency_id,
                    'from_amount': from_amount,
                    'to_amount': to_amount,
                    'exchange_rate': float(exchange_rate),
                    'exchange_date': exchange_date,
                    'exchange_time': exchange_time,
                    'description': description,
                    'payment_mode_id': payment_mode_id,
                    'project_id': project_id,
                    'vendor_id': vendor_id,
                    'source_id': new_transaction.id  # Link to the CurrencyExchangeTransaction
                }

                # Create the journal with Unposted status
                journal_result = create_fund_transfer_journal(
                    db_session=db_session,
                    transaction_data=transaction_data,
                    created_by_user_id=current_user.id,
                    app_id=current_user.app_id,
                    base_currency_id=base_currency_id
                )

                db_session.commit()

                # In submit_exchange_transaction, after db_session.commit():
                return jsonify({
                    'status': 'success',
                    'message': 'Exchange transaction created successfully',
                    'transaction_id': new_transaction.id,
                    'journals_created': journal_result.get('journals_created', 1),
                    'journal_numbers': [
                        journal_result.get('journal_1_number'),
                        journal_result.get('journal_2_number')  # Will be None for single journal
                    ]
                })

            except ValueError as e:
                db_session.rollback()
                logger.error(f'Validation error: {e}')
                return jsonify({
                    'status': 'error',
                    'message': str(e)
                }), 400

            except SQLAlchemyError as e:
                db_session.rollback()
                logger.error(f'Database error: {e}')
                return jsonify({
                    'status': 'error',
                    'message': 'Database error occurred',
                    'details': str(e)
                }), 500

            except Exception as e:
                db_session.rollback()
                logger.error(f'Unexpected error: {e} \n{traceback.format_exc()}')
                return jsonify({
                    'status': 'error',
                    'message': 'An unexpected error occurred',
                    'details': str(e)
                }), 500


@fund_transfers_bp.route('/exchange_transactions/<int:transaction_id>/post', methods=['POST'])
@login_required
def post_exchange_to_ledger(transaction_id):
    with Session() as db_session:
        try:
            # Load transaction
            transaction = db_session.query(CurrencyExchangeTransaction).filter_by(
                id=transaction_id, app_id=current_user.app_id
            ).first()

            if not transaction:
                return jsonify({'status': 'error', 'message': 'Transaction not found'}), 404

            if transaction.is_posted_to_ledger:
                return jsonify({'status': 'error', 'message': 'Transaction already posted'}), 400

            # Find ALL journals associated with this transaction
            journals = db_session.query(Journal).filter(
                Journal.app_id == current_user.app_id,
                Journal.entries.any(and_(
                    JournalEntry.source_type == 'fund_transfer',
                    JournalEntry.source_id == transaction_id
                ))
            ).all()

            if not journals:
                return jsonify({'status': 'error', 'message': 'No journals found for this transaction'}), 400

            # Ensure no earlier unposted transactions
            prior_unposted = db_session.query(CurrencyExchangeTransaction).filter(
                CurrencyExchangeTransaction.app_id == current_user.app_id,
                CurrencyExchangeTransaction.is_posted_to_ledger == False,
                CurrencyExchangeTransaction.id != transaction.id,
                or_(
                    CurrencyExchangeTransaction.exchange_date < transaction.exchange_date,
                    and_(
                        CurrencyExchangeTransaction.exchange_date == transaction.exchange_date,
                        CurrencyExchangeTransaction.exchange_time < (transaction.exchange_time or time.min)
                    )
                )
            ).count()

            if prior_unposted > 0:
                return jsonify({
                    "status": "error",
                    "message": "You have earlier fund transfer transactions that haven't been posted to the ledger. Please post those first."
                }), 400

            # Update ALL journals status from 'Unposted' to 'Posted'
            for journal in journals:
                journal.status = 'Posted'
                journal.updated_by = current_user.id
                journal.updated_at = datetime.now()

            # Mark transaction as posted - THIS IS WHEN IT ACTUALLY GETS POSTED
            transaction.is_posted_to_ledger = True
            transaction.status = "complete"
            transaction.datetime_modified = datetime.now()

            db_session.commit()

            return jsonify({
                'status': 'success',
                'message': 'Transaction posted to ledger successfully',
                'transaction_id': transaction.id,
                'is_posted_to_ledger': True,  # Now it's posted
                'journals_posted': len(journals),
                'journal_numbers': [journal.journal_number for journal in journals]
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f'Error posting fund transfer transaction: {e}')
            return jsonify({
                'status': 'error',
                'message': 'Failed to post transaction to ledger',
                'detail': str(e)
            }), 500


@fund_transfers_bp.route('/exchange_transactions/<int:transaction_id>/update', methods=['POST'])
@login_required
def update_exchange_transaction(transaction_id):
    with Session() as db_session:
        try:
            # Get the transaction to update with exchange rates
            transaction = db_session.query(CurrencyExchangeTransaction).options(
                joinedload(CurrencyExchangeTransaction.exchange_rates)
            ).filter_by(
                id=transaction_id,
                app_id=current_user.app_id
            ).first()

            if not transaction:
                return jsonify({'status': 'error', 'message': 'Transaction not found'}), 404

            # Get form data
            to_amount = Decimal(request.form['to_amount'])
            from_amount = Decimal(request.form['from_amount'])
            exchange_date = request.form['exchange_date']
            exchange_time_str = request.form.get('exchange_time')
            description = request.form.get('description', '')

            # Parse date and time
            try:
                exchange_date = datetime.strptime(exchange_date, '%Y-%m-%d').date()

                if exchange_time_str:
                    try:
                        if len(exchange_time_str.split(':')) == 2:
                            exchange_time = datetime.strptime(exchange_time_str, '%H:%M').time()
                        else:
                            exchange_time = datetime.strptime(exchange_time_str, '%H:%M:%S').time()
                    except ValueError as e:
                        logger.error(f"Error parsing time: {e}")
                        exchange_time = None
                else:
                    exchange_time = None
            except ValueError as e:
                return jsonify({
                    'status': 'error',
                    'message': 'Invalid date/time format'
                }), 400

            # Get base currency info for the company
            base_currency_info = get_base_currency(db_session, current_user.app_id)
            if not base_currency_info:
                return jsonify({
                    'status': 'error',
                    'message': 'Base currency not defined for this company'
                }), 400

            base_currency_id = base_currency_info["base_currency_id"]

            # Calculate exchange rate based on currency direction
            if transaction.to_currency_id == int(base_currency_id):
                exchange_rate = round(1 / (from_amount / to_amount), 10)
            else:
                exchange_rate = round(from_amount / to_amount, 10)

            # Store old values for comparison
            old_from_amount = transaction.from_amount
            old_to_amount = transaction.to_amount
            old_exchange_date = transaction.exchange_date

            # Update transaction fields
            transaction.exchange_date = exchange_date
            transaction.exchange_time = exchange_time
            transaction.description = description
            transaction.from_amount = from_amount
            transaction.to_amount = to_amount
            transaction.exchange_rate = exchange_rate
            transaction.datetime_modified = datetime.now()
            transaction.is_posted_to_ledger = True
            transaction.status = "complete"

            # Delete existing exchange rate records
            exchange_rates_deleted = 0
            if transaction.exchange_rates:
                for exchange_rate_obj in transaction.exchange_rates:
                    db_session.delete(exchange_rate_obj)
                    exchange_rates_deleted += 1

            # Handle journal updates - delete existing journals and create new ones
            # Find existing journals for this transaction
            existing_journals = db_session.query(Journal).filter(
                Journal.app_id == current_user.app_id,
                Journal.entries.any(and_(
                    JournalEntry.source_type == 'fund_transfer',
                    JournalEntry.source_id == transaction_id
                ))
            ).all()

            # Delete existing journals if they exist
            journals_deleted = 0
            if existing_journals:
                for journal in existing_journals:
                    # Delete journal entries first
                    db_session.query(JournalEntry).filter(
                        JournalEntry.journal_id == journal.id
                    ).delete()
                    # Delete the journal
                    db_session.delete(journal)
                    journals_deleted += 1

            # Create new journals with updated data
            transaction_data = {
                'from_account_id': transaction.from_account_id,
                'to_account_id': transaction.to_account_id,
                'from_currency_id': transaction.from_currency_id,
                'to_currency_id': transaction.to_currency_id,
                'from_amount': float(from_amount),
                'to_amount': float(to_amount),
                'exchange_rate': float(exchange_rate),
                'exchange_date': exchange_date,
                'exchange_time': exchange_time,
                'description': description,
                'payment_mode_id': None,  # You can add these to form if needed
                'project_id': None,
                'vendor_id': None,
                'source_id': transaction.id
            }

            # Create new journals (this will create new exchange rate records)
            journal_result = create_fund_transfer_journal(
                db_session=db_session,
                transaction_data=transaction_data,
                created_by_user_id=current_user.id,
                app_id=current_user.app_id,
                base_currency_id=base_currency_id
            )

            db_session.commit()

            response_data = {
                'status': 'success',
                'message': 'Transaction updated successfully',
                'transaction_id': transaction.id,
                'journals_recreated': journal_result['journals_created'],
                'journals_deleted': journals_deleted,
                'exchange_rates_deleted': exchange_rates_deleted,
                'updated_fields': {
                    'date': exchange_date.strftime('%Y-%m-%d'),
                    'time': exchange_time.strftime('%H:%M:%S') if exchange_time else None,
                    'amounts': {
                        'from': float(from_amount),
                        'to': float(to_amount)
                    },
                    'exchange_rate': float(exchange_rate)
                }
            }

            # Add journal details to response
            for i in range(1, journal_result['journals_created'] + 1):
                response_data[f'journal_{i}_id'] = journal_result[f'journal_{i}_id']
                response_data[f'journal_{i}_number'] = journal_result[f'journal_{i}_number']

            return jsonify(response_data)

        except Exception as e:
            db_session.rollback()
            logger.error(f'Error updating transaction {transaction_id}: {str(e)}')
            return jsonify({
                'status': 'error',
                'message': 'Failed to update transaction',
                'details': str(e)
            }), 500


@fund_transfers_bp.route('/api/exchange_transactions/<int:transaction_id>')
@login_required
def get_exchange_transaction(transaction_id):
    db_session = Session()
    try:

        transaction = db_session.query(CurrencyExchangeTransaction).filter_by(id=transaction_id,
                                                                              app_id=current_user.app_id).first()
        return jsonify({
            'id': transaction.id,
            'exchange_date': transaction.exchange_date.isoformat(),
            'exchange_time': transaction.exchange_time.strftime('%H:%M:%S') if transaction.exchange_time else None,
            'description': transaction.description,
            'from_account': {
                'id': transaction.from_account.id,
                'sub_category': transaction.from_account.sub_category
            },
            'from_currency': {
                'id': transaction.from_currency.id,
                'user_currency': transaction.from_currency.user_currency
            },
            'from_amount': float(transaction.from_amount),
            'to_account': {
                'id': transaction.to_account.id,
                'sub_category': transaction.to_account.sub_category
            },
            'to_currency': {
                'id': transaction.to_currency.id,
                'user_currency': transaction.to_currency.user_currency
            },
            'to_amount': float(transaction.to_amount)
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f'An error occured {e}')
        return jsonify({'error': str(e)}), 500

    finally:
        db_session.close()


@fund_transfers_bp.route('/exchange_transactions/<int:transaction_id>/delete', methods=['POST'])
@login_required
def cancel_exchange_transaction(transaction_id):
    db_session = Session()
    try:
        # Get the transaction
        transaction = db_session.query(CurrencyExchangeTransaction).filter_by(
            id=transaction_id, app_id=current_user.app_id
        ).first()

        if not transaction:
            return jsonify({
                'status': 'error',
                'message': 'Transaction not found'
            }), 404

        # Delete posted journals for this specific transaction

        for source_type in ['fund_transfer']:
            delete_journal_entries_by_source(db_session, source_type, transaction_id, current_user.app_id)


        # Update transaction status
        transaction.status = "cancelled"
        transaction.is_posted_to_ledger = False
        transaction.datetime_modified = datetime.now()

        db_session.commit()

        return jsonify({
            'status': 'success',
            'message': 'Transaction deleted successfully',
        })
    except Exception as e:
        db_session.rollback()
        logger.error(f"Error deleting exchange transaction {transaction_id}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
    finally:
        db_session.close()


@fund_transfers_bp.route('/submit_exchange_rates', methods=['POST'])
@login_required
def submit_exchange_rates():
    db_session = Session()
    app_id = current_user.app_id

    try:
        base_currency = int(request.form.get('base_currency_id'))
        date = datetime.strptime(request.form['date'], '%Y-%m-%d').date()

        updated_currencies = []  # Track which rates were updated
        new_currencies = []  # Track which rates were newly created

        for currency in db_session.query(Currency).filter_by(app_id=app_id).all():
            if currency.id == base_currency:
                continue  # Skip base currency

            if to_value := request.form.get(f'inverse_value_{currency.id}'):
                rate = Decimal(to_value)

                # Check for existing rate
                existing = db_session.query(ExchangeRate).filter(
                    ExchangeRate.app_id == app_id,
                    ExchangeRate.from_currency_id == currency.id,
                    ExchangeRate.to_currency_id == base_currency,
                    ExchangeRate.date == date
                ).first()

                if existing:
                    # Check if rate actually changed
                    if existing.rate != rate:
                        existing.rate = rate
                        updated_currencies.append(currency.user_currency)
                    # If rate is same, no change needed
                else:
                    # Add new rate
                    db_session.add(ExchangeRate(
                        app_id=app_id,
                        from_currency_id=currency.id,
                        to_currency_id=base_currency,
                        rate=rate,
                        date=date
                    ))
                    new_currencies.append(currency.user_currency)

        db_session.commit()

        # Build informative message
        message_parts = []
        if new_currencies:
            message_parts.append(f"Created new rates for: {', '.join(new_currencies)}")
        if updated_currencies:
            message_parts.append(f"Updated rates for: {', '.join(updated_currencies)}")

        if not message_parts:
            message = "No changes made - rates already up to date"
        else:
            message = "Rates saved successfully. " + ". ".join(message_parts)

        return jsonify({
            'status': 'success',
            'message': message,
            'details': {
                'new': new_currencies,
                'updated': updated_currencies
            }
        })

    except ValueError as e:
        db_session.rollback()
        return jsonify({'status': 'error', 'message': f'Invalid data: {str(e)}'}), 400
    except Exception as e:
        db_session.rollback()
        return jsonify({'status': 'error', 'message': f'System error: {str(e)}'}), 500
    finally:
        db_session.close()
