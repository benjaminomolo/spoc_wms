# app/routes/payroll/record_payment.py

import logging
import traceback
from datetime import datetime, date
from decimal import Decimal

from sqlalchemy.orm import joinedload

from ai import resolve_exchange_rate_for_transaction
from models import Deduction, DeductionPayment, PaymentStatusEnum, PayrollTransaction, PayrollPayment, PayrollPeriod, \
    PayRollStatusEnum, Currency
from services.payroll_helpers import update_payroll_period_status
from services.post_to_ledger import post_payroll_to_ledger, post_payroll_payment_to_ledger, \
    create_payroll_payment_journal_entries, create_deduction_payment_journal_entries
from services.post_to_ledger_reversal import repost_transaction, update_transaction, update_payroll_payment_to_ledger, \
    update_deduction_payment_to_ledger, delete_journal_entries_by_source
from utils_and_helpers.exchange_rates import get_or_create_exchange_rate_for_transaction
from . import payroll_bp
from flask import Blueprint, jsonify, render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from db import Session

logger = logging.getLogger()


@payroll_bp.route('/make_payment/<int:payrollTransactionId>', methods=['POST'])
def make_payment(payrollTransactionId):
    db_session = Session()
    try:
        data = request.json

        # Convert amount to Decimal
        amount = Decimal(data.get('amount'))
        # Try to convert 'paymentDate' from string to a datetime object
        payment_date_str = data.get('paymentDate') or None

        if payment_date_str:
            try:
                # Assuming the date format in the request is 'YYYY-MM-DD'
                payment_date = datetime.strptime(payment_date_str, '%Y-%m-%d').date()  # Convert to date only
            except ValueError:
                return jsonify({"success": False, "message": "Invalid date format. Expected format: YYYY-MM-DD"}), 400
        else:
            # Default to today's date if not provided
            payment_date = date.today()

        payment_method = int(data.get('paymentMethod')) if data.get(
            'paymentMethod') else None  # Ensure this is a valid enum value
        payment_account = int(data.get('fundingAccount')) if data.get(
            'fundingAccount') else None  # Ensure this is an integer (foreign key)
        reference = data.get('reference') or None
        fx_gain_loss = Decimal(str(data.get('fx_gain_loss', 0)))
        notes = data.get('notes') or None
        base_currency_id = data.get('base_currency_id')
        exchange_rate = Decimal(str(data.get('exchange_rate', 1))) if data.get('exchange_rate') else None

        # Fetch payroll transaction with employee details
        payroll_transaction = db_session.query(PayrollTransaction).options(
            joinedload(PayrollTransaction.employees)
        ).filter_by(id=payrollTransactionId).first()

        if not payroll_transaction:
            return jsonify({"success": False, "message": "Payroll transaction not found"}), 404

        # Ensure payment does not exceed balance due
        if amount > payroll_transaction.balance_due:
            return jsonify({"success": False, "message": "Payment amount exceeds balance due"}), 400

        currency_id = payroll_transaction.currency_id

        exchange_rate_id = None
        currency_id = payroll_transaction.currency_id
        rate_obj = None

        if base_currency_id and currency_id != base_currency_id:
            # Exchange rate is required for foreign currency
            if not exchange_rate:
                return jsonify({
                    'success': False,
                    'message': 'Exchange rate is required for foreign currency payments'
                }), 400

            # Create exchange rate record
            rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                session=db_session,
                action='create',
                from_currency_id=currency_id,
                to_currency_id=base_currency_id,
                rate_value=float(exchange_rate),
                rate_date=payment_date,
                app_id=current_user.app_id,
                created_by=current_user.id,
                source_type='payroll_payment',
                source_id=None,
                currency_exchange_transaction_id=None
            )
            exchange_rate_id = rate_id

        # Create payroll payment entry
        payroll_payment = PayrollPayment(
            payroll_transaction_id=payrollTransactionId,
            payment_date=payment_date,
            amount=amount,
            currency_id=payroll_transaction.currency_id,
            payment_account=payment_account,
            payment_method=payment_method,
            reference=reference,
            notes=notes,
            is_posted_to_ledger=True,
            exchange_rate_id=exchange_rate_id,
            created_by=current_user.id
        )
        db_session.add(payroll_payment)
        db_session.flush()  # Get the payroll_payment ID

        if rate_obj:
            rate_obj.source_id = payroll_payment.id
            db_session.add(rate_obj)

        # ✅ Create journal entries for the payment (Posted status)
        journal = create_payroll_payment_journal_entries(
            db_session=db_session,
            payment_account_id=int(payment_account),
            payroll_transaction=payroll_transaction,
            payroll_payment=payroll_payment,
            current_user_id=current_user.id,
            app_id=current_user.app_id,
            exchange_rate_id=exchange_rate_id,
            base_currency_id=base_currency_id,
            status="Posted"

        )

        if not journal:
            db_session.rollback()
            return jsonify({"success": False, "message": "Failed to create accounting entries for payment"}), 500

        # Update payroll transaction
        payroll_transaction.paid_amount += amount
        payroll_transaction.balance_due -= amount
        payroll_transaction.payment_status = PaymentStatusEnum.PAID if payroll_transaction.balance_due == 0 else PaymentStatusEnum.PARTLY_PAID

        # Now, check all transactions for the same payroll_period
        payroll_period = db_session.query(PayrollPeriod).filter_by(id=payroll_transaction.payroll_period_id).first()

        if payroll_period:
            # Check if all transactions for this payroll_period are fully paid
            all_paid = db_session.query(PayrollTransaction).filter_by(payroll_period_id=payroll_period.id).all()

            # If all transactions have a zero balance_due, mark the period as PAID
            if all(p.balance_due == 0 for p in all_paid):
                payroll_period.payment_status = PayRollStatusEnum.PAID
            else:
                payroll_period.payment_status = PayRollStatusEnum.PARTIAL

        db_session.commit()

        return jsonify({"success": True, "message": "Payment recorded successfully"}), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error is {e}')
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_session.close()


@payroll_bp.route('/pay_all_payroll_in_full/<int:payrollPeriodId>', methods=['POST'])
def pay_all_payroll_in_full(payrollPeriodId):
    db_session = Session()
    try:
        data = request.json

        # Convert 'paymentDate' from string to a datetime object
        payment_date_str = data.get('paymentDate') or None
        if payment_date_str:
            try:
                payment_date = datetime.strptime(payment_date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({"success": False, "message": "Invalid date format. Expected format: YYYY-MM-DD"}), 400
        else:
            payment_date = datetime.today()

        payment_method = int(data.get('fullPaymentMethod')) if data.get('fullPaymentMethod') else None
        payment_account = int(data.get('fullFundingAccount')) if data.get('fullFundingAccount') else None
        reference = data.get('reference') or None
        notes = data.get('notes') or None
        # Get exchange rates per currency from frontend
        exchange_rates = data.get('exchange_rates', {})
        fx_gain_losses = data.get('fx_gain_losses', {})
        base_currency_id = data.get('base_currency_id')

        # Fetch all unpaid payroll records for the given payroll period
        payroll_records = db_session.query(PayrollTransaction).filter(
            PayrollTransaction.payroll_period_id == payrollPeriodId,
            PayrollTransaction.balance_due > 0
        ).all()

        if not payroll_records:
            return jsonify(
                {"success": False, "message": "No outstanding payroll records found for this payroll period"}), 404

        # Process payments for each payroll record
        for payroll in payroll_records:
            payment_amount = payroll.balance_due  # Pay off the entire balance

            currency_id = payroll.currency_id

            exchange_rate_id, notification = resolve_exchange_rate_for_transaction(session=db_session,
                                                                                   currency_id=currency_id,
                                                                                   transaction_date=payment_date,
                                                                                   app_id=current_user.app_id)

            # Create payroll payment entry
            payroll_payment = PayrollPayment(
                payroll_transaction_id=payroll.id,
                payment_date=payment_date,
                amount=payment_amount,
                currency_id=payroll.currency_id,
                payment_account=payment_account,
                payment_method=payment_method,
                reference=reference,
                notes=notes,
                exchange_rate_id=exchange_rate_id,
                is_posted_to_ledger=False
            )
            db_session.add(payroll_payment)

            # Update payroll record
            payroll.paid_amount += payment_amount
            payroll.balance_due = 0
            payroll.is_posted_to_ledger = False
            payroll.payment_status = PaymentStatusEnum.PAID

        # After processing all payroll records, update the payroll period status to PAID
        payroll_period = db_session.query(PayrollPeriod).filter_by(id=payrollPeriodId).first()
        if payroll_period:
            payroll_period.payment_status = PayRollStatusEnum.PAID
            db_session.add(payroll_period)

        db_session.commit()
        return jsonify({"success": True, "message": "All payroll records for this period have been paid in full"}), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error: {e}')
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_session.close()


@payroll_bp.route('/pay_selected_payroll_in_full', methods=['POST'])
def pay_selected_payroll_in_full():
    db_session = Session()
    try:
        data = request.json

        # Get the list of selected payroll transactions from frontend
        selected_payments = data.get('selected_payments', [])

        if not selected_payments:
            selected_ids = data.get('selected_ids', [])
        else:
            selected_ids = [p['id'] for p in selected_payments]

        if not selected_ids:
            return jsonify({"success": False, "message": "No payroll records selected"}), 400

        exchange_rates = data.get('exchange_rates', {})
        fx_gain_losses = data.get('fx_gain_losses', {})

        payment_date_str = data.get('paymentDate') or data.get('fullPaymentDate')
        if payment_date_str:
            try:
                payment_date = datetime.strptime(payment_date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({"success": False, "message": "Invalid date format"}), 400
        else:
            payment_date = date.today()

        payment_method = int(data.get('fullPaymentMethod')) if data.get('fullPaymentMethod') else None
        payment_account = int(data.get('fullFundingAccount')) if data.get('fullFundingAccount') else None
        reference = data.get('reference') or None
        notes = data.get('notes') or None
        base_currency_id = int(data.get('base_currency_id'))

        # Fetch payroll records with lock for atomic processing
        payroll_records = db_session.query(PayrollTransaction).filter(
            PayrollTransaction.id.in_(selected_ids),
            PayrollTransaction.balance_due > 0
        ).with_for_update().all()

        if not payroll_records:
            return jsonify({"success": False, "message": "No outstanding payroll records found"}), 404

        affected_periods = set()
        successful_payments = 0
        failed_payments = []

        # Process all payments within the same transaction
        for payroll in payroll_records:
            try:
                payment_amount = payroll.balance_due
                if selected_payments:
                    matching = next((p for p in selected_payments if p['id'] == payroll.id), None)
                    if matching:
                        payment_amount = matching['amount']

                currency_id = payroll.currency_id
                exchange_rate_id = None
                exchange_rate_obj = None

                if base_currency_id and currency_id != base_currency_id:
                    rate_value = exchange_rates.get(str(currency_id))

                    if not rate_value:
                        raise Exception(
                            f'Exchange rate required for currency {payroll.employees.currency.user_currency}')

                    exchange_rate_id, exchange_rate_obj = get_or_create_exchange_rate_for_transaction(
                        session=db_session,
                        action='create',
                        from_currency_id=currency_id,
                        to_currency_id=base_currency_id,
                        rate_value=float(rate_value),
                        rate_date=payment_date,
                        app_id=current_user.app_id,
                        created_by=current_user.id,
                        source_type='payroll_payment',
                        source_id=None,
                        currency_exchange_transaction_id=None
                    )
                else:
                    exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
                        session=db_session,
                        currency_id=currency_id,
                        transaction_date=payment_date,
                        app_id=current_user.app_id
                    )

                # Create payroll payment entry
                payroll_payment = PayrollPayment(
                    payroll_transaction_id=payroll.id,
                    payment_date=payment_date,
                    amount=payment_amount,
                    currency_id=payroll.currency_id,
                    payment_account=payment_account,
                    payment_method=payment_method,
                    reference=reference,
                    notes=notes,
                    exchange_rate_id=exchange_rate_id,
                    is_posted_to_ledger=True,
                    created_by=current_user.id
                )
                db_session.add(payroll_payment)
                db_session.flush()

                if exchange_rate_obj:
                    exchange_rate_obj.source_id = payroll_payment.id
                    db_session.add(exchange_rate_obj)

                # Create journal entries
                journal = create_payroll_payment_journal_entries(
                    db_session=db_session,
                    payroll_transaction=payroll,
                    payroll_payment=payroll_payment,
                    payment_account_id=payment_account,
                    current_user_id=current_user.id,
                    app_id=current_user.app_id,
                    exchange_rate_id=exchange_rate_id,
                    base_currency_id=base_currency_id,
                    status="Posted"
                )

                if not journal:
                    raise Exception('Failed to create journal entries')

                # Update payroll record
                payroll.paid_amount = Decimal(payroll.paid_amount or 0) + Decimal(payment_amount)
                payroll.balance_due = Decimal(payroll.net_salary) - Decimal(payroll.paid_amount)

                if payroll.balance_due <= 0:
                    payroll.payment_status = PaymentStatusEnum.PAID
                else:
                    payroll.payment_status = PaymentStatusEnum.PARTLY_PAID

                affected_periods.add(payroll.payroll_period_id)
                successful_payments += 1

            except Exception as e:
                # If any payment fails, rollback everything and raise
                db_session.rollback()
                logger.error(f"Error processing payment for payroll {payroll.id}: {e} \n{traceback.format_exc()}")
                raise Exception(
                    f"Payment failed for {payroll.employees.first_name} {payroll.employees.last_name}: {str(e)}")

        # Update status for affected payroll periods (only if all payments succeeded)
        for period_id in affected_periods:
            payroll_period = db_session.query(PayrollPeriod).filter_by(id=period_id).first()
            if payroll_period:
                all_transactions = db_session.query(PayrollTransaction).filter_by(
                    payroll_period_id=period_id
                ).all()

                if all(p.balance_due == 0 for p in all_transactions):
                    payroll_period.payment_status = PayRollStatusEnum.PAID
                else:
                    payroll_period.payment_status = PayRollStatusEnum.PARTIAL

                db_session.add(payroll_period)

        # Commit everything atomically
        db_session.commit()

        return jsonify({
            "success": True,
            "message": f"Successfully paid {successful_payments} payroll records",
            "results": {
                "successful": successful_payments,
                "failed": 0,
                "failed_details": []
            }
        }), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error: {e}')
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_session.close()


@payroll_bp.route('/make_deduction_payment/<int:deductionId>', methods=['POST'])
def make_deduction_payment(deductionId):
    db_session = Session()
    try:
        data = request.json

        # Convert amount to Decimal
        amount = Decimal(data.get('amount'))

        # Convert 'paymentDate' from string to a datetime object
        payment_date_str = data.get('paymentDate') or None
        if payment_date_str:
            try:
                payment_date = datetime.strptime(payment_date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({"success": False, "message": "Invalid date format. Expected format: YYYY-MM-DD"}), 400
        else:
            payment_date = date.today()

        payment_method = int(data.get('paymentMethod')) if data.get('paymentMethod') else None
        payment_account = int(data.get('fundingAccount')) if data.get('fundingAccount') else None
        reference = data.get('reference') or None
        notes = data.get('notes') or None
        fx_gain_loss = Decimal(str(data.get('fx_gain_loss', 0)))
        base_currency_id = data.get('base_currency_id')
        exchange_rate = Decimal(str(data.get('exchange_rate', 1))) if data.get('exchange_rate') else None

        # Fetch deduction record with employee details
        deduction = db_session.query(Deduction).options(
            joinedload(Deduction.payroll_transactions)
            .joinedload(PayrollTransaction.employees)
        ).filter_by(id=deductionId).first()

        if not deduction:
            return jsonify({"success": False, "message": "Deduction record not found"}), 404

        # Ensure payment does not exceed balance due
        if amount > deduction.balance_due:
            return jsonify({"success": False, "message": "Payment amount exceeds balance due"}), 400

        currency_id = deduction.currency_id
        exchange_rate_id = None
        rate_obj = None

        # Handle exchange rate - FOLLOW PAYROLL PATTERN
        if base_currency_id and currency_id != int(base_currency_id):
            # Foreign currency - create new rate
            if not exchange_rate:
                return jsonify({
                    'success': False,
                    'message': 'Exchange rate is required for foreign currency payments'
                }), 400

            rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                session=db_session,
                action='create',
                from_currency_id=currency_id,
                to_currency_id=base_currency_id,
                rate_value=float(exchange_rate),
                rate_date=payment_date,
                app_id=current_user.app_id,
                created_by=current_user.id,
                source_type='deduction_payment',
                source_id=None,
                currency_exchange_transaction_id=None
            )
            exchange_rate_id = rate_id
        else:
            # Base currency - ALSO create a rate with value 1.0 (follow payroll pattern)
            # This ensures deduction_payment.exchange_rate is never None
            rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                session=db_session,
                action='create',
                from_currency_id=currency_id,
                to_currency_id=base_currency_id,
                rate_value=1.0,  # Base currency rate is 1
                rate_date=payment_date,
                app_id=current_user.app_id,
                created_by=current_user.id,
                source_type='deduction_payment',
                source_id=None,
                currency_exchange_transaction_id=None
            )
            exchange_rate_id = rate_id

        # Create deduction payment entry
        deduction_payment = DeductionPayment(
            deduction_id=deductionId,
            payment_date=payment_date,
            amount=amount,
            currency_id=deduction.currency_id,
            payment_account=payment_account,
            payment_method=payment_method,
            reference=reference,
            notes=notes,
            is_posted_to_ledger=True,
            exchange_rate_id=exchange_rate_id,
            created_by=current_user.id
        )
        db_session.add(deduction_payment)
        db_session.flush()

        # Link exchange rate to this payment if created
        if rate_obj:
            rate_obj.source_id = deduction_payment.id
            db_session.add(rate_obj)

        # Create journal entries for the payment
        journal = create_deduction_payment_journal_entries(
            db_session=db_session,
            payment_account_id=int(payment_account),
            deduction=deduction,
            deduction_payment=deduction_payment,
            current_user_id=current_user.id,
            app_id=current_user.app_id,
            exchange_rate_id=exchange_rate_id,
            base_currency_id=base_currency_id,
            fx_gain_loss=fx_gain_loss,
            status="Posted"
        )

        if not journal:
            db_session.rollback()
            return jsonify(
                {"success": False, "message": "Failed to create accounting entries for deduction payment"}), 500

        # Update deduction record
        deduction.paid_amount += amount
        deduction.balance_due -= amount
        deduction.payment_status = PaymentStatusEnum.PAID if deduction.balance_due == 0 else PaymentStatusEnum.PARTLY_PAID

        db_session.commit()
        return jsonify({"success": True, "message": "Deduction payment recorded successfully"}), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error in make_deduction_payment: {e}\n{traceback.format_exc()}')
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_session.close()


@payroll_bp.route('/pay_selected_deductions_in_full', methods=['POST'])
def pay_selected_deductions_in_full():
    db_session = Session()
    try:
        data = request.json

        # Get the list of selected deductions from frontend
        selected_payments = data.get('selected_payments', [])

        if not selected_payments:
            selected_ids = data.get('selected_ids', [])
        else:
            selected_ids = [p['id'] for p in selected_payments]

        if not selected_ids:
            return jsonify({"success": False, "message": "No deduction records selected"}), 400

        # Get exchange rates and FX gain/loss data
        exchange_rates = data.get('exchange_rates', {})
        fx_gain_losses = data.get('fx_gain_loss', {})

        # Get payment date
        payment_date_str = data.get('fullPaymentDate') or data.get('paymentDate')
        if payment_date_str:
            try:
                payment_date = datetime.strptime(payment_date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({"success": False, "message": "Invalid date format. Expected format: YYYY-MM-DD"}), 400
        else:
            payment_date = date.today()

        payment_method = int(data.get('fullPaymentMethod')) if data.get('fullPaymentMethod') else None
        payment_account = int(data.get('fullFundingAccount')) if data.get('fullFundingAccount') else None
        reference = data.get('reference') or None
        notes = data.get('notes') or None
        base_currency_id = int(data.get('base_currency_id')) if data.get('base_currency_id') else None

        if not payment_account:
            return jsonify({"success": False, "message": "No payment account selected"}), 400

        if not base_currency_id:
            base_currency = db_session.query(Currency).filter_by(
                app_id=current_user.app_id, currency_index=1
            ).first()
            base_currency_id = base_currency.id if base_currency else None

        # Fetch deduction records with lock for atomic processing
        deduction_records = db_session.query(Deduction).options(
            joinedload(Deduction.payroll_transactions)
            .joinedload(PayrollTransaction.employees)
        ).filter(
            Deduction.id.in_(selected_ids),
            Deduction.balance_due > 0
        ).with_for_update().all()

        if not deduction_records:
            return jsonify({"success": False, "message": "No outstanding deduction records found"}), 404

        affected_periods = set()
        successful_payments = 0
        failed_payments = []

        # Process all payments within the same transaction
        for deduction in deduction_records:
            try:
                # Get payment amount (full balance unless partial specified)
                payment_amount = deduction.balance_due
                if selected_payments:
                    matching = next((p for p in selected_payments if p['id'] == deduction.id), None)
                    if matching:
                        payment_amount = Decimal(str(matching['amount']))

                currency_id = deduction.currency_id
                exchange_rate_id = None
                exchange_rate_obj = None

                # Handle exchange rate for foreign currency
                if base_currency_id and currency_id != base_currency_id:
                    rate_value = exchange_rates.get(str(currency_id))

                    if not rate_value:
                        raise Exception(
                            f'Exchange rate required for currency {deduction.payroll_transactions.employees.currency.user_currency}')

                    # ✅ Use get_or_create_exchange_rate_for_transaction for creating new rate
                    exchange_rate_id, exchange_rate_obj = get_or_create_exchange_rate_for_transaction(
                        session=db_session,
                        action='create',
                        from_currency_id=currency_id,
                        to_currency_id=base_currency_id,
                        rate_value=float(rate_value),
                        rate_date=payment_date,
                        app_id=current_user.app_id,
                        created_by=current_user.id,
                        source_type='deduction_payment',
                        source_id=None,
                        currency_exchange_transaction_id=None
                    )
                else:
                    # ✅ Use resolve_exchange_rate_for_transaction for base currency
                    exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
                        session=db_session,
                        currency_id=currency_id,
                        transaction_date=payment_date,
                        app_id=current_user.app_id
                    )

                # Get FX gain/loss for this deduction
                fx_gain_loss = Decimal(str(fx_gain_losses.get(str(currency_id), 0)))

                # Create deduction payment entry
                deduction_payment = DeductionPayment(
                    deduction_id=deduction.id,
                    payment_date=payment_date,
                    amount=payment_amount,
                    currency_id=deduction.currency_id,
                    payment_account=payment_account,
                    payment_method=payment_method,
                    reference=reference,
                    notes=notes,
                    is_posted_to_ledger=True,
                    exchange_rate_id=exchange_rate_id,
                    created_by=current_user.id
                )
                db_session.add(deduction_payment)
                db_session.flush()

                # Link exchange rate to this payment if created
                if exchange_rate_obj:
                    exchange_rate_obj.source_id = deduction_payment.id
                    db_session.add(exchange_rate_obj)

                # Create journal entries for the payment
                journal = create_deduction_payment_journal_entries(
                    db_session=db_session,
                    deduction_payment=deduction_payment,
                    payment_account_id=payment_account,
                    deduction=deduction,
                    current_user_id=current_user.id,
                    app_id=current_user.app_id,
                    exchange_rate_id=exchange_rate_id,
                    base_currency_id=base_currency_id,
                    fx_gain_loss=fx_gain_loss,
                    status="Posted"
                )

                if not journal:
                    raise Exception('Failed to create journal entries')

                # Update deduction record
                deduction.paid_amount = Decimal(deduction.paid_amount or 0) + payment_amount
                deduction.balance_due = Decimal(deduction.amount or 0) - Decimal(deduction.paid_amount)

                if deduction.balance_due <= 0:
                    deduction.payment_status = PaymentStatusEnum.PAID
                else:
                    deduction.payment_status = PaymentStatusEnum.PARTLY_PAID

                # Track affected payroll periods
                if deduction.payroll_transactions and deduction.payroll_transactions.payroll_period_id:
                    affected_periods.add(deduction.payroll_transactions.payroll_period_id)

                successful_payments += 1
                logger.info(
                    f"✅ Successfully processed deduction payment for {deduction.payroll_transactions.employees.first_name} - {deduction.deduction_type}")

            except Exception as e:
                logger.error(f"Error processing payment for deduction {deduction.id}: {e}\n{traceback.format_exc()}")
                failed_payments.append({
                    'id': deduction.id,
                    'deduction': f"{deduction.deduction_type} - {deduction.payroll_transactions.employees.first_name} {deduction.payroll_transactions.employees.last_name}",
                    'reason': str(e)
                })
                db_session.rollback()
                db_session.expire_all()
                continue

        # Update status for affected payroll periods
        for period_id in affected_periods:
            period_deductions = db_session.query(Deduction).join(
                PayrollTransaction, Deduction.payroll_transactions
            ).filter(
                PayrollTransaction.payroll_period_id == period_id
            ).all()

            all_paid = all(d.balance_due == 0 for d in period_deductions)

            if all_paid:
                payroll_period = db_session.query(PayrollPeriod).filter_by(id=period_id).first()
                if payroll_period:
                    payroll_period.payment_status = PayRollStatusEnum.PAID
                    db_session.add(payroll_period)

        db_session.commit()

        message = f"Successfully paid {successful_payments} deduction record(s)"
        if failed_payments:
            message += f", {len(failed_payments)} failed"

        return jsonify({
            "success": True,
            "message": message,
            "results": {
                "successful": successful_payments,
                "failed": len(failed_payments),
                "failed_details": failed_payments
            }
        }), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error in pay_selected_deductions_in_full: {e}\n{traceback.format_exc()}')
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_session.close()


@payroll_bp.route('/pay_all_deduction_in_full/<int:payrollPeriodId>', methods=['POST'])
def pay_all_deduction_in_full(payrollPeriodId):
    db_session = Session()
    try:
        data = request.json

        # Convert 'paymentDate' from string to a datetime object
        payment_date_str = data.get('paymentDate') or None
        if payment_date_str:
            try:
                payment_date = datetime.strptime(payment_date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({"success": False, "message": "Invalid date format. Expected format: YYYY-MM-DD"}), 400
        else:
            payment_date = date.today()

        payment_method = int(data.get('fullPaymentMethod')) if data.get('fullPaymentMethod') else None
        payment_account = int(data.get('fullFundingAccount')) if data.get('fullFundingAccount') else None
        reference = data.get('reference') or None
        notes = data.get('notes') or None

        # Fetch all deductions for the given payroll period that have a remaining balance
        deductions = db_session.query(Deduction).filter(
            Deduction.payroll_period_id == payrollPeriodId,
            Deduction.balance_due > 0
        ).all()

        if not deductions:
            return jsonify(
                {"success": False, "message": "No outstanding deductions found for this payroll period"}), 404

        # Process payments for each deduction
        for deduction in deductions:
            payment_amount = deduction.balance_due  # Pay off the entire balance

            currency_id = deduction.currency_id

            exchange_rate_id, notification = resolve_exchange_rate_for_transaction(session=db_session,
                                                                                   currency_id=currency_id,
                                                                                   transaction_date=payment_date,
                                                                                   app_id=current_user.app_id)

            # Create deduction payment entry
            deduction_payment = DeductionPayment(
                deduction_id=deduction.id,
                payment_date=payment_date,
                amount=payment_amount,
                currency_id=deduction.currency_id,
                payment_account=payment_account,
                payment_method=payment_method,
                reference=reference,
                notes=notes,
                is_posted_to_ledger=False,
                exchange_rate_id=exchange_rate_id
            )
            db_session.add(deduction_payment)

            # Update deduction record
            deduction.paid_amount += payment_amount
            deduction.balance_due = 0
            deduction.is_posted_to_ledger = False
            deduction.payment_status = PaymentStatusEnum.PAID

        db_session.commit()
        return jsonify({"success": True, "message": "All deductions for this payroll period have been paid"}), 200

    except Exception as e:
        db_session.rollback()
        logger.error("An error occurred {e")
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_session.close()


@payroll_bp.route('/payroll_payments/<int:payment_id>/update', methods=['PUT'])
def update_payroll_payment(payment_id):
    db_session = Session()
    try:
        if not request.is_json:
            return jsonify({"success": False, "message": "Missing JSON in request"}), 415

        data = request.get_json()
        base_currency_id = data.get('base_currency_id')
        client_version = data.get('version')

        # Fetch payment with transaction and employee
        payment = db_session.query(PayrollPayment).options(
            joinedload(PayrollPayment.payroll_transactions)
            .joinedload(PayrollTransaction.employees)
        ).filter_by(id=payment_id).first()

        if not payment or not payment.payroll_transactions:
            return jsonify({"success": False, "message": "Payment not found"}), 404

        if client_version is not None and int(client_version) != payment.version:
            return jsonify({
                'success': False,
                'message': 'This record has been modified by another user. Please refresh and try again.',
                'code': 'VERSION_CONFLICT'
            }), 409

        payroll_transaction = payment.payroll_transactions
        new_amount = Decimal(data.get('amount'))
        old_amount = payment.amount
        amount_diff = new_amount - old_amount
        exchange_rate_id = payment.exchange_rate_id

        # Update ALL payment fields
        if 'paymentDate' in data:
            payment_date = datetime.strptime(data['paymentDate'], '%Y-%m-%d').date()
            payment.payment_date = payment_date

            # Handle exchange rate with manual rate from frontend
            exchange_rate_id = None
            if base_currency_id and payment.currency_id != base_currency_id:
                manual_rate = data.get('exchange_rate')
                if manual_rate:
                    # Manual rate provided from frontend
                    exchange_rate_value = float(manual_rate)
                    rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                        session=db_session,
                        action='create',
                        from_currency_id=payment.currency_id,
                        to_currency_id=base_currency_id,
                        rate_value=exchange_rate_value,
                        rate_date=payment_date,
                        app_id=current_user.app_id,
                        created_by=current_user.id,
                        source_type='payroll_payment',
                        source_id=payment_id,
                        currency_exchange_transaction_id=None
                    )
                    exchange_rate_id = rate_id
                else:
                    # Auto-resolve if no manual rate
                    exchange_rate_id, notification = resolve_exchange_rate_for_transaction(
                        session=db_session,
                        currency_id=payment.currency_id,
                        transaction_date=payment_date,
                        app_id=current_user.app_id
                    )
            else:
                exchange_rate_id, notification = resolve_exchange_rate_for_transaction(
                    session=db_session,
                    currency_id=payment.currency_id,
                    transaction_date=payment_date,
                    app_id=current_user.app_id
                )
        payment.amount = new_amount
        payment.payment_method = data.get('paymentMethod')
        payment.payment_account = data.get('fundingAccount')
        payment.reference = data.get('reference')
        payment.notes = data.get('notes')
        payment.exchange_rate_id = exchange_rate_id
        # Update transaction balances if amount changed
        if amount_diff != 0:
            payroll_transaction.paid_amount += amount_diff
            payroll_transaction.balance_due -= amount_diff

            if payroll_transaction.balance_due < 0:
                db_session.rollback()
                return jsonify({
                    "success": False,
                    "message": "Payment amount would create negative balance"
                }), 400

            payroll_transaction.payment_status = (
                PaymentStatusEnum.PAID if payroll_transaction.balance_due == 0
                else PaymentStatusEnum.PARTLY_PAID
            )

        # ✅ ALWAYS update ledger entries since journals are always stored
        try:
            # Parse payable account ID from employee or transaction
            payable_account_id = None
            if payroll_transaction.employees and payroll_transaction.employees.payable_account_id:
                payable_account_id = payroll_transaction.employees.payable_account_id
            elif hasattr(payroll_transaction, 'payable_account_id') and payroll_transaction.payable_account_id:
                payable_account_id = payroll_transaction.payable_account_id

            if not payable_account_id:
                return jsonify({
                    "success": False,
                    "message": "No payable account configured for this employee/transaction"
                }), 400

            ledger_data = {
                'creditSubCategory': payable_account_id  # ✅ Properly parsed payable account
            }

            # Update ledger with the NEW values
            # Delete existing journals
            # FIRST: Delete existing journal entries for the OLD allocation using the proper function
            delete_success, delete_message = delete_journal_entries_by_source(
                db_session=db_session,
                source_type='payroll_payment',  # or 'purchase_order_payment' depending on your system
                source_id=payment_id,  # Use the OLD allocation ID
                app_id=current_user.app_id
            )

            if not delete_success:
                return jsonify({
                    'success': False,
                    'message': f'Failed to delete existing journal entries: {delete_message}',
                    'error_type': 'journal_deletion_error'
                }), 400

            # Create fresh journals with updated data
            journals = create_payroll_payment_journal_entries(
                db_session=db_session,
                payroll_transaction=payroll_transaction,
                payroll_payment=payment,
                payment_account_id=payment.payment_account,
                current_user_id=current_user.id,
                app_id=current_user.app_id,
                exchange_rate_id=exchange_rate_id,
                base_currency_id=base_currency_id,
                status="Posted"
            )

        except Exception as e:
            db_session.rollback()
            logger.error(f'Error while updating payment ledger: {e}')
            return jsonify({"success": False, "message": f"Ledger update failed: {str(e)}"}), 500

        # Update payroll period status if needed
        if payroll_transaction.payroll_period_id:
            update_payroll_period_status(db_session, payroll_transaction.payroll_period_id)
        payment.version += 1
        db_session.commit()
        return jsonify({"success": True, "message": "Payment updated successfully"}), 200

    except ValueError as ve:
        db_session.rollback()
        return jsonify({"success": False, "message": str(ve)}), 400
    except Exception as e:
        db_session.rollback()
        logger.error(f'Error updating payment: {e}')
        return jsonify({"success": False, "message": "Internal server error"}), 500
    finally:
        db_session.close()


@payroll_bp.route('/payroll_payments/<int:payment_id>', methods=['DELETE'])
def delete_payroll_payment(payment_id):
    db_session = Session()
    try:
        # Fetch payment with transaction and employee
        payment = db_session.query(PayrollPayment).options(
            joinedload(PayrollPayment.payroll_transactions)
            .joinedload(PayrollTransaction.employees)
        ).filter_by(id=payment_id).first()

        if not payment or not payment.payroll_transactions:
            return jsonify({"success": False, "message": "Payment not found"}), 404

        payroll_transaction = payment.payroll_transactions

        try:
            # First mark as unposted

            db_session.flush()

            # Call repost_transaction with repost=False to just delete the ledger entries
            repost_transaction(
                db_session=db_session,
                ledger_filters=[{"source_type": "payroll_payment", "source_id": payment_id}],
                post_function=None,  # No reposting needed for delete
                repost=False  # This will only delete, not repost
            )
        except Exception as e:
            db_session.rollback()
            logger.error(f'Error while deleting payment ledger entries: {e}')
            return jsonify({"success": False, "message": "Failed to reverse ledger entries"}), 500

        # Update transaction balances
        payroll_transaction.paid_amount -= payment.amount
        payroll_transaction.balance_due += payment.amount

        # Update transaction status
        payroll_transaction.payment_status = (
            PaymentStatusEnum.PENDING if payroll_transaction.balance_due == payroll_transaction.net_salary
            else PaymentStatusEnum.PARTLY_PAID
        )

        # Delete the payment
        db_session.delete(payment)

        # Update payroll period status if needed
        if payroll_transaction.payroll_period_id:
            update_payroll_period_status(db_session, payroll_transaction.payroll_period_id)

        db_session.commit()
        return jsonify({"success": True, "message": "Payment deleted successfully"}), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error deleting payment: {e}')
        return jsonify({"success": False, "message": "Internal server error"}), 500
    finally:
        db_session.close()


@payroll_bp.route('/deduction_payments/<int:payment_id>/update', methods=['PUT'])
def update_deduction_payment(payment_id):
    db_session = Session()
    try:
        if not request.is_json:
            return jsonify({"success": False, "message": "Missing JSON in request"}), 415

        data = request.get_json()
        base_currency_id = data.get('base_currency_id')  # ADD THIS
        client_version = data.get('version')
        # Fetch deduction payment with related data
        deduction_payment = db_session.query(DeductionPayment).options(
            joinedload(DeductionPayment.exchange_rate),  # ADD THIS - load exchange rate
            joinedload(DeductionPayment.deductions)
            .joinedload(Deduction.payroll_transactions)
            .joinedload(PayrollTransaction.employees)
        ).filter_by(id=payment_id).first()

        if not deduction_payment or not deduction_payment.deductions:
            return jsonify({"success": False, "message": "Deduction payment not found"}), 404

        if client_version is not None and int(client_version) != deduction_payment.version:
            return jsonify({
                'success': False,
                'message': 'This record has been modified by another user. Please refresh and try again.',
                'code': 'VERSION_CONFLICT'
            }), 409

        deduction = deduction_payment.deductions
        new_amount = Decimal(data.get('amount'))
        old_amount = deduction_payment.amount
        amount_diff = new_amount - old_amount
        exchange_rate_id = deduction_payment.exchange_rate_id

        # Update ALL payment fields
        if 'paymentDate' in data:
            payment_date = datetime.strptime(data['paymentDate'], '%Y-%m-%d').date()
            deduction_payment.payment_date = payment_date

            # Handle exchange rate with manual rate from frontend
            exchange_rate_id = None
            rate_obj = None
            if base_currency_id and deduction_payment.currency_id != int(base_currency_id):
                manual_rate = data.get('exchange_rate')
                if manual_rate:
                    # Manual rate provided from frontend
                    exchange_rate_value = float(manual_rate)
                    rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                        session=db_session,
                        action='create',
                        from_currency_id=deduction_payment.currency_id,
                        to_currency_id=base_currency_id,
                        rate_value=exchange_rate_value,
                        rate_date=payment_date,
                        app_id=current_user.app_id,
                        created_by=current_user.id,
                        source_type='deduction_payment',
                        source_id=payment_id,
                        currency_exchange_transaction_id=None
                    )
                    exchange_rate_id = rate_id
                else:
                    # Auto-resolve if no manual rate
                    exchange_rate_id, notification = resolve_exchange_rate_for_transaction(
                        session=db_session,
                        currency_id=deduction_payment.currency_id,
                        transaction_date=payment_date,
                        app_id=current_user.app_id
                    )
            else:
                # Base currency - also create rate with 1.0
                rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
                    session=db_session,
                    action='create',
                    from_currency_id=deduction_payment.currency_id,
                    to_currency_id=base_currency_id,
                    rate_value=1.0,
                    rate_date=payment_date,
                    app_id=current_user.app_id,
                    created_by=current_user.id,
                    source_type='deduction_payment',
                    source_id=payment_id,
                    currency_exchange_transaction_id=None
                )
                exchange_rate_id = rate_id

            # Link exchange rate to payment if created
            if rate_obj:
                rate_obj.source_id = payment_id
                db_session.add(rate_obj)

        deduction_payment.amount = new_amount
        deduction_payment.payment_method = data.get('paymentMethod')
        deduction_payment.payment_account = data.get('fundingAccount')
        deduction_payment.reference = data.get('reference')
        deduction_payment.notes = data.get('notes')
        deduction_payment.exchange_rate_id = exchange_rate_id
        fx_gain_loss = Decimal(str(data.get('fx_gain_loss', 0)))

        # Update deduction balances if amount changed
        if amount_diff != 0:
            deduction.paid_amount += amount_diff
            deduction.balance_due -= amount_diff

            if deduction.balance_due < 0:
                db_session.rollback()
                return jsonify({
                    "success": False,
                    "message": "Payment amount would create negative balance"
                }), 400

            deduction.payment_status = (
                PaymentStatusEnum.PAID if deduction.balance_due == 0
                else PaymentStatusEnum.PARTLY_PAID
            )

        # ✅ ALWAYS update ledger entries since journals are always stored
        # ✅ ALWAYS update ledger entries since journals are always stored
        try:
            # Get deduction payable account from employee
            employee = deduction.payroll_transactions.employees
            if not employee or not employee.deduction_payable_account_id:
                return jsonify({
                    "success": False,
                    "message": "No deduction payable account configured for this employee"
                }), 400

            # Delete existing journal entries
            delete_success, delete_message = delete_journal_entries_by_source(
                db_session=db_session,
                source_type='deduction_payment',
                source_id=payment_id,
                app_id=current_user.app_id
            )

            if not delete_success:
                return jsonify({
                    'success': False,
                    'message': f'Failed to delete existing journal entries: {delete_message}',
                    'error_type': 'journal_deletion_error'
                }), 400

            # Create fresh journals with updated data
            journals = create_deduction_payment_journal_entries(
                db_session=db_session,
                deduction_payment=deduction_payment,
                payment_account_id=deduction_payment.payment_account,
                deduction=deduction,
                current_user_id=current_user.id,
                app_id=current_user.app_id,
                exchange_rate_id=exchange_rate_id,
                base_currency_id=base_currency_id,
                fx_gain_loss=fx_gain_loss,
                status="Posted"
            )

            if not journals:
                db_session.rollback()
                return jsonify({"success": False, "message": "Failed to create journal entries"}), 500
        except Exception as e:
            db_session.rollback()
            logger.error(f'Error while updating deduction payment ledger: {e}\n{traceback.format_exc()}')
            return jsonify({"success": False, "message": f"Ledger update failed: {str(e)}"}), 500
        # ADD THIS: Increment version
        deduction_payment.version += 1
        db_session.commit()
        return jsonify({"success": True, "message": "Deduction payment updated successfully"}), 200

    except ValueError as ve:
        db_session.rollback()
        return jsonify({"success": False, "message": str(ve)}), 400
    except Exception as e:
        db_session.rollback()
        logger.error(f'Error updating deduction payment: {e}\n{traceback.format_exc()}')
        return jsonify({"success": False, "message": "Internal server error"}), 500
    finally:
        db_session.close()


@payroll_bp.route('/deduction_payments/<int:payment_id>', methods=['DELETE'])
def delete_deduction_payment(payment_id):
    db_session = Session()
    try:
        # Fetch deduction payment with transaction and employee
        deduction_payment = db_session.query(DeductionPayment).options(
            joinedload(DeductionPayment.deductions)
            .joinedload(Deduction.employees)
        ).filter_by(id=payment_id).first()

        if not deduction_payment or not deduction_payment.deductions:
            return jsonify({"success": False, "message": "Deduction payment not found"}), 404

        deduction_transaction = deduction_payment.deductions

        try:
            # Mark as unposted
            deduction_payment.is_posted_to_ledger = False
            db_session.flush()

            # Delete related ledger entries
            repost_transaction(
                db_session=db_session,
                ledger_filters=[{"source_type": "deduction_payment", "source_id": payment_id}],
                post_function=None,  # No reposting needed
                repost=False  # Delete only
            )
        except Exception as e:
            db_session.rollback()
            logger.error(f'Error while deleting deduction ledger entries: {e}')
            return jsonify({"success": False, "message": "Failed to reverse ledger entries"}), 500

        # Update transaction balances
        deduction_transaction.paid_amount -= deduction_payment.amount
        deduction_transaction.balance_due += deduction_payment.amount

        # Update status
        deduction_transaction.payment_status = (
            PaymentStatusEnum.PENDING if deduction_transaction.balance_due == deduction_transaction.balance_due
            else PaymentStatusEnum.PARTLY_PAID
        )

        # Delete the deduction payment
        db_session.delete(deduction_payment)

        # Update payroll period if relevant
        if deduction_transaction.payroll_period_id:
            update_payroll_period_status(db_session, deduction_transaction.payroll_period_id)

        db_session.commit()
        return jsonify({"success": True, "message": "Deduction payment deleted successfully"}), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error deleting deduction payment: {e}')
        return jsonify({"success": False, "message": "Internal server error"}), 500
    finally:
        db_session.close()
