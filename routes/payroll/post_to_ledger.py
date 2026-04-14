# app/routes/payroll/post_to_ledger.py
import traceback
import logging
from datetime import date, datetime, timezone

from sqlalchemy.orm import joinedload

from ai import resolve_exchange_rate_for_transaction
from models import PayrollTransaction, AdvanceRepayment, DeductionPayment, Deduction, ChartOfAccounts, \
    PayrollPayment, AdvancePayment, Journal, JournalEntry
from services.post_to_ledger import post_payroll_to_ledger, post_bulk_payroll_to_ledger, post_payroll_payments_only, \
    post_advance_payment_to_ledger_internal, post_deduction_to_ledger, post_deduction_payments_only, \
    post_bulk_deduction_to_ledger, find_journals_by_source
from utils import generate_unique_journal_number, create_transaction
from . import payroll_bp
from flask import jsonify, request
from flask_login import login_required, current_user
from db import Session

logger = logging.getLogger()


@payroll_bp.route('/post_to_ledger/<int:transaction_id>', methods=['POST'])
@login_required
def post_to_ledger(transaction_id):
    db_session = Session()

    try:
        data = request.get_json()

        # Call the reusable function
        post_payroll_to_ledger(
            db_session=db_session,
            transaction_id=transaction_id,
            current_user=current_user,
            data=data
        )

        db_session.commit()
        return jsonify({"success": True, "message": "Payroll posted to ledger successfully"}), 201

    except ValueError as e:
        db_session.rollback()
        logger.error(f"[DEBUG] Error posting to ledger: {e}\n{traceback.format_exc()}")
        return jsonify({"success": False, "message": str(e)}), 404

    except Exception as e:
        db_session.rollback()
        logger.error(f"[DEBUG] Error posting to ledger: {e}\n{traceback.format_exc()}")
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_session.close()


@payroll_bp.route('/post_deduction_to_ledger/<int:transaction_id>', methods=['POST'])
@login_required
def post_deduction_to_ledger_route(transaction_id):
    db_session = Session()

    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ["debitSubCategory", "creditSubCategory", "ledgerBalanceDue"]
        missing_fields = [f for f in required_fields if f not in data]
        if missing_fields:
            return jsonify({"success": False, "message": f"Missing required fields: {', '.join(missing_fields)}"}), 400

        # Call the reusable function
        post_deduction_to_ledger(db_session, transaction_id, current_user, data)

        db_session.commit()
        return jsonify({"success": True, "message": "Deduction posted to ledger successfully."}), 201

    except ValueError as e:
        db_session.rollback()
        return jsonify({"success": False, "message": str(e)}), 404
    except Exception as e:
        db_session.rollback()
        logger.error(f"Error posting deduction to ledger: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_session.close()


@payroll_bp.route('/post_advance_payment_to_ledger', methods=['POST'])
@login_required
def post_advance_payment_to_ledger():
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()
        advance_payment_id = int(data.get('advancePaymentId'))
        logger.info(f"Received request to post advance payment {advance_payment_id} to ledger")

        if not advance_payment_id:
            return jsonify({
                'success': False,
                'message': 'Missing advance payment ID'
            }), 400

        # 1️⃣ Fetch the advance payment record
        advance_payment = db_session.query(AdvancePayment).filter_by(
            id=advance_payment_id,
            app_id=app_id
        ).first()

        if not advance_payment:
            return jsonify({
                'success': False,
                'message': 'Advance payment not found'
            }), 404

        # 2️⃣ Skip if already posted
        if advance_payment.is_posted_to_ledger:
            return jsonify({
                'success': False,
                'message': 'Advance payment already posted to ledger'
            }), 400

        # 3️⃣ Find the related journal entry
        advance_entry = (
            db_session.query(JournalEntry)
            .options(joinedload(JournalEntry.journal))
            .filter_by(source_type='advance_payment', source_id=advance_payment.id, app_id=app_id)
            .first()
        )

        if not advance_entry:
            return jsonify({
                'success': False,
                'message': 'No journal entry found for this advance payment'
            }), 404

        # ✅ 4️⃣ Directly get the parent journal from the relationship
        journal = advance_entry.journal

        if not journal:
            return jsonify({
                'success': False,
                'message': 'Related journal not found'
            }), 404

        # 5️⃣ Update journal status and audit info
        journal.status = "Posted"
        journal.updated_by = current_user.id
        journal.updated_at = datetime.now(timezone.utc)

        # 6️⃣ Mark advance payment as posted
        advance_payment.is_posted_to_ledger = True

        # 7️⃣ Commit changes
        db_session.commit()

        logger.info(f"✅ Successfully posted advance payment {advance_payment_id} (Journal {journal.id}) to ledger")

        return jsonify({
            'success': True,
            'message': f'Advance payment {advance_payment_id} posted to ledger successfully!'
        }), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f"[DEBUG] Error posting advance payment to ledger: {e}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500

    finally:
        db_session.close()


@payroll_bp.route('/post_selected_to_ledger', methods=['POST'])
def post_selected_to_ledger():
    db_session = Session()
    app_id = current_user.app_id
    try:
        data = request.get_json()

        # Validate required fields - ONLY selected_ids needed now
        if not data or 'selected_ids' not in data:
            return jsonify({
                'success': False,
                'message': 'Missing required field: selected_ids'
            }), 400

        selected_ids = data['selected_ids']

        if not isinstance(selected_ids, list) or len(selected_ids) == 0:
            return jsonify({
                'success': False,
                'message': 'No valid transactions selected for posting'
            }), 400

        results = {
            'posted_payroll_journals': 0,  # Payroll transaction journals marked as posted
            'posted_payment_journals': 0,  # Payment journals marked as posted
            'posted_advance_repayments': 0,  # Advance repayments marked as posted
            'skipped': 0,  # Transactions with no work needed
            'failed': 0,
            'failed_details': []
        }

        for transaction_id in selected_ids:
            try:
                # Verify the transaction exists
                transaction = db_session.query(PayrollTransaction) \
                    .options(
                    joinedload(PayrollTransaction.payroll_payments),
                    joinedload(PayrollTransaction.advance_repayment)
                ) \
                    .filter_by(id=transaction_id) \
                    .first()

                if not transaction:
                    results['failed'] += 1
                    results['failed_details'].append({
                        'id': transaction_id,
                        'reason': 'Transaction not found'
                    })
                    continue

                # Skip cancelled transactions
                if transaction.payment_status == 'CANCELLED':
                    results['skipped'] += 1
                    continue

                # Check for unposted payments
                unposted_payments = [p for p in transaction.payroll_payments
                                     if not p.is_posted_to_ledger]

                # ✅ Handle Payroll Transaction Journals
                payroll_journals = find_journals_by_source(
                    db_session=db_session,
                    source_type='payroll_transaction',
                    source_id=transaction_id,
                    app_id=app_id
                )

                payroll_journals_posted = False
                if payroll_journals:
                    for journal in payroll_journals:
                        if journal.status == "Unposted":
                            # ✅ Mark existing payroll journal as Posted
                            journal.status = "Posted"
                            journal.updated_by = current_user.id
                            journal.updated_at = datetime.now(timezone.utc)

                            # ✅ Update transaction posting status
                            transaction.is_posted_to_ledger = True

                            # ✅ Update deductions posting status
                            for deduction in transaction.deductions:
                                if not deduction.is_posted_to_ledger:
                                    deduction.is_posted_to_ledger = True

                            # ✅ CRITICAL: Update advance repayment posting status
                            if transaction.advance_repayment:
                                for advance_repayment in transaction.advance_repayment:
                                    if not advance_repayment.is_posted_to_ledger:
                                        advance_repayment.is_posted_to_ledger = True
                                        results['posted_advance_repayments'] += 1
                                        logger.info(f"✅ Marked advance repayment {advance_repayment.id} as posted")

                            results['posted_payroll_journals'] += 1
                            payroll_journals_posted = True

                    if not any(journal.status == "Unposted" for journal in payroll_journals):
                        # All payroll journals already posted, just sync transaction status
                        if not transaction.is_posted_to_ledger:
                            transaction.is_posted_to_ledger = True

                        # ✅ Also sync advance repayment status if not already posted
                        if transaction.advance_repayment:
                            for advance_repayment in transaction.advance_repayment:
                                if not advance_repayment.is_posted_to_ledger:
                                    advance_repayment.is_posted_to_ledger = True
                                    results['posted_advance_repayments'] += 1
                                    logger.info(f"✅ Synced advance repayment {advance_repayment.id} as posted")
                else:
                    # No payroll journals found for this transaction
                    results['failed'] += 1
                    results['failed_details'].append({
                        'id': transaction_id,
                        'reason': 'No payroll journal entries found for this transaction'
                    })
                    continue

                # ✅ Handle Payment Journals
                payment_journals_posted = False
                if unposted_payments:
                    for payment in unposted_payments:
                        # Find journals for this specific payment
                        payment_journals = find_journals_by_source(
                            db_session=db_session,
                            source_type='payroll_payment',
                            source_id=payment.id,
                            app_id=app_id
                        )

                        if payment_journals:
                            for journal in payment_journals:
                                if journal.status == "Unposted":
                                    # ✅ Mark payment journal as Posted
                                    journal.status = "Posted"
                                    journal.updated_by = current_user.id
                                    journal.updated_at = datetime.now(timezone.utc)

                                    # ✅ Update payment posting status
                                    payment.is_posted_to_ledger = True

                                    results['posted_payment_journals'] += 1
                                    payment_journals_posted = True
                                    logger.info(
                                        f"✅ Marked payment journal {journal.id} as Posted for payment {payment.id}")

                            if not any(journal.status == "Unposted" for journal in payment_journals):
                                # All payment journals already posted, just sync payment status
                                if not payment.is_posted_to_ledger:
                                    payment.is_posted_to_ledger = True
                        else:
                            # No journals found for this payment
                            logger.warning(f"No journal entries found for payment {payment.id}")

                # Determine if this transaction should be considered successful
                if payroll_journals_posted or payment_journals_posted or results['posted_advance_repayments'] > 0:
                    # Success - at least one journal or advance repayment was posted
                    pass
                else:
                    # No journals needed posting, count as skipped
                    results['skipped'] += 1
                    logger.info(f"All journals already posted for transaction {transaction_id}")

            except Exception as e:
                logger.error(f'An error occurred with {e} \n{traceback.format_exc()}')
                results['failed'] += 1
                results['failed_details'].append({
                    'id': transaction_id,
                    'reason': str(e)
                })
                continue

        db_session.commit()

        # Build response message
        message_parts = []
        if results['posted_payroll_journals']:
            message_parts.append(f"{results['posted_payroll_journals']} payroll journals posted")
        if results['posted_payment_journals']:
            message_parts.append(f"{results['posted_payment_journals']} payment journals posted")
        if results['posted_advance_repayments']:
            message_parts.append(f"{results['posted_advance_repayments']} advance repayments posted")
        if results['skipped']:
            message_parts.append(f"{results['skipped']} skipped (already posted)")
        if results['failed']:
            message_parts.append(f"{results['failed']} failed")

        return jsonify({
            'success': True,
            'message': 'Operation complete: ' + ', '.join(message_parts),
            'results': results,
            'has_failures': results['failed'] > 0,
            'failed_transactions': results['failed_details']
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in post_selected_to_ledger: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'An error occurred: {str(e)}'
        }), 500
    finally:
        db_session.close()


@payroll_bp.route('/post_selected_deductions_to_ledger', methods=['POST'])
@login_required
def post_selected_deductions_to_ledger():
    """
    Post selected DEDUCTION PAYMENTS to ledger (payments to third parties)
    This handles only the actual payment transactions, not the original deduction entries
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()

        # Validate required fields - ONLY selected_ids needed now
        if not data or 'selected_ids' not in data:
            return jsonify({
                'success': False,
                'message': 'Missing required field: selected_ids'
            }), 400

        selected_ids = data['selected_ids']

        if not isinstance(selected_ids, list) or len(selected_ids) == 0:
            return jsonify({
                'success': False,
                'message': 'No valid deduction payments selected for posting'
            }), 400

        results = {
            'posted_deduction_payment_journals': 0,  # Deduction payment journals marked as posted
            'skipped': 0,  # Payments with no work needed
            'failed': 0,
            'failed_details': []
        }

        for deduction_payment_id in selected_ids:  # ✅ Changed: Expecting deduction PAYMENT IDs
            try:
                # Verify the deduction PAYMENT exists
                deduction_payments = db_session.query(DeductionPayment) \
                    .options(
                    joinedload(DeductionPayment.deductions)
                    .joinedload(Deduction.payroll_transactions)
                    .joinedload(PayrollTransaction.employees)
                ) \
                    .filter_by(deduction_id=deduction_payment_id) \
                    .all()

                for deduction_payment in deduction_payments:

                    # Skip if deduction payment is already posted
                    if deduction_payment.is_posted_to_ledger:
                        results['skipped'] += 1
                        logger.info(f"Deduction payment {deduction_payment_id} already posted")
                        continue

                    # ✅ Handle Deduction Payment Journals ONLY
                    deduction_payment_journals = find_journals_by_source(
                        db_session=db_session,
                        source_type='deduction_payment',  # ✅ Correct source type
                        source_id=deduction_payment.id,  # ✅ Correct source ID
                        app_id=app_id
                    )

                    if deduction_payment_journals:
                        payment_journals_posted = False
                        for journal in deduction_payment_journals:
                            if journal.status == "Unposted":
                                # ✅ Mark deduction payment journal as Posted
                                journal.status = "Posted"
                                journal.updated_by = current_user.id
                                journal.updated_at = datetime.now(timezone.utc)

                                # ✅ Update deduction payment posting status
                                deduction_payment.is_posted_to_ledger = True

                                results['posted_deduction_payment_journals'] += 1
                                payment_journals_posted = True
                                logger.info(
                                    f"✅ Marked deduction payment journal {journal.id} as Posted for payment {deduction_payment_id}")

                        if payment_journals_posted:
                            # Successfully posted at least one journal
                            pass
                        else:
                            # All journals already posted, just sync payment status
                            if not deduction_payment.is_posted_to_ledger:
                                deduction_payment.is_posted_to_ledger = True
                            results['skipped'] += 1
                            logger.info(f"All journals already posted for deduction payment {deduction_payment_id}")
                    else:
                        # No journals found for this deduction payment
                        results['failed'] += 1
                        results['failed_details'].append({
                            'id': deduction_payment_id,
                            'reason': 'No journal entries found for this deduction payment'
                        })
                        continue

            except Exception as e:
                logger.error(f'An error occurred with {e} \n{traceback.format_exc()}')
                results['failed'] += 1
                results['failed_details'].append({
                    'id': deduction_payment_id,
                    'reason': str(e)
                })
                continue

        db_session.commit()

        # Build response message
        message_parts = []
        if results['posted_deduction_payment_journals']:
            message_parts.append(f"{results['posted_deduction_payment_journals']} deduction payment journals posted")
        if results['skipped']:
            message_parts.append(f"{results['skipped']} skipped (already posted)")
        if results['failed']:
            message_parts.append(f"{results['failed']} failed")

        return jsonify({
            'success': True,
            'message': 'Operation complete: ' + ', '.join(message_parts),
            'results': results,
            'has_failures': results['failed'] > 0,
            'failed_transactions': results['failed_details']
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in post_selected_deductions_to_ledger: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'An error occurred: {str(e)}'
        }), 500
    finally:
        db_session.close()
