import logging
import traceback

from flask import request, jsonify
from sqlalchemy.orm import joinedload
from db import Session
from models import InventoryEntry, InventoryTransactionDetail, InventoryEntryLineItem, JournalEntry, Journal
from services.inventory_helpers import safe_clear_stock_history_cache
from services.post_to_ledger import create_inventory_ledger_entry
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.numbers import safe_int_conversion

from . import inventory_bp
from flask_login import login_required, current_user

logger = logging.getLogger(__name__)


@inventory_bp.route('/api/bulk_post_to_ledger', methods=['POST'])
def bulk_post_to_ledger():
    """Bulk mark inventory entries and related journals as posted to the general ledger (atomic)"""

    db_session = Session()
    app_id = current_user.app_id

    try:
        data = request.get_json()
        raw_entry_ids = data.get('entry_ids', [])
        transaction_detail_ids = safe_int_conversion(raw_entry_ids)

        if not transaction_detail_ids:
            db_session.close()
            return jsonify({
                'success': False,
                'message': 'No transaction details selected for posting'
            }), 400

        # Fetch transaction details with parent inventory entries
        transaction_details = db_session.query(InventoryTransactionDetail).options(
            joinedload(InventoryTransactionDetail.inventory_entry_line_item).joinedload(
                InventoryEntryLineItem.inventory_entry)
        ).filter(
            InventoryTransactionDetail.id.in_(transaction_detail_ids),
            InventoryTransactionDetail.app_id == app_id
        ).all()

        if not transaction_details:
            db_session.close()
            return jsonify({
                'success': False,
                'message': 'No valid transaction details found'
            }), 404

        successful_details = []
        failed_details = []

        # Track journals to update
        journals_to_update = set()

        for detail in transaction_details:
            try:
                if detail.is_posted_to_ledger:
                    successful_details.append(detail.id)
                    continue

                # Mark transaction detail as posted
                detail.is_posted_to_ledger = True
                successful_details.append(detail.id)

                # Collect related journals
                journal_entries = db_session.query(JournalEntry).filter(
                    JournalEntry.source_type == "inventory_transaction",
                    JournalEntry.source_id == detail.id,
                    JournalEntry.app_id == app_id
                ).all()

                for entry in journal_entries:
                    journals_to_update.add(entry.journal_id)

                # Mark parent inventory entry as posted
                inventory_entry = detail.inventory_entry_line_item.inventory_entry
                if inventory_entry:
                    inventory_entry.status = "posted"

            except Exception as e:
                logger.error(f"Error processing detail {detail.id}: {str(e)}\n{traceback.format_exc()}")
                failed_details.append({'id': detail.id, 'error': str(e)})

        # Update all collected journals to Posted
        for journal_id in journals_to_update:
            journal = db_session.query(Journal).get(journal_id)
            if journal:
                journal.status = "Posted"

        safe_clear_stock_history_cache(logger)

        # Atomic commit: if any detail failed, rollback everything
        if failed_details:
            db_session.rollback()
            db_session.close()
            return jsonify({
                'success': False,
                'message': f'Failed to post {len(failed_details)} transaction(s)',
                'failed_entries': failed_details
            }), 400
        else:
            db_session.commit()
            db_session.close()
            return jsonify({
                'success': True,
                'message': f'All {len(successful_details)} transaction details posted successfully',
                'posted_entries': successful_details
            }), 200

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk post to ledger: {str(e)}\n{traceback.format_exc()}")
        db_session.close()
        return jsonify({
            'success': False,
            'message': f'Internal server error: {str(e)}'
        }), 500


def post_inventory_entry_to_ledger(db_session, inventory_entry, base_currency_id, current_user, posted_status):
    """
    Returns: (success, message) tuple instead of (successful_details, failed_details)
    """
    app_id = current_user.app_id

    # Skip transfer entries
    if inventory_entry.stock_movement == 'transfer':
        inventory_entry.is_posted_to_ledger = True  # Should be False for draft
        return True, "Transfer entry skipped"

    # Get all transaction details from the inventory entry
    transaction_details = []
    for line_item in inventory_entry.line_items:
        transaction_details.extend(line_item.inventory_transaction_details)

    if not transaction_details:
        logger.warning(f"No transaction details found for inventory entry {inventory_entry.id}")
        return True, "No transaction details to post"

    # Post each transaction detail
    for detail in transaction_details:
        try:
            # Use your existing create_inventory_ledger_entry function
            success = create_inventory_ledger_entry(
                db_session, base_currency_id, detail, current_user, posted_status
            )

            if not success:
                # Immediately raise exception on first failure
                raise Exception(f"Failed to post transaction detail {detail.id} to ledger")

        except Exception as e:
            logger.error(f"Error posting transaction detail {detail.id}: {str(e)}")
            # Re-raise to trigger rollback
            raise

    # Mark the inventory entry appropriately
    inventory_entry.is_posted_to_ledger = True  # Should be False for Unposted status

    return True, "All transaction details posted successfully"
