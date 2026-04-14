import traceback
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation

from flask import request, jsonify, render_template, flash, redirect, url_for, abort
from flask_login import login_required, current_user
from sqlalchemy import or_, func, literal, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from ai import get_base_currency
from db import Session
from models import Company, Module, InventoryLocation, ChartOfAccounts, Vendor, DirectSalesTransaction, \
    InventoryItemVariationLink, DirectSaleItem, Project, PaymentMode, UnitOfMeasurement, Currency, PaymentAllocation, \
    SalesTransaction, SalesPaymentStatus, SalesInvoice, InvoiceStatus, OrderStatus, CustomerCredit, \
    DirectPurchaseTransaction, InventoryItem, DirectPurchaseItem, PurchasePaymentAllocation, GoodsReceipt, \
    GoodsReceiptItem, PurchaseReturn, PurchaseOrder, InventoryEntry, PurchaseOrderItem
from services.inventory_helpers import safe_clear_stock_history_cache
from services.post_to_ledger import post_direct_sale_cogs_to_ledger, post_sales_transaction_to_ledger, \
    bulk_post_sales_transactions, post_customer_credit_to_ledger, \
    post_purchase_transaction_to_ledger, post_goods_receipt_to_ledger, bulk_post_goods_receipts
from services.post_to_ledger_reversal import reverse_direct_sales_posting, reverse_sales_invoice_posting, \
    reverse_sales_transaction_posting, delete_journal_entries_by_source
from services.purchases_helpers import generate_direct_purchase_number, allocate_direct_purchase_payment, \
    get_inventory_entries_for_direct_purchase, reverse_purchase_inventory_entries, reverse_direct_purchase_posting, \
    calculate_direct_purchase_landed_costs, calculate_goods_receipt_landed_costs
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    get_inventory_entries_for_direct_sale, reverse_sales_inventory_entries, allocate_payment
from services.vendors_and_customers import get_or_create_customer_credit_account
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction, \
    generate_next_goods_receipt_number, get_total_received_for_purchase, get_item_details
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj
from utils_and_helpers.lists import check_list_not_empty
from . import purchases_bp

import logging

logger = logging.getLogger(__name__)


@purchases_bp.route('/add_goods_receipt', methods=['POST'])
@login_required
def add_goods_receipt():
    db_session = Session()
    try:
        data = request.form
        app_id = current_user.app_id
        purchase_order_id = data.get('purchase_order_id')
        direct_purchase_id = data.get('direct_purchase_id')

        if not purchase_order_id and not direct_purchase_id:
            db_session.close()
            flash('Either Purchase Order ID or Direct Purchase ID is required', 'error')
            return redirect(request.referrer)

        # Validate receipt date
        receipt_date_str = data.get('receiptDate')
        try:
            receipt_date = datetime.strptime(receipt_date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            db_session.close()
            flash('Invalid receipt date format (YYYY-MM-DD required)', 'error')
            return redirect(request.referrer)

        # Validate received by
        received_by = data.get('receivedBy', '').strip()
        if not received_by:
            db_session.close()
            flash('Received By is required', 'error')
            return redirect(request.referrer)

        # Get purchase record
        if purchase_order_id:
            purchase_record = db_session.query(PurchaseOrder).filter_by(
                id=purchase_order_id,
                app_id=app_id
            ).options(
                joinedload(PurchaseOrder.purchase_order_items)
            ).first()
            item_relationship = 'purchase_order_items'
        else:
            purchase_record = db_session.query(DirectPurchaseTransaction).filter_by(
                id=direct_purchase_id,
                app_id=app_id
            ).options(
                joinedload(DirectPurchaseTransaction.direct_purchase_items)
            ).first()
            item_relationship = 'direct_purchase_items'

        if not purchase_record:
            db_session.close()
            flash('Purchase record not found', 'error')
            return redirect(request.referrer)

        # Create goods receipt
        receipt_number = generate_next_goods_receipt_number(db_session, app_id)
        receipt = GoodsReceipt(
            receipt_number=receipt_number,
            purchase_order_id=purchase_order_id,
            direct_purchase_id=direct_purchase_id,
            receipt_date=receipt_date,
            received_by=received_by,
            quantity_received=Decimal('0'),
            is_complete_receipt=False,
            is_posted_to_ledger=True,
            inventory_received=False,
            inventory_posted=True,
            app_id=app_id
        )
        db_session.add(receipt)
        db_session.flush()

        # Process received items
        total_received = Decimal('0')
        has_inventory = False
        has_non_inventory = False
        has_services = False
        items = getattr(purchase_record, item_relationship)
        receipt_items = []

        for item in items:
            item_id = str(item.id)
            quantity_received = data.get(f'quantity_received_{item_id}')
            condition = data.get(f'condition_{item_id}', 'Good')
            notes = data.get(f'notes_{item_id}', '').strip() or None

            try:
                qty = Decimal(quantity_received) if quantity_received else Decimal('0')
                if qty <= Decimal('0'):
                    continue

                # Validate quantity doesn't exceed ordered
                item_quantity = Decimal(str(item.quantity))
                total_received_so_far = db_session.query(
                    func.coalesce(func.sum(GoodsReceiptItem.quantity_received), 0)
                ).filter(
                    GoodsReceiptItem.app_id == app_id,
                    (GoodsReceiptItem.purchase_order_item_id == item.id) if purchase_order_id else
                    (GoodsReceiptItem.direct_purchase_item_id == item.id)
                ).scalar()

                if qty + total_received_so_far > item_quantity:
                    db_session.rollback()
                    item_name, _ = get_item_details(item)
                    flash(f"Received quantity for {item_name} exceeds ordered quantity", 'error')
                    return redirect(request.referrer)

                # Create receipt item with initial values (will be updated by landed cost calculation)
                receipt_item = GoodsReceiptItem(
                    goods_receipt_id=receipt.id,
                    purchase_order_item_id=item.id if purchase_order_id else None,
                    direct_purchase_item_id=item.id if direct_purchase_id else None,
                    quantity_received=qty,
                    allocated_amount=Decimal('0'),
                    allocated_tax_amount=Decimal('0'),
                    unit_cost=Decimal('0'),  # Will be set by landed cost calculation
                    total_cost=Decimal('0'),  # Will be set by landed cost calculation
                    allocated_shipping_handling=Decimal('0'),  # Will be set by landed cost calculation
                    received_condition=condition,
                    notes=notes,
                    inventory_adjusted=False,
                    is_posted_to_ledger=True,
                    app_id=app_id
                )
                db_session.add(receipt_item)
                receipt_items.append((item, receipt_item))
                total_received += qty

                # Track item types for ledger posting
                if item.item_type == 'inventory':
                    has_inventory = True
                elif item.item_type in ('non_inventory', 'non-inventory'):
                    has_non_inventory = True
                elif item.item_type == 'service':
                    has_services = True

            except (ValueError, TypeError, InvalidOperation) as e:
                db_session.rollback()
                flash(f"Invalid quantity for item {item.item_name}: {str(e)}", 'error')
                return redirect(request.referrer)

        if total_received == Decimal('0'):
            db_session.rollback()
            flash('At least one item must be received', 'error')
            return redirect(request.referrer)

        # Calculate allocations and landed costs
        allocations = calculate_goods_receipt_landed_costs(
            db_session=db_session,
            goods_receipt=receipt,
            purchase_record=purchase_record,
            receipt_items=[ri for (_, ri) in receipt_items],
            app_id=app_id
        )

        # Apply allocations and update costs from landed cost calculation
        for item, receipt_item in receipt_items:
            alloc = next((a for a in allocations if a['item_id'] == item.id), None)
            if alloc:
                # The landed cost calculation already sets unit_cost and total_cost on receipt_item
                # We just need to update the allocated amounts
                receipt_item.allocated_amount = alloc['allocated_amount'] - alloc['allocated_discount']
                receipt_item.allocated_tax_amount = alloc['allocated_tax']

                # Apply allocated shipping and handling if present
                if 'allocated_shipping_handling' in alloc:
                    receipt_item.allocated_shipping_handling = alloc['allocated_shipping_handling']

                # Log the cost calculation for debugging
                logger.info(
                    f"Item {item.item_name}: Unit Cost = {receipt_item.unit_cost}, Total Cost = {receipt_item.total_cost}")

        # Update receipt totals
        receipt.quantity_received = total_received
        receipt.inventory_received = has_inventory

        # Check if this is final receipt
        total_ordered = sum(Decimal(str(item.quantity)) for item in items)
        if purchase_order_id:
            overall_received = get_total_received_for_purchase(purchase_order_id, db_session, False)
        else:
            overall_received = get_total_received_for_purchase(direct_purchase_id, db_session, True)

        receipt.is_complete_receipt = (overall_received >= total_ordered)

        # Handle tax allocation for complete receipt
        if receipt.is_complete_receipt:
            total_line_tax = sum(Decimal(str(item.tax_amount or '0')) for item in items)
            purchase_level_tax = Decimal(str(purchase_record.total_tax_amount or '0')) - total_line_tax

            # Get ALL previously allocated taxes (from this and other receipts)
            if purchase_order_id:
                query = db_session.query(
                    func.sum(GoodsReceiptItem.allocated_tax_amount)
                ).join(
                    GoodsReceipt,
                    GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id
                ).filter(
                    GoodsReceiptItem.purchase_order_item_id.in_([item.id for item in items]),
                    GoodsReceipt.app_id == app_id
                )
            else:
                query = db_session.query(
                    func.sum(GoodsReceiptItem.allocated_tax_amount)
                ).join(
                    GoodsReceipt,
                    GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id
                ).filter(
                    GoodsReceiptItem.direct_purchase_item_id.in_([item.id for item in items]),
                    GoodsReceipt.app_id == app_id
                )

            total_allocated_so_far = query.scalar() or Decimal('0')

            # Calculate difference
            tax_diff = purchase_level_tax - total_allocated_so_far

            # Apply adjustment to the last item in current receipt
            # Apply adjustment to the last item in current receipt
            if abs(tax_diff) > Decimal('0.01') and receipt.receipt_items:
                last_item = receipt.receipt_items[-1]
                if last_item.allocated_tax_amount is None:
                    last_item.allocated_tax_amount = 0.0

                # Convert both to Decimal for calculation, then back to float for storage
                current_tax = Decimal(str(last_item.allocated_tax_amount))
                new_tax = current_tax + tax_diff
                last_item.allocated_tax_amount = float(new_tax)
                db_session.flush()

                # Verify the adjustment
                new_total = (query.scalar() or Decimal('0'))
                if abs(new_total - purchase_level_tax) > Decimal('0.01'):
                    raise ValueError("Tax allocation failed to balance")
                db_session.flush()

                # Verify the adjustment
                new_total = (query.scalar() or Decimal('0'))
                if abs(new_total - purchase_level_tax) > Decimal('0.01'):
                    raise ValueError("Tax allocation failed to balance")

        # Create ledger entries for ALL item types (with status 'Unposted')
        # This applies to both Purchase Orders and Direct Purchases
        # If this fails, the entire transaction should roll back
        success, message = post_goods_receipt_to_ledger(
            db_session=db_session,
            goods_receipt=receipt,
            current_user=current_user,
            status='Posted'  # Set as Unposted initially
        )
        if not success:
            raise Exception(f"Ledger creation failed: {message}")

        db_session.commit()
        flash('Goods receipt created and ledger entries prepared successfully', 'success')
        return redirect(request.referrer)

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f'Database error in add_goods_receipt: {str(e)}\n{traceback.format_exc()}')
        flash(f"Database error: {str(e)}", 'error')
        return redirect(request.referrer)

    except Exception as e:
        db_session.rollback()
        logger.error(f'Unexpected error in add_goods_receipt: {str(e)}\n{traceback.format_exc()}')
        flash(f"Unexpected error: {str(e)}", 'error')
        return redirect(request.referrer)

    finally:
        db_session.close()


@purchases_bp.route('/api/update_goods_receipt', methods=['POST'])
@login_required
def update_goods_receipt():
    db_session = Session()
    try:
        receipt_id = request.form.get('receipt_id')
        receipt_item_id = request.form.get('receipt_item_id')
        quantity_received = request.form.get('quantity_received')
        received_condition = request.form.get('received_condition')
        receipt_version = request.form.get('receipt_version', type=int)
        app_id = current_user.app_id

        # Validate required fields
        if not receipt_id or not receipt_item_id:
            return jsonify({'success': False, 'message': 'Missing receipt identifiers'}), 400

        # Fetch receipt with version check and lock
        receipt = db_session.query(GoodsReceipt).filter(
            GoodsReceipt.id == receipt_id,
            GoodsReceipt.app_id == app_id,
            GoodsReceipt.version == receipt_version
        ).with_for_update().first()

        if not receipt:
            message = 'This goods receipt was modified by another user. Please refresh and try again.'
            return jsonify({'success': False, 'message': message}), 409  # 409 Conflict is more appropriate

        # Get existing record with related data
        receipt_item = db_session.query(GoodsReceiptItem).options(
            joinedload(GoodsReceiptItem.goods_receipt),
            joinedload(GoodsReceiptItem.purchase_order_item)
        ).filter_by(
            id=receipt_item_id,
            goods_receipt_id=receipt_id
        ).first()

        if not receipt_item:
            return jsonify({'success': False, 'message': 'Receipt item not found'}), 404

        # Validate quantity is a positive number
        try:
            new_quantity = Decimal(quantity_received)
            if new_quantity <= Decimal('0'):
                return jsonify({'success': False, 'message': 'Quantity must be a positive number'}), 400
        except (ValueError, InvalidOperation):
            return jsonify({'success': False, 'message': 'Invalid quantity format'}), 400

        # Get related purchase order item
        po_item = receipt_item.purchase_order_item
        if not po_item:
            return jsonify({'success': False, 'message': 'Related purchase order item not found'}), 404

        # Get quantity already received elsewhere for this item
        total_received_elsewhere = db_session.query(func.coalesce(func.sum(GoodsReceiptItem.quantity_received), 0)) \
            .filter(
            GoodsReceiptItem.purchase_order_item_id == po_item.id,
            GoodsReceiptItem.id != receipt_item_id  # exclude this item
        ).scalar()

        # Enforce not exceeding the ordered quantity
        if total_received_elsewhere + new_quantity > Decimal(str(po_item.quantity)):
            return jsonify({
                'success': False,
                'message': (
                    f'Quantity update would exceed the ordered quantity. '
                    f'Already received: {total_received_elsewhere}, '
                    f'Attempting to add: {new_quantity}, '
                    f'Ordered: {po_item.quantity}'
                )
            }), 400

        # Get purchase order
        purchase_order = receipt_item.goods_receipt.purchase_order
        if not purchase_order:
            return jsonify({'success': False, 'message': 'Purchase order not found'}), 404

        # STEP 1: Reverse existing inventory entries
        try:

            # Find inventory entries for this goods receipt
            inventory_entries = db_session.query(InventoryEntry).filter(
                InventoryEntry.inventory_source == 'goods_receipt',
                InventoryEntry.source_id == receipt_id,
                InventoryEntry.app_id == current_user.app_id
            ).all()

            for entry in inventory_entries:
                # Reverse each inventory entry
                reverse_success = reverse_purchase_inventory_entries(db_session, entry)
                if not reverse_success:
                    logger.error(f"Warning: Could not fully reverse inventory entry {entry.id}")

        except Exception as inv_error:
            logger.error(f"Warning: Error reversing inventory entries: {str(inv_error)}")
            db_session.rollback()
            # Continue with the update anyway

        # STEP 2: Delete existing ledger entries
        delete_success, delete_message = delete_journal_entries_by_source(
            db_session=db_session,
            source_type='goods_receipt',
            source_id=receipt_id,
            app_id=current_user.app_id
        )

        if not delete_success:
            logger.error(f"Warning: Could not delete existing ledger entries: {delete_message}")

        # STEP 3: Update receipt item with new quantities
        receipt_date_str = request.form.get('receipt_date')
        if receipt_date_str:
            receipt_item.goods_receipt.receipt_date = datetime.strptime(receipt_date_str, '%Y-%m-%d').date()

        received_by = request.form.get('received_by')
        if received_by:
            receipt_item.goods_receipt.received_by = received_by

        receipt_item.quantity_received = new_quantity
        receipt_item.received_condition = received_condition
        receipt_item.notes = request.form.get('notes')

        # Reset posting flags since we're recreating everything
        receipt_item.is_posted_to_ledger = True
        receipt_item.goods_receipt.is_posted_to_ledger = True

        # STEP 4: Recalculate landed costs for ALL receipt items
        receipt = receipt_item.goods_receipt
        all_receipt_items = db_session.query(GoodsReceiptItem).filter_by(
            goods_receipt_id=receipt.id
        ).all()

        allocations = calculate_goods_receipt_landed_costs(
            db_session=db_session,
            goods_receipt=receipt,
            purchase_record=purchase_order,
            receipt_items=all_receipt_items,
            app_id=current_user.app_id
        )

        # Apply new allocations to all receipt items - UPDATED FOR LANDED COST
        for item in all_receipt_items:
            item_allocation = next((alloc for alloc in allocations if alloc['item_id'] == item.purchase_order_item_id),
                                   None)
            if item_allocation:
                # The landed cost calculation already sets unit_cost and total_cost on receipt_item
                # We just need to update the allocated amounts
                item.allocated_amount = item_allocation['allocated_amount'] - item_allocation['allocated_discount']
                item.allocated_tax_amount = item_allocation['allocated_tax']

                # Apply allocated shipping and handling if present
                if 'allocated_shipping_handling' in item_allocation:
                    item.allocated_shipping_handling = item_allocation['allocated_shipping_handling']

                # Log the cost calculation for debugging
                logger.info(
                    f"Updated item {item.purchase_order_item.item_name if item.purchase_order_item else 'N/A'}: "
                    f"Unit Cost = {item.unit_cost}, Total Cost = {item.total_cost}")

        # STEP 5: Update receipt totals
        total_received = sum(Decimal(str(item.quantity_received)) for item in all_receipt_items)
        receipt.quantity_received = total_received

        # Check if receipt is now complete
        total_ordered = sum(Decimal(str(item.quantity)) for item in purchase_order.purchase_order_items)

        # Get overall received quantity for this PO
        overall_total_received = db_session.query(
            func.coalesce(func.sum(GoodsReceiptItem.quantity_received), 0)
        ).join(GoodsReceipt).filter(
            GoodsReceipt.purchase_order_id == purchase_order.id,
            GoodsReceipt.app_id == current_user.app_id
        ).scalar()

        is_final_receipt = (overall_total_received >= total_ordered)
        receipt.is_complete_receipt = is_final_receipt

        # STEP 6: Update inventory status
        receipt.inventory_received = any(
            item.purchase_order_item.item_type == 'inventory'
            and item.quantity_received > 0
            for item in all_receipt_items
        )

        # STEP 7: Repost to ledger (create new journal entries) - UPDATED FOR CONSISTENCY
        success, message = post_goods_receipt_to_ledger(
            db_session=db_session,
            goods_receipt=receipt,
            current_user=current_user,
            status='Posted'  # Or 'Posted' depending on your workflow
        )

        if not success:
            raise Exception(f"Ledger creation failed: {message}")

        safe_clear_stock_history_cache(logger)
        # Increment version after successful update
        receipt.version += 1
        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Receipt updated successfully',
            'data': {
                'total_received': float(total_received),
                'is_complete': receipt.is_complete_receipt,
                'inventory_received': receipt.inventory_received,
                'unit_cost': float(receipt_item.unit_cost) if receipt_item.unit_cost else 0,
                'total_cost': float(receipt_item.total_cost) if receipt_item.total_cost else 0,
                'allocated_shipping_handling': float(
                    receipt_item.allocated_shipping_handling) if receipt_item.allocated_shipping_handling else 0
            }
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error updating receipt: {e}')
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


@purchases_bp.route('/api/delete_goods_receipt_item', methods=['POST'])
@login_required
def delete_goods_receipt_item():
    """
    Delete a goods receipt item and all associated entries
    NOTE: This will delete ALL items in the receipt since they share the same source_id
    """
    db_session = Session()
    try:
        receipt_id = request.form.get('receipt_id')
        receipt_item_id = request.form.get('receipt_item_id')

        if not receipt_id or not receipt_item_id:
            return jsonify({'success': False, 'message': 'Missing receipt identifiers'}), 400

        # Get the receipt item and the entire receipt
        receipt_item = db_session.query(GoodsReceiptItem).options(
            joinedload(GoodsReceiptItem.goods_receipt),
            joinedload(GoodsReceiptItem.purchase_order_item)
        ).filter_by(
            id=receipt_item_id,
            goods_receipt_id=receipt_id,
            app_id=current_user.app_id
        ).first()

        if not receipt_item:
            return jsonify({'success': False, 'message': 'Receipt item not found'}), 404

        # Get the entire receipt with all items
        receipt = receipt_item.goods_receipt
        all_receipt_items = db_session.query(GoodsReceiptItem).filter_by(
            goods_receipt_id=receipt_id
        ).all()

        # Store info for messages
        receipt_number = receipt.receipt_number
        item_name = receipt_item.purchase_order_item.item_name if receipt_item.purchase_order_item else 'Unknown Item'
        total_items_in_receipt = len(all_receipt_items)

        # STEP 1: Reverse inventory entries for the entire receipt
        try:
            inventory_entries = db_session.query(InventoryEntry).filter(
                InventoryEntry.inventory_source == 'goods_receipt',
                InventoryEntry.source_id == receipt_id,
                InventoryEntry.app_id == current_user.app_id
            ).all()

            for entry in inventory_entries:
                reverse_purchase_inventory_entries(db_session, entry)
        except Exception as inv_error:
            logger.error(f"Error reversing inventory entries: {str(inv_error)}")

        # STEP 2: Delete ledger entries for the entire receipt
        delete_journal_entries_by_source(
            db_session=db_session,
            source_type='goods_receipt',
            source_id=receipt_id,
            app_id=current_user.app_id
        )

        # STEP 3: Delete ALL receipt items in this receipt
        for item in all_receipt_items:
            db_session.delete(item)

        # STEP 4: Delete the receipt itself
        db_session.delete(receipt)

        db_session.commit()

        return jsonify({
            'success': True,
            'message': f'Receipt #{receipt_number} with {total_items_in_receipt} items has been deleted successfully. ' +
                       f'This included the item "{item_name}". All associated ledger entries and inventory adjustments have been reversed.'
        })

    except Exception as e:
        db_session.rollback()
        logger.error(f'Error deleting receipt item: {str(e)}')
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


@purchases_bp.route('/bulk_delete_goods_receipts', methods=['POST'])
@login_required
def bulk_delete_goods_receipts():
    """
    Bulk delete goods receipts including posted ones
    This will reverse all ledger entries and inventory adjustments
    """
    app_id = current_user.app_id
    db_session = Session()

    try:
        data = request.get_json()
        goods_receipt_ids = data.get('gr_ids', [])

        if not goods_receipt_ids:
            return jsonify({
                'status': 'error',
                'message': 'No goods receipt IDs provided'
            }), 400

        # Call the bulk delete function
        success, message = bulk_delete_goods_receipts_handler(db_session, goods_receipt_ids, current_user)

        if success:
            return jsonify({
                'status': 'success',
                'message': message
            })
        else:
            return jsonify({
                'status': 'error',
                'message': message
            }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Bulk goods receipt deletion route error: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'An error occurred while deleting goods receipts: {str(e)}'
        }), 500
    finally:
        db_session.close()


def bulk_delete_goods_receipts_handler(db_session, goods_receipt_ids, current_user):
    """
    Bulk delete goods receipts including reversing ledger entries and inventory adjustments
    """
    try:
        app_id = current_user.app_id
        success_count = 0
        failed_receipts = []

        for receipt_id in goods_receipt_ids:
            receipt = None
            try:
                # Get the receipt with all related data
                receipt = db_session.query(GoodsReceipt).options(
                    joinedload(GoodsReceipt.receipt_items),
                    joinedload(GoodsReceipt.purchase_order),
                    joinedload(GoodsReceipt.direct_purchase)
                ).filter(
                    GoodsReceipt.id == receipt_id,
                    GoodsReceipt.app_id == app_id
                ).first()

                if not receipt:
                    failed_receipts.append(f"Goods receipt {receipt_id} not found")
                    continue

                receipt_number = receipt.receipt_number
                total_items = len(receipt.receipt_items)

                # STEP 1: Reverse inventory entries for the entire receipt
                try:
                    inventory_entries = db_session.query(InventoryEntry).filter(
                        InventoryEntry.inventory_source == 'goods_receipt',
                        InventoryEntry.source_id == receipt_id,
                        InventoryEntry.app_id == app_id
                    ).all()

                    for entry in inventory_entries:
                        reverse_purchase_inventory_entries(db_session, entry)
                    logger.info(f"Reversed {len(inventory_entries)} inventory entries for receipt {receipt_number}")
                except Exception as inv_error:
                    logger.error(f"Error reversing inventory entries for receipt {receipt_id}: {str(inv_error)}")
                    # Rollback and skip this receipt if inventory reversal fails
                    db_session.rollback()
                    failed_receipts.append(f"Goods receipt {receipt_id}: Inventory reversal failed - {str(inv_error)}")
                    continue

                # STEP 2: Delete ledger entries for the entire receipt
                try:
                    journals_deleted = delete_journal_entries_by_source(
                        db_session=db_session,
                        source_type='goods_receipt',
                        source_id=receipt_id,
                        app_id=app_id
                    )
                    logger.info(f"Deleted {journals_deleted} journal entries for receipt {receipt_number}")
                except Exception as journal_error:
                    logger.error(f"Error deleting journal entries for receipt {receipt_id}: {str(journal_error)}")
                    # Rollback and skip this receipt if journal deletion fails
                    db_session.rollback()
                    failed_receipts.append(
                        f"Goods receipt {receipt_id}: Journal deletion failed - {str(journal_error)}")
                    continue

                # STEP 3: Delete ALL receipt items in this receipt
                for item in receipt.receipt_items:
                    db_session.delete(item)

                # STEP 4: Delete the receipt itself
                db_session.delete(receipt)

                # STEP 5: Update purchase order status if this was a PO receipt
                if receipt.purchase_order_id:
                    purchase_order = receipt.purchase_order
                    # Recalculate overall received quantity for this PO
                    overall_total_received = db_session.query(
                        func.coalesce(func.sum(GoodsReceiptItem.quantity_received), 0)
                    ).join(GoodsReceipt).filter(
                        GoodsReceipt.purchase_order_id == purchase_order.id,
                        GoodsReceipt.app_id == app_id,
                        GoodsReceipt.id != receipt_id  # Exclude the receipt being deleted
                    ).scalar()

                    total_ordered = sum(Decimal(str(item.quantity)) for item in purchase_order.purchase_order_items)

                # Commit after each successful receipt deletion
                db_session.commit()
                success_count += 1

            except Exception as e:
                # Rollback any changes for this specific receipt
                db_session.rollback()
                error_msg = f"Goods receipt {receipt_id}: {str(e)}"
                failed_receipts.append(error_msg)
                logger.error(f"Error deleting goods receipt {receipt_id}: {str(e)}", exc_info=True)
                continue

        if failed_receipts:
            # Truncate error message if too long
            error_details = ', '.join(failed_receipts[:5])  # Show first 5 errors
            if len(failed_receipts) > 5:
                error_details += f" ... and {len(failed_receipts) - 5} more"

            return False, f"Deleted {success_count} goods receipts, failed {len(failed_receipts)}: {error_details}"

        return True, f"Successfully deleted {success_count} goods receipts"

    except Exception as e:
        # Final safety rollback for any unexpected errors
        db_session.rollback()
        logger.error(f"Bulk goods receipt deletion error: {str(e)}", exc_info=True)
        return False, f"Database error: {str(e)}"


@purchases_bp.route('/goods_receipts', methods=['GET', 'POST'])
@login_required
def goods_receipt_management():
    app_id = current_user.app_id
    db_session = Session()

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Base query
        query = db_session.query(GoodsReceipt).filter_by(app_id=app_id)

        # Process filter options
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        filter_type = request.args.get('filter_type', 'receipt_date')  # Default: receipt_date
        status_filter = request.args.get('status')
        source_type_filter = request.args.get('source_type')  # 'purchase_order' or 'direct_purchase'
        filter_applied = bool(start_date or end_date or status_filter or source_type_filter)

        # Apply date filters based on filter type
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')

                if filter_type == 'receipt_date':
                    if start_date:
                        query = query.filter(GoodsReceipt.receipt_date >= start_date)
                    if end_date:
                        query = query.filter(GoodsReceipt.receipt_date <= end_date)
                elif filter_type == 'created_date':
                    if start_date:
                        query = query.filter(GoodsReceipt.created_at >= start_date)
                    if end_date:
                        query = query.filter(GoodsReceipt.created_at <= end_date)
            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')

        # Apply status filter (ledger posting status)
        if status_filter:
            if status_filter == 'posted':
                query = query.filter(GoodsReceipt.is_posted_to_ledger == True)
            elif status_filter == 'unposted':
                query = query.filter(GoodsReceipt.is_posted_to_ledger == False)
            elif status_filter == 'inventory_posted':
                query = query.filter(GoodsReceipt.inventory_posted == True)
            elif status_filter == 'inventory_unposted':
                query = query.filter(GoodsReceipt.inventory_posted == False)

        # Apply source type filter
        if source_type_filter:
            if source_type_filter == 'purchase_order':
                query = query.filter(GoodsReceipt.purchase_order_id.isnot(None))
            elif source_type_filter == 'direct_purchase':
                query = query.filter(GoodsReceipt.direct_purchase_id.isnot(None))

        # Order by latest created_at
        goods_receipts = query.order_by(GoodsReceipt.created_at.desc()).all()

        # If no goods receipts exist, return a message
        if not goods_receipts:
            flash('No Goods Receipts found.', 'info')

        # Enhance goods receipt objects with additional information
        for gr in goods_receipts:
            # Determine source type and get source details
            if gr.purchase_order_id:
                gr.source_type = 'purchase_order'
                gr.source_reference = gr.purchase_order.purchase_order_number
                gr.vendor_name = gr.purchase_order.vendor.vendor_name
            elif gr.direct_purchase_id:
                gr.source_type = 'direct_purchase'
                gr.source_reference = gr.direct_purchase.direct_purchase_number
                gr.vendor_name = gr.direct_purchase.vendor.vendor_name

            # Check if all receipt items are posted to ledger
            all_items_posted = all(item.is_posted_to_ledger for item in gr.receipt_items)
            gr.all_items_posted = all_items_posted

            # Check if receipt has inventory items
            gr.has_inventory_items = any(
                (item.purchase_order_item and item.purchase_order_item.item_type == "inventory") or
                (item.direct_purchase_item and item.direct_purchase_item.item_type == "inventory")
                for item in gr.receipt_items
            )

        # Calculate dashboard metrics
        total_goods_receipts = query.count()

        # Count by source type
        po_receipts = db_session.query(GoodsReceipt).filter_by(app_id=app_id).filter(
            GoodsReceipt.purchase_order_id.isnot(None)
        ).count()

        direct_purchase_receipts = db_session.query(GoodsReceipt).filter_by(app_id=app_id).filter(
            GoodsReceipt.direct_purchase_id.isnot(None)
        ).count()

        # Count by posting status
        posted_to_ledger = query.filter(GoodsReceipt.is_posted_to_ledger == True).count()
        unposted_to_ledger = query.filter(GoodsReceipt.is_posted_to_ledger == False).count()

        # Count by inventory status
        inventory_posted = query.filter(GoodsReceipt.inventory_posted == True).count()
        inventory_unposted = query.filter(GoodsReceipt.inventory_posted == False).count()

        # Count complete vs partial receipts
        complete_receipts = query.filter(GoodsReceipt.is_complete_receipt == True).count()
        partial_receipts = query.filter(GoodsReceipt.is_complete_receipt == False).count()

        return render_template(
            '/purchases/goods_receipts.html',
            goods_receipts=goods_receipts,
            total_goods_receipts=total_goods_receipts,
            po_receipts=po_receipts,
            direct_purchase_receipts=direct_purchase_receipts,
            posted_to_ledger=posted_to_ledger,
            unposted_to_ledger=unposted_to_ledger,
            inventory_posted=inventory_posted,
            inventory_unposted=inventory_unposted,
            complete_receipts=complete_receipts,
            partial_receipts=partial_receipts,
            company=company,
            role=role,
            filter_applied=filter_applied,
            modules=modules_data
        )
    except Exception as e:
        flash(f'An error occurred: {str(e)}', 'error')
        logger.error(f'Error in goods receipt management: {e}')
        return redirect(url_for('purchases.goods_receipt_management'))
    finally:
        db_session.close()


@purchases_bp.route('/bulk_post_goods_receipts_to_ledger', methods=['POST'])
@login_required
def bulk_post_goods_receipts_to_ledger():
    """
    Bulk post goods receipts to ledger by updating journal status from 'Unposted' to 'Posted'
    """
    app_id = current_user.app_id
    db_session = Session()

    try:
        data = request.get_json()
        goods_receipt_ids = data.get('gr_ids', [])

        if not goods_receipt_ids:
            return jsonify({
                'status': 'error',
                'message': 'No goods receipt IDs provided'
            }), 400

        # Call the bulk posting function
        success, message = bulk_post_goods_receipts(db_session, goods_receipt_ids, current_user)

        if success:
            return jsonify({
                'status': 'success',
                'message': message
            })
        else:
            return jsonify({
                'status': 'error',
                'message': message
            }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Bulk goods receipt posting route error: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f'An error occurred while posting goods receipts: {str(e)}'
        }), 500
    finally:
        db_session.close()



@purchases_bp.route('/api/get_goods_receipt_details/<int:receipt_id>/<int:receipt_item_id>', methods=['GET'])
@login_required
def get_goods_receipt_details(receipt_id, receipt_item_id):
    """
    Retrieve details for a specific goods/service receipt item to populate the edit modal
    Supports both Purchase Order and Direct Purchase receipt items
    """

    db_session = Session()
    try:
        # Query the receipt with both purchase types
        receipt = db_session.query(GoodsReceipt).filter_by(id=receipt_id).first()
        if not receipt:

            return jsonify({'success': False, 'message': 'Goods receipt not found'}), 404

        # Query the specific receipt item with both purchase types
        receipt_item = db_session.query(GoodsReceiptItem).filter_by(
            id=receipt_item_id,
            goods_receipt_id=receipt_id
        ).options(
            joinedload(GoodsReceiptItem.purchase_order_item)
            .joinedload(PurchaseOrderItem.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item),
            joinedload(GoodsReceiptItem.direct_purchase_item)
            .joinedload(DirectPurchaseItem.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item)
        ).first()

        if not receipt_item:
            logger.error(f"Receipt item ID {receipt_item_id} not found for receipt ID {receipt_id}.")
            return jsonify({'success': False, 'message': 'Receipt item not found'}), 404

        # Determine if this is for a purchase order or direct purchase
        if receipt_item.purchase_order_item_id:
            item = receipt_item.purchase_order_item
            source_type = 'purchase_order'
        elif receipt_item.direct_purchase_item_id:
            item = receipt_item.direct_purchase_item
            source_type = 'direct_purchase'
        else:
            return jsonify({'success': False, 'message': 'Receipt item not linked to any purchase'}), 400

        # Get item name based on type
        item_name = None
        if item:
            if item.item_name:
                item_name = item.item_name
            elif hasattr(item, 'inventory_item_variation_link') and item.inventory_item_variation_link:
                item_name = item.inventory_item_variation_link.inventory_item.item_name

        # Prepare response data
        receipt_data = {
            'receipt_id': receipt.id,
            'version': receipt.version,
            'receipt_item_id': receipt_item.id,
            'receipt_number': receipt.receipt_number,
            'receipt_date': receipt.receipt_date.strftime('%Y-%m-%d') if receipt.receipt_date else None,
            'received_by': receipt.received_by,
            'item_id': receipt_item.purchase_order_item_id or receipt_item.direct_purchase_item_id,
            'item_name': item_name,
            'quantity_received': float(receipt_item.quantity_received),
            'received_condition': receipt_item.received_condition,
            'notes': receipt_item.notes,
            'is_posted_to_ledger': receipt_item.is_posted_to_ledger,
            'item_type': item.item_type if item else None,
            'source_type': source_type,
            'ordered_quantity': float(item.quantity) if item else None,
            'unit_of_measurement': item.unit_of_measurement.full_name if item and item.unit_of_measurement else None
        }

        return jsonify({'success': True, 'data': receipt_data})

    except Exception as e:
        logger.error("\n!!! ERROR FETCHING GOODS RECEIPT DETAILS !!!")
        logger.error(f"Error: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

    finally:
        db_session.close()
