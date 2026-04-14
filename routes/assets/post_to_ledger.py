import logging
import traceback
from decimal import Decimal

from flask import request, jsonify
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from ai import resolve_exchange_rate_for_transaction
from db import Session
from models import InventoryEntry, InventoryTransactionDetail, InventoryEntryLineItem, JournalEntry, Journal, \
    ChartOfAccounts, AssetMovement, AssetMovementLineItem
from services.chart_of_accounts_helpers import get_retained_earnings_account_id
from services.inventory_helpers import safe_clear_stock_history_cache
from services.post_to_ledger import create_inventory_ledger_entry
from utils import create_transaction
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.numbers import safe_int_conversion

from flask_login import login_required, current_user

logger = logging.getLogger(__name__)


def post_asset_movement_to_ledger(db_session, asset_movement, current_values, base_currency_id, current_user,
                                  posted_status):
    """
    Post an asset movement and all its line items to the general ledger.

    Parameters:
    -----------
    db_session: SQLAlchemy Session
        Database session
    asset_movement: AssetMovement
        The AssetMovement object to post (already created)
    current_values: list
        List of current values (book values) from the form submission
    base_currency_id: int
        Base currency ID
    current_user: User
        Current user performing the posting

    Returns:
    --------
    tuple: (success: bool, message: str)
    """
    try:
        # Check if already posted
        if asset_movement.is_posted_to_ledger:
            return True, f"Asset movement {asset_movement.id} already posted to ledger"

        # Process each line item with its corresponding current value
        for idx, line_item in enumerate(asset_movement.line_items):
            try:
                # Get the current value for this line item
                # IMPORTANT: This should be the book value from the form submission
                current_value = current_values[idx] if idx < len(current_values) else None

                create_asset_ledger_entry(
                    db_session=db_session,
                    base_currency_id=base_currency_id,
                    line_item=line_item,
                    asset_movement=asset_movement,
                    current_value=current_value,  # Pass the form-submitted book value
                    current_user=current_user,
                    posted_status=posted_status
                )
            except Exception as e:
                logger.error(f"Failed to post line item {line_item.id}: {str(e)}")
                return False, f"Failed to post line item {line_item.id}: {str(e)}"

        # Mark asset movement as posted
        asset_movement.is_posted_to_ledger = True
        asset_movement.posted_by = current_user.id

        db_session.commit()

        return True, f"Asset movement {asset_movement.id} posted to ledger successfully"

    except Exception as e:
        logger.error(f"Error in post_asset_movement_to_ledger: {str(e)}\n{traceback.format_exc()}")
        return False, f"Error posting to ledger: {str(e)}"


def post_depreciation_to_ledger(db_session, asset_movement, base_currency_id, current_user, posted_status):
    """
    Special handler for depreciation entries - simplified for new structure.
    """
    try:
        # Check if already posted
        if asset_movement.is_posted_to_ledger:
            return True, f"Depreciation movement {asset_movement.id} already posted"

        # For depreciation, each line item already has the depreciation amount
        for line_item in asset_movement.line_items:
            # Get the asset
            asset = line_item.asset
            if not asset:
                continue

            # Check if we have a depreciation amount
            depreciation_amount = line_item.transaction_value
            if not depreciation_amount or depreciation_amount <= 0:
                continue

            # Post to ledger
            create_asset_ledger_entry(
                db_session=db_session,
                base_currency_id=base_currency_id,
                line_item=line_item,
                asset_movement=asset_movement,
                current_user=current_user,
                posted_status=posted_status
            )

        # Mark as posted
        asset_movement.is_posted_to_ledger = True
        asset_movement.posted_by = current_user.id

        db_session.commit()
        return True, f"Depreciation movement {asset_movement.id} posted successfully"

    except Exception as e:
        logger.error(f"Error in post_depreciation_to_ledger: {str(e)}\n{traceback.format_exc()}")
        return False, f"Error posting depreciation: {str(e)}"


def create_asset_ledger_entry(db_session, base_currency_id, line_item, asset_movement, current_user,
                              posted_status, current_value=None):
    """
    Post individual asset line items to the general ledger.

    Parameters:
    -----------
    current_value: float or str
        The book value from the form submission (optional, will use from line_item if not provided)
    """
    try:
        app_id = current_user.app_id

        # Get the asset
        asset = line_item.assets
        if not asset:
            logger.error(f"Line item {line_item.id} has no asset!")
            return False

        # Get asset item for account information
        asset_item = asset.asset_item
        if not asset_item:
            raise ValueError(f"Asset item not found for asset {asset.id}")

        # Only get exchange rate if currency is not base currency
        exchange_rate_id = None
        if int(asset_movement.currency_id) != int(base_currency_id):
            exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
                session=db_session,
                currency_id=asset_movement.currency_id,
                transaction_date=asset_movement.transaction_date,
                app_id=app_id
            )

        # Get movement type and determine accounting treatment
        movement_type = asset_movement.movement_type
        description = f"Asset {movement_type} - {asset_movement.reference or 'No Reference'}"

        # Get current value - prioritize passed parameter, then line_item
        book_value = current_value if current_value is not None else getattr(line_item, 'book_value', None)

        # If still None, use asset's current value as fallback
        if book_value is None:
            book_value = asset.current_value

        # Convert to float if it's a string
        if isinstance(book_value, str):
            book_value = float(book_value)

        # Handle different movement types
        if movement_type in ['acquisition', 'donation_in', 'opening_balance']:
            _handle_asset_acquisition_transaction(
                db_session, line_item, asset_movement, asset, asset_item,
                exchange_rate_id, description, current_user, app_id, posted_status
            )

        elif movement_type in ['sale', 'donation_out', 'disposal']:
            _handle_asset_disposal_transaction(
                db_session, line_item, asset_movement, asset, asset_item,
                exchange_rate_id, description, current_user, app_id, posted_status,
                current_value=book_value  # Pass the book value
            )

        elif movement_type == 'depreciation':

            _handle_depreciation_transaction(
                db_session, line_item, asset_movement, asset, asset_item,
                exchange_rate_id, description, current_user, app_id, posted_status
            )

        elif movement_type in ['assignment', 'transfer', 'return']:
            # These are non-financial movements
            logger.info(f"Skipping non-financial asset movement {movement_type} - no ledger posting required")
            return True

        return True

    except Exception as e:
        logger.error(f'Error posting asset line item {line_item.id}: {e} \n{traceback.format_exc()}')
        raise Exception(f"Failed to post asset line item {line_item.id}: {str(e)}")


def _handle_asset_acquisition_transaction(db_session, line_item, asset_movement, asset, asset_item,
                                          exchange_rate_id, description, current_user, app_id, posted_status):
    """
    Handle accounting entries for asset acquisition transactions.
    """
    transaction_value = line_item.transaction_value or asset.current_value

    # FIXED: Use asset.current_value instead of transaction_value for donations
    if asset_movement.movement_type == 'donation_in':
        # For donations, use the asset's current value (book value)
        transaction_value = asset.current_value
    else:
        transaction_value = line_item.transaction_value or asset.current_value

    if asset_movement.movement_type == 'acquisition':
        # Asset Purchase - Debit Fixed Asset, Credit Payable
        if not asset_movement.payable_account_id:
            raise ValueError("Payable account not configured for asset acquisition")

        # Create journal entries
        journal, entries = create_transaction(
            db_session=db_session,
            date=asset_movement.transaction_date,
            currency=asset_movement.currency_id,
            created_by=current_user.id,
            app_id=app_id,
            narration=f"{description} - {asset.asset_tag} ({asset_item.asset_name})",
            project_id=asset_movement.project_id,
            vendor_id=line_item.party_id,
            exchange_rate_id=exchange_rate_id,
            status=posted_status,
            journal_ref_no=asset_movement.reference,
            lines=[
                # DEBIT Fixed Asset Account
                {
                    "subcategory_id": asset_item.fixed_asset_account_id,
                    "amount": transaction_value,
                    "dr_cr": "D",
                    "description": f"Asset Purchase: {asset.asset_tag} - {asset_item.asset_name}",
                    "source_type": "asset_movement",
                    "source_id": asset_movement.id
                },
                # CREDIT Payable Account
                {
                    "subcategory_id": asset_movement.payable_account_id,
                    "amount": transaction_value,
                    "dr_cr": "C",
                    "description": f"Asset Purchase: {asset.asset_tag} - {asset_item.asset_name}",
                    "source_type": "asset_movement",
                    "source_id": asset_movement.id
                }
            ]
        )

    elif asset_movement.movement_type == 'donation_in':
        # Asset Received as Donation - Debit Fixed Asset, Credit Other Income
        if not asset_movement.adjustment_account_id:
            raise ValueError("Adjustment account not configured for asset donation")

        journal, entries = create_transaction(
            db_session=db_session,
            date=asset_movement.transaction_date,
            currency=asset_movement.currency_id,
            created_by=current_user.id,
            app_id=app_id,
            narration=f"{description} - {asset.asset_tag} ({asset_item.asset_name})",
            project_id=asset_movement.project_id,
            vendor_id=line_item.party_id,
            exchange_rate_id=exchange_rate_id,
            status=posted_status,
            journal_ref_no=asset_movement.reference,
            lines=[
                # DEBIT Fixed Asset Account
                {
                    "subcategory_id": asset_item.fixed_asset_account_id,
                    "amount": transaction_value,
                    "dr_cr": "D",
                    "description": f"Asset Donation Received: {asset.asset_tag} - {asset_item.asset_name}",
                    "source_type": "asset_movement",
                    "source_id": asset_movement.id
                },
                # CREDIT Adjustment Account (Other Income)
                {
                    "subcategory_id": asset_movement.adjustment_account_id,
                    "amount": transaction_value,
                    "dr_cr": "C",
                    "description": f"Asset Donation Received: {asset.asset_tag} - {asset_item.asset_name}",
                    "source_type": "asset_movement",
                    "source_id": asset_movement.id
                }
            ]
        )

    elif asset_movement.movement_type == 'opening_balance':
        # Opening Balance Entry - Debit Fixed Asset, Credit Retained Earnings
        retained_earnings_account_id = get_retained_earnings_account_id(db_session, app_id, current_user.id)
        retained_earnings_account = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.id == retained_earnings_account_id,
            ChartOfAccounts.app_id == app_id
        ).first()

        if not retained_earnings_account:
            raise ValueError("Retained earnings account not found")

        journal, entries = create_transaction(
            db_session=db_session,
            date=asset_movement.transaction_date,
            currency=asset_movement.currency_id,
            created_by=current_user.id,
            app_id=app_id,
            narration=f"{description} - {asset.asset_tag} ({asset_item.asset_name})",
            project_id=asset_movement.project_id,
            exchange_rate_id=exchange_rate_id,
            status=posted_status,
            journal_ref_no=asset_movement.reference,
            lines=[
                # DEBIT Fixed Asset Account
                {
                    "subcategory_id": asset_item.fixed_asset_account_id,
                    "amount": transaction_value,
                    "dr_cr": "D",
                    "description": f"Asset Opening Balance: {asset.asset_tag} - {asset_item.asset_name}",
                    "source_type": "asset_movement",
                    "source_id": asset_movement.id
                },
                # CREDIT Retained Earnings
                {
                    "subcategory_id": retained_earnings_account.id,
                    "amount": transaction_value,
                    "dr_cr": "C",
                    "description": f"Asset Opening Balance: {asset.asset_tag} - {asset_item.asset_name}",
                    "source_type": "asset_movement",
                    "source_id": asset_movement.id
                }
            ]
        )


def _handle_asset_disposal_transaction(db_session, line_item, asset_movement, asset, asset_item,
                                       exchange_rate_id, description, current_user, app_id,
                                       posted_status, current_value=None):
    """
    Handle accounting entries for asset disposal transactions (sale, donation_out, disposal).
    Uses the current_value from the form submission as the book value.
    """
    # USE THE FORM-SUBMITTED CURRENT VALUE (BOOK VALUE) INSTEAD OF ASSET.CURRENT_VALUE
    # This ensures we use the correct book value at the time of disposal
    book_value = float(current_value) if current_value is not None else asset.current_value

    # CRITICAL: Debug logging to trace values
    print(
        f"DEBUG DISPOSAL: Asset={asset.asset_tag}, Form Book Value={book_value}, Asset DB Value={asset.current_value}, Transaction Value={line_item.transaction_value}")

    if asset_movement.movement_type == 'sale':
        # Asset Sale - calculate gain or loss
        sale_price = line_item.transaction_value or 0
        gain_loss = float(sale_price) - book_value

        # Validate required accounts
        if not asset_movement.sales_account_id:
            raise ValueError("Sales account not configured for asset sale")

        # CRITICAL: Validate the sale makes sense
        if sale_price <= 0:
            raise ValueError(f"Sale price must be positive for asset {asset.asset_tag}")
        if book_value <= 0:
            raise ValueError(f"Book value must be positive for asset {asset.asset_tag}")

        journal_lines = []

        # 1. DEBIT Sales Account (Cash/Receivable) - Sale proceeds
        journal_lines.append({
            "subcategory_id": asset_movement.sales_account_id,
            "amount": sale_price,  # FULL sale amount
            "dr_cr": "D",
            "description": f"Sale proceeds: {asset.asset_tag}",
            "source_type": "asset_movement",
            "source_id": asset_movement.id
        })

        # 2. CREDIT Fixed Asset - Remove book value (from form)
        journal_lines.append({
            "subcategory_id": asset_item.fixed_asset_account_id,
            "amount": book_value,  # Asset's book value FROM FORM
            "dr_cr": "C",
            "description": f"Remove asset: {asset.asset_tag} (BV: {book_value})",
            "source_type": "asset_movement",
            "source_id": asset_movement.id
        })

        # 3. Gain/Loss entry (if any)
        if abs(gain_loss) > 0.01:  # Using epsilon for floating point comparison
            if not asset_movement.adjustment_account_id:
                raise ValueError("Adjustment account not configured for gain/loss on sale")

            if gain_loss > 0:
                # Gain on Sale - CREDIT to income account
                journal_lines.append({
                    "subcategory_id": asset_movement.adjustment_account_id,
                    "amount": gain_loss,  # Gain amount
                    "dr_cr": "C",
                    "description": f"Gain on sale: {asset.asset_tag}",
                    "source_type": "asset_movement",
                    "source_id": asset_movement.id
                })
            else:
                # Loss on Sale - DEBIT to expense account
                journal_lines.append({
                    "subcategory_id": asset_movement.adjustment_account_id,
                    "amount": abs(gain_loss),  # Loss amount (positive)
                    "dr_cr": "D",
                    "description": f"Loss on sale: {asset.asset_tag}",
                    "source_type": "asset_movement",
                    "source_id": asset_movement.id
                })

    elif asset_movement.movement_type == 'donation_out':
        # Asset Donated - Debit Donation Expense, Credit Fixed Asset
        if not asset_movement.adjustment_account_id:
            raise ValueError("Adjustment account not configured for asset donation")

        journal_lines = [
            # DEBIT Adjustment Account (Donation Expense)
            {
                "subcategory_id": asset_movement.adjustment_account_id,
                "amount": book_value,
                "dr_cr": "D",
                "description": f"Asset donated: {asset.asset_tag} (BV: {book_value})",
                "source_type": "asset_movement",
                "source_id": asset_movement.id
            },
            # CREDIT Fixed Asset (book value from form)
            {
                "subcategory_id": asset_item.fixed_asset_account_id,
                "amount": book_value,
                "dr_cr": "C",
                "description": f"Remove donated asset: {asset.asset_tag} (BV: {book_value})",
                "source_type": "asset_movement",
                "source_id": asset_movement.id
            }
        ]

    elif asset_movement.movement_type == 'disposal':
        # Asset Disposal - Debit Loss, Credit Fixed Asset
        if not asset_movement.adjustment_account_id:
            raise ValueError("Adjustment account not configured for asset disposal")

        # Calculate disposal proceeds if any
        disposal_proceeds = line_item.transaction_value or 0
        loss_on_disposal = book_value - disposal_proceeds  # Positive = loss

        journal_lines = []

        # If there are disposal proceeds (e.g., sold for scrap)
        if disposal_proceeds > 0:
            # DEBIT Sales/Cash Account for proceeds
            if not asset_movement.sales_account_id:
                raise ValueError("Sales account required for disposal with proceeds")

            journal_lines.append({
                "subcategory_id": asset_movement.sales_account_id,
                "amount": disposal_proceeds,
                "dr_cr": "D",
                "description": f"Disposal proceeds: {asset.asset_tag}",
                "source_type": "asset_movement",
                "source_id": asset_movement.id
            })

        # DEBIT Loss on Disposal (if book value > proceeds)
        if loss_on_disposal > 0.01:  # Using epsilon
            journal_lines.append({
                "subcategory_id": asset_movement.adjustment_account_id,
                "amount": loss_on_disposal,
                "dr_cr": "D",
                "description": f"Loss on disposal: {asset.asset_tag} (BV: {book_value}, Proceeds: {disposal_proceeds})",
                "source_type": "asset_movement",
                "source_id": asset_movement.id
            })
        # CREDIT Gain on Disposal (if proceeds > book value) - rare but possible
        elif loss_on_disposal < -0.01:  # Using epsilon
            gain = abs(loss_on_disposal)
            journal_lines.append({
                "subcategory_id": asset_movement.adjustment_account_id,
                "amount": gain,
                "dr_cr": "C",
                "description": f"Gain on disposal: {asset.asset_tag} (BV: {book_value}, Proceeds: {disposal_proceeds})",
                "source_type": "asset_movement",
                "source_id": asset_movement.id
            })

        # CREDIT Fixed Asset (book value from form) - ALWAYS
        journal_lines.append({
            "subcategory_id": asset_item.fixed_asset_account_id,
            "amount": book_value,
            "dr_cr": "C",
            "description": f"Remove disposed asset: {asset.asset_tag} (BV: {book_value})",
            "source_type": "asset_movement",
            "source_id": asset_movement.id
        })

    else:
        raise ValueError(f"Unsupported disposal movement type: {asset_movement.movement_type}")

    # Create the journal with clear narration
    journal, entries = create_transaction(
        db_session=db_session,
        date=asset_movement.transaction_date,
        currency=asset_movement.currency_id,
        created_by=current_user.id,
        app_id=app_id,
        narration=f"{description} - {asset.asset_tag} (Book Value: {book_value})",
        project_id=asset.project_id,
        vendor_id=line_item.party_id,
        exchange_rate_id=exchange_rate_id,
        status=posted_status,
        journal_ref_no=f"{asset_movement.reference}-{asset.asset_tag}",
        lines=journal_lines
    )

    return journal, entries


def _handle_depreciation_transaction(db_session, line_item, asset_movement, asset, asset_item,
                                     exchange_rate_id, description, current_user, app_id, posted_status):
    """
    Handle accounting entries for depreciation transactions.
    """
    depreciation_amount = line_item.transaction_value

    if not depreciation_amount or depreciation_amount <= 0:
        raise ValueError(f"Invalid depreciation amount: {depreciation_amount}")

    if not asset_item.depreciation_expense_account_id or not asset_item.accumulated_depreciation_account_id:
        raise ValueError("Depreciation accounts not configured for asset item")

    journal, entries = create_transaction(
        db_session=db_session,
        date=asset_movement.transaction_date,
        currency=asset_movement.currency_id,
        created_by=current_user.id,
        app_id=app_id,
        narration=f"{description} - {asset.asset_tag} ({asset_item.asset_name})",
        project_id=asset.project_id,
        exchange_rate_id=exchange_rate_id,
        status=posted_status,
        journal_ref_no=asset_movement.reference,
        lines=[
            # DEBIT Depreciation Expense
            {
                "subcategory_id": asset_item.depreciation_expense_account_id,
                "amount": depreciation_amount,
                "dr_cr": "D",
                "description": f"Depreciation: {asset.asset_tag} - {asset_item.asset_name}",
                "source_type": "asset_movement",
                "source_id": line_item.id
            },
            # CREDIT Accumulated Depreciation
            {
                "subcategory_id": asset_item.accumulated_depreciation_account_id,
                "amount": depreciation_amount,
                "dr_cr": "C",
                "description": f"Accumulated Depreciation: {asset.asset_tag} - {asset_item.asset_name}",
                "source_type": "asset_movement",
                "source_id": line_item.id
            }
        ]
    )


def _calculate_accumulated_depreciation(db_session, asset, as_of_date):
    """
    Calculate accumulated depreciation for an asset up to a specific date.
    """
    # Query all depreciation line items for this asset up to the given date
    depreciation_line_items = db_session.query(AssetMovementLineItem).join(
        AssetMovement
    ).filter(
        AssetMovementLineItem.asset_id == asset.id,
        AssetMovement.transaction_date <= as_of_date,
        AssetMovement.movement_type == 'depreciation',
        AssetMovement.is_posted_to_ledger == True  # Only count posted depreciation
    ).all()

    total_depreciation = sum(
        [Decimal(str(item.transaction_value or 0)) for item in depreciation_line_items]
    )

    return total_depreciation
