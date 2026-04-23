import datetime
import logging
import math
import time
import traceback
from decimal import Decimal

from dateutil.relativedelta import relativedelta
from flask import request, render_template, session
from flask_login import current_user
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
import re

from sqlalchemy.orm import joinedload

from ai import get_or_create_exchange_rate_id, get_exchange_rate, get_base_currency
from db import Session
from models import InventoryLocation, Vendor, Project, InventoryItemVariationLink, \
    InventoryCategory, PaymentMode, InventorySubCategory, InventoryItemVariation, InventoryItemAttribute, Brand, \
    UnitOfMeasurement, Currency, Company, InventoryEntry, ExchangeRate, InventoryEntryLineItem, InventorySummary, \
    InventoryTransactionDetail, ChartOfAccounts, JournalEntry, Journal, DirectSalesTransaction, UserLocationAssignment, \
    User, Asset, AssetItem, AssetTransfer, AssetMovement, AssetMovementLineItem, DepreciationRecord
from services.chart_of_accounts_helpers import update_retained_earnings_opening_balance, \
    reverse_retained_earnings_balance
from services.vendors_and_customers import handle_party_logic

from utils import generate_unique_lot, create_notification, empty_to_none
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.date_time_utils import parse_date

logger = logging.getLogger(__name__)


def suggest_next_asset_reference(db_session):
    """
    Suggest the next asset reference based on the most recent reference entered by the user.
    - If the last reference ends with a number, increment it while preserving leading zeros.
    - Otherwise, return the last reference with '-1' appended.
    - If there is no reference at all, return a clean default like 'AST-REF-1'.
    """
    app_id = current_user.app_id  # Assumes current_user is available

    # Get the last entered reference
    last_entry = db_session.query(AssetMovement.reference).filter(
        AssetMovement.app_id == app_id,
        AssetMovement.reference != None
    ).order_by(AssetMovement.id.desc()).first()

    if not last_entry or not last_entry.reference:
        return "AST-REF-1"  # Default suggestion

    last_ref = last_entry.reference.strip()

    # Check if it ends with a number
    match = re.search(r'(\d+)$', last_ref)
    if match:
        number_str = match.group(1)
        number_len = len(number_str)  # Preserve digit length
        number = int(number_str) + 1
        new_ref = last_ref[:match.start(1)] + str(number).zfill(number_len)
    else:
        new_ref = f"{last_ref}-1"

    return new_ref


def process_asset_entries(
        db_session,
        app_id,
        movement_type,
        asset_item_ids,
        asset_ids=None,
        asset_tags=None,
        serial_numbers=None,
        purchase_prices=None,
        current_values=None,
        sale_prices=None,
        disposal_values=None,
        depreciation_amounts=None,
        sale_notes=None,
        depreciation_notes=None,
        transfer_reasons=None,
        transaction_date=None,
        supplier_ids=None,
        customer_ids=None,
        supplier_names=None,
        customer_names=None,
        currency_id=None,
        base_currency_id=None,
        reference=None,
        project_ids=None,
        location_id=None,
        current_user_id=None,
        payable_account_id=None,
        adjustment_account_id=None,
        sales_account_id=None,
        handled_by=None,
        additional_data=None
):
    """
    Process asset entries using the NEW AssetMovement table structure
    """
    try:
        # ===== VALIDATION FOR OUT MOVEMENTS =====
        out_movements = ['sale', 'donation_out', 'disposal']

        # For out movements, location should NOT be required/saved
        if movement_type in out_movements:
            # Override location_id to None (not saved)
            location_id = None

            # Log that we're ignoring location for out movements
            logger.info(f"Movement type '{movement_type}' is an out movement - location will not be saved")

        # 1. Create AssetMovement header
        asset_movement = AssetMovement(
            app_id=app_id,
            movement_type=movement_type,
            transaction_date=transaction_date,
            reference=reference,
            currency_id=currency_id,
            payable_account_id=payable_account_id,
            adjustment_account_id=adjustment_account_id,
            sales_account_id=sales_account_id,
            status='completed',
            created_by=current_user_id,
            created_at=func.now(),
            handled_by=handled_by,
            location_id=location_id  # Will be None for out movements
        )

        db_session.add(asset_movement)
        db_session.flush()

        created_assets = []
        updated_assets = []

        # 2. Process each line item
        for idx in range(len(asset_item_ids)):
            try:
                # Determine party ID based on movement type
                line_party_id = None

                # Handle supplier for acquisitions/donations in
                if movement_type in ['acquisition', 'donation_in']:
                    # Create form-like dictionary for this line's supplier data
                    line_supplier_data = {}

                    # Add supplier ID if available
                    if idx < len(supplier_ids) and supplier_ids[idx]:
                        line_supplier_data['supplier_id'] = supplier_ids[idx]
                        line_supplier_data['supplier_id[]'] = supplier_ids[idx]

                    # Add supplier name if available
                    if idx < len(supplier_names) and supplier_names[idx]:
                        line_supplier_data['supplier_name'] = supplier_names[idx]
                        line_supplier_data['supplier_name[]'] = supplier_names[idx]

                    # Handle party logic using your function
                    if line_supplier_data:  # Only call if we have data
                        party_id, party_name = handle_party_logic(
                            form_data=line_supplier_data,
                            app_id=app_id,
                            db_session=db_session,
                            party_type="Vendor"
                        )
                        line_party_id = party_id

                # Handle customer for sales/donations out
                elif movement_type in ['sale', 'donation_out']:
                    # Create form-like dictionary for this line's customer data
                    line_customer_data = {}

                    # Add customer ID if available
                    if idx < len(customer_ids) and customer_ids[idx]:
                        line_customer_data['customer_id'] = customer_ids[idx]
                        line_customer_data['customer_id[]'] = customer_ids[idx]

                    # Add customer name if available
                    if idx < len(customer_names) and customer_names[idx]:
                        line_customer_data['customer_name'] = customer_names[idx]
                        line_customer_data['customer_name[]'] = customer_names[idx]

                    # Handle party logic using your function
                    if line_customer_data:  # Only call if we have data
                        party_id, party_name = handle_party_logic(
                            form_data=line_customer_data,
                            app_id=app_id,
                            db_session=db_session,
                            party_type="Client"
                        )
                        line_party_id = party_id

                # If still no party ID, but we have additional_data with party info
                if not line_party_id and additional_data:
                    # Check for generic party_id[] array
                    party_ids_list = additional_data.getlist('party_id[]')
                    party_names_list = additional_data.getlist('party_name[]')

                    party_data = {}
                    if idx < len(party_ids_list) and party_ids_list[idx]:
                        party_data['party_id'] = party_ids_list[idx]
                        party_data['party_id[]'] = party_ids_list[idx]

                    if idx < len(party_names_list) and party_names_list[idx]:
                        party_data['party_name'] = party_names_list[idx]
                        party_data['party_name[]'] = party_names_list[idx]

                    if party_data:
                        party_type_to_use = "Vendor" if movement_type in ['acquisition', 'donation_in'] else "Client"
                        party_id, party_name = handle_party_logic(
                            form_data=party_data,
                            app_id=app_id,
                            db_session=db_session,
                            party_type=party_type_to_use
                        )
                        line_party_id = party_id

                # Initialize asset variable
                asset = None

                # 3. Process the asset based on movement type
                if movement_type in ['acquisition', 'donation_in', 'opening_balance']:
                    # Determine the right transaction value
                    if movement_type == 'donation_in':
                        transaction_val = Decimal(current_values[idx]) if idx < len(current_values) and current_values[
                            idx] else Decimal('0')
                    else:
                        transaction_val = Decimal(purchase_prices[idx]) if idx < len(purchase_prices) and \
                                                                           purchase_prices[idx] else Decimal('0')

                    # CREATE NEW ASSET (ONLY ONCE!)
                    asset = _create_asset_from_line_item(
                        db_session=db_session,
                        app_id=app_id,
                        movement_type=movement_type,
                        asset_item_id=asset_item_ids[idx],
                        transaction_value=transaction_val,
                        asset_tag=asset_tags[idx] if idx < len(asset_tags) else None,
                        serial_number=serial_numbers[idx] if idx < len(serial_numbers) else None,
                        transaction_date=transaction_date,
                        project_id=project_ids[idx] if idx < len(project_ids) else None,
                        location_id=location_id,
                        party_id=line_party_id,
                        additional_data=additional_data,
                        idx=idx
                    )
                    created_assets.append(asset)

                elif movement_type in ['sale', 'donation_out', 'disposal', 'assignment', 'transfer', 'return']:
                    # UPDATE EXISTING ASSET
                    asset = _update_existing_asset_from_line_item(
                        db_session=db_session,
                        app_id=app_id,
                        movement_type=movement_type,
                        asset_id=asset_ids[idx] if idx < len(asset_ids) and asset_ids[idx] else None,
                        transaction_date=transaction_date,
                        additional_data=additional_data,
                        idx=idx
                    )
                    updated_assets.append(asset)

                # Handle depreciation
                elif movement_type == 'depreciation':
                    # UPDATE EXISTING ASSET WITH DEPRECIATION
                    if idx < len(asset_ids) and asset_ids[idx]:
                        asset_id_val = int(asset_ids[idx]) if str(asset_ids[idx]).isdigit() else None
                        if asset_id_val:
                            asset = db_session.query(Asset).filter_by(id=asset_id_val, app_id=app_id).first()

                            if asset:
                                # Get depreciation data
                                dep_amount = Decimal(depreciation_amounts[idx]) if idx < len(depreciation_amounts) and \
                                                                                   depreciation_amounts[
                                                                                       idx] else Decimal('0')
                                dep_notes = depreciation_notes[idx] if idx < len(depreciation_notes) else None

                                # Update asset's current value
                                if dep_amount > 0:
                                    previous_value = asset.current_value
                                    new_value = max(Decimal('0'), asset.current_value - dep_amount)

                                    # Update asset
                                    asset.current_value = new_value
                                    asset.last_depreciation_date = transaction_date

                                    # ✅ CREATE DEPRECIATION RECORD
                                    depreciation_record = DepreciationRecord(
                                        app_id=app_id,
                                        asset_id=asset.id,
                                        asset_movement_line_item_id=None,  # Will set after line item is created
                                        depreciation_date=transaction_date,
                                        depreciation_amount=dep_amount,
                                        previous_value=previous_value,
                                        new_value=new_value,
                                        notes=dep_notes,
                                        created_by=current_user_id,
                                        created_at=func.now()
                                    )
                                    db_session.add(depreciation_record)
                                    db_session.flush()

                                    updated_assets.append(asset)

                # 4. Create line item with NEW MODEL STRUCTURE
                # Determine transaction value
                if movement_type == 'depreciation' and asset:
                    transaction_value = Decimal(depreciation_amounts[idx]) if idx < len(depreciation_amounts) and \
                                                                              depreciation_amounts[idx] else Decimal(
                        '0')
                elif movement_type in ['sale', 'donation_out', 'disposal']:
                    transaction_value = Decimal(sale_prices[idx]) if idx < len(sale_prices) and sale_prices[
                        idx] else Decimal('0')
                else:
                    transaction_value = Decimal(purchase_prices[idx]) if idx < len(purchase_prices) and purchase_prices[
                        idx] else Decimal('0')

                # Build line notes - add a separator if multiple notes exist
                line_notes_parts = []

                if movement_type == 'depreciation' and idx < len(depreciation_notes) and depreciation_notes[idx]:
                    line_notes_parts.append(depreciation_notes[idx])

                if movement_type == 'sale' and idx < len(sale_notes) and sale_notes[idx]:
                    line_notes_parts.append(sale_notes[idx])

                if movement_type == 'transfer' and transfer_reasons and idx < len(transfer_reasons) and \
                        transfer_reasons[idx]:
                    line_notes_parts.append(transfer_reasons[idx])

                if movement_type == 'assignment' and additional_data:
                    assignment_notes = additional_data.getlist('assignment_notes[]')
                    if idx < len(assignment_notes) and assignment_notes[idx]:
                        line_notes_parts.append(assignment_notes[idx])

                # Join with separator if multiple notes
                line_notes = " | ".join(line_notes_parts) if line_notes_parts else ""

                # ===== FIX: For out movements, DO NOT save project_id =====
                should_save_project = movement_type not in out_movements

                # Create the line item
                line_item = AssetMovementLineItem(
                    asset_movement_id=asset_movement.id,
                    app_id=app_id,
                    asset_id=asset.id if asset else None,
                    party_id=line_party_id,
                    transaction_value=transaction_value,
                    line_notes=line_notes if line_notes else '',
                    project_id=project_ids[idx] if should_save_project and project_ids and idx < len(project_ids) else None,
                )

                # Set location/department fields based on movement type
                if movement_type in ['transfer', 'assignment', 'return'] and asset:
                    # Get current location/department FROM THE ASSET
                    if asset.location_id:
                        line_item.from_location_id = asset.location_id
                    if asset.department_id:
                        line_item.from_department_id = asset.department_id

                    # Get TO locations and departments from form
                    prefix = f'line_{idx}_' if idx > 0 else ''

                    if movement_type == 'transfer':
                        to_location_id = get_form_value(additional_data, f'{prefix}to_location_id[]',
                                                        'to_location_id[]')
                        to_department_id = get_form_value(additional_data, f'{prefix}to_department_id[]',
                                                          'to_department_id[]')

                        if to_location_id and to_location_id.isdigit():
                            line_item.to_location_id = int(to_location_id)
                        if to_department_id and to_department_id.isdigit():
                            line_item.to_department_id = int(to_department_id)

                    elif movement_type == 'assignment':
                        assigned_to_id = get_form_value(additional_data, f'{prefix}assigned_to_id[]',
                                                        'assigned_to_id[]')
                        to_department_id = get_form_value(additional_data, f'{prefix}to_department_id[]',
                                                          'to_department_id[]')

                        if assigned_to_id and assigned_to_id.isdigit():
                            line_item.assigned_to_id = int(assigned_to_id)
                        if to_department_id and to_department_id.isdigit():
                            line_item.to_department_id = int(to_department_id)

                    elif movement_type == 'return':
                        # For returns, to_location/to_department might be a storage location
                        to_location_id = get_form_value(additional_data, f'{prefix}to_location_id[]',
                                                        'to_location_id[]')
                        to_department_id = get_form_value(additional_data, f'{prefix}to_department_id[]',
                                                          'to_department_id[]')

                        if to_location_id and to_location_id.isdigit():
                            line_item.to_location_id = int(to_location_id)
                        if to_department_id and to_department_id.isdigit():
                            line_item.to_department_id = int(to_department_id)

                # For out movements, we don't need to set any location/department fields
                # since the asset is leaving the system

                if movement_type == 'depreciation' and asset:
                    # Find the depreciation record we just created
                    depreciation_record = db_session.query(DepreciationRecord).filter_by(
                        app_id=app_id,
                        asset_id=asset.id,
                        depreciation_date=transaction_date,
                        depreciation_amount=Decimal(depreciation_amounts[idx]) if idx < len(depreciation_amounts) and
                                                                                  depreciation_amounts[
                                                                                      idx] else Decimal('0')
                    ).order_by(DepreciationRecord.created_at.desc()).first()

                    if depreciation_record:
                        depreciation_record.asset_movement_line_item_id = line_item.id
                        logger.info(f"Linked depreciation record {depreciation_record.id} to line item {line_item.id}")

                db_session.add(line_item)
                db_session.flush()

            except IntegrityError as e:
                # Rollback the current transaction
                db_session.rollback()

                # Parse the error message to find which constraint failed
                error_msg = str(e.orig)

                # Check for asset_tag constraint
                if 'asset.asset_tag' in error_msg or 'UNIQUE constraint failed: asset.asset_tag' in error_msg:
                    asset_tag_value = asset_tags[idx] if idx < len(asset_tags) else "N/A"
                    raise ValueError(f"Asset tag '{asset_tag_value}' is already in use. Please use a unique asset tag.")

                # Check for serial_number constraint
                elif 'asset.serial_number' in error_msg or 'UNIQUE constraint failed: asset.serial_number' in error_msg:
                    serial_value = serial_numbers[idx] if idx < len(serial_numbers) else "N/A"
                    raise ValueError(
                        f"Serial number '{serial_value}' is already in use. Please use a unique serial number.")

                # Generic integrity error
                else:
                    # Try to extract more details
                    match = re.search(r'UNIQUE constraint failed: (\w+\.\w+)', error_msg)
                    if match:
                        constraint = match.group(1)
                        raise ValueError(f"Duplicate value violation: {constraint}")
                    else:
                        raise ValueError(f"Database constraint violation: {error_msg}")

            except Exception as e:
                logger.error(f"Error processing asset line item {idx}: {str(e)}")
                raise ValueError(f"Error processing asset {idx + 1}: {str(e)}")

        db_session.flush()

        return {
            'asset_movement': asset_movement,
            'created_count': len(created_assets),
            'updated_count': len(updated_assets),
            'created_assets': created_assets,
            'updated_assets': updated_assets,
            'reference': reference,
            'movement_type': movement_type
        }

    except Exception as e:
        logger.error(f"Error in process_asset_entries: {str(e)}\n{traceback.format_exc()}")
        raise

def _create_asset_from_line_item(db_session, app_id, movement_type, asset_item_id, transaction_value,
                                 asset_tag, serial_number, transaction_date, project_id, location_id, party_id,
                                 additional_data, idx):
    """
    Create a new Asset with NEW model structure
    Only called for INCOMING movements: acquisition, donation_in, opening_balance
    """
    # Validate - this function should NOT be called for out movements
    out_movements = ['sale', 'donation_out', 'disposal']
    if movement_type in out_movements:
        raise ValueError(f"_create_asset_from_line_item should not be called for movement type: {movement_type}")

    # Generate asset tag if not provided
    if not asset_tag or asset_tag.strip() == '':
        asset_tag = f"AST-{int(time.time())}-{asset_item_id}-{idx}"

    # Ensure asset tag is unique - add timestamp for uniqueness
    asset_tag = f"{asset_tag}-{int(time.time()*1000)}"[-50:]  # Trim to max length

    # Get additional data for this specific line
    prefix = f'line_{idx}_' if idx > 0 else ''

    # Get depreciation overrides WITH INDEX
    useful_life_years = get_form_value(additional_data, f'{prefix}useful_life_years[]', 'useful_life_years[]', idx=idx)
    depreciation_method = get_form_value(additional_data, f'{prefix}depreciation_method[]', 'depreciation_method[]',
                                         idx=idx)
    capitalization_date_str = get_form_value(additional_data, f'{prefix}capitalization_date[]', 'capitalization_date[]',
                                             idx=idx)

    # Get warranty info WITH INDEX
    warranty_months = get_form_value(additional_data, f'{prefix}warranty_months[]', 'warranty_months[]', idx=idx)
    warranty_expiry_str = get_form_value(additional_data, f'{prefix}warranty_expiry[]', 'warranty_expiry[]', idx=idx)

    # Get condition WITH INDEX
    condition = get_form_value(additional_data, f'{prefix}condition[]', 'condition[]', 'good', idx=idx)

    # Get current value from form - WITH INDEX
    current_value_str = get_form_value(additional_data, f'{prefix}current_value[]', 'current_value[]', idx=idx)

    # Parse dates
    capitalization_date = parse_date(capitalization_date_str) if capitalization_date_str else parse_date(
        transaction_date)
    warranty_expiry = parse_date(warranty_expiry_str) or calculate_warranty_expiry(warranty_months, transaction_date)

    # Get asset item for defaults
    asset_item = db_session.query(AssetItem).filter_by(id=asset_item_id, app_id=app_id).first()
    if not asset_item:
        raise ValueError(f"AssetItem with ID {asset_item_id} not found")

    # Determine purchase price based on movement type
    if movement_type == 'donation_in':
        purchase_price = Decimal('0')
    else:
        purchase_price = transaction_value

    # Determine current value - use from form if available, otherwise use transaction value
    if current_value_str:
        try:
            current_value = Decimal(current_value_str)
        except:
            current_value = transaction_value
    else:
        current_value = transaction_value

    # For opening_balance, current_value should be the opening value
    if movement_type == 'opening_balance':
        current_value = transaction_value

    # ===== VALIDATION FOR INCOMING MOVEMENTS =====
    # For acquisitions and opening_balance, project_id and location_id are REQUIRED
    if movement_type in ['acquisition', 'opening_balance']:
        if not project_id:
            raise ValueError(f"Department (project) is required for {movement_type}")
        if not location_id:
            raise ValueError(f"Location is required for {movement_type}")

    # For donation_in, project_id and location_id are REQUIRED but may have different rules
    if movement_type == 'donation_in':
        if not project_id:
            raise ValueError(f"Department (project) is required for donation_in")
        if not location_id:
            raise ValueError(f"Location is required for donation_in")

    # Create the asset with NEW model structure
    asset = Asset(
        app_id=app_id,
        asset_item_id=asset_item_id,
        asset_tag=asset_tag,
        serial_number=serial_number,
        status='in_stock',
        condition=condition,
        purchase_price=purchase_price,
        current_value=current_value,
        purchase_date=transaction_date if movement_type in ['acquisition', 'donation_in'] else None,
        supplier_id=party_id if movement_type in ['acquisition', 'donation_in'] else None,
        project_id=project_id,
        location_id=location_id,
        capitalization_date=capitalization_date,
        useful_life_years=int(
            useful_life_years) if useful_life_years and str(useful_life_years).isdigit() else asset_item.expected_useful_life_years,
        depreciation_method=depreciation_method if depreciation_method else asset_item.depreciation_method,
        warranty_expiry=warranty_expiry,
        created_at=func.now()
    )

    if movement_type == 'opening_balance':
        asset.purchase_date = None

    db_session.add(asset)
    db_session.flush()

    logger.info(f"Created new asset: {asset.asset_tag} (ID: {asset.id}) for movement type: {movement_type}")

    return asset


def _update_existing_asset_from_line_item(db_session, app_id, movement_type, asset_id,
                                          transaction_date, additional_data, idx):
    """
    Update an existing Asset with NEW model structure
    Called for: sale, donation_out, disposal, assignment, transfer, return
    """
    if not asset_id:
        raise ValueError(f"Asset ID is required for {movement_type}")

    # Get the asset
    asset = db_session.query(Asset).filter_by(
        id=asset_id,
        app_id=app_id
    ).first()

    if not asset:
        raise ValueError(f"Asset with ID {asset_id} not found")

    # Store previous state for audit trail (if needed)
    previous_status = asset.status
    previous_location = asset.location_id
    previous_assigned = asset.assigned_to_id
    previous_department = asset.department_id
    previous_value = asset.current_value
    previous_project = asset.project_id

    # Get additional data for this specific line
    prefix = f'line_{idx}_' if idx > 0 else ''

    # ===== FIX: For out movements, clear department and location from asset =====
    out_movements = ['sale', 'donation_out', 'disposal']

    # Update based on movement type
    if movement_type == 'sale':
        asset.status = 'sold'
        # Clear department and location since asset is leaving
        asset.project_id = None
        asset.location_id = None
        # Get sale price if available
        sale_price = get_form_value(additional_data, f'{prefix}sale_price[]', 'sale_price[]', idx=idx)
        if sale_price:
            try:
                # Don't set current_value to sale price - keep book value for accounting
                # Instead, store sale price separately or just update current_value if needed
                # For accounting, you might want to keep original value
                pass
            except:
                pass

    elif movement_type == 'donation_out':
        asset.status = 'donated'
        # Clear department and location since asset is leaving
        asset.project_id = None
        asset.location_id = None

    elif movement_type == 'disposal':
        asset.status = 'disposed'
        # Clear department and location since asset is leaving
        asset.project_id = None
        asset.location_id = None
        # Get disposal value if available
        disposal_value = get_form_value(additional_data, f'{prefix}disposal_value[]', 'disposal_value[]', idx=idx)
        if disposal_value:
            try:
                # Record disposal value, but keep current value for accounting
                pass
            except:
                pass

    elif movement_type == 'assignment':
        asset.status = 'assigned'
        assigned_to_id = get_form_value(additional_data, f'{prefix}assigned_to_id[]', 'assigned_to_id[]', idx=idx)
        to_department_id = get_form_value(additional_data, f'{prefix}to_department_id[]', 'to_department_id[]', idx=idx)

        if assigned_to_id and str(assigned_to_id).isdigit():
            asset.assigned_to_id = int(assigned_to_id)
        if to_department_id and str(to_department_id).isdigit():
            asset.department_id = int(to_department_id)
        # Note: For assignment, keep project_id and location_id as they are

    elif movement_type == 'return':
        asset.status = 'in_stock'
        asset.assigned_to_id = None
        asset.department_id = None
        # Optionally set to a storage location
        to_location_id = get_form_value(additional_data, f'{prefix}to_location_id[]', 'to_location_id[]', idx=idx)
        if to_location_id and str(to_location_id).isdigit():
            asset.location_id = int(to_location_id)

    elif movement_type == 'transfer':
        to_location_id = get_form_value(additional_data, f'{prefix}to_location_id[]', 'to_location_id[]', idx=idx)
        to_department_id = get_form_value(additional_data, f'{prefix}to_department_id[]', 'to_department_id[]', idx=idx)

        if to_location_id and str(to_location_id).isdigit():
            asset.location_id = int(to_location_id)
        if to_department_id and str(to_department_id).isdigit():
            asset.department_id = int(to_department_id)
        # Note: For transfer, project_id stays the same unless changed

    # Update timestamp
    asset.updated_at = func.now()

    # Log the change for debugging
    logger.info(f"Updated asset {asset.asset_tag} (ID: {asset.id}) - "
                f"Movement: {movement_type}, Status: {previous_status} -> {asset.status}")

    # Store previous values for line item creation (optional - for audit trail)
    asset._previous_status = previous_status
    asset._previous_location = previous_location
    asset._previous_assigned = previous_assigned
    asset._previous_department = previous_department
    asset._previous_value = previous_value
    asset._previous_project = previous_project

    return asset


def calculate_warranty_expiry(warranty_months, transaction_date):
    """Calculate warranty expiry date"""
    if warranty_months and warranty_months.isdigit():
        return transaction_date + relativedelta(months=int(warranty_months))
    return None


def get_form_value(form_data, specific_key, general_key, default=None, idx=None):
    """
    Helper to get form value with fallback
    """
    # ALWAYS try the specific_key first
    if specific_key.endswith('[]'):
        array_values = form_data.getlist(specific_key)
        if idx is not None:
            if idx < len(array_values) and array_values[idx]:
                return array_values[idx]
        elif array_values and len(array_values) > 0:
            return array_values[0]

    # Then try the general_key
    if general_key.endswith('[]'):
        array_values = form_data.getlist(general_key)
        if idx is not None:
            if idx < len(array_values) and array_values[idx]:
                return array_values[idx]
        elif array_values and len(array_values) > 0:
            return array_values[0]

    # Fallback for non-array fields
    value = form_data.get(specific_key)
    if value is None:
        value = form_data.get(general_key)

    return value if value is not None else default


def process_edit_asset_entries(
        db_session,
        app_id,
        asset_movement,  # Pass the existing asset movement object
        movement_type,
        asset_item_ids,
        asset_ids=None,
        asset_tags=None,
        serial_numbers=None,
        purchase_prices=None,
        current_values=None,
        sale_prices=None,
        disposal_values=None,
        depreciation_amounts=None,
        depreciation_notes=None,
        sale_notes=None,
        transfer_reasons=None,
        transaction_date=None,
        supplier_ids=None,
        customer_ids=None,
        supplier_names=None,
        customer_names=None,
        currency_id=None,
        base_currency_id=None,
        reference=None,
        project_ids=None,
        location_id=None,
        current_user_id=None,
        payable_account_id=None,
        adjustment_account_id=None,
        sales_account_id=None,
        additional_data=None,
        original_line_items=None
):
    """
    Process asset entries for EDIT mode - uses existing AssetMovement header
    """
    try:
        # ===== OUT MOVEMENTS HANDLING =====
        out_movements = ['sale', 'donation_out', 'disposal']

        # For out movements, DO NOT save project_id or location_id
        should_save_project = movement_type not in out_movements
        should_save_location = movement_type not in out_movements

        if movement_type in out_movements:
            # Override location_id to None for out movements
            location_id = None
            logger.info(f"Edit: Movement type '{movement_type}' is an out movement - location will not be saved")

        created_assets = []
        updated_assets = []

        # ===== DELETE REMOVED ASSETS FOR ACQUISITION-TYPE MOVEMENTS =====
        if movement_type in ['acquisition', 'opening_balance', 'donation_in'] and original_line_items:
            # Get original asset IDs from the movement
            original_asset_ids = {item.asset_id for item in original_line_items if item.asset_id}

            # Get current asset IDs from form
            current_asset_ids = set()
            if asset_ids:
                for asset_id in asset_ids:
                    if asset_id and str(asset_id).isdigit():
                        current_asset_ids.add(int(asset_id))

            # Find removed assets
            removed_asset_ids = original_asset_ids - current_asset_ids

            for asset_id in removed_asset_ids:
                # Check if this asset has any OTHER movements (excluding this one)
                other_movements = db_session.query(AssetMovementLineItem).filter(
                    AssetMovementLineItem.asset_id == asset_id,
                    AssetMovementLineItem.asset_movement_id != asset_movement.id,
                    AssetMovementLineItem.app_id == app_id
                ).first()

                if not other_movements:
                    # This asset was ONLY in this movement - delete it
                    asset = db_session.query(Asset).filter_by(id=asset_id, app_id=app_id).first()
                    if asset:
                        logger.info(f"Deleting removed asset {asset_id} - {asset.asset_tag}")
                        db_session.delete(asset)
                else:
                    logger.info(f"Asset {asset_id} has other movements, keeping it (removed from this movement only)")

        # 2. Process each line item
        for idx in range(len(asset_item_ids)):
            try:
                # Determine party ID based on movement type
                line_party_id = None

                # Store existing asset ID for edit mode uniqueness checks
                existing_asset_id = None
                if idx < len(asset_ids) and asset_ids[idx]:
                    try:
                        existing_asset_id = int(asset_ids[idx]) if str(asset_ids[idx]).isdigit() else None
                    except:
                        existing_asset_id = None

                # Handle supplier for acquisitions/donations in
                if movement_type in ['acquisition', 'donation_in']:
                    # Create form-like dictionary for this line's supplier data
                    line_supplier_data = {}

                    # Add supplier ID if available
                    if idx < len(supplier_ids) and supplier_ids[idx]:
                        line_supplier_data['supplier_id'] = supplier_ids[idx]
                        line_supplier_data['supplier_id[]'] = supplier_ids[idx]

                    # Add supplier name if available
                    if idx < len(supplier_names) and supplier_names[idx]:
                        line_supplier_data['supplier_name'] = supplier_names[idx]
                        line_supplier_data['supplier_name[]'] = supplier_names[idx]

                    # Handle party logic using your function
                    if line_supplier_data:  # Only call if we have data
                        party_id, party_name = handle_party_logic(
                            form_data=line_supplier_data,
                            app_id=app_id,
                            db_session=db_session,
                            party_type="Vendor"
                        )
                        line_party_id = party_id

                # Handle customer for sales/donations out
                elif movement_type in ['sale', 'donation_out']:
                    # Create form-like dictionary for this line's customer data
                    line_customer_data = {}

                    # Add customer ID if available
                    if idx < len(customer_ids) and customer_ids[idx]:
                        line_customer_data['customer_id'] = customer_ids[idx]
                        line_customer_data['customer_id[]'] = customer_ids[idx]

                    # Add customer name if available
                    if idx < len(customer_names) and customer_names[idx]:
                        line_customer_data['customer_name'] = customer_names[idx]
                        line_customer_data['customer_name[]'] = customer_names[idx]

                    # Handle party logic using your function
                    if line_customer_data:  # Only call if we have data
                        party_id, party_name = handle_party_logic(
                            form_data=line_customer_data,
                            app_id=app_id,
                            db_session=db_session,
                            party_type="Client"
                        )
                        line_party_id = party_id

                # If still no party ID, but we have additional_data with party info
                if not line_party_id and additional_data:
                    # Check for generic party_id[] array
                    party_ids_list = additional_data.getlist('party_id[]')
                    party_names_list = additional_data.getlist('party_name[]')

                    party_data = {}
                    if idx < len(party_ids_list) and party_ids_list[idx]:
                        party_data['party_id'] = party_ids_list[idx]
                        party_data['party_id[]'] = party_ids_list[idx]

                    if idx < len(party_names_list) and party_names_list[idx]:
                        party_data['party_name'] = party_names_list[idx]
                        party_data['party_name[]'] = party_names_list[idx]

                    if party_data:
                        party_type_to_use = "Vendor" if movement_type in ['acquisition', 'donation_in'] else "Client"
                        party_id, party_name = handle_party_logic(
                            form_data=party_data,
                            app_id=app_id,
                            db_session=db_session,
                            party_type=party_type_to_use
                        )
                        line_party_id = party_id

                # Initialize asset variable
                asset = None

                # 3. Process the asset based on movement type
                if movement_type in ['acquisition', 'donation_in', 'opening_balance']:
                    # Determine the right transaction value
                    if movement_type == 'donation_in':
                        transaction_val = Decimal(current_values[idx]) if idx < len(current_values) and current_values[
                            idx] else Decimal('0')
                    else:
                        transaction_val = Decimal(purchase_prices[idx]) if idx < len(purchase_prices) and \
                                                                           purchase_prices[idx] else Decimal('0')

                    # CREATE OR UPDATE ASSET (EDIT MODE - update if exists, create if new)
                    asset = _create_asset_from_line_item_edit(
                        db_session=db_session,
                        app_id=app_id,
                        movement_type=movement_type,
                        asset_item_id=asset_item_ids[idx],
                        transaction_value=transaction_val,
                        asset_tag=asset_tags[idx] if idx < len(asset_tags) else None,
                        serial_number=serial_numbers[idx] if idx < len(serial_numbers) else None,
                        transaction_date=transaction_date,
                        project_id=project_ids[idx] if should_save_project and idx < len(project_ids) else None,
                        location_id=location_id if should_save_location else None,
                        party_id=line_party_id,
                        additional_data=additional_data,
                        idx=idx,
                        existing_asset_id=existing_asset_id  # Pass existing asset ID for edit mode
                    )

                    if existing_asset_id:
                        updated_assets.append(asset)
                    else:
                        created_assets.append(asset)

                elif movement_type in ['sale', 'donation_out', 'disposal', 'assignment', 'transfer', 'return']:
                    # UPDATE EXISTING ASSET
                    asset = _update_existing_asset_from_line_item(
                        db_session=db_session,
                        app_id=app_id,
                        movement_type=movement_type,
                        asset_id=asset_ids[idx] if idx < len(asset_ids) and asset_ids[idx] else None,
                        transaction_date=transaction_date,
                        additional_data=additional_data,
                        idx=idx
                    )
                    updated_assets.append(asset)

                # Handle depreciation
                elif movement_type == 'depreciation':
                    # UPDATE EXISTING ASSET WITH DEPRECIATION
                    if idx < len(asset_ids) and asset_ids[idx]:
                        asset_id_val = int(asset_ids[idx]) if str(asset_ids[idx]).isdigit() else None
                        if asset_id_val:
                            asset = db_session.query(Asset).filter_by(id=asset_id_val, app_id=app_id).first()

                            if asset:
                                # Get depreciation data
                                dep_amount = Decimal(depreciation_amounts[idx]) if idx < len(depreciation_amounts) and \
                                                                                   depreciation_amounts[
                                                                                       idx] else Decimal('0')
                                dep_notes = depreciation_notes[idx] if idx < len(depreciation_notes) else None

                                # Update asset's current value
                                if dep_amount > 0:
                                    previous_value = asset.current_value
                                    new_value = max(Decimal('0'), asset.current_value - dep_amount)

                                    # Update asset
                                    asset.current_value = new_value
                                    asset.last_depreciation_date = transaction_date

                                    # CREATE DEPRECIATION RECORD (EDIT MODE)
                                    depreciation_record = DepreciationRecord(
                                        app_id=app_id,
                                        asset_id=asset.id,
                                        asset_movement_line_item_id=None,  # Will set after line item is created
                                        depreciation_date=transaction_date,
                                        depreciation_amount=dep_amount,
                                        previous_value=previous_value,
                                        new_value=new_value,
                                        notes=dep_notes,
                                        created_by=current_user_id,
                                        created_at=func.now()
                                    )
                                    db_session.add(depreciation_record)
                                    db_session.flush()

                                    updated_assets.append(asset)

                # 4. Create line item with NEW MODEL STRUCTURE
                # Determine transaction value
                if movement_type == 'depreciation' and asset:
                    transaction_value = Decimal(depreciation_amounts[idx]) if idx < len(depreciation_amounts) and \
                                                                              depreciation_amounts[idx] else Decimal(
                        '0')
                elif movement_type in ['sale', 'donation_out', 'disposal']:
                    transaction_value = Decimal(sale_prices[idx]) if idx < len(sale_prices) and sale_prices[
                        idx] else Decimal('0')
                else:
                    transaction_value = Decimal(purchase_prices[idx]) if idx < len(purchase_prices) and purchase_prices[
                        idx] else Decimal('0')

                # Build line notes - add a separator if multiple notes exist
                line_notes_parts = []

                if movement_type == 'depreciation' and idx < len(depreciation_notes) and depreciation_notes[idx]:
                    line_notes_parts.append(depreciation_notes[idx])

                if movement_type == 'sale' and idx < len(sale_notes) and sale_notes[idx]:
                    line_notes_parts.append(sale_notes[idx])

                if movement_type == 'transfer' and transfer_reasons and idx < len(transfer_reasons) and \
                        transfer_reasons[idx]:
                    line_notes_parts.append(transfer_reasons[idx])

                if movement_type == 'assignment' and additional_data:
                    assignment_notes = additional_data.getlist('assignment_notes[]')
                    if idx < len(assignment_notes) and assignment_notes[idx]:
                        line_notes_parts.append(assignment_notes[idx])

                # Join with separator if multiple notes
                line_notes = " | ".join(line_notes_parts) if line_notes_parts else ""

                # Create the line item - USE EXISTING ASSET MOVEMENT ID
                # For out movements, project_id should be None
                line_item_project_id = None
                if should_save_project and project_ids and idx < len(project_ids) and project_ids[idx]:
                    line_item_project_id = project_ids[idx]

                line_item = AssetMovementLineItem(
                    asset_movement_id=asset_movement.id,  # Use existing movement ID
                    app_id=app_id,
                    asset_id=asset.id if asset else None,
                    party_id=line_party_id,
                    transaction_value=transaction_value,
                    line_notes=line_notes if line_notes else '',
                    project_id=line_item_project_id,
                )

                # Set location/department fields based on movement type
                # Skip for out movements since no location/department needed
                if movement_type not in out_movements:
                    if movement_type in ['transfer', 'assignment', 'return'] and asset:
                        # Get current location/department FROM THE ASSET
                        if asset.location_id:
                            line_item.from_location_id = asset.location_id
                        if asset.department_id:
                            line_item.from_department_id = asset.department_id

                        # Get TO locations and departments from form
                        prefix = f'line_{idx}_' if idx > 0 else ''

                        if movement_type == 'transfer':
                            to_location_id = get_form_value(additional_data, f'{prefix}to_location_id[]',
                                                            'to_location_id[]', idx=idx)
                            to_department_id = get_form_value(additional_data, f'{prefix}to_department_id[]',
                                                              'to_department_id[]', idx=idx)

                            if to_location_id and str(to_location_id).isdigit():
                                line_item.to_location_id = int(to_location_id)
                            if to_department_id and str(to_department_id).isdigit():
                                line_item.to_department_id = int(to_department_id)

                        elif movement_type == 'assignment':
                            assigned_to_id = get_form_value(additional_data, f'{prefix}assigned_to_id[]',
                                                            'assigned_to_id[]', idx=idx)
                            to_department_id = get_form_value(additional_data, f'{prefix}to_department_id[]',
                                                              'to_department_id[]', idx=idx)

                            if assigned_to_id and str(assigned_to_id).isdigit():
                                line_item.assigned_to_id = int(assigned_to_id)
                            if to_department_id and str(to_department_id).isdigit():
                                line_item.to_department_id = int(to_department_id)

                        elif movement_type == 'return':
                            # For returns, to_location/to_department might be a storage location
                            to_location_id = get_form_value(additional_data, f'{prefix}to_location_id[]',
                                                            'to_location_id[]', idx=idx)
                            to_department_id = get_form_value(additional_data, f'{prefix}to_department_id[]',
                                                              'to_department_id[]', idx=idx)

                            if to_location_id and str(to_location_id).isdigit():
                                line_item.to_location_id = int(to_location_id)
                            if to_department_id and str(to_department_id).isdigit():
                                line_item.to_department_id = int(to_department_id)

                # LINK DEPRECIATION RECORD TO LINE ITEM (EDIT MODE)
                if movement_type == 'depreciation' and asset:
                    # Find the depreciation record we just created
                    depreciation_record = db_session.query(DepreciationRecord).filter_by(
                        app_id=app_id,
                        asset_id=asset.id,
                        depreciation_date=transaction_date,
                        depreciation_amount=Decimal(depreciation_amounts[idx]) if idx < len(depreciation_amounts) and
                                                                                  depreciation_amounts[
                                                                                      idx] else Decimal('0')
                    ).order_by(DepreciationRecord.created_at.desc()).first()

                    if depreciation_record:
                        depreciation_record.asset_movement_line_item_id = line_item.id
                        logger.info(f"Linked depreciation record {depreciation_record.id} to line item {line_item.id}")

                db_session.add(line_item)
                db_session.flush()

            except IntegrityError as e:
                # Rollback the current transaction
                db_session.rollback()

                # Parse the error message to find which constraint failed
                error_msg = str(e.orig)

                # Check for asset_tag constraint
                if 'asset.asset_tag' in error_msg or 'UNIQUE constraint failed: asset.asset_tag' in error_msg:
                    asset_tag_value = asset_tags[idx] if idx < len(asset_tags) else "N/A"

                    # In edit mode, check if this tag belongs to the SAME asset
                    if existing_asset_id:
                        # Check if this tag is used by a DIFFERENT asset
                        existing_asset = db_session.query(Asset).filter(
                            Asset.asset_tag == asset_tag_value,
                            Asset.id != existing_asset_id,
                            Asset.app_id == app_id
                        ).first()

                        if existing_asset:
                            raise ValueError(
                                f"Asset tag '{asset_tag_value}' is already in use by another asset. Please use a unique asset tag.")
                        else:
                            # Tag belongs to current asset - this is fine
                            logger.info(
                                f"Asset tag '{asset_tag_value}' belongs to current asset {existing_asset_id}, skipping uniqueness check")
                            # Continue with the operation - don't raise error
                            continue
                    else:
                        raise ValueError(
                            f"Asset tag '{asset_tag_value}' is already in use. Please use a unique asset tag.")

                # Check for serial_number constraint
                elif 'asset.serial_number' in error_msg or 'UNIQUE constraint failed: asset.serial_number' in error_msg:
                    serial_value = serial_numbers[idx] if idx < len(serial_numbers) else "N/A"

                    # In edit mode, check if this serial belongs to the SAME asset
                    # Only check uniqueness if serial_number has a value (not None, not empty string, not just whitespace)
                    if existing_asset_id and serial_value and serial_value.strip():
                        # Check if this serial is used by a DIFFERENT asset
                        existing_asset = db_session.query(Asset).filter(
                            Asset.serial_number == serial_value,
                            Asset.id != existing_asset_id,
                            Asset.app_id == app_id
                        ).first()

                        if existing_asset:
                            raise ValueError(
                                f"Serial number '{serial_value}' is already in use by another asset. Please use a unique serial number.")
                        else:
                            # Serial belongs to current asset - this is fine
                            logger.info(
                                f"Serial number '{serial_value}' belongs to current asset {existing_asset_id}, skipping uniqueness check")
                            continue
                    else:
                        # If no existing_asset_id, check for duplicates normally
                        existing_asset = db_session.query(Asset).filter(
                            Asset.serial_number == serial_value,
                            Asset.app_id == app_id
                        ).first()
                        if existing_asset:
                            raise ValueError(
                                f"Serial number '{serial_value}' is already in use. Please use a unique serial number.")

                # Generic integrity error
                else:
                    # Try to extract more details
                    match = re.search(r'UNIQUE constraint failed: (\w+\.\w+)', error_msg)
                    if match:
                        constraint = match.group(1)
                        raise ValueError(f"Duplicate value violation: {constraint}")
                    else:
                        raise ValueError(f"Database constraint violation: {error_msg}")

            except Exception as e:
                logger.error(f"Error processing asset line item {idx}: {str(e)}")
                raise ValueError(f"Error processing asset {idx + 1}: {str(e)}")

        db_session.flush()

        # Update the asset movement header with new values
        asset_movement.transaction_date = transaction_date
        asset_movement.reference = reference
        asset_movement.location_id = location_id if should_save_location else None
        asset_movement.updated_at = func.now()

        db_session.flush()

        return {
            'asset_movement': asset_movement,
            'created_count': len(created_assets),
            'updated_count': len(updated_assets),
            'created_assets': created_assets,
            'updated_assets': updated_assets,
            'reference': reference,
            'movement_type': movement_type
        }

    except Exception as e:
        logger.error(f"Error in process_edit_asset_entries: {str(e)}\n{traceback.format_exc()}")
        raise


def _create_asset_from_line_item_edit(db_session, app_id, movement_type, asset_item_id, transaction_value,
                                      asset_tag, serial_number, transaction_date, project_id, location_id, party_id,
                                      additional_data, idx, existing_asset_id=None):
    """
    Create a new Asset or UPDATE existing asset for EDIT mode
    """

    # Generate asset tag if not provided and no existing asset
    if not asset_tag and not existing_asset_id:
        asset_tag = f"AST-{int(time.time())}-{asset_item_id}-{idx}"

    # Get additional data for this specific line
    prefix = f'line_{idx}_' if idx > 0 else ''

    # Get depreciation overrides WITH INDEX
    useful_life_years = get_form_value(additional_data, f'{prefix}useful_life_years[]', 'useful_life_years[]', idx=idx)
    depreciation_method = get_form_value(additional_data, f'{prefix}depreciation_method[]', 'depreciation_method[]',
                                         idx=idx)
    capitalization_date_str = get_form_value(additional_data, f'{prefix}capitalization_date[]', 'capitalization_date[]',
                                             idx=idx)

    # Get warranty info WITH INDEX
    warranty_months = get_form_value(additional_data, f'{prefix}warranty_months[]', 'warranty_months[]', idx=idx)
    warranty_expiry_str = get_form_value(additional_data, f'{prefix}warranty_expiry[]', 'warranty_expiry[]', idx=idx)

    # Get condition WITH INDEX
    condition = get_form_value(additional_data, f'{prefix}condition[]', 'condition[]', 'good', idx=idx)

    # Get current value from form - WITH INDEX
    current_value_str = get_form_value(additional_data, f'{prefix}current_value[]', 'current_value[]', idx=idx)

    # Parse dates
    capitalization_date = parse_date(capitalization_date_str) if capitalization_date_str else parse_date(
        transaction_date)
    warranty_expiry = parse_date(warranty_expiry_str) or calculate_warranty_expiry(warranty_months, transaction_date)

    # Get asset item for defaults
    asset_item = db_session.query(AssetItem).filter_by(id=asset_item_id, app_id=app_id).first()
    if not asset_item:
        raise ValueError(f"AssetItem with ID {asset_item_id} not found")

    # Determine purchase price based on movement type
    if movement_type == 'donation_in':
        purchase_price = Decimal('0')
    else:
        purchase_price = transaction_value

    # Determine current value - use from form if available, otherwise use transaction value
    if current_value_str:
        try:
            current_value = Decimal(current_value_str)
        except:
            current_value = transaction_value
    else:
        current_value = transaction_value

    # If we have an existing asset ID, UPDATE that asset
    if existing_asset_id:
        asset = db_session.query(Asset).filter_by(id=existing_asset_id, app_id=app_id).first()
        if asset:
            # Update existing asset
            asset.asset_item_id = asset_item_id
            if asset_tag:  # Only update if provided
                asset.asset_tag = asset_tag
            if serial_number is not None:  # Allow empty to clear
                asset.serial_number = serial_number
            asset.condition = condition
            asset.purchase_price = purchase_price
            asset.current_value = current_value
            asset.purchase_date = transaction_date if movement_type in ['acquisition',
                                                                        'donation_in'] else asset.purchase_date
            asset.supplier_id = party_id if movement_type in ['acquisition', 'donation_in'] else asset.supplier_id
            asset.project_id = project_id
            asset.capitalization_date = capitalization_date
            asset.useful_life_years = int(
                useful_life_years) if useful_life_years and str(
                useful_life_years).isdigit() else asset_item.expected_useful_life_years
            asset.depreciation_method = depreciation_method if depreciation_method else asset_item.depreciation_method
            asset.warranty_expiry = warranty_expiry
            asset.updated_at = func.now()
            asset.location_id = location_id

            if movement_type == 'opening_balance':
                asset.purchase_date = None

            db_session.flush()
            return asset
        else:
            logger.warning(f"Existing asset {existing_asset_id} not found, creating new asset")

    # Create new asset (only if no existing asset ID or asset not found)
    asset = Asset(
        app_id=app_id,
        asset_item_id=asset_item_id,
        asset_tag=asset_tag,
        serial_number=serial_number,
        status='in_stock',
        condition=condition,
        purchase_price=purchase_price,
        current_value=current_value,
        purchase_date=transaction_date if movement_type in ['acquisition', 'donation_in'] else None,
        supplier_id=party_id if movement_type in ['acquisition', 'donation_in'] else None,
        project_id=project_id,
        capitalization_date=capitalization_date,
        useful_life_years=int(
            useful_life_years) if useful_life_years and str(
            useful_life_years).isdigit() else asset_item.expected_useful_life_years,
        depreciation_method=depreciation_method if depreciation_method else asset_item.depreciation_method,
        warranty_expiry=warranty_expiry,
        created_at=func.now()
    )

    if movement_type == 'opening_balance':
        asset.purchase_date = None

    db_session.add(asset)
    db_session.flush()
    return asset
