import datetime
import logging
import os
import re
import traceback
from decimal import Decimal

from flask import current_app
from flask import render_template
from flask_login import current_user
from sqlalchemy import func, case
from sqlalchemy.orm import joinedload

from ai import get_or_create_exchange_rate_id, get_exchange_rate, get_base_currency
from db import Session
from models import Attachment
from models import InventoryLocation, Vendor, Project, InventoryItemVariationLink, \
    InventoryCategory, PaymentMode, InventorySubCategory, InventoryItemVariation, InventoryItemAttribute, Brand, \
    Currency, Company, InventoryEntry, ExchangeRate, InventoryEntryLineItem, InventorySummary, \
    InventoryTransactionDetail, ChartOfAccounts, JournalEntry, Journal, DirectSalesTransaction, UserLocationAssignment, \
    User, Employee
from services.chart_of_accounts_helpers import update_retained_earnings_opening_balance, \
    reverse_retained_earnings_balance
from utils import create_notification, empty_to_none
from utils_and_helpers.cache_utils import clear_stock_history_cache

logger = logging.getLogger(__name__)


def validate_quantity_and_price(quantity, unit_price):
    if quantity is None or quantity == "":
        raise ValueError("Quantity is empty or not provided.")
    try:
        quantity = float(quantity)
    except (ValueError, TypeError):
        raise ValueError("Quantity must be a valid number.")
    if quantity <= 0:
        raise ValueError("Quantity must be a positive number.")

    if unit_price is None or unit_price == "":
        raise ValueError("Unit Price is empty or not provided.")
    try:
        unit_price = float(unit_price)
    except (ValueError, TypeError):
        raise ValueError("Unit Price must be a valid number.")
    if unit_price <= 0:
        raise ValueError("Unit Price must be a positive number.")


def validate_quantity_and_selling_price(quantity, selling_price):
    if quantity is None or quantity == "":
        raise ValueError("Quantity is empty or not provided.")
    try:
        quantity = float(quantity)
    except (ValueError, TypeError):
        raise ValueError("Quantity must be a valid number.")
    if quantity <= 0:
        raise ValueError("Quantity must be a positive number.")

    if selling_price is None or selling_price == "":
        raise ValueError("Unit Price is empty or not provided.")
    try:
        selling_price = float(selling_price)
    except (ValueError, TypeError):
        raise ValueError("Unit Price must be a valid number.")
    if selling_price < 0:
        raise ValueError("Unit Price must be a positive number.")


def ensure_default_location(db_session, app_id):
    """
    Ensure that "Main Warehouse" exists in the InventoryLocation table for the given app_id.
    If it doesn't exist, create it.

    Returns:
        int: ID of the default location.
    """
    default_location_name = "Main Warehouse"

    try:
        # Check if "Main Warehouse" already exists for the given app_id
        main_warehouse = db_session.query(InventoryLocation).filter_by(
            location=default_location_name, app_id=app_id
        ).first()

        if not main_warehouse:
            # Create "Main Warehouse" if it doesn't exist
            main_warehouse = InventoryLocation(
                app_id=app_id,
                location=default_location_name,
                description="Main Warehouse"
            )
            db_session.add(main_warehouse)
            db_session.commit()

        return main_warehouse.id

    except Exception as e:
        db_session.rollback()  # Rollback in case of error
        return None  # Or handle the error as needed

    finally:
        db_session.close()  # Ensure the session is closed


def suggest_next_inventory_reference(db_session):
    """
    Suggest the next inventory reference based on the most recent reference entered by the user.
    - If the last reference ends with a number, increment it while preserving leading zeros.
    - Otherwise, return the last reference with '-1' appended.
    - If there is no reference at all, return a clean default like 'INVNT-REF-1'.
    """
    app_id = current_user.app_id  # Assumes current_user is available

    # Get the last entered reference
    last_entry = db_session.query(InventoryEntry.reference).filter(
        InventoryEntry.app_id == app_id,
        InventoryEntry.reference != None
    ).order_by(InventoryEntry.id.desc()).first()

    if not last_entry or not last_entry.reference:
        return "INVNT-REF-1"  # Default suggestion

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


def handle_batch_variation_update(db_session, movement_type, old_values, new_values, app_id):
    """Handle batch variation updates for all movement types"""
    if movement_type == "in":
        # Convert values to consistent types for comparison
        old_loc = str(old_values['to_location']) if old_values['to_location'] is not None else None
        new_loc = str(new_values['to_location']) if new_values['to_location'] is not None else None
        old_batch = str(old_values['lot']) if old_values['lot'] is not None else None
        new_batch = str(new_values['lot']) if new_values['lot'] is not None else None

        quantity_delta = new_values['quantity'] - old_values['quantity']
        logger.info(f'Old values are {old_values} and New values are {new_values}')

        # Check if there are any relevant changes
        has_changes = (
                quantity_delta != 0 or
                old_batch != new_batch or
                old_loc != new_loc
        )

        if has_changes:
            logger.info(f'Has changes with changes {has_changes}')
            # Check if batch/location changed
            same_batch_location = (old_batch == new_batch and old_loc == new_loc)

            if same_batch_location:
                # For same batch/location, calculate net effect first
                current_qty = db_session.query(BatchVariationLink.quantity).filter_by(
                    batch_id=old_values['lot'],
                    location_id=old_values['to_location'],
                    item_id=old_values['item_id'],
                    app_id=app_id
                ).scalar()

                logger.info(f'Current quantity {current_qty}')

                if current_qty is None:
                    raise ValueError("Original batch/location not found")

                net_effect = current_qty - old_values['quantity'] + new_values['quantity']
                if net_effect < 0:
                    raise ValueError(f"Editing would leave negative inventory ({net_effect})")

                # Apply net change directly
                update_batch_link(
                    db_session=db_session,
                    batch_id=old_values['lot'],
                    location_id=old_values['to_location'],
                    item_id=old_values['item_id'],
                    adjustment=quantity_delta,  # Net change
                    app_id=app_id,
                    create_if_missing=False
                )

                logger.info(f'Applied update batch and net effect is {net_effect}')
            else:
                # For different batch/location, use transaction
                with db_session.begin_nested():
                    # Temporarily disable negative check for first operation
                    update_batch_link(
                        db_session=db_session,
                        batch_id=old_values['lot'],
                        location_id=old_values['to_location'],
                        item_id=old_values['item_id'],
                        adjustment=-old_values['quantity'],
                        app_id=app_id,
                        create_if_missing=False,
                        allow_negative=True  # Temporary
                    )

                    update_batch_link(
                        db_session=db_session,
                        batch_id=new_values['lot'],
                        location_id=new_values['to_location'],
                        item_id=new_values['item_id'],
                        adjustment=new_values['quantity'],
                        unit_price=new_values.get('unit_price'),
                        supplier_id=new_values.get('supplier_id'),
                        expiration_date=new_values.get('expiration_date'),
                        transaction_date=new_values.get('transaction_date'),
                        app_id=app_id,
                        create_if_missing=True
                    )


        else:
            # No quantity/location/batch changes, but might need to update other fields
            logger.info("No quantity, batch or location changes detected for 'in' movement")

            # Update metadata fields if they exist in the batch link
            update_batch_link(
                db_session=db_session,
                batch_id=old_values['lot'],
                location_id=old_values['to_location'],
                item_id=old_values['item_id'],
                adjustment=0,  # No quantity change
                unit_price=new_values.get('unit_price'),
                currency_id=new_values.get('currency_id'),
                supplier_id=new_values.get('supplier_id'),
                expiration_date=new_values.get('expiration_date'),
                transaction_date=new_values.get('transaction_date'),
                app_id=app_id,
                create_if_missing=False
            )

    elif movement_type in ["out", "missing"]:
        # Check if item, batch or location changed
        item_changed = old_values['item_id'] != new_values['item_id']
        batch_changed = old_values['lot'] != new_values['lot']
        location_changed = old_values['from_location'] != new_values['from_location']
        logger.info(f'Batch changed is {batch_changed} and location is {location_changed}')
        if item_changed or batch_changed or location_changed:
            # FIRST VALIDATE THE NEW LOCATION/BATCH HAS ENOUGH STOCK
            new_batch_link = db_session.query(BatchVariationLink).filter_by(
                batch_id=new_values['lot'],
                location_id=new_values['from_location'],
                item_id=new_values['item_id'],
                app_id=app_id
            ).first()

            if not new_batch_link or new_batch_link.quantity < new_values['quantity']:
                raise ValueError(
                    f"Insufficient quantity in new batch/location. "
                    f"Available: {new_batch_link.quantity if new_batch_link else 0}, "
                    f"Requested: {new_values['quantity']}"
                )

            # If validation passes, proceed with the update
            # First reverse the original missing entry
            update_batch_link(
                db_session=db_session,
                batch_id=old_values['lot'],
                location_id=old_values['from_location'],
                item_id=old_values['item_id'],
                adjustment=old_values['quantity'],  # Add back
                app_id=app_id,
                create_if_missing=False
            )

            # Then apply the new missing entry
            update_batch_link(
                db_session=db_session,
                batch_id=new_values['lot'],
                location_id=new_values['from_location'],
                item_id=new_values['item_id'],
                adjustment=-new_values['quantity'],  # Subtract
                unit_price=new_values['unit_price'],
                currency_id=new_values['currency_id'],
                supplier_id=new_values['supplier_id'],
                expiration_date=new_values['expiration_date'],
                transaction_date=new_values.get('transaction_date'),
                app_id=app_id,
                create_if_missing=True
            )

        else:  # Only quantity changed
            current_link = db_session.query(BatchVariationLink).filter_by(
                batch_id=old_values['lot'],
                location_id=old_values['from_location'],
                item_id=old_values['item_id'],
                app_id=app_id
            ).first()

            if not current_link:
                raise ValueError("Original batch/location not found")

            available = current_link.quantity + old_values['quantity']
            if new_values['quantity'] > available:
                raise ValueError(
                    f"Cannot adjust. Available: {available}, "
                    f"Requested: {new_values['quantity']}"
                )

            # Apply net adjustment
            update_batch_link(
                db_session=db_session,
                batch_id=old_values['lot'],
                location_id=old_values['from_location'],
                item_id=old_values['item_id'],
                adjustment=(old_values['quantity'] - new_values['quantity']),
                transaction_date=new_values.get('transaction_date'),
                app_id=app_id,
                create_if_missing=False
            )

    elif movement_type == "transfer":
        # For transfers, adjust both source and destination
        # First reverse the original transfer
        update_batch_link(
            db_session=db_session,
            batch_id=old_values['lot'],
            location_id=old_values['from_location'],
            item_id=old_values['item_id'],
            adjustment=old_values['quantity'],
            transaction_date=new_values.get('transaction_date'),
            app_id=app_id,
            create_if_missing=False
        )
        update_batch_link(
            db_session=db_session,
            batch_id=old_values['lot'],
            location_id=old_values['to_location'],
            item_id=old_values['item_id'],
            adjustment=-old_values['quantity'],
            transaction_date=new_values.get('transaction_date'),
            app_id=app_id,
            create_if_missing=False
        )

        # Then apply the new transfer
        update_batch_link(
            db_session=db_session,
            batch_id=new_values['lot'],
            location_id=new_values['from_location'],
            item_id=new_values['item_id'],
            adjustment=-new_values['quantity'],
            transaction_date=new_values.get('transaction_date'),
            app_id=app_id,
            create_if_missing=False
        )
        update_batch_link(
            db_session=db_session,
            batch_id=new_values['lot'],
            location_id=new_values['to_location'],
            item_id=new_values['item_id'],
            adjustment=new_values['quantity'],
            unit_price=new_values['unit_price'],
            currency_id=new_values['currency_id'],
            supplier_id=new_values['supplier_id'],
            expiration_date=new_values['expiration_date'],
            transaction_date=new_values.get('transaction_date'),
            app_id=app_id,
            create_if_missing=True
        )


def update_batch_link(db_session, batch_id, location_id, item_id, adjustment, app_id,
                      unit_price=None, currency_id=None, supplier_id=None,
                      expiration_date=None, transaction_date=None, create_if_missing=False, allow_negative=False):
    """Update or create a batch variation link with the given adjustment"""
    if not location_id:
        return

    link = db_session.query(BatchVariationLink).filter_by(
        batch_id=batch_id,
        location_id=location_id,
        item_id=item_id,
        app_id=app_id
    ).first()

    if not link and create_if_missing:
        link = BatchVariationLink(
            batch_id=batch_id,
            location_id=location_id,
            item_id=item_id,
            quantity=adjustment,  # Start with the adjustment value
            unit_cost=unit_price if unit_price is not None else 0,
            currency_id=currency_id if currency_id is not None else 1,  # Default currency
            supplier_id=supplier_id,
            expiry_date=expiration_date,
            transaction_date=transaction_date if transaction_date else datetime.datetime.now(),
            app_id=app_id
        )
        db_session.add(link)
    elif not link:
        raise ValueError(f"No inventory found for batch {batch_id} at location {location_id}")
    else:
        link.quantity += adjustment

    # Update other fields if provided
    if unit_price is not None:
        link.unit_cost = unit_price
    if currency_id is not None:
        link.currency_id = currency_id

    if expiration_date is not None:
        link.expiry_date = expiration_date
    if supplier_id is not None:
        link.supplier_id = supplier_id
    if transaction_date is not None:
        link.transaction_date = transaction_date

    if not allow_negative and link.quantity < 0:
        raise ValueError("Adjustment would result in negative inventory")


# Helper functions
def handle_supplier_logic(form_data, app_id, db_session):
    """Handle supplier logic for both existing and one-time suppliers"""
    supplier_id_str = form_data.get('supplier_id')
    supplier_id = empty_to_none(supplier_id_str)
    if not supplier_id or supplier_id.strip() == "":
        return None

    if supplier_id.isdigit():
        return int(supplier_id)

    # One-time supplier
    supplier_name = supplier_id.strip()
    existing_supplier = db_session.query(Vendor).filter_by(
        vendor_name=supplier_name,
        is_one_time=True,
        app_id=app_id
    ).first()

    if existing_supplier:
        return existing_supplier.id
    else:
        new_supplier = Vendor(
            vendor_name=supplier_name,
            is_one_time=True,
            vendor_type="Vendor",
            app_id=app_id
        )
        db_session.add(new_supplier)
        db_session.flush()  # Flush to get the ID without committing
        return new_supplier.id


def validate_inventory_item(db_session, app_id, item_id, index):
    """Validate that inventory item exists"""
    inventory_item_variation_link = db_session.query(InventoryItemVariationLink).filter_by(
        app_id=app_id, id=item_id).first()

    if not inventory_item_variation_link:
        raise ValueError(f"Inventory item {item_id} not found at index {index}")

    return inventory_item_variation_link


def convert_and_validate_values(quantity, unit_price, selling_price, index, movement_type):
    """Convert and validate numeric values"""
    try:
        quantity_dec = Decimal(quantity) if quantity else Decimal('0.0')
        unit_price_dec = Decimal(unit_price) if unit_price else Decimal('0.0')
        selling_price_dec = Decimal(selling_price) if selling_price else Decimal('0.0')
    except Exception as e:
        raise ValueError(f"Invalid numeric value in item {index}: {str(e)}")

    if quantity_dec <= 0:
        raise ValueError(f"Quantity must be positive for item {index}")

    # For adjustments, unit price can be zero
    if movement_type != 'adjustment' and unit_price_dec < 0:
        raise ValueError(f"Unit price cannot be negative for item {index}")

    return quantity_dec, unit_price_dec, selling_price_dec


def render_inventory_entry_form(db_session, app_id, company, role, modules_data, movement_type):
    """Render the inventory entry form with all required data"""
    projects = db_session.query(Project).filter_by(app_id=app_id).all()
    inventory_items = db_session.query(InventoryItemVariationLink).filter_by(app_id=app_id, status="active").all()
    payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
    categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
    subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
    variations = db_session.query(InventoryItemVariation).filter_by(app_id=app_id).all()
    attributes = db_session.query(InventoryItemAttribute).filter_by(app_id=app_id).all()

    # FROM Locations - Only locations user has access to (for sending stock)
    from_locations = get_user_accessible_locations(current_user.id, app_id)

    # TO Locations - ALL locations (for receiving stock - no permission needed)
    all_locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

    brands = db_session.query(Brand).filter_by(app_id=app_id).all()
    employees = db_session.query(Employee).filter_by(app_id=app_id).all()

    # GET request - render edit form
    vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers', 'customer', 'customers']
    vendor_types = [v.lower() for v in vendor_types]

    suppliers = (
        db_session.query(Vendor)
        .filter(
            Vendor.app_id == app_id,
            func.lower(Vendor.vendor_type).in_(vendor_types)
        )
        .all()
    )

    currencies = db_session.query(Currency).filter_by(app_id=app_id).order_by(Currency.currency_index).all()

    return render_template(
        '/inventory/inventory_entry.html',
        company=company,
        role=role,
        modules=modules_data,
        projects=projects,
        inventory_items=inventory_items,
        categories=categories,
        subcategories=subcategories,
        inventory_variations=variations,
        inventory_attributes=attributes,
        from_locations=from_locations,  # For FROM dropdown (user has access)
        all_locations=all_locations,  # For TO dropdown (all locations)
        brands=brands,
        movement_type=movement_type,
        suppliers=suppliers,
        employees=employees,
        currencies=currencies,
        payment_modes=payment_modes,
        title="Inventory Entry"
    )


def get_inventory_valuation_for_entry(db_session, app_id, item_id, location_id, movement_type, quantity=None):
    """
    Get appropriate inventory valuation for inventory entry based on company's method
    Returns: (unit_cost, currency_id, batch_id, existing_batch_found, error_message)
    """
    try:
        # Get company's inventory valuation method
        company = db_session.query(Company).filter_by(id=app_id).first()
        if not company:
            return None, None, None, False, "Company not found"

        valuation_method = company.inventory_valuation_method or 'average_weighted'

        # Check if item exists
        item = db_session.query(InventoryItemVariationLink).filter_by(
            id=item_id, app_id=app_id
        ).first()

        if not item:
            return None, None, None, False, f"Inventory item {item_id} not found"

        # For certain movement types, we need to find existing batches
        existing_batch_found = False
        batch_id = None
        unit_cost = Decimal('0.0')
        currency_id = None

        # Movement types that should use existing batches
        use_existing_batch_movements = ['adjustment', 'missing', 'transfer']

        if movement_type in use_existing_batch_movements:
            # Find existing batches for this item at this location
            existing_batches = db_session.query(BatchVariationLink).filter(
                BatchVariationLink.app_id == app_id,
                BatchVariationLink.item_id == item_id,
                BatchVariationLink.location_id == location_id,
                BatchVariationLink.quantity > 0
            ).order_by(BatchVariationLink.transaction_date.asc()).all()

            if not existing_batches:
                # No existing batches found - this is a new item entry error for adjustment/missing
                if movement_type in ['adjustment', 'missing']:
                    return None, None, None, False, f"Cannot {movement_type} item '{item.inventory_item.item_name}' - no existing inventory found at this location"
                # For transfers/out, we might allow creating new batches
                return None, None, None, False, "No existing inventory found"

            existing_batch_found = True

            if valuation_method == 'fifo':
                # Use FIFO - get the oldest batch
                oldest_batch = existing_batches[0]
                batch_id = oldest_batch.batch_id
                unit_cost = oldest_batch.unit_cost
                currency_id = oldest_batch.currency_id

            elif valuation_method == 'average_weighted':
                # Calculate weighted average cost
                total_value = Decimal('0.0')
                total_quantity = Decimal('0.0')

                for batch in existing_batches:
                    batch_qty = Decimal(str(batch.quantity))
                    batch_value = batch.unit_cost * batch_qty
                    total_value += batch_value
                    total_quantity += batch_qty

                if total_quantity > 0:
                    unit_cost = total_value / total_quantity
                    # Use the most recent batch for currency reference
                    latest_batch = max(existing_batches, key=lambda x: x.transaction_date)
                    currency_id = latest_batch.currency_id
                    batch_id = latest_batch.batch_id  # Use latest batch ID for reference
                else:
                    return None, None, None, False, "No quantity available for weighted average calculation"

            else:
                return None, None, None, False, f"Unknown inventory valuation method: {valuation_method}"

        else:
            # For 'in' movements or other types, we don't need existing batches
            # These will typically get their cost from the form input
            existing_batch_found = False

        return unit_cost, currency_id, batch_id, existing_batch_found, None

    except Exception as e:
        logger.error(f"Error in inventory valuation: {str(e)}", exc_info=True)
        return None, None, None, False, f"Error calculating inventory valuation: {str(e)}"


def handle_inventory_valuation(db_session, app_id, item_id, location_id, movement_type,
                               form_unit_cost=None, form_currency_id=None, quantity=None):
    """
    Comprehensive function to handle inventory valuation for inventory entries
    Returns: (unit_cost, currency_id, batch_id, error_message)
    """
    try:
        # Get valuation based on company method
        unit_cost, currency_id, batch_id, existing_batch_found, error = get_inventory_valuation_for_entry(
            db_session, app_id, item_id, location_id, movement_type, quantity
        )

        if error:
            return None, None, None, error

        # For movements that should use existing valuation, override form values
        use_existing_valuation = ['adjustment', 'missing', 'transfer', 'out']

        if movement_type in use_existing_valuation and existing_batch_found:
            # Use calculated values instead of form values
            return unit_cost, currency_id, batch_id, None

        elif movement_type in use_existing_valuation and not existing_batch_found:
            # This shouldn't happen if get_inventory_valuation_for_entry worked correctly
            return None, None, None, "No existing inventory found for valuation"

        else:
            # For 'in' movements or other types, use form values
            # Validate form values
            if form_unit_cost is None or form_currency_id is None:
                return None, None, None, "Unit cost and currency are required for this movement type"

            try:
                unit_cost = Decimal(str(form_unit_cost))
                if unit_cost < 0:
                    return None, None, None, "Unit cost cannot be negative"

                return unit_cost, form_currency_id, batch_id, None

            except (ValueError, TypeError):
                return None, None, None, "Invalid unit cost format"

    except Exception as e:
        logger.error(f"Error handling inventory valuation: {str(e)}", exc_info=True)
        return None, None, None, f"Error handling inventory valuation: {str(e)}"


def update_inventory_summary(db_session, app_id, item_id, location_id, quantity_change, value_change, source_type=None,
                             item_name=None):
    """
    Update or create inventory summary record using weighted average cost method.

    This is the CORE inventory valuation function that maintains inventory balances
    and calculates average costs for COGS and financial reporting.

    ⚠️  CRITICAL: This function handles real-world scenarios including negative inventory,
    data entry corrections, and timing differences between physical and system inventory.

    Parameters:
    -----------
    db_session : Session
        Database session for queries and updates

    app_id : int
        Company/application identifier (links to companies table)

    item_id : int
        Inventory item variation ID (links to inventory_item_variation_link.id)
        ⚠️ NOTE: This is NOT InventoryItem.id but the variation-specific ID

    location_id : int
        Inventory location ID (links to inventory_location.id)

    quantity_change : float
        Quantity change (positive for additions, negative for reductions)
        Examples: +10 (purchase), -5 (sale), -1 (adjustment)

    value_change : float
        Total value change in base currency (positive for additions, negative for reductions)
        Examples: +1000.00 (purchase), -500.00 (sale), -50.00 (write-off)

    source_type : str, optional
        Transaction type for special handling:
        - 'donation': Zero-value transactions allowed
        - None: Normal inventory movement

    Business Logic:
    ---------------
    1. VALIDATION: Ensures mathematically possible scenarios
       - Positive quantity cannot have negative value
       - Negative quantity cannot have positive value
       - Zero quantity cannot have non-zero value

    2. ZERO-VALUE HANDLING: For donations or missing cost data
       - Donations: Explicitly allowed with zero cost
       - Other cases: Uses current average cost or last purchase price

    3. NEGATIVE INVENTORY SUPPORT: Handles real-world scenarios
       - Sales before stock arrival (backorders)
       - Timing differences in physical vs system inventory
       - Maintains consistent cost valuation for negative quantities

    4. WEIGHTED AVERAGE CALCULATION:
       - New Avg Cost = (Current Value + Value Change) / (Current Quantity + Quantity Change)
       - Handles edge cases with partial or missing data

    5. DATA INTEGRITY PROTECTION:
       - Detects negative average costs (mathematical impossibilities)
       - Emergency correction: Uses absolute value to maintain system operation

    Special Notes:
    --------------
    - Negative average costs indicate data integrity issues from:
      * Edit operations reversing historical transactions
      * Quantity/unit price data entry errors
      * Timing mismatches in inventory movements
    - The emergency correction (abs()) is pragmatic for production systems
      where business continuity outweighs accounting perfection
    - All corrections are logged and should be reviewed periodically

    Returns:
    --------
    None: Updates InventorySummary record in database

    Raises:
    -------
    ValueError: For validation errors and data integrity issues
    Exception: For unexpected database or system errors

    Example Usage:
    --------------
    # Purchase of 10 items costing 1000 total
    update_inventory_summary(db, 1, 123, 1, 10.0, 1000.0)

    # Sale of 5 items (value calculated automatically)
    update_inventory_summary(db, 1, 123, 1, -5.0, -500.0)

    # Donation received
    update_inventory_summary(db, 1, 123, 1, 5.0, 0.0, 'donation')

    # Edit reversal scenario (common source of negative avg cost)
    update_inventory_summary(db, 1, 123, 1, -200.0, -18400.0)
    """

    try:
        # Convert to Decimal for precise calculations
        quantity_change_dec = Decimal(str(quantity_change))
        value_change_dec = Decimal(str(value_change))

        # 🆕 VALIDATION: Prevent impossible scenarios
        if quantity_change_dec > 0 and value_change_dec < 0:
            raise ValueError("Positive quantity cannot have negative value")
        if quantity_change_dec < 0 < value_change_dec:
            raise ValueError("Negative quantity cannot have positive value")
        if quantity_change_dec == 0 and value_change_dec != 0:
            raise ValueError("Zero quantity cannot have non-zero value")

        if quantity_change_dec == 0 and value_change_dec == 0:
            logger.info("No inventory change detected; skipping update.")
            return

        # Find existing summary record
        summary = db_session.query(InventorySummary).filter_by(
            app_id=app_id,
            item_id=item_id,
            location_id=location_id
        ).first()

        # Handle zero-value transactions
        if quantity_change_dec != 0 and value_change_dec == 0:
            # Handle zero-value transactions
            if quantity_change_dec != 0 and value_change_dec == 0:
                if source_type == "donation":
                    # ✅ Allow donations with zero cost
                    # Use zero cost explicitly
                    value_change_dec = Decimal('0')
                elif summary:
                    current_avg_cost = Decimal(str(summary.average_cost)) if summary.average_cost else Decimal('0')
                    if current_avg_cost > 0:
                        value_change_dec = current_avg_cost * quantity_change_dec
                    else:
                        value_change_dec = 0

                else:
                    last_price = get_last_purchase_price(db_session, app_id, item_id)
                    if last_price and last_price > 0:
                        value_change_dec = last_price * quantity_change_dec
                    else:
                        # raise ValueError(
                        #     "No average cost or last purchase price found for item; "
                        #     "cannot update inventory summary (except for donations)."
                        # )

                        message = (
                            f'Item {item_name} has no cost recorded yet. Inventory movement recorded with zero value.'
                            f'"COGS will be calculated when a purchase price is entered.')

                        create_notification(
                            db_session=db_session,
                            user_id=None,
                            company_id=app_id,
                            message=message,
                            type='info',
                            is_popup=True,
                            url=f'/inventory/stock_list_list'  # Link to item detail
                        )

                        logger.warning(
                            f"No cost information for item {item_name} #Item ID{item_id}. "
                            f"Recording inventory movement with zero value."
                        )

                        value_change_dec = Decimal('0')

        if summary:
            current_quantity = Decimal(str(summary.total_quantity))
            current_value = Decimal(str(summary.total_value))
            current_avg_cost = Decimal(str(summary.average_cost)) if summary.average_cost else Decimal('0')

            new_quantity = current_quantity + quantity_change_dec
            new_value = current_value + value_change_dec

            # Handle all negative inventory scenarios
            if current_quantity < 0:
                abs_current_qty = abs(current_quantity)

                if quantity_change_dec > 0:
                    # Adding positive stock to negative inventory
                    if quantity_change_dec != 0 and value_change_dec != 0:
                        incoming_avg_cost = value_change_dec / quantity_change_dec

                        if quantity_change_dec >= abs_current_qty:
                            # Fully resolves negative balance
                            remaining_positive = quantity_change_dec - abs_current_qty
                            new_value = incoming_avg_cost * remaining_positive
                            new_avg_cost = incoming_avg_cost
                        else:
                            # Partially resolves negative balance
                            remaining_negative = abs_current_qty - quantity_change_dec
                            new_value = -incoming_avg_cost * remaining_negative
                            new_avg_cost = incoming_avg_cost
                    else:
                        # No incoming value information
                        remaining_negative = abs_current_qty - quantity_change_dec
                        new_avg_cost = current_avg_cost
                        new_value = -new_avg_cost * remaining_negative

                elif quantity_change_dec < 0:
                    # Adding more negative stock
                    if current_value != 0 and value_change_dec != 0:
                        new_avg_cost = (current_value + value_change_dec) / new_quantity
                    else:
                        new_avg_cost = current_avg_cost

            elif new_quantity != 0:
                # Normal case calculations
                current_has_meaningful_values = (current_quantity != 0 and current_value != 0 and current_avg_cost != 0)
                incoming_has_meaningful_values = (quantity_change_dec != 0 and value_change_dec != 0)

                if current_has_meaningful_values and incoming_has_meaningful_values:

                    new_avg_cost = (current_value + value_change_dec) / new_quantity
                elif incoming_has_meaningful_values:

                    new_avg_cost = value_change_dec / quantity_change_dec
                elif current_has_meaningful_values:
                    new_avg_cost = current_avg_cost
                else:
                    new_avg_cost = get_last_purchase_price(db_session, app_id, item_id) or Decimal('0')

            else:
                # Zero quantity
                if current_avg_cost != 0:
                    new_avg_cost = current_avg_cost
                else:
                    last_price = get_last_purchase_price(db_session, app_id, item_id)
                    if last_price and last_price > 0:
                        new_avg_cost = last_price
                    else:
                        new_avg_cost = 0
                new_value = Decimal('0')

            # After calculating new_avg_cost
            # VALIDATION: Negative average cost indicates data integrity issue
            if new_avg_cost < 0:
                # Preserve the current average cost instead of using incoming cost
                new_avg_cost = -1 * new_avg_cost
                new_value = new_avg_cost * new_quantity
            # Update the summary
            summary.total_quantity = float(round(new_quantity, 6))
            summary.total_value = float(round(new_value, 2))  # Round currency to 2 decimals
            summary.average_cost = float(round(new_avg_cost, 6))
            summary.last_updated = datetime.datetime.now()

        else:

            # Create new summary with validation
            if quantity_change_dec != 0 and value_change_dec != 0:
                try:
                    avg_cost = value_change_dec / quantity_change_dec
                except ZeroDivisionError:
                    avg_cost = get_last_purchase_price(db_session, app_id, item_id) or Decimal('0')
            else:
                avg_cost = get_last_purchase_price(db_session, app_id, item_id) or Decimal('0')

            # After calculating new_avg_cost
            # Validate average cost
            if avg_cost < 0:
                logger.error(f"CRITICAL: Negative average cost {avg_cost} for new item {item_id}")
                raise ValueError(f"Negative average cost for new item: {avg_cost}")

            if quantity_change_dec != 0:
                summary = InventorySummary(
                    app_id=app_id,
                    item_id=item_id,
                    location_id=location_id,
                    total_quantity=float(round(quantity_change_dec, 6)),
                    total_value=float(round(value_change_dec, 2)),
                    average_cost=float(round(avg_cost, 6)),
                    last_updated=datetime.datetime.now()
                )
                db_session.add(summary)
    except ValueError as e:
        logger.error(f"Error in update_inventory_summary: {str(e)}\n{traceback.format_exc()}")
        # Consider whether to raise or handle gracefully based on your use case
        raise ValueError(
            f'Unable to process inventory movement. {e}')

    except Exception as e:
        logger.error(f"Error in update_inventory_summary: {str(e)}\n{traceback.format_exc()}")
        # Consider whether to raise or handle gracefully based on your use case
        raise Exception('Inventory Failed to Update. Please contact admin')


def get_current_average_cost(db_session, app_id, item_id, location_id):
    """
    Get current average cost from inventory summary

    Get the current average cost of an item variation from the inventory summary.

    ⚠️ NOTE:
    The `item_id` here refers to the **InventoryItemVariationLink.id**,
    not the `InventoryItem.id`. This ensures the average cost is tracked
    per variation (e.g., size, color, etc.) within a specific location.

    """
    summary = db_session.query(InventorySummary).filter_by(
        app_id=app_id,
        item_id=item_id,
        location_id=location_id
    ).first()

    if summary:
        return Decimal(str(summary.average_cost))
    return Decimal('0')


def process_inventory_entries(db_session, app_id, inventory_items, quantities,
                              unit_prices, selling_prices, location, from_location, to_location,
                              transaction_date, supplier_id, form_currency_id, base_currency_id, expiration_date,
                              reference, write_off_reason, project_ids, movement_type, current_user_id,
                              source_type=None, system_quantities=None, adjustments=None,
                              payable_account_id=None, write_off_account_id=None, adjustment_account_id=None,
                              sales_account_id=None, batch_notifications=None, exchange_rate_id=None,
                              is_posted_to_ledger=None, source_id=None, handled_by=None):
    """
    Process inventory entries with proper batch handling
    """

    # Create inventory entry header
    inventory_entry = create_inventory_entry_header(
        db_session, app_id, transaction_date, supplier_id, current_user_id, expiration_date, reference,
        write_off_reason, source_type, movement_type,
        from_location, to_location, form_currency_id, payable_account_id,
        write_off_account_id, adjustment_account_id, sales_account_id,
        exchange_rate_id, source_id, handled_by
    )

    # Process each line item
    for idx, (item_id, quantity, form_unit_price, selling_price, project_id) in enumerate(zip(
            inventory_items, quantities, unit_prices, selling_prices, project_ids), start=1):
        process_single_line_item(
            db_session, app_id, idx, item_id, quantity, form_unit_price, selling_price,
            location, from_location, to_location, transaction_date, supplier_id,
            form_currency_id, base_currency_id, project_id,
            movement_type, system_quantities, adjustments,
            batch_notifications, inventory_entry, is_posted_to_ledger=is_posted_to_ledger, source_type=source_type
        )

    return inventory_entry


def process_inventory_entries_for_edit(db_session, app_id, inventory_items, quantities,
                                       unit_prices, selling_prices, location, from_location, to_location,
                                       transaction_date, supplier_id, form_currency_id, base_currency_id,
                                       expiration_date, reference, project_ids, movement_type, current_user_id,
                                       source_type=None, system_quantities=None, adjustments=None,
                                       payable_account_id=None, write_off_account_id=None, adjustment_account_id=None,
                                       sales_account_id=None, batch_notifications=None, exchange_rate_id=None,
                                       existing_entry=None, posted_status=None):
    """
    Process inventory entries with proper batch handling
    """

    # Use the existing inventory entry instead of creating a new one
    inventory_entry = existing_entry

    # Process each line item
    for idx, (item_id, quantity, form_unit_price, selling_price, project_id) in enumerate(zip(
            inventory_items, quantities, unit_prices, selling_prices, project_ids), start=1):
        process_single_line_item(
            db_session, app_id, idx, item_id, quantity, form_unit_price, selling_price,
            location, from_location, to_location, transaction_date, supplier_id,
            form_currency_id, base_currency_id, project_id,
            movement_type, system_quantities, adjustments,
            batch_notifications, inventory_entry, is_posted_to_ledger=posted_status
        )

    return inventory_entry


def create_inventory_entry_header(db_session, app_id, transaction_date, supplier_id, current_user_id,
                                  expiration_date, reference, write_off_reason, source_type, movement_type,
                                  from_location, to_location, form_currency_id, payable_account_id,
                                  write_off_account_id, adjustment_account_id, sales_account_id, exchange_rate_id,
                                  source_id, handled_by):
    """
    Create inventory entry header
    """

    if source_type in ['direct_purchase', 'goods_receipt']:
        inventory_source = source_type
        source_type = 'purchase'

    else:
        inventory_source = source_type

    inventory_entry = InventoryEntry(
        app_id=app_id,
        transaction_date=transaction_date,
        supplier_id=supplier_id,
        created_by=current_user_id,
        expiration_date=expiration_date,
        reference=reference,
        inventory_source=inventory_source,
        source_type=source_type,
        stock_movement=movement_type,
        from_location=from_location,
        to_location=to_location,
        currency_id=form_currency_id,
        payable_account_id=payable_account_id,
        write_off_account_id=write_off_account_id,
        adjustment_account_id=adjustment_account_id,
        sales_account_id=sales_account_id,
        exchange_rate_id=exchange_rate_id,
        notes=write_off_reason,
        source_id=source_id,
        handled_by=handled_by
    )

    db_session.add(inventory_entry)
    db_session.flush()
    return inventory_entry


def process_single_line_item(db_session, app_id, idx, item_id, quantity, form_unit_price, selling_price,
                             location, from_location, to_location, transaction_date, supplier_id,
                             form_currency_id, base_currency_id, project_id,
                             movement_type, system_quantities, adjustments,
                             batch_notifications, inventory_entry, is_posted_to_ledger=None, source_type=None):
    """
    Process a single line item
    """

    # Validate inventory item
    transaction_detail = None
    inventory_item = validate_inventory_item(db_session, app_id, item_id, idx)

    # Convert numeric values
    quantity_dec, form_unit_price_dec, selling_price_dec = convert_numeric_values(
        quantity, form_unit_price, selling_price, idx
    )

    # Validate quantity
    validate_quantity(movement_type, quantity_dec, idx)

    # Get adjustment data if applicable
    system_quantity, adjustment_amount = get_adjustment_data(
        movement_type, system_quantities, adjustments, idx
    )

    # Determine quantities for batch operations
    batch_quantity, inventory_quantity, final_quantity = determine_batch_quantities(
        movement_type, quantity_dec, adjustment_amount
    )

    # Handle exchange rate
    exchange_rate_id = handle_exchange_rate(
        db_session, movement_type, form_currency_id, base_currency_id,
        app_id, transaction_date, batch_notifications
    )

    # CREATE LINE ITEM FIRST
    line_item = create_line_item(
        db_session, app_id, inventory_entry.id, item_id, form_unit_price,
        selling_price_dec, inventory_quantity, adjustment_amount,
        system_quantity, final_quantity, movement_type, project_id
    )
    db_session.flush()  # Ensure line item gets an ID

    # UPDATE INVENTORY ENTRY HEADER LOCATIONS BASED ON MOVEMENT TYPE
    update_inventory_entry_locations(inventory_entry, movement_type, location, from_location, to_location,
                                     adjustment_amount)

    # Process based on movement type
    if movement_type == 'transfer':
        batch_variation_id, unit_cost = process_transfer_movement(
            db_session, app_id, item_id, from_location, to_location,
            batch_quantity, form_currency_id, supplier_id,
            transaction_date, project_id, line_item.id, movement_type, is_posted_to_ledger=is_posted_to_ledger
        )
        line_item.unit_price = unit_cost

    elif movement_type == 'dept_transfer':
        # Remove from old department
        process_out_movement(
            db_session, app_id, item_id, form_currency_id, supplier_id,
            old_project_id, transaction_date, location, quantity,
            line_item.id, "out", is_posted_to_ledger
        )

        # Add to new department (same location)
        process_in_movement(
            db_session, app_id, item_id, location, quantity,
            current_unit_cost, form_currency_id, supplier_id,
            transaction_date, new_project_id, line_item.id, "in",
            "dept_transfer", is_posted_to_ledger
        )

    elif movement_type == 'adjustment':
        batch_variation_id, unit_cost = process_adjustment_movement(
            db_session, app_id, item_id, location, batch_quantity, adjustment_amount, form_unit_price, form_currency_id,
            supplier_id, transaction_date, project_id, line_item.id, inventory_entry, movement_type, is_posted_to_ledger
        )
        line_item.unit_price = unit_cost

    elif movement_type in ['missing', 'expired', 'damaged']:
        transaction_id, unit_cost = process_missing_movement(
            db_session, app_id, item_id, form_currency_id, supplier_id, project_id, transaction_date, location,
            batch_quantity,
            line_item.id, is_posted_to_ledger
        )
        line_item.unit_price = unit_cost
        inventory_entry.stock_movement = "out"

    elif movement_type in ['stock_out_sale', 'stock_out_write_off']:

        initial_movement_type = movement_type

        movement_type = 'out'

        item_name = line_item.inventory_item_variation_link.inventory_item.item_name

        transaction_id, unit_cost = process_out_movement(
            db_session, app_id, item_id, form_currency_id, supplier_id, project_id, transaction_date, location,
            batch_quantity,
            line_item.id, movement_type, is_posted_to_ledger=is_posted_to_ledger, item_name=item_name
        )
        line_item.unit_price = unit_cost
        inventory_entry.stock_movement = "out"
        inventory_entry.source_type = 'write_off' if initial_movement_type == 'stock_out_write_off' else 'sale'

    elif movement_type == 'opening_balance':

        supplier_id = None
        movement_type = "in"
        transaction_detail, unit_cost = process_opening_balance_movement(
            db_session, app_id, item_id, location, batch_quantity,
            form_unit_price_dec, form_currency_id,
            supplier_id, transaction_date, project_id, line_item.id, movement_type,
            is_posted_to_ledger=is_posted_to_ledger
        )
        line_item.unit_price = unit_cost
        inventory_entry.stock_movement = "in"

    else:  # 'in' movement
        transaction_detail, unit_cost = process_in_movement(
            db_session, app_id, item_id, location, batch_quantity,
            form_unit_price_dec, form_currency_id,
            supplier_id, transaction_date, project_id, line_item.id, movement_type, source_type, is_posted_to_ledger
        )

    # Update inventory status
    update_inventory_status(inventory_item)


def update_inventory_entry_locations(inventory_entry, movement_type, location, from_location, to_location,
                                     adjustment_amount=None):
    """
    Update or create an inventory summary record for a given item/variation/project.

    This function ensures real-time tracking of stock quantity, value, and average cost
    whenever inventory transactions occur. It is used for purchases, sales, returns,
    adjustments, transfers, and donations.

    Core responsibilities:
    ----------------------
    1. Fetch or create the inventory summary for the given item/variation/project.
    2. Apply the transaction's quantity and value changes.
    3. Maintain running totals:
        - opening_balance_quantity, opening_balance_value
        - quantity_in / quantity_out
        - value_in / value_out
        - closing_balance_quantity, closing_balance_value
    4. Recalculate the weighted average cost whenever stock is increased.
    5. Enforce validation rules:
        - IN transactions normally must have a value (except donations).
        - Prevent negative stock quantities unless explicitly allowed.
    6. Special handling:
        - **Donations**: allow positive quantity with zero value.
        - **Purchases/Receipts**: use transaction value, or fallback to last
          purchase price / average cost if missing.
        - **Sales/Issues**: reduce stock at the current average cost.

    Parameters:
    -----------
    db_session : SQLAlchemy session
        Active database session for querying and committing.
    app_id : int
        ID of the current company/application context.
    item_id : int
        ID of the inventory item being updated.
    variation_id : int or None
        ID of the item variation (if applicable).
    project_id : int or None
        ID of the related project (if stock is tracked per project).
    quantity_change : Decimal or float
        Positive for stock in, negative for stock out.
    value_change : Decimal or float
        Base currency value of the transaction (can be zero for donations).
    source_type : str, optional
        Type of source transaction (e.g., "purchase", "sale", "return",
        "donation", "adjustment").

    Raises:
    -------
    ValueError
        - If IN transaction has no cost (except donations).
        - If closing stock becomes negative (unless project rules allow it).

    Returns:
    --------
    summary : InventorySummary
        Updated summary object with recalculated balances and average cost.
    """
    if movement_type == 'transfer':
        # Transfer uses explicit from_location and to_location
        inventory_entry.from_location = from_location
        inventory_entry.to_location = to_location

    elif movement_type in ['in', 'opening_balance']:
        # IN movement: from supplier to location
        inventory_entry.from_location = None
        inventory_entry.to_location = location

    elif movement_type in ['stock_out_sale', 'stock_out_write_off']:
        # OUT movement: from location to customer
        inventory_entry.from_location = location
        inventory_entry.to_location = None

    elif movement_type == 'adjustment':
        # Adjustment: location depends on adjustment direction
        if adjustment_amount > 0:
            # Positive adjustment: adding stock (to location)
            inventory_entry.from_location = None
            inventory_entry.to_location = location
        else:
            # Negative adjustment: removing stock (from location)
            inventory_entry.from_location = location
            inventory_entry.to_location = None

    elif movement_type in ['missing', 'expired', 'damaged']:
        # Missing/expired/damaged: from location to void
        inventory_entry.from_location = location
        inventory_entry.to_location = None


def process_in_movement(db_session, app_id, item_id, location_id, quantity,
                        unit_cost, currency_id, supplier_id, transaction_date, project_id,
                        inventory_entry_line_item, movement_type, source_type=None, is_posted_to_ledger=None):
    """
    Process IN movement using average cost method
    All values are already in base currency
    """

    # Create transaction detail with unit cost (already in base currency)
    transaction_detail_id = create_new_inventory_transaction_details(
        db_session, app_id, item_id, location_id, quantity,
        unit_cost, currency_id, supplier_id, transaction_date, project_id,
        inventory_entry_line_item, movement_type, is_posted_to_ledger=is_posted_to_ledger
    )

    # Calculate value change for inventory summary
    value_change = float(Decimal(str(quantity)) * unit_cost)

    # Update inventory summary
    update_inventory_summary(
        db_session, app_id, item_id, location_id,
        float(quantity), value_change, source_type
    )

    return transaction_detail_id, float(unit_cost)


def process_opening_balance_movement(db_session, app_id, item_id, location_id, quantity,
                                     unit_cost, currency_id, supplier_id, transaction_date, project_id,
                                     inventory_entry_line_item, movement_type, is_posted_to_ledger):
    """
    Process IN movement using average cost method
    All values are already in base currency
    """

    # Create transaction detail with unit cost (already in base currency)
    transaction_detail_id = create_new_inventory_transaction_details(
        db_session, app_id, item_id, location_id, quantity,
        unit_cost, currency_id, supplier_id, transaction_date, project_id,
        inventory_entry_line_item, movement_type, is_posted_to_ledger=is_posted_to_ledger
    )

    # Calculate value change for inventory summary
    value_change = float(Decimal(str(quantity)) * unit_cost)

    # Update inventory summary
    update_inventory_summary(
        db_session, app_id, item_id, location_id,
        float(quantity), value_change
    )

    # To add to the retained earnings
    update_retained_earnings_opening_balance(db_session, app_id, current_user.id, value_change, mode='increment')

    return transaction_detail_id, float(unit_cost)


def process_out_movement(db_session, app_id, item_id, currency_id, supplier_id, project_id, transaction_date,
                         location_id, quantity, inventory_entry_line_item, movement_type, is_posted_to_ledger=None,
                         item_name=None):
    """
    Process OUT movement using average cost method
    """

    # Get current average cost for the transaction detail
    current_avg_cost = get_current_average_cost(db_session, app_id, item_id, location_id)

    # Create transaction detail for out movement
    transaction_detail_id = create_new_inventory_transaction_details(
        db_session, app_id, item_id, location_id, -quantity,  # Negative for out
        float(current_avg_cost), currency_id, supplier_id, transaction_date, project_id,
        inventory_entry_line_item, movement_type, is_posted_to_ledger=is_posted_to_ledger
    )

    # Calculate value change for inventory summary (negative)
    value_change = -float(Decimal(str(quantity)) * current_avg_cost)

    # Update inventory summary
    update_inventory_summary(
        db_session=db_session,
        app_id=app_id,
        item_id=item_id,
        location_id=location_id,
        quantity_change=-float(quantity),
        value_change=value_change,
        source_type=None,
        item_name=item_name
    )

    return transaction_detail_id, float(current_avg_cost)


def process_transfer_movement(db_session, app_id, item_id, from_location_id, to_location_id,
                              quantity, currency_id, supplier_id, transaction_date, project_id,
                              inventory_entry_line_item, movement_type, is_posted_to_ledger=None):
    """
    Process TRANSFER movement with average cost
    """
    # Get current average cost from source location
    source_avg_cost = get_current_average_cost(db_session, app_id, item_id, from_location_id)

    # Process OUT from source location
    out_detail_id, _ = process_out_movement(
        db_session, app_id, item_id, currency_id, supplier_id, project_id, transaction_date, from_location_id, quantity,
        inventory_entry_line_item, "out", is_posted_to_ledger
    )

    # Process IN to destination location with source average cost
    in_detail_id, _ = process_in_movement(
        db_session, app_id, item_id, to_location_id, quantity,
        source_avg_cost, currency_id, supplier_id, transaction_date, project_id, inventory_entry_line_item,
        "in", "Posted"
    )

    return in_detail_id, float(source_avg_cost)


def process_missing_movement(db_session, app_id, item_id, currency_id, supplier_id, project_id, transaction_date,
                             location_id, quantity,
                             inventory_entry_line_item, is_posted_to_ledger):
    """
    Process missing/expired/damaged movement using average cost method
    All values are in base currency
    """

    movement_type = "out"

    # Get current average cost for the transaction detail
    current_avg_cost = get_current_average_cost(db_session, app_id, item_id, location_id)

    # Process OUT movement (removing stock)
    # process_out_movement returns (transaction_detail_id, unit_cost_used)
    transaction_detail_id, unit_cost_used = process_out_movement(
        db_session, app_id, item_id, currency_id, supplier_id, project_id, transaction_date, location_id, quantity,
        inventory_entry_line_item, movement_type, is_posted_to_ledger=is_posted_to_ledger
    )

    # Return the transaction detail ID and the current average cost
    return transaction_detail_id, float(current_avg_cost)


def process_adjustment_movement(db_session, app_id, item_id, location_id, quantity,
                                adjustment_amount, suggested_unit_cost, currency_id,
                                supplier_id, transaction_date, project_id, inventory_entry_line_item, inventory_entry,
                                movement_type, is_posted_to_ledger=None):
    """
    Process ADJUSTMENT movement using average cost method
    All values are in base currency
    """

    if adjustment_amount > 0:
        # POSITIVE ADJUSTMENT - Add stock (IN movement)
        current_avg_cost = get_current_average_cost(db_session, app_id, item_id, location_id)
        current_avg_cost = Decimal(current_avg_cost or '0')
        suggested_unit_cost = Decimal(suggested_unit_cost or '0.01')

        # Use current average cost if available, otherwise use suggested unit cost
        unit_cost_to_use = current_avg_cost if current_avg_cost and current_avg_cost > 0 else suggested_unit_cost

        if unit_cost_to_use <= 0:
            unit_cost_to_use = suggested_unit_cost or Decimal('0.01')

        transaction_detail_id, unit_cost_used = process_in_movement(
            db_session, app_id, item_id, location_id, quantity,
            unit_cost_to_use, currency_id, supplier_id, transaction_date, project_id, inventory_entry_line_item,
            'in', is_posted_to_ledger=is_posted_to_ledger
        )

        # Update header for positive adjustment
        inventory_entry.stock_movement = 'in'
        inventory_entry.inventory_source = 'adjustment'

        return transaction_detail_id, float(unit_cost_to_use)

    else:
        # NEGATIVE ADJUSTMENT - Remove stock (OUT movement)
        current_avg_cost = get_current_average_cost(db_session, app_id, item_id, location_id)

        transaction_detail_id, unit_cost_used = process_out_movement(
            db_session, app_id, item_id, currency_id, supplier_id, project_id, transaction_date, location_id,
            abs(quantity),
            inventory_entry_line_item, 'out', is_posted_to_ledger=is_posted_to_ledger
        )

        # Update header for negative adjustment
        inventory_entry.stock_movement = 'out'
        inventory_entry.inventory_source = 'adjustment'

        return transaction_detail_id, float(current_avg_cost)


def create_new_inventory_transaction_details(db_session, app_id, item_id, location_id, quantity,
                                             unit_cost, currency_id, supplier_id, transaction_date, project_id,
                                             inventory_entry_line_item, movement_type, exchange_rate_id=None,
                                             is_posted_to_ledger=None):
    # Create batch variation
    transaction_details = InventoryTransactionDetail(
        app_id=app_id,
        item_id=item_id,
        location_id=location_id,
        quantity=float(quantity),
        transaction_date=transaction_date,
        vendor_id=supplier_id,
        unit_cost=float(unit_cost),
        total_cost=round(float(unit_cost) * float(quantity), 2),
        currency_id=currency_id,
        project_id=project_id,

        movement_type=movement_type,
        inventory_entry_line_item_id=inventory_entry_line_item,
        exchange_rate_id=exchange_rate_id,
        is_posted_to_ledger=is_posted_to_ledger
    )

    db_session.add(transaction_details)
    db_session.flush()

    return transaction_details


def convert_numeric_values(quantity, unit_price, selling_price, idx):
    """Convert and validate numeric values"""
    try:
        quantity_dec = Decimal(quantity) if quantity else Decimal('0.0')
        unit_price_dec = Decimal(unit_price) if unit_price else Decimal('0.0')
        selling_price_dec = Decimal(selling_price) if selling_price else Decimal('0.0')
        return quantity_dec, unit_price_dec, selling_price_dec
    except Exception as e:
        raise ValueError(f"Invalid numeric value in item {idx}: {str(e)}")


def validate_quantity(movement_type, quantity, idx):
    """Validate quantity based on movement type"""
    if movement_type != 'adjustment' and quantity <= 0:
        raise ValueError(f"Quantity must be positive for item {idx} for {movement_type} movement")


def get_adjustment_data(movement_type, system_quantities, adjustments, idx):
    """Get adjustment data if applicable"""
    system_quantity = None
    adjustment_amount = None

    if movement_type == 'adjustment' and system_quantities and adjustments:
        try:
            system_quantity = Decimal(system_quantities[idx - 1]) if system_quantities[idx - 1] else Decimal('0.0')
            adjustment_amount = Decimal(adjustments[idx - 1]) if adjustments[idx - 1] else Decimal('0.0')
        except (IndexError, ValueError) as e:
            raise ValueError(f"Invalid system quantity or adjustment for item {idx}: {str(e)}")

    return system_quantity, adjustment_amount


def determine_batch_quantities(movement_type, quantity, adjustment_amount):
    """Determine quantities for batch operations"""
    if movement_type == 'adjustment':
        return adjustment_amount, adjustment_amount, quantity
    else:
        return quantity, quantity, None


def handle_exchange_rate(db_session, movement_type, form_currency_id, base_currency_id,
                         app_id, transaction_date, batch_notifications):
    """Handle exchange rate conversion"""
    if (movement_type != 'transfer' and
            int(form_currency_id) != int(base_currency_id)):

        rate_id, rate_date, notification = get_or_create_exchange_rate_id(
            session=db_session,
            from_currency_id=int(form_currency_id),
            to_currency_id=base_currency_id,
            app_id=app_id,
            transaction_date=transaction_date
        )

        if batch_notifications:
            batch_notifications.add(notification)

        return rate_id

    return None


def create_line_item(db_session, app_id, inventory_entry_id, item_id,
                     unit_price, selling_price, quantity,
                     adjustment_amount, system_quantity, final_quantity,
                     movement_type, project_id):
    """Create a line item"""
    line_item = InventoryEntryLineItem(
        inventory_entry_id=inventory_entry_id,
        app_id=app_id,
        item_id=item_id,
        unit_price=float(unit_price),
        selling_price=float(selling_price),
        quantity=abs(float(quantity)),
        adjustment_amount=float(adjustment_amount) if adjustment_amount and movement_type == 'adjustment' else None,
        system_quantity=float(system_quantity) if system_quantity and movement_type == 'adjustment' else None,
        adjusted_quantity=float(final_quantity) if final_quantity and movement_type == 'adjustment' else None,
        project_id=project_id if project_id else None
    )

    db_session.add(line_item)
    return line_item


def update_inventory_status(inventory_item):
    """Update inventory item status"""
    inventory_item.inventory_item.status = "active"


def calculate_location_fifo_cogs_with_avg(db_session, item_id, location_id, quantity_sold, app_id,
                                          base_currency_id=None):
    """
    Calculate FIFO COGS for an item using BatchVariationLink and return final weighted average cost.
    Handles multi-currency using batch-specific exchange rates.
    Negative stock allowed on the last batch if insufficient quantity.

    Returns:
        cogs_details: list of batch-level deductions
        average_cost_base: weighted average cost in base currency for total quantity
    """
    from decimal import Decimal, ROUND_HALF_UP

    try:
        remaining_quantity = Decimal(quantity_sold)
        cogs_details = []
        notifications = []

        # Get item variation link
        item_link = db_session.query(InventoryItemVariationLink).options(
            joinedload(InventoryItemVariationLink.inventory_item),
            joinedload(InventoryItemVariationLink.inventory_item_variation)
        ).filter(
            InventoryItemVariationLink.id == item_id,
            InventoryItemVariationLink.app_id == app_id
        ).first()

        if not item_link:
            raise ValueError(f"Inventory item variation link with ID {item_id} not found")

        item_name = item_link.inventory_item.item_name
        variation_name = item_link.inventory_item_variation.variation_name if item_link.inventory_item_variation else ""

        # Get batches in FIFO order
        batches = db_session.query(BatchVariationLink).filter(
            BatchVariationLink.app_id == app_id,
            BatchVariationLink.item_id == item_id,
            BatchVariationLink.location_id == location_id,
        ).order_by(BatchVariationLink.transaction_date.asc()).with_for_update().all()

        last_batch = None
        unit_cost_base_last = Decimal(0)
        total_cost_base = Decimal(0)
        total_quantity_deducted = Decimal(0)

        for batch in batches:
            last_batch = batch
            available_qty = Decimal(batch.quantity)
            deduct_qty = min(remaining_quantity, available_qty)

            # Compute unit cost in base currency
            unit_cost_base = Decimal(batch.unit_cost)
            if int(batch.currency_id) != int(base_currency_id):
                if not batch.exchange_rate or not batch.exchange_rate.rate:
                    raise ValueError(f"No exchange rate found for batch {batch.id}")
                unit_cost_base *= Decimal(batch.exchange_rate.rate)

            unit_cost_base_last = unit_cost_base  # Keep for negative

            # Track total cost and quantity for weighted average
            total_cost_base += unit_cost_base * deduct_qty
            total_quantity_deducted += deduct_qty

            cogs_details.append({
                'batch_id': batch.batch_id,
                'batch_number': batch.batch.batch_number,
                'location_id': batch.location_id,
                'deduct_quantity': float(deduct_qty),
                'unit_cost': float(batch.unit_cost),
                'unit_cost_base': float(unit_cost_base),
                'currency_id': batch.currency_id,
                'supplier_id': batch.supplier_id,
                'batch_entry_id': batch.id,
                'total_cost_base': float(unit_cost_base * deduct_qty)
            })

            # Update batch quantity
            batch.quantity -= float(deduct_qty)
            remaining_quantity -= deduct_qty

        # Handle negative quantity on last batch
        if remaining_quantity > 0 and last_batch:
            last_batch.quantity -= float(remaining_quantity)
            total_cost_base += unit_cost_base_last * remaining_quantity
            total_quantity_deducted += remaining_quantity
            notifications.append(
                f"Insufficient stock: {remaining_quantity} units of {item_name} ({variation_name}) recorded as negative"
            )

            cogs_details.append({
                'batch_id': last_batch.batch_id,
                'batch_number': last_batch.batch.batch_number,
                'location_id': last_batch.location_id,
                'deduct_quantity': float(remaining_quantity),
                'unit_cost': float(last_batch.unit_cost),
                'unit_cost_base': float(unit_cost_base_last),
                'currency_id': last_batch.currency_id,
                'supplier_id': last_batch.supplier_id,
                'batch_entry_id': last_batch.id,
                'total_cost_base': float(unit_cost_base_last * remaining_quantity)
            })

        # Weighted average cost
        average_cost_base = (total_cost_base / total_quantity_deducted).quantize(Decimal("0.01"),
                                                                                 rounding=ROUND_HALF_UP) \
            if total_quantity_deducted > 0 else Decimal(0)

        # Optionally send notifications
        for msg in notifications:
            create_notification(
                db_session=db_session,
                user_id=None,
                company_id=app_id,
                message=msg,
                type='warning',
                is_popup=True,
                url=f'/inventory/items/{item_id}'
            )

        return cogs_details, float(average_cost_base)

    except Exception as e:
        db_session.rollback()
        raise e


# def calculate_inventory_quantities(db_session, app_id, item_ids, use_variation_ids=False, **kwargs):
#     """
#     Calculate inventory quantities with various filtering options
#
#     Args:
#         db_session: Database session
#         app_id: Company ID
#         item_ids: List of inventory item IDs OR variation link IDs
#         use_variation_ids: If True, item_ids are InventoryItemVariationLink IDs
#                           If False, item_ids are InventoryItem IDs (default)
#         **kwargs: Optional filters:
#             - location_id: Filter by specific location
#             - only_positive: Only include positive quantities (default: False)
#             - include_negative: Include negative quantities (default: True)
#             - group_by_location: Return quantities by location
#
#            ************************ Usage Example **********************************
#            * quantity_dict = calculate_inventory_quantities(                       *
#            *     db_session=db_session,                                            *
#            *     app_id=app_id,                                                    *
#            *     item_ids=item_ids,                                                *
#            *     location_id=location_id,  # Pass location filter if provided      *
#            *     include_negative=True     # Include negative quantities (default) *
#            * )                                                                     *
#            *************************************************************************
#     Returns:
#         Dictionary with quantities. Format depends on group_by_location:
#         - If not grouped: {item_id: total_quantity}
#           (item_id will be the same type as input: InventoryItem.id or InventoryItemVariationLink.id)
#         - If grouped: {item_id: {location_id: quantity}}
#     """
#     if not item_ids:
#         return {}
#
#     # Determine target IDs based on the use_variation_ids flag
#     if use_variation_ids:
#         # item_ids are already InventoryItemVariationLink IDs
#         target_ids = item_ids
#         id_type = 'variation'
#     else:
#         # item_ids are InventoryItem IDs - convert to variation link IDs
#         variation_link_ids = db_session.query(InventoryItemVariationLink.id).filter(
#             InventoryItemVariationLink.app_id == app_id,
#             InventoryItemVariationLink.inventory_item_id.in_(item_ids)
#         ).all()
#         target_ids = [id[0] for id in variation_link_ids]
#         id_type = 'item'
#
#     if not target_ids:
#         return {}
#
#     # Build the base query
#     query = db_session.query(InventoryTransactionDetail).filter(
#         InventoryTransactionDetail.app_id == app_id,
#         InventoryTransactionDetail.item_id.in_(target_ids)
#     )
#
#     # Location filter
#     if kwargs.get('location_id'):
#         query = query.filter(InventoryTransactionDetail.location_id == kwargs['location_id'])
#
#     # Quantity filters
#     if kwargs.get('only_positive', False):
#         query = query.filter(InventoryTransactionDetail.quantity > 0)
#     elif not kwargs.get('include_negative', True):
#         query = query.filter(InventoryTransactionDetail.quantity >= 0)
#
#     # Grouping logic
#     if kwargs.get('group_by_location', False):
#         # Group by both item_id and location_id
#         quantity_results = query.with_entities(
#             InventoryTransactionDetail.item_id,
#             InventoryTransactionDetail.location_id,
#             func.sum(InventoryTransactionDetail.quantity).label('quantity')
#         ).group_by(InventoryTransactionDetail.item_id, InventoryTransactionDetail.location_id).all()
#
#         # Convert to nested dictionary: {item_id: {location_id: quantity}}
#         result = {}
#         for item_id, location_id, quantity in quantity_results:
#             if item_id not in result:
#                 result[item_id] = {}
#             result[item_id][location_id] = float(quantity)
#
#         # If we started with InventoryItem IDs, we need to map back
#         if id_type == 'item':
#             return _map_variation_to_item_ids(db_session, app_id, result)
#         return result
#
#     else:
#         # Group by item_id only
#         quantity_results = query.with_entities(
#             InventoryTransactionDetail.item_id,
#             func.sum(InventoryTransactionDetail.quantity).label('total_quantity')
#         ).group_by(InventoryTransactionDetail.item_id).all()
#
#         result = {item_id: float(total_qty) for item_id, total_qty in quantity_results}
#
#         # If we started with InventoryItem IDs, we need to map back
#         if id_type == 'item':
#             return _map_variation_to_item_ids(db_session, app_id, result)
#         return result
#


def calculate_inventory_quantities(db_session, app_id, item_ids, use_variation_ids=False, **kwargs):
    """
    Calculate inventory quantities from InventorySummary (optimized for average cost system)

    Args:
        db_session: Database session
        app_id: Company ID
        item_ids: List of inventory item IDs OR variation link IDs
        use_variation_ids: If True, item_ids are InventoryItemVariationLink IDs
                          If False, item_ids are InventoryItem IDs (default)
        **kwargs: Optional filters:
            - location_id: Filter by specific location
            - user_location_ids: Filter by user's assigned locations
            - only_positive: Only include positive quantities (default: False)
            - include_negative: Include negative quantities (default: True)
            - group_by_location: Return quantities by location

    Returns:
        Dictionary with quantities from InventorySummary
    """
    if not item_ids:
        return {}

    # Determine target IDs based on the use_variation_ids flag
    if use_variation_ids:
        # item_ids are already InventoryItemVariationLink IDs
        target_ids = item_ids
        id_type = 'variation'

    else:
        # item_ids are InventoryItem IDs - convert to variation link IDs
        variation_link_ids = db_session.query(InventoryItemVariationLink.id).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.inventory_item_id.in_(item_ids)
        ).all()
        target_ids = [id[0] for id in variation_link_ids]
        id_type = 'item'

    if not target_ids:
        return {}

    # Build the base query - USE INVENTORY SUMMARY
    query = db_session.query(InventorySummary).filter(
        InventorySummary.app_id == app_id,
        InventorySummary.item_id.in_(target_ids)
    )

    # ===== LOCATION FILTERS =====
    # Build list of locations to filter by
    locations_to_filter = []

    # First, check for specific location filter
    if kwargs.get('location_id'):
        locations_to_filter = [kwargs['location_id']]
    # Then, check for user's assigned locations
    elif kwargs.get('user_location_ids'):
        locations_to_filter = kwargs['user_location_ids']

    # Apply location filter if we have locations to filter by
    if locations_to_filter:
        query = query.filter(InventorySummary.location_id.in_(locations_to_filter))

    # Quantity filters
    if kwargs.get('only_positive', False):
        query = query.filter(InventorySummary.total_quantity > 0)
    elif not kwargs.get('include_negative', True):
        query = query.filter(InventorySummary.total_quantity >= 0)

    # Grouping logic
    if kwargs.get('group_by_location', False):
        # Query directly from InventorySummary
        quantity_results = query.with_entities(
            InventorySummary.item_id,
            InventorySummary.location_id,
            InventorySummary.total_quantity
        ).all()

        # Convert to nested dictionary: {item_id: {location_id: quantity}}
        result = {}
        for item_id, location_id, quantity in quantity_results:
            if item_id not in result:
                result[item_id] = {}
            result[item_id][location_id] = float(quantity)

        # If we started with InventoryItem IDs, we need to map back
        if id_type == 'item':
            return _map_variation_to_item_ids(db_session, app_id, result)
        return result

    else:
        # Group by item_id only - sum across locations if multiple exist
        quantity_results = query.with_entities(
            InventorySummary.item_id,
            func.sum(InventorySummary.total_quantity).label('total_quantity')
        ).group_by(InventorySummary.item_id).all()

        result = {item_id: float(total_qty) for item_id, total_qty in quantity_results}

        # If we started with InventoryItem IDs, we need to map back
        if id_type == 'item':
            return _map_variation_to_item_ids(db_session, app_id, result)
        return result


def get_stock_by_department_filtered(db_session, app_id, item_ids, location_id, project_id, use_variation_ids=False):
    """
    Get stock quantity for a specific department
    """
    if not item_ids or not location_id or not project_id:
        return {}

    # Convert to variation link IDs if needed
    if not use_variation_ids:
        variation_link_ids = db_session.query(InventoryItemVariationLink.id).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.inventory_item_id.in_(item_ids)
        ).all()
        target_ids = [id[0] for id in variation_link_ids]
    else:
        target_ids = item_ids

    if not target_ids:
        return {}

    # Query transaction details for specific department
    # Since out is already negative, just SUM directly
    results = db_session.query(
        InventoryTransactionDetail.item_id,
        func.sum(InventoryTransactionDetail.quantity).label('available_quantity')
    ).filter(
        InventoryTransactionDetail.app_id == app_id,
        InventoryTransactionDetail.item_id.in_(target_ids),
        InventoryTransactionDetail.location_id == location_id,
        InventoryTransactionDetail.project_id == project_id
    ).group_by(
        InventoryTransactionDetail.item_id
    ).all()

    return {row.item_id: float(row.available_quantity) for row in results}


def get_stock_by_department_breakdown(db_session, app_id, item_ids, location_id, use_variation_ids=False):
    """
    Get stock quantities grouped by department for transfer form
    """
    if not item_ids or not location_id:
        return None

    # Convert to variation link IDs if needed
    if not use_variation_ids:
        variation_link_ids = db_session.query(InventoryItemVariationLink.id).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.inventory_item_id.in_(item_ids)
        ).all()
        target_ids = [id[0] for id in variation_link_ids]
    else:
        target_ids = item_ids

    if not target_ids:
        return None

    # Query transaction details grouped by department
    # Since out is already negative, just SUM directly
    results = db_session.query(
        InventoryTransactionDetail.item_id,
        Project.id.label('project_id'),
        Project.name.label('project_name'),
        func.sum(InventoryTransactionDetail.quantity).label('available_quantity')
    ).join(
        Project, InventoryTransactionDetail.project_id == Project.id
    ).filter(
        InventoryTransactionDetail.app_id == app_id,
        InventoryTransactionDetail.item_id.in_(target_ids),
        InventoryTransactionDetail.location_id == location_id,
        Project.is_active == True
    ).group_by(
        InventoryTransactionDetail.item_id, Project.id, Project.name
    ).having(
        func.sum(InventoryTransactionDetail.quantity) != 0
    ).all()

    # Format response for frontend
    departments = []
    for row in results:
        departments.append({
            'item_id': row.item_id,
            'project_id': row.project_id,
            'project_name': row.project_name,
            'available_quantity': float(row.available_quantity)
        })

    return departments


def _map_variation_to_item_ids(db_session, app_id, variation_results):
    """
    Helper function to map variation link IDs back to inventory item IDs
    """
    if not variation_results:
        return {}

    # Get all variation link IDs from the results
    variation_ids = list(variation_results.keys())

    # Query to map variation IDs to inventory item IDs
    mapping_query = db_session.query(
        InventoryItemVariationLink.id,
        InventoryItemVariationLink.inventory_item_id
    ).filter(
        InventoryItemVariationLink.app_id == app_id,
        InventoryItemVariationLink.id.in_(variation_ids)
    ).all()

    variation_to_item_map = {variation_id: item_id for variation_id, item_id in mapping_query}

    # Convert the results
    if isinstance(next(iter(variation_results.values())), dict):
        # Nested dictionary (grouped by location)
        result = {}
        for variation_id, location_data in variation_results.items():
            item_id = variation_to_item_map.get(variation_id)
            if item_id:
                if item_id not in result:
                    result[item_id] = {}
                # Merge location data
                for location_id, quantity in location_data.items():
                    result[item_id][location_id] = result[item_id].get(location_id, 0) + quantity
        return result
    else:
        # Flat dictionary
        result = {}
        for variation_id, quantity in variation_results.items():
            item_id = variation_to_item_map.get(variation_id)
            if item_id:
                result[item_id] = result.get(item_id, 0) + quantity
        return result


def get_inventory_quantity_and_cost(db_session, app_id, item_ids, use_variation_ids=False, **kwargs):
    """
    Get inventory quantities AND average costs from InventorySummary

    Args:
        db_session: Database session
        app_id: Company ID
        item_ids: List of inventory item IDs OR variation link IDs
        use_variation_ids: If True, item_ids are InventoryItemVariationLink IDs
                          If False, item_ids are InventoryItem IDs (default)
        **kwargs: Optional filters:
            - location_id: Filter by specific location
            - only_positive: Only include positive quantities (default: False)
            - include_negative: Include negative quantities (default: True)
            - group_by_location: Return quantities by location

    Returns:
        Dictionary with quantities and average costs:
        - If group_by_location=True: {item_id: {location_id: {'quantity': float, 'average_cost': float}}}
        - If group_by_location=False: {item_id: {'quantity': float, 'average_cost': float}}
    """
    if not item_ids:
        return {}

    # Determine target IDs based on the use_variation_ids flag
    if use_variation_ids:
        target_ids = item_ids
        id_type = 'variation'
    else:
        variation_link_ids = db_session.query(InventoryItemVariationLink.id).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.inventory_item_id.in_(item_ids)
        ).all()
        target_ids = [id[0] for id in variation_link_ids]
        id_type = 'item'

    if not target_ids:
        return {}

    # Build the base query
    query = db_session.query(InventorySummary).filter(
        InventorySummary.app_id == app_id,
        InventorySummary.item_id.in_(target_ids)
    )

    # Location filter
    if kwargs.get('location_id'):
        query = query.filter(InventorySummary.location_id == kwargs['location_id'])

    # Quantity filters
    if kwargs.get('only_positive', False):
        query = query.filter(InventorySummary.total_quantity > 0)
    elif not kwargs.get('include_negative', True):
        query = query.filter(InventorySummary.total_quantity >= 0)

    # Grouping logic
    if kwargs.get('group_by_location', False):
        # Query with both quantity and average_cost
        results = query.with_entities(
            InventorySummary.item_id,
            InventorySummary.location_id,
            InventorySummary.total_quantity,
            InventorySummary.average_cost
        ).all()

        # Convert to nested dictionary: {item_id: {location_id: {'quantity': float, 'average_cost': float}}}
        result = {}
        for item_id, location_id, quantity, avg_cost in results:
            if item_id not in result:
                result[item_id] = {}
            result[item_id][location_id] = {
                'quantity': float(quantity),
                'average_cost': float(avg_cost) if avg_cost else 0
            }

        # If we started with InventoryItem IDs, map back
        if id_type == 'item':
            return _map_variation_to_item_ids_with_cost(db_session, app_id, result)
        return result

    else:
        # Group by item_id only - calculate weighted average cost across locations
        results = query.with_entities(
            InventorySummary.item_id,
            func.sum(InventorySummary.total_quantity).label('total_quantity'),
            func.sum(InventorySummary.total_quantity * InventorySummary.average_cost).label('total_cost'),
            func.sum(InventorySummary.total_quantity).label('weight_sum')
        ).group_by(InventorySummary.item_id).all()

        result = {}
        for item_id, total_qty, total_cost, weight_sum in results:
            total_qty = float(total_qty) if total_qty else 0
            avg_cost = 0
            if weight_sum and weight_sum > 0:
                avg_cost = float(total_cost / weight_sum) if total_cost else 0

            result[item_id] = {
                'quantity': total_qty,
                'average_cost': avg_cost
            }

        # If we started with InventoryItem IDs, map back
        if id_type == 'item':
            return _map_variation_to_item_ids_with_cost(db_session, app_id, result)
        return result


def _map_variation_to_item_ids_with_cost(db_session, app_id, variation_results):
    """
    Helper function to map variation link IDs back to inventory item IDs with cost data
    variation_results format: {variation_id: {'quantity': float, 'average_cost': float}}
    or {variation_id: {location_id: {'quantity': float, 'average_cost': float}}}
    """
    if not variation_results:
        return {}

    # Get mapping from variation link ID to inventory item ID
    variation_ids = list(variation_results.keys())
    mapping = db_session.query(
        InventoryItemVariationLink.id,
        InventoryItemVariationLink.inventory_item_id
    ).filter(
        InventoryItemVariationLink.app_id == app_id,
        InventoryItemVariationLink.id.in_(variation_ids)
    ).all()

    item_id_to_variations = {}
    for var_id, item_id in mapping:
        if item_id not in item_id_to_variations:
            item_id_to_variations[item_id] = []
        item_id_to_variations[item_id].append(var_id)

    # Check if results are location-grouped
    first_value = next(iter(variation_results.values())) if variation_results else None
    is_location_grouped = isinstance(first_value, dict) and first_value and isinstance(next(iter(first_value.values())),
                                                                                       dict)

    result = {}

    for item_id, var_ids in item_id_to_variations.items():
        if is_location_grouped:
            # Location-grouped format
            result[item_id] = {}
            location_data = {}

            for var_id in var_ids:
                var_data = variation_results.get(var_id, {})
                for location_id, data in var_data.items():
                    if location_id not in location_data:
                        location_data[location_id] = {'quantity': 0, 'total_cost': 0}
                    location_data[location_id]['quantity'] += data.get('quantity', 0)
                    location_data[location_id]['total_cost'] += data.get('quantity', 0) * data.get('average_cost', 0)

            for location_id, data in location_data.items():
                avg_cost = 0
                if data['quantity'] > 0:
                    avg_cost = data['total_cost'] / data['quantity']
                result[item_id][location_id] = {
                    'quantity': data['quantity'],
                    'average_cost': avg_cost
                }
        else:
            # Simple format
            total_quantity = 0
            total_cost = 0

            for var_id in var_ids:
                var_data = variation_results.get(var_id, {})
                quantity = var_data.get('quantity', 0)
                avg_cost = var_data.get('average_cost', 0)

                total_quantity += quantity
                total_cost += quantity * avg_cost

            avg_cost = 0
            if total_quantity > 0:
                avg_cost = total_cost / total_quantity

            result[item_id] = {
                'quantity': total_quantity,
                'average_cost': avg_cost
            }

    return result


def get_weighted_average_cost(db_session, app_id, item_id, location_id=None, base_currency_id=None):
    """
    Calculate weighted average cost for an inventory item in base currency
    Returns: Decimal average cost or None if no stock exists

    item_id is the variation item id
    """
    try:
        # Query to get batches with their quantities and costs
        query = db_session.query(BatchVariationLink).filter(
            BatchVariationLink.app_id == app_id,
            BatchVariationLink.item_id == item_id,
            BatchVariationLink.quantity > 0  # Only positive quantities for averaging
        )

        if location_id:
            query = query.filter(BatchVariationLink.location_id == location_id)

        batches = query.all()

        if not batches:
            return None

        total_value_base = Decimal('0')
        total_quantity = Decimal('0')

        # Pre-fetch all needed exchange rates to minimize database queries
        exchange_rates = {}
        batches_needing_conversion = []

        for batch in batches:
            if base_currency_id and int(batch.currency_id) != int(base_currency_id):
                batches_needing_conversion.append(batch)
            else:
                # No conversion needed, process immediately
                batch_cost_base = Decimal(batch.unit_cost)
                batch_quantity = Decimal(batch.quantity)
                total_value_base += batch_cost_base * batch_quantity
                total_quantity += batch_quantity

        # Process batches needing conversion
        for batch in batches_needing_conversion:
            cache_key = f"{batch.currency_id}_{base_currency_id}_{batch.transaction_date}"

            if cache_key not in exchange_rates:

                # Get exchange rate using existing function
                rate_id, rate_date, notification = get_or_create_exchange_rate_id(
                    session=db_session,
                    from_currency_id=int(batch.currency_id),
                    to_currency_id=base_currency_id,
                    app_id=app_id,
                    transaction_date=batch.transaction_date
                )

                if rate_id:
                    exchange_rate_obj = db_session.query(ExchangeRate).filter_by(id=rate_id).first()
                    exchange_rates[cache_key] = Decimal(
                        exchange_rate_obj.rate) if exchange_rate_obj and exchange_rate_obj.rate else None
                else:
                    exchange_rates[cache_key] = None

            batch_cost_base = Decimal(batch.unit_cost)

            if exchange_rates[cache_key]:
                batch_cost_base *= exchange_rates[cache_key]
            else:
                logger.warning(f"No exchange rate found for batch {batch.id}, using original cost")

            batch_quantity = Decimal(batch.quantity)
            total_value_base += batch_cost_base * batch_quantity
            total_quantity += batch_quantity

        if total_quantity > 0:
            average_cost_base = total_value_base / total_quantity
            return round(average_cost_base, 6)

        return None

    except Exception as e:
        logger.error(f"Error calculating weighted average cost: {e}")
        return None


def get_last_purchase_price(db_session, app_id, item_id, base_currency_id=None):
    """
    Get the last purchase price for an inventory item.
    Uses the stored exchange rate from the InventoryTransactionDetail record.
    """
    try:
        # Use eager loading to fetch exchange rate in the same query
        last_purchase = db_session.query(InventoryTransactionDetail).options(
            joinedload(InventoryTransactionDetail.exchange_rate)
        ).filter(
            InventoryTransactionDetail.app_id == app_id,
            InventoryTransactionDetail.item_id == item_id,
            InventoryTransactionDetail.movement_type == 'in',
            InventoryTransactionDetail.unit_cost > 0
        ).order_by(
            InventoryTransactionDetail.transaction_date.desc(),
            InventoryTransactionDetail.created_at.desc()
        ).first()

        if last_purchase:
            purchase_price = Decimal(last_purchase.unit_cost)

            # Check if we need to convert to base currency
            if (base_currency_id and last_purchase.currency_id and
                    int(last_purchase.currency_id) != int(base_currency_id)):

                # Use the relationship directly - no additional query
                if last_purchase.exchange_rate and last_purchase.exchange_rate.rate:
                    purchase_price *= Decimal(last_purchase.exchange_rate.rate)
                else:
                    logger.warning(f"No exchange rate for transaction {last_purchase.id}")

            return purchase_price
        return None

    except Exception as e:
        logger.error(f"Error getting last purchase price: {e}")
        return None


def calculate_inventory_valuation(db_session, app_id, item_ids=None, use_variation_ids=False, **kwargs):
    """
    Calculate inventory valuation using InventorySummary data (average cost method)

    Args:
        db_session: Database session
        app_id: Company ID
        item_ids: List of inventory item IDs OR variation link IDs (optional)
        use_variation_ids: If True, item_ids are InventoryItemVariationLink IDs
                          If False, item_ids are InventoryItem IDs (default)
        **kwargs: Optional filters:
            - location_id: Filter by specific location
            - currency_id: Target currency for valuation (default: base currency)
            - only_positive: Only include positive quantities (default: False)
            - include_negative: Include negative quantities (default: False)
            - group_by_location: Return valuation by location
            - group_by_item: Return valuation by item (default: True)
            - as_of_date: Valuation as of specific date (default: current date)

    Returns:
        Dictionary with valuation data using average cost method
    """
    # Get base currency if no currency specified
    currency_id = kwargs.get('currency_id')
    # Get base currency information for comparison
    base_currency_info = get_base_currency(db_session, app_id)
    if not base_currency_info:
        raise ValueError("Base currency not configured for this company")

    base_currency_id = base_currency_info["base_currency_id"]

    # If no currency specified, use base currency
    if not currency_id:
        currency_id = base_currency_id

    # Determine target IDs
    target_ids = []
    if item_ids:
        if use_variation_ids:
            target_ids = item_ids
            id_type = 'variation'
        else:
            # Convert InventoryItem IDs to variation link IDs
            variation_link_ids = db_session.query(InventoryItemVariationLink.id).filter(
                InventoryItemVariationLink.app_id == app_id,
                InventoryItemVariationLink.inventory_item_id.in_(item_ids)
            ).all()
            target_ids = [id[0] for id in variation_link_ids]
            id_type = 'item'
    else:
        # Get all variation links if no specific items provided
        variation_links = db_session.query(InventoryItemVariationLink.id).filter(
            InventoryItemVariationLink.app_id == app_id
        ).all()
        target_ids = [id[0] for id in variation_links]
        id_type = 'variation'

    if not target_ids:
        return {} if kwargs.get('group_by_item', True) else 0.0

    # Build base query for inventory summary
    query = db_session.query(InventorySummary).filter(
        InventorySummary.app_id == app_id,
        InventorySummary.item_id.in_(target_ids)
    )

    # Apply location filter
    location_id = kwargs.get('location_id')
    if location_id:
        query = query.filter(InventorySummary.location_id == location_id)

    # Quantity filters
    if kwargs.get('only_positive', False):
        query = query.filter(InventorySummary.total_quantity > 0)
    elif not kwargs.get('include_negative', True):
        query = query.filter(InventorySummary.total_quantity >= 0)

    # Date filter (using last_updated instead of transaction_date)
    as_of_date = kwargs.get('as_of_date')
    if as_of_date:
        query = query.filter(InventorySummary.last_updated <= as_of_date)

    # Get all inventory summaries for the target items
    summaries = query.all()

    # Group summaries by item and optionally location
    summaries_by_item = {}
    for summary in summaries:
        item_id = summary.item_id
        if item_id not in summaries_by_item:
            summaries_by_item[item_id] = []
        summaries_by_item[item_id].append(summary)

    result = {}

    for item_id, item_summaries in summaries_by_item.items():
        if kwargs.get('group_by_location'):
            # Group by location
            location_valuation = {}

            for summary in item_summaries:
                loc_id = summary.location_id
                quantity = summary.total_quantity
                value = Decimal(str(summary.total_value))
                avg_cost = Decimal(str(summary.average_cost)) if summary.average_cost else Decimal('0.0')

                # Convert value to target currency if needed
                if currency_id != base_currency_id:  # ← Now base_currency_id is defined!
                    exchange_rate = get_exchange_rate(db_session, base_currency_id, currency_id, app_id)
                    if exchange_rate:
                        value = value * Decimal(str(exchange_rate))
                        avg_cost = avg_cost * Decimal(str(exchange_rate))

                location_valuation[loc_id] = {
                    'quantity': float(quantity),
                    'value': float(value),
                    'average_cost': float(avg_cost)
                }

            result[item_id] = location_valuation

        else:
            # Calculate total for the item across all locations
            total_quantity = 0.0
            total_value = Decimal('0.0')

            for summary in item_summaries:
                total_quantity += summary.total_quantity
                total_value += Decimal(str(summary.total_value))

            # Convert to target currency if needed
            if currency_id != base_currency_id:  # ← Now base_currency_id is defined!
                exchange_rate = get_exchange_rate(db_session, base_currency_id, currency_id, app_id)
                if exchange_rate:
                    total_value = total_value * Decimal(str(exchange_rate))

            if total_quantity > 0:
                avg_cost = total_value / Decimal(str(total_quantity))
            else:
                avg_cost = Decimal('0.0')

            if kwargs.get('group_by_item', True):
                result[item_id] = {
                    'quantity': float(total_quantity),
                    'value': float(total_value),
                    'average_cost': float(avg_cost)
                }
            else:
                return float(total_value)

    # Map back to original item IDs if needed
    if id_type == 'item' and result:
        result = _map_variation_to_item_valuation(db_session, app_id, result)

    return result


def _map_variation_to_item_valuation(db_session, app_id, variation_valuation):
    """
    Map variation-level valuation back to item-level valuation
    """
    item_valuation = {}

    # Get all variation links for the app
    variation_links = db_session.query(
        InventoryItemVariationLink.id,
        InventoryItemVariationLink.inventory_item_id
    ).filter(InventoryItemVariationLink.app_id == app_id).all()

    variation_to_item = {var_id: item_id for var_id, item_id in variation_links}

    for variation_id, valuation_data in variation_valuation.items():
        item_id = variation_to_item.get(variation_id)
        if not item_id:
            continue

        if item_id not in item_valuation:
            item_valuation[item_id] = {
                'quantity': 0.0,
                'value': Decimal('0.0'),
                'average_cost': Decimal('0.0')
            }

        if isinstance(valuation_data, dict) and 'quantity' in valuation_data:
            # Single location or overall valuation
            item_valuation[item_id]['quantity'] += valuation_data['quantity']
            item_valuation[item_id]['value'] += Decimal(str(valuation_data['value']))
        else:
            # Multiple locations
            for loc_id, loc_data in valuation_data.items():
                item_valuation[item_id]['quantity'] += loc_data['quantity']
                item_valuation[item_id]['value'] += Decimal(str(loc_data['value']))

    # Calculate average cost for each item
    for item_id, data in item_valuation.items():
        if data['quantity'] > 0:
            data['average_cost'] = float(data['value'] / Decimal(str(data['quantity'])))
        else:
            data['average_cost'] = 0.0
        data['value'] = float(data['value'])

    return item_valuation


def reverse_inventory_entry(db_session, inventory_entry):
    """
    Reverse an inventory entry by deleting all related transactions and line items
    """


    try:
        total_reversed_cost = 0
        items_to_reverse = []  # Store items for batch reversal

        # First, reverse any ledger transactions if they exist
        transaction_detail_ids = []
        for line_item in inventory_entry.line_items:
            for transaction_detail in line_item.inventory_transaction_details:
                if transaction_detail.is_posted_to_ledger:
                    transaction_detail_ids.append(transaction_detail.id)

        # First pass: collect all reversal data AND delete properly
        for line_item in inventory_entry.line_items:
            # Get all transaction details for this line item
            transaction_details = db_session.query(InventoryTransactionDetail).filter_by(
                inventory_entry_line_item_id=line_item.id
            ).all()

            # Collect reversal data AND delete each transaction detail properly
            for transaction in transaction_details:
                total_reversed_cost += transaction.total_cost
                items_to_reverse.append({
                    'app_id': transaction.app_id,
                    'item_id': transaction.item_id,
                    'location_id': transaction.location_id,
                    'quantity': -transaction.quantity,
                    'total_cost': -transaction.total_cost
                })

                # DELETE PROPERLY using ORM (not bulk delete)
                db_session.delete(transaction)

            # Delete the line item itself
            db_session.delete(line_item)

        # Second pass: execute all reversals in batch
        for item in items_to_reverse:
            update_inventory_summary(
                db_session,
                item['app_id'],
                item['item_id'],
                item['location_id'],
                item['quantity'],
                item['total_cost']
            )


        # Commit the deletions
        db_session.flush()

    except Exception as e:
        logger.error(f"Error reversing inventory entry {inventory_entry.id}: {str(e)}")
        db_session.rollback()
        raise


def remove_inventory_journal_entries(db_session, inventory_entry_id):
    """
    Remove all journals related to an inventory entry
    Returns: (success, message)
    """
    try:
        # Find all journals that have entries linked to this inventory entry
        journals_to_delete = db_session.query(Journal).join(JournalEntry).filter(
            JournalEntry.source_type == "inventory_transaction",
            JournalEntry.source_id.in_(
                db_session.query(InventoryTransactionDetail.id)
                .join(InventoryEntryLineItem)
                .filter(InventoryEntryLineItem.inventory_entry_id == inventory_entry_id)
            )
        ).distinct().all()

        # Delete the journals (entries will cascade)
        for journal in journals_to_delete:
            db_session.delete(journal)
            logger.info(f"Deleted journal ids {journal.id}")

        return True, "Journal entries removed successfully"

    except Exception as e:
        logger.error(f"Error removing inventory journal entries: {str(e)}")
        return False, str(e)


def reverse_single_transaction_detail(db_session, transaction_detail):
    """Reverse a single transaction detail and update inventory summary"""

    from services.post_to_ledger_reversal import reverse_inventory_posting
    # Reverse the inventory impact
    update_inventory_summary(
        db_session,
        transaction_detail.app_id,
        transaction_detail.item_id,
        transaction_detail.location_id,
        -transaction_detail.quantity,  # Reverse quantity
        -transaction_detail.total_cost  # Reverse value
    )

    # Always attempt to reverse ledger posting
    ledger_reverse_result = reverse_inventory_posting(db_session, [transaction_detail.id], current_user)
    if not ledger_reverse_result['success']:
        raise Exception(f"Failed to reverse ledger transaction: {ledger_reverse_result['message']}")


def render_edit_inventory_entry_form(db_session, app_id, company, role, modules_data, inventory_entry):
    """Render the edit inventory entry form with all required data"""
    projects = db_session.query(Project).filter_by(app_id=app_id).all()
    inventory_items = db_session.query(InventoryItemVariationLink).filter_by(app_id=app_id, status="active").all()
    payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
    categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
    subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
    variations = db_session.query(InventoryItemVariation).filter_by(app_id=app_id).all()
    attributes = db_session.query(InventoryItemAttribute).filter_by(app_id=app_id).all()

    # FROM Locations - Only locations user has access to
    from_locations = get_user_accessible_locations(current_user.id, app_id)

    # TO Locations - ALL locations
    all_locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

    brands = db_session.query(Brand).filter_by(app_id=app_id).all()
    vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers', 'customer', 'customers']
    vendor_types = [v.lower() for v in vendor_types]
    employees = db_session.query(Employee).filter_by(app_id=app_id).all()

    suppliers = (
        db_session.query(Vendor)
        .filter(
            Vendor.app_id == app_id,
            func.lower(Vendor.vendor_type).in_(vendor_types)
        )
        .all()
    )
    currencies = db_session.query(Currency).filter_by(app_id=app_id).order_by(Currency.currency_index).all()

    # Get existing attachments for this inventory entry
    from models import Attachment
    attachments = db_session.query(Attachment).filter_by(
        record_type='inventory_entry',
        record_id=inventory_entry.id
    ).all()

    # Prepare edit data (same as before)
    edit_data = {
        'movement_type': inventory_entry.inventory_source,
        'movement_direction': inventory_entry.stock_movement,
        'source_type': inventory_entry.source_type,
        'transaction_date': inventory_entry.transaction_date.strftime(
            '%Y-%m-%d') if inventory_entry.transaction_date else '',
        'supplier_id': inventory_entry.supplier_id,
        'expiration_date': inventory_entry.expiration_date.strftime(
            '%Y-%m-%d') if inventory_entry.expiration_date else '',

        'reference': inventory_entry.reference,
        'notes': inventory_entry.notes,
        'from_location': inventory_entry.from_location,
        'to_location': inventory_entry.to_location,
        'currency_id': inventory_entry.currency_id,
        'payable_account_id': inventory_entry.payable_account_id,
        'write_off_account_id': inventory_entry.write_off_account_id,
        'adjustment_account_id': inventory_entry.adjustment_account_id,
        'sales_account_id': inventory_entry.sales_account_id,
        'version': inventory_entry.version,
        'handled_by': inventory_entry.handled_by,
        'line_items': [
            {
                'item_id': line.item_id,
                'quantity': line.quantity,
                'unit_price': line.unit_price,
                'selling_price': line.selling_price,
                'system_quantity': line.system_quantity,
                'adjustment_amount': line.adjustment_amount,
                'adjusted_quantity': line.adjusted_quantity,
                'project_id': line.project_id,
            }
            for line in inventory_entry.line_items
        ]
    }

    return render_template(
        '/inventory/edit_inventory_entry.html',
        company=company,
        role=role,
        modules=modules_data,
        projects=projects,
        inventory_items=inventory_items,
        categories=categories,
        subcategories=subcategories,
        inventory_variations=variations,
        inventory_attributes=attributes,
        from_locations=from_locations,
        all_locations=all_locations,
        brands=brands,
        suppliers=suppliers,
        currencies=currencies,
        payment_modes=payment_modes,
        edit_data=edit_data,
        inventory_entry=inventory_entry,
        employees=employees,
        attachments=attachments,  # ← ADD THIS
        title="Edit Inventory Entry"
    )


def get_inventory_entry_with_details(db_session, entry_id, app_id, lock=False):
    """
    Get inventory entry with all related details for your data model
    """
    query = db_session.query(InventoryEntry). \
        options(
        joinedload(InventoryEntry.line_items),
        joinedload(InventoryEntry.vendor),
        joinedload(InventoryEntry.project),
        joinedload(InventoryEntry.from_inventory_location),
        joinedload(InventoryEntry.to_inventory_location),
        joinedload(InventoryEntry.currency),
        joinedload(InventoryEntry.payable_account),
        joinedload(InventoryEntry.write_off_account),
        joinedload(InventoryEntry.adjustment_account),
        joinedload(InventoryEntry.sales_account)
    ). \
        filter_by(id=entry_id, app_id=app_id)
    if lock:
        query = query.with_for_update()

    return query.first()


def get_retained_earnings_account(db_session, app_id, user_id=None):
    """
    Fetch or create the retained earnings account for the given company (app_id).

    Logic:
    1. Look for a system retained earnings account.
    2. If not found, ensure the "System Equity" category exists.
    3. Create the retained earnings account under "System Equity" if missing.
    4. Fallback to any equity account or the first account if something is really wrong.
    """
    # 1️⃣ Look for existing system retained earnings account
    retained_earnings_account = db_session.query(ChartOfAccounts).filter(
        ChartOfAccounts.app_id == app_id,
        ChartOfAccounts.sub_category.ilike('%retained earnings%'),
        ChartOfAccounts.is_system_account.is_(True)
    ).first()

    if retained_earnings_account:
        return retained_earnings_account

    # 2️⃣ Ensure System Equity category exists
    equity_category = db_session.query(Category).filter_by(
        app_id=app_id,
        account_type="Equity",
        category="System Equity"
    ).first()

    if not equity_category:
        equity_category = Category(
            app_id=app_id,
            account_type="Equity",
            category="System Equity",
            category_id="SYS-3900"  # system-assigned code
        )
        db_session.add(equity_category)
        db_session.flush()

    # 3️⃣ Create the retained earnings account if it still doesn't exist
    retained_earnings_account = ChartOfAccounts(
        app_id=app_id,
        parent_account_type="Equity",
        parent_account_type_id="3",
        category="System Equity",
        category_id=equity_category.category_id,
        category_fk=equity_category.id,
        sub_category="Retained Earnings",
        sub_category_id="SYS-3901",
        normal_balance="Credit",
        is_active=True,
        is_system_account=True,
        created_by=user_id
    )
    db_session.add(retained_earnings_account)
    db_session.flush()

    return retained_earnings_account


def suggest_next_pos_order_reference(db_session):
    """
    Suggest the next POS order reference based on the most recent reference.
    Format: pos_yy_mm_dd_00001 (e.g., pos_25_03_15_00001 for March 15, 2025)
    """
    app_id = current_user.app_id
    today = datetime.datetime.now()
    today_prefix = f"pos_{today.strftime('%y_%m_%d')}_"

    # Get the last entered POS order reference for today (looking for today's prefix)
    last_entry = db_session.query(DirectSalesTransaction.sale_reference).filter(
        DirectSalesTransaction.app_id == app_id,
        DirectSalesTransaction.sale_reference.isnot(None),
        DirectSalesTransaction.sale_reference.like(f'{today_prefix}%')
    ).order_by(DirectSalesTransaction.id.desc()).first()

    # If no previous reference exists for today, start from 00001
    if not last_entry or not last_entry.sale_reference:
        return f"{today_prefix}00001"

    last_ref = last_entry.sale_reference.strip()

    # Check if it ends with a number (the sequence part)
    match = re.search(r'(\d+)$', last_ref)
    if match:
        number_str = match.group(1)
        number_len = len(number_str)  # Preserve digit length (should be 5)
        number = int(number_str) + 1
        new_ref = last_ref[:match.start(1)] + str(number).zfill(number_len)
    else:
        # If no number found, start from 00001
        new_ref = f"{today_prefix}00001"

    return new_ref


def assign_user_to_location(user_id, location_id, app_id, assigned_by, role='staff'):
    """Assign a user to a location - update if exists, create if not"""
    with Session() as db_session:
        # Check if assignment already exists
        existing_assignment = db_session.query(UserLocationAssignment).filter_by(
            user_id=user_id,
            location_id=location_id,
            app_id=app_id
        ).first()

        if existing_assignment:
            # Update existing record
            existing_assignment.is_active = True
            existing_assignment.role_at_location = role
            existing_assignment.assigned_by = assigned_by
            db_session.commit()
            return existing_assignment
        else:
            # Create new record
            assignment = UserLocationAssignment(
                user_id=user_id,
                location_id=location_id,
                app_id=app_id,
                assigned_by=assigned_by,
                role_at_location=role,
                is_active=True
            )
            db_session.add(assignment)
            db_session.commit()
            return assignment


def get_user_accessible_locations(user_id, app_id):
    """Get all locations accessible to a user"""
    with Session() as db_session:
        locations = db_session.query(InventoryLocation).join(
            UserLocationAssignment
        ).filter(
            UserLocationAssignment.user_id == user_id,
            UserLocationAssignment.app_id == app_id,
            UserLocationAssignment.is_active == True
        ).all()
        return locations


def can_user_access_location(user_id, location_id, app_id):
    """Check if a user has access to a specific location"""
    with Session() as db_session:
        assignment = db_session.query(UserLocationAssignment).filter(
            UserLocationAssignment.user_id == user_id,
            UserLocationAssignment.location_id == location_id,
            UserLocationAssignment.app_id == app_id,
            UserLocationAssignment.is_active == True
        ).first()
        return assignment is not None


def get_location_users(location_id, app_id):
    """Get all users assigned to a location"""
    with Session() as db_session:
        users = db_session.query(User).join(
            UserLocationAssignment
        ).filter(
            UserLocationAssignment.location_id == location_id,
            UserLocationAssignment.app_id == app_id,
            UserLocationAssignment.is_active == True
        ).all()
        return users


def deactivate_user_location_assignment(user_id, location_id, app_id):
    """Deactivate a user's assignment to a location"""
    with Session() as db_session:
        assignment = db_session.query(UserLocationAssignment).filter(
            UserLocationAssignment.user_id == user_id,
            UserLocationAssignment.location_id == location_id,
            UserLocationAssignment.app_id == app_id
        ).first()

        if assignment:
            assignment.is_active = False
            db_session.commit()
            return True
        return False


def update_user_location_role(user_id, location_id, app_id, new_role):
    """Update a user's role at a location"""
    with Session() as db_session:
        assignment = db_session.query(UserLocationAssignment).filter(
            UserLocationAssignment.user_id == user_id,
            UserLocationAssignment.location_id == location_id,
            UserLocationAssignment.app_id == app_id,
            UserLocationAssignment.is_active == True
        ).first()

        if assignment:
            assignment.role_at_location = new_role
            db_session.commit()
            return True
        return False


def get_user_location_assignments(user_id, app_id):
    """Get all location assignments for a user"""
    with Session() as db_session:
        assignments = db_session.query(UserLocationAssignment).filter(
            UserLocationAssignment.user_id == user_id,
            UserLocationAssignment.app_id == app_id,
            UserLocationAssignment.is_active == True
        ).all()
        return assignments


def get_user_locations_with_roles(user_id, app_id):
    """Get all locations with roles for a user"""
    with Session() as db_session:
        assignments = db_session.query(
            InventoryLocation,
            UserLocationAssignment.role_at_location
        ).join(
            UserLocationAssignment,
            InventoryLocation.id == UserLocationAssignment.location_id
        ).filter(
            UserLocationAssignment.user_id == user_id,
            UserLocationAssignment.app_id == app_id,
            UserLocationAssignment.is_active == True
        ).all()
        return assignments


def validate_user_location_access(location_id, user_id, app_id):
    """Validate that user has access to the specified location"""
    return can_user_access_location(user_id, location_id, app_id)


def safe_clear_stock_history_cache(logger=None):
    """
    Safely clear stock history cache without interrupting main operations.
    Logs success or failure if a logger is provided.

    Args:
        logger (logging.Logger, optional): Logger instance for logging events.
    """
    try:
        clear_stock_history_cache()
        if logger:
            logger.info("✅ Successfully cleared stock history cache after inventory operation")
    except Exception as e:
        if logger:
            logger.error(f"⚠️ Cache clearing failed: {e}")
        # Don't raise the exception — allow main operation to continue


def get_item_details(item):
    """Returns (item_name_with_variation, description) with guaranteed fallbacks"""
    # Primary path - when item_name exists
    if hasattr(item, 'item_name') and item.item_name:
        item_name = item.item_name
        variation_name = ""

        # Check for variation
        if hasattr(item, 'inventory_item_variation_link') and item.inventory_item_variation_link:
            if hasattr(item.inventory_item_variation_link, 'inventory_item_variation'):
                variation = item.inventory_item_variation_link.inventory_item_variation
                if variation and hasattr(variation, 'variation_name') and variation.variation_name:
                    variation_name = variation.variation_name

        # Combine item name with variation if exists
        if variation_name:
            full_name = f"{item_name} - {variation_name}"
        else:
            full_name = item_name

        return (
            full_name,
            item.description if hasattr(item, 'description') and item.description else "-"
        )

    # Secondary path - inventory item fallback
    try:
        if (hasattr(item, 'inventory_item_variation_link') and
                item.inventory_item_variation_link and
                hasattr(item.inventory_item_variation_link, 'inventory_item')):
            inv_item = item.inventory_item_variation_link.inventory_item
            item_name = inv_item.item_name if hasattr(inv_item, 'item_name') and inv_item.item_name else "-"
            variation_name = ""

            # Get variation if exists
            if hasattr(item.inventory_item_variation_link, 'inventory_item_variation'):
                variation = item.inventory_item_variation_link.inventory_item_variation
                if variation and hasattr(variation, 'variation_name') and variation.variation_name:
                    variation_name = variation.variation_name

            # Combine item name with variation if exists
            if variation_name:
                full_name = f"{item_name} - {variation_name}"
            else:
                full_name = item_name

            return (
                full_name,
                inv_item.item_description if hasattr(inv_item,
                                                     'item_description') and inv_item.item_description else "-"
            )
    except AttributeError:
        pass

    # Final fallback
    return "-", "-"


def process_attachments(request, record_type, record_id, current_user_id, db_session, existing_attachments=None):
    """
    Process attachments for a record (inventory_entry, asset_movement, etc.)

    Args:
        request: The Flask request object
        record_type: String - 'inventory_entry', 'asset_movement', etc.
        record_id: Integer - The ID of the record to attach files to
        current_user_id: Integer - ID of the user uploading
        db_session: The database session (passed from the route)
        existing_attachments: List of existing Attachment objects (for edit mode)

    Returns:
        tuple: (new_attachments_count, deleted_attachments_count)
    """
    new_count = 0
    deleted_count = 0

    try:
        # Handle deleted attachments (for edit mode)
        deleted_ids = request.form.get('deleted_attachments', '')
        if deleted_ids:
            deleted_ids_list = [int(id) for id in deleted_ids.split(',') if id.strip().isdigit()]
            for attachment_id in deleted_ids_list:
                attachment = db_session.query(Attachment).filter_by(id=attachment_id).first()
                if attachment and attachment.record_type == record_type and attachment.record_id == record_id:
                    # Delete the physical file
                    if os.path.exists(attachment.file_path):
                        os.remove(attachment.file_path)
                    # Delete the database record
                    db_session.delete(attachment)
                    deleted_count += 1

        # Handle new attachments
        uploaded_files = request.files.getlist('attachments[]')
        attachment_descriptions = request.form.getlist('attachment_description[]')

        # Create upload directory if it doesn't exist
        upload_folder = current_app.config['UPLOAD_FOLDER_ATTACHMENTS']
        os.makedirs(upload_folder, exist_ok=True)

        for index, file in enumerate(uploaded_files):
            if file and file.filename:
                # Check file size (max 10MB)
                file.seek(0, 2)
                file_size = file.tell()
                file.seek(0)

                if file_size > 10 * 1024 * 1024:
                    current_app.logger.warning(f"File {file.filename} exceeds 10MB limit. Skipped.")
                    continue

                # Generate unique filename
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')
                unique_filename = f"{record_type}_{record_id}_{timestamp}_{file.filename}"

                # Save file
                file_path = os.path.join(upload_folder, unique_filename)
                file.save(file_path)

                # Get file extension
                file_ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''

                # Get description
                description = attachment_descriptions[index] if index < len(attachment_descriptions) else ''

                # Create attachment record (NO commit here)
                attachment = Attachment(
                    record_type=record_type,
                    record_id=record_id,
                    filename=unique_filename,
                    original_filename=file.filename,
                    file_path=file_path,
                    file_size=file_size,
                    file_type=file_ext,
                    description=description,
                    uploaded_by=current_user_id
                )
                db_session.add(attachment)
                new_count += 1

        # NO commit here - let the main route handle it

    except Exception as e:
        current_app.logger.error(f"Error processing attachments: {str(e)}")
        raise

    return new_count, deleted_count
