# app/routes/inventory/inventory_items.py
import os
import traceback
import uuid
from decimal import Decimal

import sqlalchemy
from flask import Blueprint, jsonify, render_template, flash, redirect, url_for, request, current_app
from flask_login import login_required, current_user
from sqlalchemy import func, case
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
from sqlalchemy import or_
from werkzeug.utils import secure_filename

from decorators import role_required, require_permission, cached_route
from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Project, ChartOfAccounts, PaymentMode, Category, Department, Employee, \
    PayrollPeriod, InventoryCategory, InventorySubCategory, InventoryLocation, InventoryItem, InventoryItemVariation, \
    InventoryItemAttribute, Brand, InventoryItemVariationLink, UnitOfMeasurement, Vendor, InventoryEntry, \
    InventoryEntryLineItem, InventorySummary, ItemSellingPrice, InventoryTransactionDetail
import logging

from services.inventory_helpers import calculate_inventory_quantities, get_last_purchase_price, \
    get_weighted_average_cost, calculate_inventory_valuation, safe_clear_stock_history_cache, \
    get_user_accessible_locations
from utils import empty_to_none, calculate_net_quantity
from utils_and_helpers.amounts_utils import format_amount
from utils_and_helpers.cache_keys import inventory_items_cache_key
from utils_and_helpers.file_utils import allowed_file, is_file_size_valid, file_exists, generate_unique_filename
from . import inventory_bp

logger = logging.getLogger(__name__)


@inventory_bp.route('/add_new_inventory_item', methods=['GET', 'POST'])
@role_required(['Admin', 'Supervisor', 'Contributor'])
@login_required
def add_new_inventory_item():
    db_session = Session()
    app_id = current_user.app_id
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

    try:
        if request.method == 'POST':

            try:
                # Retrieve form data
                item_name = request.form.get('item_name').strip()
                item_code_str = request.form.get('item_code2').strip() or None
                item_code = empty_to_none(item_code_str)
                item_category_id = request.form.get('category')
                item_subcategory_id = request.form.get('subcategory')
                brand_id_str = request.form.get('brand_id') or None
                brand_id = empty_to_none(brand_id_str)
                description = request.form.get('description') or None
                reorder_point = request.form.get('reorder_point') if request.form.get('reorder_point') else 0

                uom_id = request.form.get('uom')

                # Handle file upload
                file = request.files.get('item_image')
                image_filename = None

                if file and allowed_file(file.filename):
                    if is_file_size_valid(file):
                        filename, file_extension = os.path.splitext(file.filename)
                        unique_filename = f"{app_id}_{uuid.uuid4()}{file_extension}"
                        image_filename = secure_filename(unique_filename)
                        file_path = os.path.join(current_app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)

                        if file_exists(file_path):
                            image_filename = generate_unique_filename(current_app.config['UPLOAD_FOLDER_INVENTORY'],
                                                                      image_filename)

                            file_path = os.path.join(current_app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)

                        try:
                            file.save(file_path)
                        except Exception as e:
                            logger.warning(f"Error saving file: {e}")
                            return jsonify({'success': False, 'message': 'Error saving file'}), 500
                    else:
                        return jsonify(
                            {'success': False,
                             'message': 'File size exceeds the maximum limit or invalid file type'}), 400

                # Create new inventory item
                new_inventory_item = InventoryItem(
                    item_name=item_name,
                    item_code=item_code,
                    brand_id=brand_id,
                    item_category_id=item_category_id,
                    item_subcategory_id=item_subcategory_id,
                    reorder_point=reorder_point,
                    uom_id=uom_id,
                    image_filename=image_filename,
                    item_description=description,
                    app_id=app_id
                )
                db_session.add(new_inventory_item)
                db_session.flush()  # Get the ID before commit

                # Process variations if they exist
                attribute_ids = request.form.getlist('attribute_ids[]')
                variation_ids = request.form.getlist('variation_ids[]')
                variations_added = False

                # Check for duplicate variations before creating any
                for attr_id, var_id in zip(attribute_ids, variation_ids):
                    if attr_id and var_id:  # Only check if both are provided
                        # Check if this variation already exists for this item
                        existing_variation = db_session.query(InventoryItemVariationLink).filter_by(
                            inventory_item_id=new_inventory_item.id,
                            attribute_id=attr_id,
                            inventory_item_variation_id=var_id
                        ).first()

                        if existing_variation:
                            db_session.rollback()
                            attribute_name = db_session.query(InventoryItemAttribute).get(attr_id).attribute_name
                            variation_name = db_session.query(InventoryItemVariation).get(var_id).variation_name
                            return jsonify({
                                'success': False,
                                'message': f'This item already has a variation with "{attribute_name}: {variation_name}"'
                            }), 400

                # Create variation links if no duplicates found
                if not attribute_ids or not variation_ids or all(attr == '' for attr in attribute_ids) or all(
                        var == '' for var in variation_ids):
                    # Create default variation link if no variations provided
                    variation_link = InventoryItemVariationLink(
                        inventory_item_id=new_inventory_item.id,
                        attribute_id=None,
                        inventory_item_variation_id=None,
                        status="active",
                        app_id=app_id
                    )
                    db_session.add(variation_link)
                else:
                    # Create all valid variation links
                    for attr_id, var_id in zip(attribute_ids, variation_ids):
                        if attr_id and var_id:  # Only create if both are provided
                            variation_link = InventoryItemVariationLink(
                                inventory_item_id=new_inventory_item.id,
                                attribute_id=attr_id,
                                inventory_item_variation_id=var_id,
                                status="active",
                                app_id=app_id
                            )
                            db_session.add(variation_link)
                            variations_added = True

                db_session.commit()

                flash("New Item has been added successfully", "success")

                return redirect(url_for("inventory.add_new_inventory_item"))

            except IntegrityError as e:
                db_session.rollback()
                error_msg = f'Error adding item to database: {str(e)} \n{traceback.format_exc()}'
                logger.error(error_msg)
                error_msg = f'Item Code: "{item_code}" is already in use. Please use a unique item code.'

                flash(f'{error_msg}', "error")
                return redirect(url_for("inventory.add_new_inventory_item"))

            except Exception as e:
                db_session.rollback()
                error_msg = f'Error adding item to database: {str(e)} \n{traceback.format_exc()}'
                logger.error(error_msg)
                flash(f'{error_msg}', "error")
                return redirect(url_for("inventory.add_new_inventory_item"))

        else:
            # GET request - render the form
            categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
            subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
            brands = db_session.query(Brand).filter_by(app_id=app_id).all()
            attributes = db_session.query(InventoryItemAttribute).filter_by(app_id=app_id).all()
            variations = db_session.query(InventoryItemVariation).filter_by(app_id=app_id).all()
            projects = db_session.query(Project).filter_by(app_id=app_id).all()
            locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
            uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()
            suppliers = db_session.query(Vendor).filter_by(app_id=app_id).all()
            currencies = db_session.query(Currency).filter_by(app_id=app_id).order_by(Currency.currency_index).all()

            return render_template(
                '/inventory/new_inventory_item.html',
                company=company,
                role=role,
                modules=modules_data,
                categories=categories,
                subcategories=subcategories,
                brands=brands,
                inventory_attributes=attributes,
                inventory_variations=variations,
                projects=projects,
                locations=locations,
                uoms=uoms,
                suppliers=suppliers,
                currencies=currencies,
                title="Add Inventory Item"
            )

    except Exception as e:
        db_session.rollback()
        error_msg = f'Unexpected error in inventory_entry: {str(e)}'
        logger.error(error_msg)
        flash(f'{error_msg}', "error")
        return redirect(request.referrer) or redirect(url_for("inventory.add_new_inventory_item"))

    finally:
        db_session.close()


@inventory_bp.route('/inventory_item/<int:item_id>', methods=['GET', 'POST'])
@login_required
def inventory_item_details(item_id):
    db_session = Session()
    app_id = current_user.app_id
    company = db_session.query(Company).filter_by(id=app_id).first()
    api_key = company.api_key
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

    try:
        item = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()
        if not item:
            flash('Inventory item not found.', 'error')
            return redirect(url_for('inventory.inventory_entries'))

        if request.method == 'POST':
            # Handle basic item info
            item_name = request.form['item_name']
            item_code_str = request.form['item_code']
            item_code = empty_to_none(item_code_str)
            item_category_id = request.form.get('category')
            item_subcategory_id = request.form.get('subcategory')
            brand_id_str = request.form.get('brand_id') or None
            brand_id = empty_to_none(brand_id_str)
            description = request.form.get('description') or None
            reorder_point = request.form['reorder_point']

            # Accounting fields

            uom_id = request.form.get('uom')

            # Validate required accounting fields
            if not uom_id:
                flash('All accounting fields are required: COGS Account, Asset Account, Sales Account, UoM.', 'error')
                return redirect(url_for('inventory.inventory_item_details', item_id=item_id))

            # Handle file upload
            file = request.files.get('item_image')
            if file and allowed_file(file.filename):
                if is_file_size_valid(file):
                    filename, file_extension = os.path.splitext(file.filename)
                    unique_filename = f"{app_id}_{uuid.uuid4()}{file_extension}"
                    image_filename = secure_filename(unique_filename)
                    file_path = os.path.join(current_app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)

                    if file_exists(file_path):
                        image_filename = generate_unique_filename(current_app.config['UPLOAD_FOLDER_INVENTORY'],
                                                                  image_filename)
                        file_path = os.path.join(current_app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)

                    try:
                        file.save(file_path)
                        logger.info(f"File saved successfully: {file_path}")
                    except Exception as e:
                        logger.error(f"Error saving file: {e}")
                        return jsonify({'success': False, 'message': 'Error saving file'}), 500
                else:
                    logger.warning("File size exceeds the maximum limit or invalid file type")
                    return jsonify(
                        {'success': False, 'message': 'File size exceeds the maximum limit or invalid file type'}), 400
            else:
                image_filename = item.image_filename if item and item.image_filename else None

            # Update item details
            item.item_name = item_name
            item.item_code = item_code
            item.item_category_id = item_category_id
            item.item_subcategory_id = item_subcategory_id
            item.brand_id = brand_id
            item.item_description = description
            item.reorder_point = reorder_point
            item.image_filename = image_filename

            item.uom_id = uom_id

            # Handle attributes and variations
            attribute_ids = request.form.getlist('attribute_ids[]')
            variation_ids = request.form.getlist('variation_ids[]')
            existing_pair_ids = request.form.getlist('existing_pair_ids[]')

            # Get existing pairs
            existing_pairs = {pair.id: pair for pair in item.inventory_item_variation_link}

            # Track pairs to process
            pairs_to_delete = []
            pairs_to_update = []
            new_pairs = []
            pairs_with_inventory = []

            # Process each submitted row
            for idx, pair_id in enumerate(existing_pair_ids):
                attribute_id = attribute_ids[idx] if idx < len(attribute_ids) else None
                variation_id = variation_ids[idx] if idx < len(variation_ids) else None

                if pair_id == 'new':
                    # New pair to add
                    if attribute_id and variation_id:
                        new_pairs.append((attribute_id, variation_id))
                else:
                    # Check if marked for deletion
                    delete_marked = request.form.get(f'delete_pair_{pair_id}') == '1'

                    if delete_marked:
                        # Mark for deletion
                        pairs_to_delete.append(int(pair_id))
                    else:
                        # Keep or update this pair
                        pair = existing_pairs.get(int(pair_id))
                        if pair:
                            # Check if values changed
                            if str(pair.attribute_id) != attribute_id or str(
                                    pair.inventory_item_variation_id) != variation_id:
                                pairs_to_update.append((int(pair_id), attribute_id, variation_id))

            # Validate deletions - check if they have inventory
            for pair_id in pairs_to_delete:
                pair = existing_pairs.get(pair_id)
                if pair:
                    # Check if there are inventory records for this variation
                    has_inventory = db_session.query(InventorySummary).filter_by(
                        app_id=app_id, item_id=pair.id
                    ).first() is not None

                    if has_inventory:
                        pairs_with_inventory.append(pair_id)

            if pairs_with_inventory:
                # Format error message for user
                error_message = f"Cannot delete {len(pairs_with_inventory)} attribute-variation pair(s) because they have existing inventory. "
                error_message += "Please transfer or sell all inventory before deleting."
                flash(error_message, 'error')
                db_session.rollback()
                return redirect(url_for('inventory.inventory_item_details', item_id=item_id))

            # Delete marked pairs
            for pair_id in pairs_to_delete:
                pair = existing_pairs.get(pair_id)
                if pair:
                    db_session.delete(pair)
                    logger.info(f"Deleted variation pair {pair_id}")

            # Update existing pairs
            for pair_id, attribute_id, variation_id in pairs_to_update:
                pair = existing_pairs.get(pair_id)
                if pair:
                    if attribute_id:
                        pair.attribute_id = int(attribute_id)
                    if variation_id:
                        pair.inventory_item_variation_id = int(variation_id)
                    logger.info(f"Updated variation pair {pair_id}")

            # Add new pairs
            for attribute_id, variation_id in new_pairs:
                new_pair = InventoryItemVariationLink(
                    app_id=app_id,
                    inventory_item_id=item.id,
                    attribute_id=int(attribute_id),
                    inventory_item_variation_id=int(variation_id)
                )
                db_session.add(new_pair)
                logger.info(f"Added new variation pair: attribute {attribute_id}, variation {variation_id}")

            try:
                db_session.commit()
                logger.info(f"Inventory item updated successfully: {item_id}")
                flash('Inventory item updated successfully!', 'success')
                return redirect(url_for('inventory.inventory_item_details', item_id=item.id))

            except sqlalchemy.exc.IntegrityError as e:
                db_session.rollback()
                logger.error(f"Database integrity error: {e}")

                # Check if it's a foreign key constraint error
                if "foreign key constraint" in str(e).lower() or "null constraint" in str(e).lower():
                    flash(
                        "Cannot update item. One or more attribute-variation pairs are being used in other records (e.g., backorders, inventory entries). Please resolve these references first.",
                        'error')
                else:
                    flash("Database error occurred. Please try again or contact support.", 'error')

                return redirect(url_for('inventory.inventory_item_details', item_id=item_id))

        # GET REQUEST - UPDATED FOR NEW STRUCTURE
        # GET REQUEST - UPDATED FOR NEW STRUCTURE
        # Fetch categories, subcategories, attributes, variations etc.
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
        subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
        attributes = db_session.query(InventoryItemAttribute).filter_by(app_id=app_id).all()
        variations = db_session.query(InventoryItemVariation).filter_by(app_id=app_id).all()
        locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
        inventory_variations = variations

        location_names = {loc.id: loc.location for loc in locations}

        # Get inventory quantities directly from InventorySummary table
        grouped_entries = {}

        # Get all variation link IDs for this item
        variation_links = db_session.query(InventoryItemVariationLink).filter_by(
            app_id=app_id, inventory_item_id=item_id
        ).options(
            joinedload(InventoryItemVariationLink.inventory_item_attributes),
            joinedload(InventoryItemVariationLink.inventory_item_variation)
        ).all()

        for variation_link in variation_links:
            # Get summary data for this variation
            summaries = db_session.query(InventorySummary).filter_by(
                app_id=app_id, item_id=variation_link.id
            ).options(joinedload(InventorySummary.location)).all()

            attribute_name = variation_link.inventory_item_attributes.attribute_name if variation_link.inventory_item_attributes else None
            variation_name = variation_link.inventory_item_variation.variation_name if variation_link.inventory_item_variation else None

            if attribute_name not in grouped_entries:
                grouped_entries[attribute_name] = {}

            if variation_name not in grouped_entries[attribute_name]:
                grouped_entries[attribute_name][variation_name] = {}

            for summary in summaries:
                location_name = summary.location.location if summary.location else None
                if location_name not in grouped_entries[attribute_name][variation_name]:
                    grouped_entries[attribute_name][variation_name][location_name] = {
                        'quantity': summary.total_quantity,
                        'average_cost': summary.average_cost if summary.average_cost else 0,
                        'total_value': summary.total_value if summary.total_value else 0
                    }

        # Get selling prices for each variation link
        selling_prices_dict = {}
        variation_link_ids = [link.id for link in variation_links]

        if variation_link_ids:
            selling_prices = db_session.query(ItemSellingPrice).options(
                joinedload(ItemSellingPrice.currency),
                joinedload(ItemSellingPrice.customer_group)
            ).filter(
                ItemSellingPrice.inventory_item_variation_link_id.in_(variation_link_ids),
                ItemSellingPrice.app_id == app_id,
                ItemSellingPrice.is_active == True
            ).all()

            # Group selling prices by variation link ID
            for price in selling_prices:
                if price.inventory_item_variation_link_id not in selling_prices_dict:
                    selling_prices_dict[price.inventory_item_variation_link_id] = []
                selling_prices_dict[price.inventory_item_variation_link_id].append({
                    'id': price.id,
                    'selling_price': float(price.selling_price),
                    'currency_code': price.currency.user_currency if price.currency else 'USD',
                    'min_quantity': float(price.min_quantity),
                    'max_quantity': float(price.max_quantity) if price.max_quantity else None,
                    'customer_group_name': price.customer_group.name if price.customer_group else 'All Customers',
                    'effective_from': price.effective_from.isoformat() if price.effective_from else None,
                    'effective_to': price.effective_to.isoformat() if price.effective_to else None
                })

        # CORRECTED: Process entries with proper percentage calculation
        processed_entries = []
        total_value = 0
        total_quantity = 0

        # FIRST PASS: Calculate totals
        for attribute, variations in grouped_entries.items():
            for variation, locations in variations.items():
                for location, data in locations.items():
                    if isinstance(data, dict):
                        item_value = data.get('total_value', 0)
                        item_quantity = data.get('quantity', 0)
                        total_value += item_value
                        total_quantity += item_quantity

        # SECOND PASS: Calculate percentages (after totals are known)
        for attribute, variations in grouped_entries.items():
            for variation, locations in variations.items():
                for location, data in locations.items():
                    if isinstance(data, dict):
                        item_value = data.get('total_value', 0)
                        item_quantity = data.get('quantity', 0)

                        # Find the corresponding variation link ID
                        variation_link_id = None
                        for link in variation_links:
                            if (
                                    link.inventory_item_attributes and link.inventory_item_attributes.attribute_name == attribute) and \
                                    (
                                            link.inventory_item_variation and link.inventory_item_variation.variation_name == variation):
                                variation_link_id = link.id
                                break

                        processed_entries.append({
                            'attribute': attribute,
                            'variation': variation,
                            'location': location,
                            'quantity': item_quantity,
                            'avg_cost': data.get('average_cost'),
                            'value': item_value,
                            'percentage': (item_value / total_value * 100) if total_value > 0 else 0,
                            'selling_prices': selling_prices_dict.get(variation_link_id, [])
                        })

        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()
        brands = db_session.query(Brand).filter_by(app_id=app_id).all()
        # Get accounting accounts for dropdowns
        chart_of_accounts = db_session.query(ChartOfAccounts).filter_by(app_id=app_id).all()

        return render_template(
            'inventory/inventory_item_details.html',
            item=item,
            item_category_name=item.inventory_category.category_name if item.inventory_category else None,
            item_subcategory_name=item.inventory_subcategory.subcategory_name if item.inventory_subcategory else None,
            categories=categories,
            subcategories=subcategories,
            modules=modules_data,
            uoms=uoms,
            brands=brands,
            processed_entries=processed_entries,  # Pass the processed data
            total_value=total_value,
            total_quantity=total_quantity,
            inventory_attributes=attributes,
            inventory_variations=inventory_variations,
            company=company,
            role=role,
            module_name="Inventory",
            api_key=api_key,
            chart_of_accounts=chart_of_accounts
        )

    except Exception as e:
        logger.error(f"Error in inventory_item_details route: {e}\n{traceback.format_exc()}")
        flash('An error occurred while processing your request.', 'error')
        return redirect(url_for('inventory.inventory_items'))
    finally:
        db_session.close()


@inventory_bp.route('/discontinue_item', methods=['POST'])
@login_required
def discontinue_item():
    db_session = Session()
    try:
        data = request.get_json()
        item_id = data.get('item_id')
        app_id = current_user.app_id

        # Validate item_id
        if not item_id:
            return jsonify({'success': False, 'error': 'Item ID is required.'}), 400

        # Fetch the item from the database
        item = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()
        if not item:
            return jsonify({'success': False, 'error': 'Item not found.'}), 404

        # Check if the item is already discontinued
        if item.status == 'discontinued':
            return jsonify({'success': False, 'error': 'Item is already discontinued.'}), 400

        # Check if the item has 0 quantities in all locations using optimized function
        quantities = calculate_inventory_quantities(
            db_session=db_session,
            app_id=app_id,
            item_ids=[item_id],
            use_variation_ids=False,
            group_by_location=False
        )

        net_quantity = quantities.get(item_id, 0)

        # Check if the item has 0 quantities across all locations
        if net_quantity > 0:
            return jsonify({'success': False,
                            'error': f'Item cannot be discontinued. {net_quantity} quantities still available across all locations.'}), 400

        # Update the item status to "discontinued"
        item.status = 'discontinued'
        safe_clear_stock_history_cache(logger)
        db_session.commit()

        return jsonify({'success': True, 'message': 'Item discontinued successfully.'})

    except IntegrityError as e:
        db_session.rollback()
        logger.error(f"Integrity error while discontinuing item {item_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': 'Cannot discontinue item. It is currently in use by other records in the system.'
        }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error discontinuing item {item_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)}), 500

    finally:
        db_session.close()


@inventory_bp.route('/delete_item', methods=['POST'])
@login_required
def delete_item():
    db_session = Session()
    try:
        data = request.get_json()
        item_id = data.get('item_id')
        app_id = current_user.app_id
        logger.info(f'Delete item request: {data}')

        # Validate item_id
        if not item_id:
            return jsonify({'success': False, 'error': 'Item ID is required.'}), 400

        # Fetch the item from the database
        item = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()
        if not item:
            return jsonify({'success': False, 'error': 'Item not found.'}), 404

        # Check if the item has 0 quantities in all locations using optimized function
        quantities = calculate_inventory_quantities(
            db_session=db_session,
            app_id=app_id,
            item_ids=[item_id],
            use_variation_ids=False,
            group_by_location=False
        )

        net_quantity = quantities.get(item_id, 0)

        # Check if the item has 0 quantities across all locations
        if net_quantity > 0:
            return jsonify({'success': False,
                            'error': f'Item cannot be deleted. {net_quantity} quantities still available across all locations.'}), 400

        # First, check and delete all inventory variation links for this item
        variation_links = db_session.query(InventoryItemVariationLink).filter(
            InventoryItemVariationLink.inventory_item_id == item_id,
            InventoryItemVariationLink.app_id == app_id
        ).all()

        # Delete all variation links
        for variation_link in variation_links:
            db_session.delete(variation_link)

        # Now delete the main item
        db_session.delete(item)
        safe_clear_stock_history_cache(logger)
        db_session.commit()

        return jsonify({'success': True, 'message': 'Item deleted successfully.'})

    except IntegrityError as e:
        db_session.rollback()
        logger.error(f"Integrity error while deleting item {item_id}: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'error': 'Cannot delete item. It is currently in use by other records in the system.'
        }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error deleting item {item_id}: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

    finally:
        db_session.close()


@inventory_bp.route('/reactivate_item', methods=['POST'])
@login_required
def reactivate_item():
    db_session = Session()
    try:
        data = request.get_json()
        item_id = data.get('item_id')
        app_id = current_user.app_id

        item = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()
        if not item:
            return jsonify({'success': False, 'error': 'Item not found'})

        item.status = 'active'
        db_session.commit()

        return jsonify({'success': True, 'message': 'Item reactivated successfully'})

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error reactivating item: {e}")
        return jsonify({'success': False, 'error': 'An error occurred while reactivating the item'})
    finally:
        db_session.close()


@inventory_bp.route('/inventory_items')
@login_required
def inventory_items():
    """
    Render the inventory items page with all necessary data
    """
    db_session = Session()

    try:
        # Fetch all necessary data for the template
        categories = db_session.query(InventoryCategory).filter_by(app_id=current_user.app_id).all()
        subcategories = db_session.query(InventorySubCategory).filter_by(app_id=current_user.app_id).all()
        brands = db_session.query(Brand).filter_by(app_id=current_user.app_id).all()
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        return render_template(
            'inventory/inventory_items.html',  # Your template file name
            categories=categories,
            subcategories=subcategories,
            brands=brands,
            company=company,
            modules=modules_data,
            role=role  # Assuming your user model has a role attribute
        )

    except Exception as e:
        logger.error(f"Error rendering inventory page: {str(e)}\n{traceback.format_exc()}")
        return "An error occurred while loading the inventory page", 500

    finally:
        db_session.close()


@inventory_bp.route('/api/inventory_items', methods=['GET'])
@login_required
@cached_route(timeout=300, key_func=inventory_items_cache_key)
def api_inventory_items():
    """
    Return JSON data of inventory items with accounting information and selling prices
    Filtered by user's assigned locations
    """
    db_session = Session()
    app_id = current_user.app_id
    user_id = current_user.id

    try:
        # Get pagination parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        status = request.args.get('status', 'active')
        location_id = request.args.get('location_id', type=int)
        include_prices = request.args.get('include_prices', 'false').lower() == 'true'
        description_search_term = request.args.get('description_search', '').strip()
        search_term = request.args.get('q', '')
        category_id = request.args.get('category_id', type=int)
        subcategory_id = request.args.get('subcategory_id', type=int)
        brand_id = request.args.get('brand_id', type=int)
        sort_by = request.args.get('sort_by', 'item_name')
        sort_order = request.args.get('sort_order', 'asc')

        # ===== GET USER'S ACCESSIBLE LOCATIONS =====
        user_locations = get_user_accessible_locations(user_id, app_id)
        user_location_ids = [loc.id for loc in user_locations] if user_locations else []

        # If user has no location access, return empty results
        if not user_location_ids:
            return jsonify({
                'success': True,
                'items': [],
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_pages': 0,
                    'total_items': 0,
                    'has_next': False,
                    'has_prev': False
                }
            })

        # Build base query
        query = db_session.query(InventoryItem).filter_by(app_id=app_id)

        # Apply search filters
        if description_search_term:
            query = query.filter(
                or_(
                    InventoryItem.item_name.ilike(f'%{description_search_term}%'),
                    InventoryItem.item_code.ilike(f'%{description_search_term}%'),
                    InventoryItem.item_description.ilike(f'%{description_search_term}%')
                )
            )
        elif search_term:
            query = query.filter(
                or_(
                    InventoryItem.item_name.ilike(f'%{search_term}%'),
                    InventoryItem.item_code.ilike(f'%{search_term}%')
                )
            )

        if category_id:
            query = query.filter_by(item_category_id=category_id)

        if subcategory_id:
            query = query.filter_by(item_subcategory_id=subcategory_id)

        if brand_id:
            query = query.filter_by(brand_id=brand_id)

        if status != 'all':
            query = query.filter_by(status=status)

        # Apply sorting (except quantity)
        if sort_by != 'total_quantity':
            if sort_by == 'item_name':
                order_column = InventoryItem.item_name
            elif sort_by == 'item_code':
                order_column = InventoryItem.item_code
            elif sort_by == 'category':
                order_column = InventoryItem.item_category_id
            elif sort_by == 'status':
                order_column = InventoryItem.status
            else:
                order_column = InventoryItem.item_name

            if sort_order == 'desc':
                query = query.order_by(order_column.desc())
            else:
                query = query.order_by(order_column.asc())

        # Get total count
        total_items = query.count()

        # Apply pagination
        total_pages = (total_items + per_page - 1) // per_page if total_items > 0 else 1
        items = query.offset((page - 1) * per_page).limit(per_page).all()

        # Pre-fetch all item IDs
        item_ids = [item.id for item in items]

        # ===== CALCULATE QUANTITIES WITH USER LOCATION FILTER =====
        quantity_dict = calculate_inventory_quantities(
            db_session=db_session,
            app_id=app_id,
            item_ids=item_ids,
            location_id=location_id,
            user_location_ids=user_location_ids  # Pass user's assigned locations
        )

        # If sorting by total_quantity, sort after calculating
        if sort_by == 'total_quantity':
            items_with_quantity = []
            for item in items:
                total_quantity = quantity_dict.get(item.id, 0.0)
                items_with_quantity.append((item, total_quantity))

            items_with_quantity.sort(key=lambda x: x[1], reverse=(sort_order == 'desc'))
            items = [item for item, _ in items_with_quantity]

        # Pre-fetch selling prices if requested
        selling_prices_dict = {}
        if include_prices:
            variation_link_ids = []
            for item in items:
                for variation_link in item.inventory_item_variation_link:
                    variation_link_ids.append(variation_link.id)

            if variation_link_ids:
                selling_prices = db_session.query(ItemSellingPrice).options(
                    joinedload(ItemSellingPrice.currency),
                    joinedload(ItemSellingPrice.customer_group)
                ).filter(
                    ItemSellingPrice.inventory_item_variation_link_id.in_(variation_link_ids),
                    ItemSellingPrice.app_id == app_id,
                    ItemSellingPrice.is_active == True
                ).all()

                for price in selling_prices:
                    if price.inventory_item_variation_link_id not in selling_prices_dict:
                        selling_prices_dict[price.inventory_item_variation_link_id] = []
                    selling_prices_dict[price.inventory_item_variation_link_id].append({
                        'id': price.id,
                        'selling_price': float(price.selling_price),
                        'currency_id': price.currency_id,
                        'currency_code': price.currency.currency_code if price.currency else '',
                        'min_quantity': float(price.min_quantity),
                        'max_quantity': float(price.max_quantity) if price.max_quantity else None,
                        'customer_group_id': price.customer_group_id,
                        'customer_group_name': price.customer_group.name if price.customer_group else 'All Customers',
                        'effective_from': price.effective_from.isoformat() if price.effective_from else None,
                        'effective_to': price.effective_to.isoformat() if price.effective_to else None,
                        'price_type': price.price_type,
                        'markup_percentage': float(price.markup_percentage) if price.markup_percentage else None,
                        'margin_percentage': float(price.margin_percentage) if price.margin_percentage else None
                    })

        # Serialize items
        items_data = []
        for item in items:
            total_quantity = quantity_dict.get(item.id, 0.0)

            variations_data = []
            for variation_link in item.inventory_item_variation_link:
                variation_data = {
                    'id': variation_link.id,
                    'variation_name': variation_link.inventory_item_variation.variation_name if variation_link.inventory_item_variation else None,
                    'attribute_name': variation_link.inventory_item_attributes.attribute_name if variation_link.inventory_item_attributes else None,
                    'status': variation_link.status
                }

                if include_prices and variation_link.id in selling_prices_dict:
                    variation_data['selling_prices'] = selling_prices_dict[variation_link.id]

                variations_data.append(variation_data)

            item_data = {
                'id': item.id,
                'item_name': item.item_name,
                'item_code': item.item_code,
                'status': item.status,
                'reorder_point': item.reorder_point,
                'total_quantity': total_quantity,
                'category': {
                    'id': item.inventory_category.id if item.inventory_category else None,
                    'name': item.inventory_category.category_name if item.inventory_category else None
                },
                'subcategory': {
                    'id': item.inventory_subcategory.id if item.inventory_subcategory else None,
                    'name': item.inventory_subcategory.subcategory_name if item.inventory_subcategory else None
                },
                'brand': {
                    'id': item.brand.id if item.brand else None,
                    'name': item.brand.name if item.brand else None
                },
                'accounting': {
                    'cogs_account': {
                        'id': item.cogs_account_id,
                        'name': item.cogs_account.sub_category if item.cogs_account else None,
                        'code': item.cogs_account.sub_category_id if item.cogs_account else None
                    },
                    'asset_account': {
                        'id': item.asset_account_id,
                        'name': item.asset_account.sub_category if item.asset_account else None,
                        'code': item.asset_account.sub_category_id if item.asset_account else None
                    },
                    'sales_account': {
                        'id': item.sales_account_id,
                        'name': item.sales_account.sub_category if item.sales_account else None,
                        'code': item.sales_account.sub_category_id if item.sales_account else None
                    }
                },
                'uom': {
                    'id': item.uom_id,
                    'name': item.unit_of_measurement.full_name if item.unit_of_measurement else None,
                    'abbreviation': item.unit_of_measurement.abbreviation if item.unit_of_measurement else None
                },
                'variations': variations_data,
                'attributes_variations_count': len(item.inventory_item_variation_link),
                'image_filename': item.image_filename,
                'description': item.item_description
            }
            items_data.append(item_data)

        return jsonify({
            'success': True,
            'items': items_data,
            'include_prices': include_prices,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_items': total_items,
                'has_next': page < total_pages,
                'has_prev': page > 1
            }
        })

    except Exception as e:
        logger.error(f"Error in api_inventory_items: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching inventory items'
        }), 500

    finally:
        db_session.close()
@inventory_bp.route('/api/inventory_items/<int:item_id>', methods=['GET'])
@login_required
def api_inventory_item_detail(item_id):
    """
    Return detailed JSON data for a specific inventory item with financial metrics and selling prices
    Example usage
    GET /api/inventory_items/123?include_prices=true
    """
    db_session = Session()
    app_id = current_user.app_id

    try:

        # Check if prices should be included
        include_prices = request.args.get('include_prices', 'false').lower() == 'true'

        item = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()

        if not item:
            return jsonify({
                'success': False,
                'message': 'Inventory item not found'
            }), 404

        # Get base currency for the app
        base_currency = db_session.query(Currency).filter_by(app_id=app_id, currency_index=1).first()
        base_currency_id = base_currency.id if base_currency else None

        # Get all variation IDs for this item
        variation_ids = [pair.id for pair in item.inventory_item_variation_link]

        # Calculate financial metrics using the new valuation function
        item_valuation = calculate_inventory_valuation(
            db_session=db_session,
            app_id=app_id,
            item_ids=[item_id],  # Use item ID (not variation IDs)
            use_variation_ids=False,
            group_by_item=True
        )

        # Get valuation by location
        location_valuation = calculate_inventory_valuation(
            db_session=db_session,
            app_id=app_id,
            item_ids=[item_id],
            use_variation_ids=False,
            group_by_location=True
        )

        # Extract overall metrics
        total_quantity = 0
        total_value = Decimal('0')
        overall_avg_cost = None

        if item_id in item_valuation:
            item_data = item_valuation[item_id]
            total_quantity = item_data['quantity']
            total_value = Decimal(str(item_data['value']))
            overall_avg_cost = item_data['average_cost']

        # Get last purchase price using existing function
        last_purchase_prices = []
        for variation_id in variation_ids:
            last_price = get_last_purchase_price(
                db_session, app_id, variation_id, base_currency_id=base_currency_id
            )
            if last_price:
                last_purchase_prices.append(last_price)

        overall_last_purchase = max(last_purchase_prices) if last_purchase_prices else None

        # Prepare location details
        location_details = []
        if item_id in location_valuation:
            for location_id, loc_data in location_valuation[item_id].items():
                location = db_session.query(InventoryLocation).filter_by(id=location_id).first()
                if location:
                    location_details.append({
                        'id': location.id,
                        'name': location.location,
                        'quantity': loc_data['quantity'],
                        'average_cost': loc_data['average_cost'],
                        'total_value': loc_data['value']
                    })

        # Get selling prices if requested
        selling_prices_dict = {}
        if include_prices and variation_ids:
            selling_prices = db_session.query(ItemSellingPrice).options(
                joinedload(ItemSellingPrice.currency),
                joinedload(ItemSellingPrice.customer_group)
            ).filter(
                ItemSellingPrice.inventory_item_variation_link_id.in_(variation_ids),
                ItemSellingPrice.app_id == app_id,
                ItemSellingPrice.is_active == True
            ).all()

            # Group prices by variation link ID
            for price in selling_prices:
                if price.inventory_item_variation_link_id not in selling_prices_dict:
                    selling_prices_dict[price.inventory_item_variation_link_id] = []
                selling_prices_dict[price.inventory_item_variation_link_id].append({
                    'id': price.id,
                    'selling_price': float(price.selling_price),
                    'currency_id': price.currency_id,
                    'currency_code': price.currency.user_currency if price.currency else '',
                    'min_quantity': float(price.min_quantity),
                    'max_quantity': float(price.max_quantity) if price.max_quantity else None,
                    'customer_group_id': price.customer_group_id,
                    'customer_group_name': price.customer_group.name if price.customer_group else 'All Customers',
                    'effective_from': price.effective_from.isoformat() if price.effective_from else None,
                    'effective_to': price.effective_to.isoformat() if price.effective_to else None,
                    'price_type': price.price_type,
                    'markup_percentage': float(price.markup_percentage) if price.markup_percentage else None,
                    'margin_percentage': float(price.margin_percentage) if price.margin_percentage else None,
                    'is_active': price.is_active
                })

        # Get attribute-variation pairs with detailed valuation and prices
        attributes_variations = []
        for pair in item.inventory_item_variation_link:
            # Get valuation for this specific variation
            variation_valuation = calculate_inventory_valuation(
                db_session=db_session,
                app_id=app_id,
                item_ids=[pair.id],
                use_variation_ids=True,
                currency_id=base_currency_id,
                group_by_item=True
            )

            variation_qty = 0
            variation_cost = None
            variation_value = Decimal('0')

            if pair.id in variation_valuation:
                var_data = variation_valuation[pair.id]
                variation_qty = var_data['quantity']
                variation_value = Decimal(str(var_data['value']))
                variation_cost = var_data['average_cost']

            variation_data = {
                'id': pair.id,
                'attribute': {
                    'id': pair.attribute_id if pair.inventory_item_attributes else None,
                    'name': pair.inventory_item_attributes.attribute_name if pair.inventory_item_attributes else None
                },
                'variation': {
                    'id': pair.inventory_item_variation_id if pair.inventory_item_variation else None,
                    'name': pair.inventory_item_variation.variation_name if pair.inventory_item_variation else None
                },
                'quantity': variation_qty,
                'average_cost': format_amount(variation_cost),
                'total_value': format_amount(float(variation_value)),
                'status': pair.status
            }

            # Add selling prices if requested and available
            if include_prices and pair.id in selling_prices_dict:
                variation_data['selling_prices'] = selling_prices_dict[pair.id]

            attributes_variations.append(variation_data)

        item_data = {
            'id': item.id,
            'item_name': item.item_name,
            'item_code': item.item_code,
            'status': item.status,
            'reorder_level': item.reorder_point,
            'reorder_point': item.reorder_point,
            'category': {
                'id': item.inventory_category.id if item.inventory_category else None,
                'name': item.inventory_category.category_name if item.inventory_category else None
            },
            'subcategory': {
                'id': item.inventory_subcategory.id if item.inventory_subcategory else None,
                'name': item.inventory_subcategory.subcategory_name if item.inventory_subcategory else None
            },
            'brand': {
                'id': item.brand.id if item.brand else None,
                'name': item.brand.name if item.brand else None
            },
            'accounting': {
                'cogs_account': {
                    'id': item.cogs_account_id,
                    'name': item.cogs_account.sub_category if item.cogs_account else None,
                    'code': item.cogs_account.sub_category_id if item.cogs_account else None
                },
                'asset_account': {
                    'id': item.asset_account_id,
                    'name': item.asset_account.sub_category if item.asset_account else None,
                    'code': item.asset_account.sub_category_id if item.asset_account else None
                },
                'sales_account': {
                    'id': item.sales_account_id,
                    'name': item.sales_account.sub_category if item.sales_account else None,
                    'code': item.sales_account.sub_category_id if item.sales_account else None
                }
            },
            'uom': {
                'id': item.uom_id,
                'name': item.unit_of_measurement.full_name if item.unit_of_measurement else None,
                'abbreviation': item.unit_of_measurement.abbreviation if item.unit_of_measurement else None
            },
            'financial_metrics': {
                'average_cost': format_amount(overall_avg_cost),
                'last_purchase_price': format_amount(overall_last_purchase),
                'total_quantity': total_quantity,
                'total_value': format_amount(float(total_value)),
                'currency': {
                    'id': base_currency.id if base_currency else None,
                    'code': base_currency.user_currency if base_currency else None,
                    'symbol': base_currency.user_currency if base_currency else None
                }
            },
            'stock_locations': location_details,
            'attributes_variations': attributes_variations,
            'attributes_variations_count': len(attributes_variations),
            'image_filename': item.image_filename,
            'description': item.item_description,
            'created_at': item.created_at.isoformat() if item.created_at else None,
            'updated_at': item.updated_at.isoformat() if item.updated_at else None,
            'include_prices': include_prices
        }

        return jsonify({
            'success': True,
            'item': item_data
        })

    except Exception as e:
        logger.error(f"Error in api_inventory_item_detail: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching inventory item details'
        }), 500

    finally:
        db_session.close()


@inventory_bp.route('/api/inventory_items/search', methods=['GET'])
@login_required
def api_inventory_items_search():
    """
    Search inventory items with various filters
    """
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get search parameters
        search_term = request.args.get('q', '')
        category_id = request.args.get('category_id', type=int)
        subcategory_id = request.args.get('subcategory_id', type=int)
        brand_id = request.args.get('brand_id', type=int)
        status = request.args.get('status', 'active')
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        logger.info(f'Data is {request.args}')
        # Validate page and per_page values
        if page < 1:
            page = 1
        if per_page < 1:
            per_page = 50

        # Build base query
        query = db_session.query(InventoryItem).filter_by(app_id=app_id)

        # Apply filters
        if search_term:
            query = query.filter(InventoryItem.item_name.ilike(f'%{search_term}%'))

        if category_id:
            query = query.filter_by(item_category_id=category_id)

        if subcategory_id:
            query = query.filter_by(item_subcategory_id=subcategory_id)

        if brand_id:
            query = query.filter_by(brand_id=brand_id)

        if status != 'all':
            query = query.filter_by(status=status)

        # Get total count
        total_items = query.count()

        # Calculate total pages
        total_pages = (total_items + per_page - 1) // per_page if total_items > 0 else 1

        # Validate page number
        if page > total_pages:
            page = total_pages

        # Apply pagination
        items = query.offset((page - 1) * per_page).limit(per_page).all()

        # Serialize items
        items_data = []
        for item in items:
            items_data.append({
                'id': item.id,
                'item_name': item.item_name,
                'item_code': item.item_code,
                'status': item.status,
                'category': item.inventory_category.category_name if item.inventory_category else None,
                'subcategory': item.inventory_subcategory.subcategory_name if item.inventory_subcategory else None,
                'brand': item.brand.name if item.brand else None
            })

        return jsonify({
            'success': True,
            'items': items_data,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_items': total_items
            }
        })

    except Exception as e:
        logger.error(f"Error in api_inventory_items_search: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while searching inventory items'
        }), 500

    finally:
        db_session.close()


@inventory_bp.route('/api/inventory/item/<int:item_id>/transactions', methods=["GET"])
def get_item_transactions(item_id):
    """Get paginated transaction history for a specific item"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    # Pagination parameters
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    movement_type = request.args.get("movement_type")  # 'in', 'out', etc.

    with Session() as db_session:
        try:
            # Validate company
            company = db_session.query(Company).filter_by(api_key=api_key).first()
            if not company:
                return jsonify({"error": "Invalid API key"}), 403

            app_id = company.id

            # First, get all variation links for this item
            variation_links = db_session.query(InventoryItemVariationLink.id).filter_by(
                app_id=app_id,
                inventory_item_id=item_id
            ).all()

            variation_link_ids = [link.id for link in variation_links]

            if not variation_link_ids:
                return jsonify({
                    'transactions': [],
                    'pagination': {
                        'page': page,
                        'per_page': per_page,
                        'total': 0,
                        'pages': 0
                    }
                })

            # Base query for transactions
            query = db_session.query(
                InventoryTransactionDetail,
                InventoryLocation.location.label('location_name'),
                InventoryItemVariationLink.id.label('variation_link_id'),
                InventoryItemVariation.variation_name,
                InventoryItemAttribute.attribute_name,
                InventoryEntry.inventory_source,
                InventoryEntry.source_id,
                InventoryEntry.reference
            ).join(
                InventoryLocation,
                InventoryTransactionDetail.location_id == InventoryLocation.id
            ).join(
                InventoryItemVariationLink,
                InventoryTransactionDetail.item_id == InventoryItemVariationLink.id
            ).outerjoin(
                InventoryItemVariation,
                InventoryItemVariationLink.inventory_item_variation_id == InventoryItemVariation.id
            ).outerjoin(
                InventoryItemAttribute,
                InventoryItemVariationLink.attribute_id == InventoryItemAttribute.id
            ).join(
                InventoryEntryLineItem,
                InventoryTransactionDetail.inventory_entry_line_item_id == InventoryEntryLineItem.id
            ).join(
                InventoryEntry,
                InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
            ).filter(
                InventoryTransactionDetail.app_id == app_id,
                InventoryTransactionDetail.item_id.in_(variation_link_ids)
            )

            # Apply filters
            if start_date:
                query = query.filter(InventoryTransactionDetail.transaction_date >= start_date)
            if end_date:
                query = query.filter(InventoryTransactionDetail.transaction_date <= end_date)
            if movement_type:
                query = query.filter(InventoryTransactionDetail.movement_type == movement_type)

            # Get total count for pagination
            total_count = query.count()

            # Apply pagination and ordering
            transactions = query.order_by(
                InventoryTransactionDetail.transaction_date.desc()
            ).offset((page - 1) * per_page).limit(per_page).all()

            # Format the response
            transaction_list = []
            for trans, location_name, var_link_id, var_name, attr_name, source, source_id, reference in transactions:
                transaction_list.append({
                    'id': trans.id,
                    'date': trans.transaction_date.strftime('%Y-%m-%d'),
                    'datetime': trans.created_at.strftime('%Y-%m-%d %H:%M:%S') if trans.created_at else None,
                    'movement_type': trans.movement_type,
                    'variation': f"{attr_name or ''} {var_name or ''}".strip() or 'Default',
                    'location': location_name,
                    'quantity': abs(float(trans.quantity or 0)),
                    'unit_cost': float(trans.unit_cost),
                    'total_cost': abs(float(trans.total_cost)),
                    'source': source,
                    'source_id': source_id,
                    'reference': reference
                })

            # Calculate pagination info
            total_pages = (total_count + per_page - 1) // per_page

            return jsonify({
                'transactions': transaction_list,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': total_count,
                    'pages': total_pages,
                    'has_next': page < total_pages,
                    'has_prev': page > 1
                }
            })

        except Exception as e:
            logger.error(f"Error fetching item transactions: {str(e)}")
            return jsonify({"error": str(e)}), 500


@inventory_bp.route('/api/inventory/item/<int:item_id>/stock-trend', methods=["GET"])
def get_item_stock_trend(item_id):
    """Get stock level trend data for charts"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    period = request.args.get("period", "30")  # days

    with Session() as db_session:
        try:
            company = db_session.query(Company).filter_by(api_key=api_key).first()
            if not company:
                return jsonify({"error": "Invalid API key"}), 403

            app_id = company.id
            end_date = date.today()
            start_date = end_date - timedelta(days=int(period))

            # Get all variation links
            variation_links = db_session.query(InventoryItemVariationLink.id).filter_by(
                app_id=app_id,
                inventory_item_id=item_id
            ).all()

            variation_link_ids = [link.id for link in variation_links]

            if not variation_link_ids:
                return jsonify({
                    'labels': [],
                    'datasets': []
                })

            # Get daily running totals
            from sqlalchemy import func, and_

            # This is a simplified version - you might need to adjust based on your schema
            daily_totals = db_session.query(
                func.date(InventoryTransactionDetail.transaction_date).label('date'),
                InventoryTransactionDetail.location_id,
                func.sum(InventoryTransactionDetail.quantity).label('net_change')
            ).filter(
                InventoryTransactionDetail.app_id == app_id,
                InventoryTransactionDetail.item_id.in_(variation_link_ids),
                InventoryTransactionDetail.transaction_date.between(start_date, end_date)
            ).group_by(
                func.date(InventoryTransactionDetail.transaction_date),
                InventoryTransactionDetail.location_id
            ).order_by('date').all()

            # Format for chart.js
            labels = []
            datasets = {}

            for day in daily_totals:
                date_str = day.date.strftime('%Y-%m-%d')
                if date_str not in labels:
                    labels.append(date_str)

                # Get location name
                location = db_session.query(InventoryLocation).get(day.location_id)
                loc_name = location.location if location else f"Location {day.location_id}"

                if loc_name not in datasets:
                    datasets[loc_name] = []

                # We need to calculate running total - this is simplified
                datasets[loc_name].append(float(day.net_change))

            return jsonify({
                'labels': labels,
                'datasets': [
                    {
                        'label': loc,
                        'data': data,
                        'borderColor': f'hsl({i * 60 % 360}, 70%, 50%)',
                        'fill': False
                    }
                    for i, (loc, data) in enumerate(datasets.items())
                ]
            })

        except Exception as e:
            logger.error(f"Error fetching stock trend: {str(e)}")
            return jsonify({"error": str(e)}), 500


@inventory_bp.route('/api/inventory/item/<int:item_id>/summary', methods=["GET"])
def get_item_summary(item_id):
    """Get summary statistics for the item"""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as db_session:
        try:
            company = db_session.query(Company).filter_by(api_key=api_key).first()
            if not company:
                return jsonify({"error": "Invalid API key"}), 403

            app_id = company.id

            # Get all variation links
            variation_links = db_session.query(InventoryItemVariationLink.id).filter_by(
                app_id=app_id,
                inventory_item_id=item_id
            ).all()

            variation_link_ids = [link.id for link in variation_links]

            if not variation_link_ids:
                return jsonify({
                    'total_quantity': 0,
                    'total_value': 0,
                    'locations': 0,
                    'variations': 0,
                    'last_transaction': None,
                    'avg_cost': 0
                })

            # Get summary from InventorySummary table
            summaries = db_session.query(InventorySummary).filter(
                InventorySummary.app_id == app_id,
                InventorySummary.item_id.in_(variation_link_ids)
            ).all()

            total_quantity = sum(s.total_quantity for s in summaries)
            total_value = sum(float(s.total_value) for s in summaries)
            locations = len(set(s.location_id for s in summaries))

            # Get last transaction date
            last_trans = db_session.query(InventoryTransactionDetail).filter(
                InventoryTransactionDetail.app_id == app_id,
                InventoryTransactionDetail.item_id.in_(variation_link_ids)
            ).order_by(InventoryTransactionDetail.transaction_date.desc()).first()

            # Calculate average cost (weighted)
            avg_cost = total_value / total_quantity if total_quantity > 0 else 0

            return jsonify({
                'total_quantity': round(total_quantity, 2),
                'total_value': round(total_value, 2),
                'locations': locations,
                'variations': len(variation_links),
                'last_transaction': last_trans.transaction_date.strftime('%Y-%m-%d') if last_trans else None,
                'avg_cost': round(avg_cost, 2)
            })

        except Exception as e:
            logger.error(f"Error fetching item summary: {str(e)}")
            return jsonify({"error": str(e)}), 500
