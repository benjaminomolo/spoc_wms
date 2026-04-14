import logging
import traceback

from flask import Blueprint, jsonify, render_template, flash, redirect, url_for, request
from flask_login import login_required, current_user
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from decorators import role_required, require_permission
from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Project, ChartOfAccounts, PaymentMode, Category, Department, Employee, \
    PayrollPeriod, InventoryCategory, InventorySubCategory, InventoryLocation, InventoryItem, InventoryItemVariation, \
    InventoryItemAttribute, Brand, InventoryItemVariationLink

inventory_routes = Blueprint('inventory_routes', __name__)
# Set up logging
logger = logging.getLogger(__name__)


# @inventory_routes.route('/inventory/dashboard', methods=["GET"])
# @login_required
# @require_permission('Inventory', 'view')  # Requires view access
# def inventory_dashboard():
#     db_session = Session()
#     try:
#         app_id = current_user.app_id
#         role = current_user.role
#         company = db_session.query(Company).filter_by(id=app_id).first()
#         modules_data = [mod.module_name for mod in
#                         db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
#         module_name = "Inventory"
#         api_key = company.api_key
#
#         # Fetch necessary data
#         categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
#         subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
#         locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
#         items = db_session.query(InventoryItemVariationLink).filter_by(app_id=app_id, status="active").all()
#         variations = db_session.query(InventoryItemVariation).filter_by(app_id=app_id).all()
#         attributes = db_session.query(InventoryItemAttribute).filter_by(app_id=app_id).all()
#         brands = db_session.query(Brand).filter_by(app_id=app_id).all()
#
#         base_currency_info = get_base_currency(db_session, app_id)
#         if not base_currency_info:
#             return jsonify({"error": "Base currency not defined for this company"}), 400
#
#         base_currency_id = base_currency_info["base_currency_id"]
#         base_currency = base_currency_info["base_currency"]
#
#         currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
#
#         return render_template(
#             '/inventory/inventory_dashboard.html',
#             company=company,
#             role=role,
#             modules=modules_data,
#             module_name=module_name,
#             api_key=api_key,
#             base_currency=base_currency,
#             currencies=currencies,
#             categories=categories,
#             subcategories=subcategories,
#             locations=locations,
#             variations=variations,
#             attributes=attributes,
#             brands=brands,
#             items=items
#         )
#
#     finally:
#         db_session.close()


# @inventory_routes.route('/inventory/manage_attributes_variations')
# @role_required(['Admin', 'Contributor'])
# def manage_attributes_variations():
#     try:
#         app_id = current_user.app_id
#
#         with Session() as db_session:
#             role = current_user.role
#             company = db_session.query(Company).filter_by(id=app_id).first()
#             modules_data = [mod.module_name for mod in
#                             db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
#             module_name = "Inventory"
#             attributes = db_session.query(InventoryItemAttribute).options(
#                 joinedload(InventoryItemAttribute.inventory_item_variation_link)
#             ).filter_by(app_id=app_id).all()
#             variations = db_session.query(InventoryItemVariation).options(
#                 joinedload(InventoryItemVariation.inventory_item_variation_link)
#                 .joinedload(InventoryItemVariationLink.inventory_item_attributes),
#                 joinedload(InventoryItemVariation.inventory_item_attributes)
#             ).filter_by(app_id=app_id).all()
#
#         return render_template(
#             'inventory/manage_attributes_variations.html',
#             attributes=attributes,
#             variations=variations,
#             company=company,
#             role=role,
#             modules=modules_data,
#             module_name=module_name
#         )
#
#     except Exception as e:
#         logger.error(f"Error loading attributes/variations: {str(e)}\n{traceback.format_exc()}")
#         flash('An error occurred while loading the inventory attributes.', 'danger')
#
#         # Use referrer to redirect back if available, otherwise fallback
#         return redirect(request.referrer or url_for('inventory_routes.inventory_dashboard'))


# Edit attribute
@inventory_routes.route('/attribute/<int:attribute_id>/edit', methods=['POST'])
@role_required(['Admin', 'Contributor'])
def edit_inventory_attribute(attribute_id):
    try:
        app_id = current_user.app_id

        with Session() as db_session:
            attribute = db_session.query(InventoryItemAttribute).filter_by(
                id=attribute_id,
                app_id=app_id
            ).first()

            if not attribute:
                return jsonify({"message": "Attribute not found", "success": False}), 404

            new_name = request.form.get('attribute_name', '').strip()

            if not new_name:
                return jsonify({"message": "Attribute name is required", "success": False})

            if new_name.lower() != attribute.attribute_name.lower():
                # Check if new name already exists (case-insensitive)
                existing = db_session.query(InventoryItemAttribute).filter(
                    func.lower(InventoryItemAttribute.attribute_name) == new_name.lower(),
                    InventoryItemAttribute.app_id == app_id,
                    InventoryItemAttribute.id != attribute.id  # avoid self-conflict
                ).first()

                if existing:
                    return jsonify({"message": "Attribute with this name already exists", "success": False})

                attribute.attribute_name = new_name
                db_session.commit()

            return jsonify({"message": "Attribute updated successfully", "success": True})

    except Exception as e:
        logger.error(f"Error editing inventory attribute: {str(e)}")
        return jsonify({"message": "An error occurred while updating the inventory attribute.", "success": False})


@inventory_routes.route('/variation/<int:variation_id>/edit', methods=['POST'])
@role_required(['Admin', 'Contributor'])
def edit_inventory_variation(variation_id):
    try:
        db_session = Session()
        app_id = current_user.app_id

        variation = db_session.query(InventoryItemVariation).filter_by(
            id=variation_id,
            app_id=app_id
        ).first()

        if not variation:
            return jsonify({'success': False, 'error': 'Variation not found'}), 404

        variation_name = request.form.get('variation_name', '').strip()

        if not variation_name:
            return jsonify({'success': False, 'error': 'Variation name is required'}), 400

        if variation_name.lower() != (variation.variation_name or "").lower():
            # Check for duplicates
            existing = db_session.query(InventoryItemVariation).filter(
                func.lower(InventoryItemVariation.variation_name) == variation_name.lower(),
                InventoryItemVariation.app_id == app_id,
                InventoryItemVariation.id != variation_id  # Exclude self
            ).first()

            if existing:
                return jsonify({'success': False, 'error': 'Variation with this name already exists'}), 400

            variation.variation_name = variation_name
            db_session.commit()

        return jsonify({'success': True, 'message': 'Variation updated successfully!'})

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error updating variation: {str(e)}")
        return jsonify({'success': False, 'error': 'An unexpected error occurred'}), 500

    finally:
        db_session.close()


# Check if variation is used in inventory (API endpoint)
@inventory_routes.route('/api/variation/<int:variation_id>/inventory_usage')
@role_required(['Admin', 'Contributor'])
def variation_inventory_usage(variation_id):
    variation = InventoryItemVariation.query.filter_by(
        id=variation_id,
        app_id=current_user.company_id
    ).first_or_404()

    count = InventoryItemVariationLink.query.filter_by(
        inventory_item_variation_id=variation_id
    ).count()

    return jsonify({
        'used_in_inventory': count > 0,
        'count': count
    })


@inventory_routes.route('/attribute/<int:attribute_id>/delete', methods=['POST'])
@role_required(['Admin', 'Contributor'])
def delete_inventory_attribute(attribute_id):
    try:
        app_id = current_user.app_id

        with Session() as db_session:
            attribute = db_session.query(InventoryItemAttribute).filter_by(
                id=attribute_id,
                app_id=app_id
            ).first()

            if not attribute:
                return jsonify({'error': 'Attribute not found'}), 404

            # Check for variations
            variation_count = db_session.query(InventoryItemVariation).filter_by(
                attribute_id=attribute_id,
                app_id=app_id
            ).count()

            if variation_count > 0:
                return jsonify({
                    'error': f'This attribute has {variation_count} variations and cannot be deleted.'
                }), 400

            db_session.delete(attribute)
            db_session.commit()

            return jsonify({
                'success': f'Attribute "{attribute.attribute_name}" deleted successfully'
            })

    except Exception as e:
        logger.error(f"Failed to delete attribute {attribute_id}: {str(e)}")
        return jsonify({
            'error': f'Failed to delete attribute: {str(e)}'
        }), 500


@inventory_routes.route('/variation/<int:variation_id>/delete', methods=['POST'])
@role_required(['Admin', 'Contributor'])
def delete_inventory_variation(variation_id):
    try:
        app_id = current_user.app_id

        with Session() as db_session:
            variation = db_session.query(InventoryItemVariation).filter_by(
                id=variation_id,
                app_id=app_id
            ).first()

            if not variation:
                return jsonify({'error': 'Variation not found'}), 404

            # Check if variation is used
            usage_count = db_session.query(InventoryItemVariationLink).filter_by(
                inventory_item_variation_id=variation_id
            ).count()

            if usage_count > 0:
                return jsonify({
                    'error': f'This variation is used in {usage_count} inventory entries and cannot be deleted.'
                }), 400

            db_session.delete(variation)
            db_session.commit()

            return jsonify({
                'success': f'Variation "{variation.variation_name}" deleted successfully'
            })

    except Exception as e:
        logger.error(f"Failed to delete variation {variation_id}: {str(e)}")
        return jsonify({
            'error': f'Failed to delete variation: {str(e)}'
        }), 500
