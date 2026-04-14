import logging

from flask import Blueprint, render_template, jsonify, flash, request, redirect
from flask_login import login_required, current_user
from sqlalchemy import union_all, literal

from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Project, PaymentMode, Vendor, PurchaseOrder, DirectPurchaseTransaction, \
    PurchaseTransaction

purchase_routes = Blueprint('purchase_routes', __name__)
logger = logging.getLogger(__name__)


@purchase_routes.route('/purchases/dashboard', methods=["GET"])
@login_required
def purchases_dashboard():
    try:
        app_id = current_user.app_id
        role = current_user.role

        # Use context manager for session handling
        with Session() as db_session:
            # Get company info and validate
            company = db_session.get(Company, app_id)
            if not company:
                return jsonify({"error": "Company not found"}), 404

            # Get all necessary data in optimized queries
            modules_data = db_session.query(Module.module_name).filter(
                Module.app_id == app_id,
                Module.included == 'yes'
            ).all()
            modules_data = [mod[0] for mod in modules_data]  # Extract just module names

            # Get base currency with error handling
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            # Get purchase references using optimized union query
            purchase_numbers = db_session.execute(
                union_all(
                    db_session.query(
                        PurchaseOrder.purchase_order_number.label('number'),
                        literal('purchase_order').label('type')
                    ).filter(
                        PurchaseOrder.app_id == app_id,
                        PurchaseOrder.purchase_order_number.isnot(None)
                    ),
                    db_session.query(
                        DirectPurchaseTransaction.direct_purchase_number.label('number'),
                        literal('direct_purchase').label('type')
                    ).filter(
                        DirectPurchaseTransaction.app_id == app_id,
                        DirectPurchaseTransaction.direct_purchase_number.isnot(None)
                    )
                ).order_by('number')
            ).all()

            purchase_refs = db_session.execute(
                union_all(
                    db_session.query(
                        PurchaseTransaction.reference_number.label('reference'),
                        literal('purchase_order').label('type')
                    ).filter(
                        PurchaseTransaction.app_id == app_id,
                        PurchaseTransaction.reference_number.isnot(None)
                    ),
                    db_session.query(
                        DirectPurchaseTransaction.purchase_reference.label('reference'),
                        literal('direct_purchase').label('type')
                    ).filter(
                        DirectPurchaseTransaction.app_id == app_id,
                        DirectPurchaseTransaction.purchase_reference.isnot(None)
                    )
                ).order_by('reference')
            ).all()

            # Get other data with optimized queries
            projects = db_session.query(Project).filter_by(app_id=app_id).all()
            payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
            vendors = db_session.query(Vendor).filter_by(app_id=app_id).all()

            return render_template(
                '/purchases/purchases_dashboard.html',
                company=company,
                role=role,
                modules=modules_data,
                module_name="Purchases",
                api_key=company.api_key,
                base_currency=base_currency_info["base_currency"],
                projects=projects,
                payment_modes=payment_modes,
                vendors=vendors,
                purchase_numbers=[{'number': ref.number, 'type': ref.type} for ref in purchase_numbers],
                references=[{'reference': ref.reference, 'type': ref.type} for ref in purchase_refs]
            )

    except Exception as e:
        logger.error(f"Error in purchases_dashboard: {str(e)}")
        flash("An unexpected error occurred", "error")
        return redirect(request.referrer)
