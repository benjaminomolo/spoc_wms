import logging

from flask import Blueprint, render_template, jsonify, flash, redirect, request
from flask_login import login_required, current_user
from sqlalchemy import literal, union_all

from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Project, Category, ChartOfAccounts, PaymentMode, Vendor, SalesInvoice, \
    DirectSalesTransaction, SalesTransaction

sales_routes = Blueprint('sales_routes', __name__)

logger = logging.getLogger(__name__)


@sales_routes.route('/sales/dashboard', methods=["GET"])
@login_required
def sales_dashboard():
    try:
        app_id = current_user.app_id
        role = current_user.role

        with Session() as db_session:
            # Get company info
            company = db_session.query(Company).filter_by(id=app_id).first()
            if not company:
                return jsonify({"error": "Company not found"}), 404

            # Get base currency
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            # Get modules
            modules_data = db_session.query(Module.module_name).filter(
                Module.app_id == app_id,
                Module.included == 'yes'
            ).all()
            modules_data = [mod[0] for mod in modules_data]

            # 🆕 CRITICAL: Get currencies for the dropdown
            currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

            # ✅ FIXED: Get BOTH invoice sales numbers AND direct sales numbers
            invoice_sales_numbers = db_session.query(
                SalesInvoice.invoice_number.label('number'),
                literal('invoice').label('type')
            ).filter(
                SalesInvoice.app_id == app_id,
                SalesInvoice.invoice_number.isnot(None),
                SalesInvoice.invoice_number != ''
            ).order_by('number').all()

            # ✅ ADD: Direct sales numbers
            direct_sales_numbers = db_session.query(
                DirectSalesTransaction.direct_sale_number.label('number'),
                literal('direct').label('type')
            ).filter(
                DirectSalesTransaction.app_id == app_id,
                DirectSalesTransaction.direct_sale_number.isnot(None),
                DirectSalesTransaction.direct_sale_number != ''
            ).order_by('number').all()

            # ✅ FIXED: Get BOTH invoice references AND direct sales references
            invoice_references = db_session.query(
                SalesTransaction.reference_number.label('reference'),
                literal('invoice').label('type')
            ).filter(
                SalesTransaction.app_id == app_id,
                SalesTransaction.reference_number.isnot(None),
                SalesTransaction.reference_number != ''
            ).order_by('reference').all()

            # ✅ ADD: Direct sales references
            direct_sales_references = db_session.query(
                DirectSalesTransaction.sale_reference.label('reference'),
                literal('direct').label('type')
            ).filter(
                DirectSalesTransaction.app_id == app_id,
                DirectSalesTransaction.sale_reference.isnot(None),
                DirectSalesTransaction.sale_reference != ''
            ).order_by('reference').all()

            # Get other data
            projects = db_session.query(Project).filter_by(app_id=app_id).all()
            payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
            vendors = db_session.query(Vendor).filter_by(app_id=app_id).all()

            # ✅ COMBINE: Both invoice and direct sales data
            all_sales_numbers = list(invoice_sales_numbers) + list(direct_sales_numbers)
            all_references = list(invoice_references) + list(direct_sales_references)

            return render_template(
                '/sales/sales_dashboard.html',
                company=company,
                role=role,
                modules=modules_data,
                module_name="Sales",
                api_key=company.api_key,
                base_currency=base_currency_info["base_currency"],
                currencies=currencies,
                projects=projects,
                payment_modes=payment_modes,
                vendors=vendors,
                invoice_sales_numbers=[{'number': ref.number, 'type': ref.type} for ref in all_sales_numbers],
                references=[{'reference': ref.reference, 'type': ref.type} for ref in all_references]
            )

    except Exception as e:
        logger.error(f"Error in sales_dashboard: {str(e)}")
        flash("An unexpected error occurred", "error")
        return redirect(request.referrer)
