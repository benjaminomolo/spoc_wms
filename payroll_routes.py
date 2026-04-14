from flask import Blueprint, jsonify, render_template
from flask_login import login_required, current_user

from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Department, Employee, \
    PayrollPeriod, Project

payroll_routes = Blueprint('payroll_routes', __name__)


@payroll_routes.route('/payroll/dashboard', methods=["GET"])
@login_required
def payroll_dashboard():
    db_session = Session()
    try:
        app_id = current_user.app_id
        role = current_user.role
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        module_name = "Payroll"
        api_key = company.api_key

        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency = base_currency_info["base_currency"]

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        departments = db_session.query(Department).filter_by(app_id=app_id).all()
        employees = db_session.query(Employee).filter_by(app_id=app_id).all()
        projects = db_session.query(Project).filter_by(app_id=app_id).all()
        payroll_periods = (
            db_session.query(PayrollPeriod)
            .filter_by(app_id=app_id)
            .order_by(PayrollPeriod.id.desc())
            .all()
        )


        return render_template(
            '/payroll/payroll_dashboard.html',
            company=company,
            role=role,
            modules=modules_data,
            module_name=module_name,
            api_key=api_key,
            base_currency=base_currency,
            currencies=currencies,
            payroll_periods=payroll_periods,
            employees=employees,
            departments=departments,
            projects=projects
        )

    finally:
        db_session.close()
