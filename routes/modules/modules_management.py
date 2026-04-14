# app/routes/modules/module_management.py


import logging
from datetime import datetime

from flask import jsonify, request, redirect, url_for, render_template, flash
from flask_login import login_required, current_user
from sqlalchemy import func, case, distinct, or_
from sqlalchemy.orm import joinedload

from ai import resolve_exchange_rate_for_transaction
from db import Session
from models import Company, Module, Currency, Department, Employee, \
    PayrollPeriod, Deduction, BenefitType, \
    DeductionType, PaymentMode, DeductionPayment, AdvancePayment, User, AdvanceRepayment, ChartOfAccounts, \
    Category
from services.post_to_ledger_reversal import repost_transaction
from utils import create_notification

# Import the same blueprint
from . import module_bp

logger = logging.getLogger()


@module_bp.route('/', methods=['GET'])
@login_required
def modules():
    db_session = Session()
    try:
        app_id = current_user.app_id
        # Get company name
        company = db_session.query(Company.name).filter_by(id=app_id)

        # Get user role
        user = db_session.query(User.role).filter_by(app_id=app_id).first()
        role = user[0] if user else "user"  # Default role if not found

        # Get modules data
        modules_data = db_session.query(Module).filter_by(app_id=app_id).all()
        included_modules = [module.module_name for module in modules_data if module.included == 'yes']

        # Available modules
        available_modules = ['Payroll', 'Inventory', 'Purchase Order', 'Sales', 'POS', 'Invoices', 'Asset Management']

        return render_template('modules.html',
                               included_modules=included_modules,
                               available_modules=available_modules,
                               company=company,
                               role=role,
                               title="Modules")
    except Exception as e:
        # Log the error if needed
        # You might want to return an error page here
        return render_template('error.html', error=str(e)), 500
    finally:
        # This will always execute, ensuring session is closed
        db_session.close()


@module_bp.route('/update_module', methods=['POST'])
@login_required
def update_module():
    db_session = Session()
    app_id = current_user.app_id
    data = request.get_json()
    module_name = data['module_name']
    included = data['included']

    try:
        module = db_session.query(Module).filter_by(module_name=module_name, app_id=app_id).first()

        if module:
            module.included = included
        else:
            module = Module(module_name=module_name, app_id=app_id, included=included)
            db_session.add(module)

        db_session.commit()
        return jsonify({'success': True})

    except Exception as e:
        db_session.rollback()  # Rollback the session in case of an error
        logger.info(f"An error occurred: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

    finally:
        db_session.close()  # Ensure the session is closed
