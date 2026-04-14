# app/routes/payroll/settings_and_administration.py


# app/routes/payroll/payrun.py
import decimal
import logging
from collections import defaultdict
from datetime import datetime, date

from flask import Blueprint, jsonify, render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from sqlalchemy import func, distinct
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import joinedload

from ai import get_base_currency
from db import Session
from models import Company, Module, Currency, Department, Employee, \
    PayrollPeriod, EmployeeBenefit, EmployeeDeduction, Vendor, PayrollTransaction, Deduction, Benefit, BenefitType, \
    DeductionType, PayRollStatusEnum, AdvanceRepayment, AdvancePayment

# Import the same blueprint
from . import payroll_bp

logger = logging.getLogger()


@payroll_bp.route('/add_department', methods=['POST'])
def add_department():
    db_session = Session()
    try:
        # Get form data
        data = request.get_json()
        department_name = data.get('department_name')
        department_head_id = data.get('department_head_id') or None
        location = data.get('location') or None
        description = data.get('description') or None
        budget = data.get('budget') or None
        app_id = current_user.app_id  # Assuming you're using Flask-Login

        # Create new department
        new_department = Department(
            department_name=department_name,
            department_head_id=department_head_id,
            location=location,
            description=description,
            budget=budget,
            app_id=app_id
        )

        # Add and commit to database
        db_session.add(new_department)
        db_session.commit()

        # Return the new department's ID
        return jsonify({
            'success': True,
            'message': 'Department added successfully!',
            'department_id': new_department.id  # Returning the new department's ID
        })

    except SQLAlchemyError as e:
        logger.error(f'An error occurred {e}')
        db_session.rollback()
        return jsonify({'success': False, 'message': str(e)})
    except Exception as e:
        logger.error(f'An error occurred {e}')
        db_session.rollback()
        return jsonify({'success': False, 'message': str(e)})
    finally:
        db_session.close()


@payroll_bp.route('/manage_payroll', methods=['GET'])
def manage_payroll():
    # Start a new session for this request
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Fetch data from the database
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        company = db_session.query(Company).filter_by(id=app_id).all()
        departments = db_session.query(Department).filter_by(app_id=app_id).all()
        benefits = db_session.query(Benefit).filter_by(app_id=app_id).all()
        deductions = db_session.query(Deduction).filter_by(app_id=app_id).all()
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        # Pass the data to the template
        return render_template('/payroll/manage_payroll.html',
                               departments=departments,
                               benefits=benefits,
                               deductions=deductions,
                               currencies=currencies,
                               modules=modules_data,
                               module_name="Payroll",
                               company=company,
                               role=role)
    except Exception as e:
        # Handle exceptions and log if necessary
        db_session.rollback()
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        # Ensure that the session is closed after the request
        db_session.close()


@payroll_bp.route('/update_data/<int:data_id>', methods=['POST'])
def update_data(data_id):
    db_session = Session()
    data = request.get_json()

    # Get the entity type from query parameters
    entity_type = request.args.get('type')

    try:
        # Fetch the appropriate entity and update its fields
        if entity_type == 'department':
            entity = db_session.query(Department).get_or_404(data_id)
            entity.department_name = data.get('department_name', entity.department_name)
            entity.department_head_id = data.get('department_head_id', entity.department_head_id)
            entity.location = data.get('location', entity.location)
            entity.description = data.get('description', entity.description)
            entity.budget = data.get('budget', entity.budget)
        elif entity_type == 'benefit':
            entity = db_session.query(Benefit).get_or_404(data_id)
            entity.name = data.get('name', entity.name)
            entity.description = data.get('description', entity.description)
            entity.is_rate = data.get('is_rate', entity.is_rate)
            entity.rate = data.get('rate', entity.rate)
            entity.fixed_amount = data.get('fixed_amount', entity.fixed_amount)
            entity.currency_id = data.get('currency', entity.currency_id)
        elif entity_type == 'deduction':
            entity = db_session.query(Deduction).get_or_404(data_id)
            entity.name = data.get('name', entity.name)
            entity.description = data.get('description', entity.description)
            entity.is_rate = data.get('is_rate', entity.is_rate)
            entity.rate = data.get('rate', entity.rate)
            entity.fixed_amount = data.get('fixed_amount', entity.fixed_amount)
            entity.currency_id = data.get('currency', entity.currency_id)
        else:
            return jsonify({"success": False, "message": "Invalid entity type"}), 400

        # Commit changes to the database
        db_session.commit()
        return jsonify({"success": True, "message": "Data updated successfully"})

    except Exception as e:
        # Handle exceptions and rollback changes
        db_session.rollback()
        return jsonify({"success": False, "message": str(e)}), 500

    finally:
        # Ensure that the session is closed after the request
        db_session.close()
