# app/routes/payroll/employees.py
import logging
from datetime import datetime

from flask import Blueprint, jsonify, render_template, redirect, url_for, flash, request
from flask_login import login_required, current_user
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.orm import joinedload

from ai import get_base_currency, get_exchange_rate
from db import Session
from models import Company, Module, Currency, Department, Employee, \
    EmployeeBenefit, EmployeeDeduction, Vendor, PayrollTransaction, Deduction, Benefit, BenefitType, AdvancePayment
from . import payroll_bp
from utils import empty_to_none
from utils_and_helpers.date_time_utils import parse_date

logger = logging.getLogger()


@payroll_bp.route('/add_employee', methods=['GET', 'POST'])
@login_required
def add_employee():
    with Session() as db_session:
        try:
            app_id = current_user.app_id

            # Eager load the company's logo to avoid DetachedInstanceError
            company = db_session.query(Company).options(joinedload(Company.departments),
                                                        joinedload(Company.currency),
                                                        joinedload(Company.employees),
                                                        joinedload(Company.benefit_types),
                                                        joinedload(Company.deduction_types)).filter_by(
                id=app_id).first()

            role = current_user.role
            modules_data = [mod.module_name for mod in
                            db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

            if request.method == 'POST':
                try:
                    # Capture form data (ensure the fields match those in your model)
                    first_name = request.form.get('first_name').strip() or None
                    last_name = request.form.get('last_name').strip() or None
                    middle_name = request.form.get('middle_name').strip() or None
                    date_of_birth = request.form.get('date_of_birth') or None  # Format: YYYY-MM-DD
                    gender = request.form.get('gender')
                    email = request.form.get('email').strip() or None
                    emergency_contact_name = request.form.get('emergency_contact_name').strip() or None
                    emergency_contact_phone = request.form.get('emergency_contact_phone').strip() or None
                    department_id = int(request.form.get('department_id'))
                    project_id = request.form.get('project_id') or None
                    job_title = request.form.get('job_title').strip()
                    base_salary = request.form.get('base_salary').strip() or 0
                    base_currency = request.form.get('base_currency') or None
                    payment_account_number = request.form.get('payment_account_number').strip() or None
                    payment_account_name = request.form.get('payment_account_name').strip() or None
                    payment_platform = request.form.get('payment_platform').strip() or None
                    tax_id = request.form.get('tax_id').strip() or None
                    hire_date = request.form.get('hire_date')  # Format: YYYY-MM-DD
                    termination_date = request.form.get('termination_date')  # Optional, can be blank
                    last_promotion_date = request.form.get('last_promotion_date')  # Optional
                    employment_status = request.form.get('employment_status')
                    work_location = request.form.get('work_location').strip() or None
                    performance_rating = request.form.get('performance_rating')
                    skills = request.form.get('skills') or None
                    certifications = request.form.get('certifications') or None
                    notes = request.form.get('notes') or None
                    employee_id_str = request.form.get('employee_id').strip() or None
                    employee_id = empty_to_none(employee_id_str)

                    # Capture accounting information
                    payroll_expense_account_id = request.form.get('payroll_expense_account_id') or None
                    deduction_payable_account_id = request.form.get('deduction_payable_account_id') or None
                    advance_account_id = request.form.get('advance_account_id') or None
                    payable_account_id = request.form.get('payable_account_id') or None

                    # Convert date fields to date objects (if applicable)
                    date_of_birth = datetime.strptime(date_of_birth, '%Y-%m-%d') if date_of_birth else None
                    hire_date = datetime.strptime(hire_date, '%Y-%m-%d') if hire_date else None
                    termination_date = datetime.strptime(termination_date,
                                                         '%Y-%m-%d') if termination_date else None
                    last_promotion_date = datetime.strptime(last_promotion_date,
                                                            '%Y-%m-%d') if last_promotion_date else None

                    salary_type = request.form.get('salary_type')

                    # Convert accounting IDs to integers if they exist
                    if payroll_expense_account_id:
                        payroll_expense_account_id = int(payroll_expense_account_id)
                    if deduction_payable_account_id:
                        deduction_payable_account_id = int(deduction_payable_account_id)
                    if advance_account_id:
                        advance_account_id = int(advance_account_id)
                    if payable_account_id:
                        payable_account_id = int(payable_account_id)

                    # Create a new Employee instance based on the form data
                    new_employee = Employee(
                        first_name=first_name,
                        middle_name=middle_name,
                        last_name=last_name,
                        employee_id=employee_id,
                        date_of_birth=date_of_birth,
                        gender=gender,
                        email=email,
                        emergency_contact_name=emergency_contact_name,
                        emergency_contact_phone=emergency_contact_phone,
                        department_id=department_id,
                        job_title=job_title,
                        salary_type=salary_type,
                        base_salary=base_salary,
                        base_currency=base_currency,
                        payment_account_number=payment_account_number,
                        payment_account_name=payment_account_name,
                        payment_platform=payment_platform,
                        tax_id=tax_id,
                        hire_date=hire_date,
                        termination_date=termination_date,
                        last_promotion_date=last_promotion_date,
                        employment_status=employment_status,
                        work_location=work_location,
                        performance_rating=float(performance_rating) if performance_rating else None,
                        # Handle empty string,
                        skills=skills,
                        certifications=certifications,
                        notes=notes,
                        is_active=True,  # Default to active, unless you want to handle otherwise
                        app_id=app_id,  # Replace with dynamic value if needed
                        # Accounting information
                        payroll_expense_account_id=payroll_expense_account_id,
                        deduction_payable_account_id=deduction_payable_account_id,
                        advance_account_id=advance_account_id,
                        payable_account_id=payable_account_id,
                        project_id=project_id
                    )

                    # Add and commit the new employee to the database
                    db_session.add(new_employee)
                    db_session.flush()

                    # Now handle employee benefits and deductions

                    # Add Employee Benefits
                    benefits = request.form.getlist('benefits[]')  # Get selected benefits from form
                    for benefit_id in benefits:
                        if benefit_id != 'add_new_benefit':  # Skip the "Add New Benefit" option
                            new_employee_benefit = EmployeeBenefit(
                                employee_id=new_employee.id,
                                benefit_type_id=benefit_id
                            )
                            db_session.add(new_employee_benefit)

                    # Add Employee Deductions
                    deductions = request.form.getlist('deductions[]')  # Get selected deductions from form
                    for deduction_id in deductions:
                        if deduction_id != 'add_new_deduction':  # Skip the "Add New Deduction" option
                            new_employee_deduction = EmployeeDeduction(
                                employee_id=new_employee.id,
                                deduction_type_id=deduction_id
                            )
                            db_session.add(new_employee_deduction)

                    # Adding new employee to vendor table as a payee
                    # Create a new Vendor object and add it to the database
                    new_vendor = Vendor(
                        vendor_name=" ".join(filter(None, [first_name, middle_name, last_name])),
                        vendor_id=None,
                        designation="Payroll",
                        tel_contact=None,
                        email=email,
                        address=None,
                        city=None,
                        state_province=None,
                        postal_code=None,
                        country=None,
                        website=None,
                        payment_terms=None,
                        tax_id=tax_id,
                        vendor_type="Other",
                        primary_contact_person=None,
                        secondary_contact_person=None,
                        preferred_communication_method=None,
                        certifications=certifications,
                        notes=notes,
                        is_one_time=False,
                        app_id=current_user.app_id
                    )
                    db_session.add(new_vendor)
                    db_session.flush()

                    new_employee.payee_id = new_vendor.id

                    # Commit the changes for benefits and deductions
                    db_session.commit()
                    # Flash a success message
                    flash('Employee added successfully!', 'success')
                    return redirect(url_for('payroll.add_employee'))  # Replace with the actual view route for employees


                except IntegrityError as e:
                    # Handle database-related errors
                    db_session.rollback()  # Rollback in case of error
                    logger.error(f'Error Message is here {e}')
                    flash(f'Employee ID already exists', 'warning')

                except SQLAlchemyError as e:
                    # Handle database-related errors
                    db_session.rollback()  # Rollback in case of error
                    logger.error(f'Error Message is here {e}')
                    flash(f'An error occurred while adding the employee: {str(e)}', 'error')

                except Exception as e:
                    # Handle general errors
                    db_session.rollback()
                    logger.error(f'Error Message is here {e}')
                    flash(f'An unexpected error occurred: {str(e)}', 'error')

            # If it's a GET request, render the form for adding an employee
            return render_template('/payroll/add_employee.html', company=company, modules=modules_data, role=role,
                                   module_name="Payroll")

        except Exception as e:
            # Handle any exceptions that occur outside the POST block
            logger.error(f'Error in add_employee route: {e}')
            flash(f'An error occurred: {str(e)}', 'error')
            return redirect(url_for('payroll.add_employee'))


@payroll_bp.route('/edit_employee/<int:id>', methods=['GET', 'POST'])
@login_required
def update_employee(id):
    with Session() as db_session:
        try:
            app_id = current_user.app_id

            # Eager load the company's logo and other related data
            company = db_session.query(Company).options(
                joinedload(Company.departments),
                joinedload(Company.currency),
                joinedload(Company.employees),
                joinedload(Company.benefit_types),
                joinedload(Company.deduction_types)
            ).filter_by(id=app_id).first()

            role = current_user.role
            modules_data = [mod.module_name for mod in
                            db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

            # Fetch the employee details with accounting relationships
            employee = db_session.query(Employee).options(
                joinedload(Employee.employee_benefits),
                joinedload(Employee.employee_deductions),
                joinedload(Employee.payee)
            ).filter_by(id=id, app_id=app_id).first()

            if not employee:
                flash("Employee not found", "error")
                return redirect(url_for('payroll.employee_list'))

            # Get employee's selected benefits and deductions
            employee_benefit_ids = [benefit.benefit_type_id for benefit in employee.employee_benefits]
            employee_deduction_ids = [deduction.deduction_type_id for deduction in employee.employee_deductions]

            if request.method == 'POST':
                try:
                    # Capture form data
                    first_name = request.form.get('first_name') or employee.first_name
                    last_name = request.form.get('last_name') or employee.last_name
                    middle_name = (request.form.get('middle_name') or "").strip() or employee.middle_name
                    date_of_birth = request.form.get('date_of_birth') or employee.date_of_birth
                    gender = request.form.get('gender') or employee.gender
                    email = (request.form.get('email') or "").strip() or employee.email
                    emergency_contact_name = (request.form.get(
                        'emergency_contact_name') or "").strip() or employee.emergency_contact_name
                    emergency_contact_phone = (request.form.get(
                        'emergency_contact_phone') or "").strip() or employee.emergency_contact_phone
                    department_id = int(request.form.get('department_id')) if request.form.get(
                        'department_id') else employee.department_id
                    project_id_str = request.form.get('project_id') if request.form.get(
                        'project_id') else employee.project_id
                    job_title = request.form.get('job_title') or employee.job_title
                    salary_type = request.form.get('salary_type') or employee.salary_type
                    base_salary = float(request.form.get('base_salary')) if request.form.get(
                        'base_salary') else employee.base_salary
                    base_currency = request.form.get('base_currency') or employee.base_currency
                    payment_account_number = (request.form.get(
                        'payment_account_number') or "").strip() or employee.payment_account_number
                    payment_account_name = (request.form.get(
                        'payment_account_name') or "").strip() or employee.payment_account_name
                    payment_platform = (request.form.get('payment_platform') or "").strip() or employee.payment_platform
                    tax_id = (request.form.get('tax_id') or "").strip() or employee.tax_id
                    hire_date = request.form.get('hire_date') or employee.hire_date
                    termination_date = request.form.get('termination_date') or employee.termination_date
                    last_promotion_date = request.form.get('last_promotion_date') or employee.last_promotion_date
                    employment_status = request.form.get('employment_status') or employee.employment_status
                    work_location = (request.form.get('work_location') or "").strip() or employee.work_location
                    performance_rating = float(request.form.get('performance_rating')) if request.form.get(
                        'performance_rating') else employee.performance_rating
                    skills = request.form.get('skills') or employee.skills
                    certifications = request.form.get('certifications') or employee.certifications
                    notes = request.form.get('notes') or employee.notes
                    employee_id_str = request.form.get('employee_id').strip()

                    # Capture accounting information
                    payroll_expense_account_id = request.form.get('payroll_expense_account_id') or None
                    deduction_payable_account_id = request.form.get('deduction_payable_account_id') or None
                    advance_account_id = request.form.get('advance_account_id') or None
                    payable_account_id = request.form.get('payable_account_id') or None

                    employee_id = empty_to_none(employee_id_str)
                    project_id = empty_to_none(project_id_str)

                    # Convert date fields to datetime objects
                    date_of_birth = parse_date(request.form.get('date_of_birth') or employee.date_of_birth)
                    hire_date = parse_date(request.form.get('hire_date') or employee.hire_date)
                    termination_date = parse_date(request.form.get('termination_date') or employee.termination_date)
                    last_promotion_date = parse_date(
                        request.form.get('last_promotion_date') or employee.last_promotion_date)

                    # Convert accounting IDs to integers if they exist
                    if payroll_expense_account_id:
                        payroll_expense_account_id = int(payroll_expense_account_id)
                    if deduction_payable_account_id:
                        deduction_payable_account_id = int(deduction_payable_account_id)
                    if advance_account_id:
                        advance_account_id = int(advance_account_id)
                    if payable_account_id:
                        payable_account_id = int(payable_account_id)

                    # Update the employee instance
                    employee.first_name = first_name
                    employee.middle_name = middle_name
                    employee.last_name = last_name
                    employee.employee_id = employee_id
                    employee.date_of_birth = date_of_birth
                    employee.gender = gender
                    employee.email = email
                    employee.emergency_contact_name = emergency_contact_name
                    employee.emergency_contact_phone = emergency_contact_phone
                    employee.project_id = project_id
                    employee.department_id = department_id
                    employee.job_title = job_title
                    employee.salary_type = salary_type
                    employee.base_salary = base_salary
                    employee.base_currency = base_currency
                    employee.payment_account_number = payment_account_number
                    employee.payment_account_name = payment_account_name
                    employee.payment_platform = payment_platform
                    employee.tax_id = tax_id
                    employee.hire_date = hire_date
                    employee.termination_date = termination_date
                    employee.last_promotion_date = last_promotion_date
                    employee.employment_status = employment_status
                    employee.work_location = work_location
                    employee.performance_rating = performance_rating
                    employee.skills = skills
                    employee.certifications = certifications
                    employee.notes = notes
                    employee.is_active = True

                    # Update accounting information
                    employee.payroll_expense_account_id = payroll_expense_account_id
                    employee.deduction_payable_account_id = deduction_payable_account_id
                    employee.advance_account_id = advance_account_id
                    employee.payable_account_id = payable_account_id

                    # Update the vendor table as well
                    if employee.payee:
                        employee.payee.vendor_name = " ".join(filter(None, [first_name, middle_name, last_name]))
                        employee.payee.email = email
                        employee.payee.tax_id = tax_id
                        employee.payee.certifications = certifications
                        employee.payee.notes = notes
                    else:
                        # Create a new Vendor object and add it to the database
                        new_vendor = Vendor(
                            vendor_name=" ".join(filter(None, [first_name, middle_name, last_name])),
                            vendor_id=None,
                            designation="Payroll",
                            tel_contact=None,
                            email=email,
                            address=None,
                            city=None,
                            state_province=None,
                            postal_code=None,
                            country=None,
                            website=None,
                            payment_terms=None,
                            tax_id=tax_id,
                            vendor_type="Other",
                            primary_contact_person=None,
                            secondary_contact_person=None,
                            preferred_communication_method=None,
                            certifications=certifications,
                            notes=notes,
                            is_one_time=False,
                            app_id=current_user.app_id
                        )
                        db_session.add(new_vendor)
                        db_session.flush()

                    # Handle employee benefits update
                    benefits = request.form.getlist('benefits[]')
                    db_session.query(EmployeeBenefit).filter_by(employee_id=employee.id).delete()
                    for benefit_id in benefits:
                        if benefit_id != 'add_new_benefit':
                            new_employee_benefit = EmployeeBenefit(
                                employee_id=employee.id,
                                benefit_type_id=benefit_id
                            )
                            db_session.add(new_employee_benefit)

                    # Handle employee deductions update
                    deductions = request.form.getlist('deductions[]')
                    db_session.query(EmployeeDeduction).filter_by(employee_id=employee.id).delete()
                    for deduction_id in deductions:
                        if deduction_id != 'add_new_deduction':
                            new_employee_deduction = EmployeeDeduction(
                                employee_id=employee.id,
                                deduction_type_id=deduction_id
                            )
                            db_session.add(new_employee_deduction)

                    # Commit changes for benefits and deductions
                    db_session.commit()

                    flash('Employee updated successfully!', 'success')
                    referrer = request.referrer
                    if referrer:
                        return redirect(referrer)
                    else:
                        return redirect(url_for('payroll.view_employee', id=id))

                except IntegrityError as e:
                    db_session.rollback()
                    logger.error(f'IntegrityError: {e}')
                    flash('Employee ID already exists', 'warning')

                except SQLAlchemyError as e:
                    db_session.rollback()
                    logger.error(f'SQLAlchemyError: {e}')
                    flash(f'An error occurred while updating the employee: {str(e)}', 'error')

                except Exception as e:
                    db_session.rollback()
                    logger.error(f'Exception: {e}')
                    flash(f'An unexpected error occurred: {str(e)}', 'error')

            # If it's a GET request, render the form with the existing employee data
            return render_template('/payroll/edit_employee.html',
                                   company=company,
                                   modules=modules_data,
                                   role=role,
                                   employee=employee,
                                   employee_benefit_ids=employee_benefit_ids,
                                   employee_deduction_ids=employee_deduction_ids,
                                   module_name="Payroll")

        except Exception as e:
            logger.error(f'Error in update_employee route: {e}')
            flash(f'An error occurred: {str(e)}', 'error')
            return redirect(url_for('payroll.employee_list'))


@payroll_bp.route('/employee_profile/<int:id>', methods=['GET', 'POST'])
def view_employee(id):
    db_session = Session()  # Initialize the session
    try:
        app_id = current_user.app_id

        # Fetch the employee details
        employee = db_session.query(Employee).filter_by(id=id).first()

        if not employee:
            logger.warning(f"Employee with ID {id} not found.")
            return "Employee not found", 404

        # Fetch company details
        company = db_session.query(Company).filter_by(id=app_id).first()

        if not company:
            logger.warning(f"Company with ID {app_id} not found.")
            return "Company not found", 404

        # Fetch role and modules data
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch payroll transactions for the employee
        payroll_transactions = db_session.query(PayrollTransaction).filter_by(employee_id=id).all()

        # Fetch deductions for the employee
        deductions = db_session.query(Deduction).filter_by(employee_id=id).all()

        # Fetch benefits for the employee
        benefits = db_session.query(Benefit).filter_by(employee_id=id).all()
        employee_benefits = db_session.query(EmployeeBenefit).filter_by(employee_id=id).all()
        employee_deductions = db_session.query(EmployeeDeduction).filter_by(employee_id=id).all()

        # Render the template with all the data
        return render_template(
            '/payroll/employee_profile.html',
            employee=employee,
            payroll_transactions=payroll_transactions,
            deductions=deductions,
            benefits=benefits,  # Fixed variable name (benefit -> benefits)
            company=company,
            employee_benefits=employee_benefits,
            employee_deductions=employee_deductions,
            modules=modules_data,
            role=role,
            module_name="Payroll"
        )

    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"An error occurred: {e}")
        return "An error occurred while processing your request.", 500

    finally:
        # Ensure the session is always closed
        db_session.close()


@payroll_bp.route('/api/employees/<string:employee_id>', methods=['GET'])
def get_employee(employee_id):
    try:
        db_session = Session()
        # Query the database for the employee with the given employee_id
        employee = db_session.query(Employee).filter(Employee.id == employee_id).first()

        if employee:
            # Return the employee data, including base_currency
            return jsonify({
                "id": employee.id,
                "employee_id": employee.employee_id,
                "first_name": employee.first_name,
                "middle_name": employee.middle_name,
                "last_name": employee.last_name,
                "date_of_birth": employee.date_of_birth.isoformat() if employee.date_of_birth else None,
                "gender": employee.gender,
                "email": employee.email,
                "emergency_contact_name": employee.emergency_contact_name,
                "emergency_contact_phone": employee.emergency_contact_phone,
                "department_id": employee.department_id,
                "job_title": employee.job_title,
                "address": employee.address,
                "salary_type": employee.salary_type,
                "base_salary": employee.base_salary,
                "base_currency_id": employee.base_currency,
                "payment_account_number": employee.payment_account_number,
                "payment_account_name": employee.payment_account_name,
                "payment_platform": employee.payment_platform,
                "tax_id": employee.tax_id,
                "hire_date": employee.hire_date.isoformat() if employee.hire_date else None,
                "termination_date": employee.termination_date.isoformat() if employee.termination_date else None,
                "last_promotion_date": employee.last_promotion_date.isoformat() if employee.last_promotion_date else None,
                "employment_status": employee.employment_status,
                "work_location": employee.work_location,
                "performance_rating": employee.performance_rating if employee.performance_rating else None,
                "skills": employee.skills,
                "certifications": employee.certifications,
                "notes": employee.notes,
                "is_active": employee.is_active,
                "creation_date": employee.creation_date.isoformat() if employee.creation_date else None,
                "updated_at": employee.updated_at.isoformat() if employee.updated_at else None,
                "app_id": employee.app_id,
                # Relationships (if needed, you can expand these)
                "company": employee.company.name if employee.company else None,
                "base_currency": employee.currency.user_currency if employee.currency else None

            })
        else:
            # Return a 404 error if the employee is not found
            return jsonify({"error": "Employee not found"}), 404

    except Exception as e:
        # Handle any errors that occur during the query
        return jsonify({"error": str(e)}), 500


@payroll_bp.route('/get_employee_summary', methods=['POST'])
def get_employee_summary():
    db_session = Session()
    try:
        data = request.get_json()
        employee_ids = data.get('employee_ids', [])
        employees = db_session.query(Employee).filter(Employee.id.in_(employee_ids)).all()

        total_gross = sum(employee.base_salary for employee in employees)
        total_deductions = sum(employee.deductions for employee in employees)
        total_benefits = sum(employee.benefits for employee in employees)
        total_net = total_gross - total_deductions + total_benefits

        return jsonify({
            'employees': [{'id': emp.id, 'name': f'{emp.first_name} {emp.last_name}'} for emp in employees],
            'total_gross': total_gross,
            'total_deductions': total_deductions,
            'total_benefits': total_benefits,
            'total_net': total_net
        })
    finally:
        db_session.close()  # Ensures session is closed even if an error occurs


@payroll_bp.route('/get_employee_details', methods=['POST'])
@login_required
def get_employee_details():
    from payroll import calculate_salary, get_payroll_frequency
    db_session = Session()
    try:
        selected_employee_ids = request.json.get('employee_ids', [])
        payrun_number = request.json.get('payrun_number')
        start_date_str = request.json.get('start_date')  # Get start date from request
        end_date_str = request.json.get('end_date')  # Get end date from request

        # Convert string dates to date objects
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

        # Determine payroll frequency
        try:
            payroll_frequency = get_payroll_frequency(start_date, end_date)
        except Exception as e:
            logger.error(f'Eorr is {e}')
            return jsonify({
                "error": str(e),
                "message": f"{e}. Please choose a valid period."
            }), 400  # Send error response to frontend

        app_id = current_user.app_id

        employees = db_session.query(Employee).filter(
            Employee.id.in_(selected_employee_ids),
            Employee.app_id == app_id,
            Employee.is_active == True
        ).all()

        if not employees:
            return jsonify({
                'success': False,
                'message': 'No employees found for the selected IDs.'
            }), 404

        currency_options = db_session.query(Currency).filter_by(app_id=app_id).all()

        employee_details = []
        for employee in employees:

            salary = calculate_salary(employee.base_salary, employee.salary_type, payroll_frequency)

            # Convert benefits to base currency
            benefits = []
            for eb in employee.employee_benefits:
                benefit_amount = eb.benefit_types.fixed_amount or 0
                benefit_currency = eb.benefit_types.currency  # Get the currency object

                # Check if currency is not None before attempting conversion
                if benefit_currency is not None:
                    benefit_currency_id = benefit_currency.id
                    if benefit_currency_id != employee.base_currency:
                        try:
                            rate = get_exchange_rate(db_session, benefit_currency_id, employee.base_currency, app_id)
                            benefit_amount = benefit_amount * rate
                            print(f'benefit amount is {benefit_amount}, benefit_currency_id {benefit_currency_id}')

                        except Exception as e:
                            logger.error(f"Error converting benefit amount for employee {employee.id}: {str(e)}")
                            benefit_amount = 0  # Default to 0 if conversion fails
                else:
                    benefit_currency_id = None  # Currency is None, so no conversion needed

                benefits.append({
                    'benefit_type': eb.benefit_types.name,
                    'benefit_type_id': eb.benefit_types.id,
                    'rate': eb.benefit_types.rate or 0,
                    'currency': benefit_currency_id,
                    'amount': benefit_amount
                })

            # Convert deductions to base currency
            deductions = []
            for ed in employee.employee_deductions:
                deduction_amount = ed.deduction_type.fixed_amount or 0
                deduction_currency = ed.deduction_type.currency  # Get the currency object

                # Check if currency is not None before attempting conversion
                if deduction_currency is not None:
                    deduction_currency_id = deduction_currency.id
                    if deduction_currency_id != employee.base_currency:
                        try:
                            rate = get_exchange_rate(db_session, deduction_currency_id, employee.base_currency, app_id)
                            deduction_amount = deduction_amount * rate
                        except Exception as e:
                            logger.warning(f"Error converting deduction amount for employee {employee.id}: {str(e)}")
                            deduction_amount = 0  # Default to 0 if conversion fails
                else:
                    deduction_currency_id = None  # Currency is None, so no conversion needed

                deductions.append({
                    'deduction_type': ed.deduction_type.name,
                    'deduction_id': ed.deduction_type.id,
                    'rate': ed.deduction_type.rate or 0,
                    'currency': deduction_currency_id,
                    'amount': deduction_amount
                })

            # Fetch advance payments for the employee
            advance_payments = db_session.query(AdvancePayment).filter(
                AdvancePayment.employee_id == employee.id,
                AdvancePayment.repayment_status.in_(['Pending', 'Partly Paid'])
                # Only include unpaid or partly paid advances
            ).all()

            # Convert advance payments to a structured format
            advances = []
            for ap in advance_payments:
                # Ensure values are not None before converting to float
                deduction_per_payroll = float(ap.deduction_per_payroll or 0)
                remaining_balance = float(ap.remaining_balance or 0)

                # Ensure deduction does not exceed remaining balance
                adjusted_deduction = min(deduction_per_payroll, remaining_balance)

                advances.append({
                    'id': ap.id,
                    'advance_amount': ap.advance_amount,
                    'remaining_balance': ap.remaining_balance,  # Include any remaining balance
                    'currency': ap.currency_id,  # Assuming advance payment has a currency field
                    'date_issued': ap.payment_date.strftime('%Y-%m-%d'),
                    'repayment_status': ap.repayment_status,
                    'deduction_per_payroll': float(adjusted_deduction)  # Send adjusted deduction
                })

            # Attach to employee data
            employee_data = {
                'payrun_number': payrun_number,
                'id': employee.id,
                'first_name': employee.first_name,
                'last_name': employee.last_name,
                'salary_type': employee.salary_type,
                'base_salary': employee.base_salary,
                'calculated_salary': salary,
                'currency': employee.base_currency,
                'currency_id': employee.base_currency,
                'currency_user_currency': employee.currency.user_currency,
                'benefit_types': benefits,
                'deduction_types': deductions,
                'advance_payments': advances,  # Add advances here
                'currency_options': [{'currency_id': c.id, 'currency_name': c.user_currency} for c in currency_options]
            }

            employee_details.append(employee_data)

        return jsonify({
            'success': True,
            'employees': employee_details
        })

    except Exception as e:
        logger.error(f"Error fetching employee details: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while fetching employee details.'
        }), 500

    finally:
        db_session.close()
