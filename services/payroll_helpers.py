from datetime import datetime, timedelta, date
from sqlalchemy.orm import Session

from models import PayrollPeriod, Employee, PayrollTransaction, PayRollStatusEnum, AdvancePayment
from utils import convert_to_base_currency


def generate_next_pay_run_number(db_session: Session, app_id: int):
    now = datetime.now()
    month_year = now.strftime("%Y%m")  # e.g., "202502" for February 2025

    # Get a lock on the last payroll period row
    last_pay_run = db_session.query(PayrollPeriod).filter_by(
        app_id=app_id
    ).with_for_update().order_by(  # This locks the row for this transaction
        PayrollPeriod.id.desc()
    ).first()

    if last_pay_run and last_pay_run.payroll_period_name.startswith(f"PR-{month_year}"):
        # Extract the last sequence number and increment it
        last_number_part = int(last_pay_run.payroll_period_name.split('-')[-1])
        next_number = last_number_part + 1
    else:
        # Start new sequence if no previous record or new month
        next_number = 1

    # Format the sequence number with leading zeros
    sequence_number = str(next_number).zfill(5)  # Ensures it remains 5 digits (00001, 00002, etc.)

    # Generate the new pay run period name
    new_pay_run_name = f"PR-{month_year}-{sequence_number}"

    return new_pay_run_name


def get_payroll_frequency(start_date: date, end_date: date) -> str:
    """
    Determine the payroll frequency based on the start and end dates.

    Args:
        start_date (date): Start date of the pay
        roll period.
        end_date (date): End date of the payroll period.

    Returns:
        str: The frequency of the payroll period (e.g., 'Monthly', 'Weekly', etc.).
    """
    # Calculate the difference between the dates
    delta = end_date - start_date

    # Determine the payroll frequency based on the duration
    if timedelta(days=27) <= delta <= timedelta(days=32):  # Check if delta is between 28 and 32 days inclusive
        return 'Monthly'
    elif delta == timedelta(days=7) or delta == timedelta(days=8):
        return 'Weekly'
    elif delta == timedelta(days=14) or delta == timedelta(days=16):
        return 'Bi-weekly'
    elif delta == timedelta(days=1):
        return 'Daily'
    else:
        raise ValueError(
            f"The selected period of {delta.days} days does not match any allowed payroll frequency (Daily, Weekly, Bi-weekly, Monthly).")


def validate_payroll_period(db_session: Session, app_id: int, start_date: date, end_date: date) -> str:
    """
    Validates the payroll period.
    Raises a ValueError if validation fails.
    """
    # Validate date format and range
    if start_date > end_date:
        raise ValueError("Start date must be before end date.")

    # Calculate the difference between the dates
    delta = end_date - start_date


    # Validate against period frequency strings
    if timedelta(days=27) <= delta <= timedelta(days=32):
        period_frequency = 'Monthly'
    elif delta == timedelta(days=7):
        period_frequency = 'Weekly'
    elif delta == timedelta(days=14):
        period_frequency = 'Bi-weekly'
    elif delta == timedelta(days=1):
        period_frequency = 'Daily'
    else:
        raise ValueError(
            f"The selected period of {delta.days} days does not match any allowed payroll frequency (Daily, Weekly, Bi-weekly, Monthly).")
    return period_frequency


def calculate_salary(base_salary, salary_type, payroll_frequency):
    """
    Calculate the salary for the current pay period based on salary type and payroll frequency.
    """

    if salary_type in ['Piece rate', 'Commission']:
        # Set salary to 0 for Piece rate or Commission
        return 0

    if salary_type == 'Monthly':
        if payroll_frequency == 'Monthly':
            return base_salary
        elif payroll_frequency == 'Bi-weekly':
            return base_salary / 2
        elif payroll_frequency == 'Weekly':
            return base_salary / 4
        elif payroll_frequency == 'Daily':
            return base_salary / 30  # Approximate
        else:
            raise ValueError(f"Unsupported payroll frequency: {payroll_frequency}")

    elif salary_type == 'Hourly':
        # Assuming 40 hours per week for hourly employees
        if payroll_frequency == 'Monthly':
            return base_salary * 40 * 4  # 40 hours/week * 4 weeks
        elif payroll_frequency == 'Bi-weekly':
            return base_salary * 40 * 2  # 40 hours/week * 2 weeks
        elif payroll_frequency == 'Weekly':
            return base_salary * 40  # 40 hours/week
        elif payroll_frequency == 'Daily':
            return base_salary * 8  # 8 hours/day
        else:
            raise ValueError(f"Unsupported payroll frequency: {payroll_frequency}")

    elif salary_type == 'Daily':
        if payroll_frequency == 'Monthly':
            return base_salary * 30  # Approximate
        elif payroll_frequency == 'Bi-weekly':
            return base_salary * 14  # 14 days
        elif payroll_frequency == 'Weekly':
            return base_salary * 7  # 7 days
        elif payroll_frequency == 'Daily':
            return base_salary
        else:
            raise ValueError(f"Unsupported payroll frequency: {payroll_frequency}")

    elif salary_type == 'Weekly':
        if payroll_frequency == 'Monthly':
            return base_salary * 4  # 4 weeks
        elif payroll_frequency == 'Bi-weekly':
            return base_salary * 2  # 2 weeks
        elif payroll_frequency == 'Weekly':
            return base_salary
        elif payroll_frequency == 'Daily':
            return base_salary / 7  # Approximate
        else:
            raise ValueError(f"Unsupported payroll frequency: {payroll_frequency}")

    elif salary_type == 'Bi-weekly':
        if payroll_frequency == 'Monthly':
            return base_salary * 2  # 2 pay periods per month
        elif payroll_frequency == 'Bi-weekly':
            return base_salary
        elif payroll_frequency == 'Weekly':
            return base_salary / 2  # Approximate
        elif payroll_frequency == 'Daily':
            return base_salary / 14  # Approximate
        else:
            raise ValueError(f"Unsupported payroll frequency: {payroll_frequency}")

    else:
        raise ValueError(f"Unsupported salary type: {salary_type}")


def calculate_total_deductions(employee, app_id):
    total_deductions = 0
    db_session = Session()

    try:
        for ed in employee.employee_deductions:
            deduction_type = ed.deduction_type
            deduction_amount = 0  # Default to 0

            if deduction_type.is_rate:
                # Deduction is percentage-based
                deduction_amount = (deduction_type.rate / 100) * employee.base_salary
            else:
                # Deduction is a fixed amount
                deduction_amount = deduction_type.fixed_amount

            deduction_currency = deduction_type.currency.id if deduction_type.currency else None

            # Convert to base currency if needed
            if deduction_currency and deduction_currency != employee.base_currency:
                deduction_amount = convert_to_base_currency(deduction_amount, deduction_currency, app_id)

            total_deductions += deduction_amount

        return total_deductions
    finally:
        db_session.close()


def calculate_total_benefits(employee, app_id):
    total_benefits = 0
    db_session = Session()

    try:
        for eb in employee.employee_benefits:
            benefit_type = eb.benefit_types
            benefit_amount = 0  # Default to 0

            if benefit_type.is_rate:
                # Benefit is percentage-based
                benefit_amount = (benefit_type.rate / 100) * employee.base_salary
            else:
                # Benefit is a fixed amount
                benefit_amount = benefit_type.fixed_amount

            benefit_currency = benefit_type.currency.id if benefit_type.currency else None

            # Convert to base currency if needed
            if benefit_currency and benefit_currency != employee.base_currency:
                benefit_amount = convert_to_base_currency(benefit_amount, benefit_currency, app_id)

            total_benefits += benefit_amount

        return total_benefits
    finally:
        db_session.close()


def update_payroll_period_status(db_session, period_id):
    """Helper to update payroll period status based on transaction balances"""
    period = db_session.query(PayrollPeriod).filter_by(id=period_id).first()
    if period:
        transactions = db_session.query(PayrollTransaction).filter_by(
            payroll_period_id=period_id
        ).all()

        if all(t.balance_due == 0 for t in transactions):
            period.payment_status = PayRollStatusEnum.PAID
        else:
            period.payment_status = PayRollStatusEnum.PARTIAL
        db_session.commit()


