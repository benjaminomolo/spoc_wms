# app/routes/payroll/__init__.py

from flask import Blueprint

payroll_bp = Blueprint('payroll', __name__, url_prefix='/payroll')

# Import submodules so routes get registered
from . import employees, payrun, post_to_ledger, dashboard, record_payment, benefits_and_deductions, \
    settings_and_administration, advances

__all__ = ['payroll_bp']
