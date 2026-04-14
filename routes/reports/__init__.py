# app/routes/reports/__init__.py

from flask import Blueprint

financial_reports_bp = Blueprint('financial_reports', __name__, url_prefix='/financial_reports')

# Import submodules so routes get registered
from . import balance_sheet, trial_balance, income_and_expense, cash_flow

__all__ = ['financial_reports_bp']
