# app/routes/general_ledger/__init__.py

from flask import Blueprint

general_ledger_bp = Blueprint('general_ledger', __name__, url_prefix='/general_ledger')

# Import submodules so routes get registered
from . import chart_of_accounts, journals, apis, vendors_and_customers

__all__ = ['general_ledger_bp']
