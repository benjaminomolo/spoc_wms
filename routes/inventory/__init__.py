# app/routes/inventory/__init__.py

from flask import Blueprint

inventory_bp = Blueprint('inventory', __name__, url_prefix='/inventory')

# Import submodules so routes get registered
from . import inventory_dashboard, inventory_items, inventory_management, inventory_reports, apis, refresh, \
    post_to_ledger, pos, backorders

__all__ = ['inventory_bp']


