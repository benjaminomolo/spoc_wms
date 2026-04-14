# app/routes/assets/__init__.py

from flask import Blueprint

asset_bp = Blueprint('assets', __name__, url_prefix='/assets')

# Import submodules so routes get registered
from . import asset_types, asset_management, apis, post_to_ledger, asset_dashboard, assets

__all__ = ['asset_bp']
