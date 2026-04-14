# app/routes/fund_transfers/__init__.py

from flask import Blueprint

# Blueprint for fund transfers and FX
fund_transfers_bp = Blueprint(
    'fund_transfers',  # internal name
    __name__,
    url_prefix='/fund-transfers'  # URL prefix
)

# Import submodules so routes get registered
from . import fund_transfer  # add fx_routes here if needed

__all__ = ['fund_transfers_bp']

