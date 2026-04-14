# app/routes/purchases/__init__.py

from flask import Blueprint

purchases_bp = Blueprint('purchases', __name__, url_prefix='/purchases')

# Import submodules so routes get registered
from . import direct_purchase, apis, purchase_orders, purchase_transactions, goods_receipt, purchase_items

__all__ = ['purchases_bp']



