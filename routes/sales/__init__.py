# app/routes/sales/__init__.py

from flask import Blueprint

sales_bp = Blueprint('sales', __name__, url_prefix='/sales')

# Import submodules so routes get registered
from . import (sales_transaction, apis, invoices, quotations, sales_orders, delivery_notes, payment_receipts,
               customer_credits, sales_items)

__all__ = ['sales_bp']

