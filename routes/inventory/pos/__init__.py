# app/routes/inventory/pos/__init__.py

from flask import Blueprint


# Define a separate blueprint for POS
pos_bp = Blueprint('pos', __name__, url_prefix='/inventory/pos')

# Import the routes so they get registered
from . import pos
# Exempt the entire POS blueprint from CSRF

__all__ = ['pos_bp']
