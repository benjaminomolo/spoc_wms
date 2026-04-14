# app/routes/multi_currency/__init__.py

from flask import Blueprint
from flask import jsonify, request
from datetime import datetime

from flask_login import login_required

multi_currency_bp = Blueprint('multi_currency', __name__, url_prefix='/multi_currency')

# Import submodules so routes get registered
from . import exchange_rates

__all__ = ['multi_currency_bp']

