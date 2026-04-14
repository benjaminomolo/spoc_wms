# app/routes/payroll/__init__.py

from flask import Blueprint

module_bp = Blueprint('module', __name__, url_prefix='/module')

# Import submodules so routes get registered
from . import modules_management

__all__ = ['module_bp']
