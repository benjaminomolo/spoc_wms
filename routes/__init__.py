# routes/__init__.py

from .payroll import payroll_bp
from .general_ledger import general_ledger_bp
from .inventory import inventory_bp
from routes.inventory.pos import pos_bp
from .sales import sales_bp
from .modules import module_bp
from .reports import financial_reports_bp
from .fund_transfers import fund_transfers_bp
from .purchases import purchases_bp
from .assets import  asset_bp
from .multi_currency import multi_currency_bp


def register_routes(app):
    """Centralized route registration."""
    app.register_blueprint(payroll_bp)
    app.register_blueprint(general_ledger_bp)
    app.register_blueprint(inventory_bp)
    app.register_blueprint(pos_bp)
    app.register_blueprint(sales_bp)
    app.register_blueprint(module_bp)
    app.register_blueprint(financial_reports_bp)
    app.register_blueprint(fund_transfers_bp)
    app.register_blueprint(purchases_bp)
    app.register_blueprint(asset_bp)
    app.register_blueprint(multi_currency_bp)
