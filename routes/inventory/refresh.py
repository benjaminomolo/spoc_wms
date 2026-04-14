from flask_login import login_required

from configs import cache
from . import inventory_bp
import logging

from configs import cache
from flask import current_app, request, flash, url_for, redirect

logger = logging.getLogger(__name__)


@inventory_bp.route('/stock_list_grid/refresh', methods=['POST'])
@login_required
def refresh_stock_list_grid():
    """Clear cache for stock list grid and redirect back"""
    try:
        # Clear the entire cache
        cache.clear()
        logger.info("Successfully cleared entire cache for stock list grid refresh")
    except Exception as e:
        logger.error(f"Cache clearing failed: {e}")
        flash('Error clearing cache. Please try again.', 'error')

    # Redirect back to the same page with current parameters
    hide_zero_items = request.args.get('hide_zero_items', 'false')
    return redirect(url_for('inventory.stock_list_grid', hide_zero_items=hide_zero_items))


@inventory_bp.route('/stock_movement_history/refresh', methods=['POST'])
@login_required
def refresh_stock_movement_history():
    """Clear cache for stock list grid and redirect back"""
    try:
        # Clear the entire cache
        cache.clear()
        logger.info("Successfully cleared entire cache for stock list grid refresh")
    except Exception as e:
        logger.error(f"Cache clearing failed: {e}")
        flash('Error clearing cache. Please try again.', 'error')

    return redirect(url_for('inventory.stock_movement_history'))



@inventory_bp.route('/stock_list_list/refresh', methods=['POST'])
@login_required
def refresh_stock_list_list():
    """Clear cache for stock list grid and redirect back"""
    try:
        # Clear the entire cache
        cache.clear()
        logger.info("Successfully cleared entire cache for stock list grid refresh")
    except Exception as e:
        logger.error(f"Cache clearing failed: {e}")
        flash('Error clearing cache. Please try again.', 'error')

    # Redirect back to the same page with current parameters
    hide_zero_items = request.args.get('hide_zero_items', 'false')
    return redirect(url_for('inventory.stock_list_list', hide_zero_items=hide_zero_items))
