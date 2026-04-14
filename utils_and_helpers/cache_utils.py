import logging

from configs import cache
from flask import current_app, request

logger = logging.getLogger(__name__)


def invalidate_stock_history_cache():
    """Invalidate all stock history cache"""
    cache.clear()
    logger.info("Stock history cache cleared")


def invalidate_stock_cache_for_filters(filters=None):
    """Invalidate cache for specific filters"""
    if filters is None:
        filters = {}

    # For simple cache, we can't easily delete by pattern, so we clear all
    # For production with Redis, we could be more specific
    cache.clear()
    logger.info("Stock history cache cleared for specific filters")


# Use this function when inventory data changes
def on_inventory_data_changed(item_id=None):
    """Call this when inventory data is modified"""
    invalidate_stock_history_cache()
    logger.info(f"Cache invalidated due to inventory change for item {item_id}")


def clear_stock_history_cache():
    try:
        cache.clear()
        current_app.logger.info("Successfully cleared entire cache")
    except Exception as e:
        current_app.logger.error(f"Cache clearing failed: {e}")


def clear_stock_list_grid_cache():
    try:
        cache.clear()
        current_app.logger.info("Successfully cleared entire cache")
    except Exception as e:
        current_app.logger.error(f"Cache clearing failed: {e}")


def clear_inventory_dashboard_cache_key():
    try:
        cache.clear()
        current_app.logger.info("Successfully cleared entire cache")
    except Exception as e:
        current_app.logger.error(f"Cache clearing failed: {e}")
