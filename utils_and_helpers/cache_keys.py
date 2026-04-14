from flask import has_request_context, request, g

# utils_and_helpers/cache_keys.py
from flask import has_request_context, request, g
import hashlib

from flask_login import current_user


def generate_cache_key(prefix, include_pagination=False, include_user=False):
    """Generate a unique cache key based on request parameters and options."""
    if not has_request_context():
        return f"{prefix}_default"

    args = request.args.to_dict()

    if not include_pagination:
        args.pop("page", None)
        args.pop("per_page", None)

    sorted_args = sorted(args.items())

    # Start with prefix
    key_parts = [prefix]

    # Add user isolation if needed
    if include_user:
        key_parts.append(str(current_user.id))
        key_parts.append(str(current_user.app_id))

    # Add hashed arguments
    args_str = str(sorted_args).encode("utf-8")
    args_hash = hashlib.md5(args_str).hexdigest()
    key_parts.append(args_hash)

    return "_".join(key_parts)


def inventory_items_cache_key():
    """Cache key for inventory items with pagination + user isolation."""
    return generate_cache_key(
        prefix="inventory_items",
        include_pagination=True,
        include_user=True
    )


def stock_history_cache_key():
    """Generate cache key for stock movement history that includes pagination"""
    args = request.args.to_dict()
    # Include pagination parameters in cache key
    page = args.get('page', '1')
    per_page = args.get('per_page', '50')

    # Create a stable key that includes all filter parameters + pagination
    key_parts = [f"{k}:{v}" for k, v in sorted(args.items())]
    key_parts.append(f"user:{current_user.id}")
    key_parts.append(f"app:{current_user.app_id}")

    return f"stock_history_{hash('_'.join(key_parts))}"


def stock_list_list_cache_key():
    """Generate cache key for stock list list with filters"""
    if not has_request_context():
        if hasattr(g, 'cache_key'):
            return g.cache_key
        return "stock_list_list_default"

    # For POST requests (AJAX filtering), use JSON data
    if request.method == 'POST' and request.is_json:
        data = request.get_json() or {}
        # Include all filter parameters in cache key, INCLUDING PAGINATION
        filter_params = {
            'hide_zero_items': data.get('hide_zero_items', False),
            'start_date': data.get('start_date'),
            'end_date': data.get('end_date'),
            'category': data.get('category'),
            'subcategory': data.get('subcategory'),
            'brand': data.get('brand'),
            'attribute': data.get('attribute'),
            'variation': data.get('variation'),
            'location': data.get('location'),
            'page': data.get('page', 1),           # Include page
            'per_page': data.get('per_page', 20)   # Include per_page
        }
        sorted_params = sorted(filter_params.items())
        key = f"stock_list_list_{current_user.id}_{current_user.app_id}_{hash(frozenset(sorted_params))}"

    # For GET requests (initial page load), use query parameters
    else:
        args = request.args.to_dict()
        # Include pagination parameters in cache key
        page = args.get('page', '1')
        per_page = args.get('per_page', '20')
        # Create a stable key that includes all parameters
        sorted_args = sorted(args.items())
        key = f"stock_list_list_{current_user.id}_{current_user.app_id}_{hash(frozenset(sorted_args))}"

    g.cache_key = key
    return key


def stock_list_grid_cache_key():
    """Generate cache key for stock list grid"""
    if not has_request_context():
        if hasattr(g, 'cache_key'):
            return g.cache_key
        return "stock_list_grid_default"

    # For grid view, include pagination parameters in cache key
    args = request.args.to_dict()
    # Include pagination parameters
    page = args.get('page', '1')
    per_page = args.get('per_page', '20')

    # Create a stable key that includes all parameters
    sorted_args = sorted(args.items())
    key = f"stock_list_grid_{current_user.id}_{current_user.app_id}_{hash(frozenset(sorted_args))}"

    g.cache_key = key
    return key

def inventory_dashboard_cache_key():
    """Generate cache key for stock history"""
    if not has_request_context():
        if hasattr(g, 'cache_key'):
            return g.cache_key
        return "inventory_dashboard_default"

    args = request.args.to_dict()
    args.pop('page', None)
    args.pop('per_page', None)
    sorted_args = sorted(args.items())
    key = f"inventory_dashboard_{hash(frozenset(sorted_args))}"

    g.cache_key = key
    return key


def sales_history_cache_key():
    return generate_cache_key("sales_history")


def ledger_cache_key():
    return generate_cache_key("general_ledger")
