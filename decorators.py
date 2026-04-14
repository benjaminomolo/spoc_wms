import functools
from functools import wraps
from flask import abort, flash, redirect, url_for, request, g, current_app
from flask_login import current_user

from configs import cache


def role_required(roles):
    """
    Decorator to restrict access to views based on user roles.

    Args:
        roles (str or list): Allowed role(s) to access the view.

    Usage:
        @role_required('Admin')
        @role_required(['Admin', 'Contributor', 'Viewer,])
    """

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Normalize single role into a list
            allowed_roles = [roles] if isinstance(roles, str) else roles

            # If not logged in, redirect to login
            if not current_user.is_authenticated:
                flash('Please log in to access this page.', 'warning')
                return redirect(url_for('auth.login', next=request.url))

            # If user role is allowed or special Admin handling
            if current_user.role in allowed_roles:
                return f(*args, **kwargs)

            if 'Contributor' in allowed_roles and current_user.role == 'Admin':
                # Allow Admin to access Contributor routes
                return f(*args, **kwargs)

            # Otherwise, deny access
            flash('You do not have permission to access this page.', 'danger')
            return redirect(request.referrer) or abort(403)

        return decorated_function

    return decorator


def require_permission(module_name, access_type='view'):
    """Decorator to protect routes based on permissions"""

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('login'))

            # Admins bypass permission checks
            if current_user.role == 'Admin':
                return f(*args, **kwargs)

            # Check permissions from g.user_permissions
            if not g.get('user_permissions', {}).get(module_name, {}).get(access_type, False):
                abort(403)  # Forbidden
            return f(*args, **kwargs)

        return decorated_function

    return decorator


from flask import request

import functools
from flask import request, current_app


def cached_route(timeout=300, key_func=None):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            # Use the existing cache object directly
            return cache.cached(timeout=timeout, key_prefix=key_func)(f)(*args, **kwargs)
        return wrapped
    return decorator
