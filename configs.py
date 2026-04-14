import hashlib
import json
import os

from flask import request
from flask_login import current_user


# MySQL database connection details
db_username = 'omolobe'
db_password = os.environ.get('DB_PASSWORD')
db_hostname = 'omolobe.mysql.pythonanywhere-services.com'
db_name = 'omolobe$spoc_wmsdb'

# *********************************Handling Image and File uploads section ********************************************
# Define the main uploads folder and the inventory subfolder
# configs.py
UPLOAD_FOLDER_NAME = 'uploads'
UPLOAD_FOLDER_INVENTORY_NAME = 'inventory'
UPLOAD_FOLDER_QUOTATION_NAME = 'quotations'
UPLOAD_FOLDER_SALES_ORDER_NAME = 'sales_orders'
UPLOAD_FOLDER_LOGOS_NAME = 'logos'
# Add to your configuration
UPLOAD_FOLDER_FOOTER_NAME = 'footers'

# Role Permissions

ROLE_PERMISSIONS = {
    'Admin': {'can_view': True, 'can_edit': True, 'can_approve': True, 'can_administer': True},
    'Supervisor': {'can_view': True, 'can_edit': True, 'can_approve': True, 'can_administer': False},
    'Contributor': {'can_view': True, 'can_edit': True, 'can_approve': False, 'can_administer': False},
    'Viewer': {'can_view': True, 'can_edit': False, 'can_approve': False, 'can_administer': False},
}

# Flask Caching
from flask_caching import Cache

# Create the cache instance
cache = Cache(config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 300,  # 5 minutes
    'CACHE_THRESHOLD': 100,
})


def make_cache_key():
    """
    Generic cache key generator.
    Uses all request args + path + current user ID.
    Works across routes.
    """
    # Flatten request.args to dict
    args = request.args.to_dict(flat=True)
    args['user_id'] = getattr(current_user, "id", "anon")
    args['path'] = request.path
    key = f"cache:{hashlib.md5(json.dumps(args, sort_keys=True).encode()).hexdigest()}"
    return key
