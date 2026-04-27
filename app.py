import datetime
import datetime
import decimal
import logging
import os
import uuid
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from logging.handlers import RotatingFileHandler

import bcrypt
from flask import Flask, render_template, redirect, url_for, session, jsonify, abort, flash
from flask_login import LoginManager, login_user, current_user, login_required, logout_user
from flask_wtf.csrf import CSRFProtect, validate_csrf, logger, generate_csrf
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from sqlalchemy import func, and_, case, distinct
from sqlalchemy.exc import IntegrityError, NoResultFound, OperationalError, SQLAlchemyError
from sqlalchemy.orm import joinedload
from werkzeug.exceptions import BadRequest
from werkzeug.utils import secure_filename, send_from_directory

from ai import check_existing_chart_of_accounts, get_industry_template, \
    generate_chart_of_accounts, get_exchange_rate, get_or_create_exchange_rate_id, \
    get_base_currency, resolve_exchange_rate_for_transaction
from api_routes import api_routes  # Import the Blueprint
from configs import (
    UPLOAD_FOLDER_ATTACHMENTS_NAME,
    UPLOAD_FOLDER_NAME,
    UPLOAD_FOLDER_INVENTORY_NAME,
    UPLOAD_FOLDER_QUOTATION_NAME,
    UPLOAD_FOLDER_SALES_ORDER_NAME,
    UPLOAD_FOLDER_LOGOS_NAME,
    ROLE_PERMISSIONS,
    UPLOAD_FOLDER_FOOTER_NAME, cache
)
from db import Session
from downloads import download_route
from expense_routes import expense_routes
from general_ledger_routes import general_ledger_routes
from inventory_routes import inventory_routes
from models import ActivityLog, Currency, Company, Category, ChartOfAccounts, DirectPurchaseTransaction, \
    InventoryItemVariationLink, InventoryItem, InventoryLocation, InventoryItemVariation, InventoryItemAttribute, \
    InventoryCategory, InventorySubCategory, InventoryEntry, Brand, Quotation, QuotationItem, QuotationStatus, \
    QuotationNote, QuotationApproval, QuotationAttachment, QuotationHistory, QuotationStatusLog, SalesOrderStatusLog, \
    SalesOrder, SalesOrderItem, SalesTransaction, SalesOrderApproval, SalesInvoiceApproval, SalesInvoiceNote, \
    SalesPaymentStatus, SalesInvoiceStatusLog, SalesInvoice, SalesInvoiceHistory, SalesOrderHistory, \
    SalesOrderNote, SalesOrderAttachment, SalesInvoiceItem, DirectSalesTransaction, InvoiceStatus, \
    OrderStatus, PurchasePaymentStatus, PurchasePaymentAllocation, PurchaseTransaction, PurchaseOrder, \
    PurchaseOrderItem, PurchaseOrderHistory, \
    Project, PaymentMode, PayrollPayment, PayrollTransaction, PayrollPeriod, PurchaseReturn, DirectPurchaseItem, \
    AdvancePayment, \
    DeductionType, BenefitType, Employee, \
    UserPreference, GoodsReceipt, GoodsReceiptItem, AdvanceRepayment, Department, LoanApplicant, LoanApplication, \
    Notification, NotificationStatus, DeliveryNote, DeliveryStatus, DeliveryNoteNote, \
    DeliveryNoteItem, \
    Deduction, Benefit, \
    ExchangeRate, Module, Vendor, UnitOfMeasurement, User, LoanStatus, LoanHistory, CurrencyExchangeTransaction, \
    UserModuleAccess, AssetMovement, Asset
from multi_currency_routes import multi_currency_routes
from payroll_routes import payroll_routes
from public import public_routes
from purchase_routes import purchase_routes
from report_routes import report_routes
from routes import register_routes
from routes.inventory.pos.pos import pos_bp
from sales_routes import sales_routes
from send_mail import send_reset_email, generate_reset_token, notify_loan_approval, \
    notify_applicant, notify_admin, send_mail, nb_send_mail
from templates import industry_templates, CURRENCY_LIST
from user_settings_routes import company_settings_routes
from utils import apply_date_filters, get_cash_balances, is_cash_related, create_transaction, \
    validate_date, validate_quantity_and_price, validate_quantity_and_selling_price, handle_batch_variation_update, \
    check_exchange_rate_required, calculate_net_quantity, calculate_batch_available_quantity, \
    calculate_available_quantity, calculate_fifo_cogs, \
    update_inventory_quantity, generate_next_goods_receipt_number, \
    get_total_received_for_purchase, \
    update_purchase_inventory_quantity, get_or_create_default_location, get_prepaid_payments_to_apply, \
    get_first_prepaid_payment_id_by_po, apply_prepaid_payments, generate_next_return_number, \
    update_purchase_return_inventory_quantity, create_notification, \
    get_prepaid_balance_shipping, get_first_accessible_module, get_module_redirect_url, generate_unique_journal_number, \
    get_item_name, calculate_fx_gain_loss, allocate_payment
from utils_and_helpers.file_utils import file_exists, allowed_file

app = Flask(__name__)
app.register_blueprint(api_routes)
app.register_blueprint(sales_routes)
app.register_blueprint(purchase_routes)
app.register_blueprint(download_route)
app.register_blueprint(general_ledger_routes)
app.register_blueprint(payroll_routes)
app.register_blueprint(inventory_routes)
app.register_blueprint(company_settings_routes)
app.register_blueprint(report_routes)
app.register_blueprint(public_routes)
app.register_blueprint(expense_routes)
app.register_blueprint(multi_currency_routes)

# Register all routes in one go
register_routes(app)

app.secret_key = 'y8ariLg$xCrnGus'

WORDNIK_API_KEY = "w11klvtrn54p3yvsgo3rld1fp1yndpd6jchpxgib0qzxjnrx2"

# Configure session lifetime (now properly set to 1 hour)
# Session configuration (24 hours)
app.config['PERMANENT_SESSION_LIFETIME'] = datetime.timedelta(hours=24)
app.permanent_session_lifetime = datetime.timedelta(hours=24)  # Must match the config above

# CSRF configuration (8 hours - 28800 seconds)
app.config['WTF_CSRF_TIME_LIMIT'] = 28800  # 8 hours in seconds
csrf = CSRFProtect(app)

csrf.exempt(pos_bp)

# Initialize caching with the app
cache.init_app(app)

ERROR_MESSAGE = ""

# *********************************Handling Image and File uploads section ********************************************
# Define the main uploads folder and the inventory subfolder


UPLOAD_FOLDER_MAIN = os.path.join(app.root_path, UPLOAD_FOLDER_NAME)
UPLOAD_FOLDER_INVENTORY = os.path.join(UPLOAD_FOLDER_MAIN, UPLOAD_FOLDER_INVENTORY_NAME)
UPLOAD_FOLDER_QUOTATION = os.path.join(UPLOAD_FOLDER_MAIN, UPLOAD_FOLDER_QUOTATION_NAME)
UPLOAD_FOLDER_SALES_ORDER = os.path.join(UPLOAD_FOLDER_MAIN, UPLOAD_FOLDER_SALES_ORDER_NAME)
UPLOAD_FOLDER_LOGOS = os.path.join(UPLOAD_FOLDER_MAIN, UPLOAD_FOLDER_LOGOS_NAME)
UPLOAD_FOLDER_FOOTER = os.path.join(UPLOAD_FOLDER_MAIN, UPLOAD_FOLDER_FOOTER_NAME)
UPLOAD_FOLDER_ATTACHMENTS = os.path.join(UPLOAD_FOLDER_MAIN, UPLOAD_FOLDER_ATTACHMENTS_NAME)


# --- Setup variables ---
# Detect PythonAnywhere by checking if we're in a read-only environment
try:
    # Try to write a test file - if it fails, use home directory
    with open('test_write.tmp', 'w') as f:
        f.write('test')
    os.remove('test_write.tmp')
    log_file = 'app.log'  # Can write to current directory
except (IOError, OSError):
    # Can't write here, use home directory
    log_file = os.path.join(os.path.expanduser('~'), 'app.log')

log_max_bytes = int(2.5 * 1024 * 1024)  # 2.5 MB
backup_count = 0  # Delete instead of rotate
log_level = logging.DEBUG  # Capture all log levels, including debug

# --- Ensure log directory exists (optional if app.log is in root) ---
log_dir = os.path.dirname(log_file)
if log_dir:
    os.makedirs(log_dir, exist_ok=True)

# --- Create formatter ---
formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(name)s: %(message)s")

# --- Set up handlers ---
# Rotating file handler (delete contents if log exceeds 2.5MB)
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=log_max_bytes,
    backupCount=backup_count
)
file_handler.setFormatter(formatter)

# Stream handler for console output
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

# Add this logger setup (put it near your other imports)
performance_logger = logging.getLogger('performance')
performance_logger.setLevel(logging.WARNING)

# Create file handler for slow routes log
fh = logging.FileHandler('slow_routes.log')
fh.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setFormatter(formatter)
performance_logger.addHandler(fh)

# --- Configure root logger ---
logger = logging.getLogger()
logger.setLevel(log_level)

# Clear existing handlers to avoid duplication
if logger.hasHandlers():
    logger.handlers.clear()

# Add custom handlers
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Create the main uploads folder if it doesn't exist
if not os.path.exists(UPLOAD_FOLDER_MAIN):
    os.makedirs(UPLOAD_FOLDER_MAIN)

# Create the inventory subfolder if it doesn't exist
if not os.path.exists(UPLOAD_FOLDER_INVENTORY):
    os.makedirs(UPLOAD_FOLDER_INVENTORY)

# Create the quotation subfolder if it doesn't exist
if not os.path.exists(UPLOAD_FOLDER_QUOTATION):
    os.makedirs(UPLOAD_FOLDER_QUOTATION)

# Create the sales order subfolder if it doesn't exist
if not os.path.exists(UPLOAD_FOLDER_SALES_ORDER):
    os.makedirs(UPLOAD_FOLDER_SALES_ORDER)

# Create the logos subfolder if it doesn't exist
if not os.path.exists(UPLOAD_FOLDER_LOGOS):
    os.makedirs(UPLOAD_FOLDER_LOGOS)

# Update the Flask configuration
app.config['UPLOAD_FOLDER_INVENTORY'] = UPLOAD_FOLDER_INVENTORY
app.config['UPLOAD_FOLDER_LOGOS'] = UPLOAD_FOLDER_LOGOS
app.config['ALLOWED_EXTENSIONS'] = {'png', 'jpg', 'jpeg', 'gif', 'pdf', 'jpg', 'doc', 'docx'}
app.config['UPLOAD_FOLDER_ATTACHMENTS'] = UPLOAD_FOLDER_ATTACHMENTS

# Register Roboto font
pdfmetrics.registerFont(TTFont('Roboto', 'Roboto-Regular.ttf'))
pdfmetrics.registerFont(TTFont('Roboto-Bold', 'Roboto-Bold.ttf'))

from flask import request, g
import time


@app.before_request
def load_permissions_and_start_timer():
    # Start performance timer FIRST
    g.start_time = time.time()

    # Then your existing permissions code (unchanged)
    if request.method == 'POST':
        submission_key = f"{request.path}-{request.remote_addr}-{time.time() // 60}"
        if hasattr(g, 'last_submission') and g.last_submission == submission_key:
            return "Please don't double submit forms", 400
        g.last_submission = submission_key

    if current_user.is_authenticated:
        g.user_permissions = {}
        if current_user.role == 'Admin':
            g.user_permissions['*'] = {'view': True, 'edit': True, 'approve': True, 'administer': True}
        else:
            for access in current_user.module_access:
                g.user_permissions[access.module.module_name] = {
                    'view': access.can_view,
                    'edit': access.can_edit,
                    'approve': access.can_approve,
                    'administer': access.can_administer
                }


# Keep the after_request the same as above
@app.after_request
def log_performance(response):
    if hasattr(g, 'start_time'):
        duration = time.time() - g.start_time
        if duration > 2.0:
            user_info = "Anonymous"
            app_info = "None"

            if current_user.is_authenticated:
                user_info = f"{current_user.id}({current_user.role})"
                app_info = getattr(current_user, 'app_id', 'None')

            performance_logger.warning(
                f"SLOW_ROUTE - {duration:.2f}s - {request.method} {request.path} - "
                f"User: {user_info} - App: {app_info}"
            )

    return response

# Add this to your context processor
@app.context_processor
def inject_permissions():
    # Make available in templates
    def has_permission(module_name, access_type='view'):
        if current_user.is_authenticated:
            if current_user.role == 'Admin':
                return True
            return g.get('user_permissions', {}).get(module_name, {}).get(access_type, False)
        return False

    return {'has_permission': has_permission}


@app.route('/refresh-csrf', methods=['GET'])
def refresh_csrf():
    token = generate_csrf()
    response = jsonify({'csrf_token': token})
    response.headers.add('X-CSRFToken', token)  # Optional header

    return response


# *********************************Handling Image and File uploads section End ********************************************

# # SSH tunnel configuration
# sshtunnel.SSH_TIMEOUT = 10.0
# sshtunnel.TUNNEL_TIMEOUT = 10.0
#
# # PythonAnywhere SSH connection details
# ssh_hostname = 'ssh.pythonanywhere.com'
# ssh_username = 'omolobe'
# ssh_password = '#Just4pythonanywhere'
#
# # MySQL database connection details
# db_username = 'omolobe'
# db_password = '#Just4swiftresolve'
# db_hostname = 'omolobe.mysql.pythonanywhere-services.com'
# db_name = 'omolobe$creditrustdb'
#
# # Establish SSH tunnel
# # Establish SSH tunnel
# # Establish SSH tunnel without 'with' clause
# tunnel = sshtunnel.SSHTunnelForwarder(
#     ssh_hostname,
#     ssh_username=ssh_username,
#     ssh_password=ssh_password,
#     remote_bind_address=(db_hostname, 3306)  # MySQL hostname and port on PythonAnywhere
# )
#
# # Start the SSH tunnel
# tunnel.start()
# # Create SQLAlchemy engine through the SSH tunnel
# engine = create_engine(f'mysql+mysqldb://{db_username}:{db_password}@127.0.0.1:{tunnel.local_bind_port}/{db_name}',
#                        echo=True)


# Setup Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'  # Redirect to the login page if not authenticated


# Callback to reload user object from the user ID stored in the session
@login_manager.user_loader
def load_user(user_id):
    user_session = Session()
    user_session.close()
    return user_session.query(User).get(int(user_id))


# Cache for module redirect URLs
import concurrent.futures


@app.route('/', methods=["GET", "POST"])
@csrf.exempt
def login():
    if request.method == "POST":
        start_time = time.time()
        email = request.form.get('email').strip()
        password = request.form.get('password')

        user_session = Session()
        try:
            users = user_session.query(User).join(Company).filter(
                func.lower(User.email) == func.lower(email)
            ).all()

            # Parallel password checking
            matching_users = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                # Submit all password checks
                future_to_user = {
                    executor.submit(user.check_password, password): user
                    for user in users
                }

                # Collect results as they complete
                for future in concurrent.futures.as_completed(future_to_user):
                    user = future_to_user[future]
                    try:
                        if future.result():
                            matching_users.append(user)
                    except Exception as e:
                        print(f"Error checking password for user {user.id}: {e}")

            if not matching_users:
                return jsonify(success=False, message="Invalid credentials")

            if len(matching_users) == 1:
                user = matching_users[0]
                login_user(user)
                session.update({
                    'user_email': user.email,
                    'app_id': user.app_id,
                    'role': user.role,
                    'user_name': user.name,
                    'user_id': user.id
                })

                first_module = get_first_accessible_module(user, user_session)
                redirect_url = get_module_redirect_url(first_module, user.role)
                return jsonify(success=True, redirect_url=redirect_url)

            # Multiple companies - store user data in session
            pending_users = []
            for u in matching_users:
                pending_users.append({
                    'id': u.id,
                    'app_id': u.app_id,
                    'app_name': u.company.app_name,
                    'role': u.role,
                    'email': u.email,
                    'name': u.name
                })

            session['pending_users'] = pending_users

            return jsonify(
                success=True,
                users=[{
                    'id': u.id,
                    'app_name': u.company.app_name,
                    'role': u.role
                } for u in matching_users]
            )
        finally:
            user_session.close()

    return render_template('login.html')


@app.route('/select_app', methods=["POST"])
def select_app():
    selected_user_id = request.form.get('selected_user_id')
    user_session = Session()
    try:
        user = user_session.query(User).get(selected_user_id)
        if user:
            login_user(user)
            session.update({
                'user_email': user.email,
                'app_id': user.app_id,
                'user_name': user.name,
                'role': user.role,
                'user_id': user.id
            })

            # Async logging (optional optimization)
            def log_activity():
                with Session() as log_session:
                    log_session.add(ActivityLog(
                        activity="user login",
                        user=user.email,
                        details="log in successful",
                        app_id=user.app_id
                    ))
                    log_session.commit()

            import threading
            threading.Thread(target=log_activity).start()

            # Pass user object and existing session to avoid extra query
            first_module = get_first_accessible_module(user, user_session)
            redirect_url = get_module_redirect_url(first_module, user.role)

            # Keep your original response format - redirect
            return redirect(redirect_url)  # ← Keep this as is

    finally:
        user_session.close()

    return redirect(url_for('login'))


@app.route('/uploads/logos/<path:filename>')
def serve_logo(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER_LOGOS'], filename, environ=request.environ)


@app.route('/logout')
@login_required
def logout():
    logout_user()
    session.pop('user_email', None)
    session.pop('app_id', None)  # Clear app_id from session
    session.pop('user_name', None)  # Clear app_id from session
    session.pop('role', None)  # Clear app_id from session
    session.pop('user_id', None)
    return redirect(url_for('login'))


@app.route('/register', methods=["POST", "GET"])
def register():
    if request.method == "POST":
        user_session = Session()
        try:
            # --- Get and validate form data ---
            fname = request.form.get('fname', '').strip() or None
            lname = request.form.get('lname', '').strip()
            if not lname:
                return jsonify({"success": False, "message": "Last name is required"}), 400

            name = f"{fname} {lname}" if fname else lname
            email = request.form.get('email', '').strip()
            password = request.form.get('pwd', '').strip()
            company_name = request.form.get('company_name', '').strip()
            app_name = request.form.get('app_name', '').strip().lower()
            company_address = request.form.get('company_address', '').strip()
            currency = request.form.get('currency', '').strip()
            phone = None
            website = None

            if not all([email, password, company_name, app_name]):
                return jsonify({
                    "success": False,
                    "message": "All required fields must be filled"
                }), 400

            # --- Hash password ---
            hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

            # --- Check if company already exists ---
            existing_company = user_session.query(Company).filter_by(app_name=app_name).first()
            if existing_company:
                return jsonify({
                    "success": False,
                    "message": "An application with this name already exists. Please choose a different app name."
                }), 400

            # --- Create Company ---
            company = Company(
                name=company_name,
                app_name=app_name,
                email=email,
                address=company_address,
                phone=phone,
                website=website,
                package="Standard",
                logo=None,
                date_created=datetime.date.today()  # Force set if server_default isn't reliable
            )
            user_session.add(company)
            user_session.flush()

            # --- Create Admin User ---
            new_user = User(
                name=name,
                email=email,
                password=hashed_password,
                app_id=company.id,
                role="Admin"
            )
            user_session.add(new_user)
            user_session.flush()

            # --- Set base currency ---
            new_currency = Currency(
                user_currency=currency,
                currency_index=1,
                app_id=company.id
            )
            user_session.add(new_currency)

            # --- ✅ ADD PRE-GENERATED PAYMENT MODES ---
            system_payment_modes = ['Cash', 'Bank Transfer', 'Mobile Money', 'Cheque', 'Credit Card', 'Debit Card']

            for mode_name in system_payment_modes:
                payment_mode = PaymentMode(
                    payment_mode=mode_name,
                    app_id=company.id,
                    is_active=True
                )
                user_session.add(payment_mode)

            # --- Add selected modules and grant full access ---
            module_priority_map = {
                "Sales": 80,
                "Purchases": 70,
                "Payroll": 40,
                "Inventory": 50,
                "POS": 60,
                "General Ledger": 100,
                "Expenses": 90,
                "Assets": 30
            }

            for module_name in request.form.getlist('modules'):
                priority = module_priority_map.get(module_name, 0)

                new_module = Module(
                    module_name=module_name,
                    app_id=company.id,
                    priority=priority,
                    included='yes'
                )
                user_session.add(new_module)
                user_session.flush()

                access = UserModuleAccess(
                    user_id=new_user.id,
                    module_id=new_module.id,
                    can_view=True,
                    can_edit=True,
                    can_approve=True,
                    can_administer=True
                )
                user_session.add(access)

            # --- Final commit ---
            user_session.commit()

            return jsonify({
                "success": True,
                "message": "Registration successful",
                "redirect": url_for('login')
            })

        except Exception as e:
            user_session.rollback()
            logger.error(f"Registration error: {str(e)}")
            return jsonify({
                "success": False,
                "message": "An error occurred during registration"
            }), 500

        finally:
            user_session.close()

    # GET request
    return render_template('registration.html')


@app.route('/forgot-password', methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        data = request.get_json()  # Get the JSON data from the frontend
        email = data.get("email")

        # Initialize the session
        user_session = Session()

        try:
            # Check if the user exists
            user = user_session.query(User).filter_by(email=email).first()

            if user:
                token = generate_reset_token()
                # Store the token in the database (ensure it expires after a set time)
                user.reset_token = token  # Assuming a column `reset_token` in the User model
                user_session.commit()

                send_reset_email(email, token)

                return jsonify(
                    {"message": "A password reset link has been sent to your email.", "status": "success"}), 200
            else:
                return jsonify({"message": "No account found with this email address.", "status": "danger"}), 400

        except Exception as e:
            # Handle any database errors
            user_session.rollback()
            return jsonify({"message": "An error occurred while processing your request.", "status": "danger"}), 500

        finally:
            # Ensure the session is always closed
            user_session.close()

    return render_template("forgot_password.html")


@app.route('/reset-password', methods=['GET', 'POST'])
def reset_password():
    token = request.args.get('token')  # Get the token from query string

    # Create a new session
    user_session = Session()

    if request.method == "POST":
        try:
            new_password = request.form.get("new_password")
            confirm_password = request.form.get("confirm_password")

            print(f'New password: {new_password}, Confirm password: {confirm_password}')

            # Check if the token is valid
            user = user_session.query(User).filter_by(reset_token=token).first()

            if not user:
                return jsonify({"message": "Invalid or expired token.", "status": "danger"}), 400

            # Check if passwords match
            if new_password != confirm_password:
                return jsonify({"message": "Passwords do not match.", "status": "danger"}), 400

            # Hash the new password
            hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
            user.password = hashed_password
            user.reset_token = None  # Clear the reset token after successful reset
            user_session.commit()

            return jsonify({"message": "Your password has been successfully reset.", "status": "success"}), 200

        except Exception as e:
            user_session.rollback()  # Rollback the session in case of error
            return jsonify({"message": str(e), "status": "danger"}), 500

        finally:
            user_session.close()  # Ensure the session is closed

    # If it's a GET request, just render the form
    return render_template('reset_password.html')


@app.route('/add-charts-of-accounts')
@login_required
def add_chart_of_accounts():
    user_session = Session()
    try:
        chart_of_accounts_data = user_session.query(ChartOfAccounts).filter_by(app_id=current_user.app_id).all()
        categories_data = user_session.query(Category).filter_by(app_id=current_user.app_id).all()

        categories = []
        for cat in categories_data:
            if cat.account_type == "Income":
                code_name = "INC"
            elif cat.account_type == "Liability":
                code_name = "LIA"
            elif cat.account_type == "Equity":
                code_name = "EQT"
            elif cat.account_type == "Asset":
                code_name = "AST"
            else:
                code_name = "EXP"

            categories.append({
                "code": f"{code_name} {cat.category_id}",
                "name": cat.category,
                "type": cat.account_type
            })
        app_id = current_user.app_id
        company = user_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        user_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        # Build account tree structure
        account_tree = {}

        # Group by account type
        for account in chart_of_accounts_data:
            account_type = account.parent_account_type
            if account_type not in account_tree:
                account_tree[account_type] = []

            # Find or create category
            category = next((c for c in account_tree[account_type]
                             if c['name'] == account.categories.category), None)

            if not category:
                category = {
                    'name': account.categories.category,
                    'code': account.categories.category_id,
                    'subcategories': []
                }
                account_tree[account_type].append(category)

            # Add subcategory
            category['subcategories'].append({
                'name': account.sub_category,
                'code': account.sub_category_id,
                'is_cash': account.is_cash,
                'is_bank': account.is_bank,
                'is_receivable': account.is_receivable,
                'is_payable': account.is_payable
            })

        return render_template('charts-of-accounts.html', categories=categories, charts=chart_of_accounts_data,
                               company=company, role=role, modules=modules_data, account_tree=account_tree,
                               title="Chart of Accounts",
                               module_name="General Ledger")

    except Exception as e:
        flash(f'Error fetching data: {str(e)}', 'danger')
        logger.error(f'Error is {e} ')
        user_session.close()
        return redirect(request.referrer)

    finally:
        user_session.close()


@app.route('/check_chart_of_accounts', methods=['GET'])
@login_required
def check_chart_of_accounts():
    db_session = Session()
    try:
        app_id = current_user.app_id
        company_id = app_id
        existing_accounts = check_existing_chart_of_accounts(db_session, company_id)

        # Return True if no chart of accounts exists, else False
        return jsonify({"has_chart_of_accounts": bool(existing_accounts)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db_session.close()


@app.route('/update_do_not_show_again', methods=['POST'])
@login_required
def update_do_not_show_again():
    db_session = None  # Initialize session variable
    try:
        # Get the current user
        user_id = current_user.id  # Assuming `current_user` is provided by Flask-Login
        app_id = current_user.app_id  # Assuming `app_id` is part of the user model

        # Get the preference from the request
        data = request.get_json()
        preference_type = data.get('preference_type')  # e.g., 'auto_generate_chart_of_accounts'
        do_not_show_again = data.get('do_not_show_again', False)  # Can be True or False

        if not preference_type:
            return jsonify({'success': False, 'error': 'preference_type is required'}), 400

        # Create a new database session
        db_session = Session()

        # Check if the preference already exists for the user
        user_preference = db_session.query(UserPreference).filter(
            UserPreference.user_id == user_id,
            UserPreference.preference_type == preference_type
        ).first()

        # If the preference exists, update it; otherwise, create a new one
        if user_preference:
            user_preference.do_not_show_again = do_not_show_again
        else:
            user_preference = UserPreference(
                user_id=user_id,
                preference_type=preference_type,
                do_not_show_again=do_not_show_again
            )
            db_session.add(user_preference)

        # Commit the changes to the database
        db_session.commit()

        return jsonify({'success': True})

    except Exception as e:
        # Rollback in case of error
        if db_session:
            db_session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

    finally:
        # Close the session to release resources
        if db_session:
            db_session.close()


@app.route('/get_do_not_show_again', methods=['POST'])
@login_required
def get_do_not_show_again():
    db_session = None  # Initialize session variable
    try:
        # Get the current user
        user_id = current_user.id  # Assuming `current_user` is provided by Flask-Login

        # Get the preference type from the request
        data = request.get_json()
        preference_type = data.get('preference_type')  # e.g., 'auto_generate_chart_of_accounts'

        if not preference_type:
            return jsonify({'success': False, 'error': 'preference_type is required'}), 400

        # Create a new database session
        db_session = Session()

        # Fetch the user's preference
        user_preference = db_session.query(UserPreference).filter(
            UserPreference.user_id == user_id,
            UserPreference.preference_type == preference_type
        ).first()

        # Return the preference
        do_not_show_again = user_preference.do_not_show_again if user_preference else False
        return jsonify({'do_not_show_again': do_not_show_again})

    except Exception as e:
        logger.error(f'Error is {e}')
        return jsonify({'error': str(e)}), 500

    finally:
        # Close the session to release resources
        if db_session:
            db_session.close()


@app.route('/get_accounts_for_industry', methods=['GET'])
@login_required
def get_accounts_for_industry():
    try:
        # Get selected industry from the request
        industry = request.args.get('industry')
        if not industry:
            return jsonify({'error': 'Industry not selected'}), 400

        # Fetch the default accounts template for the selected industry
        accounts = get_industry_template(industry)

        # Format the accounts into a list of dictionaries to send as JSON
        formatted_accounts = [
            {
                'account_type': account["parent_account_type"],
                'category_id': account["category_id"],
                'category': account["category"],
                'sub_category_id': account["sub_category_id"],
                'sub_category': account["sub_category"],
                'report_section': account["report_section"],
                'is_bank': account["is_bank"],
                'is_cash': account["is_cash"],
                'normal_balance': account["normal_balance"]
            }
            for account in accounts
        ]

        return jsonify({'accounts': formatted_accounts})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/get_industries', methods=['GET'])
@login_required
def get_industries():
    try:
        # Extract the keys from industry_templates as the available industries
        industries = list(industry_templates.keys())

        # Format industries into a list of dictionaries
        formatted_industries = [{'id': industry, 'name': industry} for industry in industries]

        return jsonify({'industries': formatted_industries})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/auto_generate_chart_of_accounts', methods=['POST'])
@login_required
def auto_generate_chart_of_accounts():
    db_session = Session()  # Initialize database session
    try:
        # Get the current user's app_id and company_id
        app_id = current_user.app_id
        company_id = app_id

        # Get form data from the request
        industry = request.form.get('industry')  # Selected industry
        accounts_to_keep = request.form.getlist('accounts_to_keep')  # List of selected subcategory names
        do_not_show_again = request.form.get('do_not_show_again') == 'on'  # Checkbox value

        # Validate required fields
        if not industry:
            return jsonify({'success': False, 'error': 'Industry is required'}), 400

        # Call the function to generate the chart of accounts
        generate_chart_of_accounts(db_session, company_id, industry, accounts_to_keep)

        # Update the "Do Not Show Again" preference if the checkbox is checked
        if do_not_show_again:
            user_preference = db_session.query(UserPreference).filter(
                UserPreference.user_id == current_user.id,
                UserPreference.preference_type == 'auto_generate_chart_of_accounts'
            ).first()

            if user_preference:
                user_preference.do_not_show_again = True
            else:
                user_preference = UserPreference(
                    user_id=current_user.id,
                    preference_type='auto_generate_chart_of_accounts',
                    do_not_show_again=True
                )
                db_session.add(user_preference)

        # Commit the changes to the database
        db_session.commit()

        return jsonify({'success': True, 'message': 'Chart of accounts generated successfully'})

    except Exception as e:
        # Rollback in case of error
        logger.debug(f'Error is {e}')
        db_session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

    finally:
        # Close the session to release resources
        db_session.close()


@app.route('/update-subcategory/<int:id>', methods=['PUT'])
@login_required
def update_subcategory(id):
    user_session = Session()
    try:
        chart_of_account = user_session.query(ChartOfAccounts).filter_by(id=id).first()

        if chart_of_account:
            if chart_of_account.app_id != current_user.app_id:
                return jsonify({'success': False, 'error': 'Unauthorized'}), 401

            # Get JSON data from request
            data = request.get_json()
            chart_of_account.sub_category = data.get('subCategory')
            chart_of_account.sub_category_id = data.get('subCategoryId')
            chart_of_account.normal_balance = data.get('normalBalance')
            report_section_id_raw = data.get('reportSectionId')
            report_section_id = int(report_section_id_raw) if report_section_id_raw not in (None, '', 'null') else None

            # Convert JS boolean string to Python boolean
            is_bank_raw = data.get('isBank')
            is_cash_raw = data.get('isCash')

            chart_of_account.is_receivable = str(data.get('isReceivable')).lower() == 'true'
            chart_of_account.is_payable = str(data.get('isPayable')).lower() == 'true'

            chart_of_account.is_bank = str(is_bank_raw).lower() == 'true'
            chart_of_account.is_cash = str(is_cash_raw).lower() == 'true'
            chart_of_account.report_section_id = report_section_id

            user_session.commit()
            return jsonify({'success': True, 'message': 'Subcategory updated successfully'}), 200
        else:
            return jsonify({'success': False, 'error': 'Chart of Account not found'}), 404

    except SQLAlchemyError as e:
        user_session.rollback()
        logger.error(f'Error occurred: {str(e)}\n{traceback.format_exc()}')
        return jsonify({'success': False, 'error': 'Database error: ' + str(e)}), 500

    finally:
        user_session.close()


@app.route('/delete-subcategory/<int:id>', methods=['DELETE'])
@login_required
def delete_subcategory(id):
    try:
        with Session() as session:
            # Retrieve the chart of account entry by id and app_id
            chart_of_account = session.query(ChartOfAccounts).filter_by(
                id=id, app_id=current_user.app_id
            ).first()

            if not chart_of_account:
                return jsonify({'success': False, 'error': 'Chart of Account not found or unauthorized'}), 404

            # Check for transactions associated with the subcategory
            transactions_exist = session.query(Transaction).filter_by(subcategory_id=chart_of_account.id).first()
            if transactions_exist:
                return jsonify({'success': False, 'error': 'Cannot delete subcategory with existing transactions'}), 400

            # Delete the chart of account entry
            session.delete(chart_of_account)
            session.commit()

            return jsonify({'success': True, 'message': 'Subcategory deleted successfully'}), 200

    except SQLAlchemyError as e:
        print(f'error {e}')
        return jsonify({'success': False, 'error': 'Database error occurred'}), 500

    except Exception as e:
        return jsonify({'success': False, 'error': 'An unexpected error occurred'}), 500


@app.route('/get_category_id/')
@login_required
def get_category_id():
    if current_user.is_authenticated:
        db_session = Session()
        try:
            category_name = request.args.get('parent_category_name')

            # Adjusted query to filter by category name and app_id
            category = db_session.query(Category.category_id).filter_by(category=category_name,
                                                                        app_id=current_user.app_id).first()

            if category:
                return jsonify({'category_id': category[0]})
            else:
                return jsonify({'error': 'Category not found'}), 404

        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            db_session.close()

    return jsonify({'error': 'Unauthorized'}), 401


@app.route('/get_account_type_id/')
@login_required
def get_account_type_id():
    if current_user.is_authenticated:
        app_id = current_user.app_id  # Assuming current_user has a app_id attribute

        account_type_name = request.args.get('inputAccountType_name').lower()

        account_type_ids = {
            "asset": 4,
            "liability": 2,
            "equity": 3,
            "income": 1,
            "expense": 5
        }

        account_type_id = account_type_ids.get(account_type_name, 6)  # Default to 6 if not found

        # Optionally, you can query the database based on app_id
        # Example: Retrieve category ID based on category name and app_id
        db_session = Session()  # Assuming you have a session configured
        category = db_session.query(Category.category_id).filter_by(category=account_type_name,
                                                                    app_id=app_id).first()

        if category:
            account_type_id = category.category_id

        db_session.close()

        return jsonify({'account_type_id': account_type_id})
    else:
        return jsonify({'error': 'Unauthorized'}), 401


@app.route('/manage_projects', methods=['GET', 'POST'])
@login_required
def manage_projects():
    db_session = Session()
    try:
        app_id = current_user.app_id

        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Get distinct locations and descriptions for filter dropdowns
        locations = [loc[0] for loc in db_session.query(Project.location).filter_by(app_id=app_id).distinct().all() if loc[0]]
        descriptions = [desc[0] for desc in db_session.query(Project.description).filter_by(app_id=app_id).distinct().all() if desc[0]]

        # Get all projects for display
        projects = [{
            "id": project.id,
            "name": project.name,
            "project_id": project.project_id,
            "location": project.location,
            "description": project.description,
            "is_active": project.is_active if hasattr(project, 'is_active') else True
        } for project in db_session.query(Project).filter_by(app_id=app_id).all()]

        if request.method == 'POST':
            try:
                action_type = request.form.get('type')

                if action_type == 'add':
                    project_id = request.form.get('project_id', '').strip()

                    # Validate required fields
                    if not project_id:
                        flash('Project ID is required.', 'danger')
                        return redirect(url_for('manage_projects'))

                    # Check if project_id already exists within the same company
                    existing_project = db_session.query(Project).filter_by(project_id=project_id, app_id=app_id).first()
                    if existing_project:
                        flash('Project ID already exists within your company. Please choose another.', 'danger')
                    else:
                        new_project = Project(
                            name=request.form.get('name', '').strip(),
                            project_id=project_id,
                            location=request.form.get('location', '').strip(),
                            description=request.form.get('description', '').strip(),
                            app_id=app_id,
                            is_active=True
                        )
                        db_session.add(new_project)

                        # Store activity log
                        new_log = ActivityLog(
                            activity="project_management",
                            user=current_user.email,
                            details=f"Project added: {new_project.name} ({project_id})",
                            app_id=current_user.app_id
                        )
                        db_session.add(new_log)

                        db_session.commit()
                        flash('Project added successfully!', 'success')

                elif action_type == 'update':
                    project_id = request.form.get('id')
                    project = db_session.query(Project).filter_by(id=project_id, app_id=app_id).first()

                    if project:
                        # Check if the new project_id is already taken by another project
                        new_project_id = request.form.get('project_id', '').strip()
                        if new_project_id and new_project_id != project.project_id:
                            existing = db_session.query(Project).filter_by(project_id=new_project_id, app_id=app_id).first()
                            if existing:
                                flash('Project ID already exists within your company. Please choose another.', 'danger')
                                return redirect(url_for('manage_projects'))

                        # Update project details
                        project.name = request.form.get('name', '').strip()
                        project.project_id = new_project_id
                        project.location = request.form.get('location', '').strip()
                        project.description = request.form.get('description', '').strip()

                        # Update active status if field exists
                        if hasattr(project, 'is_active'):
                            project.is_active = request.form.get('is_active', 'on') == 'on'

                        # Store activity log
                        new_log = ActivityLog(
                            activity="project_management",
                            user=current_user.email,
                            details=f"Project updated: {project.name} ({new_project_id})",
                            app_id=current_user.app_id
                        )
                        db_session.add(new_log)

                        db_session.commit()
                        flash('Project updated successfully!', 'success')
                    else:
                        flash('Project not found.', 'error')

                elif action_type == 'delete':
                    project_id = request.form.get('id')
                    project = db_session.query(Project).filter_by(id=project_id, app_id=app_id).first()

                    if project:
                        # Check if project is being used in any assets
                        assets_using_project = db_session.query(Asset).filter_by(project_id=project.project_id, app_id=app_id).first()

                        # Check if project is being used in any asset movements
                        movements_using_project = db_session.query(AssetMovement).filter_by(project_id=project.id, app_id=app_id).first()

                        # Check if project is being used in inventory (if applicable)
                        inventory_using_project = db_session.query(InventoryEntry).filter_by(project_id=project.id, app_id=app_id).first()

                        if assets_using_project or movements_using_project or inventory_using_project:
                            flash('Cannot delete project. It is currently being used in assets or movements.', 'danger')
                        else:
                            # Option 1: Soft delete (set inactive)
                            if hasattr(project, 'is_active'):
                                project.is_active = False
                                flash('Project deactivated successfully!', 'success')
                            else:
                                # Option 2: Hard delete
                                db_session.delete(project)
                                flash('Project deleted successfully!', 'success')

                            # Store activity log
                            new_log = ActivityLog(
                                activity="project_management",
                                user=current_user.email,
                                details=f"Project deleted: {project.name} ({project.project_id})",
                                app_id=current_user.app_id
                            )
                            db_session.add(new_log)

                            db_session.commit()
                    else:
                        flash('Project not found.', 'error')

                return redirect(url_for('manage_projects'))

            except IntegrityError as e:
                db_session.rollback()
                logger.error(f"IntegrityError in manage_projects: {str(e)}")
                flash('Database integrity error occurred. This project ID may already be in use.', 'danger')
            except Exception as e:
                db_session.rollback()
                logger.error(f"Error in manage_projects: {str(e)}\n{traceback.format_exc()}")
                flash(f'An error occurred: {str(e)}', 'danger')

        return render_template('manage_projects.html',
                               projects=projects,
                               locations=locations,
                               descriptions=descriptions,
                               company=company,
                               role=role,
                               modules=modules_data,
                               title="Manage Departments",
                               module_name="Warehouse Management")

    except Exception as e:
        logger.error(f"Unexpected error in manage_projects: {str(e)}\n{traceback.format_exc()}")
        flash('An unexpected error occurred', 'error')
        return redirect(url_for('dashboard'))
    finally:
        db_session.close()
 

# Route for adding vendor


# Route for submitting vendor data

# Corrected not exported


# ********************** Payment Mode Routes ************************************
@app.route('/add_payment_mode_form')
@login_required
def add_payment_mode_form():
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()

        return render_template('payment_mode.html', payment_modes=payment_modes,
                               company=company, role=role, modules=modules_data, module_name="General Ledger")



    except OperationalError:
        db_session.rollback()
        return redirect(url_for('add_payment_mode_form'))

    except Exception as e:
        db_session.rollback()
        flash(f'An error occurred while retrieving data: {str(e)}', 'error')
        return redirect(url_for('add_payment_mode_form'))


    finally:
        db_session.close()


#
@app.route('/add_payment_mode', methods=['POST'])
@login_required
def add_payment_mode():
    if request.method == 'POST':
        user_session = Session()
        # Get data from the form
        payment_mode = request.form['paymentMode']

        try:
            # Check if the payment mode already exists for the current company
            existing_payment_mode = user_session.query(PaymentMode).filter_by(
                payment_mode=payment_mode,
                app_id=current_user.app_id
            ).first()

            if existing_payment_mode:
                flash('Payment mode already exists for your company. Please choose another.', 'error')
                user_session.close()
                return redirect(url_for('add_payment_mode_form'))

            # Create a new PaymentMode object and add it to the database
            new_payment_mode = PaymentMode(payment_mode=payment_mode, app_id=current_user.app_id)
            user_session.add(new_payment_mode)

            # Store login activity
            new_log = ActivityLog(
                activity="payment mode management",
                user=current_user.email,  # Assuming 'user' in ActivityLog is a string for the user's email
                details=f"{payment_mode}: has been added successfully",
                app_id=current_user.app_id
            )
            user_session.add(new_log)

            user_session.commit()
            flash('Payment mode added successfully!', 'success')
            return redirect(url_for('add_payment_mode_form'))

        except IntegrityError as e:
            user_session.rollback()
            flash(f'An error occurred while adding the payment mode. Please contact admin {e}', 'danger')

        finally:
            user_session.close()


@app.route('/edit_payment_mode/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_payment_mode(id):
    db_session = Session()
    app_id = current_user.app_id
    # Fetch company name, user role, and modules for rendering the template
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

    try:
        # Fetch the payment mode to be edited
        payment_mode = db_session.query(PaymentMode).filter_by(id=id, app_id=app_id).first()
        if not payment_mode:
            flash('Payment mode not found or you do not have permission to edit this payment mode.', 'error')
            return redirect(url_for('add_payment_mode_form'))

        if request.method == 'POST':
            new_name = request.form['paymentMode'].strip()

            # Check if the new payment mode already exists for the current company
            existing_payment_mode = db_session.query(PaymentMode).filter(
                PaymentMode.payment_mode == new_name,
                PaymentMode.app_id == app_id,
                PaymentMode.id != id
            ).first()

            if existing_payment_mode:
                flash('Payment mode already exists for your company. Please choose another.', 'danger')
            else:
                payment_mode.payment_mode = new_name

                # Store login activity
                new_log = ActivityLog(
                    activity="payment mode management",
                    user=current_user.email,  # Assuming 'user' in ActivityLog is a string for the user's email
                    details=f"{payment_mode.payment_mode} has been changed to {new_name} successfully",
                    app_id=app_id
                )
                db_session.add(new_log)
                db_session.commit()
                flash('Payment mode updated successfully!', 'success')
                return redirect(url_for('add_payment_mode_form'))

        return render_template('edit_payment_mode.html', payment_mode=payment_mode, company=company,
                               modules=modules_data, role=role)

    except IntegrityError:
        db_session.rollback()
        flash('An integrity error occurred while updating the payment mode. Please try again.', 'danger')
    except Exception as e:
        db_session.rollback()
        flash(f'An unexpected error occurred: {str(e)}', 'danger')
    finally:
        db_session.close()


@app.route('/delete_payment_mode/<int:id>', methods=['GET', 'POST'])
@login_required
def delete_payment_mode(id):
    db_session = Session()
    app_id = current_user.app_id
    try:
        payment_mode = db_session.query(PaymentMode).filter_by(id=id, app_id=app_id).first()
        if payment_mode:
            # Check if there are transactions associated with the vendor
            transactions = db_session.query(Transaction).filter_by(payment_mode_id=id,
                                                                   app_id=current_user.app_id).first()
            if transactions:
                flash('Cannot delete vendor with associated transactions.', 'danger')
            else:
                db_session.delete(payment_mode)

                # Store login activity
                new_log = ActivityLog(
                    activity="payment mode management",
                    user=current_user.email,  # Assuming 'user' in ActivityLog is a string for the user's email
                    details=f"{payment_mode.payment_mode} has been deleted successfully",
                    app_id=current_user.app_id
                )
                db_session.add(new_log)

                db_session.commit()

                flash('Vendor deleted successfully!', 'success')
        else:
            flash('Payment Mode not found or you do not have permission to delete this Payment Mode.', 'error')
    except NoResultFound:
        flash('Payment Mode not found.', 'error')

    except IntegrityError as e:
        db_session.rollback()
        flash('An error occurred while deleting the payment mode. Please try again.', 'danger')
        db_session.close()

    finally:
        db_session.close()

    return redirect(url_for('add_payment_mode_form'))


# **************************** End of Payment Mode Routes ******************************


@app.route('/add_transaction', methods=["POST", "GET"])
@login_required
def add_transaction():
    try:
        with Session() as db_session:

            app_id = current_user.app_id

            # Defensive user check
            user = db_session.query(User).filter_by(id=current_user.id).first()
            if not user or user.role != 'Admin':
                flash('You do not have permission to access this page.', 'warning')
                return redirect(url_for('general_ledger.dashboard'))

            # Fetch all required data
            transaction_categories = db_session.query(Category).filter_by(app_id=app_id).all()
            payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).order_by(
                PaymentMode.payment_mode).all()
            vendors = db_session.query(Vendor).filter_by(app_id=app_id).order_by(Vendor.vendor_name.asc()).all()
            projects = db_session.query(Project).filter_by(app_id=app_id).all()
            currencies = db_session.query(Currency).filter_by(app_id=app_id).order_by(Currency.currency_index).all()

            # Get cash/bank accounts
            cash_accounts = (
                db_session.query(ChartOfAccounts.id)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    (ChartOfAccounts.is_cash == True) | (ChartOfAccounts.is_bank == True)
                )
                .all()
            )
            cash_account_ids = [acc.id for acc in cash_accounts]

            # Get payable/receivable accounts
            payable_receivable_accounts = (
                db_session.query(ChartOfAccounts.id)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    (ChartOfAccounts.is_payable == True) | (ChartOfAccounts.is_receivable == True)
                ).all()
            )
            payable_receivable_ids = [acc.id for acc in payable_receivable_accounts]

            # Modules
            modules_data = [
                mod.module_name for mod in
                db_session.query(Module).filter_by(app_id=app_id, included='yes').all()
            ]

            # Company details
            company = db_session.query(Company).filter_by(id=app_id).scalar()

            return render_template(
                'add_transaction.html',
                transaction_categories=transaction_categories,
                payment_modes=payment_modes,
                vendors=vendors,
                project_names=projects,
                currencies=currencies,
                company=company,
                role=user.role,
                modules=modules_data,
                module_name="General Ledger",
                cash_account_ids=cash_account_ids,
                payable_receivable_ids=payable_receivable_ids
            )

    except Exception as e:
        logger.error(f'Error has occured {e}')
        traceback(f'Error has occured {e}')
        flash(f'An error occurred while fetching data: {str(e)}', 'danger')
        return redirect(request.referrer)


@app.route('/get_categories/<account_type>')
@login_required
def get_categories(account_type):
    db_session = Session()
    try:
        # Fetch categories based on account_type and app_id
        categories = db_session.query(Category).filter_by(
            account_type=account_type,
            app_id=current_user.app_id
        ).all()

        # Prepare JSON response
        category_list = [{'id': category.id, 'category_id': category.category_id, 'category_name': category.category}
                         for category in categories]

        return jsonify(category_list)

    except Exception as e:
        db_session.rollback()
        return jsonify({'error': str(e)}), 500

    finally:
        db_session.close()


@app.route('/subcategories/<int:category_id>')
@login_required
def get_subcategories(category_id):
    db_session = Session()
    try:
        # Fetch the category to verify it exists
        cat = db_session.query(Category).filter(
            Category.id == category_id,
            Category.app_id == current_user.app_id
        ).first()

        if not cat:
            return jsonify({'error': 'Category not found or you do not have permission to access it.'}), 404

        # Fetch subcategories based on category_id and app_id
        sub_cat_data_query = db_session.query(
            ChartOfAccounts.id,
            ChartOfAccounts.sub_category_id,
            ChartOfAccounts.sub_category
        ).filter(
            ChartOfAccounts.category_fk == cat.id,
            ChartOfAccounts.app_id == current_user.app_id
        ).all()

        # Prepare JSON response
        subcategories_list = [{'id': item[0], 'sub_id': item[1], 'name': item[2]} for item in sub_cat_data_query]

        return jsonify(subcategories_list)

    except Exception as e:
        db_session.rollback()
        return jsonify({'error': str(e)}), 500

    finally:
        db_session.close()


@app.route('/api/chart_of_accounts', methods=["GET"])
@login_required
def get_chart_of_accounts():
    db_session = Session()
    try:
        # Fetch the app_id from the current user
        app_id = current_user.app_id

        # Fetch chart of accounts data filtered by app_id
        chart_of_accounts_data = db_session.query(ChartOfAccounts).filter_by(app_id=app_id).all()
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Convert Company object to dictionary
        company_dict = {
            'id': company.id,
            'name': company.name,  # Add other relevant fields
            'address': company.address,  # Include fields you want in the response
            # You can add any other fields that need to be sent in the response
        }

        # Prepare JSON response for chart of accounts
        chart_of_accounts_list = [{'account_type': item.parent_account_type,
                                   'category': item.categories.category,
                                   'category_code': item.category_id,
                                   'category_id': item.category_fk,
                                   'sub_category_id': item.id,
                                   'sub_category_code': item.sub_category_id,
                                   'sub_category': item.sub_category,
                                   'is_cash': item.is_cash,
                                   'is_bank': item.is_bank,
                                   'is_receivable': item.is_receivable,
                                   'is_payable': item.is_payable}
                                  for item in chart_of_accounts_data]

        # Prepare the final response
        response = {
            'company': company_dict,  # Use the dictionary here
            'role': role,
            'modules': modules_data,
            'chart_of_accounts': chart_of_accounts_list
        }

        return jsonify(response)

    except Exception as e:
        db_session.rollback()
        return jsonify({'error': str(e)}), 500

    finally:
        db_session.close()


from collections import defaultdict


@app.route('/api/chart_of_accounts_grouped', methods=["GET"])
@login_required
def get_chart_of_accounts_grouped():
    db_session = Session()
    try:
        app_id = current_user.app_id

        chart_of_accounts_data = (
            db_session.query(ChartOfAccounts)
            .filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_system_account.is_(False)
            )
            .all()
        )

        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [
            mod.module_name
            for mod in db_session.query(Module)
            .filter_by(app_id=app_id)
            .filter_by(included='yes')
            .all()
        ]

        company_dict = {
            'id': company.id,
            'name': company.name,
            'address': company.address,
        }

        # Group accounts by category
        grouped_accounts = defaultdict(lambda: {"category": None, "category_code": None, "accounts": []})

        for item in chart_of_accounts_data:
            cat_id = item.category_fk
            grouped_accounts[cat_id]["category"] = item.categories.category
            grouped_accounts[cat_id]["category_code"] = item.category_id
            grouped_accounts[cat_id]["accounts"].append({
                'account_type': item.parent_account_type,
                'sub_category_id': item.id,
                'sub_category_code': item.sub_category_id,
                'sub_category': item.sub_category,
                'is_cash': item.is_cash,
                'is_bank': item.is_bank,
                'is_receivable': item.is_receivable,
                'is_payable': item.is_payable
            })

        # Convert defaultdict to normal dict for JSON
        grouped_accounts_list = [
            {
                'category_id': cat_id,
                'category': data['category'],
                'category_code': data['category_code'],
                'accounts': data['accounts']
            }
            for cat_id, data in grouped_accounts.items()
        ]

        response = {
            'company': company_dict,
            'role': role,
            'modules': modules_data,
            'chart_of_accounts_grouped': grouped_accounts_list
        }

        return jsonify(response)

    except Exception as e:
        db_session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()


@app.route('/api/accounts/payable/<int:payable_account_id>', methods=['GET'])
@login_required
def get_payable_account_info(payable_account_id):
    """
    Get payable account information
    """
    db_session = Session()
    try:
        account = db_session.query(ChartOfAccounts).filter_by(
            id=payable_account_id,
            app_id=current_user.app_id  # Ensure account belongs to user's company
        ).first()

        if not account:
            return jsonify({"error": "Payable account not found"}), 404

        return jsonify({
            "account_name": account.sub_category,
            "account_code": account.sub_category_id,
            "account_type": account.parent_account_type
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


import traceback



@app.route('/get_transactions', methods=["GET"])
def get_transactions():
    # Extract the API key from the request headers (adjusted to match the header key in a standard format)

    api_key = request.headers.get("X-API-Key")  # Capitalized as "X-API-Key"

    # Check if the API key is provided
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        # Query the company associated with this API key
        company = user_session.query(Company).filter_by(api_key=api_key).first()

        # Check if the API key is valid
        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        try:
            # Fetch app_id from the company record
            app_id = company.id

            # Query transactions associated with the app_id
            transactions = user_session.query(Transaction).filter_by(app_id=app_id).all()

            # Construct JSON response
            transactions_list = [
                {
                    "id": transaction.id,
                    "transaction_type": transaction.transaction_type,
                    "date": transaction.date.strftime('%Y-%m-%d'),
                    "category": transaction.category.category if transaction.category else None,
                    "subcategory": transaction.chart_of_accounts.sub_category if transaction.chart_of_accounts else None,
                    "currency": transaction.currencies.user_currency,
                    "amount": transaction.amount,
                    "dr_cr": transaction.dr_cr,
                    "description": transaction.description,
                    "payment_mode": transaction.payment_mode.payment_mode if transaction.payment_mode else None,
                    "payment_to_vendor": transaction.vendor.vendor_name if transaction.vendor else None,
                    "project_name": transaction.project.name if transaction.project else None,
                    "date_added": transaction.date_added.strftime('%Y-%m-%d'),
                    "normal_balance": transaction.chart_of_accounts.normal_balance if transaction.chart_of_accounts else None,
                    "report_section": transaction.chart_of_accounts.report_section.name if transaction.chart_of_accounts.report_section else None

                }
                for transaction in transactions
            ]

            return jsonify(transactions_list)

        except Exception as e:
            # Log the exception or handle it as needed
            print(f'Error is {e}')
            return jsonify({"error": str(e)}), 500


@app.route('/get_transactions_base_currency', methods=["GET"])
def get_transactions_base_currency():
    api_key = request.headers.get("X-API-Key")

    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        company = user_session.query(Company).filter_by(api_key=api_key).first()

        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        try:
            app_id = company.id
            base_currency_info = get_base_currency(user_session, app_id)

            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Get end_date from query params or default to today
            end_date_str = request.args.get('end_date')
            if end_date_str:
                end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
            else:
                end_date = datetime.datetime.today().date()

            transactions = user_session.query(Transaction).filter_by(app_id=app_id).all()

            # Collect distinct foreign currency IDs
            foreign_currency_ids = set(
                t.currency for t in transactions if t.currency != base_currency_id
            )

            transactions_list = []

            for transaction in transactions:
                transaction_currency_id = transaction.currency
                original_amount = transaction.amount
                transaction_currency = transaction.currencies.user_currency

                amount_in_base = original_amount

                rate = 1.0
                if transaction_currency_id != base_currency_id:
                    if transaction.exchange_rate and transaction.exchange_rate.rate:
                        rate = Decimal(transaction.exchange_rate.rate)
                    else:
                        # Optional: fallback to latest known or 1.0
                        raise Exception("An error occured. Please contact Netbooks support")

                    amount_in_base = round(Decimal(original_amount) * rate, 2)

                transactions_list.append({
                    "id": transaction.id,
                    "transaction_type": transaction.transaction_type,
                    "date": transaction.date.strftime('%Y-%m-%d'),
                    "category": transaction.chart_of_accounts.categories.category if transaction.chart_of_accounts else None,
                    "subcategory": transaction.chart_of_accounts.sub_category if transaction.chart_of_accounts else None,
                    "currency": transaction_currency,
                    "amount": float(original_amount),
                    "amount_in_base_currency": float(amount_in_base),
                    "base_currency": base_currency,
                    "dr_cr": transaction.dr_cr,
                    "description": transaction.description,
                    "payment_mode": transaction.payment_mode.payment_mode if transaction.payment_mode else None,
                    "payment_to_vendor": transaction.vendor.vendor_name if transaction.vendor else None,
                    "project_name": transaction.project.name if transaction.project else None,
                    "date_added": transaction.date_added.strftime('%Y-%m-%d'),
                    "source_type": transaction.source_type,
                    "normal_balance": transaction.chart_of_accounts.normal_balance if transaction.chart_of_accounts else None,
                    "is_cash_equivalent": (
                            transaction.chart_of_accounts.is_cash or transaction.chart_of_accounts.is_bank) if transaction.chart_of_accounts else False,
                    "report_section": transaction.chart_of_accounts.report_section.name if transaction.chart_of_accounts and transaction.chart_of_accounts.report_section else None
                })

            return jsonify(transactions_list)

        except Exception as e:
            logger.error(f'Error is {e}')
            return jsonify({"error": str(e)}), 500


@app.route('/get_cash_flow_transactions', methods=["GET"])
def get_cash_flow_transactions():
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        company = user_session.query(Company).filter_by(api_key=api_key).first()
        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        try:
            app_id = company.id

            # get all cash/bank account IDs
            cash_account_ids = {
                a.id for a in user_session.query(ChartOfAccounts.id)
                .filter(or_(ChartOfAccounts.is_cash == True,
                            ChartOfAccounts.is_bank == True))
                .all()
            }

            # pre-load relationships to avoid N+1
            transactions = (
                user_session.query(Transaction)
                .filter_by(app_id=app_id)
                .options(
                    joinedload(Transaction.currencies),
                    joinedload(Transaction.exchange_rate),
                    joinedload(Transaction.chart_of_accounts)
                    .joinedload(ChartOfAccounts.report_section),
                    joinedload(Transaction.payment_mode),
                    joinedload(Transaction.vendor),
                    joinedload(Transaction.project),
                    joinedload(Transaction.category)
                )
                .all()
            )

            # create a lookup to see if a journal affects cash
            journal_cash_status = {}
            for txn in transactions:
                if txn.journal_number not in journal_cash_status:
                    related = [t for t in transactions if t.journal_number == txn.journal_number]
                    has_cash = any(t.subcategory_id in cash_account_ids for t in related)
                    all_cash = all(t.subcategory_id in cash_account_ids for t in related)
                    # only mark journals that involve cash AND non-cash
                    affects_cash = has_cash and not all_cash
                    journal_cash_status[txn.journal_number] = affects_cash

            transactions_list = []
            for txn in transactions:
                affects_cash = journal_cash_status.get(txn.journal_number, False)
                is_cash_entry = txn.subcategory_id in cash_account_ids if affects_cash else False

                cash_flow_direction = None
                if affects_cash:
                    if is_cash_entry:
                        cash_flow_direction = (
                            "inflow" if txn.dr_cr == "Credit" else "outflow"
                        )
                    else:
                        cash_flow_direction = (
                            "outflow" if txn.dr_cr == "Credit" else "inflow"
                        )

                    transactions_list.append({
                        "id": txn.id,
                        "transaction_type": txn.transaction_type,
                        "date": txn.date.strftime('%Y-%m-%d'),
                        "category": txn.category.category if txn.category else None,
                        "subcategory": txn.chart_of_accounts.sub_category if txn.chart_of_accounts else None,
                        "currency": txn.currencies.user_currency if txn.currencies else None,
                        "amount": float(txn.amount),
                        "dr_cr": txn.dr_cr,
                        "description": txn.description,
                        "payment_mode": txn.payment_mode.payment_mode if txn.payment_mode else None,
                        "payment_to_vendor": txn.vendor.vendor_name if txn.vendor else None,
                        "project_name": txn.project.name if txn.project else None,
                        "date_added": txn.date_added.strftime('%Y-%m-%d'),
                        "normal_balance": txn.chart_of_accounts.normal_balance if txn.chart_of_accounts else None,
                        "report_section": (
                            txn.chart_of_accounts.report_section.name
                            if txn.chart_of_accounts
                               and txn.chart_of_accounts.categories
                               and txn.chart_of_accounts.report_section
                            else None
                        ),
                        "journal_number": txn.journal_number,
                        "is_cash_flow": affects_cash,
                        "is_cash_entry": is_cash_entry,
                        "cash_flow_direction": cash_flow_direction
                    })
            return jsonify(transactions_list)

        except Exception as e:
            print(f"Error: {str(e)}")
            return jsonify({"error": str(e)}), 500


@app.route('/get_cash_flow_transactions_base_currency', methods=["GET"])
def get_cash_flow_transactions_base_currency():
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        return jsonify({"error": "API key is missing"}), 401

    with Session() as user_session:
        company = user_session.query(Company).filter_by(api_key=api_key).first()
        if not company:
            return jsonify({"error": "Invalid API key"}), 403

        try:
            app_id = company.id
            base_currency_info = get_base_currency(user_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Get cash/bank accounts for THIS company only
            cash_account_ids = {
                a.id for a in user_session.query(ChartOfAccounts.id)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    or_(ChartOfAccounts.is_cash == True,
                        ChartOfAccounts.is_bank == True)
                ).all()
            }

            # Load transactions with related data
            transactions = (
                user_session.query(Transaction)
                .filter_by(app_id=app_id)
                .options(
                    joinedload(Transaction.currencies),
                    joinedload(Transaction.exchange_rate),
                    joinedload(Transaction.chart_of_accounts)
                    .joinedload(ChartOfAccounts.report_section),
                    joinedload(Transaction.payment_mode),
                    joinedload(Transaction.vendor),
                    joinedload(Transaction.project),
                    joinedload(Transaction.category)
                ).all()
            )

            # Map journal numbers → affects_cash
            journal_cash_status = {}
            for transaction in transactions:
                if transaction.journal_number not in journal_cash_status:
                    journal_transactions = [
                        t for t in transactions if t.journal_number == transaction.journal_number
                    ]
                    journal_account_ids = {t.subcategory_id for t in journal_transactions}
                    all_accounts_are_cash = journal_account_ids.issubset(cash_account_ids)
                    affects_cash = (
                            any(t.subcategory_id in cash_account_ids for t in journal_transactions)
                            and not all_accounts_are_cash
                    )
                    journal_cash_status[transaction.journal_number] = affects_cash

            transactions_list = []

            for transaction in transactions:
                transaction_currency_id = transaction.currency
                original_amount = transaction.amount
                amount_in_base = original_amount

                if transaction_currency_id != base_currency_id:
                    exchange_rate = transaction.exchange_rate.rate if transaction.exchange_rate else None
                    if exchange_rate:
                        amount_in_base = round(
                            Decimal(str(original_amount)) * Decimal(str(exchange_rate)),
                            2
                        )

                affects_cash = journal_cash_status.get(transaction.journal_number, False)
                is_cash_entry = transaction.subcategory_id in cash_account_ids if affects_cash else False

                cash_flow_direction = None
                if affects_cash:
                    if is_cash_entry:
                        cash_flow_direction = (
                            "inflow" if transaction.dr_cr == "Credit" else "outflow"
                        )
                    else:
                        cash_flow_direction = (
                            "inflow" if transaction.dr_cr == "Debit" else "outflow"
                        )

                    transactions_list.append({
                        "id": transaction.id,
                        "transaction_type": transaction.transaction_type,
                        "date": transaction.date.strftime('%Y-%m-%d'),
                        "category": transaction.category.category if transaction.category else None,
                        "subcategory": transaction.chart_of_accounts.sub_category if transaction.chart_of_accounts else None,
                        "currency": transaction.currencies.user_currency if transaction.currencies else None,
                        "amount": original_amount,
                        "amount_in_base_currency": amount_in_base,
                        "base_currency": base_currency,
                        "dr_cr": transaction.dr_cr,
                        "description": transaction.description,
                        "payment_mode": transaction.payment_mode.payment_mode if transaction.payment_mode else None,
                        "payment_to_vendor": transaction.vendor.vendor_name if transaction.vendor else None,
                        "project_name": transaction.project.name if transaction.project else None,
                        "date_added": transaction.date_added.strftime('%Y-%m-%d'),
                        "normal_balance": transaction.chart_of_accounts.normal_balance if transaction.chart_of_accounts else None,
                        "report_section": transaction.chart_of_accounts.report_section.name if transaction.chart_of_accounts and transaction.chart_of_accounts.report_section else None,
                        # New
                        "journal_number": transaction.journal_number,
                        "is_cash_flow": affects_cash,
                        "is_cash_entry": is_cash_entry,
                        "cash_flow_direction": cash_flow_direction
                    })
            return jsonify(transactions_list)

        except Exception as e:
            print(f'Error: {str(e)}')
            return jsonify({"error": str(e)}), 500


@app.route('/display_projects', methods=["GET"])
@login_required
def display_projects():
    user_session = Session()

    try:
        # Fetch app_id from current_user
        app_id = current_user.app_id

        # Query projects associated with the current company
        project_data = user_session.query(Project).filter_by(app_id=app_id).all()

        # Prepare projects list
        projects = []

        for project in project_data:
            projects.append({
                "Project Name": project.name,
                "Project ID": project.project_id,
                "Location": project.location,
                "Description": project.description
            })

        return jsonify(projects)

    finally:
        user_session.close()


@app.route('/all_transactions', methods=["GET"])
@login_required
def all_transactions():
    with Session() as db_session:
        app_id = current_user.app_id

        # Fetch company details
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        try:
            # Process filter options
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            filter_type = request.args.get('filter_type', 'transaction_date')  # Default to transaction_date

            query = db_session.query(Transaction).filter_by(app_id=app_id).order_by(
                Transaction.id.desc())
            filter_applied = bool(start_date or end_date)
            # Apply date filters using the apply_date_filters function
            query = apply_date_filters(query, start_date, end_date, filter_type, Transaction, 'date', 'date_added')

            transaction_entries_data = query.all()

            return render_template("general_ledger/display_transactions.html", transactions=transaction_entries_data,
                                   company=company,
                                   role=role, modules=modules_data, filter_applied=filter_applied,
                                   module_name="General Ledger")

        except Exception as e:
            db_session.rollback()
            # Handle exceptions, log if necessary
            flash(f'Error fetching transactions: {str(e)}', 'danger')
            return redirect(url_for('all_transactions'))  # Adjust to your error handling route


@app.route('/transaction/<int:transaction_id>')
@login_required
def view_transaction(transaction_id):
    with Session() as user_session:
        app_id = current_user.app_id

        # Fetch company details
        company = user_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role

        # Fetch transaction
        transaction = user_session.query(Transaction).filter_by(id=transaction_id, app_id=app_id).first()

        if transaction is None:
            abort(404)  # Handle case where transaction doesn't exist or doesn't belong to the user's company

        # Fetch categories and payment modes
        expense_categories_query = user_session.query(ChartOfAccounts.category, ChartOfAccounts.category_id).filter_by(
            app_id=app_id).distinct().all()

        expense_categories = [
            {
                f"{'INC' if str(cat_id).startswith('1') else 'LIA' if str(cat_id).startswith('2') else 'EQT' if str(cat_id).startswith('3') else 'AST' if str(cat_id).startswith('4') else 'EXP'} {cat_id}": category}
            for category, cat_id in expense_categories_query
        ]

        payment_modes = user_session.query(PaymentMode).filter_by(app_id=app_id).distinct().all()
        vendors = user_session.query(Vendor).filter_by(app_id=app_id).all()
        projects = user_session.query(Project).filter_by(app_id=app_id).all()
        transaction_categories = user_session.query(Category).filter_by(app_id=app_id).all()
        modules_data = [mod.module_name for mod in
                        user_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        currencies = user_session.query(Currency).filter_by(app_id=app_id).order_by(
            Currency.currency_index).distinct().all()

        return render_template(
            'transaction.html',
            transaction=transaction,
            transaction_categories=transaction_categories,
            payment_modes=payment_modes,
            vendors=vendors,
            project_names=projects,
            currencies=currencies,
            expense_categories=expense_categories,
            transaction_id=transaction.id,
            company=company,
            role=role,
            modules=modules_data
        )


@app.route('/delete_transaction/<int:transaction_id>', methods=['POST'])
@login_required
def delete_transaction(transaction_id):
    with Session() as db_session:
        try:
            # Fetch the transaction to be deleted
            transaction = db_session.query(Transaction).filter_by(id=transaction_id, app_id=current_user.app_id).first()

            if transaction is None:
                return jsonify({'error': 'Transaction not found or not authorized'}), 404

            # Fetch all related transactions with the same inventory_id (if applicable)
            related_inventory_transactions = []
            if transaction.source_type == "inventory_entry":
                related_inventory_transactions = db_session.query(Transaction).filter_by(
                    source_id=transaction.source_id,
                    app_id=current_user.app_id
                ).all()

            # Fetch all related transactions with the same advance_repayment_id (if applicable)
            related_advance_repayment_transactions = []
            if transaction.source_type == "advance_repayment":
                related_advance_repayment_transactions = db_session.query(Transaction).filter_by(
                    source_id=transaction.source_id,
                    app_id=current_user.app_id
                ).all()

            # Fetch all related transactions with the same advance_payment_id (if applicable)
            related_advance_payment_transactions = []
            if transaction.source_type == "advance_payment":
                related_advance_payment_transactions = db_session.query(Transaction).filter_by(
                    source_id=transaction.source_id,
                    app_id=current_user.app_id
                ).all()

            # Fetch all related payroll transactions with the same payroll_transactions (if applicable)
            related_payroll_transactions = []
            if transaction.source_type == "payroll_transaction":
                related_payroll_transactions = db_session.query(Transaction).filter_by(
                    source_id=transaction.source_id,
                    app_id=current_user.app_id
                ).all()

            # Fetch all related payroll payments with the same payroll_payment (if applicable)
            related_payroll_payment = []
            if transaction.source_type == "payroll_payment":
                related_payroll_payment = db_session.query(Transaction).filter_by(
                    source_id=transaction.source_id,
                    app_id=current_user.app_id
                ).all()

            # Notify the user if there are related transactions
            total_related_transactions = (
                    len(related_inventory_transactions) +
                    len(related_payroll_transactions) +
                    len(related_payroll_payment) +
                    len(related_advance_payment_transactions) +
                    len(related_advance_repayment_transactions) - 1
            )

            if total_related_transactions > 0:
                return jsonify({
                    'related_transactions': True,
                    'message': f'{total_related_transactions} other transactions are related to this entry. Deleting this transaction will also delete the related transactions. Proceed?'
                }), 200

            # Dictionary mapping source types to their respective models
            source_models = {
                "inventory_entry": InventoryEntry,
                "advance_payment": AdvancePayment,
                "advance_repayment": AdvanceRepayment,
                "payroll_payment": PayrollPayment,
                "payroll_transaction": PayrollTransaction
            }

            reset_message = ""

            # Update the related source entry's `is_posted_to_ledger`
            if transaction.source_type in source_models:
                related_entry = db_session.query(source_models[transaction.source_type]).filter_by(
                    id=transaction.source_id).first()
                if related_entry:
                    related_entry.is_posted_to_ledger = False
                    reset_message = f" and related {transaction.source_type.replace('_', ' ')} entry has been reset to not posted to ledger status"

            # Delete the transaction
            db_session.delete(transaction)
            db_session.commit()

            return jsonify({'success': f"Transaction deleted successfully{reset_message}!"}), 200

        except Exception as e:
            db_session.rollback()
            return jsonify({'error': f'Failed to delete transaction: {str(e)}'}), 500


@app.route('/delete_related_transactions/<int:transaction_id>', methods=['POST'])
@login_required
def delete_related_transactions(transaction_id):
    print(f"Entered delete_related_transactions function with transaction_id: {transaction_id}")

    with Session() as db_session:
        try:
            # Fetch the transaction based on the passed transaction_id
            transaction = db_session.query(Transaction).filter_by(id=transaction_id, app_id=current_user.app_id).first()

            if transaction is None:
                return jsonify({'error': 'Transaction not found or not authorized'}), 404

            # Fetch all related transactions with the same source_id
            related_transactions = db_session.query(Transaction).filter_by(
                source_id=transaction.source_id,
                app_id=current_user.app_id,
                source_type=transaction.source_type
            ).all()

            print(f"Found {len(related_transactions)} related transactions to delete.")

            # Reset the related entries based on the source_type
            source_models = {
                "inventory_entry": InventoryEntry,
                "advance_payment": AdvancePayment,
                "advance_repayment": AdvanceRepayment,
                "payroll_payment": PayrollPayment,
                "payroll_transaction": PayrollTransaction
            }

            reset_message = ""

            for rel_transaction in related_transactions:
                # Fetch and reset the related source entry's `is_posted_to_ledger`
                if rel_transaction.source_type in source_models:
                    related_entry = db_session.query(source_models[rel_transaction.source_type]).filter_by(
                        id=rel_transaction.source_id).first()
                    if related_entry:
                        related_entry.is_posted_to_ledger = False
                        reset_message = f" and related {rel_transaction.source_type.replace('_', ' ')} entry has been reset to not posted to ledger status"

                # Delete the related transaction
                print(f"Deleting related transaction with ID: {rel_transaction.id}")
                db_session.delete(rel_transaction)

            db_session.commit()
            print("All related transactions deleted successfully.")

            return jsonify({'success': f'All related transactions deleted successfully{reset_message}!'}), 200

        except Exception as e:
            db_session.rollback()
            print(f"Exception occurred: {str(e)}")
            return jsonify({'error': f'Failed to delete related transactions: {str(e)}'}), 500


@app.route('/transactions/<int:transaction_id>', methods=['POST'])
@login_required
def update_transaction(transaction_id):
    db_session = Session()
    try:
        transaction = db_session.query(Transaction).get(transaction_id)

        if transaction is None or transaction.app_id != current_user.app_id:
            abort(404)

        # Get base currency
        base_currency_info = get_base_currency(db_session, current_user.app_id)
        if not base_currency_info:
            flash("Base currency not defined for this company", "error")
            return redirect(request.referrer)

        base_currency_id = base_currency_info["base_currency_id"]

        # Parse fields
        form_currency = int(request.form['currency'])
        transaction_date = datetime.datetime.strptime(request.form['date'], '%Y-%m-%d').date()

        # Handle exchange rate if currency is not base
        exchange_rate_id = None
        if form_currency != int(base_currency_id):
            rate_id, rate_date, notification = get_or_create_exchange_rate_id(
                session=db_session,
                from_currency_id=form_currency,
                to_currency_id=base_currency_id,
                app_id=current_user.app_id,
                transaction_date=transaction_date
            )
            exchange_rate_id = rate_id
            if notification:
                flash(notification, "info")

        # Update transaction fields
        transaction.date = transaction_date
        transaction.project_id = request.form.get('project_name') or None
        transaction.vendor_id = request.form.get('payment_to_vendor') or None
        transaction.payment_mode_id = request.form.get('payment_mode') or None
        transaction.transaction_type = request.form['account_type']
        transaction.category_id = request.form.get('category')
        transaction.subcategory_id = request.form.get('subcategory')
        transaction.currency = form_currency
        transaction.amount = request.form['amount']
        transaction.dr_cr = request.form['debit_credit']
        transaction.description = request.form['description'].strip()
        transaction.updated_by = current_user.id
        transaction.exchange_rate_id = exchange_rate_id  # Assign exchange rate if applicable

        db_session.commit()
        flash('Transaction updated successfully!', 'success')

    except Exception as e:
        logger.error(f'Error is {e}')
        db_session.rollback()
        flash(f"Error updating transaction: {e}", "error")

    finally:
        db_session.close()

    return redirect(url_for("all_transactions"))


# @app.route('/payroll/post_to_ledger/<int:transaction_id>', methods=['POST'])
# @login_required
# def post_to_ledger(transaction_id):
#     db_session = Session()
#     app_id = current_user.app_id
#
#     try:
#         data = request.get_json()
#
#         # Fetch the overall payroll transaction
#         payroll_transaction = db_session.query(PayrollTransaction).filter_by(
#             id=transaction_id
#         ).first()
#
#         if not payroll_transaction:
#             return jsonify({"success": False, "message": "Payroll transaction not found"}), 404
#
#         payroll_period = payroll_transaction.payroll_period
#         payroll_period_name = payroll_period.payroll_period_name
#         end_date = payroll_period.end_date
#         currency_id = payroll_transaction.currency_id
#         ledger_balance_due = float(data['ledgerBalanceDue'])
#
#         # Check if this transaction has ever been posted
#         payroll_transaction_already_posted = db_session.query(Transaction).filter_by(
#             source_id=transaction_id,
#             source_type="payroll_transaction"
#         ).first()
#
#         # -------------------------
#         # handle advance repayment deduction if any
#         # -------------------------
#         total_advance_deducted = 0.0
#         advance_repayments = db_session.query(AdvanceRepayment).filter_by(
#             payroll_transaction_id=transaction_id,
#             app_id=app_id
#         ).all()
#
#         if advance_repayments:
#             for repayment in advance_repayments:
#
#                 if not repayment.is_posted_to_ledger:
#                     advance_payment = repayment.advance_payments
#
#                     # find the original prepaid transaction
#                     prepaid_txn = db_session.query(Transaction).filter_by(
#                         app_id=app_id,
#                         source_type="advance_payment",
#                         source_id=advance_payment.id,
#                         dr_cr="D"
#                     ).first()
#                     if not prepaid_txn:
#                         return jsonify({
#                             "success": False,
#                             "message": f"Advance payment ledger entry not found for advance ID {advance_payment.id} to {advance_payment.employees.first_name}"
#                         }), 500
#
#                     prepaid_account_category = prepaid_txn.category_id
#                     prepaid_account_subcategory = prepaid_txn.subcategory_id
#
#                     # Credit prepaid salaries
#                     journal_number = generate_unique_journal_number(db_session, app_id)
#
#                     create_transaction(
#                         db_session=db_session,
#                         transaction_type="Asset",
#                         date=end_date,
#                         category_id=prepaid_account_category,
#                         subcategory_id=prepaid_account_subcategory,
#                         currency=currency_id,
#                         amount=repayment.payment_amount,
#                         dr_cr="C",
#                         description=f"Advance recovery for {advance_payment.employees.first_name} - {payroll_period_name}",
#                         payment_mode_id=None,
#                         project_id=None,
#                         vendor_id=None,
#                         created_by=current_user.id,
#                         source_type="payroll_advance_repayment",
#                         source_id=repayment.id,
#                         app_id=app_id,
#                         exchange_rate_id=repayment.exchange_rate_id,
#                         journal_number=journal_number
#                     )
#
#                     # Expense debit
#                     create_transaction(
#                         db_session, "Expense", end_date,
#                         int(data['debitAccountCategory']), int(data['debitSubCategory']),
#                         currency_id, repayment.payment_amount, "D",
#                         f"Salary for Payroll Period: {payroll_period_name} for for {advance_payment.employees.first_name}",
#                         None, None, None, current_user.id,
#                         "payroll_transaction", transaction_id, app_id,
#                         repayment.exchange_rate_id, journal_number
#                     )
#
#                     repayment.is_posted_to_ledger = True
#                     total_advance_deducted += float(repayment.payment_amount)
#
#         # Get unposted payments for this payroll transaction
#         unposted_payments = db_session.query(PayrollPayment).filter_by(
#             payroll_transaction_id=transaction_id,
#             is_posted_to_ledger=False
#         ).all()
#
#         credit_account = int(data['creditSubCategory']) if data['creditSubCategory'] else None
#         credit_account_category = int(data['creditAccountCategory']) if data['creditAccountCategory'] else None
#
#         if not credit_account:
#             if payroll_transaction_already_posted:
#                 credit_account = payroll_transaction_already_posted.subcategory_id
#                 credit_account_category = payroll_transaction_already_posted.chart_of_accounts.categories.id
#             else:
#                 # no fallback possible
#                 credit_account = None
#                 credit_account_category = None
#
#         # -----------------------------
#         # 1. Handle accrual of unpaid salaries (if no payments yet)
#         # -----------------------------
#         if not unposted_payments:
#             if not payroll_transaction_already_posted and ledger_balance_due > 0:
#                 # Generate a single journal number for the accrual
#                 journal_number = generate_unique_journal_number(db_session, app_id)
#
#                 description = f"Unpaid Salary for Payroll Period: {payroll_period_name}"
#
#                 # Resolve exchange rate
#                 exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
#                     session=db_session,
#                     currency_id=currency_id,
#                     transaction_date=end_date,
#                     app_id=app_id
#                 )
#
#                 # Expense debit
#                 create_transaction(
#                     db_session, "Expense", end_date,
#                     int(data['debitAccountCategory']), int(data['debitSubCategory']),
#                     currency_id, ledger_balance_due, "D", description,
#                     None, None, None, current_user.id,
#                     "payroll_transaction", transaction_id, app_id,
#                     exchange_rate_id, journal_number
#                 )
#
#                 # Liability credit
#                 create_transaction(
#                     db_session, "Liability", end_date,
#                     int(data['creditAccountCategory']), int(data['creditSubCategory']),
#                     currency_id, ledger_balance_due, "C", description,
#                     None, None, None, current_user.id,
#                     "payroll_transaction", transaction_id, app_id,
#                     exchange_rate_id, journal_number
#                 )
#
#                 payroll_transaction.is_posted_to_ledger = True
#
#         # -----------------------------
#         # 2. Handle actual cash/bank payments
#         # -----------------------------
#         for payroll_payment in unposted_payments:
#             amount = float(payroll_payment.amount)
#             payment_account_id = payroll_payment.payment_account
#             payment_method_id = payroll_payment.payment_method
#             payment_date = payroll_payment.payment_date
#             reference = payroll_payment.reference
#             payment_id = payroll_payment.id
#
#             # resolve payment category
#             chart_account = db_session.query(ChartOfAccounts).filter_by(
#                 id=payment_account_id
#             ).first()
#
#             if not chart_account:
#                 return jsonify({"success": False, "message": f"Payment account ID {payment_account_id} not found"}), 404
#
#             payment_category_id = chart_account.category_fk
#
#             # Resolve exchange rate
#             exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
#                 session=db_session,
#                 currency_id=currency_id,
#                 transaction_date=payment_date,
#                 app_id=app_id
#             )
#
#             # Generate a fresh journal number for *each* payment
#             journal_number = generate_unique_journal_number(db_session, app_id)
#             if payroll_transaction_already_posted:
#
#                 # payment settling liability
#                 # CREDIT cash/bank (Asset)
#                 create_transaction(
#                     db_session, "Asset", payment_date,
#                     payment_category_id, payment_account_id,
#                     currency_id, amount, "C",
#                     f"{reference + ' ' if reference else ''}Payroll payment for {payroll_period_name} for {payroll_transaction.employees.first_name}",
#                     payment_method_id, None, None, current_user.id,
#                     "payroll_payment", payment_id, app_id,
#                     exchange_rate_id, journal_number
#                 )
#
#                 # DEBIT liability
#                 create_transaction(
#                     db_session, "Liability", payment_date,
#                     credit_account_category, credit_account,
#                     currency_id, amount, "D",
#                     f"Reducing unpaid salaries for {payroll_period_name} for {payroll_transaction.employees.first_name}",
#                     payment_method_id, None, None, current_user.id,
#                     "payroll_payment", payment_id, app_id,
#                     exchange_rate_id, journal_number
#                 )
#
#             else:
#
#                 # payment with no prior accrual
#                 # CREDIT cash/bank (Asset)
#                 create_transaction(
#                     db_session, "Asset", payment_date,
#                     payment_category_id, payment_account_id,
#                     currency_id, amount, "C",
#                     f"{reference + ' ' if reference else ''}Payroll payment for {payroll_period_name} for {payroll_transaction.employees.first_name}",
#                     payment_method_id, None, None, current_user.id,
#                     "payroll_payment", payment_id, app_id,
#                     exchange_rate_id, journal_number
#                 )
#
#                 # DEBIT expense
#                 create_transaction(
#                     db_session, "Expense", payment_date,
#                     int(data['debitAccountCategory']), int(data['debitSubCategory']),
#                     currency_id, amount, "D",
#                     f"{reference + ' ' if reference else ''}Salary paid for {payroll_period_name} for {payroll_transaction.employees.first_name}",
#                     payment_method_id, None, None, current_user.id,
#                     "payroll_payment", payment_id, app_id,
#                     exchange_rate_id, journal_number
#                 )
#
#                 # if remaining balance due, create accrual for the balance
#                 if ledger_balance_due > 0:
#                     # separate journal number for the unpaid accrual
#                     accrual_journal = generate_unique_journal_number(db_session, app_id)
#
#                     description = f"Unpaid Salaries for Payroll Period: {payroll_period_name} for {payroll_transaction.employees.first_name}"
#
#                     # Expense debit
#                     create_transaction(
#                         db_session, "Expense", payment_date,
#                         int(data['debitAccountCategory']), int(data['debitSubCategory']),
#                         currency_id, ledger_balance_due, "D", description,
#                         None, None, None, current_user.id,
#                         "payroll_transaction", transaction_id, app_id,
#                         exchange_rate_id, accrual_journal
#                     )
#
#                     # Liability credit
#                     create_transaction(
#                         db_session, "Liability", payment_date,
#                         credit_account_category, credit_account,
#                         currency_id, ledger_balance_due, "C", description,
#                         None, None, None, current_user.id,
#                         "payroll_transaction", transaction_id, app_id,
#                         exchange_rate_id, accrual_journal
#                     )
#
#             # mark payment posted
#             payroll_payment.is_posted_to_ledger = True
#
#         # mark the transaction posted if no unposted payments left
#         remaining = db_session.query(PayrollPayment).filter_by(
#             payroll_transaction_id=transaction_id,
#             is_posted_to_ledger=False
#         ).count()
#
#         if remaining == 0:
#             payroll_transaction.is_posted_to_ledger = True
#
#         db_session.commit()
#         return jsonify({"success": True, "message": "Payroll posted to ledger successfully"}), 201
#
#     except Exception as e:
#         db_session.rollback()
#         logger.error(f"[DEBUG] Error posting to ledger: {e}")
#         return jsonify({"success": False, "message": str(e)}), 500
#


@app.route('/payroll/dashboard', methods=['GET'])
@login_required
def payroll_dashboard():
    db_session = Session()

    try:
        app_id = current_user.app_id
        role = db_session.query(User.role).filter_by(id=current_user.id).scalar()
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Get filters from query parameters
        currency_id = request.args.get('currency')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        # Default to earliest and latest dates if not provided
        if not start_date or not end_date:
            # Fetch the earliest and latest available transaction dates from the database
            earliest_date = db_session.query(func.min(PayrollTransaction.creation_date)).filter_by(
                app_id=app_id).scalar()
            latest_date = db_session.query(func.max(PayrollTransaction.creation_date)).filter_by(app_id=app_id).scalar()

            # Use the fetched dates if no dates are provided in the request
            start_date = earliest_date if not start_date else start_date
            end_date = latest_date if not end_date else end_date

        # Ensure start_date and end_date are datetime objects
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        # Fetch user currency
        user_currency = db_session.query(Currency).filter_by(app_id=app_id).order_by(Currency.currency_index).all()

        # Determine the selected currency
        if currency_id is None:
            main_currency = db_session.query(Currency).filter_by(currency_index=1, app_id=app_id).first()
            currency_id = main_currency.id
            selected_currency = main_currency.user_currency  # Use the main currency's user_currency
        else:
            selected_currency = db_session.query(Currency.user_currency).filter_by(id=currency_id).scalar()

        # Base filters for payroll transactions
        base_filters = [PayrollTransaction.app_id == app_id, PayrollTransaction.currency_id == currency_id]

        if start_date:
            base_filters.append(PayrollTransaction.creation_date >= start_date)
        if end_date:
            base_filters.append(PayrollTransaction.creation_date <= end_date)

        # Total Employees
        total_employees = db_session.query(func.count(Employee.id)).filter_by(app_id=app_id).scalar()

        # Total Payroll Cost
        total_payroll_cost = (
                db_session.query(func.sum(PayrollTransaction.gross_salary))
                .filter(*base_filters)
                .scalar() or 0
        )

        # Total Deductions
        total_deductions = (
                db_session.query(func.sum(Deduction.amount))
                .filter(Deduction.app_id == app_id)
                .filter(Deduction.currency_id == currency_id)
                .scalar() or 0
        )

        # Total Benefits
        total_benefits = (
                db_session.query(func.sum(Benefit.amount))
                .filter(Benefit.app_id == app_id)
                .filter(Benefit.currency_id == currency_id)
                .scalar() or 0
        )

        # Payroll Cost by Department
        department_payroll = (
            db_session.query(
                Department.department_name,
                func.sum(PayrollTransaction.gross_salary).label('total_payroll')
            )
            .join(Employee, Employee.department_id == Department.id)
            .join(PayrollTransaction, PayrollTransaction.employee_id == Employee.id)
            .filter(*base_filters)
            .group_by(Department.department_name)
            .all()
        )
        department_names = [row.department_name for row in department_payroll]
        department_payroll_costs = [row.total_payroll for row in department_payroll]

        # Payment Status Distribution
        payment_status_counts = (
            db_session.query(
                PayrollTransaction.payment_status,
                func.count(PayrollTransaction.id))
            .filter(*base_filters)
            .group_by(PayrollTransaction.payment_status)
            .all()
        )
        payment_status_labels = [status.value for status, _ in payment_status_counts]
        payment_status_counts = [count for _, count in payment_status_counts]

        # Recent Payroll Transactions
        recent_transactions = (
            db_session.query(PayrollTransaction)
            .join(Employee, PayrollTransaction.employee_id == Employee.id)
            .join(PayrollPeriod, PayrollTransaction.payroll_period_id == PayrollPeriod.id)
            .filter(*base_filters)
            .order_by(PayrollTransaction.creation_date.desc())
            .limit(10)
            .all()
        )

        # Recent Deductions
        recent_deductions = (
            db_session.query(Deduction)
            .join(Employee, Deduction.employee_id == Employee.id)
            .join(DeductionType, Deduction.deduction_type_id == DeductionType.id)
            .filter(Deduction.app_id == app_id)
            .filter(Deduction.currency_id == currency_id)
            .order_by(Deduction.creation_date.desc())
            .limit(10)
            .all()
        )

        # Recent Benefits
        recent_benefits = (
            db_session.query(Benefit)
            .join(Employee, Benefit.employee_id == Employee.id)
            .join(BenefitType, Benefit.benefit_type_id == BenefitType.id)
            .filter(Benefit.app_id == app_id)
            .filter(Benefit.currency_id == currency_id)
            .order_by(Benefit.creation_date.desc())
            .limit(10)
            .all()
        )

        return render_template(
            "payroll/payroll_dashboard.html",
            total_employees=total_employees,
            total_payroll_cost=total_payroll_cost,
            total_deductions=total_deductions,
            total_benefits=total_benefits,
            department_names=department_names,
            department_payroll_costs=department_payroll_costs,
            payment_status_labels=payment_status_labels,
            payment_status_counts=payment_status_counts,
            recent_transactions=recent_transactions,
            recent_deductions=recent_deductions,
            recent_benefits=recent_benefits,
            company=company,
            role=role,
            modules=modules_data,
            user_currency=user_currency,  # Pass user currencies for the filter
            selected_currency=selected_currency,  # Pass the selected currency's user_currency to the template
            module_name="Payroll"
        )

    except Exception as e:
        print(f"[ERROR] An error occurred while fetching payroll dashboard data: {e}")
        return jsonify({"success": False, "message": "An error occurred while processing your request."}), 500

    finally:
        db_session.close()


# ------------------- End Payroll ----------------------------------


@app.route("/expense_report", methods=["GET", "POST"])
@login_required
def expense_report():
    db_session = Session()
    if request.method == "POST":
        start_date = request.form["start_date"]
        end_date = request.form["end_date"]
        subcategory_totals = db_session.query(Transaction.category, Transaction.subcategory,
                                              func.sum(Transaction.amount).label("total")). \
            filter(Transaction.transaction_type == 'Expense'). \
            filter(Transaction.date >= start_date, Transaction.date <= end_date). \
            group_by(Transaction.category, Transaction.subcategory). \
            all()
    else:
        subcategory_totals = db_session.query(Transaction.category, Transaction.subcategory,
                                              func.sum(Transaction.amount).label("total")). \
            filter(Transaction.transaction_type == 'Expense'). \
            group_by(Transaction.category, Transaction.subcategory). \
            all()
    category_totals = {}

    for category, subcategory, total in subcategory_totals:
        if category not in category_totals:
            category_totals[category] = {'subtotals': {}, 'total': 0}
        category_totals[category]['subtotals'][subcategory] = total
        category_totals[category]['total'] += total

    total_expense = sum([category_total['total'] for category_total in category_totals.values()])

    return render_template('expense_report.html', category_totals=category_totals, total_expense=total_expense)


@app.route('/loan_application', methods=['GET', 'POST'])
def loan_application():
    db_session = Session()
    try:
        if request.method == 'POST':
            # Get form data
            date_of_request = datetime.datetime.now().date()  # Use current date
            loan_applicant_name = request.form['loan_applicant_name'].strip()
            nin_number = request.form['nin_number']
            telephone = request.form['telephone']
            email = request.form['email'].strip()
            currency = "UGX"
            principal_amount = int(request.form['principal_amount'])
            date_of_payment = datetime.datetime.strptime(request.form['date_of_payment'], '%Y-%m-%d')
            interest_amount = float(request.form['interest_amount'].split(' ')[0].replace(',', ''))
            guarantor_name = request.form['guarantor_name'].strip()
            guarantor_contact = request.form['guarantor_contact'].strip()
            deposit_number = request.form['deposit_number']
            registered_names = request.form['registered_names'].strip()
            repayment_period = request.form['repayment_period']
            repayment_amount = principal_amount + interest_amount

            # Check if the applicant already exists by NIN number
            existing_applicant = db_session.query(LoanApplicant).filter(LoanApplicant.nin_number == nin_number).first()

            user_name = loan_applicant_name.strip().split()[0]

            # Call send mail function before saving the information to the database
            notify_applicant(loan_amount=principal_amount, user_name=user_name, user_email=email,
                             loan_term_days=repayment_period, repayment_sum=repayment_amount)

            if existing_applicant:
                # Use the existing applicant's details
                loan_applicant = existing_applicant
            else:
                # Create a new loan applicant if NIN does not exist
                loan_applicant = LoanApplicant(
                    name=loan_applicant_name,
                    nin_number=nin_number,
                    telephone=telephone,
                    email=email,
                    country_code='+256',  # Default country code, or you can get from form if needed
                    app_id=4  # Assuming a default company, this can be adjusted as per your logic
                )
                db_session.add(loan_applicant)
                db_session.commit()

            # Create a new loan application instance
            new_loan = LoanApplication(
                date_of_request=date_of_request,
                loan_applicant_id=loan_applicant.id,  # Reference the existing or newly created applicant
                currency=currency,
                principal_amount=principal_amount,
                telephone=telephone,
                email=email,
                date_of_payment=date_of_payment,
                interest_amount=interest_amount,
                guarantor_name=guarantor_name,
                guarantor_contact=guarantor_contact,
                deposit_number=deposit_number,
                registered_names=registered_names,
                loan_period=repayment_period,  # Default penalty amount
                app_id=4
            )

            # Add to the database
            db_session.add(new_loan)
            db_session.commit()

            # Add initial loan history entry
            new_loan_history = LoanHistory(
                loan_id=new_loan.id,
                deadline_date=new_loan.date_of_payment,
                penalty_date=None,
                repayment_amount=repayment_amount,
                penalty_amount=0,
                notes="Initial loan entry",
                app_id=4
            )

            db_session.add(new_loan_history)
            db_session.commit()

            notify_admin(loan_amount=principal_amount, user_name=user_name, user_email=email,
                         loan_term_days=repayment_period, repayment_sum=repayment_amount)

            flash(f'Your application has been submitted successfully', category='success')

            return redirect(url_for('loan_application'))

        return render_template('loan_application.html')

    except Exception as e:
        db_session.rollback()  # Rollback the transaction in case of an error
        flash(f'An error occurred while fetching loan data: {str(e)}', 'danger')
        return redirect(url_for('loan_application'))  # Redirect to an error page or dashboard

    finally:
        db_session.close()  # Ensure the session is closed after the request is processed


@app.route('/loans-dashboard', methods=['GET', 'POST'])
@login_required
def loans_dashboard():
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Fetch loans related to the current app_id
        loans = db_session.query(LoanApplication).filter(
            LoanApplication.app_id == app_id,
            LoanApplication.loan_status != "NOT_APPROVED"
        ).all()
        print(f"Loans fetched for app_id {app_id}: {loans}")  # Print fetched loans

        # Initialize variables
        loan_details = []
        total_unpaid_principal = 0
        total_discounts_offered = 0
        total_paid = 0
        total_principal_given_out = 0
        total_unpaid_cancelled = 0
        total_unpaid_active = 0
        total_interest_paid = 0  # Initialize total interest paid
        total_interest_unpaid = 0  # Initialize total interest unpaid
        total_interest_lost = 0
        remaining_amount = 0

        for loan in loans:
            # Fetch the loan history entries for the loan
            loan_history = db_session.query(LoanHistory).filter_by(loan_id=loan.id).all()
            print(f"Loan history for loan {loan.id}: {loan_history}")  # Print loan history entries

            # Fetch the latest loan history entry for the loan
            latest_history = loan_history[-1] if loan_history else None
            print(f"Latest history for loan {loan.id}: {latest_history}")  # Print latest history for each loan

            if latest_history:
                total_fine_amount = latest_history.total_fine_amount
                total_amount_paid = sum(history.amount_paid for history in loan_history)
                total_discount_amount = sum(history.discount_amount for history in loan_history)
                due_date = latest_history.new_due_date if latest_history.new_due_date else latest_history.deadline_date
                total_principal_and_initial_interest = loan.principal_amount + loan.interest_amount
                remaining_amount = total_principal_and_initial_interest + total_fine_amount - total_amount_paid - total_discount_amount

                # Add to the total unpaid principal
                total_unpaid_principal += remaining_amount

                # Add to the total discounts offered
                total_discounts_offered += total_discount_amount

                # Add to the total amount paid
                total_paid += total_amount_paid

                # Add to the total principal given out
                total_principal_given_out += loan.principal_amount

                # Calculate interest paid for this loan
                interest_paid = max(0, total_amount_paid - loan.principal_amount)
                total_interest_paid += interest_paid

                # Calculate interest unpaid for active loans
                interest_unpaid = max(0, (loan.interest_amount + total_fine_amount) - interest_paid)
                if loan.loan_status != LoanStatus.DISCONTINUED:
                    total_interest_unpaid += interest_unpaid  # Unpaid interest for active loans

                # Track lost interest for cancelled loans
                if loan.loan_status == LoanStatus.DISCONTINUED:
                    total_interest_lost += interest_unpaid  # Add to a separate variable

                # Calculate unpaid amounts based on loan status
                if loan.loan_status == LoanStatus.DISCONTINUED:
                    total_unpaid_cancelled += remaining_amount  # Add to the total unpaid for cancelled loans
                else:
                    total_unpaid_active += remaining_amount  # Add to the total unpaid for active loans

                print(
                    f"Due date for loan {loan.id}: {due_date}, Remaining amount: {remaining_amount}, Interest Paid: {interest_paid}, Interest Unpaid: {interest_unpaid}")
            else:
                due_date = None
                total_discount_amount = sum(history.discount_amount for history in loan_history)
                total_amount_paid = sum(history.amount_paid for history in loan_history)
                total_fine_amount = latest_history.total_fine_amount if latest_history else 0
                total_principal_and_initial_interest = loan.principal_amount + loan.interest_amount
                remaining_amount = total_principal_and_initial_interest + total_fine_amount - total_amount_paid - total_discount_amount

                # Add to the total unpaid principal
                total_unpaid_principal += remaining_amount

                # Add to the total discounts offered
                total_discounts_offered += total_discount_amount

                # Add to the total amount paid
                total_paid += total_amount_paid

                # Add to the total principal given out
                total_principal_given_out += loan.principal_amount

                # Calculate interest paid for this loan
                interest_paid = max(0, total_amount_paid - loan.principal_amount)
                total_interest_paid += interest_paid

                # Calculate interest unpaid for active loans
                interest_unpaid = max(0, (loan.interest_amount + total_fine_amount) - interest_paid)
                if loan.loan_status != LoanStatus.DISCONTINUED:
                    total_interest_unpaid += interest_unpaid  # Unpaid interest for active loans

                # Track lost interest for cancelled loans
                if loan.loan_status == LoanStatus.DISCONTINUED:
                    total_interest_lost += interest_unpaid  # Add to a separate variable

                # Calculate unpaid amounts based on loan status
                if loan.loan_status == LoanStatus.DISCONTINUED:
                    total_unpaid_cancelled += remaining_amount  # Add to the total unpaid for cancelled loans
                else:
                    total_unpaid_active += remaining_amount  # Add to the total unpaid for active loans

                print(f"No history for loan {loan.id}, showing full amount: {remaining_amount}")

            loan_details.append({
                'loan': loan,
                'due_date': due_date,
                'remaining_amount': remaining_amount,
                'total_fine_amount': total_fine_amount,
                'total_amount_paid': total_amount_paid,
                'total_discount_amount': total_discount_amount,
                'interest_paid': interest_paid

            })
        # Sort loan_details by date_of_request in descending order
        loan_details_sorted = sorted(loan_details, key=lambda x: x['loan'].date_of_request, reverse=True)

        # Limit loan_details to the latest 10 transactions
        loan_details_limited = loan_details_sorted[:10]

        # Fetch fines and penalties from LoanHistory (or your relevant table)
        loan_history_all = db_session.query(LoanHistory).filter(LoanHistory.app_id == app_id).all()
        print(f"Loan history for app_id {app_id}: {loan_history_all}")  # Print loan history data

        # Fetch role and company details of the current user
        role = current_user.role
        company = db_session.query(Company).filter_by(id=app_id).first()
        print(f"User role: {role}, Company: {company}")  # Print user role and company

        # Fetch modules data (to display additional data if needed)
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter(Module.app_id == app_id, Module.included == 'yes').all()]
        print(f"Modules data: {modules_data}")  # Print modules data

        # Calculate total loans, interest, unpaid principal
        total_loans = len(loans)
        total_interest = sum(loan.interest_amount for loan in loans)
        print(f"Total loans: {total_loans}, Total interest: {total_interest}")  # Print total loans and interest

        # Sum fines and penalties (assuming fine/penalty columns exist in LoanHistory)
        total_fines_penalties = sum(
            history.new_interest_amount + history.penalty_amount for history in loan_history_all)
        print(f"Total fines and penalties: {total_fines_penalties}")  # Print total fines and penalties
        total_interest_amount = total_interest
        # Add fines/penalties to total interest
        total_interest += total_fines_penalties
        print(f"Total interest after adding fines/penalties: {total_interest}")  # Print total interest after adjustment

        # Calculate total amounts for loans summary
        total_principal_amount = sum(loan.principal_amount for loan in loans)
        print(f"Total principle amount: {total_principal_amount}")  # Print total principle amount

        # Calculate total profit
        total_profit = total_paid - total_principal_given_out

        # Fetch most recent loan applications
        most_recent_loans = db_session.query(LoanApplication).filter_by(app_id=app_id).order_by(
            LoanApplication.date_of_request.desc()).limit(10).all()

        # Fetch not approved loan applications
        not_approved_loans = db_session.query(LoanApplication).filter_by(app_id=app_id,
                                                                         loan_status=LoanStatus.NOT_APPROVED).all()

        return render_template(
            'loans_dashboard.html',
            company=company,
            role=role,
            loans=loan_details_limited,  # Pass the modified loan data with due_date and remaining_amount
            total_loans=total_loans,
            total_interest=total_interest,
            total_interest_amount=total_interest_amount,
            total_unpaid_amount=total_unpaid_principal,  # Use total unpaid principal here
            total_principal_amount=total_principal_amount,
            total_remaining_amount=remaining_amount,
            modules=modules_data,
            module_name="Loans",
            total_profit=total_profit,
            total_unpaid_cancelled=total_unpaid_cancelled,
            total_unpaid_active=total_unpaid_active,  # Pass total unpaid for active loans
            total_discounts_offered=total_discounts_offered,
            recent_loans=most_recent_loans,
            not_approved_loans=not_approved_loans,
            total_paid=total_paid,
            total_interest_paid=total_interest_paid,  # Pass total interest paid
            total_interest_unpaid=total_interest_lost  # Pass total interest unpaid
        )

    except Exception as e:
        db_session.rollback()
        flash(f'An error occurred while fetching loan data: {str(e)}', 'danger')
        print(f"Error occurred: {e}")  # Print error message
        return redirect(url_for('loans_dashboard'))  # Redirect to an error page or dashboard

    finally:
        db_session.close()


@app.route('/loan-applications', methods=['GET', 'POST'])
@login_required
def loan_applications():
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Fetch loans related to the current app_id
        loans = db_session.query(LoanApplication).filter_by(app_id=app_id).all()

        # Initialize variables
        loan_details = []
        total_unpaid_principal = 0
        total_discounts_offered = 0
        total_paid = 0
        total_principal_given_out = 0
        total_unpaid_cancelled = 0
        total_unpaid_active = 0
        total_interest_paid = 0  # Initialize total interest paid
        total_interest_unpaid = 0  # Initialize total interest unpaid

        for loan in loans:
            # Fetch the loan history entries for the loan
            loan_history = db_session.query(LoanHistory).filter_by(loan_id=loan.id).all()
            print(f"Loan history for loan {loan.id}: {loan_history}")  # Print loan history entries

            # Fetch the latest loan history entry for the loan
            latest_history = loan_history[-1] if loan_history else None
            print(f"Latest history for loan {loan.id}: {latest_history}")  # Print latest history for each loan

            if latest_history:
                total_fine_amount = latest_history.total_fine_amount
                total_amount_paid = sum(history.amount_paid for history in loan_history)
                total_discount_amount = sum(history.discount_amount for history in loan_history)
                due_date = latest_history.new_due_date if latest_history.new_due_date else latest_history.deadline_date
                total_principal_and_initial_interest = loan.principal_amount + loan.interest_amount
                remaining_amount = total_principal_and_initial_interest + total_fine_amount - total_amount_paid - total_discount_amount

                # Add to the total unpaid principal
                total_unpaid_principal += remaining_amount

                # Add to the total discounts offered
                total_discounts_offered += total_discount_amount

                # Add to the total amount paid
                total_paid += total_amount_paid

                # Add to the total principal given out
                total_principal_given_out += loan.principal_amount

                # Calculate interest paid for this loan
                interest_paid = max(0, total_amount_paid - loan.principal_amount)
                total_interest_paid += interest_paid

                # Calculate interest unpaid for this loan
                interest_unpaid = max(0, (loan.interest_amount + total_fine_amount) - interest_paid)
                if loan.loan_status != LoanStatus.CANCELLED:
                    total_interest_unpaid += interest_unpaid

                # Calculate unpaid amounts based on loan status
                if loan.loan_status == LoanStatus.CANCELLED:
                    total_unpaid_cancelled += remaining_amount  # Add to the total unpaid for cancelled loans
                else:
                    total_unpaid_active += remaining_amount  # Add to the total unpaid for active loans

                print(
                    f"Due date for loan {loan.id}: {due_date}, Remaining amount: {remaining_amount}, Interest Paid: {interest_paid}, Interest Unpaid: {interest_unpaid}")
            else:
                due_date = None
                total_discount_amount = sum(history.discount_amount for history in loan_history)
                total_amount_paid = sum(history.amount_paid for history in loan_history)
                total_fine_amount = latest_history.total_fine_amount if latest_history else 0
                total_principal_and_initial_interest = loan.principal_amount + loan.interest_amount
                remaining_amount = total_principal_and_initial_interest + total_fine_amount - total_amount_paid - total_discount_amount

                # Add to the total unpaid principal
                total_unpaid_principal += remaining_amount

                # Add to the total discounts offered
                total_discounts_offered += total_discount_amount

                # Add to the total amount paid
                total_paid += total_amount_paid

                # Add to the total principal given out
                total_principal_given_out += loan.principal_amount

                # Calculate interest paid for this loan
                interest_paid = max(0, total_amount_paid - loan.principal_amount)
                total_interest_paid += interest_paid

                # Calculate interest unpaid for this loan
                interest_unpaid = max(0, (loan.interest_amount + total_fine_amount) - interest_paid)
                if loan.loan_status != LoanStatus.CANCELLED:
                    total_interest_unpaid += interest_unpaid

                # Calculate unpaid amounts based on loan status
                if loan.loan_status == LoanStatus.CANCELLED:
                    total_unpaid_cancelled += remaining_amount  # Add to the total unpaid for cancelled loans
                else:
                    total_unpaid_active += remaining_amount  # Add to the total unpaid for active loans

                print(f"No history for loan {loan.id}, showing full amount: {remaining_amount}")

            loan_details.append({
                'loan': loan,
                'due_date': due_date,
                'remaining_amount': remaining_amount,
                'total_fine_amount': total_fine_amount,
                'total_amount_paid': total_amount_paid,
                'total_discount_amount': total_discount_amount,
                'interest_paid': interest_paid,
                'interest_unpaid': interest_unpaid,
                'repayment_period': loan.loan_period
            })

        # Fetch role and company details of the current user
        role = current_user.role
        company = db_session.query(Company).filter_by(id=app_id).first()
        print(f"User role: {role}, Company: {company}")  # Print user role and company

        # Fetch modules data (to display additional data if needed)
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter(Module.app_id == app_id, Module.included == 'yes').all()]

        return render_template(
            'loan_applications.html',
            company=company,
            role=role,
            loans=loan_details,  # Pass the modified loan data with due_date and remaining_amount
            modules=modules_data,
            module_name="Loans"
        )

    except Exception as e:
        db_session.rollback()
        flash(f'An error occurred while fetching loan data: {str(e)}', 'danger')
        print(f"Error occurred: {e}")  # Print error message
        return redirect(url_for('loans_dashboard'))  # Redirect to an error page or dashboard

    finally:
        db_session.close()


@app.route('/loan_update/<int:loan_id>', methods=['GET', 'POST'])
@login_required
def loan_update(loan_id):
    db_session = Session()
    app_id = current_user.app_id
    loan = db_session.query(LoanApplication).filter_by(id=loan_id).first()

    try:
        if request.method == 'POST':
            payment_date = datetime.datetime.strptime(request.form['payment_date'], '%Y-%m-%d').date()
            payment_amount = float(request.form['payment_amount'])
            payment_type = request.form['payment_type']  # Get payment type (loan repayment or discount)

            # Initialize discount amount based on payment type
            discount_amount = 0
            if payment_type == 'discount':
                discount_amount = max(0, payment_amount)  # Prevent negative discount
                payment_amount = 0

            if payment_type == "discontinue":
                loan.loan_status = LoanStatus.DISCONTINUED

            # Query LoanHistory to get the total paid amount, fines, and discounts
            loan_history_entries = db_session.query(LoanHistory).filter_by(loan_id=loan.id).all()

            # Calculate the total paid amount, total fines (penalty + interest), and total discounts
            total_paid = sum(entry.amount_paid for entry in loan_history_entries)
            total_penalty = sum(entry.penalty_amount for entry in loan_history_entries)
            total_interest = sum(entry.new_interest_amount for entry in loan_history_entries)
            total_fine = total_penalty + total_interest  # Total fine = Penalty + Interest
            total_discount_amount = sum(entry.discount_amount for entry in loan_history_entries)

            total_principal_and_interest = loan.principal_amount + loan.interest_amount

            # Calculate the total remaining amount (Principal + Interest + Fines - Total Paid - Total Discount)
            total_remaining_amount = max(0,
                                         total_principal_and_interest + total_fine - total_paid - total_discount_amount)

            # Apply the discount amount to reduce the total remaining amount
            total_remaining_amount = max(0, total_remaining_amount - discount_amount)

            # Determine the latest deadline date from LoanHistory
            latest_deadline_date = max(
                [entry.deadline_date for entry in loan_history_entries if entry.deadline_date],
                default=loan.date_of_payment
            )

            print(
                f'total remaining amount is {total_remaining_amount} and total fine was {total_fine} and total paid was {total_paid}')

            # Add entry to LoanHistory (tracking payment, fines, and discount)
            new_loan_history = LoanHistory(
                loan_id=loan.id,
                deadline_date=loan.date_of_payment,  # Use the latest deadline date
                penalty_date=None,
                creation_date=payment_date,
                repayment_amount=total_remaining_amount,  # Updated repayment amount with fines and discount applied
                amount_paid=payment_amount,
                discount_amount=discount_amount,  # Store the discount amount if applicable
                total_fine_amount=total_fine,  # Sum of all fines applied so far (only for tracking)
                notes=f"{payment_type.capitalize()} of {payment_amount} received.",
                app_id=app_id
            )

            db_session.add(new_loan_history)
            db_session.commit()

            # Handling Zero Balance Here incase it was a payment that means we have to add ne payment to voerall payment

            if payment_type == "payment":
                total_paid += payment_amount

                # Calculate the total remaining amount (Principal + Interest + Fines - Total Paid - Total Discount)
                total_remaining_amount = max(0,
                                             total_principal_and_interest + total_fine - total_paid - total_discount_amount)

                # Apply the discount amount to reduce the total remaining amount
                total_remaining_amount = max(0, total_remaining_amount - discount_amount)

            # Check if the loan is fully paid
            if total_remaining_amount <= 0:
                loan.loan_status = LoanStatus.REPAID
                final_history = LoanHistory(
                    loan_id=loan.id,
                    deadline_date=loan.date_of_payment,  # Use the latest deadline date
                    penalty_date=None,
                    repayment_amount=0,
                    amount_paid=0,
                    penalty_amount=None,
                    total_fine_amount=total_fine,
                    discount_amount=discount_amount,  # Store any discount applied during final repayment
                    notes="Loan fully repaid with discount." if payment_type == "discount" else (
                        "Loan has been discontinued" if payment_type == "discontinue" else "Loan fully repaid."),
                    app_id=app_id
                )
                db_session.add(final_history)
                db_session.commit()

            # Send email notification to the applicant
            applicant_email = loan.loan_applicant.email
            applicant_name = loan.loan_applicant.name
            loan_amount = loan.principal_amount
            loan_term_days = (loan.date_of_payment - loan.date_of_request).days if loan.date_of_payment else 0
            repayment_sum = total_principal_and_interest + total_fine

            # Prepare email subject and body
            subject = "Loan Update Notification"
            body_message = (
                f"Dear {applicant_name},\n\n"
                f"Your loan (ID: {loan.id}) has been updated.\n"
            )

            # Add payment amount if payment type is 'payment'
            if payment_type == 'payment':
                # Check if the loan is fully cleared
                if total_remaining_amount <= 0:
                    body_message += (
                        "Congratulations! Your loan has been fully cleared.\n\n"
                    )
                else:
                    body_message += (
                        f"Payment Amount: UGX {payment_amount:,.0f}\n" "Please ensure the remaining amount is paid by the due date.\n\n"
                    )

            # Add discount applied if payment type is 'discount'
            if payment_type == 'discount':
                body_message += f"Discount Applied: UGX {discount_amount:,.0f}\n"

            # Handle discontinued loans
            if payment_type == 'discontinue':
                body_message += (
                    f"Discontinued with remaining amount of UGX {total_remaining_amount:,.0f}\n\n"
                )
            else:
                # Add the total remaining amount for non-discontinued loans
                body_message += (
                    f"Total Remaining Amount: UGX {total_remaining_amount:,.0f}\n\n"
                )

            # Add the closing message
            body_message += (
                "Thank you for using our services.\n\n"
                "Best regards,\n"
                "Creditrust Capital Solutions"
            )

            # Send the email
            if not send_mail(applicant_email, subject, body_message):
                flash("Failed to send email notification to the applicant.", "warning")

            return redirect(url_for('loans_dashboard'))

    finally:
        db_session.close()  # Ensure the session is closed after the operation is complete

    return render_template('loan_update.html', loan=loan)


@app.route('/approve-loan/<int:loan_id>', methods=['POST'])
@login_required
def approve_loan(loan_id):
    db_session = Session()
    payment_name = "Owembabazi Charlotte"
    payment_number = "0788740812"
    try:
        loan = db_session.query(LoanApplication).filter_by(id=loan_id).first()
        print(f'loan status is {LoanStatus.PENDING}')

        if loan and loan.loan_status == LoanStatus.PENDING:
            loan.loan_status = LoanStatus.APPROVED
            db_session.commit()

            notify_loan_approval(loan_amount=loan.principal_amount, user_email=loan.loan_applicant.email,
                                 user_name=loan.loan_applicant.name, payment_name=payment_name,
                                 payment_number=payment_number)

            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'message': 'Loan not found or already approved.'})


    except Exception as e:
        db_session.rollback()
        return jsonify({'success': False, 'message': str(e)})

    finally:
        db_session.close()


@app.route('/cancel-loan/<int:loan_id>', methods=['POST'])
@login_required
def cancel_loan(loan_id):
    db_session = Session()
    try:
        loan = db_session.query(LoanApplication).filter_by(id=loan_id).first()
        if loan:
            # Update loan status to NOT_APPROVED
            loan.loan_status = LoanStatus.NOT_APPROVED
            db_session.commit()

            # Send email notification to the applicant
            applicant_email = loan.loan_applicant.email
            applicant_name = loan.loan_applicant.name
            loan_amount = loan.principal_amount

            subject = "Loan Cancellation Notification"
            body_message = (
                f"Dear {applicant_name},\n\n"
                f"We regret to inform you that your loan request for UGX {loan_amount:,.0f} has been cancelled.\n\n"
                "If you have any questions or need further assistance, please contact our support team.\n\n"
                "Best regards,\n"
                "Creditrust Capital Solutions"
            )

            # Send the email
            if not send_mail(applicant_email, subject, body_message):
                flash("Failed to send email notification to the applicant.", "warning")

            return jsonify({"success": True, "message": "Loan cancelled successfully."})
        else:
            return jsonify({"success": False, "message": "Loan not found."}), 404
    except Exception as e:
        db_session.rollback()
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        db_session.close()


@app.route('/loans-applicant-details')
@login_required
def applicant_details():
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Fetch the company name and role
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role

        # Fetch modules data for the current app_id
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch all loan applicants for the current app_id
        loan_applicants = db_session.query(LoanApplicant).filter_by(app_id=app_id).all()

        # Initialize a list to hold applicant details with their loan applications, history, and total amount paid
        applicant_details = []

        for applicant in loan_applicants:
            # Fetch all loan applications for the current applicant
            loan_applications = db_session.query(LoanApplication).filter_by(loan_applicant_id=applicant.id).all()

            # Initialize a list to hold loan application details with their history
            loan_application_details = []

            # Initialize total amount paid by the applicant
            total_amount_paid_by_applicant = 0

            for loan_application in loan_applications:
                # Fetch all loan history entries for the current loan application
                loan_history = db_session.query(LoanHistory).filter_by(loan_id=loan_application.id).all()

                # Calculate total amount paid for this loan application
                total_amount_paid_for_loan = sum(history.amount_paid for history in loan_history)

                # Add to the total amount paid by the applicant
                total_amount_paid_by_applicant += total_amount_paid_for_loan

                # Append loan application details with its history
                loan_application_details.append({
                    'loan_application': loan_application,
                    'loan_history': loan_history,
                    'total_amount_paid_for_loan': total_amount_paid_for_loan  # Total paid for this loan
                })

            # Append applicant details with their loan applications, history, and total amount paid
            applicant_details.append({
                'applicant': applicant,
                'loan_applications': loan_application_details,
                'total_amount_paid_by_applicant': total_amount_paid_by_applicant  # Total paid by the applicant
            })

        return render_template(
            'loan_applicant_details.html',
            company=company,
            role=role,
            modules=modules_data,
            module_name="Loans",
            applicant_details=applicant_details
        )

    except Exception as e:
        db_session.rollback()
        flash(f'An error occurred while fetching applicant details: {str(e)}', 'danger')
        print(f"Error occurred: {e}")  # Print error message for debugging
        return redirect(url_for('loans_dashboard'))  # Redirect to an error page or dashboard

    finally:
        db_session.close()


@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    db_session = Session()

    try:

        app_id = current_user.app_id
        # Fetch the company name and role
        role = current_user.role
        users = db_session.query(User).filter_by(app_id=app_id).all()
        currency_preferences = db_session.query(Currency).filter_by(app_id=app_id).all()
        company = db_session.query(Company).filter_by(id=app_id).first()
        module_name = "General Ledger"

        if request.method == 'POST':

            if 'change_role' in request.form:
                user_id = request.form.get('user_id')
                new_role = request.form.get('role')
                user = db_session.query(User).filter_by(id=user_id, app_id=current_user.app_id).first()
                if user:
                    user.role = new_role
                    db_session.commit()
                    flash(f"Role of user {user.name} changed to {new_role}", 'success')
                else:
                    flash('User not found.', 'danger')

            if 'change_password' in request.form:
                old_password = request.form.get('old_password')
                new_password = request.form.get('new_password')
                confirm_password = request.form.get('confirm_password')

                if not current_user.check_password(old_password):
                    flash('Old password is incorrect.', 'danger')
                elif new_password != confirm_password:
                    flash('New passwords do not match.', 'danger')
                else:
                    user = db_session.query(User).filter_by(email=current_user.email,
                                                            app_id=current_user.app_id).first()
                    user.password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())
                    db_session.commit()
                    flash('Password changed successfully.', 'success')

            if 'add_user' in request.form:
                name = request.form.get('name')
                email = request.form.get('email')
                new_user_role = request.form.get('role')
                password = request.form.get('password')
                position = request.form.get('position')

                new_user = User(
                    name=name,
                    email=email,
                    position=position,
                    role=new_user_role,
                    password=bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()),
                    app_id=current_user.app_id
                )
                db_session.add(new_user)

                try:
                    # Assign permissions based on role
                    all_modules = db_session.query(Module).filter_by(app_id=current_user.app_id, included='yes').all()

                    perms = ROLE_PERMISSIONS.get(new_user_role, {})
                    for module in all_modules:
                        access = UserModuleAccess(
                            user_id=new_user.id,
                            module_id=module.id,
                            can_view=perms.get('can_view', False),
                            can_edit=perms.get('can_edit', False),
                            can_approve=perms.get('can_approve', False),
                            can_administer=perms.get('can_administer', False)
                        )
                        db_session.add(access)

                    db_session.commit()

                    # Send an email with credentials
                    subject = "Your New Account Details"
                    message = f"Hello {name},\n\n" \
                              f"An account has been created for you on our system.\n" \
                              f"Your login details are:\n\n" \
                              f"Email: {email}\n" \
                              f"Password: {password}\n\n" \
                              f"Please log in and change your password immediately.\n\n" \
                              f"Best regards,\nThe Team"

                    nb_send_mail(email, subject, message)
                    flash('New user added successfully.', 'success')
                except IntegrityError:
                    db_session.rollback()
                    flash('Email already exists.', 'danger')

            if 'update_modules' in request.form:
                user_id = request.form.get('user_id')
                print(f'Form data is {request.form}')
                # Delete existing permissions
                db_session.query(UserModuleAccess).filter_by(user_id=user_id).delete()

                # Add new permissions
                for module_id in request.form.getlist('module_ids'):
                    access = UserModuleAccess(
                        user_id=user_id,
                        module_id=module_id,
                        can_view=bool(request.form.get(f'can_view_{module_id}')),
                        can_edit=bool(request.form.get(f'can_edit_{module_id}')),
                        can_approve=bool(request.form.get(f'can_approve_{module_id}')),
                        can_administer=bool(request.form.get(f'can_administer_{module_id}'))
                    )
                    db_session.add(access)

                flash('Module permissions updated successfully', 'success')
            if 'update_currencies' in request.form:
                try:
                    main_currency = request.form.get('main_currency')

                    if not main_currency:
                        raise Exception("Main currency must be set")

                    additional_currencies = [currency for currency in request.form.getlist('additional_currency') if
                                             currency.strip()]

                    if main_currency in additional_currencies:
                        raise Exception(f"You cannot set Main currency {main_currency} as Additional currency.")

                    # Query existing currency preferences for the current user
                    existing_currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
                    existing_currency_map = {currency.user_currency: currency for currency in existing_currencies}
                    print(f'main_currency {main_currency} additional_currencies {additional_currencies} \
                    existing_currencies{existing_currencies}, existing_currency_map {existing_currency_map}')
                    if main_currency:
                        if main_currency in existing_currency_map:
                            if existing_currency_map[main_currency].currency_index != 1:
                                # Update the main currency to index 1 and previous main currency to index 2
                                for currency in existing_currencies:
                                    if currency.currency_index == 1:
                                        currency.currency_index = 2
                                existing_currency_map[main_currency].currency_index = 1
                            # else:
                            #     raise ValueError("Main currency is already set.")
                        else:
                            # If there's an existing main currency, set its index to 2
                            for currency in existing_currencies:
                                if currency.currency_index == 1:
                                    currency.currency_index = 2

                            # Add new main currency
                            main_currency_preference = Currency(user_currency=main_currency, currency_index=1,
                                                                app_id=app_id)
                            db_session.add(main_currency_preference)

                    if additional_currencies:
                        for currency in additional_currencies:
                            if currency in existing_currency_map:
                                if existing_currency_map[currency].currency_index != 2:
                                    existing_currency_map[currency].currency_index = 2

                            else:
                                # Add new additional currency
                                other_currency_preference = Currency(user_currency=currency, currency_index=2,
                                                                     app_id=app_id)
                                db_session.add(other_currency_preference)
                        company.has_multiple_currencies = True
                    db_session.commit()
                    flash("Currency preferences updated successfully.", "success")

                except ValueError as ve:
                    db_session.rollback()
                    flash(f"ValueError: {ve} - Check the values for currencies.", "info")
                except IntegrityError as e:
                    db_session.rollback()
                    flash(f"Currency is already in use in some of the transaction entries: Error details: {e}", "error")
                except Exception as e:
                    db_session.rollback()
                    flash(f"An error occurred: {e}", "error")
                finally:
                    db_session.close()
                    return redirect(url_for('settings'))

            if 'update_company' in request.form:
                app_name = request.form.get('app_name')
                if app_id and app_name:
                    name = request.form.get('name')
                    address = request.form.get('address')
                    phone = request.form.get('phone')
                    email = request.form.get('email')
                    website = request.form.get('website')

                    # Handle the logo upload
                    logo = request.files.get('logo')
                    logo_filename = None
                    if logo and allowed_file(logo.filename):
                        # Secure the filename and save it
                        logo_filename = secure_filename(logo.filename)
                        logo.save(os.path.join(UPLOAD_FOLDER_LOGOS, logo_filename))

                    # Update the company record
                    company = db_session.query(Company).filter_by(id=app_id).first()
                    company.name = name
                    company.address = address
                    company.phone = phone
                    company.email = email
                    company.website = website
                    if logo_filename:
                        company.logo = logo_filename  # Save the logo filename in the company record

                    try:
                        db_session.commit()
                        flash('Company details updated successfully.', 'success')
                    except Exception as e:
                        db_session.rollback()
                        flash('Error updating company details: ' + str(e), 'danger')
                    finally:
                        db_session.close()
                        return redirect(url_for('settings'))

            if 'update_footer_template' in request.form:
                footer_type = request.form.get('footer_type', 'text')
                company.footer_type = footer_type

                if footer_type == 'text':
                    # Handle text footer - preserve line breaks exactly as entered
                    footer_text = request.form.get('footer_text', '').strip()
                    company.footer = footer_text  # Store text directly in footer field

                    # Clear any existing template file if switching from file to text
                    if company.footer and allowed_file(company.footer):
                        try:
                            file_path = os.path.join(UPLOAD_FOLDER_FOOTER, company.footer)
                            if file_exists(file_path):
                                os.remove(file_path)
                        except Exception as e:
                            db_session.rollback()
                            flash(f'Error removing old template file: {str(e)}', 'warning')

                else:
                    # Handle file upload - same logic as logo
                    template_file = request.files.get('footer_template_file')

                    if template_file and template_file.filename:
                        # Validate file type using your existing function
                        if not allowed_file(template_file.filename):
                            flash(f'Invalid file type. Only {", ".join(app.config["ALLOWED_EXTENSIONS"])} allowed.',
                                  'danger')
                            return redirect(url_for('settings'))

                        # Generate unique filename with timestamp
                        timestamp = int(time.time())
                        file_ext = os.path.splitext(template_file.filename)[1].lower()
                        filename = f"footer_{app_id}_{timestamp}{file_ext}"
                        filename = secure_filename(filename)

                        # Delete old file if exists
                        if company.footer and allowed_file(company.footer):
                            try:
                                old_file_path = os.path.join(UPLOAD_FOLDER_FOOTER, company.footer)
                                if file_exists(old_file_path):
                                    os.remove(old_file_path)
                            except Exception as e:
                                db_session.rollback()
                                flash(f'Error removing old template: {str(e)}', 'warning')

                        # Save new file
                        try:
                            os.makedirs(UPLOAD_FOLDER_FOOTER, exist_ok=True)
                            file_path = os.path.join(UPLOAD_FOLDER_FOOTER, filename)
                            template_file.save(file_path)
                            company.footer = filename  # Store filename in footer field
                            company.footer_type = 'template'
                        except Exception as e:
                            db_session.rollback()
                            flash(f'Error saving template file: {str(e)}', 'danger')
                            return redirect(url_for('settings'))
                    elif not (company.footer and allowed_file(company.footer)):
                        flash('No template file selected', 'danger')
                        return redirect(url_for('settings'))

                try:
                    db_session.commit()
                    flash('Footer template updated successfully', 'success')
                except Exception as e:
                    db_session.rollback()
                    flash(f'Error updating footer template: {str(e)}', 'danger')

        # Final fetching of data after possible updates
        main_currency = next((cp.user_currency for cp in currency_preferences if cp.currency_index == 1), None)
        additional_currencies = [cp.user_currency for cp in currency_preferences if cp.currency_index == 2]
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        all_modules = db_session.query(Module).filter_by(app_id=app_id).all()

        return render_template('settings.html', role=role, main_currency=main_currency,
                               additional_currencies=additional_currencies, company=company, users=users,
                               modules=modules_data, module_name=module_name, currency_list=CURRENCY_LIST,
                               all_modules=all_modules)

    finally:
        db_session.close()


@app.route('/activity_logs', methods=['GET'])
@login_required  # Ensure the user is logged in
def activity_logs():
    db_session = Session()  # Open a new session
    app_id = current_user.app_id

    try:
        # Fetch the company name and role
        company_name = db_session.query(Company.name).filter_by(id=app_id).first()[0]
        role = db_session.query(User.role).filter_by(app_id=app_id).first()[0]
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        return render_template('activity_logs.html', company_name=company_name, role=role,
                               modules=modules_data)

    except Exception as e:
        print(f"An error occurred while fetching activity logs: {e}")  # Log the error for debugging
        return "An error occurred while processing your request.", 500  # Return a 500 status code for server errors

    finally:
        db_session.close()  # Ensure the session is closed


@app.route('/get_activity_logs', methods=['GET'])
@login_required  # Ensure the user is logged in
def get_activity_logs():
    db_session = Session()  # Open a new session
    app_id = session.get('app_id')

    try:
        if app_id:
            logs = db_session.query(ActivityLog).filter_by(app_id=app_id).all()
        else:
            logs = []

        return jsonify([{
            'user': log.user,
            'activity': log.activity,
            'date': log.date,
            'details': log.details
        } for log in logs])

    except Exception as e:
        print(f"An error occurred while fetching activity logs: {e}")  # Log the error for debugging
        return jsonify({'error': str(e)}), 500  # Return a 500 status code for server errors

    finally:
        db_session.close()  # Ensure the session is closed


@app.route('/income_expense_report', methods=['GET'])
@login_required
def income_expense_report():
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Fetch the company name, role, and modules data
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        api_key = company.api_key

        print(f'Modules data is {modules_data}')

        # Fetch base currency info
        base_currency_info = get_base_currency(db_session, app_id)

        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_name = base_currency_info["base_currency"]

        return render_template(
            'base_currency_income_and_expense_report.html',
            company=company,
            role=role,
            modules=modules_data,
            base_currency=base_currency_name,
            api_key=api_key  # Pass the api_key data to the template
        )
    finally:
        db_session.close()


@app.route('/base_income_expense_report', methods=['GET'])
@login_required
def base_income_expense_report():
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Fetch the company name, role, and modules data
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        api_key = company.api_key
        print(f'Modules data is {modules_data}')

        return render_template(
            'base_currency_income_and_expense_report.html',
            company=company,
            role=role,
            modules=modules_data,
            api_key=api_key  # Pass the api_key data to the template
        )
    finally:
        db_session.close()


@app.route('/balance_sheet', methods=['GET', 'POST'])
@login_required
def balance_sheet():
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Fetch company details
        company = db_session.query(Company).filter_by(id=app_id).first()
        if not company:
            return jsonify({'success': False, 'message': 'Company not found'}), 404

        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        api_key = company.api_key

        # Fetch base currency info
        base_currency_info = get_base_currency(db_session, app_id)

        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_name = base_currency_info["base_currency"]

        # Fetch currency data
        currency_data = db_session.query(Currency).filter_by(app_id=app_id).all()

        # Initialize filter variables
        currency = None
        start_date = None
        end_date = None

        if request.method == 'POST':
            # Handle JSON Data with error checking
            try:
                filters = request.get_json()
                if not filters:
                    return jsonify({'success': False, 'message': 'No filter data provided'}), 400

                currency = filters.get('currency')
                start_date_str = filters.get('startDate')
                end_date_str = filters.get('endDate')

                # Date parsing with validation
                if start_date_str:
                    try:
                        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
                    except ValueError:
                        return jsonify({'success': False, 'message': 'Invalid start date format'}), 400

                if end_date_str:
                    try:
                        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
                    except ValueError:
                        return jsonify({'success': False, 'message': 'Invalid end date format'}), 400

                # Handle Default Currency Selection
                if not currency:
                    default_currency = db_session.query(Currency.id).filter(
                        Currency.app_id == app_id,
                        Currency.currency_index == 1
                    ).first()
                    currency = default_currency.id if default_currency else None

                # Validate date range
                if start_date and end_date and start_date > end_date:
                    return jsonify({'success': False, 'message': 'Start date must be before or equal to end date'}), 400

                # Set Default Date Range
                if not start_date:
                    start_date = db_session.query(func.min(Transaction.date)).filter(
                        Transaction.app_id == app_id).scalar()
                if not end_date:
                    end_date = db_session.query(func.max(Transaction.date)).filter(
                        Transaction.app_id == app_id).scalar()

                return jsonify({
                    'currency': currency,
                    'start_date': start_date.strftime('%Y-%m-%d') if start_date else None,
                    'end_date': end_date.strftime('%Y-%m-%d') if end_date else None
                })

            except Exception as e:
                db_session.rollback()
                return jsonify({'success': False, 'message': str(e)}), 500

        else:  # GET Request
            try:
                currency = request.args.get('currency')
                start_date_str = request.args.get('startDate')
                end_date_str = request.args.get('endDate')

                # Date parsing with validation
                if start_date_str:
                    try:
                        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
                    except ValueError:
                        return render_template('error.html', error='Invalid start date format'), 400

                if end_date_str:
                    try:
                        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
                    except ValueError:
                        return render_template('error.html', error='Invalid end date format'), 400

                # Handle Default Currency Selection
                if not currency:
                    default_currency = db_session.query(Currency.id).filter(
                        Currency.app_id == app_id,
                        Currency.currency_index == 1
                    ).first()
                    currency = default_currency.id if default_currency else None

                # Set Default Date Range
                if not start_date:
                    start_date = db_session.query(func.min(Transaction.date)).filter(
                        Transaction.app_id == app_id).scalar()
                if not end_date:
                    end_date = db_session.query(func.max(Transaction.date)).filter(
                        Transaction.app_id == app_id).scalar()

                return render_template(
                    'base_currency_balance_sheet.html',
                    company=company,
                    role=role,
                    modules=modules_data,
                    api_key=api_key,
                    currency=currency,
                    start_date=start_date.strftime('%Y-%m-%d') if start_date else None,
                    end_date=end_date.strftime('%Y-%m-%d') if end_date else None,
                    currency_data=currency_data,
                    base_currency=base_currency_name
                )

            except Exception as e:
                db_session.rollback()
                return render_template('error.html', error=str(e)), 500

    except Exception as e:
        # Handle any exceptions that occur before the try block
        return jsonify({'success': False, 'message': str(e)}), 500

    finally:
        # Ensure session is always closed
        db_session.close()


@app.route('/trial_balance', methods=['GET'])
@login_required
def trial_balance():
    db_session = Session()  # Open a new session

    try:
        app_id = current_user.app_id

        # Fetch the company name and role
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = db_session.query(User.role).filter_by(app_id=app_id).first()[0]
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        api_key = company.api_key
        # Fetch base currency info
        base_currency_info = get_base_currency(db_session, app_id)

        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_name = base_currency_info["base_currency"]

        return render_template(
            'base_currency_trial_balance.html',
            company=company,
            role=role,
            modules=modules_data,
            api_key=api_key,
            base_currency=base_currency_name
        )

    except Exception as e:
        print(f"An error occurred while fetching trial balance data: {e}")  # Log the error for debugging
        return "An error occurred while processing your request.", 500  # Return a 500 status code for server errors

    finally:
        db_session.close()  # Ensure the session is closed


@app.route('/cash_flow_statement', methods=['GET', 'POST'])
@login_required
def cash_flow_statement():
    db_session = Session()

    try:
        # Retrieve app_id from the current user's session
        app_id = current_user.app_id

        # Fetch the company name and role
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Get the main currency (where currency_index = 1)
        main_currency = db_session.query(Currency).filter_by(currency_index=1, app_id=app_id).first()

        # Check if the request is a POST request (AJAX)
        if request.method == 'POST' and request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            # Get filter parameters from the request body (JSON)
            data = request.get_json()
            currency = data.get('currency', main_currency.id)
            start_date = data.get('start_date')
            end_date = data.get('end_date')
            subcategory_id = data.get('subcategory')
        else:
            # Get filter parameters from the URL query string (GET request)
            currency = request.args.get('currency', main_currency.id)
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            subcategory_id = request.args.get('subcategory')

        print(
            f"Received parameters: app_id={app_id}, currency={currency}, start_date={start_date}, end_date={end_date}, subcategory={subcategory_id}")

        # Validate the dates using the validate_date function
        if start_date:
            start_date = validate_date(start_date, is_start_date=True)
        else:
            # If no start_date is provided, fetch from the earliest available transaction date
            earliest_transaction = db_session.query(Transaction.date).filter_by(app_id=app_id).order_by(
                Transaction.date.asc()).first()
            print(f'Earliest transaction date is {earliest_transaction}')
            start_date = earliest_transaction[0] if earliest_transaction else None

        if end_date:
            end_date = validate_date(end_date, is_start_date=False, compare_date=start_date)
        else:
            # If no end_date is provided, fetch till the latest available transaction date
            latest_transaction = db_session.query(Transaction.date).filter_by(app_id=app_id).order_by(
                Transaction.date.desc()).first()
            end_date = latest_transaction[0] if latest_transaction else None

        print(f"Parsed start_date={start_date}, end_date={end_date} and currency is {currency}")

        # Fetch cash balances and total cash balance
        cash_balances, total_cash_balance = get_cash_balances(db_session, app_id, currency, start_date, end_date)

        # Fetch all subcategories for the dropdown
        subcategories = {entry["subcategory"]: entry["subcategory_id"] for entry in cash_balances}
        print(f'Lenth of subcategories is {len(subcategories)}')

        # Filter cash balances to include only the selected/default account
        if subcategory_id:
            cash_balances = [balance for balance in cash_balances if
                             balance.get('subcategory_id') == int(subcategory_id)]
            print(f'Yes subcategory id is available {subcategory_id} {cash_balances}')
        else:
            print(f'No subcategory id is available {subcategory_id} {cash_balances}')
            # If no subcategory is provided, use the first cash-related account as the default
            if cash_balances:
                subcategory_id = cash_balances[0].get('subcategory_id')
                cash_balances = [cash_balances[0]]  # Only include the default account

        print(f"Filtered cash_balances={cash_balances}, total_cash_balance={total_cash_balance}")

        # Fetch the selected subcategory (if any)
        selected_subcategory_id = int(subcategory_id) if subcategory_id else None

        # Fetch the selected currency (if any)
        selected_currency_id = int(currency) if currency else main_currency.id

        # get user_currency to render front end
        user_currency = db_session.query(Currency.user_currency).filter_by(id=currency).scalar()

        # Build query to fetch transactions, including filters based on provided parameters
        transactions_query = db_session.query(Transaction).filter_by(app_id=app_id)

        # Filter by currency
        transactions_query = transactions_query.filter_by(currency=currency)

        # Filter by date range if provided
        if start_date:
            transactions_query = transactions_query.filter(Transaction.date >= start_date)

        if end_date:
            transactions_query = transactions_query.filter(Transaction.date <= end_date)

        # Filter by subcategory (chart_of_accounts.id) if provided
        if subcategory_id:
            transactions_query = transactions_query.filter(Transaction.subcategory_id == subcategory_id)

        transactions = transactions_query.all()

        print(f"Fetched transactions: {len(transactions)} records")

        # Filter for only cash-related transactions
        def is_cash_transaction(transaction):
            return (
                    transaction.transaction_type == "Asset" and
                    (is_cash_related(transaction.category.category) or
                     (transaction.chart_of_accounts and is_cash_related(transaction.chart_of_accounts.sub_category)))
            )

        cash_transactions = [t for t in transactions if is_cash_transaction(t)]

        print(f"Filtered cash transactions: {len(cash_transactions)} records")

        # Build transaction details for cash-related transactions (debits and credits)
        transactions_list = [
            {
                "id": transaction.id,
                "transaction_type": transaction.transaction_type,
                "date": transaction.date.strftime('%Y-%m-%d'),
                "category": transaction.category.category if transaction.category else "Unknown Category",
                "subcategory": transaction.chart_of_accounts.sub_category if transaction.chart_of_accounts else "Unknown Subcategory",
                "subcategory_id": transaction.subcategory_id if transaction.subcategory_id else None,
                "currency": transaction.currencies.user_currency if transaction.currencies else "Unknown Currency",
                "amount": round(transaction.amount, 2),  # Amounts are already divided by 1000 in the query
                "dr_cr": transaction.dr_cr,
                "description": transaction.description or "No Description",
                "payment_mode": transaction.payment_mode.payment_mode if transaction.payment_mode else "Unknown Payment Mode",
                "payment_to_vendor": transaction.vendor.vendor_name if transaction.vendor else "No Vendor",
                "project_name": transaction.project.name if transaction.project else "No Project",
                "date_added": transaction.date_added.strftime('%Y-%m-%d') if transaction.date_added else "Unknown Date"
            }
            for transaction in cash_transactions
        ]

        print(f"Constructed transaction details: {len(transactions_list)} records")

        # Prepare the response data with both the cash balances and transaction details
        response = {
            "cash_balances": cash_balances,
            "total_cash_balance": round(total_cash_balance, 2),
            "transactions": transactions_list
        }

        # Check if the request is an AJAX request
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify(response)  # Return JSON data for AJAX requests

        # Render the template for non-AJAX requests
        return render_template(
            'cash_flow_statement.html',
            response=response,
            company=company,
            modules=modules_data,
            role=role,
            user_currency=user_currency,
            subcategories=subcategories,
            selected_subcategory_id=selected_subcategory_id,
            selected_currency_id=selected_currency_id,
            cash_account=cash_balances[0] if cash_balances else None
        )

    except (ValueError, SQLAlchemyError) as e:
        db_session.rollback()
        flash(f"An error has occurred: {e}", "error")
        return redirect(url_for('financial_statement'))
    finally:
        db_session.close()


@app.route('/equity_report', methods=['GET'])
@login_required
def equity_report():
    db_session = Session()

    app_id = current_user.app_id

    # Fetch the company name and role
    company_name = db_session.query(Company.name).filter_by(id=app_id).first()[0]
    role = db_session.query(User.role).filter_by(app_id=app_id).first()[0]
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

    return render_template(
        'equity_report.html',
        company_name=company_name,
        role=role,
        modules=modules_data
    )


@app.route('/get_currency', methods=['GET'])
@login_required
def get_currency():
    db_session = Session()  # Open a new session

    try:
        app_id = current_user.app_id  # Fetch the company ID for the current user

        # Query the Currency table for the current company
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()

        # Convert the query result to a list of dictionaries with currency_id
        currencies_list = [
            {
                'currency_id': currency.id,
                'user_currency': currency.user_currency,
                'currency_index': currency.currency_index
            }
            for currency in currencies
        ]

        # Return the list as a JSON response
        return jsonify(currencies_list)

    except Exception as e:
        print(f"An error occurred while fetching currencies: {e}")  # Log the error for debugging
        return jsonify({'error': str(e)}), 500  # Return a 500 status code for server errors

    finally:
        db_session.close()  # Ensure the session is closed


@app.route('/api/batches', methods=['GET'])
@login_required
def get_batches():
    search_term = request.args.get('term', '')
    app_id = current_user.app_id  # Get the current user's app_id
    db_session = Session()

    try:
        if not search_term:
            return jsonify({'error': 'Search term cannot be empty'}), 400

        # Query the database for batches that match the search term
        batches = db_session.query(Batch).filter(
            Batch.batch_number.ilike(f'%{search_term}%'),
            Batch.app_id == app_id
        ).limit(10).all()  # Limit the number of results for performance

        # Return the batch numbers as JSON
        return jsonify([batch.batch_number for batch in batches])

    finally:
        # Close the session to release resources
        db_session.close()


@app.route('/api/item_codes', methods=['GET'])
@login_required
def get_item_codes():
    query = request.args.get('query', '').strip()
    app_id = current_user.app_id

    if not app_id:
        return jsonify({'error': 'app_id is required'}), 400

    if not query:
        return jsonify({'error': 'Query cannot be empty'}), 400

    with Session() as db_session:
        # Fetch matching item codes with a limit
        matching_items = (
            db_session.query(InventoryItem)
            .filter(InventoryItem.app_id == app_id, InventoryItem.item_code.ilike(f'%{query}%'))
            .limit(10)  # Limit results for performance
            .all()
        )

        # Return the item codes as a JSON response
        return jsonify([{'id': item.id, 'item_code': item.item_code} for item in matching_items])


@app.route('/inventory/files/<filename>')
def uploaded_file(filename):
    folder_path = app.config['UPLOAD_FOLDER_INVENTORY']
    file_path = os.path.join(folder_path, filename)

    # Check if the requested file exists
    if not file_exists(file_path):
        # Serve the placeholder image if the requested file does not exist
        folder_path = os.path.dirname(app.config['PLACEHOLDER_IMAGE'])
        filename = os.path.basename(app.config['PLACEHOLDER_IMAGE'])
        file_path = os.path.join(folder_path, filename)

    return send_from_directory(folder_path, filename, environ=request.environ)


@app.route('/add_brand', methods=['POST'])
@login_required
def add_brand():
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Extract the form data
        brand_name = request.form.get('name')
        brand_description = request.form.get('description')

        # Create a new brand
        new_brand = Brand(
            app_id=app_id,
            name=brand_name,
            description=brand_description
        )
        db_session.add(new_brand)
        db_session.commit()

        # Return the new brand details as a JSON response
        return jsonify({'id': new_brand.id, 'name': new_brand.name})
    finally:
        db_session.close()  # Ensure the session is closed


# Add a new Unit of Measurement
@app.route('/add_uom', methods=['POST'])
@login_required
def add_uom():
    db_session = Session()  # Use the existing session from SQLAlchemy
    try:
        app_id = current_user.app_id  # Get the current user's app_id

        # Extract the form data
        full_name = request.form.get('full_name')
        abbreviation = request.form.get('abbreviation')

        # Create a new unit of measurement
        new_uom = UnitOfMeasurement(
            app_id=app_id,
            full_name=full_name,
            abbreviation=abbreviation
        )
        db_session.add(new_uom)
        db_session.commit()

        # Return the new unit of measurement details as a JSON response
        return jsonify({'id': new_uom.id, 'full_name': new_uom.full_name, 'abbreviation': new_uom.abbreviation}), 201
    except Exception as e:
        db_session.rollback()  # Rollback in case of error
        return jsonify({'error': str(e)}), 400
    finally:
        db_session.close()  # Ensure the session is closed


# Get all Units of Measurement
@app.route('/uoms', methods=['GET'])
@login_required
def get_uoms():
    db_session = Session()
    try:
        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=current_user.app_id).all()
        return jsonify(
            [{"id": uom.id, "full_name": uom.full_name, "abbreviation": uom.abbreviation} for uom in uoms]), 200
    finally:
        db_session.close()


# Get a specific Unit of Measurement
@app.route('/uom/<int:uom_id>', methods=['GET'])
@login_required
def get_uom(uom_id):
    db_session = Session()
    try:
        uom = db_session.query(UnitOfMeasurement).filter_by(id=uom_id, app_id=current_user.app_id).first_or_404()
        return jsonify({"id": uom.id, "full_name": uom.full_name, "abbreviation": uom.abbreviation}), 200
    finally:
        db_session.close()


# Update a Unit of Measurement
@app.route('/uom/<int:uom_id>', methods=['PUT'])
@login_required
def update_uom(uom_id):
    db_session = Session()
    try:
        uom = UnitOfMeasurement.query.filter_by(id=uom_id, app_id=current_user.app_id).first_or_404()
        # Extract the form data
        full_name = request.form.get('full_name', uom.full_name)
        abbreviation = request.form.get('abbreviation', uom.abbreviation)

        # Update the unit of measurement
        uom.full_name = full_name
        uom.abbreviation = abbreviation
        db_session.commit()

        return jsonify({"message": "Unit of Measurement updated"}), 200
    except Exception as e:
        db_session.rollback()
        return jsonify({'error': str(e)}), 400
    finally:
        db_session.close()


@app.route('/add_attribute', methods=['POST'])
@login_required
def add_attribute():
    db_session = Session()
    app_id = current_user.app_id
    # Get the new attribute name and app_id from the request
    new_attribute_name = request.form.get('attribute_name').strip()

    # Create a new InventoryItemAttribute instance
    new_attribute = InventoryItemAttribute(attribute_name=new_attribute_name, app_id=app_id)

    try:
        # Add the new attribute to the database
        db_session.add(new_attribute)
        db_session.commit()
        return jsonify({'id': new_attribute.id, 'attribute_name': new_attribute.attribute_name,
                        'message': 'Attribute added successfully!'}), 201
    except Exception as e:
        # Handle any database errors
        db_session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()


@app.route('/add_variation', methods=['POST'])
@login_required
def add_variation():
    db_session = Session()
    try:
        app_id = current_user.app_id
        attribute_id = request.form.get('attribute_id')
        variation_name = request.form.get('variation_name').strip()

        if attribute_id and variation_name:
            # Create new variation in the database
            new_variation = InventoryItemVariation(
                attribute_id=attribute_id,
                variation_name=variation_name,
                app_id=app_id
            )
            db_session.add(new_variation)
            db_session.commit()

            return jsonify({
                'message': 'Variation added successfully',
                'id': new_variation.id,
                'variation_name': new_variation.variation_name
            })
        else:
            return jsonify({'error': 'Missing attribute or variation name'}), 400
    finally:
        db_session.close()  # Ensure the session is closed


@app.route('/add_inventory_location', methods=['POST'])
@login_required
def add_inventory_location():
    db_session = Session()

    # Extract data from the form
    location_name = request.form.get('location').strip()
    description = request.form.get('description').strip()

    # Validate the form data
    if not location_name:
        return jsonify({'success': False, 'error': 'All fields are required'}), 400

    # Create a new InventoryLocation entry
    new_location = InventoryLocation(
        app_id=current_user.app_id,  # Assuming current_user has an app_id field
        location=location_name,
        description=description
    )

    try:
        # Add to the session and commit to the database
        db_session.add(new_location)
        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Location added successfully',
            'location_id': new_location.id,
            'location_name': new_location.location
        })

    except Exception as e:
        db_session.rollback()  # Rollback in case of error
        return jsonify({'success': False, 'message': f"An error has occurred str{(e)}"}), 500

    finally:
        db_session.close()


@app.route('/add_inventory_category', methods=['POST'])
@login_required
def add_inventory_category():
    db_session = Session()
    try:
        app_id = current_user.app_id
        category_name = request.form.get('category_name').strip()

        new_inventory_category = InventoryCategory(
            category_name=category_name,
            app_id=app_id
        )
        db_session.add(new_inventory_category)
        db_session.commit()

        return jsonify({
            'id': new_inventory_category.id,
            'category_name': category_name
        })
    finally:
        db_session.close()  # Ensure that the session is closed


@app.route('/add_inventory_subcategory', methods=['POST'])
@login_required
def add_inventory_subcategory():
    db_session = Session()
    try:
        app_id = current_user.app_id
        subcategory_name = request.form.get('subcategory_name').strip()
        category_id = request.form.get('category_id')

        new_inventory_subcategory = InventorySubCategory(
            item_category_id=category_id,
            subcategory_name=subcategory_name,
            app_id=app_id
        )
        db_session.add(new_inventory_subcategory)
        db_session.commit()

        return jsonify({
            'id': new_inventory_subcategory.id,
            'subcategory_name': subcategory_name,
            'category_id': category_id
        })
    finally:
        db_session.close()  # Ensure that the session is closed


@app.route('/get_item_details/<int:item_id>', methods=['GET'])
@login_required
def get_item_details(item_id):
    db_session = Session()

    try:
        item = db_session.query(InventoryItem).filter_by(id=item_id, app_id=current_user.app_id).first()
        if item:
            return jsonify({
                'item_code': item.item_code,
                'category_id': item.inventory_category.category_name,
                'subcategory_id': item.inventory_subcategory.subcategory_name
            }), 200
        else:
            return jsonify({'error': 'Item not found'}), 404
    except Exception as e:
        print(f"Error fetching item details: {e}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        db_session.close()


@app.route('/inventory_entries', methods=['GET', 'POST'])
@login_required
def inventory_entries():
    db_session = Session()
    app_id = current_user.app_id

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        # Fetch enabled modules for the app
        modules_data = (
            db_session.query(Module.module_name)
            .filter_by(app_id=app_id, included='yes')
            .all()
        )
        modules_data = [mod.module_name for mod in modules_data]

        # Process filter options
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        filter_type = request.args.get('filter_type', 'transaction_date')  # Default to transaction_date
        filter_applied = bool(start_date or end_date)
        query = db_session.query(InventoryEntry).filter_by(app_id=app_id).order_by(InventoryEntry.date_added.desc())

        # Apply date filters based on the filter type
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

                if filter_type == 'transaction_date':
                    if start_date:
                        query = query.filter(InventoryEntry.transaction_date >= start_date)
                    if end_date:
                        query = query.filter(InventoryEntry.transaction_date <= end_date)
                elif filter_type == 'date_added':
                    if start_date:
                        query = query.filter(InventoryEntry.date_added >= start_date)
                    if end_date:
                        query = query.filter(InventoryEntry.date_added <= end_date)
            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')

        inventory_entries_data = query.all()

        # Convert decimal values to float in the query result if needed
        for entry in inventory_entries_data:
            if isinstance(entry.quantity, decimal.Decimal):
                entry.quantity = float(entry.quantity)
            if isinstance(entry.unit_price, decimal.Decimal):
                entry.unit_price = float(entry.unit_price)

        return render_template(
            'inventory_entries.html',
            inventory_entries=inventory_entries_data,
            modules=modules_data,
            company=company,
            role=role,
            filter_applied=filter_applied,
            module_name="Inventory"
        )
    finally:
        db_session.close()


@app.route('/inventory_entries/edit/<int:entry_id>', methods=['GET', 'POST'])
@login_required
def edit_inventory_entry(entry_id):
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role

        # Fetch dropdown data (keep your existing code)
        projects = db_session.query(Project).filter_by(app_id=app_id).all()
        currencies = db_session.query(Currency).filter_by(app_id=app_id).order_by(Currency.currency_index).all()
        locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
        inventory_item = db_session.query(InventoryItem).filter_by(app_id=app_id).all()
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
        subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
        variations_data = db_session.query(InventoryItemVariation).filter_by(app_id=app_id).all()
        attributes_data = db_session.query(InventoryItemAttribute).filter_by(app_id=app_id).all()
        suppliers = db_session.query(Vendor).filter_by(app_id=app_id).all()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        brands = db_session.query(Brand).filter_by(app_id=app_id).all()
        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()

        # Fetch entry to be edited
        entry = db_session.query(InventoryEntry).filter_by(app_id=app_id).filter_by(id=entry_id).first()
        if not entry:
            flash("Inventory entry not found.", "error")
            return redirect(url_for('inventory_entries'))

        if request.method == 'POST':
            # Process form data (keep your existing form processing code)
            new_from_location = request.form.get('from_location') or None
            new_to_location = request.form.get('to_location') or None
            new_item_id = request.form['item_name']
            new_quantity = float(request.form['quantity'])
            batch_number = request.form.get('batch_number') or None
            batch_id = request.form.get('batch_id') or None
            selling_price = float(request.form['selling_price'])
            unit_price = float(request.form['unit_price'])
            uom = request.form['uom']
            # Option 2 (Better): Use double quotes for the f-string

            # Handle expiration date
            expiry_date = request.form.get('expiration_date')
            expiration_date = datetime.datetime.strptime(expiry_date, '%Y-%m-%d') if expiry_date else None

            # Transaction date
            transaction_date = (
                datetime.datetime.strptime(request.form['transaction_date'], '%Y-%m-%d')
                if request.form['transaction_date']
                else None
            )

            # Validate inputs (keep your existing validation)
            validate_quantity_and_price(new_quantity, unit_price)
            validate_quantity_and_selling_price(new_quantity, selling_price)
            logger.info(f'Form data is {request.form}')
            # Handle batch selection/creation (keep your existing batch handling)
            if batch_id:
                new_batch_id = int(batch_id)
            elif batch_number:
                existing_batch = db_session.query(Batch).filter(
                    func.lower(Batch.batch_number) == batch_number.lower(),
                    Batch.app_id == app_id
                ).first()
                if existing_batch:
                    new_batch_id = existing_batch.id
                else:
                    new_batch = Batch(batch_number=batch_number, app_id=app_id)
                    db_session.add(new_batch)
                    db_session.flush()
                    new_batch_id = new_batch.id
            else:
                new_batch_id = entry.lot

            # Store old values
            old_values = {
                'quantity': entry.quantity,
                'from_location': entry.from_location,
                'to_location': entry.to_location,
                'lot': entry.lot,
                'item_id': entry.item_id
            }

            # Update the inventory entry (keep your existing entry update code)
            entry.item_id = new_item_id

            entry.project_id = request.form['project_id'] or None
            entry.supplier_id = request.form['supplier'] or None
            entry.uom = request.form['uom']
            entry.currency_id = request.form['currency']
            entry.quantity = new_quantity
            entry.unit_price = unit_price
            entry.selling_price = selling_price
            entry.expiration_date = expiration_date
            entry.from_location = new_from_location
            entry.to_location = new_to_location
            entry.lot = new_batch_id
            entry.transaction_date = (
                datetime.datetime.strptime(request.form['transaction_date'], '%Y-%m-%d')
                if request.form['transaction_date']
                else None
            )
            entry.date_added = datetime.datetime.today()
            entry.updated_by = current_user.id

            # Handle BatchVariationLink updates based on movement type
            handle_batch_variation_update(
                db_session=db_session,
                movement_type=entry.stock_movement,
                old_values=old_values,
                new_values={
                    'quantity': new_quantity,
                    'from_location': new_from_location,
                    'to_location': new_to_location,
                    'lot': new_batch_id,
                    'item_id': new_item_id,
                    'unit_price': unit_price,
                    'currency_id': request.form['currency'],
                    'supplier_id': request.form['supplier'],
                    'uom': uom,
                    'expiration_date': expiration_date,
                    'transaction_date': transaction_date
                },
                app_id=app_id
            )

            db_session.commit()
            flash('Inventory entry updated successfully!', 'success')
            return redirect(url_for('inventory_entries'))

        inventory_items = db_session.query(InventoryItemVariationLink).filter_by(app_id=app_id, status="active").all()

        return render_template('/inventory/edit_inventory.html', entry=entry, inventory_item=inventory_item,
                               categories=categories, subcategories=subcategories, projects=projects,
                               currencies=currencies, inventory_attributes=attributes_data,
                               inventory_variations=variations_data, suppliers=suppliers,
                               modules=modules_data, inventory_items=inventory_items,
                               locations=locations, brands=brands, uoms=uoms,
                               company=company, role=role, module_name="Inventory")

    except ValueError as e:
        flash(f"Validation error: {str(e)}", "error")
        db_session.rollback()
        return redirect(url_for("edit_inventory_entry", entry_id=entry_id))
    except Exception as e:
        flash(f"An error occurred: {str(e)}", "error")
        db_session.rollback()
        return redirect(url_for("edit_inventory_entry", entry_id=entry_id))
    finally:
        db_session.close()


@app.route('/delete_inventory_transaction/<int:transaction_id>', methods=['POST'])
@login_required
def delete_inventory_transaction(transaction_id):
    with Session() as db_session:
        app_id = current_user.app_id
        try:
            # Fetch the inventory transaction
            inventory_transaction = db_session.query(InventoryEntry).filter_by(
                id=transaction_id,
                app_id=app_id
            ).first()

            if not inventory_transaction:
                return jsonify({'error': 'Inventory transaction not found or not authorized'}), 404

            # Start a nested transaction for atomic operations
            with db_session.begin_nested():
                # Handle ledger transaction if exists
                related_transaction = db_session.query(Transaction).filter_by(
                    source_id=transaction_id,
                    source_type="inventory_entry",
                    app_id=app_id
                ).first()

                # Get the correct location ID based on movement type
                location_id = None
                if inventory_transaction.stock_movement == "in":
                    location_id = inventory_transaction.to_location
                elif inventory_transaction.stock_movement in ["out", "missing"]:
                    location_id = inventory_transaction.from_location

                # Fetch the BatchVariationLink
                batch_variation_link = None
                if location_id and inventory_transaction.lot:
                    batch_variation_link = db_session.query(BatchVariationLink).filter_by(
                        batch_id=inventory_transaction.lot,
                        location_id=location_id,
                        item_id=inventory_transaction.item_id,  # Corrected column name
                        app_id=app_id
                    ).first()

                # Update inventory quantities
                if batch_variation_link:
                    if inventory_transaction.stock_movement == "in":
                        batch_variation_link.quantity -= inventory_transaction.quantity
                    elif inventory_transaction.stock_movement == "out":
                        batch_variation_link.quantity += inventory_transaction.quantity
                    elif inventory_transaction.stock_movement == "missing":
                        # For missing items, we add back the quantity (same as 'out')
                        batch_variation_link.quantity += inventory_transaction.quantity
                    elif inventory_transaction.stock_movement == "transfer":
                        # Handle transfer movements
                        from_link = db_session.query(BatchVariationLink).filter_by(
                            batch_id=inventory_transaction.lot,
                            location_id=inventory_transaction.from_location,
                            item_id=inventory_transaction.item_id,
                            app_id=app_id
                        ).first()
                        to_link = db_session.query(BatchVariationLink).filter_by(
                            batch_id=inventory_transaction.lot,
                            location_id=inventory_transaction.to_location,
                            item_id=inventory_transaction.item_id,
                            app_id=app_id
                        ).first()

                        if from_link:
                            from_link.quantity += inventory_transaction.quantity
                        if to_link:
                            to_link.quantity -= inventory_transaction.quantity

                # Delete the transactions
                if related_transaction:
                    db_session.delete(related_transaction)
                db_session.delete(inventory_transaction)

                # Log the activity
                new_log = ActivityLog(
                    user=current_user.email,
                    activity=f"Inventory {inventory_transaction.stock_movement} deleted",
                    details=f"Deleted transaction ID {transaction_id}",
                    app_id=app_id
                )
                db_session.add(new_log)

            db_session.commit()
            flash('Inventory transaction deleted successfully!', 'success')
            return redirect(url_for('inventory_entries'))

        except IntegrityError as e:
            db_session.rollback()
            flash('Cannot delete - referenced in other records', 'warning'), 400
            return redirect(url_for('inventory_entries'))

        except Exception as e:
            db_session.rollback()
            flash(f'Failed to delete transaction: {str(e)}', 'error'), 500
            return redirect(url_for('inventory_entries'))


@app.route('/inventory_item/<int:item_id>', methods=['GET', 'POST'])
@login_required
def inventory_item_details(item_id):
    db_session = Session()
    app_id = current_user.app_id
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

    try:
        print(f"Fetching inventory item details for item_id: {item_id}, app_id: {app_id}")
        item = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()
        if not item:
            print(f"Inventory item not found for item_id: {item_id}")
            flash('Inventory item not found.', 'error')
            return redirect(url_for('inventory_entries'))

        if request.method == 'POST':
            print("Processing POST request to update inventory item")
            # Handle basic item info
            item_name = request.form['item_name']
            item_code = request.form['item_code']
            item_category_id = request.form.get('category')
            item_subcategory_id = request.form.get('subcategory')
            brand_id = request.form.get('brand_id') or None
            description = request.form.get('description') or None
            reorder_point = request.form['reorder_point']

            # Handle file upload
            file = request.files.get('item_image')
            if file and allowed_file(file.filename):
                if is_file_size_valid(file):
                    filename, file_extension = os.path.splitext(file.filename)
                    unique_filename = f"{app_id}_{uuid.uuid4()}{file_extension}"
                    image_filename = secure_filename(unique_filename)
                    file_path = os.path.join(app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)

                    if file_exists(file_path):
                        image_filename = generate_unique_filename(app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)
                        file_path = os.path.join(app.config['UPLOAD_FOLDER_INVENTORY'], image_filename)

                    try:
                        file.save(file_path)
                        logger.info(f"File saved successfully: {file_path}")
                    except Exception as e:
                        logger.error(f"Error saving file: {e}")
                        return jsonify({'success': False, 'message': 'Error saving file'}), 500
                else:
                    logger.warning("File size exceeds the maximum limit or invalid file type")
                    return jsonify(
                        {'success': False, 'message': 'File size exceeds the maximum limit or invalid file type'}), 400
            else:
                image_filename = item.image_filename if item and item.image_filename else None
                print("No new file uploaded, keeping existing image filename")

            # Update item details
            item.item_name = item_name
            item.item_code = item_code
            item.item_category_id = item_category_id
            item.item_subcategory_id = item_subcategory_id
            item.brand_id = brand_id
            item.item_description = description
            item.reorder_point = reorder_point
            item.image_filename = image_filename

            # Handle attributes and variations
            attribute_ids = request.form.getlist('attribute_ids[]')
            variation_ids = request.form.getlist('variation_ids[]')
            existing_pair_ids = request.form.getlist('existing_pair_ids[]')

            print(f"Processing attributes/variations - Received {len(attribute_ids)} pairs")

            # First handle updates to existing pairs
            existing_pairs = {pair.id: pair for pair in item.inventory_item_variation_link}
            print(f'existing pairs are {existing_pairs}')
            for pair_id, attribute_id, variation_id in zip(existing_pair_ids, attribute_ids, variation_ids):
                if pair_id == 'new':
                    # This is a new pair to be added
                    if attribute_id and variation_id:
                        new_pair = InventoryItemVariationLink(
                            app_id=app_id,
                            inventory_item_id=item.id,
                            attribute_id=attribute_id,
                            inventory_item_variation_id=variation_id
                        )
                        db_session.add(new_pair)
                        print(f"Added new attribute-variation pair: {attribute_id}-{variation_id}")
                else:
                    # This is an existing pair to be updated
                    pair = existing_pairs.get(int(pair_id))
                    if pair:
                        pair.attribute_id = attribute_id
                        pair.inventory_item_variation_id = variation_id
                        print(f"Updated existing pair {pair_id}: {attribute_id}-{variation_id}")
                    else:
                        print(f"Warning: Pair ID {pair_id} not found in existing pairs")

            # Handle deletions - find pairs that weren't included in the form submission
            submitted_pair_ids = {int(id) for id in existing_pair_ids if id != 'new'}
            for pair in item.inventory_item_variation_link:
                if pair.id not in submitted_pair_ids:
                    print(f"Removing pair {pair.id} as it wasn't in the submitted form")
                    db_session.delete(pair)

            db_session.commit()
            logger.info(f"Inventory item updated successfully: {item_id}")
            flash('Inventory item updated successfully!', 'success')
            return redirect(url_for('inventory_item_details', item_id=item.id, modules=modules_data))

        # Fetch categories, subcategories, attributes, variations etc.
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
        subcategories = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
        attributes = db_session.query(InventoryItemAttribute).filter_by(app_id=app_id).all()
        variations = db_session.query(InventoryItemVariation).filter_by(app_id=app_id).all()
        locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

        attribute_names = {attr.id: attr.attribute_name for attr in attributes}
        variation_names = {var.id: var.variation_name for var in variations}
        location_names = {loc.id: loc.location for loc in locations}

        # Query to get the total incoming and outgoing quantities
        entries_incoming = db_session.query(
            InventoryItemVariationLink.attribute_id,
            InventoryItemVariationLink.inventory_item_variation_id,
            InventoryEntry.to_location,
            func.sum(InventoryEntry.quantity).label('incoming_quantity')
        ).join(
            InventoryEntry, InventoryEntry.item_id == InventoryItemVariationLink.id
        ).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.inventory_item_id == item_id,
            InventoryEntry.stock_movement.in_(['in', 'transfer'])
        ).group_by(
            InventoryItemVariationLink.attribute_id,
            InventoryItemVariationLink.inventory_item_variation_id,
            InventoryEntry.to_location
        ).all()

        entries_outgoing = db_session.query(
            InventoryItemVariationLink.attribute_id,
            InventoryItemVariationLink.inventory_item_variation_id,
            InventoryEntry.from_location,
            func.sum(InventoryEntry.quantity).label('outgoing_quantity')
        ).join(
            InventoryEntry, InventoryEntry.item_id == InventoryItemVariationLink.id
        ).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.inventory_item_id == item_id,
            InventoryEntry.stock_movement.in_(['out', 'transfer', 'missing'])
        ).group_by(
            InventoryItemVariationLink.attribute_id,
            InventoryItemVariationLink.inventory_item_variation_id,
            InventoryEntry.from_location
        ).all()

        # Create a dictionary to track net quantities at each location
        grouped_entries = {}

        # Process incoming entries
        for entry in entries_incoming:
            attribute_name = attribute_names.get(entry.attribute_id)
            variation_name = variation_names.get(entry.inventory_item_variation_id)
            location_name = location_names.get(entry.to_location)

            if attribute_name not in grouped_entries:
                grouped_entries[attribute_name] = {}

            if variation_name not in grouped_entries[attribute_name]:
                grouped_entries[attribute_name][variation_name] = {}

            if location_name not in grouped_entries[attribute_name][variation_name]:
                grouped_entries[attribute_name][variation_name][location_name] = 0

            if entry.incoming_quantity is not None:
                grouped_entries[attribute_name][variation_name][location_name] += entry.incoming_quantity

        # Process outgoing entries
        for entry in entries_outgoing:
            attribute_name = attribute_names.get(entry.attribute_id)
            variation_name = variation_names.get(entry.inventory_item_variation_id)
            location_name = location_names.get(entry.from_location)

            if attribute_name in grouped_entries and variation_name in grouped_entries[attribute_name]:
                if location_name in grouped_entries[attribute_name][variation_name]:
                    if entry.outgoing_quantity is not None:
                        grouped_entries[attribute_name][variation_name][location_name] -= entry.outgoing_quantity

        print("Rendering inventory item details template")
        return render_template(
            'inventory_item_details.html',
            item=item,
            item_category_name=item.inventory_category.category_name if item.inventory_category else None,
            item_subcategory_name=item.inventory_subcategory.subcategory_name if item.inventory_subcategory else None,
            categories=categories,
            subcategories=subcategories,
            modules=modules_data,
            grouped_entries=grouped_entries,
            inventory_attributes=attributes,
            inventory_variations=variations,
            company=company,
            role=role,
            module_name="Inventory"
        )

    except Exception as e:
        logger.error(f"Error in inventory_item_details route: {e}")
        flash('An error occurred while processing your request.', 'error')
        return redirect(url_for('inventory_entries'))
    finally:
        db_session.close()


@app.route('/uploads/inventory/<path:filename>')
@login_required
def serve_inventory_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER_INVENTORY'], filename, environ=request.environ)


@app.route('/inventory_dashboard')
@login_required
def inventory_dashboard():
    db_session = Session()

    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role

        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Call the utility function to check if exchange rates are required
        show_exchange_rate_modal, base_currency, currencies = check_exchange_rate_required(app_id, db_session)

        # Fetch necessary data
        categories_data = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()
        subcategories_data = db_session.query(InventorySubCategory).filter_by(app_id=app_id).all()
        locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
        items = db_session.query(InventoryItem).filter_by(app_id=app_id, status="active").all()
        variations = db_session.query(InventoryItemVariation).filter_by(app_id=app_id).all()
        attributes = db_session.query(InventoryItemAttribute).filter_by(app_id=app_id).all()
        brands = db_session.query(Brand).filter_by(app_id=app_id).all()

        # FIFO-BASED STOCK VALUATION
        base_currency_id = db_session.query(Currency.id).filter_by(user_currency=base_currency, app_id=app_id).scalar()
        total_stock_value = Decimal('0.0')

        # Get current stock quantities for all items
        current_stock = db_session.query(
            InventoryItem.id,
            InventoryItem.item_name,
            func.coalesce(
                func.sum(
                    case(
                        (InventoryEntry.stock_movement == 'in', InventoryEntry.quantity),
                        (InventoryEntry.stock_movement.in_(['out', 'missing']), -InventoryEntry.quantity),
                        else_=0
                    )
                ), 0
            ).label('current_qty')
        ).join(
            InventoryItemVariationLink, InventoryItemVariationLink.inventory_item_id == InventoryItem.id
        ).join(
            InventoryEntry, InventoryEntry.item_id == InventoryItemVariationLink.id, isouter=True
        ).filter(
            InventoryItem.app_id == app_id,
            InventoryItem.status == 'active'
        ).group_by(
            InventoryItem.id,
            InventoryItem.item_name
        ).all()

        # Calculate FIFO value for each item's current stock
        for item in current_stock:
            if item.current_qty <= 0:
                continue

            # Get all inventory batches for this item (oldest first)
            batches = db_session.query(
                BatchVariationLink.id,
                BatchVariationLink.quantity,
                BatchVariationLink.unit_cost,
                BatchVariationLink.currency_id,
                BatchVariationLink.transaction_date
            ).join(
                InventoryItemVariationLink, BatchVariationLink.item_id == InventoryItemVariationLink.id
            ).join(
                InventoryItem, InventoryItemVariationLink.inventory_item_id == InventoryItem.id
            ).filter(
                InventoryItemVariationLink.inventory_item_id == item.id,
                BatchVariationLink.quantity > 0,
                BatchVariationLink.app_id == app_id
            ).order_by(
                BatchVariationLink.transaction_date.asc()
            ).all()

            remaining_qty = Decimal(item.current_qty)
            item_value = Decimal('0.0')

            for batch in batches:
                if remaining_qty <= 0:
                    break

                qty_to_use = min(remaining_qty, Decimal(str(batch.quantity)))

                # Convert to base currency if needed
                if batch.currency_id == base_currency_id:
                    cost = Decimal(str(batch.unit_cost))
                else:
                    rate = get_exchange_rate(db_session, batch.currency_id, base_currency_id, app_id)
                    if not rate:
                        print(
                            f"Exchange rate not found for currency_id: {batch.currency_id} to base_currency_id: {base_currency_id}")
                        continue
                    cost = Decimal(str(batch.unit_cost)) * Decimal(str(rate))

                item_value += qty_to_use * cost
                remaining_qty -= qty_to_use

            total_stock_value += item_value

        # Calculate the total number of active products
        total_number_of_products_active = db_session.query(func.count(InventoryItem.id)).filter_by(
            app_id=app_id,
            status='active'
        ).scalar()

        total_number_of_products_discontinued = db_session.query(func.count(InventoryItem.id)).filter_by(
            app_id=app_id,
            status='discontinued'
        ).scalar()

        # Category/Subcategory Breakdown
        entries = db_session.query(
            InventoryCategory.category_name,
            InventorySubCategory.subcategory_name,
            InventoryEntry.quantity,
            InventoryEntry.stock_movement
        ).join(
            InventoryItemVariationLink, InventoryEntry.item_id == InventoryItemVariationLink.id
        ).join(
            InventoryItem, InventoryItemVariationLink.inventory_item_id == InventoryItem.id
        ).join(
            InventoryCategory, InventoryItem.item_category_id == InventoryCategory.id
        ).join(
            InventorySubCategory, InventoryItem.item_subcategory_id == InventorySubCategory.id
        ).filter(
            InventoryEntry.app_id == app_id,
            InventoryItem.status == 'active'
        ).all()

        # Initialize dictionary to hold totals
        totals = defaultdict(lambda: defaultdict(float))

        # Process entries
        for entry in entries:
            category = entry.category_name
            subcategory = entry.subcategory_name
            quantity = entry.quantity
            movement = entry.stock_movement

            if movement == 'in':
                totals[category][subcategory] += quantity
            elif movement != 'in':
                totals[category][subcategory] -= quantity

        # Convert to a list of tuples for output
        category_breakdown = [
            (category, subcategory, total)
            for category, subcategories in totals.items()
            for subcategory, total in subcategories.items()
        ]

        # Prepare data for the chart
        categories = [f"{cat} - {subcat}" for cat, subcat, _ in category_breakdown]
        quantities = [float(qty) for _, _, qty in category_breakdown]

        # Stock Movement Over Time
        def get_stock_movement(movement_type):

            return db_session.query(
                func.strftime('%Y-%m', InventoryEntry.transaction_date).label('month'),
                func.sum(InventoryEntry.quantity).label(movement_type)
            ).filter(
                InventoryEntry.app_id == app_id,
                InventoryEntry.stock_movement == movement_type
            ).group_by(
                func.strftime('%Y-%m', InventoryEntry.transaction_date)
            ).order_by(
                func.strftime('%Y', InventoryEntry.transaction_date),
                func.strftime('%m', InventoryEntry.transaction_date)
            ).all()

        stock_movement_in = get_stock_movement('in')
        stock_movement_out = get_stock_movement('out')

        # Prepare data for the chart (In)
        months_in = [month for month, _ in stock_movement_in]
        movement_quantities_in = [qty for _, qty in stock_movement_in]

        # Prepare data for the chart (Out)
        months_out = [month for month, _ in stock_movement_out]
        movement_quantities_out = [qty for _, qty in stock_movement_out]

        # Low Stock Alerts
        low_stock_items = db_session.query(
            InventoryItem.item_name,
            func.coalesce(
                func.sum(
                    case(
                        (InventoryEntry.stock_movement == 'in', InventoryEntry.quantity),
                        (InventoryEntry.stock_movement.in_(['out', 'missing']), -InventoryEntry.quantity),
                        else_=0
                    )
                ), 0
            ).label('current_qty'),
            InventoryItem.reorder_point
        ).join(
            InventoryItemVariationLink, InventoryItemVariationLink.inventory_item_id == InventoryItem.id
        ).join(
            InventoryEntry, InventoryEntry.item_id == InventoryItemVariationLink.id, isouter=True
        ).filter(
            InventoryItem.app_id == app_id,
            InventoryItem.status == 'active'
        ).group_by(
            InventoryItem.id,
            InventoryItem.item_name,
            InventoryItem.reorder_point
        ).having(
            func.coalesce(
                func.sum(
                    case(
                        (InventoryEntry.stock_movement == 'in', InventoryEntry.quantity),
                        (InventoryEntry.stock_movement.in_(['out', 'missing']), -InventoryEntry.quantity),
                        else_=0
                    )
                ), 0
            ) < InventoryItem.reorder_point
        ).all()

        # Convert data to simple types
        low_stock_items = [(item_name, float(quantity), reorder_point)
                           for item_name, quantity, reorder_point in low_stock_items]

        return render_template(
            'inventory_dashboard.html',
            net_value_by_currency=total_stock_value,
            total_number_of_products_active=total_number_of_products_active,
            total_number_of_products_discontinued=total_number_of_products_discontinued,
            categories=categories,
            quantities=quantities,
            months_in=months_in,
            movement_quantities_in=movement_quantities_in,
            months_out=months_out,
            movement_quantities_out=movement_quantities_out,
            low_stock_items=low_stock_items,
            categories_data=categories_data,
            subcategories_data=subcategories_data,
            locations=locations,
            items=items,
            variations=variations,
            attributes=attributes,
            brands=brands,
            show_exchange_rate_modal=show_exchange_rate_modal,
            currencies=currencies,
            base_currency=base_currency,
            modules=modules_data,
            company=company,
            role=role,
            module_name="Inventory"
        )

    finally:
        db_session.close()


@app.route('/missing_item', methods=['POST'])
@login_required
def missing_item():
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Step 1: Retrieve and validate form data
        item_id = request.form.get('item_id')
        variation = request.form.get('variation') or None
        batch = request.form.get('batch') or None
        quantity = request.form.get('quantity')
        from_location = request.form.get('from_location')
        print(f'data received is {request.form}')
        # Validate inputs
        if not item_id or not quantity or not from_location:
            return jsonify({'status': 'error', 'message': 'Missing required fields'}), 400

        try:
            quantity = float(quantity)
            if quantity <= 0:
                return jsonify({'status': 'error', 'message': 'Quantity must be positive'}), 400
        except ValueError:
            return jsonify({'status': 'error', 'message': 'Invalid quantity'}), 400

        # Get the variation link
        inventory_item_variation = db_session.query(InventoryItemVariationLink).filter_by(
            inventory_item_id=item_id,
            inventory_item_variation_id=variation,
            app_id=app_id
        ).first()

        if not inventory_item_variation:
            return jsonify({'status': 'error', 'message': 'Item variation not found'}), 404

        variation_link_id = inventory_item_variation.id

        # Step 2: Check available quantity
        available_quantity = calculate_available_quantity(db_session, app_id, variation_link_id, from_location)

        if Decimal(str(available_quantity)) < Decimal(str(quantity)):
            return jsonify({
                'status': 'error',
                'message': f'Insufficient quantity. Available: {available_quantity}, Requested: {quantity}'
            }), 400

        # Step 3: Handle batch-specific or FIFO logic
        if batch:
            # Batch-specific processing
            batch_variation = db_session.query(BatchVariationLink).filter_by(
                batch_id=batch,
                location_id=from_location,
                item_id=variation_link_id,
                app_id=app_id
            ).first()

            if not batch_variation or batch_variation.quantity < quantity:
                return jsonify({
                    'status': 'error',
                    'message': 'Insufficient quantity in specified batch'
                }), 400

            # Create inventory entry
            new_entry = InventoryEntry(
                app_id=app_id,
                item_id=variation_link_id,
                transaction_date=datetime.datetime.now(),
                date_added=datetime.datetime.now(),
                from_location=from_location,
                quantity=quantity,
                stock_movement="missing",
                uom=batch_variation.uom,
                currency_id=batch_variation.currency_id,
                unit_price=batch_variation.unit_cost,
                lot=batch_variation.batch_id,
                supplier_id=batch_variation.supplier_id,
                created_by=current_user.id,
                is_posted_to_ledger=False
            )

            db_session.add(new_entry)
            batch_variation.quantity -= quantity
            db_session.commit()

            return jsonify({
                'status': 'success',
                'message': f'Marked {quantity} items as missing from batch {batch}'
            })

        else:
            # FIFO processing
            remaining_qty = quantity
            batches = db_session.query(BatchVariationLink).filter(
                BatchVariationLink.item_id == variation_link_id,
                BatchVariationLink.location_id == from_location,
                BatchVariationLink.quantity > 0,
                BatchVariationLink.app_id == app_id
            ).order_by(
                BatchVariationLink.transaction_date.asc()
            ).all()

            if not batches:
                return jsonify({
                    'status': 'error',
                    'message': 'No available batches found'
                }), 400

            for batch_variation in batches:
                if remaining_qty <= 0:
                    break

                qty_to_deduct = min(remaining_qty, batch_variation.quantity)

                # Create inventory entry
                new_entry = InventoryEntry(
                    app_id=app_id,
                    item_id=variation_link_id,
                    transaction_date=datetime.datetime.now(),
                    date_added=datetime.datetime.now(),
                    from_location=from_location,
                    quantity=qty_to_deduct,
                    stock_movement="missing",
                    uom=batch_variation.uom,
                    currency_id=batch_variation.currency_id,
                    unit_price=batch_variation.unit_cost,
                    lot=batch_variation.batch_id,
                    supplier_id=batch_variation.supplier_id,
                    created_by=current_user.id,
                    is_posted_to_ledger=False
                )

                db_session.add(new_entry)
                batch_variation.quantity -= qty_to_deduct
                remaining_qty -= qty_to_deduct

            if remaining_qty > 0:
                db_session.rollback()
                return jsonify({
                    'status': 'error',
                    'message': f'Could only mark {quantity - remaining_qty} of {quantity} as missing'
                }), 400

            db_session.commit()
            notification_message = "Please make journal entries for the missing items"
            create_notification(
                db_session=db_session,
                company_id=app_id,  # Notify the company
                message=notification_message,
                type='info',  # Notification type
                is_popup=True,  # Show as a popup
                url=f"/inventory/inventory_entries"  # Link to the quotation
            )
            return jsonify({
                'status': 'success',
                'message': f'Successfully marked {quantity} items as missing using FIFO'
            })

    except Exception as e:
        db_session.rollback()
        print(f'Error: {str(e)}')
        return jsonify({
            'status': 'error',
            'message': f'An error occurred: {str(e)}'
        }), 500

    finally:
        db_session.close()


@app.route('/discontinue_item', methods=['POST'])
@login_required
def discontinue_item():
    db_session = Session()
    try:
        data = request.get_json()
        item_id = data.get('item_id')
        app_id = current_user.app_id

        # Validate item_id
        if not item_id:
            return jsonify({'success': False, 'error': 'Item ID is required.'}), 400

        # Fetch the item from the database
        item = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()
        if not item:
            return jsonify({'success': False, 'error': 'Item not found.'}), 404

        # Check if the item is already discontinued
        if item.status == 'discontinued':
            return jsonify({'success': False, 'error': 'Item is already discontinued.'}), 400

        # Check if the item has 0 quantities in all locations
        # Calculate net quantity across all locations
        net_quantity = calculate_net_quantity(item_id, app_id, db_session)

        # Check if the item has 0 quantities across all locations
        if net_quantity > 0:
            return jsonify({'success': False,
                            'error': f'Item cannot be discontinued. {net_quantity} Quantities still available across all locations.'}), 400

        # Update the item status to "discontinued"
        item.status = 'discontinued'
        db_session.commit()

        return jsonify({'success': True, 'message': 'Item discontinued successfully.'})

    except Exception as e:
        db_session.rollback()
        return jsonify({'success': False, 'error': str(e)}), 500

    finally:
        db_session.close()


@app.route('/stock_list_grid')
@login_required
def stock_list_grid():
    db_session = Session()
    try:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Create case expressions for stock movements
        stock_in_case = case(
            (InventoryEntry.stock_movement.in_(['in', 'transfer']), InventoryEntry.quantity),
            else_=0
        )

        stock_out_case = case(
            (InventoryEntry.stock_movement != 'in', InventoryEntry.quantity),
            else_=0
        )

        # Query to get stock values grouped by category
        results = db_session.query(
            InventoryItem.id,
            InventoryItem.item_name,
            InventoryItem.image_filename,
            InventoryCategory.category_name,
            InventorySubCategory.subcategory_name,
            (func.sum(stock_in_case) - func.sum(stock_out_case)).label('total_quantity')
        ).join(
            InventoryCategory, InventoryItem.item_category_id == InventoryCategory.id
        ).join(
            InventorySubCategory, InventoryItem.item_subcategory_id == InventorySubCategory.id
        ).join(
            InventoryItemVariationLink, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).join(
            InventoryEntry, InventoryEntry.item_id == InventoryItemVariationLink.id
        ).filter(
            InventoryItem.app_id == app_id,  # Filter by app_id
            InventoryItem.status == 'active'  # Ensure item status is 'active'
        ).group_by(
            InventoryItem.id,
            InventoryCategory.category_name,
            InventorySubCategory.subcategory_name
        ).all()

        # Print results for debugging
        for item in results:
            print(
                f"ItemID: {item.id} Item: {item.item_name}, Quantity: {item.total_quantity}, Image name: {item.image_filename}")

        # Organize data by category
        stock_by_category = {}
        for item in results:
            category = item.category_name
            if category not in stock_by_category:
                stock_by_category[category] = []

            # Determine stock status based on quantity
            status = 'In Stock'
            if item.total_quantity <= 0:
                status = 'Out of Stock'
            elif item.total_quantity <= 5:  # You can adjust this threshold
                status = 'Low Stock'

            stock_by_category[category].append({
                'id': item.id,
                'item_name': item.item_name,
                'quantity': item.total_quantity,
                'image_filename': item.image_filename,
                'subcategory_name': item.subcategory_name,
                'status': status  # Added status for potential UI display
            })

        return render_template(
            'stock_list_grid.html',
            stock_by_category=stock_by_category,
            modules=modules_data,
            company=company,
            role=role,
            module_name="Inventory"
        )

    except Exception as e:
        # Log the error and show a flash message to the user
        app.logger.error(f"Error retrieving stock list: {e}")
        flash("An error occurred while retrieving the stock list. Please try again later.", "error")
        return redirect(url_for('dashboard'))  # Redirect to a safe page

    finally:
        # Ensure the database session is closed, even if an error occurs
        db_session.close()


@app.route('/stock_list_list')
@login_required
def stock_list_list():
    db_session = Session()
    app_id = current_user.app_id

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        # Fetch all modules for the app
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Check if batch grouping is requested
        group_by_batch = request.args.get('group_by_batch', default=False, type=lambda x: x.lower() == 'true')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        filter_applied = bool(start_date or end_date)

        # Base query with common joins
        query = db_session.query(
            InventoryItem.id,
            InventoryItem.item_name,
            InventoryItem.image_filename,
            InventoryCategory.category_name,
            InventorySubCategory.subcategory_name,
            Brand.name.label('brand'),
            InventoryItemAttribute.attribute_name.label('attribute'),
            InventoryItemVariation.variation_name.label('variation'),
            InventoryItemVariation.id.label('variation_id'),
            InventoryLocation.location.label('location'),
            InventoryLocation.id.label('location_id'),
            InventoryItem.reorder_point,
            (func.coalesce(func.sum(
                case(
                    (InventoryEntry.to_location == InventoryLocation.id, InventoryEntry.quantity),
                    else_=0
                )
            ), 0) - func.coalesce(func.sum(
                case(
                    (InventoryEntry.from_location == InventoryLocation.id, InventoryEntry.quantity),
                    else_=0
                )
            ), 0)).label('total_quantity'),
            func.coalesce(func.sum(
                case(
                    (InventoryEntry.stock_movement == 'missing', InventoryEntry.quantity),
                    else_=0
                )
            ), 0).label('missing_items')
        ).join(
            InventoryCategory, InventoryItem.item_category_id == InventoryCategory.id
        ).join(
            InventorySubCategory, InventoryItem.item_subcategory_id == InventorySubCategory.id
        ).outerjoin(
            InventoryItemVariationLink, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).outerjoin(
            InventoryEntry, InventoryEntry.item_id == InventoryItemVariationLink.id
        ).outerjoin(
            Brand, InventoryItem.brand_id == Brand.id
        ).outerjoin(
            InventoryItemAttribute, InventoryItemVariationLink.attribute_id == InventoryItemAttribute.id
        ).outerjoin(
            InventoryItemVariation, InventoryItemVariationLink.inventory_item_variation_id == InventoryItemVariation.id
        ).outerjoin(
            InventoryLocation, or_(
                and_(InventoryEntry.to_location == InventoryLocation.id, InventoryEntry.to_location.isnot(None)),
                and_(InventoryEntry.from_location == InventoryLocation.id, InventoryEntry.from_location.isnot(None))
            )
        ).filter(
            InventoryItem.app_id == app_id,
            InventoryItem.status == 'active',
            or_(
                InventoryEntry.to_location.isnot(None),
                InventoryEntry.from_location.isnot(None)
            )
        ).group_by(
            InventoryItem.id,
            InventoryItem.item_name,
            InventoryItem.image_filename,
            InventoryCategory.category_name,
            InventorySubCategory.subcategory_name,
            Brand.name,
            InventoryItemAttribute.attribute_name,
            InventoryItemVariation.variation_name,
            InventoryItemVariation.id,
            InventoryLocation.location,
            InventoryLocation.id,
            InventoryItem.reorder_point
        )
        # Add batch-related fields and joins only if group_by_batch is True
        if group_by_batch:
            query = query.add_columns(
                Batch.batch_number.label('batch'),
                Batch.id.label('batch_id')
            ).outerjoin(
                Batch, InventoryEntry.lot == Batch.id
            )

        # Group by batch if requested
        if group_by_batch:
            query = query.group_by(
                InventoryItem.id,
                InventoryCategory.id,
                InventorySubCategory.id,
                Brand.id,
                InventoryItemAttribute.id,
                InventoryItemVariation.id,
                InventoryLocation.id,
                Batch.id
            )
        else:
            query = query.group_by(
                InventoryItem.id,
                InventoryCategory.id,
                InventorySubCategory.id,
                Brand.id,
                InventoryItemAttribute.id,
                InventoryItemVariation.id,
                InventoryLocation.id
            )

        # Apply date filters using the apply_date_filters function
        query = apply_date_filters(query, start_date, end_date, "transaction_date", InventoryEntry, 'transaction_date',
                                   'date_added')

        results = query.all()

        # Process data for rendering
        stock_by_category = {}
        for item in results:
            category = item.category_name

            # Determine stock status
            if item.total_quantity == 0:
                status = 'Out of Stock'
            elif 0 < item.total_quantity <= item.reorder_point + 2:
                status = 'Low Stock'
            else:
                status = 'In Stock'

            if category not in stock_by_category:
                stock_by_category[category] = []

            stock_data = {
                'id': item.id,
                'item_name': item.item_name,
                'quantity': item.total_quantity,
                'image_filename': item.image_filename,
                'subcategory_name': item.subcategory_name,
                'brand': item.brand,
                'attribute': item.attribute,
                'variation': item.variation,
                'variation_id': item.variation_id,
                'location': item.location,
                'missing_items': item.missing_items,
                'status': status
            }

            if group_by_batch:
                stock_data.update({
                    'batch': item.batch,
                    'batch_id': item.batch_id
                })

            stock_by_category[category].append(stock_data)

        return render_template(
            'stock_list_list.html',
            stock_by_category=stock_by_category,
            modules=modules_data,
            group_by_batch=group_by_batch,
            company=company,
            role=role,
            filter_applied=filter_applied,
            module_name="Inventory"
        )

    except Exception as e:
        print(f'Error fetching stock list: {e}')
        return "An error occurred while fetching the stock list."

    finally:
        db_session.close()


@app.route('/warehouse_management', methods=['GET', 'POST'])
@login_required
def warehouse_management():
    db_session = Session()
    app_id = current_user.app_id

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        # Fetch all modules for the app
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch all warehouses (locations) for the app
        locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

        # Check if no locations are found
        if not locations:
            location_name = 'Warehouse'
            description = 'Default generic location created automatically'

            existing_location = db_session.query(InventoryLocation).filter_by(
                app_id=app_id,
                location=location_name
            ).first()

            if not existing_location:
                new_location = InventoryLocation(
                    app_id=app_id,
                    location=location_name,
                    description=description
                )
                try:
                    db_session.add(new_location)
                    db_session.commit()
                    locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
                except Exception as e:
                    db_session.rollback()
                    return jsonify({'success': False, 'error': str(e)}), 500

        selected_location_id = request.args.get('location_id', type=int)
        selected_location = None

        if not selected_location_id and locations:
            selected_location_id = locations[0].id
            selected_location = db_session.query(InventoryLocation).get(selected_location_id)
        elif selected_location_id:
            selected_location = db_session.query(InventoryLocation).get(selected_location_id)

        group_by_batch = request.args.get('group_by_batch', default=False, type=lambda x: x.lower() == 'true')

        # Base query components
        query = db_session.query(
            InventoryItem.id,
            InventoryItem.item_name,
            InventoryItem.image_filename,
            InventoryCategory.category_name,
            InventorySubCategory.subcategory_name,
            Brand.name.label('brand'),
            InventoryItemAttribute.attribute_name.label('attribute'),
            InventoryItemVariation.variation_name.label('variation'),
            InventoryItemVariation.id.label('variation_id'),
            InventoryItem.reorder_point,
            (func.coalesce(func.sum(
                case(
                    (InventoryEntry.to_location == selected_location_id, InventoryEntry.quantity)
                )
            ), 0) - func.coalesce(func.sum(
                case(
                    (InventoryEntry.from_location == selected_location_id, InventoryEntry.quantity)
                )
            ), 0)).label('total_quantity'),
            func.coalesce(func.sum(
                case(
                    (
                        (InventoryEntry.stock_movement == 'missing') &
                        (InventoryEntry.from_location == selected_location_id),
                        InventoryEntry.quantity
                    )
                )
            ), 0).label('missing_items')
        ).join(
            InventoryCategory, InventoryItem.item_category_id == InventoryCategory.id
        ).join(
            InventorySubCategory, InventoryItem.item_subcategory_id == InventorySubCategory.id
        ).join(
            InventoryItemVariationLink, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).join(
            InventoryEntry, InventoryEntry.item_id == InventoryItemVariationLink.id
        ).outerjoin(
            Brand, InventoryItem.brand_id == Brand.id
        ).outerjoin(
            InventoryItemAttribute, InventoryItemVariationLink.attribute_id == InventoryItemAttribute.id
        ).outerjoin(
            InventoryItemVariation, InventoryItemVariationLink.inventory_item_variation_id == InventoryItemVariation.id
        ).filter(
            InventoryItem.app_id == app_id,
            InventoryItem.status == 'active'
        )

        # Add batch fields if grouping by batch
        if group_by_batch:
            query = query.add_columns(
                Batch.batch_number.label('batch'),
                Batch.id.label('batch_id')
            ).outerjoin(
                Batch, InventoryEntry.lot == Batch.id
            ).group_by(
                InventoryItem.id,
                InventoryCategory.id,
                InventorySubCategory.id,
                Brand.id,
                InventoryItemAttribute.id,
                InventoryItemVariation.id,
                Batch.id
            )
        else:
            query = query.group_by(
                InventoryItem.id,
                InventoryCategory.id,
                InventorySubCategory.id,
                Brand.id,
                InventoryItemAttribute.id,
                InventoryItemVariation.id
            )

        results = query.all()

        # Process data for rendering
        stock_by_category = {}
        total_items = 0
        total_in_stock_items = 0
        total_missing_items = 0
        total_low_stock_items = 0

        for item in results:
            category = item.category_name
            status = 'Out of Stock' if item.total_quantity == 0 else \
                'Low Stock' if 0 < item.total_quantity <= item.reorder_point + 2 else \
                    'In Stock'

            if category not in stock_by_category:
                stock_by_category[category] = []

            item_data = {
                'id': item.id,
                'item_name': item.item_name,
                'quantity': item.total_quantity,
                'image_filename': item.image_filename,
                'subcategory_name': item.subcategory_name,
                'brand': item.brand,
                'attribute': item.attribute,
                'variation': item.variation,
                'variation_id': item.variation_id if item.variation_id is not None else '',
                'missing_items': item.missing_items,
                'status': status
            }

            if group_by_batch:
                item_data.update({
                    'batch': item.batch,
                    'batch_id': item.batch_id
                })

            stock_by_category[category].append(item_data)

            # Update metrics
            total_items += 1
            if status == 'In Stock':
                total_in_stock_items += 1
            elif status == 'Low Stock':
                total_low_stock_items += 1

        # Calculate total warehouses and missing items
        total_warehouses = db_session.query(func.count(InventoryLocation.id)) \
            .filter(InventoryLocation.app_id == app_id) \
            .scalar()

        total_missing_items = db_session.query(
            func.count(distinct(InventoryItemVariationLink.inventory_item_id))
        ).join(
            InventoryEntry, InventoryEntry.item_id == InventoryItemVariationLink.id
        ).filter(
            InventoryEntry.stock_movement == "missing",
            InventoryEntry.app_id == app_id,
            InventoryEntry.from_location == selected_location_id
        ).scalar()

        return render_template(
            'warehouse_management.html',
            stock_by_category=stock_by_category,
            locations=locations,
            selected_location_id=selected_location_id,
            modules=modules_data,
            selected_location_name=selected_location.location if selected_location else '',
            group_by_batch=group_by_batch,
            total_items=total_items,
            total_in_stock_items=total_in_stock_items,
            total_missing_items=total_missing_items,
            total_low_stock_items=total_low_stock_items,
            total_warehouses=total_warehouses,
            company=company,
            role=role,
            module_name="Inventory"
        )

    except Exception as e:
        print(f'Error fetching warehouse data: {e}')
        flash("An error occurred while fetching warehouse data.", "error")
        return redirect(url_for('dashboard'))

    finally:
        db_session.close()


@app.route('/edit_warehouse', methods=['POST'])
@login_required
def edit_warehouse():
    db_session = Session()
    try:
        location_id = request.form.get('location_id')
        new_location_name = request.form.get('new_location_name')

        # Fetch the location from the database
        location = db_session.query(InventoryLocation).filter_by(id=location_id).first()
        if location:
            # Update the location name
            location.location = new_location_name
            db_session.commit()
            return jsonify({"message": "Warehouse name updated successfully!"}), 200
        else:
            return jsonify({"error": "Warehouse not found."}), 404

    except Exception as e:
        db_session.rollback()
        print(f'Error updating warehouse name: {e}')
        return jsonify({"error": "An error occurred while updating the warehouse name."}), 500

    finally:
        db_session.close()


@app.route('/transfer_item', methods=['POST'])
@login_required
def transfer_item():
    db_session = Session()
    app_id = current_user.app_id  # Assuming you have a current_user object

    try:
        # Step 1: Retrieve and validate form data
        item_id = request.form.get('item_id')
        variation = request.form.get('variation')
        batch = request.form.get('batch')
        quantity = request.form.get('quantity')
        to_location = request.form.get('to_location')
        from_location = request.form.get('from_location')

        # Debug: Print all received data
        print(f"Item ID: {item_id}")
        print(f"Variation: {variation}")
        print(f"Batch: {batch}")
        print(f"Quantity: {quantity}")
        print(f"To Location: {to_location}")
        print(f"From Location: {from_location}")

        # Validate inputs
        if not item_id or not to_location or not quantity or not from_location:
            error_message = 'Missing required fields: item_id, to_location, quantity, or from_location'
            return jsonify({'status': 'error', 'message': error_message}), 400

        try:
            quantity = int(quantity)
        except ValueError:
            error_message = 'Quantity must be a valid integer'
            return jsonify({'status': 'error', 'message': error_message}), 400

        if quantity <= 0:
            error_message = 'Quantity must be a positive number'
            return jsonify({'status': 'error', 'message': error_message}), 400

        inventory_item_variation = db_session.query(InventoryItemVariationLink).filter_by(inventory_item_id=item_id,
                                                                                          inventory_item_variation_id=variation,
                                                                                          app_id=app_id).first()
        item_id = inventory_item_variation.id

        # Step 2: Check if the total available quantity is sufficient
        available_quantity = calculate_available_quantity(db_session, app_id, item_id, from_location)
        if available_quantity < quantity:
            error_message = f'Insufficient quantity for item in location ID: {from_location}. Available: {available_quantity}, Requested: {quantity}'
            return jsonify({'status': 'error', 'message': error_message}), 400

        # Step 3: Handle batch-specific transfer or FIFO logic
        if batch:
            # Fetch the latest entry for the specified batch
            latest_item_entry = db_session.query(InventoryEntry).filter(
                InventoryEntry.app_id == app_id,
                InventoryEntry.item_id == item_id,
                InventoryEntry.to_location == from_location,
                InventoryEntry.lot == batch  # Filter by batch
            ).order_by(InventoryEntry.transaction_date.desc()).first()

            if not latest_item_entry:
                error_message = f'No entry found for item  in location'
                return jsonify({'status': 'error', 'message': error_message}), 404

            # Create a new InventoryEntry for the transfer
            new_entry = InventoryEntry(
                app_id=app_id,
                item_id=item_id,
                date_added=datetime.datetime.now(),
                transaction_date=datetime.datetime.now(),
                currency_id=latest_item_entry.currency_id,
                uom=latest_item_entry.uom,
                unit_price=latest_item_entry.unit_price,
                expiration_date=latest_item_entry.expiration_date,
                selling_price=latest_item_entry.selling_price,
                quantity=quantity,
                stock_movement="transfer",
                supplier_id=latest_item_entry.supplier_id,
                lot=batch,
                to_location=to_location,
                from_location=from_location,
                created_by=current_user.id
            )

            db_session.add(new_entry)
            db_session.commit()

            success_message = f"Item successfully transferred."
            return jsonify({'status': 'success', 'message': success_message}), 200

        else:
            # Fetch all batches for the item in the from_location, ordered by the earliest transaction date
            batches = db_session.query(
                InventoryEntry.lot,
                func.min(InventoryEntry.transaction_date).label('earliest_date')
            ).filter(
                InventoryEntry.app_id == app_id,
                InventoryEntry.item_id == item_id,
                InventoryEntry.to_location == from_location
            ).group_by(
                InventoryEntry.lot
            ).order_by(
                'earliest_date'  # Order by the earliest transaction date
            ).all()

            remaining_quantity = quantity

            print(f'remaining qty is {remaining_quantity}')

            # Iterate over batches and transfer the requested quantity
            for batch in batches:
                if remaining_quantity <= 0:
                    break

                # Calculate the total available quantity for this batch
                batch_available_quantity = calculate_batch_available_quantity(db_session, app_id, item_id, variation,
                                                                              from_location, batch.lot)
                print(f'batch_available_quantity is {batch_available_quantity}')
                # If the batch's available quantity is zero, skip to the next batch
                if batch_available_quantity <= 0:
                    continue

                # Determine the quantity to transfer from this batch
                transfer_quantity = min(remaining_quantity, batch_available_quantity)

                # Fetch the first entry for this batch to use its details
                first_entry = db_session.query(InventoryEntry).filter(
                    InventoryEntry.app_id == app_id,
                    InventoryEntry.item_id == item_id,
                    InventoryEntry.to_location == from_location,
                    InventoryEntry.lot == batch.lot  # Filter by the current batch
                ).order_by(InventoryEntry.transaction_date.asc()).first()

                if not first_entry:
                    continue  # Skip if no entries are found (should not happen)

                # Create a new InventoryEntry for the transfer
                new_entry = InventoryEntry(
                    app_id=app_id,
                    item_id=item_id,
                    date_added=datetime.datetime.now(),
                    transaction_date=datetime.datetime.now(),
                    currency_id=first_entry.currency_id,
                    uom=first_entry.uom,
                    unit_price=first_entry.unit_price,
                    expiration_date=first_entry.expiration_date,
                    selling_price=first_entry.selling_price,
                    quantity=transfer_quantity,
                    stock_movement="transfer",
                    supplier_id=first_entry.supplier_id,
                    lot=batch.lot,
                    to_location=to_location,
                    from_location=from_location,
                    created_by=current_user.id
                )

                db_session.add(new_entry)
                db_session.commit()

                # Update the remaining quantity
                remaining_quantity -= transfer_quantity

            if remaining_quantity > 0:
                error_message = f'Insufficient quantity for item in location. Could only transfer {quantity - remaining_quantity} out of {quantity}.'
                return jsonify({'status': 'error', 'message': error_message}), 400

            success_message = f"Item successfully transferred from using FIFO."
            return jsonify({'status': 'success', 'message': success_message}), 200

    except Exception as e:
        db_session.rollback()
        error_message = f'An error occurred while transferring the item: {str(e)}'
        print(f'error is {error_message}')
        return jsonify({'status': 'error', 'message': error_message}), 500

    finally:
        db_session.close()


@app.route('/get_vendors', methods=['GET'])
@login_required
def get_vendors():
    # Ensure user is logged in and has the necessary app_id
    app_id = current_user.app_id

    # Start a new database session
    db_session = Session()

    try:
        # Query the vendors from the database (you can filter based on app_id or other criteria)
        vendors = db_session.query(Vendor).filter_by(app_id=app_id).all()

        # Prepare the list of vendor data to send back
        vendor_data = [
            {"id": vendor.id, "name": vendor.vendor_name} for vendor in vendors
        ]
        print(f'vendor data is {vendor_data}')

        # Return the list of vendors as a JSON response
        return jsonify(vendor_data)

    except Exception as e:
        # Handle any exceptions that occur
        print(f"Error fetching vendors: {e}")
        return jsonify({"message": "Failed to retrieve vendors"}), 500

    finally:
        # Close the session to avoid memory leaks
        db_session.close()






# ****************************** Sale ******************************************************************
# Quotations -----------------------------------


def check_list_not_empty(lst):
    """
    Checks if the list has at least one item.

    Args:
        lst (list): The list to check.

    Raises:
        ValueError: If the list is empty.
    """
    if not lst:  # This checks if the list is empty (same as len(lst) == 0)
        raise ValueError("The list is empty.")


@app.route('/add_quotation_note', methods=['POST'])
@login_required
def add_quotation_note():
    try:
        # Validate CSRF token
        validate_csrf(request.headers.get('X-CSRFToken'))
    except BadRequest:
        return jsonify({"error": "Invalid CSRF token"}), 400

    data = request.get_json()
    note_content = data.get('note_content')
    quotation_id = data.get('quotation_id')
    created_by = current_user.id  # Assuming the current user is the creator
    recipient = data.get('recipient') or None
    quotation_number = data.get('quotation_number')
    company_id = current_user.app_id

    print(f'data is {data} and currnet company id is {company_id} and {type(company_id)}')

    if not note_content or not quotation_id:
        return jsonify({'success': False, "message": "Missing required fields", "status": "danger"}), 400

    try:
        with Session() as db_session:
            new_note = QuotationNote(
                quotation_id=quotation_id,
                note_type='internal',  # Assuming all notes added here are internal
                note_content=note_content,
                created_by=created_by,
                recipient=recipient,
                app_id=company_id
            )
            db_session.add(new_note)

            db_session.flush()

            # Create a notification
            notification_message = f"A new note has been added to Quotation #{quotation_number} by {current_user.name}."

            print(f'Recepeient is Before test with {recipient}')
            if not recipient:
                print(f'Recepeient is None with {recipient}')
                # Notify the company
                create_notification(
                    db_session=db_session,
                    company_id=company_id,  # Notify the company
                    message=notification_message,
                    type='info',  # Notification type
                    is_popup=True,  # Show as a popup
                    url=f"/sales/quotation/{quotation_id}"  # Link to the quotation
                )
            else:
                # Notify the specific user
                create_notification(
                    db_session=db_session,
                    user_id=recipient,  # Notify the specific user
                    message=notification_message,
                    type='info',  # Notification type
                    is_popup=True,  # Show as a popup
                    url=f"/sales/quotation/{quotation_id}"  # Link to the quotation
                )

            db_session.commit()

            # Return the newly created note data
            return jsonify({
                "id": new_note.id,
                "note_content": new_note.note_content,
                "created_at": new_note.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "created_by": new_note.user.name,
                "recipient": new_note.user_recipient.name if new_note.user_recipient else "",
                "message": "Note saved successfully",
                "status": "success"
            }), 201
    except Exception as e:
        return jsonify({"message": f"error: {str(e)}", "status": "danger"}), 500


@app.route('/approve_quotation', methods=['POST'])
def approve_quotation():
    """
    Approves a quotation by its ID.
    """

    db_session = Session()
    try:

        # Get data from the request
        data = request.get_json()
        quotation_id = data.get('quotation_id')

        if not quotation_id:
            return jsonify({"status": "error", "message": "Quotation ID is required"}), 400

        # Fetch the quotation from the database
        quotation = db_session.query(Quotation).filter_by(id=quotation_id).first()
        if not quotation:
            return jsonify({"status": "error", "message": "Quotation not found"}), 404

        # Update the quotation status to "accepted"
        quotation.status = QuotationStatus.approved

        db_session.commit()

        # Return a success response
        return jsonify({
            "status": "success",
            "message": f"Quotation approved successfully."
        }), 200

    except Exception as e:
        # Handle any errors
        db_session.rollback()
        return jsonify({
            "status": "error",
            "message": f"An error occurred: {str(e)}"
        }), 500
    finally:
        db_session.close()


@app.route('/cancel_quotation', methods=['POST'])
def cancel_quotation():
    """
    Approves a quotation by its ID.
    """

    db_session = Session()
    try:

        # Get data from the request
        data = request.get_json()
        quotation_id = data.get('quotation_id')

        if not quotation_id:
            return jsonify({"status": "error", "message": "Quotation ID is required"}), 400

        # Fetch the quotation from the database
        quotation = db_session.query(Quotation).filter_by(id=quotation_id).first()
        if not quotation:
            return jsonify({"status": "error", "message": "Quotation not found"}), 404

        # Update the quotation status to "accepted"
        quotation.status = QuotationStatus.cancelled

        db_session.commit()

        # Return a success response
        return jsonify({
            "status": "success",
            "message": f"Quotation {quotation_id} cancelled successfully."
        }), 200

    except Exception as e:
        # Handle any errors
        db_session.rollback()
        return jsonify({
            "status": "error",
            "message": f"An error occurred: {str(e)}"
        }), 500
    finally:
        db_session.close()


# Delete Quotation Route
@app.route('/delete_quotation/<int:quotation_id>', methods=['POST'])
@login_required
def delete_quotation(quotation_id):
    app_id = current_user.app_id
    db_session = Session()

    # Print statement to log the start of the deletion process

    # Fetch the quotation from the database
    quotation = db_session.query(Quotation).filter_by(id=quotation_id, app_id=app_id).first()

    # Check if the quotation exists
    if not quotation:
        flash('Quotation not found or you do not have permission to delete it.', 'error')
        db_session.close()
        return redirect(url_for('quotation_management'))

    try:
        # Print statement before deletion

        # Step 1: Delete related records in all dependent tables
        db_session.query(QuotationHistory).filter_by(quotation_id=quotation_id).delete()
        db_session.query(QuotationItem).filter_by(quotation_id=quotation_id).delete()
        db_session.query(QuotationNote).filter_by(quotation_id=quotation_id).delete()
        db_session.query(QuotationAttachment).filter_by(quotation_id=quotation_id).delete()
        db_session.query(QuotationApproval).filter_by(quotation_id=quotation_id).delete()
        db_session.query(QuotationStatusLog).filter_by(quotation_id=quotation_id).delete()

        # Step 2: Delete the quotation
        db_session.delete(quotation)
        db_session.commit()

        # Print statement after successful deletion

        # Flash success message
        flash('Quotation deleted successfully!', 'success')

    except Exception as e:
        # Print statement if an exception occurs

        # Rollback the transaction
        db_session.rollback()

        # Flash error message
        flash(f'An error occurred: {str(e)}', 'error')

    finally:
        # Print statement to confirm session closure

        # Close the session
        db_session.close()

    return redirect(url_for('quotation_management'))


# Sales Order Management Route


@app.route('/add_sales_order_note', methods=['POST'])
@login_required
def add_sales_order_note():
    try:
        # Validate CSRF token
        validate_csrf(request.headers.get('X-CSRFToken'))
    except BadRequest:
        return jsonify({"error": "Invalid CSRF token"}), 400

    data = request.get_json()
    note_content = data.get('note_content')
    sales_order_id = data.get('sales_order_id')
    created_by = current_user.id  # Assuming the current user is the creator
    recipient = data.get('recipient') or None
    sales_order_number = data.get('sales_order_number')
    company_id = current_user.app_id

    print(f'data is {data} and currnet company id is {company_id} and {type(company_id)}')

    if not note_content or not sales_order_id:
        return jsonify({'success': False, "message": "Missing required fields", "status": "danger"}), 400

    try:
        with Session() as db_session:
            new_note = SalesOrderNote(
                sales_order_id=sales_order_id,
                note_type='internal',  # Assuming all notes added here are internal
                note_content=note_content,
                created_by=created_by,
                recipient=recipient,
                app_id=company_id
            )
            db_session.add(new_note)

            db_session.flush()

            # Create a notification
            notification_message = f"A new note has been added to sales_order #{sales_order_number} by {current_user.name}."

            print(f'Recepeient is Before test with {recipient}')
            if not recipient:
                print(f'Recepeient is None with {recipient}')
                # Notify the company
                create_notification(
                    db_session=db_session,
                    company_id=company_id,  # Notify the company
                    message=notification_message,
                    type='info',  # Notification type
                    is_popup=True,  # Show as a popup
                    url=f"/sales/sales_order/{sales_order_id}"  # Link to the sales_order
                )
            else:
                # Notify the specific user
                create_notification(
                    db_session=db_session,
                    user_id=recipient,  # Notify the specific user
                    message=notification_message,
                    type='info',  # Notification type
                    is_popup=True,  # Show as a popup
                    url=f"/sales/sales_order/{sales_order_id}"  # Link to the sales_order
                )

            db_session.commit()

            # Return the newly created note data
            return jsonify({
                "id": new_note.id,
                "note_content": new_note.note_content,
                "created_at": new_note.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "created_by": new_note.user.name,
                "recipient": new_note.user_recipient.name if new_note.user_recipient else "",
                "message": "Note saved successfully",
                "status": "success"
            }), 201
    except Exception as e:
        return jsonify({"message": f"error: {str(e)}", "status": "danger"}), 500


# Edit sales_order Route


@app.route('/approve_sales_order', methods=['POST'])
def approve_sales_order():
    """
    Approves a sales_order by its ID.
    """

    db_session = Session()
    try:

        # Get data from the request
        data = request.get_json()
        sales_order_id = data.get('sales_order_id')

        if not sales_order_id:
            return jsonify({"status": "error", "message": "Sales Order ID is required"}), 400

        # Fetch the sales_order from the database
        sales_order = db_session.query(SalesOrder).filter_by(id=sales_order_id).first()
        if not sales_order:
            return jsonify({"status": "error", "message": "Sales Order not found"}), 404

        # Update the sales_order status to "accepted"
        sales_order.status = OrderStatus.approved

        db_session.commit()

        # Return a success response
        return jsonify({
            "status": "success",
            "message": f"Sales Order approved successfully."
        }), 200

    except Exception as e:
        # Handle any errors
        db_session.rollback()
        return jsonify({
            "status": "error",
            "message": f"An error occurred: {str(e)}"
        }), 500
    finally:
        db_session.close()


@app.route('/cancel_sales_order', methods=['POST'])
@login_required
def cancel_sales_order():
    """
    Cancels a sales order by its ID.
    """

    sales_order_id = request.form.get('sales_order_id')
    try:

        if not sales_order_id:
            flash("Sales Order ID is required", "error")
            return redirect(url_for('sales_order_management'))

        with Session() as db_session:
            # Fetch the sales order from the database
            sales_order = db_session.query(SalesOrder).filter_by(id=sales_order_id).first()
            if not sales_order:
                flash("Sales Order not found", "error")
                return redirect(url_for('sales_order_management'))

            # Update the sales order status to "cancelled" or "rejected" based on the current status
            if sales_order.status == OrderStatus.approved:
                sales_order.status = OrderStatus.rejected  # Rejected if it was approved
            else:
                sales_order.status = OrderStatus.canceled  # Cancelled if it's in any other state

            db_session.commit()

            flash(f"Sales Order {sales_order.sales_order_number} cancelled successfully.", "success")
            return redirect(url_for('sales_order_management'))  # Replace with actual page

    except Exception as e:
        db_session.rollback()
        flash(f"An error occurred: {str(e)}", "error")
        return redirect(url_for('sales_order_details', sales_order_id=sales_order_id))


# Delete sales_order Route
@app.route('/delete_sales_order/<int:sales_order_id>', methods=['POST'])
@login_required
def delete_sales_order(sales_order_id):
    app_id = current_user.app_id
    db_session = Session()

    try:
        sales_order = db_session.query(SalesOrder).filter_by(id=sales_order_id, app_id=app_id).first()
        if not sales_order:
            flash('Sales Order not found!', 'error')
            return redirect(url_for('sales_order_management'))

        if sales_order.status.name != 'draft':
            logger.info(f'Status is {sales_order.status} and nae is {sales_order.status.name}')
            flash('Cannot delete Approved Sales Order', 'warning')
            return redirect(url_for('sales_order_management'))

        # Step 1: Delete related records in all dependent tables
        db_session.query(SalesOrderHistory).filter_by(sales_order_id=sales_order_id).delete()
        db_session.query(SalesOrderItem).filter_by(sales_order_id=sales_order_id).delete()
        db_session.query(SalesOrderNote).filter_by(sales_order_id=sales_order_id).delete()
        db_session.query(SalesOrderAttachment).filter_by(sales_order_id=sales_order_id).delete()
        db_session.query(SalesOrderApproval).filter_by(sales_order_id=sales_order_id).delete()
        db_session.query(SalesOrderStatusLog).filter_by(sales_order_id=sales_order_id).delete()

        db_session.delete(sales_order)
        db_session.commit()
        flash('Sales Order deleted successfully!', 'success')
    except Exception as e:
        db_session.rollback()
        logger.error(f'An error occured {e}')
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(url_for('sales_order_management'))
    finally:
        db_session.close()  # Ensure session is always closed
        return redirect(url_for('sales_order_management'))


# Delivery Note Management Route


@app.route('/sales/edit_delivery_note/<int:delivery_note_id>', methods=['GET', 'POST'])
@login_required
def edit_delivery_note(delivery_note_id):
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

    # Fetch the delivery note to edit
    delivery_note = db_session.query(DeliveryNote).filter_by(id=delivery_note_id, app_id=app_id).first()

    if not delivery_note:
        flash("Delivery Note not found.", "error")
        return redirect(url_for('delivery_note_management'))

    if request.method == 'POST':
        print("POST request received. Processing form data...")
        print(f"Form Data: {request.form}")

        try:
            # Extract customer data
            customer_id = request.form.get('customer_id', '').strip()  # Get customer_id (may be empty)
            customer_name = request.form.get('customer_name', '').strip()  # Get customer_name

            if customer_id and customer_id.isdigit():
                # Existing customer (convert ID to integer)
                customer_id = int(customer_id)
                print(f"Existing customer selected. Customer ID: {customer_id}")
            else:
                # New customer (customer_id is empty or invalid)
                print(f"New customer selected. Customer name: {customer_name}")

                # Check if a one-time customer with the same name exists
                existing_customer = db_session.query(Vendor).filter_by(
                    vendor_name=customer_name,
                    is_one_time=True,
                    app_id=app_id
                ).first()

                if existing_customer:
                    customer_id = existing_customer.id  # Use existing one-time customer
                    print(f"Found existing one-time customer. Customer ID: {customer_id}")
                else:
                    # Create a new one-time customer
                    print(f"Creating new one-time customer: {customer_name}")
                    new_customer = Vendor(
                        vendor_name=customer_name,
                        is_one_time=True,
                        vendor_type="Customer",
                        app_id=app_id
                    )
                    db_session.add(new_customer)
                    db_session.commit()
                    customer_id = new_customer.id  # Use new customer's ID
                    print(f"New one-time customer created. Customer ID: {customer_id}")

            # Update delivery note details
            print("Updating delivery note details...")
            delivery_note.delivery_number = request.form['delivery_number']
            delivery_note.customer_id = customer_id
            delivery_note.delivery_date = datetime.datetime.strptime(request.form['delivery_date'], '%Y-%m-%d').date()
            delivery_note.shipping_address = request.form['shipping_address'] or None
            delivery_note.delivery_method = request.form.get('delivery_method', None)
            delivery_note.additional_notes = request.form.get('additional_notes', '').strip()

            # Update delivered_by and received_by details
            delivered_by_name = request.form.get('delivered_by_name', '').strip()
            received_by_name = request.form.get('received_by_name', '').strip()

            # Extract time inputs
            delivered_by_time = request.form.get('delivered_by_time', '').strip()
            received_by_time = request.form.get('received_by_time', '').strip()

            # Ensure delivered_by_time and received_by_time are properly formatted
            if delivered_by_time:
                try:
                    print(f"Parsing delivered_by_time: {delivered_by_time}")

                    # If time has ':00' (i.e., HH:MM:00), remove the ':00' part
                    if ':00' in delivered_by_time:
                        delivered_by_time = delivered_by_time[:-3]  # Remove the last three characters ':00'

                    # Parse time only (no date part)
                    delivered_by_time = datetime.datetime.strptime(delivered_by_time, "%H:%M").time()
                    print(f"Parsed delivered_by_time (time only): {delivered_by_time}")
                except ValueError as e:
                    print(f"Error parsing delivered_by_time: {e}")
                    delivered_by_time = None
            else:
                delivered_by_time = None

            if received_by_time:
                try:
                    print(f"Parsing received_by_time: {received_by_time}")

                    # If time has ':00' (i.e., HH:MM:00), remove the ':00' part
                    if ':00' in received_by_time:
                        received_by_time = received_by_time[:-3]  # Remove the last three characters ':00'

                    # Parse time only (no date part)
                    received_by_time = datetime.datetime.strptime(received_by_time, "%H:%M").time()
                    print(f"Parsed received_by_time (time only): {received_by_time}")
                except ValueError as e:
                    print(f"Error parsing received_by_time: {e}")
                    received_by_time = None
            else:
                received_by_time = None

            delivery_note.delivered_by_name = delivered_by_name
            delivery_note.delivered_by_time = delivered_by_time
            delivery_note.received_by_name = received_by_name
            delivery_note.received_by_time = received_by_time

            # Commit updated delivery note
            db_session.commit()
            print("Delivery note details updated successfully.")

            # Handle Delivery Note Items
            print("Processing updated delivery note items...")

            # Fetch lists from request form and ensure they are not None
            item_type_list = request.form.getlist('item_type[]')
            item_name_list = request.form.getlist('item_name[]')
            inventory_item_list = request.form.getlist('inventory_item[]')
            description_list = request.form.getlist('item_description[]') or None
            quantity_delivered_list = request.form.getlist('quantity[]')
            uom_list = request.form.getlist('uom[]')

            # Debug prints
            print(f"Number of items to update: {len(item_type_list)}")
            print(f"item_name_list: {item_name_list}")
            print(f"inventory_item_list: {inventory_item_list}")
            print(f"description_list: {description_list}")
            print(f"quantity_delivered_list: {quantity_delivered_list}")
            print(f"uom_list: {uom_list}")

            # Remove old items and re-add updated items
            db_session.query(DeliveryNoteItem).filter_by(delivery_note_id=delivery_note.id).delete()

            for idx, (item_type, item_name, inventory_item, description, quantity_delivered, uom) in enumerate(
                    zip(item_type_list, item_name_list, inventory_item_list, description_list, quantity_delivered_list,
                        uom_list),
                    start=1
            ):
                print(f"Updating item {idx}: Type={item_type}, Name={item_name}, Inventory ID={inventory_item}")

                # Handling item ID based on type
                item_id = int(inventory_item) if item_type == "inventory" and inventory_item else None
                item_name = item_name if item_type != "inventory" else None  # If not inventory, store item name instead

                # Fetch description for inventory items
                if item_type == "inventory":
                    inventory_item_obj = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()
                    description = inventory_item_obj.item_description if inventory_item_obj else description

                # Create updated DeliveryNoteItem
                updated_item = DeliveryNoteItem(
                    delivery_note_id=delivery_note.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    description=description,
                    quantity_delivered=quantity_delivered,
                    uom=uom,
                    app_id=app_id
                )

                print(f"Adding updated item {idx} to database...")
                db_session.add(updated_item)

            # Commit all updated items to the database
            db_session.commit()
            print("Delivery note items updated successfully.")

            # Return JSON success response
            print("Delivery note updated successfully. Returning JSON response...")
            flash("Delivery Note updated successfully!", "success")
            return jsonify({
                'success': True,
                'message': 'Delivery Note updated successfully!',
                'delivery_note_id': delivery_note.id
            })

        except Exception as e:
            db_session.rollback()
            print(f"An error occurred: {str(e)}")  # Print the error for debugging
            # Return JSON error response
            flash(f"An error occurred: {str(e)}", "error")
            return jsonify({
                'success': False,
                'message': f'An error occurred: {str(e)}'
            })

        finally:
            print("Closing the session...")
            db_session.close()

    else:
        print("GET request: Rendering template for editing delivery note...")

        # Fetch existing delivery note details
        customers = db_session.query(Vendor).filter_by(app_id=app_id).all()
        sales_orders = db_session.query(SalesOrder).filter_by(app_id=app_id).all()
        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()
        inventory_items = db_session.query(InventoryItemVariationLink).filter_by(app_id=app_id, status="active").all()

        # Render the template with delivery note details
        return render_template('/sales/edit_delivery_note.html', delivery_note=delivery_note,
                               customers=customers, sales_orders=sales_orders, inventory_items=inventory_items,
                               uoms=uoms,
                               company=company, role=role, modules=modules_data)


@app.route('/add_delivery_note_note', methods=['POST'])
@login_required
def add_delivery_note_note():
    try:
        # Validate CSRF token
        validate_csrf(request.headers.get('X-CSRFToken'))
    except BadRequest:
        return jsonify({"error": "Invalid CSRF token"}), 400

    data = request.get_json()
    note_content = data.get('note_content')
    delivery_note_id = data.get('delivery_note_id')
    created_by = current_user.id  # Assuming the current user is the creator
    recipient = data.get('recipient')
    delivery_note_number = data.get('delivery_number')
    company_id = current_user.app_id

    print(f'data is {data} and current company id is {company_id} and {type(company_id)}')

    if not note_content or not delivery_note_id:
        return jsonify({'success': False, "message": "Missing required fields", "status": "danger"}), 400

    try:
        with Session() as db_session:
            new_note = DeliveryNoteNote(
                delivery_note_id=delivery_note_id,
                note_type='internal',  # Assuming all notes added here are internal
                note_content=note_content,
                created_by=created_by,
                recipient=recipient,
                app_id=company_id
            )
            db_session.add(new_note)

            db_session.flush()

            # Create a notification
            notification_message = f"A new note has been added to delivery note #{delivery_note_number} by {current_user.name}."

            print(f'Recipient is Before test with {recipient}')
            if not recipient:
                print(f'Recipient is None with {recipient}')
                # Notify the company
                create_notification(
                    db_session=db_session,
                    company_id=company_id,  # Notify the company
                    message=notification_message,
                    type='info',  # Notification type
                    is_popup=True,  # Show as a popup
                    url=f"/delivery/delivery_note/{delivery_note_id}"  # Link to the delivery note
                )
            else:
                # Notify the specific user
                create_notification(
                    db_session=db_session,
                    user_id=recipient,  # Notify the specific user
                    message=notification_message,
                    type='info',  # Notification type
                    is_popup=True,  # Show as a popup
                    url=f"/delivery/delivery_note/{delivery_note_id}"  # Link to the delivery note
                )

            db_session.commit()

            # Return the newly created note data
            return jsonify({
                "id": new_note.id,
                "note_content": new_note.note_content,
                "created_at": new_note.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "created_by": new_note.user.name,
                "message": "Note saved successfully",
                "status": "success"
            }), 201
    except Exception as e:
        return jsonify({"message": f"error: {str(e)}", "status": "danger"}), 500


@app.route('/approve_delivery_note', methods=['POST'])
@login_required
def approve_delivery_note():
    """
    Approves a delivery note by its ID.
    """

    db_session = Session()
    try:
        # Get data from the request
        data = request.get_json()
        delivery_note_id = data.get('delivery_note_id')

        if not delivery_note_id:
            return jsonify({"status": "error", "message": "Delivery Note ID is required"}), 400

        # Fetch the delivery note from the database
        delivery_note = db_session.query(DeliveryNote).filter_by(id=delivery_note_id).first()
        if not delivery_note:
            return jsonify({"status": "error", "message": "Delivery Note not found"}), 404

        # Update the delivery note status to "approved"
        delivery_note.status = DeliveryStatus.delivered  # Assuming "approved" is a valid status

        db_session.commit()

        # Return a success response
        return jsonify({
            "status": "success",
            "message": f"Delivery Note approved successfully."
        }), 200

    except Exception as e:
        # Handle any errors
        db_session.rollback()
        return jsonify({
            "status": "error",
            "message": f"An error occurred: {str(e)}"
        }), 500
    finally:
        db_session.close()


@app.route('/sales/delete_delivery_note/<int:delivery_note_id>', methods=['POST'])
@login_required
def delete_delivery_note(delivery_note_id):
    app_id = current_user.app_id
    db_session = Session()

    try:
        # Fetch the delivery note to delete
        delivery_note = db_session.query(DeliveryNote).filter_by(id=delivery_note_id, app_id=app_id).first()

        if not delivery_note:
            flash("Delivery Note not found.", "error")
            return redirect(url_for('delivery_note_management'))

        # Delete associated delivery note items
        db_session.query(DeliveryNoteItem).filter_by(delivery_note_id=delivery_note.id).delete()

        # Delete the delivery note
        db_session.delete(delivery_note)
        db_session.commit()

        flash("Delivery Note deleted successfully!", "success")
        return redirect(url_for('delivery_note_management'))

    except Exception as e:
        db_session.rollback()
        flash(f"An error occurred while deleting the delivery note: {str(e)}", "error")
        return redirect(url_for('delivery_note_management'))

    finally:
        db_session.close()


@app.route('/cancel_sales_invoice', methods=['POST'])
@login_required
def cancel_sales_invoice():
    """
    Cancels an invoice by its ID.
    """
    invoice_id = request.form.get('invoice_id')
    try:
        if not invoice_id:
            flash("Invoice ID is required", "error")
            return redirect(url_for('invoice_management'))

        with Session() as db_session:
            # Fetch the invoice from the database
            invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id).first()
            if not invoice:
                flash("Invoice not found", "error")
                return redirect(url_for('invoice_management'))

            # Update the invoice status to "canceled"
            invoice.status = InvoiceStatus.canceled
            db_session.commit()

            flash(f"Invoice {invoice.invoice_number} canceled successfully.", "success")
            return redirect(url_for('invoice_management'))  # Redirect to invoice management page

    except Exception as e:
        db_session.rollback()
        flash(f"An error occurred: {str(e)}", "error")
        return redirect(url_for('invoice_details', invoice_id=invoice_id))


@app.route('/delete_sales_invoice/<int:invoice_id>', methods=['POST'])
@login_required
def delete_sales_invoice(invoice_id):
    app_id = current_user.app_id
    db_session = Session()
    invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id, app_id=app_id).first()

    try:
        if invoice.status.name != 'draft':
            flash('Cannot delete Approved Sales Invoice', 'warning')
            return redirect(url_for('sales_order_management'))

        # Step 1: Delete related records in all dependent tables
        db_session.query(SalesInvoiceHistory).filter_by(invoice_id=invoice_id).delete()
        db_session.query(SalesInvoiceItem).filter_by(invoice_id=invoice_id).delete()
        db_session.query(SalesInvoiceNote).filter_by(invoice_id=invoice_id).delete()
        db_session.query(SalesInvoiceApproval).filter_by(invoice_id=invoice_id).delete()
        db_session.query(SalesInvoiceStatusLog).filter_by(invoice_id=invoice_id).delete()

        # Delete the invoice
        db_session.delete(invoice)
        db_session.commit()
        flash('Invoice deleted successfully!', 'success')


    except Exception as e:
        db_session.rollback()
        flash(f'An error occurred: {str(e)}', 'error')
    finally:
        db_session.close()

    return redirect(url_for('invoice_management'))


@app.route('/add_invoice_note', methods=['POST'])
@login_required
def add_invoice_note():
    try:
        # Validate CSRF token
        validate_csrf(request.headers.get('X-CSRFToken'))
    except BadRequest:
        return jsonify({"error": "Invalid CSRF token"}), 400

    data = request.get_json()
    note_content = data.get('note_content')
    invoice_id = data.get('invoice_id')
    created_by = current_user.id  # Assuming the current user is the creator
    recipient = data.get('recipient')
    invoice_number = data.get('invoice_number')
    company_id = current_user.app_id

    print(f'data is {data} and currnet company id is {company_id} and {type(company_id)}')

    if not note_content or not invoice_id:
        return jsonify({'success': False, "message": "Missing required fields", "status": "danger"}), 400

    try:
        with Session() as db_session:
            new_note = SalesInvoiceNote(
                invoice_id=invoice_id,
                note_type='internal',  # Assuming all notes added here are internal
                note_content=note_content,
                created_by=created_by,
                recipient=recipient,
                app_id=company_id
            )
            db_session.add(new_note)

            db_session.flush()

            # Create a notification
            notification_message = f"A new note has been added to sales_order #{invoice_number} by {current_user.name}."

            print(f'Recepeient is Before test with {recipient}')
            if not recipient:
                print(f'Recepeient is None with {recipient}')
                # Notify the company
                create_notification(
                    db_session=db_session,
                    company_id=company_id,  # Notify the company
                    message=notification_message,
                    type='info',  # Notification type
                    is_popup=True,  # Show as a popup
                    url=f"/sales/invoice/{invoice_id}"  # Link to the sales_order
                )
            else:
                # Notify the specific user
                create_notification(
                    db_session=db_session,
                    user_id=recipient,  # Notify the specific user
                    message=notification_message,
                    type='info',  # Notification type
                    is_popup=True,  # Show as a popup
                    url=f"/sales/invoice/{invoice_id}"  # Link to the sales_order
                )

            db_session.commit()

            # Return the newly created note data
            return jsonify({
                "id": new_note.id,
                "note_content": new_note.note_content,
                "created_at": new_note.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "created_by": new_note.user.name,
                "recipient": new_note.user_recipient.name if new_note.user_recipient else "",
                "message": "Note saved successfully",
                "status": "success"
            }), 201
    except Exception as e:
        return jsonify({"message": f"error: {str(e)}", "status": "danger"}), 500


@app.route('/api/record_payment', methods=['POST'])
def record_payment():
    # Parse form data from the request
    transaction_type = request.form.get('transaction_type')

    if transaction_type != "direct_sale":
        invoice_id = int(request.form.get('invoiceId'))
        amount = request.form.get('amount')
        payment_date = request.form.get('paymentDate')
        asset_account_id = int(request.form.get('fundingAccount'))

        reference = request.form.get('reference') or None
        payment_method = request.form.get('paymentMethod')

        # Ensure payment_method is not None or an empty string before converting to int
        payment_method = int(payment_method) if payment_method and payment_method.strip().isdigit() else None

        created_by = current_user.id  # Assuming the user ID is passed from the frontend
        app_id = current_user.app_id  # Assuming the company ID is passed from the frontend

        tax_payable_account_id = request.form.get('tax_account_id')
        tax_payable_account_id = int(
            tax_payable_account_id) if tax_payable_account_id and tax_payable_account_id.strip() else None

        # Validate required fields
        if not all([invoice_id, amount, payment_date, asset_account_id]):
            flash('Missing required fields.', 'error')
            return redirect(url_for('invoice_details', invoice_id=invoice_id))  # Redirect to a relevant route

        # Validate amount is a valid decimal
        try:
            amount = Decimal(amount)
        except InvalidOperation:
            flash('Amount must be a valid number.', 'error')
            return redirect(url_for('invoice_details', invoice_id=invoice_id))  # Redirect to a relevant route

        # Convert payment date to a datetime object
        try:
            payment_date = datetime.datetime.strptime(payment_date, '%Y-%m-%d')
        except ValueError:
            flash('Invalid payment date format. Use YYYY-MM-DD.', 'error')
            return redirect(url_for('invoice_details', invoice_id=invoice_id))  # Redirect to a relevant route

        # Start a database session
        db_session = Session()

        try:
            # Fetch the invoice
            invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id).first()
            if not invoice:
                flash('Invoice not found.', 'error')
                return redirect(url_for('invoice_details', invoice_id=invoice_id))  # Redirect to a relevant route

            # Validate payment amount
            if amount <= 0:
                flash('Payment amount must be greater than 0.', 'error')
                return redirect(url_for('invoice_details', invoice_id=invoice_id))  # Redirect to a relevant route

            # Check if the payment exceeds the invoice balance, filtering by payment_status and excluding refunded and cancelled payments
            total_paid = db_session.query(func.sum(SalesTransaction.amount_paid)).filter(
                SalesTransaction.invoice_id == invoice_id,
                SalesTransaction.payment_status.in_(
                    [SalesPaymentStatus.full, SalesPaymentStatus.partial, SalesPaymentStatus.paid]),
                # Use enum values
                SalesTransaction.payment_status != SalesPaymentStatus.refund,  # Exclude 'refund' payments
                SalesTransaction.payment_status != SalesPaymentStatus.cancelled  # Exclude 'cancelled' payments
            ).scalar() or Decimal('0.00')

            remaining_balance = invoice.total_amount - total_paid
            if amount > remaining_balance:
                flash(f'Payment amount exceeds the remaining balance of {remaining_balance:.2f}.', 'error')
                return redirect(url_for('invoice_details', invoice_id=invoice_id))  # Redirect to a relevant route

            # Determine the payment status based on total_paid and remaining_balance
            if total_paid + amount == invoice.total_amount:
                payment_status = SalesPaymentStatus.paid
            elif total_paid + amount < invoice.total_amount:
                payment_status = SalesPaymentStatus.partial
            else:
                flash("Payment exceeds the total amount", "error")
                return redirect(url_for("invoice_details", invoice_id=invoice_id))

            # Create a new SalesTransaction record
            new_payment = SalesTransaction(
                invoice_id=invoice_id,
                customer_id=invoice.customer_id,
                payment_date=payment_date,
                amount_paid=amount,  # Pass as Decimal
                currency_id=invoice.currency,
                reference_number=reference,
                is_posted_to_ledger=False,  # Will be updated after ledger posting
                # Determine the payment status based on the amount and remaining_balance
                payment_status=payment_status,
                created_by=created_by,
                app_id=app_id
            )
            db_session.add(new_payment)
            db_session.commit()

            if (remaining_balance - amount) > 0:
                invoice.status = InvoiceStatus.partially_paid
            else:
                invoice.status = InvoiceStatus.paid

            # Allocate the payment to base and tax amounts
            allocate_payment(
                sale_transaction_id=new_payment.id,
                invoice_id=invoice_id,
                payment_amount=amount,
                payment_date=payment_date,
                db_session=db_session,
                payment_mode=payment_method,
                total_tax_amount=invoice.total_tax_amount,
                payment_account=asset_account_id,
                tax_payable_account_id=tax_payable_account_id,
                credit_sale_account=None,
                reference=reference
            )

            db_session.commit()

            # Flash success message
            flash('Payment recorded successfully!', 'success')
            return redirect(url_for('invoice_details', invoice_id=invoice_id))  # Redirect to a relevant route

        except Exception as e:
            db_session.rollback()
            print(f'error is {e}')
            flash(f'An error occurred: {str(e)}', 'error')
            return redirect(url_for('invoice_details', invoice_id=invoice_id))  # Redirect to a relevant route

        finally:
            db_session.close()

    else:
        direct_sale_id = int(request.form.get('direct_sale_id'))
        amount = request.form.get('amount')
        payment_date = request.form.get('paymentDate')
        asset_account_id = request.form.get('fundingAccount')
        payment_method = request.form.get('paymentMethod') or None
        reference = request.form.get('reference') or None

        # Validate amount is a valid decimal
        try:
            amount = Decimal(amount)
        except InvalidOperation:
            flash('Amount must be a valid number.', 'error')
            return redirect(url_for('direct_sales_transaction_details',
                                    transaction_id=direct_sale_id))  # Redirect to a relevant route

        # Convert payment date to a datetime object
        try:
            payment_date = datetime.datetime.strptime(payment_date, '%Y-%m-%d')
        except ValueError:
            flash('Invalid payment date format. Use YYYY-MM-DD.', 'error')
            return redirect(url_for('direct_sales_transaction_details',
                                    transaction_id=direct_sale_id))  # Redirect to a relevant route

        # Start a database session
        db_session = Session()

        try:
            # Fetch the direct sale transaction
            direct_sale = db_session.query(DirectSalesTransaction).filter_by(id=direct_sale_id).first()
            if not direct_sale:
                flash('Transaction not found.', 'error')
                return redirect(url_for('direct_sales_transaction_details',
                                        transaction_id=direct_sale_id))  # Redirect to a relevant route

            # Validate payment amount
            if amount <= 0:
                flash('Payment amount must be greater than 0.', 'error')
                return redirect(url_for('direct_sales_transaction_details',
                                        transaction_id=direct_sale_id))  # Redirect to a relevant route

            # Check if the payment exceeds the balance
            total_paid = db_session.query(func.sum(DirectSalesTransaction.amount_paid)).filter(
                DirectSalesTransaction.id == direct_sale_id,
                DirectSalesTransaction.payment_status.in_(['full', 'partial'])
            ).scalar() or Decimal('0.00')

            remaining_balance = direct_sale.total_amount - total_paid
            if amount > remaining_balance:
                flash(f'Payment amount exceeds the remaining balance of {remaining_balance:.2f}.', 'error')
                return redirect(url_for('direct_sales_transaction_details',
                                        transaction_id=direct_sale_id))  # Redirect to a relevant route

            if (remaining_balance - amount) > 0:
                direct_sale.status = OrderStatus.partially_paid
            else:
                direct_sale.status = OrderStatus.paid

            direct_sale.amount_paid += amount

            # Access the first PaymentAllocation object (if it exists)
            if direct_sale.payment_allocations:
                payment_allocation = direct_sale.payment_allocations[0]
                tax_payable_account_id = payment_allocation.tax_payable_account_id
                credit_sale_account = payment_allocation.credit_sale_account
            else:
                # Handle the case where no payment allocations exist
                tax_payable_account_id = None
                credit_sale_account = None

            # Allocate the payment to base and tax amounts
            allocate_direct_sale_payment(
                direct_sale_id=direct_sale_id,
                payment_amount=amount,
                db_session=db_session,
                payment_date=payment_date,
                payment_mode=payment_method,
                total_tax_amount=direct_sale.total_tax_amount,
                payment_account=asset_account_id,
                tax_payable_account_id=tax_payable_account_id,
                credit_sale_account=credit_sale_account,
                reference=reference
            )

            db_session.commit()

            # Flash success message
            flash('Payment recorded successfully!', 'success')
            return redirect(url_for('direct_sales_transaction_details',
                                    transaction_id=direct_sale_id))  # Redirect to a relevant route

        except Exception as e:
            db_session.rollback()
            logger.error(f'error is {e}')
            flash(f'An error occurred: {str(e)}', 'error')
            return redirect(url_for('direct_sales_transaction_details',
                                    transaction_id=direct_sale_id))  # Redirect to a relevant route

        finally:
            db_session.close()


def generate_direct_sale_number():
    with Session() as db_session:
        app_id = current_user.app_id  # Assuming `current_user` is available and has `app_id`

        # Get current month and last two digits of the year
        now = datetime.datetime.now()
        month_year = now.strftime("%m%y")  # e.g., "0325" for March 2025

        # Query the last direct sale receipt for the company
        last_sale = db_session.query(DirectSalesTransaction).filter_by(app_id=app_id).order_by(
            DirectSalesTransaction.direct_sale_number.desc()).first()

        if last_sale:
            # Extract the last month-year part of the receipt number
            last_month_year = last_sale.direct_sale_number.split('-')[1]

            if last_month_year == month_year:
                # If the month-year is the same, continue incrementing the sequence
                last_number_part = last_sale.direct_sale_number.split('-')[-1]
                next_number = int(last_number_part) + 1
            else:
                # If the month has changed, restart numbering from 1
                next_number = 1
        else:
            next_number = 1  # Start with 1 if no previous receipt exists

        # Format the sequence number with leading zeros
        sequence_number = str(next_number).zfill(5)  # Ensures it remains 5 digits (00001, 00002, etc.)

        # Generate the new receipt number
        new_sale_number = f"SALE-{month_year}-{sequence_number}"

        return new_sale_number


@app.route('/approve_direct_sale', methods=['POST'])
@login_required
def approve_direct_sale():
    """
    Approves a Direct Sale by its ID.
    """
    db_session = Session()
    try:
        # Get data from the request
        data = request.form
        direct_sale_id = data.get('direct_sale_id')

        if not direct_sale_id:
            return jsonify({"status": "error", "message": "Direct Sale ID is required"}), 400

        # Fetch the Direct Sale from the database
        direct_sale = db_session.query(DirectSalesTransaction).filter_by(id=direct_sale_id).first()
        if not direct_sale:
            return jsonify({"status": "error", "message": "Direct Sale not found"}), 404

        # Update the Direct Sale status to "approved"
        if direct_sale.amount_paid == 0:
            direct_sale.status = OrderStatus.unpaid

        elif direct_sale.amount_paid < direct_sale.total_amount:
            direct_sale.status = OrderStatus.partially_paid

        else:
            direct_sale.status = OrderStatus.paid

        # Commit the changes to the database
        db_session.commit()

        flash("Direct Sale approved successfully.", "success")
        return redirect(url_for("direct_sales_transaction_details", transaction_id=direct_sale_id))


    except Exception as e:
        # Handle any errors
        db_session.rollback()
        return jsonify({
            "status": "error",
            "message": f"An error occurred: {str(e)}"
        }), 500

    finally:
        db_session.close()


@app.route('/edit_sales_transaction/<int:transaction_id>', methods=['POST'])
@login_required
def edit_sales_transaction(transaction_id):
    app_id = current_user.app_id
    db_session = Session()

    try:
        # Fetch the transaction by ID and app_id
        transaction = db_session.query(SalesTransaction).filter_by(id=transaction_id, app_id=app_id).first()

        if not transaction:
            flash("Transaction not found.", "error")
            return redirect(url_for('view_sales_transactions'))

        # Update basic fields
        new_amount = Decimal(request.form.get('amount'))
        new_payment_date = datetime.datetime.strptime(request.form.get('paymentDate'), '%Y-%m-%d')
        new_reference = request.form.get('reference')
        new_payment_method = request.form.get('paymentMethod')
        new_asset_account = request.form.get('assetAccount')
        new_funding_account = request.form.get('fundingAccount')

        # Ensure it's not None or empty before converting to int
        new_payment_method = int(
            new_payment_method) if new_payment_method and new_payment_method.strip().isdigit() else None

        # Fetch the associated invoice
        invoice = db_session.query(SalesInvoice).filter_by(id=transaction.invoice_id).first()
        if not invoice:
            flash("Associated invoice not found.", "error")
            return redirect(url_for('view_sales_transactions'))

        # Calculate the total paid amount for the invoice (including the current transaction's original amount)
        total_paid = db_session.query(func.sum(SalesTransaction.amount_paid)).filter(
            SalesTransaction.invoice_id == invoice.id,
            SalesTransaction.payment_status.in_(
                [SalesPaymentStatus.full, SalesPaymentStatus.partial, SalesPaymentStatus.paid]),
            SalesTransaction.payment_status != SalesPaymentStatus.refund,
            SalesTransaction.payment_status != SalesPaymentStatus.cancelled
        ).scalar() or Decimal('0.00')

        # Calculate the remaining balance before updating the current transaction
        remaining_balance_before_update = invoice.total_amount - (total_paid - transaction.amount_paid)

        # Validate the new amount against the remaining balance
        if new_amount > remaining_balance_before_update:
            flash(f"Payment amount exceeds the remaining balance of {remaining_balance_before_update:.2f}.", "error")
            return redirect(url_for('sales_transaction_details', transaction_id=transaction_id))

        # Update the amount_paid field of the SalesTransaction
        transaction.amount_paid = new_amount  # <-- This line updates the amount_paid field
        transaction.payment_date = new_payment_date  # Update payment_date
        transaction.reference_number = new_reference  # Update reference_number

        # Determine the new payment status
        if new_amount == remaining_balance_before_update:
            new_payment_status = SalesPaymentStatus.full
        elif new_amount < remaining_balance_before_update:
            new_payment_status = SalesPaymentStatus.partial
        else:
            flash("Payment exceeds the total amount", "error")
            return redirect(url_for('sales_transaction_details', transaction_id=transaction_id))

        # Update the payment status
        transaction.payment_status = new_payment_status

        # Update the invoice status based on the new payment
        if new_payment_status == SalesPaymentStatus.full:
            invoice.status = InvoiceStatus.paid
        elif new_payment_status == SalesPaymentStatus.partial:
            invoice.status = InvoiceStatus.partially_paid

        # Update payment allocations
        if transaction.payment_allocations:
            allocation = transaction.payment_allocations[0]  # Assuming one allocation per transaction

            # Recalculate the tax amount based on the original tax ratio or invoice tax rate
            if invoice.total_tax_amount > 0 and invoice.total_amount > 0:
                # Calculate the tax ratio from the invoice
                tax_ratio = invoice.total_tax_amount / invoice.total_amount
                # Apply the same ratio to the new payment amount
                new_tax_amount = new_amount * tax_ratio
                new_base_amount = new_amount - new_tax_amount
            else:
                # If no tax is applicable, allocate the entire amount to the base
                new_tax_amount = Decimal('0.00')
                new_base_amount = new_amount

            # Update the allocation amounts
            allocation.allocated_base_amount = new_base_amount
            allocation.allocated_tax_amount = new_tax_amount

            # Update the payment account and tax account (if provided)
            allocation.payment_account = int(request.form.get('fundingAccount'))
            allocation.tax_payable_account_id = int(request.form.get('taxAccount')) if request.form.get(
                'taxAccount') else None
            allocation.payment_mode = new_payment_method  # Update payment_mode

        # Commit changes to the database
        db_session.commit()

        flash("Transaction updated successfully.", "success")
        return redirect(url_for('sales_transaction_details', transaction_id=transaction_id))

    except Exception as e:
        db_session.rollback()
        print(f"An error occurred: {e}")
        flash("An error occurred while updating the transaction.", "error")
        return redirect(url_for('sales_transaction_details', transaction_id=transaction_id))

    finally:
        db_session.close()


@app.route('/sales/transactions')
@login_required
def view_sales_transactions():
    app_id = current_user.app_id
    db_session = Session()

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch filter options from request
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        filter_type = request.args.get('filter_type', 'payment_date')  # Default: payment_date
        status_filter = request.args.get('status')
        filter_applied = bool(start_date or end_date or status_filter)
        # Base queries for invoice-based and direct sales transactions
        invoice_based_query = db_session.query(SalesTransaction).filter(
            SalesTransaction.app_id == app_id
        )

        direct_sales_query = db_session.query(DirectSalesTransaction).filter_by(app_id=app_id)

        # Apply date filters based on filter type
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

                if filter_type == 'payment_date':
                    if start_date:
                        invoice_based_query = invoice_based_query.filter(SalesTransaction.payment_date >= start_date)
                        direct_sales_query = direct_sales_query.filter(
                            DirectSalesTransaction.payment_date >= start_date)
                    if end_date:
                        invoice_based_query = invoice_based_query.filter(SalesTransaction.payment_date <= end_date)
                        direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.payment_date <= end_date)
                elif filter_type == 'invoice_date':
                    if start_date:
                        invoice_based_query = invoice_based_query.filter(
                            SalesTransaction.invoice.invoice_date >= start_date)
                    if end_date:
                        invoice_based_query = invoice_based_query.filter(
                            SalesTransaction.invoice.invoice_date <= end_date)
            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')

        # Apply status filter
        if status_filter:
            if status_filter == 'posted':
                invoice_based_query = invoice_based_query.filter(SalesTransaction.is_posted_to_ledger == True)
                direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.is_posted_to_ledger == True)
            elif status_filter == 'not_posted':
                invoice_based_query = invoice_based_query.filter(SalesTransaction.is_posted_to_ledger == False)
                direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.is_posted_to_ledger == False)

        # Fetch filtered transactions
        invoice_based_transactions = invoice_based_query.all()
        direct_sales_transactions = direct_sales_query.all()

        # Combine both lists into a single list
        combined_transactions = []

        # Process invoice-based transactions
        for transaction in invoice_based_transactions:
            invoice_id = transaction.invoice_id
            total_invoice_amount = transaction.invoice.total_amount if transaction.invoice else 0

            # Calculate total paid for this invoice, excluding cancelled transactions
            total_paid = db_session.query(func.sum(SalesTransaction.amount_paid)) \
                             .filter(SalesTransaction.invoice_id == invoice_id,
                                     SalesTransaction.app_id == app_id,
                                     SalesTransaction.payment_status != SalesPaymentStatus.cancelled) \
                             .scalar() or 0

            # Calculate remaining balance
            remaining_balance = total_invoice_amount - total_paid

            combined_transactions.append({
                'id': transaction.id,
                'type': 'invoice',
                'invoice_number': transaction.invoice.invoice_number if transaction.invoice else None,
                'customer_name': transaction.customer.vendor_name,
                'status': transaction.invoice.status,
                'payment_date': transaction.payment_date,
                'amount_paid': transaction.amount_paid,
                'currency': transaction.currency.user_currency,
                'reference_number': transaction.reference_number,
                'total_invoice_amount': total_invoice_amount,
                'total_paid': total_paid,
                'remaining_balance': remaining_balance,
                'payment_mode': transaction.payment_allocations[0].payment_modes.payment_mode
                if transaction.payment_allocations and transaction.payment_allocations[0].payment_modes
                else None,

                'posted_to_ledger': transaction.is_posted_to_ledger,  # Add ledger posting status
                'payment_progress': (
                    "Cancelled" if transaction.payment_status.value == "cancelled"
                    else "Final" if remaining_balance == 0
                    else "Ongoing" if total_paid > transaction.amount_paid
                    else "Initial"
                )

            })

        # Process direct sales transactions
        for transaction in direct_sales_transactions:
            total_sale_amount = transaction.total_amount  # Assuming this field exists
            total_paid = db_session.query(func.sum(DirectSalesTransaction.amount_paid)) \
                             .filter_by(direct_sale_number=transaction.direct_sale_number, app_id=app_id).scalar() or 0

            remaining_balance = total_sale_amount - total_paid

            combined_transactions.append({
                'id': transaction.id,
                'type': 'direct',
                'invoice_number': None,  # Direct sales don't have invoices
                'direct_sale_number': transaction.direct_sale_number if transaction.direct_sale_number else None,
                'status': transaction.status,
                'customer_name': transaction.customer.vendor_name,
                'payment_date': transaction.payment_date,
                'amount_paid': transaction.amount_paid,
                'currency': transaction.currency.user_currency,
                'reference_number': transaction.sale_reference,
                'total_sale_amount': total_sale_amount,
                'total_paid': total_paid,
                'has_inventory_items': transaction.direct_sale_items[0].item_type,
                'remaining_balance': remaining_balance,
                'payment_mode': transaction.payment_allocations[0].payment_modes.payment_mode
                if transaction.payment_allocations and transaction.payment_allocations[0].payment_modes
                else None,

                'posted_to_ledger': transaction.is_posted_to_ledger,  # Add ledger posting status
                'sale_type': (
                    "Cancelled" if transaction.payment_status.lower() == "cancelled"
                    else "Installment" if remaining_balance > 0
                    else "Full"
                )
            })

        # Close the session
        db_session.close()

        # Render the template with the combined transactions
        return render_template('/sales/sales_transactions.html',
                               sales_transactions=combined_transactions,
                               company=company,
                               role=role,
                               filter_applied=filter_applied,
                               modules=modules_data)

    except Exception as e:
        db_session.close()
        # Log the error and return an error message or redirect to an error page
        print(f"An error occurred: {e}")
        return "An error occurred while processing your request.", 500


@app.route('/cancel_sales_transaction/<int:transaction_id>', methods=['POST'])
@login_required
def cancel_sales_transaction(transaction_id):
    db_session = Session()

    try:
        # Fetch the transaction by ID
        transaction = db_session.query(SalesTransaction).filter_by(id=transaction_id).first()

        if not transaction:
            flash("Transaction not found.", "error")
            return redirect(url_for('view_sales_transactions'))

        # Check if transaction was posted to ledger
        if transaction.is_posted_to_ledger:

            # Create reversal entries for each original transaction
            payment_allocation_ids = [pa.id for pa in transaction.payment_allocations]

            original_transactions = db_session.query(Transaction).filter(
                (Transaction.source_type.in_(["invoice_payment", "direct_sale_payment"])) &
                (Transaction.source_id.in_(payment_allocation_ids))
            ).all()

            journal_number = generate_unique_journal_number(db_session, current_user.app_id)

            for idx, original_txn in enumerate(original_transactions, 1):
                create_transaction(
                    db_session=db_session,
                    transaction_type=original_txn.transaction_type,
                    date=datetime.datetime.now().date(),
                    category_id=original_txn.category_id,
                    subcategory_id=original_txn.subcategory_id,
                    currency=original_txn.currency,
                    amount=original_txn.amount,
                    dr_cr="C" if original_txn.dr_cr == "D" else "D",
                    description=f"Reversal of cancelled transaction {transaction.invoice.invoice_number if transaction.invoice else ''}",
                    payment_mode_id=original_txn.payment_mode_id,
                    vendor_id=original_txn.vendor_id,
                    created_by=current_user.id,
                    project_id=original_txn.project_id,
                    source_type="transaction_reversal",
                    source_id=original_txn.id,
                    app_id=original_txn.app_id,
                    journal_number=journal_number
                )

        # Update the transaction status to "cancelled"
        transaction.payment_status = SalesPaymentStatus.cancelled
        invoice_id = transaction.invoice_id
        # Check if the payment exceeds the invoice balance, filtering by payment_status and excluding refunded and cancelled payments
        total_paid = db_session.query(func.sum(SalesTransaction.amount_paid)).filter(
            SalesTransaction.invoice_id == invoice_id,
            SalesTransaction.payment_status.in_(
                [SalesPaymentStatus.full, SalesPaymentStatus.partial, SalesPaymentStatus.paid]),
            # Use enum values
            SalesTransaction.payment_status != SalesPaymentStatus.refund,  # Exclude 'refund' payments
            SalesTransaction.payment_status != SalesPaymentStatus.cancelled  # Exclude 'cancelled' payments
        ).scalar() or Decimal('0.00')

        remaining_balance = transaction.invoice.total_amount - total_paid
        logger.info(f'Total Paid is {total_paid} for invoice amount {transaction.invoice.total_amount}')
        # Determine the payment status based on total_paid and remaining_balance
        if total_paid == transaction.invoice.total_amount:
            logger.info(f'Total Paid is is equal {total_paid}')
            payment_status = InvoiceStatus.paid
        elif 0 < total_paid < transaction.invoice.total_amount:
            payment_status = InvoiceStatus.partially_paid

        else:
            payment_status = InvoiceStatus.unpaid
        transaction.invoice.status = payment_status

        db_session.commit()

        flash("Transaction cancelled successfully with reversal entries.", "success")
        return redirect(url_for('view_sales_transactions'))

    except Exception as e:
        logger.error(f"\n[ERROR] Exception occurred: {str(e)}")

        db_session.rollback()
        flash("An error occurred while cancelling the transaction.", "error")
        return redirect(url_for('view_sales_transactions'))

    finally:
        db_session.close()


@app.route('/cancel_direct_sales_transaction/<int:transaction_id>', methods=['POST'])
@login_required
def cancel_direct_sales_transaction(transaction_id):
    db_session = Session()

    try:
        # Fetch the transaction by ID
        transaction = db_session.query(DirectSalesTransaction).filter_by(id=transaction_id).first()

        if not transaction:
            flash("Transaction not found.", "error")
            return redirect(url_for('view_sales_transactions'))

        # Check if transaction was posted to ledger
        if transaction.is_posted_to_ledger:

            # Create reversal entries for each original transaction
            payment_allocation_ids = [pa.id for pa in transaction.payment_allocations]

            original_transactions = db_session.query(Transaction).filter(
                (Transaction.source_type.in_(["invoice_payment", "direct_sale_payment"])) &
                (Transaction.source_id.in_(payment_allocation_ids))
            ).all()
            journal_number = generate_unique_journal_number(db_session, current_user.app_id)
            for idx, original_txn in enumerate(original_transactions, 1):
                create_transaction(
                    db_session=db_session,
                    transaction_type=original_txn.transaction_type,
                    date=datetime.datetime.now().date(),
                    category_id=original_txn.category_id,
                    subcategory_id=original_txn.subcategory_id,
                    currency=original_txn.currency,
                    amount=original_txn.amount,
                    dr_cr="C" if original_txn.dr_cr == "D" else "D",
                    description=f"Reversal of cancelled transaction {transaction.direct_sale_number if transaction.direct_sale_number else ''}",
                    payment_mode_id=original_txn.payment_mode_id,
                    vendor_id=original_txn.vendor_id,
                    created_by=current_user.id,
                    project_id=original_txn.project_id,
                    source_type="transaction_reversal",
                    source_id=original_txn.id,
                    app_id=original_txn.app_id,
                    journal_number=journal_number
                )

            # If it's a direct sale with inventory items, reverse those too
            if hasattr(transaction, 'direct_sale_items'):
                inventory_items = [item for item in transaction.direct_sale_items if item.item_type == "inventory"]

                if inventory_items:
                    inventory_item_ids = [item.id for item in inventory_items]

                    inventory_transactions = db_session.query(Transaction).filter(
                        (Transaction.source_type == "direct_sale_item") &
                        (Transaction.source_id.in_(inventory_item_ids))
                    ).all()

                    for idx, inv_txn in enumerate(inventory_transactions, 1):
                        create_transaction(
                            db_session=db_session,
                            transaction_type=inv_txn.transaction_type,
                            date=datetime.datetime.now().date(),
                            category_id=inv_txn.category_id,
                            subcategory_id=inv_txn.subcategory_id,
                            currency=inv_txn.currency,
                            amount=inv_txn.amount,
                            dr_cr="C" if inv_txn.dr_cr == "D" else "D",
                            description=f"Reversal of inventory transaction for cancelled sale {transaction.direct_sale_number}",
                            payment_mode_id=inv_txn.payment_mode_id,
                            vendor_id=inv_txn.vendor_id,
                            created_by=current_user.id,
                            project_id=inv_txn.project_id,
                            source_type="inventory_reversal",
                            source_id=inv_txn.id,
                            app_id=inv_txn.app_id,
                            journal_number=journal_number
                        )

                    # Restore inventory quantities
                    for item in inventory_items:
                        inventory_item = item.inventory_item_variation_link.inventory_item

                        inventory_quantity = db_session.query(BatchVariationLink).filter_by(
                            batch_id=inventory_transaction.lot,
                            location_id=inventory_transaction.to_location,
                            item_id=inventory_item,
                            app_id=current_user.app_id
                        ).first()

                        if inventory_quantity:
                            old_quantity = inventory_quantity.quantity
                            inventory_quantity.quantity += item.quantity

        # Update the transaction status to "cancelled"
        transaction.payment_status = "cancelled"
        transaction.status = OrderStatus.canceled.name
        db_session.commit()

        flash("Transaction cancelled successfully with reversal entries.", "success")
        return redirect(url_for('view_sales_transactions'))

    except Exception as e:
        logger.error(f"\n[ERROR] Exception occurred: {str(e)}")

        db_session.rollback()

        flash("An error occurred while cancelling the transaction.", "error")
        return redirect(url_for('view_sales_transactions'))

    finally:

        db_session.close()


@app.route('/delete_sales_transaction/<int:transaction_id>', methods=['GET'])
@login_required
def delete_sales_transaction(transaction_id):
    app_id = current_user.app_id
    db_session = Session()

    try:
        # Fetch the sales transaction by ID and app_id
        transaction = db_session.query(SalesTransaction).filter_by(id=transaction_id, app_id=app_id).first()

        if not transaction:
            flash('Sales transaction not found.', 'error')
            return redirect(url_for('view_sales_transactions'))

        # Delete the transaction
        db_session.delete(transaction)
        db_session.commit()

        flash('Sales transaction deleted successfully!', 'success')
        return redirect(url_for('view_sales_transactions'))

    except Exception as e:
        db_session.rollback()
        logger.error(f"An error occurred while deleting the sales transaction: {str(e)}")
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(url_for('sales_transaction_details', transaction_id=transaction_id))

    finally:
        db_session.close()


@app.route('/delete_direct_sales_transaction/<int:transaction_id>', methods=['GET'])
@login_required
def delete_direct_sales_transaction(transaction_id):
    app_id = current_user.app_id
    db_session = Session()
    logger.info(f'Transaction ID is {transaction_id}')
    try:
        # Fetch the sales transaction by ID and app_id
        transaction = db_session.query(DirectSalesTransaction).filter_by(id=transaction_id, app_id=app_id).first()

        if not transaction:
            flash('Sales transaction not found.', 'error')
            return redirect(url_for('view_sales_transactions'))

        # Delete the transaction
        db_session.delete(transaction)
        db_session.commit()

        flash('Sales transaction deleted successfully!', 'success')
        return redirect(url_for('view_sales_transactions'))

    except Exception as e:
        db_session.rollback()
        logger.error(f"An error occurred while deleting the sales transaction: {str(e)}")
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(url_for('sales_transaction_details', transaction_id=transaction_id))

    finally:
        db_session.close()


@app.route('/post_sales_transaction_to_ledger', methods=['POST'])
@login_required
def post_sales_transaction_to_ledger():
    db_session = Session()
    try:
        transaction_id = request.form.get('ledgerTransactionId')
        transaction_type = request.form.get('transactionType')  # 'invoice' or 'direct'
        income_account_id = request.form.get('incomeAccount')
        income_account_category = request.form.get('incomeAccountCategory')

        # Get inventory accounts only for direct sales with inventory items
        inventory_account_id = None
        inventory_account_category = None
        cogs_account_id = None
        cogs_account_category = None
        if transaction_type == 'direct':
            inventory_account_id = request.form.get('inventoryAccount')
            inventory_account_category = request.form.get('inventoryAccountCategory')
            cogs_account_id = request.form.get('cogsAccount')
            cogs_account_category = request.form.get('cogsAccountCategory')

            # Handle direct sale transaction
            direct_sale = db_session.query(DirectSalesTransaction).get(transaction_id)
            if not direct_sale:
                flash("Direct sale not found", "error"), 404
                return redirect(request.referrer)

            if direct_sale.is_posted_to_ledger:
                flash("Direct sale already posted to ledger", "error"), 400
                return redirect(request.referrer)

            # Check for unposted payment allocations
            unposted_allocations = [pa for pa in direct_sale.payment_allocations if not pa.is_posted_to_ledger]

            if not unposted_allocations:
                flash("No unposted payment allocations found", "error"), 400
                return redirect(request.referrer)

            # Post payment allocations to ledger
            for allocation in unposted_allocations:
                # Generate one journal number per allocation
                journal_number = generate_unique_journal_number(db_session, current_user.app_id)

                currency_id = direct_sale.currency_id

                exchange_rate_id, notification = resolve_exchange_rate_for_transaction(session=db_session,
                                                                                       currency_id=currency_id,
                                                                                       transaction_date=allocation.payment_date,
                                                                                       app_id=current_user.app_id)

                # Debit Cash/Bank account
                create_transaction(
                    db_session=db_session,
                    transaction_type="Asset",
                    date=allocation.payment_date,
                    category_id=allocation.chart_of_accounts_asset.category_fk,
                    subcategory_id=allocation.payment_account,
                    currency=direct_sale.currency_id,
                    amount=allocation.allocated_base_amount + allocation.allocated_tax_amount,
                    dr_cr="D",
                    description=f"Payment received for direct sale {direct_sale.direct_sale_number}",
                    payment_mode_id=allocation.payment_mode,
                    vendor_id=direct_sale.customer_id,
                    created_by=current_user.id,
                    project_id=direct_sale.project_id,
                    source_type="direct_sale_payment",
                    source_id=allocation.id,
                    app_id=direct_sale.app_id,
                    exchange_rate_id=exchange_rate_id,
                    journal_number=journal_number
                )

                # Credit Revenue account (using the selected income account)
                create_transaction(
                    db_session=db_session,
                    transaction_type="Income",
                    date=allocation.payment_date,
                    category_id=income_account_category,
                    subcategory_id=income_account_id,
                    currency=direct_sale.currency_id,
                    amount=allocation.allocated_base_amount,
                    dr_cr="C",
                    description=f"Revenue from direct sale {direct_sale.direct_sale_number}",
                    vendor_id=direct_sale.customer_id,
                    payment_mode_id=allocation.payment_mode,
                    created_by=direct_sale.created_by,
                    source_type="direct_sale_payment",
                    source_id=allocation.id,
                    app_id=direct_sale.app_id,
                    project_id=direct_sale.project_id,
                    exchange_rate_id=exchange_rate_id,
                    journal_number=journal_number
                )

                # Credit Tax Payable account if tax exists
                if allocation.allocated_tax_amount > 0 and allocation.tax_payable_account_id:
                    create_transaction(
                        db_session=db_session,
                        transaction_type="Liability",
                        date=allocation.payment_date,
                        category_id=allocation.chart_of_accounts_tax.category_fk,
                        subcategory_id=allocation.tax_payable_account_id,
                        currency=direct_sale.currency_id,
                        amount=allocation.allocated_tax_amount,
                        dr_cr="C",
                        description=f"Tax collected for direct sale {direct_sale.direct_sale_number}",
                        vendor_id=direct_sale.customer_id,
                        payment_mode_id=allocation.payment_mode,
                        project_id=direct_sale.project_id,
                        created_by=direct_sale.created_by,
                        source_type="direct_sale_payment",
                        source_id=allocation.id,
                        app_id=direct_sale.app_id,
                        exchange_rate_id=exchange_rate_id,
                        journal_number=journal_number
                    )

                allocation.is_posted_to_ledger = True

            # Post inventory items to ledger if any inventory items exist
            # Post inventory items to ledger if any inventory items exist
            if any(item.item_type == "inventory" for item in direct_sale.direct_sale_items):
                if not inventory_account_id or not cogs_account_id:
                    return jsonify(success=False,
                                   message="Inventory and COGS accounts are required for inventory items"), 400

                for item in direct_sale.direct_sale_items:

                    if item.item_type == "inventory":
                        try:
                            # CHANGE 1: Now returns only cogs_details without total_cogs
                            cogs_details = calculate_fifo_cogs(
                                db_session=db_session,
                                item_id=item.inventory_item_variation_link.id,
                                quantity_sold=item.quantity,
                                app_id=direct_sale.app_id,
                                source_type="direct_sale",
                                source_id=direct_sale.id,
                                customer_id=direct_sale.customer_id,
                                invoice_location_id=item.location_id,  # Use item-level location
                                uom=item.uom
                            )

                            # CHANGE 2: Process each batch separately
                            for batch in cogs_details:
                                currency_id = batch['currency_id']

                                exchange_rate_id, notification = resolve_exchange_rate_for_transaction(
                                    session=db_session,
                                    currency_id=currency_id,
                                    transaction_date=direct_sale.payment_date,
                                    app_id=current_user.app_id)
                                batch_journal_number = generate_unique_journal_number(db_session, current_user.app_id)

                                batch_cost = Decimal(str(batch['unit_cost'])) * Decimal(str(batch['deduct_quantity']))

                                # Debit COGS account in original currency
                                create_transaction(
                                    db_session=db_session,
                                    transaction_type="Expense",
                                    date=direct_sale.payment_date,
                                    category_id=cogs_account_category,
                                    subcategory_id=cogs_account_id,
                                    currency=batch['currency_id'],  # Original currency
                                    amount=float(batch_cost),
                                    dr_cr="D",
                                    description=f"COGS (Batch {batch['batch_id']}) for direct sale {direct_sale.direct_sale_number}",
                                    vendor_id=direct_sale.customer_id,
                                    created_by=direct_sale.created_by,
                                    source_type="direct_sale_item",
                                    payment_mode_id=None,
                                    project_id=direct_sale.project_id,
                                    source_id=item.id,
                                    app_id=direct_sale.app_id,
                                    exchange_rate_id=exchange_rate_id,
                                    journal_number=batch_journal_number
                                )

                                # Credit Inventory account in original currency
                                create_transaction(
                                    db_session=db_session,
                                    transaction_type="Asset",
                                    date=direct_sale.payment_date,
                                    category_id=inventory_account_category,
                                    subcategory_id=inventory_account_id,
                                    currency=batch['currency_id'],  # Original currency
                                    amount=float(batch_cost),
                                    dr_cr="C",
                                    description=f"Inventory Reduction (Batch {batch['batch_id']}) for direct sale {direct_sale.direct_sale_number}",
                                    vendor_id=direct_sale.customer_id,
                                    payment_mode_id=None,
                                    project_id=direct_sale.project_id,
                                    created_by=current_user.id,
                                    source_type="direct_sale_item",
                                    source_id=item.id,
                                    app_id=direct_sale.app_id,
                                    exchange_rate_id=exchange_rate_id,
                                    journal_number=batch_journal_number
                                )

                            # Update inventory quantity (unchanged)
                            update_inventory_quantity(
                                db_session=db_session,
                                item_id=item.inventory_item_variation_link.inventory_item.id,
                                quantity_sold=item.quantity,
                                sales_invoice_id=None,
                                created_by=direct_sale.created_by,
                                app_id=direct_sale.app_id,
                                cogs_details=cogs_details,
                                date=direct_sale.payment_date
                            )

                        except Exception as e:
                            db_session.rollback()
                            flash(f"Error processing inventory item: {str(e)}", "error"), 400
                            return redirect(request.referrer)

            direct_sale.is_posted_to_ledger = True
            db_session.commit()
            flash('Direct sale posted to ledger successfully', 'success')
            return redirect(request.referrer)


        elif transaction_type == 'invoice':
            # Handle invoice payment transaction
            sales_transaction = db_session.query(SalesTransaction).get(transaction_id)
            if not sales_transaction:
                flash("Sales transaction not found", "error"), 404
                return redirect(request.referrer)

            if sales_transaction.is_posted_to_ledger:
                flash("Transaction already posted to ledger", "error"), 400
                return redirect(request.referrer)

            # Check for unposted payment allocations
            unposted_allocations = [pa for pa in sales_transaction.payment_allocations if not pa.is_posted_to_ledger]

            if not unposted_allocations:
                return jsonify(success=False, message="No unposted payment allocations found"), 400

            # Post payment allocations to ledger
            for allocation in unposted_allocations:
                invoice_journal_number = generate_unique_journal_number(db_session, current_user.app_id)

                currency_id = sales_transaction.currency_id

                exchange_rate_id, notification = resolve_exchange_rate_for_transaction(session=db_session,
                                                                                       currency_id=currency_id,
                                                                                       transaction_date=allocation.payment_date,
                                                                                       app_id=current_user.app_id)

                # Debit Cash/Bank account
                create_transaction(
                    db_session=db_session,
                    transaction_type="Asset",
                    date=allocation.payment_date,
                    category_id=allocation.chart_of_accounts_asset.category_fk,
                    subcategory_id=allocation.payment_account,
                    currency=sales_transaction.currency_id,
                    amount=allocation.allocated_base_amount + allocation.allocated_tax_amount,
                    dr_cr="D",
                    description=f"Payment received for invoice {sales_transaction.invoice.invoice_number}",
                    payment_mode_id=allocation.payment_mode,
                    vendor_id=sales_transaction.customer_id,
                    created_by=sales_transaction.created_by,
                    source_type="invoice_payment",
                    source_id=allocation.id,
                    project_id=sales_transaction.invoice.project_id,
                    app_id=sales_transaction.app_id,
                    exchange_rate_id=exchange_rate_id,
                    journal_number=invoice_journal_number
                )

                # Credit Account Receivable (modified to handle missing tax account)
                receivable_amount = allocation.allocated_base_amount
                tax_amount = allocation.allocated_tax_amount

                # Check if tax should be posted to Receivable instead
                tax_payable_account = db_session.query(Transaction).filter_by(
                    source_type="sales_invoice_tax",
                    source_id=sales_transaction.invoice.id,
                    dr_cr="C",
                    transaction_type="Liability").first()

                post_tax_to_receivable = (tax_amount > 0 and not tax_payable_account)

                if post_tax_to_receivable:
                    receivable_amount += tax_amount  # Combine base + tax

                # Credit Account Receivable account (using the selected income account)
                account_receivable_account = db_session.query(Transaction).filter_by(
                    source_type="sales_invoice_receivable",
                    source_id=sales_transaction.invoice_id,
                    dr_cr="D",
                    transaction_type="Asset").first()

                if not account_receivable_account:
                    flash(
                        f"Please ensure that the related invoice {sales_transaction.invoice.invoice_number} is first posted to ledger before proceeding",
                        "warning")
                    return redirect(request.referrer)

                create_transaction(
                    db_session=db_session,
                    transaction_type="Asset",
                    date=allocation.payment_date,
                    category_id=account_receivable_account.category_id,
                    subcategory_id=account_receivable_account.subcategory_id,
                    currency=sales_transaction.currency_id,
                    amount=receivable_amount,
                    dr_cr="C",
                    description=f"Revenue from invoice {sales_transaction.invoice.invoice_number}",
                    vendor_id=sales_transaction.customer_id,
                    created_by=current_user.id,
                    source_type="invoice_payment",
                    source_id=allocation.id,
                    payment_mode_id=allocation.payment_mode,
                    app_id=sales_transaction.app_id,
                    project_id=sales_transaction.invoice.project_id,
                    exchange_rate_id=exchange_rate_id,
                    journal_number=invoice_journal_number
                )

                # Credit Tax Payable account if tax exists

                if tax_amount > 0 and tax_payable_account and not post_tax_to_receivable:
                    create_transaction(
                        db_session=db_session,
                        transaction_type="Liability",
                        date=allocation.payment_date,
                        category_id=tax_payable_account.chart_of_accounts.category_fk,
                        subcategory_id=tax_payable_account.subcategory_id,
                        currency=sales_transaction.currency_id,
                        amount=allocation.allocated_tax_amount,
                        dr_cr="C",
                        description=f"Tax collected for invoice {sales_transaction.invoice.invoice_number}",
                        vendor_id=sales_transaction.customer_id,
                        created_by=sales_transaction.created_by,
                        source_type="invoice_payment",
                        source_id=allocation.id,
                        payment_mode_id=None,
                        project_id=sales_transaction.invoice.project_id,
                        app_id=sales_transaction.app_id,
                        exchange_rate_id=exchange_rate_id,
                        journal_number=invoice_journal_number
                    )

                allocation.is_posted_to_ledger = True

            sales_transaction.is_posted_to_ledger = True
            db_session.commit()
            flash("Invoice payment posted to ledger successfully", "success")
            return redirect(request.referrer)

        else:
            flash("Invalid transaction type", "error"), 400
            return redirect(request.referrer)

    except Exception as e:
        db_session.rollback()
        flash(f"Error posting to ledger: {str(e)}", "error"), 500
        return redirect(request.referrer)
    finally:
        db_session.close()


#************************** END SALES MODULE ********************


# ***************************PURCHASE ORDER MODULE ******************************
# Purchase Order Management Route
@app.route('/purchases/purchase_orders', methods=['GET', 'POST'])
@login_required
def purchase_order_management():
    app_id = current_user.app_id
    db_session = Session()

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        # Base query
        query = db_session.query(PurchaseOrder).filter_by(app_id=app_id)

        # Process filter options
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        filter_type = request.args.get('filter_type', 'purchase_order_date')  # Default: purchase_order_date
        status_filter = request.args.get('status')
        filter_applied = bool(start_date or end_date or status_filter)
        # Apply date filters based on filter type
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

                if filter_type == 'purchase_order_date':
                    if start_date:
                        query = query.filter(PurchaseOrder.purchase_order_date >= start_date)
                    if end_date:
                        query = query.filter(PurchaseOrder.purchase_order_date <= end_date)
                elif filter_type == 'expiry_date':
                    if start_date:
                        query = query.filter(PurchaseOrder.delivery_date >= start_date)
                    if end_date:
                        query = query.filter(PurchaseOrder.delivery_date <= end_date)
            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')

        # Apply status filter
        if status_filter:
            query = query.filter(PurchaseOrder.status == status_filter)

        # Check for expired purchase orders
        current_date = datetime.datetime.now()
        expired_purchase_orders = query.filter(
            and_(
                PurchaseOrder.delivery_date.isnot(None),  # Ensure delivery_date is not None
                PurchaseOrder.delivery_date < current_date,
                PurchaseOrder.status == OrderStatus.approved
            )
        ).all()

        # Mark expired purchase orders as expired
        for purchase_order in expired_purchase_orders:
            purchase_order.status = OrderStatus.expired
            db_session.commit()

        # Order by latest created_at
        purchase_orders = query.order_by(PurchaseOrder.created_at.desc()).all()

        # If no purchase orders exist, return a message
        if not purchase_orders:
            flash('No Purchase Orders found. Add a new purchase order to get started.', 'info')

        for po in purchase_orders:
            # Check for inventory items
            po.has_inventory_item = any(
                item.item_type == "inventory" for item in po.purchase_order_items
            )

            # Check for unposted purchase transactions
            has_unposted_purchase_tx = any(
                tx.is_posted_to_ledger is False for tx in po.purchase_transactions
            )

            # Check for unposted goods receipts
            has_unposted_goods_receipt = any(
                gr.is_posted_to_ledger is False for gr in po.goods_receipts
            )

            has_unposted_shipping_handling = not po.shipping_handling_posted

            # Combine both
            po.has_unposted_transaction = has_unposted_purchase_tx or has_unposted_goods_receipt or has_unposted_shipping_handling

        # Calculate dashboard metrics
        total_purchase_orders = query.count()

        approved_purchase_orders = query.filter(PurchaseOrder.status == OrderStatus.approved).count()
        received_purchase_orders = query.filter(PurchaseOrder.status == OrderStatus.received).count()
        cancelled_purchase_orders = query.filter(PurchaseOrder.status == OrderStatus.canceled).count()
        overdue_purchase_orders = query.filter(PurchaseOrder.status == OrderStatus.expired).count()

        return render_template(
            '/purchases/purchase_orders.html',
            purchase_orders=purchase_orders,
            total_purchase_orders=total_purchase_orders,
            approved_purchase_orders=approved_purchase_orders,
            received_purchase_orders=received_purchase_orders,
            cancelled_purchase_orders=cancelled_purchase_orders,
            overdue_purchase_orders=overdue_purchase_orders,
            company=company,
            role=role,
            filter_applied=filter_applied,
            modules=modules_data
        )
    except Exception as e:
        flash(f'An error occurred: {str(e)}', 'error')
        logger.error(f'Error is: {e}')
        return redirect(url_for('purchase_order_management'))
    finally:
        db_session.close()


@app.route('/purchases/send_to_customer/<int:purchase_order_id>', methods=['POST'])
def send_purchase_order(purchase_order_id):
    db_session = Session()
    try:
        purchase_order = db_session.query(PurchaseOrder).get(purchase_order_id)

        if purchase_order and purchase_order.status.name not in ['draft', 'canceled', 'rejected']:
            purchase_order.status = OrderStatus.unpaid
            db_session.commit()
            flash('Purchase Order sent to customer successfully!', 'success')
        else:
            flash('Purchase Order cannot be sent.', 'error')
    except Exception as e:
        db_session.rollback()
        flash(f'An error occurred: {str(e)}', 'error')
    finally:
        db_session.close()  # Ensure session is closed

    return redirect(request.referrer)


@app.route('/cancel_purchase_order', methods=['POST'])
@login_required
def cancel_purchase_order():
    purchase_order_id = request.form.get('purchase_order_id')

    try:
        if not purchase_order_id:
            flash("Purchase Order ID is required", "error")
            return redirect(url_for('purchase_order_management'))

        with Session() as db_session:
            # Fetch purchase order with related data in single query
            purchase_order = (
                db_session.query(PurchaseOrder)
                .options(
                    joinedload(PurchaseOrder.purchase_transactions),
                    joinedload(PurchaseOrder.goods_receipts),
                    joinedload(PurchaseOrder.purchase_order_history)
                )
                .filter_by(id=purchase_order_id, app_id=current_user.app_id)
                .first()
            )

            if not purchase_order:
                flash("Purchase Order not found", "error")
                return redirect(url_for('purchase_order_management'))

            # Check cancellation restrictions
            cancellation_errors = []

            # 1. Check if goods have been received
            if any(receipt.quantity_received > 0 for receipt in purchase_order.goods_receipts):
                cancellation_errors.append("Goods have already been received")

            # 2. Check if payments have been made
            if any(txn.amount_paid > 0 for txn in purchase_order.purchase_transactions):
                cancellation_errors.append("Payments have already been made")

            # 3. Check if any transactions posted to ledger
            if any(
                    alloc.is_posted_to_ledger
                    for txn in purchase_order.purchase_transactions
                    for alloc in txn.payment_allocations
            ):
                cancellation_errors.append("Transactions have been posted to ledger")

            if cancellation_errors:
                flash(f"Cannot cancel: {', '.join(cancellation_errors)}", "error")
                return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

            # Store previous status for history
            previous_status = purchase_order.status

            # Update status
            if purchase_order.status == OrderStatus.approved:
                purchase_order.status = OrderStatus.rejected
            else:
                purchase_order.status = OrderStatus.canceled

            # Create history record
            history_entry = PurchaseOrderHistory(
                purchase_order_id=purchase_order.id,
                changed_by=current_user.id,
                change_date=func.now(),
                change_description=(
                    f"Status changed from {previous_status} to {purchase_order.status} "
                    f"(Cancellation requested by {current_user.username})"
                ),
                app_id=current_user.app_id
            )
            db_session.add(history_entry)

            db_session.commit()
            flash(f"Purchase Order {purchase_order.purchase_order_number} cancelled successfully", "success")
            return redirect(url_for('purchase_order_management'))

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error cancelling PO {purchase_order_id}: {str(e)}", exc_info=True)
        flash(f"An error occurred: {str(e)}", "error")
        return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))


@app.route('/delete_purchase_order/<int:purchase_order_id>', methods=['POST'])
@login_required
def delete_purchase_order(purchase_order_id):
    app_id = current_user.app_id
    db_session = Session()
    purchase_order = db_session.query(PurchaseOrder).filter_by(id=purchase_order_id, app_id=app_id).first()

    try:
        db_session.delete(purchase_order)
        db_session.commit()
        flash('Purchase Order deleted successfully!', 'success')
    except Exception as e:
        db_session.rollback()
        flash(f'An error occurred: {str(e)}', 'error')

    return redirect(url_for('purchase_order_management'))


@app.route('/cancel_direct_purchase', methods=['POST'])
@login_required
def cancel_direct_purchase():
    direct_purchase_id = request.form.get('direct_purchase_id')

    try:
        if not direct_purchase_id:
            flash("Direct Purchase ID is required", "error")
            return redirect(url_for('direct_purchase_management'))

        with Session() as db_session:
            # Fetch direct purchase with related data in single query
            direct_purchase = (
                db_session.query(DirectPurchaseTransaction)
                .options(
                    joinedload(DirectPurchaseTransaction.payment_allocations),
                    joinedload(DirectPurchaseTransaction.goods_receipts)
                )
                .filter_by(id=direct_purchase_id, app_id=current_user.app_id)
                .first()
            )

            if not direct_purchase:
                flash("Direct Purchase not found", "error")
                return redirect(url_for('direct_purchase_management'))

            # Check cancellation restrictions
            cancellation_errors = []

            # 1. Check if goods have been received
            if any(receipt.quantity_received > 0 for receipt in direct_purchase.goods_receipts):
                cancellation_errors.append("Goods have already been received")

            # 2. Check if payments have been made
            if direct_purchase.amount_paid > 0:
                cancellation_errors.append("Payments have already been made")

            # 3. Check if any transactions posted to ledger
            if any(alloc.is_posted_to_ledger for alloc in direct_purchase.payment_allocations):
                cancellation_errors.append("Transactions have been posted to ledger")

            if cancellation_errors:
                flash(f"Cannot cancel: {', '.join(cancellation_errors)}", "error")
                return redirect(url_for('purchases.direct_purchase_details', direct_purchase_id=direct_purchase_id))

            # Store previous status for history
            previous_status = direct_purchase.status

            # Update status
            if direct_purchase.status == OrderStatus.approved:
                direct_purchase.status = OrderStatus.rejected
            else:
                direct_purchase.status = OrderStatus.canceled

            db_session.commit()
            flash(f"Direct Purchase {direct_purchase.direct_purchase_number} cancelled successfully", "success")
            return redirect(url_for('direct_purchase_management'))

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error cancelling Direct Purchase {direct_purchase_id}: {str(e)}", exc_info=True)
        flash(f"An error occurred: {str(e)}", "error")
        return redirect(url_for('direct_purchase_details', direct_purchase_id=direct_purchase_id))


def calculate_allocations(items, receipt_items, purchase_record):
    allocations = []

    # Map for quick lookup
    receipt_item_map = {
        ri.direct_purchase_item_id or ri.purchase_order_item_id: ri
        for ri in receipt_items
    }

    total_order_value = sum(Decimal(str(item.total_price or 0)) for item in items)
    if total_order_value == 0:
        raise ValueError("Total order value is zero.")

    purchase_level_tax = Decimal(str(purchase_record.total_tax_amount or 0)) - sum(
        Decimal(str(item.tax_amount or 0)) for item in items
    )
    purchase_level_discount = Decimal(str(purchase_record.calculated_discount_amount or 0))

    for item in items:
        receipt_item = receipt_item_map.get(item.id)
        if not receipt_item:
            continue

        ordered_qty = Decimal(str(item.quantity))
        received_qty = Decimal(str(receipt_item.quantity_received or 0))

        if ordered_qty <= 0 or received_qty <= 0:
            continue

        # Receipt ratio
        receipt_ratio = received_qty / ordered_qty

        # Base allocated amount for this receipt
        line_total = Decimal(str(item.total_price or 0))
        allocated_amount = line_total * receipt_ratio

        # Now allocate tax and discount proportionally
        share_ratio = allocated_amount / total_order_value

        allocated_tax = purchase_level_tax * share_ratio
        allocated_discount = purchase_level_discount * share_ratio

        allocations.append({
            "item_id": item.id,
            "allocated_amount": allocated_amount.quantize(Decimal('0.00')),
            "allocated_tax": allocated_tax.quantize(Decimal('0.00')),
            "allocated_discount": allocated_discount.quantize(Decimal('0.00'))
        })

    return allocations


@app.route('/add_goods_receipt', methods=['POST'])
@login_required
def add_goods_receipt():
    db_session = Session()
    try:
        data = request.form
        app_id = current_user.app_id
        purchase_order_id = data.get('purchase_order_id')
        direct_purchase_id = data.get('direct_purchase_id')

        if not purchase_order_id and not direct_purchase_id:
            db_session.close()
            flash('Either Purchase Order ID or Direct Purchase ID is required', 'error')
            return redirect(request.referrer)

        # Validate receipt date
        receipt_date_str = data.get('receiptDate')
        try:
            receipt_date = datetime.datetime.strptime(receipt_date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            db_session.close()
            flash('Invalid receipt date format (YYYY-MM-DD required)', 'error')
            return redirect(request.referrer)

        # Validate received by
        received_by = data.get('receivedBy', '').strip()
        if not received_by:
            db_session.close()
            flash('Received By is required', 'error')
            return redirect(request.referrer)

        # Get purchase record
        if purchase_order_id:
            purchase_record = db_session.query(PurchaseOrder).filter_by(
                id=purchase_order_id,
                app_id=app_id
            ).options(
                joinedload(PurchaseOrder.purchase_order_items)
            ).first()
            item_relationship = 'purchase_order_items'
        else:
            purchase_record = db_session.query(DirectPurchaseTransaction).filter_by(
                id=direct_purchase_id,
                app_id=app_id
            ).options(
                joinedload(DirectPurchaseTransaction.direct_purchase_items)
            ).first()
            item_relationship = 'direct_purchase_items'

        if not purchase_record:
            db_session.close()
            flash('Purchase record not found', 'error')
            return redirect(request.referrer)

        # Create goods receipt
        receipt_number = generate_next_goods_receipt_number(db_session, app_id)
        receipt = GoodsReceipt(
            receipt_number=receipt_number,
            purchase_order_id=purchase_order_id,
            direct_purchase_id=direct_purchase_id,
            receipt_date=receipt_date,
            received_by=received_by,
            quantity_received=Decimal('0'),
            is_complete_receipt=False,
            is_posted_to_ledger=False,
            inventory_received=False,
            inventory_posted=False,
            app_id=app_id
        )
        db_session.add(receipt)
        db_session.flush()

        # Process received items
        total_received = Decimal('0')
        has_inventory = False
        items = getattr(purchase_record, item_relationship)
        receipt_items = []

        for item in items:
            item_id = str(item.id)
            quantity_received = data.get(f'quantity_received_{item_id}')
            condition = data.get(f'condition_{item_id}', 'Good')
            notes = data.get(f'notes_{item_id}', '').strip() or None

            try:
                qty = Decimal(quantity_received) if quantity_received else Decimal('0')
                if qty <= Decimal('0'):
                    continue

                # Validate quantity doesn't exceed ordered
                item_quantity = Decimal(str(item.quantity))
                total_received_so_far = db_session.query(
                    func.coalesce(func.sum(GoodsReceiptItem.quantity_received), 0)
                ).filter(
                    GoodsReceiptItem.app_id == app_id,
                    (GoodsReceiptItem.purchase_order_item_id == item.id) if purchase_order_id else
                    (GoodsReceiptItem.direct_purchase_item_id == item.id)
                ).scalar()

                if qty + total_received_so_far > item_quantity:
                    db_session.rollback()
                    flash(f"Received quantity for {item.item_name} exceeds ordered quantity", 'error')
                    return redirect(request.referrer)

                # Create receipt item
                receipt_item = GoodsReceiptItem(
                    goods_receipt_id=receipt.id,
                    purchase_order_item_id=item.id if purchase_order_id else None,
                    direct_purchase_item_id=item.id if direct_purchase_id else None,
                    quantity_received=qty,
                    allocated_amount=Decimal('0'),
                    allocated_tax_amount=Decimal('0'),
                    received_condition=condition,
                    notes=notes,
                    inventory_adjusted=False,
                    is_posted_to_ledger=False,
                    app_id=app_id
                )
                db_session.add(receipt_item)
                receipt_items.append((item, receipt_item))
                total_received += qty

                if item.item_type == 'inventory':
                    has_inventory = True

            except (ValueError, TypeError, InvalidOperation) as e:
                db_session.rollback()
                flash(f"Invalid quantity for item {item.item_name}: {str(e)}", 'error')
                return redirect(request.referrer)

        if total_received == Decimal('0'):
            db_session.rollback()
            flash('At least one item must be received', 'error')
            return redirect(request.referrer)

        # Calculate allocations
        allocations = calculate_allocations(
            items=items,
            receipt_items=[ri for (_, ri) in receipt_items],
            purchase_record=purchase_record
        )

        # Apply allocations
        for item, receipt_item in receipt_items:
            alloc = next((a for a in allocations if a['item_id'] == item.id), None)
            if alloc:
                receipt_item.allocated_amount = alloc['allocated_amount'] - alloc['allocated_discount']
                receipt_item.allocated_tax_amount = alloc['allocated_tax']

        # Update receipt totals
        receipt.quantity_received = total_received
        receipt.inventory_received = has_inventory
        db_session.commit()

        # Check if this is final receipt
        total_ordered = sum(Decimal(str(item.quantity)) for item in items)
        if purchase_order_id:
            overall_received = get_total_received_for_purchase(purchase_order_id, db_session, False)
        else:
            overall_received = get_total_received_for_purchase(direct_purchase_id, db_session, True)

        receipt.is_complete_receipt = (overall_received >= total_ordered)

        if receipt.is_complete_receipt:
            total_line_tax = sum(Decimal(str(item.tax_amount or '0')) for item in items)
            purchase_level_tax = Decimal(str(purchase_record.total_tax_amount or '0')) - total_line_tax

            # Get ALL previously allocated taxes (from this and other receipts)
            if purchase_order_id:
                query = db_session.query(
                    func.sum(GoodsReceiptItem.allocated_tax_amount)
                ).join(
                    GoodsReceipt,
                    GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id
                ).filter(
                    GoodsReceiptItem.purchase_order_item_id.in_([item.id for item in items]),
                    GoodsReceipt.app_id == app_id
                )
            else:
                query = db_session.query(
                    func.sum(GoodsReceiptItem.allocated_tax_amount)
                ).join(
                    GoodsReceipt,
                    GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id
                ).filter(
                    GoodsReceiptItem.direct_purchase_item_id.in_([item.id for item in items]),
                    GoodsReceipt.app_id == app_id
                )

            total_allocated_so_far = query.scalar() or Decimal('0')

            # Calculate difference
            tax_diff = purchase_level_tax - total_allocated_so_far

            # Apply adjustment to the last item in current receipt
            if abs(tax_diff) > Decimal('0.01') and receipt.receipt_items:
                last_item = receipt.receipt_items[-1]
                if last_item.allocated_tax_amount is None:
                    last_item.allocated_tax_amount = Decimal('0')

                last_item.allocated_tax_amount += tax_diff
                db_session.flush()

                # Verify the adjustment
                new_total = (query.scalar() or Decimal('0'))
                if abs(new_total - purchase_level_tax) > Decimal('0.01'):
                    raise ValueError("Tax allocation failed to balance")

        db_session.commit()
        flash('Goods receipt created successfully', 'success')
        return redirect(request.referrer)

    except SQLAlchemyError as e:
        db_session.rollback()
        flash(f"Database error: {str(e)}", 'error')
        return redirect(request.referrer)

    except Exception as e:
        db_session.rollback()
        flash(f"Unexpected error: {str(e)}", 'error')
        return redirect(request.referrer)

    finally:
        db_session.close()


@app.route('/post_goods_receipt_to_ledger', methods=['POST'])
@login_required
def post_goods_receipt_to_ledger():
    db_session = Session()
    try:
        # Get form data
        receipt_id = request.form.get('receiptId')

        payable_account_id = request.form.get('receiptPayableAccount')
        payable_account_category = request.form.get('receiptPayableAccountCategory')
        inventory_account_id = request.form.get('inventoryAccount', None)
        inventory_account_category = request.form.get('inventoryAccountCategory')
        expense_account_id = request.form.get('expenseAccount', None)
        expense_account_category = request.form.get('expenseAccountCategory', None)
        service_account_id = request.form.get('serviceExpenseAccount', None)
        service_account_category = request.form.get('serviceExpenseAccountCategory', None)

        # Get the goods receipt item
        goods_receipt_item = db_session.query(GoodsReceiptItem).filter_by(id=receipt_id).first()
        if not goods_receipt_item:
            flash("Goods receipt item not found", "error")
            return redirect(request.referrer)

        # Check if already posted
        if goods_receipt_item.is_posted_to_ledger:
            flash("Goods receipt item already posted to ledger", "error")
            return redirect(url_for("purchase_order_details",
                                    purchase_order_id=goods_receipt_item.purchase_order_item.purchase_orders.id))

        # Get related records
        goods_receipt = goods_receipt_item.goods_receipt
        purchase_order_item = goods_receipt_item.purchase_order_item
        purchase_order = purchase_order_item.purchase_orders

        # Use Decimal for accuracy
        allocated_amount = Decimal(goods_receipt_item.allocated_amount or 0)
        allocated_tax_amount = Decimal(goods_receipt_item.allocated_tax_amount or 0)
        gross_amount = allocated_amount + allocated_tax_amount

        # Determine accounts based on item type
        if goods_receipt_item.purchase_order_item.item_type == "inventory":
            debit_account_id = inventory_account_id
            debit_account_category = inventory_account_category
            source_type = "goods_receipt_inventory"

        elif goods_receipt_item.purchase_order_item.item_type == "service":
            debit_account_id = service_account_id
            debit_account_category = service_account_category
            source_type = "goods_receipt_service"

        else:  # non-inventory
            debit_account_id = expense_account_id
            debit_account_category = expense_account_category
            source_type = "goods_receipt_expense"

        # Get all available prepaid payments with balances
        prepaid_payments = get_prepaid_payments_to_apply(goods_receipt_item, db_session)

        # Apply prepaid payments to cover the gross amount
        applied_prepaid, payable_amount = apply_prepaid_payments(
            goods_receipt_item,
            gross_amount,
            prepaid_payments,
            db_session
        )

        description = f"Goods receipt {goods_receipt.receipt_number} for {purchase_order_item.item_name or purchase_order_item.inventory_item_variation_link.inventory_item.item_name}"

        # ========== TRANSACTION POSTING ========== #
        transactions = []
        currency_id = purchase_order.currency

        exchange_rate_id, notification = resolve_exchange_rate_for_transaction(session=db_session,
                                                                               currency_id=currency_id,
                                                                               transaction_date=goods_receipt.receipt_date,
                                                                               app_id=current_user.app_id)

        journal_number = generate_unique_journal_number(db_session, current_user.app_id)
        payment_mode_id = getattr(goods_receipt_item, 'payment_mode', None)

        # 1. Debit the appropriate account
        transactions.append({
            'type': "Asset" if goods_receipt_item.purchase_order_item.item_type == "inventory" else "Expense",
            'date': goods_receipt.receipt_date,
            'category_id': debit_account_category,
            'subcategory_id': debit_account_id,
            'amount': gross_amount,
            'dr_cr': "D",
            'description': description,
            'source_type': source_type
        })

        # 3. Handle prepaid and payable accounts
        # Apply all prepaid payments first
        for prepaid in applied_prepaid:
            # Get the category ID for the prepaid account
            prepaid_account_category = db_session.query(ChartOfAccounts.category_fk) \
                .filter(ChartOfAccounts.id == prepaid['prepaid_account_id']) \
                .scalar()

            transactions.append({
                'type': "Asset",
                'date': goods_receipt.receipt_date,
                'category_id': prepaid_account_category,
                'subcategory_id': prepaid['prepaid_account_id'],
                'amount': prepaid['amount_applied'],
                'dr_cr': "C",
                'description': f"Applied prepaid for {description}",
                'source_type': "prepaid_application_goods"
            })

        # Then handle remaining payable amount
        if payable_amount > 0:
            transactions.append({
                'type': "Liability",
                'date': goods_receipt.receipt_date,
                'category_id': payable_account_category,
                'subcategory_id': payable_account_id,
                'amount': payable_amount,
                'dr_cr': "C",
                'description': description,
                'source_type': "goods_receipt_payable"
            })

        # Calculate and balance totals
        total_debit = sum(Decimal(str(t['amount'])) for t in transactions if t['dr_cr'] == "D")
        total_credit = sum(Decimal(str(t['amount'])) for t in transactions if t['dr_cr'] == "C")

        discrepancy = total_debit - total_credit  # Now both are Decimal, so no type conflict

        if abs(discrepancy) > Decimal('0.01'):
            for t in transactions:
                if t['source_type'] == "goods_receipt_payable":
                    t['amount'] = Decimal(str(t['amount'])) + discrepancy  # Ensure Decimal
                    print(f"[BALANCE ADJUSTMENT] Adjusted payable by {discrepancy}. New payable amount: {t['amount']}")
                    break

        # Post all transactions

        for t in transactions:
            transaction_date = t['date']
            create_transaction(
                db_session=db_session,
                transaction_type=t['type'],
                date=transaction_date,
                category_id=t['category_id'],
                subcategory_id=t['subcategory_id'],
                currency=purchase_order.currency,
                amount=t['amount'],
                dr_cr=t['dr_cr'],
                description=t['description'],
                payment_mode_id=None,
                project_id=purchase_order.project_id,
                vendor_id=purchase_order.vendor_id,
                created_by=current_user.id,
                source_type=t['source_type'],
                source_id=goods_receipt_item.id,
                app_id=goods_receipt_item.app_id,
                exchange_rate_id=exchange_rate_id,
                journal_number=journal_number
            )

        # Update inventory if needed
        if (goods_receipt_item.purchase_order_item.item_type == 'inventory' and
                purchase_order_item.item_id and
                not goods_receipt_item.inventory_adjusted):
            location_id = get_or_create_default_location(db_session, current_user.app_id)

            update_purchase_inventory_quantity(
                db_session=db_session,
                item_id=purchase_order_item.item_id,
                quantity_received=goods_receipt_item.quantity_received,
                goods_receipt_id=goods_receipt_item.id,
                created_by=current_user.id,
                app_id=goods_receipt.app_id,
                batch_id=goods_receipt.receipt_number,
                unit_cost=purchase_order_item.unit_price,
                currency_id=purchase_order.currency,
                uom=purchase_order_item.uom,
                supplier_id=purchase_order.vendor_id,
                location_id=location_id,
                date=goods_receipt.receipt_date
            )
            goods_receipt_item.inventory_adjusted = True

        # Mark receipt item as posted
        goods_receipt_item.is_posted_to_ledger = True

        if all(gr_item.is_posted_to_ledger for gr_item in goods_receipt.receipt_items):
            goods_receipt.is_posted_to_ledger = True

        db_session.commit()

        flash("Goods receipt successfully posted to ledger", "success")
        return redirect(url_for("purchase_order_details", purchase_order_id=purchase_order.id))

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error posting to ledger: {str(e)}", "error")
        flash(f"Error posting to ledger: {str(e)}", "error")
        return redirect(request.referrer)
    finally:
        db_session.close()


@app.route('/update_inventory', methods=['POST'])
@login_required
def update_inventory():
    receipt_item_id = request.form.get('receipt_item_id')
    quantity = float(request.form.get('quantity'))
    item_name = request.form.get('item_name')
    item_type = request.form.get('item_type')

    try:
        db_session = Session()

        # 1. Find the receipt item with related purchase order data
        receipt_item = db_session.query(GoodsReceiptItem).filter_by(id=receipt_item_id).first()
        if not receipt_item:
            flash("Receipt item not found", "error")
            return redirect(request.referrer)

        # 2. Only process inventory items
        if item_type != "inventory":
            flash("Non-inventory items do not require stock updates", "info")
            return redirect(request.referrer)

        # 3. Get related purchase order item
        po_item = receipt_item.direct_purchase_item
        if not po_item:
            flash("Associated purchase order item not found", "error")
            return redirect(request.referrer)
        location_id = get_or_create_default_location(db_session, current_user.app_id)
        date = receipt_item.goods_receipt.receipt_date
        currency_id = receipt_item.goods_receipt.direct_purchase.currency_id

        exchange_rate_id, notification = resolve_exchange_rate_for_transaction(session=db_session,
                                                                               currency_id=currency_id,
                                                                               transaction_date=date,
                                                                               app_id=current_user.app_id)

        # 4. Update inventory using your existing function
        update_purchase_inventory_quantity(
            db_session=db_session,
            item_id=po_item.item_id,
            quantity_received=receipt_item.quantity_received,
            goods_receipt_id=receipt_item.id,
            created_by=current_user.id,
            app_id=receipt_item.app_id,
            batch_id=receipt_item.goods_receipt.receipt_number,
            unit_cost=po_item.unit_price,
            currency_id=currency_id,
            uom=receipt_item.direct_purchase_item.uom,
            supplier_id=receipt_item.direct_purchase_item.direct_purchases.vendor_id,
            location_id=location_id,
            date=receipt_item.goods_receipt.receipt_date
        )

        # 5. Mark as processed
        receipt_item.is_posted_to_ledger = True
        receipt_item.inventory_adjusted = True
        db_session.commit()
        flash("Inventory updated successfully", "success")
        return redirect(request.referrer)

    except Exception as e:
        db_session.rollback()
        flash(f"Error updating inventory: {str(e)}", "error")
        return redirect(request.referrer)
    finally:
        db_session.close()


@app.route('/post_purchase_shipping_and_handling_to_ledger', methods=['POST'])
@login_required
def post_purchase_shipping_and_handling_to_ledger():
    db_session = Session()
    try:

        # Get form data
        po_id = request.form.get('shippingAndHandlingledgerPurchaseOrderId')
        payable_account_id = request.form.get('shippingAndHandlingPayableAccount')
        payable_account_category = request.form.get('shippingAndHandlingPayableAccountCategory')
        expense_account_id = request.form.get('otherExpenseAccount')
        expense_account_category = request.form.get('otherExpenseAccountCategory')
        date_of_transaction = request.form.get('transactionDate', None)
        transaction_date = datetime.datetime.strptime(date_of_transaction, '%Y-%m-%d') if date_of_transaction else None

        # Get the purchase order
        purchase_order = db_session.query(PurchaseOrder).filter_by(id=po_id).first()
        if not purchase_order:
            flash("Purchase order not found", "error")
            return redirect(request.referrer)

        # Calculate total shipping and handling amounts
        shipping_cost = round(float(purchase_order.shipping_cost or 0), 2)
        handling_cost = round(float(purchase_order.handling_cost or 0), 2)
        total_amount = round(shipping_cost + handling_cost, 2)

        # Check if already posted
        if purchase_order.shipping_handling_posted:
            flash("Shipping and handling costs already posted to ledger", "error")
            return redirect(url_for("purchase_order_details", purchase_order_id=po_id))

        # Process prepaid payment if available
        purchase_payment_id, prepaid_payment_id = get_first_prepaid_payment_id_by_po(purchase_order, db_session)
        prepaid_payment_category_id = db_session.query(ChartOfAccounts.category_fk).filter_by(
            id=prepaid_payment_id).scalar() if prepaid_payment_id else None

        prepaid_balance, total_credits = get_prepaid_balance_shipping(purchase_payment_id, prepaid_payment_id,
                                                                      purchase_order.id,
                                                                      db_session) if prepaid_payment_id else (0, 0)
        amount_to_apply = min(total_amount, prepaid_balance) if prepaid_balance > 0 else 0

        currency_id = purchase_order.currency

        exchange_rate_id, notification = resolve_exchange_rate_for_transaction(session=db_session,
                                                                               currency_id=currency_id,
                                                                               transaction_date=transaction_date,
                                                                               app_id=current_user.app_id)

        description = f"Shipping & handling for PO {purchase_order.purchase_order_number}"

        journal_number = generate_unique_journal_number(db_session, current_user.app_id)

        # ========== DEBIT SIDE ========== #
        # 1. Debit the full amount to shipping/handling expense account
        create_transaction(
            db_session=db_session,
            transaction_type="Expense",
            date=transaction_date,
            category_id=expense_account_category,
            subcategory_id=expense_account_id,
            currency=purchase_order.currency,
            amount=total_amount,
            dr_cr="D",
            description=description,
            payment_mode_id=None,
            project_id=purchase_order.project_id,
            vendor_id=purchase_order.vendor_id,
            created_by=current_user.id,
            source_type="shipping_handling_expense",
            source_id=purchase_order.id,
            app_id=purchase_order.app_id,
            exchange_rate_id=exchange_rate_id,
            journal_number=journal_number
        )

        # ========== CREDIT SIDE ========== #
        # Calculate credit distribution
        credit_distribution = {
            'prepaid': round(amount_to_apply, 2),
            'payable': round(total_amount - amount_to_apply, 2)
        }

        # Auto-balance any discrepancies by adjusting the payable account
        total_credited = sum(credit_distribution.values())
        discrepancy = total_amount - total_credited

        if abs(discrepancy) > 0.01:
            print(f"[BALANCE ADJUSTMENT] Applying discrepancy of {discrepancy} to payable account")
            credit_distribution['payable'] += discrepancy
            total_credited = sum(credit_distribution.values())
            print(f"[BALANCE ADJUSTED] New payable amount: {credit_distribution['payable']}")

        # Apply prepaid amount if available
        if credit_distribution['prepaid'] > 0:
            create_transaction(
                db_session=db_session,
                transaction_type="Asset",
                date=transaction_date,
                category_id=prepaid_payment_category_id,
                subcategory_id=prepaid_payment_id,
                currency=purchase_order.currency,
                amount=credit_distribution['prepaid'],
                dr_cr="C",
                description=f"Applied prepaid for {description}",
                payment_mode_id=None,
                project_id=purchase_order.project_id,
                vendor_id=purchase_order.vendor_id,
                created_by=current_user.id,
                source_type="prepaid_application_shipping",
                source_id=purchase_order.id,
                app_id=purchase_order.app_id,
                exchange_rate_id=exchange_rate_id,
                journal_number=journal_number
            )

        # Apply remaining amount to accounts payable
        if credit_distribution['payable'] > 0:
            create_transaction(
                db_session=db_session,
                transaction_type="Liability",
                date=transaction_date,
                category_id=payable_account_category,
                subcategory_id=payable_account_id,
                currency=purchase_order.currency,
                amount=credit_distribution['payable'],
                dr_cr="C",
                description=f"Amount payable for {description}",
                payment_mode_id=None,
                project_id=purchase_order.project_id,
                vendor_id=purchase_order.vendor_id,
                created_by=current_user.id,
                source_type="shipping_handling_payable",
                source_id=purchase_order.id,
                app_id=purchase_order.app_id,
                exchange_rate_id=exchange_rate_id,
                journal_number=journal_number
            )

        # Final balance verification (for logging only)
        final_debit = total_amount
        final_credit = sum(credit_distribution.values())

        # Mark shipping/handling as posted
        purchase_order.shipping_handling_posted = True
        db_session.commit()

        flash("Shipping and handling costs successfully posted to ledger", "success")
        return redirect(url_for("purchase_order_details", purchase_order_id=po_id))

    except Exception as e:
        logger.debug(f'\n[ERROR] Exception occurred: {str(e)}')
        db_session.rollback()
        flash(f"Error posting shipping/handling to ledger: {str(e)}", "error")
        return redirect(request.referrer)
    finally:
        db_session.close()


@app.route('/api/update_purchase_payment', methods=['POST'])
def update_purchase_payment():
    print("\n=== STARTING PAYMENT UPDATE PROCESS ===")
    print(f"Incoming form data: {request.form}")

    try:
        # Parse form data
        print("\n[1/5] PARSING INPUT DATA")
        purchase_order_id = int(request.form.get('purchaseOrderId'))
        transaction_id = int(request.form.get('transactionId'))
        amount = request.form.get('amount')
        payment_date = request.form.get('paymentDate')
        asset_account_id = request.form.get('fundingAccount') or None
        reference = request.form.get('reference') or None
        payment_method = request.form.get('paymentMethod') or None
        payment_method = int(payment_method) if payment_method and payment_method.strip().isdigit() else None
        payment_type = request.form.get('paymentType')

        created_by = current_user.id
        app_id = current_user.app_id

        print(f"PO ID: {purchase_order_id}, Transaction ID: {transaction_id}")
        print(f"New Amount: {amount}, New Date: {payment_date}")
        print(f"Asset Account: {asset_account_id}, Payment Method: {payment_method}")

        # Validate amount
        try:
            new_amount = Decimal(amount)
            print(f"Validated amount: {new_amount}")
        except InvalidOperation:
            print("VALIDATION FAILED: Invalid amount format")
            flash('Amount must be a valid number.', 'error')
            return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

        # Validate date
        try:
            new_payment_date = datetime.datetime.strptime(payment_date, '%Y-%m-%d').date()
            print(f"Validated payment date: {new_payment_date}")
        except ValueError:
            print("VALIDATION FAILED: Invalid date format")
            flash('Invalid payment date format. Use YYYY-MM-DD.', 'error')
            return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

        # Start database session
        db_session = Session()
        print("\n[2/5] DATABASE SESSION STARTED")

        try:
            # Fetch existing allocation and transaction
            print("\n[3/5] FETCHING EXISTING TRANSACTION")
            allocation = db_session.query(PurchasePaymentAllocation).filter_by(payment_id=transaction_id).first()
            if not allocation:
                print(f"Allocation not found for transaction: ID {transaction_id}")
                flash('Payment allocation not found.', 'error')
                return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

            transaction = db_session.query(PurchaseTransaction).filter_by(id=transaction_id).first()
            if not transaction:
                print(f"Transaction not found: ID {transaction_id}")
                flash('Payment transaction not found.', 'error')
                return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

            # Verify transaction is not posted to ledger
            if allocation.is_posted_to_ledger:
                flash('This transaction is already posted to the ledger. Amount updates require reverse entries.',
                      'warning')
                # Allow updating metadata only
                allocation.reference = reference
                allocation.payment_mode = payment_method
                allocation.payment_date = new_payment_date
                db_session.commit()
                flash('Metadata updated successfully.', 'success')
                return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

            # Fetch purchase order
            print("\n[4/5] FETCHING PURCHASE ORDER")
            purchase_order = db_session.query(PurchaseOrder).filter_by(id=purchase_order_id).first()
            if not purchase_order:
                print(f"PO NOT FOUND: ID {purchase_order_id}")
                flash('Purchase Order not found.', 'error')
                return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

            # Calculate old allocation amount
            old_amount = (
                    allocation.allocated_inventory +
                    allocation.allocated_non_inventory +
                    allocation.allocated_services +
                    allocation.allocated_other_expenses
            )

            # Calculate remaining balance
            print("\n[5/5] CALCULATING BALANCE")
            total_paid = db_session.query(func.sum(PurchaseTransaction.amount_paid)).filter(
                PurchaseTransaction.purchase_order_id == purchase_order_id,
                PurchaseTransaction.payment_status.in_(['paid', 'partial'])
            ).scalar() or Decimal('0.00')

            remaining_balance = purchase_order.total_amount - total_paid + old_amount
            print(f"Total Paid: {total_paid}, Remaining Balance (including previous amount): {remaining_balance}")

            if new_amount > remaining_balance:
                flash(f'Payment amount exceeds the remaining balance of {remaining_balance:.2f}.', 'error')
                return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

            if new_amount <= 0:
                print("VALIDATION FAILED: Amount must be positive")
                flash('Payment amount must be greater than 0.', 'error')
                return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

            # Remove previous allocation
            db_session.delete(allocation)

            # Create new allocation using the standard allocation function
            new_allocation = allocate_purchase_payment(
                purchase_transaction_id=transaction_id,
                payment_date=new_payment_date,
                payment_amount=new_amount,
                db_session=db_session,
                payment_mode_id=payment_method,
                payment_type=payment_type,
                payment_account_id=asset_account_id,
                inventory_account_id=allocation.inventory_account_id,
                non_inventory_account_id=allocation.non_inventory_account_id,
                other_expense_account_id=allocation.other_expense_account_id,
                other_expense_service_id=allocation.other_expense_service_id,
                tax_payable_id=allocation.tax_payable_account_id,
                tax_receivable_id=allocation.tax_receivable_account_id,
                credit_purchase_account_id=allocation.credit_purchase_account,
                reference=reference
            )

            # Update transaction status
            new_total_paid = total_paid + new_amount - old_amount
            transaction.amount_paid = new_amount
            transaction.payment_date = new_payment_date
            transaction.reference_number = reference
            transaction.payment_status = "paid" if new_total_paid == purchase_order.total_amount else "partial"
            transaction.purchase_orders.status = OrderStatus.paid if new_total_paid == purchase_order.total_amount else OrderStatus.partially_paid

            db_session.commit()
            print("TRANSACTION COMMITTED SUCCESSFULLY")
            flash('Payment updated successfully!', 'success')
            return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

        except Exception as e:
            db_session.rollback()
            print(f"\n!!! ERROR OCCURRED !!!")
            print(f"Type: {type(e)}")
            print(f"Message: {str(e)}")
            flash(f'An error occurred: {str(e)}', 'error')
            return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

        finally:
            db_session.close()
            print("DATABASE SESSION CLOSED")

    except Exception as e:
        print(f"\n!!! OUTER ERROR !!!")
        print(f"Error: {str(e)}")
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))


@app.route('/api/update_receipt', methods=['POST'])
@login_required
def update_receipt():
    db_session = Session()
    try:
        data = request.form
        receipt_id = data.get('receipt_id')
        receipt_item_id = data.get('receipt_item_id')
        quantity_received = data.get('quantity_received')
        received_condition = data.get('received_condition')
        app_id = current_user.app_id

        if not receipt_id or not receipt_item_id:
            return jsonify({'success': False, 'message': 'Missing receipt identifiers'}), 400

        # Load receipt item with all necessary relationships
        receipt_item = db_session.query(GoodsReceiptItem).options(
            joinedload(GoodsReceiptItem.goods_receipt).joinedload(GoodsReceipt.purchase_order),
            joinedload(GoodsReceiptItem.goods_receipt).joinedload(GoodsReceipt.direct_purchase),
            joinedload(GoodsReceiptItem.purchase_order_item),
            joinedload(GoodsReceiptItem.direct_purchase_item)
        ).filter_by(id=receipt_item_id, goods_receipt_id=receipt_id, app_id=app_id).first()

        if not receipt_item:
            return jsonify({'success': False, 'message': 'Receipt item not found'}), 404

        # Handle ledger-posted items
        if receipt_item.is_posted_to_ledger:
            quantity_received = receipt_item.quantity_received
            received_condition = receipt_item.received_condition

        try:
            new_quantity = Decimal(quantity_received).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
            if new_quantity <= 0:
                return jsonify({'success': False, 'message': 'Quantity must be positive'}), 400
        except:
            return jsonify({'success': False, 'message': 'Invalid quantity format'}), 400

        # Get the purchase item (PO or direct purchase)
        purchase_item = receipt_item.purchase_order_item or receipt_item.direct_purchase_item
        if not purchase_item:
            return jsonify({'success': False, 'message': 'Related purchase item not found'}), 404

        ordered_quantity = Decimal(str(purchase_item.quantity)).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)

        # Calculate total received so far (excluding current receipt item)
        total_received_so_far = db_session.query(
            func.coalesce(func.sum(GoodsReceiptItem.quantity_received), Decimal('0'))
        ).filter(
            GoodsReceiptItem.app_id == app_id,
            (GoodsReceiptItem.purchase_order_item_id == purchase_item.id) if receipt_item.purchase_order_item_id else
            (GoodsReceiptItem.direct_purchase_item_id == purchase_item.id),
            GoodsReceiptItem.id != receipt_item.id
        ).scalar()

        if new_quantity + total_received_so_far > ordered_quantity:
            item_name = getattr(purchase_item, 'item_name', 'item')
            return jsonify({
                'success': False,
                'message': f"Total received quantity for '{item_name}' exceeds the ordered quantity."
            }), 400

        # Get all items from the purchase record (PO or direct purchase)
        receipt = receipt_item.goods_receipt
        if receipt.purchase_order_id:
            purchase_record = receipt.purchase_order
            items = purchase_record.purchase_order_items
            item_relationship = 'purchase_order_items'
        else:
            purchase_record = receipt.direct_purchase
            items = purchase_record.direct_purchase_items
            item_relationship = 'direct_purchase_items'

        # Update receipt item quantity
        receipt_item.quantity_received = new_quantity
        receipt_item.received_condition = received_condition
        receipt_item.notes = data.get('notes', '').strip() or None

        # Get all receipt items for this receipt
        receipt_items = db_session.query(GoodsReceiptItem).filter_by(
            goods_receipt_id=receipt.id, app_id=app_id
        ).all()

        # Recalculate allocations for all items in this receipt using the same logic as add_goods_receipt
        allocations = calculate_allocations(
            items=items,
            receipt_items=receipt_items,
            purchase_record=purchase_record
        )

        # Apply allocations to all receipt items
        for ri in receipt_items:
            alloc = next(
                (a for a in allocations if a['item_id'] == (ri.purchase_order_item_id or ri.direct_purchase_item_id)),
                None)
            if alloc:
                ri.allocated_amount = alloc['allocated_amount'] - alloc['allocated_discount']
                ri.allocated_tax_amount = alloc['allocated_tax']

        # Update receipt totals
        total_received = sum(
            Decimal(str(item.quantity_received)).quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
            for item in receipt_items
        )
        receipt.quantity_received = total_received

        # Check for final receipt
        total_ordered = sum(Decimal(str(item.quantity)) for item in items)

        if receipt.purchase_order_id:
            overall_received = get_total_received_for_purchase(receipt.purchase_order_id, db_session, False)
        else:
            overall_received = get_total_received_for_purchase(receipt.direct_purchase_id, db_session, True)

        is_final_receipt = (overall_received >= total_ordered)
        receipt.is_complete_receipt = is_final_receipt

        # Update purchase record status if PO
        if receipt.purchase_order_id:
            purchase_record.is_complete_receipt = is_final_receipt

        # Handle residual tax for final receipt
        if is_final_receipt:
            total_line_tax = sum(Decimal(str(item.tax_amount or '0')) for item in items)
            purchase_level_tax = Decimal(str(purchase_record.total_tax_amount or '0')) - total_line_tax

            # Get ALL previously allocated taxes (from this and other receipts)
            if receipt.purchase_order_id:
                query = db_session.query(
                    func.sum(GoodsReceiptItem.allocated_tax_amount)
                ).join(
                    GoodsReceipt,
                    GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id
                ).filter(
                    GoodsReceiptItem.purchase_order_item_id.in_([item.id for item in items]),
                    GoodsReceipt.app_id == app_id
                )
            else:
                query = db_session.query(
                    func.sum(GoodsReceiptItem.allocated_tax_amount)
                ).join(
                    GoodsReceipt,
                    GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id
                ).filter(
                    GoodsReceiptItem.direct_purchase_item_id.in_([item.id for item in items]),
                    GoodsReceipt.app_id == app_id
                )

            total_allocated_so_far = query.scalar() or Decimal('0')

            # Calculate difference
            tax_diff = purchase_level_tax - total_allocated_so_far

            # Apply adjustment to the last item in current receipt
            if abs(tax_diff) > Decimal('0.01') and receipt.receipt_items:
                last_item = receipt.receipt_items[-1]
                if last_item.allocated_tax_amount is None:
                    last_item.allocated_tax_amount = Decimal('0')

                last_item.allocated_tax_amount += tax_diff
                db_session.flush()

        # Update inventory status
        receipt.inventory_received = any(
            (item.purchase_order_item and item.purchase_order_item.item_type == 'inventory') or
            (item.direct_purchase_item and item.direct_purchase_item.item_type == 'inventory')
            for item in receipt_items
            if item.quantity_received > 0
        )

        # Update receipt metadata if provided
        if 'receipt_date' in data:
            try:
                receipt.receipt_date = datetime.datetime.strptime(data['receipt_date'], '%Y-%m-%d').date()
            except ValueError:
                pass  # Ignore invalid dates

        if 'received_by' in data:
            receipt.received_by = data['received_by'].strip()

        db_session.commit()
        return jsonify({
            'success': True,
            'message': 'Receipt and allocations updated successfully',
            'data': {
                'total_received': float(total_received),
                'is_complete': is_final_receipt,
                'inventory_received': receipt.inventory_received
            }
        })

    except Exception as e:
        db_session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


@app.route('/api/cancel_direct_payment', methods=['POST'])
@login_required
def cancel_direct_payment():
    db_session = Session()
    try:
        app_id = current_user.app_id
        allocation_id = request.form.get('transaction_id')  # Payment allocation ID

        print(f"\n=== CANCEL DIRECT PAYMENT REQUEST ===")
        print(f"Allocation ID: {allocation_id}")
        print(f"App ID: {app_id}")
        print(f"Current User: {current_user.id}")

        # Get the payment allocation record
        payment_allocation = db_session.query(PurchasePaymentAllocation).filter_by(
            id=allocation_id,
            app_id=app_id,
            payment_type='direct_purchase'
        ).first()

        if not payment_allocation:
            print("ERROR: Payment allocation not found")
            return jsonify({'success': False, 'message': 'Payment allocation not found'}), 404

        # Check if allocation is already cancelled (marked by zero amounts)
        if (
                Decimal(payment_allocation.allocated_inventory or 0) == 0 and
                Decimal(payment_allocation.allocated_non_inventory or 0) == 0 and
                Decimal(payment_allocation.allocated_services or 0) == 0 and
                Decimal(payment_allocation.allocated_other_expenses or 0) == 0 and
                Decimal(payment_allocation.allocated_tax_receivable or 0) == 0 and
                Decimal(payment_allocation.allocated_tax_payable or 0) == 0
        ):
            print("ERROR: Payment allocation already cancelled")
            return jsonify({'success': False, 'message': 'Payment allocation already cancelled'}), 400

        # Get the related direct purchase transaction (for reference only)
        direct_purchase = db_session.query(DirectPurchaseTransaction).filter_by(
            id=payment_allocation.direct_purchase_id,
            app_id=app_id
        ).first()

        if not direct_purchase:
            print("ERROR: Related direct purchase not found")
            return jsonify({'success': False, 'message': 'Direct purchase not found'}), 404

        print("\nProceeding with payment cancellation...")

        # Calculate total allocation amount to be reversed
        total_allocation = (
                Decimal(payment_allocation.allocated_inventory or 0) +
                Decimal(payment_allocation.allocated_non_inventory or 0) +
                Decimal(payment_allocation.allocated_services or 0) +
                Decimal(payment_allocation.allocated_other_expenses or 0) +
                Decimal(payment_allocation.allocated_tax_receivable or 0) +
                Decimal(payment_allocation.allocated_tax_payable or 0)
        )

        # Zero out the allocation amounts
        payment_allocation.allocated_inventory = 0
        payment_allocation.allocated_non_inventory = 0
        payment_allocation.allocated_services = 0
        payment_allocation.allocated_other_expenses = 0
        payment_allocation.allocated_tax_receivable = 0
        payment_allocation.allocated_tax_payable = 0

        # Mark as not posted to ledger (if it was)
        payment_allocation.is_posted_to_ledger = False

        # Update the direct purchase's amount_paid (reduce by cancelled amount)
        direct_purchase.amount_paid = Decimal(direct_purchase.amount_paid or 0) - total_allocation

        print(f"\nPayment allocation cancelled. Amount reversed: {total_allocation}")
        print(f"New amount paid on direct purchase: {direct_purchase.amount_paid}")

        db_session.commit()
        print("Payment allocation successfully cancelled in database")

        return jsonify({
            'success': True,
            'message': 'Direct payment allocation cancelled',
            'amount_reversed': float(total_allocation),
            'new_amount_paid': float(direct_purchase.amount_paid)
        })

    except Exception as e:
        print(f"\n!!! ERROR DURING DIRECT PAYMENT CANCELLATION !!!")
        print(f"Error Type: {type(e)}")
        print(f"Error Message: {str(e)}")

        db_session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500

    finally:
        print("\nClosing database session")
        db_session.close()
        print("=== END OF CANCEL DIRECT PAYMENT PROCESS ===")


@app.route('/api/cancel_receipt', methods=['POST'])
@login_required
def cancel_receipt():
    db_session = Session()
    try:
        app_id = current_user.app_id
        receipt_id = request.form.get('receipt_id')
        receipt_item_id = request.form.get('receipt_item_id')

        print(f"\n=== CANCEL RECEIPT REQUEST ===")
        print(f"Receipt ID: {receipt_id}")
        print(f"Receipt Item ID: {receipt_item_id}")
        print(f"App ID: {app_id}")
        print(f"Current User: {current_user.id}")

        # Get the receipt item
        receipt_item = db_session.query(GoodsReceiptItem).filter_by(
            id=receipt_item_id,
            receipt_id=receipt_id,
            app_id=app_id
        ).first()

        receipt = db_session.query(GoodsReceipt).filter_by(
            id=receipt_id,
            app_id=app_id
        ).first()

        print(f"\nReceipt Query Result: {receipt} and Item: {receipt_item}")

        if not receipt or not receipt_item:
            print("ERROR: Receipt or item not found")
            return jsonify({'success': False, 'message': 'Receipt or item not found'}), 404

        print(f"\nCurrent Receipt Status: {receipt.status}")

        # Check if posted to ledger
        requires_reversal = receipt.is_posted_to_ledger

        print("\nProceeding with cancellation...")
        print(f"Requires ledger reversal: {requires_reversal}")

        # Prepare all receipt item values as Decimals
        item_amount = Decimal(receipt_item.amount or 0)
        item_tax = Decimal(receipt_item.tax_amount or 0)
        total_item_amount = item_amount + item_tax

        # # Update amounts
        # original_amount_received = Decimal(sale_transaction.amount_received or 0)
        # new_amount_received = original_amount_received - total_item_amount
        #
        # print(f"\nUpdating amounts by: {total_item_amount}")
        # print(f"New Amount Received: {new_amount_received}")

        # # Mark receipt item as cancelled
        # receipt_item.status = ReceiptItemStatus.cancelled
        # receipt_item.cancelled_at = datetime.utcnow()
        # receipt_item.cancelled_by = current_user.id
        #
        # # Check if all items are cancelled
        # active_items = db_session.query(ReceiptItem).filter_by(
        #     receipt_id=receipt_id,
        #     status=ReceiptItemStatus.active,
        #     app_id=app_id
        # ).count()
        #
        # if active_items == 0:
        #     receipt.status = ReceiptStatus.cancelled
        #     print("All receipt items cancelled - marking receipt as cancelled")
        #
        # # Update sale transaction status
        # if new_amount_received <= Decimal('0.00'):
        #     sale_transaction.payment_status = SalePaymentStatus.unpaid
        #     print("Sale transaction status set to 'unpaid' (no payments remaining)")
        # else:
        #     sale_transaction.payment_status = SalePaymentStatus.partially_paid
        #
        # sale_transaction.amount_received = new_amount_received
        # db_session.commit()
        # print("Receipt and sale transaction successfully updated in database")

        return jsonify({
            'success': True,
            'message': 'Please check with admin',
            'requires_reversal': requires_reversal,
            'receipt_id': receipt_id
        })

    except Exception as e:
        print(f"\n!!! ERROR DURING RECEIPT CANCELLATION !!!")
        print(f"Error Type: {type(e)}")
        print(f"Error Message: {str(e)}")

        db_session.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500

    finally:
        print("\nClosing database session")
        db_session.close()
        print("=== END OF CANCEL RECEIPT PROCESS ===")


@app.route('/api/update_direct_purchase_payment', methods=['POST'])
def update_direct_purchase_payment():
    try:
        # Parse form data
        transaction_id = int(request.form.get('transactionId'))
        amount = request.form.get('amount')
        payment_date = request.form.get('paymentDate')
        reference = request.form.get('reference') or None
        asset_account_id = request.form.get('fundingAccount') or None
        payment_method = request.form.get('paymentMethod') or None
        payment_method = int(payment_method) if payment_method and payment_method.strip().isdigit() else None
        payment_type = "direct_purchase"

        created_by = current_user.id
        app_id = current_user.app_id

        # Validate amount
        try:
            new_amount = Decimal(amount)
        except InvalidOperation:
            flash('Amount must be a valid number.', 'error')
            return redirect(request.referrer)

        # Validate date
        try:
            new_payment_date = datetime.datetime.strptime(payment_date, '%Y-%m-%d').date()
        except ValueError:
            flash('Invalid payment date format. Use YYYY-MM-DD.', 'error')
            return redirect(request.referrer)

        db_session = Session()
        allocation = db_session.query(PurchasePaymentAllocation).filter_by(id=transaction_id).first()
        if not allocation:
            flash('Payment allocation not found.', 'error')
            return redirect(request.referrer)

        transaction = db_session.query(DirectPurchaseTransaction).filter_by(id=allocation.direct_purchase_id).first()
        if not transaction:
            flash('Direct Purchase transaction not found.', 'error')
            return redirect(request.referrer)

        if allocation.is_posted_to_ledger:
            flash('This transaction is already posted to the ledger. Amount updates require reverse entries.',
                  'warning')
            # Allow updating metadata only
            allocation.reference = reference
            allocation.payment_mode = payment_method
            allocation.payment_date = new_payment_date
            db_session.commit()
            flash('Metadata updated successfully.', 'success')
            return redirect(request.referrer)

        # Reallocate using same logic as new payments
        old_amount = (
                allocation.allocated_inventory +
                allocation.allocated_non_inventory +
                allocation.allocated_services +
                allocation.allocated_other_expenses
        )

        total_paid = transaction.amount_paid
        remaining_balance = transaction.total_amount - total_paid + old_amount
        if new_amount > remaining_balance:
            flash(f'Payment amount exceeds the remaining balance of {remaining_balance:.2f}.', 'error')
            return redirect(request.referrer)

        if new_amount <= 0:
            flash('Payment amount must be greater than 0.', 'error')
            return redirect(request.referrer)

        # Adjust payment status
        new_total_paid = total_paid + new_amount - old_amount
        transaction.amount_paid = new_total_paid
        transaction.payment_status = "full" if new_total_paid == transaction.total_amount else "partial"
        transaction.status = OrderStatus.paid if new_total_paid == transaction.total_amount else OrderStatus.partially_paid
        transaction.payment_date = new_payment_date

        # Remove previous allocation and replace with new one
        db_session.delete(allocation)

        new_allocation = allocate_direct_purchase_payment(
            direct_purchase_id=transaction.id,
            payment_date=new_payment_date,
            payment_amount=new_amount,
            db_session=db_session,
            payment_mode_id=payment_method,
            payment_account_id=asset_account_id,
            inventory_account_id=allocation.inventory_account_id,
            non_inventory_account_id=allocation.non_inventory_account_id,
            other_expense_account_id=allocation.other_expense_account_id,
            other_expense_service_id=allocation.other_expense_service_id,
            tax_payable_id=allocation.tax_payable_account_id,
            tax_receivable_id=allocation.tax_receivable_account_id,
            credit_purchase_account_id=allocation.credit_purchase_account,
            reference=reference
        )

        db_session.add(new_allocation)
        db_session.commit()

        flash("Payment updated successfully!", 'success')
        return redirect(request.referrer)

    except Exception as e:
        print(f"Exception occurred: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return redirect(request.referrer)

    finally:
        db_session.close()


@app.route('/approve_direct_purchase', methods=['POST'])
@login_required
def approve_direct_purchase():
    """
    Approves a Direct Purchase by its ID.
    """
    db_session = Session()
    try:
        # Get data from the request
        data = request.get_json()
        direct_purchase_id = data.get('direct_purchase_id')

        print(f'Data is {data}')

        if not direct_purchase_id:
            return jsonify({"status": "error", "message": "Direct Purchase ID is required"}), 400

        # Fetch the Direct Purchase from the database
        direct_purchase = db_session.query(DirectPurchaseTransaction).filter_by(id=direct_purchase_id).first()
        if not direct_purchase:
            return jsonify({"status": "error", "message": "Direct Purchase not found"}), 404

        # Update the Direct Purchase status based on payment
        if direct_purchase.amount_paid == 0:
            direct_purchase.status = OrderStatus.unpaid
        elif direct_purchase.amount_paid < direct_purchase.total_amount:
            direct_purchase.status = OrderStatus.partially_paid
        else:
            direct_purchase.status = OrderStatus.paid

        # Commit the changes to the database
        db_session.commit()

        flash("Direct Purchase approved successfully.", "success")
        return jsonify({"status": "success", "message": "Direct Purchase approved successfully."}), 200

    except Exception as e:
        # Handle any errors
        db_session.rollback()
        print(f'error is {e}')
        flash(f"An error occurred: {str(e)}", "error")
        return jsonify({"status": "error", "message": f"An error occurred: {str(e)}"}), 500

    finally:
        db_session.close()


@app.route('/purchases/transactions')
@login_required
def view_purchase_transactions():
    app_id = current_user.app_id
    db_session = Session()

    try:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch filter options from request
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        filter_type = request.args.get('filter_type', 'payment_date')  # Default: payment_date
        status_filter = request.args.get('status')
        filter_applied = bool(start_date or end_date or status_filter)
        # Base queries for PO-based and direct purchase transactions
        po_based_query = db_session.query(PurchaseTransaction).filter(
            PurchaseTransaction.app_id == app_id
        ).order_by(PurchaseTransaction.created_at.desc())  # Sort by created_at in descending order

        direct_purchase_query = db_session.query(DirectPurchaseTransaction).filter_by(app_id=app_id)

        # Apply date filters based on filter type
        if start_date or end_date:
            try:
                if start_date:
                    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

                if filter_type == 'payment_date':
                    if start_date:
                        po_based_query = po_based_query.filter(PurchaseTransaction.payment_date >= start_date)
                        direct_purchase_query = direct_purchase_query.filter(
                            DirectPurchaseTransaction.payment_date >= start_date)
                    if end_date:
                        po_based_query = po_based_query.filter(PurchaseTransaction.payment_date <= end_date)
                        direct_purchase_query = direct_purchase_query.filter(
                            DirectPurchaseTransaction.payment_date <= end_date)
                elif filter_type == 'order_date':
                    if start_date:
                        po_based_query = po_based_query.filter(
                            PurchaseTransaction.purchase_orders.order_date >= start_date)
                    if end_date:
                        po_based_query = po_based_query.filter(
                            PurchaseTransaction.purchase_orders.order_date <= end_date)
            except ValueError:
                flash('Invalid date format. Please use YYYY-MM-DD.', 'error')

        # Apply status filter
        if status_filter:
            if status_filter == 'posted':
                po_based_query = po_based_query.filter(PurchaseTransaction.is_posted_to_ledger == True)
                direct_purchase_query = direct_purchase_query.filter(
                    DirectPurchaseTransaction.is_posted_to_ledger == True)
            elif status_filter == 'not_posted':
                po_based_query = po_based_query.filter(PurchaseTransaction.is_posted_to_ledger == False)
                direct_purchase_query = direct_purchase_query.filter(
                    DirectPurchaseTransaction.is_posted_to_ledger == False)

        # Fetch filtered transactions
        po_based_transactions = po_based_query.all()
        direct_purchase_transactions = direct_purchase_query.all()

        # Combine both lists into a single list
        combined_transactions = []

        # Process PO-based transactions
        for transaction in po_based_transactions:
            po_id = transaction.purchase_order_id
            total_po_amount = transaction.purchase_orders.total_amount if transaction.purchase_orders else 0

            # Calculate total paid for this PO, excluding cancelled transactions
            total_paid = db_session.query(func.sum(PurchaseTransaction.amount_paid)) \
                             .filter(PurchaseTransaction.purchase_order_id == po_id,
                                     PurchaseTransaction.app_id == app_id,
                                     PurchaseTransaction.payment_status != PurchasePaymentStatus.cancelled) \
                             .scalar() or 0

            # Calculate remaining balance
            remaining_balance = total_po_amount - total_paid

            # Calculate unposted transactions count
            unposted_goods_receipt_count = db_session.query(GoodsReceiptItem). \
                join(GoodsReceipt, GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id). \
                join(PurchaseOrderItem, GoodsReceiptItem.purchase_order_item_id == PurchaseOrderItem.id). \
                filter(
                GoodsReceipt.purchase_order_id == po_id,
                PurchaseOrderItem.item_type == "inventory",
                GoodsReceiptItem.is_posted_to_ledger == False
            ).count()

            unposted_purchase_returns_count = db_session.query(PurchaseReturn).filter_by(
                purchase_order_id=po_id, is_posted_to_ledger=False).count()
            unposted_payment_allocations_count = db_session.query(PurchasePaymentAllocation).filter_by(
                payment_id=transaction.id, is_posted_to_ledger=False).count()
            unposted_total = unposted_goods_receipt_count + unposted_purchase_returns_count + unposted_payment_allocations_count

            combined_transactions.append({
                'id': transaction.id,
                'po_id': transaction.purchase_order_id,
                'type': 'purchase_order',
                'po_number': transaction.purchase_orders.purchase_order_number if transaction.purchase_orders else None,
                'vendor_name': transaction.vendor.vendor_name,
                'payment_date': transaction.payment_date,
                'amount_paid': transaction.amount_paid,
                'currency': transaction.currency.user_currency,
                'reference_number': transaction.reference_number,
                'total_po_amount': total_po_amount,
                'total_paid': total_paid,
                'remaining_balance': remaining_balance,
                'payment_mode': transaction.payment_allocations[0].payment_modes.payment_mode
                if transaction.payment_allocations and transaction.payment_allocations[0].payment_modes
                else None,
                'posted_to_ledger': transaction.is_posted_to_ledger,
                'status': transaction.payment_status.name,
                'payment_progress': "Final" if remaining_balance == 0 else "Ongoing" if total_paid > transaction.amount_paid else "Initial",
                'unposted_total': unposted_total  # Add unposted total to the dictionary
            })

        # Process direct purchase transactions
        for transaction in direct_purchase_transactions:
            total_purchase_amount = transaction.total_amount
            total_paid = db_session.query(func.sum(DirectPurchaseTransaction.amount_paid)) \
                             .filter_by(direct_purchase_number=transaction.direct_purchase_number,
                                        app_id=app_id).scalar() or 0

            remaining_balance = total_purchase_amount - total_paid

            # Calculate unposted transactions count
            unposted_goods_receipt_count = db_session.query(GoodsReceiptItem). \
                join(GoodsReceipt, GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id). \
                join(DirectPurchaseItem, GoodsReceiptItem.direct_purchase_item_id == DirectPurchaseItem.id). \
                filter(
                GoodsReceipt.direct_purchase_id == transaction.id,
                DirectPurchaseItem.item_type == "inventory",
                GoodsReceiptItem.is_posted_to_ledger == False
            ).count()
            unposted_purchase_returns_count = db_session.query(PurchaseReturn).filter_by(
                direct_purchase_id=transaction.id, is_posted_to_ledger=False).count()
            unposted_payment_allocations_count = db_session.query(PurchasePaymentAllocation).filter_by(
                direct_purchase_id=transaction.id, is_posted_to_ledger=False).count()
            unposted_total = unposted_goods_receipt_count + unposted_purchase_returns_count + unposted_payment_allocations_count

            combined_transactions.append({
                'id': transaction.id,
                'type': 'direct',
                'po_number': None,  # Direct purchases don't have POs
                'direct_purchase_number': transaction.direct_purchase_number if transaction.direct_purchase_number else None,
                'vendor_name': transaction.vendor.vendor_name,
                'payment_date': transaction.payment_date,
                'amount_paid': transaction.amount_paid,
                'currency': transaction.currency.user_currency,
                'reference_number': transaction.purchase_reference,
                'total_purchase_amount': total_purchase_amount,
                'total_paid': total_paid,
                'remaining_balance': remaining_balance,
                'payment_mode': transaction.payment_allocations[0].payment_modes.payment_mode
                if transaction.payment_allocations and transaction.payment_allocations[0].payment_modes
                else None,
                'posted_to_ledger': transaction.is_posted_to_ledger,
                'status': transaction.status.name,
                'purchase_type': "Installment" if remaining_balance > 0 else "Full",
                'unposted_total': unposted_total  # Add unposted total to the dictionary
            })

        # Close the session
        db_session.close()

        # Render the template with the combined transactions
        return render_template('/purchases/purchase_transactions.html',
                               purchase_transactions=combined_transactions,
                               company=company,
                               role=role,
                               filter_applied=filter_applied,
                               modules=modules_data)

    except Exception as e:
        db_session.close()
        # Log the error and return an error message or redirect to an error page
        print(f"An error occurred: {e}")
        return "An error occurred while processing your request.", 500


@app.route('/edit_purchase_transaction/<int:transaction_id>', methods=['POST'])
@login_required
def edit_purchase_transaction(transaction_id):
    app_id = current_user.app_id
    db_session = Session()

    try:
        # Fetch the transaction by ID and app_id
        transaction = db_session.query(PurchaseTransaction).filter_by(id=transaction_id, app_id=app_id).first()

        if not transaction:
            flash("Transaction not found.", "error")
            return redirect(url_for('view_purchase_transactions'))

        # Update basic fields
        new_amount = Decimal(request.form.get('amount'))
        new_payment_date = datetime.datetime.strptime(request.form.get('paymentDate'), '%Y-%m-%d')
        new_reference = request.form.get('reference')
        new_payment_method = request.form.get('paymentMethod')
        new_asset_account = request.form.get('assetAccount')
        new_funding_account = request.form.get('fundingAccount')

        # Ensure it's not None or empty before converting to int
        new_payment_method = int(
            new_payment_method) if new_payment_method and new_payment_method.strip().isdigit() else None

        # Fetch the associated bill
        bill = db_session.query(PurchaseBill).filter_by(id=transaction.bill_id).first()
        if not bill:
            flash("Associated bill not found.", "error")
            return redirect(url_for('view_purchase_transactions'))

        # Calculate the total paid amount for the bill (including the current transaction's original amount)
        total_paid = db_session.query(func.sum(PurchaseTransaction.amount_paid)).filter(
            PurchaseTransaction.bill_id == bill.id,
            PurchaseTransaction.payment_status.in_(
                [PurchasePaymentStatus.full, PurchasePaymentStatus.partial, PurchasePaymentStatus.paid]),
            PurchaseTransaction.payment_status != PurchasePaymentStatus.refund,
            PurchaseTransaction.payment_status != PurchasePaymentStatus.cancelled
        ).scalar() or Decimal('0.00')

        # Calculate the remaining balance before updating the current transaction
        remaining_balance_before_update = bill.total_amount - (total_paid - transaction.amount_paid)

        # Validate the new amount against the remaining balance
        if new_amount > remaining_balance_before_update:
            flash(f"Payment amount exceeds the remaining balance of {remaining_balance_before_update:.2f}.", "error")
            return redirect(url_for('purchase_transaction_details', transaction_id=transaction_id))

        # Update the transaction fields
        transaction.amount_paid = new_amount
        transaction.payment_date = new_payment_date
        transaction.reference_number = new_reference

        # Determine the new payment status
        if new_amount == remaining_balance_before_update:
            new_payment_status = PurchasePaymentStatus.full
        elif new_amount < remaining_balance_before_update:
            new_payment_status = PurchasePaymentStatus.partial
        else:
            flash("Payment exceeds the total amount", "error")
            return redirect(url_for('purchase_transaction_details', transaction_id=transaction_id))

        # Update the payment status
        transaction.payment_status = new_payment_status

        # Update the bill status based on the new payment
        if new_payment_status == PurchasePaymentStatus.full:
            bill.status = BillStatus.paid
        elif new_payment_status == PurchasePaymentStatus.partial:
            bill.status = BillStatus.partially_paid

        # Update payment allocations
        if transaction.payment_allocations:
            allocation = transaction.payment_allocations[0]  # Assuming one allocation per transaction

            # Recalculate the tax amount based on the original tax ratio or bill tax rate
            if bill.total_tax_amount > 0 and bill.total_amount > 0:
                # Calculate the tax ratio from the bill
                tax_ratio = bill.total_tax_amount / bill.total_amount
                # Apply the same ratio to the new payment amount
                new_tax_amount = new_amount * tax_ratio
                new_base_amount = new_amount - new_tax_amount
            else:
                # If no tax is applicable, allocate the entire amount to the base
                new_tax_amount = Decimal('0.00')
                new_base_amount = new_amount

            # Update the allocation amounts
            allocation.allocated_base_amount = new_base_amount
            allocation.allocated_tax_amount = new_tax_amount

            # Update the payment account and tax account (if provided)
            allocation.payment_account = int(request.form.get('fundingAccount'))
            allocation.tax_payable_account_id = int(request.form.get('taxAccount')) if request.form.get(
                'taxAccount') else None
            allocation.payment_mode = new_payment_method
            allocation.credit_purchase_account = int(request.form.get('creditPurchaseAccount')) if request.form.get(
                'creditPurchaseAccount') else None

        # Commit changes to the database
        db_session.commit()

        flash("Purchase transaction updated successfully.", "success")
        return redirect(url_for('purchase_transaction_details', transaction_id=transaction_id))

    except Exception as e:
        db_session.rollback()
        print(f"An error occurred: {e}")
        flash("An error occurred while updating the purchase transaction.", "error")
        return redirect(url_for('purchase_transaction_details', transaction_id=transaction_id))

    finally:
        db_session.close()


@app.route('/purchases/direct_purchase_transaction_details/<int:transaction_id>', methods=['GET'])
@login_required
def direct_purchase_transaction_details(transaction_id):
    app_id = current_user.app_id
    with Session() as db_session:
        # Fetch company, role, and modules data
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

        # Fetch payment modes
        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()

        # Fetch the direct purchase transaction
        direct_purchase = db_session.query(DirectPurchaseTransaction).filter_by(id=transaction_id,
                                                                                app_id=app_id).first()
        if not direct_purchase:
            return jsonify({"error": "Direct purchase transaction not found"}), 404

        # Calculate total payments made (excluding refunded payments)
        total_paid = db_session.query(
            func.sum(
                PurchasePaymentAllocation.allocated_base_amount + PurchasePaymentAllocation.allocated_tax_amount)).filter(
            PurchasePaymentAllocation.direct_purchase_id == transaction_id
        ).scalar() or Decimal('0.00')

        # Calculate the remaining balance
        remaining_balance = Decimal(direct_purchase.total_amount) - total_paid

        # Prepare direct purchase data
        direct_purchase_data = {
            "id": direct_purchase.id,
            "direct_purchase_number": direct_purchase.direct_purchase_number,
            "payment_date": direct_purchase.payment_date.strftime("%Y-%m-%d"),
            "purchase_reference": direct_purchase.purchase_reference,
            "terms_and_conditions": direct_purchase.terms_and_conditions,
            "vendor": {
                "name": direct_purchase.vendor.vendor_name,
                "contact": f"{direct_purchase.vendor.tel_contact}{' | ' + direct_purchase.vendor.email if direct_purchase.vendor.email else ''}",
                "address": direct_purchase.vendor.address or None,
                "city_country": f"{direct_purchase.vendor.city or ''} {direct_purchase.vendor.country or ''}".strip() or None
            } if direct_purchase.vendor else None,
            "currency": direct_purchase.currency.user_currency if direct_purchase.currency else None,
            "total_amount": float(direct_purchase.total_amount),
            "amount_paid": float(total_paid),
            "remaining_balance": float(remaining_balance),
            "payment_status": direct_purchase.payment_status,
            "status": direct_purchase.status,
            "total_line_subtotal": float(direct_purchase.total_line_subtotal),
            "calculated_discount_amount": float(direct_purchase.calculated_discount_amount),
            "purchase_tax_rate": float(direct_purchase.purchase_tax_rate),
            "shipping_cost": float(direct_purchase.shipping_cost),
            "handling_cost": float(direct_purchase.handling_cost),
            "direct_purchase_items": [
                {
                    "id": item.id,
                    "item_name": item.item_name if item.item_name else item.inventory_item_variation_link.inventory_item.item_name,
                    "description": item.description if item.item_name else item.inventory_item_variation_link.inventory_item.item_description,
                    "quantity": item.quantity,
                    "uom": item.unit_of_measurement.full_name,
                    "unit_price": float(item.unit_price),
                    "tax_rate": float(item.tax_rate),
                    "tax_amount": float(item.tax_amount),
                    "discount_amount": float(item.discount_amount),
                    "discount_rate": float(item.discount_rate),
                    "total_price": float(item.total_price)
                } for item in direct_purchase.direct_purchase_items
            ],
            "payment_allocations": [
                {
                    "id": allocation.id,
                    "allocated_base_amount": float(allocation.allocated_base_amount),
                    "allocated_tax_amount": float(allocation.allocated_tax_amount),
                    "payment_mode": allocation.payment_modes.payment_mode if allocation.payment_modes else None,
                    "is_posted_to_ledger": allocation.is_posted_to_ledger,
                    "created_at": allocation.created_at.strftime("%Y-%m-%d %H:%M:%S")
                } for allocation in direct_purchase.payment_allocations
            ],
            "created_by": direct_purchase.user.name,
            "created_at": direct_purchase.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": direct_purchase.updated_at.strftime("%Y-%m-%d %H:%M:%S")
        }

    # Return JSON or render template
    if request.args.get('format') == 'json':
        return jsonify(direct_purchase_data)
    else:
        return render_template('/purchases/direct_purchase_transaction_details.html',
                               direct_purchase=direct_purchase_data,
                               company=company,
                               modules=modules_data,
                               payment_modes=payment_modes,
                               role=role,
                               module_name="Purchases")


@app.route('/create_purchase_return', methods=['POST'])
@login_required
def create_purchase_return():
    try:
        receipt_item_id = request.form.get('receipt_item_id')
        quantity = Decimal(request.form.get('quantity'))
        receipt_id = request.form.get('receipt_id')
        reason = request.form.get('reason', '').strip()
        return_date_str = request.form.get('return_date')

        print(f'Data is {request.form}')
        if not all([receipt_item_id, quantity, reason, return_date_str]):
            flash("Missing required fields", "error")
            return redirect(request.referrer)

        try:
            return_date = datetime.datetime.strptime(return_date_str, '%Y-%m-%d').date()
            if return_date > datetime.datetime.today().date():
                flash("Return date cannot be in the future", "error")
                return redirect(request.referrer)
        except ValueError:
            flash("Invalid date format", "error")
            return redirect(request.referrer)

        with (Session() as db_session):
            # Create the return
            return_number = generate_next_return_number(db_session)
            # Get receipt item and calculate total returned in a single query
            result = db_session.query(
                GoodsReceiptItem,
                func.coalesce(func.sum(PurchaseReturn.quantity), Decimal('0'))
            ).outerjoin(
                PurchaseReturn,
                PurchaseReturn.receipt_item_id == GoodsReceiptItem.id
            ).options(
                joinedload(GoodsReceiptItem.goods_receipt),
                joinedload(GoodsReceiptItem.purchase_order_item).joinedload(PurchaseOrderItem.purchase_orders),
                joinedload(GoodsReceiptItem.direct_purchase_item).joinedload(DirectPurchaseItem.direct_purchases)
            ).filter(
                GoodsReceiptItem.id == receipt_item_id,
                GoodsReceiptItem.app_id == current_user.app_id
            ).group_by(GoodsReceiptItem.id).first()

            if not result:
                flash("Receipt item not found", "error")
                return redirect(request.referrer)

            receipt_item, total_returned = result
            available_qty = receipt_item.quantity_received - total_returned

            if quantity <= 0:
                flash("Return quantity must be positive", "error")
                return redirect(request.referrer)

            if quantity > available_qty:
                flash(f"Invalid quantity. Maximum available: {available_qty}", "error")
                return redirect(request.referrer)

            # Determine if it's PO or Direct Purchase
            if receipt_item.purchase_order_item_id:
                purchase = receipt_item.purchase_order_item.purchase_orders
                purchase_order_id = purchase.id
                direct_purchase_id = None
            else:
                purchase = receipt_item.direct_purchase_item.direct_purchases
                direct_purchase_id = purchase.id
                purchase_order_id = None

            # Calculate allocations proportionally
            if receipt_item.quantity_received > 0:
                ratio = quantity / receipt_item.quantity_received
                allocated_amount = (receipt_item.allocated_amount * ratio).quantize(Decimal('0.01'))
                allocated_tax = (receipt_item.allocated_tax_amount * ratio).quantize(Decimal('0.01'))
            else:
                allocated_amount = allocated_tax = Decimal('0')

            purchase_return = PurchaseReturn(
                return_number=return_number,
                purchase_order_id=purchase_order_id,
                direct_purchase_id=direct_purchase_id,
                receipt_item_id=receipt_item.id,
                vendor_id=purchase.vendor_id,
                return_date=return_date,
                reason=reason,
                quantity=quantity,
                allocated_amount=allocated_amount,
                allocated_tax_amount=allocated_tax,
                status='draft',
                app_id=current_user.app_id,
                created_by=current_user.id
            )

            db_session.add(purchase_return)
            db_session.commit()

            flash(f"Return {return_number} created successfully for {return_date.strftime('%Y-%m-%d')}", "success")
            return redirect(request.referrer)

    except Exception as e:

        logger.error(f"Error creating purchase return: {str(e)}", exc_info=True)
        flash(f"Error creating return: {str(e)}", "error")
        return redirect(request.referrer)


@app.route('/api/receipt_items/<int:receipt_item_id>/available_quantity')
@login_required
def get_available_quantity(receipt_item_id):
    with Session() as db_session:
        # Get the receipt item
        receipt_item = db_session.query(GoodsReceiptItem).filter_by(
            id=receipt_item_id,
            app_id=current_user.app_id
        ).first()

        if not receipt_item:
            return jsonify({'error': 'Receipt item not found'}), 404

        # Calculate total already returned
        total_returned = db_session.query(
            func.coalesce(func.sum(PurchaseReturn.quantity), Decimal('0'))
        ).filter(
            PurchaseReturn.receipt_item_id == receipt_item_id
        ).scalar()

        available = receipt_item.quantity_received - total_returned

        return jsonify({
            'available_quantity': str(available),
            'quantity_received': str(receipt_item.quantity_received),
            'total_returned': str(total_returned)
        })


@app.route('/cancel_purchase_return', methods=['POST'])
@login_required
def cancel_purchase_return():
    db_session = Session()
    try:
        return_id = request.form.get('return_id')
        reason = request.form.get('reason', '').strip()

        if not return_id or not reason:
            flash("Missing required fields", "error")
            return redirect(request.referrer)

        return_obj = db_session.query(PurchaseReturn).filter_by(
            id=return_id,
            app_id=current_user.app_id
        ).first()

        if not return_obj:
            flash("Return not found", "error")
            return redirect(request.referrer)

        if return_obj.status != 'draft':
            flash("Only draft returns can be cancelled", "error")
            return redirect(request.referrer)

        return_obj.status = 'canceled'
        return_obj.cancel_reason = reason
        return_obj.canceled_by = current_user.id
        return_obj.canceled_at = func.now()

        db_session.commit()
        flash("Return cancelled successfully", "success")
        return redirect(request.referrer)

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error cancelling return: {str(e)}", exc_info=True)
        flash(f"Error cancelling return: {str(e)}", "error")
        return redirect(request.referrer)


@app.route('/approve_purchase_return', methods=['POST'])
@login_required
def approve_purchase_return():
    try:
        return_id = request.form.get('return_id')
        print(f' data is herereere  {request.form}')
        if not return_id:
            flash("Missing return ID", "error")
            return redirect(request.referrer)

        with Session() as db_session:
            return_obj = db_session.query(PurchaseReturn).filter_by(
                id=return_id,
                app_id=current_user.app_id
            ).first()

            if not return_obj:
                flash("Return not found", "error")
                return redirect(request.referrer)

            if return_obj.status != 'draft':
                flash("Only draft returns can be approved", "error")
                return redirect(request.referrer)

            return_obj.status = 'approved'

            db_session.commit()
            flash("Return approved successfully", "success")
            return redirect(request.referrer)

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error approving return: {str(e)}", exc_info=True)
        flash(f"Error approving return: {str(e)}", "error")
        return redirect(request.referrer)


@app.route('/post_purchase_return_to_ledger', methods=['POST'])
@login_required
def post_purchase_return_to_ledger():
    db_session = Session()
    try:
        # collect form data
        purchase_return_id = request.form.get('purchase_return_id')
        raw_return_amount = request.form.get('return_amount', '').strip()
        return_amount = float(raw_return_amount) if raw_return_amount else 0.0
        original_amount = float(request.form.get('original_purchase_amount'))

        raw_posting_date = request.form.get('return_posting_date', '').strip()
        try:
            posting_date = datetime.datetime.strptime(raw_posting_date, '%Y-%m-%d').date()
        except ValueError:
            flash("Invalid posting date format. Use YYYY-MM-DD", "error")
            return redirect(request.referrer)

        no_refund_option = request.form.get('no_refund_option', 'credit_supplier')

        # account selections
        inventory_account_id = request.form.get('inventory_return_account')
        inventory_account_category_id = request.form.get('inventory_return_account_category')
        refund_account_id = request.form.get('refund_account')
        refund_account_category_id = request.form.get('refund_account_category')
        supplier_account_id = request.form.get('supplier_credit_account')
        supplier_account_category_id = request.form.get('supplier_credit_account_category')
        loss_account_id = request.form.get('loss_account')
        loss_account_category_id = request.form.get('loss_account_category')
        write_off_reason = request.form.get('write_off_reason', '')

        # get the purchase return record
        purchase_return = db_session.query(PurchaseReturn).options(
            joinedload(PurchaseReturn.receipt_items),
            joinedload(PurchaseReturn.purchase_orders),
            joinedload(PurchaseReturn.direct_purchases)
        ).filter_by(id=purchase_return_id, app_id=current_user.app_id).first()
        if not purchase_return:
            flash("Purchase return not found", "error")
            return redirect(request.referrer)

        # get original purchase entry
        purchase_entry = db_session.query(InventoryEntry).filter_by(
            app_id=current_user.app_id,
            source_type='goods_receipt',
            source_id=purchase_return.receipt_items.id
        ).first()
        if not purchase_entry:
            flash("Original purchase entry not found", "error")
            return redirect(request.referrer)

        # item details
        item = (purchase_return.receipt_items.purchase_order_item or
                purchase_return.receipt_items.direct_purchase_item)
        item_name = getattr(item, 'item_name', 'Unknown Item')
        uom = getattr(item, 'uom', 'EA')
        uom_name = purchase_return.receipt_items.purchase_order_item.unit_of_measurement.full_name or \
                   purchase_return.receipt_items.direct_purchase_item.unit_of_measurement.full_name
        item_name_full = get_item_name(purchase_return.receipt_items)

        # update inventory quantity
        update_purchase_return_inventory_quantity(
            db_session=db_session,
            item_id=purchase_entry.item_id,
            quantity_returned=purchase_return.quantity,
            purchase_return_id=purchase_return_id,
            created_by=current_user.id,
            app_id=current_user.app_id,
            unit_cost=purchase_entry.unit_price,
            currency_id=purchase_entry.currency_id,
            uom=uom,
            supplier_id=purchase_return.vendor_id,
            location_id=purchase_entry.to_location,
            batch_id=purchase_entry.lot,
            date=purchase_return.return_date
        )

        currency = (purchase_return.purchase_orders.currency if purchase_return.purchase_orders
                    else purchase_return.direct_purchases.currency_id)
        exchange_rate_id, _ = resolve_exchange_rate_for_transaction(
            session=db_session,
            currency_id=currency,
            transaction_date=posting_date,
            app_id=current_user.app_id
        )

        journal_number = generate_unique_journal_number(db_session, current_user.app_id)
        return_total = purchase_return.allocated_amount + purchase_return.allocated_tax_amount

        # 1. CREDIT inventory
        create_transaction(
            db_session=db_session,
            transaction_type='Asset',
            date=posting_date,
            category_id=inventory_account_category_id,
            subcategory_id=inventory_account_id,
            currency=currency,
            amount=return_total,
            dr_cr='C',
            description=f"Return of {purchase_return.quantity} {uom_name} {item_name_full}",
            payment_mode_id=None,
            project_id=purchase_return.purchase_orders.project_id,
            vendor_id=purchase_return.vendor_id,
            created_by=current_user.id,
            source_type='purchase_return',
            source_id=purchase_return_id,
            app_id=current_user.app_id,
            exchange_rate_id=exchange_rate_id,
            journal_number=journal_number
        )

        # 2. DEBIT supplier payable (whole return amount)
        create_transaction(
            db_session=db_session,
            transaction_type='Liability',
            date=posting_date,
            category_id=supplier_account_category_id,
            subcategory_id=supplier_account_id,
            currency=currency,
            amount=return_total,
            dr_cr='D',
            description=f"Supplier account credited for PR#{purchase_return.return_number}",
            payment_mode_id=None,
            project_id=purchase_return.purchase_orders.project_id,
            vendor_id=purchase_return.vendor_id,
            created_by=current_user.id,
            source_type='purchase_return',
            source_id=purchase_return_id,
            app_id=current_user.app_id,
            exchange_rate_id=exchange_rate_id,
            journal_number=journal_number
        )

        # if any cash refund
        if return_amount > 0:
            cash_journal = generate_unique_journal_number(db_session, current_user.app_id)

            # 3. DR Cash
            create_transaction(
                db_session=db_session,
                transaction_type='Asset',
                date=posting_date,
                category_id=refund_account_category_id,
                subcategory_id=refund_account_id,
                currency=currency,
                amount=return_amount,
                dr_cr='D',
                description=f"Cash refund for PR#{purchase_return.return_number}",
                payment_mode_id=None,
                project_id=purchase_return.purchase_orders.project_id,
                vendor_id=purchase_return.vendor_id,
                created_by=current_user.id,
                source_type='purchase_return',
                source_id=purchase_return_id,
                app_id=current_user.app_id,
                exchange_rate_id=exchange_rate_id,
                journal_number=cash_journal
            )

            # 4. CR Supplier
            create_transaction(
                db_session=db_session,
                transaction_type='Liability',
                date=posting_date,
                category_id=supplier_account_category_id,
                subcategory_id=supplier_account_id,
                currency=currency,
                amount=return_amount,
                dr_cr='C',
                description=f"Supplier account reduced for cash refund PR#{purchase_return.return_number}",
                payment_mode_id=None,
                project_id=purchase_return.purchase_orders.project_id,
                vendor_id=purchase_return.vendor_id,
                created_by=current_user.id,
                source_type='purchase_return',
                source_id=purchase_return_id,
                app_id=current_user.app_id,
                exchange_rate_id=exchange_rate_id,
                journal_number=cash_journal
            )

        # if write-off needed
        remaining_amount = return_total - Decimal(return_amount)
        if remaining_amount > 0:
            if no_refund_option == 'write_off':
                loss_journal = generate_unique_journal_number(db_session, current_user.app_id)

                # 5. DR Loss
                create_transaction(
                    db_session=db_session,
                    transaction_type='Expense',
                    date=posting_date,
                    category_id=loss_account_category_id,
                    subcategory_id=loss_account_id,
                    currency=currency,
                    amount=remaining_amount,
                    dr_cr='D',
                    description=f"Write-off for PR#{purchase_return.return_number}: {write_off_reason}",
                    payment_mode_id=None,
                    project_id=purchase_return.purchase_orders.project_id,
                    vendor_id=purchase_return.vendor_id,
                    created_by=current_user.id,
                    source_type='purchase_return',
                    source_id=purchase_return_id,
                    app_id=current_user.app_id,
                    exchange_rate_id=exchange_rate_id,
                    journal_number=loss_journal
                )

                # 6. CR Supplier
                create_transaction(
                    db_session=db_session,
                    transaction_type='Liability',
                    date=posting_date,
                    category_id=supplier_account_category_id,
                    subcategory_id=supplier_account_id,
                    currency=currency,
                    amount=remaining_amount,
                    dr_cr='C',
                    description=f"Supplier account settled by write-off PR#{purchase_return.return_number}",
                    payment_mode_id=None,
                    project_id=purchase_return.purchase_orders.project_id,
                    vendor_id=purchase_return.vendor_id,
                    created_by=current_user.id,
                    source_type='purchase_return',
                    source_id=purchase_return_id,
                    app_id=current_user.app_id,
                    exchange_rate_id=exchange_rate_id,
                    journal_number=loss_journal
                )

        purchase_return.is_posted_to_ledger = True
        db_session.commit()
        flash("Purchase return successfully posted to ledger", "success")
        return redirect(request.referrer)

    except Exception as e:
        db_session.rollback()
        logger.exception("Error posting purchase return to ledger")
        flash(f"Error posting to ledger: {str(e)}", "error")
        return redirect(request.referrer)
    finally:
        db_session.close()


@app.route('/deep_analytics')
@login_required
def deep_analytics():
    db_session = Session()

    # Get the API key and company ID for the logged-in user's company
    app_id = 1
    api_key = db_session.query(Company.api_key).filter_by(id=app_id).scalar()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role

    # Fetch enabled modules for the app
    modules_data = (
        db_session.query(Module.module_name)
        .filter_by(app_id=app_id, included='yes')
        .all()
    )
    modules_data = [mod.module_name for mod in modules_data]

    # Ensure that the API key exists
    if api_key is None:
        return "API Key not found for the company."

    # Hardcoded API key (if needed for testing)
    api_key = "060225-api-a2881684-e4ba-11ef-b8c7-0affe5cb3fc9"

    # Power BI Embed URL with dynamic parameters
    embed_url = f"https://app.powerbi.com/reportEmbed?reportId=7d1f4e23-b75f-46fb-817f-e35100368de8&autoAuth=true&ctid=50133190-7037-4efb-9e86-51e8fb7616c4&filter=get_company/CompanyID eq '{app_id}'"

    # Pass variables to the template
    return render_template('general_ledger_analytics.html',
                           api_key=api_key,
                           company=company,
                           role=role,
                           modules=modules_data,
                           module_name="General Ledger",
                           embed_url=embed_url)


@app.route('/notifications', methods=['GET'])
@login_required
def get_notifications():
    user_id = current_user.id
    app_id = current_user.app_id
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))

    if not user_id and not app_id:
        return jsonify({'error': 'User ID or Company ID is required'}), 400

    db_session = Session()  # Create a new session
    try:
        query = db_session.query(Notification)

        # Apply the OR filter for user_id or app_id
        if user_id or app_id:
            query = query.filter(
                or_(
                    Notification.user_id == user_id,
                    Notification.company_id == app_id
                )
            )

        total_notifications = query.count()
        unread_count = query.filter(Notification.status == NotificationStatus.unread).count()

        notifications = query.filter_by(status=NotificationStatus.unread).order_by(Notification.created_at.desc()) \
            .offset((page - 1) * per_page) \
            .limit(per_page) \
            .all()

        response = {
            'metadata': {
                'total_notifications': total_notifications,
                'unread_count': unread_count,
                'current_page': page,
                'per_page': per_page,
                'total_pages': (total_notifications + per_page - 1) // per_page
            },
            'notifications': [{
                'id': n.id,
                'message': n.message,
                'type': n.type.name,
                'status': n.status.name,
                'is_popup': n.is_popup,
                'url': n.url,
                'created_at': n.created_at.strftime('%Y-%m-%d %H:%M:%S')
            } for n in notifications]
        }

        db_session.commit()  # Commit changes
        return jsonify(response)
    except Exception as e:
        db_session.rollback()  # Rollback in case of error
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()  # Close the session


# Mark Multiple Notifications as Read
@app.route('/notifications/read', methods=['PUT'])
@login_required
def mark_notifications_as_read():
    print("Marking multiple notifications as read...")  # Debugging: Start of the function
    data = request.json  # Expecting {'ids': [1, 2, 3]} in request body

    if not data or not isinstance(data.get('ids'), list):
        print("Error: Invalid request format")  # Debugging: Invalid request format
        return jsonify({'error': 'Invalid request format'}), 400

    print(f"Notification IDs to mark as read: {data['ids']}")  # Debugging: Print IDs

    db_session = Session()  # Create a new session
    try:
        notifications = db_session.query(Notification).filter(Notification.id.in_(data['ids'])).all()

        if not notifications:
            print("Error: No notifications found")  # Debugging: No notifications found
            return jsonify({'error': 'No notifications found'}), 404

        print(
            f"Found {len(notifications)} notifications to mark as read")  # Debugging: Print number of notifications found

        for notification in notifications:
            notification.status = NotificationStatus.read

        db_session.commit()  # Commit changes
        print(f"{len(notifications)} notifications marked as read")  # Debugging: Success message
        return jsonify({'message': f'{len(notifications)} notifications marked as read'})
    except Exception as e:
        db_session.rollback()  # Rollback in case of error
        print(f"Error marking notifications as read: {str(e)}")  # Debugging: Print error
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()  # Close the session
        print("Database session closed")  # Debugging: Session closed


# Mark Single Notification as Read
@app.route('/notifications/read/<int:notification_id>', methods=['PUT'])
@login_required
def mark_as_read(notification_id):
    print(f"Marking notification {notification_id} as read...")  # Debugging: Start of the function
    db_session = Session()  # Create a new session
    try:
        notification = db_session.query(Notification).filter_by(id=notification_id).first()

        if not notification:
            print(f"Error: Notification {notification_id} not found")  # Debugging: Notification not found
            return jsonify({'error': 'Notification not found'}), 404

        print(f"Found notification {notification_id} to mark as read")  # Debugging: Notification found
        notification.status = NotificationStatus.read

        db_session.commit()  # Commit changes
        print(f"Notification {notification_id} marked as read")  # Debugging: Success message
        return jsonify({'message': 'Notification marked as read'})
    except Exception as e:
        db_session.rollback()  # Rollback in case of error
        print(f"Error marking notification {notification_id} as read: {str(e)}")  # Debugging: Print error
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()  # Close the session
        print("Database session closed")  # Debugging: Session closed


@app.route('/exchange_rates', methods=['GET'])
@login_required
def view_exchange_rates():
    with Session() as db_session:
        try:
            app_id = current_user.app_id

            # Fetch necessary data
            company = db_session.query(Company).filter_by(id=app_id).first()
            role = current_user.role
            modules_data = [mod.module_name for mod in
                            db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

            # Get filter parameters (all optional)
            filters = {
                'from_currency': request.args.get('from_currency'),
                'to_currency': request.args.get('to_currency'),
                'start_date': request.args.get('start_date'),
                'end_date': request.args.get('end_date'),
                'filter_type': request.args.get('filter_type', 'date')
            }

            filter_applied = any([
                filters['from_currency'],
                filters['to_currency'],
                filters['start_date'],
                filters['end_date']
            ])
            query = db_session.query(ExchangeRate).filter_by(app_id=app_id).order_by(ExchangeRate.date.desc())

            # Apply currency filters if specified
            if filters['from_currency']:
                query = query.filter(ExchangeRate.from_currency_id == filters['from_currency'])
            if filters['to_currency']:
                query = query.filter(ExchangeRate.to_currency_id == filters['to_currency'])

            # Apply date filters if specified
            query = apply_date_filters(
                query=query,
                start_date=filters['start_date'],
                end_date=filters['end_date'],
                filter_type=filters['filter_type'],
                model=ExchangeRate,
                date_field='date',
                date_added_field='date'
            )

            # Execute and format results
            exchange_rates = query.all()
            # Get all currencies for dropdowns
            currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
            # Get base currency info
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                logger.error("Base currency not defined for company")
                return jsonify({"error": "Base currency not defined for this company"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]
            return render_template(
                'exchange_rates.html',
                exchange_rates=exchange_rates,
                base_currency=base_currency,
                currencies=currencies,
                company=company,
                role=role,
                modules=modules_data,
                current_filters=filters,
                filter_applied=filter_applied,
                module_name="General Ledger"
            )

        except Exception as e:
            db_session.rollback()
            flash(f'Error fetching exchange rates: {str(e)}', 'danger')
            return redirect(request.referrer)


@app.route('/exchange_rates/<int:rate_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_exchange_rate(rate_id):
    with Session() as db_session:
        try:
            app_id = current_user.app_id
            role = current_user.role

            exchange_rate = db_session.query(ExchangeRate).filter_by(id=rate_id, app_id=app_id).first()
            if not exchange_rate:
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    return jsonify({'success': False, 'message': 'Exchange rate not found or access denied.'})
                flash("Exchange rate not found or access denied.", "danger")
                return redirect(url_for('view_exchange_rates'))

            if request.method == 'POST':
                # Get form data
                new_rate = request.form.get('rate')

                # For AJAX requests, we only update the rate
                if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                    if not new_rate:
                        return jsonify({'success': False, 'message': 'Rate is required.'})

                    exchange_rate.rate = new_rate
                    db_session.commit()
                    return jsonify({'success': True, 'new_rate': new_rate})

                # For regular form submissions (if you still want to support this)
                new_date = request.form.get('date')
                from_currency_id = request.form.get('from_currency_id')
                to_currency_id = request.form.get('to_currency_id')

                if not (new_rate and new_date and from_currency_id and to_currency_id):
                    flash("All fields are required.", "danger")
                    return redirect(request.url)

                exchange_rate.rate = new_rate
                exchange_rate.date = new_date
                exchange_rate.from_currency_id = from_currency_id
                exchange_rate.to_currency_id = to_currency_id

                db_session.commit()
                flash("Exchange rate updated successfully.", "success")
                return redirect(url_for('view_exchange_rates'))

            # Only render template for GET requests (regular page load)
            currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
            return render_template(
                'edit_exchange_rate.html',
                exchange_rate=exchange_rate,
                currencies=currencies,
                company=db_session.query(Company).filter_by(id=app_id).first(),
                role=role,
                module_name="General Ledger"
            )

        except Exception as e:
            db_session.rollback()
            if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                return jsonify({'success': False, 'message': str(e)})
            flash(f"Error editing exchange rate: {str(e)}", "danger")
            return redirect(url_for('view_exchange_rates'))


@app.route('/submit_exchange_transaction', methods=['POST'])
@login_required
def submit_exchange_transaction():
    if request.method == 'POST':
        db_session = Session()
        try:
            logger.info(f'Form data is {request.form}')
            # Get form data
            from_account_id = request.form.get('from_account_id')
            to_account_id = request.form.get('to_account_id')
            from_amount = request.form.get('from_amount')
            to_amount = request.form.get('to_amount')
            from_currency_id = request.form.get('from_currency')
            to_currency_id = request.form.get('currency')
            exchange_date = request.form.get('exchange_date')
            exchange_time_str = request.form.get('exchange_time')
            description = request.form.get('description', '')

            # Basic validation
            if not all([from_account_id, to_account_id, from_amount, to_amount, exchange_date]):
                return jsonify({
                    'status': 'error',
                    'message': 'All required fields must be filled'
                }), 400

            # Convert to proper types
            try:
                from_account_id = int(from_account_id)
                to_account_id = int(to_account_id)
                from_amount = float(from_amount)
                to_amount = float(to_amount)
                exchange_date = datetime.datetime.strptime(exchange_date, '%Y-%m-%d').date()

                # Time parsing similar to your date handling
                if exchange_time_str:  # e.g., "16:03" or "16:03:00"
                    try:
                        # Handle both "HH:MM" and "HH:MM:SS" formats
                        if len(exchange_time_str.split(':')) == 2:
                            exchange_time = datetime.datetime.strptime(exchange_time_str, '%H:%M').time()
                        else:
                            exchange_time = datetime.datetime.strptime(exchange_time_str, '%H:%M:%S').time()
                    except ValueError as e:
                        logger.error(f"Error parsing time: {e}")
                        exchange_time = None
                else:
                    exchange_time = None
            except (ValueError, TypeError) as e:
                return jsonify({
                    'status': 'error',
                    'message': 'Invalid data format'
                }), 400

            # Get base currency info for the company
            base_currency_info = get_base_currency(db_session, current_user.app_id)
            if not base_currency_info:
                return jsonify({'status': 'error', 'message': 'Base currency not defined for this company'}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency_name = base_currency_info["base_currency"]

            # Determine which side is base currency and which is foreign
            if int(to_currency_id) == int(base_currency_id):
                exchange_rate = (
                        Decimal(str(to_amount)) / Decimal(str(from_amount))
                ).quantize(Decimal('0.0000000001'), rounding=ROUND_HALF_UP)

            else:
                exchange_rate = (
                        Decimal(str(from_amount)) / Decimal(str(to_amount))
                ).quantize(Decimal('0.0000000001'), rounding=ROUND_HALF_UP)
            # Create the transaction
            new_transaction = CurrencyExchangeTransaction(
                from_account_id=from_account_id,
                to_account_id=to_account_id,
                from_currency_id=from_currency_id,
                to_currency_id=to_currency_id,
                from_amount=from_amount,
                to_amount=to_amount,
                exchange_rate=exchange_rate,
                exchange_date=exchange_date,
                exchange_time=exchange_time,
                description=description,
                datetime_added=datetime.datetime.now(),
                app_id=current_user.app_id
            )

            db_session.add(new_transaction)
            db_session.commit()

            return jsonify({
                'status': 'success',
                'message': 'Exchange transaction created successfully',
                'transaction_id': new_transaction.id
            })

        except SQLAlchemyError as e:
            logger.error(f'Error is {e}')
            return jsonify({
                'status': 'error',
                'message': 'Database error occurred',
                'details': str(e)
            }), 500

        except Exception as e:
            logger.error(f'Error is {e}')
            return jsonify({
                'status': 'error',
                'message': 'An unexpected error occurred',
                'details': str(e)
            }), 500
        finally:
            db_session.close()


@app.route('/exchange_transactions/<int:transaction_id>/cancel', methods=['POST'])
@login_required
def cancel_exchange_transaction(transaction_id):
    db_session = Session()
    try:
        transaction = db_session.query(CurrencyExchangeTransaction).filter_by(id=transaction_id,
                                                                              app_id=current_user.app_id).first()

        if transaction.is_posted_to_ledger:
            return jsonify({
                'status': 'error',
                'message': 'Cannot cancel a transaction that has already been posted to the ledger'
            }), 400

        transaction.status = "cancelled"

        # Delete the transaction
        db_session.delete(transaction)
        db_session.commit()

        return jsonify({
            'status': 'success',
            'message': 'Transaction cancelled successfully'
        })
    except Exception as e:
        db_session.rollback()
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
    finally:
        db_session.close()


@app.route('/workflow_docs')
@login_required
def workflow_docs():
    db_session = Session()
    app_id = current_user.app_id
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
    """Render the interactive workflow documentation"""
    return render_template('workflow_docs.html',
                           role=current_user.role,
                           company=db_session.query(Company).filter_by(id=app_id).first(),
                           modules=modules_data
                           )


def find_transaction_groups(db_session, app_id):
    print('Finding transaction groups...')
    # Query to find groups of similar transactions
    groups = db_session.query(
        Transaction.date,
        Transaction.payment_mode_id,
        Transaction.project_id,
        Transaction.vendor_id,
        Transaction.description,
        func.count(Transaction.id).label('count')
    ).filter(
        # Catch both NULL/None and empty strings
        or_(
            Transaction.journal_number == None,
            Transaction.journal_number == ''
        ),
        Transaction.app_id == app_id
    ).group_by(
        Transaction.date,
        Transaction.payment_mode_id,
        Transaction.project_id,
        Transaction.vendor_id,
        Transaction.description
    ).having(
        func.count(Transaction.id) > 0
    ).all()

    print(f'Found {len(groups)} transaction groups')
    return groups


from sqlalchemy import or_


def update_transactions_with_journal_numbers(db_session, app_id):
    try:
        print(f"Starting journal number update for app_id: {app_id}")
        groups = find_transaction_groups(db_session, app_id)

        if not groups:
            print("No transaction groups found needing journal numbers")
            return 0

        total_updated = 0

        for group in groups:
            date, payment_mode_id, project_id, vendor_id, description, count = group

            print(f"\nProcessing group: {date}, {description} ({count} transactions)")
            print(f"Payment Mode: {payment_mode_id}, Project: {project_id}, Vendor: {vendor_id}")

            # Generate a new journal number for this group
            journal_number = generate_unique_journal_number(db_session, app_id)
            print(f"Generated journal number: {journal_number}")

            # Update all transactions in this group
            result = db_session.query(Transaction).filter(
                Transaction.date == date,
                Transaction.payment_mode_id == payment_mode_id,
                Transaction.project_id == project_id,
                Transaction.vendor_id == vendor_id,
                Transaction.description == description,
                or_(
                    Transaction.journal_number == None,
                    Transaction.journal_number == ''
                ),
                Transaction.app_id == app_id
            ).update(
                {'journal_number': journal_number},
                synchronize_session=False
            )

            print(f"Updated {result} transactions with journal number")
            total_updated += result

        db_session.commit()
        print(f"\nSuccessfully updated {total_updated} transactions total")
        return total_updated

    except Exception as e:
        db_session.rollback()
        print(f"Error updating journal numbers: {str(e)}")
        raise  # Re-raise the exception for the calling function to handle


@app.route('/api/transactions/update-journal-numbers', methods=['GET', 'POST'])
def update_journal_numbers():
    db_session = Session()
    try:
        # Get app_id from request (could be from body, headers, or session)
        app_id = current_user.app_id  # or get it from session if authenticated

        if not app_id:
            return jsonify({"error": "app_id is required"}), 400

        update_transactions_with_journal_numbers(db_session, app_id)

        return jsonify({
            "success": True,
            "message": "Journal numbers updated successfully"
        }), 200

    except Exception as e:
        db_session.rollback()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/fx_gain_loss', methods=['GET'])
@login_required
def get_fx_gain_loss():
    db_session = Session()
    print('I am here)')

    try:
        app_id = current_user.app_id  # Assuming your user object has app_id
        foreign_currency_id = request.args.get('foreign_currency_id', type=int)
        base_currency_id = request.args.get('base_currency_id', type=int)
        exchange_date_str = request.args.get('exchange_date')
        foreign_account_id = request.args.get('foreign_account_id', type=int)  # Optional

        if not (foreign_currency_id and base_currency_id and exchange_date_str):
            return jsonify({'error': 'Missing required parameters'}), 400

        # Parse exchange date
        try:
            exchange_date = datetime.datetime.strptime(exchange_date_str, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({'error': 'Invalid exchange_date format. Use YYYY-MM-DD'}), 400

        result = calculate_fx_gain_loss(
            db_session=db_session,
            app_id=app_id,
            foreign_currency_id=foreign_currency_id,
            base_currency_id=base_currency_id,
            exchange_date=exchange_date,
            foreign_account_id=foreign_account_id  # Can be None
        )

        return jsonify(result)

    except Exception as e:
        db_session.rollback()
        return jsonify({'error': str(e)}), 500

    finally:
        db_session.close()


