import traceback
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from flask import request, jsonify, render_template, flash, redirect, url_for, abort
from flask_login import login_required, current_user
from flask_wtf.csrf import validate_csrf
from sqlalchemy import or_, func, literal, desc, and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload
from werkzeug.exceptions import BadRequest

from ai import get_base_currency
from db import Session
from models import Company, Module, InventoryLocation, ChartOfAccounts, Vendor, DirectSalesTransaction, \
    InventoryItemVariationLink, DirectSaleItem, Project, PaymentMode, UnitOfMeasurement, Currency, PaymentAllocation, \
    SalesTransaction, SalesPaymentStatus, SalesInvoice, InvoiceStatus, OrderStatus, CustomerCredit, \
    DirectPurchaseTransaction, InventoryItem, DirectPurchaseItem, PurchasePaymentAllocation, GoodsReceipt, \
    GoodsReceiptItem, PurchaseReturn, PurchaseOrder, PurchaseOrderItem, PurchaseOrderStatusLog, PurchaseOrderHistory, \
    PurchaseOrderNote, PurchaseTransaction, JournalEntry, PurchasePaymentStatus, Journal, PurchaseOrderApproval, User, \
    ActivityLog
from services.chart_of_accounts_helpers import group_accounts_by_category
from services.inventory_helpers import safe_clear_stock_history_cache
from services.post_to_ledger import post_direct_sale_cogs_to_ledger, post_sales_transaction_to_ledger, \
    bulk_post_sales_transactions, post_customer_credit_to_ledger, \
    post_purchase_transaction_to_ledger, update_prepaid_journals_for_po, create_purchase_payment_journal_entries, \
    post_goods_receipt_to_ledger
from services.post_to_ledger_reversal import reverse_direct_sales_posting, reverse_sales_invoice_posting, \
    reverse_sales_transaction_posting, delete_journal_entries_by_source
from services.purchases_helpers import generate_direct_purchase_number, allocate_direct_purchase_payment, \
    get_inventory_entries_for_direct_purchase, reverse_purchase_inventory_entries, reverse_direct_purchase_posting, \
    generate_next_purchase_order_number, allocate_purchase_payment, calculate_goods_receipt_landed_costs
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    get_inventory_entries_for_direct_sale, reverse_sales_inventory_entries, allocate_payment
from services.vendors_and_customers import get_or_create_customer_credit_account
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction, \
    create_notification
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj, \
    process_transaction_exchange_rate
from utils_and_helpers.forms import optimistic_lock, get_locked_record
from utils_and_helpers.lists import check_list_not_empty
from . import purchases_bp

import logging

logger = logging.getLogger(__name__)


@purchases_bp.route('/add_purchase_order', methods=['GET', 'POST'])
@login_required
def add_purchase_order():
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]

    # Get warehouses, accounts payable accounts, AND expense accounts
    warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

    accounts_payable_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.parent_account_type == 'Liability',
            ChartOfAccounts.is_payable == True,
            ChartOfAccounts.is_system_account.is_(False)
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    grouped_payable_accounts = group_accounts_by_category(accounts_payable_accounts)

    expense_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.parent_account_type == 'Expense',
            ChartOfAccounts.is_system_account.is_(False)
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    grouped_expense_accounts = group_accounts_by_category(expense_accounts)

    if request.method == 'POST':

        try:
            check_list_not_empty(lst=request.form.getlist('item_type[]'))
            # Extract vendor data
            vendor_id = request.form.get('vendor_id')
            if vendor_id.isdigit():
                vendor_id = int(vendor_id)
            else:
                vendor_name = vendor_id
                existing_vendor = db_session.query(Vendor).filter_by(
                    vendor_name=vendor_name,
                    is_one_time=True,
                    app_id=app_id
                ).first()

                if existing_vendor:
                    vendor_id = existing_vendor.id
                else:
                    new_vendor = Vendor(
                        vendor_name=vendor_name,
                        is_one_time=True,
                        vendor_type="vendor",
                        app_id=app_id
                    )
                    db_session.add(new_vendor)
                    db_session.flush()
                    vendor_id = new_vendor.id

            # Extract purchase order details
            next_purchase_order_number = generate_next_purchase_order_number(db_session, app_id)
            purchase_order_number = next_purchase_order_number
            purchase_order_reference = request.form.get('purchase_reference') or None
            purchase_order_date = datetime.strptime(request.form['purchase_order_date'], '%Y-%m-%d').date()
            overall_line_subtotal = float(request.form['overall_line_subtotal'])
            total_amount = float(request.form['total_amount'])
            shipping_cost = float(request.form['shipping_cost'])
            handling_cost = float(request.form['handling_cost'])
            total_tax_amount = float(request.form['total_tax'])
            terms_and_conditions = request.form['terms_and_conditions'] or None
            currency = request.form['currency']

            # Accounts payable account
            accounts_payable_id = request.form.get('accounts_payable') or None
            if accounts_payable_id:
                accounts_payable_id = int(accounts_payable_id)

            # KEEP EXPENSE ACCOUNT ALLOCATION for proper accounting
            service_expense_account_id = int(request.form['expenseAccount']) if request.form.get(
                'expenseAccount') else None
            shipping_handling_account_id = int(request.form['shippingHandlingAccount']) if request.form.get(
                'shippingHandlingAccount') else None
            non_inventory_expense_account_id = int(request.form['nonInventoryExpenseAccount']) if request.form.get(
                'nonInventoryExpenseAccount') else None

            # NEW: Delivery fields
            delivery_date = (
                datetime.strptime(request.form['delivery_date'], '%Y-%m-%d').date()
                if request.form['delivery_date'] else (datetime.today().date() + timedelta(days=30))
            )

            shipping_address = request.form.get('shipping_address')  # Optional field
            delivery_method = request.form.get('delivery_method')  # Optional field

            net_inventory_total = request.form.get('net_inventory_total') or None
            net_non_inventory_total = request.form.get('net_non_inventory_total') or None
            total_expenses = request.form.get('total_expenses') or None

            purchase_order_discount_type = request.form['overall_discount_type'] or None
            purchase_order_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0
            purchase_order_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0
            project_id = request.form.get('project') or None

            if purchase_order_discount_type == "amount":
                calculated_discount_amount = purchase_order_discount_value
            else:
                calculated_discount_amount = (overall_line_subtotal * purchase_order_discount_value) / 100
            rate_obj = None
            # ===== HANDLE EXCHANGE RATE =====
            try:
                exchange_rate_id, exchange_rate_value, rate_obj = process_transaction_exchange_rate(
                    db_session=db_session,
                    request=request,
                    currency_id=currency,
                    transaction_date=purchase_order_date,
                    app_id=app_id,
                    created_by=current_user.id,
                    source_type='purchase_order',
                    source_id=None
                )
            except ValueError as e:
                return jsonify({'success': False, 'message': str(e)}), 400

            # Create new purchase order
            # Create new purchase order with expense accounts
            new_purchase_order = PurchaseOrder(
                purchase_order_number=purchase_order_number,
                purchase_order_reference=purchase_order_reference,
                vendor_id=vendor_id,
                purchase_order_date=purchase_order_date,
                accounts_payable_id=accounts_payable_id,
                # NEW: Add expense account references
                service_expense_account_id=service_expense_account_id,
                shipping_handling_account_id=shipping_handling_account_id,
                non_inventory_expense_account_id=non_inventory_expense_account_id,
                delivery_date=delivery_date,
                shipping_address=shipping_address,
                delivery_method=delivery_method,
                inventory_total=net_inventory_total,
                non_inventory_total=net_non_inventory_total,
                expenses_total=total_expenses,
                currency=currency,
                total_line_subtotal=overall_line_subtotal,
                total_amount=total_amount,
                purchase_order_discount_type=purchase_order_discount_type,
                purchase_order_discount_value=purchase_order_discount_value,
                calculated_discount_amount=calculated_discount_amount,
                purchase_order_tax_rate=purchase_order_tax_rate,
                total_tax_amount=total_tax_amount,
                shipping_cost=shipping_cost,
                handling_cost=handling_cost,
                status="draft",
                terms_and_conditions=terms_and_conditions,
                app_id=app_id,
                project_id=project_id,
                shipping_handling_posted=True,
                exchange_rate_id=exchange_rate_id,
                created_by=current_user.id
            )

            db_session.add(new_purchase_order)
            db_session.flush()

            # Update exchange rate with purchase order ID if one was created
            if rate_obj:
                rate_obj.source_id = new_purchase_order.id
                db_session.add(rate_obj)

            # Handle purchase order items
            item_type_list = request.form.getlist('item_type[]')
            item_name_list = request.form.getlist('item_name[]')
            inventory_item_list = request.form.getlist('inventory_item[]')
            description_list = request.form.getlist('item_description[]')
            qty_list = request.form.getlist('quantity[]')
            warehouse_list = request.form.getlist('warehouse[]')  # Get warehouse locations
            uom_list = request.form.getlist('uom[]')
            unit_price_list = request.form.getlist('unit_price[]')
            discount_list = request.form.getlist('discount_value[]')
            tax_list = request.form.getlist('tax_value[]')
            subtotal_list = request.form.getlist('total_price[]')

            for idx, (item_type, non_inventory_item, inventory_item, description, qty, uom, unit_price, warehouse,
                      discount_amount,
                      tax_amount, subtotal) in enumerate(zip(
                item_type_list, item_name_list, inventory_item_list, description_list, qty_list, uom_list,
                unit_price_list, warehouse_list, discount_list, tax_list, subtotal_list),
                start=1):

                valid_item_id = int(inventory_item) if item_type == "inventory" and inventory_item else None
                item_name = None if item_type == "inventory" else non_inventory_item

                if item_type == "inventory":
                    inventory_item = db_session.query(InventoryItem).filter_by(id=valid_item_id, app_id=app_id).first()
                    description = inventory_item.item_description if inventory_item else None
                    try:
                        if not warehouse or str(warehouse).strip() == '':
                            raise ValueError("Location (warehouse) must be specified.")
                        location_id = int(warehouse)
                    except ValueError as e:
                        raise ValueError(f"Invalid warehouse ID: {e}")
                else:
                    # For non-inventory items, location_id can be None or set to a default
                    location_id = None  # or set to a default location if your schema allows

                discount_amount = float(discount_amount) if discount_amount else 0
                tax_amount = float(tax_amount) if tax_amount else 0

                subtotal_after_tax = float(subtotal) - tax_amount if float(subtotal) != 0 else 1
                subtotal_after_discount = float(subtotal) - tax_amount + discount_amount if float(subtotal) != 0 else 1

                discount_rate = round((discount_amount / subtotal_after_discount) * 100, 2)
                tax_rate = round((tax_amount / subtotal_after_tax) * 100, 2)
                # Handle warehouse/location

                new_item = PurchaseOrderItem(
                    purchase_order_id=new_purchase_order.id,
                    item_type=item_type,
                    item_id=valid_item_id,
                    item_name=item_name,
                    description=description,
                    quantity=qty,
                    currency=currency,
                    unit_price=unit_price,
                    total_price=subtotal,
                    uom=uom,
                    discount_amount=discount_amount,
                    discount_rate=discount_rate,
                    tax_amount=tax_amount,
                    tax_rate=tax_rate,
                    app_id=app_id,
                    location_id=location_id
                )

                db_session.add(new_item)

            db_session.flush()

            # Log history
            history_entry = PurchaseOrderHistory(
                purchase_order_id=new_purchase_order.id,
                changed_by=current_user.id,
                change_description="Purchase Order Created",
                app_id=app_id
            )
            db_session.add(history_entry)

            # Add status log
            status_log_entry = PurchaseOrderStatusLog(
                purchase_order_id=new_purchase_order.id,
                status="draft",
                changed_by=current_user.id,
                app_id=app_id
            )
            db_session.add(status_log_entry)
            db_session.commit()

            return jsonify({
                'success': True,
                'message': 'Purchase Order added successfully!',
                'purchase_order_id': purchase_order_number
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f'An error occurred: {str(e)}\n{traceback.format_exc()}')
            return jsonify({
                'success': False,
                'message': f'An error occurred: {str(e)}'
            })

        finally:
            db_session.close()

    else:

        # List of acceptable vendor types
        vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers', 'partner', 'other']

        # Normalize to lowercase for consistency
        vendor_types = [v.lower() for v in vendor_types]

        # Query vendors whose vendor_type matches any in the list
        vendors = (
            db_session.query(Vendor)
            .filter(
                Vendor.app_id == app_id,
                func.lower(Vendor.vendor_type).in_(vendor_types)
            )
            .all()
        )
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id
        inventory_items = db_session.query(InventoryItemVariationLink).join(InventoryItem).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.status == "active"
        ).order_by(InventoryItem.item_name.asc()).all()

        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()
        projects = db_session.query(Project).filter_by(app_id=app_id).all()
        return render_template('/purchases/new_purchase_order.html',
                               base_currency=base_currency,
                               base_currency_id=base_currency_id,
                               base_currency_code=base_currency_code,
                               currencies=currencies,
                               inventory_items=inventory_items,
                               uoms=uoms,
                               vendors=vendors,
                               modules=modules_data,
                               company=company,
                               role=role,
                               projects=projects,
                               warehouses=warehouses,
                               grouped_payable_accounts=grouped_payable_accounts,
                               grouped_expense_accounts=grouped_expense_accounts)


@purchases_bp.route('/edit_purchase_order/<int:purchase_order_id>', methods=['GET', 'POST'])
@login_required
def edit_purchase_order(purchase_order_id):
    app_id = current_user.app_id
    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

    # Get warehouses and accounts for the form
    warehouses = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()

    accounts_payable_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.parent_account_type == 'Liability',
            ChartOfAccounts.is_payable == True,
            ChartOfAccounts.is_system_account.is_(False)
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    expense_accounts = (
        db_session.query(ChartOfAccounts)
        .filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.parent_account_type == 'Expense',
            ChartOfAccounts.is_system_account.is_(False)
        )
        .order_by(ChartOfAccounts.category.asc(), ChartOfAccounts.sub_category.asc())
        .all()
    )

    grouped_expense_accounts = group_accounts_by_category(expense_accounts)
    grouped_payable_accounts = group_accounts_by_category(accounts_payable_accounts)

    purchase_order = db_session.query(PurchaseOrder).options(
        joinedload(PurchaseOrder.exchange_rate)  # Eager load exchange rate
    ).filter_by(id=purchase_order_id, app_id=app_id).first()

    if not purchase_order:
        db_session.close()
        return jsonify({'success': False, 'message': 'Purchase Order not found'}), 404

    if request.method == 'POST':
        try:
            form_version = request.form.get('version', type=int)
            # Lock the record
            locked_purchase_order = get_locked_record(db_session, PurchaseOrder, purchase_order_id, form_version)

            if not locked_purchase_order:
                return jsonify({
                    'success': False,
                    'message': 'This purchase order was modified by another user. Please refresh and try again.',
                    'error_type': 'optimistic_lock_failed'
                }), 409  # 409 Conflict is appropriate for version conflicts

            # Now work with the locked record
            purchase_order = locked_purchase_order

            check_list_not_empty(request.form.getlist('item_type[]'))

            # Extract vendor data
            vendor_id = request.form.get('vendor_id', '').strip()
            vendor_name = request.form.get('vendor_name', '').strip()

            if vendor_id and vendor_id.isdigit():
                vendor_id = int(vendor_id)
            else:
                existing_vendor = db_session.query(Vendor).filter_by(
                    vendor_name=vendor_name,
                    is_one_time=True,
                    app_id=app_id
                ).first()

                if existing_vendor:
                    vendor_id = existing_vendor.id
                else:
                    new_vendor = Vendor(
                        vendor_name=vendor_name,
                        is_one_time=True,
                        vendor_type="Vendor",
                        app_id=app_id
                    )
                    db_session.add(new_vendor)
                    db_session.flush()
                    vendor_id = new_vendor.id

            # Extract accounting details
            accounts_payable_id = request.form.get('accounts_payable') or None
            if accounts_payable_id:
                accounts_payable_id = int(accounts_payable_id)

            # Extract expense account references
            service_expense_account_id = int(request.form['expenseAccount']) if request.form.get(
                'expenseAccount') else None
            shipping_handling_account_id = int(request.form['shippingHandlingAccount']) if request.form.get(
                'shippingHandlingAccount') else None
            non_inventory_expense_account_id = int(request.form['nonInventoryExpenseAccount']) if request.form.get(
                'nonInventoryExpenseAccount') else None

            # Extract purchase order reference
            purchase_order_reference = request.form.get('purchase_reference') or None

            # Updating general purchase_order details
            purchase_order.vendor_id = vendor_id
            purchase_order.purchase_order_date = datetime.strptime(request.form['purchase_order_date'],
                                                                   '%Y-%m-%d').date()
            purchase_order.purchase_order_reference = purchase_order_reference
            purchase_order.accounts_payable_id = accounts_payable_id
            purchase_order.service_expense_account_id = service_expense_account_id
            purchase_order.shipping_handling_account_id = shipping_handling_account_id
            purchase_order.non_inventory_expense_account_id = non_inventory_expense_account_id

            # Delivery information
            purchase_order.delivery_date = datetime.strptime(request.form['delivery_date'], '%Y-%m-%d').date() if \
                request.form['delivery_date'] else None
            purchase_order.shipping_address = request.form.get('shipping_address', '')
            purchase_order.delivery_method = request.form.get('delivery_method', '')

            net_inventory_total = request.form.get('net_inventory_total') or None
            net_non_inventory_total = request.form.get('net_non_inventory_total') or None
            total_expenses = request.form.get('total_expenses') or None

            purchase_order.inventory_total = net_inventory_total
            purchase_order.non_inventory_total = net_non_inventory_total
            purchase_order.expenses_total = total_expenses

            purchase_order.total_amount = float(request.form['total_amount'])

            purchase_order_discount_type = request.form['overall_discount_type'] or None
            purchase_order_discount_value = float(request.form['overall_discount_value']) if request.form[
                'overall_discount_value'] else 0.0
            project_id_str = request.form['project'] or None
            project_id = empty_to_none(project_id_str)
            total_tax_amount = request.form['total_tax']

            purchase_order_tax_rate = float(request.form['overall_tax']) if request.form['overall_tax'] else 0

            overall_line_subtotal = float(request.form['overall_line_subtotal'])

            if purchase_order_discount_type == "amount":
                calculated_discount_amount = purchase_order_discount_value
            else:
                calculated_discount_amount = (overall_line_subtotal * purchase_order_discount_value) / 100

            purchase_order.calculated_discount_amount = calculated_discount_amount
            purchase_order.total_line_subtotal = overall_line_subtotal
            purchase_order.total_tax_amount = total_tax_amount
            purchase_order.purchase_order_tax_rate = purchase_order_tax_rate
            purchase_order.purchase_order_discount_type = purchase_order_discount_type
            purchase_order.purchase_order_discount_value = purchase_order_discount_value

            purchase_order.shipping_cost = float(request.form['shipping_cost']) if request.form['shipping_cost'] else 0
            purchase_order.handling_cost = float(request.form['handling_cost']) if request.form['handling_cost'] else 0
            purchase_order.currency = int(request.form['currency'])
            purchase_order.terms_and_conditions = request.form['terms_and_conditions'] or None
            purchase_order.status = "draft"
            purchase_order.updated_at = datetime.now()
            purchase_order.project_id = project_id
            purchase_order.shipping_handling_posted = True
            rate_obj = None
            # ===== HANDLE EXCHANGE RATE USING HELPER FUNCTION =====
            try:
                exchange_rate_id, exchange_rate_value, rate_obj = process_transaction_exchange_rate(
                    db_session=db_session,
                    request=request,
                    currency_id=purchase_order.currency,
                    transaction_date=purchase_order.purchase_order_date,
                    app_id=app_id,
                    created_by=current_user.id,
                    source_type='purchase_order',
                    source_id=purchase_order.id
                )

                # Update purchase order with exchange rate
                if exchange_rate_id:
                    purchase_order.exchange_rate_id = exchange_rate_id

            except ValueError as e:
                return jsonify({'success': False, 'message': str(e)}), 400

            # Handle purchase_order Items
            item_type_list = request.form.getlist('item_type[]')
            item_name_list = request.form.getlist('item_name[]')
            inventory_item_list = request.form.getlist('inventory_item[]')
            description_list = request.form.getlist('item_description[]')
            qty_list = request.form.getlist('quantity[]')
            uom_list = request.form.getlist('uom[]')
            unit_price_list = request.form.getlist('unit_price[]')
            warehouse_list = request.form.getlist('warehouse[]')  # NEW: Warehouse locations
            discount_list = request.form.getlist('discount_value[]')
            tax_list = request.form.getlist('tax_value[]')
            subtotal_list = request.form.getlist('total_price[]')

            # Remove old items and re-add updated items
            db_session.query(PurchaseOrderItem).filter_by(purchase_order_id=purchase_order.id).delete()

            for idx, (item_type, non_inventory_item, inventory_item, description, qty, uom, unit_price, warehouse,
                      discount_amount,
                      tax_amount, subtotal) in enumerate(zip(
                item_type_list, item_name_list, inventory_item_list, description_list, qty_list, uom_list,
                unit_price_list, warehouse_list, discount_list, tax_list, subtotal_list), start=1):

                # Handling item ID based on type
                valid_item_id = int(inventory_item) if item_type == "inventory" and inventory_item else None
                item_id = None if item_type != "inventory" else valid_item_id

                # If not an inventory item, store item name instead
                item_name = None if item_type == "inventory" else non_inventory_item

                # Fetch description for inventory items
                if item_type == "inventory":
                    inventory_item_obj = db_session.query(InventoryItem).filter_by(id=item_id, app_id=app_id).first()
                    description = inventory_item_obj.item_description if inventory_item_obj else description
                    # Handle warehouse/location
                    try:
                        if not warehouse or str(warehouse).strip() == '':
                            raise ValueError("Location (warehouse) must be specified.")
                        location_id = int(warehouse)
                    except ValueError as e:
                        raise ValueError(f"Invalid warehouse ID: {e}")

                else:
                    # For non-inventory items, location_id can be None or set to a default
                    location_id = None  # or set to a default location if your schema allows

                # Convert values safely
                discount_amount = float(discount_amount) if discount_amount else 0
                tax_amount = float(tax_amount) if tax_amount else 0

                # Prevent division by zero
                subtotal_after_tax = float(subtotal) - tax_amount if float(subtotal) != 0 else 1
                subtotal_after_discount = float(subtotal) - tax_amount + discount_amount if float(subtotal) != 0 else 1

                discount_rate = round((discount_amount / subtotal_after_discount) * 100, 2)
                tax_rate = round((tax_amount / subtotal_after_tax) * 100, 2)

                # Create updated PurchaseOrderItem with new fields
                updated_item = PurchaseOrderItem(
                    purchase_order_id=purchase_order.id,
                    item_type=item_type,
                    item_id=item_id,
                    item_name=item_name,
                    description=description,
                    quantity=qty,
                    currency=purchase_order.currency,
                    unit_price=unit_price,
                    total_price=subtotal,
                    uom=uom,
                    discount_amount=discount_amount,
                    discount_rate=discount_rate,
                    tax_amount=tax_amount,
                    tax_rate=tax_rate,
                    location_id=location_id,  # NEW: Warehouse location
                    unit_cost=None,  # Will be calculated when PO is converted
                    total_cost=None,  # Will be calculated when PO is converted
                    app_id=app_id
                )

                db_session.add(updated_item)

            # Log history entry for purchase_order update
            history_entry = PurchaseOrderHistory(
                purchase_order_id=purchase_order.id,
                changed_by=current_user.id,
                change_description="Purchase Order Updated",
                app_id=app_id
            )
            db_session.add(history_entry)

            # Add status log entry during purchase_order update
            status_log_entry = PurchaseOrderStatusLog(
                purchase_order_id=purchase_order.id,
                status="draft",  # Keep as draft since it's an edit
                changed_by=current_user.id,
                app_id=app_id
            )
            db_session.add(status_log_entry)
            purchase_order.version += 1
            db_session.commit()

            return jsonify({
                'success': True,
                'message': 'Purchase Order updated successfully!',
                'purchase_order_id': purchase_order.id
            })

        except ValueError as e:
            db_session.rollback()
            return jsonify({
                'success': False,
                'message': f'An error occurred: Please place at least one line item'
            })

        except Exception as e:
            db_session.rollback()
            logger.error(f"An error occurred: {str(e)}\n{traceback.format_exc()}")
            return jsonify({
                'success': False,
                'message': f'An error occurred: {str(e)}'
            })

        finally:
            db_session.close()

    else:
        # GET request - Fetch existing data for the form
        vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers', 'partner', 'other']

        # Normalize to lowercase for consistency
        vendor_types = [v.lower() for v in vendor_types]

        # Query vendors whose vendor_type matches any in the list
        vendors = (
            db_session.query(Vendor)
            .filter(
                Vendor.app_id == app_id,
                func.lower(Vendor.vendor_type).in_(vendor_types)
            )
            .all()
        )

        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id
        inventory_items = db_session.query(InventoryItemVariationLink).join(InventoryItem).filter(
            InventoryItemVariationLink.app_id == app_id,
            InventoryItemVariationLink.status == "active"
        ).order_by(InventoryItem.item_name.asc()).all()

        uoms = db_session.query(UnitOfMeasurement).filter_by(app_id=app_id).all()
        projects = db_session.query(Project).filter_by(app_id=app_id).all()

        # Render the template with purchase order details and all required data
        return render_template('/purchases/edit_purchase_order.html',
                               purchase_order=purchase_order,
                               currencies=currencies,
                               base_currency=base_currency,
                               base_currency_id=base_currency_id,
                               base_currency_code=base_currency_code,
                               inventory_items=inventory_items,
                               uoms=uoms,
                               vendors=vendors,
                               company=company,
                               role=role,
                               modules=modules_data,
                               projects=projects,
                               warehouses=warehouses,
                               grouped_payable_accounts=grouped_payable_accounts,
                               grouped_expense_accounts=grouped_expense_accounts)


@purchases_bp.route('/purchase_orders', methods=['GET', 'POST'])
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
                    start_date = datetime.strptime(start_date, '%Y-%m-%d')
                if end_date:
                    end_date = datetime.strptime(end_date, '%Y-%m-%d')

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
        current_date = datetime.now()
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
        draft_purchase_orders = query.filter(PurchaseOrder.status == OrderStatus.draft).count()
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
            draft_purchase_orders=draft_purchase_orders,
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


@purchases_bp.route('/purchase_order/<int:purchase_order_id>', methods=['GET'])
@login_required
def purchase_order_details(purchase_order_id):
    app_id = current_user.app_id
    with Session() as db_session:
        company = db_session.query(Company).filter_by(id=app_id).first()
        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        payment_modes = db_session.query(PaymentMode).filter_by(app_id=app_id).all()
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id
        # Get purchase order with related data
        purchase_order = db_session.query(PurchaseOrder).options(
            joinedload(PurchaseOrder.vendor),
            joinedload(PurchaseOrder.currencies),
            joinedload(PurchaseOrder.purchase_order_notes).joinedload(PurchaseOrderNote.user),
            joinedload(PurchaseOrder.purchase_order_notes).joinedload(PurchaseOrderNote.user_recipient),
            joinedload(PurchaseOrder.purchase_order_items).joinedload(PurchaseOrderItem.inventory_item_variation_link)
            .joinedload(InventoryItemVariationLink.inventory_item),
            joinedload(PurchaseOrder.purchase_order_items).joinedload(PurchaseOrderItem.unit_of_measurement),
            joinedload(PurchaseOrder.purchase_order_items).joinedload(PurchaseOrderItem.location),  # ADD THIS LINE
            joinedload(PurchaseOrder.purchase_transactions).joinedload(PurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.payment_modes),
            joinedload(PurchaseOrder.purchase_transactions).joinedload(PurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.chart_of_accounts_asset)
        ).filter_by(id=purchase_order_id, app_id=app_id).first()

        if not purchase_order:
            abort(404, "Purchase Order not found")

        # Check if this PO includes inventory items
        inventory_item_exist = any(
            item.item_type == "inventory" for item in purchase_order.purchase_order_items
        )

        # Check if all items have been received
        total_order_quantity = sum(item.quantity for item in purchase_order.purchase_order_items)

        # Initialize total received quantity
        total_received_quantity = 0

        has_unposted_advance_payment = any(
            allocation.payment_type == "advance_payment" and not allocation.is_posted_to_ledger
            for txn in purchase_order.purchase_transactions
            for allocation in txn.payment_allocations
        )

        # Iterate over each goods receipt to sum the quantities of received items
        for receipt in purchase_order.goods_receipts:
            total_received_quantity += sum(item.quantity_received for item in receipt.receipt_items)

        all_goods_received = total_order_quantity == total_received_quantity

        # Calculate total paid and remaining balance
        total_paid = db_session.query(
            func.sum(PurchaseTransaction.amount_paid)).filter(
            PurchaseTransaction.purchase_order_id == purchase_order_id,
            PurchaseTransaction.payment_status != PurchasePaymentStatus.cancelled
        ).scalar() or Decimal('0.00')

        remaining_balance = purchase_order.total_amount - total_paid

        # REPLACED: Get tax account from JournalEntry instead of Transaction
        tax_account_entry = db_session.query(JournalEntry).filter_by(
            source_type="purchase_order_tax",
            source_id=purchase_order_id,
            app_id=app_id
        ).join(JournalEntry.chart_of_accounts).first()

        # Add this query to get purchase returns
        purchase_returns = db_session.query(PurchaseReturn).options(
            joinedload(PurchaseReturn.receipt_items)
            .joinedload(GoodsReceiptItem.purchase_order_item)
        ).filter(
            PurchaseReturn.purchase_order_id == purchase_order_id,
            PurchaseReturn.app_id == app_id
        ).all()

        # Calculate total returned amounts
        total_returned_amount = sum(float(r.allocated_amount) for r in purchase_returns)
        total_returned_tax = sum(float(r.allocated_tax_amount) for r in purchase_returns)

        # NEW: Get journal entries related to this purchase order for ledger tracking
        purchase_order_journals = db_session.query(Journal).options(
            joinedload(Journal.entries).joinedload(JournalEntry.chart_of_accounts)
        ).filter(
            Journal.journal_ref_no == purchase_order.purchase_order_number,
            Journal.app_id == app_id
        ).all()

        # Prepare journal data for the response
        journal_data = []
        for journal in purchase_order_journals:
            journal_data.append({
                "id": journal.id,
                "journal_number": journal.journal_number,
                "date": journal.date.strftime("%Y-%m-%d"),
                "narration": journal.narration,
                "status": journal.status,
                "total_debit": float(journal.total_debit),
                "total_credit": float(journal.total_credit),
                "entries": [
                    {
                        "id": entry.id,
                        "line_number": entry.line_number,
                        "account_name": entry.chart_of_accounts.sub_category if entry.chart_of_accounts else None,
                        "account_code": entry.chart_of_accounts.sub_category_id if entry.chart_of_accounts else None,
                        "amount": float(entry.amount),
                        "dr_cr": entry.dr_cr,
                        "description": entry.description
                    } for entry in journal.entries
                ]
            })

        # Prepare purchase order data
        purchase_order_data = {
            "id": purchase_order.id,
            "purchase_order_number": purchase_order.purchase_order_number,
            "purchase_order_date": purchase_order.purchase_order_date.strftime("%Y-%m-%d"),
            "delivery_date": purchase_order.delivery_date.strftime(
                "%Y-%m-%d") if purchase_order.delivery_date else None,
            "shipping_address": purchase_order.shipping_address,
            "delivery_method": purchase_order.delivery_method,
            "vendor": {
                "id": purchase_order.vendor.id,
                "name": purchase_order.vendor.vendor_name,
                "contact": f"{purchase_order.vendor.tel_contact}{' | ' + purchase_order.vendor.email if purchase_order.vendor.email else ''}",
                "address": purchase_order.vendor.address or None,
                "city_country": f"{purchase_order.vendor.city or ''}{', ' + purchase_order.vendor.country if purchase_order.vendor.country else ''}".strip() or None
            },
            "currency": purchase_order.currencies.user_currency if purchase_order.currency else None,
            "currency_id": purchase_order.currency if purchase_order.currency else None,
            "exchange_rate": purchase_order.exchange_rate.rate if purchase_order.exchange_rate else 1,
            "total_amount": float(purchase_order.total_amount),
            "total_paid": float(total_paid),
            "remaining_balance": float(remaining_balance),
            "status": purchase_order.status,
            "subtotal": float(purchase_order.total_line_subtotal),
            "shipping_handling_posted": purchase_order.shipping_handling_posted,
            "purchase_order_transactions": [
                {
                    "id": transaction.id,
                    "version": transaction.version,
                    "payment_date": transaction.payment_date.strftime("%Y-%m-%d"),
                    "amount_paid": float(transaction.amount_paid),
                    "reference_number": transaction.reference_number,
                    "payment_status": transaction.payment_status.value,
                    "is_posted_to_ledger": transaction.is_posted_to_ledger,
                    "payment_allocations": [
                        {
                            "id": allocation.id,
                            "allocated_inventory": float(allocation.allocated_inventory),
                            "allocated_non_inventory": float(allocation.allocated_non_inventory),
                            "allocated_services": float(allocation.allocated_services),
                            "allocated_other_expenses": float(allocation.allocated_other_expenses),
                            "allocated_tax_receivable": float(allocation.allocated_tax_receivable),
                            "allocated_tax_payable": float(allocation.allocated_tax_payable),
                            "payment_type": allocation.payment_type,
                            "paying_account_id": allocation.payment_account_id,
                            "paying_account_name": f'{allocation.chart_of_accounts_asset.sub_category} - ({allocation.chart_of_accounts_asset.sub_category_id})' if allocation.chart_of_accounts_asset else None,
                            "is_posted_to_ledger": allocation.is_posted_to_ledger,
                            "payment_mode": allocation.payment_modes.payment_mode if allocation.payment_modes else None
                        } for allocation in transaction.payment_allocations
                    ]
                } for transaction in purchase_order.purchase_transactions
            ],
            "purchase_order_notes": [
                {
                    "id": note.id,
                    "content": note.note_content,
                    "type": note.note_type,
                    "created_by": note.user.name,
                    "recipient": note.user_recipient.name if note.user_recipient else None,
                    "created_at": note.created_at.strftime("%Y-%m-%d %H:%M") if note.created_at else None
                } for note in purchase_order.purchase_order_notes
            ],
            "purchase_order_items": [
                {
                    "id": item.id,
                    "item_type": item.item_type,
                    "item_id": item.item_id,
                    "item_name": item.item_name if item.item_name else (
                        item.inventory_item_variation_link.inventory_item.item_name
                        if item.inventory_item_variation_link and item.inventory_item_variation_link.inventory_item
                        else None
                    ),
                    "description": item.description if item.description else (
                        item.inventory_item_variation_link.inventory_item.item_description
                        if item.inventory_item_variation_link and item.inventory_item_variation_link.inventory_item
                        else None
                    ),
                    "quantity": float(item.quantity),
                    "uom": item.unit_of_measurement.full_name if item.unit_of_measurement else None,
                    "unit_price": float(item.unit_price),
                    "tax_rate": float(item.tax_rate),
                    "tax_amount": float(item.tax_amount),
                    "discount_amount": float(item.discount_amount),
                    "discount_rate": float(item.discount_rate),
                    "total_price": float(item.total_price),
                    "unit_cost": float(item.unit_cost) if item.unit_cost else None,
                    "total_cost": float(item.total_cost) if item.total_cost else None,
                    "location_id": item.location_id,
                    "location": item.location.location if item.location else None
                    # CORRECT: Use 'location' not 'location_name'
                } for item in purchase_order.purchase_order_items
            ],
            "terms_and_conditions": purchase_order.terms_and_conditions,
            "purchase_order_discount": float(purchase_order.calculated_discount_amount),
            "purchase_order_discount_type": purchase_order.purchase_order_discount_type,
            "purchase_order_tax_rate": float(purchase_order.purchase_order_tax_rate),
            "total_tax_amount": float(purchase_order.total_tax_amount),
            "shipping_cost": float(purchase_order.shipping_cost),
            "handling_cost": float(purchase_order.handling_cost),
            # REPLACED: Tax accounts from JournalEntry
            "tax_accounts": [
                {
                    'tax_account_id': tax_account_entry.subcategory_id,
                    'tax_account': tax_account_entry.chart_of_accounts.sub_category if tax_account_entry.chart_of_accounts else None,
                    'tax_account_category_id': tax_account_entry.chart_of_accounts.categories.id if tax_account_entry and tax_account_entry.chart_of_accounts and tax_account_entry.chart_of_accounts.categories else None,
                    'tax_account_category': tax_account_entry.chart_of_accounts.categories.category if tax_account_entry and tax_account_entry.chart_of_accounts and tax_account_entry.chart_of_accounts.categories else None,
                }
            ] if tax_account_entry else [],
            # NEW: Journal data for ledger tracking
            "journals": journal_data,
            "users": [{
                "id": user.id,
                "name": user.name,
                "email": user.email
            } for user in company.users],
            "goods_receipts": [
                {
                    "id": receipt.id,
                    "receipt_number": receipt.receipt_number,
                    "receipt_date": receipt.receipt_date.strftime("%Y-%m-%d"),
                    "received_by": receipt.received_by,
                    "quantity_received": float(receipt.quantity_received),
                    "is_complete_receipt": receipt.is_complete_receipt,
                    "is_posted_to_ledger": receipt.is_posted_to_ledger,
                    "inventory_received": receipt.inventory_received,
                    "created_at": receipt.created_at.strftime("%Y-%m-%d %H:%M"),
                    "receipt_items": [
                        {
                            "id": item.id,
                            "purchase_order_item_id": item.purchase_order_item_id,
                            "quantity_received": float(item.quantity_received),
                            "allocated_amount": float(item.allocated_amount),
                            "received_condition": item.received_condition,
                            "notes": item.notes,
                            "inventory_adjusted": item.inventory_adjusted,
                            "is_posted_to_ledger": item.is_posted_to_ledger,
                            "item_type": next(
                                (poi.item_type for poi in purchase_order.purchase_order_items
                                 if poi.id == item.purchase_order_item_id),
                                None
                            ),
                            "item_name": (
                                lambda po_item: (
                                    po_item.item_name if po_item.item_name else (
                                        po_item.inventory_item_variation_link.inventory_item.item_name
                                        if po_item.inventory_item_variation_link and po_item.inventory_item_variation_link.inventory_item
                                        else None
                                    )
                                ) if po_item else None
                            )(next(
                                (poi for poi in purchase_order.purchase_order_items
                                 if poi.id == item.purchase_order_item_id), None
                            )),
                            "location": next(  # CORRECT: Use 'location' not 'location_name'
                                (poi.location.location for poi in purchase_order.purchase_order_items
                                 if poi.id == item.purchase_order_item_id and poi.location),
                                None
                            )
                        } for item in receipt.receipt_items
                    ]
                } for receipt in purchase_order.goods_receipts
            ],
            "purchase_returns": [
                {
                    "id": r.id,
                    "return_number": r.return_number,
                    "item_name": r.receipt_items.purchase_order_item.item_name if r.receipt_items.purchase_order_item.item_name else r.receipt_items.purchase_order_item.inventory_item_variation_link.inventory_item.item_name,
                    "return_date": r.return_date.strftime("%Y-%m-%d"),
                    "quantity": float(r.quantity),
                    "amount": float(r.allocated_amount),
                    "tax_amount": float(r.allocated_tax_amount),
                    "total_amount": float(r.allocated_amount + r.allocated_tax_amount),
                    "reason": r.reason,
                    "status": r.status,
                    "is_posted_to_ledger": r.is_posted_to_ledger
                } for r in purchase_returns
            ],
            "total_returned_amount": total_returned_amount,
            "total_returned_tax": total_returned_tax
        }

        if request.args.get('format') == 'json':
            return jsonify(purchase_order_data)

        return render_template('/purchases/purchase_order_details.html',
                               purchase_order=purchase_order_data,
                               company=company,
                               modules=modules_data,
                               currencies=currencies,
                               base_currency=base_currency,
                               base_currency_id=base_currency_id,
                               base_currency_code=base_currency_code,
                               payment_modes=payment_modes,
                               inventory_item_exist=inventory_item_exist,
                               all_goods_received=all_goods_received,
                               role=role,
                               has_unposted_advance_payment=has_unposted_advance_payment,
                               module_name="Purchase")


@purchases_bp.route('/bulk_approve_purchase_orders', methods=['POST'])
@login_required
def bulk_approve_purchase_orders():
    db_session = Session()
    try:
        data = request.get_json()
        po_ids = data.get('po_ids', [])

        if not po_ids:
            return jsonify({"status": "error", "message": "No purchase order IDs provided"}), 400

        approved_pos = 0
        failed_pos = []
        error_messages = []

        for po_id in po_ids:
            try:
                purchase_order = db_session.query(PurchaseOrder).filter_by(id=po_id).first()
                if not purchase_order:
                    failed_pos.append(po_id)
                    error_messages.append(f"Purchase Order {po_id} not found")
                    continue

                # Check if PO is in draft status (only draft POs can be approved)
                if purchase_order.status != OrderStatus.draft:
                    failed_pos.append(po_id)
                    error_messages.append(
                        f"Purchase Order {purchase_order.purchase_order_number} is not in draft status")
                    continue

                # Update the purchase order status to approved
                purchase_order.status = OrderStatus.approved

                # Create approval history record
                approval_record = PurchaseOrderApproval(
                    purchase_order_id=purchase_order.id,
                    approver_id=current_user.id,
                    approval_date=datetime.now(timezone.utc),
                    approval_status='approved',
                    comments='Bulk approval',
                    app_id=current_user.app_id
                )
                db_session.add(approval_record)

                # Create status log
                status_log = PurchaseOrderStatusLog(
                    purchase_order_id=purchase_order.id,
                    changed_by=current_user.id,
                    status=OrderStatus.approved.value,
                    change_date=datetime.now(timezone.utc),
                    app_id=current_user.app_id
                )
                db_session.add(status_log)

                approved_pos += 1

            except Exception as e:
                failed_pos.append(po_id)
                error_messages.append(f"Error approving Purchase Order {po_id}: {str(e)}")
                continue

        db_session.commit()

        if approved_pos > 0:
            message = f"Successfully approved {approved_pos} purchase orders"
            if failed_pos:
                message += f". Failed to approve {len(failed_pos)} purchase orders"
            return jsonify({
                "status": "success",
                "message": message,
                "approved_count": approved_pos,
                "failed_count": len(failed_pos),
                "failed_pos": failed_pos[:10]  # Return first 10 failed IDs for reference
            })
        else:
            return jsonify({
                "status": "error",
                "message": "No purchase orders were approved. Errors: " + "; ".join(error_messages[:5]),
                "approved_count": 0,
                "failed_count": len(failed_pos)
            }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk_approve_purchase_orders: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": f"An error occurred during bulk approval: {str(e)}"
        }), 500
    finally:
        db_session.close()


@purchases_bp.route('/bulk_cancel_purchase_orders', methods=['POST'])
@login_required
def bulk_cancel_purchase_orders():
    db_session = Session()
    try:
        data = request.get_json()
        po_ids = data.get('po_ids', [])

        if not po_ids:
            return jsonify({"status": "error", "message": "No purchase order IDs provided"}), 400

        cancelled_pos = 0
        failed_pos = []
        error_messages = []

        for po_id in po_ids:
            try:
                # Fetch purchase order with related data in single query
                purchase_order = (
                    db_session.query(PurchaseOrder)
                    .options(
                        joinedload(PurchaseOrder.purchase_transactions),
                        joinedload(PurchaseOrder.goods_receipts)
                    )
                    .filter_by(id=po_id, app_id=current_user.app_id)
                    .first()
                )

                if not purchase_order:
                    failed_pos.append(po_id)
                    error_messages.append(f"Purchase Order {po_id} not found")
                    continue

                # Check cancellation restrictions
                cancellation_errors = []

                # 1. Check if goods have been received
                if any(receipt.quantity_received > 0 for receipt in purchase_order.goods_receipts):
                    cancellation_errors.append("Goods have already been received")

                # 2. Check if payments have been made
                if any(txn.amount_paid > 0 for txn in purchase_order.purchase_transactions):
                    cancellation_errors.append("Payments have already been made")

                # 3. Check if any transactions posted to ledger
                # if any(
                #         alloc.is_posted_to_ledger
                #         for txn in purchase_order.purchase_transactions
                #         for alloc in txn.payment_allocations
                # ):
                #     cancellation_errors.append("Transactions have been posted to ledger")

                if cancellation_errors:
                    failed_pos.append(po_id)
                    error_messages.append(
                        f"Purchase Order {purchase_order.purchase_order_number}: {', '.join(cancellation_errors)}")
                    continue

                # Store previous status for history
                previous_status = purchase_order.status

                # Update status based on current status
                if purchase_order.status == OrderStatus.approved:
                    purchase_order.status = OrderStatus.rejected
                else:
                    purchase_order.status = OrderStatus.canceled

                # Create history record
                history_entry = PurchaseOrderHistory(
                    purchase_order_id=purchase_order.id,
                    changed_by=current_user.id,
                    change_date=datetime.now(timezone.utc),
                    change_description=(
                        f"Status changed from {previous_status} to {purchase_order.status} "
                        f"(Bulk cancellation by {current_user.name})"
                    ),
                    app_id=current_user.app_id
                )
                db_session.add(history_entry)

                # Create status log
                status_log = PurchaseOrderStatusLog(
                    purchase_order_id=purchase_order.id,
                    changed_by=current_user.id,
                    status=purchase_order.status.value,
                    change_date=datetime.now(timezone.utc),
                    app_id=current_user.app_id
                )
                db_session.add(status_log)

                cancelled_pos += 1

            except Exception as e:
                failed_pos.append(po_id)
                error_messages.append(f"Error cancelling Purchase Order {po_id}: {str(e)}")
                continue

        db_session.commit()

        if cancelled_pos > 0:
            message = f"Successfully cancelled {cancelled_pos} purchase orders"
            if failed_pos:
                message += f". Failed to cancel {len(failed_pos)} purchase orders"
            return jsonify({
                "status": "success",
                "message": message,
                "cancelled_count": cancelled_pos,
                "failed_count": len(failed_pos),
                "failed_pos": failed_pos[:10]
            })
        else:
            return jsonify({
                "status": "error",
                "message": "No purchase orders were cancelled. Errors: " + "; ".join(error_messages[:5]),
                "cancelled_count": 0,
                "failed_count": len(failed_pos)
            }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk_cancel_purchase_orders: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": f"An error occurred during bulk cancellation: {str(e)}"
        }), 500
    finally:
        db_session.close()


@purchases_bp.route('/bulk_delete_purchase_orders', methods=['POST'])
@login_required
def bulk_delete_purchase_orders():
    db_session = Session()
    try:
        data = request.get_json()
        po_ids = data.get('po_ids', [])

        if not po_ids:
            return jsonify({"status": "error", "message": "No purchase order IDs provided"}), 400

        deleted_pos = 0
        failed_pos = []
        error_messages = []

        for po_id in po_ids:
            try:
                # Fetch purchase order with all related data for validation and deletion
                purchase_order = (
                    db_session.query(PurchaseOrder)
                    .options(
                        joinedload(PurchaseOrder.purchase_transactions),
                        joinedload(PurchaseOrder.goods_receipts),
                        joinedload(PurchaseOrder.purchase_order_items),
                        joinedload(PurchaseOrder.purchase_order_history),
                        joinedload(PurchaseOrder.purchase_order_status_logs),
                        joinedload(PurchaseOrder.purchase_order_notes),
                        joinedload(PurchaseOrder.purchase_order_approvals),
                        joinedload(PurchaseOrder.purchase_returns)
                    )
                    .filter_by(id=po_id, app_id=current_user.app_id)
                    .first()
                )

                if not purchase_order:
                    failed_pos.append(po_id)
                    error_messages.append(f"Purchase Order {po_id} not found")
                    continue

                # Check deletion restrictions - more strict than cancellation
                deletion_errors = []

                # 1. Check if goods have been received (any receipt with quantity > 0)
                if any(receipt.quantity_received > 0 for receipt in purchase_order.goods_receipts):
                    deletion_errors.append("Goods have been received - cannot delete")

                # 2. Check if any payments have been made (any payment > 0)
                if any(txn.amount_paid > 0 for txn in purchase_order.purchase_transactions):
                    deletion_errors.append("Payments have been made - cannot delete")

                # 3. Check if any transactions posted to ledger
                # if any(
                #         alloc.is_posted_to_ledger
                #         for txn in purchase_order.purchase_transactions
                #         for alloc in txn.payment_allocations
                # ):
                #     deletion_errors.append("Transactions posted to ledger - cannot delete")

                # 4. Additional check: Only allow deletion of draft or rejected POs
                if purchase_order.status not in [OrderStatus.draft, OrderStatus.rejected, OrderStatus.canceled]:
                    deletion_errors.append(f"Cannot delete purchase order with status: {purchase_order.status.value}")

                if deletion_errors:
                    failed_pos.append(po_id)
                    error_messages.append(
                        f"Purchase Order {purchase_order.purchase_order_number}: {', '.join(deletion_errors)}")
                    continue

                # Delete related records in correct order to maintain referential integrity

                # 1. Delete purchase returns
                for purchase_return in purchase_order.purchase_returns:
                    db_session.delete(purchase_return)

                # 2. Delete goods receipts and their items
                for goods_receipt in purchase_order.goods_receipts:
                    # Delete goods receipt items first
                    for receipt_item in goods_receipt.receipt_items:
                        db_session.delete(receipt_item)
                    db_session.delete(goods_receipt)

                # 3. Delete purchase transactions and their allocations
                for transaction in purchase_order.purchase_transactions:
                    # Delete payment allocations first
                    for allocation in transaction.payment_allocations:
                        db_session.delete(allocation)
                    db_session.delete(transaction)

                # 4. Delete purchase order items
                for item in purchase_order.purchase_order_items:
                    db_session.delete(item)

                # 5. Delete history and logs
                for history in purchase_order.purchase_order_history:
                    db_session.delete(history)

                for status_log in purchase_order.purchase_order_status_logs:
                    db_session.delete(status_log)

                for note in purchase_order.purchase_order_notes:
                    db_session.delete(note)

                for approval in purchase_order.purchase_order_approvals:
                    db_session.delete(approval)

                # 6. Finally delete the purchase order itself
                db_session.delete(purchase_order)
                deleted_pos += 1

            except Exception as e:
                failed_pos.append(po_id)
                error_messages.append(f"Error deleting Purchase Order {po_id}: {str(e)}")
                continue

        db_session.commit()

        if deleted_pos > 0:
            message = f"Successfully deleted {deleted_pos} purchase orders"
            if failed_pos:
                message += f". Failed to delete {len(failed_pos)} purchase orders"
            return jsonify({
                "status": "success",
                "message": message,
                "deleted_count": deleted_pos,
                "failed_count": len(failed_pos),
                "failed_pos": failed_pos[:10]
            })
        else:
            return jsonify({
                "status": "error",
                "message": "No purchase orders were deleted. Errors: " + "; ".join(error_messages[:5]),
                "deleted_count": 0,
                "failed_count": len(failed_pos)
            }), 400

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error in bulk_delete_purchase_orders: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            "status": "error",
            "message": f"An error occurred during bulk deletion: {str(e)}"
        }), 500
    finally:
        db_session.close()


@purchases_bp.route('/add_purchase_order_note', methods=['POST'])
@login_required
def add_purchase_order_note():
    try:
        # Validate CSRF token
        validate_csrf(request.headers.get('X-CSRFToken'))
    except BadRequest:
        return jsonify({"error": "Invalid CSRF token"}), 400

    data = request.get_json()
    note_content = data.get('note_content')
    purchase_order_id = data.get('purchase_order_id')
    created_by = current_user.id  # Current user is the creator
    recipient = data.get('recipient') or None
    purchase_order_number = data.get('purchase_order_number')
    company_id = current_user.app_id

    if not note_content or not purchase_order_id:
        return jsonify({
            'success': False,
            "message": "Note content and purchase order ID are required",
            "status": "danger"
        }), 400

    try:
        with Session() as db_session:
            # Create new purchase order note
            new_note = PurchaseOrderNote(
                purchase_order_id=purchase_order_id,
                note_type='internal',  # Default to internal note
                note_content=note_content,
                created_by=created_by,
                recipient=recipient,
                app_id=company_id
            )
            db_session.add(new_note)
            db_session.flush()

            # Create notification message
            notification_message = (
                f"A new note has been added to purchase order #{purchase_order_number} "
                f"by {current_user.name}."
            )

            # Create appropriate notification
            if not recipient:
                # Notify the entire company
                create_notification(
                    db_session=db_session,
                    company_id=company_id,
                    message=notification_message,
                    type='info',
                    is_popup=True,
                    url=f"/purchases/purchase_order/{purchase_order_id}"
                )
            else:
                # Notify specific recipient
                create_notification(
                    db_session=db_session,
                    user_id=recipient,
                    message=notification_message,
                    type='info',
                    is_popup=True,
                    url=f"/purchases/purchase_order/{purchase_order_id}"
                )

            db_session.commit()

            # Get recipient name if exists
            recipient_name = None
            if recipient:
                recipient_user = db_session.query(User).get(recipient)
                recipient_name = recipient_user.name if recipient_user else None

            # Return success response with note details
            return jsonify({
                "id": new_note.id,
                "note_content": new_note.note_content,
                "created_at": new_note.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "created_by": current_user.name,
                "recipient": recipient_name,
                "message": "Note added successfully",
                "status": "success"
            }), 201

    except Exception as e:
        return jsonify({
            "message": f"Error adding note: {str(e)}",
            "status": "danger"
        }), 500


@purchases_bp.route('/api/record_purchase_payment', methods=['POST'])
def record_purchase_payment():
    try:
        # Parse input data
        purchase_order_id = int(request.form.get('purchaseOrderId'))
        amount = request.form.get('amount')
        payment_date = request.form.get('paymentDate')
        asset_account_id = int(request.form.get('fundingAccount'))
        prepaid_account_id = request.form.get('prepaidAccount')
        reference = request.form.get('reference') or None
        payment_method = request.form.get('paymentMethod') or None
        payment_method = int(payment_method) if payment_method and payment_method.strip().isdigit() else None
        payment_type = request.form.get('paymentType')
        update_existing_journals = request.form.get('update_existing_journals') == 'true'
        payment_currency_id = request.form.get('payment_currency')  # This comes from your hidden select
        base_currency_id = request.form.get('base_currency_id')
        exchange_rate = request.form.get('exchange_rate')
        exchange_rate_value = float(exchange_rate) if exchange_rate else None
        created_by = current_user.id
        app_id = current_user.app_id

        # Validate required fields
        if not all([purchase_order_id, amount, payment_date, asset_account_id]):
            flash('Missing required fields.', 'error')
            return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

        # Validate amount and date
        try:
            amount = Decimal(amount)
            payment_date = datetime.strptime(payment_date, '%Y-%m-%d')
        except (InvalidOperation, ValueError) as e:
            flash('Invalid amount or date format.', 'error')
            return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

        # Start database session
        db_session = Session()

        try:
            # Fetch purchase order with all transactions pre-loaded
            purchase_order = db_session.query(PurchaseOrder) \
                .options(joinedload(PurchaseOrder.purchase_transactions)) \
                .filter_by(id=purchase_order_id) \
                .first()

            if not purchase_order:
                flash('Purchase Order not found.', 'error')
                return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

            # Calculate remaining balance
            total_paid = db_session.query(func.sum(PurchaseTransaction.amount_paid)).filter(
                PurchaseTransaction.purchase_order_id == purchase_order_id,
                PurchaseTransaction.payment_status.in_(['paid', 'partial'])
            ).scalar() or Decimal('0.00')

            remaining_balance = purchase_order.total_amount - total_paid

            if amount <= 0 or amount > remaining_balance:
                flash(f'Invalid payment amount. Must be between 0 and {remaining_balance:.2f}', 'error')
                return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

            # Handle prepaid account changes for advance payments
            if payment_type == 'advance_payment' and prepaid_account_id:
                prepaid_account_id = int(prepaid_account_id)
                # UPDATE: Set the preferred prepaid account on the purchase order
                if purchase_order.preferred_prepaid_account_id != prepaid_account_id:
                    purchase_order.preferred_prepaid_account_id = prepaid_account_id

                # Check if we need to update existing journals
                if update_existing_journals:
                    update_result = update_prepaid_journals_for_po(
                        db_session, purchase_order_id, prepaid_account_id, created_by
                    )
                    if not update_result['success']:
                        flash(f'Failed to update existing journals: {update_result["error"]}', 'error')
                        return redirect(url_for('purchase_order_details', purchase_order_id=purchase_order_id))

            # Determine payment status
            payment_status = PurchasePaymentStatus.paid if amount == remaining_balance else PurchasePaymentStatus.partial
            rate_obj = None
            # After getting purchase_order, before creating payment
            try:
                exchange_rate_id, exchange_rate_value, rate_obj = process_transaction_exchange_rate(
                    db_session=db_session,
                    request=request,
                    currency_id=purchase_order.currency,  # PO currency
                    transaction_date=payment_date.date(),
                    app_id=app_id,
                    created_by=created_by,
                    source_type='purchase_order_payment',
                    source_id=None  # Will update after creating payment
                )
            except ValueError as e:
                flash(str(e), 'error')
                return redirect(url_for('purchases.purchase_order_details', purchase_order_id=purchase_order_id))

            # Create payment transaction
            new_payment = PurchaseTransaction(
                purchase_order_id=purchase_order_id,
                vendor_id=purchase_order.vendor_id,
                payment_date=payment_date,
                amount_paid=amount,
                currency_id=purchase_order.currency,
                reference_number=reference,
                payment_status=payment_status,
                created_by=created_by,
                app_id=app_id
            )
            db_session.add(new_payment)
            db_session.flush()

            # Create payment allocation
            allocation = allocate_purchase_payment(
                purchase_transaction=new_payment,
                payment_date=payment_date,
                payment_amount=amount,
                db_session=db_session,
                payment_mode_id=payment_method,
                payment_account_id=asset_account_id,
                payment_type=payment_type,
                inventory_account_id=None,
                non_inventory_account_id=None,
                other_expense_account_id=None,
                other_expense_service_id=None,
                tax_payable_id=None,
                tax_receivable_id=None,
                credit_purchase_account_id=None,
                prepaid_account_id=prepaid_account_id,
                reference=reference,
                exchange_rate_id=exchange_rate_id
            )
            db_session.add(allocation)
            db_session.flush()

            # Create journal entries
            journal_entries = create_purchase_payment_journal_entries(
                db_session=db_session,
                purchase_transaction=new_payment,
                payment_amount=amount,
                payment_date=payment_date,
                payment_type=payment_type,
                base_currency_id=base_currency_id,
                asset_account_id=asset_account_id,
                prepaid_account_id=prepaid_account_id,
                payment_method_id=payment_method,
                reference=reference,
                created_by=created_by,
                app_id=app_id,
                allocation_id=allocation.id,
                exchange_rate_id=exchange_rate_id,
                exchange_rate_value=exchange_rate_value,
                status="Posted"
            )
            if not journal_entries:
                raise Exception("Posting to legder failed")

            # If remaining balance is 0 after this payment, update ALL transactions to "paid"
            # Determine payment status for this transaction only
            if remaining_balance <= 0:
                payment_status = PurchasePaymentStatus.paid
            elif amount == 0:
                payment_status = PurchasePaymentStatus.unpaid
            else:
                payment_status = PurchasePaymentStatus.partial

            new_payment.payment_status = payment_status

            # DO NOT update other transactions - leave them as they are
            # They already have their correct status from when they were recorded

            # Update PO status
            purchase_order.status = OrderStatus.paid if amount == remaining_balance else OrderStatus.partially_paid

            if rate_obj:
                rate_obj.source_id = new_payment.id
                db_session.add(rate_obj)

            db_session.commit()

            flash('Payment recorded successfully! Journal entries created.', 'success')
            return redirect(url_for('purchases.purchase_order_details', purchase_order_id=purchase_order_id))

        except Exception as e:
            db_session.rollback()
            logger.error(f"Error: {str(e)}\n{traceback.format_exc()}")

            flash(f'An error occurred: {str(e)}', 'error')
            return redirect(url_for('purchases.purchase_order_details', purchase_order_id=purchase_order_id))

        finally:
            db_session.close()

    except Exception as e:
        logger.error(f"Outer error: {str(e)}")
        flash(f'An error occurred: {str(e)}', 'error')
        return redirect(url_for('purchases.purchase_order_details', purchase_order_id=purchase_order_id))


@purchases_bp.route('/api/get_purchase_payment_details/<int:transaction_id>', methods=['GET'])
@login_required
def get_payment_details(transaction_id):
    """
    Retrieve payment details for a specific transaction to populate the edit form
    """
    db_session = Session()
    try:

        # Query the purchase transaction with all necessary relationships
        purchase_transaction = db_session.query(PurchaseTransaction) \
            .options(
            joinedload(PurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.chart_of_accounts_asset),  # funding account
            joinedload(PurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.chart_of_accounts_prepaid),  # prepaid account
            joinedload(PurchaseTransaction.payment_allocations)
            .joinedload(PurchasePaymentAllocation.payment_modes)  # payment method
        ) \
            .filter(PurchaseTransaction.id == transaction_id) \
            .first()

        if not purchase_transaction:
            logger.error(f"No purchase transaction found for ID: {transaction_id}")
            return jsonify({'success': False, 'message': 'Payment transaction not found'}), 404

        # Get the first payment allocation
        allocation = purchase_transaction.payment_allocations[0] if purchase_transaction.payment_allocations else None

        if not allocation:
            logger.error(f"No payment allocation found for transaction: {transaction_id}")
            return jsonify({'success': False, 'message': 'Payment allocation not found'}), 404

        # Get category information for accounts
        funding_account_category = None
        if allocation.chart_of_accounts_asset and allocation.chart_of_accounts_asset.categories:
            funding_account_category = allocation.chart_of_accounts_asset.categories

        prepaid_account_category = None
        if allocation.chart_of_accounts_prepaid and allocation.chart_of_accounts_prepaid.categories:
            prepaid_account_category = allocation.chart_of_accounts_prepaid.categories

        # Prepare the response data
        payment_data = {
            'transaction_id': purchase_transaction.id,
            'purchase_order_id': purchase_transaction.purchase_order_id,
            'amount_paid': float(purchase_transaction.amount_paid),
            'payment_date': purchase_transaction.payment_date.strftime('%Y-%m-%d'),
            'reference_number': purchase_transaction.reference_number,
            'payment_status': purchase_transaction.payment_status.name,
            'is_posted_to_ledger': allocation.is_posted_to_ledger,
            'payment_type': allocation.payment_type,
            'payment_method_id': allocation.payment_mode,
            'payment_method': allocation.payment_modes.payment_mode if allocation.payment_modes else None,

            # Funding Account (Cash/Bank) - using chart_of_accounts_asset relationship
            'funding_account_id': allocation.payment_account_id,
            'funding_account_subcategory_name': allocation.chart_of_accounts_asset.sub_category if allocation.chart_of_accounts_asset else None,
            'funding_account_category': allocation.chart_of_accounts_asset.category_fk if allocation.chart_of_accounts_asset else None,
            'funding_account_category_name': funding_account_category.category if funding_account_category else None,

            # Prepaid Account (for advance payments) - using chart_of_accounts_prepaid relationship
            'prepaid_account_id': allocation.prepaid_account_id,
            'prepaid_account_subcategory_name': allocation.chart_of_accounts_prepaid.sub_category if allocation.chart_of_accounts_prepaid else None,
            'prepaid_account_category': allocation.chart_of_accounts_prepaid.category_fk if allocation.chart_of_accounts_prepaid else None,
            'prepaid_account_category_name': prepaid_account_category.category if prepaid_account_category else None
        }

        return jsonify({'success': True, 'data': payment_data})

    except Exception as e:
        logger.error(f"\n!!! ERROR FETCHING PAYMENT DETAILS !!!")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Error fetching payment details: {str(e)}'}), 500

    finally:
        db_session.close()


@purchases_bp.route('/api/update_purchase_payment', methods=['POST'])
@login_required
def update_purchase_payment():
    """
    Update an existing purchase payment
    """
    try:
        # Parse input data
        transaction_version = request.form.get('transaction_version', type=int)
        exchange_rate = request.form.get('exchange_rate')
        exchange_rate_value = float(exchange_rate) if exchange_rate else None
        base_currency_id = request.form.get('base_currency_id')
        transaction_id = int(request.form.get('transactionId'))
        purchase_order_id = int(request.form.get('purchaseOrderId'))
        amount = request.form.get('amount')
        payment_date = request.form.get('paymentDate')
        funding_account_id = request.form.get('fundingAccount')
        prepaid_account_id = request.form.get('prepaidAccount')
        reference = request.form.get('reference') or None
        payment_method = request.form.get('paymentMethod') or None
        payment_method = int(payment_method) if payment_method and payment_method.strip().isdigit() else None
        payment_type = request.form.get('paymentType')
        update_existing_journals = request.form.get('update_existing_journals') == 'true'

        created_by = current_user.id
        app_id = current_user.app_id

        # Validate required fields
        if not all([transaction_id, purchase_order_id, amount, payment_date, funding_account_id]):
            return jsonify({
                'success': False,
                'message': 'Missing required fields.',
                'error_type': 'missing_fields'
            }), 400

        # Validate amount and date
        try:
            amount = Decimal(amount)
            payment_date = datetime.strptime(payment_date, '%Y-%m-%d')
        except (InvalidOperation, ValueError) as e:
            return jsonify({
                'success': False,
                'message': 'Invalid amount or date format.',
                'error_type': 'validation_error'
            }), 400

        # Start database session
        db_session = Session()

        try:
            # Fetch the existing transaction with related data
            # Fetch the existing transaction with version check and lock
            purchase_transaction = db_session.query(PurchaseTransaction) \
                .filter(
                PurchaseTransaction.id == transaction_id,
                PurchaseTransaction.app_id == app_id,
                PurchaseTransaction.version == transaction_version
            ) \
                .with_for_update() \
                .first()

            if not purchase_transaction:
                return jsonify({
                    'success': False,
                    'message': 'This payment was modified by another user. Please refresh and try again.',
                    'error_type': 'not_found'
                }), 404

            # Get the payment allocation
            allocation = db_session.query(PurchasePaymentAllocation) \
                .filter_by(payment_id=transaction_id) \
                .first()

            if not allocation:
                return jsonify({
                    'success': False,
                    'message': 'Payment allocation not found.',
                    'error_type': 'not_found'
                }), 404

            # Fetch purchase order with account relationships AND all transactions
            purchase_order = db_session.query(PurchaseOrder) \
                .options(
                joinedload(PurchaseOrder.service_expense_account),
                joinedload(PurchaseOrder.shipping_handling_account),
                joinedload(PurchaseOrder.non_inventory_expense_account),
                joinedload(PurchaseOrder.preferred_prepaid_account),
                joinedload(PurchaseOrder.purchase_transactions)  # ADD THIS LINE to load all transactions
            ) \
                .filter_by(id=purchase_order_id) \
                .first()

            if not purchase_order:
                return jsonify({
                    'success': False,
                    'message': 'Purchase Order not found.',
                    'error_type': 'not_found'
                }), 404

            # Calculate remaining balance (excluding current transaction)
            total_paid_excluding_current = db_session.query(func.sum(PurchaseTransaction.amount_paid)) \
                                               .filter(
                PurchaseTransaction.purchase_order_id == purchase_order_id,
                PurchaseTransaction.payment_status.in_(['paid', 'partial']),
                PurchaseTransaction.id != transaction_id  # Exclude current transaction
            ) \
                                               .scalar() or Decimal('0.00')

            total_paid_with_updated = total_paid_excluding_current + amount
            remaining_balance = purchase_order.total_amount - total_paid_with_updated

            max_allowed_amount = purchase_order.total_amount - total_paid_excluding_current
            if amount <= 0 or amount > max_allowed_amount:
                logger.warning(f'Amount is not within range')
                message = f'Invalid payment amount. Must be between 0 and {max_allowed_amount:.2f}'
                flash(f'{message}', "warning")
                return redirect(url_for('purchases.purchase_order_details', purchase_order_id=purchase_order_id))

            # Add this after getting purchase_order and before creating/updating payment
            try:
                exchange_rate_id, exchange_rate_value, rate_obj = process_transaction_exchange_rate(
                    db_session=db_session,
                    request=request,
                    currency_id=purchase_order.currency,
                    transaction_date=payment_date.date(),
                    app_id=app_id,
                    created_by=created_by,
                    source_type='purchase_order_payment',
                    source_id=transaction_id
                )
            except ValueError as e:
                flash(str(e), 'error')
                return redirect(url_for('purchases.purchase_order_details', purchase_order_id=purchase_order_id))

            # Handle prepaid account changes for advance payments
            if payment_type == 'advance_payment' and prepaid_account_id:
                prepaid_account_id = int(prepaid_account_id)

                # Check if prepaid account changed
                original_prepaid_account = allocation.prepaid_account_id

                if original_prepaid_account != prepaid_account_id and update_existing_journals:
                    update_result = update_prepaid_journals_for_po(
                        db_session, purchase_order_id, prepaid_account_id, created_by
                    )

                    # UPDATE: Set the preferred prepaid account on the purchase order

                    purchase_order.preferred_prepaid_account_id = prepaid_account_id
                    if not update_result['success']:
                        return jsonify({
                            'success': False,
                            'message': f'Failed to update existing journals: {update_result["error"]}',
                            'error_type': 'journal_update_error'
                        }), 400

            # FIRST: Delete existing journal entries for this transaction
            # FIRST: Delete existing journal entries for the OLD allocation using the proper function
            delete_success, delete_message = delete_journal_entries_by_source(
                db_session=db_session,
                source_type='purchase_order_payment',  # or 'purchase_order_payment' depending on your system
                source_id=allocation.id,  # Use the OLD allocation ID
                app_id=app_id
            )

            if not delete_success:
                return jsonify({
                    'success': False,
                    'message': f'Failed to delete existing journal entries: {delete_message}',
                    'error_type': 'journal_deletion_error'
                }), 400

            # Delete the old allocation
            db_session.delete(allocation)
            db_session.flush()  # Ensure the old allocation is removed

            # Update the purchase transaction
            purchase_transaction.amount_paid = amount
            purchase_transaction.payment_date = payment_date
            purchase_transaction.reference_number = reference
            purchase_transaction.updated_at = func.now()
            purchase_transaction.is_posted_to_ledger = True
            purchase_transaction.version += 1  # ADD THIS LINE

            db_session.flush()  # Ensure the old allocation is removed
            # Determine payment status
            if amount == 0:
                payment_status = PurchasePaymentStatus.unpaid
            elif remaining_balance <= 0:
                payment_status = PurchasePaymentStatus.paid
            else:
                payment_status = PurchasePaymentStatus.partial

            purchase_transaction.payment_status = payment_status

            # If remaining balance is 0, update ALL transactions for this PO to "paid" without additional query
            # Determine payment status for this transaction only
            if remaining_balance <= 0:
                payment_status = PurchasePaymentStatus.paid
            elif amount == 0:
                payment_status = PurchasePaymentStatus.unpaid
            else:
                payment_status = PurchasePaymentStatus.partial

            purchase_transaction.payment_status = payment_status

            # DO NOT update other transactions - leave them as they are
            # They already have their correct status from when they were recorded
            # Update PO status
            if remaining_balance <= 0:
                purchase_order.status = OrderStatus.paid
            elif total_paid_with_updated > 0:
                purchase_order.status = OrderStatus.partially_paid
            else:
                purchase_order.status = OrderStatus.unpaid

            # Get account IDs from the PurchaseOrder for the allocation function
            inventory_account_id = None  # This should come from PO items or default inventory account
            non_inventory_account_id = purchase_order.non_inventory_expense_account_id
            other_expense_account_id = purchase_order.shipping_handling_account_id
            other_expense_service_id = purchase_order.service_expense_account_id
            tax_payable_id = None  # This should come from tax settings
            tax_receivable_id = None  # This should come from tax settings
            credit_purchase_account_id = purchase_order.accounts_payable_id

            # Use the allocate_purchase_payment function to create a new allocation with properly calculated values
            new_allocation = allocate_purchase_payment(
                purchase_transaction=purchase_transaction,
                payment_date=payment_date,
                payment_amount=amount,
                db_session=db_session,
                payment_mode_id=payment_method,
                payment_type=payment_type,
                payment_account_id=funding_account_id,
                inventory_account_id=inventory_account_id,
                non_inventory_account_id=non_inventory_account_id,
                other_expense_account_id=other_expense_account_id,
                other_expense_service_id=other_expense_service_id,
                tax_payable_id=tax_payable_id,
                tax_receivable_id=tax_receivable_id,
                credit_purchase_account_id=credit_purchase_account_id,
                prepaid_account_id=prepaid_account_id if payment_type == 'advance_payment' else None,
                reference=reference
            )

            db_session.flush()

            # Create new journal entries for the updated payment using the properly allocated amounts
            try:
                journal_entries = create_purchase_payment_journal_entries(
                    db_session=db_session,
                    purchase_transaction=purchase_transaction,
                    payment_amount=amount,
                    payment_date=payment_date,
                    payment_type=payment_type,
                    base_currency_id=base_currency_id,  # Add this
                    asset_account_id=funding_account_id,
                    prepaid_account_id=prepaid_account_id,
                    payment_method_id=payment_method,
                    reference=reference,
                    created_by=created_by,
                    app_id=app_id,
                    allocation_id=new_allocation.id,
                    exchange_rate_id=exchange_rate_id,  # Add this
                    exchange_rate_value=exchange_rate_value,  # Add this
                    status="Posted"
                )

                # Update allocation to mark as posted
                new_allocation.is_posted_to_ledger = True

            except Exception as journal_error:
                logger.error(f"Error creating journal entries: {journal_error}")
                traceback.print_exc()
                message = f'Payment update failed due to journal entry error: {str(journal_error)}'
                flash(f'{message}', "error")
                return redirect(url_for('purchases.purchase_order_details', purchase_order_id=purchase_order_id))

            # Repost all goods receipts for this purchase order to reflect updated payment allocations
            # try:
            #     # Get all goods receipts for this PO
            #     goods_receipts = db_session.query(GoodsReceipt) \
            #         .filter_by(purchase_order_id=purchase_order_id, app_id=app_id) \
            #         .options(joinedload(GoodsReceipt.receipt_items).joinedload(GoodsReceiptItem.purchase_order_item)) \
            #         .all()
            #
            #     for goods_receipt in goods_receipts:
            #         # Delete existing ledger entries for this goods receipt
            #         delete_success, delete_message = delete_journal_entries_by_source(
            #             db_session=db_session,
            #             source_type='goods_receipt',
            #             source_id=goods_receipt.id,
            #             app_id=app_id
            #         )
            #
            #         if not delete_success:
            #             logger.warning(f"Failed to delete existing goods receipt journals: {delete_message}")
            #             # Continue anyway to try creating new ones
            #
            #         # Recalculate landed costs for goods receipt items based on updated payments
            #         receipt_items_list = [ri for ri in goods_receipt.receipt_items]
            #         recalculated_allocations = calculate_goods_receipt_landed_costs(
            #             db_session=db_session,
            #             goods_receipt=goods_receipt,
            #             purchase_record=purchase_order,
            #             receipt_items=receipt_items_list,
            #             app_id=app_id
            #         )
            #         # Preserve the original posting status when reposting
            #         current_status = 'Posted' if goods_receipt.is_posted_to_ledger else 'Unposted'
            #
            #         # Repost goods receipt to ledger with updated allocations
            #         success, message = post_goods_receipt_to_ledger(
            #             db_session=db_session,
            #             goods_receipt=goods_receipt,
            #             current_user=current_user,
            #             status=current_status
            #         )
            #
            #         if success:
            #             logger.info(f"Reposted goods receipt {goods_receipt.receipt_number} after payment update")
            #         else:
            #             logger.warning(f"Failed to repost goods receipt {goods_receipt.receipt_number}: {message}")

            # except Exception as e:
            #     logger.error(f"Error reposting goods receipts after payment update: {str(e)}\n{traceback.format_exc()}")
            #     # Don't rollback the entire transaction - payment update should still succeed
            #     return jsonify({
            #         'success': True,
            #         'message': 'Payment updated successfully!',
            #         'warning': f'Goods receipt reposting failed: {str(e)}. Please repost receipts manually.',
            #         'purchase_order_id': purchase_order_id
            #     })

            db_session.commit()

            flash('Payment updated successfully!', 'success')
            return redirect(url_for('purchases.purchase_order_details', purchase_order_id=purchase_order_id))

        except Exception as e:
            db_session.rollback()
            logger.error(f"Error: {str(e)}\n{traceback.format_exc()}")
            flash(f'An error occurred {e}', "error")
            return redirect(url_for('purchases.purchase_order_details', purchase_order_id=purchase_order_id))

        finally:
            db_session.close()

    except Exception as e:
        logger.error(f"\n!!! OUTER ERROR IN PAYMENT UPDATE !!!")
        logger.error(f"Error: {str(e)}\n{traceback.format_exc()}")
        flash(f'An error occurred {e}', 'danger')
        return redirect(url_for('purchases.purchase_order_details', purchase_order_id=purchase_order_id))


@purchases_bp.route('/api/cancel_payment', methods=['POST'])
@login_required
def cancel_payment():
    """
    Cancel a payment transaction and delete related journal entries
    """
    db_session = Session()
    try:
        data = request.form
        transaction_id = data.get('transaction_id')
        app_id = current_user.app_id

        if not transaction_id:
            return jsonify({
                'success': False,
                'message': 'Transaction ID is required'
            }), 400

        # Get the payment transaction with all related data
        transaction = db_session.query(PurchaseTransaction).options(
            joinedload(PurchaseTransaction.purchase_orders),
            joinedload(PurchaseTransaction.payment_allocations)
        ).filter_by(
            id=transaction_id,
            app_id=app_id
        ).first()

        if not transaction:
            return jsonify({
                'success': False,
                'message': 'Payment transaction not found'
            }), 404

        # Check if already cancelled
        if transaction.payment_status == PurchasePaymentStatus.cancelled:
            return jsonify({
                'success': False,
                'message': 'Payment is already cancelled'
            }), 400

        # Get payment allocation
        allocation = transaction.payment_allocations[0] if transaction.payment_allocations else None

        # Check if posted to ledger
        is_posted_to_ledger = allocation.is_posted_to_ledger if allocation else False

        # STEP 1: Delete journal entries if they exist
        if allocation:
            delete_success, delete_message = delete_journal_entries_by_source(
                db_session=db_session,
                source_type='purchase_order_payment',
                source_id=allocation.id,
                app_id=app_id
            )

            if not delete_success:
                logger.warning(f"Could not delete journal entries: {delete_message}")
                # Continue with cancellation anyway - we'll still mark as cancelled

        # STEP 2: Update payment status to cancelled
        transaction.payment_status = PurchasePaymentStatus.cancelled

        # STEP 3: Update allocation status
        if allocation:
            allocation.is_cancelled = True

        # STEP 4: Recalculate purchase order status and remaining balance
        purchase_order = transaction.purchase_orders
        if purchase_order:
            # Recalculate total paid (excluding cancelled transactions)
            total_paid = db_session.query(
                func.sum(PurchaseTransaction.amount_paid)
            ).filter(
                PurchaseTransaction.purchase_order_id == purchase_order.id,
                PurchaseTransaction.payment_status.in_(['paid', 'partial']),
                PurchaseTransaction.id != transaction_id  # Exclude this cancelled transaction
            ).scalar() or Decimal('0')

            # Update PO status based on new total paid
            if total_paid == Decimal('0'):
                purchase_order.status = OrderStatus.unpaid
            elif total_paid < purchase_order.total_amount:
                purchase_order.status = OrderStatus.partially_paid
            else:
                purchase_order.status = OrderStatus.paid

        # STEP 5: Create audit log

        new_log = ActivityLog(
            activity="purchase order payment cancellation",
            user=current_user.email,
            details=f'Payment #{transaction.id} cancelled. Amount: {transaction.amount_paid}.',
            app_id=current_user.app_id
        )

        db_session.add(new_log)

        db_session.commit()

        return jsonify({
            'success': True,
            'message': 'Payment cancelled successfully',
            'transaction_id': transaction_id,
            'was_posted_to_ledger': is_posted_to_ledger
        })

    except SQLAlchemyError as e:
        db_session.rollback()
        logger.error(f"Database error cancelling payment: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Database error: {str(e)}'
        }), 500

    except Exception as e:
        db_session.rollback()
        logger.error(f"Unexpected error cancelling payment: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f'Unexpected error: {str(e)}'
        }), 500

    finally:
        db_session.close()


@purchases_bp.route('/purchases/send_to_customer/<int:purchase_order_id>', methods=['POST'])
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
