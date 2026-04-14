# models_master.py

import decimal
import enum
import uuid
from decimal import Decimal

import bcrypt
from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey, Enum, UniqueConstraint, Boolean, Text, Date, \
    Float, Numeric, DECIMAL, Index, Time, CheckConstraint
from sqlalchemy.orm import relationship, object_session, validates, declared_attr
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event, select, func
from datetime import datetime, timezone

Base = declarative_base()


# *******************************************************************Buidling Data Base structure********************************************************

# *******************************************************************Buidling Data Base structure********************************************************

# Define the UserPreference model
class UserPreference(Base):
    __tablename__ = 'user_preferences'

    id = Column(Integer, primary_key=True)  # Auto-incrementing primary key
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)  # Foreign key to User table
    preference_type = Column(String(50), nullable=False)  # Preference type (e.g., 'prompt_x', 'prompt_y')
    do_not_show_again = Column(Boolean, default=False)  # Boolean flag for preference
    user = relationship('User', back_populates='user_preferences')  # Relationship to the User model

    # Unique constraint on user_id and preference_type
    __table_args__ = (
        UniqueConstraint('user_id', 'preference_type', name='unique_user_preference'),
    )


class UserModuleAccess(Base):
    __tablename__ = 'user_module_access'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    module_id = Column(Integer, ForeignKey('modules.id'), nullable=False)
    can_view = Column(Boolean, default=False, server_default='0')  # Changed to '0'
    can_edit = Column(Boolean, default=False, server_default='0')  # Changed to '0'
    can_approve = Column(Boolean, default=False, server_default='0')  # Changed to '0'
    can_administer = Column(Boolean, default=False, server_default='0')  # Changed to '0'

    user = relationship('User', back_populates='module_access')
    module = relationship('Module', back_populates='user_access')


# Define Notification Types
class NotificationType(enum.Enum):
    info = "info"
    warning = "warning"
    success = "success"
    error = "error"


# Define Notification Status
class NotificationStatus(enum.Enum):
    unread = "unread"
    read = "read"
    archived = "archived"


# Define Notification Scope
class NotificationScope(enum.Enum):
    user = "user"  # Specific to a user
    company = "company"  # Sent to all users in a company


# Define Notifications Table
class Notification(Base):
    __tablename__ = 'notifications'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=True)  # Nullable for company-wide notifications
    message = Column(String(255), nullable=False)
    type = Column(Enum(NotificationType), default=NotificationType.info)
    status = Column(Enum(NotificationStatus), default=NotificationStatus.unread)
    is_popup = Column(Boolean, default=False)
    url = Column(String(255), nullable=True)
    scope = Column(Enum(NotificationScope), nullable=False)  # Either 'user' or 'company'
    created_at = Column(DateTime, default=func.now())

    # Relationships
    user = relationship("User", back_populates="notifications")

    def __repr__(self):
        return f"<Notification(scope={self.scope}, message='{self.message}', status={self.status})>"


class Module(Base):
    __tablename__ = 'modules'
    id = Column(Integer, primary_key=True)
    module_name = Column(String(100), nullable=False)
    included = Column(String(100), nullable=False)
    priority = Column(Integer, default=0)  # Higher numbers = higher priority

    user_access = relationship('UserModuleAccess', back_populates='module')


class Currency(Base):
    __tablename__ = 'currency'
    id = Column(Integer, primary_key=True)
    user_currency = Column(String(100), nullable=False)
    currency_index = Column(Integer, nullable=False)

    # Relationship to InventoryEntry
    inventory_entries = relationship('InventoryEntry', back_populates='currency')
    purchase_orders = relationship('PurchaseOrder', back_populates='currencies')

    exchange_rates = relationship('ExchangeRate', foreign_keys='ExchangeRate.to_currency_id',
                                  back_populates='currencies')

    from_exchange_rates = relationship('ExchangeRate', foreign_keys='ExchangeRate.from_currency_id',
                                       back_populates='from_currency')

    from_currency_exchange_transaction = relationship('CurrencyExchangeTransaction',
                                                      foreign_keys='CurrencyExchangeTransaction.from_currency_id',
                                                      back_populates='from_currency')
    to_currency_exchange_transaction = relationship('CurrencyExchangeTransaction',
                                                    foreign_keys='CurrencyExchangeTransaction.to_currency_id',
                                                    back_populates='from_currency')

    journals = relationship('Journal', back_populates='currency')

    inventory_transaction_details = relationship('InventoryTransactionDetail', back_populates='currency')
    quotation = relationship('Quotation', back_populates='currencies')
    quotation_items = relationship('QuotationItem', back_populates='currencies')
    employees = relationship("Employee", back_populates="currency")
    payroll_transactions = relationship('PayrollTransaction', back_populates="currency")
    deduction_types = relationship("DeductionType", back_populates="currency")
    benefit_types = relationship("BenefitType", back_populates="currency")
    deductions = relationship('Deduction', back_populates="currency")
    benefits = relationship('Benefit', back_populates="currency")
    payroll_payments = relationship('PayrollPayment', back_populates='currency')
    deduction_payments = relationship('DeductionPayment', back_populates='currency')
    advance_payments = relationship('AdvancePayment', back_populates='currency')
    sales_orders = relationship('SalesOrder', back_populates='currencies')
    sales_order_items = relationship('SalesOrderItem', back_populates='currencies')
    sales_invoices = relationship('SalesInvoice', back_populates='currencies')
    sales_invoice_items = relationship('SalesInvoiceItem', back_populates='currencies')
    sales_transaction = relationship('SalesTransaction', back_populates='currency')

    bulk_payments = relationship("BulkPayment", back_populates="currency")

    payment_receipts = relationship('PaymentReceipt', back_populates='currency')
    direct_sale_items = relationship("DirectSaleItem", back_populates='currencies')
    direct_sales = relationship('DirectSalesTransaction', back_populates='currency')
    purchase_order_items = relationship('PurchaseOrderItem', back_populates='currencies')
    purchase_transactions = relationship('PurchaseTransaction', back_populates='currency')
    direct_purchases = relationship('DirectPurchaseTransaction', back_populates='currency')
    direct_purchase_items = relationship("DirectPurchaseItem", back_populates='currencies')

    expense_transactions = relationship('ExpenseTransaction', back_populates='currency')
    item_selling_prices = relationship("ItemSellingPrice", back_populates="currency")

    customer_credits = relationship("CustomerCredit", back_populates="currency")
    asset_movements = relationship("AssetMovement", back_populates="currency")  # Also add this


class ExchangeRate(Base):
    __tablename__ = 'exchange_rates'
    id = Column(Integer, primary_key=True)
    from_currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    to_currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    rate = Column(DECIMAL(20, 10), nullable=False)
    currency_exchange_transaction_id = Column(Integer, ForeignKey('currency_exchange_transaction.id'), nullable=True)
    date = Column(Date, nullable=False)  # Stores the date the rate applies to

    # NEW COLUMNS
    source_type = Column(String(50), nullable=True)  # e.g., 'manual', 'api', 'bank', 'transaction'
    source_id = Column(Integer, nullable=True)  # ID reference to the source (if applicable)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=True)  # User who created this exchange rate

    # Relationship for created_by
    creator = relationship("User", foreign_keys=[created_by], back_populates="created_exchange_rates")

    # Keep existing 'currencies' relationship (likely using to_currency_id)
    currencies = relationship('Currency', foreign_keys=[to_currency_id], back_populates='exchange_rates')

    # Add a new, separate relationship for from_currency_id
    from_currency = relationship('Currency', foreign_keys=[from_currency_id], back_populates='from_exchange_rates')

    journals = relationship('Journal', back_populates='exchange_rate')

    sales_transactions = relationship('PaymentAllocation', back_populates='exchange_rate')
    purchase_transactions = relationship('PurchasePaymentAllocation', back_populates='exchange_rate')
    payroll_payments = relationship('PayrollPayment', back_populates='exchange_rate')
    deduction_payments = relationship('DeductionPayment', back_populates='exchange_rate')
    advance_repayments = relationship('AdvanceRepayment', back_populates='exchange_rate')
    advance_payments = relationship('AdvancePayment', back_populates='exchange_rate')
    currency_exchange_transaction = relationship('CurrencyExchangeTransaction', back_populates='exchange_rates')
    inventory_transaction_details = relationship("InventoryTransactionDetail", back_populates="exchange_rate")
    inventory_entry = relationship("InventoryEntry", back_populates="exchange_rate")
    customer_credits = relationship("CustomerCredit", back_populates="exchange_rate")
    expense_transactions = relationship("ExpenseTransaction", back_populates="exchange_rate")
    sales_invoices = relationship('SalesInvoice', back_populates='exchange_rate')
    bulk_payments = relationship("BulkPayment", back_populates="exchange_rate")
    purchase_orders = relationship("PurchaseOrder", back_populates="exchange_rate")
    payroll_transactions = relationship("PayrollTransaction", back_populates="exchange_rate")
    deductions = relationship("Deduction", back_populates="exchange_rate")
    benefits = relationship("Benefit", back_populates="exchange_rate")

    quotations = relationship("Quotation", back_populates="exchange_rate")
    sales_orders = relationship("SalesOrder", back_populates="exchange_rate")

    __table_args__ = (
        Index('idx_exchange_rates_currency_date', 'from_currency_id', 'to_currency_id', 'date'),
        Index('idx_exchange_rates_source', 'source_type', 'source_id'),
        Index('idx_exchange_rates_created_by', 'created_by'),  # Optional index for created_by
    )


class CurrencyExchangeTransaction(Base):
    __tablename__ = 'currency_exchange_transaction'

    id = Column(Integer, primary_key=True)

    from_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)
    to_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)

    from_currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    to_currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)

    from_amount = Column(Numeric(18, 2), nullable=False)  # e.g., 100.00
    to_amount = Column(Numeric(18, 2), nullable=False)  # e.g., 380000.00
    exchange_rate = Column(Numeric(20, 10), nullable=False)  # e.g., 3800.000000

    exchange_date = Column(Date, nullable=False)
    exchange_time = Column(Time, nullable=True)  # New column for exchange time
    description = Column(String(255), nullable=True)

    status = Column(String(50), nullable=False, default="pending")

    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)  # Indicates if entry is posted to the ledger

    datetime_added = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # --- Relationships ---
    from_account = relationship('ChartOfAccounts', foreign_keys=[from_account_id], backref='outgoing_exchange_account')
    to_account = relationship('ChartOfAccounts', foreign_keys=[to_account_id], backref='incoming_exchange_account')

    # Keep existing 'currencies' relationship (likely using to_currency_id)
    to_currency = relationship('Currency', foreign_keys=[to_currency_id],
                               back_populates='to_currency_exchange_transaction')

    # Add a new, separate relationship for from_currency_id
    from_currency = relationship('Currency', foreign_keys=[from_currency_id],
                                 back_populates='from_currency_exchange_transaction')

    exchange_rates = relationship('ExchangeRate', back_populates='currency_exchange_transaction')


    def __repr__(self):
        return f"<CurrencyExchangeTransaction(id={self.id}, from={self.from_currency} {self.from_amount}, to={self.to_currency} {self.to_amount})>"


class ChartOfAccounts(Base):
    __tablename__ = 'chart_of_accounts'
    id = Column(Integer, primary_key=True)
    parent_account_type = Column(String(100), unique=False)
    parent_account_type_id = Column(String(100), unique=False)
    category_id = Column(String(100))
    category_fk = Column(Integer, ForeignKey('category.id'), unique=False)
    category = Column(String(100))
    sub_category_id = Column(String(100), unique=False)
    sub_category = Column(String(100))

    # Financial reporting flags

    is_bank = Column(Boolean, default=False)
    is_cash = Column(Boolean, default=False)
    is_receivable = Column(Boolean, default=False)
    is_payable = Column(Boolean, default=False)

    is_monetary = Column(Boolean, default=False)

    # NEW: Active flag
    is_active = Column(Boolean, default=True)

    # NEW: System account flag - accounts managed by system, not users
    is_system_account = Column(Boolean, default=False)

    normal_balance = Column(Enum('Debit', 'Credit', name='normal_balances'), nullable=False)
    report_section_id = Column(Integer, ForeignKey('report_sections.id'))  # link to report section
    created_by = Column(Integer, ForeignKey('users.id'))


    categories = relationship('Category', back_populates='chart_of_accounts')
    journal_entries = relationship('JournalEntry', back_populates='chart_of_accounts')
    # Updated relationships with explicit foreign key specifications
    payroll_payments_funding = relationship(
        "PayrollPayment",
        foreign_keys="[PayrollPayment.payment_account]",
        back_populates="funding_account"
    )

    payroll_transactions_payable = relationship(
        "PayrollTransaction",
        foreign_keys="[PayrollTransaction.payable_account_id]",
        back_populates="payable_account"
    )

    payroll_deductions_payable = relationship(
        "Deduction",
        foreign_keys="[Deduction.payable_account_id]",
        back_populates="payable_account"
    )

    # Add these new relationships for employee accounting linkages
    employee_payroll_expense_accounts = relationship(
        "Employee",
        foreign_keys="[Employee.payroll_expense_account_id]",
        back_populates="payroll_expense_account"
    )

    employee_deduction_payable_accounts = relationship(
        "Employee",
        foreign_keys="[Employee.deduction_payable_account_id]",
        back_populates="deduction_payable_account"
    )

    employee_advance_accounts = relationship(
        "Employee",
        foreign_keys="[Employee.advance_account_id]",
        back_populates="advance_account"
    )

    employee_payable_accounts = relationship(
        "Employee",
        foreign_keys="[Employee.payable_account_id]",
        back_populates="payable_account"
    )

    deduction_payments = relationship('DeductionPayment', back_populates='chart_of_accounts')
    advance_payments = relationship('AdvancePayment', back_populates='chart_of_accounts')
    payment_allocations_asset = relationship("PaymentAllocation", back_populates="chart_of_accounts_asset",
                                             foreign_keys="[PaymentAllocation.payment_account]")
    payment_allocations_tax = relationship("PaymentAllocation", back_populates="chart_of_accounts_tax",
                                           foreign_keys="[PaymentAllocation.tax_payable_account_id]")
    payment_allocations_receivable = relationship("PaymentAllocation", back_populates="chart_of_accounts_receivable",
                                                  foreign_keys="[PaymentAllocation.credit_sale_account]")

    payment_allocations_write_off = relationship("PaymentAllocation", back_populates="chart_of_accounts_write_off",
                                                 foreign_keys="[PaymentAllocation.write_off_account_id]")

    bulk_payments = relationship("BulkPayment",
                                 back_populates="payment_account",
                                 foreign_keys="BulkPayment.payment_account_id")

    purchase_payment_allocations_asset = relationship("PurchasePaymentAllocation",
                                                      back_populates="chart_of_accounts_asset",
                                                      foreign_keys="[PurchasePaymentAllocation.payment_account_id]")
    purchase_payment_allocations_tax_receivable = relationship("PurchasePaymentAllocation",
                                                               back_populates="chart_of_accounts_tax_receivable",
                                                               foreign_keys="[PurchasePaymentAllocation.tax_receivable_account_id]")
    purchase_payment_allocations_tax_payable = relationship("PurchasePaymentAllocation",
                                                            back_populates="chart_of_accounts_tax_payable",
                                                            foreign_keys="[PurchasePaymentAllocation.tax_payable_account_id]")
    purchase_payment_allocations_payable = relationship("PurchasePaymentAllocation",
                                                        back_populates="chart_of_accounts_payable",
                                                        foreign_keys="[PurchasePaymentAllocation.credit_purchase_account]")
    purchase_payment_inventory_account = relationship("PurchasePaymentAllocation",
                                                      back_populates="inventory_account",
                                                      foreign_keys="[PurchasePaymentAllocation.inventory_account_id]")
    purchase_payment_non_inventory_account = relationship("PurchasePaymentAllocation",
                                                          back_populates="non_inventory_account",
                                                          foreign_keys="[PurchasePaymentAllocation.non_inventory_account_id]")

    purchase_payment_allocations_other_expense = relationship("PurchasePaymentAllocation",
                                                              back_populates="chart_of_accounts_other_expense",
                                                              foreign_keys="[PurchasePaymentAllocation.other_expense_account_id]")

    purchase_payment_allocations_service_expense = relationship("PurchasePaymentAllocation",
                                                                back_populates="chart_of_accounts_service_expense",
                                                                foreign_keys="[PurchasePaymentAllocation.other_expense_service_id]")

    purchase_payment_allocations_prepaid = relationship("PurchasePaymentAllocation",
                                                        back_populates="chart_of_accounts_other_expense",
                                                        foreign_keys="[PurchasePaymentAllocation.prepaid_account_id]")
    user = relationship(
        'User',
        back_populates='chart_of_account'
    )

    report_section = relationship('ReportSection', back_populates='sub_category')
    expense_payment_account = relationship('ExpenseTransaction', back_populates='payment_account')
    expense_items = relationship('ExpenseItem', back_populates='subcategory')

    # Add this relationship
    opening_balances = relationship('OpeningBalance', back_populates='account')

    inventory_cogs_account = relationship("InventoryItem", back_populates="cogs_account",
                                          foreign_keys="[InventoryItem.cogs_account_id]")
    inventory_asset_account = relationship("InventoryItem", back_populates="asset_account",
                                           foreign_keys="[InventoryItem.asset_account_id]")
    inventory_sales_account = relationship("InventoryItem", back_populates="sales_account",
                                           foreign_keys="[InventoryItem.sales_account_id]")
    inventory_tax_account = relationship("InventoryItem", back_populates="tax_account",
                                         foreign_keys="[InventoryItem.tax_account_id]")

    # Add these relationships to the existing ChartOfAccounts model
    inventory_location_discount = relationship(
        "InventoryLocation",
        foreign_keys="[InventoryLocation.discount_account_id]",
        back_populates="discount_account"
    )

    inventory_location_payment = relationship(
        "InventoryLocation",
        foreign_keys="[InventoryLocation.payment_account_id]",
        back_populates="payment_account"
    )

    inventory_location_card_payment = relationship(
        "InventoryLocation",
        foreign_keys="[InventoryLocation.card_payment_account_id]",
        back_populates="card_payment_account"
    )

    inventory_location_mobile_money = relationship(
        "InventoryLocation",
        foreign_keys="[InventoryLocation.mobile_money_account_id]",
        back_populates="mobile_money_account"
    )

    # Add these relationships
    inventory_entries_payable = relationship('InventoryEntry', foreign_keys='InventoryEntry.payable_account_id',
                                             back_populates='payable_account')
    inventory_entries_write_off = relationship('InventoryEntry', foreign_keys='InventoryEntry.write_off_account_id',
                                               back_populates='write_off_account')
    inventory_entries_adjustment = relationship('InventoryEntry', foreign_keys='InventoryEntry.adjustment_account_id',
                                                back_populates='adjustment_account')

    inventory_entries_sale = relationship('InventoryEntry', foreign_keys='InventoryEntry.sales_account_id',
                                          back_populates='sales_account')

    # Back-populates for invoices
    tax_invoices = relationship('SalesInvoice', back_populates='invoice_tax_account',
                                foreign_keys='SalesInvoice.tax_account_id')
    receivable_invoices = relationship('SalesInvoice', back_populates='account_receivable',
                                       foreign_keys='SalesInvoice.account_receivable_id')
    revenue_invoices = relationship('SalesInvoice', back_populates='revenue_account',
                                    foreign_keys='SalesInvoice.revenue_account_id')
    discount_invoices = relationship('SalesInvoice', back_populates='discount_account',
                                     foreign_keys='SalesInvoice.discount_account_id')

    direct_sales = relationship(
        "DirectSalesTransaction",
        back_populates="revenue_account"
    )

    # Add this relationship
    purchase_orders_ap = relationship('PurchaseOrder', foreign_keys='PurchaseOrder.accounts_payable_id',
                                      back_populates='accounts_payable')
    purchase_orders_service_expense = relationship('PurchaseOrder',
                                                   foreign_keys='PurchaseOrder.service_expense_account_id',
                                                   back_populates='service_expense_account')
    purchase_orders_shipping = relationship('PurchaseOrder', foreign_keys='PurchaseOrder.shipping_handling_account_id',
                                            back_populates='shipping_handling_account')
    purchase_orders_non_inventory = relationship('PurchaseOrder',
                                                 foreign_keys='PurchaseOrder.non_inventory_expense_account_id',
                                                 back_populates='non_inventory_expense_account')
    purchase_order_prepaid = relationship('PurchaseOrder', foreign_keys='PurchaseOrder.preferred_prepaid_account_id',
                                          back_populates='preferred_prepaid_account')

    # Asset

    # ✅ ADD these relationships for AssetItem:
    asset_fixed_asset_accounts = relationship(
        "AssetItem",
        foreign_keys="[AssetItem.fixed_asset_account_id]",
        back_populates="fixed_asset_account"
    )

    asset_depreciation_accounts = relationship(
        "AssetItem",
        foreign_keys="[AssetItem.depreciation_expense_account_id]",
        back_populates="depreciation_expense_account"
    )

    asset_accumulated_depreciation_accounts = relationship(
        "AssetItem",
        foreign_keys="[AssetItem.accumulated_depreciation_account_id]",
        back_populates="accumulated_depreciation_account"
    )

    # Asset Movement Header - Payable Account
    asset_movements_payable = relationship(
        "AssetMovement",
        foreign_keys="[AssetMovement.payable_account_id]",
        back_populates="payable_account"
    )

    # Asset Movement Header - Adjustment Account
    asset_movements_adjustment = relationship(
        "AssetMovement",
        foreign_keys="[AssetMovement.adjustment_account_id]",
        back_populates="adjustment_account"
    )

    # Asset Movement Header - Sales Account
    asset_movements_sales = relationship(
        "AssetMovement",
        foreign_keys="[AssetMovement.sales_account_id]",
        back_populates="sales_account"
    )

    __table_args__ = (

        Index('idx_charts_parent_type', 'parent_account_type'),  # For transaction_type
    )


class OpeningBalance(Base):
    __tablename__ = 'opening_balances'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)
    balance = Column(Numeric(15, 2), nullable=False)  # Store positive or negative

    # Audit fields
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc), nullable=False)
    created_by = Column(Integer, ForeignKey('users.id'))

    # Relationships
    account = relationship('ChartOfAccounts', back_populates='opening_balances')
    vendor_balances = relationship('OpeningBalanceVendor', back_populates='opening_balance')
    creator = relationship('User')

    # Index for performance on queries filtering by account and company
    __table_args__ = (
        Index('ix_opening_balances_account_app', 'account_id'),
    )


class OpeningBalanceVendor(Base):
    __tablename__ = 'opening_balance_vendors'

    id = Column(Integer, primary_key=True)
    opening_balance_id = Column(Integer, ForeignKey('opening_balances.id'), nullable=False)
    vendor_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)  # Can be vendor or customer
    balance = Column(Numeric(15, 2), nullable=False)  # Positive for receivables, negative for payables

    # Additional details
    description = Column(Text)  # Optional description for the balance
    reference_number = Column(String(100))  # Invoice number, contract number, etc.
    due_date = Column(Date)  # When the amount is due

    # Audit fields
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc), nullable=False)
    created_by = Column(Integer, ForeignKey('users.id'))

    # Relationships
    opening_balance = relationship('OpeningBalance', back_populates='vendor_balances')
    vendor = relationship('Vendor', back_populates='vendor_balances')
    creator = relationship('User', back_populates='vendor_balances')

    # Indexes for performance
    __table_args__ = (
        Index('ix_opening_balance_vendors_opening_balance', 'opening_balance_id'),
        Index('ix_opening_balance_vendors_vendor', 'vendor_id'),
    )


class ReportSection(Base):
    __tablename__ = 'report_sections'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)

    sub_category = relationship('ChartOfAccounts', back_populates='report_section')


class Category(Base):
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True)
    account_type = Column(String(100))
    category_id = Column(String(100), unique=False)
    category = Column(String(100))

    chart_of_accounts = relationship('ChartOfAccounts', back_populates='categories')



class Project(Base):
    __tablename__ = 'projects'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=False)
    project_id = Column(String(100), unique=False)
    location = Column(String(100))
    description = Column(String(1000))
    is_active = Column(Boolean, default=True)

    employees = relationship('Employee', back_populates='project')  # Added employees relationship

    journals = relationship('Journal', back_populates='project')
    inventory_entries = relationship('InventoryEntry', back_populates='project')
    internet_payments = relationship('InternetPayment', back_populates='project')
    direct_purchases = relationship('DirectPurchaseTransaction', back_populates='project')
    purchase_orders = relationship('PurchaseOrder', back_populates='project')
    inventory_location = relationship('InventoryLocation', back_populates='project')
    sales_invoices = relationship('SalesInvoice', back_populates='project')
    direct_sales = relationship('DirectSalesTransaction', back_populates='project')
    bulk_payments = relationship("BulkPayment",
                                 back_populates="project",
                                 foreign_keys="BulkPayment.project_id")

    expense_transactions = relationship('ExpenseTransaction', back_populates='project')
    inventory_transaction_details = relationship('InventoryTransactionDetail', back_populates='project')
    assets = relationship("Asset", back_populates="project")
    asset_movements = relationship("AssetMovement", back_populates="project")
    sales_orders = relationship("SalesOrder", back_populates="project")
    quotations = relationship("Quotation", back_populates="project")


    Index('ix_projects_is_active', 'is_active'),
    Index('ix_projects_app_active', 'is_active')


class Vendor(Base):
    __tablename__ = 'vendors'
    id = Column(Integer, primary_key=True)
    vendor_name = Column(String(100), nullable=False)
    vendor_id = Column(String(100), unique=False)
    designation = Column(String(100))
    tel_contact = Column(String(100))
    email = Column(String(100))
    address = Column(String(200))
    city = Column(String(100))
    state_province = Column(String(100))
    postal_code = Column(String(20))
    country = Column(String(100))
    website = Column(String(200))
    payment_terms = Column(String(50))
    tax_id = Column(String(50))
    vendor_status = Column(String(50), default='Active')
    vendor_type = Column(String(50))
    rating = Column(Integer)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    notes = Column(Text)
    primary_contact_person = Column(String(100))
    secondary_contact_person = Column(String(100))
    preferred_communication_method = Column(String(50))
    certifications = Column(Text)
    compliance_status = Column(String(50), default='Compliant')
    is_one_time = Column(Boolean, default=False)  # New column for one-time customers
    # Add this relationship if you have a Customer model
    customer_group_id = Column(Integer, ForeignKey('customer_groups.id'), nullable=True)
    is_active = Column(Boolean, default=True)


    journals = relationship('Journal', back_populates='vendor')

    inventory_entries = relationship('InventoryEntry', back_populates='vendor')
    inventory_transaction_details = relationship('InventoryTransactionDetail', back_populates='vendor')
    purchase_orders = relationship('PurchaseOrder', back_populates='vendor')
    sales_orders = relationship('SalesOrder', back_populates='customer')
    invoices = relationship('SalesInvoice', back_populates='customer')
    delivery_notes = relationship('DeliveryNote', back_populates='customer')
    sales_returns = relationship('SalesReturn', back_populates='customer')
    quotations = relationship('Quotation', back_populates='customer')
    sales_transaction = relationship('SalesTransaction', back_populates='customer')

    bulk_payments = relationship("BulkPayment", back_populates="customer")

    payment_receipts = relationship('PaymentReceipt', back_populates='customer')
    direct_sales = relationship('DirectSalesTransaction', back_populates='customer')
    backorders = relationship('Backorder', back_populates='supplier')
    purchase_returns = relationship('PurchaseReturn', back_populates='vendor')
    purchase_transactions = relationship('PurchaseTransaction', back_populates='vendor')
    direct_purchases = relationship('DirectPurchaseTransaction', back_populates='vendor')
    expense_transactions = relationship('ExpenseTransaction', back_populates='vendor')
    employees = relationship("Employee", back_populates="payee")
    vendor_balances = relationship('OpeningBalanceVendor', back_populates='vendor')
    customer_group = relationship("CustomerGroup", back_populates="customers")
    customer_credits = relationship("CustomerCredit", back_populates="customer")

    asset_movement_line_items = relationship("AssetMovementLineItem", back_populates="party")
    assets = relationship("Asset", back_populates="supplier")


class CustomerCredit(Base):
    __tablename__ = 'customer_credits'

    id = Column(Integer, primary_key=True)

    # Links to customer (vendor) and source transactions
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)

    # 👇 Add this - Link directly to bulk payment (for payments with no invoices)
    bulk_payment_id = Column(Integer, ForeignKey('bulk_payments.id'), nullable=True)

    # 👇 Keep existing - Link to payment allocation (for overpayments from invoices)
    payment_allocation_id = Column(Integer, ForeignKey('payment_allocations.id'), nullable=True)

    # Credit details
    original_amount = Column(Numeric(15, 2), default=0)  # Original credit amount
    available_amount = Column(Numeric(15, 2), default=0)  # Currently available amount
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)

    # Dates
    created_date = Column(DateTime, default=func.now())
    issued_date = Column(DateTime, default=func.now())  # When credit was issued
    expires_date = Column(DateTime)  # Optional: credit expiration

    # Status and tracking
    status = Column(String(20), default='active')  # active, used, expired, refunded
    credit_reason = Column(String(100))  # overpayment, return, discount, etc.
    reference_number = Column(String(50))  # Optional reference number

    # 👇 Notes field for additional information
    # Use this to track:
    # - Write-off reasons and dates
    # - Manual adjustments
    # - Customer communication notes
    # - Audit trail for changes
    notes = Column(Text, nullable=True)

    # Audit fields
    created_by = Column(Integer, ForeignKey('users.id'))
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # 👇 New: Link to exchange rate used at issuance
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)

    # Relationships
    customer = relationship("Vendor", back_populates="customer_credits")
    currency = relationship("Currency", back_populates="customer_credits")
    creator = relationship("User", back_populates="customer_credits", foreign_keys=[created_by])
    # 👇 Relationship to exchange rates
    exchange_rate = relationship("ExchangeRate", back_populates="customer_credits")

    # Credit applications (when credits are used)
    credit_applications = relationship("CreditApplication", back_populates="customer_credit")
    payment_allocation = relationship("PaymentAllocation", back_populates="created_credits")
    bulk_payment = relationship("BulkPayment", back_populates="customer_credits")

    # -------- Helper property for formatting --------
    @property
    def issued_date_formatted(self):
        return self.issued_date.strftime("%Y-%m-%d") if self.issued_date else None


class CreditApplication(Base):
    __tablename__ = 'credit_applications'

    id = Column(Integer, primary_key=True)

    # Links to credit and target transactions
    credit_id = Column(Integer, ForeignKey('customer_credits.id'), nullable=False)
    target_type = Column(String(20), nullable=False)  # 'sales_invoice' or 'direct_sale'
    target_id = Column(Integer, nullable=False)  # ID of the invoice or sale where credit was applied

    # NEW: Direct link to payment allocation
    payment_allocation_id = Column(Integer, ForeignKey('payment_allocations.id'), nullable=True)

    # Application details
    applied_amount = Column(Numeric(15, 2), nullable=False)
    applied_amount_invoice_currency = Column(Numeric(15, 2), nullable=True)

    application_date = Column(DateTime, default=func.now())

    # Status and tracking
    status = Column(String(20), default='applied')  # applied, reversed
    notes = Column(Text)

    # Audit fields
    applied_by = Column(Integer, ForeignKey('users.id'))
    created_at = Column(DateTime, default=func.now())

    # Relationships
    customer_credit = relationship("CustomerCredit", back_populates="credit_applications")
    applier = relationship("User", back_populates="credit_applications", foreign_keys=[applied_by])
    payment_allocation = relationship("PaymentAllocation", back_populates="credit_applications")


class PaymentMode(Base):
    __tablename__ = 'payment_modes'
    id = Column(Integer, primary_key=True)
    payment_mode = Column(String(100), nullable=False, unique=False)
    is_active = Column(Boolean, default=True)


    journals = relationship('Journal', back_populates='payment_mode')

    payroll_payments = relationship('PayrollPayment', back_populates='payment_modes')
    deduction_payments = relationship('DeductionPayment', back_populates='payment_modes')
    advance_payments = relationship("AdvancePayment", back_populates="payment_modes")
    payment_allocations = relationship("PaymentAllocation", back_populates="payment_modes")
    payment_receipts = relationship("PaymentReceipt", back_populates="payment_modes")
    purchase_payment_allocations = relationship("PurchasePaymentAllocation", back_populates="payment_modes")
    expense_transactions = relationship('ExpenseTransaction', back_populates='payment_mode')

    bulk_payments = relationship("BulkPayment", back_populates="payment_mode")


class Journal(Base):
    __tablename__ = 'journals'

    id = Column(Integer, primary_key=True)
    journal_number = Column(String(50), nullable=False, unique=False, index=True)
    journal_ref_no = Column(String(50), nullable=True, index=True)
    narration = Column(Text, nullable=True)
    date = Column(Date, index=True)
    date_added = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    payment_mode_id = Column(Integer, ForeignKey('payment_modes.id'), index=True)
    project_id = Column(Integer, ForeignKey('projects.id'), index=True)
    vendor_id = Column(Integer, ForeignKey('vendors.id'), index=True)
    currency_id = Column(Integer, ForeignKey('currency.id'), index=True)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False, index=True)
    updated_by = Column(Integer, ForeignKey('users.id'), nullable=True, index=True)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True, index=True)
    reconciled = Column(Boolean, default=False, index=True)
    reconciliation_date = Column(Date, nullable=True, index=True)
    adjustment_date = Column(Date, nullable=True, index=True)
    adjustment_reason = Column(String(255), nullable=True)
    total_debit = Column(Numeric(15, 2), default=0.0)
    total_credit = Column(Numeric(15, 2), default=0.0)
    balance = Column(Numeric(15, 2), default=0.0)
    status = Column(String(20), default='Posted', index=True)

    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index('idx_journals_date', 'date'),
        Index('idx_journals_status', 'status'),
        Index('idx_journals_vendor', 'vendor_id'),
        Index('idx_journals_project', 'project_id'),
        Index('idx_journals_created', 'created_by', 'date_added'),
        # ADD THESE NEW INDEXES:
        Index('idx_journals_app_status_date', 'status', 'date'),  # Most important!
        Index('idx_journals_currency', 'currency_id'),
        Index('idx_journals_payment_mode', 'payment_mode_id'),
        Index('idx_journals_exchange_rate', 'exchange_rate_id'),
        # ADD THESE:
        Index('idx_journals_app_status_date_currency', 'status', 'date', 'currency_id'),
        Index('idx_journals_app_status_date_exchange', 'status', 'date', 'exchange_rate_id'),

    )

    vendor = relationship('Vendor', back_populates='journals')
    project = relationship('Project', back_populates='journals')
    payment_mode = relationship('PaymentMode', back_populates='journals')
    currency = relationship("Currency", back_populates="journals")
    created_user = relationship('User', back_populates='created_journals', foreign_keys=[created_by])
    updated_user = relationship('User', back_populates='updated_journals', foreign_keys=[updated_by])
    exchange_rate = relationship("ExchangeRate", back_populates="journals")
    entries = relationship("JournalEntry", back_populates="journal", cascade="all, delete-orphan",
                           order_by="JournalEntry.line_number")
    internet_payments = relationship('InternetPayment', back_populates='journals')

    def is_balanced(self):
        return abs(float(self.total_debit) - float(self.total_credit)) < 0.01

    def update_totals(self):
        """
        Recalculate journal totals based on the current journal entries.
        This ensures the journal header reflects the actual sum of all entries.
        """
        if self.entries:
            total_debit = Decimal('0.00')
            total_credit = Decimal('0.00')

            for entry in self.entries:
                if entry.dr_cr == 'D':
                    total_debit += Decimal(str(entry.amount))
                elif entry.dr_cr == 'C':
                    total_credit += Decimal(str(entry.amount))

            self.total_debit = total_debit
            self.total_credit = total_credit
            self.balance = total_debit - total_credit
        else:
            # If no entries, reset totals to zero
            self.total_debit = Decimal('0.00')
            self.total_credit = Decimal('0.00')
            self.balance = Decimal('0.00')


class JournalEntry(Base):
    __tablename__ = 'journal_entries'

    id = Column(Integer, primary_key=True)
    journal_id = Column(Integer, ForeignKey('journals.id'), nullable=False, index=True)
    line_number = Column(Integer, nullable=False)

    journal_number = Column(String(50), nullable=False, index=True)

    date = Column(Date, index=True)
    subcategory_id = Column(Integer, ForeignKey('chart_of_accounts.id'), index=True)
    amount = Column(Numeric(15, 2), default=0.00)
    dr_cr = Column(String(1), index=True)
    description = Column(String(100))
    source_type = Column(String(50), nullable=True, index=True)
    source_id = Column(Integer, nullable=True, index=True)
    date_added = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
    reconciled = Column(Boolean, default=False, index=True)
    reconciliation_date = Column(Date, nullable=True, index=True)

    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index('idx_journal_entries_journal_id_line', 'journal_id', 'line_number'),
        Index('idx_journal_entries_date', 'date'),
        Index('idx_journal_entries_subcategory', 'subcategory_id'),
        Index('idx_journal_entries_dr_cr', 'dr_cr'),
        Index('idx_journal_entries_journal_number', 'journal_number'),

        # ADD THESE NEW INDEXES:
        Index('idx_entries_app_journal', 'journal_id'),  # For joining
        Index('idx_entries_subcategory_app', 'subcategory_id'),  # For category filtering

        Index('idx_entries_app_subcategory_journal', 'subcategory_id', 'journal_id'),
        Index('idx_entries_app_date_dr_cr', 'date', 'dr_cr'),

        UniqueConstraint('journal_id', 'line_number', name='uq_journal_line_number'),
        CheckConstraint('amount >= 0', name='chk_journal_entries_positive_amount'),
    )

    journal = relationship("Journal", back_populates="entries")
    chart_of_accounts = relationship('ChartOfAccounts', back_populates='journal_entries')

    def get_account_code(self):
        return self.chart_of_accounts.sub_category_id if self.chart_of_accounts else None

    @validates('dr_cr')
    def validate_dr_cr(self, key, value):
        if value not in ('D', 'C'):
            raise ValueError("dr_cr must be either 'D' or 'C'")
        return value


# Event listener for auto-incrementing line numbers
@event.listens_for(JournalEntry, 'before_insert')
def set_line_number(mapper, connection, target):
    if target.line_number is None and target.journal_id is not None:
        max_line = connection.execute(
            select(func.max(JournalEntry.line_number))
            .where(JournalEntry.journal_id == target.journal_id)
        ).scalar()
        target.line_number = (max_line or 0) + 1


# Event listener to set denormalized fields before insert
@event.listens_for(JournalEntry, 'before_insert')
def set_denormalized_fields(mapper, connection, target):
    if target.journal_id is not None:
        journal = connection.execute(
            select(Journal.journal_number, Journal.date)
            .where(Journal.id == target.journal_id)
        ).first()

        if journal:
            target.journal_number = journal.journal_number

            if target.date is None:
                target.date = journal.date


# iNTRASONIC Expiry dates tracking

class InternetPayment(Base):
    __tablename__ = 'internet_payments'

    id = Column(Integer, primary_key=True)
    transaction_fk = Column(Integer, ForeignKey('transactions.id'))
    journal_fk = Column(Integer, ForeignKey('journals.id'))
    site_id = Column(Integer, ForeignKey('projects.id'))
    date_of_expiry = Column(Date)

    journals = relationship('Journal', back_populates='internet_payments')

    project = relationship('Project', back_populates='internet_payments')


class LoanStatus(enum.Enum):
    PENDING = "Pending"
    APPROVED = "Approved"
    ACTIVE = "Active"
    REPAID = "Repaid"
    OVERDUE = "Overdue"
    DEFAULTED = "Defaulted"
    EXTENDED = "Extended"
    RESTARTED = "Restarted"  # In case of restart due to missed payments
    CANCELLED = "Cancelled"  # If the loan is cancelled for any reason
    PARTIALLY_REPAID = "Partially Repaid"  # If part of the loan is repaid before the due date
    NOT_APPROVED = "Not Approved"
    DISCONTINUED = "Discontinued"


class LoanApplicant(Base):
    __tablename__ = 'loan_applicant'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    nin_number = Column(String(50), nullable=False, unique=True)
    telephone = Column(String(20), nullable=False)
    email = Column(String(100), nullable=False)
    country_code = Column(String(10), default='+256')

    loan_application = relationship('LoanApplication', back_populates='loan_applicant')


class LoanApplication(Base):
    __tablename__ = 'loan_application'
    id = Column(Integer, primary_key=True)
    date_of_request = Column(Date, default=func.current_date(), nullable=False)
    loan_applicant_id = Column(Integer, ForeignKey('loan_applicant.id'), nullable=False)
    loan_status = Column(Enum(LoanStatus), default=LoanStatus.PENDING)  # Use Enum for loan status
    currency = Column(String(100))
    principal_amount = Column(Float)
    telephone = Column(String(20))  # Changed to String
    email = Column(String(100))
    date_of_payment = Column(Date)
    interest_amount = Column(Float)
    guarantor_name = Column(String(100))
    guarantor_contact = Column(String(100))
    loan_period = Column(Integer, default=0)
    deposit_number = Column(String(20))
    registered_names = Column(String(100), nullable=False)

    loan_history = relationship('LoanHistory', back_populates='loan_application')
    loan_applicant = relationship('LoanApplicant', back_populates='loan_application')


class LoanHistory(Base):
    __tablename__ = 'loan_history'

    id = Column(Integer, primary_key=True)
    loan_id = Column(Integer, ForeignKey('loan_application.id'), nullable=False)

    # Dates
    deadline_date = Column(Date)  # Original due date
    penalty_date = Column(Date)  # Date fine was applied
    restart_date = Column(Date, nullable=True)  # If the loan restarted, store restart date
    new_due_date = Column(Date, nullable=True)  # New due date if restarted
    creation_date = Column(DateTime,
                           default=lambda: datetime.now(timezone.utc))  # Creation date with default to current UTC time

    # Status & Financials
    repayment_amount = Column(Float, default=0)  # Expected repayment amount
    amount_paid = Column(Float, default=0)  # Amount borrower has paid
    penalty_amount = Column(Float, default=0)  # Latest penalty fine applied
    total_fine_amount = Column(Float, default=0)  # Sum of all fines applied
    new_interest_amount = Column(Float, default=0)  # Adjusted interest rate if restarted
    discount_amount = Column(Float, default=0)  # Discount amount applied to the loan
    notes = Column(String(200), default="N/A")  # Any comments

    # Relationships
    loan_application = relationship('LoanApplication', back_populates='loan_history')


# ******************Pay Roll Schema ********************************************

class PayRollStatusEnum(enum.Enum):
    PENDING = "Pending"
    PARTIAL = "Partial"
    PAID = "Paid"
    OPEN = "Open"
    CLOSED = "Closed"
    CANCELLED = "Cancelled"


class PaymentStatusEnum(enum.Enum):
    PENDING = "pending"
    PAID = "paid"
    PARTLY_PAID = "partly paid"
    CANCELLED = "cancelled"


class PayrollPeriodStatusEnum(enum.Enum):
    OPEN = "Open"
    CLOSED = "Closed"
    CANCELLED = "Cancelled"


class Employee(Base):
    __tablename__ = 'employees'

    id = Column(Integer, primary_key=True)
    employee_id = Column(String(50), nullable=True)  # Made employee_id unique
    payee_id = Column(Integer, ForeignKey('vendors.id'), nullable=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)  # Added project relationship
    first_name = Column(String(50), nullable=False)
    middle_name = Column(String(50), nullable=True)
    last_name = Column(String(50), nullable=False)
    date_of_birth = Column(Date, nullable=True)
    gender = Column(Enum('Male', 'Female', 'Other'), default='Other')
    email = Column(String(100), nullable=True)
    emergency_contact_name = Column(String(100), nullable=True)
    emergency_contact_phone = Column(String(15), nullable=True)
    department_id = Column(Integer, ForeignKey('departments.id'), nullable=True)
    job_title = Column(String(100), nullable=False)
    address = Column(String(255), nullable=True)
    salary_type = Column(Enum('Hourly', 'Monthly', 'Daily', 'Weekly', 'Bi-weekly', 'Piece Rate', 'Commission', 'Other',
                              name='salary_type_enum'), nullable=False)
    base_salary = Column(Numeric(scale=2))
    base_currency = Column(Integer, ForeignKey('currency.id'), nullable=True)
    payment_account_number = Column(String(50), nullable=True)
    payment_account_name = Column(String(50), nullable=True)
    payment_platform = Column(String(50), nullable=True)
    tax_id = Column(String(20), nullable=True)
    hire_date = Column(Date, nullable=True)
    termination_date = Column(Date, nullable=True)
    last_promotion_date = Column(Date, nullable=True)
    employment_status = Column(
        Enum('Full-time', 'Part-time', 'Contract', 'Intern', 'Temporary', 'Seasonal', 'Freelance', 'Apprentice',
             name='employment_status_enum'),
        nullable=False
    )
    work_location = Column(String(100), nullable=True)
    performance_rating = Column(DECIMAL(3, 2), nullable=True)
    skills = Column(Text, nullable=True)
    certifications = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True)
    creation_date = Column(DateTime,
                           default=lambda: datetime.now(timezone.utc))  # Creation date with default to current UTC time
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # --- Accounting linkages ---
    payroll_expense_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    deduction_payable_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    advance_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    payable_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)

    # Relationships
    # Explicitly specify foreign key for department relationship
    department = relationship("Department", back_populates="employees", foreign_keys=[department_id])
    project = relationship("Project", back_populates="employees",
                           foreign_keys=[project_id])  # Added project relationship
    benefits = relationship('Benefit', back_populates='employees', cascade="all, delete-orphan")
    deductions = relationship("Deduction", back_populates="employees", cascade="all, delete-orphan")
    currency = relationship("Currency", back_populates="employees")
    payroll_transactions = relationship("PayrollTransaction", back_populates="employees")
    employee_deductions = relationship("EmployeeDeduction", back_populates="employees")
    employee_benefits = relationship("EmployeeBenefit", back_populates="employees")
    advance_payments = relationship("AdvancePayment", back_populates="employees")
    payee = relationship("Vendor", back_populates="employees")

    # Accounting relationships
    payroll_expense_account = relationship("ChartOfAccounts", foreign_keys=[payroll_expense_account_id])
    deduction_payable_account = relationship("ChartOfAccounts", foreign_keys=[deduction_payable_account_id])
    advance_account = relationship("ChartOfAccounts", foreign_keys=[advance_account_id])
    payable_account = relationship("ChartOfAccounts", foreign_keys=[payable_account_id])

    # Asset
    approved_asset_transfers = relationship("AssetTransfer",
                                            foreign_keys="[AssetTransfer.approved_by_id]",
                                            back_populates="approved_by")

    assigned_assets = relationship("Asset", back_populates="assigned_to")
    # ✅ ADD THIS relationship for AssetMovementLineItem assignments
    asset_movement_assignments = relationship(
        "AssetMovementLineItem",
        foreign_keys="[AssetMovementLineItem.assigned_to_id]",
        back_populates="assigned_to"
    )

    # Indexes for better query performance

    Index('ix_employees_department_id', 'department_id'),
    Index('ix_employees_project_id', 'project_id'),  # Added index for project_id
    Index('ix_employees_is_active', 'is_active'),
    Index('ix_employees_employment_status', 'employment_status'),
    Index('ix_employees_hire_date', 'hire_date'),
    Index('ix_employees_termination_date', 'termination_date'),

    # Indexes for accounting linkages
    Index('ix_employees_payroll_expense_account_id', 'payroll_expense_account_id'),
    Index('ix_employees_deduction_payable_account_id', 'deduction_payable_account_id'),
    Index('ix_employees_advance_account_id', 'advance_account_id'),
    Index('ix_employees_payable_account_id', 'payable_account_id'),

    # Composite indexes for common query patterns
    Index('ix_employees_app_active', 'is_active'),
    Index('ix_employees_app_department', 'department_id'),
    Index('ix_employees_app_project', 'project_id'),  # Added composite index
    Index('ix_employees_name_search', 'first_name', 'last_name'),
    Index('ix_employees_app_status_active', 'employment_status', 'is_active')


class Department(Base):
    __tablename__ = 'departments'

    id = Column(Integer, primary_key=True)
    department_name = Column(String(100), nullable=True)  # Default value set to "General"
    department_head_id = Column(Integer, ForeignKey('employees.id'), nullable=True)  # FK to Employee
    location = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    budget = Column(DECIMAL(15, 2), nullable=True)
    creation_date = Column(DateTime,
                           default=lambda: datetime.now(timezone.utc))  # Creation date with default to current UTC time
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Relationships to Employee table
    # Explicitly define relationship for employees, specifying foreign key path
    employees = relationship("Employee", back_populates="department", foreign_keys=[Employee.department_id])

    company = relationship("Company", back_populates="departments")

    #Assets
    asset_transfers_from = relationship("AssetTransfer",
                                        foreign_keys="[AssetTransfer.from_department_id]",
                                        back_populates="from_department")

    asset_transfers_to = relationship("AssetTransfer",
                                      foreign_keys="[AssetTransfer.to_department_id]",
                                      back_populates="to_department")

    # (You should already have this from Asset model):
    assets = relationship("Asset", back_populates="department")


class PayrollTransaction(Base):
    __tablename__ = 'payroll_transactions'

    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey('employees.id'), nullable=False)
    gross_salary = Column(DECIMAL(10, 2), nullable=True)
    payroll_period_id = Column(Integer, ForeignKey('payroll_periods.id'), nullable=False)
    net_salary = Column(DECIMAL(10, 2), nullable=True)

    payment_status = Column(Enum(PaymentStatusEnum), default=PaymentStatusEnum.PENDING)  # Use PaymentStatusEnum
    payable_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'),
                                nullable=True)  # New field for liability account
    paid_amount = Column(DECIMAL(10, 2), default=0.00)
    balance_due = Column(DECIMAL(10, 2), nullable=True)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)  # ADD THIS
    creation_date = Column(DateTime,
                           default=lambda: datetime.now(timezone.utc))  # Creation date with default to current UTC time
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)

    # Relationships
    employees = relationship("Employee", back_populates="payroll_transactions")
    payroll_period = relationship("PayrollPeriod", back_populates="payroll_transactions")
    currency = relationship("Currency", back_populates="payroll_transactions")
    benefits = relationship('Benefit', back_populates='payroll_transactions')
    deductions = relationship('Deduction', back_populates='payroll_transactions')
    payroll_payments = relationship('PayrollPayment', back_populates='payroll_transactions')
    advance_repayment = relationship('AdvanceRepayment', back_populates='payroll_transactions')
    payable_account = relationship(
        "ChartOfAccounts",
        foreign_keys=[payable_account_id],
        back_populates="payroll_transactions_payable"
    )
    creator = relationship('User', back_populates='payroll_transactions')
    exchange_rate = relationship("ExchangeRate", back_populates="payroll_transactions")  # ADD THIS

    __table_args__ = (
        Index('idx_payroll_transactions_period', 'payroll_period_id'),
        Index('idx_payroll_transactions_employee', 'employee_id'),
        Index('idx_payroll_transactions_currency', 'currency_id'),
        Index('idx_payroll_transactions_exchange_rate', 'exchange_rate_id'),
        Index('idx_payroll_transactions_created_by', 'created_by'),
    )

class PayrollPayment(Base):
    __tablename__ = 'payroll_payments'

    id = Column(Integer, primary_key=True)
    payroll_transaction_id = Column(Integer, ForeignKey('payroll_transactions.id', ondelete="CASCADE"), nullable=False)
    payment_date = Column(Date, nullable=False)
    amount = Column(DECIMAL(15, 2), nullable=False)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)  # Uses FK instead of string
    payment_method = Column(Integer, ForeignKey('payment_modes.id'), nullable=True)
    payment_account = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)

    reference = Column(String(255), nullable=True)  # Transaction ID, cheque number, etc.
    notes = Column(Text, nullable=True)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)  # Indicates if entry is posted to the ledger
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    created_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)
    version = Column(Integer, default=1, nullable=False)

    # Relationships
    payroll_transactions = relationship("PayrollTransaction", back_populates="payroll_payments")
    currency = relationship("Currency", back_populates='payroll_payments')
    # Explicitly specify foreign keys for both account relationships
    funding_account = relationship(
        "ChartOfAccounts",
        foreign_keys=[payment_account],
        back_populates="payroll_payments_funding"
    )

    payment_modes = relationship('PaymentMode', back_populates='payroll_payments')
    exchange_rate = relationship('ExchangeRate', back_populates='payroll_payments')
    creator = relationship("User", back_populates='payroll_payments')



class PayrollPeriod(Base):
    __tablename__ = 'payroll_periods'

    id = Column(Integer, primary_key=True)
    payroll_period_name = Column(String(50), nullable=False)  # Removed unique=True here
    payroll_frequency = Column(
        Enum('Monthly', 'Bi-weekly', 'Weekly', 'Daily', name='payroll_frequency_enum'),
        nullable=False
    )
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    payroll_status = Column(Enum(PayrollPeriodStatusEnum),
                            default=PayrollPeriodStatusEnum.OPEN)  # Use PayrollPeriodStatusEnum
    payment_status = Column(Enum(PayRollStatusEnum), default=PayRollStatusEnum.PENDING)  # Use PayRollStatusEnum
    creation_date = Column(DateTime,
                           default=lambda: datetime.now(timezone.utc))  # Creation date with default to current UTC time
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)  # ADD THIS

    # Relationships
    payroll_transactions = relationship("PayrollTransaction", back_populates="payroll_period")
    benefits = relationship("Benefit", back_populates="payroll_periods")
    deductions = relationship("Deduction", back_populates="payroll_periods")
    advance_repayment = relationship("AdvanceRepayment", back_populates="payroll_periods")
    creator = relationship("User", back_populates="payroll_periods")  # ADD THIS

    __table_args__ = (
        UniqueConstraint('payroll_period_name',name='_payroll_period_name_app_uc'),
    )


class DeductionType(Base):
    __tablename__ = 'deduction_types'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    rate = Column(Numeric(scale=2), nullable=True)  # Deduction rate
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=True)  # FK to Currency table
    fixed_amount = Column(Numeric(scale=2))  # Deduction fixed amount
    is_rate = Column(Boolean, default=False)
    creation_date = Column(DateTime,
                           default=lambda: datetime.now(timezone.utc))  # Creation date with default to current UTC time
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    deductions = relationship("Deduction", back_populates="deduction_type", cascade="all, delete-orphan")
    employee_deductions = relationship("EmployeeDeduction", back_populates="deduction_type")
    currency = relationship("Currency", back_populates="deduction_types")

    def __repr__(self):
        return (f"<DeductionType(id={self.id}, name='{self.name}', rate={self.rate}, "
                f"fixed_amount={self.fixed_amount}, is_rate={self.is_rate})>")


class Deduction(Base):
    __tablename__ = 'deductions'

    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey('employees.id'), nullable=False)
    deduction_type_id = Column(Integer, ForeignKey('deduction_types.id'), nullable=False)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=True)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)  # ADD THIS
    amount = Column(Numeric(scale=2))
    paid_amount = Column(DECIMAL(10, 2), default=0.00)
    balance_due = Column(DECIMAL(10, 2), nullable=True, default=0.00)

    payable_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'),
                                nullable=True)  # New field for liability account
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)  # Indicates if entry is posted to the ledger
    payroll_transaction_id = Column(Integer, ForeignKey('payroll_transactions.id'), nullable=False)

    payroll_period_id = Column(Integer, ForeignKey('payroll_periods.id'), nullable=False)
    payment_status = Column(Enum(PaymentStatusEnum), default=PaymentStatusEnum.PENDING)
    creation_date = Column(DateTime,
                           default=lambda: datetime.now(timezone.utc))  # Creation date with default to current UTC time
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    employees = relationship("Employee", back_populates="deductions")
    deduction_type = relationship("DeductionType", back_populates="deductions")
    currency = relationship('Currency', back_populates="deductions")
    payroll_transactions = relationship('PayrollTransaction', back_populates='deductions')
    payroll_periods = relationship("PayrollPeriod", back_populates="deductions")
    deduction_payments = relationship('DeductionPayment', back_populates='deductions')
    exchange_rate = relationship('ExchangeRate', back_populates="deductions")
    payable_account = relationship(
        "ChartOfAccounts",
        foreign_keys=[payable_account_id],
        back_populates="payroll_deductions_payable"
    )
    __table_args__ = (
        Index('idx_deductions_payroll_transaction', 'payroll_transaction_id'),
        Index('idx_deductions_payroll_period', 'payroll_period_id'),
        Index('idx_deductions_employee', 'employee_id'),
        Index('idx_deductions_exchange_rate', 'exchange_rate_id'),
        Index('idx_deductions_status', 'payment_status'),
    )
    def __repr__(self):
        return (f"<Deduction(id={self.id}, employee_id={self.employee_id}, "
                f"deduction_type_id={self.deduction_type_id}, amount={self.amount}, "
                f"paid_amount={self.paid_amount}, balance_due={self.balance_due})>, currency={self.currency}")


class BenefitType(Base):
    __tablename__ = 'benefit_types'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    rate = Column(Numeric(scale=2))  # Benefit rate
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=True)  # FK to Currency table
    fixed_amount = Column(Numeric(scale=2))  # Benefit fixed amount
    is_rate = Column(Boolean, default=False)
    creation_date = Column(DateTime,
                           default=lambda: datetime.now(timezone.utc))  # Creation date with default to current UTC time
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    benefits = relationship('Benefit', back_populates='benefit_type', cascade="all, delete-orphan")
    employee_benefits = relationship("EmployeeBenefit", back_populates="benefit_types")
    currency = relationship('Currency', back_populates="benefit_types")

    def __repr__(self):
        return (f"<BenefitType(id={self.id}, name='{self.name}', rate={self.rate}, "
                f"fixed_amount={self.fixed_amount}, is_rate={self.is_rate})>")


class Benefit(Base):
    __tablename__ = 'benefits'

    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey('employees.id'), nullable=False)
    benefit_type_id = Column(Integer, ForeignKey('benefit_types.id'), nullable=False)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=True)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)  # ADD THIS
    amount = Column(Numeric(scale=2))
    payroll_transaction_id = Column(Integer, ForeignKey('payroll_transactions.id'), nullable=False)
    payroll_period_id = Column(Integer, ForeignKey('payroll_periods.id'), nullable=False)
    creation_date = Column(DateTime,
                           default=lambda: datetime.now(timezone.utc))  # Creation date with default to current UTC time
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    employees = relationship("Employee", back_populates="benefits")
    benefit_type = relationship("BenefitType", back_populates="benefits")
    currency = relationship('Currency', back_populates="benefits")
    payroll_transactions = relationship('PayrollTransaction', back_populates='benefits')
    payroll_periods = relationship("PayrollPeriod", back_populates="benefits")
    exchange_rate = relationship('ExchangeRate', back_populates="benefits")


    __table_args__ = (
        Index('idx_benefits_payroll_transaction', 'payroll_transaction_id'),
        Index('idx_benefits_payroll_period', 'payroll_period_id'),
        Index('idx_benefits_employee', 'employee_id'),
        Index('idx_benefits_exchange_rate', 'exchange_rate_id'),
    )
    def __repr__(self):
        return (f"<Benefit(benefit_id={self.benefit_id}, employee_id={self.employee_id}, "
                f"benefit_type_id={self.benefit_type_id}, amount={self.amount}, "
                f"calculated_amount={self.calculated_amount})>")


class EmployeeDeduction(Base):
    __tablename__ = 'employee_deductions'

    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey('employees.id'), nullable=False)  # Employee associated with the deduction
    deduction_type_id = Column(Integer, ForeignKey('deduction_types.id'), nullable=False)  # Type of deduction

    # Relationships
    employees = relationship("Employee", back_populates="employee_deductions")
    deduction_type = relationship("DeductionType", back_populates="employee_deductions")

    def __repr__(self):
        return f"<EmployeeDeduction(id={self.id}, employee_id={self.employee_id}, deduction_type_id={self.deduction_type_id})>"


class EmployeeBenefit(Base):
    __tablename__ = 'employee_benefits'

    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey('employees.id'), nullable=False)  # Employee associated with the benefit
    benefit_type_id = Column(Integer, ForeignKey('benefit_types.id'), nullable=False)  # Type of benefit

    # Relationships
    employees = relationship("Employee", back_populates="employee_benefits")
    benefit_types = relationship("BenefitType", back_populates="employee_benefits")

    def __repr__(self):
        return f"<EmployeeBenefit(id={self.id}, employee_id={self.employee_id}, benefit_type_id={self.benefit_type_id})>"


class DeductionPayment(Base):
    __tablename__ = 'deduction_payments'

    id = Column(Integer, primary_key=True)
    deduction_id = Column(Integer, ForeignKey('deductions.id', ondelete="CASCADE"), nullable=False)
    payment_date = Column(Date, nullable=False)
    amount = Column(DECIMAL(15, 2), nullable=False)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)  # Uses FK instead of string
    payment_method = Column(Integer, ForeignKey('payment_modes.id'), nullable=True)
    payment_account = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    reference = Column(String(255), nullable=True)  # Transaction ID, cheque number, etc.
    notes = Column(Text, nullable=True)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)  # Indicates if entry is posted to the ledger
    created_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)
    version = Column(Integer, default=1, nullable=False)

    # Relationships
    deductions = relationship("Deduction", back_populates="deduction_payments")
    currency = relationship("Currency", back_populates='deduction_payments')
    chart_of_accounts = relationship('ChartOfAccounts', back_populates='deduction_payments')
    payment_modes = relationship('PaymentMode', back_populates='deduction_payments')
    exchange_rate = relationship("ExchangeRate", back_populates="deduction_payments")
    creator = relationship("User", back_populates="deduction_payments")

class AdvancePayment(Base):
    __tablename__ = 'advance_payments'

    id = Column(Integer, primary_key=True)
    employee_id = Column(Integer, ForeignKey('employees.id'), nullable=False)
    advance_amount = Column(DECIMAL(10, 2), nullable=False)  # Full advance amount
    amount_paid = Column(DECIMAL(10, 2), nullable=False, default=decimal.Decimal("0.00"))
    remaining_balance = Column(DECIMAL(10, 2), nullable=False, default=decimal.Decimal("0.00"))
    payment_date = Column(Date, nullable=False, default=lambda: datetime.now(timezone.utc))
    payment_method = Column(Integer, ForeignKey('payment_modes.id'), nullable=True)
    payment_account = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    deduction_per_payroll = Column(DECIMAL(10, 2), nullable=False)
    repayment_status = Column(
        Enum('Pending', 'Partly Paid', 'Paid', name='repayment_status_enum'),
        default='Pending'
    )
    notes = Column(Text, nullable=True)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)
    version = Column(Integer, default=1, nullable=False)

    # Relationships
    employees = relationship("Employee", back_populates="advance_payments")
    currency = relationship("Currency", back_populates="advance_payments")
    payment_modes = relationship("PaymentMode", back_populates="advance_payments")
    chart_of_accounts = relationship('ChartOfAccounts', back_populates='advance_payments')
    advance_repayment = relationship('AdvanceRepayment', back_populates='advance_payments')
    exchange_rate = relationship('ExchangeRate', back_populates='advance_payments')

    # Auto-update remaining balance and ensure it doesn't go below 0
    def update_balances(self):
        # Ensure both advance_amount and amount_paid are Decimal and perform the calculation
        self.remaining_balance = max(
            decimal.Decimal(str(self.advance_amount)) - decimal.Decimal(str(self.amount_paid)),
            decimal.Decimal("0.00")
        )
        self.update_repayment_status()

    # Ensure repayment status is updated correctly
    def update_repayment_status(self):
        if self.remaining_balance == 0:
            self.repayment_status = 'Paid'
        elif self.remaining_balance < self.advance_amount:
            self.repayment_status = 'Partly Paid'
        else:
            self.repayment_status = 'Pending'


class AdvanceRepayment(Base):
    __tablename__ = 'advance_repayment'

    id = Column(Integer, primary_key=True)
    advance_payment_id = Column(Integer, ForeignKey('advance_payments.id'),
                                nullable=False)  # Link to the advance payment
    payment_date = Column(Date, nullable=False)  # Date when the payment was made
    payment_amount = Column(DECIMAL(10, 2), nullable=False)  # Amount paid
    payroll_id = Column(Integer, ForeignKey('payroll_periods.id'),
                        nullable=True)  # Link to the payroll record (if applicable)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)
    payroll_transaction_id = Column(Integer, ForeignKey('payroll_transactions.id'),
                                    nullable=True)

    # Relationships
    advance_payments = relationship("AdvancePayment", back_populates="advance_repayment")
    payroll_periods = relationship("PayrollPeriod", back_populates="advance_repayment")
    payroll_transactions = relationship("PayrollTransaction", back_populates="advance_repayment")
    exchange_rate = relationship("ExchangeRate", back_populates="advance_repayments")


# **********************Pay Roll End ********************************************************
class ActivityLog(Base):
    __tablename__ = 'activity_log'
    id = Column(Integer, primary_key=True)
    user = Column(String(50), nullable=False)
    activity = Column(String(255), nullable=False)
    date = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    details = Column(String(200), nullable=True)


# Asset Management

class AssetItem(Base):
    """DEFINES a type of asset - like a product catalog for assets"""
    __tablename__ = 'asset_item'

    id = Column(Integer, primary_key=True)

    # Identification
    asset_name = Column(String(100), nullable=False)  # "Dell XPS 13 Laptop"
    asset_code = Column(String(50), unique=True)  # "LAP-DELL-XPS13"

    # Categorization (REUSE existing)
    brand_id = Column(Integer, ForeignKey('brand.id'))
    item_category_id = Column(Integer, ForeignKey('inventory_category.id'))
    item_subcategory_id = Column(Integer, ForeignKey('inventory_subcategory.id'))

    # Description
    asset_description = Column(String(300))
    image_filename = Column(String(255))

    # Status
    status = Column(String(50), default="active", nullable=False)

    # ======================
    # DEFAULT SETTINGS for all instances of this type
    # ======================

    # Financial defaults
    expected_useful_life_years = Column(Integer, default=5)
    depreciation_method = Column(String(50), default='straight_line')
    salvage_value_percentage = Column(Numeric(5, 2), default=10.00)

    # Accounting defaults
    fixed_asset_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)
    depreciation_expense_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)
    accumulated_depreciation_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)

    # Maintenance default
    maintenance_interval_days = Column(Integer, default=365)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    # ======================
    # RELATIONSHIPS
    # ======================

    brand = relationship("Brand", back_populates="asset_items")
    inventory_category = relationship("InventoryCategory", back_populates="asset_items")
    inventory_subcategory = relationship("InventorySubCategory", back_populates="asset_items")

    # Accounting

    # Child assets of this type
    assets = relationship("Asset", back_populates="asset_item", cascade="all, delete-orphan")

    fixed_asset_account = relationship("ChartOfAccounts",
                                       foreign_keys=[fixed_asset_account_id],
                                       back_populates="asset_fixed_asset_accounts")  # MATCH ChartOfAccounts relationship name
    depreciation_expense_account = relationship("ChartOfAccounts",
                                                foreign_keys=[depreciation_expense_account_id],
                                                back_populates="asset_depreciation_accounts")  # MATCH ChartOfAccounts
    accumulated_depreciation_account = relationship("ChartOfAccounts",
                                                    foreign_keys=[accumulated_depreciation_account_id],
                                                    back_populates="asset_accumulated_depreciation_accounts")  # MATCH


class Asset(Base):
    """A SPECIFIC physical asset instance"""
    __tablename__ = 'asset'

    id = Column(Integer, primary_key=True)

    # Asset type
    asset_item_id = Column(Integer, ForeignKey('asset_item.id'), nullable=False)

    # Unique identification
    asset_tag = Column(String(100), unique=True, nullable=False)
    serial_number = Column(String(100), unique=True)

    # Current state
    status = Column(String(50), default='in_stock')
    condition = Column(String(50), default='good')

    # Current location/assignment
    location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)
    assigned_to_id = Column(Integer, ForeignKey('employees.id'), nullable=True)
    department_id = Column(Integer, ForeignKey('departments.id'), nullable=True)

    # Specific Purchase details values
    purchase_date = Column(Date)
    purchase_price = Column(Numeric(10, 2), default=0.00)
    current_value = Column(Numeric(10, 2), nullable=False, default=0.00)
    supplier_id = Column(Integer, ForeignKey('vendors.id'), nullable=True)

    # Depreciation
    useful_life_years = Column(Integer, nullable=True)
    depreciation_method = Column(String(50), nullable=True)
    capitalization_date = Column(Date)
    last_depreciation_date = Column(Date, nullable=True)

    # Warranty
    warranty_expiry = Column(Date, nullable=True)

    # Project (optional - could also be at transaction level)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    # Relationships - ADD back_populates
    asset_item = relationship("AssetItem", back_populates="assets")  # ADD back_populates
    location = relationship("InventoryLocation", back_populates="assets")  # ADD back_populates
    supplier = relationship("Vendor", back_populates="assets")
    assigned_to = relationship("Employee", back_populates="assigned_assets")  # This one is OK
    department = relationship("Department", back_populates="assets")  # ADD back_populates
    project = relationship("Project", back_populates="assets")  # ADD back_populates
    movement_line_items = relationship("AssetMovementLineItem", back_populates="assets")
    # Lifecycle records
    maintenance_records = relationship("MaintenanceRecord", back_populates="asset")
    depreciation_records = relationship("DepreciationRecord", back_populates="asset")
    transfer_history = relationship("AssetTransfer", back_populates="asset")


class AssetMovement(Base):
    """Header table for asset movements"""
    __tablename__ = 'asset_movements'

    id = Column(Integer, primary_key=True)

    # Transaction details
    movement_type = Column(String(50), nullable=False)
    transaction_date = Column(Date, nullable=False, default=func.current_date())
    reference = Column(String(100), nullable=True)

    # Project
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)

    # Currency
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)

    # Accounting fields (FK to ChartOfAccounts)
    payable_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    adjustment_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    sales_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)

    # Status
    status = Column(String(20), default='draft')
    is_posted_to_ledger = Column(Boolean, default=False)

    # User tracking
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    created_at = Column(DateTime, default=func.now())

    # ======================
    # RELATIONSHIPS
    # ======================

    # Company relationship

    # Currency relationship
    currency = relationship('Currency', back_populates='asset_movements')

    # Project relationship
    project = relationship('Project', back_populates='asset_movements')

    # Accounting relationships (to ChartOfAccounts)
    payable_account = relationship(
        'ChartOfAccounts',
        foreign_keys=[payable_account_id],
        back_populates='asset_movements_payable'
    )

    adjustment_account = relationship(
        'ChartOfAccounts',
        foreign_keys=[adjustment_account_id],
        back_populates='asset_movements_adjustment'
    )

    sales_account = relationship(
        'ChartOfAccounts',
        foreign_keys=[sales_account_id],
        back_populates='asset_movements_sales'
    )

    # Creator relationship
    creator = relationship('User', foreign_keys=[created_by], back_populates='created_asset_movements')

    # Line items relationship
    line_items = relationship(
        'AssetMovementLineItem',
        back_populates='asset_movement',
        cascade='all, delete-orphan'
    )


class AssetMovementLineItem(Base):
    __tablename__ = 'asset_movement_line_items'

    id = Column(Integer, primary_key=True)
    asset_movement_id = Column(Integer, ForeignKey('asset_movements.id'), nullable=False)

    # Asset reference
    asset_id = Column(Integer, ForeignKey('asset.id'), nullable=False)

    # Movement-specific data - BOTH FROM AND TO
    from_location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)  # Source location
    from_department_id = Column(Integer, ForeignKey('departments.id'), nullable=True)  # Source department
    to_location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)  # Destination location
    to_department_id = Column(Integer, ForeignKey('departments.id'), nullable=True)  # Destination department

    # Assignment specific
    assigned_to_id = Column(Integer, ForeignKey('employees.id'), nullable=True)  # For assignments

    # Financial
    transaction_value = Column(Numeric(10, 2), default=0.00)

    # Party (for purchases, sales, donations)
    party_id = Column(Integer, ForeignKey('vendors.id'), nullable=True)

    # Notes
    line_notes = Column(Text, nullable=True)

    # Relationships
    asset_movement = relationship('AssetMovement', back_populates='line_items')
    assets = relationship('Asset', back_populates='movement_line_items')
    from_location = relationship('InventoryLocation', foreign_keys=[from_location_id])
    from_department = relationship('Department', foreign_keys=[from_department_id])
    to_location = relationship('InventoryLocation', foreign_keys=[to_location_id])
    to_department = relationship('Department', foreign_keys=[to_department_id])
    assigned_to = relationship('Employee', foreign_keys=[assigned_to_id])
    party = relationship('Vendor', back_populates="asset_movement_line_items")
    depreciation_record = relationship("DepreciationRecord", back_populates="movement_line_item",
                                       uselist=False)  # ✅ ADD THIS


class MaintenanceRecord(Base):
    __tablename__ = 'maintenance_record'

    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey('asset.id'), nullable=False)

    maintenance_date = Column(Date, nullable=False)
    maintenance_type = Column(String(50))  # preventive, corrective, emergency
    description = Column(Text)
    cost = Column(Numeric(10, 2))
    performed_by = Column(String(100))
    next_maintenance_date = Column(Date)

    created_at = Column(DateTime, default=func.now())

    # ✅ UPDATE relationships:
    asset = relationship("Asset", back_populates="maintenance_records")


class DepreciationRecord(Base):
    __tablename__ = 'depreciation_record'

    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey('asset.id'), nullable=False)

    # Link to the asset movement that created this depreciation
    asset_movement_line_item_id = Column(Integer, ForeignKey('asset_movement_line_items.id'),
                                         nullable=True)  # ✅ ADD THIS

    # Depreciation details
    depreciation_date = Column(Date, nullable=False)
    depreciation_amount = Column(Numeric(10, 2), nullable=False)

    # Value tracking
    previous_value = Column(Numeric(10, 2), nullable=False)
    new_value = Column(Numeric(10, 2), nullable=False)

    # Optional notes
    notes = Column(Text, nullable=True)

    # User tracking
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())

    # ======================
    # RELATIONSHIPS
    # ======================

    asset = relationship("Asset", back_populates="depreciation_records")
    creator = relationship("User", foreign_keys=[created_by])
    movement_line_item = relationship("AssetMovementLineItem", back_populates="depreciation_record")  # ✅ ADD THIS


class AssetTransfer(Base):
    __tablename__ = 'asset_transfer'

    id = Column(Integer, primary_key=True)
    asset_id = Column(Integer, ForeignKey('asset.id'), nullable=False)

    from_location_id = Column(Integer, ForeignKey('inventory_location.id'))
    to_location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=False)
    from_department_id = Column(Integer, ForeignKey('departments.id'))
    to_department_id = Column(Integer, ForeignKey('departments.id'))

    transfer_date = Column(Date, nullable=False)
    reason = Column(Text)
    approved_by_id = Column(Integer, ForeignKey('employees.id'))

    created_at = Column(DateTime, default=func.now())

    # ✅ UPDATE relationships:
    asset = relationship("Asset", back_populates="transfer_history")
    from_location = relationship("InventoryLocation",
                                 foreign_keys=[from_location_id],
                                 back_populates="asset_transfers_from")
    to_location = relationship("InventoryLocation",
                               foreign_keys=[to_location_id],
                               back_populates="asset_transfers_to")
    from_department = relationship("Department",
                                   foreign_keys=[from_department_id],
                                   back_populates="asset_transfers_from")
    to_department = relationship("Department",
                                 foreign_keys=[to_department_id],
                                 back_populates="asset_transfers_to")
    approved_by = relationship("Employee",
                               foreign_keys=[approved_by_id],
                               back_populates="approved_asset_transfers")


# Inventory
class InventoryItem(Base):
    __tablename__ = 'inventory_item'
    id = Column(Integer, primary_key=True)
    item_name = Column(String(100), nullable=False)
    item_code = Column(String(50))
    brand_id = Column(Integer, ForeignKey('brand.id'))  # Link to the brand table
    item_category_id = Column(Integer, ForeignKey('inventory_category.id'))
    item_subcategory_id = Column(Integer, ForeignKey('inventory_subcategory.id'))
    reorder_point = Column(Integer, default=0)  # Default value
    item_description = Column(String(300))  # Default value
    image_filename = Column(String(255))  # Adjust the length as needed
    status = Column(String(255), default="active", nullable=False)  # 'active' or 'discontinued'

    # ✅ New accounting fields
    cogs_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)
    asset_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)
    sales_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)
    tax_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)

    # ✅ New unit of measurement field
    uom_id = Column(Integer, ForeignKey('unit_of_measurement.id'), nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())  # Note: onupdate is SQLAlchemy-side

    brand = relationship("Brand", back_populates="inventory_items")
    inventory_category = relationship("InventoryCategory", back_populates="inventory_item")
    inventory_subcategory = relationship("InventorySubCategory", back_populates="inventory_item")
    inventory_item_variation_link = relationship("InventoryItemVariationLink", back_populates="inventory_item",
                                                 uselist=True)

    cogs_account = relationship("ChartOfAccounts",
                                back_populates="inventory_cogs_account",
                                foreign_keys=[cogs_account_id])
    asset_account = relationship("ChartOfAccounts",
                                 back_populates="inventory_asset_account",
                                 foreign_keys=[asset_account_id])
    sales_account = relationship("ChartOfAccounts",
                                 back_populates="inventory_sales_account",
                                 foreign_keys=[sales_account_id])
    tax_account = relationship("ChartOfAccounts",
                               back_populates="inventory_tax_account",
                               foreign_keys=[tax_account_id])
    unit_of_measurement = relationship("UnitOfMeasurement", back_populates="inventory_item")


class ItemSellingPrice(Base):
    __tablename__ = 'item_selling_prices'

    id = Column(Integer, primary_key=True)

    # Link to the specific inventory item variation
    inventory_item_variation_link_id = Column(
        Integer,
        ForeignKey('inventory_item_variation_link.id'),
        nullable=False
    )

    # Pricing information
    selling_price = Column(Numeric(15, 4), nullable=False)  # Base selling price
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)

    # Price tiers for quantity breaks
    min_quantity = Column(Numeric(10, 2), default=1.0)  # Minimum quantity for this price tier
    max_quantity = Column(Numeric(10, 2), nullable=True)  # Maximum quantity (null means no upper limit)

    # Customer group pricing (optional)
    customer_group_id = Column(Integer, ForeignKey('customer_groups.id'), nullable=True)

    # Effective date range
    effective_from = Column(DateTime, default=func.now(), nullable=False)
    effective_to = Column(DateTime, nullable=True)  # Null means no expiration

    # Pricing strategy
    price_type = Column(
        String(20),
        default='standard',
        nullable=False,
        comment='standard, promotional, wholesale, retail, contract'
    )

    # Markup/margin information (for reference)
    markup_percentage = Column(Numeric(5, 2), nullable=True)
    margin_percentage = Column(Numeric(5, 2), nullable=True)

    # Status
    is_active = Column(Boolean, default=True, nullable=False)

    # Audit fields
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    updated_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    inventory_item_variation_link = relationship(
        "InventoryItemVariationLink",
        back_populates="selling_prices"
    )
    currency = relationship("Currency", back_populates="item_selling_prices")
    customer_group = relationship("CustomerGroup", back_populates="item_selling_prices")
    created_user = relationship("User", foreign_keys=[created_by], back_populates="created_selling_prices")
    updated_user = relationship("User", foreign_keys=[updated_by], back_populates="updated_selling_prices")

    # Indexes for better performance
    __table_args__ = (
        Index('ix_selling_prices_item_variation', 'inventory_item_variation_link_id'),
        Index('ix_selling_prices_effective_dates', 'effective_from', 'effective_to'),
        Index('ix_selling_prices_currency', 'currency_id'),
        Index('ix_selling_prices_customer_group', 'customer_group_id'),
        Index('ix_selling_prices_active', 'is_active'),
        UniqueConstraint(

            'inventory_item_variation_link_id',
            'currency_id',
            'min_quantity',
            'customer_group_id',
            'price_type',
            name='uq_selling_price_combo'
        ),
    )

    def __repr__(self):
        return f"<ItemSellingPrice(id={self.id}, price={self.selling_price}, currency={self.currency_id})>"


class CustomerGroup(Base):
    __tablename__ = 'customer_groups'

    id = Column(Integer, primary_key=True)
    # Group information
    name = Column(String(100), nullable=False, unique=False)
    description = Column(String(300), nullable=True)

    # Pricing settings
    default_discount_percentage = Column(Numeric(5, 2), default=0.0)
    default_tax_percentage = Column(Numeric(5, 2), default=0.0)

    # Status
    is_active = Column(Boolean, default=True, nullable=False)
    is_default = Column(Boolean, default=False, nullable=False)  # Added is_default field

    # Audit fields
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    updated_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    # Relationships
    customers = relationship("Vendor", back_populates="customer_group")
    item_selling_prices = relationship("ItemSellingPrice", back_populates="customer_group")
    created_user = relationship("User", foreign_keys=[created_by], back_populates="created_customer_groups")
    updated_user = relationship("User", foreign_keys=[updated_by], back_populates="updated_customer_groups")

    # Indexes for better performance
    __table_args__ = (
        Index('ix_customer_groups_name', 'name'),
        Index('ix_customer_groups_active', 'is_active'),
        Index('ix_customer_groups_default', 'is_default'),  # Added index for is_default
        UniqueConstraint('name', name='uq_customer_group_name_per_company'),
    )

    def __repr__(self):
        return f"<CustomerGroup(id={self.id}, name={self.name})>"


class Brand(Base):
    __tablename__ = 'brand'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(String(300))

    inventory_items = relationship("InventoryItem", back_populates="brand")
    asset_items = relationship("AssetItem", back_populates="brand")


class UnitOfMeasurement(Base):
    __tablename__ = 'unit_of_measurement'

    id = Column(Integer, primary_key=True)
    full_name = Column(String(100), nullable=False)  # Required field for the full name
    abbreviation = Column(String(20), nullable=True)  # Optional field for abbreviation

    # Relationships

    quotation_items = relationship("QuotationItem", back_populates='unit_of_measurement')
    sales_order_items = relationship("SalesOrderItem", back_populates='unit_of_measurement')
    delivery_note_items = relationship("DeliveryNoteItem", back_populates='unit_of_measurement')
    sales_invoice_items = relationship("SalesInvoiceItem", back_populates='unit_of_measurement')
    direct_sale_items = relationship("DirectSaleItem", back_populates='unit_of_measurement')
    backorder_items = relationship("BackorderItem", back_populates='unit_of_measurement')

    purchase_order_items = relationship("PurchaseOrderItem", back_populates='unit_of_measurement')
    direct_purchase_items = relationship("DirectPurchaseItem", back_populates='unit_of_measurement')

    inventory_item = relationship("InventoryItem", back_populates='unit_of_measurement')

    def __init__(self, full_name, abbreviation=None):

        self.full_name = full_name
        self.abbreviation = abbreviation

    def __repr__(self):
        return f"<UnitOfMeasurement(id={self.id}, full_name={self.full_name}, abbreviation={self.abbreviation})>"


class InventoryCategory(Base):
    __tablename__ = 'inventory_category'
    id = Column(Integer, primary_key=True)
    category_name = Column(String(50), nullable=False)

    inventory_item = relationship('InventoryItem', back_populates='inventory_category')
    inventory_subcategory = relationship('InventorySubCategory', back_populates='inventory_category')
    asset_items = relationship("AssetItem", back_populates="inventory_category")


class InventorySubCategory(Base):
    __tablename__ = 'inventory_subcategory'
    id = Column(Integer, primary_key=True)
    item_category_id = Column(Integer, ForeignKey('inventory_category.id'))
    subcategory_name = Column(String(50), nullable=False)

    inventory_item = relationship('InventoryItem', back_populates='inventory_subcategory')
    inventory_category = relationship('InventoryCategory', back_populates='inventory_subcategory')
    asset_items = relationship("AssetItem", back_populates="inventory_subcategory")


class InventoryItemAttribute(Base):
    __tablename__ = 'inventory_item_attribute'
    id = Column(Integer, primary_key=True)
    attribute_name = Column(String(50), nullable=False)

    inventory_item_variation_link = relationship('InventoryItemVariationLink',
                                                 back_populates='inventory_item_attributes')
    inventory_item_variation = relationship("InventoryItemVariation", back_populates="inventory_item_attributes")


class InventoryItemVariation(Base):
    __tablename__ = 'inventory_item_variation'
    id = Column(Integer, primary_key=True)
    variation_name = Column(String(50), nullable=True)
    attribute_id = Column(Integer, ForeignKey('inventory_item_attribute.id'))

    inventory_item_variation_link = relationship("InventoryItemVariationLink",
                                                 back_populates="inventory_item_variation")
    inventory_item_attributes = relationship("InventoryItemAttribute", back_populates="inventory_item_variation")


class InventoryItemVariationLink(Base):
    __tablename__ = 'inventory_item_variation_link'
    id = Column(Integer, primary_key=True)
    inventory_item_id = Column(Integer, ForeignKey('inventory_item.id'), nullable=False)
    inventory_item_variation_id = Column(Integer, ForeignKey('inventory_item_variation.id'), nullable=True)
    attribute_id = Column(Integer, ForeignKey('inventory_item_attribute.id'), nullable=True)
    status = Column(String(255), default="active", nullable=False)  # 'active' or 'discontinued'

    # Relationships
    inventory_item = relationship("InventoryItem", back_populates="inventory_item_variation_link")
    inventory_item_variation = relationship("InventoryItemVariation", back_populates="inventory_item_variation_link")
    inventory_item_attributes = relationship("InventoryItemAttribute", back_populates="inventory_item_variation_link")

    quotation_items = relationship('QuotationItem',
                                   back_populates='inventory_item_variation_link')
    sales_order_items = relationship('SalesOrderItem',
                                     back_populates='inventory_item_variation_link')
    delivery_note_items = relationship('DeliveryNoteItem',
                                       back_populates='inventory_item_variation_link')
    sales_invoice_items = relationship('SalesInvoiceItem',
                                       back_populates='inventory_item_variation_link')
    direct_sale_items = relationship('DirectSaleItem',
                                     back_populates='inventory_item_variation_link')

    purchase_order_items = relationship('PurchaseOrderItem',
                                        back_populates='inventory_item_variation_link')

    direct_purchase_items = relationship('DirectPurchaseItem',
                                         back_populates='inventory_item_variation_link')

    inventory_entry_line_items = relationship("InventoryEntryLineItem",
                                              back_populates="inventory_item_variation_link")

    inventory_transaction_details = relationship('InventoryTransactionDetail',
                                                 back_populates='inventory_item_variation_link')

    backorder_items = relationship('BackorderItem', back_populates='inventory_item_variation_link')  # Fixed relation
    inventory_summary = relationship('InventorySummary', back_populates='inventory_item_variation')
    # Add this relationship
    selling_prices = relationship(
        "ItemSellingPrice",
        back_populates="inventory_item_variation_link",
        cascade="all, delete-orphan"
    )


class InventoryLocation(Base):
    __tablename__ = 'inventory_location'
    id = Column(Integer, primary_key=True)
    location = Column(String(100), nullable=False)  # Location name, set as required
    description = Column(String(300))  # Optional description
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)
    # New accounting fields (optional)
    discount_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), index=True, nullable=True)
    payment_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), index=True, nullable=True)
    # New payment method accounts
    card_payment_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), index=True, nullable=True)
    mobile_money_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), index=True, nullable=True)
    # Workflow type configuration
    workflow_type = Column(String(20), default='process_payment')  # 'process_payment', 'order_slip'

    user_location_assignments = relationship(
        "UserLocationAssignment",
        back_populates="location"
    )

    # relationships
    discount_account = relationship(
        "ChartOfAccounts",
        foreign_keys=[discount_account_id],
        back_populates="inventory_location_discount"
    )

    payment_account = relationship(
        "ChartOfAccounts",
        foreign_keys=[payment_account_id],
        back_populates="inventory_location_payment"
    )

    # New relationships for card and mobile money accounts
    card_payment_account = relationship(
        "ChartOfAccounts",
        foreign_keys=[card_payment_account_id],
        back_populates="inventory_location_card_payment"
    )

    mobile_money_account = relationship(
        "ChartOfAccounts",
        foreign_keys=[mobile_money_account_id],
        back_populates="inventory_location_mobile_money"
    )

    # Relationship with the Company table
    project = relationship("Project", back_populates="inventory_location")
    # Separate relationships for from_location and to_location in InventoryEntry
    inventory_entries_from = relationship(
        "InventoryEntry",
        foreign_keys="[InventoryEntry.from_location]",
        back_populates="from_inventory_location"
    )
    inventory_entries_to = relationship(
        "InventoryEntry",
        foreign_keys="[InventoryEntry.to_location]",
        back_populates="to_inventory_location"
    )

    inventory_transaction_details = relationship('InventoryTransactionDetail',
                                                 back_populates='location')  # Fixed relationship

    direct_sale_items = relationship('DirectSaleItem', back_populates='location')
    sales_order_items = relationship('SalesOrderItem', back_populates='location')
    quotation_items = relationship('QuotationItem', back_populates='location')
    invoice_items = relationship('SalesInvoiceItem', back_populates='location')
    inventory_summary = relationship('InventorySummary', back_populates='location')
    direct_purchase_items = relationship('DirectPurchaseItem', back_populates='location')
    purchase_order_items = relationship('PurchaseOrderItem', back_populates='location')

    asset_transfers_from = relationship("AssetTransfer",
                                        foreign_keys="[AssetTransfer.from_location_id]",
                                        back_populates="from_location")

    asset_transfers_to = relationship("AssetTransfer",
                                      foreign_keys="[AssetTransfer.to_location_id]",
                                      back_populates="to_location")

    assets = relationship("Asset", back_populates="location")
    asset_movement_line_items_from = relationship(
        "AssetMovementLineItem",
        foreign_keys="[AssetMovementLineItem.from_location_id]",  # If you add this field
        back_populates="from_location"
    )

    asset_movement_line_items_to = relationship(
        "AssetMovementLineItem",
        foreign_keys="[AssetMovementLineItem.to_location_id]",
        back_populates="to_location"
    )


class UserLocationAssignment(Base):
    __tablename__ = 'user_location_assignment'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=False)

    # Assignment details
    role_at_location = Column(String(50), default='staff')  # 'manager', 'staff', 'viewer', 'cashier'
    permissions = Column(Text)  # JSON string for additional permissions
    is_active = Column(Boolean, default=True)
    assigned_by = Column(Integer, ForeignKey('users.id'))  # Who assigned this user
    assigned_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    user = relationship("User", foreign_keys=[user_id], back_populates="user_location_assignments")
    location = relationship("InventoryLocation", foreign_keys=[location_id], back_populates="user_location_assignments")
    assigner = relationship("User", foreign_keys=[assigned_by], back_populates="assigned_user_locations")


class InventoryEntry(Base):
    __tablename__ = 'inventory_entries'
    id = Column(Integer, primary_key=True)
    transaction_date = Column(Date, nullable=False, default=func.current_date())
    supplier_id = Column(Integer, ForeignKey("vendors.id"), nullable=True)
    date_added = Column(Date, nullable=False, default=func.current_date())
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    updated_by = Column(Integer, ForeignKey('users.id'), nullable=True)
    expiration_date = Column(Date, nullable=True)

    # Reference and tracking
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)
    reference = Column(String(100), nullable=True)
    is_posted_to_ledger = Column(Boolean, default=False)
    inventory_source = Column(String(50), nullable=True, default="purchase")
    source_type = Column(String(50), nullable=True)
    source_id = Column(Integer, nullable=True)

    # Movement type
    stock_movement = Column(String(100))  # 'in', 'out', 'transfer', etc.

    # Location information
    from_location = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)
    to_location = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)

    # Currency
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)

    # Accounting fields (header level - can be overridden at line level)
    payable_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    write_off_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    adjustment_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    sales_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)

    # NEW: Notes column for write-off reason or other comments
    notes = Column(String(100), nullable=True)
    version = Column(Integer, default=1, nullable=False)

    # Relationships
    vendor = relationship("Vendor", back_populates='inventory_entries')
    project = relationship('Project', back_populates='inventory_entries')
    currency = relationship('Currency', back_populates='inventory_entries')
    payable_account = relationship('ChartOfAccounts', foreign_keys=[payable_account_id],
                                   back_populates='inventory_entries_payable')
    write_off_account = relationship('ChartOfAccounts', foreign_keys=[write_off_account_id],
                                     back_populates='inventory_entries_write_off')
    adjustment_account = relationship('ChartOfAccounts', foreign_keys=[adjustment_account_id],
                                      back_populates='inventory_entries_adjustment')
    sales_account = relationship('ChartOfAccounts', foreign_keys=[sales_account_id],
                                 back_populates='inventory_entries_sale')
    created_user = relationship('User', back_populates='created_inventory_transactions', foreign_keys=[created_by])
    updated_user = relationship('User', back_populates='updated_inventory_transactions', foreign_keys=[updated_by])

    # Relationship to line items
    line_items = relationship("InventoryEntryLineItem", back_populates="inventory_entry",
                              cascade="all, delete-orphan")

    # Relationships for from_location and to_location with InventoryLocation table
    from_inventory_location = relationship('InventoryLocation', foreign_keys=[from_location],
                                           back_populates='inventory_entries_from')
    to_inventory_location = relationship('InventoryLocation', foreign_keys=[to_location],
                                         back_populates='inventory_entries_to')
    exchange_rate = relationship("ExchangeRate", back_populates="inventory_entry")

    # Calculated properties
    @property
    def total_quantity(self):
        return sum(item.quantity for item in self.line_items)

    @property
    def total_value(self):
        """Calculate total value as float"""
        total = 0
        for item in self.line_items:
            qty = float(item.quantity) if item.quantity is not None else 0
            price = float(item.unit_price) if item.unit_price is not None else 0
            total += qty * price
        return total

    # NEW: Property to calculate amount due
    @property
    def amount_due(self):
        return max(self.total_value - (self.amount_paid or 0), 0)


class InventoryEntryLineItem(Base):
    __tablename__ = 'inventory_entry_line_items'
    id = Column(Integer, primary_key=True)
    inventory_entry_id = Column(Integer, ForeignKey('inventory_entries.id'), nullable=False)

    # Item information
    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=False)

    # Pricing and quantities
    unit_price = Column(Numeric(10, 2), default=0.00)
    selling_price = Column(Numeric(10, 2), default=0.00)
    quantity = Column(Float, nullable=False, default=0.0)

    # Adjustment tracking
    adjustment_amount = Column(Numeric(10, 2), nullable=True)
    system_quantity = Column(Numeric(10, 2), nullable=True)
    adjusted_quantity = Column(Numeric(10, 2), nullable=True)

    # Relationships
    inventory_entry = relationship("InventoryEntry", back_populates="line_items")
    inventory_item_variation_link = relationship('InventoryItemVariationLink',
                                                 back_populates='inventory_entry_line_items')
    inventory_transaction_details = relationship('InventoryTransactionDetail',
                                                 back_populates='inventory_entry_line_item')

    # Calculated properties with null handling
    @property
    def line_total(self):
        if self.quantity is None or self.unit_price is None:
            return Decimal('0.00')
        return Decimal(str(self.quantity)) * Decimal(str(self.unit_price))

    # Add property for safe quantity access
    @property
    def safe_quantity(self):
        return self.quantity if self.quantity is not None else 0.0

    # Add property for safe unit_price access
    @property
    def safe_unit_price(self):
        return self.unit_price if self.unit_price is not None else Decimal('0.00')


class InventoryTransactionDetail(Base):
    __tablename__ = 'inventory_transaction_details'

    id = Column(Integer, primary_key=True)

    # Item information
    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=False)

    # Location information
    location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)

    # References
    vendor_id = Column(Integer, ForeignKey("vendors.id"), nullable=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)

    # Quantity information
    quantity = Column(Float, nullable=False, default=0.0)

    # Movement type (in, out, transfer, adjustment, missing, expired, damaged)
    movement_type = Column(String(20), nullable=False)

    transaction_date = Column(Date, nullable=False, default=func.current_date())

    # Cost information
    unit_cost = Column(Numeric(10, 2), nullable=False)  # Cost per unit at time of transaction
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)
    total_cost = Column(Numeric(12, 2), nullable=False)  # quantity * unit_cost

    # References

    # Link to the specific line item in the inventory entry
    inventory_entry_line_item_id = Column(Integer, ForeignKey('inventory_entry_line_items.id'), nullable=False)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, default=func.current_timestamp(), onupdate=func.current_timestamp())

    # 🆕 NEW: Posted to ledger flag
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)

    # Relationships
    inventory_item_variation_link = relationship('InventoryItemVariationLink',
                                                 back_populates='inventory_transaction_details')
    location = relationship('InventoryLocation', back_populates='inventory_transaction_details')
    currency = relationship('Currency', back_populates='inventory_transaction_details')
    inventory_entry_line_item = relationship("InventoryEntryLineItem", back_populates="inventory_transaction_details")
    exchange_rate = relationship("ExchangeRate", back_populates="inventory_transaction_details")
    vendor = relationship("Vendor", back_populates="inventory_transaction_details")
    project = relationship("Project", back_populates="inventory_transaction_details")

    # Indexes for better performance
    __table_args__ = (
        Index('ix_transaction_details_item_location', 'item_id', 'location_id'),
        Index('ix_transaction_details_date_type', 'transaction_date', 'movement_type'),
        Index('ix_transaction_details_entry_line_item', 'inventory_entry_line_item_id'),
    )


# The InventorySummary table!
class InventorySummary(Base):
    __tablename__ = 'inventory_summary'
    id = Column(Integer, primary_key=True)



    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=False)
    location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=False)
    total_quantity = Column(Float, default=0.0)
    total_value = Column(Numeric(15, 4), default=0.0)
    average_cost = Column(Numeric(15, 4), default=0.0)
    last_updated = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationships
    inventory_item_variation = relationship('InventoryItemVariationLink', back_populates='inventory_summary')
    location = relationship('InventoryLocation', back_populates='inventory_summary')

    __table_args__ = (
        UniqueConstraint('item_id', 'location_id', name='uq_inventory_summary'),
        Index('ix_summary_app_item', 'item_id'),
        Index('ix_summary_app_location', 'location_id'),
        Index('ix_summary_app_item_location', 'item_id', 'location_id'),
    )


# Enum for order status
class OrderStatus(enum.Enum):
    draft = "Draft"
    pending = "Pending"
    approved = "Approved"
    received = "Received"
    sent = "Sent"
    closed = "Closed"
    canceled = "Canceled"
    progress = "In Progress"
    invoiced = "Invoiced"
    delivered = "Delivered"
    returned = "Returned"
    partially_received = "Partially Received"
    partially_paid = "Partially Paid"
    awaiting_payment = "Awaiting Payment"
    paid = "Paid"
    rejected = "Rejected"
    unpaid = "Unpaid"
    expired = "Expired"


# Purchase Order model
# Purchase Order Table
class PurchaseOrder(Base):
    __tablename__ = 'purchase_orders'
    id = Column(Integer, primary_key=True)
    purchase_order_number = Column(String(100), nullable=False)
    purchase_order_reference = Column(String(255), nullable=True)  # New column
    purchase_order_date = Column(Date, nullable=False, default=func.current_date())
    delivery_date = Column(Date, nullable=False, default=func.current_date())
    shipping_address = Column(String(255), nullable=True)  # Or use a foreign key to an Address table
    delivery_method = Column(String(50), nullable=True)
    vendor_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    total_line_subtotal = Column(Numeric(18, 2), nullable=False)
    total_amount = Column(Numeric(10, 2), nullable=False)
    purchase_order_discount_type = Column(Enum('amount', 'percentage'), default="amount")
    purchase_order_discount_value = Column(Numeric(15, 2), default=0.0)
    calculated_discount_amount = Column(Numeric(15, 2), default=0.0)
    total_tax_amount = Column(Numeric(18, 2), default=0.0)
    purchase_order_tax_rate = Column(Numeric(10, 2), nullable=True)
    shipping_cost = Column(Numeric(10, 2), nullable=True, default=0.0)
    handling_cost = Column(Numeric(10, 2), nullable=True, default=0.0)
    shipping_handling_posted = Column(Boolean, default=False, nullable=False)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)

    # New columns for category totals
    inventory_total = Column(Numeric(18, 2), nullable=False, default=0.0)
    non_inventory_total = Column(Numeric(18, 2), nullable=False, default=0.0)
    expenses_total = Column(Numeric(18, 2), nullable=False, default=0.0)

    # Accounts Payable relationship
    accounts_payable_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    preferred_prepaid_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    service_expense_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    shipping_handling_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    non_inventory_expense_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)

    currency = Column(Integer, ForeignKey('currency.id'))
    status = Column(Enum(OrderStatus), nullable=False, default=OrderStatus.draft)
    terms_and_conditions = Column(Text, nullable=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())
    version = Column(Integer, default=1, nullable=False)

    vendor = relationship('Vendor', back_populates='purchase_orders')
    purchase_order_items = relationship('PurchaseOrderItem', back_populates='purchase_orders')
    purchase_order_history = relationship('PurchaseOrderHistory', back_populates='purchase_orders')
    purchase_order_status_logs = relationship('PurchaseOrderStatusLog', back_populates='purchase_orders')
    currencies = relationship('Currency', back_populates='purchase_orders')
    purchase_returns = relationship('PurchaseReturn', back_populates='purchase_orders')
    purchase_order_notes = relationship('PurchaseOrderNote', back_populates='purchase_orders')
    purchase_order_approvals = relationship('PurchaseOrderApproval', back_populates='purchase_orders')
    purchase_transactions = relationship('PurchaseTransaction', back_populates='purchase_orders')
    goods_receipts = relationship('GoodsReceipt', back_populates='purchase_order')
    project = relationship('Project', back_populates='purchase_orders')

    # Accounts Payable relationship
    accounts_payable = relationship('ChartOfAccounts', foreign_keys=[accounts_payable_id],
                                    back_populates='purchase_orders_ap')

    # NEW: Expense Account Relationships
    service_expense_account = relationship('ChartOfAccounts', foreign_keys=[service_expense_account_id],
                                           back_populates='purchase_orders_service_expense')
    shipping_handling_account = relationship('ChartOfAccounts', foreign_keys=[shipping_handling_account_id],
                                             back_populates='purchase_orders_shipping')
    non_inventory_expense_account = relationship('ChartOfAccounts', foreign_keys=[non_inventory_expense_account_id],
                                                 back_populates='purchase_orders_non_inventory')
    preferred_prepaid_account = relationship('ChartOfAccounts', foreign_keys=[preferred_prepaid_account_id],
                                             back_populates='purchase_order_prepaid')

    exchange_rate = relationship('ExchangeRate', back_populates='purchase_orders')
    user = relationship("User", back_populates="purchase_orders", foreign_keys=[created_by])

    __table_args__ = (

        Index('idx_purchase_orders_exchange_rate', 'exchange_rate_id'),
        Index('idx_purchase_orders_date', 'purchase_order_date'),
        Index('idx_purchase_orders_vendor', 'vendor_id'),
        Index('idx_purchase_orders_status', 'status'),
        Index('idx_purchase_orders_currency', 'currency'),
        Index('idx_purchase_orders_project', 'project_id'),
        Index('idx_purchase_orders_created_by', 'created_by'),
        Index('idx_purchase_orders_app_date', 'purchase_order_date'),
        Index('idx_purchase_orders_vendor_status', 'vendor_id', 'status'),
        Index('idx_purchase_orders_currency_date', 'currency', 'purchase_order_date'),
    )


# Purchase Order Item Table
class PurchaseOrderItem(Base):
    __tablename__ = 'purchase_order_items'
    id = Column(Integer, primary_key=True)
    purchase_order_id = Column(Integer, ForeignKey('purchase_orders.id'), nullable=False)
    item_type = Column(String(50), nullable=False)  # 'inventory', 'service', or 'non_inventory'
    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=True)
    item_name = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    quantity = Column(Numeric(15, 2), nullable=False)
    currency = Column(Integer, ForeignKey('currency.id'))
    unit_price = Column(Numeric(15, 2), nullable=False)
    total_price = Column(Numeric(18, 2), nullable=False)

    # NEW: Landed cost columns (calculated when PO is converted to actual purchase)
    unit_cost = Column(Numeric(15, 4), nullable=True)  # True cost per unit including allocations
    total_cost = Column(Numeric(18, 2), nullable=True)  # True total cost including allocations

    uom = Column(Integer, ForeignKey('unit_of_measurement.id'), nullable=False)
    tax_rate = Column(Numeric(10, 2), nullable=True)
    tax_amount = Column(Numeric(10, 2), nullable=True)
    discount_amount = Column(Float, nullable=True)
    discount_rate = Column(Numeric(15, 2), nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())
    location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)

    purchase_orders = relationship('PurchaseOrder', back_populates='purchase_order_items')
    inventory_item_variation_link = relationship('InventoryItemVariationLink', back_populates='purchase_order_items')
    currencies = relationship('Currency', back_populates='purchase_order_items')
    unit_of_measurement = relationship("UnitOfMeasurement", back_populates="purchase_order_items")
    goods_receipt_items = relationship("GoodsReceiptItem", back_populates="purchase_order_item")
    location = relationship('InventoryLocation', back_populates='purchase_order_items')


# Purchase Order Status Log Table
class PurchaseOrderStatusLog(Base):
    __tablename__ = 'purchase_order_status_log'
    id = Column(Integer, primary_key=True)
    purchase_order_id = Column(Integer, ForeignKey('purchase_orders.id'), nullable=False)
    status = Column(String(50), nullable=False)
    changed_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    change_date = Column(DateTime, nullable=False, default=func.now())

    purchase_orders = relationship('PurchaseOrder', back_populates='purchase_order_status_logs')
    user = relationship('User', back_populates='purchase_order_status_logs')


# Purchase Order History Table
class PurchaseOrderHistory(Base):
    __tablename__ = 'purchase_order_history'
    id = Column(Integer, primary_key=True)
    purchase_order_id = Column(Integer, ForeignKey('purchase_orders.id'), nullable=False)
    changed_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    change_date = Column(DateTime, nullable=False, default=func.now())
    change_description = Column(Text, nullable=False)

    purchase_orders = relationship('PurchaseOrder', back_populates='purchase_order_history')
    user = relationship('User', back_populates='purchase_order_history')


class PurchaseOrderNote(Base):
    __tablename__ = 'purchase_order_notes'
    id = Column(Integer, primary_key=True)
    purchase_order_id = Column(Integer, ForeignKey('purchase_orders.id'), nullable=False)
    note_type = Column(String(50), nullable=False)  # 'internal' or 'vendor'
    note_content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    recipient = Column(Integer, ForeignKey('users.id'), nullable=True)

    purchase_orders = relationship('PurchaseOrder', back_populates='purchase_order_notes')
    user = relationship("User", back_populates="purchase_order_notes", foreign_keys=[created_by])
    user_recipient = relationship("User", back_populates="purchase_order_notes_recipient", foreign_keys=[recipient])


# Purchase Order Approval Table
class PurchaseOrderApproval(Base):
    __tablename__ = 'purchase_order_approvals'
    id = Column(Integer, primary_key=True)
    purchase_order_id = Column(Integer, ForeignKey('purchase_orders.id'), nullable=False)
    approver_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    approval_status = Column(String(50), nullable=False)  # e.g., pending, approved, rejected
    approval_date = Column(DateTime, nullable=True)
    comments = Column(Text, nullable=True)

    purchase_orders = relationship('PurchaseOrder', back_populates='purchase_order_approvals')
    approver = relationship('User', back_populates='purchase_order_approvals')


# Purchase Return Table
class PurchaseReturn(Base):
    __tablename__ = 'purchase_returns'
    id = Column(Integer, primary_key=True)
    return_number = Column(String(100), nullable=False)
    quantity = Column(Numeric(15, 2), nullable=False)
    allocated_amount = Column(Numeric(15, 2), nullable=False)
    allocated_tax_amount = Column(Numeric(15, 2), nullable=False)
    inventory_reversed = Column(Boolean, default=False, nullable=False)
    purchase_order_id = Column(Integer, ForeignKey('purchase_orders.id'), nullable=True)
    direct_purchase_id = Column(Integer, ForeignKey('direct_purchases.id'), nullable=True)
    receipt_item_id = Column(Integer, ForeignKey('goods_receipt_items.id'), nullable=False)
    vendor_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    return_date = Column(Date, nullable=False, default=func.current_date())
    reason = Column(String(200), nullable=True)
    status = Column(Enum('draft', 'pending', 'approved', 'completed', name='return_status'),
                    default='draft', nullable=False)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)

    # Relationships
    purchase_orders = relationship('PurchaseOrder', back_populates='purchase_returns')
    direct_purchases = relationship('DirectPurchaseTransaction', back_populates='purchase_returns')
    vendor = relationship('Vendor', back_populates='purchase_returns')
    receipt_items = relationship('GoodsReceiptItem', back_populates='purchase_returns')
    user = relationship('User', back_populates='purchase_returns')



class PurchasePaymentStatus(enum.Enum):
    unpaid = 'unpaid'
    full = 'full'
    partial = 'partial'
    refund = 'refund'
    paid = 'paid'
    cancelled = 'cancelled'
    on_credit = 'on_credit'  # Additional status specific to purchases


# Purchase Transaction Table (Payments Made)
class PurchaseTransaction(Base):
    __tablename__ = 'purchase_transactions'
    id = Column(Integer, primary_key=True)
    purchase_order_id = Column(Integer, ForeignKey('purchase_orders.id'), nullable=False)
    vendor_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    payment_date = Column(Date, nullable=False, default=func.current_date())
    amount_paid = Column(Numeric(18, 2), nullable=False)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)

    reference_number = Column(String(100), nullable=True)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    payment_status = Column(Enum(PurchasePaymentStatus), nullable=False, default=PurchasePaymentStatus.unpaid)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    version = Column(Integer, default=1, nullable=False)

    purchase_orders = relationship('PurchaseOrder', back_populates='purchase_transactions')
    vendor = relationship('Vendor', back_populates='purchase_transactions')
    currency = relationship("Currency", back_populates='purchase_transactions')
    payment_allocations = relationship('PurchasePaymentAllocation', back_populates='purchase_transactions')
    user = relationship('User', back_populates='purchase_transactions')

    __table_args__ = (
     # 1. Most critical - company + date range (used in dashboard and reports)
        Index('idx_purchase_transactions_app_date', 'payment_date'),

        # 2. Filtering by company + status (used in dashboard filters)
        Index('idx_purchase_transactions_app_status', 'payment_status'),

        # 3. Foreign key - used in JOINs with purchase_orders
        Index('idx_purchase_transactions_po_id', 'purchase_order_id'),

        # 4. Foreign key - used in JOINs with vendors (filtering by vendor)
        Index('idx_purchase_transactions_vendor_id', 'vendor_id'),
    )

class DirectPurchaseTransaction(Base):
    __tablename__ = 'direct_purchases'
    id = Column(Integer, primary_key=True)
    vendor_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    payment_date = Column(Date, nullable=False, default=func.current_date())
    total_amount = Column(Numeric(18, 2), nullable=False, default=0.0)
    amount_paid = Column(Numeric(18, 2), nullable=False, default=0.0)
    total_line_subtotal = Column(Numeric(18, 2), nullable=False, default=0.0)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)

    # New columns for category totals
    inventory_total = Column(Numeric(18, 2), nullable=False, default=0.0)
    non_inventory_total = Column(Numeric(18, 2), nullable=False, default=0.0)
    expenses_total = Column(Numeric(18, 2), nullable=False, default=0.0)

    is_complete_receipt = Column(Boolean, default=False, nullable=False)

    purchase_reference = Column(String(100), nullable=True)
    purchase_tax_rate = Column(Numeric(15, 2), nullable=True, default=0.0)
    shipping_cost = Column(Numeric(15, 2), nullable=True, default=0.0)
    handling_cost = Column(Numeric(15, 2), nullable=True, default=0.0)
    purchase_discount_type = Column(Enum('amount', 'percentage'), default="amount")
    purchase_discount_value = Column(Numeric(15, 2), default=0.0)
    terms_and_conditions = Column(Text, nullable=True)
    calculated_discount_amount = Column(Numeric(15, 2), default=0.0)
    total_tax_amount = Column(Numeric(18, 2), default=0.0)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)

    # Add these shipping-related fields:
    shipping_address = Column(String(255), nullable=True)  # Could also be a foreign key to an Address table
    delivery_method = Column(String(50), nullable=True)  # e.g., "Ground", "Air", "Sea"
    expected_delivery_date = Column(Date, nullable=True)

    direct_purchase_number = Column(String(50), nullable=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)
    status = Column(Enum(OrderStatus), nullable=False, default=OrderStatus.draft)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    payment_status = Column(String(20), nullable=False, default='full')  # Values: 'full', 'partial', 'refund'
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    version = Column(Integer, default=1, nullable=False)

    vendor = relationship('Vendor', back_populates='direct_purchases')
    user = relationship('User', back_populates='direct_purchases')
    currency = relationship("Currency", back_populates='direct_purchases')
    goods_receipts = relationship("GoodsReceipt", back_populates='direct_purchase')

    shipping_handling_posted = Column(Boolean, default=False, nullable=False)

    payment_allocations = relationship('PurchasePaymentAllocation', back_populates='direct_purchases')
    direct_purchase_items = relationship('DirectPurchaseItem', back_populates='direct_purchases')
    purchase_returns = relationship('PurchaseReturn', back_populates='direct_purchases')
    project = relationship('Project', back_populates='direct_purchases')


class DirectPurchaseItem(Base):
    __tablename__ = 'direct_purchase_items'
    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(Integer, ForeignKey('direct_purchases.id'), nullable=False)
    item_type = Column(String(50), nullable=False)  # 'inventory', 'service', or 'non_inventory'
    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=True)
    item_name = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    quantity = Column(Numeric(15, 2), nullable=False)
    uom = Column(Integer, ForeignKey('unit_of_measurement.id'), nullable=False)
    currency = Column(Integer, ForeignKey('currency.id'))
    unit_price = Column(Numeric(15, 2), nullable=False)
    total_price = Column(Numeric(15, 2), nullable=False)

    # NEW: Calculated unit cost including discounts and taxes
    unit_cost = Column(Numeric(15, 2), nullable=True)  # True cost per unit
    total_cost = Column(Numeric(15, 2), nullable=True)  # True total cost

    tax_rate = Column(Numeric(15, 2), nullable=True)
    tax_amount = Column(Numeric(15, 2), nullable=True)
    discount_amount = Column(Numeric(15, 2), nullable=True)
    discount_rate = Column(Numeric(15, 2), nullable=True)

    # 👇 New field
    location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)


    direct_purchases = relationship('DirectPurchaseTransaction', back_populates='direct_purchase_items')
    inventory_item_variation_link = relationship('InventoryItemVariationLink', back_populates='direct_purchase_items')

    unit_of_measurement = relationship('UnitOfMeasurement', back_populates='direct_purchase_items')
    currencies = relationship('Currency', back_populates='direct_purchase_items')
    goods_receipt_items = relationship('GoodsReceiptItem', back_populates='direct_purchase_item')
    # 👇 Optional relationship (add this if you have a Location model)
    location = relationship('InventoryLocation', back_populates='direct_purchase_items')


class PurchasePaymentAllocation(Base):
    __tablename__ = 'purchase_payment_allocations'
    id = Column(Integer, primary_key=True)
    payment_id = Column(Integer, ForeignKey('purchase_transactions.id'), nullable=True)
    payment_date = Column(Date, nullable=False, default=func.current_date())

    direct_purchase_id = Column(Integer, ForeignKey('direct_purchases.id'), nullable=True)

    # Allocation amounts (split by type)
    allocated_inventory = Column(Numeric(18, 2), default=0.0)  # Inventory items
    allocated_non_inventory = Column(Numeric(18, 2), default=0.0)  # Non Inventory
    allocated_services = Column(Numeric(18, 2), default=0.0)  # Services
    allocated_other_expenses = Column(Numeric(18, 2), default=0.0)  # Shipping/handling
    allocated_tax_receivable = Column(Numeric(18, 2), default=0.0)
    allocated_tax_payable = Column(Numeric(18, 2), default=0.0)

    # Account references
    inventory_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'))
    non_inventory_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'))
    other_expense_service_id = Column(Integer, ForeignKey('chart_of_accounts.id'))  # Shipping and Handling
    other_expense_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'))  # Shipping and Handling

    tax_receivable_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'))
    tax_payable_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'))

    payment_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'))  # Asset cash equivalent
    credit_purchase_account = Column(Integer, ForeignKey('chart_of_accounts.id'))
    prepaid_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'))  # Explicit prepaid account

    # Control fields
    payment_mode = Column(Integer, ForeignKey('payment_modes.id'), nullable=True)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)
    payment_type = Column(Enum('advance_payment', 'regular_payment', 'direct_purchase', name='transaction_type_enum'),
                          nullable=False)

    reference = Column(String(100), nullable=True)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)

    purchase_transactions = relationship('PurchaseTransaction', back_populates='payment_allocations')
    direct_purchases = relationship('DirectPurchaseTransaction', back_populates='payment_allocations')
    payment_modes = relationship('PaymentMode', back_populates='purchase_payment_allocations')

    inventory_account = relationship("ChartOfAccounts", back_populates="purchase_payment_inventory_account",
                                     foreign_keys=[inventory_account_id])
    non_inventory_account = relationship("ChartOfAccounts", back_populates="purchase_payment_non_inventory_account",
                                         foreign_keys=[non_inventory_account_id])
    chart_of_accounts_asset = relationship("ChartOfAccounts", back_populates="purchase_payment_allocations_asset",
                                           foreign_keys=[payment_account_id])
    chart_of_accounts_tax_receivable = relationship("ChartOfAccounts",
                                                    back_populates="purchase_payment_allocations_tax_receivable",
                                                    foreign_keys=[tax_receivable_account_id])
    chart_of_accounts_tax_payable = relationship("ChartOfAccounts",
                                                 back_populates="purchase_payment_allocations_tax_payable",
                                                 foreign_keys=[tax_payable_account_id])
    chart_of_accounts_payable = relationship("ChartOfAccounts", back_populates="purchase_payment_allocations_payable",
                                             foreign_keys=[credit_purchase_account])
    chart_of_accounts_other_expense = relationship("ChartOfAccounts",
                                                   back_populates="purchase_payment_allocations_other_expense",
                                                   foreign_keys=[other_expense_account_id])
    chart_of_accounts_service_expense = relationship("ChartOfAccounts",
                                                     back_populates="purchase_payment_allocations_service_expense",
                                                     foreign_keys=[other_expense_service_id])

    chart_of_accounts_prepaid = relationship("ChartOfAccounts",
                                             back_populates="purchase_payment_allocations_prepaid",
                                             foreign_keys=[prepaid_account_id])
    exchange_rate = relationship("ExchangeRate", back_populates="purchase_transactions")

    __table_args__ = (
        # Index for the exchange rate foreign key
        Index('idx_purchase_payment_allocations_exchange_rate', 'exchange_rate_id'),
        Index('idx_purchase_payment_allocations_payment_id', 'payment_id'),
        Index('idx_purchase_payment_allocations_direct_purchase', 'direct_purchase_id'),
    )
class GoodsReceipt(Base):
    __tablename__ = 'goods_receipts'
    id = Column(Integer, primary_key=True)
    receipt_number = Column(String(100), nullable=False)
    purchase_order_id = Column(Integer, ForeignKey('purchase_orders.id'), nullable=True)
    direct_purchase_id = Column(Integer, ForeignKey('direct_purchases.id'), nullable=True)
    receipt_date = Column(Date, nullable=False, default=func.current_date())
    received_by = Column(String(100), nullable=False)
    quantity_received = Column(Numeric(15, 2), nullable=False)
    received_condition = Column(String(50), nullable=True)
    is_complete_receipt = Column(Boolean, default=False, nullable=False)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Inventory impact
    inventory_received = Column(Boolean, default=False)
    inventory_posted = Column(Boolean, default=False)

    version = Column(Integer, default=1, nullable=False)


    purchase_order = relationship('PurchaseOrder', back_populates='goods_receipts')
    direct_purchase = relationship('DirectPurchaseTransaction', back_populates='goods_receipts')
    receipt_items = relationship('GoodsReceiptItem', back_populates='goods_receipt')


# Goods Receipt Item Model
class GoodsReceiptItem(Base):
    __tablename__ = 'goods_receipt_items'
    id = Column(Integer, primary_key=True)
    goods_receipt_id = Column(Integer, ForeignKey('goods_receipts.id'), nullable=False)
    purchase_order_item_id = Column(Integer, ForeignKey('purchase_order_items.id'), nullable=True)
    direct_purchase_item_id = Column(Integer, ForeignKey('direct_purchase_items.id'), nullable=True)
    quantity_received = Column(Numeric(15, 2), nullable=False)
    allocated_amount = Column(Numeric(15, 2), nullable=True)  # Tracks how much payment is allocated to this receipt
    allocated_tax_amount = Column(Numeric(15, 2), nullable=True)  # New column for allocated tax portion
    # NEW: Single column for combined shipping and handling costs
    allocated_shipping_handling = Column(Numeric(15, 2), nullable=True)  # Sum of allocated shipping + handling costs

    # NEW: Landed cost columns for goods receipt
    unit_cost = Column(Numeric(15, 4), nullable=True)  # Actual cost per unit including allocations
    total_cost = Column(Numeric(18, 2), nullable=True)  # Actual total cost including allocations

    received_condition = Column(Enum('Good', 'Damaged', 'Partial', name='receipt_condition'), default='Good')
    notes = Column(Text, nullable=True)
    inventory_adjusted = Column(Boolean, default=False, nullable=False)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)
    # Relationships
    goods_receipt = relationship('GoodsReceipt', back_populates='receipt_items')
    purchase_order_item = relationship('PurchaseOrderItem', back_populates='goods_receipt_items')
    direct_purchase_item = relationship('DirectPurchaseItem', back_populates='goods_receipt_items')
    purchase_returns = relationship('PurchaseReturn', back_populates='receipt_items')


# Enum for Quotation Status
class QuotationStatus(enum.Enum):
    draft = "Draft"
    approved = "Approved"
    accepted = "Accepted"
    rejected = "Rejected"
    expired = "Expired"
    cancelled = "Cancelled"


# Enum for Invoice Status
class InvoiceStatus(enum.Enum):
    draft = "Draft"
    unpaid = "Unpaid"
    partially_paid = "Partially Paid"
    paid = "Paid"
    canceled = "Canceled"


# Enum for Delivery Note Status
class DeliveryStatus(enum.Enum):
    draft = "Draft"
    partly_delivered = "Partly Delivered"
    approved = "Approved"
    pending = "Pending"
    delivered = "Delivered"
    canceled = "Canceled"


# Quotation Note Table
class QuotationNote(Base):
    __tablename__ = 'quotation_notes'
    id = Column(Integer, primary_key=True)
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=False)
    note_type = Column(String(50), nullable=False)  # 'internal' or 'customer'
    note_content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    recipient = Column(Integer, ForeignKey('users.id'), nullable=True)

    quotation = relationship('Quotation', back_populates='quotation_notes')
    # Add quotation relationships to the Company model
    user = relationship("User", back_populates="quotation_notes", foreign_keys=[created_by])
    user_recipient = relationship("User", back_populates="quotation_notes_recipient", foreign_keys=[recipient])


# Quotation Attachment Table
class QuotationAttachment(Base):
    __tablename__ = 'quotation_attachments'
    id = Column(Integer, primary_key=True)
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=False)
    file_name = Column(String(255), nullable=False)
    file_location = Column(String(255), nullable=False)
    uploaded_at = Column(DateTime, default=func.now(), nullable=False)

    quotation = relationship('Quotation', back_populates='quotation_attachments')


# Quotation Table
class Quotation(Base):
    __tablename__ = 'quotations'
    id = Column(Integer, primary_key=True)
    quotation_number = Column(String(100), nullable=False)
    quotation_reference = Column(String(100), nullable=True)  # ✅ ADDED THIS FIELD
    project_id = Column(Integer, ForeignKey('projects.id'))
    quotation_date = Column(Date, nullable=False, default=func.current_date())
    expiry_date = Column(Date, nullable=True)
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    total_line_subtotal = Column(Numeric(10, 2), nullable=False)  # Changed to Numeric
    total_amount = Column(Numeric(18, 2), nullable=False)  # Changed to Numeric
    quotation_discount_type = Column(Enum('amount', 'percentage'), default="amount")
    quotation_discount_value = Column(Numeric(10, 2), default=0.0)  # Changed to Numeric
    calculated_discount_amount = Column(Numeric(18, 2), default=0.0)  # Changed to Numeric
    quotation_tax_rate = Column(Numeric(4, 2), nullable=True)  # Changed to Numeric
    total_tax_amount = Column(Numeric(18, 2), default=0.0)  # Changed to Numeric
    shipping_cost = Column(Numeric(10, 2), nullable=True, default=0.0)  # Changed to Numeric
    handling_cost = Column(Numeric(10, 2), nullable=True, default=0.0)  # Changed to Numeric
    currency = Column(Integer, ForeignKey('currency.id'))
    status = Column(Enum(QuotationStatus), nullable=False, default=QuotationStatus.draft)
    terms_and_conditions = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())

    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)

    version = Column(Integer, default=1, nullable=False)

    customer = relationship('Vendor', back_populates='quotations')
    project = relationship('Project', back_populates='quotations')
    quotation_items = relationship('QuotationItem', back_populates='quotation')
    quotation_notes = relationship('QuotationNote', back_populates='quotation')
    quotation_attachments = relationship('QuotationAttachment', back_populates='quotation')
    approvals = relationship('QuotationApproval', back_populates='quotation')
    history = relationship('QuotationHistory', back_populates='quotation')
    status_logs = relationship('QuotationStatusLog', back_populates='quotation')
    currencies = relationship('Currency', back_populates='quotation')
    sales_orders = relationship('SalesOrder', back_populates='quotation')
    delivery_reference_number = relationship('DeliveryReferenceNumber', back_populates='quotations')
    invoices = relationship('SalesInvoice', back_populates='quotation')
    delivery_notes = relationship('DeliveryNote', back_populates='quotation')
    payment_receipts = relationship('PaymentReceipt', back_populates='quotation')
    exchange_rate = relationship('ExchangeRate', back_populates='quotations')
    __table_args__ = (

        Index('idx_quotations_exchange_rate', 'exchange_rate_id'),
    )


class QuotationItem(Base):
    __tablename__ = 'quotation_items'
    id = Column(Integer, primary_key=True)
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=False)
    item_type = Column(String(50), nullable=False)  # 'inventory', 'service', or 'non_inventory'
    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=True)  # Generic item ID
    item_name = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    quantity = Column(Numeric(10, 2), nullable=False)  # Changed to Numeric
    currency = Column(Integer, ForeignKey('currency.id'))
    unit_price = Column(Numeric(10, 2), nullable=False)  # Changed to Numeric
    total_price = Column(Numeric(15, 2), nullable=False)  # Changed to Numeric
    uom = Column(Integer, ForeignKey('unit_of_measurement.id'), nullable=False)
    tax_rate = Column(Numeric(10, 2), nullable=True)  # Changed to Numeric
    tax_amount = Column(Numeric(10, 2), nullable=True)  # Changed to Numeric
    discount_amount = Column(Numeric(10, 2), nullable=True)  # Changed to Numeric
    discount_rate = Column(Numeric(10, 2), nullable=True)  # Changed to Numeric
    location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())

    # Relationships
    quotation = relationship('Quotation', back_populates='quotation_items')
    inventory_item_variation_link = relationship('InventoryItemVariationLink', back_populates='quotation_items')
    currencies = relationship('Currency', back_populates='quotation_items')
    unit_of_measurement = relationship("UnitOfMeasurement", back_populates="quotation_items")
    location = relationship('InventoryLocation', back_populates='quotation_items')


# Quotation Approval Table
class QuotationApproval(Base):
    __tablename__ = 'quotation_approvals'
    id = Column(Integer, primary_key=True)
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=False)
    approver_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    approval_status = Column(String(50), nullable=False)  # e.g., pending, approved, rejected
    approval_date = Column(DateTime, nullable=True)
    comments = Column(Text, nullable=True)

    quotation = relationship('Quotation', back_populates='approvals')
    approver = relationship('User', back_populates='quotation_approvals')


# Quotation History Table
class QuotationHistory(Base):
    __tablename__ = 'quotation_history'
    id = Column(Integer, primary_key=True)
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=False)
    changed_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    change_date = Column(DateTime, nullable=False, default=func.now())
    change_description = Column(Text, nullable=False)

    quotation = relationship('Quotation', back_populates='history')
    user = relationship('User', back_populates='quotation_history')


# Quotation Status Log Table
class QuotationStatusLog(Base):
    __tablename__ = 'quotation_status_log'
    id = Column(Integer, primary_key=True)
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=False)
    status = Column(String(50), nullable=False)  # e.g., draft, sent, accepted
    changed_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    change_date = Column(DateTime, nullable=False, default=func.now())

    quotation = relationship('Quotation', back_populates='status_logs')
    user = relationship('User', back_populates='quotation_status_logs')


# Sales Order Table

class SalesOrder(Base):
    __tablename__ = 'sales_orders'
    id = Column(Integer, primary_key=True)
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=True)
    sales_order_number = Column(String(100), nullable=False)
    sales_order_reference = Column(String(100), nullable=True)  # ✅ ADDED THIS FIELD
    project_id = Column(Integer, ForeignKey('projects.id'))
    sales_order_date = Column(Date, nullable=False, default=func.current_date())
    expiry_date = Column(Date, nullable=True)
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    total_line_subtotal = Column(Numeric(18, 2), nullable=False)  # Changed to Numeric
    total_amount = Column(Numeric(10, 2), nullable=False)  # Changed to Numeric
    sales_order_discount_type = Column(Enum('amount', 'percentage'), default="amount")
    sales_order_discount_value = Column(Numeric(15, 2), default=0.0)  # Changed to Numeric
    calculated_discount_amount = Column(Numeric(15, 2), default=0.0)  # Changed to Numeric
    total_tax_amount = Column(Numeric(18, 2), default=0.0)  # Changed to Numeric
    sales_order_tax_rate = Column(Numeric(10, 2), nullable=True)  # Changed to Numeric
    shipping_cost = Column(Numeric(10, 2), nullable=True, default=0.0)  # Changed to Numeric
    handling_cost = Column(Numeric(10, 2), nullable=True, default=0.0)  # Changed to Numeric
    currency = Column(Integer, ForeignKey('currency.id'))
    status = Column(Enum(OrderStatus), nullable=False, default=OrderStatus.draft)
    terms_and_conditions = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())

    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)
    version = Column(Integer, default=1, nullable=False)


    customer = relationship('Vendor', back_populates='sales_orders')
    project = relationship('Project', back_populates='sales_orders')
    sales_order_items = relationship('SalesOrderItem', back_populates='sales_orders')
    sales_order_history = relationship('SalesOrderHistory', back_populates='sales_orders')
    sales_order_status_logs = relationship('SalesOrderStatusLog', back_populates='sales_orders')
    currencies = relationship('Currency', back_populates='sales_orders')
    invoices = relationship('SalesInvoice', back_populates='sales_orders')
    delivery_reference_number = relationship('DeliveryReferenceNumber', back_populates='sales_orders')
    sales_returns = relationship('SalesReturn', back_populates='sales_orders')
    sales_order_notes = relationship('SalesOrderNote', back_populates='sales_orders')
    sales_order_attachments = relationship('SalesOrderAttachment', back_populates='sales_orders')
    sales_order_approvals = relationship('SalesOrderApproval', back_populates='sales_orders')
    quotation = relationship('Quotation', back_populates='sales_orders')
    delivery_notes = relationship('DeliveryNote', back_populates='sales_order')
    payment_receipts = relationship('PaymentReceipt', back_populates='sales_order')
    exchange_rate = relationship('ExchangeRate', back_populates='sales_orders')  # ✅ ADD THIS

    __table_args__ = (
        Index('idx_sales_orders_exchange_rate', 'exchange_rate_id'),  # ✅ ADD THIS
    )

# Sales Order Item Table
class SalesOrderItem(Base):
    __tablename__ = 'sales_order_items'
    id = Column(Integer, primary_key=True)
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=False)
    item_type = Column(String(50), nullable=False)  # 'inventory', 'service', or 'non_inventory'
    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=True)  # Generic item ID
    item_name = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    quantity = Column(Numeric(15, 2), nullable=False)
    currency = Column(Integer, ForeignKey('currency.id'))
    unit_price = Column(Numeric(15, 2), nullable=False)
    total_price = Column(Numeric(18, 2), nullable=False)  # Calculated as quantity * unit_price - discounts
    uom = Column(Integer, ForeignKey('unit_of_measurement.id'), nullable=False)
    tax_rate = Column(Numeric(10, 2), nullable=True)  # Tax percentage, e.g., 18.0 for 18%
    tax_amount = Column(Numeric(10, 2), nullable=True)
    discount_amount = Column(Float, nullable=True)
    discount_rate = Column(Numeric(15, 2), nullable=True)  # Discount percentage
    location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())

    # Relationships
    sales_orders = relationship('SalesOrder', back_populates='sales_order_items')
    inventory_item_variation_link = relationship('InventoryItemVariationLink', back_populates='sales_order_items')
    currencies = relationship('Currency', back_populates='sales_order_items')
    unit_of_measurement = relationship("UnitOfMeasurement", back_populates="sales_order_items")
    location = relationship('InventoryLocation', back_populates='sales_order_items')


# Sales Order Status Log Table
class SalesOrderStatusLog(Base):
    __tablename__ = 'sales_order_status_log'
    id = Column(Integer, primary_key=True)
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=False)
    status = Column(String(50), nullable=False)  # e.g., draft, pending, completed, cancelled
    changed_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    change_date = Column(DateTime, nullable=False, default=func.now())

    sales_orders = relationship('SalesOrder', back_populates='sales_order_status_logs')
    user = relationship('User', back_populates='sales_order_status_logs')


# Sales Order History Table
class SalesOrderHistory(Base):
    __tablename__ = 'sales_order_history'
    id = Column(Integer, primary_key=True)
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=False)
    changed_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    change_date = Column(DateTime, nullable=False, default=func.now())
    change_description = Column(Text, nullable=False)

    sales_orders = relationship('SalesOrder', back_populates='sales_order_history')
    user = relationship('User', back_populates='sales_order_history')


class SalesOrderNote(Base):
    __tablename__ = 'sales_order_notes'
    id = Column(Integer, primary_key=True)
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=False)
    note_type = Column(String(50), nullable=False)  # 'internal' or 'customer'
    note_content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    recipient = Column(Integer, ForeignKey('users.id'), nullable=True)

    sales_orders = relationship('SalesOrder', back_populates='sales_order_notes')
    # Add quotation relationships to the Company model
    user = relationship("User", back_populates="sales_order_notes", foreign_keys=[created_by])
    user_recipient = relationship("User", back_populates="sales_order_notes_recipient", foreign_keys=[recipient])


# Quotation Attachment Table
class SalesOrderAttachment(Base):
    __tablename__ = 'sales_order_attachments'
    id = Column(Integer, primary_key=True)
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=False)
    file_name = Column(String(255), nullable=False)
    file_location = Column(String(255), nullable=False)
    uploaded_at = Column(DateTime, default=func.now(), nullable=False)

    sales_orders = relationship('SalesOrder', back_populates='sales_order_attachments')


# Quotation Approval Table
class SalesOrderApproval(Base):
    __tablename__ = 'sales_order_approvals'
    id = Column(Integer, primary_key=True)
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=False)
    approver_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    approval_status = Column(String(50), nullable=False)  # e.g., pending, approved, rejected
    approval_date = Column(DateTime, nullable=True)
    comments = Column(Text, nullable=True)

    sales_orders = relationship('SalesOrder', back_populates='sales_order_approvals')
    approver = relationship('User', back_populates='sales_order_approvals')


# Sales Invoice Table

# Sales Invoice Table
class SalesInvoice(Base):
    __tablename__ = 'sales_invoices'
    id = Column(Integer, primary_key=True)
    invoice_number = Column(String(100), nullable=False)  # Unique invoice number
    invoice_date = Column(Date, nullable=False, default=func.current_date())  # Date the invoice was created
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=True, default=None)  # Linked to SalesOrder
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=True, default=None)  # Linked to SalesOrder
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)  # Linked to Vendor (Customer)
    total_line_subtotal = Column(Numeric(18, 2), nullable=False)  # Subtotal before discounts and taxes
    total_amount = Column(Numeric(15, 2), nullable=False)  # Total amount due (after discounts and taxes)
    invoice_discount_type = Column(Enum('amount', 'percentage'), default="amount")  # Discount type
    invoice_discount_value = Column(Numeric(15, 2), default=0.0)  # Discount value (amount or percentage)
    calculated_discount_amount = Column(Numeric(15, 2), default=0.0)  # Calculated discount amount
    total_tax_amount = Column(Numeric(18, 2), default=0.0)  # Changed to Numeric
    invoice_tax_rate = Column(Numeric(15, 2), nullable=True)  # Overall tax rate for the invoice
    shipping_cost = Column(Numeric(15, 2), nullable=True, default=0.0)  # Shipping cost
    handling_cost = Column(Numeric(15, 2), nullable=True, default=0.0)  # Handling cost
    due_date = Column(Date, nullable=True)  # Payment due date
    currency = Column(Integer, ForeignKey('currency.id'))
    status = Column(Enum(InvoiceStatus), nullable=False, default=InvoiceStatus.unpaid)  # Invoice status
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)  # Indicates if entry is posted to the ledger
    terms_and_conditions = Column(Text, nullable=True)  # Terms and conditions
    invoice_reference = Column(String(100), nullable=True)
    project_id = Column(Integer, ForeignKey('projects.id'))

    # ✅ LEDGER ACCOUNTS
    tax_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)  # Tax GL account
    account_receivable_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)  # A/R GL account
    revenue_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)
    discount_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)

    # ✅ EXCHANGE RATE
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)  # Exchange rate used

    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())  # Timestamp when the invoice was created
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())  # Timestamp when the invoice was last updated


    # Relationships
    sales_orders = relationship('SalesOrder', back_populates='invoices')  # Linked to SalesOrder
    quotation = relationship('Quotation', back_populates='invoices')  # Linked to SalesOrder
    customer = relationship('Vendor', back_populates='invoices')  # Linked to Vendor (Customer)
    invoice_items = relationship('SalesInvoiceItem', back_populates='invoice')  # Linked to Invoice Items
    invoice_notes = relationship('SalesInvoiceNote', back_populates='invoice')  # Linked to Invoice Notes
    approvals = relationship('SalesInvoiceApproval', back_populates='invoice')  # Linked to Invoice Approvals
    history = relationship('SalesInvoiceHistory', back_populates='invoice')  # Linked to Invoice History
    status_logs = relationship('SalesInvoiceStatusLog', back_populates='invoice')  # Linked to Invoice Status Logs
    currencies = relationship('Currency', back_populates='sales_invoices')  # Linked to Currency
    sales_transactions = relationship('SalesTransaction',
                                      back_populates='invoice')  # Linked to Sales Transactions (Payments)
    delivery_notes = relationship('DeliveryNote', back_populates='invoices')  # Linked to Currency
    payment_allocations = relationship('PaymentAllocation', back_populates='invoices')

    user = relationship("User", back_populates="invoices")
    project = relationship('Project', back_populates='sales_invoices')
    backorders = relationship('Backorder', back_populates='invoices')

    # ✅ NEW RELATIONSHIPS WITH back_populates
    invoice_tax_account = relationship('ChartOfAccounts', foreign_keys=[tax_account_id], back_populates='tax_invoices')
    account_receivable = relationship('ChartOfAccounts', foreign_keys=[account_receivable_id],
                                      back_populates='receivable_invoices')
    revenue_account = relationship('ChartOfAccounts', foreign_keys=[revenue_account_id],
                                   back_populates='revenue_invoices')
    discount_account = relationship('ChartOfAccounts', foreign_keys=[discount_account_id],
                                    back_populates='discount_invoices')

    # ✅ EXCHANGE RATE RELATIONSHIP
    exchange_rate = relationship('ExchangeRate', foreign_keys=[exchange_rate_id],
                                 back_populates='sales_invoices')

    # Composite unique constraint
    __table_args__ = (
        # Indexes for common query patterns
        Index('idx_invoices_customer_id', 'customer_id'),  # For finding invoices by customer
        Index('idx_invoices_status', 'status'),  # For filtering by status (unpaid, paid, etc.)
        Index('idx_invoices_date_range', 'invoice_date', 'due_date'),  # For date range queries
        Index('idx_invoices_created_by', 'created_by'),  # For finding invoices created by a user
        Index('idx_invoices_project', 'project_id'),  # For project-based filtering
        Index('idx_invoices_currency', 'currency'),  # For currency-based queries
        Index('idx_invoices_posted', 'is_posted_to_ledger'),  # For filtering unposted/posted entries
        Index('idx_invoices_exchange_rate', 'exchange_rate_id'),  # For exchange rate lookups
        Index('idx_invoices_combined', 'customer_id', 'status'),  # Common combined filter
        Index('idx_invoices_created_at', 'created_at'),  # For time-based reporting

        # Composite indexes for complex queries
        Index('idx_invoices_customer_status_date', 'customer_id', 'status', 'invoice_date'),  # Customer reports
        Index('idx_invoices_app_date', 'invoice_date'),  # Company date range
    )


class SalesInvoiceItem(Base):
    __tablename__ = 'sales_invoice_items'
    id = Column(Integer, primary_key=True)
    invoice_id = Column(Integer, ForeignKey('sales_invoices.id'), nullable=False)
    item_type = Column(String(50), nullable=False)  # 'inventory', 'service', or 'non_inventory'
    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=True)
    item_name = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    quantity = Column(Numeric(15, 2), nullable=False)
    uom = Column(Integer, ForeignKey('unit_of_measurement.id'), nullable=False)
    currency = Column(Integer, ForeignKey('currency.id'))
    unit_price = Column(Numeric(15, 2), nullable=False)
    total_price = Column(Numeric(15, 2), nullable=False)  # quantity * unit_price
    tax_rate = Column(Numeric(15, 2), nullable=True)
    tax_amount = Column(Numeric(15, 2), nullable=True)
    discount_amount = Column(Numeric(15, 2), nullable=True)
    discount_rate = Column(Numeric(15, 2), nullable=True)

    location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)

    # Relationships
    invoice = relationship('SalesInvoice', back_populates='invoice_items')
    inventory_item_variation_link = relationship('InventoryItemVariationLink', back_populates='sales_invoice_items')

    unit_of_measurement = relationship('UnitOfMeasurement', back_populates='sales_invoice_items')
    currencies = relationship('Currency', back_populates='sales_invoice_items')
    location = relationship('InventoryLocation', back_populates='invoice_items')


class SalesInvoiceNote(Base):
    __tablename__ = 'sales_invoice_notes'
    id = Column(Integer, primary_key=True)
    invoice_id = Column(Integer, ForeignKey('sales_invoices.id'), nullable=False)
    note_type = Column(String(50), nullable=False)  # 'internal' or 'customer'
    note_content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    recipient = Column(Integer, ForeignKey('users.id'), nullable=True)

    # Relationships
    invoice = relationship('SalesInvoice', back_populates='invoice_notes')
    user = relationship("User", back_populates="sales_invoice_notes", foreign_keys=[created_by])
    user_recipient = relationship("User", back_populates="sales_invoice_notes_recipient", foreign_keys=[recipient])


class SalesInvoiceApproval(Base):
    __tablename__ = 'sales_invoice_approvals'
    id = Column(Integer, primary_key=True)
    invoice_id = Column(Integer, ForeignKey('sales_invoices.id'), nullable=False)  # Linked to SalesInvoice
    approver_id = Column(Integer, ForeignKey('users.id'), nullable=False)  # Linked to User (Approver)
    approval_status = Column(String(50), nullable=False)  # e.g., pending, approved, rejected
    approval_date = Column(DateTime, nullable=True)  # Date of approval
    comments = Column(Text, nullable=True)  # Comments from the approver

    # Relationships
    invoice = relationship('SalesInvoice', back_populates='approvals')  # Linked to SalesInvoice
    approver = relationship('User', back_populates='sales_invoice_approvals')  # Linked to User (Approver)


class SalesInvoiceHistory(Base):
    __tablename__ = 'sales_invoice_history'
    id = Column(Integer, primary_key=True)
    invoice_id = Column(Integer, ForeignKey('sales_invoices.id'), nullable=False)  # Linked to SalesInvoice
    changed_by = Column(Integer, ForeignKey('users.id'), nullable=False)  # Linked to User (who made the change)
    change_date = Column(DateTime, nullable=False, default=func.now())  # Timestamp of the change
    change_description = Column(Text, nullable=False)  # Description of the change

    # Relationships
    invoice = relationship('SalesInvoice', back_populates='history')  # Linked to SalesInvoice
    user = relationship('User', back_populates='sales_invoice_history')  # Linked to User (who made the change)


class SalesInvoiceStatusLog(Base):
    __tablename__ = 'sales_invoice_status_log'
    id = Column(Integer, primary_key=True)
    invoice_id = Column(Integer, ForeignKey('sales_invoices.id'), nullable=False)  # Linked to SalesInvoice
    status = Column(String(50), nullable=False)  # e.g., draft, unpaid, partially_paid, paid, canceled
    changed_by = Column(Integer, ForeignKey('users.id'), nullable=False)  # Linked to User (who made the change)
    change_date = Column(DateTime, nullable=False, default=func.now())  # Timestamp of the change

    # Relationships
    invoice = relationship('SalesInvoice', back_populates='status_logs')  # Linked to SalesInvoice
    user = relationship('User', back_populates='sales_invoice_status_logs')  # Linked to User (who made the change)

class SalesPaymentStatus(enum.Enum):
    unpaid = 'unpaid'
    full = 'full'
    partial = 'partial'
    refund = 'refund'
    paid = 'paid'
    cancelled = 'cancelled'


# Sales Transaction Table (Payments Received)
class BulkPayment(Base):
    __tablename__ = 'bulk_payments'

    id = Column(Integer, primary_key=True)
    bulk_payment_number = Column(String(50), nullable=True, unique=True)  # 👈 ADD THIS
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    total_amount = Column(Numeric(18, 2), nullable=False)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    payment_date = Column(Date, nullable=False)
    payment_method = Column(Integer, ForeignKey('payment_modes.id'), nullable=True)
    payment_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)  # 👈 ADD THIS
    reference = Column(String(100), nullable=True)

    # Exchange rate for the payment (if foreign currency)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)

    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    created_at = Column(DateTime, default=func.now())
    version = Column(Integer, default=1, nullable=False)

    # Relationships
    customer = relationship("Vendor", back_populates="bulk_payments")
    currency = relationship("Currency", back_populates="bulk_payments")
    payment_mode = relationship("PaymentMode", back_populates="bulk_payments")
    payment_account = relationship("ChartOfAccounts", foreign_keys=[payment_account_id])
    project = relationship("Project", back_populates="bulk_payments")  # 👈 ADD THIS
    exchange_rate = relationship("ExchangeRate", back_populates="bulk_payments")
    creator = relationship("User", foreign_keys=[created_by], back_populates="bulk_payments")
    transactions = relationship("SalesTransaction", back_populates="bulk_payment")
    customer_credits = relationship("CustomerCredit", back_populates="bulk_payment")

    __table_args__ = (
        Index('idx_bulk_payments_customer', 'customer_id'),
        Index('idx_bulk_payments_date', 'payment_date'),
        Index('idx_bulk_payments_account', 'payment_account_id'),
        Index('idx_bulk_payments_project', 'project_id'),  # 👈 ADD THIS
    )


class SalesTransaction(Base):
    __tablename__ = 'sales_transactions'
    id = Column(Integer, primary_key=True)
    invoice_id = Column(Integer, ForeignKey('sales_invoices.id'), nullable=False)
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    payment_date = Column(Date, nullable=False, default=func.current_date())
    amount_paid = Column(Numeric(18, 2), nullable=False)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)  # Uses FK instead of string

    # NEW COLUMN - Link to bulk payment if this transaction is part of a group payment
    bulk_payment_id = Column(Integer, ForeignKey('bulk_payments.id'), nullable=True)

    reference_number = Column(String(100), nullable=True)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)  # Indicates if entry is posted to the ledger

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    payment_status = Column(Enum(SalesPaymentStatus), nullable=False, default=SalesPaymentStatus.unpaid)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)

    invoice = relationship('SalesInvoice', back_populates='sales_transactions')
    customer = relationship('Vendor', back_populates='sales_transaction')

    currency = relationship("Currency", back_populates='sales_transaction')
    payment_allocations = relationship('PaymentAllocation', back_populates='sales_transaction')
    payment_receipt = relationship('PaymentReceipt', back_populates='sales_transaction')
    user = relationship('User', back_populates='sales_transaction')

    # NEW RELATIONSHIP
    bulk_payment = relationship('BulkPayment', back_populates='transactions')

    __table_args__ = (
        Index('idx_sales_transactions_invoice', 'invoice_id'),
        Index('idx_sales_transactions_customer', 'customer_id'),
        Index('idx_sales_transactions_date', 'payment_date'),
        Index('idx_sales_transactions_status', 'payment_status'),
        Index('idx_sales_transactions_bulk', 'bulk_payment_id')
    )


class DirectSalesTransaction(Base):
    __tablename__ = 'direct_sales'
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    payment_date = Column(Date, nullable=False, default=func.current_date())
    total_amount = Column(Numeric(18, 2), nullable=False, default=0.0)
    amount_paid = Column(Numeric(18, 2), nullable=False, default=0.0)
    total_line_subtotal = Column(Numeric(18, 2), nullable=False, default=0.0)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)  # Uses FK instead of string

    sale_reference = Column(String(100), nullable=True)
    sales_tax_rate = Column(Numeric(15, 2), nullable=True, default=0.0)  # Handling cost
    shipping_cost = Column(Numeric(15, 2), nullable=True, default=0.0)  # Handling cost
    handling_cost = Column(Numeric(15, 2), nullable=True, default=0.0)  # Handling cost
    sales_discount_type = Column(Enum('amount', 'percentage'), default="amount")  # Discount type
    sales_discount_value = Column(Numeric(15, 2), default=0.0)  # Discount value (amount or percentage)
    terms_and_conditions = Column(Text, nullable=True)  # Terms and conditions
    calculated_discount_amount = Column(Numeric(15, 2), default=0.0)  # Calculated discount amount
    total_tax_amount = Column(Numeric(18, 2), default=0.0)  # Changed to Numeric
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)  # Indicates if entry is posted to the ledger
    is_pos = Column(Boolean, default=False, nullable=False)  # Indicates if this is a POS transaction

    direct_sale_number = Column(String(50), nullable=True)  # Unique identifier for direct sales
    project_id = Column(Integer, ForeignKey('projects.id'))
    status = Column(Enum(OrderStatus), nullable=False, default=OrderStatus.draft)  # Sale transaction status

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    payment_status = Column(String(20), nullable=False, default='full')  # Values: 'full', 'partial', 'refund'
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    version = Column(Integer, default=1, nullable=False)

    revenue_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)

    customer = relationship('Vendor', back_populates='direct_sales')
    user = relationship('User', back_populates='direct_sales')

    currency = relationship("Currency", back_populates='direct_sales')

    payment_allocations = relationship('PaymentAllocation', back_populates='direct_sales')
    payment_receipt = relationship('PaymentReceipt', back_populates='direct_sales')

    direct_sale_items = relationship('DirectSaleItem',
                                     back_populates='direct_sales')  # For direct sales items
    project = relationship('Project', back_populates='direct_sales')
    backorders = relationship('Backorder', back_populates='direct_sales')

    revenue_account = relationship(
        "ChartOfAccounts",
        back_populates="direct_sales"
    )


class DirectSaleItem(Base):
    __tablename__ = 'direct_sale_items'
    id = Column(Integer, primary_key=True)
    transaction_id = Column(Integer, ForeignKey('direct_sales.id'), nullable=False)
    item_type = Column(String(50), nullable=False)  # 'inventory', 'service', or 'non_inventory'
    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=True)
    item_name = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    quantity = Column(Numeric(15, 2), nullable=False)
    uom = Column(Integer, ForeignKey('unit_of_measurement.id'), nullable=False)
    currency = Column(Integer, ForeignKey('currency.id'))
    unit_price = Column(Numeric(15, 2), nullable=False)
    total_price = Column(Numeric(15, 2), nullable=False)  # quantity * unit_price
    tax_rate = Column(Numeric(15, 2), nullable=True, default=0)
    tax_amount = Column(Numeric(15, 2), nullable=True, default=0)
    discount_amount = Column(Numeric(15, 2), nullable=True, default=0)
    discount_rate = Column(Numeric(15, 2), nullable=True, default=0)

    location_id = Column(Integer, ForeignKey('inventory_location.id'), nullable=True)  # Location for the variation

    # Relationships
    direct_sales = relationship('DirectSalesTransaction', back_populates='direct_sale_items')
    inventory_item_variation_link = relationship('InventoryItemVariationLink', back_populates='direct_sale_items')

    unit_of_measurement = relationship('UnitOfMeasurement', back_populates='direct_sale_items')
    currencies = relationship('Currency', back_populates='direct_sale_items')
    location = relationship('InventoryLocation', back_populates='direct_sale_items')


class PaymentAllocation(Base):
    __tablename__ = 'payment_allocations'
    id = Column(Integer, primary_key=True)
    payment_id = Column(Integer, ForeignKey('sales_transactions.id'), nullable=True)
    payment_date = Column(Date, nullable=False, default=func.current_date())
    invoice_id = Column(Integer, ForeignKey('sales_invoices.id'), nullable=True)
    direct_sale_id = Column(Integer, ForeignKey('direct_sales.id'), nullable=True)
    allocated_base_amount = Column(Numeric(18, 2), nullable=False)  # Allocated to the base amount
    allocated_tax_amount = Column(Numeric(18, 2), nullable=False)  # Allocated to the tax amount
    payment_account = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    tax_payable_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    credit_sale_account = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)
    write_off_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=True)  # NEW

    # NEW: Payment type column
    payment_type = Column(String(20), nullable=False, default='cash')  # 'cash' or 'credit'

    payment_mode = Column(Integer, ForeignKey('payment_modes.id'), nullable=True)

    overpayment_amount = Column(Numeric(18, 2), default=0)  # NEW: Track overpayment amount
    reference = Column(String(100), nullable=True)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)  # Indicates if entry is posted to the ledger
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True)

    sales_transaction = relationship('SalesTransaction', back_populates='payment_allocations')
    invoices = relationship('SalesInvoice', back_populates='payment_allocations')
    direct_sales = relationship('DirectSalesTransaction', back_populates='payment_allocations')
    payment_modes = relationship('PaymentMode', back_populates='payment_allocations')
    chart_of_accounts_asset = relationship("ChartOfAccounts", back_populates="payment_allocations_asset",
                                           foreign_keys=[payment_account])
    chart_of_accounts_tax = relationship("ChartOfAccounts", back_populates="payment_allocations_tax",
                                         foreign_keys=[tax_payable_account_id])
    chart_of_accounts_receivable = relationship("ChartOfAccounts", back_populates="payment_allocations_receivable",
                                                foreign_keys=[credit_sale_account])

    chart_of_accounts_write_off = relationship(  # NEW
        "ChartOfAccounts", back_populates="payment_allocations_write_off", foreign_keys=[write_off_account_id]
    )

    # Credit application relation
    credit_applications = relationship("CreditApplication", back_populates="payment_allocation")  # NEW
    # 👇 NEW: Relationship to credits created from this payment
    created_credits = relationship("CustomerCredit",
                                   back_populates="payment_allocation",
                                   foreign_keys="CustomerCredit.payment_allocation_id")
    # 👇 New relationship for exchange rate
    exchange_rate = relationship("ExchangeRate", back_populates="sales_transactions")


class PaymentReceipt(Base):
    __tablename__ = 'payment_receipts'

    id = Column(Integer, primary_key=True)
    payment_receipt_number = Column(String(100), nullable=False)
    sales_transaction_id = Column(Integer, ForeignKey('sales_transactions.id'), nullable=True)
    direct_sales_id = Column(Integer, ForeignKey('direct_sales.id'), nullable=True)
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=True)
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=True)

    # Customer Details
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    received_by_name = Column(String(255), nullable=True)  # Person making the payment (for individuals)

    # Payment Details
    # Payment Details
    amount_received = Column(Numeric(18, 2), nullable=False)
    balance_due = Column(Numeric(18, 2), nullable=False)  # Remaining balance after this payment

    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    payment_mode = Column(Integer, ForeignKey('payment_modes.id'), nullable=True)
    payment_date = Column(Date, nullable=False, default=func.current_date())
    reference_number = Column(String(100), nullable=True)  # Bank ref, mobile money ref, etc.
    description = Column(String(255))
    notes = Column(Text)  # Additional notes about the payment

    status = Column(Enum('draft', 'paid', 'cancelled', 'refunded', 'failed'), nullable=False, default='draft')

    # Issuer Details
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)  # Staff who issued the receipt
    issued_by_name = Column(String(255))  # Name of the staff who issued

    # System Fields
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    sales_transaction = relationship('SalesTransaction', back_populates='payment_receipt')
    customer = relationship('Vendor', back_populates='payment_receipts')
    currency = relationship('Currency', back_populates='payment_receipts')
    payment_modes = relationship('PaymentMode', back_populates='payment_receipts')
    user = relationship('User', back_populates='payment_receipts')
    quotation = relationship('Quotation', back_populates='payment_receipts')
    sales_order = relationship('SalesOrder', back_populates='payment_receipts')
    direct_sales = relationship('DirectSalesTransaction', back_populates='payment_receipt')



class DeliveryNote(Base):
    __tablename__ = 'delivery_notes'

    id = Column(Integer, primary_key=True)
    delivery_number = Column(String(100), nullable=False)
    delivery_reference_number = Column(Integer, ForeignKey('delivery_reference_numbers.id'), nullable=True)
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    delivery_date = Column(Date, nullable=False, default=func.current_date())
    shipping_address = Column(String(255), nullable=True)  # Or use a foreign key to an Address table
    delivery_method = Column(String(50), nullable=True)
    additional_notes = Column(Text, nullable=True)  # Renamed from "notes" for clarity
    delivered_by_name = Column(String(100), nullable=True)
    delivered_by_time = Column(Time, nullable=True)
    received_by_name = Column(String(100), nullable=True)
    received_by_time = Column(Time, nullable=True)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())
    status = Column(Enum(DeliveryStatus), nullable=False, default=DeliveryStatus.draft)

    # New fields for quotation_id and sales_order_id
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=True, default=None)
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=True, default=None)
    invoice_id = Column(Integer, ForeignKey('sales_invoices.id'), nullable=True, default=None)

    # Relationships
    customer = relationship('Vendor', back_populates='delivery_notes')
    delivery_items = relationship('DeliveryNoteItem', back_populates='delivery_note')
    user = relationship('User', back_populates='delivery_notes')

    delivery_reference = relationship('DeliveryReferenceNumber', back_populates='delivery_notes')
    delivery_note_notes = relationship('DeliveryNoteNote', back_populates='delivery_notes')

    # New relationships for quotation and sales_order
    quotation = relationship('Quotation', back_populates='delivery_notes')
    sales_order = relationship('SalesOrder', back_populates='delivery_notes')
    invoices = relationship('SalesInvoice', back_populates='delivery_notes')



class DeliveryNoteItem(Base):
    __tablename__ = 'delivery_note_items'

    id = Column(Integer, primary_key=True)
    delivery_note_id = Column(Integer, ForeignKey('delivery_notes.id'), nullable=False)
    item_type = Column(String(50), nullable=False)  # Added item_type
    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=True)
    item_name = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)
    quantity_delivered = Column(Float, nullable=False, default=0)  # Added quantity_delivered
    uom = Column(Integer, ForeignKey('unit_of_measurement.id'), nullable=False)

    # Relationships
    delivery_note = relationship('DeliveryNote', back_populates='delivery_items')
    inventory_item_variation_link = relationship('InventoryItemVariationLink', back_populates='delivery_note_items')
    unit_of_measurement = relationship("UnitOfMeasurement", back_populates="delivery_note_items")


# Tracking Reference Table
class DeliveryReferenceNumber(Base):
    __tablename__ = 'delivery_reference_numbers'

    id = Column(Integer, primary_key=True)
    delivery_reference_number = Column(String(100), nullable=False)  # Unique Tracking ID
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=True)
    quotation_id = Column(Integer, ForeignKey('quotations.id'), nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=True, onupdate=func.now())

    # Relationships
    sales_orders = relationship('SalesOrder', back_populates='delivery_reference_number')
    quotations = relationship('Quotation', back_populates='delivery_reference_number')
    delivery_notes = relationship('DeliveryNote', back_populates='delivery_reference')


class DeliveryNoteNote(Base):
    __tablename__ = 'delivery_note_notes'
    id = Column(Integer, primary_key=True)
    delivery_note_id = Column(Integer, ForeignKey('delivery_notes.id'), nullable=False)
    note_type = Column(String(50), nullable=False)  # 'internal' or 'customer'
    note_content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    recipient = Column(Integer, ForeignKey('users.id'), nullable=True)

    # Relationships
    delivery_notes = relationship('DeliveryNote', back_populates='delivery_note_notes')
    user = relationship("User", back_populates="delivery_note_notes", foreign_keys=[created_by])
    user_recipient = relationship("User", back_populates="delivery_note_notes_recipient", foreign_keys=[recipient])


# Sales Return Table
class SalesReturn(Base):
    __tablename__ = 'sales_returns'
    id = Column(Integer, primary_key=True)
    return_number = Column(String(100), nullable=False)
    sales_order_id = Column(Integer, ForeignKey('sales_orders.id'), nullable=False)
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=False)
    return_date = Column(Date, nullable=False, default=func.current_date())
    reason = Column(String(200), nullable=True)
    total_amount = Column(Float, nullable=False)

    sales_orders = relationship('SalesOrder', back_populates='sales_returns')


class Backorder(Base):
    __tablename__ = 'backorders'

    id = Column(Integer, primary_key=True)
    backorder_number = Column(String(100), nullable=False)

    # Source document tracking
    source_type = Column(Enum('sales_order', 'sales_invoice', 'direct_sale', name='backorder_source_type'),
                         nullable=True)
    source_id = Column(Integer, nullable=True)
    invoice_id = Column(Integer, ForeignKey('sales_invoices.id'), nullable=True)
    direct_sale_id = Column(Integer, ForeignKey('direct_sales.id'), nullable=True)
    # Customer information
    customer_id = Column(Integer, ForeignKey('vendors.id'), nullable=True)

    # Dates
    backorder_date = Column(DateTime, default=func.now(), nullable=False)
    expected_fulfillment_date = Column(DateTime, nullable=True)
    actual_fulfillment_date = Column(DateTime, nullable=True)
    # Status tracking
    status = Column(Enum('pending', 'partially_fulfilled', 'fulfilled', 'canceled', name="backorder_status"),
                    default='pending', nullable=False)
    priority = Column(Integer, default=2)
    # System fields
    created_at = Column(DateTime, server_default=func.now())
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relationships using back_populates
    supplier = relationship('Vendor', back_populates='backorders')
    backorder_items = relationship('BackorderItem', back_populates='backorders')
    user = relationship("User", back_populates="backorders", foreign_keys=[created_by])
    invoices = relationship("SalesInvoice", back_populates="backorders")
    direct_sales = relationship("DirectSalesTransaction", back_populates="backorders")

class BackorderItem(Base):
    __tablename__ = 'backorder_items'

    id = Column(Integer, primary_key=True)
    backorder_id = Column(Integer, ForeignKey('backorders.id'), nullable=False)

    # Item identification
    item_id = Column(Integer, ForeignKey('inventory_item_variation_link.id'), nullable=False)

    # Quantities
    original_quantity = Column(Numeric(15, 2), nullable=False)
    fulfilled_quantity = Column(Numeric(15, 2), default=0, nullable=False)
    remaining_quantity = Column(Numeric(15, 2), nullable=False)

    # UOM
    uom_id = Column(Integer, ForeignKey('unit_of_measurement.id'), nullable=False)

    # Status tracking
    status = Column(
        Enum('pending', 'partially_fulfilled', 'fulfilled', 'canceled', 'discontinued', name="backorder_item_status"),
        default='pending', nullable=False)

    # Dates
    expected_fulfillment_date = Column(DateTime, nullable=True)
    actual_fulfillment_date = Column(DateTime, nullable=True)

    # System fields
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # Relationships using back_populates
    backorders = relationship('Backorder', back_populates='backorder_items')
    inventory_item_variation_link = relationship('InventoryItemVariationLink', back_populates='backorder_items')
    unit_of_measurement = relationship('UnitOfMeasurement', back_populates='backorder_items')
    backorder_fulfillments = relationship('BackorderFulfillment', back_populates='backorder_items')


class BackorderFulfillment(Base):
    __tablename__ = 'backorder_fulfillments'

    id = Column(Integer, primary_key=True)
    backorder_item_id = Column(Integer, ForeignKey('backorder_items.id'), nullable=False)

    # Fulfillment details
    fulfillment_quantity = Column(Numeric(15, 2), nullable=False)
    fulfillment_date = Column(DateTime, default=func.now(), nullable=False)

    # System fields
    created_at = Column(DateTime, server_default=func.now())

    # Relationships using back_populates
    backorder_items = relationship('BackorderItem', back_populates='backorder_fulfillments')

    __table_args__ = (
        Index('idx_fulfillment_backorder_item', 'backorder_item_id'),
    )




class ExpenseStatusEnum(enum.Enum):
    draft = 'draft'
    approved = 'approved'
    posted = 'posted'


class ExpenseTransaction(Base):
    __tablename__ = 'expense_transactions'

    id = Column(Integer, primary_key=True)
    expense_entry_number = Column(String(100), nullable=True, unique=False)  # You can add a UniqueConstraint if needed

    # Foreign keys
    project_id = Column(Integer, ForeignKey('projects.id'), nullable=True)
    vendor_id = Column(Integer, ForeignKey('vendors.id'), nullable=True)
    currency_id = Column(Integer, ForeignKey('currency.id'), nullable=False)
    payment_mode_id = Column(Integer, ForeignKey('payment_modes.id'), nullable=True)

    payment_account_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)
    payment_date = Column(Date, nullable=False, default=func.current_date())

    # Financial fields
    total_amount = Column(Numeric(18, 2), nullable=False, default=0.00)
    exchange_rate_id = Column(Integer, ForeignKey('exchange_rates.id'), nullable=True, index=True)

    # Status tracking
    status = Column(Enum(ExpenseStatusEnum), default=ExpenseStatusEnum.draft, nullable=False)
    is_posted_to_ledger = Column(Boolean, default=False, nullable=False)

    # Header fields
    expense_ref_no = Column(String(50), nullable=True, index=True)
    narration = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Audit
    created_by = Column(Integer, ForeignKey('users.id'), nullable=False)

    # Relationships
    project = relationship('Project', back_populates='expense_transactions')
    vendor = relationship('Vendor', back_populates='expense_transactions')
    currency = relationship("Currency", back_populates='expense_transactions')
    user = relationship('User', back_populates='expense_transactions')
    payment_account = relationship('ChartOfAccounts', back_populates='expense_payment_account')
    payment_mode = relationship('PaymentMode', back_populates='expense_transactions')
    expense_items = relationship('ExpenseItem', back_populates='expense_transaction', cascade='all, delete-orphan')
    exchange_rate = relationship("ExchangeRate", back_populates="expense_transactions")

    __table_args__ = (
        Index('idx_expense_app_date', 'payment_date'),
        Index('idx_expense_vendor', 'vendor_id'),
        Index('idx_expense_currency', 'currency_id'),
        Index('idx_expense_app_status', 'status'),
    )


class ExpenseItem(Base):
    __tablename__ = 'expense_items'

    id = Column(Integer, primary_key=True)
    expense_transaction_id = Column(Integer, ForeignKey('expense_transactions.id'), nullable=False)
    subcategory_id = Column(Integer, ForeignKey('chart_of_accounts.id'), nullable=False)
    amount = Column(Numeric(18, 2), nullable=False)
    description = Column(String(100), nullable=True)

    # Relationships
    expense_transaction = relationship('ExpenseTransaction', back_populates='expense_items')
    subcategory = relationship('ChartOfAccounts', back_populates='expense_items')
