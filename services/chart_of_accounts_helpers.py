# app/services/chart_of_accounts_helpers.py

import logging
import math
from datetime import datetime
from decimal import Decimal

from sqlalchemy import func

from ai import get_base_currency, get_exchange_rate
from models import ChartOfAccounts, \
    Category, OpeningBalance, Journal, PaymentMode

logger = logging.getLogger(__name__)


# Helper function to calculate account balance using the same logic as get_all_balances
def calculate_account_balance(db_session, account_id, app_id):
    from decimal import Decimal
    from sqlalchemy.orm import joinedload

    # Get the account
    account = db_session.query(ChartOfAccounts).filter(
        ChartOfAccounts.id == account_id,
        ChartOfAccounts.app_id == app_id
    ).first()

    if not account:
        return Decimal('0')

    # Get all journal entries for this account
    journal_entries = db_session.query(JournalEntry).options(
        joinedload(JournalEntry.journal).joinedload(Journal.currency)
    ).filter(
        JournalEntry.subcategory_id == account_id,
        JournalEntry.app_id == app_id
    ).all()

    # Get base currency info
    base_currency_info = get_base_currency(db_session, app_id)
    if not base_currency_info:
        return Decimal('0')

    base_currency_id = base_currency_info["base_currency_id"]

    # Calculate balance using the same logic as get_all_balances
    account_balance = Decimal('0')

    for entry in journal_entries:
        amount = Decimal(str(entry.amount))
        currency_id = entry.journal.currency_id if entry.journal else base_currency_id

        # Normalize dr_cr and normal_balance to first letter uppercase
        txn_dc = entry.dr_cr.upper()[0] if entry.dr_cr else 'D'  # 'D' or 'C'
        normal_bal = account.normal_balance.upper()[0] if account.normal_balance else 'D'  # 'D' or 'C'

        # Determine if we add or subtract
        adjusted_amount = amount if txn_dc == normal_bal else -amount

        # Convert to base currency if needed
        if currency_id == base_currency_id:
            converted_amount = adjusted_amount
        else:
            # Get exchange rate
            try:
                rate = get_exchange_rate(db_session, currency_id, base_currency_id, app_id)
                converted_amount = adjusted_amount * Decimal(str(rate))
            except Exception as e:
                logger.error(f"Currency conversion error for journal entry {entry.id}: {str(e)}")
                converted_amount = Decimal('0')

        account_balance += converted_amount

    return round(account_balance, 2)


# Helper function to check for open transactions
def has_open_transactions(db_session, account_id, app_id):
    from datetime import datetime, timedelta
    from models import JournalEntry

    # Check if account is referenced in any recent journal entries (last 30 days)
    recent_entry_count = db_session.query(JournalEntry).join(
        JournalEntry.journal
    ).filter(
        JournalEntry.subcategory_id == account_id,
        JournalEntry.app_id == app_id,
        Journal.date >= datetime.now() - timedelta(days=30)
    ).count()

    return recent_entry_count > 0


def get_retained_earnings_account_id(db_session, app_id, user_id):
    """
    Fetch or create the retained earnings account and return its ID
    """
    # Check if it already exists
    retained_earnings_account = db_session.query(ChartOfAccounts).filter(
        ChartOfAccounts.sub_category.ilike('%retained earnings%'),
        ChartOfAccounts.app_id == app_id,
        ChartOfAccounts.is_system_account.is_(True)
    ).first()

    # Ensure equity category exists
    equity_category = db_session.query(Category).filter_by(
        account_type="Equity",
        category="System Equity",
        app_id=app_id
    ).first()

    if not equity_category:
        equity_category = Category(
            account_type="Equity",
            category_id="SYS-3900",
            category="System Equity",
            app_id=app_id
        )
        db_session.add(equity_category)
        db_session.flush()

    # Create account if not found
    if not retained_earnings_account:
        retained_earnings_account = ChartOfAccounts(
            parent_account_type="Equity",
            parent_account_type_id="3",
            category="System Equity",
            category_id=equity_category.category_id,
            category_fk=equity_category.id,
            sub_category="Retained Earnings",
            sub_category_id="SYS-3901",
            normal_balance="Credit",
            is_active=True,
            created_by=user_id,
            app_id=app_id,
            is_system_account=True
        )
        db_session.add(retained_earnings_account)
        db_session.flush()

    return retained_earnings_account.id


def update_retained_earnings_opening_balance(db_session, app_id, user_id, amount, mode='increment'):
    """
    Update retained earnings opening balance.
    mode='increment' → add to balance
    mode='replace'   → overwrite balance
    """

    amount = Decimal(str(amount))
    account_id = get_retained_earnings_account_id(db_session, app_id, user_id)

    ob = db_session.query(OpeningBalance).filter_by(
        account_id=account_id,
        app_id=app_id
    ).first()

    if math.isclose(amount, 0.0, abs_tol=0.001):
        return account_id

    if ob:
        if mode == 'increment':
            ob.balance += Decimal(str(amount))
        elif mode == 'replace':
            ob.balance = amount
        else:
            raise ValueError("Invalid mode: must be 'increment' or 'replace'")
        ob.updated_at = datetime.now()
    else:
        ob = OpeningBalance(
            account_id=account_id,
            balance=amount,
            created_by=user_id,
            app_id=app_id
        )
        db_session.add(ob)

    return account_id


def reverse_retained_earnings_balance(db_session, app_id, user_id, reversal_amount):
    """
    Reverse (subtract) a retained earnings opening balance by a given amount.
    If no balance exists, this creates a negative retained earnings balance.
    Allows negative retained earnings balances.
    """
    if math.isclose(reversal_amount, 0.0, abs_tol=0.001):
        return None  # Nothing to reverse

    account_id = get_retained_earnings_account_id(db_session, app_id, user_id)

    ob = db_session.query(OpeningBalance).filter_by(
        account_id=account_id,
        app_id=app_id
    ).first()

    if ob:
        ob.balance -= reversal_amount  # 🔁 Decrease (can go negative)
        ob.updated_at = datetime.now()

    else:
        # No balance yet — create a negative retained earnings opening balance
        ob = OpeningBalance(
            account_id=account_id,
            balance=-reversal_amount,  # Negative balance allowed
            created_by=user_id,
            app_id=app_id
        )
        db_session.add(ob)

    return account_id



def group_accounts_by_category(accounts):
    """Group accounts by category and sort categories alphabetically"""
    grouped = {}
    for account in accounts:
        if account.category not in grouped:
            grouped[account.category] = []
        grouped[account.category].append(account)

    # Sort accounts within each category by sub_category
    for category in grouped:
        grouped[category] = sorted(grouped[category], key=lambda x: x.sub_category)

    # Return sorted by category name
    return dict(sorted(grouped.items()))


def get_or_create_payment_mode(db_session, payment_method_name, app_id, current_user):
    """
    Get or create a payment mode by name for the given app_id
    """
    try:
        # Normalize the payment method name
        normalized_name = payment_method_name.strip().lower()

        # Map common POS payment methods to proper names
        payment_mode_map = {
            'cash': 'Cash',
            'card': 'Credit Card',
            'mobile': 'Mobile Payment'
        }

        # Get the proper display name, fallback to capitalized input
        display_name = payment_mode_map.get(normalized_name, payment_method_name.capitalize())

        # Check if payment mode already exists
        payment_mode = db_session.query(PaymentMode).filter(
            func.lower(PaymentMode.payment_mode) == normalized_name,
            PaymentMode.app_id == app_id
        ).first()

        if payment_mode:
            return payment_mode.id

        # Create new payment mode if it doesn't exist
        new_payment_mode = PaymentMode(
            payment_mode=display_name,
            app_id=app_id
        )
        db_session.add(new_payment_mode)
        db_session.flush()  # Get the ID without committing

        return new_payment_mode.id

    except Exception as e:
        logger.error(f"Error in get_or_create_payment_mode: {str(e)}")
        raise e



def get_or_create_suspense_account(db_session, app_id, created_by_user_id=None):
    """
    Get or create the system suspense account for multi-currency payment bridging.

    This account is used as a temporary clearing account when processing
    payments in one currency against invoices in other currencies.

    The suspense account should always have a zero balance after all
    allocations are complete.

    Args:
        db_session: Database session
        app_id: Company/App ID
        created_by_user_id: User ID creating the account (optional)

    Returns:
        int: ID of the suspense account
    """
    try:
        # Try to find existing suspense account
        suspense_account = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.sub_category == 'Suspense - Currency Clearing',
            ChartOfAccounts.is_system_account == True
        ).first()

        if suspense_account:
            return suspense_account.id

        # Step 1: Find or create SYSTEM-ONLY current assets category
        system_current_assets_category = db_session.query(Category).filter(
            Category.app_id == app_id,
            Category.account_type == 'asset',
            Category.category == 'System Current Assets',  # Unique system category name
            Category.category_id == 'SYS-1200'  # System-specific ID (12xx for current assets)
        ).first()

        if not system_current_assets_category:
            system_current_assets_category = Category(
                account_type='asset',
                category_id='SYS-1200',
                category='System Current Assets',
                app_id=app_id
            )
            db_session.add(system_current_assets_category)
            db_session.flush()

        # Step 2: Create the suspense account in ChartOfAccounts
        suspense_account = ChartOfAccounts(
            # Basic account information
            parent_account_type='Asset',
            parent_account_type_id='SYS-1200',
            category_id='SYS-1200',
            category_fk=system_current_assets_category.id,
            category='System Current Assets',
            sub_category_id='SYS-1210',  # System-specific ID for suspense
            sub_category='Suspense - Currency Clearing',

            # Account flags
            is_bank=False,
            is_cash=False,
            is_receivable=False,
            is_payable=False,
            is_monetary=True,
            is_active=True,
            is_system_account=True,  # Mark as system account

            # Financial properties
            normal_balance='Debit',  # Asset accounts have debit normal balance

            # Ownership
            created_by=created_by_user_id,
            app_id=app_id
        )

        db_session.add(suspense_account)
        db_session.flush()

        logger.info(f"Created suspense account with ID: {suspense_account.id} for app_id: {app_id}")
        return suspense_account.id

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error getting/creating suspense account for app_id {app_id}: {str(e)}")
        raise


def get_system_account(db_session, app_id, account_type, created_by_user_id=None):
    """
    Unified function to get or create system accounts.

    Args:
        db_session: Database session
        app_id: Company/App ID
        account_type: Type of system account needed:
            - 'customer_credit'
            - 'write_off'
            - 'suspense'
            - 'fx_gain_loss'
        created_by_user_id: User ID creating the account (optional)

    Examples of usage:
    # Usage examples:
    suspense_id = get_system_account(db_session, app_id, 'suspense', current_user.id)
    credit_id = get_system_account(db_session, app_id, 'customer_credit', current_user.id)
    write_off_id = get_system_account(db_session, app_id, 'write_off', current_user.id)
    fx_id = get_system_account(db_session, app_id, 'fx_gain_loss', current_user.id)

    Returns:
        int: ID of the requested system account
    """
    try:
        # Define account configurations
        account_configs = {
            'customer_credit': {
                'sub_category': 'Customer Credits',
                'parent_type': 'Liability',
                'parent_category_id': 'SYS-2000',
                'parent_category_name': 'System Liabilities',
                'sub_category_id': 'SYS-2100',
                'normal_balance': 'Credit',
                'is_payable': True
            },
            'write_off': {
                'sub_category': 'System Miscellaneous Income',
                'parent_type': 'Income',
                'parent_category_id': 'SYS-4000',
                'parent_category_name': 'System Income',
                'sub_category_id': 'SYS-4100',
                'normal_balance': 'Credit',
                'is_payable': False
            },
            'suspense': {
                'sub_category': 'Suspense - Currency Clearing',
                'parent_type': 'Asset',
                'parent_category_id': 'SYS-1200',
                'parent_category_name': 'System Current Assets',
                'sub_category_id': 'SYS-1210',
                'normal_balance': 'Debit',
                'is_payable': False
            },
            'fx_gain_loss': {
                'sub_category': 'Foreign Exchange Gain/Loss',
                'parent_type': 'Income',
                'parent_category_id': 'SYS-4000',
                'parent_category_name': 'System Income',
                'sub_category_id': 'SYS-4200',
                'normal_balance': 'Credit',  # Can be both gain (credit) or loss (debit)
                'is_payable': False
            }
        }

        if account_type not in account_configs:
            raise ValueError(f"Unknown system account type: {account_type}")

        config = account_configs[account_type]

        # Try to find existing account
        account = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.sub_category == config['sub_category'],
            ChartOfAccounts.is_system_account == True
        ).first()

        if account:
            return account.id

        # Find or create system category
        system_category = db_session.query(Category).filter(
            Category.app_id == app_id,
            Category.account_type == config['parent_type'].lower(),
            Category.category == config['parent_category_name'],
            Category.category_id == config['parent_category_id']
        ).first()

        if not system_category:
            system_category = Category(
                account_type=config['parent_type'].lower(),
                category_id=config['parent_category_id'],
                category=config['parent_category_name'],
                app_id=app_id
            )
            db_session.add(system_category)
            db_session.flush()

        # Create the account
        new_account = ChartOfAccounts(
            parent_account_type=config['parent_type'],
            parent_account_type_id=config['parent_category_id'],
            category_id=config['parent_category_id'],
            category_fk=system_category.id,
            category=config['parent_category_name'],
            sub_category_id=config['sub_category_id'],
            sub_category=config['sub_category'],

            is_bank=False,
            is_cash=False,
            is_receivable=False,
            is_payable=config.get('is_payable', False),
            is_monetary=True,
            is_active=True,
            is_system_account=True,

            normal_balance=config['normal_balance'],

            created_by=created_by_user_id,
            app_id=app_id
        )

        db_session.add(new_account)
        db_session.flush()

        logger.info(f"Created {account_type} account with ID: {new_account.id} for app_id: {app_id}")
        return new_account.id

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error getting/creating {account_type} account for app_id {app_id}: {str(e)}")
        raise


def get_all_system_accounts(db_session, app_id, created_by_user_id=None):
    """
    Get or create all system accounts in one go.
    Returns a dictionary with all system account IDs.


    # Usage in process_payment:
    system_accounts = get_all_system_accounts(
        db_session=db_session,
        app_id=current_user.app_id,
        created_by_user_id=current_user.id
    )

    suspense_account_id = system_accounts['suspense']
    fx_gain_loss_account_id = system_accounts['fx_gain_loss']
    customer_credit_account_id = system_accounts['customer_credit']  # Now included!
    write_off_account_id = system_accounts.get('write_off')  # May be None if not needed


    """
    try:
        account_configs = {
            'customer_credit': {
                'sub_category': 'Customer Credits',
                'parent_type': 'Liability',
                'parent_category_id': 'SYS-2000',
                'parent_category_name': 'System Liabilities',
                'sub_category_id': 'SYS-2100',
                'normal_balance': 'Credit',
                'is_payable': True
            },
            'write_off': {
                'sub_category': 'System Miscellaneous Income',
                'parent_type': 'Income',
                'parent_category_id': 'SYS-4000',
                'parent_category_name': 'System Income',
                'sub_category_id': 'SYS-4100',
                'normal_balance': 'Credit',
                'is_payable': False
            },
            'suspense': {
                'sub_category': 'Suspense - Currency Clearing',
                'parent_type': 'Asset',
                'parent_category_id': 'SYS-1200',
                'parent_category_name': 'System Current Assets',
                'sub_category_id': 'SYS-1210',
                'normal_balance': 'Debit',
                'is_payable': False
            },
            'fx_gain_loss': {
                'sub_category': 'Foreign Exchange Gain/Loss',
                'parent_type': 'Income',
                'parent_category_id': 'SYS-4000',
                'parent_category_name': 'System Income',
                'sub_category_id': 'SYS-4200',
                'normal_balance': 'Credit',
                'is_payable': False
            }
        }

        # First, try to find all existing system accounts
        existing_accounts = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.is_system_account == True,
            ChartOfAccounts.sub_category.in_([
                config['sub_category'] for config in account_configs.values()
            ])
        ).all()

        account_map = {acc.sub_category: acc.id for acc in existing_accounts}

        # Check which categories need to be created
        categories_needed = {}
        for acc_type, config in account_configs.items():
            if config['sub_category'] not in account_map:
                categories_needed[config['parent_category_id']] = {
                    'parent_type': config['parent_type'],
                    'parent_category_id': config['parent_category_id'],
                    'parent_category_name': config['parent_category_name']
                }

        # Create missing categories in one batch
        category_map = {}
        if categories_needed:
            for cat_id, cat_data in categories_needed.items():
                existing_category = db_session.query(Category).filter(
                    Category.app_id == app_id,
                    Category.account_type == cat_data['parent_type'].lower(),
                    Category.category_id == cat_id
                ).first()

                if existing_category:
                    category_map[cat_id] = existing_category.id
                else:
                    new_category = Category(
                        account_type=cat_data['parent_type'].lower(),
                        category_id=cat_id,
                        category=cat_data['parent_category_name'],
                        app_id=app_id
                    )
                    db_session.add(new_category)
                    db_session.flush()
                    category_map[cat_id] = new_category.id

        # Create missing accounts
        result = {}
        for acc_type, config in account_configs.items():
            if config['sub_category'] in account_map:
                result[acc_type] = account_map[config['sub_category']]
            else:
                # Create the account
                new_account = ChartOfAccounts(
                    parent_account_type=config['parent_type'],
                    parent_account_type_id=config['parent_category_id'],
                    category_id=config['parent_category_id'],
                    category_fk=category_map[config['parent_category_id']],
                    category=config['parent_category_name'],
                    sub_category_id=config['sub_category_id'],
                    sub_category=config['sub_category'],

                    is_bank=False,
                    is_cash=False,
                    is_receivable=False,
                    is_payable=config.get('is_payable', False),
                    is_monetary=True,
                    is_active=True,
                    is_system_account=True,

                    normal_balance=config['normal_balance'],

                    created_by=created_by_user_id,
                    app_id=app_id
                )
                db_session.add(new_account)
                db_session.flush()
                result[acc_type] = new_account.id

        return result

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error getting/creating system accounts for app_id {app_id}: {str(e)}")
        raise


