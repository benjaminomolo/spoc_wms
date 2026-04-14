import datetime
import random
import string
import traceback
from decimal import Decimal, ROUND_HALF_UP
import logging
from typing import Optional

from flask import url_for, flash
from flask_login import current_user
from sqlalchemy import func, case, or_, Integer
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from db import Session
from exceptions import DatabaseError
from models import InventoryEntry, InventoryLocation, Currency, ExchangeRate, \
    Backorder, BackorderItem, PurchaseOrder, PurchaseTransaction, PurchasePaymentAllocation, DirectPurchaseItem, \
    DirectPurchaseTransaction, GoodsReceipt, GoodsReceiptItem, PurchaseOrderItem, PurchaseReturn, NotificationScope, \
    Notification, NotificationStatus, UnitOfMeasurement, User, Module, UserModuleAccess, ChartOfAccounts, \
    ExpenseTransaction, PaymentAllocation, SalesTransaction, SalesInvoice, DirectSalesTransaction, DirectSaleItem, \
    InventoryItemVariationLink, InventoryEntryLineItem, Journal, JournalEntry
from utils_and_helpers.numbers import generate_sequence_number

# Set up logging
logger = logging.getLogger(__name__)

# Define cash-related keywords
cash_keywords = ["cash", "bank", "cheq", "saving", "deposit", "fund", "check", "savings"]


def is_cash_related(text):
    """Check if the text contains any of the cash-related keywords."""
    text = text.lower()
    return any(keyword in text for keyword in cash_keywords)


def is_cash_transaction(transaction):
    """Check if transaction is cash-related"""
    return (
            transaction.transaction_type == "Asset" and (
            (transaction.chart_of_accounts and transaction.chart_of_accounts.categories and
             is_cash_related(transaction.chart_of_accounts.categories.category)) or
            (transaction.chart_of_accounts and
             is_cash_related(transaction.chart_of_accounts.sub_category))
    ))


def is_cash_equivalent(entry):
    """
    Checks if the transaction is cash- or bank-equivalent
    based on its linked chart of accounts.
    """
    coa = entry.chart_of_accounts

    if not coa:
        return False

    return coa.is_cash == 1 or coa.is_bank == 1


def get_cash_balances(db_session, app_id, currency=None, start_date=None, end_date=None):
    """Fetch cash balances in their original currencies without conversion"""
    logger.debug("Calculating cash balances (original currency)")

    # Base filter for journals
    base_filter = [Journal.app_id == app_id]
    if currency:
        base_filter.append(Journal.currency_id == currency)

    # Query current period journals with entries
    date_filter = []
    if start_date:
        date_filter.append(Journal.date >= start_date)
    if end_date:
        date_filter.append(Journal.date <= end_date)

    current_journals = (
        db_session.query(Journal)
        .options(joinedload(Journal.entries).joinedload(JournalEntry.chart_of_accounts))
        .filter(*base_filter, *date_filter)
        .all()
    )

    # Query opening journals (date before start_date)
    opening_journals = []
    if start_date:
        opening_journals = (
            db_session.query(Journal)
            .options(joinedload(Journal.entries).joinedload(JournalEntry.chart_of_accounts))
            .filter(*base_filter, Journal.date < start_date)
            .all()
        )

    # Helper to get cash-equivalent entries
    def get_cash_entries(journals):
        entries = []
        for journal in journals:
            for entry in journal.entries:
                if entry.chart_of_accounts and entry.chart_of_accounts.is_cash:
                    entries.append((journal, entry))
        return entries

    current_cash_entries = get_cash_entries(current_journals)
    opening_cash_entries = get_cash_entries(opening_journals)

    # Accumulate balances per currency
    balances = {}
    opening_balances = {}

    for journal, entry in current_cash_entries:
        curr = journal.currency_id
        if curr not in balances:
            balances[curr] = {"debit": Decimal('0'), "credit": Decimal('0')}
        if entry.dr_cr == "D":
            balances[curr]["debit"] += entry.amount
        else:
            balances[curr]["credit"] += entry.amount

    for journal, entry in opening_cash_entries:
        curr = journal.currency_id
        if curr not in opening_balances:
            opening_balances[curr] = {"debit": Decimal('0'), "credit": Decimal('0')}
        if entry.dr_cr == "D":
            opening_balances[curr]["debit"] += entry.amount
        else:
            opening_balances[curr]["credit"] += entry.amount

    # Format results
    result = []
    for curr in balances:
        opening = opening_balances.get(curr, {"debit": Decimal('0'), "credit": Decimal('0')})
        net = (balances[curr]["debit"] + opening["debit"]) - (balances[curr]["credit"] + opening["credit"])

        result.append({
            "currency": curr,
            "amount": float(net),
            "original_amount": float(balances[curr]["debit"] - balances[curr]["credit"]),
            "opening_balance": float(opening["debit"] - opening["credit"])
        })

    return result


# def get_cash_balances_with_base(
#         db_session, app_id, currency=None, start_date=None, end_date=None,
#         with_base_currency=False, base_currency=None, base_currency_code=None
# ):
#     """Fetch and calculate cash balances including opening balances."""
#
#     # Base query filtered by app_id and optionally currency
#     base_filter = [Journal.app_id == app_id]
#     if currency and not with_base_currency:
#         base_filter.append(Journal.currency_id == currency)
#
#     # Query journals within date range (including end_date)
#     date_filter = []
#     if start_date:
#         date_filter.append(Journal.date >= start_date)
#     if end_date:
#         date_filter.append(Journal.date <= end_date)
#
#     # Query current period journals with their entries
#     current_journals = (
#         db_session.query(Journal)
#         .options(joinedload(Journal.entries).joinedload(JournalEntry.chart_of_accounts))
#         .filter(*base_filter, *date_filter)
#         .all()
#     )
#
#     # Query opening journals (date before start_date)
#     opening_journals = []
#     if start_date:
#         opening_journals = (
#             db_session.query(Journal)
#             .options(joinedload(Journal.entries).joinedload(JournalEntry.chart_of_accounts))
#             .filter(*base_filter, Journal.date < start_date)
#             .all()
#         )
#
#     # Get all journal entries that are cash-equivalent
#     def get_cash_entries(journals):
#         cash_entries = []
#         for journal in journals:
#             for entry in journal.entries:
#                 if is_cash_equivalent(entry):  # This function needs to be updated for JournalEntry
#                     cash_entries.append((journal, entry))
#         return cash_entries
#
#     current_cash_entries = get_cash_entries(current_journals)
#     opening_cash_entries = get_cash_entries(opening_journals)
#
#     # Use sets for quick lookup of opening journal ids to avoid double counting
#     opening_journal_ids = {journal.id for journal, _ in opening_cash_entries}
#
#     # Helper dicts to accumulate balances keyed by (category, subcategory, subcategory_id)
#     opening_balances = {}
#     current_balances = {}
#
#     # Function to accumulate balances for journal entries
#     def update_balances_from_entry(journal, entry, balances_dict):
#         chart_account = entry.chart_of_accounts
#         if not chart_account:
#             return
#
#         category = chart_account.categories.category if chart_account.categories else None
#         subcategory = chart_account.sub_category
#         subcategory_id = chart_account.sub_category_id
#         key = (category, subcategory, subcategory_id)
#
#         if key not in balances_dict:
#             balances_dict[key] = {"debit": Decimal('0.0'), "credit": Decimal('0.0')}
#
#         # Convert amount to base currency if needed
#         amount = Decimal(str(entry.amount))
#         if with_base_currency and journal.currency_id != base_currency_code:
#             exchange_rate = journal.exchange_rate.rate if journal.exchange_rate else Decimal('1.0')
#             amount = amount * exchange_rate
#
#         if entry.dr_cr == 'D':
#             balances_dict[key]["debit"] += amount
#         else:
#             balances_dict[key]["credit"] += amount
#
#     # Process opening balances
#     for journal, entry in opening_cash_entries:
#         update_balances_from_entry(journal, entry, opening_balances)
#
#     # Process current balances (excluding entries from opening journals)
#     for journal, entry in current_cash_entries:
#         if journal.id in opening_journal_ids:
#             continue  # Skip to avoid double counting
#         update_balances_from_entry(journal, entry, current_balances)
#
#     # Aggregate opening + current balances and compute total cash
#     formatted_cash_balances = []
#     total_cash_balance = Decimal('0.0')
#
#     keys = set(opening_balances.keys()) | set(current_balances.keys())
#
#     for key in keys:
#         opening = opening_balances.get(key, {"debit": Decimal('0.0'), "credit": Decimal('0.0')})
#         current = current_balances.get(key, {"debit": Decimal('0.0'), "credit": Decimal('0.0')})
#
#         balance_amount = round(
#             (current.get("debit", 0) + opening.get("debit", 0)) -
#             (current.get("credit", 0) + opening.get("credit", 0)),
#             2
#         )
#
#         cat, subcat, subcat_id = key
#
#         formatted_cash_balances.append({
#             "category": cat,
#             "subcategory": subcat,
#             "subcategory_id": subcat_id,
#             "amount": float(balance_amount)
#         })
#
#         total_cash_balance += balance_amount
#
#     return formatted_cash_balances, round(total_cash_balance, 2)


def get_cash_balances_with_base(
        db_session, app_id, currency=None, start_date=None, end_date=None,
        with_base_currency=False, base_currency_id=None, base_currency=None
):
    """Fixed version with proper currency filtering for both scenarios"""
    from ai import get_base_currency, get_exchange_rate
    try:
        # Get base currency if not provided
        if not base_currency_id or not base_currency:
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return [], 0.0
            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

        # Build base filters - no currency filter at database level
        base_filters = [Journal.app_id == app_id, Journal.status == 'Posted']

        from sqlalchemy.orm import selectinload

        # Query for journals with entries
        query = (
            db_session.query(Journal)
            .options(
                selectinload(Journal.entries).selectinload(JournalEntry.chart_of_accounts),
                selectinload(Journal.currency),
                selectinload(Journal.exchange_rate)
            )
            .filter(*base_filters)
        )

        # Apply date filters
        if start_date and end_date:
            query = query.filter(Journal.date.between(start_date, end_date))
        elif start_date:
            query = query.filter(Journal.date >= start_date)
        elif end_date:
            query = query.filter(Journal.date <= end_date)

        all_journals = query.all()

        # Get cash accounts
        cash_accounts = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            or_(
                ChartOfAccounts.is_cash == True,
                ChartOfAccounts.is_bank == True
            )
        ).all()

        cash_account_ids = [account.id for account in cash_accounts]

        # Sum balances by account and currency first
        account_currency_balances = {}  # {account_id: {currency_id: balance}}

        for journal in all_journals:
            for entry in journal.entries:
                if entry.subcategory_id not in cash_account_ids:
                    continue

                chart_account = entry.chart_of_accounts
                if not chart_account:
                    continue

                account_id = entry.subcategory_id
                currency_id = journal.currency_id
                amount = Decimal(str(entry.amount))

                # Initialize account tracking
                if account_id not in account_currency_balances:
                    account_currency_balances[account_id] = {}

                # Initialize currency tracking
                if currency_id not in account_currency_balances[account_id]:
                    account_currency_balances[account_id][currency_id] = Decimal('0.0')

                # Calculate net effect based on dr_cr and normal balance
                normal_balance = chart_account.normal_balance.upper()[0] if chart_account.normal_balance else 'D'
                txn_dc = entry.dr_cr.upper()[0]

                # Debits increase asset accounts, Credits decrease asset accounts
                net_effect = amount if txn_dc == normal_balance else -amount

                account_currency_balances[account_id][currency_id] += net_effect

        # Process balances based on currency filtering scenario
        formatted_cash_balances = []
        total_cash_balance = Decimal('0.0')

        for account_id, currency_balances in account_currency_balances.items():
            account = next((acc for acc in cash_accounts if acc.id == account_id), None)
            if not account:
                continue

            # SCENARIO 1: Filter by specific currency (show in that currency)
            if currency and not with_base_currency:
                # Convert currency to integer to match the keys in currency_balances
                currency_int = int(currency)

                if currency_int in currency_balances:
                    original_balance = currency_balances[currency_int]
                    rounded_balance = round(original_balance, 2)

                    formatted_cash_balances.append({
                        "category": account.categories.category if account.categories else None,
                        "subcategory": account.sub_category,
                        "subcategory_id": account.id,
                        "amount": float(rounded_balance),
                        "currency_id": currency_int  # Use integer here too
                    })
                    total_cash_balance += rounded_balance

            # SCENARIO 2: Show all currencies converted to base currency
            else:
                account_balance_base = Decimal('0.0')

                for currency_id, original_balance in currency_balances.items():
                    if currency_id == base_currency_id:
                        # No conversion needed
                        converted_balance = original_balance
                    else:
                        # Convert using latest exchange rate
                        try:
                            rate = get_exchange_rate(
                                db_session,
                                currency_id,
                                base_currency_id,
                                app_id,
                                end_date
                            )
                            converted_balance = original_balance * Decimal(str(rate))
                        except Exception as e:
                            logger.error(f"Currency conversion error: {str(e)}")
                            converted_balance = Decimal('0.0')

                    account_balance_base += converted_balance

                # Round final balance
                rounded_balance = round(account_balance_base, 2)

                if rounded_balance != 0:
                    formatted_cash_balances.append({
                        "category": account.categories.category if account.categories else None,
                        "subcategory": account.sub_category,
                        "subcategory_id": account.id,
                        "amount": float(rounded_balance),
                        "currency_id": base_currency_id  # Base currency for converted amounts
                    })
                    total_cash_balance += rounded_balance

        return formatted_cash_balances, float(total_cash_balance)

    except Exception as e:
        logger.error(f"Error in get_cash_balances_with_base: {str(e)}", exc_info=True)
        return [], 0.0


def get_bank_balances(db_session, app_id, currency=None, start_date=None, end_date=None):
    """Fetch bank balances in their original currencies without conversion"""
    logger.debug("Calculating bank balances (original currency)")

    # Build query
    query = db_session.query(Transaction).filter_by(app_id=app_id)

    if currency:
        query = query.filter_by(currency=currency)
    if start_date:
        query = query.filter(Transaction.date >= start_date)
    if end_date:
        query = query.filter(Transaction.date <= end_date)

    transactions = query.all()

    # Get opening balances
    opening_query = db_session.query(Transaction).filter_by(app_id=app_id)
    if currency:
        opening_query = opening_query.filter_by(currency=currency)
    if start_date:
        opening_query = opening_query.filter(Transaction.date < start_date)
    opening_transactions = opening_query.all()

    # Filter bank transactions
    bank_transactions = [t for t in transactions if t.chart_of_accounts and t.chart_of_accounts.is_bank]
    opening_bank = [t for t in opening_transactions if t.chart_of_accounts and t.chart_of_accounts.is_bank]

    # Group by currency and account
    balances = {}
    opening_balances = {}

    for t in bank_transactions:
        key = (t.currency, t.chart_of_accounts.id)
        if key not in balances:
            balances[key] = {
                "account_id": t.chart_of_accounts.id,
                "account_name": t.chart_of_accounts.name,
                "currency": t.currency,
                "debit": Decimal('0'),
                "credit": Decimal('0')
            }

        if t.dr_cr == "D":
            balances[key]["debit"] += t.amount
        else:
            balances[key]["credit"] += t.amount

    for t in opening_bank:
        key = (t.currency, t.chart_of_accounts.id)
        if key not in opening_balances:
            opening_balances[key] = {
                "debit": Decimal('0'),
                "credit": Decimal('0')
            }

        if t.dr_cr == "D":
            opening_balances[key]["debit"] += t.amount
        else:
            opening_balances[key]["credit"] += t.amount

    # Format results
    result = []
    for key in balances:
        currency = key[0]
        account_id = key[1]
        opening = opening_balances.get(key, {"debit": Decimal('0'), "credit": Decimal('0')})

        net = (balances[key]["debit"] + opening["debit"]) - \
              (balances[key]["credit"] + opening["credit"])

        result.append({
            "account_id": account_id,
            "account_name": balances[key]["account_name"],
            "currency": currency,
            "amount": float(net),
            "original_amount": float(balances[key]["debit"] - balances[key]["credit"]),
            "opening_balance": float(opening["debit"] - opening["credit"])
        })

    return result


def update_balances(transaction, balances_dict, with_base_currency, base_currency, db_session, app_id, end_date=None):
    """
    Update balances dictionary with transaction amounts.
    Works with Journal + JournalEntry structure.
    """
    # Determine whether input is a JournalEntry
    entry = transaction
    journal = getattr(entry, "journal", None)

    if not journal or not entry.chart_of_accounts or not entry.chart_of_accounts.categories:
        return

    cat_subcat_key = (
        entry.chart_of_accounts.categories.category,
        entry.chart_of_accounts.sub_category,
        entry.chart_of_accounts.id
    )

    if cat_subcat_key not in balances_dict:
        balances_dict[cat_subcat_key] = {"debit": Decimal('0.0'), "credit": Decimal('0.0')}

    amount = Decimal(str(entry.amount))

    # Convert to base currency if needed
    if with_base_currency and journal.currency_id != base_currency:
        exchange_rate = journal.exchange_rate.rate if journal.exchange_rate else Decimal('1.0')
        amount = amount * exchange_rate

    if entry.dr_cr == "D":
        balances_dict[cat_subcat_key]["debit"] += amount
    else:
        balances_dict[cat_subcat_key]["credit"] += amount


def apply_date_filters(query, start_date, end_date, filter_type, model, date_field, date_added_field):
    """
    Apply date filters to a query based on the provided filter type and model.

    Parameters:
    - query: The query object to which the filters will be applied.
    - start_date (str): The start date in 'YYYY-MM-DD' format.
    - end_date (str): The end date in 'YYYY-MM-DD' format.
    - filter_type (str): The type of date filter to apply ('transaction_date' or 'date_added').
    - model: The model class (e.g., Transaction or Inventory) to filter on.
    - date_field (str): The name of the date field to filter on.
    - date_added_field (str): The name of the date added field to filter on.

    Returns:
    - query: The modified query with the date filters applied.
    """
    if start_date or end_date:
        try:
            if start_date:
                start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            if end_date:
                end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

            if filter_type == 'transaction_date':
                if start_date:
                    query = query.filter(getattr(model, date_field) >= start_date)
                if end_date:
                    query = query.filter(getattr(model, date_field) <= end_date)
            elif filter_type == 'date_added':
                if start_date:
                    query = query.filter(getattr(model, date_added_field) >= start_date)
                if end_date:
                    query = query.filter(getattr(model, date_added_field) <= end_date)

        except ValueError as e:
            logger.error(f"Error parsing date: {e}")
            # Handle the error as needed (e.g., raise an exception, return an error message, etc.)

    return query


def create_transaction(
        db_session,
        date,
        currency,
        created_by,
        app_id,
        journal_number=None,
        journal_ref_no=None,
        narration=None,
        payment_mode_id=None,
        project_id=None,
        vendor_id=None,
        exchange_rate_id=None,
        status='Posted',
        lines=None
        # List of dicts: [{"subcategory_id":..., "amount":..., "dr_cr":..., "description":..., "source_type":..., "source_id":...}]
):
    """
    Create a journal with multiple journal entries in a single transaction.

    :param db_session: SQLAlchemy session
    :param date: Journal date
    :param currency: Currency ID
    :param created_by: User ID who creates the journal
    :param app_id: Company ID
    :param journal_number: Optional journal number; auto-generated if None
    :param journal_ref_no: Optional journal reference
    :param narration: Optional journal narration
    :param payment_mode_id: Optional payment mode ID
    :param project_id: Optional project ID
    :param vendor_id: Optional vendor ID
    :param exchange_rate_id: Optional exchange rate ID
    :param status: Journal status (default 'Posted')
    :param lines: List of journal entry lines
    :return: Journal instance and list of JournalEntry instances
    """
    if not lines:
        raise ValueError("At least one line is required to create a journal.")

    # Generate journal number if not provided
    if not journal_number:
        # You might want to use your existing journal number generation logic here
        journal_number = generate_unique_journal_number(db_session, app_id)

    # 1️⃣ Create journal header
    journal = Journal(
        journal_number=journal_number,
        journal_ref_no=journal_ref_no,
        narration=narration,
        date=date,
        payment_mode_id=payment_mode_id,
        project_id=project_id,
        vendor_id=vendor_id,
        currency_id=currency,
        created_by=created_by,
        updated_by=created_by,
        app_id=app_id,
        exchange_rate_id=exchange_rate_id,
        status=status
    )
    db_session.add(journal)
    db_session.flush()  # Get journal.id

    # 2️⃣ Create journal entries
    entries = []
    for idx, line in enumerate(lines, start=1):
        entry = JournalEntry(
            journal_id=journal.id,
            line_number=idx,
            journal_number=journal.journal_number,
            app_id=app_id,
            date=date,
            subcategory_id=line.get("subcategory_id"),
            amount=line.get("amount"),
            dr_cr=line.get("dr_cr"),
            description=line.get("description"),
            source_type=line.get("source_type"),
            source_id=line.get("source_id")
        )
        db_session.add(entry)
        entries.append(entry)

    db_session.flush()

    # 3️⃣ Update journal totals
    journal.update_totals()
    db_session.flush()

    return journal, entries


def validate_date(date_str, is_start_date=False, compare_date=None):
    """
    Validates the given date string:
    - Ensures it follows the 'YYYY-MM-DD' format.
    - For start dates, checks that it is not in the future.
    - For end dates, checks that it is not earlier than the start date.
    """
    try:
        # Convert the input date_str to a datetime.datetime object
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d')

        # If compare_date is provided, convert it to datetime if it's a datetime.date
        if isinstance(compare_date, datetime.date) and not isinstance(compare_date, datetime.datetime):
            compare_date = datetime.datetime.combine(compare_date, datetime.datetime.min.time())  # Convert to datetime

        # If it's a start date, ensure it's not in the future
        if is_start_date and date > datetime.datetime.now():
            raise ValueError("Start date cannot be in the future.")

        # If it's an end date, ensure it's not earlier than the start date
        if not is_start_date and compare_date and date < compare_date:
            raise ValueError("End date cannot be earlier than start date.")

        return date

    except ValueError as e:
        raise ValueError(f"Invalid date format or logic error: {e}")


def generate_unique_lot(app_id, db_session):
    """Generate a unique lot number in the format 'Lot-timestamp-random'."""
    while True:
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        random_suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        lot = f"Lot-{timestamp}-{random_suffix}"

        # Check uniqueness
        existing_lot = db_session.query(InventoryEntry).filter_by(lot=lot, app_id=app_id).first()
        if not existing_lot:
            return lot


from threading import Lock

journal_lock = Lock()


def generate_unique_journal_number(db_session, app_id):
    with journal_lock:  # Ensures only one thread generates numbers at a time
        today = datetime.datetime.now().strftime('%Y%m%d')
        prefix = f"JN-{today}-"

        max_seq = db_session.query(
            func.max(
                func.cast(
                    func.substring(
                        Journal.journal_number,
                        len(prefix) + 1,
                        6
                    ),
                    Integer
                )
            )
        ).filter(
            Journal.journal_number.like(f"{prefix}%"),
            Journal.app_id == app_id
        ).scalar() or 0

        return f"{prefix}{(max_seq + 1):06d}"


expense_lock = Lock()  # Global thread-safe lock for expense numbers


def generate_unique_expense_number(db_session, app_id):
    with expense_lock:
        today = datetime.datetime.now().strftime('%Y%m%d')
        prefix = f"EX-{today}-"

        max_seq = db_session.query(
            func.max(
                func.cast(
                    func.substring(
                        ExpenseTransaction.expense_entry_number,
                        len(prefix) + 1,
                        6
                    ),
                    Integer
                )
            )
        ).filter(
            ExpenseTransaction.expense_entry_number.like(f"{prefix}%"),
            ExpenseTransaction.app_id == app_id
        ).scalar() or 0

        return f"{prefix}{(max_seq + 1):06d}"


def get_or_create_batch(app_id, lot, db_session):
    """Get an existing batch or create a new one.

    Args:
        app_id: The application ID
        lot: The batch/lot number (case-insensitive match)
        db_session: The database session to use

    Returns:
        Batch: The existing or newly created batch record

    Raises:
        Exception: If there's a database error
    """
    try:
        # Validate inputs
        if not app_id or not lot:
            raise ValueError("app_id and lot must be provided")

        if not isinstance(lot, str):
            raise TypeError("lot must be a string")

        # Use ilike for case-insensitive matching
        batch = db_session.query(Batch).filter(
            Batch.app_id == app_id,
            Batch.batch_number.ilike(lot)  # Case-insensitive matching
        ).first()

        if not batch:
            # Create a new batch if not found
            batch = Batch(app_id=app_id, batch_number=lot)
            db_session.add(batch)
            db_session.commit()
            # Refresh to get any database defaults
            db_session.refresh(batch)

        return batch

    except Exception as e:
        # Rollback in case of any error
        db_session.rollback()
        # Re-raise the exception for the caller to handle
        raise


def validate_quantity_and_price(quantity, unit_price):
    if quantity is None or quantity == "":
        raise ValueError("Quantity is empty or not provided.")
    try:
        quantity = float(quantity)
    except (ValueError, TypeError):
        raise ValueError("Quantity must be a valid number.")
    if quantity <= 0:
        raise ValueError("Quantity must be a positive number.")

    if unit_price is None or unit_price == "":
        raise ValueError("Unit Price is empty or not provided.")
    try:
        unit_price = float(unit_price)
    except (ValueError, TypeError):
        raise ValueError("Unit Price must be a valid number.")
    if unit_price <= 0:
        raise ValueError("Unit Price must be a positive number.")


def validate_quantity_and_selling_price(quantity, selling_price):
    if quantity is None or quantity == "":
        raise ValueError("Quantity is empty or not provided.")
    try:
        quantity = float(quantity)
    except (ValueError, TypeError):
        raise ValueError("Quantity must be a valid number.")
    if quantity <= 0:
        raise ValueError("Quantity must be a positive number.")

    if selling_price is None or selling_price == "":
        raise ValueError("Unit Price is empty or not provided.")
    try:
        selling_price = float(selling_price)
    except (ValueError, TypeError):
        raise ValueError("Unit Price must be a valid number.")
    if selling_price < 0:
        raise ValueError("Unit Price must be a positive number.")


def ensure_default_location(db_session, app_id):
    """
    Ensure that "Main Warehouse" exists in the InventoryLocation table for the given app_id.
    If it doesn't exist, create it.

    Returns:
        int: ID of the default location.
    """
    default_location_name = "Main Warehouse"

    try:
        # Check if "Main Warehouse" already exists for the given app_id
        main_warehouse = db_session.query(InventoryLocation).filter_by(
            location=default_location_name, app_id=app_id
        ).first()

        if not main_warehouse:
            # Create "Main Warehouse" if it doesn't exist
            main_warehouse = InventoryLocation(
                app_id=app_id,
                location=default_location_name,
                description="Main Warehouse"
            )
            db_session.add(main_warehouse)
            db_session.commit()

        return main_warehouse.id

    except Exception as e:
        db_session.rollback()  # Rollback in case of error
        return None  # Or handle the error as needed

    finally:
        db_session.close()  # Ensure the session is closed


def generate_next_batch_number(db_session):
    app_id = current_user.app_id  # Assuming `current_user` is available and has `app_id`

    # Get current month and last two digits of the year
    now = datetime.datetime.now()
    month_year = now.strftime("%m%y")  # e.g., "0425" for April 2025

    # Query the last inventory item for the company
    last_batch = db_session.query(Batch).filter(
        Batch.app_id == app_id,
        Batch.batch_number.startswith("INVNT-")
    ).order_by(Batch.batch_number.desc()).first()

    if last_batch:
        try:
            last_month_year = last_batch.batch_number.split('-')[1]
        except IndexError:
            last_month_year = ""

        if last_month_year == month_year:
            try:
                last_number_part = last_batch.batch_number.split('-')[-1]
                next_number = int(last_number_part) + 1
            except ValueError:
                next_number = 1
        else:
            next_number = 1
    else:
        next_number = 1  # Start with 1 if no previous inventory exists

    sequence_number = str(next_number).zfill(5)  # Format: 00001, 00002, etc.
    return f"INVNT-{month_year}-{sequence_number}"


def handle_batch_variation_update(db_session, movement_type, old_values, new_values, app_id):
    """Handle batch variation updates for all movement types"""
    if movement_type == "in":
        # Convert values to consistent types for comparison
        old_loc = str(old_values['to_location']) if old_values['to_location'] is not None else None
        new_loc = str(new_values['to_location']) if new_values['to_location'] is not None else None
        old_batch = str(old_values['lot']) if old_values['lot'] is not None else None
        new_batch = str(new_values['lot']) if new_values['lot'] is not None else None

        quantity_delta = new_values['quantity'] - old_values['quantity']
        logger.info(f'Old values are {old_values} and New values are {new_values}')

        # Check if there are any relevant changes
        has_changes = (
                quantity_delta != 0 or
                old_batch != new_batch or
                old_loc != new_loc
        )

        if has_changes:
            logger.info(f'Has changes with changes {has_changes}')
            # Check if batch/location changed
            same_batch_location = (old_batch == new_batch and old_loc == new_loc)

            if same_batch_location:
                # For same batch/location, calculate net effect first
                current_qty = db_session.query(BatchVariationLink.quantity).filter_by(
                    batch_id=old_values['lot'],
                    location_id=old_values['to_location'],
                    item_id=old_values['item_id'],
                    app_id=app_id
                ).scalar()

                logger.info(f'Current quantity {current_qty}')

                if current_qty is None:
                    raise ValueError("Original batch/location not found")

                net_effect = current_qty - old_values['quantity'] + new_values['quantity']
                if net_effect < 0:
                    raise ValueError(f"Editing would leave negative inventory ({net_effect})")

                # Apply net change directly
                update_batch_link(
                    db_session=db_session,
                    batch_id=old_values['lot'],
                    location_id=old_values['to_location'],
                    item_id=old_values['item_id'],
                    adjustment=quantity_delta,  # Net change
                    app_id=app_id,
                    create_if_missing=False
                )

                logger.info(f'Applied update batch and net effect is {net_effect}')
            else:
                # For different batch/location, use transaction
                with db_session.begin_nested():
                    # Temporarily disable negative check for first operation
                    update_batch_link(
                        db_session=db_session,
                        batch_id=old_values['lot'],
                        location_id=old_values['to_location'],
                        item_id=old_values['item_id'],
                        adjustment=-old_values['quantity'],
                        app_id=app_id,
                        create_if_missing=False,
                        allow_negative=True  # Temporary
                    )

                    update_batch_link(
                        db_session=db_session,
                        batch_id=new_values['lot'],
                        location_id=new_values['to_location'],
                        item_id=new_values['item_id'],
                        adjustment=new_values['quantity'],
                        unit_price=new_values.get('unit_price'),
                        supplier_id=new_values.get('supplier_id'),
                        uom=new_values.get('uom'),
                        expiration_date=new_values.get('expiration_date'),
                        transaction_date=new_values.get('transaction_date'),
                        app_id=app_id,
                        create_if_missing=True
                    )


        else:
            # No quantity/location/batch changes, but might need to update other fields
            logger.info("No quantity, batch or location changes detected for 'in' movement")

            # Update metadata fields if they exist in the batch link
            update_batch_link(
                db_session=db_session,
                batch_id=old_values['lot'],
                location_id=old_values['to_location'],
                item_id=old_values['item_id'],
                adjustment=0,  # No quantity change
                unit_price=new_values.get('unit_price'),
                currency_id=new_values.get('currency_id'),
                supplier_id=new_values.get('supplier_id'),
                uom=new_values.get('uom'),
                expiration_date=new_values.get('expiration_date'),
                transaction_date=new_values.get('transaction_date'),
                app_id=app_id,
                create_if_missing=False
            )

    elif movement_type in ["out", "missing"]:
        # Check if item, batch or location changed
        item_changed = old_values['item_id'] != new_values['item_id']
        batch_changed = old_values['lot'] != new_values['lot']
        location_changed = old_values['from_location'] != new_values['from_location']
        logger.info(f'Batch changed is {batch_changed} and location is {location_changed}')
        if item_changed or batch_changed or location_changed:
            # FIRST VALIDATE THE NEW LOCATION/BATCH HAS ENOUGH STOCK
            new_batch_link = db_session.query(BatchVariationLink).filter_by(
                batch_id=new_values['lot'],
                location_id=new_values['from_location'],
                item_id=new_values['item_id'],
                app_id=app_id
            ).first()

            if not new_batch_link or new_batch_link.quantity < new_values['quantity']:
                raise ValueError(
                    f"Insufficient quantity in new batch/location. "
                    f"Available: {new_batch_link.quantity if new_batch_link else 0}, "
                    f"Requested: {new_values['quantity']}"
                )

            # If validation passes, proceed with the update
            # First reverse the original missing entry
            update_batch_link(
                db_session=db_session,
                batch_id=old_values['lot'],
                location_id=old_values['from_location'],
                item_id=old_values['item_id'],
                adjustment=old_values['quantity'],  # Add back
                app_id=app_id,
                create_if_missing=False
            )

            # Then apply the new missing entry
            update_batch_link(
                db_session=db_session,
                batch_id=new_values['lot'],
                location_id=new_values['from_location'],
                item_id=new_values['item_id'],
                adjustment=-new_values['quantity'],  # Subtract
                unit_price=new_values['unit_price'],
                currency_id=new_values['currency_id'],
                uom=new_values['uom'],
                supplier_id=new_values['supplier_id'],
                expiration_date=new_values['expiration_date'],
                transaction_date=new_values.get('transaction_date'),
                app_id=app_id,
                create_if_missing=True
            )

        else:  # Only quantity changed
            current_link = db_session.query(BatchVariationLink).filter_by(
                batch_id=old_values['lot'],
                location_id=old_values['from_location'],
                item_id=old_values['item_id'],
                app_id=app_id
            ).first()

            if not current_link:
                raise ValueError("Original batch/location not found")

            available = current_link.quantity + old_values['quantity']
            if new_values['quantity'] > available:
                raise ValueError(
                    f"Cannot adjust. Available: {available}, "
                    f"Requested: {new_values['quantity']}"
                )

            # Apply net adjustment
            update_batch_link(
                db_session=db_session,
                batch_id=old_values['lot'],
                location_id=old_values['from_location'],
                item_id=old_values['item_id'],
                adjustment=(old_values['quantity'] - new_values['quantity']),
                transaction_date=new_values.get('transaction_date'),
                app_id=app_id,
                create_if_missing=False
            )

    elif movement_type == "transfer":
        # For transfers, adjust both source and destination
        # First reverse the original transfer
        update_batch_link(
            db_session=db_session,
            batch_id=old_values['lot'],
            location_id=old_values['from_location'],
            item_id=old_values['item_id'],
            adjustment=old_values['quantity'],
            transaction_date=new_values.get('transaction_date'),
            app_id=app_id,
            create_if_missing=False
        )
        update_batch_link(
            db_session=db_session,
            batch_id=old_values['lot'],
            location_id=old_values['to_location'],
            item_id=old_values['item_id'],
            adjustment=-old_values['quantity'],
            transaction_date=new_values.get('transaction_date'),
            app_id=app_id,
            create_if_missing=False
        )

        # Then apply the new transfer
        update_batch_link(
            db_session=db_session,
            batch_id=new_values['lot'],
            location_id=new_values['from_location'],
            item_id=new_values['item_id'],
            adjustment=-new_values['quantity'],
            transaction_date=new_values.get('transaction_date'),
            app_id=app_id,
            create_if_missing=False
        )
        update_batch_link(
            db_session=db_session,
            batch_id=new_values['lot'],
            location_id=new_values['to_location'],
            item_id=new_values['item_id'],
            adjustment=new_values['quantity'],
            unit_price=new_values['unit_price'],
            currency_id=new_values['currency_id'],
            uom=new_values['uom'],
            supplier_id=new_values['supplier_id'],
            expiration_date=new_values['expiration_date'],
            transaction_date=new_values.get('transaction_date'),
            app_id=app_id,
            create_if_missing=True
        )


def update_batch_link(db_session, batch_id, location_id, item_id, adjustment, app_id,
                      unit_price=None, currency_id=None, uom=None, supplier_id=None,
                      expiration_date=None, transaction_date=None, create_if_missing=False, allow_negative=False):
    """Update or create a batch variation link with the given adjustment"""
    if not location_id:
        return

    link = db_session.query(BatchVariationLink).filter_by(
        batch_id=batch_id,
        location_id=location_id,
        item_id=item_id,
        app_id=app_id
    ).first()

    if not link and create_if_missing:
        link = BatchVariationLink(
            batch_id=batch_id,
            location_id=location_id,
            item_id=item_id,
            quantity=adjustment,  # Start with the adjustment value
            unit_cost=unit_price if unit_price is not None else 0,
            currency_id=currency_id if currency_id is not None else 1,  # Default currency
            supplier_id=supplier_id,
            uom=uom if uom is not None else 1,  # Default UOM
            expiry_date=expiration_date,
            transaction_date=transaction_date if transaction_date else datetime.datetime.now(),
            app_id=app_id
        )
        db_session.add(link)
    elif not link:
        raise ValueError(f"No inventory found for batch {batch_id} at location {location_id}")
    else:
        link.quantity += adjustment

    # Update other fields if provided
    if unit_price is not None:
        link.unit_cost = unit_price
    if currency_id is not None:
        link.currency_id = currency_id
    if uom is not None:
        link.uom = uom
    if expiration_date is not None:
        link.expiry_date = expiration_date
    if supplier_id is not None:
        link.supplier_id = supplier_id
    if transaction_date is not None:
        link.transaction_date = transaction_date

    if not allow_negative and link.quantity < 0:
        raise ValueError("Adjustment would result in negative inventory")


def check_exchange_rate_required(app_id, db_session):
    """
    Checks if the user has multiple currencies and whether exchange rates have been entered for today.

    Args:
        app_id (int): The application ID of the current user.

    Returns:
        tuple: (show_exchange_rate_modal, base_currency, currencies)
            - show_exchange_rate_modal (bool): Whether the modal should be shown.
            - base_currency (str): The user's base currency.
            - currencies (list): List of user's currencies.
    """

    try:
        # Retrieve all currencies for the user
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        user_currencies = [currency.user_currency for currency in currencies]

        # Base currency is the first currency
        base_currency = user_currencies[0] if user_currencies else None

        # If the user has more than one currency, check if exchange rates exist
        if len(user_currencies) > 1:
            today = datetime.datetime.today().date()
            rates_exist = db_session.query(ExchangeRate).filter_by(
                app_id=app_id,
                date=today
            ).first()

            # If rates do not exist, set flag to show exchange rate modal
            show_exchange_rate_modal = rates_exist is None
            return show_exchange_rate_modal, base_currency, currencies

        # If the user has only one currency, no need to show modal
        return False, base_currency, currencies

    finally:
        # Ensure the session is closed
        db_session.close()


def calculate_available_quantity(db_session, app_id, item_id, location_id):
    """
    Calculate available quantity for an inventory item at a specific location.
    """
    # Input validation
    if None in (app_id, item_id, location_id):
        raise ValueError("app_id, item_id, and location_id are required parameters")

    try:
        # Debug: Log the parameters
        logger.info(f"Calculating quantity for app_id={app_id}, item_id={item_id}, location_id={location_id}")

        # Build the query step by step for debugging
        query = db_session.query(
            func.coalesce(
                func.sum(
                    case(
                        (InventoryEntry.to_location == location_id, InventoryEntryLineItem.quantity),
                        else_=0
                    )
                ), 0
            ) - func.coalesce(
                func.sum(
                    case(
                        (InventoryEntry.from_location == location_id, InventoryEntryLineItem.quantity),
                        else_=0
                    )
                ), 0
            )
        ).join(
            InventoryEntry,
            InventoryEntry.id == InventoryEntryLineItem.inventory_entry_id
        ).filter(
            InventoryEntryLineItem.item_id == item_id,
            InventoryEntry.app_id == app_id
        )

        # Debug: Print the generated SQL
        logger.info(f"SQL Query: {str(query)}")

        available_qty = query.scalar()

        logger.info(f"Raw result: {available_qty}")
        return available_qty if available_qty is not None else 0

    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}\n{traceback.format_exc()}")
        raise DatabaseError("Failed to calculate available quantity") from e


def calculate_batch_available_quantity(db_session, app_id, item_id, variation, from_location, batch_lot):
    """Calculate the available quantity for a specific batch."""
    return db_session.query(
        func.coalesce(func.sum(
            case(
                (InventoryEntry.to_location == from_location, InventoryEntry.quantity),
                else_=0
            )
        ), 0)
        -
        func.coalesce(func.sum(
            case(
                (InventoryEntry.from_location == from_location, InventoryEntry.quantity),
                else_=0
            )
        ), 0)
    ).filter(
        InventoryEntry.app_id == app_id,
        InventoryEntry.item_id == item_id,
        InventoryEntry.lot == batch_lot  # Filter by the current batch
    ).scalar()


def calculate_net_quantity(item_id, app_id, db_session):
    """
    Calculate the net available quantity for an item across all locations,
    considering both incoming (to_location) and outgoing (from_location) quantities.
    """
    # Calculate total incoming quantities across all locations
    total_incoming = db_session.query(func.sum(InventoryEntry.quantity)).filter_by(
        item_id=item_id,
        app_id=app_id
    ).filter(
        InventoryEntry.to_location.isnot(None)  # Only consider entries with a to_location
    ).scalar() or 0

    # Calculate total outgoing quantities across all locations
    total_outgoing = db_session.query(func.sum(InventoryEntry.quantity)).filter_by(
        item_id=item_id,
        app_id=app_id
    ).filter(
        InventoryEntry.from_location.isnot(None)  # Only consider entries with a from_location
    ).scalar() or 0

    # Calculate net quantity
    net_quantity = total_incoming - total_outgoing

    return net_quantity


def convert_to_base_currency(amount, from_currency_id, app_id, db_session):
    try:
        today = datetime.datetime.today()
        base_currency_id = db_session.query(ExchangeRate.to_currency_id).filter_by(
            app_id=app_id,
            from_currency_id=from_currency_id,
            date=today
        ).first()

        if not base_currency_id:
            raise Exception('Exchange rate not found for currency conversion.')

        exchange_rate = db_session.query(ExchangeRate.rate).filter_by(
            app_id=app_id,
            from_currency_id=from_currency_id,
            to_currency_id=base_currency_id,
            date=today
        ).scalar()

        if not exchange_rate:
            raise Exception('Exchange rate not found for the specified currencies.')

        return amount * exchange_rate
    except Exception as e:
        logger.error(f"Error converting currency: {str(e)}")
        raise e
    finally:
        db_session.close()


def generate_next_backorder_number(db_session, app_id):
    """Generate Delivery Note number: DN-MMYY-XXXXX"""
    return generate_sequence_number(
        prefix="DN",
        table_model=Backorder,
        number_field="backorder_number",
        db_session=db_session,  # ← ADD THIS
        app_id=app_id  # ← ADD THIS
    )


# My original CALCULATE FIFO COGS
# def calculate_fifo_cogs(db_session, item_id, quantity_sold, app_id, source_type, source_id,
#                         customer_id, uom=None):
#     """Calculate COGS using FIFO in original currencies"""
#     try:
#
#         remaining_quantity = Decimal(quantity_sold)
#         cogs_details = []
#
#         # 1. Get or create latest batch reference
#         latest_batch = db_session.query(BatchVariationLink).filter(
#             BatchVariationLink.app_id == app_id,
#             BatchVariationLink.item_id == item_id,
#         ).order_by(
#             BatchVariationLink.transaction_date.desc()
#         ).first()
#
#         # 2. Process available stock
#
#         batches = db_session.query(BatchVariationLink).filter(
#             BatchVariationLink.app_id == app_id,
#             BatchVariationLink.item_id == item_id,
#             BatchVariationLink.quantity > 0
#         ).order_by(
#             BatchVariationLink.transaction_date.asc(),
#             BatchVariationLink.id.asc()
#         ).with_for_update().all()
#
#         for i, batch in enumerate(batches, 1):
#             if remaining_quantity <= 0:
#                 break
#
#             deduct = min(remaining_quantity, Decimal(str(batch.quantity)))
#
#             cogs_details.append({
#                 'batch_id': batch.batch_id,
#                 'location_id': batch.location_id,
#                 'deduct_quantity': float(deduct),
#                 'unit_cost': float(batch.unit_cost),
#                 'currency_id': batch.currency_id,  # Original currency
#                 'uom': batch.uom,
#                 'supplier_id': batch.supplier_id,
#                 'batch_entry_id': batch.id,
#                 'total_cost': float(Decimal(batch.unit_cost) * deduct)
#
#             })
#
#             remaining_quantity -= deduct
#
#         # 3. Handle backorder if needed (unchanged)
#         backorder_id = None
#         if remaining_quantity > 0:
#             backorder_number = generate_next_backorder_number(db_session)
#
#             backorder = Backorder(
#                 backorder_number=backorder_number,
#                 source_type=source_type,
#                 source_id=source_id,
#                 customer_id=customer_id,
#                 backorder_date=func.now(),
#                 status='pending',
#                 created_by=current_user.id,
#                 app_id=app_id
#             )
#             db_session.add(backorder)
#             db_session.flush()
#             backorder_id = backorder.id
#             backorder_item = BackorderItem(
#                 backorder_id=backorder_id,
#                 item_id=item_id,
#                 original_quantity=float(remaining_quantity),
#                 fulfilled_quantity=0,
#                 remaining_quantity=float(remaining_quantity),
#                 uom_id=(latest_batch.uom if latest_batch else uom),
#                 status='pending',
#                 app_id=app_id
#             )
#             db_session.add(backorder_item)
#             db_session.flush()
#
#         return cogs_details  # Now returns just the details
#
#     except Exception as e:
#         logger.error(f"Error message: {str(e)}")
#         db_session.rollback()
#         raise e
#


def calculate_fifo_cogs(db_session, item_id, quantity_sold, app_id, source_type, source_id,
                        customer_id, invoice_location_id=None, uom=None):
    """Calculate COGS with location priority using your exact model structure"""
    try:
        remaining_quantity = Decimal(quantity_sold)
        cogs_details = []
        notifications = []
        # Get the item variation link with all related data
        item_link = db_session.query(InventoryItemVariationLink).options(
            joinedload(InventoryItemVariationLink.inventory_item),
            joinedload(InventoryItemVariationLink.inventory_item_variation)
        ).filter(
            InventoryItemVariationLink.id == item_id,
            InventoryItemVariationLink.app_id == app_id
        ).first()

        if not item_link:
            raise ValueError(f"Inventory item variation link with ID {item_id} not found")

        item_name = item_link.inventory_item.item_name
        variation_name = item_link.inventory_item_variation.variation_name if item_link.inventory_item_variation else ""

        # Get latest batch for UOM/currency fallback (using your BatchVariationLink model)
        latest_batch = db_session.query(BatchVariationLink).filter(
            BatchVariationLink.app_id == app_id,
            BatchVariationLink.item_id == item_id,
        ).order_by(BatchVariationLink.transaction_date.desc()).first()
        logger.info(f'Invoice location id is {invoice_location_id}')
        # PHASE 1: Deduct from specified location if provided
        if invoice_location_id:
            location_batches = db_session.query(BatchVariationLink).filter(
                BatchVariationLink.app_id == app_id,
                BatchVariationLink.item_id == item_id,
                BatchVariationLink.location_id == invoice_location_id,
                BatchVariationLink.quantity > 0
            ).order_by(
                BatchVariationLink.transaction_date.asc()  # FIFO within location
            ).with_for_update().all()

            for batch in location_batches:
                if remaining_quantity <= 0:
                    break

                deduct = min(remaining_quantity, Decimal(str(batch.quantity)))
                cogs_details.append({
                    'batch_id': batch.batch_id,
                    'batch_number': batch.batch.batch_number,
                    'location_id': batch.location_id,
                    'deduct_quantity': float(deduct),
                    'unit_cost': float(batch.unit_cost),
                    'currency_id': batch.currency_id,  # From BatchVariationLink
                    'uom': batch.uom,  # From BatchVariationLink
                    'supplier_id': batch.supplier_id,
                    'batch_entry_id': batch.id,
                    'total_cost': float(Decimal(batch.unit_cost) * deduct),
                    'priority_location': True
                })
                remaining_quantity -= deduct

        # PHASE 2: Fall back to other locations
        if remaining_quantity > 0:
            other_batches = db_session.query(BatchVariationLink).filter(
                BatchVariationLink.app_id == app_id,
                BatchVariationLink.item_id == item_id,
                BatchVariationLink.quantity > 0,
                or_(
                    BatchVariationLink.location_id != invoice_location_id,
                    invoice_location_id == None  # ✅ Fixed
                )
            ).order_by(
                BatchVariationLink.transaction_date.asc()
            ).with_for_update().all()

            for batch in other_batches:
                if remaining_quantity <= 0:
                    break

                deduct = min(remaining_quantity, Decimal(str(batch.quantity)))
                cogs_details.append({
                    'batch_id': batch.batch_id,
                    'batch_number': batch.batch.batch_number,
                    'location_id': batch.location_id,
                    'deduct_quantity': float(deduct),
                    'unit_cost': float(batch.unit_cost),
                    'currency_id': batch.currency_id,
                    'uom': batch.uom,
                    'supplier_id': batch.supplier_id,
                    'batch_entry_id': batch.id,
                    'total_cost': float(Decimal(batch.unit_cost) * deduct),
                    'priority_location': False
                })
                remaining_quantity -= deduct

        # PHASE 3: Create backorder if needed
        if remaining_quantity > 0:
            backorder_qty = float(remaining_quantity)
            backorder = Backorder(
                backorder_number=generate_next_backorder_number(db_session),
                source_type=source_type,
                source_id=source_id,
                customer_id=customer_id,
                backorder_date=func.now(),
                status='pending',
                created_by=current_user.id,
                app_id=app_id
            )

            db_session.add(backorder)
            db_session.flush()

            back_order_item = BackorderItem(
                backorder_id=backorder.id,
                item_id=item_id,
                original_quantity=backorder_qty,
                fulfilled_quantity=0,
                remaining_quantity=backorder_qty,
                uom_id=uom or (latest_batch.uom if latest_batch else None),  # Prefer invoice UOM
                status='pending',
                app_id=app_id
            )
            db_session.add(back_order_item)

            logger.info(f'Back Order ID is {backorder.id}')

            # Create backorder notification
            flash(f"Backorder created for {backorder_qty} {item_name}" +
                  (f" ({variation_name})" if variation_name else "") +
                  f" (Item #{item_id}) - {remaining_quantity} units short", 'info')
            # Create backorder notification with proper item/variation names
            notifications.append(
                f"Backorder created for {backorder_qty} {item_name}" +
                (f" ({variation_name})" if variation_name else "") +
                f" (Item #{item_id}) - {remaining_quantity} units short"
            )
        # Create deduction notifications for each batch used
        for deduction in cogs_details:
            notifications.append(
                f"Deducted {deduction['deduct_quantity']} {item_name}" +
                (f" ({variation_name})" if variation_name else "") +
                f" from Batch {deduction['batch_number']} at Location {deduction['location_id']}"
            )

        # Create all notifications
        for message in notifications:
            create_notification(
                db_session=db_session,
                user_id=None,
                company_id=app_id,
                message=message,
                type='warning' if 'Backorder' in message else 'info',
                is_popup=True,
                url=f'/inventory/backorders'  # Link to item detail
            )
        logger.info(f'Cogs details are {cogs_details}')
        return cogs_details

    except Exception as e:
        logger.error(f"COGS Calculation Error: {str(e)}", exc_info=True)
        db_session.rollback()
        raise e


def update_inventory_quantity(db_session, item_id, quantity_sold, sales_invoice_id, created_by, app_id, cogs_details,
                              date=None):
    """Executes inventory deductions in original currencies"""
    with db_session.begin_nested():

        remaining_quantity = Decimal(quantity_sold)
        deductions_made = []

        if not cogs_details:
            remaining_quantity = Decimal('0')
            return True

        for plan in cogs_details:
            if remaining_quantity <= 0:
                break

            batch = db_session.query(BatchVariationLink).filter(
                BatchVariationLink.id == plan['batch_entry_id'],
                BatchVariationLink.quantity > 0
            ).with_for_update().first()

            if not batch:
                continue

            planned_deduct = Decimal(str(plan['deduct_quantity']))
            available = Decimal(str(batch.quantity))

            if available < planned_deduct:
                error_msg = (f"Batch {batch.batch_id} has {available} units, "
                             f"but {planned_deduct} were allocated in COGS calculation")
                logger.error(f"❌ {error_msg}")
                raise Exception(error_msg)

            # Perform deduction (in original currency)
            batch.quantity = float(available - planned_deduct)
            remaining_quantity -= planned_deduct

            deductions_made.append({
                'batch': batch.batch_id,
                'currency': batch.currency_id,
                'deducted': float(planned_deduct),
                'at_unit_cost': float(batch.unit_cost),
                'total_cost': float(planned_deduct * Decimal(str(batch.unit_cost))),
                'remaining_stock': float(batch.quantity)
            })

            # Create inventory entry in original currency
            inventory_entry = InventoryEntry(
                app_id=app_id,
                item_id=item_id,
                transaction_date=date,
                from_location=batch.location_id,
                stock_movement="out",
                quantity=float(planned_deduct),
                unit_price=Decimal(str(batch.unit_cost)),  # Original unit cost
                currency_id=batch.currency_id,  # Original currency
                uom=batch.uom,
                source_type="sales_invoice",
                source_id=sales_invoice_id,
                created_by=created_by,
                supplier_id=batch.supplier_id,
                lot=batch.batch_id,
                is_posted_to_ledger=True
            )
            db_session.add(inventory_entry)

        return True


def generate_next_purchase_order_number(db_session):
    app_id = current_user.app_id

    # Get current month and last two digits of the year
    now = datetime.datetime.now()
    month_year = now.strftime("%m%y")  # e.g., "0224" for February 2024

    # Query the last purchase_order for the company
    last_purchase_order = db_session.query(PurchaseOrder).filter_by(app_id=app_id).order_by(
        PurchaseOrder.purchase_order_number.desc()).first()

    if last_purchase_order:
        # Extract the last month-year part of the purchase_order number
        last_month_year = last_purchase_order.purchase_order_number.split('-')[1]

        if last_month_year == month_year:
            # If the month-year is the same, continue incrementing the sequence
            last_number_part = last_purchase_order.purchase_order_number.split('-')[-1]
            next_number = int(last_number_part) + 1
        else:
            # If the month has changed, restart numbering from 1
            next_number = 1
    else:
        next_number = 1  # Start with 1 if no previous purchase_order exists

    # Format the sequence number with leading zeros
    sequence_number = str(next_number).zfill(5)  # Ensures it remains 5 digits (00001, 00002, etc.)

    # Generate the new purchase_order number
    new_purchase_order_number = f"PO-{month_year}-{sequence_number}"

    return new_purchase_order_number


def allocate_payment(
        sale_transaction_id: int,
        invoice_id: int,
        payment_date,
        payment_amount: Decimal,
        db_session: Session,
        payment_mode: int,
        total_tax_amount: Decimal,
        payment_account: int,
        tax_payable_account_id: int,
        credit_sale_account: int,
        reference: Optional[str] = None
) -> PaymentAllocation:
    """
    Allocates a payment to an invoice proportionally between the base amount and tax amount.
    Considers both line-item taxes and the overall invoice tax.

    Args:
        sale_transaction_id (int): The ID of the sales transaction.
        invoice_id (int): The ID of the invoice.
        payment_amount (Decimal): The amount being paid.
        db_session (Session): The database session.
        payment_mode (int): The payment mode ID.
        total_tax_amount (Decimal): The total tax amount for the invoice.
        payment_account (int): The payment account ID.
        tax_payable_account_id (int): The tax payable account ID.
        credit_sale_account (int): The credit sale account ID.
        reference (str, optional): A reference for the payment. Defaults to None.

    Returns:
        PaymentAllocation: The newly created payment allocation record.
    """
    from ai import resolve_exchange_rate_for_transaction
    # Fetch the invoice
    invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id).first()
    if not invoice:
        raise ValueError("Invoice not found")

    # Validate payment amount
    if payment_amount <= 0:
        raise ValueError("Payment amount must be greater than 0.")

    # Validate invoice total amount
    if invoice.total_amount == 0:
        raise ValueError("Invoice total amount cannot be zero.")

    # Fetch all line items for the invoice
    line_items = db_session.query(SalesTransaction).filter_by(id=sale_transaction_id).all()

    # Convert payment amount to Decimal (if not already)
    payment_amount = Decimal(payment_amount)

    total_tax_amount = Decimal(total_tax_amount)

    # Calculate proportional allocation
    allocated_tax_amount = (total_tax_amount / Decimal(invoice.total_amount)) * payment_amount
    allocated_base_amount = payment_amount - allocated_tax_amount  # The rest goes to the base

    # Round to 2 decimal places
    allocated_tax_amount = allocated_tax_amount.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)
    allocated_base_amount = allocated_base_amount.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)

    currency_id = invoice.currency

    exchange_rate_id, notification = resolve_exchange_rate_for_transaction(session=db_session,
                                                                           currency_id=currency_id,
                                                                           transaction_date=payment_date,
                                                                           app_id=current_user.app_id)

    # Create payment allocation entry
    payment_allocation = PaymentAllocation(
        payment_id=sale_transaction_id,  # Assuming payment is linked to invoice
        direct_sale_id=None,
        payment_date=payment_date,
        invoice_id=invoice_id,
        allocated_base_amount=allocated_base_amount,
        allocated_tax_amount=allocated_tax_amount,
        payment_account=payment_account,
        tax_payable_account_id=tax_payable_account_id,
        credit_sale_account=credit_sale_account,
        created_at=datetime.datetime.now(),
        updated_at=datetime.datetime.now(),
        payment_mode=payment_mode,
        app_id=invoice.app_id,
        exchange_rate_id=exchange_rate_id,
        reference=reference  # Use the reference parameter (defaults to None if not provided)
    )

    db_session.add(payment_allocation)
    db_session.commit()

    return payment_allocation


def generate_next_goods_receipt_number(db_session=None, app_id=None):
    """Generate goods receipt number: GR-MMYY-XXXXX"""
    return generate_sequence_number(
        prefix="GR",
        table_model=GoodsReceipt,
        number_field="receipt_number",
        db_session=db_session,
        app_id=app_id
    )


def get_total_received_for_purchase(purchase_id, db_session, is_direct_purchase=True):
    """
    Calculates total received quantity for either a direct purchase or a purchase order.

    :param purchase_id: ID of the purchase (either direct or PO)
    :param db_session: SQLAlchemy session
    :param is_direct_purchase: Boolean flag, True if direct purchase, False if purchase order
    :return: Total quantity received as Decimal
    """
    query = db_session.query(func.sum(GoodsReceiptItem.quantity_received))

    if is_direct_purchase:
        query = query.filter(GoodsReceiptItem.direct_purchase_item_id.in_(
            db_session.query(DirectPurchaseItem.id).filter_by(transaction_id=purchase_id)
        ))
    else:
        query = query.filter(GoodsReceiptItem.purchase_order_item_id.in_(
            db_session.query(PurchaseOrderItem.id).filter_by(purchase_order_id=purchase_id)
        ))

    total_received = query.scalar() or Decimal('0.00')
    return total_received


def update_purchase_inventory_quantity(db_session, item_id, quantity_received, goods_receipt_id, created_by, app_id,
                                       batch_id=None, unit_cost=None, currency_id=None,
                                       uom=None, supplier_id=None, location_id=None, date=None):
    """Executes inventory additions for goods receipt in original currencies"""
    with db_session.begin_nested():

        # Create new batch entry
        if batch_id:
            existing_batch = db_session.query(Batch).filter(
                func.lower(Batch.batch_number) == batch_id.lower(),
                Batch.app_id == app_id
            ).first()
            if existing_batch:
                new_batch_id = existing_batch.id
            else:
                new_batch = Batch(batch_number=batch_id, app_id=app_id)
                db_session.add(new_batch)
                db_session.flush()
                new_batch_id = new_batch.id

        # Create inventory entry (for purchases we don't need batch-level tracking like sales)
        inventory_entry = InventoryEntry(
            app_id=app_id,
            item_id=item_id,
            transaction_date=date,
            from_location=None,
            to_location=location_id,  # For purchases, we're adding to a location
            stock_movement="in",  # Opposite of sales which uses "out"
            quantity=float(quantity_received),
            unit_price=Decimal(str(unit_cost)) if unit_cost else Decimal('0'),
            currency_id=currency_id,
            uom=uom,
            source_type="goods_receipt",
            source_id=goods_receipt_id,
            created_by=created_by,
            supplier_id=supplier_id,

            lot=new_batch_id,
            is_posted_to_ledger=True
        )
        db_session.add(inventory_entry)

        # Create batch variation link
        batch_variation_entry = BatchVariationLink(
            app_id=app_id,
            batch_id=new_batch_id,
            item_id=item_id,
            location_id=location_id,
            quantity=float(quantity_received),
            transaction_date=datetime.datetime.now().date(),
            unit_cost=Decimal(str(unit_cost)) if unit_cost else Decimal('0'),
            expiry_date=None,
            uom=uom,
            supplier_id=supplier_id,
            currency_id=currency_id
        )
        db_session.add(batch_variation_entry)

        return True


def get_or_create_default_location(db_session, app_id):
    location_id = db_session.query(InventoryLocation.id).filter_by(app_id=app_id).first()

    if not location_id:
        location_id = ensure_default_location(db_session, app_id)
    else:
        location_id = location_id[0]
    return location_id


def get_or_create_uom(db_session, app_id, unit_name, abbrev):
    """
    Looks up a UOM by full name for a given app_id. If not found, creates one.
    """
    uom = db_session.query(UnitOfMeasurement).filter_by(
        app_id=app_id,
        full_name=unit_name,
        abbreviation=abbrev
    ).first()

    if not uom:
        uom = UnitOfMeasurement(app_id=app_id, full_name=unit_name, abbreviation=unit_name)
        db_session.add(uom)
        db_session.commit()
    return uom


# Helper functions
def get_prepaid_payments_to_apply(goods_receipt_item, db_session):
    """Get all applicable prepaid payments for a goods receipt item with their available balances"""
    try:
        # Get all prepaid payments for this PO with available balance
        prepaid_payments = db_session.query(
            PurchasePaymentAllocation.payment_id,
            PurchasePaymentAllocation.prepaid_account_id,
            PurchaseTransaction.amount_paid,
            PurchaseTransaction.currency_id
        ).join(
            PurchaseTransaction,
            PurchasePaymentAllocation.payment_id == PurchaseTransaction.id
        ).filter(
            PurchaseTransaction.purchase_order_id == goods_receipt_item.purchase_order_item.purchase_orders.id,
            PurchasePaymentAllocation.payment_type == 'advance_payment',
            PurchasePaymentAllocation.prepaid_account_id.isnot(None)
        ).order_by(
            PurchasePaymentAllocation.is_posted_to_ledger.desc(),
            PurchaseTransaction.payment_date.asc()  # Older payments first
        ).all()

        if not prepaid_payments:
            return []

        # Calculate available balance for each prepaid payment
        payments_with_balance = []
        applied_amount = 0

        for payment in prepaid_payments:
            # Step 1: Get all PO item IDs for the purchase order
            purchase_order_id = goods_receipt_item.purchase_order_item.purchase_orders.id

            po_item_ids = db_session.query(PurchaseOrderItem.id).filter(
                PurchaseOrderItem.purchase_order_id == purchase_order_id
            ).all()

            po_item_ids = [item.id for item in po_item_ids]

            # Step 2: Get all goods receipt items linked to any of the PO items (only posted ones)
            related_receipt_items = db_session.query(GoodsReceiptItem.id).filter(
                GoodsReceiptItem.purchase_order_item_id.in_(po_item_ids),
                GoodsReceiptItem.is_posted_to_ledger == True
            ).all()

            receipt_item_ids = [item.id for item in related_receipt_items]

            # Calculate the available balance, passing the applied amount
            balance, already_applied_credit = get_prepaid_balance(payment.payment_id, receipt_item_ids, db_session,
                                                                  applied_amount, purchase_order_id)

            if balance > 0:
                payments_with_balance.append({
                    'payment_id': payment.payment_id,
                    'prepaid_account_id': payment.prepaid_account_id,
                    'available_balance': balance,
                })

                # Update the applied amounts dictionary if you are applying this payment
                # For example, if you decide to apply the full balance:
                applied_amount = already_applied_credit  # Update the applied amount

        return payments_with_balance

    except Exception as e:
        logger.error(f"[ERROR] Failed to get prepaid payments: {str(e)}")
        return []


def get_prepaid_balance(payment_id, goods_receipt_items, db_session, applied_amount, purchase_order_id):
    """Calculate available prepaid balance considering applications to specific goods receipts"""
    if not payment_id:
        return 0

    try:
        # First get the prepaid payment allocation record
        payment_allocation = db_session.query(PurchasePaymentAllocation) \
            .filter(PurchasePaymentAllocation.payment_id == payment_id) \
            .first()

        if not payment_allocation:
            logger.error("[WARNING] No payment allocation record found for payment")
            return 0

        prepaid_account_id = payment_allocation.prepaid_account_id
        payment_allocation_id = payment_allocation.id

        if not prepaid_account_id:
            logger.error("[WARNING] No prepaid account found for payment")
            return 0

        # Calculate total debits (initial prepayments)
        total_debits = db_session.query(
            func.sum(Transaction.amount)
        ).filter(
            Transaction.source_type == 'purchase_prepayment',
            Transaction.source_id == payment_allocation_id,
            Transaction.subcategory_id == prepaid_account_id,
            Transaction.dr_cr == 'D'
        ).scalar() or 0

        # Calculate total credits (applications against prepaid)
        # Filter by all relevant goods receipt items
        # Calculate total credits (applications against prepaid)
        total_credits = db_session.query(
            func.sum(Transaction.amount)
        ).filter(
            Transaction.source_type == 'prepaid_application_goods',
            Transaction.subcategory_id == prepaid_account_id,
            Transaction.dr_cr == 'C',
            Transaction.source_id.in_(goods_receipt_items)  # Filter by receipt items
        ).scalar() or 0

        # Calculate credits from goods receipts
        total_credits_by_po = db_session.query(
            func.sum(Transaction.amount)
        ).filter(
            Transaction.source_type == 'prepaid_application_shipping',
            Transaction.subcategory_id == prepaid_account_id,
            Transaction.dr_cr == 'C',
            Transaction.source_id == purchase_order_id
        ).scalar() or 0

        # Instead:
        balance = total_debits - (total_credits + total_credits_by_po - applied_amount)
        balance = max(balance, 0)  # Ensure balance doesn't go negative

        return balance, total_credits

    except Exception as e:
        logger.error(f"[ERROR] Failed to calculate prepaid balance: {str(e)}")
        return 0


def get_prepaid_balance_shipping(payment_id, prepaid_account_id, purchase_order_id, db_session):
    """
    Calculate available prepaid balance, accounting for applications to
    goods receipts and shipping charges on a purchase order.
    Returns (remaining_balance, total_goods_applied).
    """

    try:
        if not prepaid_account_id:
            logger.warning("[WARNING] No prepaid account found for payment")
            return 0, 0

        # Get related PO item IDs
        po_item_ids = db_session.query(PurchaseOrderItem.id).filter(
            PurchaseOrderItem.purchase_order_id == purchase_order_id
        ).all()
        po_item_ids = [id for (id,) in po_item_ids]

        # Get posted goods receipt item IDs
        receipt_item_ids = db_session.query(GoodsReceiptItem.id).filter(
            GoodsReceiptItem.purchase_order_item_id.in_(po_item_ids),
            GoodsReceiptItem.is_posted_to_ledger == True
        ).all()
        receipt_item_ids = [id for (id,) in receipt_item_ids]

        # Query helper
        def sum_transactions(source_type, source_filter, dr_cr):
            return db_session.query(func.sum(Transaction.amount)).filter(
                Transaction.source_type == source_type,
                Transaction.subcategory_id == prepaid_account_id,
                Transaction.dr_cr == dr_cr,
                source_filter
            ).scalar() or 0

        # Calculate debit (initial prepayment)
        total_debits = sum_transactions(
            'purchase_prepayment',
            Transaction.source_id == payment_id,
            'D'
        )

        # Credits applied to goods receipts
        total_credits_goods = sum_transactions(
            'prepaid_application_goods',
            Transaction.source_id.in_(receipt_item_ids),
            'C'
        )

        # Credits applied to shipping (against PO)
        total_credits_shipping = sum_transactions(
            'prepaid_application_shipping',
            Transaction.source_id == purchase_order_id,
            'C'
        )

        balance = max(total_debits - (total_credits_goods + total_credits_shipping), 0)

        return balance, total_credits_goods

    except Exception as e:
        logger.error(f"[ERROR] Failed to calculate prepaid balance: {str(e)}")
        return 0, 0


def apply_prepaid_payments(goods_receipt_item, gross_amount, prepaid_payments, db_session):
    """Apply prepaid payments to a goods receipt item until amount is covered or no more prepaid available"""
    applied_prepaid = []
    remaining_amount = Decimal(str(gross_amount))  # Ensure remaining_amount is Decimal

    for payment in prepaid_payments:
        if remaining_amount <= Decimal('0'):
            break

        # Convert available_balance to Decimal to avoid type mismatch
        available_balance = Decimal(str(payment['available_balance']))
        amount_to_apply = min(remaining_amount, available_balance)

        if amount_to_apply > Decimal('0'):
            applied_prepaid.append({
                'payment_id': payment['payment_id'],
                'prepaid_account_id': payment['prepaid_account_id'],
                'amount_applied': float(amount_to_apply),  # Convert back to float if needed by other functions
            })
            remaining_amount -= amount_to_apply

    return applied_prepaid, float(remaining_amount)  # Return as float if needed, else keep as Decimal


def get_first_prepaid_payment_id_by_po(purchase_order, db_session):
    """Get the first applicable purchase payment ID and prepaid account ID for a purchase order."""
    result = db_session.query(PurchasePaymentAllocation) \
        .join(PurchaseTransaction, PurchasePaymentAllocation.payment_id == PurchaseTransaction.id) \
        .filter(
        PurchaseTransaction.purchase_order_id == purchase_order.id,
        PurchasePaymentAllocation.payment_type == 'advance_payment',
        PurchasePaymentAllocation.prepaid_account_id.isnot(None)
    ) \
        .order_by(PurchasePaymentAllocation.is_posted_to_ledger.desc()) \
        .first()

    if result:
        return result.id, result.prepaid_account_id
    else:
        return None, None


def generate_direct_purchase_number(db_session):
    app_id = current_user.app_id  # Assuming `current_user` is available and has `app_id`

    # Get current month and last two digits of the year
    now = datetime.datetime.now()
    month_year = now.strftime("%m%y")  # e.g., "0325" for March 2025

    # Query the last direct purchase for the company
    last_purchase = db_session.query(DirectPurchaseTransaction).filter_by(app_id=app_id).order_by(
        DirectPurchaseTransaction.direct_purchase_number.desc()).first()

    if last_purchase and last_purchase.direct_purchase_number:
        # Extract the last month-year part of the purchase number
        try:
            last_month_year = last_purchase.direct_purchase_number.split('-')[1]
        except IndexError:
            last_month_year = None

        if last_month_year == month_year:
            # If the month-year is the same, continue incrementing the sequence
            last_number_part = last_purchase.direct_purchase_number.split('-')[-1]
            next_number = int(last_number_part) + 1
        else:
            # If the month has changed, restart numbering from 1
            next_number = 1
    else:
        next_number = 1  # Start with 1 if no previous purchase exists

    # Format the sequence number with leading zeros
    sequence_number = str(next_number).zfill(5)  # Ensures it remains 5 digits (00001, 00002, etc.)

    # Generate the new purchase number
    new_purchase_number = f"PUR-{month_year}-{sequence_number}"

    return new_purchase_number


def generate_next_return_number(db_session):
    app_id = current_user.app_id  # Assuming current_user is available

    # Get current month and last two digits of year
    now = datetime.datetime.now()
    month_year = now.strftime("%m%y")  # e.g. "0325" for March 2025

    # Query the last return for this company
    last_return = db_session.query(PurchaseReturn).filter_by(app_id=app_id).order_by(
        PurchaseReturn.return_number.desc()).first()

    if last_return:
        # Extract the month-year part from the return number
        try:
            last_month_year = last_return.return_number.split('-')[1]

            if last_month_year == month_year:
                # Same month - increment sequence number
                last_seq = int(last_return.return_number.split('-')[-1])
                next_number = last_seq + 1
            else:
                # New month - reset to 1
                next_number = 1
        except (IndexError, ValueError):
            # Handle unexpected format by resetting
            next_number = 1
    else:
        # First return for this company
        next_number = 1

    # Format with leading zeros (5 digits)
    sequence_number = str(next_number).zfill(5)

    # Generate new return number (format: PR-0325-00001)
    new_return_number = f"RET-{month_year}-{sequence_number}"

    return new_return_number


def update_purchase_return_inventory_quantity(db_session, item_id, quantity_returned, purchase_return_id, created_by,
                                              app_id, unit_cost, currency_id, uom, supplier_id,
                                              location_id, batch_id, date):
    """Executes inventory returns with batch-level tracking"""
    with db_session.begin_nested():

        # Lock batch record for update
        batch_record = db_session.query(BatchVariationLink).filter(
            BatchVariationLink.batch_id == batch_id,
            BatchVariationLink.location_id == location_id
        ).with_for_update().first()

        if not batch_record:
            error_msg = f"Batch {batch_id} not found at location {location_id}"
            logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)

        if Decimal(str(batch_record.quantity)) < Decimal(str(quantity_returned)):
            error_msg = (f"Cannot return {quantity_returned} units. Only {batch_record.quantity} available "
                         f"in batch {batch_id} at location {location_id}")

            raise ValueError(error_msg)

        # Create inventory movement record
        inventory_entry = InventoryEntry(
            app_id=app_id,
            item_id=item_id,
            transaction_date=date,
            from_location=location_id,
            stock_movement="out",
            quantity=float(quantity_returned),
            unit_price=Decimal(str(unit_cost)),
            currency_id=currency_id,
            uom=uom,
            source_type="purchase_return",
            source_id=purchase_return_id,
            created_by=created_by,
            supplier_id=supplier_id,
            lot=batch_id,
            is_posted_to_ledger=True
        )
        db_session.add(inventory_entry)

        # Update batch quantity
        batch_record.quantity = float(Decimal(str(batch_record.quantity)) - Decimal(str(quantity_returned)))

        return {
            'batch': batch_id,
            'location': location_id,
            'quantity': float(quantity_returned),
            'unit_cost': float(unit_cost),
            'currency': currency_id,
            'remaining': batch_record.quantity
        }


# Create Notification
def create_notification(db_session, user_id=None, company_id=None, message='', type='info', is_popup=False, url=None):
    scope = NotificationScope.user if user_id else NotificationScope.company
    new_notification = Notification(
        user_id=user_id,
        company_id=company_id,
        message=message,
        type=type,
        status=NotificationStatus.unread,
        is_popup=is_popup,
        url=url,
        scope=scope
    )
    db_session.add(new_notification)
    db_session.flush()  # Delays commit for batch operations

    return new_notification


def get_converted_cost(db_session, unit_cost, from_currency_id, to_currency_id, app_id, as_of_date=None):
    """
    Convert the unit cost from one currency to another using the exchange rate as of a specific date.
    """
    from ai import get_exchange_rate

    if from_currency_id == to_currency_id:
        return Decimal(str(unit_cost))

    rate = get_exchange_rate(db_session, from_currency_id, to_currency_id, app_id, as_of_date)

    if not rate:
        raise ValueError(f"Missing exchange rate from {from_currency_id} to {to_currency_id} as of {as_of_date}")

    return Decimal(str(unit_cost)) * Decimal(str(rate))


def get_first_accessible_module(user, db_session):
    """Returns the first module name the user has access to"""

    # Short-circuit for admins
    if user.role == 'Admin':
        return 'Inventory'

    module = db_session.query(Module.module_name) \
        .join(UserModuleAccess) \
        .filter(
        Module.included == "yes",
        UserModuleAccess.user_id == user.id,  # Use user.id from the object
        UserModuleAccess.can_view == True
    ) \
        .order_by(Module.priority.desc()) \
        .first()

    return module[0] if module else 'Sales'


def get_module_redirect_url(module_name, user_role):
    """Unified module routing with automatic contributor redirection"""

    module_routes = {
        'General Ledger': {
            'dashboard': 'general_ledger_routes.general_ledger_dashboard',
            'alt': 'general_ledger.journal_entries'
        },
        'Sales': {
            'dashboard': 'sales_routes.sales_dashboard',
            'alt': 'sales.view_sales_transactions'
        },
        'Purchases': {
            'dashboard': 'purchases_routes.purchases_dashboard',
            'alt': 'purchases.view_purchase_transactions'
        },
        'Inventory': {
            'dashboard': 'inventory.wms_dashboard',
            'alt': 'inventory.stock_movement_history'
        },
        'Reports': {
            'dashboard': 'reports_routes.reports_dashboard',
            'alt': 'financial_reports.balance_sheet'
        },
        'POS': {
            'dashboard': 'sales_routes.sales_dashboard',
            'alt': 'pos.pos_page'
        }
    }

    # Get module config or fallback to Sales
    module_config = module_routes.get(module_name, module_routes['Sales'])

    # Choose route based on role
    route_key = 'alt' if user_role == 'Contributor' else 'dashboard'

    return url_for(module_config.get(route_key, module_routes['Sales']['dashboard']))


def get_cash_flow_with_categories(db_session, app_id, currency_id, start_date, end_date):
    """Calculate cash flow data with hierarchical categories and subcategories"""
    currency = db_session.query(Currency).filter_by(id=currency_id, app_id=app_id).first()
    currency_symbol = currency.user_currency if currency else ""
    logger.info(f'I am here ')
    # Initialize result structure with the same format as balance sheet
    result = {
        'currency': currency_symbol,
        'operating_activities': {'categories': {}},
        'investing_activities': {'categories': {}},
        'financing_activities': {'categories': {}},
        'cash_at_beginning': 0,
        'cash_at_end': 0
    }

    # 1. Get all cash accounts (is_bank=True)
    cash_accounts = db_session.query(ChartOfAccounts).filter_by(
        app_id=app_id,
        is_bank=True
    ).all()

    # 2. Calculate cash at beginning and end of period
    for account in cash_accounts:
        # Beginning balance (all transactions before start_date)
        if start_date:
            beg_balance = db_session.query(
                func.sum(case(
                    [
                        (Transaction.dr_cr == 'D', Transaction.amount),
                        (Transaction.dr_cr == 'C', -Transaction.amount)
                    ],
                    else_=0
                ))
            ).filter(
                Transaction.account_id == account.id,
                Transaction.date < start_date,
                Transaction.currency_id == currency_id
            ).scalar() or 0
            result['cash_at_beginning'] += float(beg_balance)

        # Ending balance (all transactions up to end_date)
        end_balance = db_session.query(
            func.sum(case(
                [
                    (Transaction.dr_cr == 'D', Transaction.amount),
                    (Transaction.dr_cr == 'C', -Transaction.amount)
                ],
                else_=0
            ))
        ).filter(
            Transaction.account_id == account.id,
            Transaction.date <= end_date,
            Transaction.currency_id == currency_id
        ).scalar() or 0
        result['cash_at_end'] += float(end_balance)

    # 3. Get all transactions in the period with category info
    transactions = db_session.query(
        Transaction,
        ChartOfAccounts.category,
        ChartOfAccounts.sub_category,
        ChartOfAccounts.parent_account_type,
        ChartOfAccounts.is_bank
    ).join(
        ChartOfAccounts,
        Transaction.account_id == ChartOfAccounts.id
    ).filter(
        Transaction.app_id == app_id,
        Transaction.date >= start_date,
        Transaction.date <= end_date,
        Transaction.currency_id == currency_id
    ).all()

    # 4. Categorize transactions with hierarchical structure
    for tx, category, subcategory, parent_type, is_bank in transactions:
        if is_bank:
            continue  # Skip direct cash account transactions (already handled)

        amount = float(tx.amount) if tx.dr_cr == 'D' else -float(tx.amount)
        description = tx.description or f"{category} - {subcategory}"

        # Determine activity type based on account classification
        activity_type = None
        if parent_type in ['Current Assets', 'Current Liabilities'] or tx.transaction_type in ['Income', 'Expense']:
            activity_type = 'operating_activities'
        elif parent_type in ['Fixed Assets', 'Long-term Investments']:
            activity_type = 'investing_activities'
        elif parent_type in ['Long-term Liabilities', 'Equity']:
            activity_type = 'financing_activities'

        if activity_type:
            # Initialize category if not exists
            if category not in result[activity_type]['categories']:
                result[activity_type]['categories'][category] = {
                    'subcategories': {},
                    'total': 0
                }

            # Initialize subcategory if not exists
            if subcategory not in result[activity_type]['categories'][category]['subcategories']:
                result[activity_type]['categories'][category]['subcategories'][subcategory] = {
                    'total': 0,
                    'transactions': []
                }

            # Add transaction to subcategory
            result[activity_type]['categories'][category]['subcategories'][subcategory]['transactions'].append({
                'description': description,
                'amount': amount,
                'date': tx.date.strftime('%Y-%m-%d')
            })

            # Update totals
            result[activity_type]['categories'][category]['subcategories'][subcategory]['total'] += amount
            result[activity_type]['categories'][category]['total'] += amount

    # Calculate section totals
    for activity_type in ['operating_activities', 'investing_activities', 'financing_activities']:
        result[activity_type]['total'] = sum(
            cat['total'] for cat in result[activity_type]['categories'].values()
        )

    # Calculate overall net increase
    result['net_increase_cash'] = (
            result['operating_activities']['total'] +
            result['investing_activities']['total'] +
            result['financing_activities']['total']
    )

    logger.info(f'Result is {result}')

    return result


def format_amount(value):
    return f"({abs(value):,.2f})" if value < 0 else f"{value:,.2f}"


def get_item_name(receipt_item):
    """Safely extract item name from receipt item with fallbacks"""
    if receipt_item.purchase_order_item:
        item = receipt_item.purchase_order_item
        if hasattr(item, 'item_name') and item.item_name:
            return item.item_name
        if (hasattr(item, 'inventory_item_variation_link') and
                item.inventory_item_variation_link and
                item.inventory_item_variation_link.inventory_item):
            return item.inventory_item_variation_link.inventory_item.item_name

    if receipt_item.direct_purchase_item:
        item = receipt_item.direct_purchase_item
        if hasattr(item, 'item_name') and item.item_name:
            return item.item_name
        if (hasattr(item, 'inventory_item_variation_link') and
                item.inventory_item_variation_link and
                item.inventory_item_variation_link.inventory_item):
            return item.inventory_item_variation_link.inventory_item.item_name

    return "Unknown Item"


def calculate_fx_gain_loss(db_session, app_id, foreign_currency_id, base_currency_id,
                           exchange_date, foreign_account_id=None):
    """
    Calculates FX gain/loss for a foreign currency, optionally scoped to a specific account.

    Parameters:
        db_session: SQLAlchemy session
        app_id: App/tenant ID
        foreign_currency_id: ID of the foreign currency (e.g., USD)
        base_currency_id: ID of the base currency (e.g., UGX)
        exchange_date: Date up to which to calculate FX impact
        foreign_account_id: (Optional) ID of the foreign subcategory/account

    Returns:
        dict with:
            - foreign_balance: net balance in foreign currency
            - book_value: value at historical rates (base currency)
            - current_value: value at current rate (base currency)
            - fx_gain_or_loss: difference (current - book)
    """
    from models import Transaction, ExchangeRate
    from decimal import Decimal

    foreign_balance = Decimal('0.0')
    book_value = Decimal('0.0')

    # Step 1: Fetch historical transactions for book value (before exchange_date)
    historical_txns = db_session.query(Transaction).filter(
        Transaction.app_id == app_id,
        Transaction.currency == foreign_currency_id,
        Transaction.date < exchange_date  # Important: strictly less than exchange_date
    )

    # Step 2: Fetch all transactions up to and including exchange_date for current foreign balance
    current_txns = db_session.query(Transaction).filter(
        Transaction.app_id == app_id,
        Transaction.currency == foreign_currency_id,
        Transaction.date <= exchange_date
    )

    if foreign_account_id:
        historical_txns = historical_txns.filter(Transaction.subcategory_id == foreign_account_id)
        current_txns = current_txns.filter(Transaction.subcategory_id == foreign_account_id)

    historical_txns = historical_txns.order_by(Transaction.date).all()
    current_txns = current_txns.order_by(Transaction.date).all()

    for txn in historical_txns:
        direction = Decimal('1.0') if txn.dr_cr == 'D' else Decimal('-1.0')
        amount = Decimal(txn.amount) * direction
        if txn.exchange_rate_id and txn.exchange_rate:
            fx_rate = Decimal(txn.exchange_rate.rate)
        else:
            fx_rate = Decimal('1.0')  # Fallback
        book_value += amount * fx_rate

    for txn in current_txns:
        direction = Decimal('1.0') if txn.dr_cr == 'D' else Decimal('-1.0')
        amount = Decimal(txn.amount) * direction
        foreign_balance += amount

    # Step 3: Get latest exchange rate as of the exchange date
    latest_rate_obj = db_session.query(ExchangeRate).filter(
        ExchangeRate.app_id == app_id,
        ExchangeRate.from_currency_id == foreign_currency_id,
        ExchangeRate.to_currency_id == base_currency_id,
        ExchangeRate.date <= exchange_date
    ).order_by(ExchangeRate.date.desc()).first()

    if latest_rate_obj:
        current_rate = Decimal(latest_rate_obj.rate)
    else:
        current_rate = Decimal('1.0')

    current_value = foreign_balance * current_rate
    fx_gain_or_loss = current_value - book_value

    return {
        'foreign_balance': round(foreign_balance, 2),
        'book_value': round(book_value, 2),
        'current_value': round(current_value, 2),
        'fx_gain_or_loss': round(fx_gain_or_loss, 2)
    }


def calculate_realized_fx_gain_loss(db_session, app_id, foreign_currency_id, base_currency_id,
                                    amount_exchanged, exchange_date):
    """
    Calculates the realized FX gain/loss when a specific amount of foreign currency is exchanged.

    Parameters:
        db_session: SQLAlchemy session
        app_id: App/tenant ID
        foreign_currency_id: Foreign currency (e.g. USD)
        base_currency_id: Base currency (e.g. UGX)
        amount_exchanged: Amount of foreign currency exchanged (e.g. 100 USD)
        exchange_date: Date of exchange (used to get latest rate and transactions)

    Returns:
        dict: {
            'book_value': value based on historical book rate (base currency),
            'actual_value': value at current exchange rate (base currency),
            'fx_gain_or_loss': difference
        }
    """
    from models import Transaction, ExchangeRate
    from decimal import Decimal

    amount_exchanged = Decimal(amount_exchanged)

    # Step 1: Get book value per unit
    txns = db_session.query(Transaction).filter(
        Transaction.app_id == app_id,
        Transaction.currency == foreign_currency_id,
        Transaction.date <= exchange_date,
        Transaction.transaction_type == 'Asset'  # Optional: restrict to cash/bank
    ).order_by(Transaction.date).all()

    total_foreign = Decimal('0.0')
    total_book_value = Decimal('0.0')

    for txn in txns:
        direction = Decimal('1.0') if txn.dr_cr == 'D' else Decimal('-1.0')
        foreign_amount = Decimal(txn.amount) * direction
        rate = Decimal(txn.exchange_rate.rate) if txn.exchange_rate else Decimal('1.0')

        total_foreign += foreign_amount
        total_book_value += foreign_amount * rate

    if total_foreign == 0:
        raise ValueError("No available foreign balance to calculate book value.")

    book_value_per_unit = total_book_value / total_foreign

    # Step 2: Get current market rate
    latest_rate = db_session.query(ExchangeRate).filter(
        ExchangeRate.app_id == app_id,
        ExchangeRate.from_currency_id == foreign_currency_id,
        ExchangeRate.to_currency_id == base_currency_id,
        ExchangeRate.date <= exchange_date
    ).order_by(ExchangeRate.date.desc()).first()

    if not latest_rate:
        raise ValueError("No exchange rate found for the given date.")

    current_rate = Decimal(latest_rate.rate)

    # Step 3: Calculate realized values
    book_value = amount_exchanged * book_value_per_unit
    actual_value = amount_exchanged * current_rate
    fx_gain_or_loss = actual_value - book_value

    return {
        'book_value': round(book_value, 2),
        'actual_value': round(actual_value, 2),
        'fx_gain_or_loss': round(fx_gain_or_loss, 2)
    }


from decimal import Decimal, ROUND_HALF_UP


def get_weighted_book_value(db_session, app_id, foreign_currency_id, foreign_account_id, foreign_amount,
                            currency_exchange_id):
    """
    Calculate weighted average book value of the foreign_amount considering prior transactions
    up to the exact datetime of a currency exchange transaction.

    Returns:
        Decimal: weighted book value in base currency
    """
    from models import Transaction, ExchangeRate, CurrencyExchangeTransaction
    from sqlalchemy import and_, or_, func
    from datetime import datetime, time

    foreign_amount = Decimal(foreign_amount).quantize(Decimal('0.01'))

    # Step 1: Get the exchange transaction
    fx_txn = db_session.query(CurrencyExchangeTransaction).filter_by(
        id=currency_exchange_id,
        app_id=app_id
    ).first()

    if not fx_txn:
        raise ValueError(f"CurrencyExchangeTransaction {currency_exchange_id} not found.")

    # Combine exchange date and time to full datetime
    exchange_datetime = datetime.combine(
        fx_txn.exchange_date,
        fx_txn.exchange_time if fx_txn.exchange_time else time.min
    )

    # Step 2: Fetch transactions BEFORE this datetime (handle date + transaction_time)
    txns = db_session.query(Transaction).filter(
        Transaction.app_id == app_id,
        Transaction.currency == foreign_currency_id,
        Transaction.subcategory_id == foreign_account_id,
        Transaction.transaction_type == 'Asset',
        or_(
            Transaction.date < fx_txn.exchange_date,
            and_(
                Transaction.date == fx_txn.exchange_date,
                or_(
                    and_(
                        Transaction.transaction_time.isnot(None),
                        Transaction.transaction_time < exchange_datetime.time()
                    ),
                    and_(
                        Transaction.transaction_time.is_(None),
                        Transaction.date_added < exchange_datetime
                    )
                )
            )
        )
    ).order_by(
        Transaction.date,
        Transaction.transaction_time
    ).all()

    total_foreign_qty = Decimal('0.0')
    total_base_value = Decimal('0.0')

    for txn in txns:
        direction = Decimal('1.0') if txn.dr_cr == 'D' else Decimal('-1.0')
        txn_foreign_amt = Decimal(str(txn.amount)) * direction
        txn_rate = Decimal(str(txn.exchange_rate.rate)) if txn.exchange_rate else Decimal('1.0')
        total_foreign_qty += txn_foreign_amt
        total_base_value += txn_foreign_amt * txn_rate

    if total_foreign_qty == 0:
        latest_rate_obj = db_session.query(ExchangeRate).filter_by(
            app_id=app_id,
            from_currency_id=foreign_currency_id,
            to_currency_id=fx_txn.to_currency_id
        ).filter(
            ExchangeRate.date <= fx_txn.exchange_date
        ).order_by(ExchangeRate.date.desc()).first()

        if not latest_rate_obj:
            raise ValueError(
                f"No exchange rate found for currency {foreign_currency_id} on or before {fx_txn.exchange_date}")

        book_value_per_unit = Decimal(str(latest_rate_obj.rate))
    else:
        book_value_per_unit = (total_base_value / total_foreign_qty).quantize(Decimal('0.0001'))

    book_value = (foreign_amount * book_value_per_unit).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    return book_value


def get_fx_gain_account_id(db_session, app_id):
    query = db_session.query(ChartOfAccounts).filter_by(app_id=app_id, is_fx_gain=True).first()
    return query.id, query.category_fk


def get_fx_loss_account_id(db_session, app_id):
    query = db_session.query(ChartOfAccounts).filter_by(app_id=app_id, is_fx_loss=True).first()
    return query.id, query.category_fk


from decimal import Decimal, ROUND_HALF_UP


def compute_fx_gain_loss(transactions):
    """
    Compute realized FX gain or loss for each currency exchange transaction.

    Parameters:
        transactions (list): A list of dicts with keys:
            - from_currency (str)
            - to_currency (str)
            - amount (Decimal): amount in foreign currency
            - book_value_rate (Decimal): weighted average value before exchange
            - actual_rate (Decimal): actual rate used in the transaction

    Returns:
        list: A list of dicts with calculated gain/loss and metadata
    """
    results = []
    for txn in transactions:
        book_value = txn['amount'] * txn['book_value_rate']
        actual_value = txn['amount'] * txn['actual_rate']
        gain_loss = (actual_value - book_value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        results.append({
            'from_currency': txn['from_currency'],
            'to_currency': txn['to_currency'],
            'amount': txn['amount'],
            'book_value': book_value,
            'actual_value': actual_value,
            'gain_or_loss': gain_loss,
            'type': 'gain' if gain_loss > 0 else 'loss' if gain_loss < 0 else 'none'
        })
    return results


def get_monetary_accounts(session: Session, app_id: int):
    """Retrieve all accounts explicitly marked as monetary"""
    return session.query(ChartOfAccounts).filter(
        ChartOfAccounts.app_id == app_id,
        ChartOfAccounts.is_monetary == True
    ).all()


from collections import defaultdict


# def calculate_fx_revaluation(db_session, app_id, as_of_date, start_date=None, base_currency_id=None,
#                              base_currency_code=None):
#     """
#     Optimized FX revaluation calculation.
#     """
#     from ai import get_base_currency
#     from report_routes import get_latest_exchange_rate, get_historical_exchange_rate
#     from collections import defaultdict
#
#     try:
#
#         base_currency_id = base_currency_id
#         base_currency_code = base_currency_code
#
#         # 2. Get all needed data in optimized queries
#         monetary_accounts = get_monetary_accounts(db_session, app_id)
#         monetary_account_ids = {acct.id for acct in monetary_accounts}  # Using set for faster lookups
#
#         # Single query for account info with only needed columns
#         accounts = db_session.query(
#             ChartOfAccounts.id,
#             ChartOfAccounts.sub_category,
#             ChartOfAccounts.parent_account_type
#
#         ).filter(ChartOfAccounts.app_id == app_id).all()
#
#         account_info = {
#             a.id: {
#                 'name': a.sub_category,
#                 'type': a.parent_account_type,
#                 'currency': base_currency_code
#             }
#             for a in accounts
#         }
#
#         # 3. Pre-fetch all exchange rates
#         currencies = db_session.query(Currency.id).filter(
#             Currency.app_id == app_id,
#             Currency.id != base_currency_id
#         ).all()
#
#         exchange_rates = {}
#         historical_rate_cache = {}  # Cache for historical rates: {(currency_id, date): rate}
#
#         for currency in currencies:
#             rate = get_latest_exchange_rate(
#                 db_session, app_id, currency.id, base_currency_id, as_of_date
#             )
#             exchange_rates[currency.id] = rate if rate else 1.0
#
#         result = {
#             'total_fx_gain_loss': 0.0,
#             'details': defaultdict(lambda: {
#                 'account_id': None,
#                 'account_name': '',
#                 'account_type': '',
#                 'fx_gain_loss': 0.0,
#                 'currency': ''
#             })
#         }
#
#         # ---------- A. Handle Opening Balances (Net) ----------
#         if start_date is not None:
#             # Use aggregation in database rather than fetching all transactions
#             opening_balances = defaultdict(float)
#
#             opening_txns = db_session.query(
#                 Transaction.subcategory_id,
#                 Transaction.currency,
#                 Transaction.dr_cr,
#                 func.sum(Transaction.amount).label('total_amount')
#             ).filter(
#                 Transaction.app_id == app_id,
#                 Transaction.date < start_date,
#                 Transaction.subcategory_id.in_(monetary_account_ids),
#                 Transaction.currency != base_currency_id
#             ).group_by(
#                 Transaction.subcategory_id,
#                 Transaction.currency,
#                 Transaction.dr_cr
#             ).all()
#
#             for txn in opening_txns:
#                 amount = float(txn.total_amount)
#                 if txn.dr_cr == 'C':
#                     amount *= -1
#                 opening_balances[(txn.subcategory_id, txn.currency)] += amount
#
#             # Calculate FX gain/loss on net opening balances
#             for (acc_id, currency), balance in opening_balances.items():
#                 cache_key = (currency, start_date)
#                 if cache_key not in historical_rate_cache:
#                     historical_rate_cache[cache_key] = get_historical_exchange_rate(
#                         db_session, currency, base_currency_id, start_date
#                     ) or exchange_rates.get(currency, 1.0)
#
#                 historical_rate = historical_rate_cache[cache_key]
#                 current_rate = exchange_rates.get(currency, 1.0)
#                 fx_diff = (current_rate - historical_rate) * balance
#
#                 result['total_fx_gain_loss'] += fx_diff
#
#                 # Update detail using defaultdict
#                 detail = result['details'][acc_id]
#                 if not detail['account_id']:
#                     detail.update({
#                         'account_id': acc_id,
#                         'account_name': account_info[acc_id]['name'],
#                         'account_type': account_info[acc_id]['type'],
#                         'currency': account_info[acc_id]['currency']
#                     })
#                 detail['fx_gain_loss'] += fx_diff
#
#         # ---------- B. Process Period Transactions ----------
#         period_txns_query = db_session.query(
#             Transaction.subcategory_id,
#             Transaction.currency,
#             Transaction.amount,
#             Transaction.dr_cr,
#             Transaction.date
#         ).filter(
#             Transaction.app_id == app_id,
#             Transaction.date <= as_of_date,
#             Transaction.subcategory_id.in_(monetary_account_ids),
#             Transaction.currency != base_currency_id
#         )
#
#         if start_date is not None:
#             period_txns_query = period_txns_query.filter(Transaction.date >= start_date)
#
#         period_txns = period_txns_query.all()
#
#         for txn in period_txns:
#             acc_id = txn.subcategory_id
#             currency = txn.currency
#             cache_key = (currency, txn.date)
#
#             if cache_key not in historical_rate_cache:
#                 historical_rate_cache[cache_key] = get_historical_exchange_rate(
#                     db_session, currency, base_currency_id, txn.date
#                 ) or exchange_rates.get(currency, 1.0)
#
#             historical_rate = historical_rate_cache[cache_key]
#             current_rate = exchange_rates.get(currency, 1.0)
#
#             fx_diff = (current_rate - historical_rate) * float(txn.amount)
#             if txn.dr_cr == 'C':
#                 fx_diff *= -1
#
#             result['total_fx_gain_loss'] += fx_diff
#
#             # Update detail using defaultdict
#             detail = result['details'][acc_id]
#             if not detail['account_id']:
#                 detail.update({
#                     'account_id': acc_id,
#                     'account_name': account_info[acc_id]['name'],
#                     'account_type': account_info[acc_id]['type'],
#                     'currency': account_info[acc_id]['currency']
#                 })
#             detail['fx_gain_loss'] += fx_diff
#
#         # Convert defaultdict to list for final result
#         result['details'] = list(result['details'].values())
#         logger.info(f'FX revaluation results: {result}')
#         return result
#
#     except Exception as e:
#         logger.error(f"Error calculating FX revaluation: {str(e)}", exc_info=True)
#         raise
#


def calculate_fx_revaluation(db_session, app_id, as_of_date, start_date=None, base_currency_id=None,
                             base_currency_code=None):
    """
    Optimized FX revaluation calculation for Journal/JournalEntry structure.
    """
    from ai import get_base_currency
    from report_routes import get_latest_exchange_rate, get_historical_exchange_rate
    from collections import defaultdict

    try:
        base_currency_id = base_currency_id
        base_currency_code = base_currency_code

        # 2. Get all needed data in optimized queries
        monetary_accounts = get_monetary_accounts(db_session, app_id)
        monetary_account_ids = {acct.id for acct in monetary_accounts}

        # Single query for account info with only needed columns
        accounts = db_session.query(
            ChartOfAccounts.id,
            ChartOfAccounts.sub_category,
            ChartOfAccounts.parent_account_type
        ).filter(ChartOfAccounts.app_id == app_id).all()

        account_info = {
            a.id: {
                'name': a.sub_category,
                'type': a.parent_account_type,
                'currency': base_currency_code
            }
            for a in accounts
        }

        # 3. Pre-fetch all exchange rates
        currencies = db_session.query(Currency.id).filter(
            Currency.app_id == app_id,
            Currency.id != base_currency_id
        ).all()

        exchange_rates = {}
        historical_rate_cache = {}

        for currency in currencies:
            rate = get_latest_exchange_rate(
                db_session, app_id, currency.id, base_currency_id, as_of_date
            )
            exchange_rates[currency.id] = rate if rate else 1.0

        result = {
            'total_fx_gain_loss': 0.0,
            'details': defaultdict(lambda: {
                'account_id': None,
                'account_name': '',
                'account_type': '',
                'fx_gain_loss': 0.0,
                'currency': ''
            })
        }

        # ---------- A. Handle Opening Balances (Net) ----------
        if start_date is not None:
            opening_balances = defaultdict(float)

            # Query for opening balances from JournalEntry
            opening_entries = db_session.query(
                JournalEntry.subcategory_id,
                Journal.currency_id,
                JournalEntry.dr_cr,
                func.sum(JournalEntry.amount).label('total_amount')
            ).join(
                Journal, Journal.id == JournalEntry.journal_id
            ).filter(
                Journal.app_id == app_id,
                Journal.date < start_date,
                Journal.status == 'Posted',  # Only include posted journals
                JournalEntry.subcategory_id.in_(monetary_account_ids),
                Journal.currency_id != base_currency_id
            ).group_by(
                JournalEntry.subcategory_id,
                Journal.currency_id,
                JournalEntry.dr_cr
            ).all()

            for entry in opening_entries:
                amount = float(entry.total_amount)
                if entry.dr_cr == 'C':
                    amount *= -1
                opening_balances[(entry.subcategory_id, entry.currency_id)] += amount

            # Calculate FX gain/loss on net opening balances
            for (acc_id, currency), balance in opening_balances.items():
                cache_key = (currency, start_date)
                if cache_key not in historical_rate_cache:
                    historical_rate_cache[cache_key] = get_historical_exchange_rate(
                        db_session, currency, base_currency_id, start_date
                    ) or exchange_rates.get(currency, 1.0)

                historical_rate = historical_rate_cache[cache_key]
                current_rate = exchange_rates.get(currency, 1.0)
                fx_diff = (current_rate - historical_rate) * balance

                result['total_fx_gain_loss'] += fx_diff

                # Update detail using defaultdict
                detail = result['details'][acc_id]
                if not detail['account_id']:
                    detail.update({
                        'account_id': acc_id,
                        'account_name': account_info[acc_id]['name'],
                        'account_type': account_info[acc_id]['type'],
                        'currency': account_info[acc_id]['currency']
                    })
                detail['fx_gain_loss'] += fx_diff

        # ---------- B. Process Period Journal Entries ----------
        period_entries_query = db_session.query(
            JournalEntry.subcategory_id,
            Journal.currency_id,
            JournalEntry.amount,
            JournalEntry.dr_cr,
            Journal.date
        ).join(
            Journal, Journal.id == JournalEntry.journal_id
        ).filter(
            Journal.app_id == app_id,
            Journal.date <= as_of_date,
            Journal.status == 'Posted',  # Only include posted journals
            JournalEntry.subcategory_id.in_(monetary_account_ids),
            Journal.currency_id != base_currency_id
        )

        if start_date is not None:
            period_entries_query = period_entries_query.filter(Journal.date >= start_date)

        period_entries = period_entries_query.all()

        for entry in period_entries:
            acc_id = entry.subcategory_id
            currency = entry.currency_id
            cache_key = (currency, entry.date)

            if cache_key not in historical_rate_cache:
                historical_rate_cache[cache_key] = get_historical_exchange_rate(
                    db_session, currency, base_currency_id, entry.date
                ) or exchange_rates.get(currency, 1.0)

            historical_rate = historical_rate_cache[cache_key]
            current_rate = exchange_rates.get(currency, 1.0)

            fx_diff = (current_rate - historical_rate) * float(entry.amount)
            if entry.dr_cr == 'C':
                fx_diff *= -1

            result['total_fx_gain_loss'] += fx_diff

            # Update detail using defaultdict
            detail = result['details'][acc_id]
            if not detail['account_id']:
                detail.update({
                    'account_id': acc_id,
                    'account_name': account_info[acc_id]['name'],
                    'account_type': account_info[acc_id]['type'],
                    'currency': account_info[acc_id]['currency']
                })
            detail['fx_gain_loss'] += fx_diff

        # Convert defaultdict to list for final result
        result['details'] = list(result['details'].values())
        logger.info(f'FX revaluation results: {result}')
        return result

    except Exception as e:
        logger.error(f"Error calculating FX revaluation: {str(e)}", exc_info=True)
        raise


def normalize_form_value(value):
    if value in ('', 'None', None):
        return None
    return value


def empty_to_none(value):
    return None if value in ("", "None") else value


def get_item_details(item):
    """Returns (item_name_with_variation, description) with guaranteed fallbacks"""
    # Primary path - when item_name exists
    if hasattr(item, 'item_name') and item.item_name:
        item_name = item.item_name
        variation_name = ""

        # Check for variation
        if hasattr(item, 'inventory_item_variation_link') and item.inventory_item_variation_link:
            if hasattr(item.inventory_item_variation_link, 'inventory_item_variation'):
                variation = item.inventory_item_variation_link.inventory_item_variation
                if variation and hasattr(variation, 'variation_name') and variation.variation_name:
                    variation_name = variation.variation_name

        # Combine item name with variation if exists
        if variation_name:
            full_name = f"{item_name} - {variation_name}"
        else:
            full_name = item_name

        return (
            full_name,
            item.description if hasattr(item, 'description') and item.description else "-"
        )

    # Secondary path - inventory item fallback
    try:
        if (hasattr(item, 'inventory_item_variation_link') and
                item.inventory_item_variation_link and
                hasattr(item.inventory_item_variation_link, 'inventory_item')):
            inv_item = item.inventory_item_variation_link.inventory_item
            item_name = inv_item.item_name if hasattr(inv_item, 'item_name') and inv_item.item_name else "-"
            variation_name = ""

            # Get variation if exists
            if hasattr(item.inventory_item_variation_link, 'inventory_item_variation'):
                variation = item.inventory_item_variation_link.inventory_item_variation
                if variation and hasattr(variation, 'variation_name') and variation.variation_name:
                    variation_name = variation.variation_name

            # Combine item name with variation if exists
            if variation_name:
                full_name = f"{item_name} - {variation_name}"
            else:
                full_name = item_name

            return (
                full_name,
                inv_item.item_description if hasattr(inv_item,
                                                     'item_description') and inv_item.item_description else "-"
            )
    except AttributeError:
        pass

    # Final fallback
    return "-", "-"


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
