# ai.py
import datetime
from decimal import Decimal
import logging
import requests
from flask import jsonify, Response, url_for
from flask_login import current_user
from requests import RequestException

from models import ExchangeRate, Currency
from utils import create_notification

logger = logging.getLogger(__name__)


# Function to check if chart of accounts exists
def check_existing_chart_of_accounts(db_session, company_id: int) -> list:
    """
    Check if a Chart of Accounts exists for the given company.
    Returns the list of existing chart of accounts if available.
    """
    from app import ChartOfAccounts  # Importing here to avoid circular import
    try:
        return db_session.query(ChartOfAccounts).filter(ChartOfAccounts.app_id == company_id).all()
    except Exception as e:
        db_session.rollback()  # We roll back in case of error
        raise Exception(f"Error checking existing chart of accounts: {str(e)}")


# Function to get the industry template
def get_industry_template(industry: str) -> list:
    """
    Get the default chart of accounts template for the specified industry.
    """
    from templates import industry_templates  # This is where we are importing industry templates.

    if industry not in industry_templates:
        return []  # Return empty list if the industry is not found
    return industry_templates[industry]


def generate_chart_of_accounts(db_session, company_id: int, industry: str, accounts_to_keep: list = None) -> tuple[
    Response, int]:
    """
    Generate the chart of accounts based on the selected industry, ensuring only necessary accounts are created.
    """
    from app import Category, ChartOfAccounts
    import json

    if accounts_to_keep is None:
        accounts_to_keep = []

    accounts_to_store = accounts_to_keep
    new_chart_of_accounts = []
    existing_categories = {}

    for category in db_session.query(Category).filter(Category.app_id == company_id).all():
        existing_categories[category.category] = category

    category_mapping = {
        "Income": 1,
        "Liability": 2,
        "Equity": 3,
        "Asset": 4
    }

    accounts_to_store = [json.loads(account) if isinstance(account, str) else account for account in accounts_to_keep]

    for account in accounts_to_store:
        parent_category = account["account_type"]
        category = account["category"]
        category_id = account["category_id"]
        subcategory = account["sub_category"]
        sub_category_id = account["sub_category_id"]
        normal_balance = account["normal_balance"]
        report_section = account["report_section"] or None
        is_bank = account["is_bank"]
        is_cash = account["is_cash"]

        parent_category_id = category_mapping.get(parent_category, 5)

        existing_category = existing_categories.get(category)

        if not existing_category:
            new_category = Category(
                category=category,
                category_id=category_id,
                account_type=parent_category,
                app_id=company_id
            )
            db_session.add(new_category)
            db_session.flush()
            existing_categories[category] = new_category
            existing_category = new_category

        existing_sub_category = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.category_fk == existing_category.id,
            ChartOfAccounts.sub_category == subcategory
        ).first()

        if not existing_sub_category:
            new_account = ChartOfAccounts(
                parent_account_type=parent_category,
                parent_account_type_id=parent_category_id,
                category_id=category_id,
                category_fk=existing_category.id,
                category=category,
                sub_category_id=sub_category_id,
                sub_category=subcategory,
                normal_balance=normal_balance,
                report_section_id=report_section,
                is_bank=is_bank,
                is_cash=is_cash,
                app_id=company_id,
                created_by=current_user.id
            )
            new_chart_of_accounts.append(new_account)

    try:
        db_session.add_all(new_chart_of_accounts)
        db_session.commit()

        # Fixed serialization - choose either method:
        # Method 1 (if you add to_dict() to model):
        # new_accounts_dicts = [account.to_dict() for account in new_chart_of_accounts]

        # Method 2 (using SQLAlchemy introspection):
        new_accounts_dicts = []
        for account in new_chart_of_accounts:
            account_dict = {column.name: getattr(account, column.name)
                            for column in account.__table__.columns}
            new_accounts_dicts.append(account_dict)

        return jsonify(new_accounts_dicts), 201

    except Exception as e:
        db_session.rollback()
        logger.debug(f'Error that occurred is {e}')
        return jsonify({'error': 'An error occurred while generating the chart of accounts.'}), 500


# Function to handle the chart of accounts creation based on the user's inputs
def handle_chart_of_accounts_creation(db_session, company_id: int, industry: str, accounts_to_keep: list = None):
    """
    Main handler to check for existing chart of accounts, generate a new one if needed,
    and customize it based on user input.
    """
    from app import ChartOfAccounts  # Importing here to avoid circular import
    try:
        # Check if a chart of accounts already exists
        existing_accounts = check_existing_chart_of_accounts(db_session, company_id)

        # If no existing chart, generate one
        if not existing_accounts:
            print("No existing chart of accounts found. Generating new one based on industry template.")
            generate_chart_of_accounts(db_session, company_id, industry, accounts_to_keep)

        # If the chart of accounts exists, we do nothing (user is assumed to have configured it)
        else:
            print("Chart of accounts already exists. Assuming user is satisfied.")

    except Exception as e:
        db_session.rollback()  # Ensure we rollback in case of error
        raise Exception(f"Error handling chart of accounts creation: {str(e)}")


def get_exchange_rate(db_session, from_currency_id: int, to_currency_id: int, app_id: int,
                      as_of_date: datetime.date = None):
    """
    Fetch the most recent exchange rate as of `as_of_date`.
    If rate is stored in reverse, invert it before returning.
    """

    if not as_of_date:
        as_of_date = datetime.datetime.now().date()

    # Look for exchange rate on or before `as_of_date`, in either direction
    exchange_rate_query = db_session.query(ExchangeRate).filter(
        ExchangeRate.app_id == app_id,
        ExchangeRate.date <= as_of_date,
        (
                (ExchangeRate.from_currency_id == from_currency_id) & (ExchangeRate.to_currency_id == to_currency_id) |
                (ExchangeRate.from_currency_id == to_currency_id) & (ExchangeRate.to_currency_id == from_currency_id)
        )
    ).order_by(ExchangeRate.date.desc(), ExchangeRate.id.desc()).first()
    if not exchange_rate_query:
        # Fallback only makes sense if as_of_date is today or close to it
        if as_of_date >= datetime.datetime.now().date() - datetime.timedelta(days=2):
            from_user_currency = db_session.query(Currency.user_currency).filter_by(app_id=app_id,
                                                                                    id=from_currency_id).scalar()
            to_user_currency = db_session.query(Currency.user_currency).filter_by(app_id=app_id,
                                                                                  id=to_currency_id).scalar()

            return fetch_and_store_external_rate(
                db_session, from_user_currency, to_user_currency,
                from_currency_id, to_currency_id, app_id
            )
        else:
            raise ValueError(
                f"No historical exchange rate found for {from_currency_id} to {to_currency_id} as of {as_of_date}")

    if exchange_rate_query.from_currency_id == from_currency_id:
        return exchange_rate_query.rate
    else:
        return 1 / exchange_rate_query.rate


def fetch_and_store_external_rate(db_session, from_currency_id: int, to_currency_id: int, from_currency: int,
                                  to_currency: int, app_id: int):
    """
    Fetch exchange rate from an external API and store it in the database.
    """

    from app import ExchangeRate  # Ensure ExchangeRate is properly imported
    API_URL = f"https://api.exchangerate-api.com/v4/latest/{from_currency_id}"

    try:
        response = requests.get(API_URL)
        data = response.json()

        if "rates" in data and str(to_currency_id) in data["rates"]:
            rate = Decimal(str(data["rates"][str(to_currency_id)]))

            # Store the new exchange rate in the database
            new_exchange_rate = ExchangeRate(
                app_id=app_id,
                from_currency_id=from_currency,
                to_currency_id=to_currency,
                rate=rate,
                date=datetime.datetime.today().date()
            )
            db_session.add(new_exchange_rate)
            db_session.commit()

            print(f"Stored new exchange rate: {from_currency_id} -> {to_currency_id} = {rate}")
            return rate
        else:
            print(f"Exchange rate not found for {from_currency_id} -> {to_currency_id}")
            return None

    except Exception as e:
        print(f"Error fetching exchange rate: {e}")
        return None


class ExchangeRateFetchError(Exception):
    """Custom exception for exchange rate fetch failures."""
    pass


def fetch_and_store_external_rate_with_transaction_date(
        db_session, from_currency_id: int, to_currency_id: int,
        from_currency: int, to_currency: int, app_id: int, transaction_date
):
    """
    Fetch exchange rate from an external API and store it in the database.

    Returns:
        Decimal: The fetched exchange rate.

    Raises:
        ExchangeRateFetchError: If API fails or rate is not found.
    """
    from app import ExchangeRate
    API_URL = f"https://api.exchangerate-api.com/v4/latest/{from_currency_id}"

    try:
        response = requests.get(API_URL, timeout=5)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx, 5xx)
        data = response.json()

        if "rates" not in data or str(to_currency_id) not in data["rates"]:
            raise ExchangeRateFetchError(
                f"Exchange rate not found for {from_currency_id} → {to_currency_id}. "
                "Please enter the rate manually."
            )

        rate = Decimal(str(data["rates"][str(to_currency_id)]))

        # Store the new exchange rate in the database
        new_exchange_rate = ExchangeRate(
            app_id=app_id,
            from_currency_id=from_currency,
            to_currency_id=to_currency,
            rate=rate,
            date=transaction_date
        )
        db_session.add(new_exchange_rate)
        db_session.commit()

        return rate

    except RequestException as e:
        raise ExchangeRateFetchError(
            "Failed to fetch exchange rate from external API. "
            "Please enter the rate manually."
        ) from e
    except Exception as e:
        raise ExchangeRateFetchError(
            f"Unexpected error while fetching exchange rate: {e}. "
            "Please enter the rate manually."
        ) from e


def get_base_currency(session, app_id: int):
    """
    Returns the base currency ID and user_currency (e.g., '1', 'USD')
    for the given company (app_id), based on currency_index = 1.
    """
    from models import Currency
    base_currency = session.query(Currency).filter_by(app_id=app_id, currency_index=1).first()

    if base_currency:
        return {
            "base_currency_id": base_currency.id,
            "base_currency": base_currency.user_currency
        }
    return None


def get_or_create_exchange_rate_id(session, from_currency_id, to_currency_id, app_id, transaction_date):
    from app import ExchangeRate
    from models import Currency

    # Ensure transaction_date is a date object
    if isinstance(transaction_date, datetime.datetime):
        transaction_date = transaction_date.date()
    elif isinstance(transaction_date, str):
        try:
            transaction_date = datetime.datetime.strptime(transaction_date, "%Y-%m-%d").date()
        except ValueError:
            try:
                transaction_date = datetime.datetime.fromisoformat(transaction_date).date()
            except ValueError:
                raise ValueError("Unsupported date format for transaction_date")
    elif not isinstance(transaction_date, datetime.date):
        raise TypeError("transaction_date must be a date, datetime, or string")

    notification_msg = None
    # Step 1: Check for rate on the exact transaction_date
    exact_rate = session.query(ExchangeRate).filter(
        ExchangeRate.from_currency_id == from_currency_id,
        ExchangeRate.to_currency_id == to_currency_id,
        ExchangeRate.app_id == app_id,
        ExchangeRate.date == transaction_date,
        ExchangeRate.currency_exchange_transaction_id.is_(None)  # More explicit than == None
    ).first()

    if exact_rate:
        return exact_rate.id, exact_rate.date, notification_msg

    # Step 2: Check for the most recent available rate in DB
    recent_rate = session.query(ExchangeRate).filter(
        ExchangeRate.from_currency_id == from_currency_id,
        ExchangeRate.to_currency_id == to_currency_id,
        ExchangeRate.app_id == app_id,
        ExchangeRate.currency_exchange_transaction_id.is_(None)  # Corrected this line
    ).order_by(ExchangeRate.date.desc()).first()

    if recent_rate:
        # Duplicate and store under the transaction_date
        duplicated_rate = ExchangeRate(
            from_currency_id=from_currency_id,
            to_currency_id=to_currency_id,
            app_id=app_id,
            rate=recent_rate.rate,
            date=transaction_date
        )
        session.add(duplicated_rate)
        session.commit()

        # Only generate message if this is a new estimated rate
        notification_msg = (
            f"Auto-generated exchange rate {duplicated_rate.rate} applied for "
            f"{transaction_date}. Please verify against actual rates."
        )

        return duplicated_rate.id, duplicated_rate.date, notification_msg

    # Step 3: Fetch from external API and let it store for transaction_date
    from_currency_code = session.query(Currency.user_currency).filter_by(id=from_currency_id).scalar()
    to_currency_code = session.query(Currency.user_currency).filter_by(id=to_currency_id).scalar()

    if not from_currency_code or not to_currency_code:
        return None, None

    rate = fetch_and_store_external_rate_with_transaction_date(
        db_session=session,
        from_currency_id=from_currency_code,
        to_currency_id=to_currency_code,
        from_currency=from_currency_id,
        to_currency=to_currency_id,
        app_id=app_id,
        transaction_date=transaction_date
    )

    # It has already stored it inside the function. Just return it now
    stored_rate = session.query(ExchangeRate).filter_by(
        from_currency_id=from_currency_id,
        to_currency_id=to_currency_id,
        app_id=app_id,
        date=transaction_date
    ).first()

    # Only generate message if this is a new estimated rate
    notification_msg = (
        f"Auto-generated exchange rate {stored_rate.rate} applied for "
        f"{transaction_date}. Please verify against actual rates."
    )

    return (stored_rate.id, stored_rate.date, notification_msg) if stored_rate else (None, None, None)


def resolve_exchange_rate_for_transaction(session, currency_id, transaction_date, app_id):
    """
    Resolves an exchange rate for a single-currency transaction.

    If the transaction currency is different from the company base currency,
    attempts to get or create the exchange rate record and returns its ID.

    Args:
        session: SQLAlchemy session
        currency_id: A transaction CURRENCY ID e.g 1
        transaction_date: Date of transaction
        app_id: The company ID

    Returns:
        (exchange_rate_id, notification_msg)
    """
    base_currency_info = get_base_currency(session, app_id)
    if not base_currency_info:
        raise ValueError("Base currency not defined for this company.")

    base_currency_id = base_currency_info["base_currency_id"]

    if currency_id == base_currency_id:
        # No exchange rate needed
        return None, None

    # Get or create the exchange rate (base -> transaction currency)
    exchange_rate_id, rate_date, notification_msg = get_or_create_exchange_rate_id(
        session=session,
        from_currency_id=currency_id,
        to_currency_id=base_currency_id,
        app_id=app_id,
        transaction_date=transaction_date
    )

    if notification_msg:
        message = f"Auto-generated rates applied: {notification_msg}"

        create_notification(
            db_session=session,
            user_id=None,
            company_id=app_id,
            message=message,
            type='info',
            is_popup=True,
            url=url_for('view_exchange_rates')
        )

    return exchange_rate_id, notification_msg
