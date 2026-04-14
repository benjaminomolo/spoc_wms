import datetime
import logging
from decimal import Decimal

import requests
from flask_login import current_user

from models import ChartOfAccounts, Category, ExchangeRate

logger = logging.getLogger(__name__)


def get_exchange_rate_and_obj(db_session, from_currency_id: int, to_currency_id: int, app_id: int,
                              as_of_date: datetime.date = None):
    """
    Fetch the most recent exchange rate as of `as_of_date`.
    If rate is stored in reverse, invert it before returning.
    Returns a tuple of (exchange_rate_object, calculated_rate)
    """
    from app import ExchangeRate, Currency

    if not as_of_date:
        as_of_date = datetime.datetime.now().date()

    elif isinstance(as_of_date, str):
        as_of_date = datetime.datetime.strptime(as_of_date, "%Y-%m-%d").date()

    # Look for exchange rate on or before `as_of_date`, in either direction
    exchange_rate_obj = db_session.query(ExchangeRate).filter(
        ExchangeRate.app_id == app_id,
        ExchangeRate.date <= as_of_date,
        (
                (ExchangeRate.from_currency_id == from_currency_id) & (ExchangeRate.to_currency_id == to_currency_id) |
                (ExchangeRate.from_currency_id == to_currency_id) & (ExchangeRate.to_currency_id == from_currency_id)
        )
    ).order_by(ExchangeRate.date.desc(), ExchangeRate.id.desc()).first()

    if not exchange_rate_obj:
        # Fallback only makes sense if as_of_date is today or close to it
        if as_of_date >= datetime.datetime.now().date() - datetime.timedelta(days=2):
            from_user_currency = db_session.query(Currency.user_currency).filter_by(app_id=app_id,
                                                                                    id=from_currency_id).scalar()
            to_user_currency = db_session.query(Currency.user_currency).filter_by(app_id=app_id,
                                                                                  id=to_currency_id).scalar()

            # Modify fetch_and_store_external_rate to also return the object
            exchange_rate_obj = fetch_and_store_external_rate(
                db_session,
                from_currency_id, to_currency_id, from_user_currency, to_user_currency, app_id, as_of_date
            )
            if exchange_rate_obj:
                # For newly created rate, check direction
                if exchange_rate_obj.from_currency_id == from_currency_id:
                    calculated_rate = exchange_rate_obj.rate
                else:
                    calculated_rate = 1 / exchange_rate_obj.rate
                return exchange_rate_obj, calculated_rate
            else:
                raise ValueError(f"Failed to fetch external exchange rate for {from_currency_id} to {to_currency_id}")
        else:
            raise ValueError \
                (f"No historical exchange rate found for {from_currency_id} to {to_currency_id} as of {as_of_date}")

    # Calculate the rate based on the direction
    if exchange_rate_obj.from_currency_id == from_currency_id:
        calculated_rate = exchange_rate_obj.rate
    else:
        calculated_rate = 1 / exchange_rate_obj.rate

    return exchange_rate_obj, calculated_rate


def fetch_and_store_external_rate(db_session, from_currency_id: int, to_currency_id: int, from_currency,
                                  to_currency, app_id: int, as_of_date):
    """
    Fetch exchange rate from an external API and store it in the database.
    """
    from app import ExchangeRate  # Ensure ExchangeRate is properly imported
    API_URL = f"https://api.exchangerate-api.com/v4/latest/{from_currency}"

    try:
        response = requests.get(API_URL)
        data = response.json()

        if "rates" in data and str(to_currency) in data["rates"]:
            rate = Decimal(str(data["rates"][str(to_currency)]))

            # Store the new exchange rate in the database
            new_exchange_rate = ExchangeRate(
                from_currency_id=from_currency_id,
                to_currency_id=to_currency_id,
                app_id=app_id,
                rate=rate,
                date=as_of_date,
                created_by=current_user.id,
                source_type=None,
                source_id=None,  # Can be None for new transactions
                currency_exchange_transaction_id=None
            )
            db_session.add(new_exchange_rate)
            db_session.commit()

            return new_exchange_rate
        else:
            return None

    except Exception as e:
        logger.error(f"Error fetching external exchange rate: {e}")
        return None


def get_or_create_fx_clearing_account(db_session, app_id, created_by_user_id=None):
    """
    Get or create the system FX clearing account (admin account not modifiable by users)
    Uses 'Asset' as parent_account_type and creates system-specific categories
    """
    try:
        # Try to find existing FX clearing account
        fx_clearing_account = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.sub_category == 'FX Clearing Account',
            ChartOfAccounts.is_system_account == True
        ).first()

        if fx_clearing_account:
            return fx_clearing_account.id

        # Step 1: Find or create SYSTEM-ONLY asset category structure
        # Use unique category names to avoid conflicts with user categories
        system_asset_category = db_session.query(Category).filter(
            Category.app_id == app_id,
            Category.account_type == 'Asset',
            Category.category == 'System Assets',  # Unique system category name
            Category.category_id == 'SYS-1000'  # System-specific ID
        ).first()

        if not system_asset_category:
            # Create system asset category structure
            system_asset_category = Category(
                account_type='Asset',
                category_id='SYS-1000',  # System-specific ID prefix
                category='System Assets',  # Unique system category name
                app_id=app_id
            )
            db_session.add(system_asset_category)
            db_session.flush()

        # Step 2: Create the FX clearing account in ChartOfAccounts
        fx_clearing_account = ChartOfAccounts(
            # Basic account information - using Asset as parent type
            parent_account_type='Asset',
            parent_account_type_id='SYS-1000',
            category_id='SYS-1000',
            category_fk=system_asset_category.id,
            category='System Assets',  # System category
            sub_category_id='SYS-1100',  # System-specific ID
            sub_category='FX Clearing Account',

            # Account flags
            is_bank=False,
            is_cash=False,
            is_receivable=False,
            is_payable=False,
            is_monetary=True,  # FX clearing involves monetary amounts
            is_active=True,
            is_system_account=True,  # Mark as system account

            # Financial properties
            normal_balance='Debit',  # Assets have debit normal balance

            # Ownership
            created_by=created_by_user_id,
            app_id=app_id
        )

        db_session.add(fx_clearing_account)
        db_session.commit()

        logger.info(f"Created FX clearing account with ID: {fx_clearing_account.id} for app_id: {app_id}")
        return fx_clearing_account.id

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error getting/creating FX clearing account for app_id {app_id}: {str(e)}")
        raise


def get_or_create_exchange_rate_for_transaction(session, action='get',
                                                source_type=None, source_id=None,
                                                from_currency_id=None, to_currency_id=None,
                                                rate_value=None, rate_date=None, app_id=None,
                                                created_by=None,
                                                currency_exchange_transaction_id=None):
    """
    Generic function to either GET an existing exchange rate by source or CREATE a new rate snapshot.

    Args:
        session: SQLAlchemy session
        action: 'get' or 'create'

        # For GET action (both required):
        source_type: Type of transaction (e.g., 'sale', 'purchase', 'payroll', 'journal')
        source_id: ID of the source transaction record

        # For CREATE action (all required except created_by and currency_exchange_transaction_id):
        from_currency_id: Source currency ID
        to_currency_id: Target currency ID
        rate_value: The exchange rate
        rate_date: Date this rate applies to
        app_id: Company ID
        created_by: Optional - User ID who created this rate
        source_type: Type of transaction (for tracking)
        source_id: ID of the source transaction (if available, can be None for new transactions)
        currency_exchange_transaction_id: Optional - for currency exchange specific transactions

    Returns:
        For 'get': (rate_id, rate_object) or (None, None) if not found
        For 'create': (new_rate_id, new_rate_object) or (None, None) if currencies are same
    """
    from app import ExchangeRate
    import datetime

    if action == 'get':
        # GET mode - retrieve existing rate by source_type AND source_id
        if not source_type or not source_id:
            return None, None

        rate = session.query(ExchangeRate).filter(
            ExchangeRate.source_type == source_type,
            ExchangeRate.source_id == source_id
        ).first()

        return (rate.id, rate) if rate else (None, None)

    elif action == 'create':
        # CREATE mode - create new rate snapshot

        # Validate required fields
        if None in [from_currency_id, to_currency_id, rate_value, rate_date, app_id, source_type]:
            raise ValueError(
                "from_currency_id, to_currency_id, rate_value, rate_date, app_id, and source_type are required for create action")

        # No rate needed for same currency
        if from_currency_id == to_currency_id:
            return None, None

        # Ensure rate_date is a date object
        if isinstance(rate_date, datetime.datetime):
            rate_date = rate_date.date()
        elif isinstance(rate_date, str):
            rate_date = datetime.datetime.strptime(rate_date, '%Y-%m-%d').date()

        # Create new rate record
        new_rate = ExchangeRate(
            from_currency_id=from_currency_id,
            to_currency_id=to_currency_id,
            app_id=app_id,
            rate=rate_value,
            date=rate_date,
            created_by=created_by,
            source_type=source_type,
            source_id=source_id,  # Can be None for new transactions
            currency_exchange_transaction_id=currency_exchange_transaction_id
        )

        session.add(new_rate)
        session.flush()

        return new_rate.id, new_rate

    else:
        raise ValueError("action must be either 'get' or 'create'")


def process_multi_currency_through_suspense(db_session,
                                            source_account_id,  # Where funds come from (Customer Credit, Cash, etc.)
                                            destination_account_id,  # Where funds go to (AR, AP, Expense, etc.)
                                            source_amount,  # Amount in source currency
                                            source_currency_id,  # Currency of source
                                            source_rate_id,  # Exchange rate ID for source
                                            destination_amount,  # Amount in destination currency
                                            destination_currency_id,  # Currency of destination
                                            destination_rate_id,  # Exchange rate ID for destination
                                            base_currency_id,  # Base currency ID (UGX)
                                            fx_account_id,  # FX Gain/Loss account ID
                                            suspense_account_id,  # Suspense account ID
                                            description,  # Human-readable description
                                            source_type,  # e.g., 'payment_receipt', 'credit_application', 'payment'
                                            source_id,  # ID of source record
                                            current_user_id,
                                            app_id,
                                            transaction_date,
                                            source_account_type,  # 'Asset', 'Liability', 'Equity', 'Income', 'Expense'
                                            destination_account_type):  # 'Asset', 'Liability', 'Equity', 'Income', 'Expense'
    """
    ===========================================================================
    PROCESS MULTI-CURRENCY TRANSACTIONS THROUGH SUSPENSE ACCOUNT
    ===========================================================================

    WHAT THIS FUNCTION DOES:
    ------------------------
    Handles any transaction involving two different currencies by using a
    suspense account as a bridge. It automatically calculates and records any
    foreign exchange gains or losses.

    WORKS FOR BOTH:
    - Receiving money (Customer payments, refunds from suppliers)
    - Paying money (Supplier payments, refunds to customers)

    PARAMETERS:
    -----------
    source_account_id      : Account funds are coming FROM
    destination_account_id : Account funds are going TO
    source_amount         : Amount in source currency
    source_currency_id    : Currency of source amount
    source_rate_id        : Exchange rate ID for source
    destination_amount    : Amount in destination currency
    destination_currency_id : Currency of destination amount
    destination_rate_id   : Exchange rate ID for destination
    base_currency_id      : Your base currency (e.g., UGX)
    fx_account_id         : Account for FX gains/losses
    suspense_account_id   : Suspense account for bridging
    description           : What this transaction is for
    source_type           : Type of source record
    source_id             : ID of source record
    current_user_id       : User performing the action
    app_id                : Company/Application ID
    transaction_date      : Date of transaction
    source_account_type   : 'Asset', 'Liability', 'Equity', 'Income', 'Expense'
    destination_account_type : 'Asset', 'Liability', 'Equity', 'Income', 'Expense'

    RETURNS:
    --------
    (success, message) tuple

    EXAMPLES:
    ---------

    # 1. RECEIVING PAYMENT (Customer pays you)
    # Source: Customer Credit (Liability) decreases
    # Destination: AR (Asset) increases
    process_multi_currency_through_suspense(
        source_account_id=customer_credit_id,
        destination_account_id=ar_account_id,
        source_amount=17.12, source_currency_id=eur_id, source_rate_id=rate_id_4000,
        destination_amount=50, destination_currency_id=usd_id, destination_rate_id=rate_id_3800,
        source_account_type="Liability", destination_account_type="Asset",
        ...
    )

    # 2. MAKING PAYMENT (You pay supplier)
    # Source: Bank (Asset) decreases
    # Destination: AP (Liability) decreases
    process_multi_currency_through_suspense(
        source_account_id=bank_account_id,
        destination_account_id=ap_account_id,
        source_amount=850, source_currency_id=eur_id, source_rate_id=rate_id_4000,
        destination_amount=1000, destination_currency_id=usd_id, destination_rate_id=rate_id_3800,
        source_account_type="Asset", destination_account_type="Liability",
        ...
    )
    """

    from utils import generate_unique_journal_number, create_transaction

    try:
        # ===== GET ACTUAL RATE VALUES using your existing function =====
        # Get source rate value
        _, source_rate_obj = get_or_create_exchange_rate_for_transaction(
            session=db_session,
            action='get',
            source_type=source_type,
            source_id=source_id
        )
        source_rate = float(source_rate_obj.rate) if source_rate_obj else 1.0

        # Get destination rate value
        _, destination_rate_obj = get_or_create_exchange_rate_for_transaction(
            session=db_session,
            action='get',
            source_type=source_type,
            source_id=source_id
        )
        destination_rate = float(destination_rate_obj.rate) if destination_rate_obj else 1.0

        # Calculate base currency equivalents for FX determination
        source_base = source_amount * source_rate
        destination_base = destination_amount * destination_rate
        fx_difference = destination_base - source_base

        logger.info(f"=== PROCESSING MULTI-CURRENCY TRANSACTION ===")
        logger.info(f"Description: {description}")
        logger.info(f"Source: {source_amount} (Rate: {source_rate}) = {source_base} UGX")
        logger.info(f"Destination: {destination_amount} (Rate: {destination_rate}) = {destination_base} UGX")
        logger.info(
            f"FX Difference: {fx_difference} {'GAIN' if fx_difference > 0 else 'LOSS' if fx_difference < 0 else 'NONE'}")

        # ===== STEP 1: Move funds to suspense (in source currency) =====
        journal1_number = generate_unique_journal_number(db_session, app_id)

        # Determine debit/credit direction based on account type
        # Assets decrease with Credit, Liabilities/Equity decrease with Debit
        source_dr_cr = "D" if source_account_type in ['Liability', 'Equity', 'Income'] else "C"
        suspense_dr_cr = "C" if source_dr_cr == "D" else "D"

        lines_journal1 = [{
            "subcategory_id": source_account_id,
            "amount": source_amount,
            "dr_cr": source_dr_cr,
            "description": f"Move to suspense - {description}",
            "source_type": source_type,
            "source_id": source_id
        }, {
            "subcategory_id": suspense_account_id,
            "amount": source_amount,
            "dr_cr": suspense_dr_cr,
            "description": f"Funds in suspense - {description}",
            "source_type": source_type,
            "source_id": source_id
        }]

        create_transaction(
            db_session=db_session,
            date=transaction_date,
            currency=source_currency_id,
            created_by=current_user_id,
            app_id=app_id,
            journal_number=journal1_number,
            journal_ref_no=f"{source_type}-{source_id}-SUSPENSE-IN",
            narration=f"Suspense entry - {description}",
            exchange_rate_id=source_rate_id,
            lines=lines_journal1
        )

        logger.info(f"STEP 1 complete: Moved {source_amount} from account {source_account_id} to suspense")

        # ===== STEP 2: Apply from suspense to destination (in destination currency) =====
        journal2_number = generate_unique_journal_number(db_session, app_id)

        # Assets increase with Debit, Liabilities/Equity increase with Credit
        destination_dr_cr = "D" if destination_account_type == 'Asset' else "C"
        suspense_dr_cr_step2 = "C" if destination_dr_cr == "D" else "D"

        lines_journal2 = [{
            "subcategory_id": suspense_account_id,
            "amount": destination_amount,
            "dr_cr": suspense_dr_cr_step2,
            "description": f"Apply suspense to {description}",
            "source_type": source_type,
            "source_id": source_id
        }, {
            "subcategory_id": destination_account_id,
            "amount": destination_amount,
            "dr_cr": destination_dr_cr,
            "description": f"Applied to {description}",
            "source_type": source_type,
            "source_id": source_id
        }]

        create_transaction(
            db_session=db_session,
            date=transaction_date,
            currency=destination_currency_id,
            created_by=current_user_id,
            app_id=app_id,
            journal_number=journal2_number,
            journal_ref_no=f"{source_type}-{source_id}-SUSPENSE-OUT",
            narration=f"Suspense application - {description}",
            exchange_rate_id=destination_rate_id,
            lines=lines_journal2
        )

        logger.info(f"STEP 2 complete: Applied {destination_amount} from suspense to account {destination_account_id}")

        # ===== STEP 3: Handle FX gain/loss in base currency =====
        if abs(fx_difference) > 0.01:
            journal3_number = generate_unique_journal_number(db_session, app_id)

            if fx_difference > 0:  # GAIN
                lines_journal3 = [{
                    "subcategory_id": suspense_account_id,
                    "amount": abs(fx_difference),
                    "dr_cr": "D",
                    "description": f"FX gain on {description}",
                    "source_type": source_type,
                    "source_id": source_id
                }, {
                    "subcategory_id": fx_account_id,
                    "amount": abs(fx_difference),
                    "dr_cr": "C",
                    "description": f"Foreign exchange gain on {description}",
                    "source_type": source_type,
                    "source_id": source_id
                }]
                logger.info(f"FX GAIN of {abs(fx_difference)} UGX")

            else:  # LOSS
                lines_journal3 = [{
                    "subcategory_id": fx_account_id,
                    "amount": abs(fx_difference),
                    "dr_cr": "D",
                    "description": f"Foreign exchange loss on {description}",
                    "source_type": source_type,
                    "source_id": source_id
                }, {
                    "subcategory_id": suspense_account_id,
                    "amount": abs(fx_difference),
                    "dr_cr": "C",
                    "description": f"FX loss on {description}",
                    "source_type": source_type,
                    "source_id": source_id
                }]
                logger.info(f"FX LOSS of {abs(fx_difference)} UGX")

            create_transaction(
                db_session=db_session,
                date=transaction_date,
                currency=base_currency_id,
                created_by=current_user_id,
                app_id=app_id,
                journal_number=journal3_number,
                journal_ref_no=f"{source_type}-{source_id}-FX",
                narration=f"FX adjustment - {description}",
                exchange_rate_id=None,
                lines=lines_journal3
            )

            logger.info(f"STEP 3 complete: Recorded FX {'gain' if fx_difference > 0 else 'loss'}")

        logger.info(f"=== TRANSACTION COMPLETE: {description} ===")
        return True, f"Successfully processed {description}"

    except Exception as e:
        logger.error(f"Error in process_multi_currency_through_suspense: {str(e)}\n{traceback.format_exc()}")
        return False, str(e)


def process_transaction_exchange_rate(db_session, request, currency_id, transaction_date,
                                      app_id, created_by, source_type, source_id=None):
    """
    ===========================================================================
    HANDLE EXCHANGE RATE FOR FOREIGN CURRENCY TRANSACTIONS
    ===========================================================================

    WHAT THIS FUNCTION DOES:
    ------------------------
    Processes exchange rate input from a form submission for any transaction
    that involves foreign currency. Creates or retrieves an exchange rate
    record and validates the rate.

    WHEN TO USE:
    ------------
    This function should be called for ANY transaction that involves currency:
    - Sales Invoices (add_sales_invoice)
    - Purchase Orders (add_purchase_order)
    - Direct Purchase Transactions (add_direct_purchase_transaction)
    - Payments (record_payment, process_payment)
    - Credit Notes
    - Any other transaction with foreign currency

    HOW IT WORKS:
    -------------
    1. Checks if the transaction currency differs from the base currency
    2. If same currency, returns None (no exchange rate needed)
    3. If different currency, validates the exchange rate input
    4. Creates or retrieves an exchange rate record in the database
    5. Returns the exchange rate ID, value, and object for further use

    PARAMETERS:
    -----------
    db_session           : Database session (required)
    request              : Flask request object containing form data (required)
    currency_id          : The currency ID of the transaction (e.g., USD) (required)
    transaction_date     : Date of the transaction (required)
    app_id               : Application ID (required)
    created_by           : User ID creating the transaction (required)
    source_type          : Type of source (e.g., 'invoice', 'purchase_order',
                          'payment', 'credit_note') (required)
    source_id            : Optional source ID (used after transaction is created)
                          Pass None initially, then update after transaction is created

    RETURNS:
    --------
    tuple: (exchange_rate_id, exchange_rate_value, rate_obj)
        - exchange_rate_id : Database ID of the exchange rate (or None)
        - exchange_rate_value : The actual exchange rate value (or None)
        - rate_obj : The ExchangeRate object (or None)

    EXCEPTIONS:
    -----------
    Raises ValueError if:
        - Exchange rate is required but missing
        - Exchange rate value is invalid (<= 0)
        - Exchange rate format is invalid (not a number)

    EXAMPLE USAGE:
    --------------
    try:
        exchange_rate_id, exchange_rate_value, rate_obj = handle_exchange_rate(
            db_session=db_session,
            request=request,
            currency_id=currency_id,
            transaction_date=invoice_date,
            app_id=app_id,
            created_by=current_user.id,
            source_type='invoice',
            source_id=None
        )

        # Create your transaction with the exchange rate
        new_invoice = SalesInvoice(
            # ... other fields ...
            exchange_rate_id=exchange_rate_id
        )
        db_session.add(new_invoice)
        db_session.flush()

        # Update the exchange rate with the actual transaction ID
        if rate_obj:
            rate_obj.source_id = new_invoice.id
            db_session.add(rate_obj)

    except ValueError as e:
        return jsonify({'success': False, 'message': str(e)}), 400

    ===========================================================================
    """
    # Get base currency ID from the form (this should be passed from the template)
    base_currency_id = int(request.form.get('base_currency_id')) if request.form.get('base_currency_id') else None

    # Get the exchange rate entered by the user
    exchange_rate = request.form.get('exchange_rate')

    # Initialize return values
    exchange_rate_id = None
    rate_obj = None
    exchange_rate_value = None

    # Check if we need an exchange rate (transaction currency different from base currency)
    if base_currency_id and int(currency_id) != base_currency_id:
        # FOREIGN CURRENCY TRANSACTION - Exchange rate is REQUIRED

        # Validate that the user entered an exchange rate
        if not exchange_rate or exchange_rate.strip() == '':
            raise ValueError('Exchange rate is required for foreign currency transactions')

        # Validate that the exchange rate is a valid positive number
        try:
            exchange_rate_value = float(exchange_rate)
            if exchange_rate_value <= 0:
                raise ValueError('Exchange rate must be greater than 0')
        except ValueError as e:
            raise ValueError(f'Invalid exchange rate format: {e}')

        # Create or retrieve an exchange rate record in the database
        # This function handles both creation and retrieval of existing rates
        rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
            session=db_session,
            action='create',  # Always create a new rate for this transaction
            from_currency_id=currency_id,  # Source currency (e.g., USD)
            to_currency_id=base_currency_id,  # Target currency (e.g., UGX)
            rate_value=exchange_rate_value,  # The rate value
            rate_date=transaction_date,  # Date the rate applies
            app_id=app_id,  # Company/Application ID
            created_by=created_by,  # User who created this
            source_type=source_type,  # What type of transaction
            source_id=source_id,  # ID of the transaction (if known)
            currency_exchange_transaction_id=None  # For future currency conversions
        )
        exchange_rate_id = rate_id

    # Return the exchange rate details
    # If no exchange rate was needed, all values will be None
    return exchange_rate_id, exchange_rate_value, rate_obj


def handle_exchange_rate_for_edit(db_session, document, currency_id, base_currency_id, exchange_rate_value, rate_date,
                                  current_user_id, source_type):
    """
    Handle exchange rate for document edits - handles all scenarios
    """
    is_foreign = int(currency_id) != int(base_currency_id)


    # Scenario 1: Document is now in base currency
    if not is_foreign:
        if document.exchange_rate_id:
            document.exchange_rate_id = None
        return None

    # Convert to float FIRST
    try:
        exchange_rate_float = float(exchange_rate_value) if exchange_rate_value else 0
        exchange_rate_value = exchange_rate_float
    except ValueError:
        raise ValueError('Invalid exchange rate format')

    # Scenario 2: Document is in foreign currency - need exchange rate
    if not exchange_rate_value or exchange_rate_value <= 0:
        raise ValueError('Exchange rate is required for foreign currency transactions')

    exchange_rate_value = float(exchange_rate_value)

    # Scenario 2a: Document already had an exchange rate (update existing)
    if document.exchange_rate_id and document.exchange_rate:
        # Update existing rate
        document.exchange_rate.rate = exchange_rate_value
        document.exchange_rate.rate_date = rate_date
        document.exchange_rate.updated_at = datetime.datetime.now()
        document.exchange_rate.updated_by = current_user_id
        db_session.add(document.exchange_rate)
        return document.exchange_rate.id

    # Scenario 2b: Document didn't have an exchange rate (was base, now foreign)
    # Create new exchange rate
    rate_id, rate_obj = get_or_create_exchange_rate_for_transaction(
        session=db_session,
        action='create',
        from_currency_id=currency_id,
        to_currency_id=base_currency_id,
        rate_value=exchange_rate_value,
        rate_date=rate_date,
        app_id=document.app_id,
        created_by=current_user_id,
        source_type=source_type,
        source_id=document.id,
        currency_exchange_transaction_id=None
    )
    document.exchange_rate_id = rate_id
    return rate_id
