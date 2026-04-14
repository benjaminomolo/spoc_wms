import datetime
import traceback
from decimal import Decimal

from sqlalchemy import or_

from models import CustomerCredit, CreditApplication, ChartOfAccounts, Category, Vendor, PaymentAllocation

import logging

from utils import empty_to_none, create_transaction
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj

logger = logging.getLogger(__name__)


def get_or_create_vendor(db_session, app_id, customer_data=None, customer_group_id=None):
    """
    Get or create a vendor/customer for POS transactions

    Args:
        db_session: Database session
        app_id: Company ID
        customer_data: Dict with customer details (optional)
        customer_group_id: ID of customer group (optional)

    Returns:
        Vendor object
    """
    from app import Vendor  # Import your Vendor model

    # Default to walk-in customer if no customer data provided
    if not customer_data:
        # Try to find existing walk-in customer
        walk_in_customer = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.vendor_name == 'Walk-in Customer',
            Vendor.is_one_time == False
        ).first()

        if walk_in_customer:
            return walk_in_customer

        # Create new walk-in customer
        walk_in_customer = Vendor(
            vendor_name='Walk-in Customer',
            vendor_id=f'WALK-IN',
            vendor_type='customer',
            vendor_status='Active',
            is_one_time=False,
            app_id=app_id,
            customer_group_id=customer_group_id,
            created_at=datetime.datetime.now()
        )
        db_session.add(walk_in_customer)
        db_session.flush()
        return walk_in_customer

    # Handle one-time customer creation
    vendor_name = customer_data.get('name', 'One-time Customer')
    email = customer_data.get('email')
    phone = customer_data.get('phone')

    # Check if customer already exists by email or phone
    existing_customer = None
    if email:
        existing_customer = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.email == email,
            Vendor.vendor_type == 'customer'
        ).first()

    if not existing_customer and phone:
        existing_customer = db_session.query(Vendor).filter(
            Vendor.app_id == app_id,
            Vendor.tel_contact == phone,
            Vendor.vendor_type == 'customer'
        ).first()

    if existing_customer:
        # Update customer group if provided
        if customer_group_id and existing_customer.customer_group_id != customer_group_id:
            existing_customer.customer_group_id = customer_group_id
        return existing_customer

    # Create new one-time customer
    one_time_customer = Vendor(
        vendor_name=vendor_name,
        vendor_id=f'OT-{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}',
        email=email,
        tel_contact=phone,
        vendor_type='customer',
        vendor_status='Active',
        is_one_time=True,
        app_id=app_id,
        customer_group_id=customer_group_id,
        created_at=datetime.datetime.now(),
        address=customer_data.get('address'),
        city=customer_data.get('city'),
        state_province=customer_data.get('state_province'),
        postal_code=customer_data.get('postal_code'),
        country=customer_data.get('country')
    )

    db_session.add(one_time_customer)
    db_session.flush()
    return one_time_customer


def create_credit_from_overpayment(invoice, payment, overpayment_amount, current_user, db_session=None):
    should_close_session = False
    if db_session is None:
        db_session = Session()
        should_close_session = True

    try:
        credit = CustomerCredit(
            app_id=current_user.app_id,
            customer_id=invoice.customer_id,
            payment_allocation_id=payment.id,
            original_amount=overpayment_amount,
            available_amount=overpayment_amount,
            currency_id=invoice.currency,
            exchange_rate_id=payment.exchange_rate_id,
            created_date=datetime.datetime.now(),
            issued_date=datetime.datetime.now(),
            status='active',
            credit_reason='overpayment',
            reference_number=payment.reference,
            created_by=current_user.id
        )
        db_session.add(credit)
        db_session.flush()  # Get ID without committing

        if should_close_session:
            db_session.commit()
            db_session.close()
        else:
            db_session.flush()  # Let caller commit

        return credit

    except Exception as e:
        if should_close_session:
            db_session.rollback()
            db_session.close()
        raise e


def create_credit_from_bulk_payment(bulk_payment, amount, current_user, db_session=None):
    """
    Create a customer credit from a bulk payment (no invoices)

    Args:
        bulk_payment: BulkPayment object
        amount: Amount to credit (usually the full payment amount)
        current_user: Current user object
        db_session: Database session (optional)
    """
    should_close_session = False
    if db_session is None:
        db_session = Session()
        should_close_session = True

    try:
        credit = CustomerCredit(
            app_id=current_user.app_id,
            customer_id=bulk_payment.customer_id,
            bulk_payment_id=bulk_payment.id,  # Link directly to bulk payment
            original_amount=amount,
            available_amount=amount,
            currency_id=bulk_payment.currency_id,
            created_date=datetime.datetime.now(),
            issued_date=bulk_payment.payment_date,
            status='active',
            credit_reason='prepayment',  # or 'unallocated_payment'
            reference_number=bulk_payment.reference,
            created_by=current_user.id
        )
        db_session.add(credit)
        db_session.flush()

        if should_close_session:
            db_session.commit()
            db_session.close()
        else:
            db_session.flush()

        return credit

    except Exception as e:
        if should_close_session:
            db_session.rollback()
            db_session.close()
        raise e



def get_customer_credits(customer_id, app_id, db_session=None):
    return db_session.query(CustomerCredit).filter(
        CustomerCredit.customer_id == customer_id,
        CustomerCredit.app_id == app_id,
        CustomerCredit.status == 'active',
        CustomerCredit.available_amount > 0,
        or_(
            CustomerCredit.expires_date.is_(None),
            CustomerCredit.expires_date >= datetime.datetime.now()
        )
    ).all()


def normalize_vendor_type(vendor_type):
    """Normalize vendor type to only 'vendor' or 'customer'"""
    if not vendor_type:
        return 'vendor'  # Default to vendor if type is not specified

    vendor_type_lower = vendor_type.lower().strip()

    # Map various customer variations to 'customer'
    if vendor_type_lower in ['customer', 'customers', 'client', 'clients', 'buyer', 'buyers']:
        return 'customer'

    # Map various vendor variations to 'vendor'
    elif vendor_type_lower in ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers']:
        return 'vendor'

    # Default to 'vendor' for any unrecognized types
    else:
        return 'vendor'


def get_or_create_customer_credit_account(db_session, app_id, created_by_user_id=None):
    """
    Get or create the system customer credit account (admin account not modifiable by users)
    Uses 'Liability' as parent_account_type and creates system-specific categories
    """
    try:
        # Try to find existing customer credit account
        credit_account = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.sub_category == 'Customer Credits',
            ChartOfAccounts.is_system_account == True
        ).first()

        if credit_account:
            return credit_account.id

        # Step 1: Find or create SYSTEM-ONLY liability category structure
        # Use unique category names to avoid conflicts with user categories
        system_liability_category = db_session.query(Category).filter(
            Category.app_id == app_id,
            Category.account_type == 'liability',
            Category.category == 'System Liabilities',  # Unique system category name
            Category.category_id == 'SYS-2000'  # System-specific ID
        ).first()

        if not system_liability_category:
            # Create system liability category structure
            system_liability_category = Category(
                account_type='liability',
                category_id='SYS-2000',  # System-specific ID prefix
                category='System Liabilities',  # Unique system category name
                app_id=app_id
            )
            db_session.add(system_liability_category)
            db_session.flush()

        # Step 2: Create the customer credit account in ChartOfAccounts
        credit_account = ChartOfAccounts(
            # Basic account information - using Liability as parent type
            parent_account_type='Liability',
            parent_account_type_id='SYS-2000',
            category_id='SYS-2000',
            category_fk=system_liability_category.id,
            category='System Liabilities',  # System category
            sub_category_id='SYS-2100',  # System-specific ID
            sub_category='Customer Credits',

            # Account flags
            is_bank=False,
            is_cash=False,
            is_receivable=False,
            is_payable=True,
            is_monetary=True,
            is_active=True,
            is_system_account=True,  # Mark as system account

            # Financial properties
            normal_balance='Credit',  # Liabilities have credit normal balance

            # Ownership
            created_by=created_by_user_id,
            app_id=app_id
        )

        db_session.add(credit_account)
        db_session.commit()

        logger.info(f"Created customer credit account with ID: {credit_account.id} for app_id: {app_id}")
        return credit_account.id

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error getting/creating customer credit account for app_id {app_id}: {str(e)}")
        raise


def get_or_create_write_off_account(db_session, app_id, created_by_user_id=None):
    """
    Get or create the system write-off account (miscellaneous income)
    Uses 'Income' as parent_account_type and creates system-specific categories
    """
    try:
        # Try to find existing write-off account
        write_off_account = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            ChartOfAccounts.sub_category == 'Miscellaneous Income',
            ChartOfAccounts.is_system_account == True
        ).first()

        if write_off_account:
            return write_off_account.id

        # Step 1: Find or create SYSTEM-ONLY income category structure
        system_income_category = db_session.query(Category).filter(
            Category.app_id == app_id,
            Category.account_type == 'income',
            Category.category == 'System Income',  # Unique system category name
            Category.category_id == 'SYS-4000'  # System-specific ID
        ).first()

        if not system_income_category:
            # Create system income category structure
            system_income_category = Category(
                account_type='Income',
                category_id='SYS-4000',  # System-specific ID prefix
                category='System Income',  # Unique system category name
                app_id=app_id
            )
            db_session.add(system_income_category)
            db_session.flush()

        # Step 2: Create the write-off account in ChartOfAccounts
        write_off_account = ChartOfAccounts(
            # Basic account information
            parent_account_type='Income',
            parent_account_type_id='SYS-4000',
            category_id='SYS-4000',
            category_fk=system_income_category.id,
            category='System Income',
            sub_category_id='SYS-4100',  # System-specific ID
            sub_category='System Miscellaneous Income',

            # Account flags
            is_bank=False,
            is_cash=False,
            is_receivable=False,
            is_payable=False,
            is_monetary=True,
            is_active=True,
            is_system_account=True,  # Mark as system account

            # Financial properties
            normal_balance='Credit',  # Income accounts have credit normal balance

            # Ownership
            created_by=created_by_user_id,
            app_id=app_id
        )

        db_session.add(write_off_account)
        db_session.flush()

        logger.info(f"Created write-off account with ID: {write_off_account.id} for app_id: {app_id}")
        return write_off_account.id

    except Exception as e:
        db_session.rollback()
        logger.error(f"Error getting/creating write-off account for app_id {app_id}: {str(e)}")
        raise


def handle_party_logic(form_data, app_id, db_session, party_type="Vendor"):
    """
    Handle party logic for both existing and one-time parties (suppliers/customers)

    Args:
        form_data: The form data containing the party ID/name
        app_id: Company/application ID
        db_session: Database session
        party_type: Type of party - "Vendor" for suppliers, "Client" for customers

    Returns:
        tuple: (party_id, party_name) or (None, None) if not found/created
    """
    try:
        # Try different field names
        party_id_str = form_data.get('party_id') or form_data.get('supplier_id') or form_data.get(
            'customer_id') or form_data.get('supplier_id[]')
        party_name_from_form = form_data.get('party_name') or form_data.get('supplier_name') or form_data.get(
            'customer_name', '') or form_data.get('supplier_name[]')

        party_id = empty_to_none(party_id_str)
        party_name = party_name_from_form.strip() if party_name_from_form else None

        # CASE 1: No party ID but we have a party name
        if (not party_id or party_id.strip() == "") and party_name:
            # Check if one-time party already exists with this name
            existing_party = db_session.query(Vendor).filter_by(
                vendor_name=party_name,
                is_one_time=True,
                app_id=app_id,
                vendor_type=party_type
            ).first()

            if existing_party:
                return existing_party.id, existing_party.vendor_name
            else:
                # Create new one-time party
                new_party = Vendor(
                    vendor_name=party_name,
                    is_one_time=True,
                    vendor_type=party_type,
                    app_id=app_id,
                    created_at=datetime.datetime.now()
                )
                db_session.add(new_party)
                db_session.flush()
                return new_party.id, new_party.vendor_name

        # CASE 2: We have a party ID
        if party_id and party_id.strip() != "":
            # If it's a numeric ID, return it
            if party_id.isdigit():
                party_id_int = int(party_id)
                # Verify the party exists
                party = db_session.query(Vendor).filter_by(
                    id=party_id_int,
                    app_id=app_id
                ).first()
                if party:
                    return party_id_int, party.vendor_name
                else:
                    # Party ID doesn't exist, create one-time party with provided name or ID as name
                    party_name = party_name or party_id.strip()
            else:
                # It's a name (one-time party)
                party_name = party_name or party_id.strip()

            # Create one-time party (reusing the same logic as above)
            existing_party = db_session.query(Vendor).filter_by(
                vendor_name=party_name,
                is_one_time=True,
                app_id=app_id,
                vendor_type=party_type
            ).first()

            if existing_party:
                return existing_party.id, existing_party.vendor_name
            else:
                new_party = Vendor(
                    vendor_name=party_name,
                    is_one_time=True,
                    vendor_type=party_type,
                    app_id=app_id,
                    created_at=datetime.datetime.now()
                )
                db_session.add(new_party)
                db_session.flush()
                return new_party.id, new_party.vendor_name

        # CASE 3: No ID and no name
        return None, None

    except Exception as e:
        logger.error(f"Error in handle_party_logic: {str(e)}")
        # Return what we have
        return None, party_name


def apply_existing_credits_to_invoice(invoice, selected_credits, current_user, db_session, payment_date=None):
    """
    SCENARIO 2: Apply existing customer credits to an invoice.
    Uses exchange rates provided from frontend (already calculated at credit issue date).
    """
    try:
        if not selected_credits:
            return Decimal('0.00'), [], "No credits selected"

        total_applied = Decimal('0.00')
        applications = []

        for credit_data in selected_credits:
            credit_id = credit_data['id']
            amount_to_apply = Decimal(str(credit_data['amount']))

            # The frontend already sent the converted amount?
            # If not, we need the rate
            exchange_rate = credit_data.get('exchange_rate')
            converted_amount = credit_data.get('converted_amount')

            # Get the credit (for validation and updating balance)
            credit = db_session.query(CustomerCredit).filter_by(
                id=credit_id,
                customer_id=invoice.customer_id,
                app_id=current_user.app_id,
                status='active'
            ).first()

            if not credit:
                return total_applied, applications, f"Credit {credit_id} not found or not active"

            if credit.available_amount < amount_to_apply:
                return total_applied, applications, f"Insufficient credit. Available: {credit.available_amount}, Requested: {amount_to_apply}"

            # Determine amount in invoice currency
            if credit.currency_id == invoice.currency:
                amount_in_invoice = amount_to_apply
                exchange_rate = 1
                exchange_rate_id = None
            else:
                # Use the rate provided from frontend (already calculated at credit issue date)
                if converted_amount is not None:
                    # Frontend already calculated the converted amount
                    amount_in_invoice = Decimal(str(converted_amount))
                    exchange_rate = float(amount_in_invoice) / float(amount_to_apply) if amount_to_apply else 1
                elif exchange_rate is not None:
                    # Frontend provided the rate, calculate converted amount
                    amount_in_invoice = amount_to_apply * Decimal(str(exchange_rate))
                else:
                    # Fallback to database query if rate not provided
                    rate_obj, rate = get_exchange_rate_and_obj(
                        db_session,
                        credit.currency_id,
                        invoice.currency,
                        app_id=current_user.app_id,
                        as_of_date=credit.issued_date
                    )
                    amount_in_invoice = amount_to_apply * rate
                    exchange_rate = float(rate)

                exchange_rate_id = credit.exchange_rate_id  # Use credit's stored rate ID

            # Create credit application record
            application = CreditApplication(
                app_id=current_user.app_id,
                credit_id=credit.id,
                target_type='sales_invoice',
                target_id=invoice.id,
                payment_allocation_id=None,  # Will be set after allocation created
                applied_amount=amount_to_apply,
                applied_amount_invoice_currency=amount_in_invoice,
                application_date=payment_date or datetime.datetime.now(),
                status='applied',
                applied_by=current_user.id,
                notes=f"Applied {amount_to_apply} {credit.currency.user_currency} to invoice {invoice.invoice_number}"
            )
            db_session.add(application)

            # Update credit balance
            credit.available_amount -= amount_to_apply
            if credit.available_amount == 0:
                credit.status = 'used'

            total_applied += amount_in_invoice
            applications.append(application)

            logger.info(f"Applied {amount_to_apply} from credit #{credit.id} to invoice #{invoice.id}")

        db_session.flush()
        return total_applied, applications, None

    except Exception as e:
        logger.error(f"Error applying credits: {str(e)}")
        return Decimal('0.00'), [], str(e)


def delete_credit_applications(db_session, payment_allocation_id, current_user):
    """
    Delete all credit applications linked to a payment allocation.
    Restores credit balances and permanently removes application records.

    Args:
        db_session: Database session
        payment_allocation_id: ID of the payment allocation being deleted
        current_user: Current user object

    Returns:
        tuple: (success, message, deleted_count)
    """
    try:
        if not payment_allocation_id:
            return True, "No payment allocation ID provided", 0

        # Find credit applications directly by payment_allocation_id
        credit_apps = db_session.query(CreditApplication).filter_by(
            payment_allocation_id=payment_allocation_id,
            status='applied'
        ).all()

        deleted_count = 0

        for credit_app in credit_apps:
            # Get the associated credit
            credit = credit_app.customer_credit

            if not credit:
                logger.warning(f"Credit not found for application #{credit_app.id}, skipping balance restore")
            else:
                # Restore the credit balance
                credit.available_amount += credit_app.applied_amount
                credit.status = 'active'  # In case it was marked as used
                credit.updated_at = datetime.datetime.now()
                logger.info(f"Restored {credit_app.applied_amount} to credit #{credit.id}")

            # Delete the application record
            db_session.delete(credit_app)
            deleted_count += 1
            logger.info(f"Deleted credit application #{credit_app.id}")

        if deleted_count > 0:
            db_session.flush()
            return True, f"Successfully deleted {deleted_count} credit application(s) and restored balances", deleted_count
        else:
            return True, "No credit applications found for this payment allocation", 0

    except Exception as e:
        logger.error(f"Error deleting credit applications: {str(e)}")
        return False, str(e), 0


# Optional helper to check if there are any credit applications before proceeding
def has_credit_applications(db_session, payment_allocation_id):
    """
    Check if a payment allocation has any linked credit applications.

    Returns:
        bool: True if credit applications exist, False otherwise
    """
    if not payment_allocation_id:
        return False

    count = db_session.query(CreditApplication).filter_by(
        payment_allocation_id=payment_allocation_id
    ).count()

    return count > 0

def cancel_all_credit_relationships(db_session, payment_allocation, current_user, visited=None):
    """
    Master function to handle ALL credit relationships in one pass.

    Uses relationships and avoids redundant processing.

    Args:
        db_session: SQLAlchemy session
        payment_allocation: PaymentAllocation object being cancelled
        current_user: Current user for audit logging
        visited: Set of processed payment allocation IDs (prevents cycles)

    Returns:
        tuple: (success, message, stats_dict)

    """
    if visited is None:
        visited = set()

    # Prevent infinite recursion
    if payment_allocation.id in visited:
        return True, "Already processed", {}

    visited.add(payment_allocation.id)

    stats = {
        'applications_deleted': 0,
        'source_credits_restored': 0,
        'created_credits_deleted': 0,
        'downstream_transactions_cancelled': 0
    }

    try:
        # ===== PART 1: Handle credit applications (credits USED in this payment) =====
        if hasattr(payment_allocation, 'credit_applications') and payment_allocation.credit_applications:
            for application in list(payment_allocation.credit_applications):
                source_credit = application.customer_credit
                if source_credit:
                    source_credit.available_amount += application.applied_amount

                    if source_credit.available_amount >= source_credit.original_amount:
                        source_credit.status = 'active'
                    elif source_credit.available_amount > 0:
                        source_credit.status = 'partial'

                    stats['source_credits_restored'] += 1

                db_session.delete(application)
                stats['applications_deleted'] += 1

        # ===== PART 2: Handle created credits (credits FROM overpayment) =====
        if hasattr(payment_allocation, 'created_credits') and payment_allocation.created_credits:
            for credit in list(payment_allocation.created_credits):
                if credit.credit_applications:
                    for app in credit.credit_applications:
                        if app.payment_allocation:
                            downstream_allocation = app.payment_allocation

                            # RECURSIVE CALL - just pass the downstream allocation
                            sub_success, sub_msg, sub_stats = cancel_all_credit_relationships(
                                db_session,
                                downstream_allocation,
                                current_user,
                                visited
                            )

                            if sub_success:
                                # Merge stats
                                for key in sub_stats:
                                    stats[key] = stats.get(key, 0) + sub_stats[key]

                                # Get transaction using sale_transaction_id from payment_allocation
                                if downstream_allocation.sale_transaction_id:
                                    downstream_transaction = db_session.query(SalesTransaction).filter_by(
                                        id=downstream_allocation.sale_transaction_id
                                    ).first()

                                    if downstream_transaction and downstream_transaction.payment_status != SalesPaymentStatus.cancelled:
                                        downstream_transaction.payment_status = SalesPaymentStatus.cancelled
                                        downstream_transaction.is_posted_to_ledger = False
                                        stats['downstream_transactions_cancelled'] += 1
                                        logger.info(f"Cancelled downstream transaction #{downstream_allocation.sale_transaction_id}")

                                # Delete downstream allocation
                                if downstream_allocation:
                                    db_session.delete(downstream_allocation)
                            else:
                                return False, f"Failed to clean up downstream transaction: {sub_msg}", stats

                    db_session.refresh(credit)
                    if not credit.credit_applications:
                        db_session.delete(credit)
                        stats['created_credits_deleted'] += 1
                else:
                    db_session.delete(credit)
                    stats['created_credits_deleted'] += 1

        return True, "Success", stats

    except Exception as e:
        logger.error(f"Error in cancel_all_credit_relationships: {str(e)}")
        return False, str(e), stats
