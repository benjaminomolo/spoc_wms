#app/routes/general_ledger/chart_of_accounts.py

import logging
import math
import traceback
from datetime import datetime
from decimal import Decimal

from flask import flash, redirect, request, render_template, jsonify, url_for
from flask_login import login_required, current_user
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from sqlalchemy.orm import joinedload

from ai import get_base_currency, get_exchange_rate
from db import Session
from decorators import role_required
from models import ChartOfAccounts, Category, Company, Module, OpeningBalance, ActivityLog, JournalEntry, Journal, \
    OpeningBalanceVendor
from services.chart_of_accounts_helpers import calculate_account_balance, has_open_transactions, \
    get_retained_earnings_account_id
from services.post_to_ledger import post_opening_balances_to_ledger
from . import general_ledger_bp

logger = logging.getLogger()


@general_ledger_bp.route('/add-charts-of-accounts')
@login_required
@role_required(['Admin', 'Supervisor', 'Viewer'])
def add_chart_of_accounts():
    user_session = Session()
    try:
        chart_of_accounts_data = user_session.query(ChartOfAccounts) \
            .filter_by(app_id=current_user.app_id) \
            .options(
            joinedload(ChartOfAccounts.opening_balances)  # load the OpeningBalance
            .joinedload(OpeningBalance.vendor_balances)  # then load OpeningBalanceVendor
            .joinedload(OpeningBalanceVendor.vendor)  # then load Vendor info
        ) \
            .all()
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

            # Fetch vendor/customer balances

            # Prepare vendor/customer balances
            vendor_balances = []
            if account.opening_balances:
                # Use the first opening balance (or iterate if multiple)
                ob = account.opening_balances[0]
                for vb in getattr(ob, 'vendor_balances', []):
                    vendor_name = getattr(vb.vendor, 'vendor_name', 'Unknown')
                    vendor_balances.append({
                        "vendor_id": vb.vendor_id,
                        "vendor_name": vendor_name,
                        "balance": float(vb.balance)
                    })

            # Add subcategory
            category['subcategories'].append({
                'id': account.id,
                'name': account.sub_category,
                'code': account.sub_category_id,
                'is_cash': account.is_cash,
                'is_bank': account.is_bank,
                'is_receivable': account.is_receivable,
                'is_payable': account.is_payable,
                'normal_balance': account.normal_balance,
                'opening_balance': float(account.opening_balances[0].balance) if account.opening_balances else 0.0,
                'vendor_balances': vendor_balances
            })

        # Fetch base currency info
        base_currency_info = get_base_currency(user_session, app_id)

        if not base_currency_info:
            return jsonify({"error": "Base currency not defined for this company"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency_name = base_currency_info["base_currency"]

        return render_template('charts-of-accounts.html', categories=categories, charts=chart_of_accounts_data,
                               company=company, role=role, modules=modules_data, account_tree=account_tree,
                               title="Chart of Accounts", base_currency=base_currency_name,
                               base_currency_id=base_currency_id,
                               module_name="General Ledger")

    except Exception as e:
        flash(f'Error fetching data: {str(e)}', 'danger')
        logger.error(f'Error is {e}\n{traceback.format_exc()} ')
        user_session.close()
        return redirect(request.referrer)

    finally:
        user_session.close()


@general_ledger_bp.route('/submit_chart_of_accounts', methods=['POST'])
@role_required(['Admin', 'Supervisor'])
@login_required
def submit_chart_of_accounts():
    if current_user.is_authenticated:
        user_session = Session()

        try:
            # Get JSON data instead of form data
            data = request.get_json()

            logger.info(f'Data is {data}')

            # Validate required fields
            if not all(key in data for key in ['inputAccountType', 'parentCategory',
                                               'subCategories', 'subCategoryIds']):
                return jsonify({'success': False, 'error': 'Missing required fields'}), 400

            # Ensure the category exists before entering the loop
            category = user_session.query(Category).filter_by(app_id=current_user.app_id,
                                                              category_id=data['parentCategoryId']).first()

            # Process each sub-category
            for i in range(len(data['subCategories'])):
                sub_category_name = data['subCategories'][i].strip().lower()
                # Prevent manual creation of Retained Earnings
                if 'retained earnings' in sub_category_name:
                    return jsonify({
                        'success': False,
                        'error': "You cannot manually create a Retained Earnings account. This account is managed automatically."
                    }), 400

                # Check if the sub_category_id already exists for the current app_id
                existing_entry = user_session.query(ChartOfAccounts).filter_by(
                    app_id=current_user.app_id,
                    sub_category_id=data['subCategoryIds'][i]
                ).first()

                if existing_entry:
                    return jsonify({'success': False,
                                    'error': f'Duplicate Sub-category ID "{data["subCategoryIds"][i]}" found for your company.'}), 409

                # Create new account entry
                chart_of_accounts_entry = ChartOfAccounts(
                    app_id=current_user.app_id,
                    category_fk=category.id,
                    category_id=data['parentCategoryId'],
                    category=data['parentCategory'],
                    parent_account_type=data['inputAccountType'],
                    parent_account_type_id=data['inputAccountTypeId'],
                    sub_category_id=data['subCategoryIds'][i],
                    sub_category=data['subCategories'][i],
                    normal_balance=data['normalBalances'][i] if i < len(data['normalBalances']) else 'Debit',
                    is_bank=data['isBank'][i] if i < len(data['isBank']) else False,
                    is_cash=data['isCash'][i] if i < len(data['isCash']) else False,
                    is_receivable=data['isReceivable'][i] if i < len(data.get('isReceivable', [])) else False,
                    is_payable=data['isPayable'][i] if i < len(data.get('isPayable', [])) else False,
                    report_section_id=(
                        data['reportSectionIds'][i] if i < len(data['reportSectionIds']) and data['reportSectionIds'][
                            i] != ''
                        else None
                    ),
                    created_by=current_user.id
                )
                user_session.add(chart_of_accounts_entry)

            user_session.commit()
            return jsonify({'success': True})

        except Exception as e:
            user_session.rollback()
            logger.error(f'An error occurred while submitting chart of accounts {str(e)}')
            return jsonify({'success': False, 'error': str(e)}), 500
        finally:
            user_session.close()

    return jsonify({'success': False, 'error': 'Not authenticated'}), 401


# --- SAVE OPENING BALANCES API ---

@general_ledger_bp.route('/api/opening-balances', methods=['POST'])
@login_required
def save_opening_balances():
    user_session = Session()
    try:
        data = request.get_json()
        conversion_date = datetime.strptime(data['conversion_date'], '%Y-%m-%d').date()
        balances = data['balances']
        base_currency_id = int(data.get('currency_id'))
        retained_earnings_amount = float(data.get('retained_earnings', 0.0))

        # First delete all vendor balances
        vendor_deleted_count = user_session.query(OpeningBalanceVendor).filter_by(
            app_id=current_user.app_id
        ).delete()

        # Then delete all opening balances
        opening_balance_deleted_count = user_session.query(OpeningBalance).filter_by(
            app_id=current_user.app_id
        ).delete()

        # Flush to ensure deletions are processed
        user_session.flush()

        # --- Step 1: Process NEW account balances ---
        for balance_data in balances:
            account_id = int(balance_data['account_id'])
            total_balance = float(balance_data.get('total_balance', 0.0))
            vendor_balances = balance_data.get('vendor_balances', [])

            # Skip if total balance is zero AND no vendor balances
            if math.isclose(total_balance, 0.0, abs_tol=0.001) and not vendor_balances:
                logger.info(f'Skipping account {account_id} (zero balance, no vendors)')
                continue

            # Create NEW OpeningBalance (all old ones were deleted above)
            ob = OpeningBalance(
                account_id=account_id,
                balance=total_balance,
                created_by=current_user.id,
                app_id=current_user.app_id
            )
            user_session.add(ob)
            user_session.flush()  # Flush to get the ID

            # --- Step 1a: Handle vendor/customer splits ---
            for vb in vendor_balances:
                vendor_id = int(vb['vendor_id'])
                amount = float(vb['amount'])

                # Only create vendor balance entry if amount is not zero
                if not math.isclose(amount, 0.0, abs_tol=0.001):
                    vendor_ob = OpeningBalanceVendor(
                        opening_balance_id=ob.id,
                        vendor_id=vendor_id,
                        balance=amount,
                        created_by=current_user.id,
                        app_id=current_user.app_id,
                    )
                    user_session.add(vendor_ob)
                else:
                    logger.info(f'Skipped zero vendor balance for vendor_id={vendor_id}')

        # --- Step 2: Handle Retained Earnings ---
        retained_earnings_account_id = None

        # Use the helper function to get or create retained earnings account
        retained_earnings_account_id = get_retained_earnings_account_id(
                db_session=user_session,
                app_id=current_user.app_id,
                user_id=current_user.id
        )

        # Only create retained earnings if amount is not zero
        if not math.isclose(retained_earnings_amount, 0.0, abs_tol=0.001):

            # Create retained earnings opening balance
            re_ob = OpeningBalance(
                account_id=retained_earnings_account_id,
                balance=retained_earnings_amount,
                created_by=current_user.id,
                app_id=current_user.app_id
            )
            user_session.add(re_ob)
        else:
            logger.info('Skipped retained earnings (zero amount)')

        user_session.flush()

        # --- Step 3: Prepare balances for posting ---
        ledger_balances = []
        for balance_data in balances:
            total_balance = float(balance_data.get('total_balance', 0.0))
            vendor_balances = balance_data.get('vendor_balances', [])

            # Only include in ledger posting if there's a non-zero balance OR vendor balances
            if not math.isclose(total_balance, 0.0, abs_tol=0.001) or vendor_balances:
                ledger_balances.append(balance_data)

        # --- Step 4: Post to Ledger ---
        post_opening_balances_to_ledger(
            db_session=user_session,
            conversion_date=conversion_date,
            current_user=current_user,
            base_currency_id=base_currency_id,
            balances_data=ledger_balances,
            retained_earnings_account_id=retained_earnings_account_id
        )

        # --- Step 5: Save conversion date ---
        company = user_session.query(Company).filter_by(id=current_user.app_id).first()
        company.opening_balances_date = conversion_date
        company.opening_balances_set = True

        user_session.commit()
        return jsonify({'success': True, 'message': 'Opening balances saved successfully'})

    except Exception as e:
        user_session.rollback()
        logger.error(f'Error saving opening balances: {str(e)}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        user_session.close()


@general_ledger_bp.route('/api/all-balances', methods=['GET'])
@login_required
def get_all_balances():
    """Get current balances for all accounts in base currency"""
    try:
        with Session() as db_session:
            app_id = current_user.app_id

            # 1. Get company's base currency
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined for this company"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency_code = base_currency_info["base_currency"]

            # 2. Get ALL accounts
            accounts = db_session.query(ChartOfAccounts).filter(
                ChartOfAccounts.app_id == app_id
            ).all()

            # 3. Get all journal entries for these accounts
            account_ids = [account.id for account in accounts]
            journal_entries = db_session.query(JournalEntry).options(
                joinedload(JournalEntry.journal).joinedload(Journal.currency)
            ).filter(
                JournalEntry.subcategory_id.in_(account_ids),
                JournalEntry.app_id == app_id
            ).all()

            # Group journal entries by account
            entries_by_account = {}
            for entry in journal_entries:
                if entry.subcategory_id not in entries_by_account:
                    entries_by_account[entry.subcategory_id] = []
                entries_by_account[entry.subcategory_id].append(entry)

            # 4. Calculate balances for all accounts
            results = []
            totals = {
                'asset': Decimal('0'),
                'liability': Decimal('0'),
                'equity': Decimal('0'),
                'income': Decimal('0'),
                'expense': Decimal('0')
            }

            for account in accounts:
                # Track balances by currency
                currency_balances = {}  # {currency_id: {'original': Decimal, 'converted': Decimal}}

                # Get entries for this account
                entries = entries_by_account.get(account.id, [])

                for entry in entries:
                    amount = Decimal(str(entry.amount))
                    currency_id = entry.journal.currency_id if entry.journal else base_currency_id

                    # Normalize dr_cr and normal_balance to first letter uppercase
                    txn_dc = entry.dr_cr.upper()[0]  # 'D' or 'C'
                    normal_bal = account.normal_balance.upper()[0]  # 'D' or 'C'

                    # Determine if we add or subtract
                    adjusted_amount = amount if txn_dc == normal_bal else -amount

                    # Initialize currency tracking if needed
                    if currency_id not in currency_balances:
                        currency_balances[currency_id] = {
                            'original': Decimal('0'),
                            'converted': Decimal('0'),
                            'currency_code': entry.journal.currency.user_currency if entry.journal and entry.journal.currency else base_currency_code
                        }

                    # Update original balance
                    currency_balances[currency_id]['original'] += adjusted_amount

                # Convert all currency balances to base currency
                account_balance = Decimal('0')
                currency_details = []

                for currency_id, balance_info in currency_balances.items():
                    original_balance = balance_info['original']

                    if currency_id == base_currency_id:
                        # No conversion needed for base currency
                        converted_balance = original_balance
                        rate = Decimal('1')
                    else:
                        # Convert foreign currency balance using latest rate
                        try:
                            rate = get_exchange_rate(
                                db_session,
                                currency_id,
                                base_currency_id,
                                app_id
                            )
                            converted_balance = original_balance * Decimal(str(rate))
                        except Exception as e:
                            logger.error(f"Currency conversion error for account {account.id}: {str(e)}")
                            converted_balance = Decimal('0')
                            rate = Decimal('0')

                    account_balance += converted_balance

                    currency_details.append({
                        'original_balance': float(original_balance),
                        'currency_code': balance_info['currency_code'],
                        'converted_balance': float(converted_balance),
                        'exchange_rate': float(rate),
                        'is_base_currency': currency_id == base_currency_id
                    })

                # Get account type from the category
                account_type = account.categories.account_type.lower() if account.categories else 'other'

                # Update totals by account type
                if account_type in totals:
                    totals[account_type] += account_balance

                account_data = {
                    "account_id": account.id,
                    "account_name": account.sub_category,
                    "account_code": account.sub_category_id,
                    "account_type": account_type,
                    "normal_balance": account.normal_balance,
                    "balance": float(account_balance),
                    "currency_details": currency_details,
                    "base_currency_code": base_currency_code,
                    "is_cash": account.is_cash,
                    "is_bank": account.is_bank,
                    "is_receivable": account.is_receivable,
                    "is_payable": account.is_payable
                }

                # Include bank_name if account is bank and field exists
                if account.is_bank and hasattr(account, 'bank_name'):
                    account_data['bank_name'] = account.bank_name

                results.append(account_data)

            return jsonify({
                "success": True,
                "data": {
                    "accounts": results,
                    "summary": {
                        "asset_total": float(totals['asset']),
                        "liability_total": float(totals['liability']),
                        "equity_total": float(totals['equity']),
                        "income_total": float(totals['income']),
                        "expense_total": float(totals['expense']),
                        "base_currency_id": base_currency_id,
                        "base_currency_code": base_currency_code,
                        "timestamp": datetime.now().isoformat()
                    }
                }
            })

    except Exception as e:
        logger.error(f'Error getting all balances: {str(e)}')
        return jsonify({
            "success": False,
            "error": "Failed to load balances",
            "details": str(e)
        }), 500


@general_ledger_bp.route("/lock_opening_balances", methods=["POST"])
@login_required
def lock_opening_balances():
    db_session = Session()
    try:
        # 0) Check authorization
        if getattr(current_user, "role", None) != "Admin":
            return jsonify({"success": False, "message": "Only Admins can lock opening balances."}), 403

        # 1) Fetch company
        company = db_session.query(Company).filter_by(id=current_user.app_id).first()
        if not company:
            return jsonify({"success": False, "message": "Company not found."}), 404

        # 2) Check if already locked
        if company.opening_balances_locked:
            return jsonify({"success": False, "message": "Opening balances are already locked."})

        # 3) Lock opening balances
        company.opening_balances_locked = True
        db_session.commit()

        return jsonify({"success": True, "message": "Opening balances locked successfully."})

    except SQLAlchemyError as e:
        db_session.rollback()
        return jsonify({"success": False, "message": "Database error: " + str(e)}), 500
    except Exception as e:
        db_session.rollback()
        return jsonify({"success": False, "message": "Unexpected error: " + str(e)}), 500
    finally:
        db_session.close()


@general_ledger_bp.route('/api/account-status/<int:account_id>', methods=['PUT'])
@login_required
def update_account_status(account_id):
    try:
        data = request.get_json()
        is_active = data.get('is_active', True)

        # Use the same session pattern as your other endpoints
        with Session() as db_session:
            account = db_session.query(ChartOfAccounts).filter_by(id=account_id, app_id=current_user.app_id).first()
            if not account:
                return jsonify({'success': False, 'error': 'Account not found'}), 404

            # If deactivating, perform validation checks
            if not is_active:
                # Check 1: Current balance must be zero using the same logic as get_all_balances
                current_balance = calculate_account_balance(db_session, account_id, current_user.app_id)

                if abs(current_balance) > 0.01:  # Allow for small rounding errors
                    base_currency_info = get_base_currency(db_session, current_user.app_id)
                    base_currency_code = base_currency_info["base_currency"] if base_currency_info else "USD"

                    return jsonify({
                        'success': False,
                        'error': f'Cannot deactivate account with non-zero balance ({current_balance:,.2f}). Please zero out the balance first.',
                        'balance': current_balance,
                        'currency': base_currency_code
                    }), 400

                # Check 2: Account shouldn't be used in any open transactions
                if has_open_transactions(db_session, account_id, current_user.app_id):
                    return jsonify({
                        'success': False,
                        'error': 'Cannot deactivate account that is referenced in open transactions.'
                    }), 400

            # Update account status
            account.is_active = is_active
            db_session.commit()

            return jsonify({
                'success': True,
                'message': 'Account status updated successfully',
                'new_status': 'active' if is_active else 'inactive'
            })

    except Exception as e:
        logger.error(f'Error updating account status: {str(e)}')
        return jsonify({'success': False, 'error': 'Internal server error'}), 500


@general_ledger_bp.route('/new_category')
@login_required
def new_category():
    app_id = current_user.app_id

    db_session = Session()
    company = db_session.query(Company).filter_by(id=app_id).first()
    role = current_user.role
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

    try:
        parent_categories = db_session.query(Category).filter_by(app_id=app_id).all()

        return render_template('new_category.html', parent_categories=parent_categories, company=company,
                               role=role, modules=modules_data, module_name="General Ledger")
    except Exception as e:
        flash(f'Error fetching parent categories: {str(e)}', 'error')
        logger.error(f'Operational error has been caught: {e}')
        db_session.rollback()  # Rollback if necessary
        db_session.close()
        return redirect(url_for('new_category'))  # Ensure redirect is returned
    except OperationalError as e:
        logger.error(f'Operational error has been caught: {e}')
        db_session.rollback()  # Rollback if necessary
        db_session.close()
        return redirect(url_for("new_category"))  # Redirect properly
    finally:
        db_session.close()  # Ensure session is always closed


@general_ledger_bp.route('/submit_category', methods=['POST'])
@login_required
def submit_category():
    if current_user.is_authenticated:
        user_session = Session()

        account_type = request.form["account_type"]
        category = request.form["category"].strip()
        category_id = request.form['category_id'].strip()

        app_id = current_user.app_id

        try:
            # Check if the category_id already exists for the current app_id
            existing_category = user_session.query(Category).filter_by(app_id=app_id,
                                                                       category_id=category_id).first()
            if existing_category:
                flash(f'Duplicate Category ID "{category_id}" found for your company.', 'danger')
                return redirect(url_for('new_category'))

            # Create a new category entry if no duplicates found
            new_category_data = Category(
                category=category,
                category_id=category_id,
                account_type=account_type,
                app_id=app_id
            )
            user_session.add(new_category_data)

            # Store login activity
            new_log = ActivityLog(
                activity="chart of accounts",
                user=current_user.email,  # Assuming 'user' in ActivityLog is a string for the user's email
                details=f"New Category {category}:{category_id} added successfully",
                app_id=current_user.app_id
            )
            user_session.add(new_log)

            user_session.commit()
            flash(f"New Category {category}:{category_id} added successfully", "success")
            return redirect(url_for('general_ledger.new_category'))
        except IntegrityError as e:
            logger.error(f'An error occured {e}')
            user_session.rollback()
            flash('Error: Duplicate category ID.', 'danger')
        except Exception as e:
            logger.error(f'An error of Exception occured {e}')
            user_session.rollback()
            flash(f'An error occurred: {str(e)}', 'danger')
        finally:
            user_session.close()

    flash('You need to be logged in to perform this action.', 'warning')
    return redirect(url_for('general_ledger.add_chart_of_accounts'))


@general_ledger_bp.route('/delete_category/<int:id>', methods=['POST'])
@login_required
def delete_category(id):
    user_session = Session()  # Create a new session

    try:
        # Fetch the category to delete
        category = user_session.query(Category).filter_by(id=id).first()

        if not category:
            flash('Error: Category not found.', 'danger')
            return redirect(url_for('new_category'))

        # Check if the user is authorized to delete the category
        if category.app_id != current_user.app_id:
            flash('Unauthorized: You do not have permission to delete this category.', 'error')
            return redirect(url_for('new_category'))

        # Check if the category is in use

        accounts = user_session.query(ChartOfAccounts).filter_by(category_id=category.category_id).first()

        if accounts:
            flash('Error: Category is in use and cannot be deleted.', 'error')
            return redirect(url_for('new_category'))

        # Delete the category
        user_session.delete(category)

        # Log the activity
        new_log = ActivityLog(
            activity="chart of accounts",
            user=current_user.email,  # Assuming 'user' in ActivityLog is a string for the user's email
            details=f"{category.category} deleted successfully",
            app_id=current_user.app_id
        )
        user_session.add(new_log)

        # Commit the transaction
        user_session.commit()
        flash('Category deleted successfully!', 'success')

    except Exception as e:
        # Rollback in case of error
        user_session.rollback()
        flash(f'Error: Unable to delete category. {str(e)}', 'danger')

    finally:
        # Ensure the session is always closed
        user_session.close()

    return redirect(url_for('general_ledger.new_category'))


@general_ledger_bp.route('/edit_category/<int:id>', methods=['POST'])
@login_required
def edit_category(id):
    user_session = Session()
    category = user_session.query(Category).filter_by(id=id).first()

    if not category:
        flash('Error: Category not found.', 'danger')
        return redirect(url_for('new_category'))

    if category.app_id != current_user.app_id:
        flash('Unauthorized: You do not have permission to edit this category.', 'danger')
    else:
        edited_account_type = request.form["account_type"]
        edited_category = request.form["category"].strip()
        edited_category_id = request.form['category_id'].strip()

        try:
            # Check if the edited category_id already exists for the current app_id
            existing_category = user_session.query(Category).filter(
                Category.id != id,
                Category.app_id == current_user.app_id,
                Category.category_id == edited_category_id
            ).first()

            if existing_category:
                flash(f'Duplicate Category ID "{edited_category_id}" found for your company.', 'danger')
            else:

                user_session.query(ChartOfAccounts).filter_by(category_id=category.category_id).update(
                    {'parent_account_type': edited_account_type, 'category': edited_category,
                     'category_id': edited_category_id})

                # Update the category
                category.account_type = edited_account_type
                category.category = edited_category
                category.category_id = edited_category_id

                # Store login activity
                new_log = ActivityLog(
                    activity="chart of accounts",
                    user=current_user.email,  # Assuming 'user' in ActivityLog is a string for the user's email
                    details=f"{category.category} changed to {edited_category} with code {edited_category_id} successfully",
                    app_id=current_user.app_id
                )
                user_session.add(new_log)

                user_session.commit()
                flash('Category updated successfully!', 'success')

        except IntegrityError as e:
            user_session.rollback()
            logger.error(f'Error occurred: {str(e)}\n{traceback.format_exc()}')
            flash('Error: Duplicate category ID.', 'danger')
        except Exception as e:
            user_session.rollback()
            logger.error(f'Error occurred: {str(e)}\n{traceback.format_exc()}')
            flash(f'An error occurred: {str(e)}', 'danger')
        finally:
            user_session.close()

    return redirect(url_for('general_ledger.new_category'))


@general_ledger_bp.route('/api/check-account-transactions/<int:account_id>', methods=['POST'])
@login_required
@role_required(['Admin', 'Supervisor'])
def check_account_transactions(account_id):
    """Check if account has transactions without vendor/customer before enabling receivable/payable"""
    db_session = Session()
    try:
        app_id = current_user.app_id

        data = request.get_json()
        check_receivable = data.get('check_receivable', False)
        check_payable = data.get('check_payable', False)

        if not (check_receivable or check_payable):
            return jsonify({'can_proceed': True})

        # Query journal entries for this account without vendor_id
        # Fix: Access as tuple elements
        query = db_session.query(
            JournalEntry,
            Journal.journal_number,
            Journal.date,
            JournalEntry.description
        ).join(
            Journal, Journal.id == JournalEntry.journal_id
        ).filter(
            JournalEntry.subcategory_id == account_id,
            Journal.app_id == app_id,
            Journal.status == 'Posted',
            Journal.vendor_id.is_(None)
        )

        results = query.all()

        if results:
            transactions_list = []
            for result in results:
                # Result is a tuple: (JournalEntry, journal_number, date, description)
                journal_entry = result[0]
                journal_number = result[1]
                date = result[2]
                description = result[3] or journal_entry.description or 'No description'

                transactions_list.append({
                    'journal_number': journal_number,
                    'date': date.strftime('%Y-%m-%d'),
                    'description': description
                })

            return jsonify({
                'can_proceed': False,  # BLOCK the edit
                'count': len(results),
                'transactions': transactions_list
            })

        return jsonify({'can_proceed': True})

    except Exception as e:
        logger.error(f'Error checking account transactions: {e}\n{traceback.format_exc()}')
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()
