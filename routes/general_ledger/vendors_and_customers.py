#app/routes/general_ledger/vendors_and_customers.py

import logging
import math
import traceback
from datetime import datetime
from decimal import Decimal

from flask import flash, redirect, request, render_template, jsonify, url_for
from flask_login import login_required, current_user
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError, NoResultFound
from sqlalchemy.orm import joinedload

from ai import get_base_currency, get_exchange_rate
from db import Session
from decorators import role_required
from models import ChartOfAccounts, Category, Company, Module, OpeningBalance, ActivityLog, JournalEntry, Journal, \
    Vendor, CustomerCredit
from services.chart_of_accounts_helpers import calculate_account_balance, has_open_transactions
from services.post_to_ledger import post_opening_balances_to_ledger
from services.vendors_and_customers import normalize_vendor_type
from . import general_ledger_bp

logger = logging.getLogger()


@general_ledger_bp.route('/api/vendors', methods=['GET'])
@login_required
def get_vendors():
    """Get all vendors and customers for the current company"""
    db_session = Session()
    try:
        company_id = current_user.app_id

        # Get type filter from query parameters
        type_filter = request.args.get('type', 'all')  # all, vendor, customer, supplier

        query = db_session.query(Vendor).filter_by(app_id=company_id, is_active=True)

        # Apply type filter if specified
        if type_filter != 'all':
            # Handle different type mappings - vendor can include both vendor and supplier types
            if type_filter.lower() == 'vendor':
                # Filter for vendor or supplier types using separate filters
                query = query.filter(
                    (Vendor.vendor_type.ilike('vendor')) |
                    (Vendor.vendor_type.ilike('supplier'))
                )
            else:
                query = query.filter(Vendor.vendor_type.ilike(type_filter))

        vendors = query.order_by(Vendor.vendor_name).all()

        vendors_list = []
        for vendor in vendors:
            vendor_type = normalize_vendor_type(vendor.vendor_type)

            vendors_list.append({
                'id': vendor.id,
                'name': vendor.vendor_name,
                'code': vendor.vendor_id or '',
                'type': vendor_type,
                'contact': vendor.tel_contact or '',
                'email': vendor.email or '',
                'status': vendor.vendor_status,
                'address': f"{vendor.address or ''} {vendor.city or ''} {vendor.state_province or ''}".strip(),
                'payment_terms': vendor.payment_terms or '',
                'tax_id': vendor.tax_id or ''
            })

        return jsonify({
            'success': True,
            'vendors': vendors_list,
            'count': len(vendors_list)
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to fetch vendors: {str(e)}'
        }), 500
    finally:
        db_session.close()


@general_ledger_bp.route('/api/vendors/<int:vendor_id>', methods=['GET'])
@login_required
def get_vendor(vendor_id):
    """Get specific vendor details"""
    db_session = Session()
    try:
        company_id = current_user.app_id

        vendor = db_session.query(Vendor).filter_by(
            id=vendor_id,
            app_id=company_id,
            is_active=True
        ).first()

        if not vendor:
            return jsonify({
                'success': False,
                'error': 'Vendor not found'
            }), 404

        # Normalize vendor type to only 2 options: 'vendor' or 'customer'
        vendor_type = normalize_vendor_type(vendor.vendor_type)

        vendor_data = {
            'id': vendor.id,
            'name': vendor.vendor_name,
            'code': vendor.vendor_id or '',
            'type': vendor_type,
            'contact': vendor.tel_contact or '',
            'email': vendor.email or '',
            'status': vendor.vendor_status,
            'address': vendor.address or '',
            'city': vendor.city or '',
            'state': vendor.state_province or '',
            'postal_code': vendor.postal_code or '',
            'country': vendor.country or '',
            'website': vendor.website or '',
            'payment_terms': vendor.payment_terms or '',
            'tax_id': vendor.tax_id or '',
            'primary_contact': vendor.primary_contact_person or '',
            'secondary_contact': vendor.secondary_contact_person or ''
        }

        return jsonify({
            'success': True,
            'vendor': vendor_data
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to fetch vendor: {str(e)}'
        }), 500
    finally:
        db_session.close()


@general_ledger_bp.route('/add_vendor', methods=['POST'])
@login_required
def add_vendor():
    if request.method == 'POST':
        user_session = Session()

        # Get data from the form
        vendor_name = request.form.get('vendorName').strip() or None
        vendor_id = request.form['vendorId'].strip() or None
        designation = request.form.get('designation').strip() or None
        tel_contact = request.form.get('telContact').strip() or None
        email = request.form.get('email', '').strip() or None
        address = request.form.get('address').strip() or None
        city = request.form.get('city').strip() or None
        state_province = request.form.get('stateProvince').strip() or None
        postal_code = request.form.get('postalCode').strip() or None
        country = request.form.get('country') or None
        website = request.form.get('website').strip() or None
        payment_terms = request.form.get('paymentTerms').strip() or None
        tax_id = request.form.get('taxId').strip() or None
        vendor_type = request.form.get('vendorType').strip() or None
        primary_contact_person = request.form.get('primaryContactPerson').strip() or None
        secondary_contact_person = request.form.get('secondaryContactPerson').strip() or None
        preferred_communication_method = request.form.get(
            'preferredCommunicationMethod').strip() or None
        certifications = request.form.get('certifications').strip() or None
        notes = request.form.get('notes').strip() or None
        is_one_time = request.form.get('is_one_time', False)

        try:
            # Create a new Vendor object and add it to the database
            new_vendor = Vendor(
                vendor_name=vendor_name,
                vendor_id=vendor_id,
                designation=designation,
                tel_contact=tel_contact,
                email=email,
                address=address,
                city=city,
                state_province=state_province,
                postal_code=postal_code,
                country=country,
                website=website,
                payment_terms=payment_terms,
                tax_id=tax_id,
                vendor_type=vendor_type,
                primary_contact_person=primary_contact_person,
                secondary_contact_person=secondary_contact_person,
                preferred_communication_method=preferred_communication_method,
                certifications=certifications,
                notes=notes,
                is_one_time=is_one_time,
                is_active=True,
                app_id=current_user.app_id
            )
            user_session.add(new_vendor)

            # Store login activity
            new_log = ActivityLog(
                activity="vendor management",
                user=current_user.email,
                details=f"{vendor_name}: {vendor_id} has been added successfully",
                app_id=current_user.app_id
            )
            user_session.add(new_log)
            user_session.commit()

            # Return a JSON response
            return jsonify({
                "success": True,
                "vendorId": new_vendor.id,  # Include the vendor ID in the response
                "vendorName": new_vendor.vendor_name,  # Include the vendor ID in the response
                "message": "Vendor added successfully."
            })

        except IntegrityError as e:
            user_session.rollback()
            return jsonify({"success": False, "message": f"An error occurred: {str(e)}"}), 400

        finally:
            user_session.close()

    return jsonify({"success": False, "message": "Invalid request method."}), 405


@general_ledger_bp.route('/edit_vendor/<int:vendor_id>', methods=['GET', 'POST'])
@login_required
def edit_vendor(vendor_id):
    db_session = Session()
    app_id = current_user.app_id
    vendor = db_session.query(Vendor).filter_by(id=vendor_id, app_id=current_user.app_id).first()
    company = db_session.query(Company.name).filter_by(id=app_id).first()
    modules_data = [mod.module_name for mod in
                    db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
    role = current_user.role

    if vendor is None:
        flash('Vendor not found or you do not have permission to edit this vendor.', 'danger')
        db_session.close()
        return redirect(url_for('general_ledger.add_vendor_form'))

    if request.method == 'POST':
        old_name = vendor.vendor_name.strip()
        old_id = vendor.vendor_id
        new_name = request.form['vendorName'].strip()
        new_id = request.form['vendorId'].strip()

        try:
            # Check if the new vendor_id already exists for the current app_id

            new_id = new_id.strip() if new_id else None

            if new_id:
                existing_vendor = db_session.query(Vendor).filter(
                    Vendor.id != vendor_id,
                    Vendor.app_id == current_user.app_id,
                    Vendor.vendor_id == new_id
                ).first()

                if existing_vendor:
                    flash('Vendor ID already exists within your company. Please choose another.', 'danger')
                    db_session.close()
                    return redirect(url_for('general_ledger.edit_vendor', vendor_id=vendor_id))

            # Update vendor details
            vendor.vendor_name = new_name
            vendor.vendor_id = new_id
            vendor.designation = request.form.get('designation').strip() or None
            vendor.tel_contact = request.form.get('telContact').strip() or None
            vendor.email = request.form.get('email').strip() or None
            vendor.address = request.form.get('address').strip() or None
            vendor.city = request.form.get('city').strip() or None
            vendor.state_province = request.form.get('stateProvince').strip() or None
            vendor.postal_code = request.form.get('postalCode').strip() or None
            vendor.country = request.form.get('country') or None
            vendor.website = request.form.get('website').strip() or None
            vendor.payment_terms = request.form.get('paymentTerms').strip() or None
            vendor.tax_id = request.form.get('taxId').strip() or None
            vendor.vendor_type = request.form.get('vendorType').strip() or None
            vendor.primary_contact_person = request.form.get('primaryContactPerson').strip() or None
            vendor.secondary_contact_person = request.form.get('secondaryContactPerson').strip() or None
            vendor.preferred_communication_method = request.form.get('preferredCommunicationMethod') or None
            vendor.certifications = request.form.get('certifications').strip() or None
            vendor.notes = request.form.get('notes').strip() or None
            vendor.is_active = True
            vendor.is_one_time = request.form.get('is_one_time', False)

            # Store login activity
            new_log = ActivityLog(
                activity="vendor management",
                user=current_user.email,  # Assuming 'user' in ActivityLog is a string for the user's email
                details=f"{old_name}:{old_id} has been changed to {new_name}: {new_id} successfully",
                app_id=current_user.app_id
            )
            db_session.add(new_log)

            db_session.commit()

            flash('Vendor updated successfully!', 'success')
            db_session.close()
            return redirect(url_for('general_ledger.add_vendor_form', edited=True))

        except IntegrityError as e:
            db_session.rollback()
            logger.error(f'Error Occured adding vendor: {e}\n{traceback.format_exc()}')
            flash('An error occurred while updating the vendor. Please try again.', 'danger')
            db_session.close()
            return render_template('edit_vendor.html', vendor=vendor, company=company, modules=modules_data,
                                   role=role)

    db_session.close()
    return render_template('edit_vendor.html', vendor=vendor, company=company, modules=modules_data,
                           role=role)


@general_ledger_bp.route('/add_vendor_form')
@login_required
def add_vendor_form():
    db_session = Session()
    try:
        app_id = current_user.app_id  # Get the company ID of the logged-in user
        company = db_session.query(Company).filter_by(id=app_id).first()
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id).filter_by(included='yes').all()]
        role = current_user.role
        vendors_data = db_session.query(Vendor).filter_by(app_id=app_id).all()
        vendors = [item for item in vendors_data]
    except OperationalError:
        db_session.rollback()
        return redirect(url_for('add_vendor_form'))
    except Exception as e:
        db_session.rollback()
        flash(f'An error occurred: {str(e)}', 'danger')
        return redirect(url_for('add_vendor_form'))
    finally:
        db_session.close()
    return render_template('add_vendor.html', vendors=vendors, company=company, role=role,
                           modules=modules_data, title="Payee and Payer Management", module_name="General Ledger")


@general_ledger_bp.route('/delete_vendor/<int:vendor_id>', methods=['GET', 'POST'])
@login_required
def delete_vendor(vendor_id):
    db_session = Session()
    try:
        vendor = db_session.query(Vendor).filter_by(id=vendor_id, app_id=current_user.app_id).first()
        if vendor:
            # Check if there are transactions associated with the vendor
            transactions = db_session.query(Journal).filter_by(vendor_id=vendor_id,
                                                               app_id=current_user.app_id).first()
            if transactions:
                flash('Cannot delete vendor with associated transactions.', 'error')
            else:
                db_session.delete(vendor)

                # Store login activity
                new_log = ActivityLog(
                    activity="vendor management",
                    user=current_user.email,  # Assuming 'user' in ActivityLog is a string for the user's email
                    details=f"{vendor.vendor_name} has been deleted successfully",
                    app_id=current_user.app_id
                )
                db_session.add(new_log)

                db_session.commit()
                flash('Vendor deleted successfully!', 'success')
        else:
            flash('Vendor not found or you do not have permission to delete this vendor.', 'error')
    except NoResultFound:
        flash('Vendor not found.', 'error')
    finally:
        db_session.close()

    return redirect(url_for('add_vendor_form', deleted=True))
