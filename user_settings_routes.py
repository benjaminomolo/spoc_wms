import os

from flask import Blueprint, flash, redirect, url_for
from flask_login import current_user, login_required


from db import Session
from models import Company

company_settings_routes = Blueprint('company_settings_routes', __name__)


@company_settings_routes.route('/settings/delete_footer_template', methods=['POST'])
@login_required
def delete_footer_template():
    from app import allowed_file, UPLOAD_FOLDER_FOOTER, file_exists
    db_session = Session()
    try:
        # Refresh the session to ensure we have latest data
        db_session.expire_all()

        company = db_session.query(Company).filter_by(id=current_user.app_id).first()
        if not company:
            flash('Company not found', 'danger')
            return redirect(url_for('settings'))

        # Debug print before changes

        if company.footer:
            try:
                file_path = os.path.join(UPLOAD_FOLDER_FOOTER, company.footer)

                if file_exists(file_path):
                    os.remove(file_path)

                # Explicitly update the fields
                company.footer = None
                company.footer_type = None

                # Force immediate commit
                db_session.commit()

                flash('Footer template removed successfully', 'success')
            except Exception as e:
                db_session.rollback()
                print(f"Error occurred: {str(e)}")
                flash(f'Error removing template file: {str(e)}', 'danger')
        else:
            # Still clear the fields if they exist
            if company.footer is not None or company.footer_type is not None:
                company.footer = None
                company.footer_type = None
                db_session.commit()
            flash('Footer template reference cleared', 'info')

        # Verify changes after operation
        db_session.refresh(company)
        print(f"Final state - Footer: {company.footer}, Footer Type: {company.footer_type}")

        return redirect(url_for('settings'))
    except Exception as e:
        db_session.rollback()
        print(f"Unexpected error: {str(e)}")
        flash('An unexpected error occurred', 'danger')
        return redirect(url_for('settings'))
    finally:
        db_session.close()
