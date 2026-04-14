import logging

from flask import Blueprint, render_template, redirect, request, flash, url_for
from flask_login import login_required, current_user
from sqlalchemy import or_

from db import Session
from models import Category, Module, Company, ChartOfAccounts

multi_currency_routes = Blueprint('multi_currency_routes', __name__)
# Set up logging
logger = logging.getLogger(__name__)


@multi_currency_routes.route('/multi-currency/fx_account_settings', methods=['GET', 'POST'])
@login_required
def configure_fx_accounts():
    with Session() as db_session:
        app_id = current_user.app_id

        if request.method == 'POST':
            # Handle form submission
            account_id = request.form.get('account_id')
            action = request.form.get('action')  # 'add' or 'remove'

            if not account_id or not action:
                flash('Missing required parameters', 'danger')
                return redirect(url_for('multi_currency_routes.configure_fx_accounts'))

            account = db_session.query(ChartOfAccounts).filter_by(
                id=account_id,
                app_id=app_id
            ).first()

            if not account:
                flash('Account not found', 'danger')
                return redirect(url_for('multi_currency_routes.configure_fx_accounts'))

            # Update monetary status
            account.is_monetary = (action == 'add')
            db_session.commit()

            flash(f"Account {'marked as' if action == 'add' else 'removed from'} monetary", 'success')
            return redirect(url_for('multi_currency_routes.configure_fx_accounts'))

        # GET request - show the form
        monetary_accounts = db_session.query(ChartOfAccounts).filter_by(
            app_id=app_id,
            is_monetary=True
        ).order_by(ChartOfAccounts.category, ChartOfAccounts.sub_category).all()

        non_monetary_accounts = db_session.query(ChartOfAccounts).filter(
            ChartOfAccounts.app_id == app_id,
            or_(
                ChartOfAccounts.is_monetary == False,
                ChartOfAccounts.is_monetary == None
            )
        ).order_by(ChartOfAccounts.category, ChartOfAccounts.sub_category).all()

        return render_template(
            'multi-currency/fx_account_settings.html',
            monetary_accounts=monetary_accounts,
            non_monetary_accounts=non_monetary_accounts,
            company=db_session.query(Company).filter_by(id=app_id).first(),
            role=current_user.role,
            modules=[m.module_name for m in db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]
        )
