import logging
import traceback

from flask import jsonify, request
from datetime import datetime

from flask_login import login_required, current_user

from db import Session
from models import Currency
from report_routes import get_latest_exchange_rates_batch
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj
from . import multi_currency_bp

logger = logging.getLogger()

@multi_currency_bp.route('/api/get_exchange_rate', methods=['POST'])
@login_required
def api_get_exchange_rate():
    """
    API endpoint to get exchange rate for a specific date and currency pair.
    Uses centralized get_exchange_rate_and_obj function.
    """
    db_session = Session()
    try:
        data = request.get_json()
        from_currency_id = data.get('from_currency_id')
        to_currency_id = data.get('to_currency_id')
        date_str = data.get('date')
        app_id = current_user.app_id

        if not all([from_currency_id, to_currency_id, date_str]):
            return jsonify({'success': False, 'error': 'Missing required fields'}), 400

        # Parse date
        rate_date = datetime.strptime(date_str, '%Y-%m-%d').date()

        # Same currency check
        if int(from_currency_id) == int(to_currency_id):
            return jsonify({
                'success': True,
                'rate': 1.0,
                'rate_id': None,
                'source': 'same_currency'
            })

        try:
            # Use centralized function
            rate_obj, calculated_rate = get_exchange_rate_and_obj(
                db_session=db_session,
                from_currency_id=int(from_currency_id),
                to_currency_id=int(to_currency_id),
                app_id=app_id,
                as_of_date=rate_date
            )

            # Determine source type for response
            if rate_obj.date == rate_date:
                source = 'exact_match'
            elif rate_obj.date < rate_date:
                source = f'nearest_rate_from_{rate_obj.date.strftime("%Y-%m-%d")}'
            else:
                source = 'inverse_calculation'

            return jsonify({
                'success': True,
                'rate': float(calculated_rate),
                'rate_id': rate_obj.id,
                'rate_date': rate_obj.date.strftime('%Y-%m-%d'),
                'source': source
            })

        except ValueError as e:
            # No rate found in database and external fetch failed or date too old
            return jsonify({
                'success': False,
                'error': str(e),
                'details': f'No exchange rate available for {rate_date}'
            }), 404

    except Exception as e:
        logger.error(f"Error in api_get_exchange_rate: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        db_session.close()

@multi_currency_bp.route('/api/exchange-rates', methods=['GET'])
@login_required
def get_exchange_rates():
    """
    API endpoint to get exchange rates for multiple currencies for a specific date.
    Uses centralized get_exchange_rate_and_obj function for each currency pair.
    """
    db_session = Session()
    try:
        # Get parameters from frontend
        as_of_date = request.args.get('date')
        base_currency = request.args.get('base_currency')
        base_currency_id = request.args.get('base_currency_id')

        # Validate required parameters
        if not base_currency or not base_currency_id:
            return jsonify({
                'success': False,
                'error': 'base_currency and base_currency_id are required'
            }), 400

        app_id = current_user.app_id

        # Parse date
        if not as_of_date:
            as_of_date = datetime.today().date()
        else:
            as_of_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()

        # Get all foreign currency IDs (excluding base currency)
        foreign_currencies = db_session.query(Currency).filter(
            Currency.app_id == app_id,
            Currency.id != int(base_currency_id)
        ).all()

        # Get rates for each foreign currency using centralized function
        formatted_rates = {}
        rate_details = {}  # For additional metadata if needed

        for currency in foreign_currencies:
            try:
                # Try direct rate (foreign → base)
                rate_obj, rate = get_exchange_rate_and_obj(
                    db_session=db_session,
                    from_currency_id=currency.id,
                    to_currency_id=int(base_currency_id),
                    app_id=app_id,
                    as_of_date=as_of_date
                )
                formatted_rates[currency.user_currency] = float(rate)
                rate_details[currency.user_currency] = {
                    'rate_id': rate_obj.id,
                    'rate_date': rate_obj.date.strftime('%Y-%m-%d'),
                    'direction': 'direct'
                }

            except ValueError:
                # Try inverse rate (base → foreign)
                try:
                    rate_obj, rate = get_exchange_rate_and_obj(
                        db_session=db_session,
                        from_currency_id=int(base_currency_id),
                        to_currency_id=currency.id,
                        app_id=app_id,
                        as_of_date=as_of_date
                    )
                    # Invert the rate for display
                    inverted_rate = 1.0 / float(rate)
                    formatted_rates[currency.user_currency] = inverted_rate
                    rate_details[currency.user_currency] = {
                        'rate_id': rate_obj.id,
                        'rate_date': rate_obj.date.strftime('%Y-%m-%d'),
                        'direction': 'inverse'
                    }

                except ValueError:
                    # No rate found in either direction
                    logger.warning(f"No exchange rate found for {currency.user_currency} on {as_of_date}")
                    continue

        return jsonify({
            'success': True,
            'date': as_of_date.strftime('%Y-%m-%d'),
            'base_currency': base_currency,
            'rates': formatted_rates,
            'rate_details': rate_details  # Optional: useful for debugging
        })

    except Exception as e:
        logger.error(f"Error in get_exchange_rates: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        db_session.close()
