import logging
from datetime import datetime

from flask_login import current_user
from sqlalchemy import func
from db import Session

logger = logging.getLogger(__name__)
def safe_int_conversion(id_input):
    """
    Safely convert various input types to a list of integers.
    Handles: lists, strings, integers, mixed types, and invalid values.
    """
    result = []

    if not id_input:
        return result

    # Handle single integer
    if isinstance(id_input, int):
        return [id_input]

    # Handle single string
    if isinstance(id_input, str):
        # Check if it's a comma-separated string
        if ',' in id_input:
            id_list = id_input.split(',')
        else:
            id_list = [id_input]
    # Handle lists, tuples, sets
    elif isinstance(id_input, (list, tuple, set)):
        id_list = list(id_input)
    else:
        # Try to convert other iterables
        try:
            id_list = list(id_input)
        except (TypeError, ValueError):
            id_list = [id_input]

    # Convert each item to integer
    for item in id_list:
        try:
            # Handle None, empty strings, etc.
            if item is None or item == '':
                continue
            result.append(int(item))
        except (ValueError, TypeError):
            # Skip invalid values but log them
            logger.warning(f"Invalid ID value skipped: {item}")
            continue

    return result


def generate_sequence_number(prefix="SALE", table_model=None, number_field=None, db_session=None, app_id=None):
    """
    Generic function to generate sequence numbers.

    Args:
        prefix: The prefix for the number (e.g., "SALE", "PUR")
        table_model: The SQLAlchemy model class
        number_field: The field name containing the sequence number
        db_session: Database session (creates new if None)
        app_id: Application ID (uses current_user if None)

    Returns:
        New unique sequence number
    """
    if app_id is None:
        app_id = current_user.app_id

    month_year = datetime.now().strftime("%m%y")

    # Handle session
    if db_session is None:
        with Session() as temp_session:
            return _generate_sequence(temp_session, app_id, month_year, prefix, table_model, number_field)
    else:
        return _generate_sequence(db_session, app_id, month_year, prefix, table_model, number_field)


def _generate_sequence(db_session, app_id, month_year, prefix, table_model, number_field):
    """Core generation logic"""
    # Query with lock
    last_record = db_session.query(table_model).filter(
        getattr(table_model, 'app_id') == app_id,
        getattr(table_model, number_field).isnot(None),
        getattr(table_model, number_field).like(f'{prefix}-{month_year}-%')
    ).order_by(
        getattr(table_model, number_field).desc()
    ).with_for_update().first()

    if last_record:
        last_number = getattr(last_record, number_field)
        parts = last_number.split('-')

        if len(parts) == 3:
            last_sequence = int(parts[2])
            next_number = last_sequence + 1
        else:
            # Malformed number
            next_number = 1
    else:
        # Check if there are any records at all (different month)
        any_record = db_session.query(table_model).filter_by(
            app_id=app_id
        ).order_by(
            getattr(table_model, number_field).desc()
        ).first()

        if any_record:
            # Records exist but different month
            next_number = 1
        else:
            # First record ever
            next_number = 1

    return f"{prefix}-{month_year}-{str(next_number).zfill(5)}"


def _get_int_or_none(value, default=None):
    """Helper function to safely convert to int"""
    if value and str(value).isdigit():
        return int(value)
    return default



