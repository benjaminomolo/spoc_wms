from flask import flash, redirect, url_for
from sqlalchemy import select, and_


def validate_required_fields(fields: dict, redirect_endpoint: str, **redirect_kwargs):
    """
    Validates required fields.

    Args:
        fields (dict): {"Field Name": value}
        redirect_endpoint (str): Flask endpoint to redirect to if validation fails
        **redirect_kwargs: kwargs for url_for()

    Returns:
        None if valid, otherwise a redirect response.
    """

    missing_fields = [
        field_name for field_name, value in fields.items()
        if value is None or value == '' or (hasattr(value, '__len__') and len(value) == 0)
    ]

    if missing_fields:
        flash(
            f"Missing required fields: {', '.join(missing_fields)}.",
            "error"
        )
        return redirect(url_for(redirect_endpoint, **redirect_kwargs))

    return None


def get_int_or_none(value):
    """
    Convert a form value to integer or return None if value is None/empty/not a number.

    Args:
        value: The form value to convert (can be string, int, None)

    Returns:
        int or None: Integer value if valid, otherwise None
    """
    if value is None:
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, str):
        # Remove whitespace and check if empty
        value = value.strip()
        if not value:
            return None

        # Try to convert to integer
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    # If it's another numeric type, try to convert to int
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


from contextlib import contextmanager


@contextmanager
def optimistic_lock(db_session, model, record_id, version):
    """
    Context manager for optimistic locking.

    Usage:
        with optimistic_lock(db_session, BulkPayment, receipt_id, form_version) as receipt:
            if not receipt:
                flash('Record was modified by another user', 'error')
                return redirect(...)
            # Edit receipt here
    """
    stmt = select(model).where(
        and_(
            model.id == record_id,
            model.version == version
        )
    ).with_for_update()

    result = db_session.execute(stmt)
    receipt = result.scalar_one_or_none()

    try:
        yield receipt
    except Exception:
        # No automatic rollback - caller should handle
        raise


def get_locked_record(db_session, model, record_id, version):
    """
    Returns locked record or None. No context manager needed.
    """
    stmt = select(model).where(
        and_(
            model.id == record_id,
            model.version == version
        )
    ).with_for_update()

    result = db_session.execute(stmt)
    return result.scalar_one_or_none()
