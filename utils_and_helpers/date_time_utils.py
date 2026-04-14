# app/utils/date_time_utils.py

import logging
import datetime

logger = logging.getLogger(__name__)


def parse_date(date_field):
    if isinstance(date_field, datetime.date):  # If it's already a date object, return it
        return date_field
    if date_field:  # If it's not empty
        return datetime.datetime.strptime(date_field, '%Y-%m-%d')
    return None  # If no date is provided, return None


def get_filter_date(time_filter):
    """Helper to get filter date range based on time filter string"""
    today = datetime.date.today()

    if time_filter == 'today':
        return today, today
    elif time_filter == 'yesterday':
        yesterday = today - datetime.timedelta(days=1)
        return yesterday, yesterday
    elif time_filter == 'week':
        start = today - datetime.timedelta(days=today.weekday())
        return start, today
    elif time_filter == 'month':
        start = today.replace(day=1)
        return start, today
    elif time_filter == 'last_month':
        first_day_current = today.replace(day=1)
        last_day_previous = first_day_current - datetime.timedelta(days=1)
        start = last_day_previous.replace(day=1)
        return start, last_day_previous
    elif time_filter == 'quarter':
        quarter_month = ((today.month - 1) // 3) * 3 + 1
        start = today.replace(month=quarter_month, day=1)
        return start, today
    elif time_filter == 'year':
        start = today.replace(month=1, day=1)
        return start, today
    return None, None


def get_date_range_from_filter(time_filter, custom_start=None, custom_end=None):
    """Get date range from time filter"""
    today = datetime.date.today()

    if time_filter == 'today':
        return today, today
    elif time_filter == 'yesterday':
        yesterday = today - datetime.timedelta(days=1)
        return yesterday, yesterday
    elif time_filter == 'week':
        start = today - datetime.timedelta(days=today.weekday())
        return start, today
    elif time_filter == 'month':
        start = today.replace(day=1)
        return start, today
    elif time_filter == 'last_month':
        first_day_current = today.replace(day=1)
        last_day_previous = first_day_current - datetime.timedelta(days=1)
        start = last_day_previous.replace(day=1)
        return start, last_day_previous
    elif time_filter == 'quarter':
        quarter_month = ((today.month - 1) // 3) * 3 + 1
        start = today.replace(month=quarter_month, day=1)
        return start, today
    elif time_filter == 'year':
        start = today.replace(month=1, day=1)
        return start, today
    elif time_filter == 'custom' and custom_start and custom_end:
        # Handle custom dates
        try:
            if isinstance(custom_start, str):
                start = datetime.datetime.strptime(custom_start, '%Y-%m-%d').date()
            else:
                start = custom_start
            if isinstance(custom_end, str):
                end = datetime.datetime.strptime(custom_end, '%Y-%m-%d').date()
            else:
                end = custom_end
            return start, end
        except (ValueError, TypeError):
            # Fallback to default range if custom dates are invalid
            start = today - datetime.timedelta(days=30)
            return start, today
    else:
        # Default to last 30 days
        start = today - datetime.timedelta(days=30)
        return start, today
