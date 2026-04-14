from decimal import Decimal, ROUND_HALF_UP


def format_amount(amount):
    if amount is None:
        return "0.00"  # or return "-" or "" if you prefer
    try:
        return f"{Decimal(amount):,.2f}"
    except Exception:
        return "0.00"


def format_currency(amount, symbol='$', decimals=2):
    """
    Format a number as a currency string.

    Args:
        amount (Decimal, float, int): The numeric amount to format.
        symbol (str): The currency symbol to prepend. Default is '$'.
        decimals (int): Number of decimal places. Default is 2.

    Returns:
        str: Formatted currency string, e.g., "$1,234.56"
    """
    try:
        amount = float(amount)
        return f"{symbol}{amount:,.{decimals}f}"
    except (ValueError, TypeError):
        return f"{symbol}0.00"


def _to_decimal(value):
    return Decimal(str(value or 0))


def _float_for_journal(d: Decimal) -> float:
    # convert Decimal to float safely for your create_transaction call
    return float(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))



# In utils.py - keep it simple and fast
def parse_number(value, default=0, type_cast=float):
    """Ultra-fast number parser for 95% of cases"""
    if not value:
        return default
    try:
        # Quick cleanup for common formats
        if isinstance(value, str):
            value = value.strip().replace(',', '')
        return type_cast(value)
    except (ValueError, TypeError):
        return default

# Use the comprehensive function only for complex cases
def parse_complex_number(value, default=0):
    """Use this when you need currency symbols, parentheses, etc."""
    # Only import re when needed
    import re
    if not value:
        return default
    try:
        cleaned = re.sub(r'[$€£¥\s]', '', str(value))
        cleaned = cleaned.replace(',', '')
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]
        return float(cleaned)
    except:
        return default
