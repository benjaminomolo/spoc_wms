
def check_list_not_empty(lst):
    """
    Checks if the list has at least one item.

    Args:
        lst (list): The list to check.

    Raises:
        ValueError: If the list is empty.
    """
    if not lst:  # This checks if the list is empty (same as len(lst) == 0)
        raise ValueError("The list is empty.")
