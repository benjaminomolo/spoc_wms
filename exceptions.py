# exceptions.py

class BaseAppError(Exception):
    """Base class for custom application-level exceptions."""
    def __init__(self, message=None):
        if message is None:
            message = "An unexpected application error occurred."
        super().__init__(message)


class DatabaseError(BaseAppError):
    """Raised when a database operation fails."""
    def __init__(self, message="A database error occurred."):
        super().__init__(message)


class ValidationError(BaseAppError):
    """Raised when input validation fails."""
    def __init__(self, message="Invalid data provided."):
        super().__init__(message)


class InventoryError(BaseAppError):
    """Raised when inventory logic fails (e.g., over-issue, negative stock)."""
    def __init__(self, message="Inventory operation failed."):
        super().__init__(message)


class NotFoundError(BaseAppError):
    """Raised when a requested resource or record is not found."""
    def __init__(self, message="The requested resource was not found."):
        super().__init__(message)


class AuthorizationError(BaseAppError):
    """Raised when a user tries to perform an unauthorized action."""
    def __init__(self, message="You are not authorized to perform this action."):
        super().__init__(message)
