"""Application-independent contracts for the refactored plugin.

The legacy ``xiuxian`` package remains available during migration.  New code
should depend on this package instead of importing NoneBot, Flask or a database
driver from a domain module.
"""

from .errors import ConflictError, DomainError, ForbiddenError, ValidationError
from .result import OperationOutcome, ReplyPlan
from .application import OperationRunner

__all__ = [
    "ConflictError",
    "DomainError",
    "ForbiddenError",
    "OperationOutcome",
    "OperationRunner",
    "ReplyPlan",
    "ValidationError",
]
