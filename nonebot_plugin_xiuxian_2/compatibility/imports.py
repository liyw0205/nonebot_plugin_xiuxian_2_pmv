"""Import shims for staged module migrations.

Keep this module dependency-free so old imports can be redirected without
initializing NoneBot or opening a database.
"""

from ..core.result import OperationOutcome, ReplyPlan

__all__ = ["OperationOutcome", "ReplyPlan"]
