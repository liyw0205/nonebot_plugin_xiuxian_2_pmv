"""No schema is owned here.

The compaction pass writes a JSON progress file under ``data/maintenance`` and
rewrites existing columns in place; it never creates tables, so a database
without the receipt tables simply reports those tasks as done.  The module exists
because every feature package must state its migration ownership explicitly.
"""

from __future__ import annotations

from ...infrastructure.database import DatabaseUnitOfWork


def apply_receipt_compaction(uow: DatabaseUnitOfWork) -> None:
    """Intentionally a no-op: this feature owns no tables."""

    return None


MIGRATION_KEY = "maintenance.001"

__all__ = ["apply_receipt_compaction", "MIGRATION_KEY"]
