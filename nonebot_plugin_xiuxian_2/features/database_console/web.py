"""Web surface of the database console.

``ROUTES`` stays empty for two reasons.  ``GET /database`` is already declared by
the ``runtime_web`` manifest (``bootstrap/platform_manifest.py``), so claiming it
here would be a duplicate route, and the remaining three paths are still
registered by the legacy Flask module ``xiuxian/xiuxian_web/database.py``.  The
GET half of the row path resolves the row through ``row_key``/``read_row`` and the
DELETE half through ``delete_row``; only the write delegation is listed.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("GET", "/database", "DatabaseConsoleApplication.list_tables"),
    ("GET", "/table/<table_name>", "DatabaseConsoleApplication.table_data"),
    ("POST", "/table/<table_name>/<row_id>", "DatabaseConsoleApplication.update_row"),
    ("POST", "/batch_edit/<table_name>", "DatabaseConsoleApplication.batch_edit"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
