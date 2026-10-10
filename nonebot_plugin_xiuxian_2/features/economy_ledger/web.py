"""Web delegation record for the economy-ledger read model.

The routes are registered by the runtime Web adapter so this feature does not
declare duplicate ``RouteSpec`` entries.  The legacy WSGI module still owns
registration and injects the existing application into that adapter.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("GET", "/api/v1/economy-logs", "EconomyLedgerApplication.query_page"),
    ("GET", "/api/v1/economy-logs/export", "EconomyLedgerApplication.iter_export_rows"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
