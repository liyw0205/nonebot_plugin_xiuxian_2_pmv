"""Web surface of the release-check and upgrade owner.

``ROUTES`` stays empty: the three paths below are still registered by the legacy
Flask module ``xiuxian/xiuxian_web/pages.py`` behind ``WebPermission.UPDATE``, and
``/pages/update`` plus the ``/api/v1/**`` equivalents belong to the ``runtime_web``
manifest.  ``GET /update`` only renders ``update.html``, so it delegates nothing
and is not listed.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("GET", "/check_update", "UpdateApplication.check_update"),
    ("GET", "/get_releases", "UpdateApplication.latest_releases"),
    ("POST", "/perform_update", "UpdateApplication.perform_update_with_backup"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
