"""Web surface of the cache-file download owner.

``ROUTES`` stays empty: the path below is still registered by the legacy Flask
module ``xiuxian/xiuxian_web/system.py:165`` behind the admin session, which
maps ``CacheFileOutsideRoot``/``CacheFileNotRegular`` to ``403`` and
``CacheFileNotFound`` to ``404`` before delegating to this owner.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("GET", "/download/<path:filepath>", "CacheFileApplication.resolve_download"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
