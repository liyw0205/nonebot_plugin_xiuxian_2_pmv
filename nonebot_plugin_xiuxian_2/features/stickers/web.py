"""Web surface of the sticker-catalogue owner.

``ROUTES`` is empty: all four paths below are still registered by the legacy
Flask module ``xiuxian/xiuxian_web/stickers.py`` behind the admin session, and
they delegate to the module-level ``sticker_application`` built by
``features/stickers/runtime.py``.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("GET", "/api/messages/stickers", "StickerApplication.catalog"),
    ("POST", "/api/messages/stickers/install", "StickerApplication.start_install"),
    ("GET", "/api/messages/stickers/install/<job_id>", "StickerApplication.install_status"),
    (
        "GET",
        "/api/messages/stickers/file/<pack_id>/<path:filename>",
        "StickerApplication.resolve_file",
    ),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
