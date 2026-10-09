"""Web surface of the QQ image-upload owner.

``ROUTES`` is empty: ``POST /upload_image`` is still registered by the legacy
Flask module ``xiuxian/xiuxian_web/system.py:254``.  That handler keeps the
loopback-only gate for itself, reads the uploaded part, picks a QQ bot with
``QqImageUploadApplication.select_qq_bot`` and then delegates the upload to
``QqImageUploadApplication.upload_image``.
"""

ROUTES = ()
LEGACY_ROUTES = (
    ("POST", "/upload_image", "QqImageUploadApplication.upload_image"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
