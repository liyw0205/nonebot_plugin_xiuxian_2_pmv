"""Durable request contract of the QQ image-upload owner.

Two literals define this boundary: the adapter name that makes a bot eligible to
receive the upload, and the file-identification mode the QQ API is called with.
Both used to be inline in ``QqImageUploadApplication``.
"""

from __future__ import annotations

QQ_ADAPTER_NAME = "QQ"
UPLOAD_FILE_MODE = "md5"

__all__ = ["QQ_ADAPTER_NAME", "UPLOAD_FILE_MODE"]
