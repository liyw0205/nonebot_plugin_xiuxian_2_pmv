"""Failure contract for files served out of the runtime cache directory.

``GET /download/<path:filepath>`` hands a user-controlled relative path to this
owner, so the durable contract is the confinement rule plus the three ways a
request can be refused.  There is deliberately no size, age or extension policy:
the route serves whatever the runtime cache already contains, and the checks in
``CacheFileRepository.resolve_download`` are the whole guarantee.
"""

from __future__ import annotations


class CacheFileOutsideRoot(ValueError):
    """Raised when a resolved cache path escapes its configured root."""


class CacheFileNotFound(FileNotFoundError):
    """Raised when a requested cache file does not exist."""


class CacheFileNotRegular(ValueError):
    """Raised when a requested cache path is not a regular file."""


__all__ = [
    "CacheFileNotFound",
    "CacheFileNotRegular",
    "CacheFileOutsideRoot",
]
