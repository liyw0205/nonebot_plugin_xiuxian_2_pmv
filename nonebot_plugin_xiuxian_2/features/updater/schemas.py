"""Durable bounds of the release-check and upgrade owner.

Two of these values are wire-visible.  ``UPDATE_ASSET_NAME`` is the asset name a
release must carry for the operator flow to accept it, and ``RELEASE_TAG_PATTERN``
is the guard that keeps a caller-supplied tag out of the download path, the
extract directory and the backup file name; a tag that fails it is rejected
before any provider call.  ``DEFAULT_RELEASE_LIST_COUNT`` is what the releases
panel asks for when the client sends no count.
"""

from __future__ import annotations

import re


UPDATE_ASSET_NAME = "project.tar.gz"
RELEASE_TAG_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}\Z")
DEFAULT_RELEASE_LIST_COUNT = 10

__all__ = ["DEFAULT_RELEASE_LIST_COUNT", "RELEASE_TAG_PATTERN", "UPDATE_ASSET_NAME"]
