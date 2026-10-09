"""Download and install contract of the sticker-pack owner.

One sticker pack is a zip published as a GitHub release asset plus a
``stickers-manifest.json`` index; locally it is ``data/stickers/<pack_id>`` with
``pack.json`` and ``*.webp`` members.  Every published release URL is built from
``FILE_REPO_OWNER``/``FILE_REPO_NAME``/``STICKERS_RELEASE_TAG``, every accepted
name matches one of the patterns below, and no download or extraction may exceed
the caps declared here.  These values used to be spread across
``repository.py`` next to their single use site.
"""

from __future__ import annotations

import re

FILE_REPO_OWNER = "liyw0205"
FILE_REPO_NAME = "nonebot_plugin_xiuxian_2_pmv_file"
STICKERS_RELEASE_TAG = "stickers-latest"
STICKERS_MANIFEST_NAME = "stickers-manifest.json"
DOWNLOAD_PROXY_PREFIX = "https://ghproxy.net/"
DOWNLOAD_USER_AGENT = "xiuxian-web-stickers/1.0"

MAX_STICKER_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_STICKER_FILES = 2048
MAX_STICKER_ARCHIVE_MEMBERS = 4096
MAX_STICKER_FILE_BYTES = 16 * 1024 * 1024
MAX_STICKER_UNCOMPRESSED_BYTES = 128 * 1024 * 1024
DOWNLOAD_CHUNK_BYTES = 64 * 1024
MANIFEST_TIMEOUT_SECONDS = 20
ARCHIVE_TIMEOUT_SECONDS = 60

PACK_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
STICKER_FILE_PATTERN = re.compile(r"^[A-Za-z0-9._-]+\.webp$")
ZIP_NAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]+\.zip$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
STICKER_TOKEN_PATTERN = re.compile(r"^([a-z0-9][a-z0-9_-]{0,31})/([A-Za-z0-9._-]+)$")

# A download may only ever be re-directed inside these hosts, and the first
# request of a chain may only target the release host or its mirror.
ALLOWED_DOWNLOAD_HOSTS = frozenset(
    {
        "github.com",
        "ghproxy.net",
        "objects.githubusercontent.com",
        "release-assets.githubusercontent.com",
    }
)
INITIAL_REQUEST_HOSTS = frozenset({"github.com", "ghproxy.net"})

__all__ = [
    "ALLOWED_DOWNLOAD_HOSTS",
    "ARCHIVE_TIMEOUT_SECONDS",
    "DOWNLOAD_CHUNK_BYTES",
    "DOWNLOAD_PROXY_PREFIX",
    "DOWNLOAD_USER_AGENT",
    "FILE_REPO_NAME",
    "FILE_REPO_OWNER",
    "INITIAL_REQUEST_HOSTS",
    "MANIFEST_TIMEOUT_SECONDS",
    "MAX_MANIFEST_BYTES",
    "MAX_STICKER_ARCHIVE_BYTES",
    "MAX_STICKER_ARCHIVE_MEMBERS",
    "MAX_STICKER_FILE_BYTES",
    "MAX_STICKER_FILES",
    "MAX_STICKER_UNCOMPRESSED_BYTES",
    "PACK_ID_PATTERN",
    "SHA256_PATTERN",
    "STICKER_FILE_PATTERN",
    "STICKER_TOKEN_PATTERN",
    "STICKERS_MANIFEST_NAME",
    "STICKERS_RELEASE_TAG",
    "ZIP_NAME_PATTERN",
]
