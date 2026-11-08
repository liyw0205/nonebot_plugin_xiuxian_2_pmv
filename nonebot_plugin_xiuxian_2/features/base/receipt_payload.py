"""Compact idempotency payloads for operation receipts.

Several operation tables store a *full snapshot* in a ``payload`` column whose
only code use is ``stored == candidate`` for duplicate/conflict detection
(``past_life_choice_operations``, ``past_life_final_operations``,
``past_life_start_operations``, ``world_boss_full_refresh_operations``).  On the
live database those columns hold 140 MB of the 290 MB ``xiuxian.db`` and grow by
~1.5 MB per active day, while the parts a player can ever see are stored
separately in ``response_json``/``result_json``.

This module keeps the equality contract and drops the duplicated snapshot:

* ``build_payload`` writes a small versioned document with the identity the row
  already carries (``kind``, ``user_id``) plus a SHA-256 of the canonical
  snapshot string that would previously have been stored verbatim;
* ``payload_matches`` compares digests, and still recognises legacy rows by
  hashing the stored string, so a retry issued after this change against a
  receipt written before it is still a replay, not a conflict;
* ``payload_identity`` exposes ``user_id``/``kind`` without materialising the
  snapshot (legacy rows are parsed only as a fallback).

Trade-off, recorded in the audit: the full per-step internal state of an old
receipt is no longer recoverable.  The player-visible narrative stays in
``response_json``, and the current run state stays in the feature table itself.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

RECEIPT_VERSION = 2


def snapshot_digest(snapshot: str) -> str:
    """SHA-256 of the exact canonical snapshot string a v1 row would have stored."""

    return hashlib.sha256(str(snapshot).encode("utf-8")).hexdigest()


def _canonical(document: Mapping[str, Any]) -> str:
    return json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_payload(*, kind: str, snapshot: str, user_id: str = "", **fields: Any) -> str:
    """Build the compact v2 payload for ``snapshot``.

    ``fields`` are small, human-useful identity values (stage, revision,
    choice index, reward totals); they exist so an operator can still see *what*
    the receipt was about without the full snapshot.
    """

    document: dict[str, Any] = {
        "v": RECEIPT_VERSION,
        "kind": str(kind),
        "digest": snapshot_digest(snapshot),
    }
    if user_id not in ("", None):
        document["user_id"] = str(user_id)
    for key, value in fields.items():
        if value is not None:
            document[key] = value
    return _canonical(document)


def _load(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", "replace")
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def payload_identity(stored: Any) -> dict[str, str]:
    """Return ``{"user_id": ..., "kind": ...}`` when the payload carries them."""

    data = _load(stored)
    if not isinstance(data, dict):
        return {}
    identity: dict[str, str] = {}
    if data.get("user_id") not in (None, ""):
        identity["user_id"] = str(data["user_id"])
    if data.get("kind") not in (None, ""):
        identity["kind"] = str(data["kind"])
    return identity


def payload_matches(stored: Any, candidate: Any) -> bool:
    """True when ``candidate`` is the same request as the stored receipt."""

    if isinstance(stored, (bytes, bytearray)):
        stored = stored.decode("utf-8", "replace")
    if isinstance(candidate, (bytes, bytearray)):
        candidate = candidate.decode("utf-8", "replace")
    if not isinstance(candidate, str):
        candidate = _canonical(candidate) if isinstance(candidate, dict) else str(candidate)
    if not isinstance(stored, str):
        stored = _canonical(stored) if isinstance(stored, dict) else str(stored)
    data = _load(candidate)
    if not isinstance(data, dict) or data.get("v") != RECEIPT_VERSION:
        # Legacy candidate: byte equality is still the contract.
        return stored == candidate
    digest = data.get("digest")
    if not isinstance(digest, str) or not digest:
        return False
    previous = _load(stored)
    if isinstance(previous, dict) and previous.get("v") == RECEIPT_VERSION:
        return previous.get("digest") == digest
    # Legacy row: the stored string *is* the snapshot, so hash it.
    return snapshot_digest(stored) == digest


__all__ = [
    "RECEIPT_VERSION",
    "snapshot_digest",
    "build_payload",
    "payload_identity",
    "payload_matches",
]
