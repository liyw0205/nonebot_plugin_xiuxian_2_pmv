"""Converters that rewrite legacy receipt snapshots into the compact formats.

Each converter receives one stored row (``rowid`` + the snapshot column +
``user_id`` when the table has it) and returns the columns to update, or ``None``
when the row needs no change (already compact) or must not be touched (corrupt).

Returning ``None`` for a corrupt row is deliberate: the startup pass counts it as
``skipped`` and logs it, and the row stays byte-identical.  Nothing here deletes
or rewrites replay data (``response_json`` and the past-life/world-boss
``result_json`` are not in any converter's return value).
"""

from __future__ import annotations

import json
from typing import Any, Callable

from ..base.receipt_payload import RECEIPT_VERSION, build_payload
from ..pet.receipt_codec import ReceiptDecodeError, decode_receipt, encode_receipt

# Snapshot columns per task, used by ``schemas.TASKS``.
PAST_LIFE_EXTRA = {
    "advance": ("choice_idx", "stage", "revision"),
    "final": ("ending", "score", "exp", "stone", "points", "completed_at"),
    "start": ("choices_count", "refresh_slot_start"),
}


def document_of(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str) or not value.lstrip().startswith("{"):
        return None
    try:
        document = json.loads(value)
    except (TypeError, ValueError):
        return None
    return document if isinstance(document, dict) else None


def already_compact(value: Any) -> bool:
    document = document_of(value)
    return bool(document) and document.get("v") == RECEIPT_VERSION


def foreign_version(document: dict[str, Any] | None) -> bool:
    """True when the document declares a version this build does not own."""

    if not isinstance(document, dict):
        return False
    version = document.get("v")
    return version is not None and version != RECEIPT_VERSION


def convert_pet_hatch(row: dict[str, Any]) -> dict[str, Any] | None:
    payload = row.get("result_json")
    if already_compact(payload):
        return None
    try:
        pets, updated_meta, bag_limit = decode_receipt(payload)
    except ReceiptDecodeError:
        return None
    if not pets and not updated_meta and not bag_limit:
        return None
    return {"result_json": encode_receipt(pets, updated_meta, bag_limit)}


def convert_past_life(row: dict[str, Any], kind: str) -> dict[str, Any] | None:
    payload = row.get("payload")
    if not isinstance(payload, str) or already_compact(payload):
        return None
    document = document_of(payload)
    if document is None or foreign_version(document):
        return None
    fields: dict[str, Any] = {}
    for name in PAST_LIFE_EXTRA[kind]:
        value = document.get(name)
        if isinstance(value, (int, str)) and value != "":
            fields[name] = value
    if kind == "advance":
        expected = document.get("expected")
        if isinstance(expected, dict):
            for name in ("stage", "revision"):
                value = expected.get(name)
                if isinstance(value, int):
                    fields.setdefault(name, value)
    user_id = str(row.get("user_id") or document.get("user_id") or "")
    return {"payload": build_payload(kind=kind, snapshot=payload, user_id=user_id, **fields)}


def convert_world_boss(row: dict[str, Any]) -> dict[str, Any] | None:
    payload = row.get("payload")
    if not isinstance(payload, str) or already_compact(payload):
        return None
    document = document_of(payload)
    if document is None or foreign_version(document):
        return None
    bosses = document.get("bosses")
    trigger = document.get("trigger")
    return {
        "payload": build_payload(
            kind="full_refresh",
            snapshot=payload,
            trigger=str(trigger) if isinstance(trigger, str) else "",
            expected_revision=document.get("expected_revision"),
            boss_count=len(bosses) if isinstance(bosses, list) else 0,
        )
    }


def past_life_converter(kind: str) -> Callable[[dict[str, Any]], dict[str, Any] | None]:
    def convert(row: dict[str, Any]) -> dict[str, Any] | None:
        return convert_past_life(row, kind)

    return convert


__all__ = [
    "already_compact",
    "foreign_version",
    "document_of",
    "convert_pet_hatch",
    "convert_past_life",
    "convert_world_boss",
    "past_life_converter",
    "PAST_LIFE_EXTRA",
]
