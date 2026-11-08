"""Compact replay payload for pet operation receipts.

A pet receipt (``pet_hatch_operations.result_json``) only has to answer one
question: "what did the player already receive for this operation id?".  Before
this codec existed the answer was written as the *fully materialised* pet
dictionaries produced by ``xiuxian_utils.pet_system._normalize_pet``, which sets
``pet["skill"] = pet["skills"][0]``.  Serialising that snapshot therefore stored
the same 33-key skill definition twice per pet (measured on the live database:
1.40 KB of the 1.84 KB per pet, 37.9% of the whole receipt), and every catalog
attribute that the replay screen never reads was stored again for every pet.

The codec keeps the replay contract and removes the duplication:

* every pet key is preserved verbatim, including unknown/extra fields and the
  exact shape of ``skill`` and ``skills`` (multiple skills, ``skill`` that is not
  ``skills[0]``, non-mapping entries), so a stored ``PetHatchResult`` round-trips
  without weakening what callers can read;
* skill definitions collapse into one table per receipt and are referenced by
  index, so a ten-egg batch shares its repeated definitions;
* only the frozen display/identity subset of a mapping-skill is stored
  (``skill_id``, ``name``, ``type``, ``scope``, ``category``, ``desc``).  Display
  text is frozen at write time so a later catalog rename cannot rewrite history,
  while catalog-owned numbers (``base_power``/``effect``/``star_bonus``/...) stay
  re-resolvable by ``skill_id`` and are not duplicated into the receipt;
* JSON is written without ASCII escaping so CJK text costs 3 UTF-8 bytes instead
  of 6 escaped bytes (the value is only ever read back through ``json.loads``).

Reading is deliberately strict about corruption: a malformed or unknown-version
receipt raises :class:`ReceiptDecodeError` instead of pretending the operation
returned no pets.  Callers must surface that as an explicit failure and must not
charge again -- the receipt row exists, so the operation did happen.
"""

from __future__ import annotations

import json
from typing import Any, Iterable, Sequence

RECEIPT_VERSION = 2

# Frozen display/identity subset of one skill definition.  Order is the storage
# order of the skill table rows, so it must stay append-only.
_FROZEN_SKILL_FIELDS = ("skill_id", "name", "type", "scope", "category", "desc")

_HAS_SKILL = 1
_HAS_SKILLS = 2
_RAW_KEY = "raw"


class ReceiptDecodeError(ValueError):
    """A stored receipt exists but cannot be interpreted."""


def _skill_ref(value: Any, table: list, index: dict[str, int]) -> Any:
    """Return the stored reference for one skill value (mapping or verbatim)."""

    if isinstance(value, dict):
        frozen = [value.get(field, "") for field in _FROZEN_SKILL_FIELDS]
        marker = json.dumps(frozen, ensure_ascii=False, sort_keys=True)
        slot = index.get(marker)
        if slot is None:
            slot = len(table)
            index[marker] = slot
            table.append(frozen)
        return slot
    return {_RAW_KEY: value}


def encode_receipt(
    pets: Iterable[Sequence[Any]],
    updated_meta: Sequence[Any] = (),
    bag_limit: int = 0,
) -> str:
    """Serialise ``((pet, active), ...)`` into the compact v2 document."""

    table: list[list] = []
    index: dict[str, int] = {}
    rows: list[list] = []
    for entry in pets:
        if isinstance(entry, (list, tuple)) and len(entry) == 2:
            pet, active = entry[0], entry[1]
        else:  # tolerate a bare pet mapping
            pet, active = entry, False
        if not isinstance(pet, dict):
            continue
        flags = 0
        skill_ref: Any = None
        skills_ref: Any = None
        if "skill" in pet:
            flags |= _HAS_SKILL
            skill_ref = _skill_ref(pet["skill"], table, index)
        if "skills" in pet:
            flags |= _HAS_SKILLS
            value = pet["skills"]
            if isinstance(value, list):
                skills_ref = [_skill_ref(item, table, index) for item in value]
            else:
                skills_ref = _skill_ref(value, table, index)
        stored = {key: value for key, value in pet.items() if key not in ("skill", "skills")}
        rows.append([stored, bool(active), skill_ref, skills_ref, flags])
    document = {
        "v": RECEIPT_VERSION,
        "pets": rows,
        "skills": table,
        "updated_meta": list(updated_meta or ()),
        "bag_limit": int(bag_limit or 0),
    }
    return json.dumps(document, ensure_ascii=False, separators=(",", ":"))


def _resolve(ref: Any, table: Sequence[Sequence[Any]]) -> Any:
    if isinstance(ref, int) and not isinstance(ref, bool):
        if ref < 0 or ref >= len(table):
            raise ReceiptDecodeError("skill reference out of range")
        frozen = table[ref]
        if not isinstance(frozen, (list, tuple)) or len(frozen) != len(_FROZEN_SKILL_FIELDS):
            raise ReceiptDecodeError("malformed skill definition")
        return dict(zip(_FROZEN_SKILL_FIELDS, frozen))
    if isinstance(ref, dict) and _RAW_KEY in ref and len(ref) == 1:
        return ref[_RAW_KEY]
    raise ReceiptDecodeError("malformed skill reference")


def _restore_pet(row: Any, table: Sequence[Sequence[Any]]) -> tuple[dict, bool]:
    if not isinstance(row, (list, tuple)) or len(row) != 5:
        raise ReceiptDecodeError("malformed pet row")
    stored, active, skill_ref, skills_ref, flags = row
    if not isinstance(stored, dict) or not isinstance(flags, int) or isinstance(flags, bool):
        raise ReceiptDecodeError("malformed pet row")
    if flags & ~(_HAS_SKILL | _HAS_SKILLS):
        raise ReceiptDecodeError("unknown pet flags")
    pet: dict[str, Any] = {str(key): value for key, value in stored.items()}
    if flags & _HAS_SKILL:
        pet["skill"] = _resolve(skill_ref, table)
    if flags & _HAS_SKILLS:
        if isinstance(skills_ref, list):
            pet["skills"] = [_resolve(item, table) for item in skills_ref]
        else:
            pet["skills"] = _resolve(skills_ref, table)
    return pet, bool(active)


def decode_receipt(value: Any) -> tuple[tuple[tuple[dict, bool], ...], tuple, int]:
    """Return ``(pets, updated_meta, bag_limit)`` for a stored receipt.

    Accepts the compact v2 document and the legacy v1 document.  Raises
    :class:`ReceiptDecodeError` when a receipt exists but cannot be interpreted
    (invalid JSON, unknown version, malformed rows or references) so callers can
    fail closed instead of silently replaying "no pets".
    """

    if isinstance(value, (dict, list)):
        data = value
    elif isinstance(value, (str, bytes, bytearray)):
        text = value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else value
        if not text.strip():
            raise ReceiptDecodeError("empty receipt payload")
        try:
            data = json.loads(text)
        except (TypeError, ValueError) as exc:
            raise ReceiptDecodeError("receipt payload is not valid JSON") from exc
    elif value is None:
        raise ReceiptDecodeError("missing receipt payload")
    else:
        raise ReceiptDecodeError("unsupported receipt payload type")
    if not isinstance(data, dict):
        raise ReceiptDecodeError("receipt payload is not an object")
    version = data.get("v")
    if version is not None and version != RECEIPT_VERSION:
        raise ReceiptDecodeError(f"unknown receipt version {version!r}")
    updated_meta = tuple(data.get("updated_meta") or ())
    try:
        bag_limit = int(data.get("bag_limit") or 0)
    except (TypeError, ValueError):
        bag_limit = 0
    if version == RECEIPT_VERSION:
        table = data.get("skills")
        rows = data.get("pets")
        if not isinstance(table, list) or not isinstance(rows, list):
            raise ReceiptDecodeError("malformed v2 receipt")
        pets = tuple(_restore_pet(row, table) for row in rows)
        return pets, updated_meta, bag_limit
    # legacy v1: pets are stored as [[pet_mapping, active], ...]
    rows = data.get("pets")
    if rows is None:
        rows = []
    if not isinstance(rows, list):
        raise ReceiptDecodeError("malformed legacy receipt")
    pets = []
    for entry in rows:
        if not isinstance(entry, (list, tuple)) or len(entry) != 2 or not isinstance(entry[0], dict):
            raise ReceiptDecodeError("malformed legacy pet entry")
        pets.append((dict(entry[0]), bool(entry[1])))
    return tuple(pets), updated_meta, bag_limit


__all__ = ["RECEIPT_VERSION", "ReceiptDecodeError", "encode_receipt", "decode_receipt"]
