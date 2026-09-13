from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def project_legacy_fields(row: Mapping[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    """Read-only projection for legacy JSON/row callers."""
    return {field: row[field] for field in fields if field in row}


__all__ = ["project_legacy_fields"]
