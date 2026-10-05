"""Compatibility wrapper for the feature-owned bounded parser workflow."""
from __future__ import annotations

from typing import Any


async def run_native_parse(text: str) -> list[dict[str, Any]]:
    from ..room_store import entertainment_application

    return await entertainment_application.media_parser.parse_metas(text or "")
