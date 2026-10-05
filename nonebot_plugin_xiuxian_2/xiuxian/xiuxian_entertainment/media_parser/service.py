"""Compatibility facade for the feature-owned media parser workflow."""
from __future__ import annotations

from typing import Any

from ....features.entertainment.media_parser_application import (
    collect_media_urls,
    dedupe_media_urls_preserve_order,
    format_media_meta_line as _format_meta_line,
)


async def extract_links(text: str) -> list[tuple[str, str]]:
    from ..room_store import entertainment_application

    return entertainment_application.media_parser.extract_links(text or "")


async def parse_text(text: str) -> list[dict[str, Any]]:
    from ..room_store import entertainment_application

    return await entertainment_application.media_parser.parse_metas(text or "")


def last_init_error() -> str | None:
    return None


async def run_parse_and_build_messages(
    text: str,
) -> tuple[list[str], list[str], list[str], list[str]]:
    from ..room_store import entertainment_application

    return await entertainment_application.media_parser.parse_and_build_messages(text)


__all__ = [
    "collect_media_urls",
    "dedupe_media_urls_preserve_order",
    "extract_links",
    "last_init_error",
    "parse_text",
    "run_parse_and_build_messages",
]
