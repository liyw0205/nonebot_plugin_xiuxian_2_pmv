from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
from urllib.request import Request

from ...paths import get_paths
from .application import StickerApplication
from .repository import StickerRepository


def build_sticker_application(
    root: Path | None = None,
    *,
    open_url: Callable[[Request, int], Any] | None = None,
    thread_starter: Callable[[Callable[[], None]], None] | None = None,
) -> StickerApplication:
    data_root = Path(root) if root is not None else get_paths().data / "stickers"
    repository = StickerRepository(data_root, open_url=open_url)
    return StickerApplication(repository, thread_starter=thread_starter)


__all__ = ["build_sticker_application"]
