from __future__ import annotations

from pathlib import Path

from .repository import CacheFileRepository


class CacheFileApplication:
    def __init__(self, repository: CacheFileRepository | None = None) -> None:
        self._repository = repository or CacheFileRepository()

    def resolve_download(self, cache_root: str | Path, filepath: str) -> Path:
        return self._repository.resolve_download(cache_root, filepath)


__all__ = ["CacheFileApplication"]
