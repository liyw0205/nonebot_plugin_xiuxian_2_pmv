from __future__ import annotations

from pathlib import Path

# The refusal types are the slice contract; they are declared once in schemas.py
# and re-exported here because the legacy Web module imports them at this path.
from .schemas import CacheFileNotFound, CacheFileNotRegular, CacheFileOutsideRoot


class CacheFileRepository:
    def resolve_download(self, cache_root: str | Path, filepath: str) -> Path:
        root = Path(cache_root).resolve()
        candidate = root.joinpath(str(filepath)).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise CacheFileOutsideRoot(filepath) from exc

        if not candidate.exists():
            raise CacheFileNotFound(filepath)
        if not candidate.is_file():
            raise CacheFileNotRegular(filepath)
        return candidate


__all__ = [
    "CacheFileNotFound",
    "CacheFileNotRegular",
    "CacheFileOutsideRoot",
    "CacheFileRepository",
]
