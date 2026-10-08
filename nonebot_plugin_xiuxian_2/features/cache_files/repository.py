from __future__ import annotations

from pathlib import Path


class CacheFileOutsideRoot(ValueError):
    """Raised when a resolved cache path escapes its configured root."""


class CacheFileNotFound(FileNotFoundError):
    """Raised when a requested cache file does not exist."""


class CacheFileNotRegular(ValueError):
    """Raised when a requested cache path is not a regular file."""


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
