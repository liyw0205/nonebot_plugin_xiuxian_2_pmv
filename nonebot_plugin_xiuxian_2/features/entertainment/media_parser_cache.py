from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from nonebot.log import logger

from ...paths import get_paths
from ...xiuxian.xiuxian_entertainment.media_parser.config import (
    default_raw_config,
    media_parser_cache_dir,
)


def _iter_cache_files(roots: list[Path]):
    for root in roots:
        if root.is_dir():
            for path in root.rglob("*"):
                if path.is_file():
                    yield path


def cleanup_media_parser_cache(
    *, keep_days: int | None = None, max_total_mb: int | None = None, include_legacy: bool = True
) -> dict[str, Any]:
    download = (default_raw_config().get("download") or {})
    if keep_days is None:
        keep_days = int(download.get("cache_keep_days") or 0)
    if max_total_mb is None:
        max_total_mb = int(download.get("cache_max_total_mb") or 0)

    roots = [media_parser_cache_dir()]
    if include_legacy:
        roots.append((get_paths().data / "media_parser_cache").resolve())

    removed = 0
    freed = 0
    now = time.time()
    files = list(_iter_cache_files(roots))
    remain: list[Path] = []
    threshold = now - keep_days * 86400 if keep_days and keep_days > 0 else None
    for path in files:
        try:
            stat = path.stat()
        except OSError:
            continue
        if threshold is not None and stat.st_mtime < threshold:
            try:
                path.unlink(missing_ok=True)
                removed += 1
                freed += stat.st_size
                continue
            except OSError:
                pass
        remain.append(path)

    if max_total_mb and max_total_mb > 0:
        sized: list[tuple[float, int, Path]] = []
        total = 0
        for path in remain:
            try:
                stat = path.stat()
            except OSError:
                continue
            sized.append((stat.st_mtime, stat.st_size, path))
            total += stat.st_size
        limit = max_total_mb * 1024 * 1024
        if total > limit:
            sized.sort(key=lambda row: row[0])
            for _, size, path in sized:
                if total <= limit:
                    break
                try:
                    path.unlink(missing_ok=True)
                    removed += 1
                    freed += size
                    total -= size
                except OSError:
                    pass

    for root in roots:
        if not root.is_dir():
            continue
        for directory in sorted(root.rglob("*"), reverse=True):
            if not directory.is_dir():
                continue
            try:
                next(directory.iterdir())
            except StopIteration:
                try:
                    directory.rmdir()
                except OSError:
                    pass
            except OSError:
                pass

    result = {
        "removed": removed,
        "freed_bytes": freed,
        "keep_days": keep_days,
        "max_total_mb": max_total_mb,
        "roots": [str(root) for root in roots],
    }
    if removed:
        logger.info(
            f"[media_parser_cache] 清理完成：删除 {removed} 个文件，"
            f"释放 {freed / (1024 * 1024):.1f}MB "
            f"(keep_days={keep_days}, max_total_mb={max_total_mb})"
        )
    return result


__all__ = ["cleanup_media_parser_cache"]
