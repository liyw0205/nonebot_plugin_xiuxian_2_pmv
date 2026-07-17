"""媒体解析缓存清理：按天数 + 总大小上限。"""

from __future__ import annotations

import time
from pathlib import Path

from nonebot.log import logger

from ....paths import get_paths
from .config import default_raw_config, media_parser_cache_dir


def _iter_cache_files(roots: list[Path]) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if path.is_file():
                files.append(path)
    return files


def cleanup_media_parser_cache(
    *,
    keep_days: int | None = None,
    max_total_mb: int | None = None,
    include_legacy: bool = True,
) -> dict:
    """
    清理视频解析缓存。
    - keep_days: 超过该天数的文件删除；0/None 用配置，配置也为 0 则跳过按天清理
    - max_total_mb: 总大小上限；超限从最旧删起
    - include_legacy: 同时清理旧目录 data/media_parser_cache
    """
    raw = default_raw_config()
    download = raw.get("download") or {}
    if keep_days is None:
        keep_days = int(download.get("cache_keep_days") or 0)
    if max_total_mb is None:
        max_total_mb = int(download.get("cache_max_total_mb") or 0)

    roots = [media_parser_cache_dir()]
    if include_legacy:
        roots.append((get_paths().data / "media_parser_cache").resolve())
        # messaging 通用媒体缓存也在 cache/media，不在此清，避免误伤

    files = _iter_cache_files(roots)
    removed = 0
    freed = 0
    now = time.time()

    # 1) 按天清理
    if keep_days and keep_days > 0:
        threshold = now - keep_days * 86400
        remain: list[Path] = []
        for path in files:
            try:
                mtime = path.stat().st_mtime
                size = path.stat().st_size
            except OSError:
                continue
            if mtime < threshold:
                try:
                    path.unlink(missing_ok=True)
                    removed += 1
                    freed += size
                except OSError:
                    remain.append(path)
            else:
                remain.append(path)
        files = remain

    # 2) 按总大小清理（最旧优先）
    if max_total_mb and max_total_mb > 0:
        limit = max_total_mb * 1024 * 1024
        sized: list[tuple[float, int, Path]] = []
        total = 0
        for path in files:
            try:
                st = path.stat()
            except OSError:
                continue
            sized.append((st.st_mtime, st.st_size, path))
            total += st.st_size
        if total > limit:
            sized.sort(key=lambda x: x[0])  # oldest first
            for mtime, size, path in sized:
                if total <= limit:
                    break
                try:
                    path.unlink(missing_ok=True)
                    removed += 1
                    freed += size
                    total -= size
                except OSError:
                    pass

    # 清理空目录（仅 cache 子树）
    for root in roots:
        if not root.is_dir():
            continue
        for dirpath in sorted(root.rglob("*"), reverse=True):
            if dirpath.is_dir():
                try:
                    next(dirpath.iterdir())
                except StopIteration:
                    try:
                        dirpath.rmdir()
                    except OSError:
                        pass
                except OSError:
                    pass

    result = {
        "removed": removed,
        "freed_bytes": freed,
        "keep_days": keep_days,
        "max_total_mb": max_total_mb,
        "roots": [str(r) for r in roots],
    }
    if removed:
        logger.info(
            f"[media_parser_cache] 清理完成：删除 {removed} 个文件，"
            f"释放 {freed / (1024 * 1024):.1f}MB "
            f"(keep_days={keep_days}, max_total_mb={max_total_mb})"
        )
    return result
