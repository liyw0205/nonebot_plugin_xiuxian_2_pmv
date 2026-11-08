"""Disk-space measurement and a guarded one-time SQLite space reclaim.

A row rewrite (e.g. compacting a receipt payload) usually frees pages *inside*
existing pages first, and only later returns whole pages to the freelist.  The
file therefore keeps its size until a `VACUUM` rebuilds it.  This module makes
that explicit instead of implying that "smaller rows" means "smaller file":

* :func:`database_space` reports the file size, page accounting, freelist bytes
  (the guaranteed reclaimable floor) and page-internal `unused` bytes from
  `dbstat` when the build supports it;
* :func:`reclaim_space` runs `VACUUM` only when a caller asked for it, the
  database actually has freelist pages, and the volume has enough headroom for
  the rebuild.  It never runs inside a transaction (SQLite forbids it), and it
  never touches a database it did not measure successfully.

The caller owns the policy (when, once-only marking, logging); this module only
refuses to do something unsafe.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any

# A rebuild needs roughly the database size again as temporary space; keep a
# safety factor so a nearly-full volume is never the place we discover that.
HEADROOM_FACTOR = 2.2
MIN_HEADROOM_BYTES = 256 * 1024 * 1024


def _ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=2.0)
    conn.execute("PRAGMA busy_timeout=2000")
    return conn


def database_space(path: str | Path) -> dict[str, Any]:
    """Return size/free-space accounting for one SQLite file (read-only)."""

    database = Path(path)
    if not database.is_file():
        return {
            "path": str(database),
            "exists": False,
            "file_bytes": 0,
            "page_size": 0,
            "page_count": 0,
            "freelist_pages": 0,
            "freelist_bytes": 0,
            "page_unused_bytes": 0,
            "reclaimable_bytes": 0,
        }
    report: dict[str, Any] = {"path": str(database), "exists": True}
    report["file_bytes"] = database.stat().st_size
    conn = _ro(database)
    try:
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0] or 0)
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0] or 0)
        freelist = int(conn.execute("PRAGMA freelist_count").fetchone()[0] or 0)
        try:
            unused = int(conn.execute("SELECT COALESCE(SUM(unused),0) FROM dbstat").fetchone()[0] or 0)
        except sqlite3.DatabaseError:
            unused = 0
    finally:
        conn.close()
    report.update(
        page_size=page_size,
        page_count=page_count,
        freelist_pages=freelist,
        freelist_bytes=page_size * freelist,
        page_unused_bytes=unused,
        reclaimable_bytes=page_size * freelist,
    )
    return report


def available_bytes(path: str | Path) -> int:
    """Free bytes on the volume holding ``path`` (its nearest existing parent)."""

    target = Path(path)
    while not target.exists() and target != target.parent:
        target = target.parent
    try:
        return int(shutil.disk_usage(str(target)).free)
    except OSError:
        return 0


def reclaim_space(
    path: str | Path,
    *,
    headroom_factor: float = HEADROOM_FACTOR,
    min_headroom_bytes: int = MIN_HEADROOM_BYTES,
    free_bytes: int | None = None,
    require_backup_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Run ``VACUUM`` when it is safe; return the outcome with before/after sizes.

    ``status`` is one of ``reclaimed`` / ``no_free_pages`` / ``no_headroom`` /
    ``no_backup`` / ``missing`` / ``failed``.  ``failed`` carries ``error``.

    ``require_backup_dir`` implements the deployment's existing backup rule: the
    rebuild is a file-level rewrite, so it only runs when that directory already
    holds at least one snapshot archive.  Without one the reclaim stays pending
    and a later boot retries.
    """

    before = database_space(path)
    if not before["exists"]:
        return {"status": "missing", "before": before, "after": before}
    if before["freelist_pages"] <= 0:
        return {"status": "no_free_pages", "before": before, "after": before}
    if require_backup_dir is not None:
        backup_dir = Path(require_backup_dir)
        has_backup = backup_dir.is_dir() and any(backup_dir.glob("*.zip"))
        if not has_backup:
            return {
                "status": "no_backup",
                "before": before,
                "after": before,
                "backup_dir": str(backup_dir),
            }
    headroom = available_bytes(path) if free_bytes is None else int(free_bytes)
    required = max(int(min_headroom_bytes), int(before["file_bytes"] * float(headroom_factor)))
    if headroom < required:
        return {
            "status": "no_headroom",
            "before": before,
            "after": before,
            "required_bytes": required,
            "available_bytes": headroom,
        }
    try:
        # VACUUM cannot run inside a transaction: use autocommit and a dedicated
        # connection so no other code's unit of work is involved.
        conn = sqlite3.connect(str(Path(path)), timeout=30, isolation_level=None)
        try:
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("VACUUM")
        finally:
            conn.close()
    except (sqlite3.DatabaseError, OSError) as exc:
        return {"status": "failed", "error": f"{type(exc).__name__}: {exc}", "before": before, "after": database_space(path)}
    after = database_space(path)
    return {
        "status": "reclaimed",
        "before": before,
        "after": after,
        "reclaimed_bytes": max(0, int(before["file_bytes"]) - int(after["file_bytes"])),
    }


__all__ = ["database_space", "available_bytes", "reclaim_space", "HEADROOM_FACTOR", "MIN_HEADROOM_BYTES"]
