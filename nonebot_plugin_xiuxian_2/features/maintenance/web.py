"""This feature exposes no web routes.

Receipt compaction is a startup maintenance step; operators read its outcome from
the startup log and the state file under ``data/maintenance``.
"""

from __future__ import annotations

ROUTES: tuple = ()

__all__ = ["ROUTES"]
