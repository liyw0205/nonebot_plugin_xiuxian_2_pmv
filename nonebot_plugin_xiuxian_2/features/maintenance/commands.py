"""This feature exposes no player commands.

Compaction runs from the startup hook, not from a matcher, so the command table is
explicitly empty rather than omitted (an omitted table reads as "not audited").
"""

from __future__ import annotations

COMMANDS: tuple = ()

__all__ = ["COMMANDS"]
