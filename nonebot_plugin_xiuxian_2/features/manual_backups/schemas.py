"""Result contract of one manual backup run."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ManualBackupResult:
    success: bool
    plugin_backup: Path | str
    config_backup: Path | str
    error: str = ""


__all__ = ["ManualBackupResult"]
