from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any


DATA_DIR_ENV = "XIUXIAN_DATA_DIR"


@dataclass(frozen=True, slots=True)
class XiuxianPaths:
    """Resolved filesystem locations used by the plugin runtime."""

    package_root: Path
    data: Path

    @property
    def data_root(self) -> Path:
        return self.data.parent

    @property
    def player_db(self) -> Path:
        return self.data / "player.db"

    @property
    def game_db(self) -> Path:
        return self.data / "xiuxian.db"

    @property
    def trade_db(self) -> Path:
        return self.data / "trade.db"

    @property
    def impart_db(self) -> Path:
        return self.data / "xiuxian_impart.db"

    @property
    def message_db(self) -> Path:
        return self.data / "message.db"

    @property
    def backups(self) -> Path:
        return self.data / "backups"

    @property
    def cache(self) -> Path:
        return self.data / "cache"

    @property
    def players(self) -> Path:
        return self.data / "players"

    @property
    def work(self) -> Path:
        return self.data / "work"


_lock = RLock()
_paths: XiuxianPaths | None = None


def _normalize_data_dir(value: str | os.PathLike[str] | None) -> Path:
    if value is None or not str(value).strip():
        value = os.environ.get(DATA_DIR_ENV)
    if value is None or not str(value).strip():
        return Path.cwd() / "data" / "xiuxian"
    return Path(value).expanduser().resolve(strict=False)


def configure_paths(data_dir: str | os.PathLike[str] | None = None) -> XiuxianPaths:
    """Configure runtime paths once before feature modules are imported."""
    global _paths
    resolved = XiuxianPaths(
        package_root=Path(__file__).resolve().parent,
        data=_normalize_data_dir(data_dir),
    )
    with _lock:
        _paths = resolved
    return resolved


def configure_paths_from_nonebot(config: Any) -> XiuxianPaths:
    value = getattr(config, "xiuxian_data_dir", None)
    return configure_paths(value)


def get_paths() -> XiuxianPaths:
    global _paths
    with _lock:
        if _paths is None:
            _paths = configure_paths()
        return _paths


def reset_paths_for_test() -> None:
    global _paths
    with _lock:
        _paths = None
