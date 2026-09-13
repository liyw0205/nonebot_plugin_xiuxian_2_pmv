from __future__ import annotations

import warnings
from collections import Counter
import json
from threading import Lock

from ..features.daily_fortune.commands import handle_daily_fortune


_hits = Counter()
_lock = Lock()


def record_compatibility_hit(name: str) -> int:
    with _lock:
        key = str(name)
        _hits[key] += 1
        current = _read_persisted()
        current[key] = int(current.get(key, 0)) + 1
        _write_persisted(current)
        return _hits[key]


def compatibility_hits() -> dict[str, int]:
    with _lock:
        persisted = _read_persisted()
        merged = {str(key): int(value) for key, value in persisted.items()}
        for key, value in _hits.items():
            # The current process counter is already included in the
            # persisted snapshot when writes succeed; retain it on I/O
            # failure without double-counting successful writes.
            merged[key] = max(merged.get(key, 0), int(value))
        return merged


def _hit_path():
    try:
        from ..paths import get_paths

        return get_paths().data / "compatibility_hits.json"
    except Exception:
        return None


def _read_persisted() -> dict[str, int]:
    path = _hit_path()
    if path is None or not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _write_persisted(value: dict[str, int]) -> None:
    path = _hit_path()
    if path is None:
        return
    try:
        from ..infrastructure.filesystem import atomic_write

        atomic_write(path, json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8"))
    except (OSError, TypeError, ValueError):
        # Compatibility telemetry is best effort and must never break a
        # player command during a read-only or damaged data directory.
        return


def forward_daily_fortune(*args, **kwargs):
    """Compatibility alias; behavior is implemented by the new application."""
    warnings.warn(
        "forward_daily_fortune is a compatibility shim and will be removed after the migration cycle",
        DeprecationWarning,
        stacklevel=2,
    )
    record_compatibility_hit("daily_fortune")
    return handle_daily_fortune(*args, **kwargs)


__all__ = ["compatibility_hits", "forward_daily_fortune", "record_compatibility_hit"]
