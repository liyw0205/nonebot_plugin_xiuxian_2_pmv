"""Bounded process-wide cache for immutable shared read models."""

from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from threading import Event, RLock
import sys
import time
from typing import Any, Callable, Hashable


@dataclass(frozen=True)
class _Entry:
    database_path: str
    database_paths: tuple[str, ...]
    generations: tuple[tuple[str, int], ...]
    snapshot: bool
    expires_at: float
    size_bytes: int
    value: Any


def shared_snapshot_key(database_path: str | Path, view: str, *parts: Hashable) -> tuple:
    path = Path(database_path).expanduser().resolve()
    try:
        stat = path.stat()
        identity = (stat.st_dev, stat.st_ino)
    except OSError:
        identity = (0, 0)
    return (str(path), *identity, "shared_snapshot", str(view), *parts)


class ProcessReadCache:
    def __init__(
        self,
        *,
        max_entries: int = 32,
        max_bytes: int = 1024 * 1024,
        max_inflight: int | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        if max_entries < 1 or max_bytes < 1 or (max_inflight is not None and max_inflight < 1):
            raise ValueError("cache limits must be positive")
        self._max_entries = max_entries
        self._max_bytes = max_bytes
        self._max_inflight = max_entries if max_inflight is None else max_inflight
        self._clock = clock
        self._lock = RLock()
        self._entries: OrderedDict[Hashable, _Entry] = OrderedDict()
        self._entries_bytes = 0
        self._inflight: dict[
            Hashable,
            tuple[str, tuple[str, ...], tuple[tuple[str, int], ...], bool, Event],
        ] = {}
        self._generations: dict[str, int] = {}
        self._snapshot_generations: dict[str, int] = {}
        self._stats = {
            name: 0
            for name in (
                "hits",
                "misses",
                "waits",
                "expirations",
                "evictions",
                "invalidations",
                "oversized",
                "bypasses",
                "partial_hits",
            )
        }

    @staticmethod
    def _database_path(database_path: str | Path) -> str:
        return str(Path(database_path).expanduser().resolve())

    def _estimate_size(self, value: Any) -> int:
        pending = [value]
        seen = set()
        size_bytes = 128
        while pending:
            item = pending.pop()
            identity = id(item)
            if identity in seen:
                continue
            seen.add(identity)
            size_bytes += sys.getsizeof(item)
            if size_bytes > self._max_bytes:
                return size_bytes
            if isinstance(item, dict):
                pending.extend(item.keys())
                pending.extend(item.values())
            elif isinstance(item, (list, tuple, set, frozenset)):
                pending.extend(item)
        return size_bytes

    def _remove_entry(self, key: Hashable) -> _Entry | None:
        entry = self._entries.pop(key, None)
        if entry is not None:
            self._entries_bytes -= entry.size_bytes
        return entry

    def _cleanup_generation(self, database_path: str, snapshot: bool) -> None:
        generations = self._snapshot_generations if snapshot else self._generations
        if not any(
            database_path in entry.database_paths and entry.snapshot == snapshot
            for entry in self._entries.values()
        ) and not any(
            database_path in paths and is_snapshot == snapshot
            for _, paths, _, is_snapshot, _ in self._inflight.values()
        ):
            generations.pop(database_path, None)

    def _generation_token(
        self, database_paths: tuple[str, ...], snapshot: bool
    ) -> tuple[tuple[str, int], ...]:
        generations = self._snapshot_generations if snapshot else self._generations
        return tuple((path, generations.get(path, 0)) for path in database_paths)

    def get_or_load(
        self,
        key: Hashable,
        *,
        database_path: str | Path,
        ttl: float,
        loader: Callable[[], Any],
        snapshot: bool = False,
        dependency_databases: tuple[str | Path, ...] = (),
    ) -> Any:
        path = self._database_path(database_path)
        if ttl <= 0:
            return loader()

        database_paths = tuple(
            dict.fromkeys(
                (path, *(self._database_path(item) for item in dependency_databases))
            )
        )
        cache_key = (bool(snapshot), path, database_paths[1:], key)

        while True:
            with self._lock:
                now = self._clock()
                expired = [entry_key for entry_key, entry in self._entries.items() if entry.expires_at <= now]
                expired_paths = set()
                for entry_key in expired:
                    entry = self._remove_entry(entry_key)
                    if entry is not None:
                        expired_paths.update(
                            (database, entry.snapshot) for database in entry.database_paths
                        )
                    self._stats["expirations"] += 1
                for expired_path, expired_snapshot in expired_paths:
                    self._cleanup_generation(expired_path, expired_snapshot)

                generation = self._generation_token(database_paths, snapshot)
                cached = self._entries.get(cache_key)
                if (
                    cached is not None
                    and cached.database_path == path
                    and cached.database_paths == database_paths
                    and cached.generations == generation
                    and cached.snapshot == snapshot
                ):
                    self._entries.move_to_end(cache_key)
                    self._stats["hits"] += 1
                    return deepcopy(cached.value)
                if cached is not None:
                    self._remove_entry(cache_key)

                pending = self._inflight.get(cache_key)
                if pending is not None and pending[:4] == (
                    path,
                    database_paths,
                    generation,
                    snapshot,
                ):
                    event = pending[4]
                    self._stats["waits"] += 1
                    owner = False
                elif pending is not None:
                    event = pending[4]
                    self._stats["waits"] += 1
                    owner = False
                elif len(self._inflight) >= self._max_inflight:
                    event = next(iter(self._inflight.values()))[4]
                    self._stats["waits"] += 1
                    owner = False
                else:
                    event = Event()
                    self._inflight[cache_key] = (
                        path,
                        database_paths,
                        generation,
                        snapshot,
                        event,
                    )
                    self._stats["misses"] += 1
                    owner = True

            if not owner:
                event.wait()
                continue

            try:
                value = loader()
            except BaseException:
                with self._lock:
                    pending = self._inflight.get(cache_key)
                    if pending is not None and pending[4] is event:
                        self._inflight.pop(cache_key, None)
                        event.set()
                        for database in database_paths:
                            self._cleanup_generation(database, snapshot)
                raise

            try:
                size_bytes = self._estimate_size((cache_key, value))
            except Exception:
                size_bytes = self._max_bytes + 1
            retry_after_invalidation = False
            with self._lock:
                pending = self._inflight.get(cache_key)
                current_generation = self._generation_token(database_paths, snapshot)
                if current_generation == generation:
                    if size_bytes <= self._max_bytes:
                        self._remove_entry(cache_key)
                        self._entries[cache_key] = _Entry(
                            path,
                            database_paths,
                            generation,
                            snapshot,
                            self._clock() + ttl,
                            size_bytes,
                            deepcopy(value),
                        )
                        self._entries_bytes += size_bytes
                        self._entries.move_to_end(cache_key)
                        while (
                            len(self._entries) > self._max_entries
                            or self._entries_bytes > self._max_bytes
                        ):
                            evicted_key, evicted = self._entries.popitem(last=False)
                            self._entries_bytes -= evicted.size_bytes
                            self._stats["evictions"] += 1
                            for database in evicted.database_paths:
                                self._cleanup_generation(database, evicted.snapshot)
                    else:
                        self._stats["oversized"] += 1
                if pending is not None and pending[4] is event:
                    self._inflight.pop(cache_key, None)
                    event.set()
                if current_generation != generation:
                    event.set()
                    retry_after_invalidation = True
                for database in database_paths:
                    self._cleanup_generation(database, snapshot)
            if retry_after_invalidation:
                continue
            return deepcopy(value)

    def invalidate_database(self, database_path: str | Path) -> None:
        path = self._database_path(database_path)
        with self._lock:
            has_inflight = any(
                path in paths and not snapshot
                for _, paths, _, snapshot, _ in self._inflight.values()
            )
            for key, entry in list(self._entries.items()):
                if path in entry.database_paths and not entry.snapshot:
                    self._remove_entry(key)
            if has_inflight:
                self._generations[path] = self._generations.get(path, 0) + 1
            else:
                self._generations.pop(path, None)
            self._cleanup_generation(path, False)
            self._stats["invalidations"] += 1

    def get_snapshot_ceiling(
        self, prefix: tuple, value: bytes, *, database_path: str | Path,
        dependency_databases: tuple[str | Path, ...] = (),
    ) -> tuple[bytes, Any] | None:
        """Reuse a completed ordered snapshot within the existing cache bounds."""
        path = self._database_path(database_path)
        paths = tuple(dict.fromkeys((path, *(self._database_path(item) for item in dependency_databases))))
        with self._lock:
            now = self._clock()
            selected = None
            for cache_key, entry in self._entries.items():
                key = cache_key[-1]
                if (not entry.snapshot or entry.database_paths != paths
                        or entry.expires_at <= now
                        or entry.generations != self._generation_token(entry.database_paths, True)
                        or not isinstance(key, tuple) or len(key) != len(prefix) + 1
                        or key[:-1] != prefix or not isinstance(key[-1], bytes)
                        or key[-1] < value):
                    continue
                if selected is None or key[-1] < selected[0]:
                    selected = (key[-1], cache_key, entry)
            if selected is None:
                return None
            ceiling, cache_key, entry = selected
            self._entries.move_to_end(cache_key)
            self._stats["partial_hits"] += 1
            return ceiling, deepcopy(entry.value)

    def invalidate_snapshot_database(self, database_path: str | Path) -> None:
        """Explicitly invalidate shared snapshots after data replacement or schema changes."""
        path = self._database_path(database_path)
        with self._lock:
            has_inflight = any(
                path in paths and snapshot
                for _, paths, _, snapshot, _ in self._inflight.values()
            )
            for key, entry in list(self._entries.items()):
                if path in entry.database_paths and entry.snapshot:
                    self._remove_entry(key)
            if has_inflight:
                self._snapshot_generations[path] = self._snapshot_generations.get(path, 0) + 1
            else:
                self._snapshot_generations.pop(path, None)
            self._cleanup_generation(path, True)
            self._stats["invalidations"] += 1

    def database_generation(self, database_path: str | Path, *, snapshot: bool = False) -> int:
        path = self._database_path(database_path)
        with self._lock:
            generations = self._snapshot_generations if snapshot else self._generations
            return generations.get(path, 0)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                **self._stats,
                "entries": len(self._entries),
                "inflight": len(self._inflight),
                "estimated_bytes": self._entries_bytes,
                "max_entries": self._max_entries,
                "max_bytes": self._max_bytes,
                "max_inflight": self._max_inflight,
                "generation_paths": len(self._generations) + len(self._snapshot_generations),
            }

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._entries_bytes = 0
            for _, paths, _, snapshot, _ in self._inflight.values():
                generations = self._snapshot_generations if snapshot else self._generations
                for path in paths:
                    generations[path] = generations.get(path, 0) + 1
            for path in tuple(self._generations):
                self._cleanup_generation(path, False)
            for path in tuple(self._snapshot_generations):
                self._cleanup_generation(path, True)
            for name in self._stats:
                self._stats[name] = 0


process_read_cache = ProcessReadCache(max_inflight=8)
