from __future__ import annotations

import sqlite3
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from threading import RLock

from ...infrastructure.clock import SystemClock


class ArenaOpponentRepository:
    """Read-only candidate projection and bounded process-local target cache."""

    def __init__(self, game_db, player_db, *, clock=None, ttl_seconds=180, capacity=2048):
        self.game_db, self.player_db = Path(game_db), Path(player_db)
        self.clock = clock if clock is not None else SystemClock()
        self.ttl_seconds, self.capacity = int(ttl_seconds), int(capacity)
        if self.ttl_seconds <= 0 or self.capacity <= 0:
            raise ValueError("opponent cache bounds must be positive")
        self._cache = OrderedDict()
        self._lock = RLock()

    def require_available(self):
        if not self.game_db.is_file() or not self.player_db.is_file():
            raise FileNotFoundError("arena opponent databases are unavailable")

    @contextmanager
    def _connection(self):
        self.require_available()
        connection = sqlite3.connect(f"{self.player_db.resolve().as_uri()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("ATTACH DATABASE ? AS profiles", (f"{self.game_db.resolve().as_uri()}?mode=ro",))
            connection.execute("PRAGMA query_only=ON")
            yield connection
        finally:
            connection.close()

    def candidates(self, user_id, *, target_ids=None):
        parameters = [str(user_id)]
        predicate = "a.user_id<>?"
        if target_ids is not None:
            target_ids = tuple(str(value) for value in target_ids)[:3]
            if not target_ids:
                return []
            predicate += " AND a.user_id IN (" + ",".join("?" for _ in target_ids) + ")"
            parameters.extend(target_ids)
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT CAST(a.user_id AS TEXT) AS user_id,a.score,p.user_name "
                "FROM arena AS a JOIN profiles.user_xiuxian AS p "
                "ON p.user_id=CAST(a.user_id AS TEXT) WHERE " + predicate
                + " ORDER BY CAST(a.user_id AS TEXT),p.rowid", parameters,
            )
            candidates = {}
            for row in rows:
                try:
                    score = int(row["score"])
                except (TypeError, ValueError, OverflowError):
                    continue
                candidates.setdefault(row["user_id"], {
                    "user_id": row["user_id"], "user_name": str(row["user_name"] or row["user_id"]),
                    "score": score,
                })
            return list(candidates.values())

    def set_cache(self, user_id, targets):
        user_id = str(user_id)
        normalized = []
        seen = {user_id}
        for target in targets[:3]:
            target_id = str(target["user_id"])
            if target_id and target_id not in seen:
                normalized.append({"user_id": target_id, "score": int(target["score"])})
                seen.add(target_id)
        with self._lock:
            now = self.clock.now().timestamp()
            for key in tuple(self._cache):
                if self._cache[key][0] <= now:
                    del self._cache[key]
            self._cache.pop(user_id, None)
            if normalized:
                self._cache[user_id] = (now + self.ttl_seconds, normalized)
            while len(self._cache) > self.capacity:
                self._cache.popitem(last=False)

    def get_cache(self, user_id):
        user_id = str(user_id)
        with self._lock:
            cached = self._cache.get(user_id)
            if cached is None:
                return None
            if cached[0] <= self.clock.now().timestamp():
                del self._cache[user_id]
                return None
            self._cache.move_to_end(user_id)
            return [dict(target) for target in cached[1]]

    def clear_cache(self, user_id):
        with self._lock:
            self._cache.pop(str(user_id), None)


__all__ = ["ArenaOpponentRepository"]
