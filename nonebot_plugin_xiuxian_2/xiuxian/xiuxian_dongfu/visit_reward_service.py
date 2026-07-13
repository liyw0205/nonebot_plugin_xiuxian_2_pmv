from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class DongfuVisitRewardResult:
    status: str

    @property
    def succeeded(self) -> bool:
        return self.status in {"rewarded", "duplicate"}


class DongfuVisitRewardService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database, self._player_database, self._lock = Path(game_database), Path(player_database), lock or RLock()

    def reward(self, operation_id, visitor_id, target_id, gain):
        operation_id, visitor_id, target_id, gain = str(operation_id).strip(), str(visitor_id), str(target_id), int(gain)
        if not operation_id or not visitor_id or not target_id or visitor_id == target_id or gain < 0:
            raise ValueError("valid visit reward is required")
        payload = "|".join((visitor_id, target_id, str(gain)))
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True; conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS dongfu_visit_reward_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                old = conn.execute("SELECT payload FROM dongfu_visit_reward_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if old is not None:
                    conn.rollback(); return DongfuVisitRewardResult("duplicate" if str(old[0]) == payload else "state_changed")
                if conn.execute("SELECT 1 FROM user_xiuxian WHERE user_id=%s", (visitor_id,)).fetchone() is None:
                    conn.rollback(); return DongfuVisitRewardResult("user_missing")
                visitor = conn.execute('SELECT built FROM player_data."dongfu_status" WHERE user_id=%s', (visitor_id,)).fetchone()
                target = conn.execute('SELECT built FROM player_data."dongfu_status" WHERE user_id=%s', (target_id,)).fetchone()
                if visitor is None or int(visitor[0] or 0) != 1 or target is None or int(target[0] or 0) != 1:
                    conn.rollback(); return DongfuVisitRewardResult("dongfu_changed")
                updated = conn.execute("UPDATE user_xiuxian SET stone=stone+%s WHERE user_id=%s", (gain, visitor_id))
                if updated.rowcount != 1:
                    conn.rollback(); return DongfuVisitRewardResult("state_changed")
                conn.execute("INSERT INTO dongfu_visit_reward_operations (operation_id,payload) VALUES (%s,%s)", (operation_id, payload)); conn.commit(); return DongfuVisitRewardResult("rewarded")
            except Exception:
                conn.rollback(); raise
            finally:
                if attached: conn.execute("DETACH DATABASE player_data")


__all__ = ["DongfuVisitRewardResult", "DongfuVisitRewardService"]
