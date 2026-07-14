from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_buff.relation_transaction_utils import increment_stat
from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class OrdinaryTribulationResult:
    status: str
    successful: bool = False
    rate: int = 0
    item_used: bool = False
    user_id: str = ""
    target_level: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class OrdinaryTribulationService:
    """Commit one resolved ordinary tribulation without partial state."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _saved_result(payload, successful, rate, item_used, *, status="duplicate"):
        data = json.loads(str(payload))
        return OrdinaryTribulationResult(
            status,
            bool(successful),
            int(rate),
            bool(item_used),
            str(data[0]),
            str(data[4]),
        )

    def replay(self, operation_id, user_id):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("ordinary_tribulation_operations"):
                return None
            previous = conn.execute(
                "SELECT payload,successful,rate,item_used "
                "FROM ordinary_tribulation_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            saved = self._saved_result(*previous)
            if saved.user_id != user_id:
                return OrdinaryTribulationResult("operation_conflict")
            return saved

    def settle(
        self, operation_id, user_id, *, expected_level, expected_exp,
        expected_rate, target_level, successful, new_rate, occurred_at,
        power=0, consume_destiny_pill=False,
    ) -> OrdinaryTribulationResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, target_level = str(expected_level), str(target_level)
        expected_exp, expected_rate, new_rate, power = map(
            int, (expected_exp, expected_rate, new_rate, power)
        )
        successful, consume_destiny_pill = bool(successful), bool(consume_destiny_pill)
        occurred_at = str(occurred_at)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = json.dumps(
            [user_id, expected_level, expected_exp, expected_rate, target_level,
             successful, new_rate, occurred_at, power, consume_destiny_pill],
            ensure_ascii=True, separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS ordinary_tribulation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,successful INTEGER NOT NULL,"
                    "rate INTEGER NOT NULL,item_used INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,successful,rate,item_used FROM ordinary_tribulation_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous:
                    conn.rollback()
                    saved = self._saved_result(*previous)
                    if saved.user_id != user_id:
                        return OrdinaryTribulationResult("operation_conflict", False, 0, False)
                    return saved
                user = conn.execute(
                    "SELECT level,exp FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                state = conn.execute(
                    "SELECT current_rate FROM user_tribulation WHERE user_id=%s", (user_id,)
                ).fetchone()
                actual_rate = int(state[0]) if state else 30
                if user is None or str(user[0]) != expected_level or int(user[1] or 0) != expected_exp or actual_rate != expected_rate:
                    conn.rollback()
                    return OrdinaryTribulationResult(
                        "state_changed", False, actual_rate, False, user_id, target_level
                    )
                if consume_destiny_pill:
                    changed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-1,bind_num=MIN(COALESCE(bind_num,0),goods_num-1),"
                        "day_num=COALESCE(day_num,0)+1,all_num=COALESCE(all_num,0)+1,update_time=%s,action_time=%s "
                        "WHERE user_id=%s AND goods_id=1996 AND goods_num>0",
                        (occurred_at, occurred_at, user_id),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return OrdinaryTribulationResult(
                            "item_missing", False, actual_rate, False, user_id, target_level
                        )
                if successful:
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET level=%s,power=%s WHERE user_id=%s AND level=%s AND exp=%s",
                        (target_level, power, user_id, expected_level, expected_exp),
                    )
                    conn.execute("DELETE FROM user_tribulation WHERE user_id=%s", (user_id,))
                else:
                    changed = conn.execute(
                        "UPDATE user_tribulation SET current_rate=%s,last_time=%s WHERE user_id=%s AND current_rate=%s",
                        (new_rate, occurred_at, user_id, expected_rate),
                    )
                if changed.rowcount != 1:
                    conn.rollback()
                    return OrdinaryTribulationResult(
                        "state_changed", False, actual_rate, False, user_id, target_level
                    )
                increment_stat(conn, user_id, "渡劫次数", 1)
                increment_stat(conn, user_id, "渡劫成功" if successful else "渡劫失败", 1)
                if consume_destiny_pill:
                    increment_stat(conn, user_id, "天命丹消耗", 1)
                conn.execute(
                    "INSERT INTO ordinary_tribulation_operations(operation_id,payload,successful,rate,item_used) VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, int(successful), new_rate, int(consume_destiny_pill)),
                )
                conn.commit()
                return OrdinaryTribulationResult(
                    "applied",
                    successful,
                    new_rate,
                    consume_destiny_pill,
                    user_id,
                    target_level,
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


__all__ = ["OrdinaryTribulationResult", "OrdinaryTribulationService"]
