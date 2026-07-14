from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock

from ..xiuxian_buff.relation_transaction_utils import increment_stat
from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class HeartDevilTribulationResult:
    status: str
    successful: bool = False
    rate: int = 0
    heart_devil_count: int = 0
    item_used: bool = False
    user_id: str = ""
    devil_name: str = ""
    message: str = ""
    battle_messages: list = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class HeartDevilTribulationService:
    """Commit one already-resolved heart-devil encounter atomically."""

    def __init__(self, game_database, player_database, lock=None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _saved_result(
        payload, successful, rate, heart_devil_count, item_used, *, status="duplicate"
    ):
        data = json.loads(str(payload))
        return HeartDevilTribulationResult(
            status=status,
            successful=bool(successful),
            rate=int(rate),
            heart_devil_count=int(heart_devil_count),
            item_used=bool(item_used),
            user_id=str(data[0]),
            devil_name=str(data[6] or ""),
            message=str(data[8] or "") if len(data) > 8 else "",
            battle_messages=list(data[9] or []) if len(data) > 9 else [],
        )

    def replay(self, operation_id, user_id):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        with closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("heart_devil_tribulation_operations"):
                return None
            previous = conn.execute(
                "SELECT payload,successful,rate,heart_devil_count,item_used "
                "FROM heart_devil_tribulation_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            saved = self._saved_result(*previous)
            if saved.user_id != user_id:
                return HeartDevilTribulationResult("operation_conflict")
            return saved

    def settle(
        self, operation_id, user_id, *, expected_rate, expected_count,
        successful, new_rate, occurred_at, devil_name="", consume_destiny_pill=False,
        message="", battle_messages=None,
    ) -> HeartDevilTribulationResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_rate, expected_count, new_rate = map(int, (expected_rate, expected_count, new_rate))
        successful, consume_destiny_pill = bool(successful), bool(consume_destiny_pill)
        occurred_at, devil_name, message = (
            str(occurred_at), str(devil_name), str(message)
        )
        battle_messages = list(battle_messages or [])
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        payload = json.dumps(
            [
                user_id, expected_rate, expected_count, successful, new_rate,
                occurred_at, devil_name, consume_destiny_pill, message,
                battle_messages,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),)); attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS heart_devil_tribulation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,successful INTEGER NOT NULL,rate INTEGER NOT NULL,"
                    "heart_devil_count INTEGER NOT NULL,item_used INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,successful,rate,heart_devil_count,item_used FROM heart_devil_tribulation_operations WHERE operation_id=%s", (operation_id,)
                ).fetchone()
                if previous:
                    conn.rollback()
                    saved = self._saved_result(*previous)
                    if saved.user_id != user_id:
                        return HeartDevilTribulationResult("operation_conflict", False, 0, 0, False)
                    return saved
                state = conn.execute("SELECT current_rate,heart_devil_count FROM user_tribulation WHERE user_id=%s", (user_id,)).fetchone()
                actual_rate, actual_count = (int(state[0]), int(state[1])) if state else (30, 0)
                if actual_rate != expected_rate or actual_count != expected_count:
                    conn.rollback(); return HeartDevilTribulationResult("state_changed", False, actual_rate, actual_count, False)
                if consume_destiny_pill:
                    consumed = conn.execute(
                        "UPDATE back SET goods_num=goods_num-1,bind_num=MIN(COALESCE(bind_num,0),goods_num-1),"
                        "day_num=COALESCE(day_num,0)+1,all_num=COALESCE(all_num,0)+1,update_time=%s,action_time=%s "
                        "WHERE user_id=%s AND goods_id=1996 AND goods_num>0", (occurred_at, occurred_at, user_id),
                    )
                    if consumed.rowcount != 1:
                        conn.rollback(); return HeartDevilTribulationResult("item_missing", False, actual_rate, actual_count, False)
                new_count = expected_count + 1
                changed = conn.execute(
                    "UPDATE user_tribulation SET current_rate=%s,heart_devil_count=%s,last_time=%s "
                    "WHERE user_id=%s AND current_rate=%s AND heart_devil_count=%s",
                    (new_rate, new_count, occurred_at, user_id, expected_rate, expected_count),
                )
                if changed.rowcount == 0 and state is None:
                    conn.execute(
                        "INSERT INTO user_tribulation(user_id,current_rate,heart_devil_count,last_time) VALUES(%s,%s,%s,%s)",
                        (user_id, new_rate, new_count, occurred_at),
                    )
                elif changed.rowcount != 1:
                    conn.rollback(); return HeartDevilTribulationResult("state_changed", False, actual_rate, actual_count, False)
                increment_stat(conn, user_id, "心魔劫次数", 1)
                increment_stat(conn, user_id, "心魔劫成功" if successful else "心魔劫失败", 1)
                if consume_destiny_pill:
                    increment_stat(conn, user_id, "天命丹消耗", 1)
                conn.execute(
                    "INSERT INTO heart_devil_tribulation_operations(operation_id,payload,successful,rate,heart_devil_count,item_used) VALUES(%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, int(successful), new_rate, new_count, int(consume_destiny_pill)),
                )
                conn.commit()
                return HeartDevilTribulationResult(
                    "applied", successful, new_rate, new_count,
                    consume_destiny_pill, user_id, devil_name, message,
                    battle_messages,
                )
            except Exception:
                conn.rollback(); raise
            finally:
                if attached:
                    try: conn.execute("DETACH DATABASE player_data")
                    except Exception: pass


__all__ = ["HeartDevilTribulationResult", "HeartDevilTribulationService"]
