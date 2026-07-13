from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ClosingSettlementResult:
    status: str
    exp_gain: int = 0
    stone_cost: int = 0
    hp: int = 0
    mp: int = 0
    atk: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ClosingSettlementService:
    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def settle(self, operation_id, user_id, expected_create_time, exp_gain, stone_cost, hp, mp, atk, power) -> ClosingSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_create_time = str(expected_create_time)
        values = tuple(int(value) for value in (exp_gain, stone_cost, hp, mp, atk, power))
        exp_gain, stone_cost, hp, mp, atk, power = values
        if not operation_id or min(exp_gain, stone_cost, hp, mp, atk, power) < 0:
            raise ValueError("valid operation and non-negative settlement values are required")
        payload = json.dumps([user_id, expected_create_time, *values], separators=(",", ":"))
        result = ClosingSettlementResult("", exp_gain, stone_cost, hp, mp, atk)
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS closing_settlement_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
                previous = conn.execute("SELECT payload,result_json FROM closing_settlement_operations WHERE operation_id=%s", (operation_id,)).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ClosingSettlementResult("state_changed")
                    saved = json.loads(str(previous[1]))
                    return ClosingSettlementResult("duplicate", *saved)
                user = conn.execute("SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                cd = conn.execute("SELECT type,create_time FROM user_cd WHERE user_id=%s", (user_id,)).fetchone()
                if user is None or cd is None:
                    conn.rollback()
                    return ClosingSettlementResult("user_missing")
                if int(cd[0] or 0) != 1 or str(cd[1]) != expected_create_time:
                    conn.rollback()
                    return ClosingSettlementResult("state_changed")
                if int(user[0]) < stone_cost:
                    conn.rollback()
                    return ClosingSettlementResult("stone_insufficient")
                changed = conn.execute("UPDATE user_xiuxian SET exp=COALESCE(exp,0)+%s,stone=stone-%s,hp=%s,mp=%s,atk=%s,power=%s WHERE user_id=%s AND stone>=%s", (exp_gain, stone_cost, hp, mp, atk, power, user_id, stone_cost))
                cleared = conn.execute("UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s AND type=1 AND CAST(create_time AS TEXT)=%s", (user_id, expected_create_time))
                if changed.rowcount != 1 or cleared.rowcount != 1:
                    conn.rollback()
                    return ClosingSettlementResult("state_changed")
                saved = [exp_gain, stone_cost, hp, mp, atk]
                conn.execute("INSERT INTO closing_settlement_operations (operation_id,payload,result_json) VALUES (%s,%s,%s)", (operation_id, payload, json.dumps(saved)))
                conn.commit()
                return ClosingSettlementResult("applied", *saved)
            except Exception:
                conn.rollback()
                raise


__all__ = ["ClosingSettlementResult", "ClosingSettlementService"]
