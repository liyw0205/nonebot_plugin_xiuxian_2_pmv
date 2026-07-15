from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend

SQLITE_MAX_INT = 2**63 - 1



@dataclass(frozen=True)
class ImpartClosingSettlementResult:
    status: str
    exp_gain: int = 0
    blessing_cost: int = 0
    exp_day_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


def _increment_stat(conn, user_id: str, key: str, amount: int) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS player_data.statistics(user_id TEXT PRIMARY KEY)")
    columns = {str(row[1]) for row in conn.execute("PRAGMA player_data.table_info(statistics)").fetchall()}
    if key not in columns:
        conn.execute(
            f"ALTER TABLE player_data.statistics ADD COLUMN {db_backend.quote_ident(key)} INTEGER DEFAULT 0"
        )
    field = db_backend.quote_ident(key)
    changed = conn.execute(
        f"UPDATE player_data.statistics SET {field}=COALESCE({field},0)+%s WHERE user_id=%s",
        (int(amount), user_id),
    )
    if changed.rowcount == 0:
        conn.execute(
            f"INSERT INTO player_data.statistics(user_id,{field}) VALUES(%s,%s)",
            (user_id, int(amount)),
        )


class ImpartClosingSettlementService:
    def __init__(self, game_db, impart_db, player_db, lock=None):
        self.game_db = Path(game_db)
        self.impart_db = Path(impart_db)
        self.player_db = Path(player_db)
        self.lock = lock or RLock()

    def get_result(self, operation_id: str) -> ImpartClosingSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS impart_closing_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            old = conn.execute(
                "SELECT payload,result_json FROM impart_closing_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if old is None:
                return None
            saved = json.loads(str(old[1]))
            return ImpartClosingSettlementResult("duplicate", *saved)

    def settle(
        self,
        operation_id,
        user_id,
        expected_create_time,
        expected_exp,
        expected_exp_day,
        exp_gain,
        blessing_cost,
        closing_minutes,
        hp,
        mp,
        atk,
        power,
    ) -> ImpartClosingSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_create_time = str(expected_create_time)
        values = tuple(
            int(value)
            for value in (
                expected_exp,
                expected_exp_day,
                exp_gain,
                blessing_cost,
                closing_minutes,
                hp,
                mp,
                atk,
                power,
            )
        )
        expected_exp, expected_exp_day, exp_gain, blessing_cost, closing_minutes, hp, mp, atk, power = values
        power = max(0, min(power, SQLITE_MAX_INT))
        expected_exp = max(0, min(expected_exp, SQLITE_MAX_INT))
        exp_gain = max(0, min(exp_gain, SQLITE_MAX_INT))
        hp = max(0, min(hp, SQLITE_MAX_INT))
        mp = max(0, min(mp, SQLITE_MAX_INT))
        atk = max(0, min(atk, SQLITE_MAX_INT))
        if not operation_id or min(values) < 0 or blessing_cost > expected_exp_day:
            raise ValueError("invalid impart closing settlement")
        # Request identity only — exp/hp rolls live in result_json; create_time identifies the closing session.
        payload = json.dumps(
            [user_id, expected_create_time], ensure_ascii=False, separators=(",", ":")
        )
        with self.lock, closing(db_backend.connect(self.game_db)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self.impart_db),))
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_db),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_closing_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                old = conn.execute(
                    "SELECT payload,result_json FROM impart_closing_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    if str(old[0]) != payload:
                        return ImpartClosingSettlementResult("operation_conflict")
                    saved = json.loads(str(old[1]))
                    return ImpartClosingSettlementResult("duplicate", *saved)

                user = conn.execute(
                    "SELECT COALESCE(exp,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                cd = conn.execute(
                    "SELECT type,create_time FROM user_cd WHERE user_id=%s", (user_id,)
                ).fetchone()
                impart = conn.execute(
                    "SELECT COALESCE(exp_day,0) FROM impart_data.xiuxian_impart WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None or cd is None or impart is None:
                    conn.rollback()
                    return ImpartClosingSettlementResult("user_missing")
                if (
                    int(user[0]) != expected_exp
                    or int(cd[0] or 0) != 4
                    or str(cd[1]) != expected_create_time
                    or int(impart[0]) != expected_exp_day
                ):
                    conn.rollback()
                    return ImpartClosingSettlementResult("state_changed")

                changed = conn.execute(
                    "UPDATE user_xiuxian SET exp=exp+%s,hp=%s,mp=%s,atk=%s,power=%s "
                    "WHERE user_id=%s AND exp=%s",
                    (exp_gain, hp, mp, atk, power, user_id, expected_exp),
                )
                cleared = conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL "
                    "WHERE user_id=%s AND type=4 AND CAST(create_time AS TEXT)=%s",
                    (user_id, expected_create_time),
                )
                blessed = conn.execute(
                    "UPDATE impart_data.xiuxian_impart SET exp_day=exp_day-%s "
                    "WHERE user_id=%s AND exp_day=%s AND exp_day>=%s",
                    (blessing_cost, user_id, expected_exp_day, blessing_cost),
                )
                if changed.rowcount != 1 or cleared.rowcount != 1 or blessed.rowcount != 1:
                    conn.rollback()
                    return ImpartClosingSettlementResult("state_changed")

                _increment_stat(conn, user_id, "虚神界闭关时长", closing_minutes)
                _increment_stat(conn, user_id, "虚神界闭关修为", exp_gain)
                _increment_stat(conn, user_id, "虚神界闭关祝福时长", blessing_cost)
                remaining = expected_exp_day - blessing_cost
                saved = [exp_gain, blessing_cost, remaining]
                conn.execute(
                    "INSERT INTO impart_closing_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, json.dumps(saved, separators=(",", ":"))),
                )
                conn.commit()
                return ImpartClosingSettlementResult("applied", *saved)
            except Exception:
                conn.rollback()
                raise


__all__ = ["ImpartClosingSettlementResult", "ImpartClosingSettlementService"]
