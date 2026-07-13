from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .relation_transaction_utils import increment_stat, set_field


@dataclass(frozen=True)
class PartnerCultivationResult:
    status: str
    exp_1: int
    exp_2: int
    used_count: int
    affection_1: int
    affection_2: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PartnerCultivationService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(
        self, operation_id, user_id_1, user_id_2, *, expected_exp_1, expected_exp_2,
        exp_1, exp_2, used_count, power_1, power_2, hp_1, mp_1, atk_1, hp_2, mp_2, atk_2,
        level_rate_1=0, level_rate_2=0, expected_affection_1=None, expected_affection_2=None,
        affection_1=0, affection_2=0,
    ) -> PartnerCultivationResult:
        operation_id = str(operation_id).strip()
        user_id_1, user_id_2 = str(user_id_1), str(user_id_2)
        values = tuple(int(value) for value in (
            expected_exp_1, expected_exp_2, exp_1, exp_2, used_count, power_1, power_2,
            hp_1, mp_1, atk_1, hp_2, mp_2, atk_2, level_rate_1, level_rate_2,
            affection_1, affection_2,
        ))
        if not operation_id or values[4] <= 0 or values[2] < 0 or values[3] < 0:
            raise ValueError("invalid partner cultivation operation")
        expected_affection_1 = None if expected_affection_1 is None else int(expected_affection_1)
        expected_affection_2 = None if expected_affection_2 is None else int(expected_affection_2)
        payload = json.dumps(
            [user_id_1, user_id_2, *values, expected_affection_1, expected_affection_2],
            separators=(",", ":"), ensure_ascii=True,
        )

        def result(status):
            return PartnerCultivationResult(status, values[2], values[3], values[4], values[15], values[16])

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_cultivation_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,exp_1 INTEGER NOT NULL,"
                    "exp_2 INTEGER NOT NULL,used_count INTEGER NOT NULL,affection_1 INTEGER NOT NULL,"
                    "affection_2 INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,exp_1,exp_2,used_count,affection_1,affection_2 "
                    "FROM partner_cultivation_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return result("operation_conflict")
                    return PartnerCultivationResult("duplicate", *(int(value) for value in previous[1:]))

                rows = conn.execute(
                    "SELECT user_id,exp FROM user_xiuxian WHERE user_id IN (%s,%s)",
                    (user_id_1, user_id_2),
                ).fetchall()
                current = {str(row[0]): int(row[1]) for row in rows}
                if current != {user_id_1: values[0], user_id_2: values[1]}:
                    conn.rollback()
                    return result("state_changed")

                if expected_affection_1 is not None:
                    for user_id, partner_id, expected in (
                        (user_id_1, user_id_2, expected_affection_1),
                        (user_id_2, user_id_1, expected_affection_2),
                    ):
                        row = conn.execute(
                            "SELECT partner_id,affection FROM player_data.partner WHERE user_id=%s", (user_id,),
                        ).fetchone()
                        if row is None or str(row[0]) != partner_id or int(row[1] or 0) != expected:
                            conn.rollback()
                            return result("state_changed")

                for user_id, expected_exp, gain, power, hp, mp, atk, rate in (
                    (user_id_1, values[0], values[2], values[5], values[7], values[8], values[9], values[13]),
                    (user_id_2, values[1], values[3], values[6], values[10], values[11], values[12], values[14]),
                ):
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET exp=exp+%s,power=%s,hp=%s,mp=%s,atk=%s,"
                        "level_up_rate=COALESCE(level_up_rate,0)+%s WHERE user_id=%s AND exp=%s",
                        (gain, power, hp, mp, atk, rate, user_id, expected_exp),
                    )
                    if changed.rowcount != 1:
                        conn.rollback()
                        return result("state_changed")
                    increment_stat(conn, user_id, "双修次数", values[4])

                if expected_affection_1 is not None:
                    set_field(conn, "partner", user_id_1, "affection", expected_affection_1 + values[15], "INTEGER")
                    set_field(conn, "partner", user_id_2, "affection", expected_affection_2 + values[16], "INTEGER")

                conn.execute(
                    "INSERT INTO partner_cultivation_operations "
                    "(operation_id,payload,exp_1,exp_2,used_count,affection_1,affection_2) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (operation_id, payload, values[2], values[3], values[4], values[15], values[16]),
                )
                conn.commit()
                return result("applied")
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


__all__ = ["PartnerCultivationResult", "PartnerCultivationService"]
