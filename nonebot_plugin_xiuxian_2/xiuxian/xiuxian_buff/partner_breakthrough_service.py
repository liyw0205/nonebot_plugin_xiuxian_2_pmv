from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class PartnerBreakthroughResult:
    status: str
    reward_exp: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class PartnerBreakthroughService:
    def __init__(self, game_database: str | Path, player_database: str | Path, lock: RLock | None = None):
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    def apply(self, operation_id, user_id, partner_id, new_level, *, expected_user_exp, expected_partner_exp, expected_affection, reward_exp, partner_power):
        operation_id, user_id, partner_id = str(operation_id).strip(), str(user_id), str(partner_id)
        expected_user_exp, expected_partner_exp = int(expected_user_exp), int(expected_partner_exp)
        expected_affection, reward_exp, partner_power = int(expected_affection), int(reward_exp), int(partner_power)
        if not operation_id or reward_exp <= 0:
            raise ValueError("invalid partner breakthrough reward")
        payload = json.dumps(
            [user_id, partner_id, str(new_level), expected_user_exp, expected_partner_exp, expected_affection, reward_exp, partner_power],
            separators=(",", ":"), ensure_ascii=False,
        )
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS partner_breakthrough_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reward_exp INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,reward_exp FROM partner_breakthrough_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    return PartnerBreakthroughResult("duplicate" if str(previous[0]) == payload else "operation_conflict", int(previous[1]))
                relation = conn.execute("SELECT partner_id,affection FROM player_data.partner WHERE user_id=%s", (user_id,)).fetchone()
                reciprocal = conn.execute("SELECT partner_id FROM player_data.partner WHERE user_id=%s", (partner_id,)).fetchone()
                if relation is None or reciprocal is None or str(relation[0]) != partner_id or str(reciprocal[0]) != user_id or int(relation[1] or 0) != expected_affection:
                    conn.rollback()
                    return PartnerBreakthroughResult("state_changed", reward_exp)
                users = conn.execute("SELECT user_id,exp FROM user_xiuxian WHERE user_id IN (%s,%s)", (user_id, partner_id)).fetchall()
                if {str(row[0]): int(row[1]) for row in users} != {user_id: expected_user_exp, partner_id: expected_partner_exp}:
                    conn.rollback()
                    return PartnerBreakthroughResult("state_changed", reward_exp)
                changed = conn.execute(
                    "UPDATE user_xiuxian SET exp=exp+%s,power=%s WHERE user_id=%s AND exp=%s",
                    (reward_exp, partner_power, partner_id, expected_partner_exp),
                )
                if changed.rowcount != 1:
                    conn.rollback()
                    return PartnerBreakthroughResult("state_changed", reward_exp)
                conn.execute("INSERT INTO partner_breakthrough_operations (operation_id,payload,reward_exp) VALUES (%s,%s,%s)", (operation_id, payload, reward_exp))
                conn.commit()
                return PartnerBreakthroughResult("applied", reward_exp)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


__all__ = ["PartnerBreakthroughResult", "PartnerBreakthroughService"]
