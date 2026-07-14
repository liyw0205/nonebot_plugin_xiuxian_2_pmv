from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock

from ..xiuxian_buff.relation_transaction_utils import increment_stat
from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class StoneRobberyResult:
    status: str
    robber_id: str = ""
    victim_id: str = ""
    winner_id: str = ""
    battle_messages: list = field(default_factory=list)
    robber_hp: int = 0
    robber_mp: int = 0
    victim_hp: int = 0
    victim_mp: int = 0
    requested_amount: int = 0
    transferred_amount: int = 0
    loser_balance: int = 0
    stamina_cost: int = 0
    robber_stamina: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class StoneRobberySettlementService:
    """Commit robbery combat, stamina, stones and statistics atomically."""

    _SNAPSHOT_FIELDS = ("hp", "mp", "user_stamina", "exp", "stone")

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._player_database = Path(player_database)
        self._lock = lock or RLock()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stone_robbery_operations("
            "operation_id TEXT PRIMARY KEY,robber_id TEXT NOT NULL,victim_id TEXT NOT NULL,"
            "result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

    @classmethod
    def _snapshot(cls, value) -> tuple[int | None, ...]:
        data = dict(value)
        return tuple(
            None if field in {"hp", "mp"} and data[field] is None
            else int(data[field] or 0)
            for field in cls._SNAPSHOT_FIELDS
        )

    @staticmethod
    def _saved_result(value, status="duplicate") -> StoneRobberyResult:
        return StoneRobberyResult(status=status, **json.loads(str(value)))

    def replay(self, operation_id, robber_id, victim_id):
        operation_id = str(operation_id).strip()
        robber_id, victim_id = str(robber_id), str(victim_id)
        if not operation_id:
            raise ValueError("valid robbery operation is required")
        with closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("stone_robbery_operations"):
                return None
            previous = conn.execute(
                "SELECT robber_id,victim_id,result_json FROM stone_robbery_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            if str(previous[0]) != robber_id or str(previous[1]) != victim_id:
                return StoneRobberyResult("operation_conflict")
            return self._saved_result(previous[2])

    def settle(
        self,
        operation_id,
        robber_id,
        victim_id,
        *,
        expected_robber,
        expected_victim,
        robber_final,
        victim_final,
        winner_id,
        battle_messages,
        stamina_cost=15,
    ) -> StoneRobberyResult:
        operation_id = str(operation_id).strip()
        robber_id, victim_id = str(robber_id), str(victim_id)
        winner_id = str(winner_id)
        robber_snapshot = self._snapshot(expected_robber)
        victim_snapshot = self._snapshot(expected_victim)
        robber_final = (max(1, int(robber_final[0])), max(0, int(robber_final[1])))
        victim_final = (max(1, int(victim_final[0])), max(0, int(victim_final[1])))
        battle_messages = list(battle_messages or [])
        stamina_cost = int(stamina_cost)
        if (
            not operation_id
            or robber_id == victim_id
            or winner_id not in {robber_id, victim_id}
            or stamina_cost < 0
        ):
            raise ValueError("valid robbery participants, winner and stamina are required")

        def current_result(status):
            return StoneRobberyResult(
                status=status,
                robber_id=robber_id,
                victim_id=victim_id,
                winner_id=winner_id,
            )

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS player_data", (str(self._player_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                previous = conn.execute(
                    "SELECT robber_id,victim_id,result_json FROM stone_robbery_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != robber_id or str(previous[1]) != victim_id:
                        return current_result("operation_conflict")
                    return self._saved_result(previous[2])

                rows = conn.execute(
                    "SELECT user_id,hp,mp,"
                    "COALESCE(user_stamina,0),COALESCE(exp,0),COALESCE(stone,0) "
                    "FROM user_xiuxian WHERE user_id IN (%s,%s)",
                    (robber_id, victim_id),
                ).fetchall()
                users = {
                    str(row[0]): tuple(
                        None if index < 2 and item is None else int(item or 0)
                        for index, item in enumerate(row[1:])
                    )
                    for row in rows
                }
                if robber_id not in users or victim_id not in users:
                    conn.rollback()
                    return current_result("user_missing")
                if users[robber_id] != robber_snapshot or users[victim_id] != victim_snapshot:
                    conn.rollback()
                    return current_result("state_changed")
                robber_hp = (
                    robber_snapshot[3] // 2
                    if robber_snapshot[0] is None else robber_snapshot[0]
                )
                victim_hp = (
                    victim_snapshot[3] // 2
                    if victim_snapshot[0] is None else victim_snapshot[0]
                )
                if robber_hp <= robber_snapshot[3] / 10:
                    conn.rollback()
                    return current_result("robber_injured")
                if victim_hp <= victim_snapshot[3] / 10:
                    conn.rollback()
                    return current_result("victim_injured")
                if robber_snapshot[2] < stamina_cost:
                    conn.rollback()
                    return current_result("stamina_insufficient")

                loser_id = victim_id if winner_id == robber_id else robber_id
                loser_snapshot = users[loser_id]
                requested_amount = min(int(min(loser_snapshot[4], 1000000) * 0.1), 1000000)
                transferred_amount = min(requested_amount, loser_snapshot[4])
                robber_stone = robber_snapshot[4]
                victim_stone = victim_snapshot[4]
                if winner_id == robber_id:
                    robber_stone += transferred_amount
                    victim_stone -= transferred_amount
                else:
                    robber_stone -= transferred_amount
                    victim_stone += transferred_amount

                robber_changed = conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s,user_stamina=%s,stone=%s "
                    "WHERE user_id=%s AND hp IS %s AND mp IS %s "
                    "AND COALESCE(user_stamina,0)=%s AND COALESCE(exp,0)=%s "
                    "AND COALESCE(stone,0)=%s",
                    (
                        robber_final[0], robber_final[1], robber_snapshot[2] - stamina_cost,
                        robber_stone, robber_id, *robber_snapshot,
                    ),
                )
                victim_changed = conn.execute(
                    "UPDATE user_xiuxian SET hp=%s,mp=%s,stone=%s "
                    "WHERE user_id=%s AND hp IS %s AND mp IS %s "
                    "AND COALESCE(user_stamina,0)=%s AND COALESCE(exp,0)=%s "
                    "AND COALESCE(stone,0)=%s",
                    (
                        victim_final[0], victim_final[1], victim_stone,
                        victim_id, *victim_snapshot,
                    ),
                )
                if robber_changed.rowcount != 1 or victim_changed.rowcount != 1:
                    conn.rollback()
                    return current_result("state_changed")

                increment_stat(conn, winner_id, "抢灵石成功", 1)
                increment_stat(conn, loser_id, "抢灵石失败", 1)
                loser_balance = victim_stone if loser_id == victim_id else robber_stone
                saved = {
                    "robber_id": robber_id,
                    "victim_id": victim_id,
                    "winner_id": winner_id,
                    "battle_messages": battle_messages,
                    "robber_hp": robber_final[0],
                    "robber_mp": robber_final[1],
                    "victim_hp": victim_final[0],
                    "victim_mp": victim_final[1],
                    "requested_amount": requested_amount,
                    "transferred_amount": transferred_amount,
                    "loser_balance": loser_balance,
                    "stamina_cost": stamina_cost,
                    "robber_stamina": robber_snapshot[2] - stamina_cost,
                }
                conn.execute(
                    "INSERT INTO stone_robbery_operations "
                    "(operation_id,robber_id,victim_id,result_json) VALUES (%s,%s,%s,%s)",
                    (
                        operation_id,
                        robber_id,
                        victim_id,
                        json.dumps(saved, ensure_ascii=False, separators=(",", ":")),
                    ),
                )
                conn.commit()
                return StoneRobberyResult("applied", **saved)
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    try:
                        conn.execute("DETACH DATABASE player_data")
                    except Exception:
                        pass


__all__ = ["StoneRobberyResult", "StoneRobberySettlementService"]
