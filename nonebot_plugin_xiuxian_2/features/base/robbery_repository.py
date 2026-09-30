from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import AttachedDatabaseUnitOfWork


class _StateChanged(Exception):
    pass


@dataclass(frozen=True)
class BaseStoneRobberyResult:
    status: str
    robber_id: str = ""
    victim_id: str = ""
    winner_id: str = ""
    battle_messages: list[Any] = field(default_factory=list)
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


class BaseStoneRobberySqlRepository:
    SNAPSHOT_FIELDS = ("hp", "mp", "user_stamina", "exp", "stone")
    OPERATION_COLUMNS = {"operation_id", "robber_id", "victim_id", "result_json"}
    STATISTICS_COLUMNS = {"user_id", "抢灵石成功", "抢灵石失败"}

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)

    @staticmethod
    def _columns(uow: AttachedDatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA {schema}.table_info("{table}")')
        }

    @classmethod
    def _schema_ready(cls, uow: AttachedDatabaseUnitOfWork) -> bool:
        return (
            cls.OPERATION_COLUMNS.issubset(cls._columns(uow, "stone_robbery_operations"))
            and {"user_id", "hp", "mp", "user_stamina", "exp", "stone"}.issubset(
                cls._columns(uow, "user_xiuxian")
            )
            and cls.STATISTICS_COLUMNS.issubset(cls._columns(uow, "statistics", "player_data"))
        )

    @staticmethod
    def _snapshot(value: Mapping[str, Any]) -> tuple[int | None, ...]:
        data = dict(value)
        return tuple(
            None if field in {"hp", "mp"} and data.get(field) is None
            else int(data.get(field) or 0)
            for field in BaseStoneRobberySqlRepository.SNAPSHOT_FIELDS
        )

    @staticmethod
    def _saved_result(value: Any, status: str = "duplicate") -> BaseStoneRobberyResult:
        return BaseStoneRobberyResult(status=status, **json.loads(str(value)))

    def get_result(self, operation_id: str, robber_id: str, victim_id: str):
        operation_id = str(operation_id).strip()
        robber_id, victim_id = str(robber_id), str(victim_id)
        if not operation_id or not self.game_database.is_file() or not self.player_database.is_file():
            return None
        with AttachedDatabaseUnitOfWork(
            self.game_database,
            attachments={"player_data": self.player_database},
            read_only=True,
        ) as uow:
            if not self._schema_ready(uow):
                return None
            row = uow.query_one(
                "SELECT robber_id,victim_id,result_json FROM stone_robbery_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
        if row is None:
            return None
        if str(row["robber_id"]) != robber_id or str(row["victim_id"]) != victim_id:
            return BaseStoneRobberyResult("operation_conflict")
        return self._saved_result(row["result_json"])

    def settle(
        self,
        operation_id: str,
        robber_id: str,
        victim_id: str,
        *,
        expected_robber: Mapping[str, Any],
        expected_victim: Mapping[str, Any],
        robber_final: tuple[int, int],
        victim_final: tuple[int, int],
        winner_id: str,
        battle_messages: list[Any] | None,
        stamina_cost: int = 15,
    ) -> BaseStoneRobberyResult:
        operation_id = str(operation_id).strip()
        robber_id, victim_id, winner_id = str(robber_id), str(victim_id), str(winner_id)
        robber_snapshot = self._snapshot(expected_robber)
        victim_snapshot = self._snapshot(expected_victim)
        robber_final = (max(1, int(robber_final[0])), max(0, int(robber_final[1])))
        victim_final = (max(1, int(victim_final[0])), max(0, int(victim_final[1])))
        battle_messages = list(battle_messages or [])
        stamina_cost = int(stamina_cost)
        if not operation_id or robber_id == victim_id or winner_id not in {robber_id, victim_id} or stamina_cost < 0:
            raise ValueError("valid robbery participants, winner and stamina are required")

        def result(status: str) -> BaseStoneRobberyResult:
            return BaseStoneRobberyResult(status=status, robber_id=robber_id, victim_id=victim_id, winner_id=winner_id)

        if not self.game_database.is_file() or not self.player_database.is_file():
            return result("schema_missing")
        with AttachedDatabaseUnitOfWork(
            self.game_database,
            attachments={"player_data": self.player_database},
            immediate=True,
        ) as uow:
            if not self._schema_ready(uow):
                return result("schema_missing")
            previous = uow.query_one(
                "SELECT robber_id,victim_id,result_json FROM stone_robbery_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["robber_id"]) != robber_id or str(previous["victim_id"]) != victim_id:
                    return result("operation_conflict")
                return self._saved_result(previous["result_json"])

            rows = uow.query_all(
                "SELECT user_id,hp,mp,COALESCE(user_stamina,0) AS user_stamina,"
                "COALESCE(exp,0) AS exp,COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id IN (?,?)",
                (robber_id, victim_id),
            )
            users = {
                str(row["user_id"]): tuple(
                    None if index < 2 and row[field] is None else int(row[field] or 0)
                    for index, field in enumerate(self.SNAPSHOT_FIELDS)
                )
                for row in rows
            }
            if robber_id not in users or victim_id not in users:
                return result("user_missing")
            if users[robber_id] != robber_snapshot or users[victim_id] != victim_snapshot:
                return result("state_changed")
            robber_hp = robber_snapshot[3] // 2 if robber_snapshot[0] is None else robber_snapshot[0]
            victim_hp = victim_snapshot[3] // 2 if victim_snapshot[0] is None else victim_snapshot[0]
            if robber_hp <= robber_snapshot[3] / 10:
                return result("robber_injured")
            if victim_hp <= victim_snapshot[3] / 10:
                return result("victim_injured")
            if robber_snapshot[2] < stamina_cost:
                return result("stamina_insufficient")

            loser_id = victim_id if winner_id == robber_id else robber_id
            loser_snapshot = users[loser_id]
            requested_amount = min(int(min(loser_snapshot[4], 1000000) * 0.1), 1000000)
            transferred_amount = min(requested_amount, loser_snapshot[4])
            robber_stone, victim_stone = robber_snapshot[4], victim_snapshot[4]
            if winner_id == robber_id:
                robber_stone += transferred_amount
                victim_stone -= transferred_amount
            else:
                robber_stone -= transferred_amount
                victim_stone += transferred_amount

            try:
                with uow.savepoint("stone_robbery_assets"):
                    robber_changed = uow.execute(
                        "UPDATE user_xiuxian SET hp=?,mp=?,user_stamina=?,stone=? WHERE user_id=? AND hp IS ? AND mp IS ? "
                        "AND COALESCE(user_stamina,0)=? AND COALESCE(exp,0)=? AND COALESCE(stone,0)=?",
                        (robber_final[0], robber_final[1], robber_snapshot[2] - stamina_cost, robber_stone, robber_id, *robber_snapshot),
                    )
                    victim_changed = uow.execute(
                        "UPDATE user_xiuxian SET hp=?,mp=?,stone=? WHERE user_id=? AND hp IS ? AND mp IS ? "
                        "AND COALESCE(user_stamina,0)=? AND COALESCE(exp,0)=? AND COALESCE(stone,0)=?",
                        (victim_final[0], victim_final[1], victim_stone, victim_id, *victim_snapshot),
                    )
                    if robber_changed.rowcount != 1 or victim_changed.rowcount != 1:
                        raise _StateChanged
            except _StateChanged:
                return result("state_changed")

            loser_balance = victim_stone if loser_id == victim_id else robber_stone
            saved = {
                "robber_id": robber_id, "victim_id": victim_id, "winner_id": winner_id,
                "battle_messages": battle_messages, "robber_hp": robber_final[0], "robber_mp": robber_final[1],
                "victim_hp": victim_final[0], "victim_mp": victim_final[1], "requested_amount": requested_amount,
                "transferred_amount": transferred_amount, "loser_balance": loser_balance,
                "stamina_cost": stamina_cost, "robber_stamina": robber_snapshot[2] - stamina_cost,
            }
            for user_id, column in ((winner_id, '"抢灵石成功"'), (loser_id, '"抢灵石失败"')):
                changed = uow.execute(
                    f'UPDATE player_data.statistics SET {column}=COALESCE({column},0)+1 WHERE user_id=?',
                    (user_id,),
                )
                if changed.rowcount == 0:
                    uow.execute(f'INSERT INTO player_data.statistics(user_id,{column}) VALUES(?,1)', (user_id,))
            uow.execute(
                "INSERT INTO stone_robbery_operations(operation_id,robber_id,victim_id,result_json) VALUES(?,?,?,?)",
                (operation_id, robber_id, victim_id, json.dumps(saved, ensure_ascii=False, separators=(",", ":"))),
            )
            return BaseStoneRobberyResult("applied", **saved)


__all__ = ["BaseStoneRobberyResult", "BaseStoneRobberySqlRepository"]
