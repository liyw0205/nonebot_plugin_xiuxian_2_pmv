"""Feature-owned persistence for normal player-versus-player settlement."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import AttachedDatabaseUnitOfWork


class _StateChanged(Exception):
    pass


class NormalPvpSqlRepository:
    """Persist PvP effects without request-time schema changes."""

    OPERATION_COLUMNS = {"operation_id", "payload", "result_json"}
    PLAYER_COLUMNS = {"user_id", "hp", "mp", "user_stamina", "exp"}
    STATISTICS_COLUMNS = {"user_id", "切磋胜利", "切磋失败"}

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
            cls.OPERATION_COLUMNS.issubset(cls._columns(uow, "normal_pvp_operations"))
            and cls.PLAYER_COLUMNS.issubset(cls._columns(uow, "user_xiuxian"))
            and cls.STATISTICS_COLUMNS.issubset(cls._columns(uow, "statistics", "player_data"))
        )

    @staticmethod
    def _payload(challenger_id: str, opponent_id: str, stamina_cost: int) -> str:
        return json.dumps(
            {
                "challenger_id": str(challenger_id),
                "opponent_id": str(opponent_id),
                "stamina_cost": int(stamina_cost),
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )

    @staticmethod
    def _participants(payload: Any) -> tuple[str | None, str | None]:
        try:
            value = json.loads(str(payload))
        except (TypeError, ValueError):
            return None, None
        if isinstance(value, Mapping):
            return value.get("challenger_id"), value.get("opponent_id")
        if isinstance(value, list) and len(value) >= 2:
            return value[0], value[1]
        return None, None

    @staticmethod
    def _saved_result(value: Any, status: str = "duplicate") -> dict[str, Any]:
        saved = json.loads(str(value))
        return {"status": status, **dict(saved)}

    def get_result(self, operation_id: str, challenger_id: str, opponent_id: str) -> dict[str, Any] | None:
        operation_id = str(operation_id).strip()
        challenger_id, opponent_id = str(challenger_id), str(opponent_id)
        if not operation_id or not self.game_database.is_file() or not self.player_database.is_file():
            return None
        try:
            with AttachedDatabaseUnitOfWork(
                self.game_database,
                attachments={"player_data": self.player_database},
                read_only=True,
            ) as uow:
                if not self._schema_ready(uow):
                    return None
                row = uow.query_one(
                    "SELECT payload,result_json FROM normal_pvp_operations WHERE operation_id=?",
                    (operation_id,),
                )
        except Exception:
            return None
        if row is None:
            return None
        participants = self._participants(row["payload"])
        if participants != (challenger_id, opponent_id):
            return {"status": "operation_conflict"}
        return self._saved_result(row["result_json"])

    def settle(
        self,
        operation_id: str,
        challenger_id: str,
        opponent_id: str,
        *,
        expected_challenger_hp: int,
        expected_challenger_mp: int,
        expected_challenger_stamina: int,
        expected_challenger_exp: int,
        expected_opponent_hp: int,
        expected_opponent_mp: int,
        expected_opponent_stamina: int,
        expected_opponent_exp: int,
        challenger_final_hp: int,
        challenger_final_mp: int,
        opponent_final_hp: int,
        opponent_final_mp: int,
        winner_id: str = "",
        winner_name: str = "没有人",
        battle_messages: list[Any] | None = None,
        stamina_cost: int = 1,
    ) -> dict[str, Any]:
        operation_id = str(operation_id).strip()
        challenger_id, opponent_id = str(challenger_id), str(opponent_id)
        winner_id = str(winner_id or "")
        stamina_cost = int(stamina_cost)
        snapshots = tuple(
            int(value)
            for value in (
                expected_challenger_hp,
                expected_challenger_mp,
                expected_challenger_stamina,
                expected_challenger_exp,
                expected_opponent_hp,
                expected_opponent_mp,
                expected_opponent_stamina,
                expected_opponent_exp,
            )
        )
        finals = tuple(
            max(1, int(value))
            for value in (challenger_final_hp, challenger_final_mp, opponent_final_hp, opponent_final_mp)
        )
        if not operation_id or challenger_id == opponent_id or stamina_cost < 0:
            raise ValueError("invalid normal pvp settlement arguments")
        if winner_id not in {"", challenger_id, opponent_id}:
            raise ValueError("invalid normal pvp winner")
        payload = self._payload(challenger_id, opponent_id, stamina_cost)
        messages = list(battle_messages or [])

        def result(status: str) -> dict[str, Any]:
            return {
                "status": status,
                "winner_id": winner_id,
                "winner_name": str(winner_name),
                "battle_messages": messages,
            }

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
                "SELECT payload,result_json FROM normal_pvp_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return result("operation_conflict")
                return self._saved_result(previous["result_json"])

            rows = uow.query_all(
                "SELECT user_id,COALESCE(hp,0) AS hp,COALESCE(mp,0) AS mp,"
                "COALESCE(user_stamina,0) AS user_stamina,COALESCE(exp,0) AS exp "
                "FROM user_xiuxian WHERE user_id IN (?,?)",
                (challenger_id, opponent_id),
            )
            current = {
                str(row["user_id"]): tuple(int(row[field]) for field in ("hp", "mp", "user_stamina", "exp"))
                for row in rows
            }
            if challenger_id not in current or opponent_id not in current:
                return result("user_missing")
            challenger_snapshot, opponent_snapshot = snapshots[:4], snapshots[4:]
            if current[challenger_id] != challenger_snapshot or current[opponent_id] != opponent_snapshot:
                return result("state_changed")
            if challenger_snapshot[0] <= challenger_snapshot[3] / 10:
                return result("challenger_injured")
            if challenger_snapshot[2] < stamina_cost:
                return result("stamina_insufficient")

            try:
                with uow.savepoint("normal_pvp_assets"):
                    changed = uow.execute(
                        "UPDATE user_xiuxian SET hp=?,mp=?,user_stamina=user_stamina-? "
                        "WHERE user_id=? AND hp=? AND mp=? AND user_stamina=? AND exp=? AND user_stamina>=?",
                        (finals[0], finals[1], stamina_cost, challenger_id, *challenger_snapshot, stamina_cost),
                    )
                    if changed.rowcount != 1:
                        raise _StateChanged
                    changed = uow.execute(
                        "UPDATE user_xiuxian SET hp=?,mp=? "
                        "WHERE user_id=? AND hp=? AND mp=? AND user_stamina=? AND exp=?",
                        (finals[2], finals[3], opponent_id, *opponent_snapshot),
                    )
                    if changed.rowcount != 1:
                        raise _StateChanged
            except _StateChanged:
                return result("state_changed")

            if winner_id:
                loser_id = opponent_id if winner_id == challenger_id else challenger_id
                for user_id, column in ((winner_id, "切磋胜利"), (loser_id, "切磋失败")):
                    changed = uow.execute(
                        f'UPDATE player_data.statistics SET "{column}"=COALESCE("{column}",0)+1 WHERE user_id=?',
                        (user_id,),
                    )
                    if changed.rowcount == 0:
                        uow.execute(
                            f'INSERT INTO player_data.statistics(user_id,"{column}") VALUES(?,1)',
                            (user_id,),
                        )

            saved = {
                "winner_id": winner_id,
                "winner_name": str(winner_name),
                "battle_messages": messages,
                "challenger_hp": finals[0],
                "challenger_mp": finals[1],
                "opponent_hp": finals[2],
                "opponent_mp": finals[3],
            }
            uow.execute(
                "INSERT INTO normal_pvp_operations(operation_id,payload,result_json) VALUES(?,?,?)",
                (operation_id, payload, json.dumps(saved, ensure_ascii=False, separators=(",", ":"))),
            )
            return {"status": "applied", **saved}


__all__ = ["NormalPvpSqlRepository"]
