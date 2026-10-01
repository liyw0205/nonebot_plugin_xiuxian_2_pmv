from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .player_stats_repository import append_player_outbox


@dataclass(frozen=True)
class DufangBetResult:
    status: str
    cost: int = 0
    wallet_stone: int = 0
    bet_id: str = ""
    resolution: Mapping[str, Any] | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DufangBetSqlRepository:
    _GAME_COLUMNS = {
        "dufang_bets": {"bet_id", "user_id", "cost", "status", "placed_at", "settled_at"},
        "dufang_bet_operations": {"operation_id", "payload", "cost", "wallet_stone", "bet_id", "created_at"},
        "user_xiuxian": {"user_id", "stone"},
        "dufang_bet_resolutions": {"operation_id", "plan_json", "created_at"},
        "dufang_player_outbox": {
            "event_id", "operation_id", "event_type", "payload_json", "status", "created_at", "updated_at",
        },
    }

    def __init__(self, game_database: str | Path) -> None:
        self.game_database = Path(game_database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        safe_schema = "player_data" if schema == "player_data" else "main"
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA {safe_schema}.table_info("{table}")')
        }

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        if any(not required.issubset(cls._columns(uow, table)) for table, required in cls._GAME_COLUMNS.items()):
            return False
        for table, key_column in (
            ("dufang_bets", "bet_id"),
            ("dufang_bet_operations", "operation_id"),
            ("dufang_bet_resolutions", "operation_id"),
            ("dufang_player_outbox", "event_id"),
        ):
            primary_key = next(
                (
                    int(row["pk"])
                    for row in uow.query_all(f'PRAGMA main.table_info("{table}")')
                    if str(row["name"]).casefold() == key_column
                ),
                0,
            )
            if primary_key != 1:
                return False
        return True

    @staticmethod
    def _resolution(uow: DatabaseUnitOfWork, operation_id: str) -> Mapping[str, Any] | None:
        row = uow.query_one(
            "SELECT plan_json FROM dufang_bet_resolutions WHERE operation_id=?",
            (operation_id,),
        )
        return None if row is None else json.loads(str(row["plan_json"]))

    def get_resolution(self, operation_id: str) -> DufangBetResult:
        operation_id = str(operation_id).strip()
        if not self.game_database.is_file():
            return DufangBetResult("schema_missing")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return DufangBetResult("schema_missing")
            row = uow.query_one(
                "SELECT o.payload,o.cost,o.wallet_stone,o.bet_id,b.status "
                "FROM dufang_bet_operations o JOIN dufang_bets b ON b.bet_id=o.bet_id "
                "WHERE o.operation_id=?",
                (operation_id,),
            )
            if row is None:
                return DufangBetResult("not_found")
            resolution = self._resolution(uow, operation_id)
            if resolution is None:
                return DufangBetResult(
                    "resolution_missing", int(row["cost"]), int(row["wallet_stone"]), str(row["bet_id"])
                )
            return DufangBetResult(
                "applied", int(row["cost"]), int(row["wallet_stone"]), str(row["bet_id"]), resolution
            )

    def schema_ready(self) -> bool:
        if not self.game_database.is_file():
            return False
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            return self._schema_ready(uow)

    def total_cost(self, user_id: str) -> int:
        if not Path(self.game_database).is_file():
            return 0
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return 0
            row = uow.query_one(
                "SELECT COALESCE(SUM(cost),0) AS total_cost FROM dufang_bets WHERE user_id=?",
                (str(user_id),),
            )
            return 0 if row is None else int(row["total_cost"])

    def pending_resolutions(self, *, limit: int = 25) -> list[Mapping[str, str]]:
        limit = max(1, min(int(limit), 25))
        if not self.game_database.is_file():
            return []
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return []
            return uow.query_all(
                "SELECT b.bet_id AS operation_id,b.user_id FROM dufang_bets b "
                "JOIN dufang_bet_resolutions r ON r.operation_id=b.bet_id "
                "WHERE b.status='pending' ORDER BY b.placed_at,b.bet_id LIMIT ?",
                (limit,),
            )

    def pending_count(self) -> int:
        if not self.game_database.is_file():
            return 0
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if not self._schema_ready(uow):
                return 0
            row = uow.query_one("SELECT COUNT(*) AS count FROM dufang_bets WHERE status='pending'")
            return 0 if row is None else int(row["count"])

    def place(
        self,
        operation_id: str,
        user_id: str,
        cost: int,
        placed_at: str,
        resolution: Mapping[str, Any],
    ) -> DufangBetResult:
        operation_id, user_id, cost = str(operation_id).strip(), str(user_id), int(cost)
        placed_at = str(placed_at).strip()
        if not operation_id or cost <= 0 or not placed_at:
            raise ValueError("operation id, positive cost and placement time are required")
        if not isinstance(resolution, Mapping):
            raise ValueError("a frozen resolution plan is required")
        normalized_resolution = dict(resolution)
        payout_outcome = str(normalized_resolution.get("payout_outcome", ""))
        gain = int(normalized_resolution.get("gain", -1))
        requested_loss = int(normalized_resolution.get("requested_loss", -1))
        if payout_outcome not in {"win", "loss"} or gain < 0 or requested_loss < 0:
            raise ValueError("invalid frozen payout plan")
        if (payout_outcome == "win" and (gain <= 0 or requested_loss != 0)) or (
            payout_outcome == "loss" and (gain != 0 or requested_loss < 0)
        ):
            raise ValueError("frozen payout plan does not match its outcome")
        resolution_json = json.dumps(
            normalized_resolution, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        if not Path(self.game_database).is_file():
            return DufangBetResult("schema_missing")
        payload = json.dumps([user_id, cost], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return DufangBetResult("schema_missing")
            previous = uow.query_one("SELECT payload,cost,wallet_stone,bet_id FROM dufang_bet_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return DufangBetResult("state_changed")
                frozen = self._resolution(uow, operation_id)
                if frozen is None:
                    return DufangBetResult(
                        "resolution_missing", int(previous["cost"]), int(previous["wallet_stone"]), str(previous["bet_id"])
                    )
                return DufangBetResult(
                    "duplicate", int(previous["cost"]), int(previous["wallet_stone"]),
                    str(previous["bet_id"]), frozen,
                )
            user = uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return DufangBetResult("user_missing")
            wallet = int(user["stone"])
            if wallet < cost:
                return DufangBetResult("stone_insufficient", wallet_stone=wallet)
            if uow.execute("UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone>=?", (cost, user_id, cost)).rowcount != 1:
                return DufangBetResult("state_changed", wallet_stone=wallet)
            uow.execute("INSERT INTO dufang_bets VALUES(?,?,?,?,?,NULL)", (operation_id, user_id, cost, "pending", placed_at))
            uow.execute("INSERT INTO dufang_bet_operations VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)", (operation_id, payload, cost, wallet - cost, operation_id))
            uow.execute(
                "INSERT INTO dufang_bet_resolutions(operation_id,plan_json,created_at) VALUES(?,?,?)",
                (operation_id, resolution_json, placed_at),
            )
            append_player_outbox(
                uow,
                operation_id=operation_id,
                event_type="bet",
                payload={"user_id": user_id, "cost": cost, "occurred_at": placed_at},
                created_at=placed_at,
            )
            return DufangBetResult("applied", cost, wallet - cost, operation_id, normalized_resolution)


__all__ = ["DufangBetSqlRepository", "DufangBetResult"]
