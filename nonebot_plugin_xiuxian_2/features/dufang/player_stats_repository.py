from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class DufangPlayerStatsResult:
    status: str
    applied: int = 0
    pending: int = 0


def _encoded(payload: Mapping[str, Any]) -> str:
    return json.dumps(dict(payload), ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def append_player_outbox(
    uow: DatabaseUnitOfWork,
    *,
    operation_id: str,
    event_type: str,
    payload: Mapping[str, Any],
    created_at: str,
) -> None:
    operation_id, event_type = str(operation_id).strip(), str(event_type).strip()
    if not operation_id or event_type not in {"bet", "payout"} or not str(created_at).strip():
        raise ValueError("invalid dufang player outbox event")
    event_id = f"{operation_id}:{event_type}"
    payload_json = _encoded(payload)
    uow.execute(
        "INSERT OR IGNORE INTO dufang_player_outbox("
        "event_id,operation_id,event_type,payload_json,status,created_at,updated_at) "
        "VALUES(?,?,?,?,'pending',?,?)",
        (event_id, operation_id, event_type, payload_json, created_at, created_at),
    )
    previous = uow.query_one(
        "SELECT payload_json FROM dufang_player_outbox WHERE operation_id=? AND event_type=?",
        (operation_id, event_type),
    )
    if previous is None or str(previous["payload_json"]) != payload_json:
        raise RuntimeError("dufang player outbox payload conflicts with its operation")


class DufangPlayerStatsSqlRepository:
    _PLAYER_COLUMNS = {"user_id", "count", "total_cost", "profit", "loss", "last_update"}
    _RECEIPT_COLUMNS = {"operation_id", "event_type", "payload_json", "created_at"}

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @classmethod
    def _player_schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        if not cls._PLAYER_COLUMNS.issubset(cls._columns(uow, "unseal_data")):
            return False
        if not cls._RECEIPT_COLUMNS.issubset(cls._columns(uow, "dufang_player_operation_receipts")):
            return False
        primary_keys = {
            str(row["name"]).casefold(): int(row["pk"])
            for row in uow.query_all('PRAGMA table_info("dufang_player_operation_receipts")')
        }
        user_key = next(
            (
                int(row["pk"])
                for row in uow.query_all('PRAGMA table_info("unseal_data")')
                if str(row["name"]).casefold() == "user_id"
            ),
            0,
        )
        return user_key == 1 and primary_keys.get("operation_id") == 1 and primary_keys.get("event_type") == 2

    def schema_ready(self) -> bool:
        if not self.player_database.is_file():
            return False
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            return self._player_schema_ready(uow)

    def total_cost(self, user_id: str) -> int:
        if not self.player_database.is_file():
            return 0
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            if not self._player_schema_ready(uow):
                return 0
            row = uow.query_one(
                "SELECT COALESCE(total_cost,0) AS total_cost FROM unseal_data WHERE user_id=?",
                (str(user_id),),
            )
            return 0 if row is None else int(row["total_cost"])

    def reconcile(self, *, limit: int = 25, priority_event_id: str = "") -> DufangPlayerStatsResult:
        limit = max(1, min(int(limit), 25))
        if not self.game_database.is_file() or not self.player_database.is_file():
            return DufangPlayerStatsResult("schema_missing", pending=0)
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            if not self._player_schema_ready(uow):
                return DufangPlayerStatsResult("schema_missing")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if not {"event_id", "operation_id", "event_type", "payload_json", "status"}.issubset(
                self._columns(uow, "dufang_player_outbox")
            ):
                return DufangPlayerStatsResult("schema_missing")
            events = []
            if priority_event_id:
                priority = uow.query_one(
                    "SELECT event_id,operation_id,event_type,payload_json,created_at "
                    "FROM dufang_player_outbox WHERE event_id=? AND status='pending'",
                    (str(priority_event_id),),
                )
                if priority is not None:
                    events.append(priority)
            remaining = limit - len(events)
            if remaining > 0:
                events.extend(
                    uow.query_all(
                        "SELECT event_id,operation_id,event_type,payload_json,created_at "
                        "FROM dufang_player_outbox WHERE status='pending' "
                        "AND event_id<>? ORDER BY created_at,event_id LIMIT ?",
                        (str(priority_event_id), remaining),
                    )
                )

        applied = 0
        for event in events:
            event_id = str(event["event_id"])
            operation_id = str(event["operation_id"])
            event_type = str(event["event_type"])
            payload_json = str(event["payload_json"])
            payload = json.loads(payload_json)
            user_id = str(payload.get("user_id", "")).strip()
            occurred_at = str(payload.get("occurred_at", "")).strip()
            if not user_id or not occurred_at or event_type not in {"bet", "payout"}:
                raise RuntimeError("invalid dufang player outbox payload")

            with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
                if not self._player_schema_ready(uow):
                    return DufangPlayerStatsResult("schema_missing", applied=applied, pending=len(events) - applied)
                previous = uow.query_one(
                    "SELECT payload_json FROM dufang_player_operation_receipts "
                    "WHERE operation_id=? AND event_type=?",
                    (operation_id, event_type),
                )
                if previous is not None:
                    if str(previous["payload_json"]) != payload_json:
                        raise RuntimeError("dufang player operation receipt conflicts with its outbox")
                elif event_type == "bet":
                    cost = int(payload["cost"])
                    if cost <= 0:
                        raise RuntimeError("invalid dufang bet projection amount")
                    uow.execute(
                        "INSERT INTO unseal_data(user_id,count,total_cost,last_update) VALUES(?,1,?,?) "
                        "ON CONFLICT(user_id) DO UPDATE SET count=COALESCE(count,0)+1,"
                        "total_cost=COALESCE(total_cost,0)+excluded.total_cost,last_update=excluded.last_update",
                        (user_id, cost, occurred_at),
                    )
                    uow.execute(
                        "INSERT INTO dufang_player_operation_receipts(operation_id,event_type,payload_json,created_at) "
                        "VALUES(?,?,?,?)",
                        (operation_id, event_type, payload_json, occurred_at),
                    )
                else:
                    gain, loss = int(payload["gain"]), int(payload["loss"])
                    if gain < 0 or loss < 0 or (gain and loss):
                        raise RuntimeError("invalid dufang payout projection amount")
                    uow.execute(
                        "INSERT INTO unseal_data(user_id,profit,loss,last_update) VALUES(?,?,?,?) "
                        "ON CONFLICT(user_id) DO UPDATE SET profit=COALESCE(profit,0)+excluded.profit,"
                        "loss=COALESCE(loss,0)+excluded.loss,last_update=excluded.last_update",
                        (user_id, gain, loss, occurred_at),
                    )
                    uow.execute(
                        "INSERT INTO dufang_player_operation_receipts(operation_id,event_type,payload_json,created_at) "
                        "VALUES(?,?,?,?)",
                        (operation_id, event_type, payload_json, occurred_at),
                    )

            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                changed = uow.execute(
                    "UPDATE dufang_player_outbox SET status='sent',updated_at=? "
                    "WHERE event_id=? AND status='pending'",
                    (str(event["created_at"]), event_id),
                ).rowcount
                if changed not in {0, 1}:
                    raise RuntimeError("dufang player outbox state changed")
            applied += 1

        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT COUNT(*) AS count FROM dufang_player_outbox WHERE status='pending'"
            )
        pending = 0 if row is None else int(row["count"])
        return DufangPlayerStatsResult(
            "applied" if applied else "idle", applied=applied, pending=pending
        )


__all__ = ["DufangPlayerStatsResult", "DufangPlayerStatsSqlRepository", "append_player_outbox"]
