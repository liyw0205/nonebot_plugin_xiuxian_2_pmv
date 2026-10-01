from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class DufangShareRecipientResult:
    user_id: str
    user_name: str
    status: str
    amount: int = 0
    wallet_stone: int = 0


@dataclass(frozen=True)
class DufangShareSettlementResult:
    status: str
    task_status: str = ""
    event_type: str = ""
    event_title: str = ""
    event_description: str = ""
    bonus_percent: int = 0
    total: int = 0
    completed: int = 0
    total_amount: int = 0
    recipients: tuple[DufangShareRecipientResult, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class DufangShareSqlRepository:
    """Settle a frozen sharing batch using startup-owned schemas."""

    _GAME_COLUMNS = {
        "dufang_share_operations": {
            "operation_id", "source_id", "event_type", "event_title", "event_description",
            "effect_amount", "bonus_percent", "total", "completed", "total_amount",
            "status", "created_at", "updated_at",
        },
        "dufang_share_progress": {
            "operation_id", "ordinal", "target_id", "target_name", "status",
            "actual_amount", "wallet_stone", "reason", "updated_at",
        },
        "economy_log": {"user_id", "source", "action", "stone_delta", "item_delta", "detail", "trace_id", "created_at"},
        "user_xiuxian": {"user_id", "stone"},
    }
    _PLAYER_COLUMNS = {
        "user_id", "shared_profit", "shared_loss", "received_profit", "received_loss", "last_update",
    }
    _PLAYER_RECEIPT_COLUMNS = {
        "operation_id", "target_id", "source_id", "event_type", "amount", "updated_at",
    }

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        safe_schema = "player_data" if schema == "player_data" else "main"
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA {safe_schema}.table_info("{table}")')
        }

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        for table, required in cls._GAME_COLUMNS.items():
            if not required.issubset(cls._columns(uow, table)):
                return False
        return True

    @classmethod
    def _player_schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        if not cls._PLAYER_COLUMNS.issubset(cls._columns(uow, "unseal_data")):
            return False
        if not cls._PLAYER_RECEIPT_COLUMNS.issubset(cls._columns(uow, "dufang_share_player_receipts")):
            return False
        player_key = next(
            (
                int(row["pk"])
                for row in uow.query_all('PRAGMA main.table_info("unseal_data")')
                if str(row["name"]).casefold() == "user_id"
            ),
            0,
        )
        return player_key == 1

    @staticmethod
    def _normalize_recipients(source_id: str, recipients: Iterable[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
        normalized: list[tuple[str, str]] = []
        seen: set[str] = set()
        for target_id, target_name in recipients:
            target_id = str(target_id).strip()
            if not target_id or target_id == source_id or target_id in seen:
                raise ValueError("sharing recipients must be unique and different from the source")
            seen.add(target_id)
            normalized.append((target_id, str(target_name) or "未知道友"))
        if not normalized:
            raise ValueError("at least one sharing recipient is required")
        return tuple(normalized)

    @staticmethod
    def _result(
        uow: DatabaseUnitOfWork,
        operation_id: str,
        status: str,
        applied_now: set[str] | None = None,
    ) -> DufangShareSettlementResult:
        operation = uow.query_one(
            "SELECT event_type,event_title,event_description,bonus_percent,total,completed,total_amount,status "
            "FROM dufang_share_operations WHERE operation_id=?",
            (operation_id,),
        )
        if operation is None:
            return DufangShareSettlementResult(status)
        just_applied = applied_now or set()
        recipients = []
        for row in uow.query_all(
            "SELECT target_id,target_name,status,actual_amount,wallet_stone "
            "FROM dufang_share_progress WHERE operation_id=? ORDER BY ordinal",
            (operation_id,),
        ):
            recipient_status = str(row["status"])
            if recipient_status == "applied" and str(row["target_id"]) not in just_applied:
                recipient_status = "duplicate"
            recipients.append(
                DufangShareRecipientResult(
                    str(row["target_id"]),
                    str(row["target_name"]),
                    recipient_status,
                    int(row["actual_amount"]),
                    int(row["wallet_stone"]),
                )
            )
        return DufangShareSettlementResult(
            status,
            str(operation["status"]),
            str(operation["event_type"]),
            str(operation["event_title"]),
            str(operation["event_description"]),
            int(operation["bonus_percent"]),
            int(operation["total"]),
            int(operation["completed"]),
            int(operation["total_amount"]),
            tuple(recipients),
        )

    def settle(
        self,
        *,
        operation_id: str,
        source_id: str,
        event_type: str,
        event_title: str,
        event_description: str,
        effect_amount: int,
        bonus_percent: int,
        recipients: Iterable[tuple[str, str]],
        occurred_at: str,
        chunk_size: int = 100,
    ) -> DufangShareSettlementResult:
        operation_id, source_id = str(operation_id).strip(), str(source_id).strip()
        event_type, event_title = str(event_type).strip(), str(event_title)
        event_description, occurred_at = str(event_description), str(occurred_at).strip()
        effect_amount, bonus_percent = int(effect_amount), int(bonus_percent)
        chunk_size = max(1, int(chunk_size))
        if (
            not operation_id
            or not source_id
            or event_type not in {"profit", "loss"}
            or not event_title
            or effect_amount <= 0
            or not 0 <= bonus_percent <= 50
            or not occurred_at
        ):
            raise ValueError("invalid sharing settlement request")
        normalized = self._normalize_recipients(source_id, recipients)
        if not self.game_database.is_file() or not self.player_database.is_file():
            return DufangShareSettlementResult("schema_missing")
        with DatabaseUnitOfWork(self.game_database, read_only=True) as game_uow:
            if not self._schema_ready(game_uow):
                return DufangShareSettlementResult("schema_missing")
        with DatabaseUnitOfWork(self.player_database, read_only=True) as player_uow:
            if not self._player_schema_ready(player_uow):
                return DufangShareSettlementResult("schema_missing")

        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return DufangShareSettlementResult("schema_missing")
            previous = uow.query_one(
                "SELECT source_id,status FROM dufang_share_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is None:
                uow.execute(
                    "INSERT INTO dufang_share_operations("
                    "operation_id,source_id,event_type,event_title,event_description,effect_amount,"
                    "bonus_percent,total,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        operation_id, source_id, event_type, event_title, event_description,
                        effect_amount, bonus_percent, len(normalized), occurred_at, occurred_at,
                    ),
                )
                uow.executemany(
                    "INSERT INTO dufang_share_progress("
                    "operation_id,ordinal,target_id,target_name,updated_at) VALUES(?,?,?,?,?)",
                    (
                        (operation_id, ordinal, target_id, target_name, occurred_at)
                        for ordinal, (target_id, target_name) in enumerate(normalized)
                    ),
                )
            elif str(previous["source_id"]) != source_id:
                return self._result(uow, operation_id, "operation_conflict")

        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            pending = uow.query_all(
                "SELECT target_id FROM dufang_share_progress "
                "WHERE operation_id=? AND status='pending' ORDER BY ordinal LIMIT ?",
                (operation_id, chunk_size),
            )

        applied_now: set[str] = set()
        for pending_row in pending:
            target_id = str(pending_row["target_id"])
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                if not self._schema_ready(uow):
                    return DufangShareSettlementResult("schema_missing")
                progress = uow.query_one(
                    "SELECT target_name,status FROM dufang_share_progress "
                    "WHERE operation_id=? AND target_id=?",
                    (operation_id, target_id),
                )
                if progress is None or str(progress["status"]) != "pending":
                    continue
                operation = uow.query_one(
                    "SELECT source_id,event_type,event_title,effect_amount "
                    "FROM dufang_share_operations WHERE operation_id=?",
                    (operation_id,),
                )
                frozen_source = str(operation["source_id"])
                frozen_type = str(operation["event_type"])
                frozen_title = str(operation["event_title"])
                frozen_amount = int(operation["effect_amount"])
                source = uow.query_one(
                    "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?",
                    (frozen_source,),
                )
                target = uow.query_one(
                    "SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?",
                    (target_id,),
                )
                if source is None or target is None:
                    reason = "source_missing" if source is None else "target_missing"
                    changed = uow.execute(
                        "UPDATE dufang_share_progress SET status='skipped',reason=?,updated_at=? "
                        "WHERE operation_id=? AND target_id=? AND status='pending'",
                        (reason, occurred_at, operation_id, target_id),
                    ).rowcount
                    if changed != 1:
                        raise RuntimeError("sharing progress state changed")
                    uow.execute(
                        "UPDATE dufang_share_operations SET completed=completed+1,"
                        "status=CASE WHEN completed+1>=total THEN 'completed' ELSE 'running' END,updated_at=? "
                        "WHERE operation_id=?",
                        (occurred_at, operation_id),
                    )
                    continue

                previous_stone = int(target["stone"])
                actual_amount = (
                    frozen_amount if frozen_type == "profit" else min(frozen_amount, previous_stone)
                )
                if actual_amount <= 0:
                    changed = uow.execute(
                        "UPDATE dufang_share_progress SET status='skipped',reason='zero_balance',"
                        "wallet_stone=?,updated_at=? WHERE operation_id=? AND target_id=? AND status='pending'",
                        (previous_stone, occurred_at, operation_id, target_id),
                    ).rowcount
                    if changed != 1:
                        raise RuntimeError("sharing progress state changed")
                    uow.execute(
                        "UPDATE dufang_share_operations SET completed=completed+1,"
                        "status=CASE WHEN completed+1>=total THEN 'completed' ELSE 'running' END,updated_at=? "
                        "WHERE operation_id=?",
                        (occurred_at, operation_id),
                    )
                    continue

                stone_delta = actual_amount if frozen_type == "profit" else -actual_amount
                if uow.execute(
                    "UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? WHERE user_id=?",
                    (stone_delta, target_id),
                ).rowcount != 1:
                    raise RuntimeError("sharing target state changed")
                wallet_stone = previous_stone + stone_delta
                detail = json.dumps(
                    {
                        "source_id": frozen_source,
                        "event_title": frozen_title,
                        "requested_amount": frozen_amount,
                        "previous_stone": previous_stone,
                        "final_stone": wallet_stone,
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                uow.execute(
                    "INSERT INTO economy_log(user_id,source,action,stone_delta,item_delta,detail,trace_id,created_at) "
                    "VALUES(?,'dufang',?,?,'[]',?,?,?)",
                    (target_id, f"dufang_share_{frozen_type}", stone_delta, detail, operation_id, occurred_at),
                )
                changed = uow.execute(
                    "UPDATE dufang_share_progress SET status='applied',actual_amount=?,wallet_stone=?,updated_at=? "
                    "WHERE operation_id=? AND target_id=? AND status='pending'",
                    (actual_amount, wallet_stone, occurred_at, operation_id, target_id),
                ).rowcount
                if changed != 1:
                    raise RuntimeError("sharing progress state changed")
                uow.execute(
                    "UPDATE dufang_share_operations SET completed=completed+1,total_amount=total_amount+?,"
                    "status=CASE WHEN completed+1>=total THEN 'completed' ELSE 'running' END,updated_at=? "
                    "WHERE operation_id=?",
                    (actual_amount, occurred_at, operation_id),
                )
                applied_now.add(target_id)
            self._sync_player_statistics(operation_id, source_id)

        self._sync_player_statistics(operation_id, source_id)
        with DatabaseUnitOfWork(self.game_database) as uow:
            return self._result(
                uow,
                operation_id,
                "applied" if applied_now else "duplicate",
                applied_now,
            )

    def exists(self, operation_id: str) -> bool:
        if not self.game_database.is_file():
            return False
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            if not {"operation_id"}.issubset(self._columns(uow, "dufang_share_operations")):
                return False
            return uow.query_one(
                "SELECT 1 AS present FROM dufang_share_operations WHERE operation_id=?",
                (str(operation_id).strip(),),
            ) is not None

    def _sync_player_statistics(self, operation_id: str, source_id: str) -> None:
        with DatabaseUnitOfWork(self.game_database) as game_uow:
            operation = game_uow.query_one(
                "SELECT source_id,event_type FROM dufang_share_operations WHERE operation_id=?",
                (operation_id,),
            )
            if operation is None or str(operation["source_id"]) != source_id:
                raise RuntimeError("sharing operation is missing or has a different source")
            event_type = str(operation["event_type"])
            rows = game_uow.query_all(
                "SELECT target_id,actual_amount,updated_at FROM dufang_share_progress "
                "WHERE operation_id=? AND status='applied' ORDER BY ordinal",
                (operation_id,),
            )
        received_field = "received_profit" if event_type == "profit" else "received_loss"
        shared_field = "shared_profit" if event_type == "profit" else "shared_loss"
        for row in rows:
            target_id, amount = str(row["target_id"]), int(row["actual_amount"])
            updated_at = str(row["updated_at"])
            with DatabaseUnitOfWork(self.player_database, immediate=True) as player_uow:
                if not self._player_schema_ready(player_uow):
                    raise RuntimeError("dufang player schema is not ready")
                receipt = player_uow.query_one(
                    "SELECT source_id,event_type,amount FROM dufang_share_player_receipts "
                    "WHERE operation_id=? AND target_id=?",
                    (operation_id, target_id),
                )
                if receipt is not None:
                    if (
                        str(receipt["source_id"]) != source_id
                        or str(receipt["event_type"]) != event_type
                        or int(receipt["amount"]) != amount
                    ):
                        raise RuntimeError("sharing player receipt conflicts with frozen progress")
                    continue
                player_uow.execute(
                    f"INSERT INTO unseal_data(user_id,{received_field},last_update) VALUES(?,?,?) "
                    f"ON CONFLICT(user_id) DO UPDATE SET {received_field}="
                    f"COALESCE({received_field},0)+excluded.{received_field},last_update=excluded.last_update",
                    (target_id, amount, updated_at),
                )
                player_uow.execute(
                    f"INSERT INTO unseal_data(user_id,{shared_field},last_update) VALUES(?,?,?) "
                    f"ON CONFLICT(user_id) DO UPDATE SET {shared_field}="
                    f"COALESCE({shared_field},0)+excluded.{shared_field},last_update=excluded.last_update",
                    (source_id, amount, updated_at),
                )
                player_uow.execute(
                    "INSERT INTO dufang_share_player_receipts("
                    "operation_id,target_id,source_id,event_type,amount,updated_at) VALUES(?,?,?,?,?,?)",
                    (operation_id, target_id, source_id, event_type, amount, updated_at),
                )

    def resume(
        self, *, operation_id: str, source_id: str, occurred_at: str = ""
    ) -> DufangShareSettlementResult:
        operation_id, source_id = str(operation_id).strip(), str(source_id).strip()
        if not self.game_database.is_file() or not self.player_database.is_file():
            return DufangShareSettlementResult("schema_missing")
        with DatabaseUnitOfWork(self.game_database) as uow:
            if not {"operation_id", "source_id"}.issubset(self._columns(uow, "dufang_share_operations")):
                return DufangShareSettlementResult("schema_missing")
            operation = uow.query_one(
                "SELECT source_id,event_type,event_title,event_description,effect_amount,bonus_percent,created_at "
                "FROM dufang_share_operations WHERE operation_id=?",
                (operation_id,),
            )
            if operation is None:
                return DufangShareSettlementResult("not_found")
            if str(operation["source_id"]) != source_id:
                return self._result(uow, operation_id, "operation_conflict")
            recipients = tuple(
                (str(row["target_id"]), str(row["target_name"]))
                for row in uow.query_all(
                    "SELECT target_id,target_name FROM dufang_share_progress "
                    "WHERE operation_id=? ORDER BY ordinal",
                    (operation_id,),
                )
            )
        if not recipients:
            return DufangShareSettlementResult("not_found")
        return self.settle(
            operation_id=operation_id,
            source_id=source_id,
            event_type=str(operation["event_type"]),
            event_title=str(operation["event_title"]),
            event_description=str(operation["event_description"]),
            effect_amount=int(operation["effect_amount"]),
            bonus_percent=int(operation["bonus_percent"]),
            recipients=recipients,
            occurred_at=str(occurred_at).strip() or str(operation["created_at"]),
        )


__all__ = ["DufangShareRecipientResult", "DufangShareSettlementResult", "DufangShareSqlRepository"]
