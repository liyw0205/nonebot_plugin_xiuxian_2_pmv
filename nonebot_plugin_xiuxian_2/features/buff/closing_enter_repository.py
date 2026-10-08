"""Persistence boundary for entering normal cultivation retreat (``闭关``)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.database import AttachedDatabaseUnitOfWork


@dataclass(frozen=True)
class ClosingEnterResult:
    status: str
    started_at: str = ""
    entry_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

    def as_data(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "started_at": self.started_at,
            "entry_count": int(self.entry_count),
        }


class ClosingEnterSqlRepository:
    """Atomically set ``user_cd.type`` and increment the player statistic.

    The caller normally owns the attached unit of work so the operation ledger,
    game state, player projection, and feature receipt commit together.  The
    repository deliberately never creates request tables at call time.
    """

    OPERATION_COLUMNS = {"operation_id", "payload", "result_json"}
    GAME_USER_COLUMNS = {"user_id", "root_type"}
    GAME_CD_COLUMNS = {"user_id", "type", "create_time", "scheduled_time"}
    PLAYER_STATISTICS_COLUMNS = {"user_id", "闭关次数"}

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)
        self.clock = clock

    @staticmethod
    def _columns(uow: AttachedDatabaseUnitOfWork, table: str, schema: str = "main") -> set[str]:
        return {
            str(row["name"])
            for row in uow.query_all(f'PRAGMA {schema}.table_info("{table}")')
        }

    @classmethod
    def schema_ready(cls, uow: AttachedDatabaseUnitOfWork) -> bool:
        operation_rows = uow.query_all(
            'PRAGMA main.table_info("closing_enter_operations")'
        )
        statistics_rows = uow.query_all(
            'PRAGMA player_data.table_info("statistics")'
        )
        return (
            cls.OPERATION_COLUMNS.issubset(
                {str(row["name"]) for row in operation_rows}
            )
            and any(
                str(row["name"]) == "operation_id" and int(row["pk"] or 0) == 1
                for row in operation_rows
            )
            and cls.GAME_USER_COLUMNS.issubset(cls._columns(uow, "user_xiuxian"))
            and cls.GAME_CD_COLUMNS.issubset(cls._columns(uow, "user_cd"))
            and cls.PLAYER_STATISTICS_COLUMNS.issubset(
                {str(row["name"]) for row in statistics_rows}
            )
            and any(
                str(row["name"]) == "user_id" and int(row["pk"] or 0) == 1
                for row in statistics_rows
            )
        )

    @staticmethod
    def _payload(user_id: str) -> str:
        # ``started_at`` is an outcome, not request identity: retries can use a
        # fresh clock value while retaining the same operation receipt.
        return json.dumps([str(user_id)], ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _saved_result(value: Any, status: str = "duplicate") -> ClosingEnterResult:
        try:
            saved = json.loads(str(value))
        except (TypeError, ValueError):
            return ClosingEnterResult("operation_conflict")
        if isinstance(saved, dict):
            return ClosingEnterResult(
                status,
                str(saved.get("started_at") or ""),
                int(saved.get("entry_count") or 0),
            )
        if isinstance(saved, (list, tuple)) and len(saved) >= 2:
            return ClosingEnterResult(status, str(saved[0]), int(saved[1]))
        return ClosingEnterResult("operation_conflict")

    def enter_in_uow(
        self,
        uow: AttachedDatabaseUnitOfWork,
        operation_id: str,
        user_id: str,
        started_at: str,
    ) -> ClosingEnterResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        started_at = str(started_at).strip()
        if not operation_id or not user_id or not started_at:
            raise ValueError("operation, user and start time are required")
        if not self.schema_ready(uow):
            return ClosingEnterResult("schema_missing")

        payload = self._payload(user_id)
        previous = uow.query_one(
            "SELECT payload,result_json FROM closing_enter_operations WHERE operation_id=?",
            (operation_id,),
        )
        if previous is not None:
            if str(previous["payload"]) != payload:
                return ClosingEnterResult("operation_conflict")
            return self._saved_result(previous["result_json"])

        user = uow.query_one(
            "SELECT root_type FROM user_xiuxian WHERE user_id=? ORDER BY rowid LIMIT 1",
            (user_id,),
        )
        cd = uow.query_one(
            "SELECT COALESCE(type,0) AS type FROM user_cd WHERE user_id=? ORDER BY rowid LIMIT 1",
            (user_id,),
        )
        if user is None or cd is None:
            return ClosingEnterResult("user_missing")
        if str(user["root_type"] or "") == "伪灵根":
            return ClosingEnterResult("ineligible")
        if int(cd["type"] or 0) != 0:
            return ClosingEnterResult("busy")

        changed = uow.execute(
            "UPDATE user_cd SET type=1,create_time=?,scheduled_time=NULL "
            "WHERE rowid=(SELECT rowid FROM user_cd WHERE user_id=? "
            "AND COALESCE(type,0)=0 ORDER BY rowid LIMIT 1) "
            "AND COALESCE(type,0)=0",
            (started_at, user_id),
        )
        if changed.rowcount != 1:
            return ClosingEnterResult("state_changed")

        # Migration-owned statistics schema gives this statement a stable
        # unique key, allowing a first entry for players without a row yet.
        uow.execute(
            'INSERT INTO player_data.statistics(user_id,"闭关次数") VALUES(?,1) '
            'ON CONFLICT(user_id) DO UPDATE SET "闭关次数"='
            'COALESCE(player_data.statistics."闭关次数",0)+1',
            (user_id,),
        )
        stat = uow.query_one(
            'SELECT COALESCE("闭关次数",0) AS entry_count '
            "FROM player_data.statistics WHERE user_id=?",
            (user_id,),
        )
        if stat is None:
            return ClosingEnterResult("state_changed")
        entry_count = int(stat["entry_count"] or 0)
        result_json = json.dumps(
            {"started_at": started_at, "entry_count": entry_count},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        uow.execute(
            "INSERT INTO closing_enter_operations"
            "(operation_id,payload,result_json) VALUES(?,?,?)",
            (operation_id, payload, result_json),
        )
        return ClosingEnterResult("applied", started_at, entry_count)

    def enter(self, operation_id: str, user_id: str, started_at: str) -> ClosingEnterResult:
        """Convenience adapter for direct repository callers and tests."""
        if not self.game_database.is_file() or not self.player_database.is_file():
            return ClosingEnterResult("schema_missing")
        try:
            with AttachedDatabaseUnitOfWork(
                self.game_database,
                attachments={"player_data": self.player_database},
                immediate=True,
            ) as uow:
                return self.enter_in_uow(uow, operation_id, user_id, started_at)
        except (FileNotFoundError, OSError):
            return ClosingEnterResult("schema_missing")


__all__ = ["ClosingEnterResult", "ClosingEnterSqlRepository"]
