from __future__ import annotations
from ...core.numeric import as_int_like

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ..base.public_display_snapshot import (
    shared_display_snapshot,
    shared_display_snapshot_with_live_value,
)
from ..base.ranking_position import PersonalRank
from ..base.schema_catalog import sqlite_table_columns


TOWER_FIELDS = ("current_floor", "max_floor", "score", "weekly_purchases")


@dataclass(frozen=True)
class TowerFloorResetResult:
    status: str
    total: int = 0
    changed: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class TowerStateRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    @staticmethod
    def _period_key(today: date) -> str:
        iso = today.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"

    @staticmethod
    def _default(today: date) -> dict[str, Any]:
        return {
            "current_floor": 0,
            "max_floor": 0,
            "score": 0,
            "weekly_purchases": {"_last_reset": today.isoformat()},
        }

    @staticmethod
    def _weekly(value: Any, today: date) -> tuple[dict[str, int | str], bool]:
        changed = False
        if isinstance(value, str):
            try:
                value = json.loads(value) if value else {}
            except (TypeError, ValueError):
                value, changed = {}, True
        if not isinstance(value, dict):
            value, changed = {}, True
        try:
            reset = date.fromisoformat(str(value.get("_last_reset", "")))
        except (TypeError, ValueError):
            reset = None
        if reset is None or reset.isocalendar()[:2] != today.isocalendar()[:2]:
            return {"_last_reset": today.isoformat()}, True
        weekly: dict[str, int | str] = {"_last_reset": reset.isoformat()}
        for raw_key, raw_amount in value.items():
            key = str(raw_key)
            if key == "_last_reset":
                continue
            try:
                amount = int(raw_amount)
            except (TypeError, ValueError):
                changed = True
                continue
            if amount < 0:
                changed = True
                continue
            weekly[key] = amount
            if key != raw_key or not isinstance(raw_amount, int) or isinstance(raw_amount, bool):
                changed = True
        return weekly, changed

    def initialize(self, user_id: str, today: date) -> dict[str, Any]:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            row = uow.query_one(
                "SELECT current_floor,max_floor,score,weekly_purchases FROM tower WHERE user_id=?",
                (user_id,),
            )
            if row is not None:
                result = dict(row)
                result["current_floor"] = as_int_like(result["current_floor"])
                result["max_floor"] = as_int_like(result["max_floor"])
                result["score"] = as_int_like(result["score"])
                weekly, weekly_changed = self._weekly(result["weekly_purchases"], today)
                result["weekly_purchases"] = weekly
                if weekly_changed:
                    period_key = self._period_key(today)
                    uow.execute(
                        "UPDATE tower SET weekly_purchases=? WHERE user_id=?",
                        (json.dumps(weekly, ensure_ascii=True, sort_keys=True), user_id),
                    )
                    uow.execute(
                        "INSERT INTO tower_state_operations(operation_id,user_id,kind,period_key,snapshot,created_at) "
                        "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)",
                        (
                            f"tower-state-week:{user_id}:{period_key}",
                            user_id,
                            "week",
                            period_key,
                            json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
                        ),
                    )
                return result
            state = self._default(today)
            uow.execute(
                "INSERT INTO tower(user_id,current_floor,max_floor,score,weekly_purchases) VALUES(?,?,?,xiuxian_int_store(?,'tower_score','actor'),?)",
                (
                    user_id,
                    state["current_floor"],
                    state["max_floor"],
                    state["score"],
                    json.dumps(state["weekly_purchases"], ensure_ascii=True, sort_keys=True),
                ),
            )
            uow.execute(
                "INSERT INTO tower_state_operations(operation_id,user_id,kind,period_key,snapshot,created_at) "
                "VALUES(?,?,?,?,?,CURRENT_TIMESTAMP)",
                (
                    f"tower-state-init:{user_id}",
                    user_id,
                    "initialize",
                    self._period_key(today),
                    json.dumps(state, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
                ),
            )
            return state

    def read(self, user_id: str, today: date) -> dict[str, Any]:
        """Read the display projection without initializing or persisting repairs."""
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT current_floor,max_floor,score,weekly_purchases FROM tower WHERE user_id=?",
                (user_id,),
            )
            if row is None:
                return self._default(today)
            result = dict(row)
            result["current_floor"] = as_int_like(result["current_floor"])
            result["max_floor"] = as_int_like(result["max_floor"])
            result["score"] = as_int_like(result["score"])
            weekly, _ = self._weekly(result["weekly_purchases"], today)
            result["weekly_purchases"] = weekly
            return result

    def reset_all_floors(
        self, *, operation_id: str, source: str, period_key: str
    ) -> TowerFloorResetResult:
        operation_id, source, period_key = (
            str(operation_id).strip(), str(source).strip(), str(period_key).strip()
        )
        if not operation_id or source not in {"admin", "scheduler"} or not period_key:
            raise ValueError("valid tower reset identity is required")
        if not Path(self.player_database).is_file():
            return TowerFloorResetResult("schema_missing")

        required = {
            "tower": {"user_id", "current_floor"},
            "tower_state_operations": {
                "operation_id", "user_id", "kind", "period_key", "snapshot"
            },
        }
        request = {"source": source, "period_key": period_key}
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            for table, columns in required.items():
                actual = {
                    str(row["name"])
                    for row in uow.query_all(f"PRAGMA table_info({table})")
                }
                if not columns <= actual:
                    return TowerFloorResetResult("schema_missing")

            previous = uow.query_one(
                "SELECT user_id,kind,period_key,snapshot FROM tower_state_operations "
                "WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                try:
                    saved = json.loads(str(previous["snapshot"] or "{}"))
                except (TypeError, ValueError):
                    return TowerFloorResetResult("operation_conflict")
                if (
                    not isinstance(saved, dict)
                    or str(previous["user_id"]) != "0"
                    or str(previous["kind"]) != "reset_all_floors"
                    or str(previous["period_key"]) != period_key
                    or saved.get("request") != request
                ):
                    return TowerFloorResetResult("operation_conflict")
                result = saved.get("result")
                if not isinstance(result, dict):
                    return TowerFloorResetResult("operation_conflict")
                total_value = result.get("total")
                changed_value = result.get("changed")
                if (
                    not isinstance(total_value, int)
                    or isinstance(total_value, bool)
                    or not isinstance(changed_value, int)
                    or isinstance(changed_value, bool)
                    or total_value < 0
                    or changed_value < 0
                    or changed_value > total_value
                ):
                    return TowerFloorResetResult("operation_conflict")
                return TowerFloorResetResult(
                    "duplicate", total_value, changed_value
                )

            total_row = uow.query_one("SELECT COUNT(*) AS total FROM tower")
            total = int(total_row["total"] if total_row is not None else 0)
            changed = uow.execute(
                "UPDATE tower SET current_floor=0 "
                "WHERE current_floor IS NULL OR current_floor<>0"
            ).rowcount
            result = {"total": total, "changed": int(changed)}
            snapshot = json.dumps(
                {"request": request, "result": result},
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            uow.execute(
                "INSERT INTO tower_state_operations(operation_id,user_id,kind,period_key,snapshot) "
                "VALUES(?,?,?,?,?)",
                (operation_id, "0", "reset_all_floors", period_key, snapshot),
            )
            return TowerFloorResetResult("applied", total, int(changed))

    def ranking(self, field: str, limit: int = 50) -> list[tuple[str, int]]:
        if field not in {"current_floor", "score"}:
            raise ValueError("unsupported tower ranking field")
        limit = min(50, max(0, int(limit)))
        if limit == 0 or not Path(self.player_database).is_file():
            return []
        with DatabaseUnitOfWork(self.player_database) as uow:
            columns = sqlite_table_columns(uow, self.player_database, "tower")
            if not {"user_id", field} <= columns:
                return []
            rows = uow.query_all(
                f'SELECT user_id,COALESCE("{field}",0) AS value '
                f'FROM tower ORDER BY xiuxian_int_text(COALESCE("{field}",0)) COLLATE xiuxian_integer DESC,user_id ASC LIMIT ?',
                (limit,),
            )
        return [(str(row["user_id"]), as_int_like(row["value"])) for row in rows]

    def ranking_snapshot(self, field: str, limit: int = 50) -> dict[str, Any]:
        if field not in {"current_floor", "score"}:
            raise ValueError("unsupported tower ranking field")
        limit = min(50, max(0, int(limit)))
        return shared_display_snapshot(
            self.player_database,
            "tower-rank-v1",
            field,
            limit,
            loader=lambda: self.ranking(field, limit),
        )

    def ranking_snapshot_with_rank(
        self, user_id: str, field: str = "current_floor", limit: int = 50
    ) -> tuple[dict[str, Any], PersonalRank]:
        """Share a cold display read with the exact rank, but never cache that rank."""
        if field not in {"current_floor", "score"}:
            raise ValueError("unsupported tower ranking field")
        normalized_limit = min(50, max(0, int(limit)))
        user_id = str(user_id).strip()
        if not user_id:
            return self.ranking_snapshot(field, normalized_limit), PersonalRank()

        return shared_display_snapshot_with_live_value(
            self.player_database,
            "tower-rank-v1",
            field,
            normalized_limit,
            loader=lambda: self._ranking_and_rank(field, normalized_limit, user_id),
            read_live=lambda: self.rank_of(user_id, field),
        )

    def _ranking_and_rank(
        self, field: str, limit: int, user_id: str
    ) -> tuple[list[tuple[str, int]], PersonalRank]:
        if not Path(self.player_database).is_file():
            return [], PersonalRank()
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            columns = sqlite_table_columns(uow, self.player_database, "tower")
            if not {"user_id", field} <= columns:
                return [], PersonalRank()
            rows = uow.query_all(
                f'SELECT user_id,COALESCE("{field}",0) AS value '
                f'FROM tower ORDER BY xiuxian_int_text(COALESCE("{field}",0)) COLLATE xiuxian_integer DESC,user_id ASC LIMIT ?',
                (limit,),
            )
            rank = self._rank_in_uow(uow, user_id, field)
        return [(str(row["user_id"]), as_int_like(row["value"])) for row in rows], rank

    def rank_of(self, user_id: str, field: str = "current_floor") -> PersonalRank:
        """本人通天塔名次：并列名次 + 有记录人数 + 本人数值。"""
        if field not in {"current_floor", "score"}:
            raise ValueError("unsupported tower ranking field")
        user_id = str(user_id).strip()
        if not user_id or not Path(self.player_database).is_file():
            return PersonalRank()
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            columns = sqlite_table_columns(uow, self.player_database, "tower")
            if not {"user_id", field} <= columns:
                return PersonalRank()
            return self._rank_in_uow(uow, user_id, field)

    @staticmethod
    def _rank_in_uow(
        uow: DatabaseUnitOfWork, user_id: str, field: str
    ) -> PersonalRank:
        row = uow.query_one(
            f'WITH target AS (SELECT COALESCE((SELECT "{field}" FROM tower '
            "WHERE user_id=? ORDER BY rowid ASC LIMIT 1),0) AS value) "
            f'SELECT target.value AS value, '
            f'COUNT(CASE WHEN xiuxian_int_cmp(COALESCE(t."{field}",0),0)>0 THEN 1 END) AS total, '
            f'COUNT(CASE WHEN target.value>0 AND '
            f'xiuxian_int_cmp(COALESCE(t."{field}",0),target.value)>0 THEN 1 END) AS better '
            "FROM tower AS t CROSS JOIN target",
            (user_id,),
        )
        value = as_int_like((row or {}).get("value"))
        total = int((row or {}).get("total") or 0)
        better = int((row or {}).get("better") or 0)
        if value <= 0:
            return PersonalRank(0, total, 0)
        return PersonalRank(better + 1, total, value)


__all__ = ["TOWER_FIELDS", "TowerFloorResetResult", "TowerStateRepository"]
