from __future__ import annotations

from pathlib import Path

from ...core.errors import OperationConflictError
from ...infrastructure.database import DatabaseUnitOfWork


class ClosingStatisticsRepository:
    _METRICS = ("闭关时长", "闭关修为", "闭关灵石消耗")

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def record(
        self,
        *,
        event_id: str,
        user_id: str,
        increments: dict[str, int],
        occurred_at: str,
    ) -> bool:
        if set(increments) != set(self._METRICS):
            raise ValueError("closing statistics metrics are incomplete")
        if any(int(increments[key]) < 0 for key in self._METRICS):
            raise ValueError("closing statistics increments must be nonnegative")
        wrote = False
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            columns = {
                str(row["name"])
                for row in uow.query_all("PRAGMA table_info(statistics)")
            }
            if "user_id" not in columns or not set(self._METRICS) <= columns:
                raise RuntimeError("buff.009 schema_missing: statistics")
            receipt_columns = {
                str(row["name"])
                for row in uow.query_all("PRAGMA table_info(closing_statistics_events)")
            }
            if not {"event_id", "event_key", "user_id", "increment", "created_at"} <= receipt_columns:
                raise RuntimeError("buff.009 schema_missing: closing_statistics_events")
            for event_key in self._METRICS:
                increment = int(increments[event_key])
                prior = uow.query_one(
                    "SELECT user_id,increment FROM closing_statistics_events "
                    "WHERE event_id=? AND event_key=?",
                    (str(event_id), event_key),
                )
                if prior is not None:
                    if str(prior["user_id"]) != str(user_id) or int(prior["increment"]) != increment:
                        raise OperationConflictError(str(event_id), "buff.closing.statistics")
                    continue
                wrote = True
                uow.execute(
                    "INSERT INTO closing_statistics_events(event_id,event_key,user_id,increment,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (str(event_id), event_key, str(user_id), increment, str(occurred_at)),
                )
                uow.execute(
                    f'INSERT INTO statistics(user_id,"{event_key}") VALUES (?,?) '
                    f'ON CONFLICT(user_id) DO UPDATE SET "{event_key}"='
                    f'COALESCE(statistics."{event_key}",0)+excluded."{event_key}"',
                    (str(user_id), increment),
                )
        return wrote


__all__ = ["ClosingStatisticsRepository"]
