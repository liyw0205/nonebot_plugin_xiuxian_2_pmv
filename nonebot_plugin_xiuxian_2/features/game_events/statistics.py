from __future__ import annotations

from pathlib import Path

from ...core.errors import OperationConflictError
from ...infrastructure.database import DatabaseUnitOfWork


class GameEventStatisticsRepository:
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
        if not event_id or not increments:
            raise ValueError("game event statistics require an event ID and increments")
        normalized = {str(key): int(value) for key, value in increments.items()}
        if any(not key or value < 0 for key, value in normalized.items()):
            raise ValueError("game event statistics increments must be nonnegative")

        wrote = False
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            columns = {
                str(row["name"]): int(row["pk"])
                for row in uow.query_all('PRAGMA table_info("statistics")')
            }
            if not columns.get("user_id") or not set(normalized) <= set(columns):
                raise RuntimeError("game_events.001 schema_missing: statistics")
            receipt_columns = {
                str(row["name"])
                for row in uow.query_all('PRAGMA table_info("game_event_statistics_events")')
            }
            required = {"event_id", "event_key", "user_id", "increment", "created_at"}
            if not required <= receipt_columns:
                raise RuntimeError("game_events.001 schema_missing: game_event_statistics_events")

            for event_key, increment in normalized.items():
                prior = uow.query_one(
                    "SELECT user_id,increment FROM game_event_statistics_events "
                    "WHERE event_id=? AND event_key=?",
                    (str(event_id), event_key),
                )
                if prior is not None:
                    same_increment = (
                        float(prior["increment"]) == float(increment) if increment > 2**63 - 1
                        else int(prior["increment"]) == increment
                    )
                    if str(prior["user_id"]) != str(user_id) or not same_increment:
                        raise OperationConflictError(str(event_id), "game_event.statistics")
                    continue
                wrote = True
                uow.execute(
                    "INSERT INTO game_event_statistics_events "
                    "(event_id,event_key,user_id,increment,created_at) VALUES(?,?,?,?,?)",
                    (str(event_id), event_key, str(user_id), str(increment), str(occurred_at)),
                )
                uow.execute(
                    f'INSERT INTO statistics(user_id,"{event_key}") VALUES (?,?) '
                    f'ON CONFLICT(user_id) DO UPDATE SET "{event_key}"='
                    f'COALESCE(statistics."{event_key}",0)+excluded."{event_key}"',
                    (str(user_id), str(increment)),
                )
        return wrote
