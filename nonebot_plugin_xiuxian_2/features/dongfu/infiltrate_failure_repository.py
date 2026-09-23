from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class _InfiltrateFailureStateChanged(Exception):
    pass


@dataclass(frozen=True)
class DongfuInfiltrateFailureResult:
    status: str
    infiltrate_left: int = 0
    intrude_left: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


class DongfuInfiltrateFailureSqlRepository:
    """Atomically persist one detected and failed cave-dwelling infiltration."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def settle(
        self,
        operation_id: str,
        visitor_id: str,
        target_id: str,
        day: str,
        mode_field: str,
        mode_limit: int,
        target_limit: int,
        loss: int,
        consume_guard: bool,
    ) -> DongfuInfiltrateFailureResult:
        operation_id = str(operation_id).strip()
        visitor_id, target_id, day, mode_field = map(
            str, (visitor_id, target_id, day, mode_field)
        )
        mode_limit, target_limit, loss = map(int, (mode_limit, target_limit, loss))
        consume_guard = int(bool(consume_guard))
        if (
            not operation_id
            or visitor_id == target_id
            or mode_field not in {"infiltrate_active_count", "infiltrate_random_count"}
            or mode_limit < 1
            or target_limit < 1
            or loss < 0
        ):
            raise ValueError("valid infiltration failure operation is required")
        payload = "|".join(
            map(
                str,
                (
                    visitor_id,
                    target_id,
                    day,
                    mode_field,
                    mode_limit,
                    target_limit,
                    loss,
                    consume_guard,
                ),
            )
        )

        try:
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                uow.attach_database(self.player_database, "player_data")
                previous = uow.query_one(
                    "SELECT payload,infiltrate_left,intrude_left "
                    "FROM dongfu_infiltrate_failure_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if previous is not None:
                    if str(previous["payload"]) != payload:
                        return DongfuInfiltrateFailureResult("state_changed")
                    return DongfuInfiltrateFailureResult(
                        "duplicate",
                        int(previous["infiltrate_left"]),
                        int(previous["intrude_left"]),
                    )

                user = uow.query_one(
                    "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (visitor_id,)
                )
                visitor = uow.query_one(
                    f"SELECT built,infiltrate_date,{mode_field} "
                    "FROM player_data.dongfu_status WHERE user_id=?",
                    (visitor_id,),
                )
                target = uow.query_one(
                    "SELECT built,intrude_date,intrude_count,patrol_guard "
                    "FROM player_data.dongfu_status WHERE user_id=?",
                    (target_id,),
                )
                if (
                    user is None
                    or visitor is None
                    or target is None
                    or int(visitor["built"] or 0) != 1
                    or int(target["built"] or 0) != 1
                ):
                    return DongfuInfiltrateFailureResult("state_changed")

                mode_count = (
                    int(visitor[mode_field] or 0)
                    if str(visitor["infiltrate_date"] or "") == day
                    else 0
                )
                intrude_count = (
                    int(target["intrude_count"] or 0)
                    if str(target["intrude_date"] or "") == day
                    else 0
                )
                if mode_count >= mode_limit or intrude_count >= target_limit:
                    return DongfuInfiltrateFailureResult(
                        "daily_limit",
                        max(0, mode_limit - mode_count),
                        max(0, target_limit - intrude_count),
                    )

                mode_count += 1
                intrude_count += 1
                visitor_update = uow.execute(
                    f"UPDATE player_data.dongfu_status SET infiltrate_date=?,{mode_field}=? "
                    "WHERE user_id=?",
                    (day, mode_count, visitor_id),
                )
                target_update = uow.execute(
                    "UPDATE player_data.dongfu_status "
                    "SET intrude_date=?,intrude_count=?,patrol_guard=MAX(patrol_guard-?,0) "
                    "WHERE user_id=?",
                    (day, intrude_count, consume_guard, target_id),
                )
                stone_update = uow.execute(
                    "UPDATE user_xiuxian SET "
                    "stone=MAX(CAST(COALESCE(stone,0) AS REAL)-CAST(? AS REAL),0) "
                    "WHERE user_id=?",
                    (loss, visitor_id),
                )
                if (
                    visitor_update.rowcount != 1
                    or target_update.rowcount != 1
                    or stone_update.rowcount != 1
                ):
                    raise _InfiltrateFailureStateChanged

                infiltrate_left = max(0, mode_limit - mode_count)
                intrude_left = max(0, target_limit - intrude_count)
                uow.execute(
                    "INSERT INTO dongfu_infiltrate_failure_operations("
                    "operation_id,payload,infiltrate_left,intrude_left) VALUES(?,?,?,?)",
                    (operation_id, payload, infiltrate_left, intrude_left),
                )
                return DongfuInfiltrateFailureResult(
                    "settled", infiltrate_left, intrude_left
                )
        except _InfiltrateFailureStateChanged:
            return DongfuInfiltrateFailureResult("state_changed")


__all__ = ["DongfuInfiltrateFailureResult", "DongfuInfiltrateFailureSqlRepository"]
