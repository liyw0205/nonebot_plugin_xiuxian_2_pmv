from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...core.numeric import require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork


class _InfiltrateFailureStateChanged(Exception):
    def __init__(self, reason: str, subject: str):
        self.reason, self.subject = reason, subject


@dataclass(frozen=True)
class DongfuInfiltrateFailureResult:
    status: str
    infiltrate_left: int = 0
    intrude_left: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


class DongfuInfiltrateFailureSqlRepository:
    """Atomically persist one detected and failed cave-dwelling infiltration."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _reject(suffix: str, subject: str) -> None:
        raise _InfiltrateFailureStateChanged(
            f"dongfu_infiltrate_failure_{suffix}", subject
        )

    @classmethod
    def _verify_player(cls, uow: DatabaseUnitOfWork, user_id: str, expected: dict) -> None:
        row = uow.query_one("SELECT * FROM user_xiuxian WHERE user_id=?", (user_id,))
        if row is not None and row.get("stone") is not None:
            row["stone"] = require_exact_integer(row["stone"], field="stone", subject="actor")
        if row is None or row != expected:
            cls._reject("stone_projection_mismatch", "stone_wallet")

    @classmethod
    def _verify_state(cls, uow: DatabaseUnitOfWork, user_id: str, expected: dict, subject: str) -> None:
        row = uow.query_one(
            "SELECT * FROM player_data.dongfu_status WHERE user_id=?", (user_id,)
        )
        if row is None or row != expected:
            cls._reject(f"{subject}_projection_mismatch", subject)

    @classmethod
    def _verify_receipt(cls, uow: DatabaseUnitOfWork, operation_id: str, expected: dict) -> None:
        row = uow.query_one(
            "SELECT payload,infiltrate_left,intrude_left "
            "FROM dongfu_infiltrate_failure_operations WHERE operation_id=?",
            (operation_id,),
        )
        if row is None or row != expected:
            cls._reject("receipt_projection_mismatch", "receipt")

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
                        return DongfuInfiltrateFailureResult(
                            "state_changed", reason="operation_conflict"
                        )
                    return DongfuInfiltrateFailureResult(
                        "duplicate",
                        int(previous["infiltrate_left"]),
                        int(previous["intrude_left"]),
                    )

                user = uow.query_one(
                    "SELECT * FROM user_xiuxian WHERE user_id=?", (visitor_id,)
                )
                visitor = uow.query_one(
                    "SELECT * FROM player_data.dongfu_status WHERE user_id=?",
                    (visitor_id,),
                )
                target = uow.query_one(
                    "SELECT * FROM player_data.dongfu_status WHERE user_id=?",
                    (target_id,),
                )
                if user is None:
                    return DongfuInfiltrateFailureResult(
                        "state_changed", reason="user_missing", subject="visitor"
                    )
                if visitor is None or int(visitor["built"] or 0) != 1:
                    return DongfuInfiltrateFailureResult(
                        "state_changed", reason="dongfu_missing", subject="visitor"
                    )
                if target is None or int(target["built"] or 0) != 1:
                    return DongfuInfiltrateFailureResult(
                        "state_changed", reason="dongfu_missing", subject="target"
                    )

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
                        reason="infiltrate_limit" if mode_count >= mode_limit else "intrude_limit",
                        subject="visitor" if mode_count >= mode_limit else "target",
                    )

                mode_count += 1
                intrude_count += 1
                expected_user = dict(user)
                expected_user["stone"] = require_exact_integer(
                    user["stone"] if user["stone"] is not None else 0,
                    field="stone",
                    subject="actor",
                ) - loss
                expected_visitor = dict(visitor)
                expected_visitor.update(infiltrate_date=day, **{mode_field: mode_count})
                expected_target = dict(target)
                expected_target.update(
                    intrude_date=day,
                    intrude_count=intrude_count,
                    patrol_guard=max(0, int(target["patrol_guard"] or 0) - consume_guard),
                )

                visitor_update = uow.execute(
                    f"UPDATE player_data.dongfu_status SET infiltrate_date=?,{mode_field}=? "
                    "WHERE user_id=?",
                    (day, mode_count, visitor_id),
                )
                if visitor_update.rowcount != 1:
                    self._reject("visitor_write_rejected", "visitor")
                self._verify_state(uow, visitor_id, expected_visitor, "visitor")

                target_update = uow.execute(
                    "UPDATE player_data.dongfu_status "
                    "SET intrude_date=?,intrude_count=?,patrol_guard=MAX(patrol_guard-?,0) "
                    "WHERE user_id=?",
                    (day, intrude_count, consume_guard, target_id),
                )
                if target_update.rowcount != 1:
                    self._reject("target_write_rejected", "target")
                self._verify_state(uow, target_id, expected_target, "target")

                stone_update = uow.execute(
                    "UPDATE user_xiuxian SET "
                    "stone=xiuxian_int_nonneg(xiuxian_int_sub(COALESCE(stone,0),?,'stone','actor')) "
                    "WHERE user_id=?",
                    (loss, visitor_id),
                )
                if stone_update.rowcount != 1:
                    self._reject("stone_write_rejected", "stone_wallet")
                self._verify_player(uow, visitor_id, expected_user)

                infiltrate_left = max(0, mode_limit - mode_count)
                intrude_left = max(0, target_limit - intrude_count)
                receipt = {
                    "payload": payload,
                    "infiltrate_left": infiltrate_left,
                    "intrude_left": intrude_left,
                }
                receipt_write = uow.execute(
                    "INSERT INTO dongfu_infiltrate_failure_operations("
                    "operation_id,payload,infiltrate_left,intrude_left) VALUES(?,?,?,?)",
                    (operation_id, payload, infiltrate_left, intrude_left),
                )
                if receipt_write.rowcount != 1:
                    self._reject("receipt_write_rejected", "receipt")
                self._verify_receipt(uow, operation_id, receipt)
                self._verify_state(uow, visitor_id, expected_visitor, "visitor")
                self._verify_state(uow, target_id, expected_target, "target")
                self._verify_player(uow, visitor_id, expected_user)
                return DongfuInfiltrateFailureResult(
                    "settled", infiltrate_left, intrude_left
                )
        except _InfiltrateFailureStateChanged as exc:
            return DongfuInfiltrateFailureResult(
                "state_changed", reason=exc.reason, subject=exc.subject
            )


__all__ = ["DongfuInfiltrateFailureResult", "DongfuInfiltrateFailureSqlRepository"]
