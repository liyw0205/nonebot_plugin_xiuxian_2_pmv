from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...core.numeric import as_int_like, require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork
from .operation_schema import operation_databases_ready, operation_schema_ready


@dataclass(frozen=True)
class DongfuArrayUpgradeResult:
    status: str
    level: int = 0
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self):
        return self.status in {"upgraded", "duplicate"}


class _ArrayUpgradeStateChanged(Exception):
    def __init__(self, reason: str, subject: str):
        self.reason, self.subject = reason, subject


class DongfuArrayUpgradeSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path):
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _reject(suffix: str, subject: str) -> None:
        raise _ArrayUpgradeStateChanged(
            f"dongfu_array_{suffix}", subject
        )

    @classmethod
    def _verify_player(cls, uow, user_id: str, expected: dict) -> None:
        row = uow.query_one("SELECT * FROM user_xiuxian WHERE user_id=?", (user_id,))
        if row is not None and row.get("stone") is not None:
            row["stone"] = require_exact_integer(
                row["stone"], field="stone", subject="actor"
            )
        if row is None or row != expected:
            cls._reject("player_projection_mismatch", "stone_wallet")

    @classmethod
    def _verify_inventory(cls, uow, user_id: str, item_id: int, expected: dict) -> None:
        row = uow.query_one(
            "SELECT * FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id)
        )
        if row is None or row != expected:
            cls._reject("inventory_projection_mismatch", f"item:{item_id}")

    @classmethod
    def _verify_state(cls, uow, user_id: str, expected: dict) -> None:
        row = uow.query_one(
            "SELECT * FROM player_data.dongfu_status WHERE user_id=?", (user_id,)
        )
        if row is None or row != expected:
            cls._reject("state_projection_mismatch", "array")

    @classmethod
    def _verify_receipt(cls, uow, operation_id: str, expected: dict) -> None:
        row = uow.query_one(
            "SELECT payload,level FROM dongfu_array_upgrade_operations WHERE operation_id=?",
            (operation_id,),
        )
        if row is None or row != expected:
            cls._reject("receipt_projection_mismatch", "receipt")

    def upgrade(
        self,
        operation_id,
        user_id,
        expected_level,
        next_level,
        stone_cost,
        item_id,
        item_cost,
    ):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_level, next_level, stone_cost, item_id, item_cost = map(
            int, (expected_level, next_level, stone_cost, item_id, item_cost)
        )
        payload = "|".join(
            map(str, (user_id, expected_level, next_level, stone_cost, item_id, item_cost))
        )
        if not operation_databases_ready(self.game_database, self.player_database):
            return DongfuArrayUpgradeResult("schema_missing")

        try:
            with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
                uow.attach_database(self.player_database, "player_data")
                if not operation_schema_ready(uow, "dongfu_array_upgrade_operations"):
                    return DongfuArrayUpgradeResult("schema_missing")
                old = uow.query_one(
                    "SELECT payload,level FROM dongfu_array_upgrade_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if old is not None:
                    if str(old["payload"]) == payload:
                        return DongfuArrayUpgradeResult("duplicate", int(old["level"]))
                    return DongfuArrayUpgradeResult(
                        "state_changed", reason="operation_conflict", subject="receipt"
                    )

                user = uow.query_one(
                    "SELECT * FROM user_xiuxian WHERE user_id=?", (user_id,)
                )
                state = uow.query_one(
                    "SELECT * FROM player_data.dongfu_status WHERE user_id=?", (user_id,)
                )
                if user is None:
                    return DongfuArrayUpgradeResult("user_missing")
                if state is None or int(state["built"] or 0) != 1:
                    return DongfuArrayUpgradeResult("dongfu_missing")
                if int(state["array_level"] or 0) != expected_level:
                    return DongfuArrayUpgradeResult(
                        "state_changed", reason="array_level_changed", subject="array"
                    )
                if as_int_like(user["stone"] or 0) < stone_cost:
                    return DongfuArrayUpgradeResult("stone_insufficient")

                item = None
                if item_cost:
                    item = uow.query_one(
                        "SELECT * FROM back WHERE user_id=? AND goods_id=? AND goods_num>=?",
                        (user_id, item_id, item_cost),
                    )
                    if item is None:
                        return DongfuArrayUpgradeResult("item_insufficient")

                expected_user = dict(user)
                expected_user["stone"] = require_exact_integer(
                    user["stone"], field="stone", subject="actor"
                ) - stone_cost
                changed = uow.execute(
                    "UPDATE user_xiuxian SET "
                    "stone=xiuxian_int_store(xiuxian_int_sub(stone,?,'stone','actor')) "
                    "WHERE user_id=? AND xiuxian_int_cmp(stone,?,1,'stone','actor')>=0",
                    (stone_cost, user_id, stone_cost),
                )
                if changed.rowcount != 1:
                    self._reject("player_write_rejected", "stone_wallet")
                self._verify_player(uow, user_id, expected_user)

                expected_item = None
                if item is not None:
                    expected_item = dict(item)
                    expected_item["goods_num"] = int(item["goods_num"]) - item_cost
                    bind_update = ""
                    if "bind_num" in item:
                        expected_item["bind_num"] = min(
                            max(int(item["bind_num"] or 0) - item_cost, 0),
                            expected_item["goods_num"],
                        )
                        bind_update = (
                            ",bind_num=MIN(MAX(COALESCE(bind_num,0)-?,0),goods_num-?)"
                        )
                        item_params = (item_cost, item_cost)
                    else:
                        item_params = ()
                    consumed = uow.execute(
                        "UPDATE back SET goods_num=goods_num-?"
                        + bind_update
                        + " WHERE user_id=? AND goods_id=? AND goods_num>=?",
                        (item_cost, *item_params, user_id, item_id, item_cost),
                    )
                    if consumed.rowcount != 1:
                        self._reject("inventory_write_rejected", f"item:{item_id}")
                    self._verify_inventory(uow, user_id, item_id, expected_item)

                expected_state = dict(state)
                expected_state["array_level"] = next_level
                changed = uow.execute(
                    "UPDATE player_data.dongfu_status SET array_level=? "
                    "WHERE user_id=? AND array_level=?",
                    (next_level, user_id, expected_level),
                )
                if changed.rowcount != 1:
                    self._reject("state_write_rejected", "array")
                self._verify_state(uow, user_id, expected_state)

                receipt = {"payload": payload, "level": next_level}
                inserted = uow.execute(
                    "INSERT INTO dongfu_array_upgrade_operations(operation_id,payload,level) "
                    "VALUES(?,?,?)",
                    (operation_id, payload, next_level),
                )
                if inserted.rowcount != 1:
                    self._reject("receipt_write_rejected", "receipt")
                self._verify_receipt(uow, operation_id, receipt)

                # A receipt trigger may rewrite any earlier projection.
                self._verify_player(uow, user_id, expected_user)
                if expected_item is not None:
                    self._verify_inventory(uow, user_id, item_id, expected_item)
                self._verify_state(uow, user_id, expected_state)
                return DongfuArrayUpgradeResult("upgraded", next_level)
        except _ArrayUpgradeStateChanged as exc:
            return DongfuArrayUpgradeResult(
                "state_changed", reason=exc.reason, subject=exc.subject
            )
