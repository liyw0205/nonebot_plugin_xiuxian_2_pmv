from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class UnbindResult:
    status: str
    user_id: str
    charm_item_id: int
    target_item_id: int
    quantity: int

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class UnbindSqlRepository:
    """Atomically consume unbind charms and reduce a target bound quantity."""

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def apply(
        self,
        operation_id: str,
        user_id: str,
        charm_item_id: int,
        target_item_id: int,
        requested_quantity: int,
    ) -> UnbindResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        charm_item_id = int(charm_item_id)
        target_item_id = int(target_item_id)
        requested_quantity = int(requested_quantity)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if requested_quantity <= 0:
            raise ValueError("requested_quantity must be positive")
        if charm_item_id == target_item_id:
            raise ValueError("charm and target item must differ")

        def result(status: str, quantity: int = 0) -> UnbindResult:
            return UnbindResult(status, user_id, charm_item_id, target_item_id, int(quantity))

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT quantity FROM unbind_item_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                return result("duplicate", int(previous["quantity"]))

            charm = uow.query_one(
                "SELECT goods_num, bind_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, charm_item_id),
            )
            if charm is None or int(charm["goods_num"] or 0) <= 0:
                return result("charm_missing")
            target = uow.query_one(
                "SELECT goods_num, bind_num FROM back WHERE user_id=? AND goods_id=?",
                (user_id, target_item_id),
            )
            if target is None or int(target["goods_num"] or 0) <= 0:
                return result("target_missing")
            if int(target["bind_num"] or 0) <= 0:
                return result("not_bound")

            quantity = min(
                requested_quantity,
                int(charm["goods_num"] or 0),
                int(target["bind_num"] or 0),
            )
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(back)")}
            charm_updates = ["goods_num=goods_num-?"]
            charm_params: list[object] = [quantity]
            if "bind_num" in columns:
                charm_updates.append(
                    "bind_num=CASE WHEN goods_num-?=0 THEN 0 "
                    "WHEN COALESCE(bind_num, 0)>=? THEN COALESCE(bind_num, 0)-? "
                    "ELSE MIN(COALESCE(bind_num, 0), goods_num-?) END"
                )
                charm_params.extend((quantity, quantity, quantity, quantity))
            for column in ("update_time", "action_time"):
                if column in columns:
                    charm_updates.append(f"{column}=CURRENT_TIMESTAMP")
            consumed = uow.execute(
                f"UPDATE back SET {', '.join(charm_updates)} "
                "WHERE user_id=? AND goods_id=? AND goods_num>=?",
                (*charm_params, user_id, charm_item_id, quantity),
            )
            target_updates = ["bind_num=bind_num-?"]
            target_params: list[object] = [quantity]
            if "update_time" in columns:
                target_updates.append("update_time=CURRENT_TIMESTAMP")
            if "action_time" in columns:
                target_updates.append("action_time=CURRENT_TIMESTAMP")
            unbound = uow.execute(
                f"UPDATE back SET {', '.join(target_updates)} "
                "WHERE user_id=? AND goods_id=? AND bind_num>=?",
                (*target_params, user_id, target_item_id, quantity),
            )
            if consumed.rowcount != 1 or unbound.rowcount != 1:
                return result("state_changed")
            uow.execute(
                "INSERT INTO unbind_item_operations "
                "(operation_id,user_id,charm_item_id,target_item_id,quantity) VALUES(?,?,?,?,?)",
                (operation_id, user_id, charm_item_id, target_item_id, quantity),
            )
            return result("applied", quantity)


__all__ = ["UnbindResult", "UnbindSqlRepository"]
