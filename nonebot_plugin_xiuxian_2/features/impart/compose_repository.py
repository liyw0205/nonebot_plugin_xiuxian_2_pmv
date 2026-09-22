from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork
from .card_bonus import refresh_card_bonuses


@dataclass(frozen=True)
class CardComposeResult:
    status: str
    source_quantity: int = 0
    target_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ImpartCardComposeSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def compose(
        self,
        operation_id,
        user_id,
        source_card,
        target_card,
        expected_source_quantity,
        expected_target_quantity,
        cost=5,
        card_definitions=None,
    ) -> CardComposeResult:
        operation_id = str(operation_id).strip()
        user_id, source_card, target_card = map(str, (user_id, source_card, target_card))
        expected_source_quantity, expected_target_quantity, cost = map(
            int, (expected_source_quantity, expected_target_quantity, cost)
        )
        if not operation_id or not source_card or not target_card or cost <= 0:
            raise ValueError("invalid compose request")
        if source_card == target_card:
            return CardComposeResult("same_card")

        payload = json.dumps(
            [user_id, source_card, target_card, cost],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE IF NOT EXISTS impart_card_compose_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                "source_quantity INTEGER NOT NULL,target_quantity INTEGER NOT NULL)"
            )
            old = uow.query_one(
                "SELECT payload,source_quantity,target_quantity "
                "FROM impart_card_compose_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                if str(old["payload"]) != payload:
                    return CardComposeResult(
                        "state_changed", int(old["source_quantity"]), int(old["target_quantity"])
                    )
                return CardComposeResult(
                    "duplicate", int(old["source_quantity"]), int(old["target_quantity"])
                )

            source = uow.query_one(
                "SELECT quantity FROM impart_cards WHERE user_id=? AND card_name=?",
                (user_id, source_card),
            )
            target = uow.query_one(
                "SELECT quantity FROM impart_cards WHERE user_id=? AND card_name=?",
                (user_id, target_card),
            )
            source_quantity = 0 if source is None else int(source["quantity"])
            target_quantity = 0 if target is None else int(target["quantity"])
            if (source_quantity, target_quantity) != (
                expected_source_quantity,
                expected_target_quantity,
            ):
                return CardComposeResult("state_changed", source_quantity, target_quantity)
            if source_quantity < cost:
                return CardComposeResult("card_missing", source_quantity, target_quantity)

            consumed = uow.execute(
                "UPDATE impart_cards SET quantity=quantity-? "
                "WHERE user_id=? AND card_name=? AND quantity=?",
                (cost, user_id, source_card, source_quantity),
            )
            if consumed.rowcount != 1:
                return CardComposeResult("state_changed")
            uow.execute(
                "DELETE FROM impart_cards WHERE user_id=? AND card_name=? AND quantity=0",
                (user_id, source_card),
            )
            uow.execute(
                "INSERT INTO impart_cards(user_id,card_name,quantity) VALUES(?,?,1) "
                "ON CONFLICT(user_id,card_name) DO UPDATE SET "
                "quantity=impart_cards.quantity+1",
                (user_id, target_card),
            )
            new_source, new_target = source_quantity - cost, target_quantity + 1
            if card_definitions is not None:
                refresh_card_bonuses(uow.connection, user_id, card_definitions, schema="main")
            uow.execute(
                "INSERT INTO impart_card_compose_operations VALUES(?,?,?,?)",
                (operation_id, payload, new_source, new_target),
            )
            return CardComposeResult("applied", new_source, new_target)


__all__ = ["ImpartCardComposeSqlRepository", "CardComposeResult"]
