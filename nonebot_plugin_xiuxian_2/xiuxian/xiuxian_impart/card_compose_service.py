from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .card_bonus import refresh_card_bonuses


@dataclass(frozen=True)
class CardComposeResult:
    status: str
    source_quantity: int = 0
    target_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class CardComposeService:
    """Consume duplicate cards and create one selected card atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def compose(
        self, operation_id, user_id, source_card, target_card,
        expected_source_quantity, expected_target_quantity, cost=5, card_definitions=None,
    ) -> CardComposeResult:
        operation_id = str(operation_id).strip()
        user_id, source_card, target_card = map(str, (user_id, source_card, target_card))
        expected_source_quantity = int(expected_source_quantity)
        expected_target_quantity = int(expected_target_quantity)
        cost = int(cost)
        if not operation_id or not source_card or not target_card or cost <= 0:
            raise ValueError("invalid compose request")
        if source_card == target_card:
            return CardComposeResult("same_card")
        payload = json.dumps(
            [user_id, source_card, target_card, expected_source_quantity,
             expected_target_quantity, cost], ensure_ascii=False,
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_card_compose_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "source_quantity INTEGER NOT NULL,target_quantity INTEGER NOT NULL)"
                )
                old = conn.execute(
                    "SELECT payload,source_quantity,target_quantity FROM "
                    "impart_card_compose_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    status = "duplicate" if old[0] == payload else "state_changed"
                    return CardComposeResult(status, int(old[1]), int(old[2]))
                source = conn.execute(
                    "SELECT quantity FROM impart_cards WHERE user_id=%s AND card_name=%s",
                    (user_id, source_card),
                ).fetchone()
                target = conn.execute(
                    "SELECT quantity FROM impart_cards WHERE user_id=%s AND card_name=%s",
                    (user_id, target_card),
                ).fetchone()
                source_quantity = int(source[0]) if source else 0
                target_quantity = int(target[0]) if target else 0
                if (source_quantity, target_quantity) != (
                    expected_source_quantity, expected_target_quantity,
                ):
                    conn.rollback()
                    return CardComposeResult("state_changed", source_quantity, target_quantity)
                if source_quantity < cost:
                    conn.rollback()
                    return CardComposeResult("card_missing", source_quantity, target_quantity)
                consumed = conn.execute(
                    "UPDATE impart_cards SET quantity=quantity-%s WHERE user_id=%s "
                    "AND card_name=%s AND quantity=%s",
                    (cost, user_id, source_card, source_quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return CardComposeResult("state_changed")
                conn.execute(
                    "DELETE FROM impart_cards WHERE user_id=%s AND card_name=%s AND quantity=0",
                    (user_id, source_card),
                )
                conn.execute(
                    "INSERT INTO impart_cards(user_id,card_name,quantity) VALUES (%s,%s,1) "
                    "ON CONFLICT(user_id,card_name) DO UPDATE SET quantity=impart_cards.quantity+1",
                    (user_id, target_card),
                )
                new_source, new_target = source_quantity - cost, target_quantity + 1
                if card_definitions is not None:
                    refresh_card_bonuses(conn, user_id, card_definitions)
                conn.execute(
                    "INSERT INTO impart_card_compose_operations VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, new_source, new_target),
                )
                conn.commit()
                return CardComposeResult("applied", new_source, new_target)
            except Exception:
                conn.rollback()
                raise


__all__ = ["CardComposeResult", "CardComposeService"]
