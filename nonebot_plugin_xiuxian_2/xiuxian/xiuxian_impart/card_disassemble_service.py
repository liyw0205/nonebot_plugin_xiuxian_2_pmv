from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class CardDisassembleResult:
    status: str
    card_quantity: int = 0
    stone_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class CardDisassembleService:
    """Convert duplicate cards into longing crystals atomically."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def disassemble(
        self, operation_id, user_id, card_name, quantity,
        expected_card_quantity, expected_stone_quantity, reward_per_card=2,
    ) -> CardDisassembleResult:
        operation_id = str(operation_id).strip()
        user_id, card_name = str(user_id), str(card_name)
        quantity, expected_card_quantity, expected_stone_quantity, reward_per_card = map(
            int, (quantity, expected_card_quantity, expected_stone_quantity, reward_per_card)
        )
        if not operation_id or not card_name or quantity <= 0 or reward_per_card <= 0:
            raise ValueError("invalid disassemble request")
        payload = json.dumps(
            [user_id, card_name, quantity, expected_card_quantity,
             expected_stone_quantity, reward_per_card], ensure_ascii=False,
        )
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_card_disassemble_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,"
                    "card_quantity INTEGER NOT NULL,stone_quantity INTEGER NOT NULL)"
                )
                old = conn.execute(
                    "SELECT payload,card_quantity,stone_quantity FROM "
                    "impart_card_disassemble_operations WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if old:
                    conn.rollback()
                    status = "duplicate" if old[0] == payload else "state_changed"
                    return CardDisassembleResult(status, int(old[1]), int(old[2]))
                card = conn.execute(
                    "SELECT quantity FROM impart_cards WHERE user_id=%s AND card_name=%s",
                    (user_id, card_name),
                ).fetchone()
                state = conn.execute(
                    "SELECT stone_num FROM xiuxian_impart WHERE user_id=%s", (user_id,),
                ).fetchone()
                card_quantity = int(card[0]) if card else 0
                if state is None:
                    conn.rollback()
                    return CardDisassembleResult("user_missing", card_quantity)
                stone_quantity = int(state[0] or 0)
                if (card_quantity, stone_quantity) != (
                    expected_card_quantity, expected_stone_quantity,
                ):
                    conn.rollback()
                    return CardDisassembleResult("state_changed", card_quantity, stone_quantity)
                # Keep one copy so an acquired card and its passive bonus cannot disappear.
                if card_quantity - quantity < 1:
                    conn.rollback()
                    return CardDisassembleResult("card_missing", card_quantity, stone_quantity)
                consumed = conn.execute(
                    "UPDATE impart_cards SET quantity=quantity-%s WHERE user_id=%s "
                    "AND card_name=%s AND quantity=%s",
                    (quantity, user_id, card_name, card_quantity),
                )
                rewarded = conn.execute(
                    "UPDATE xiuxian_impart SET stone_num=stone_num+%s WHERE user_id=%s "
                    "AND stone_num=%s",
                    (quantity * reward_per_card, user_id, stone_quantity),
                )
                if consumed.rowcount != 1 or rewarded.rowcount != 1:
                    conn.rollback()
                    return CardDisassembleResult("state_changed")
                new_card = card_quantity - quantity
                new_stone = stone_quantity + quantity * reward_per_card
                conn.execute(
                    "INSERT INTO impart_card_disassemble_operations VALUES (%s,%s,%s,%s)",
                    (operation_id, payload, new_card, new_stone),
                )
                conn.commit()
                return CardDisassembleResult("applied", new_card, new_stone)
            except Exception:
                conn.rollback()
                raise


__all__ = ["CardDisassembleResult", "CardDisassembleService"]
