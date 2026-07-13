from __future__ import annotations

import json
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend
from .card_bonus import refresh_card_bonuses


@dataclass(frozen=True)
class ImpartPrayerSettlementResult:
    status: str
    cards: tuple[str, ...] = ()
    new_cards: tuple[str, ...] = ()
    card_counts: tuple[tuple[str, int], ...] = ()
    item_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ImpartPrayerSettlementService:
    """Consume wishing stones and persist the fixed card batch atomically."""

    def __init__(
        self,
        game_database: str | Path,
        impart_database: str | Path,
        lock: RLock | None = None,
    ) -> None:
        self._game_database = Path(game_database)
        self._impart_database = Path(impart_database)
        self._lock = lock or RLock()

    @staticmethod
    def _identity(user_id: str, item_id: int, quantity: int) -> str:
        return json.dumps(
            [user_id, item_id, quantity],
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _from_row(status: str, row) -> ImpartPrayerSettlementResult:
        return ImpartPrayerSettlementResult(
            status,
            tuple(str(card) for card in json.loads(str(row[1]))),
            tuple(str(card) for card in json.loads(str(row[2]))),
            tuple((str(name), int(count)) for name, count in json.loads(str(row[3]))),
            int(row[4]),
        )

    @staticmethod
    def _normalize_request(operation_id, user_id, item_id, quantity):
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        quantity = int(quantity)
        if not operation_id or not user_id or item_id <= 0 or quantity <= 0:
            raise ValueError("invalid impart prayer request")
        return operation_id, user_id, item_id, quantity

    def replay(self, operation_id, user_id, item_id, quantity):
        operation_id, user_id, item_id, quantity = self._normalize_request(
            operation_id, user_id, item_id, quantity
        )
        identity = self._identity(user_id, item_id, quantity)
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            if not conn.table_exists("impart_prayer_operations"):
                return None
            previous = conn.execute(
                "SELECT identity_json,cards_json,new_cards_json,card_counts_json,item_remaining "
                "FROM impart_prayer_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            if str(previous[0]) != identity:
                return ImpartPrayerSettlementResult("operation_conflict")
            return self._from_row("duplicate", previous)

    def settle(
        self,
        operation_id,
        user_id,
        item_id,
        quantity,
        cards,
        card_definitions,
    ) -> ImpartPrayerSettlementResult:
        operation_id, user_id, item_id, quantity = self._normalize_request(
            operation_id, user_id, item_id, quantity
        )
        cards = tuple(str(card).strip() for card in cards)
        definitions = {str(name): dict(definition) for name, definition in dict(card_definitions).items()}
        if len(cards) != quantity or any(not card or card not in definitions for card in cards):
            raise ValueError("card batch must match the prayer quantity and definitions")
        identity = self._identity(user_id, item_id, quantity)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            attached = False
            try:
                conn.execute("ATTACH DATABASE %s AS impart_data", (str(self._impart_database),))
                attached = True
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS impart_prayer_operations("
                    "operation_id TEXT PRIMARY KEY,identity_json TEXT NOT NULL,cards_json TEXT NOT NULL,"
                    "new_cards_json TEXT NOT NULL,card_counts_json TEXT NOT NULL,"
                    "item_remaining INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT identity_json,cards_json,new_cards_json,card_counts_json,item_remaining "
                    "FROM impart_prayer_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != identity:
                        return ImpartPrayerSettlementResult("operation_conflict")
                    return self._from_row("duplicate", previous)

                columns = set(conn.column_names("back"))
                bind_expression = ",COALESCE(bind_num,0)" if "bind_num" in columns else ",0"
                item = conn.execute(
                    f"SELECT COALESCE(goods_num,0){bind_expression} FROM back "
                    "WHERE user_id=%s AND goods_id=%s",
                    (user_id, item_id),
                ).fetchone()
                if item is None or int(item[0]) < quantity:
                    conn.rollback()
                    return ImpartPrayerSettlementResult("item_missing")
                impart_user = conn.execute(
                    "SELECT 1 FROM impart_data.xiuxian_impart WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if impart_user is None:
                    conn.rollback()
                    return ImpartPrayerSettlementResult("user_missing")

                item_count = int(item[0])
                item_remaining = item_count - quantity
                updates = ["goods_num=goods_num-%s"]
                update_params: list[object] = [quantity]
                if "bind_num" in columns:
                    updates.append(
                        "bind_num=CASE WHEN goods_num-%s=0 THEN 0 "
                        "WHEN COALESCE(bind_num,0)>=%s THEN COALESCE(bind_num,0)-%s "
                        "ELSE MIN(COALESCE(bind_num,0),goods_num-%s) END"
                    )
                    update_params.extend((quantity, quantity, quantity, quantity))
                consumed = conn.execute(
                    f"UPDATE back SET {', '.join(updates)} WHERE user_id=%s AND goods_id=%s "
                    "AND COALESCE(goods_num,0)>=%s",
                    (*update_params, user_id, item_id, quantity),
                )
                if consumed.rowcount != 1:
                    conn.rollback()
                    return ImpartPrayerSettlementResult("state_changed")

                existing = {
                    str(row[0]): int(row[1])
                    for row in conn.execute(
                        "SELECT card_name,quantity FROM impart_data.impart_cards WHERE user_id=%s",
                        (user_id,),
                    ).fetchall()
                }
                new_cards = tuple(dict.fromkeys(card for card in cards if card not in existing))
                increments = Counter(cards)
                for card_name, amount in increments.items():
                    conn.execute(
                        "INSERT INTO impart_data.impart_cards(user_id,card_name,quantity) "
                        "VALUES (%s,%s,%s) ON CONFLICT(user_id,card_name) DO UPDATE SET "
                        "quantity=impart_cards.quantity+EXCLUDED.quantity",
                        (user_id, card_name, amount),
                    )
                card_counts = tuple(
                    (card_name, existing.get(card_name, 0) + amount)
                    for card_name, amount in increments.items()
                )
                refresh_card_bonuses(conn, user_id, definitions)

                cards_json = json.dumps(cards, ensure_ascii=False, separators=(",", ":"))
                new_cards_json = json.dumps(new_cards, ensure_ascii=False, separators=(",", ":"))
                counts_json = json.dumps(card_counts, ensure_ascii=False, separators=(",", ":"))
                conn.execute(
                    "INSERT INTO impart_prayer_operations("
                    "operation_id,identity_json,cards_json,new_cards_json,card_counts_json,item_remaining"
                    ") VALUES (%s,%s,%s,%s,%s,%s)",
                    (
                        operation_id,
                        identity,
                        cards_json,
                        new_cards_json,
                        counts_json,
                        item_remaining,
                    ),
                )
                conn.commit()
                return ImpartPrayerSettlementResult(
                    "applied", cards, new_cards, card_counts, item_remaining
                )
            except Exception:
                conn.rollback()
                raise
            finally:
                if attached:
                    conn.execute("DETACH DATABASE impart_data")


__all__ = ["ImpartPrayerSettlementResult", "ImpartPrayerSettlementService"]
