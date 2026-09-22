from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .card_bonus import refresh_card_bonuses


@dataclass(frozen=True)
class ImpartPrayerResult:
    status: str
    cards: tuple[str, ...] = ()
    new_cards: tuple[str, ...] = ()
    card_counts: tuple[tuple[str, int], ...] = ()
    item_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ImpartPrayerSqlRepository:
    def __init__(self, game_database: str | Path, impart_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.impart_database = str(impart_database)

    @staticmethod
    def _result_from_row(status: str, row: Any) -> ImpartPrayerResult:
        return ImpartPrayerResult(
            status=status,
            cards=tuple(json.loads(str(row["cards_json"]))),
            new_cards=tuple(json.loads(str(row["new_cards_json"]))),
            card_counts=tuple(tuple(item) for item in json.loads(str(row["card_counts_json"]))),
            item_remaining=int(row["item_remaining"]),
        )

    def settle(
        self,
        operation_id: str,
        user_id: str,
        item_id: int,
        quantity: int,
        cards: list[str] | tuple[str, ...],
        card_definitions: dict[str, dict[str, Any]],
    ) -> ImpartPrayerResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id).strip()
        item_id = int(item_id)
        quantity = int(quantity)
        cards = tuple(str(card).strip() for card in cards)
        definitions = {str(name): dict(value) for name, value in dict(card_definitions).items()}
        if (
            not operation_id
            or not user_id
            or item_id <= 0
            or quantity <= 0
            or len(cards) != quantity
            or any(not card or card not in definitions for card in cards)
        ):
            raise ValueError("invalid prayer request")

        identity = json.dumps([user_id, item_id, quantity], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.impart_database, "impart_data")
            uow.execute(
                "CREATE TABLE IF NOT EXISTS impart_prayer_operations("
                "operation_id TEXT PRIMARY KEY,identity_json TEXT NOT NULL,cards_json TEXT NOT NULL,"
                "new_cards_json TEXT NOT NULL,card_counts_json TEXT NOT NULL,item_remaining INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = uow.query_one(
                "SELECT identity_json,cards_json,new_cards_json,card_counts_json,item_remaining "
                "FROM impart_prayer_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous is not None:
                if str(previous["identity_json"]) != identity:
                    return ImpartPrayerResult("operation_conflict")
                return self._result_from_row("duplicate", previous)

            columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(back)").fetchall()}
            bind_expression = ",COALESCE(bind_num,0)" if "bind_num" in columns else ",0"
            item = uow.query_one(
                f"SELECT COALESCE(goods_num,0) AS goods_num{bind_expression} "
                "FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            impart_user = uow.query_one(
                "SELECT 1 FROM impart_data.xiuxian_impart WHERE user_id=?",
                (user_id,),
            )
            if impart_user is None:
                return ImpartPrayerResult("user_missing")
            if item is None or int(item["goods_num"]) < quantity:
                return ImpartPrayerResult("item_missing")

            item_count = int(item["goods_num"])
            item_remaining = item_count - quantity
            updates = ["goods_num=goods_num-?"]
            update_params: list[object] = [quantity]
            if "bind_num" in columns:
                updates.append(
                    "bind_num=CASE WHEN goods_num-?=0 THEN 0 "
                    "WHEN COALESCE(bind_num,0)>=? THEN COALESCE(bind_num,0)-? "
                    "ELSE MIN(COALESCE(bind_num,0),goods_num-?) END"
                )
                update_params.extend((quantity, quantity, quantity, quantity))
            consumed = uow.execute(
                f"UPDATE back SET {', '.join(updates)} WHERE user_id=? AND goods_id=? "
                "AND COALESCE(goods_num,0)>=?",
                (*update_params, user_id, item_id, quantity),
            )
            if consumed.rowcount != 1:
                return ImpartPrayerResult("state_changed")

            existing = {
                str(row["card_name"]): int(row["quantity"])
                for row in uow.execute(
                    "SELECT card_name,quantity FROM impart_data.impart_cards WHERE user_id=?",
                    (user_id,),
                ).fetchall()
            }
            increments = Counter(cards)
            for card_name, amount in increments.items():
                uow.execute(
                    "INSERT INTO impart_data.impart_cards(user_id,card_name,quantity) VALUES(?,?,?) "
                    "ON CONFLICT(user_id,card_name) DO UPDATE SET "
                    "quantity=impart_cards.quantity+excluded.quantity",
                    (user_id, card_name, amount),
                )
            new_cards = tuple(dict.fromkeys(card for card in cards if card not in existing))
            card_counts = tuple(
                (card_name, existing.get(card_name, 0) + amount)
                for card_name, amount in increments.items()
            )
            refresh_card_bonuses(uow.connection, user_id, definitions)

            uow.execute(
                "INSERT INTO impart_prayer_operations("
                "operation_id,identity_json,cards_json,new_cards_json,card_counts_json,item_remaining) "
                "VALUES(?,?,?,?,?,?)",
                (
                    operation_id,
                    identity,
                    json.dumps(cards, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(new_cards, ensure_ascii=False, separators=(",", ":")),
                    json.dumps(card_counts, ensure_ascii=False, separators=(",", ":")),
                    item_remaining,
                ),
            )
            return ImpartPrayerResult("applied", cards, new_cards, card_counts, item_remaining)


__all__ = ["ImpartPrayerSqlRepository", "ImpartPrayerResult"]
