from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .card_bonus import refresh_card_bonuses


@dataclass(frozen=True)
class ImpartDrawResult:
    status: str
    wish: int = 0
    draw_count: int = 0
    cards: tuple[str, ...] = ()
    new_cards: tuple[str, ...] = ()
    existing_cards: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ImpartDrawSqlRepository:
    """Atomically settle the paid, game-stone card draw."""

    def __init__(
        self,
        game_database: str | Path,
        impart_database: str | Path,
        player_database: str | Path,
    ) -> None:
        self.game_database = str(game_database)
        self.impart_database = str(impart_database)
        self.player_database = str(player_database)

    @staticmethod
    def _cards(value: Any) -> tuple[str, ...]:
        try:
            return tuple(str(card) for card in json.loads(str(value or "[]")))
        except (TypeError, ValueError, json.JSONDecodeError):
            return ()

    def get_result(
        self, operation_id: str, user_id: str, requested_pulls: int
    ) -> ImpartDrawResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id or not Path(self.game_database).is_file():
            return None
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            table = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='impart_draw_operations'"
            )
            if table is None:
                return None
            columns = {
                str(row[1])
                for row in uow.execute("PRAGMA table_info(impart_draw_operations)").fetchall()
            }
            if not {"wish", "draw_count"}.issubset(columns):
                return None
            if "payload" not in columns:
                return ImpartDrawResult("schema_missing")
            cards_column = ",COALESCE(cards_json,'[]')" if "cards_json" in columns else ", '[]'"
            old = uow.query_one(
                "SELECT payload,wish,draw_count" + cards_column
                + " FROM impart_draw_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is None:
                return None
            values = list(old.values())
            try:
                prior_payload = json.loads(str(values[0]))
                prior_user, prior_cost, prior_pulls = prior_payload[:3]
                prior_cost, prior_pulls = int(prior_cost), int(prior_pulls)
            except (TypeError, ValueError):
                return ImpartDrawResult("schema_missing")
            if (
                str(prior_user) != str(user_id)
                or prior_pulls != int(values[2])
                or prior_cost != int(values[2]) * 10_000_000
            ):
                return ImpartDrawResult("operation_conflict")
            # Old receipts only carried effective pulls, so replay them only
            # when the request is unambiguous. New receipts preserve raw input.
            prior_requested = int(prior_payload[3]) if len(prior_payload) > 3 else prior_pulls
            if prior_requested != int(requested_pulls):
                return ImpartDrawResult("operation_conflict")
            return ImpartDrawResult(
                "duplicate", int(values[1]), int(values[2]), self._cards(values[3])
            )

    def draw(
        self,
        operation_id: str,
        user_id: str,
        expected_stone: int,
        expected_wish: int,
        expected_count: int,
        cost: int,
        new_wish: int,
        pulls: int,
        cards: tuple[str, ...] | list[str],
        card_definitions: Mapping[str, Mapping[str, Any]] | None = None,
        statistics_user_id: str | None = None,
        statistics: Mapping[str, int] | None = None,
        requested_pulls: int | None = None,
    ) -> ImpartDrawResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        expected_stone, expected_wish, expected_count, cost, new_wish, pulls = map(
            int, (expected_stone, expected_wish, expected_count, cost, new_wish, pulls)
        )
        requested_pulls = int(requested_pulls if requested_pulls is not None else pulls)
        cards = tuple(str(card).strip() for card in cards)
        if (
            not operation_id
            or not user_id
            or cost <= 0
            or pulls <= 0
            or requested_pulls < pulls
            or len(cards) != pulls
            or new_wish < 0
        ):
            raise ValueError("invalid draw request")
        definitions = dict(card_definitions or {})
        if definitions and any(card not in definitions for card in cards):
            raise ValueError("draw contains an unknown card")

        payload = json.dumps(
            [user_id, cost, pulls, requested_pulls],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.impart_database, "impart_data")
            uow.attach_database(self.player_database, "player_data")
            table = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='impart_draw_operations'"
            )
            if table is None:
                return ImpartDrawResult("schema_missing")
            columns = {
                str(row[1])
                for row in uow.execute("PRAGMA table_info(impart_draw_operations)").fetchall()
            }
            if "cards_json" not in columns:
                return ImpartDrawResult("schema_missing")
            old = uow.query_one(
                "SELECT payload,wish,draw_count,COALESCE(cards_json,'[]') "
                "FROM impart_draw_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                if str(old["payload"]) != payload:
                    return ImpartDrawResult("operation_conflict")
                return ImpartDrawResult(
                    "duplicate", int(old["wish"]), int(old["draw_count"]), self._cards(old["cards_json"])
                )

            if not self._statistics_schema_ready(uow):
                return ImpartDrawResult("schema_missing")

            user = uow.query_one(
                "SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            state = uow.query_one(
                "SELECT wish,impart_num FROM impart_data.xiuxian_impart WHERE user_id=?",
                (user_id,),
            )
            cards_table = uow.query_one(
                "SELECT 1 AS present FROM impart_data.sqlite_master "
                "WHERE type='table' AND name='impart_cards'"
            )
            if user is None or state is None or cards_table is None:
                return ImpartDrawResult("schema_missing")
            if int(user["stone"]) != expected_stone or tuple(map(int, (state["wish"], state["impart_num"]))) != (
                expected_wish,
                expected_count,
            ):
                return ImpartDrawResult("state_changed")
            if expected_stone < cost:
                return ImpartDrawResult("stone_missing")
            if expected_count + pulls > 100:
                return ImpartDrawResult("limit_exceeded")

            existing_card_counts = self._card_counts(uow, "impart_data", user_id)
            new_cards = tuple(
                dict.fromkeys(card for card in cards if card not in existing_card_counts)
            )
            duplicate_count = len(cards) - len(new_cards)

            charged = uow.execute(
                "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)-? "
                "WHERE user_id=? AND CAST(COALESCE(stone,0) AS REAL)>=?",
                (cost, user_id, cost),
            )
            if charged.rowcount != 1:
                return ImpartDrawResult("state_changed")
            changed = uow.execute(
                "UPDATE impart_data.xiuxian_impart SET wish=?,impart_num=impart_num+? "
                "WHERE user_id=?",
                (new_wish, pulls, user_id),
            )
            if changed.rowcount != 1:
                return ImpartDrawResult("state_changed")
            for card_name, quantity in Counter(cards).items():
                uow.execute(
                    "INSERT INTO impart_data.impart_cards(user_id,card_name,quantity) "
                    "VALUES(?,?,?) ON CONFLICT(user_id,card_name) DO UPDATE SET "
                    "quantity=impart_cards.quantity+excluded.quantity",
                    (user_id, card_name, quantity),
                )
            if definitions:
                refresh_card_bonuses(uow.connection, user_id, definitions)
            self._apply_statistics(
                uow,
                statistics_user_id or user_id,
                {
                    **dict(statistics or {}),
                    "传承新卡": len(new_cards),
                    "传承重复卡": duplicate_count,
                },
            )
            uow.execute(
                "INSERT INTO impart_draw_operations(operation_id,payload,wish,draw_count,cards_json) "
                "VALUES(?,?,?,?,?)",
                (operation_id, payload, new_wish, pulls, json.dumps(cards, ensure_ascii=False, separators=(",", ":"))),
            )
            return ImpartDrawResult(
                "applied", new_wish, pulls, cards, new_cards,
                tuple(existing_card_counts),
            )

    @staticmethod
    def _card_counts(uow: DatabaseUnitOfWork, schema: str, user_id: str) -> dict[str, int]:
        return {
            str(row["card_name"]): int(row["quantity"])
            for row in uow.query_all(
                f"SELECT card_name,quantity FROM {schema}.impart_cards WHERE user_id=?",
                (str(user_id),),
            )
        }

    @staticmethod
    def _statistics_schema_ready(uow: DatabaseUnitOfWork) -> bool:
        columns = {
            str(row[1])
            for row in uow.execute("PRAGMA player_data.table_info(statistics)").fetchall()
        }
        return "user_id" in columns

    @staticmethod
    def _apply_statistics(
        uow: DatabaseUnitOfWork,
        user_id: str,
        values: Mapping[str, int],
    ) -> None:
        if not values:
            return
        columns = {
            str(row[1])
            for row in uow.execute("PRAGMA player_data.table_info(statistics)").fetchall()
        }
        if not set(values).issubset(columns):
            raise RuntimeError("Impart draw statistics schema is incomplete")
        uow.execute(
            "INSERT OR IGNORE INTO player_data.statistics(user_id) VALUES(?)",
            (str(user_id),),
        )
        for field, increment in values.items():
            uow.execute(
                f'UPDATE player_data.statistics SET "{field}"=COALESCE("{field}",0)+? WHERE user_id=?',
                (int(increment), str(user_id)),
            )


@dataclass(frozen=True)
class ImpartCrystalDrawResult:
    status: str
    wish: int = 0
    stone_num: int = 0
    exp_day: int = 0
    cards: tuple[str, ...] = ()
    new_cards: tuple[str, ...] = ()
    existing_cards: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ImpartCrystalDrawSqlRepository:
    """Settle the crystal-based Impart prayer in the feature database."""

    def __init__(self, database: str | Path, player_database: str | Path) -> None:
        self.database = str(database)
        self.player_database = str(player_database)

    def get_result(
        self, operation_id: str, user_id: str, cost: int, pulls: int
    ) -> ImpartCrystalDrawResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id or not Path(self.database).is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            table = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='impart_crystal_draw_operations'"
            )
            if table is None:
                return None
            old = uow.query_one(
                "SELECT payload,wish,stone_num,exp_day,cards_json "
                "FROM impart_crystal_draw_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is None:
                return None
            try:
                prior_user, prior_cost, prior_pulls = json.loads(str(old["payload"]))
            except (TypeError, ValueError):
                return ImpartCrystalDrawResult("schema_missing")
            if (str(prior_user), int(prior_cost), int(prior_pulls)) != (
                str(user_id), int(cost), int(pulls)
            ):
                return ImpartCrystalDrawResult("operation_conflict")
            return ImpartCrystalDrawResult(
                "duplicate",
                int(old["wish"]),
                int(old["stone_num"]),
                int(old["exp_day"]),
                tuple(self._decode_cards(old["cards_json"])),
            )

    def draw(
        self,
        operation_id: str,
        user_id: str,
        expected_stone: int,
        expected_wish: int,
        cost: int,
        new_wish: int,
        exp_minutes: int,
        cards: tuple[str, ...] | list[str],
        card_definitions: Mapping[str, Mapping[str, Any]],
        statistics_user_id: str | None = None,
        statistics: Mapping[str, int] | None = None,
    ) -> ImpartCrystalDrawResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        expected_stone, expected_wish, cost, new_wish, exp_minutes = map(
            int, (expected_stone, expected_wish, cost, new_wish, exp_minutes)
        )
        cards = tuple(str(card).strip() for card in cards)
        definitions = dict(card_definitions)
        if (
            not operation_id
            or not user_id
            or cost <= 0
            or exp_minutes < 0
            or any(not card or card not in definitions for card in cards)
        ):
            raise ValueError("invalid crystal draw request")
        payload = json.dumps([user_id, cost, len(cards)], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            table = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='impart_crystal_draw_operations'"
            )
            if table is None:
                return ImpartCrystalDrawResult("schema_missing")
            old = uow.query_one(
                "SELECT payload,wish,stone_num,exp_day,cards_json "
                "FROM impart_crystal_draw_operations WHERE operation_id=?",
                (operation_id,),
            )
            if old is not None:
                if str(old["payload"]) != payload:
                    return ImpartCrystalDrawResult("operation_conflict")
                return ImpartCrystalDrawResult(
                    "duplicate", int(old["wish"]), int(old["stone_num"]),
                    int(old["exp_day"]), tuple(self._decode_cards(old["cards_json"])),
                )
            if not ImpartDrawSqlRepository._statistics_schema_ready(uow):
                return ImpartCrystalDrawResult("schema_missing")
            state = uow.query_one(
                "SELECT wish,stone_num,exp_day FROM xiuxian_impart WHERE user_id=?",
                (user_id,),
            )
            if state is None:
                return ImpartCrystalDrawResult("user_missing")
            if (int(state["stone_num"]), int(state["wish"])) != (expected_stone, expected_wish):
                return ImpartCrystalDrawResult("state_changed")
            if expected_stone < cost:
                return ImpartCrystalDrawResult("stone_missing")
            existing_card_counts = ImpartDrawSqlRepository._card_counts(
                uow, "main", user_id
            )
            new_cards = tuple(
                dict.fromkeys(card for card in cards if card not in existing_card_counts)
            )
            duplicate_count = len(cards) - len(new_cards)
            stone_num = expected_stone - cost
            exp_day = int(state["exp_day"] or 0) + exp_minutes
            changed = uow.execute(
                "UPDATE xiuxian_impart SET stone_num=?,wish=?,exp_day=? WHERE user_id=? "
                "AND stone_num=? AND wish=?",
                (stone_num, new_wish, exp_day, user_id, expected_stone, expected_wish),
            )
            if changed.rowcount != 1:
                return ImpartCrystalDrawResult("state_changed")
            for card_name, quantity in Counter(cards).items():
                uow.execute(
                    "INSERT INTO impart_cards(user_id,card_name,quantity) VALUES(?,?,?) "
                    "ON CONFLICT(user_id,card_name) DO UPDATE SET quantity=impart_cards.quantity+excluded.quantity",
                    (user_id, card_name, quantity),
                )
            refresh_card_bonuses(uow.connection, user_id, definitions, schema="main")
            ImpartDrawSqlRepository._apply_statistics(
                uow,
                statistics_user_id or user_id,
                {
                    **dict(statistics or {}),
                    "传承新卡": len(new_cards),
                    "传承重复卡": duplicate_count,
                },
            )
            uow.execute(
                "INSERT INTO impart_crystal_draw_operations(operation_id,payload,wish,stone_num,exp_day,cards_json) "
                "VALUES(?,?,?,?,?,?)",
                (operation_id, payload, new_wish, stone_num, exp_day, json.dumps(cards, ensure_ascii=False, separators=(",", ":"))),
            )
            return ImpartCrystalDrawResult(
                "applied", new_wish, stone_num, exp_day, cards,
                new_cards, tuple(existing_card_counts),
            )

    @staticmethod
    def _decode_cards(value: Any) -> tuple[str, ...]:
        try:
            return tuple(str(card) for card in json.loads(str(value or "[]")))
        except (TypeError, ValueError, json.JSONDecodeError):
            return ()


__all__ = [
    "ImpartDrawResult",
    "ImpartDrawSqlRepository",
    "ImpartCrystalDrawResult",
    "ImpartCrystalDrawSqlRepository",
]
