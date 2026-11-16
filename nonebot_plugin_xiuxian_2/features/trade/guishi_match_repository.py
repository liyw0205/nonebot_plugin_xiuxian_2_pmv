from __future__ import annotations

from ...core.numeric import require_exact_integer
from ...core.errors import DomainError
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from datetime import timezone, tzinfo
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .guishi_lifecycle import DEFAULT_GUISHI_TIMEZONE, baitan_deadline, qiugou_deadline, business_time, parse_created_at


@dataclass(frozen=True)
class GuishiMatchResult:
    status: str
    qiugou_order_id: str
    baitan_order_id: str
    buyer_id: str = ""
    seller_id: str = ""
    item_id: int = 0
    item_name: str = ""
    quantity: int = 0
    amount: int = 0
    qiugou_completed: bool = False
    baitan_completed: bool = False
    price_refund: int = 0
    statistics_recorded: bool = False

    @property
    def matched(self) -> bool:
        return self.status in {"matched", "duplicate"}


class GuishiOrderMatchSqlRepository:
    """Match one Guishi buy/sell pair in a single trade-db transaction."""

    def __init__(self, trade_database: str | Path, *, player_database: str | Path | None = None,
                 clock: Any | None = None, business_timezone: tzinfo = DEFAULT_GUISHI_TIMEZONE) -> None:
        self.trade_database = str(trade_database)
        self.player_database = str(player_database) if player_database is not None else None
        self.clock = clock
        self.business_timezone = business_timezone

    def _expired(self, uow: DatabaseUnitOfWork, order_id: str, order_type: str) -> bool:
        if self.clock is None:
            return False
        if uow.query_one("SELECT 1 FROM sqlite_master WHERE type='table' AND name='guishi_order_create_operations'") is None:
            return False
        row = uow.query_one("SELECT MAX(created_at) AS created_at FROM guishi_order_create_operations "
                            "WHERE order_id=? AND order_type IN (?,?)",
                            (order_id, order_type, "求购" if order_type == "qiugou" else "摆摊"))
        anchor = parse_created_at(row['created_at'])
        if anchor is None:
            return False
        created = business_time(anchor, self.business_timezone)
        deadline = qiugou_deadline(created) if order_type == "qiugou" else baitan_deadline(created)
        return business_time(self.clock.now(), timezone.utc) >= deadline.astimezone(timezone.utc)

    def pending_order_expiry(self, order_id: str) -> str | None:
        with DatabaseUnitOfWork(self.trade_database, read_only=True) as uow:
            row = uow.query_one("SELECT item_type FROM guishi_item WHERE id=?", (order_id,))
            if row is None:
                return None
            if row['item_type'] not in {'qiugou', '求购', 'baitan', '摆摊'}:
                return None
            kind = "qiugou" if row['item_type'] in {'qiugou', '求购'} else "baitan"
            return f"{kind}_expired" if self._expired(uow, order_id, kind) else None

    @staticmethod
    def _statistics(uow, user_id, keys, quantity, amount):
        uow.execute("INSERT OR IGNORE INTO guishi_player.statistics(user_id) VALUES(?)", (user_id,))
        columns = ",".join(f'"{key}"' for key in keys)
        previous = uow.query_one(f"SELECT {columns} FROM guishi_player.statistics WHERE user_id=?", (user_id,))
        if previous is None:
            raise DomainError("statistics_write_rejected", "Guishi statistics insert was rejected",
                              {"reason": "statistics_write_rejected", "subject": user_id})
        values = [require_exact_integer(previous[key] or 0, field=key, subject=user_id) + delta
                  for key, delta in zip(keys, (1, quantity, amount))]
        assignments = ",".join(f'"{key}"=xiuxian_int_store(?)' for key in keys)
        written = uow.execute(f"UPDATE guishi_player.statistics SET {assignments} WHERE user_id=?", (*values, user_id))
        if written.rowcount != 1:
            raise DomainError("statistics_write_rejected", "Guishi statistics update was rejected",
                              {"reason": "statistics_write_rejected", "subject": user_id})

    @staticmethod
    def _payload(qiugou_order_id: str, baitan_order_id: str) -> str:
        return json.dumps([qiugou_order_id, baitan_order_id], ensure_ascii=False)

    @staticmethod
    def _info(uow: DatabaseUnitOfWork, user_id: str) -> tuple[int, dict[str, int]]:
        row = uow.query_one(
            "SELECT COALESCE(stored_stone,0) AS stored_stone,items "
            "FROM guishi_info WHERE user_id=?",
            (user_id,),
        )
        if row is None:
            return 0, {}
        try:
            items = json.loads(row.get("items") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            items = {}
        if not isinstance(items, dict):
            items = {}
        normalized: dict[str, int] = {}
        for key, value in items.items():
            try:
                normalized[str(key)] = int(value)
            except (TypeError, ValueError):
                continue
        return require_exact_integer(row.get("stored_stone"), field="stored_stone", subject=user_id), normalized

    @staticmethod
    def _save_info(
        uow: DatabaseUnitOfWork, user_id: str, stored_stone: int, items: dict[str, int]
    ) -> None:
        uow.execute(
            "INSERT INTO guishi_info(user_id,stored_stone,items) VALUES(?,xiuxian_int_store(?,'stored_stone','actor'),?) "
            "ON CONFLICT(user_id) DO UPDATE SET stored_stone=excluded.stored_stone,"
            "items=excluded.items",
            (user_id, int(stored_stone), json.dumps(items, ensure_ascii=False, sort_keys=True)),
        )

    @staticmethod
    def _replay(
        uow: DatabaseUnitOfWork,
        operation_id: str,
        payload: str,
        qiugou_order_id: str,
        baitan_order_id: str,
    ) -> GuishiMatchResult | None:
        if not operation_id:
            return None
        row = uow.query_one(
            "SELECT payload,result FROM guishi_match_operations WHERE operation_id=?",
            (operation_id,),
        )
        if row is None:
            return None
        if str(row["payload"]) != payload:
            return GuishiMatchResult(
                "operation_conflict", qiugou_order_id, baitan_order_id
            )
        values = json.loads(str(row["result"]))
        values["status"] = "duplicate"
        return GuishiMatchResult(**values)

    def match(
        self,
        *,
        operation_id: str,
        qiugou_order_id: str,
        baitan_order_id: str,
    ) -> GuishiMatchResult:
        operation_id = str(operation_id or "").strip()
        qiugou_order_id = str(qiugou_order_id).strip()
        baitan_order_id = str(baitan_order_id).strip()
        payload = self._payload(qiugou_order_id, baitan_order_id)

        with DatabaseUnitOfWork(self.trade_database, immediate=True) as uow:
            replay = self._replay(
                uow, operation_id, payload, qiugou_order_id, baitan_order_id
            )
            if replay is not None:
                return replay

            qiugou = uow.query_one(
                "SELECT user_id,item_id,item_name,item_type,price,quantity,"
                "COALESCE(filled_quantity,0) AS filled_quantity "
                "FROM guishi_item WHERE id=?",
                (qiugou_order_id,),
            )
            baitan = uow.query_one(
                "SELECT user_id,item_id,item_name,item_type,price,quantity,"
                "COALESCE(filled_quantity,0) AS filled_quantity "
                "FROM guishi_item WHERE id=?",
                (baitan_order_id,),
            )
            if qiugou is None or baitan is None:
                return GuishiMatchResult("order_missing", qiugou_order_id, baitan_order_id)
            if str(qiugou["item_type"]) not in {"qiugou", "求购"}:
                return GuishiMatchResult("qiugou_invalid", qiugou_order_id, baitan_order_id)
            if str(baitan["item_type"]) not in {"baitan", "摆摊"}:
                return GuishiMatchResult("baitan_invalid", qiugou_order_id, baitan_order_id)
            if self._expired(uow, qiugou_order_id, "qiugou"):
                return GuishiMatchResult("qiugou_expired", qiugou_order_id, baitan_order_id)
            if self._expired(uow, baitan_order_id, "baitan"):
                return GuishiMatchResult("baitan_expired", qiugou_order_id, baitan_order_id)

            buyer_id = str(qiugou["user_id"])
            seller_id = str(baitan["user_id"])
            item_id = int(baitan["item_id"] or 0)
            item_name = str(baitan["item_name"] or "")
            qiugou_remaining = max(
                int(qiugou["quantity"] or 0) - int(qiugou["filled_quantity"] or 0), 0
            )
            baitan_remaining = max(
                int(baitan["quantity"] or 0) - int(baitan["filled_quantity"] or 0), 0
            )
            if qiugou_remaining <= 0:
                uow.execute("DELETE FROM guishi_item WHERE id=?", (qiugou_order_id,))
                return GuishiMatchResult(
                    "qiugou_completed", qiugou_order_id, baitan_order_id, buyer_id=buyer_id
                )
            if baitan_remaining <= 0:
                uow.execute("DELETE FROM guishi_item WHERE id=?", (baitan_order_id,))
                return GuishiMatchResult(
                    "baitan_completed",
                    qiugou_order_id,
                    baitan_order_id,
                    buyer_id=buyer_id,
                    seller_id=seller_id,
                    item_id=item_id,
                    item_name=item_name,
                )
            if (
                buyer_id == seller_id
                or str(qiugou["item_name"]) != item_name
                or int(baitan["price"] or 0) > int(qiugou["price"] or 0)
            ):
                return GuishiMatchResult("not_matchable", qiugou_order_id, baitan_order_id)

            quantity = min(qiugou_remaining, baitan_remaining)
            execution_price = int(baitan["price"] or 0)
            amount = quantity * execution_price
            # The buyer froze ``quantity * bid`` when creating the order but the pair
            # settles at the stall price, so the difference belongs back to the buyer.
            price_refund = quantity * max(
                int(qiugou["price"] or 0) - execution_price, 0
            )
            buyer_stone, buyer_items = self._info(uow, buyer_id)
            seller_stone, seller_items = self._info(uow, seller_id)
            item_key = str(item_id)
            buyer_items[item_key] = int(buyer_items.get(item_key, 0)) + quantity
            self._save_info(uow, buyer_id, buyer_stone + price_refund, buyer_items)
            self._save_info(uow, seller_id, seller_stone + amount, seller_items)

            qiugou_completed = quantity == qiugou_remaining
            baitan_completed = quantity == baitan_remaining
            if qiugou_completed:
                uow.execute("DELETE FROM guishi_item WHERE id=?", (qiugou_order_id,))
            else:
                uow.execute(
                    "UPDATE guishi_item SET filled_quantity=COALESCE(filled_quantity,0)+? "
                    "WHERE id=?",
                    (quantity, qiugou_order_id),
                )
            if baitan_completed:
                uow.execute("DELETE FROM guishi_item WHERE id=?", (baitan_order_id,))
            else:
                uow.execute(
                    "UPDATE guishi_item SET filled_quantity=COALESCE(filled_quantity,0)+? "
                    "WHERE id=?",
                    (quantity, baitan_order_id),
                )

            result = GuishiMatchResult(
                "matched",
                qiugou_order_id,
                baitan_order_id,
                buyer_id,
                seller_id,
                item_id,
                item_name,
                quantity,
                amount,
                qiugou_completed,
                baitan_completed,
                price_refund,
                self.player_database is not None,
            )
            if self.player_database is not None:
                if not Path(self.player_database).is_file():
                    raise DomainError("schema_missing", "Guishi statistics database is missing",
                                      {"reason": "schema_missing", "subject": "guishi_match_statistics"})
                uow.attach_database(self.player_database, "guishi_player")
                columns = {row['name'] for row in uow.query_all('PRAGMA guishi_player.table_info(statistics)')}
                required = {"user_id", "鬼市购买次数", "鬼市购买数量", "鬼市消费灵石",
                            "鬼市售出次数", "鬼市售出数量", "鬼市收入灵石"}
                if not required <= columns:
                    raise DomainError("schema_missing", "Guishi statistics migration is missing",
                                      {"reason": "schema_missing", "subject": "guishi_match_statistics"})
                self._statistics(uow, buyer_id, ("鬼市购买次数", "鬼市购买数量", "鬼市消费灵石"), quantity, amount)
                self._statistics(uow, seller_id, ("鬼市售出次数", "鬼市售出数量", "鬼市收入灵石"), quantity, amount)
            if operation_id:
                uow.execute(
                    "INSERT INTO guishi_match_operations(operation_id,payload,result) "
                    "VALUES(?,?,?)",
                    (operation_id, payload, json.dumps(asdict(result), ensure_ascii=False)),
                )
            return result


GuishiMatchSqlRepository = GuishiOrderMatchSqlRepository
GuishiMatch = GuishiMatchResult


__all__ = [
    "GuishiMatch",
    "GuishiMatchResult",
    "GuishiMatchSqlRepository",
    "GuishiOrderMatchSqlRepository",
]
