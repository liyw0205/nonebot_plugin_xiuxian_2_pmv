from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from ...core.numeric import exact_crystal_integer, require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AuctionSettlementResult:
    status: str
    operation_id: str
    session_id: str = ""
    results: tuple[dict[str, Any], ...] = ()
    reason: str = ""
    subject: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


# ``XiuConfig.max_goods_num`` defaults to 1000; mirror it so a repository that is
# built without an explicit cap never falls back to "one copy is a full backpack".
DEFAULT_MAX_GOODS_NUM = 1000


def configured_max_goods_num() -> int:
    """Read the configured per-item backpack cap without importing config eagerly."""

    try:
        from ...xiuxian.xiuxian_config import XiuConfig

        return max(int(XiuConfig().max_goods_num), 1)
    except Exception:  # pragma: no cover - isolated maintenance contexts
        return DEFAULT_MAX_GOODS_NUM


class _SettlementRejected(Exception):
    def __init__(self, result: AuctionSettlementResult) -> None:
        self.result = result


SETTLEMENT_WRITE_REJECTIONS = frozenset(
    f"auction_settlement_{step}_rejected" for step in (
        "inventory_write", "balance_write", "pending_write", "history_write",
        "item_delete", "session_write", "receipt_write",
    )
)


class _SettlementWrites:
    """Keep cumulative expectations until every settlement write has finished."""

    def __init__(self, uow: DatabaseUnitOfWork, operation_id: str, session_id: str) -> None:
        self.uow, self.operation_id, self.session_id = uow, operation_id, session_id
        self.rows: dict[tuple, tuple[dict, str, str]] = {}

    def reject(self, reason: str, subject: str, status: str = "state_changed") -> None:
        raise _SettlementRejected(AuctionSettlementResult(
            status, self.operation_id, self.session_id, reason=reason, subject=subject,
        ))

    def affected(self, cursor: Any, count: int, reason: str, subject: str) -> None:
        if cursor.rowcount != count:
            self.reject(reason, subject)

    def before(self, sql: str, params: tuple) -> dict | None:
        previous = self.rows.get((sql, params))
        return dict(previous[0]) if previous else self.uow.query_one(sql, params)

    def expect(self, sql: str, params: tuple, expected: dict, reason: str, subject: str) -> None:
        self.rows[(sql, params)] = (expected, reason, subject)
        self._check(sql, params, expected, reason, subject)

    def _check(self, sql: str, params: tuple, expected: dict, reason: str, subject: str) -> None:
        actual = self.uow.query_one(sql, params)
        if actual is None or any(
            (exact_crystal_integer(actual[key]) != value if isinstance(value, int)
             else actual[key] != value) for key, value in expected.items()
        ):
            self.reject(reason, subject)

    def verify(self) -> None:
        for (sql, params), (expected, reason, subject) in self.rows.items():
            self._check(sql, params, expected, reason, subject)


class AuctionSettlementSqlRepository:
    """Atomic auction-session settlement owned by the auction feature."""

    def __init__(
        self,
        database: str | Path,
        max_goods_num: int | None = None,
        *,
        item_type_resolver: Callable[[int], Mapping[str, Any] | None] | None = None,
    ) -> None:
        self.database = str(database)
        self.max_goods_num = max(
            int(max_goods_num) if max_goods_num is not None else configured_max_goods_num(),
            1,
        )
        self.item_type_resolver = item_type_resolver

    @staticmethod
    def _payload(value: Mapping[str, Any]) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _resolve_default_item_type(item_id: int) -> str | None:
        try:
            from ...xiuxian.xiuxian_utils.item_json import Items

            info = Items().get_data_by_item_id(item_id)
        except (ImportError, OSError, RuntimeError, ValueError):
            return None
        if not info:
            return None
        return str(info.get("type") or info.get("item_type") or "") or None

    def _item_type(self, item_id: int, resolved: Mapping[str, str]) -> str | None:
        explicit = resolved.get(str(item_id))
        if explicit:
            return str(explicit)
        if self.item_type_resolver is not None:
            try:
                info = self.item_type_resolver(item_id)
            except (ImportError, OSError, RuntimeError, ValueError):
                info = None
            if info:
                return str(info.get("type") or info.get("item_type") or "") or None
            return None
        return self._resolve_default_item_type(item_id)

    def settle_active(
        self,
        operation_id: str,
        *,
        end_time: float,
        fee_rate: float,
        item_types: Mapping[int, str],
    ) -> AuctionSettlementResult:
        try:
            return self._settle_active(
                operation_id,
                end_time=end_time,
                fee_rate=fee_rate,
                item_types=item_types,
            )
        except _SettlementRejected as rejected:
            return rejected.result

    def _settle_active(
        self,
        operation_id: str,
        *,
        end_time: float,
        fee_rate: float,
        item_types: Mapping[int, str],
    ) -> AuctionSettlementResult:
        operation_id = str(operation_id).strip()
        normalized_types = {str(int(key)): str(value) for key, value in item_types.items()}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            previous = uow.query_one(
                "SELECT action,payload,result FROM auction_session_operations WHERE operation_id=?",
                (operation_id,),
            )
            session = uow.query_one(
                "SELECT session_id,start_time FROM auction_sessions WHERE status='active'"
            )
            session_id = str(session["session_id"]) if session is not None else ""
            payload = self._payload(
                {"session_id": session_id, "fee_rate": float(fee_rate), "item_types": normalized_types}
            )
            if previous is not None:
                try:
                    old_payload = json.loads(str(previous["payload"]))
                except (TypeError, ValueError):
                    old_payload = None
                same_request = (
                    isinstance(old_payload, dict)
                    and str(old_payload.get("fee_rate")) == str(float(fee_rate))
                    and dict(old_payload.get("item_types") or {}) == normalized_types
                    and (not session_id or str(old_payload.get("session_id")) == session_id)
                )
                if str(previous["action"]) != "finish" or not same_request:
                    return AuctionSettlementResult(
                        "state_changed", operation_id,
                        reason="auction_settlement_operation_conflict", subject=operation_id,
                    )
                value = json.loads(str(previous["result"]))
                return AuctionSettlementResult(
                    "duplicate", operation_id, str(value.get("session_id", "")),
                    tuple(dict(item) for item in value.get("results", ())),
                )

            if session is None:
                return AuctionSettlementResult("empty", operation_id)
            rows = uow.query_all(
                "SELECT id,item_id,name,start_price,seller_id,seller_name,bids,is_system,last_bid_time "
                "FROM auction_current ORDER BY id"
            )
            writes = _SettlementWrites(uow, operation_id, session_id)
            results: list[dict[str, Any]] = []
            for row in rows:
                auction_id = str(row["id"])
                item_id = int(row["item_id"])
                name = str(row["name"])
                seller_id, seller_name = str(row["seller_id"]), str(row["seller_name"])
                is_system = bool(row["is_system"])
                bids_value = json.loads(row["bids"] or "{}")
                bids = {str(key): int(value) for key, value in bids_value.items()}
                winner_id = final_price = winner_name = None
                fee = earnings = 0
                status = "流拍"
                delivery = "none"
                item_type = self._item_type(item_id, normalized_types)
                if (bids or not is_system) and not item_type:
                    writes.reject("item_missing", str(item_id), "item_missing")
                if bids:
                    winner_id, final_price = max(bids.items(), key=lambda entry: entry[1])
                    winner = uow.query_one(
                        "SELECT user_name FROM user_xiuxian WHERE user_id=?", (winner_id,)
                    )
                    seller = True if is_system else uow.query_one(
                        "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (seller_id,)
                    )
                    if winner is None or not seller:
                        writes.reject("participant_missing", winner_id if winner is None else seller_id,
                                      "participant_missing")
                    winner_name = str(winner["user_name"] or winner_id)
                    # A winner who already holds a full stack still owns the item they
                    # paid for: park it instead of blocking the whole session.
                    if self._inventory_full(uow, winner_id, item_id):
                        self._park_pending(
                            uow,
                            auction_id=auction_id,
                            item_id=item_id,
                            item_name=name,
                            item_type=item_type,
                            owner_id=winner_id,
                            owner_name=winner_name,
                            role="winner",
                            reason="inventory_full",
                            writes=writes,
                        )
                        delivery = "pending"
                    else:
                        self._grant_item(uow, winner_id, item_id, name, item_type, writes)
                        delivery = "backpack"
                    fee = 0 if is_system else int(final_price * float(fee_rate))
                    earnings = 0 if is_system else final_price - fee
                    if not is_system:
                        self._credit_balance(uow, seller_id, earnings, writes)
                    for bidder_id, locked in bids.items():
                        if bidder_id != winner_id:
                            self._credit_balance(uow, bidder_id, locked, writes)
                    status = "待领取" if delivery == "pending" else "成交"
                elif not is_system:
                    if uow.query_one(
                        "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (seller_id,)
                    ) is None:
                        writes.reject("participant_missing", seller_id, "participant_missing")
                    if self._inventory_full(uow, seller_id, item_id):
                        self._park_pending(
                            uow,
                            auction_id=auction_id,
                            item_id=item_id,
                            item_name=name,
                            item_type=item_type,
                            owner_id=seller_id,
                            owner_name=seller_name,
                            role="seller",
                            reason="inventory_full",
                            writes=writes,
                        )
                        status = "待领取"
                        delivery = "pending"
                    else:
                        self._grant_item(uow, seller_id, item_id, name, item_type, writes)
                        delivery = "backpack"

                record = {
                    "auction_id": auction_id,
                    "item_id": item_id,
                    "item_name": name,
                    "start_price": int(row["start_price"]),
                    "final_price": final_price,
                    "seller_id": seller_id,
                    "seller_name": seller_name,
                    "winner_id": winner_id,
                    "winner_name": winner_name,
                    "status": status,
                    "fee": fee,
                    "seller_earnings": earnings,
                    "delivery": delivery,
                    "pending_reason": "inventory_full" if delivery == "pending" else "",
                    "start_time": float(row["last_bid_time"] or session["start_time"]),
                    "end_time": float(end_time),
                }
                cursor = uow.execute(
                    "INSERT INTO auction_history (auction_id,item_id,item_name,start_price,final_price,"
                    "seller_id,seller_name,winner_id,winner_name,status,fee,seller_earnings,start_time,end_time) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    tuple(record[key] for key in (
                        "auction_id", "item_id", "item_name", "start_price", "final_price",
                        "seller_id", "seller_name", "winner_id", "winner_name", "status", "fee",
                        "seller_earnings", "start_time", "end_time",
                    )),
                )
                reason = "auction_settlement_history_write_rejected"
                writes.affected(cursor, 1, reason, auction_id)
                history = {key: value for key, value in record.items()
                           if key not in {"delivery", "pending_reason"}}
                writes.expect("SELECT * FROM auction_history WHERE id=?", (cursor.lastrowid,),
                              history, reason, auction_id)
                results.append(record)

            writes.affected(uow.execute("DELETE FROM auction_current"), len(rows),
                            "auction_settlement_item_delete_rejected", session_id)
            cursor = uow.execute(
                "UPDATE auction_sessions SET status='settled',finish_operation_id=?,settled_at=? "
                "WHERE session_id=? AND status='active'",
                (operation_id, float(end_time), session_id),
            )
            reason = "auction_settlement_session_write_rejected"
            writes.affected(cursor, 1, reason, session_id)
            writes.expect("SELECT * FROM auction_sessions WHERE session_id=?", (session_id,),
                          {"status": "settled", "finish_operation_id": operation_id,
                           "settled_at": float(end_time)}, reason, session_id)
            result_value = {"session_id": session_id, "results": results}
            receipt = {"action": "finish", "payload": payload, "result": self._payload(result_value)}
            cursor = uow.execute(
                "INSERT INTO auction_session_operations(operation_id,action,payload,result) VALUES(?,?,?,?)",
                (operation_id, receipt["action"], receipt["payload"], receipt["result"]),
            )
            reason = "auction_settlement_receipt_write_rejected"
            writes.affected(cursor, 1, reason, operation_id)
            writes.expect("SELECT * FROM auction_session_operations WHERE operation_id=?", (operation_id,),
                          receipt, reason, operation_id)
            writes.verify()
            if uow.query_one("SELECT id FROM auction_current LIMIT 1") is not None:
                writes.reject("auction_settlement_item_delete_rejected", session_id)
            return AuctionSettlementResult("settled", operation_id, session_id, tuple(results))

    @staticmethod
    def _credit_balance(uow: DatabaseUnitOfWork, user_id: str, amount: int, writes: _SettlementWrites) -> None:
        sql, params = "SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,)
        before = writes.before(sql, params)
        if before is None:
            writes.reject("participant_missing", user_id, "participant_missing")
        expected = require_exact_integer(before["stone"] or 0, field="stone", subject=user_id) + amount
        cursor = uow.execute(
            "UPDATE user_xiuxian SET stone=xiuxian_int_store(xiuxian_int_add(COALESCE(stone,0),?,'stone',?)) WHERE user_id=?",
            (amount, user_id, user_id),
        )
        reason = "auction_settlement_balance_write_rejected"
        writes.affected(cursor, 1, reason, user_id)
        writes.expect(sql, params, {"stone": expected}, reason, user_id)

    def _inventory_full(self, uow: DatabaseUnitOfWork, user_id: str, item_id: int) -> bool:
        row = uow.query_one(
            "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id)
        )
        return bool(row and int(row["goods_num"] or 0) >= self.max_goods_num)

    @staticmethod
    def _park_pending(
        uow: DatabaseUnitOfWork,
        *,
        auction_id: str,
        item_id: int,
        item_name: str,
        item_type: str | None,
        owner_id: str,
        owner_name: str,
        role: str,
        reason: str,
        writes: _SettlementWrites,
    ) -> None:
        """Store an item that could not enter a backpack so it is never destroyed.

        The write shares the settlement transaction: if this table is missing (an
        unmigrated database) the insert fails and the whole settlement rolls back,
        leaving the session pending instead of silently dropping the item.
        """

        cursor = uow.execute(
            "INSERT INTO auction_pending_items(auction_id,item_id,item_name,item_type,"
            "owner_id,owner_name,role,reason) VALUES(?,?,?,?,?,?,?,?)",
            (
                auction_id,
                int(item_id),
                item_name,
                str(item_type or ""),
                owner_id,
                owner_name,
                role,
                reason,
            ),
        )
        failure = "auction_settlement_pending_write_rejected"
        writes.affected(cursor, 1, failure, auction_id)
        writes.expect("SELECT * FROM auction_pending_items WHERE id=?", (cursor.lastrowid,),
                      {"auction_id": auction_id, "item_id": int(item_id), "item_name": item_name,
                       "item_type": str(item_type or ""), "owner_id": owner_id, "owner_name": owner_name,
                       "role": role, "reason": reason, "claimed_at": None, "claim_operation_id": None},
                      failure, auction_id)

    @staticmethod
    def _grant_item(uow: DatabaseUnitOfWork, user_id: str, item_id: int, name: str,
                    item_type: str, writes: _SettlementWrites) -> None:
        sql, params = "SELECT * FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id)
        before = writes.before(sql, params) or {}
        subject = f"{user_id}:{item_id}"
        expected = {"goods_name": name, "goods_type": item_type,
                    "goods_num": require_exact_integer(before.get("goods_num") or 0, field="goods_num", subject=subject) + 1,
                    "bind_num": require_exact_integer(before.get("bind_num") or 0, field="bind_num", subject=subject) + 1}
        cursor = uow.execute(
            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
            "VALUES(?,?,?,?,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1) "
            "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,"
            "goods_type=excluded.goods_type,goods_num=COALESCE(back.goods_num,0)+1,"
            "bind_num=COALESCE(back.bind_num,0)+1,update_time=CURRENT_TIMESTAMP",
            (user_id, item_id, name, item_type),
        )
        reason = "auction_settlement_inventory_write_rejected"
        writes.affected(cursor, 1, reason, subject)
        writes.expect(sql, params, expected, reason, subject)


__all__ = [
    "AuctionSettlementResult",
    "AuctionSettlementSqlRepository",
    "DEFAULT_MAX_GOODS_NUM",
    "configured_max_goods_num",
]
