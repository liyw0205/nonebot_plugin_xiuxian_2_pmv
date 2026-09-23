from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class AuctionSettlementResult:
    status: str
    operation_id: str
    session_id: str = ""
    results: tuple[dict[str, Any], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"settled", "duplicate"}


class _SettlementRejected(Exception):
    def __init__(self, result: AuctionSettlementResult) -> None:
        self.result = result


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
        self.max_goods_num = max(int(max_goods_num or 1), 1)
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
                    return AuctionSettlementResult("state_changed", operation_id)
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
                item_type = self._item_type(item_id, normalized_types)
                if (bids or not is_system) and not item_type:
                    raise _SettlementRejected(
                        AuctionSettlementResult("item_missing", operation_id, session_id)
                    )
                if bids:
                    winner_id, final_price = max(bids.items(), key=lambda entry: entry[1])
                    winner = uow.query_one(
                        "SELECT user_name FROM user_xiuxian WHERE user_id=?", (winner_id,)
                    )
                    seller = True if is_system else uow.query_one(
                        "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (seller_id,)
                    )
                    if winner is None or not seller:
                        raise _SettlementRejected(
                            AuctionSettlementResult("participant_missing", operation_id, session_id)
                        )
                    winner_name = str(winner["user_name"] or winner_id)
                    if self._inventory_full(uow, winner_id, item_id):
                        raise _SettlementRejected(
                            AuctionSettlementResult("inventory_full", operation_id, session_id)
                        )
                    self._grant_item(uow, winner_id, item_id, name, item_type)
                    fee = 0 if is_system else int(final_price * float(fee_rate))
                    earnings = 0 if is_system else final_price - fee
                    if not is_system:
                        uow.execute(
                            "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(? AS REAL) WHERE user_id=?",
                            (earnings, seller_id),
                        )
                    for bidder_id, locked in bids.items():
                        if bidder_id != winner_id and uow.execute(
                            "UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(? AS REAL) WHERE user_id=?",
                            (locked, bidder_id),
                        ).rowcount != 1:
                            raise _SettlementRejected(
                                AuctionSettlementResult("participant_missing", operation_id, session_id)
                            )
                    status = "成交"
                elif not is_system:
                    if uow.query_one(
                        "SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (seller_id,)
                    ) is None:
                        raise _SettlementRejected(
                            AuctionSettlementResult("participant_missing", operation_id, session_id)
                        )
                    if self._inventory_full(uow, seller_id, item_id):
                        status = "卖家背包满流拍"
                    else:
                        self._grant_item(uow, seller_id, item_id, name, item_type)

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
                    "start_time": float(row["last_bid_time"] or session["start_time"]),
                    "end_time": float(end_time),
                }
                uow.execute(
                    "INSERT INTO auction_history (auction_id,item_id,item_name,start_price,final_price,"
                    "seller_id,seller_name,winner_id,winner_name,status,fee,seller_earnings,start_time,end_time) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    tuple(record[key] for key in (
                        "auction_id", "item_id", "item_name", "start_price", "final_price",
                        "seller_id", "seller_name", "winner_id", "winner_name", "status", "fee",
                        "seller_earnings", "start_time", "end_time",
                    )),
                )
                results.append(record)

            uow.execute("DELETE FROM auction_current")
            uow.execute(
                "UPDATE auction_sessions SET status='settled',finish_operation_id=?,settled_at=? "
                "WHERE session_id=? AND status='active'",
                (operation_id, float(end_time), session_id),
            )
            result_value = {"session_id": session_id, "results": results}
            uow.execute(
                "INSERT INTO auction_session_operations(operation_id,action,payload,result) VALUES(?,?,?,?)",
                (operation_id, "finish", payload, self._payload(result_value)),
            )
            return AuctionSettlementResult("settled", operation_id, session_id, tuple(results))

    def _inventory_full(self, uow: DatabaseUnitOfWork, user_id: str, item_id: int) -> bool:
        row = uow.query_one(
            "SELECT goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id)
        )
        return bool(row and int(row["goods_num"] or 0) >= self.max_goods_num)

    @staticmethod
    def _grant_item(uow: DatabaseUnitOfWork, user_id: str, item_id: int, name: str, item_type: str) -> None:
        uow.execute(
            "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
            "VALUES(?,?,?,?,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1) "
            "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,"
            "goods_type=excluded.goods_type,goods_num=COALESCE(back.goods_num,0)+1,"
            "bind_num=COALESCE(back.bind_num,0)+1,update_time=CURRENT_TIMESTAMP",
            (user_id, item_id, name, item_type),
        )


__all__ = ["AuctionSettlementResult", "AuctionSettlementSqlRepository"]
