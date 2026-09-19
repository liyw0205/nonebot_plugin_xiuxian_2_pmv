from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from ...infrastructure.database import DatabaseUnitOfWork


class WorldEventClaimRepository(Protocol):
    def claim(self, operation_id: str, event_key: str, event_id: str, user_id: str, expected_claimed: Mapping[str, Any], stone: int, exp: int, items: Sequence[Mapping[str, Any]], max_goods_num: int) -> Any: ...


class WorldEventClaimSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    @staticmethod
    def _result(status: str, stone: int = 0, exp: int = 0) -> dict[str, Any]:
        return {"status": status, "stone": int(stone), "exp": int(exp)}

    def claim(self, operation_id: str, event_key: str, event_id: str, user_id: str, expected_claimed: Mapping[str, Any], stone: int, exp: int, items: Sequence[Mapping[str, Any]], max_goods_num: int) -> dict[str, Any]:
        operation_id, event_key, event_id, user_id = map(str, (operation_id, event_key, event_id, user_id))
        stone, exp, max_goods_num = max(0, int(stone)), max(0, int(exp)), int(max_goods_num)
        rewards = tuple((int(item["id"]), str(item.get("name", "")), str(item.get("type", item.get("item_type", ""))), int(item["amount"])) for item in items if int(item.get("amount", 0)) > 0)
        payload = json.dumps([event_key, event_id, user_id], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            uow.execute("CREATE TABLE IF NOT EXISTS demon_claim_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,stone INTEGER NOT NULL DEFAULT 0,exp INTEGER NOT NULL DEFAULT 0,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,stone,exp FROM demon_claim_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return self._result("state_changed")
                return self._result("duplicate", previous["stone"], previous["exp"])
            user = uow.query_one("SELECT 1 AS present FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return self._result("user_missing")
            row = uow.query_one("SELECT event_id,claimed FROM player_data.world_event_state WHERE user_id=?", (event_key,))
            if row is None or str(row["event_id"]) != event_id:
                return self._result("state_changed")
            try:
                current = json.loads(str(row["claimed"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                return self._result("state_changed")
            if current != dict(expected_claimed):
                return self._result("state_changed")
            if current.get(user_id):
                return self._result("already_claimed")
            for item_id, _, _, amount in rewards:
                inventory = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                if (int(inventory["goods_num"]) if inventory else 0) + amount > max_goods_num:
                    return self._result("inventory_full")
            current[user_id] = True
            uow.execute("UPDATE player_data.world_event_state SET claimed=? WHERE user_id=?", (json.dumps(current, ensure_ascii=False), event_key))
            uow.execute("UPDATE user_xiuxian SET stone=CAST(COALESCE(stone,0) AS REAL)+CAST(? AS REAL), exp=CAST(COALESCE(exp,0) AS REAL)+CAST(? AS REAL) WHERE user_id=?", (stone, exp, user_id))
            for item_id, name, item_type, amount in rewards:
                uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.goods_num,update_time=excluded.update_time", (user_id, item_id, name, item_type, amount, amount))
            uow.execute("INSERT INTO demon_claim_operations(operation_id,payload,stone,exp) VALUES(?,?,?,?)", (operation_id, payload, stone, exp))
            return self._result("applied", stone, exp)


class LegacyWorldEventClaimRepository:
    """Lazy adapter around the existing ATTACH DATABASE transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def claim(self, operation_id: str, event_key: str, event_id: str, user_id: str, expected_claimed: Mapping[str, Any], stone: int, exp: int, items: Sequence[Mapping[str, Any]], max_goods_num: int) -> Any:
        from ...xiuxian.xiuxian_world_events.transaction_service import DemonClaimService
        return DemonClaimService(self.game_database, self.player_database).claim(operation_id, event_key, event_id, user_id, expected_claimed, stone, exp, items, max_goods_num)


__all__ = ["LegacyWorldEventClaimRepository", "WorldEventClaimRepository", "WorldEventClaimSqlRepository"]
