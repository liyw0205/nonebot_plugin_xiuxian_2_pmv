from __future__ import annotations

from pathlib import Path
import json
from datetime import datetime
from typing import Any, Callable, Protocol, Sequence

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


class CombatSettlementRepository(Protocol):
    def settle(self, operation_id: str, user_id: str, expected_daily: dict[str, Any], snapshot: str,
               daily_limit: int, stone: int, items: tuple[dict[str, Any], ...], max_goods_num: int) -> Any: ...


class CombatSettlementSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock: Any | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock or SystemClock()

    def settle(self, operation_id: str, user_id: str, expected_daily: dict[str, Any], snapshot: str, daily_limit: int, stone: int, items: tuple[dict[str, Any], ...], max_goods_num: int) -> dict[str, Any]:
        operation_id, user_id, snapshot = str(operation_id).strip(), str(user_id), str(snapshot)
        expected = {str(key): str(value) for key, value in dict(expected_daily).items()}
        daily_limit, stone, max_goods_num = int(daily_limit), int(stone), int(max_goods_num)
        rewards = tuple((int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"])) for item in items if int(item["amount"]) > 0)
        if not operation_id or not snapshot or min(daily_limit, stone, max_goods_num) < 0 or not expected.get("date"):
            raise ValueError("valid operation, daily state and combat snapshot are required")
        payload = json.dumps([user_id, expected, snapshot, daily_limit, stone, rewards, max_goods_num], ensure_ascii=True, sort_keys=True)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            previous = uow.query_one("SELECT payload,stone,rewards FROM map_combat_settlement_operations WHERE operation_id=?", (operation_id,))
            if previous:
                if str(previous["payload"]) != payload:
                    return {"status": "state_changed", "stone": 0, "rewards": ()}
                return {"status": "duplicate", "stone": int(previous["stone"]), "rewards": tuple(tuple(map(int, value)) for value in json.loads(str(previous["rewards"]))) }
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return {"status": "user_missing", "stone": 0, "rewards": ()}
            daily = uow.query_one("SELECT date,combat_count,resource_total_count FROM player_data.map_daily_limit WHERE user_id=?", (user_id,))
            stored = uow.query_one("SELECT snapshot FROM player_data.map_combat_settlement WHERE user_id=?", (user_id,))
            if daily is None or (str(daily["date"]), str(daily["combat_count"]), str(daily["resource_total_count"])) != (expected["date"], expected.get("combat_count", "0"), expected.get("resource_total_count", "0")) or stored is None or str(stored["snapshot"] or "") != snapshot:
                return {"status": "state_changed", "stone": 0, "rewards": ()}
            if int(daily["combat_count"] or 0) >= daily_limit:
                return {"status": "limit_reached", "stone": 0, "rewards": ()}
            totals: dict[int, int] = {}
            metadata: dict[int, tuple[str, str]] = {}
            for item_id, name, item_type, amount in rewards:
                totals[item_id] = totals.get(item_id, 0) + amount
                metadata[item_id] = (name, item_type)
            for item_id, amount in totals.items():
                row = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                if (int(row["goods_num"]) if row else 0) + amount > max_goods_num:
                    return {"status": "inventory_full", "stone": 0, "rewards": ()}
            uow.execute("UPDATE player_data.map_daily_limit SET combat_count=?,resource_total_count=? WHERE user_id=?", (int(daily["combat_count"] or 0) + 1, int(daily["resource_total_count"] or 0) + 1, user_id))
            uow.execute("UPDATE player_data.map_combat_settlement SET snapshot=? WHERE user_id=?", ("", user_id))
            if stone:
                uow.execute("UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? WHERE user_id=?", (stone, user_id))
            now = self.clock.now()
            for item_id, amount in totals.items():
                name, item_type = metadata[item_id]
                uow.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time", (user_id, item_id, name, item_type, amount, now, now, amount))
            compact = tuple(sorted(totals.items()))
            uow.execute("INSERT INTO map_combat_settlement_operations(operation_id,payload,stone,rewards) VALUES(?,?,?,?)", (operation_id, payload, stone, json.dumps(compact)))
            return {"status": "applied", "stone": stone, "rewards": compact}


class LegacyCombatSettlementRepository:
    """Lazy adapter around the historical attached-database transaction."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def settle(self, operation_id: str, user_id: str, expected_daily: dict[str, Any], snapshot: str,
               daily_limit: int, stone: int, items: tuple[dict[str, Any], ...], max_goods_num: int) -> Any:
        try:
            from ...xiuxian.xiuxian_map.transaction_service import MapCombatSettlementService
        except (ImportError, RuntimeError, ValueError):
            return {"status": "not_ready", "stone": 0, "rewards": ()}
        return MapCombatSettlementService(self.game_database, self.player_database).settle(
            operation_id,
            user_id,
            expected_daily,
            snapshot,
            daily_limit,
            stone,
            items,
            max_goods_num,
        )


__all__ = ["CombatSettlementRepository", "CombatSettlementSqlRepository", "LegacyCombatSettlementRepository"]
