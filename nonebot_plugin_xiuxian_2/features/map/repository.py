from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol
import json
from ...infrastructure.database import DatabaseUnitOfWork


class MapRepository(Protocol):
    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...


class MapMovementSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def move(self, operation_id: str, user_id: str, expected_position: dict[str, Any], target_position: dict[str, Any], expected_stamina: int, cost: int) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = {key: str(dict(expected_position)[key]) for key in ("realm", "heaven", "node_id")}
        target = {key: str(dict(target_position)[key]) for key in ("realm", "heaven", "node_id")}
        expected_stamina, cost = int(expected_stamina), int(cost)
        if not operation_id or expected_stamina < 0 or cost <= 0 or expected == target:
            raise ValueError("valid operation, distinct positions and positive cost are required")
        payload = json.dumps([user_id, expected, target, expected_stamina, cost], ensure_ascii=True, sort_keys=True)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            old = uow.query_one("SELECT payload,stamina FROM map_movement_operations WHERE operation_id=?", (operation_id,))
            if old:
                return {"status": "duplicate" if str(old["payload"]) == payload else "state_changed", "stamina": int(old["stamina"]) if str(old["payload"]) == payload else expected_stamina}
            user = uow.query_one("SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None:
                return {"status": "user_missing", "stamina": expected_stamina}
            stamina = int(user["user_stamina"] or 0)
            if stamina != expected_stamina:
                return {"status": "state_changed", "stamina": stamina}
            if stamina < cost:
                return {"status": "stamina_insufficient", "stamina": stamina}
            row = uow.query_one("SELECT realm,heaven,node_id,visited_nodes FROM player_data.map_status WHERE user_id=?", (user_id,))
            if row is None or (str(row["realm"]), str(row["heaven"]), str(row["node_id"])) != tuple(expected.values()):
                return {"status": "state_changed", "stamina": stamina}
            visited = row["visited_nodes"]
            try:
                visited = json.loads(visited) if isinstance(visited, str) else visited
            except json.JSONDecodeError:
                visited = []
            visited = [str(item) for item in visited] if isinstance(visited, list) else []
            if target["node_id"] not in visited:
                visited.append(target["node_id"])
            remaining = stamina - cost
            uow.execute("UPDATE player_data.map_status SET realm=?,heaven=?,node_id=?,visited_nodes=? WHERE user_id=?", (*target.values(), json.dumps(visited, ensure_ascii=False), user_id))
            uow.execute("UPDATE user_xiuxian SET user_stamina=? WHERE user_id=?", (remaining, user_id))
            uow.execute("INSERT INTO map_movement_operations(operation_id,payload,stamina) VALUES(?,?,?)", (operation_id, payload, remaining))
            return {"status": "applied", "stamina": remaining}


class MapHomeReturnSqlRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def return_home(self, operation_id: str, user_id: str) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        if not operation_id or not user_id:
            raise ValueError("operation and user are required")
        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            previous = uow.query_one("SELECT payload,result_status,realm,heaven,node_id,node_name FROM map_home_return_operations WHERE operation_id=?", (operation_id,))
            if previous:
                if str(previous["payload"]) != payload:
                    return {"status": "operation_conflict"}
                result = {"status": "duplicate" if str(previous["result_status"]) == "applied" else str(previous["result_status"])}
                result.update({key: str(previous[key] or "") for key in ("realm", "heaven", "node_id", "node_name")})
                return result
            def record(status: str, **values: str) -> dict[str, Any]:
                fields = {"realm": "", "heaven": "", "node_id": "", "node_name": "", **values}
                uow.execute("INSERT INTO map_home_return_operations(operation_id,payload,result_status,realm,heaven,node_id,node_name) VALUES(?,?,?,?,?,?,?)", (operation_id, payload, status, fields["realm"], fields["heaven"], fields["node_id"], fields["node_name"]))
                return {"status": status, **fields}
            table = uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='dongfu_status'")
            if table is None:
                return record("dongfu_missing")
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(dongfu_status)")}
            selected = "built,realm,heaven,node_id" + (",node_name" if "node_name" in columns else "")
            dongfu = uow.query_one(f"SELECT {selected} FROM dongfu_status WHERE user_id=?", (user_id,))
            if dongfu is None or int(dongfu["built"] or 0) != 1:
                return record("dongfu_missing")
            realm, heaven, node_id = str(dongfu["realm"] or ""), str(dongfu["heaven"] or ""), str(dongfu["node_id"] or "")
            if not all((realm, heaven, node_id)):
                return record("dongfu_invalid")
            position = uow.query_one("SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='map_status'")
            if position is None:
                return record("position_missing")
            current = uow.query_one("SELECT visited_nodes FROM map_status WHERE user_id=?", (user_id,))
            if current is None:
                return record("position_missing")
            visited = current["visited_nodes"]
            try:
                visited = json.loads(visited) if isinstance(visited, str) else visited
            except json.JSONDecodeError:
                visited = []
            visited = [str(item) for item in visited] if isinstance(visited, list) else []
            if node_id not in visited:
                visited.append(node_id)
            uow.execute("UPDATE map_status SET realm=?,heaven=?,node_id=?,visited_nodes=? WHERE user_id=?", (realm, heaven, node_id, json.dumps(visited, ensure_ascii=False), user_id))
            return record("applied", realm=realm, heaven=heaven, node_id=node_id, node_name=str(dongfu.get("node_name") or node_id))


class MapInteractiveSqlQueryRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def get_active(self, user_id: str) -> dict[str, Any] | None:
        user_id = str(user_id).strip()
        if not user_id:
            return None
        with DatabaseUnitOfWork(self.player_database) as uow:
            row = uow.query_one("SELECT action_id,state_json,settlement_json FROM map_interactive_actions WHERE user_id=? AND status='active'", (user_id,))
        if row is None:
            return None
        try:
            action = json.loads(str(row["state_json"]))
        except json.JSONDecodeError:
            action = {}
        if not isinstance(action, dict):
            action = {}
        action["action_id"] = str(row["action_id"])
        if row["settlement_json"]:
            try:
                settlement = json.loads(str(row["settlement_json"]))
            except json.JSONDecodeError:
                settlement = {}
            action["settlement"] = settlement if isinstance(settlement, dict) else {}
        return action


class LegacyMapRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def invoke(self, action: str, operation_id: str, user_id: str, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_map.transaction_service import (
            MapCombatLifecycleService, MapCombatSettlementService, MapDongfuBuildService,
            MapExploreSettlementService, MapExploreStartService, MapHomeReturnService,
            MapInteractiveActionService, MapMissionClaimService, MapMovementSettlementService,
            MapResourceRewardService, SeedPurchaseService,
        )
        mapping = {
            "move": (MapMovementSettlementService, "move", (self.game_database, self.player_database)),
            "return_home": (MapHomeReturnService, "return_home", (self.player_database,)),
            "interactive_start": (MapInteractiveActionService, "start", (self.game_database, self.player_database)),
            "interactive_finish": (MapInteractiveActionService, "save_settlement", (self.game_database, self.player_database)),
            "combat_start": (MapCombatLifecycleService, "start", (self.game_database, self.player_database)),
            "combat_settle": (MapCombatSettlementService, "settle", (self.game_database, self.player_database)),
            "explore_start": (MapExploreStartService, "start", (self.game_database, self.player_database)),
            "explore_settle": (MapExploreSettlementService, "settle", (self.game_database, self.player_database)),
            "resource_reward": (MapResourceRewardService, "settle", (self.game_database, self.player_database)),
            "mission_claim": (MapMissionClaimService, "claim", (self.game_database, self.player_database)),
            "purchase_seed": (SeedPurchaseService, "purchase", (self.game_database,)),
            "build_dongfu": (MapDongfuBuildService, "build", (self.game_database, self.player_database)),
        }
        cls, method, databases = mapping[action]
        return getattr(cls(*databases), method)(operation_id, user_id, **kwargs)


__all__ = ["LegacyMapRepository", "MapHomeReturnSqlRepository", "MapInteractiveSqlQueryRepository", "MapMovementSqlRepository", "MapRepository"]
