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


class MapInteractiveStartSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def start(self, operation_id: str, user_id: str, action_type: str, expected_stamina: int, stamina_cost: int, expected_position: dict[str, Any], expected_daily: dict[str, Any], daily_limit: int, expected_cooldown: str, action: dict[str, Any]) -> dict[str, Any]:
        operation_id, user_id, action_type = str(operation_id).strip(), str(user_id).strip(), str(action_type).strip()
        expected = {key: str(dict(expected_position)[key]) for key in ("realm", "heaven", "node_id")}
        daily = {str(key): str(value) for key, value in dict(expected_daily).items()}
        expected_stamina, stamina_cost, daily_limit = int(expected_stamina), int(stamina_cost), int(daily_limit)
        action = dict(action)
        required = {"action_id", "action", "start_ts", "ready_ts", "expire_ts", "cooldown_sec"}
        if not operation_id or not user_id or not action_type or min(expected_stamina, stamina_cost, daily_limit) < 0 or not daily.get("date") or not required.issubset(action) or str(action["action_id"]) != operation_id or str(action["action"]) != action_type:
            raise ValueError("valid interactive action snapshots are required")
        identity = json.dumps([user_id, action_type], ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        action_json = json.dumps(action, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            previous = uow.query_one("SELECT payload,result_status,stamina,action_json FROM map_interactive_start_operations WHERE operation_id=?", (operation_id,))
            if previous:
                if str(previous["payload"]) != identity:
                    return {"status": "operation_conflict", "stamina": expected_stamina, "action": {}}
                result = {"status": "duplicate" if str(previous["result_status"]) == "applied" else str(previous["result_status"]), "stamina": int(previous["stamina"] or 0)}
                try: result["action"] = json.loads(str(previous["action_json"]))
                except json.JSONDecodeError: result["action"] = {}
                return result
            user = uow.query_one("SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None: status, stamina = "user_missing", expected_stamina
            else: stamina = int(user["user_stamina"] or 0); status = "state_changed" if stamina != expected_stamina else ""
            row = uow.query_one("SELECT realm,heaven,node_id FROM player_data.map_status WHERE user_id=?", (user_id,))
            if not status and (row is None or (str(row["realm"]), str(row["heaven"]), str(row["node_id"])) != tuple(expected.values())): status = "state_changed"
            limit_row = uow.query_one("SELECT date,gather_count,resource_total_count FROM player_data.map_daily_limit WHERE user_id=?", (user_id,))
            if not status and (limit_row is None or (str(limit_row["date"]), str(limit_row["gather_count"]), str(limit_row["resource_total_count"])) != (daily["date"], daily.get("gather_count", "0"), daily.get("resource_total_count", "0"))): status = "state_changed"
            if not status and limit_row is not None and int(limit_row["gather_count"] or 0) >= daily_limit: status = "limit_reached"
            if not status and stamina < stamina_cost: status = "stamina_insufficient"
            if not status:
                active = uow.query_one("SELECT action_id,state_json,expires_at FROM player_data.map_interactive_actions WHERE user_id=? AND status='active'", (user_id,))
                if active is not None: status, action = "already_running", json.loads(str(active["state_json"]))
            uow.execute("INSERT INTO map_interactive_start_operations(operation_id,payload,result_status,stamina,action_json) VALUES(?,?,?,?,?)", (operation_id, identity, status or "applied", stamina - stamina_cost if not status else stamina, action_json if not status else json.dumps(action, ensure_ascii=True, sort_keys=True)))
            if status: return {"status": status, "stamina": stamina, "action": action if status == "already_running" else {}}
            remaining = stamina - stamina_cost
            uow.execute("UPDATE user_xiuxian SET user_stamina=? WHERE user_id=?", (remaining, user_id))
            uow.execute("INSERT INTO player_data.map_interactive_actions(user_id,action_id,action_type,status,state_json,settlement_json,ready_at,expires_at,cooldown_seconds,updated_at) VALUES(?,?,?,'active',?,'',?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET action_id=excluded.action_id,action_type=excluded.action_type,status='active',state_json=excluded.state_json,settlement_json='',ready_at=excluded.ready_at,expires_at=excluded.expires_at,cooldown_seconds=excluded.cooldown_seconds,updated_at=excluded.updated_at", (user_id, operation_id, action_type, action_json, str(action["ready_ts"]), str(action["expire_ts"]), int(action["cooldown_sec"]), str(action["start_ts"])))
            return {"status": "applied", "stamina": remaining, "action": action}


class MapInteractiveFailureSqlRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def finish_failure(self, operation_id: str, user_id: str, action_id: str, outcome: str, cooldown_until: str) -> dict[str, Any]:
        operation_id, user_id, action_id = str(operation_id).strip(), str(user_id).strip(), str(action_id).strip()
        outcome, cooldown_until = str(outcome).strip(), str(cooldown_until).strip()
        if not operation_id or not user_id or not action_id or outcome not in {"expired", "failed", "invalid"} or not cooldown_until:
            raise ValueError("valid terminal action is required")
        payload = json.dumps([user_id, action_id, outcome, cooldown_until], ensure_ascii=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            previous = uow.query_one("SELECT payload,result_status,action_json FROM map_interactive_terminal_operations WHERE operation_id=?", (operation_id,))
            if previous:
                if str(previous["payload"]) != payload:
                    return {"status": "operation_conflict", "action": {}}
                try: action = json.loads(str(previous["action_json"]))
                except json.JSONDecodeError: action = {}
                return {"status": "duplicate" if str(previous["result_status"]) == "applied" else str(previous["result_status"]), "action": action}
            row = uow.query_one("SELECT state_json FROM map_interactive_actions WHERE user_id=? AND action_id=? AND status='active'", (user_id, action_id))
            action: dict[str, Any] = {}
            status = "state_changed"
            if row is not None:
                try: action = json.loads(str(row["state_json"]))
                except json.JSONDecodeError: action = {}
                action["action_id"] = action_id
                uow.execute("UPDATE map_interactive_actions SET status=?,updated_at=? WHERE user_id=? AND action_id=? AND status='active'", (outcome, cooldown_until, user_id, action_id))
                uow.execute("INSERT INTO map_cooldown(user_id,gather_cd_until) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET gather_cd_until=excluded.gather_cd_until", (user_id, cooldown_until))
                status = "applied"
            uow.execute("INSERT INTO map_interactive_terminal_operations(operation_id,payload,result_status,action_json) VALUES(?,?,?,?)", (operation_id, payload, status, json.dumps(action, ensure_ascii=True, sort_keys=True)))
            return {"status": status, "action": action}


class MapInteractiveSettlementSqlRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def save_settlement(self, user_id: str, action_id: str, settlement: dict[str, Any]) -> dict[str, Any]:
        user_id, action_id = str(user_id).strip(), str(action_id).strip()
        if not user_id or not action_id or not isinstance(settlement, dict):
            raise ValueError("user, action and settlement are required")
        value = json.dumps(settlement, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            row = uow.query_one("SELECT state_json,settlement_json FROM map_interactive_actions WHERE user_id=? AND action_id=? AND status='active'", (user_id, action_id))
            if row is None:
                return {"status": "state_changed", "action": {}}
            try: action = json.loads(str(row["state_json"]))
            except json.JSONDecodeError: action = {}
            action["action_id"] = action_id
            if row["settlement_json"]:
                try: action["settlement"] = json.loads(str(row["settlement_json"]))
                except json.JSONDecodeError: action["settlement"] = {}
                return {"status": "duplicate", "action": action}
            uow.execute("UPDATE map_interactive_actions SET settlement_json=?,updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND action_id=? AND status='active'", (value, user_id, action_id))
            action["settlement"] = settlement
            return {"status": "applied", "action": action}


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


__all__ = ["LegacyMapRepository", "MapHomeReturnSqlRepository", "MapInteractiveFailureSqlRepository", "MapInteractiveSettlementSqlRepository", "MapInteractiveSqlQueryRepository", "MapInteractiveStartSqlRepository", "MapMovementSqlRepository", "MapRepository"]
