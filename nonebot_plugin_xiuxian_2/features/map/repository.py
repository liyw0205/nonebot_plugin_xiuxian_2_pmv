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
    def __init__(self, player_database: str | Path, game_database: str | Path | None = None) -> None:
        self.player_database = str(player_database)
        self.game_database = str(game_database) if game_database else None

    def replay_start(self, operation_id: str, user_id: str, action_type: str) -> dict[str, Any] | None:
        operation_id, user_id, action_type = str(operation_id).strip(), str(user_id).strip(), str(action_type).strip()
        if not operation_id or not user_id or not action_type:
            raise ValueError("operation, user and action are required")
        if not self.game_database:
            return None
        payload = json.dumps([user_id, action_type], ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database) as uow:
            row = uow.query_one("SELECT payload,result_status,stamina,action_json FROM map_interactive_start_operations WHERE operation_id=?", (operation_id,))
        if row is None:
            return None
        if str(row["payload"]) != payload:
            return {"status": "operation_conflict", "stamina": 0, "action": {}}
        try:
            action = json.loads(str(row["action_json"]))
        except json.JSONDecodeError:
            action = {}
        return {"status": "duplicate" if str(row["result_status"]) == "applied" else str(row["result_status"]), "stamina": int(row["stamina"] or 0), "action": action if isinstance(action, dict) else {}}

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


class MapProjectionSqlRepository:
    """Read and normalize map daily limits and cooldown projections."""

    _COOLDOWN_FIELDS = frozenset(
        {"gather_cd_until", "combat_cd_until", "explore_start_cd_until"}
    )

    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def daily_limit(self, user_id: str, today: str) -> dict[str, int | str]:
        user_id, today = str(user_id).strip(), str(today).strip()
        if not user_id or not today:
            raise ValueError("user and date are required")
        fields = ("date", "gather_count", "combat_count", "explore_count", "resource_total_count")
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            row = uow.query_one(
                "SELECT date,gather_count,combat_count,explore_count,resource_total_count "
                "FROM map_daily_limit WHERE user_id=?",
                (user_id,),
            )
            if row is None or str(row["date"] or "") != today:
                values = (today, 0, 0, 0, 0)
                uow.execute(
                    "INSERT INTO map_daily_limit(user_id,date,gather_count,combat_count,explore_count,resource_total_count) "
                    "VALUES(?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET date=excluded.date,gather_count=0,combat_count=0,explore_count=0,resource_total_count=0",
                    (user_id, *values),
                )
                return dict(zip(fields, values))
            return {
                "date": str(row["date"]),
                **{field: int(row[field] or 0) for field in fields[1:]},
            }

    def cooldown_until(self, user_id: str, field: str) -> str | None:
        user_id, field = str(user_id).strip(), str(field).strip()
        if not user_id or field not in self._COOLDOWN_FIELDS:
            raise ValueError("valid cooldown field and user are required")
        with DatabaseUnitOfWork(self.player_database) as uow:
            row = uow.query_one(
                f"SELECT \"{field}\" FROM map_cooldown WHERE user_id=?",
                (user_id,),
            )
        if row is None or row[field] in (None, ""):
            return None
        return str(row[field])


class MapStatusSqlQueryRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def get(self, user_id: str) -> dict[str, Any] | None:
        user_id = str(user_id).strip()
        if not user_id:
            return None
        with DatabaseUnitOfWork(self.player_database) as uow:
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(map_status)")}
            if not columns:
                return None
            visited_field = ",visited_nodes" if "visited_nodes" in columns else ""
            row = uow.query_one(
                f"SELECT realm,heaven,node_id{visited_field} FROM map_status WHERE user_id=?",
                (user_id,),
            )
        if row is None or not all(str(row[field] or "") for field in ("realm", "heaven", "node_id")):
            return None
        visited = row["visited_nodes"] if "visited_nodes" in columns else []
        try:
            visited = json.loads(visited) if isinstance(visited, str) else visited
        except json.JSONDecodeError:
            visited = []
        return {
            "user_id": user_id,
            "realm": str(row["realm"]),
            "heaven": str(row["heaven"]),
            "node_id": str(row["node_id"]),
            "visited_nodes": [str(item) for item in visited] if isinstance(visited, list) else [],
        }


class MapStatusSqlWriteRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def upsert(
        self,
        user_id: str,
        realm: str,
        heaven: str,
        node_id: str,
        visited_nodes: list[str],
    ) -> dict[str, Any]:
        user_id = str(user_id).strip()
        if not user_id or not all(str(value).strip() for value in (realm, heaven, node_id)):
            raise ValueError("valid map status is required")
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(map_status)")}
            if not columns:
                raise RuntimeError("map_status schema is missing")
            if "visited_nodes" not in columns:
                uow.execute("ALTER TABLE map_status ADD COLUMN visited_nodes TEXT DEFAULT '[]'")
            current = uow.query_one("SELECT visited_nodes FROM map_status WHERE user_id=?", (user_id,))
            merged = self._merge_visited(current["visited_nodes"] if current else [], visited_nodes)
            uow.execute(
                "INSERT INTO map_status(user_id,realm,heaven,node_id,visited_nodes) VALUES(?,?,?,?,?) "
                "ON CONFLICT(user_id) DO UPDATE SET realm=excluded.realm,heaven=excluded.heaven,"
                "node_id=excluded.node_id,visited_nodes=excluded.visited_nodes",
                (user_id, str(realm), str(heaven), str(node_id), json.dumps(merged, ensure_ascii=False)),
            )
        return {"realm": str(realm), "heaven": str(heaven), "node_id": str(node_id), "visited_nodes": merged}

    @staticmethod
    def _merge_visited(current: Any, incoming: list[str]) -> list[str]:
        try:
            current = json.loads(current) if isinstance(current, str) else current
        except json.JSONDecodeError:
            current = []
        values = current if isinstance(current, list) else []
        values = [str(value) for value in values]
        for value in incoming:
            value = str(value)
            if value and value not in values:
                values.append(value)
        return values


class MapExploreStatusSqlQueryRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def get(self, user_id: str) -> dict[str, Any] | None:
        user_id = str(user_id).strip()
        if not user_id:
            return None
        with DatabaseUnitOfWork(self.player_database) as uow:
            columns = {
                str(row["name"])
                for row in uow.query_all("PRAGMA table_info(map_explore_status)")
            }
            if not columns:
                return None
            legacy_field = ",reward_plan" if "reward_plan" in columns else ""
            row = uow.query_one(
                "SELECT running,node_type,node_name,start_time,duration_min,settlement,"
                f"max_duration_min,interval_min{legacy_field} FROM map_explore_status WHERE user_id=?",
                (user_id,),
            )
        if row is None:
            return None
        settlement = self._blank_snapshot(row["settlement"])
        legacy = self._blank_snapshot(row["reward_plan"]) if "reward_plan" in columns else ""
        if not settlement and legacy.startswith("{") and legacy.endswith("}"):
            settlement = legacy
        return {
            "running": int(row["running"] or 0),
            "node_type": str(row["node_type"] or ""),
            "node_name": str(row["node_name"] or ""),
            "start_time": str(row["start_time"] or ""),
            "duration_min": int(row["duration_min"] or 0),
            "settlement": settlement,
            "max_duration_min": int(row["max_duration_min"] or 0),
            "interval_min": int(row["interval_min"] or 0),
            "reward_plan": "",
        }

    @staticmethod
    def _blank_snapshot(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        if not text or text.lower() in {"none", "null", "undefined"}:
            return ""
        return text


class MapMissionSqlQueryRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def get(self, user_id: str) -> dict[str, Any] | None:
        user_id = str(user_id).strip()
        if not user_id:
            return None
        with DatabaseUnitOfWork(self.player_database) as uow:
            row = uow.query_one(
                "SELECT date,mission_type,target,claimed,settlement FROM map_mission WHERE user_id=?",
                (user_id,),
            )
        if row is None:
            return None
        return {
            "date": str(row["date"] or ""),
            "mission_type": str(row["mission_type"] or ""),
            "target": int(row["target"] or 0),
            "claimed": int(row["claimed"] or 0),
            "settlement": str(row["settlement"] or ""),
        }


class MapDongfuSqlQueryRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def get(self, user_id: str) -> dict[str, Any] | None:
        user_id = str(user_id).strip()
        if not user_id:
            return None
        with DatabaseUnitOfWork(self.player_database) as uow:
            row = uow.query_one(
                "SELECT built,realm,heaven,node_id,node_name,node_type FROM dongfu_status WHERE user_id=?",
                (user_id,),
            )
        if row is None:
            return None
        return {
            "built": int(row["built"] or 0),
            "realm": str(row["realm"] or ""),
            "heaven": str(row["heaven"] or ""),
            "node_id": str(row["node_id"] or ""),
            "node_name": str(row["node_name"] or ""),
            "node_type": str(row["node_type"] or ""),
        }


class MapNearbyPlayersSqlQueryRepository:
    def __init__(self, player_database: str | Path, game_database: str | Path) -> None:
        self.player_database = str(player_database)
        self.game_database = str(game_database)

    def list(self, realm: str, heaven: str, node_id: str) -> list[dict[str, Any]]:
        realm, heaven, node_id = str(realm or ""), str(heaven or ""), str(node_id or "")
        if not realm or not heaven or not node_id:
            return []
        with DatabaseUnitOfWork(self.player_database) as uow:
            uow.attach_database(self.game_database, "game_data")
            rows = uow.query_all(
                "SELECT map_status.user_id,game_data.user_xiuxian.user_name,"
                "game_data.user_xiuxian.level,game_data.user_xiuxian.power "
                "FROM map_status JOIN game_data.user_xiuxian "
                "ON CAST(game_data.user_xiuxian.user_id AS TEXT)=CAST(map_status.user_id AS TEXT) "
                "WHERE map_status.realm=? AND map_status.heaven=? AND map_status.node_id=? "
                "ORDER BY map_status.rowid ASC",
                (realm, heaven, node_id),
            )
        return [
            {
                "user_id": str(row["user_id"]),
                "user_name": str(row["user_name"] or ""),
                "level": str(row["level"] or ""),
                "power": int(row["power"] or 0),
            }
            for row in rows
        ]


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


class MapResourceRewardSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock: Any | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.clock = clock

    def settle(self, operation_id: str, user_id: str, expected_daily: dict[str, Any], daily_limit: int, stone: int, items: list[dict[str, Any]] | tuple[dict[str, Any], ...], max_goods_num: int, *, action_id: str, action_settlement: dict[str, Any], cooldown_until: str) -> dict[str, Any]:
        operation_id, user_id, action_id = str(operation_id).strip(), str(user_id).strip(), str(action_id).strip()
        expected = {str(key): str(value) for key, value in dict(expected_daily).items()}
        daily_limit, stone, max_goods_num = int(daily_limit), int(stone), int(max_goods_num)
        rewards = tuple((int(item["id"]), str(item["name"]), str(item["type"]), int(item["amount"])) for item in items if int(item["amount"]) > 0)
        if not operation_id or not user_id or not action_id or not expected.get("date") or min(daily_limit, stone, max_goods_num) < 0 or not isinstance(action_settlement, dict) or not cooldown_until:
            raise ValueError("valid resource reward lifecycle is required")
        settlement_json = json.dumps(action_settlement, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        payload = json.dumps([user_id, expected, daily_limit, stone, rewards, max_goods_num, action_id, settlement_json, cooldown_until], ensure_ascii=True, sort_keys=True)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            old = uow.query_one("SELECT payload,stone,rewards FROM map_resource_reward_operations WHERE operation_id=?", (operation_id,))
            if old:
                if str(old["payload"]) != payload:
                    return {"status": "state_changed", "stone": 0, "rewards": ()}
                return {"status": "duplicate", "stone": int(old["stone"]), "rewards": tuple(tuple(map(int, value)) for value in json.loads(str(old["rewards"]))) }
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return {"status": "user_missing", "stone": 0, "rewards": ()}
            action = uow.query_one("SELECT status,settlement_json FROM player_data.map_interactive_actions WHERE user_id=? AND action_id=?", (user_id, action_id))
            if action is None or str(action["status"]) != "active" or str(action["settlement_json"] or "") != settlement_json:
                return {"status": "state_changed", "stone": 0, "rewards": ()}
            daily = uow.query_one("SELECT date,gather_count,resource_total_count FROM player_data.map_daily_limit WHERE user_id=?", (user_id,))
            if daily is None or (str(daily["date"]), str(daily["gather_count"]), str(daily["resource_total_count"])) != (expected["date"], expected.get("gather_count", "0"), expected.get("resource_total_count", "0")):
                return {"status": "state_changed", "stone": 0, "rewards": ()}
            if int(daily["gather_count"] or 0) >= daily_limit:
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
            uow.execute("UPDATE player_data.map_daily_limit SET gather_count=?,resource_total_count=? WHERE user_id=?", (int(daily["gather_count"] or 0) + 1, int(daily["resource_total_count"] or 0) + 1, user_id))
            if stone:
                uow.execute("UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? WHERE user_id=?", (stone, user_id))
            now = self.clock.now() if self.clock is not None else None
            now_value = now.isoformat() if now is not None else ""
            for item_id, amount in totals.items():
                name, item_type = metadata[item_id]
                uow.execute("INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time", (user_id, item_id, name, item_type, amount, now_value, now_value, amount))
            uow.execute("UPDATE player_data.map_interactive_actions SET status='completed',updated_at=? WHERE user_id=? AND action_id=? AND status='active'", (now_value or cooldown_until, user_id, action_id))
            uow.execute("INSERT INTO player_data.map_cooldown(user_id,gather_cd_until) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET gather_cd_until=excluded.gather_cd_until", (user_id, cooldown_until))
            compact = tuple(sorted(totals.items()))
            uow.execute("INSERT INTO map_resource_reward_operations(operation_id,payload,stone,rewards) VALUES(?,?,?,?)", (operation_id, payload, stone, json.dumps(compact)))
            return {"status": "applied", "stone": stone, "rewards": compact}


class MapExploreStartSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def start(self, operation_id: str, user_id: str, expected_stamina: int, stamina_cost: int, expected_position: dict[str, Any], expected_status: dict[str, Any], expected_daily: dict[str, Any], daily_limit: int, expected_cooldown: str, cooldown_until: str, new_status: dict[str, Any]) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        stamina, cost, limit = int(expected_stamina), int(stamina_cost), int(daily_limit)
        position = {key: str(value) for key, value in dict(expected_position).items()}
        status = {key: str(value) for key, value in dict(expected_status).items()}
        daily = {key: str(value) for key, value in dict(expected_daily).items()}
        target = {key: str(value) for key, value in dict(new_status).items()}
        expected_cooldown = "" if expected_cooldown is None else str(expected_cooldown)
        if not operation_id or not user_id or min(stamina, cost, limit) < 0 or not {"realm", "heaven", "node_id"}.issubset(position) or status.get("running", "0") != "0" or not daily.get("date") or target.get("running") != "1":
            raise ValueError("valid operation and exploration snapshots are required")
        payload = json.dumps([user_id, stamina, cost, position, status, daily, limit, expected_cooldown, str(cooldown_until), target], ensure_ascii=True, sort_keys=True)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            old = uow.query_one("SELECT payload,stamina FROM map_explore_start_operations WHERE operation_id=?", (operation_id,))
            if old:
                return {"status": "duplicate" if str(old["payload"]) == payload else "state_changed", "stamina": int(old["stamina"]) if str(old["payload"]) == payload else stamina}
            user = uow.query_one("SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None: return {"status": "user_missing", "stamina": stamina}
            current_stamina = int(user["user_stamina"] or 0)
            if current_stamina != stamina: return {"status": "state_changed", "stamina": current_stamina}
            if current_stamina < cost: return {"status": "stamina_insufficient", "stamina": current_stamina}
            pos = uow.query_one("SELECT realm,heaven,node_id FROM player_data.map_status WHERE user_id=?", (user_id,))
            if pos is None or tuple(str(pos[key]) for key in position) != tuple(position.values()): return {"status": "state_changed", "stamina": current_stamina}
            state = uow.query_one("SELECT running,node_type,node_name,start_time,duration_min,settlement,max_duration_min,interval_min FROM player_data.map_explore_status WHERE user_id=?", (user_id,))
            if state is None or any(str("" if state[key] is None else state[key]) != wanted for key, wanted in status.items()):
                return {"status": "already_running" if state is not None and int(state["running"] or 0) == 1 else "state_changed", "stamina": current_stamina}
            daily_row = uow.query_one("SELECT date,explore_count,resource_total_count FROM player_data.map_daily_limit WHERE user_id=?", (user_id,))
            if daily_row is None or (str(daily_row["date"]), str(daily_row["explore_count"]), str(daily_row["resource_total_count"])) != (daily["date"], daily.get("explore_count", "0"), daily.get("resource_total_count", "0")): return {"status": "state_changed", "stamina": current_stamina}
            if int(daily_row["explore_count"] or 0) >= limit: return {"status": "limit_reached", "stamina": current_stamina}
            cd = uow.query_one("SELECT explore_start_cd_until FROM player_data.map_cooldown WHERE user_id=?", (user_id,))
            current_cd = "" if cd is None or cd["explore_start_cd_until"] is None else str(cd["explore_start_cd_until"])
            if current_cd != expected_cooldown: return {"status": "state_changed", "stamina": current_stamina}
            remaining = current_stamina - cost
            uow.execute("UPDATE user_xiuxian SET user_stamina=? WHERE user_id=?", (remaining, user_id))
            fields = ("running", "node_type", "node_name", "start_time", "duration_min", "settlement", "max_duration_min", "interval_min")
            uow.execute("UPDATE player_data.map_explore_status SET " + ",".join(f"{key}=?" for key in fields) + " WHERE user_id=?", tuple(target.get(key, "") for key in fields) + (user_id,))
            uow.execute("INSERT INTO player_data.map_cooldown(user_id,explore_start_cd_until) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET explore_start_cd_until=excluded.explore_start_cd_until", (user_id, str(cooldown_until)))
            uow.execute("INSERT INTO map_explore_start_operations(operation_id,payload,stamina) VALUES(?,?,?)", (operation_id, payload, remaining))
            return {"status": "applied", "stamina": remaining}


class MapExploreSettlementSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock: Any) -> None:
        self.game_database, self.player_database, self.clock = str(game_database), str(player_database), clock

    def settle(self, operation_id: str, user_id: str, expected_state: dict[str, Any], expected_daily: dict[str, Any], daily_limit: int, stone: int, items: list[dict[str, Any]], max_goods_num: int) -> dict[str, Any]:
        state = {key: str(value) for key, value in dict(expected_state).items()}
        daily = {key: str(value) for key, value in dict(expected_daily).items()}
        rewards = tuple((int(x["id"]), str(x["name"]), str(x["type"]), int(x["amount"])) for x in items if int(x["amount"]) > 0)
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        daily_limit, stone, max_goods_num = int(daily_limit), int(stone), int(max_goods_num)
        if not operation_id or state.get("running") != "1" or not daily.get("date") or min(daily_limit, stone, max_goods_num) < 0:
            raise ValueError("valid explore settlement is required")
        payload = json.dumps([user_id, state, daily, daily_limit, stone, rewards, max_goods_num], ensure_ascii=True, sort_keys=True)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            old = uow.query_one("SELECT payload,stone,rewards FROM map_explore_settlement_operations WHERE operation_id=?", (operation_id,))
            if old:
                if str(old["payload"]) != payload:
                    return {"status": "state_changed", "stone": 0, "rewards": ()}
                return {"status": "duplicate", "stone": int(old["stone"]), "rewards": tuple(tuple(map(int, x)) for x in json.loads(str(old["rewards"])))}
            if uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_id=?", (user_id,)) is None:
                return {"status": "user_missing", "stone": 0, "rewards": ()}
            current = uow.query_one("SELECT running,node_type,node_name,start_time,duration_min,settlement,max_duration_min,interval_min FROM player_data.map_explore_status WHERE user_id=?", (user_id,))
            if current is None or any(str("" if current[k] is None else current[k]) != wanted for k, wanted in state.items()):
                return {"status": "state_changed", "stone": 0, "rewards": ()}
            daily_row = uow.query_one("SELECT date,explore_count,resource_total_count FROM player_data.map_daily_limit WHERE user_id=?", (user_id,))
            if daily_row is None or (str(daily_row["date"]), str(daily_row["explore_count"]), str(daily_row["resource_total_count"])) != (daily["date"], daily.get("explore_count", "0"), daily.get("resource_total_count", "0")):
                return {"status": "state_changed", "stone": 0, "rewards": ()}
            if int(daily_row["explore_count"] or 0) >= daily_limit:
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
            uow.execute("UPDATE player_data.map_daily_limit SET explore_count=?,resource_total_count=? WHERE user_id=?", (int(daily_row["explore_count"] or 0) + 1, int(daily_row["resource_total_count"] or 0) + 1, user_id))
            uow.execute("UPDATE player_data.map_explore_status SET running=0,node_type='',node_name='',start_time='',duration_min=0,settlement='',max_duration_min=0,interval_min=0 WHERE user_id=?", (user_id,))
            if stone:
                uow.execute("UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? WHERE user_id=?", (stone, user_id))
            now = self.clock.now().isoformat()
            for item_id, amount in totals.items():
                name, item_type = metadata[item_id]
                uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time", (user_id, item_id, name, item_type, amount, now, now, amount))
            compact = tuple(sorted(totals.items()))
            uow.execute("INSERT INTO map_explore_settlement_operations(operation_id,payload,stone,rewards) VALUES(?,?,?,?)", (operation_id, payload, stone, json.dumps(compact)))
            return {"status": "applied", "stone": stone, "rewards": compact}


class MapMissionClaimSqlRepository:
    ALLOWED_PROGRESS = frozenset({"gather_count", "explore_count", "combat_count", "fish_count", "mine_count"})
    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock: Any) -> None:
        self.game_database, self.player_database, self.clock = str(game_database), str(player_database), clock
    def claim(self, operation_id: str, user_id: str, expected_mission: dict[str, Any], expected_daily: dict[str, Any], progress_key: str, stone: int, items: list[dict[str, Any]], max_goods_num: int) -> dict[str, Any]:
        mission={k:str(v) for k,v in dict(expected_mission).items()};daily={k:str(v) for k,v in dict(expected_daily).items()};progress_key=str(progress_key)
        rewards=tuple((int(x['id']),str(x['name']),str(x['type']),int(x['amount'])) for x in items if int(x['amount'])>0);operation_id,user_id=str(operation_id).strip(),str(user_id);stone,max_goods_num=int(stone),int(max_goods_num)
        if not operation_id or not mission.get('date') or progress_key not in self.ALLOWED_PROGRESS or min(stone,max_goods_num)<0:raise ValueError('valid mission claim required')
        payload=json.dumps([user_id,mission,daily,progress_key,stone,rewards,max_goods_num],ensure_ascii=True,sort_keys=True)
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data');old=uow.query_one('SELECT payload,stone,rewards FROM map_mission_claim_operations WHERE operation_id=?',(operation_id,))
            if old:
                return {'status':'duplicate','stone':int(old['stone']),'rewards':tuple(tuple(map(int,x)) for x in json.loads(str(old['rewards'])))} if str(old['payload'])==payload else {'status':'state_changed','stone':0,'rewards':()}
            if uow.query_one('SELECT 1 FROM user_xiuxian WHERE user_id=?',(user_id,)) is None:return {'status':'user_missing','stone':0,'rewards':()}
            row=uow.query_one('SELECT date,mission_type,target,claimed,settlement FROM player_data.map_mission WHERE user_id=?',(user_id,))
            if row is None or any(str('' if row[k] is None else row[k])!=v for k,v in mission.items()):return {'status':'state_changed','stone':0,'rewards':()}
            progress=uow.query_one(f'SELECT date,"{progress_key}" AS progress FROM player_data.map_daily_limit WHERE user_id=?',(user_id,))
            if progress is None or (str(progress['date']),str(progress['progress']))!=(daily.get('date',''),daily.get(progress_key,'0')):return {'status':'state_changed','stone':0,'rewards':()}
            if int(mission.get('claimed','0'))!=0:return {'status':'already_claimed','stone':0,'rewards':()}
            if int(progress['progress'] or 0)<int(mission.get('target','0')):return {'status':'not_completed','stone':0,'rewards':()}
            totals={};meta={}
            for item_id,name,item_type,amount in rewards:totals[item_id]=totals.get(item_id,0)+amount;meta[item_id]=(name,item_type)
            for item_id,amount in totals.items():
                item=uow.query_one('SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?',(user_id,item_id))
                if (int(item['goods_num']) if item else 0)+amount>max_goods_num:return {'status':'inventory_full','stone':0,'rewards':()}
            uow.execute('UPDATE player_data.map_mission SET claimed=1 WHERE user_id=?',(user_id,))
            if stone:uow.execute('UPDATE user_xiuxian SET stone=COALESCE(stone,0)+? WHERE user_id=?',(stone,user_id))
            now=self.clock.now().isoformat()
            for item_id,amount in totals.items():
                name,item_type=meta[item_id];uow.execute('INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time',(user_id,item_id,name,item_type,amount,now,now,amount))
            compact=tuple(sorted(totals.items()));uow.execute('INSERT INTO map_mission_claim_operations(operation_id,payload,stone,rewards) VALUES(?,?,?,?)',(operation_id,payload,stone,json.dumps(compact)))
            return {'status':'applied','stone':stone,'rewards':compact}


class MapSeedPurchaseSqlRepository:
    def __init__(self, game_database: str | Path, *, clock: Any) -> None:
        self.game_database,self.clock=str(game_database),clock
    def purchase(self,operation_id:str,user_id:str,item_id:int,item_name:str,quantity:int,unit_cost:int,expected_stone:int,max_goods_num:int)->dict[str,Any]:
        operation_id,user_id,item_name=str(operation_id).strip(),str(user_id),str(item_name);item_id,quantity,unit_cost,expected_stone,max_goods_num=map(int,(item_id,quantity,unit_cost,expected_stone,max_goods_num))
        if not operation_id or quantity<=0 or min(item_id,unit_cost,expected_stone,max_goods_num)<0:raise ValueError('valid seed purchase required')
        payload=json.dumps([user_id,item_id,item_name,quantity,unit_cost,expected_stone,max_goods_num],ensure_ascii=True,sort_keys=True)
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            old=uow.query_one('SELECT payload,quantity,cost,stone,inventory FROM map_seed_purchase_operations WHERE operation_id=?',(operation_id,))
            if old:
                return {'status':'duplicate','quantity':int(old['quantity']),'cost':int(old['cost']),'stone':int(old['stone']),'inventory':int(old['inventory'])} if str(old['payload'])==payload else {'status':'state_changed','quantity':0,'cost':0,'stone':expected_stone,'inventory':0}
            user=uow.query_one('SELECT stone FROM user_xiuxian WHERE user_id=?',(user_id,))
            if user is None:return {'status':'user_missing','quantity':0,'cost':0,'stone':expected_stone,'inventory':0}
            stone=int(user['stone'] or 0)
            if stone!=expected_stone:return {'status':'state_changed','quantity':0,'cost':0,'stone':stone,'inventory':0}
            cost=quantity*unit_cost
            if stone<cost:return {'status':'stone_insufficient','quantity':0,'cost':0,'stone':stone,'inventory':0}
            row=uow.query_one('SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?',(user_id,item_id));inventory=int(row['goods_num']) if row else 0
            if inventory+quantity>max_goods_num:return {'status':'inventory_full','quantity':0,'cost':0,'stone':stone,'inventory':inventory}
            stone-=cost;inventory+=quantity;now=self.clock.now().isoformat();uow.execute('UPDATE user_xiuxian SET stone=? WHERE user_id=?',(stone,user_id));uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",(user_id,item_id,item_name,'特殊物品',quantity,now,now,quantity));uow.execute('INSERT INTO map_seed_purchase_operations(operation_id,payload,quantity,cost,stone,inventory) VALUES(?,?,?,?,?,?)',(operation_id,payload,quantity,cost,stone,inventory));return {'status':'applied','quantity':quantity,'cost':cost,'stone':stone,'inventory':inventory}


class MapDongfuBuildSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path)->None:self.game_database,self.player_database=str(game_database),str(player_database)
    def build(self,operation_id:str,user_id:str,expected_stone:int,cost:int,expected_position:dict[str,Any],dongfu:dict[str,Any])->dict[str,Any]:
        operation_id,user_id=str(operation_id).strip(),str(user_id);expected_stone,cost=int(expected_stone),int(cost);position={k:str(v) for k,v in dict(expected_position).items()};dongfu={k:str(v) for k,v in dict(dongfu).items()}
        if not operation_id or min(expected_stone,cost)<0 or not {'realm','heaven','node_id'}.issubset(position) or dongfu.get('built')!='1':raise ValueError('valid dongfu build required')
        payload=json.dumps([user_id,expected_stone,cost,position,dongfu],ensure_ascii=True,sort_keys=True)
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data');old=uow.query_one('SELECT payload,stone FROM map_dongfu_build_operations WHERE operation_id=?',(operation_id,))
            if old:return {'status':'duplicate','stone':int(old['stone'])} if str(old['payload'])==payload else {'status':'state_changed','stone':expected_stone}
            user=uow.query_one('SELECT stone FROM user_xiuxian WHERE user_id=?',(user_id,))
            if user is None:return {'status':'user_missing','stone':expected_stone}
            stone=int(user['stone'] or 0)
            if stone!=expected_stone:return {'status':'state_changed','stone':stone}
            if stone<cost:return {'status':'stone_insufficient','stone':stone}
            pos=uow.query_one('SELECT realm,heaven,node_id FROM player_data.map_status WHERE user_id=?',(user_id,))
            if pos is None or tuple(str(pos[k]) for k in position)!=tuple(position.values()):return {'status':'state_changed','stone':stone}
            existing=uow.query_one('SELECT built FROM player_data.dongfu_status WHERE user_id=?',(user_id,))
            if existing is not None and int(existing['built'] or 0)==1:return {'status':'already_built','stone':stone}
            remaining=stone-cost;uow.execute('UPDATE user_xiuxian SET stone=? WHERE user_id=?',(remaining,user_id));fields=('built','realm','heaven','node_id','node_name','node_type');uow.execute('INSERT INTO player_data.dongfu_status(user_id,'+','.join(fields)+') VALUES('+','.join(['?']*7)+') ON CONFLICT(user_id) DO UPDATE SET '+','.join(f'{k}=excluded.{k}' for k in fields),(user_id,*(dongfu.get(k,'') for k in fields)));uow.execute('INSERT INTO map_dongfu_build_operations(operation_id,payload,stone) VALUES(?,?,?)',(operation_id,payload,remaining));return {'status':'applied','stone':remaining}


class MapCombatLifecycleQueryRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path)->None:self.game_database,self.player_database=str(game_database),str(player_database)
    @staticmethod
    def _parse(value:Any)->dict[str,Any]:
        try: result=json.loads(str(value))
        except (TypeError,ValueError): return {}
        return result if isinstance(result,dict) else {}
    def replay_start(self,operation_id:str,user_id:str)->dict[str,Any]|None:
        operation_id,user_id=str(operation_id).strip(),str(user_id).strip();payload=json.dumps([user_id],ensure_ascii=True,separators=(',',':'))
        if not operation_id or not user_id:raise ValueError('operation and user required')
        with DatabaseUnitOfWork(self.game_database) as uow:row=uow.query_one('SELECT payload,result_status,stamina,task_json FROM map_combat_start_operations WHERE operation_id=?',(operation_id,))
        if row is None:return None
        if str(row['payload'])!=payload:return {'status':'operation_conflict','stamina':0,'task':{},'snapshot':''}
        task=self._parse(row['task_json']);status='duplicate' if str(row['result_status'])=='applied' else str(row['result_status']);return {'status':status,'stamina':int(row['stamina'] or 0),'task':task,'snapshot':json.dumps(task,ensure_ascii=False,sort_keys=True) if task else ''}
    def get_pending(self,user_id:str)->dict[str,Any]|None:
        with DatabaseUnitOfWork(self.player_database) as uow:row=uow.query_one('SELECT snapshot FROM map_combat_settlement WHERE user_id=?',(str(user_id),))
        snapshot='' if row is None or row['snapshot'] is None else str(row['snapshot'])
        if not snapshot:return None
        return {'status':'pending','stamina':0,'task':self._parse(snapshot),'snapshot':snapshot}


class MapCombatLifecycleStartSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database, self.player_database = str(game_database), str(player_database)

    def start(self, operation_id: str, user_id: str, expected_stamina: int, stamina_cost: int, expected_position: dict[str, Any], expected_daily: dict[str, Any], daily_limit: int, expected_cooldown: str, task: dict[str, Any]) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id).strip()
        expected_stamina, stamina_cost, daily_limit = map(int, (expected_stamina, stamina_cost, daily_limit))
        position = {str(k): str(v) for k, v in dict(expected_position).items()}
        daily = {str(k): str(v) for k, v in dict(expected_daily).items()}
        expected_cooldown = "" if expected_cooldown is None else str(expected_cooldown)
        task = dict(task)
        required = {"task_id", "status", "started_at", "cooldown_until", "daily", "enemy", "node_name", "node_type"}
        if not operation_id or not user_id or min(expected_stamina, stamina_cost, daily_limit) < 0 or not {"realm", "heaven", "node_id"}.issubset(position) or not daily.get("date") or not required.issubset(task) or str(task["task_id"]) != operation_id or str(task["status"]) != "running":
            raise ValueError("valid combat lifecycle snapshots are required")
        payload = json.dumps([user_id], ensure_ascii=True, separators=(",", ":")); task_json = json.dumps(task, ensure_ascii=False, sort_keys=True)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            previous = uow.query_one("SELECT payload,result_status,stamina,task_json FROM map_combat_start_operations WHERE operation_id=?", (operation_id,))
            if previous:
                if str(previous["payload"]) != payload: return {"status": "operation_conflict", "stamina": 0, "task": {}, "snapshot": ""}
                old_task = self._parse(previous["task_json"]); return {"status": "duplicate" if str(previous["result_status"]) == "applied" else str(previous["result_status"]), "stamina": int(previous["stamina"] or 0), "task": old_task, "snapshot": json.dumps(old_task, ensure_ascii=False, sort_keys=True) if old_task else ""}
            user = uow.query_one("SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None: return self._record(uow, operation_id, payload, "user_missing", expected_stamina, {})
            stamina = int(user["user_stamina"] or 0)
            if stamina != expected_stamina: return self._record(uow, operation_id, payload, "state_changed", stamina, {})
            pos = uow.query_one("SELECT realm,heaven,node_id FROM player_data.map_status WHERE user_id=?", (user_id,))
            if pos is None or tuple(str(pos[k]) for k in position) != tuple(position.values()): return self._record(uow, operation_id, payload, "state_changed", stamina, {})
            dr = uow.query_one("SELECT date,combat_count,resource_total_count FROM player_data.map_daily_limit WHERE user_id=?", (user_id,))
            if dr is None or (str(dr["date"]), str(dr["combat_count"]), str(dr["resource_total_count"])) != (daily.get("date", ""), daily.get("combat_count", "0"), daily.get("resource_total_count", "0")): return self._record(uow, operation_id, payload, "state_changed", stamina, {})
            if int(dr["combat_count"] or 0) >= daily_limit: return self._record(uow, operation_id, payload, "limit_reached", stamina, {})
            cd = uow.query_one("SELECT combat_cd_until FROM player_data.map_cooldown WHERE user_id=?", (user_id,)); current_cd = "" if cd is None or cd["combat_cd_until"] is None else str(cd["combat_cd_until"])
            if current_cd != expected_cooldown: return self._record(uow, operation_id, payload, "state_changed", stamina, {})
            if current_cd and current_cd > str(task["started_at"]): return self._record(uow, operation_id, payload, "cooldown", stamina, {"cooldown_until": current_cd})
            pending = uow.query_one("SELECT snapshot FROM player_data.map_combat_settlement WHERE user_id=?", (user_id,))
            if pending is not None and str(pending["snapshot"] or ""): return self._record(uow, operation_id, payload, "already_running", stamina, self._parse(pending["snapshot"]))
            if stamina < stamina_cost: return self._record(uow, operation_id, payload, "stamina_insufficient", stamina, {})
            remaining = stamina - stamina_cost
            uow.execute("UPDATE user_xiuxian SET user_stamina=? WHERE user_id=?", (remaining, user_id))
            uow.execute("INSERT INTO player_data.map_cooldown(user_id,combat_cd_until) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET combat_cd_until=excluded.combat_cd_until", (user_id, str(task["cooldown_until"])))
            uow.execute("INSERT INTO player_data.map_combat_settlement(user_id,snapshot) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET snapshot=excluded.snapshot", (user_id, task_json))
            return self._record(uow, operation_id, payload, "applied", remaining, task)

    @staticmethod
    def _parse(value: Any) -> dict[str, Any]:
        try: parsed = json.loads(str(value))
        except (TypeError, ValueError): return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _record(uow: DatabaseUnitOfWork, operation_id: str, payload: str, status: str, stamina: int, task: dict[str, Any]) -> dict[str, Any]:
        task_json = json.dumps(task, ensure_ascii=False, sort_keys=True)
        uow.execute("INSERT INTO map_combat_start_operations(operation_id,payload,result_status,stamina,task_json) VALUES(?,?,?,?,?)", (operation_id, payload, status, stamina, task_json))
        return {"status": status, "stamina": stamina, "task": task, "snapshot": task_json if task else ""}


class MapCombatLifecyclePlanSqlRepository:
    def __init__(self, player_database: str | Path) -> None:
        self.player_database = str(player_database)

    def save_plan(self, operation_id: str, user_id: str, task_id: str, plan: dict[str, Any]) -> dict[str, Any]:
        operation_id, user_id, task_id = str(operation_id).strip(), str(user_id).strip(), str(task_id).strip()
        plan = dict(plan)
        if not operation_id or not user_id or not task_id or str(plan.get("task_id", "")) != task_id or str(plan.get("status", "")) != "planned":
            raise ValueError("valid combat plan is required")
        snapshot = json.dumps(plan, ensure_ascii=False, sort_keys=True)
        with DatabaseUnitOfWork(self.player_database, immediate=True) as uow:
            old = uow.query_one("SELECT payload,snapshot FROM map_combat_plan_operations WHERE operation_id=?", (operation_id,))
            if old:
                return {"status": "duplicate", "task": self._parse(old["snapshot"]), "snapshot": str(old["snapshot"])} if str(old["payload"]) == snapshot else {"status": "operation_conflict", "task": {}, "snapshot": ""}
            current = uow.query_one("SELECT snapshot FROM map_combat_settlement WHERE user_id=?", (user_id,))
            current_snapshot = "" if current is None or current["snapshot"] is None else str(current["snapshot"])
            current_task = self._parse(current_snapshot)
            if str(current_task.get("task_id", "")) != task_id or str(current_task.get("status", "")) != "running":
                return {"status": "state_changed", "task": {}, "snapshot": ""}
            uow.execute("UPDATE map_combat_settlement SET snapshot=? WHERE user_id=?", (snapshot, user_id))
            uow.execute("INSERT INTO map_combat_plan_operations(operation_id,user_id,task_id,payload,snapshot) VALUES(?,?,?,?,?)", (operation_id, user_id, task_id, snapshot, snapshot))
            return {"status": "applied", "task": plan, "snapshot": snapshot}

    @staticmethod
    def _parse(value: Any) -> dict[str, Any]:
        try: parsed = json.loads(str(value))
        except (TypeError, ValueError): return {}
        return parsed if isinstance(parsed, dict) else {}


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


__all__ = ["LegacyMapRepository", "MapCombatLifecyclePlanSqlRepository", "MapCombatLifecycleQueryRepository", "MapCombatLifecycleStartSqlRepository", "MapDongfuBuildSqlRepository", "MapDongfuSqlQueryRepository", "MapExploreSettlementSqlRepository", "MapExploreStartSqlRepository", "MapExploreStatusSqlQueryRepository", "MapMissionClaimSqlRepository", "MapMissionSqlQueryRepository", "MapNearbyPlayersSqlQueryRepository", "MapProjectionSqlRepository", "MapStatusSqlQueryRepository", "MapStatusSqlWriteRepository", "MapSeedPurchaseSqlRepository", "MapHomeReturnSqlRepository", "MapInteractiveFailureSqlRepository", "MapInteractiveSettlementSqlRepository", "MapInteractiveSqlQueryRepository", "MapInteractiveStartSqlRepository", "MapMovementSqlRepository", "MapResourceRewardSqlRepository", "MapRepository"]
