from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import json

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock


class DungeonRepository(Protocol):
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def operation_result(self, *args: Any, **kwargs: Any) -> Any: ...
    def replay(self, *args: Any, **kwargs: Any) -> Any: ...
    def prepare(self, *args: Any, **kwargs: Any) -> Any: ...
    def settle(self, *args: Any, **kwargs: Any) -> Any: ...
    def resolve_rejection(self, *args: Any, **kwargs: Any) -> Any: ...
    def operation_session_result(self, *args: Any, **kwargs: Any) -> Any: ...
    def session_transition(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyDungeonRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _purchase_service(self):
        from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonPurchaseService

        return DungeonPurchaseService(self.game_database)

    def _explore_service(self):
        from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonExploreOperationService

        return DungeonExploreOperationService(self.game_database, self.player_database)

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._purchase_service().purchase(*args, **kwargs)

    def operation_result(self, *args: Any, **kwargs: Any) -> Any:
        return self._purchase_service().operation_result(*args, **kwargs)

    def replay(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().replay(*args, **kwargs)

    def prepare(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().prepare(*args, **kwargs)

    def settle(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().settle(*args, **kwargs)

    def resolve_rejection(self, *args: Any, **kwargs: Any) -> Any:
        return self._explore_service().resolve_rejection(*args, **kwargs)

    def operation_session_result(self, operation_id: str, user_id: str, action: str) -> Any:
        from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonSessionService
        return DungeonSessionService(self.player_database).operation_result(operation_id, user_id, action)

    def session_transition(self, operation_id: str, user_id: str, expected: dict[str, Any], dungeon: dict[str, Any], action: str) -> Any:
        from ...xiuxian.xiuxian_dungeon.transaction_service import DungeonSessionService
        service = DungeonSessionService(self.player_database)
        return getattr(service, action)(operation_id, user_id, expected, dungeon)


class DungeonPurchaseSqlRepository(LegacyDungeonRepository):
    REJECTIONS = {"stone_insufficient": "灵石不足，无法兑换。", "inventory_full": "背包中该物品数量已达上限。", "state_changed": "兑换未结算：数据刚被其他操作改动，请重试。", "user_missing": "未找到道友数据，兑换失败。"}

    def __init__(self, game_database: str | Path, player_database: str | Path, *, clock: Any = None) -> None:
        super().__init__(game_database, player_database)
        self.clock = clock or SystemClock()

    @staticmethod
    def _payload(user_id: str, item_id: int, quantity: int, bind_flag: int) -> str:
        return json.dumps([user_id, item_id, quantity, bind_flag], ensure_ascii=True, separators=(",", ":"))

    def operation_result(self, operation_id: str, user_id: str, item_id: int, quantity: int, bind_flag: int = 1) -> dict[str, Any] | None:
        payload = self._payload(str(user_id), int(item_id), int(quantity), int(bind_flag))
        with DatabaseUnitOfWork(self.game_database) as uow:
            row = uow.query_one("SELECT payload,result_status,quantity,cost,stone,inventory,response FROM dungeon_purchase_operations WHERE operation_id=?", (str(operation_id),))
        if row is None:
            return None
        if str(row["payload"]) != payload:
            return self._result("state_changed", int(quantity), 0, 0, 0, "")
        status = "duplicate" if str(row["result_status"]) == "applied" else str(row["result_status"])
        return {"status": status, "quantity": int(row["quantity"]), "cost": int(row["cost"]), "stone": int(row["stone"]), "inventory": int(row["inventory"]), "response": str(row["response"])}

    def purchase(self, operation_id: str, user_id: str, item_id: int, item_name: str, item_type: str, quantity: int, unit_cost: int, expected_stone: int, max_goods: int, bind_flag: int = 1) -> dict[str, Any]:
        operation_id,user_id,item_name,item_type=str(operation_id).strip(),str(user_id),str(item_name),str(item_type);item_id,quantity,unit_cost,expected_stone,max_goods,bind_flag=map(int,(item_id,quantity,unit_cost,expected_stone,max_goods,bind_flag));payload=self._payload(user_id,item_id,quantity,bind_flag);cost=quantity*unit_cost
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            old=uow.query_one("SELECT payload,result_status,quantity,cost,stone,inventory,response FROM dungeon_purchase_operations WHERE operation_id=?",(operation_id,))
            if old:
                if str(old["payload"])!=payload:return self._result("state_changed",quantity,cost,0,0,item_name)
                status="duplicate" if str(old["result_status"])=="applied" else str(old["result_status"]);return {"status":status,"quantity":int(old["quantity"]),"cost":int(old["cost"]),"stone":int(old["stone"]),"inventory":int(old["inventory"]),"response":str(old["response"])}
            user=uow.query_one("SELECT COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?",(user_id,))
            if user is None:return self._record(uow,operation_id,payload,self._result("user_missing",quantity,cost,0,0,item_name))
            stone=int(user["stone"])
            if stone!=expected_stone:return self._record(uow,operation_id,payload,self._result("state_changed",quantity,cost,stone,0,item_name))
            if stone<cost:return self._record(uow,operation_id,payload,self._result("stone_insufficient",quantity,cost,stone,0,item_name))
            item=uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?",(user_id,item_id));inventory,bound=(int(item["goods_num"]),int(item["bind_num"])) if item else (0,0)
            if inventory<0 or bound<0 or bound>inventory:return self._record(uow,operation_id,payload,self._result("state_changed",quantity,cost,stone,inventory,item_name))
            if inventory+quantity>max_goods:return self._record(uow,operation_id,payload,self._result("inventory_full",quantity,cost,stone,inventory,item_name))
            new_stone,new_inventory=stone-cost,inventory+quantity;now=self.clock.now().isoformat();uow.execute("UPDATE user_xiuxian SET stone=? WHERE user_id=? AND COALESCE(stone,0)=?",(new_stone,user_id,expected_stone));uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",(user_id,item_id,item_name,item_type,quantity,now,now,quantity if bind_flag else 0));return self._record(uow,operation_id,payload,self._result("applied",quantity,cost,new_stone,new_inventory,item_name))

    def _result(self,status:str,quantity:int,cost:int,stone:int,inventory:int,item_name:str)->dict[str,Any]:
        response=f"成功兑换{item_name}×{quantity}，消耗{cost}灵石。" if status in {"applied","duplicate"} else self.REJECTIONS.get(status,"兑换失败。")
        return {"status":status,"quantity":quantity,"cost":cost,"stone":stone,"inventory":inventory,"response":response}

    @staticmethod
    def _record(uow:DatabaseUnitOfWork,operation_id:str,payload:str,result:dict[str,Any])->dict[str,Any]:
        uow.execute("INSERT INTO dungeon_purchase_operations(operation_id,payload,result_status,quantity,cost,stone,inventory,response) VALUES(?,?,?,?,?,?,?,?)",(operation_id,payload,result["status"],result["quantity"],result["cost"],result["stone"],result["inventory"],result["response"]));return result


class DungeonSessionSqlRepository(DungeonPurchaseSqlRepository):
    _STATUS_INTEGER_FIELDS = {"current_layer", "total_layers", "reset_generation"}
    _STATUS_FIELDS = (
        "dungeon_id", "dungeon_name", "dungeon_status", "current_layer",
        "total_layers", "last_reset_date", "reset_generation", "reset_operation_id",
    )

    def prepare(self, operation_id: str, user_id: str, plan: dict[str, Any]) -> dict[str, Any]:
        operation_id,user_id=str(operation_id).strip(),str(user_id);plan=dict(plan)
        if not operation_id or not plan: raise ValueError("operation and plan required")
        identity=json.dumps({"action":"explore","user_id":user_id},ensure_ascii=True,sort_keys=True);prepared=json.dumps(plan,ensure_ascii=True,sort_keys=True,separators=(",",":"))
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            existing=self._explore_row(uow,operation_id)
            if existing:return self._explore_result(existing,identity)
            uow.execute("INSERT INTO dungeon_explore_operations(operation_id,request_identity,phase,prepared_json,result_status,result_json,current_layer,dungeon_status) VALUES(?,?,'prepared',?,'','{}',0,'')",(operation_id,identity,prepared))
        return {"status":"prepared","phase":"prepared","result_status":"","response":{},"plan":plan,"current_layer":0,"dungeon_status":""}

    def resolve_rejection(self, operation_id: str, user_id: str, result_status: str, response: dict[str, Any], max_goods_num: int, current_layer: int = 0, dungeon_status: str = "") -> dict[str, Any]:
        identity=json.dumps({"action":"explore","user_id":str(user_id)},ensure_ascii=True,sort_keys=True);response_json=json.dumps(dict(response),ensure_ascii=True,sort_keys=True,separators=(",",":"))
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            existing=self._explore_row(uow,str(operation_id))
            if existing:return self._explore_result(existing,identity)
            uow.execute("INSERT INTO dungeon_explore_operations(operation_id,request_identity,phase,prepared_json,result_status,result_json,current_layer,dungeon_status) VALUES(?,?,'completed','{}',?,?,?,?)",(str(operation_id),identity,str(result_status),response_json,int(current_layer),str(dungeon_status)))
        return {"status":"applied","phase":"completed","result_status":str(result_status),"response":dict(response),"plan":{},"current_layer":int(current_layer),"dungeon_status":str(dungeon_status)}

    @staticmethod
    def _explore_row(uow: DatabaseUnitOfWork, operation_id: str):
        return uow.query_one("SELECT request_identity,phase,prepared_json,result_status,result_json,current_layer,dungeon_status FROM dungeon_explore_operations WHERE operation_id=?",(operation_id,))

    @staticmethod
    def _explore_result(row: Any, identity: str) -> dict[str, Any]:
        if str(row["request_identity"])!=identity:return {"status":"operation_conflict","phase":"","result_status":"","response":{},"plan":{},"current_layer":0,"dungeon_status":""}
        def load(value):
            try:return json.loads(str(value or "{}"))
            except (TypeError,ValueError):return {}
        phase=str(row["phase"] or "");return {"status":"duplicate" if phase=="completed" else phase,"phase":phase,"result_status":str(row["result_status"] or ""),"response":load(row["result_json"]),"plan":load(row["prepared_json"]),"current_layer":int(row["current_layer"] or 0),"dungeon_status":str(row["dungeon_status"] or "")}

    @classmethod
    def _normalize_status(cls, value: dict[str, Any]) -> dict[str, Any]:
        return {
            key: int(raw or 0) if key in cls._STATUS_INTEGER_FIELDS else str(raw or "")
            for key, raw in value.items()
            if key in cls._STATUS_FIELDS
        }

    @staticmethod
    def _members(value: Any) -> list[str]:
        try:
            value = json.loads(value or "[]") if isinstance(value, str) else value
        except (TypeError, ValueError, json.JSONDecodeError):
            value = []
        return [str(item) for item in value] if isinstance(value, list) else []

    @classmethod
    def _current_team(cls, uow: DatabaseUnitOfWork, user_id: str) -> dict[str, Any] | None:
        table = uow.query_one("SELECT 1 AS present FROM player_data.sqlite_master WHERE type='table' AND name='teams'")
        if table is None:
            return None
        columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(teams)")}
        selected = ["user_id", "leader", "members"]
        if "version" in columns:
            selected.append("version")
        for row in uow.query_all("SELECT " + ",".join(selected) + " FROM player_data.teams"):
            members = cls._members(row["members"])
            if str(row["leader"]) != user_id and user_id not in members:
                continue
            result = {"team_id": str(row["user_id"]), "leader": str(row["leader"]), "members": members}
            if "version" in columns:
                result["version"] = int(row["version"] or 0)
            return result
        return None

    @staticmethod
    def _explore_conflict(uow: DatabaseUnitOfWork, operation_id: str, status: str, plan: dict[str, Any]) -> dict[str, Any]:
        expected = plan.get("expected_status") if isinstance(plan.get("expected_status"), dict) else {}
        response = {"battle_messages": [], "message": "探索未结算：状态已被其他操作改动，请重新发起。"}
        if status == "team_changed":
            response["message"] = "探索未结算：队伍已解散或成员变动，请重新组队发起。"
        elif status == "user_missing":
            response["message"] = "探索未结算：队伍成员数据已不存在，请重新发起探索。"
        elif status == "inventory_full":
            response["message"] = "背包中该物品数量已达上限，本次探索未结算。"
        uow.execute(
            "UPDATE dungeon_explore_operations SET phase='completed',result_status=?,result_json=?,current_layer=?,dungeon_status=?,updated_at=CURRENT_TIMESTAMP WHERE operation_id=? AND phase='prepared'",
            (status, json.dumps(response, ensure_ascii=True, sort_keys=True), int(expected.get("current_layer", 0) or 0), str(expected.get("dungeon_status", "") or ""), operation_id),
        )
        return {"status": "applied", "phase": "completed", "result_status": status, "response": response, "plan": None, "current_layer": int(expected.get("current_layer", 0) or 0), "dungeon_status": str(expected.get("dungeon_status", "") or "")}

    def settle(self, operation_id: str, user_id: str, max_goods_num: int) -> dict[str, Any]:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        max_goods_num = int(max_goods_num)
        if not operation_id or max_goods_num < 0:
            raise ValueError("valid operation and inventory limit are required")
        identity = json.dumps({"action": "explore", "user_id": user_id}, ensure_ascii=True, sort_keys=True)
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, "player_data")
            row = self._explore_row(uow, operation_id)
            if row is None:
                return {"status": "missing", "phase": "", "result_status": "", "response": {}, "plan": {}, "current_layer": 0, "dungeon_status": ""}
            if str(row["request_identity"]) != identity:
                return {"status": "operation_conflict", "phase": "", "result_status": "", "response": {}, "plan": {}, "current_layer": 0, "dungeon_status": ""}
            if str(row["phase"]) == "completed":
                return self._explore_result(row, identity)
            if str(row["phase"]) != "prepared":
                return {"status": "invalid_phase", "phase": str(row["phase"]), "result_status": "", "response": {}, "plan": {}, "current_layer": 0, "dungeon_status": ""}
            plan = json.loads(str(row["prepared_json"] or "{}"))
            if not isinstance(plan, dict) or not isinstance(plan.get("members"), list) or not plan["members"]:
                return {"status": "invalid_plan", "phase": "prepared", "result_status": "", "response": {}, "plan": plan if isinstance(plan, dict) else {}, "current_layer": 0, "dungeon_status": ""}
            expected_status = self._normalize_status(plan.get("expected_status", {}))
            status_columns = {str(item["name"]) for item in uow.query_all("PRAGMA player_data.table_info(player_dungeon_status)")}
            required = set(self._STATUS_FIELDS) & status_columns
            if not expected_status or not required.issubset(expected_status) or any(key not in status_columns for key in expected_status):
                return self._explore_conflict(uow, operation_id, "state_changed", plan)
            selected = list(expected_status)
            status_row = uow.query_one("SELECT " + ",".join(selected) + " FROM player_data.player_dungeon_status WHERE user_id=?", (user_id,))
            current_status = self._normalize_status(status_row or {})
            if current_status != expected_status:
                return self._explore_conflict(uow, operation_id, "state_changed", plan)
            expected_team = plan.get("team")
            current_team = self._current_team(uow, user_id)
            if expected_team is None:
                team_matches = current_team is None
            else:
                normalized_team = {"team_id": str(expected_team.get("team_id", "")), "leader": str(expected_team.get("leader", "")), "members": self._members(expected_team.get("members", []))}
                if "version" in expected_team:
                    normalized_team["version"] = int(expected_team.get("version", 0) or 0)
                team_matches = current_team == normalized_team
            if not team_matches:
                return self._explore_conflict(uow, operation_id, "team_changed", plan)
            inventory_rows: list[tuple[str, dict[str, Any], int, int]] = []
            seen: set[str] = set()
            for member in plan["members"]:
                member_id = str(member.get("user_id", ""))
                if not member_id or member_id in seen:
                    return {"status": "invalid_plan", "phase": "prepared", "result_status": "", "response": {}, "plan": plan, "current_layer": 0, "dungeon_status": ""}
                seen.add(member_id)
                expected = member.get("expected", {})
                user = uow.query_one("SELECT hp,mp,stone,exp FROM user_xiuxian WHERE user_id=?", (member_id,))
                if user is None:
                    return self._explore_conflict(uow, operation_id, "user_missing", plan)
                current_resources = {key: int(user[key] or 0) for key in ("hp", "mp", "stone", "exp")}
                expected_resources = {key: int(expected.get(key, 0) or 0) for key in current_resources}
                final_hp, final_mp = int(member.get("final_hp", expected_resources["hp"])), int(member.get("final_mp", expected_resources["mp"]))
                if final_hp < 1 or final_mp < 0:
                    return {"status": "invalid_plan", "phase": "prepared", "result_status": "", "response": {}, "plan": plan, "current_layer": 0, "dungeon_status": ""}
                cd = uow.query_one("SELECT COALESCE(type,0) AS type FROM user_cd WHERE user_id=? ORDER BY rowid DESC LIMIT 1", (member_id,))
                if current_resources != expected_resources or int(cd["type"] if cd else 0) != int(expected.get("cd_type", 0) or 0):
                    return self._explore_conflict(uow, operation_id, "state_changed", plan)
                for item in member.get("items", []):
                    item_id, amount = int(item.get("id", 0)), int(item.get("amount", 0))
                    if item_id <= 0 or amount <= 0:
                        return {"status": "invalid_plan", "phase": "prepared", "result_status": "", "response": {}, "plan": plan, "current_layer": 0, "dungeon_status": ""}
                    inventory = uow.query_one("SELECT COALESCE(goods_num,0) AS goods_num,COALESCE(bind_num,0) AS bind_num FROM back WHERE user_id=? AND goods_id=?", (member_id, item_id))
                    goods_num, bind_num = (int(inventory["goods_num"]), int(inventory["bind_num"])) if inventory else (0, 0)
                    if goods_num < 0 or bind_num < 0 or bind_num > goods_num or goods_num != int(item.get("expected_num", 0) or 0) or bind_num != int(item.get("expected_bind_num", 0) or 0):
                        return self._explore_conflict(uow, operation_id, "state_changed", plan)
                    if goods_num + amount > max_goods_num:
                        return self._explore_conflict(uow, operation_id, "inventory_full", plan)
                    inventory_rows.append((member_id, item, goods_num, bind_num))
            now = SystemClock().now().isoformat()
            for member in plan["members"]:
                member_id = str(member["user_id"])
                uow.execute("UPDATE user_xiuxian SET hp=?,mp=?,stone=COALESCE(stone,0)+?,exp=COALESCE(exp,0)+? WHERE user_id=?", (int(member.get("final_hp", member["expected"]["hp"])), int(member.get("final_mp", member["expected"]["mp"])), int(member.get("stone_delta", 0)), int(member.get("exp_delta", 0)), member_id))
                for item in member.get("items", []):
                    amount = int(item["amount"])
                    uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time", (member_id, int(item["id"]), str(item["name"]), str(item["type"]), amount, now, now, amount))
            current_layer = int(expected_status.get("current_layer", 0))
            total_layers = int(expected_status.get("total_layers", current_layer))
            final_layer = total_layers if bool(plan.get("complete")) else min(current_layer + 1, total_layers) if bool(plan.get("advance")) else current_layer
            final_status = "completed" if final_layer >= total_layers else "exploring"
            where = " AND ".join(f"{column}=?" for column in selected)
            updated = uow.execute("UPDATE player_data.player_dungeon_status SET current_layer=?,dungeon_status=? WHERE user_id=? AND " + where, (final_layer, final_status, user_id, *(expected_status[column] for column in selected)))
            if updated.rowcount != 1:
                return self._explore_conflict(uow, operation_id, "state_changed", plan)
            response = plan.get("response", {}) if isinstance(plan.get("response"), dict) else {}
            uow.execute("UPDATE dungeon_explore_operations SET phase='completed',result_status='applied',result_json=?,current_layer=?,dungeon_status=?,updated_at=CURRENT_TIMESTAMP WHERE operation_id=? AND phase='prepared'", (json.dumps(response, ensure_ascii=True, sort_keys=True), final_layer, final_status, operation_id))
            return {"status": "applied", "phase": "completed", "result_status": "applied", "response": response, "plan": None, "current_layer": final_layer, "dungeon_status": final_status}

    def replay(self, operation_id: str, user_id: str) -> dict[str, Any]:
        identity = json.dumps({"action":"explore","user_id":str(user_id)}, ensure_ascii=True, sort_keys=True)
        with DatabaseUnitOfWork(self.game_database) as uow:
            row = self._explore_row(uow, str(operation_id))
        if row is None: return {"status":"missing","phase":"","result_status":"","response":{},"plan":{},"current_layer":0,"dungeon_status":""}
        return self._explore_result(row, identity)
    def operation_session_result(self, operation_id: str, user_id: str, action: str) -> dict[str, Any] | None:
        with DatabaseUnitOfWork(self.player_database) as uow:
            row = uow.query_one("SELECT payload,result_status,dungeon_status FROM dungeon_session_operations WHERE operation_id=?", (str(operation_id),))
        if row is None: return None
        try: payload=json.loads(str(row["payload"]))
        except (TypeError,ValueError): return {"status":"state_changed","dungeon_status":str(row["dungeon_status"])}
        if str(payload.get("user_id",""))!=str(user_id) or str(payload.get("action",""))!=str(action): return {"status":"state_changed","dungeon_status":str(row["dungeon_status"])}
        status=str(row["result_status"]);return {"status":"duplicate" if status=="applied" else status,"dungeon_status":str(row["dungeon_status"])}

    def session_transition(self, operation_id: str, user_id: str, expected: dict[str, Any], dungeon: dict[str, Any], action: str) -> dict[str, Any]:
        operation_id,user_id,action=str(operation_id).strip(),str(user_id),str(action);expected=dict(expected);dungeon=dict(dungeon)
        if not operation_id or action not in {"enter","exit"}: raise ValueError("valid operation required")
        payload=json.dumps({"user_id":user_id,"dungeon":dungeon,"action":action},ensure_ascii=True,sort_keys=True)
        with DatabaseUnitOfWork(self.player_database,immediate=True) as uow:
            old=uow.query_one("SELECT payload,result_status,dungeon_status FROM dungeon_session_operations WHERE operation_id=?",(operation_id,))
            if old:
                status=str(old["result_status"]);return {"status":"state_changed" if str(old["payload"])!=payload else ("duplicate" if status=="applied" else status),"dungeon_status":str(old["dungeon_status"])}
            row=uow.query_one("SELECT dungeon_id,dungeon_status,current_layer,total_layers,last_reset_date,reset_generation,reset_operation_id FROM player_dungeon_status WHERE user_id=?",(user_id,))
            if row is None:return self._record_session(uow,operation_id,payload,"state_changed","")
            current={k:(int(row[k] or 0) if k in {"current_layer","total_layers","reset_generation"} else str(row[k] or "")) for k in row.keys()}
            normalized={k:(int(expected.get(k,0) or 0) if k in {"current_layer","total_layers","reset_generation"} else str(expected.get(k,"") or "")) for k in current}
            if current!=normalized or current["dungeon_id"]!=str(dungeon.get("dungeon_id","")) or current["last_reset_date"]!=str(dungeon.get("date","")):return self._record_session(uow,operation_id,payload,"state_changed",current["dungeon_status"])
            if current["dungeon_status"]=="completed":return self._record_session(uow,operation_id,payload,"completed","completed")
            if action=="exit" and current["dungeon_status"]!="exploring":return self._record_session(uow,operation_id,payload,"not_exploring",current["dungeon_status"])
            new_status="exploring" if action=="enter" else "exited";uow.execute("UPDATE player_dungeon_status SET dungeon_status=? WHERE user_id=?",(new_status,user_id));return self._record_session(uow,operation_id,payload,"applied",new_status)

    @staticmethod
    def _record_session(uow:DatabaseUnitOfWork,operation_id:str,payload:str,status:str,dungeon_status:str)->dict[str,Any]:
        uow.execute("INSERT INTO dungeon_session_operations(operation_id,payload,result_status,dungeon_status) VALUES(?,?,?,?)",(operation_id,payload,status,dungeon_status));return {"status":status,"dungeon_status":dungeon_status}


__all__ = ["DungeonPurchaseSqlRepository", "DungeonRepository", "DungeonSessionSqlRepository", "LegacyDungeonRepository"]
