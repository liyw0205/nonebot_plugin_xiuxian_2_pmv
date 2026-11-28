from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol
from ...core.numeric import as_int_like, bind_sqlite_param, require_exact_integer
from ...core.errors import DomainError

import json
from datetime import date, datetime

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock
from ..base.schema_catalog import sqlite_table_columns
from .battle_repository import (
    WorldBossBattleSettlementSqlRepository,
    WorldBossDailyLimitSnapshot,
)


class BossRepository(Protocol):
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def settle(self, *args: Any, **kwargs: Any) -> Any: ...
    def settlement_result(self, operation_id: str) -> Any: ...
    def settlement_payload(self, operation_id: str) -> dict[str, Any] | None: ...
    def weekly_purchases(self, user_id: str, today: Any = None) -> dict[str, Any] | None: ...
    def daily_limit_snapshot(self, user_id: str) -> WorldBossDailyLimitSnapshot: ...
    def battle_checked_at_snapshot(self, user_id: str) -> str: ...


class LegacyBossRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, activity_database: str | Path | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.activity_database = str(activity_database) if activity_database else None

    def _services(self):
        from ...xiuxian.xiuxian_boss.transaction_service import BossPurchaseService, WorldBossBattleSettlementService

        return (
            BossPurchaseService(self.game_database, self.player_database),
            WorldBossBattleSettlementService(self.game_database, self.player_database, self.activity_database),
        )

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        kwargs.pop("requested_quantity", None)
        return self._services()[0].purchase(*args, **kwargs)

    def settle(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[1].settle(*args, **kwargs)

    def settlement_result(self, operation_id: str) -> Any:
        return self._services()[1].get_result(operation_id)

    def settlement_payload(self, operation_id: str) -> dict[str, Any] | None:
        return WorldBossBattleSettlementSqlRepository(
            self.game_database, self.player_database, self.activity_database
        ).settlement_payload(operation_id)

    def daily_limit_snapshot(self, user_id: str) -> WorldBossDailyLimitSnapshot:
        return WorldBossBattleSettlementSqlRepository(
            self.game_database,
            self.player_database,
            self.activity_database,
        ).daily_limit_snapshot(user_id)

    def weekly_purchases(self, user_id: str, today=None) -> dict[str, Any] | None:
        return BossPurchaseSqlRepository(
            self.game_database,
            self.player_database,
            self.activity_database,
        ).weekly_purchases(user_id, today)

    def battle_checked_at_snapshot(self, user_id: str) -> str:
        return WorldBossBattleSettlementSqlRepository(self.game_database, self.player_database).battle_checked_at_snapshot(user_id)


class BossPurchaseSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path, activity_database: str | Path | None = None, *, clock=None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.activity_database = str(activity_database) if activity_database else None
        self.clock = clock or SystemClock()
        self._settlement = WorldBossBattleSettlementSqlRepository(
            self.game_database,
            self.player_database,
            self.activity_database,
            clock=self.clock,
        )

    def settle(self, *args: Any, **kwargs: Any) -> Any:
        return self._settlement.settle(*args, **kwargs)

    def settlement_result(self, operation_id: str) -> Any:
        return self._settlement.settlement_result(operation_id)

    def settlement_payload(self, operation_id: str) -> dict[str, Any] | None:
        return self._settlement.settlement_payload(operation_id)

    def daily_limit_snapshot(self, user_id: str) -> WorldBossDailyLimitSnapshot:
        return self._settlement.daily_limit_snapshot(user_id)

    def battle_checked_at_snapshot(self, user_id: str) -> str:
        return self._settlement.battle_checked_at_snapshot(user_id)

    def weekly_purchases(self, user_id: str, today=None) -> dict[str, Any] | None:
        user_id = str(user_id).strip()
        if not user_id or not Path(self.player_database).is_file():
            return None
        today = today or self.clock.now().date()
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            boss_columns = sqlite_table_columns(uow, self.player_database, "boss")
            if "weekly_purchases" in boss_columns:
                row = uow.query_one(
                    "SELECT weekly_purchases AS weekly FROM boss WHERE user_id=?",
                    (user_id,),
                )
            else:
                weekly_columns = sqlite_table_columns(uow, self.player_database, "boss_weekly_purchases")
                if not {"user_id", "weekly_purchases"} <= weekly_columns:
                    return None
                row = uow.query_one(
                    "SELECT weekly_purchases AS weekly FROM boss_weekly_purchases WHERE user_id=?",
                    (user_id,),
                )
            value = row["weekly"] if row is not None else {}
        return self._weekly(value, today)

    @staticmethod
    def _write_rejected(target: str, user_id: str) -> DomainError:
        code = f"boss_{target}_write_rejected"
        return DomainError(code, f"{target} write did not persist exactly",
                           {"reason": code, "subject": str(user_id)})

    @staticmethod
    def _weekly(value, today):
        if isinstance(today, datetime): today=today.date()
        if not isinstance(today, date): today=date.fromisoformat(str(today))
        try:data=json.loads(value) if isinstance(value,str) else dict(value or {})
        except (TypeError,ValueError):data={}
        try:reset=date.fromisoformat(str(data.get("_last_reset","")))
        except (TypeError,ValueError):reset=None
        if reset is None or reset.isocalendar()[:2]!=today.isocalendar()[:2]:return {"_last_reset":today.isoformat()}
        return {str(k):v for k,v in data.items()}

    def purchase(self, operation_id, user_id, item_id, item_name, item_type, quantity, unit_cost,
                 weekly_limit, expected_integral, expected_weekly_purchases, max_goods_num,
                 today=None, requested_quantity=None):
        operation_id, user_id, item_name, item_type = str(operation_id).strip(), str(user_id), str(item_name), str(item_type)
        item_id, quantity, unit_cost, weekly_limit, expected_integral, max_goods_num = map(
            int, (item_id, quantity, unit_cost, weekly_limit, expected_integral, max_goods_num)
        )
        includes_requested_quantity = requested_quantity is not None
        requested_quantity = quantity if requested_quantity is None else int(requested_quantity)
        today = today or self.clock.now().date()
        weekly = self._weekly(expected_weekly_purchases, today)
        request_values = [user_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, max_goods_num]
        payload_value = ({"request": request_values, "requested_quantity": requested_quantity}
                         if includes_requested_quantity else request_values)
        payload = json.dumps(payload_value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        if not operation_id or quantity<=0 or min(item_id,unit_cost,weekly_limit,expected_integral,max_goods_num)<0:raise ValueError("valid boss purchase required")
        def result(status,integral=expected_integral,purchased=0,inventory=0,reason=""):
            ok=status in {"applied","duplicate"};return {"status":status,"reason":reason or ("" if ok else status),"subject":user_id,"quantity":quantity if ok else 0,"cost":quantity*unit_cost if ok else 0,"integral":int(integral),"purchased":int(purchased),"inventory":int(inventory)}
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,"player_data");old=uow.query_one("SELECT payload,quantity,cost,integral,purchased,inventory FROM boss_purchase_operations WHERE operation_id=?",(operation_id,))
            if old:
                def corrupt():
                    return DomainError("boss_receipt_corrupt", "boss purchase receipt is inconsistent", {"reason": "boss_purchase_receipt_invalid", "subject": user_id})
                try:
                    saved_payload = json.loads(str(old["payload"]))
                except (TypeError, ValueError) as exc:
                    raise corrupt() from exc
                saved_request = saved_payload.get("request") if isinstance(saved_payload, dict) else saved_payload
                if not isinstance(saved_request, list) or len(saved_request) != 8:raise corrupt()
                for index in (1, 4, 5, 6, 7):
                    saved_request[index] = require_exact_integer(saved_request[index], field="boss_receipt_request", subject=user_id)
                saved_requested_quantity = saved_payload.get("requested_quantity", saved_request[4]) if isinstance(saved_payload, dict) else saved_request[4]
                saved_requested_quantity = require_exact_integer(saved_requested_quantity, field="boss_receipt_requested_quantity", subject=user_id)
                if (saved_request != request_values or saved_requested_quantity != requested_quantity
                        or isinstance(saved_payload, dict) and set(saved_payload) - {"request", "requested_quantity", "_rejection"}):
                    return result("state_changed", reason="operation_conflict")
                values = {key: require_exact_integer(old[key], field=f"boss_receipt_{key}", subject=user_id)
                          for key in ("quantity", "cost", "integral", "purchased", "inventory")}
                if any(value < 0 for value in values.values()):raise corrupt()
                if isinstance(saved_payload, dict) and "_rejection" in saved_payload:
                    rejection = saved_payload["_rejection"]
                    if (not isinstance(rejection, dict) or rejection.get("status") not in {
                        "user_missing", "state_changed", "limit_reached", "integral_insufficient", "inventory_full"
                    } or not rejection.get("reason") or rejection.get("subject") != user_id
                        or values["quantity"] != 0 or values["cost"] != 0
                        or any(type(rejection.get(key)) is not int or rejection[key] != value for key, value in values.items())):
                        raise corrupt()
                    return dict(rejection)
                if values["quantity"] == 0:
                    if values["cost"] != 0:raise corrupt()
                    return result("state_changed", reason="legacy_rejection")
                if values["quantity"] != quantity or values["cost"] != quantity * unit_cost:raise corrupt()
                return {"status":"duplicate","reason":"","subject":user_id,**values}
            if uow.query_one("SELECT 1 AS ok FROM user_xiuxian WHERE user_id=?",(user_id,)) is None:return self._record(uow,operation_id,payload,result("user_missing"))
            integral=uow.query_one("SELECT COALESCE(integral,0) AS integral FROM player_data.boss_limit WHERE user_id=?",(user_id,));boss_columns=sqlite_table_columns(uow, self.player_database, "boss", schema="player_data")
            if "weekly_purchases" in boss_columns:
                weekly_table = "boss"
                boss = uow.query_one("SELECT COALESCE(weekly_purchases,'{}') AS weekly FROM player_data.boss WHERE user_id=?", (user_id,))
                boss = boss or {"weekly": "{}"}
            else:
                weekly_table = "boss_weekly_purchases"
                weekly_row = uow.query_one("SELECT weekly_purchases AS weekly FROM player_data.boss_weekly_purchases WHERE user_id=?", (user_id,))
                boss = weekly_row or {"weekly": "{}"}
            if integral is None:return self._record(uow,operation_id,payload,result("state_changed"))
            current=self._weekly(boss["weekly"],today)
            if require_exact_integer(integral["integral"], field="boss_integral", subject=user_id)!=expected_integral or current!=weekly:return self._record(uow,operation_id,payload,result("state_changed", reason="boss_integral_changed" if require_exact_integer(integral["integral"],field="boss_integral",subject=user_id)!=expected_integral else "weekly_purchase_changed"))
            purchased=int(weekly.get(str(item_id),0) or 0)
            if purchased+quantity>weekly_limit:return self._record(uow,operation_id,payload,result("limit_reached",purchased=purchased))
            cost=quantity*unit_cost
            if expected_integral<cost:return self._record(uow,operation_id,payload,result("integral_insufficient",purchased=purchased))
            item=uow.query_one("SELECT COALESCE(goods_num,0) AS n,bind_num FROM back WHERE user_id=? AND goods_id=?",(user_id,item_id));inventory=int(item["n"]) if item else 0
            if inventory+quantity>max_goods_num:return self._record(uow,operation_id,payload,result("inventory_full",purchased=purchased,inventory=inventory))
            new_integral,new_purchased,new_inventory=expected_integral-cost,purchased+quantity,inventory+quantity;weekly[str(item_id)]=new_purchased
            changed = uow.execute("UPDATE player_data.boss_limit SET integral=xiuxian_int_store(?,'boss_integral','actor') WHERE user_id=? AND xiuxian_int_cmp(COALESCE(integral,0),?,1,'boss_integral','actor')=0", (bind_sqlite_param(new_integral,exact=True),user_id,bind_sqlite_param(expected_integral,exact=True)))
            if changed.rowcount != 1:
                raise DomainError("boss_integral_write_rejected", "boss integral write did not affect one row",
                                  {"reason": "boss_integral_write_rejected", "subject": user_id})
            saved = uow.query_one("SELECT integral FROM player_data.boss_limit WHERE user_id=?", (user_id,))
            if saved is None or require_exact_integer(saved["integral"], field="boss_integral", subject=user_id) != new_integral:
                raise DomainError("boss_integral_write_rejected", "boss integral purchase did not persist exactly", {"reason": "boss_integral_write_rejected", "subject": user_id})
            encoded_weekly = json.dumps(weekly, ensure_ascii=True)
            if weekly_table == "boss":
                changed = uow.execute("INSERT INTO player_data.boss(user_id,weekly_purchases) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET weekly_purchases=excluded.weekly_purchases", (user_id, encoded_weekly))
                weekly_sql = "SELECT weekly_purchases AS weekly FROM player_data.boss WHERE user_id=?"
            else:
                changed = uow.execute("INSERT INTO player_data.boss_weekly_purchases(user_id,weekly_purchases) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET weekly_purchases=excluded.weekly_purchases", (user_id, encoded_weekly))
                weekly_sql = "SELECT weekly_purchases AS weekly FROM player_data.boss_weekly_purchases WHERE user_id=?"
            saved_weekly = uow.query_one(weekly_sql, (user_id,))
            if changed.rowcount != 1 or saved_weekly is None or self._weekly(saved_weekly["weekly"], today) != weekly:
                raise self._write_rejected("weekly", user_id)

            stamp = (today.date() if isinstance(today, datetime) else today).isoformat()
            changed = uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.goods_num,update_time=excluded.update_time", (user_id, item_id, item_name, item_type, quantity, stamp, stamp, quantity))
            saved_item = uow.query_one("SELECT goods_name,goods_type,goods_num,bind_num,update_time FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
            expected_bind = (require_exact_integer(item["bind_num"], field="boss_inventory_bind", subject=user_id) if item and item["bind_num"] is not None else 0) + quantity
            if (changed.rowcount != 1 or saved_item is None or saved_item["goods_name"] != item_name
                    or saved_item["goods_type"] != item_type
                    or require_exact_integer(saved_item["goods_num"], field="boss_inventory", subject=user_id) != new_inventory
                    or require_exact_integer(saved_item["bind_num"], field="boss_inventory_bind", subject=user_id) != expected_bind
                    or saved_item["update_time"] != stamp):
                raise self._write_rejected("inventory", user_id)
            outcome = self._record(uow, operation_id, payload, result("applied", new_integral, new_purchased, new_inventory))
            # Later triggers can change assets already checked by an earlier step.
            saved_integral = uow.query_one("SELECT integral FROM player_data.boss_limit WHERE user_id=?", (user_id,))
            if saved_integral is None or require_exact_integer(saved_integral["integral"], field="boss_integral", subject=user_id) != new_integral:
                raise self._write_rejected("integral", user_id)
            final_weekly = uow.query_one(weekly_sql, (user_id,))
            if final_weekly is None or self._weekly(final_weekly["weekly"], today) != weekly:
                raise self._write_rejected("weekly", user_id)
            final_item = uow.query_one("SELECT goods_name,goods_type,goods_num,bind_num,update_time FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
            if final_item != saved_item:
                raise self._write_rejected("inventory", user_id)
            return outcome

    @staticmethod
    def _record(uow,operation_id,payload,result):
        if result["status"] not in {"applied", "duplicate"}:
            payload_value = json.loads(payload)
            if isinstance(payload_value, dict) and isinstance(payload_value.get("request"), list):
                payload_value["_rejection"] = result
            else:
                payload_value = {"request": payload_value, "_rejection": result}
            payload = json.dumps(payload_value, ensure_ascii=True, separators=(",", ":"))
        changed = uow.execute("INSERT INTO boss_purchase_operations(operation_id,payload,quantity,cost,integral,purchased,inventory) VALUES(?,?,?,xiuxian_int_store(?,'stone','actor'),xiuxian_int_store(?,'boss_integral','actor'),?,?)", (operation_id, payload, result["quantity"], bind_sqlite_param(result["cost"], exact=True), bind_sqlite_param(result["integral"], exact=True), result["purchased"], result["inventory"]))
        saved = uow.query_one("SELECT payload,quantity,cost,integral,purchased,inventory FROM boss_purchase_operations WHERE operation_id=?", (operation_id,))
        expected = (payload, int(result["quantity"]), int(result["cost"]), int(result["integral"]), int(result["purchased"]), int(result["inventory"]))
        actual = None if saved is None else (
            str(saved["payload"]),
            require_exact_integer(saved["quantity"], field="boss_receipt_quantity", subject=result["subject"]),
            require_exact_integer(saved["cost"], field="boss_receipt_cost", subject=result["subject"]),
            require_exact_integer(saved["integral"], field="boss_receipt_integral", subject=result["subject"]),
            require_exact_integer(saved["purchased"], field="boss_receipt_purchased", subject=result["subject"]),
            require_exact_integer(saved["inventory"], field="boss_receipt_inventory", subject=result["subject"]),
        )
        if changed.rowcount != 1 or actual != expected:
            raise BossPurchaseSqlRepository._write_rejected("receipt", str(result["subject"]))
        return result


__all__ = [
    "BossPurchaseSqlRepository",
    "BossRepository",
    "LegacyBossRepository",
    "WorldBossBattleSettlementSqlRepository",
]
