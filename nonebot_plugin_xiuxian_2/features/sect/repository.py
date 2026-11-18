from __future__ import annotations

import json
from ...core.errors import DomainError
from ...core.numeric import as_int_like, bind_sqlite_param, require_exact_integer
from pathlib import Path
from typing import Any, Mapping, Protocol

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock
from .schema import SCHEMA_MISSING, table_ready


class _StateChanged(Exception):
    """Abort a compare-and-set mutation without turning it into a hard error."""


class _DonationRejected(Exception):
    def __init__(self, reason, subject):
        self.reason, self.subject = reason, subject


class _RenameRejected(Exception):
    def __init__(self, reason, subject):
        self.reason, self.subject = reason, subject


class _PositionRejected(Exception):
    def __init__(self, reason, subject):
        self.reason, self.subject = reason, subject


class _RemovalRejected(Exception):
    def __init__(self, reason, subject):
        self.reason, self.subject = reason, subject


class _MemberJoinRejected(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        self.reason, self.subject = reason, subject


class _ShopPurchaseRejected(Exception):
    def __init__(self, reason: str, subject: str) -> None:
        self.reason, self.subject = reason, subject


def weekly_shop_key(moment) -> str:
    """本周限购的周期键：与 `purchase` 写入 `sect_shop_weekly_purchases.week_key` 同源。"""
    return str(moment.strftime("%G-W%V"))


class SectRepository(Protocol):
    def donate(self, *args: Any, **kwargs: Any) -> Any: ...
    def change_position(self, *args: Any, **kwargs: Any) -> Any: ...
    def kick(self, *args: Any, **kwargs: Any) -> Any: ...
    def leave(self, *args: Any, **kwargs: Any) -> Any: ...
    def rename(self, *args: Any, **kwargs: Any) -> Any: ...
    def join(self, *args: Any, **kwargs: Any) -> Any: ...
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def weekly_shop_progress(self, *args: Any, **kwargs: Any) -> Any: ...
    def learn_main(self, *args: Any, **kwargs: Any) -> Any: ...
    def learn_secondary(self, *args: Any, **kwargs: Any) -> Any: ...
    def claim_elixir(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacySectRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def _service(self, name: str):
        from ...compatibility.legacy_sect_elixir_claim import SectElixirClaimService
        from ...compatibility.legacy_sect_member_join import SectMemberJoinService
        from ...compatibility.legacy_sect_shop_purchase import SectShopPurchaseService
        from ...compatibility.legacy_sect_main_buff_learn import SectMainBuffLearnService
        from ...compatibility.legacy_sect_secondary_buff_learn import SectSecBuffLearnService
        return {"join": SectMemberJoinService, "purchase": SectShopPurchaseService, "learn_main": SectMainBuffLearnService, "learn_secondary": SectSecBuffLearnService, "claim_elixir": SectElixirClaimService}[name](self.database)

    def join(self, *args: Any, **kwargs: Any) -> Any:
        return self._service("join").join(*args, **kwargs)

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._service("purchase").purchase(*args, **kwargs)

    def learn_main(self, *args: Any, **kwargs: Any) -> Any:
        return self._service("learn_main").learn(*args, **kwargs)

    def learn_secondary(self, *args: Any, **kwargs: Any) -> Any:
        return self._service("learn_secondary").learn(*args, **kwargs)

    def claim_elixir(self, *args: Any, **kwargs: Any) -> Any:
        return self._service("claim_elixir").claim(*args, **kwargs)


class SectRenameSqlRepository:
    def __init__(self, database: str | Path, *, clock=None) -> None:
        self.database = str(database)
        self.clock = clock or SystemClock()

    def claim_elixir(self, operation_id, user_id, sect_id, contribution_required, materials_required, rewards, max_goods_num, *, legacy=False):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        sect_id, contribution_required, materials_required, max_goods_num = map(
            int, (sect_id, contribution_required, materials_required, max_goods_num))
        normalized = tuple((int(r[0]), str(r[1]), str(r[2]), int(r[3])) for r in rewards)
        if not operation_id or not normalized or any(r[0] < 0 or r[3] <= 0 for r in normalized):
            raise ValueError("valid operation and rewards are required")
        payload = json.dumps([user_id, sect_id, contribution_required, materials_required, normalized], ensure_ascii=True)
        try:
            return self._claim_elixir(operation_id, user_id, sect_id, contribution_required,
                                      materials_required, normalized, max_goods_num, payload, legacy)
        except DomainError as exc:
            return {"status": "state_changed", "reason": exc.details.get("reason") or exc.code,
                    "subject": str(exc.details.get("subject") or user_id)}

    @staticmethod
    def _elixir_reject(step, subject):
        reason = f"sect_elixir_{step}_write_rejected"
        raise DomainError(reason, reason, {"reason": reason, "subject": str(subject)})

    @staticmethod
    def _elixir_rewards(raw, subject):
        try:
            rows = json.loads(raw)
            if not isinstance(rows, list) or not rows:
                raise ValueError("missing rewards")
            result = []
            for row in rows:
                if not isinstance(row, list) or len(row) != 4:
                    raise ValueError("invalid reward row")
                item, name, kind, count = row
                if type(item) is not int or item < 0 or type(count) is not int or count <= 0 or not isinstance(name, str) or not isinstance(kind, str):
                    raise ValueError("invalid reward fields")
                result.append(tuple(row))
            return tuple(result)
        except (ValueError, TypeError) as exc:
            raise DomainError("sect_elixir_receipt_invalid", "invalid elixir receipt", {"subject": subject}) from exc

    def elixir_claim_receipt(self, operation_id, user_id):
        if not Path(self.database).is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not table_ready(uow, "sect_elixir_claim_operations", frozenset({"operation_id", "payload", "rewards"})):
                return None
            old = uow.query_one("SELECT payload,rewards FROM sect_elixir_claim_operations WHERE operation_id=?", (operation_id,))
            if old is None:
                return None
            try:
                payload = json.loads(old["payload"])
                if not isinstance(payload, list) or len(payload) != 5:
                    raise ValueError("invalid request")
            except (ValueError, TypeError) as exc:
                raise DomainError("sect_elixir_receipt_invalid", "invalid elixir request", {"subject": operation_id}) from exc
            if str(payload[0]) != str(user_id):
                raise DomainError("operation_conflict", "elixir receipt belongs to another player", {"subject": user_id})
            return {"status": "duplicate", "rewards": self._elixir_rewards(old["rewards"], operation_id)}

    def _claim_elixir(self, operation_id, user_id, sect_id, contribution_required,
                      materials_required, normalized, max_goods_num, payload, legacy):
        reject = lambda reason, subject=user_id: {"status": reason, "reason": reason, "subject": subject}
        number = lambda value, field: require_exact_integer(value, field=field, subject=user_id)
        if not Path(self.database).is_file():
            return reject("schema_missing")
        with DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            if not table_ready(uow, "sect_elixir_claim_operations", frozenset({"operation_id", "payload", "rewards"})):
                return reject("schema_missing")
            old = uow.query_one("SELECT rewards FROM sect_elixir_claim_operations WHERE operation_id=?", (operation_id,))
            if old:
                return {"status": "duplicate", "rewards": self._elixir_rewards(old["rewards"], operation_id)}
            user = uow.query_one("SELECT sect_id,sect_position,COALESCE(sect_contribution,0) AS contribution,"
                                 "COALESCE(sect_elixir_get,0) AS claimed FROM user_xiuxian WHERE user_id=?", (user_id,))
            sect = uow.query_one("SELECT COALESCE(elixir_room_level,0) AS level,COALESCE(sect_materials,0) AS materials "
                                 "FROM sects WHERE sect_id=?", (sect_id,))
            if user is None or sect is None or int(user["sect_id"] or 0) != sect_id:
                return {**reject("user_missing" if user is None else "sect_missing" if sect is None else "sect_changed"),
                        "status": "membership_changed"}
            position = 15 if legacy and user["sect_position"] is None else int(user["sect_position"] or 0)
            if position == 15:
                return reject("position_ineligible")
            if number(sect["level"], "elixir_room_level") <= 0:
                return reject("room_missing")
            if number(user["contribution"], "sect_contribution") < contribution_required:
                return reject("contribution_insufficient")
            if number(sect["materials"], "sect_materials") < materials_required:
                return reject("materials_insufficient", str(sect_id))
            if number(user["claimed"], "sect_elixir_get") == 1:
                return reject("already_claimed")
            expected = {}
            for item_id, _, _, qty in normalized:
                if item_id not in expected:
                    row = uow.query_one("SELECT COALESCE(goods_num,0) AS n,COALESCE(bind_num,0) AS b "
                                        "FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                    expected[item_id] = (number(row["n"], "sect_elixir_inventory"), number(row["b"], "sect_elixir_bound")) if row else (0, 0)
                count, bound = expected[item_id]
                if count < 0 or bound < 0 or bound > count:
                    return reject("sect_elixir_inventory_invalid", str(item_id))
                expected[item_id] = (count + qty, bound + qty)
                if count + qty > max_goods_num:
                    return reject("inventory_full", str(item_id))
            stamp = self.clock.now().isoformat()
            metadata_update = "goods_name=excluded.goods_name,goods_type=excluded.goods_type," if legacy else ""
            for item_id, name, kind, qty in normalized:
                changed = uow.execute(
                    "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                    "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                    "goods_num=xiuxian_int_store(xiuxian_int_add(COALESCE(back.goods_num,0),excluded.goods_num)),"
                    "bind_num=xiuxian_int_store(xiuxian_int_add(COALESCE(back.bind_num,0),excluded.goods_num)),"
                    + metadata_update + "update_time=excluded.update_time", (user_id, item_id, name, kind, qty, stamp, stamp, qty))
                if changed.rowcount != 1:
                    self._elixir_reject("item", item_id)
            changed = uow.execute("UPDATE user_xiuxian SET sect_elixir_get=1 WHERE user_id=? AND COALESCE(sect_elixir_get,0)=0", (user_id,))
            if changed.rowcount != 1:
                self._elixir_reject("user", user_id)
            encoded = json.dumps(normalized, ensure_ascii=True)
            changed = uow.execute("INSERT INTO sect_elixir_claim_operations(operation_id,payload,rewards) VALUES(?,?,?)", (operation_id, payload, encoded))
            if changed.rowcount != 1:
                self._elixir_reject("receipt", operation_id)
            for item_id, counts in expected.items():
                row = uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                if row is None or (number(row["goods_num"], "sect_elixir_inventory"), number(row["bind_num"], "sect_elixir_bound")) != counts:
                    self._elixir_reject("item", item_id)
            saved_user = uow.query_one("SELECT sect_id,sect_position,COALESCE(sect_contribution,0) AS contribution,"
                                       "COALESCE(sect_elixir_get,0) AS claimed FROM user_xiuxian WHERE user_id=?", (user_id,))
            if saved_user != {**user, "claimed": 1}:
                self._elixir_reject("user", user_id)
            saved_sect = uow.query_one("SELECT COALESCE(elixir_room_level,0) AS level,COALESCE(sect_materials,0) AS materials "
                                       "FROM sects WHERE sect_id=?", (sect_id,))
            if saved_sect != sect:
                self._elixir_reject("eligibility", sect_id)
            saved = uow.query_one("SELECT payload,rewards FROM sect_elixir_claim_operations WHERE operation_id=?", (operation_id,))
            if saved != {"payload": payload, "rewards": encoded}:
                self._elixir_reject("receipt", operation_id)
            return {"status": "applied", "rewards": normalized}
    def learn_secondary(self, operation_id, user_id, sect_id, buff_id, materials_cost, *, expected_catalog, forbidden_positions=(12,14,15)):
        operation_id,user_id=str(operation_id).strip(),str(user_id);sect_id,buff_id,materials_cost=int(sect_id),int(buff_id),int(materials_cost)
        try:
            return self._learn_secondary(operation_id, user_id, sect_id, buff_id, materials_cost,
                                         expected_catalog=expected_catalog,
                                         forbidden_positions=forbidden_positions)
        except DomainError as exc:
            if exc.code.startswith("sect_secbuff_") and exc.code.endswith("_write_rejected"):
                return {
                    "status": "state_changed",
                    "user_id": user_id,
                    "sect_id": sect_id,
                    "buff_id": buff_id,
                    "materials_cost": materials_cost,
                    "reason": exc.details.get("reason") or exc.code,
                    "subject": str(exc.details.get("subject") or operation_id),
                }
            raise

    @staticmethod
    def _secbuff_reject(step: str, subject: str | int) -> None:
        reason = f"sect_secbuff_{step}_write_rejected"
        raise DomainError(reason, f"{reason}: {subject}", {"reason": reason, "subject": str(subject)})

    def _learn_secondary(self, operation_id, user_id, sect_id, buff_id, materials_cost, *, expected_catalog, forbidden_positions):
        base = {"user_id": user_id, "sect_id": sect_id, "buff_id": buff_id}
        if materials_cost < 0:
            return {"status": "invalid_materials", **base, "materials_cost": materials_cost}
        with DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            if not table_ready(uow, "sect_secbuff_learn_operations", frozenset(
                {"operation_id", "user_id", "sect_id", "buff_id", "materials_cost", "materials_left"}
            )):
                return {"status": "schema_missing", **base, "reason": "schema_missing", "subject": operation_id}
            old=uow.query_one("SELECT user_id,sect_id,buff_id,materials_cost,materials_left FROM sect_secbuff_learn_operations WHERE operation_id=?",(operation_id,))
            if old:
                if (str(old["user_id"]),int(old["sect_id"]),int(old["buff_id"]),int(old["materials_cost"]))!=(user_id,sect_id,buff_id,materials_cost):return {**base, "status":"state_changed", "reason": "operation_conflict", "subject": operation_id}
                return {"status":"duplicate","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"materials_cost":old["materials_cost"],"materials_left":old["materials_left"]}
            user=uow.query_one("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?",(user_id,));
            if user is None or user["sect_id"] is None or int(user["sect_id"])!=sect_id:return {"status":"membership_changed","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"reason":"user_missing" if user is None else "sect_changed"}
            if int(user["sect_position"] or 0) in {int(v) for v in forbidden_positions}:return {"status":"position_forbidden","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            sect=uow.query_one("SELECT secbuff,COALESCE(sect_materials,0) AS materials FROM sects WHERE sect_id=?",(sect_id,));
            if sect is None or str(sect["secbuff"])!=str(expected_catalog):return {"status":"catalog_changed","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"reason":"sect_missing" if sect is None else "sect_catalog_changed"}
            materials=require_exact_integer(sect["materials"], field="sect_materials", subject=str(sect_id));buff=uow.query_one("SELECT sec_buff FROM BuffInfo WHERE user_id=?",(user_id,))
            if materials<materials_cost:return {"status":"materials_insufficient","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"materials_cost":materials_cost,"materials_left":materials}
            if buff is None:return {"status":"buff_missing","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            current_buff=require_exact_integer(buff["sec_buff"] or 0, field="sec_buff", subject=user_id)
            if current_buff==buff_id:return {"status":"already_learned","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            left=materials-materials_cost
            sa=uow.execute("UPDATE sects SET sect_materials=xiuxian_int_store(?,'sect_materials','actor') WHERE sect_id=? AND xiuxian_int_cmp(sect_materials,?,1,'sect_materials','actor')=0 AND secbuff=?",(left,sect_id,materials,str(expected_catalog)))
            if sa.rowcount != 1:
                self._secbuff_reject("materials", sect_id)
            saved_sect=uow.query_one("SELECT secbuff,sect_materials FROM sects WHERE sect_id=?",(sect_id,))
            if saved_sect is None or str(saved_sect["secbuff"]) != str(expected_catalog) or require_exact_integer(saved_sect["sect_materials"], field="sect_materials", subject=str(sect_id)) != left:
                self._secbuff_reject("materials", sect_id)
            ba=uow.execute("UPDATE BuffInfo SET sec_buff=? WHERE user_id=? AND COALESCE(sec_buff,0)<>?",(buff_id,user_id,buff_id))
            if ba.rowcount != 1:
                self._secbuff_reject("buff", user_id)
            saved_buff=uow.query_one("SELECT sec_buff FROM BuffInfo WHERE user_id=?",(user_id,))
            if saved_buff is None or require_exact_integer(saved_buff["sec_buff"], field="sec_buff", subject=user_id) != buff_id:
                self._secbuff_reject("buff", user_id)
            recorded=uow.execute("INSERT INTO sect_secbuff_learn_operations(operation_id,user_id,sect_id,buff_id,materials_cost,materials_left) VALUES(?,?,?,?,?,?)",(operation_id,user_id,sect_id,buff_id,materials_cost,left))
            if recorded.rowcount != 1:
                self._secbuff_reject("receipt", operation_id)
            saved_receipt=uow.query_one("SELECT user_id,sect_id,buff_id,materials_cost,materials_left FROM sect_secbuff_learn_operations WHERE operation_id=?",(operation_id,))
            if (saved_receipt is None or str(saved_receipt["user_id"]) != user_id or int(saved_receipt["sect_id"]) != sect_id
                    or int(saved_receipt["buff_id"]) != buff_id or int(saved_receipt["materials_cost"]) != materials_cost
                    or int(saved_receipt["materials_left"]) != left):
                self._secbuff_reject("receipt", operation_id)
            return {"status":"learned","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"materials_cost":materials_cost,"materials_left":left}
    def learn_main(self, operation_id, user_id, sect_id, buff_id, materials_cost, *, expected_catalog, forbidden_positions=(12,14,15)):
        operation_id,user_id=str(operation_id).strip(),str(user_id);sect_id,buff_id,materials_cost=int(sect_id),int(buff_id),int(materials_cost)
        try:
            return self._learn_main(operation_id, user_id, sect_id, buff_id, materials_cost,
                                    expected_catalog=expected_catalog,
                                    forbidden_positions=forbidden_positions)
        except DomainError as exc:
            if exc.code.startswith("sect_mainbuff_") and exc.code.endswith("_write_rejected"):
                return {
                    "status": "state_changed",
                    "user_id": user_id,
                    "sect_id": sect_id,
                    "buff_id": buff_id,
                    "materials_cost": materials_cost,
                    "reason": exc.details.get("reason") or exc.code,
                    "subject": str(exc.details.get("subject") or operation_id),
                }
            raise

    @staticmethod
    def _mainbuff_reject(step: str, subject: str | int) -> None:
        reason = f"sect_mainbuff_{step}_write_rejected"
        raise DomainError(reason, f"{reason}: {subject}", {"reason": reason, "subject": str(subject)})

    def _learn_main(self, operation_id, user_id, sect_id, buff_id, materials_cost, *, expected_catalog, forbidden_positions):
        base = {"user_id": user_id, "sect_id": sect_id, "buff_id": buff_id}
        if materials_cost < 0:
            return {"status": "invalid_materials", **base, "materials_cost": materials_cost}
        with DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            if not table_ready(uow, "sect_mainbuff_learn_operations", frozenset(
                {"operation_id", "user_id", "sect_id", "buff_id", "materials_cost", "materials_left"}
            )):
                return {"status": "schema_missing", **base, "reason": "schema_missing", "subject": operation_id}
            old=uow.query_one("SELECT user_id,sect_id,buff_id,materials_cost,materials_left FROM sect_mainbuff_learn_operations WHERE operation_id=?",(operation_id,))
            if old:
                if (str(old["user_id"]),int(old["sect_id"]),int(old["buff_id"]),int(old["materials_cost"]))!=(user_id,sect_id,buff_id,materials_cost):return {**{"status":"state_changed","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}, "reason": "operation_conflict", "subject": operation_id}
                return {"status":"duplicate","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"materials_cost":old["materials_cost"],"materials_left":old["materials_left"]}
            user=uow.query_one("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?",(user_id,));
            if user is None or user["sect_id"] is None or int(user["sect_id"])!=sect_id:return {"status":"membership_changed","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"reason":"user_missing" if user is None else "sect_changed"}
            if int(user["sect_position"] or 0) in {int(v) for v in forbidden_positions}:return {"status":"position_forbidden","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            sect=uow.query_one("SELECT mainbuff,COALESCE(sect_materials,0) AS materials FROM sects WHERE sect_id=?",(sect_id,));
            if sect is None or str(sect["mainbuff"])!=str(expected_catalog):return {"status":"catalog_changed","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"reason":"sect_missing" if sect is None else "sect_catalog_changed"}
            materials=require_exact_integer(sect["materials"], field="sect_materials", subject=str(sect_id));buff=uow.query_one("SELECT main_buff FROM BuffInfo WHERE user_id=?",(user_id,))
            if materials<materials_cost:return {"status":"materials_insufficient","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"materials_cost":materials_cost,"materials_left":materials}
            if buff is None:return {"status":"buff_missing","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            current_buff=require_exact_integer(buff["main_buff"] or 0, field="main_buff", subject=user_id)
            if current_buff==buff_id:return {"status":"already_learned","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            left=materials-materials_cost
            sa=uow.execute("UPDATE sects SET sect_materials=xiuxian_int_store(?,'sect_materials','actor') WHERE sect_id=? AND xiuxian_int_cmp(sect_materials,?,1,'sect_materials','actor')=0 AND mainbuff=?",(left,sect_id,materials,str(expected_catalog)))
            if sa.rowcount != 1:
                self._mainbuff_reject("materials", sect_id)
            saved_sect=uow.query_one("SELECT mainbuff,sect_materials FROM sects WHERE sect_id=?",(sect_id,))
            if saved_sect is None or str(saved_sect["mainbuff"]) != str(expected_catalog) or require_exact_integer(saved_sect["sect_materials"], field="sect_materials", subject=str(sect_id)) != left:
                self._mainbuff_reject("materials", sect_id)
            ba=uow.execute("UPDATE BuffInfo SET main_buff=? WHERE user_id=? AND COALESCE(main_buff,0)<>?",(buff_id,user_id,buff_id))
            if ba.rowcount != 1:
                self._mainbuff_reject("buff", user_id)
            saved_buff=uow.query_one("SELECT main_buff FROM BuffInfo WHERE user_id=?",(user_id,))
            if saved_buff is None or require_exact_integer(saved_buff["main_buff"], field="main_buff", subject=user_id) != buff_id:
                self._mainbuff_reject("buff", user_id)
            recorded=uow.execute("INSERT INTO sect_mainbuff_learn_operations(operation_id,user_id,sect_id,buff_id,materials_cost,materials_left) VALUES(?,?,?,?,?,?)",(operation_id,user_id,sect_id,buff_id,materials_cost,left))
            if recorded.rowcount != 1:
                self._mainbuff_reject("receipt", operation_id)
            saved_receipt=uow.query_one("SELECT user_id,sect_id,buff_id,materials_cost,materials_left FROM sect_mainbuff_learn_operations WHERE operation_id=?",(operation_id,))
            if (saved_receipt is None or str(saved_receipt["user_id"]) != user_id or int(saved_receipt["sect_id"]) != sect_id
                    or int(saved_receipt["buff_id"]) != buff_id or int(saved_receipt["materials_cost"]) != materials_cost
                    or int(saved_receipt["materials_left"]) != left):
                self._mainbuff_reject("receipt", operation_id)
            return {"status":"learned","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"materials_cost":materials_cost,"materials_left":left}
    def purchase(self, operation_id, user_id, sect_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, legacy_purchased, max_goods_num, week_key=None):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        sect_id, item_id, quantity, unit_cost, weekly_limit, legacy_purchased, max_goods_num = map(
            int, (sect_id, item_id, quantity, unit_cost, weekly_limit, legacy_purchased, max_goods_num)
        )
        week_key = str(week_key or weekly_shop_key(self.clock.now()))
        payload = f"{user_id}|{sect_id}|{item_id}|{item_name}|{item_type}|{quantity}|{unit_cost}|{weekly_limit}|{week_key}"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old = uow.query_one(
                "SELECT payload,quantity,cost,contribution,materials,purchased "
                "FROM sect_shop_purchase_operations WHERE operation_id=?", (operation_id,)
            )
            if old:
                if str(old["payload"]) != payload:
                    return {"status": "state_changed", "reason": "operation_conflict", "subject": "actor"}
                return {
                    "status": "duplicate",
                    **{
                        key: require_exact_integer(old[key], field=f"sect_receipt_{key}", subject=user_id)
                        for key in ("quantity", "cost", "contribution", "materials", "purchased")
                    },
                }
            user = uow.query_one(
                "SELECT sect_id,COALESCE(sect_contribution,0) AS contribution "
                "FROM user_xiuxian WHERE user_id=?", (user_id,)
            )
            sect = uow.query_one(
                "SELECT COALESCE(sect_materials,0) AS materials,COALESCE(closed,0) AS closed "
                "FROM sects WHERE sect_id=?", (sect_id,)
            )
            if user is None or sect is None or int(user["sect_id"] or 0) != sect_id:
                return {
                    "status": "membership_changed",
                    "reason": "user_missing" if user is None else "sect_missing" if sect is None else "sect_changed",
                }
            contribution = require_exact_integer(user["contribution"], field="sect_contribution", subject=user_id)
            materials = require_exact_integer(sect["materials"], field="sect_materials", subject="sect")
            if int(sect["closed"]):
                return {"status": "sect_closed", "contribution": contribution, "materials": materials}
            row = uow.query_one(
                "SELECT quantity FROM sect_shop_weekly_purchases WHERE user_id=? AND week_key=? AND item_id=?",
                (user_id, week_key, item_id),
            )
            purchased = require_exact_integer(row["quantity"], field="sect_shop_purchased", subject=user_id) if row else legacy_purchased
            if purchased + quantity > weekly_limit:
                return {"status": "limit_reached", "contribution": contribution, "materials": materials, "purchased": purchased}
            cost = quantity * unit_cost
            if contribution < cost:
                return {"status": "contribution_insufficient", "contribution": contribution, "materials": materials, "purchased": purchased}
            if materials < cost:
                return {"status": "materials_insufficient", "contribution": contribution, "materials": materials, "purchased": purchased}
            item = uow.query_one(
                "SELECT COALESCE(goods_num,0) AS n,COALESCE(bind_num,0) AS b FROM back WHERE user_id=? AND goods_id=?",
                (user_id, item_id),
            )
            inventory = require_exact_integer(item["n"], field="sect_shop_inventory", subject=user_id) if item else 0
            bound_inventory = require_exact_integer(item["b"], field="sect_shop_bound_inventory", subject=user_id) if item else 0
            if inventory < 0 or bound_inventory < 0 or bound_inventory > inventory:
                return {"status": "inventory_invalid", "contribution": contribution, "materials": materials, "purchased": purchased}
            if inventory + quantity > max_goods_num:
                return {"status": "inventory_full", "contribution": contribution, "materials": materials, "purchased": purchased}
            before_contribution, before_materials = contribution, materials
            contribution -= cost
            materials -= cost
            purchased += quantity
            expected_inventory = inventory + quantity
            expected_bound_inventory = bound_inventory + quantity
            try:
                with uow.savepoint("sect_shop_purchase"):
                    member_write = uow.execute(
                        "UPDATE user_xiuxian SET sect_contribution=xiuxian_int_store(?,'sect_contribution','actor') "
                        "WHERE user_id=? AND sect_id=? AND xiuxian_int_cmp(sect_contribution,?,1,'sect_contribution','actor')=0",
                        (bind_sqlite_param(contribution, exact=True), user_id, sect_id,
                         bind_sqlite_param(before_contribution, exact=True)),
                    )
                    saved_member = uow.query_one("SELECT sect_contribution FROM user_xiuxian WHERE user_id=?", (user_id,))
                    if member_write.rowcount != 1 or saved_member is None or require_exact_integer(saved_member["sect_contribution"], field="sect_contribution", subject=user_id) != contribution:
                        raise _ShopPurchaseRejected("sect_shop_member_write_rejected", user_id)
                    sect_write = uow.execute(
                        "UPDATE sects SET sect_materials=xiuxian_int_store(?,'sect_materials','actor') "
                        "WHERE sect_id=? AND xiuxian_int_cmp(sect_materials,?,1,'sect_materials','actor')=0",
                        (bind_sqlite_param(materials, exact=True), sect_id, bind_sqlite_param(before_materials, exact=True)),
                    )
                    saved_sect = uow.query_one("SELECT sect_materials FROM sects WHERE sect_id=?", (sect_id,))
                    if sect_write.rowcount != 1 or saved_sect is None or require_exact_integer(saved_sect["sect_materials"], field="sect_materials", subject="sect") != materials:
                        raise _ShopPurchaseRejected("sect_shop_state_write_rejected", str(sect_id))
                    stamp = self.clock.now().isoformat()
                    inventory_write = uow.execute(
                        "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_num=xiuxian_int_add(COALESCE(back.goods_num,0),excluded.goods_num), "
                        "bind_num=xiuxian_int_add(COALESCE(back.bind_num,0),excluded.bind_num),update_time=excluded.update_time",
                        (user_id, item_id, str(item_name), str(item_type), quantity, stamp, stamp, quantity),
                    )
                    saved_item = uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                    if inventory_write.rowcount != 1 or saved_item is None or (
                        require_exact_integer(saved_item["goods_num"], field="sect_shop_inventory", subject=user_id) != expected_inventory
                        or require_exact_integer(saved_item["bind_num"], field="sect_shop_bound_inventory", subject=user_id) != expected_bound_inventory
                    ):
                        raise _ShopPurchaseRejected("sect_shop_inventory_write_rejected", user_id)
                    weekly_write = uow.execute(
                        "INSERT INTO sect_shop_weekly_purchases(user_id,week_key,item_id,quantity) VALUES(?,?,?,?) "
                        "ON CONFLICT(user_id,week_key,item_id) DO UPDATE SET quantity=excluded.quantity",
                        (user_id, week_key, item_id, purchased),
                    )
                    saved_weekly = uow.query_one(
                        "SELECT quantity FROM sect_shop_weekly_purchases WHERE user_id=? AND week_key=? AND item_id=?",
                        (user_id, week_key, item_id),
                    )
                    if weekly_write.rowcount != 1 or saved_weekly is None or require_exact_integer(saved_weekly["quantity"], field="sect_shop_purchased", subject=user_id) != purchased:
                        raise _ShopPurchaseRejected("sect_shop_weekly_write_rejected", user_id)
                    receipt = uow.execute(
                        "INSERT INTO sect_shop_purchase_operations(operation_id,payload,quantity,cost,contribution,materials,purchased) "
                        "VALUES(?,?,?,xiuxian_int_store(?,'stone','actor'),?,?,?)",
                        (operation_id, payload, quantity, bind_sqlite_param(cost, exact=True),
                         bind_sqlite_param(contribution, exact=True), bind_sqlite_param(materials, exact=True), purchased),
                    )
                    saved_receipt = uow.query_one(
                        "SELECT payload,quantity,cost,contribution,materials,purchased "
                        "FROM sect_shop_purchase_operations WHERE operation_id=?", (operation_id,)
                    )
                    if receipt.rowcount != 1 or saved_receipt is None or (
                        str(saved_receipt["payload"]) != payload
                        or require_exact_integer(saved_receipt["quantity"], field="sect_receipt_quantity", subject=operation_id) != quantity
                        or require_exact_integer(saved_receipt["cost"], field="sect_receipt_cost", subject=operation_id) != cost
                        or require_exact_integer(saved_receipt["contribution"], field="sect_receipt_contribution", subject=operation_id) != contribution
                        or require_exact_integer(saved_receipt["materials"], field="sect_receipt_materials", subject=operation_id) != materials
                        or require_exact_integer(saved_receipt["purchased"], field="sect_receipt_purchased", subject=operation_id) != purchased
                    ):
                        raise _ShopPurchaseRejected("sect_shop_receipt_write_rejected", operation_id)
                    final_member = uow.query_one("SELECT sect_id,sect_contribution FROM user_xiuxian WHERE user_id=?", (user_id,))
                    final_sect = uow.query_one("SELECT sect_materials FROM sects WHERE sect_id=?", (sect_id,))
                    final_item = uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (user_id, item_id))
                    final_weekly = uow.query_one(
                        "SELECT quantity FROM sect_shop_weekly_purchases WHERE user_id=? AND week_key=? AND item_id=?",
                        (user_id, week_key, item_id),
                    )
                    if final_member is None or int(final_member["sect_id"] or 0) != sect_id or require_exact_integer(final_member["sect_contribution"], field="sect_contribution", subject=user_id) != contribution:
                        raise _ShopPurchaseRejected("sect_shop_member_write_rejected", user_id)
                    if final_sect is None or require_exact_integer(final_sect["sect_materials"], field="sect_materials", subject="sect") != materials:
                        raise _ShopPurchaseRejected("sect_shop_state_write_rejected", str(sect_id))
                    if final_item is None or require_exact_integer(final_item["goods_num"], field="sect_shop_inventory", subject=user_id) != expected_inventory or require_exact_integer(final_item["bind_num"], field="sect_shop_bound_inventory", subject=user_id) != expected_bound_inventory:
                        raise _ShopPurchaseRejected("sect_shop_inventory_write_rejected", user_id)
                    if final_weekly is None or require_exact_integer(final_weekly["quantity"], field="sect_shop_purchased", subject=user_id) != purchased:
                        raise _ShopPurchaseRejected("sect_shop_weekly_write_rejected", user_id)
            except _ShopPurchaseRejected as exc:
                return {
                    "status": "state_changed", "reason": exc.reason, "subject": str(exc.subject),
                    "contribution": before_contribution, "materials": before_materials, "purchased": purchased - quantity,
                }
            return {"status":"applied","quantity":quantity,"cost":cost,"contribution":contribution,"materials":materials,"purchased":purchased}
    def donate(self, operation_id, user_id, sect_id, stone, materials):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        sect_id, stone, materials = int(sect_id), int(stone), int(materials)
        data = {"user_id": user_id, "sect_id": sect_id, "stone": stone, "materials": materials}
        if stone <= 0:
            return {"status": "invalid_amount", **data}
        if materials < 0:
            return {"status": "invalid_materials", **data}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old = uow.query_one("SELECT user_id,sect_id,stone,materials FROM sect_donation_operations WHERE operation_id=?", (operation_id,))
            if old:
                if (str(old["user_id"]), int(old["sect_id"]), as_int_like(old["stone"]), as_int_like(old["materials"])) != (user_id, sect_id, stone, materials):
                    return {"status": "operation_conflict", "reason": "operation_conflict", **data}
                return {"status": "duplicate", **data}
            user = uow.query_one("SELECT sect_id,stone,COALESCE(sect_contribution,0) AS contribution FROM user_xiuxian WHERE user_id=?", (user_id,))
            sect = uow.query_one("SELECT COALESCE(sect_used_stone,0) AS reserve,COALESCE(sect_scale,0) AS scale,COALESCE(sect_materials,0) AS materials FROM sects WHERE sect_id=?", (sect_id,))
            if user is None:
                return {"status": "user_missing", **data}
            if user["sect_id"] is None or int(user["sect_id"]) != sect_id:
                return {"status": "sect_changed", **data}
            wallet, contribution = as_int_like(user["stone"]), as_int_like(user["contribution"])
            if wallet < stone:
                return {"status": "stone_insufficient", **data}
            if sect is None:
                return {"status": "sect_missing", **data}
            reserve, scale, supply = (as_int_like(sect[key]) for key in ("reserve", "scale", "materials"))
            try:
                with uow.savepoint("sect_donation_complete"):
                    updated = uow.execute(
                        "UPDATE user_xiuxian SET stone=xiuxian_int_store(?,'stone','actor'),sect_contribution=xiuxian_int_store(?,'sect_contribution','actor') WHERE user_id=? AND sect_id=? AND xiuxian_int_cmp(stone,?,1,'stone','actor')=0 AND xiuxian_int_cmp(COALESCE(sect_contribution,0),?,1,'sect_contribution','actor')=0",
                        (wallet - stone, contribution + stone, user_id, sect_id, wallet, contribution),
                    )
                    saved = uow.query_one("SELECT stone,sect_contribution FROM user_xiuxian WHERE user_id=?", (user_id,))
                    if updated.rowcount != 1 or saved is None or tuple(as_int_like(saved[key]) for key in ("stone", "sect_contribution")) != (wallet - stone, contribution + stone):
                        raise _DonationRejected("sect_donation_member_write_rejected", "actor")
                    updated = uow.execute(
                        "UPDATE sects SET sect_used_stone=xiuxian_int_store(?,'sect_used_stone','actor'),sect_scale=?,sect_materials=xiuxian_int_store(?,'sect_materials','actor') WHERE sect_id=? AND xiuxian_int_cmp(COALESCE(sect_used_stone,0),?,1,'sect_used_stone','actor')=0 AND xiuxian_int_cmp(COALESCE(sect_scale,0),?)=0 AND xiuxian_int_cmp(COALESCE(sect_materials,0),?,1,'sect_materials','actor')=0",
                        (reserve + stone, scale + stone, supply + materials, sect_id, reserve, scale, supply),
                    )
                    saved = uow.query_one("SELECT sect_used_stone,sect_scale,sect_materials FROM sects WHERE sect_id=?", (sect_id,))
                    if updated.rowcount != 1 or saved is None or tuple(as_int_like(saved[key]) for key in ("sect_used_stone", "sect_scale", "sect_materials")) != (reserve + stone, scale + stone, supply + materials):
                        raise _DonationRejected("sect_donation_assets_write_rejected", "sect")
                    receipt = uow.execute("INSERT INTO sect_donation_operations(operation_id,user_id,sect_id,stone,materials) VALUES(?,?,?,xiuxian_int_store(?,'stone','actor'),?)", (operation_id, user_id, sect_id, stone, materials))
                    if receipt.rowcount != 1:
                        raise _DonationRejected("sect_donation_receipt_write_rejected", "actor")
            except _DonationRejected as exc:
                return {"status": "write_rejected", "reason": exc.reason, "subject": exc.subject, **data}
            return {"status": "donated", **data}
    def change_position(self, operation_id, actor_id, target_id, requested_position, position_limits, *, manager_max_position):
        operation_id, actor_id, target_id = str(operation_id).strip(), str(actor_id), str(target_id);requested_position=int(requested_position);manager_max_position=int(manager_max_position);limits={int(k):max(0,int(v)) for k,v in position_limits.items()}
        if requested_position not in limits:return {"status":"invalid_position","actor_id":actor_id,"target_id":target_id}
        if actor_id==target_id:return {"status":"self_target","actor_id":actor_id,"target_id":target_id}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old=uow.query_one("SELECT actor_id,target_id,sect_id,actor_name,target_name,old_position,new_position FROM sect_position_change_operations WHERE operation_id=?",(operation_id,))
            if old:
                if str(old["actor_id"]) != actor_id or str(old["target_id"]) != target_id or int(old["new_position"]) != requested_position:
                    return {"status":"operation_conflict","actor_id":actor_id,"target_id":target_id,"sect_id":old["sect_id"],"reason":"operation_conflict","subject":"actor"}
                return {"status":"duplicate","actor_id":actor_id,"target_id":target_id,"sect_id":old["sect_id"],"actor_name":str(old["actor_name"] or ""),"target_name":str(old["target_name"] or ""),"old_position":old["old_position"],"new_position":old["new_position"]}
            actor=uow.query_one("SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?",(actor_id,));target=uow.query_one("SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?",(target_id,))
            if actor is None:return {"status":"actor_missing","actor_id":actor_id,"target_id":target_id}
            if actor["sect_id"] is None:return {"status":"actor_without_sect","actor_id":actor_id,"target_id":target_id}
            if target is None:return {"status":"target_missing","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if int(actor["sect_position"] or 0)>manager_max_position:return {"status":"actor_not_manager","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if target["sect_id"]!=actor["sect_id"]:return {"status":"target_not_member","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if int(target["sect_position"] or 0)<=int(actor["sect_position"] or 0):return {"status":"target_not_below_actor","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if requested_position<=int(actor["sect_position"]):return {"status":"position_not_below_actor","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            current=int(target["sect_position"]);count=uow.query_one("SELECT COUNT(*) AS n FROM user_xiuxian WHERE sect_id=? AND sect_position=? AND user_id<>?",(actor["sect_id"],requested_position,target_id))
            if limits[requested_position]>0 and int(count["n"])>=limits[requested_position]:return {"status":"position_full","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if current==requested_position:return {"status":"unchanged","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"],"old_position":current,"new_position":requested_position}
            try:
                with uow.savepoint("sect_position_change"):
                    changed=uow.execute("UPDATE user_xiuxian SET sect_position=? WHERE user_id=? AND sect_id=? AND sect_position=?",(requested_position,target_id,actor["sect_id"],current))
                    saved_target=uow.query_one("SELECT sect_position FROM user_xiuxian WHERE user_id=? AND sect_id=?",(target_id,actor["sect_id"]))
                    if changed.rowcount != 1 or saved_target is None or int(saved_target["sect_position"]) != requested_position:
                        raise _PositionRejected("sect_position_member_write_rejected", target_id)
                    receipt=uow.execute("INSERT INTO sect_position_change_operations(operation_id,actor_id,target_id,sect_id,actor_name,target_name,old_position,new_position) VALUES(?,?,?,?,?,?,?,?)",(operation_id,actor_id,target_id,actor["sect_id"],str(actor["user_name"] or ""),str(target["user_name"] or ""),current,requested_position))
                    if receipt.rowcount != 1:
                        raise _PositionRejected("sect_position_receipt_write_rejected", operation_id)
                    saved_receipt=uow.query_one("SELECT actor_id,target_id,sect_id,actor_name,target_name,old_position,new_position FROM sect_position_change_operations WHERE operation_id=?",(operation_id,))
                    expected=(actor_id,target_id,int(actor["sect_id"]),str(actor["user_name"] or ""),str(target["user_name"] or ""),current,requested_position)
                    actual=None if saved_receipt is None else (str(saved_receipt["actor_id"]),str(saved_receipt["target_id"]),int(saved_receipt["sect_id"]),str(saved_receipt["actor_name"] or ""),str(saved_receipt["target_name"] or ""),int(saved_receipt["old_position"]),int(saved_receipt["new_position"]))
                    if actual != expected:
                        raise _PositionRejected("sect_position_receipt_write_rejected", operation_id)
            except _PositionRejected as exc:
                return {"status":"state_changed","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"],"reason":exc.reason,"subject":str(exc.subject)}
            return {"status":"changed","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"],"actor_name":str(actor["user_name"] or ""),"target_name":str(target["user_name"] or ""),"old_position":current,"new_position":requested_position}
    def kick(self, operation_id, actor_id, target_id, *, manager_max_position):
        operation_id, actor_id, target_id = str(operation_id).strip(), str(actor_id), str(target_id)
        manager_max_position = int(manager_max_position)
        if actor_id == target_id: return {"status": "self_target", "actor_id": actor_id, "target_id": target_id}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old = uow.query_one("SELECT operation_type,actor_id,target_id,sect_id,sect_name,actor_name,target_name,actor_position,target_position FROM sect_member_removal_operations WHERE operation_id=?", (operation_id,))
            if old:
                if str(old["operation_type"]) != "kick" or str(old["actor_id"]) != actor_id or str(old["target_id"]) != target_id:
                    return {"status": "operation_conflict", "actor_id": actor_id, "target_id": target_id, "sect_id": old["sect_id"], "reason": "operation_conflict", "subject": "actor"}
                return {"status": "duplicate", "actor_id": actor_id, "target_id": target_id, "sect_id": old["sect_id"], "sect_name": str(old["sect_name"] or ""), "actor_name": str(old["actor_name"] or ""), "target_name": str(old["target_name"] or ""), "actor_position": old["actor_position"], "target_position": old["target_position"]}
            actor=uow.query_one("SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?",(actor_id,));target=uow.query_one("SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?",(target_id,))
            if actor is None:return {"status":"actor_not_found","actor_id":actor_id,"target_id":target_id}
            if actor["sect_id"] is None:return {"status":"actor_not_in_sect","actor_id":actor_id,"target_id":target_id}
            if target is None:return {"status":"target_not_found","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if int(actor["sect_position"] or 0)>manager_max_position:return {"status":"insufficient_rank","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if target["sect_id"]!=actor["sect_id"]:return {"status":"different_sect","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if int(target["sect_position"] or 0)<=int(actor["sect_position"] or 0):return {"status":"target_not_lower","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            sect=uow.query_one("SELECT sect_name FROM sects WHERE sect_id=?",(actor["sect_id"],));
            if sect is None:return {"status":"sect_not_found","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            try:
                with uow.savepoint("sect_kick"):
                    changed=uow.execute("UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL,sect_contribution=0 WHERE user_id=? AND sect_id=? AND sect_position=?",(target_id,target["sect_id"],target["sect_position"]))
                    saved_target=uow.query_one("SELECT sect_id,sect_position,COALESCE(sect_contribution,0) AS sect_contribution FROM user_xiuxian WHERE user_id=?",(target_id,))
                    if changed.rowcount != 1 or saved_target is None or saved_target["sect_id"] is not None or saved_target["sect_position"] is not None or int(saved_target["sect_contribution"] or 0) != 0:
                        raise _RemovalRejected("sect_kick_member_write_rejected", target_id)
                    receipt=uow.execute("INSERT INTO sect_member_removal_operations(operation_id,operation_type,actor_id,target_id,sect_id,sect_name,actor_name,target_name,actor_position,target_position) VALUES(?,?,?,?,?,?,?,?,?,?)",(operation_id,"kick",actor_id,target_id,actor["sect_id"],str(sect["sect_name"] or ""),str(actor["user_name"] or ""),str(target["user_name"] or ""),actor["sect_position"],target["sect_position"]))
                    if receipt.rowcount != 1:
                        raise _RemovalRejected("sect_kick_receipt_write_rejected", operation_id)
                    saved_receipt=uow.query_one("SELECT operation_type,actor_id,target_id,sect_id,sect_name,actor_name,target_name,actor_position,target_position FROM sect_member_removal_operations WHERE operation_id=?",(operation_id,))
                    expected=("kick",actor_id,target_id,int(actor["sect_id"]),str(sect["sect_name"] or ""),str(actor["user_name"] or ""),str(target["user_name"] or ""),actor["sect_position"],target["sect_position"])
                    actual=None if saved_receipt is None else (str(saved_receipt["operation_type"]),str(saved_receipt["actor_id"]),str(saved_receipt["target_id"]),int(saved_receipt["sect_id"]),str(saved_receipt["sect_name"] or ""),str(saved_receipt["actor_name"] or ""),str(saved_receipt["target_name"] or ""),saved_receipt["actor_position"],saved_receipt["target_position"])
                    if actual != expected:
                        raise _RemovalRejected("sect_kick_receipt_write_rejected", operation_id)
            except _RemovalRejected as exc:
                return {"status":"state_changed","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"],"reason":exc.reason,"subject":str(exc.subject)}
            return {"status":"kicked","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"],"sect_name":str(sect["sect_name"] or ""),"actor_name":str(actor["user_name"] or ""),"target_name":str(target["user_name"] or ""),"actor_position":actor["sect_position"],"target_position":target["sect_position"]}
    def leave(self, operation_id, user_id, *, owner_position=0):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old = uow.query_one("SELECT operation_type,actor_id,target_id,sect_id,sect_name,actor_name,target_name,actor_position,target_position FROM sect_member_removal_operations WHERE operation_id=?", (operation_id,))
            if old:
                if str(old["operation_type"]) != "leave" or str(old["actor_id"]) != user_id or str(old["target_id"]) != user_id:
                    return {"status": "operation_conflict", "user_id": user_id, "target_id": user_id, "sect_id": old["sect_id"], "reason": "operation_conflict", "subject": "actor"}
                return {"status": "duplicate", "user_id": user_id, "sect_id": old["sect_id"], "sect_name": str(old["sect_name"] or ""), "actor_name": str(old["actor_name"] or ""), "target_name": str(old["target_name"] or ""), "actor_position": old["actor_position"], "target_position": old["target_position"]}
            actor = uow.query_one("SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?", (user_id,))
            if actor is None: return {"status": "user_not_found", "user_id": user_id, "target_id": user_id}
            if actor["sect_id"] is None: return {"status": "not_in_sect", "user_id": user_id, "target_id": user_id}
            if int(actor["sect_position"] or 0) == int(owner_position): return {"status": "owner_cannot_leave", "user_id": user_id, "target_id": user_id, "sect_id": actor["sect_id"], "actor_name": str(actor["user_name"] or ""), "target_name": str(actor["user_name"] or ""), "actor_position": actor["sect_position"], "target_position": actor["sect_position"]}
            sect = uow.query_one("SELECT sect_name FROM sects WHERE sect_id=?", (actor["sect_id"],))
            if sect is None: return {"status": "sect_not_found", "user_id": user_id, "target_id": user_id, "sect_id": actor["sect_id"]}
            try:
                with uow.savepoint("sect_leave"):
                    changed = uow.execute("UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL,sect_contribution=0 WHERE user_id=? AND sect_id=? AND sect_position=?", (user_id,actor["sect_id"],actor["sect_position"]))
                    saved_actor=uow.query_one("SELECT sect_id,sect_position,COALESCE(sect_contribution,0) AS sect_contribution FROM user_xiuxian WHERE user_id=?",(user_id,))
                    if changed.rowcount != 1 or saved_actor is None or saved_actor["sect_id"] is not None or saved_actor["sect_position"] is not None or int(saved_actor["sect_contribution"] or 0) != 0:
                        raise _RemovalRejected("sect_leave_member_write_rejected", user_id)
                    receipt=uow.execute("INSERT INTO sect_member_removal_operations(operation_id,operation_type,actor_id,target_id,sect_id,sect_name,actor_name,target_name,actor_position,target_position) VALUES(?,?,?,?,?,?,?,?,?,?)", (operation_id,"leave",user_id,user_id,actor["sect_id"],str(sect["sect_name"] or ""),str(actor["user_name"] or ""),str(actor["user_name"] or ""),actor["sect_position"],actor["sect_position"]))
                    if receipt.rowcount != 1:
                        raise _RemovalRejected("sect_leave_receipt_write_rejected", operation_id)
                    saved_receipt=uow.query_one("SELECT operation_type,actor_id,target_id,sect_id,sect_name,actor_name,target_name,actor_position,target_position FROM sect_member_removal_operations WHERE operation_id=?",(operation_id,))
                    expected=("leave",user_id,user_id,int(actor["sect_id"]),str(sect["sect_name"] or ""),str(actor["user_name"] or ""),str(actor["user_name"] or ""),actor["sect_position"],actor["sect_position"])
                    actual=None if saved_receipt is None else (str(saved_receipt["operation_type"]),str(saved_receipt["actor_id"]),str(saved_receipt["target_id"]),int(saved_receipt["sect_id"]),str(saved_receipt["sect_name"] or ""),str(saved_receipt["actor_name"] or ""),str(saved_receipt["target_name"] or ""),saved_receipt["actor_position"],saved_receipt["target_position"])
                    if actual != expected:
                        raise _RemovalRejected("sect_leave_receipt_write_rejected", operation_id)
            except _RemovalRejected as exc:
                return {"status":"state_changed","user_id":user_id,"target_id":user_id,"sect_id":actor["sect_id"],"reason":exc.reason,"subject":str(exc.subject)}
            return {"status": "left", "user_id": user_id, "target_id": user_id, "sect_id": actor["sect_id"], "sect_name": str(sect["sect_name"] or ""), "actor_name": str(actor["user_name"] or ""), "target_name": str(actor["user_name"] or ""), "actor_position": actor["sect_position"], "target_position": actor["sect_position"]}
    @staticmethod
    def _member_limit(scale: int) -> int:
        return min(20 + max(0, int(scale)) // 50_000_000, 100)

    def join(self, operation_id, user_id, sect_id, *, member_position=12):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        sect_id, member_position = int(sect_id), int(member_position)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not table_ready(
                uow,
                "sect_member_join_operations",
                frozenset({"operation_id", "user_id", "sect_id", "member_count", "member_limit"}),
            ):
                return {"status": SCHEMA_MISSING, "user_id": user_id, "sect_id": sect_id}
            old = uow.query_one("SELECT o.user_id,o.sect_id,o.member_count,o.member_limit,s.sect_name FROM sect_member_join_operations o LEFT JOIN sects s ON s.sect_id=o.sect_id WHERE o.operation_id=?", (operation_id,))
            if old:
                if str(old["user_id"]) != user_id or int(old["sect_id"]) != sect_id:
                    return {"status": "operation_conflict", "user_id": user_id, "sect_id": sect_id, "reason": "operation_conflict", "subject": "actor"}
                return {"status": "duplicate", "user_id": user_id, "sect_id": sect_id, "sect_name": str(old["sect_name"] or ""), "member_count": int(old["member_count"]), "member_limit": int(old["member_limit"])}
            user = uow.query_one("SELECT sect_id FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None: return {"status": "user_missing", "user_id": user_id, "sect_id": sect_id}
            if user["sect_id"] is not None: return {"status": "already_in_sect", "user_id": user_id, "sect_id": sect_id}
            sect = uow.query_one("SELECT sect_name,sect_scale,join_open,closed FROM sects WHERE sect_id=?", (sect_id,))
            if sect is None: return {"status": "sect_missing", "user_id": user_id, "sect_id": sect_id}
            if int(sect["closed"] or 0) == 1: return {"status": "sect_closed", "user_id": user_id, "sect_id": sect_id, "sect_name": str(sect["sect_name"] or "")}
            if int(sect["join_open"] or 0) != 1: return {"status": "join_closed", "user_id": user_id, "sect_id": sect_id, "sect_name": str(sect["sect_name"] or "")}
            limit = self._member_limit(int(sect["sect_scale"] or 0));count = int(uow.query_one("SELECT COUNT(*) AS n FROM user_xiuxian WHERE sect_id=?", (sect_id,))["n"])
            if count >= limit: return {"status": "sect_full", "user_id": user_id, "sect_id": sect_id, "sect_name": str(sect["sect_name"] or ""), "member_count": count, "member_limit": limit}
            try:
                with uow.savepoint("sect_member_join"):
                    changed = uow.execute("UPDATE user_xiuxian SET sect_id=?,sect_position=? WHERE user_id=? AND sect_id IS NULL", (sect_id,member_position,user_id))
                    expected_count = count + 1
                    saved_user = uow.query_one("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?", (user_id,))
                    saved_sect = uow.query_one("SELECT join_open,closed FROM sects WHERE sect_id=?", (sect_id,))
                    current_count = int(uow.query_one("SELECT COUNT(*) AS n FROM user_xiuxian WHERE sect_id=?", (sect_id,))["n"])
                    if (
                        changed.rowcount != 1
                        or saved_user is None
                        or saved_user["sect_id"] is None
                        or int(saved_user["sect_id"]) != sect_id
                        or saved_user["sect_position"] is None
                        or int(saved_user["sect_position"]) != member_position
                        or current_count != expected_count
                    ):
                        raise _MemberJoinRejected("sect_member_join_member_write_rejected", user_id)
                    if saved_sect is None or int(saved_sect["join_open"] or 0) != 1 or int(saved_sect["closed"] or 0) != 0:
                        raise _MemberJoinRejected("sect_member_join_state_write_rejected", "sect")
                    receipt = uow.execute("INSERT INTO sect_member_join_operations(operation_id,user_id,sect_id,member_count,member_limit) VALUES(?,?,?,?,?)", (operation_id,user_id,sect_id,expected_count,limit))
                    if receipt.rowcount != 1:
                        raise _MemberJoinRejected("sect_member_join_receipt_write_rejected", operation_id)
                    saved_receipt = uow.query_one("SELECT user_id,sect_id,member_count,member_limit FROM sect_member_join_operations WHERE operation_id=?", (operation_id,))
                    expected_receipt = (user_id, sect_id, expected_count, limit)
                    actual_receipt = None if saved_receipt is None else (str(saved_receipt["user_id"]), int(saved_receipt["sect_id"]), int(saved_receipt["member_count"]), int(saved_receipt["member_limit"]))
                    if actual_receipt != expected_receipt:
                        raise _MemberJoinRejected("sect_member_join_receipt_write_rejected", operation_id)
                    saved_user = uow.query_one("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?", (user_id,))
                    saved_sect = uow.query_one("SELECT join_open,closed FROM sects WHERE sect_id=?", (sect_id,))
                    current_count = int(uow.query_one("SELECT COUNT(*) AS n FROM user_xiuxian WHERE sect_id=?", (sect_id,))["n"])
                    if saved_user is None or saved_user["sect_id"] is None or int(saved_user["sect_id"]) != sect_id or saved_user["sect_position"] is None or int(saved_user["sect_position"]) != member_position or current_count != expected_count:
                        raise _MemberJoinRejected("sect_member_join_member_write_rejected", user_id)
                    if saved_sect is None or int(saved_sect["join_open"] or 0) != 1 or int(saved_sect["closed"] or 0) != 0:
                        raise _MemberJoinRejected("sect_member_join_state_write_rejected", "sect")
            except _MemberJoinRejected as exc:
                return {"status": "state_changed", "user_id": user_id, "sect_id": sect_id, "reason": exc.reason, "subject": exc.subject}
            return {"status": "joined", "user_id": user_id, "sect_id": sect_id, "sect_name": str(sect["sect_name"] or ""), "member_count": expected_count, "member_limit": limit}

    def rename(self, operation_id, actor_id, sect_id, new_name, cost, card_id):
        operation_id, actor_id, new_name = str(operation_id).strip(), str(actor_id), str(new_name).strip()
        sect_id, cost, card_id = int(sect_id), int(cost), int(card_id)
        payload = f"{actor_id}|{sect_id}|{new_name}|{cost}|{card_id}"
        try:
            with DatabaseUnitOfWork(self.database, immediate=True) as uow:
                old = uow.query_one("SELECT payload,previous_name,new_name FROM sect_rename_operations WHERE operation_id=?", (operation_id,))
                if old:
                    return {"status": "duplicate", "previous_name": str(old["previous_name"]), "new_name": str(old["new_name"])}
                actor = uow.query_one("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?", (actor_id,))
                sect = uow.query_one("SELECT sect_name,sect_owner,sect_used_stone FROM sects WHERE sect_id=?", (sect_id,))
                if actor is None or sect is None or int(actor["sect_id"] or 0) != sect_id:
                    return {"status": "sect_missing"}
                if str(sect["sect_owner"]) != actor_id:
                    return {"status": "not_owner"}
                if str(sect["sect_name"]) == new_name or uow.query_one("SELECT 1 AS ok FROM sects WHERE sect_name=? AND sect_id<>?", (new_name, sect_id)):
                    return {"status": "name_exists"}
                card = uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (actor_id, card_id))
                if as_int_like(sect["sect_used_stone"] or 0) < cost:
                    return {"status": "stone_insufficient"}
                if card is None or int(card["goods_num"] or 0) <= 0 or int(card["bind_num"] or 0) <= 0:
                    return {"status": "card_insufficient"}
                previous_name = str(sect["sect_name"])
                previous_reserve = as_int_like(sect["sect_used_stone"] or 0)
                expected_reserve = previous_reserve - cost
                changed = uow.execute("UPDATE sects SET sect_name=?,sect_used_stone=xiuxian_int_store(xiuxian_int_sub(sect_used_stone,?,'sect_used_stone','actor')) WHERE sect_id=? AND sect_owner=?", (new_name, cost, sect_id, actor_id))
                saved_sect = uow.query_one("SELECT sect_name,sect_used_stone FROM sects WHERE sect_id=?", (sect_id,))
                if changed.rowcount != 1 or saved_sect is None or str(saved_sect["sect_name"]) != new_name or as_int_like(saved_sect["sect_used_stone"] or 0) != expected_reserve:
                    raise _RenameRejected("sect_rename_state_write_rejected", "sect")
                goods_num, bind_num = int(card["goods_num"] or 0), int(card["bind_num"] or 0)
                changed = uow.execute("UPDATE back SET goods_num=goods_num-1,bind_num=bind_num-1 WHERE user_id=? AND goods_id=?", (actor_id, card_id))
                saved_card = uow.query_one("SELECT goods_num,bind_num FROM back WHERE user_id=? AND goods_id=?", (actor_id, card_id))
                if changed.rowcount != 1 or saved_card is None or (int(saved_card["goods_num"] or 0), int(saved_card["bind_num"] or 0)) != (goods_num - 1, bind_num - 1):
                    raise _RenameRejected("sect_rename_card_write_rejected", "actor")
                receipt = uow.execute("INSERT INTO sect_rename_operations(operation_id,payload,previous_name,new_name) VALUES(?,?,?,?)", (operation_id, payload, previous_name, new_name))
                saved_receipt = uow.query_one("SELECT payload,previous_name,new_name FROM sect_rename_operations WHERE operation_id=?", (operation_id,))
                if receipt.rowcount != 1 or saved_receipt is None or (str(saved_receipt["payload"]), str(saved_receipt["previous_name"]), str(saved_receipt["new_name"])) != (payload, previous_name, new_name):
                    raise _RenameRejected("sect_rename_receipt_write_rejected", "actor")
                return {"status": "renamed", "previous_name": previous_name, "new_name": new_name}
        except _RenameRejected as exc:
            return {"status": "write_rejected", "reason": exc.reason, "subject": exc.subject, "previous_name": previous_name, "new_name": new_name}

    def weekly_shop_progress(self, user_id, *, item_ids=None, week_key=None, fallback=None) -> dict[str, int]:
        """本人本周在各商品上的已购数量（只读）；缺库/缺表或未迁移返回空 dict。

        与 `purchase` 写路径共用同一张 `sect_shop_weekly_purchases` 与同一周期键，
        因此商店视图不会与限购判定出现口径分歧；`fallback`（迁移前的个人 JSON 记录）
        仅在表中没有该商品行时补位，与 `purchase` 的 `legacy_purchased` 同序。
        请求路径不做任何 DDL。
        """
        user_id = str(user_id).strip()
        if not user_id:
            return {}
        week_key = str(week_key or weekly_shop_key(self.clock.now()))
        purchased: dict[str, int] = {}
        if Path(self.database).is_file():
            try:
                with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                    table = uow.query_one(
                        "SELECT 1 AS present FROM sqlite_master "
                        "WHERE type='table' AND name='sect_shop_weekly_purchases'"
                    )
                    if table is not None:
                        rows = uow.query_all(
                            "SELECT item_id,quantity FROM sect_shop_weekly_purchases "
                            "WHERE user_id=? AND week_key=?",
                            (user_id, week_key),
                        )
                    else:
                        rows = ()
            except (OSError, ValueError, TypeError, KeyError):
                rows = ()
            purchased = {str(row["item_id"]): max(0, int(row["quantity"] or 0)) for row in rows}
        if isinstance(fallback, Mapping):
            for key, value in fallback.items():
                try:
                    purchased.setdefault(str(key), max(0, int(value or 0)))
                except (TypeError, ValueError):
                    continue
        if item_ids is None:
            return purchased
        wanted = {str(item_id) for item_id in item_ids}
        return {key: value for key, value in purchased.items() if key in wanted}


__all__ = ["SectRepository", "LegacySectRepository", "SectRenameSqlRepository", "weekly_shop_key"]
