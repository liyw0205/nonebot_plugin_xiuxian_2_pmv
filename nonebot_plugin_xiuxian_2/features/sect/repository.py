from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from ...infrastructure.database import DatabaseUnitOfWork


class SectRepository(Protocol):
    def donate(self, *args: Any, **kwargs: Any) -> Any: ...
    def change_position(self, *args: Any, **kwargs: Any) -> Any: ...
    def kick(self, *args: Any, **kwargs: Any) -> Any: ...
    def leave(self, *args: Any, **kwargs: Any) -> Any: ...
    def rename(self, *args: Any, **kwargs: Any) -> Any: ...
    def join(self, *args: Any, **kwargs: Any) -> Any: ...
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def learn_main(self, *args: Any, **kwargs: Any) -> Any: ...
    def learn_secondary(self, *args: Any, **kwargs: Any) -> Any: ...
    def claim_elixir(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacySectRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def _service(self, name: str):
        from ...xiuxian.xiuxian_sect.transaction_service import (
            SectMemberJoinService, SectShopPurchaseService, SectMainBuffLearnService,
            SectSecBuffLearnService, SectElixirClaimService,
        )
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


class SectRenameSqlRepository(LegacySectRepository):
    def learn_main(self, operation_id, user_id, sect_id, buff_id, materials_cost, *, expected_catalog, forbidden_positions=(12,14,15)):
        operation_id,user_id=str(operation_id).strip(),str(user_id);sect_id,buff_id,materials_cost=int(sect_id),int(buff_id),int(materials_cost)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old=uow.query_one("SELECT user_id,sect_id,buff_id,materials_cost,materials_left FROM sect_mainbuff_learn_operations WHERE operation_id=?",(operation_id,))
            if old:
                if (str(old["user_id"]),int(old["sect_id"]),int(old["buff_id"]))!=(user_id,sect_id,buff_id):return {"status":"state_changed","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
                return {"status":"duplicate","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"materials_cost":old["materials_cost"],"materials_left":old["materials_left"]}
            user=uow.query_one("SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?",(user_id,));
            if user is None or user["sect_id"] is None or int(user["sect_id"])!=sect_id:return {"status":"membership_changed","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            if int(user["sect_position"] or 0) in {int(v) for v in forbidden_positions}:return {"status":"position_forbidden","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            sect=uow.query_one("SELECT mainbuff,COALESCE(sect_materials,0) AS materials FROM sects WHERE sect_id=?",(sect_id,));
            if sect is None or str(sect["mainbuff"])!=str(expected_catalog):return {"status":"catalog_changed","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            materials=int(sect["materials"]);buff=uow.query_one("SELECT main_buff FROM BuffInfo WHERE user_id=?",(user_id,))
            if materials<materials_cost:return {"status":"materials_insufficient","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"materials_cost":materials_cost,"materials_left":materials}
            if buff is None:return {"status":"buff_missing","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            if int(buff["main_buff"] or 0)==buff_id:return {"status":"already_learned","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            left=materials-materials_cost;sa=uow.execute("UPDATE sects SET sect_materials=? WHERE sect_id=? AND sect_materials=? AND mainbuff=?",(left,sect_id,materials,str(expected_catalog)));ba=uow.execute("UPDATE BuffInfo SET main_buff=? WHERE user_id=? AND COALESCE(main_buff,0)<>?",(buff_id,user_id,buff_id))
            if sa.rowcount!=1 or ba.rowcount!=1:return {"status":"state_changed","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id}
            uow.execute("INSERT INTO sect_mainbuff_learn_operations(operation_id,user_id,sect_id,buff_id,materials_cost,materials_left) VALUES(?,?,?,?,?,?)",(operation_id,user_id,sect_id,buff_id,materials_cost,left));return {"status":"learned","user_id":user_id,"sect_id":sect_id,"buff_id":buff_id,"materials_cost":materials_cost,"materials_left":left}
    def purchase(self, operation_id, user_id, sect_id, item_id, item_name, item_type, quantity, unit_cost, weekly_limit, legacy_purchased, max_goods_num, week_key=None):
        operation_id,user_id=str(operation_id).strip(),str(user_id);sect_id,item_id,quantity,unit_cost,weekly_limit,legacy_purchased,max_goods_num=int(sect_id),int(item_id),int(quantity),int(unit_cost),int(weekly_limit),int(legacy_purchased),int(max_goods_num);week_key=str(week_key or __import__('datetime').date.today().strftime('%G-W%V'));payload=f'{user_id}|{sect_id}|{item_id}|{item_name}|{item_type}|{quantity}|{unit_cost}|{weekly_limit}|{week_key}'
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old=uow.query_one("SELECT payload,quantity,cost,contribution,materials,purchased FROM sect_shop_purchase_operations WHERE operation_id=?",(operation_id,))
            if old:
                if str(old["payload"])!=payload:return {"status":"state_changed"}
                return {"status":"duplicate","quantity":old["quantity"],"cost":old["cost"],"contribution":old["contribution"],"materials":old["materials"],"purchased":old["purchased"]}
            user=uow.query_one("SELECT sect_id,COALESCE(sect_contribution,0) AS contribution FROM user_xiuxian WHERE user_id=?",(user_id,));sect=uow.query_one("SELECT COALESCE(sect_materials,0) AS materials,COALESCE(closed,0) AS closed FROM sects WHERE sect_id=?",(sect_id,))
            if user is None or sect is None or int(user["sect_id"] or 0)!=sect_id:return {"status":"membership_changed"}
            contribution,materials=int(float(user["contribution"] or 0)),int(float(sect["materials"] or 0))
            if int(sect["closed"]):return {"status":"sect_closed","contribution":contribution,"materials":materials}
            row=uow.query_one("SELECT quantity FROM sect_shop_weekly_purchases WHERE user_id=? AND week_key=? AND item_id=?",(user_id,week_key,item_id));purchased=int(row["quantity"]) if row else legacy_purchased
            if purchased+quantity>weekly_limit:return {"status":"limit_reached","contribution":contribution,"materials":materials,"purchased":purchased}
            cost=quantity*unit_cost
            if contribution<cost:return {"status":"contribution_insufficient","contribution":contribution,"materials":materials,"purchased":purchased}
            if materials<cost:return {"status":"materials_insufficient","contribution":contribution,"materials":materials,"purchased":purchased}
            item=uow.query_one("SELECT COALESCE(goods_num,0) AS n FROM back WHERE user_id=? AND goods_id=?",(user_id,item_id));inventory=int(item["n"]) if item else 0
            if inventory+quantity>max_goods_num:return {"status":"inventory_full","contribution":contribution,"materials":materials,"purchased":purchased}
            contribution-=cost;materials-=cost;purchased+=quantity;uow.execute("UPDATE user_xiuxian SET sect_contribution=? WHERE user_id=?",(contribution,user_id));uow.execute("UPDATE sects SET sect_materials=? WHERE sect_id=?",(materials,sect_id));stamp=__import__('datetime').datetime.now().isoformat();uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.goods_num,update_time=excluded.update_time",(user_id,item_id,str(item_name),str(item_type),quantity,stamp,stamp,quantity));uow.execute("INSERT INTO sect_shop_weekly_purchases(user_id,week_key,item_id,quantity) VALUES(?,?,?,?) ON CONFLICT(user_id,week_key,item_id) DO UPDATE SET quantity=excluded.quantity",(user_id,week_key,item_id,purchased));uow.execute("INSERT INTO sect_shop_purchase_operations(operation_id,payload,quantity,cost,contribution,materials,purchased) VALUES(?,?,?,?,?,?,?)",(operation_id,payload,quantity,cost,contribution,materials,purchased));return {"status":"applied","quantity":quantity,"cost":cost,"contribution":contribution,"materials":materials,"purchased":purchased}
    def donate(self, operation_id, user_id, sect_id, stone, materials):
        operation_id,user_id=str(operation_id).strip(),str(user_id);sect_id,stone,materials=int(sect_id),int(stone),int(materials)
        if stone<=0:return {"status":"invalid_amount","user_id":user_id,"sect_id":sect_id,"stone":stone,"materials":materials}
        if materials<0:return {"status":"invalid_materials","user_id":user_id,"sect_id":sect_id,"stone":stone,"materials":materials}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old=uow.query_one("SELECT stone,materials FROM sect_donation_operations WHERE operation_id=?",(operation_id,))
            if old:return {"status":"duplicate","user_id":user_id,"sect_id":sect_id,"stone":old["stone"],"materials":old["materials"]}
            user=uow.query_one("SELECT sect_id,stone FROM user_xiuxian WHERE user_id=?",(user_id,));sect=uow.query_one("SELECT sect_id FROM sects WHERE sect_id=?",(sect_id,))
            if user is None:return {"status":"user_missing","user_id":user_id,"sect_id":sect_id,"stone":stone,"materials":materials}
            if user["sect_id"] is None or int(user["sect_id"])!=sect_id:return {"status":"sect_changed","user_id":user_id,"sect_id":sect_id,"stone":stone,"materials":materials}
            if int(user["stone"] or 0)<stone:return {"status":"stone_insufficient","user_id":user_id,"sect_id":sect_id,"stone":stone,"materials":materials}
            if sect is None:return {"status":"sect_missing","user_id":user_id,"sect_id":sect_id,"stone":stone,"materials":materials}
            updated=uow.execute("UPDATE user_xiuxian SET stone=stone-?,sect_contribution=COALESCE(sect_contribution,0)+? WHERE user_id=? AND sect_id=? AND stone>=?",(stone,stone,user_id,sect_id,stone));sect_updated=uow.execute("UPDATE sects SET sect_used_stone=COALESCE(sect_used_stone,0)+?,sect_scale=COALESCE(sect_scale,0)+?,sect_materials=COALESCE(sect_materials,0)+? WHERE sect_id=?",(stone,stone,materials,sect_id))
            if updated.rowcount!=1:return {"status":"user_changed","user_id":user_id,"sect_id":sect_id,"stone":stone,"materials":materials}
            if sect_updated.rowcount!=1:return {"status":"sect_changed","user_id":user_id,"sect_id":sect_id,"stone":stone,"materials":materials}
            uow.execute("INSERT INTO sect_donation_operations(operation_id,user_id,sect_id,stone,materials) VALUES(?,?,?,?,?)",(operation_id,user_id,sect_id,stone,materials));return {"status":"donated","user_id":user_id,"sect_id":sect_id,"stone":stone,"materials":materials}
    def change_position(self, operation_id, actor_id, target_id, requested_position, position_limits, *, manager_max_position):
        operation_id, actor_id, target_id = str(operation_id).strip(), str(actor_id), str(target_id);requested_position=int(requested_position);manager_max_position=int(manager_max_position);limits={int(k):max(0,int(v)) for k,v in position_limits.items()}
        if requested_position not in limits:return {"status":"invalid_position","actor_id":actor_id,"target_id":target_id}
        if actor_id==target_id:return {"status":"self_target","actor_id":actor_id,"target_id":target_id}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old=uow.query_one("SELECT sect_id,actor_name,target_name,old_position,new_position FROM sect_position_change_operations WHERE operation_id=?",(operation_id,))
            if old:return {"status":"duplicate","actor_id":actor_id,"target_id":target_id,"sect_id":old["sect_id"],"actor_name":str(old["actor_name"] or ""),"target_name":str(old["target_name"] or ""),"old_position":old["old_position"],"new_position":old["new_position"]}
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
            changed=uow.execute("UPDATE user_xiuxian SET sect_position=? WHERE user_id=? AND sect_id=? AND sect_position=?",(requested_position,target_id,actor["sect_id"],current))
            if changed.rowcount!=1:return {"status":"state_changed","actor_id":actor_id,"target_id":target_id}
            uow.execute("INSERT INTO sect_position_change_operations(operation_id,actor_id,target_id,sect_id,actor_name,target_name,old_position,new_position) VALUES(?,?,?,?,?,?,?,?)",(operation_id,actor_id,target_id,actor["sect_id"],str(actor["user_name"] or ""),str(target["user_name"] or ""),current,requested_position))
            return {"status":"changed","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"],"actor_name":str(actor["user_name"] or ""),"target_name":str(target["user_name"] or ""),"old_position":current,"new_position":requested_position}
    def kick(self, operation_id, actor_id, target_id, *, manager_max_position):
        operation_id, actor_id, target_id = str(operation_id).strip(), str(actor_id), str(target_id)
        manager_max_position = int(manager_max_position)
        if actor_id == target_id: return {"status": "self_target", "actor_id": actor_id, "target_id": target_id}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old = uow.query_one("SELECT sect_id,sect_name,actor_name,target_name,actor_position,target_position FROM sect_member_removal_operations WHERE operation_id=?", (operation_id,))
            if old: return {"status": "duplicate", "actor_id": actor_id, "target_id": target_id, "sect_id": old["sect_id"], "sect_name": str(old["sect_name"] or ""), "actor_name": str(old["actor_name"] or ""), "target_name": str(old["target_name"] or ""), "actor_position": old["actor_position"], "target_position": old["target_position"]}
            actor=uow.query_one("SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?",(actor_id,));target=uow.query_one("SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?",(target_id,))
            if actor is None:return {"status":"actor_not_found","actor_id":actor_id,"target_id":target_id}
            if actor["sect_id"] is None:return {"status":"actor_not_in_sect","actor_id":actor_id,"target_id":target_id}
            if target is None:return {"status":"target_not_found","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if int(actor["sect_position"] or 0)>manager_max_position:return {"status":"insufficient_rank","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if target["sect_id"]!=actor["sect_id"]:return {"status":"different_sect","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            if int(target["sect_position"] or 0)<=int(actor["sect_position"] or 0):return {"status":"target_not_lower","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            sect=uow.query_one("SELECT sect_name FROM sects WHERE sect_id=?",(actor["sect_id"],));
            if sect is None:return {"status":"sect_not_found","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"]}
            changed=uow.execute("UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL,sect_contribution=0 WHERE user_id=? AND sect_id=? AND sect_position=?",(target_id,target["sect_id"],target["sect_position"]))
            if changed.rowcount!=1:return {"status":"state_changed","actor_id":actor_id,"target_id":target_id}
            uow.execute("INSERT INTO sect_member_removal_operations(operation_id,operation_type,actor_id,target_id,sect_id,sect_name,actor_name,target_name,actor_position,target_position) VALUES(?,?,?,?,?,?,?,?,?,?)",(operation_id,"kick",actor_id,target_id,actor["sect_id"],str(sect["sect_name"] or ""),str(actor["user_name"] or ""),str(target["user_name"] or ""),actor["sect_position"],target["sect_position"]))
            return {"status":"kicked","actor_id":actor_id,"target_id":target_id,"sect_id":actor["sect_id"],"sect_name":str(sect["sect_name"] or ""),"actor_name":str(actor["user_name"] or ""),"target_name":str(target["user_name"] or ""),"actor_position":actor["sect_position"],"target_position":target["sect_position"]}
    def leave(self, operation_id, user_id, *, owner_position=0):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old = uow.query_one("SELECT sect_id,sect_name,actor_name,target_name,actor_position,target_position FROM sect_member_removal_operations WHERE operation_id=?", (operation_id,))
            if old:
                return {"status": "duplicate", "user_id": user_id, "sect_id": old["sect_id"], "sect_name": str(old["sect_name"] or ""), "actor_name": str(old["actor_name"] or ""), "target_name": str(old["target_name"] or ""), "actor_position": old["actor_position"], "target_position": old["target_position"]}
            actor = uow.query_one("SELECT sect_id,sect_position,user_name FROM user_xiuxian WHERE user_id=?", (user_id,))
            if actor is None: return {"status": "user_not_found", "user_id": user_id, "target_id": user_id}
            if actor["sect_id"] is None: return {"status": "not_in_sect", "user_id": user_id, "target_id": user_id}
            if int(actor["sect_position"] or 0) == int(owner_position): return {"status": "owner_cannot_leave", "user_id": user_id, "target_id": user_id, "sect_id": actor["sect_id"], "actor_name": str(actor["user_name"] or ""), "target_name": str(actor["user_name"] or ""), "actor_position": actor["sect_position"], "target_position": actor["sect_position"]}
            sect = uow.query_one("SELECT sect_name FROM sects WHERE sect_id=?", (actor["sect_id"],))
            if sect is None: return {"status": "sect_not_found", "user_id": user_id, "target_id": user_id, "sect_id": actor["sect_id"]}
            changed = uow.execute("UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL,sect_contribution=0 WHERE user_id=? AND sect_id=? AND sect_position=?", (user_id,actor["sect_id"],actor["sect_position"]))
            if changed.rowcount != 1: return {"status": "state_changed", "user_id": user_id, "target_id": user_id}
            uow.execute("INSERT INTO sect_member_removal_operations(operation_id,operation_type,actor_id,target_id,sect_id,sect_name,actor_name,target_name,actor_position,target_position) VALUES(?,?,?,?,?,?,?,?,?,?)", (operation_id,"leave",user_id,user_id,actor["sect_id"],str(sect["sect_name"] or ""),str(actor["user_name"] or ""),str(actor["user_name"] or ""),actor["sect_position"],actor["sect_position"]))
            return {"status": "left", "user_id": user_id, "target_id": user_id, "sect_id": actor["sect_id"], "sect_name": str(sect["sect_name"] or ""), "actor_name": str(actor["user_name"] or ""), "target_name": str(actor["user_name"] or ""), "actor_position": actor["sect_position"], "target_position": actor["sect_position"]}
    @staticmethod
    def _member_limit(scale: int) -> int:
        return min(20 + max(0, int(scale)) // 50_000_000, 100)

    def join(self, operation_id, user_id, sect_id, *, member_position=12):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        sect_id, member_position = int(sect_id), int(member_position)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            old = uow.query_one("SELECT user_id,sect_id,member_count,member_limit FROM sect_member_join_operations WHERE operation_id=?", (operation_id,))
            if old:
                if str(old["user_id"]) != user_id or int(old["sect_id"]) != sect_id:
                    return {"status": "operation_conflict", "user_id": user_id, "sect_id": sect_id}
                return {"status": "duplicate", "user_id": user_id, "sect_id": sect_id, "member_count": int(old["member_count"]), "member_limit": int(old["member_limit"])}
            user = uow.query_one("SELECT sect_id FROM user_xiuxian WHERE user_id=?", (user_id,))
            if user is None: return {"status": "user_missing", "user_id": user_id, "sect_id": sect_id}
            if user["sect_id"] is not None: return {"status": "already_in_sect", "user_id": user_id, "sect_id": sect_id}
            sect = uow.query_one("SELECT sect_name,sect_scale,join_open,closed FROM sects WHERE sect_id=?", (sect_id,))
            if sect is None: return {"status": "sect_missing", "user_id": user_id, "sect_id": sect_id}
            if int(sect["closed"] or 0) == 1: return {"status": "sect_closed", "user_id": user_id, "sect_id": sect_id, "sect_name": str(sect["sect_name"] or "")}
            if int(sect["join_open"] or 0) != 1: return {"status": "join_closed", "user_id": user_id, "sect_id": sect_id, "sect_name": str(sect["sect_name"] or "")}
            limit = self._member_limit(int(sect["sect_scale"] or 0));count = int(uow.query_one("SELECT COUNT(*) AS n FROM user_xiuxian WHERE sect_id=?", (sect_id,))["n"])
            if count >= limit: return {"status": "sect_full", "user_id": user_id, "sect_id": sect_id, "member_count": count, "member_limit": limit}
            changed = uow.execute("UPDATE user_xiuxian SET sect_id=?,sect_position=? WHERE user_id=? AND sect_id IS NULL", (sect_id,member_position,user_id))
            if changed.rowcount != 1: return {"status": "state_changed", "user_id": user_id, "sect_id": sect_id}
            count += 1
            uow.execute("INSERT INTO sect_member_join_operations(operation_id,user_id,sect_id,member_count,member_limit) VALUES(?,?,?,?,?)", (operation_id,user_id,sect_id,count,limit))
            return {"status": "joined", "user_id": user_id, "sect_id": sect_id, "sect_name": str(sect["sect_name"] or ""), "member_count": count, "member_limit": limit}

    def learn_secondary(self, *args: Any, **kwargs: Any) -> Any:
        return super().learn_secondary(*args, **kwargs)

    def claim_elixir(self, *args: Any, **kwargs: Any) -> Any:
        return super().claim_elixir(*args, **kwargs)

    def rename(self, operation_id, actor_id, sect_id, new_name, cost, card_id):
        operation_id, actor_id, new_name = str(operation_id).strip(), str(actor_id), str(new_name).strip()
        sect_id, cost, card_id = int(sect_id), int(cost), int(card_id)
        payload = f"{actor_id}|{sect_id}|{new_name}|{cost}|{card_id}"
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
            if int(sect["sect_used_stone"] or 0) < cost:
                return {"status": "stone_insufficient"}
            if card is None or int(card["goods_num"] or 0) <= 0 or int(card["bind_num"] or 0) <= 0:
                return {"status": "card_insufficient"}
            previous_name = str(sect["sect_name"])
            uow.execute("UPDATE sects SET sect_name=?,sect_used_stone=sect_used_stone-? WHERE sect_id=? AND sect_owner=?", (new_name, cost, sect_id, actor_id))
            uow.execute("UPDATE back SET goods_num=goods_num-1,bind_num=bind_num-1 WHERE user_id=? AND goods_id=?", (actor_id, card_id))
            uow.execute("INSERT INTO sect_rename_operations(operation_id,payload,previous_name,new_name) VALUES(?,?,?,?)", (operation_id, payload, previous_name, new_name))
            return {"status": "renamed", "previous_name": previous_name, "new_name": new_name}


__all__ = ["SectRepository", "LegacySectRepository", "SectRenameSqlRepository"]
