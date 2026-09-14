from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from ...infrastructure.database import DatabaseUnitOfWork


class SectRepository(Protocol):
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

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return super().purchase(*args, **kwargs)

    def learn_main(self, *args: Any, **kwargs: Any) -> Any:
        return super().learn_main(*args, **kwargs)

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
