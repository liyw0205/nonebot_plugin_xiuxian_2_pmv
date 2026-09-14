from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from ...infrastructure.database import DatabaseUnitOfWork


class SectRepository(Protocol):
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
