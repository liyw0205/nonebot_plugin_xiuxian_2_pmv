from __future__ import annotations
from ...core.numeric import as_int_like
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork
from ...core.errors import DomainError
from .schema import ELIXIR_ROOM_COLUMNS, SCHEMA_MISSING, table_ready

class SectElixirRoomSqlRepository:
    def __init__(self, database: str | Path) -> None: self.database=str(database)
    def upgrade(self, operation_id, actor_id, sect_id, expected_level, next_level, stone_cost, scale_cost, *, owner_position=0) -> dict[str, Any]:
        operation_id,actor_id=str(operation_id).strip(),str(actor_id); sect_id,expected_level,next_level,stone_cost,scale_cost=int(sect_id),int(expected_level),int(next_level),max(int(stone_cost),0),max(int(scale_cost),0)
        if not operation_id: raise ValueError('operation_id must not be empty')
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not table_ready(uow, "sect_elixir_room_operations", ELIXIR_ROOM_COLUMNS):
                return {"status": SCHEMA_MISSING}
            old=uow.query_one('SELECT from_level,to_level,stone_cost,scale_cost FROM sect_elixir_room_operations WHERE operation_id=?',(operation_id,))
            if old:return {'status':'duplicate','actor_id':actor_id,'sect_id':sect_id,'from_level':int(old['from_level']),'to_level':int(old['to_level']),'stone_cost':int(old['stone_cost']),'scale_cost':int(old['scale_cost'])}
            user=uow.query_one('SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id=?',(actor_id,)); sect=uow.query_one('SELECT sect_owner,COALESCE(elixir_room_level,0) AS level,COALESCE(sect_used_stone,0) AS stones,COALESCE(sect_scale,0) AS scale FROM sects WHERE sect_id=?',(sect_id,))
            if user is None:return {'status':'actor_missing','actor_id':actor_id,'sect_id':sect_id}
            if sect is None:return {'status':'sect_missing','actor_id':actor_id,'sect_id':sect_id}
            base={'actor_id':actor_id,'sect_id':sect_id,'from_level':int(sect['level']),'to_level':next_level,'stone_cost':stone_cost,'scale_cost':scale_cost}
            if int(user['sect_id'] or 0)!=sect_id:return {'status':'membership_changed',**base}
            if str(sect['sect_owner'])!=actor_id or int(user['sect_position'] or 0)!=int(owner_position):return {'status':'not_owner','reason':'sect_owner_changed' if str(sect['sect_owner'])!=actor_id else 'sect_position_changed',**base}
            if int(sect['level'])!=expected_level:return {'status':'level_changed','reason':'sect_elixir_level_changed',**base}
            if as_int_like(sect['stones'])<stone_cost:return {'status':'stone_insufficient','reason':'sect_stone_insufficient',**base}
            if as_int_like(sect['scale'])<scale_cost:return {'status':'scale_insufficient',**base}
            changed=uow.execute('UPDATE sects SET elixir_room_level=?,sect_used_stone=xiuxian_int_store(xiuxian_int_sub(sect_used_stone,?,\'sect_used_stone\',\'actor\')),sect_scale=sect_scale-? WHERE sect_id=? AND sect_owner=? AND COALESCE(elixir_room_level,0)=? AND xiuxian_int_cmp(COALESCE(sect_used_stone,0),?,1,\'sect_used_stone\',\'actor\')>=0 AND xiuxian_int_cmp(COALESCE(sect_scale,0),?)>=0',(next_level,stone_cost,scale_cost,sect_id,actor_id,expected_level,stone_cost,scale_cost))
            if changed.rowcount!=1: raise DomainError('sect_elixir_write_rejected','elixir room write count mismatch')
            uow.execute('INSERT INTO sect_elixir_room_operations(operation_id,actor_id,sect_id,from_level,to_level,stone_cost,scale_cost) VALUES(?,?,?,?,?,?,?)',(operation_id,actor_id,sect_id,expected_level,next_level,stone_cost,scale_cost))
            return {'status':'upgraded',**base}
