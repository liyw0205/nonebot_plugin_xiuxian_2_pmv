from __future__ import annotations
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork

class SectPracticeSqlRepository:
    def __init__(self, database: str | Path) -> None: self.database=str(database)
    def upgrade(self, operation_id, user_id, sect_id, practice_type, expected_level, next_level, stone_cost, materials_cost) -> dict[str, Any]:
        operation_id,user_id,practice_type=str(operation_id).strip(),str(user_id),str(practice_type); sect_id,expected_level,next_level,stone_cost,materials_cost=int(sect_id),int(expected_level),int(next_level),max(int(stone_cost),0),max(int(materials_cost),0)
        if not operation_id: raise ValueError('operation_id must not be empty')
        columns={'attack':'attack_practice','health':'health_practice','mana':'mana_practice'}
        column=columns.get(practice_type)
        if column is None:return {'status':'invalid_practice_type','user_id':user_id,'sect_id':sect_id,'practice_type':practice_type}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute('CREATE TABLE IF NOT EXISTS sect_practice_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,sect_id INTEGER NOT NULL,practice_type TEXT NOT NULL,from_level INTEGER NOT NULL,to_level INTEGER NOT NULL,stone_cost INTEGER NOT NULL,materials_cost INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
            old=uow.query_one('SELECT practice_type,from_level,to_level,stone_cost,materials_cost FROM sect_practice_operations WHERE operation_id=?',(operation_id,))
            if old:return {'status':'duplicate','user_id':user_id,'sect_id':sect_id,'practice_type':str(old['practice_type']),'from_level':int(old['from_level']),'to_level':int(old['to_level']),'stone_cost':int(old['stone_cost']),'materials_cost':int(old['materials_cost'])}
            user=uow.query_one(f'SELECT sect_id,COALESCE({column},0) AS level FROM user_xiuxian WHERE user_id=?',(user_id,)); sect=uow.query_one('SELECT sect_owner,COALESCE(sect_materials,0) AS materials,COALESCE(sect_used_stone,0) AS stones FROM sects WHERE sect_id=?',(sect_id,))
            base={'user_id':user_id,'sect_id':sect_id,'practice_type':practice_type,'from_level':expected_level,'to_level':next_level,'stone_cost':stone_cost,'materials_cost':materials_cost}
            if user is None:return {'status':'user_missing',**base}
            if sect is None:return {'status':'sect_missing',**base}
            if int(user['sect_id'] or 0)!=sect_id:return {'status':'membership_changed',**base}
            if int(user['level'])!=expected_level:return {'status':'level_changed',**base}
            if int(sect['stones'])<stone_cost:return {'status':'stone_insufficient',**base}
            if int(sect['materials'])<materials_cost:return {'status':'materials_insufficient',**base}
            changed=uow.execute(f'UPDATE user_xiuxian SET {column}=? WHERE user_id=? AND sect_id=? AND COALESCE({column},0)=?',(next_level,user_id,sect_id,expected_level)); assets=uow.execute('UPDATE sects SET sect_used_stone=sect_used_stone-?,sect_materials=sect_materials-? WHERE sect_id=? AND COALESCE(sect_used_stone,0)>=? AND COALESCE(sect_materials,0)>=?',(stone_cost,materials_cost,sect_id,stone_cost,materials_cost))
            if changed.rowcount!=1 or assets.rowcount!=1: raise RuntimeError('practice state changed concurrently')
            uow.execute('INSERT INTO sect_practice_operations(operation_id,user_id,sect_id,practice_type,from_level,to_level,stone_cost,materials_cost) VALUES(?,?,?,?,?,?,?,?)',(operation_id,user_id,sect_id,practice_type,expected_level,next_level,stone_cost,materials_cost))
            return {'status':'upgraded',**base}
