from __future__ import annotations
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork

class SectScheduledMaterialSqlRepository:
    def __init__(self, database: str | Path) -> None: self.database=str(database)
    def grant(self, grant_key: str, sect_id: int, multiplier: int) -> dict[str, Any]:
        grant_key,sect_id,multiplier=str(grant_key).strip(),int(sect_id),max(int(multiplier),0)
        if not grant_key: raise ValueError('grant_key must not be empty')
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute('CREATE TABLE IF NOT EXISTS sect_scheduled_material_grants(grant_key TEXT NOT NULL,sect_id INTEGER NOT NULL,materials INTEGER NOT NULL,combat_power INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(grant_key,sect_id))')
            old=uow.query_one('SELECT materials,combat_power FROM sect_scheduled_material_grants WHERE grant_key=? AND sect_id=?',(grant_key,sect_id))
            if old:return {'status':'duplicate','grant_key':grant_key,'sect_id':sect_id,'materials':int(old['materials']),'combat_power':int(old['combat_power'])}
            sect=uow.query_one('SELECT sect_scale,sect_owner FROM sects WHERE sect_id=?',(sect_id,))
            if sect is None:return {'status':'sect_missing','grant_key':grant_key,'sect_id':sect_id,'materials':0,'combat_power':0}
            if sect['sect_owner'] is None:return {'status':'sect_inactive','grant_key':grant_key,'sect_id':sect_id,'materials':0,'combat_power':0}
            materials=max(int(sect['sect_scale'] or 0),0)*multiplier
            power=uow.query_one('SELECT COALESCE(SUM(power),0) AS total FROM user_xiuxian WHERE sect_id=?',(sect_id,)); combat_power=int(power['total'] or 0)
            uow.execute('UPDATE sects SET sect_materials=CAST(COALESCE(sect_materials,0) AS REAL)+?,combat_power=? WHERE sect_id=?',(materials,combat_power,sect_id))
            uow.execute('INSERT INTO sect_scheduled_material_grants(grant_key,sect_id,materials,combat_power) VALUES(?,?,?,?)',(grant_key,sect_id,materials,combat_power))
            return {'status':'granted','grant_key':grant_key,'sect_id':sect_id,'materials':materials,'combat_power':combat_power}
