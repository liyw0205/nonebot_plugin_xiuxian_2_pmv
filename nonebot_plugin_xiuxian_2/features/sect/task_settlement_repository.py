from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from ...infrastructure.database import DatabaseUnitOfWork

class SectTaskSettlementSqlRepository:
    def __init__(self, database: str | Path, *, clock=None) -> None: self.database=str(database); self.clock=clock
    def settle(self, operation_id, user_id, sect_id, period, cost_type, cost, exp_reward, sect_reward, expected_task_key=None, expected_task_data=None) -> dict[str, Any]:
        operation_id,user_id,period,cost_type=str(operation_id).strip(),str(user_id),str(period).strip(),str(cost_type).strip().lower(); sect_id,cost,exp_reward,sect_reward=int(sect_id),int(cost),int(exp_reward),int(sect_reward); materials_reward=sect_reward*10
        if not operation_id or not period: raise ValueError('operation_id and period are required')
        base={'user_id':user_id,'sect_id':sect_id,'period':period,'cost_type':cost_type,'cost':cost,'exp_reward':exp_reward,'sect_reward':sect_reward,'materials_reward':materials_reward}
        if cost_type not in {'hp','stone'}: return {'status':'invalid_cost_type',**base}
        if min(cost,exp_reward,sect_reward)<0:return {'status':'invalid_amount',**base}
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute('CREATE TABLE IF NOT EXISTS sect_task_settlement_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,sect_id INTEGER NOT NULL,period TEXT NOT NULL,cost_type TEXT NOT NULL,cost INTEGER NOT NULL,exp_reward INTEGER NOT NULL,sect_reward INTEGER NOT NULL,materials_reward INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
            old=uow.query_one('SELECT cost_type,cost,exp_reward,sect_reward,materials_reward FROM sect_task_settlement_operations WHERE operation_id=?',(operation_id,))
            if old:return {'status':'duplicate',**base,'cost_type':str(old['cost_type']),'cost':int(old['cost']),'exp_reward':int(old['exp_reward']),'sect_reward':int(old['sect_reward']),'materials_reward':int(old['materials_reward'])}
            task=uow.query_one('SELECT sect_id,status,task_key,task_data FROM sect_task_state WHERE user_id=? AND period=?',(user_id,period))
            if task is None or str(task['status'])!='accepted':return {'status':'task_missing',**base}
            if int(task['sect_id'])!=sect_id:return {'status':'task_sect_changed',**base}
            if expected_task_key is not None and str(task['task_key'])!=str(expected_task_key):return {'status':'task_snapshot_changed',**base}
            if expected_task_data is not None and json.loads(task['task_data'] or '{}') != dict(expected_task_data):return {'status':'task_snapshot_changed',**base}
            user=uow.query_one('SELECT sect_id,stone,hp FROM user_xiuxian WHERE user_id=?',(user_id,));
            if user is None:return {'status':'user_missing',**base}
            if user['sect_id'] is None or int(user['sect_id'])!=sect_id:return {'status':'sect_changed',**base}
            balance=int(user['hp'] if cost_type=='hp' else user['stone'] or 0)
            if balance<cost:return {'status':f'{cost_type}_insufficient',**base}
            if uow.query_one('SELECT sect_id FROM sects WHERE sect_id=?',(sect_id,)) is None:return {'status':'sect_missing',**base}
            column='hp' if cost_type=='hp' else 'stone'; now=(self.clock.now() if self.clock else datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
            user_update=uow.execute(f'UPDATE user_xiuxian SET {column}={column}-?,exp=COALESCE(exp,0)+?,sect_task=COALESCE(sect_task,0)+1,sect_contribution=COALESCE(sect_contribution,0)+? WHERE user_id=? AND sect_id=? AND {column}>=?',(cost,exp_reward,sect_reward,user_id,sect_id,cost))
            sect_update=uow.execute('UPDATE sects SET sect_used_stone=COALESCE(sect_used_stone,0)+?,sect_scale=COALESCE(sect_scale,0)+?,sect_materials=COALESCE(sect_materials,0)+? WHERE sect_id=?',(sect_reward,sect_reward,materials_reward,sect_id))
            task_update=uow.execute('UPDATE sect_task_state SET status="completed",progress=target,updated_at=?,completed_at=? WHERE user_id=? AND period=? AND sect_id=? AND status="accepted"',(now,now,user_id,period,sect_id))
            if user_update.rowcount!=1 or sect_update.rowcount!=1 or task_update.rowcount!=1: raise RuntimeError('task settlement state changed concurrently')
            uow.execute('INSERT INTO sect_task_settlement_operations(operation_id,user_id,sect_id,period,cost_type,cost,exp_reward,sect_reward,materials_reward) VALUES(?,?,?,?,?,?,?,?,?)',(operation_id,user_id,sect_id,period,cost_type,cost,exp_reward,sect_reward,materials_reward))
            return {'status':'settled',**base}
