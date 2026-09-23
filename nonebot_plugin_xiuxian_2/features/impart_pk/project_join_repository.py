from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class ImpartProjectJoinResult:
    status:str; pk_num:int=0; member_count:int=0
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}

class ImpartProjectJoinSqlRepository:
    def __init__(self,player_database:str|Path,capacity:int=40): self.player_database=str(player_database); self.capacity=int(capacity)
    def join(self,operation_id,user_id,*,legacy_pk_num=7,legacy_members=None):
        operation_id,user_id=str(operation_id).strip(),str(user_id); legacy_pk_num=int(legacy_pk_num); payload=json.dumps([user_id],separators=(',',':'))
        if not operation_id or not user_id or legacy_pk_num<0 or self.capacity<=0: raise ValueError('invalid impart project join')
        with DatabaseUnitOfWork(self.player_database,immediate=True) as uow:
            uow.execute('CREATE TABLE IF NOT EXISTS impart_pk_state(user_id TEXT PRIMARY KEY,pk_num INTEGER NOT NULL DEFAULT 7,win_num INTEGER NOT NULL DEFAULT 0)'); uow.execute('CREATE TABLE IF NOT EXISTS impart_project_members(user_id TEXT PRIMARY KEY,joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); uow.execute('CREATE TABLE IF NOT EXISTS impart_project_meta(meta_key TEXT PRIMARY KEY,meta_value TEXT NOT NULL)'); uow.execute('CREATE TABLE IF NOT EXISTS impart_project_join_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)');
            if uow.query_one("SELECT 1 FROM impart_project_meta WHERE meta_key='legacy_members_imported'") is None:
                members=sorted({str(x) for x in (legacy_members or ()) if str(x)}); uow.executemany('INSERT OR IGNORE INTO impart_project_members(user_id) VALUES(?)',((x,) for x in members)); uow.execute("INSERT INTO impart_project_meta(meta_key,meta_value) VALUES('legacy_members_imported',?)",(str(len(members)),))
            old=uow.query_one('SELECT payload,result_json FROM impart_project_join_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return ImpartProjectJoinResult('duplicate',*json.loads(old['result_json'])) if str(old['payload'])==payload else ImpartProjectJoinResult('operation_conflict')
            state=uow.query_one('SELECT pk_num FROM impart_pk_state WHERE user_id=?',(user_id,)); pk_num=legacy_pk_num if state is None else int(state['pk_num'] or 0)
            if state is None: uow.execute('INSERT INTO impart_pk_state(user_id,pk_num,win_num) VALUES(?,?,0)',(user_id,pk_num))
            member=uow.query_one('SELECT 1 FROM impart_project_members WHERE user_id=?',(user_id,)); count=int(uow.query_one('SELECT COUNT(*) AS count FROM impart_project_members')['count'])
            if member is not None: return ImpartProjectJoinResult('already_joined',pk_num,count)
            if count>=self.capacity: return ImpartProjectJoinResult('capacity_full',pk_num,count)
            uow.execute('CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY,"虚神界投影次数" INTEGER DEFAULT 0)'); uow.execute('INSERT INTO impart_project_members(user_id) VALUES(?)',(user_id,)); uow.execute('INSERT INTO statistics(user_id,"虚神界投影次数") VALUES(?,1) ON CONFLICT(user_id) DO UPDATE SET "虚神界投影次数"=COALESCE(statistics."虚神界投影次数",0)+1',(user_id,)); count+=1; saved=[pk_num,count]; uow.execute('INSERT INTO impart_project_join_operations(operation_id,payload,result_json) VALUES(?,?,?)',(operation_id,payload,json.dumps(saved,separators=(',',':')))); return ImpartProjectJoinResult('applied',*saved)
