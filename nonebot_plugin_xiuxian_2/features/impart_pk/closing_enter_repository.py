from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class ImpartClosingEnterResult:
    status:str; started_at:str=''; entry_count:int=0
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}

class ImpartClosingEnterSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path): self.game_database,self.player_database=str(game_database),str(player_database)
    def enter(self,operation_id,user_id,started_at):
        operation_id,user_id,started_at=str(operation_id).strip(),str(user_id),str(started_at).strip(); payload=json.dumps([user_id],separators=(',',':'))
        if not operation_id or not user_id or not started_at: raise ValueError('operation, user and start time are required')
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS impart_closing_enter_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT payload,result_json FROM impart_closing_enter_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return ImpartClosingEnterResult('duplicate',*json.loads(old['result_json'])) if str(old['payload'])==payload else ImpartClosingEnterResult('operation_conflict')
            user=uow.query_one('SELECT root_type FROM user_xiuxian WHERE user_id=?',(user_id,)); cd=uow.query_one('SELECT COALESCE(type,0) AS type FROM user_cd WHERE user_id=?',(user_id,))
            if user is None or cd is None: return ImpartClosingEnterResult('user_missing')
            if str(user['root_type'] or '')=='伪灵根': return ImpartClosingEnterResult('ineligible')
            if int(cd['type'] or 0)!=0: return ImpartClosingEnterResult('busy')
            if uow.execute('UPDATE user_cd SET type=4,create_time=?,scheduled_time=NULL WHERE user_id=? AND COALESCE(type,0)=0',(started_at,user_id)).rowcount!=1: return ImpartClosingEnterResult('state_changed')
            stat=uow.query_one('SELECT COALESCE(虚神界闭关次数,0) AS count FROM player_data.statistics WHERE user_id=?',(user_id,)); entry_count=(0 if stat is None else int(stat['count']))+1
            uow.execute('UPDATE player_data.statistics SET 虚神界闭关次数=? WHERE user_id=?',(entry_count,user_id)); saved=[started_at,entry_count]; uow.execute('INSERT INTO impart_closing_enter_operations(operation_id,payload,result_json) VALUES(?,?,?)',(operation_id,payload,json.dumps(saved,separators=(',',':')))); return ImpartClosingEnterResult('applied',*saved)
