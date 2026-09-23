from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class ImpartExploreResult:
    status:str; exp_day:int=0; impart_lv:int=0; impart_num:int=0
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}

class ImpartExploreSqlRepository:
    def __init__(self,game_database:str|Path,impart_database:str|Path,player_database:str|Path): self.game_database,self.impart_database,self.player_database=map(str,(game_database,impart_database,player_database))
    def settle(self,operation_id,user_id,*,event_type,expected_exp_day,expected_impart_lv,expected_impart_num,time_cost,new_impart_lv,legacy_state=None):
        operation_id,user_id,event_type=str(operation_id).strip(),str(user_id),str(event_type); expected_exp_day,expected_impart_lv,expected_impart_num,time_cost,new_impart_lv=map(int,(expected_exp_day,expected_impart_lv,expected_impart_num,time_cost,new_impart_lv)); payload=json.dumps([user_id],separators=(',',':'))
        if not operation_id or event_type not in {'stay','fail','down','up','down_rate','up_rate'} or expected_impart_num<=0 or time_cost<0 or not 0<=new_impart_lv<=30: raise ValueError('invalid impart exploration settlement')
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.impart_database,'impart_data'); uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS impart_explore_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT payload,result_json FROM impart_explore_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return ImpartExploreResult('duplicate',*json.loads(old['result_json'])) if str(old['payload'])==payload else ImpartExploreResult('operation_conflict')
            impart=uow.query_one('SELECT exp_day,impart_lv FROM impart_data.xiuxian_impart WHERE user_id=?',(user_id,)); daily=uow.query_one('SELECT impart_num FROM player_data.impart_pk_daily WHERE user_id=?',(user_id,))
            if impart is None or daily is None or (int(impart['exp_day'] or 0),int(impart['impart_lv'] or 0))!=(expected_exp_day,expected_impart_lv) or int(daily['impart_num'] or 0)!=expected_impart_num: return ImpartExploreResult('state_changed')
            if expected_exp_day<time_cost: return ImpartExploreResult('time_insufficient')
            new_exp_day,new_num=expected_exp_day-time_cost,expected_impart_num-1
            uow.execute('UPDATE impart_data.xiuxian_impart SET exp_day=?,impart_lv=? WHERE user_id=?',(new_exp_day,new_impart_lv,user_id)); uow.execute('UPDATE player_data.impart_pk_daily SET impart_num=? WHERE user_id=?',(new_num,user_id))
            saved=[new_exp_day,new_impart_lv,new_num]; uow.execute('INSERT INTO impart_explore_operations(operation_id,payload,result_json) VALUES(?,?,?)',(operation_id,payload,json.dumps(saved,separators=(',',':')))); return ImpartExploreResult('applied',*saved)
