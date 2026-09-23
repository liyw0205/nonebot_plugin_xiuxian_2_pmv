from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class ImpartTrainingResult:
    status:str; exp_day:int=0; exp:int=0; exp_used:int=0; exp_count:int=0; exp_load:int=0; exp_gain:int=0
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}

class ImpartTrainingSqlRepository:
    def __init__(self,game_database:str|Path,impart_database:str|Path,player_database:str|Path): self.game_database,self.impart_database,self.player_database=map(str,(game_database,impart_database,player_database))
    def settle(self,operation_id,user_id,*,expected_exp,expected_exp_day,expected_daily,exp_cost,exp_gain,exp_load_gain,power,legacy_state=None):
        operation_id,user_id=str(operation_id).strip(),str(user_id); expected_exp,expected_exp_day,exp_cost,exp_gain,exp_load_gain,power=map(int,(expected_exp,expected_exp_day,exp_cost,exp_gain,exp_load_gain,power)); expected_daily={k:int(expected_daily[k]) for k in ('exp_used','exp_count','exp_load','exp_gain')}; payload=json.dumps([user_id,exp_cost],separators=(',',':'))
        if not operation_id or exp_cost<=0 or exp_gain<=0 or exp_load_gain<0 or power<0: raise ValueError('invalid impart training settlement')
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.impart_database,'impart_data'); uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS impart_training_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT payload,result_json FROM impart_training_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return ImpartTrainingResult('duplicate',*json.loads(old['result_json'])) if str(old['payload'])==payload else ImpartTrainingResult('operation_conflict')
            user=uow.query_one('SELECT exp FROM user_xiuxian WHERE user_id=?',(user_id,)); impart=uow.query_one('SELECT exp_day FROM impart_data.xiuxian_impart WHERE user_id=?',(user_id,)); daily=uow.query_one('SELECT exp_used,exp_count,exp_load,exp_gain FROM player_data.impart_pk_daily WHERE user_id=?',(user_id,))
            if user is None or impart is None or daily is None or int(user['exp'] or 0)!=expected_exp or int(impart['exp_day'] or 0)!=expected_exp_day or any(int(daily[k] or 0)!=v for k,v in expected_daily.items()): return ImpartTrainingResult('state_changed')
            if expected_exp_day<exp_cost: return ImpartTrainingResult('time_insufficient')
            new_exp_day,new_exp,new_used,new_count,new_load,new_gain=expected_exp_day-exp_cost,expected_exp+exp_gain,expected_daily['exp_used']+exp_cost,expected_daily['exp_count']+1,min(100,expected_daily['exp_load']+exp_load_gain),expected_daily['exp_gain']+exp_gain
            uow.execute('UPDATE impart_data.xiuxian_impart SET exp_day=? WHERE user_id=?',(new_exp_day,user_id))
            if uow.execute('UPDATE user_xiuxian SET exp=?,power=? WHERE user_id=? AND exp=?',(new_exp,power,user_id,expected_exp)).rowcount!=1: return ImpartTrainingResult('state_changed')
            if uow.execute('UPDATE player_data.impart_pk_daily SET exp_used=?,exp_count=?,exp_load=?,exp_gain=? WHERE user_id=?',(new_used,new_count,new_load,new_gain,user_id)).rowcount!=1: return ImpartTrainingResult('state_changed')
            saved=[new_exp_day,new_exp,new_used,new_count,new_load,new_gain]; uow.execute('INSERT INTO impart_training_operations(operation_id,payload,result_json) VALUES(?,?,?)',(operation_id,payload,json.dumps(saved,separators=(',',':')))); return ImpartTrainingResult('applied',*saved)
