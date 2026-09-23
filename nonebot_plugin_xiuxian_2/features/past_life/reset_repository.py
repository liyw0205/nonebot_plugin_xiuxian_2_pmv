from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

FIELDS=("state","stage","revision","alloc","accumulated","talent","birth_scenario","total_score","score_breakdown","event_indices","event_snapshots","early_death_rolls","history","last_run_time","total_runs","best_ending","best_score","endings_log","achievement_points")
JSON_FIELDS={"alloc","accumulated","score_breakdown","event_indices","event_snapshots","early_death_rolls","history","endings_log"}
DEFAULT={"state":0,"stage":0,"revision":0,"alloc":{},"accumulated":{},"talent":"","birth_scenario":"","total_score":0,"score_breakdown":{},"event_indices":[],"event_snapshots":[] ,"early_death_rolls":{},"history":[],"last_run_time":None,"total_runs":0,"best_ending":"","best_score":0,"endings_log":[],"achievement_points":0}
@dataclass(frozen=True)
class PastLifeResetResult:
    status:str; data:dict
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}
class PastLifeResetSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path|None=None): self.game_database,self.player_database=str(game_database),str(player_database or game_database)
    def reset_one(self,operation_id,user_id,clear_history=False):
        operation_id,user_id=str(operation_id).strip(),str(user_id).strip(); clear_history=bool(clear_history); payload=json.dumps([user_id,clear_history],separators=(',',':'))
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS past_life_reset_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT user_id,payload,result_json FROM past_life_reset_operations WHERE operation_id=?',(operation_id,))
            if old is not None:return PastLifeResetResult('duplicate' if str(old['payload'])==payload else 'operation_conflict',json.loads(old['result_json']) if str(old['payload'])==payload else {})
            row=uow.query_one('SELECT * FROM player_data.past_life WHERE user_id=?',(user_id,));
            if row is None:return PastLifeResetResult('user_missing',{})
            current=dict(row); revision=int(current.get('revision') or 0)+1
            reset={'state':0,'stage':0,'revision':revision,'alloc':{},'accumulated':{},'talent':'','birth_scenario':'','total_score':0,'score_breakdown':{},'event_indices':[],'event_snapshots':[],'early_death_rolls':{},'history':[],'last_run_time':None};
            if not clear_history:
                for field in ('total_runs','best_ending','best_score','endings_log','achievement_points'): reset[field]=current.get(field)
            else:
                reset.update({'total_runs':0,'best_ending':'','best_score':0,'endings_log':[],'achievement_points':0})
            cols=set(current); fields=[]; vals=[]
            for field,value in reset.items():
                if field not in cols: continue
                fields.append(f'"{field}"=?'); vals.append(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':')) if field in JSON_FIELDS else value)
            uow.execute(f'UPDATE player_data.past_life SET {",".join(fields)} WHERE user_id=?',(*vals,user_id)); result={'revision':revision,'clear_history':clear_history}; uow.execute('INSERT INTO past_life_reset_operations(operation_id,user_id,payload,result_json) VALUES(?,?,?,?)',(operation_id,user_id,payload,json.dumps(result,separators=(',',':')))); return PastLifeResetResult('applied',result)
