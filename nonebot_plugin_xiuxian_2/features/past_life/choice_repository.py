from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

FIELDS=("state","stage","revision","alloc","accumulated","talent","birth_scenario","total_score","score_breakdown","event_indices","event_snapshots","early_death_rolls","history","last_run_time","total_runs","best_ending","best_score","endings_log","achievement_points")
JSON_FIELDS={"alloc","accumulated","score_breakdown","event_indices","event_snapshots","early_death_rolls","history","endings_log"}
INT_FIELDS={"state","stage","revision","total_score","total_runs","best_score","achievement_points"}
IMMUTABLE=("alloc","talent","birth_scenario","event_indices","event_snapshots","last_run_time","total_runs","best_ending","best_score","endings_log","achievement_points")
DEFAULT={"state":0,"stage":0,"revision":0,"alloc":{},"accumulated":{},"talent":"","birth_scenario":"","total_score":0,"score_breakdown":{},"event_indices":[],"event_snapshots":[],"early_death_rolls":{},"history":[],"last_run_time":None,"total_runs":0,"best_ending":"","best_score":0,"endings_log":[],"achievement_points":0}
@dataclass(frozen=True)
class PastLifeChoiceResult:
    status:str; response:dict|None=None
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}
class PastLifeChoiceSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path|None=None): self.game_database,self.player_database=str(game_database),str(player_database or game_database)
    def _norm(self,value):
        out={}
        for f in FIELDS:
            x=dict(value or {}).get(f,DEFAULT[f])
            if f in JSON_FIELDS and isinstance(x,str):
                try:x=json.loads(x)
                except (TypeError,ValueError):pass
            if f in INT_FIELDS:
                try:x=int(x)
                except (TypeError,ValueError):pass
            out[f]=x
        return out
    def _canon(self,x): return json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    def _enc(self,f,x): return self._canon(x) if f in JSON_FIELDS else x
    def _schema(self,uow):
        uow.execute('CREATE TABLE IF NOT EXISTS player_data.past_life(user_id TEXT PRIMARY KEY)'); cols={str(r['name']) for r in uow.query_all('PRAGMA player_data.table_info(past_life)')}
        for f in FIELDS:
            if f not in cols:uow.execute(f'ALTER TABLE player_data.past_life ADD COLUMN "{f}" {"INTEGER" if f in INT_FIELDS else "TEXT"} DEFAULT NULL')
        uow.execute('CREATE TABLE IF NOT EXISTS past_life_choice_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,kind TEXT NOT NULL,payload TEXT NOT NULL,response_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
    def _read(self,uow,user_id):
        r=uow.query_one('SELECT * FROM player_data.past_life WHERE user_id=?',(user_id,)); return None if r is None else self._norm(dict(r))
    def _write(self,uow,user_id,state):
        vals=[self._enc(f,state[f]) for f in FIELDS]; uow.execute('UPDATE player_data.past_life SET '+','.join(f'"{f}"=?' for f in FIELDS)+' WHERE user_id=?',(*vals,user_id))
    def advance(self,operation_id,user_id,choice_idx,expected_state,final_state,response):
        operation_id,user_id=str(operation_id).strip(),str(user_id).strip(); choice_idx=int(choice_idx); exp=self._norm(expected_state); final=self._norm(final_state); response=dict(response)
        if not operation_id or not user_id or choice_idx<=0 or not response.get('message') or response.get('is_end') is not False or int(exp['state'])!=2 or int(final['state'])!=2 or int(final['stage'])!=int(exp['stage'])+1 or int(final['revision'])!=int(exp['revision'])+1: raise ValueError('valid non-terminal past life choice is required')
        for f in IMMUTABLE:
            if self._canon(final[f])!=self._canon(exp[f]): raise ValueError(f'past life choice changed immutable field: {f}')
        payload=self._canon({'user_id':user_id,'choice_idx':choice_idx,'expected':exp,'final':final})
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); self._schema(uow); old=uow.query_one('SELECT user_id,response_json FROM past_life_choice_operations WHERE operation_id=?',(operation_id,))
            if old is not None:return PastLifeChoiceResult('duplicate',json.loads(old['response_json'])) if str(old['user_id'])==user_id else PastLifeChoiceResult('operation_conflict')
            if uow.query_one('SELECT 1 FROM user_xiuxian WHERE user_id=?',(user_id,)) is None:return PastLifeChoiceResult('user_missing')
            current=self._read(uow,user_id)
            if current is None:return PastLifeChoiceResult('user_missing')
            if self._canon(current)!=self._canon(exp):return PastLifeChoiceResult('state_changed')
            self._write(uow,user_id,final); uow.execute('INSERT INTO past_life_choice_operations(operation_id,user_id,kind,payload,response_json) VALUES(?,?,?,?,?)',(operation_id,user_id,'advance',payload,self._canon(response))); return PastLifeChoiceResult('applied',response)
