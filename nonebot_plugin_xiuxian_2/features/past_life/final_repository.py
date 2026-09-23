from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

FIELDS=("state","stage","revision","alloc","accumulated","talent","birth_scenario","total_score","score_breakdown","event_indices","event_snapshots","early_death_rolls","history","last_run_time","total_runs","best_ending","best_score","endings_log","achievement_points")
JSON_FIELDS={"alloc","accumulated","score_breakdown","event_indices","event_snapshots","early_death_rolls","history","endings_log"}
@dataclass(frozen=True)
class PastLifeFinalResult:
    status:str; rewards:dict
    @property
    def succeeded(self): return self.status in {'applied','duplicate'}
class PastLifeFinalSettlementSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path|None=None,max_goods_num:int=1000): self.game_database,self.player_database,self.max_goods_num=str(game_database),str(player_database or game_database),max(1,int(max_goods_num))
    def _canon(self,x): return json.dumps(x,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    def _schema(self,uow):
        uow.execute('CREATE TABLE IF NOT EXISTS player_data.past_life(user_id TEXT PRIMARY KEY)'); cols={str(r['name']) for r in uow.query_all('PRAGMA player_data.table_info(past_life)')}
        for f in FIELDS:
            if f not in cols:uow.execute(f'ALTER TABLE player_data.past_life ADD COLUMN "{f}" {"INTEGER" if f in {"state","stage","revision","total_score","total_runs","best_score","achievement_points"} else "TEXT"} DEFAULT NULL')
        uow.execute('CREATE TABLE IF NOT EXISTS past_life_final_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
        uow.execute('CREATE TABLE IF NOT EXISTS economy_log(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,source TEXT NOT NULL,action TEXT NOT NULL,stone_delta INTEGER NOT NULL DEFAULT 0,exp_delta INTEGER NOT NULL DEFAULT 0,item_delta TEXT NOT NULL DEFAULT "[]",detail TEXT NOT NULL DEFAULT "{}",trace_id TEXT,created_at TEXT NOT NULL)')
    def _encode(self, field, value):
        return self._canon(value) if field in JSON_FIELDS else value

    def _normalize(self, value):
        result = dict(value or {})
        for field in FIELDS:
            raw = result.get(field)
            if field in JSON_FIELDS and isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except (TypeError, ValueError):
                    pass
            if field in {"state", "stage", "revision", "total_score", "total_runs", "best_score", "achievement_points"} and raw is not None:
                try:
                    raw = int(raw)
                except (TypeError, ValueError):
                    pass
            result[field] = raw
        return result
    def settle(self,operation_id,user_id,expected_state,final_state,ending_name,score,exp_reward,stone_reward,achievement_points,item_reward=None,completed_at=None,choice_response=None):
        operation_id,user_id=str(operation_id).strip(),str(user_id).strip(); score,exp_reward,stone_reward,achievement_points=map(int,(score,exp_reward,stone_reward,achievement_points)); completed_at=str(completed_at or '')
        if not operation_id or min(score,exp_reward,stone_reward,achievement_points)<0: raise ValueError('invalid past life final settlement')
        item=None if not item_reward else {'id':int(item_reward['id']),'name':str(item_reward['name']),'type':str(item_reward['type']),'num':max(0,int(item_reward.get('num',1)))}
        payload=self._canon({'user_id':user_id,'expected':expected_state,'ending':str(ending_name),'score':score,'exp':exp_reward,'stone':stone_reward,'points':achievement_points,'item':item,'completed_at':completed_at,'choice_response':choice_response})
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); self._schema(uow); old=uow.query_one('SELECT user_id,payload,result_json FROM past_life_final_operations WHERE operation_id=?',(operation_id,))
            if old is not None:return PastLifeFinalResult('duplicate',json.loads(old['result_json'])) if str(old['user_id'])==user_id and str(old['payload'])==payload else PastLifeFinalResult('operation_conflict',{})
            user=uow.query_one('SELECT exp,stone FROM user_xiuxian WHERE user_id=?',(user_id,)); row=uow.query_one('SELECT * FROM player_data.past_life WHERE user_id=?',(user_id,))
            if user is None or row is None:return PastLifeFinalResult('user_missing',{})
            current=self._normalize(dict(row))
            for f,v in dict(expected_state).items():
                if self._canon(current.get(f))!=self._canon(v):return PastLifeFinalResult('state_changed',{})
            persisted=dict(final_state); persisted.update({'state':0,'last_run_time':completed_at,'total_runs':int(expected_state.get('total_runs',0))+1,'best_score':max(int(expected_state.get('best_score',0)),score),'best_ending':str(ending_name),'achievement_points':int(expected_state.get('achievement_points',0))+achievement_points})
            uow.execute('UPDATE user_xiuxian SET exp=COALESCE(exp,0)+?,stone=COALESCE(stone,0)+? WHERE user_id=?',(exp_reward,stone_reward,user_id))
            if item and item['num']:
                existing=uow.query_one('SELECT goods_num FROM back WHERE user_id=? AND goods_id=?',(user_id,item['id']))
                if existing:uow.execute('UPDATE back SET goods_name=?,goods_type=?,goods_num=MIN(COALESCE(goods_num,0)+?,?),update_time=? WHERE user_id=? AND goods_id=?',(item['name'],item['type'],item['num'],self.max_goods_num,completed_at,user_id,item['id']))
                else:uow.execute('INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time) VALUES(?,?,?,?,?,?,?)',(user_id,item['id'],item['name'],item['type'],min(item['num'],self.max_goods_num),completed_at,completed_at))
            cols={str(r['name']) for r in uow.query_all('PRAGMA player_data.table_info(past_life)')}; fields=[f for f in FIELDS if f in cols]; uow.execute('UPDATE player_data.past_life SET '+','.join(f'"{f}"=?' for f in fields)+' WHERE user_id=?',(*[self._encode(f,persisted.get(f)) for f in fields],user_id))
            rewards={'exp':exp_reward,'stone':stone_reward,'points':achievement_points,'item':item}; uow.execute('INSERT INTO past_life_final_operations(operation_id,user_id,payload,result_json) VALUES(?,?,?,?)',(operation_id,user_id,payload,self._canon(rewards))); uow.execute('INSERT INTO economy_log(user_id,source,action,stone_delta,exp_delta,item_delta,detail,trace_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)',(user_id,'past_life','past_life_final',stone_reward,exp_reward,self._canon([item] if item else []),self._canon({'ending':ending_name,'score':score}),operation_id,completed_at)); return PastLifeFinalResult('applied',rewards)
