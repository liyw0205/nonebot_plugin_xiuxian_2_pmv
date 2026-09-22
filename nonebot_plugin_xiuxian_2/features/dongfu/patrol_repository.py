from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class DongfuPatrolResult:
    status:str; patrol_count:int=0; patrol_guard:int=0
    @property
    def succeeded(self): return self.status in {'patrolled','duplicate'}

class DongfuPatrolSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path): self.game_database,self.player_database=str(game_database),str(player_database)
    def patrol(self,operation_id,user_id,day,stamina_cost,daily_limit,stone_gain,reward=None,max_goods_num=999999999):
        operation_id,user_id,day=str(operation_id).strip(),str(user_id),str(day); stamina_cost,daily_limit,stone_gain,max_goods_num=map(int,(stamina_cost,daily_limit,stone_gain,max_goods_num)); payload=json.dumps([user_id,day],separators=(',',':'))
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS dongfu_patrol_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,patrol_count INTEGER NOT NULL,patrol_guard INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT payload,patrol_count,patrol_guard FROM dongfu_patrol_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return DongfuPatrolResult('duplicate',int(old['patrol_count']),int(old['patrol_guard'])) if str(old['payload'])==payload else DongfuPatrolResult('state_changed')
            user=uow.query_one('SELECT user_stamina FROM user_xiuxian WHERE user_id=?',(user_id,));
            if user is None: return DongfuPatrolResult('user_missing')
            if int(user['user_stamina'] or 0)<stamina_cost: return DongfuPatrolResult('stamina_insufficient')
            row=uow.query_one('SELECT built,patrol_date,patrol_count,patrol_guard FROM player_data.dongfu_status WHERE user_id=?',(user_id,));
            if row is None or int(row['built'] or 0)!=1: return DongfuPatrolResult('dongfu_missing')
            count,guard=(int(row['patrol_count'] or 0),int(row['patrol_guard'] or 0)) if str(row['patrol_date'] or '')==day else (0,0)
            if count>=daily_limit: return DongfuPatrolResult('daily_limit',count,guard)
            if reward:
                item=uow.query_one('SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?',(user_id,int(reward[0]))); 
                if (0 if item is None else int(item['goods_num']))+int(reward[2])>max_goods_num: return DongfuPatrolResult('inventory_full',count,guard)
            count,guard=count+1,min(3,guard+1)
            if uow.execute('UPDATE user_xiuxian SET user_stamina=user_stamina-?,stone=COALESCE(stone,0)+? WHERE user_id=? AND user_stamina>=?',(stamina_cost,stone_gain,user_id,stamina_cost)).rowcount!=1: return DongfuPatrolResult('state_changed')
            if uow.execute('UPDATE player_data.dongfu_status SET patrol_date=?,patrol_count=?,patrol_guard=? WHERE user_id=?',(day,count,guard,user_id)).rowcount!=1: return DongfuPatrolResult('state_changed')
            if reward: uow.execute('INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num',(user_id,int(reward[0]),str(reward[1]),'特殊物品',int(reward[2]),int(reward[2])))
            uow.execute('INSERT INTO dongfu_patrol_operations(operation_id,payload,patrol_count,patrol_guard) VALUES(?,?,?,?)',(operation_id,payload,count,guard)); return DongfuPatrolResult('patrolled',count,guard)
