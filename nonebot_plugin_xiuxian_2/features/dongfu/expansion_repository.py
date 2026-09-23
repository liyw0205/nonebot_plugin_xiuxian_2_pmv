from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class DongfuExpansionResult:
    status:str; user_id:str=''; previous_count:int=0; current_count:int=0; deed_cost:int=0; stone_cost:int=0
    @property
    def succeeded(self): return self.status in {'expanded','duplicate'}

class DongfuExpansionSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path): self.game_database,self.player_database=str(game_database),str(player_database)
    def expand(self,operation_id,user_id,deed_id,base_plot_count,max_plot_count,stone_cost_per_level):
        operation_id,user_id=str(operation_id).strip(),str(user_id); deed_id,base_plot_count,max_plot_count,stone_cost_per_level=map(int,(deed_id,base_plot_count,max_plot_count,stone_cost_per_level))
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS dongfu_expansion_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,previous_count INTEGER NOT NULL,current_count INTEGER NOT NULL,deed_cost INTEGER NOT NULL,stone_cost INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT previous_count,current_count,deed_cost,stone_cost FROM dongfu_expansion_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return DongfuExpansionResult('duplicate',user_id,int(old['previous_count']),int(old['current_count']),int(old['deed_cost']),int(old['stone_cost']))
            user=uow.query_one('SELECT stone FROM user_xiuxian WHERE user_id=?',(user_id,)); row=uow.query_one('SELECT built,plot_count FROM player_data.dongfu_status WHERE user_id=?',(user_id,))
            if user is None: return DongfuExpansionResult('user_missing',user_id)
            if row is None or int(row['built'] or 0)!=1: return DongfuExpansionResult('dongfu_missing',user_id)
            previous=max(base_plot_count,int(row['plot_count'] or 0));
            if previous>=max_plot_count: return DongfuExpansionResult('max_plots',user_id,previous,previous)
            current=previous+1; deed_cost=current-base_plot_count; stone_cost=stone_cost_per_level*deed_cost
            item=uow.query_one('SELECT goods_num FROM back WHERE user_id=? AND goods_id=? AND goods_num>=?',(user_id,deed_id,deed_cost));
            if item is None: return DongfuExpansionResult('deed_insufficient',user_id,previous,previous,deed_cost,stone_cost)
            if int(user['stone'] or 0)<stone_cost: return DongfuExpansionResult('stone_insufficient',user_id,previous,previous,deed_cost,stone_cost)
            if uow.execute('UPDATE back SET goods_num=goods_num-? WHERE user_id=? AND goods_id=? AND goods_num>=?',(deed_cost,user_id,deed_id,deed_cost)).rowcount!=1: return DongfuExpansionResult('deed_changed',user_id,previous,previous,deed_cost,stone_cost)
            if uow.execute('UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND stone>=?',(stone_cost,user_id,stone_cost)).rowcount!=1: return DongfuExpansionResult('stone_changed',user_id,previous,previous,deed_cost,stone_cost)
            if uow.execute('UPDATE player_data.dongfu_status SET plot_count=? WHERE user_id=? AND plot_count=?',(current,user_id,previous)).rowcount!=1: return DongfuExpansionResult('dongfu_changed',user_id,previous,previous,deed_cost,stone_cost)
            uow.execute('INSERT INTO dongfu_expansion_operations(operation_id,user_id,previous_count,current_count,deed_cost,stone_cost) VALUES(?,?,?,?,?,?)',(operation_id,user_id,previous,current,deed_cost,stone_cost)); return DongfuExpansionResult('expanded',user_id,previous,current,deed_cost,stone_cost)
