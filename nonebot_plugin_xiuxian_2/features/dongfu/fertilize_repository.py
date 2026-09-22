from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class DongfuFertilizeResult:
    status:str
    @property
    def succeeded(self): return self.status in {'fertilized','duplicate'}

class DongfuFertilizeSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path): self.game_database,self.player_database=str(game_database),str(player_database)
    @staticmethod
    def _canonical(slots): return json.dumps(slots,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    def fertilize(self,operation_id,user_id,expected_slots,slot_no,item_id,fertilizer_max):
        operation_id,user_id=str(operation_id).strip(),str(user_id); expected_slots=self._canonical(json.loads(expected_slots)); slot_no,item_id,fertilizer_max=map(int,(slot_no,item_id,fertilizer_max)); payload='|'.join(map(str,(user_id,slot_no,item_id)))
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS dongfu_fertilize_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT payload FROM dongfu_fertilize_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return DongfuFertilizeResult('duplicate' if str(old['payload'])==payload else 'state_changed')
            row=uow.query_one('SELECT built,plant_slots FROM player_data.dongfu_status WHERE user_id=?',(user_id,));
            if row is None or int(row['built'] or 0)!=1: return DongfuFertilizeResult('dongfu_missing')
            slots=json.loads(str(row['plant_slots'] or ''))
            if self._canonical(slots)!=expected_slots: return DongfuFertilizeResult('state_changed')
            if slot_no>len(slots) or int(slots[slot_no-1].get('seed_id') or 0)<=0: return DongfuFertilizeResult('plot_empty')
            if int(slots[slot_no-1].get('fertilizer') or 0)>=fertilizer_max: return DongfuFertilizeResult('fertilizer_full')
            if uow.query_one('SELECT goods_num FROM back WHERE user_id=? AND goods_id=? AND goods_num>=1',(user_id,item_id)) is None: return DongfuFertilizeResult('item_insufficient')
            slots[slot_no-1]['fertilizer']=int(slots[slot_no-1].get('fertilizer') or 0)+1
            if uow.execute('UPDATE back SET goods_num=goods_num-1 WHERE user_id=? AND goods_id=? AND goods_num>=1',(user_id,item_id)).rowcount!=1: return DongfuFertilizeResult('state_changed')
            if uow.execute('UPDATE player_data.dongfu_status SET plant_slots=? WHERE user_id=?',(self._canonical(slots),user_id)).rowcount!=1: return DongfuFertilizeResult('state_changed')
            uow.execute('INSERT INTO dongfu_fertilize_operations(operation_id,payload) VALUES(?,?)',(operation_id,payload)); return DongfuFertilizeResult('fertilized')
