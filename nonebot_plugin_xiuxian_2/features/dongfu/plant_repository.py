from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class DongfuPlantResult:
    status:str
    @property
    def succeeded(self): return self.status in {'planted','duplicate'}

class DongfuPlantSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path): self.game_database,self.player_database=str(game_database),str(player_database)
    @staticmethod
    def _canonical(value): return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    def plant(self,operation_id,user_id,expected_slots,slot_no,seed_id,seed_name,plant_start,plant_finish):
        operation_id,user_id=str(operation_id).strip(),str(user_id); expected_slots=self._canonical(json.loads(expected_slots)); slot_no,seed_id=int(slot_no),int(seed_id); payload='|'.join(map(str,(user_id,slot_no,seed_id)))
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS dongfu_plant_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT payload FROM dongfu_plant_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return DongfuPlantResult('duplicate' if str(old['payload'])==payload else 'state_changed')
            row=uow.query_one('SELECT built,plant_slots FROM player_data.dongfu_status WHERE user_id=?',(user_id,));
            if row is None or int(row['built'] or 0)!=1: return DongfuPlantResult('dongfu_missing')
            slots=json.loads(str(row['plant_slots'] or ''))
            if self._canonical(slots)!=expected_slots: return DongfuPlantResult('state_changed')
            if slot_no<1 or slot_no>len(slots) or int(slots[slot_no-1].get('seed_id') or 0)>0: return DongfuPlantResult('plot_occupied')
            if uow.query_one('SELECT goods_num FROM back WHERE user_id=? AND goods_id=? AND goods_num>=1',(user_id,seed_id)) is None: return DongfuPlantResult('seed_insufficient')
            slots[slot_no-1].update({'seed_id':seed_id,'seed_name':str(seed_name),'plant_start':str(plant_start),'plant_finish':str(plant_finish),'fertilizer':0})
            if uow.execute('UPDATE back SET goods_num=goods_num-1 WHERE user_id=? AND goods_id=? AND goods_num>=1',(user_id,seed_id)).rowcount!=1: return DongfuPlantResult('state_changed')
            active=next((s for s in slots if int(s.get('seed_id') or 0)>0),None); legacy=(1,int(active['seed_id']),active.get('plant_start',''),active.get('plant_finish','')) if active else (0,0,'','')
            if uow.execute('UPDATE player_data.dongfu_status SET plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=? WHERE user_id=?',(self._canonical(slots),*legacy,user_id)).rowcount!=1: return DongfuPlantResult('state_changed')
            uow.execute('INSERT INTO dongfu_plant_operations(operation_id,payload) VALUES(?,?)',(operation_id,payload)); return DongfuPlantResult('planted')
