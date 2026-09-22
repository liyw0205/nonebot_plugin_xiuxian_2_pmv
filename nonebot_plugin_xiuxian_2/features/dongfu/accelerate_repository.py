from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork

@dataclass(frozen=True)
class DongfuAccelerateResult:
    status:str
    @property
    def succeeded(self): return self.status in {'accelerated','duplicate'}

class DongfuAccelerateSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path): self.game_database,self.player_database=str(game_database),str(player_database)
    @staticmethod
    def _canonical(slots): return json.dumps(slots,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    def accelerate(self,operation_id,user_id,expected_slots,slot_no,item_id,now,new_finish):
        operation_id,user_id=str(operation_id).strip(),str(user_id); expected_slots=self._canonical(json.loads(expected_slots)); slot_no,item_id=int(slot_no),int(item_id); now,new_finish=str(now),str(new_finish); payload='|'.join((user_id,str(slot_no),str(item_id)))
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data'); uow.execute('CREATE TABLE IF NOT EXISTS dongfu_accelerate_operations(operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)'); old=uow.query_one('SELECT payload FROM dongfu_accelerate_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return DongfuAccelerateResult('duplicate' if str(old['payload'])==payload else 'state_changed')
            row=uow.query_one('SELECT built,plant_slots FROM player_data.dongfu_status WHERE user_id=?',(user_id,));
            if row is None or int(row['built'] or 0)!=1: return DongfuAccelerateResult('dongfu_missing')
            slots=json.loads(str(row['plant_slots'] or '')); 
            if self._canonical(slots)!=expected_slots: return DongfuAccelerateResult('state_changed')
            if slot_no>len(slots) or int(slots[slot_no-1].get('seed_id') or 0)<=0: return DongfuAccelerateResult('plot_empty')
            old_finish=str(slots[slot_no-1].get('plant_finish') or '');
            if not old_finish or old_finish<=now: return DongfuAccelerateResult('already_mature')
            if uow.query_one('SELECT goods_num FROM back WHERE user_id=? AND goods_id=? AND goods_num>=1',(user_id,item_id)) is None: return DongfuAccelerateResult('item_insufficient')
            slots[slot_no-1]['plant_finish']=new_finish; active=next((slot for slot in slots if int(slot.get('seed_id') or 0)>0),None); legacy=(1,int(active['seed_id']),active.get('plant_start',''),active.get('plant_finish','')) if active else (0,0,'','')
            if uow.execute('UPDATE back SET goods_num=goods_num-1 WHERE user_id=? AND goods_id=? AND goods_num>=1',(user_id,item_id)).rowcount!=1: return DongfuAccelerateResult('state_changed')
            if uow.execute('UPDATE player_data.dongfu_status SET plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=? WHERE user_id=?',(self._canonical(slots),*legacy,user_id)).rowcount!=1: return DongfuAccelerateResult('state_changed')
            uow.execute('INSERT INTO dongfu_accelerate_operations(operation_id,payload) VALUES(?,?)',(operation_id,payload)); return DongfuAccelerateResult('accelerated')
