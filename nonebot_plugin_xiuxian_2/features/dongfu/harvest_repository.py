from __future__ import annotations
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from ...infrastructure.database import DatabaseUnitOfWork
from .operation_schema import operation_databases_ready, operation_schema_ready
from .plant_slots import canonical_plant_slots, legacy_plant_fields, normalize_plant_slots

@dataclass(frozen=True)
class DongfuHarvestResult:
    status:str; rewards:tuple=()
    @property
    def succeeded(self): return self.status in {'harvested','duplicate'}

@dataclass(frozen=True)
class DongfuHarvestSnapshotResult:
    status:str
    snapshot:dict|None=None
    @property
    def succeeded(self): return self.status in {'prepared','existing'}

class DongfuHarvestSqlRepository:
    def __init__(self,game_database:str|Path,player_database:str|Path): self.game_database,self.player_database=str(game_database),str(player_database)
    @staticmethod
    def _canonical(value): return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))

    @classmethod
    def _decode_snapshot(cls,value):
        try: snapshot=json.loads(value) if isinstance(value,str) else value
        except (TypeError,ValueError): return None
        if not isinstance(snapshot,dict): return None
        if not all(key in snapshot for key in ('expected_slots','slot_numbers','items','failed_slots')): return None
        if not isinstance(snapshot['expected_slots'],list) or not isinstance(snapshot['slot_numbers'],list): return None
        if not isinstance(snapshot['items'],list) or not isinstance(snapshot['failed_slots'],list): return None
        return snapshot

    def prepare_snapshot(self,user_id,expected_slots,snapshot,*,base_plot_count=3,max_plot_count=6,fertilizer_max=3,seed_names=None):
        user_id=str(user_id); seed_names={int(key):str(value) for key,value in (seed_names or {}).items()}
        snapshot=self._decode_snapshot(snapshot)
        if snapshot is None or self._canonical(snapshot['expected_slots'])!=self._canonical(expected_slots): return DongfuHarvestSnapshotResult('snapshot_invalid')
        if not operation_databases_ready(self.game_database,self.player_database): return DongfuHarvestSnapshotResult('schema_missing')
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data')
            if not operation_schema_ready(uow,'dongfu_harvest_operations'): return DongfuHarvestSnapshotResult('schema_missing')
            if uow.query_one('SELECT 1 AS found FROM user_xiuxian WHERE user_id=?',(user_id,)) is None: return DongfuHarvestSnapshotResult('user_missing')
            row=uow.query_one('SELECT built,plot_count,plant_slots,planting,plant_seed_id,plant_start,plant_finish,harvest_settlement FROM player_data.dongfu_status WHERE user_id=?',(user_id,))
            if row is None or int(row['built'] or 0)!=1: return DongfuHarvestSnapshotResult('dongfu_missing')
            if row.get('harvest_settlement'):
                existing=self._decode_snapshot(row['harvest_settlement'])
                return DongfuHarvestSnapshotResult('existing',existing) if existing is not None else DongfuHarvestSnapshotResult('snapshot_invalid')
            state=dict(row)
            slots=normalize_plant_slots(state,base_plot_count=base_plot_count,max_plot_count=max_plot_count,fertilizer_max=fertilizer_max,seed_names=seed_names)
            if self._canonical(slots)!=self._canonical(expected_slots): return DongfuHarvestSnapshotResult('state_changed')
            legacy=legacy_plant_fields(slots,valid_seed_ids=set(seed_names) if seed_names else None)
            slot_json=canonical_plant_slots(slots)
            changed=uow.execute(
                'UPDATE player_data.dongfu_status SET plot_count=?,plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=?,harvest_settlement=? '
                'WHERE user_id=? AND built=1 AND plot_count IS ? AND plant_slots IS ? AND planting IS ? AND plant_seed_id IS ? '
                'AND plant_start IS ? AND plant_finish IS ? AND harvest_settlement IS ?',
                (state['plot_count'],slot_json,*legacy,self._canonical(snapshot),user_id,row['plot_count'],row['plant_slots'],row['planting'],row['plant_seed_id'],row['plant_start'],row['plant_finish'],row['harvest_settlement']),
            )
            if changed.rowcount!=1: return DongfuHarvestSnapshotResult('state_changed')
            return DongfuHarvestSnapshotResult('prepared',snapshot)

    def harvest(self,operation_id,user_id,expected_slots,slot_numbers,rewards,max_goods_num,settled_at):
        operation_id,user_id=str(operation_id).strip(),str(user_id); slot_numbers=tuple(sorted({int(v) for v in slot_numbers})); max_goods_num=int(max_goods_num); payload=self._canonical([user_id,slot_numbers]); reward_rows=[(int(x['id']),str(x['name']),str(x['type']),int(x['amount'])) for x in rewards if int(x['amount'])>0]
        if not operation_databases_ready(self.game_database, self.player_database): return DongfuHarvestResult('schema_missing')
        with DatabaseUnitOfWork(self.game_database,immediate=True) as uow:
            uow.attach_database(self.player_database,'player_data')
            if not operation_schema_ready(uow, 'dongfu_harvest_operations'): return DongfuHarvestResult('schema_missing')
            old=uow.query_one('SELECT payload,rewards FROM dongfu_harvest_operations WHERE operation_id=?',(operation_id,))
            if old is not None: return DongfuHarvestResult('duplicate' if str(old['payload'])==payload else 'state_changed',tuple(tuple(x) for x in json.loads(old['rewards'])))
            user=uow.query_one('SELECT 1 AS found FROM user_xiuxian WHERE user_id=?',(user_id,)); row=uow.query_one('SELECT built,plant_slots FROM player_data.dongfu_status WHERE user_id=?',(user_id,));
            if user is None: return DongfuHarvestResult('user_missing')
            if row is None or int(row['built'] or 0)!=1: return DongfuHarvestResult('dongfu_missing')
            actual=json.loads(str(row['plant_slots']));
            if self._canonical(actual)!=self._canonical(expected_slots): return DongfuHarvestResult('state_changed')
            for slot_no in slot_numbers:
                if slot_no<1 or slot_no>len(actual): return DongfuHarvestResult('state_changed')
                if not str(actual[slot_no-1].get('plant_finish') or '') or str(actual[slot_no-1].get('plant_finish'))>str(settled_at): return DongfuHarvestResult('not_mature')
            totals={}; meta={}
            for item_id,name,item_type,amount in reward_rows: totals[item_id]=totals.get(item_id,0)+amount; meta[item_id]=(name,item_type)
            for item_id,amount in totals.items():
                item=uow.query_one('SELECT COALESCE(goods_num,0) AS goods_num FROM back WHERE user_id=? AND goods_id=?',(user_id,item_id));
                if (0 if item is None else int(item['goods_num']))+amount>max_goods_num: return DongfuHarvestResult('inventory_full')
            for slot_no in slot_numbers: actual[slot_no-1]={'slot':slot_no,'seed_id':0,'seed_name':'','plant_start':'','plant_finish':'','fertilizer':0}
            active=next((s for s in actual if int(s.get('seed_id') or 0)>0),None); legacy=(1,int(active['seed_id']),active.get('plant_start',''),active.get('plant_finish','')) if active else (0,0,'','')
            if uow.execute('UPDATE player_data.dongfu_status SET plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=?,harvest_settlement=? WHERE user_id=?',(self._canonical(actual),*legacy,'',user_id)).rowcount!=1: return DongfuHarvestResult('state_changed')
            now=datetime.now().isoformat(sep=' ',timespec='seconds')
            for item_id,amount in totals.items():
                name,item_type=meta[item_id]; uow.execute('INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time',(user_id,item_id,name,item_type,amount,now,now,amount))
            compact=tuple(sorted(totals.items())); uow.execute('INSERT INTO dongfu_harvest_operations(operation_id,payload,rewards) VALUES(?,?,?)',(operation_id,payload,json.dumps(compact))); return DongfuHarvestResult('harvested',compact)
