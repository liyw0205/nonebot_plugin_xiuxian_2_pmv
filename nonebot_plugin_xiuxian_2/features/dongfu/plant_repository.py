from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...core.errors import DomainError
from ...infrastructure.database import DatabaseUnitOfWork
from .operation_schema import operation_databases_ready, operation_schema_ready
from .plant_diagnostics import plant_diagnosis

@dataclass(frozen=True)
class DongfuPlantResult:
    status:str
    reason:str=""
    subject:str=""
    @property
    def succeeded(self): return self.status in {'planted','duplicate'}

class DongfuPlantSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path):
        self.game_database, self.player_database = str(game_database), str(player_database)

    @staticmethod
    def _canonical(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))

    @staticmethod
    def _reject(suffix, subject):
        reason = 'dongfu_plant_' + suffix
        raise DomainError(reason, reason, {'reason': reason, 'subject': subject})

    @classmethod
    def _verify_inventory(cls, uow, user_id, seed_id, expected):
        row = uow.query_one('SELECT * FROM back WHERE user_id=? AND goods_id=?', (user_id, seed_id))
        if row is None or dict(row) != expected:
            cls._reject('inventory_projection_mismatch', f'item:{seed_id}')

    @classmethod
    def _verify_state(cls, uow, user_id, expected, slot_no):
        row = uow.query_one('SELECT * FROM player_data.dongfu_status WHERE user_id=?', (user_id,))
        if row is None or dict(row) != expected:
            cls._reject('state_projection_mismatch', f'plot:{slot_no}')

    @classmethod
    def _verify_receipt(cls, uow, operation_id, payload):
        row = uow.query_one('SELECT payload FROM dongfu_plant_operations WHERE operation_id=?', (operation_id,))
        if row is None or row['payload'] != payload:
            cls._reject('receipt_projection_mismatch', 'receipt')

    def plant(self, operation_id, user_id, expected_slots, slot_no, seed_id, seed_name, plant_start, plant_finish):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_slots = self._canonical(json.loads(expected_slots))
        slot_no, seed_id = int(slot_no), int(seed_id)
        payload = '|'.join(map(str, (user_id, slot_no, seed_id)))
        if not operation_databases_ready(self.game_database, self.player_database):
            return DongfuPlantResult('schema_missing')
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, 'player_data')
            if not operation_schema_ready(uow, 'dongfu_plant_operations'):
                return DongfuPlantResult('schema_missing')
            old = uow.query_one('SELECT payload FROM dongfu_plant_operations WHERE operation_id=?', (operation_id,))
            if old is not None:
                return (DongfuPlantResult('duplicate') if str(old['payload']) == payload
                        else DongfuPlantResult('state_changed', reason='operation_conflict'))
            row = uow.query_one('SELECT * FROM player_data.dongfu_status WHERE user_id=?', (user_id,))
            if row is None or int(row['built'] or 0) != 1:
                return DongfuPlantResult('dongfu_missing')
            slots = json.loads(str(row['plant_slots'] or ''))
            if self._canonical(slots) != expected_slots:
                return DongfuPlantResult('state_changed', **plant_diagnosis(slots, expected_slots))
            if slot_no < 1 or slot_no > len(slots) or int(slots[slot_no - 1].get('seed_id') or 0) > 0:
                return DongfuPlantResult('plot_occupied')
            item = uow.query_one('SELECT * FROM back WHERE user_id=? AND goods_id=? AND goods_num>=1', (user_id, seed_id))
            if item is None:
                return DongfuPlantResult('seed_insufficient')

            expected_item = dict(item)
            expected_item['goods_num'] = int(item['goods_num']) - 1
            bind_update = ''
            if 'bind_num' in item:
                expected_item['bind_num'] = min(max(int(item['bind_num'] or 0) - 1, 0), expected_item['goods_num'])
                bind_update = ',bind_num=MIN(MAX(COALESCE(bind_num,0)-1,0),goods_num-1)'
            consumed = uow.execute(
                'UPDATE back SET goods_num=goods_num-1' + bind_update
                + ' WHERE user_id=? AND goods_id=? AND goods_num>=1', (user_id, seed_id),
            )
            if consumed.rowcount != 1:
                self._reject('inventory_write_rejected', f'item:{seed_id}')
            self._verify_inventory(uow, user_id, seed_id, expected_item)

            slots[slot_no - 1].update(seed_id=seed_id, seed_name=str(seed_name), plant_start=str(plant_start),
                                      plant_finish=str(plant_finish), fertilizer=0)
            active = next((slot for slot in slots if int(slot.get('seed_id') or 0) > 0), None)
            legacy = (1, int(active['seed_id']), active.get('plant_start', ''), active.get('plant_finish', '')) if active else (0, 0, '', '')
            expected_state = dict(row)
            expected_state.update(plant_slots=self._canonical(slots), planting=legacy[0], plant_seed_id=legacy[1],
                                  plant_start=legacy[2], plant_finish=legacy[3])
            changed = uow.execute(
                'UPDATE player_data.dongfu_status SET plant_slots=?,planting=?,plant_seed_id=?,plant_start=?,plant_finish=? WHERE user_id=?',
                (expected_state['plant_slots'], *legacy, user_id),
            )
            if changed.rowcount != 1:
                self._reject('state_write_rejected', f'plot:{slot_no}')
            self._verify_state(uow, user_id, expected_state, slot_no)

            receipt = uow.execute('INSERT INTO dongfu_plant_operations(operation_id,payload) VALUES(?,?)',
                                  (operation_id, payload))
            if receipt.rowcount != 1:
                self._reject('receipt_write_rejected', 'receipt')
            self._verify_receipt(uow, operation_id, payload)
            # A receipt trigger can change earlier writes in the same transaction.
            self._verify_inventory(uow, user_id, seed_id, expected_item)
            self._verify_state(uow, user_id, expected_state, slot_no)
            return DongfuPlantResult('planted')
