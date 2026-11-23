from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from ...core.errors import DomainError
from ...infrastructure.database import DatabaseUnitOfWork
from .operation_schema import operation_databases_ready, operation_schema_ready
from .plant_diagnostics import plant_diagnosis

@dataclass(frozen=True)
class DongfuAccelerateResult:
    status:str
    reason:str=""
    subject:str=""
    @property
    def succeeded(self): return self.status in {'accelerated','duplicate'}

class DongfuAccelerateSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path):
        self.game_database, self.player_database = str(game_database), str(player_database)

    @staticmethod
    def _canonical(slots):
        return json.dumps(slots, ensure_ascii=False, sort_keys=True, separators=(',', ':'))

    @staticmethod
    def _reject(suffix, subject):
        reason = 'dongfu_accelerate_' + suffix
        raise DomainError(reason, reason, {'reason': reason, 'subject': subject})

    @classmethod
    def _verify_inventory(cls, uow, user_id, item_id, expected):
        row = uow.query_one('SELECT * FROM back WHERE user_id=? AND goods_id=?', (user_id, item_id))
        if row is None or dict(row) != expected:
            cls._reject('inventory_projection_mismatch', f'item:{item_id}')

    @classmethod
    def _verify_state(cls, uow, user_id, expected, slot_no):
        row = uow.query_one('SELECT * FROM player_data.dongfu_status WHERE user_id=?', (user_id,))
        if row is None or dict(row) != expected:
            cls._reject('state_projection_mismatch', f'plot:{slot_no}')

    @classmethod
    def _verify_receipt(cls, uow, operation_id, payload):
        row = uow.query_one('SELECT payload FROM dongfu_accelerate_operations WHERE operation_id=?', (operation_id,))
        if row is None or row['payload'] != payload:
            cls._reject('receipt_projection_mismatch', 'receipt')

    def accelerate(self, operation_id, user_id, expected_slots, slot_no, item_id, now, new_finish):
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_slots = self._canonical(json.loads(expected_slots))
        slot_no, item_id = int(slot_no), int(item_id)
        now, new_finish = str(now), str(new_finish)
        payload = '|'.join((user_id, str(slot_no), str(item_id)))
        if not operation_databases_ready(self.game_database, self.player_database):
            return DongfuAccelerateResult('schema_missing')
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, 'player_data')
            if not operation_schema_ready(uow, 'dongfu_accelerate_operations'):
                return DongfuAccelerateResult('schema_missing')
            old = uow.query_one('SELECT payload FROM dongfu_accelerate_operations WHERE operation_id=?', (operation_id,))
            if old is not None:
                return (DongfuAccelerateResult('duplicate') if str(old['payload']) == payload
                        else DongfuAccelerateResult('state_changed', reason='operation_conflict'))
            row = uow.query_one('SELECT * FROM player_data.dongfu_status WHERE user_id=?', (user_id,))
            if row is None or int(row['built'] or 0) != 1:
                return DongfuAccelerateResult('dongfu_missing')
            slots = json.loads(str(row['plant_slots'] or ''))
            if self._canonical(slots) != expected_slots:
                return DongfuAccelerateResult('state_changed', **plant_diagnosis(slots, expected_slots))
            if slot_no < 1 or slot_no > len(slots) or int(slots[slot_no - 1].get('seed_id') or 0) <= 0:
                return DongfuAccelerateResult('plot_empty')
            old_finish = str(slots[slot_no - 1].get('plant_finish') or '')
            if not old_finish or datetime.fromisoformat(old_finish) <= datetime.fromisoformat(now):
                return DongfuAccelerateResult('already_mature', reason='plant_finish_missing' if not old_finish else 'already_mature')
            item = uow.query_one('SELECT * FROM back WHERE user_id=? AND goods_id=? AND goods_num>=1', (user_id, item_id))
            if item is None:
                return DongfuAccelerateResult('item_insufficient')

            expected_item = dict(item)
            expected_item['goods_num'] = int(item['goods_num']) - 1
            bind_update = ''
            if 'bind_num' in item:
                expected_item['bind_num'] = min(max(int(item['bind_num'] or 0) - 1, 0), expected_item['goods_num'])
                bind_update = ',bind_num=MIN(MAX(COALESCE(bind_num,0)-1,0),goods_num-1)'
            consumed = uow.execute(
                'UPDATE back SET goods_num=goods_num-1' + bind_update
                + ' WHERE user_id=? AND goods_id=? AND goods_num>=1', (user_id, item_id),
            )
            if consumed.rowcount != 1:
                self._reject('inventory_write_rejected', f'item:{item_id}')
            self._verify_inventory(uow, user_id, item_id, expected_item)

            slots[slot_no - 1]['plant_finish'] = new_finish
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

            receipt = uow.execute('INSERT INTO dongfu_accelerate_operations(operation_id,payload) VALUES(?,?)',
                                  (operation_id, payload))
            if receipt.rowcount != 1:
                self._reject('receipt_write_rejected', 'receipt')
            self._verify_receipt(uow, operation_id, payload)
            # A receipt trigger can change earlier writes in the same transaction.
            self._verify_inventory(uow, user_id, item_id, expected_item)
            self._verify_state(uow, user_id, expected_state, slot_no)
            return DongfuAccelerateResult('accelerated')
