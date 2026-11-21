from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ...core.errors import DomainError
from ...core.numeric import require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork
from .operation_schema import operation_databases_ready, operation_schema_ready

@dataclass(frozen=True)
class DongfuPatrolResult:
    status:str; patrol_count:int=0; patrol_guard:int=0
    reason:str=""
    subject:str=""
    @property
    def succeeded(self): return self.status in {'patrolled','duplicate'}

class DongfuPatrolSqlRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path):
        self.game_database, self.player_database = str(game_database), str(player_database)

    @staticmethod
    def _reject(suffix, subject):
        reason = 'dongfu_patrol_' + suffix
        raise DomainError(reason, reason, {'reason': reason, 'subject': subject})

    @classmethod
    def _verify_player(cls, uow, user_id, expected):
        row = uow.query_one('SELECT * FROM user_xiuxian WHERE user_id=?', (user_id,))
        if row is not None:
            row['stone'] = require_exact_integer(row['stone'], field='stone', subject='actor')
        if row is None or row != expected:
            cls._reject('player_projection_mismatch', 'actor')

    @classmethod
    def _verify_state(cls, uow, user_id, expected):
        row = uow.query_one('SELECT * FROM player_data.dongfu_status WHERE user_id=?', (user_id,))
        if row is None or row != expected:
            cls._reject('state_projection_mismatch', 'patrol')

    @classmethod
    def _verify_inventory(cls, uow, user_id, item_id, expected):
        row = uow.query_one('SELECT * FROM back WHERE user_id=? AND goods_id=?', (user_id, item_id))
        if row is None or any(row.get(key) != value for key, value in expected.items()):
            cls._reject('inventory_projection_mismatch', f'item:{item_id}')

    @classmethod
    def _verify_receipt(cls, uow, operation_id, expected):
        row = uow.query_one('SELECT payload,patrol_count,patrol_guard FROM dongfu_patrol_operations WHERE operation_id=?', (operation_id,))
        if row is None or row != expected:
            cls._reject('receipt_projection_mismatch', 'receipt')

    def patrol(self, operation_id, user_id, day, stamina_cost, daily_limit, stone_gain, reward=None, max_goods_num=999999999):
        operation_id, user_id, day = str(operation_id).strip(), str(user_id), str(day)
        stamina_cost, daily_limit, stone_gain, max_goods_num = map(int, (stamina_cost, daily_limit, stone_gain, max_goods_num))
        payload = json.dumps([user_id, day], separators=(',', ':'))
        if not operation_databases_ready(self.game_database, self.player_database):
            return DongfuPatrolResult('schema_missing')
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            uow.attach_database(self.player_database, 'player_data')
            if not operation_schema_ready(uow, 'dongfu_patrol_operations'):
                return DongfuPatrolResult('schema_missing')
            old = uow.query_one('SELECT payload,patrol_count,patrol_guard FROM dongfu_patrol_operations WHERE operation_id=?', (operation_id,))
            if old is not None:
                return (DongfuPatrolResult('duplicate', int(old['patrol_count']), int(old['patrol_guard']))
                        if str(old['payload']) == payload else DongfuPatrolResult('state_changed', reason='operation_conflict'))
            user = uow.query_one('SELECT * FROM user_xiuxian WHERE user_id=?', (user_id,))
            if user is None:
                return DongfuPatrolResult('user_missing')
            if int(user['user_stamina'] or 0) < stamina_cost:
                return DongfuPatrolResult('stamina_insufficient')
            row = uow.query_one('SELECT * FROM player_data.dongfu_status WHERE user_id=?', (user_id,))
            if row is None or int(row['built'] or 0) != 1:
                return DongfuPatrolResult('dongfu_missing')
            count, guard = ((int(row['patrol_count'] or 0), int(row['patrol_guard'] or 0))
                            if str(row['patrol_date'] or '') == day else (0, 0))
            if count >= daily_limit:
                return DongfuPatrolResult('daily_limit', count, guard)
            item = None
            if reward:
                item_id, item_name, amount = int(reward[0]), str(reward[1]), int(reward[2])
                item = uow.query_one('SELECT * FROM back WHERE user_id=? AND goods_id=?', (user_id, item_id))
                if (int(item['goods_num'] or 0) if item else 0) + amount > max_goods_num:
                    return DongfuPatrolResult('inventory_full', count, guard)

            count, guard = count + 1, min(3, guard + 1)
            expected_user = dict(user)
            expected_user.update(user_stamina=int(user['user_stamina'] or 0) - stamina_cost,
                                 stone=require_exact_integer(user['stone'] if user['stone'] is not None else 0,
                                                             field='stone', subject='actor') + stone_gain)
            changed = uow.execute(
                "UPDATE user_xiuxian SET user_stamina=user_stamina-?,stone=xiuxian_int_store(xiuxian_int_add(COALESCE(stone,0),?,'stone','actor')) WHERE user_id=? AND user_stamina>=?",
                (stamina_cost, stone_gain, user_id, stamina_cost),
            )
            if changed.rowcount != 1:
                self._reject('player_write_rejected', 'actor')
            self._verify_player(uow, user_id, expected_user)

            expected_state = dict(row)
            expected_state.update(patrol_date=day, patrol_count=count, patrol_guard=guard)
            changed = uow.execute('UPDATE player_data.dongfu_status SET patrol_date=?,patrol_count=?,patrol_guard=? WHERE user_id=?',
                                  (day, count, guard, user_id))
            if changed.rowcount != 1:
                self._reject('state_write_rejected', 'patrol')
            self._verify_state(uow, user_id, expected_state)

            if reward:
                expected_item = dict(item) if item else {
                    'user_id': user_id, 'goods_id': item_id, 'goods_name': item_name, 'goods_type': '特殊物品',
                }
                expected_item.update(goods_num=(int(item['goods_num'] or 0) if item else 0) + amount,
                                     bind_num=(int(item['bind_num'] or 0) if item else 0) + amount)
                changed = uow.execute(
                    'INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num) VALUES(?,?,?,?,?,?) '
                    'ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_num=back.goods_num+excluded.goods_num,bind_num=COALESCE(back.bind_num,0)+excluded.bind_num',
                    (user_id, item_id, item_name, '特殊物品', amount, amount),
                )
                if changed.rowcount != 1:
                    self._reject('inventory_write_rejected', f'item:{item_id}')
                self._verify_inventory(uow, user_id, item_id, expected_item)

            changed = uow.execute('INSERT INTO dongfu_patrol_operations(operation_id,payload,patrol_count,patrol_guard) VALUES(?,?,?,?)',
                                  (operation_id, payload, count, guard))
            if changed.rowcount != 1:
                self._reject('receipt_write_rejected', 'receipt')
            self._verify_receipt(uow, operation_id, {'payload': payload, 'patrol_count': count, 'patrol_guard': guard})
            # Receipt triggers can alter previously verified player or inventory rows.
            self._verify_player(uow, user_id, expected_user)
            self._verify_state(uow, user_id, expected_state)
            if reward:
                self._verify_inventory(uow, user_id, item_id, expected_item)
            return DongfuPatrolResult('patrolled', count, guard)
