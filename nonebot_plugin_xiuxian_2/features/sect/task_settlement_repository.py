from __future__ import annotations

from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from ...core.errors import DomainError
from ...core.numeric import as_int_like, bind_sqlite_param, require_exact_integer
from .task_state_repository import _task_object, _verified_task_result


class SectTaskSettlementSqlRepository:
    _OPERATION_COLUMNS = {
        "operation_id",
        "user_id",
        "sect_id",
        "period",
        "cost_type",
        "cost",
        "exp_reward",
        "sect_reward",
        "materials_reward",
        "weekly_event_status",
        "created_at",
    }

    def __init__(self, database: str | Path, *, clock: Any | None = None, task_timezone=None) -> None:
        self.database = str(database)
        self.clock = clock or SystemClock()
        self.task_timezone = task_timezone or ZoneInfo('Asia/Shanghai')

    @staticmethod
    def _reject(step, subject):
        reason = f'sect_task_settlement_{step}_write_rejected'
        raise DomainError(reason, reason, {'reason': reason, 'subject': str(subject)})

    @staticmethod
    def _number(value, field, subject):
        if field in {'hp', 'exp', 'sect_scale', 'sect_task_legacy_reward'}:
            try:
                raw = bytes(value).decode('ascii') if isinstance(value, (bytes, bytearray, memoryview)) else value
                number = Decimal(str(raw))
                if not number.is_finite():
                    raise ValueError('non-finite value')
                if field != 'sect_task_legacy_reward' and number != number.to_integral_value():
                    return float(number)
                return as_int_like(value)
            except (ValueError, TypeError, InvalidOperation, UnicodeError) as exc:
                raise DomainError(f'{field}_invalid_numeric_value', 'invalid task numeric value',
                                  {'subject': str(subject)}) from exc
        return require_exact_integer(value, field=field, subject=str(subject))

    def _receipt(self, row, user_id, operation_id):
        if str(row['user_id']) != str(user_id):
            raise DomainError('operation_conflict', 'task receipt belongs to another player')
        data = {**row, 'status': 'duplicate'}
        for field in ('sect_id', 'cost', 'exp_reward', 'sect_reward', 'materials_reward'):
            parser_field = 'sect_task_legacy_reward' if field in {'exp_reward', 'sect_reward', 'materials_reward'} else 'sect_task_receipt'
            data[field] = self._number(row[field], parser_field, operation_id)
        if data['cost_type'] not in {'hp', 'stone'} or any(data[key] < 0 for key in ('cost', 'exp_reward', 'sect_reward', 'materials_reward')):
            raise DomainError('sect_task_receipt_invalid', 'invalid task settlement receipt', {'subject': operation_id})
        return data

    def settlement_receipt(self, operation_id, user_id):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._schema_ready(uow):
                raise DomainError('schema_missing', 'task settlement schema is not ready')
            row = uow.query_one('SELECT user_id,sect_id,period,cost_type,cost,exp_reward,sect_reward,materials_reward,weekly_event_status '
                                'FROM sect_task_settlement_operations WHERE operation_id=?', (str(operation_id),))
            return self._receipt(row, user_id, operation_id) if row is not None else None

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='sect_task_settlement_operations'"
        )
        if table is None:
            return False
        columns = {
            str(row["name"])
            for row in uow.query_all("PRAGMA table_info(sect_task_settlement_operations)")
        }
        return cls._OPERATION_COLUMNS.issubset(columns)

    @_verified_task_result
    def settle(
        self,
        operation_id: str,
        user_id: str,
        sect_id: int,
        period: str,
        cost_type: str,
        cost: int,
        exp_reward: int,
        sect_reward: int,
        expected_task_key: str | None = None,
        expected_task_data: dict[str, Any] | None = None,
        *,
        current_period: str | None = None,
    ) -> dict[str, Any]:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        period = str(period).strip()
        cost_type = str(cost_type).strip().lower()
        sect_id, cost, exp_reward, sect_reward = map(
            int, (sect_id, cost, exp_reward, sect_reward)
        )
        materials_reward = sect_reward * 10
        if not operation_id or not period:
            raise ValueError("operation_id and period are required")

        base = {
            "user_id": user_id,
            "sect_id": sect_id,
            "period": period,
            "cost_type": cost_type,
            "cost": cost,
            "exp_reward": exp_reward,
            "sect_reward": sect_reward,
            "materials_reward": materials_reward,
        }
        if cost_type not in {"hp", "stone"}:
            return {"status": "invalid_cost_type", **base}
        if min(cost, exp_reward, sect_reward) < 0:
            return {"status": "invalid_amount", **base}

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return {"status": "schema_missing", **base}

            previous = uow.query_one(
                "SELECT user_id,sect_id,period,cost_type,cost,exp_reward,sect_reward,materials_reward,weekly_event_status "
                "FROM sect_task_settlement_operations WHERE operation_id=?",
                (operation_id,),
            )
            if previous:
                if (int(previous['sect_id']), str(previous['period'])) != (sect_id, period):
                    return {'status': 'operation_conflict', **base}
                return self._receipt(previous, user_id, operation_id)
            if current_period is not None and period != current_period:
                return {'status': 'state_changed', **base, 'reason': 'sect_task_period_changed', 'subject': user_id}

            task = uow.query_one(
                "SELECT sect_id,status,task_key,task_data,progress,target FROM sect_task_state "
                "WHERE user_id=? AND period=?",
                (user_id, period),
            )
            if task is None or str(task["status"]) != "accepted":
                return {"status": "task_missing", **base}
            if int(task["sect_id"]) != sect_id:
                return {"status": "task_sect_changed", **base}
            if expected_task_key is not None and str(task["task_key"]) != str(expected_task_key):
                return {**{"status": "task_snapshot_changed", **base}, "reason": "task_key_changed", "subject": ""}
            if expected_task_data is not None and _task_object(task["task_data"], user_id) != dict(expected_task_data):
                return {**{"status": "task_snapshot_changed", **base}, "reason": "task_contents_changed", "subject": ""}

            user = uow.query_one(
                "SELECT sect_id,stone,hp,exp,sect_task,sect_contribution FROM user_xiuxian WHERE user_id=?",
                (user_id,),
            )
            if user is None:
                return {"status": "user_missing", **base}
            if user["sect_id"] is None or int(user["sect_id"]) != sect_id:
                return {"status": "sect_changed", **base}

            balance = self._number(user[cost_type] or 0, cost_type, user_id)
            if balance < cost:
                return {"status": f"{cost_type}_insufficient", **base}
            sect = uow.query_one('SELECT sect_used_stone,sect_scale,sect_materials FROM sects WHERE sect_id=?', (sect_id,))
            if sect is None:
                return {"status": "sect_missing", **base}

            column = "hp" if cost_type == "hp" else "stone"
            user_expected = {column: balance-cost,
                             'exp': self._number(user['exp'] or 0, 'exp', user_id)+exp_reward,
                             'sect_task': self._number(user['sect_task'] or 0, 'sect_task', user_id)+1,
                             'sect_contribution': self._number(user['sect_contribution'] or 0, 'sect_contribution', user_id)+sect_reward}
            sect_expected = {field: self._number(sect[field] or 0, field, sect_id)+delta for field, delta in
                             (('sect_used_stone', sect_reward), ('sect_scale', sect_reward), ('sect_materials', materials_reward))}
            now = self.clock.now()
            now = now.astimezone(self.task_timezone) if now.tzinfo is not None else now
            now = now.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
            user_update = uow.execute(
                f"UPDATE user_xiuxian SET {column}=?,exp=?,sect_task=?,sect_contribution=? "
                f"WHERE user_id=? AND sect_id=? AND xiuxian_int_cmp({column},?)>=0",
                (*[bind_sqlite_param(v, exact=True) for v in user_expected.values()], user_id, sect_id, cost),
            )
            if user_update.rowcount != 1:
                self._reject('user', user_id)
            sect_update = uow.execute(
                "UPDATE sects SET sect_used_stone=?,sect_scale=?,sect_materials=? WHERE sect_id=?",
                (*[bind_sqlite_param(v, exact=True) for v in sect_expected.values()], sect_id),
            )
            if sect_update.rowcount != 1:
                self._reject('sect', sect_id)
            task_update = uow.execute(
                'UPDATE sect_task_state SET status="completed",progress=target, '
                "updated_at=?,completed_at=? WHERE user_id=? AND period=? "
                'AND sect_id=? AND status="accepted"',
                (now, now, user_id, period, sect_id),
            )
            if task_update.rowcount != 1:
                self._reject('state', user_id)

            recorded = uow.execute(
                "INSERT INTO sect_task_settlement_operations("
                "operation_id,user_id,sect_id,period,cost_type,cost,exp_reward,"
                "sect_reward,materials_reward,weekly_event_status) "
                "VALUES(?,?,?,?,?,xiuxian_int_store(?),xiuxian_int_store(?),xiuxian_int_store(?),xiuxian_int_store(?),'pending')",
                (operation_id, user_id, sect_id, period, cost_type, str(cost), str(exp_reward), str(sect_reward), str(materials_reward)),
            )
            if recorded.rowcount != 1:
                self._reject('receipt', operation_id)
            saved_user = uow.query_one(f'SELECT sect_id,{column},exp,sect_task,sect_contribution FROM user_xiuxian WHERE user_id=?', (user_id,))
            if saved_user is None or saved_user['sect_id'] != sect_id or any(
                self._number(saved_user[field], field, user_id) != expected for field, expected in user_expected.items()
            ):
                self._reject('user', user_id)
            saved_sect = uow.query_one('SELECT sect_used_stone,sect_scale,sect_materials FROM sects WHERE sect_id=?', (sect_id,))
            if saved_sect is None or any(self._number(saved_sect[field], field, sect_id) != expected for field, expected in sect_expected.items()):
                self._reject('sect', sect_id)
            saved_task = uow.query_one('SELECT sect_id,status,task_key,task_data,progress,target,updated_at,completed_at '
                                       'FROM sect_task_state WHERE user_id=? AND period=?', (user_id, period))
            if saved_task != {**task, 'status': 'completed', 'progress': task['target'], 'updated_at': now, 'completed_at': now}:
                self._reject('state', user_id)
            saved = uow.query_one('SELECT user_id,sect_id,period,cost_type,cost,exp_reward,sect_reward,materials_reward,weekly_event_status '
                                  'FROM sect_task_settlement_operations WHERE operation_id=?', (operation_id,))
            if saved is None or saved["weekly_event_status"] != "pending" or any(
                self._number(saved[field], 'sect_task_receipt', operation_id) != expected if isinstance(expected, int) else saved[field] != expected
                for field, expected in base.items()
            ):
                self._reject('receipt', operation_id)
            return {"status": "settled", **base, "weekly_event_status": "pending"}
