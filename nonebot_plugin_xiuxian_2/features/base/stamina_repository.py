from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...core.numeric import require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork


class PlayerStaminaSqlRepository:
    """Atomically consume stamina from the existing player projection."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _schema_ready(uow: DatabaseUnitOfWork) -> bool:
        columns = {
            str(row["name"])
            for row in uow.query_all('PRAGMA table_info("user_xiuxian")')
        }
        return {"user_id", "user_stamina"}.issubset(columns)

    @staticmethod
    def _result(status: str, stamina: int | None = None) -> dict[str, Any]:
        return {
            "status": status,
            "stamina": None if stamina is None else int(stamina),
        }

    @staticmethod
    def _operation_schema_ready(uow: DatabaseUnitOfWork) -> bool:
        rows = uow.query_all('PRAGMA table_info("player_stamina_operations")')
        columns = {str(row["name"]) for row in rows}
        return (
            {"operation_id", "payload", "stamina_after"}.issubset(columns)
            and any(
                str(row["name"]) == "operation_id" and int(row["pk"] or 0) == 1
                for row in rows
            )
        )

    def consume(
        self,
        user_id: str,
        amount: int,
        *,
        expected_stamina: int | None = None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            return self._consume(user_id, amount, expected_stamina=expected_stamina, operation_id=operation_id)
        except DomainError as exc:
            # Refusals cross the transaction boundary before becoming results.
            if exc.code not in {'stamina_user_write_rejected', 'stamina_receipt_write_rejected'}:
                raise
            return {'status': 'state_changed', **exc.details}

    @staticmethod
    def _reject(step, subject, stamina):
        reason = f'stamina_{step}_write_rejected'
        raise DomainError(reason, reason, {'reason': reason, 'subject': str(subject), 'stamina': stamina})

    def _consume(self, user_id, amount, *, expected_stamina, operation_id):
        user_id = str(user_id).strip()
        amount = int(amount)
        operation_id = str(operation_id or "").strip()
        if not user_id or amount < 0:
            return self._result("invalid")
        if amount == 0:
            return self._result("applied", expected_stamina)
        if not self.database.is_file():
            return self._result("schema_missing")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return self._result("schema_missing")
            payload = json.dumps([user_id, amount], separators=(",", ":"))
            if operation_id:
                if not self._operation_schema_ready(uow):
                    return self._result("schema_missing")
                previous = uow.query_one(
                    "SELECT payload,stamina_after FROM player_stamina_operations WHERE operation_id=?",
                    (operation_id,),
                )
                if previous is not None:
                    if str(previous["payload"]) != payload:
                        return self._result("operation_conflict")
                    try:
                        remaining = require_exact_integer(previous['stamina_after'], field='user_stamina', subject=operation_id)
                        if remaining < 0:
                            raise ValueError('negative stamina receipt')
                    except (DomainError, ValueError) as exc:
                        raise DomainError('stamina_receipt_invalid', 'invalid stamina receipt', {'subject': operation_id}) from exc
                    return self._result("duplicate", remaining)
            row = uow.query_one(
                "SELECT rowid AS _rowid,COALESCE(user_stamina,0) AS user_stamina "
                "FROM user_xiuxian WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return self._result("user_missing")
            current = require_exact_integer(row['user_stamina'], field='user_stamina', subject=user_id)
            if expected_stamina is not None and current != int(expected_stamina):
                return {**self._result("state_changed", current), 'reason': 'stamina_changed', 'subject': user_id}
            if current < amount:
                return self._result("stamina_insufficient", current)
            changed = uow.execute(
                "UPDATE user_xiuxian SET user_stamina=? WHERE rowid=? "
                "AND user_id=? AND COALESCE(user_stamina,0)=? "
                "AND COALESCE(user_stamina,0)>=?",
                (current - amount, row["_rowid"], user_id, current, amount),
            )
            if changed.rowcount != 1:
                self._reject('user', user_id, current)
            remaining = current - amount
            if operation_id:
                recorded = uow.execute(
                    "INSERT INTO player_stamina_operations(operation_id,payload,stamina_after) "
                    "VALUES(?,?,?)",
                    (operation_id, payload, remaining),
                )
                if recorded.rowcount != 1:
                    self._reject('receipt', operation_id, current)
            saved_user = uow.query_one('SELECT user_id,user_stamina FROM user_xiuxian WHERE rowid=?', (row['_rowid'],))
            if saved_user != {'user_id': user_id, 'user_stamina': remaining}:
                self._reject('user', user_id, current)
            if operation_id:
                saved_receipt = uow.query_one('SELECT payload,stamina_after FROM player_stamina_operations WHERE operation_id=?', (operation_id,))
                if saved_receipt != {'payload': payload, 'stamina_after': remaining}:
                    self._reject('receipt', operation_id, current)
            return self._result("applied", remaining)

    def recover(
        self,
        max_stamina: int,
        points: int,
        *,
        batch_size: int = 1000,
    ) -> dict[str, Any]:
        values = {}
        for field, value in (('max_stamina', max_stamina), ('stamina_recovery_points', points), ('batch_size', batch_size)):
            try:
                values[field] = require_exact_integer(value, field=field, subject=field)
            except DomainError:
                return {'status': 'invalid', 'reason': 'stamina_recovery_config_invalid', 'subject': field, 'updated': 0}
            if field != 'batch_size' and values[field] < 0:
                return {'status': 'invalid', 'reason': 'stamina_recovery_config_invalid', 'subject': field, 'updated': 0}
        max_stamina, points = values['max_stamina'], values['stamina_recovery_points']
        batch_size = max(1, values['batch_size'])
        if points == 0:
            return {"status": "applied", "updated": 0}
        if not self.database.is_file():
            return {"status": "schema_missing", "reason": "schema_missing", "subject": "user_xiuxian", "updated": 0}

        total = 0
        last_rowid = 0
        while True:
            try:
                updated, cursor = self._recover_batch(max_stamina, points, batch_size, last_rowid)
            except DomainError as exc:
                return {'status': 'schema_missing' if exc.code == 'schema_missing' else 'state_changed', 'reason': exc.details.get('reason') or exc.code,
                        **exc.details, 'updated': total}
            if cursor is None:
                break
            last_rowid = cursor
            total += updated
        return {"status": "applied", "updated": total}

    def _recover_batch(self, max_stamina, points, batch_size, last_rowid):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                raise DomainError('schema_missing', 'stamina recovery schema missing', {'subject': 'user_xiuxian'})
            rows = uow.query_all(
                "SELECT rowid AS _rowid,user_id,COALESCE(user_stamina,0) AS user_stamina FROM user_xiuxian "
                "WHERE rowid>? AND (COALESCE(user_stamina,0)<? OR typeof(user_stamina) NOT IN ('integer','null')) "
                "ORDER BY rowid LIMIT ?", (last_rowid, max_stamina, batch_size),
            )
            if not rows:
                return 0, None
            expected = {}
            for row in rows:
                current = require_exact_integer(row['user_stamina'], field='user_stamina', subject=str(row['user_id']))
                if current < max_stamina:
                    expected[row['_rowid']] = {**row, 'user_stamina': min(current+points, max_stamina)}
            last_rowid = int(rows[-1]['_rowid'])
            if not expected:
                return 0, last_rowid
            changed = uow.execute(
                'UPDATE user_xiuxian SET user_stamina=MIN(COALESCE(user_stamina,0)+?,?) '
                'WHERE rowid BETWEEN ? AND ? AND COALESCE(user_stamina,0)<?',
                (points, max_stamina, int(rows[0]['_rowid']), last_rowid, max_stamina),
            )
            # Re-read only the bounded target set, below SQLite's legacy bind limit.
            ids = list(expected)
            for start in range(0, len(ids), 500):
                batch = ids[start:start+500]
                saved = {row['_rowid']: row for row in uow.query_all(
                    'SELECT rowid AS _rowid,user_id,user_stamina FROM user_xiuxian WHERE rowid IN ('
                    + ','.join('?' for _ in batch) + ')', tuple(batch))}
                for rowid in batch:
                    if saved.get(rowid) != expected[rowid]:
                        raise DomainError('stamina_recovery_write_rejected', 'stamina recovery write rejected',
                                          {'subject': str(expected[rowid]['user_id'])})
            if changed.rowcount != len(expected):
                raise DomainError('stamina_recovery_write_rejected', 'stamina recovery write count mismatch',
                                  {'subject': f'rows:{ids[0]}-{ids[-1]}'})
            return len(expected), last_rowid

    def restore(
        self,
        user_id: str,
        points: int,
        max_stamina: int,
    ) -> dict[str, Any]:
        """Restore one user's stamina without creating a schema or user row."""
        user_id = str(user_id).strip()
        points, max_stamina = int(points), int(max_stamina)
        if not user_id or points < 0 or max_stamina < 0:
            return self._result("invalid")
        if points == 0:
            return self._result("applied")
        if not self.database.is_file():
            return self._result("schema_missing")

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return self._result("schema_missing")
            row = uow.query_one(
                "SELECT rowid AS _rowid,COALESCE(user_stamina,0) AS user_stamina "
                "FROM user_xiuxian WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return self._result("user_missing")
            current = int(row["user_stamina"] or 0)
            restored = min(current + points, max_stamina)
            changed = uow.execute(
                "UPDATE user_xiuxian SET user_stamina=? WHERE rowid=? "
                "AND user_id=? AND COALESCE(user_stamina,0)=?",
                (restored, row["_rowid"], user_id, current),
            )
            if changed.rowcount != 1:
                return self._result("state_changed", current)
            return self._result("applied", restored)


__all__ = ["PlayerStaminaSqlRepository"]
