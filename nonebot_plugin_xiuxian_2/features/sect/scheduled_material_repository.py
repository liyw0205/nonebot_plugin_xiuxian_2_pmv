from __future__ import annotations

from pathlib import Path
from typing import Any

from ...core.errors import DomainError
from ...core.numeric import as_int_like, require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork


class SectScheduledMaterialSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def list_targets(self) -> list[tuple[Any, ...]]:
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            rows = uow.query_all(
                "SELECT sect_id,sect_scale,elixir_room_level FROM sects "
                "WHERE sect_owner IS NOT NULL ORDER BY xiuxian_int_text(sect_scale) COLLATE xiuxian_integer DESC"
            )
            return [
                (row["sect_id"], row["sect_scale"], row["elixir_room_level"])
                for row in rows
            ]

    def grant(self, grant_key: str, sect_id: int, multiplier: int) -> dict[str, Any]:
        grant_key = str(grant_key).strip()
        sect_id, multiplier = int(sect_id), max(int(multiplier), 0)
        if not grant_key:
            raise ValueError("grant_key must not be empty")
        base = {
            "grant_key": grant_key,
            "sect_id": sect_id,
            "materials": 0,
            "combat_power": 0,
        }
        try:
            return self._grant(base, multiplier)
        except DomainError as exc:
            return {**base, 'status': 'state_changed',
                    'reason': exc.details.get('reason') or exc.code,
                    'subject': str(exc.details.get('subject') or sect_id)}

    @staticmethod
    def _number(value, field, subject):
        return require_exact_integer(0 if value is None else value, field=field, subject=str(subject))

    @staticmethod
    def _reject(target, sect_id):
        reason = f'sect_material_{target}_write_rejected'
        raise DomainError(reason, f'{reason}: {sect_id}', {'reason': reason, 'subject': str(sect_id)})

    def _grant(self, base, multiplier):
        grant_key, sect_id = base['grant_key'], base['sect_id']
        if not Path(self.database).is_file():
            return {**base, 'status': 'schema_missing', 'reason': 'schema_missing', 'subject': str(sect_id)}
        with DatabaseUnitOfWork(self.database, immediate=True, require_exists=True) as uow:
            receipt_table = uow.query_one(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                ("sect_scheduled_material_grants",),
            )
            if receipt_table is None:
                return {"status": "schema_missing", **base, 'reason': 'schema_missing', 'subject': str(sect_id)}

            old = uow.query_one(
                "SELECT materials,combat_power FROM sect_scheduled_material_grants "
                "WHERE grant_key=? AND sect_id=?",
                (grant_key, sect_id),
            )
            if old:
                return {
                    "status": "duplicate",
                    **base,
                    "materials": self._number(old["materials"], 'sect_material_receipt', sect_id),
                    "combat_power": as_int_like(old["combat_power"]),
                }

            sect = uow.query_one(
                "SELECT sect_scale,sect_owner,sect_materials FROM sects WHERE sect_id=?", (sect_id,)
            )
            if sect is None:
                return {"status": "sect_missing", **base, 'reason': 'sect_missing', 'subject': str(sect_id)}
            if sect["sect_owner"] is None:
                return {"status": "sect_inactive", **base, 'reason': 'sect_inactive', 'subject': str(sect_id)}

            materials = max(self._number(sect['sect_scale'], 'sect_scale', sect_id), 0) * multiplier
            members = uow.query_all(
                "SELECT user_id,power FROM user_xiuxian WHERE sect_id=?",
                (sect_id,),
            )
            combat_power = sum(as_int_like(row['power']) for row in members)
            current_materials = self._number(sect['sect_materials'], 'sect_materials', sect_id)
            written = uow.execute(
                "UPDATE sects SET sect_materials=xiuxian_int_store(?,'sect_materials','actor'),"
                "combat_power=xiuxian_int_store(?) WHERE sect_id=?",
                (str(current_materials + materials), str(combat_power), sect_id),
            )
            if written.rowcount != 1:
                self._reject('state', sect_id)
            recorded = uow.execute(
                "INSERT INTO sect_scheduled_material_grants "
                "(grant_key,sect_id,materials,combat_power) VALUES(?,?,xiuxian_int_store(?),xiuxian_int_store(?))",
                (grant_key, sect_id, str(materials), str(combat_power)),
            )
            if recorded.rowcount != 1:
                self._reject('receipt', sect_id)
            saved = uow.query_one('SELECT sect_materials,combat_power FROM sects WHERE sect_id=?', (sect_id,))
            if (saved is None or self._number(saved['sect_materials'], 'sect_materials', sect_id) != current_materials + materials
                    or self._number(saved['combat_power'], 'combat_power', sect_id) != combat_power):
                self._reject('state', sect_id)
            receipt = uow.query_one('SELECT materials,combat_power FROM sect_scheduled_material_grants '
                                    'WHERE grant_key=? AND sect_id=?', (grant_key, sect_id))
            if (receipt is None or self._number(receipt['materials'], 'sect_material_receipt', sect_id) != materials
                    or self._number(receipt['combat_power'], 'sect_combat_power_receipt', sect_id) != combat_power):
                self._reject('receipt', sect_id)
            return {
                "status": "granted",
                **base,
                "materials": materials,
                "combat_power": combat_power,
            }


__all__ = ["SectScheduledMaterialSqlRepository"]
