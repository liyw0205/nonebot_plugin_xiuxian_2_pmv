from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from ...core.errors import DomainError
from ...infrastructure.database import DatabaseUnitOfWork
from ..base.inventory_application import PlayerInventoryApplication
from ..base.inventory_repository import InventoryGrantResult
from .legacy_state import crystal_balance, effective_state_row, effective_state_row_id


class LegacyCrystalSqlRepository:
    """Keep unused legacy write APIs inside the same exact transaction boundary."""

    def __init__(self, impart_database: str | Path, game_database: str | Path) -> None:
        self.impart_database = str(impart_database)
        self.game_database = str(game_database)

    @staticmethod
    def _replay(uow, schema, operation_id, payload):
        old = uow.query_one(
            f"SELECT payload,result_json FROM {schema}.impart_legacy_crystal_operations "
            "WHERE operation_id=?", (operation_id,),
        )
        if old is None:
            return None
        if old["payload"] != payload:
            raise DomainError(
                "operation_conflict", "legacy crystal request identity conflicts",
                {"reason": "operation_conflict", "subject": "actor"},
            )
        return json.loads(old["result_json"])

    @staticmethod
    def _save(uow, schema, operation_id, payload, result):
        uow.execute(
            f"INSERT INTO {schema}.impart_legacy_crystal_operations "
            "(operation_id,payload,result_json) VALUES(?,?,?)",
            (operation_id, payload, json.dumps(result, ensure_ascii=True, sort_keys=True)),
        )

    def change(self, operation_id: str, user_id: str | None, delta: int) -> int:
        payload = json.dumps(["change", user_id, int(delta)], separators=(",", ":"))
        with DatabaseUnitOfWork(self.impart_database, immediate=True, require_exists=True) as uow:
            replay = self._replay(uow, "main", operation_id, payload)
            if replay is not None:
                return int(replay["rows"])
            users = [str(user_id)] if user_id is not None else [
                str(row["user_id"]) for row in uow.query_all(
                    "SELECT DISTINCT user_id FROM xiuxian_impart"
                )
            ]
            states = []
            for uid in users:
                row = effective_state_row(uow, uid)
                if row is None:
                    continue
                previous = crystal_balance(row, subject=uid)
                if previous + int(delta) < 0:
                    raise DomainError(
                        "stone_missing", "crystal debit exceeds exact balance",
                        {"reason": "crystal_missing", "subject": uid},
                    )
                states.append((uid, effective_state_row_id(row), previous))
            total = 0
            for uid, row_id, previous in states:
                final = previous + int(delta)
                changed = uow.execute(
                    "UPDATE xiuxian_impart SET stone_num=?,stone_num_exact=?,"
                    "stone_num_precision_state='exact' WHERE rowid=? AND user_id=? "
                    "AND stone_num_precision_state='exact' "
                    "AND xiuxian_int_cmp(COALESCE(stone_num_exact,stone_num,0),?)=0",
                    (final, str(final), row_id, uid, previous),
                )
                if changed.rowcount != 1:
                    raise DomainError("impart_write_rejected", "effective impart row changed inside crystal change transaction")
                total += 1
            self._save(uow, "main", operation_id, payload, {"rows": total})
            return total

    def convert(self, operation_id: str, user_id: str, max_goods_num: int):
        payload = json.dumps(["convert", str(user_id)], separators=(",", ":"))
        with DatabaseUnitOfWork(self.game_database, immediate=True, require_exists=True) as uow:
            uow.attach_database(self.impart_database, "impart_data")
            replay = self._replay(uow, "impart_data", operation_id, payload)
            if replay is not None:
                return InventoryGrantResult(**replay)
            row = effective_state_row(uow, user_id, schema="impart_data")
            if row is None:
                return None
            previous = crystal_balance(row, subject=user_id)
            if previous < 100:
                return None
            grant = PlayerInventoryApplication(self.game_database).grant_item(
                user_id, 20005, "祈愿石", "特殊道具", previous // 100,
                bind_flag=1, max_goods_num=int(max_goods_num), require_full=True, uow=uow,
            )
            if not grant.succeeded:
                return grant
            changed = uow.execute(
                "UPDATE impart_data.xiuxian_impart SET stone_num=0,stone_num_exact='0',"
                "stone_num_precision_state='exact' WHERE rowid=? AND user_id=? "
                "AND stone_num_precision_state='exact' "
                "AND xiuxian_int_cmp(COALESCE(stone_num_exact,stone_num,0),?)=0",
                (effective_state_row_id(row), str(user_id), previous),
            )
            if changed.rowcount != 1:
                raise DomainError("impart_write_rejected", "effective impart row changed inside crystal conversion transaction")
            self._save(uow, "impart_data", operation_id, payload, asdict(grant))
            return grant
