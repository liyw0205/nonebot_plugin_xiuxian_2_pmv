from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .account_repository import BankAccountRepository
from .history_view import BankOperationView
from .legacy_account_repository import BankLegacyAccountReadRepository


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class BankAccountInfoApplication:
    def __init__(self, database: str | Path, *, player_database: str | Path | None = None, repository: BankAccountRepository | None = None) -> None:
        self.database = str(database)
        self.player_database = str(player_database or database)
        self.repository = repository or BankAccountRepository()
        self.legacy_account_reader = BankLegacyAccountReadRepository()

    def get_legacy_info(self, *, user_id: str, default_saved_at: str) -> dict[str, Any]:
        user_id = str(user_id)
        bank_data = None
        if Path(self.player_database).is_file():
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                bank_data = self.legacy_account_reader.read_fields(uow, user_id)
        if not bank_data:
            return {"savestone": 0, "savetime": str(default_saved_at), "banklevel": "1"}

        savestone = bank_data.get("savestone", 0)
        savetime = bank_data.get("savetime", str(default_saved_at))
        bank_level = str(bank_data.get("banklevel", "1"))
        try:
            savestone = int(savestone)
        except Exception:
            savestone = 0
        return {
            "savestone": savestone,
            "savetime": str(savetime),
            "banklevel": bank_level,
        }

    def legacy_record_status(self, *, user_id: str) -> str:
        if not Path(self.player_database).is_file():
            return "missing"
        with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
            return self.legacy_account_reader.record_status(uow, str(user_id))

    def recent_operations(
        self, *, user_id: str, limit: int = 5
    ) -> tuple[BankOperationView, ...]:
        user_id = str(user_id).strip()
        limit = max(int(limit), 0)
        if not user_id or limit == 0 or not Path(self.database).is_file():
            return ()
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                self.repository.assert_schema_ready(uow)
                rows = uow.query_all(
                    "SELECT deposited,interest,wallet_after,saved_after,created_at,payload "
                    "FROM bank_account_operations WHERE user_id=? "
                    "ORDER BY created_at DESC,rowid DESC LIMIT ?",
                    (user_id, limit),
                )
        except (OSError, sqlite3.Error, RuntimeError):
            return ()
        return tuple(
            BankOperationView(
                kind=self._operation_kind(row),
                amount=abs(_integer(row["deposited"])),
                interest=max(_integer(row["interest"]), 0),
                saved_after=max(_integer(row["saved_after"]), 0),
                wallet_after=max(_integer(row["wallet_after"]), 0),
                created_at=str(row["created_at"] or ""),
            )
            for row in rows
        )

    @staticmethod
    def _operation_kind(row: Any) -> str:
        deposited = _integer(row["deposited"])
        interest = _integer(row["interest"])
        action = ""
        try:
            payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
            if isinstance(payload, (list, tuple)) and payload and isinstance(payload[0], str):
                action = payload[0]
        except (TypeError, ValueError):
            action = ""
        if action == "withdraw" or deposited < 0:
            return "取出"
        if action == "interest":
            return "结息"
        if action == "upgrade":
            return "会员升级"
        if deposited > 0:
            return "存入"
        if interest > 0:
            return "结息"
        return "账户操作"

    def get_info(self, *, user_id: str) -> dict[str, Any]:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        if not Path(self.database).is_file():
            return {"status": "schema_missing", "user_id": user_id}
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.repository.assert_schema_ready(uow)
            wallet = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            account = self.repository.existing_account(uow, user_id)
        if wallet is None:
            return {"status": "user_missing", "user_id": user_id}
        if account is None:
            return {
                "status": "account_missing", "user_id": user_id,
                "wallet_stone": int(wallet["stone"] or 0), "saved_stone": 0,
                "bank_level": "1", "updated_at": "",
            }
        return {
            "status": "ok",
            "user_id": user_id,
            "wallet_stone": int(wallet["stone"] or 0),
            "saved_stone": int(account["saved_stone"]),
            "bank_level": str(account["bank_level"]),
            "updated_at": str(account["updated_at"]),
        }


__all__ = ["BankAccountInfoApplication"]
