from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .account_repository import BankAccountRepository


class BankAccountBootstrapApplication:
    def __init__(self, game_database: str | Path, player_database: str | Path, *, repository: BankAccountRepository | None = None) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self.repository = repository or BankAccountRepository()

    def ensure_account(self, *, user_id: str, default_level: str) -> dict[str, Any]:
        user_id = str(user_id).strip()
        if not user_id:
            raise ValueError("user_id is required")
        with DatabaseUnitOfWork(self.game_database, immediate=True) as uow:
            self.repository.ensure_schema(uow)
            existing = self.repository.existing_account(uow, user_id)
            if existing is not None:
                return {"status": "existing", "user_id": user_id, "saved_stone": int(existing["saved_stone"]), "bank_level": str(existing["bank_level"])}
            uow.attach_database(self.player_database, "player_data")
            table = uow.query_one("SELECT 1 AS present FROM player_data.sqlite_master WHERE type='table' AND name='bankinfo'")
            if table is None:
                return {"status": "legacy_missing", "user_id": user_id}
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA player_data.table_info(bankinfo)")}
            if not {"user_id", "savestone", "savetime", "banklevel"}.issubset(columns):
                return {"status": "legacy_invalid", "user_id": user_id}
            legacy = uow.query_one(
                "SELECT COALESCE(savestone,0) AS saved_stone,COALESCE(savetime,'') AS updated_at,COALESCE(banklevel,?) AS bank_level "
                "FROM player_data.bankinfo WHERE user_id=?",
                (str(default_level), user_id),
            )
            if legacy is None:
                return {"status": "legacy_missing", "user_id": user_id}
            uow.execute(
                "INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) VALUES(?,?,?,?)",
                (user_id, int(legacy["saved_stone"]), str(legacy["bank_level"]), str(legacy["updated_at"])),
            )
            return {
                "status": "imported",
                "user_id": user_id,
                "saved_stone": int(legacy["saved_stone"]),
                "bank_level": str(legacy["bank_level"]),
            }


__all__ = ["BankAccountBootstrapApplication"]
