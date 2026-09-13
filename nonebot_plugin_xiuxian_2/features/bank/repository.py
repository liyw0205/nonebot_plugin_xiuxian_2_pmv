from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class BankRepository(Protocol):
    def deposit(self, *args: Any) -> Any: ...
    def withdraw(self, *args: Any) -> Any: ...
    def upgrade(self, *args: Any) -> Any: ...
    def settle_interest(self, *args: Any) -> Any: ...


class LegacyBankRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_bank.transaction_service import (
            BankDepositService, BankInterestService, BankUpgradeService, BankWithdrawalService,
        )
        return (
            BankDepositService(self.game_database, self.player_database),
            BankWithdrawalService(self.game_database, self.player_database),
            BankUpgradeService(self.game_database, self.player_database),
            BankInterestService(self.game_database, self.player_database),
        )

    def deposit(self, *args: Any) -> Any:
        return self._services()[0].deposit(*args)

    def withdraw(self, *args: Any) -> Any:
        return self._services()[1].withdraw(*args)

    def upgrade(self, *args: Any) -> Any:
        return self._services()[2].upgrade(*args)

    def settle_interest(self, *args: Any) -> Any:
        return self._services()[3].settle(*args)


__all__ = ["BankRepository", "LegacyBankRepository"]
