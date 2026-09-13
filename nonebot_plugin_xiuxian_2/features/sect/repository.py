from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class SectRepository(Protocol):
    def join(self, *args: Any, **kwargs: Any) -> Any: ...
    def purchase(self, *args: Any, **kwargs: Any) -> Any: ...
    def learn_main(self, *args: Any, **kwargs: Any) -> Any: ...
    def learn_secondary(self, *args: Any, **kwargs: Any) -> Any: ...
    def claim_elixir(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacySectRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def _service(self, name: str):
        from ...xiuxian.xiuxian_sect.transaction_service import (
            SectMemberJoinService, SectShopPurchaseService, SectMainBuffLearnService,
            SectSecBuffLearnService, SectElixirClaimService,
        )
        return {"join": SectMemberJoinService, "purchase": SectShopPurchaseService, "learn_main": SectMainBuffLearnService, "learn_secondary": SectSecBuffLearnService, "claim_elixir": SectElixirClaimService}[name](self.database)

    def join(self, *args: Any, **kwargs: Any) -> Any:
        return self._service("join").join(*args, **kwargs)

    def purchase(self, *args: Any, **kwargs: Any) -> Any:
        return self._service("purchase").purchase(*args, **kwargs)

    def learn_main(self, *args: Any, **kwargs: Any) -> Any:
        return self._service("learn_main").learn(*args, **kwargs)

    def learn_secondary(self, *args: Any, **kwargs: Any) -> Any:
        return self._service("learn_secondary").learn(*args, **kwargs)

    def claim_elixir(self, *args: Any, **kwargs: Any) -> Any:
        return self._service("claim_elixir").claim(*args, **kwargs)


__all__ = ["SectRepository", "LegacySectRepository"]
