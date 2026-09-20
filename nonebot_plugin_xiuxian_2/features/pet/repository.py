from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class PetRepository(Protocol):
    def switch(self, *args: Any, **kwargs: Any) -> Any: ...
    def travel_claim(self, *args: Any, **kwargs: Any) -> Any: ...
    def travel_start(self, *args: Any, **kwargs: Any) -> Any: ...
    def feed(self, *args: Any, **kwargs: Any) -> Any: ...
    def hatch(self, *args: Any, **kwargs: Any) -> Any: ...
    def hatch_result(self, *args: Any, **kwargs: Any) -> Any: ...


class LegacyPetRepository:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def _services(self):
        from ...xiuxian.xiuxian_pet.transaction_service import PetTravelClaimService, PetTravelStartService, PetFeedService, PetHatchService

        return (PetTravelClaimService(self.game_database, self.player_database), PetTravelStartService(self.player_database), PetFeedService(self.game_database, self.player_database), PetHatchService(self.game_database, self.player_database))

    def travel_claim(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[0].claim(*args, **kwargs)

    def travel_start(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[1].start(*args, **kwargs)

    def feed(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[2].feed(*args, **kwargs)

    def hatch(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[3].hatch(*args, **kwargs)

    def hatch_result(self, *args: Any, **kwargs: Any) -> Any:
        return self._services()[3].get_result(*args, **kwargs)

    def switch(self, *args: Any, **kwargs: Any) -> Any:
        from ...xiuxian.xiuxian_pet.transaction_service import PetActiveSwitchService

        return PetActiveSwitchService(self.player_database).switch(*args, **kwargs)


__all__ = ["PetRepository", "LegacyPetRepository"]
