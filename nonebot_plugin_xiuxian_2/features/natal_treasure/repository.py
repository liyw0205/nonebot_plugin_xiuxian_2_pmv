from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class NatalTreasureRepository(Protocol):
    def awaken(self, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...
    def reawaken(self, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...
    def train(self, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...
    def upgrade(self, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...
    def engrave(self, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...
    def forget(self, operation_id: str, user_id: str, **kwargs: Any) -> Any: ...


class LegacyNatalTreasureRepository:
    def __init__(self, player_database: str | Path, game_database: str | Path) -> None:
        self.player_database = str(player_database)
        self.game_database = str(game_database)

    def _service(self, name: str):
        from ...xiuxian.xiuxian_natal_treasure.transaction_service import (
            AwakenService, EffectUpgradeService, EngravingService, ForgetEffectService,
            NatalTrainingService, ReawakenService,
        )
        classes = {"awaken": AwakenService, "reawaken": ReawakenService, "train": NatalTrainingService, "upgrade": EffectUpgradeService, "engrave": EngravingService, "forget": ForgetEffectService}
        cls = classes[name]
        return cls(self.player_database) if name == "awaken" else cls(self.game_database, self.player_database)

    def _invoke(self, name: str, operation_id: str, user_id: str, **kwargs: Any) -> Any:
        return getattr(self._service(name), name if name not in {"train", "upgrade", "engrave", "forget"} else {"train": "train", "upgrade": "upgrade", "engrave": "engrave", "forget": "forget"}[name])(operation_id, user_id, **kwargs)

    def awaken(self, operation_id, user_id, **kwargs): return self._invoke("awaken", operation_id, user_id, **kwargs)
    def reawaken(self, operation_id, user_id, **kwargs): return self._invoke("reawaken", operation_id, user_id, **kwargs)
    def train(self, operation_id, user_id, **kwargs): return self._invoke("train", operation_id, user_id, **kwargs)
    def upgrade(self, operation_id, user_id, **kwargs): return self._invoke("upgrade", operation_id, user_id, **kwargs)
    def engrave(self, operation_id, user_id, **kwargs): return self._invoke("engrave", operation_id, user_id, **kwargs)
    def forget(self, operation_id, user_id, **kwargs): return self._invoke("forget", operation_id, user_id, **kwargs)


__all__ = ["LegacyNatalTreasureRepository", "NatalTreasureRepository"]
