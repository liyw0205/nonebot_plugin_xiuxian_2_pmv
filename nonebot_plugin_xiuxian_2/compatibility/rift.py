from pathlib import Path
from ..features.rift.application import RiftApplication
from ._feature_facade import FeatureFacade


class RiftService(FeatureFacade):
    feature = "rift"
    def __init__(self, game_database: str | Path, player_database: str | Path):
        self.application = RiftApplication(game_database, player_database)


__all__ = ["RiftService"]
