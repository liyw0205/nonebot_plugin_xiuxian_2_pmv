from pathlib import Path
from ..features.natal_treasure.application import NatalTreasureApplication
from ._feature_facade import FeatureFacade


class NatalTreasureService(FeatureFacade):
    feature = "natal_treasure"
    def __init__(self, player_database: str | Path, game_database: str | Path):
        self.application = NatalTreasureApplication(player_database, game_database)


__all__ = ["NatalTreasureService"]
