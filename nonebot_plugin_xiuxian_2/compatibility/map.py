from pathlib import Path
from ..features.map.application import MapApplication
from ._feature_facade import FeatureFacade


class MapService(FeatureFacade):
    feature = "map"
    def __init__(self, game_database: str | Path, player_database: str | Path):
        self.application = MapApplication(game_database, player_database)


__all__ = ["MapService"]
