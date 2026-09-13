from pathlib import Path
from ..features.buff.application import BuffApplication
from ._feature_facade import FeatureFacade


class BuffService(FeatureFacade):
    feature = "buff"
    def __init__(self, game_database: str | Path, player_database: str | Path):
        self.application = BuffApplication(game_database, player_database)


__all__ = ["BuffService"]
