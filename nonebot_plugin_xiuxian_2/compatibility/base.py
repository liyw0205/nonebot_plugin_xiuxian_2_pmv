from pathlib import Path
from ..features.base.application import BaseApplication
from ._feature_facade import FeatureFacade


class BaseService(FeatureFacade):
    feature = "base"
    def __init__(self, game_database: str | Path, player_database: str | Path):
        self.application = BaseApplication(game_database, player_database)


__all__ = ["BaseService"]
