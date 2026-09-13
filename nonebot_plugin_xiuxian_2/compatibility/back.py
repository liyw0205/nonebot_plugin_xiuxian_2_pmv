from pathlib import Path
from ..features.back.application import BackApplication
from ._feature_facade import FeatureFacade


class BackService(FeatureFacade):
    feature = "back"
    def __init__(self, game_database: str | Path, player_database: str | Path | None = None):
        self.application = BackApplication(game_database, player_database)


__all__ = ["BackService"]
