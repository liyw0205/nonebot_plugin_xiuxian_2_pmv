from pathlib import Path
from ..features.trade.application import TradeApplication
from ._feature_facade import FeatureFacade


class TradeService(FeatureFacade):
    feature = "trade"
    def __init__(self, game_database: str | Path, trade_database: str | Path):
        self.application = TradeApplication(game_database, trade_database)


__all__ = ["TradeService"]
