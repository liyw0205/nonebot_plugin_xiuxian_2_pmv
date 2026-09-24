from pathlib import Path


def test_trade_application_does_not_construct_legacy_repository_by_default():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/trade/application.py").read_text(encoding="utf-8")
    assert "repository=repository, feature=\"trade\"" in source
    assert "repository or LegacyTradeFeatureRepository" not in source


def test_legacy_auction_session_service_is_confined_to_rollback_adapter():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2"
    repository = (root / "features/trade/repository.py").read_text(encoding="utf-8")
    compatibility = (root / "compatibility/legacy_trade_auction_sessions.py").read_text(encoding="utf-8")

    assert "AuctionSessionService" not in repository
    assert "LegacyTradeAuctionSessionAdapter" in repository
    assert "AuctionSessionService" in compatibility
