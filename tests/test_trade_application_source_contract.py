from pathlib import Path


def test_trade_application_does_not_construct_legacy_repository_by_default():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/trade/application.py").read_text(encoding="utf-8")
    assert "repository=repository, feature=\"trade\"" in source
    assert "repository or LegacyTradeFeatureRepository" not in source


def test_legacy_auction_session_service_is_confined_to_rollback_adapter():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2"
    repository = (root / "features/trade/repository.py").read_text(encoding="utf-8")
    compatibility = (root / "compatibility/legacy_trade_auction_sessions.py").read_text(encoding="utf-8")
    transaction_service = (root / "xiuxian/xiuxian_trade/transaction_service.py").read_text(encoding="utf-8")

    assert "AuctionSessionService" not in repository
    assert "LegacyTradeAuctionSessionAdapter" in repository
    assert "AuctionSessionService" in compatibility
    assert "AuctionSessionStartSqlRepository" in compatibility
    assert "AuctionSettlementSqlRepository" in compatibility
    assert "transaction_service" not in compatibility
    assert "class AuctionSessionService" not in transaction_service


def test_legacy_settlement_adapter_uses_feature_repository_without_old_dependencies():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2"
    source = (root / "features/auction/settlement.py").read_text(encoding="utf-8")

    assert "AuctionSettlementSqlRepository" in source
    assert "_auction_dependencies" not in source
    assert "transaction_service" not in source


def test_legacy_auction_bid_adapter_uses_feature_repository_without_old_trade_repository():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2"
    source = (root / "features/auction/repository.py").read_text(encoding="utf-8")
    trade_facade = (root / "xiuxian/xiuxian_trade/__init__.py").read_text(
        encoding="utf-8"
    )

    assert "AuctionBidSqlRepository" in source
    assert "xiuxian.xiuxian_trade.repository" not in source
    assert "TradeRepository(" not in source
    assert "auction_repository=_auction_bid_repository" in trade_facade
