from pathlib import Path


def test_trade_application_does_not_construct_legacy_repository_by_default():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/trade/application.py").read_text(encoding="utf-8")
    assert "repository=repository, feature=\"trade\"" in source
    assert "repository or LegacyTradeFeatureRepository" not in source
