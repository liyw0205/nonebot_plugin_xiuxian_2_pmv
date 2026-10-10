from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="game_events",
    title="游戏事件统计投影",
    owner="gameplay",
    migration_version="game_events.001",
    test_tag="game_events",
)

__all__ = ["FEATURE"]
