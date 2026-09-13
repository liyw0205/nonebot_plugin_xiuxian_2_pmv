from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="world_events",
    title="世界事件奖励",
    owner="gameplay",
    commands=(CommandSpec("领取魔修奖励", permission="user"),),
    routes=(RouteSpec("/api/v1/world-events/demon/claim", methods=("POST",), permission="user"),),
    config=(ConfigSpec("world_events_enabled", "bool", default=True, reloadable=True, description="世界事件奖励新实现灰度开关"),),
    migration_version="world_events.001",
    test_tag="world_events",
)

__all__ = ["FEATURE"]
