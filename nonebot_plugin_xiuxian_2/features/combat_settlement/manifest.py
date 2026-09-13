from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="combat_settlement",
    title="地图战斗结算",
    owner="gameplay",
    commands=(CommandSpec("节点战斗结算", aliases=("战斗结算",), permission="user"),),
    routes=(RouteSpec("/api/v1/combat/settle", methods=("POST",), permission="user"),),
    config=(ConfigSpec("combat_settlement_enabled", "bool", default=True, reloadable=True, description="地图战斗结算新实现灰度开关"),),
    migration_version="combat_settlement.001",
    test_tag="combat_settlement",
)

__all__ = ["FEATURE"]
