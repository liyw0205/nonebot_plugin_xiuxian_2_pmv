from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="tianti_settlement",
    title="炼体结算",
    owner="gameplay",
    commands=(CommandSpec("炼体结算", aliases=("炼体收获",), permission="user"),),
    routes=(RouteSpec("/api/v1/tianti/settle", methods=("POST",), permission="user"),),
    config=(ConfigSpec("tianti_settlement_enabled", "bool", default=True, reloadable=True, description="炼体结算新实现灰度开关"),),
    migration_version="tianti_settlement.001",
    test_tag="tianti_settlement",
)

__all__ = ["FEATURE"]
