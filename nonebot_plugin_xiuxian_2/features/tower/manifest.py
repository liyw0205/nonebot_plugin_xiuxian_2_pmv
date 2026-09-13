from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="tower",
    title="通天塔资产结算",
    owner="gameplay",
    commands=(CommandSpec("爬塔", aliases=("通天塔兑换", "挑战通天塔"), permission="user"),),
    routes=(
        RouteSpec("/api/v1/tower/purchase", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/tower/settle", methods=("POST",), permission="user"),
    ),
    config=(ConfigSpec("tower_enabled", "bool", default=True, reloadable=True, description="通天塔新资产结算灰度开关"),),
    migration_version="tower.001",
    test_tag="tower",
)

__all__ = ["FEATURE"]
