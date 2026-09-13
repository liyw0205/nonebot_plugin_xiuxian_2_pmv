from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="arena",
    title="竞技场资产结算",
    owner="gameplay",
    commands=(
        CommandSpec("竞技场挑战", aliases=("竞技场购买次数",), permission="user"),
        CommandSpec("竞技场兑换", aliases=("竞技场商店",), permission="user"),
    ),
    routes=(
        RouteSpec("/api/v1/arena/purchase", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/arena/challenge-purchase", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/arena/settle", methods=("POST",), permission="user"),
    ),
    config=(ConfigSpec("arena_enabled", "bool", default=True, reloadable=True, description="竞技场新资产结算灰度开关"),),
    migration_version="arena.001",
    test_tag="arena",
)

__all__ = ["FEATURE"]
