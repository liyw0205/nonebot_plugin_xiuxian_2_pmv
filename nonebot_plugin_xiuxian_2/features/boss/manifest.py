from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="boss",
    title="世界BOSS资产结算",
    owner="gameplay",
    commands=(CommandSpec("世界BOSS兑换", permission="user"), CommandSpec("讨伐世界BOSS", permission="user")),
    routes=(RouteSpec("/api/v1/boss/purchase", methods=("POST",), permission="user"), RouteSpec("/api/v1/boss/settle", methods=("POST",), permission="user")),
    config=(ConfigSpec("boss_enabled", "bool", default=True, reloadable=True, description="世界BOSS资产结算灰度开关"),),
    migration_version="boss.001",
    test_tag="boss",
)

__all__ = ["FEATURE"]
