from ...bootstrap.registry import CommandSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="package_reward",
    title="礼包奖励",
    owner="gameplay",
    commands=(CommandSpec("使用礼包", aliases=("开启礼包",), permission="user"),),
    routes=(RouteSpec("/api/v1/package-reward/open", methods=("POST",), permission="user"),),
    jobs=(),
    migration_version="package_reward.001",
    test_tag="package_reward",
)

__all__ = ["FEATURE"]
