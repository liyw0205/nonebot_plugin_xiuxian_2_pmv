from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="puppet",
    title="灵田傀儡资产操作",
    owner="gameplay",
    commands=(CommandSpec("购买灵田傀儡", aliases=("灵田傀儡升级",), permission="user"),),
    routes=(
        RouteSpec("/api/v1/puppet/purchase", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/puppet/upgrade", methods=("POST",), permission="user"),
    ),
    config=(ConfigSpec("puppet_enabled", "bool", default=True, reloadable=True, description="灵田傀儡新资产操作灰度开关"),),
    migration_version="puppet.001",
    test_tag="puppet",
)

__all__ = ["FEATURE"]
