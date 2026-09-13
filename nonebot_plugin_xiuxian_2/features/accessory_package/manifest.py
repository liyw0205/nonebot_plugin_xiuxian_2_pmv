from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="accessory_package",
    title="饰品礼包",
    owner="gameplay",
    commands=(CommandSpec("使用饰品礼包", aliases=("开启饰品礼包",), permission="user"),),
    routes=(RouteSpec("/api/v1/accessory-package", methods=("POST",), permission="user"),),
    config=(ConfigSpec("accessory_package_enabled", "bool", default=True, reloadable=True, description="饰品礼包新实现灰度开关"),),
    migration_version="accessory_package.001",
    test_tag="accessory_package",
)

__all__ = ["FEATURE"]
