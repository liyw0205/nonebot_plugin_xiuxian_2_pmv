from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="admin_asset",
    title="管理员资产调整",
    owner="operations",
    commands=(CommandSpec("神秘力量", aliases=("管理员灵石",), permission="admin"),),
    routes=(
        RouteSpec("/api/v1/admin/assets/item", methods=("POST",), permission="admin"),
        RouteSpec("/api/v1/admin/assets/stone", methods=("POST",), permission="admin"),
    ),
    config=(ConfigSpec("admin_asset_enabled", "bool", default=True, reloadable=True, description="管理员资产新实现灰度开关"),),
    migration_version="admin_asset.001",
    test_tag="admin_asset",
)

__all__ = ["FEATURE"]
