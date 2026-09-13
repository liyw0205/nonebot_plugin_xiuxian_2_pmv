from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="stone_gift",
    title="灵石赠送",
    owner="gameplay",
    commands=(CommandSpec("送灵石", permission="user"),),
    routes=(RouteSpec("/api/v1/stone-gift", methods=("POST",), permission="user"),),
    config=(ConfigSpec("stone_gift_fee_rate", "float", default=0.1, reloadable=True, description="灵石赠送手续费率"),),
    migration_version="stone_gift.002",
    test_tag="stone_gift",
)

__all__ = ["FEATURE"]
