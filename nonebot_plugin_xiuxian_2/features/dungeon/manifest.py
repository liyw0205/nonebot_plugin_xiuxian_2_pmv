from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="dungeon",
    title="副本探索与兑换",
    owner="gameplay",
    commands=(CommandSpec("副本兑换", permission="user"), CommandSpec("探索副本", permission="user")),
    routes=(RouteSpec("/api/v1/dungeon/purchase", methods=("POST",), permission="user"), RouteSpec("/api/v1/dungeon/explore/replay", methods=("POST",), permission="user"), RouteSpec("/api/v1/dungeon/explore/prepare", methods=("POST",), permission="user"), RouteSpec("/api/v1/dungeon/explore/settle", methods=("POST",), permission="user")),
    config=(ConfigSpec("dungeon_enabled", "bool", default=True, reloadable=True, description="副本新资产边界灰度开关"),),
    migration_version="dungeon.001",
    test_tag="dungeon",
)

__all__ = ["FEATURE"]
