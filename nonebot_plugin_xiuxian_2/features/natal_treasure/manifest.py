from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec

FEATURE = FeatureManifest(
    key="natal_treasure",
    title="本命法宝资产操作",
    owner="gameplay",
    commands=(CommandSpec("本命法宝", aliases=("觉醒本命法宝", "重塑本命法宝"), permission="user"),),
    routes=tuple(RouteSpec(f"/api/v1/natal-treasure/{action}", methods=("POST",), permission="user") for action in ("awaken", "reawaken", "train", "upgrade", "engrave", "forget")),
    config=(ConfigSpec("natal_treasure_enabled", "bool", default=True, reloadable=True, description="本命法宝新资产操作灰度开关"),),
    migration_version="natal_treasure.001",
    test_tag="natal_treasure",
)

__all__ = ["FEATURE"]
