from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="daily_fortune",
    title="每日运势",
    owner="gameplay",
    config=(ConfigSpec("daily_fortune_enabled", "bool", default=True, reloadable=True, description="每日运势新实现灰度开关"),),
    commands=(CommandSpec("今日运势", aliases=("占卜", "卜卦", "求签", "运势", "算命"), permission="user"),),
    routes=(RouteSpec("/api/v1/daily-fortune", methods=("GET", "POST"), permission="user"),),
    jobs=(),
    migration_version="daily_fortune.001",
    test_tag="daily_fortune",
)


__all__ = ["FEATURE"]
