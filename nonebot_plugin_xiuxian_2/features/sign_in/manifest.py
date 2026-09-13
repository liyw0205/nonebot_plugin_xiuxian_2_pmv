from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="sign_in",
    title="修仙签到",
    owner="gameplay",
    commands=(CommandSpec("修仙签到", aliases=("签到",), permission="user"),),
    routes=(RouteSpec("/api/v1/sign-in", methods=("POST",), permission="user"),),
    config=(
        ConfigSpec("sign_in_lower_limit", "int", default=100000, reloadable=True, description="签到灵石下限"),
        ConfigSpec("sign_in_upper_limit", "int", default=500000, reloadable=True, description="签到灵石上限"),
    ),
    migration_version="sign_in.001",
    test_tag="sign_in",
)

__all__ = ["FEATURE"]
