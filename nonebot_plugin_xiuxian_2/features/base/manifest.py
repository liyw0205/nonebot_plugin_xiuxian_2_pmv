from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec

_ACTIONS = ("breakthrough", "tribulation", "rename", "stone_contest", "stone_robbery", "sign")
FEATURE = FeatureManifest(
    key="base", title="修炼与基础资产", owner="gameplay",
    commands=(CommandSpec("修炼", aliases=("突破",), permission="user"),),
    routes=tuple(RouteSpec(f"/api/v1/base/{action}", methods=("POST",), permission="user") for action in _ACTIONS)
    + (
        RouteSpec("/api/v1/base/xiangyuan/create", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/base/xiangyuan/claim", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/base/xiangyuan/group", methods=("GET",), permission="user"),
    ),
    config=(ConfigSpec("base_enabled", "bool", default=True, reloadable=True, description="基础修炼事务灰度开关"),),
    migration_version="base.008", test_tag="base",
)

__all__ = ["FEATURE"]
