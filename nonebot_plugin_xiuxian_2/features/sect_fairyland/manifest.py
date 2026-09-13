from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="sect_fairyland",
    title="宗门炼体堂修行",
    owner="gameplay",
    commands=(CommandSpec("宗门淬体修行", aliases=("淬体修行", "宗门炼体堂修行", "炼体堂修行", "宗门炼体堂领取"), permission="user"),),
    routes=(RouteSpec("/api/v1/sect/fairyland/claim", methods=("POST",), permission="user"),),
    config=(ConfigSpec("sect_fairyland_enabled", "bool", default=True, reloadable=True, description="宗门炼体堂新实现灰度开关"),),
    migration_version="sect_fairyland.001",
    test_tag="sect_fairyland",
)

__all__ = ["FEATURE"]
