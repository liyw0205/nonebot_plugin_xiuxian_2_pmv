from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="sect",
    title="宗门成员与资产操作",
    owner="gameplay",
    commands=(CommandSpec("加入宗门", permission="user"), CommandSpec("宗门商店兑换", permission="user"), CommandSpec("学习宗门功法", permission="user"), CommandSpec("领取宗门炼体堂", permission="user")),
    routes=(RouteSpec("/api/v1/sect/join", methods=("POST",), permission="user"), RouteSpec("/api/v1/sect/purchase", methods=("POST",), permission="user"), RouteSpec("/api/v1/sect/learn-main", methods=("POST",), permission="user"), RouteSpec("/api/v1/sect/learn-secondary", methods=("POST",), permission="user"), RouteSpec("/api/v1/sect/elixir/claim", methods=("POST",), permission="user")),
    config=(ConfigSpec("sect_enabled", "bool", default=True, reloadable=True, description="宗门资产动作灰度开关"),),
    migration_version="sect.001",
    test_tag="sect",
)

__all__ = ["FEATURE"]
