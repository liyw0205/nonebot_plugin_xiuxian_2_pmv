from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="work",
    title="悬赏令接取",
    owner="gameplay",
    commands=(CommandSpec("悬赏令", aliases=("悬赏令接取", "悬赏令结算"), permission="user"),),
    routes=(
        RouteSpec("/api/v1/work/claim", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/work/settle", methods=("POST",), permission="user"),
    ),
    config=(ConfigSpec("work_claim_enabled", "bool", default=True, reloadable=True, description="悬赏令接取新实现灰度开关"),),
    migration_version="work.001",
    test_tag="work",
)

__all__ = ["FEATURE"]
