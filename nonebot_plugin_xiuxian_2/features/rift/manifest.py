from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec

_ACTIONS = ("generate", "enter", "terminate", "event_settle", "speedup", "settle")
FEATURE = FeatureManifest(
    key="rift", title="裂隙世界", owner="gameplay",
    commands=(CommandSpec("裂隙", aliases=("进入裂隙", "裂隙探索"), permission="user"),),
    routes=tuple(RouteSpec(f"/api/v1/rift/{action}", methods=("POST",), permission="user") for action in _ACTIONS),
    config=(ConfigSpec("rift_enabled", "bool", default=True, reloadable=True, description="裂隙世界新事务灰度开关"),),
    migration_version="rift.001", test_tag="rift",
)

__all__ = ["FEATURE"]
