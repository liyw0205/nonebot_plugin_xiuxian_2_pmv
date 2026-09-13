from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec

_ACTIONS = ("open", "upgrade_field", "rename", "training_start", "training_complete", "stone_training", "pvp_settle")
FEATURE = FeatureManifest(
    key="buff", title="功法与洞天福地", owner="gameplay",
    commands=(CommandSpec("功法", aliases=("洞天福地购买", "洞天福地查看"), permission="user"),),
    routes=tuple(RouteSpec(f"/api/v1/buff/{action}", methods=("POST",), permission="user") for action in _ACTIONS),
    config=(ConfigSpec("buff_enabled", "bool", default=True, reloadable=True, description="功法和洞天福地新事务灰度开关"),),
    migration_version="buff.001", test_tag="buff",
)

__all__ = ["FEATURE"]
