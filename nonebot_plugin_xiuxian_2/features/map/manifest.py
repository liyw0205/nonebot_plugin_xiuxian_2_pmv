from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec
from ...compatibility.command_inventory import commands_for

_ACTIONS = ("move", "return_home", "interactive_start", "interactive_finish", "combat_start", "combat_settle", "explore_start", "explore_settle", "resource_reward", "mission_claim", "purchase_seed", "build_dongfu")
FEATURE = FeatureManifest(
    key="map", title="地图探索与战斗", owner="gameplay",
    commands=commands_for("map"),
    routes=tuple(RouteSpec(f"/api/v1/map/{action}", methods=("POST",), permission="user") for action in _ACTIONS),
    config=(ConfigSpec("map_enabled", "bool", default=True, reloadable=True, description="地图探索新事务灰度开关"),),
    migration_version="map.001", test_tag="map",
)

__all__ = ["FEATURE"]
