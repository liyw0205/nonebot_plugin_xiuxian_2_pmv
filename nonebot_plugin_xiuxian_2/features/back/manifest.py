from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec

_ACTIONS = ("open_package", "use_item", "change_equipment", "learn_skill", "repair", "use_pet_eggs", "alchemy", "unbind")
FEATURE = FeatureManifest(
    key="back", title="背包与物品", owner="gameplay",
    commands=(CommandSpec("背包", aliases=("使用物品", "装备"), permission="user"),),
    routes=tuple(RouteSpec(f"/api/v1/back/{action}", methods=("POST",), permission="user") for action in _ACTIONS),
    config=(ConfigSpec("back_enabled", "bool", default=True, reloadable=True, description="背包物品新事务灰度开关"),),
    migration_version="back.017", test_tag="back",
)

__all__ = ["FEATURE"]
