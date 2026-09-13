from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="pet",
    title="宠物资产操作",
    owner="gameplay",
    commands=(CommandSpec("宠物喂食", permission="user"), CommandSpec("宠物游历", permission="user"), CommandSpec("领取宠物游历", permission="user"), CommandSpec("砸蛋", permission="user")),
    routes=(RouteSpec("/api/v1/pet/travel/claim", methods=("POST",), permission="user"), RouteSpec("/api/v1/pet/travel/start", methods=("POST",), permission="user"), RouteSpec("/api/v1/pet/feed", methods=("POST",), permission="user"), RouteSpec("/api/v1/pet/hatch", methods=("POST",), permission="user")),
    config=(ConfigSpec("pet_enabled", "bool", default=True, reloadable=True, description="宠物资产动作灰度开关"),),
    migration_version="pet.001",
    test_tag="pet",
)

__all__ = ["FEATURE"]
