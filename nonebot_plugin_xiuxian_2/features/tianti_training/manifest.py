from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="tianti_training",
    title="炼体进阶",
    owner="gameplay",
    commands=(
        CommandSpec("灵石炼体", permission="user"),
        CommandSpec("炼体药浴", aliases=("药浴",), permission="user"),
        CommandSpec("炼体突破", permission="user"),
        CommandSpec("冲窍", permission="user"),
    ),
    routes=(
        RouteSpec("/api/v1/tianti/train", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/tianti/bath", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/tianti/breakthrough", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/tianti/qiaoxue", methods=("POST",), permission="user"),
    ),
    config=(ConfigSpec("tianti_training_enabled", "bool", default=True, reloadable=True, description="炼体进阶新实现灰度开关"),),
    migration_version="tianti_training.001",
    test_tag="tianti_training",
)

__all__ = ["FEATURE"]
