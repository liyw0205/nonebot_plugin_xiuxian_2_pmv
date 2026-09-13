from ...bootstrap.registry import CommandSpec, ConfigSpec, FeatureManifest, RouteSpec


FEATURE = FeatureManifest(
    key="mixelixir",
    title="炼丹灵田与结算",
    owner="gameplay",
    commands=(CommandSpec("灵田收取", aliases=("灵田结算",), permission="user"),),
    routes=(
        RouteSpec("/api/v1/mixelixir/harvest", methods=("POST",), permission="user"),
        RouteSpec("/api/v1/mixelixir/settle", methods=("POST",), permission="user"),
    ),
    config=(ConfigSpec("mixelixir_enabled", "bool", default=True, reloadable=True, description="炼丹灵田新实现灰度开关"),),
    migration_version="mixelixir.001",
    test_tag="mixelixir",
)

__all__ = ["FEATURE"]
