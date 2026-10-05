from ...bootstrap.registry import ConfigSpec, FeatureManifest, RouteSpec
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="info",
    title="Player information",
    owner="gameplay",
    commands=commands_for("info"),
    routes=(RouteSpec("/api/v1/info/users/search", methods=("GET",), permission="admin"),),
    config=(ConfigSpec("info_enabled", "bool", default=True, reloadable=True, description="Enable Player information"),),
    migration_version="legacy.info.001",
    test_tag="info",
)

__all__ = ["FEATURE"]
