from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="impart",
    title="Impart cards",
    owner="gameplay",
    commands=commands_for("impart"),
    config=(ConfigSpec("impart_enabled", "bool", default=True, reloadable=True, description="Enable Impart cards"),),
    migration_version="legacy.impart.001",
    test_tag="impart",
)

__all__ = ["FEATURE"]
