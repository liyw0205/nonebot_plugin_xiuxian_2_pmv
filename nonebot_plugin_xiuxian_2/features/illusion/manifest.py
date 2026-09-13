from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="illusion",
    title="Illusion choice",
    owner="gameplay",
    commands=commands_for("illusion"),
    config=(ConfigSpec("illusion_enabled", "bool", default=True, reloadable=True, description="Enable Illusion choice"),),
    migration_version="illusion.001",
    test_tag="illusion",
)

__all__ = ["FEATURE"]
