from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="beg",
    title="Novice gifts",
    owner="gameplay",
    commands=commands_for("beg"),
    config=(ConfigSpec("beg_enabled", "bool", default=True, reloadable=True, description="Enable Novice gifts"),),
    migration_version="beg.001",
    test_tag="beg",
)

__all__ = ["FEATURE"]
