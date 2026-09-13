from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="dufang",
    title="Gambling",
    owner="gameplay",
    commands=commands_for("dufang"),
    config=(ConfigSpec("dufang_enabled", "bool", default=True, reloadable=True, description="Enable Gambling"),),
    migration_version="legacy.dufang.001",
    test_tag="dufang",
)

__all__ = ["FEATURE"]
