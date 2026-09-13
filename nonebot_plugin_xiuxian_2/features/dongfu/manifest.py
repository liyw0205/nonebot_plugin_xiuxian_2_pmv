from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="dongfu",
    title="Cave dwelling",
    owner="gameplay",
    commands=commands_for("dongfu"),
    config=(ConfigSpec("dongfu_enabled", "bool", default=True, reloadable=True, description="Enable Cave dwelling"),),
    migration_version="legacy.dongfu.001",
    test_tag="dongfu",
)

__all__ = ["FEATURE"]
