from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="fusion",
    title="Fusion",
    owner="gameplay",
    commands=commands_for("fusion"),
    config=(ConfigSpec("fusion_enabled", "bool", default=True, reloadable=True, description="Enable Fusion"),),
    migration_version="legacy.fusion.001",
    test_tag="fusion",
)

__all__ = ["FEATURE"]
