from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="simulator",
    title="Simulator",
    owner="gameplay",
    commands=commands_for("simulator"),
    config=(ConfigSpec("simulator_enabled", "bool", default=True, reloadable=True, description="Enable Simulator"),),
    migration_version="legacy.simulator.001",
    test_tag="simulator",
)

__all__ = ["FEATURE"]
