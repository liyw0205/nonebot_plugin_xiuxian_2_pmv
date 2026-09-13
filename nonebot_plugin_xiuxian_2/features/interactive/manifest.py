from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="interactive",
    title="Interactive rewards",
    owner="gameplay",
    commands=commands_for("interactive"),
    config=(ConfigSpec("interactive_enabled", "bool", default=True, reloadable=True, description="Enable Interactive rewards"),),
    migration_version="interactive.001",
    test_tag="interactive",
)

__all__ = ["FEATURE"]
