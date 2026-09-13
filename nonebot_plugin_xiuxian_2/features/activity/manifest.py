from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="activity",
    title="Activity lifecycle",
    owner="gameplay",
    commands=commands_for("activity"),
    config=(ConfigSpec("activity_enabled", "bool", default=True, reloadable=True, description="Enable Activity lifecycle"),),
    migration_version="legacy.activity.001",
    test_tag="activity",
)

__all__ = ["FEATURE"]
