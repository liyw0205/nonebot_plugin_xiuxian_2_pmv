from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="status",
    title="Status",
    owner="gameplay",
    commands=commands_for("status"),
    config=(ConfigSpec("status_enabled", "bool", default=True, reloadable=True, description="Enable Status"),),
    migration_version="legacy.status.001",
    test_tag="status",
)

__all__ = ["FEATURE"]
