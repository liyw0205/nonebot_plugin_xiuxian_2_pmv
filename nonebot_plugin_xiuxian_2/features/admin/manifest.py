from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="admin",
    title="Administration",
    owner="gameplay",
    commands=commands_for("admin"),
    config=(ConfigSpec("admin_enabled", "bool", default=True, reloadable=True, description="Enable Administration"),),
    migration_version="legacy.admin.001",
    test_tag="admin",
)

__all__ = ["FEATURE"]
