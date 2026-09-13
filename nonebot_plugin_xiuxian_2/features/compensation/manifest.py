from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="compensation",
    title="Compensation",
    owner="gameplay",
    commands=commands_for("compensation"),
    config=(ConfigSpec("compensation_enabled", "bool", default=True, reloadable=True, description="Enable Compensation"),),
    migration_version="legacy.compensation.001",
    test_tag="compensation",
)

__all__ = ["FEATURE"]
