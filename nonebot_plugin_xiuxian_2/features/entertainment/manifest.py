from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="entertainment",
    title="Entertainment",
    owner="gameplay",
    commands=commands_for("entertainment"),
    config=(ConfigSpec("entertainment_enabled", "bool", default=True, reloadable=True, description="Enable Entertainment"),),
    migration_version="legacy.entertainment.001",
    test_tag="entertainment",
)

__all__ = ["FEATURE"]
