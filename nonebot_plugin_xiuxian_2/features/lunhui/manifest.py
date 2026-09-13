from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="lunhui",
    title="Reincarnation",
    owner="gameplay",
    commands=commands_for("lunhui"),
    config=(ConfigSpec("lunhui_enabled", "bool", default=True, reloadable=True, description="Enable Reincarnation"),),
    migration_version="legacy.lunhui.001",
    test_tag="lunhui",
)

__all__ = ["FEATURE"]
