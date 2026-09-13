from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="tianti",
    title="Body cultivation compatibility",
    owner="gameplay",
    commands=commands_for("tianti"),
    config=(ConfigSpec("tianti_enabled", "bool", default=True, reloadable=True, description="Enable Body cultivation compatibility"),),
    migration_version="legacy.tianti.001",
    test_tag="tianti",
)

__all__ = ["FEATURE"]
