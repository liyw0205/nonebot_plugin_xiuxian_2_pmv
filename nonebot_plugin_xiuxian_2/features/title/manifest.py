from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="title",
    title="Titles",
    owner="gameplay",
    commands=commands_for("title"),
    config=(ConfigSpec("title_enabled", "bool", default=True, reloadable=True, description="Enable Titles"),),
    migration_version="title.001",
    test_tag="title",
)

__all__ = ["FEATURE"]
