from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="past_life",
    title="Past life",
    owner="gameplay",
    commands=commands_for("past_life"),
    config=(ConfigSpec("past_life_enabled", "bool", default=True, reloadable=True, description="Enable Past life"),),
    migration_version="legacy.past_life.001",
    test_tag="past_life",
)

__all__ = ["FEATURE"]
