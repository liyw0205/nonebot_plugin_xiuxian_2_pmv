from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="training",
    title="Training",
    owner="gameplay",
    commands=commands_for("training"),
    config=(ConfigSpec("training_enabled", "bool", default=True, reloadable=True, description="Enable Training"),),
    migration_version="legacy.training.001",
    test_tag="training",
)

__all__ = ["FEATURE"]
