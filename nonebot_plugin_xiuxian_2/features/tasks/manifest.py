from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="tasks",
    title="Tasks",
    owner="gameplay",
    commands=commands_for("tasks"),
    config=(ConfigSpec("tasks_enabled", "bool", default=True, reloadable=True, description="Enable Tasks"),),
    migration_version="legacy.tasks.001",
    test_tag="tasks",
)

__all__ = ["FEATURE"]
