from ...bootstrap.registry import ConfigSpec, FeatureManifest
from ...compatibility.command_inventory import commands_for


FEATURE = FeatureManifest(
    key="impart_pk",
    title="Impart training",
    owner="gameplay",
    commands=commands_for("impart_pk"),
    config=(ConfigSpec("impart_pk_enabled", "bool", default=True, reloadable=True, description="Enable Impart training"),),
    migration_version="legacy.impart_pk.001",
    test_tag="impart_pk",
)

__all__ = ["FEATURE"]
