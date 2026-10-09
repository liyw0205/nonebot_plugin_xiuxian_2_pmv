from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="plugin_backups",
    title="插件整包备份与恢复",
    owner="operations",
    test_tag="plugin_backups",
)

__all__ = ["FEATURE"]
