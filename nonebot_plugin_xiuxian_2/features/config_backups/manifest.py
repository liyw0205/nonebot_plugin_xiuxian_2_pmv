from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="config_backups",
    title="配置文件备份",
    owner="operations",
    test_tag="config_backups",
)

__all__ = ["FEATURE"]
