from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="database_backups",
    title="数据库备份与恢复",
    owner="operations",
    test_tag="database_backups",
)

__all__ = ["FEATURE"]
