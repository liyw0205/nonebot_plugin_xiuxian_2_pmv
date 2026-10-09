from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="database_console",
    title="数据库控制台",
    owner="operations",
    test_tag="database_console",
)

__all__ = ["FEATURE"]
