from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="maintenance",
    title="启动期存储整理",
    owner="operations",
    test_tag="maintenance",
)

__all__ = ["FEATURE"]
