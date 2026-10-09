from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="updater",
    title="版本检查与更新执行",
    owner="operations",
    test_tag="updater",
)

__all__ = ["FEATURE"]
