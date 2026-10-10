from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="logs",
    title="运维日志查询",
    owner="operations",
    test_tag="logs",
)

__all__ = ["FEATURE"]
