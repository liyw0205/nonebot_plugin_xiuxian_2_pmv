from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="scheduler",
    title="任务调度管理面",
    owner="operations",
    test_tag="scheduler",
)

__all__ = ["FEATURE"]
