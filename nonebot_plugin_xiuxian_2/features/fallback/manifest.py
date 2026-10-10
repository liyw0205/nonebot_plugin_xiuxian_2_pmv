from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="fallback",
    title="空消息兜底回复",
    owner="messaging",
    test_tag="fallback",
)

__all__ = ["FEATURE"]
