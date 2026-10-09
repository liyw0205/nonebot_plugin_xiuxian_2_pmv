from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="messages",
    title="Web 消息发送",
    owner="operations",
    test_tag="messages",
)

__all__ = ["FEATURE"]
