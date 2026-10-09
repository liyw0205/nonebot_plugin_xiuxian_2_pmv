from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="terminal",
    title="Web 终端与 PTY 会话",
    owner="operations",
    test_tag="terminal",
)

__all__ = ["FEATURE"]
