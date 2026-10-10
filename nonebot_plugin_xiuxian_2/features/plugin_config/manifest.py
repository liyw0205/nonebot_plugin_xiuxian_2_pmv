from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="plugin_config",
    title="插件配置管理",
    owner="operations",
    test_tag="plugin_config",
)

__all__ = ["FEATURE"]
