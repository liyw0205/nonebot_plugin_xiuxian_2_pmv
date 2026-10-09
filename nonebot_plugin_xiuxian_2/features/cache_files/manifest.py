from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="cache_files",
    title="运行缓存文件下载",
    owner="operations",
    test_tag="cache_files",
)

__all__ = ["FEATURE"]
