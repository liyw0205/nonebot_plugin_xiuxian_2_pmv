from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="manual_backups",
    title="手动整包备份",
    owner="operations",
    test_tag="manual_backups",
)

__all__ = ["FEATURE"]
