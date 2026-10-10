from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="economy_ledger",
    title="经济流水查询",
    owner="operations",
    migration_version="economy_ledger.001",
    test_tag="economy_ledger",
)

__all__ = ["FEATURE"]
