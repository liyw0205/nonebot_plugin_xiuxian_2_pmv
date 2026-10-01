from ...bootstrap.registry import FeatureManifest


FEATURE = FeatureManifest(
    key="player_state",
    title="Player vital state",
    owner="gameplay",
    test_tag="player_state",
)

__all__ = ["FEATURE"]
