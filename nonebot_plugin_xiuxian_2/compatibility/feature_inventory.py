"""Manifest inventory for legacy feature packages during the migration cycle.

These declarations intentionally have no new commands/routes: the old package
still owns those registrations.  Listing the owners here makes the registry
and release tooling complete without pretending the old handlers are already
new application services.
"""

from __future__ import annotations

from ..bootstrap.registry import FeatureManifest


LEGACY_FEATURE_KEYS = (
    "xiuxian_activity",
    "xiuxian_admin",
    "xiuxian_arena",
    "xiuxian_back",
    "xiuxian_bank",
    "xiuxian_base",
    "xiuxian_beg",
    "xiuxian_boss",
    "xiuxian_buff",
    "xiuxian_compensation",
    "xiuxian_dongfu",
    "xiuxian_dufang",
    "xiuxian_dungeon",
    "xiuxian_entertainment",
    "xiuxian_fusion",
    "xiuxian_impart",
    "xiuxian_impart_pk",
    "xiuxian_info",
    "xiuxian_lunhui",
    "xiuxian_map",
    "xiuxian_mixelixir",
    "xiuxian_natal_treasure",
    "xiuxian_past_life",
    "xiuxian_pet",
    "xiuxian_puppet",
    "xiuxian_rift",
    "xiuxian_sect",
    "xiuxian_simulator",
    "xiuxian_status",
    "xiuxian_tasks",
    "xiuxian_tianti",
    "xiuxian_title",
    "xiuxian_trade",
    "xiuxian_training",
    "xiuxian_work",
    "xiuxian_world_events",
)


FEATURES = tuple(
    FeatureManifest(
        key=key,
        title=f"{key} 兼容层",
        owner="compatibility",
        test_tag=f"legacy:{key}",
        compatibility=True,
        migration_target=f"features/{key.removeprefix('xiuxian_').casefold()}",
        removal_release="next-major",
    )
    for key in LEGACY_FEATURE_KEYS
)


__all__ = ["FEATURES", "LEGACY_FEATURE_KEYS"]
