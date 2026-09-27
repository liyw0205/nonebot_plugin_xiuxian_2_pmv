from __future__ import annotations

from scripts.check_full_refactor_progress import _slice_status
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


def test_fairyland_claim_is_feature_owned_and_keeps_lazy_legacy_fallback():
    status = _slice_status()["sect"]
    assert status["fairyland_claim_application_owned"]
    assert status["fairyland_claim_repository_owned"]
    assert status["fairyland_claim_request_path_has_no_ddl"]
    assert status["fairyland_claim_player_migration_routed"]
    assert status["fairyland_claim_legacy_fallback_retained"]


def test_fairyland_claim_legacy_service_isolated_behind_compatibility_imports():
    status = _slice_status()["sect"]
    assert status["fairyland_claim_service_isolated"]
    assert status["fairyland_claim_rollback_import_isolated"]

    from nonebot_plugin_xiuxian_2.compatibility.legacy_sect_fairyland_claim import (
        FairylandClaimService as CompatibilityService,
    )
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.fairyland_claim_service import (
        FairylandClaimService as LegacyModuleService,
    )
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import (
        FairylandClaimService as TransactionService,
    )

    assert LegacyModuleService is CompatibilityService
    assert TransactionService is CompatibilityService


def test_fairyland_claim_schema_migration_is_player_only():
    migrations = build_migrations()
    game = {migration.version for migration in migrations_for_database(migrations, "game_db")}
    player = {migration.version for migration in migrations_for_database(migrations, "player_db")}
    assert "sect_fairyland.002" not in game
    assert "sect_fairyland.002" in player
