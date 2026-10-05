from __future__ import annotations

import tempfile
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.compensation.migrations import (
    apply_compensation_reward_claim_schema,
)
from nonebot_plugin_xiuxian_2.features.compensation.reward_claim_repository import (
    CompensationRewardClaimSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import build_migrations
from scripts.check_full_refactor_progress import _slice_status


def test_compensation_claim_schema_is_registered_as_followup_legacy_migration():
    versions = [migration.version for migration in build_migrations()]
    assert "legacy.compensation.001" in versions
    assert "legacy.compensation.002" in versions
    assert "legacy.compensation.003" in versions
    assert "legacy.compensation.004" in versions
    assert "legacy.compensation.005" in versions
    assert "legacy.compensation.006" in versions
    assert "legacy.compensation.007" in versions
    assert versions == sorted(set(versions))


def test_claim_schema_migration_preserves_existing_rows():
    with tempfile.TemporaryDirectory() as temp:
        database = Path(temp) / "game.db"
        with DatabaseUnitOfWork(database) as uow:
            uow.execute(
                "CREATE TABLE reward_claims("
                "reward_type TEXT NOT NULL,record_id TEXT NOT NULL,user_id TEXT NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                "PRIMARY KEY(reward_type,record_id,user_id))"
            )
            uow.execute(
                "INSERT INTO reward_claims(reward_type,record_id,user_id) VALUES(?,?,?)",
                ("补偿", "old", "u"),
            )
            apply_compensation_reward_claim_schema(uow)

        repository = CompensationRewardClaimSqlRepository(database, 99)
        assert repository.has_claimed("补偿", "old", "u")


def test_progress_gate_covers_compensation_request_schema_boundary():
    status = _slice_status()["compensation"]
    assert status["claim_schema_migration_owned"]
    assert status["claim_request_path_has_no_ddl"]
    assert status["claim_schema_checked_read_only"]
    assert status["redeem_entry_handles_schema_missing"]
    assert status["invitation_application_owned"]
    assert status["invitation_request_path_has_no_ddl"]
    assert status["invitation_schema_migration_owned"]
    assert status["invitation_schema_checked"]
    assert status["invitation_snapshot_migration_owned"]
    assert status["invitation_request_path_has_no_json"]
    assert status["invitation_binding_application_owned"]
    assert status["invitation_binding_projection_owned"]
    assert status["invitation_definition_migration_owned"]
    assert status["invitation_definition_application_owned"]
    assert status["invitation_definition_schema_checked"]
    assert status["definition_schema_migration_owned"]
    assert status["definition_request_path_has_no_ddl"]
    assert status["definition_legacy_migration_receipt_checked"]
    assert status["reward_catalog_migration_owned"]
    assert status["reward_definition_runtime_sql_owned"]
    assert status["reward_definition_request_path_has_no_ddl"]
    assert status["reward_runtime_does_not_write_json"]
    assert status["claim_precheck_uses_point_lookup"]
    assert status["item_catalog_construction_is_deferred"]
    assert status["redeem_claim_checks_definition_version"]
    assert status["reward_migration_reconciles_sql_claims"]
    assert status["reward_web_counts_use_sql_aggregates"]
    assert status["reward_web_definition_saves_use_sql"]
    assert status["reward_web_delete_clear_report_sql_failures"]
    assert status["claim_delete_is_atomic_with_definition"]
    assert status["reward_inventory_application_owned"]
    assert status["reward_inventory_legacy_writer_disabled"]
