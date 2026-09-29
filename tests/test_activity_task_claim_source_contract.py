from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_default_activity_task_claim_uses_feature_application_and_startup_schema():
    service = (ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_activity/service.py").read_text(encoding="utf-8")
    plugin = (ROOT / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    assert "ActivityTaskClaimApplication" in service
    assert "_activity_task_claim_application().claim(" in service
    assert 'Migration("activity_reward.004", "activity_task_reward_claims"' in plugin
    assert 'Migration("activity_reward.005", "activity_task_reward_legacy_receipts"' in plugin


def test_legacy_task_claim_facade_has_no_cross_database_write_transaction():
    source = (ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_activity/transaction_service.py").read_text(encoding="utf-8")
    claim = source[
        source.index("class ActivityTaskClaimService:"):
        source.index("class ActivityPassClaimService:")
    ]
    assert "ActivityTaskClaimApplication" in claim
    assert "ATTACH DATABASE" not in claim
    assert "CREATE TABLE" not in claim
