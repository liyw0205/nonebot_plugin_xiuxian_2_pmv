from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_default_activity_pass_claim_uses_feature_application_and_registered_schema():
    service = (ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_activity/service.py").read_text(encoding="utf-8")
    plugin = (ROOT / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
    cli = (ROOT / "nonebot_plugin_xiuxian_2/cli.py").read_text(encoding="utf-8")
    assert "claim_application = _activity_pass_claim_application()" in service
    assert "claim_application.claim(" in service
    assert "claim_application.resume_pending(operation_id, uid)" in service
    assert 'Migration("activity_reward.006", "activity_pass_reward_claims"' in plugin
    assert 'Migration("activity_reward.007", "activity_pass_reward_legacy_receipts"' in plugin
    assert '"activity_reward.pass.claim": context.services["activity_pass_claim"].reconcile' in plugin
    assert '"activity_reward.pass.claim": activity_pass_claim.reconcile' in cli


def test_legacy_pass_claim_facade_cannot_attach_or_create_request_schema():
    source = (ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_activity/transaction_service.py").read_text(encoding="utf-8")
    facade = source[
        source.index("class ActivityPassClaimService:"):
        source.index("class ActivityPointShopPurchaseResult:")
    ]
    assert "ActivityPassClaimApplication" in facade
    assert "ATTACH DATABASE" not in facade
    assert "CREATE TABLE" not in facade
