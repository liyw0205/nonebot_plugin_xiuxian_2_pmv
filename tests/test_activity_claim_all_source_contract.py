from pathlib import Path


def test_activity_claim_all_handler_remains_feature_owned():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_activity/service.py").read_text(encoding="utf-8")
    assert "activity_claim_all_application.run(" in source
    assert "_activity_claim_all_service().run(" not in source
