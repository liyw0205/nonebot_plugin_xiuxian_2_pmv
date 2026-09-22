from pathlib import Path


def test_activity_reward_application_defaults_to_claim_all_application():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/activity_reward/application.py").read_text(encoding="utf-8")
    assert "ActivityClaimAllApplication" in source
    assert "self.repository or LegacyActivityRewardRepository" not in source
