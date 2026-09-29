from pathlib import Path


def test_activity_reward_application_defaults_to_claim_all_application():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/activity_reward/application.py").read_text(encoding="utf-8")
    assert "ActivityClaimAllApplication" in source
    assert "self.repository or LegacyActivityRewardRepository" not in source


def test_default_command_and_web_do_not_use_legacy_claim_coordinator():
    root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2"
    plugin = (root / "plugin.py").read_text(encoding="utf-8")
    services = plugin[plugin.index('"activity_reward": ActivityRewardApplication('):]
    assert "repository=LegacyActivityRewardRepository" not in services.split('"combat_settlement":', 1)[0]

    activity_repository = (root / "features/activity/repository.py").read_text(encoding="utf-8")
    claim_all = activity_repository.split("    def _claim_all(", 1)[1].split("    def _claim_tasks(", 1)[0]
    assert "ActivityClaimAllApplication(self.database).run(" in claim_all
    assert "service import claim_activity_rewards" not in claim_all

    service = (root / "xiuxian/xiuxian_activity/service.py").read_text(encoding="utf-8")
    assert "activity_claim_all_application = ActivityClaimAllApplication(get_paths().game_db)" in service
