from pathlib import Path


def test_base_application_does_not_construct_unused_legacy_repository_by_default():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/base/application.py").read_text(encoding="utf-8")
    assert "repository=repository, feature=\"base\"" in source
    assert "repository or LegacyBaseRepository" not in source
