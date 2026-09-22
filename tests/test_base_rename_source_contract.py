from pathlib import Path


def test_base_rename_defaults_to_feature_owned_repository():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/base/application.py").read_text(encoding="utf-8")
    assert "BaseRenameSqlRepository" in source
    assert "if self.repository is None" in source
