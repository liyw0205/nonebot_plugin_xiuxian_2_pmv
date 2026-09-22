from pathlib import Path


def test_mixelixir_application_does_not_construct_legacy_repository_by_default():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/mixelixir/application.py").read_text(encoding="utf-8")
    assert "return self.repository or LegacyMixelixirRepository" not in source
