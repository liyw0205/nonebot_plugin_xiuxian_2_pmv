from pathlib import Path


def test_natal_treasure_application_does_not_eagerly_construct_legacy_repository():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/natal_treasure/application.py").read_text(encoding="utf-8")
    assert "repository=repository, feature=\"natal_treasure\"" in source
    assert "repository or LegacyNatalTreasureRepository" not in source
