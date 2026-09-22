from pathlib import Path


def test_pet_application_does_not_eagerly_construct_unused_legacy_repository():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/features/pet/application.py").read_text(encoding="utf-8")
    assert "return self.repository or LegacyPetRepository" not in source
