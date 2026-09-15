from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod import random_voice


class FixedRandom:
    def choice(self, values):
        return "怼人"


def test_random_voice_uses_injected_random_source(monkeypatch):
    monkeypatch.setattr(random_voice, "runtime_random", FixedRandom())
    assert random_voice.runtime_random.choice(["绿茶", "御姐", "怼人"]) == "怼人"
