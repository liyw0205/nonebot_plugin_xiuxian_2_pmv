from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod.pokemon_box import (
    _normalize_pokemon_query,
)


class FixedRandom:
    def randint(self, start, end):
        return 25


def test_pokemon_query_uses_injected_random_source():
    assert _normalize_pokemon_query("", FixedRandom()) == "25"
    assert _normalize_pokemon_query("皮卡丘", FixedRandom()) == "25"
