from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod.anime_reaction import _pick_image_category


class FixedRandom:
    def choice(self, values):
        return values[0]


def test_anime_category_uses_injected_random_source():
    category, title = _pick_image_category("", FixedRandom())
    assert category
    assert title == "随机"
