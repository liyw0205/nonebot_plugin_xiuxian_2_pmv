from datetime import datetime, timezone

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod.game_utils import now_text


class FixedClock:
    def now(self):
        return datetime(2026, 9, 16, 12, 34, 56, tzinfo=timezone.utc)


def test_now_text_uses_injected_clock():
    assert now_text(FixedClock()) == "2026-09-16 12:34:56"
