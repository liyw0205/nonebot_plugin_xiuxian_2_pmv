from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod import half_ten


class FixedClock:
    def now(self):
        from datetime import datetime, timezone

        return datetime(2026, 9, 16, 13, 14, 15, tzinfo=timezone.utc)


class FixedRandom:
    def shuffle(self, values):
        values.reverse()


def test_half_ten_game_uses_runtime_clock_and_random(monkeypatch):
    monkeypatch.setattr(half_ten, "runtime_clock", FixedClock())
    monkeypatch.setattr(half_ten, "runtime_random", FixedRandom())

    game = half_ten.HalfTenGame("room", "u1", "User")
    game.players.append("u2")
    game.player_names["u2"] = "Other"
    result = game.start_and_settle()

    assert game.create_time == "2026-09-16 13:14:15"
    assert game.start_time == "2026-09-16 13:14:15"
    assert set(game.cards) == {"u1", "u2"}
    assert game.status == "finished"
