from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod import minesweeper


class FixedClock:
    def now(self):
        from datetime import datetime, timezone

        return datetime(2026, 9, 16, 14, 15, 16, tzinfo=timezone.utc)


class FixedRandom:
    def shuffle(self, values):
        values.reverse()


def test_minesweeper_uses_runtime_clock_and_random(monkeypatch):
    monkeypatch.setattr(minesweeper, "runtime_clock", FixedClock())
    monkeypatch.setattr(minesweeper, "runtime_random", FixedRandom())

    game = minesweeper.MinesweeperGame("ms-test", "u1", "User", 5, 5, 3)
    minesweeper.plant_mines(game, 0, 0)

    assert game.create_time == "2026-09-16 14:15:16"
    assert game.last_action_time == "2026-09-16 14:15:16"
    assert game.board[0][0] != -1
    assert game.board[0][1] != -1
    assert game.board[1][0] != -1
    assert sum(cell == -1 for row in game.board for cell in row) == 3
