class MemoryRoomApplication:
    def __init__(self, states):
        self.states = {game_type: list(values) for game_type, values in states.items()}
        self.writes = []
        self.deletes = []

    def room_states(self, game_type):
        return list(self.states.get(game_type, ()))

    def save_room_state(self, game_type, room_id, state):
        self.writes.append((game_type, room_id, state))

    def delete_room_state(self, game_type, room_id):
        self.deletes.append((game_type, room_id))
        return True


def _game_modules():
    import nonebot

    try:
        nonebot.get_driver()
    except ValueError:
        nonebot.init()
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment.mod import (
        gomoku,
        half_ten,
        minesweeper,
    )

    return gomoku, half_ten, minesweeper


def test_game_room_managers_restore_their_own_schema_and_user_index():
    gomoku, half_ten, minesweeper = _game_modules()
    gomoku_game = gomoku.GomokuGame("same-room", "u1", "One")
    half_game = half_ten.HalfTenGame("same-room", "u2", "Two")
    mines_game = minesweeper.MinesweeperGame("same-room", "u3", "Three", 3, 3, 2)
    app = MemoryRoomApplication({
        "gomoku": [gomoku_game.to_dict()],
        "half_ten": [half_game.to_dict()],
        "minesweeper": [mines_game.to_dict()],
    })

    gomoku_manager = gomoku.GomokuRoomManager(app)
    half_manager = half_ten.HalfTenRoomManager(app)
    mines_manager = minesweeper.MinesweeperManager(app)
    gomoku_manager.load_rooms()
    half_manager.load_rooms()
    mines_manager.load_rooms()

    assert gomoku_manager.get_user_room("u1") == "same-room"
    assert half_manager.get_user_room("u2") == "same-room"
    assert mines_manager.get_user_game("u3").game_id == "same-room"
    assert gomoku_manager.get_room("same-room").board == gomoku_game.board
    assert half_manager.get_room("same-room").players == half_game.players
    assert mines_manager.games["same-room"].board == mines_game.board


def test_game_room_managers_write_and_delete_with_explicit_type_scope():
    gomoku, half_ten, minesweeper = _game_modules()
    app = MemoryRoomApplication({})
    gomoku_manager = gomoku.GomokuRoomManager(app)
    half_manager = half_ten.HalfTenRoomManager(app)
    mines_manager = minesweeper.MinesweeperManager(app)
    gomoku_manager.rooms["g"] = gomoku.GomokuGame("g", "u1", "One")
    half_manager.rooms["h"] = half_ten.HalfTenGame("h", "u2", "Two")
    mines_manager.games["m"] = minesweeper.MinesweeperGame("m", "u3", "Three", 3, 3, 2)

    gomoku_manager.save_room("g")
    half_manager.save_room("h")
    mines_manager.save("m")
    gomoku_manager.delete_room("g")
    half_manager.delete_room("h")
    mines_manager.delete("m")

    assert [(game_type, room_id) for game_type, room_id, _ in app.writes] == [
        ("gomoku", "g"),
        ("half_ten", "h"),
        ("minesweeper", "m"),
    ]
    assert app.deletes == [
        ("gomoku", "g"),
        ("half_ten", "h"),
        ("minesweeper", "m"),
    ]
