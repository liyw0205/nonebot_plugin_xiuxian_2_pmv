from pathlib import Path


PLAYER_FIGHT_SOURCE = Path(
    "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/player_fight.py"
).read_text(encoding="utf-8")
RIFTMAKE_SOURCE = Path(
    "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/riftmake.py"
).read_text(encoding="utf-8")
HANDLE_SOURCE = Path(
    "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/xiuxian2_handle.py"
).read_text(encoding="utf-8")


def test_battle_modules_do_not_open_unused_impart_connection_at_import():
    for source in (PLAYER_FIGHT_SOURCE, RIFTMAKE_SOURCE):
        assert "XIUXIAN_IMPART_BUFF" not in source
        assert "xiuxian_impart =" not in source


def test_attribute_module_defers_legacy_impart_connection():
    assert "xiuxian_impart = XIUXIAN_IMPART_BUFF()" not in HANDLE_SOURCE
    assert "class _LazyImpartProxy" in HANDLE_SOURCE
    assert "xiuxian_impart = _LazyImpartProxy()" in HANDLE_SOURCE
