from pathlib import Path


def test_rift_damage_handler_uses_feature_application_resolver():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/__init__.py"
    ).read_text(encoding="utf-8")
    start = source.index("async def _roll_rift_event")
    end = source.index("async def _roll_rift_boss_event", start)
    handler = source[start:end]
    assert "rift_application.roll_damage_event(" in handler
    assert "get_dxsj_info(" not in handler
