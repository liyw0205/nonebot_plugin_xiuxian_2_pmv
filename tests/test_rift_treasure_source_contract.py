from pathlib import Path


def test_rift_treasure_handler_uses_feature_application():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/__init__.py"
    ).read_text(encoding="utf-8")
    handler = source[
        source.index("async def _roll_rift_event") : source.index(
            "async def _roll_rift_boss_event", source.index("async def _roll_rift_event")
        )
    ]
    assert "rift_application.roll_treasure(" in handler
    assert "get_treasure_info(" not in handler
