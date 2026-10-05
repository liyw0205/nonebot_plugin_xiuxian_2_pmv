from pathlib import Path


SOURCE_ROOT = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"


def test_default_unseal_info_handler_uses_feature_stats_snapshot():
    source = (SOURCE_ROOT / "xiuxian" / "xiuxian_dufang" / "__init__.py").read_text(
        encoding="utf-8"
    )
    start = source.index("async def unseal_message_")
    end = source.index("def _draw_unseal_resolution", start)
    handler = source[start:end]

    assert "dufang_application.player_stats_snapshot(" in handler
    assert "get_unseal_data(" not in handler
    assert "_player_data_manager()" not in handler
