from pathlib import Path


def test_rift_speedup_handler_uses_application():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_rift/__init__.py").read_text(encoding="utf-8")
    assert "rift_application.speedup(" in source
    assert "rift_application.execute_legacy_call" not in source
