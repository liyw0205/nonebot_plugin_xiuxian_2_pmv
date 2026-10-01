from pathlib import Path


ROOT = Path(__file__).parents[1]
SOURCE = (
    ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/xiuxian2_handle.py"
).read_text(encoding="utf-8")


def test_wishing_stone_conversion_uses_bounded_inventory_application():
    body = SOURCE.split("def convert_stone_to_wishing_stone", 1)[1].split(
        "def add_impart_exp_day", 1
    )[0]

    assert "PlayerInventoryApplication" in body
    assert "require_full=True" in body
    assert "if not grant.succeeded" in body
    assert "sql_message.send_back(" not in body
