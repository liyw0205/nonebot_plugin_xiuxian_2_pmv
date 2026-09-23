from pathlib import Path

def test_newapi_delete_uses_feature_application():
    source = (Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/newapi_store.py").read_text(encoding="utf-8")
    start = source.index("def delete_accounts(")
    end = source.index("\ndef resolve_targets", start)
    section = source[start:end]
    assert "entertainment_application.delete_accounts(" in section
    assert "_run_entertainment_write(" not in section
