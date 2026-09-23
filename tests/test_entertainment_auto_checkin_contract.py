from pathlib import Path

def test_newapi_auto_checkin_uses_feature_application_for_valid_account():
    source=(Path(__file__).parents[1]/'nonebot_plugin_xiuxian_2/xiuxian/xiuxian_entertainment/mod/newapi_store.py').read_text(encoding='utf-8')
    assert 'entertainment_application.toggle_auto_checkin(' in source
    assert 'save_accounts(qq_id, accounts)' not in source[source.index('def toggle_auto_checkin'):]
