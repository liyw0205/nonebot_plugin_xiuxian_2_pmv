from pathlib import Path

def test_past_life_final_handler_uses_feature_application_by_default():
    source=(Path(__file__).parents[1]/'nonebot_plugin_xiuxian_2/xiuxian/xiuxian_past_life/past_life_events.py').read_text(encoding='utf-8')
    assert '_past_life_application.final_settle(' in source
