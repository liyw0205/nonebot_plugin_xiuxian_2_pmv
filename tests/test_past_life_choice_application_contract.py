from pathlib import Path

def test_past_life_choice_handler_uses_feature_application_for_non_terminal_advance():
    source=(Path(__file__).parents[1]/'nonebot_plugin_xiuxian_2/xiuxian/xiuxian_past_life/past_life_events.py').read_text(encoding='utf-8')
    assert '_past_life_application.choice(' in source
