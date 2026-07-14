from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_config import XiuConfig


def test_runtime_scope_only_allows_full_test_group() -> None:
    config = XiuConfig()

    assert config.put_bot == ["102569432", "900000021"]
    assert config.response_group is True
    assert config.shield_group == ["01DEF55E88575D870F0FBA68AD1D0D72"]
    assert config.shield_private is True
