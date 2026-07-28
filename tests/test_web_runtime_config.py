import importlib.util
from pathlib import Path

import pytest

_MODULE_PATH = (
    Path(__file__).parents[1]
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_web"
    / "web_runtime.py"
)
_spec = importlib.util.spec_from_file_location("xiuxian_web_runtime", _MODULE_PATH)
assert _spec and _spec.loader
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
web_enabled_from_env = _module.web_enabled_from_env


def test_web_status_defaults_to_enabled(monkeypatch):
    monkeypatch.delenv("XIUXIAN_WEB_STATUS", raising=False)

    assert web_enabled_from_env() is True
    assert _module.os.environ["XIUXIAN_WEB_STATUS"] == "true"


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_web_status_accepts_enabled_values(monkeypatch, value):
    monkeypatch.setenv("XIUXIAN_WEB_STATUS", value)

    assert web_enabled_from_env() is True


@pytest.mark.parametrize("value", ["0", "false", "FALSE", "no", "off"])
def test_web_status_accepts_disabled_values(monkeypatch, value):
    monkeypatch.setenv("XIUXIAN_WEB_STATUS", value)

    assert web_enabled_from_env() is False


def test_web_status_rejects_invalid_value(monkeypatch):
    monkeypatch.setenv("XIUXIAN_WEB_STATUS", "maybe")

    with pytest.raises(ValueError, match="XIUXIAN_WEB_STATUS"):
        web_enabled_from_env()
