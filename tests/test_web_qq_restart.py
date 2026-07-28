import importlib.util
from pathlib import Path

_MODULE_PATH = (
    Path(__file__).parents[1]
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_web"
    / "qq_restart.py"
)
_spec = importlib.util.spec_from_file_location("xiuxian_web_qq_restart", _MODULE_PATH)
assert _spec and _spec.loader
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
detect_restart = _module.detect_restart


def _ignore_docker(monkeypatch):
    original = Path.exists
    monkeypatch.setattr(
        Path,
        "exists",
        lambda self: False if str(self) == "/.dockerenv" else original(self),
    )


def test_detect_restart_uses_verified_manage_script(tmp_path: Path, monkeypatch):
    _ignore_docker(monkeypatch)
    command = tmp_path / "manage.sh"
    command.write_text("case x in\nrestart) stop; start ;;\nesac\n")
    command.chmod(0o755)

    result = detect_restart(tmp_path)

    assert result["automatic"] is True
    assert result["mode"] == "manager"
    assert result["command"] == str(command)


def test_detect_restart_rejects_script_without_restart(tmp_path: Path, monkeypatch):
    _ignore_docker(monkeypatch)
    command = tmp_path / "manage.sh"
    command.write_text("case x in\nstart) true ;;\nesac\n")
    command.chmod(0o755)

    result = detect_restart(tmp_path)

    assert result["automatic"] is False
    assert result["mode"] == "manual"


def test_detect_restart_never_auto_restarts_docker(tmp_path: Path, monkeypatch):
    original = Path.exists
    monkeypatch.setattr(
        Path,
        "exists",
        lambda self: True if str(self) == "/.dockerenv" else original(self),
    )

    result = detect_restart(tmp_path)

    assert result["automatic"] is False
    assert result["mode"] == "docker"


def test_detect_restart_never_auto_restarts_windows(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(_module.sys, "platform", "win32")

    result = detect_restart(tmp_path)

    assert result["automatic"] is False
    assert result["mode"] == "windows"
