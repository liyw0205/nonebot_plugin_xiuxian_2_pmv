import base64
import importlib.util
import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_MODULE_PATH = (
    Path(__file__).parents[1]
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_web"
    / "qq_bind.py"
)
_spec = importlib.util.spec_from_file_location("xiuxian_web_qq_bind", _MODULE_PATH)
assert _spec and _spec.loader
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
BindTaskStore = _module.BindTaskStore
decrypt_bind_secret = _module.decrypt_bind_secret
merge_qq_bots_env = _module.merge_qq_bots_env
qr_png_bytes = _module.qr_png_bytes


def test_decrypt_bind_secret_round_trip():
    key = AESGCM.generate_key(bit_length=256)
    nonce = b"0123456789ab"
    encrypted = nonce + AESGCM(key).encrypt(nonce, b"secret-value", None)

    assert decrypt_bind_secret(
        base64.b64encode(encrypted).decode(),
        base64.b64encode(key).decode(),
    ) == "secret-value"


def test_merge_qq_bots_env_preserves_existing_config(tmp_path: Path):
    env_file = tmp_path / ".env.dev"
    existing = [
        {
            "id": "old-bot",
            "token": "retired-token",
            "secret": "retired-secret",
            "intent": {"at_messages": True},
            "use_websocket": True,
        },
        {
            "id": "10001",
            "token": "old-token",
            "secret": "old-secret",
            "intent": {"direct_message": True},
            "use_websocket": False,
        },
    ]
    env_file.write_text(
        "HOST=0.0.0.0\nPORT=8080\nQQ_BOTS='\n"
        + json.dumps(existing, ensure_ascii=False, indent=2)
        + "\n'\nNICKNAME=[\"修仙\"]\n",
        encoding="utf-8",
    )

    replaced = merge_qq_bots_env(env_file, "10001", "new-secret")

    assert replaced is True
    text = env_file.read_text(encoding="utf-8")
    assert "HOST=0.0.0.0" in text
    assert "NICKNAME=[\"修仙\"]" in text
    payload = text.split("QQ_BOTS='\n", 1)[1].split("\n'", 1)[0]
    bots = json.loads(payload)
    assert bots == [
        {
            "id": "10001",
            "token": "new-secret",
            "secret": "new-secret",
            "intent": {
                "direct_message": True,
                "c2c_group_at_messages": True,
            },
            "use_websocket": True,
        }
    ]


def test_merge_qq_bots_env_adds_websocket_bot(tmp_path: Path):
    env_file = tmp_path / ".env.dev"
    env_file.write_text("HOST=127.0.0.1\n", encoding="utf-8")

    replaced = merge_qq_bots_env(env_file, "20002", "bound-secret")

    assert replaced is False
    text = env_file.read_text(encoding="utf-8")
    payload = text.split("QQ_BOTS='\n", 1)[1].split("\n'", 1)[0]
    assert json.loads(payload) == [
        {
            "id": "20002",
            "token": "bound-secret",
            "secret": "bound-secret",
            "intent": {
                "c2c_group_at_messages": True,
                "direct_message": True,
            },
            "use_websocket": True,
        }
    ]
    assert (tmp_path / ".env.dev.bak").read_text(encoding="utf-8") == "HOST=127.0.0.1\n"


def test_qr_png_is_local_png():
    content = qr_png_bytes("https://q.qq.com/example?task_id=abc")

    assert content.startswith(b"\x89PNG\r\n\x1a\n")
    assert len(content) > 100


def test_task_store_expires_without_exposing_key(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr("time.time", lambda: now[0])
    store = BindTaskStore(ttl=10)
    store.add("task-1", "key-1")

    assert store.get("task-1") is not None
    assert "key-1" not in repr(store.public_status("task-1", "waiting"))

    now[0] = 1011.0
    assert store.get("task-1") is None
    assert store.public_status("task-1", "waiting") == {"status": "expired"}


def test_config_page_uses_persistent_restart_button():
    template = (
        Path(__file__).parents[1]
        / "nonebot_plugin_xiuxian_2"
        / "xiuxian"
        / "xiuxian_web"
        / "templates"
        / "config.html"
    ).read_text(encoding="utf-8")

    assert 'id="qqBindRestart"' in template
    assert "restartQqBindBot()" in template
    assert "qqBindRestart').hidden = false" in template
    assert "body: JSON.stringify({ confirm: true })" in template
    assert "confirm(`绑定已完成" not in template
    restart_function = template.split("async function restartQqBindBot()", 1)[1].split(
        "async function pollQqBind()", 1
    )[0]
    assert "alert(" not in restart_function


def test_task_store_keeps_completed_result_without_secret():
    store = BindTaskStore(ttl=10)
    store.add("task-2", "private-key")
    result = {
        "success": True,
        "status": "completed",
        "appid": "123456",
        "message": "绑定完成",
    }

    store.complete("task-2", result)

    assert store.get("task-2") is None
    assert store.completed("task-2") == result
    assert "private-key" not in repr(store.completed("task-2"))
