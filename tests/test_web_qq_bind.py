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
            "id": "10001",
            "token": "old-token",
            "secret": "old-secret",
            "intent": {"direct_message": True},
            "use_websocket": False,
        }
    ]
    env_file.write_text(
        "HOST=0.0.0.0\nPORT=8080\nQQ_BOTS='\n"
        + json.dumps(existing, ensure_ascii=False, indent=2)
        + "\n'\nNICKNAME=[\"修仙\"]\n",
        encoding="utf-8",
    )

    created = merge_qq_bots_env(env_file, "10001", "new-secret")

    assert created is False
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
            "intent": {"direct_message": True},
            "use_websocket": True,
        }
    ]


def test_merge_qq_bots_env_adds_websocket_bot(tmp_path: Path):
    env_file = tmp_path / ".env.dev"
    env_file.write_text("HOST=127.0.0.1\n", encoding="utf-8")

    created = merge_qq_bots_env(env_file, "20002", "bound-secret")

    assert created is True
    text = env_file.read_text(encoding="utf-8")
    payload = text.split("QQ_BOTS='\n", 1)[1].split("\n'", 1)[0]
    assert json.loads(payload) == [
        {
            "id": "20002",
            "token": "bound-secret",
            "secret": "bound-secret",
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
