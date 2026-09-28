import asyncio
import json
import importlib
import sqlite3
from types import SimpleNamespace

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.features.world_events.application import DemonEventLifecycleApplication
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.transaction_service import (
    DemonEventLifecycleService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.transaction_service import STATE_FIELDS
from nonebot_plugin_xiuxian_2.compatibility.legacy_demon_event_lifecycle import (
    DemonEventLifecycleResult as LegacyDemonEventLifecycleResult,
    DemonEventLifecycleService as LegacyDemonEventLifecycleService,
)


def test_world_events_facade_uses_feature_application_not_legacy_service():
    world_events = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events"
    )
    assert hasattr(world_events.demon_event_lifecycle_application, "transition")
    assert not hasattr(world_events, "_demon_event_lifecycle_service")
    assert DemonEventLifecycleService is LegacyDemonEventLifecycleService


def test_legacy_event_lifecycle_result_and_service_keep_import_identity():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.transaction_service import (
        DemonEventLifecycleResult,
    )

    assert DemonEventLifecycleService is LegacyDemonEventLifecycleService
    assert DemonEventLifecycleResult is LegacyDemonEventLifecycleResult


def idle():
    return {field: ({} if field in {"bosses", "participants", "claimed"} else 0 if field in {"active", "manual"} else "") for field in STATE_FIELDS}


def active(event_id="event-1", manual=0):
    value = idle()
    value.update({"active": 1, "status": "active", "event_id": event_id, "event_type": "demon_invasion", "name": "魔修入侵", "period": "2026-07-14", "manual": manual, "bosses": {"练气境": {"wave": 1}}, "started_at": "18:00", "ends_at": "22:00"})
    return value


def create_db(path, snapshot):
    conn = sqlite3.connect(path)
    definitions = ",".join(f'"{field}" {"INTEGER" if field in {"active", "manual"} else "TEXT"}' for field in STATE_FIELDS)
    conn.execute(f"CREATE TABLE world_event_state (user_id TEXT PRIMARY KEY,{definitions})")
    if snapshot is not None:
        values = [json.dumps(snapshot[field], ensure_ascii=False) if field in {"bosses", "participants", "claimed"} else snapshot[field] for field in STATE_FIELDS]
        conn.execute(f"INSERT INTO world_event_state VALUES ({','.join('?' for _ in range(14))})", ("global", *values))
    conn.commit(); conn.close()


def test_auto_start_and_replay_use_fixed_target(tmp_path):
    db, expected, target = tmp_path / "player.db", idle(), active()
    create_db(db, expected)
    service = DemonEventLifecycleService(db)
    result = service.transition("start-day", "global", "auto_start", expected, target)
    assert result.status == "applied" and result.state == target
    assert service.transition("start-day", "global", "auto_start", expected, target) == result
    assert service.transition("start-day", "global", "auto_start", expected, active("other")).status == "operation_conflict"


def test_manual_finish_preserves_event_data_and_rejects_stale_cycle(tmp_path):
    db, expected = tmp_path / "player.db", active(manual=1)
    create_db(db, expected)
    target = dict(expected)
    target.update({"active": 0, "status": "finished", "last_result": "manual finish"})
    service = DemonEventLifecycleService(db)
    result = service.transition("finish-1", "global", "manual_finish", expected, target)
    assert result.status == "applied" and result.state["bosses"] == expected["bosses"]
    stale = active("old-event")
    assert service.transition("finish-old", "global", "auto_finish", stale, dict(target, event_id="old-event")).status == "state_changed"


def test_feature_application_replays_and_accepts_legacy_lifecycle_operations(tmp_path):
    db, expected, target = tmp_path / "player.db", idle(), active()
    create_db(db, expected)
    legacy = DemonEventLifecycleService(db)
    original = legacy.transition("legacy-start", "global", "auto_start", expected, target)
    application = DemonEventLifecycleApplication(db)

    replay = application.replay("legacy-start")
    repeated = application.transition("legacy-start", "global", "auto_start", expected, target)

    assert original.status == replay.status == repeated.status == "applied"
    assert replay.state == repeated.state == target
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM demon_event_lifecycle_operations").fetchone()[0] == 1
    conn.close()


def test_lifecycle_operation_failure_rolls_back_complete_state(tmp_path):
    db, expected, target = tmp_path / "player.db", idle(), active()
    create_db(db, expected)
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE demon_event_lifecycle_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL)")
    conn.execute("CREATE TRIGGER reject_lifecycle BEFORE INSERT ON demon_event_lifecycle_operations BEGIN SELECT RAISE(ABORT, 'reject lifecycle'); END")
    conn.commit(); conn.close()
    with pytest.raises(Exception, match="reject lifecycle"):
        DemonEventLifecycleService(db).transition("start", "global", "manual_start", expected, target)
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT status,event_id FROM world_event_state").fetchone() == ("", "")
    conn.close()


def test_lifecycle_verification_failure_rolls_back_state_and_operation(tmp_path):
    db, expected, target = tmp_path / "player.db", idle(), active()
    create_db(db, expected)
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TRIGGER tamper_lifecycle_state AFTER UPDATE ON world_event_state "
        "BEGIN UPDATE world_event_state SET status='tampered' WHERE user_id=NEW.user_id; END"
    )
    conn.commit(); conn.close()
    with pytest.raises(RuntimeError, match="state verification failed"):
        DemonEventLifecycleService(db).transition("start-verify", "global", "manual_start", expected, target)
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT status,event_id FROM world_event_state").fetchone() == ("", "")
    assert conn.execute("SELECT COUNT(*) FROM demon_event_lifecycle_operations").fetchone()[0] == 0
    conn.close()


def test_real_auto_and_manual_entries_share_lifecycle_service():
    text = open("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_world_events/__init__.py", encoding="utf-8").read()
    for start, end in [
        ("def _start_auto_demon_invasion", "def _finish_auto_demon_invasion"),
        ("def _finish_auto_demon_invasion", "def _refresh_defeated_demon_bosses"),
        ("async def start_demon_invasion_", "async def start_spirit_vein_"),
        ("async def close_world_event_", "async def close_spirit_vein_"),
    ]:
        body = text[text.index(start):text.index(end, text.index(start))]
        assert "demon_event_lifecycle_application." in body
        assert "DemonEventLifecycleService" not in text
        assert "_save_state(state)" not in body


def test_manual_finish_replay_skips_transition(monkeypatch):
    from nonebot.exception import FinishedException

    world_events = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events"
    )
    finished = active(manual=1)
    finished.update({"active": 0, "status": "finished"})
    replayed = LegacyDemonEventLifecycleResult(
        status="applied", action="manual_finish", state=finished
    )

    class ReplayOnlyApplication:
        def replay(self, operation_id):
            assert operation_id == "demon-lifecycle:manual-finish:message-1"
            return replayed

        def transition(self, *args, **kwargs):
            pytest.fail("replayed lifecycle operation must not transition again")

    async def assign_bot(*, bot, event):
        return bot, None

    async def handle_send(*args, **kwargs):
        return None

    monkeypatch.setattr(
        world_events, "demon_event_lifecycle_application", ReplayOnlyApplication()
    )
    monkeypatch.setattr(world_events, "_load_state", lambda: active())
    monkeypatch.setattr(world_events, "assign_bot", assign_bot)
    monkeypatch.setattr(world_events, "handle_send", handle_send)

    with pytest.raises(FinishedException):
        asyncio.run(
            world_events.close_world_event_(
                object(), SimpleNamespace(message_id="message-1", id="")
            )
        )
