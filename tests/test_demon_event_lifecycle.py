import json
import sqlite3

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.demon_event_lifecycle_service import (
    DemonEventLifecycleService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.demon_wave_refresh_service import STATE_FIELDS


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
        assert "demon_event_lifecycle_service" in body
        assert "_save_state(state)" not in body
