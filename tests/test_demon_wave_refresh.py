import json
import sqlite3

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_world_events.demon_wave_refresh_service import (
    DemonWaveRefreshService,
    STATE_FIELDS,
)


def state():
    boss = {"wave": 1, "boss_hp": 0, "boss_max_hp": 1000}
    participant = {"realm": "练气境", "wave": 1, "damage": 100, "reward_multiplier": 1.0}
    return {
        "active": 1, "status": "active", "event_id": "event-1", "event_type": "demon_invasion",
        "name": "魔修入侵", "period": "2026-07-14", "manual": 0,
        "bosses": {"练气境": boss}, "participants": {"p1": participant}, "claimed": {},
        "started_at": "2026-07-14 18:00:00", "ends_at": "2026-07-14 22:00:00", "last_result": "",
    }


def create_db(path, snapshot):
    conn = sqlite3.connect(path)
    definitions = ",".join(f'"{field}" {"INTEGER" if field in {"active", "manual"} else "TEXT"}' for field in STATE_FIELDS)
    conn.execute(f"CREATE TABLE world_event_state (user_id TEXT PRIMARY KEY,{definitions})")
    values = [json.dumps(snapshot[field], ensure_ascii=False) if field in {"bosses", "participants", "claimed"} else snapshot[field] for field in STATE_FIELDS]
    conn.execute(f"INSERT INTO world_event_state VALUES ({','.join('?' for _ in range(14))})", ("global", *values))
    conn.commit()
    conn.close()


def test_refresh_atomically_replaces_boss_and_unlocks_reward(tmp_path):
    db, expected = tmp_path / "player.db", state()
    create_db(db, expected)
    replacement = {"wave": 2, "boss_hp": 2000, "boss_max_hp": 2000}
    service = DemonWaveRefreshService(db)
    result = service.refresh("slot-1", "global", expected, {"练气境": replacement}, "refreshed")
    assert result.status == "applied" and result.refreshed_realms == ("练气境",)
    assert result.state["participants"]["p1"]["reward_ready"] == 1
    assert service.refresh("slot-1", "global", expected, {"练气境": replacement}, "refreshed") == result


def test_refresh_rejects_snapshot_change_and_payload_conflict(tmp_path):
    db, expected = tmp_path / "player.db", state()
    create_db(db, expected)
    changed = dict(expected)
    changed["participants"] = {}
    service = DemonWaveRefreshService(db)
    assert service.refresh("slot-1", "global", changed, {"练气境": {"wave": 2}}, "x").status == "state_changed"
    replacement = {"wave": 2, "boss_hp": 2, "boss_max_hp": 2}
    assert service.refresh("slot-2", "global", expected, {"练气境": replacement}, "x").status == "applied"
    assert service.refresh("slot-2", "global", expected, {"练气境": replacement}, "different").status == "operation_conflict"


def test_refresh_failure_rolls_back_all_event_fields(tmp_path):
    db, expected = tmp_path / "player.db", state()
    create_db(db, expected)
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE demon_wave_refresh_operations (operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL)")
    conn.execute("CREATE TRIGGER reject_wave_op BEFORE INSERT ON demon_wave_refresh_operations BEGIN SELECT RAISE(ABORT, 'reject wave'); END")
    conn.commit(); conn.close()
    with pytest.raises(Exception, match="reject wave"):
        DemonWaveRefreshService(db).refresh("slot-1", "global", expected, {"练气境": {"wave": 2}}, "x")
    conn = sqlite3.connect(db)
    bosses, participants = conn.execute("SELECT bosses,participants FROM world_event_state").fetchone()
    assert json.loads(bosses) == expected["bosses"] and json.loads(participants) == expected["participants"]
    conn.close()


def test_refresh_verification_failure_rolls_back_state_and_operation(tmp_path):
    db, expected = tmp_path / "player.db", state()
    create_db(db, expected)
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TRIGGER tamper_wave_state AFTER UPDATE ON world_event_state "
        "BEGIN UPDATE world_event_state SET last_result='tampered' WHERE user_id=NEW.user_id; END"
    )
    conn.commit(); conn.close()
    with pytest.raises(RuntimeError, match="state verification failed"):
        DemonWaveRefreshService(db).refresh("slot-verify", "global", expected, {"练气境": {"wave": 2}}, "x")
    conn = sqlite3.connect(db)
    bosses, participants, last_result = conn.execute("SELECT bosses,participants,last_result FROM world_event_state").fetchone()
    assert json.loads(bosses) == expected["bosses"] and json.loads(participants) == expected["participants"]
    assert last_result == expected["last_result"]
    assert conn.execute("SELECT COUNT(*) FROM demon_wave_refresh_operations").fetchone()[0] == 0
    conn.close()


def test_real_refresh_entry_uses_transaction_service():
    text = open("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_world_events/__init__.py", encoding="utf-8").read()
    body = text[text.index("def _refresh_defeated_demon_bosses"):text.index("@scheduler.scheduled_job", text.index("def _refresh_defeated_demon_bosses"))]
    assert "demon_wave_refresh_service.refresh(" in body
    assert "_save_state(state)" not in body
