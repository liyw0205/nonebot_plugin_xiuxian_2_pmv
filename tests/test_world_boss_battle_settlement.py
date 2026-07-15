import json
import sqlite3

import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_boss.battle_settlement_service import (
    WorldBossBattleSettlementService,
)


OLD_BOSS = {
    "name": "旧首领", "jj": "练气境", "气血": 1000, "总血量": 1000,
    "攻击": 100, "真元": 100, "max_stone": 5000, "stone": 5000,
}
NEW_BOSS = {
    "name": "新首领", "jj": "练气境", "气血": 1200, "总血量": 1200,
    "攻击": 120, "真元": 120, "max_stone": 6000, "stone": 6000,
}


def create_databases(tmp_path):
    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    activity = tmp_path / "activity.db"
    conn = sqlite3.connect(game)
    conn.execute(
        "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_stamina INTEGER,hp INTEGER,mp INTEGER,"
        "exp INTEGER,stone INTEGER)"
    )
    conn.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT)")
    conn.execute(
        "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,"
        "create_time TEXT,update_time TEXT,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
    )
    conn.execute("INSERT INTO user_xiuxian VALUES ('10001',100,2000,500,10000,2000)")
    conn.execute("INSERT INTO user_cd VALUES ('10001','old-check')")
    conn.commit()
    conn.close()

    conn = sqlite3.connect(player)
    conn.execute("CREATE TABLE boss(user_id TEXT PRIMARY KEY,boss_stone INTEGER,boss_integral INTEGER,boss_battle_count INTEGER)")
    conn.execute("CREATE TABLE boss_limit(user_id TEXT PRIMARY KEY,integral INTEGER)")
    conn.execute("INSERT INTO boss VALUES ('10001',100,10,2)")
    conn.execute("INSERT INTO boss_limit VALUES ('10001',50)")
    conn.execute("CREATE TABLE world_boss_state(state_key TEXT PRIMARY KEY,bosses TEXT NOT NULL,updated_at TEXT NOT NULL)")
    conn.execute("INSERT INTO world_boss_state VALUES ('global',?,'old')", (json.dumps([OLD_BOSS]),))
    conn.commit()
    conn.close()
    sqlite3.connect(activity).close()
    return game, player, activity


def settle(service, operation_id="op-1", settled=None, item=None, activities=None, killed=False, boss_index=0, stamina_cost=10):
    settled = settled or [{**OLD_BOSS, "气血": 800}]
    return service.settle(
        operation_id=operation_id,
        user_id="10001",
        expected_bosses=[OLD_BOSS],
        settled_bosses=settled,
        boss_index=boss_index,
        expected_stamina=100,
        stamina_cost=stamina_cost,
        expected_hp=2000,
        expected_mp=500,
        final_hp=600,
        final_mp=300,
        expected_exp=10000,
        exp_reward=200,
        expected_stone=2000,
        stone_reward=500,
        expected_daily_stone=100,
        expected_daily_integral=10,
        expected_total_integral=50,
        integral_reward=20,
        expected_battle_count=2,
        battle_limit=30,
        expected_checked_at="old-check",
        checked_at="new-check",
        item=item,
        max_goods_num=10,
        actual_damage=200,
        killed=killed,
        daily_period="2026-07-14",
        weekly_period="2026-W29",
        activity_bosses=activities,
    )


def test_single_transaction_updates_full_battle_lifecycle(tmp_path):
    game, player, activity = create_databases(tmp_path)
    service = WorldBossBattleSettlementService(game, player, activity)
    activities = [{
        "key": "summer", "boss_name": "炎君", "max_hp": 1000, "daily_fight_limit": 3,
        "hit_hp_cap_ratio": 0.2, "multiplier": 1.0,
        "server_milestones": [{"key": "p80", "hp_percent": 80}],
    }]
    result = settle(
        service,
        settled=[NEW_BOSS],
        item={"id": 9001, "name": "首领遗物", "type": "神物", "quantity": 1, "bind": True},
        activities=activities,
        killed=True,
    )
    assert result.status == "applied"
    assert (result.stamina, result.battle_count, result.stone, result.exp, result.integral) == (90, 3, 2500, 10200, 70)
    conn = sqlite3.connect(game)
    assert conn.execute("SELECT user_stamina,hp,mp,exp,stone FROM user_xiuxian").fetchone() == (90, 600, 300, 10200, 2500)
    assert conn.execute("SELECT goods_num,bind_num FROM back WHERE goods_id=9001").fetchone() == (1, 1)
    assert conn.execute("SELECT COUNT(*) FROM world_boss_battle_operations").fetchone()[0] == 1
    conn.close()
    conn = sqlite3.connect(player)
    assert json.loads(conn.execute("SELECT bosses FROM world_boss_state").fetchone()[0]) == [NEW_BOSS]
    assert conn.execute("SELECT boss_stone,boss_integral,boss_battle_count FROM boss").fetchone() == (600, 30, 3)
    assert conn.execute("SELECT integral FROM boss_limit").fetchone()[0] == 70
    assert conn.execute('SELECT "讨伐世界BOSS","击败世界BOSS" FROM statistics').fetchone() == (1, 1)
    daily, weekly = conn.execute("SELECT daily_progress,weekly_progress FROM xiuxian_tasks").fetchone()
    assert json.loads(daily)["daily_boss"] == 1
    assert json.loads(weekly)["weekly_boss"] == 1
    conn.close()
    conn = sqlite3.connect(activity)
    assert conn.execute("SELECT hp_left FROM activity_boss_state").fetchone()[0] == 800
    assert conn.execute("SELECT total_damage FROM activity_boss_damage").fetchone()[0] == 200
    assert conn.execute("SELECT milestone_key FROM activity_boss_milestone").fetchone()[0] == "p80"
    conn.close()


def test_operation_replay_is_idempotent_and_conflict_is_rejected(tmp_path):
    game, player, activity = create_databases(tmp_path)
    service = WorldBossBattleSettlementService(game, player, activity)
    first = settle(service)
    second = settle(service)
    assert (first.status, second.status) == ("applied", "duplicate")
    # request identity differs (stamina_cost); mutable settled snapshot is not part of payload key
    assert settle(service, stamina_cost=11).status == "state_changed"
    conn = sqlite3.connect(player)
    assert conn.execute("SELECT boss_battle_count FROM boss").fetchone()[0] == 3
    conn.close()


def test_snapshot_change_and_full_inventory_do_not_consume_cost(tmp_path):
    game, player, activity = create_databases(tmp_path)
    conn = sqlite3.connect(player)
    conn.execute("UPDATE world_boss_state SET bosses=?", (json.dumps([{**OLD_BOSS, "气血": 999}]),))
    conn.commit()
    conn.close()
    service = WorldBossBattleSettlementService(game, player, activity)
    assert settle(service).status == "boss_changed"
    conn = sqlite3.connect(player)
    conn.execute("UPDATE world_boss_state SET bosses=?", (json.dumps([OLD_BOSS]),))
    conn.commit()
    conn.close()
    conn = sqlite3.connect(game)
    conn.execute("INSERT INTO back VALUES ('10001',9001,'遗物','神物',10,'','','0')")
    conn.commit()
    conn.close()
    assert settle(service, item={"id": 9001, "name": "遗物", "type": "神物", "quantity": 1}).status == "inventory_full"
    conn = sqlite3.connect(game)
    assert conn.execute("SELECT user_stamina,stone,exp FROM user_xiuxian").fetchone() == (100, 2000, 10000)
    conn.close()


def test_operation_failure_rolls_back_every_database(tmp_path):
    game, player, activity = create_databases(tmp_path)
    conn = sqlite3.connect(game)
    conn.execute(
        "CREATE TABLE world_boss_battle_operations(operation_id TEXT PRIMARY KEY,payload TEXT,boss_hp INTEGER,"
        "stamina INTEGER,battle_count INTEGER,stone INTEGER,exp INTEGER,integral INTEGER,activity_lines TEXT,created_at TEXT)"
    )
    conn.execute("CREATE TRIGGER reject_operation BEFORE INSERT ON world_boss_battle_operations BEGIN SELECT RAISE(ABORT,'reject operation'); END")
    conn.commit()
    conn.close()
    with pytest.raises(Exception, match="reject operation"):
        settle(WorldBossBattleSettlementService(game, player, activity))
    conn = sqlite3.connect(game)
    assert conn.execute("SELECT user_stamina,hp,mp,exp,stone FROM user_xiuxian").fetchone() == (100, 2000, 500, 10000, 2000)
    conn.close()
    conn = sqlite3.connect(player)
    assert json.loads(conn.execute("SELECT bosses FROM world_boss_state").fetchone()[0]) == [OLD_BOSS]
    assert conn.execute("SELECT boss_stone,boss_integral,boss_battle_count FROM boss").fetchone() == (100, 10, 2)
    conn.close()


def test_real_entry_uses_composite_service_without_segmented_side_paths():
    path = "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_boss/__init__.py"
    with open(path, encoding="utf-8") as source_file:
        text = source_file.read()
    handler = text[text.index("async def battle_"):text.index("@boss_info.handle")]
    assert "world_boss_battle_settlement_service.settle(" in handler
    assert "Boss_fight(\n        user_id,\n        bossinfo,\n        type_in=1," in handler
    post_fight = handler[handler.index("result, victor, bossinfo_new, status_list"):]
    assert "boss_reward_service.grant(" not in post_fight
    assert "boss_battle_cost_service.consume(" not in handler
    assert "old_boss_info.save_boss(group_boss)" not in post_fight
    assert 'update_statistics_value(user_id, "讨伐世界BOSS")' not in post_fight
    assert 'record_task_progress(user_id, "boss")' not in post_fight
    assert "record_cooperative_boss_hit(" not in post_fight
