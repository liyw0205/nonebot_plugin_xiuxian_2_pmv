import tempfile
import unittest
import importlib
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.transaction_service import SectWeeklyRewardClaimService
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_weekly import SectWeeklyGoalManager
from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.features.sect.weekly_progress_repository import SectWeeklyProgressSqlRepository
from scripts.check_full_refactor_progress import _slice_status
from tests.test_db_backend import db_backend


def test_sect_weekly_facade_has_no_legacy_claim_service_dependency():
    weekly = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_weekly_commands"
    )
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_weekly_commands.py"
    ).read_text(encoding="utf-8")
    assert not hasattr(weekly, "_legacy_sect_weekly_reward_service")
    assert "SectWeeklyRewardClaimService" not in source
    assert "_legacy_sect_weekly_reward_service" not in source


def test_sect_weekly_facade_does_not_construct_a_legacy_sql_manager():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_weekly_commands.py"
    ).read_text(encoding="utf-8")
    assert "_sect_weekly_application().get_sect_info(sect_id)" in source
    assert "_sql_message" not in source
    assert "XiuxianDateManage" not in source


def test_sect_weekly_claim_uses_feature_application_without_legacy_fallback():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_weekly_commands.py"
    ).read_text(encoding="utf-8")
    handler = source[
        source.index("@sect_weekly_claim.handle"):
        source.index("@sect_weekly_rank.handle")
    ]
    assert "SectWeeklyRewardClaimService" not in source
    assert "_legacy_sect_weekly_reward_service" not in source
    assert "_sect_weekly_application().claim_weekly(" in handler
    assert "_legacy_sect_weekly_reward_service().claim(" not in handler


def test_sect_weekly_migrations_route_game_and_player_schema_separately():
    from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database

    migrations = build_migrations()
    game = {migration.version for migration in migrations_for_database(migrations, "game_db")}
    player = {migration.version for migration in migrations_for_database(migrations, "player_db")}
    assert "sect.011" in game
    assert "sect.011" not in player
    assert "sect.012" in player
    assert "sect.012" not in game


def test_sect_weekly_goal_manager_does_not_create_schema_during_requests():
    source = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_weekly.py").read_text(encoding="utf-8")
    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    repository = Path("nonebot_plugin_xiuxian_2/features/sect/weekly_progress_repository.py").read_text(encoding="utf-8")
    assert "schema is not ready; run migrations first" in repository
    assert "CREATE TABLE" not in repository
    assert "ALTER TABLE" not in repository


def test_sect_weekly_progress_profile_read_uses_application():
    source = Path("nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sect_weekly.py").read_text(encoding="utf-8")
    assert "_sect_application().get_user_profile(str(user_id))" in source
    assert "self._sql_message().get_user_info_with_id(" not in source


def test_sect_weekly_progress_persistence_is_feature_owned():
    sect = _slice_status()["sect"]
    assert sect["sect_weekly_progress_application_owned"]
    assert sect["sect_weekly_progress_repository_owned"]
    assert sect["sect_weekly_progress_request_path_has_no_ddl"]
    assert sect["sect_weekly_progress_manager_no_legacy_writes"]


def test_sect_weekly_manager_delegates_progress_to_feature_application():
    with tempfile.TemporaryDirectory() as temp:
        game = Path(temp) / "game.db"
        with db_backend.transaction(game) as conn:
            conn.execute(
                "CREATE TABLE sect_weekly_goal(sect_id INTEGER NOT NULL,week_key TEXT NOT NULL,"
                "goal_key TEXT NOT NULL,progress INTEGER NOT NULL DEFAULT 0,target INTEGER NOT NULL,"
                "participants TEXT NOT NULL DEFAULT '{}',claimed_users TEXT NOT NULL DEFAULT '[]',"
                "updated_at TEXT NOT NULL DEFAULT '',PRIMARY KEY(sect_id,week_key,goal_key))"
            )
            conn.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_name TEXT)")
            conn.execute("INSERT INTO sects VALUES(1,'青云宗')")
        application = SectApplication(
            game,
            weekly_progress_repository=SectWeeklyProgressSqlRepository(game),
        )
        manager = SectWeeklyGoalManager()
        with patch(
            "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_weekly._sect_application",
            return_value=application,
        ):
            first = manager.record_event("u", "sect_task_complete", 14, {"sect_id": 1})
            second = manager.record_event("u", "sect_task_complete", 1, {"sect_id": 1})
            goals = manager.list_goals(1)
            rank = manager.weekly_rank(week_key=manager.current_week_key())
        assert ("sect_diligence", 14, False) == (first[0]["goal_key"], first[0]["progress"], first[0]["completed"])
        assert ("同门勤务", 15, True) == (second[0]["name"], second[0]["progress"], second[0]["completed"])
        diligence = next(goal for goal in goals if goal["key"] == "sect_diligence")
        assert (15, 15, True) == (diligence["progress"], diligence["raw_progress"], diligence["completed"])
        assert 15 == rank[0]["total_progress"]


class SectWeeklyRewardClaimTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,stone INTEGER,exp INTEGER,sect_contribution INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',1,10,20,30)")
            conn.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_scale INTEGER,sect_materials INTEGER)")
            conn.execute("INSERT INTO sects VALUES(1,100,200)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))")
            conn.execute("CREATE TABLE sect_weekly_goal(sect_id INTEGER,week_key TEXT,goal_key TEXT,progress INTEGER,target INTEGER,claimed_users TEXT,updated_at TEXT,PRIMARY KEY(sect_id,week_key,goal_key))")
            conn.executemany("INSERT INTO sect_weekly_goal VALUES(1,'2026-W29',%s,%s,%s,'[]','')", (("g1", 10, 10), ("g2", 20, 20), ("pending", 1, 5)))
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE boss_limit(user_id TEXT PRIMARY KEY,integral INTEGER)")
            conn.execute("INSERT INTO boss_limit VALUES('u',7)")
        self.service = SectWeeklyRewardClaimService(self.game, self.player)
        self.goals = [
            {"key": "g1", "name": "目标一", "target": 10, "rewards": {"stone": 5, "exp": 6, "sect_contribution": 7, "sect_scale": 8, "sect_materials": 9, "boss_integral": 10, "items": [{"id": 101, "name": "周常令", "type": "道具", "amount": 2}]}},
            {"key": "g2", "name": "目标二", "target": 20, "rewards": {"stone": 11, "sect_contribution": 12, "sect_scale": 13, "sect_materials": 14, "boss_integral": 15, "items": [{"id": 101, "name": "周常令", "type": "道具", "amount": 3}]}},
        ]

    def tearDown(self):
        self.tmp.cleanup()

    def claim(self, operation_id="op", **changes):
        args = dict(operation_id=operation_id, user_id="u", sect_id=1, week_key="2026-W29", goals=self.goals, max_goods_num=100)
        args.update(changes)
        return self.service.claim(**args)

    def test_batch_claim_updates_all_assets_and_is_idempotent(self):
        self.assertEqual("applied", self.claim().status)
        self.assertEqual("duplicate", self.claim().status)
        with db_backend.connection(self.game) as conn:
            self.assertEqual((26, 26, 49), tuple(conn.execute("SELECT stone,exp,sect_contribution FROM user_xiuxian").fetchone()))
            self.assertEqual((121, 223), tuple(conn.execute("SELECT sect_scale,sect_materials FROM sects").fetchone()))
            self.assertEqual(5, conn.execute("SELECT goods_num FROM back").fetchone()[0])
            self.assertEqual(['["u"]', '["u"]'], [row[0] for row in conn.execute("SELECT claimed_users FROM sect_weekly_goal WHERE goal_key IN ('g1','g2') ORDER BY goal_key")])
        with db_backend.connection(self.player) as conn:
            self.assertEqual(32, conn.execute("SELECT integral FROM boss_limit").fetchone()[0])

    def test_single_goal_uses_same_transaction_path(self):
        result = self.claim(goals=self.goals[:1])
        self.assertEqual("applied", result.status)
        self.assertEqual(("目标一", "周常令x2、灵石5、修为6、宗门贡献7、宗门建设度8、宗门资材9、BOSS积分10"), result.rewards[0])
        with db_backend.connection(self.game) as conn:
            self.assertEqual('["u"]', conn.execute("SELECT claimed_users FROM sect_weekly_goal WHERE goal_key='g1'").fetchone()[0])
            self.assertEqual('[]', conn.execute("SELECT claimed_users FROM sect_weekly_goal WHERE goal_key='g2'").fetchone()[0])

    def test_rechecks_week_membership_completion_and_claim_state(self):
        pending = [{**self.goals[0], "key": "pending", "target": 5}]
        self.assertEqual("not_completed", self.claim(goals=pending).status)
        self.assertEqual("not_completed", self.claim(week_key="2026-W28").status)
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET sect_id=2 WHERE user_id='u'")
        self.assertEqual("sect_changed", self.claim().status)

    def test_inventory_full_and_operation_conflict_do_not_mutate(self):
        self.assertEqual("inventory_full", self.claim(max_goods_num=4).status)
        self.assertEqual("applied", self.claim().status)
        self.assertEqual("operation_conflict", self.claim(goals=self.goals[:1]).status)

    def test_operation_failure_rolls_back_both_databases(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE sect_weekly_reward_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_json TEXT,created_at TEXT)")
            conn.execute("CREATE TRIGGER fail_weekly_operation BEFORE INSERT ON sect_weekly_reward_operations BEGIN SELECT RAISE(ABORT,'operation failed'); END")
        with self.assertRaises(Exception):
            self.claim()
        with db_backend.connection(self.game) as conn:
            self.assertEqual((10, 20, 30), tuple(conn.execute("SELECT stone,exp,sect_contribution FROM user_xiuxian").fetchone()))
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM back").fetchone()[0])
            self.assertEqual(['[]', '[]'], [row[0] for row in conn.execute("SELECT claimed_users FROM sect_weekly_goal WHERE goal_key IN ('g1','g2') ORDER BY goal_key")])
        with db_backend.connection(self.player) as conn:
            self.assertEqual(7, conn.execute("SELECT integral FROM boss_limit").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
