import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, OutboxStore
from ..repository import MapMissionClaimSqlRepository


class Clock:
    def now(self):
        return datetime(2026, 9, 15, 12)


class MissionRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',10)")
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            uow.execute(
                "CREATE TABLE map_mission_claim_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,stone INTEGER,rewards TEXT)"
            )
            OutboxStore().ensure_schema(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "CREATE TABLE map_mission(user_id TEXT PRIMARY KEY,date TEXT,"
                "mission_type TEXT,target INTEGER,claimed INTEGER,settlement TEXT)"
            )
            uow.execute("INSERT INTO map_mission VALUES('u','2026-09-15','gather',5,0,'snap')")
            uow.execute(
                "CREATE TABLE map_daily_limit(user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER)"
            )
            uow.execute("INSERT INTO map_daily_limit VALUES('u','2026-09-15',5)")
        self.repository = MapMissionClaimSqlRepository(self.game, self.player, clock=Clock())
        self.mission = {
            "date": "2026-09-15",
            "mission_type": "gather",
            "target": 5,
            "claimed": 0,
            "settlement": "snap",
        }
        self.daily = {"date": "2026-09-15", "gather_count": 5}

    def tearDown(self):
        self.temp.cleanup()

    def claim(self, operation_id="x", daily=None):
        return self.repository.claim(
            operation_id,
            "u",
            self.mission,
            daily or self.daily,
            "gather_count",
            7,
            [{"id": 1, "name": "m", "type": "m", "amount": 2}],
            99,
        )

    def test_success_replay(self):
        first = self.claim()
        replay = self.claim()
        self.assertEqual(("applied", "duplicate"), (first["status"], replay["status"]))
        self.assertEqual(first["effects_event_id"], replay["effects_event_id"])
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            event = uow.query_one(
                "SELECT event_type,payload_json FROM domain_outbox WHERE event_id=?",
                (first["effects_event_id"],),
            )
            self.assertEqual("game_event.projection", event["event_type"])
            self.assertEqual("map_mission_complete", json.loads(event["payload_json"])["event_key"])
            self.assertEqual(1, uow.query_one("SELECT COUNT(*) AS n FROM domain_outbox")["n"])

    def test_legacy_receipt_without_outbox_does_not_offer_dispatch(self):
        first = self.claim("legacy")
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "DELETE FROM domain_outbox WHERE event_id=?",
                (first["effects_event_id"],),
            )
        replay = self.claim("legacy")
        self.assertEqual("duplicate", replay["status"])
        self.assertNotIn("effects_event_id", replay)

    def test_outbox_failure_rolls_back_claim_and_rewards(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute(
                "CREATE TRIGGER fail_mission_event BEFORE INSERT ON domain_outbox "
                "BEGIN SELECT RAISE(ABORT, 'outbox failed'); END"
            )
        with self.assertRaises(Exception):
            self.claim("outbox-fail")
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(10, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])
            self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM map_mission_claim_operations")["n"])
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertEqual(0, uow.query_one("SELECT claimed FROM map_mission WHERE user_id='u'")["claimed"])

    def test_incomplete(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("UPDATE map_daily_limit SET gather_count=4")
        self.assertEqual("not_completed", self.claim("incomplete", {"date": "2026-09-15", "gather_count": 4})["status"])

    def test_stale(self):
        result = self.repository.claim(
            "stale", "u", {**self.mission, "settlement": "bad"}, self.daily,
            "gather_count", 0, [], 99,
        )
        self.assertEqual("state_changed", result["status"])


if __name__ == "__main__":
    unittest.main()
