from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest

from tests import mixed_player_projection_seed as fixture


class PlayerProjectionSeedTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.paths = SimpleNamespace(game_db=root / "xiuxian.db", player_db=root / "player.db")
        with sqlite3.connect(self.paths.game_db) as connection:
            connection.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT UNIQUE,exp INTEGER)")
            connection.executemany("INSERT INTO user_xiuxian VALUES(?,?,?)",
                                   ((index, str(1000 + index), index * 13) for index in range(1, 42)))
        with sqlite3.connect(self.paths.player_db) as connection:
            connection.execute("CREATE TABLE avatar(user_id TEXT PRIMARY KEY,main_id TEXT,avatar_id TEXT,"
                               "active_id TEXT,create_time TEXT)")
            connection.execute("CREATE TABLE title(user_id TEXT PRIMARY KEY,unlocked TEXT,equipped TEXT)")
            connection.execute("INSERT INTO avatar VALUES('1008','1008','1009','1009','old')")
            connection.execute("INSERT INTO title VALUES('1009','[]',NULL)")
        self.catalog = root / "\u4fee\u70bc\u7269\u54c1" / "\u79f0\u53f7.json"
        self.catalog.parent.mkdir()
        self.definitions = {"30001": {"name": "Beginner", "url": ""},
                            "30002": {"name": "Veteran", "url": ""}}
        self.catalog.write_text(json.dumps(self.definitions), encoding="utf-8")
        self.actors = ["1016", "1017"]

    def rows(self, database, query):
        with sqlite3.connect(database) as connection:
            return connection.execute(query).fetchall()

    def test_seed_uses_existing_users_and_nonempty_real_catalog_projections_without_game_writes(self):
        original = self.rows(self.paths.game_db, "SELECT * FROM user_xiuxian ORDER BY id")
        catalog_bytes = self.catalog.read_bytes()
        saved = fixture.seed(self.paths, self.actors, row_count=8)
        self.assertEqual(8, saved["population_count"])
        self.assertEqual({"avatar": 8, "title": 8}, saved["feature_table_rows"])
        self.assertEqual(hashlib.sha256(catalog_bytes).hexdigest(), saved["catalog_sha256"])
        self.assertEqual({key: value["name"] for key, value in self.definitions.items()}, saved["title_names"])
        avatar = self.rows(self.paths.player_db, "SELECT user_id,main_id,avatar_id,active_id FROM avatar")
        population = {row[0] for row in avatar}
        self.assertTrue(set(self.actors + ["1008", "1009"]) <= population)
        reserves = {row[2] for row in avatar}
        self.assertEqual(8, len(reserves))
        self.assertFalse(population & reserves)
        self.assertTrue(reserves <= {row[1] for row in original})
        self.assertTrue(all(owner == main == active for owner, main, _, active in avatar))
        self.assertEqual({owner: target for owner, _, target, _ in avatar if owner in self.actors},
                         saved["actor_pairs"])
        self.assertEqual(8, saved["reserve_population_count"])
        self.assertEqual(2 * 8 + fixture.MAX_ACTORS, saved["roster_scan_row_bound"])
        for table in fixture.TABLES:
            self.assertEqual(set(self.actors), set(saved["snapshot"][table]["actor_rows"]))
        for actor, row in saved["snapshot"]["title"]["actor_rows"].items():
            self.assertEqual(list(fixture.TITLE_IDS), json.loads(row["unlocked"]))
            self.assertEqual(fixture.TITLE_IDS[int(actor) % len(fixture.TITLE_IDS)], row["equipped"])
        self.assertEqual(saved["snapshot"], fixture.snapshot(self.paths, self.actors))
        self.assertEqual(original, self.rows(self.paths.game_db, "SELECT * FROM user_xiuxian ORDER BY id"))
        self.assertEqual(catalog_bytes, self.catalog.read_bytes())
        repeated = fixture.seed(self.paths, self.actors, row_count=8)
        self.assertEqual(saved, repeated)

    def test_snapshot_detects_nonactor_changes_and_bounds_rows_without_expanding_exact_payload(self):
        saved = fixture.seed(self.paths, self.actors, row_count=8)
        with sqlite3.connect(self.paths.player_db) as connection:
            connection.execute("UPDATE avatar SET active_id='1017' WHERE user_id='1008'")
        changed = fixture.snapshot(self.paths, self.actors)
        self.assertNotEqual(saved["snapshot"]["avatar"]["sha256"], changed["avatar"]["sha256"])
        self.assertEqual(saved["snapshot"]["avatar"]["actor_rows"], changed["avatar"]["actor_rows"])
        self.assertEqual(saved["snapshot"]["title"], changed["title"])
        with self.assertRaisesRegex(ValueError, "streaming row bound"):
            fixture.snapshot(self.paths, self.actors, max_rows=7)
        with sqlite3.connect(self.paths.player_db) as connection:
            connection.execute("UPDATE title SET equipped='30002' WHERE user_id='1016'")
        self.assertNotEqual(saved["snapshot"]["title"]["actor_rows"],
                            fixture.snapshot(self.paths, self.actors)["title"]["actor_rows"])

    def test_seed_rejects_missing_users_bad_catalog_and_row_limits_before_player_writes(self):
        original = self.rows(self.paths.player_db, "SELECT * FROM avatar")
        for actors, count, reason in ((["1016", "1016"], 8, "unique"),
                                      (["99999"], 8, "already exist"),
                                      (self.actors, 1, "population"),
                                      (self.actors, fixture.POPULATION + 1, "population"),
                                      (self.actors, True, "population"),
                                      (self.actors, 2, "owners exceed")):
            with self.subTest(actors=actors, count=count), self.assertRaisesRegex(ValueError, reason):
                fixture.seed(self.paths, actors, row_count=count)
            self.assertEqual(original, self.rows(self.paths.player_db, "SELECT * FROM avatar"))
        invalid = deepcopy(self.definitions)
        invalid["30001"]["url"] = "fixture-media"
        self.catalog.write_text(json.dumps(invalid), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "without media"):
            fixture.seed(self.paths, self.actors, row_count=8)
        self.assertEqual(original, self.rows(self.paths.player_db, "SELECT * FROM avatar"))

    def test_unknown_existing_owner_is_rejected_without_erasing_it(self):
        with sqlite3.connect(self.paths.player_db) as connection:
            connection.execute("INSERT INTO title VALUES('99999','[]',NULL)")
        original = self.rows(self.paths.player_db, "SELECT * FROM title ORDER BY user_id")
        with self.assertRaisesRegex(ValueError, "already exist"):
            fixture.seed(self.paths, self.actors, row_count=8)
        self.assertEqual(original, self.rows(self.paths.player_db, "SELECT * FROM title ORDER BY user_id"))

    def test_insufficient_distinct_reserve_users_is_rejected_before_writes(self):
        original = self.rows(self.paths.player_db, "SELECT * FROM avatar")
        with self.assertRaisesRegex(ValueError, "one distinct existing reserve"):
            fixture.seed(self.paths, self.actors, row_count=22)
        self.assertEqual(original, self.rows(self.paths.player_db, "SELECT * FROM avatar"))


if __name__ == "__main__":
    unittest.main()
