from collections import Counter
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from scripts import mixed_command_probe
from tests import mixed_cold_rank_fixture as fixture
from tests.test_mixed_player_projection_fixture import manifest, titled_result


def saved_manifest():
    saved = manifest()
    actors = saved["actors"]
    saved["activity_rank"] = {"oracle": {
        "actors": {actor: {"sign_rank": index + 1, "sign_value": index + 1,
                           "boss_rank": index + 1, "boss_value": index + 10}
                   for index, actor in enumerate(actors)},
        "totals": {"sign": 8192, "boss": 8192}, "sign": [], "boss": [], "boss_display": [],
    }}
    saved["natal_snapshot"] = {"rows": 8192, "actor_rows": {
        actor: {"name": f"SyntheticNatal{actor}", "level": 1 + int(actor) % 7} for actor in actors}}
    return saved


class ColdRankFixtureTests(unittest.TestCase):
    def test_background_seed_updates_only_existing_nonactors_and_detects_later_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT UNIQUE,exp,stone,power)")
                uow.executemany("INSERT INTO user_xiuxian VALUES(?,?,?,?,?)",
                                ((index, str(index), 10, 20, 30) for index in range(1, 4225)))
            actors = list(map(str, range(1, 129)))
            seeded = fixture._seed_background(database, actors, 1.5)
            self.assertEqual(4096, seeded["snapshot"]["rows"])
            self.assertFalse(set(actors) & set(seeded["actors"]))
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(4224, uow.query_one("SELECT COUNT(*) AS n FROM user_xiuxian")["n"])
                self.assertEqual(4096, uow.query_one("SELECT COUNT(DISTINCT exp) AS n FROM user_xiuxian WHERE id>128")["n"])
                self.assertEqual({"exp": 10, "stone": 20, "power": 30},
                                 uow.query_one("SELECT exp,stone,power FROM user_xiuxian WHERE user_id='1'"))
                self.assertEqual({"exp": 1_000_000, "stone": 1_000_000_000, "power": 1_500_000},
                                 uow.query_one("SELECT exp,stone,power FROM user_xiuxian WHERE user_id='129'"))
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("UPDATE user_xiuxian SET exp=exp+1 WHERE user_id='129'")
            self.assertNotEqual(seeded["snapshot"], fixture._background_snapshot(database, seeded["actors"]))
            with DatabaseUnitOfWork(database) as uow:
                uow.execute("DELETE FROM user_xiuxian WHERE user_id='129'")
            with self.assertRaisesRegex(ValueError, "4096"):
                fixture._seed_background(database, actors, 1.5)

    def test_natal_population_is_bounded_and_snapshot_includes_every_row(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = SimpleNamespace(player_db=Path(directory) / "player.db")
            with DatabaseUnitOfWork(paths.player_db) as uow:
                uow.execute("CREATE TABLE avatar(user_id TEXT PRIMARY KEY)")
                uow.executemany("INSERT INTO avatar VALUES(?)", ((str(index),) for index in range(1, 8193)))
            saved = fixture._seed_natal(paths, ["1", "8192"])
            self.assertEqual(8192, saved["rows"])
            self.assertEqual({"1", "8192"}, set(saved["actor_rows"]))
            self.assertEqual(1, saved["actor_rows"]["1"]["form"])
            self.assertEqual(saved, fixture._natal_snapshot(paths, ["1", "8192"]))
            with DatabaseUnitOfWork(paths.player_db) as uow:
                uow.execute("UPDATE natal_treasure SET name='changed nonactor' WHERE user_id='2'")
            self.assertNotEqual(saved["sha256"], fixture._natal_snapshot(paths, ["1", "8192"])["sha256"])
            with DatabaseUnitOfWork(paths.player_db) as uow:
                uow.execute("INSERT INTO avatar VALUES('8193')")
            with self.assertRaisesRegex(ValueError, "8192"):
                fixture._seed_natal(paths, ["1"])

    def test_all_phase_plans_retain_writes_and_activity_coverage_and_require_natal_reply(self):
        saved = saved_manifest()
        for phase, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], spec["requests"], phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], spec["requests"], phase))
            counts = Counter(row["key"] for row in plan)
            self.assertEqual(128, len(plan))
            self.assertEqual(56, counts["profile_info"])
            self.assertEqual((0, 32, 32)[phase], counts["stone_training"])
            for key, *_ in fixture.activity.ACTIVITY_COMMANDS:
                self.assertEqual(12, counts[key])
            profiles = [row for row in plan if row["key"] == "profile_info"]
            self.assertEqual(56, len({row["actor"] for row in profiles}))
            for request in profiles:
                result = titled_result(request, 0, 0)
                result["replies"][0] += f"\n本命法宝: {request['expected_natal']}\n"
                self.assertEqual("success", fixture.classify_result(request, result)["outcome"])
                result["replies"][0] = result["replies"][0].replace(request["expected_natal"], "wrong")
                self.assertEqual("profile_natal_projection_mismatch", fixture.classify_result(request, result)["reason"])

    def test_background_and_natal_invariants_add_to_inherited_failures(self):
        saved = saved_manifest()
        saved["cold_rank_background"] = {"actors": ["background"], "snapshot": {"rows": 4096, "sha256": "scores"}}
        checked = {"ok": True, "errors": [], "invariants": "existing assets/receipts/ranks"}
        context = SimpleNamespace(paths=SimpleNamespace(game_db="not-opened"))
        with patch.object(fixture.activity, "check_invariants", return_value=checked), \
                patch.object(fixture, "_background_snapshot", return_value=saved["cold_rank_background"]["snapshot"]) as scores, \
                patch.object(fixture, "_natal_snapshot", return_value=saved["natal_snapshot"]) as natal:
            self.assertTrue(fixture.check_invariants(context, [], saved, [])["ok"])
            checked["errors"] = ["original error"]
            scores.return_value = {"rows": 4096, "sha256": "changed"}
            natal.return_value = {}
            actual = fixture.check_invariants(context, [], saved, [])
            self.assertFalse(actual["ok"])
            self.assertEqual(3, len(actual["errors"]))
            self.assertIn("original error", actual["errors"])

    def test_cli_selects_the_new_scenario_without_changing_resource_or_phase_flags(self):
        args = mixed_command_probe.parse_args([
            "--fixture", "not-opened", "--report", "not-written", "--scenario", "cold-rank-v14"])
        self.assertEqual(128, args.participants)
        self.assertEqual("cold-rank-v14", args.scenario)
        self.assertIsNone(args.phase_index)


if __name__ == "__main__":
    unittest.main()
