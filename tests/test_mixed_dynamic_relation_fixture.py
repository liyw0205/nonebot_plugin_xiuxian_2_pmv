from collections import Counter
from copy import deepcopy
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from nonebot_plugin_xiuxian_2.core.numeric import register_numeric_functions, sqlite_int_sort_key
from tests import mixed_dynamic_relation_fixture as fixture
from tests.test_mixed_relation_fixture import manifest as relation_manifest, reply_for


def manifest():
    saved = relation_manifest()
    scores = {actor: {"power": 1, "exp": {"canonical": 1000 + index * 100},
                      "stone": {"canonical": 1_000_000 + index * 1000}}
              for index, actor in enumerate(saved["actors"])}
    histograms = {column: Counter(sqlite_int_sort_key(values[column]["canonical"])
                                  for values in scores.values()) for column in ("exp", "stone")}
    low, high = (saved["groups"][name] for name in ("training_low", "exact_blob"))
    actors = [low[0], *low[1:17], *high[:8]]
    saved.update(score_evidence=scores, dynamic_rank_oracle={
        "base_generation": {"generation": 100, "exp_generation": 80, "stone_generation": 90},
        "writer": low[0], "max_writes": fixture.MAX_WRITES,
        "actors": fixture._rank_states(histograms, scores, low[0], actors),
    })
    return saved


def profile_result(request, number):
    oracle = request["dynamic_rank_oracle"]
    state = oracle["states"][number]
    reply = reply_for({**request, "expected_ranks": state})
    return {"request": request, "key": request["key"], "outcome": "success", "replies": [reply],
            "rank_state_observations": [{"actor": request["actor"],
                "exp_generation": oracle["base_generation"]["exp_generation"] + number,
                "stone_generation": oracle["base_generation"]["stone_generation"] + number,
                "exp_key": state["exp_key"], "stone_key": state["stone_key"]}]}


class DynamicRelationPlanTests(unittest.TestCase):
    def test_frozen_counts_aliases_single_writer_and_all_profile_strata(self):
        saved, numbers = manifest(), set()
        writer = saved["groups"]["training_low"][0]
        self.assertEqual(512, sum(spec["requests"] for spec in fixture.phase_specs()))
        for phase, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], spec["requests"], phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], spec["requests"], phase))
            self.assertEqual(
                ({"profile_info": 32, "my_mentor": 48, "my_partner": 48},
                 {"profile_info": 64, "my_mentor": 32, "my_partner": 32, "stone_training": 64},
                 {"profile_info": 32, "my_mentor": 32, "my_partner": 32, "stone_training": 96})[phase],
                dict(Counter(row["key"] for row in plan)),
            )
            self.assertFalse(numbers.intersection(row["number"] for row in plan))
            numbers.update(row["number"] for row in plan)
            self.assertEqual(spec["requests"], len({row["number"] for row in plan}))
            profiles = [row for row in plan if row["key"] == "profile_info"]
            self.assertEqual({"writer", "dynamic_low", "exact_high"}, {row["rank_cohort"] for row in profiles})
            self.assertEqual(set(saved["dynamic_rank_oracle"]["actors"]), {row["actor"] for row in profiles})
            for index, row in enumerate(profiles):
                self.assertEqual(index % 4 == 0, row["actor"] == writer)
                self.assertNotIn("expected_ranks", row)
                self.assertNotIn("actors", row["dynamic_rank_oracle"])
                self.assertEqual(97, len(row["dynamic_rank_oracle"]["states"]))
            writes = [row for row in plan if row["key"] == "stone_training"]
            self.assertTrue(all(row["actor"] == writer for row in writes))
            mentors = {row["command"] for row in plan if row["key"] == "my_mentor"}
            self.assertEqual({"我的师徒", "我的徒弟", "我的师父", "我的师傅"}, mentors)
        self.assertEqual(512, len(numbers))
        for mode, count, phase in (("pure", 127, 0), ("mixed", 192, 0), ("pure", 128, -1),
                                   ("contention", 192, 3)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(saved, mode, count, phase)

    def test_observed_generation_keys_and_both_reply_ranks_are_required(self):
        request = fixture.request_plan(manifest(), "pure", 128)[0]
        result = profile_result(request, 4)
        self.assertEqual("success", fixture.classify_result(request, result)["outcome"])
        self.assertNotIn("expected_ranks", request)
        alterations = (
            ("dynamic_rank_state_count_mismatch", lambda row: row.update(rank_state_observations=[])),
            ("dynamic_rank_state_count_mismatch", lambda row: row["rank_state_observations"].append(
                deepcopy(row["rank_state_observations"][0]))),
            ("dynamic_rank_state_actor_mismatch", lambda row: row["rank_state_observations"][0].update(actor="other")),
            ("dynamic_rank_generation_mismatch", lambda row: row["rank_state_observations"][0].update(exp_generation=85)),
            ("dynamic_rank_generation_invalid", lambda row: row["rank_state_observations"][0].update(exp_generation=True)),
            ("dynamic_rank_generation_out_of_range", lambda row: row["rank_state_observations"][0].update(
                exp_generation=79, stone_generation=89)),
            ("dynamic_rank_generation_out_of_range", lambda row: row["rank_state_observations"][0].update(
                exp_generation=177, stone_generation=187)),
            ("dynamic_rank_target_key_mismatch", lambda row: row["rank_state_observations"][0].update(stone_key="00")),
        )
        for reason, alter in alterations:
            damaged = deepcopy(result)
            alter(damaged)
            with self.subTest(reason=reason):
                self.assertEqual({"outcome": "unclassified", "reason": reason},
                                 fixture.classify_result(request, damaged))
        for column, title in (("exp", "修为排行"), ("stone", "灵石排行")):
            damaged = deepcopy(result)
            rank = request["dynamic_rank_oracle"]["states"][4][column]
            damaged["replies"][0] = damaged["replies"][0].replace(f"{title}: 第{rank}位", f"{title}: 第999位")
            self.assertEqual("exact_" + column + "_rank_mismatch",
                             fixture.classify_result(request, damaged)["reason"])

    def test_business_boundary_errors_and_relation_failure_are_not_hidden(self):
        plan = fixture.request_plan(manifest(), "pure", 128)
        request, result = plan[0], profile_result(plan[0], 2)
        for fields, outcome in (({"errors": ["failure"]}, "exception"),
                                ({"blocked": ["outside"]}, "boundary"),
                                ({"replies": ["慢...慢一..点"]}, "cooldown")):
            damaged = {**result, **fields, "rank_state_observations": []}
            self.assertEqual(outcome, fixture.classify_result(request, damaged)["outcome"])
        failure = {"outcome": "unclassified", "reason": "profile_relation_mismatch"}
        with patch.object(fixture.relation, "classify_result", return_value=failure) as classify:
            self.assertEqual(failure, fixture.classify_result(request, result))
            self.assertEqual({column: request["dynamic_rank_oracle"]["states"][2][column]
                              for column in ("exp", "stone")}, classify.call_args.args[0]["expected_ranks"])
            self.assertEqual(failure, fixture.classify_result(plan[1], {"replies": []}))


class DynamicRankOracleTests(unittest.TestCase):
    def test_multiplicities_ties_removed_groups_and_large_signed_integers_match_python(self):
        huge = 10**100
        numbers = {"writer": (0, 10_000), "tie": (100, 9000), "duplicate": (100, 9000),
                   "crossed": (200, 8000), "low": (-huge, -huge), "high": (huge, huge),
                   "zero": (0, 0)}
        scores = {actor: {column: {"canonical": value} for column, value in zip(("exp", "stone"), values)}
                  for actor, values in numbers.items()}
        histograms = {column: Counter(sqlite_int_sort_key(values[index]) for values in numbers.values())
                      for index, column in enumerate(("exp", "stone"))}
        original = deepcopy(histograms)
        states = fixture._rank_states(histograms, scores, "writer", list(numbers), max_writes=8)
        self.assertEqual(original, histograms)
        for number in range(9):
            current = {**numbers, "writer": (100 * number, 10_000 - 1000 * number)}
            for actor, values in current.items():
                for index, column in enumerate(("exp", "stone")):
                    expected = 1 + sum(value > values[index] for value in {row[index] for row in current.values()})
                    with self.subTest(number=number, actor=actor, column=column):
                        self.assertEqual(expected, states[actor][number][column])
                        self.assertEqual(sqlite_int_sort_key(values[index]).hex(), states[actor][number][column + "_key"])
        self.assertEqual(states["writer"][1]["exp"], states["tie"][1]["exp"])
        self.assertEqual(states["writer"][1]["stone"], states["tie"][1]["stone"])
        with self.assertRaisesRegex(ValueError, "absent"):
            fixture._rank_states({"exp": {}, "stone": {}}, scores, "writer", ["writer"])
        for maximum in (-1, 97):
            with self.assertRaisesRegex(ValueError, "bounded"):
                fixture._rank_states(histograms, scores, "writer", ["writer"], max_writes=maximum)

    def test_small_streaming_histogram_preserves_raw_types_and_enforces_group_budget(self):
        values = [(None, None), (b"-1000000000000000000000000000001", -(10**30 + 1)),
                  ("invalid", 0), (0, 0), ("1e3", 1000), (b"1000", 1000),
                  (2**63 - 1, 2**63 - 1), (b"1000000000000000000000000000001", 10**30 + 1)]
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "oracle.db"
            with sqlite3.connect(database) as connection:
                register_numeric_functions(connection)
                connection.execute("CREATE TABLE user_xiuxian(user_id TEXT,exp,stone)")
                connection.executemany("INSERT INTO user_xiuxian VALUES(?,?,?)",
                                       [(str(index), raw, raw) for index, (raw, _) in enumerate(values)])
                connection.execute("CREATE TABLE xiuxian_numeric_rank_generation(singleton,generation,exp_generation,stone_generation)")
                connection.execute("INSERT INTO xiuxian_numeric_rank_generation VALUES(1,100,80,90)")
            actors = [str(index) for index in range(1, len(values))]
            scores = {str(index): {column: {"canonical": number} for column in ("exp", "stone")}
                      for index, (_, number) in enumerate(values) if number is not None}
            oracle, evidence = fixture._dynamic_rank_oracle(database, scores, "3", actors, max_writes=3)
            self.assertEqual({"generation": 100, "exp_generation": 80, "stone_generation": 90}, oracle["base_generation"])
            self.assertEqual(0, evidence["request_time_oracle_sql"])
            self.assertEqual({"distinct_groups": 5, "rows": 7}, evidence["columns"]["exp"])
            for index, actor in enumerate(actors, 1):
                expected = 1 + sum(number > values[index][1] for number in
                                   {number for _, number in values if number is not None})
                self.assertEqual(expected, oracle["actors"][actor][0]["exp"])
            with self.assertRaisesRegex(ValueError, "distinct-group bound"):
                fixture._dynamic_rank_oracle(database, scores, "3", actors, max_groups=1)
            with sqlite3.connect(database) as connection:
                self.assertEqual(["null", "blob", "text", "integer", "text", "blob", "integer", "blob"],
                                 [row[0] for row in connection.execute("SELECT typeof(exp) FROM user_xiuxian")])

    def test_seed_changes_only_sixteen_low_readers_and_refreshes_initial_oracle_hash(self):
        saved = manifest()
        saved.update(initial={actor: {"exp": values["exp"]["canonical"],
                                      "stone": values["stone"]["canonical"], "power": 1}
                              for actor, values in saved["score_evidence"].items()},
                     exp_cap=10_000_000, power_multiplier=1.5, limits=["608 requests old", "population retained"],
                     seed_sha256="stale")
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "seed.db"
            with sqlite3.connect(database) as connection:
                register_numeric_functions(connection)
                connection.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT,exp,stone,power)")
                for index, actor in enumerate(saved["actors"]):
                    initial = saved["initial"][actor]
                    exp, stone = initial["exp"], initial["stone"]
                    if actor in saved["groups"]["preserved_background"]:
                        exp, stone = float(10**18 + index * 1000), float(10**19 + index * 1000)
                    elif actor in saved["groups"]["exact_blob"]:
                        exp, stone = str(10**30 + index).encode("ascii"), str(10**31 + index).encode("ascii")
                    connection.execute("INSERT INTO user_xiuxian VALUES(?,?,?,?,?)", (index + 1, actor, exp, stone, 1))
                connection.execute("CREATE TABLE xiuxian_numeric_rank_generation(singleton,generation,exp_generation,stone_generation)")
                connection.execute("INSERT INTO xiuxian_numeric_rank_generation VALUES(1,100,80,90)")
            saved["score_evidence"] = fixture.strata._score_evidence(fixture.strata._raw_scores(database, saved["actors"]))
            original = deepcopy(saved["score_evidence"])
            context = SimpleNamespace(paths=SimpleNamespace(game_db=database))
            manager = SimpleNamespace(_clear_read_cache=Mock(), _clear_field_list_cache=Mock())
            with patch.object(fixture.relation, "seed", return_value=saved) as inherited:
                seeded = fixture.seed(context, [manager])
            inherited.assert_called_once_with(context, [manager], participants=128)
            writer = saved["groups"]["training_low"][0]
            initial = seeded["initial"][writer]
            changed = set(saved["groups"]["training_low"][1:17])
            for actor in saved["actors"]:
                if actor not in changed:
                    self.assertEqual(original[actor], seeded["score_evidence"][actor])
            for index, actor in enumerate(saved["groups"]["training_low"][1:17], 1):
                self.assertEqual(initial["exp"] + 400 * index, seeded["initial"][actor]["exp"])
                self.assertEqual(initial["stone"] - 4000 * index, seeded["initial"][actor]["stone"])
                self.assertEqual(int((initial["exp"] + 400 * index) * 1.5 + .5), seeded["initial"][actor]["power"])
            self.assertEqual(seeded["dynamic_rank_oracle"]["base_generation"], seeded["setup_generation"])
            self.assertEqual(25, len(seeded["dynamic_rank_oracle"]["actors"]))
            self.assertNotEqual("stale", seeded["seed_sha256"])
            self.assertEqual(seeded["seed_sha256"], fixture.strata.gameplay._digest(
                {key: value for key, value in seeded.items() if key != "seed_sha256"}))
            manager._clear_read_cache.assert_called_once_with()
            manager._clear_field_list_cache.assert_called_once_with()


class DynamicRelationInvariantTests(unittest.TestCase):
    def test_delegated_failures_generation_other_users_and_snapshot_bounds_remain_visible(self):
        saved = manifest()
        plan = fixture.request_plan(saved, "mixed", 192, 1)
        profile = profile_result(plan[0], 1)
        profile["profile_display_observations"] = [{"actor": profile["request"]["actor"],
                                                   "exp_key": "00", "stone_key": "00"}]
        write = {"request": next(row for row in plan if row["key"] == "stone_training"), "outcome": "success"}
        base = saved["dynamic_rank_oracle"]["base_generation"]
        checked = {"ok": True, "errors": [], "generation_after": {field: value + 1 for field, value in base.items()},
                   "invariants": "delegated relation invariants"}
        readonly = {actor: values for actor, values in saved["score_evidence"].items()
                    if actor != saved["dynamic_rank_oracle"]["writer"]}
        context = SimpleNamespace(paths=SimpleNamespace(game_db="unused-small-fixture"))
        with patch.object(fixture.relation, "check_invariants", return_value=checked) as inherited, \
                patch.object(fixture.strata, "_raw_scores", return_value=[]) as raw, \
                patch.object(fixture.strata, "_score_evidence", return_value=readonly) as evidence:
            result = fixture.check_invariants(context, [], saved, [profile, write])
            self.assertTrue(result["ok"])
            self.assertEqual(127, result["nonwriter_score_checks"])
            self.assertEqual(127, len(raw.call_args.args[1]))
            self.assertFalse(result["dynamic_rank_checks"]["atomic_profile_claim"])
            self.assertEqual(1, result["dynamic_rank_checks"]["display_rank_snapshot_divergences"])
            self.assertEqual({1: 1}, result["dynamic_rank_checks"]["observed_write_offsets"])
            checked["errors"] = ["relation rows or pending mentor invitations changed"]
            result = fixture.check_invariants(context, [], saved, [profile, write])
            self.assertFalse(result["ok"])
            self.assertIn(checked["errors"][0], result["errors"])
            checked["errors"] = []
            checked["generation_after"]["exp_generation"] += 1
            self.assertFalse(fixture.check_invariants(context, [], saved, [profile, write])["ok"])
            checked["generation_after"]["exp_generation"] -= 1
            evidence.return_value = {}
            self.assertFalse(fixture.check_invariants(context, [], saved, [profile, write])["ok"])
            evidence.return_value = readonly
            future_profile = profile_result(plan[0], 2)
            result = fixture.check_invariants(context, [], saved, [future_profile, write])
            self.assertIn("profile rank snapshot exceeds the final fresh write count", result["errors"])
            other_write = deepcopy(write)
            other_write["request"]["actor"] = saved["groups"]["training_low"][1]
            result = fixture.check_invariants(context, [], saved, [profile, other_write])
            self.assertIn("dynamic traffic wrote a user other than the sole writer", result["errors"])
            self.assertEqual(6, inherited.call_count)


if __name__ == "__main__":
    unittest.main()
