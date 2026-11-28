from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from itertools import combinations
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from nonebot_plugin_xiuxian_2.core.numeric import sqlite_int_sort_key
from tests import mixed_player_projection_fixture as fixture
from tests.test_mixed_multiwriter_fixture import profile_result, score
from tests.test_mixed_relation_fixture import manifest as relation_manifest


def manifest():
    saved = relation_manifest()
    low, high, background = (saved["groups"][name]
                              for name in ("training_low", "exact_blob", "preserved_background"))
    scores = {}
    for index, actor in enumerate(low[:32]):
        scores[actor] = score(832_000 + 1000 * index, 500_000_000 + 10_000 * index)
    for index, actor in enumerate(low[32:]):
        scores[actor] = score(832_050 + 1000 * index, 499_999_500 + 10_000 * index)
    for index, actor in enumerate(background):
        scores[actor] = score(10**20 + index * 65_536, 10**21 + index * 65_536, storage="real")
    for index, actor in enumerate(high):
        scores[actor] = score(10**30 + index, 10**31 + index, storage="blob")
    histograms = {column: Counter(sqlite_int_sort_key(values[column]["canonical"])
                                  for values in scores.values()) for column in ("exp", "stone")}
    actors = low[:32] + high[:24]
    saved.update(score_evidence=scores, rank_stability={
        "fixed_pivots": low[32:], "dynamic_low_readers": [], "exact_high_readers": high[:24]},
        dynamic_rank_oracle={
            "base_generation": {"generation": 100, "exp_generation": 80, "stone_generation": 90},
            "writers": low[:32], "max_writes": 32, "state_offsets": [0, 1, 32],
            "actors": fixture._rank_states(histograms, scores, low[:32], actors, low[32:]),
        }, player_projection={
            "feature_table_rows": {"avatar": 8192, "title": 8192},
            "title_names": {"t0": "ProjectionFirst", "t1": "ProjectionSecond"},
            "snapshot": {"title": {"actor_rows": {
                actor: {"equipped": f"t{index % 2}"} for index, actor in enumerate(saved["actors"])}}},
            "limits": ["nonempty player display rows; active=self"],
        })
    return saved


def titled_result(request, number, written=None):
    result = profile_result(request, number, written)
    result["replies"][0] = result["replies"][0].replace(
        "\n境界:", f"\n称号: {request['expected_title_name']}\n境界:", 1)
    return result


class PlayerProjectionOracleTests(unittest.TestCase):
    def test_every_writer_subset_matches_independent_full_dense_ranks(self):
        huge = 10**100
        writers, pivots = ["w0", "w1", "w2"], ["p0", "p1", "p2"]
        numbers = {"w0": (-huge, 10_000), "w1": (0, 20_000), "w2": (huge, 30_000),
                   "p0": (-huge + 50, 9500), "p1": (50, 19_500), "p2": (huge + 50, 29_500),
                   "low": (-huge - 10_000, -huge), "high": (huge + 10_000, huge),
                   "tied_high": (huge + 10_000, huge)}
        scores = {actor: score(*values) for actor, values in numbers.items()}
        histogram = {column: Counter(sqlite_int_sort_key(values[index]) for values in numbers.values())
                     for index, column in enumerate(("exp", "stone"))}
        original = deepcopy(histogram)
        actors = writers + ["low", "high", "tied_high"]
        states = fixture._rank_states(histogram, scores, writers, actors, pivots)
        self.assertEqual(original, histogram)
        for number in range(4):
            for changed in combinations(writers, number):
                current = {actor: (values[0] + 100, values[1] - 1000) if actor in changed else values
                           for actor, values in numbers.items()}
                for actor in actors:
                    written = int(actor in changed) if actor in writers else None
                    candidates = states[actor][0 if number == 0 else 2 if number == 3 else 1]
                    selected, = [row for row in candidates if row["write_state"] == written]
                    for index, column in enumerate(("exp", "stone")):
                        expected = 1 + sum(value > current[actor][index]
                                           for value in {row[index] for row in current.values()})
                        with self.subTest(changed=changed, actor=actor, column=column):
                            self.assertEqual(expected, selected[column])
                            self.assertEqual(sqlite_int_sort_key(current[actor][index]).hex(),
                                             selected[column + "_key"])
        for actor in writers:
            old, new = states[actor][0][0], states[actor][2][0]
            self.assertEqual(old["exp"] - 1, new["exp"])
            self.assertEqual(old["stone"] + 1, new["stone"])

    def test_unproved_rank_shapes_and_invalid_bounds_are_rejected(self):
        saved = manifest()
        writers, actors = saved["dynamic_rank_oracle"]["writers"], list(saved["dynamic_rank_oracle"]["actors"])
        pivots, scores = saved["rank_stability"]["fixed_pivots"], saved["score_evidence"]
        histogram = {column: Counter(sqlite_int_sort_key(values[column]["canonical"])
                                     for values in scores.values()) for column in ("exp", "stone")}
        with self.assertRaisesRegex(ValueError, "bounded"):
            fixture._rank_states(histogram, scores, writers, actors + [pivots[0]], pivots)
        overlapping = deepcopy(scores)
        overlapping[writers[1]] = score(832_050, 500_010_000)
        with self.assertRaisesRegex(ValueError, "overlap"):
            fixture._rank_states(histogram, overlapping, writers, actors, pivots)
        for key, count in ((832_000, 2), (832_100, 1)):
            bad = deepcopy(histogram)
            bad["exp"][sqlite_int_sort_key(key)] = count
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "endpoint"):
                fixture._rank_states(bad, scores, writers, actors, pivots)
        extra_pivot = deepcopy(histogram)
        extra_pivot["exp"][sqlite_int_sort_key(832_025)] = 1
        with self.assertRaisesRegex(ValueError, "unique fixed pivot"):
            fixture._rank_states(extra_pivot, scores, writers, actors, pivots)
        for bound in (0, fixture.ORACLE_GROUPS + 1):
            with self.subTest(bound=bound), self.assertRaisesRegex(ValueError, "histogram bound"):
                fixture._dynamic_rank_oracle("not-opened", scores, writers, actors, pivots, max_groups=bound)


class PlayerProjectionPlanTests(unittest.TestCase):
    def test_all_phases_cover_distinct_writer_profiles_titles_and_disjoint_requests(self):
        saved, numbers = manifest(), set()
        specs = fixture.phase_specs()
        self.assertEqual([(128, 10, 20), (128, 20, 35), (128, 20, 50)],
                         [(row["requests"], row["concurrency"], row["arrival_rate"]) for row in specs])
        for phase, spec in enumerate(specs):
            plan = fixture.request_plan(saved, spec["mode"], spec["requests"], phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], spec["requests"], phase))
            current = {row["number"] for row in plan}
            self.assertEqual(128, len(current))
            self.assertFalse(numbers & current)
            numbers.update(current)
            profiles = [row for row in plan if row["key"] == "profile_info"]
            self.assertEqual({"writer": 32, "exact_high": 24}, dict(Counter(row["rank_cohort"] for row in profiles)))
            self.assertEqual(set(saved["dynamic_rank_oracle"]["actors"]), {row["actor"] for row in profiles})
            self.assertFalse(set(saved["rank_stability"]["fixed_pivots"]) & {row["actor"] for row in profiles})
            self.assertEqual({"ProjectionFirst", "ProjectionSecond"}, {row["expected_title_name"] for row in profiles})
            self.assertTrue(all(len(row["dynamic_rank_oracle"]["states"]) == 3 for row in profiles))
            self.assertEqual((0, 32, 32)[phase], sum(row["key"] == "stone_training" for row in plan))
            self.assertEqual(128, len(plan))
            if phase:
                self.assertEqual(Counter(saved["dynamic_rank_oracle"]["writers"]),
                                 Counter(row["actor"] for row in plan if row["key"] == "stone_training"))
            for request in profiles:
                result = titled_result(request, 0, 0)
                self.assertEqual("success", fixture.classify_result(request, result)["outcome"])
        self.assertEqual(384, len(numbers))
        for mode, count, phase in (("pure", 127, 0), ("mixed", 128, 0), ("pure", 128, -1)):
            with self.subTest(mode=mode, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(saved, mode, count, phase)

    def test_exact_title_and_snapshot_target_pair_are_both_required(self):
        request = fixture.request_plan(manifest(), "pure", 128)[0]
        for number, written in ((0, 0), (1, 0), (1, 1), (31, 0), (31, 1), (32, 1)):
            self.assertEqual("success", fixture.classify_result(request, titled_result(request, number, written))["outcome"])
        for number, written in ((0, 1), (32, 0)):
            result = titled_result(request, 1, written)
            observed = result["rank_state_observations"][0]
            for column in ("exp", "stone"):
                observed[column + "_generation"] = request["dynamic_rank_oracle"]["base_generation"][column + "_generation"] + number
            self.assertEqual("multiwriter_rank_target_key_mismatch", fixture.classify_result(request, result)["reason"])
        result = titled_result(request, 1, 1)
        result["rank_state_observations"][0]["stone_key"] = request["dynamic_rank_oracle"]["states"][0][0]["stone_key"]
        self.assertEqual("multiwriter_rank_target_key_mismatch", fixture.classify_result(request, result)["reason"])
        result = titled_result(request, 1, 1)
        result["replies"][0] = result["replies"][0].replace(request["expected_title_name"], "wrong-title")
        self.assertEqual("profile_equipped_title_mismatch", fixture.classify_result(request, result)["reason"])
        result = titled_result(request, 1, 1)
        result["replies"][0] += f"\n称号: {request['expected_title_name']}\n"
        self.assertEqual("profile_equipped_title_mismatch", fixture.classify_result(request, result)["reason"])
        for fields, outcome in (({"errors": ["failure"]}, "exception"),
                                ({"blocked": ["outside"]}, "boundary"),
                                ({"replies": ["慢...慢一..点"]}, "cooldown")):
            self.assertEqual(outcome, fixture.classify_result(request, {**titled_result(request, 1), **fields})["outcome"])


class PlayerProjectionSeedTests(unittest.TestCase):
    def test_seed_changes_only_low_scores_and_seeds_bounded_player_tables(self):
        saved = manifest()
        low = saved["groups"]["training_low"]
        for index, actor in enumerate(low):
            saved["score_evidence"][actor] = score(1000 + index * 256, 1_000_000 + index * 1_000_037)
        saved.update(initial={actor: {"exp": values["exp"]["canonical"], "stone": values["stone"]["canonical"],
                                      "power": values["power"]} for actor, values in saved["score_evidence"].items()},
                     exp_cap=10_000_000, power_multiplier=1.5, limits=["608 requests old", "population retained"],
                     feature_table_rows={}, seed_sha256="stale")
        original, current = deepcopy(saved["score_evidence"]), deepcopy(saved["score_evidence"])
        updates = []

        def execute(sql, values):
            self.assertIn("UPDATE user_xiuxian SET exp=?,stone=?,power=? WHERE user_id=?", sql)
            exp, stone, power, actor = values
            updates.append(actor)
            current[actor] = score(exp, stone)
            self.assertEqual(power, current[actor]["power"])

        @contextmanager
        def uow(database):
            yield SimpleNamespace(execute=execute)

        def oracle(database, scores, writers, actors, pivots):
            histogram = {column: Counter(sqlite_int_sort_key(values[column]["canonical"])
                                         for values in scores.values()) for column in ("exp", "stone")}
            base = {"generation": 100, "exp_generation": 80, "stone_generation": 90}
            return {"base_generation": base, "writers": writers, "max_writes": 32,
                    "actors": fixture._rank_states(histogram, scores, writers, actors, pivots)}, {"columns": {}}

        projection = deepcopy(saved["player_projection"])
        manager = SimpleNamespace(_clear_read_cache=Mock(), _clear_field_list_cache=Mock())
        context = SimpleNamespace(paths=SimpleNamespace(game_db="not-opened", player_db="not-opened-player"))
        with patch.object(fixture.relation, "seed", return_value=saved) as inherited, \
                patch.object(fixture.strata.shared, "_uow", uow), \
                patch.object(fixture.strata, "_raw_scores", return_value=[]), \
                patch.object(fixture.strata, "_score_evidence", side_effect=lambda _: deepcopy(current)), \
                patch.object(fixture, "_dynamic_rank_oracle", side_effect=oracle), \
                patch.object(fixture.player_seed, "seed", return_value=projection) as player_seed:
            seeded = fixture.seed(context, [manager])
        inherited.assert_called_once_with(context, [manager], participants=128)
        player_seed.assert_called_once_with(context.paths, saved["actors"])
        self.assertEqual(low, updates)
        for actor in saved["actors"]:
            if actor not in low:
                self.assertEqual(original[actor], seeded["score_evidence"][actor])
        self.assertEqual({"avatar": 8192, "title": 8192}, seeded["feature_table_rows"])
        self.assertEqual(56, len(seeded["dynamic_rank_oracle"]["actors"]))
        self.assertEqual(seeded["seed_sha256"], fixture.strata.gameplay._digest(
            {key: value for key, value in seeded.items() if key != "seed_sha256"}))
        manager._clear_read_cache.assert_called_once_with()
        manager._clear_field_list_cache.assert_called_once_with()

    def test_invariants_keep_original_checks_and_reject_player_table_or_title_changes(self):
        saved = manifest()
        request = fixture.request_plan(saved, "pure", 128)[0]
        result = titled_result(request, 0, 0)
        checked = {"ok": True, "errors": [], "invariants": "symmetric one-write-per-actor original checks"}
        context = SimpleNamespace(paths=SimpleNamespace(game_db="not-opened"))
        with patch.object(fixture.multiwriter, "check_invariants", return_value=checked) as inherited, \
                patch.object(fixture.player_seed, "snapshot", return_value=deepcopy(saved["player_projection"]["snapshot"])) as snapshot:
            verified = fixture.check_invariants(context, [], saved, [result])
            self.assertTrue(verified["ok"])
            self.assertEqual(1, verified["equipped_title_reply_checks"])
            snapshot.assert_called_once_with(context.paths, saved["actors"])
            checked["errors"] = ["existing asset/receipt assertion"]
            self.assertIn(checked["errors"][0], fixture.check_invariants(context, [], saved, [result])["errors"])
            checked["errors"] = []
            snapshot.return_value = {}
            self.assertIn("player-projection traffic changed avatar/title rows",
                          fixture.check_invariants(context, [], saved, [result])["errors"])
            snapshot.return_value = saved["player_projection"]["snapshot"]
            result["replies"][0] = result["replies"][0].replace(request["expected_title_name"], "wrong-title")
            self.assertIn("profile_equipped_title_mismatch", fixture.check_invariants(context, [], saved, [result])["errors"])
            self.assertEqual(4, inherited.call_count)


if __name__ == "__main__":
    unittest.main()
