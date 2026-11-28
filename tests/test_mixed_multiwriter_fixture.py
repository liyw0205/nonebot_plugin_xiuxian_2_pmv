from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from itertools import combinations
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from nonebot_plugin_xiuxian_2.core.numeric import sqlite_int_sort_key
from tests import mixed_multiwriter_fixture as fixture
from tests.test_mixed_relation_fixture import manifest as relation_manifest, reply_for


def score(exp, stone, *, storage="integer"):
    return {"power": int(exp * 1.5 + .5), **{
        column: {"canonical": value, "storage": storage, "key_hex": sqlite_int_sort_key(value).hex()}
        for column, value in (("exp", exp), ("stone", stone))}}


def manifest():
    saved = relation_manifest()
    low, high, background = (saved["groups"][name]
                              for name in ("training_low", "exact_blob", "preserved_background"))
    scores = {actor: score(1000 + index * 256, 1_000_000 + index * 1_000_037)
              for index, actor in enumerate(saved["actors"])}
    for actor in low[:32]:
        scores[actor] = score(1000, 1_000_000)
    for index, actor in enumerate(low[32:48]):
        scores[actor] = score(979 + 10 * index, 1_000_210 - 100 * index)
    for index, actor in enumerate(background):
        scores[actor] = score(10**20 + index * 65_536, 10**21 + index * 65_536, storage="real")
    for index, actor in enumerate(high):
        scores[actor] = score(10**30 + index, 10**31 + index, storage="blob")
    histograms = {column: Counter(sqlite_int_sort_key(values[column]["canonical"])
                                  for values in scores.values()) for column in ("exp", "stone")}
    actors = low[:48] + high[:8]
    saved.update(score_evidence=scores, rank_stability={
        "dynamic_low_readers": low[32:48], "exact_high_readers": high[:8]},
        dynamic_rank_oracle={
            "base_generation": {"generation": 100, "exp_generation": 80, "stone_generation": 90},
            "writers": low[:32], "max_writes": 32, "state_offsets": [0, 1, 32],
            "actors": fixture._rank_states(histograms, scores, low[:32], actors),
        })
    return saved


def profile_result(request, number, written=None):
    oracle = request["dynamic_rank_oracle"]
    candidates = oracle["states"][0 if number == 0 else 2 if number == oracle["max_writes"] else 1]
    state = next((item for item in candidates if item["write_state"] == written), candidates[0])
    return {"request": request, "key": request["key"], "outcome": "success",
            "replies": [reply_for({**request, "expected_ranks": state})],
            "rank_state_observations": [{"actor": request["actor"],
                "exp_generation": oracle["base_generation"]["exp_generation"] + number,
                "stone_generation": oracle["base_generation"]["stone_generation"] + number,
                "exp_key": state["exp_key"], "stone_key": state["stone_key"]}]}


class MultiwriterPlanTests(unittest.TestCase):
    def test_frozen_plans_cover_all_profile_cohorts_and_unique_writers(self):
        saved, numbers = manifest(), set()
        specs = fixture.phase_specs()
        self.assertEqual([(128, 10, 20), (128, 20, 35), (128, 20, 50)],
                         [(row["requests"], row["concurrency"], row["arrival_rate"]) for row in specs])
        for phase, spec in enumerate(specs):
            plan = fixture.request_plan(saved, spec["mode"], 128, phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 128, phase))
            expected = {"profile_info": 56, "my_mentor": 36, "my_partner": 36} if phase == 0 else {
                "profile_info": 56, "my_mentor": 20, "my_partner": 20, "stone_training": 32}
            self.assertEqual(expected, dict(Counter(row["key"] for row in plan)))
            current = {row["number"] for row in plan}
            self.assertEqual(128, len(current))
            self.assertFalse(numbers & current)
            numbers.update(current)
            profiles = [row for row in plan if row["key"] == "profile_info"]
            self.assertEqual(set(saved["dynamic_rank_oracle"]["actors"]), {row["actor"] for row in profiles})
            self.assertEqual({"writer": 32, "dynamic_low": 16, "exact_high": 8},
                             dict(Counter(row["rank_cohort"] for row in profiles)))
            self.assertTrue(all(len(row["dynamic_rank_oracle"]["states"]) == 3 for row in profiles))
            self.assertTrue(all("actors" not in row["dynamic_rank_oracle"] and "expected_ranks" not in row
                                for row in profiles))
            writes = [row for row in plan if row["key"] == "stone_training"]
            if phase:
                self.assertEqual(Counter(saved["groups"]["training_low"][:32]),
                                 Counter(row["actor"] for row in writes))
            self.assertFalse(set(saved["groups"]["preserved_background"]) & {row["actor"] for row in plan})
            self.assertEqual({"我的师徒", "我的徒弟", "我的师父", "我的师傅"},
                             {row["command"] for row in plan if row["key"] == "my_mentor"})
            if spec["mode"] == "contention":
                for index in range(0, 128, 8):
                    self.assertEqual(["profile_info", "profile_info", "stone_training", "stone_training"],
                                     [row["key"] for row in plan[index:index + 4]])
                    self.assertEqual([row["actor"] for row in plan[index:index + 2]],
                                     [row["actor"] for row in plan[index + 2:index + 4]])
        self.assertEqual(384, len(numbers))
        for mode, count, phase in (("pure", 127, 0), ("mixed", 128, 0), ("pure", 128, -1), ("mixed", 128, 3)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(saved, mode, count, phase)

    def test_generation_requires_the_observed_whole_target_pair(self):
        request = fixture.request_plan(manifest(), "pure", 128)[0]
        for number, written in ((0, 0), (1, 0), (1, 1), (31, 0), (31, 1), (32, 1)):
            result = profile_result(request, number, written)
            self.assertEqual("success", fixture.classify_result(request, result)["outcome"])
        old = request["dynamic_rank_oracle"]["states"][0][0]
        new = request["dynamic_rank_oracle"]["states"][2][0]
        for number, exp_key, stone_key in (
            (0, new["exp_key"], new["stone_key"]), (32, old["exp_key"], old["stone_key"]),
            (1, old["exp_key"], new["stone_key"]), (1, new["exp_key"], old["stone_key"]),
        ):
            result = profile_result(request, number)
            result["rank_state_observations"][0].update(exp_key=exp_key, stone_key=stone_key)
            self.assertEqual("multiwriter_rank_target_key_mismatch", fixture.classify_result(request, result)["reason"])
        alterations = (
            ("multiwriter_rank_state_count_mismatch", lambda row: row.update(rank_state_observations=[])),
            ("multiwriter_rank_state_count_mismatch", lambda row: row["rank_state_observations"].append(
                deepcopy(row["rank_state_observations"][0]))),
            ("multiwriter_rank_state_actor_mismatch", lambda row: row["rank_state_observations"][0].update(actor="other")),
            ("multiwriter_rank_generation_mismatch", lambda row: row["rank_state_observations"][0].update(exp_generation=82)),
            ("multiwriter_rank_generation_invalid", lambda row: row["rank_state_observations"][0].update(exp_generation=True)),
            ("multiwriter_rank_generation_out_of_range", lambda row: row["rank_state_observations"][0].update(
                exp_generation=79, stone_generation=89)),
            ("multiwriter_rank_generation_out_of_range", lambda row: row["rank_state_observations"][0].update(
                exp_generation=113, stone_generation=123)),
        )
        for reason, alter in alterations:
            result = profile_result(request, 1)
            alter(result)
            with self.subTest(reason=reason):
                self.assertEqual(reason, fixture.classify_result(request, result)["reason"])
        for column, title in (("exp", "修为排行"), ("stone", "灵石排行")):
            result = profile_result(request, 1)
            rank = request["dynamic_rank_oracle"]["states"][1][0][column]
            result["replies"][0] = result["replies"][0].replace(f"{title}: 第{rank}位", f"{title}: 第99999位")
            self.assertEqual("exact_" + column + "_rank_mismatch", fixture.classify_result(request, result)["reason"])

    def test_business_failures_and_inherited_relation_checks_remain_visible(self):
        plan = fixture.request_plan(manifest(), "pure", 128)
        request = plan[0]
        for fields, outcome in (({"errors": ["failure"]}, "exception"),
                                ({"blocked": ["outside"]}, "boundary"),
                                ({"replies": ["慢...慢一..点"]}, "cooldown")):
            result = {**profile_result(request, 1), **fields, "rank_state_observations": []}
            self.assertEqual(outcome, fixture.classify_result(request, result)["outcome"])
        failure = {"outcome": "unclassified", "reason": "profile_relation_mismatch"}
        with patch.object(fixture.relation, "classify_result", return_value=failure) as inherited:
            self.assertEqual(failure, fixture.classify_result(request, profile_result(request, 1)))
            self.assertIn("expected_ranks", inherited.call_args.args[0])
            self.assertEqual(failure, fixture.classify_result(plan[1], {}))


class MultiwriterOracleTests(unittest.TestCase):
    def test_symmetric_subsets_ties_and_huge_signed_values_match_independent_dense_ranks(self):
        huge = 10**100
        numbers = {"w0": (0, 10_000), "w1": (0, 10_000), "w2": (0, 10_000),
                   "tie_old": (0, 10_000), "tie_new": (100, 9000), "inside": (50, 9500),
                   "low": (-huge, -huge), "high": (huge, huge)}
        writers = ["w0", "w1", "w2"]
        scores = {actor: score(*values) for actor, values in numbers.items()}
        histograms = {column: Counter(sqlite_int_sort_key(values[index]) for values in numbers.values())
                      for index, column in enumerate(("exp", "stone"))}
        original = deepcopy(histograms)
        states = fixture._rank_states(histograms, scores, writers, list(numbers))
        self.assertEqual(original, histograms)
        for number in range(4):
            for changed in combinations(writers, number):
                current = {actor: (values[0] + 100, values[1] - 1000) if actor in changed else values
                           for actor, values in numbers.items()}
                for actor, values in current.items():
                    candidates = states[actor][0 if number == 0 else 2 if number == 3 else 1]
                    own_state = int(actor in changed) if actor in writers else None
                    selected, = [candidate for candidate in candidates if candidate["write_state"] == own_state]
                    for index, column in enumerate(("exp", "stone")):
                        expected = 1 + sum(value > values[index] for value in {row[index] for row in current.values()})
                        with self.subTest(number=number, changed=changed, actor=actor, column=column):
                            self.assertEqual(expected, selected[column])
                            self.assertEqual(sqlite_int_sort_key(values[index]).hex(), selected[column + "_key"])
        with self.assertRaisesRegex(ValueError, "bounded"):
            fixture._rank_states(histograms, scores, writers + ["w0"], list(numbers))
        bad = deepcopy(scores)
        bad["w1"] = score(50, 9500)
        with self.assertRaisesRegex(ValueError, "identical"):
            fixture._rank_states(histograms, bad, writers, list(numbers))
        bad_histogram = deepcopy(histograms)
        bad_histogram["exp"][sqlite_int_sort_key(0)] = 2
        with self.assertRaisesRegex(ValueError, "multiplicity"):
            fixture._rank_states(bad_histogram, scores, writers, list(numbers))

    def test_seed_refreshes_only_48_low_users_and_preserves_other_raw_scores(self):
        saved = manifest()
        low = saved["groups"]["training_low"]
        for index, actor in enumerate(low):
            saved["score_evidence"][actor] = score(1000 + index * 256, 1_000_000 + index * 1_000_037)
        saved.update(initial={actor: {"exp": values["exp"]["canonical"], "stone": values["stone"]["canonical"],
                                      "power": values["power"]} for actor, values in saved["score_evidence"].items()},
                     exp_cap=10_000_000, power_multiplier=1.5, limits=["608 requests old", "population retained"],
                     seed_sha256="stale")
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

        def oracle(database, scores, writers, actors):
            histograms = {column: Counter(sqlite_int_sort_key(values[column]["canonical"])
                                          for values in scores.values()) for column in ("exp", "stone")}
            base = {"generation": 100, "exp_generation": 80, "stone_generation": 90}
            return {"base_generation": base, "writers": writers, "max_writes": 32,
                    "actors": fixture._rank_states(histograms, scores, writers, actors)}, {
                        "columns": {column: {"old_key_count": histograms[column][sqlite_int_sort_key(value)],
                                              "new_key_count": histograms[column][sqlite_int_sort_key(value + delta)]}
                                    for column, value, delta in (("exp", 1000, 100), ("stone", 1_000_000, -1000))}}

        manager = SimpleNamespace(_clear_read_cache=Mock(), _clear_field_list_cache=Mock())
        context = SimpleNamespace(paths=SimpleNamespace(game_db="unused-test-db"))
        with patch.object(fixture.relation, "seed", return_value=saved) as inherited, \
                patch.object(fixture.strata.shared, "_uow", uow), \
                patch.object(fixture.strata, "_raw_scores", return_value=[]), \
                patch.object(fixture.strata, "_score_evidence", side_effect=lambda _: deepcopy(current)), \
                patch.object(fixture, "_dynamic_rank_oracle", side_effect=oracle):
            seeded = fixture.seed(context, [manager])
        inherited.assert_called_once_with(context, [manager], participants=128)
        self.assertEqual(low[:48], updates)
        for actor in saved["actors"]:
            if actor not in low[:48]:
                self.assertEqual(original[actor], seeded["score_evidence"][actor])
        for actor in low[:32]:
            self.assertEqual((1000, 1_000_000), tuple(seeded["initial"][actor][key] for key in ("exp", "stone")))
        for index, actor in enumerate(low[32:48]):
            self.assertEqual((979 + 10 * index, 1_000_210 - 100 * index),
                             tuple(seeded["initial"][actor][key] for key in ("exp", "stone")))
        self.assertEqual(56, len(seeded["dynamic_rank_oracle"]["actors"]))
        self.assertEqual(seeded["seed_sha256"], fixture.strata.gameplay._digest(
            {key: value for key, value in seeded.items() if key != "seed_sha256"}))
        manager._clear_read_cache.assert_called_once_with()
        manager._clear_field_list_cache.assert_called_once_with()


class MultiwriterInvariantTests(unittest.TestCase):
    def test_original_checks_and_generation_identity_nonwriter_bounds_are_preserved(self):
        saved = manifest()
        plan = fixture.request_plan(saved, "mixed", 128, 1)
        request = next(row for row in plan if row["key"] == "profile_info")
        profile = profile_result(request, 1, 1)
        profile["profile_display_observations"] = [{"actor": request["actor"], "exp_key": "00", "stone_key": "00"}]
        write = {"request": next(row for row in plan if row["key"] == "stone_training"), "outcome": "success"}
        base = saved["dynamic_rank_oracle"]["base_generation"]
        checked = {"ok": True, "errors": [], "generation_after": {key: value + 1 for key, value in base.items()},
                   "invariants": "all inherited relation/asset/receipt/feature/JSON checks"}
        readonly = {actor: values for actor, values in saved["score_evidence"].items()
                    if actor not in saved["dynamic_rank_oracle"]["writers"]}
        context = SimpleNamespace(paths=SimpleNamespace(game_db="unused-test-db"))
        with patch.object(fixture.relation, "check_invariants", return_value=checked) as inherited, \
                patch.object(fixture.strata, "_raw_scores", return_value=[]) as raw, \
                patch.object(fixture.strata, "_score_evidence", return_value=readonly) as evidence:
            result = fixture.check_invariants(context, [], saved, [profile, write])
            self.assertTrue(result["ok"])
            self.assertEqual(96, result["nonwriter_score_checks"])
            self.assertEqual(96, len(raw.call_args.args[1]))
            self.assertEqual({"new": 1}, result["dynamic_rank_checks"]["observed_target_states"])
            self.assertEqual({1: 1}, result["dynamic_rank_checks"]["observed_write_offsets"])
            self.assertEqual(1, result["dynamic_rank_checks"]["display_rank_snapshot_divergences"])
            self.assertFalse(result["dynamic_rank_checks"]["atomic_profile_claim"])
            checked["errors"] = ["relation rows or pending mentor invitations changed"]
            self.assertIn(checked["errors"][0], fixture.check_invariants(context, [], saved, [profile, write])["errors"])
            checked["errors"] = []
            checked["generation_after"]["exp_generation"] += 1
            self.assertFalse(fixture.check_invariants(context, [], saved, [profile, write])["ok"])
            checked["generation_after"]["exp_generation"] -= 1
            evidence.return_value = {}
            self.assertFalse(fixture.check_invariants(context, [], saved, [profile, write])["ok"])
            evidence.return_value = readonly
            duplicate = deepcopy(write)
            duplicate["request"]["number"] += 10_000
            self.assertIn("multiwriter traffic must attempt at most one write per permitted writer",
                          fixture.check_invariants(context, [], saved, [profile, write, duplicate])["errors"])
            outsider = deepcopy(write)
            outsider["request"]["actor"] = saved["groups"]["training_low"][50]
            self.assertIn("multiwriter traffic must attempt at most one write per permitted writer",
                          fixture.check_invariants(context, [], saved, [profile, outsider])["errors"])
            self.assertIn("multiwriter rank snapshot exceeds the final fresh write count",
                          fixture.check_invariants(context, [], saved, [profile_result(request, 2), write])["errors"])
            self.assertEqual(7, inherited.call_count)


if __name__ == "__main__":
    unittest.main()
