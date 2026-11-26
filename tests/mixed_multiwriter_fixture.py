"""Bounded symmetric writers with observed-key exact-rank reply checks."""
from __future__ import annotations

from bisect import bisect_right
from collections import Counter
import random

from nonebot_plugin_xiuxian_2.core.numeric import sqlite_int_sort_key
from tests import mixed_relation_fixture as relation


strata = relation.strata
PARTICIPANTS = relation.PARTICIPANTS
WRITERS = 32
ORACLE_GROUPS = 16_384
SHUFFLE_SEED = 20261202


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": 128,
             "concurrency": cap, "arrival_rate": rate, "order": "multiwriter-seed-20261202"}
            for mode, cap, rate in (("pure", 10, 20), ("mixed", 20, 35), ("contention", 20, 50))]


def _rank_states(histograms, scores, writers, actors):
    if (not 1 <= len(writers) <= WRITERS or len(set(writers)) != len(writers)
            or not actors or len(set(actors)) != len(actors) or not set(writers) <= set(actors)):
        raise ValueError("invalid bounded multiwriter targets")
    counts = {column: Counter(histograms[column]) for column in ("exp", "stone")}
    if any(not isinstance(key, bytes) or not isinstance(count, int) or isinstance(count, bool) or count <= 0
           for histogram in counts.values() for key, count in histogram.items()):
        raise ValueError("invalid multiwriter histogram")
    keys = {actor: {column: sqlite_int_sort_key(scores[actor][column]["canonical"])
                    for column in counts} for actor in actors}
    if any(key is None or key not in counts[column]
           for values in keys.values() for column, key in values.items()):
        raise ValueError("multiwriter target is absent from histogram")
    old = keys[writers[0]]
    if any(keys[actor] != old for actor in writers):
        raise ValueError("multiwriter initial scores are not identical")
    if any(counts[column][key] < len(writers) for column, key in old.items()):
        raise ValueError("multiwriter histogram lacks writer multiplicity")
    new = {column: sqlite_int_sort_key(scores[writers[0]][column]["canonical"] + delta)
           for column, delta in (("exp", strata.gameplay.EXP_GAIN),
                                 ("stone", -strata.gameplay.REQUESTED_STONE))}
    states = {actor: [] for actor in actors}
    # With one identical update per writer, all intermediate n share the same distinct keys.
    for number in (0, 1, len(writers)):
        current = {column: histogram.copy() for column, histogram in counts.items()}
        for column in counts:
            current[column][old[column]] -= number
            if not current[column][old[column]]:
                del current[column][old[column]]
            if number:
                current[column][new[column]] += number
        ordered = {column: sorted(histogram) for column, histogram in current.items()}
        for actor in actors:
            choices = ([(0, old)] if number == 0 else [(1, new)] if number == len(writers)
                       else [(0, old), (1, new)]) if actor in writers else [(None, keys[actor])]
            states[actor].append([{
                **{column: 1 + len(ordered[column]) - bisect_right(ordered[column], key)
                   for column, key in target.items()},
                **{column + "_key": key.hex() for column, key in target.items()},
                "write_state": written,
            } for written, target in choices])
    return states


def _dynamic_rank_oracle(database, scores, writers, actors, *, max_groups=ORACLE_GROUPS):
    if not 1 <= max_groups <= ORACLE_GROUPS:
        raise ValueError("invalid multiwriter histogram bound")
    histograms, evidence = {}, {}
    with strata.shared._uow(database, read_only=True) as uow:
        generation = strata._generation(uow)
        for column in ("exp", "stone"):
            expression = f'xiuxian_int_sort_key_compact("{column}")'
            cursor = uow.connection.execute(
                f"SELECT {expression},COUNT(*) FROM user_xiuxian "
                f"WHERE {expression} IS NOT NULL GROUP BY {expression} ORDER BY {expression} DESC"
            )
            histogram = {}
            while rows := cursor.fetchmany(128):
                for key, count in rows:
                    if len(histogram) >= max_groups:
                        raise ValueError("multiwriter oracle exceeds its distinct-group bound")
                    histogram[bytes(key)] = int(count)
            histograms[column] = histogram
            old = sqlite_int_sort_key(scores[writers[0]][column]["canonical"])
            delta = strata.gameplay.EXP_GAIN if column == "exp" else -strata.gameplay.REQUESTED_STONE
            new = sqlite_int_sort_key(scores[writers[0]][column]["canonical"] + delta)
            evidence[column] = {"distinct_groups": len(histogram), "rows": sum(histogram.values()),
                                "old_key_count": histogram.get(old, 0), "new_key_count": histogram.get(new, 0)}
    states = _rank_states(histograms, scores, writers, actors)
    return {"base_generation": generation, "writers": list(writers), "max_writes": len(writers),
            "state_offsets": [0, 1, len(writers)], "actors": states}, {
                "method": "setup streaming multiplicity histogram; dense ranks at n=0, intermediate, final; observed target key pair selects writer state",
                "columns": evidence, "generation": generation,
                "max_groups_per_column": max_groups, "request_time_oracle_sql": 0,
            }


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    manifest = relation.seed(context, managers, participants=participants)
    low, high = (manifest["groups"][name] for name in ("training_low", "exact_blob"))
    writers, readers = list(low[:WRITERS]), list(low[WRITERS:WRITERS + 16])
    initial = manifest["initial"][writers[0]]
    exp, stone = initial["exp"], initial["stone"]
    if exp + strata.gameplay.EXP_GAIN >= manifest["exp_cap"] or stone < strata.gameplay.REQUESTED_STONE:
        raise ValueError("multiwriter seed would hit the real training resource limits")
    with strata.shared._uow(context.paths.game_db) as uow:
        values = [(actor, exp, stone) for actor in writers]
        values += [(actor, exp - 21 + 10 * index, stone + 210 - 100 * index)
                   for index, actor in enumerate(readers)]
        for actor, actor_exp, actor_stone in values:
            power = int(actor_exp * manifest["power_multiplier"] + .5)
            uow.execute("UPDATE user_xiuxian SET exp=?,stone=?,power=? WHERE user_id=?",
                        (actor_exp, actor_stone, power, actor))
    scores = strata._score_evidence(strata._raw_scores(context.paths.game_db, manifest["actors"]))
    for actor in manifest["groups"]["preserved_background"] + high + low[WRITERS + 16:]:
        if scores[actor] != manifest["score_evidence"][actor]:
            raise ValueError("multiwriter seed changed a preserved participant")
    for actor in manifest["actors"]:
        manifest["initial"][actor].update(
            {column: scores[actor][column]["canonical"] for column in ("exp", "stone")},
            power=scores[actor]["power"],
        )
    oracle, evidence = _dynamic_rank_oracle(context.paths.game_db, scores, writers, writers + readers + high[:8])
    if any(values["old_key_count"] != WRITERS or values["new_key_count"] != 0
           for values in evidence["columns"].values()):
        raise ValueError("multiwriter endpoints collide with nonwriter scores; dense-rank crossing is not assured")
    manifest.update(
        fixture_version="mixed-multiwriter-v9", phase_specs=phase_specs(), shuffle_seed=SHUFFLE_SEED,
        score_evidence=scores, dynamic_rank_oracle=oracle, rank_oracle=evidence,
        setup_generation=oracle["base_generation"],
        expected_ranks={actor: {column: states[0][0][column] for column in ("exp", "stone")}
                        for actor, states in oracle["actors"].items()},
        rank_stability={"writers": writers, "dynamic_low_readers": readers,
                        "exact_high_readers": list(high[:8]), "max_writes": WRITERS,
                        "proof": "one identical update per writer; observed dual-generation offset fixes histogram and target key pair fixes own state"},
    )
    manifest["strata_ranges"] = {group: {column: {
        "min": min(scores[actor][column]["canonical"] for actor in members),
        "max": max(scores[actor][column]["canonical"] for actor in members),
        "storage_types": sorted({scores[actor][column]["storage"] for actor in members}),
    } for column in ("exp", "stone")} for group, members in manifest["groups"].items()}
    manifest["limits"] = [line for line in manifest["limits"] if not (
        line.startswith(("608 requests", "1024 requests", "bounded setup rank oracle"))
        or "readonly rank expectations exclude mutable low ranks" in line
    )] + [
        "384 requests; 56 profile actors per phase: 32 symmetric writers, 16 low readers, 8 exact high readers; not all 128 seeded participants dispatch",
        "mixed/contention each attempt one real write per writer; shared SQLite/generation and same-user read/write overlap, not repeated same-user write contention",
        "only three distinct histogram shapes (n=0,1..31,32); symmetric keys favor rank-cache reuse; not arbitrary multiwriter dynamic ranks",
        "bounded setup histogram <=16384 groups/column; three compact candidate states per profile actor; no request-time oracle SQL",
        "96 nonwriter participants retain raw scores/storage/keys/power; REAL/BLOB strata unchanged; profile display may predate rank snapshot",
        "actual observed offsets/overlap must be reported; finite diagnostic samples do not establish sustained 50 successful requests/s",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = strata.gameplay._digest(manifest)
    process_read_cache.clear()
    for manager in managers:
        for method in ("_clear_read_cache", "_clear_field_list_cache"):
            clear = getattr(manager, method, None)
            if callable(clear):
                clear()
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs)
            or (mode, count) != (specs[phase_number]["mode"], specs[phase_number]["requests"])):
        raise ValueError("invalid frozen multiwriter plan")
    oracle = manifest["dynamic_rank_oracle"]
    writers = list(oracle["writers"])
    readers = list(manifest["rank_stability"]["dynamic_low_readers"])
    high = list(manifest["rank_stability"]["exact_high_readers"])
    if (len(writers), len(readers), len(high), len(set(writers + readers + high))) != (32, 16, 8, 56):
        raise ValueError("invalid frozen multiwriter actor groups")
    for offset, actors in enumerate((writers, readers, high)):
        random.Random(SHUFFLE_SEED + offset).shuffle(actors)
    relation_count = 0

    def profile(actor, cohort):
        return {"key": "profile_info", "command": "我的修仙信息", "actor": actor,
                "success_markers": ["道号", "修为排行", "灵石排行"], "rank_cohort": cohort,
                "expected_relation": relation._relation_expectation(manifest, actor),
                "dynamic_rank_oracle": {"base_generation": oracle["base_generation"],
                                        "max_writes": oracle["max_writes"], "states": oracle["actors"][actor]}}

    def relation_read():
        nonlocal relation_count
        number = relation_count // 2
        partner = relation_count % 2 == 1
        relation_count += 1
        master = number % 2 == 0
        actor = (manifest["groups"]["exact_blob"][number // 2 % 32] if master
                 else manifest["groups"]["training_low"][number // 2 % 64])
        command = "我的道侣" if partner else (
            "我的徒弟" if master and number // 2 % 2 else "我的师徒" if master
            else ("我的师徒", "我的师父", "我的师傅")[number // 2 % 3])
        return {"key": "my_partner" if partner else "my_mentor", "command": command, "actor": actor,
                "success_markers": ["我的道侣", "结契信息"] if partner else
                ["我的徒弟" if command == "我的徒弟" else "师父", "今日剩余传功次数"],
                "expected_relation": relation._relation_expectation(manifest, actor)}

    groups = []
    for index, actor in enumerate(writers):
        actor_profile = profile(actor, "writer")
        secondary = (profile(readers[index], "dynamic_low") if index < 16 else
                     profile(high[index - 16], "exact_high") if index < 24 else relation_read())
        related = relation_read()
        write = {"key": "stone_training", "command": "灵石修炼", "input_text": "灵石修炼 1000",
                 "actor": actor, "requested_stone": strata.gameplay.REQUESTED_STONE,
                 "exp_gain": strata.gameplay.EXP_GAIN}
        if mode == "pure":
            groups.append([actor_profile, relation_read(), secondary, related])
        elif mode == "mixed":
            groups.append([actor_profile, related, write, secondary] if index % 2 == 0
                          else [write, actor_profile, secondary, related])
        else:
            groups.append([actor_profile, write, secondary, related])
    if mode == "contention":
        ordered = [row for index in range(0, WRITERS, 2)
                   for slot in range(4) for row in (groups[index][slot], groups[index + 1][slot])]
    else:
        ordered = [row for group in groups for row in group]
    return [{**row, "input_text": row.get("input_text", row["command"]),
             "number": 890_000_000 + phase_number * 100_000 + index,
             "event_type": "group", "phase_label": mode} for index, row in enumerate(ordered)]


def _observed_state(request, result):
    oracle = request.get("dynamic_rank_oracle")
    if not isinstance(oracle, dict):
        raise ValueError("missing_multiwriter_rank_oracle")
    observed = result.get("rank_state_observations", ())
    if not isinstance(observed, list) or len(observed) != 1:
        raise ValueError("multiwriter_rank_state_count_mismatch")
    observed = observed[0]
    if not isinstance(observed, dict) or observed.get("actor") != request["actor"]:
        raise ValueError("multiwriter_rank_state_actor_mismatch")
    offsets = []
    for column in ("exp", "stone"):
        generation = observed.get(column + "_generation")
        if not isinstance(generation, int) or isinstance(generation, bool):
            raise ValueError("multiwriter_rank_generation_invalid")
        offsets.append(generation - oracle["base_generation"][column + "_generation"])
    if offsets[0] != offsets[1]:
        raise ValueError("multiwriter_rank_generation_mismatch")
    number, maximum = offsets[0], oracle["max_writes"]
    if not 0 <= number <= maximum or len(oracle["states"]) != 3:
        raise ValueError("multiwriter_rank_generation_out_of_range")
    candidates = oracle["states"][0 if number == 0 else 2 if number == maximum else 1]
    matching = [state for state in candidates if all(
        observed.get(column + "_key") == state[column + "_key"] for column in ("exp", "stone"))]
    if len(matching) != 1:
        raise ValueError("multiwriter_rank_target_key_mismatch")
    return number, matching[0]


def classify_result(request, result):
    if request["key"] != "profile_info":
        return relation.classify_result(request, result)
    classified = strata.gameplay.classify_result(request, result)
    if classified["outcome"] != "success":
        return classified
    try:
        _, state = _observed_state(request, result)
    except ValueError as exc:
        return {"outcome": "unclassified", "reason": str(exc)}
    return relation.classify_result(
        {**request, "expected_ranks": {column: state[column] for column in ("exp", "stone")}}, result
    )


def check_invariants(context, managers, manifest, results):
    checked = relation.check_invariants(context, managers, manifest, results)
    errors, oracle = list(checked["errors"]), manifest["dynamic_rank_oracle"]
    writers = set(oracle["writers"])
    writes = [row for row in results if row.get("request", row)["key"] == "stone_training"]
    attempts = Counter(row.get("request", row)["actor"] for row in writes)
    fresh = sum(row.get("outcome") == "success" for row in writes)
    if set(attempts) - writers or any(count != 1 for count in attempts.values()):
        errors.append("multiwriter traffic must attempt at most one write per permitted writer")
    generation = checked["generation_after"]
    if fresh > oracle["max_writes"] or any(
        generation[field] - oracle["base_generation"][field] != fresh
        for field in ("generation", "exp_generation", "stone_generation")
    ):
        errors.append("multiwriter generations do not equal fresh successful training receipts")
    readonly = [actor for actor in manifest["actors"] if actor not in writers]
    actual = strata._score_evidence(strata._raw_scores(context.paths.game_db, readonly))
    if actual != {actor: manifest["score_evidence"][actor] for actor in readonly}:
        errors.append("multiwriter traffic changed a nonwriter participant's raw scores/storage/keys/power")
    offsets, target_states = Counter(), Counter()
    rank_checks, display_checks, divergences = 0, 0, 0
    for row in results:
        request = row.get("request", row)
        if request["key"] != "profile_info" or row.get("outcome") != "success":
            continue
        try:
            number, state = _observed_state(request, row)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if number > fresh:
            errors.append("multiwriter rank snapshot exceeds the final fresh write count")
        classified = classify_result(request, row)
        if classified["outcome"] != "success":
            errors.append(classified["reason"])
        rank_checks += 2
        offsets[number] += 1
        target_states["fixed" if state["write_state"] is None else "new" if state["write_state"] else "old"] += 1
        for display in row.get("profile_display_observations", ()):
            if display.get("actor") == request["actor"]:
                display_checks += 1
                divergences += any(display.get(column + "_key") != state[column + "_key"]
                                   for column in ("exp", "stone"))
    return {**checked, "ok": not errors, "errors": errors, "nonwriter_score_checks": len(readonly),
            "dynamic_rank_checks": {"exact_reply_ranks": rank_checks, "observed_write_offsets": dict(offsets),
                "observed_target_states": dict(target_states), "attempted_writers": len(attempts),
                "fresh_training": fresh, "display_key_checks": display_checks,
                "display_rank_snapshot_divergences": divergences, "atomic_profile_claim": False,
                "display_snapshot_semantics": "profile attributes may predate rank snapshot"},
            "invariants": checked["invariants"] + "; symmetric one-write-per-actor generations/receipts, observed snapshot target pairs/reply ranks, 96 unchanged nonwriter participants"}
