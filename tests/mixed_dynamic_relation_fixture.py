"""Single-writer changing exact ranks alongside the populated relation fixture."""
from __future__ import annotations

from bisect import bisect_right
from collections import Counter
import random
import re

from nonebot_plugin_xiuxian_2.core.numeric import sqlite_int_sort_key
from tests import mixed_relation_fixture as relation


strata = relation.strata
PARTICIPANTS = relation.PARTICIPANTS
MAX_WRITES = 96
ORACLE_GROUPS = 16_384
SHUFFLE_SEED = 20261201


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": count,
             "concurrency": cap, "arrival_rate": rate, "order": "dynamic-relation-seed-20261201"}
            for mode, count, cap, rate in
            (("pure", 128, 10, 20), ("mixed", 192, 20, 35), ("contention", 192, 20, 50))]


def _rank_states(histograms, scores, writer, actors, *, max_writes=MAX_WRITES):
    if (not 0 <= max_writes <= MAX_WRITES or writer not in actors
            or not actors or len(set(actors)) != len(actors)):
        raise ValueError("invalid bounded dynamic rank targets")
    counts = {column: Counter(histograms[column]) for column in ("exp", "stone")}
    if any(not isinstance(key, bytes) or count <= 0
           for histogram in counts.values() for key, count in histogram.items()):
        raise ValueError("invalid dynamic rank histogram")
    keys = {actor: {column: sqlite_int_sort_key(scores[actor][column]["canonical"])
                    for column in counts} for actor in actors}
    if any(key is None or key not in counts[column]
           for values in keys.values() for column, key in values.items()):
        raise ValueError("dynamic rank target is absent from histogram")
    states = {actor: [] for actor in actors}
    for number in range(max_writes + 1):
        ordered = {column: sorted(histogram) for column, histogram in counts.items()}
        for actor in actors:
            states[actor].append({
                **{column: 1 + len(ordered[column]) - bisect_right(ordered[column], key)
                   for column, key in keys[actor].items()},
                **{column + "_key": key.hex() for column, key in keys[actor].items()},
            })
        if number == max_writes:
            break
        for column, delta in (("exp", strata.gameplay.EXP_GAIN),
                              ("stone", -strata.gameplay.REQUESTED_STONE)):
            previous = keys[writer][column]
            counts[column][previous] -= 1
            if not counts[column][previous]:
                del counts[column][previous]
            updated = sqlite_int_sort_key(scores[writer][column]["canonical"] + delta * (number + 1))
            counts[column][updated] += 1
            keys[writer][column] = updated
    return states


def _dynamic_rank_oracle(database, scores, writer, actors, *, max_writes=MAX_WRITES,
                         max_groups=ORACLE_GROUPS):
    if not 1 <= max_groups <= ORACLE_GROUPS:
        raise ValueError("invalid dynamic rank histogram bound")
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
                        raise ValueError("dynamic rank oracle exceeds its distinct-group bound")
                    histogram[bytes(key)] = int(count)
            histograms[column] = histogram
            evidence[column] = {"distinct_groups": len(histogram), "rows": sum(histogram.values())}
    states = _rank_states(histograms, scores, writer, actors, max_writes=max_writes)
    return {"base_generation": generation, "writer": writer, "max_writes": max_writes,
            "actors": states}, {
                "method": "setup streaming GROUP BY key/COUNT(*); Python multiplicity histogram and dense ordinals",
                "columns": evidence, "generation": generation,
                "max_groups_per_column": max_groups, "request_time_oracle_sql": 0,
            }


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    manifest = relation.seed(context, managers, participants=participants)
    low, high = (manifest["groups"][name] for name in ("training_low", "exact_blob"))
    writer = low[0]
    initial = manifest["initial"][writer]
    if (initial["exp"] + MAX_WRITES * strata.gameplay.EXP_GAIN >= manifest["exp_cap"]
            or initial["stone"] < MAX_WRITES * strata.gameplay.REQUESTED_STONE):
        raise ValueError("dynamic writer would hit the real training resource limits")
    with strata.shared._uow(context.paths.game_db) as uow:
        for index, actor in enumerate(low[1:17], 1):
            exp, stone = initial["exp"] + 400 * index, initial["stone"] - 4000 * index
            power = int(exp * manifest["power_multiplier"] + .5)
            uow.execute("UPDATE user_xiuxian SET exp=?,stone=?,power=? WHERE user_id=?",
                        (exp, stone, power, actor))
    scores = strata._score_evidence(strata._raw_scores(context.paths.game_db, manifest["actors"]))
    for actor in manifest["groups"]["preserved_background"] + high:
        if scores[actor] != manifest["score_evidence"][actor]:
            raise ValueError("dynamic seed changed a preserved REAL or exact BLOB actor")
    for actor in manifest["actors"]:
        manifest["initial"][actor].update(
            {column: scores[actor][column]["canonical"] for column in ("exp", "stone")},
            power=scores[actor]["power"],
        )
    readers = [writer, *low[1:17], *high[:8]]
    oracle, evidence = _dynamic_rank_oracle(context.paths.game_db, scores, writer, readers)
    manifest.update(
        fixture_version="mixed-dynamic-relation-v7", phase_specs=phase_specs(), shuffle_seed=SHUFFLE_SEED,
        score_evidence=scores, dynamic_rank_oracle=oracle, rank_oracle=evidence,
        setup_generation=oracle["base_generation"],
        expected_ranks={actor: {column: states[0][column] for column in ("exp", "stone")}
                        for actor, states in oracle["actors"].items()},
        rank_stability={"writer": writer, "dynamic_low_readers": list(low[1:17]),
                        "exact_high_readers": list(high[:8]), "max_writes": MAX_WRITES,
                        "proof": "ranks and target keys depend on observed snapshot dual-generation offsets"},
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
        "512 requests; one real writer crossing 16 low readers and itself; not multiwriter dynamic correctness or sustained 50/s capacity",
        "25 profile actors, n=0..96 exact rank/key states; setup histogram <=16384 groups/column; no request-time oracle SQL",
        "127 nonwriter participants retain raw scores/storage/keys/power; preserved REAL/BLOB strata remain unchanged",
        "profile display attributes can predate the rank snapshot; no whole-reply atomicity claim",
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
        raise ValueError("invalid frozen dynamic relation plan")
    low, high = (list(manifest["groups"][name]) for name in ("training_low", "exact_blob"))
    writer, readers = low[0], [*low[1:17], *high[:8]]
    random.Random(SHUFFLE_SEED).shuffle(high)
    random.Random(SHUFFLE_SEED + 1).shuffle(low)
    sequence = {"pure": ("profile_info", "my_mentor", "my_partner", "my_mentor",
                         "profile_info", "my_partner", "my_mentor", "my_partner"),
                "mixed": ("profile_info", "my_mentor", "profile_info", "my_partner"),
                "contention": ("profile_info", "my_mentor", "my_partner")}[mode]
    result, reads, counts = [], 0, Counter()
    for index in range(count):
        write = mode == "mixed" and index % 3 == 2 or mode == "contention" and index % 2 == 1
        key = "stone_training" if write else sequence[reads % len(sequence)]
        number = counts[key]
        if write:
            actor = writer
            row = {"key": key, "command": "灵石修炼", "input_text": "灵石修炼 1000",
                   "requested_stone": strata.gameplay.REQUESTED_STONE, "exp_gain": strata.gameplay.EXP_GAIN}
        else:
            if key == "profile_info":
                actor = writer if number % 4 == 0 else readers[(number - number // 4 - 1) % len(readers)]
                oracle = manifest["dynamic_rank_oracle"]
                row = {"key": key, "command": "我的修仙信息", "success_markers": ["道号", "修为排行", "灵石排行"],
                       "rank_cohort": "writer" if actor == writer else "dynamic_low" if actor in readers[:16] else "exact_high",
                       "dynamic_rank_oracle": {field: oracle[field] for field in ("base_generation", "writer", "max_writes")}}
                row["dynamic_rank_oracle"]["states"] = oracle["actors"][actor]
            else:
                actor = high[number // 2 % len(high)] if number % 2 == 0 else low[number // 2 % len(low)]
                command = "我的道侣" if key == "my_partner" else "我的师徒"
                if key == "my_mentor":
                    if number % 2 == 0 and number // 2 % 2:
                        command = "我的徒弟"
                    elif number % 2:
                        command = ("我的师徒", "我的师父", "我的师傅")[number // 2 % 3]
                row = {"key": key, "command": command,
                       "success_markers": ["我的道侣", "结契信息"] if key == "my_partner" else
                       ["我的徒弟" if command == "我的徒弟" else "师父", "今日剩余传功次数"]}
            row.update(input_text=row["command"], expected_relation=relation._relation_expectation(manifest, actor))
            reads += 1
        counts[key] += 1
        result.append({**row, "actor": actor, "number": 890_000_000 + phase_number * 100_000 + index,
                       "event_type": "group", "phase_label": mode})
    return result


def _observed_state(request, result):
    oracle = request.get("dynamic_rank_oracle")
    if not isinstance(oracle, dict):
        raise ValueError("missing_dynamic_rank_oracle")
    observations = result.get("rank_state_observations", ())
    if not isinstance(observations, list) or len(observations) != 1:
        raise ValueError("dynamic_rank_state_count_mismatch")
    observed = observations[0]
    if not isinstance(observed, dict) or observed.get("actor") != request["actor"]:
        raise ValueError("dynamic_rank_state_actor_mismatch")
    offsets = []
    for column in ("exp", "stone"):
        generation = observed.get(column + "_generation")
        if not isinstance(generation, int) or isinstance(generation, bool):
            raise ValueError("dynamic_rank_generation_invalid")
        offsets.append(generation - oracle["base_generation"][column + "_generation"])
    if offsets[0] != offsets[1]:
        raise ValueError("dynamic_rank_generation_mismatch")
    number = offsets[0]
    if not 0 <= number <= oracle["max_writes"] or len(oracle["states"]) != oracle["max_writes"] + 1:
        raise ValueError("dynamic_rank_generation_out_of_range")
    state = oracle["states"][number]
    if any(observed.get(column + "_key") != state[column + "_key"] for column in ("exp", "stone")):
        raise ValueError("dynamic_rank_target_key_mismatch")
    return number, state


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
    writes = [row for row in results if row.get("request", row)["key"] == "stone_training"]
    fresh = sum(row.get("outcome") == "success" for row in writes)
    if any(row.get("request", row)["actor"] != oracle["writer"] for row in writes):
        errors.append("dynamic traffic wrote a user other than the sole writer")
    generation = checked["generation_after"]
    if fresh > oracle["max_writes"] or any(
        generation[field] - oracle["base_generation"][field] != fresh
        for field in ("generation", "exp_generation", "stone_generation")
    ):
        errors.append("dynamic generations do not equal fresh successful training receipts")
    readonly = [actor for actor in manifest["actors"] if actor != oracle["writer"]]
    actual = strata._score_evidence(strata._raw_scores(context.paths.game_db, readonly))
    if actual != {actor: manifest["score_evidence"][actor] for actor in readonly}:
        errors.append("dynamic traffic changed a nonwriter participant's raw scores/storage/keys/power")
    offsets, displays, divergences, display_checks, rank_checks = Counter(), [], 0, 0, 0
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
            errors.append("profile rank snapshot exceeds the final fresh write count")
        classification = classify_result(request, row)
        if classification["outcome"] != "success":
            errors.append(classification["reason"])
        rank_checks += 2
        offsets[number] += 1
        text = "\n".join(map(str, row.get("replies", ())))
        if len(displays) < 8:
            displays.append({"actor": request["actor"], "rank_write_offset": number,
                             **{column: re.findall(title + r": ([^\r\n]+)", text)
                                for column, title in (("exp_text", "修为"), ("stone_text", "灵石"))}})
        for display in row.get("profile_display_observations", ()):
            if display.get("actor") == request["actor"]:
                display_checks += 1
                divergences += any(display.get(column + "_key") != state[column + "_key"]
                                   for column in ("exp", "stone"))
    return {**checked, "ok": not errors, "errors": errors, "nonwriter_score_checks": len(readonly),
            "dynamic_rank_checks": {"exact_reply_ranks": rank_checks, "observed_write_offsets": dict(offsets),
                                    "fresh_training": fresh, "display_key_checks": display_checks,
                                    "display_rank_snapshot_divergences": divergences, "display_text_samples": displays,
                                    "atomic_profile_claim": False,
                                    "display_snapshot_semantics": "profile attributes may predate rank snapshot"},
            "invariants": checked["invariants"] + "; sole-writer generations/fresh receipts, observed snapshot rank keys/replies, 127 unchanged nonwriter participants"}
