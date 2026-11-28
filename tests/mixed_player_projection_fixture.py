"""Distinct live writers crossing separate fixed pivots, without request oracle SQL."""
from __future__ import annotations

from bisect import bisect_right
from collections import Counter

from nonebot_plugin_xiuxian_2.core.numeric import sqlite_int_sort_key
from tests import mixed_multiwriter_fixture as multiwriter
from tests import mixed_player_projection_seed as player_seed


relation = multiwriter.relation
strata = multiwriter.strata
PARTICIPANTS = multiwriter.PARTICIPANTS
WRITERS = multiwriter.WRITERS
ORACLE_GROUPS = multiwriter.ORACLE_GROUPS
SHUFFLE_SEED = multiwriter.SHUFFLE_SEED


def phase_specs():
    return [{**row, "order": f"player-projection-seed-{SHUFFLE_SEED}"}
            for row in multiwriter.phase_specs()]


def _rank_states(histograms, scores, writers, actors, pivots):
    if (not 1 <= len(writers) <= WRITERS or len(set(writers)) != len(writers)
            or len(pivots) != len(writers) or len(set(pivots)) != len(pivots)
            or set(writers) & set(pivots) or not actors or len(set(actors)) != len(actors)
            or not set(writers) <= set(actors) or set(pivots) & set(actors)):
        raise ValueError("invalid bounded player-projection targets")
    counts = {column: Counter(histograms[column]) for column in ("exp", "stone")}
    if any(not isinstance(key, bytes) or not isinstance(count, int) or isinstance(count, bool) or count <= 0
           for histogram in counts.values() for key, count in histogram.items()):
        raise ValueError("invalid player-projection histogram")
    keys = {actor: {column: sqlite_int_sort_key(scores[actor][column]["canonical"])
                    for column in counts} for actor in set(actors) | set(pivots)}
    if any(key is None or key not in counts[column]
           for values in keys.values() for column, key in values.items()):
        raise ValueError("player-projection target is absent from histogram")
    changed = {actor: {column: sqlite_int_sort_key(scores[actor][column]["canonical"] + delta)
                       for column, delta in (("exp", strata.gameplay.EXP_GAIN),
                                             ("stone", -strata.gameplay.REQUESTED_STONE))}
               for actor in writers}
    for column, histogram in counts.items():
        intervals = sorted(tuple(sorted((keys[actor][column], changed[actor][column])))
                           for actor in writers)
        if any(first[1] >= second[0] for first, second in zip(intervals, intervals[1:])):
            raise ValueError("player-projection writer intervals overlap")
        for actor, pivot in zip(writers, pivots):
            old, new = keys[actor][column], changed[actor][column]
            if histogram[old] != 1 or histogram.get(new, 0):
                raise ValueError("player-projection writer endpoint is not exclusive")
            lower, upper = sorted((old, new))
            interior = [key for key in histogram if lower < key < upper]
            if interior != [keys[pivot][column]] or histogram[keys[pivot][column]] != 1:
                raise ValueError("player-projection writer must cross exactly its unique fixed pivot")
        if any(lower <= keys[actor][column] <= upper
               for actor in set(actors) - set(writers) for lower, upper in intervals):
            raise ValueError("player-projection readonly profile intersects a writer interval")
    ordered = {column: sorted(histogram) for column, histogram in counts.items()}
    states = {}
    for actor in actors:
        own_states = []
        for written in (0, 1) if actor in writers else (None,):
            target = changed[actor] if written else keys[actor]
            ranks = {}
            for column, key in target.items():
                current = ordered[column]
                if written:
                    current = sorted((set(current) - {keys[actor][column]}) | {key})
                ranks[column] = 1 + len(current) - bisect_right(current, key)
            own_states.append({**ranks, **{column + "_key": key.hex() for column, key in target.items()},
                               "write_state": written})
        if actor in writers and (own_states[1]["exp"] != own_states[0]["exp"] - 1
                                 or own_states[1]["stone"] != own_states[0]["stone"] + 1):
            raise ValueError("player-projection writer did not cross one dense rank per column")
        states[actor] = [[own_states[0]], own_states, [own_states[-1]]]
    return states


def _dynamic_rank_oracle(database, scores, writers, actors, pivots, *, max_groups=ORACLE_GROUPS):
    if not 1 <= max_groups <= ORACLE_GROUPS:
        raise ValueError("invalid player-projection histogram bound")
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
                        raise ValueError("player-projection oracle exceeds its distinct-group bound")
                    histogram[bytes(key)] = int(count)
            histograms[column] = histogram
            evidence[column] = {"distinct_groups": len(histogram), "rows": sum(histogram.values()),
                                "exclusive_writer_endpoints": len(writers), "unique_fixed_pivots": len(pivots)}
    states = _rank_states(histograms, scores, writers, actors, pivots)
    return {"base_generation": generation, "writers": list(writers), "max_writes": len(writers),
            "state_offsets": [0, 1, len(writers)], "actors": states}, {
                "method": "setup streaming multiplicity histogram; disjoint writer intervals each cross one fixed pivot; observed target pair selects own state independently of other writers",
                "columns": evidence, "generation": generation,
                "max_groups_per_column": max_groups, "request_time_oracle_sql": 0,
            }


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    manifest = relation.seed(context, managers, participants=participants)
    low, high = (manifest["groups"][name] for name in ("training_low", "exact_blob"))
    writers, pivots = list(low[:WRITERS]), list(low[WRITERS:])
    values = [(actor, 832_000 + 1000 * index, 500_000_000 + 10_000 * index)
              for index, actor in enumerate(writers)]
    values += [(actor, 832_050 + 1000 * index, 499_999_500 + 10_000 * index)
               for index, actor in enumerate(pivots)]
    if (max(exp for _, exp, _ in values) + strata.gameplay.EXP_GAIN >= manifest["exp_cap"]
            or min(stone for _, _, stone in values) < strata.gameplay.REQUESTED_STONE):
        raise ValueError("player-projection seed would hit real training resource limits")
    with strata.shared._uow(context.paths.game_db) as uow:
        for actor, exp, stone in values:
            power = int(exp * manifest["power_multiplier"] + .5)
            uow.execute("UPDATE user_xiuxian SET exp=?,stone=?,power=? WHERE user_id=?",
                        (exp, stone, power, actor))
    scores = strata._score_evidence(strata._raw_scores(context.paths.game_db, manifest["actors"]))
    for actor in manifest["groups"]["preserved_background"] + high:
        if scores[actor] != manifest["score_evidence"][actor]:
            raise ValueError("player-projection seed changed a preserved participant")
    for actor in manifest["actors"]:
        manifest["initial"][actor].update(
            {column: scores[actor][column]["canonical"] for column in ("exp", "stone")},
            power=scores[actor]["power"],
        )
    oracle, evidence = _dynamic_rank_oracle(context.paths.game_db, scores, writers, writers + high[:24], pivots)
    manifest.update(
        fixture_version="mixed-player-projection-v10", phase_specs=phase_specs(), shuffle_seed=SHUFFLE_SEED,
        score_evidence=scores, dynamic_rank_oracle=oracle, rank_oracle=evidence,
        setup_generation=oracle["base_generation"],
        expected_ranks={actor: {column: states[0][0][column] for column in ("exp", "stone")}
                        for actor, states in oracle["actors"].items()},
        rank_stability={"writers": writers, "fixed_pivots": pivots, "dynamic_low_readers": [],
                        "exact_high_readers": list(high[:24]), "max_writes": WRITERS,
                        "proof": "disjoint writer intervals and exclusive endpoints preserve other writers' distinct-above contribution; own full target pair selects one-rank crossing"},
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
        "384 requests; 56 profile actors per phase: 32 distinct writers and 24 exact high readers; 32 fixed pivots never dispatch profiles",
        "mixed/contention each attempt one real write per writer, crossing its own fixed pivot once; not repeated same-user write contention",
        "32 disjoint score intervals preserve distinct counts and other-writer rank contributions; not arbitrary rank crossings or sustained 50 successful requests/s",
        "bounded setup histogram <=16384 groups/column; old/new target-pair candidates; no request-time oracle SQL, audit triggers, or replay tracing",
        "32 REAL and 32 BLOB participants retain raw scores/storage/keys/power; all 96 nonwriters must remain unchanged during traffic",
        "profile attributes may predate rank snapshot; generation offsets, actual overlap and finite diagnostic sample limits must be reported",
    ]
    manifest["player_projection"] = player_seed.seed(context.paths, manifest["actors"])
    manifest["feature_table_rows"].update(manifest["player_projection"]["feature_table_rows"])
    manifest["limits"] += manifest["player_projection"]["limits"]
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
    readers = manifest["rank_stability"]["exact_high_readers"]
    if len(readers) != 24 or manifest["rank_stability"]["dynamic_low_readers"]:
        raise ValueError("invalid frozen player-projection actor groups")
    adapted = {**manifest, "rank_stability": {**manifest["rank_stability"],
               "dynamic_low_readers": readers[:16], "exact_high_readers": readers[16:]}}
    plan = multiwriter.request_plan(adapted, mode, count, phase_number)
    for row in plan:
        if row.get("rank_cohort") == "dynamic_low":
            row["rank_cohort"] = "exact_high"
        if row["key"] == "profile_info":
            projection = manifest["player_projection"]
            equipped = projection["snapshot"]["title"]["actor_rows"][row["actor"]]["equipped"]
            row["expected_title_name"] = projection["title_names"][equipped]
    return plan


def classify_result(request, result):
    classified = multiwriter.classify_result(request, result)
    if request["key"] != "profile_info" or classified["outcome"] != "success":
        return classified
    expected = request.get("expected_title_name")
    text = "\n".join(map(str, result.get("replies", ()))).replace("\r\n", "\n").replace("\r", "\n")
    if not isinstance(expected, str) or not expected or text.count(f"称号: {expected}\n") != 1:
        return {"outcome": "unclassified", "reason": "profile_equipped_title_mismatch"}
    return classified


def check_invariants(context, managers, manifest, results):
    checked = multiwriter.check_invariants(context, managers, manifest, results)
    errors = list(checked["errors"])
    if player_seed.snapshot(context.paths, manifest["actors"]) != manifest["player_projection"]["snapshot"]:
        errors.append("player-projection traffic changed avatar/title rows")
    title_checks = 0
    for row in results:
        request = row.get("request", row)
        if request["key"] == "profile_info" and row.get("outcome") == "success":
            classified = classify_result(request, row)
            if classified["outcome"] != "success":
                errors.append(classified["reason"])
            title_checks += 1
    return {**checked, "ok": not errors, "errors": errors, "equipped_title_reply_checks": title_checks,
            "player_projection_row_checks": manifest["player_projection"]["feature_table_rows"],
            "invariants": checked["invariants"].replace(
                "symmetric one-write-per-actor", "disjoint-pivot distinct-score one-write-per-actor")
                + "; full bounded avatar/title table hashes unchanged and equipped-title reply checked"}
