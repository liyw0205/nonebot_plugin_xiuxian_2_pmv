"""Frozen near/mid/deep exact ranks with a populated distinct tail and writes."""
from __future__ import annotations

import random

from tests import mixed_read_strata_fixture as strata

PARTICIPANTS = 128
BAND_USERS = 8192
SHUFFLE_SEED = 20261130
BASE = {"exp": 10**10, "stone": 10**12}
classify_result = strata.classify_result


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": count,
             "concurrency": cap, "arrival_rate": rate, "order": "seed-20261130"}
            for mode, count, cap, rate in
            (("pure", 96, 10, 20), ("mixed", 192, 20, 35), ("contention", 128, 20, 50))]


def _grouped_oracle(database, actors):
    """Stream grouped counts in setup; no product COUNT/rank-seek implementation."""
    ranks, evidence = {actor: {} for actor in actors}, {}
    with strata.shared._uow(database, read_only=True) as uow:
        generation = strata._generation(uow)
        for column in ("exp", "stone"):
            expression = f'xiuxian_int_sort_key_compact("{column}")'
            targets = uow.query_all(f"SELECT user_id,{expression} AS rank_key FROM user_xiuxian "
                                    f"WHERE user_id IN ({strata.shared._marks(actors)})", actors)
            if len(targets) != len(actors) or any(row["rank_key"] is None for row in targets):
                raise ValueError("rank-seek oracle requires existing non-NULL targets")
            by_key = {}
            for row in targets:
                by_key.setdefault(bytes(row["rank_key"]), []).append(str(row["user_id"]))
            cursor = uow.connection.execute(f"SELECT {expression},COUNT(*) FROM user_xiuxian "
                                            f"WHERE {expression} IS NOT NULL GROUP BY {expression} "
                                            f"ORDER BY {expression} DESC")
            groups, higher_rows, target_rows = 0, 0, {}
            while rows := cursor.fetchmany(128):
                for key, count in rows:
                    groups += 1
                    if groups > 16384:
                        raise ValueError("rank-seek oracle exceeds its distinct-group bound")
                    for actor in by_key.pop(bytes(key), ()):
                        ranks[actor][column] = groups
                        target_rows[actor] = higher_rows
                    higher_rows += int(count)
            if by_key:
                raise ValueError("rank-seek oracle missed targets")
            evidence[column] = {"distinct_groups": groups, "rows": higher_rows,
                                "target_higher_rows": target_rows}
    return ranks, {"method": "setup streaming GROUP BY key, COUNT(*) per group; Python dense ordinal",
                   "columns": evidence, "generation": generation, "request_time_oracle_sql": 0}


def seed(context, managers, *, participants=PARTICIPANTS):
    manifest = strata.seed(context, managers, participants=participants)
    paths, actors = context.paths, manifest["actors"]
    readers = manifest["groups"]["exact_blob"]
    selected = strata.shared._rows(paths.game_db, "SELECT user_id FROM user_xiuxian "
        f"WHERE user_id NOT IN ({strata.shared._marks(actors)}) ORDER BY id LIMIT ?", (*actors, BAND_USERS))
    band = [str(row["user_id"]) for row in selected]
    if len(band) != BAND_USERS:
        raise ValueError("rank-seek needs 8192 existing distinct nonactors")
    cohorts = {"near": readers[:8], "mid": readers[8:16], "deep": readers[16:]}
    with strata.shared._uow(paths.game_db) as uow:
        uow.executemany("UPDATE user_xiuxian SET exp=?,stone=? WHERE user_id=?",
            ((BASE["exp"] + index * 1000, BASE["stone"] + index * 1000, actor)
             for index, actor in enumerate(band)))
        for cohort, members in (("mid", cohorts["mid"]), ("deep", cohorts["deep"])):
            offset = BAND_USERS // 2 if cohort == "mid" else 0
            for index, actor in enumerate(members):
                values = [str(BASE[column] + (offset + index) * 1000 + 500).encode("ascii")
                          for column in ("exp", "stone")]
                uow.execute("UPDATE user_xiuxian SET exp=?,stone=? WHERE user_id=?", (*values, actor))
    scores = strata._score_evidence(strata._raw_scores(paths.game_db, actors))
    for actor in actors:
        for column in ("exp", "stone"):
            manifest["initial"][actor][column] = scores[actor][column]["canonical"]
    readonly = manifest["groups"]["preserved_background"] + readers
    minimum = {column: min(scores[actor][column]["canonical"] for actor in readonly)
               for column in ("exp", "stone")}
    maximum = {"exp": max(manifest["initial"][actor]["exp"] for actor in manifest["groups"]["training_low"])
                      + 64 * strata.gameplay.EXP_GAIN,
               "stone": max(manifest["initial"][actor]["stone"] for actor in manifest["groups"]["training_low"])}
    if any(maximum[column] >= minimum[column] for column in minimum):
        raise ValueError("training can cross a rank reader")
    ranks, oracle = _grouped_oracle(paths.game_db, readers)
    manifest.update(fixture_version="mixed-rank-seek-v6", cohorts=cohorts, score_evidence=scores,
        expected_ranks=ranks, rank_oracle=oracle, setup_generation=oracle["generation"],
        band_actors=band, band_scores_sha256=strata.gameplay._digest(
            strata._score_evidence(strata._raw_scores(paths.game_db, band))),
        phase_specs=phase_specs(), shuffle_seed=SHUFFLE_SEED,
        rank_stability={"training_max": maximum, "readonly_min": minimum,
                        "proof": "all reachable training scores remain below every readonly target"})
    manifest["limits"] += [
        "8192 existing nonactors receive temporary distinct scores; no extra users or retained million DB",
        "32 exact readers: 8 near original REAL groups, 8 mid-band, 16 deep-band; no mutable-crossing rank claim",
        "416 requests, short offered bursts <=50/s; not sustained 50 successful/s or 20 simultaneous CPU work",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = strata.gameplay._digest(manifest)
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs) or
            (mode, count) != (specs[phase_number]["mode"], specs[phase_number]["requests"])):
        raise ValueError("invalid frozen rank-seek plan")
    readers = list(manifest["groups"]["exact_blob"])
    low = list(manifest["groups"]["training_low"])
    random.Random(SHUFFLE_SEED).shuffle(readers)
    random.Random(SHUFFLE_SEED + 1).shuffle(low)
    cohorts = {actor: name for name, members in manifest["cohorts"].items() for actor in members}
    reads = {key: (command, markers) for key, command, markers in strata.shared.READ_SPECS}
    sequences = {"pure": ("profile_info", "asset_overview", "profile_info", "activity_rank"),
                 "mixed": ("profile_info", "backpack", "profile_info", "activity_rank"),
                 "contention": ("profile_info", "mind_state")}
    result, read_count, profiles, writes = [], 0, 0, 0
    for index in range(count):
        write = mode == "mixed" and index % 3 == 2 or mode == "contention" and index % 2 == 1
        if write:
            actor = low[0] if mode == "contention" else low[writes % len(low)]
            row = {"key": "stone_training", "command": "灵石修炼", "input_text": "灵石修炼 1000",
                   "requested_stone": 1000, "exp_gain": 100}
            writes += 1
        else:
            keys = sequences[mode]
            key = keys[read_count % len(keys)]
            actor = readers[profiles % len(readers)] if key == "profile_info" else low[read_count % len(low)]
            command, markers = reads[key]
            row = {"key": key, "command": command, "input_text": command, "success_markers": list(markers)}
            if key == "profile_info":
                row.update(expected_ranks=dict(manifest["expected_ranks"][actor]), rank_cohort=cohorts[actor])
                profiles += 1
            read_count += 1
        result.append({**row, "actor": actor, "number": 890_000_000 + phase_number * 100_000 + index,
                       "event_type": "group", "phase_label": mode})
    return result


def check_invariants(context, managers, manifest, results):
    checked = strata.check_invariants(context, managers, manifest, results)
    errors = list(checked["errors"])
    actual = strata._score_evidence(strata._raw_scores(context.paths.game_db, manifest["band_actors"]))
    if strata.gameplay._digest(actual) != manifest["band_scores_sha256"]:
        errors.append("distinct score band changed during requests")
    return {**checked, "ok": not errors, "errors": errors,
            "band_score_checks": len(actual), "exact_reply_rank_checks":
            2 * sum(row.get("key") == "profile_info" and row.get("outcome") == "success" for row in results)}
