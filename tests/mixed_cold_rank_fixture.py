"""Cold diverse-score ranks and populated natal display on the retained fixture."""
from __future__ import annotations

from tests import mixed_activity_rank_fixture as activity


player = activity.player
shared = activity.shared
PARTICIPANTS = activity.PARTICIPANTS
SCORE_ROWS = 4096
NATAL_ROWS = player.player_seed.POPULATION
phase_specs = activity.phase_specs


def _background_snapshot(database, actors):
    rows = []
    with shared._uow(database, read_only=True) as uow:
        for offset in range(0, len(actors), 512):
            batch = actors[offset:offset + 512]
            rows.extend(uow.query_all(
                "SELECT user_id,exp,stone,power,typeof(exp) AS exp_type,"
                "typeof(stone) AS stone_type FROM user_xiuxian "
                f"WHERE user_id IN ({shared._marks(batch)})", batch))
    if len(rows) != len(actors):
        raise ValueError("cold-rank background users are missing")
    rows.sort(key=lambda row: str(row["user_id"]))
    return {"rows": len(rows), "sha256": activity._digest(rows)}


def _seed_background(database, actors, multiplier):
    excluded = set(actors)
    with shared._uow(database) as uow:
        rows = uow.query_all("SELECT user_id FROM user_xiuxian ORDER BY id LIMIT ?",
                             (SCORE_ROWS + PARTICIPANTS,))
        background = [str(row["user_id"]) for row in rows
                      if str(row["user_id"]) not in excluded][:SCORE_ROWS]
        if len(background) != SCORE_ROWS or len(set(background)) != SCORE_ROWS:
            raise ValueError("cold-rank seed needs 4096 distinct existing nonparticipants")
        for index, actor in enumerate(background):
            exp, stone = 1_000_000 + index * 1000, 1_000_000_000 + index * 10_000
            changed = uow.execute(
                "UPDATE user_xiuxian SET exp=?,stone=?,power=? WHERE user_id=?",
                (exp, stone, int(exp * multiplier + .5), actor))
            if changed.rowcount != 1:
                raise ValueError("cold-rank seed did not update exactly one existing user")
    return {"actors": background, "snapshot": _background_snapshot(database, background),
            "exp_start": 1_000_000, "exp_step": 1000,
            "stone_start": 1_000_000_000, "stone_step": 10_000}


def _natal_snapshot(paths, actors):
    with shared._uow(paths.player_db, read_only=True) as uow:
        return player.player_seed._table_snapshot(uow, "natal_treasure", set(actors), NATAL_ROWS)


def _seed_natal(paths, actors):
    with shared._uow(paths.player_db) as uow:
        owners = [str(row["user_id"]) for row in uow.query_all(
            "SELECT user_id FROM avatar ORDER BY user_id LIMIT ?", (NATAL_ROWS + 1,))]
        if len(owners) != NATAL_ROWS or not set(actors) <= set(owners):
            raise ValueError("natal display seed needs exactly 8192 existing avatar owners")
        uow.execute("CREATE TABLE IF NOT EXISTS natal_treasure("
                    "user_id TEXT PRIMARY KEY,form INTEGER,name TEXT,level INTEGER)")
        uow.executemany(
            "INSERT INTO natal_treasure(user_id,form,name,level) VALUES(?,1,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET form=excluded.form,name=excluded.name,level=excluded.level",
            ((actor, f"SyntheticNatal{actor}", 1 + int(actor) % 7) for actor in owners))
    return _natal_snapshot(paths, actors)


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    manifest = activity.seed(context, managers, participants=participants)
    background = _seed_background(context.paths.game_db, manifest["actors"], manifest["power_multiplier"])
    stability = manifest["rank_stability"]
    writers = manifest["dynamic_rank_oracle"]["writers"]
    oracle, evidence = player._dynamic_rank_oracle(
        context.paths.game_db, manifest["score_evidence"], writers,
        writers + stability["exact_high_readers"], stability["fixed_pivots"])
    if any(row["distinct_groups"] < SCORE_ROWS for row in evidence["columns"].values()):
        raise ValueError("cold-rank seed did not produce the declared score diversity")
    natal = _seed_natal(context.paths, manifest["actors"])
    manifest.update(
        fixture_version="mixed-cold-rank-v14", cold_rank_background=background,
        natal_snapshot=natal, dynamic_rank_oracle=oracle, rank_oracle=evidence,
        setup_generation=oracle["base_generation"],
        expected_ranks={actor: {column: states[0][0][column] for column in ("exp", "stone")}
                        for actor, states in oracle["actors"].items()},
    )
    manifest["feature_table_rows"]["natal_treasure"] = natal["rows"]
    manifest["limits"] += [
        "4096 existing nonparticipants have distinct synthetic exp/stone above writer intervals; not 1M distinct scores",
        "same 32 real cultivation writers/24 huge readers/32 fixed pivots and all REAL/BLOB participant values preserved",
        "8192 natal display rows hashed; all profile natal replies checked; no natal writes during traffic",
        "cold-rank remainder scans and global invalidation measured; activity sign/damage mutations and index write cost unmeasured",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = player.strata.gameplay._digest(manifest)
    process_read_cache.clear()
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    plan = activity.request_plan(manifest, mode, count, phase_number)
    for request in plan:
        if request["key"] == "profile_info":
            row = manifest["natal_snapshot"]["actor_rows"][request["actor"]]
            request["expected_natal"] = f"{row['name']} (Lv.{row['level']})"
    return plan


def classify_result(request, result):
    classified = activity.classify_result(request, result)
    if request["key"] != "profile_info" or classified["outcome"] != "success":
        return classified
    expected = request.get("expected_natal")
    text = "\n".join(map(str, result.get("replies", ()))).replace("\r\n", "\n").replace("\r", "\n")
    if not expected or text.count(f"本命法宝: {expected}\n") != 1:
        return {"outcome": "unclassified", "reason": "profile_natal_projection_mismatch"}
    return classified


def check_invariants(context, managers, manifest, results):
    checked = activity.check_invariants(context, managers, manifest, results)
    errors = list(checked["errors"])
    background = manifest["cold_rank_background"]
    if _background_snapshot(context.paths.game_db, background["actors"]) != background["snapshot"]:
        errors.append("cold-rank traffic changed nonparticipant scores/types/power")
    if _natal_snapshot(context.paths, manifest["actors"]) != manifest["natal_snapshot"]:
        errors.append("cold-rank traffic changed natal display rows")
    checked_profiles = 0
    for row in results:
        request = row.get("request", row)
        if request["key"] == "profile_info" and row.get("outcome") == "success":
            classified = classify_result(request, row)
            if classified["outcome"] != "success":
                errors.append(classified["reason"])
            checked_profiles += 1
    return {**checked, "ok": not errors, "errors": errors,
            "cold_rank_background_checks": background["snapshot"]["rows"],
            "natal_reply_checks": checked_profiles, "natal_row_checks": manifest["natal_snapshot"]["rows"],
            "invariants": checked["invariants"] + "; diverse background scores and natal rows unchanged; exact natal replies"}
