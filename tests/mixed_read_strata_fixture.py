"""Bounded read strata and real training writes on the retained synthetic DB.

Setup/oracles run before traffic. All requests in a phase share the same DB;
only the outer harness restores after draining and checking the phase.
"""
from __future__ import annotations

import random
import re

from tests import mixed_command_fixture as shared
from tests import mixed_rank_gameplay_fixture as gameplay


PARTICIPANTS = 128
ORACLE_STEPS = 512
SHUFFLE_SEED = 20261128
SECT_IDS = tuple(8_900_001 + index for index in range(4))
READ_KEYS = (
    "asset_overview", "mind_state", "asset_overview", "mind_state",
    "profile_info", "cultivation_value", "backpack", "activity_rank",
    "asset_overview", "mind_state", "asset_overview", "mind_state",
    "profile_info", "equipment", "realm_rank", "stone_rank",
)
READS = {row[0]: row for row in shared.READ_SPECS}


def phase_specs():
    return [
        {"label": label, "mode": mode, "requests": requests,
         "concurrency": concurrency, "arrival_rate": rate,
         "order": "shuffle-seed-20261128"}
        for label, mode, requests, concurrency, rate in (
            ("cold", "pure", 128, 10, 20),
            ("pure", "pure", 224, 20, 35),
            ("readmostly", "readmostly", 224, 10, 20),
            ("mixed", "mixed", 224, 20, 35),
            ("contention", "contention", 224, 20, 50),
        )
    ]


def _raw_scores(database, actors):
    fields = ",".join(
        f'{column},typeof({column}) AS {column}_storage,'
        f'xiuxian_int_text({column}) AS {column}_canonical,'
        f'hex(xiuxian_int_sort_key_compact({column})) AS {column}_key,'
        f'quote({column}) AS {column}_sql_literal'
        for column in ("exp", "stone")
    )
    return shared._rows(database, f"SELECT id,user_id,power,{fields} FROM user_xiuxian "
                        f"WHERE user_id IN ({shared._marks(actors)}) ORDER BY id", actors)


def _score_evidence(rows):
    return {
        str(row["user_id"]): {
            "source_pk": int(row["id"]), "power": int(row["power"]),
            **{column: {
                "canonical": int(row[column + "_canonical"]),
                "storage": row[column + "_storage"],
                "key_hex": row[column + "_key"],
                "sql_literal": row[column + "_sql_literal"],
                "raw_repr": repr(row[column]),
            } for column in ("exp", "stone")},
        } for row in rows
    }


def _generation(uow):
    row = uow.query_one("SELECT generation,exp_generation,stone_generation "
                        "FROM xiuxian_numeric_rank_generation WHERE singleton=1")
    if row is None:
        raise ValueError("numeric rank generation state is missing")
    return {key: int(value) for key, value in row.items()}


def _rank_oracle(database, actors, *, max_steps=ORACLE_STEPS):
    """Find dense ranks by bounded next-distinct index seeks, never COUNT."""
    if not 1 <= max_steps <= ORACLE_STEPS or not actors:
        raise ValueError("invalid bounded rank oracle")
    ranks = {actor: {} for actor in actors}
    evidence = {}
    with shared._uow(database, read_only=True) as uow:
        generation = _generation(uow)
        for column in ("exp", "stone"):
            expression = f'xiuxian_int_sort_key_compact("{column}")'
            targets = uow.query_all(
                f"SELECT user_id,{expression} AS rank_key FROM user_xiuxian "
                f"WHERE user_id IN ({shared._marks(actors)})", actors)
            if len(targets) != len(actors) or any(row["rank_key"] is None for row in targets):
                raise ValueError("rank oracle targets must exist and have non-NULL scores")
            pending = {bytes(row["rank_key"]) for row in targets}
            first = (f"SELECT {expression} AS rank_key FROM user_xiuxian "
                     f"WHERE {expression} IS NOT NULL ORDER BY {expression} DESC LIMIT 1")
            next_sql = (f"SELECT {expression} AS rank_key FROM user_xiuxian "
                        f"WHERE {expression} < ? AND {expression} IS NOT NULL "
                        f"ORDER BY {expression} DESC LIMIT 1")
            plans = [uow.query_all("EXPLAIN QUERY PLAN " + first),
                     uow.query_all("EXPLAIN QUERY PLAN " + next_sql, (max(pending),))]
            details = [str(row["detail"]) for plan in plans for row in plan]
            if (any("TEMP B-TREE" in detail.upper() for detail in details)
                    or any(not any("INDEX" in str(row["detail"]).upper() for row in plan)
                           for plan in plans)):
                raise ValueError("rank oracle requires existing expression indexes")
            found, previous = {}, None
            for step in range(1, max_steps + 1):
                row = uow.query_one(first if previous is None else next_sql,
                                    () if previous is None else (previous,))
                if row is None:
                    break
                key = bytes(row["rank_key"])
                if previous is not None and key >= previous:
                    raise ValueError("rank oracle did not advance to a distinct score")
                if key in pending:
                    found[key] = step
                    pending.remove(key)
                previous = key
                if not pending:
                    break
            if pending:
                raise ValueError(f"{column} oracle exceeded {max_steps} distinct seeks")
            for row in targets:
                ranks[str(row["user_id"])][column] = found[bytes(row["rank_key"])]
            evidence[column] = {"seeks": step, "query_plans": details,
                                "max_steps": max_steps, "target_keys": len(found)}
    return ranks, {"method": "bounded indexed next-distinct seek", "columns": evidence,
                   "generation": generation, "max_steps_per_column": max_steps,
                   "request_time_oracle_sql": 0}


def _exact_scores(background):
    """Mix exact int64 boundaries and neighbors of the actual stored REALs."""
    result = []
    for index in range(32):
        row = background[index]
        pair = {}
        for column in ("exp", "stone"):
            base = int(row[column + "_canonical"])
            if index < 8:
                value = 2**63 + (index % 4)
            elif index < 16:
                value = base + (index % 5 - 2)
            elif index < 24:
                value = (10**20 if column == "exp" else 10**21) + (index % 4 - 1)
            else:
                value = base + (index % 3 - 1) * 65_537
            if value <= 2**63 - 1:
                raise ValueError("exact read stratum must use overflow-safe BLOB scores")
            pair[column] = value
        result.append(pair)
    return result


def _feature_snapshot(paths, actors):
    marks = shared._marks(actors)
    return {
        "bank": shared._rows(paths.game_db, f"SELECT user_id,saved_stone,bank_level "
                             f"FROM bank_accounts WHERE user_id IN ({marks}) ORDER BY user_id", actors),
        "arena": shared._rows(paths.player_db, f"SELECT user_id,score,rank,honor_points "
                              f"FROM arena WHERE user_id IN ({marks}) ORDER BY user_id", actors),
        "status": shared._rows(paths.player_db, f"SELECT user_id,two_exp_protect "
                               f"FROM status WHERE user_id IN ({marks}) ORDER BY user_id", actors),
        "impart": shared._rows(paths.impart_db, "SELECT user_id,impart_hp_per,impart_atk_per,"
                               f"impart_mp_per,impart_lv,wish FROM xiuxian_impart WHERE user_id IN ({marks}) "
                               "ORDER BY user_id,id", actors),
        "sects": shared._rows(paths.game_db, "SELECT sect_id,sect_name,sect_owner,sect_scale,sect_used_stone "
                              f"FROM sects WHERE sect_id IN ({shared._marks(SECT_IDS)}) ORDER BY sect_id", SECT_IDS),
        "attributes": shared._rows(paths.game_db, "SELECT user_id,level,root_type,hp,mp,atk,"
                                   f"user_stamina,sect_id,sect_contribution FROM user_xiuxian "
                                   f"WHERE user_id IN ({marks}) ORDER BY user_id", actors),
    }


def _seed_features(paths, actors):
    from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_state
    from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
    from nonebot_plugin_xiuxian_2.features.impart.migrations import apply_impart_base_schema
    from nonebot_plugin_xiuxian_2.features.impart.repository import ImpartRepository
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import PartnerProtectionService

    with shared._uow(paths.game_db) as uow:
        apply_bank_accounts(uow)
        if uow.query_one(f"SELECT sect_id FROM sects WHERE sect_id IN ({shared._marks(SECT_IDS)}) LIMIT 1", SECT_IDS):
            raise ValueError("reserved synthetic sect namespace is not empty")
        for index, sect in enumerate(SECT_IDS):
            uow.execute("INSERT INTO sects(sect_id,sect_name,sect_owner,sect_scale,sect_used_stone) "
                        "VALUES(?,?,?,?,?)", (sect, f"SyntheticReadStratum{index}", actors[index],
                                              100_000 + index * 1000, 20_000 + index))
        for index, actor in enumerate(actors):
            uow.execute("UPDATE user_xiuxian SET sect_id=?,sect_contribution=?,hp=?,mp=?,atk=? WHERE user_id=?",
                        (SECT_IDS[index % 4], 10_000 + index, 1000 + index, 900 + index,
                         100 + index % 17, actor))
            uow.execute("INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) VALUES(?,?,?,?) "
                        "ON CONFLICT(user_id) DO UPDATE SET saved_stone=excluded.saved_stone,"
                        "bank_level=excluded.bank_level,updated_at=excluded.updated_at",
                        (actor, 1_000_000 + index * 13_001, str(index % 3 + 1), shared.ANCHOR))
    with shared._uow(paths.player_db) as uow:
        apply_arena_state(uow)
        PartnerProtectionService.ensure_schema(uow.connection)
        for index, actor in enumerate(actors):
            uow.execute("INSERT INTO arena(user_id,score,rank,honor_points) VALUES(?,?,?,?) "
                        "ON CONFLICT(user_id) DO UPDATE SET score=excluded.score,rank=excluded.rank,"
                        "honor_points=excluded.honor_points", (actor, 1000 + index * 31,
                            ("青铜", "白银", "黄金")[index % 3], 500 + index * 17))
            uow.execute("INSERT INTO status(user_id,two_exp_protect) VALUES(?,?) "
                        "ON CONFLICT(user_id) DO UPDATE SET two_exp_protect=excluded.two_exp_protect",
                        (actor, ("off", "on", "refusal")[index % 3]))
    with shared._uow(paths.impart_db) as uow:
        apply_impart_base_schema(uow)
    repository = ImpartRepository(paths.impart_db)
    for actor in actors:
        repository.state(actor, ensure=True)
    with shared._uow(paths.impart_db) as uow:
        for index, actor in enumerate(actors):
            uow.execute("UPDATE xiuxian_impart SET impart_hp_per=?,impart_atk_per=?,impart_mp_per=?,"
                        "impart_lv=?,wish=? WHERE user_id=?",
                        ((index % 5 + 1) / 100, (index % 7 + 1) / 100,
                         (index % 3 + 1) / 100, 1 + index % 9, index % 31, actor))


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    if participants != PARTICIPANTS:
        raise ValueError("read strata requires exactly 128 existing participants")
    paths = context.paths
    ids = [1 + index * 999_999 // (participants - 1) for index in range(participants)]
    selected = shared._rows(paths.game_db, "SELECT user_id FROM user_xiuxian "
                            f"WHERE id IN ({shared._marks(ids)}) ORDER BY id", ids)
    actors = [str(row["user_id"]) for row in selected]
    if len(actors) != participants:
        raise ValueError("read stratum source PKs are missing")
    source = _raw_scores(paths.game_db, actors)
    manifest = gameplay.seed(context, managers, participants=participants)
    if actors != manifest["actors"]:
        raise ValueError("read stratum actors changed during base seed")
    groups = {"training_low": actors[:64], "preserved_background": actors[64:96], "exact_blob": actors[96:]}
    source_by_actor = {str(row["user_id"]): row for row in source}
    background = [source_by_actor[actor] for actor in groups["preserved_background"]]
    exact = _exact_scores(background)
    with shared._uow(paths.game_db) as uow:
        for actor in groups["preserved_background"]:
            row = source_by_actor[actor]
            uow.execute("UPDATE user_xiuxian SET exp=?,stone=? WHERE user_id=?", (row["exp"], row["stone"], actor))
        for actor, scores in zip(groups["exact_blob"], exact):
            uow.execute("UPDATE user_xiuxian SET exp=?,stone=? WHERE user_id=?",
                        (bind_sqlite_param(scores["exp"], exact=True),
                         bind_sqlite_param(scores["stone"], exact=True), actor))
    _seed_features(paths, actors)
    scores = _score_evidence(_raw_scores(paths.game_db, actors))
    original = _score_evidence(source)
    for actor in groups["preserved_background"]:
        if any(scores[actor][column] != original[actor][column] for column in ("exp", "stone")):
            raise ValueError("background original raw score/type/key was not preserved")
    for actor in groups["exact_blob"]:
        if any(scores[actor][column]["storage"] != "blob" for column in ("exp", "stone")):
            raise ValueError("exact read stratum was not stored as BLOB")
    training_max = {
        "exp": max(manifest["initial"][actor]["exp"] for actor in groups["training_low"]) + 224 * gameplay.EXP_GAIN,
        "stone": max(manifest["initial"][actor]["stone"] for actor in groups["training_low"]),
    }
    if training_max["exp"] >= manifest["exp_cap"]:
        raise ValueError("frozen writes could hit the actual cultivation cap")
    readonly = groups["preserved_background"] + groups["exact_blob"]
    readonly_min = {column: min(scores[actor][column]["canonical"] for actor in readonly)
                    for column in ("exp", "stone")}
    if any(training_max[column] >= readonly_min[column] for column in training_max):
        raise ValueError("training could change a readonly profile's exact rank")
    for actor in actors:
        for column in ("exp", "stone"):
            manifest["initial"][actor][column] = scores[actor][column]["canonical"]
    ranks, oracle = _rank_oracle(paths.game_db, actors)
    manifest.update(
        fixture_version="mixed-read-strata-v4", groups=groups, score_evidence=scores,
        original_score_evidence=original, expected_ranks=ranks, rank_oracle=oracle,
        setup_generation=oracle["generation"], feature_snapshot=_feature_snapshot(paths, actors),
        exact_score_sources={actor: {
            "background_source_pk": int(row["id"]), "background_source_user": str(row["user_id"]),
            "background_stored_canonical": {column: int(row[column + "_canonical"])
                                              for column in ("exp", "stone")},
            "family": ("int64_upper_boundary", "stored_background_neighbors", "decimal_boundary", "stored_background_spread")[index // 8],
        } for index, (actor, row) in enumerate(zip(groups["exact_blob"], background))},
        strata_ranges={group: {column: {
            "min": min(scores[actor][column]["canonical"] for actor in members),
            "max": max(scores[actor][column]["canonical"] for actor in members),
            "storage_types": sorted({scores[actor][column]["storage"] for actor in members}),
        } for column in ("exp", "stone")} for group, members in groups.items()},
        rank_stability={"training_max_including_all_224_writes": training_max,
                        "readonly_min": readonly_min,
                        "proof": "only low actors write; all reachable low keys remain below every readonly key"},
        phase_specs=phase_specs(), shuffle_seed=SHUFFLE_SEED,
        precision_rejected_actors=[actor for actor in groups["preserved_background"]
            if scores[actor]["stone"]["storage"] == "real"
            and abs(scores[actor]["stone"]["canonical"]) >= 2**53],
    )
    manifest["feature_table_rows"].update({
        table: shared._rows(database, f'SELECT COUNT(*) AS n FROM "{table}"')[0]["n"]
        for database, table in ((paths.game_db, "bank_accounts"), (paths.game_db, "sects"),
                                (paths.player_db, "arena"), (paths.player_db, "status"),
                                (paths.impart_db, "xiuxian_impart"))
    })
    manifest["limits"] = [
        "1M pre-existing users; 64 low writable, 32 preserved raw background, 32 exact BLOB readers",
        "8192 activity users and 128 populated inventory/equipment/status/bank/arena/impart actors; not all features",
        "pre-existing REAL ties remain REAL; exact BLOB boundary examples do not reconstruct lost source precision",
        "cold preserved REAL profiles may correctly refuse stone precision; excluded from rank success and successful throughput",
        "bounded setup rank oracle <=512 indexed distinct seeks/column; no request-time oracle SQL",
        "cold means process result-cache miss, not cold OS page cache; readonly rank expectations exclude mutable low ranks",
        "1024 requests and short offered-load stages are not sustained multi-user capacity proof",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = gameplay._digest(manifest)
    managers[0]._clear_read_cache()
    process_read_cache.clear()
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs)
            or mode != specs[phase_number]["mode"] or count != specs[phase_number]["requests"]):
        raise ValueError("invalid frozen read stratum plan")
    cold = mode == "pure" and phase_number == 0 and count == 128
    if count == 128 and not cold:
        raise ValueError("128 requests are reserved for the all-actor cold phase")
    low = manifest["groups"]["training_low"]
    high = manifest["groups"]["exact_blob"]
    low_order, high_order = list(low), list(high)
    random.Random(SHUFFLE_SEED).shuffle(low_order)
    random.Random(SHUFFLE_SEED + 1).shuffle(high_order)
    cold_actors = list(manifest["actors"])
    random.Random(SHUFFLE_SEED).shuffle(cold_actors)
    result, read_count, write_count, profile_count = [], 0, 0, 0
    for index in range(count):
        write = (mode == "readmostly" and index % 10 == 9
                 or mode == "mixed" and index % 10 in {7, 8, 9}
                 or mode == "contention" and index % 2 == 1)
        if write:
            actor = low[0] if mode == "contention" else low_order[write_count % len(low_order)]
            row = {"key": "stone_training", "command": "灵石修炼",
                   "input_text": f"灵石修炼 {gameplay.REQUESTED_STONE}",
                   "requested_stone": gameplay.REQUESTED_STONE, "exp_gain": gameplay.EXP_GAIN}
            write_count += 1
        else:
            key = "profile_info" if cold else READ_KEYS[read_count % len(READ_KEYS)]
            key, command, markers = READS[key]
            if cold:
                actor = cold_actors[index]
            elif key == "profile_info":
                actor = high_order[profile_count % len(high_order)]
                profile_count += 1
            else:
                actor = low[0] if mode == "contention" else low_order[read_count % len(low_order)]
            row = {"key": key, "command": command, "input_text": command, "success_markers": list(markers)}
            if key == "profile_info":
                row["expected_ranks"] = dict(manifest["expected_ranks"][actor])
                if actor in manifest.get("precision_rejected_actors", ()):
                    row["expected_guard"] = "stone_precision_lost_real"
            read_count += 1
        result.append({**row, "actor": actor, "number": 890_000_000 + phase_number * 100_000 + index,
                       "event_type": "group", "phase_label": "cold" if cold else mode})
    return result


def classify_result(request, result):
    classified = gameplay.classify_result(request, result)
    if request.get("expected_guard") == "stone_precision_lost_real":
        if classified["outcome"] in {"boundary", "timeout", "exception", "cooldown"}:
            return classified
        text = "\n".join(map(str, result.get("replies", ())))
        if "灵石余额的历史 REAL 记录已无法确定精确整数，未结算" in text:
            return {"outcome": "business_rejection", "reason": "stone_precision_lost_real"}
        return {"outcome": "unclassified", "reason": "missing_expected_precision_guard"}
    if request["key"] != "profile_info" or classified["outcome"] != "success":
        return classified
    expected = request.get("expected_ranks")
    if expected is None:
        return {"outcome": "unclassified", "reason": "missing_exact_rank_expectation"}
    text = "\n".join(map(str, result.get("replies", ())))
    for column, title in (("exp", "修为排行"), ("stone", "灵石排行")):
        actual = re.findall(re.escape(title) + r": 第([0-9]+)位", text)
        if len(actual) != 1 or int(actual[0]) != expected[column]:
            return {"outcome": "unclassified", "reason": f"exact_{column}_rank_mismatch"}
    return classified


def check_invariants(context, managers, manifest, results):
    low = set(manifest["groups"]["training_low"])
    training = dict(manifest, actors=manifest["groups"]["training_low"],
                    inventory=[row for row in manifest["inventory"] if row["user_id"] in low])
    checked = gameplay.check_invariants(context, managers, training, results)
    errors = list(checked["errors"])
    readonly = manifest["groups"]["preserved_background"] + manifest["groups"]["exact_blob"]
    actual = _score_evidence(_raw_scores(context.paths.game_db, readonly))
    expected = {actor: manifest["score_evidence"][actor] for actor in readonly}
    if actual != expected:
        errors.append("readonly raw scores/storage/keys/power changed")
    if _feature_snapshot(context.paths, manifest["actors"]) != manifest["feature_snapshot"]:
        errors.append("read/training traffic changed seeded status/features/attributes")
    if shared._inventory(context.paths.game_db, manifest["actors"]) != manifest["inventory"]:
        errors.append("read/training traffic changed readonly inventory")
    for result in results:
        request = result.get("request", result)
        if request["key"] == "stone_training" and request["actor"] not in low:
            errors.append("a write targeted the readonly rank stratum")
    with shared._uow(context.paths.game_db, read_only=True) as uow:
        generation = _generation(uow)
    has_writes = any(result.get("request", result)["key"] == "stone_training" for result in results)
    if not has_writes and generation != manifest["setup_generation"]:
        errors.append("pure reads advanced numeric rank generations")
    checks = shared._rows(context.paths.impart_db, "PRAGMA quick_check")
    if any(list(row.values()) != ["ok"] for row in checks):
        errors.append("impart fixture quick_check failed")
    return {**checked, "ok": not errors, "errors": errors, "generation_after": generation,
            "readonly_score_checks": len(readonly), "feature_checks": list(manifest["feature_snapshot"]),
            "invariants": checked["invariants"] + "; readonly raw scores/type/key/power, all inventories, status/features/attributes unchanged; pure-read generation unchanged"}
