"""Bounded live season/boss/training ranks beside the accepted player writers."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import hashlib
import json
import re

from tests import mixed_arena_training_fixture as arena_training


player = arena_training.player
shared = arena_training.shared
PARTICIPANTS = player.PARTICIPANTS
SEASON_POPULATION = 256
BOSS_POPULATION = 2048
COMMANDS = (
    ("my_season_rank", "我的赛季", "season"),
    ("boss_integral_rank", "世界BOSS积分排行榜", "boss"),
    ("training_rank", "历练排行榜", "completed"),
    ("training_points_rank", "历练积分排行榜", "points"),
)
TARGET_KEYS = frozenset(row[0] for row in COMMANDS)


def phase_specs():
    return [{**row, "order": "rank-projection-v31"} for row in player.phase_specs()]


def _encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"),
                      default=lambda item: {"blob": bytes(item).hex()}).encode("ascii")


def _snapshot(paths):
    snapshots = {}
    tables = ((paths.game_db, "season_rank", SEASON_POPULATION * 15),
              (paths.player_db, "boss_limit", BOSS_POPULATION),
              (paths.player_db, "training", arena_training.POPULATION),
              (paths.player_db, "arena", arena_training.POPULATION),
              (paths.player_db, "arena_state_operations", arena_training.POPULATION),
              (paths.game_db, "arena_season_reward_operations", arena_training.POPULATION))
    for database, table, bound in tables:
        with shared._uow(database, read_only=True) as uow:
            cursor = uow.connection.execute(
                f'SELECT rowid AS _fixture_rowid,* FROM "{table}" ORDER BY rowid LIMIT ?',
                (bound + 1,))
            digest, count = hashlib.sha256(), 0
            digest.update(_encoded([column[0] for column in cursor.description]) + b"\n")
            try:
                while rows := cursor.fetchmany(128):
                    for row in rows:
                        count += 1
                        if count > bound:
                            raise ValueError(f"V31 {table} exceeds its full-table snapshot bound")
                        digest.update(_encoded(dict(row)) + b"\n")
            finally:
                cursor.close()
            snapshots[table] = {"rows": count, "sha256": digest.hexdigest()}
    return snapshots


def _identities(now):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.periods import get_season_key
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.season_service import build_season_rank_key
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.season_rank_service import (
        DEFAULT_SEASON_RANK_TYPES, SEASON_RANK_MODES,
    )

    return [(build_season_rank_key(kind, mode, now=now), mode,
             get_season_key(now=now, mode=mode), kind)
            for mode in SEASON_RANK_MODES for kind in DEFAULT_SEASON_RANK_TYPES]


def _seed_rank_tables(paths, actors, owners, now):
    from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param
    from nonebot_plugin_xiuxian_2.features.boss.migrations import apply_boss_player_schema
    from nonebot_plugin_xiuxian_2.infrastructure.database import message_store

    ordered = list(actors) + [owner for owner in owners if owner not in set(actors)]
    if (len(set(ordered)) != len(ordered) or len(ordered) < BOSS_POPULATION
            or not actors or len(actors) > SEASON_POPULATION
            or not set(actors) <= set(owners)):
        raise ValueError("V31 population must reuse existing distinct synthetic owners")
    season_owners, boss_owners = ordered[:SEASON_POPULATION], ordered[:BOSS_POPULATION]
    identities = _identities(now)
    if len(identities) != 15 or len({row[0] for row in identities}) != 15:
        raise ValueError("V31 requires the current default fifteen distinct season keys")
    allowed_keys, allowed_owners = {row[0] for row in identities}, set(season_owners)
    with shared._uow(paths.game_db) as uow:
        message_store.apply_season_rank_schema(uow)
        existing = uow.query_all("SELECT rank_key,user_id,sect_id FROM season_rank LIMIT ?",
                                 (SEASON_POPULATION * 15 + 1,))
        if (len(existing) > SEASON_POPULATION * 15 or any(
                row["rank_key"] not in allowed_keys or str(row["user_id"]) not in allowed_owners
                or row["sect_id"] != 0 for row in existing)):
            raise ValueError("V31 refuses season rows outside the controlled owner/key namespace")
    with shared._uow(paths.player_db) as uow:
        apply_boss_player_schema(uow)
        existing = uow.query_all("SELECT user_id FROM boss_limit LIMIT ?", (BOSS_POPULATION + 1,))
        if (len(existing) > BOSS_POPULATION or any(
                str(row["user_id"]) not in set(boss_owners) for row in existing)):
            raise ValueError("V31 refuses boss rows outside the controlled owner namespace")
    with shared._uow(paths.game_db) as uow:
        for key_index, (rank_key, mode, period, kind) in enumerate(identities):
            for index, owner in enumerate(season_owners):
                score = (10 ** 40 + key_index * 100 + index // 2 if index < 8
                         else 10_000 + key_index * 100 + (SEASON_POPULATION - index) // 3)
                uow.execute(
                    "INSERT INTO season_rank(rank_key,mode,period_key,rank_type,user_id,sect_id,"
                    "score,extra,updated_at) VALUES(?,?,?,?,?,0,?,'{}',?) "
                    "ON CONFLICT(rank_key,user_id,sect_id) DO UPDATE SET "
                    "mode=excluded.mode,period_key=excluded.period_key,rank_type=excluded.rank_type,"
                    "score=excluded.score,extra=excluded.extra,updated_at=excluded.updated_at",
                    (rank_key, mode, period, kind, owner, bind_sqlite_param(score, exact=True),
                     f"{now:%Y-%m-%d} 00:{index // 60:02}:{index % 60:02}"))
        storage = {str(row["storage"]): int(row["n"]) for row in uow.query_all(
            "SELECT typeof(score) AS storage,COUNT(*) AS n FROM season_rank GROUP BY typeof(score)")}
        if storage.get("real", 0):
            raise ValueError("V31 season seed must not round oversized values through REAL")
    with shared._uow(paths.player_db) as uow:
        for index, owner in enumerate(boss_owners):
            integral = 10 ** 40 + index // 2 if index < 8 else (BOSS_POPULATION - index) // 3
            uow.execute("INSERT INTO boss_limit(user_id,integral) VALUES(?,?) "
                        "ON CONFLICT(user_id) DO UPDATE SET integral=excluded.integral",
                        (owner, bind_sqlite_param(integral, exact=True)))
    return {"identities": identities, "season_owners": season_owners,
            "boss_owners": boss_owners, "season_storage_classes": storage}


def _boss_oracle(rows, actors, names):
    first = {}
    for row in rows:
        owner = row["user_id"]
        if owner is not None and str(owner).strip():
            first.setdefault(str(owner), {**row, "value": int(row["integral"] or 0)})
    ordered = sorted(first.values(), key=lambda row: (-row["value"], row["row_id"]))
    top = [{"user_id": str(row["user_id"]), "value": row["value"],
            "name": names.get(str(row["user_id"])) or str(row["user_id"])} for row in ordered[:50]]
    values = [row["value"] for row in ordered]
    total = sum(value > 0 for value in values)
    return {"top": top, "actors": {actor: {
        "value": first.get(actor, {}).get("value", 0), "total": total,
        "rank": 1 + sum(value > first[actor]["value"] for value in values)
        if first.get(actor, {}).get("value", 0) > 0 else 0} for actor in actors}}


def _oracle(paths, actors, identities):
    with shared._uow(paths.game_db, read_only=True) as uow:
        season = uow.query_all("SELECT rowid AS row_id,* FROM season_rank ORDER BY rowid LIMIT ?",
                               (SEASON_POPULATION * 15 + 1,))
    with shared._uow(paths.player_db, read_only=True) as uow:
        boss = uow.query_all("SELECT rowid AS row_id,user_id,integral FROM boss_limit ORDER BY rowid LIMIT ?",
                             (BOSS_POPULATION + 1,))
    if len(season) != SEASON_POPULATION * 15 or len(boss) != BOSS_POPULATION:
        raise ValueError("V31 oracle requires all declared populated season/boss rows")
    bosses = [str(row["user_id"]) for row in boss]
    names = {}
    for start in range(0, len(bosses), 400):
        chunk = bosses[start:start + 400]
        for row in shared._rows(paths.game_db,
                f"SELECT user_id,user_name FROM user_xiuxian WHERE user_id IN ({shared._marks(chunk)}) "
                "ORDER BY rowid", chunk):
            names.setdefault(str(row["user_id"]), row["user_name"])
    by_key = {}
    for row in season:
        by_key.setdefault(row["rank_key"], []).append({**row, "value": int(row["score"] or 0)})
    seasons = {actor: [] for actor in actors}
    for key, mode, period, kind in identities:
        rows = by_key.get(key, [])
        if len(rows) != SEASON_POPULATION:
            raise ValueError("V31 season key lacks its complete population")
        for actor in actors:
            own = sorted((row for row in rows if str(row["user_id"]) == actor),
                         key=lambda row: (-row["value"], row["updated_at"], row["row_id"]))
            if not own:
                raise ValueError("V31 season oracle is missing a dispatch actor")
            value = own[0]["value"]
            seasons[actor].append({"rank_key": key, "mode": mode, "period": period,
                "rank_type": kind, "value": value, "rank": 1 + sum(row["value"] > value for row in rows)})
    return {"season": seasons, "boss": _boss_oracle(boss, actors, names)}


def seed(context, managers, *, participants=PARTICIPANTS):
    manifest = arena_training.seed(context, managers, participants=participants)
    actors = manifest["actors"]
    owners = [str(row["user_id"]) for row in shared._rows(context.paths.player_db,
        "SELECT user_id FROM avatar ORDER BY user_id LIMIT ?", (arena_training.POPULATION + 1,))]
    if len(owners) != arena_training.POPULATION or len(set(owners)) != len(owners):
        raise ValueError("V31 requires the existing 8192 synthetic avatar owners")
    population = _seed_rank_tables(context.paths, actors, owners, datetime.now())
    snapshot = _snapshot(context.paths)
    manifest.update(fixture_version="mixed-rank-projection-v31", phase_specs=phase_specs(),
        rank_projection={"population": population,
            "oracle": _oracle(context.paths, actors, population["identities"]), "snapshot": snapshot,
            "cache_observation": "actual hits/misses are driver phase deltas, not inferred per-request attribution; one phase-start clear only; no traffic cache clears"})
    manifest["feature_table_rows"].update({table: row["rows"] for table, row in snapshot.items()})
    manifest["limits"] = [line for line in manifest["limits"] if not line.startswith((
        "four target paths replace relation reads", "arena honor preserves"))] + [
        "384 requests: pure72/mixed40/contention40 target slots cycle season/boss/training; training fields alternate; 56 exact profiles/phase and 32 real exp+stone writes per mixed phase retained",
        "season has15current keys x256existing owners=3840rows; boss has2048existing owners; training retains8192rows; feature tables are not million-row tables",
        "season/boss traffic is readonly; integer and exact ASCII-BLOB oversized scores are seeded without REAL rounding; season score-update behavior is outside this sample",
        "Python exact-integer competition ranks and stable TOP ordering are setup oracles; all six selected table hashes stream full bounded contents; zero request-time oracle SQL",
        "public training/boss snapshots retain normal TTL; repeated reads mix natural cold misses and warm hits; phase cache counters do not identify each caller's hit rate",
        "all requests share one target and DB; restore only after drain/invariants; no all674 coverage or sustained50successful/s claim",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = player.strata.gameplay._digest(manifest)
    return manifest


def _target_specs(count, phase_number):
    specs = []
    for index in range(count):
        cycle = index % 3
        specs.append(COMMANDS[cycle if cycle < 2 else 2 + ((index // 3 + phase_number) % 2)])
    return specs


def request_plan(manifest, mode, count, phase_number=0):
    plan = player.request_plan(manifest, mode, count, phase_number)
    readers = manifest["groups"]["training_low"] + manifest["groups"]["exact_blob"]
    if len(readers) != 96 or len(set(readers)) != 96:
        raise ValueError("V31 target readers require the96exact-value participants")
    slots = [index for index, row in enumerate(plan) if row["key"] in {"my_partner", "my_mentor"}]
    expected = 72 if mode == "pure" else 40
    if len(slots) != expected:
        raise ValueError("V31 frozen target slot count changed")
    for ordinal, (index, spec) in enumerate(zip(slots, _target_specs(expected, phase_number))):
        key, command, field = spec
        actor = readers[(ordinal * 11 + phase_number * 3) % len(readers)]
        row = plan[index]
        request = {"key": key, "command": command, "input_text": command, "actor": actor,
            "number": row["number"], "event_type": "group", "phase_label": mode}
        oracle = manifest["rank_projection"]["oracle"]
        if field == "season":
            request["expected_rank_projection"] = oracle["season"][actor]
        elif field == "boss":
            request["expected_rank_projection"] = {**oracle["boss"]["actors"][actor],
                                                    "top": oracle["boss"]["top"]}
        else:
            training = manifest["arena_training"]["oracle"]["training"][field]
            request["expected_arena_training"] = {**training["actors"][actor],
                                                   "top": training["top"], "field": field}
        plan[index] = request
    return plan


def classify_result(request, result):
    if request["key"] in {"training_rank", "training_points_rank"}:
        return arena_training.classify_result(request, result)
    if request["key"] not in TARGET_KEYS:
        return player.classify_result(request, result)
    classified = shared.classify_result(request, result)
    if classified["outcome"] in {"boundary", "timeout", "exception", "cooldown", "business_rejection"}:
        return classified
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to

    text = "\n".join(map(str, result.get("replies", ()))).replace("\r\n", "\n").replace("\r", "\n")
    expected = request["expected_rank_projection"]
    if request["key"] == "my_season_rank":
        text = re.sub(r"\A\[CQ:at,qq=" + re.escape(str(request["actor"])) + r"\][ \t]*", "", text)
        lines, headings = ["【我的赛季】"], []
        labels = {"weekly": "周榜", "monthly": "月榜", "quarterly": "季度榜"}
        for row in expected:
            heading = f"【{row['period']}{labels[row['mode']]}】"
            if heading not in headings:
                headings.append(heading)
                lines.append(heading)
            lines.append(f"{row['rank_type']}：积分{number_to(row['value'])}，当前第{row['rank']}名")
        if [line.strip() for line in text.splitlines() if line.strip()] != lines:
            return {"outcome": "unclassified", "reason": "rank_projection_exact_reply_mismatch"}
        return {"outcome": "success", "reason": "exact_rank_projection_reply"}
    else:
        lines = ["【世界BOSS积分排行榜】"] + [
            f"第{index}位 | {row['name']} | {number_to(row['value'])}\n"
            for index, row in enumerate(expected["top"], 1)]
        lines += [f"你的名次：第{expected['rank']}名 / 共{expected['total']}人"
                  f"（世界积分 {number_to(expected['value'])}点）" if expected["rank"]
                  else "你还没有世界积分记录。", "公共榜单快照截至"]
    if not all(text.count(line) == 1 for line in lines):
        return {"outcome": "unclassified", "reason": "rank_projection_exact_reply_mismatch"}
    offsets = [text.index(line) for line in lines]
    if offsets != sorted(offsets):
        return {"outcome": "unclassified", "reason": "rank_projection_reply_order_mismatch"}
    return {"outcome": "success", "reason": "exact_rank_projection_reply"}


def check_invariants(context, managers, manifest, results):
    controls = [row for row in results if row.get("request", row)["key"] not in TARGET_KEYS]
    checked = player.check_invariants(context, managers, manifest, controls)
    errors = list(checked["errors"])
    snapshot = _snapshot(context.paths)
    if snapshot != manifest["rank_projection"]["snapshot"]:
        errors.append("V31 season/boss/training/arena rows or operations changed")
    counts = Counter(row.get("request", row)["key"] for row in results)
    for row in results:
        request = row.get("request", row)
        if request["key"] in TARGET_KEYS:
            if row.get("outcome") != "success" or classify_result(request, row)["outcome"] != "success":
                errors.append(f"V31 exact target reply failed: {request['key']}")
    if results:
        first = results[0].get("request", results[0])
        mode = first["phase_label"]
        phase = {"pure": 0, "mixed": 1, "contention": 2}.get(mode, -1)
        expected = Counter(spec[0] for spec in _target_specs(72 if mode == "pure" else 40, phase))
        expected.update(profile_info=56, stone_training=0 if mode == "pure" else 32)
        if counts != +expected or len(results) != 128 or any(
                row.get("request", row)["phase_label"] != mode for row in results):
            errors.append("V31 frozen target/control command counts changed")
    return {**checked, "ok": not errors, "errors": errors,
        "outcomes": dict(Counter(row.get("outcome") for row in results)),
        "rank_projection_checks": {"per_command": dict(counts), "snapshot": snapshot,
            "oracle": "setup Python integer competition ranks/rowid ties, full read-only table hashes; zero traffic oracle SQL",
            "cache_observation": manifest["rank_projection"]["cache_observation"]},
        "invariants": checked["invariants"] + "; V31 full bounded season/boss/training/arena/operation hashes unchanged; exact15season ranks and ordered public/individual rank replies"}
