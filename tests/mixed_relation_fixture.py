"""Frozen, populated relation reads with real low-stratum training writes."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import random
import re

from tests import mixed_read_strata_fixture as strata


PARTICIPANTS = 128
SHUFFLE_SEED = 20261129
PENDING_APPLICATIONS = 4096
PENDING_PER_MASTER = 128
PENDING_EXPIRES_AT = 4_102_444_800
READ_KEYS = ("profile_info", "my_mentor", "my_partner", "my_mentor", "my_partner")
MENTOR_FIELDS = {
    "mentor_id": "TEXT", "apprentice_ids": "TEXT", "bind_time": "TEXT",
    "mentor_cd_until": "TEXT", "apprentice_cd_until": "TEXT",
    "mentor_rebind_cd": "TEXT", "mentor_history": "TEXT", "mentor_protect": "TEXT",
    "mentor_apply_time": "TEXT", "mentor_apply_target": "TEXT",
    "breakthrough_reward_count": "INTEGER",
}
PARTNER_FIELDS = {"partner_id": "TEXT", "bind_time": "TEXT", "affection": "INTEGER"}


def phase_specs():
    return [
        {"label": label, "mode": label, "requests": count,
         "concurrency": limit, "arrival_rate": rate, "order": "relation-seed-20261129"}
        for label, count, limit, rate in (
            ("pure", 160, 10, 20), ("mixed", 256, 20, 35), ("contention", 192, 20, 50),
        )
    ]


def _relation_layout(groups):
    high, low, background = (list(groups[name]) for name in
                             ("exact_blob", "training_low", "preserved_background"))
    actors = low + background + high
    if (len(high), len(low), len(background), len(set(actors))) != (32, 64, 32, 128):
        raise ValueError("relation layout requires disjoint 32/64/32 actor strata")
    mentor = {actor: {
        "mentor_id": None, "apprentice_ids": [], "bind_time": None,
        "mentor_cd_until": None, "apprentice_cd_until": None,
        "mentor_rebind_cd": {}, "mentor_history": [], "mentor_protect": "off",
        "mentor_apply_time": None, "mentor_apply_target": None,
        "breakthrough_reward_count": 0,
    } for actor in actors}
    offset = 0
    for index, master in enumerate(high):
        count = 7 if index == 0 else 3 if index == 1 else 5 if index < 8 else 1
        children = low[offset:offset + count]
        mentor[master]["apprentice_ids"] = children
        for child in children:
            mentor[child].update(mentor_id=master, bind_time=strata.shared.ANCHOR)
        offset += count
    if offset != len(low):
        raise ValueError("relation layout did not assign every low actor exactly once")
    pairs = list(zip(high, low[:32]))
    pairs += list(zip(low[32::2], low[33::2]))
    pairs += list(zip(background[::2], background[1::2]))
    partner = {}
    for index, (first, second) in enumerate(pairs):
        for owner, target in ((first, second), (second, first)):
            partner[owner] = {"partner_id": target, "bind_time": strata.shared.ANCHOR,
                              "affection": 1000 + index * 137 + (owner == second)}
    return {"mentor": mentor, "partner": partner}


def _write_relation_tables(paths, layout, high):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.transaction_service import MentorApplicationService

    applicants = strata.shared._rows(paths.game_db, "SELECT user_id FROM user_xiuxian "
                                    f"WHERE user_id NOT IN ({strata.shared._marks(layout['mentor'])}) "
                                    "ORDER BY id LIMIT ?", (*layout["mentor"], PENDING_APPLICATIONS))
    if len(applicants) != PENDING_APPLICATIONS:
        raise ValueError("relation fixture needs 4096 existing nonactor mentor applicants")
    with strata.shared._uow(paths.player_db) as uow:
        for table, fields in (("mentor", MENTOR_FIELDS), ("partner", PARTNER_FIELDS)):
            uow.execute(f'CREATE TABLE IF NOT EXISTS "{table}"(user_id TEXT PRIMARY KEY)')
            columns = {row["name"] for row in uow.query_all(f'PRAGMA table_info("{table}")')}
            for field, kind in fields.items():
                if field not in columns:
                    uow.execute(f'ALTER TABLE "{table}" ADD COLUMN "{field}" {kind}')
            names = ["user_id", *fields]
            quoted = ",".join(f'"{field}"' for field in names)
            updates = ",".join(f'"{field}"=excluded."{field}"' for field in fields)
            for actor, data in layout[table].items():
                values = [json.dumps(data[field], ensure_ascii=False) if isinstance(data[field], (dict, list))
                          else data[field] for field in fields]
                uow.execute(f'INSERT INTO "{table}"({quoted}) VALUES({strata.shared._marks(names)}) '
                            f'ON CONFLICT(user_id) DO UPDATE SET {updates}', (actor, *values))
        MentorApplicationService._ensure_schema(uow.connection)
        if uow.query_one("SELECT invite_id FROM mentor_applications LIMIT 1"):
            raise ValueError("relation fixture requires the original application table to be empty")
        for index, applicant in enumerate(applicants):
            uow.execute("INSERT INTO mentor_applications(invite_id,mentor_id,apprentice_id,status,"
                        "created_at,expires_at,resolved_at) VALUES(?,?,?,'pending',?,?,NULL)",
                        (f"codex-relation-v5:invite:{index:04d}", high[index % len(high)],
                         str(applicant["user_id"]), 1_767_225_600 + index, PENDING_EXPIRES_AT))


def _mentor_cd_path(paths, store):
    source = (Path(paths.game_db).parent / "diagnostic-legacy").resolve()
    target = Path(store.data_path).resolve()
    if (not source.is_dir() or not target.is_relative_to(source)
            or target.name != "mentor_exp_cd.json"):
        raise ValueError("mentor cooldown JSON is outside isolated fixture diagnostic-legacy")
    return target


def _seed_mentor_cd(paths, actors, store):
    target = _mentor_cd_path(paths, store)
    data = {"mentor_exp_cd": {actor: 0 for actor in actors}}
    target.write_text(json.dumps(data, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    store.data = deepcopy(data)
    return data


def _json_snapshot(paths):
    root = Path(paths.game_db).parent.parent.resolve()
    trees = (Path(paths.game_db).parent, root / "command-source")
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for tree in trees for path in sorted(tree.rglob("*.json")) if path.is_file()}


def _relation_snapshot(paths):
    return {table: strata.shared._rows(paths.player_db, f'SELECT * FROM "{table}" ORDER BY {order}')
            for table, order in (("mentor", "user_id"), ("partner", "user_id"),
                                 ("mentor_applications", "invite_id"))}


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_exp_cd import mentor_exp_cd

    if participants != PARTICIPANTS:
        raise ValueError("relation fixture requires exactly 128 existing participants")
    _mentor_cd_path(context.paths, mentor_exp_cd)
    manifest = strata.seed(context, managers, participants=participants)
    layout = _relation_layout(manifest["groups"])
    _write_relation_tables(context.paths, layout, manifest["groups"]["exact_blob"])
    cd_data = _seed_mentor_cd(context.paths, manifest["actors"], mentor_exp_cd)
    service = partner._mentor_application_service()
    if Path(service._player_database).resolve() != Path(context.paths.player_db).resolve():
        raise ValueError("pending mentor application service escaped the shared isolated player DB")
    if len(service.list_pending(manifest["groups"]["exact_blob"][0])) != PENDING_PER_MASTER:
        raise ValueError("relation fixture pending invitations do not match the seeded live service")
    actors = manifest["actors"]
    profiles = strata.shared._rows(context.paths.game_db, "SELECT user_id,user_name,level FROM user_xiuxian "
                                  f"WHERE user_id IN ({strata.shared._marks(actors)})", actors)
    if len(profiles) != len(actors):
        raise ValueError("relation fixture actor display profiles are missing")
    manifest.update(
        fixture_version="mixed-relation-v5", phase_specs=phase_specs(), shuffle_seed=SHUFFLE_SEED,
        relations=layout, relation_profiles={str(row["user_id"]): row for row in profiles},
        relation_snapshot=_relation_snapshot(context.paths), mentor_cd_data=cd_data,
        relation_limits={"max_apprentices": int(partner.MENTOR_MAX_APPRENTICES),
                         "transmission_limit": int(partner.mentor_transmission_limit)},
        relation_seed_counts={"mentor_rows": 128, "mentor_pairs": 64, "partner_rows": 128,
                              "partner_pairs": 64, "pending_applications": PENDING_APPLICATIONS,
                              "pending_per_master": PENDING_PER_MASTER, "mentor_cd_users": 128},
    )
    for table, rows in manifest["relation_snapshot"].items():
        manifest["feature_table_rows"][table] = len(rows)
    manifest["json_snapshot"] = _json_snapshot(context.paths)
    manifest["limits"] += [
        "608 requests across profile/relation/training paths; not all 674 matchers or sustained 50/s capacity",
        "32 exact high masters and 64 low pupils; 32 preserved REAL actors have empty mentors and only pair with each other",
        "legacy full-profile display normalizes another user's balance; preserved REAL actors are excluded as relation readers/targets",
        "historical seven-pupil master is displayed intact above the configured new-binding cap; no gameplay rule is changed",
        "4096 live mentor applications on existing nonactor users, 128 per high master; no expired-row cleanup claim",
        "cooldowns remain enabled; zero transmission usage is seeded only in the existing isolated diagnostic-legacy JSON",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = strata.gameplay._digest(manifest)
    for manager in managers:
        for method in ("_clear_read_cache", "_clear_field_list_cache"):
            clear = getattr(manager, method, None)
            if callable(clear):
                clear()
    return manifest


def _relation_expectation(manifest, actor):
    relations, profiles = manifest["relations"], manifest["relation_profiles"]
    mentor = relations["mentor"][actor]
    spouse = relations["partner"][actor]
    projection = lambda target: {field: profiles[target][field] for field in ("user_name", "level")}
    return {"actor": projection(actor),
            "mentor": projection(mentor["mentor_id"]) if mentor["mentor_id"] else None,
            "apprentices": [projection(child) for child in mentor["apprentice_ids"]],
            "partner": {**projection(spouse["partner_id"]), "bind_time": spouse["bind_time"],
                        "affection": spouse["affection"]},
            "pending_count": PENDING_PER_MASTER if actor in manifest["groups"]["exact_blob"] else 0,
            "max_apprentices": manifest["relation_limits"]["max_apprentices"],
            "transmission_limit": manifest["relation_limits"]["transmission_limit"]}


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs) or mode != specs[phase_number]["mode"]
            or count != specs[phase_number]["requests"]):
        raise ValueError("invalid frozen relation plan")
    high, low = (list(manifest["groups"][name]) for name in ("exact_blob", "training_low"))
    random.Random(SHUFFLE_SEED).shuffle(high)
    low_order = list(low)
    random.Random(SHUFFLE_SEED + 1).shuffle(low_order)
    counts = {key: 0 for key in ("profile_info", "my_mentor", "my_partner", "stone_training")}
    result, reads = [], 0
    for index in range(count):
        write = mode == "mixed" and index % 4 == 3 or mode == "contention" and index % 2 == 1
        if write:
            key = "stone_training"
            actor = low[0] if mode == "contention" else low_order[counts[key] % len(low_order)]
            row = {"key": key, "command": "灵石修炼", "input_text": f"灵石修炼 {strata.gameplay.REQUESTED_STONE}",
                   "requested_stone": strata.gameplay.REQUESTED_STONE, "exp_gain": strata.gameplay.EXP_GAIN}
        else:
            sequence = READ_KEYS if mode == "pure" else READ_KEYS[:3]
            key = sequence[reads % len(sequence)]
            number = counts[key]
            if key == "profile_info":
                actor = high[number % len(high)]
                row = {"key": key, "command": "我的修仙信息", "input_text": "我的修仙信息",
                       "success_markers": ["道号", "修为排行", "灵石排行"],
                       "expected_ranks": dict(manifest["expected_ranks"][actor]),
                       "expected_relation": _relation_expectation(manifest, actor)}
            else:
                actor = high[(number // 2) % len(high)] if number % 2 == 0 else low_order[(number // 2) % len(low_order)]
                command = "我的道侣" if key == "my_partner" else "我的师徒"
                if key == "my_mentor":
                    if number % 2 == 0 and number // 2 % 2:
                        command = "我的徒弟"
                    elif number % 2:
                        command = ("我的师徒", "我的师父", "我的师傅")[(number // 2) % 3]
                row = {"key": key, "command": command, "input_text": command,
                       "success_markers": ["我的道侣", "结契信息"] if key == "my_partner" else
                       ["我的徒弟" if command == "我的徒弟" else "师父", "今日剩余传功次数"],
                       "expected_relation": _relation_expectation(manifest, actor)}
            reads += 1
        counts[key] += 1
        result.append({**row, "actor": actor, "number": 890_000_000 + phase_number * 100_000 + index,
                       "event_type": "group", "phase_label": mode})
    return result


def classify_result(request, result):
    classified = strata.classify_result(request, result)
    if request["key"] not in {"profile_info", "my_mentor", "my_partner"}:
        return classified
    if request["key"] == "profile_info" and classified["outcome"] != "success":
        return classified
    if classified["outcome"] in {"boundary", "timeout", "exception", "cooldown", "business_rejection"}:
        return classified
    expected = request.get("expected_relation")
    text = "\n".join(map(str, result.get("replies", ()))).replace("\r\n", "\n").replace("\r", "\n")
    if expected is None or not all(marker in text for marker in request.get("success_markers", ())):
        return {"outcome": "unclassified", "reason": "missing_relation_response"}
    if request["key"] == "profile_info":
        spouse, children = expected["partner"], expected["apprentices"]
        names = "、".join(child["user_name"] for child in children[:3]) if children else "无"
        if len(children) > 3:
            names += f"等{len(children)}人"
        mentor_name = expected["mentor"]["user_name"] if expected["mentor"] else "无"
        pattern = (r"关系：道侣：" + re.escape(spouse["user_name"]) + r" \([^\n；]+\)；师父："
                   + re.escape(mentor_name) + "；徒弟：" + re.escape(names) + r"(?:\n|$)")
        if len(re.findall(pattern, text)) != 1:
            return {"outcome": "unclassified", "reason": "profile_relation_mismatch"}
        if f"道号: {expected['actor']['user_name']}\n" not in text or f"境界: {expected['actor']['level']}\n" not in text:
            return {"outcome": "unclassified", "reason": "profile_actor_mismatch"}
    elif request["key"] == "my_partner":
        spouse = expected["partner"]
        for title, value in (("道侣", spouse["user_name"]), ("境界", spouse["level"]),
                             ("绑定时间", spouse["bind_time"])):
            actual = re.findall(re.escape(title) + r"\s*\n> ([^\n]+)(?:\n|$)", text)
            if actual != [str(value)]:
                return {"outcome": "unclassified", "reason": "partner_projection_mismatch"}
        if len(re.findall(r"亲密度\s*\n> " + str(spouse["affection"]) + r"（", text)) != 1:
            return {"outcome": "unclassified", "reason": "partner_affection_mismatch"}
    else:
        command, mentor = request["command"], expected["mentor"]
        if command != "我的徒弟":
            line = (f"{mentor['user_name']}（{mentor['level']}，拜师时间：{strata.shared.ANCHOR}）"
                    if mentor else "无")
            pattern = r"师父：" if command in {"我的师父", "我的师傅"} else r"师父\s*\n> "
            if len(re.findall(pattern + re.escape(line) + r"(?:\n|$)", text)) != 1:
                return {"outcome": "unclassified", "reason": "mentor_projection_mismatch"}
        if command not in {"我的师父", "我的师傅"}:
            count = f"徒弟（{len(expected['apprentices'])}/{expected['max_apprentices']}）"
            if text.count(count) != 1:
                return {"outcome": "unclassified", "reason": "apprentice_count_mismatch"}
            actual = re.findall(r"(?:^|\n)(?:> )?- ([^\n]+)", text)
            wanted = [f"{child['user_name']}（{child['level']}）" for child in expected["apprentices"]]
            if actual != wanted:
                return {"outcome": "unclassified", "reason": "apprentice_order_or_projection_mismatch"}
            pending = (r"待处理拜师申请：" if command == "我的徒弟" else r"待处理拜师申请\s*\n> ")
            if len(re.findall(pending + str(expected["pending_count"]) + r"条(?:\n|$)", text)) != 1:
                return {"outcome": "unclassified", "reason": "pending_mentor_count_mismatch"}
        limit = expected["transmission_limit"]
        if not re.search(r"今日剩余传功次数[：\s]*(?:> )?" + f"{limit}/{limit}" + r"(?:\n|$)", text):
            return {"outcome": "unclassified", "reason": "mentor_transmission_usage_mismatch"}
    return {"outcome": "success", "reason": "exact_relation_response"}


def check_invariants(context, managers, manifest, results):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.mentor_exp_cd import mentor_exp_cd

    checked = strata.check_invariants(context, managers, manifest, results)
    errors = list(checked["errors"])
    if _relation_snapshot(context.paths) != manifest["relation_snapshot"]:
        errors.append("relation rows or pending mentor invitations changed")
    if _json_snapshot(context.paths) != manifest["json_snapshot"]:
        errors.append("relation traffic changed isolated JSON files")
    if mentor_exp_cd.data != manifest["mentor_cd_data"]:
        errors.append("relation traffic changed in-memory mentor transmission usage")
    _mentor_cd_path(context.paths, mentor_exp_cd)
    return {**checked, "ok": not errors, "errors": errors,
            "relation_seed_counts": manifest["relation_seed_counts"],
            "invariants": checked["invariants"] + "; exact relation/application rows and all isolated JSON unchanged; mentor transmission singleton unchanged"}
