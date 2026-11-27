"""Populated arena/training reads alongside the existing live cultivation writes."""
from __future__ import annotations

from collections import Counter
import hashlib
import json

from nonebot_plugin_xiuxian_2.infrastructure.clock import SystemClock
from tests import mixed_player_projection_fixture as player


shared = player.strata.shared
PARTICIPANTS = player.PARTICIPANTS
POPULATION = 8192
COMMANDS = (
    ("training_rank", "历练排行榜", "completed"),
    ("training_points_rank", "历练积分排行榜", "points"),
    ("arena_info", "我的竞技场", "info"),
    ("arena_honor", "我的荣誉", "honor"),
)


def phase_specs():
    return [{**row, "order": "arena-training-v18"} for row in player.phase_specs()]


def _digest(rows):
    encoded = json.dumps(rows, sort_keys=True, ensure_ascii=True, separators=(",", ":"),
                         default=lambda value: {"blob": value.hex()}).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _snapshot(paths):
    result = {}
    with shared._uow(paths.player_db, read_only=True) as uow:
        for table in ("arena", "training", "arena_state_operations"):
            rows = uow.query_all(f'SELECT * FROM "{table}" ORDER BY rowid LIMIT ?', (POPULATION + 1,))
            if len(rows) > POPULATION:
                raise ValueError("arena/training snapshot exceeded its row bound")
            result[table] = {"rows": len(rows), "sha256": _digest(rows)}
    result["honor_receipts"] = shared._rows(paths.game_db,
        "SELECT * FROM arena_season_reward_operations ORDER BY rowid LIMIT ?", (POPULATION + 1,))
    if len(result["honor_receipts"]) > POPULATION:
        raise ValueError("honor receipt snapshot exceeded its row bound")
    return result


def _oracle(paths, actors):
    from nonebot_plugin_xiuxian_2.core.numeric import as_int_like

    with shared._uow(paths.player_db, read_only=True) as uow:
        arena = uow.query_all("SELECT rowid AS row_id,* FROM arena ORDER BY rowid")
        training = uow.query_all("SELECT rowid AS row_id,* FROM training ORDER BY rowid")
    if len(arena) != POPULATION or len(training) != POPULATION:
        raise ValueError("arena/training oracle requires 8192 populated rows each")
    ordered = sorted(arena, key=lambda row: -as_int_like(row["score"]))
    ordinals = {str(row["user_id"]): index for index, row in enumerate(ordered[:1000], 1)}
    by_actor = {str(row["user_id"]): row for row in arena if str(row["user_id"]) in actors}
    result = {"arena": {}, "training": {}}
    for actor in actors:
        row = by_actor[actor]
        position = ordinals.get(actor, 0)
        base = {"青铜": 100, "白银": 200, "黄金": 300, "铂金": 400,
                "钻石": 600, "王者": 1000}.get(row["rank"], 100)
        bonus = (500 if position == 1 else 300 if 2 <= position <= 3 else
                 200 if 4 <= position <= 10 else 100 if 11 <= position <= 50 else
                 50 if 51 <= position <= 100 else 0)
        cap = 10 + int(row["daily_extra_challenges"])
        result["arena"][actor] = {
            "score": as_int_like(row["score"]), "rank": row["rank"],
            "honor_points": as_int_like(row["honor_points"]),
            "total_honor_earned": as_int_like(row["total_honor_earned"]),
            "position": position, "base": base, "bonus": bonus,
            "total": base + bonus, "cap": cap,
            "remaining": max(0, cap - int(row["daily_challenges_used"])),
            "buys": int(row["daily_challenge_buys"]),
        }
    for field in ("completed", "points"):
        ordered = sorted(training, key=lambda row: -as_int_like(row[field]))
        top = [{"user_id": str(row["user_id"]), "value": as_int_like(row[field])}
               for row in ordered[:50]]
        names = shared._rows(paths.game_db,
            f"SELECT user_id,user_name FROM user_xiuxian WHERE user_id IN ({shared._marks(top)})",
            [row["user_id"] for row in top])
        names = {str(row["user_id"]): row["user_name"] for row in names}
        values = {str(row["user_id"]): as_int_like(row[field]) for row in ordered}
        total = sum(value > 0 for value in values.values())
        result["training"][field] = {
            "top": [{**row, "name": names[row["user_id"]]} for row in top],
            "actors": {actor: {"value": values[actor], "total": total,
                "rank": 1 + sum(value > values[actor] for value in values.values())
                if values[actor] > 0 else 0} for actor in actors},
        }
    return result


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param
    from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_state
    from nonebot_plugin_xiuxian_2.features.arena.state_repository import ARENA_FIELDS, ArenaStateRepository
    from nonebot_plugin_xiuxian_2.features.training.migrations import apply_training_state

    manifest = player.seed(context, managers, participants=participants)
    actors = manifest["actors"]
    owners = shared._rows(context.paths.player_db,
                         "SELECT user_id FROM avatar ORDER BY user_id LIMIT ?", (POPULATION + 1,))
    owners = [str(row["user_id"]) for row in owners]
    if len(owners) != POPULATION or not set(actors) <= set(owners):
        raise ValueError("arena/training owners must be the existing 8192 synthetic avatar owners")
    today = SystemClock().now().date()
    defaults = ArenaStateRepository._default(today)
    defaults["weekly_purchases"] = json.dumps(defaults["weekly_purchases"], sort_keys=True)
    actor_index = {actor: index for index, actor in enumerate(actors)}
    owner_set = set(owners)
    with shared._uow(context.paths.player_db) as uow:
        apply_arena_state(uow)
        apply_training_state(uow)
        for table in ("arena", "training"):
            count = uow.query_one(f'SELECT COUNT(*) AS n FROM "{table}"')["n"]
            if count > POPULATION:
                raise ValueError("existing arena/training population exceeds the bound")
            existing = uow.query_all(f'SELECT user_id FROM "{table}"')
            if any(str(row["user_id"]) not in owner_set for row in existing):
                raise ValueError("existing arena/training owner escapes the declared population")
        for index, actor in enumerate(owners):
            state = dict(defaults)
            score = 5000 + (POPULATION - index) // 3
            if actor in actor_index:
                offset = actor_index[actor]
                score = 1_000_000 - offset // 2 if offset < 16 else 1000 + offset
                state.update(honor_points=500 + offset, total_honor_earned=1000 + offset,
                             daily_extra_challenges=offset % 3, daily_challenge_buys=offset % 3,
                             daily_challenges_used=offset % 7, rank=("青铜", "黄金", "王者")[offset % 3])
            state["score"] = score
            values = tuple(state[field] for field in ARENA_FIELDS)
            uow.execute(f"INSERT INTO arena(user_id,{','.join(ARENA_FIELDS)}) "
                        f"VALUES(?,{shared._marks(values)}) ON CONFLICT(user_id) DO UPDATE SET "
                        + ",".join(f'{field}=excluded.{field}' for field in ARENA_FIELDS), (actor, *values))
            completed = (POPULATION - index) // 3
            points = completed * 17
            if actor not in actor_index and index < 8:
                points = 10 ** 40 + index
            uow.execute("INSERT INTO training(user_id,completed,points) VALUES(?,?,?) "
                        "ON CONFLICT(user_id) DO UPDATE SET completed=excluded.completed,points=excluded.points",
                        (actor, completed, bind_sqlite_param(points, exact=True)))
    manifest["feature_snapshot"] = player.strata._feature_snapshot(context.paths, actors)
    manifest.update(fixture_version="mixed-arena-training-v18", phase_specs=phase_specs(),
        arena_training={"oracle": _oracle(context.paths, set(actors)),
                        "snapshot": _snapshot(context.paths), "date": today.isoformat()})
    manifest["feature_table_rows"].update(arena=POPULATION, training=POPULATION)
    manifest["limits"] += [
        "8192 arena/training rows each, tied values and >int64 training points; current normalized dates",
        "four target paths replace relation reads; 56 exact profiles/phase and 32 real writes per mixed phase retained",
        "arena honor preserves legacy TOP1000 ordinal, including outside=0; preview is not reward settlement",
        "arena/training rows remain read-only in traffic; cultivation changes game exp/stone only",
        "short 384-request scenario is neither all674 coverage nor sustained50 successful requests/s",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = player.strata.gameplay._digest(manifest)
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    plan = player.request_plan(manifest, mode, count, phase_number)
    readers = manifest["groups"]["training_low"] + manifest["groups"]["exact_blob"]
    if len(readers) != 96 or len(set(readers)) != 96:
        raise ValueError("arena/training reads require 96 exact-value participants")
    targets = 0
    for index, row in enumerate(plan):
        if row["key"] not in {"my_partner", "my_mentor"}:
            continue
        key, command, field = COMMANDS[targets % len(COMMANDS)]
        actor = readers[(targets // 4 * 11 + phase_number * 3) % len(readers)]
        oracle = manifest["arena_training"]["oracle"]
        expected = (oracle["arena"][actor] if field in {"info", "honor"} else
                    {**oracle["training"][field]["actors"][actor],
                     "top": oracle["training"][field]["top"], "field": field})
        plan[index] = {"key": key, "command": command, "input_text": command, "actor": actor,
                       "number": row["number"], "event_type": "group", "phase_label": mode,
                       "expected_arena_training": expected}
        targets += 1
    if targets != (72 if mode == "pure" else 40):
        raise ValueError("arena/training target slot count changed")
    return plan


def classify_result(request, result):
    if request["key"] not in {row[0] for row in COMMANDS}:
        return player.classify_result(request, result)
    classified = shared.classify_result(request, result)
    if classified["outcome"] in {"boundary", "timeout", "exception", "cooldown", "business_rejection"}:
        return classified
    text = "\n".join(map(str, result.get("replies", ()))).replace("\r\n", "\n").replace("\r", "\n")
    expected, key = request["expected_arena_training"], request["key"]
    if key.startswith("training_"):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to
        field = expected["field"]
        lines = [f"第{index}位 | {row['name']} | {number_to(row['value'])}\n"
                 for index, row in enumerate(expected["top"], 1)]
        label, unit = ("历练完成次数", "次") if field == "completed" else ("历练积分", "点")
        lines += ([f"你的名次：第{expected['rank']}名 / 共{expected['total']}人"
                   f"（{label} {number_to(expected['value'])}{unit}）"] if expected["rank"] else
                  [f"你还没有{label}记录。"])
        lines += ["公共榜单快照截至"]
    elif key == "arena_info":
        lines = ["**竞技场信息**", f"当前积分\n> {expected['score']}\n",
                 f"当前段位\n> {expected['rank']}\n",
                 f"今日挑战\n> {expected['remaining']}/{expected['cap']}次\n",
                 f"今日购买\n> {expected['buys']}/3次"]
    else:
        lines = ["**我的荣誉**", f"当前段位\n> {expected['rank']}\n",
                 f"当前排名\n> 第{expected['position']}名\n",
                 f"当前荣誉值\n> {expected['honor_points']}点\n",
                 f"累计获得荣誉值\n> {expected['total_honor_earned']}点\n",
                 f"基础奖励\n> {expected['base']}点", f"排名奖励\n> {expected['bonus']}点",
                 f"总计\n> {expected['total']}点", "【最近荣誉结算】"]
    if not all(text.count(line) == 1 for line in lines):
        return {"outcome": "unclassified", "reason": "arena_training_exact_reply_mismatch"}
    if key.startswith("training_"):
        offsets = [text.index(line) for line in lines[:50]]
        if offsets != sorted(offsets):
            return {"outcome": "unclassified", "reason": "training_public_order_mismatch"}
    return {"outcome": "success", "reason": "exact_arena_training_reply"}


def check_invariants(context, managers, manifest, results):
    checked = player.check_invariants(context, managers, manifest, results)
    errors = list(checked["errors"])
    if _snapshot(context.paths) != manifest["arena_training"]["snapshot"]:
        errors.append("arena/training states, operations or honor receipts changed")
    counts = Counter(row.get("request", row)["key"] for row in results)
    for row in results:
        request = row.get("request", row)
        if request["key"] in {item[0] for item in COMMANDS}:
            if row.get("outcome") != "success" or classify_result(request, row)["outcome"] != "success":
                errors.append(f"arena/training exact reply failed: {request['key']}")
    if results:
        pure = results[0].get("request", results[0])["phase_label"] == "pure"
        expected = {key: 18 if pure else 10 for key, *_ in COMMANDS}
        expected.update(profile_info=56, stone_training=0 if pure else 32)
        if counts != +Counter(expected) or len(results) != 128:
            errors.append("arena/training frozen command counts changed")
    return {**checked, "ok": not errors, "errors": errors,
            "arena_training_checks": {"per_command": dict(counts), "rows": POPULATION,
                "oracle": "setup Python integer sort, rowid ties, competition training/TOP1000 ordinal arena; zero traffic oracle SQL"},
            "invariants": checked["invariants"] + "; full arena/training/operation hashes and honor receipts unchanged; exact target replies"}
