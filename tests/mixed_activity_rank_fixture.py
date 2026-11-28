"""Activity rank traffic layered onto the retained 1M player fixture."""
from __future__ import annotations

from collections import Counter
import hashlib
import json

from tests import mixed_command_fixture as shared
from tests import mixed_player_projection_fixture as player


PARTICIPANTS = player.PARTICIPANTS
ACTIVITY_COMMANDS = (
    ("activity_sign_rank", "活动排行", "活动排行 签到榜", "sign"),
    ("activity_boss_board", "活动排行", "活动排行 首领榜", "boss"),
    ("activity_boss_display", "活动首领排行", "活动首领排行", "boss_display"),
)
BOARD_LIMIT = 10
BOSS_DISPLAY_LIMIT = 15
ACTIVITY_PER_COMMAND_PER_PHASE = 12
REWARD_TIERS = (
    (1, 10, "排行前十"),
    (11, 128, "排行前128"),
    (129, 8192, "活动参与奖励"),
)


def phase_specs():
    return [dict(row) for row in player.phase_specs()]


def _digest(rows) -> str:
    payload = json.dumps(rows, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


def _display_name(row) -> str:
    user_id = str(row.get("user_id") or "")
    name = str(row.get("user_name") or "").strip()
    if name:
        return name
    return f"修士·{user_id[-4:]}" if len(user_id) > 6 else user_id or "无名修士"


def _activity_projection(config):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_rules import get_gameplay_activities

    projected = json.loads(json.dumps(config))
    extensions = projected.setdefault("extensions", {})
    if not isinstance(extensions, dict):
        raise ValueError("activity extensions projection must be an object")
    extensions["rank"] = {
        "boards": [
            {"key": "sign", "label": "签到榜", "source": "sign", "limit": BOARD_LIMIT},
            {"key": "boss", "label": "首领榜", "source": "boss", "limit": BOARD_LIMIT},
        ],
        "rewards": [
            {"board": board, "rank_min": low, "rank_max": high,
             "name": name, "reward": "灵石x1"}
            for board in ("sign", "boss")
            for low, high, name in REWARD_TIERS
        ],
    }
    bosses = [row for row in get_gameplay_activities(projected)
              if row.get("type") == "activity_boss"]
    if len(bosses) != 1 or not str(bosses[0].get("key") or "").strip():
        raise ValueError("activity rank fixture requires exactly one active boss key")
    return projected, str(bosses[0]["key"])


def _ranked_rows(paths, activity_key):
    with shared._uow(paths.game_db, read_only=True) as uow:
        sign = uow.query_all(
            "SELECT a.user_id,a.sign_days,a.total_sign_days,a.last_sign_date,"
            "u.user_name FROM activity_user a LEFT JOIN user_xiuxian u "
            "ON u.user_id=a.user_id WHERE a.sign_days>0"
        )
        boss = uow.query_all(
            "SELECT d.user_id,d.total_damage,u.user_name FROM activity_boss_damage d "
            "LEFT JOIN user_xiuxian u ON u.user_id=d.user_id "
            "WHERE d.activity_key=? AND d.total_damage>0",
            (activity_key,),
        )
    if len(sign) < 8192 or len(boss) < 8192:
        raise ValueError("activity rank fixture needs at least 8192 populated rows per selected board")
    sign.sort(key=lambda row: (-int(row["sign_days"] or 0),
                               -int(row["total_sign_days"] or 0),
                               str(row.get("last_sign_date") or ""),
                               str(row["user_id"])))
    boss.sort(key=lambda row: (-int(row["total_damage"] or 0), str(row["user_id"])))
    return sign, boss


def _state_digest(paths, activity_key):
    with shared._uow(paths.game_db, read_only=True) as uow:
        sign = uow.query_all(
            "SELECT user_id,sign_days,total_sign_days,last_sign_date FROM activity_user "
            "ORDER BY user_id"
        )
        boss = uow.query_all(
            "SELECT activity_key,user_id,total_damage FROM activity_boss_damage "
            "WHERE activity_key=? ORDER BY user_id",
            (activity_key,),
        )
    return {"sign_rows": len(sign), "sign_sha256": _digest(sign),
            "boss_rows": len(boss), "boss_sha256": _digest(boss)}


def _oracle(paths, actors, activity_key):
    sign, boss = _ranked_rows(paths, activity_key)
    actor_set = set(map(str, actors))
    sign_ranks = {str(row["user_id"]): index for index, row in enumerate(sign, 1)}
    boss_ranks = {str(row["user_id"]): index for index, row in enumerate(boss, 1)}
    sign_values = {str(row["user_id"]): int(row["sign_days"] or 0) for row in sign}
    boss_values = {str(row["user_id"]): int(row["total_damage"] or 0) for row in boss}
    if not actor_set <= set(sign_ranks) or not actor_set <= set(boss_ranks):
        raise ValueError("all activity ranking request actors must have positive sign and boss rows")
    selected = {}
    for actor in actor_set:
        sign_rank, boss_rank = sign_ranks[actor], boss_ranks[actor]
        selected[actor] = {
            "sign_rank": sign_rank,
            "sign_value": sign_values[actor],
            "boss_rank": boss_rank,
            "boss_value": boss_values[actor],
        }
    return {
        "sign": [{"rank": index, "user_id": str(row["user_id"]),
                  "name": _display_name(row), "value": int(row["sign_days"] or 0)}
                 for index, row in enumerate(sign[:BOARD_LIMIT], 1)],
        "boss": [{"rank": index, "user_id": str(row["user_id"]),
                  "name": _display_name(row), "value": int(row["total_damage"] or 0)}
                 for index, row in enumerate(boss[:BOARD_LIMIT], 1)],
        "boss_display": [{"rank": index, "user_id": str(row["user_id"]),
                          "name": _display_name(row), "value": int(row["total_damage"] or 0)}
                         for index, row in enumerate(boss[:BOSS_DISPLAY_LIMIT], 1)],
        "actors": selected,
        "totals": {"sign": len(sign), "boss": len(boss)},
        "state_digest": _state_digest(paths, activity_key),
        "activity_key": activity_key,
    }


def seed(context, managers, *, participants=PARTICIPANTS):
    if participants != PARTICIPANTS:
        raise ValueError("activity rank traffic requires the existing 128 participant set")
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    manifest = player.seed(context, managers, participants=participants)
    config_path = activity_config.CONFIG_PATH.resolve(strict=True)
    if not config_path.is_relative_to(context.paths.game_db.parent.resolve()):
        raise ValueError("activity rank config must remain under the fixture restore root")
    projected, boss_key = _activity_projection(activity_config._load_config_projection_readonly())
    activity_config._save_config_projection(projected)
    activity_key = activity_config._activity_config_key(projected)
    # The relation fixture captured JSON before this isolated activity projection was written.
    manifest["json_snapshot"] = player.relation._json_snapshot(context.paths)
    oracle = _oracle(context.paths, manifest["actors"], boss_key)
    config_bytes = config_path.read_bytes()
    manifest.update(
        fixture_version="mixed-activity-rank-v12",
        activity_rank={"config_sha256": hashlib.sha256(config_bytes).hexdigest(),
                       "activity_key": activity_key, "boss_key": boss_key,
                       "oracle": oracle,
                       "limits": ["8192+ positive rows per selected board; bounded to existing feature rows",
                                  "activity rank data is read-only during traffic; real distinct-user cultivation writes remain active",
                                  "public rows are validated against the frozen fixture; personalized rank is exact and live"]},
    )
    manifest["feature_table_rows"].update(
        {"activity_user": oracle["state_digest"]["sign_rows"],
         "activity_boss_damage_selected_key": oracle["state_digest"]["boss_rows"]}
    )
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = player.strata.gameplay._digest(manifest)
    process_read_cache.clear()
    return manifest


def _tier(rank):
    return next((name for low, high, name in REWARD_TIERS if low <= rank <= high), None)


def _request_for(key, command, input_text, actor, manifest):
    oracle = manifest["activity_rank"]["oracle"]
    state = oracle["actors"][actor]
    if key == "activity_boss_display":
        return {"key": key, "command": command, "input_text": input_text, "actor": actor,
                "expected_activity": {"top": oracle["boss_display"]}}
    if key == "activity_sign_rank":
        board, rank_key, value_key = "sign", "sign_rank", "sign_value"
    else:
        board, rank_key, value_key = "boss", "boss_rank", "boss_value"
    return {"key": key, "command": command, "input_text": input_text, "actor": actor,
            "expected_activity": {"board": board, "rank": state[rank_key],
                                  "value": state[value_key], "total": oracle["totals"][board],
                                  "tier": _tier(state[rank_key]),
                                  "top": oracle[board if board == "sign" else "boss"]}}


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs) or mode != specs[phase_number]["mode"]
            or count != specs[phase_number]["requests"]):
        raise ValueError("invalid frozen activity rank plan")
    plan = player.request_plan(manifest, mode, count, phase_number)
    replaceable = [index for index, row in enumerate(plan)
                   if row["key"] in {"my_mentor", "my_partner"}]
    required = ACTIVITY_PER_COMMAND_PER_PHASE * len(ACTIVITY_COMMANDS)
    if len(replaceable) < required:
        raise ValueError("activity plan cannot retain the frozen write/profile coverage")
    actors = list(manifest["groups"]["training_low"])
    scores = manifest.get("score_evidence", {})
    if len(actors) < ACTIVITY_PER_COMMAND_PER_PHASE * len(ACTIVITY_COMMANDS):
        raise ValueError("activity rank traffic needs enough exact-integer profile actors")
    if any(any(scores.get(actor, {}).get(field, {}).get("storage") != "integer"
                   for field in ("exp", "stone")) for actor in actors):
        raise ValueError("activity rank profile actors must have exact integer exp and stone")
    route_rows = []
    for key, command, input_text, _board in ACTIVITY_COMMANDS:
        route_rows.extend((key, command, input_text, index)
                          for index in range(ACTIVITY_PER_COMMAND_PER_PHASE))
    for ordinal, (index, (key, command, input_text, _board)) in enumerate(zip(replaceable, route_rows)):
        actor = actors[(phase_number * required + ordinal * 17) % len(actors)]
        plan[index] = {**_request_for(key, command, input_text, actor, manifest),
                       "number": plan[index]["number"], "event_type": "group",
                       "phase_label": mode}
    return plan


def _ordered_contains(text, lines):
    offset = 0
    for line in lines:
        found = text.find(line, offset)
        if found < 0:
            return False
        offset = found + len(line)
    return True


def classify_result(request, result):
    key = request["key"]
    if key not in {item[0] for item in ACTIVITY_COMMANDS}:
        return player.classify_result(request, result)
    base = player.classify_result(request, result)
    if base["outcome"] in {"boundary", "timeout", "exception", "cooldown", "business_rejection"}:
        return base
    text = "\n".join(map(str, result.get("replies", ()))).replace("\r\n", "\n").replace("\r", "\n")
    expected = request["expected_activity"]
    from nonebot_plugin_xiuxian_2.features.activity.rank import position_text, value_text
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to

    if key == "activity_boss_display":
        oracle_rows = expected.get("top")
        # The public-only route carries its exact top rows explicitly, not a personal rank.
        if oracle_rows is None:
            return {"outcome": "unclassified", "reason": "missing_activity_display_oracle"}
        lines = [f"{row['rank']}. {row['name']} 伤害 {number_to(row['value'])}"
                 for row in oracle_rows]
    else:
        suffix = "天" if expected["board"] == "sign" else "点"
        position = (f"我的名次：{position_text(expected['rank'], expected['total'])}"
                    f"（{value_text(expected['board'], expected['value'])}）")
        lines = [f"{row['rank']}. {row['name']} "
                 f"{value_text(expected['board'], row['value'])}"
                 for row in expected["top"]]
        if position not in text:
            return {"outcome": "unclassified", "reason": "activity_exact_rank_or_value_mismatch"}
        if expected["tier"]:
            if f"可领取：{expected['tier']}（灵石x1）" not in text:
                return {"outcome": "unclassified", "reason": "activity_live_reward_tier_mismatch"}
        elif "当前名次没有可领取的排行奖励" not in text:
            return {"outcome": "unclassified", "reason": "activity_live_reward_eligibility_mismatch"}
    if not _ordered_contains(text, lines):
        return {"outcome": "unclassified", "reason": "activity_public_rank_rows_mismatch"}
    if "公共榜单快照截至" not in text:
        return {"outcome": "unclassified", "reason": "activity_display_freshness_missing"}
    return {"outcome": "success", "reason": "exact_live_activity_rank_and_snapshot_display"}


def check_invariants(context, managers, manifest, results):
    checked = player.check_invariants(context, managers, manifest, results)
    errors = list(checked["errors"])
    activity = manifest["activity_rank"]
    actual_state = _state_digest(context.paths, activity["boss_key"])
    if actual_state != activity["oracle"]["state_digest"]:
        errors.append("activity rank feature rows changed during read/training traffic")
    expected_counts = Counter({key: ACTIVITY_PER_COMMAND_PER_PHASE
                               for key, _command, _input_text, _board in ACTIVITY_COMMANDS})
    counts = Counter()
    for row in results:
        request = row.get("request", row)
        if request.get("key") in expected_counts:
            counts[request["key"]] += 1
            classification = classify_result(request, row)
            if row.get("outcome") != "success" or classification["outcome"] != "success":
                errors.append(f"activity rank request {request.get('number')} failed oracle validation")
    if results and counts != expected_counts:
        errors.append(f"activity route sample counts differ: {dict(counts)}")
    return {**checked, "ok": not errors, "errors": errors,
            "activity_rank_checks": {"per_command": dict(counts),
                                     "feature_rows_unchanged": actual_state == activity["oracle"]["state_digest"],
                                     "rank_oracle": "setup-time full ordered rows; no traffic oracle SQL"},
            "invariants": checked["invariants"] + "; exact sign/boss standing oracle and activity read-only table hashes"}
