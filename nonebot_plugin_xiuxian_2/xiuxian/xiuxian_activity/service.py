import random
import time
from copy import deepcopy

from nonebot.log import logger

from ...paths import get_paths
from ..xiuxian_compensation.common import get_item_list, send_reward_to_user
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.activity_helpers import as_bool as _as_bool
from .activity_config import (
    CONFIG_PATH,
    DEFAULT_ACTIVITY_PASS,
    DEFAULT_ACTIVITY_STAGES,
    STAGE_TYPE_LABELS,
    _activity_config_key,
    _activity_info_mode,
    _get_extensions,
    _sign_reply_mode,
    activity_runtime_state,
    activity_state,
    load_config,
    save_config,
)
from .activity_rules import get_gameplay_activities
from .activity_storage import (
    DB_PATH,
    DEFAULT_COLLECT_DROP_EVENTS,
    DEFAULT_POINT_EVENT_RULES,
    _sql_message,
    ensure_activity_files,
    now_str,
    resolve_daohao,
    today_str,
)
from .activity_utils import (
    _as_float,
    _as_int,
    _clean_text,
    _drop_rate,
)
from .activity_views import (
    ACTIVITY_EVENT_CHOICES,
    ACTIVITY_EVENT_LABELS,
    STAGE_FEATURES,
    _activity_event_text,
    _format_activity_task,
    _format_reward_result,
    _scope_label,
    _stage_feature_text,
    _stage_time_text,
    _task_status_text,
)

from .activity_storage import *
from .activity_rules import *
from .activity_pass import *
from .activity_progress import *
from .point_shop_service import ActivityPointShopPurchaseService
from .task_claim_service import ActivityTaskClaimService
from .sign_settlement_service import ActivitySignSettlementService
from .pass_claim_service import ActivityPassClaimService
from .collect_exchange_service import ActivityCollectExchangeService
from .claim_all_service import ActivityClaimAllService


point_shop_purchase_service = ActivityPointShopPurchaseService(DB_PATH, get_paths().game_db)
activity_task_claim_service = ActivityTaskClaimService(DB_PATH, get_paths().game_db)
activity_sign_settlement_service = ActivitySignSettlementService(DB_PATH, get_paths().game_db)
activity_pass_claim_service = ActivityPassClaimService(DB_PATH, get_paths().game_db)
activity_collect_exchange_service = ActivityCollectExchangeService(DB_PATH, get_paths().game_db)
activity_claim_all_service = ActivityClaimAllService(DB_PATH)


def _reward_by_day(config: dict, day_index: int) -> dict:
    rewards = config.get("daily_rewards") or []
    if not rewards:
        return {}

    for reward in rewards:
        if _as_int(reward.get("day")) == day_index:
            return reward

    repeat_last = bool(_get_extensions(config).get("repeat_last_daily_reward", True))
    if repeat_last:
        ordered = sorted(rewards, key=lambda item: _as_int(item.get("day")))
        return ordered[-1] if ordered else {}
    return {}


def _milestone_by_days(config: dict, sign_days: int) -> dict:
    for reward in config.get("milestone_rewards") or []:
        if _as_int(reward.get("days")) == sign_days:
            return reward
    return {}


def get_user_sign(user_id: str) -> dict:
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM activity_user WHERE user_id=%s", (str(user_id),))
        row = cur.fetchone()
        if row:
            data = dict(row)
            data["sign_days"] = _as_int(data.get("sign_days"))
            data["total_sign_days"] = _as_int(data.get("total_sign_days"), data["sign_days"])
            return data
        return {
            "user_id": str(user_id),
            "sign_days": 0,
            "last_sign_date": "",
            "total_sign_days": 0,
        }
    finally:
        conn.close()


def parse_reward(reward: str) -> list[dict]:
    reward = str(reward or "").strip()
    if not reward:
        return []
    return get_item_list(reward)


def send_reward_items(user_id: str, reward_items: list[dict]) -> list[str]:
    if not reward_items:
        return []
    return send_reward_to_user(str(user_id), reward_items)


def _choose_collect_char(activity: dict) -> str:
    letters = _collect_letters(activity, _collect_phrases(activity))
    if not letters:
        return ""
    total_weight = sum(max(1, _as_int(item.get("weight"), 1)) for item in letters)
    needle = random.uniform(0, total_weight)
    current = 0
    for item in letters:
        current += max(1, _as_int(item.get("weight"), 1))
        if needle <= current:
            return _clean_text(item.get("char"))
    return _clean_text(letters[-1].get("char"))


def _record_activity_task_progress(
    cur,
    config: dict,
    user_id: str,
    event_key: str,
    amount: int,
    messages: list[str],
) -> None:
    runtime = activity_runtime_state(config)
    if not runtime.get("ok") or "task" not in set(runtime.get("features") or []):
        return
    activity_key = _activity_config_key(config)
    for task in get_activity_tasks(config):
        if event_key not in task.get("events", []):
            continue
        scope_type = task["scope_type"]
        scope_key = _task_scope_key(scope_type)
        task_key = task["key"]
        target = max(1, _as_int(task.get("target"), 1))
        cur.execute(
            """
            SELECT progress, claimed FROM activity_task_progress
            WHERE activity_key=%s AND user_id=%s AND scope_type=%s AND scope_key=%s AND task_key=%s
            """,
            (activity_key, user_id, scope_type, scope_key, task_key),
        )
        row = cur.fetchone()
        old_progress = max(0, _as_int(row["progress"] if row else 0))
        claimed = bool(_as_int(row["claimed"] if row else 0))
        if claimed:
            continue
        new_progress = min(target, old_progress + max(1, amount))
        if new_progress <= old_progress and row:
            continue
        ts = now_str()
        cur.execute(
            """
            INSERT INTO activity_task_progress (
                activity_key, user_id, scope_type, scope_key, task_key,
                progress, target, claimed, update_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s)
            ON CONFLICT(activity_key, user_id, scope_type, scope_key, task_key) DO UPDATE SET
                progress = excluded.progress,
                target = excluded.target,
                update_time = excluded.update_time
            """,
            (activity_key, user_id, scope_type, scope_key, task_key, new_progress, target, ts),
        )
        if old_progress < target <= new_progress:
            messages.append(f"活动任务完成：{task['name']}，发送 活动任务领取")


def record_activity_event(user_id: str, event_key: str, amount: int = 1) -> list[str]:
    uid = str(user_id)
    event = str(event_key)
    times = max(0, _as_int(amount, 1))
    if times <= 0:
        return []

    cfg = load_config()
    activities = get_gameplay_activities(cfg)
    if not activities and not get_activity_tasks(cfg) and not _activity_pass_config(cfg).get("enabled"):
        return []
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok") or not runtime.get("can_produce"):
        return []
    features = set(runtime.get("features") or [])
    multiplier = max(0.0, _as_float(runtime.get("multiplier"), 1.0))

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    messages: list[str] = []
    try:
        cur = conn.cursor()
        _record_activity_task_progress(cur, cfg, uid, event, times, messages)
        _record_activity_pass_progress(cur, cfg, uid, event, times, messages)
        for activity in activities:
            ok, _ = activity_state(activity)
            if not ok:
                continue
            if activity.get("type") == "event_points":
                if "points" not in features:
                    continue
                for rule in activity.get("event_rules") or []:
                    if rule.get("event") != event:
                        continue
                    points = int(max(0, _as_int(rule.get("points"), 0)) * times * multiplier)
                    if points <= 0:
                        continue
                    daily_limit = max(0, _as_int(rule.get("daily_limit"), 0))
                    if daily_limit > 0:
                        cur.execute(
                            """
                            SELECT COALESCE(SUM(points), 0) AS count
                            FROM activity_point_event_log
                            WHERE activity_key=%s AND user_id=%s AND event_key=%s AND record_date=%s
                            """,
                            (activity["key"], uid, event, today_str()),
                        )
                        row = cur.fetchone()
                        current_points = _as_int(row["count"] if row else 0)
                        remaining = daily_limit - current_points
                        if remaining <= 0:
                            continue
                        points = min(points, remaining)
                    if points <= 0:
                        continue
                    ts = now_str()
                    cur.execute(
                        """
                        INSERT INTO activity_point_balance (
                            activity_key, user_id, points, total_points, update_time
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT(activity_key, user_id) DO UPDATE SET
                            points = activity_point_balance.points + excluded.points,
                            total_points = activity_point_balance.total_points + excluded.total_points,
                            update_time = excluded.update_time
                        """,
                        (activity["key"], uid, points, points, ts),
                    )
                    cur.execute(
                        """
                        INSERT INTO activity_point_event_log (
                            activity_key, user_id, event_key, points, record_date, create_time
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (activity["key"], uid, event, points, today_str(), ts),
                    )
                    messages.append(
                        f"活动积分：{activity['name']} 获得{points}{activity.get('point_name', '活动积分')}"
                    )
                continue

            if activity.get("type") == "activity_boss":
                if "boss" not in features:
                    continue
                drop_events = activity.get("drop_events") or []
                if event in drop_events:
                    items = activity.get("items") or []
                    if items:
                        pick = random.choice(items)
                        cur.execute(
                            """
                            INSERT INTO activity_item_inventory (activity_key, user_id, item_id, count, update_time)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT(activity_key, user_id, item_id) DO UPDATE SET
                                count = activity_item_inventory.count + excluded.count,
                                update_time = excluded.update_time
                            """,
                            (activity["key"], uid, pick["id"], 1, now_str()),
                        )
                        messages.append(f"活动掉落：{activity['name']} 获得【{pick['name']}】")
                continue

            if activity.get("type") != "collect_words" or event not in activity.get("drop_events", []):
                continue
            if "collect" not in features:
                continue
            daily_limit = max(0, _as_int(activity.get("daily_drop_limit"), 8))
            if daily_limit <= 0:
                continue
            cur.execute(
                """
                SELECT COUNT(*) AS count
                FROM activity_collect_drop_log
                WHERE activity_key=%s AND user_id=%s AND drop_date=%s
                """,
                (activity["key"], uid, today_str()),
            )
            row = cur.fetchone()
            current_count = _as_int(row["count"] if row else 0)
            remaining = daily_limit - current_count
            if remaining <= 0:
                continue

            rolls = min(remaining, max(1, min(times, _as_int(activity.get("rolls_per_record"), 1))))
            drop_rate = min(_drop_rate(activity.get("drop_rate"), 0.35) * multiplier, 1.0)
            pity_threshold = max(0, _as_int(activity.get("pity_threshold"), 0))
            pity_count = (
                _get_collect_pity_count(cur, activity["key"], uid, event)
                if pity_threshold > 0 else 0
            )
            pity_changed = False
            for _ in range(rolls):
                guaranteed = pity_threshold > 0 and pity_count + 1 >= pity_threshold
                if not guaranteed and random.random() > drop_rate:
                    if pity_threshold > 0:
                        pity_count += 1
                        pity_changed = True
                    continue
                word_char = _choose_collect_char(activity)
                if not word_char:
                    continue
                if pity_threshold > 0:
                    pity_count = 0
                    pity_changed = True
                cur.execute(
                    """
                    INSERT INTO activity_collect_inventory (
                        activity_key, user_id, word_char, count, update_time
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(activity_key, user_id, word_char) DO UPDATE SET
                        count = activity_collect_inventory.count + excluded.count,
                        update_time = excluded.update_time
                    """,
                    (activity["key"], uid, word_char, 1, now_str()),
                )
                cur.execute(
                    """
                    INSERT INTO activity_collect_drop_log (
                        activity_key, user_id, event_key, word_char, drop_date, create_time
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (activity["key"], uid, event, word_char, today_str(), now_str()),
                )
                drop_label = "活动保底" if guaranteed else "活动掉落"
                messages.append(f"{drop_label}：{activity['name']} 获得字牌「{word_char}」")
            if pity_changed:
                _set_collect_pity_count(cur, activity["key"], uid, event, pity_count)
        conn.commit()
        return messages
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_collect_inventory(user_id: str, activity_key: str) -> dict[str, int]:
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        return _get_collect_inventory_map(conn.cursor(), activity_key, str(user_id))
    finally:
        conn.close()


def build_collect_bag_text(user_id: str) -> str:
    uid = str(user_id)
    activities = get_gameplay_activities(load_config())
    collect_activities = [activity for activity in activities if activity.get("type") == "collect_words"]
    lines = ["【活动背包】"]
    if not collect_activities:
        lines.append("暂无集字活动")
        return "\n".join(lines)

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        for activity in collect_activities:
            ok, reason = activity_state(activity)
            inventory = _get_collect_inventory_map(cur, activity["key"], uid)
            claims = _get_collect_claim_map(cur, activity["key"], uid)
            letters = _collect_letters(activity, _collect_phrases(activity))
            letter_text = "、".join(
                f"{item['char']}x{inventory.get(item['char'], 0)}"
                for item in letters
            ) or "暂无"
            lines.extend([
                "",
                f"【{activity['name']}】{'进行中' if ok else reason}",
                f"字牌：{letter_text}",
            ])
            pity_threshold = max(0, _as_int(activity.get("pity_threshold"), 0))
            if pity_threshold > 0:
                pity_map = _collect_pity_progress_map(cur, activity["key"], uid)
                pity_parts = []
                for event_key in activity.get("drop_events") or []:
                    label = ACTIVITY_EVENT_LABELS.get(event_key, event_key)
                    pity_parts.append(
                        f"{label} {min(pity_threshold, pity_map.get(event_key, 0))}/{pity_threshold}"
                    )
                if pity_parts:
                    lines.append("保底进度：" + "、".join(pity_parts))
            phrases = _collect_phrases(activity)
            if not phrases:
                lines.append("暂无兑换词组")
                continue
            lines.append("可兑换词组：")
            for phrase in phrases:
                need = _phrase_need_counter(phrase["phrase"])
                owned = sum(min(inventory.get(word_char, 0), count) for word_char, count in need.items())
                total_need = sum(need.values())
                claimed = claims.get(phrase["phrase"], 0)
                limit = _as_int(phrase.get("limit"), 1)
                limit_text = "不限" if limit <= 0 else f"{claimed}/{limit}"
                lines.append(
                    f"- {phrase['name']}：{owned}/{total_need}，已兑换 {limit_text}，"
                    f"兑换：活动兑换 {phrase['name']}"
                )
        return "\n".join(lines).strip()
    finally:
        conn.close()


def _find_collect_phrase(config: dict, query: str) -> tuple[dict, dict] | None:
    text = _clean_text(query)
    if not text:
        return None
    for activity in get_gameplay_activities(config):
        if activity.get("type") != "collect_words":
            continue
        ok, _ = activity_state(activity)
        if not ok:
            continue
        for phrase in _collect_phrases(activity):
            phrase_text = _clean_text(phrase.get("phrase"))
            phrase_name = _clean_text(phrase.get("name"))
            if text in (phrase_text, phrase_name):
                return activity, phrase
            if phrase_text and phrase_text in text:
                return activity, phrase
            if phrase_name and phrase_name in text:
                return activity, phrase
    return None


def claim_collect_phrase(user_id: str, query: str, operation_id: str | None = None) -> tuple[bool, str]:
    uid = str(user_id)
    target = _clean_text(query)
    if not target:
        return False, "请发送：活动兑换 端午安康"

    cfg = load_config()
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放"
    if "exchange" not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放集字兑换"
    found = _find_collect_phrase(cfg, target)
    if not found:
        return False, "未找到可兑换的活动词组，或活动当前不可兑换"

    activity, phrase = found
    need = _phrase_need_counter(phrase["phrase"])
    if not need:
        return False, "兑换词组配置错误"
    try:
        reward_items = parse_reward(phrase.get("reward") or "")
    except Exception as e:
        return False, f"兑换奖励配置错误：{e}"

    ensure_activity_files()
    result = activity_collect_exchange_service.exchange(
        operation_id or f"activity-exchange:{uid}:{time.time_ns()}", uid, activity["key"],
        phrase["phrase"], need, _as_int(phrase.get("limit"), 1), reward_items,
        XiuConfig().max_goods_num,
    )
    if not result.succeeded:
        if result.status == "tokens_insufficient":
            return False, "字牌不足，还缺：" + "、".join(f"{char}x{count}" for char, count in result.missing)
        return False, {"limit_reached":"该词组已达到兑换次数上限","inventory_full":"背包空间不足，奖励未领取","user_missing":"角色不存在","operation_conflict":"兑换请求冲突，请重新发送"}.get(result.status,"兑换状态已变化，请重试")
    reward_text = "，".join(result.rewards)
    if reward_text:
        return True, f"{activity['name']}兑换成功：{phrase['name']}\n{reward_text}"
    return True, f"{activity['name']}兑换成功：{phrase['name']}"


def _get_point_balance(cur, activity_key: str, user_id: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT points, total_points
        FROM activity_point_balance
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    row = cur.fetchone()
    if not row:
        return {"points": 0, "total_points": 0}
    return {
        "points": max(0, _as_int(row["points"])),
        "total_points": max(0, _as_int(row["total_points"])),
    }


def _get_point_purchase_map(cur, activity_key: str, user_id: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT item_key, count
        FROM activity_point_purchase
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    return {
        str(row["item_key"]): max(0, _as_int(row["count"]))
        for row in cur.fetchall()
    }


def _get_point_shop_total_purchase(cur, activity_key: str, item_key: str) -> int:
    cur.execute(
        """
        SELECT COALESCE(SUM(count), 0) AS count
        FROM activity_point_purchase
        WHERE activity_key=%s AND item_key=%s
        """,
        (str(activity_key), str(item_key)),
    )
    row = cur.fetchone()
    return max(0, _as_int(row["count"] if row else 0))


def build_activity_points_text(user_id: str) -> str:
    uid = str(user_id)
    activities = [
        activity
        for activity in get_gameplay_activities(load_config())
        if activity.get("type") == "event_points"
    ]
    lines = ["【活动积分】"]
    if not activities:
        lines.append("暂无积分活动")
        return "\n".join(lines)

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        for activity in activities:
            ok, reason = activity_state(activity)
            balance = _get_point_balance(cur, activity["key"], uid)
            point_name = activity.get("point_name") or "活动积分"
            lines.extend([
                "",
                f"【{activity['name']}】{'进行中' if ok else reason}",
                f"当前{point_name}：{balance['points']}，累计获得：{balance['total_points']}",
            ])
            rules = activity.get("event_rules") or []
            if rules:
                rule_text = "、".join(
                    f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+{_as_int(rule.get('points'))}"
                    for rule in rules
                )
                lines.append(f"积分来源：{rule_text}")
        return "\n".join(lines).strip()
    finally:
        conn.close()


def build_activity_shop_text(user_id: str) -> str:
    uid = str(user_id)
    activities = [
        activity
        for activity in get_gameplay_activities(load_config())
        if activity.get("type") == "event_points"
    ]
    lines = ["【活动商店】"]
    if not activities:
        lines.append("暂无积分商店")
        return "\n".join(lines)

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        for activity in activities:
            ok, reason = activity_state(activity)
            point_name = activity.get("point_name") or "活动积分"
            balance = _get_point_balance(cur, activity["key"], uid)
            purchases = _get_point_purchase_map(cur, activity["key"], uid)
            lines.extend([
                "",
                f"【{activity['name']}】{'进行中' if ok else reason}",
                f"当前{point_name}：{balance['points']}",
            ])
            shop = activity.get("shop") or []
            if not shop:
                lines.append("暂无商店商品")
                continue
            for item in shop:
                item_key = str(item.get("item_key") or "")
                bought = purchases.get(item_key, 0)
                limit = _as_int(item.get("limit"), 1)
                limit_text = "不限" if limit <= 0 else f"{bought}/{limit}"
                stock_limit = _as_int(item.get("stock_limit"), 0)
                stock_text = ""
                if stock_limit > 0:
                    sold = _get_point_shop_total_purchase(cur, activity["key"], item_key)
                    stock_text = f"，全服库存 {sold}/{stock_limit}"
                lines.append(
                    f"- {item.get('name') or item_key}：{_as_int(item.get('cost'))}{point_name}，"
                    f"已兑换 {limit_text}{stock_text}，奖励：{item.get('reward') or '暂无奖励'}"
                )
        return "\n".join(lines).strip()
    finally:
        conn.close()


def build_activity_task_progress_text(user_id: str) -> str:
    cfg = load_config()
    activity_key = _activity_config_key(cfg)
    tasks = get_activity_tasks(cfg)
    lines = [f"【{cfg.get('name', '节日签到活动')} · 活动任务】"]
    if not tasks:
        lines.append("暂无活动任务")
        return "\n".join(lines)

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        progress_map = _get_task_progress_map(cur, activity_key, str(user_id))
        for scope_type in ("daily", "weekly"):
            scope_tasks = [task for task in tasks if task.get("scope_type") == scope_type]
            if not scope_tasks:
                continue
            scope_key = _task_scope_key(scope_type)
            lines.append("")
            lines.append(f"【{_scope_label(scope_type)}目标】")
            for task in scope_tasks:
                key = (scope_type, scope_key, task["key"])
                state = progress_map.get(key, {})
                target = max(1, _as_int(task.get("target"), 1))
                progress = min(target, max(0, _as_int(state.get("progress"), 0)))
                claimed = bool(state.get("claimed"))
                event_text = _activity_event_text(task.get("events"))
                desc = _clean_text(task.get("description"))
                if not desc:
                    desc = f"{event_text} {target} 次" if event_text else f"目标 {target}"
                lines.append(
                    f"- {task['name']}：{_task_status_text(progress, target, claimed)}，"
                    f"{desc}，奖励：{task.get('reward') or '暂无奖励'}"
                )
        lines.append("")
        lines.append("领奖：活动任务领取（自动领取全部可领任务）")
        return "\n".join(lines).strip()
    finally:
        conn.close()


def _select_claimable_tasks(cur, config: dict, user_id: str, query: str = "") -> list[tuple[dict, str, str, int]]:
    activity_key = _activity_config_key(config)
    target_text = _clean_text(query)
    tasks = get_activity_tasks(config)
    selected: list[tuple[dict, str, str, int]] = []
    for task in tasks:
        scope_type = task["scope_type"]
        scope_key = _task_scope_key(scope_type)
        if target_text and target_text not in {
            task["key"],
            task["name"],
            scope_type,
            _scope_label(scope_type),
        } and target_text not in task["name"]:
            continue
        cur.execute(
            """
            SELECT progress, target, claimed
            FROM activity_task_progress
            WHERE activity_key=%s AND user_id=%s AND scope_type=%s AND scope_key=%s AND task_key=%s
            """,
            (activity_key, user_id, scope_type, scope_key, task["key"]),
        )
        row = cur.fetchone()
        progress = max(0, _as_int(row["progress"] if row else 0))
        target = max(1, _as_int(row["target"] if row else task.get("target"), task.get("target", 1)))
        claimed = bool(_as_int(row["claimed"] if row else 0))
        if claimed or progress < target:
            continue
        selected.append((task, scope_type, scope_key, target))
    return selected


def claim_activity_tasks(user_id: str, query: str = "", operation_id: str | None = None) -> tuple[bool, str]:
    uid = str(user_id)
    if operation_id:
        previous = activity_task_claim_service.get_result(operation_id, uid)
        if previous is not None:
            if not previous.succeeded:
                return False, "领取请求冲突，请重新发送"
            lines = ["活动任务奖励领取成功："]
            for name, reward_text in previous.rewards:
                lines.append(f"- {name}：{reward_text or '暂无奖励'}")
            return True, "\n".join(lines)
    cfg = load_config()
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放"
    if "claim" not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放活动领奖"
    activity_key = _activity_config_key(cfg)
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        claimable = _select_claimable_tasks(cur, cfg, uid, query)
        if not claimable:
            return False, "当前没有可领取的活动任务奖励"
        tasks = []
        for task, scope_type, scope_key, target in claimable:
            reward_text = _clean_text(task.get("reward"))
            try:
                reward_items = parse_reward(reward_text)
            except Exception as e:
                return False, f"任务【{task['name']}】奖励配置错误：{e}"
            tasks.append((task["key"], scope_type, scope_key, target, reward_text, reward_items, task["name"]))
    finally:
        conn.close()

    result = activity_task_claim_service.claim(
        operation_id or f"activity-task:{uid}:{time.time_ns()}", uid, activity_key, tasks, XiuConfig().max_goods_num
    )
    if not result.succeeded:
        messages = {
            "inventory_full": "背包空间不足，奖励未领取",
            "user_missing": "角色不存在",
            "state_changed": "任务状态已变化，请重新查询",
            "operation_conflict": "领取请求冲突，请重新发送",
        }
        return False, messages.get(result.status, "当前没有可领取的活动任务奖励")
    lines = ["活动任务奖励领取成功："]
    for name, reward_text in result.rewards:
        lines.append(f"- {name}：{reward_text or '暂无奖励'}")
    return True, "\n".join(lines)


def build_activity_pass_text(user_id: str) -> str:
    cfg = load_config()
    pass_cfg = _activity_pass_config(cfg)
    lines = [f"【{pass_cfg['name']}】"]
    if not pass_cfg.get("enabled"):
        lines.append("活动战令未开启")
        return "\n".join(lines)
    activity_key = _activity_config_key(cfg)
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        balance = _get_pass_balance(cur, activity_key, str(user_id), pass_cfg)
        cur.execute(
            """
            SELECT level FROM activity_pass_reward_claim
            WHERE activity_key=%s AND user_id=%s
            """,
            (activity_key, str(user_id)),
        )
        claimed = {max(0, _as_int(row["level"])) for row in cur.fetchall()}
        exp_name = pass_cfg.get("exp_name") or "活跃值"
        lines.extend([
            f"等级：{balance['level']}/{balance['max_level']}",
            f"{exp_name}：{balance['exp']}/{balance['level_exp']}（累计 {balance['total_exp']}）",
        ])
        catchup = _pass_catchup_state(cur, cfg, activity_key, str(user_id), pass_cfg)
        if catchup.get("enabled"):
            if catchup.get("active"):
                lines.append(
                    f"追赶加成：已触发 {catchup['catchup_multiplier']:.2f}x，"
                    f"当前落后最高等级 {catchup['gap']} 级"
                )
            else:
                lines.append(
                    f"追赶加成：第{catchup['start_day']}天后、落后{catchup['level_gap']}级时触发"
                )
        rules = pass_cfg.get("event_rules") or []
        if rules:
            rule_text = "、".join(
                f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+{_as_int(rule.get('exp'))}"
                for rule in rules
            )
            lines.append(f"获取来源：{rule_text}")
        rewards = pass_cfg.get("level_rewards") or []
        if rewards:
            lines.append("")
            lines.append("【等级奖励】")
            for reward in rewards:
                level = _as_int(reward.get("level"), 0)
                if level <= 0:
                    continue
                if level in claimed:
                    status = "已领取"
                elif balance["level"] >= level:
                    status = "可领取"
                else:
                    status = "未达成"
                lines.append(
                    f"- Lv.{level} {reward.get('name') or '等级奖励'}：{status}，"
                    f"{reward.get('reward') or '暂无奖励'}"
                )
        lines.append("")
        lines.append("领奖：活动战令领取（自动领取全部可领等级奖励）")
        return "\n".join(lines).strip()
    finally:
        conn.close()


def claim_activity_pass_rewards(user_id: str, query: str = "", operation_id: str | None = None) -> tuple[bool, str]:
    uid = str(user_id)
    if operation_id:
        previous = activity_pass_claim_service.get_result(operation_id, uid)
        if previous is not None:
            if not previous.succeeded:
                return False, "领取请求冲突，请重新发送"
            lines = ["活动战令奖励领取成功："]
            for level, name, reward_text in previous.rewards:
                lines.append(f"- Lv.{level} {name}：{reward_text or '暂无奖励'}")
            return True, "\n".join(lines)
    cfg = load_config()
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放"
    if "claim" not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放活动领奖"
    pass_cfg = _activity_pass_config(cfg)
    if not pass_cfg.get("enabled"):
        return False, "活动战令未开启"
    activity_key = _activity_config_key(cfg)
    target_text = _clean_text(query)
    target_level = _as_int(target_text, 0) if target_text.isdigit() else 0
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    reward_jobs: list[tuple[dict, list[dict]]] = []
    try:
        cur = conn.cursor()
        balance = _get_pass_balance(cur, activity_key, uid, pass_cfg)
        cur.execute(
            """
            SELECT level FROM activity_pass_reward_claim
            WHERE activity_key=%s AND user_id=%s
            """,
            (activity_key, uid),
        )
        claimed = {max(0, _as_int(row["level"])) for row in cur.fetchall()}
        for reward in pass_cfg.get("level_rewards") or []:
            level = _as_int(reward.get("level"), 0)
            if level <= 0 or level in claimed or balance["level"] < level:
                continue
            if target_level and target_level != level:
                continue
            if target_text and not target_level and target_text not in {
                _clean_text(reward.get("name")),
                f"Lv.{level}",
                f"lv.{level}",
                str(level),
            } and target_text not in _clean_text(reward.get("name")):
                continue
            reward_text = _clean_text(reward.get("reward"))
            try:
                reward_items = parse_reward(reward_text)
            except Exception as e:
                conn.rollback()
                return False, f"战令Lv.{level}奖励配置错误：{e}"
            reward_jobs.append({"level": level, "name": reward.get("name"), "reward": reward_text, "reward_items": reward_items})
        if not reward_jobs:
            return False, "当前没有可领取的活动战令奖励"
    finally:
        conn.close()

    result = activity_pass_claim_service.claim(
        operation_id or f"activity-pass:{uid}:{time.time_ns()}", uid, activity_key,
        balance["level"], reward_jobs, XiuConfig().max_goods_num,
    )
    if not result.succeeded:
        return False, {"inventory_full":"背包空间不足，奖励未领取","user_missing":"角色不存在","state_changed":"战令状态已变化，请重新查询","operation_conflict":"领取请求冲突，请重新发送"}.get(result.status,"活动战令奖励已领取")
    lines = ["活动战令奖励领取成功："]
    for level, name, reward_text in result.rewards:
        lines.append(f"- Lv.{level} {name}：{reward_text or '暂无奖励'}")
    return True, "\n".join(lines)


def claim_activity_rewards(user_id: str, operation_id: str | None = None) -> tuple[bool, str]:
    uid = str(user_id)
    operation_id = operation_id or f"activity:claim-all:{uid}:{time.time_ns()}"
    from .activity_boss import claim_boss_milestone_reward, claim_boss_rank_reward

    result = activity_claim_all_service.run(
        operation_id,
        uid,
        {
            "tasks": lambda child_id: claim_activity_tasks(uid, operation_id=child_id),
            "pass": lambda child_id: claim_activity_pass_rewards(uid, operation_id=child_id),
            "boss_milestone": lambda child_id: claim_boss_milestone_reward(
                uid, operation_id=child_id
            ),
            "boss_rank": lambda child_id: claim_boss_rank_reward(uid, operation_id=child_id),
        },
    )
    if result.status == "retryable_failure":
        logger.warning(
            f"活动总领奖可重试失败 operation_id={operation_id} user_id={uid}: {result.text}"
        )
    return result.ok, result.text


def _parse_shop_query(query: str) -> tuple[str, int]:
    text = _clean_text(query)
    if not text:
        return "", 1
    parts = text.split()
    if len(parts) >= 2:
        try:
            quantity = int(parts[-1])
        except ValueError:
            return text, 1
        return " ".join(parts[:-1]).strip(), max(1, quantity)
    return text, 1


def _find_point_shop_item(config: dict, query: str) -> tuple[dict, dict] | None:
    text = _clean_text(query)
    if not text:
        return None
    fallback: tuple[dict, dict] | None = None
    for activity in get_gameplay_activities(config):
        if activity.get("type") != "event_points":
            continue
        ok, _ = activity_state(activity)
        if not ok:
            continue
        for item in activity.get("shop") or []:
            item_key = _clean_text(item.get("item_key"))
            item_name = _clean_text(item.get("name"))
            if text in (item_key, item_name):
                return activity, item
            if not fallback and ((item_key and text in item_key) or (item_name and text in item_name)):
                fallback = (activity, item)
    return fallback


def _multiply_reward_items(reward_items: list[dict], quantity: int) -> list[dict]:
    multiplier = max(1, _as_int(quantity, 1))
    items = []
    for item in reward_items:
        copied = dict(item)
        copied["quantity"] = max(1, _as_int(copied.get("quantity"), 1)) * multiplier
        items.append(copied)
    return items


def claim_point_shop_item(user_id: str, query: str, operation_id: str | None = None) -> tuple[bool, str]:
    uid = str(user_id)
    target, quantity = _parse_shop_query(query)
    if not target:
        return False, "请发送：活动购买 灵石补给"

    cfg = load_config()
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放"
    if "shop" not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放活动商店"
    found = _find_point_shop_item(cfg, target)
    if not found:
        return False, "未找到可兑换的活动商品，或活动当前不可兑换"

    activity, item = found
    cost = _as_int(item.get("cost"), 0)
    if cost <= 0:
        return False, "商品积分价格配置错误"
    reward_text = _clean_text(item.get("reward"))
    try:
        reward_items = _multiply_reward_items(parse_reward(reward_text), quantity)
    except Exception as e:
        return False, f"商品奖励配置错误：{e}"

    item_key = _clean_text(item.get("item_key"))
    point_name = activity.get("point_name") or "活动积分"
    total_cost = cost * quantity
    ensure_activity_files()
    operation_id = str(operation_id or f"activity-shop:{uid}:{activity['key']}:{item_key}:{now_str()}")
    result = point_shop_purchase_service.purchase(
        operation_id, uid, activity["key"], item_key, quantity, cost,
        _as_int(item.get("limit"), 1), _as_int(item.get("stock_limit"), 0),
        reward_items, XiuConfig().max_goods_num,
    )
    if result.status == "points_insufficient":
        return False, f"{point_name}不足，还缺 {max(total_cost - result.points, 0)}"
    if result.status == "personal_limit":
        return False, f"该商品兑换次数不足，当前已兑换 {result.personal_count}/{_as_int(item.get('limit'), 1)}"
    if result.status == "stock_insufficient":
        return False, f"该商品全服库存不足，当前已兑换 {result.total_count}/{_as_int(item.get('stock_limit'), 0)}"
    if result.status == "inventory_full":
        return False, "背包中该奖励物品已达持有上限"
    if result.status in {"operation_conflict", "state_changed", "user_missing"}:
        return False, "兑换状态已变化，请重新尝试"

    reward_msg = [
        f"获得灵石 {number_to(reward['quantity'])} 枚"
        if reward["type"] == "stone" else f"获得 {reward['name']} x{reward['quantity']}"
        for reward in reward_items
    ]

    reward_result = "，".join(reward_msg) if reward_msg else reward_text
    item_name = item.get("name") or item_key
    quantity_text = f"x{quantity}" if quantity > 1 else ""
    return True, f"{activity['name']}兑换成功：{item_name}{quantity_text}\n消耗：{total_cost}{point_name}\n奖励：{reward_result}"


def _fetch_count(cur, sql: str, params=()) -> int:
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row:
        return 0
    if isinstance(row, dict):
        return _as_int(next(iter(row.values()), 0))
    try:
        return _as_int(row[0])
    except Exception:
        return _as_int(row["count"] if "count" in row.keys() else 0)


def _activity_data_counts(cur, activity_key: str, activity_type: str) -> dict:
    if activity_type == "event_points":
        cur.execute(
            """
            SELECT
                COUNT(*) AS user_count,
                COALESCE(SUM(points), 0) AS current_points,
                COALESCE(SUM(total_points), 0) AS total_points
            FROM activity_point_balance
            WHERE activity_key=%s
            """,
            (activity_key,),
        )
        row = cur.fetchone()
        cur.execute(
            """
            SELECT COALESCE(SUM(count), 0) AS count
            FROM activity_point_purchase
            WHERE activity_key=%s
            """,
            (activity_key,),
        )
        purchase_row = cur.fetchone()
        return {
            "user_count": _as_int(row["user_count"] if row else 0),
            "current_points": _as_int(row["current_points"] if row else 0),
            "total_points": _as_int(row["total_points"] if row else 0),
            "purchase_count": _as_int(purchase_row["count"] if purchase_row else 0),
        }

    if activity_type == "activity_boss":
        cur.execute(
            """
            SELECT hp_left, max_hp
            FROM activity_boss_state
            WHERE activity_key=%s
            """,
            (activity_key,),
        )
        hp_row = cur.fetchone()
        cur.execute(
            """
            SELECT
                COUNT(*) AS user_count,
                COALESCE(SUM(total_damage), 0) AS total_damage
            FROM activity_boss_damage
            WHERE activity_key=%s
            """,
            (activity_key,),
        )
        damage_row = cur.fetchone()
        fight_count = _fetch_count(
            cur,
            "SELECT COUNT(*) AS count FROM activity_boss_fight_log WHERE activity_key=%s",
            (activity_key,),
        )
        milestone_count = _fetch_count(
            cur,
            "SELECT COUNT(*) AS count FROM activity_boss_milestone WHERE activity_key=%s",
            (activity_key,),
        )
        item_count = _fetch_count(
            cur,
            "SELECT COALESCE(SUM(count), 0) AS count FROM activity_item_inventory WHERE activity_key=%s",
            (activity_key,),
        )
        return {
            "user_count": _as_int(damage_row["user_count"] if damage_row else 0),
            "total_damage": _as_int(damage_row["total_damage"] if damage_row else 0),
            "fight_count": fight_count,
            "item_count": item_count,
            "milestone_count": milestone_count,
            "hp_left": _as_int(hp_row["hp_left"] if hp_row else 0),
            "max_hp": _as_int(hp_row["max_hp"] if hp_row else 0),
        }

    cur.execute(
        """
        SELECT
            COUNT(DISTINCT user_id) AS user_count,
            COALESCE(SUM(count), 0) AS inventory_count
        FROM activity_collect_inventory
        WHERE activity_key=%s
        """,
        (activity_key,),
    )
    inventory_row = cur.fetchone()
    drop_count = _fetch_count(
        cur,
        "SELECT COUNT(*) AS count FROM activity_collect_drop_log WHERE activity_key=%s",
        (activity_key,),
    )
    cur.execute(
        """
        SELECT COALESCE(SUM(count), 0) AS count
        FROM activity_collect_claim
        WHERE activity_key=%s
        """,
        (activity_key,),
    )
    claim_row = cur.fetchone()
    return {
        "user_count": _as_int(inventory_row["user_count"] if inventory_row else 0),
        "inventory_count": _as_int(inventory_row["inventory_count"] if inventory_row else 0),
        "drop_count": drop_count,
        "claim_count": _as_int(claim_row["count"] if claim_row else 0),
    }


def _activity_task_data_overview(cur, config: dict, uid: str = "") -> dict:
    activity_key = _activity_config_key(config)
    tasks = get_activity_tasks(config)
    daily_key = _task_scope_key("daily")
    weekly_key = _task_scope_key("weekly")
    cur.execute(
        """
        SELECT
            COUNT(DISTINCT user_id) AS user_count,
            COALESCE(SUM(CASE WHEN progress >= target THEN 1 ELSE 0 END), 0) AS complete_count,
            COALESCE(SUM(claimed), 0) AS claim_count
        FROM activity_task_progress
        WHERE activity_key=%s AND scope_key IN (%s, %s)
        """,
        (activity_key, daily_key, weekly_key),
    )
    row = cur.fetchone()
    overview = {
        "activity_key": activity_key,
        "daily_task_count": len([task for task in tasks if task.get("scope_type") == "daily"]),
        "weekly_task_count": len([task for task in tasks if task.get("scope_type") == "weekly"]),
        "user_count": _as_int(row["user_count"] if row else 0),
        "complete_count": _as_int(row["complete_count"] if row else 0),
        "claim_count": _as_int(row["claim_count"] if row else 0),
    }
    if uid:
        progress_map = _get_task_progress_map(cur, activity_key, uid)
        user_rows = []
        for task in tasks:
            scope_type = task["scope_type"]
            scope_key = _task_scope_key(scope_type)
            state = progress_map.get((scope_type, scope_key, task["key"]), {})
            target = max(1, _as_int(task.get("target"), 1))
            progress = min(target, max(0, _as_int(state.get("progress"), 0)))
            user_rows.append({
                "task_key": task["key"],
                "name": task["name"],
                "scope_type": scope_type,
                "scope_key": scope_key,
                "progress": progress,
                "target": target,
                "claimed": bool(state.get("claimed")),
            })
        overview["user"] = user_rows
    return overview


def _activity_pass_data_overview(cur, config: dict, uid: str = "", limit: int = 10) -> dict:
    activity_key = _activity_config_key(config)
    pass_cfg = _activity_pass_config(config)
    if not pass_cfg.get("enabled"):
        return {"enabled": False, "activity_key": activity_key}
    cur.execute(
        """
        SELECT
            COUNT(*) AS user_count,
            COALESCE(SUM(total_exp), 0) AS total_exp,
            COALESCE(MAX(level), 0) AS max_level
        FROM activity_pass_balance
        WHERE activity_key=%s
        """,
        (activity_key,),
    )
    row = cur.fetchone()
    cur.execute(
        """
        SELECT user_id, total_exp, level, update_time
        FROM activity_pass_balance
        WHERE activity_key=%s
        ORDER BY level DESC, total_exp DESC, update_time ASC
        LIMIT %s
        """,
        (activity_key, max(1, min(_as_int(limit, 10), 50))),
    )
    overview = {
        "enabled": True,
        "activity_key": activity_key,
        "name": pass_cfg.get("name"),
        "exp_name": pass_cfg.get("exp_name"),
        "level_exp": pass_cfg.get("level_exp"),
        "max_level": pass_cfg.get("max_level"),
        "catchup": {
            "enabled": bool(pass_cfg.get("catchup_enabled")),
            "start_day": pass_cfg.get("catchup_start_day"),
            "level_gap": pass_cfg.get("catchup_level_gap"),
            "multiplier": pass_cfg.get("catchup_multiplier"),
        },
        "user_count": _as_int(row["user_count"] if row else 0),
        "total_exp": _as_int(row["total_exp"] if row else 0),
        "highest_level": _as_int(row["max_level"] if row else 0),
        "top_users": _attach_display_names([dict(item) for item in cur.fetchall()]),
    }
    if uid:
        overview["user"] = _get_pass_balance(cur, activity_key, uid, pass_cfg)
        overview["user_catchup"] = _pass_catchup_state(cur, config, activity_key, uid, pass_cfg)
    return overview


def get_activity_data_overview(
    activity_key: str | None = None,
    user_id: str | None = None,
    limit: int = 10,
) -> dict:
    cfg = load_config()
    activities = get_gameplay_activities(cfg)
    key_filter = _clean_text(activity_key)
    uid = _clean_text(user_id)
    row_limit = max(1, min(_as_int(limit, 10), 50))

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        sign_summary = {
            "user_count": _fetch_count(cur, "SELECT COUNT(*) AS count FROM activity_user"),
            "today_count": _fetch_count(
                cur,
                "SELECT COUNT(*) AS count FROM activity_sign_log WHERE sign_date=%s",
                (today_str(),),
            ),
            "log_count": _fetch_count(cur, "SELECT COUNT(*) AS count FROM activity_sign_log"),
        }
        cur.execute(
            """
            SELECT user_id, sign_days, total_sign_days, last_sign_date
            FROM activity_user
            ORDER BY sign_days DESC, total_sign_days DESC, last_sign_date ASC
            LIMIT %s
            """,
            (row_limit,),
        )
        sign_rank = _attach_display_names([dict(row) for row in cur.fetchall()])

        activity_rows = []
        for activity in activities:
            if key_filter and activity["key"] != key_filter:
                continue
            ok, reason = activity_state(activity)
            row = {
                "key": activity["key"],
                "name": activity.get("name", ""),
                "type": activity.get("type", ""),
                "enabled": bool(activity.get("enabled")),
                "state": "进行中" if ok else reason,
                "counts": _activity_data_counts(cur, activity["key"], activity.get("type", "")),
            }
            if activity.get("type") == "event_points":
                cur.execute(
                    """
                    SELECT user_id, points, total_points, update_time
                    FROM activity_point_balance
                    WHERE activity_key=%s
                    ORDER BY total_points DESC, points DESC, update_time ASC
                    LIMIT %s
                    """,
                    (activity["key"], row_limit),
                )
                row["top_users"] = _attach_display_names([dict(item) for item in cur.fetchall()])
                if uid:
                    row["user"] = {
                        "balance": _get_point_balance(cur, activity["key"], uid),
                        "purchases": _get_point_purchase_map(cur, activity["key"], uid),
                    }
            elif activity.get("type") == "activity_boss":
                cur.execute(
                    """
                    SELECT user_id, total_damage, update_time
                    FROM activity_boss_damage
                    WHERE activity_key=%s AND total_damage>0
                    ORDER BY total_damage DESC, update_time ASC
                    LIMIT %s
                    """,
                    (activity["key"], row_limit),
                )
                row["top_users"] = _attach_display_names([dict(item) for item in cur.fetchall()])
                if uid:
                    cur.execute(
                        """
                        SELECT total_damage, update_time
                        FROM activity_boss_damage
                        WHERE activity_key=%s AND user_id=%s
                        """,
                        (activity["key"], uid),
                    )
                    damage_row = cur.fetchone()
                    cur.execute(
                        """
                        SELECT item_id, count
                        FROM activity_item_inventory
                        WHERE activity_key=%s AND user_id=%s
                        """,
                        (activity["key"], uid),
                    )
                    row["user"] = {
                        "damage": dict(damage_row) if damage_row else {"total_damage": 0, "update_time": ""},
                        "items": {
                            str(item["item_id"]): max(0, _as_int(item["count"]))
                            for item in cur.fetchall()
                        },
                        "today_fight_count": _fetch_count(
                            cur,
                            """
                            SELECT COUNT(*) AS count FROM activity_boss_fight_log
                            WHERE activity_key=%s AND user_id=%s AND fight_date=%s
                            """,
                            (activity["key"], uid, today_str()),
                        ),
                    }
            else:
                cur.execute(
                    """
                    SELECT user_id, COUNT(*) AS drop_count, MAX(create_time) AS last_time
                    FROM activity_collect_drop_log
                    WHERE activity_key=%s
                    GROUP BY user_id
                    ORDER BY drop_count DESC, last_time ASC
                    LIMIT %s
                    """,
                    (activity["key"], row_limit),
                )
                row["top_users"] = _attach_display_names([dict(item) for item in cur.fetchall()])
                if uid:
                    row["user"] = {
                        "inventory": _get_collect_inventory_map(cur, activity["key"], uid),
                        "claims": _get_collect_claim_map(cur, activity["key"], uid),
                        "pity": _collect_pity_progress_map(cur, activity["key"], uid),
                    }
            activity_rows.append(row)

        user_sign = None
        if uid:
            cur.execute("SELECT * FROM activity_user WHERE user_id=%s", (uid,))
            row = cur.fetchone()
            if row:
                user_sign = dict(row)
                user_sign["sign_days"] = _as_int(user_sign.get("sign_days"))
                user_sign["total_sign_days"] = _as_int(user_sign.get("total_sign_days"), user_sign["sign_days"])
            else:
                user_sign = {
                    "user_id": uid,
                    "sign_days": 0,
                    "last_sign_date": "",
                    "total_sign_days": 0,
                }
        return {
            "sign": sign_summary,
            "sign_rank": sign_rank,
            "activities": activity_rows,
            "tasks": _activity_task_data_overview(cur, cfg, uid),
            "activity_pass": _activity_pass_data_overview(cur, cfg, uid, row_limit),
            "runtime": activity_runtime_state(cfg),
            "user_id": uid,
            "user_sign": user_sign,
        }
    finally:
        conn.close()


def reset_activity_data(scope: str, activity_key: str | None = None) -> str:
    target_scope = _clean_text(scope, "activity")
    key = _clean_text(activity_key)
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    try:
        cur = conn.cursor()
        deleted = 0
        if target_scope in {"sign", "all"}:
            for table in ("activity_user", "activity_sign_log"):
                cur.execute(f"DELETE FROM {table}")
                deleted += max(0, cur.rowcount)

        if target_scope in {"activity", "gameplay", "all"}:
            if target_scope == "activity" and not key:
                raise ValueError("请选择要清空的玩法活动")
            tables = (
                "activity_collect_inventory",
                "activity_collect_claim",
                "activity_collect_drop_log",
                "activity_collect_pity_state",
                "activity_point_balance",
                "activity_point_event_log",
                "activity_point_purchase",
                "activity_item_inventory",
                "activity_boss_state",
                "activity_boss_damage",
                "activity_boss_fight_log",
                "activity_boss_milestone",
                "activity_boss_milestone_claim",
                "activity_boss_rank_claim",
            )
            for table in tables:
                if key:
                    cur.execute(f"DELETE FROM {table} WHERE activity_key=%s", (key,))
                else:
                    cur.execute(f"DELETE FROM {table}")
                deleted += max(0, cur.rowcount)

        if target_scope in {"task", "tasks", "pass", "activity_pass", "all"}:
            activity_key_for_main = _activity_config_key(load_config())
            task_pass_tables = (
                "activity_task_progress",
                "activity_task_claim_log",
                "activity_pass_balance",
                "activity_pass_event_log",
                "activity_pass_reward_claim",
            )
            for table in task_pass_tables:
                if key:
                    cur.execute(f"DELETE FROM {table} WHERE activity_key=%s", (key,))
                elif target_scope != "all":
                    cur.execute(f"DELETE FROM {table} WHERE activity_key=%s", (activity_key_for_main,))
                else:
                    cur.execute(f"DELETE FROM {table}")
                deleted += max(0, cur.rowcount)

        if target_scope not in {"sign", "activity", "gameplay", "task", "tasks", "pass", "activity_pass", "all"}:
            raise ValueError("清理范围无效")
        conn.commit()
        return f"已清理活动数据，影响记录 {deleted} 条"
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def adjust_activity_points(activity_key: str, user_id: str, amount: int) -> dict:
    key = _clean_text(activity_key)
    uid = _clean_text(user_id)
    delta = _as_int(amount, 0)
    if not key:
        raise ValueError("请选择积分活动")
    activity = next(
        (
            item
            for item in get_gameplay_activities(load_config())
            if item.get("key") == key
        ),
        None,
    )
    if not activity or activity.get("type") != "event_points":
        raise ValueError("请选择积分活动")
    if not uid:
        raise ValueError("请输入用户ID")
    if delta == 0:
        raise ValueError("调整数量不能为 0")

    ts = now_str()
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        balance = _get_point_balance(cur, key, uid)
        next_points = max(0, balance["points"] + delta)
        next_total = balance["total_points"] + max(0, delta)
        cur.execute(
            """
            INSERT INTO activity_point_balance (
                activity_key, user_id, points, total_points, update_time
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(activity_key, user_id) DO UPDATE SET
                points = excluded.points,
                total_points = excluded.total_points,
                update_time = excluded.update_time
            """,
            (key, uid, next_points, next_total, ts),
        )
        conn.commit()
        return {"points": next_points, "total_points": next_total}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def adjust_collect_word(activity_key: str, user_id: str, word_char: str, amount: int) -> dict:
    key = _clean_text(activity_key)
    uid = _clean_text(user_id)
    char = _clean_text(word_char)
    delta = _as_int(amount, 0)
    if not key:
        raise ValueError("请选择集字活动")
    activity = next(
        (
            item
            for item in get_gameplay_activities(load_config())
            if item.get("key") == key
        ),
        None,
    )
    if not activity or activity.get("type") != "collect_words":
        raise ValueError("请选择集字活动")
    if not uid:
        raise ValueError("请输入用户ID")
    if not char:
        raise ValueError("请输入字牌")
    if delta == 0:
        raise ValueError("调整数量不能为 0")
    char = char[0]

    ts = now_str()
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        inventory = _get_collect_inventory_map(cur, key, uid)
        next_count = max(0, inventory.get(char, 0) + delta)
        cur.execute(
            """
            INSERT INTO activity_collect_inventory (
                activity_key, user_id, word_char, count, update_time
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(activity_key, user_id, word_char) DO UPDATE SET
                count = excluded.count,
                update_time = excluded.update_time
            """,
            (key, uid, char, next_count, ts),
        )
        conn.commit()
        return {"word_char": char, "count": next_count}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def adjust_activity_pass_exp(user_id: str, amount: int) -> dict:
    uid = _clean_text(user_id)
    delta = _as_int(amount, 0)
    if not uid:
        raise ValueError("请输入用户ID")
    if delta == 0:
        raise ValueError("调整数量不能为 0")

    cfg = load_config()
    pass_cfg = _activity_pass_config(cfg)
    if not pass_cfg.get("enabled"):
        raise ValueError("活动战令未开启")
    activity_key = _activity_config_key(cfg)
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        before = _get_pass_balance(cur, activity_key, uid, pass_cfg)
        if delta > 0:
            _, after = _grant_pass_exp(cur, activity_key, uid, pass_cfg, delta)
        else:
            next_total = max(0, before["total_exp"] + delta)
            level_exp = max(1, _as_int(pass_cfg.get("level_exp"), 100))
            max_level = max(1, _as_int(pass_cfg.get("max_level"), 12))
            level = _calc_pass_level(next_total, level_exp, max_level)
            current_exp = _pass_current_exp(next_total, level, level_exp, max_level)
            cur.execute(
                """
                INSERT INTO activity_pass_balance (
                    activity_key, user_id, exp, total_exp, level, update_time
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(activity_key, user_id) DO UPDATE SET
                    exp = excluded.exp,
                    total_exp = excluded.total_exp,
                    level = excluded.level,
                    update_time = excluded.update_time
                """,
                (activity_key, uid, current_exp, next_total, level, now_str()),
            )
            after = {
                "exp": current_exp,
                "total_exp": next_total,
                "level": level,
                "level_exp": level_exp,
                "max_level": max_level,
            }
        conn.commit()
        return after
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _finish_sign_log(user_id: str, sign_date: str, status: str, message: str):
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE activity_sign_log
            SET reward_status=%s, reward_message=%s, finish_time=%s
            WHERE user_id=%s AND sign_date=%s
            """,
            (str(status), str(message), now_str(), str(user_id), str(sign_date)),
        )
        conn.commit()
    finally:
        conn.close()


def claim_sign(user_id: str, operation_id: str | None = None) -> tuple[bool, str]:
    cfg = load_config()
    runtime = activity_runtime_state(cfg)
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放"
    if "sign" not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放活动签到"

    uid = str(user_id)
    today = today_str()
    ts = now_str()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM activity_user WHERE user_id=%s", (uid,))
        row = cur.fetchone()
        if row and row["last_sign_date"] == today:
            return False, "今日已经领取过活动签到"

        current_sign_days = _as_int(row["sign_days"] if row else 0)
        if row and "total_sign_days" in row.keys():
            current_total_sign_days = _as_int(row["total_sign_days"], current_sign_days)
        else:
            current_total_sign_days = current_sign_days
        sign_days = current_sign_days + 1
        total_sign_days = current_total_sign_days + 1
        daily_reward = _reward_by_day(cfg, sign_days)
        milestone_reward = _milestone_by_days(cfg, sign_days)
        daily_reward_text = str(daily_reward.get("reward") or "")
        milestone_reward_text = str(milestone_reward.get("reward") or "")
        try:
            daily_reward_items = parse_reward(daily_reward_text)
            milestone_reward_items = parse_reward(milestone_reward_text)
        except Exception as e:
            return False, f"活动奖励配置错误：{e}"

    finally:
        conn.close()

    result = activity_sign_settlement_service.settle(
        operation_id or f"activity-sign:{uid}:{time.time_ns()}", uid, today,
        current_sign_days, current_total_sign_days, daily_reward_items,
        milestone_reward_items, XiuConfig().max_goods_num,
        daily_reward_text, milestone_reward_text,
    )
    if not result.succeeded:
        return False, {"already_signed":"今日已经领取过活动签到","inventory_full":"背包空间不足，奖励未领取","user_missing":"角色不存在","state_changed":"签到状态已变化，请重试","operation_conflict":"签到请求冲突，请重新发送"}.get(result.status,"活动签到失败")
    daily_msg, milestone_msg = [], []

    reply_mode = _sign_reply_mode(cfg)
    lines = [
        f"{cfg.get('festival_name', '节日')}签到成功",
        f"累计签到：{sign_days} 天",
    ]
    if reply_mode == "minimal":
        milestone_name = _clean_text(milestone_reward.get("name"))
        if milestone_reward_text or milestone_name:
            lines.append(f"达成：{milestone_name or f'累计{sign_days}天'}")
    elif reply_mode == "normal":
        if milestone_reward_text or milestone_reward.get("name"):
            title = str(milestone_reward.get("name") or f"累计{sign_days}天奖励")
            lines.append(_format_reward_result(title, milestone_reward_text, milestone_msg))
    else:
        lines.append(_format_reward_result("今日奖励", daily_reward_text, daily_msg))
        if milestone_reward_text or milestone_reward.get("name"):
            title = str(milestone_reward.get("name") or f"累计{sign_days}天奖励")
            lines.append(_format_reward_result(title, milestone_reward_text, milestone_msg))
    try:
        lines.extend(record_activity_event(uid, "sign_in"))
    except Exception as e:
        logger.warning(f"活动签到记录玩法掉落失败 user_id={uid}: {e}")
    log_lines = list(lines)
    if reply_mode == "minimal":
        log_lines.append(_format_reward_result("今日奖励", daily_reward_text, daily_msg))
        if milestone_reward_text or milestone_reward.get("name"):
            title = str(milestone_reward.get("name") or f"累计{sign_days}天奖励")
            log_lines.append(_format_reward_result(title, milestone_reward_text, milestone_msg))
    return True, "\n".join(lines)


def _append_gameplay_summary(lines: list[str], cfg: dict, *, detail: bool) -> None:
    gameplay_activities = get_gameplay_activities(cfg)
    if not gameplay_activities:
        return
    lines.append("")
    lines.append("【玩法活动】")
    for activity in gameplay_activities:
        ok, reason = activity_state(activity)
        status = "进行中" if ok else reason
        lines.append(f"- {activity.get('name', '集字活动')}：{status}")
        if not detail:
            continue
        if activity.get("description"):
            lines.append(f"  {activity.get('description')}")
        if activity.get("type") == "event_points":
            point_name = activity.get("point_name") or "活动积分"
            rules = activity.get("event_rules") or []
            if rules:
                rule_text = "、".join(
                    f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+{_as_int(rule.get('points'))}"
                    for rule in rules
                )
                lines.append(f"  积分来源：{rule_text}")
            shop = activity.get("shop") or []
            if shop:
                shop_text = "、".join(str(item.get("name") or item.get("item_key")) for item in shop)
                lines.append(f"  {point_name}商店：{shop_text}")
            continue
        if activity.get("type") == "activity_boss":
            mode = activity.get("mode") or "cooperative"
            lines.append(f"  首领：{activity.get('boss_name', '活动首领')}")
            if mode in {"item_raid", "both"}:
                item_names = "、".join(str(it.get("name")) for it in (activity.get("items") or []))
                lines.append(f"  道具讨伐：{item_names or '未配置'}（随机伤害区间）")
                lines.append("  命令：活动讨伐 [首领名] 道具名")
            if mode in {"cooperative", "both"}:
                cap_pct = int(_as_float(activity.get("hit_hp_cap_ratio"), 0.01) * 100)
                lines.append(
                    f"  全服协作：攻力×{int(_as_float(activity.get('atk_ratio'), 0.1) * 100)}%，"
                    f"每日{_as_int(activity.get('daily_fight_limit'), 3)}次，单次伤害上限{cap_pct}%血量"
                )
                lines.append("  命令：活动讨伐 / 讨伐世界BOSS 也会计入（若开启）")
            lines.append("  活动首领 / 活动首领排行 / 活动首领领奖")
            continue

        event_text = _activity_event_text(activity.get("drop_events"))
        lines.append(
            f"  掉落来源：{event_text or '未配置'}，"
            f"每日上限：{_as_int(activity.get('daily_drop_limit'), 0)}，"
            f"掉落概率：{int(_drop_rate(activity.get('drop_rate'), 0) * 100)}%"
        )
        phrases = activity.get("phrases") or []
        if phrases:
            phrase_text = "、".join(str(item.get("name") or item.get("phrase")) for item in phrases)
            lines.append(f"  兑换词组：{phrase_text}")


def _append_stage_summary(lines: list[str], cfg: dict, *, detail: bool = False) -> None:
    runtime = activity_runtime_state(cfg)
    stage = runtime.get("stage") or {}
    lines.append("")
    lines.append("【活动阶段】")
    lines.append(f"当前：{runtime.get('stage_name') or '未开放'}")
    if stage:
        time_text = _stage_time_text(stage)
        if time_text:
            lines.append(f"阶段时间：{time_text}")
    lines.append("开放内容：" + _stage_feature_text(list(runtime.get("features") or [])))
    multiplier = _as_float(runtime.get("multiplier"), 1.0)
    if detail or abs(multiplier - 1.0) > 0.001:
        lines.append(f"阶段倍率：{multiplier:.2f}x")
    next_stage = runtime.get("next_stage")
    if next_stage:
        lines.append(
            f"下一阶段：{next_stage.get('name', '活动阶段')}（{_stage_time_text(next_stage)}）"
        )
    if not runtime.get("can_produce") and "claim" in set(runtime.get("features") or []):
        lines.append("当前阶段主要用于领奖、兑换和结算，不再产出活动积分、字牌或战令经验。")


def _append_pass_summary(lines: list[str], cfg: dict, user_id: str | None = None, *, detail: bool = False) -> None:
    pass_cfg = _activity_pass_config(cfg)
    if not pass_cfg.get("enabled"):
        return
    lines.append("")
    lines.append(f"【{pass_cfg['name']}】")
    if user_id:
        ensure_activity_files()
        conn = db_backend.connect(DB_PATH)
        conn.row_factory = db_backend.Row
        try:
            balance = _get_pass_balance(conn.cursor(), _activity_config_key(cfg), str(user_id), pass_cfg)
        finally:
            conn.close()
        lines.append(
            f"等级 {balance['level']}/{balance['max_level']}，"
            f"{pass_cfg['exp_name']} {balance['exp']}/{balance['level_exp']}"
        )
    else:
        lines.append(f"每 {pass_cfg['level_exp']}{pass_cfg['exp_name']} 提升 1 级")
    if detail:
        rules = pass_cfg.get("event_rules") or []
        if rules:
            rule_text = "、".join(
                f"{ACTIVITY_EVENT_LABELS.get(rule.get('event'), rule.get('event'))}+{_as_int(rule.get('exp'))}"
                for rule in rules
            )
            lines.append(f"来源：{rule_text}")
    lines.append("命令：活动战令 / 活动战令领取")


def _append_task_summary(lines: list[str], cfg: dict, user_id: str | None = None, *, detail: bool = False) -> None:
    tasks = get_activity_tasks(cfg)
    if not tasks:
        return
    lines.append("")
    lines.append("【活动目标】")
    if not user_id:
        daily_count = len([task for task in tasks if task.get("scope_type") == "daily"])
        weekly_count = len([task for task in tasks if task.get("scope_type") == "weekly"])
        lines.append(f"每日 {daily_count} 项，周常 {weekly_count} 项")
        lines.append("命令：活动任务 / 活动任务领取")
        return

    activity_key = _activity_config_key(cfg)
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        progress_map = _get_task_progress_map(conn.cursor(), activity_key, str(user_id))
    finally:
        conn.close()
    for scope_type in ("daily", "weekly"):
        scope_tasks = [task for task in tasks if task.get("scope_type") == scope_type]
        if not scope_tasks:
            continue
        scope_key = _task_scope_key(scope_type)
        finished = 0
        claimable = 0
        for task in scope_tasks:
            state = progress_map.get((scope_type, scope_key, task["key"]), {})
            target = max(1, _as_int(task.get("target"), 1))
            progress = min(target, max(0, _as_int(state.get("progress"), 0)))
            claimed = bool(state.get("claimed"))
            if claimed:
                finished += 1
            elif progress >= target:
                claimable += 1
        label = _scope_label(scope_type)
        lines.append(f"{label}：已领 {finished}/{len(scope_tasks)}，可领 {claimable}")
    daily_tips = []
    daily_key = _task_scope_key("daily")
    for task in [task for task in tasks if task.get("scope_type") == "daily"]:
        state = progress_map.get(("daily", daily_key, task["key"]), {})
        target = max(1, _as_int(task.get("target"), 1))
        progress = min(target, max(0, _as_int(state.get("progress"), 0)))
        claimed = bool(state.get("claimed"))
        if claimed:
            continue
        if progress >= target:
            daily_tips.append(f"{task['name']}可领")
        else:
            daily_tips.append(f"{task['name']} {progress}/{target}")
        if len(daily_tips) >= 3:
            break
    if daily_tips:
        lines.append("今日建议：" + "、".join(daily_tips))
    if detail:
        lines.append("查看明细：活动任务")
    lines.append("领奖：活动任务领取")


def _append_action_summary(lines: list[str], cfg: dict, user_id: str | None = None) -> None:
    if not user_id:
        return
    uid = str(user_id)
    activity_key = _activity_config_key(cfg)
    tips: list[str] = []
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        claimable_tasks = len(_select_claimable_tasks(cur, cfg, uid))
        if claimable_tasks:
            tips.append(f"{claimable_tasks}个任务奖励可领")

        pass_cfg = _activity_pass_config(cfg)
        if pass_cfg.get("enabled"):
            balance = _get_pass_balance(cur, activity_key, uid, pass_cfg)
            cur.execute(
                """
                SELECT level FROM activity_pass_reward_claim
                WHERE activity_key=%s AND user_id=%s
                """,
                (activity_key, uid),
            )
            claimed = {max(0, _as_int(row["level"])) for row in cur.fetchall()}
            pass_claimable = [
                reward for reward in pass_cfg.get("level_rewards") or []
                if _as_int(reward.get("level"), 0) > 0
                and _as_int(reward.get("level"), 0) <= balance["level"]
                and _as_int(reward.get("level"), 0) not in claimed
            ]
            if pass_claimable:
                tips.append(f"{len(pass_claimable)}档战令奖励可领")
            catchup = _pass_catchup_state(cur, cfg, activity_key, uid, pass_cfg)
            if catchup.get("active"):
                tips.append(f"战令追赶{catchup['catchup_multiplier']:.2f}x生效")

        for activity in get_gameplay_activities(cfg):
            if activity.get("type") != "collect_words":
                continue
            pity_threshold = max(0, _as_int(activity.get("pity_threshold"), 0))
            if pity_threshold <= 0:
                continue
            pity_map = _collect_pity_progress_map(cur, activity["key"], uid)
            near_events = [
                ACTIVITY_EVENT_LABELS.get(event_key, event_key)
                for event_key in activity.get("drop_events") or []
                if pity_map.get(event_key, 0) >= max(1, pity_threshold - 1)
            ]
            if near_events:
                tips.append(f"{activity['name']}接近保底：{near_events[0]}")
                break
    finally:
        conn.close()

    if not tips:
        return
    lines.append("")
    lines.append("【行动建议】")
    lines.append("；".join(tips[:4]))
    lines.append("一键领取：活动领取")


def build_activity_rewards_text() -> str:
    cfg = load_config()
    lines = [f"【{cfg.get('name', '节日签到活动')} · 奖励】"]
    lines.append("")
    lines.append("【每日签到奖励】")
    rewards = cfg.get("daily_rewards") or []
    if rewards:
        for reward in sorted(rewards, key=lambda item: _as_int(item.get("day"))):
            day = _as_int(reward.get("day"))
            name = str(reward.get("name") or f"第{day}天")
            lines.append(f"- 第{day}天 {name}：{reward.get('reward') or '暂无奖励'}")
    else:
        lines.append("暂无每日奖励配置")

    milestones = cfg.get("milestone_rewards") or []
    if milestones:
        lines.append("")
        lines.append("【累计签到奖励】")
        for reward in sorted(milestones, key=lambda item: _as_int(item.get("days"))):
            days = _as_int(reward.get("days"))
            name = str(reward.get("name") or f"累计{days}天")
            lines.append(f"- {name}：{reward.get('reward') or '暂无奖励'}")
    return "\n".join(lines).strip()


def build_activity_tasks_text() -> str:
    cfg = load_config()
    lines = [f"【{cfg.get('name', '节日签到活动')} · 任务】"]
    daily_tasks = cfg.get("daily_tasks") or []
    if daily_tasks:
        lines.append("")
        lines.append("【每日活动任务】")
        for task in daily_tasks:
            if isinstance(task, dict):
                lines.append(_format_activity_task(task))
    else:
        lines.append("")
        lines.append("暂无每日活动任务")

    weekly_tasks = cfg.get("weekly_tasks") or []
    if weekly_tasks:
        lines.append("")
        lines.append("【周常活动任务】")
        for task in weekly_tasks:
            if isinstance(task, dict):
                lines.append(_format_activity_task(task))
    _append_pass_summary(lines, cfg, detail=True)
    return "\n".join(lines).strip()


def build_activity_gameplay_text() -> str:
    cfg = load_config()
    lines = [f"【{cfg.get('name', '节日签到活动')} · 玩法】"]
    _append_stage_summary(lines, cfg, detail=True)
    _append_gameplay_summary(lines, cfg, detail=True)
    if len(lines) <= 1:
        lines.append("")
        lines.append("暂无玩法活动")
    return "\n".join(lines).strip()


def build_activity_info(user_id: str | None = None) -> str:
    cfg = load_config()
    ok, reason = activity_state(cfg)
    lines = [
        f"【{cfg.get('name', '节日签到活动')}】",
        str(cfg.get("description") or ""),
        f"状态：{'进行中' if ok else reason}",
        f"活动时间：{cfg.get('start_time', '0')} 至 {cfg.get('end_time', '无限')}",
        f"签到命令：{cfg.get('sign_command', '活动签到')}",
    ]
    if user_id:
        user = get_user_sign(str(user_id))
        lines.extend([
            "",
            f"我的累计签到：{int(user.get('sign_days', 0) or 0)} 天",
            f"上次签到：{user.get('last_sign_date') or '暂无'}",
        ])

    if _activity_info_mode(cfg) == "full":
        _append_stage_summary(lines, cfg, detail=True)
        lines.append("")
        lines.append("【每日签到奖励】")
        rewards = cfg.get("daily_rewards") or []
        if rewards:
            for reward in sorted(rewards, key=lambda item: _as_int(item.get("day"))):
                day = _as_int(reward.get("day"))
                name = str(reward.get("name") or f"第{day}天")
                lines.append(f"- 第{day}天 {name}：{reward.get('reward') or '暂无奖励'}")
        else:
            lines.append("暂无每日奖励配置")

        milestones = cfg.get("milestone_rewards") or []
        if milestones:
            lines.append("")
            lines.append("【累计签到奖励】")
            for reward in sorted(milestones, key=lambda item: _as_int(item.get("days"))):
                days = _as_int(reward.get("days"))
                name = str(reward.get("name") or f"累计{days}天")
                lines.append(f"- {name}：{reward.get('reward') or '暂无奖励'}")

        daily_tasks = cfg.get("daily_tasks") or []
        if daily_tasks:
            lines.append("")
            lines.append("【每日活动任务】")
            for task in daily_tasks:
                if isinstance(task, dict):
                    lines.append(_format_activity_task(task))

        weekly_tasks = cfg.get("weekly_tasks") or []
        if weekly_tasks:
            lines.append("")
            lines.append("【周常活动任务】")
            for task in weekly_tasks:
                if isinstance(task, dict):
                    lines.append(_format_activity_task(task))

        _append_gameplay_summary(lines, cfg, detail=True)
        _append_task_summary(lines, cfg, user_id, detail=True)
        _append_pass_summary(lines, cfg, user_id, detail=True)
        _append_action_summary(lines, cfg, user_id)
    else:
        lines.extend([
            "",
            "查询奖励：活动奖励",
            "查询任务：活动任务",
            "领取任务：活动任务领取",
            "活动战令：活动战令 / 活动战令领取",
            "玩法说明：活动玩法",
            "个人进度：活动排行 / 活动背包 / 活动积分",
        ])
        _append_action_summary(lines, cfg, user_id)
        _append_stage_summary(lines, cfg, detail=False)
        _append_task_summary(lines, cfg, user_id, detail=False)
        _append_pass_summary(lines, cfg, user_id, detail=False)
        _append_gameplay_summary(lines, cfg, detail=False)

    return "\n".join(lines).strip()


def get_rank(limit: int = 10) -> list[dict]:
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, sign_days, total_sign_days, last_sign_date
            FROM activity_user
            ORDER BY sign_days DESC, total_sign_days DESC, last_sign_date ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        rows = [dict(row) for row in cur.fetchall()]
        return _attach_display_names(rows)
    finally:
        conn.close()


def build_rank_text(limit: int = 10) -> str:
    cfg = load_config()
    rows = get_rank(limit)
    lines = [f"【{cfg.get('name', '节日签到活动')}排行】"]
    if not rows:
        lines.append("暂无排行数据")
        return "\n".join(lines)

    for index, row in enumerate(rows, 1):
        name = row.get("display_name") or row.get("user_name") or resolve_daohao(str(row.get("user_id") or ""))
        lines.append(
            f"{index}. {name} 累计签到 "
            f"{int(row.get('sign_days', 0) or 0)} 天"
        )
    return "\n".join(lines)


def set_enabled(enabled: bool, target: str | None = None) -> str:
    cfg = load_config()
    target_text = _clean_text(target)
    action_text = "开启" if enabled else "关闭"
    if not target_text:
        cfg["enabled"] = bool(enabled)
        save_config(cfg)
        return f"已{action_text}签到活动"

    if target_text in {"全部", "所有", "all", "ALL"}:
        cfg["enabled"] = bool(enabled)
        for activity in cfg.get("gameplay_activities") or []:
            if isinstance(activity, dict):
                activity["enabled"] = bool(enabled)
        extensions = cfg.setdefault("extensions", {})
        if isinstance(extensions, dict):
            activity_pass = extensions.setdefault("activity_pass", deepcopy(DEFAULT_ACTIVITY_PASS))
            if isinstance(activity_pass, dict):
                activity_pass["enabled"] = bool(enabled)
        save_config(cfg)
        return f"已{action_text}全部活动"

    if target_text in {"签到", "节日签到", "签到活动"}:
        cfg["enabled"] = bool(enabled)
        save_config(cfg)
        return f"已{action_text}签到活动"

    if target_text in {"玩法", "玩法活动"}:
        changed_count = 0
        for activity in cfg.get("gameplay_activities") or []:
            if isinstance(activity, dict):
                activity["enabled"] = bool(enabled)
                changed_count += 1
        if changed_count:
            save_config(cfg)
            return f"已{action_text}{changed_count}个玩法活动"
        return "当前没有配置玩法活动"

    if target_text in {"战令", "活动战令", "通行证", "活动通行证", "活跃"}:
        extensions = cfg.setdefault("extensions", {})
        if not isinstance(extensions, dict):
            extensions = {}
            cfg["extensions"] = extensions
        activity_pass = extensions.setdefault("activity_pass", deepcopy(DEFAULT_ACTIVITY_PASS))
        if not isinstance(activity_pass, dict):
            activity_pass = deepcopy(DEFAULT_ACTIVITY_PASS)
            extensions["activity_pass"] = activity_pass
        activity_pass["enabled"] = bool(enabled)
        save_config(cfg)
        return f"已{action_text}活动战令"

    type_targets = {
        "集字": "collect_words",
        "集字活动": "collect_words",
        "积分": "event_points",
        "积分活动": "event_points",
        "活动积分": "event_points",
        "活动商店": "event_points",
        "首领": "activity_boss",
        "活动首领": "activity_boss",
        "BOSS": "activity_boss",
        "boss": "activity_boss",
    }
    if target_text in type_targets:
        target_type = type_targets[target_text]
        changed_count = 0
        for activity in cfg.get("gameplay_activities") or []:
            if isinstance(activity, dict) and _clean_text(activity.get("type"), "collect_words") == target_type:
                activity["enabled"] = bool(enabled)
                changed_count += 1
        if changed_count:
            save_config(cfg)
            return f"已{action_text}{changed_count}个{target_text}"
        return f"当前没有配置{target_text}"

    changed = False
    for activity in cfg.get("gameplay_activities") or []:
        if not isinstance(activity, dict):
            continue
        names = {
            _clean_text(activity.get("key")),
            _clean_text(activity.get("name")),
            _clean_text(activity.get("template_key")),
            _clean_text(activity.get("type")),
        }
        if target_text in names:
            activity["enabled"] = bool(enabled)
            changed = True
            break

    if not changed:
        return f"未找到活动：{target_text}"
    save_config(cfg)
    return f"已{action_text}{target_text}"


ensure_activity_files()
