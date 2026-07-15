"""活动首领：道具讨伐（随机伤害）与全服协作首领（攻击十分之一、单次上限1%血量、每日3次）。"""

from __future__ import annotations

import hashlib
import time

from nonebot.log import logger

from ...paths import get_paths
from ..xiuxian_utils import db_backend
from ..xiuxian_utils.utils import number_to
from .service import (
    DB_PATH,
    _as_bool,
    _as_float,
    _as_int,
    _clean_text,
    _sql_message,
    STAGE_FEATURES,
    activity_runtime_state,
    ensure_activity_files,
    get_gameplay_activities,
    load_config,
    now_str,
    resolve_daohao,
    today_str,
)
from .boss_reward_claim_service import BossRewardClaimService
from .boss_coop_settlement_service import ActivityBossCoopSettlementService
from .boss_item_raid_settlement_service import ActivityBossItemRaidSettlementService

BOSS_MODES = {"item_raid", "cooperative", "both"}

activity_boss_coop_settlement_service = ActivityBossCoopSettlementService(DB_PATH)
activity_boss_item_raid_settlement_service = ActivityBossItemRaidSettlementService(DB_PATH)


def _fixed_item_damage(operation_id: str, damage_min: int, damage_max: int) -> int:
    span = damage_max - damage_min + 1
    value = int.from_bytes(hashlib.sha256(operation_id.encode()).digest()[:8], "big")
    return damage_min + value % span


def _runtime_gate(feature: str) -> tuple[bool, str, float]:
    runtime = activity_runtime_state(load_config())
    if not runtime.get("ok"):
        return False, runtime.get("reason") or "活动未开放", 0.0
    if feature not in set(runtime.get("features") or []):
        return False, f"当前阶段【{runtime.get('stage_name', '活动阶段')}】不开放{STAGE_FEATURES.get(feature, feature)}", 0.0
    return True, "", max(0.0, _as_float(runtime.get("multiplier"), 1.0))


def _eternal_boss_max_hp() -> int:
    # SQLite INTEGER is signed 64-bit; clamp so activity/world-boss co-damage can persist.
    sqlite_max = 2**63 - 1
    try:
        from ..xiuxian_boss.makeboss import get_boss_exp

        info = get_boss_exp("永恒境")
        if info and info.get("总血量"):
            return max(1, min(int(info["总血量"]), sqlite_max))
    except Exception as e:
        logger.warning(f"读取永恒境首领血量失败: {e}")
    return min(10**18, sqlite_max)


def normalize_activity_boss(raw: dict, index: int, key: str) -> dict:
    mode = _clean_text(raw.get("mode"), "cooperative")
    if mode not in BOSS_MODES:
        mode = "cooperative"
    max_hp = _as_int(raw.get("max_hp"), 0)
    if max_hp <= 0:
        max_hp = _eternal_boss_max_hp()
    items = []
    for row in raw.get("items") or []:
        if not isinstance(row, dict):
            continue
        item_id = _clean_text(row.get("id") or row.get("item_id"))
        if not item_id:
            continue
        dmin = max(1, _as_int(row.get("damage_min"), 100))
        dmax = max(dmin, _as_int(row.get("damage_max"), 500))
        items.append({
            "id": item_id,
            "name": _clean_text(row.get("name"), item_id),
            "damage_min": dmin,
            "damage_max": dmax,
            "cost": max(1, _as_int(row.get("cost"), 1)),
        })
    rank_rewards = []
    for row in raw.get("rank_rewards") or []:
        if not isinstance(row, dict):
            continue
        rank_rewards.append({
            "rank_min": max(1, _as_int(row.get("rank_min"), 1)),
            "rank_max": max(1, _as_int(row.get("rank_max"), 1)),
            "reward": _clean_text(row.get("reward")),
            "name": _clean_text(row.get("name"), "排行奖励"),
        })
    server_milestones = []
    for row in raw.get("server_milestones") or []:
        if not isinstance(row, dict):
            continue
        hp_percent = _as_float(row.get("hp_percent"), 0)
        if hp_percent <= 0:
            continue
        server_milestones.append({
            "key": _clean_text(row.get("key"), f"p{int(hp_percent)}"),
            "hp_percent": min(100.0, max(0.0, hp_percent)),
            "name": _clean_text(row.get("name"), "全服进度奖励"),
            "reward": _clean_text(row.get("reward")),
        })
    server_milestones.sort(key=lambda x: x["hp_percent"], reverse=True)

    return {
        "key": key,
        "type": "activity_boss",
        "template_key": _clean_text(raw.get("template_key"), key),
        "enabled": _as_bool(raw.get("enabled")),
        "name": _clean_text(raw.get("name"), f"活动首领{index}"),
        "description": _clean_text(raw.get("description"), "全服协力讨伐活动首领。"),
        "start_time": _clean_text(raw.get("start_time"), "0"),
        "end_time": _clean_text(raw.get("end_time"), "无限"),
        "boss_name": _clean_text(raw.get("boss_name"), "活动首领"),
        "mode": mode,
        "max_hp": max_hp,
        "atk_ratio": min(1.0, max(0.01, _as_float(raw.get("atk_ratio"), 0.1))),
        "hit_hp_cap_ratio": min(0.2, max(0.001, _as_float(raw.get("hit_hp_cap_ratio"), 0.01))),
        "daily_fight_limit": max(1, _as_int(raw.get("daily_fight_limit"), 3)),
        "items": items,
        "rank_rewards": rank_rewards,
        "server_milestones": server_milestones,
        "drop_events": list(raw.get("drop_events") or []) if isinstance(raw.get("drop_events"), list) else [],
    }


def init_boss_tables(conn) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activity_item_inventory (
            activity_key TEXT NOT NULL,
            user_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            count INTEGER NOT NULL DEFAULT 0,
            update_time TEXT DEFAULT '',
            PRIMARY KEY(activity_key, user_id, item_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activity_boss_state (
            activity_key TEXT PRIMARY KEY,
            hp_left INTEGER NOT NULL,
            max_hp INTEGER NOT NULL,
            update_time TEXT DEFAULT ''
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activity_boss_damage (
            activity_key TEXT NOT NULL,
            user_id TEXT NOT NULL,
            total_damage INTEGER NOT NULL DEFAULT 0,
            update_time TEXT DEFAULT '',
            PRIMARY KEY(activity_key, user_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activity_boss_fight_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            activity_key TEXT NOT NULL,
            user_id TEXT NOT NULL,
            damage INTEGER NOT NULL DEFAULT 0,
            fight_date TEXT DEFAULT '',
            source TEXT DEFAULT '',
            create_time TEXT DEFAULT ''
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activity_boss_milestone (
            activity_key TEXT NOT NULL,
            milestone_key TEXT NOT NULL,
            unlocked_time TEXT DEFAULT '',
            PRIMARY KEY(activity_key, milestone_key)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activity_boss_milestone_claim (
            activity_key TEXT NOT NULL,
            user_id TEXT NOT NULL,
            milestone_key TEXT NOT NULL,
            create_time TEXT DEFAULT '',
            PRIMARY KEY(activity_key, user_id, milestone_key)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activity_boss_rank_claim (
            activity_key TEXT NOT NULL,
            user_id TEXT NOT NULL,
            tier_key TEXT NOT NULL,
            create_time TEXT DEFAULT '',
            PRIMARY KEY(activity_key, user_id, tier_key)
        )
    """)


def _active_boss_activities(config=None):
    from .service import activity_state

    out = []
    for act in get_gameplay_activities(config):
        if act.get("type") != "activity_boss":
            continue
        ok, _ = activity_state(act)
        if not ok:
            continue
        out.append(act)
    return out


def _find_boss_activity(query: str, config=None):
    text = _clean_text(query)
    for act in get_gameplay_activities(config):
        if act.get("type") != "activity_boss":
            continue
        from .service import activity_state
        if not text:
            continue
        if text in {
            act.get("key", ""),
            act.get("name", ""),
            act.get("boss_name", ""),
            act.get("template_key", ""),
        } or text in act.get("name", "") or text in act.get("boss_name", ""):
            ok, _ = activity_state(act)
            if ok or text:
                return act
    if not text:
        active = _active_boss_activities(config)
        return active[0] if len(active) == 1 else None
    return None


def _ensure_boss_hp(cur, activity: dict) -> tuple[int, int]:
    key = activity["key"]
    max_hp = int(activity["max_hp"])
    cur.execute(
        "SELECT hp_left, max_hp FROM activity_boss_state WHERE activity_key=%s",
        (key,),
    )
    row = cur.fetchone()
    ts = now_str()
    if row is None:
        cur.execute(
            """
            INSERT INTO activity_boss_state (activity_key, hp_left, max_hp, update_time)
            VALUES (%s, %s, %s, %s)
            """,
            (key, max_hp, max_hp, ts),
        )
        return max_hp, max_hp
    hp_left = max(0, _as_int(row["hp_left"]))
    stored_max = max(1, _as_int(row["max_hp"], max_hp))
    if stored_max != max_hp:
        ratio = hp_left / stored_max if stored_max else 1
        hp_left = int(max_hp * ratio)
        cur.execute(
            "UPDATE activity_boss_state SET hp_left=%s, max_hp=%s, update_time=%s WHERE activity_key=%s",
            (hp_left, max_hp, ts, key),
        )
    return hp_left, max_hp


def grant_activity_item(activity_key: str, user_id: str, item_id: str, count: int = 1) -> None:
    if count <= 0:
        return
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO activity_item_inventory (activity_key, user_id, item_id, count, update_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(activity_key, user_id, item_id) DO UPDATE SET
                count = activity_item_inventory.count + excluded.count,
                update_time = excluded.update_time
            """,
            (str(activity_key), str(user_id), str(item_id), count, now_str()),
        )
        conn.commit()
    finally:
        conn.close()


def _today_fight_count(cur, activity_key: str, user_id: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*) AS c FROM activity_boss_fight_log
        WHERE activity_key=%s AND user_id=%s AND fight_date=%s AND source IN ('coop', 'world_boss', 'item')
        """,
        (activity_key, user_id, today_str()),
    )
    row = cur.fetchone()
    return _as_int(row["c"] if row else 0)


def _check_server_milestones(cur, activity: dict, hp_left: int, max_hp: int) -> None:
    if max_hp <= 0:
        return
    percent_left = 100.0 * hp_left / max_hp
    for ms in activity.get("server_milestones") or []:
        threshold = float(ms.get("hp_percent", 0))
        if percent_left > threshold:
            continue
        mkey = ms.get("key") or f"p{threshold}"
        cur.execute(
            """
            INSERT OR IGNORE INTO activity_boss_milestone (activity_key, milestone_key, unlocked_time)
            VALUES (%s, %s, %s)
            """,
            (activity["key"], mkey, now_str()),
        )


def _apply_damage(cur, activity: dict, user_id: str, damage: int, source: str) -> tuple[int, int, int]:
    damage = max(0, int(damage))
    hp_left, max_hp = _ensure_boss_hp(cur, activity)
    if damage <= 0:
        return 0, hp_left, max_hp

    actual = min(damage, hp_left)
    new_hp = hp_left - actual
    ts = now_str()
    cur.execute(
        "UPDATE activity_boss_state SET hp_left=%s, update_time=%s WHERE activity_key=%s",
        (new_hp, ts, activity["key"]),
    )
    cur.execute(
        """
        INSERT INTO activity_boss_damage (activity_key, user_id, total_damage, update_time)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT(activity_key, user_id) DO UPDATE SET
            total_damage = activity_boss_damage.total_damage + excluded.total_damage,
            update_time = excluded.update_time
        """,
        (activity["key"], user_id, actual, ts),
    )
    cur.execute(
        """
        INSERT INTO activity_boss_fight_log (
            activity_key, user_id, damage, fight_date, source, create_time
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (activity["key"], user_id, actual, today_str(), source, ts),
    )
    _check_server_milestones(cur, activity, new_hp, max_hp)
    return actual, new_hp, max_hp


def calc_coop_damage(activity: dict, user_id: str) -> int:
    user = _sql_message.get_user_info_with_id(user_id)
    if not user:
        return 1
    atk = max(0, _as_int(user.get("atk") or user.get("攻击"), 0))
    raw = int(atk * float(activity.get("atk_ratio", 0.1)))
    cap = int(activity["max_hp"] * float(activity.get("hit_hp_cap_ratio", 0.01)))
    if raw <= 0:
        return 1
    return max(1, min(raw, cap))


def record_cooperative_boss_hit(user_id: str, raw_damage: int | None = None) -> list[str]:
    messages: list[str] = []
    allowed, _, multiplier = _runtime_gate("boss")
    if not allowed:
        return messages
    acts = _active_boss_activities()
    if not acts:
        return messages

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        for activity in acts:
            mode = activity.get("mode", "cooperative")
            if mode not in {"cooperative", "both"}:
                continue
            if _today_fight_count(cur, activity["key"], str(user_id)) >= activity["daily_fight_limit"]:
                continue
            damage = raw_damage if raw_damage is not None else calc_coop_damage(activity, user_id)
            damage = int(damage * multiplier)
            cap = int(activity["max_hp"] * float(activity.get("hit_hp_cap_ratio", 0.01)))
            damage = min(max(1, int(damage)), cap)
            actual, hp_left, max_hp = _apply_damage(cur, activity, str(user_id), damage, "world_boss")
            if actual > 0:
                messages.append(
                    f"活动首领·{activity['boss_name']} 计入伤害 {number_to(actual)}，"
                    f"剩余 {number_to(hp_left)}/{number_to(max_hp)}"
                )
        conn.commit()
    finally:
        conn.close()
    return messages


def fight_cooperative_boss(user_id: str, query: str = "", operation_id: str | None = None) -> tuple[bool, str]:
    allowed, reason, multiplier = _runtime_gate("boss")
    if not allowed:
        return False, reason
    activity = _find_boss_activity(query)
    if not activity:
        return False, "当前没有可挑战的活动首领，或请指定首领名称"
    mode = activity.get("mode", "cooperative")
    if mode not in {"cooperative", "both"}:
        return False, "该首领请使用活动讨伐 道具名"

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        used = _today_fight_count(cur, activity["key"], str(user_id))
        limit = activity["daily_fight_limit"]
        damage = max(1, int(calc_coop_damage(activity, user_id) * multiplier))
        cap = int(activity["max_hp"] * float(activity.get("hit_hp_cap_ratio", 0.01)))
        damage = min(damage, max(1, cap))
        hp_left, max_hp = _ensure_boss_hp(cur, activity)
    finally:
        conn.close()

    result = activity_boss_coop_settlement_service.settle(
        operation_id or f"activity-boss-coop:{user_id}:{time.time_ns()}",
        str(user_id), activity["key"], hp_left, max_hp, used, limit, damage,
        today_str(), now_str(), activity.get("server_milestones") or (),
    )
    if result.status == "limit_reached":
        return False, f"今日挑战次数已用完（{limit}次）"
    if result.status == "boss_defeated":
        return False, "首领已被全服击破"
    if result.status in {"state_changed", "operation_conflict"}:
        return False, "首领状态已变化，请重试"
    if not result.succeeded:
        return False, "活动首领讨伐失败，请重试"

    name = resolve_daohao(user_id)
    cap_pct = int(float(activity.get("hit_hp_cap_ratio", 0.01)) * 100)
    pct = 100.0 * result.hp_left / result.max_hp if result.max_hp else 0
    lines = [
        f"【{activity['boss_name']}】",
        f"{name} 造成伤害 {number_to(result.damage)}（单次上限为首领血量 {cap_pct}%）",
        f"首领剩余 {number_to(result.hp_left)} / {number_to(result.max_hp)}（{pct:.2f}%）",
        f"今日剩余挑战 {max(0, limit - result.fight_count)} 次",
    ]
    if result.hp_left <= 0:
        lines.append("首领已被全服击破！可领取全服进度奖励与排行奖励。")
    return True, "\n".join(lines)


def use_item_on_boss(user_id: str, query: str, operation_id: str | None = None) -> tuple[bool, str]:
    allowed, reason, multiplier = _runtime_gate("boss")
    if not allowed:
        return False, reason
    text = _clean_text(query)
    if not text:
        return False, "请发送：活动讨伐 爆竹 或 活动讨伐 年兽 爆竹"

    parts = text.split()
    item_query = parts[-1] if len(parts) >= 2 else parts[0]
    boss_query = " ".join(parts[:-1]) if len(parts) >= 2 else ""

    activity = _find_boss_activity(boss_query)
    if not activity:
        return False, "未找到活动首领"
    mode = activity.get("mode", "item_raid")
    if mode not in {"item_raid", "both"}:
        return False, "该首领不支持道具讨伐，请用活动讨伐"

    item_def = None
    for it in activity.get("items") or []:
        if item_query in {it["id"], it["name"]}:
            item_def = it
            break
    if not item_def:
        return False, f"未找到道具【{item_query}】，请查看活动玩法说明"

    operation_id = operation_id or f"activity-boss-item:{user_id}:{time.time_ns()}"

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT count FROM activity_item_inventory
            WHERE activity_key=%s AND user_id=%s AND item_id=%s
            """,
            (activity["key"], str(user_id), item_def["id"]),
        )
        row = cur.fetchone()
        have = _as_int(row["count"] if row else 0)
        need = item_def["cost"]
        base_damage = _fixed_item_damage(
            operation_id, item_def["damage_min"], item_def["damage_max"]
        )
        damage = max(1, int(base_damage * multiplier))
        hp_left, max_hp = _ensure_boss_hp(cur, activity)
        used = _today_fight_count(cur, activity["key"], str(user_id))
        limit = activity["daily_fight_limit"]
    finally:
        conn.close()

    result = activity_boss_item_raid_settlement_service.settle(
        operation_id, str(user_id), activity["key"], item_def["id"], have, need,
        hp_left, max_hp, used, limit, damage, today_str(), now_str(),
        activity.get("server_milestones") or (),
    )
    if result.status == "item_insufficient":
        return False, f"【{item_def['name']}】不足（需要{need}，持有{result.inventory or 0}）"
    if result.status == "limit_reached":
        return False, f"今日挑战次数已用完（{limit}次）"
    if result.status == "boss_defeated":
        return False, "首领已被击退"
    if result.status in {"state_changed", "operation_conflict"}:
        return False, "首领或库存状态已变化，请重试"
    if not result.succeeded:
        return False, "活动首领讨伐失败，请重试"

    name = resolve_daohao(user_id)
    lines = [
        f"【{activity['boss_name']}】",
        f"{name} 使用【{item_def['name']}】造成 {number_to(result.damage)} 点伤害",
        f"首领剩余 {number_to(result.hp_left)} / {number_to(result.max_hp)}",
    ]
    if result.hp_left <= 0:
        lines.append("首领已被击退！全服可领取进度奖励。")
    return True, "\n".join(lines)


def build_boss_status_text(user_id: str, query: str = "") -> str:
    activity = _find_boss_activity(query)
    if not activity:
        active = _active_boss_activities()
        if not active:
            return "当前没有进行中的活动首领玩法"
        lines = ["【活动首领】"]
        for act in active:
            lines.append(_boss_status_block(act, user_id))
        return "\n\n".join(lines)
    return _boss_status_block(activity, user_id)


def _boss_status_block(activity: dict, user_id: str) -> str:
    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        hp_left, max_hp = _ensure_boss_hp(cur, activity)
        conn.commit()
        cur.execute(
            """
            SELECT total_damage FROM activity_boss_damage
            WHERE activity_key=%s AND user_id=%s
            """,
            (activity["key"], str(user_id)),
        )
        row = cur.fetchone()
        my_dmg = _as_int(row["total_damage"] if row else 0)
        used = _today_fight_count(cur, activity["key"], str(user_id))
        limit = activity["daily_fight_limit"]
        pct = 100.0 * hp_left / max_hp if max_hp else 0
        lines = [
            f"【{activity['boss_name']}】{activity.get('name', '')}",
            activity.get("description", ""),
            f"全服血量 {number_to(hp_left)} / {number_to(max_hp)}（{pct:.2f}%）",
            f"我的累计伤害 {number_to(my_dmg)}",
        ]
        if activity.get("mode") in {"cooperative", "both"}:
            lines.append(f"今日挑战 {used}/{limit}")
        items = []
        for it in activity.get("items") or []:
            cur.execute(
                """
                SELECT count FROM activity_item_inventory
                WHERE activity_key=%s AND user_id=%s AND item_id=%s
                """,
                (activity["key"], str(user_id), it["id"]),
            )
            r2 = cur.fetchone()
            cnt = _as_int(r2["count"] if r2 else 0)
            if cnt > 0:
                items.append(f"{it['name']}x{cnt}")
        if items:
            lines.append("活动道具：" + "、".join(items))
        return "\n".join(lines)
    finally:
        conn.close()


def build_boss_rank_text(query: str = "", limit: int = 10) -> str:
    activity = _find_boss_activity(query)
    if not activity:
        active = _active_boss_activities()
        activity = active[0] if len(active) == 1 else None
    if not activity:
        return "请指定活动首领名称后查询排行"

    ensure_activity_files()
    conn = db_backend.connect(DB_PATH)
    conn.row_factory = db_backend.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, total_damage
            FROM activity_boss_damage
            WHERE activity_key=%s AND total_damage>0
            ORDER BY total_damage DESC
            LIMIT %s
            """,
            (activity["key"], limit),
        )
        rows = cur.fetchall()
        lines = [f"【{activity['boss_name']}伤害排行】"]
        if not rows:
            lines.append("暂无数据")
            return "\n".join(lines)
        for i, row in enumerate(rows, 1):
            uid = str(row["user_id"])
            name = resolve_daohao(uid)
            lines.append(f"{i}. {name} 伤害 {number_to(_as_int(row['total_damage']))}")
        return "\n".join(lines)
    finally:
        conn.close()


def _boss_reward_claim_service() -> BossRewardClaimService:
    return BossRewardClaimService(DB_PATH, get_paths().game_db)


def claim_boss_milestone_reward(
    user_id: str,
    query: str = "",
    operation_id: str | None = None,
) -> tuple[bool, str]:
    service = _boss_reward_claim_service()
    if operation_id:
        previous = service.get_result(operation_id, user_id)
        if previous is not None:
            if previous.succeeded:
                return True, "已领取：" + "、".join(previous.names)
            return False, "领取请求冲突，请重新发送"
    allowed, reason, _ = _runtime_gate("claim")
    if not allowed:
        return False, reason
    activity = _find_boss_activity(query)
    if not activity:
        return False, "未找到活动首领"
    ensure_activity_files()
    milestones = [
        {"key": ms.get("key") or f"p{ms.get('hp_percent')}", "name": ms.get("name"), "reward": ms.get("reward", "")}
        for ms in activity.get("server_milestones") or []
    ]
    result = service.claim_milestones(
        user_id, activity["key"], milestones, operation_id=operation_id
    )
    if result.succeeded:
        return True, "已领取：" + "、".join(result.names)
    messages = {"not_unlocked": "全服进度奖励尚未解锁", "already_claimed": "没有可领取的全服进度奖励（可能已领过）", "inventory_full": "背包空间不足，奖励未领取", "user_missing": "角色不存在", "operation_conflict": "领取请求冲突，请重新发送"}
    return False, messages.get(result.status, "领取失败，请稍后重试")


def claim_boss_rank_reward(
    user_id: str,
    query: str = "",
    operation_id: str | None = None,
) -> tuple[bool, str]:
    service = _boss_reward_claim_service()
    if operation_id:
        previous = service.get_result(operation_id, user_id)
        if previous is not None:
            if previous.succeeded:
                return True, f"{resolve_daohao(user_id)} 第{previous.rank}名，已领取【{previous.names[0]}】"
            return False, "领取请求冲突，请重新发送"
    allowed, reason, _ = _runtime_gate("claim")
    if not allowed:
        return False, reason
    activity = _find_boss_activity(query)
    if not activity:
        return False, "未找到活动首领"
    ensure_activity_files()
    result = service.claim_rank(
        user_id,
        activity["key"],
        activity.get("rank_rewards") or [],
        operation_id=operation_id,
    )
    if result.succeeded:
        return True, f"{resolve_daohao(user_id)} 第{result.rank}名，已领取【{result.names[0]}】"
    if result.status == "not_participant":
        return False, "你尚未参与该首领讨伐，无法领取排行奖励"
    if result.status == "not_eligible":
        return False, f"你的排名为第{result.rank}名，不在奖励档位内"
    messages = {"already_claimed": "该档排行奖励已领取", "inventory_full": "背包空间不足，奖励未领取", "user_missing": "角色不存在", "operation_conflict": "领取请求冲突，请重新发送"}
    return False, messages.get(result.status, "领取失败，请稍后重试")


def claim_boss_rewards(user_id: str, query: str = "") -> tuple[bool, str]:
    text = _clean_text(query)
    if text in {"排行", "排名", "伤害榜"}:
        return claim_boss_rank_reward(user_id, "")
    if text in {"进度", "全服", "里程碑", "宝箱"}:
        return claim_boss_milestone_reward(user_id, "")
    if not text:
        ok1, m1 = claim_boss_milestone_reward(user_id, "")
        ok2, m2 = claim_boss_rank_reward(user_id, "")
        parts = []
        if ok1:
            parts.append(m1)
        if ok2:
            parts.append(m2)
        if parts:
            return True, "\n".join(parts)
        return False, f"{m1}；{m2}"
    if "排行" in text or "排名" in text:
        return claim_boss_rank_reward(user_id, text.replace("排行", "").replace("排名", "").strip())
    if "进度" in text or "全服" in text:
        return claim_boss_milestone_reward(user_id, text)
    return claim_boss_rank_reward(user_id, text)
