from .activity_config import _activity_config_key, _activity_elapsed_days, activity_runtime_state
from .activity_rules import _activity_pass_config
from .activity_storage import now_str, today_str
from .activity_utils import _as_float, _as_int


def _calc_pass_level(total_exp: int, level_exp: int, max_level: int) -> int:
    return min(max(0, _as_int(total_exp, 0)) // max(1, level_exp), max(1, max_level))


def _pass_current_exp(total_exp: int, level: int, level_exp: int, max_level: int) -> int:
    if level >= max_level:
        return level_exp
    return max(0, total_exp - level * level_exp)


def _get_pass_balance(cur, activity_key: str, user_id: str, pass_cfg: dict | None = None) -> dict:
    cfg = pass_cfg or _activity_pass_config()
    level_exp = max(1, _as_int(cfg.get("level_exp"), 100))
    max_level = max(1, _as_int(cfg.get("max_level"), 12))
    cur.execute(
        """
        SELECT exp, total_exp, level
        FROM activity_pass_balance
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    row = cur.fetchone()
    total_exp = max(0, _as_int(row["total_exp"] if row else 0))
    level = _calc_pass_level(total_exp, level_exp, max_level)
    current_exp = _pass_current_exp(total_exp, level, level_exp, max_level)
    return {
        "exp": current_exp,
        "total_exp": total_exp,
        "level": level,
        "level_exp": level_exp,
        "max_level": max_level,
    }


def _grant_pass_exp(cur, activity_key: str, user_id: str, pass_cfg: dict, gained_exp: int) -> tuple[dict, dict]:
    before = _get_pass_balance(cur, activity_key, user_id, pass_cfg)
    total_exp = before["total_exp"] + max(0, _as_int(gained_exp, 0))
    level_exp = before["level_exp"]
    max_level = before["max_level"]
    level = _calc_pass_level(total_exp, level_exp, max_level)
    current_exp = _pass_current_exp(total_exp, level, level_exp, max_level)
    ts = now_str()
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
        (activity_key, user_id, current_exp, total_exp, level, ts),
    )
    return before, {
        "exp": current_exp,
        "total_exp": total_exp,
        "level": level,
        "level_exp": level_exp,
        "max_level": max_level,
    }


def _pass_catchup_state(cur, config: dict, activity_key: str, user_id: str, pass_cfg: dict) -> dict:
    enabled = bool(pass_cfg.get("catchup_enabled"))
    start_day = max(1, _as_int(pass_cfg.get("catchup_start_day"), 5))
    level_gap = max(1, _as_int(pass_cfg.get("catchup_level_gap"), 3))
    catchup_multiplier = max(1.0, _as_float(pass_cfg.get("catchup_multiplier"), 1.5))
    elapsed_day = _activity_elapsed_days(config)
    balance = _get_pass_balance(cur, activity_key, user_id, pass_cfg)
    cur.execute(
        """
        SELECT COALESCE(MAX(level), 0) AS level
        FROM activity_pass_balance
        WHERE activity_key=%s
        """,
        (activity_key,),
    )
    row = cur.fetchone()
    highest_level = max(0, _as_int(row["level"] if row else 0))
    gap = max(0, highest_level - balance["level"])
    active = enabled and elapsed_day >= start_day and gap >= level_gap and catchup_multiplier > 1.0
    return {
        "enabled": enabled,
        "active": active,
        "elapsed_day": elapsed_day,
        "start_day": start_day,
        "level_gap": level_gap,
        "catchup_multiplier": catchup_multiplier,
        "highest_level": highest_level,
        "user_level": balance["level"],
        "gap": gap,
        "multiplier": catchup_multiplier if active else 1.0,
    }


def _record_activity_pass_progress(
    cur,
    config: dict,
    user_id: str,
    event_key: str,
    amount: int,
    messages: list[str],
) -> None:
    runtime = activity_runtime_state(config)
    if not runtime.get("ok") or "pass" not in set(runtime.get("features") or []):
        return
    pass_cfg = _activity_pass_config(config)
    if not pass_cfg.get("enabled"):
        return
    activity_key = _activity_config_key(config)
    exp_name = pass_cfg.get("exp_name") or "活跃值"
    multiplier = max(0.0, _as_float(runtime.get("multiplier"), 1.0))
    catchup = _pass_catchup_state(cur, config, activity_key, user_id, pass_cfg)
    multiplier *= max(1.0, _as_float(catchup.get("multiplier"), 1.0))
    for rule in pass_cfg.get("event_rules") or []:
        if rule.get("event") != event_key:
            continue
        exp = int(max(0, _as_int(rule.get("exp"), 0)) * max(1, amount) * multiplier)
        if exp <= 0:
            continue
        daily_limit = max(0, _as_int(rule.get("daily_limit"), 0))
        if daily_limit > 0:
            cur.execute(
                """
                SELECT COALESCE(SUM(exp), 0) AS count
                FROM activity_pass_event_log
                WHERE activity_key=%s AND user_id=%s AND event_key=%s AND record_date=%s
                """,
                (activity_key, user_id, event_key, today_str()),
            )
            row = cur.fetchone()
            current_exp = _as_int(row["count"] if row else 0)
            remaining = daily_limit - current_exp
            if remaining <= 0:
                continue
            exp = min(exp, remaining)
        if exp <= 0:
            continue
        ts = now_str()
        cur.execute(
            """
            INSERT INTO activity_pass_event_log (
                activity_key, user_id, event_key, exp, record_date, create_time
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (activity_key, user_id, event_key, exp, today_str(), ts),
        )
        before, after = _grant_pass_exp(cur, activity_key, user_id, pass_cfg, exp)
        catchup_text = "（追赶加成）" if catchup.get("active") else ""
        if after["level"] > before["level"]:
            messages.append(
                f"活动战令：获得{exp}{exp_name}{catchup_text}，提升至{after['level']}级，发送 活动战令领取 领奖"
            )
        else:
            messages.append(
                f"活动战令：获得{exp}{exp_name}{catchup_text}（{after['exp']}/{after['level_exp']}）"
            )


__all__ = [
    name for name in globals()
    if name.startswith("_") and not name.startswith("__")
]
