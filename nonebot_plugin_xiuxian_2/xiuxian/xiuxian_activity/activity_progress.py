from collections import Counter

from .activity_storage import now_str
from .activity_utils import _as_int, _clean_text


def _get_collect_pity_count(cur, activity_key: str, user_id: str, event_key: str) -> int:
    cur.execute(
        """
        SELECT miss_count
        FROM activity_collect_pity_state
        WHERE activity_key=%s AND user_id=%s AND event_key=%s
        """,
        (str(activity_key), str(user_id), str(event_key)),
    )
    row = cur.fetchone()
    return max(0, _as_int(row["miss_count"] if row else 0))


def _set_collect_pity_count(cur, activity_key: str, user_id: str, event_key: str, miss_count: int) -> None:
    cur.execute(
        """
        INSERT INTO activity_collect_pity_state (
            activity_key, user_id, event_key, miss_count, update_time
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT(activity_key, user_id, event_key) DO UPDATE SET
            miss_count = excluded.miss_count,
            update_time = excluded.update_time
        """,
        (str(activity_key), str(user_id), str(event_key), max(0, _as_int(miss_count)), now_str()),
    )


def _collect_pity_progress_map(cur, activity_key: str, user_id: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT event_key, miss_count
        FROM activity_collect_pity_state
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    return {
        str(row["event_key"]): max(0, _as_int(row["miss_count"]))
        for row in cur.fetchall()
    }


def _get_collect_inventory_map(cur, activity_key: str, user_id: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT word_char, count
        FROM activity_collect_inventory
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    return {
        str(row["word_char"]): max(0, _as_int(row["count"]))
        for row in cur.fetchall()
    }


def _get_collect_claim_map(cur, activity_key: str, user_id: str) -> dict[str, int]:
    cur.execute(
        """
        SELECT phrase, count
        FROM activity_collect_claim
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    return {
        str(row["phrase"]): max(0, _as_int(row["count"]))
        for row in cur.fetchall()
    }


def _phrase_need_counter(phrase: str) -> Counter:
    return Counter(word_char for word_char in str(phrase or "") if word_char.strip())


def _get_task_progress_map(cur, activity_key: str, user_id: str) -> dict[tuple[str, str, str], dict]:
    cur.execute(
        """
        SELECT scope_type, scope_key, task_key, progress, target, claimed, claim_time
        FROM activity_task_progress
        WHERE activity_key=%s AND user_id=%s
        """,
        (str(activity_key), str(user_id)),
    )
    result: dict[tuple[str, str, str], dict] = {}
    for row in cur.fetchall():
        key = (str(row["scope_type"]), str(row["scope_key"]), str(row["task_key"]))
        result[key] = {
            "progress": max(0, _as_int(row["progress"])),
            "target": max(1, _as_int(row["target"], 1)),
            "claimed": bool(_as_int(row["claimed"], 0)),
            "claim_time": _clean_text(row["claim_time"]),
        }
    return result


__all__ = [
    name for name in globals()
    if name.startswith("_") and not name.startswith("__")
]
