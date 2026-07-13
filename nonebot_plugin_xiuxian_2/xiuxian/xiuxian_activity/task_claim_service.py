from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ActivityTaskClaimResult:
    status: str
    rewards: tuple[tuple[str, str], ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ActivityTaskClaimService:
    """Claim activity task state and game rewards in one transaction."""

    def __init__(self, activity_database: str | Path, game_database: str | Path, lock: RLock | None = None) -> None:
        self._activity_database = Path(activity_database)
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def _reward_rows(tasks) -> tuple[int, tuple[tuple[int, str, str, int], ...]]:
        stone = 0
        items: dict[int, list] = {}
        for task in tasks:
            for reward in task[5]:
                quantity = int(reward["quantity"])
                if quantity <= 0:
                    raise ValueError("reward quantity must be positive")
                if str(reward["type"]) == "stone":
                    stone += quantity
                    continue
                item_id = int(reward["id"])
                item_type = str(reward["type"])
                if item_type in {"辅修功法", "神通", "功法", "身法", "瞳术"}:
                    item_type = "技能"
                elif item_type in {"法器", "防具"}:
                    item_type = "装备"
                metadata = [str(reward["name"]), item_type]
                if item_id in items and items[item_id][:2] != metadata:
                    raise ValueError("conflicting reward metadata")
                items.setdefault(item_id, metadata + [0])[2] += quantity
        return stone, tuple((item_id, row[0], row[1], row[2]) for item_id, row in sorted(items.items()))

    def claim(self, operation_id, user_id, activity_key, tasks, max_goods_num) -> ActivityTaskClaimResult:
        operation_id, user_id, activity_key = map(str, (operation_id, user_id, activity_key))
        max_goods_num = int(max_goods_num)
        normalized = tuple(
            (str(task[0]), str(task[1]), str(task[2]), int(task[3]), str(task[4]), tuple(task[5]), str(task[6]))
            for task in tasks
        )
        if not operation_id.strip() or not activity_key or not normalized or max_goods_num < 0:
            raise ValueError("valid task claim is required")
        stone, item_rows = self._reward_rows(normalized)
        payload = json.dumps(
            [user_id, activity_key, [(row[0], row[1], row[2], row[3], row[4], row[6]) for row in normalized], stone, item_rows, max_goods_num],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        rewards = tuple((row[6], row[4]) for row in normalized)

        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("ATTACH DATABASE %s AS game_data", (str(self._game_database),))
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS activity_task_claim_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,result_json TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,result_json FROM activity_task_claim_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return ActivityTaskClaimResult("operation_conflict")
                    return ActivityTaskClaimResult("duplicate", tuple(tuple(row) for row in json.loads(previous[1])))
                if conn.execute("SELECT 1 FROM game_data.user_xiuxian WHERE user_id=%s", (user_id,)).fetchone() is None:
                    conn.rollback()
                    return ActivityTaskClaimResult("user_missing")
                for task_key, scope_type, scope_key, target, _, _, _ in normalized:
                    row = conn.execute(
                        "SELECT progress,claimed FROM activity_task_progress WHERE activity_key=%s AND user_id=%s "
                        "AND scope_type=%s AND scope_key=%s AND task_key=%s",
                        (activity_key, user_id, scope_type, scope_key, task_key),
                    ).fetchone()
                    if row is None or int(row[1]) or int(row[0]) < target:
                        conn.rollback()
                        return ActivityTaskClaimResult("state_changed")
                for item_id, _, _, quantity in item_rows:
                    row = conn.execute(
                        "SELECT COALESCE(goods_num,0) FROM game_data.back WHERE user_id=%s AND goods_id=%s",
                        (user_id, item_id),
                    ).fetchone()
                    if (int(row[0]) if row else 0) + quantity > max_goods_num:
                        conn.rollback()
                        return ActivityTaskClaimResult("inventory_full")

                now = datetime.now()
                for task_key, scope_type, scope_key, target, reward_text, _, _ in normalized:
                    changed = conn.execute(
                        "UPDATE activity_task_progress SET claimed=1,claim_time=%s,update_time=%s,target=%s "
                        "WHERE activity_key=%s AND user_id=%s AND scope_type=%s AND scope_key=%s AND task_key=%s "
                        "AND claimed=0 AND progress>=%s",
                        (now, now, target, activity_key, user_id, scope_type, scope_key, task_key, target),
                    )
                    if changed.rowcount != 1:
                        raise RuntimeError("activity task state changed")
                    conn.execute(
                        "INSERT INTO activity_task_claim_log(activity_key,user_id,scope_type,scope_key,task_key,reward,create_time) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s)",
                        (activity_key, user_id, scope_type, scope_key, task_key, reward_text, now),
                    )
                if stone:
                    conn.execute("UPDATE game_data.user_xiuxian SET stone=COALESCE(stone,0)+%s WHERE user_id=%s", (stone, user_id))
                for item_id, name, item_type, quantity in item_rows:
                    conn.execute(
                        "INSERT INTO game_data.back(user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(user_id,goods_id) DO UPDATE SET "
                        "goods_name=excluded.goods_name,goods_type=excluded.goods_type,goods_num=back.goods_num+excluded.goods_num,"
                        "bind_num=COALESCE(back.bind_num,0)+excluded.bind_num,update_time=excluded.update_time",
                        (user_id, item_id, name, item_type, quantity, now, now, quantity),
                    )
                result_json = json.dumps(rewards, ensure_ascii=True, separators=(",", ":"))
                conn.execute(
                    "INSERT INTO activity_task_claim_operations(operation_id,payload,result_json) VALUES(%s,%s,%s)",
                    (operation_id, payload, result_json),
                )
                conn.commit()
                return ActivityTaskClaimResult("applied", rewards)
            except Exception:
                conn.rollback()
                raise


__all__ = ["ActivityTaskClaimResult", "ActivityTaskClaimService"]
