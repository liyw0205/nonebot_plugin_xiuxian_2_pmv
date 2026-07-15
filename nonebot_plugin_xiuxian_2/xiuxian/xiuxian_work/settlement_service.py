from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class WorkSettlementResult:
    status: str
    exp: int
    item_awarded: bool
    success_kind: str = ""
    item_msg: str = ""
    scheduled_time: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class WorkSettlementService:
    """Atomically settle an accepted work order and its rewards."""

    def __init__(self, game_database: str | Path, lock: RLock | None = None) -> None:
        self._game_database = Path(game_database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id) -> str:
        # Request identity only — work snapshot and random rewards are outcomes/concurrency checks.
        return json.dumps([str(user_id)], ensure_ascii=True, separators=(",", ":"))

    def _ensure_ops(self, conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS work_settlement_operations ("
            "operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, exp INTEGER NOT NULL, "
            "item_awarded INTEGER NOT NULL, result_json TEXT, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(work_settlement_operations)").fetchall()}
        if "result_json" not in cols:
            try:
                conn.execute("ALTER TABLE work_settlement_operations ADD COLUMN result_json TEXT")
            except Exception:
                pass

    def _from_row(self, status: str, exp: int, item_awarded: bool, result_json: str | None) -> WorkSettlementResult:
        success_kind = item_msg = scheduled_time = ""
        if result_json:
            try:
                data = json.loads(result_json)
                success_kind = str(data.get("success_kind") or "")
                item_msg = str(data.get("item_msg") or "")
                scheduled_time = str(data.get("scheduled_time") or "")
            except Exception:
                pass
        return WorkSettlementResult(
            status,
            int(exp),
            bool(item_awarded),
            success_kind=success_kind,
            item_msg=item_msg,
            scheduled_time=scheduled_time,
        )

    def get_result(self, operation_id: str) -> WorkSettlementResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            self._ensure_ops(conn)
            previous = conn.execute(
                "SELECT payload, exp, item_awarded, result_json FROM work_settlement_operations WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return self._from_row("duplicate", int(previous[1]), bool(previous[2]), previous[3])

    def settle(
        self,
        operation_id,
        user_id,
        expected_work,
        exp_gain,
        item,
        max_exp,
        max_goods_num,
        *,
        success_kind: str = "",
        item_msg: str = "",
    ) -> WorkSettlementResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected = dict(expected_work)
        exp_gain, max_exp, max_goods_num = map(int, (exp_gain, max_exp, max_goods_num))
        reward = (int(item["id"]), str(item["name"]), str(item["type"])) if item else None
        if not operation_id or min(exp_gain, max_exp, max_goods_num) < 0 or not expected.get("scheduled_time"):
            raise ValueError("valid operation, work state and rewards are required")
        payload = self._payload(user_id)

        with self._lock, closing(db_backend.connect(self._game_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_ops(conn)
                previous = conn.execute(
                    "SELECT payload, exp, item_awarded, result_json FROM work_settlement_operations WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorkSettlementResult("state_changed", 0, False)
                    return self._from_row("duplicate", int(previous[1]), bool(previous[2]), previous[3])

                user = conn.execute("SELECT COALESCE(exp, 0) FROM user_xiuxian WHERE user_id=%s", (user_id,)).fetchone()
                work = conn.execute(
                    "SELECT type, create_time, scheduled_time FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None:
                    conn.rollback()
                    return WorkSettlementResult("user_missing", 0, False)
                if work is None or int(work[0] or 0) != 2 or tuple(map(str, work[1:])) != (
                    str(expected.get("create_time")),
                    str(expected.get("scheduled_time")),
                ):
                    conn.rollback()
                    return WorkSettlementResult("state_changed", 0, False)

                applied_exp = max(0, min(exp_gain, max_exp - int(user[0] or 0)))
                if reward is not None:
                    current_item = conn.execute(
                        "SELECT COALESCE(goods_num, 0) FROM back WHERE user_id=%s AND goods_id=%s",
                        (user_id, reward[0]),
                    ).fetchone()
                    if (int(current_item[0]) if current_item else 0) + 1 > max_goods_num:
                        conn.rollback()
                        return WorkSettlementResult("inventory_full", 0, False)

                conn.execute("UPDATE user_xiuxian SET exp=exp+%s WHERE user_id=%s", (applied_exp, user_id))
                conn.execute(
                    "UPDATE user_cd SET type=%s, create_time=%s, scheduled_time=%s WHERE user_id=%s",
                    (0, 0, None, user_id),
                )
                if reward is not None:
                    now = datetime.now()
                    conn.execute(
                        "INSERT INTO back (user_id,goods_id,goods_name,goods_type,goods_num,create_time,update_time,bind_num) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=EXCLUDED.goods_name, "
                        "goods_type=EXCLUDED.goods_type, goods_num=back.goods_num+EXCLUDED.goods_num, "
                        "bind_num=COALESCE(back.bind_num,0)+EXCLUDED.bind_num, update_time=EXCLUDED.update_time",
                        (user_id, reward[0], reward[1], reward[2], 1, now, now, 1),
                    )
                result_json = json.dumps(
                    {
                        "success_kind": str(success_kind or ""),
                        "item_msg": str(item_msg or ""),
                        "scheduled_time": str(expected.get("scheduled_time") or ""),
                    },
                    ensure_ascii=True,
                    separators=(",", ":"),
                )
                conn.execute(
                    "INSERT INTO work_settlement_operations (operation_id,payload,exp,item_awarded,result_json) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (operation_id, payload, applied_exp, int(reward is not None), result_json),
                )
                conn.commit()
                return WorkSettlementResult(
                    "applied",
                    applied_exp,
                    reward is not None,
                    success_kind=str(success_kind or ""),
                    item_msg=str(item_msg or ""),
                    scheduled_time=str(expected.get("scheduled_time") or ""),
                )
            except Exception:
                conn.rollback()
                raise


__all__ = ["WorkSettlementResult", "WorkSettlementService"]
