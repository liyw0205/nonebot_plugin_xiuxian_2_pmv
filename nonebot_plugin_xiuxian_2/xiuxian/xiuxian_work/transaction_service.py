from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from ..xiuxian_utils import db_backend
from datetime import datetime
from datetime import date, datetime

@dataclass(frozen=True)
class WorkClaimResult:
    status: str
    task_name: str | None = None
    started_at: str | None = None
    remaining_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class WorkClaimService:
    """Atomically claim one work offer and persist its immutable snapshot."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, task_index) -> str:
        # Request identity only — count/offer/start time are concurrency checks or outcomes.
        return json.dumps([str(user_id), int(task_index)], ensure_ascii=True, separators=(",", ":"))

    def get_result(self, operation_id: str) -> WorkClaimResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS work_claim_operations ("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_name TEXT NOT NULL,"
                "started_at TEXT NOT NULL,remaining_count INTEGER NOT NULL,"
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,task_name,started_at,remaining_count FROM work_claim_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return WorkClaimResult("duplicate", str(previous[1]), str(previous[2]), int(previous[3]))

    def claim(
        self,
        operation_id,
        user_id,
        expected_count,
        expected_offer,
        task_index,
        started_at,
    ) -> WorkClaimResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        expected_count = int(expected_count)
        task_index = int(task_index)
        started_at = str(started_at)
        offer = dict(expected_offer)
        tasks = list(dict(offer.get("tasks") or {}).items())
        # 优先使用刷新时固化的 task_order，避免 sort_keys 后编号错位
        order = offer.get("task_order")
        if isinstance(order, list) and order:
            task_map = dict(tasks)
            ordered = []
            for name in order:
                key = str(name)
                if key in task_map:
                    ordered.append((key, task_map[key]))
            for name, data in tasks:
                if name not in {n for n, _ in ordered}:
                    ordered.append((name, data))
            tasks = ordered
        if not operation_id:
            raise ValueError("operation_id is required")
        # 次数用尽 / 编号越界：返回状态，不要抛异常把 Matcher 打成 ERROR
        if expected_count <= 0:
            return WorkClaimResult("count_insufficient")
        if task_index < 1 or task_index > len(tasks):
            return WorkClaimResult("invalid_task")
        task_name, task_data = tasks[task_index - 1]
        snapshot = {
            "tasks": offer["tasks"],
            "task_order": [name for name, _ in tasks],
            "status": 2,
            "refresh_time": offer.get("refresh_time"),
            "user_level": offer.get("user_level"),
            "selected_task": task_name,
            "selected_task_data": task_data,
        }
        payload = self._payload(user_id, task_index)
        snapshot_json = json.dumps(snapshot, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_claim_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,task_name TEXT NOT NULL,"
                    "started_at TEXT NOT NULL,remaining_count INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_active_snapshots ("
                    "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                previous = conn.execute(
                    "SELECT payload,task_name,started_at,remaining_count FROM work_claim_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorkClaimResult("operation_conflict")
                    return WorkClaimResult("duplicate", str(previous[1]), str(previous[2]), int(previous[3]))

                user = conn.execute(
                    "SELECT COALESCE(work_num,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                work = conn.execute(
                    "SELECT COALESCE(type,0),create_time,scheduled_time FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None or work is None:
                    conn.rollback()
                    return WorkClaimResult("user_missing")
                if int(user[0]) != expected_count or int(work[0]) != 0:
                    conn.rollback()
                    return WorkClaimResult("state_changed")

                remaining = expected_count - 1
                # 同一 user_id 若有重复行，更新多行也算成功
                conn.execute(
                    "UPDATE user_xiuxian SET work_num=%s WHERE user_id=%s "
                    "AND CAST(COALESCE(work_num,0) AS INTEGER)=%s",
                    (remaining, user_id, expected_count),
                )
                conn.execute(
                    "UPDATE user_cd SET type=2,create_time=%s,scheduled_time=%s WHERE user_id=%s AND COALESCE(type,0)=0",
                    (started_at, task_name, user_id),
                )
                conn.execute(
                    "INSERT INTO work_active_snapshots(user_id,snapshot,updated_at) VALUES(%s,%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET snapshot=EXCLUDED.snapshot,updated_at=EXCLUDED.updated_at",
                    (user_id, snapshot_json, started_at),
                )
                conn.execute(
                    "INSERT INTO work_claim_operations(operation_id,payload,task_name,started_at,remaining_count) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, task_name, started_at, remaining),
                )
                conn.commit()
                return WorkClaimResult("applied", task_name, started_at, remaining)
            except Exception:
                conn.rollback()
                raise

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
                from ..xiuxian_utils.cd_time import cd_time_matches

                if work is None or int(work[0] or 0) != 2:
                    conn.rollback()
                    return WorkSettlementResult("state_changed", 0, False)
                # scheduled_time 是任务名，必须一致；create_time 坏值不拦
                if str(work[2] or "") != str(expected.get("scheduled_time") or ""):
                    conn.rollback()
                    return WorkSettlementResult("state_changed", 0, False)
                if not cd_time_matches(work[1], expected.get("create_time")):
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

                conn.execute("UPDATE user_xiuxian SET exp=CAST(COALESCE(exp,0) AS REAL)+CAST(%s AS REAL) WHERE user_id=%s", (applied_exp, user_id))
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

@dataclass(frozen=True)
class WorkItemUseResult:
    status: str
    action: str | None = None
    item_remaining: int = 0
    result_snapshot: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class WorkItemUseService:
    """Consume work items together with the work-state transition they cause."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def accelerate(
        self, operation_id, user_id, item_id, expected_item_count, expected_work, accelerated_at
    ) -> WorkItemUseResult:
        return self._apply(
            operation_id, user_id, item_id, expected_item_count, "accelerate",
            dict(expected_work), {"accelerated_at": str(accelerated_at)},
        )

    def capture(
        self, operation_id, user_id, item_id, expected_item_count, expected_work_type, new_offer
    ) -> WorkItemUseResult:
        return self._apply(
            operation_id, user_id, item_id, expected_item_count, "capture",
            {"type": int(expected_work_type)}, {"offer": dict(new_offer)},
        )

    def _apply(self, operation_id, user_id, item_id, expected_item_count, action, expected, result):
        operation_id = str(operation_id).strip()
        user_id, item_id = str(user_id), int(item_id)
        expected_item_count = int(expected_item_count)
        if not operation_id or expected_item_count <= 0 or action not in {"accelerate", "capture"}:
            raise ValueError("valid operation, item snapshot and action are required")
        payload = json.dumps(
            [user_id, item_id, expected_item_count, action, expected, result],
            ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str,
        )
        result_json = json.dumps(result, ensure_ascii=True, sort_keys=True, separators=(",", ":"))

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_item_use_operations ("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,action TEXT NOT NULL,"
                    "item_remaining INTEGER NOT NULL,result_snapshot TEXT NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,action,item_remaining,result_snapshot FROM work_item_use_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorkItemUseResult("operation_conflict")
                    return WorkItemUseResult(
                        "duplicate", str(previous[1]), int(previous[2]), json.loads(str(previous[3]))
                    )

                item = conn.execute(
                    "SELECT COALESCE(goods_num,0),COALESCE(bind_num,0) FROM back "
                    "WHERE user_id=%s AND goods_id=%s", (user_id, item_id),
                ).fetchone()
                work = conn.execute(
                    "SELECT COALESCE(type,0),create_time,scheduled_time FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if work is None:
                    conn.rollback()
                    return WorkItemUseResult("user_missing")
                if item is None or int(item[0]) < 1:
                    conn.rollback()
                    return WorkItemUseResult("item_missing")
                if int(item[0]) != expected_item_count:
                    conn.rollback()
                    return WorkItemUseResult("state_changed")

                if action == "accelerate":
                    actual_work = {
                        "type": int(work[0]), "create_time": str(work[1]),
                        "scheduled_time": str(work[2]),
                    }
                    normalized_expected = {
                        "type": int(expected.get("type", 0)),
                        "create_time": str(expected.get("create_time")),
                        "scheduled_time": str(expected.get("scheduled_time")),
                    }
                    if actual_work != normalized_expected or actual_work["type"] != 2:
                        conn.rollback()
                        return WorkItemUseResult("state_changed")
                    conn.execute(
                        "UPDATE user_cd SET create_time=%s WHERE user_id=%s AND type=2",
                        (result["accelerated_at"], user_id),
                    )
                else:
                    if int(work[0]) != int(expected["type"]) or int(work[0]) != 0:
                        conn.rollback()
                        return WorkItemUseResult("state_changed")
                    conn.execute(
                        "CREATE TABLE IF NOT EXISTS work_offer_snapshots ("
                        "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                    )
                    offer = dict(result["offer"])
                    conn.execute(
                        "INSERT INTO work_offer_snapshots(user_id,snapshot,updated_at) VALUES(%s,%s,%s) "
                        "ON CONFLICT(user_id) DO UPDATE SET snapshot=EXCLUDED.snapshot,updated_at=EXCLUDED.updated_at",
                        (user_id, json.dumps(offer, ensure_ascii=True, sort_keys=True), str(offer.get("refresh_time", ""))),
                    )

                remaining = expected_item_count - 1
                bind_remaining = min(max(0, int(item[1]) - 1), remaining)
                conn.execute(
                    "UPDATE back SET goods_num=%s,bind_num=%s WHERE user_id=%s AND goods_id=%s AND goods_num=%s",
                    (remaining, bind_remaining, user_id, item_id, expected_item_count),
                )
                conn.execute(
                    "INSERT INTO work_item_use_operations(operation_id,payload,action,item_remaining,result_snapshot) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, action, remaining, result_json),
                )
                conn.commit()
                return WorkItemUseResult("applied", action, remaining, result)
            except Exception:
                conn.rollback()
                raise

def _dump(value) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)

@dataclass(frozen=True)
class WorkRefreshResult:
    status: str
    remaining_count: int = 0
    offer: dict | None = None

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class WorkRefreshSettlementService:
    """Replace a work offer and consume one refresh count in one transaction."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _payload(user_id, force: bool) -> str:
        # Request identity only — count/cd/offer blobs are concurrency checks or outcomes.
        return json.dumps([str(user_id), bool(force)], ensure_ascii=True, separators=(",", ":"))

    def get_result(self, operation_id: str) -> WorkRefreshResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id:
            return None
        with self._lock, closing(db_backend.connect(self._database)) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS work_refresh_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,remaining_count INTEGER NOT NULL,"
                "offer_snapshot TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            previous = conn.execute(
                "SELECT payload,remaining_count,offer_snapshot FROM work_refresh_operations "
                "WHERE operation_id=%s",
                (operation_id,),
            ).fetchone()
            if previous is None:
                return None
            return WorkRefreshResult("duplicate", int(previous[1]), json.loads(str(previous[2])))

    def refresh(
        self,
        operation_id,
        user_id,
        expected_count,
        expected_cd,
        expected_offer,
        new_offer,
        force=False,
    ) -> WorkRefreshResult:
        operation_id, user_id = str(operation_id).strip(), str(user_id)
        expected_count = int(expected_count)
        expected_cd = dict(expected_cd or {})
        expected_offer = dict(expected_offer) if expected_offer else None
        new_offer = dict(new_offer)
        force = bool(force)
        if not operation_id or expected_count <= 0 or not new_offer.get("tasks"):
            raise ValueError("valid operation, refresh count and fixed offer are required")
        payload = self._payload(user_id, force)
        offer_json = _dump(new_offer)

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_offer_snapshots("
                    "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_refresh_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,remaining_count INTEGER NOT NULL,"
                    "offer_snapshot TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,remaining_count,offer_snapshot FROM work_refresh_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorkRefreshResult("operation_conflict")
                    return WorkRefreshResult("duplicate", int(previous[1]), json.loads(str(previous[2])))

                user = conn.execute(
                    "SELECT COALESCE(work_num,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                cd = conn.execute(
                    "SELECT COALESCE(type,0),create_time,scheduled_time FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                if user is None or cd is None:
                    conn.rollback()
                    return WorkRefreshResult("user_missing")

                def _blank(v):
                    if v is None:
                        return None
                    s = str(v).strip()
                    return None if s == "" or s.lower() in {"none", "null"} else v

                actual_cd = {
                    "type": int(cd[0] or 0),
                    "create_time": _blank(cd[1]),
                    "scheduled_time": _blank(cd[2]),
                }
                normalized_cd = {
                    "type": int(expected_cd.get("type", 0) or 0),
                    "create_time": _blank(expected_cd.get("create_time")),
                    "scheduled_time": _blank(expected_cd.get("scheduled_time")),
                }
                # work_num may be TEXT historically; coerce both sides
                if int(float(user[0] or 0)) != expected_count or actual_cd != normalized_cd or actual_cd["type"] != 0:
                    conn.rollback()
                    return WorkRefreshResult("state_changed")

                stored = conn.execute(
                    "SELECT snapshot FROM work_offer_snapshots WHERE user_id=%s", (user_id,)
                ).fetchone()
                stored_offer = json.loads(str(stored[0])) if stored else None
                if stored_offer is not None and stored_offer != expected_offer:
                    conn.rollback()
                    return WorkRefreshResult("state_changed")
                if not force and expected_offer and int(expected_offer.get("status", 1)) == 1:
                    conn.rollback()
                    return WorkRefreshResult("offer_exists")

                remaining = expected_count - 1
                # 同一 user_id 若有重复 user_xiuxian 行，会更新多行；rowcount>=1 即成功
                changed = conn.execute(
                    "UPDATE user_xiuxian SET work_num=%s WHERE user_id=%s "
                    "AND CAST(COALESCE(work_num,0) AS INTEGER)=%s",
                    (remaining, user_id, expected_count),
                )
                if changed.rowcount < 1:
                    conn.rollback()
                    return WorkRefreshResult("state_changed")
                conn.execute(
                    "INSERT INTO work_offer_snapshots(user_id,snapshot,updated_at) VALUES(%s,%s,%s) "
                    "ON CONFLICT(user_id) DO UPDATE SET snapshot=EXCLUDED.snapshot,updated_at=EXCLUDED.updated_at",
                    (user_id, offer_json, str(new_offer.get("refresh_time", ""))),
                )
                conn.execute(
                    "INSERT INTO work_refresh_operations(operation_id,payload,remaining_count,offer_snapshot) "
                    "VALUES(%s,%s,%s,%s)",
                    (operation_id, payload, remaining, offer_json),
                )
                conn.commit()
                return WorkRefreshResult("applied", remaining, new_offer)
            except Exception:
                conn.rollback()
                raise

def _dump(value) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)

@dataclass(frozen=True)
class WorkAbortCleanupResult:
    status: str
    penalty: int = 0
    stone_remaining: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class WorkAbortCleanupService:
    """Close active or offered work together with its CD, assets and snapshots."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    def cleanup(
        self,
        operation_id,
        user_id,
        reason,
        expected_cd,
        expected_offer=None,
        expected_stone=None,
        penalty=0,
    ) -> WorkAbortCleanupResult:
        operation_id, user_id, reason = str(operation_id).strip(), str(user_id), str(reason).strip()
        expected_cd = dict(expected_cd or {})
        expected_offer = dict(expected_offer) if expected_offer else None
        penalty = int(penalty)
        expected_stone = None if expected_stone is None else int(expected_stone)
        if not operation_id or reason not in {"active_abort", "offer_abort", "expired", "reset"} or penalty < 0:
            raise ValueError("invalid work cleanup request")
        if reason == "active_abort" and expected_stone is None:
            raise ValueError("active abort requires a stone snapshot")
        payload = _dump([user_id, reason, expected_cd, expected_offer, expected_stone, penalty])

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_offer_snapshots("
                    "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_active_snapshots("
                    "user_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,updated_at TEXT NOT NULL)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS work_abort_cleanup_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,reason TEXT NOT NULL,"
                    "penalty INTEGER NOT NULL,stone_remaining INTEGER NOT NULL,"
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                )
                previous = conn.execute(
                    "SELECT payload,penalty,stone_remaining FROM work_abort_cleanup_operations "
                    "WHERE operation_id=%s", (operation_id,),
                ).fetchone()
                if previous is not None:
                    conn.rollback()
                    if str(previous[0]) != payload:
                        return WorkAbortCleanupResult("operation_conflict")
                    return WorkAbortCleanupResult("duplicate", int(previous[1]), int(previous[2]))

                cd = conn.execute(
                    "SELECT COALESCE(type,0),create_time,scheduled_time FROM user_cd WHERE user_id=%s",
                    (user_id,),
                ).fetchone()
                user = conn.execute(
                    "SELECT COALESCE(stone,0) FROM user_xiuxian WHERE user_id=%s", (user_id,)
                ).fetchone()
                if cd is None or user is None:
                    conn.rollback()
                    return WorkAbortCleanupResult("user_missing")
                actual_cd = {"type": int(cd[0]), "create_time": cd[1], "scheduled_time": cd[2]}
                normalized_cd = {
                    "type": int(expected_cd.get("type", 0)),
                    "create_time": expected_cd.get("create_time"),
                    "scheduled_time": expected_cd.get("scheduled_time"),
                }
                if actual_cd != normalized_cd:
                    conn.rollback()
                    return WorkAbortCleanupResult("state_changed")
                if reason == "active_abort" and actual_cd["type"] != 2:
                    conn.rollback()
                    return WorkAbortCleanupResult("state_changed")

                stored = conn.execute(
                    "SELECT snapshot FROM work_offer_snapshots WHERE user_id=%s", (user_id,)
                ).fetchone()
                stored_offer = json.loads(str(stored[0])) if stored else None
                if stored_offer is not None and stored_offer != expected_offer:
                    conn.rollback()
                    return WorkAbortCleanupResult("state_changed")

                stone = int(user[0])
                if expected_stone is not None and stone != expected_stone:
                    conn.rollback()
                    return WorkAbortCleanupResult("state_changed")
                applied_penalty = min(penalty, stone) if reason == "active_abort" else 0
                remaining = stone - applied_penalty
                if applied_penalty:
                    # 重复 user_id 行：CAST 比较 + rowcount>=1
                    changed = conn.execute(
                        "UPDATE user_xiuxian SET stone=%s WHERE user_id=%s "
                        "AND CAST(COALESCE(stone,0) AS REAL)=CAST(%s AS REAL)",
                        (remaining, user_id, stone),
                    )
                    if changed.rowcount < 1:
                        conn.rollback()
                        return WorkAbortCleanupResult("state_changed")
                conn.execute(
                    "UPDATE user_cd SET type=0,create_time=0,scheduled_time=NULL WHERE user_id=%s", (user_id,)
                )
                conn.execute("DELETE FROM work_offer_snapshots WHERE user_id=%s", (user_id,))
                conn.execute("DELETE FROM work_active_snapshots WHERE user_id=%s", (user_id,))
                conn.execute(
                    "INSERT INTO work_abort_cleanup_operations(operation_id,payload,reason,penalty,stone_remaining) "
                    "VALUES(%s,%s,%s,%s,%s)",
                    (operation_id, payload, reason, applied_penalty, remaining),
                )
                conn.commit()
                return WorkAbortCleanupResult("applied", applied_penalty, remaining)
            except Exception:
                conn.rollback()
                raise

@dataclass(frozen=True)
class WorkDailyRefreshResetResult:
    status: str
    business_date: str
    task_status: str = ""
    reset_count: int = 0
    total: int = 0
    completed: int = 0
    changed: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}

class WorkDailyRefreshResetService:
    """Reset a date-frozen player set in durable chunks."""

    def __init__(self, database: str | Path, lock: RLock | None = None) -> None:
        self._database = Path(database)
        self._lock = lock or RLock()

    @staticmethod
    def _normalize_date(value) -> str:
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.isoformat()
        return date.fromisoformat(str(value).strip()).isoformat()

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS work_daily_refresh_reset_operations("
            "business_date TEXT PRIMARY KEY,reset_count INTEGER NOT NULL,total INTEGER NOT NULL,"
            "completed INTEGER NOT NULL DEFAULT 0,changed INTEGER NOT NULL DEFAULT 0,"
            "status TEXT NOT NULL DEFAULT 'running',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS work_daily_refresh_reset_targets("
            "business_date TEXT NOT NULL,user_id TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
            "previous_count INTEGER,final_count INTEGER,updated_at TEXT NOT NULL,"
            "PRIMARY KEY(business_date,user_id))"
        )

    @staticmethod
    def _result(conn, business_date, status):
        row = conn.execute(
            "SELECT reset_count,total,completed,changed,status "
            "FROM work_daily_refresh_reset_operations WHERE business_date=%s",
            (business_date,),
        ).fetchone()
        if row is None:
            return WorkDailyRefreshResetResult(status, business_date)
        return WorkDailyRefreshResetResult(
            status,
            business_date,
            str(row[4]),
            int(row[0]),
            int(row[1]),
            int(row[2]),
            int(row[3]),
        )

    def reset(
        self,
        business_date,
        reset_count,
        *,
        chunk_size=500,
        updated_at=None,
    ) -> WorkDailyRefreshResetResult:
        business_date = self._normalize_date(business_date)
        reset_count = int(reset_count)
        chunk_size = max(1, int(chunk_size))
        updated_at = str(updated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if reset_count < 0:
            raise ValueError("reset count must not be negative")

        with self._lock, closing(db_backend.connect(self._database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                operation = conn.execute(
                    "SELECT reset_count,status FROM work_daily_refresh_reset_operations "
                    "WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                if operation is None:
                    user_ids = tuple(
                        str(row[0])
                        for row in conn.execute(
                            "SELECT DISTINCT user_id FROM user_xiuxian ORDER BY user_id"
                        ).fetchall()
                    )
                    task_status = "completed" if not user_ids else "running"
                    conn.execute(
                        "INSERT INTO work_daily_refresh_reset_operations("
                        "business_date,reset_count,total,status,created_at,updated_at) "
                        "VALUES(%s,%s,%s,%s,%s,%s)",
                        (
                            business_date,
                            reset_count,
                            len(user_ids),
                            task_status,
                            updated_at,
                            updated_at,
                        ),
                    )
                    conn.executemany(
                        "INSERT INTO work_daily_refresh_reset_targets("
                        "business_date,user_id,updated_at) VALUES(%s,%s,%s)",
                        (
                            (business_date, user_id, updated_at)
                            for user_id in user_ids
                        ),
                    )
                    conn.commit()
                    if not user_ids:
                        return self._result(conn, business_date, "applied")
                else:
                    if int(operation[0]) != reset_count:
                        result = self._result(conn, business_date, "operation_conflict")
                        conn.rollback()
                        return result
                    if str(operation[1]) == "completed":
                        result = self._result(conn, business_date, "duplicate")
                        conn.rollback()
                        return result
                    conn.commit()

                conn.execute("BEGIN IMMEDIATE")
                pending = conn.execute(
                    "SELECT user_id FROM work_daily_refresh_reset_targets "
                    "WHERE business_date=%s AND status='pending' ORDER BY user_id LIMIT %s",
                    (business_date, chunk_size),
                ).fetchall()
                changed = 0
                for pending_row in pending:
                    user_id = str(pending_row[0])
                    user = conn.execute(
                        "SELECT COUNT(*),MIN(COALESCE(work_num,0)),"
                        "MAX(COALESCE(work_num,0)) FROM user_xiuxian WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                    row_count = int(user[0] or 0) if user is not None else 0
                    if row_count == 0:
                        conn.execute(
                            "UPDATE work_daily_refresh_reset_targets SET status='skipped',"
                            "updated_at=%s WHERE business_date=%s AND user_id=%s AND status='pending'",
                            (updated_at, business_date, user_id),
                        )
                        continue
                    previous_count = int(user[1] or 0)
                    previous_max = int(user[2] or 0)
                    updated = conn.execute(
                        "UPDATE user_xiuxian SET work_num=%s WHERE user_id=%s",
                        (reset_count, user_id),
                    )
                    if updated.rowcount != row_count:
                        raise db_backend.IntegrityError("work refresh reset target changed")
                    changed += int(
                        previous_count != reset_count or previous_max != reset_count
                    )
                    conn.execute(
                        "UPDATE work_daily_refresh_reset_targets SET status='applied',"
                        "previous_count=%s,final_count=%s,updated_at=%s "
                        "WHERE business_date=%s AND user_id=%s AND status='pending'",
                        (
                            previous_count,
                            reset_count,
                            updated_at,
                            business_date,
                            user_id,
                        ),
                    )

                progress = conn.execute(
                    "SELECT COUNT(*),COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) "
                    "FROM work_daily_refresh_reset_targets WHERE business_date=%s",
                    (business_date,),
                ).fetchone()
                completed = int(progress[0]) - int(progress[1])
                task_status = "completed" if int(progress[1]) == 0 else "running"
                conn.execute(
                    "UPDATE work_daily_refresh_reset_operations SET completed=%s,changed=changed+%s,"
                    "status=%s,updated_at=%s WHERE business_date=%s",
                    (completed, changed, task_status, updated_at, business_date),
                )
                result = self._result(conn, business_date, "applied")
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise

__all__ = [
    "WorkClaimResult",
    "WorkClaimService",
    "WorkSettlementResult",
    "WorkSettlementService",
    "WorkItemUseResult",
    "WorkItemUseService",
    "WorkRefreshResult",
    "WorkRefreshSettlementService",
    "WorkAbortCleanupResult",
    "WorkAbortCleanupService",
    "WorkDailyRefreshResetResult",
    "WorkDailyRefreshResetService",
]
