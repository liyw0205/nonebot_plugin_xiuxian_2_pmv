from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Callable, Mapping

from ..xiuxian_utils import db_backend


@dataclass(frozen=True)
class ActivityClaimAllStepResult:
    name: str
    ok: bool
    text: str


@dataclass(frozen=True)
class ActivityClaimAllResult:
    status: str
    ok: bool = False
    text: str = ""
    steps: tuple[ActivityClaimAllStepResult, ...] = ()

    @property
    def completed(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ActivityClaimAllService:
    """Run the fixed activity reward steps with durable retry progress."""

    STEP_LABELS = {
        "tasks": "任务",
        "pass": "战令",
        "boss_milestone": "首领进度",
        "boss_rank": "首领排行",
    }

    def __init__(self, activity_database: str | Path, lock: RLock | None = None) -> None:
        self._activity_database = Path(activity_database)
        self._lock = lock or RLock()

    @classmethod
    def step_names(cls) -> tuple[str, ...]:
        return tuple(cls.STEP_LABELS)

    @staticmethod
    def _ensure_schema(conn) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity_claim_all_operations("
            "operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,status TEXT NOT NULL,"
            "result_json TEXT NOT NULL DEFAULT '',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS activity_claim_all_steps("
            "operation_id TEXT NOT NULL,step_name TEXT NOT NULL,ordinal INTEGER NOT NULL,"
            "status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,"
            "ok INTEGER,result_text TEXT NOT NULL DEFAULT '',error_text TEXT NOT NULL DEFAULT '',"
            "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(operation_id,step_name))"
        )

    @staticmethod
    def _encode_result(ok: bool, text: str, steps: tuple[ActivityClaimAllStepResult, ...]) -> str:
        return json.dumps(
            {
                "ok": bool(ok),
                "text": str(text),
                "steps": [
                    {"name": step.name, "ok": step.ok, "text": step.text}
                    for step in steps
                ],
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )

    @staticmethod
    def _decode_result(status: str, raw: str) -> ActivityClaimAllResult:
        data = json.loads(raw)
        steps = tuple(
            ActivityClaimAllStepResult(str(row["name"]), bool(row["ok"]), str(row["text"]))
            for row in data.get("steps") or ()
        )
        return ActivityClaimAllResult(status, bool(data.get("ok")), str(data.get("text") or ""), steps)

    def _load_or_create(self, operation_id: str, user_id: str) -> ActivityClaimAllResult | None:
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._ensure_schema(conn)
                row = conn.execute(
                    "SELECT user_id,status,result_json FROM activity_claim_all_operations "
                    "WHERE operation_id=%s",
                    (operation_id,),
                ).fetchone()
                if row is None:
                    conn.execute(
                        "INSERT INTO activity_claim_all_operations(operation_id,user_id,status) "
                        "VALUES(%s,%s,'pending')",
                        (operation_id, user_id),
                    )
                    conn.executemany(
                        "INSERT INTO activity_claim_all_steps(operation_id,step_name,ordinal) "
                        "VALUES(%s,%s,%s)",
                        [
                            (operation_id, step_name, ordinal)
                            for ordinal, step_name in enumerate(self.step_names())
                        ],
                    )
                    conn.commit()
                    return None
                if str(row[0]) != user_id:
                    conn.rollback()
                    return ActivityClaimAllResult("operation_conflict", text="领取请求冲突，请重新发送")
                names = tuple(
                    str(step[0])
                    for step in conn.execute(
                        "SELECT step_name FROM activity_claim_all_steps WHERE operation_id=%s "
                        "ORDER BY ordinal",
                        (operation_id,),
                    ).fetchall()
                )
                if names != self.step_names():
                    conn.rollback()
                    return ActivityClaimAllResult("operation_conflict", text="领取计划冲突，请重新发送")
                if str(row[1]) == "completed" and str(row[2]):
                    conn.rollback()
                    return self._decode_result("duplicate", str(row[2]))
                conn.commit()
                return None
            except Exception:
                conn.rollback()
                raise

    def _step_rows(self, operation_id: str) -> tuple[ActivityClaimAllStepResult, ...]:
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            self._ensure_schema(conn)
            rows = conn.execute(
                "SELECT step_name,ok,result_text FROM activity_claim_all_steps "
                "WHERE operation_id=%s AND status='completed' ORDER BY ordinal",
                (operation_id,),
            ).fetchall()
            return tuple(
                ActivityClaimAllStepResult(str(row[0]), bool(row[1]), str(row[2]))
                for row in rows
            )

    def _completed_step_names(self, operation_id: str) -> set[str]:
        return {step.name for step in self._step_rows(operation_id)}

    def _start_step(self, operation_id: str, step_name: str) -> None:
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE activity_claim_all_steps SET status='running',attempts=attempts+1,"
                    "error_text='',updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s "
                    "AND step_name=%s AND status!='completed'",
                    (operation_id, step_name),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _complete_step(self, operation_id: str, step_name: str, ok: bool, text: str) -> None:
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE activity_claim_all_steps SET status='completed',ok=%s,result_text=%s,"
                    "error_text='',updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s AND step_name=%s "
                    "AND status!='completed'",
                    (int(bool(ok)), str(text), operation_id, step_name),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _fail_step(self, operation_id: str, step_name: str, error: str) -> None:
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE activity_claim_all_steps SET status='failed_retryable',error_text=%s,"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s AND step_name=%s "
                    "AND status!='completed'",
                    (str(error), operation_id, step_name),
                )
                conn.execute(
                    "UPDATE activity_claim_all_operations SET status='pending',"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (operation_id,),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @classmethod
    def _format_completed(cls, steps: tuple[ActivityClaimAllStepResult, ...]) -> tuple[bool, str]:
        successes = [step.text for step in steps if step.ok]
        if successes:
            return True, "\n\n".join(successes)
        lines = ["暂无可领取奖励"]
        lines.extend(f"{cls.STEP_LABELS[step.name]}：{step.text}" for step in steps)
        return False, "\n".join(lines)

    def _finish(self, operation_id: str) -> ActivityClaimAllResult:
        steps = self._step_rows(operation_id)
        if tuple(step.name for step in steps) != self.step_names():
            raise RuntimeError("activity claim-all plan is incomplete")
        ok, text = self._format_completed(steps)
        result_json = self._encode_result(ok, text, steps)
        with self._lock, closing(db_backend.connect(self._activity_database)) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE activity_claim_all_operations SET status='completed',result_json=%s,"
                    "updated_at=CURRENT_TIMESTAMP WHERE operation_id=%s",
                    (result_json, operation_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return ActivityClaimAllResult("applied", ok, text, steps)

    def run(
        self,
        operation_id: str,
        user_id: str,
        runners: Mapping[str, Callable[[str], tuple[bool, str]]],
    ) -> ActivityClaimAllResult:
        operation_id = str(operation_id).strip()
        user_id = str(user_id)
        if not operation_id or not user_id or tuple(runners) != self.step_names():
            raise ValueError("fixed activity claim-all operation is required")

        existing = self._load_or_create(operation_id, user_id)
        if existing is not None:
            return existing

        completed = self._completed_step_names(operation_id)
        for step_name, runner in runners.items():
            if step_name in completed:
                continue
            self._start_step(operation_id, step_name)
            child_operation_id = f"{operation_id}:{step_name.replace('_', '-')}"
            try:
                ok, result_text = runner(child_operation_id)
                self._complete_step(operation_id, step_name, ok, result_text)
            except Exception as exc:
                self._fail_step(operation_id, step_name, str(exc))
                label = self.STEP_LABELS[step_name]
                return ActivityClaimAllResult(
                    "retryable_failure",
                    False,
                    f"活动奖励领取未完成，请重试\n{label}：{exc}",
                    self._step_rows(operation_id),
                )
        return self._finish(operation_id)


__all__ = [
    "ActivityClaimAllResult",
    "ActivityClaimAllService",
    "ActivityClaimAllStepResult",
]
