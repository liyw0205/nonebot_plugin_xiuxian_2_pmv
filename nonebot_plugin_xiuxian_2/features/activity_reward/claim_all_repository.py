from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from ...infrastructure.database import DatabaseUnitOfWork


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


class ActivityClaimAllRepository:
    STEP_LABELS = {"tasks": "任务", "pass": "战令", "boss_milestone": "首领进度", "boss_rank": "首领排行"}

    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @classmethod
    def step_names(cls) -> tuple[str, ...]:
        return tuple(cls.STEP_LABELS)

    @staticmethod
    def _encode(ok: bool, text: str, steps: tuple[ActivityClaimAllStepResult, ...]) -> str:
        return json.dumps({"ok": ok, "text": text, "steps": [step.__dict__ for step in steps]}, ensure_ascii=True, separators=(",", ":"))

    @staticmethod
    def _decode(status: str, raw: str) -> ActivityClaimAllResult:
        data = json.loads(raw)
        return ActivityClaimAllResult(
            status, bool(data.get("ok")), str(data.get("text") or ""),
            tuple(ActivityClaimAllStepResult(str(row["name"]), bool(row["ok"]), str(row["text"])) for row in data.get("steps") or ()),
        )

    def _ensure(self, uow: DatabaseUnitOfWork) -> None:
        uow.execute("CREATE TABLE IF NOT EXISTS activity_claim_all_operations(operation_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,status TEXT NOT NULL,result_json TEXT NOT NULL DEFAULT '',created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        uow.execute("CREATE TABLE IF NOT EXISTS activity_claim_all_steps(operation_id TEXT NOT NULL,step_name TEXT NOT NULL,ordinal INTEGER NOT NULL,status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,ok INTEGER,result_text TEXT NOT NULL DEFAULT '',error_text TEXT NOT NULL DEFAULT '',updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY(operation_id,step_name))")

    def prepare(self, operation_id: str, user_id: str) -> ActivityClaimAllResult | None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            self._ensure(uow)
            row = uow.query_one("SELECT user_id,status,result_json FROM activity_claim_all_operations WHERE operation_id=?", (operation_id,))
            if row is None:
                uow.execute("INSERT INTO activity_claim_all_operations(operation_id,user_id,status) VALUES(?,?,?)", (operation_id,user_id,"pending"))
                uow.executemany("INSERT INTO activity_claim_all_steps(operation_id,step_name,ordinal) VALUES(?,?,?)", ((operation_id,name,index) for index,name in enumerate(self.step_names())))
                return None
            if str(row["user_id"]) != user_id:
                return ActivityClaimAllResult("operation_conflict", text="领取请求冲突，请重新发送")
            names = tuple(str(x["step_name"]) for x in uow.query_all("SELECT step_name FROM activity_claim_all_steps WHERE operation_id=? ORDER BY ordinal", (operation_id,)))
            if names != self.step_names():
                return ActivityClaimAllResult("operation_conflict", text="领取计划冲突，请重新发送")
            if str(row["status"]) == "completed" and str(row["result_json"]):
                return self._decode("duplicate", str(row["result_json"]))
            return None

    def completed_steps(self, operation_id: str) -> tuple[ActivityClaimAllStepResult, ...]:
        with DatabaseUnitOfWork(self.database) as uow:
            self._ensure(uow)
            return tuple(ActivityClaimAllStepResult(str(row["step_name"]), bool(row["ok"]), str(row["result_text"])) for row in uow.query_all("SELECT step_name,ok,result_text FROM activity_claim_all_steps WHERE operation_id=? AND status='completed' ORDER BY ordinal", (operation_id,)))

    def start_step(self, operation_id: str, name: str) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("UPDATE activity_claim_all_steps SET status='running',attempts=attempts+1,error_text='' WHERE operation_id=? AND step_name=? AND status!='completed'", (operation_id,name))

    def complete_step(self, operation_id: str, name: str, ok: bool, text: str) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("UPDATE activity_claim_all_steps SET status='completed',ok=?,result_text=?,error_text='' WHERE operation_id=? AND step_name=? AND status!='completed'", (int(ok),str(text),operation_id,name))

    def fail_step(self, operation_id: str, name: str, error: str) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("UPDATE activity_claim_all_steps SET status='failed_retryable',error_text=? WHERE operation_id=? AND step_name=? AND status!='completed'", (str(error),operation_id,name))
            uow.execute("UPDATE activity_claim_all_operations SET status='pending' WHERE operation_id=?", (operation_id,))

    def finish(self, operation_id: str) -> ActivityClaimAllResult:
        steps = self.completed_steps(operation_id)
        if tuple(step.name for step in steps) != self.step_names():
            raise RuntimeError("activity claim-all plan is incomplete")
        successes = [step.text for step in steps if step.ok]
        ok = bool(successes)
        text = "\n\n".join(successes) if successes else "\n".join(["暂无可领取奖励", *(f"{self.STEP_LABELS[step.name]}：{step.text}" for step in steps)])
        result = ActivityClaimAllResult("applied", ok, text, steps)
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("UPDATE activity_claim_all_operations SET status='completed',result_json=?,updated_at=CURRENT_TIMESTAMP WHERE operation_id=?", (self._encode(ok,text,steps),operation_id))
        return result

    def run(self, operation_id: str, user_id: str, runners: Mapping[str, Callable[[str], tuple[bool, str]]]) -> ActivityClaimAllResult:
        if not operation_id or not user_id or tuple(runners) != self.step_names():
            raise ValueError("fixed activity claim-all operation is required")
        existing = self.prepare(operation_id, user_id)
        if existing is not None:
            return existing
        completed = {step.name for step in self.completed_steps(operation_id)}
        for name, runner in runners.items():
            if name in completed:
                continue
            self.start_step(operation_id, name)
            try:
                ok, text = runner(f"{operation_id}:{name.replace('_','-')}")
                self.complete_step(operation_id, name, ok, text)
            except Exception as exc:
                self.fail_step(operation_id, name, str(exc))
                return ActivityClaimAllResult("retryable_failure", text=f"活动奖励领取未完成，请重试\n{self.STEP_LABELS[name]}：{exc}", steps=self.completed_steps(operation_id))
        return self.finish(operation_id)


__all__ = ["ActivityClaimAllRepository", "ActivityClaimAllResult", "ActivityClaimAllStepResult"]
