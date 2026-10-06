from __future__ import annotations

from collections.abc import Callable
from time import perf_counter
from typing import Any


class AdminQqidApplication:
    """Freeze a complete conversion batch before using the existing ID writer."""

    def __init__(
        self,
        candidate_repository,
        batch_repository,
        admin_application,
        *,
        resolve_id: Callable[[str], str | None],
        logger: Any | None = None,
    ) -> None:
        self.candidate_repository = candidate_repository
        self.batch_repository = batch_repository
        self.admin_application = admin_application
        self.resolve_id = resolve_id
        self.logger = logger

    def _log_error(self, error: BaseException) -> None:
        if self.logger is not None:
            self.logger.warning("QQID conversion interrupted: {}", type(error).__name__)

    @staticmethod
    def _result(batch, entries, timings, *, pending=False, error_type="") -> dict:
        counts = {name: 0 for name in ("applied", "unchanged", "resolve_failed", "conflict")}
        waiting = updated_cells = 0
        for entry in entries:
            status = str(entry["status"])
            if status in counts:
                counts[status] += 1
            else:
                waiting += 1
            if status == "applied":
                updated_cells += int((entry.get("result") or {}).get("data", {}).get("updated_cells", 0))
        total = int(batch["total"]) if batch else 0
        waiting = max(waiting, total - sum(counts.values()))
        if pending or waiting:
            status = "pending"
        elif error_type:
            status = "failed"
        elif counts["resolve_failed"] or counts["conflict"]:
            status = "partial"
        else:
            status = "completed" if total else "empty"
        return {
            "status": status,
            "batch_id": str(batch["batch_id"]) if batch else "",
            "total": total,
            **counts,
            "pending": waiting,
            "updated_cells": updated_cells,
            "error_type": error_type,
            **{key: round(value, 6) for key, value in timings.items()},
        }

    def run(self, operation_id: str, operator_id: str) -> dict:
        batch, entries = None, []
        timings = {"scan_seconds": 0.0, "resolve_seconds": 0.0, "update_seconds": 0.0}
        try:
            # This is a separate batch lock, never the single-ID writer's flock.
            with self.batch_repository.exclusive_run():
                batch = self.batch_repository.get(operation_id) or self.batch_repository.get_active()
                if batch is None:
                    started = perf_counter()
                    try:
                        candidates = self.candidate_repository.snapshot()
                    finally:
                        timings["scan_seconds"] += perf_counter() - started
                    if not candidates:
                        return self._result(None, [], timings)
                    batch = self.batch_repository.create(operation_id, operator_id, candidates)
                batch_id = str(batch["batch_id"])
                self.batch_repository.bind_request(operation_id, batch_id)
                entries = self.batch_repository.entries(batch_id)
                if batch["status"] == "completed":
                    return self._result(batch, entries, timings)

                started = perf_counter()
                try:
                    for entry in entries:
                        if entry["status"] != "pending":
                            continue
                        target, error_code = None, ""
                        try:
                            resolved = self.resolve_id(entry["source_id"])
                            target = str(resolved).strip() if resolved is not None else None
                            if not target:
                                target, error_code = None, "unresolved"
                        except Exception as exc:
                            self._log_error(exc)
                            error_code = type(exc).__name__
                        self.batch_repository.freeze_resolution(
                            batch_id, entry["source_id"], target, error_code=error_code,
                        )
                finally:
                    timings["resolve_seconds"] += perf_counter() - started

                # Refresh after resolving all sources: duplicate targets reject both entries.
                entries = self.batch_repository.entries(batch_id)
                if any(entry["status"] == "pending" for entry in entries):
                    return self._result(batch, entries, timings, pending=True)
                started = perf_counter()
                try:
                    for entry in entries:
                        if entry["status"] not in {"resolved", "failed"}:
                            continue
                        previous = entry.get("result") or {}
                        if (entry["status"] == "failed" and previous.get("status") == "rejected"
                                and previous.get("code") == "reconcile_pending"):
                            entry = self.batch_repository.retry_rejected(batch_id, entry["source_id"])
                        try:
                            outcome = self.admin_application.update_user_id(
                                entry["child_operation_id"], entry["source_id"], entry["target_id"],
                            )
                        except Exception as exc:
                            self._log_error(exc)
                            self.batch_repository.record_result(
                                batch_id, entry["source_id"], "failed",
                                {"status": "failed", "code": "writer_exception", "error_type": type(exc).__name__},
                            )
                            break
                        safe_result = {
                            "status": outcome.status,
                            "code": outcome.code,
                            "data": {"updated_cells": int((outcome.data or {}).get("updated_cells", 0))},
                            "replayed": outcome.replayed,
                        }
                        status = "applied" if outcome.ok else (
                            "conflict" if outcome.status == "rejected" and outcome.code != "reconcile_pending" else "failed"
                        )
                        self.batch_repository.record_result(batch_id, entry["source_id"], status, safe_result)
                        if not outcome.ok:
                            break
                finally:
                    timings["update_seconds"] += perf_counter() - started
                entries = self.batch_repository.entries(batch_id)
                if not any(entry["status"] in {"pending", "resolved", "failed"} for entry in entries):
                    batch = self.batch_repository.complete(batch_id)
                return self._result(batch, entries, timings)
        except Exception as exc:
            self._log_error(exc)
            if batch is not None:
                try:
                    entries = self.batch_repository.entries(str(batch["batch_id"]))
                except Exception:
                    pass
            return self._result(batch, entries, timings, pending=batch is not None, error_type=type(exc).__name__)

    @staticmethod
    def format_result(result: dict) -> tuple[bool, str]:
        status = result["status"]
        headings = {
            "completed": "QQID\u8f6c\u6362\u5b8c\u6210\u3002",
            "partial": "QQID\u8f6c\u6362\u6279\u6b21\u5df2\u7ed3\u675f\uff0c\u4f46\u6709\u5931\u8d25\u6216\u51b2\u7a81\uff0c\u672a\u5168\u90e8\u6210\u529f\u3002",
            "pending": "QQID\u8f6c\u6362\u5c1a\u672a\u5b8c\u6210\uff0c\u6279\u6b21\u5df2\u4fdd\u7559\uff1b\u518d\u6b21\u53d1\u9001\u8f6c\u6362QQID\u53ef\u6062\u590d\uff0c\u8bf7\u52ff\u624b\u52a8\u91cd\u590d\u8fc1\u79fb\u3002",
            "empty": "\u672a\u627e\u5230\u4efb\u4f55\u53ef\u8fc1\u79fbID\u3002",
            "failed": "QQID\u8f6c\u6362\u672a\u80fd\u5f00\u59cb\uff0c\u8bf7\u68c0\u67e5\u542f\u52a8\u8fc1\u79fb\u3001\u6570\u636e\u5e93\u53ca\u670d\u52a1\u65e5\u5fd7\u3002",
        }
        message = headings.get(status, headings["failed"])
        if result.get("total"):
            message += (
                f"\n\u5019\u9009\uff1a{result['total']}\uff0c\u6210\u529f\uff1a{result['applied']}\uff0c\u65e0\u9700\u53d8\u66f4\uff1a{result['unchanged']}"
                f"\n\u89e3\u6790\u5931\u8d25\uff1a{result['resolve_failed']}\uff0c\u51b2\u7a81\uff1a{result['conflict']}\uff0c\u5f85\u6062\u590d\uff1a{result['pending']}"
                f"\n\u5df2\u786e\u8ba4\u66f4\u65b0\u5355\u5143\u683c\uff1a{result['updated_cells']}"
            )
        message += (
            f"\n\u672c\u6b21\u8017\u65f6\uff1a\u626b\u63cf {result.get('scan_seconds', 0):.2f}s\uff0c"
            f"\u89e3\u6790 {result.get('resolve_seconds', 0):.2f}s\uff0c\u8fc1\u79fb {result.get('update_seconds', 0):.2f}s"
        )
        return status == "completed", message


__all__ = ["AdminQqidApplication"]
