from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.observability import (
    AuditLogger,
    current_context,
    elapsed_ms,
    observation_fields,
    trace_context,
)


class ObservabilityTests(unittest.TestCase):
    def test_trace_fields_include_redacted_scope_and_elapsed_time(self) -> None:
        with trace_context(
            request_id="req-1",
            operation_id="op-1",
            job_id="job-1",
            user_scope="user-1234",
        ):
            fields = observation_fields()
            self.assertEqual(fields["request_id"], "req-1")
            self.assertEqual(fields["operation_id"], "op-1")
            self.assertEqual(fields["job_id"], "job-1")
            self.assertEqual(fields["user_scope"], "us***34")
            self.assertGreaterEqual(fields["duration_ms"], 0)
            self.assertGreaterEqual(elapsed_ms(), 0)
        self.assertEqual(current_context(), {"request_id": "", "operation_id": "", "job_id": "", "user_scope": ""})

    def test_audit_logger_upgrades_legacy_table_and_persists_correlation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                uow.execute(
                    "CREATE TABLE web_audit (audit_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "request_id TEXT NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL, "
                    "status INTEGER NOT NULL, actor TEXT NOT NULL, created_at TEXT NOT NULL)"
                )
            AuditLogger(database).record(
                request_id="req-2",
                operation_id="op-2",
                job_id="job-2",
                user_scope="user-5678",
                method="POST",
                path="/api/v1/test",
                status=200,
                actor="admin",
                duration_ms=12,
            )
            rows = AuditLogger(database).list()
            self.assertEqual(len(rows), 1)
            self.assertEqual(
                {
                    rows[0]["request_id"],
                    rows[0]["operation_id"],
                    rows[0]["job_id"],
                    rows[0]["user_scope"],
                    rows[0]["duration_ms"],
                },
                {"req-2", "op-2", "job-2", "us***78", 12},
            )


if __name__ == "__main__":
    unittest.main()
