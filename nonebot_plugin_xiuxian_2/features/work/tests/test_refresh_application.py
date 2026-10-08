from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import Mock

from ..refresh_application import WorkRefreshApplication
from ..refresh_repository import WorkRefreshResult


class _Repository:
    def __init__(self, result: WorkRefreshResult) -> None:
        self.result = result

    def refresh(self, *args, **kwargs):
        return self.result


class WorkRefreshApplicationTests(unittest.TestCase):
    def test_applied_refresh_projects_only_after_repository_success(self):
        offer = {"tasks": {"Task": {"time": 5}}, "task_order": ["Task"], "status": 1}
        writer = Mock()
        app = WorkRefreshApplication(
            Path("unused.db"),
            repository=_Repository(WorkRefreshResult("applied", 2, offer)),
            legacy_projection_writer=writer,
        )

        result = app.refresh(
            operation_id="refresh-1",
            user_id="u",
            expected_count=3,
            expected_cd={"type": 0},
            expected_offer=None,
            new_offer=offer,
        )

        self.assertEqual(result.status, "applied")
        writer.assert_called_once_with("u", offer)

    def test_rejected_refresh_does_not_project(self):
        writer = Mock()
        app = WorkRefreshApplication(
            Path("unused.db"),
            repository=_Repository(WorkRefreshResult("state_changed")),
            legacy_projection_writer=writer,
        )

        app.refresh(
            operation_id="refresh-2",
            user_id="u",
            expected_count=3,
            expected_cd={"type": 0},
            expected_offer=None,
            new_offer={"tasks": {"Task": {"time": 5}}},
        )

        writer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
