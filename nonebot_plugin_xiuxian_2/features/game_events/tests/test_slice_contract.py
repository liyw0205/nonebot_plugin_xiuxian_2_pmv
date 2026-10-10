from __future__ import annotations

import unittest

from ..application import GameEventApplication
from ..manifest import FEATURE
from ..migrations import MIGRATION_VERSION, MIGRATIONS
from ..schemas import GameEventStatisticsRequest


class _Repository:
    def __init__(self, changed: bool) -> None:
        self.changed = changed
        self.calls = []

    def record(self, **kwargs):
        self.calls.append(kwargs)
        return self.changed


class GameEventsSliceContractTests(unittest.TestCase):
    def test_manifest_and_migration_target_player_projection(self) -> None:
        self.assertEqual(FEATURE.key, "game_events")
        self.assertEqual(FEATURE.migration_version, "game_events.001")
        self.assertEqual(MIGRATION_VERSION, "game_events.001")
        self.assertEqual(MIGRATIONS, (MIGRATION_VERSION,))

    def test_application_normalizes_and_reports_replay(self) -> None:
        repository = _Repository(changed=False)
        result = GameEventApplication("player.db", repository=repository).record_statistics(
            GameEventStatisticsRequest(
                event_id=" event:1 ",
                user_id=" user ",
                increments={"stat": 2},
                occurred_at=" 2026-10-09T00:00:00+00:00 ",
            )
        )
        self.assertEqual(result.status, "replayed")
        self.assertFalse(result.changed)
        self.assertEqual(repository.calls[0]["event_id"], "event:1")
        self.assertEqual(repository.calls[0]["increments"], {"stat": 2})


if __name__ == "__main__":
    unittest.main()
