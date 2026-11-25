from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from nonebot_plugin_xiuxian_2.features.base import public_display_snapshot as snapshots
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import ProcessReadCache


class PublicDisplaySnapshotTests(unittest.TestCase):
    def test_only_display_rows_are_cached_and_personal_value_is_read_live_on_hits(self):
        cache = ProcessReadCache()
        loader = Mock(return_value=([("leader", 100)], (2, 50)))
        live = Mock(return_value=(1, 120))
        with patch.object(snapshots, "process_read_cache", cache):
            first, cold_value = snapshots.shared_display_snapshot_with_live_value(
                "/tmp/display-snapshot-test.db", "board", loader=loader, read_live=live
            )
            live.assert_not_called()
            second, hot_value = snapshots.shared_display_snapshot_with_live_value(
                "/tmp/display-snapshot-test.db", "board", loader=loader, read_live=live
            )
            plain = snapshots.shared_display_snapshot(
                "/tmp/display-snapshot-test.db", "board", loader=Mock()
            )

        loader.assert_called_once_with()
        live.assert_called_once_with()
        self.assertEqual((2, 50), cold_value)
        self.assertEqual((1, 120), hot_value)
        self.assertEqual(first, second)
        self.assertEqual(first, plain)
        self.assertEqual({"rows", "as_of"}, set(first))

    def test_discarded_loader_cannot_supply_another_requests_personal_value(self):
        loader = Mock(return_value=([("old-leader", 100)], (2, 50)))
        live = Mock(return_value=(1, 120))

        def return_competing_fill(*args, **kwargs):
            # An invalidated load can lose the refill race and return a cache hit.
            kwargs["loader"]()
            return {
                "rows": [("new-leader", 150)],
                "as_of": "2026-11-26T00:00:00+00:00",
                "_load_token": "a-different-request",
            }

        with patch.object(
            snapshots.process_read_cache, "get_or_load", side_effect=return_competing_fill
        ):
            snapshot, value = snapshots.shared_display_snapshot_with_live_value(
                "/tmp/display-snapshot-test.db", "board", loader=loader, read_live=live
            )

        self.assertEqual([("new-leader", 150)], snapshot["rows"])
        self.assertEqual((1, 120), value)
        self.assertNotIn("_load_token", snapshot)
        live.assert_called_once_with()

    def test_real_cache_invalidation_retry_reuses_only_the_final_live_value(self):
        cache = ProcessReadCache()
        database = "/tmp/display-snapshot-retry.db"
        attempts = []
        live = Mock()

        def load():
            attempts.append(len(attempts) + 1)
            attempt = attempts[-1]
            if attempt == 1:
                cache.invalidate_snapshot_database(database)
            return [("leader", attempt)], (attempt, attempt * 10)

        with patch.object(snapshots, "process_read_cache", cache):
            snapshot, value = snapshots.shared_display_snapshot_with_live_value(
                database, "board", loader=load, read_live=live
            )

        self.assertEqual([1, 2], attempts)
        self.assertEqual([("leader", 2)], snapshot["rows"])
        self.assertEqual((2, 20), value)
        self.assertEqual({"rows", "as_of"}, set(snapshot))
        live.assert_not_called()


if __name__ == "__main__":
    unittest.main()
