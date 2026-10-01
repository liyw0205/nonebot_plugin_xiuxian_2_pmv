from __future__ import annotations

import unittest

import nonebot

try:
    nonebot.get_driver()
except ValueError:
    nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import external_api


class ExternalApiCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        with external_api._real_id_cache_lock:
            external_api._real_id_cache.clear()

    def tearDown(self) -> None:
        with external_api._real_id_cache_lock:
            external_api._real_id_cache.clear()

    def test_prune_removes_expired_entries(self) -> None:
        with external_api._real_id_cache_lock:
            external_api._real_id_cache.update(
                {"expired": (9.0, "old"), "live": (11.0, "new")}
            )

        external_api._prune_real_id_cache(10.0)

        self.assertNotIn("expired", external_api._real_id_cache)
        self.assertEqual(external_api._real_id_cache["live"], (11.0, "new"))

    def test_prune_caps_cache_by_oldest_expiry(self) -> None:
        original_limit = external_api._REAL_ID_CACHE_MAX_ENTRIES
        external_api._REAL_ID_CACHE_MAX_ENTRIES = 2
        try:
            with external_api._real_id_cache_lock:
                external_api._real_id_cache.update(
                    {
                        "old": (11.0, "old"),
                        "middle": (12.0, "middle"),
                        "new": (13.0, "new"),
                    }
                )

            external_api._prune_real_id_cache(10.0)

            self.assertNotIn("old", external_api._real_id_cache)
            self.assertEqual(set(external_api._real_id_cache), {"middle", "new"})
        finally:
            external_api._REAL_ID_CACHE_MAX_ENTRIES = original_limit


if __name__ == "__main__":
    unittest.main()
