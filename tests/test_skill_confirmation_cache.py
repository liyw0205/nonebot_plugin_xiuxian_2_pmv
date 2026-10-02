import unittest

from nonebot_plugin_xiuxian_2.features.back.skill_confirmation_cache import (
    SkillConfirmationCache,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class SkillConfirmationCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.cache = SkillConfirmationCache(
            ttl_seconds=30,
            max_entries=2,
            clock=self.clock,
        )

    def put(self, user_id: str, invite_id: str) -> None:
        self.cache.put(
            user_id,
            goods_id=100,
            item_name="测试功法",
            skill_type="功法",
            invite_id=invite_id,
        )

    def test_replacement_keeps_only_latest_confirmation(self) -> None:
        self.put("user", "first")
        self.clock.now = 5
        self.put("user", "second")

        self.assertEqual(self.cache.get("user").invite_id, "second")
        self.assertFalse(self.cache.discard("user", expected_invite_id="first"))
        self.assertEqual(self.cache.get("user").invite_id, "second")
        self.assertTrue(self.cache.discard("user", expected_invite_id="second"))
        self.assertIsNone(self.cache.get("user"))

    def test_expired_confirmation_is_removed_and_not_retrievable(self) -> None:
        self.put("user", "invite")
        self.clock.now = 30

        self.assertIsNone(self.cache.get("user"))
        self.assertEqual(self.cache.seconds_until_next_expiry(), None)

    def test_capacity_evicts_oldest_pending_confirmation(self) -> None:
        self.put("first", "first-invite")
        self.clock.now = 1
        self.put("second", "second-invite")
        self.clock.now = 2
        self.put("third", "third-invite")

        self.assertIsNone(self.cache.get("first"))
        self.assertEqual(self.cache.get("second").invite_id, "second-invite")
        self.assertEqual(self.cache.get("third").invite_id, "third-invite")

    def test_expiry_sweep_removes_stale_prefix(self) -> None:
        self.put("first", "first-invite")
        self.clock.now = 10
        self.put("second", "second-invite")
        self.clock.now = 30

        self.assertEqual(self.cache.purge_expired(), 1)
        self.assertIsNone(self.cache.get("first"))
        self.assertEqual(self.cache.get("second").invite_id, "second-invite")


if __name__ == "__main__":
    unittest.main()
