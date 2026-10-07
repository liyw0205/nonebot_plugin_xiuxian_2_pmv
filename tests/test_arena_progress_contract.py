import unittest

from scripts.check_full_refactor_progress import PACKAGE, _arena_owner_status, _slice_status


class ArenaProgressContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        paths = {
            "handlers": "xiuxian/xiuxian_arena/__init__.py",
            "application": "features/arena/application.py",
            "repository": "features/arena/repository.py",
            "opponent_application": "features/arena/opponent_application.py",
            "opponent_repository": "features/arena/opponent_repository.py",
            "state_repository": "features/arena/state_repository.py",
            "migrations": "features/arena/migrations.py",
            "plugin": "plugin.py",
        }
        cls.sources = {name: (PACKAGE / path).read_text(encoding="utf-8") for name, path in paths.items()}

    def test_two_frozen_commands_have_source_bound_owner_evidence(self):
        for key, value in _arena_owner_status(self.sources).items():
            self.assertTrue(value, key)
            self.assertTrue(_slice_status()["arena"][key], key)

    def test_gate_rejects_owner_receipt_quantity_and_recovery_regressions(self):
        owner = "purchase_and_challenge_handlers_reach_feature_sql_owners"
        purchase = "purchase_replays_before_live_catalog_and_preserves_raw_quantity"
        cache = "opponent_queries_and_all_cache_consumers_share_one_bounded_owner"
        challenge = "challenge_replay_and_failures_cannot_fall_through_to_nomatch_rewards"
        receipts = "atomic_receipts_iso_week_and_scoped_started_recovery_are_feature_owned"
        cases = (
            ("handlers", "arena_application.purchase(", "legacy_arena_application.purchase(", owner),
            ("handlers", "arena_application.purchase_result(", "legacy_purchase_result(", purchase),
            ("handlers", "clamp_quantity=True", "clamp_quantity=False", purchase),
            ("handlers", 'item_type=item_info["type"], quantity=quantity,',
             'item_type=item_info["type"], quantity=1,', purchase),
            ("opponent_repository", "mode=ro", "mode=rw", cache),
            ("opponent_repository", "targets[:3]", "targets[:30]", cache),
            ("handlers", "if opponent_player is None:", "if False:", challenge),
            ("repository", 'result["status"] in {"applied", "duplicate"}',
             'result["status"] in {"applied", "duplicate", "state_changed"}', receipts),
            ("application", 'action in {"arena.purchase", "arena.settle"}',
             'action in {"arena.purchase", "arena.settle", "arena.challenge_purchase"}', receipts),
        )
        for source, before, after, gate in cases:
            with self.subTest(source=source, mutation=after):
                self.assertIn(before, self.sources[source])
                changed = dict(self.sources)
                changed[source] = changed[source].replace(before, after, 1)
                self.assertFalse(_arena_owner_status(changed)[gate])

    def test_gate_rejects_a_second_legacy_opponent_cache(self):
        changed = dict(self.sources)
        changed["handlers"] += "\narena_opponent_cache = {}\n"
        self.assertFalse(_arena_owner_status(changed)[
            "opponent_queries_and_all_cache_consumers_share_one_bounded_owner"
        ])
