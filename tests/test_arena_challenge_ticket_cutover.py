from pathlib import Path
import unittest


ROOT = Path(__file__).parents[1]


class ArenaChallengeTicketCutoverTests(unittest.TestCase):
    def test_default_entry_uses_feature_application(self) -> None:
        source = (ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_arena/__init__.py").read_text(
            encoding="utf-8"
        )
        handler = source.split("async def use_arena_challenge_ticket", 1)[1]
        self.assertIn("arena_application.use_challenge_ticket(", handler)
        self.assertNotIn("arena_challenge_ticket_service.use(", handler)
        self.assertNotIn("arena_limit.add_challenge_count", handler)
        self.assertNotIn("sql_message.update_back_j", handler)

    def test_default_repository_owns_ticket_transaction(self) -> None:
        source = (ROOT / "nonebot_plugin_xiuxian_2/features/arena/repository.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("def use_challenge_ticket(self, operation_id", source)
        self.assertIn("arena_challenge_ticket_operations", source)
        self.assertNotIn("ArenaChallengeTicketService", source)

    def test_legacy_ticket_service_and_request_time_ddl_are_removed(self) -> None:
        source = (
            ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_arena/transaction_service.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("class ArenaChallengeTicketService", source)
        self.assertNotIn('CREATE TABLE IF NOT EXISTS arena_challenge_ticket_operations', source)

    def test_ticket_schema_is_registered_as_game_startup_migration(self) -> None:
        source = (ROOT / "nonebot_plugin_xiuxian_2/plugin.py").read_text(encoding="utf-8")
        self.assertIn(
            'Migration("arena.004", "arena_challenge_ticket_operations", apply_arena_challenge_ticket)',
            source,
        )


if __name__ == "__main__":
    unittest.main()
