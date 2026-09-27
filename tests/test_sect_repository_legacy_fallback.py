from pathlib import Path
import unittest


class SectRepositoryLegacyFallbackTests(unittest.TestCase):
    def test_default_repository_is_separate_from_explicit_rollback_adapter(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/features/sect/repository.py"
        ).read_text(encoding="utf-8")

        default_repository = source[source.index("class SectRenameSqlRepository:"):]
        self.assertIn("class LegacySectRepository:", source)
        self.assertIn("class SectRenameSqlRepository:", source)
        self.assertNotIn("class SectRenameSqlRepository(LegacySectRepository)", source)
        self.assertNotIn("transaction_service import", default_repository)
        self.assertNotIn("self._service(", default_repository)


if __name__ == "__main__":
    unittest.main()
