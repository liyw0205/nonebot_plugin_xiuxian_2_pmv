from __future__ import annotations

import unittest

from ..attribute_application import PlayerAttributeApplication


class PlayerAttributeApplicationTest(unittest.TestCase):
    def test_delegates_ratio_current_flag_and_provider_ports(self) -> None:
        calls = []

        def provider(user_id, **kwargs):
            calls.append((user_id, kwargs))
            return {"final_atk": 42}

        application = PlayerAttributeApplication(provider)
        result = application.get_final_attributes(
            "u",
            ratio=0.5,
            include_current=False,
            base_provider=lambda _user_id: {"base_atk": 1},
        )

        self.assertEqual(result, {"final_atk": 42})
        self.assertEqual(calls[0][0], "u")
        self.assertEqual(calls[0][1]["ratio"], 0.5)
        self.assertFalse(calls[0][1]["include_current"])
        self.assertIn("base_provider", calls[0][1])

    def test_default_provider_is_explicit_compatibility_adapter(self) -> None:
        application = PlayerAttributeApplication()

        self.assertEqual(application.provider.__name__, "_legacy_attribute_provider")


if __name__ == "__main__":
    unittest.main()
