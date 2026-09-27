from __future__ import annotations

import unittest

from nonebot_plugin_xiuxian_2.core.numeric import (
    as_int_like,
    normalize_numeric_row,
    normalize_sect_row,
    normalize_user_row,
)


class CoreNumericTests(unittest.TestCase):
    def test_user_rows_normalize_oversized_and_scientific_text_without_mutating(self):
        source = {"sect_contribution": str(2**70), "stone": "1.25e3", "name": "u"}

        normalized = normalize_user_row(source)

        self.assertEqual(2**70, normalized["sect_contribution"])
        self.assertEqual(1250, normalized["stone"])
        self.assertEqual("u", normalized["name"])
        self.assertEqual(str(2**70), source["sect_contribution"])

    def test_sect_normalization_is_limited_to_sect_numeric_fields(self):
        normalized = normalize_sect_row(
            {"sect_scale": str(2**70), "sect_contribution": str(2**70)}
        )

        self.assertEqual(2**70, normalized["sect_scale"])
        self.assertEqual(str(2**70), normalized["sect_contribution"])

    def test_generic_normalizer_and_integer_parser_keep_fallbacks(self):
        self.assertEqual({"score": 42}, normalize_numeric_row({"score": "42"}, {"score"}))
        self.assertEqual(7, as_int_like("not-a-number", 7))
        self.assertEqual(0, as_int_like(None))


if __name__ == "__main__":
    unittest.main()
