from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import nonebot

nonebot.init()

from .... import paths
from ....xiuxian.xiuxian_entertainment.media_parser import card


class MediaParserCardTests(unittest.TestCase):
    def test_font_selection_uses_custom_xiuxian_data_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / "custom-data"
            custom_font = data_dir / "font" / "SarasaMonoSC-Bold.ttf"
            custom_font.parent.mkdir(parents=True)
            custom_font.touch()

            with patch.dict(os.environ, {paths.DATA_DIR_ENV: str(data_dir)}):
                paths.reset_paths_for_test()
                try:
                    marker = object()
                    with patch.object(card.ImageFont, "truetype", return_value=marker) as load_font:
                        self.assertIs(card._pick_font(18), marker)
                    self.assertEqual(load_font.call_args.args[0], str(custom_font))
                finally:
                    paths.reset_paths_for_test()


if __name__ == "__main__":
    unittest.main()
