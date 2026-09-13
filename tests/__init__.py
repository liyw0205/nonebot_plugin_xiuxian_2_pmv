"""Package-level unittest bootstrap.

``unittest discover`` imports this package before importing individual test
modules.  Set up an isolated data directory at that earliest point so module
imports cannot initialize the production ``data/xiuxian`` tree.
"""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory

from .bootstrap import copy_static_data


_temporary_data: TemporaryDirectory[str] | None = None
_requested = os.environ.get("XIUXIAN_TEST_DATA_DIR")
if _requested:
    _data_dir = copy_static_data(
        Path(__file__).resolve().parents[1] / "data" / "xiuxian",
        Path(_requested),
    )
else:
    _temporary_data = TemporaryDirectory(prefix="xiuxian-unittest-")
    _data_dir = copy_static_data(
        Path(__file__).resolve().parents[1] / "data" / "xiuxian",
        Path(_temporary_data.name) / "xiuxian",
    )

# Tests may patch this value for a single case, but the default process-wide
# value is always isolated from the repository's runtime directory.
os.environ["XIUXIAN_DATA_DIR"] = str(_data_dir)

__all__ = ["_data_dir"]
