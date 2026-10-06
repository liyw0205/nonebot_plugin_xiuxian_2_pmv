from __future__ import annotations

from unittest.mock import patch

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from nonebot_plugin_xiuxian_2.features.admin.command_control_repository import (
    AdminCommandControlRepository,
)
from tests.test_admin_command_control import _compat


@pytest.fixture
def store(tmp_path):
    repository = AdminCommandControlRepository(tmp_path / "command_disable.json")
    return repository, _compat(repository)


def test_round_trip_preserves_normalized_entries_without_mutable_facade_state(store):
    repository, facade = store
    facade["sync_command_registry"]({"修仙签到": "xiuxian_base"})
    assert facade["set_command_disabled"]("修仙签到", disabled=True) == (True, "")
    loaded = facade["load_command_disable_memory"]()
    assert loaded["修仙签到"] == {"disabled": True, "module": "xiuxian_base"}
    loaded["修仙签到"]["disabled"] = False
    loaded["injected"] = {"disabled": True, "module": "xiuxian_base"}
    original = repository.path.read_bytes()
    facade["save_command_disable_memory"]()

    reopened = _compat(AdminCommandControlRepository(repository.path))
    assert reopened["load_command_disable_memory"]() == {
        "修仙签到": {"disabled": True, "module": "xiuxian_base"}
    }
    assert repository.path.read_bytes() == original
    assert list(repository.path.parent.glob(".*.tmp")) == []


def test_invalid_file_is_rejected_without_reset_or_replacement(store):
    repository, facade = store
    repository.path.write_text("{broken", encoding="utf-8")
    for operation in (facade["load_command_disable_memory"], facade["save_command_disable_memory"]):
        with pytest.raises(ValueError):
            operation()
    assert repository.path.read_text(encoding="utf-8") == "{broken"
    assert list(repository.path.parent.glob("command_disable.json.invalid.*.bak")) == []


def test_filesystem_error_is_not_silently_swallowed_by_compatibility_writer(store):
    repository, facade = store
    facade["sync_command_registry"]({"修仙签到": "xiuxian_base"})
    original = repository.path.read_bytes()
    with patch("nonebot_plugin_xiuxian_2.infrastructure.filesystem.atomic.os.replace", side_effect=OSError("disk full")):
        with pytest.raises(OSError, match="disk full"):
            facade["set_command_disabled"]("修仙签到", disabled=True)
    assert repository.path.read_bytes() == original
    assert not facade["is_command_disabled"]("修仙签到")


def test_legacy_save_has_no_pending_memory_to_flush_or_missing_file_to_create(store):
    repository, facade = store
    facade["save_command_disable_memory"]()
    assert facade["load_command_disable_memory"]() == {}
    assert not repository.path.exists()
