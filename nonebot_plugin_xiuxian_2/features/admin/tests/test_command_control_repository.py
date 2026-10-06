from __future__ import annotations

import importlib.util
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest


@pytest.fixture(scope="module")
def owner_module():
    prefix = "_isolated_admin_command_control"
    created = []

    def load(name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        created.append(name)
        spec.loader.exec_module(module)
        return module

    try:
        for suffix in ("", ".features", ".features.admin", ".infrastructure"):
            name = prefix + suffix
            module = ModuleType(name)
            module.__path__ = []
            sys.modules[name] = module
            created.append(name)
        source = Path(__file__).resolve()
        filesystem = load(
            prefix + ".infrastructure.filesystem",
            source.parents[3] / "infrastructure/filesystem/atomic.py",
        )
        owner = load(
            prefix + ".features.admin.command_control_repository",
            source.parents[1] / "command_control_repository.py",
        )
        owner.isolated_filesystem = filesystem
        yield owner
    finally:
        for name in reversed(created):
            sys.modules.pop(name, None)


@pytest.fixture
def repository(owner_module, tmp_path):
    return owner_module.AdminCommandControlRepository(tmp_path / "command_disable.json")


def test_missing_file_reads_do_not_create_state(repository):
    assert repository.read_entries() == {}
    assert repository.known_commands() == frozenset()
    assert repository.known_modules() == frozenset()
    assert repository.collect_command_list_rows() == []
    assert repository.is_command_disabled("unknown") is False
    assert repository.set_command_disabled("unknown", disabled=True)[0] is False
    assert repository.apply_disable_targets("", disabled=True)[0] == []
    assert not repository.path.exists()


def test_registry_aliases_and_defensive_copies_are_shared_only_by_path(owner_module, repository, tmp_path):
    repository.sync_command_registry({"a": "xiuxian_a", "b": "xiuxian_b"})
    repository.rebuild_alias_index({" short ": " a ", "": "b"})
    other = owner_module.AdminCommandControlRepository(repository.path.parent / "." / repository.path.name)

    assert other.resolve_primary_name(" short ") == "a"
    assert other.known_commands() == frozenset({"a", "b"})
    assert other.known_modules() == frozenset({"xiuxian_a", "xiuxian_b"})
    assert other.set_command_disabled("short", disabled=True) == (True, "")
    assert repository.is_command_disabled("a") is True
    copy = other.read_entries()
    copy["a"]["disabled"] = False
    copy["b"]["module"] = "wrong"
    assert repository.is_command_disabled("a") is True
    assert repository.commands_in_module("xiuxian_b") == ["b"]
    unrelated = owner_module.AdminCommandControlRepository(tmp_path / "other.json")
    assert unrelated.known_commands() == frozenset()
    assert unrelated.resolve_primary_name("short") == "short"


def test_wrapped_unknown_data_and_dormant_commands_survive_sync(repository):
    dormant = {"disabled": True, "module": "removed", "unknown": {"preserve": [1, 2]}}
    repository.path.write_text(json.dumps({
        "version": 9,
        "custom": ["keep"],
        "commands": {
            " a ": {"disabled": False, "module": "old", "extra": {"source": "legacy"}},
            "dormant": dormant,
            " ": "untouched unknown key",
        },
    }), encoding="utf-8")

    assert repository.sync_command_registry({"a": "xiuxian_a", "b": "xiuxian_b"}) == {
        "a": {"disabled": False, "module": "xiuxian_a"},
        "b": {"disabled": False, "module": "xiuxian_b"},
    }
    assert repository.set_command_disabled("dormant", disabled=False)[0] is False
    repository.set_command_disabled("a", disabled=True)
    saved = json.loads(repository.path.read_text(encoding="utf-8"))
    assert saved["version"] == 9
    assert saved["custom"] == ["keep"]
    assert saved["commands"]["dormant"] == dormant
    assert saved["commands"][" "] == "untouched unknown key"
    assert saved["commands"][" a "] == {
        "disabled": True, "module": "xiuxian_a", "extra": {"source": "legacy"},
    }
    repository.sync_command_registry({"a": "xiuxian_a", "dormant": "xiuxian_returned"})
    assert repository.is_command_disabled("dormant") is True
    assert repository.commands_in_module("xiuxian_returned") == ["dormant"]
    assert json.loads(repository.path.read_text(encoding="utf-8"))["commands"]["b"]["disabled"] is False


def test_flat_map_and_scalar_flags_keep_the_legacy_document_shape(repository):
    repository.path.write_text('{"a":1,"b":0,"other":{"disabled":true,"extra":[1]}}', encoding="utf-8")

    assert repository.set_command_disabled("a", disabled=False) == (True, "")

    saved = json.loads(repository.path.read_text(encoding="utf-8"))
    assert "commands" not in saved
    assert saved == {"a": False, "b": 0, "other": {"disabled": True, "extra": [1]}}
    repository.sync_command_registry({"a": "xiuxian_a"})
    saved = json.loads(repository.path.read_text(encoding="utf-8"))
    assert "commands" not in saved
    assert saved["a"] == {"disabled": False, "module": "xiuxian_a"}
    assert saved["other"] == {"disabled": True, "extra": [1]}


def test_identical_sync_and_toggles_do_not_write(repository):
    registry = {"a": "xiuxian_a", "b": "xiuxian_a"}
    repository.sync_command_registry(registry)
    repository.rebuild_alias_index({"alias": "a"})
    original = repository.path.read_bytes()

    with patch.object(repository, "_persist", side_effect=AssertionError("duplicate write")):
        assert repository.sync_command_registry(registry)["a"]["disabled"] is False
        assert repository.set_command_disabled("a", disabled=False) == (True, "")
        assert repository.apply_disable_targets("a,alias,xiuxian_a", disabled=False) == (["a", "b"], [])

    assert repository.path.read_bytes() == original


def test_failed_atomic_replace_keeps_disk_cache_and_registry_unchanged(owner_module, repository):
    repository.sync_command_registry({"a": "xiuxian_a"})
    assert repository.is_command_disabled("a") is False
    original = repository.path.read_bytes()

    with patch.object(owner_module.isolated_filesystem.os, "replace", side_effect=OSError("disk")):
        with pytest.raises(OSError, match="disk"):
            repository.set_command_disabled("a", disabled=True)
        with pytest.raises(OSError, match="disk"):
            repository.sync_command_registry({"b": "xiuxian_b"})

    assert repository.path.read_bytes() == original
    assert repository.read_entries() == {"a": {"disabled": False, "module": "xiuxian_a"}}
    assert list(repository.path.parent.glob(".command_disable.json.*.tmp")) == []


@pytest.mark.parametrize("bad", [b"{broken", b"[]", b'{"commands":[]}', b'{"commands":null}'])
def test_corrupt_files_fail_without_replacing_or_publishing_empty_state(repository, bad):
    repository.sync_command_registry({"a": "xiuxian_a"})
    repository.set_command_disabled("a", disabled=True)
    valid = repository.path.read_bytes()
    assert repository.is_command_disabled("a") is True
    cached = repository._cache[repository.path]
    repository.path.write_bytes(bad)

    for action in (
        repository.read_entries,
        lambda: repository.is_command_disabled("a"),
        lambda: repository.set_command_disabled("a", disabled=False),
        lambda: repository.apply_disable_targets("a", disabled=False),
        lambda: repository.sync_command_registry({"b": "xiuxian_b"}),
    ):
        with pytest.raises(ValueError):
            action()
        assert repository.path.read_bytes() == bad
        assert repository._cache[repository.path] == cached

    repository.path.write_bytes(valid)
    assert repository.read_entries() == {"a": {"disabled": True, "module": "xiuxian_a"}}


def test_cache_observes_external_replacement_even_with_same_size_and_timestamp(owner_module, repository, tmp_path):
    repository.path.write_text('{"a":false}', encoding="utf-8")
    assert repository.is_command_disabled("a") is False
    original = repository.path.stat()
    other_path = tmp_path / "external.json"
    other_path.write_text('{"a":true }', encoding="utf-8")
    os.utime(other_path, ns=(original.st_atime_ns, original.st_mtime_ns))
    other = owner_module.AdminCommandControlRepository(other_path)
    assert other.is_command_disabled("a") is True
    assert repository.is_command_disabled("a") is False

    os.replace(other_path, repository.path)

    assert repository.is_command_disabled("a") is True
    assert other.read_entries() == {}


def test_file_removal_keeps_runtime_registry_without_creating_a_file(repository):
    repository.sync_command_registry({"a": "xiuxian_a"})
    repository.set_command_disabled("a", disabled=True)
    repository.path.unlink()

    assert repository.read_entries() == {"a": {"disabled": False, "module": "xiuxian_a"}}
    assert repository.is_command_disabled("a") is False
    assert not repository.path.exists()


def test_batch_alias_module_precedence_deduplication_and_partial_errors(repository):
    repository.sync_command_registry({"a": "xiuxian_a", "b": "xiuxian_a", "c": "xiuxian_c"})
    repository.rebuild_alias_index({"alias": "a", "a": "c"})

    with patch.object(repository, "_persist", wraps=repository._persist) as persist:
        changed, errors = repository.apply_disable_targets("a,alias,alias,xiuxian_a,unknown,xiuxian_missing", disabled=True)

    assert changed == ["a", "b"]
    assert len(errors) == 2
    assert "unknown" in errors[0]
    assert "xiuxian_missing" in errors[1]
    assert persist.call_count == 1
    assert repository.read_entries()["a"]["disabled"] is True
    assert repository.read_entries()["c"]["disabled"] is False
    assert repository.set_command_disabled("a", disabled=True) == (True, "")
    assert repository.read_entries()["c"]["disabled"] is True


def test_admin_exemption_covers_single_bulk_alias_and_route_reads(repository):
    repository.path.write_text(json.dumps({"commands": {
        "admin": {"disabled": True, "module": "xiuxian_admin"},
        "normal": {"disabled": False, "module": "xiuxian_other"},
    }}), encoding="utf-8")
    repository.rebuild_alias_index({"admin-alias": "admin"})
    original = repository.path.read_bytes()

    assert repository.set_command_disabled("admin", disabled=True)[0] is False
    assert repository.set_command_disabled("admin-alias", disabled=False)[0] is False
    changed, errors = repository.apply_disable_targets("admin,admin-alias,xiuxian_admin", disabled=True)
    assert changed == []
    assert len(errors) == 3
    assert repository.commands_in_module("xiuxian_admin") == []
    assert repository.is_command_disabled("admin") is False
    assert repository.is_command_disabled("admin-alias") is False
    assert [row[0] for row in repository.collect_command_list_rows()] == ["normal"]
    assert repository.path.read_bytes() == original


def test_lists_preserve_filter_order_status_and_disabled_selection(repository):
    repository.sync_command_registry({"b": "xiuxian_z", "a": "xiuxian_a", "loose": "", "admin": "xiuxian_admin"})
    repository.set_command_disabled("a", disabled=True)

    assert repository.collect_command_list_rows() == [
        ("a", "xiuxian_a", "禁用"), ("b", "xiuxian_z", "启用"), ("loose", "", "启用"),
    ]
    assert repository.collect_command_list_rows("xiuxian_a/loose") == [
        ("a", "xiuxian_a", "禁用"), ("loose", "", "启用"),
    ]
    assert repository.collect_command_list_rows("a，z", only_disabled=True) == [("a", "xiuxian_a", "禁用")]
    assert repository.collect_command_list_rows("not-found") == []


def test_multiple_instances_serialize_mutations_without_lost_updates(owner_module, repository):
    registry = {f"command-{index}": "xiuxian_test" for index in range(16)}
    repository.sync_command_registry(registry)
    repository.rebuild_alias_index({f"alias-{index}": name for index, name in enumerate(registry)})

    def disable(index):
        other = owner_module.AdminCommandControlRepository(repository.path)
        return other.set_command_disabled(f"alias-{index}", disabled=True)

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(disable, range(16))) == [(True, "")] * 16

    assert all(entry["disabled"] for entry in repository.read_entries().values())
    assert len(repository.collect_command_list_rows(only_disabled=True)) == 16


def test_invalid_input_does_not_publish_registry_or_alias_changes(repository):
    repository.sync_command_registry({"a": "xiuxian_a"})
    repository.rebuild_alias_index({"old": "a"})
    original = repository.path.read_bytes()

    with pytest.raises(TypeError):
        repository.sync_command_registry({"b": None})
    with pytest.raises(TypeError):
        repository.rebuild_alias_index({"new": None})
    with pytest.raises(TypeError):
        repository.set_command_disabled("a", disabled="false")
    with pytest.raises(TypeError):
        repository.apply_disable_targets("a", disabled=1)

    assert repository.path.read_bytes() == original
    assert repository.known_commands() == frozenset({"a"})
    assert repository.resolve_primary_name("old") == "a"
    assert repository.resolve_primary_name("new") == "new"


def test_route_candidate_lookups_normalize_once_without_cloning_the_document(owner_module, repository):
    commands = {f"command-{index}": {"disabled": bool(index % 2), "module": "xiuxian_test"} for index in range(12)}
    repository.path.write_text(json.dumps({"commands": commands}), encoding="utf-8")
    other = owner_module.AdminCommandControlRepository(repository.path)

    with patch.object(repository, "_entry", wraps=repository._entry) as normalize:
        with patch.object(owner_module.copy, "deepcopy", side_effect=AssertionError("route cloned document")):
            for _ in range(3):
                for index in range(12):
                    assert repository.is_command_disabled(f"command-{index}") is bool(index % 2)
                    assert other.is_command_disabled(f"command-{index}") is bool(index % 2)

    assert normalize.call_count == len(commands)


def test_active_view_refreshes_only_after_successful_writes_and_file_replacement(repository, tmp_path):
    repository.sync_command_registry({"a": "xiuxian_a"})
    assert repository.is_command_disabled("a") is False
    initial = repository._active_cache[repository.path]

    repository.set_command_disabled("a", disabled=True)
    assert repository.path not in repository._active_cache
    assert repository.is_command_disabled("a") is True
    changed = repository._active_cache[repository.path]
    assert changed is not initial
    with patch.object(repository, "_persist", side_effect=OSError("failed")):
        with pytest.raises(OSError, match="failed"):
            repository.set_command_disabled("a", disabled=False)
    assert repository._active_cache[repository.path] is changed
    assert repository.is_command_disabled("a") is True

    replacement = tmp_path / "replacement.json"
    replacement.write_text('{"commands":{"a":{"disabled":false,"module":"xiuxian_a"}}}', encoding="utf-8")
    os.replace(replacement, repository.path)
    assert repository.is_command_disabled("a") is False
    assert repository._active_cache[repository.path] is not changed
    refreshed = repository._active_cache[repository.path]
    with patch.object(repository, "_entry", side_effect=AssertionError("repeated normalization")):
        assert repository.is_command_disabled("a") is False
        assert repository.is_command_disabled("missing") is False
    assert repository._active_cache[repository.path] is refreshed


def test_registry_only_sync_refreshes_view_without_writing_and_failed_sync_keeps_it(repository):
    repository.sync_command_registry({"a": "xiuxian_a", "b": "xiuxian_b"})
    repository.apply_disable_targets("a,b", disabled=True)
    assert repository.is_command_disabled("a") is True
    initial = repository._active_cache[repository.path]
    original = repository.path.read_bytes()

    with patch.object(repository, "_persist", side_effect=OSError("unexpected write")):
        assert repository.sync_command_registry({"b": "xiuxian_b"}) == {
            "b": {"disabled": True, "module": "xiuxian_b"},
        }
        active = repository._active_cache[repository.path]
        assert active is not initial
        assert repository.is_command_disabled("a") is False
        assert repository.is_command_disabled("b") is True
        with pytest.raises(OSError, match="unexpected write"):
            repository.sync_command_registry({"new": "xiuxian_new"})
    assert repository._active_cache[repository.path] is active
    assert repository.known_commands() == frozenset({"b"})
    assert repository.path.read_bytes() == original


def test_corrupt_replacement_does_not_publish_or_serve_a_stale_active_view(repository):
    repository.sync_command_registry({"a": "xiuxian_a"})
    repository.set_command_disabled("a", disabled=True)
    assert repository.is_command_disabled("a") is True
    active = repository._active_cache[repository.path]
    repository.path.write_text("{broken", encoding="utf-8")

    for _ in range(2):
        with pytest.raises(ValueError):
            repository.is_command_disabled("a")
        assert repository._active_cache[repository.path] is active


def test_module_batch_computes_raw_key_locations_once(repository):
    repository.sync_command_registry({f"command-{index}": "xiuxian_test" for index in range(24)})

    with patch.object(repository, "_locations", wraps=repository._locations) as locations:
        changed, errors = repository.apply_disable_targets("xiuxian_test", disabled=True)

    assert len(changed) == 24
    assert errors == []
    assert locations.call_count == 1
    assert all(entry["disabled"] for entry in repository.read_entries().values())
