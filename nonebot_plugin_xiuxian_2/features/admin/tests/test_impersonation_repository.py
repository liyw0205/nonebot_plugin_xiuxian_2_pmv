from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import Mock

import pytest

from ..impersonation_application import AdminImpersonationApplication
from ..impersonation_repository import AdminImpersonationRepository


def test_application_preserves_empty_injected_repository_and_shared_default():
    repository = AdminImpersonationRepository()
    application = AdminImpersonationApplication(repository)
    assert not repository
    assert application.repository is repository
    assert application.mapping is repository
    assert AdminImpersonationApplication().repository is AdminImpersonationApplication().repository
    assert application.repository is not AdminImpersonationApplication().repository


def test_mapping_and_application_share_exact_identity_keys_without_persistence():
    repository = AdminImpersonationRepository()
    application = AdminImpersonationApplication(repository)
    assert application.set_target(" real-admin ", "unregistered/target") is None
    assert application.get_target(" real-admin ") == "unregistered/target"
    assert application.get_target("real-admin") is None
    application.mapping[" real-admin "] = "next-target"
    assert application.get_target(" real-admin ") == "next-target"
    assert application.set_target(" real-admin ", "third-target") == "next-target"
    copied = repository.copy()
    copied[" real-admin "] = "detached"
    assert repository[" real-admin "] == "third-target"
    assert application.cancel(" real-admin ") == "third-target"
    assert application.cancel(" real-admin ") is None
    assert AdminImpersonationRepository().snapshot() == {}


def test_legacy_mapping_get_pop_clear_and_snapshot_iteration():
    repository = AdminImpersonationRepository()
    repository.update({"one": "target-a", "two": "target-b"})
    iterator = iter(repository)
    entries = repository.items()
    repository.clear()
    assert list(iterator) == ["one", "two"]
    assert dict(entries) == {"one": "target-a", "two": "target-b"}
    assert repository.get("missing", "fallback") == "fallback"
    assert repository.pop("missing", None) is None
    with pytest.raises(KeyError):
        repository.pop("missing")
    assert repository.setdefault("one", "target") == "target"
    assert repository.setdefault("one", "ignored") == "target"
    assert repository.pop("one") == "target"
    assert not repository


@pytest.mark.parametrize("admin,target", [("", "target"), ("admin", ""), ("admin", " "), (None, "target")])
def test_invalid_set_does_not_replace_existing_state(admin, target):
    repository = AdminImpersonationRepository()
    repository.set("admin", "existing")
    with pytest.raises(ValueError):
        repository.set(admin, target)
    assert repository.snapshot() == {"admin": "existing"}


def test_concurrent_cancel_has_one_winner_and_no_contains_delete_race():
    repository = AdminImpersonationRepository()
    repository.set("admin", "target")
    ready = Barrier(8)

    def cancel():
        ready.wait()
        return repository.cancel("admin")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: cancel(), range(8)))
    assert results.count("target") == 1
    assert results.count(None) == 7
    assert repository.snapshot() == {}


def test_concurrent_get_set_cancel_only_exposes_complete_targets():
    repository = AdminImpersonationRepository()
    targets = {f"target-{index}" for index in range(4)}

    def worker(index):
        for _ in range(100):
            repository.set("admin", f"target-{index}")
            assert repository.get("admin") in targets | {None}
            assert repository.cancel("admin") in targets | {None}

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(worker, range(4)))
    assert repository.snapshot() == {}


def test_target_resolution_preserves_mention_name_then_arbitrary_id_order():
    application = AdminImpersonationApplication(AdminImpersonationRepository())
    by_id = Mock(return_value=None)
    by_name = Mock(return_value={"user_id": "named-id", "user_name": "name"})
    assert application.resolve_target("name", mentioned_id="mentioned-id", by_id=by_id,
                                      by_name=by_name) == ("mentioned-id", None)
    by_id.assert_called_once_with("mentioned-id")
    by_name.assert_not_called()
    target, profile = application.resolve_target(" name ", by_id=by_id, by_name=by_name)
    assert target == "named-id" and profile["user_name"] == "name"
    by_id.assert_called_once()
    by_name.return_value = None
    assert application.resolve_target("unregistered-id", by_id=by_id, by_name=by_name) == ("unregistered-id", None)
    assert application.resolve_target("", by_id=by_id, by_name=by_name) == (None, None)
    assert application.repository.snapshot() == {}


def test_target_lookup_exception_never_mutates_existing_impersonation():
    application = AdminImpersonationApplication(AdminImpersonationRepository())
    application.set_target("admin", "original-target")
    with pytest.raises(RuntimeError):
        application.resolve_target("name", by_id=Mock(), by_name=Mock(side_effect=RuntimeError("unavailable")))
    assert application.get_target("admin") == "original-target"
