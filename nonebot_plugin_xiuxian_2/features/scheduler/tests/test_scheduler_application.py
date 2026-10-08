from __future__ import annotations

from unittest.mock import Mock

import pytest

from nonebot_plugin_xiuxian_2.features.scheduler.application import SchedulerAdminApplication


_CASES = (
    ("list_jobs", ()),
    ("set_enabled", ("daily-reset", False)),
    ("reschedule", ("daily-reset", {"type": "interval", "seconds": 60})),
    ("queue_manual_run", ("daily-reset",)),
    ("get_run", ("run-1",)),
)


@pytest.mark.parametrize(("method", "args"), _CASES)
def test_scheduler_admin_application_delegates_without_changing_arguments_or_result(
    method: str,
    args: tuple,
) -> None:
    manager = Mock()
    expected = {"value": object()}
    getattr(manager, method).return_value = expected
    application = SchedulerAdminApplication(manager)

    result = getattr(application, method)(*args)

    assert result is expected
    getattr(manager, method).assert_called_once_with(*args)


@pytest.mark.parametrize(("method", "args"), _CASES)
def test_scheduler_admin_application_preserves_manager_exceptions(
    method: str,
    args: tuple,
) -> None:
    manager = Mock()
    failure = ValueError("manager failure")
    getattr(manager, method).side_effect = failure
    application = SchedulerAdminApplication(manager)

    with pytest.raises(ValueError) as raised:
        getattr(application, method)(*args)

    assert raised.value is failure
    getattr(manager, method).assert_called_once_with(*args)
