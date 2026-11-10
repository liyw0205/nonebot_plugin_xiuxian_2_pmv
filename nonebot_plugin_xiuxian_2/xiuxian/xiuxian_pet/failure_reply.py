"""Pet commands share safe diagnostics and framework control-flow handling."""

from ..command_guard import (
    BUSY_FAILURE_TEXT,
    CONFLICT_FAILURE_TEXT,
    GENERIC_FAILURE_TEXT,
    VALIDATION_FAILURE_TEXT,
    failure_reply,
    failure_text,
    guard_command as _guard,
)


def guard_pet_handler(action, *, send=None):
    return _guard(action, send=send)
