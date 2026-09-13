from __future__ import annotations

from datetime import datetime
from typing import Any


def blueprint(application: Any, *, permission):
    from ...adapters.web.blueprints.tianti_training import create_blueprint

    return create_blueprint(application=application, permission=permission)


def parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


__all__ = ["blueprint", "parse_datetime"]
