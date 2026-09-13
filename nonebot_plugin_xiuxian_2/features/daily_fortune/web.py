from __future__ import annotations

from ...adapters.web import create_api_blueprint
from .application import DailyFortuneApplication


def blueprint(application: DailyFortuneApplication, *, permission=None, ids=None):
    return create_api_blueprint(application, permission=permission, ids=ids)


__all__ = ["blueprint"]
