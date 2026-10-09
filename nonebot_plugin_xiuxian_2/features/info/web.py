"""Feature entry point for the player-info Web adapter."""

from __future__ import annotations

from typing import Any


def blueprint(application: Any, *, permission: Any):
    from ...adapters.web.blueprints.info import create_blueprint

    return create_blueprint(application=application, permission=permission)


__all__ = ["blueprint"]
