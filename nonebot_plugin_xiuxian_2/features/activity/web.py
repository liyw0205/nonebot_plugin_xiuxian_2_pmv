from __future__ import annotations

from typing import Any


def blueprint(application: Any, *, permission: Any):
    """Read-only activity state route for the web panel."""
    from ...adapters.web.blueprints.activity import create_blueprint

    return create_blueprint(application=application, permission=permission)


__all__ = ["blueprint"]
