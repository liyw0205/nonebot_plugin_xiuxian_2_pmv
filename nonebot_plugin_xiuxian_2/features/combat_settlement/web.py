from __future__ import annotations

from typing import Any


def blueprint(application: Any, *, permission):
    from ...adapters.web.blueprints.combat_settlement import create_blueprint

    return create_blueprint(application=application, permission=permission)


__all__ = ["blueprint"]
