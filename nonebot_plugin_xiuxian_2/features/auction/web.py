from typing import Any


def blueprint(application: Any, *, permission, settlement: Any | None = None, clock=None, ids=None):
    from ...adapters.web.blueprints.auction import create_blueprint

    return create_blueprint(
        application=application,
        settlement=settlement or application,
        permission=permission,
        clock=clock,
        ids=ids,
    )


__all__ = ["blueprint"]
