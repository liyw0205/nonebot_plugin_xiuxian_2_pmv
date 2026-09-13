from typing import Any


def blueprint(application: Any, *, permission):
    from ...adapters.web.blueprints.tower import create_blueprint

    return create_blueprint(application=application, permission=permission)


__all__ = ["blueprint"]
