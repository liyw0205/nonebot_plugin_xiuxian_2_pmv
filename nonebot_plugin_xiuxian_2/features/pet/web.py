from typing import Any


def blueprint(application: Any, *, permission):
    from ...adapters.web.blueprints.pet import create_blueprint

    return create_blueprint(application=application, permission=permission)


__all__ = ["blueprint"]
