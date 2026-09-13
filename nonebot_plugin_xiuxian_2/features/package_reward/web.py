"""Transport-neutral declaration for the package reward Web boundary."""

from typing import Any


def blueprint(application: Any, *, permission=None):
    from ...adapters.web.blueprints.package_reward import create_blueprint

    return create_blueprint(application=application, permission=permission)


__all__ = ["blueprint"]