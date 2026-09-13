from __future__ import annotations

from typing import Any


def blueprint(application: Any, *, permission):
    """Return the transport adapter without importing Flask in the feature."""
    from ...adapters.web.blueprints.accessory_package import create_blueprint

    return create_blueprint(application=application, permission=permission)


__all__ = ["blueprint"]
