from __future__ import annotations

from ...adapters.web.api import create_sign_in_blueprint


def blueprint(application, *, permission=None, ids=None):
    return create_sign_in_blueprint(application, permission=permission, ids=ids)


__all__ = ["blueprint"]
