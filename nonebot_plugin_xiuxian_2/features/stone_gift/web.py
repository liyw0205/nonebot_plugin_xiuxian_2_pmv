from ...adapters.web.api import create_stone_gift_blueprint


def blueprint(application, *, permission=None, ids=None):
    return create_stone_gift_blueprint(application, permission=permission, ids=ids)


__all__ = ["blueprint"]
