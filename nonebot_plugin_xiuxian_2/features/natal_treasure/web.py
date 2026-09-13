def blueprint(application, *, permission):
    from ...adapters.web.blueprints.legacy_feature import create_blueprint
    return create_blueprint("natal_treasure", application, ("awaken", "reawaken", "train", "upgrade", "engrave", "forget"), permission)


__all__ = ["blueprint"]
