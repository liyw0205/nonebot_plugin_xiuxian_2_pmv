def blueprint(application, *, permission):
    from ...adapters.web.blueprints.legacy_feature import create_blueprint
    return create_blueprint("base", application, ("breakthrough", "tribulation", "rename", "stone_contest", "stone_robbery", "sign"), permission)


__all__ = ["blueprint"]
