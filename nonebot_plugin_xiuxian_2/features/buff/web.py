def blueprint(application, *, permission):
    from ...adapters.web.blueprints.legacy_feature import create_blueprint
    return create_blueprint("buff", application, ("open", "upgrade_field", "rename", "training_start", "training_complete", "stone_training", "pvp_settle"), permission)


__all__ = ["blueprint"]
