def blueprint(application, *, permission):
    from ...adapters.web.blueprints.legacy_feature import create_blueprint
    return create_blueprint("back", application, ("open_package", "use_item", "change_equipment", "learn_skill", "repair", "use_pet_eggs", "alchemy", "unbind"), permission)


__all__ = ["blueprint"]
