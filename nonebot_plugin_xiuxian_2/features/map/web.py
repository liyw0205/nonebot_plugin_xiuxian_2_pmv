def blueprint(application, *, permission):
    from ...adapters.web.blueprints.legacy_feature import create_blueprint
    return create_blueprint("map", application, ("move", "return_home", "interactive_start", "interactive_finish", "combat_start", "combat_settle", "explore_start", "explore_settle", "resource_reward", "mission_claim", "purchase_seed", "build_dongfu"), permission)


__all__ = ["blueprint"]
