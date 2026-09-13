def blueprint(application, *, permission):
    from ...adapters.web.blueprints.legacy_feature import create_blueprint
    return create_blueprint("rift", application, ("generate", "enter", "terminate", "event_settle", "speedup", "settle"), permission)


__all__ = ["blueprint"]
