def blueprint(application, *, permission):
    from ...adapters.web.blueprints.legacy_feature import create_blueprint
    return create_blueprint("trade", application, ("deposit", "withdraw", "enqueue", "dequeue", "session_start", "session_finish", "purchase"), permission)


__all__ = ["blueprint"]
