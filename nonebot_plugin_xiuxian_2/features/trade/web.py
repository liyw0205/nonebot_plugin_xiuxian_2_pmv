def blueprint(application, *, permission):
    from ...adapters.web.blueprints.legacy_feature import create_blueprint
    return create_blueprint(
        "trade",
        application,
        ("deposit", "withdraw", "enqueue", "dequeue", "session_start", "session_finish", "purchase"),
        permission,
        action_permissions={"session_start": "admin", "session_finish": "admin"},
    )


__all__ = ["blueprint"]
