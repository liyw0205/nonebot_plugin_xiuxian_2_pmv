"""Legacy Web delegation for the operational log readers."""

ROUTES = ()
LEGACY_ROUTES = (
    ("GET", "/api/logs/users", "LogsApplication.users"),
    ("GET", "/api/logs/user_messages", "LogsApplication.user_messages"),
    ("GET", "/api/logs/files", "LogsApplication.files"),
    ("GET", "/api/logs/read", "LogsApplication.read"),
    ("GET", "/api/logs/tail", "LogsApplication.tail"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
