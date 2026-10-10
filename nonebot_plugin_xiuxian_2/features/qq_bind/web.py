ROUTES = ()
LEGACY_ROUTES = (
    ("POST", "/api/config/qq-bind/start", "QqBindApplication.start"),
    ("GET", "/api/config/qq-bind/qr/<task_id>", "QqBindApplication.qr_png"),
    ("POST", "/api/config/qq-bind/poll", "QqBindApplication.poll"),
    ("GET", "/api/config/qq-bind/restart-capability", "QqBindApplication.restart_capability"),
    ("POST", "/api/config/qq-bind/restart", "QqBindApplication.restart"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
