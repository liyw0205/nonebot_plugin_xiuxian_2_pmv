ROUTES = ()
LEGACY_ROUTES = (
    ("GET", "/config", "PluginConfigApplication.config_by_category"),
    ("POST", "/save_config", "PluginConfigApplication.save_values"),
)

__all__ = ["LEGACY_ROUTES", "ROUTES"]
