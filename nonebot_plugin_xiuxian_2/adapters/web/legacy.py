from __future__ import annotations

import warnings

from flask import Blueprint, redirect

from ...compatibility.commands import record_compatibility_hit
from .blueprints._common import guard


LEGACY_REDIRECTS = {
    "/config": "/pages/config",
    "/database": "/pages/database",
    "/scheduler": "/pages/scheduler",
    "/backups": "/pages/backups",
    "/activity": "/pages/activity",
    "/logs": "/pages/logs",
    "/economy_logs": "/pages/economy_logs",
    "/commands": "/pages/commands",
    "/command_registry": "/pages/command_registry",
    "/reward-center": "/pages/reward_center",
    "/update": "/pages/update",
}


def create_legacy_blueprint(permission=None):
    blueprint = Blueprint("legacy_web", __name__)
    resolver = permission or (lambda _required: True)
    for index, (source, target) in enumerate(LEGACY_REDIRECTS.items()):
        @guard("admin", resolver)
        def redirect_endpoint(source=source, target=target):
            warnings.warn(
                f"legacy web URL {source} is deprecated; use {target}",
                DeprecationWarning,
                stacklevel=2,
            )
            record_compatibility_hit(f"web:{source}")
            return redirect(target, code=308)

        blueprint.add_url_rule(source, f"legacy_redirect_{index}", redirect_endpoint, methods=("GET",))
    return blueprint


__all__ = ["LEGACY_REDIRECTS", "create_legacy_blueprint"]
