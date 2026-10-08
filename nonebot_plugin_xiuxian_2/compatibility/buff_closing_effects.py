from __future__ import annotations

import sys

from ..features.buff.closing_effects_application import ClosingEffectsApplication
from ..features.buff.closing_log import log_closing_event_once


def _statistics_user(user_id: str) -> str:
    """Preserve the old helper for explicit compatibility callers."""
    legacy_utils = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
    impersonating = getattr(legacy_utils, "_impersonating_users", {})
    return str(impersonating.get(str(user_id), user_id))


class LegacyBuffClosingEffects(ClosingEffectsApplication):
    """Compatibility name for the feature-owned closing effects application."""


# Explicit compatibility callers retain operation_id=f"task-progress:{operation_id}".
__all__ = ["LegacyBuffClosingEffects", "log_closing_event_once", "_statistics_user"]
