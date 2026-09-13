"""Lazy bridges from stable manifest job IDs to legacy implementations."""

from __future__ import annotations

import importlib
import warnings
from typing import Any, Callable


_ALIASES = {
    "daily_reset_stone_limits": "daily_reset_stone_limits_job",
}

# Jobs outside the historical scheduler module keep their legacy implementation
# for the compatibility release, but are resolved explicitly rather than by
# guessing a module from the job id.
_TARGETS = {
    "auto_guishi_transactions": ("xiuxian.xiuxian_trade", "auto_guishi_transactions_job"),
    "auto_harvest": ("xiuxian.xiuxian_puppet", "auto_harvest_scheduled"),
    "auto_start_auction": ("xiuxian.xiuxian_trade", "auto_start_auction_job"),
    "check_auction_end": ("xiuxian.xiuxian_trade", "check_auction_end_job"),
    "clear_expired_baitan_orders": ("xiuxian.xiuxian_trade", "clear_expired_baitan_orders_job"),
    "daily_dungeon_reset": ("xiuxian.xiuxian_dungeon", "daily_dungeon_reset"),
    "demon_invasion_refresh_schedule": (
        "xiuxian.xiuxian_world_events",
        "demon_invasion_refresh_schedule_job",
    ),
    "demon_invasion_schedule": ("xiuxian.xiuxian_world_events", "demon_invasion_schedule_job"),
    "generate_all_bosses": ("xiuxian.xiuxian_boss", "generate_all_bosses_task"),
    "newapi_auto_checkin_daily": (
        "xiuxian.xiuxian_entertainment.mod.newapi_scheduler",
        "newapi_auto_checkin_daily",
    ),
    "recover_user_stamina": ("xiuxian.xiuxian_utils.lay_out", "limit_all_stamina_"),
    "reset_message_rate_limits": ("xiuxian.xiuxian_utils.lay_out", "limit_all_message_"),
    "sect_materials_grant": ("xiuxian.xiuxian_sect", "materialsupdate_"),
    "spirit_vein_schedule": ("xiuxian.xiuxian_world_events", "spirit_vein_schedule_job"),
}


def legacy_job_handler(job_id: str) -> Callable[..., Any]:
    """Return a callable that resolves the old module only when executed.

    This keeps CLI health checks usable without initializing NoneBot while the
    deployed plugin still executes the exact APScheduler handler.
    """

    stable_id = str(job_id)
    target_name = _ALIASES.get(stable_id, stable_id)
    target_module, target_name = _TARGETS.get(
        stable_id,
        ("xiuxian.xiuxian_scheduler", target_name),
    )

    def handler(*args: Any, **kwargs: Any) -> Any:
        warnings.warn(
            f"legacy job {stable_id} is deprecated; migrate it before the removal release",
            DeprecationWarning,
            stacklevel=2,
        )
        try:
            from .commands import record_compatibility_hit

            record_compatibility_hit(f"job:{stable_id}")
        except Exception:
            # A telemetry failure must not prevent a scheduled maintenance job
            # from running during the compatibility period.
            pass
        module = importlib.import_module(f"nonebot_plugin_xiuxian_2.{target_module}")
        target = getattr(module, target_name, None)
        if not callable(target):
            raise RuntimeError(f"legacy job handler is unavailable: {stable_id}")
        return target(*args, **kwargs)

    handler.__name__ = f"legacy_{stable_id}"
    handler.__qualname__ = handler.__name__
    return handler


__all__ = ["legacy_job_handler"]
