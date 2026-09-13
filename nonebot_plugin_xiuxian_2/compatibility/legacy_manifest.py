"""Declarations for the still-loaded legacy gameplay packages.

The legacy scheduler remains implemented by APScheduler for one release
cycle, but its public job IDs are now declared before wiring so collisions are
detectable and the admin API can expose a stable inventory.
"""

from __future__ import annotations

from ..bootstrap.registry import FeatureManifest, JobSpec


_JOB_IDS = (
    "auto_guishi_transactions",
    "auto_harvest",
    "auto_start_auction",
    "auto_handle_inactive_sect_owners_job",
    "backup_database_files",
    "cleanup_media_parser_cache_job",
    "check_auction_end",
    "clear_expired_baitan_orders",
    "daily_dungeon_reset",
    "daily_add_impart_lv",
    "daily_clean_expired_items",
    "daily_reset_arena",
    "daily_reset_beg",
    "daily_reset_boss_limits",
    "daily_reset_day_num",
    "daily_reset_illusion",
    "daily_reset_impart_num",
    "daily_reset_impart_pk",
    "daily_reset_lottery",
    "daily_reset_mixelixir_num",
    "daily_reset_sect_task",
    "daily_reset_sign",
    "daily_reset_stone_limits",
    "daily_reset_two_exp",
    "daily_reset_work_refresh_num",
    "daily_reset_xiangyuan",
    "demon_invasion_refresh_schedule",
    "demon_invasion_schedule",
    "generate_all_bosses",
    "newapi_auto_checkin_daily",
    "recover_user_stamina",
    "reset_message_rate_limits",
    "reset_data_by_time_job",
    "scheduled_rift_generation_job",
    "sect_materials_grant",
    "spirit_vein_schedule",
    "weekly_reduce_arena_rank",
    "weekly_reduce_impart_lv",
    "weekly_reset_tower_floors",
)


FEATURE = FeatureManifest(
    key="legacy_scheduler",
    title="旧调度任务兼容层",
    owner="compatibility",
    jobs=tuple(
        JobSpec(
            id=job_id,
            title=f"兼容任务：{job_id}",
            owner="compatibility",
            schedule="legacy",
            retry_policy="legacy",
            idempotency_key="{job_id}:{scheduled_at}",
        )
        for job_id in _JOB_IDS
    ),
    test_tag="legacy_scheduler",
)


__all__ = ["FEATURE"]
