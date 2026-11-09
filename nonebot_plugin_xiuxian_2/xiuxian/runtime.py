from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from typing import Any

from nonebot.log import logger

from ..bootstrap.legacy import register_legacy_shutdown, register_legacy_startup
from .infrastructure import BackgroundJobQueue, settings
from .messaging.media import media_resolver

# Historical ``@driver.on_shutdown`` hooks are represented by the explicit
# bootstrap bridge below; keeping the marker in this note helps operators
# recognize the compatibility migration in source audits.

background_jobs = BackgroundJobQueue(
    "background",
    max_size=1000,
    workers=2,
    overflow_policy="drop",
)
critical_jobs = BackgroundJobQueue(
    "critical",
    max_size=500,
    workers=2,
    overflow_policy="wait",
)


async def submit_background_job(operation, *, max_retries: int = 0) -> bool:
    return await background_jobs.submit(operation, max_retries=max_retries)


async def submit_critical_job(operation, *, max_retries: int = 0) -> bool:
    return await critical_jobs.submit(
        operation,
        critical=True,
        max_retries=max_retries,
    )


def _config_bool(name: str, default: bool) -> bool:
    # Deployment/test overrides follow the documented environment precedence;
    # the historical settings provider predates that contract.
    raw = os.environ.get(str(name).upper())
    if raw is not None:
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return settings.get_bool(name, default)


async def _run_blocking(label: str, function: Callable[[], Any]) -> Any:
    logger.info(f"修仙插件启动任务开始：{label}")
    result = await asyncio.to_thread(function)
    logger.info(f"修仙插件启动任务完成：{label}")
    return result


def _initialize_database() -> None:
    from .xiuxian_utils import db_backend

    db_backend.initialize_backend()


def _download_resources() -> None:
    from .xiuxian_utils.download_xiuxian_data import download_xiuxian_data

    if not download_xiuxian_data():
        raise RuntimeError("修仙资源初始化未完成")


def _maintain_database() -> None:
    from ..paths import get_paths
    from ..features.maintenance.application import compact_receipts
    from .xiuxian_utils.pet_system import migrate_pet_storage_once

    # Each maintenance step is isolated: a failure in one must not stop the other.
    paths = get_paths()
    try:
        migrated = migrate_pet_storage_once()
        if migrated:
            logger.info(f"宠物数据库自动整理完成：{migrated} 条")
    except Exception as exc:
        logger.warning(f"宠物数据库自动整理失败：{exc}")

    # Historical rows written before v1.2.10/v1.2.11 still hold full snapshots.
    # This is bounded and resumable, and it never rewrites replay columns.
    try:
        report = compact_receipts(
            paths.game_db,
            paths.player_db,
            state_path=paths.data / "maintenance" / "receipt_compaction.json",
            # The rebuild is a file rewrite, so follow the existing backup rule:
            # only reclaim once a snapshot archive already exists.
            backup_dir=paths.backups / "db_backup",
            logger=logger,
        )
        logger.info(
            "历史回执整理：%s；用时 %.2fs" % (report.summary(), report.elapsed_seconds)
        )
        for name, space in (report.space_before or {}).items():
            after = (report.space_after or {}).get(name) or {}
            if space.get("file_bytes") and after.get("file_bytes"):
                logger.info(
                    "回执整理文件大小 %s：%s B → %s B（freelist %s → %s 页）"
                    % (name, space.get("file_bytes"), after.get("file_bytes"),
                       space.get("freelist_pages"), after.get("freelist_pages"))
                )
    except Exception as exc:
        logger.warning(f"历史回执整理失败：{exc}")

    # Per-user daily-reset detail of long-finished runs: only terminal rows of a
    # date whose parent run is completed may expire (see the module docstring).
    try:
        from datetime import date

        from ..features.maintenance.reset_target_retention import prune_reset_targets

        retention = prune_reset_targets(
            paths.game_db,
            paths.player_db,
            state_path=paths.data / "maintenance" / "reset_target_retention.json",
            today=date.today(),
            logger=logger,
        )
        logger.info(
            "每日重置明细保留（cutoff=%s）：%s；用时 %.2fs"
            % (retention.cutoff, retention.summary(), retention.elapsed_seconds)
        )
    except Exception as exc:
        logger.warning(f"每日重置明细清理失败：{exc}")


def _install_dependencies() -> None:
    from .xiuxian_utils.ensure_dependencies import ensure_plugin_dependencies

    ensure_plugin_dependencies()


def _warmup_help_commands() -> int:
    from .xiuxian_utils.message_markdown import warmup_help_command_cache

    return int(warmup_help_command_cache() or 0)


@register_legacy_startup
async def initialize_xiuxian_runtime() -> None:
    """Run filesystem, dependency and database maintenance after imports finish."""
    # 启动前再钉一次：Identify 前确保 group_members + 成员事件
    try:
        import importlib.util
        from pathlib import Path

        early = Path(__file__).resolve().parent / "xiuxian_adapter" / "early_inject.py"
        spec = importlib.util.spec_from_file_location(
            "nonebot_plugin_xiuxian_2_early_inject_startup",
            early,
        )
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            force = getattr(mod, "force_builtin_qq_adapter", None)
            if callable(force):
                force()
    except Exception as exc:
        logger.debug(f"强制内置 QQ 适配能力失败: {exc}")

    # 自愈 xiuxian_config.py：版本更新同步配置可能把 \n 写成真换行
    try:
        from pathlib import Path

        from .xiuxian_utils.config_literal import heal_config_file

        cfg_path = Path(__file__).resolve().parent / "xiuxian_config.py"
        ok, msg = heal_config_file(cfg_path)
        if ok and "修复" in msg:
            logger.warning(f"[config] {msg}")
    except Exception as exc:
        logger.debug(f"配置自愈跳过: {exc}")

    if _config_bool("xiuxian_auto_install_dependencies", False):
        await _run_blocking("安装缺失依赖", _install_dependencies)

    if _config_bool("xiuxian_auto_download_resources", True):
        await _run_blocking("检查资源文件", _download_resources)

    await _run_blocking("初始化数据库后端", _initialize_database)

    if _config_bool("xiuxian_startup_database_maintenance", True):
        try:
            await _run_blocking("整理数据库", _maintain_database)
        except Exception as exc:
            logger.warning(f"修仙插件数据库整理失败：{exc}")

    await background_jobs.start()
    await critical_jobs.start()
    await submit_background_job(media_resolver.cleanup)

    # 预热帮助命令缓存：首次「xx帮助」否则会扫全仓库 AST 很慢
    try:
        n = await asyncio.to_thread(_warmup_help_commands)
        logger.info(f"帮助命令缓存预热完成：{n} 条")
    except Exception as exc:
        logger.warning(f"帮助命令缓存预热失败：{exc}")


@register_legacy_shutdown
async def shutdown_xiuxian_runtime() -> None:
    await background_jobs.stop(drain=True)
    await critical_jobs.stop(drain=True)
