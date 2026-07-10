from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from nonebot import get_driver
from nonebot.log import logger

from .infrastructure import BackgroundJobQueue


driver = get_driver()
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
    value = getattr(driver.config, name, default)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


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
    from .xiuxian_utils.pet_system import migrate_pet_storage_once

    migrated = migrate_pet_storage_once()
    if migrated:
        logger.info(f"宠物数据库自动整理完成：{migrated} 条")


def _install_dependencies() -> None:
    from .xiuxian_utils.ensure_dependencies import ensure_plugin_dependencies

    ensure_plugin_dependencies()


@driver.on_startup
async def initialize_xiuxian_runtime() -> None:
    """Run filesystem, dependency and database maintenance after imports finish."""
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


@driver.on_shutdown
async def shutdown_xiuxian_runtime() -> None:
    await background_jobs.stop(drain=True)
    await critical_jobs.stop(drain=True)
