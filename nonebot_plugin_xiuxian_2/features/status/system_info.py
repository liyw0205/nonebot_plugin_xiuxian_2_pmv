from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import platform
import time
from typing import Any, Callable

_AUTO_PSUTIL = object()


def _optional_psutil() -> Any | None:
    try:
        import psutil
    except ImportError:
        return None
    return psutil


def _format_duration(seconds: float) -> str:
    if seconds <= 0:
        return "未知"
    total = max(0, int(seconds))
    days, remainder = divmod(total, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{days}天{hours}小时{minutes}分{seconds}秒"


@dataclass(frozen=True)
class SystemInfoSnapshot:
    sections: tuple[tuple[str, tuple[tuple[str, str], ...]], ...]

    def render(self) -> str:
        lines = ["系统信息"]
        for section, fields in self.sections:
            lines.append("")
            lines.append(f"【{section}】")
            lines.extend(f"{name}: {value}" for name, value in fields)
        return "\n".join(lines)


class SystemInfoProvider:
    """Collect the legacy host report through injectable, read-only system APIs."""

    def __init__(
        self,
        *,
        platform_module: Any = platform,
        psutil_module: Any = _AUTO_PSUTIL,
        clock: Callable[[], float] = time.time,
        local_datetime: Callable[[float], datetime] = datetime.fromtimestamp,
    ) -> None:
        self._platform = platform_module
        self._psutil = _optional_psutil() if psutil_module is _AUTO_PSUTIL else psutil_module
        self._clock = clock
        self._local_datetime = local_datetime

    def snapshot(self) -> SystemInfoSnapshot:
        system = (
            ("平台", self._platform.platform()),
            ("系统", self._platform.system()),
            ("版本", self._platform.version()),
            ("机器", self._platform.machine()),
            ("处理器", self._platform.processor()),
            ("Python版本", self._platform.python_version()),
        )
        if self._psutil is None:
            unavailable = "psutil未安装"
            uptime = (("系统启动时间", unavailable), ("系统运行时间", unavailable))
            cpu = tuple((name, unavailable) for name in (
                "物理核心数", "逻辑核心数", "CPU使用率", "CPU频率"
            ))
            memory = tuple((name, unavailable) for name in ("总内存", "已用内存", "内存使用率"))
            disk = tuple((name, unavailable) for name in ("总磁盘空间", "已用空间", "磁盘使用率"))
        else:
            uptime = self._uptime()
            cpu = self._cpu()
            memory = self._memory()
            disk = self._disk()

        return SystemInfoSnapshot((
            ("运行时间", uptime),
            ("系统信息", system),
            ("CPU信息", cpu),
            ("内存信息", memory),
            ("磁盘信息", disk),
        ))

    def _uptime(self) -> tuple[tuple[str, str], ...]:
        try:
            boot_time = self._psutil.boot_time()
            uptime_seconds = self._clock() - boot_time
            return (
                ("系统启动时间", self._local_datetime(boot_time).strftime("%Y-%m-%d %H:%M:%S")),
                ("系统运行时间", _format_duration(uptime_seconds)),
            )
        except Exception:
            return (("系统启动时间", "获取失败"), ("系统运行时间", "获取失败"))

    def _cpu(self) -> tuple[tuple[str, str], ...]:
        try:
            frequency = "未知"
            if hasattr(self._psutil, "cpu_freq"):
                current_frequency = self._psutil.cpu_freq()
                if current_frequency.current != "未知":
                    frequency = f"{current_frequency.current:.2f}MHz"
            return (
                ("物理核心数", str(self._psutil.cpu_count(logical=False))),
                ("逻辑核心数", str(self._psutil.cpu_count(logical=True))),
                ("CPU使用率", f"{self._psutil.cpu_percent()}%"),
                ("CPU频率", frequency),
            )
        except Exception:
            return tuple((name, "获取失败") for name in (
                "物理核心数", "逻辑核心数", "CPU使用率", "CPU频率"
            ))

    def _memory(self) -> tuple[tuple[str, str], ...]:
        try:
            memory = self._psutil.virtual_memory()
            return (
                ("总内存", f"{memory.total / (1024**3):.2f}GB"),
                ("已用内存", f"{memory.used / (1024**3):.2f}GB"),
                ("内存使用率", f"{memory.percent}%"),
            )
        except Exception:
            return tuple((name, "获取失败") for name in ("总内存", "已用内存", "内存使用率"))

    def _disk(self) -> tuple[tuple[str, str], ...]:
        try:
            disk = self._psutil.disk_usage("/")
            return (
                ("总磁盘空间", f"{disk.total / (1024**3):.2f}GB"),
                ("已用空间", f"{disk.used / (1024**3):.2f}GB"),
                ("磁盘使用率", f"{disk.percent}%"),
            )
        except Exception:
            return tuple((name, "获取失败") for name in ("总磁盘空间", "已用空间", "磁盘使用率"))


__all__ = ["SystemInfoProvider", "SystemInfoSnapshot"]
