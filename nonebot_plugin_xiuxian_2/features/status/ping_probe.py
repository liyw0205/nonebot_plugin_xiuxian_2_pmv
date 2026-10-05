from __future__ import annotations

import asyncio
import platform
import re
import subprocess
from concurrent.futures import Executor
from dataclasses import dataclass
from typing import Callable, Sequence


@dataclass(frozen=True)
class PingTarget:
    name: str
    host: str
    region: str


@dataclass(frozen=True)
class PingResult:
    name: str
    host: str
    region: str
    delay_ms: float
    is_timeout: bool
    emoji: str


@dataclass(frozen=True)
class PingSnapshot:
    results: tuple[PingResult, ...]

    def render(self) -> str:
        lines = ["网络延迟测试", ""]
        for region in ("国内", "国外"):
            lines.append(f"【{region}站点】")
            for result in self.results:
                if result.region != region:
                    continue
                if result.is_timeout:
                    lines.append(f"{result.emoji} {result.name}: 超时(0ms)")
                else:
                    lines.append(f"{result.emoji} {result.name}: {result.delay_ms:.3f}ms")
            lines.append("")
        return "\n".join(lines)


PING_TARGETS: tuple[PingTarget, ...] = (
    PingTarget("百度", "www.baidu.com", "国内"),
    PingTarget("腾讯", "www.qq.com", "国内"),
    PingTarget("阿里", "www.aliyun.com", "国内"),
    PingTarget("必应", "cn.bing.com", "国内"),
    PingTarget("GitHub", "github.com", "国外"),
    PingTarget("Gitee", "gitee.com", "国外"),
    PingTarget("谷歌", "www.google.com", "国外"),
    PingTarget("苹果", "www.apple.com", "国外"),
)

PING_TIMEOUT_SECONDS = 10.0
_WINDOWS_AVERAGE = re.compile(r"平均 = (\d+)ms")
_POSIX_AVERAGE = re.compile(r"min/avg/max/mdev = [\d.]+/([\d.]+)/")
CommandRunner = Callable[[Sequence[str], float], str]


def ping_emoji(delay_ms: float) -> str:
    if delay_ms == 0:
        return "💀"
    if delay_ms < 20:
        return "🚀"
    if delay_ms < 50:
        return "⚡"
    if delay_ms < 100:
        return "🐎"
    if delay_ms < 200:
        return "🐢"
    return "🐌"


def run_ping_command(command: Sequence[str], timeout: float) -> str:
    result = subprocess.run(
        list(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    return result.stdout


class PingProbe:
    """Run the fixed status probes without persistence or user-provided hosts."""

    def __init__(
        self,
        *,
        runner: CommandRunner | None = None,
        system: Callable[[], str] = platform.system,
        targets: Sequence[PingTarget] = PING_TARGETS,
        executor: Executor | None = None,
        timeout_seconds: float = PING_TIMEOUT_SECONDS,
    ) -> None:
        self._runner = runner or run_ping_command
        self._system = system
        self._targets = tuple(targets)
        self._executor = executor
        self._timeout_seconds = float(timeout_seconds)

    async def probe_all(self) -> PingSnapshot:
        system_name = str(self._system()).lower()
        parameter = "-n" if system_name == "windows" else "-c"
        results = await asyncio.gather(
            *(self._probe_target(target, system_name=system_name, parameter=parameter) for target in self._targets)
        )
        return PingSnapshot(tuple(results))

    async def _probe_target(
        self,
        target: PingTarget,
        *,
        system_name: str,
        parameter: str,
    ) -> PingResult:
        command = ("ping", parameter, "4", target.host)
        loop = asyncio.get_running_loop()
        try:
            output = await loop.run_in_executor(
                self._executor,
                self._runner,
                command,
                self._timeout_seconds,
            )
            matcher = _WINDOWS_AVERAGE if system_name == "windows" else _POSIX_AVERAGE
            match = matcher.search(str(output))
            delay_ms = float(match.group(1)) if match else 0.0
            is_timeout = match is None
        except Exception:
            delay_ms, is_timeout = 0.0, True
        return PingResult(
            name=target.name,
            host=target.host,
            region=target.region,
            delay_ms=delay_ms,
            is_timeout=is_timeout,
            emoji=ping_emoji(delay_ms),
        )


__all__ = [
    "PING_TARGETS",
    "PING_TIMEOUT_SECONDS",
    "PingProbe",
    "PingResult",
    "PingSnapshot",
    "PingTarget",
    "ping_emoji",
    "run_ping_command",
]
