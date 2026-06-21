"""
启动时检测修仙插件 requirements.txt，缺失则对当前 Python 解释器执行 pip install。

与 nonebot_plugin_xiuxian_2_pmv_file 安装脚本对齐：
- Termux：跳过已由 pkg 提供的 numpy / Pillow / psutil / pathlib / asyncio
- 始终使用 sys.executable -m pip（nb run / venv 激活后即为虚拟环境）
- 默认清华 PyPI 镜像（可通过环境变量 XIUXIAN_PIP_INDEX 覆盖）

设 XIUXIAN_SKIP_AUTO_PIP=1 可关闭自动安装。
"""
from __future__ import annotations

import importlib.util
import os
import re
import subprocess
import sys
from pathlib import Path

from ..xiuxian_config import Xiu_Plugin

_PKG_LINE_RE = re.compile(
    r"^\s*([A-Za-z0-9][A-Za-z0-9_.-]*)\s*(?:[<>=!~].*)?$"
)

# pip 包名 -> import 检测名
_IMPORT_ALIASES: dict[str, str] = {
    "pysocks": "socks",
    "pillow": "PIL",
    "ujson": "ujson",
    "pyyaml": "yaml",
}

# Termux install_termux.sh 中与 pkg 重叠、requirements 里会跳过的项
_TERMUX_SKIP_PACKAGES = frozenset(
    {"numpy", "pillow", "psutil", "pathlib", "asyncio"}
)

_DEFAULT_INDEX = "https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple"
_ENSURE_RAN = False


def _is_termux() -> bool:
    prefix = os.environ.get("PREFIX", "/data/data/com.termux/files/usr")
    return os.path.isdir(prefix) and os.path.isfile(os.path.join(prefix, "bin", "pkg"))


def _log_info(msg: str) -> None:
    try:
        from nonebot.log import logger

        logger.opt(colors=True).info(msg)
    except Exception:
        print(msg, file=sys.stderr)


def _log_warning(msg: str) -> None:
    try:
        from nonebot.log import logger

        logger.opt(colors=True).warning(msg)
    except Exception:
        print(msg, file=sys.stderr)


def _log_error(msg: str) -> None:
    try:
        from nonebot.log import logger

        logger.opt(colors=True).error(msg)
    except Exception:
        print(msg, file=sys.stderr)


def find_requirements_txt() -> Path | None:
    """定位 requirements.txt（开发仓库根 / xiu2 项目根 / 插件上级目录）。"""
    candidates: list[Path] = []
    for parent in [Xiu_Plugin.parent] + list(Xiu_Plugin.parents)[:6]:
        candidates.append(parent / "requirements.txt")
    cwd = Path.cwd()
    candidates.extend(
        [
            cwd / "requirements.txt",
            cwd.parent / "requirements.txt",
        ]
    )
    seen: set[str] = set()
    for p in candidates:
        key = str(p.resolve()) if p.exists() else str(p)
        if key in seen:
            continue
        seen.add(key)
        if p.is_file():
            return p.resolve()
    return None


def _normalize_pkg_name(line: str) -> str | None:
    line = line.split("#", 1)[0].strip()
    if not line or line.startswith("-"):
        return None
    m = _PKG_LINE_RE.match(line)
    if not m:
        return None
    return m.group(1)


def _import_name_for_package(pkg_name: str) -> str:
    key = pkg_name.lower().replace("-", "_")
    return _IMPORT_ALIASES.get(key, pkg_name.replace("-", "_"))


def _package_importable(pkg_name: str) -> bool:
    """是否已能通过 import 使用（含 importlib.metadata 兜底）。"""
    mod = _import_name_for_package(pkg_name)
    if importlib.util.find_spec(mod) is not None:
        return True
    try:
        from importlib.metadata import PackageNotFoundError, distribution

        dist_name = pkg_name.replace("_", "-")
        try:
            distribution(dist_name)
            return True
        except PackageNotFoundError:
            pass
        try:
            distribution(pkg_name)
            return True
        except PackageNotFoundError:
            return False
    except Exception:
        return False


def parse_requirement_lines(requirements_path: Path) -> list[str]:
    lines: list[str] = []
    skip_termux = _is_termux()
    for raw in requirements_path.read_text(encoding="utf-8").splitlines():
        pkg = _normalize_pkg_name(raw)
        if not pkg:
            continue
        if skip_termux and pkg.lower() in _TERMUX_SKIP_PACKAGES:
            continue
        lines.append(raw.split("#", 1)[0].strip())
    return lines


def _pip_base_cmd() -> list[str]:
    return [sys.executable, "-m", "pip"]


def _pip_install_args() -> list[str]:
    index = (os.environ.get("XIUXIAN_PIP_INDEX") or _DEFAULT_INDEX).strip()
    args = ["install", "-U", "--upgrade-strategy", "eager"]
    if index:
        args.extend(["-i", index])
    return args


def _run_pip_install(requirement_specs: list[str], timeout: int = 600) -> tuple[bool, str]:
    if not requirement_specs:
        return True, ""
    cmd = _pip_base_cmd() + _pip_install_args() + requirement_specs
    _log_info(
        f"<yellow>修仙插件：正在安装缺失依赖（{len(requirement_specs)} 项），"
        f"Python={sys.executable}</yellow>"
    )
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PIP_DISABLE_PIP_VERSION_CHECK": "1"},
        )
    except subprocess.TimeoutExpired:
        return False, "pip 安装超时"
    except Exception as e:
        return False, str(e)
    out = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        tail = out.strip()[-2000:] if out else "无输出"
        return False, tail
    return True, out


def ensure_plugin_dependencies(*, force: bool = False) -> None:
    """
    检测并安装缺失依赖。默认每进程只执行一次；失败只打日志，不阻断加载（除关键包可再抛）。
    """
    global _ENSURE_RAN
    if _ENSURE_RAN and not force:
        return
    if os.environ.get("XIUXIAN_SKIP_AUTO_PIP", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        _log_info("<yellow>XIUXIAN_SKIP_AUTO_PIP 已设置，跳过自动 pip 安装</yellow>")
        _ENSURE_RAN = True
        return

    req_path = find_requirements_txt()
    if not req_path:
        _log_warning(
            "<yellow>未找到 requirements.txt，跳过修仙插件依赖自检。"
            "若使用 xiu2 安装，请确认项目根目录存在 requirements.txt</yellow>"
        )
        _ENSURE_RAN = True
        return

    specs = parse_requirement_lines(req_path)
    if not specs:
        _ENSURE_RAN = True
        return

    missing: list[str] = []
    for spec in specs:
        pkg = _normalize_pkg_name(spec)
        if not pkg:
            continue
        if not _package_importable(pkg):
            missing.append(spec)

    if not missing:
        _ENSURE_RAN = True
        return

    _log_warning(
        f"<yellow>修仙插件依赖缺失 {len(missing)} 项："
        f"{', '.join(_normalize_pkg_name(s) or s for s in missing)}</yellow>"
    )

    ok, detail = _run_pip_install(missing)
    if not ok:
        _log_error(
            "<red>自动 pip 安装失败。请在本机虚拟环境中手动执行：\n"
            f"  {sys.executable} -m pip install -r {req_path}\n"
            "Termux 也可：xiu2 update-deps\n"
            f"详情：{detail}</red>"
        )
        _ENSURE_RAN = True
        return

    still_missing: list[str] = []
    for spec in missing:
        pkg = _normalize_pkg_name(spec)
        if pkg and not _package_importable(pkg):
            still_missing.append(pkg)

    if still_missing:
        _log_error(
            "<red>pip 安装后仍无法导入："
            + ", ".join(still_missing)
            + f"。请检查 Python 环境是否为 {sys.executable}</red>"
        )
    else:
        _log_info("<green>修仙插件依赖自检完成，缺失包已安装。</green>")

    _ENSURE_RAN = True


def get_runtime_python_hint() -> str:
    """供状态/调试：当前解释器与常见 venv 路径提示。"""
    parts = [f"executable={sys.executable}", f"prefix={sys.prefix}"]
    home = Path.home()
    for venv in (home / "myenv", Path("/root/myenv")):
        py = venv / "bin" / "python"
        if py.is_file():
            parts.append(f"install_venv={venv}")
            break
    return "; ".join(parts)