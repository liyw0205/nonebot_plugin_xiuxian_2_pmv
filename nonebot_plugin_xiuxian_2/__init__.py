from pathlib import Path
from pkgutil import iter_modules
import os
import sys


def _bootstrap_test_data_dir() -> None:
    """Load the package-level test bootstrap before discovery imports us.

    Root-level ``unittest discover -s .`` can import this package while it is
    walking packages, before it reaches ``tests/__init__.py``.  Importing the
    stdlib-only test bootstrap here keeps that entry point isolated too.
    """
    if "unittest" not in sys.modules or not any(
        token in {"discover", "-q", "-v"} or token.startswith("test")
        for token in sys.argv[1:]
    ):
        return
    if "XIUXIAN_DATA_DIR" in os.environ:
        return
    try:
        __import__("tests")
    except Exception:
        # Test discovery must remain able to report its own import failures;
        # production plugin loading is unaffected by this best-effort hook.
        return


_bootstrap_test_data_dir()

from nonebot import get_driver, load_all_plugins, require

from .paths import configure_paths_from_nonebot


def _force_builtin_qq_early() -> None:
    """不经过 xiuxian 包 import，避免把内部模块提前 import 导致 load_plugins 失败。"""
    import importlib.util

    early = Path(__file__).resolve().parent / "xiuxian" / "xiuxian_adapter" / "early_inject.py"
    if not early.is_file():
        return
    spec = importlib.util.spec_from_file_location(
        "nonebot_plugin_xiuxian_2_early_inject",
        early,
    )
    if spec is None or spec.loader is None:
        return
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    force = getattr(mod, "force_builtin_qq_adapter", None)
    if callable(force):
        force()


# 插件包一加载就强制内置 QQ 能力（intent/成员事件），不依赖启动脚本
# CLI commands do not open a NoneBot transport. Skipping the adapter patch
# keeps every CLI response machine-readable and leaves transport setup to the
# real driver process.
_CLI_COMMANDS = {"manifest", "health", "migrate", "reconcile", "backup", "restore", "serve"}
if not (len(sys.argv) > 1 and sys.argv[1] in _CLI_COMMANDS):
    try:
        _force_builtin_qq_early()
    except Exception:
        pass

package_dir = Path(__file__).parent

# 非玩法子插件 / 库模块：不要当作 nonebot 插件加载
_INTERNAL_PACKAGES = {
    "infrastructure",
    "messaging",
    "qq_compat",
}
_NON_PLUGIN_MODULES = {
    "xiuxian_adapter",
    "xiuxian_utils",
    "adapter_compat",
    "adapter_message_actions",
    "adapter_message_records",
    "adapter_message_sender",
    "broadcast_manager",
    "command_disable",
    "on_compat",
    "runtime",
    "xiuxian_config",
}

def _load_legacy_plugins_if_initialized() -> None:
    """Keep legacy loading for a real NoneBot process, but allow pure imports.

    Domain/application tests and CLI tooling should not need to initialize a
    global NoneBot driver merely to import the package.
    """
    try:
        driver = get_driver()
    except ValueError:
        configure_paths_from_nonebot(type("Config", (), {})())
        return

    configure_paths_from_nonebot(driver.config)
    plugin_modules = [
        f"{__name__}.xiuxian.{module.name}"
        for module in iter_modules([str(package_dir / "xiuxian")])
        if not module.name.startswith("_")
        and module.name not in _INTERNAL_PACKAGES
        and module.name not in _NON_PLUGIN_MODULES
    ]
    _record_legacy_runtime_load(plugin_modules)
    apscheduler = require("nonebot_plugin_apscheduler")
    # Legacy decorators are collected during import and activated later by
    # the refactored lifecycle, after manifests and repositories are wired.
    from .compatibility.scheduler import install_scheduler_bridge

    install_scheduler_bridge(apscheduler)
    load_all_plugins(plugin_modules, [])
    # The new composition root owns readiness, migrations and the refactored
    # Web/job registries. Legacy hooks remain only as compatibility shims.
    from .plugin import install_driver_hooks

    install_driver_hooks(driver)


def _record_legacy_runtime_load(plugin_modules: list[str]) -> None:
    """Make loading the historical packages visible to the P7 release gate.

    A process can load every old matcher without receiving a single legacy
    command.  Counting only command calls would therefore make a release look
    clean while the old runtime is still authoritative.  The marker is emitted
    once per startup, before the modules are imported, so a release baseline
    cannot hide a later compatibility runtime load.
    """
    if not plugin_modules:
        return
    try:
        from .compatibility.commands import record_compatibility_hit

        record_compatibility_hit("runtime:legacy_package_load")
    except Exception:
        # Telemetry must never prevent the historical runtime from starting;
        # command and URL shims still record their own hits independently.
        return


_load_legacy_plugins_if_initialized()
