from pathlib import Path
from pkgutil import iter_modules

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
try:
    _force_builtin_qq_early()
except Exception:
    pass

package_dir = Path(__file__).parent
configure_paths_from_nonebot(get_driver().config)

# 非玩法子插件 / 库模块：不要当作 nonebot 插件加载
_INTERNAL_PACKAGES = {
    "infrastructure",
    "messaging",
    "qq_compat",
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

plugin_modules = [
    f"{__name__}.xiuxian.{module.name}"
    for module in iter_modules([str(package_dir / "xiuxian")])
    if not module.name.startswith("_") and module.name not in _INTERNAL_PACKAGES
]

require("nonebot_plugin_apscheduler")
load_all_plugins(plugin_modules, [])
