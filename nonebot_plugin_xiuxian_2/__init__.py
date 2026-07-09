from pathlib import Path
from pkgutil import iter_modules

from nonebot import get_driver, load_all_plugins, require

from .paths import configure_paths_from_nonebot

package_dir = Path(__file__).parent
configure_paths_from_nonebot(get_driver().config)
plugin_modules = [
    f"{__name__}.xiuxian.{module.name}"
    for module in iter_modules([str(package_dir / "xiuxian")])
    if not module.name.startswith("_")
]

require("nonebot_plugin_apscheduler")
load_all_plugins(plugin_modules, [])
