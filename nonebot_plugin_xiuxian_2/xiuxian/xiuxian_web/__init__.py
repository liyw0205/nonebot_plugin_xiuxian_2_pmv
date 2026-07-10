import asyncio
import threading

from nonebot import get_driver
from nonebot.log import logger
from werkzeug.serving import BaseWSGIServer, make_server

from .core import (
    HOST,
    PORT,
    XiuConfig,
    app,
    initialize_web_storage,
)
from .config import (  # noqa: F401
    CONFIG_EDITABLE_FIELDS,
    EXCLUDED_CONFIG_FIELDS,
    format_list_value_for_display,
    get_command_icon,
    get_config_category_icon,
    get_config_values,
    get_root_rate,
    save_config_values,
)

# Import route modules so their @app.route decorators register on the shared app.
from . import pages as _pages_routes  # noqa: F401,E402
from . import backups as _backups_routes  # noqa: F401,E402
from . import database as _database_routes  # noqa: F401,E402
from . import commands as _commands_routes  # noqa: F401,E402
from . import system as _system_routes  # noqa: F401,E402
from . import logs as _logs_routes  # noqa: F401,E402
from . import messages as _messages_routes  # noqa: F401,E402
from . import economy_logs as _economy_logs_routes  # noqa: F401,E402
from . import activity as _activity_routes  # noqa: F401,E402
from . import reward_center as _reward_center_routes  # noqa: F401,E402
from . import command_registry_web as _command_registry_routes  # noqa: F401,E402
from . import scheduler as _scheduler_routes  # noqa: F401,E402


_server: BaseWSGIServer | None = None
_server_thread: threading.Thread | None = None


def _web_enabled() -> bool:
    value = getattr(get_driver().config, "xiuxian_web_status", XiuConfig().web_status)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def start_web_server() -> None:
    global _server, _server_thread
    if _server is not None or not _web_enabled():
        return
    initialize_web_storage()
    host = str(getattr(get_driver().config, "xiuxian_web_host", HOST))
    port = int(getattr(get_driver().config, "xiuxian_web_port", PORT))
    server = make_server(host, port, app, threaded=True)
    thread = threading.Thread(
        target=server.serve_forever,
        name="xiuxian-web",
        daemon=True,
    )
    _server = server
    _server_thread = thread
    thread.start()
    logger.info(f"修仙管理面板已启动：{host}:{server.server_port}")


def stop_web_server() -> None:
    global _server, _server_thread
    server = _server
    thread = _server_thread
    _server = None
    _server_thread = None
    if server is None:
        return

    server.shutdown()
    server.server_close()
    if thread is not None and thread.is_alive():
        thread.join(timeout=5)
    logger.info("修仙管理面板已停止")


driver = get_driver()


@driver.on_startup
async def start_web_server_on_startup() -> None:
    try:
        await asyncio.to_thread(start_web_server)
    except Exception as exc:
        logger.error(f"修仙管理面板启动失败：{exc}")


@driver.on_shutdown
async def stop_web_server_on_shutdown() -> None:
    await asyncio.to_thread(stop_web_server)
