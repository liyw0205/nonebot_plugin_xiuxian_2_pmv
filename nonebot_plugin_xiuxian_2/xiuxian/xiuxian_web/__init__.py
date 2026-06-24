from .core import *  # noqa: F401,F403
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


def run_flask():
    app.run(host=HOST, port=PORT, debug=False)


if XiuConfig().web_status:
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    logger.info(f"修仙管理面板已启动：{HOST}:{PORT}")
