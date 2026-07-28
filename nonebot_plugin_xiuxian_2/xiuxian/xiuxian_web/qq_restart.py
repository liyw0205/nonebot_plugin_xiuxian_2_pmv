from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


def _supports_restart(command: Path) -> bool:
    try:
        source = command.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False
    return "restart)" in source or "restart) stop" in source


def detect_restart(project_dir: Path | None = None) -> dict[str, Any]:
    root = Path(project_dir or Path.cwd()).resolve()
    if sys.platform.startswith("win"):
        return {
            "automatic": False,
            "mode": "windows",
            "message": "请关闭当前窗口后重新运行“启动修仙.bat”。",
        }
    if Path("/.dockerenv").exists():
        return {
            "automatic": False,
            "mode": "docker",
            "message": "请在宿主机执行 Docker 安装脚本的 start，或 docker restart 当前容器。",
        }

    candidates = [root / "manage.sh"]
    project_name = root.name
    managed = shutil.which(project_name)
    if managed:
        candidates.append(Path(managed))
    for command in candidates:
        if command.is_file() and os.access(command, os.X_OK) and _supports_restart(command):
            return {
                "automatic": True,
                "mode": "manager",
                "command": str(command.resolve()),
                "message": f"将通过 {command.name} restart 重启。",
            }
    return {
        "automatic": False,
        "mode": "manual",
        "message": "当前启动方式没有可验证的外部重启管理器，请在终端停止后重新运行 nb run。",
    }


def schedule_restart(capability: dict[str, Any], delay: float = 1.0) -> bool:
    if not capability.get("automatic"):
        return False
    command = str(capability.get("command") or "")
    if not command:
        return False

    def launch() -> None:
        time.sleep(max(0.1, float(delay)))
        with open(os.devnull, "rb") as stdin, open(os.devnull, "ab") as output:
            subprocess.Popen(
                [command, "restart"],
                stdin=stdin,
                stdout=output,
                stderr=output,
                close_fds=True,
                start_new_session=True,
            )

    threading.Thread(target=launch, name="xiuxian-web-restart", daemon=True).start()
    return True
