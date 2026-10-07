from typing import Any

from .core import (
    api_error,
    api_success,
    app,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from ...features.admin.command_control_application import AdminCommandControlApplication
from ...features.admin.command_control_repository import COMMAND_DISABLE_EXEMPT_MODULE


def _command_list_groups(rows: list[tuple[str, str, str]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    current_mod: str | None = None
    bucket: list[dict[str, Any]] = []

    def flush(mod_key: str) -> None:
        nonlocal bucket
        if not bucket:
            return
        groups.append(
            {
                "module": mod_key,
                "label": mod_key or "（未归类）",
                "commands": bucket,
                "total": len(bucket),
                "disabled_count": sum(1 for command in bucket if command["disabled"]),
            }
        )
        bucket = []

    for name, module, status in rows:
        mod_key = module or ""
        if mod_key != current_mod:
            flush(current_mod if current_mod is not None else "")
            current_mod = mod_key
        bucket.append({"name": name, "disabled": status == "禁用"})
    if current_mod is not None or bucket:
        flush(current_mod if current_mod is not None else "")
    return groups


@app.route("/command_registry")
def command_registry():
    if "admin_id" not in session:
        return redirect(url_for("login"))

    q = (request.args.get("q") or "").strip()
    only_disabled = request.args.get("only_disabled") in ("1", "true", "yes")
    rows = AdminCommandControlApplication().collect_command_list_rows(
        q, only_disabled=only_disabled
    )
    groups = _command_list_groups(rows)
    total = len(rows)
    disabled_n = sum(1 for _, _, s in rows if s == "禁用")

    return render_template(
        "command_registry.html",
        groups=groups,
        filters={"q": q, "only_disabled": only_disabled},
        stats={"total": total, "disabled": disabled_n, "modules": len(groups)},
    )


@app.route("/api/command_registry/toggle", methods=["POST"])
def api_command_registry_toggle():
    if "admin_id" not in session:
        return api_error("未登录", status=401)

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        data = {}
    name = (data.get("name") or "").strip()
    if not name:
        return api_error("缺少指令名")

    if data.get("disabled") is not None:
        disabled = bool(data.get("disabled"))
    elif data.get("enabled") is not None:
        disabled = not bool(data.get("enabled"))
    else:
        return api_error("请指定 enabled 或 disabled")

    ok, err = AdminCommandControlApplication().set_command_disabled(name, disabled=disabled)
    if not ok:
        return api_error(err or "操作失败")
    return api_success(name=name, disabled=disabled)


@app.route("/api/command_registry/bulk_toggle", methods=["POST"])
def api_command_registry_bulk_toggle():
    if "admin_id" not in session:
        return api_error("未登录", status=401)

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        data = {}
    module = (data.get("module") or "").strip()
    if not module:
        return api_error("缺少子模块名")
    if module == COMMAND_DISABLE_EXEMPT_MODULE:
        return api_error("管理员模块不参与指令禁用")

    if data.get("disabled") is not None:
        disabled = bool(data.get("disabled"))
    else:
        return api_error("请指定 disabled")

    changed, errors = AdminCommandControlApplication().apply_disable_targets(
        module, disabled=disabled
    )
    if not changed:
        msg = errors[0] if errors else "无变更"
        return api_error(msg)
    return api_success(module=module, disabled=disabled, count=len(changed))
