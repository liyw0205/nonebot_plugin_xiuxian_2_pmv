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

from ..command_disable import (
    COMMAND_DISABLE_EXEMPT_MODULE,
    apply_disable_targets,
    collect_command_list_groups,
    collect_command_list_rows,
    set_command_disabled,
)


@app.route("/command_registry")
def command_registry():
    if "admin_id" not in session:
        return redirect(url_for("login"))

    q = (request.args.get("q") or "").strip()
    only_disabled = request.args.get("only_disabled") in ("1", "true", "yes")
    groups = collect_command_list_groups(q, only_disabled=only_disabled)
    rows = collect_command_list_rows(q, only_disabled=only_disabled)
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

    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return api_error("缺少指令名")

    if data.get("disabled") is not None:
        disabled = bool(data.get("disabled"))
    elif data.get("enabled") is not None:
        disabled = not bool(data.get("enabled"))
    else:
        return api_error("请指定 enabled 或 disabled")

    ok, err = set_command_disabled(name, disabled=disabled)
    if not ok:
        return api_error(err or "操作失败")
    return api_success(name=name, disabled=disabled)


@app.route("/api/command_registry/bulk_toggle", methods=["POST"])
def api_command_registry_bulk_toggle():
    if "admin_id" not in session:
        return api_error("未登录", status=401)

    data = request.get_json(silent=True) or {}
    module = (data.get("module") or "").strip()
    if not module:
        return api_error("缺少子模块名")
    if module == COMMAND_DISABLE_EXEMPT_MODULE:
        return api_error("管理员模块不参与指令禁用")

    if data.get("disabled") is not None:
        disabled = bool(data.get("disabled"))
    else:
        return api_error("请指定 disabled")

    changed, errors = apply_disable_targets(module, disabled=disabled)
    if not changed:
        msg = errors[0] if errors else "无变更"
        return api_error(msg)
    return api_success(module=module, disabled=disabled, count=len(changed))
