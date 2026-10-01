import json
from datetime import datetime

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
from ..xiuxian_compensation.common import (
    DATA_CONFIG,
    clear_records,
    create_item_message,
    delete_record,
    generate_unique_id,
    get_claim_count,
    get_item_list,
    get_reward_definition,
    get_reward_used_count,
    load_data,
    runtime_ids,
    save_data,
    upsert_reward_definition,
)


REWARD_KIND_CONFIG = {
    "gift": {"title": "礼包", "config_key": "礼包"},
    "compensation": {"title": "补偿", "config_key": "补偿"},
    "redeem": {"title": "兑换码", "config_key": "兑换码"},
}

TIME_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M")


def _reward_config(kind: str) -> dict:
    meta = REWARD_KIND_CONFIG.get(str(kind or "").strip())
    if not meta:
        raise ValueError("发放类型无效")
    config = DATA_CONFIG[meta["config_key"]]
    return {**meta, "data_config": config}


def _clean_text(value, default: str = "") -> str:
    text = str(value if value is not None else "").strip()
    return text or default


def _normalize_datetime(
    value,
    field_name: str,
    *,
    empty_value=None,
    infinite: bool = False,
    allow_special: bool = True,
):
    text = _clean_text(value)
    if allow_special and infinite and text in ("", "0", "无限"):
        return "无限"
    if allow_special and not infinite and text in ("", "0"):
        return empty_value
    if not text:
        raise ValueError(f"请选择{field_name}")

    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    raise ValueError(f"{field_name}格式应为 YYYY-MM-DD HH:MM:SS")


def _normalize_record_id(kind: str, value: str, data: dict) -> str:
    record_id = _clean_text(value)
    if record_id in ("", "0", "随机"):
        return generate_unique_id(list(data.keys()))
    if any(ch.isspace() for ch in record_id) or "/" in record_id or "\\" in record_id:
        raise ValueError("编号不能包含空白字符或斜杠")
    if kind == "redeem":
        return record_id.upper()
    return record_id


def _claimed_count(config: dict, record_id: str) -> int:
    return get_claim_count(config, record_id)


def _editable_reward_text(items: list[dict]) -> str:
    parts = []
    for item in items or []:
        name = item.get("name") or item.get("id") or ""
        quantity = int(item.get("quantity") or 1)
        if name:
            parts.append(f"{name}x{quantity}")
    return ",".join(parts)


def _serialize_record(kind: str, record_id: str, record: dict, config: dict) -> dict:
    items = record.get("items") or []
    item_names = create_item_message(items)
    usage_limit = int(record.get("usage_limit") or 0)
    used_count = get_reward_used_count(
        config, record_id, int(record.get("used_count") or 0)
    )
    claimed_count = _claimed_count(config, record_id)
    return {
        "id": record_id,
        "kind": kind,
        "items": items,
        "items_text": ",".join(item_names),
        "reward_text": _editable_reward_text(items),
        "reason": record.get("reason", ""),
        "start_time": record.get("start_time"),
        "expire_time": record.get("expire_time", "无限"),
        "create_time": record.get("create_time", ""),
        "usage_limit": usage_limit,
        "used_count": used_count,
        "claimed_count": claimed_count,
        "version": int(record.get("_definition_version") or 0),
    }


def _serialize_records(kind: str) -> list[dict]:
    meta = _reward_config(kind)
    config = meta["data_config"]
    data = load_data(config)
    return [
        _serialize_record(kind, record_id, record, config)
        for record_id, record in sorted(data.items(), key=lambda item: item[0])
    ]


def _normalize_payload(payload: dict) -> tuple[str, str, dict]:
    if not isinstance(payload, dict):
        raise ValueError("请求数据无效")

    kind = _clean_text(payload.get("kind"))
    meta = _reward_config(kind)
    config = meta["data_config"]
    raw_record_id = payload.get("id")
    data = (
        load_data(config)
        if _clean_text(raw_record_id) in ("", "0", "随机")
        else {}
    )
    record_id = _normalize_record_id(kind, raw_record_id, data)
    if kind == "compensation":
        existing = load_data(config).get(record_id)
    else:
        existing = get_reward_definition(config, record_id)
    reward_items = get_item_list(_clean_text(payload.get("reward")))
    start_special = bool(payload.get("start_special"))
    expire_special = bool(payload.get("expire_special"))
    start_time = _normalize_datetime(
        payload.get("start_time"),
        "生效时间",
        empty_value=None,
        allow_special=start_special,
    )
    expire_time = _normalize_datetime(
        payload.get("expire_time"),
        "过期时间",
        infinite=True,
        allow_special=expire_special,
    )

    if start_time and expire_time != "无限":
        start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        expire_dt = datetime.strptime(expire_time, "%Y-%m-%d %H:%M:%S")
        if start_dt > expire_dt:
            raise ValueError("生效时间不能晚于过期时间")

    record = {
        "items": reward_items,
        "expire_time": expire_time,
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_time": start_time,
    }

    if kind == "redeem":
        try:
            usage_limit = int(payload.get("usage_limit") or 0)
        except (TypeError, ValueError):
            raise ValueError("使用上限必须是数字")
        if usage_limit < 0:
            raise ValueError("使用上限不能小于 0")
        record["usage_limit"] = usage_limit
    else:
        record["reason"] = _clean_text(payload.get("reason"), meta["title"])
    expected_version = payload.get("expected_version")
    if expected_version in (None, "") and existing is not None:
        expected_version = existing.get("_definition_version")
    if expected_version not in (None, ""):
        expected_version = int(expected_version)
        if expected_version <= 0:
            raise ValueError("定义版本无效")
        record["_definition_version"] = expected_version

    return kind, record_id, record


@app.route("/reward-center")
def reward_center():
    if "admin_id" not in session:
        return redirect(url_for("login"))

    return render_template(
        "reward_center.html",
        reward_kinds={
            key: {"key": key, "title": value["title"]}
            for key, value in REWARD_KIND_CONFIG.items()
        },
    )


@app.route("/api/reward-center/records")
def api_reward_records():
    if "admin_id" not in session:
        return api_error("未登录")

    try:
        kind = _clean_text(request.args.get("kind"), "gift")
        return api_success(kind=kind, records=_serialize_records(kind))
    except Exception as e:
        return api_error(str(e))


@app.route("/api/reward-center/records", methods=["POST"])
def api_save_reward_record():
    if "admin_id" not in session:
        return api_error("未登录")

    try:
        payload = request.get_json() or {}
        kind, record_id, record = _normalize_payload(payload)
        meta = _reward_config(kind)
        config = meta["data_config"]
        if kind == "compensation":
            data = load_data(config)
            data[record_id] = record
            save_data(config, data)
            saved_record = load_data(config)[record_id]
        else:
            idempotency_key = _clean_text(
                request.headers.get("Idempotency-Key") or payload.get("operation_id")
            )
            operation_id = (
                f"reward-center:{session['admin_id']}:{idempotency_key}"
                if idempotency_key
                else f"reward-center:{session['admin_id']}:{runtime_ids.new_id()}"
            )
            request_identity = json.dumps(
                payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
            )
            result = upsert_reward_definition(
                config, operation_id, request_identity, record_id, record
            )
            if not result.succeeded:
                message = (
                    "奖励定义已变化，请刷新后重试"
                    if result.status == "definition_changed"
                    else f"奖励定义保存失败：{result.status}"
                )
                return api_error(message)
            saved_record = dict(result.record or {})
        return api_success(
            message=f"{meta['title']}已保存",
            kind=kind,
            record=_serialize_record(kind, record_id, saved_record, config),
            records=_serialize_records(kind),
        )
    except Exception as e:
        return api_error(str(e))


@app.route("/api/reward-center/records/<kind>/<record_id>", methods=["DELETE"])
def api_delete_reward_record(kind, record_id):
    if "admin_id" not in session:
        return api_error("未登录")

    try:
        meta = _reward_config(kind)
        idempotency_key = request.headers.get("Idempotency-Key")
        operation_id = (
            f"reward-center:{session['admin_id']}:delete:{idempotency_key}"
            if idempotency_key
            else None
        )
        result = delete_record(record_id, meta["data_config"], operation_id)
        if not result.succeeded:
            message = (
                "补偿定义已变化，请刷新后重试"
                if kind == "compensation"
                else f"{meta['title']}删除失败：{result.status}"
            )
            return api_error(message)
        return api_success(message=f"{meta['title']}已删除", records=_serialize_records(kind))
    except Exception as e:
        return api_error(str(e))


@app.route("/api/reward-center/records/<kind>/clear", methods=["POST"])
def api_clear_reward_records(kind):
    if "admin_id" not in session:
        return api_error("未登录")

    try:
        meta = _reward_config(kind)
        idempotency_key = request.headers.get("Idempotency-Key")
        operation_id = (
            f"reward-center:{session['admin_id']}:clear:{idempotency_key}"
            if idempotency_key
            else None
        )
        result = clear_records(
            meta["data_config"], operation_id
        )
        if not result.succeeded:
            message = (
                "补偿列表已变化，请刷新后重试"
                if kind == "compensation"
                else f"{meta['title']}清空失败：{result.status}"
            )
            return api_error(message)
        return api_success(message=f"{meta['title']}已清空", records=[])
    except Exception as e:
        return api_error(str(e))
