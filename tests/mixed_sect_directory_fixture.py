"""Compose the V21 population with populated catalogs and sect-directory fanout.

Only setup/checks add data work. Traffic reuses the existing matcher dispatcher,
practice receipts and full selected-row invariants on the sole retained 1M DB.
"""
from __future__ import annotations

import ast
from collections import Counter
from copy import deepcopy
import re

from tests import mixed_sect_shared_fixture as base


PARTICIPANTS = base.PARTICIPANTS
PRACTICES = base.PRACTICES
COMMANDS = (
    ("sect_directory", "宗门列表"),
    ("sect_main_view", "宗门功法查看"),
    ("sect_sec_view", "宗门神通查看"),
    *tuple(row for row in base.COMMANDS if row[0] in PRACTICES or row[0] == "asset_overview"),
)
VIEWS = {
    "sect_directory": ("宗门列表", None),
    "sect_main_view": ("宗门功法", "【宗门功法】"),
    "sect_sec_view": ("宗门速通", "【宗门神通】"),
}


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": 128,
             "concurrency": cap, "arrival_rate": rate, "order": "sect-directory-v22"}
            for mode, cap, rate in (("pure", 10, 10), ("mixed", 20, 20),
                                    ("contention", 20, 35))]


def _canonical(text):
    return text.replace("\r\n", "\n").replace("\r", "\n").strip()


def _reply_digest(nodes):
    return base._digest(_canonical("\n".join(nodes)))


def _catalog_selection(module):
    """Cross-check real item/buff definitions before traffic, never mock them."""
    buff_data = module.BuffJsonDate()
    selected = {}
    for kind, getter, config_key in (
        ("功法", buff_data.get_main_buff, "宗门主功法参数"),
        ("神通", buff_data.get_sec_buff, "宗门神通参数"),
    ):
        config = module.config[config_key]
        if int(config["建设度"]) <= 0 or int(config["学习资材消耗"]) <= 0:
            raise ValueError("sect catalog requires valid live catalog config")
        candidates = []
        for item_id in module.items.get_data_by_item_type((kind,)):
            item = module.items.get_data_by_item_id(int(item_id))
            if item and item.get("level") in module.buffrankkey and item.get("name"):
                candidates.append((module.buffrankkey[item["level"]], int(item_id), item))
        ordered = sorted(candidates, key=lambda row: (row[0], row[1]))
        chosen = ordered[:1]
        if chosen:
            chosen.extend(row for row in ordered if row[0] != chosen[0][0])
        chosen = chosen[:2]
        if len(chosen) != 2 or chosen[0][1] == chosen[1][1]:
            raise ValueError("sect catalog requires two distinct real ranks per kind")
        for _, item_id, item in chosen:
            raw = getter(item_id)
            # Skill loading swaps the legacy rank/level fields.
            if (raw.get("name") != item.get("name") or raw.get("rank") != item.get("level")
                    or raw.get("level") != item.get("rank")):
                raise ValueError("sect item and BuffJsonDate catalog definitions disagree")
        selected[kind] = [{"id": item_id, "level": item["level"], "name": item["name"],
                           "rank": rank} for rank, item_id, item in chosen]
    return selected


def _catalog_nodes(catalog):
    return {
        "sect_main_view": [row["level"] + row["name"] for row in catalog["功法"]],
        "sect_sec_view": [row["level"] + ":" + row["name"] for row in catalog["神通"]],
    }


def _catalog_storage(catalog):
    # The existing catalog parser splits commas without stripping whitespace.
    return "[" + ",".join(str(row["id"]) for row in reversed(catalog)) + "]"


def _directory_nodes(sects, owner_names, member_counts):
    """Independent bounded setup oracle from raw rows, not the new runtime SQL."""
    from nonebot_plugin_xiuxian_2.core.numeric import as_int_like

    nodes = []
    for sect in sorted(sects.values(), key=lambda row: int(row["sect_id"])):
        sect_id = int(sect["sect_id"])
        count = member_counts.get(sect_id, 0)
        maximum = min(20 + as_int_like(base._raw_value(sect["sect_scale"])) // 50_000_000, 100)
        if sect["closed"]:
            status = "宗门已封闭"
        elif not sect["join_open"]:
            status = "宗门关闭加入"
        elif count >= maximum:
            status = f"人数已满 ({count}/{maximum})"
        else:
            status = f"可加入 ({count}/{maximum})"
        owner = owner_names.get(str(sect["sect_owner"]))
        owner = "暂无" if owner is None else owner
        nodes.append(f"编号{sect_id}：{sect['sect_name']}\n宗主：{owner}\n宗门状态：{status}\n"
                     f"建设度：{base._number(as_int_like(base._raw_value(sect['sect_scale'])))}\n")
    return nodes


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_sect

    manifest = base.seed(context, managers, participants=participants)
    database, catalog = context.paths.game_db, _catalog_selection(xiuxian_sect)
    # Reverse stored order so both views exercise their real rank ordering.
    main = _catalog_storage(catalog["功法"])
    secondary = _catalog_storage(catalog["神通"])
    with base.shared._uow(database) as uow:
        for index, sect_id in enumerate(manifest["sect_ids"]):
            closed, join_open = (0, 1) if index < 4 else (int(index % 3 == 1), int(index % 3 != 2))
            changed = uow.execute("UPDATE sects SET mainbuff=?,secbuff=?,closed=?,join_open=? WHERE sect_id=?",
                                  (main, secondary, closed, join_open, sect_id))
            if changed.rowcount != 1:
                raise ValueError("sect directory catalog seed identity is not unique")
    sects = base._sects(database)
    if int(base.shared._rows(database, "SELECT COUNT(*) AS n FROM sects")[0]["n"]) != len(sects):
        raise ValueError("sect directory scenario requires unique bounded sect identities")
    sect_ids = [int(row["sect_id"]) for row in sects.values()]
    owners = [str(row["sect_owner"]) for row in sects.values() if row["sect_owner"] is not None]
    names = {}
    if owners:
        for row in base.shared._rows(database, "SELECT user_id,user_name FROM user_xiuxian "
                                     f"WHERE user_id IN ({base.shared._marks(owners)}) ORDER BY rowid", owners):
            names.setdefault(str(row["user_id"]), row["user_name"])
    counts = {int(row["sect_id"]): int(row["n"]) for row in base.shared._rows(
        database, "SELECT sect_id,COUNT(*) AS n FROM user_xiuxian "
        f"WHERE sect_id IN ({base.shared._marks(sect_ids)}) GROUP BY sect_id", sect_ids)}
    nodes = {"sect_directory": _directory_nodes(sects, names, counts), **_catalog_nodes(catalog)}
    manifest.update(fixture_version="mixed-sect-directory-v22", phase_specs=phase_specs(),
                    initial_sects=sects, frozen=base._frozen_tables(context.paths),
                    catalog=catalog, display_nodes=nodes,
                    display_oracles={key: {"sha256": _reply_digest(value), "entries": len(value)}
                                     for key, value in nodes.items()})
    manifest["limits"] = [line for line in manifest["limits"]
                           if not line.startswith("pure128 reads;")]
    manifest["limits"].extend([
        "V22 adds populated128-sect directory fanout plus two real main/two real secondary catalog entries per seeded sect",
        "pure:16 directory+56 main+56 secondary; mixed/contention:16 directory+32 main+32 secondary+32 practice+16 assets",
        "catalog search/learning, exp writes and their runtime SQL are not sampled; direct regressions cover changed semantic boundaries",
        "catalog setup uses real Items/BuffJsonDate/config; directory oracle freezes every bounded existing/seeded sect entry",
        "background sects alternate closed/join flags; four actor sects stay open; counts and catalog IDs do not mutate during traffic",
        "previous V21 results remain independent; this new load covers directory fanout and nonempty catalog reads only",
    ])
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = base._digest(manifest)
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs) or mode != specs[phase_number]["mode"]
            or count != specs[phase_number]["requests"]):
        raise ValueError("invalid frozen sect directory plan")
    plan, commands = base.request_plan(manifest, mode, count, phase_number), dict(COMMANDS)
    for index, row in enumerate(plan):
        old = row["key"]
        if mode == "pure":
            key = ("sect_directory" if index % 8 == 0 else
                   "sect_main_view" if (index - index // 8 - 1) % 2 == 0 else "sect_sec_view")
        else:
            key = {"my_sect": "sect_main_view", "sect_buildings": "sect_sec_view"}.get(old, old)
            if old == "asset_overview" and index // 4 % 2 == 0:
                key = "sect_directory"
        if key in VIEWS:
            row.update(key=key, command=commands[key], input_text=commands[key],
                       performance_role="optimized_target",
                       expected_reply=deepcopy(manifest["display_oracles"][key]))
            row.pop("expected_sect")
        else:
            row["performance_role"] = "unchanged_control"
    if Counter(row["key"] for row in plan) != Counter(
            {"sect_directory": 16, "sect_main_view": 56, "sect_sec_view": 56} if mode == "pure" else
            {"sect_directory": 16, "sect_main_view": 32, "sect_sec_view": 32,
             "asset_overview": 16, "sect_attack_upgrade": 11, "sect_health_upgrade": 11,
             "sect_mana_upgrade": 10}):
        raise ValueError("sect directory request distribution differs from the frozen plan")
    return plan


def _transport_text(request, result):
    replies = result.get("replies", ())
    if len(replies) != 1 or not isinstance(replies[0], str) or len(replies[0]) > 1_000_000:
        return None
    raw, (label, title) = replies[0], VIEWS[request["key"]]
    if raw.startswith("{"):
        try:
            payload = ast.literal_eval(raw)
        except (ValueError, SyntaxError, RecursionError):
            return None
        if (not isinstance(payload, dict) or set(payload) not in
                ({"group_id", "messages"}, {"user_id", "messages"})
                or not isinstance(payload["messages"], list) or not payload["messages"]):
            return None
        contents, senders = [], set()
        for node in payload["messages"]:
            if not isinstance(node, dict) or set(node) != {"type", "data"} or node["type"] != "node":
                return None
            data = node["data"]
            if (not isinstance(data, dict) or set(data) != {"name", "uin", "content"}
                    or data["name"] != label or not isinstance(data["content"], str)
                    or not str(data["uin"]).isdigit()):
                return None
            contents.append(data["content"])
            senders.add(str(data["uin"]))
        if len(senders) != 1 or len(contents) not in (1, request["expected_reply"]["entries"]):
            return None
        return _canonical("\n".join(contents))
    raw = re.sub(r"^\[CQ:at,qq=[0-9]+\][ \t]*", "", raw)
    raw = _canonical(raw)
    if title and raw.startswith(title):
        raw = raw[len(title):]
    return _canonical(raw)


def classify_result(request, result):
    if request["key"] not in VIEWS:
        return base.classify_result(request, result)
    classified = base.shared.classify_result(request, result)
    if classified["outcome"] in {"boundary", "timeout", "exception", "cooldown"}:
        return classified
    text = _transport_text(request, result)
    valid = text is not None and base._digest(text) == request["expected_reply"]["sha256"]
    return ({"outcome": "success", "reason": "exact_sect_directory_catalog_reply"} if valid else
            {"outcome": "unclassified", "reason": "sect_directory_catalog_reply_mismatch"})


def check_invariants(context, managers, manifest, results):
    classified = []
    for row in results:
        classified.append(row if "outcome" in row else
                          {**row, "outcome": classify_result(row.get("request", row), row)})
    return base.check_invariants(context, managers, manifest, classified)
