"""Nonempty skill assembly and unchanged equipment controls on the sole fixture."""
from __future__ import annotations

from collections import Counter
import ast
import hashlib
import json

from tests import mixed_player_projection_fixture as player


shared = player.strata.shared
PARTICIPANTS = player.PARTICIPANTS
MAX_TABLE_ROWS = 32_768
SLOTS = (("main_buff", "功法", "主功法"), ("sub_buff", "辅修功法", "辅修功法"),
         ("sec_buff", "神通", "神通"), ("effect1_buff", "身法", "身法"),
         ("effect2_buff", "瞳术", "瞳术"))
COMMANDS = (("skill_info", "我的功法", "optimized_target"),
            ("equipment_info", "我的装备", "unchanged_control"))
MAX_REPLY_BYTES = 131_072
MAX_FORWARD_NODES = 32


def phase_specs():
    return [{**row, "order": "shared-query-v19"} for row in player.phase_specs()]


def _encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"),
                      default=lambda value: {"blob": value.hex()}).encode("ascii")


def _snapshot(paths):
    result = {}
    with shared._uow(paths.game_db, read_only=True) as uow:
        for table in ("back", "BuffInfo"):
            cursor = uow.connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid LIMIT ?',
                                             (MAX_TABLE_ROWS + 1,))
            digest, count = hashlib.sha256(), 0
            digest.update(_encoded([column[0] for column in cursor.description]) + b"\n")
            try:
                while rows := cursor.fetchmany(128):
                    for row in rows:
                        count += 1
                        if count > MAX_TABLE_ROWS:
                            raise ValueError("shared-query table snapshot exceeds its row bound")
                        digest.update(_encoded(dict(row)) + b"\n")
            finally:
                cursor.close()
            result[table] = {"rows": count, "sha256": digest.hexdigest()}
    return result


def _catalog():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.back_util import EQUIPMENT_RANK_ORDER

    catalog, selected = Items(), {}
    for category, amount in [(category, 1) for _, category, _ in SLOTS] + [("法器", 4), ("防具", 4)]:
        ids = sorted(catalog.get_data_by_item_type((category,)), key=int)[:amount]
        if len(ids) != amount:
            raise ValueError(f"shared-query catalog lacks {amount} entries for {category}")
        rows = [{"id": int(item), "definition": dict(catalog.get_data_by_item_id(int(item)))}
                for item in ids]
        if any(not row["definition"].get("name") or row["definition"].get("item_type") != category
               for row in rows):
            raise ValueError("shared-query item catalog has an invalid category or name")
        if category in {"法器", "防具"} and any(not any(
                rank in row["definition"].get("level", "") for rank in EQUIPMENT_RANK_ORDER) for row in rows):
            raise ValueError("shared-query equipment is outside the displayed rank categories")
        selected[category] = rows
    ids = [row["id"] for rows in selected.values() for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("shared-query catalog IDs overlap")
    return selected


def _equipment_blocks(catalog, amount, bound):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_info_messages import (
        get_weapon_info_msg, get_armor_info_msg,
    )

    blocks = []
    for category, formatter in (("法器", get_weapon_info_msg), ("防具", get_armor_info_msg)):
        for index, item in enumerate(catalog[category]):
            block = formatter(item["id"], item["definition"])
            block += f"\n拥有数量: {amount} (绑定: {bound})"
            if index == 0:
                block += "\n※已装备※"
            blocks.append(block)
    return blocks


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    manifest = player.seed(context, managers, participants=participants)
    actors, catalog = manifest["actors"], _catalog()
    oracle = {}
    with shared._uow(context.paths.game_db) as uow:
        for index, actor in enumerate(actors):
            amount, bound = 20 + index % 3, index % 2
            uow.execute("DELETE FROM back WHERE user_id=? AND goods_type IN ('装备','技能')", (actor,))
            slot_ids = [catalog[category][0]["id"] for _, category, _ in SLOTS]
            equipment = [item for category in ("法器", "防具") for item in catalog[category]]
            equipped = {catalog[category][0]["id"] for category in ("法器", "防具")}
            for item in equipment + [catalog[category][0] for _, category, _ in SLOTS]:
                definition, item_id = item["definition"], item["id"]
                uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num,state,"
                            "create_time,update_time,all_num) VALUES(?,?,?,?,?,?,?,?,?,?)",
                            (actor, item_id, definition["name"], definition["type"], amount, bound,
                             int(item_id in equipped), shared.ANCHOR, shared.ANCHOR, amount))
            cursor = uow.execute("UPDATE BuffInfo SET main_buff=?,sub_buff=?,sec_buff=?,effect1_buff=?,"
                                 "effect2_buff=?,faqi_buff=?,armor_buff=? WHERE user_id=?",
                                 (*slot_ids, catalog["法器"][0]["id"], catalog["防具"][0]["id"], actor))
            if cursor.rowcount != 1:
                raise ValueError("shared-query requires one existing BuffInfo row per actor")
            oracle[actor] = {
                "skill_lines": [f'{label}：{catalog[category][0]["definition"]["name"]}'
                                for _, category, label in SLOTS],
                "equipment_blocks": _equipment_blocks(catalog, amount, bound),
                "equipment_names": [item["definition"]["name"] for item in equipment],
                "quantity": amount, "bound": bound, "equipped_count": 2,
            }
    manifest["inventory"] = shared._inventory(context.paths.game_db, actors)
    snapshot = _snapshot(context.paths)
    manifest.update(fixture_version="mixed-shared-query-v19", phase_specs=phase_specs(),
                    shared_query={"oracle": oracle, "snapshot": snapshot,
                                  "catalog_sha256": hashlib.sha256(_encoded(catalog)).hexdigest(),
                                  "roles": {key: role for key, _, role in COMMANDS}})
    manifest["feature_table_rows"].update({table: value["rows"] for table, value in snapshot.items()})
    manifest["limits"] += [
        "384 requests; two new real matcher paths replace relation slots; 56 exact profiles per phase retained",
        "each of 128 actors has eight catalog equipment rows, two equipped items and five nonempty skill slots",
        "my skills is the optimized target; equipment is an unchanged control already passing backpack rows",
        "inventory and BuffInfo full-table hashes are streamed within 32768 rows/table; no traffic-time oracle SQL",
        "32 real distinct-user exp/stone writes per mixed phase retain exact rank, receipt and generation checks",
        "automatic listings, material/refining and actual battle paths are not exercised by this load",
        "new per-command actor is unique within each phase; existing equipment cooldown is neither disabled nor bypassed",
        "finite short load is not all674 coverage or sustained50 successful requests/s",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = player.strata.gameplay._digest(manifest)
    process_read_cache.clear()
    for manager in managers:
        for name in ("_clear_read_cache", "_clear_field_list_cache"):
            clear = getattr(manager, name, None)
            if callable(clear):
                clear()
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    plan = player.request_plan(manifest, mode, count, phase_number)
    readers = manifest["groups"]["training_low"] + manifest["groups"]["exact_blob"]
    if len(readers) != 96 or len(set(readers)) != 96:
        raise ValueError("shared-query paths require 96 exact-value participants")
    targets = 0
    for index, row in enumerate(plan):
        if row["key"] not in {"my_partner", "my_mentor"}:
            continue
        key, command, role = COMMANDS[targets % len(COMMANDS)]
        actor = readers[(targets + phase_number * 7) % len(readers)]
        plan[index] = {"key": key, "command": command, "input_text": command, "actor": actor,
                       "number": row["number"], "event_type": "group", "phase_label": mode,
                       "performance_role": role,
                       "expected_shared_query": manifest["shared_query"]["oracle"][actor]}
        targets += 1
    if targets != (72 if mode == "pure" else 40):
        raise ValueError("shared-query target slot count changed")
    return plan


def _forward_contents(value):
    if isinstance(value, dict):
        if set(value) not in ({"group_id", "messages"}, {"user_id", "messages"}):
            raise ValueError("unknown forward envelope")
        recipient = value.get("group_id", value.get("user_id"))
        if type(recipient) not in (str, int):
            raise ValueError("invalid forward recipient")
        value = value["messages"]
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_FORWARD_NODES:
        raise ValueError("invalid forward nodes")
    contents = []
    for node in value:
        if (not isinstance(node, dict) or set(node) != {"type", "data"}
                or node["type"] != "node" or not isinstance(node["data"], dict)):
            raise ValueError("unknown forward node")
        data = node["data"]
        if (set(data) != {"name", "uin", "content"}
                or type(data["name"]) is not str or type(data["uin"]) not in (str, int)
                or type(data["content"]) is not str):
            raise ValueError("unknown forward content")
        contents.append(data["content"])
    return contents


def _reply_text(result):
    replies = result.get("replies", ())
    if not isinstance(replies, (list, tuple)) or len(replies) > MAX_FORWARD_NODES:
        raise ValueError("invalid replies")
    contents, total = [], 0
    for reply in replies:
        if type(reply) is not str:
            raise ValueError("invalid reply text")
        total += len(reply.encode("utf-8"))
        if total > MAX_REPLY_BYTES:
            raise ValueError("reply size exceeds bound")
        stripped = reply.lstrip()
        # OneBot CQ segments prefix ordinary text; they are not Python lists.
        if stripped.startswith(("{", "[")) and not stripped.startswith("[CQ:"):
            try:
                expression = ast.parse(reply, mode="eval")
                stack, nodes = [(expression, 0)], 0
                while stack:
                    node, depth = stack.pop()
                    nodes += 1
                    if depth > 16 or nodes > 1024:
                        raise ValueError("forward syntax exceeds bound")
                    stack.extend((child, depth + 1) for child in ast.iter_child_nodes(node))
                contents.extend(_forward_contents(ast.literal_eval(expression)))
            except (SyntaxError, TypeError, ValueError, RecursionError) as exc:
                raise ValueError("invalid bounded forward envelope") from exc
        else:
            contents.append(reply)
    return "\n".join(contents).replace("\r\n", "\n").replace("\r", "\n")


def classify_result(request, result):
    if request["key"] not in {row[0] for row in COMMANDS}:
        return player.classify_result(request, result)
    classified = shared.classify_result(request, result)
    if classified["outcome"] in {"boundary", "timeout", "exception", "cooldown", "business_rejection"}:
        return classified
    try:
        text = _reply_text(result)
    except ValueError:
        return {"outcome": "unclassified", "reason": "shared_query_invalid_transport_envelope"}
    expected = request["expected_shared_query"]
    if request["key"] == "skill_info":
        lines = ["**我的功法**", *expected["skill_lines"]]
        valid = all(text.count(line + "\n") == 1 for line in lines)
    else:
        blocks = expected["equipment_blocks"]
        valid = (len(blocks) == 8 and all(text.count(block) == 1 for block in blocks)
                 and text.count("※已装备※") == expected["equipped_count"]
                 and text.count("第 1/1 页") == 1)
    if not valid:
        return {"outcome": "unclassified", "reason": "shared_query_exact_reply_mismatch"}
    return {"outcome": "success", "reason": "exact_shared_query_reply"}


def check_invariants(context, managers, manifest, results):
    checked = player.check_invariants(context, managers, manifest, results)
    errors = list(checked["errors"])
    if _snapshot(context.paths) != manifest["shared_query"]["snapshot"]:
        errors.append("shared-query traffic changed complete inventory or BuffInfo tables")
    counts = Counter(row.get("request", row)["key"] for row in results)
    seen = set()
    for row in results:
        request = row.get("request", row)
        if request["key"] not in {item[0] for item in COMMANDS}:
            continue
        identity = request["key"], request["actor"]
        if identity in seen:
            errors.append("shared-query repeated a per-command actor within one phase")
        seen.add(identity)
        if row.get("outcome") != "success" or classify_result(request, row)["outcome"] != "success":
            errors.append(f'shared-query exact reply failed: {request["key"]}')
    if results:
        pure = results[0].get("request", results[0])["phase_label"] == "pure"
        expected = {key: 36 if pure else 20 for key, *_ in COMMANDS}
        expected.update(profile_info=56, stone_training=0 if pure else 32)
        if counts != +Counter(expected) or len(results) != 128:
            errors.append("shared-query frozen command counts changed")
    return {**checked, "ok": not errors, "errors": errors,
            "shared_query_checks": {"per_command": dict(counts), "snapshot": manifest["shared_query"]["snapshot"],
                                    "roles": manifest["shared_query"]["roles"],
                                    "oracle": "setup catalog names and equipment detail/count/state; zero traffic oracle SQL"},
            "invariants": checked["invariants"] + "; complete bounded inventory/BuffInfo hashes unchanged; exact nonempty skill/equipment replies"}
