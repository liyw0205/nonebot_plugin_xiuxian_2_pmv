"""One bounded high-cardinality inventory extension, real reads and writes.

The alchemy target stops at a genuine absent-furnace rejection. Its requests
are never counted as successful throughput or claimed as refinement load.
"""
from __future__ import annotations

from collections import Counter

from tests import mixed_command_fixture as shared
from tests import mixed_rank_gameplay_fixture as base
from tests import mixed_shared_query_fixture as transport
from tests import mixed_sect_shared_fixture as bounded


PARTICIPANTS = 128
UNRELATED_ITEMS = 192
MISSING_FURNACE = "虚构不存在丹炉"
COMMANDS = (
    ("pill_backpack", "丹药背包"),
    ("herb_backpack", "药材背包"),
    ("herb_detail", "药材背包详细"),
    ("equipment", "我的装备"),
)


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": 128,
             "concurrency": cap, "arrival_rate": rate, "order": "inventory-projection-v26"}
            for mode, cap, rate in (("pure", 10, 20), ("mixed", 20, 20),
                                    ("contention", 20, 35))]


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_info_messages import (
        get_weapon_info_msg, get_armor_info_msg,
    )
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.back_util import get_yaocai_info_msg

    manifest = base.seed(context, managers, participants=participants)
    actors, catalog = manifest["actors"], Items()
    initial = shared._rows(context.paths.game_db,
                           "SELECT * FROM back WHERE user_id=? ORDER BY goods_id", (actors[0],))
    existing = {int(row["goods_id"]) for row in initial}
    ids = [int(key) for key in sorted(catalog.get_data_by_item_type(
        ("功法", "辅修功法", "神通", "身法", "瞳术")), key=int) if int(key) not in existing][:UNRELATED_ITEMS]
    furnace_id = min(map(int, catalog.get_data_by_item_type(("炼丹炉",))))
    furnace = catalog.get_data_by_item_id(furnace_id)
    if len(ids) != UNRELATED_ITEMS or furnace_id in existing or furnace["name"] == MISSING_FURNACE:
        raise ValueError("V26 inventory catalog differs from the frozen coverage")
    extra = [(item, catalog.get_data_by_item_id(item)) for item in ids] + [(furnace_id, furnace)]
    if any(not row["name"] or row["type"] not in {"技能", "炼丹炉"} for _, row in extra):
        raise ValueError("V26 unrelated inventory must contain real catalog skills and furnace")
    with shared._uow(context.paths.game_db) as uow:
        uow.executemany("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num,state,"
                        "create_time,update_time,all_num) VALUES(?,?,?,?,20,0,0,?,?,20)",
                        ((actor, item, definition["name"], definition["type"], shared.ANCHOR, shared.ANCHOR)
                         for actor in actors for item, definition in extra))
    herbs = [row for row in initial if row["goods_type"] == "药材"]
    pills = [row for row in initial if row["goods_type"] == "丹药"]
    equipment = [row for row in initial if row["goods_type"] == "装备"]
    if (len(herbs), len(pills), len(equipment)) != (4, 4, 8):
        raise ValueError("V26 requires the existing4herbs/4pills/8equipment seed")
    recipe = (f"配方 主药{herbs[0]['goods_name']}1药引{herbs[1]['goods_name']}1"
              f"辅药{herbs[2]['goods_name']}1丹炉{MISSING_FURNACE}")
    oracles = {}
    for key, rows in (("pill_backpack", pills), ("herb_backpack", herbs), ("herb_detail", herbs)):
        blocks = []
        for row in rows:
            definition = catalog.get_data_by_item_id(row["goods_id"])
            block = f"名字：{definition['name']}\n"
            if key == "pill_backpack" and definition.get("level"):
                block += f"品级：{definition['level']}\n"
            if key == "herb_detail":
                block = get_yaocai_info_msg(row["goods_id"], definition)
                block += "\n拥有数量：20，绑定数量：0"
            else:
                block += f"ID：{row['goods_id']}\n拥有数量：20，绑定数量：0"
            blocks.append(block)
        oracles[key] = blocks
    oracles["equipment"] = []
    for row in equipment:
        definition = catalog.get_data_by_item_id(row["goods_id"])
        formatter = get_weapon_info_msg if definition["item_type"] == "法器" else get_armor_info_msg
        text = formatter(row["goods_id"], definition) + "\n拥有数量: 20 (绑定: 0)"
        if row["state"]:
            text += "\n※已装备※"
        oracles["equipment"].append(text)
    manifest["inventory"] = shared._inventory(context.paths.game_db, actors)
    snapshot = transport._snapshot(context.paths)
    manifest.update(fixture_version="mixed-inventory-projection-v26", phase_specs=phase_specs(),
                    initial_generation=bounded._generation(context.paths.game_db),
                    inventory_projection={"extra_catalog_ids": [item for item, _ in extra],
                        "per_actor_rows": len(initial) + len(extra), "unrelated_skills": len(ids),
                        "oracles": oracles, "recipe": recipe, "snapshot": snapshot,
                        "missing_furnace_reply": f"请检查炼丹炉：{MISSING_FURNACE} 是否在背包中！"})
    manifest["feature_table_rows"].update({table: value["rows"] for table, value in snapshot.items()})
    manifest["limits"] += [
        "V26 has213 positive rows per128actors:192 unrelated catalog skills plus original20items and1real furnace",
        "four successful typed display paths and one genuine recipe absent-furnace rejection; generation is covered by regressions only",
        "recipe refusals are separately counted, not successful refinement or successful throughput",
        "mixed/contention retain32 real exp+stone training writes each; all requests share one instance/DB",
        "contention reuses one writer; display actor/command identities remain unique within each phase; cooldowns stay enabled",
        "one213item cardinality, no large herb set/recipe combinatorics; not all674/41coverage or sustained50successful/s",
    ]
    manifest.pop("seed_sha256", None)
    manifest["seed_sha256"] = base._digest(manifest)
    return manifest


def request_plan(manifest, mode, count, phase_number=0):
    if mode not in {"pure", "mixed", "contention"} or count != 128 or phase_number not in range(3):
        raise ValueError("invalid V26 frozen plan")
    actors, plan, reads, writes = manifest["actors"], [], Counter(), 0
    for index in range(count):
        number = 890_000_000 + phase_number * 100_000 + index
        if mode != "pure" and index % 4 == 3:
            actor = actors[0 if mode == "contention" else writes]
            row = {"key": "stone_training", "command": "灵石修炼",
                   "input_text": f"灵石修炼 {base.REQUESTED_STONE}",
                   "requested_stone": base.REQUESTED_STONE, "exp_gain": base.EXP_GAIN,
                   "performance_role": "unchanged_write_control"}
            writes += 1
        elif index % 4 == 2:
            key = "recipe_preflight"
            actor = actors[(reads[key] + phase_number * 32) % len(actors)]
            row = {"key": key, "command": "配方", "input_text": manifest["inventory_projection"]["recipe"],
                   "expected_reply": manifest["inventory_projection"]["missing_furnace_reply"],
                   "performance_role": "optimized_rejection_path"}
            reads[key] += 1
        else:
            display = sum(reads[key] for key, _ in COMMANDS)
            key, command = COMMANDS[display % len(COMMANDS)]
            actor = actors[(reads[key] + phase_number * 32) % len(actors)]
            row = {"key": key, "command": command, "input_text": command,
                   "expected_blocks": manifest["inventory_projection"]["oracles"][key],
                   "performance_role": "optimized_display"}
            reads[key] += 1
        plan.append({**row, "actor": actor, "number": number, "event_type": "group", "phase_label": mode})
    return plan


def classify_result(request, result):
    if request["key"] == "stone_training":
        return base.classify_result(request, result)
    general = shared.classify_result(request, result)
    if general["outcome"] in {"boundary", "timeout", "exception", "cooldown"}:
        return general
    try:
        text = transport._reply_text(result)
    except ValueError:
        return {"outcome": "unclassified", "reason": "v26_invalid_envelope"}
    if request["key"] == "recipe_preflight":
        if text.count(request["expected_reply"]) == 1:
            return {"outcome": "business_rejection", "reason": "furnace_absent"}
    elif _display_matches(request, text) and text.count("第 1/1 页") == 1:
        return {"outcome": "success", "reason": "v26_inventory_exact_display"}
    return {"outcome": "unclassified", "reason": "v26_reply_mismatch"}


def _display_matches(request, text):
    blocks = request["expected_blocks"]
    if all(text.count(block) == 1 for block in blocks):
        return True
    if request["key"] not in {"pill_backpack", "herb_backpack"}:
        return False
    if not blocks or any(not block.startswith("名字：") for block in blocks):
        return False
    names = [block.splitlines()[0].removeprefix("名字：") for block in blocks]
    lines = text.splitlines()
    expected = sorted(f"- {name} 数量:20 绑定:0" for name in names)
    actual = sorted(line for line in lines if line.startswith("- "))
    uses = sorted(line for line in lines if line.startswith("  使用："))
    expected_uses = sorted(f"  使用：使用 {name}" for name in names) if request["key"] == "pill_backpack" else []
    return actual == expected and uses == expected_uses


def check_invariants(context, managers, manifest, results):
    checked = base.check_invariants(context, managers, manifest, results)
    errors = list(checked["errors"])
    if transport._snapshot(context.paths) != manifest["inventory_projection"]["snapshot"]:
        errors.append("V26 complete inventory/BuffInfo snapshot changed")
    generation = bounded._generation(context.paths.game_db)
    if generation != {key: value + checked["fresh_training"]
                      for key, value in manifest["initial_generation"].items()}:
        errors.append("V26 exact exp/stone generation deltas differ from successful writes")
    seen = set()
    for row in results:
        request = row["request"]
        if classify_result(request, row) != {"outcome": row["outcome"], "reason": row["reason"]}:
            errors.append("V26 outcome disagrees with stored replies")
        expected = "business_rejection" if request["key"] == "recipe_preflight" else "success"
        if row["outcome"] != expected:
            errors.append(f"V26 unexpected outcome for {request['number']}: {row['outcome']}")
        if request["key"] in dict(COMMANDS):
            identity = request["key"], request["actor"]
            if identity in seen:
                errors.append("V26 duplicate display cooldown identity")
            seen.add(identity)
    counts = Counter(row["key"] for row in results)
    if results:
        pure = results[0]["request"]["phase_label"] == "pure"
        expected = Counter({key: 24 if pure else 16 for key, _ in COMMANDS})
        expected.update(recipe_preflight=32, stone_training=0 if pure else 32)
        if counts != +expected or len(results) != 128:
            errors.append("V26 command counts differ from frozen plan")
    return {**checked, "ok": not errors, "errors": errors,
            "inventory_projection_checks": {"per_command": dict(counts),
                "generation": generation,
                "snapshot": manifest["inventory_projection"]["snapshot"],
                "per_actor_rows": manifest["inventory_projection"]["per_actor_rows"]},
            "invariants": checked["invariants"] + "; bounded full inventory/BuffInfo hashes and exact selected display replies; missing-furnace refusal"}
