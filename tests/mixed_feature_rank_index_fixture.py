"""Bounded season/BOSS live ranks with real score-changing gameplay.

The outer driver owns the sole fixture, matcher dispatch, resource guards and
phase-boundary restoration. Setup oracles never execute during traffic.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import datetime
import json
import re

from tests import mixed_rank_projection_fixture as ranks
from tests import mixed_sect_shared_fixture as sect


shared = sect.shared
PARTICIPANTS = sect.PARTICIPANTS
MESSAGE_BASE = 932_000_000
DONATION = 1000
ITEM_ID = 1999
MAX_WRITES = 4
COMMANDS = (
    ("my_season_rank", "我的赛季"),
    ("boss_integral_rank", "世界BOSS积分排行榜"),
    ("season_power_rank", "赛季榜"),
    ("sect_donate", "宗门捐献"),
    ("boss_purchase", "世界BOSS兑换"),
)
READ_KEYS = frozenset(key for key, _ in COMMANDS[:3])
WRITE_KEYS = frozenset(key for key, _ in COMMANDS[3:])
MUTABLE = {
    "game_db": {"user_xiuxian", "sects", "season_rank", "back", "sect_weekly_goal",
                "sect_donation_operations", "boss_purchase_operations", "operation_ledger",
                "operation_audit", "economy_log", "xiuxian_numeric_rank_generation"},
    "player_db": {"boss_limit", "boss", "boss_weekly_purchases"},
}
CHECKED_TABLES = {
    "game_db": ("season_rank", "back", "sect_weekly_goal", "sect_donation_operations",
                "boss_purchase_operations", "operation_ledger", "operation_audit", "economy_log"),
    "player_db": ("boss_limit", "boss", "boss_weekly_purchases"),
}


def phase_specs():
    return [{"label": mode, "mode": mode, "requests": 96, "concurrency": cap,
             "arrival_rate": arrival, "order": "feature-rank-index-v32"}
            for mode, cap, arrival in (("pure", 10, 20), ("mixed", 20, 35),
                                       ("contention", 20, 50))]


def _rows(database, table):
    with shared._uow(database, read_only=True) as uow:
        columns = uow.query_all(f"PRAGMA table_info({sect._quote(table)})")
        if not columns:
            raise ValueError(f"V32 required table is absent: {table}")
        keys = [row["name"] for row in sorted(columns, key=lambda row: int(row["pk"] or 0))
                if int(row["pk"] or 0)]
        ordering = ",".join(map(sect._quote, keys)) if keys else "rowid"
        result = uow.query_all(f"SELECT * FROM {sect._quote(table)} ORDER BY {ordering} LIMIT ?",
                               (sect.MAX_TABLE_ROWS + 1,))
    if len(result) > sect.MAX_TABLE_ROWS:
        raise ValueError(f"V32 explicit table exceeds bounded full-row oracle: {table}")
    return [sect._json_row(row) for row in result]


def _tables(paths):
    return {name + ":" + table: _rows(getattr(paths, name), table)
            for name, tables in CHECKED_TABLES.items() for table in tables}


def _frozen(paths):
    snapshots, exclusions = {}, {}
    for name in ("game_db", "player_db", "impart_db", "trade_db"):
        with shared._uow(getattr(paths, name), read_only=True) as uow:
            for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table' "
                                     "AND name NOT LIKE 'sqlite_%' ORDER BY name"):
                table, key = row["name"], name + ":" + row["name"]
                if table in MUTABLE.get(name, ()):
                    continue
                signature = sect._table_signature(uow, table)
                (exclusions if "excluded" in signature else snapshots)[key] = signature
    return {"tables": snapshots, "exclusions": exclusions}


def _rules():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_boss, xiuxian_sect
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_sect.sect_weekly import SECT_WEEKLY_EVENT_MAP
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.season_rank_service import EVENT_SEASON_RANK_TYPES

    raw = xiuxian_boss.config["世界积分商品"].get(str(ITEM_ID))
    item = xiuxian_boss._items().get_data_by_item_id(ITEM_ID)
    rate = xiuxian_sect.config.get("宗门捐献资材倍率", 1)
    goals = SECT_WEEKLY_EVENT_MAP.get("sect_donate", ())
    maximum = int(xiuxian_boss.boss_purchase_command_application.max_goods_num_provider())
    if (not raw or int(raw["cost"]) != 1000 or int(raw.get("weekly_limit", 1)) != 10
            or not item or item.get("name") != "渡厄丹" or item.get("type") != "丹药"
            or int(DONATION * rate) < 0 or maximum < 14
            or EVENT_SEASON_RANK_TYPES.get("sect_donate") != ("宗门贡献",)
            or len(goals) != 1 or goals[0].key != "sect_supply"
            or goals[0].target <= 24 * DONATION):
        raise ValueError("V32 requires the actual unchanged donation/catalog/weekly rules")
    return {"cost": int(raw["cost"]), "weekly_limit": int(raw["weekly_limit"]),
            "item_name": item["name"], "item_type": item["type"], "max_goods_num": maximum,
            "materials_rate": rate, "materials": int(DONATION * rate),
            "weekly_goal": goals[0].key, "weekly_target": goals[0].target,
            "purchase_day": xiuxian_boss.boss_purchase_command_application.clock.now().date().isoformat()}


def _set_feature_rows(paths, actors, owners, now, rules):
    from nonebot_plugin_xiuxian_2.features.title.migrations import apply_title_schema
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title.title_data import load_title_data

    population = ranks._seed_rank_tables(paths, actors, owners, now)
    with shared._uow(paths.game_db) as uow:
        for key_index, (key, _, _, _) in enumerate(population["identities"]):
            for index, owner in enumerate(population["season_owners"][8:], 8):
                uow.execute("UPDATE season_rank SET score=? WHERE rank_key=? AND user_id=? AND sect_id=0",
                            (10_000_000 + (ranks.SEASON_POPULATION - index) * 10_000
                             + key_index * 100, key, owner))
        for actor in actors:
            uow.execute("INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,"
                        "create_time,update_time,bind_num) VALUES(?,?,?,?,10,?,?,3) "
                        "ON CONFLICT(user_id,goods_id) DO UPDATE SET goods_name=excluded.goods_name,"
                        "goods_type=excluded.goods_type,goods_num=10,bind_num=3,"
                        "create_time=excluded.create_time,update_time=excluded.update_time",
                        (actor, ITEM_ID, rules["item_name"], rules["item_type"],
                         rules["purchase_day"], rules["purchase_day"]))
    titles = sorted(str(key) for key in load_title_data())
    if not titles:
        raise ValueError("V32 requires the actual title catalog; title checks remain enabled")
    with shared._uow(paths.player_db) as uow:
        apply_title_schema(uow)
        columns = {str(row["name"]) for row in uow.query_all('PRAGMA table_info("boss")')}
        weekly_table = "boss" if "weekly_purchases" in columns else "boss_weekly_purchases"
        for index, owner in enumerate(population["boss_owners"][8:], 8):
            value = (30_000_000 + (PARTICIPANTS - index) * 10_000 if index < PARTICIPANTS
                     else 5_000_000_000 + (ranks.BOSS_POPULATION - index) * 10_000)
            uow.execute("UPDATE boss_limit SET integral=? WHERE user_id=?", (value, owner))
        for actor in actors:
            # Existing historical unlocks avoid unrelated new-title writes; the
            # real eligibility/check code still runs for every donation.
            uow.execute("INSERT INTO title(user_id,unlocked,equipped) VALUES(?,?,'') "
                        "ON CONFLICT(user_id) DO UPDATE SET unlocked=excluded.unlocked,equipped=''",
                        (actor, json.dumps(titles, ensure_ascii=False)))
            uow.execute(f"INSERT INTO {weekly_table}(user_id,weekly_purchases) VALUES(?,?) "
                        "ON CONFLICT(user_id) DO UPDATE SET weekly_purchases=excluded.weekly_purchases",
                        (actor, json.dumps({"_last_reset": rules["purchase_day"]})))
    return population, weekly_table, len(titles)


def _intervals(oracle, donors, buyers, rules):
    seasons = oracle["season"]
    for actor in donors:
        for row in seasons[actor]:
            if row["rank_type"] == "宗门贡献":
                rows = [entry for entries in seasons.values() for entry in entries
                        if entry["rank_key"] == row["rank_key"]]
                if any(row["value"] < other["value"] <= row["value"] + MAX_WRITES * DONATION
                       for other in rows):
                    raise ValueError("V32 donation can cross an actor rank interval")
    boss = oracle["boss"]
    top_ids = {row["user_id"] for row in boss["top"]}
    for actor in buyers:
        value = boss["actors"][actor]["value"]
        if actor in top_ids or value <= MAX_WRITES * rules["cost"]:
            raise ValueError("V32 BOSS writer overlaps TOP50 or lacks live points")
        if any(value - MAX_WRITES * rules["cost"] <= other["value"] < value
               for other in boss["actors"].values()):
            raise ValueError("V32 purchase can cross an actor rank interval")


def seed(context, managers, *, participants=PARTICIPANTS):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache

    manifest = sect.seed(context, managers, participants=participants)
    now, actors, rules = datetime.now().replace(microsecond=0), manifest["actors"], _rules()
    owners = [str(row["user_id"]) for row in shared._rows(context.paths.game_db,
        f"SELECT user_id FROM user_xiuxian WHERE id IN ({shared._marks(manifest['population_ids'])}) "
        "ORDER BY id", manifest["population_ids"])]
    if len(owners) != sect.POPULATION or len(set(owners)) != sect.POPULATION:
        raise ValueError("V32 requires all8192existing synthetic owners")
    population, weekly_table, title_count = _set_feature_rows(context.paths, actors, owners, now, rules)
    initial = _tables(context.paths)
    for table in ("sect_donation_operations", "boss_purchase_operations", "operation_ledger", "operation_audit"):
        if any(str(row["operation_id"]).startswith(("sect:932", "boss-purchase:932"))
               for row in initial["game_db:" + table]):
            raise ValueError("V32 receipt namespace is not empty")
    oracle = ranks._oracle(context.paths, actors, population["identities"])
    donors, buyers = actors[8:32], actors[32:56]
    _intervals(oracle, donors, buyers, rules)
    season = initial["game_db:season_rank"]
    for donor in donors:
        for row in oracle["season"][donor]:
            if row["rank_type"] == "宗门贡献" and any(
                    other["rank_key"] == row["rank_key"]
                    and row["value"] < int(sect._raw_value(other["score"])) <= row["value"] + 4000
                    for other in season):
                raise ValueError("V32 donation can cross a full-population rank interval")
    boss_rows = initial["player_db:boss_limit"]
    for buyer in buyers:
        value = oracle["boss"]["actors"][buyer]["value"]
        if any(value - 4 * rules["cost"] <= int(sect._raw_value(row["integral"])) < value
               for row in boss_rows):
            raise ValueError("V32 purchase can cross a full-population rank interval")
    identities = {mode: period for _, mode, period, _ in population["identities"]}
    monthly_key = next(key for key, mode, _, kind in population["identities"]
                       if mode == "monthly" and kind == "战力")
    names = {actor: row["user_name"] for actor, row in manifest["initial_users"]["actors"].items()}
    for row in shared._rows(context.paths.game_db,
            f"SELECT user_id,user_name FROM user_xiuxian WHERE user_id IN ({shared._marks(population['season_owners'])})",
            population["season_owners"]):
        names[str(row["user_id"])] = row["user_name"] or str(row["user_id"])
    top = sorted((row for row in season if row["rank_key"] == monthly_key),
                 key=lambda row: (-int(sect._raw_value(row["score"])), row["updated_at"]))[:10]
    public = {"periods": identities, "top": [{"name": names[str(row["user_id"])],
               "value": int(sect._raw_value(row["score"]))} for row in top]}
    manifest.update(fixture_version="mixed-feature-rank-index-v32", phase_specs=phase_specs(),
        initial_users=sect._selected_users(context.paths.game_db, manifest["population_ids"], actors),
        initial_sects=sect._sects(context.paths.game_db),
        initial_generation=sect._generation(context.paths.game_db),
        feature_rank_index={"rules": rules, "population": population, "oracle": oracle,
            "public": public, "donors": donors, "buyers": buyers, "weekly_table": weekly_table,
            "initial": initial, "frozen": _frozen(context.paths),
            "started_at": now.replace(hour=0, minute=0, second=0).isoformat(sep=" "),
            "historical_title_count": title_count})
    for obsolete in ("rules", "oracle", "frozen", "hot_groups", "wallet", "materials", "seed_sha256"):
        manifest.pop(obsolete, None)
    manifest["feature_table_rows"].update({key: len(value) for key, value in initial.items()})
    manifest["limits"] = [
        "288requests/version: three96request phases; three read matchers plus real donation/BOSS purchase; not all674matchers",
        "same sole1Musers/shared instance/shared DB;128actors,8192existing sectmembers,3840seasonrows,2048bossrows; no million feature-table claim",
        "mixed24distinctdonors+24distinctbuyers; contention6users/type x4requests; all rules/cooldowns/CAS/receipts remain enabled",
        "writes use ordinary exact integers separated by10000; first8readonly actors retain oversized BLOB scores; no actor/background rank crossing within4writes",
        "donations change real wallet/contribution/sectassets/weeklyprogress and three separate season rows; purchases change real points/bound inventory/weeklycounter",
        "season modes update in separate real commits; reply oracle permits only independently reachable per-row scores and exact constant ranks",
        "all real title checks remain enabled; seeded historical actual-catalog titles avoid unrelated new unlock writes; title/reward replay workload is not covered",
        "public BOSS TOP50 and monthly power TOP10 never change in this workload; existing public snapshot TTL untouched, onephase-startclear only",
        "complete bounded mutable tables and other bounded tables checked after drain; tables beyond32768rows explicitly excluded; no additional JSON snapshot",
        "full actor rows+8064background member hash checked; remaining991808users lack traffic full-column checks; outer driver restores/verifies original1Mrankdigest",
        "setup/check/restore IO excluded from measured traffic; no perrequest oracleSQL; bounded candidates do not establish point-in-time atomicity across15boards",
        "short finite load does not prove sustained50successes/s; arrivals/cap/actualoverlap/success throughput reported separately",
    ]
    manifest["seed_sha256"] = sect._digest(manifest)
    process_read_cache.clear()
    return manifest


def _operation(request):
    if request["key"] == "sect_donate":
        return f"sect:{request['number']}:donate:{request['actor']}"
    return f"boss-purchase:{request['number']}:{request['actor']}"


def request_plan(manifest, mode, count, phase_number=0):
    specs = phase_specs()
    if (not 0 <= phase_number < len(specs) or mode != specs[phase_number]["mode"]
            or count != 96 or len(manifest["actors"]) != PARTICIPANTS
            or len(set(manifest["actors"])) != PARTICIPANTS):
        raise ValueError("invalid V32 frozen phase shape")
    state, actors = manifest["feature_rank_index"], manifest["actors"]
    if (state["donors"] != actors[8:32] or state["buyers"] != actors[32:56]
            or len(set(state["donors"] + state["buyers"])) != 48):
        raise ValueError("invalid V32 disjoint bounded feature writers")
    plan, reads, write_counts = [], 0, Counter()
    keys = [key for key, _ in COMMANDS[:3]]
    commands = dict(COMMANDS)
    for index in range(count):
        if mode == "pure" or index % 4 < 2:
            key = keys[reads % 3]
            actor = actors[(reads * 5 + phase_number * 7) % 56]
            reads += 1
        else:
            key = "sect_donate" if index % 4 == 2 else "boss_purchase"
            group = state["donors"] if key == "sect_donate" else state["buyers"]
            actor = group[(index // 4) % (6 if mode == "contention" else 24)]
            write_counts[(key, actor)] += 1
        command = commands[key]
        text = command + ({"sect_donate": " 1000", "boss_purchase": " 1999 1",
                           "season_power_rank": " 月榜 战力"}.get(key, ""))
        plan.append({"key": key, "command": command, "input_text": text, "actor": actor,
            "number": MESSAGE_BASE + phase_number * 100_000 + index, "event_type": "group",
            "phase_label": mode, "performance_role": "optimized_target" if key in READ_KEYS else "live_write_control",
            "sect_id": int(manifest["initial_users"]["actors"][actor]["sect_id"]),
            "feature_rules": state["rules"]})
    for request in plan:
        actor, key = request["actor"], request["key"]
        if key == "my_season_rank":
            expected = deepcopy(state["oracle"]["season"][actor])
            for row in expected:
                n = write_counts[("sect_donate", actor)] if row["rank_type"] == "宗门贡献" else 0
                row["values"] = [row["value"] + step * DONATION for step in range(n + 1)]
            request["expected_rank_projection"] = expected
        elif key == "boss_integral_rank":
            expected = deepcopy(state["oracle"]["boss"]["actors"][actor])
            expected["top"] = state["oracle"]["boss"]["top"]
            expected["values"] = [expected["value"] - step * state["rules"]["cost"]
                                  for step in range(write_counts[("boss_purchase", actor)] + 1)]
            request["expected_rank_projection"] = expected
        elif key == "season_power_rank":
            request["expected_public"] = state["public"]
    return plan


def _text(request, result):
    text = "\n".join(map(str, result.get("replies", ()))).replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"\A\[CQ:at,qq=" + re.escape(str(request["actor"])) + r"\][ \t]*", "", text)


def classify_result(request, result):
    classified = shared.classify_result(request, result)
    if classified["outcome"] in {"boundary", "timeout", "exception", "cooldown"}:
        return classified
    text, key, rules = _text(request, result), request["key"], request["feature_rules"]
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if key == "sect_donate":
        expected = (f"道友捐献灵石{DONATION}枚，宗门建设度增加：{DONATION}，"
                    f"宗门资材增加：{rules['materials']}，宗门贡献度增加：{DONATION}点，蒸蒸日上！")
        valid = lines == [expected]
    elif key == "boss_purchase":
        valid = lines == [f"道友成功兑换获得：{rules['item_name']}1个"]
        if lines == ["兑换未完成：活动进度已更新，请重新兑换。"]:
            return {"outcome": "business_rejection", "reason": "purchase_state_changed"}
    elif key == "my_season_rank":
        expected, offset, seen = request["expected_rank_projection"], 1, set()
        valid = bool(lines) and lines[0] == "【我的赛季】"
        labels = {"weekly": "周榜", "monthly": "月榜", "quarterly": "季度榜"}
        for row in expected:
            if row["mode"] not in seen:
                heading = f"【{row['period']}{labels[row['mode']]}】"
                valid = valid and offset < len(lines) and lines[offset] == heading
                seen.add(row["mode"])
                offset += 1
            choices = {f"{row['rank_type']}：积分{sect._number(value)}，当前第{row['rank']}名"
                       for value in row["values"]}
            valid = valid and offset < len(lines) and lines[offset] in choices
            offset += 1
        valid = valid and offset == len(lines)
    elif key == "boss_integral_rank":
        expected = request["expected_rank_projection"]
        prefix = ["【世界BOSS积分排行榜】"] + [
            f"第{index}位 | {row['name']} | {sect._number(row['value'])}"
            for index, row in enumerate(expected["top"], 1)]
        choices = {f"你的名次：第{expected['rank']}名 / 共{expected['total']}人"
                   f"（世界积分 {sect._number(value)}点）" for value in expected["values"]}
        valid = (len(lines) == len(prefix) + 2 and lines[:len(prefix)] == prefix
                 and lines[-2] in choices and bool(re.fullmatch(
                     r"公共榜单快照截至 [0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} UTC"
                     r"（最长缓存 60 分钟）；个人名次与资格按实时数据计算。", lines[-1])))
    elif key == "season_power_rank":
        public = request["expected_public"]
        periods = public["periods"]
        expected = ["【赛季榜】", f"本期榜单：{periods['monthly']}月榜", f"周榜：{periods['weekly']}",
            f"月榜：{periods['monthly']}", f"季度榜：{periods['quarterly']}",
            f"【{periods['monthly']}月榜战力榜 前十】"]
        expected += [f"{index}. {row['name']} 积分{sect._number(row['value'])}"
                     for index, row in enumerate(public["top"], 1)]
        expected += ["可发送：赛季榜、赛季榜 周榜、赛季榜 月榜、赛季榜 季度榜、赛季榜 周榜 战力、我的赛季",
                     "可看榜单：交易活跃、讨伐、宗门贡献、试炼、战力。"]
        valid = lines == expected
    else:
        valid = False
    return ({"outcome": "success", "reason": "exact_feature_score_reply"} if valid else
            {"outcome": "unclassified", "reason": "feature_score_reply_mismatch"})


def _outcome(result):
    value = result.get("outcome")
    return value.get("outcome") if isinstance(value, dict) else value


def _additions(initial, current, identity):
    old = {identity(row): row for row in initial}
    new = {identity(row): row for row in current}
    if len(old) != len(initial) or len(new) != len(current) or any(new.get(key) != row for key, row in old.items()):
        raise ValueError("V32 immutable prior receipt/audit/log rows changed or duplicate identity")
    return [row for key, row in new.items() if key not in old]


def _stamp(value, state):
    try:
        stamp = datetime.fromisoformat(str(value))
        return datetime.fromisoformat(state["started_at"]) <= stamp <= datetime.now()
    except (TypeError, ValueError):
        return False


def _write_oracle(manifest, results, current):
    state, errors = manifest["feature_rank_index"], []
    rules, initial = state["rules"], state["initial"]
    writes, successes = {}, set()
    for result in results:
        request = result.get("request", result)
        observed = classify_result(request, result)["outcome"]
        if _outcome(result) != observed or observed not in {"success", "business_rejection"}:
            errors.append(f"V32 invalid result for{request['number']}: {_outcome(result)}/{observed}")
        if request["key"] in WRITE_KEYS:
            operation = _operation(request)
            if operation in writes:
                errors.append("V32 repeated planned operation identity")
            writes[operation] = (request, observed)
            if observed == "success":
                successes.add(operation)
    new = {}
    for table, identity in (("sect_donation_operations", lambda row: row["operation_id"]),
                             ("boss_purchase_operations", lambda row: row["operation_id"]),
                             ("operation_ledger", lambda row: (row["operation_id"], row["action"])),
                             ("operation_audit", lambda row: row["audit_id"]),
                             ("economy_log", lambda row: row["id"])):
        try:
            key = "game_db:" + table
            new[table] = _additions(initial[key], current[key], identity)
        except ValueError as exc:
            errors.append(str(exc) + ":" + table)
            new[table] = []
    expected_donations = {operation for operation in successes if writes[operation][0]["key"] == "sect_donate"}
    expected_purchases = {operation for operation in writes if writes[operation][0]["key"] == "boss_purchase"}
    if {row["operation_id"] for row in new["sect_donation_operations"]} != expected_donations:
        errors.append("V32 donation receipts differ from successful commands")
    if {row["operation_id"] for row in new["boss_purchase_operations"]} != expected_purchases:
        errors.append("V32 purchase receipts do not cover every successful/rejected request")
    donation_counts, purchase_counts, sect_counts = Counter(), Counter(), Counter()
    for receipt in new["sect_donation_operations"]:
        planned = writes.get(receipt["operation_id"])
        if not planned:
            continue
        request = planned[0]
        expected = {"operation_id": _operation(request), "user_id": request["actor"],
            "sect_id": request["sect_id"], "stone": DONATION, "materials": rules["materials"]}
        if any(receipt.get(key) != value for key, value in expected.items()):
            errors.append("V32 donation receipt payload mismatch")
        donation_counts[request["actor"]] += 1
        sect_counts[str(request["sect_id"])] += 1
    purchase_rows = {}
    for receipt in new["boss_purchase_operations"]:
        planned = writes.get(receipt["operation_id"])
        if not planned:
            continue
        request, outcome = planned
        actor = request["actor"]
        try:
            payload = json.loads(receipt["payload"])
            expected = [actor, ITEM_ID, rules["item_name"], rules["item_type"], 1,
                        rules["cost"], rules["weekly_limit"], rules["max_goods_num"]]
            if payload.get("request") != expected or payload.get("requested_quantity") != 1:
                raise ValueError("purchase receipt request mismatch")
            if outcome == "success":
                if (set(payload) != {"request", "requested_quantity"} or receipt["quantity"] != 1
                        or receipt["cost"] != rules["cost"] or receipt["purchased"] < 1
                        or receipt["inventory"] != 10 + receipt["purchased"]):
                    raise ValueError("successful purchase receipt mismatch")
                old_value = state["oracle"]["boss"]["actors"][actor]["value"]
                if receipt["integral"] != old_value - receipt["purchased"] * rules["cost"]:
                    raise ValueError("purchase receipt points mismatch")
                purchase_counts[actor] += 1
                purchase_rows.setdefault(actor, []).append(receipt["purchased"])
            else:
                rejection = payload.get("_rejection", {})
                if (rejection.get("status") != "state_changed" or rejection.get("reason") not in {
                        "boss_integral_changed", "weekly_purchase_changed"}
                        or rejection.get("subject") != actor or receipt["quantity"] != 0 or receipt["cost"] != 0
                        or any(rejection.get(key) != receipt[key]
                               for key in ("quantity", "cost", "integral", "purchased", "inventory"))):
                    raise ValueError("purchase CAS rejection did not retain exact zero-write receipt")
        except (AttributeError, TypeError, ValueError, KeyError) as exc:
            errors.append("V32 " + str(exc))
    if any(sorted(purchase_rows[actor]) != list(range(1, count + 1))
           for actor, count in purchase_counts.items()):
        errors.append("V32 purchase receipt sequence lost or duplicated a settled item")
    expected_ids, saved_outcomes = set(writes), {}
    for table in ("operation_ledger", "operation_audit"):
        rows = new[table]
        if len(rows) != len(writes) or {row["operation_id"] for row in rows} != expected_ids:
            errors.append("V32 ledger/audit lacks exact one-per-request correspondence:" + table)
        for row in rows:
            planned = writes.get(row["operation_id"])
            if not planned:
                continue
            request, observed = planned
            action = "sect.donate" if request["key"] == "sect_donate" else "boss.purchase"
            status = "applied" if observed == "success" else "rejected"
            if row["action"] != action or row["status"] != status:
                errors.append("V32 ledger/audit action or status mismatch")
            if table == "operation_ledger":
                try:
                    saved = json.loads(row["result_json"])
                    if (saved["operation_id"] != row["operation_id"] or saved["action"] != action
                            or saved["status"] != status or saved["ok"] != (status == "applied")
                            or saved["replayed"]):
                        raise ValueError("ledger result identity/status mismatch")
                    data = saved["data"]
                    if request["key"] == "sect_donate":
                        if data != {"status": "donated", "user_id": request["actor"],
                                    "sect_id": request["sect_id"], "stone": DONATION,
                                    "materials": rules["materials"]}:
                            raise ValueError("donation ledger data mismatch")
                    elif data.get("subject") != request["actor"] or data.get("status") != (
                            "applied" if status == "applied" else "state_changed"):
                        raise ValueError("purchase ledger data mismatch")
                    from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import request_hash

                    payload = ({"user_id": request["actor"], "sect_id": request["sect_id"],
                        "stone": DONATION, "materials": rules["materials"]}
                        if action == "sect.donate" else {"user_id": request["actor"], "item_id": ITEM_ID,
                            "quantity": 1, "unit_cost": rules["cost"], "weekly_limit": rules["weekly_limit"],
                            "max_goods_num": rules["max_goods_num"], "requested_quantity": 1})
                    if row["request_hash"] != request_hash(payload):
                        raise ValueError("ledger request hash differs from actual request")
                    if saved["before"] != {} or saved["after"] != {}:
                        raise ValueError("unexpected ledger asset projection")
                    if action == "sect.donate":
                        if saved["consumed"] != {} or saved["granted"] != {}:
                            raise ValueError("unexpected donation ledger grant")
                    elif status == "applied" and (saved["consumed"] != {"integral": rules["cost"]}
                            or saved["granted"] != {"integral": data["integral"]}):
                        raise ValueError("purchase ledger consumption/grant mismatch")
                    elif status == "rejected" and (saved["consumed"] or saved["granted"]):
                        raise ValueError("rejected purchase ledger contains a grant")
                    saved_outcomes[row["operation_id"]] = saved
                except (TypeError, ValueError, KeyError) as exc:
                    errors.append("V32 " + str(exc))
            else:
                saved = saved_outcomes.get(row["operation_id"])
                if saved is None or row["category"] != saved["audit_category"]:
                    errors.append("V32 audit category/result correspondence mismatch")
                else:
                    for column in ("before", "after", "consumed", "granted"):
                        if json.loads(row[column + "_json"]) != saved[column]:
                            errors.append("V32 audit independent asset assertion mismatch:" + column)
    economy = new["economy_log"]
    economy_counts = Counter()
    for row in economy:
        actor = str(row["user_id"])
        expected = {"sect_id": int(manifest["initial_users"]["actors"].get(actor, {}).get("sect_id", -1)),
            "source": "sect", "action": "donate", "stone_delta": -DONATION,
            "exp_delta": 0, "sect_contribution_delta": DONATION, "sect_scale_delta": DONATION,
            "sect_materials_delta": rules["materials"], "trace_id": None, "event_id": None}
        if (any(row.get(key) != value for key, value in expected.items())
                or json.loads(row["item_delta"]) != []
                or json.loads(row["detail"]) != {"donate_stone": DONATION, "materials_rate": rules["materials_rate"]}
                or not _stamp(row["created_at"], state)):
            errors.append("V32 exact donation economy delta mismatch")
        economy_counts[actor] += 1
    if economy_counts != donation_counts:
        errors.append("V32 donation economy rows differ from committed receipts")
    return donation_counts, purchase_counts, sect_counts, errors


def _expected_tables(manifest, current, donors, buyers, sect_counts):
    state, errors = manifest["feature_rank_index"], []
    rules = state["rules"]
    expected = deepcopy(state["initial"])
    for row in expected["game_db:season_rank"]:
        actor, count = str(row["user_id"]), donors[str(row["user_id"])]
        if count and row["rank_type"] == "宗门贡献":
            actual = next((value for value in current["game_db:season_rank"]
                if (value["rank_key"], value["user_id"], value["sect_id"]) ==
                   (row["rank_key"], row["user_id"], row["sect_id"])), None)
            row["score"] = int(sect._raw_value(row["score"])) + count * DONATION
            if actual is None or not _stamp(actual["updated_at"], state):
                errors.append("V32 season changed row has invalid update stamp")
            else:
                row["updated_at"] = actual["updated_at"]
            extra = {"event_key": "sect_donate", "amount": DONATION, "source": "sect",
                     "action": "donate", "sect_id": int(manifest["initial_users"]["actors"][actor]["sect_id"])}
            if actual is None or json.loads(actual["extra"]) != extra:
                errors.append("V32 season event extra differs from real donation")
            else:
                row["extra"] = actual["extra"]
    for row in expected["player_db:boss_limit"]:
        row["integral"] = (int(sect._raw_value(row["integral"])) - buyers[str(row["user_id"])] * rules["cost"]
                           if buyers[str(row["user_id"])] else row["integral"])
    for row in expected["game_db:back"]:
        count = buyers[str(row["user_id"])] if int(row["goods_id"]) == ITEM_ID else 0
        if count:
            row.update(goods_num=10 + count, bind_num=3 + count, goods_name=rules["item_name"],
                       goods_type=rules["item_type"], update_time=rules["purchase_day"])
    weekly_key = "player_db:" + state["weekly_table"]
    for row in expected[weekly_key]:
        count = buyers[str(row["user_id"])]
        if count:
            value = json.loads(row["weekly_purchases"])
            value[str(ITEM_ID)] = count
            actual = next((item for item in current[weekly_key] if item["user_id"] == row["user_id"]), None)
            if actual is None or json.loads(actual["weekly_purchases"]) != value:
                errors.append("V32 purchase weekly JSON delta mismatch")
            else:
                row["weekly_purchases"] = actual["weekly_purchases"]
    weekly = expected["game_db:sect_weekly_goal"]
    week = state["public"]["periods"]["weekly"]
    for sect_id, count in sect_counts.items():
        rows = [row for row in weekly if str(row["sect_id"]) == sect_id and row["week_key"] == week
                and row["goal_key"] == rules["weekly_goal"]]
        if not rows:
            row = {"sect_id": int(sect_id), "week_key": week, "goal_key": rules["weekly_goal"],
                "progress": 0, "target": rules["weekly_target"], "participants": "{}",
                "claimed_users": "[]", "updated_at": ""}
            weekly.append(row)
        else:
            row = rows[0]
        participants = json.loads(row["participants"])
        for actor, number in donors.items():
            if str(manifest["initial_users"]["actors"][actor]["sect_id"]) == sect_id:
                participants[actor] = int(participants.get(actor, 0)) + number * DONATION
        row["progress"] = min(int(row["progress"]) + count * DONATION, rules["weekly_target"])
        actual = next((item for item in current["game_db:sect_weekly_goal"] if
            (item["sect_id"], item["week_key"], item["goal_key"]) ==
            (row["sect_id"], row["week_key"], row["goal_key"])), None)
        if (actual is None or json.loads(actual["participants"]) != participants
                or not _stamp(actual["updated_at"], state)):
            errors.append("V32 exact weekly participants/progress delta mismatch")
        else:
            row.update(participants=actual["participants"], updated_at=actual["updated_at"])
    explicit = {"game_db:season_rank", "game_db:back", "game_db:sect_weekly_goal",
                "player_db:boss_limit", "player_db:boss", "player_db:boss_weekly_purchases"}
    for key in explicit:
        if sorted(expected[key], key=sect._encoded) != sorted(current[key], key=sect._encoded):
            errors.append("V32 complete mutable table differs from exact committed deltas:" + key)
    return errors


def check_invariants(context, managers, manifest, results):
    state, current = manifest["feature_rank_index"], _tables(context.paths)
    donors, buyers, sect_counts, errors = _write_oracle(manifest, results, current)
    errors.extend(_expected_tables(manifest, current, donors, buyers, sect_counts))
    users, sects = deepcopy(manifest["initial_users"]["actors"]), deepcopy(manifest["initial_sects"])
    for actor, count in donors.items():
        users[actor]["stone"] -= count * DONATION
        users[actor]["sect_contribution"] += count * DONATION
    for sect_id, count in sect_counts.items():
        for column, delta in (("sect_used_stone", DONATION), ("sect_scale", DONATION),
                              ("sect_materials", state["rules"]["materials"])):
            sects[sect_id][column] += count * delta
    selected = sect._selected_users(context.paths.game_db, manifest["population_ids"], manifest["actors"])
    if selected != {"actors": users, "background": manifest["initial_users"]["background"]}:
        errors.append("V32 full selected actor/background rows differ from exact donation deltas")
    if sect._sects(context.paths.game_db) != sects:
        errors.append("V32 complete sect rows differ from exact donation assets")
    frozen = _frozen(context.paths)
    if frozen != state["frozen"]:
        errors.append("V32 bounded unrelated tables/receipts/title/eligibility changed")
    generation = sect._generation(context.paths.game_db)
    expected_generation = {key: value + (sum(donors.values()) if key in {"generation", "stone_generation"} else 0)
                           for key, value in manifest["initial_generation"].items()}
    if generation != expected_generation:
        errors.append("V32 wallet/exp generation delta differs from committed donations")
    if ranks._identities(datetime.now()) != state["population"]["identities"]:
        errors.append("V32 season period changed during the frozen load")
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_boss

    if xiuxian_boss.boss_purchase_command_application.clock.now().date().isoformat() != state["rules"]["purchase_day"]:
        errors.append("V32 BOSS purchase business day changed during the frozen load")
    if int(shared._rows(context.paths.game_db, "SELECT COUNT(*) AS n FROM user_xiuxian")[0]["n"]) != manifest["users"]:
        errors.append("V32 main synthetic user count changed")
    counts = Counter(row.get("request", row)["key"] for row in results)
    if results:
        first = results[0].get("request", results[0])
        phase = {"pure": 0, "mixed": 1, "contention": 2}.get(first["phase_label"], -1)
        planned = request_plan(manifest, first["phase_label"], 96, phase)
        if [row.get("request", row) for row in results] != planned:
            # Completion order may differ; compare complete request identities.
            if sorted((row.get("request", row) for row in results), key=lambda row: row["number"]) != planned:
                errors.append("V32 frozen complete request plan/counts changed")
    return {"ok": not errors, "errors": errors, "per_command": dict(counts),
        "outcomes": dict(Counter(_outcome(row) for row in results)),
        "successful_donations": sum(donors.values()), "successful_purchases": sum(buyers.values()),
        "generation_after": generation, "frozen_tables": sorted(frozen["tables"]),
        "exclusions": frozen["exclusions"], "feature_table_rows": {key: len(value) for key, value in current.items()},
        "invariants": "real command replies/candidate scores/exact ranks; committed receipt-only deltas; full bounded season/BOSS/back/weekly/ledger/audit/economy tables; full selected users/sects; no duplicate debit/item, lost update or negative balance; unrelated bounded tables unchanged",
        "limits": manifest["limits"]}
