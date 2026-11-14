"""Run finite, explicit parameter paths through the live command registry."""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass
import inspect
import json
from pathlib import Path
import re
import sqlite3
import sys
import traceback
from types import ModuleType
from unittest.mock import AsyncMock, patch

import tests
from tests.command_probe import (
    FixtureSnapshot, ProbeBlocked, install_boundary, inventory, isolate_source,
    prepare, run_case,
)


@dataclass(frozen=True)
class Case:
    key: str
    group: str
    command: str
    form: str
    precondition: str
    expected_branch: str
    outcome: str
    run: object


CASES: list[Case] = []


def case(key, command, form, precondition, expected_branch, *, group="log-core", outcome="success"):
    def register(function):
        CASES.append(Case(key, group, command, form, precondition, expected_branch, outcome, function))
        return function
    return register


class Probe:
    def __init__(self, context, managers, entries):
        self.context, self.managers, self.entries = context, managers, entries
        self.calls = []
        self.evidence = {}

    async def command(self, name, text=None, *, actor="900001", number=None):
        from tests.parameter_exception_watch import watch_handler_catches
        candidates = [entry for entry in self.entries if entry["command"] == name or name in entry["aliases"]]
        assert len(candidates) == 1, (name, [entry["module"] for entry in candidates])
        with watch_handler_catches(candidates[0]) as caught:
            result = await run_case(candidates[0], "parameter", number or 910000 + len(self.calls),
                                    input_text=text or name, actor=actor)
        if caught:
            result.update(status="exception", caught_exceptions=caught)
        self.calls.append({"command": name, **result})
        if result["status"] == "blocked":
            raise ProbeBlocked(json.dumps(result, ensure_ascii=False))
        assert result["status"] == "pass", json.dumps(result, ensure_ascii=False)
        return "\n".join(result["replies"])

    def player(self, uid="900001", **values):
        from nonebot_plugin_xiuxian_2.core.numeric import adapt_sqlite_params
        game = self.managers[0]
        game.conn.execute("UPDATE user_xiuxian SET " + ",".join(f"{key}=?" for key in values)
                          + " WHERE user_id=?", adapt_sqlite_params((*values.values(), uid)))
        game.conn.commit()
        game._clear_read_cache()

    def rows(self, database, sql, params=()):
        with sqlite3.connect(database) as connection:
            return connection.execute(sql, params).fetchall()

    def seed(self, database, sql, params=()):
        from nonebot_plugin_xiuxian_2.core.numeric import adapt_sqlite_params
        with sqlite3.connect(database) as connection:
            connection.execute(sql, adapt_sqlite_params(params))

    def impart(self, uid="900001", **values):
        from nonebot_plugin_xiuxian_2.features.impart.repository import ImpartRepository
        ImpartRepository(self.context.paths.impart_db).state(uid, ensure=True)
        if values:
            self.seed(self.context.paths.impart_db, "UPDATE xiuxian_impart SET "
                      + ",".join(f"{key}=?" for key in values) + " WHERE user_id=?", (*values.values(), uid))

    def clear_cooldowns(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.lay_out import _shutdown_cooldown_runtimes
        _shutdown_cooldown_runtimes()

    def grant_item(self, item_id, quantity=5, uid="900001"):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
        definition = Items().get_data_by_item_id(item_id)
        assert definition, item_id
        self.managers[0].send_back(uid, item_id, definition["name"], definition["type"], quantity)
        return definition["name"]


@case("help-keyword", "修仙帮助", "关键词", "真实注册表，未构建帮助索引", "帮助检索命中实际签到命令")
async def help_keyword(probe):
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_base as base
    base._HELP_SEARCH_ENTRIES = None
    text = await probe.command("修仙帮助", "修仙帮助 签到")
    assert "帮助检索" in text and "签到" in text, text
    assert base._HELP_SEARCH_ENTRIES and any("签到" in row["command"] for row in base._HELP_SEARCH_ENTRIES)
    probe.evidence["indexed_commands"] = len(base._HELP_SEARCH_ENTRIES)


@case("help-keyword-nested-install", "修仙帮助", "关键词", "与生产 src.plugins 一致的嵌套包身份", "从插件根包检索实际 manifest")
async def help_keyword_nested(probe):
    import nonebot_plugin_xiuxian_2 as package
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_base as base
    root_name = "src.plugins.nonebot_plugin_xiuxian_2"
    with patch.dict(sys.modules, {"src": ModuleType("src"), root_name: package}), \
            patch.object(base, "__package__", root_name + ".xiuxian.xiuxian_base"), \
            patch.object(base, "_HELP_SEARCH_ENTRIES", None):
        text = await probe.command("修仙帮助", "修仙帮助 签到")
        assert "帮助检索" in text and base._HELP_SEARCH_ENTRIES, text
        probe.evidence["indexed_commands"] = len(base._HELP_SEARCH_ENTRIES)


@case("stone-training-valid-amount", "灵石修炼", "正整数数量", "注册玩家，1000 灵石，0 修为", "真实结算增加修为并扣费，重复事件无二次扣费")
async def stone_training(probe):
    probe.player(exp=0, stone=1000)
    database = probe.context.paths.game_db
    sql = "SELECT exp,stone FROM user_xiuxian WHERE user_id='900001'"
    before = probe.rows(database, sql)[0]
    text = await probe.command("灵石修炼", "灵石修炼 100", number=910010)
    after = probe.rows(database, sql)[0]
    assert "修炼结束" in text and after[0] > before[0] and after[1] < before[1], (text, before, after)
    replay = await probe.command("灵石修炼", "灵石修炼 100", number=910010)
    assert "已经处理" in replay and probe.rows(database, sql)[0] == after, replay
    probe.evidence.update(before=before, after=after, replay_unchanged=True)


@case("stone-gift-name-quantity", "送灵石", "真实道号 正整数数量", "双方注册且资源充足，赠送冷却未用", "双方灵石变化与赠送回执，重复消息不重复扣费")
async def stone_gift(probe):
    database = probe.context.paths.game_db
    sql = "SELECT user_id,stone FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id"
    before = probe.rows(database, sql)
    text = await probe.command("送灵石", "送灵石 诊断对手 1000", number=910020)
    after = probe.rows(database, sql)
    assert after[0][1] < before[0][1] and after[1][1] > before[1][1], (text, before, after)
    await probe.command("送灵石", "送灵石 诊断对手 1000", number=910020)
    assert probe.rows(database, sql) == after
    probe.evidence.update(before=before, after=after, replay_unchanged=True)


@case("impart-prayer-valid-count", "传承祈愿", "祈愿次数", "真实传承初始化，100 结晶", "结晶扣20、祈愿/时间/冻结回执入库且重放不重复")
async def impart_prayer(probe):
    probe.impart(stone_num=100)
    database = probe.context.paths.impart_db
    sql = "SELECT stone_num,wish,exp_day FROM xiuxian_impart WHERE user_id='900001'"
    before = probe.rows(database, sql)
    text = await probe.command("传承祈愿", "传承祈愿 2", number=910030)
    after = probe.rows(database, sql)
    assert after and all(row[0] == 80 for row in after), (text, before, after)
    assert all(row[2] > before[0][2] for row in after), (text, before, after)
    await probe.command("传承祈愿", "传承祈愿 2", number=910030)
    assert probe.rows(database, sql) == after
    probe.evidence.update(before=before, after=after, replay_unchanged=True)


@case("impart-closing-high-realm", "虚神界出关", "闭关上下文（无额外参数）", "最高合法境界、超 int64 修为、闭关60分钟、30祝福", "出关结算落库，type清零、祝福与回执一致")
async def impart_closing(probe):
    from datetime import timedelta
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart_pk as module
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.data_source import jsondata
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import OtherSet
    levels = jsondata.level_data()
    level = max(levels, key=lambda name: int(OtherSet().set_closing_type(name)))
    probe.player(level=level, exp=10 ** 25, power=10 ** 25)
    probe.impart(exp_day=30)
    probe.managers[0].get_user_cd("900001")
    start = (module.runtime_clock.now() - timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S")
    probe.seed(probe.context.paths.game_db,
               "UPDATE user_cd SET type=4,create_time=? WHERE user_id='900001'", (start,))
    before = probe.rows(probe.context.paths.game_db, "SELECT exp,power FROM user_xiuxian WHERE user_id='900001'")
    text = await probe.command("虚神界出关", number=910040)
    state = probe.rows(probe.context.paths.game_db, "SELECT type FROM user_cd WHERE user_id='900001'")
    assert state == [(0,)] and "闭关结束" in text, (state, text)
    receipt = probe.rows(probe.context.paths.game_db, "SELECT result_json FROM impart_closing_operations")
    assert len(receipt) == 1
    after = probe.rows(probe.context.paths.game_db, "SELECT exp,power FROM user_xiuxian WHERE user_id='900001'")
    assert after[0][0] >= before[0][0]
    await probe.command("虚神界出关", number=910040)
    assert probe.rows(probe.context.paths.game_db, "SELECT exp,power FROM user_xiuxian WHERE user_id='900001'") == after
    probe.evidence.update(level=level, before=before, after=after, receipt=json.loads(receipt[0][0]), replay_unchanged=True)


def seed_cultivation(probe, protection):
    probe.player(exp=500)
    probe.player("900002", exp=300)
    for uid in ("900001", "900002"):
        probe.impart(uid)
    probe.seed(probe.context.paths.player_db,
               "INSERT INTO status(user_id,two_exp_protect) VALUES('900002',?) "
               "ON CONFLICT(user_id) DO UPDATE SET two_exp_protect=excluded.two_exp_protect", (protection,))


@case("cultivation-name-count-direct", "双修", "真实道号 次数后缀", "非道侣、对方保护关闭、双方次数和修为充足", "双方真实修为增加、消耗两次、持久结算回执")
async def cultivation_direct(probe):
    seed_cultivation(probe, "off")
    sql = "SELECT exp FROM user_xiuxian WHERE user_id IN ('900001','900002') ORDER BY user_id"
    before = probe.rows(probe.context.paths.game_db, sql)
    text = await probe.command("双修", "双修 诊断对手 2次")
    after = probe.rows(probe.context.paths.game_db, sql)
    assert all(new[0] > old[0] for new, old in zip(after, before)), (text, before, after)
    usage = probe.rows(probe.context.paths.player_db, "SELECT used_count FROM partner_two_exp_usage ORDER BY user_id")
    assert usage == [(2,), (2,)], (text, usage)
    probe.evidence.update(before=before, after=after, usage=usage)


@case("cultivation-invite-accept", "双修", "真实道号 次数 + 受邀方同意", "保护开启、资源/次数充足，邀请过期定时器边界受控", "邀请pending→accepted、双方修为和次数同事务提交")
async def cultivation_invite(probe):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff import partner
    seed_cultivation(probe, "on")
    with patch.object(partner, "expire_invite", new=AsyncMock()):
        text = await probe.command("双修", "双修 诊断对手 2")
    sql = "SELECT status,inviter_id,target_id FROM partner_cultivation_invites"
    pending = probe.rows(probe.context.paths.player_db, sql)
    assert pending == [("pending", "900001", "900002")], (text, pending)
    before = probe.rows(probe.context.paths.game_db, "SELECT exp FROM user_xiuxian ORDER BY user_id")
    probe.clear_cooldowns()
    text = await probe.command("同意双修", actor="900002")
    after = probe.rows(probe.context.paths.game_db, "SELECT exp FROM user_xiuxian ORDER BY user_id")
    assert probe.rows(probe.context.paths.player_db, sql)[0][0] == "accepted", text
    assert after[0][0] > before[0][0] and after[1][0] > before[1][0], (text, before, after)
    probe.evidence.update(before=before, after=after, invitation="accepted", timer="not exercised; scheduled expiry stubbed")


def parameter_inventory(entries):
    result = []
    for entry in entries:
        sources = [inspect.getsource(inspect.unwrap(handler.call)) for handler in entry["matcher"].handlers]
        source = "\n".join(sources)
        relevant = any(token in source for token in (
            "CommandArg(", "RegexGroup(", "RegexDict(", "event.get_message(",
            "event.message", "event.get_plaintext(", "event.get_plain_text(",
        )) or bool(entry["patterns"])
        if not relevant:
            continue
        assigned = [spec for spec in CASES if spec.command in {entry["command"], *entry["aliases"]}]
        result.append({
            **{key: value for key, value in entry.items() if key != "matcher"},
            "parameter_evidence": [token for token in ("CommandArg(", "RegexGroup(", "RegexDict(", "event.get_message(", "event.message", "event.get_plaintext(") if token in source],
            "syntax_literals": list(dict.fromkeys(re.findall(r'[\"\']([^\"\'\n]{1,100})[\"\']', source)))[:50],
            "planned_cases": [spec.key for spec in assigned],
            "coverage": "explicit_cases" if assigned else "pending_semantic_cases",
        })
    return result


from tests.parameter_cases_assets import register as register_assets

register_assets(case)

from tests.parameter_cases_impart import register as register_impart

register_impart(case)

from tests.parameter_cases_pet import register as register_pet

register_pet(case)

from tests.parameter_cases_market import register as register_market

register_market(case)

from tests.parameter_cases_sect_body import register as register_sect_body

register_sect_body(case)

from tests.parameter_cases_bag_consumers import register as register_bag_consumers

register_bag_consumers(case)

from tests.parameter_cases_activity import register as register_activity

register_activity(case)

from tests.parameter_cases_admin import register as register_admin

register_admin(case)

from tests.parameter_cases_map_fusion import register as register_map_fusion

register_map_fusion(case)

from tests.parameter_cases_secondary import register as register_secondary

register_secondary(case)

from tests.parameter_cases_remaining import register as register_remaining

register_remaining(case)

from tests.parameter_cases_additional import register as register_additional

register_additional(case)


async def execute(args, checkpoint):
    context, managers = await prepare()
    entries = inventory()
    matrix = parameter_inventory(entries)
    selected = [spec for spec in CASES if (not args.group or spec.group in args.group)
                and (not args.case or spec.key in args.case)]
    if args.retry_from:
        previous = json.loads(args.retry_from.read_text(encoding="utf-8"))
        failed = {row["key"] for row in previous["results"] if row["status"] != "pass"}
        selected = [spec for spec in selected if spec.key in failed]
    snapshot = FixtureSnapshot(tests._data_dir, managers)
    rows = []
    report = {"scope": "real registered parameter dispatch/DI/handler/application/repository; isolated migrated fixture; mock transport",
              "base_sha": args.base_sha, "parameter_commands": len(matrix), "matrix": matrix, "results": rows}
    checkpoint(report)
    if not args.inventory_only:
        assert selected, "no explicit parameter cases selected"
        for spec in selected:
            snapshot.restore()
            probe = Probe(context, managers, entries)
            row = {key: value for key, value in vars(spec).items() if key != "run"}
            try:
                await spec.run(probe)
                assert probe.calls, "a parameter case must dispatch a real command"
                row.update(status="pass", actual_branch=spec.expected_branch)
            except ProbeBlocked as exc:
                row.update(status="blocked", actual_branch="external or routing boundary", error=str(exc))
            except Exception:
                row.update(status="exception", actual_branch="failed before asserted outcome", error=traceback.format_exc())
            row.update(calls=probe.calls, evidence=probe.evidence)
            rows.append(row)
            report["summary"] = dict(Counter(row["status"] for row in rows))
            report["outcomes"] = dict(Counter(row["outcome"] for row in rows if row["status"] == "pass"))
            checkpoint(report)
            print(json.dumps({key: row[key] for key in ("key", "command", "status", "outcome")}, ensure_ascii=False), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--group", action="append")
    parser.add_argument("--case", action="append")
    parser.add_argument("--retry-from", type=Path)
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--base-sha", default="505f4cb70f82304da5d9c56721e980abaf75f226")
    args = parser.parse_args()
    with args.report.open("w", encoding="utf-8") as report:
        isolate_source()
        install_boundary(tests._data_dir)

        def checkpoint(value):
            report.seek(0)
            json.dump(value, report, ensure_ascii=False, indent=2)
            report.write("\n")
            report.truncate()
            report.flush()

        result = asyncio.run(execute(args, checkpoint))
        checkpoint(result)
        print(json.dumps({key: result.get(key) for key in ("parameter_commands", "summary", "outcomes")}, ensure_ascii=False))
        return int(result.get("summary", {}).get("exception", 0) > 0)


if __name__ == "__main__":
    raise SystemExit(main())
