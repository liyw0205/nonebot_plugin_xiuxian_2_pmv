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

# A script invocation puts ``tests/`` on sys.path[0], so the repository root
# must be restored before importing the test package used by the probe.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

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

    async def command(
        self, name, text=None, *, actor="900001", number=None,
        allow_exception=False, timeout=8, event_type="group",
    ):
        from tests.parameter_exception_watch import watch_handler_catches
        candidates = [entry for entry in self.entries if entry["command"] == name or name in entry["aliases"]]
        assert len(candidates) == 1, (name, [entry["module"] for entry in candidates])
        with watch_handler_catches(candidates[0]) as caught:
            result = await run_case(candidates[0], "parameter", number or 910000 + len(self.calls),
                                    input_text=text or name, actor=actor, timeout=timeout,
                                    event_type=event_type)
        if caught:
            result.update(status="exception", caught_exceptions=caught)
        self.calls.append({"command": name, **result})
        if result["status"] == "blocked":
            raise ProbeBlocked(json.dumps(result, ensure_ascii=False))
        assert result["status"] == "pass" or (allow_exception and result["status"] == "exception"), json.dumps(result, ensure_ascii=False)
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
            columns = {
                str(row[1])
                for row in self.rows(
                    self.context.paths.impart_db,
                    "PRAGMA table_info(xiuxian_impart)",
                )
            }
            if "stone_num" in values and {
                "stone_num_exact", "stone_num_precision_state"
            }.issubset(columns):
                # The migrated fixture protects the legacy mirror with a
                # trigger; seed both representations as one exact value.
                values = dict(values)
                values["stone_num_exact"] = str(values["stone_num"])
                values["stone_num_precision_state"] = "exact"
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
    assert "剩余思恋结晶：80颗" in text, text
    receipt_sql = "SELECT payload,stone_num FROM impart_crystal_draw_operations"
    receipt = probe.rows(database, receipt_sql)
    replay = await probe.command("传承祈愿", "传承祈愿 2", number=910030)
    assert probe.rows(database, sql) == after
    assert "已经处理" in replay and probe.rows(database, receipt_sql) == receipt
    conflict = await probe.command("传承祈愿", "传承祈愿 3", number=910030)
    assert "请求与已处理请求不一致" in conflict, conflict
    assert probe.rows(database, sql) == after and probe.rows(database, receipt_sql) == receipt
    probe.evidence.update(before=before, after=after, receipt=receipt, replay_unchanged=True,
                          changed_payload_conflict=True)


@case("impart-prayer-consistent-legacy-rows", "传承祈愿", "祈愿次数 + 一致历史镜像行",
      "隔离迁移库中同一角色两条结晶/保底/闭关状态相同的 legacy 行",
      "合法祈愿只扣有效行；历史副本不写且不把镜像余额相加", group="impart-parameters")
async def impart_prayer_consistent_rows(probe):
    probe.impart(stone_num=100)
    database = probe.context.paths.impart_db
    columns = [row[1] for row in probe.rows(database, "PRAGMA table_info(xiuxian_impart)") if row[1] != "id"]
    names = ",".join('"' + column + '"' for column in columns)
    probe.seed(database, "DROP INDEX IF EXISTS xiuxian_impart_user_id_unique")
    probe.seed(database, f"INSERT INTO xiuxian_impart({names}) SELECT {names} FROM xiuxian_impart WHERE user_id='900001'")
    sql = "SELECT rowid,stone_num,wish,exp_day FROM xiuxian_impart WHERE user_id='900001' ORDER BY rowid"
    before = probe.rows(database, sql)
    assert len(before) == 2 and before[0][1:] == before[1][1:], before
    receipt_sql = "SELECT operation_id,payload,stone_num,exp_day FROM impart_crystal_draw_operations"
    text = await probe.command("传承祈愿", "传承祈愿 2", number=910031)
    after = probe.rows(database, sql)
    receipts = probe.rows(database, receipt_sql)
    assert "剩余思恋结晶：80颗" in text, text
    assert len(after) == 2 and after[0][1] == 80 and after[1][1] == 100, (before, after)
    assert after[1][2:] == before[1][2:] and len(receipts) == 1, (before, after, receipts)
    replay = await probe.command("传承祈愿", "传承祈愿 2", number=910031)
    assert "已经处理" in replay
    assert probe.rows(database, sql) == after and probe.rows(database, receipt_sql) == receipts
    probe.evidence.update(before=before, after=after, receipt_count=len(receipts), replay_unchanged=True,
                          effective_row_only=True, historical_row_unchanged=True)


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

from tests.parameter_cases_auction import register as register_auction

register_auction(case)

from tests.parameter_cases_world_boss import register as register_world_boss

register_world_boss(case)

from tests.parameter_cases_sect_body import register as register_sect_body

register_sect_body(case)

from tests.parameter_cases_sect_materials import register as register_sect_materials

register_sect_materials(case)

from tests.parameter_cases_sect_daily import register as register_sect_daily

register_sect_daily(case)

from tests.parameter_cases_sect_weekly import register as register_sect_weekly

register_sect_weekly(case)

from tests.parameter_cases_sect_mainbuff import register as register_sect_mainbuff

register_sect_mainbuff(case)

from tests.parameter_cases_sect_secbuff import register as register_sect_secbuff

register_sect_secbuff(case)

from tests.parameter_cases_sect_buff_search import register as register_sect_buff_search

register_sect_buff_search(case)

from tests.parameter_cases_sect_practice import register as register_sect_practice

register_sect_practice(case)

from tests.parameter_cases_sect_fairyland import register as register_sect_fairyland

register_sect_fairyland(case)

from tests.parameter_cases_sect_elixir_room import register as register_sect_elixir_room

register_sect_elixir_room(case)

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

from tests.parameter_cases_team_invite import register as register_team_invite

register_team_invite(case)

from tests.parameter_cases_team_join import register as register_team_join

register_team_join(case)

from tests.parameter_cases_team_reject import register as register_team_reject

register_team_reject(case)

from tests.parameter_cases_team_transfer import register as register_team_transfer

register_team_transfer(case)

from tests.parameter_cases_team_leave import register as register_team_leave

register_team_leave(case)

from tests.parameter_cases_team_kick import register as register_team_kick

register_team_kick(case)

from tests.parameter_cases_team_disband import register as register_team_disband

register_team_disband(case)

from tests.parameter_cases_team_view import register as register_team_view

register_team_view(case)

from tests.parameter_cases_dungeon_exit import register as register_dungeon_exit

register_dungeon_exit(case)

from tests.parameter_cases_dungeon_status import register as register_dungeon_status

register_dungeon_status(case)

from tests.parameter_cases_dungeon_info import register as register_dungeon_info

register_dungeon_info(case)

from tests.parameter_cases_dungeon_history import register as register_dungeon_history

register_dungeon_history(case)

from tests.parameter_cases_dungeon_explore import register as register_dungeon_explore

register_dungeon_explore(case)

from tests.parameter_cases_dungeon_shop import register as register_dungeon_shop

register_dungeon_shop(case)

from tests.parameter_cases_title_unlock import register as register_title_unlock

register_title_unlock(case)

from tests.parameter_cases_title_info import register as register_title_info

register_title_info(case)

from tests.parameter_cases_title_list import register as register_title_list

register_title_list(case)

from tests.parameter_cases_achievement import register as register_achievement

register_achievement(case)

from tests.parameter_cases_achievement_check import register as register_achievement_check

register_achievement_check(case)

from tests.parameter_cases_title_grant import register as register_title_grant

register_title_grant(case)

from tests.parameter_cases_title_refresh import register as register_title_refresh

register_title_refresh(case)

from tests.parameter_cases_title_help import register as register_title_help

register_title_help(case)

from tests.parameter_cases_help_search import register as register_help_search

register_help_search(case)

from tests.parameter_cases_log_view import register as register_log_view

register_log_view(case)

from tests.parameter_cases_statistics_view import register as register_statistics_view

register_statistics_view(case)

from tests.parameter_cases_additional import register as register_additional

register_additional(case)

from tests.parameter_cases_tianti_profile import register as register_tianti_profile

register_tianti_profile(case)

from tests.parameter_cases_tianti_bath_history import register as register_tianti_bath_history

register_tianti_bath_history(case)

from tests.parameter_cases_tianti_bath import register as register_tianti_bath

register_tianti_bath(case)

from tests.parameter_cases_tianti_stone import register as register_tianti_stone

register_tianti_stone(case)

from tests.parameter_cases_tianti_breakthrough import register as register_tianti_breakthrough

register_tianti_breakthrough(case)

from tests.parameter_cases_tianti_qiaoxue import register as register_tianti_qiaoxue

register_tianti_qiaoxue(case)

from tests.parameter_cases_tianti_settlement import register as register_tianti_settlement

register_tianti_settlement(case)

from tests.parameter_cases_ranking import register as register_ranking

register_ranking(case)

from tests.parameter_cases_tower_shop import register as register_tower_shop

register_tower_shop(case)

from tests.parameter_cases_training_shop import register as register_training_shop

register_training_shop(case)

from tests.parameter_cases_training_purchase import register as register_training_purchase

register_training_purchase(case)

from tests.parameter_cases_dongfu_harvest import register as register_dongfu_harvest

register_dongfu_harvest(case)

from tests.parameter_cases_dongfu_fertilize import register as register_dongfu_fertilize

register_dongfu_fertilize(case)

from tests.parameter_cases_dongfu_accelerate import register as register_dongfu_accelerate

register_dongfu_accelerate(case)

from tests.parameter_cases_dongfu_plant import register as register_dongfu_plant

register_dongfu_plant(case)

from tests.parameter_cases_dongfu_patrol import register as register_dongfu_patrol

register_dongfu_patrol(case)

from tests.parameter_cases_dongfu_infiltrate import register as register_dongfu_infiltrate

register_dongfu_infiltrate(case)

from tests.parameter_cases_dongfu_array import register as register_dongfu_array

register_dongfu_array(case)

from tests.parameter_cases_dongfu_visit import register as register_dongfu_visit

register_dongfu_visit(case)


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
