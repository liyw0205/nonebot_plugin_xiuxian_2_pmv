"""Focused real-dispatch crystal prayer cases, isolated from player storage."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import tests
from tests.parameter_probe import case, execute, install_boundary, isolate_source


def seed_balance(probe, value, *, mirror=False):
    from nonebot_plugin_xiuxian_2.features.impart.repository import ImpartRepository
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    database = probe.context.paths.impart_db
    ImpartRepository(database).state("900001", ensure=True)
    with DatabaseUnitOfWork(database) as uow:
        uow.execute(
            "UPDATE xiuxian_impart SET stone_num=?,stone_num_exact=?,wish=0,exp_day=0 "
            "WHERE user_id=?", (value, str(value), "900001"),
        )
        if mirror:
            # Startup reconciliation installs this guard. Drop it only in this
            # isolated fixture so the command path can inspect legacy duplicates.
            uow.execute("DROP INDEX IF EXISTS xiuxian_impart_user_id_unique")
            fields = [str(row[1]) for row in uow.execute("PRAGMA table_info(xiuxian_impart)") if row[1] != "id"]
            columns = ",".join('"' + field + '"' for field in fields)
            uow.execute(f"INSERT INTO xiuxian_impart({columns}) SELECT {columns} FROM xiuxian_impart WHERE user_id=?", ("900001",))


def snapshot(probe):
    return (
        probe.rows(probe.context.paths.impart_db, "SELECT * FROM xiuxian_impart ORDER BY rowid"),
        probe.rows(probe.context.paths.impart_db, "SELECT * FROM impart_crystal_draw_operations ORDER BY operation_id"),
        probe.rows(probe.context.paths.impart_db, "SELECT * FROM impart_cards ORDER BY user_id,card_name"),
        probe.rows(probe.context.paths.player_db, "SELECT * FROM statistics ORDER BY user_id"),
        probe.rows(probe.context.paths.game_db, "SELECT * FROM operation_ledger ORDER BY operation_id"),
    )


async def successful_prayer(probe, value, mirror):
    seed_balance(probe, value, mirror=mirror)
    text = await probe.command("传承祈愿", "传承祈愿 2", number=920001)
    expected = value - 20
    assert f"剩余思恋结晶：{expected}颗" in text, text
    balances = probe.rows(probe.context.paths.impart_db, "SELECT stone_num_exact FROM xiuxian_impart WHERE user_id='900001'")
    assert [int(row[0]) for row in balances] == ([expected, value] if mirror else [expected])
    receipts = probe.rows(probe.context.paths.impart_db, "SELECT stone_num_exact FROM impart_crystal_draw_operations")
    assert receipts == [(str(expected),)]
    stats = probe.rows(probe.context.paths.player_db, 'SELECT "传承祈愿次数","思恋结晶消耗" FROM statistics WHERE user_id=\'900001\'')
    assert stats == [(2, 20)], stats
    once = snapshot(probe)
    replay = await probe.command("传承祈愿", "传承祈愿 2", number=920001)
    assert "已经处理" in replay and snapshot(probe) == once
    conflict = await probe.command("传承祈愿", "传承祈愿 3", number=920001)
    assert "请求与已处理请求不一致" in conflict and snapshot(probe) == once
    probe.evidence.update(exact_debit=20, exact_remaining=str(expected), mirror_count=len(balances),
                          receipt_exact=True, replay_zero_write=True, payload_conflict_zero_write=True)


@case("crystal-normal", "传承祈愿", "合法次数 2", "真实迁移库，余额100", "扣20且回执/回复精确", group="crystal-exact")
async def normal(probe):
    await successful_prayer(probe, 100, False)


@case("crystal-670-prayer-67", "传承祈愿", "合法次数 67", "真实迁移库，余额670", "精确扣除670并返回0", group="crystal-exact")
async def sixty_seven_pulls(probe):
    seed_balance(probe, 670)
    before = snapshot(probe)
    info = await probe.command("传承信息", "传承信息")
    assert "> 670颗" in info, info
    assert snapshot(probe) == before
    text = await probe.command("传承祈愿", "传承祈愿67", number=920067)
    assert "消耗思恋结晶：670颗" in text, text
    assert "剩余思恋结晶：0颗" in text, text
    balances = probe.rows(
        probe.context.paths.impart_db,
        "SELECT stone_num,stone_num_exact,stone_num_precision_state "
        "FROM xiuxian_impart WHERE user_id='900001'",
    )
    assert balances == [(0, "0", "exact")], balances
    receipts = probe.rows(
        probe.context.paths.impart_db,
        "SELECT stone_num,stone_num_exact,stone_num_precision_state "
        "FROM impart_crystal_draw_operations",
    )
    assert receipts == [(0, "0", "exact")], receipts
    stats = probe.rows(
        probe.context.paths.player_db,
        'SELECT "传承祈愿次数","思恋结晶消耗" FROM statistics WHERE user_id=\'900001\'',
    )
    assert stats == [(67, 670)], stats
    once = snapshot(probe)
    info = await probe.command("传承信息", "传承信息")
    assert "> 0颗" in info and snapshot(probe) == once, info
    replay = await probe.command("传承祈愿", "传承祈愿67", number=920067)
    assert "已经处理" in replay and snapshot(probe) == once, replay
    probe.evidence.update(exact_debit=670, exact_remaining="0", requested_pulls=67)


@case("crystal-large-mirrors", "传承祈愿", "合法次数 2", "两个一致镜像，10**45+123精确余额", "只扣有效行20，历史副本不变", group="crystal-exact")
async def large(probe):
    await successful_prayer(probe, 10**45 + 123, True)


@case("crystal-real-refusal", "传承祈愿", "合法次数 2", "已失精度旧REAL", "具体精度拒绝且业务零写", group="crystal-exact", outcome="rejection")
async def real_refusal(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
    from nonebot_plugin_xiuxian_2.features.impart.migrations import apply_impart_exact_crystal_balances

    seed_balance(probe, 100)
    with DatabaseUnitOfWork(probe.context.paths.impart_db) as uow:
        uow.execute("DROP TRIGGER impart_crystal_exact_write_guard")
        uow.execute("UPDATE xiuxian_impart SET stone_num=CAST(? AS REAL),stone_num_exact=NULL WHERE user_id='900001'", (float(2**63),))
        apply_impart_exact_crystal_balances(uow)
    before = snapshot(probe)
    info = await probe.command("传承信息", "传承信息")
    assert "超出精确范围的浮点数保存" in info, info
    assert snapshot(probe) == before
    reply = await probe.command("传承祈愿", "传承祈愿 2")
    assert "超出精确范围的浮点数保存" in reply, reply
    assert snapshot(probe) == before
    probe.evidence.update(reason="crystal_precision_lost_real", zero_business_write=True)


def seed_divergent_rows(probe):
    seed_balance(probe, 670, mirror=True)
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    with DatabaseUnitOfWork(probe.context.paths.impart_db) as uow:
        fields = [str(row[1]) for row in uow.execute("PRAGMA table_info(xiuxian_impart)") if row[1] != "id"]
        columns = ",".join('"' + field + '"' for field in fields)
        uow.execute(f"INSERT INTO xiuxian_impart({columns}) SELECT {columns} FROM xiuxian_impart WHERE user_id=? ORDER BY rowid LIMIT 1", ("900001",))
    rows = probe.rows(
        probe.context.paths.impart_db,
        "SELECT id FROM xiuxian_impart WHERE user_id='900001' ORDER BY id",
    )
    assert len(rows) == 3, rows
    for row, balance in zip(rows, (670, 460, 310)):
        probe.seed(
            probe.context.paths.impart_db,
            "UPDATE xiuxian_impart SET stone_num=?,stone_num_exact=? WHERE id=?",
            (balance, str(balance), row[0]),
        )
    return rows


def history_rows(probe):
    return probe.rows(probe.context.paths.impart_db,
                      "SELECT * FROM xiuxian_impart WHERE user_id='900001' ORDER BY rowid")[1:]


@case("crystal-divergent-info", "传承信息", "无参", "有效行670，历史副本460/310", "展示有效行670，不被历史副本阻断", group="crystal-exact")
async def divergent_info(probe):
    seed_divergent_rows(probe)
    before = snapshot(probe)
    reply = await probe.command("传承信息", "传承信息")
    assert "> 670颗" in reply and snapshot(probe) == before, reply
    probe.evidence.update(effective_balance=670, ignored_history=[460, 310], zero_write=True)


@case("crystal-divergent-info-argument", "传承信息", "带参 67", "有效行670，历史副本460/310", "带参信息路径同样展示有效行670", group="crystal-exact")
async def divergent_info_argument(probe):
    seed_divergent_rows(probe)
    before = snapshot(probe)
    reply = await probe.command("传承信息", "传承信息 67")
    assert "> 670颗" in reply and snapshot(probe) == before, reply
    probe.evidence.update(effective_balance=670, ignored_history=[460, 310], argument=True, zero_write=True)


@case("crystal-divergent-prayer", "传承祈愿", "无参", "有效行670，历史副本460/310", "只扣有效行10颗，历史副本零写", group="crystal-exact")
async def divergent_prayer(probe):
    seed_divergent_rows(probe)
    before_history = history_rows(probe)
    reply = await probe.command("传承祈愿", "传承祈愿", number=920068)
    assert "剩余思恋结晶：660颗" in reply, reply
    balances = probe.rows(
        probe.context.paths.impart_db,
        "SELECT stone_num_exact FROM xiuxian_impart WHERE user_id='900001' ORDER BY id",
    )
    assert [int(row[0]) for row in balances] == [660, 460, 310], balances
    assert history_rows(probe) == before_history
    probe.evidence.update(effective_balance=670, remaining=660, ignored_history=[460, 310], zero_history_write=True)


@case("crystal-divergent-prayer-67", "传承祈愿", "带参 67", "有效行670，历史副本460/310", "精确扣670并只写有效行，历史副本零写", group="crystal-exact")
async def divergent_sixty_seven(probe):
    seed_divergent_rows(probe)
    before = snapshot(probe)
    before_history = history_rows(probe)
    reply = await probe.command("传承祈愿", "传承祈愿67", number=920067)
    assert "消耗思恋结晶：670颗" in reply and "剩余思恋结晶：0颗" in reply, reply
    balances = probe.rows(
        probe.context.paths.impart_db,
        "SELECT stone_num_exact FROM xiuxian_impart WHERE user_id='900001' ORDER BY id",
    )
    assert [int(row[0]) for row in balances] == [0, 460, 310], balances
    assert history_rows(probe) == before_history
    assert snapshot(probe) != before
    probe.evidence.update(effective_balance=670, remaining=0, ignored_history=[460, 310], zero_history_write=True)


@case("crystal-zero", "传承祈愿", "无参", "合法零余额", "信息显示零，祈愿具体拒绝不足且零写", group="crystal-exact", outcome="rejection")
async def zero_balance(probe):
    seed_balance(probe, 0)
    before = snapshot(probe)
    info = await probe.command("传承信息", "传承信息 67")
    assert "> 0颗" in info, info
    reply = await probe.command("传承祈愿", "传承祈愿")
    assert "不足" in reply, reply
    assert snapshot(probe) == before


@case("crystal-missing", "传承信息", "无参", "无传承行", "按既有规则建立零余额当前行，祈愿不足零写", group="crystal-exact", outcome="rejection")
async def missing_state(probe):
    probe.seed(probe.context.paths.impart_db, "DELETE FROM xiuxian_impart WHERE user_id='900001'")
    info = await probe.command("传承信息", "传承信息")
    assert "> 0颗" in info, info
    before = snapshot(probe)
    reply = await probe.command("传承祈愿", "传承祈愿 67")
    assert "不足" in reply and snapshot(probe) == before, reply


@case("crystal-invalid", "传承信息", "无参", "有效行非法精确余额", "信息与祈愿均具体拒绝非法数值且零写", group="crystal-exact", outcome="rejection")
async def invalid_state(probe):
    seed_balance(probe, 670)
    probe.seed(probe.context.paths.impart_db, "UPDATE xiuxian_impart SET stone_num_exact='1.5' WHERE user_id='900001'")
    before = snapshot(probe)
    for command in ("传承信息", "传承祈愿"):
        reply = await probe.command(command, command + " 67")
        assert "非法" in reply or "无效" in reply, reply
        assert snapshot(probe) == before


@case("crystal-receipt-refusal", "传承祈愿", "相同事件合法次数 2", "旧回执余额REAL不可恢复", "不得冒充重复成功或payload冲突", group="crystal-exact", outcome="rejection")
async def receipt_refusal(probe):
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

    seed_balance(probe, 100)
    with DatabaseUnitOfWork(probe.context.paths.impart_db) as uow:
        uow.execute(
            "INSERT INTO impart_crystal_draw_operations(operation_id,payload,wish,stone_num,"
            "stone_num_precision_state,exp_day,cards_json) VALUES(?,?,0,?,'precision_lost_real',0,'[]')",
            ("impart-crystal-draw:920002:900001", '["900001",20,2,"requested_pulls_v1"]', float(2**63)),
        )
    before = snapshot(probe)
    reply = await probe.command("传承祈愿", "传承祈愿 2", number=920002)
    assert "超出精确范围的浮点数保存" in reply and "已经处理" not in reply, reply
    assert snapshot(probe) == before
    probe.evidence.update(reason="crystal_precision_lost_real", corrupt_receipt_not_success=True)


def main():
    isolate_source()
    install_boundary(tests._data_dir)
    result = asyncio.run(execute(SimpleNamespace(group=["crystal-exact"], case=None,
        retry_from=None, inventory_only=False, base_sha="crystal-exact-regression"), lambda value: None))
    assert result["summary"] == {"pass": 12}, result["results"]
    print("Crystal dispatch: 12 isolated matcher cases; zero exceptions/blocked")


if __name__ == "__main__":
    main()
