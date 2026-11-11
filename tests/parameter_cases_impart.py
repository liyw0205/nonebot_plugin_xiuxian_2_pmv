"""Real card names, combinations, counts, and historical state branches."""
from unittest.mock import patch


def definitions():
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart as module
    return module.impart_application.card_definitions()


def cards(probe):
    return probe.rows(probe.context.paths.impart_db,
                      "SELECT card_name,quantity FROM impart_cards WHERE user_id='900001' ORDER BY card_name")


def seed_card(probe, name, count):
    probe.seed(probe.context.paths.impart_db,
               "INSERT INTO impart_cards(user_id,card_name,quantity) VALUES('900001',?,?)", (name, count))


def register(case):
    @case("impart-draw-multiple", "传承抽卡", "实际抽卡次数", "足够灵石，真实卡图定义和初始化传承",
          "实际抽2张卡，扣20000000灵石，次数与回执持久化且重放不重复", group="impart-parameters")
    async def draw(probe):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart as module
        probe.impart()
        probe.player(stone=100_000_000)
        with patch.object(module, "_rank_success", return_value=True):
            text = await probe.command("传承抽卡", "传承抽卡 2", number=930001)
        assert sum(row[1] for row in cards(probe)) == 2, text
        assert probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(80_000_000,)]
        assert probe.rows(probe.context.paths.impart_db, "SELECT impart_num FROM xiuxian_impart WHERE user_id='900001'") == [(2,)]
        after = cards(probe)
        await probe.command("传承抽卡", "传承抽卡 2", number=930001)
        assert after == cards(probe)
        probe.evidence.update(cards=after, spent=20_000_000, replay_unchanged=True)

    for command in ("传承祈愿", "传承抽卡"):
        def build(command):
            async def run(probe):
                from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart as module
                probe.impart(stone_num=100)
                probe.player(stone=100_000_000)
                with patch.object(module, "_rank_success", return_value=False):
                    text = await probe.command(command, command + " 2", number=930011)
                assert not cards(probe), (text, cards(probe))
                state = probe.rows(probe.context.paths.impart_db, "SELECT stone_num,wish,exp_day,impart_num FROM xiuxian_impart WHERE user_id='900001'")
                assert state[0][1] == 20, (text, state)
                if command == "传承祈愿":
                    assert state[0][0] == 80 and state[0][2] == 132, (text, state)
                else:
                    assert state[0][3] == 2
                    assert probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(80_000_000,)]
                replay = await probe.command(command, command + " 2", number=930011)
                assert "已经处理" in replay, replay
                assert probe.rows(probe.context.paths.impart_db, "SELECT stone_num,wish,exp_day,impart_num FROM xiuxian_impart WHERE user_id='900001'") == state
                probe.evidence.update(state=state, cards=[], rng="miss branch", frozen_replay=True)
            return run
        case("impart-" + command + "-miss-replay", command, "合法2次，随机边界固定未中奖",
             "资源充足、保底0；只控制随机结果，真实结算未替换", "无卡仍正确扣费/累计保底与次数/时间，原回执重放",
             group="impart-parameters")(build(command))

    @case("impart-compose-two-card-names", "传承合成", "合法源卡名 合法目标卡名", "源卡6张，目标卡0张",
          "源卡减5/目标卡加1、回执持久化且重放不再次消费", group="impart-parameters")
    async def compose(probe):
        probe.impart()
        source, target = list(definitions())[:2]
        seed_card(probe, source, 6)
        text = await probe.command("传承合成", f"传承合成 {source} {target}", number=930002)
        assert dict(cards(probe)) == {source: 1, target: 1}, (text, cards(probe))
        after = cards(probe)
        await probe.command("传承合成", f"传承合成 {source} {target}", number=930002)
        assert cards(probe) == after
        probe.evidence.update(source=source, target=target, after=after, replay_unchanged=True)

    @case("impart-disassemble-name-count", "传承分解", "真实卡名 正整数数量", "源卡4张、结晶100颗",
          "保留2张卡并增加4结晶、持久回执且重放不增加", group="impart-parameters")
    async def disassemble(probe):
        probe.impart(stone_num=100)
        name = next(iter(definitions()))
        seed_card(probe, name, 4)
        text = await probe.command("传承分解", f"传承分解 {name} 2", number=930003)
        assert dict(cards(probe)) == {name: 2}, (text, cards(probe))
        assert probe.rows(probe.context.paths.impart_db, "SELECT stone_num FROM xiuxian_impart WHERE user_id='900001'") == [(104,)]
        await probe.command("传承分解", f"传承分解 {name} 2", number=930003)
        assert dict(cards(probe)) == {name: 2}
        assert probe.rows(probe.context.paths.impart_db, "SELECT stone_num FROM xiuxian_impart WHERE user_id='900001'") == [(104,)]
        probe.evidence.update(card=name, remaining=2, crystals=104, replay_unchanged=True)

    @case("impart-back-keyword-page", "传承背包", "真实卡名 页码", "持有合法卡并初始化传承",
          "筛选实际卡并显示背包页，不修改资产", group="impart-parameters")
    async def card_back(probe):
        probe.impart()
        name = next(iter(definitions()))
        seed_card(probe, name, 4)
        before = cards(probe)
        text = await probe.command("传承背包", f"传承背包 {name} 1")
        assert name in text and "4" in text and cards(probe) == before, text
        probe.evidence.update(card=name, inventory_unchanged=True)

    @case("impart-prayer-inconsistent-history", "传承祈愿", "合法次数 + 历史重复结晶不一致",
          "两条历史传承记录仅结晶余额不同，有足够资源", "明确历史结晶记录拒绝，所有历史行/卡/资产不变",
          group="impart-parameters", outcome="rejection")
    async def divergent(probe):
        probe.impart(stone_num=100)
        database = probe.context.paths.impart_db
        columns = [row[1] for row in probe.rows(database, "PRAGMA table_info(xiuxian_impart)") if row[1] != "id"]
        names = ",".join('"' + column + '"' for column in columns)
        probe.seed(database, f"INSERT INTO xiuxian_impart({names}) SELECT {names} FROM xiuxian_impart WHERE user_id='900001'")
        probe.seed(database, "UPDATE xiuxian_impart SET stone_num=101 WHERE rowid=(SELECT MAX(rowid) FROM xiuxian_impart)")
        before = probe.rows(database, "SELECT * FROM xiuxian_impart ORDER BY rowid")
        text = await probe.command("传承祈愿", "传承祈愿 2")
        assert "历史" in text and "结晶" in text, text
        assert probe.rows(database, "SELECT * FROM xiuxian_impart ORDER BY rowid") == before
        assert not cards(probe) and not probe.rows(database, "SELECT * FROM impart_crystal_draw_operations")
        probe.evidence.update(reason="impart_rows_stone_num", historical_rows_unchanged=True, no_receipt=True)

    for command in ("传承祈愿", "传承抽卡", "传承分解"):
        for label, value in (("zero", "0"), ("negative", "-2"), ("format", "two"), ("extra", "2 extra")):
            def build(command, value):
                async def run(probe):
                    probe.impart(stone_num=100)
                    probe.player(stone=100_000_000)
                    name = next(iter(definitions()))
                    seed_card(probe, name, 4)
                    database = probe.context.paths.impart_db
                    before = probe.rows(database, "SELECT * FROM xiuxian_impart ORDER BY rowid")
                    argument = f"{name} {value}" if command == "传承分解" else value
                    text = await probe.command(command, command + " " + argument)
                    assert any(token in text for token in ("次数", "格式", "无效")), text
                    assert probe.rows(database, "SELECT * FROM xiuxian_impart ORDER BY rowid") == before
                    assert dict(cards(probe)) == {name: 4}
                    assert probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(100_000_000,)]
                    probe.evidence.update(inventory_unchanged=True, assets_unchanged=True)
                return run
            case("impart-" + command + "-reject-" + label, command, label,
                 "足够灵石/结晶/持卡，参数拒绝不能靠资源不足通过", "参数拒绝且所有资产不变",
                 group="impart-parameters", outcome="rejection")(build(command, value))
