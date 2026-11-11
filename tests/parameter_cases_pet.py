"""Pet parameters use real content and persisted owned UIDs."""
def system():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import pet_system
    return pet_system


def seed_pet(probe, uid="900001", *, bag_count=1):
    pets = system()
    template = next(iter(pets.load_pet_pool().values()))
    active = pets.create_pet_instance(template)
    active.update(stars=1, exp=0, total_exp=0)
    pets.grant_pet_as_active(uid, active)
    for _ in range(bag_count):
        pets.grant_pet(uid, pets.create_pet_instance(template))
    return pets.get_pet_doc(uid)


def feed_item(probe):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
    item_id = int(next(iter(Items().get_data_by_item_type("药材"))))
    return item_id, probe.grant_item(item_id, 10)


def register(case):
    @case("pet-travel-return-restart", "宠物游历", "真实场景 1小时 → 到期领取", "合法主宠、派遣已落库、重建应用模拟重启",
          "到期奖励与游历清除同事务落库，重复领取不重复发奖", group="pet-recovery")
    async def travel_return(probe):
        from unittest.mock import patch
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_pet as m
        from nonebot_plugin_xiuxian_2.features.pet.application import PetApplication
        seed_pet(probe)
        pets = system()
        scene = next(iter(pets.get_pet_travel_scenes()))
        await probe.command("宠物游历", f"宠物游历 {scene} 1")
        pending = pets.get_pet_doc("900001")
        pending["travel"]["start_at"] -= 7200
        pending["travel"]["end_at"] -= 7200
        pets.save_pet_doc("900001", pending)
        from tests.parameter_cases_assets import backpack
        before = backpack(probe)
        app = PetApplication(probe.context.paths.game_db, player_database=probe.context.paths.player_db)
        with patch.object(m, "pet_application", app):
            text = await probe.command("领取宠物游历", number=940010)
        after = pets.get_pet_doc("900001")
        assert "游历归来" in text and not after.get("travel"), text
        saved = backpack(probe)
        assert saved != before, (text, saved)
        await probe.command("领取宠物游历", number=940010)
        assert backpack(probe) == saved and pets.get_pet_doc("900001") == after
        probe.evidence.update(scene=scene, expired_persisted_context=True, rewards= saved, repeat_unchanged=True)

    @case("pet-hatch-count-replay", "砸蛋", "合法批量数量2", "足够灵石、空宠物栏、真实宠物池",
          "孵化2只真实宠物，扣2次费用，重放原宠物且资产不变", group="pet-parameters")
    async def hatch(probe):
        pets = system()
        probe.player(stone=10_000_000)
        text = await probe.command("砸蛋", "砸蛋 2", number=940001)
        doc = pets.get_pet_doc("900001")
        assert doc["active"] and len(doc["bag"]) == 1, (text, doc)
        assert probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(10_000_000 - 2 * pets.EGG_COST,)]
        await probe.command("砸蛋", "砸蛋 2", number=940001)
        assert pets.get_pet_doc("900001") == doc
        probe.evidence.update(pet_uids=[doc["active"]["uid"], doc["bag"][0]["uid"]], spent=2 * pets.EGG_COST, frozen_replay=True)

    for command in ("查看宠物", "宠物背包"):
        def build(command):
            async def run(probe):
                doc = seed_pet(probe)
                uid = doc["bag"][0]["uid"]
                args = uid if command == "查看宠物" else uid + " 1"
                text = await probe.command(command, command + " " + args)
                assert uid in text, text
                assert system().get_pet_doc("900001") == doc
                probe.evidence.update(owned_uid=uid, state_unchanged=True)
            return run
        case("pet-read-" + command, command, "真实自有UID" + (" 页码" if command == "宠物背包" else ""),
             "真实主宠和背包宠物", "展示指定宠物，状态不变", group="pet-parameters")(build(command))

    @case("pet-switch-owned-uid", "出战宠物", "背包自有UID", "主宠和背包中另一只宠物",
          "背包宠物切换出战，原主宠回背包，持久回执且重放不变", group="pet-parameters")
    async def switch(probe):
        before = seed_pet(probe)
        uid = before["bag"][0]["uid"]
        text = await probe.command("出战宠物", "出战宠物 " + uid, number=940002)
        after = system().get_pet_doc("900001")
        assert after["active"]["uid"] == uid and any(pet["uid"] == before["active"]["uid"] for pet in after["bag"]), text
        await probe.command("出战宠物", "出战宠物 " + uid, number=940002)
        assert system().get_pet_doc("900001") == after
        probe.evidence.update(prior_active=before["active"]["uid"], active=uid, replay_unchanged=True)

    @case("pet-feed-name-count", "宠物喂食", "内容中的合法药材名 数量2", "未满星主宠与10份适用药材",
          "精确消耗2份材料，主宠总经验增加并持久化，重复不消耗", group="pet-parameters")
    async def feed(probe):
        before = seed_pet(probe)
        item_id, name = feed_item(probe)
        text = await probe.command("宠物喂食", f"宠物喂食 {name} 2", number=940003)
        after = system().get_pet_doc("900001")
        assert probe.managers[0].goods_num("900001", item_id) == 8, text
        assert after["active"]["total_exp"] > before["active"]["total_exp"], text
        await probe.command("宠物喂食", f"宠物喂食 {name} 2", number=940003)
        assert probe.managers[0].goods_num("900001", item_id) == 8 and system().get_pet_doc("900001") == after
        probe.evidence.update(item_id=item_id, before_exp=before["active"]["total_exp"], after_exp=after["active"]["total_exp"], replay_unchanged=True)

    @case("pet-release-owned-uid", "放生宠物", "背包自有UID", "主宠与背包宠物，背包有返还空间",
          "只移除选定宠物，按真实返还规则增加材料，主宠保留", group="pet-parameters")
    async def release(probe):
        before = seed_pet(probe)
        target = before["bag"][0]
        pets = system()
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items
        definition = Items().get_data_by_item_id(pets.PET_RELEASE_REFUND_ITEM_ID)
        refund = pets.calc_pet_release_refund(target, definition)[0]
        text = await probe.command("放生宠物", "放生宠物 " + target["uid"])
        after = pets.get_pet_doc("900001")
        assert not after["bag"] and after["active"]["uid"] == before["active"]["uid"], text
        assert probe.managers[0].goods_num("900001", pets.PET_RELEASE_REFUND_ITEM_ID) == refund, text
        probe.evidence.update(released=target["uid"], refund_item=pets.PET_RELEASE_REFUND_ITEM_ID, refund=refund)

    @case("pet-release-keyword", "一键放生", "内容中的实际稀有度", "同稀有度主宠和2只背包宠物",
          "匹配两只背包宠物批量放生，跳过出战宠物", group="pet-parameters")
    async def release_batch(probe):
        before = seed_pet(probe, bag_count=2)
        keyword = before["active"]["rarity"]
        text = await probe.command("一键放生", "一键放生 " + keyword)
        after = system().get_pet_doc("900001")
        assert not after["bag"] and after["active"] == before["active"], text
        probe.evidence.update(keyword=keyword, removed_uids=[pet["uid"] for pet in before["bag"]], active_retained=True)

    @case("pet-reroll-owned-uid", "宠物启明", "自有宠物UID", "真实宠物与3枚启明石",
          "消耗1枚启明石，将合法新技能与业务回执持久化", group="pet-parameters")
    async def reroll(probe):
        before = seed_pet(probe)
        pets = system()
        probe.grant_item(pets.QIMING_STONE_ID, 3)
        uid = before["active"]["uid"]
        text = await probe.command("宠物启明", "宠物启明 " + uid)
        after = pets.get_pet_doc("900001")
        assert "启明成功" in text and probe.managers[0].goods_num("900001", pets.QIMING_STONE_ID) == 2, text
        assert any(after["active"]["skill"]["skill_id"] in group for group in pets.load_pet_skill_pool().values()), after["active"]
        probe.evidence.update(uid=uid, skill=after["active"]["skill"]["skill_id"], consumed=1)

    @case("pet-travel-scene-duration", "宠物游历", "实际场景名称 小时数2", "合法出战宠物，没有进行中游历",
          "真实游历上下文持久化；忙碌时再次派遣明确拒绝且不覆盖", group="pet-parameters")
    async def travel(probe):
        before = seed_pet(probe)
        pets = system()
        scene = next(iter(pets.get_pet_travel_scenes()))
        text = await probe.command("宠物游历", "宠物游历 " + scene + " 2")
        after = pets.get_pet_doc("900001")
        journey = after["travel"]
        assert journey and journey["scene"] == scene and journey["duration_hours"] == 2, (text, journey)
        assert journey["pet_uid"] == before["active"]["uid"]
        text = await probe.command("宠物游历", "宠物游历 " + scene + " 2")
        assert "已有" in text and pets.get_pet_doc("900001") == after, text
        probe.evidence.update(scene=scene, duration=2, persisted=journey, busy_unchanged=True)

    @case("pet-fusion-owned-materials", "宠物融合", "实际自有同名材料UID组合", "主宠四星满经验，按领域规则准备足够同名本体",
          "主宠突破五星、精确消耗指定材料、回执持久化", group="pet-parameters")
    async def fusion(probe):
        pets = system()
        before = seed_pet(probe, bag_count=0)
        before["active"].update(stars=4, exp=pets.exp_to_next_star(4))
        need = pets.fusion_need(4)
        template = pets.load_pet_pool()[before["active"]["pet_id"]]
        before["bag"] = [pets.create_pet_instance(template) for _ in range(need)]
        pets.save_pet_doc("900001", before)
        text = await probe.command("宠物融合", "宠物融合 " + " ".join(pet["uid"] for pet in before["bag"]))
        after = pets.get_pet_doc("900001")
        assert after["active"]["stars"] == 5 and not after["bag"], (text, after)
        probe.evidence.update(materials=need, stars=5)

    for command in ("查看宠物", "出战宠物", "放生宠物", "宠物启明"):
        def build(command):
            async def run(probe):
                before = seed_pet(probe)
                other = seed_pet(probe, "900002")
                uid = other["active"]["uid"]
                text = await probe.command(command, command + " " + uid)
                assert any(token in text for token in ("未找到", "不存在")), text
                assert system().get_pet_doc("900001") == before and system().get_pet_doc("900002") == other
                probe.evidence.update(other_owner_uid=uid, both_states_unchanged=True)
            return run
        case("pet-owner-" + command, command, "另一虚构玩家的真实UID", "双方均有真实宠物",
             "所有权拒绝且双方宠物不变", group="pet-parameters", outcome="rejection")(build(command))

    for label, value in (("zero", "0"), ("negative", "-2"), ("format", "abc"), ("extra", "2 extra"), ("huge", "9" * 350)):
        def build(value):
            async def run(probe):
                before = seed_pet(probe)
                text = await probe.command("砸蛋", "砸蛋 " + value)
                assert any(token in text for token in ("范围", "用法")), text
                assert system().get_pet_doc("900001") == before
                assert probe.rows(probe.context.paths.game_db, "SELECT stone FROM user_xiuxian WHERE user_id='900001'") == [(1_000_000,)]
                probe.evidence.update(assets_unchanged=True, pets_unchanged=True)
            return run
        case("pet-hatch-reject-" + label, "砸蛋", label, "真实宠物与足够单次费用",
             "数量拒绝且不扣费、不生成宠物", group="pet-parameters", outcome="rejection")(build(value))

    for label, value in (("zero", "0"), ("negative", "-2"), ("extra", "2 extra"), ("huge", "9" * 350)):
        def build(value):
            async def run(probe):
                before = seed_pet(probe)
                item_id, name = feed_item(probe)
                text = await probe.command("宠物喂食", f"宠物喂食 {name} {value}")
                assert probe.managers[0].goods_num("900001", item_id) == 10 and system().get_pet_doc("900001") == before, text
                probe.evidence.update(item_id=item_id, inventory_unchanged=True, pets_unchanged=True)
            return run
        case("pet-feed-reject-" + label, "宠物喂食", "合法材料 " + label, "可喂食主宠与真实材料库存",
             "参数或库存拒绝，宠物与材料均不变", group="pet-parameters", outcome="rejection")(build(value))

    @case("pet-travel-extra-argument", "宠物游历", "实际场景 合法小时数 多余参数", "可派遣主宠，无正在游历",
          "格式拒绝且不创建游历上下文", group="pet-parameters", outcome="rejection")
    async def extra_travel(probe):
        before = seed_pet(probe)
        scene = next(iter(system().get_pet_travel_scenes()))
        text = await probe.command("宠物游历", f"宠物游历 {scene} 2 extra")
        assert system().get_pet_doc("900001") == before, text
        assert "格式" in text or "用法" in text, text
        probe.evidence.update(no_journey_created=True)
