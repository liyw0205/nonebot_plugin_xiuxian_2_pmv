import random
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, UserBuffDate
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to
from ..xiuxian_config import convert_rank

sql_message = XiuxianDateManage()
items = Items()

# 入世事件 - 侧重红尘历练、人际交往、宗门事务等
# 完整入世事件组
WORLDLY_EVENTS = {
    "reward": {
        "stone": {
            "descriptions": [
                ("帮助凡间城镇解决妖兽祸患，官府酬谢{}灵石", "base_amount + random.randint(500000, 2000000)"),
                ("在坊市捡漏买到珍稀材料，转手获利{}灵石", "base_amount + random.randint(1000000, 3000000)"),
                ("完成宗门任务，获得{}灵石奖励", "base_amount + random.randint(800000, 1500000)"),
                ("救治了一位受伤的富商，获赠{}灵石", "base_amount + random.randint(700000, 1800000)"),
                ("在拍卖会上慧眼识珠，低价购得宝物，价值{}灵石", "base_amount * 2 + random.randint(0, 1000000)"),
                ("在修真界相亲会上遇到知己，获得对方赠予的{}灵石", "base_amount + random.randint(500000, 2000000)"),
                ("与道侣结缘庆典，收得礼金{}灵石", "base_amount * 1.5 + random.randint(0, 1000000)"),
                ("在古墓探险中发现前辈修士遗留的{}灵石", "base_amount * 1.2 + random.randint(800000, 2000000)"),
                ("赢得修真界赌石大会，切出灵晶价值{}灵石", "base_amount * 3 + random.randint(0, 1000000)"),
                ("帮助商队击退劫修，获赠{}灵石酬谢", "base_amount + random.randint(600000, 1800000)"),
                ("出售自制符箓，获利{}灵石", "base_amount * 0.8 + random.randint(300000, 900000)"),
                ("在废弃洞府中发现{}灵石", "base_amount + random.randint(500000, 1500000)")
            ],
            "base_amount": 2000000
        },
        "exp": {
            "descriptions": [
                ("与宗门长老论道三日，修为精进{}", "base_percent + random.uniform(0.001, 0.003)"),
                ("在比武大会上力压群雄，心境突破，修为提升{}", "base_percent * 1.5 + random.uniform(0.002, 0.004)"),
                ("协助处理宗门事务，获得前辈指点，修为增加{}", "base_percent + random.uniform(0.001, 0.002)"),
                ("在藏书阁参悟前辈心得，领悟{}修为", "base_percent + random.uniform(0.0015, 0.0035)"),
                ("救治同门获得功德，修为自然增长{}", "base_percent * 0.8 + random.uniform(0.001, 0.002)"),
                ("与道侣双修悟道，修为增长{}", "base_percent * 2 + random.uniform(0.002, 0.005)"),
                ("为红颜知己炼制护身法宝，丹道感悟提升{}修为", "base_percent + random.uniform(0.0015, 0.003)"),
                ("参悟上古残碑，领悟失传功法，修为提升{}", "base_percent * 1.8 + random.uniform(0.003, 0.006)"),
                ("服用珍稀灵果，修为暴涨{}", "base_percent * 2.5 + random.uniform(0.004, 0.008)"),
                ("在灵眼泉中修炼三日，修为精进{}", "base_percent * 1.5 + random.uniform(0.002, 0.005)"),
                ("顿悟剑道真意，修为突破{}", "base_percent * 1.3 + random.uniform(0.002, 0.004)"),
                ("观摩元婴修士斗法，感悟良多，修为增加{}", "base_percent + random.uniform(0.001, 0.003)")
            ],
            "base_percent": 0.003
        },
        "item": {
            "descriptions": [
                ("完成宗门悬赏任务，奖励{}一件", None),
                ("在坊市淘宝，意外购得{}", None),
                ("帮助炼器长老打下手，获赠{}", None),
                ("救治同门修士，对方赠予{}作为谢礼", None),
                ("在宗门大比中获胜，奖品是{}", None),
                ("偶遇前世道侣，获赠定情信物{}", None),
                ("在姻缘树下寻得仙缘宝物{}", None),
                ("探索古修士遗迹，寻得{}", None),
                ("在秘境入口处偶得{}", None),
                ("击杀妖兽首领，掉落{}", None),
                ("完成宗门试炼，奖励{}", None),
                ("在拍卖行低价拍得稀世{}", None)
            ],
            "types": ["功法", "神通", "药材", "法器", "防具"],
            "rank_offset": 5
        },
        "points": {
            "descriptions": [
                ("解决宗门危机，获得{}成就点", "base_amount + random.randint(100, 300)"),
                ("在修真界闯出名号，声望增加{}点", "base_amount * 1.2 + random.randint(50, 200)"),
                ("完成一系列艰难任务，累计获得{}成就点", "base_amount + random.randint(150, 400)"),
                ("调解门派纠纷有功，被授予{}成就点", "base_amount * 0.8 + random.randint(80, 250)"),
                ("在修真集会上表现优异，赢得{}成就点", "base_amount + random.randint(120, 350)"),
                ("促成修真界良缘佳话，获得{}成就点", "base_amount * 1.5 + random.randint(150, 400)"),
                ("化解情劫因果，天道赐予{}成就点", "base_amount + random.randint(200, 500)"),
                ("解决修真界瘟疫危机，获得{}成就点", "base_amount * 2 + random.randint(200, 600)"),
                ("创立修真功法，流传千古获{}成就点", "base_amount * 3 + random.randint(300, 800)"),
                ("调解门派战争，避免生灵涂炭得{}成就点", "base_amount * 1.5 + random.randint(150, 450)"),
                ("修复上古传送阵，获{}成就点", "base_amount + random.randint(300, 700)"),
                ("在炼丹大会夺冠，赢得{}成就点", "base_amount * 1.2 + random.randint(180, 500)")
            ],
            "base_amount": 50
        }
    },
    "punish": {
        "stone": {
            "descriptions": [
                ("遭同门算计，被迫交出{}灵石平息事端", "base_amount + random.randint(200000, 800000)"),
                ("购买法宝时被奸商所骗，损失{}灵石", "base_amount + random.randint(300000, 1000000)"),
                ("宗门征税，上缴{}灵石", "base_amount * 0.7 + random.randint(0, 500000)"),
                ("被劫修盯上，抢走{}灵石", "base_amount + random.randint(400000, 1200000)"),
                ("投资商铺失败，亏损{}灵石", "base_amount * 0.8 + random.randint(100000, 600000)"),
                ("为情所困，在酒楼买醉花费{}灵石", "base_amount * 0.7 + random.randint(0, 500000)"),
                ("遭遇情劫，为化解劫数花费{}灵石", "base_amount + random.randint(400000, 800000)"),
                ("遭天降陨石砸中，为疗伤花费{}灵石", "base_amount * 1.5 + random.randint(300000, 900000)"),
                ("误入高阶阵法，为脱困消耗{}灵石", "base_amount + random.randint(400000, 800000)"),
                ("炼器失败炸毁洞府，修缮花费{}灵石", "base_amount * 2 + random.randint(0, 500000)"),
                ("被幻术所骗购买假货，损失{}灵石", "base_amount * 0.9 + random.randint(200000, 600000)"),
                ("资助邪修被发现，缴纳罚金{}灵石", "base_amount * 1.2 + random.randint(300000, 700000)")
            ],
            "base_amount": 500000
        },
        "exp": {
            "descriptions": [
                ("被心魔所困，道心受损，修为倒退{}", "base_percent + random.uniform(0.001, 0.003)"),
                ("与人斗法受伤，疗伤期间修为流失{}", "base_percent * 0.8 + random.uniform(0.0005, 0.002)"),
                ("修炼时被同门干扰，行功出错损失{}修为", "base_percent + random.uniform(0.001, 0.0025)"),
                ("卷入宗门纷争，耽误修炼，修为减退{}", "base_percent * 0.6 + random.uniform(0.001, 0.002)"),
                ("被长辈责罚面壁思过，修为停滞倒退{}", "base_percent * 0.5 + random.uniform(0.0005, 0.0015)"),
                ("情丝缠身，修为凝滞倒退{}", "base_percent * 1.2 + random.uniform(0.001, 0.003)"),
                ("为情破戒，受宗门处罚修为消散{}", "base_percent + random.uniform(0.0015, 0.0025)"),
                ("练功走火入魔，修为倒退{}", "base_percent * 1.8 + random.uniform(0.003, 0.006)"),
                ("误食毒草，修为消散{}", "base_percent * 1.2 + random.uniform(0.002, 0.004)"),
                ("遭天雷劈中，修为受损{}", "base_percent * 2 + random.uniform(0.004, 0.007)"),
                ("被魔气侵蚀，修为流失{}", "base_percent * 1.5 + random.uniform(0.003, 0.005)"),
                ("为救凡人强行逆转功法，修为损失{}", "base_percent * 1.3 + random.uniform(0.002, 0.004)")
            ],
            "base_percent": 0.002
        },
        "item": {
            "descriptions": [
                ("遭遇同门嫉妒，{}被暗中破坏", None),
                ("在坊市被偷，丢失{}", None),
                ("与人比斗赌约失败，交出{}", None),
                ("炼器失败，{}损毁", None),
                ("为平息事端，被迫献出{}", None),
                ("遭道侣背叛，定情信物{}被夺", None),
                ("为救红颜知己，献出本命法宝{}", None),
                ("遭遇空间风暴，{}被卷入虚空", None),
                ("炼丹炸炉，{}被毁", None),
                ("被大能斗法波及，{}被震碎", None),
                ("遭灵兽偷吃，{}丢失", None),
                ("在秘境中为保命舍弃{}", None)
            ]
        },
        "hp": {
            "descriptions": [
                ("与人斗法受伤，气血损失{}", "base_percent + random.uniform(0.05, 0.1)"),
                ("遭人暗算中毒，元气大伤损失{}气血", "base_percent * 1.2 + random.uniform(0.08, 0.15)"),
                ("修炼时被干扰，气血逆行损伤{}", "base_percent * 0.9 + random.uniform(0.06, 0.12)"),
                ("为救人强行运功，损耗{}气血", "base_percent + random.uniform(0.07, 0.13)"),
                ("执行危险任务受伤，损失{}气血", "base_percent * 1.1 + random.uniform(0.09, 0.16)"),
                ("情伤反噬，心神受损气血流失{}", "base_percent * 0.9 + random.uniform(0.06, 0.12)"),
                ("为情所困走火入魔，损伤{}气血", "base_percent * 1.5 + random.uniform(0.1, 0.2)"),
                ("遭遇魔修围攻，重伤损失{}气血", "base_percent * 1.8 + random.uniform(0.12, 0.25)"),
                ("探索毒沼，中毒损失{}气血", "base_percent * 1.3 + random.uniform(0.09, 0.18)"),
                ("强行突破境界失败，反噬损失{}气血", "base_percent * 2 + random.uniform(0.15, 0.3)"),
                ("为救灵兽被妖兽所伤，损失{}气血", "base_percent * 1.2 + random.uniform(0.08, 0.16)"),
                ("遭心魔反噬，心神受损损失{}气血", "base_percent * 1.5 + random.uniform(0.1, 0.2)")
            ],
            "base_percent": 0.15
        }
    }
}

# 完整出世事件组
TRANSCENDENT_EVENTS = {
    "reward": {
        "stone": {
            "descriptions": [
                ("在灵脉深处发现{}灵石矿", "base_amount * 1.5 + random.randint(1000000, 3000000)"),
                ("采集到珍稀灵药，售得{}灵石", "base_amount + random.randint(800000, 2000000)"),
                ("探索古修士洞府，找到{}灵石", "base_amount * 2 + random.randint(0, 1500000)"),
                ("灵兽报恩，衔来{}灵石", "base_amount * 0.8 + random.randint(300000, 1200000)"),
                ("在秘境中发现灵晶，价值{}灵石", "base_amount * 3 + random.randint(0, 1000000)"),
                ("救下灵狐仙子，获赠{}灵石", "base_amount * 0.8 + random.randint(300000, 1200000)"),
                ("解开前世情缘，得洞府遗产{}灵石", "base_amount * 2 + random.randint(0, 1500000)"),
                ("在火山口发现火灵石矿脉，收获{}灵石", "base_amount * 1.8 + random.randint(1200000, 2500000)"),
                ("救助受伤灵兽，获赠{}灵石", "base_amount * 0.7 + random.randint(400000, 1000000)"),
                ("探索海底秘境，寻得{}灵石", "base_amount * 2 + random.randint(0, 800000)"),
                ("在雷暴中心采集到雷灵石，价值{}灵石", "base_amount * 3 + random.randint(0, 500000)"),
                ("发现灵脉节点，采集{}灵石", "base_amount + random.randint(600000, 1400000)")
            ],
            "base_amount": 1000000
        },
        "exp": {
            "descriptions": [
                ("观瀑布三日，顿悟天地至理，修为提升{}", "base_percent * 2 + random.uniform(0.002, 0.005)"),
                ("月下独酌，心境通明，修为自然增长{}", "base_percent + random.uniform(0.0015, 0.0035)"),
                ("参悟上古石碑，领悟大道真意，修为精进{}", "base_percent * 1.8 + random.uniform(0.003, 0.006)"),
                ("与灵兽相伴修行，修为增加{}", "base_percent * 1.2 + random.uniform(0.001, 0.003)"),
                ("在灵泉中沐浴修炼，修为提升{}", "base_percent * 1.5 + random.uniform(0.002, 0.004)"),
                ("与仙侣同参阴阳大道，修为精进{}", "base_percent * 1.8 + random.uniform(0.003, 0.006)"),
                ("了却情劫因果，道心通明修为提升{}", "base_percent * 1.5 + random.uniform(0.002, 0.004)"),
                ("观星辰运转，悟周天星斗大阵，修为提升{}", "base_percent * 2.2 + random.uniform(0.004, 0.008)"),
                ("在极光下顿悟，修为暴涨{}", "base_percent * 3 + random.uniform(0.005, 0.01)"),
                ("与天地共鸣三日，修为精进{}", "base_percent * 1.6 + random.uniform(0.003, 0.006)"),
                ("参悟生死轮回，道行增加{}", "base_percent * 1.4 + random.uniform(0.002, 0.005)"),
                ("在混沌之地边缘修炼，修为提升{}", "base_percent * 2 + random.uniform(0.004, 0.007)")
            ],
            "base_percent": 0.003
        },
        "item": {
            "descriptions": [
                ("在秘境深处发现{}", None),
                ("灵兽指引，找到前辈遗留的{}", None),
                ("参悟古碑时，{}从天而降", None),
                ("清理洞府时发现尘封的{}", None),
                ("帮助山灵解决困难，获赠{}", None),
                ("月老祠中求得姻缘法宝{}", None),
                ("前世道侣转世，归还信物{}", None),
                ("在云海之巅发现{}", None),
                ("灵脉核心孕育出{}", None),
                ("古树精魂赠予{}", None),
                ("雷劫过后天降{}", None),
                ("破解上古机关，获得{}", None)
            ],
            "types": ["功法", "神通", "药材", "法器", "防具"],
            "rank_offset": 6
        },
        "points": {
            "descriptions": [
                ("解开上古禁制，获得{}成就点", "base_amount * 1.5 + random.randint(150, 400)"),
                ("完成先贤考验，被授予{}成就点", "base_amount * 2 + random.randint(100, 300)"),
                ("修复古阵法，天道赐予{}成就点", "base_amount + random.randint(200, 500)"),
                ("参悟天地法则，明悟{}成就点", "base_amount * 1.2 + random.randint(120, 350)"),
                ("帮助山灵平衡地脉，获得{}成就点", "base_amount * 0.9 + random.randint(180, 450)"),
                ("参悟情之大道，明悟{}成就点", "base_amount * 1.2 + random.randint(120, 350)"),
                ("促成仙凡姻缘，获天道赐予{}成就点", "base_amount + random.randint(200, 500)"),
                ("修复天地灵脉，天道赐予{}成就点", "base_amount * 2.5 + random.randint(300, 700)"),
                ("参透大道真谛，得{}成就点", "base_amount * 3 + random.randint(400, 800)"),
                ("阻止域外天魔入侵，获{}成就点", "base_amount * 4 + random.randint(500, 1000)"),
                ("补全天道法则，得{}成就点", "base_amount * 2 + random.randint(250, 600)"),
                ("教化山野精怪，功德无量获{}成就点", "base_amount * 1.5 + random.randint(200, 500)")
            ],
            "base_amount": 50
        }
    },
    "punish": {
        "stone": {
            "descriptions": [
                ("遭遇空间裂缝，随身携带的{}灵石被卷入虚空", "base_amount + random.randint(300000, 900000)"),
                ("灵兽捣乱，丢失{}灵石", "base_amount * 0.7 + random.randint(200000, 600000)"),
                ("误触禁制，为脱困消耗{}灵石", "base_amount + random.randint(400000, 800000)"),
                ("采集灵药失败，损失{}灵石材料", "base_amount * 0.6 + random.randint(100000, 500000)"),
                ("阵法反噬，需{}灵石修复损伤", "base_amount * 0.9 + random.randint(300000, 700000)"),
                ("为救前世道侣转世，耗费{}灵石", "base_amount + random.randint(400000, 800000)"),
                ("情劫难渡，散财消灾花费{}灵石", "base_amount * 0.6 + random.randint(100000, 500000)"),
                ("遭遇时空乱流，丢失{}灵石", "base_amount * 1.8 + random.randint(400000, 900000)"),
                ("被灵脉反噬，消耗{}灵石疗伤", "base_amount + random.randint(500000, 1000000)"),
                ("误触上古禁制，为脱困耗费{}灵石", "base_amount * 2 + random.randint(0, 600000)"),
                ("遭天劫余波冲击，损失{}灵石", "base_amount * 1.5 + random.randint(300000, 800000)"),
                ("为镇压魔气泄露消耗{}灵石", "base_amount * 1.2 + random.randint(300000, 700000)")
            ],
            "base_amount": 500000
        },
        "exp": {
            "descriptions": [
                ("强行参悟高阶功法，道基受损修为倒退{}", "base_percent * 1.5 + random.uniform(0.002, 0.004)"),
                ("遭遇心魔劫，修为跌落{}", "base_percent * 2 + random.uniform(0.003, 0.006)"),
                ("误入时间乱流，损失{}修为", "base_percent + random.uniform(0.0015, 0.0035)"),
                ("被古修士残念冲击，修为消散{}", "base_percent * 1.2 + random.uniform(0.002, 0.005)"),
                ("强行突破失败，修为反噬损失{}", "base_percent * 1.8 + random.uniform(0.004, 0.007)"),
                ("情丝缠身，道心蒙尘修为消散{}", "base_percent * 1.2 + random.uniform(0.002, 0.005)"),
                ("为情所困，修行出错损失{}修为", "base_percent + random.uniform(0.0015, 0.0035)"),
                ("遭域外天魔入侵识海，修为倒退{}", "base_percent * 2.5 + random.uniform(0.005, 0.01)"),
                ("被混沌之气侵蚀，修为消散{}", "base_percent * 1.8 + random.uniform(0.004, 0.008)"),
                ("强闯秘境遭法则反噬，修为损失{}", "base_percent * 2.2 + random.uniform(0.005, 0.009)"),
                ("观天地大劫心神受创，修为倒退{}", "base_percent * 1.5 + random.uniform(0.003, 0.006)"),
                ("被远古诅咒缠身，修为流失{}", "base_percent * 1.3 + random.uniform(0.002, 0.005)")
            ],
            "base_percent": 0.002
        },
        "item": {
            "descriptions": [
                ("探索险地时，{}被空间裂缝吞噬", None),
                ("炼丹失败，{}化为灰烬", None),
                ("遭遇天劫余波，{}被雷击毁", None),
                ("被古禁制锁定，被迫舍弃{}", None),
                ("灵兽顽皮，{}不知去向", None),
                ("前世情债未了，{}被因果之力抹去", None),
                ("为救道侣，献出本命法宝{}", None),
                ("遭天地法则排斥，{}被湮灭", None),
                ("被混沌风暴席卷，{}遗失", None),
                ("为镇压邪魔牺牲{}", None),
                ("参悟天道失败，{}化为飞灰", None),
                ("遭遇位面裂缝，{}被吞噬", None)
            ]
        },
        "hp": {
            "descriptions": [
                ("遭遇妖兽袭击，重伤损失{}气血", "base_percent * 1.5 + random.uniform(0.1, 0.2)"),
                ("误入毒瘴，元气损伤{}", "base_percent + random.uniform(0.08, 0.16)"),
                ("强行破阵遭到反噬，气血流失{}", "base_percent * 1.8 + random.uniform(0.12, 0.25)"),
                ("被古修士残念所伤，损失{}气血", "base_percent * 1.3 + random.uniform(0.09, 0.18)"),
                ("天劫余波冲击，损伤{}气血", "base_percent * 2 + random.uniform(0.15, 0.3)"),
                ("情劫反噬，心神受损气血流失{}", "base_percent * 1.3 + random.uniform(0.09, 0.18)"),
                ("为情所困走火入魔，损伤{}气血", "base_percent * 2 + random.uniform(0.15, 0.3)"),
                ("强渡天劫，重伤损失{}气血", "base_percent * 3 + random.uniform(0.2, 0.4)"),
                ("被域外天魔所伤，魔气侵蚀损失{}气血", "base_percent * 2 + random.uniform(0.15, 0.3)"),
                ("探索混沌之地受创，损失{}气血", "base_percent * 2.5 + random.uniform(0.18, 0.35)"),
                ("遭天道反噬，气血流失{}", "base_percent * 1.8 + random.uniform(0.12, 0.25)"),
                ("被远古凶兽所伤，损失{}气血", "base_percent * 1.5 + random.uniform(0.1, 0.2)")
            ],
            "base_percent": 0.15
        }
    }
}


# 无事发生事件
NOTHING_EVENTS = {
    "worldly": [
        "在坊市闲逛一日，感受人间烟火",
        "参加修真集会，与各路修士交流",
        "在酒楼听了一天修真界八卦",
        "观摩宗门大比，受益匪浅",
        "帮助凡人解决小麻烦，心境平和",
        "在坊市听了一天说书人讲修真界趣闻",
        "参加炼丹师交流会，增长见闻",
        "观摩炼器大师锻造法宝",
        "在修真学院讲授基础功法",
        "调解修士间的纠纷",
        "品尝各地灵食佳肴",
        "参加诗词大会，与文人修士切磋",
        "在藏书阁翻阅古籍"
    ],
    "transcendent": [
        "静坐山巅观云海三日",
        "在瀑布下冥想修炼",
        "与灵兽相伴游历山水",
        "整理洞府，清扫尘埃",
        "观察星象，感悟天道",
        "在云海中打坐三日",
        "观察灵兽生活习性",
        "采集朝露炼制灵液",
        "聆听山间自然之音",
        "参悟石壁天然纹路",
        "与古树精魂交流",
        "在月光下练习剑法",
        "研究星象变化规律"
    ]
}

class TrainingEvents:
    def __init__(self):
        self.event_style = None  # 当前事件风格
    
    def handle_event(self, user_id, user_info, event_type):
        """处理历练事件"""
        # 随机选择事件风格 (入世:60% 出世:40%)
        self.event_style = random.choices(
            ["worldly", "transcendent"],
            weights=[60, 40]
        )[0]
        
        if "plus" in event_type:  # 奖励事件
            return self._handle_reward(user_id, user_info, event_type)
        elif "minus" in event_type:  # 惩罚事件
            return self._handle_punish(user_id, user_info, event_type)
        else:  # 无事发生
            return self._handle_nothing()
    
    def _handle_reward(self, user_id, user_info, event_type):
        """处理奖励事件"""
        user_buff_data = UserBuffDate(user_info['user_id'])
        sub_buff_data = user_buff_data.get_user_sub_buff_data()
        sub_buff_integral_buff = sub_buff_data.get('integral', 0) if sub_buff_data is not None else 0
        sub_buff_stone_buff = sub_buff_data.get('stone', 0) if sub_buff_data is not None else 0
        is_big_reward = "2" in event_type
        events_pool = WORLDLY_EVENTS if self.event_style == "worldly" else TRANSCENDENT_EVENTS
        reward_types = list(events_pool["reward"].keys())
        
        # 根据事件大小调整权重
        weights = {
            "stone": 60 if is_big_reward else 55,
            "exp": 10 if is_big_reward else 5,
            "item": 15 if is_big_reward else 20,
            "points": 15 if is_big_reward else 20
        }
        weights = [weights[t] for t in reward_types]
        
        # 随机选择奖励类型
        reward_type = random.choices(reward_types, weights=weights)[0]
        reward_data = events_pool["reward"][reward_type]
        desc_template, calc_rule = random.choice(reward_data["descriptions"])
        # 处理不同类型的奖励
        if reward_type == "stone":
            locals_dict = {"base_amount": reward_data["base_amount"], "random": random}
            amount = eval(calc_rule, {}, locals_dict)
            amount = int(amount * (1 + sub_buff_stone_buff))
            sql_message.update_ls(user_id, amount, 1)
            return {
                "message": desc_template.format(number_to(amount)),
                "type": "stone",
                "amount": amount
            }
        
        elif reward_type == "exp":
            locals_dict = {"base_percent": reward_data["base_percent"], "random": random}
            percent = eval(calc_rule, {}, locals_dict)
            user_rank = max(convert_rank(user_info['level'])[0] // 3, 1)
            exp = int(user_info["exp"] * percent * min(0.1 * user_rank, 1))
            sql_message.update_exp(user_id, exp)
            return {
                "message": desc_template.format(number_to(exp)),
                "type": "exp",
                "amount": exp
            }
        
        elif reward_type == "item":
            item_type = random.choice(reward_data["types"])
            if item_type in ["法器", "防具", "辅修功法"]:
                base_rank = 16
            else:
                base_rank = random.randint(5, 50) - reward_data["rank_offset"]
            zx_rank = random.randint(base_rank, base_rank + 20)
            zx_rank = min(zx_rank, 55)
            if zx_rank == 5 and random.randint(1, 100) != 100:
                zx_rank = 10
            item_id_list = items.get_random_id_list_by_rank_and_item_type(zx_rank, item_type)
            
            if item_id_list:
                item_id = random.choice(item_id_list)
                item_info = items.get_data_by_item_id(item_id)
                sql_message.send_back(user_id, item_id, item_info["name"], item_info["type"], 1)
                return {
                    "message": desc_template.format(f"{item_info['name']}"),
                    "type": "item",
                    "item_id": item_id,
                    "item_name": item_info["name"]
                }
            else:
                amount = 1000000
                sql_message.update_ls(user_id, amount, 1)
                return {
                    "message": f"探索有所收获，但没找到合适物品，获得{number_to(amount)}灵石",
                    "type": "stone",
                    "amount": amount
                }
        
        else:  # points
            locals_dict = {"base_amount": reward_data["base_amount"], "random": random}
            amount = eval(calc_rule, {}, locals_dict)
            amount = int(amount * (1 + sub_buff_integral_buff))
            return {
                "message": desc_template.format(amount),
                "type": "points",
                "amount": amount
            }
    
    def _handle_punish(self, user_id, user_info, event_type):
        """处理惩罚事件"""
        is_big_punish = "2" in event_type
        events_pool = WORLDLY_EVENTS if self.event_style == "worldly" else TRANSCENDENT_EVENTS
        punish_types = list(events_pool["punish"].keys())
        
        # 根据事件大小调整权重
        weights = {
            "stone": 30 if is_big_punish else 35,
            "exp": 10 if is_big_punish else 5,
            "item": 20 if is_big_punish else 20,
            "hp": 40 if is_big_punish else 40
        }
        weights = [weights[t] for t in punish_types]
        
        # 随机选择惩罚类型
        punish_type = random.choices(punish_types, weights=weights)[0]
        punish_data = events_pool["punish"][punish_type]
        
        # 处理物品惩罚的特殊情况
        if punish_type == "item":
            desc_template, _ = random.choice(punish_data["descriptions"])
            back_msg = sql_message.get_back_msg(user_id)
            
            if not back_msg:
                amount = 5000000
                sql_message.update_ls(user_id, amount, 2)
                return {
                    "message": f"探索遭遇意外，损失{number_to(amount)}灵石",
                    "type": "stone",
                    "amount": -amount
                }
            
            # 获取奖励事件中的物品类型列表
            reward_data = events_pool["reward"]["item"]
            item_types = random.choice(reward_data["types"])
            
            # 获取用户当前装备
            user_buff_info = UserBuffDate(user_id).BuffInfo
            equipped_items = {
                '法器': user_buff_info['faqi_buff'],
                '防具': user_buff_info['armor_buff']
            }
            
            # 优先匹配相同物品类型且未装备的物品
            same_type_items = []
            for item in back_msg:
                item_data = items.get_data_by_item_id(item["goods_id"])
                if (item_data.get("type") in item_types and 
                    item["goods_num"] > 0 and
                    not (item_data.get("type") == "装备" and 
                         item["goods_id"] in [equipped_items[item_data.get("item_type")]])):
                    same_type_items.append(item)
            
            if same_type_items:
                # 进一步筛选符合rank要求的物品
                user_rank = convert_rank(user_info["level"])[0]
                min_rank = max(user_rank - 22 - reward_data["rank_offset"], 26)
                
                valid_items = []
                for item in same_type_items:
                    item_data = items.get_data_by_item_id(item["goods_id"])
                    item_rank = item_data["rank"]
                    if item_rank >= min_rank:
                        valid_items.append(item)
                
                if valid_items:
                    item = random.choice(valid_items)
                    sql_message.update_back_j(user_id, item["goods_id"], 1)
                    return {
                        "message": desc_template.format(item["goods_name"]),
                        "type": "item",
                        "item_id": item["goods_id"],
                        "item_name": item["goods_name"],
                        "lost": True
                    }
                        
            # 没有则扣灵石
            amount = 5000000
            sql_message.update_ls(user_id, amount, 2)
            return {
                "message": f"遭遇意外，损失{number_to(amount)}灵石",
                "type": "stone",
                "amount": -amount
            }
        
        else:
            desc_template, calc_rule = random.choice(punish_data["descriptions"])
            
            if punish_type == "stone":
                locals_dict = {"base_amount": punish_data["base_amount"], "random": random}
                amount = eval(calc_rule, {}, locals_dict)
                sql_message.update_ls(user_id, amount, 2)
                return {
                    "message": desc_template.format(number_to(amount)),
                    "type": "stone",
                    "amount": -amount
                }
            
            elif punish_type == "exp":
                locals_dict = {"base_percent": punish_data["base_percent"], "random": random}
                percent = eval(calc_rule, {}, locals_dict)
                user_rank = convert_rank(user_info['level'])[0]
                exp = int(user_info["exp"] * percent * min(0.1 * user_rank, 1))
                sql_message.update_j_exp(user_id, exp)
                return {
                    "message": desc_template.format(number_to(exp)),
                    "type": "exp",
                    "amount": -exp
                }
            
            else:  # hp
                locals_dict = {"base_percent": punish_data["base_percent"], "random": random}
                percent = eval(calc_rule, {}, locals_dict)
                hp_loss = int(user_info["hp"] * percent)
                sql_message.update_user_hp_mp(user_id, user_info["hp"] - hp_loss, user_info["mp"])
                return {
                    "message": desc_template.format(number_to(hp_loss)),
                    "type": "hp",
                    "amount": -hp_loss
                }
    
    def _handle_nothing(self):
        """处理无事发生"""
        pool = NOTHING_EVENTS[self.event_style]
        desc = random.choice(pool)
        return {
            "message": f"{desc}，心境略有提升",
            "type": "nothing"
        }

training_events = TrainingEvents()
