import random
import json
import os
from pathlib import Path
from datetime import datetime

# 幻境问题
DEFAULT_QUESTIONS = [
    {
        "question": "你在修炼时遇到瓶颈，你会",
        "options": [
            "闭关苦修，不突破不出关",
            "外出游历，寻找机缘",
            "请教前辈，寻求指点"
        ],
        "explanations": [
            "专注苦修的道路，这体现了你坚韧不拔的道心。这种选择让你在修炼中更加专注，能够深入挖掘自身的潜力，但也可能因为过于封闭而错过外界的机遇。",
            "探索世界的道路，这体现了你开放进取的心态。这种选择让你能够接触更多的机缘和感悟，拓宽视野，但也可能因为分散精力而影响修炼进度。",
            "虚心求教的道路，这体现了你谦逊好学的品质。这种选择让你能够快速吸收前人的经验，少走弯路，但也可能过度依赖他人而缺乏独立思考。"
        ]
    },
    {
        "question": "面对强大的敌人，你会",
        "options": [
            "正面迎战，绝不退缩",
            "智取为上，寻找弱点",
            "暂时退避，提升实力后再战"
        ],
        "explanations": [
            "勇往直前的道路，这体现了你无畏的勇气和坚定的道心。这种选择让你在战斗中锤炼意志，但也可能因为鲁莽而陷入危险。",
            "以智取胜的道路，这体现了你灵活变通的智慧。这种选择让你能够以最小的代价获得胜利，但也可能因为过度算计而错失良机。",
            "审时度势的道路，这体现了你谨慎务实的态度。这种选择让你能够保全实力，等待最佳时机，但也可能因为犹豫不决而失去战机。"
        ]
    },
    {
        "question": "修炼最重要的是",
        "options": [
            "坚定的道心",
            "强大的功法",
            "丰富的资源"
        ],
        "explanations": [
            "重视内心的坚定，认为道心是修炼的根基。这让你在漫长修行路上不易迷失，但可能忽视外在条件的积累。",
            "追求强大的功法，相信方法得当能事半功倍。这让你进步迅速，但可能忽略心性的磨炼。",
            "注重资源的获取，认为充足的外物支持是修炼的保障。这让你修行条件优越，但可能形成对外物的依赖。"
        ]
    },
    {
        "question": "你如何看待因果",
        "options": [
            "种因得果，必须谨慎",
            "随心而行，不问因果",
            "因果循环，自有定数"
        ],
        "explanations": [
            "相信因果必报，因此行事谨慎，注重积累善缘。这让你少招祸端，但也可能束缚你的选择。",
            "率性而为，不拘泥于因果束缚。这让你活得洒脱，但可能引来不可预知的后果。",
            "相信因果自有轮回，顺其自然。这让你心境平和，但可能缺乏主动改变命运的意愿。"
        ]
    },
    {
        "question": "你追求的是",
        "options": [
            "无上大道",
            "逍遥自在",
            "守护重要之人"
        ],
        "explanations": [
            "志在攀登修炼巅峰，追求超越一切的真理。这让你目标明确，但可能牺牲生活乐趣。",
            "向往自由无拘的生活，不愿被规则束缚。这让你心境开阔，但可能在关键时刻缺少担当。",
            "把守护亲友作为修行的意义。这让你充满温情与责任感，但可能因情感牵绊影响决断。"
        ]
    },
    {
        "question": "发现秘境时，你会",
        "options": [
            "立即探索，机缘稍纵即逝",
            "做好准备再进入",
            "邀请同伴一同前往"
        ],
        "explanations": [
            "行动果断，善于抓住机会。这让你常能占得先机，但风险也更高。",
            "谨慎周密，必先筹划周全。这让你更安全，但可能错失最佳时机。",
            "重视团队合作，愿意分享机缘。这让你收获更多助力，但也可能因意见不一而降低效率。"
        ]
    },
    {
        "question": "对于仇敌，你的态度是",
        "options": [
            "斩草除根，不留后患",
            "小惩大诫，点到为止",
            "冤冤相报何时了，化解恩怨"
        ],
        "explanations": [
            "决绝狠辣，确保自身安全。这让你减少威胁，但可能树敌更多。",
            "懂得分寸，既立威又留余地。这让你维持平衡，但可能留下隐患。",
            "心胸宽广，愿化干戈为玉帛。这让你赢得人心，但可能被利用。"
        ]
    },
    {
        "question": "修炼遇到心魔，你会",
        "options": [
            "直面心魔，战胜它",
            "寻求静心之法化解",
            "暂时停止修炼调整心态"
        ],
        "explanations": [
            "勇敢坚毅，敢于直视内心黑暗。这让你成长迅速，但过程可能痛苦。",
            "善于用方法平复心绪，化解障碍。这让你平稳前进，但可能回避深层问题。",
            "懂得休息与调整，不急于求成。这让你恢复状态，但可能拖延进度。"
        ]
    },
    {
        "question": "你更倾向于",
        "options": [
            "独自修炼",
            "与志同道合者一起",
            "建立自己的势力"
        ],
        "explanations": [
            "喜欢独行，追求自我突破。这让你心无旁骛，但可能缺少外援。",
            "珍视同修之情，互相促进。这让你进步稳定，但可能受他人影响。",
            "有领袖之志，愿聚人成事。这让你影响力大，但管理压力也高。"
        ]
    },
    {
        "question": "面对天劫，你的准备是",
        "options": [
            "依靠自身实力硬抗",
            "准备大量防御法宝",
            "寻找特殊地点渡劫"
        ],
        "explanations": [
            "自信实力足以应对一切。这让你展现强大，但风险极高。",
            "未雨绸缪，用外物护身。这让你更安全，但可能过于依赖道具。",
            "善于借天地之势，选择合适环境。这让你渡劫顺利，但找到理想地点不易。"
        ]
    },
    {
        "question": "你更相信",
        "options": [
            "人定胜天",
            "天命难违",
            "天人合一"
        ],
        "explanations": [
            "坚信努力可改变命运。这让你积极进取，但可能低估客观限制。",
            "接受命运安排，不强求。这让你心境平和，但可能缺乏主动性。",
            "追求与天地和谐共处。这让你顺应自然，但可能行动不够果断。"
        ]
    },
    {
        "question": "对于宗门，你的看法是",
        "options": [
            "必须忠诚于宗门",
            "只是修炼的跳板",
            "可有可无的存在"
        ],
        "explanations": [
            "重情重义，忠于集体。这让你得到宗门支持，但可能牺牲个人自由。",
            "务实看待宗门，利用资源提升自己。这让你灵活高效，但忠诚度受质疑。",
            "独立性强，不依赖任何组织。这让你自由，但缺少后盾。"
        ]
    },
    {
        "question": "你更看重",
        "options": [
            "实力境界",
            "实战经验",
            "人脉关系"
        ],
        "explanations": [
            "专注提升自身修为。这让你根基扎实，但可能缺乏应变能力。",
            "重视战斗历练。这让你临场反应强，但可能忽视理论积累。",
            "善于经营人际网络。这让你资源丰富，但可能分散修炼精力。"
        ]
    },
    {
        "question": "修炼资源不足时，你会",
        "options": [
            "抢夺他人资源",
            "自己寻找或创造",
            "交易或合作获取"
        ],
        "explanations": [
            "果断甚至冷酷，不惜用强硬手段。这让你快速获得所需，但声誉受损。",
            "自力更生，靠智慧与劳动获取。这让你心安理得，但过程艰辛。",
            "善于与人交换合作。这让你互利共赢，但需投入时间与信任。"
        ]
    },
    {
        "question": "你更倾向于修炼",
        "options": [
            "攻击型功法",
            "防御型功法",
            "辅助型功法"
        ],
        "explanations": [
            "追求强大的杀伤力。这让你在战斗中占据优势，但防御薄弱。",
            "注重自保与持久。这让你生存能力强，但进攻不足。",
            "擅长支援与控场。这让你团队作用大，但单兵作战弱。"
        ]
    },
    {
        "question": "对于凡人，你的态度是",
        "options": [
            "视如蝼蚁",
            "平等相待",
            "庇护一方"
        ],
        "explanations": [
            "高高在上，不屑与凡人计较。这让你超然，但可能孤立。",
            "尊重生命，平等看待。这让你广结善缘，但可能浪费精力。",
            "心怀慈悲，愿意保护弱者。这让你德望高，但责任重大。"
        ]
    },
    {
        "question": "你更愿意",
        "options": [
            "追求长生",
            "追求力量",
            "追求逍遥"
        ],
        "explanations": [
            "将永生视为最高目标。这让你耐得住寂寞，但可能失去生活趣味。",
            "渴望掌控一切的力量。这让你威慑四方，但易被力量反噬。",
            "向往无拘无束的自由。这让你心境愉悦，但可能缺乏长远规划。"
        ]
    },
    {
        "question": "面对诱惑，你会",
        "options": [
            "坚守本心不为所动",
            "权衡利弊后决定",
            "先拿到手再说"
        ],
        "explanations": [
            "意志坚定，不易被迷惑。这让你保持清白，但可能错失机会。",
            "理性分析，做出最优选择。这让你利益最大化，但可能显得冷漠。",
            "果断出手，先占有再考虑。这让你常得先机，但可能陷入麻烦。"
        ]
    },
    {
        "question": "你更相信",
        "options": [
            "正道光明",
            "魔道速成",
            "亦正亦邪"
        ],
        "explanations": [
            "坚持正义，光明磊落。这让你受人尊敬，但可能限制手段。",
            "追求速效，不惧走极端。这让你进步飞快，但风险巨大。",
            "灵活应变，不拘正邪。这让你适应力强，但可能失去立场。"
        ]
    },
    {
        "question": "修仙之路，你认为最重要的是",
        "options": [
            "天赋资质",
            "勤奋努力",
            "机缘气运"
        ],
        "explanations": [
            "相信天赋决定上限。这让你发挥长处，但可能忽视努力。",
            "坚信勤能补拙。这让你稳步提升，但可能低估机遇作用。",
            "重视机缘与运气。这让你善于把握机会，但可能被动等待。"
        ]
    },
    {
        "question": "在秘境探险途中偶遇珍稀灵草，你会",
        "options": [
            "立刻采摘，以免被人捷足先登",
            "观察四周动静，确保安全后再采",
            "与同行者共享，共担风险与收益"
        ],
        "explanations": [
            "果断抢占先机，体现了你对利益的敏锐嗅觉。这让你常获好处，但易引发冲突。",
            "谨慎行事，优先考虑安全。这让你稳妥获利，但可能错失最佳时机。",
            "乐于分享，兼顾人情与合作。这让你赢得伙伴信任，但收益需平分。"
        ]
    },
    {
        "question": "若修炼中出现奇异幻象，你会",
        "options": [
            "深入其中探究真相",
            "保持清醒，尝试驱散",
            "顺其自然，任其发展"
        ],
        "explanations": [
            "勇于探索未知，可能获得顿悟，但也可能陷入迷障。",
            "理性克制，避免被幻象左右，但可能错过机缘。",
            "随缘而行，心境平和，但可能放任潜在危险。"
        ]
    },
    {
        "question": "你如何看待师承关系",
        "options": [
            "尊师重道，严守教诲",
            "师为我用，择善而从",
            "自立门户，不囿于师门"
        ],
        "explanations": [
            "重情守礼，得师长提携，但可能限制创新。",
            "灵活学习，取精华去糟粕，但可能被视为不敬。",
            "独立求索，发展自我道路，但缺少前辈指引。"
        ]
    },
    {
        "question": "在修仙界听到关于你的流言，你会",
        "options": [
            "公开澄清，维护名誉",
            "置之不理，以实力证明",
            "暗中调查，找出源头"
        ],
        "explanations": [
            "积极回应，维护形象，但可能陷入口舌之争。",
            "以行动代替辩解，彰显自信，但流言可能短期影响声望。",
            "追查到底，消除隐患，但耗费精力且可能得罪人。"
        ]
    },
    {
        "question": "若有机会进入上古遗迹，你会",
        "options": [
            "孤身探秘，独享机缘",
            "结伴而行，互相照应",
            "先研究地图与历史再行动"
        ],
        "explanations": [
            "独行体现胆识，收获可能全归己有，但风险极高。",
            "结伴降低危险，增加成功几率，但需分享成果。",
            "充分准备，提高成功率，但可能错失突发的机缘。"
        ]
    },
    {
        "question": "你更倾向的修炼环境是",
        "options": [
            "灵气充沛的洞天福地",
            "危机四伏的蛮荒之地",
            "红尘闹市中的静修之所"
        ],
        "explanations": [
            "选择优越环境，利于快速提升，但可能缺乏历练。",
            "在险境中磨砺，进步迅速，但生命安全受威胁。",
            "在纷扰中修心，心境坚韧，但灵气稀薄进步慢。"
        ]
    },
    {
        "question": "面对同门竞争，你的态度是",
        "options": [
            "全力以赴，争取第一",
            "保持中游，避免树敌",
            "主动退让，成人之美"
        ],
        "explanations": [
            "争强好胜，易获资源与认可，但人际关系紧张。",
            "中庸之道，安稳度日，但可能错失机会。",
            "谦让赢得好感，但可能被视作软弱。"
        ]
    },
    {
        "question": "你如何看待修仙界的弱肉强食",
        "options": [
            "顺应规则，变强自保",
            "试图改变，建立新秩序",
            "远离纷争，独善其身"
        ],
        "explanations": [
            "认清现实，努力提升以防被吞噬，但可能变得冷酷。",
            "怀抱理想，推动变革，但阻力巨大。",
            "避开争斗，保持清净，但可能失去话语权。"
        ]
    },
    {
        "question": "若获得一件可预知未来的法宝，你会",
        "options": [
            "频繁使用，趋吉避凶",
            "谨慎使用，避免依赖",
            "赠予可信之人，共同谋划"
        ],
        "explanations": [
            "充分利用，减少风险，但可能失去对命运的感知。",
            "适度参考，保持自主，但可能错失先机。",
            "分享信息，凝聚力量，但可能泄露天机。"
        ]
    },
    {
        "question": "你更希望自己的道号被后人记住为",
        "options": [
            "无敌战神",
            "仁心圣者",
            "隐世高人"
        ],
        "explanations": [
            "以实力震慑万世，名扬四海，但可能背负争斗之名。",
            "以德服人，流芳百世，但可能缺乏惊天动地的传奇。",
            "淡泊名利，超然物外，但可能被世人遗忘。"
        ]
    }
]

class IllusionData:
    DATA_PATH = Path(__file__).parent / "illusion"
    STATS_FILE = DATA_PATH / "illusion_stats.json"  # 改名为stats文件
    DAILY_RESET_HOUR = 8  # 每天8点重置
    
    @classmethod
    def get_or_create_user_illusion_info(cls, user_id):
        """获取或创建用户幻境信息"""
        user_id = str(user_id)
        file_path = cls.DATA_PATH / f"{user_id}.json"
        
        question_count = len(DEFAULT_QUESTIONS)  # 从硬编码的问题列表中获取数量
        
        default_data = {
            "last_participate": None,  # 上次参与时间
            "today_choice": None,      # 今日选择
            "question_index": random.randint(0, question_count - 1) if question_count > 0 else None  # 随机分配问题索引
        }
        
        if not file_path.exists():
            os.makedirs(cls.DATA_PATH, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(default_data, f, ensure_ascii=False, indent=4)
            return default_data
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # 检查是否需要重置(每天8点)
        if cls._check_reset(data.get("last_participate")):
            data["today_choice"] = None
            data["question_index"] = random.randint(0, question_count - 1) if question_count > 0 else None  # 重置时重新分配问题
            data["last_participate"] = None
            cls.save_user_illusion_info(user_id, data)
        
        # 确保所有字段都存在
        for key in default_data:
            if key not in data:
                data[key] = default_data[key]
        
        # 如果问题索引不存在或无效，分配一个
        if data["question_index"] is None or data["question_index"] >= question_count:
            data["question_index"] = random.randint(0, question_count - 1) if question_count > 0 else None
            cls.save_user_illusion_info(user_id, data)
        
        return data
    
    @classmethod
    def save_user_illusion_info(cls, user_id, data):
        """保存用户幻境信息"""
        user_id = str(user_id)
        file_path = cls.DATA_PATH / f"{user_id}.json"
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    
    @classmethod
    def get_stats(cls):
        """获取统计数据"""
        if not cls.STATS_FILE.exists():
            # 如果文件不存在，创建默认的统计数据
            default_stats = {"question_stats": [[0] * len(question["options"]) for question in DEFAULT_QUESTIONS]}
            os.makedirs(cls.STATS_FILE.parent, exist_ok=True)
            with open(cls.STATS_FILE, "w", encoding="utf-8") as f:
                json.dump(default_stats, f, ensure_ascii=False, indent=4)
            return default_stats
        
        with open(cls.STATS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            
            # 确保数据结构正确
            if "question_stats" not in data or not isinstance(data["question_stats"], list):
                data = {"question_stats": [[0] * len(question["options"]) for question in DEFAULT_QUESTIONS]}
                with open(cls.STATS_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                return data
            
            # 检查统计数据长度是否与问题数量匹配
            if len(data["question_stats"]) != len(DEFAULT_QUESTIONS):
                # 如果不匹配，重新初始化
                data = {"question_stats": [[0] * len(question["options"]) for question in DEFAULT_QUESTIONS]}
                with open(cls.STATS_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                return data
            
            # 检查每个问题的选项数量是否匹配
            for i, question in enumerate(DEFAULT_QUESTIONS):
                if len(data["question_stats"][i]) != len(question["options"]):
                    data["question_stats"][i] = [0] * len(question["options"])
                    with open(cls.STATS_FILE, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=4)
                    return data
            
            return data
    
    @classmethod
    def update_question_stats(cls, question_index, choice_index):
        """更新问题统计数据"""
        stats = cls.get_stats()
        if 0 <= question_index < len(stats["question_stats"]):
            question_stats = stats["question_stats"][question_index]
            if 0 <= choice_index < len(question_stats):
                question_stats[choice_index] += 1
                with open(cls.STATS_FILE, "w", encoding="utf-8") as f:
                    json.dump(stats, f, ensure_ascii=False, indent=4)
    
    @classmethod
    def _check_reset(cls, last_participate_str):
        """检查是否需要重置(每天8点)"""
        if not last_participate_str:
            return False
            
        try:
            last_participate = datetime.strptime(last_participate_str, "%Y-%m-%d %H:%M:%S")
            now = datetime.now()
            
            # 检查是否是新的天数且过了8点
            return (now.day > last_participate.day and now.hour >= cls.DAILY_RESET_HOUR) or \
                   (now.day == last_participate.day and now.hour >= cls.DAILY_RESET_HOUR and last_participate.hour < cls.DAILY_RESET_HOUR)
        except:
            return False
    
    @classmethod
    def reset_player_data_only(cls):
        """仅重置玩家数据（每日定时任务调用）"""
        for file in cls.DATA_PATH.glob("*.json"):
            try:
                # 直接删除玩家数据文件，下次访问时会自动创建
                file.unlink()
            except:
                continue
    
    @classmethod
    def reset_all_data(cls):
        """重置所有数据（玩家数据和问题统计数据）"""
        # 重置玩家数据
        cls.reset_player_data_only()
        
        # 重置问题统计数据
        stats = cls.get_stats()
        stats["question_stats"] = [[0] * len(question["options"]) for question in DEFAULT_QUESTIONS]
        with open(cls.STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=4)