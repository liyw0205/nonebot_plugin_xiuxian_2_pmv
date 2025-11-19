try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
from nonebot.log import logger

DATABASE = Path() / "data" / "xiuxian"
Xiu_Plugin = Path(__file__).parent.parent


class XiuConfig:
    def __init__(self):
        self.qqq = 144795954 
        # 官群设置
        self.put_bot = []  
        # ["123456"]
        # 接收消息qq,主qq，框架将只处理此qq的消息
        # 设置这个屏蔽群聊/私聊才能生效
        self.shield_group = []  
        # ["123456"]
        # 屏蔽的群聊
        self.response_group = False  
        # 反转屏蔽的群聊，仅响应这些群的消息
        self.shield_private = False  
        # 屏蔽私聊
        self.private_chat_enabled = False 
        # 私聊功能开关，默认关闭，屏蔽时开启也无效
        self.main_bo = []  
        # 负责发送消息的qq
        self.layout_bot_dict = {}
        # QQ所负责的群聊 #{群 ：bot}   其中 bot类型 []或str }
        # "123456":"123456",
        self.admin_debug = False 
        # 管理员调试模式，开启后只响应超管指令
        self.at_response = False
        # 艾特响应命令（官机请勿打开）
        self.at_sender = True
        # 消息是否艾特

        self.banned_unseal_ids = ["779151826"]  
        # 鉴石禁止群

        self.web_status = True
        # 修仙管理面板是否开启
        self.web_port = 5888 
        # 修仙管理面板端口
        self.web_host = "0.0.0.0" 
        # 修仙管理面板IP

        self.img = False 
        # 是否使用图片发送消息
        self.user_info_image = False 
        # 是否使用图片发送个人信息
        self.xiuxian_info_img = False 
        # 开启则使用网络背景图
        self.use_network_avatar = False 
        # 开启则使用网络头像（官机请勿打开）

        self.level = convert_rank('江湖好手')[1] 
        # 境界列表，别动
        self.level_up_cd = 0  
        # 突破CD(分钟)
        self.closing_exp = 100  
        # 闭关每分钟获取的修为
        self.closing_exp_upper_limit = 1.5  
        # 闭关获取修为上限（例如：1.5 下个境界的修为数*1.5）
        self.level_punishment_floor = 10  
        # 突破失败扣除修为，惩罚下限（百分比）
        self.level_punishment_limit = 20  
        # 突破失败扣除修为，惩罚上限(百分比)
        self.level_up_probability = 0.2  
        # 突破失败增加当前境界突破概率的比例
        self.tribulation_min_level = "祭道境圆满"  
        # 最低渡劫境界
        self.tribulation_base_rate = 30  
        # 基础渡劫概率30%
        self.tribulation_max_rate = 90  
        # 最大渡劫概率90%
        self.tribulation_cd = 360  
        # 6小时渡劫冷却(分钟)

        self.lunhui_min_level = "至尊境初期" 
        # 千世轮回最低境界
        self.twolun_min_level = "星芒境初期" 
        # 万世轮回最低境界
        self.threelun_min_level = "祭道境初期" 
        # 永恒轮回最低境界
        self.Infinite_reincarnation_min_level = "破虚境初期" 
        # 永恒轮回最低境界

        self.sect_min_level = "结丹境圆满" 
        # 创建宗门最低境界
        self.sect_create_cost = 5000000 
        # 创建宗门消耗
        self.sect_rename_cost = 50000000 
        # 宗门改名消耗
        self.sect_rename_cd = 1 
        # 宗门改名cd/天
        self.auto_change_sect_owner_cd = 30
        # 自动换长时间不玩宗主cd/天

        self.sign_in_lingshi_lower_limit = 100000  
        # 每日签到灵石下限
        self.sign_in_lingshi_upper_limit = 500000  
        # 每日签到灵石上限
        self.beg_max_level = "结丹境圆满" 
        # 仙途奇缘能领灵石最高境界
        self.beg_max_days = 7 
        # 仙途奇缘能领灵石最多天数
        self.beg_lingshi_lower_limit = 2000000  
        # 仙途奇缘灵石下限
        self.beg_lingshi_upper_limit = 5000000  
        # 仙途奇缘灵石上限
        self.tou = 100000  
        # 偷灵石惩罚
        self.tou_lower_limit = 0.01  
        # 偷灵石下限(百分比)
        self.tou_upper_limit = 0.50
        # 偷灵石上限(百分比)

        self.auto_select_root = True  
        # 默认开启自动选择最佳灵根
        self.remake = 100000  
        # 重入仙途的消费
        self.remaname = 10000000  
        # 修仙改名的消费
        self.max_stamina = 500 
        # 体力上限
        self.stamina_recovery_points = 2 
        # 体力恢复点数/分钟

        self.merge_forward_send = 1 
        # 使用消息合并转发,1是长文本,2是合并转发，3是合并转发的内容转换成长图发送
        self.message_optimization = True  
        # 是否开启信息优化
        # 群聊消息：如果开头没有换行则添加一个换行
        # 私聊消息：如果开头有换行则删除一个换行
        # 所有消息：如果结尾有换行则删除一个换行
        self.img_compression_limit = 90 
        # 图片压缩率，0为不压缩，最高100，jpeg请调低压缩率
        self.img_type = "webp" 
        # 图片类型，webp或者jpeg，如果机器人的图片消息不显示请使用jpeg
        self.img_send_type = "io" 
        # 图片发送类型，官方bot建议base64

        self.sql_table = ["user_xiuxian", "user_cd", "sects", "back", "BuffInfo"]  
        self.sql_user_xiuxian = [
            "id", "user_id", "user_name", "stone", "root",
            "root_type", "root_level", "level", "power",
            "create_time", "is_sign", "is_beg", "is_novice", "is_ban",
            "exp", "work_num", "level_up_cd",
            "level_up_rate", "sect_id",
            "sect_position", "hp", "mp", "atk",
            "atkpractice", "hppractice", "mppractice", "sect_task", "sect_contribution",
            "sect_elixir_get", "blessed_spot_flag", "blessed_spot_name", "user_stamina"
        ]
        self.sql_user_cd = [
            "user_id", "type", "create_time", "scheduled_time", "last_check_info_time"
        ]
        self.sql_sects = [
            "sect_id", "sect_name", "sect_owner", "sect_scale", "sect_used_stone", "join_open", "closed", "sect_fairyland",
            "sect_materials", "mainbuff", "secbuff", "elixir_room_level", "combat_power"
        ]
        self.sql_buff = [
            "id", "user_id", "main_buff", "sec_buff", "effect1_buff", "effect2_buff", "faqi_buff", 
            "fabao_weapon", "armor_buff", "atk_buff", "sub_buff", "blessed_spot"
        ]
        self.sql_back = [
            "user_id", "goods_id", "goods_name", "goods_type", "goods_num", "create_time", "update_time",
            "remake", "day_num", "all_num", "action_time", "state", "bind_num"
        ]
        # 上面是数据库校验,不知道做什么的话别动

def convert_rank(rank_name):
    """
    获取境界等级，替代原来的USERRANK
    convert_rank('江湖好手')[0] 返回江湖好手的境界等级
    convert_rank('江湖好手')[1] 返回境界列表
    """
    ranks = [
        '江湖好手', # 79
        '感气境初期', '感气境中期', '感气境圆满',
        '练气境初期', '练气境中期', '练气境圆满', # 73
        '筑基境初期', '筑基境中期', '筑基境圆满',
        '结丹境初期', '结丹境中期', '结丹境圆满',
        '金丹境初期', '金丹境中期', '金丹境圆满',
        '元神境初期', '元神境中期', '元神境圆满', # 61
        '化神境初期', '化神境中期', '化神境圆满',
        '炼神境初期', '炼神境中期', '炼神境圆满',
        '返虚境初期', '返虚境中期', '返虚境圆满',
        '大乘境初期', '大乘境中期', '大乘境圆满', # 49
        '虚道境初期', '虚道境中期', '虚道境圆满',
        '斩我境初期', '斩我境中期', '斩我境圆满',
        '遁一境初期', '遁一境中期', '遁一境圆满',
        '至尊境初期', '至尊境中期', '至尊境圆满', # 37
        '微光境初期', '微光境中期', '微光境圆满',
        '星芒境初期', '星芒境中期', '星芒境圆满',
        '月华境初期', '月华境中期', '月华境圆满',
        '耀日境初期', '耀日境中期', '耀日境圆满', # 25
        '祭道境初期', '祭道境中期', '祭道境圆满',
        '自在境初期', '自在境中期', '自在境圆满',
        '破虚境初期', '破虚境中期', '破虚境圆满',
        '无界境初期', '无界境中期', '无界境圆满', # 13
        '混元境初期', '混元境中期', '混元境圆满',
        '造化境初期', '造化境中期', '造化境圆满', 
        '永恒境初期', '永恒境中期', '永恒境圆满',
        '至高' # 0
    ]
    
    if rank_name in ranks:
        rank_number = len(ranks) - ranks.index(rank_name) - 1
        return rank_number, ranks
    else:
        return None, ranks

class JsonConfig:
    def __init__(self):
        self.config_jsonpath = DATABASE / "config.json"
        self.create_default_config()
    
    def create_default_config(self):
        """创建默认配置文件"""
        if not self.config_jsonpath.exists():
            default_data = {
                "group": [],  # 群聊禁用列表
                "private_enabled": False,  # 私聊功能开关
                "auto_root_selection": False  # 自动选择灵根开关
            }
            with open(self.config_jsonpath, 'w', encoding='utf-8') as f:
                json.dump(default_data, f)

    def read_data(self):
        """读取配置数据"""
        with open(self.config_jsonpath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if "group" not in data:
                data["group"] = []
            if "private_enabled" not in data:
                data["private_enabled"] = False
            if "auto_root_selection" not in data:
                data["auto_root_selection"] = False
            return data

    def write_data(self, key, id=None):
        """
        设置修仙功能或私聊功能的开启/关闭
        key: 
            1 为开启群聊，2 为关闭群聊
            3 为开启私聊，4 为关闭私聊
            5 为开启自动选择灵根，6 为关闭自动选择灵根
        id: 群聊ID（仅群聊使用）
        """
        json_data = self.read_data()
        if key in [1, 2]:  # 群聊相关
            group_list = json_data.get('group', [])
            if key == 1 and id and id not in group_list:
                group_list.append(id)
            elif key == 2 and id and id in group_list:
                group_list.remove(id)
            json_data['group'] = list(set(group_list))
        elif key == 3:  # 开启私聊
            json_data["private_enabled"] = True
        elif key == 4:  # 关闭私聊
            json_data["private_enabled"] = False
        elif key == 5:  # 开启自动选择灵根
            json_data["auto_root_selection"] = True
        elif key == 6:  # 关闭自动选择灵根
            json_data["auto_root_selection"] = False

        with open(self.config_jsonpath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=4)
        return True

    def is_private_enabled(self):
        """检查私聊功能是否启用"""
        data = self.read_data()
        return data.get("private_enabled", False)
            
    def get_enabled_groups(self):
        """获取开启修仙功能的群聊列表"""
        data = self.read_data()
        return list(set(data.get("group", [])))
    
    def is_auto_root_selection_enabled(self):
        """检查自动选择灵根功能是否启用"""
        data = self.read_data()
        return data.get("auto_root_selection", False)
