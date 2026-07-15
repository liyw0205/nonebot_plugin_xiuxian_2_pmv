try:
    import ujson as json
except ImportError:
    import json
import random
from datetime import datetime

from ...paths import get_paths

from ..xiuxian_config import XiuConfig
from .data_source import jsondata

DATABASE = get_paths().data


class XiuxianJsonDate:
    def __init__(self):
        self.root_jsonpath = DATABASE / "灵根.json"
        self.level_jsonpath = DATABASE / "突破概率.json"

    def beifen_linggen_get(self):
        with open(self.root_jsonpath, 'r', encoding='utf-8') as e:
            a = e.read()
            data = json.loads(a)
            lg = random.choice(data)
            return lg['name'], lg['type']

    def level_rate(self, level):
        with open(self.level_jsonpath, 'r', encoding='utf-8') as e:
            a = e.read()
            data = json.loads(a)
            return data[0][level]

    def linggen_get(self):
        """获取灵根信息"""
        data = jsondata.root_data()
        rate_dict = {}
        for i, v in data.items():
            rate_dict[i] = v["type_rate"]
        lgen = OtherSet().calculated(rate_dict)
        if data[lgen]["type_flag"]:
            flag = random.choice(data[lgen]["type_flag"])
            root = random.sample(data[lgen]["type_list"], flag)
            msg = ""
            for j in root:
                if j == root[-1]:
                    msg += j
                    break
                msg += (j + "、")

            return msg + '属性灵根', lgen
        else:
            root = random.choice(data[lgen]["type_list"])
            return root, lgen


class OtherSet(XiuConfig):

    def __init__(self):
        super().__init__()

    def set_closing_type(self, user_level):
        from .xiuxian2_handle import XiuxianDateManage

        list_all = len(self.level) - 1
        now_index = self.level.index(user_level)
        if list_all == now_index:
            need_exp = 0.001
        else:
            is_updata_level = self.level[now_index + 1]
            need_exp = XiuxianDateManage().get_level_power(is_updata_level)
        return need_exp

    def get_type(self, user_exp, rate, user_level):
        from .xiuxian2_handle import XiuxianDateManage

        list_all = len(self.level) - 1
        now_index = self.level.index(user_level)
        if list_all == now_index:
            return "道友已是最高境界，无法突破！"

        is_updata_level = self.level[now_index + 1]
        need_exp = XiuxianDateManage().get_level_power(is_updata_level)

        # 判断修为是否足够突破
        if user_exp >= need_exp:
            pass
        else:
            from .utils import number_to
            return f"道友的修为不足以突破！距离下次突破需要{number_to(need_exp - user_exp)}修为！突破境界为：{is_updata_level}"

        success_rate = True if random.randint(0, 100) < rate else False

        if success_rate:
            return [self.level[now_index + 1]]
        else:
            return '失败'

    def calculated(self, rate: dict) -> str:
        """
        根据概率计算，轮盘型
        :rate:格式{"数据名":"获取几率"}
        :return: 数据名
        """

        get_list = []  # 概率区间存放

        n = 1
        for name, value in rate.items():  # 生成数据区间
            value_rate = int(value)
            list_rate = [_i for _i in range(n, value_rate + n)]
            get_list.append(list_rate)
            n += value_rate

        now_n = n - 1
        get_random = random.randint(1, now_n)  # 抽取随机数

        index_num = None
        for list_r in get_list:
            if get_random in list_r:  # 判断随机在那个区间
                index_num = get_list.index(list_r)
                break

        return list(rate.keys())[index_num]

    def date_diff(self, new_time, old_time):
        """计算日期差（坏/空时间返回 0，避免结算入口直接炸死）"""
        from .cd_time import parse_cd_datetime

        if not isinstance(new_time, datetime):
            new_time = parse_cd_datetime(new_time, default=None)
        if not isinstance(old_time, datetime):
            old_time = parse_cd_datetime(old_time, default=None)
        if new_time is None or old_time is None:
            return 0
        try:
            return max(0, int((new_time - old_time).total_seconds()))
        except Exception:
            return 0

    def get_power_rate(self, mind, other):
        power_rate = mind / (other + mind)
        if power_rate >= 0.8:
            return "道友偷窃小辈实属天道所不齿！"
        elif power_rate <= 0.05:
            return "道友请不要不自量力！"
        else:
            return int(power_rate * 100)

    def player_fight(self, player1: dict, player2: dict):
        """Purely calculate a round-based fight and return both final states."""
        from .xiuxian2_handle import get_final_attributes

        msg1 = "{}发起攻击，造成了{}伤害\n"
        msg2 = "{}发起攻击，造成了{}伤害\n"

        play_list = []
        suc = None
        default_msg = {id(player1): msg1, id(player2): msg2}

        def get_player_speed(player: dict):
            if "速度" in player:
                return float(player.get("速度", 0) or 0)
            user_id = player.get("user_id")
            final_attr = get_final_attributes(user_id, include_current=True) if user_id else None
            return float(final_attr.get("speed", 0)) if final_attr else 0

        def calc_damage(attacker: dict, defender: dict):
            msg_tpl = default_msg[id(attacker)]
            attack = int(round(random.uniform(0.95, 1.05), 2) * attacker['攻击'])
            if random.randint(0, 100) <= attacker['会心']:
                attack = int(attack * attacker['爆伤'])
                msg_tpl = "{}发起会心一击，造成了{}伤害\n"
            damage = int(attack * (1 - defender['防御']))
            return msg_tpl, max(0, damage)

        speed_tiebreaker = {id(player1): random.random(), id(player2): random.random()}
        player1["速度"] = get_player_speed(player1)
        player2["速度"] = get_player_speed(player2)
        if player1['气血'] <= 0:
            player1['气血'] = 1
        if player2['气血'] <= 0:
            player2['气血'] = 1
        while True:
            order = sorted(
                (player1, player2),
                key=lambda p: (get_player_speed(p), speed_tiebreaker[id(p)]),
                reverse=True
            )

            for attacker in order:
                defender = player2 if attacker is player1 else player1
                if attacker['气血'] <= 0 or defender['气血'] <= 0:
                    continue

                msg_tpl, damage = calc_damage(attacker, defender)
                play_list.append(msg_tpl.format(attacker['道号'], damage))
                defender['气血'] -= damage
                play_list.append(f"{defender['道号']}剩余血量{defender['气血']}")

                if defender['气血'] <= 0:
                    play_list.append(f"{attacker['道号']}胜利")
                    suc = str(attacker['user_id'])
                    defender['气血'] = 1
                    break

            if suc:
                break

        final = {
            str(player1['user_id']): (int(player1['气血']), int(player1['真元'])),
            str(player2['user_id']): (int(player2['气血']), int(player2['真元'])),
        }
        return play_list, suc, final

    def send_hp_mp(self, user_id, hp, mp):
        from .xiuxian2_handle import XiuxianDateManage

        user_msg = XiuxianDateManage().get_user_info_with_id(user_id)
        max_hp = int(user_msg['exp'] / 2)
        max_mp = int(user_msg['exp'])

        msg = []
        hp_mp = []
        from .utils import number_to
        if user_msg['hp'] < max_hp:
            if user_msg['hp'] + hp < max_hp:
                new_hp = user_msg['hp'] + hp
                msg.append(f',回复气血：{number_to(hp)}')
            else:
                new_hp = max_hp
                msg.append(',气血已回满！')
        else:
            new_hp = user_msg['hp']
            msg.append('')

        if user_msg['mp'] < max_mp:
            if user_msg['mp'] + mp < max_mp:
                new_mp = user_msg['mp'] + mp
                msg.append(f',回复真元：{number_to(mp)}')
            else:
                new_mp = max_mp
                msg.append(',真元已回满！')
        else:
            new_mp = user_msg['mp']
            msg.append('')

        hp_mp.append(new_hp)
        hp_mp.append(new_mp)
        hp_mp.append(user_msg['exp'])

        return msg, hp_mp
