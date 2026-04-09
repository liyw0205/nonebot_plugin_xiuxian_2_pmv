try:
    import ujson as json
except ImportError:
    import json
import os
import zipfile
import random
import shutil
import sqlite3
import string
import time
from datetime import datetime, timedelta
from pathlib import Path
import threading
from nonebot.log import logger
from .data_source import jsondata
from ..xiuxian_config import XiuConfig, convert_rank
# from .. import DRIVER
from nonebot import get_driver
from .download_xiuxian_data import UpdateManager
from .item_json import Items
from .xn_xiuxian_impart_config import config_impart

WORKDATA = Path() / "data" / "xiuxian" / "work"
DATABASE = Path() / "data" / "xiuxian"
SKILLPATHH = DATABASE / "功法"
WEAPONPATH = DATABASE / "装备"
xiuxian_num = "578043031" # 这里其实是修仙1作者的QQ号
impart_num = "123451234"
trade_num = "123451234"
player_num = "123451234"
current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')


class XiuxianDateManage:
    global xiuxian_num
    _instance = {}
    _has_init = {}

    def __new__(cls):
        if cls._instance.get(xiuxian_num) is None:
            cls._instance[xiuxian_num] = super(XiuxianDateManage, cls).__new__(cls)
        return cls._instance[xiuxian_num]

    def __init__(self):
        if not self._has_init.get(xiuxian_num):
            self._has_init[xiuxian_num] = True
            self.database_path = DATABASE
            if not self.database_path.exists():
                self.database_path.mkdir(parents=True)
            self.database_path /= "xiuxian.db"
            self.conn = sqlite3.connect(self.database_path, check_same_thread=False)
            self.lock = threading.RLock()
            logger.opt(colors=True).info(f"<green>修仙数据库已连接！</green>")
            self._check_data()

    def close(self):
        with self.lock:
            if getattr(self, "conn", None):
                self.conn.close()
                self.conn = None
                logger.opt(colors=True).info(f"<green>修仙数据库关闭！</green>")

    def _check_data(self):
        """检查数据完整性"""
        with self.lock:
            c = self.conn.cursor()

            for i in XiuConfig().sql_table:
                if i == "user_xiuxian":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except sqlite3.OperationalError:
                        c.execute("""CREATE TABLE "user_xiuxian" (
      "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      "user_id" TEXT NOT NULL,
      "sect_id" INTEGER DEFAULT NULL,
      "sect_position" INTEGER DEFAULT NULL,
      "stone" integer DEFAULT 0,
      "root" TEXT,
      "root_type" TEXT,
      "root_level" integer DEFAULT 0,
      "level" TEXT,
      "power" integer DEFAULT 0,
      "create_time" integer,
      "is_sign" integer DEFAULT 0,
      "is_beg" integer DEFAULT 0,
      "is_novice" integer DEFAULT 0,
      "is_ban" integer DEFAULT 0,
      "exp" integer DEFAULT 0,
      "work_num" integer DEFAULT 5,
      "user_name" TEXT DEFAULT NULL,
      "level_up_cd" integer DEFAULT NULL,
      "level_up_rate" integer DEFAULT 0,
      "mixelixir_num" integer DEFAULT 0
    );""")
                elif i == "user_cd":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except sqlite3.OperationalError:
                        c.execute("""CREATE TABLE "user_cd" (
      "user_id" TEXT NOT NULL PRIMARY KEY,
      "type" integer DEFAULT 0,
      "create_time" TEXT DEFAULT NULL,
      "scheduled_time" TEXT,
      "last_check_info_time" TEXT DEFAULT NULL
    );""")
                elif i == "sects":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except sqlite3.OperationalError:
                        c.execute("""CREATE TABLE "sects" (
      "sect_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      "sect_name" TEXT NOT NULL,
      "sect_owner" integer,
      "sect_scale" integer NOT NULL,
      "sect_used_stone" integer,
      "join_open" integer DEFAULT 1,
      "closed" integer DEFAULT 0,
      "combat_power" integer DEFAULT 0,
      "sect_fairyland" integer
    );""")
                elif i == "back":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except sqlite3.OperationalError:
                        c.execute("""CREATE TABLE "back" (
      "user_id" TEXT NOT NULL,
      "goods_id" INTEGER NOT NULL,
      "goods_name" TEXT,
      "goods_type" TEXT,
      "goods_num" INTEGER,
      "create_time" TEXT,
      "update_time" TEXT,
      "remake" TEXT,
      "day_num" INTEGER DEFAULT 0,
      "all_num" INTEGER DEFAULT 0,
      "action_time" TEXT,
      "state" INTEGER DEFAULT 0
    );""")
                elif i == "BuffInfo":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except sqlite3.OperationalError:
                        c.execute("""CREATE TABLE "BuffInfo" (
      "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      "user_id" TEXT DEFAULT 0,
      "main_buff" integer DEFAULT 0,
      "sec_buff" integer DEFAULT 0,
      "effect1_buff" integer DEFAULT 0,
      "effect2_buff" integer DEFAULT 0,
      "faqi_buff" integer DEFAULT 0,
      "fabao_weapon" integer DEFAULT 0,
      "sub_buff" integer DEFAULT 0
    );""")

            for i in XiuConfig().sql_user_xiuxian:
                try:
                    c.execute(f"select {i} from user_xiuxian")
                except sqlite3.OperationalError:
                    sql = f"ALTER TABLE user_xiuxian ADD COLUMN {i} INTEGER DEFAULT 0;"
                    c.execute(sql)

            for d in XiuConfig().sql_user_cd:
                try:
                    c.execute(f"select {d} from user_cd")
                except sqlite3.OperationalError:
                    sql = f"ALTER TABLE user_cd ADD COLUMN {d} INTEGER DEFAULT 0;"
                    c.execute(sql)

            for s in XiuConfig().sql_sects:
                try:
                    c.execute(f"select {s} from sects")
                except sqlite3.OperationalError:
                    sql = f"ALTER TABLE sects ADD COLUMN {s} INTEGER DEFAULT 0;"
                    c.execute(sql)

            for m in XiuConfig().sql_buff:
                try:
                    c.execute(f"select {m} from BuffInfo")
                except sqlite3.OperationalError:
                    sql = f"ALTER TABLE BuffInfo ADD COLUMN {m} INTEGER DEFAULT 0;"
                    c.execute(sql)

            for b in XiuConfig().sql_back:
                try:
                    c.execute(f"select {b} from back")
                except sqlite3.OperationalError:
                    sql = f"ALTER TABLE back ADD COLUMN {b} INTEGER DEFAULT 0;"
                    c.execute(sql)

            now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            c.execute(
                """UPDATE user_cd
                   SET last_check_info_time = ?
                   WHERE last_check_info_time = '0' OR last_check_info_time IS NULL
                """,
                (now_time,)
            )

            self.conn.commit()

    @classmethod
    def close_dbs(cls):
        XiuxianDateManage().close()

    def _create_user(self, user_id: str, root: str, type: str, power: str, create_time, user_name) -> None:
        """在数据库中创建用户并初始化"""
        with self.lock:
            c = self.conn.cursor()
            sql = f"INSERT INTO user_xiuxian (user_id, stone, root, root_type, root_level, level, power, create_time, user_name, exp, work_num, sect_id, sect_position, user_stamina, is_novice) VALUES (?, 0, ?, ?, 0, '江湖好手', ?, ?, ?, 100, 5, NULL, NULL, ?, 0)"
            c.execute(sql, (user_id, root, type, power, create_time, user_name, XiuConfig().max_stamina))
            self.conn.commit()

    def today_active_users(self):
        """获取今日活跃用户数（今天有操作记录的用户）"""
        with self.lock:
            cur = self.conn.cursor()
            today = datetime.now().strftime('%Y-%m-%d')
            sql = f"SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE date(create_time) = ?"
            cur.execute(sql, (today,))
            result = cur.fetchone()
            if result:
                return result[0]
            else:
                return 0

    def yesterday_active_users(self):
        """获取昨日活跃用户数（昨天有操作记录的用户）"""
        with self.lock:
            cur = self.conn.cursor()
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            sql = f"SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE date(create_time) = ?"
            cur.execute(sql, (yesterday,))
            result = cur.fetchone()
            if result:
                return result[0]
            else:
                return 0

    def last_7days_active_users(self):
        """获取近七日活跃用户数（最近7天内有操作记录的用户）"""
        with self.lock:
            cur = self.conn.cursor()
            seven_days_ago = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
            sql = f"SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE date(create_time) >= ?"
            cur.execute(sql, (seven_days_ago,))
            result = cur.fetchone()
            if result:
                return result[0]
            else:
                return 0

    def all_users(self):
        """获取全部用户数"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "SELECT COUNT(*) FROM user_xiuxian"
            cur.execute(sql)
            result = cur.fetchone()
            if result:
                return result[0]
            else:
                return 0

    def get_user_count_by_level(self, level_name: str) -> int:
        """查询指定境界的人数"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM user_xiuxian WHERE level = ?",
                (level_name,)
            )
            return cursor.fetchone()[0]

    def total_items_quantity(self):
        """获取全部用户背包的物品数量总合"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "SELECT SUM(goods_num) FROM back"
            cur.execute(sql)
            result = cur.fetchone()
            if result and result[0] is not None:
                return result[0]
            else:
                return 0

    def get_user_info_with_id(self, user_id):
        """根据USER_ID获取用户信息,不获取功法加成"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select * from user_xiuxian WHERE user_id=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                user_dict = dict(zip(columns, result))
                return user_dict
            else:
                return None

    def get_user_info_with_name(self, user_id):
        """根据user_name获取用户信息"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select * from user_xiuxian WHERE user_name=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                user_dict = dict(zip(columns, result))
                return user_dict
            else:
                return None

    def update_all_users_stamina(self, max_stamina, stamina):
        """体力未满用户更新体力值"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"""
                UPDATE user_xiuxian
                SET user_stamina = MIN(user_stamina + ?, ?)
                WHERE user_stamina < ?
            """
            cur.execute(sql, (stamina, max_stamina, max_stamina))
            self.conn.commit()

    def update_user_stamina(self, user_id, stamina_change, key):
        """更新用户体力值 1为增加，2为减少"""
        with self.lock:
            cur = self.conn.cursor()
            max_stamina = XiuConfig().max_stamina

            if key == 1:
                cur.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (user_id,))
                current_stamina = cur.fetchone()[0]
                new_stamina = min(current_stamina + stamina_change, max_stamina)
                if current_stamina < max_stamina:
                    sql = "UPDATE user_xiuxian SET user_stamina=? WHERE user_id=?"
                    cur.execute(sql, (new_stamina, user_id))
                    self.conn.commit()

            elif key == 2:
                sql = "UPDATE user_xiuxian SET user_stamina=MAX(user_stamina-?, 0) WHERE user_id=?"
                cur.execute(sql, (stamina_change, user_id))
                self.conn.commit()

    def get_user_real_info(self, user_id):
        """根据USER_ID获取用户信息,获取功法加成"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select * from user_xiuxian WHERE user_id=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result:
                columns = cur.description
                user_data_dict = final_user_data(result, columns)
                return user_data_dict
            else:
                return None

    def get_player_data(self, user_id, boss=False):
        """根据USER_ID获取用户信息,获取属性"""
        with self.lock:
            player = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '防御': 0}
            userinfo = sql_message.get_user_real_info(user_id)
            user_weapon_data = UserBuffDate(userinfo['user_id']).get_user_weapon_data()

            impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
            boss_atk = impart_data['boss_atk'] if impart_data and impart_data['boss_atk'] is not None else 0
            impart_atk_per = impart_data['impart_atk_per'] if impart_data is not None else 0
            user_armor_data = UserBuffDate(userinfo['user_id']).get_user_armor_buff_data()
            user_main_data = UserBuffDate(userinfo['user_id']).get_user_main_buff_data()
            user1_sub_buff_data = UserBuffDate(userinfo['user_id']).get_user_sub_buff_data()
            integral_buff = user1_sub_buff_data['integral'] if user1_sub_buff_data is not None else 0
            exp_buff = user1_sub_buff_data['exp'] if user1_sub_buff_data is not None else 0

            if user_main_data != None:
                main_crit_buff = user_main_data['crit_buff']
            else:
                main_crit_buff = 0

            if user_armor_data != None:
                armor_crit_buff = user_armor_data['crit_buff']
            else:
                armor_crit_buff = 0

            if user_weapon_data != None:
                player['会心'] = int(((user_weapon_data['crit_buff']) + (armor_crit_buff) + (main_crit_buff)) * 100)
            else:
                player['会心'] = (armor_crit_buff + main_crit_buff) * 100

            player['user_id'] = userinfo['user_id']
            player['道号'] = userinfo['user_name']
            player['气血'] = userinfo['hp']
            if boss:
                player['攻击'] = int(userinfo['atk'] * (1 + boss_atk))
            else:
                player['攻击'] = int(userinfo['atk'])
            player['真元'] = userinfo['mp']
            player['exp'] = userinfo['exp']
            return player

    def get_sect_info(self, sect_id):
        """
        通过宗门编号获取宗门信息
        """
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select * from sects WHERE sect_id=?"
            cur.execute(sql, (sect_id,))
            result = cur.fetchone()
            if result:
                sect_id_dict = dict(zip((col[0] for col in cur.description), result))
                return sect_id_dict
            else:
                return None

    def get_sect_owners(self):
        """获取所有宗主的 user_id"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"SELECT user_id FROM user_xiuxian WHERE sect_position = 0"
            cur.execute(sql)
            result = cur.fetchall()
            return [row[0] for row in result]

    def get_elders(self):
        """获取所有长老的 user_id"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"SELECT user_id FROM user_xiuxian WHERE sect_position = 2"
            cur.execute(sql)
            result = cur.fetchall()
            return [row[0] for row in result]

    def create_user(self, user_id, *args):
        """校验用户是否存在"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select * from user_xiuxian WHERE user_id=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if not result:
                self._create_user(user_id, args[0], args[1], args[2], args[3], args[4])
                self.conn.commit()
                welcome_msg = f"欢迎进入修仙世界的，你的灵根为：{args[0]},类型是：{args[1]},你的战力为：{args[2]},当前境界：江湖好手"
                return True, welcome_msg
            else:
                return False, f"您已迈入修仙世界，输入【我的修仙信息】获取数据吧！"

    def get_sign(self, user_id):
        """获取用户签到信息"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "select is_sign from user_xiuxian WHERE user_id=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if not result:
                return f"修仙界没有你的足迹，输入 我要修仙 加入修仙世界吧！"
            elif result[0] == 0:
                ls = random.randint(XiuConfig().sign_in_lingshi_lower_limit, XiuConfig().sign_in_lingshi_upper_limit)
                sql2 = f"UPDATE user_xiuxian SET is_sign=1,stone=stone+? WHERE user_id=?"
                cur.execute(sql2, (ls, user_id))
                self.conn.commit()
                return f"签到成功，获取{ls}块灵石!"
            elif result[0] == 1:
                return f"贪心的人是不会有好运的！"

    def get_beg(self, user_id):
        """获取仙途奇缘信息"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select is_beg from user_xiuxian WHERE user_id=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result[0] == 0:
                ls = random.randint(XiuConfig().beg_lingshi_lower_limit, XiuConfig().beg_lingshi_upper_limit)
                sql2 = f"UPDATE user_xiuxian SET is_beg=1,stone=stone+? WHERE user_id=?"
                cur.execute(sql2, (ls, user_id))
                self.conn.commit()
                return ls
            elif result[0] == 1:
                return None

    def get_novice(self, user_id):
        """检查用户是否已领取新手礼包"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select is_novice from user_xiuxian WHERE user_id=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result[0] == 0:
                return True
            elif result[0] == 1:
                return None

    def save_novice(self, user_id):
        """标记用户已领取新手礼包"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET is_novice=1 WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()

    def novice_remake(self):
        """重置新手礼包"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET is_novice=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

    def get_user_create_time(self, user_id):
        """获取用户创建时间"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "SELECT create_time FROM user_xiuxian WHERE user_id=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result and result[0]:
                return _safe_parse_dt(result[0])
            return None

    def ramaker(self, lg, type, user_id):
        """洗灵根"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE user_xiuxian SET root=?,root_type=?,stone=stone-? WHERE user_id=?"
            cur.execute(sql, (lg, type, XiuConfig().remake, user_id))
            self.conn.commit()

            self.update_power2(user_id)
            return f"逆天之行，重获新生，新的灵根为：{lg}，类型为：{type}"

    def get_root_rate(self, name, user_id):
        """获取灵根倍率"""
        with self.lock:
            data = jsondata.root_data()
            if name == '命运道果':
                type_speeds = data[name]['type_speeds']
                user_info = sql_message.get_user_info_with_id(user_id)
                root_level = user_info['root_level']
                type_speeds2 = data['永恒道果']['type_speeds']
                type_speeds3 = (type_speeds2 + (root_level * type_speeds))
                return type_speeds3
            else:
                return data[name]['type_speeds']

    def get_level_power(self, name):
        """获取境界倍率|exp"""
        with self.lock:
            data = jsondata.level_data()
            return data[name]['power']

    def get_level_cost(self, name):
        """获取炼体境界倍率"""
        with self.lock:
            data = jsondata.exercises_level_data()
            return data[name]['cost_exp'], data[name]['cost_stone']

    def update_power2(self, user_id) -> None:
        """更新战力"""
        with self.lock:
            UserMessage = self.get_user_info_with_id(user_id)
            cur = self.conn.cursor()
            level = jsondata.level_data()
            root_rate = sql_message.get_root_rate(UserMessage['root_type'], user_id)
            sql = f"UPDATE user_xiuxian SET power=round(exp*?*?,0) WHERE user_id=?"
            cur.execute(sql, (root_rate, level[UserMessage['level']]["spend"], user_id))
            self.conn.commit()

    def update_ls(self, user_id, price, key):
        """更新灵石 1增加 2减少"""
        with self.lock:
            cur = self.conn.cursor()
            price = abs(int(price))
            if key == 1:
                sql = "UPDATE user_xiuxian SET stone=stone+? WHERE user_id=?"
                cur.execute(sql, (price, user_id))
            elif key == 2:
                sql = "UPDATE user_xiuxian SET stone=MAX(stone-?, 0) WHERE user_id=?"
                cur.execute(sql, (price, user_id))
            self.conn.commit()

    def update_exp(self, user_id, exp):
        """增加修为"""
        with self.lock:
            exp = number_count(exp)
            sql = "UPDATE user_xiuxian SET exp=exp+? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (exp, user_id))
            self.conn.commit()

    def update_j_exp(self, user_id, exp):
        """减少修为"""
        with self.lock:
            exp = number_count(exp)
            sql = "UPDATE user_xiuxian SET exp=MAX(exp-?, 0) WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (exp, user_id))
            self.conn.commit()

    def update_root(self, user_id, key):
        """更新灵根"""
        with self.lock:
            cur = self.conn.cursor()
            if int(key) == 1:
                sql = f"UPDATE user_xiuxian SET root=?,root_type=? WHERE user_id=?"
                cur.execute(sql, ("全属性灵根", "混沌灵根", user_id))
                root_name = "混沌灵根"
                self.conn.commit()

            elif int(key) == 2:
                sql = f"UPDATE user_xiuxian SET root=?,root_type=? WHERE user_id=?"
                cur.execute(sql, ("融合万物灵根", "融合灵根", user_id))
                root_name = "融合灵根"
                self.conn.commit()

            elif int(key) == 3:
                sql = f"UPDATE user_xiuxian SET root=?,root_type=? WHERE user_id=?"
                cur.execute(sql, ("月灵根", "超灵根", user_id))
                root_name = "超灵根"
                self.conn.commit()

            elif int(key) == 4:
                sql = f"UPDATE user_xiuxian SET root=?,root_type=? WHERE user_id=?"
                cur.execute(sql, ("言灵灵根", "龙灵根", user_id))
                root_name = "龙灵根"
                self.conn.commit()

            elif int(key) == 5:
                sql = f"UPDATE user_xiuxian SET root=?,root_type=? WHERE user_id=?"
                cur.execute(sql, ("金灵根", "天灵根", user_id))
                root_name = "天灵根"
                self.conn.commit()

            elif int(key) == 6:
                sql = f"UPDATE user_xiuxian SET root=?,root_type=? WHERE user_id=?"
                cur.execute(sql, ("轮回千次不灭，只为臻至巅峰", "轮回道果", user_id))
                root_name = "轮回道果"
                self.conn.commit()

            elif int(key) == 7:
                sql = f"UPDATE user_xiuxian SET root=?,root_type=? WHERE user_id=?"
                cur.execute(sql, ("轮回万次不灭，只为超越巅峰", "真·轮回道果", user_id))
                root_name = "真·轮回道果"
                self.conn.commit()

            elif int(key) == 8:
                sql = f"UPDATE user_xiuxian SET root=?,root_type=? WHERE user_id=?"
                cur.execute(sql, ("轮回无尽不灭，只为触及永恒之境", "永恒道果", user_id))
                root_name = "永恒道果"
                self.conn.commit()

            elif int(key) == 9:
                user_info = sql_message.get_user_info_with_id(user_id)
                sql = f"UPDATE user_xiuxian SET root=?,root_type=? WHERE user_id=?"
                cur.execute(sql, (f"轮回命主·{user_info['user_name']}", "命运道果", user_id))
                root_name = "命运道果"
                self.conn.commit()

            return root_name

    def update_ls_all(self, price):
        """所有用户增加灵石"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE user_xiuxian SET stone=stone+?"
            cur.execute(sql, (price,))
            self.conn.commit()

    def get_exp_rank(self, user_id):
        """修为排行"""
        with self.lock:
            sql = f"select rank from(select user_id,exp,dense_rank() over (ORDER BY exp desc) as 'rank' FROM user_xiuxian) WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            return result

    def get_stone_rank(self, user_id):
        """灵石排行"""
        with self.lock:
            sql = f"select rank from(select user_id,stone,dense_rank() over (ORDER BY stone desc) as 'rank' FROM user_xiuxian) WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            return result

    def get_ls_rank(self):
        """灵石排行榜"""
        with self.lock:
            sql = f"SELECT user_id,stone FROM user_xiuxian  WHERE stone>0 ORDER BY stone DESC LIMIT 5"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result

    def sign_remake(self):
        """重置签到"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET is_sign=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

    def beg_remake(self):
        """重置仙途奇缘"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET is_beg=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

    def ban_user(self, user_id):
        """将用户关进小黑屋"""
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT is_ban FROM user_xiuxian WHERE user_id = ?", (user_id,))
            result = cur.fetchone()
            if not result:
                return False
            if result[0] == 1:
                return False
            sql = "UPDATE user_xiuxian SET is_ban = 1 WHERE user_id = ?"
            cur.execute(sql, (user_id,))
            self.conn.commit()
            return True

    def unban_user(self, user_id):
        """解除用户小黑屋状态"""
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT is_ban FROM user_xiuxian WHERE user_id = ?", (user_id,))
            result = cur.fetchone()
            if not result:
                return False
            if result[0] == 0:
                return False
            sql = "UPDATE user_xiuxian SET is_ban = 0 WHERE user_id = ?"
            cur.execute(sql, (user_id,))
            self.conn.commit()
            return True

    def update_mixelixir_num(self, user_id):
        """增加炼丹次数"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET mixelixir_num=mixelixir_num+1 WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()

    def update_user_name(self, user_id, user_name):
        """更新用户道号"""
        with self.lock:
            cur = self.conn.cursor()
            get_name = f"select user_name from user_xiuxian WHERE user_name=?"
            cur.execute(get_name, (user_name,))
            result = cur.fetchone()
            if result:
                return "已存在该道号！"
            else:
                sql = f"UPDATE user_xiuxian SET user_name=? WHERE user_id=?"
                cur.execute(sql, (user_name, user_id))
                self.conn.commit()
                return '道友的道号更新成啦~'

    def updata_level_cd(self, user_id):
        """更新突破境界CD"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET level_up_cd=? WHERE user_id=?"
            cur = self.conn.cursor()
            now_time = datetime.now()
            cur.execute(sql, (now_time, user_id))
            self.conn.commit()

    def update_last_check_info_time(self, user_id):
        """更新查看修仙信息时间"""
        with self.lock:
            sql = "UPDATE user_cd SET last_check_info_time = ? WHERE user_id = ?"
            cur = self.conn.cursor()
            now_time = datetime.now()
            cur.execute(sql, (now_time, user_id))
            self.conn.commit()

    def get_last_check_info_time(self, user_id):
        """获取最后一次查看修仙信息时间"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "SELECT last_check_info_time FROM user_cd WHERE user_id = ?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result and result[0]:
                return _safe_parse_dt(result[0])
            return None

    def updata_level(self, user_id, level_name):
        """更新境界"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET level=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (level_name, user_id))
            self.conn.commit()

    def updata_root_level(self, user_id, level_num):
        """更新轮回等级"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET root_level=root_level+? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (level_num, user_id))
            self.conn.commit()

    def get_user_cd(self, user_id):
        """获取用户操作CD"""
        with self.lock:
            sql = f"SELECT * FROM user_cd  WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                user_cd_dict = dict(zip(columns, result))
                return user_cd_dict
            else:
                self.insert_user_cd(user_id)
                return None

    def insert_user_cd(self, user_id) -> None:
        """添加用户至CD表"""
        with self.lock:
            sql = f"INSERT INTO user_cd (user_id) VALUES (?)"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()

    def create_sect(self, user_id, sect_name) -> None:
        """创建宗门"""
        with self.lock:
            sql = f"INSERT INTO sects(sect_name, sect_owner, sect_scale, sect_used_stone, join_open, closed, combat_power) VALUES (?,?,0,0,1,0,0)"
            cur = self.conn.cursor()
            cur.execute(sql, (sect_name, user_id))
            self.conn.commit()

    def update_sect_name(self, sect_id, sect_name) -> None:
        """修改宗门名称"""
        with self.lock:
            cur = self.conn.cursor()
            get_sect_name = f"select sect_name from sects WHERE sect_name=?"
            cur.execute(get_sect_name, (sect_name,))
            result = cur.fetchone()
            if result:
                return False
            else:
                sql = f"UPDATE sects SET sect_name=? WHERE sect_id=?"
                cur.execute(sql, (sect_name, sect_id))
                self.conn.commit()
                return True

    def get_sect_info_by_qq(self, user_id):
        """通过用户qq获取宗门信息"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select * from sects WHERE sect_owner=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                sect_onwer_dict = dict(zip(columns, result))
                return sect_onwer_dict
            else:
                return None

    def calculate_sect_combat_power(self, sect_id):
        """计算宗门战力"""
        with self.lock:
            members = self.get_all_users_by_sect_id(sect_id)
            total_power = 0
            for member in members:
                user_real_info = self.get_user_real_info(member['user_id'])
                if user_real_info and 'power' in user_real_info:
                    total_power += user_real_info['power']
            return total_power

    def update_sect_combat_power(self, sect_id):
        """更新宗门战力"""
        with self.lock:
            total_power = self.calculate_sect_combat_power(sect_id)
            sql = "UPDATE sects SET combat_power = ? WHERE sect_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (total_power, sect_id))
            self.conn.commit()
            return total_power

    def combat_power_top(self):
        """宗门战力排行榜"""
        with self.lock:
            sql = f"SELECT sect_id, sect_name, combat_power FROM sects WHERE sect_owner is NOT NULL ORDER BY combat_power DESC LIMIT 50"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result

    def get_sect_info_by_id(self, sect_id):
        """通过宗门id获取宗门信息"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select * from sects WHERE sect_id=?"
            cur.execute(sql, (sect_id,))
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                sect_dict = dict(zip(columns, result))
                return sect_dict
            else:
                return None

    def update_usr_sect(self, user_id, usr_sect_id, usr_sect_position):
        """更新用户信息表的宗门信息字段"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET sect_id=?,sect_position=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (usr_sect_id, usr_sect_position, user_id))
            self.conn.commit()

    def update_sect_owner(self, user_id, sect_id):
        """更新宗门所有者"""
        with self.lock:
            sql = f"UPDATE sects SET sect_owner=? WHERE sect_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id, sect_id))
            self.conn.commit()

    def get_highest_contrib_user_except_current(self, sect_id, current_owner_id):
        """获取指定宗门的贡献最高的人，排除当前宗主"""
        with self.lock:
            cur = self.conn.cursor()
            sql = """
            SELECT user_id
            FROM user_xiuxian
            WHERE sect_id = ? AND sect_position = 1 AND user_id != ?
            ORDER BY sect_contribution DESC
            LIMIT 1
            """
            cur.execute(sql, (sect_id, current_owner_id))
            result = cur.fetchone()
            if result:
                return result
            else:
                return None

    def get_highest_contrib_user(self, sect_id):
        """获取宗门中贡献最高的用户（不限职位）"""
        with self.lock:
            cur = self.conn.cursor()
            sql = """
            SELECT user_id 
            FROM user_xiuxian 
            WHERE sect_id = ? 
            ORDER BY sect_contribution DESC 
            LIMIT 1
            """
            cur.execute(sql, (sect_id,))
            result = cur.fetchone()
            return result

    def update_sect_join_status(self, sect_id, status):
        """更新宗门加入状态"""
        with self.lock:
            sql = f"UPDATE sects SET join_open=? WHERE sect_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (status, sect_id))
            self.conn.commit()

    def update_sect_closed_status(self, sect_id, status):
        """更新宗门封闭状态"""
        with self.lock:
            sql = f"UPDATE sects SET closed=? WHERE sect_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (status, sect_id))
            self.conn.commit()

    def delete_sect(self, sect_id):
        """删除宗门并踢出所有成员"""
        with self.lock:
            cur = self.conn.cursor()
            try:
                members_sql = "SELECT user_id FROM user_xiuxian WHERE sect_id = ?"
                cur.execute(members_sql, (sect_id,))
                members = cur.fetchall()

                if members:
                    update_sql = """
                        UPDATE user_xiuxian 
                        SET sect_id = NULL, sect_position = NULL, sect_contribution = 0 
                        WHERE sect_id = ?
                    """
                    cur.execute(update_sql, (sect_id,))
                    logger.opt(colors=True).info(f"<green>已踢出宗门 {sect_id} 的所有成员，共 {len(members)} 人</green>")

                delete_sql = "DELETE FROM sects WHERE sect_id = ?"
                cur.execute(delete_sql, (sect_id,))

                self.conn.commit()
                logger.opt(colors=True).info(f"<green>宗门 {sect_id} 解散成功，已清理所有成员数据</green>")
                return True

            except Exception as e:
                self.conn.rollback()
                logger.error(f"解散宗门 {sect_id} 时发生错误: {str(e)}")
                return False

    def get_sect_name(self, sect_name):
        """通过宗门名称获取宗门ID"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "SELECT sect_id FROM sects WHERE sect_name = ?"
            cur.execute(sql, (sect_name,))
            result = cur.fetchone()
            if result:
                return result[0]
            else:
                return None

    def get_all_sect_id(self):
        """获取全部宗门id"""
        with self.lock:
            sql = "SELECT sect_id FROM sects"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            if result:
                return result
            else:
                return None

    def get_all_user_id(self):
        """获取全部用户id"""
        with self.lock:
            sql = "SELECT user_id FROM user_xiuxian"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            if result:
                return [row[0] for row in result]
            else:
                return None

    def in_closing(self, user_id, the_type):
        """更新用户操作CD"""
        with self.lock:
            now_time = None
            if the_type == 0:
                now_time = 0
            else:
                now_time = datetime.now()
            sql = "UPDATE user_cd SET type=?,create_time=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (the_type, now_time, user_id))
            self.conn.commit()

    def del_exp_decimal(self, user_id, exp):
        """去浮点"""
        with self.lock:
            sql = "UPDATE user_xiuxian SET exp=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (int(exp), user_id))
            self.conn.commit()

    def realm_top(self):
        """境界排行"""
        with self.lock:
            rank_mapping = {rank: idx for idx, rank in enumerate(convert_rank('江湖好手')[1])}

            sql = """SELECT user_name, level, exp FROM user_xiuxian 
                WHERE user_name IS NOT NULL
                ORDER BY exp DESC, (CASE level """

            for level, value in sorted(rank_mapping.items(), key=lambda x: x[1], reverse=True):
                sql += f"WHEN '{level}' THEN '{value:02}' "

            sql += """ELSE level END) ASC LIMIT 50"""

            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result

    def stone_top(self):
        """这也是灵石排行榜"""
        with self.lock:
            sql = f"SELECT user_name,stone FROM user_xiuxian WHERE user_name is NOT NULL ORDER BY stone DESC LIMIT 50"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result

    def power_top(self):
        """战力排行榜"""
        with self.lock:
            sql = f"SELECT user_name,power FROM user_xiuxian WHERE user_name is NOT NULL ORDER BY power DESC LIMIT 50"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result

    def scale_top(self):
        """宗门建设度排行榜"""
        with self.lock:
            sql = f"SELECT sect_id, sect_name, sect_scale FROM sects WHERE sect_owner is NOT NULL ORDER BY sect_scale DESC"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result

    def root_top(self):
        """这是轮回排行榜"""
        with self.lock:
            sql = f"SELECT user_name,root_level FROM user_xiuxian WHERE user_name is NOT NULL ORDER BY root_level DESC LIMIT 50"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result

    def get_all_sects(self):
        """获取所有宗门信息"""
        with self.lock:
            sql = f"SELECT * FROM sects WHERE sect_owner is NOT NULL"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            results = []
            columns = [column[0] for column in cur.description]
            for row in result:
                sect_dict = dict(zip(columns, row))
                results.append(sect_dict)
            return results

    def get_all_sects_with_member_count(self):
        """获取所有宗门及其各个宗门成员数"""
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT s.sect_id, s.sect_name, s.sect_scale, (SELECT user_name FROM user_xiuxian WHERE user_id = s.sect_owner) as user_name, COUNT(ux.user_id) as member_count
                FROM sects s
                LEFT JOIN user_xiuxian ux ON s.sect_id = ux.sect_id
                GROUP BY s.sect_id
            """)
            results = cur.fetchall()
            return results

    def update_user_is_beg(self, user_id, is_beg):
        """更新用户的最后奇缘时间"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "UPDATE user_xiuxian SET is_beg=? WHERE user_id=?"
            cur.execute(sql, (is_beg, user_id))
            self.conn.commit()

    def get_top1_user(self):
        """获取修为第一的用户"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select * from user_xiuxian ORDER BY exp DESC LIMIT 1"
            cur.execute(sql)
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                top1_dict = dict(zip(columns, result))
                return top1_dict
            else:
                return None

    def get_realm_top1_user(self):
        """获取境界第一的用户"""
        with self.lock:
            rank_mapping = {rank: idx for idx, rank in enumerate(convert_rank('江湖好手')[1])}

            sql = """SELECT user_name, level, exp FROM user_xiuxian 
                WHERE user_name IS NOT NULL
                ORDER BY exp DESC, (CASE level """

            for level, value in sorted(rank_mapping.items(), key=lambda x: x[1], reverse=True):
                sql += f"WHEN '{level}' THEN '{value:02}' "

            sql += """ELSE level END) ASC LIMIT 1"""

            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                top1_dict = dict(zip(columns, result))
                return top1_dict
            else:
                return None

    def donate_update(self, sect_id, stone_num):
        """宗门捐献更新建设度及可用灵石"""
        with self.lock:
            sql = f"UPDATE sects SET sect_used_stone=sect_used_stone+?,sect_scale=sect_scale+? WHERE sect_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (stone_num, stone_num * 1, sect_id))
            self.conn.commit()

    def update_sect_used_stone(self, sect_id, sect_used_stone, key):
        """更新宗门灵石储备"""
        with self.lock:
            cur = self.conn.cursor()
            if key == 1:
                sql = f"UPDATE sects SET sect_used_stone=sect_used_stone+? WHERE sect_id=?"
                cur.execute(sql, (sect_used_stone, sect_id))
                self.conn.commit()
            elif key == 2:
                sql = f"UPDATE sects SET sect_used_stone=sect_used_stone-? WHERE sect_id=?"
                cur.execute(sql, (sect_used_stone, sect_id))
                self.conn.commit()

    def update_sect_materials(self, sect_id, sect_materials, key):
        """更新资材"""
        with self.lock:
            cur = self.conn.cursor()
            if key == 1:
                sql = f"UPDATE sects SET sect_materials=sect_materials+? WHERE sect_id=?"
                cur.execute(sql, (sect_materials, sect_id))
                self.conn.commit()
            elif key == 2:
                sql = f"UPDATE sects SET sect_materials=sect_materials-? WHERE sect_id=?"
                cur.execute(sql, (sect_materials, sect_id))
                self.conn.commit()

    def get_all_sects_id_scale(self):
        """获取所有宗门信息"""
        with self.lock:
            sql = f"SELECT sect_id, sect_scale, elixir_room_level FROM sects WHERE sect_owner is NOT NULL ORDER BY sect_scale DESC"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result

    def get_all_users_by_sect_id(self, sect_id):
        """获取宗门所有成员信息"""
        with self.lock:
            sql = f"SELECT * FROM user_xiuxian WHERE sect_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (sect_id,))
            result = cur.fetchall()
            results = []
            for user in result:
                columns = [column[0] for column in cur.description]
                user_dict = dict(zip(columns, user))
                results.append(user_dict)
            return results

    def do_work(self, user_id, the_type, sc_time=None):
        """更新用户操作CD"""
        with self.lock:
            now_time = None
            if the_type == 1:
                now_time = datetime.now()
            elif the_type == 0:
                now_time = 0
            elif the_type == 2:
                now_time = datetime.now()
            elif the_type == 3:
                now_time = datetime.now()
            elif the_type == 4:
                now_time = datetime.now()

            sql = f"UPDATE user_cd SET type=?,create_time=?,scheduled_time=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (the_type, now_time, sc_time, user_id))
            self.conn.commit()

    def update_levelrate(self, user_id, rate):
        """更新突破成功率"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET level_up_rate=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (rate, user_id))
            self.conn.commit()

    def update_user_attribute(self, user_id, hp, mp, atk):
        """更新用户HP,MP,ATK信息"""
        with self.lock:
            hp = number_count(hp)
            mp = number_count(mp)
            atk = number_count(atk)
            sql = f"UPDATE user_xiuxian SET hp=?,mp=?,atk=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (hp, mp, atk, user_id))
            self.conn.commit()

    def update_user_hp_mp(self, user_id, hp, mp):
        """更新用户HP,MP信息"""
        with self.lock:
            hp = number_count(hp)
            mp = number_count(mp)
            sql = f"UPDATE user_xiuxian SET hp=?,mp=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (hp, mp, user_id))
            self.conn.commit()

    def update_user_sect_contribution(self, user_id, sect_contribution):
        """更新用户宗门贡献度"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET sect_contribution=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (sect_contribution, user_id))
            self.conn.commit()

    def deduct_sect_contribution(self, user_id, contribution):
        """扣除用户宗门贡献度"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "UPDATE user_xiuxian SET sect_contribution=sect_contribution-? WHERE user_id=?"
            cur.execute(sql, (contribution, user_id))
            self.conn.commit()

    def update_user_hp(self, user_id):
        """重置用户hp,mp信息"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET hp=exp/2,mp=exp,atk=exp/10 WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()

    def restate(self, user_id=None):
        """重置所有用户状态或重置对应人状态"""
        with self.lock:
            if user_id is None:
                sql = f"UPDATE user_xiuxian SET hp=exp/2,mp=exp,atk=exp/10"
                cur = self.conn.cursor()
                cur.execute(sql)
                self.conn.commit()
            else:
                sql = f"UPDATE user_xiuxian SET hp=exp/2,mp=exp,atk=exp/10 WHERE user_id=?"
                cur = self.conn.cursor()
                cur.execute(sql, (user_id,))
                self.conn.commit()

    def get_back_msg(self, user_id):
        """获取用户背包信息"""
        with self.lock:
            sql = f"SELECT * FROM back WHERE user_id=? and goods_num >= 1"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            result = cur.fetchall()
            if not result:
                return None

            columns = [column[0] for column in cur.description]
            results = []
            for row in result:
                back_dict = dict(zip(columns, row))
                results.append(back_dict)
            return results

    def check_and_adjust_goods_quantity(self):
        """检查并调整背包表中的物品数量"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "SELECT user_id, goods_id, goods_num, bind_num, goods_name FROM back"
            cur.execute(sql)
            results = cur.fetchall()

            processed_goods = ""
            for row in results:
                user_id, goods_id, goods_num, bind_num, goods_name = row
                if goods_num > XiuConfig().max_goods_num:
                    new_goods_num = XiuConfig().max_goods_num
                    sql_update = f"UPDATE back SET goods_num=? WHERE user_id=? AND goods_id=?"
                    cur.execute(sql_update, (new_goods_num, user_id, goods_id))
                    logger.opt(colors=True).info(f"<green>用户 {user_id} 的物品 {goods_name} 的数量已调整为 {new_goods_num}</green>")
                    processed_goods += f"{user_id} 的 {goods_name} 数量异常{goods_num}\n"

                if bind_num > XiuConfig().max_goods_num:
                    new_bind_num = XiuConfig().max_goods_num
                    sql_update = f"UPDATE back SET bind_num=? WHERE user_id=? AND goods_id=?"
                    cur.execute(sql_update, (new_bind_num, user_id, goods_id))

            self.conn.commit()
            if not processed_goods:
                return "无"
            return processed_goods

    def goods_num(self, user_id, goods_id, num_type=None):
        """判断用户物品数量"""
        with self.lock:
            sql = "SELECT goods_num, bind_num, state FROM back WHERE user_id=? and goods_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id, goods_id))
            result = cur.fetchone()
            if result:
                goods_num = result[0]
                bind_num = result[1]
                state = result[2]
                if num_type == 'bind':
                    return bind_num
                elif num_type == 'trade':
                    return goods_num - bind_num - state
                else:
                    return goods_num
            else:
                return 0

    def goods_max_num(self, goods_id):
        """返回物品的总数量"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"SELECT SUM(goods_num) FROM back WHERE goods_id=?"
            cur.execute(sql, (goods_id,))
            result = cur.fetchone()
            if result and result[0] is not None:
                return result[0]
            else:
                return 0

    def unbind_item(self, user_id, goods_id, quantity=1):
        """解绑物品，减少绑定数量"""
        with self.lock:
            try:
                cur = self.conn.cursor()
                sql = "SELECT goods_num, bind_num FROM back WHERE user_id=? AND goods_id=?"
                cur.execute(sql, (user_id, goods_id))
                result = cur.fetchone()

                if not result:
                    return False

                current_goods_num = result[0]
                current_bind_num = result[1]

                if current_bind_num < quantity:
                    return False

                new_bind_num = current_bind_num - quantity
                new_bind_num = max(0, new_bind_num)

                update_sql = "UPDATE back SET bind_num=?, update_time=? WHERE user_id=? AND goods_id=?"
                now_time = datetime.now()
                cur.execute(update_sql, (new_bind_num, now_time, user_id, goods_id))
                self.conn.commit()

                return True

            except Exception as e:
                logger.error(f"解绑物品时发生错误: {str(e)}")
                self.conn.rollback()
                return False

    def get_all_user_exp(self, level):
        """查询所有对应大境界玩家的修为"""
        with self.lock:
            sql = f"SELECT exp FROM user_xiuxian  WHERE level like '{level}%'"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            return result

    def update_user_atkpractice(self, user_id, atkpractice):
        """更新用户攻击修炼等级"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET atkpractice={atkpractice} WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()

    def update_user_hppractice(self, user_id, hppractice):
        """更新用户元血修炼等级"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET hppractice={hppractice} WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()

    def update_user_mppractice(self, user_id, mppractice):
        """更新用户灵海修炼等级"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET mppractice={mppractice} WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()

    def update_user_sect_task(self, user_id, sect_task):
        """更新用户宗门任务次数"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET sect_task=sect_task+? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (sect_task, user_id))
            self.conn.commit()

    def sect_task_reset(self):
        """重置宗门任务次数"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET sect_task=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

    def update_sect_scale_and_used_stone(self, sect_id, sect_used_stone, sect_scale):
        """更新宗门灵石、建设度"""
        with self.lock:
            sql = f"UPDATE sects SET sect_used_stone=?,sect_scale=? WHERE sect_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (sect_used_stone, sect_scale, sect_id))
            self.conn.commit()

    def update_sect_elixir_room_level(self, sect_id, level):
        """更新宗门丹房等级"""
        with self.lock:
            sql = f"UPDATE sects SET elixir_room_level=? WHERE sect_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (level, sect_id))
            self.conn.commit()

    def update_user_sect_elixir_get_num(self, user_id):
        """更新用户每日领取丹药领取次数"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET sect_elixir_get=1 WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()

    def sect_elixir_get_num_reset(self):
        """重置宗门丹药领取次数"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET sect_elixir_get=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

    def update_sect_mainbuff(self, sect_id, mainbuffid):
        """更新宗门当前的主修功法"""
        with self.lock:
            sql = f"UPDATE sects SET mainbuff=? WHERE sect_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (mainbuffid, sect_id))
            self.conn.commit()

    def update_sect_secbuff(self, sect_id, secbuffid):
        """更新宗门当前的神通"""
        with self.lock:
            sql = f"UPDATE sects SET secbuff=? WHERE sect_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (secbuffid, sect_id))
            self.conn.commit()

    def initialize_user_buff_info(self, user_id):
        """初始化用户buff信息"""
        with self.lock:
            sql = f"INSERT INTO BuffInfo (user_id,main_buff,sec_buff,effect1_buff,effect2_buff,faqi_buff,fabao_weapon) VALUES (?,0,0,0,0,0,0)"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()

    def get_user_buff_info(self, user_id):
        """获取用户buff信息"""
        with self.lock:
            sql = f"select * from BuffInfo WHERE user_id =?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                buff_dict = dict(zip(columns, result))
                return buff_dict
            else:
                return None

    def updata_user_main_buff(self, user_id, id):
        """更新用户主功法信息"""
        with self.lock:
            sql = f"UPDATE BuffInfo SET main_buff = ? WHERE user_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self.conn.commit()

    def updata_user_sub_buff(self, user_id, id):
        """更新用户辅修功法信息"""
        with self.lock:
            sql = f"UPDATE BuffInfo SET sub_buff = ? WHERE user_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self.conn.commit()

    def updata_user_sec_buff(self, user_id, id):
        """更新用户副功法信息"""
        with self.lock:
            sql = f"UPDATE BuffInfo SET sec_buff = ? WHERE user_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self.conn.commit()

    def updata_user_effect1_buff(self, user_id, id):
        """更新用户身法信息"""
        with self.lock:
            sql = f"UPDATE BuffInfo SET effect1_buff = ? WHERE user_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self.conn.commit()

    def updata_user_effect2_buff(self, user_id, id):
        """更新用户瞳术信息"""
        with self.lock:
            sql = f"UPDATE BuffInfo SET effect2_buff = ? WHERE user_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self.conn.commit()

    def updata_user_faqi_buff(self, user_id, id):
        """更新用户法器信息"""
        with self.lock:
            sql = f"UPDATE BuffInfo SET faqi_buff = ? WHERE user_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self.conn.commit()

    def updata_user_fabao_weapon(self, user_id, id):
        """更新用户法宝信息"""
        with self.lock:
            sql = f"UPDATE BuffInfo SET fabao_weapon = ? WHERE user_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self.conn.commit()

    def updata_user_armor_buff(self, user_id, id):
        """更新用户防具信息"""
        with self.lock:
            sql = f"UPDATE BuffInfo SET armor_buff = ? WHERE user_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (id, user_id,))
            self.conn.commit()

    def updata_user_atk_buff(self, user_id, buff):
        """更新用户永久攻击buff信息"""
        with self.lock:
            sql = f"UPDATE BuffInfo SET atk_buff=atk_buff+? WHERE user_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (buff, user_id,))
            self.conn.commit()

    def updata_user_blessed_spot(self, user_id, blessed_spot):
        """更新用户洞天福地等级"""
        with self.lock:
            sql = f"UPDATE BuffInfo SET blessed_spot=? WHERE user_id = ?"
            cur = self.conn.cursor()
            cur.execute(sql, (blessed_spot, user_id,))
            self.conn.commit()

    def update_user_blessed_spot_flag(self, user_id):
        """更新用户洞天福地是否开启"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET blessed_spot_flag=1 WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()

    def update_user_blessed_spot_name(self, user_id, blessed_spot_name):
        """更新用户洞天福地的名字"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET blessed_spot_name=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (blessed_spot_name, user_id,))
            self.conn.commit()

    def day_num_reset(self):
        """重置丹药每日使用次数"""
        with self.lock:
            sql = f"UPDATE back SET day_num=0 where goods_type='丹药'"
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

    def mixelixir_num_reset(self):
        """重置每日炼丹次数"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET mixelixir_num=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

    def reset_work_num(self, count):
        """重置用户悬赏令刷新次数"""
        with self.lock:
            sql = f"UPDATE user_xiuxian SET work_num=?"
            cur = self.conn.cursor()
            cur.execute(sql, (count,))
            self.conn.commit()

    def get_work_num(self, user_id):
        """获取用户悬赏令刷新次数"""
        with self.lock:
            sql = f"SELECT work_num FROM user_xiuxian WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result:
                work_num = result[0]
            return work_num

    def update_work_num(self, user_id, work_num):
        with self.lock:
            sql = f"UPDATE user_xiuxian SET work_num=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (work_num, user_id,))
            self.conn.commit()

    def send_back(self, user_id, goods_id, goods_name, goods_type, goods_num, bind_flag=0):
        """插入物品至背包"""
        with self.lock:
            now_time = datetime.now()
            max_goods_num = int(XiuConfig().max_goods_num)
            goods_num = min(abs(int(goods_num)), max_goods_num)

            cur = self.conn.cursor()
            back = self.get_item_by_good_id_and_user_id(user_id, goods_id)

            if back:
                if bind_flag == 1:
                    bind_num = back['bind_num'] + goods_num
                else:
                    bind_num = min(back['bind_num'], back['goods_num'])

                goods_nums = min(back['goods_num'] + goods_num, max_goods_num)
                bind_num = min(bind_num, max_goods_num, goods_nums)

                sql = "UPDATE back SET goods_num=?, update_time=?, bind_num=? WHERE user_id=? and goods_id=?"
                cur.execute(sql, (goods_nums, now_time, bind_num, user_id, goods_id))
                self.conn.commit()
            else:
                bind_num = goods_num if bind_flag == 1 else 0
                sql = """
                    INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num)
                    VALUES (?,?,?,?,?,?,?,?)
                """
                cur.execute(sql, (user_id, goods_id, goods_name, goods_type, goods_num, now_time, now_time, bind_num))
                self.conn.commit()

    def update_back_j(self, user_id, goods_id, num=1, use_key=0):
        """使用物品"""
        with self.lock:
            num = abs(int(num))
            user_id = str(user_id)
            goods_id = int(goods_id)

            back = self.get_item_by_good_id_and_user_id(user_id, goods_id)
            if not back:
                return
            if num <= 0:
                return
            if back['goods_num'] < num:
                num = back['goods_num']
            if num <= 0:
                return

            if back['goods_type'] == "丹药" and use_key == 1:
                bind_num = back['bind_num'] - num if back['bind_num'] >= num else back['bind_num']
                day_num = back['day_num'] + num
                all_num = back['all_num'] + num
            else:
                bind_num = back['bind_num'] - num if back['bind_num'] >= num else back['bind_num']
                day_num = back['day_num']
                all_num = back['all_num']

            goods_num = back['goods_num'] - num
            if goods_num <= 0:
                goods_num = 0
                bind_num = 0

            bind_num = min(bind_num, goods_num)
            bind_num = max(bind_num, 0)

            now_time = datetime.now()
            sql_str = """
                UPDATE back
                SET update_time=?,
                    action_time=?,
                    goods_num=?,
                    day_num=?,
                    all_num=?,
                    bind_num=?
                WHERE user_id=? AND goods_id=?
            """
            cur = self.conn.cursor()
            cur.execute(sql_str, (now_time, now_time, goods_num, day_num, all_num, bind_num, user_id, goods_id))
            self.conn.commit()

    def get_item_by_good_id_and_user_id(self, user_id, goods_id):
        """根据物品id、用户id获取物品信息"""
        with self.lock:
            sql = "select * from back WHERE user_id=? and goods_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (str(user_id), int(goods_id)))
            result = cur.fetchone()
            if not result:
                return None

            columns = [column[0] for column in cur.description]
            item_dict = dict(zip(columns, result))
            return item_dict

    def update_back_equipment(self, sql_str, params=None):
        with self.lock:
            cur = self.conn.cursor()
            if isinstance(sql_str, tuple) and params is None:
                sql_str, params = sql_str
            if params is not None:
                cur.execute(sql_str, params)
            else:
                cur.execute(sql_str)
            self.conn.commit()

    def reset_user_drug_resistance(self, user_id):
        """重置用户耐药性"""
        with self.lock:
            sql = "UPDATE back SET all_num=0 WHERE goods_type='丹药' AND user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (str(user_id),))
            self.conn.commit()

    def _ensure_puppet_column(self):
        """确保 user_xiuxian 表中存在 puppet_status 字段，没有就创建"""
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("PRAGMA table_info(user_xiuxian)")
            columns = [row[1] for row in cur.fetchall()]
            if "puppet_status" not in columns:
                cur.execute("ALTER TABLE user_xiuxian ADD COLUMN puppet_status INTEGER DEFAULT 0")
                self.conn.commit()

    def check_puppet_status(self, user_id):
        """查询灵田傀儡状态，没有字段会自动创建"""
        with self.lock:
            self._ensure_puppet_column()
            sql = "SELECT puppet_status FROM user_xiuxian WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if not result:
                return 0
            return result[0]

    def set_puppet_status(self, user_id, status):
        """设置灵田傀儡状态 status: 0关闭 1开启"""
        with self.lock:
            self._ensure_puppet_column()
            sql = "UPDATE user_xiuxian SET puppet_status=? WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (status, user_id))
            self.conn.commit()

    def get_all_enabled_puppets(self):
        """获取所有开启灵田傀儡的玩家 user_id 列表"""
        with self.lock:
            self._ensure_puppet_column()
            sql = "SELECT user_id FROM user_xiuxian WHERE puppet_status=1"
            cur = self.conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            return [r[0] for r in rows]


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
        list_all = len(self.level) - 1
        now_index = self.level.index(user_level)
        if list_all == now_index:
            need_exp = 0.001
        else:
            is_updata_level = self.level[now_index + 1]
            need_exp = XiuxianDateManage().get_level_power(is_updata_level)
        return need_exp

    def get_type(self, user_exp, rate, user_level):
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
        """计算日期差"""
        if isinstance(new_time, datetime):
            pass
        else:
            new_time = datetime.strptime(new_time, '%Y-%m-%d %H:%M:%S.%f')

        if isinstance(old_time, datetime):
            pass
        else:
            old_time = datetime.strptime(old_time, '%Y-%m-%d %H:%M:%S.%f')

        day = (new_time - old_time).days
        sec = (new_time - old_time).seconds

        return (day * 24 * 60 * 60) + sec

    def get_power_rate(self, mind, other):
        power_rate = mind / (other + mind)
        if power_rate >= 0.8:
            return "道友偷窃小辈实属天道所不齿！"
        elif power_rate <= 0.05:
            return "道友请不要不自量力！"
        else:
            return int(power_rate * 100)

    def player_fight(self, player1: dict, player2: dict):
        """
        回合制战斗
        type_in : 1 为完整返回战斗过程（未加）
        2：只返回战斗结果
        数据示例：
        {"道号": None, "气血": None, "攻击": None, "真元": None, '会心':None}
        """
        msg1 = "{}发起攻击，造成了{}伤害\n"
        msg2 = "{}发起攻击，造成了{}伤害\n"

        play_list = []
        suc = None
        if player1['气血'] <= 0:
            player1['气血'] = 1
        if player2['气血'] <= 0:
            player2['气血'] = 1
        while True:
            player1_gj = int(round(random.uniform(0.95, 1.05), 2) * player1['攻击'])
            if random.randint(0, 100) <= player1['会心']:
                player1_gj = int(player1_gj * player1['爆伤'])
                msg1 = "{}发起会心一击，造成了{}伤害\n"

            player2_gj = int(round(random.uniform(0.95, 1.05), 2) * player2['攻击'])
            if random.randint(0, 100) <= player2['会心']:
                player2_gj = int(player2_gj * player2['爆伤'])
                msg2 = "{}发起会心一击，造成了{}伤害\n"

            play1_sh: int = int(player1_gj * (1 - player2['防御']))
            play2_sh: int = int(player2_gj * (1 - player1['防御']))

            play_list.append(msg1.format(player1['道号'], play1_sh))
            player2['气血'] = player2['气血'] - play1_sh
            play_list.append(f"{player2['道号']}剩余血量{player2['气血']}")
            XiuxianDateManage().update_user_hp_mp(player2['user_id'], player2['气血'], player2['真元'])

            if player2['气血'] <= 0:
                play_list.append(f"{player1['道号']}胜利")
                suc = f"{player1['道号']}"
                XiuxianDateManage().update_user_hp_mp(player2['user_id'], 1, player2['真元'])
                break

            play_list.append(msg2.format(player2['道号'], play2_sh))
            player1['气血'] = player1['气血'] - play2_sh
            play_list.append(f"{player1['道号']}剩余血量{player1['气血']}\n")
            XiuxianDateManage().update_user_hp_mp(player1['user_id'], 1, player1['真元'])

            if player1['气血'] <= 0:
                play_list.append(f"{player2['道号']}胜利")
                suc = f"{player2['道号']}"
                XiuxianDateManage().update_user_hp_mp(player1['user_id'], 1, player1['真元'])
                break
            if player1['气血'] <= 0 or player2['气血'] <= 0:
                play_list.append("逻辑错误！！！")
                break

        return play_list, suc

    def send_hp_mp(self, user_id, hp, mp):
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

# 这里是交易数据部分
class TradeDataManager:
    global trade_num
    _instance = {}
    _has_init = {}

    def __new__(cls):
        if cls._instance.get(trade_num) is None:
            cls._instance[trade_num] = super(TradeDataManager, cls).__new__(cls)
        return cls._instance[trade_num]

    def __init__(self):
        if not self._has_init.get(trade_num):
            self._has_init[trade_num] = True
            self.database_path = DATABASE
            self.trade_db_path = self.database_path / "trade.db"
            if not self.trade_db_path.exists():
                self.trade_db_path.touch()
                logger.opt(colors=True).info("<green>trade数据库已创建！</green>")
            self.conn = sqlite3.connect(self.trade_db_path, check_same_thread=False)
            self.lock = threading.RLock()
            self._check_data()

    def _check_data(self):
        """检查数据完整性"""
        with self.lock:
            c = self.conn.cursor()

            # 仙肆商品表
            c.execute("""
                CREATE TABLE IF NOT EXISTS xianshi_item (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    goods_id INTEGER,
                    name TEXT,
                    type TEXT,
                    price INTEGER,
                    quantity INTEGER
                )
            """)

            # 鬼市订单表（item_type: qiugou/baitan；兼容历史中文）
            c.execute("""
                CREATE TABLE IF NOT EXISTS guishi_item (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    item_id INTEGER,
                    item_name TEXT,
                    item_type TEXT,
                    price INTEGER,
                    quantity INTEGER,
                    filled_quantity INTEGER DEFAULT 0
                )
            """)

            # 鬼市账户（灵石+暂存物品）
            c.execute("""
                CREATE TABLE IF NOT EXISTS guishi_info (
                    user_id TEXT PRIMARY KEY,
                    stored_stone INTEGER DEFAULT 0,
                    items TEXT DEFAULT '{}'
                )
            """)

            # 玩家拍卖等待区
            c.execute("""
                CREATE TABLE IF NOT EXISTS auction_player_upload (
                    user_id TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL,
                    start_price INTEGER NOT NULL,
                    user_name TEXT NOT NULL,
                    PRIMARY KEY (user_id, item_id)
                )
            """)

            # 当前拍卖表
            c.execute("""
                CREATE TABLE IF NOT EXISTS auction_current (
                    id TEXT PRIMARY KEY,
                    item_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    start_price INTEGER NOT NULL,
                    current_price INTEGER NOT NULL,
                    seller_id TEXT NOT NULL,
                    seller_name TEXT NOT NULL,
                    bids TEXT DEFAULT '{}',
                    bid_times TEXT DEFAULT '{}',
                    is_system INTEGER DEFAULT 0,
                    last_bid_time REAL DEFAULT NULL
                )
            """)

            # 拍卖历史
            c.execute("""
                CREATE TABLE IF NOT EXISTS auction_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    auction_id TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL,
                    start_price INTEGER NOT NULL,
                    final_price INTEGER,
                    seller_id TEXT NOT NULL,
                    seller_name TEXT NOT NULL,
                    winner_id TEXT,
                    winner_name TEXT,
                    status TEXT NOT NULL,
                    fee INTEGER,
                    seller_earnings INTEGER,
                    start_time REAL NOT NULL,
                    end_time REAL NOT NULL
                )
            """)
            self.conn.commit()

    def total_goods_quantity(self):
        """获取全部仙肆物品总数（不含系统无限）"""
        with self.lock:
            cur = self.conn.cursor()
            sql = """
                SELECT SUM(quantity) AS total_quantity
                FROM (
                    SELECT quantity FROM xianshi_item WHERE user_id != '0' AND quantity > 0
                    UNION ALL
                    SELECT quantity FROM guishi_item WHERE user_id != '0' AND (item_type='baitan' OR item_type='摆摊') AND quantity > 0
                )
            """
            cur.execute(sql)
            result = cur.fetchone()
            return int(result[0]) if result and result[0] is not None else 0

    def generate_unique_id(self, table_name):
        """生成唯一ID（字符串）"""
        with self.lock:
            cur = self.conn.cursor()
            for _ in range(200):
                first = str(random.randint(1, 9))
                rest = ''.join(random.choices(string.digits, k=random.randint(7, 11)))
                uid = first + rest
                cur.execute(f"SELECT 1 FROM {table_name} WHERE id = ?", (uid,))
                if not cur.fetchone():
                    return uid
            return f"{int(time.time() * 1000)}{random.randint(100, 999)}"

    # ======== 仙肆 ========

    def add_xianshi_item(self, user_id, goods_id, name, type, price, quantity):
        with self.lock:
            unique_id = self.generate_unique_id("xianshi_item")
            sql = """
                INSERT INTO xianshi_item (id, user_id, goods_id, name, type, price, quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            self.conn.execute(
                sql,
                (str(unique_id), str(user_id), int(goods_id), str(name), str(type), int(price), int(quantity))
            )
            self.conn.commit()

    def remove_xianshi_item(self, item_id):
        """
        删除仙肆物品：
        - quantity == -1 视为系统无限库存，不删除
        - quantity == 1 删除记录
        - quantity > 1 数量减1
        """
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT quantity FROM xianshi_item WHERE id = ?", (str(item_id),))
            row = cur.fetchone()
            if not row:
                return False

            qty = int(row[0])
            if qty == -1:
                return True
            if qty <= 1:
                self.conn.execute("DELETE FROM xianshi_item WHERE id = ?", (str(item_id),))
            else:
                self.conn.execute("UPDATE xianshi_item SET quantity=? WHERE id=?", (qty - 1, str(item_id)))
            self.conn.commit()
            return True

    def remove_xianshi_all_item(self, item_id):
        with self.lock:
            self.conn.execute("DELETE FROM xianshi_item WHERE id = ?", (str(item_id),))
            self.conn.commit()

    def get_xianshi_items(self, user_id=None, type=None, id=None, name=None):
        with self.lock:
            conditions = []
            params = []

            if user_id is not None:
                conditions.append("user_id = ?")
                params.append(str(user_id))
            if type:
                conditions.append("type = ?")
                params.append(str(type))
            if id:
                conditions.append("id = ?")
                params.append(str(id))
            if name:
                conditions.append("name = ?")
                params.append(str(name))

            q = "SELECT * FROM xianshi_item"
            if conditions:
                q += " WHERE " + " AND ".join(conditions)

            cur = self.conn.cursor()
            cur.execute(q, params)
            rows = cur.fetchall()
            if not rows:
                return None
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]

    # ======== 鬼市 ========

    def add_guishi_order(self, user_id, item_id, item_name, item_type, price, quantity):
        with self.lock:
            uid = self.generate_unique_id("guishi_item")
            sql = """
                INSERT INTO guishi_item (id, user_id, item_id, item_name, item_type, price, quantity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            self.conn.execute(sql, (
                str(uid), str(user_id), int(item_id), str(item_name), str(item_type), int(price), int(quantity)
            ))
            self.conn.commit()
            return uid

    def remove_guishi_order(self, order_id):
        with self.lock:
            self.conn.execute("DELETE FROM guishi_item WHERE id = ?", (str(order_id),))
            self.conn.commit()

    def increase_filled_quantity(self, order_id, amount):
        with self.lock:
            self.conn.execute(
                "UPDATE guishi_item SET filled_quantity = filled_quantity + ? WHERE id = ?",
                (int(amount), str(order_id))
            )
            self.conn.commit()

    def get_guishi_orders(self, user_id=None, name=None, type=None, id=None):
        """
        兼容：
        - type='qiugou' -> 匹配 qiugou/求购
        - type='baitan' -> 匹配 baitan/摆摊
        """
        with self.lock:
            cond = []
            params = []

            if user_id is not None:
                cond.append("user_id = ?")
                params.append(str(user_id))
            if name:
                cond.append("item_name = ?")
                params.append(str(name))
            if type:
                if type == "qiugou":
                    cond.append("(item_type = ? OR item_type = ?)")
                    params.extend(["qiugou", "求购"])
                elif type == "baitan":
                    cond.append("(item_type = ? OR item_type = ?)")
                    params.extend(["baitan", "摆摊"])
                else:
                    cond.append("item_type = ?")
                    params.append(str(type))
            if id:
                cond.append("id = ?")
                params.append(str(id))

            q = "SELECT * FROM guishi_item"
            if cond:
                q += " WHERE " + " AND ".join(cond)

            cur = self.conn.cursor()
            cur.execute(q, params)
            rows = cur.fetchall()
            if not rows:
                return None
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]

    def get_stored_stone(self, user_id):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT stored_stone FROM guishi_info WHERE user_id = ?", (str(user_id),))
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def get_stored_items(self, user_id):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT items FROM guishi_info WHERE user_id = ?", (str(user_id),))
            row = cur.fetchone()
            if not row:
                return {}
            raw = row[0] if row[0] else "{}"
            try:
                obj = json.loads(raw)
                return obj if isinstance(obj, dict) else {}
            except Exception:
                return {}

    def add_stored_item(self, user_id, item_id, quantity=1):
        with self.lock:
            user_id = str(user_id)
            item_id = str(item_id)
            quantity = int(quantity)

            current = self.get_stored_items(user_id)
            current[item_id] = int(current.get(item_id, 0)) + quantity
            payload = json.dumps(current, ensure_ascii=False)

            cur = self.conn.cursor()
            cur.execute("SELECT 1 FROM guishi_info WHERE user_id = ?", (user_id,))
            if cur.fetchone():
                cur.execute("UPDATE guishi_info SET items=? WHERE user_id=?", (payload, user_id))
            else:
                cur.execute(
                    "INSERT INTO guishi_info (user_id, stored_stone, items) VALUES (?, 0, ?)",
                    (user_id, payload)
                )
            self.conn.commit()

    def remove_stored_item(self, user_id, item_id):
        with self.lock:
            user_id = str(user_id)
            item_id = str(item_id)

            cur_items = self.get_stored_items(user_id)
            if item_id in cur_items:
                del cur_items[item_id]
                payload = json.dumps(cur_items, ensure_ascii=False)
                self.conn.execute("UPDATE guishi_info SET items=? WHERE user_id=?", (payload, user_id))
                self.conn.commit()

    def update_stored_stone(self, user_id, amount, operation):
        """
        operation: add / subtract
        subtract 下限0
        """
        with self.lock:
            user_id = str(user_id)
            amount = int(amount)

            cur = self.conn.cursor()
            cur.execute("SELECT stored_stone FROM guishi_info WHERE user_id=?", (user_id,))
            row = cur.fetchone()

            if row is None:
                init_val = amount if operation == "add" else 0
                cur.execute(
                    "INSERT INTO guishi_info (user_id, stored_stone, items) VALUES (?, ?, '{}')",
                    (user_id, init_val)
                )
                self.conn.commit()
                return

            old = int(row[0])
            if operation == "add":
                newv = old + amount
            else:
                newv = max(old - amount, 0)

            cur.execute("UPDATE guishi_info SET stored_stone=? WHERE user_id=?", (newv, user_id))
            self.conn.commit()

    # ======== 拍卖等待区 ========

    def add_player_auction_item(self, user_id, item_id, item_name, start_price, user_name):
        with self.lock:
            sql = """
                INSERT INTO auction_player_upload (user_id, item_id, item_name, start_price, user_name)
                VALUES (?, ?, ?, ?, ?)
            """
            self.conn.execute(sql, (str(user_id), int(item_id), str(item_name), int(start_price), str(user_name)))
            self.conn.commit()

    def get_player_auction_items(self, user_id=None):
        with self.lock:
            cur = self.conn.cursor()
            if user_id is None:
                cur.execute("SELECT user_id, item_id, item_name, start_price, user_name FROM auction_player_upload")
            else:
                cur.execute(
                    "SELECT user_id, item_id, item_name, start_price, user_name FROM auction_player_upload WHERE user_id = ?",
                    (str(user_id),)
                )
            rows = cur.fetchall()
            cols = ["user_id", "item_id", "item_name", "start_price", "user_name"]
            return [dict(zip(cols, r)) for r in rows]

    def remove_player_auction_item(self, user_id, item_id):
        with self.lock:
            self.conn.execute(
                "DELETE FROM auction_player_upload WHERE user_id = ? AND item_id = ?",
                (str(user_id), int(item_id))
            )
            self.conn.commit()

    def clear_player_auctions(self):
        with self.lock:
            self.conn.execute("DELETE FROM auction_player_upload")
            self.conn.commit()

    # ======== 当前拍卖 ========

    def set_current_auction(self, auction_items: list):
        with self.lock:
            self.clear_current_auction()
            cur = self.conn.cursor()
            for x in auction_items:
                bids_json = json.dumps(x.get("bids", {}), ensure_ascii=False)
                bid_times_json = json.dumps(x.get("bid_times", {}), ensure_ascii=False)
                cur.execute("""
                    INSERT INTO auction_current
                    (id, item_id, name, start_price, current_price, seller_id, seller_name, bids, bid_times, is_system, last_bid_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(x["id"]),
                    int(x["item_id"]),
                    str(x["name"]),
                    int(x["start_price"]),
                    int(x["current_price"]),
                    str(x["seller_id"]),
                    str(x["seller_name"]),
                    bids_json,
                    bid_times_json,
                    1 if bool(x.get("is_system", False)) else 0,
                    float(x.get("last_bid_time")) if x.get("last_bid_time") is not None else None
                ))
            self.conn.commit()

    def get_current_auction(self, auction_id=None):
        with self.lock:
            cur = self.conn.cursor()
            if auction_id is None:
                cur.execute("SELECT * FROM auction_current")
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                out = []
                for r in rows:
                    d = dict(zip(cols, r))
                    try:
                        d["bids"] = json.loads(d["bids"]) if d["bids"] else {}
                    except Exception:
                        d["bids"] = {}
                    try:
                        d["bid_times"] = json.loads(d["bid_times"]) if d["bid_times"] else {}
                    except Exception:
                        d["bid_times"] = {}
                    d["is_system"] = bool(d["is_system"])
                    out.append(d)
                return out
            else:
                cur.execute("SELECT * FROM auction_current WHERE id=?", (str(auction_id),))
                r = cur.fetchone()
                if not r:
                    return None
                cols = [c[0] for c in cur.description]
                d = dict(zip(cols, r))
                try:
                    d["bids"] = json.loads(d["bids"]) if d["bids"] else {}
                except Exception:
                    d["bids"] = {}
                try:
                    d["bid_times"] = json.loads(d["bid_times"]) if d["bid_times"] else {}
                except Exception:
                    d["bid_times"] = {}
                d["is_system"] = bool(d["is_system"])
                return d

    def update_auction_bid(self, auction_id, new_current_price, new_bids, new_bid_times, new_last_bid_time):
        with self.lock:
            sql = """
                UPDATE auction_current
                SET current_price=?, bids=?, bid_times=?, last_bid_time=?
                WHERE id=?
            """
            self.conn.execute(
                sql,
                (
                    int(new_current_price),
                    json.dumps(new_bids or {}, ensure_ascii=False),
                    json.dumps(new_bid_times or {}, ensure_ascii=False),
                    float(new_last_bid_time) if new_last_bid_time is not None else None,
                    str(auction_id)
                )
            )
            self.conn.commit()

    def clear_current_auction(self):
        with self.lock:
            self.conn.execute("DELETE FROM auction_current")
            self.conn.commit()

    # ======== 拍卖历史 ========

    def add_auction_history_record(self, record: dict):
        with self.lock:
            sql = """
                INSERT INTO auction_history
                (auction_id, item_id, item_name, start_price, final_price, seller_id, seller_name,
                 winner_id, winner_name, status, fee, seller_earnings, start_time, end_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.conn.execute(sql, (
                str(record["auction_id"]),
                int(record["item_id"]),
                str(record["item_name"]),
                int(record["start_price"]),
                int(record["final_price"]) if record.get("final_price") is not None else None,
                str(record["seller_id"]),
                str(record["seller_name"]),
                str(record["winner_id"]) if record.get("winner_id") is not None else None,
                str(record["winner_name"]) if record.get("winner_name") is not None else None,
                str(record["status"]),
                int(record["fee"]) if record.get("fee") is not None else None,
                int(record["seller_earnings"]) if record.get("seller_earnings") is not None else None,
                float(record["start_time"]),
                float(record["end_time"])
            ))
            self.conn.commit()

    def get_auction_history(self, auction_id=None):
        with self.lock:
            cur = self.conn.cursor()
            if auction_id is None:
                cur.execute("SELECT * FROM auction_history ORDER BY end_time DESC")
            else:
                cur.execute("SELECT * FROM auction_history WHERE auction_id=? ORDER BY end_time DESC", (str(auction_id),))
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, r)) for r in rows]

    def close(self):
        with self.lock:
            if getattr(self, "conn", None):
                self.conn.close()
                self.conn = None
                logger.opt(colors=True).info("<green>trade数据库关闭！</green>")

# 这里是Player部分
class PlayerDataManager:
    global player_num
    _instance = {}
    _has_init = {}

    def __new__(cls):
        if cls._instance.get(player_num) is None:
            cls._instance[player_num] = super(PlayerDataManager, cls).__new__(cls)
        return cls._instance[player_num]

    def __init__(self):
        if not self._has_init.get(player_num):
            self._has_init[player_num] = True
            self.database_path = DATABASE / "player.db"
            if not self.database_path.parent.exists():
                self.database_path.parent.mkdir(parents=True)

            if not self.database_path.exists():
                self.database_path.touch()
                logger.opt(colors=True).info(f"<green>player数据库已创建！</green>")

            # 持久连接
            self.conn = sqlite3.connect(self.database_path, check_same_thread=False)
            self.lock = threading.RLock()
            logger.opt(colors=True).info(f"<green>player数据库已连接！</green>")

    def _ensure_database_exists(self):
        if not self.database_path.exists():
            logger.opt(colors=True).info(f"<green>player数据库不存在，正在创建...</green>")
            self.database_path.touch()
            logger.opt(colors=True).info(f"<green>player数据库已创建！</green>")

    def _get_cursor(self):
        return self.conn.cursor()

    def _ensure_table_exists(self, table_name):
        with self.lock:
            cursor = self._get_cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            if cursor.fetchone() is None:
                cursor.execute(f"CREATE TABLE {table_name} (user_id TEXT PRIMARY KEY)")
                logger.opt(colors=True).info(f"<green>表 {table_name} 已创建！</green>")
            self.conn.commit()

    def _ensure_field_exists(self, table_name, field, data_type='TEXT'):
        with self.lock:
            cursor = self._get_cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            fields = [col[1] for col in cursor.fetchall()]
            if field not in fields:
                cursor.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN {field} {data_type} DEFAULT NULL"
                )
                logger.opt(colors=True).info(
                    f"<green>字段 {field} 已添加到表 {table_name}，类型为 {data_type}！</green>"
                )
            self.conn.commit()

    def update_or_write_data(self, user_id, table_name, field, value, data_type='TEXT'):
        dt = str(data_type).upper().strip() if data_type is not None else "TEXT"
        alias = {
            "INT": "INTEGER",
            "STR": "TEXT",
            "STRING": "TEXT",
            "FLOAT": "REAL",
            "DOUBLE": "REAL",
            "BOOL": "NUMERIC",
            "BOOLEAN": "NUMERIC",
        }
        data_type = alias.get(dt, dt)

        if data_type not in ['INTEGER', 'REAL', 'TEXT', 'BLOB', 'NUMERIC']:
            logger.warning(f"不支持的数据类型: {data_type} 已设置为默认类型：TEXT")
            data_type = 'TEXT'

        self._ensure_table_exists(table_name)
        self._ensure_field_exists(table_name, field, data_type)

        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        else:
            value = str(value)

        with self.lock:
            cursor = self._get_cursor()
            cursor.execute(
                f"UPDATE {table_name} SET {field}=? WHERE user_id=?",
                (value, str(user_id))
            )
            if cursor.rowcount == 0:
                cursor.execute(
                    f"INSERT INTO {table_name} (user_id, {field}) VALUES (?, ?)",
                    (str(user_id), value)
                )
            self.conn.commit()

    def get_fields(self, user_id, table_name):
        """通过user_id查看一个表这个主键的全部字段"""
        if user_id is None:
            logger.warning(f"尝试获取表 {table_name} 的字段数据但 user_id 为 None")
            return None

        self._ensure_table_exists(table_name)

        with self.lock:
            cursor = self._get_cursor()
            try:
                cursor.execute(f"SELECT * FROM {table_name} WHERE user_id=?", (str(user_id),))
                result = cursor.fetchone()
            except Exception as e:
                logger.error(f"查询表 {table_name} user_id {user_id} 时出错: {e}")
                return None

            if result is None:
                return None

            columns = [column[0] for column in cursor.description]
            user_dict = {}
            for col, val in zip(columns, result):
                if isinstance(val, str):
                    try:
                        user_dict[col] = json.loads(val)
                    except json.JSONDecodeError:
                        user_dict[col] = val
                else:
                    user_dict[col] = val

            return user_dict

    def get_field_data(self, user_id, table_name, field):
        if user_id is None:
            logger.warning(f"尝试获取表 {table_name} 字段 {field} 的数据但 user_id 为 None")
            return None

        self._ensure_table_exists(table_name)
        self._ensure_field_exists(table_name, field)

        with self.lock:
            cursor = self._get_cursor()
            cursor.execute(f"SELECT {field} FROM {table_name} WHERE user_id=?", (str(user_id),))
            result = cursor.fetchone()

            if result and result[0] is not None:
                val = result[0]
                if isinstance(val, str):
                    try:
                        return json.loads(val)
                    except json.JSONDecodeError:
                        pass
                return val
            return None

    def get_all_field_data(self, table_name, field):
        self._ensure_table_exists(table_name)
        self._ensure_field_exists(table_name, field)

        with self.lock:
            cursor = self._get_cursor()
            cursor.execute(f"SELECT user_id, {field} FROM {table_name}")
            result = cursor.fetchall()

            processed_results = []
            for user_id_str, val in result:
                if isinstance(val, str):
                    try:
                        processed_results.append((user_id_str, json.loads(val)))
                    except json.JSONDecodeError:
                        processed_results.append((user_id_str, val))
                else:
                    processed_results.append((user_id_str, val))
            return processed_results

    def update_all_records(self, table_name, field, value, data_type='TEXT'):
        """
        更新指定表中所有记录的某个字段的值
        """
        if data_type not in ['INTEGER', 'REAL', 'TEXT', 'BLOB', 'NUMERIC']:
            logger.warning(f"<yellow>Unsupported data type: {data_type}. Defaulting to TEXT.</yellow>")
            data_type = 'TEXT'

        self._ensure_table_exists(table_name)
        self._ensure_field_exists(table_name, field, data_type=data_type)

        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        else:
            value = str(value)

        with self.lock:
            cursor = self._get_cursor()
            cursor.execute(f"UPDATE {table_name} SET {field}=?", (value,))
            self.conn.commit()

    def get_all_records(self, table_name) -> list[dict]:
        """
        获取指定表中的所有记录
        """
        self._ensure_table_exists(table_name)

        with self.lock:
            cursor = self._get_cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            results = []
            for row in rows:
                record = {}
                for col, val in zip(columns, row):
                    if isinstance(val, str):
                        try:
                            record[col] = json.loads(val)
                        except json.JSONDecodeError:
                            record[col] = val
                    else:
                        record[col] = val
                results.append(record)
            return results

    def delete_record(self, user_id, table_name):
        """
        删除指定 user_id 在指定表中的记录
        """
        self._ensure_table_exists(table_name)

        with self.lock:
            cursor = self._get_cursor()
            cursor.execute(f"DELETE FROM {table_name} WHERE user_id=?", (str(user_id),))
            self.conn.commit()

    def close(self):
        with self.lock:
            if getattr(self, "conn", None):
                self.conn.close()
                self.conn = None
                logger.opt(colors=True).info(f"<green>player数据库已关闭！</green>")
    
# 这里是虚神界部分
class XIUXIAN_IMPART_BUFF:
    global impart_num
    _instance = {}
    _has_init = {}

    def __new__(cls):
        if cls._instance.get(impart_num) is None:
            cls._instance[impart_num] = super(XIUXIAN_IMPART_BUFF, cls).__new__(cls)
        return cls._instance[impart_num]

    def __init__(self):
        if not self._has_init.get(impart_num):
            self._has_init[impart_num] = True
            self.database_path = DATABASE
            self.db_file = self.database_path / "xiuxian_impart.db"
            if not self.database_path.exists():
                self.database_path.mkdir(parents=True)

            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self.lock = threading.RLock()
            logger.opt(colors=True).info(f"<green>xiuxian_impart数据库已连接!</green>")
            self._check_data()

    def close(self):
        with self.lock:
            if getattr(self, "conn", None):
                self.conn.close()
                self.conn = None
                logger.opt(colors=True).info(f"<green>xiuxian_impart数据库关闭!</green>")

    def _create_file(self) -> None:
        """创建数据库文件"""
        with self.lock:
            c = self.conn.cursor()
            c.execute('''CREATE TABLE xiuxian_impart
                               (NO            INTEGER PRIMARY KEY UNIQUE,
                               USERID         TEXT     ,
                               level          INTEGER  ,
                               root           INTEGER
                               );''')
            c.execute('''''')
            c.execute('''''')
            self.conn.commit()

    def _check_data(self):
        """检查数据完整性"""
        with self.lock:
            c = self.conn.cursor()

            for i in config_impart.sql_table:
                if i == "xiuxian_impart":
                    try:
                        c.execute(f"select count(1) from {i}")
                    except sqlite3.OperationalError:
                        c.execute(f"""CREATE TABLE "xiuxian_impart" (
        "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
        "user_id" TEXT DEFAULT 0,
        "impart_hp_per" integer DEFAULT 0,
        "impart_atk_per" integer DEFAULT 0,
        "impart_mp_per" integer DEFAULT 0,
        "impart_exp_up" integer DEFAULT 0,
        "boss_atk" integer DEFAULT 0,
        "impart_know_per" integer DEFAULT 0,
        "impart_burst_per" integer DEFAULT 0,
        "impart_mix_per" integer DEFAULT 0,
        "impart_reap_per" integer DEFAULT 0,
        "impart_two_exp" integer DEFAULT 0,
        "stone_num" integer DEFAULT 0,
        "impart_lv" integer DEFAULT 0,
        "impart_num" integer DEFAULT 0,
        "exp_day" integer DEFAULT 0,
        "wish" integer DEFAULT 0
        );""")

            for s in config_impart.sql_table_impart_buff:
                try:
                    c.execute(f"select {s} from xiuxian_impart")
                except sqlite3.OperationalError:
                    sql = f"ALTER TABLE xiuxian_impart ADD COLUMN {s} integer DEFAULT 0;"
                    logger.opt(colors=True).info(f"<green>{sql}</green>")
                    logger.opt(colors=True).info(f"<green>xiuxian_impart数据库核对成功!</green>")
                    c.execute(sql)

            self.conn.commit()

    @classmethod
    def close_dbs(cls):
        XIUXIAN_IMPART_BUFF().close()

    def create_user(self, user_id):
        """校验用户是否存在"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select * from xiuxian_impart WHERE user_id=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if not result:
                return False
            else:
                return True

    def _create_user(self, user_id: str) -> None:
        """在数据库中创建用户并初始化"""
        with self.lock:
            if self.create_user(user_id):
                pass
            else:
                c = self.conn.cursor()
                sql = f"INSERT INTO xiuxian_impart (user_id, impart_hp_per, impart_atk_per, impart_mp_per, impart_exp_up ,boss_atk,impart_know_per,impart_burst_per,impart_mix_per,impart_reap_per,impart_two_exp,stone_num,impart_lv,impart_num,exp_day,wish) VALUES(?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)"
                c.execute(sql, (user_id,))
                self.conn.commit()

    def get_user_impart_info_with_id(self, user_id):
        """根据USER_ID获取用户impart_buff信息"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"select * from xiuxian_impart WHERE user_id=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result:
                columns = [column[0] for column in cur.description]
                user_dict = dict(zip(columns, result))
                return user_dict
            else:
                return None

    def update_impart_hp_per(self, impart_num, user_id):
        """更新impart_hp_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_hp_per=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_impart_hp_per(self, impart_num, user_id):
        """add impart_hp_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_hp_per=impart_hp_per+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_impart_atk_per(self, impart_num, user_id):
        """更新impart_atk_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_atk_per=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_impart_atk_per(self, impart_num, user_id):
        """add impart_atk_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_atk_per=impart_atk_per+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_impart_mp_per(self, impart_num, user_id):
        """impart_mp_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_mp_per=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_impart_mp_per(self, impart_num, user_id):
        """add impart_mp_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_mp_per=impart_mp_per+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_impart_exp_up(self, impart_num, user_id):
        """impart_exp_up"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_exp_up=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_impart_exp_up(self, impart_num, user_id):
        """add impart_exp_up"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_exp_up=impart_exp_up+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_boss_atk(self, impart_num, user_id):
        """boss_atk"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET boss_atk=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_boss_atk(self, impart_num, user_id):
        """add boss_atk"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET boss_atk=boss_atk+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_impart_know_per(self, impart_num, user_id):
        """impart_know_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_know_per=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_impart_know_per(self, impart_num, user_id):
        """add impart_know_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_know_per=impart_know_per+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_impart_burst_per(self, impart_num, user_id):
        """impart_burst_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_burst_per=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_impart_burst_per(self, impart_num, user_id):
        """add impart_burst_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_burst_per=impart_burst_per+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_impart_mix_per(self, impart_num, user_id):
        """impart_mix_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_mix_per=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_impart_mix_per(self, impart_num, user_id):
        """add impart_mix_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_mix_per=impart_mix_per+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_impart_reap_per(self, impart_num, user_id):
        """impart_reap_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_reap_per=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_impart_reap_per(self, impart_num, user_id):
        """add impart_reap_per"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_reap_per=impart_reap_per+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_impart_two_exp(self, impart_num, user_id):
        """更新双修"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_two_exp=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_impart_num(self, impart_num, user_id):
        """更新抽卡次数"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_num=impart_num+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_impart_two_exp(self, impart_num, user_id):
        """add impart_two_exp"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET impart_two_exp=impart_two_exp+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_impart_wish(self, impart_num, user_id):
        """更新祈愿值/次数"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET wish=? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def add_impart_wish(self, impart_num, user_id):
        """增加祈愿值/次数"""
        with self.lock:
            cur = self.conn.cursor()
            sql = f"UPDATE xiuxian_impart SET wish=wish+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def update_stone_num(self, impart_num, user_id, type_):
        """更新结晶数量"""
        with self.lock:
            if type_ == 1:
                cur = self.conn.cursor()
                sql = f"UPDATE xiuxian_impart SET stone_num=stone_num+? WHERE user_id=?"
                cur.execute(sql, (impart_num, user_id))
                self.conn.commit()
                return True
            if type_ == 2:
                cur = self.conn.cursor()
                sql = f"UPDATE xiuxian_impart SET stone_num=stone_num-? WHERE user_id=?"
                cur.execute(sql, (impart_num, user_id))
                self.conn.commit()
                return True

    def update_impart_stone_all(self, impart_stone):
        """所有用户增加结晶"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "UPDATE xiuxian_impart SET stone_num=stone_num+?"
            cur.execute(sql, (impart_stone,))
            self.conn.commit()

    def update_impart_lv(self, user_id, impart_lv):
        """更新虚神界等级"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "UPDATE xiuxian_impart SET impart_lv=? WHERE user_id=?"
            cur.execute(sql, (impart_lv, user_id))
            self.conn.commit()

    def impart_lv_reset(self):
        """重置所有用户虚神界等级"""
        with self.lock:
            sql = f"UPDATE xiuxian_impart SET impart_lv=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

    def impart_num_reset(self):
        """重置所有用户传承抽卡次数"""
        with self.lock:
            sql = f"UPDATE xiuxian_impart SET impart_num=0"
            cur = self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()

    def get_impart_rank(self):
        """获取虚神界等级排行榜"""
        with self.lock:
            sql = "SELECT user_id, impart_lv FROM xiuxian_impart WHERE impart_lv > 0 ORDER BY impart_lv DESC, stone_num DESC LIMIT 50"
            cur = self.conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
            columns = [column[0] for column in cur.description]
            return [dict(zip(columns, row)) for row in result]

    def update_all_users_impart_lv(self, num, operation):
        """
        更新所有用户的虚神界等级
        :param num: 要增加/减少的数值
        :param operation: 1-增加, 2-减少
        """
        with self.lock:
            cur = self.conn.cursor()
            if operation == 1:
                sql = """
                UPDATE xiuxian_impart 
                SET impart_lv = CASE 
                    WHEN impart_lv + ? > 30 THEN 30 
                    ELSE impart_lv + ? 
                END 
                WHERE impart_lv >= 0
                """
                cur.execute(sql, (num, num))
            elif operation == 2:
                sql = """
                UPDATE xiuxian_impart 
                SET impart_lv = CASE 
                    WHEN impart_lv - ? < 0 THEN 0 
                    ELSE impart_lv - ? 
                END 
                WHERE impart_lv > 0
                """
                cur.execute(sql, (num, num))
            else:
                return

            self.conn.commit()

    def convert_stone_to_wishing_stone(self, user_id):
        """将思恋结晶转换为祈愿石（100:1），多余废弃"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "SELECT stone_num FROM xiuxian_impart WHERE user_id=?"
            cur.execute(sql, (user_id,))
            result = cur.fetchone()
            if result is None:
                return
            stone_num = result[0]
            if stone_num < 100:
                return
            wishing_stone_num = stone_num // 100
            sql_update = "UPDATE xiuxian_impart SET stone_num=0 WHERE user_id=?"
            cur.execute(sql_update, (user_id,))
            self.conn.commit()
            sql_message.send_back(user_id, 20005, "祈愿石", "特殊道具", wishing_stone_num, 1)

    def add_impart_exp_day(self, impart_num, user_id):
        """add impart_exp_day"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "UPDATE xiuxian_impart SET exp_day=exp_day+? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True

    def use_impart_exp_day(self, impart_num, user_id):
        """use impart_exp_day"""
        with self.lock:
            cur = self.conn.cursor()
            sql = "UPDATE xiuxian_impart SET exp_day=exp_day-? WHERE user_id=?"
            cur.execute(sql, (impart_num, user_id))
            self.conn.commit()
            return True


def leave_harm_time(user_id):
    """重伤恢复时间"""
    user_mes = sql_message.get_user_info_with_id(user_id)
    level = user_mes['level']
    level_rate = sql_message.get_root_rate(user_mes['root_type'], user_id) # 灵根倍率
    realm_rate = jsondata.level_data()[level]["spend"] # 境界倍率
    main_buff_data = UserBuffDate(user_id).get_user_main_buff_data() # 主功法数据
    main_buff_rate_buff = main_buff_data['ratebuff'] if main_buff_data else 0 # 主功法修炼倍率
    
    try:
        time = int(((user_mes['exp'] / 2) + (user_mes['exp'] / 10) - user_mes['hp']) / (user_mes['exp'] / 10))
    except ZeroDivisionError:
        time = "无穷大"
    except OverflowError:
        time = "溢出"
    time = max(time, 1)
    return time


async def impart_check(user_id):
    if XIUXIAN_IMPART_BUFF().get_user_impart_info_with_id(user_id) is None:
        XIUXIAN_IMPART_BUFF()._create_user(user_id)
        return XIUXIAN_IMPART_BUFF().get_user_impart_info_with_id(user_id)
    else:
        return XIUXIAN_IMPART_BUFF().get_user_impart_info_with_id(user_id)
    
xiuxian_impart = XIUXIAN_IMPART_BUFF()

# 这里是buff部分
class BuffJsonDate:

    def __init__(self):
        """json文件路径"""
        self.mainbuff_jsonpath = SKILLPATHH / "主功法.json"
        self.secbuff_jsonpath = SKILLPATHH / "神通.json"
        self.effect1buff_jsonpath = SKILLPATHH / "身法.json"
        self.effect2buff_jsonpath = SKILLPATHH / "瞳术.json"
        self.gfpeizhi_jsonpath = SKILLPATHH / "功法概率设置.json"
        self.weapon_jsonpath = WEAPONPATH / "法器.json"
        self.armor_jsonpath = WEAPONPATH / "防具.json"

    def get_main_buff(self, id):
        return readf(self.mainbuff_jsonpath)[str(id)]

    def get_sec_buff(self, id):
        return readf(self.secbuff_jsonpath)[str(id)]
        
    def get_effect1_buff(self, id):
        return readf(self.effect1buff_jsonpath)[str(id)]

    def get_effect2_buff(self, id):
        return readf(self.effect2buff_jsonpath)[str(id)]
        
    def get_gfpeizhi(self):
        return readf(self.gfpeizhi_jsonpath)

    def get_weapon_data(self):
        return readf(self.weapon_jsonpath)

    def get_weapon_info(self, id):
        return readf(self.weapon_jsonpath)[str(id)]

    def get_armor_data(self):
        return readf(self.armor_jsonpath)

    def get_armor_info(self, id):
        return readf(self.armor_jsonpath)[str(id)]


class UserBuffDate:
    def __init__(self, user_id):
        """用户Buff数据"""
        self.user_id = user_id

    @property
    def BuffInfo(self):
        """获取最新的 Buff 信息"""
        return get_user_buff(self.user_id)

    def get_user_main_buff_data(self):
        """获取用户主功法数据"""
        main_buff_data = None
        buff_info = self.BuffInfo
        main_buff_id = buff_info.get('main_buff', 0)
        if main_buff_id != 0:
            main_buff_data = items.get_data_by_item_id(main_buff_id)
        return main_buff_data
    
    def get_user_sub_buff_data(self):
        """获取用户辅修功法数据"""
        sub_buff_data = None
        buff_info = self.BuffInfo
        sub_buff_id = buff_info.get('sub_buff', 0)
        if sub_buff_id != 0:
            sub_buff_data = items.get_data_by_item_id(sub_buff_id)
        return sub_buff_data

    def get_user_sec_buff_data(self):
        """获取用户神通数据"""
        sec_buff_data = None
        buff_info = self.BuffInfo
        sec_buff_id = buff_info.get('sec_buff', 0)
        if sec_buff_id != 0:
            sec_buff_data = items.get_data_by_item_id(sec_buff_id)
        return sec_buff_data

    def get_user_effect1_buff_data(self):
        """获取用户身法数据"""
        effect1_buff_data = None
        buff_info = self.BuffInfo
        effect1_buff_id = buff_info.get('effect1_buff', 0)
        if effect1_buff_id != 0:
            effect1_buff_data = items.get_data_by_item_id(effect1_buff_id)
        return effect1_buff_data

    def get_user_effect2_buff_data(self):
        """获取用户瞳术数据"""
        effect2_buff_data = None
        buff_info = self.BuffInfo
        effect2_buff_id = buff_info.get('effect2_buff', 0)
        if effect2_buff_id != 0:
            effect2_buff_data = items.get_data_by_item_id(effect2_buff_id)
        return effect2_buff_data
        
    def get_user_weapon_data(self):
        """获取用户法器数据"""
        weapon_data = None
        buff_info = self.BuffInfo
        weapon_id = buff_info.get('faqi_buff', 0)
        if weapon_id != 0:
            weapon_data = items.get_data_by_item_id(weapon_id)
        return weapon_data

    def get_user_armor_buff_data(self):
        """获取用户防具数据"""
        armor_buff_data = None
        buff_info = self.BuffInfo
        armor_buff_id = buff_info.get('armor_buff', 0)
        if armor_buff_id != 0:
            armor_buff_data = items.get_data_by_item_id(armor_buff_id)
        return armor_buff_data

def calc_accessory_effects(user_id: str | int) -> dict:
    """
    计算饰品效果：
    - 基础词条总和（气血/攻击/会心/会伤/减伤/抗暴）
    - 套装件数
    - 套装激活列表（2件/4件）

    返回:
    {
      "hp_pct": float,
      "atk_pct": float,
      "crit_rate": float,
      "crit_damage": float,
      "dmg_reduction": float,
      "crit_resist": float,
      "set_count": {"烈阳":2,...},
      "set_bonus": [{"set":"烈阳","pieces":2,"type":"attack","value":0.08}, ...]
    }
    """
    result = {
        "hp_pct": 0.0,
        "atk_pct": 0.0,
        "crit_rate": 0.0,
        "crit_damage": 0.0,
        "dmg_reduction": 0.0,
        "crit_resist": 0.0,
        "set_count": {},
        "set_bonus": []
    }

    from ..xiuxian_back import AFFIX_KEY_MAP, SET_BONUS
    acc_data = get_user_accessory_data(user_id)
    equipped = acc_data.get("equipped", {})

    for _, acc in equipped.items():
        if not acc:
            continue

        set_type = acc.get("set_type", "")
        if set_type:
            result["set_count"][set_type] = result["set_count"].get(set_type, 0) + 1

        affixes = acc.get("affixes", [])
        if not isinstance(affixes, list):
            continue

        for af in affixes:
            a_type = af.get("type")
            a_val = float(af.get("value", 0))
            mapped = AFFIX_KEY_MAP.get(a_type)

            if mapped == "hp_pct":
                result["hp_pct"] += a_val
            elif mapped == "atk_pct":
                result["atk_pct"] += a_val
            elif mapped == "crit_rate":
                result["crit_rate"] += a_val
            elif mapped == "crit_damage":
                result["crit_damage"] += a_val
            elif mapped == "dmg_reduction":
                result["dmg_reduction"] += a_val
            elif mapped == "crit_resist":
                result["crit_resist"] += a_val

    # 套装激活（2件/4件）
    for set_name, cnt in result["set_count"].items():
        conf = SET_BONUS.get(set_name, {})

        if cnt >= 2 and 2 in conf:
            v = conf[2]
            result["set_bonus"].append({
                "set": set_name,
                "pieces": 2,
                "type": v.get("type"),
                "value": float(v.get("value", 0))
            })

        if cnt >= 4 and 4 in conf:
            v = conf[4]
            result["set_bonus"].append({
                "set": set_name,
                "pieces": 4,
                "type": v.get("type"),
                "value": float(v.get("value", 0))
            })

    return result


def get_user_accessory_data(user_id: str | int) -> dict:
    """
    读取玩家饰品数据（player.db -> player_accessory）
    返回结构:
    {
        "equipped": {"手镯": {...}|None, "戒指": ..., "手环": ..., "项链": ...},
        "bag": [...]
    }
    """
    data = player_data_manager.get_fields(str(user_id), "player_accessory")
    if not data:
        return {
            "equipped": {"手镯": None, "戒指": None, "手环": None, "项链": None},
            "bag": []
        }

    equipped = data.get("equipped") or {"手镯": None, "戒指": None, "手环": None, "项链": None}
    bag = data.get("bag") or []
    return {"equipped": equipped, "bag": bag}

def final_user_data(user_data, columns):
    """传入数据库行与列描述，返回叠加buff后的用户信息（统一口径）"""
    user_dict = dict(zip((col[0] for col in columns), user_data))
    user_id = user_dict.get("user_id")
    if not user_id:
        return user_dict

    final_attr = get_final_attributes(user_id, ratio=1.0, include_current=True)
    if not final_attr:
        return user_dict

    # 仅覆盖需要动态计算的字段，其他字段保持原样
    user_dict["hp"] = int(final_attr["current_hp"])
    user_dict["mp"] = int(final_attr["current_mp"])
    user_dict["atk"] = int(final_attr["final_atk"])
    return user_dict

def get_base_attributes(user_id: str | int) -> dict | None:
    """获取基础属性（不吃任何buff）"""
    user = sql_message.get_user_info_with_id(user_id)
    if not user:
        return None

    return {
        "user_id": user["user_id"],
        "nickname": user["user_name"],
        "level": user["level"],
        "exp": int(user["exp"]),
        "stone": int(user["stone"]),

        "base_hp": int(user["hp"]),
        "base_mp": int(user["mp"]),
        "base_atk": int(user["atk"]),

        "atkpractice": int(user.get("atkpractice", 0)),
        "hppractice": int(user.get("hppractice", 0)),
        "mppractice": int(user.get("mppractice", 0)),
    }


def get_final_attributes(user_id: str | int, ratio: float = 1.0, include_current: bool = True) -> dict | None:
    """获取buff加成后的最终属性（统一口径）"""
    base = get_base_attributes(user_id)
    if not base:
        return None

    # buff数据
    user_buff = UserBuffDate(user_id)
    buff_info = user_buff.BuffInfo or {}

    main = user_buff.get_user_main_buff_data() or {}
    weapon = user_buff.get_user_weapon_data() or {}
    armor = user_buff.get_user_armor_buff_data() or {}

    impart = xiuxian_impart.get_user_impart_info_with_id(user_id) or {}

    # 主功法
    main_hp = float(main.get("hpbuff", 0))
    main_mp = float(main.get("mpbuff", 0))
    main_atk = float(main.get("atkbuff", 0))
    main_crit = float(main.get("crit_buff", 0))
    main_critatk = float(main.get("critatk", 0))
    main_def = float(main.get("def_buff", 0))

    # 装备
    weapon_atk = float(weapon.get("atk_buff", 0))
    weapon_crit = float(weapon.get("crit_buff", 0))
    weapon_critatk = float(weapon.get("critatk", 0))
    weapon_def = float(weapon.get("def_buff", 0))

    armor_atk = float(armor.get("atk_buff", 0))
    armor_crit = float(armor.get("crit_buff", 0))
    armor_def = float(armor.get("def_buff", 0))

    # 传承
    impart_hp = float(impart.get("impart_hp_per", 0))
    impart_mp = float(impart.get("impart_mp_per", 0))
    impart_atk = float(impart.get("impart_atk_per", 0))
    impart_know = float(impart.get("impart_know_per", 0))
    impart_burst = float(impart.get("impart_burst_per", 0))
    boss_atk = float(impart.get("boss_atk", 0))

    # 修炼等级
    hppractice = base["hppractice"] * 0.05
    mppractice = base["mppractice"] * 0.05
    atkpractice = base["atkpractice"] * 0.04

    # 永久攻击buff
    perm_atk = int(buff_info.get("atk_buff", 0) or 0)

    # ===== 常规最终值（不含饰品）=====
    max_hp = int((base["exp"] / 2) * (1 + main_hp + impart_hp + hppractice))
    max_mp = int(base["exp"] * (1 + main_mp + impart_mp + mppractice))

    current_hp = int(base["base_hp"] * (1 + main_hp + impart_hp + hppractice))
    current_mp = int(base["base_mp"] * (1 + main_mp + impart_mp + mppractice))

    final_atk = int(
        (base["base_atk"] * (1 + atkpractice) * (1 + main_atk) * (1 + weapon_atk) * (1 + armor_atk)) * (1 + impart_atk)
    ) + perm_atk

    crit_rate = max(0, min(1, weapon_crit + armor_crit + main_crit + impart_know))
    crit_damage = 1.5 + impart_burst + weapon_critatk + main_critatk
    damage_reduction = main_def + weapon_def + armor_def
    armor_penetration = 0.0  # 可在其他系统叠加

    # ===== 饰品加成 =====
    acc_effect = calc_accessory_effects(user_id)

    # 百分比型基础属性加成
    max_hp = int(max_hp * (1 + acc_effect["hp_pct"]))
    current_hp = int(current_hp * (1 + acc_effect["hp_pct"]))
    final_atk = int(final_atk * (1 + acc_effect["atk_pct"]))

    # 战斗率属性加成
    crit_rate += acc_effect["crit_rate"]
    crit_damage += acc_effect["crit_damage"]
    damage_reduction += acc_effect["dmg_reduction"]

    # 上限裁剪（防止溢出）
    damage_reduction = min(0.95, damage_reduction)

    # 比例缩放（例如PVE多队平衡）
    max_hp = int(max_hp * ratio)
    max_mp = int(max_mp * ratio)
    current_hp = int(current_hp * ratio) if include_current else max_hp
    current_mp = int(current_mp * ratio) if include_current else max_mp
    final_atk = int(final_atk * ratio)

    return {
        **base,
        "max_hp": max_hp,
        "max_mp": max_mp,
        "current_hp": current_hp,
        "current_mp": current_mp,
        "final_atk": final_atk,

        "crit_rate": crit_rate,
        "crit_damage": crit_damage,
        "damage_reduction": damage_reduction,
        "armor_penetration": armor_penetration,
        "boss_damage_bonus": boss_atk,
        "accessory_effect": acc_effect,
    }

def get_weapon_info_msg(weapon_id, weapon_info=None):
    """
    获取一个法器(武器)信息msg
    :param weapon_id:法器(武器)ID
    :param weapon_info:法器(武器)信息json,可不传
    :return 法器(武器)信息msg
    """
    msg = ''
    if weapon_info is None:
        weapon_info = items.get_data_by_item_id(weapon_id)
    atk_buff_msg = f"提升{int(weapon_info['atk_buff'] * 100)}%攻击力！" if weapon_info['atk_buff'] != 0 else ''
    crit_buff_msg = f"提升{int(weapon_info['crit_buff'] * 100)}%会心率！" if weapon_info['crit_buff'] != 0 else ''
    crit_atk_msg = f"提升{int(weapon_info['critatk'] * 100)}%会心伤害！" if weapon_info['critatk'] != 0 else ''
    def_buff_msg = f"{'提升' if weapon_info['def_buff'] > 0 else '降低'}{int(abs(weapon_info['def_buff']) * 100)}%减伤率！" if weapon_info['def_buff'] != 0 else ''
    zw_buff_msg = f"装备专属武器时提升伤害！！" if weapon_info['zw'] != 0 else ''
    mp_buff_msg = f"降低真元消耗{int(weapon_info['mp_buff'] * 100)}%！" if weapon_info['mp_buff'] != 0 else ''
    msg += f"名字：{weapon_info['name']}\n"
    msg += f"品阶：{weapon_info['level']}\n"
    msg += f"效果：{weapon_info['desc']}，{atk_buff_msg}{crit_buff_msg}{crit_atk_msg}{def_buff_msg}{mp_buff_msg}{zw_buff_msg}"
    return msg


def get_armor_info_msg(armor_id, armor_info=None):
    """
    获取一个法宝(防具)信息msg
    :param armor_id:法宝(防具)ID
    :param armor_info;法宝(防具)信息json,可不传
    :return 法宝(防具)信息msg
    """
    msg = ''
    if armor_info is None:
        armor_info = items.get_data_by_item_id(armor_id)
    def_buff_msg = f"提升{int(armor_info['def_buff'] * 100)}%减伤率！"
    atk_buff_msg = f"提升{int(armor_info['atk_buff'] * 100)}%攻击力！" if armor_info['atk_buff'] != 0 else ''
    crit_buff_msg = f"提升{int(armor_info['crit_buff'] * 100)}%会心率！" if armor_info['crit_buff'] != 0 else ''
    msg += f"名字：{armor_info['name']}\n"
    msg += f"品阶：{armor_info['level']}\n"
    msg += f"效果：{armor_info['desc']}，{def_buff_msg}{atk_buff_msg}{crit_buff_msg}"
    return msg


def get_main_info_msg(id):
    """获取一个主功法信息msg"""
    mainbuff = items.get_data_by_item_id(id)
    hpmsg = f"提升{round(mainbuff['hpbuff'] * 100, 0)}%气血" if mainbuff['hpbuff'] != 0 else ''
    mpmsg = f"，提升{round(mainbuff['mpbuff'] * 100, 0)}%真元" if mainbuff['mpbuff'] != 0 else ''
    atkmsg = f"，提升{round(mainbuff['atkbuff'] * 100, 0)}%攻击力" if mainbuff['atkbuff'] != 0 else ''
    ratemsg = f"，提升{round(mainbuff['ratebuff'] * 100, 0)}%修炼速度" if mainbuff['ratebuff'] != 0 else ''
    
    cri_tmsg = f"，提升{round(mainbuff['crit_buff'] * 100, 0)}%会心率" if mainbuff['crit_buff'] != 0 else ''
    def_msg = f"，{'提升' if mainbuff['def_buff'] > 0 else '降低'}{round(abs(mainbuff['def_buff']) * 100, 0)}%减伤率" if mainbuff['def_buff'] != 0 else ''
    dan_msg = f"，增加炼丹产出{round(mainbuff['dan_buff'])}枚" if mainbuff['dan_buff'] != 0 else ''
    dan_exp_msg = f"，每枚丹药额外增加{round(mainbuff['dan_exp'])}炼丹经验" if mainbuff['dan_exp'] != 0 else ''
    reap_msg = f"，提升药材收取数量{round(mainbuff['reap_buff'])}个" if mainbuff['reap_buff'] != 0 else ''
    exp_msg = f"，突破失败{round(mainbuff['exp_buff'] * 100, 0)}%经验保护" if mainbuff['exp_buff'] != 0 else ''
    critatk_msg = f"，提升{round(mainbuff['critatk'] * 100, 0)}%会心伤害" if mainbuff['critatk'] != 0 else ''
    two_msg = f"，增加{round(mainbuff['two_buff'])}次双修次数" if mainbuff['two_buff'] != 0 else ''
    number_msg = f"，提升{round(mainbuff['number'])}%突破概率" if mainbuff['number'] != 0 else ''
    
    clo_exp_msg = f"，提升{round(mainbuff['clo_exp'] * 100, 0)}%闭关经验" if mainbuff['clo_exp'] != 0 else ''
    clo_rs_msg = f"，提升{round(mainbuff['clo_rs'] * 100, 0)}%闭关生命回复" if mainbuff['clo_rs'] != 0 else ''
    random_buff_msg = f"，战斗时随机获得一个战斗属性" if mainbuff['random_buff'] != 0 else ''
    ew_name = items.get_data_by_item_id(mainbuff['ew']) if mainbuff['ew'] != 0 else ''
    ew_msg =  f"，使用{ew_name['name']}时伤害增加50%！" if mainbuff['ew'] != 0 else ''
    msg = f"{hpmsg}{mpmsg}{atkmsg}{ratemsg}{cri_tmsg}{def_msg}{dan_msg}{dan_exp_msg}{reap_msg}{exp_msg}{critatk_msg}{two_msg}{number_msg}{clo_exp_msg}{clo_rs_msg}{random_buff_msg}{ew_msg}！"
    return mainbuff, msg

def get_sub_info_msg(id): #辅修功法8
    """获取辅修信息msg"""
    subbuff = items.get_data_by_item_id(id)
    submsg = ""
    if subbuff['buff_type'] == '1':
        submsg = "提升" + subbuff['buff'] + "%攻击力"
    if subbuff['buff_type'] == '2':
        submsg = "提升" + subbuff['buff'] + "%暴击率"
    if subbuff['buff_type'] == '3':
        submsg = "提升" + subbuff['buff'] + "%暴击伤害"
    if subbuff['buff_type'] == '4':
        submsg = "提升" + subbuff['buff'] + "%每回合气血回复"
    if subbuff['buff_type'] == '5':
        submsg = "提升" + subbuff['buff'] + "%每回合真元回复"
    if subbuff['buff_type'] == '6':
        submsg = "提升" + subbuff['buff'] + "%气血吸取"
    if subbuff['buff_type'] == '7':
        submsg = "提升" + subbuff['buff'] + "%真元吸取"
    if subbuff['buff_type'] == '8':
        submsg = "给对手造成" + subbuff['buff'] + "%中毒"
    if subbuff['buff_type'] == '9':
        submsg = f"提升{subbuff['buff']}%气血吸取,提升{subbuff['buff2']}%真元吸取"

    stone_msg  = "提升{}%boss战灵石获取".format(round(subbuff['stone'] * 100, 0)) if subbuff['stone'] != 0 else ''
    integral_msg = "，提升{}点boss战积分获取".format(round(subbuff['integral'])) if subbuff['integral'] != 0 else ''
    jin_msg = "禁止对手吸取" if subbuff['jin'] != 0 else ''
    drop_msg = "，提升boss掉落率" if subbuff['drop'] != 0 else ''
    fan_msg = "使对手发出的debuff失效" if subbuff['fan'] != 0 else ''
    break_msg = "获得{}%穿甲".format(round(subbuff['break'] * 100, 0)) if subbuff['break'] != 0 else ''
    exp_msg = "，增加战斗获得的修为" if subbuff['exp'] != 0 else ''
    

    msg = f"{submsg}{stone_msg}{integral_msg}{jin_msg}{drop_msg}{fan_msg}{break_msg}{exp_msg}"
    return subbuff, msg

def get_user_buff(user_id):
    BuffInfo = sql_message.get_user_buff_info(user_id)
    if BuffInfo is None:
        sql_message.initialize_user_buff_info(user_id)
        return sql_message.get_user_buff_info(user_id)
    else:
        return BuffInfo


def readf(FILEPATH):
    with open(FILEPATH, "r", encoding="UTF-8") as f:
        data = f.read()
    return json.loads(data)


def get_sec_msg(secbuffdata):
    msg = None
    if secbuffdata is None:
        msg = "无"
        return msg
    hpmsg = f"，消耗当前血量{int(secbuffdata['hpcost'] * 100)}%" if secbuffdata['hpcost'] != 0 else ''
    mpmsg = f"，消耗真元{int(secbuffdata['mpcost'] * 100)}%" if secbuffdata['mpcost'] != 0 else ''

    if secbuffdata['skill_type'] == 1:
        shmsg = ''
        for value in secbuffdata['atkvalue']:
            shmsg += f"{value}倍、"
        if secbuffdata['turncost'] == 0:
            msg = f"攻击{len(secbuffdata['atkvalue'])}次，造成{shmsg[:-1]}伤害{hpmsg}{mpmsg}，释放概率：{secbuffdata['rate']}%"
        else:
            msg = f"连续攻击{len(secbuffdata['atkvalue'])}次，造成{shmsg[:-1]}伤害{hpmsg}{mpmsg}，休息{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"
    elif secbuffdata['skill_type'] == 2:
        msg = f"持续伤害，造成{secbuffdata['atkvalue']}倍攻击力伤害{hpmsg}{mpmsg}，持续{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"
    elif secbuffdata['skill_type'] == 3:
        if secbuffdata['bufftype'] == 1:
            msg = f"增强自身，提高{secbuffdata['buffvalue']}倍攻击力{hpmsg}{mpmsg}，持续{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"
        elif secbuffdata['bufftype'] == 2:
            msg = f"增强自身，提高{secbuffdata['buffvalue'] * 100}%减伤率{hpmsg}{mpmsg}，持续{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"
    elif secbuffdata['skill_type'] == 4:
        msg = f"封印对手{hpmsg}{mpmsg}，持续{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%，命中成功率{secbuffdata['success']}%"
    elif secbuffdata['skill_type'] == 5:
        if secbuffdata['turncost'] == 0:
            msg = f"随机伤害，造成{secbuffdata['atkvalue']}倍～{secbuffdata['atkvalue2']}倍攻击力伤害{hpmsg}{mpmsg}，释放概率：{secbuffdata['rate']}%"
        else:
            msg = f"随机伤害，造成{secbuffdata['atkvalue']}倍～{secbuffdata['atkvalue2']}倍攻击力伤害{hpmsg}{mpmsg}，休息{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"
        
    elif secbuffdata['skill_type'] == 6:
        msg = f"叠加伤害，每回合叠加{secbuffdata['buffvalue']}倍攻击力{hpmsg}{mpmsg}，持续{secbuffdata['turncost']}回合，释放概率：{secbuffdata['rate']}%"

    elif secbuffdata['skill_type'] == 7:
        msg = "变化神通，战斗时随机获得一个神通"
            
    return msg

def get_effect_info_msg(id): #身法、瞳术
    """获取秘术信息msg"""
    effectbuff = items.get_data_by_item_id(id)
    effectmsg = ""
    if effectbuff['buff_type'] == '1':
        effectmsg = f"提升{effectbuff['buff2']}%～{effectbuff['buff']}%闪避率"
    if effectbuff['buff_type'] == '2':
        effectmsg = f"提升{effectbuff['buff2']}%～{effectbuff['buff']}%命中率"
    

    msg = f"{effectmsg}"
    return effectbuff, msg

mix_elixir_infoconfigkey = ["收取时间", "收取等级", "灵田数量", '药材速度', '灵田傀儡', "丹药控火", "丹药耐药性", "炼丹记录", "炼丹经验"]

def read_player_info(user_id, info_name):
    player_data_manager = PlayerDataManager()
    user_id_str = str(user_id)
    info = {}
    record = player_data_manager.get_fields(user_id_str, info_name) # 直接获取所有字段
    if record:
        for field in mix_elixir_infoconfigkey:
            if field in record:
                info[field] = record[field]
    return info

def save_player_info(user_id, data, info_name):
    player_data_manager = PlayerDataManager()
    user_id_str = str(user_id)
    for field, value in data.items():
        player_data_manager.update_or_write_data(user_id_str, info_name, field, value, data_type="TEXT")

def get_player_info(user_id, info_name):
    MIXELIXIRINFOCONFIG = {
        "收取时间": datetime.now().strftime('%Y-%m-%d %H:%M:%S'), # str
        "收取等级": 0,
        "灵田数量": 1,
        '药材速度': 0,
        '灵田傀儡': 0,
        "丹药控火": 0,
        "丹药耐药性": 0,
        "炼丹记录": {},
        "炼丹经验": 0
    }
    
    player_info = read_player_info(user_id, info_name)
    
    # 检查并补全缺失字段
    updated_info = False
    for key in mix_elixir_infoconfigkey:
        if key not in player_info:
            player_info[key] = MIXELIXIRINFOCONFIG[key]
            updated_info = True
            
    if updated_info: # 如果有更新，则保存
        save_player_info(user_id, player_info, info_name)
        
    return player_info

def _safe_parse_dt(dt_str):
    if not dt_str:
        return None
    if isinstance(dt_str, datetime):
        return dt_str
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(dt_str), fmt)
        except ValueError:
            continue
    return None


def number_count(num):
    """
    数据库安全处理：
    如果数值超过 SQLite INTEGER 限制 (9,223,372,036,854,775,807)，返回科学计数法字符串。
    否则返回 int。
    """
    MAX_SQLITE_INT = 9223372036854775807
    try:
        val = float(num)
    except (TypeError, ValueError):
        raise ValueError("输入必须是数字")
    
    if val > MAX_SQLITE_INT:
        # 超过上限，返回科学计数法字符串，例如 "1.23e+20"
        return "{:.2e}".format(val)
    return int(val)

def backup_db_files():
    """
    兼容旧调用：转发到 UpdateManager.backup_db_files
    """
    return UpdateManager().backup_db_files()

def _qid(name: str) -> str:
    # 安全引用SQLite标识符
    return '"' + str(name).replace('"', '""') + '"'


def _get_tables(conn: sqlite3.Connection):
    # 获取所有业务表，排除sqlite内部表
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [r[0] for r in cur.fetchall() if r and r[0] and not r[0].startswith("sqlite_")]


def _get_table_info(conn: sqlite3.Connection, table: str):
    # 获取表结构信息
    cur = conn.cursor()
    cur.execute(f'PRAGMA table_info({_qid(table)})')
    return cur.fetchall()  # cid, name, type, notnull, dflt_value, pk


def _has_autoincrement(conn: sqlite3.Connection, table: str) -> bool:
    # 检查原表是否包含AUTOINCREMENT
    cur = conn.cursor()
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
    row = cur.fetchone()
    if not row or not row[0]:
        return False
    return "AUTOINCREMENT" in row[0].upper()


def _build_create_sql_by_table_info(conn: sqlite3.Connection, table: str, to_text_cols: set[str]) -> str:
    # 基于PRAGMA重建建表SQL，只改指定列为TEXT，保留主键结构
    cols = _get_table_info(conn, table)
    if not cols:
        raise RuntimeError(f"表 {table} 无字段信息")

    col_defs = []
    pk_cols = []
    pk_count = sum(1 for c in cols if int(c[5]) > 0)
    single_pk = (pk_count == 1)
    autoinc = _has_autoincrement(conn, table)

    for cid, name, ctype, notnull, dflt, pk in cols:
        raw_type = (ctype or "TEXT").upper().strip()
        new_type = "TEXT" if name in to_text_cols else raw_type

        part = f"{_qid(name)} {new_type}"
        if int(notnull) == 1:
            part += " NOT NULL"
        if dflt is not None:
            part += f" DEFAULT {dflt}"

        if int(pk) > 0 and single_pk:
            part += " PRIMARY KEY"
            # 仅当该列未改为TEXT，且原列为INTEGER主键并带AUTOINCREMENT时保留自增
            if (name not in to_text_cols) and (raw_type in ("INTEGER", "INT")) and autoinc:
                part += " AUTOINCREMENT"
        elif int(pk) > 0:
            pk_cols.append((name, int(pk)))

        col_defs.append(part)

    if pk_cols:
        pk_cols = [x[0] for x in sorted(pk_cols, key=lambda x: x[1])]
        col_defs.append("PRIMARY KEY (" + ", ".join(_qid(x) for x in pk_cols) + ")")

    tmp_table = f"{table}__tmp_rebuild"
    return f'CREATE TABLE {_qid(tmp_table)} (\n  ' + ",\n  ".join(col_defs) + "\n)"


def _rebuild_table_convert_columns_to_text(conn: sqlite3.Connection, table: str, to_text_cols: set[str]):
    # 重建表并将指定列改为TEXT，数据原样拷贝（目标列统一转str）
    cols_info = _get_table_info(conn, table)
    cols = [c[1] for c in cols_info]
    hit_cols = [c for c in cols if c in to_text_cols]
    if not hit_cols:
        return 0

    cur = conn.cursor()
    tmp_table = f"{table}__tmp_rebuild"
    cur.execute(f'DROP TABLE IF EXISTS {_qid(tmp_table)}')

    create_sql = _build_create_sql_by_table_info(conn, table, to_text_cols)
    cur.execute(create_sql)

    col_sql = ", ".join(_qid(c) for c in cols)
    cur.execute(f'SELECT {col_sql} FROM {_qid(table)}')
    rows = cur.fetchall()

    ins_sql = f'INSERT INTO {_qid(tmp_table)} ({col_sql}) VALUES ({",".join(["?"] * len(cols))})'
    copied = 0
    for row in rows:
        row = list(row)
        for i, c in enumerate(cols):
            if c in to_text_cols and row[i] is not None:
                row[i] = str(row[i])
        cur.execute(ins_sql, tuple(row))
        copied += 1

    cur.execute(f'DROP TABLE {_qid(table)}')
    cur.execute(f'ALTER TABLE {_qid(tmp_table)} RENAME TO {_qid(table)}')
    return copied


def _update_ids_in_table(conn: sqlite3.Connection, table: str, target_cols: set[str], id_map: dict[str, str]) -> int:
    # 按严格相等匹配替换ID，CAST成TEXT后比对
    cols_info = _get_table_info(conn, table)
    cols = [c[1] for c in cols_info]
    hit_cols = [c for c in cols if c in target_cols]
    if not hit_cols:
        return 0

    cur = conn.cursor()
    updated_cells = 0
    for col in hit_cols:
        for old_id, new_id in id_map.items():
            if str(old_id) == str(new_id):
                continue
            sql = f'UPDATE {_qid(table)} SET {_qid(col)}=? WHERE CAST({_qid(col)} AS TEXT)=?'
            cur.execute(sql, (str(new_id), str(old_id)))
            if cur.rowcount and cur.rowcount > 0:
                updated_cells += cur.rowcount
    return updated_cells


def _collect_all_candidate_ids(db_paths: dict, target_cols_by_db: dict) -> set[str]:
    # 从所有数据库目标字段中收集候选ID（去重）
    all_ids = set()

    for db_name, db_path in db_paths.items():
        if not db_path.exists():
            continue

        conn = sqlite3.connect(db_path, check_same_thread=False)
        try:
            tables = _get_tables(conn)
            for table in tables:
                cols_info = _get_table_info(conn, table)
                cols = {c[1] for c in cols_info}
                hit_cols = cols.intersection(target_cols_by_db.get(db_name, set()))
                if not hit_cols:
                    continue

                cur = conn.cursor()
                for col in hit_cols:
                    sql = f'SELECT DISTINCT CAST({_qid(col)} AS TEXT) FROM {_qid(table)} WHERE {_qid(col)} IS NOT NULL'
                    cur.execute(sql)
                    rows = cur.fetchall()
                    for r in rows:
                        if r and r[0] is not None:
                            v = str(r[0]).strip()
                            if v != "":
                                all_ids.add(v)
        finally:
            conn.close()

    return all_ids


def migrate_user_id_to_openid():
    """
    迁移逻辑：
    1) 先备份数据库
    2) 将目标字段类型统一改为TEXT
    3) 一次性收集所有目标字段中的ID，统一生成映射
    4) 一次性替换所有目标字段中的ID
    """
    try:
        # 先备份
        ok, backup_msg = backup_db_files()
        if not ok:
            return False, f"备份失败，终止迁移：{backup_msg}"

        db_paths = {
            "xiuxian.db": DATABASE / "xiuxian.db",
            "xiuxian_impart.db": DATABASE / "xiuxian_impart.db",
            "trade.db": DATABASE / "trade.db",
            "player.db": DATABASE / "player.db",
        }

        # 各库需要迁移的字段（player.db新增 main_id、active_id）
        target_cols_by_db = {
            "xiuxian.db": {"user_id", "sect_owner"},
            "xiuxian_impart.db": {"user_id"},
            "trade.db": {"user_id"},
            "player.db": {"user_id", "partner_id", "group_id", "main_id", "active_id"},
        }

        # =========================
        # A. 先全库目标字段改TEXT
        # =========================
        type_logs = []
        for db_name, db_path in db_paths.items():
            if not db_path.exists():
                type_logs.append(f"{db_name}: 不存在，跳过")
                continue

            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("PRAGMA foreign_keys=OFF")
            cur = conn.cursor()

            try:
                cur.execute("BEGIN")
                tables = _get_tables(conn)
                converted_rows = 0

                for table in tables:
                    to_text_cols = target_cols_by_db.get(db_name, {"user_id"})
                    converted_rows += _rebuild_table_convert_columns_to_text(conn, table, to_text_cols)

                conn.commit()
                type_logs.append(f"{db_name}: 字段类型处理完成，重建拷贝行数={converted_rows}")
            except Exception as e:
                conn.rollback()
                type_logs.append(f"{db_name}: 字段类型处理失败 -> {e}")
            finally:
                conn.close()

        # =========================
        # B. 一次性收集所有候选ID并映射
        # =========================
        all_candidate_ids = _collect_all_candidate_ids(db_paths, target_cols_by_db)
        if not all_candidate_ids:
            return False, "未找到任何可迁移ID"

        from .utils import get_real_id
        id_map = {}
        fail_ids = []

        for old_id in all_candidate_ids:
            try:
                real_id = get_real_id(old_id)
                if real_id and str(real_id).strip():
                    id_map[str(old_id)] = str(real_id).strip()
                else:
                    fail_ids.append(old_id)
            except Exception:
                fail_ids.append(old_id)

        if not id_map:
            return False, "真实ID转换全部失败，未执行替换"

        # =========================
        # C. 全库替换（一次）
        # =========================
        data_logs = []
        total_updated = 0

        for db_name, db_path in db_paths.items():
            if not db_path.exists():
                data_logs.append(f"{db_name}: 不存在，跳过")
                continue

            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("PRAGMA foreign_keys=OFF")
            cur = conn.cursor()

            try:
                cur.execute("BEGIN")
                tables = _get_tables(conn)
                target_cols = target_cols_by_db.get(db_name, {"user_id"})

                db_updated = 0
                for table in tables:
                    db_updated += _update_ids_in_table(conn, table, target_cols, id_map)

                conn.commit()
                total_updated += db_updated
                data_logs.append(f"{db_name}: ID替换完成，更新单元格={db_updated}")
            except Exception as e:
                conn.rollback()
                data_logs.append(f"{db_name}: ID替换失败 -> {e}")
            finally:
                conn.close()

        # 可选迁移 players 目录名
        players_dir = DATABASE / "players"
        rename_count = 0
        if players_dir.exists():
            for old_id, new_id in id_map.items():
                old_p = players_dir / str(old_id)
                new_p = players_dir / str(new_id)
                if old_p.exists() and (not new_p.exists()):
                    try:
                        old_p.rename(new_p)
                        rename_count += 1
                    except Exception:
                        pass

        msg = (
            f"QQID转换完成！\n"
            f"备份：{backup_msg}\n"
            f"候选ID总数：{len(all_candidate_ids)}\n"
            f"成功映射：{len(id_map)}\n"
            f"转换失败：{len(fail_ids)}\n"
            f"更新总单元格：{total_updated}\n"
            f"players目录改名：{rename_count}\n"
            f"\n[字段类型阶段]\n" + "\n".join(type_logs) +
            f"\n\n[数据替换阶段]\n" + "\n".join(data_logs)
        )
        return True, msg

    except Exception as e:
        return False, f"迁移异常中止：{e}"

def migrate_single_user_id(old_id: str, new_id: str):
    """
    手动迁移单个用户ID：old_id -> new_id
    规则：
    1) old_id不存在：不更新并提示
    2) new_id已存在：不更新并提示
    3) 执行前自动备份数据库
    """
    try:
        old_id = str(old_id).strip()
        new_id = str(new_id).strip()

        if not old_id or not new_id:
            return False, "参数错误：ID1 和 ID2 不能为空"

        if old_id == new_id:
            return False, "ID1 与 ID2 相同，无需更新"

        db_paths = {
            "xiuxian.db": DATABASE / "xiuxian.db",
            "xiuxian_impart.db": DATABASE / "xiuxian_impart.db",
            "trade.db": DATABASE / "trade.db",
            "player.db": DATABASE / "player.db",
        }

        # 各库需要迁移的字段（与批量迁移保持一致）
        target_cols_by_db = {
            "xiuxian.db": {"user_id", "sect_owner"},
            "xiuxian_impart.db": {"user_id"},
            "trade.db": {"user_id"},
            "player.db": {"user_id", "partner_id", "group_id", "main_id", "active_id"},
        }

        # 先判断 old_id 是否存在、new_id 是否已存在
        old_exists = False
        new_exists = False
        old_exists_detail = []
        new_exists_detail = []

        for db_name, db_path in db_paths.items():
            if not db_path.exists():
                continue

            conn = sqlite3.connect(db_path, check_same_thread=False)
            try:
                tables = _get_tables(conn)
                target_cols = target_cols_by_db.get(db_name, {"user_id"})
                cur = conn.cursor()

                for table in tables:
                    cols_info = _get_table_info(conn, table)
                    cols = {c[1] for c in cols_info}
                    hit_cols = cols.intersection(target_cols)
                    if not hit_cols:
                        continue

                    for col in hit_cols:
                        # 检查 old_id
                        cur.execute(
                            f'SELECT 1 FROM {_qid(table)} WHERE CAST({_qid(col)} AS TEXT)=? LIMIT 1',
                            (old_id,)
                        )
                        if cur.fetchone():
                            old_exists = True
                            old_exists_detail.append(f"{db_name}.{table}.{col}")

                        # 检查 new_id
                        cur.execute(
                            f'SELECT 1 FROM {_qid(table)} WHERE CAST({_qid(col)} AS TEXT)=? LIMIT 1',
                            (new_id,)
                        )
                        if cur.fetchone():
                            new_exists = True
                            new_exists_detail.append(f"{db_name}.{table}.{col}")
            finally:
                conn.close()

        if not old_exists:
            return False, f"ID1（{old_id}）不存在，未执行更新"

        if new_exists:
            return False, f"ID2（{new_id}）已存在，禁止覆盖。\n命中位置：{', '.join(new_exists_detail[:10])}"

        # 开始更新
        total_updated = 0
        logs = []
        id_map = {old_id: new_id}

        for db_name, db_path in db_paths.items():
            if not db_path.exists():
                logs.append(f"{db_name}: 不存在，跳过")
                continue

            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("PRAGMA foreign_keys=OFF")
            cur = conn.cursor()

            try:
                cur.execute("BEGIN")
                tables = _get_tables(conn)
                target_cols = target_cols_by_db.get(db_name, {"user_id"})

                db_updated = 0
                for table in tables:
                    db_updated += _update_ids_in_table(conn, table, target_cols, id_map)

                conn.commit()
                total_updated += db_updated
                logs.append(f"{db_name}: 更新单元格={db_updated}")
            except Exception as e:
                conn.rollback()
                logs.append(f"{db_name}: 更新失败 -> {e}")
            finally:
                conn.close()

        # 可选：迁移 players 目录名
        players_dir = DATABASE / "players"
        rename_msg = "未处理"
        if players_dir.exists():
            old_p = players_dir / old_id
            new_p = players_dir / new_id
            if old_p.exists() and (not new_p.exists()):
                try:
                    old_p.rename(new_p)
                    rename_msg = "已重命名"
                except Exception as e:
                    rename_msg = f"重命名失败: {e}"
            elif not old_p.exists():
                rename_msg = "旧目录不存在，跳过"
            else:
                rename_msg = "新目录已存在，跳过"

        msg = (
            f"手动ID更新完成：{old_id} -> {new_id}\n"
            f"总更新单元格：{total_updated}\n"
            f"players目录：{rename_msg}\n"
            f"详情：\n" + "\n".join(logs)
        )
        return True, msg

    except Exception as e:
        return False, f"手动ID更新异常：{e}"

def swap_two_user_ids(id1: str, id2: str):
    """
    交换两个用户ID：
    1) id1 -> id1bak
    2) id2 -> id1
    3) id1bak -> id2

    规则：
    - id1、id2 都必须存在
    - 执行前自动备份
    - 任一步失败则回滚（按库事务）
    """
    try:
        id1 = str(id1).strip()
        id2 = str(id2).strip()

        if not id1 or not id2:
            return False, "参数错误：ID1 和 ID2 不能为空"
        if id1 == id2:
            return False, "ID1 与 ID2 相同，无法交换"

        db_paths = {
            "xiuxian.db": DATABASE / "xiuxian.db",
            "xiuxian_impart.db": DATABASE / "xiuxian_impart.db",
            "trade.db": DATABASE / "trade.db",
            "player.db": DATABASE / "player.db",
        }

        target_cols_by_db = {
            "xiuxian.db": {"user_id", "sect_owner"},
            "xiuxian_impart.db": {"user_id"},
            "trade.db": {"user_id"},
            "player.db": {"user_id", "partner_id", "group_id", "main_id", "active_id"},
        }

        # 生成临时bak，避免冲突
        id1_bak = f"{id1}__bak__{int(time.time())}"

        # 前置存在性检查
        id1_exists = False
        id2_exists = False

        for db_name, db_path in db_paths.items():
            if not db_path.exists():
                continue

            conn = sqlite3.connect(db_path, check_same_thread=False)
            try:
                tables = _get_tables(conn)
                target_cols = target_cols_by_db.get(db_name, {"user_id"})
                cur = conn.cursor()

                for table in tables:
                    cols_info = _get_table_info(conn, table)
                    cols = {c[1] for c in cols_info}
                    hit_cols = cols.intersection(target_cols)
                    if not hit_cols:
                        continue

                    for col in hit_cols:
                        cur.execute(
                            f'SELECT 1 FROM {_qid(table)} WHERE CAST({_qid(col)} AS TEXT)=? LIMIT 1',
                            (id1,)
                        )
                        if cur.fetchone():
                            id1_exists = True

                        cur.execute(
                            f'SELECT 1 FROM {_qid(table)} WHERE CAST({_qid(col)} AS TEXT)=? LIMIT 1',
                            (id2,)
                        )
                        if cur.fetchone():
                            id2_exists = True
            finally:
                conn.close()

        if not id1_exists or not id2_exists:
            return False, f"交换失败：ID1存在={id1_exists}，ID2存在={id2_exists}。要求两者都存在。"

        total_updated = 0
        logs = []

        # 按库执行三步替换，每个库单独事务
        for db_name, db_path in db_paths.items():
            if not db_path.exists():
                logs.append(f"{db_name}: 不存在，跳过")
                continue

            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("PRAGMA foreign_keys=OFF")
            cur = conn.cursor()

            try:
                cur.execute("BEGIN")
                tables = _get_tables(conn)
                target_cols = target_cols_by_db.get(db_name, {"user_id"})

                db_updated = 0
                # 第一步：id1 -> id1_bak
                for table in tables:
                    db_updated += _update_ids_in_table(conn, table, target_cols, {id1: id1_bak})
                # 第二步：id2 -> id1
                for table in tables:
                    db_updated += _update_ids_in_table(conn, table, target_cols, {id2: id1})
                # 第三步：id1_bak -> id2
                for table in tables:
                    db_updated += _update_ids_in_table(conn, table, target_cols, {id1_bak: id2})

                conn.commit()
                total_updated += db_updated
                logs.append(f"{db_name}: 更新单元格={db_updated}")
            except Exception as e:
                conn.rollback()
                logs.append(f"{db_name}: 交换失败 -> {e}")
                return False, f"ID交换失败并已回滚（{db_name}）：{e}"
            finally:
                conn.close()

        # players目录也做交换
        players_dir = DATABASE / "players"
        rename_msg = "未处理"
        if players_dir.exists():
            p1 = players_dir / id1
            p2 = players_dir / id2
            pb = players_dir / id1_bak
            try:
                # 同样三步交换
                if p1.exists():
                    p1.rename(pb)
                if p2.exists():
                    p2.rename(p1)
                if pb.exists():
                    pb.rename(p2)
                rename_msg = "已完成交换"
            except Exception as e:
                rename_msg = f"目录交换失败: {e}"

        msg = (
            f"ID交换完成：{id1} - {id2}\n"
            f"总更新单元格：{total_updated}\n"
            f"players目录：{rename_msg}\n"
            f"详情：\n" + "\n".join(logs)
        )
        return True, msg

    except Exception as e:
        return False, f"ID交换异常：{e}"

driver = get_driver()
sql_message = XiuxianDateManage()  # sql类
items = Items()
trade_manager = TradeDataManager()
player_data_manager = PlayerDataManager()

@driver.on_shutdown
async def close_db():
    # 统一调用单例关闭连接
    XiuxianDateManage().close()
    XIUXIAN_IMPART_BUFF().close()
    TradeDataManager().close()
    PlayerDataManager().close()
