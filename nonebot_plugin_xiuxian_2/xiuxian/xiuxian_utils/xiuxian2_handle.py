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
from nonebot.log import logger
from .data_source import jsondata
from ..xiuxian_config import XiuConfig, convert_rank
# from .. import DRIVER
from nonebot import get_driver
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
            else:
                self.database_path /= "xiuxian.db"
                self.conn = sqlite3.connect(self.database_path, check_same_thread=False)
            logger.opt(colors=True).info(f"<green>修仙数据库已连接！</green>")
            self._check_data()

    def close(self):
        self.conn.close()
        logger.opt(colors=True).info(f"<green>修仙数据库关闭！</green>")

    def _check_data(self):
        """检查数据完整性"""
        c = self.conn.cursor()

        for i in XiuConfig().sql_table:
            if i == "user_xiuxian":
                try:
                    c.execute(f"select count(1) from {i}")
                except sqlite3.OperationalError:
                    c.execute("""CREATE TABLE "user_xiuxian" (
      "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      "user_id" INTEGER NOT NULL,
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
  "user_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  "type" integer DEFAULT 0,
  "create_time" integer DEFAULT NULL,
  "scheduled_time" integer,
  "last_check_info_time" integer DEFAULT NULL
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
  "user_id" INTEGER NOT NULL,
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
  "user_id" integer DEFAULT 0,
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
                logger.opt(colors=True).info("<yellow>sql_user_xiuxian有字段不存在，开始创建\n</yellow>")
                sql = f"ALTER TABLE user_xiuxian ADD COLUMN {i} INTEGER DEFAULT 0;"
                logger.opt(colors=True).info(f"<green>{sql}</green>")
                c.execute(sql)

        for d in XiuConfig().sql_user_cd:
            try:
                c.execute(f"select {d} from user_cd")
            except sqlite3.OperationalError:
                logger.opt(colors=True).info("<yellow>sql_user_cd有字段不存在，开始创建</yellow>")
                sql = f"ALTER TABLE user_cd ADD COLUMN {d} INTEGER DEFAULT 0;"
                logger.opt(colors=True).info(f"<green>{sql}</green>")
                c.execute(sql)

        for s in XiuConfig().sql_sects:
            try:
                c.execute(f"select {s} from sects")
            except sqlite3.OperationalError:
                logger.opt(colors=True).info("<yellow>sql_sects有字段不存在，开始创建</yellow>")
                sql = f"ALTER TABLE sects ADD COLUMN {s} INTEGER DEFAULT 0;"
                logger.opt(colors=True).info(f"<green>{sql}</green>")
                c.execute(sql)

        for m in XiuConfig().sql_buff:
            try:
                c.execute(f"select {m} from BuffInfo")
            except sqlite3.OperationalError:
                logger.opt(colors=True).info("<yellow>sql_buff有字段不存在，开始创建</yellow>")
                sql = f"ALTER TABLE BuffInfo ADD COLUMN {m} INTEGER DEFAULT 0;"
                logger.opt(colors=True).info(f"<green>{sql}</green>")
                c.execute(sql)

        for b in XiuConfig().sql_back:
            try:
                c.execute(f"select {b} from back")
            except sqlite3.OperationalError:
                logger.opt(colors=True).info("<yellow>sql_back有字段不存在，开始创建</yellow>")
                sql = f"ALTER TABLE back ADD COLUMN {b} INTEGER DEFAULT 0;"
                logger.opt(colors=True).info(f"<green>{sql}</green>")
                c.execute(sql)
        
        # 检查并更新 last_check_info_time 列的记录
        c.execute(f"""UPDATE user_cd
SET last_check_info_time = ?
WHERE last_check_info_time = '0' OR last_check_info_time IS NULL
        """, (current_time,))

        self.conn.commit()

    @classmethod
    def close_dbs(cls):
        XiuxianDateManage().close()

    def _create_user(self, user_id: str, root: str, type: str, power: str, create_time, user_name) -> None:
        """在数据库中创建用户并初始化"""
        c = self.conn.cursor()
        sql = f"INSERT INTO user_xiuxian (user_id, stone, root, root_type, root_level, level, power, create_time, user_name, exp, work_num, sect_id, sect_position, user_stamina, is_novice) VALUES (?, 0, ?, ?, 0, '江湖好手', ?, ?, ?, 100, 5, NULL, NULL, ?, 0)"
        c.execute(sql, (user_id, root, type, power, create_time, user_name,XiuConfig().max_stamina))
        self.conn.commit()

    def today_active_users(self):
        """获取今日活跃用户数（今天有操作记录的用户）"""
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
        cur = self.conn.cursor()
        # 获取昨天的日期
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
        cur = self.conn.cursor()
        # 获取7天前的日期
        seven_days_ago = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
        # 注意：这里使用 >= 条件，统计从7天前到今天（共7天）的数据
        sql = f"SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE date(create_time) >= ?"
        cur.execute(sql, (seven_days_ago,))
        result = cur.fetchone()
        if result:
            return result[0]
        else:
            return 0

    def all_users(self):
        """获取全部用户数"""
        cur = self.conn.cursor()
        sql = "SELECT COUNT(*) FROM user_xiuxian"
        cur.execute(sql)
        result = cur.fetchone()
        if result:
            return result[0]
        else:
            return 0

    def total_items_quantity(self):
        """获取全部用户背包的物品数量总合"""
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
        cur = self.conn.cursor()
        max_stamina = XiuConfig().max_stamina

        if key == 1:  # 增加体力
            cur.execute("SELECT user_stamina FROM user_xiuxian WHERE user_id=?", (user_id,))
            current_stamina = cur.fetchone()[0]
            new_stamina = min(current_stamina + stamina_change, max_stamina)
            if current_stamina < max_stamina:
                sql = "UPDATE user_xiuxian SET user_stamina=? WHERE user_id=?"
                cur.execute(sql, (new_stamina, user_id))
                self.conn.commit()
                
        elif key == 2:  # 减少体力
            sql = "UPDATE user_xiuxian SET user_stamina=MAX(user_stamina-?, 0) WHERE user_id=?"
            cur.execute(sql, (stamina_change, user_id))
            self.conn.commit()
 
    def get_user_real_info(self, user_id):
        """根据USER_ID获取用户信息,获取功法加成"""
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
        player = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '防御': 0}
        userinfo = sql_message.get_user_real_info(user_id)
        user_weapon_data = UserBuffDate(userinfo['user_id']).get_user_weapon_data()

        impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
        boss_atk = impart_data['boss_atk'] if impart_data['boss_atk'] is not None else 0
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
        :param sect_id: 宗门编号
        :return:
        """
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
        cur = self.conn.cursor()
        sql = f"SELECT user_id FROM user_xiuxian WHERE sect_position = 0"
        cur.execute(sql)
        result = cur.fetchall()
        return [row[0] for row in result]
    
    def get_elders(self):
        """获取所有长老的 user_id"""
        cur = self.conn.cursor()
        sql = f"SELECT user_id FROM user_xiuxian WHERE sect_position = 2"
        cur.execute(sql)
        result = cur.fetchall()
        return [row[0] for row in result]

    def create_user(self, user_id, *args):
        """校验用户是否存在"""
        cur = self.conn.cursor()
        sql = f"select * from user_xiuxian WHERE user_id=?"
        cur.execute(sql, (user_id,))
        result = cur.fetchone()
        if not result:
            self._create_user(user_id, args[0], args[1], args[2], args[3], args[4]) # root, type, power, create_time, user_name
            self.conn.commit()
            welcome_msg = f"欢迎进入修仙世界的，你的灵根为：{args[0]},类型是：{args[1]},你的战力为：{args[2]},当前境界：江湖好手"
            return True, welcome_msg
        else:
            return False, f"您已迈入修仙世界，输入【我的修仙信息】获取数据吧！"

    def get_sign(self, user_id):
        """获取用户签到信息"""
        cur = self.conn.cursor()
        sql = "select is_sign from user_xiuxian WHERE user_id=?"
        cur.execute(sql, (user_id,))
        result = cur.fetchone()
        if not result:
            return f"修仙界没有你的足迹，输入 我要修仙 加入修仙世界吧！"
        elif result[0] == 0:
            ls = random.randint(XiuConfig().sign_in_lingshi_lower_limit, XiuConfig().sign_in_lingshi_upper_limit)
            sql2 = f"UPDATE user_xiuxian SET is_sign=1,stone=stone+? WHERE user_id=?"
            cur.execute(sql2, (ls,user_id))
            self.conn.commit()
            return f"签到成功，获取{ls}块灵石!"
        elif result[0] == 1:
            return f"贪心的人是不会有好运的！"
        
    def get_beg(self, user_id):
        """获取仙途奇缘信息"""
        cur = self.conn.cursor()
        sql = f"select is_beg from user_xiuxian WHERE user_id=?"
        cur.execute(sql, (user_id,))
        result = cur.fetchone()
        if result[0] == 0:
            ls = random.randint(XiuConfig().beg_lingshi_lower_limit, XiuConfig().beg_lingshi_upper_limit)
            sql2 = f"UPDATE user_xiuxian SET is_beg=1,stone=stone+? WHERE user_id=?"
            cur.execute(sql2, (ls,user_id))
            self.conn.commit()
            return ls
        elif result[0] == 1:
            return None
            
    def get_novice(self, user_id):
        """检查用户是否已领取新手礼包"""
        cur = self.conn.cursor()
        sql = f"select is_novice from user_xiuxian WHERE user_id=?"
        cur.execute(sql, (user_id,))
        result = cur.fetchone()
        if result[0] == 0:            
            return True  # 可以领取
        elif result[0] == 1:
            return None  # 已领取

    def save_novice(self, user_id):
        """标记用户已领取新手礼包"""
        sql = f"UPDATE user_xiuxian SET is_novice=1 WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()
        
    def novice_remake(self):
        """重置新手礼包"""
        sql = f"UPDATE user_xiuxian SET is_novice=0"
        cur = self.conn.cursor()
        cur.execute(sql, )
        self.conn.commit()
    
    def get_user_create_time(self, user_id):
        """获取用户创建时间"""
        cur = self.conn.cursor()
        sql = f"SELECT create_time FROM user_xiuxian WHERE user_id=?"
        cur.execute(sql, (user_id,))
        result = cur.fetchone()
        if result:
            return datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S.%f")
        return None
    
    def ramaker(self, lg, type, user_id):
        """洗灵根"""
        cur = self.conn.cursor()
        sql = f"UPDATE user_xiuxian SET root=?,root_type=?,stone=stone-? WHERE user_id=?"
        cur.execute(sql, (lg, type, XiuConfig().remake, user_id))
        self.conn.commit()

        self.update_power2(user_id) # 更新战力
        return f"逆天之行，重获新生，新的灵根为：{lg}，类型为：{type}"

    def get_root_rate(self, name, user_id):
        """获取灵根倍率"""
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
        data = jsondata.level_data()
        return data[name]['power']
    
    def get_level_cost(self, name):
        """获取炼体境界倍率"""
        data = jsondata.exercises_level_data()
        return data[name]['cost_exp'], data[name]['cost_stone']

    def update_power2(self, user_id) -> None:
        """更新战力"""
        UserMessage = self.get_user_info_with_id(user_id)
        cur = self.conn.cursor()
        level = jsondata.level_data()
        root_rate = sql_message.get_root_rate(UserMessage['root_type'], user_id) 
        sql = f"UPDATE user_xiuxian SET power=round(exp*?*?,0) WHERE user_id=?"
        cur.execute(sql, (root_rate, level[UserMessage['level']]["spend"], user_id))
        self.conn.commit()

    def update_ls(self, user_id, price, key):
        """更新灵石  1为增加，2为减少"""
        cur = self.conn.cursor()
        price = abs(price)
        if key == 1:
            sql = f"UPDATE user_xiuxian SET stone=stone+? WHERE user_id=?"
            cur.execute(sql, (price, user_id))
            self.conn.commit()
        elif key == 2:
            sql = f"UPDATE user_xiuxian SET stone=stone-? WHERE user_id=?"
            cur.execute(sql, (price, user_id))
            self.conn.commit()

    def update_root(self, user_id, key):
        """更新灵根  1为混沌,2为融合,3为超,4为龙,5为天,6为千世,7为万世,8为永恒,9为命运"""
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

        return root_name  # 返回灵根名称

    def update_ls_all(self, price):
        """所有用户增加灵石"""
        cur = self.conn.cursor()
        sql = f"UPDATE user_xiuxian SET stone=stone+?"
        cur.execute(sql, (price,))
        self.conn.commit()
    
    def get_exp_rank(self, user_id):
        """修为排行"""
        sql = f"select rank from(select user_id,exp,dense_rank() over (ORDER BY exp desc) as 'rank' FROM user_xiuxian) WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        result = cur.fetchone()
        return result

    def get_stone_rank(self, user_id):
        """灵石排行"""
        sql = f"select rank from(select user_id,stone,dense_rank() over (ORDER BY stone desc) as 'rank' FROM user_xiuxian) WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        result = cur.fetchone()
        return result
    
    def get_ls_rank(self):
        """灵石排行榜"""
        sql = f"SELECT user_id,stone FROM user_xiuxian  WHERE stone>0 ORDER BY stone DESC LIMIT 5"
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchall()
        return result
        
    def sign_remake(self):
        """重置签到"""
        sql = f"UPDATE user_xiuxian SET is_sign=0"
        cur = self.conn.cursor()
        cur.execute(sql, )
        self.conn.commit()

    def beg_remake(self):
        """重置仙途奇缘"""
        sql = f"UPDATE user_xiuxian SET is_beg=0"
        cur = self.conn.cursor()
        cur.execute(sql, )
        self.conn.commit()

    def ban_user(self, user_id):
        """小黑屋"""
        sql = f"UPDATE user_xiuxian SET is_ban=1 WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()

    def update_mixelixir_num(self, user_id):
        """增加炼丹次数"""
        sql = f"UPDATE user_xiuxian SET mixelixir_num=mixelixir_num+1 WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()

    def update_user_name(self, user_id, user_name):
        """更新用户道号"""
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
        sql = f"UPDATE user_xiuxian SET level_up_cd=? WHERE user_id=?"
        cur = self.conn.cursor()
        now_time = datetime.now()
        cur.execute(sql, (now_time, user_id))
        self.conn.commit()
    
    def update_last_check_info_time(self, user_id):
        """更新查看修仙信息时间"""
        sql = "UPDATE user_cd SET last_check_info_time = ? WHERE user_id = ?"
        cur = self.conn.cursor()
        now_time = datetime.now()
        cur.execute(sql, (now_time, user_id))
        self.conn.commit()

    def get_last_check_info_time(self, user_id):
        """获取最后一次查看修仙信息时间"""
        cur = self.conn.cursor()
        sql = "SELECT last_check_info_time FROM user_cd WHERE user_id = ?"
        cur.execute(sql, (user_id,))
        result = cur.fetchone()
        if result:
           return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
        else:
            return None
        
    
    def updata_level(self, user_id, level_name):
        """更新境界"""
        sql = f"UPDATE user_xiuxian SET level=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (level_name, user_id))
        self.conn.commit()


    def updata_root_level(self, user_id, level_num):
        """更新轮回等级"""
        sql = f"UPDATE user_xiuxian SET root_level=root_level+? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (level_num, user_id))
        self.conn.commit()
        
        
    def get_user_cd(self, user_id):
        """
        获取用户操作CD
        :param user_id: QQ
        :return: 用户CD信息的字典
        """
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
        """
        添加用户至CD表
        :param user_id: qq
        :return:
        """
        sql = f"INSERT INTO user_cd (user_id) VALUES (?)"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()


    def create_sect(self, user_id, sect_name) -> None:
        """
        创建宗门
        :param user_id:qq
        :param sect_name:宗门名称
        :return:
        """
        sql = f"INSERT INTO sects(sect_name, sect_owner, sect_scale, sect_used_stone, join_open, closed, combat_power) VALUES (?,?,0,0,1,0,0)"
        cur = self.conn.cursor()
        cur.execute(sql, (sect_name, user_id))
        self.conn.commit()

    def update_sect_name(self, sect_id, sect_name) -> None:
        """
        修改宗门名称
        :param sect_id: 宗门id
        :param sect_name: 宗门名称
        :return: 返回是否更新成功的标志，True表示更新成功，False表示更新失败（已存在同名宗门）
        """
        cur = self.conn.cursor()
        get_sect_name = f"select sect_name from sects WHERE sect_name=?"
        cur.execute(get_sect_name, (sect_name,))
        result = cur.fetchone()
        if result:
            return False
        else:
            sql = f"UPDATE sects SET sect_name=? WHERE sect_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (sect_name, sect_id))
            self.conn.commit()
            return True

    def get_sect_info_by_qq(self, user_id):
        """
        通过用户qq获取宗门信息
        :param user_id:
        :return:
        """
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
        """
        计算宗门战力（所有成员战力总和）
        :param sect_id: 宗门ID
        :return: 宗门总战力
        """
        members = self.get_all_users_by_sect_id(sect_id)
        total_power = 0
        
        for member in members:
            user_real_info = self.get_user_real_info(member['user_id'])
            if user_real_info and 'power' in user_real_info:
                total_power += user_real_info['power']
        
        return total_power

    def update_sect_combat_power(self, sect_id):
        """
        更新宗门战力
        :param sect_id: 宗门ID
        """
        total_power = self.calculate_sect_combat_power(sect_id)
        sql = "UPDATE sects SET combat_power = ? WHERE sect_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (total_power, sect_id))
        self.conn.commit()
        return total_power

    def combat_power_top(self):
        """宗门战力排行榜"""
        sql = f"SELECT sect_id, sect_name, combat_power FROM sects WHERE sect_owner is NOT NULL ORDER BY combat_power DESC LIMIT 50"
        cur = self.conn.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        return result

    def get_sect_info_by_id(self, sect_id):
        """
        通过宗门id获取宗门信息
        :param sect_id:
        :return:
        """
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
        """
        更新用户信息表的宗门信息字段
        :param user_id:
        :param usr_sect_id:
        :param usr_sect_position:
        :return:
        """
        sql = f"UPDATE user_xiuxian SET sect_id=?,sect_position=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (usr_sect_id, usr_sect_position, user_id))
        self.conn.commit()

    def update_sect_owner(self, user_id, sect_id):
        """
        更新宗门所有者
        :param user_id:
        :param usr_sect_id:
        :return:
        """
        sql = f"UPDATE sects SET sect_owner=? WHERE sect_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id, sect_id))
        self.conn.commit()

    def get_highest_contrib_user_except_current(self, sect_id, current_owner_id):
        """
        获取指定宗门的贡献最高的人，排除当前宗主
        :param sect_id: 宗门ID
        :param current_owner_id: 当前宗主的ID
        :return: 贡献最高的人的ID，如果没有则返回None
        """
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
        sql = f"UPDATE sects SET join_open=? WHERE sect_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (status, sect_id))
        self.conn.commit()

    def update_sect_closed_status(self, sect_id, status):
        """更新宗门封闭状态"""
        sql = f"UPDATE sects SET closed=? WHERE sect_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (status, sect_id))
        self.conn.commit()

    def delete_sect(self, sect_id):
        """删除宗门并踢出所有成员"""
        cur = self.conn.cursor()
    
        try:
            # 1. 先获取所有宗门成员
            members_sql = "SELECT user_id FROM user_xiuxian WHERE sect_id = ?"
            cur.execute(members_sql, (sect_id,))
            members = cur.fetchall()
            
            # 2. 踢出所有成员（重置宗门信息）
            if members:
                update_sql = """
                    UPDATE user_xiuxian 
                    SET sect_id = NULL, sect_position = NULL, sect_contribution = 0 
                    WHERE sect_id = ?
                """
                cur.execute(update_sql, (sect_id,))
                logger.opt(colors=True).info(f"<green>已踢出宗门 {sect_id} 的所有成员，共 {len(members)} 人</green>")
        
            # 3. 删除宗门记录
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
        """
        通过宗门名称获取宗门ID
        :param sect_name: 宗门名称
        :return: 宗门ID，如果不存在返回None
        """
        cur = self.conn.cursor()
        sql = "SELECT sect_id FROM sects WHERE sect_name = ?"
        cur.execute(sql, (sect_name,))
        result = cur.fetchone()
        if result:
            return result[0]  # 返回宗门ID
        else:
            return None
        
    def get_all_sect_id(self):
        """获取全部宗门id"""
        sql = "SELECT sect_id FROM sects"
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchall()
        if result:
            return result
        else:
            return None

    def get_all_user_id(self):
        """获取全部用户id"""
        sql = "SELECT user_id FROM user_xiuxian"
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchall()
        if result:
            return [row[0] for row in result]
        else:
            return None


    def in_closing(self, user_id, the_type):
        """
        更新用户操作CD
        :param user_id: qq
        :param the_type: 0:无状态  1:闭关中  2:历练中  4:虚神界闭关中  5:修炼中
        :return:
        """
        now_time = None
        if the_type == 0:
            now_time = 0
        else:
            now_time = datetime.now()
        sql = "UPDATE user_cd SET type=?,create_time=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (the_type, now_time, user_id))
        self.conn.commit()


    def update_exp(self, user_id, exp):
        """增加修为"""
        exp = number_count(exp)
        sql = "UPDATE user_xiuxian SET exp=exp+? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (exp, user_id))
        self.conn.commit()
        
    def update_j_exp(self, user_id, exp):
        """减少修为"""
        exp = number_count(exp)
        sql = "UPDATE user_xiuxian SET exp=exp-? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (exp, user_id))
        self.conn.commit()

    def del_exp_decimal(self, user_id, exp):
        """去浮点"""
        sql = "UPDATE user_xiuxian SET exp=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (int(exp), user_id))
        self.conn.commit()

    
    def realm_top(self):
        """境界排行榜前50"""
        rank_mapping = {rank: idx for idx, rank in enumerate(convert_rank('江湖好手')[1])}
    
        sql = """SELECT user_name, level, exp FROM user_xiuxian 
            WHERE user_name IS NOT NULL
            ORDER BY exp DESC, (CASE level """
    
        for level, value in sorted(rank_mapping.items(), key=lambda x: x[1], reverse=True):
            sql += f"WHEN '{level}' THEN '{value:02}' "
    
        sql += """ELSE level END) ASC LIMIT 50"""
    
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchall()
        return result


    def stone_top(self):
        """这也是灵石排行榜"""
        sql = f"SELECT user_name,stone FROM user_xiuxian WHERE user_name is NOT NULL ORDER BY stone DESC LIMIT 50"
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchall()
        return result

    def power_top(self):
        """战力排行榜"""
        sql = f"SELECT user_name,power FROM user_xiuxian WHERE user_name is NOT NULL ORDER BY power DESC LIMIT 50"
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchall()
        return result

    def scale_top(self):
        """
        宗门建设度排行榜
        :return:
        """
        sql = f"SELECT sect_id, sect_name, sect_scale FROM sects WHERE sect_owner is NOT NULL ORDER BY sect_scale DESC"
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchall()
        return result

    def root_top(self):
        """这是轮回排行榜"""
        sql = f"SELECT user_name,root_level FROM user_xiuxian WHERE user_name is NOT NULL ORDER BY root_level DESC LIMIT 50"
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchall()
        return result
        
    def get_all_sects(self):
        """
        获取所有宗门信息
        :return: 宗门信息字典列表
        """
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
        """
        获取所有宗门及其各个宗门成员数
        """
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
        """
        更新用户的最后奇缘时间

        :param user_id: 用户ID
        :param is_beg: 'YYYY-MM-DD HH:MM:SS'
        """
        cur = self.conn.cursor()
        sql = "UPDATE user_xiuxian SET is_beg=? WHERE user_id=?"
        cur.execute(sql, (is_beg, user_id))
        self.conn.commit()


    def get_top1_user(self):
        """
        获取修为第一的用户
        """
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
        """
        获取境界第一的用户
        """
        rank_mapping = {rank: idx for idx, rank in enumerate(convert_rank('江湖好手')[1])}
    
        sql = """SELECT user_name, level, exp FROM user_xiuxian 
            WHERE user_name IS NOT NULL
            ORDER BY exp DESC, (CASE level """
    
        for level, value in sorted(rank_mapping.items(), key=lambda x: x[1], reverse=True):
            sql += f"WHEN '{level}' THEN '{value:02}' "
    
        sql += """ELSE level END) ASC LIMIT 1"""
    
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchone()
        if result:
            columns = [column[0] for column in cur.description]
            top1_dict = dict(zip(columns, result))
            return top1_dict
        else:
            return None
        

    def donate_update(self, sect_id, stone_num):
        """宗门捐献更新建设度及可用灵石"""
        sql = f"UPDATE sects SET sect_used_stone=sect_used_stone+?,sect_scale=sect_scale+? WHERE sect_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (stone_num, stone_num * 1, sect_id))
        self.conn.commit()

    def update_sect_used_stone(self, sect_id, sect_used_stone, key):
        """更新宗门灵石储备  1为增加,2为减少"""
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
        """更新资材  1为增加,2为减少"""
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
        """
        获取所有宗门信息
        :return
        :result[0] = sect_id   
        :result[1] = 建设度 sect_scale,
        :result[2] = 丹房等级 elixir_room_level 
        """
        sql = f"SELECT sect_id, sect_scale, elixir_room_level FROM sects WHERE sect_owner is NOT NULL ORDER BY sect_scale DESC"
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchall()
        return result

    def get_all_users_by_sect_id(self, sect_id):
        """
        获取宗门所有成员信息
        :return: 成员列表
        """
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
        """
        更新用户操作CD
        :param sc_time: 任务
        :param user_id: qq
        :param the_type: 0:无状态  1:闭关中  2:历练中  3:探索秘境中  4:虚神界闭关中
        :param the_time: 本次操作的时长
        :return:
        """
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
        sql = f"UPDATE user_xiuxian SET level_up_rate=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (rate, user_id))
        self.conn.commit()

    def update_user_attribute(self, user_id, hp, mp, atk):
        """更新用户HP,MP,ATK信息"""
        hp = number_count(hp)
        mp = number_count(mp)
        atk = number_count(atk)
        sql = f"UPDATE user_xiuxian SET hp=?,mp=?,atk=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (hp, mp, atk, user_id))
        self.conn.commit()

    def update_user_hp_mp(self, user_id, hp, mp):
        """更新用户HP,MP信息"""
        hp = number_count(hp)
        mp = number_count(mp)
        sql = f"UPDATE user_xiuxian SET hp=?,mp=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (hp, mp, user_id))
        self.conn.commit()

    def update_user_sect_contribution(self, user_id, sect_contribution):
        """更新用户宗门贡献度"""
        sql = f"UPDATE user_xiuxian SET sect_contribution=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (sect_contribution, user_id))
        self.conn.commit()

    def deduct_sect_contribution(self, user_id, contribution):
        """扣除用户宗门贡献度"""
        cur = self.conn.cursor()
        sql = "UPDATE user_xiuxian SET sect_contribution=sect_contribution-? WHERE user_id=?"
        cur.execute(sql, (contribution, user_id))
        self.conn.commit()

    def update_user_hp(self, user_id):
        """重置用户hp,mp信息"""
        sql = f"UPDATE user_xiuxian SET hp=exp/2,mp=exp,atk=exp/10 WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()

    def restate(self, user_id=None):
        """重置所有用户状态或重置对应人状态"""
        if user_id is None:
            sql = f"UPDATE user_xiuxian SET hp=exp/2,mp=exp,atk=exp/10"
            cur = self.conn.cursor()
            cur.execute(sql, )
            self.conn.commit()
        else:
            sql = f"UPDATE user_xiuxian SET hp=exp/2,mp=exp,atk=exp/10 WHERE user_id=?"
            cur = self.conn.cursor()
            cur.execute(sql, (user_id,))
            self.conn.commit()
    
    def get_back_msg(self, user_id):
        """获取用户背包信息"""
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
        """检查并调整背包表中的物品数量，确保不超过最大限制，并返回处理结果"""
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
        """
        判断用户物品数量
        :param user_id: 用户qq
        :param goods_id: 物品id
        :param num_type: 物品数量类型，可选值为 None（总数量）、'bind'（绑定数量）、'trade'（可交易数量）
        :return: 物品数量
        """
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
        cur = self.conn.cursor()
        sql = f"SELECT SUM(goods_num) FROM back WHERE goods_id=?"
        cur.execute(sql, (goods_id,))
        result = cur.fetchone()
        if result and result[0] is not None:
            return result[0]
        else:
            return 0

    def unbind_item(self, user_id, goods_id, quantity=1):
        """解绑物品，减少绑定数量
        :param user_id: 用户ID
        :param goods_id: 物品ID
        :param quantity: 解绑数量，默认1
        :return: 成功返回True，失败返回False
        """
        try:
            cur = self.conn.cursor()
            
            # 获取当前物品信息
            sql = "SELECT goods_num, bind_num FROM back WHERE user_id=? AND goods_id=?"
            cur.execute(sql, (user_id, goods_id))
            result = cur.fetchone()
            
            if not result:
                return False
            
            current_goods_num = result[0]
            current_bind_num = result[1]
            
            # 检查绑定数量是否足够
            if current_bind_num < quantity:
                return False
            
            # 更新绑定数量（减少）
            new_bind_num = current_bind_num - quantity
            
            # 确保绑定数量不为负数
            new_bind_num = max(0, new_bind_num)
            
            # 更新数据库
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
        sql = f"SELECT exp FROM user_xiuxian  WHERE level like '{level}%'"
        cur = self.conn.cursor()
        cur.execute(sql, )
        result = cur.fetchall()
        return result

    def update_user_atkpractice(self, user_id, atkpractice):
        """更新用户攻击修炼等级"""
        sql = f"UPDATE user_xiuxian SET atkpractice={atkpractice} WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()

    def update_user_hppractice(self, user_id, hppractice):
        """更新用户元血修炼等级"""
        sql = f"UPDATE user_xiuxian SET hppractice={hppractice} WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()
        
        
    def update_user_mppractice(self, user_id, mppractice):
        """更新用户灵海修炼等级"""
        sql = f"UPDATE user_xiuxian SET mppractice={mppractice} WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()
        

    def update_user_sect_task(self, user_id, sect_task):
        """更新用户宗门任务次数"""
        sql = f"UPDATE user_xiuxian SET sect_task=sect_task+? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (sect_task, user_id))
        self.conn.commit()

    def sect_task_reset(self):
        """重置宗门任务次数"""
        sql = f"UPDATE user_xiuxian SET sect_task=0"
        cur = self.conn.cursor()
        cur.execute(sql, )
        self.conn.commit()

    def update_sect_scale_and_used_stone(self, sect_id, sect_used_stone, sect_scale):
        """更新宗门灵石、建设度"""
        sql = f"UPDATE sects SET sect_used_stone=?,sect_scale=? WHERE sect_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (sect_used_stone, sect_scale, sect_id))
        self.conn.commit()

    def update_sect_elixir_room_level(self, sect_id, level):
        """更新宗门丹房等级"""
        sql = f"UPDATE sects SET elixir_room_level=? WHERE sect_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (level, sect_id))
        self.conn.commit()

    def update_user_sect_elixir_get_num(self, user_id):
        """更新用户每日领取丹药领取次数"""
        sql = f"UPDATE user_xiuxian SET sect_elixir_get=1 WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()

    def sect_elixir_get_num_reset(self):
        """重置宗门丹药领取次数"""
        sql = f"UPDATE user_xiuxian SET sect_elixir_get=0"
        cur = self.conn.cursor()
        cur.execute(sql, )
        self.conn.commit()

    def update_sect_mainbuff(self, sect_id, mainbuffid):
        """更新宗门当前的主修功法"""
        sql = f"UPDATE sects SET mainbuff=? WHERE sect_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (mainbuffid, sect_id))
        self.conn.commit()

    def update_sect_secbuff(self, sect_id, secbuffid):
        """更新宗门当前的神通"""
        sql = f"UPDATE sects SET secbuff=? WHERE sect_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (secbuffid, sect_id))
        self.conn.commit()

    def initialize_user_buff_info(self, user_id):
        """初始化用户buff信息"""
        sql = f"INSERT INTO BuffInfo (user_id,main_buff,sec_buff,effect1_buff,effect2_buff,faqi_buff,fabao_weapon) VALUES (?,0,0,0,0,0,0)"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()

    def get_user_buff_info(self, user_id):
        """获取用户buff信息"""
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
        sql = f"UPDATE BuffInfo SET main_buff = ? WHERE user_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (id, user_id,))
        self.conn.commit()
    
    def updata_user_sub_buff(self, user_id, id): #辅修功法3
        """更新用户辅修功法信息"""
        sql = f"UPDATE BuffInfo SET sub_buff = ? WHERE user_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (id, user_id,))
        self.conn.commit()
    
    def updata_user_sec_buff(self, user_id, id):
        """更新用户副功法信息"""
        sql = f"UPDATE BuffInfo SET sec_buff = ? WHERE user_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (id, user_id,))
        self.conn.commit()

    def updata_user_effect1_buff(self, user_id, id):
        """更新用户身法信息"""
        sql = f"UPDATE BuffInfo SET effect1_buff = ? WHERE user_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (id, user_id,))
        self.conn.commit()

    def updata_user_effect2_buff(self, user_id, id):
        """更新用户瞳术信息"""
        sql = f"UPDATE BuffInfo SET effect2_buff = ? WHERE user_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (id, user_id,))
        self.conn.commit()
        
    def updata_user_faqi_buff(self, user_id, id):
        """更新用户法器信息"""
        sql = f"UPDATE BuffInfo SET faqi_buff = ? WHERE user_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (id, user_id,))
        self.conn.commit()

    def updata_user_fabao_weapon(self, user_id, id):
        """更新用户法宝信息"""
        sql = f"UPDATE BuffInfo SET fabao_weapon = ? WHERE user_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (id, user_id,))
        self.conn.commit()

    def updata_user_armor_buff(self, user_id, id):
        """更新用户防具信息"""
        sql = f"UPDATE BuffInfo SET armor_buff = ? WHERE user_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (id, user_id,))
        self.conn.commit()

    def updata_user_atk_buff(self, user_id, buff):
        """更新用户永久攻击buff信息"""
        sql = f"UPDATE BuffInfo SET atk_buff=atk_buff+? WHERE user_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (buff, user_id,))
        self.conn.commit()

    def updata_user_blessed_spot(self, user_id, blessed_spot):
        """更新用户洞天福地等级"""
        sql = f"UPDATE BuffInfo SET blessed_spot=? WHERE user_id = ?"
        cur = self.conn.cursor()
        cur.execute(sql, (blessed_spot, user_id,))
        self.conn.commit()

    def update_user_blessed_spot_flag(self, user_id):
        """更新用户洞天福地是否开启"""
        sql = f"UPDATE user_xiuxian SET blessed_spot_flag=1 WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        self.conn.commit()

    def update_user_blessed_spot_name(self, user_id, blessed_spot_name):
        """更新用户洞天福地的名字"""
        sql = f"UPDATE user_xiuxian SET blessed_spot_name=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (blessed_spot_name, user_id,))
        self.conn.commit()

    def day_num_reset(self):
        """重置丹药每日使用次数"""
        sql = f"UPDATE back SET day_num=0 WHERE goods_type='丹药'"
        cur = self.conn.cursor()
        cur.execute(sql, )
        self.conn.commit()

    def mixelixir_num_reset(self):
        """重置每日炼丹次数"""
        sql = f"UPDATE user_xiuxian SET mixelixir_num=0"
        cur = self.conn.cursor()
        cur.execute(sql, )
        self.conn.commit()

    def reset_work_num(self, count):
        """重置用户悬赏令刷新次数"""
        sql = f"UPDATE user_xiuxian SET work_num=?"
        cur = self.conn.cursor()
        cur.execute(sql, (count,))
        self.conn.commit()

    def get_work_num(self, user_id):
        """获取用户悬赏令刷新次数"""
        sql = f"SELECT work_num FROM user_xiuxian WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        result = cur.fetchone()
        if result:
            work_num = result[0]
        return work_num
    
    def update_work_num(self, user_id, work_num):
        sql = f"UPDATE user_xiuxian SET work_num=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (work_num, user_id,))
        self.conn.commit()


    def send_back(self, user_id, goods_id, goods_name, goods_type, goods_num, bind_flag=0):
        """
        插入物品至背包
        :param user_id: 用户qq
        :param goods_id: 物品id
        :param goods_name: 物品名称
        :param goods_type: 物品类型
        :param goods_num: 物品数量
        :param bind_flag: 是否绑定物品,0-非绑定,1-绑定
        :return: None
        """
        now_time = datetime.now()
        max_goods_num = int(XiuConfig().max_goods_num)
        goods_num = min(abs(goods_num), max_goods_num)
        # 检查物品是否存在，存在则update
        cur = self.conn.cursor()
        back = self.get_item_by_good_id_and_user_id(user_id, goods_id)

        if back:
            # 判断是否存在，存在则update
            if bind_flag == 1:
                bind_num = back['bind_num'] + goods_num
            else:
                bind_num = min(back['bind_num'], back['goods_num'])
            goods_nums = min(back['goods_num'] + goods_num, max_goods_num)
            bind_num = min(bind_num, max_goods_num)
            sql = f"UPDATE back set goods_num=?,update_time=?,bind_num={bind_num} WHERE user_id=? and goods_id=?"
            cur.execute(sql, (goods_nums, now_time, user_id, goods_id))
            self.conn.commit()
        else:
            # 判断是否存在，不存在则INSERT
            if bind_flag == 1:
                bind_num = goods_num
            else:
                bind_num = 0
            sql = f"""
                    INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num)
            VALUES (?,?,?,?,?,?,?,?)"""
            cur.execute(sql, (user_id, goods_id, goods_name, goods_type, goods_num, now_time, now_time, bind_num))
            self.conn.commit()


    def get_item_by_good_id_and_user_id(self, user_id, goods_id):
        """根据物品id、用户id获取物品信息"""
        sql = f"select * from back WHERE user_id=? and goods_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id, goods_id))
        result = cur.fetchone()
        if not result:
            return None
    
        columns = [column[0] for column in cur.description]
        item_dict = dict(zip(columns, result))
        return item_dict


    def update_back_equipment(self, sql_str):
        """更新背包,传入sql"""
        logger.opt(colors=True).info(f"<green>执行的sql:{sql_str}</green>")
        cur = self.conn.cursor()
        cur.execute(sql_str)
        self.conn.commit()

    def reset_user_drug_resistance(self, user_id):
        """重置用户耐药性"""
        sql = f"UPDATE back SET all_num=0 where goods_type='丹药' and user_id={user_id}"
        cur = self.conn.cursor()
        cur.execute(sql, )
        self.conn.commit()

    def update_back_j(self, user_id, goods_id, num=1, use_key=0):
        """
        使用物品
        :num 减少数量  默认1
        :use_key 是否使用，丹药使用才传 默认0
        """
        num = abs(num)
        back = self.get_item_by_good_id_and_user_id(user_id, goods_id)
        if back['goods_type'] == "丹药" and use_key == 1:  # 丹药要判断耐药性、日使用上限
            if back['bind_num'] >= 1:
                bind_num = back['bind_num'] - num  # 优先使用绑定物品
            else:
                bind_num = back['bind_num']
            day_num = back['day_num'] + num
            all_num = back['all_num'] + num
        else:
            if back['bind_num'] >= 1:
                bind_num = back['bind_num'] - num
            else:
                bind_num = back['bind_num']
            day_num = back['day_num']
            all_num = back['all_num']
        goods_num = back['goods_num'] - num
        if int(goods_num) == 0:
            bind_num = 0
        bind_num = min(bind_num, goods_num)
        bind_num = max(bind_num, 0)
        
        now_time = datetime.now()
        sql_str = f"UPDATE back set update_time='{now_time}',action_time='{now_time}',goods_num={goods_num},day_num={day_num},all_num={all_num},bind_num={bind_num} WHERE user_id={user_id} and goods_id={goods_id}"
        cur = self.conn.cursor()
        cur.execute(sql_str)
        self.conn.commit()


    def _ensure_puppet_column(self):
        """确保 user_xiuxian 表中存在 puppet_status 字段，没有就创建"""
        cur = self.conn.cursor()
        # 检查字段是否存在
        cur.execute("PRAGMA table_info(user_xiuxian)")
        columns = [row[1] for row in cur.fetchall()]
        if "puppet_status" not in columns:
            # 添加新字段 默认 0
            cur.execute("ALTER TABLE user_xiuxian ADD COLUMN puppet_status INTEGER DEFAULT 0")
            self.conn.commit()

    def check_puppet_status(self, user_id):
        """查询灵田傀儡状态，没有字段会自动创建"""
        self._ensure_puppet_column()

        sql = "SELECT puppet_status FROM user_xiuxian WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (user_id,))
        result = cur.fetchone()

        # 若没有该用户记录 返回 0
        if not result:
            return 0

        return result[0]

    def set_puppet_status(self, user_id, status):
        """设置灵田傀儡状态 status: 0关闭 1开启"""
        self._ensure_puppet_column()

        sql = "UPDATE user_xiuxian SET puppet_status=? WHERE user_id=?"
        cur = self.conn.cursor()
        cur.execute(sql, (status, user_id))
        self.conn.commit()

    def get_all_enabled_puppets(self):
        """获取所有开启灵田傀儡的玩家 user_id 列表"""
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
            XiuxianDateManage().update_user_hp_mp(player1['user_id'], player1['气血'], player1['真元'])

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
                logger.opt(colors=True).info(f"<green>trade数据库已创建！</green>")
            self.conn = sqlite3.connect(self.trade_db_path, check_same_thread=False)
            self._check_data()

    def _check_data(self):
        """检查数据完整性"""
        c = self.conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS xianshi_item (
                id TEXT PRIMARY KEY,
                user_id INTEGER,
                goods_id INTEGER,
                name TEXT,
                type TEXT,
                price INTEGER,
                quantity INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS guishi_item (
                id TEXT PRIMARY KEY,
                user_id INTEGER,
                item_id INTEGER,
                item_name TEXT,
                item_type TEXT, -- '求购' 或 '摆摊'
                price INTEGER,
                quantity INTEGER,
                filled_quantity INTEGER DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS guishi_info (
                user_id INTEGER PRIMARY KEY,
                stored_stone INTEGER DEFAULT 0,
                items TEXT DEFAULT '{}'
            )
        """)
        self.conn.commit()

    def total_goods_quantity(self):
        """
        获取全部仙肆物品数量总合，包括求购和摆摊类
        """
        cur = self.conn.cursor()
        sql = """
            SELECT SUM(quantity) AS total_quantity
            FROM (
                SELECT quantity FROM xianshi_item WHERE user_id != 0
                UNION ALL
                SELECT quantity FROM guishi_item WHERE user_id != 0 AND item_type = '摆摊'
            )
        """
        cur.execute(sql)
        result = cur.fetchone()
        if result and result[0] is not None:
            return result[0]
        else:
            return 0

    def generate_unique_id(self):
        """生成唯一的 unique_id"""
        while True:
            timestamp_part = int(time.time() % 10000)
            random_part = random.randint(100, 99999)
            new_id = int(f"{timestamp_part}{random_part}") % 10**10  # 确保不超过10位
        
            # 限制在6-10位
            unique_id = max(100000, min(new_id, 9999999999))
            if not self._unique_id_exists(unique_id):
                return unique_id

    def _unique_id_exists(self, unique_id):
        """检查 unique_id 是否已经存在于数据库中"""
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM xianshi_item WHERE id = ?", (unique_id,))
        if cur.fetchone():
            return True
        cur.execute("SELECT id FROM guishi_item WHERE id = ?", (unique_id,))
        return cur.fetchone() is not None

    def add_xianshi_item(self, user_id, goods_id, name, type, price, quantity):
        """增加仙肆物品"""
        unique_id = self.generate_unique_id()
        
        sql = f"""
            INSERT INTO xianshi_item (id, user_id, goods_id, name, type, price, quantity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.execute(sql, (unique_id, user_id, goods_id, name, type, price, quantity))
        self.conn.commit()

    def add_guishi_order(self, user_id, item_id, item_name, item_type, price, quantity):
        """新增鬼市求购订单/摊位"""
        unique_id = self.generate_unique_id()
        sql = f"""
            INSERT INTO guishi_item (id, user_id, item_id, item_name, item_type, price, quantity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.execute(sql, (unique_id, user_id, item_id, item_name, item_type, price, quantity))
        self.conn.commit()
        return unique_id

    def remove_xianshi_item(self, item_id):
        """删除仙肆物品（先数量-1，如果数量-1之后等于0才删除，不是0则是更新数量）"""
        # 先查询当前物品的数量
        cur = self.conn.cursor()
        cur.execute("SELECT quantity FROM xianshi_item WHERE id = ?", (item_id,))
        result = cur.fetchone()
    
        if not result:
            logger.opt(colors=True).warning(f"<yellow>未找到ID为 {item_id} 的物品</yellow>")
            return False
    
        current_quantity = result[0]

        if str(current_quantity) == "-1":
            logger.opt(colors=True).warning(f"<yellow>系统无限物品不删除</yellow>")
            return
        elif current_quantity == 1:
            # 如果当前数量为1减1后等于0，直接删除
            sql = "DELETE FROM xianshi_item WHERE id = ?"
            self.conn.execute(sql, (item_id,))
            logger.opt(colors=True).info(f"<green>物品 {item_id} 数量减至0，已从数据库中删除</green>")
        else:
            # 如果当前数量大于1，减1后仍有剩余，更新数量
            new_quantity = current_quantity - 1
            sql = "UPDATE xianshi_item SET quantity = ? WHERE id = ?"
            self.conn.execute(sql, (new_quantity, item_id))
            logger.opt(colors=True).info(f"<green>物品 {item_id} 数量从 {current_quantity} 减少至 {new_quantity}</green>")
    
        self.conn.commit()
        return True

    def remove_xianshi_all_item(self, item_id):
        """删除所有用户的仙肆物品"""
        sql = "DELETE FROM xianshi_item WHERE id = ?"
        self.conn.execute(sql, (item_id,))
        self.conn.commit()

    def remove_guishi_order(self, order_id):
        """删除鬼市求购订单/摊位"""
        sql = "DELETE FROM guishi_item WHERE id = ?"
        self.conn.execute(sql, (order_id,))
        self.conn.commit()

    def increase_filled_quantity(self, order_id, amount):
        """增加鬼市求购订单已购买数/摊位已售出数"""
        sql = "UPDATE guishi_item SET filled_quantity = filled_quantity + ? WHERE id = ?"
        self.conn.execute(sql, (amount, order_id))
        self.conn.commit()

    def get_xianshi_items(self, user_id=None, type=None, id=None, name=None):
        """查看仙肆物品"""
        conditions = []
        params = []
        
        if user_id:
            conditions.append("user_id = ?")
            params.append(user_id)
        
        if type:
            conditions.append("type = ?")
            params.append(type)
        
        if id:
            conditions.append("id = ?")
            params.append(id)
        
        if name:
            conditions.append("name = ?")
            params.append(name)
        
        query = "SELECT * FROM xianshi_item"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        cur = self.conn.cursor()
        cur.execute(query, params)
        result = cur.fetchall()
        if not result:
            return None
        
        columns = [column[0] for column in cur.description]
        results = []
        for row in result:
            item_dict = dict(zip(columns, row))
            results.append(item_dict)
        return results

    def get_guishi_orders(self, user_id=None, name=None, type=None, id=None):
        """获取鬼市订单"""
        conditions = []
        params = []
        if user_id:
            conditions.append("user_id = ?")
            params.append(user_id)
        if name:
            conditions.append("item_name = ?")
            params.append(name)
        if type:
            conditions.append("item_type = ?")
            params.append(type)
        if id:
            conditions.append("id = ?")
            params.append(id)

        query = "SELECT * FROM guishi_item"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        cur = self.conn.cursor()
        cur.execute(query, params)
        result = cur.fetchall()
        if not result:
            return None
        
        columns = [column[0] for column in cur.description]
        results = []
        for row in result:
            item_dict = dict(zip(columns, row))
            results.append(item_dict)
        return results

    def get_stored_stone(self, user_id):
        """获取暂存灵石"""
        cur = self.conn.cursor()
        cur.execute("SELECT stored_stone FROM guishi_info WHERE user_id = ?", (user_id,))
        result = cur.fetchone()
        return result[0] if result else 0

    def get_stored_items(self, user_id):
        """获取暂存物品"""
        cur = self.conn.cursor()
        cur.execute("SELECT items FROM guishi_info WHERE user_id = ?", (user_id,))
        result = cur.fetchone()
        if result:
            return json.loads(result[0]) if result[0] else {}
        return {}

    def add_stored_item(self, user_id, item_id, quantity=1):
        """
        增加暂存物品，如果存在则更新数量，不存在则新增
        :param user_id: 用户ID
        :param item_id: 物品ID
        :param quantity: 物品数量，默认为1
        """
        # 将数量转换为整数
        quantity = int(quantity)
        
        # 获取当前的暂存物品信息
        current_items = self.get_stored_items(user_id)
        current_sum = 0
        for current_id, current in current_items.items():
            if str(current_id) == str(item_id):
               current_sum = current
        
        # 更新数量
        current_items[item_id] = current_sum + quantity
        
        # 序列化物品信息
        items_json = json.dumps(current_items)
        
        # 更新或插入记录
        cur = self.conn.cursor()
        cur.execute("SELECT items FROM guishi_info WHERE user_id = ?", (user_id,))
        result = cur.fetchone()
        
        if result:
            # 更新现有记录
            sql = "UPDATE guishi_info SET items = ? WHERE user_id = ?"
            cur.execute(sql, (items_json, user_id))
        else:
            # 插入新记录
            sql = "INSERT INTO guishi_info (user_id, items) VALUES (?, ?)"
            cur.execute(sql, (user_id, items_json))
        
        self.conn.commit()
       
    def remove_stored_item(self, user_id, item_id):
        """删除暂存物品"""
        cur = self.conn.cursor()
        cur.execute("SELECT items FROM guishi_info WHERE user_id = ?", (user_id,))
        result = cur.fetchone()
        if result:
            items = json.loads(result[0]) if result[0] else {}
            if item_id in items:
                del items[item_id]
                sql = "UPDATE guishi_info SET items = ? WHERE user_id = ?"
                cur.execute(sql, (json.dumps(items), user_id))
                self.conn.commit()

    def update_stored_stone(self, user_id, amount, operation):
        """更新暂存灵石"""
        cur = self.conn.cursor()
        if operation == 'add':
            sql = "UPDATE guishi_info SET stored_stone = stored_stone + ? WHERE user_id = ?"
        elif operation == 'subtract':
            sql = "UPDATE guishi_info SET stored_stone = stored_stone - ? WHERE user_id = ?"
        cur.execute(sql, (amount, user_id))
        if cur.rowcount == 0:
            sql_insert = "INSERT INTO guishi_info (user_id, stored_stone) VALUES (?, ?)"
            cur.execute(sql_insert, (user_id, amount))
        self.conn.commit()

    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.opt(colors=True).info(f"<green>trade数据库关闭！</green>")
    
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
            self._ensure_database_exists()

    def _ensure_database_exists(self):
        if not self.database_path.exists():
            logger.opt(colors=True).info(f"<green>player数据库不存在，正在创建...</green>")
            self.database_path.touch()
            logger.opt(colors=True).info(f"<green>player数据库已创建！</green>")

    def _get_connection(self):
        return sqlite3.connect(self.database_path, check_same_thread=False)

    def _ensure_table_exists(self, user_id, table_name):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if cursor.fetchone() is None:
            cursor.execute(f"CREATE TABLE {table_name} (user_id INTEGER PRIMARY KEY)")
            logger.opt(colors=True).info(f"<green>表 {table_name} 已创建！</green>")
        conn.commit()
        conn.close()

    def _ensure_field_exists(self, user_id, table_name, field, data_type='INTEGER'):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        fields = [col[1] for col in cursor.fetchall()]
        if field not in fields:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {field} {data_type} DEFAULT 0")
            logger.opt(colors=True).info(f"<green>字段 {field} 已添加到表 {table_name}，类型为 {data_type}！</green>")
        conn.commit()
        conn.close()

    def update_or_write_data(self, user_id, table_name, field, value, data_type='INTEGER'):
        if data_type not in ['INTEGER', 'REAL', 'TEXT', 'BLOB', 'NUMERIC']:
            logger.warning(f"<yellow>Unsupported data type: {data_type}. Defaulting to INTEGER.</yellow>")
            data_type = 'INTEGER'
        self._ensure_table_exists(user_id, table_name)
        self._ensure_field_exists(user_id, table_name, field, data_type)
        value = str(value)
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"UPDATE {table_name} SET {field}=? WHERE user_id=?", (value, user_id))
        if cursor.rowcount == 0:
            cursor.execute(f"INSERT INTO {table_name} (user_id, {field}) VALUES (?, ?)", (user_id, value))
        conn.commit()
        conn.close()

    def get_fields(self, user_id, table_name):
        """通过user_id查看一个表这个主键的全部字段"""
        if str(user_id) == "None":
            return None
            
        self._ensure_table_exists(user_id, table_name)
        
        # 检查主键是否存在
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM {table_name} WHERE user_id=?", (user_id,))
            result = cursor.fetchone()
        except Exception as e:
            result = None
        if result is None:
            logger.warning(f"用户ID {user_id} 在表 {table_name} 中不存在")
            conn.close()
            return None
        
        columns = [column[0] for column in cursor.description]
        user_dict = dict(zip(columns, result))
        conn.close()
        return user_dict

    def get_field_data(self, user_id, table_name, field):
        self._ensure_table_exists(user_id, table_name)
        self._ensure_field_exists(user_id, table_name, field)
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT {field} FROM {table_name} WHERE user_id=?", (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def get_all_field_data(self, table_name, field):
        self._ensure_table_exists(None, table_name)
        self._ensure_field_exists(None, table_name, field)
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT user_id, {field} FROM {table_name}")
        result = cursor.fetchall()
        conn.close()
        return result

    def update_all_records(self, table_name, field, value, data_type='INTEGER'):
        """
        更新指定表中所有记录的某个字段的值
        :param table_name: 表名
        :param field: 字段名
        :param value: 新值
        :param data_type: 数据类型，默认为 INTEGER
        """
        if data_type not in ['INTEGER', 'REAL', 'TEXT', 'BLOB', 'NUMERIC']:
            logger.warning(f"<yellow>Unsupported data type: {data_type}. Defaulting to INTEGER.</yellow>")
            data_type = 'INTEGER'
        
        value = str(value)
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"UPDATE {table_name} SET {field}=? ", (value,))
        conn.commit()
        conn.close()

    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.opt(colors=True).info(f"<green>player数据库关闭！</green>")
    
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
            if not self.database_path.exists():
                self.database_path.mkdir(parents=True)
                self.database_path /= "xiuxian_impart.db"
                self.conn = sqlite3.connect(self.database_path)
                # self._create_file()
            else:
                self.database_path /= "xiuxian_impart.db"
                self.conn = sqlite3.connect(self.database_path)
            logger.opt(colors=True).info(f"<green>xiuxian_impart数据库已连接!</green>")
            self._check_data()

    def close(self):
        self.conn.close()
        logger.opt(colors=True).info(f"<green>xiuxian_impart数据库关闭!</green>")

    def _create_file(self) -> None:
        """创建数据库文件"""
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
        c = self.conn.cursor()

        for i in config_impart.sql_table:
            if i == "xiuxian_impart":
                try:
                    c.execute(f"select count(1) from {i}")
                except sqlite3.OperationalError:
                    c.execute(f"""CREATE TABLE "xiuxian_impart" (
    "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
    "user_id" integer DEFAULT 0,
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
        if self.create_user(user_id):
            pass
        else:
            c = self.conn.cursor()
            sql = f"INSERT INTO xiuxian_impart (user_id, impart_hp_per, impart_atk_per, impart_mp_per, impart_exp_up ,boss_atk,impart_know_per,impart_burst_per,impart_mix_per,impart_reap_per,impart_two_exp,stone_num,impart_lv,impart_num,exp_day,wish) VALUES(?, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)"
            c.execute(sql, (user_id,))
            self.conn.commit()

    def get_user_impart_info_with_id(self, user_id):
        """根据USER_ID获取用户impart_buff信息"""
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
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_hp_per=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_impart_hp_per(self, impart_num, user_id):
        """add impart_hp_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_hp_per=impart_hp_per+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_impart_atk_per(self, impart_num, user_id):
        """更新impart_atk_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_atk_per=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_impart_atk_per(self, impart_num, user_id):
        """add  impart_atk_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_atk_per=impart_atk_per+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_impart_mp_per(self, impart_num, user_id):
        """impart_mp_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_mp_per=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_impart_mp_per(self, impart_num, user_id):
        """add impart_mp_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_mp_per=impart_mp_per+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_impart_exp_up(self, impart_num, user_id):
        """impart_exp_up"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_exp_up=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_impart_exp_up(self, impart_num, user_id):
        """add impart_exp_up"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_exp_up=impart_exp_up+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_boss_atk(self, impart_num, user_id):
        """boss_atk"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET boss_atk=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_boss_atk(self, impart_num, user_id):
        """add boss_atk"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET boss_atk=boss_atk+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_impart_know_per(self, impart_num, user_id):
        """impart_know_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_know_per=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_impart_know_per(self, impart_num, user_id):
        """add impart_know_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_know_per=impart_know_per+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_impart_burst_per(self, impart_num, user_id):
        """impart_burst_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_burst_per=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_impart_burst_per(self, impart_num, user_id):
        """add impart_burst_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_burst_per=impart_burst_per+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_impart_mix_per(self, impart_num, user_id):
        """impart_mix_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_mix_per=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_impart_mix_per(self, impart_num, user_id):
        """add impart_mix_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_mix_per=impart_mix_per+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_impart_reap_per(self, impart_num, user_id):
        """impart_reap_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_reap_per=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_impart_reap_per(self, impart_num, user_id):
        """add impart_reap_per"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_reap_per=impart_reap_per+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_impart_two_exp(self, impart_num, user_id):
        """更新双修"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_two_exp=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_impart_num(self, impart_num, user_id):
        """更新抽卡次数"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_num=impart_num+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_impart_two_exp(self, impart_num, user_id):
        """add impart_two_exp"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET impart_two_exp=impart_two_exp+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_impart_wish(self, impart_num, user_id):
        """更新抽卡次数"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET wish=? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def add_impart_wish(self, impart_num, user_id):
        """增加抽卡次数"""
        cur = self.conn.cursor()
        sql = f"UPDATE xiuxian_impart SET wish=wish+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def update_stone_num(self, impart_num, user_id, type_):
        """更新结晶数量"""
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
        cur = self.conn.cursor()
        sql = "UPDATE xiuxian_impart SET stone_num=stone_num+?"
        cur.execute(sql, (impart_stone,))
        self.conn.commit()
        
    def update_impart_lv(self, user_id, impart_lv):
        """更新虚神界等级"""
        cur = self.conn.cursor()
        sql = "UPDATE xiuxian_impart SET impart_lv=? WHERE user_id=?"
        cur.execute(sql, (impart_lv, user_id))
        self.conn.commit()

    def impart_lv_reset(self):
        """重置所有用户虚神界等级"""
        sql = f"UPDATE xiuxian_impart SET impart_lv=0"
        cur = self.conn.cursor()
        cur.execute(sql, )
        self.conn.commit()

    def impart_num_reset(self):
        """重置所有用户传承抽卡次数"""
        sql = f"UPDATE xiuxian_impart SET impart_num=0"
        cur = self.conn.cursor()
        cur.execute(sql, )
        self.conn.commit()

    def get_impart_rank(self):
        """获取虚神界等级排行榜"""
        sql = "SELECT user_id, impart_lv FROM xiuxian_impart WHERE impart_lv > 0 ORDER BY impart_lv DESC, stone_num DESC LIMIT 50"
        cur = self.conn.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        columns = [column[0] for column in cur.description]
        return [dict(zip(columns, row)) for row in result]  # 返回字典列表

    def update_all_users_impart_lv(self, num, operation):
        """
        更新所有用户的虚神界等级
        :param num: 要增加/减少的数值
        :param operation: 1-增加, 2-减少
        """
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
        cur = self.conn.cursor()
        # 获取当前思恋结晶数量
        sql = "SELECT stone_num FROM xiuxian_impart WHERE user_id=?"
        cur.execute(sql, (user_id,))
        result = cur.fetchone()
        if result is None:
            return # 用户不存在
        stone_num = result[0]
        if stone_num < 100:
            return # 不足100，无法转换
        # 计算可转换的祈愿石数量
        wishing_stone_num = stone_num // 100
        sql_update = "UPDATE xiuxian_impart SET stone_num=0 WHERE user_id=?"
        cur.execute(sql_update, (user_id,))
        self.conn.commit()
        sql_message.send_back(user_id, 20005, "祈愿石", "特殊道具", wishing_stone_num, 1)

    def add_impart_exp_day(self, impart_num, user_id):
        """add impart_exp_day"""
        cur = self.conn.cursor()
        sql = "UPDATE xiuxian_impart SET exp_day=exp_day+? WHERE user_id=?"
        cur.execute(sql, (impart_num, user_id))
        self.conn.commit()
        return True

    def use_impart_exp_day(self, impart_num, user_id):
        """use impart_exp_day"""
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

def final_user_data(user_data, columns):
    """传入用户当前信息、buff信息,返回最终信息"""
    user_dict = dict(zip((col[0] for col in columns), user_data))
    
    # 通过字段名称获取相应的值
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_dict['user_id'])
    if impart_data is None:
        xiuxian_impart._create_user(user_dict['user_id'])
  
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_dict['user_id'])
    impart_hp_per = impart_data['impart_hp_per'] if impart_data is not None else 0
    impart_mp_per = impart_data['impart_mp_per'] if impart_data is not None else 0
    impart_atk_per = impart_data['impart_atk_per'] if impart_data is not None else 0
    
    user_buff_data = UserBuffDate(user_dict['user_id']).BuffInfo
    
    armor_atk_buff = 0
    if int(user_buff_data['armor_buff']) != 0:
        armor_info = items.get_data_by_item_id(user_buff_data['armor_buff'])
        armor_atk_buff = armor_info['atk_buff']
        
    weapon_atk_buff = 0
    if int(user_buff_data['faqi_buff']) != 0:
        weapon_info = items.get_data_by_item_id(user_buff_data['faqi_buff'])
        weapon_atk_buff = weapon_info['atk_buff']
    
    main_buff_data = UserBuffDate(user_dict['user_id']).get_user_main_buff_data()
    main_hp_buff = main_buff_data['hpbuff'] if main_buff_data is not None else 0
    main_mp_buff = main_buff_data['mpbuff'] if main_buff_data is not None else 0
    main_atk_buff = main_buff_data['atkbuff'] if main_buff_data is not None else 0
    
    hppractice = user_dict['hppractice'] * 0.05 if user_dict['hppractice'] is not None else 0
    mppractice = user_dict['mppractice'] * 0.05 if user_dict['mppractice'] is not None else 0
    
    # 改成字段名称来获取相应的值
    user_dict['hp'] = int(user_dict['hp'] * (1 + main_hp_buff + impart_hp_per + hppractice))
    user_dict['mp'] = int(user_dict['mp'] * (1 + main_mp_buff + impart_mp_per + mppractice))
    user_dict['atk'] = int((user_dict['atk'] * (user_dict['atkpractice'] * 0.04 + 1) * (1 + main_atk_buff) * (
            1 + weapon_atk_buff) * (1 + armor_atk_buff)) * (1 + impart_atk_per)) + int(user_buff_data['atk_buff'])
    
    return user_dict

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
    msg += f"效果：{atk_buff_msg}{crit_buff_msg}{crit_atk_msg}{def_buff_msg}{mp_buff_msg}{zw_buff_msg}"
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
    msg += f"效果：{def_buff_msg}{atk_buff_msg}{crit_buff_msg}"
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
    for field in mix_elixir_infoconfigkey:
        data = player_data_manager.get_field_data(user_id_str, info_name, field)
        if data is not None:
            info[field] = data
    return info

def save_player_info(user_id, data, info_name):
    player_data_manager = PlayerDataManager()
    user_id_str = str(user_id)
    for field, value in data.items():
        player_data_manager.update_or_write_data(user_id_str, info_name, field, value, data_type="TEXT")

def get_player_info(user_id, info_name):
    player_info = None
    try:
        nowtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # str
        MIXELIXIRINFOCONFIG = {
            "收取时间": nowtime,
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
        for key in mix_elixir_infoconfigkey:
            if key not in list(player_info.keys()):
                player_info[key] = MIXELIXIRINFOCONFIG[key]
        save_player_info(user_id, player_info, info_name)
    except Exception as e:
        player_info = MIXELIXIRINFOCONFIG
        save_player_info(user_id, player_info, info_name)
    return player_info

def number_count(num):
    """
    根据数值大小返回原始值或科学计数法
    规则：大于等于1e+21时返回科学计数法，否则直接返回原数值
    """
    try:
        num_val = float(num)
    except (TypeError, ValueError):
        raise ValueError("输入必须是数字或科学计数法字符串")
    
    if abs(num_val) >= 1e21:  # 阈值改为大于等于
        return f"{num_val:.2e}"  # 保留2位小数的科学计数法
    else:
        return num_val  # 返回浮点数

def backup_db_files():
    """
    备份数据库文件并压缩为zip格式
    可以被定时任务调用
    """
    try:
        # 定义源数据库文件路径和目标备份目录
        db_files = [
            DATABASE / "xiuxian.db",
            DATABASE / "xiuxian_impart.db",
            DATABASE / "player.db",
            DATABASE / "trade.db"
        ]
        
        backup_dir = Path() / "data" / "xiuxian" / "backups" / "db_backup"
        
        # 创建备份目录（如果不存在）
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成时间戳用于备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 创建临时目录用于存放本次备份的文件
        temp_dir = backup_dir / f"temp_{timestamp}"
        temp_dir.mkdir(exist_ok=True)
        
        backed_up_files = []
        
        # 复制每个数据库文件到临时目录
        for db_file in db_files:
            if db_file.exists():
                backup_filename = f"{db_file.name}"
                backup_path = temp_dir / backup_filename
                
                # 复制文件
                shutil.copy2(db_file, backup_path)
                backed_up_files.append(backup_filename)
                
                logger.info(f"数据库文件复制成功: {db_file.name} -> {backup_filename}")
            else:
                logger.warning(f"数据库文件不存在，跳过备份: {db_file}")
        
        # 创建压缩包
        zip_filename = f"db_backup_{timestamp}.zip"
        zip_path = backup_dir / zip_filename
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in temp_dir.iterdir():
                zipf.write(file, file.name)
                logger.info(f"添加到压缩包: {file.name}")
        
        # 删除临时目录
        shutil.rmtree(temp_dir)
        
        # 清理旧的备份文件（保留最近30天的备份）
        clean_old_backups(backup_dir)
        
        return True, f"数据库备份完成: {zip_filename}"
        
    except Exception as e:
        error_msg = f"数据库备份失败: {str(e)}"
        logger.error(error_msg)
        # 确保清理临时目录
        try:
            if 'temp_dir' in locals() and temp_dir.exists():
                shutil.rmtree(temp_dir)
        except:
            pass
        return False, error_msg

def clean_old_backups(backup_dir, keep_days=10):
    """
    清理旧的备份文件，保留指定天数内的备份
    """
    try:
        current_time = datetime.now()
        backup_files = list(backup_dir.glob("*.zip"))
        
        for backup_file in backup_files:
            try:
                # 从文件名中提取时间戳
                # 格式: db_backup_YYYYMMDD_HHMMSS.zip
                filename_parts = backup_file.stem.split('_')
                if len(filename_parts) >= 3:
                    time_str = '_'.join(filename_parts[2:4])  # 获取时间戳部分
                    file_time = datetime.strptime(time_str, "%Y%m%d_%H%M%S")
                    
                    age_days = (current_time - file_time).days
                    
                    if age_days > keep_days:
                        backup_file.unlink()
                        logger.info(f"清理旧备份: {backup_file.name} (已保存 {age_days} 天)")
            except ValueError:
                # 如果文件名格式不匹配，跳过这个文件
                continue
                
    except Exception as e:
        logger.warning(f"清理旧备份时出错: {str(e)}")

driver = get_driver()
sql_message = XiuxianDateManage()  # sql类
items = Items()
trade_manager = TradeDataManager()
player_data_manager = PlayerDataManager()

@driver.on_shutdown
async def close_db():
    XiuxianDateManage().close()
    XIUXIAN_IMPART_BUFF().close()
    PlayerDataManager().close()
    TradeDataManager().close()
