import sqlite3
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
import os
from nonebot.log import logger
from .impart_all import impart_all

# 数据库路径
DATABASE_IMPART = Path() / "data" / "xiuxian" / "xiuxian_impart.db"

class IMPART_DATA(object):
    def __init__(self):
        self.dir_path_person = Path() / "data" / "xiuxian" / "impart"
        if not os.path.exists(self.dir_path_person):
            logger.opt(colors=True).info(f"<green>创建目录{self.dir_path_person}</green>")
            os.makedirs(self.dir_path_person)
        
        self.data_path_person = self.dir_path_person / "impart_person.json"
        self.data_all = impart_all  # 从外部导入的卡牌数据
        
        # 初始化数据库连接
        self.conn = sqlite3.connect(DATABASE_IMPART, check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # 创建传承卡牌数据表
        self._create_impart_card_table()
        
        # 迁移现有数据
        self._migrate_existing_data()

    def _create_impart_card_table(self):
        """创建传承卡牌数据表"""
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS impart_cards (
                    user_id TEXT NOT NULL,
                    card_name TEXT NOT NULL,
                    quantity INTEGER DEFAULT 1,
                    PRIMARY KEY (user_id, card_name)
                )
            ''')
            self.conn.commit()
            logger.opt(colors=True).info("<green>传承卡牌数据表创建成功</green>")
        except sqlite3.Error as e:
            logger.error(f"创建传承卡牌数据表失败: {e}")

    def _migrate_existing_data(self):
        """迁移现有的JSON数据到数据库"""
        try:
            # 检查JSON文件是否存在
            if os.path.exists(self.data_path_person):
                with open(self.data_path_person, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # 迁移数据
                migrated_count = 0
                for user_id, cards in json_data.items():
                    if isinstance(cards, list):
                        # 旧格式：列表
                        card_count = {}
                        for card_name in cards:
                            card_count[card_name] = card_count.get(card_name, 0) + 1
                        
                        for card_name, count in card_count.items():
                            self._insert_card(user_id, card_name, count)
                            migrated_count += 1
                    elif isinstance(cards, dict):
                        # 新格式：字典
                        for card_name, count in cards.items():
                            self._insert_card(user_id, card_name, count)
                            migrated_count += 1
                
                if migrated_count > 0:
                    logger.opt(colors=True).info(f"<green>成功迁移{migrated_count}条卡牌数据到数据库</green>")
                    
                    # 备份原JSON文件
                    backup_path = self.data_path_person.with_suffix('.json.backup')
                    os.rename(self.data_path_person, backup_path)
                    logger.opt(colors=True).info(f"<green>原JSON文件已备份到{backup_path}</green>")
                
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"迁移现有数据时出错: {e}")

    def _insert_card(self, user_id: str, card_name: str, quantity: int):
        """插入卡牌数据"""
        try:
            self.cursor.execute(
                "INSERT OR REPLACE INTO impart_cards (user_id, card_name, quantity) VALUES (?, ?, ?)",
                (user_id, card_name, quantity)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"插入卡牌数据失败: {e}")

    def find_user_impart(self, user_id: str) -> bool:
        """检查用户是否存在"""
        user_id = str(user_id)
        try:
            self.cursor.execute(
                "SELECT COUNT(*) FROM impart_cards WHERE user_id = ?",
                (user_id,)
            )
            result = self.cursor.fetchone()
            return result[0] > 0
        except sqlite3.Error as e:
            logger.error(f"检查用户存在失败: {e}")
            return False

    def data_person_add(self, user_id: str, name: str) -> Tuple[bool, int]:
        """
        添加单张卡片
        :param user_id: 用户ID
        :param name: 卡片名称
        :return: (是否新卡, 当前该卡片数量)
        """
        user_id = str(user_id)
        
        try:
            # 检查卡片是否已存在
            self.cursor.execute(
                "SELECT quantity FROM impart_cards WHERE user_id = ? AND card_name = ?",
                (user_id, name)
            )
            result = self.cursor.fetchone()
            
            if result:
                # 卡片已存在，增加数量
                current_count = result[0] + 1
                self.cursor.execute(
                    "UPDATE impart_cards SET quantity = ? WHERE user_id = ? AND card_name = ?",
                    (current_count, user_id, name)
                )
                is_new = False
            else:
                # 新卡片
                current_count = 1
                self.cursor.execute(
                    "INSERT INTO impart_cards (user_id, card_name, quantity) VALUES (?, ?, ?)",
                    (user_id, name, current_count)
                )
                is_new = True
            
            self.conn.commit()
            return is_new, current_count
            
        except sqlite3.Error as e:
            logger.error(f"添加卡片失败: {e}")
            return False, 0

    def data_person_add_batch(self, user_id: str, card_names: List[str]) -> Tuple[List[str], Dict[str, int]]:
        """
        批量添加卡片
        :param user_id: 用户ID
        :param card_names: 卡片名称列表
        :return: (新卡列表, 各卡片的当前数量字典)
        """
        user_id = str(user_id)
        new_cards = []
        card_counts = {}
        
        try:
            # 获取当前卡片数量
            self.cursor.execute(
                "SELECT card_name, quantity FROM impart_cards WHERE user_id = ?",
                (user_id,)
            )
            existing_cards = {row[0]: row[1] for row in self.cursor.fetchall()}
            
            # 处理每张卡片
            for card_name in card_names:
                if card_name in existing_cards:
                    # 已有卡片，增加数量
                    new_count = existing_cards[card_name] + 1
                    self.cursor.execute(
                        "UPDATE impart_cards SET quantity = ? WHERE user_id = ? AND card_name = ?",
                        (new_count, user_id, card_name)
                    )
                    card_counts[card_name] = new_count
                else:
                    # 新卡片
                    self.cursor.execute(
                        "INSERT INTO impart_cards (user_id, card_name, quantity) VALUES (?, ?, 1)",
                        (user_id, card_name)
                    )
                    new_cards.append(card_name)
                    card_counts[card_name] = 1
                    existing_cards[card_name] = 1
            
            self.conn.commit()
            return new_cards, card_counts
            
        except sqlite3.Error as e:
            logger.error(f"批量添加卡片失败: {e}")
            return [], {}

    def data_person_list(self, user_id: str) -> Optional[Dict[str, int]]:
        """
        获取用户所有卡片
        :param user_id: 用户QQ号
        :return: 字典 {卡名: 数量} 或 None
        """
        user_id = str(user_id)
        try:
            self.cursor.execute(
                "SELECT card_name, quantity FROM impart_cards WHERE user_id = ?",
                (user_id,)
            )
            result = self.cursor.fetchall()
            
            if result:
                return {row[0]: row[1] for row in result}
            else:
                return {}
                
        except sqlite3.Error as e:
            logger.error(f"获取用户卡片列表失败: {e}")
            return None

    def data_all_keys(self) -> List[str]:
        """获取所有卡片名称列表"""
        return list(self.data_all.keys())

    def data_all_(self) -> Dict:
        """获取所有卡片数据"""
        return self.data_all

    def __del__(self):
        """析构函数，关闭数据库连接"""
        try:
            if hasattr(self, 'conn'):
                self.conn.close()
        except:
            pass

# 创建全局实例
impart_data_json = IMPART_DATA()
