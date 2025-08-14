from ..xiuxian_utils.xiuxian2_handle import *
from .workmake import workmake
from .reward_data_source import savef, readf
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.utils import number_to
from datetime import datetime
import json
import os
from pathlib import Path
import random

sql_message = XiuxianDateManage()  # sql类
items = Items()

class workhandle(XiuxianJsonDate):
    def do_work(self, key, work_list=None, name=None, level="江湖好手", exp=None, user_id=None):
        """
        悬赏令核心处理逻辑(纯JSON版本)
        
        参数:
            key: 操作类型
                0 - 生成新悬赏令
                1 - 获取任务剩余时间
                2 - 结算任务奖励
            work_list: 任务列表/任务名称
            name: 任务名称
            level: 用户境界
            exp: 用户修为
            user_id: 用户ID
        """
        if key == 0:  # 生成新悬赏令
            # 获取用户信息
            user_info = sql_message.get_user_info_with_id(user_id)
            if not user_info:
                return []
                
            # 生成悬赏令数据
            data = workmake(level, exp, user_info['level'])
            get_work_list = []
            
            # 构建悬赏令数据结构
            work_data = {
                "tasks": {},
                "status": 1,
                "refresh_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "user_level": level
            }
            
            # 格式化任务数据
            for k, v in data.items():
                get_work_list.append([
                    k,          # 任务名称
                    v[0],       # 成功率
                    v[1],       # 基础奖励
                    v[2],       # 预计耗时
                    v[3],       # 额外奖励物品ID
                    v[4],       # 成功消息
                    v[5]        # 失败消息
                ])
                
                # 保存完整任务数据
                work_data["tasks"][k] = {
                    "rate": v[0],
                    "award": v[1],
                    "time": v[2],
                    "item_id": v[3],
                    "success_msg": v[4],
                    "fail_msg": v[5]
                }
            
            # 保存到JSON文件
            savef(user_id, work_data)
            
            return get_work_list

        elif key == 1:  # 获取任务剩余时间
            # 读取任务数据
            data = readf(user_id)
            if not data or data.get("status") == 0:  # 无任务或已过期
                return 0
                
            task_name = name
            if task_name in data["tasks"]:
                return data["tasks"][task_name]["time"]
            return 0

        elif key == 2:  # 结算任务奖励
            # 读取任务数据
            data = readf(user_id)
            if not data or data.get("status") != 2:  # 无任务或未接取
                return "无效的任务", 0, False, 0, False
                
            task_name = work_list
            if task_name not in data["tasks"]:
                return "无效的任务", 0, False, 0, False
                
            # 获取任务详情
            task_data = data["tasks"][task_name]
            
            # 检查是否大成功(成功率>=100%)
            bigsuc = task_data["rate"] >= 100
            
            # 计算任务结果
            if random.randint(1, 100) <= task_data["rate"]:
                return (
                    task_data["success_msg"],  # 成功消息
                    task_data["award"],        # 基础奖励
                    True,                      # 是否成功
                    task_data["item_id"],      # 额外奖励物品ID
                    bigsuc                     # 是否大成功
                )
            else:
                return (
                    task_data["fail_msg"],     # 失败消息
                    task_data["award"],        # 基础奖励
                    False,                     # 是否成功
                    0,                         # 无额外奖励
                    bigsuc                     # 是否大成功
                )

        else:
            return [], 0, False, 0, False
