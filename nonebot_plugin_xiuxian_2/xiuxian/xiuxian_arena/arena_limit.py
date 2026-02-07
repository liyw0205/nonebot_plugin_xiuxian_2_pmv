import json
from datetime import datetime
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager

player_data_manager = PlayerDataManager()

class ArenaLimit:
    def __init__(self):
        self.table_name = "arena"
        # 竞技场配置
        self.initial_score = 1000  # 初始积分
        self.win_points = 20       # 胜利获得积分
        self.lose_points = 10       # 失败扣除积分（对手）
        self.no_match_points = 10   # 无匹配获得积分
        self.daily_challenges = 10  # 每日挑战次数
        
        # 段位荣誉值奖励配置
        self.rank_honor_rewards = {
            "青铜": 100,
            "白银": 200,
            "黄金": 300,
            "铂金": 400,
            "钻石": 600,
            "王者": 1000
        }
        
        # 排名额外荣誉值奖励（前100名）
        self.ranking_honor_bonus = {
            "1": 500,    # 第1名
            "2-3": 300,  # 第2-3名
            "4-10": 200, # 第4-10名
            "11-50": 100, # 第11-50名
            "51-100": 50  # 第51-100名
        }

    def get_user_arena_info(self, user_id):
        """获取用户竞技场信息，如果不存在则自动创建"""
        user_id = str(user_id)
        user_info = player_data_manager.get_fields(user_id, self.table_name)
        
        if not user_info:
            # 初始化用户竞技场数据
            default_data = {
                "score": self.initial_score,           # 当前积分
                "total_wins": 0,                       # 总胜利次数
                "total_losses": 0,                     # 总失败次数
                "daily_challenges_used": 0,            # 今日已用挑战次数
                "last_reset_date": datetime.now().strftime("%Y-%m-%d"),  # 最后重置日期
                "win_streak": 0,                       # 连胜次数
                "max_win_streak": 0,                   # 最大连胜
                "rank": "青铜",                        # 当前段位
                "honor_points": 0,                    # 荣誉值
                "total_honor_earned": 0               # 累计获得荣誉值
            }
            # 保存初始数据
            for key, value in default_data.items():
                player_data_manager.update_or_write_data(user_id, self.table_name, key, value)
            return default_data
        
        return user_info

    def calculate_daily_honor(self, user_id):
        """计算用户每日应获得的荣誉值"""
        arena_info = self.get_user_arena_info(user_id)
        
        # 基础荣誉值（根据段位）
        base_honor = self.rank_honor_rewards.get(arena_info["rank"], 100)
        
        # 排名额外荣誉值
        ranking_bonus = 0
        user_ranking = self.get_user_ranking(user_id)
        
        if user_ranking <= 100 and user_ranking > 0:
            if user_ranking == 1:
                ranking_bonus = self.ranking_honor_bonus["1"]
            elif 2 <= user_ranking <= 3:
                ranking_bonus = self.ranking_honor_bonus["2-3"]
            elif 4 <= user_ranking <= 10:
                ranking_bonus = self.ranking_honor_bonus["4-10"]
            elif 11 <= user_ranking <= 50:
                ranking_bonus = self.ranking_honor_bonus["11-50"]
            elif 51 <= user_ranking <= 100:
                ranking_bonus = self.ranking_honor_bonus["51-100"]
        
        total_honor = base_honor + ranking_bonus
        return total_honor, base_honor, ranking_bonus

    def get_user_ranking(self, user_id):
        """获取用户当前排名"""
        all_users = self.get_arena_ranking(limit=1000)  # 获取所有用户排名
        for i, (uid, score) in enumerate(all_users, 1):
            if str(uid) == str(user_id):
                return i
        return 0  # 未找到用户

    def add_honor_points(self, user_id, amount):
        """添加荣誉值"""
        arena_info = self.get_user_arena_info(user_id)
        new_honor = arena_info["honor_points"] + amount
        new_total = arena_info["total_honor_earned"] + amount
        
        self.update_arena_data(user_id, {
            "honor_points": new_honor,
            "total_honor_earned": new_total
        })
        return new_honor

    def update_arena_data(self, user_id, data):
        """更新用户竞技场数据"""
        user_id = str(user_id)
        for key, value in data.items():
            player_data_manager.update_or_write_data(user_id, self.table_name, key, value)

    def can_challenge_today(self, user_id):
        """检查今日是否还有挑战次数"""
        arena_info = self.get_user_arena_info(user_id)
        return arena_info["daily_challenges_used"] < self.daily_challenges

    def use_challenge(self, user_id):
        """使用一次挑战次数"""
        arena_info = self.get_user_arena_info(user_id)
        arena_info["daily_challenges_used"] += 1
        self.update_arena_data(user_id, {"daily_challenges_used": arena_info["daily_challenges_used"]})

    def update_after_battle(self, user_id, is_win, is_opponent=False, opponent_id=None):
        """更新战斗结果
        Args:
            user_id: 用户ID
            is_win: 是否胜利
            is_opponent: 是否为对手（用于区分扣分逻辑）
            opponent_id: 对手ID（用于无匹配情况）
        """
        arena_info = self.get_user_arena_info(user_id)
    
        if is_win:
            # 胜利处理
            new_score = arena_info["score"] + self.win_points
            arena_info["total_wins"] += 1
            arena_info["win_streak"] += 1
            arena_info["max_win_streak"] = max(arena_info["max_win_streak"], arena_info["win_streak"])
        else:
            # 失败处理
            if is_opponent:
                # 对手失败时扣分
                new_score = max(0, arena_info["score"] - self.lose_points)  # 防止负分
            else:
                # 挑战者失败时积分不变
                new_score = arena_info["score"]
            
            arena_info["total_losses"] += 1
            arena_info["win_streak"] = 0
            
            # 无匹配情况（只有挑战者会触发）
            if opponent_id is None and not is_opponent:
                new_score += self.no_match_points
        
        # 更新段位
        new_rank = self.calculate_rank(new_score)
    
        update_data = {
            "score": new_score,
            "total_wins": arena_info["total_wins"],
            "total_losses": arena_info["total_losses"],
            "win_streak": arena_info["win_streak"],
            "max_win_streak": arena_info["max_win_streak"],
            "rank": new_rank
        }
    
        self.update_arena_data(user_id, update_data)
        return new_score, new_rank

    def calculate_rank(self, score):
        """根据积分计算段位"""
        if score >= 2500:
            return "王者"
        elif score >= 2000:
            return "钻石"
        elif score >= 1500:
            return "铂金"
        elif score >= 1200:
            return "黄金"
        elif score >= 1000:
            return "白银"
        else:
            return "青铜"

    def reset_daily_challenges(self):
        """重置所有用户每日挑战次数（定时任务调用）"""
        # 获取所有有竞技场数据的用户
        all_users = player_data_manager.get_all_field_data(self.table_name, "score")
        for user_id, _ in all_users:
            player_data_manager.update_or_write_data(str(user_id), self.table_name, "daily_challenges_used", 0)
            player_data_manager.update_or_write_data(str(user_id), self.table_name, "last_reset_date", datetime.now().strftime("%Y-%m-%d"))

    def get_arena_ranking(self, limit=50):
        """获取竞技场排行榜"""
        all_users = player_data_manager.get_all_field_data(self.table_name, "score")
        # 按积分排序
        sorted_users = sorted(all_users, key=lambda x: x[1], reverse=True)
        return sorted_users[:limit]

arena_limit = ArenaLimit()
