from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager

class STONE_LIMIT(object):
    def __init__(self):
        self.player_data_manager = PlayerDataManager()
        self.table_name = "stone_limit"

    def __ensure_user_record(self, user_id):
        """确保用户记录存在"""
        user_id = str(user_id)
        # 尝试获取记录，如果不存在则初始化
        record = self.player_data_manager.get_fields(user_id, self.table_name)
        if not record:
            # 初始化默认值
            self.player_data_manager.update_or_write_data(
                user_id, self.table_name, "send_limit", 0
            )
            self.player_data_manager.update_or_write_data(
                user_id, self.table_name, "receive_limit", 0
            )

    def get_send_limit(self, user_id):
        """获取用户今日已送灵石额度"""
        user_id = str(user_id)
        self.__ensure_user_record(user_id)
        value = self.player_data_manager.get_field_data(user_id, self.table_name, "send_limit")
        return int(value) if value is not None else 0

    def get_receive_limit(self, user_id):
        """获取用户今日已收灵石额度"""
        user_id = str(user_id)
        self.__ensure_user_record(user_id)
        value = self.player_data_manager.get_field_data(user_id, self.table_name, "receive_limit")
        return int(value) if value is not None else 0

    def update_send_limit(self, user_id, amount):
        """更新用户今日已送灵石额度"""
        user_id = str(user_id)
        self.__ensure_user_record(user_id)
        current = self.get_send_limit(user_id)
        new_value = current + amount
        self.player_data_manager.update_or_write_data(
            user_id, self.table_name, "send_limit", new_value
        )

    def update_receive_limit(self, user_id, amount):
        """更新用户今日已收灵石额度"""
        user_id = str(user_id)
        self.__ensure_user_record(user_id)
        current = self.get_receive_limit(user_id)
        new_value = current + amount
        self.player_data_manager.update_or_write_data(
            user_id, self.table_name, "receive_limit", new_value
        )

    def reset_limits(self):
        """重置所有用户额度"""
        self.player_data_manager.update_all_records(self.table_name, "send_limit", 0)
        self.player_data_manager.update_all_records(self.table_name, "receive_limit", 0)

stone_limit = STONE_LIMIT()
