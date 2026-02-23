from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager

class STONE_LIMIT(object):
    def __init__(self):
        self.player_data_manager = PlayerDataManager()
        self.table_name = "stone_limit"
        self.xiangyuan_table = "xiangyuan_limit"

    def __ensure_user_record(self, user_id):
        """确保用户记录存在"""
        user_id = str(user_id)
        record = self.player_data_manager.get_fields(user_id, self.table_name)
        if not record:
            self.player_data_manager.update_or_write_data(
                user_id, self.table_name, "send_limit", 0
            )
            self.player_data_manager.update_or_write_data(
                user_id, self.table_name, "receive_limit", 0
            )

    def __ensure_xiangyuan_record(self, user_id):
        """确保用户仙缘记录存在"""
        user_id = str(user_id)
        record = self.player_data_manager.get_fields(user_id, self.xiangyuan_table)
        if not record:
            self.player_data_manager.update_or_write_data(
                user_id, self.xiangyuan_table, "send_count", 0
            )
            self.player_data_manager.update_or_write_data(
                user_id, self.xiangyuan_table, "receive_count", 0
            )
            self.player_data_manager.update_or_write_data(
                user_id, self.xiangyuan_table, "last_reset_date", ""
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

    def get_xiangyuan_send_count(self, user_id):
        """获取用户今日已送仙缘次数"""
        user_id = str(user_id)
        self.__ensure_xiangyuan_record(user_id)
        value = self.player_data_manager.get_field_data(user_id, self.xiangyuan_table, "send_count")
        return int(value) if value is not None else 0

    def get_xiangyuan_receive_count(self, user_id):
        """获取用户今日已抢仙缘次数"""
        user_id = str(user_id)
        self.__ensure_xiangyuan_record(user_id)
        value = self.player_data_manager.get_field_data(user_id, self.xiangyuan_table, "receive_count")
        return int(value) if value is not None else 0

    def update_xiangyuan_send_count(self, user_id, count=1):
        """更新用户今日已送仙缘次数"""
        user_id = str(user_id)
        self.__ensure_xiangyuan_record(user_id)
        current = self.get_xiangyuan_send_count(user_id)
        new_value = current + count
        self.player_data_manager.update_or_write_data(
            user_id, self.xiangyuan_table, "send_count", new_value
        )

    def update_xiangyuan_receive_count(self, user_id, count=1):
        """更新用户今日已抢仙缘次数"""
        user_id = str(user_id)
        self.__ensure_xiangyuan_record(user_id)
        current = self.get_xiangyuan_receive_count(user_id)
        new_value = current + count
        self.player_data_manager.update_or_write_data(
            user_id, self.xiangyuan_table, "receive_count", new_value
        )

    def reset_limits(self):
        """重置所有用户灵石额度"""
        self.player_data_manager.update_all_records(self.table_name, "send_limit", 0)
        self.player_data_manager.update_all_records(self.table_name, "receive_limit", 0)

    def reset_xiangyuan_limits(self):
        """重置所有用户仙缘次数"""
        self.player_data_manager.update_all_records(self.xiangyuan_table, "send_count", 0)
        self.player_data_manager.update_all_records(self.xiangyuan_table, "receive_count", 0)

stone_limit = STONE_LIMIT()