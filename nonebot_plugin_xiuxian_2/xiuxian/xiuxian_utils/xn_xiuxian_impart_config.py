from ...paths import get_paths

DATABASE = get_paths().data


class IMPART_BUFF_CONFIG:
    def __init__(self):
        self.sql_table = ["xiuxian_impart"]
        # 数据库字段
        self.sql_table_impart_buff = ["id", "user_id", "impart_hp_per",
                                      "impart_atk_per", "impart_mp_per",
                                      "impart_exp_up", "boss_atk",
                                      "impart_know_per", "impart_burst_per",
                                      "impart_mix_per", "impart_reap_per",
                                      "impart_two_exp", "stone_num", "impart_lv", "impart_num",
                                      "exp_day", "wish"]

config_impart = IMPART_BUFF_CONFIG()
