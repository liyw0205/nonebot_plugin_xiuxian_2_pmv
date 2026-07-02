from .item_info_messages import readf


def _handle_module():
    from . import xiuxian2_handle

    return xiuxian2_handle


class BuffJsonDate:

    def __init__(self):
        """json文件路径"""
        handle = _handle_module()
        self.mainbuff_jsonpath = handle.SKILLPATHH / "主功法.json"
        self.secbuff_jsonpath = handle.SKILLPATHH / "神通.json"
        self.effect1buff_jsonpath = handle.SKILLPATHH / "身法.json"
        self.effect2buff_jsonpath = handle.SKILLPATHH / "瞳术.json"
        self.gfpeizhi_jsonpath = handle.SKILLPATHH / "功法概率设置.json"
        self.weapon_jsonpath = handle.WEAPONPATH / "法器.json"
        self.armor_jsonpath = handle.WEAPONPATH / "防具.json"

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
        return _handle_module().get_user_buff(self.user_id)

    def get_user_main_buff_data(self):
        """获取用户主功法数据"""
        main_buff_data = None
        buff_info = self.BuffInfo
        main_buff_id = buff_info.get('main_buff', 0)
        if main_buff_id != 0:
            main_buff_data = _handle_module().items.get_data_by_item_id(main_buff_id)
        return main_buff_data

    def get_user_sub_buff_data(self):
        """获取用户辅修功法数据"""
        sub_buff_data = None
        buff_info = self.BuffInfo
        sub_buff_id = buff_info.get('sub_buff', 0)
        if sub_buff_id != 0:
            sub_buff_data = _handle_module().items.get_data_by_item_id(sub_buff_id)
        return sub_buff_data

    def get_user_sec_buff_data(self):
        """获取用户神通数据"""
        sec_buff_data = None
        buff_info = self.BuffInfo
        sec_buff_id = buff_info.get('sec_buff', 0)
        if sec_buff_id != 0:
            sec_buff_data = _handle_module().items.get_data_by_item_id(sec_buff_id)
        return sec_buff_data

    def get_user_effect1_buff_data(self):
        """获取用户身法数据"""
        effect1_buff_data = None
        buff_info = self.BuffInfo
        effect1_buff_id = buff_info.get('effect1_buff', 0)
        if effect1_buff_id != 0:
            effect1_buff_data = _handle_module().items.get_data_by_item_id(effect1_buff_id)
        return effect1_buff_data

    def get_user_effect2_buff_data(self):
        """获取用户瞳术数据"""
        effect2_buff_data = None
        buff_info = self.BuffInfo
        effect2_buff_id = buff_info.get('effect2_buff', 0)
        if effect2_buff_id != 0:
            effect2_buff_data = _handle_module().items.get_data_by_item_id(effect2_buff_id)
        return effect2_buff_data

    def get_user_weapon_data(self):
        """获取用户法器数据"""
        weapon_data = None
        buff_info = self.BuffInfo
        weapon_id = buff_info.get('faqi_buff', 0)
        if weapon_id != 0:
            weapon_data = _handle_module().items.get_data_by_item_id(weapon_id)
        return weapon_data

    def get_user_armor_buff_data(self):
        """获取用户防具数据"""
        armor_buff_data = None
        buff_info = self.BuffInfo
        armor_buff_id = buff_info.get('armor_buff', 0)
        if armor_buff_id != 0:
            armor_buff_data = _handle_module().items.get_data_by_item_id(armor_buff_id)
        return armor_buff_data
