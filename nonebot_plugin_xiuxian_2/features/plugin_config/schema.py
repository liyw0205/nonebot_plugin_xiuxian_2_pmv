from ...xiuxian.xiuxian_config import convert_rank

LEVELS = convert_rank("江湖好手")[1]

CONFIG_EDITABLE_FIELDS = {
    "bot_uin": {
        "name": "QQ官方机器人UIN",
        "description": "QQ官方机器人UIN",
        "type": "int",
        "category": "基础设置"
    },
    "bot_uid": {
        "name": "QQ官方机器人UID",
        "description": "QQ官方机器人UID",
        "type": "str",
        "category": "基础设置"
    },
    "put_bot": {
        "name": "接收消息QQ",
        "description": "负责接收消息的QQ号列表，设置这个屏蔽群聊/私聊才能生效",
        "type": "list[str]",
        "category": "基础设置"
    },
    "main_bo": {
        "name": "主QQ",
        "description": "负责发送消息的QQ号列表",
        "type": "list[str]",
        "category": "基础设置"
    },
    "shield_group": {
        "name": "屏蔽群聊",
        "description": "屏蔽的群聊ID列表",
        "type": "list[str]",
        "category": "基础设置"
    },
    "response_group": {
        "name": "反转屏蔽",
        "description": "是否反转屏蔽的群聊（仅响应这些群的消息）",
        "type": "bool",
        "category": "基础设置"
    },
    "shield_private": {
        "name": "屏蔽私聊",
        "description": "是否屏蔽私聊消息",
        "type": "bool",
        "category": "基础设置"
    },
    "admin_debug": {
        "name": "管理员调试模式",
        "description": "开启后只响应超管指令",
        "type": "bool",
        "category": "调试设置"
    },
    "at_response": {
        "name": "艾特响应命令",
        "description": "是否只接收艾特命令（官机请勿打开）",
        "type": "bool",
        "category": "消息设置"
    },
    "at_sender": {
        "name": "消息是否艾特",
        "description": "发送消息是否艾特",
        "type": "bool",
        "category": "消息设置"
    },
    "reference_reply": {
        "name": "消息是否引用回复",
        "description": "开启后 QQ 官方普通群/C2C 的通用发送接口优先使用引用回复",
        "type": "bool",
        "category": "消息设置"
    },
    "empty_fallback": {
        "name": "空指令是否回复",
        "description": "空指令回复",
        "type": "bool",
        "category": "消息设置"
    },
    "empty_msg": {
        "name": "空指令回复",
        "description": "回复内容",
        "type": "str",
        "category": "消息设置"
    },
    "empty_fallback_image": {
        "name": "空指令随机图片",
        "description": "开启后默认回复会请求外部随机图片，高并发环境建议关闭",
        "type": "bool",
        "category": "消息设置"
    },
    "group_welcome": {
        "name": "进群欢迎（全局）",
        "description": "全局默认开启；本群可用【关闭进群欢迎】单独关闭",
        "type": "bool",
        "category": "消息设置"
    },
    "group_welcome_msg": {
        "name": "成员进群欢迎文案",
        "description": "有新人入群时发送（需 QQ intent.group_members=true）",
        "type": "str",
        "category": "消息设置"
    },
    "group_bot_join_msg": {
        "name": "Bot入群提示文案",
        "description": "机器人被拉进群时发送，与成员欢迎分开",
        "type": "str",
        "category": "消息设置"
    },
    "xiuxian_user_command_rate_window": {
        "name": "单用户限流窗口",
        "description": "单用户命令限流统计窗口（秒）",
        "type": "int",
        "category": "限流设置"
    },
    "xiuxian_user_command_rate_limit": {
        "name": "单用户限流条数",
        "description": "单用户在限流窗口内允许的命令条数",
        "type": "int",
        "category": "限流设置"
    },
    "xiuxian_user_command_rate_log_interval": {
        "name": "单用户限流日志间隔",
        "description": "同一用户触发限流后的日志输出间隔（秒）",
        "type": "int",
        "category": "限流设置"
    },
    "xiuxian_user_command_rate_cache_clean_interval": {
        "name": "单用户限流缓存清理间隔",
        "description": "单用户限流缓存清理间隔（秒）",
        "type": "int",
        "category": "限流设置"
    },
    "xiuxian_global_command_rate_window": {
        "name": "全局限流窗口",
        "description": "全局命令入口限流统计窗口（秒）",
        "type": "int",
        "category": "限流设置"
    },
    "xiuxian_global_command_rate_limit": {
        "name": "全局限流条数",
        "description": "全局命令入口在限流窗口内允许的命令条数",
        "type": "int",
        "category": "限流设置"
    },
    "xiuxian_global_command_rate_log_interval": {
        "name": "全局限流日志间隔",
        "description": "全局命令入口触发限流后的日志输出间隔（秒）",
        "type": "int",
        "category": "限流设置"
    },
    "xiuxian_global_command_overload_notice": {
        "name": "全局过载提示",
        "description": "全局命令入口过载提示，留空则不提示",
        "type": "str",
        "category": "限流设置"
    },
    "xiuxian_global_command_overload_notice_interval": {
        "name": "过载提示间隔",
        "description": "同一群/用户过载提示间隔（秒）",
        "type": "int",
        "category": "限流设置"
    },
    "xiuxian_global_command_overload_notice_rate_window": {
        "name": "过载提示频率窗口",
        "description": "过载提示全局发送频率统计窗口（秒）",
        "type": "int",
        "category": "限流设置"
    },
    "xiuxian_global_command_overload_notice_rate_limit": {
        "name": "过载提示频率上限",
        "description": "过载提示在频率窗口内的全局发送上限",
        "type": "int",
        "category": "限流设置"
    },
    "img": {
        "name": "图片发送",
        "description": "是否使用图片发送消息",
        "type": "bool",
        "category": "消息设置"
    },
    "user_info_image": {
        "name": "个人信息图片",
        "description": "是否使用图片发送个人信息",
        "type": "bool",
        "category": "消息设置"
    },
    "xiuxian_info_img": {
        "name": "网络背景图",
        "description": "开启则使用网络背景图",
        "type": "bool",
        "category": "消息设置"
    },
    "use_network_avatar": {
        "name": "网络头像",
        "description": "开启则使用网络头像",
        "type": "bool",
        "category": "消息设置"
    },
    "impart_image": {
        "name": "传承卡图",
        "description": "开启则使用发送图片",
        "type": "bool",
        "category": "消息设置"
    },
    "adapter_source": {
        "name": "适配器来源",
        "description": "vendor 使用插件内置适配器；installed 使用运行环境版本；auto 自动选择",
        "type": "str",
        "category": "运行设置"
    },
    "web_secret_key": {
        "name": "Web会话密钥",
        "description": "Flask 会话密钥；留空时使用 data/xiuxian/web_secret_key 或自动生成",
        "type": "str",
        "category": "Web安全"
    },
    "web_require_csrf": {
        "name": "CSRF校验",
        "description": "开启后 Web 写请求必须携带页面生成的 CSRF Token",
        "type": "bool",
        "category": "Web安全"
    },
    "web_session_cookie_secure": {
        "name": "HTTPS Cookie",
        "description": "HTTPS 部署时开启；纯 HTTP/局域网访问保持关闭",
        "type": "bool",
        "category": "Web安全"
    },
    "web_session_lifetime_minutes": {
        "name": "会话有效期",
        "description": "管理面板登录会话有效期（分钟）",
        "type": "int",
        "category": "Web安全"
    },
    "web_allowed_hosts": {
        "name": "Host白名单",
        "description": "允许访问面板的 Host，留空不限制，多个用逗号分隔",
        "type": "list[str]",
        "category": "Web安全"
    },
    "level_up_cd": {
        "name": "突破CD",
        "description": "突破CD（分钟）",
        "type": "int",
        "category": "修炼设置"
    },
    "closing_exp": {
        "name": "闭关修为",
        "description": "闭关每分钟获取的修为",
        "type": "int",
        "category": "修炼设置"
    },
    "tribulation_min_level": {
        "name": "最低渡劫境界",
        "description": "最低渡劫境界",
        "type": "select",
        "options": LEVELS,
        "category": "渡劫设置"
    },
    "tribulation_base_rate": {
        "name": "基础渡劫概率",
        "description": "基础渡劫概率（百分比）",
        "type": "int",
        "category": "渡劫设置"
    },
    "tribulation_max_rate": {
        "name": "最大渡劫概率",
        "description": "最大渡劫概率（百分比）",
        "type": "int",
        "category": "渡劫设置"
    },
    "tribulation_cd": {
        "name": "渡劫CD",
        "description": "渡劫冷却时间（分钟）",
        "type": "int",
        "category": "渡劫设置"
    },
    "sect_min_level": {
        "name": "创建宗门境界",
        "description": "创建宗门最低境界",
        "type": "select",
        "options": LEVELS,
        "category": "宗门设置"
    },
    "sect_create_cost": {
        "name": "创建宗门消耗",
        "description": "创建宗门消耗灵石",
        "type": "int",
        "category": "宗门设置"
    },
    "sect_rename_cost": {
        "name": "宗门改名消耗",
        "description": "宗门改名消耗灵石",
        "type": "int",
        "category": "宗门设置"
    },
    "sect_rename_cd": {
        "name": "宗门改名CD",
        "description": "宗门改名冷却时间（天）",
        "type": "int",
        "category": "宗门设置"
    },
    "auto_change_sect_owner_cd": {
        "name": "自动换宗主CD",
        "description": "自动换长时间不玩宗主CD（天）",
        "type": "int",
        "category": "宗门设置"
    },
    "closing_exp_upper_limit": {
        "name": "闭关修为上限",
        "description": "闭关获取修为上限倍数",
        "type": "float",
        "category": "修炼设置"
    },
    "mentor_transmission_limit": {
        "name": "师徒传功日限",
        "description": "师徒传功每日次数（按用户总次数）",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_max_apprentices": {
        "name": "收徒上限",
        "description": "师父最多可收徒弟数量",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_cooldown_days": {
        "name": "师父收徒冷却",
        "description": "师父逐出徒弟后的收徒冷却天数",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_apprentice_cooldown_days": {
        "name": "徒弟拜师冷却",
        "description": "徒弟离开师门后的拜师冷却天数",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_max_effect_gap": {
        "name": "传功最大境界差",
        "description": "师徒传功达到最大效果需要的境界差",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_new_bind_transmission_wait_hours": {
        "name": "新拜师传功等待",
        "description": "新拜师后多少小时内不可传功",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_same_pair_rebind_cooldown_days": {
        "name": "同对再拜冷却",
        "description": "与同一师父解除后再次拜同一人的冷却天数",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_graduate_pair_rebind_cooldown_days": {
        "name": "出师后再拜冷却",
        "description": "出师后再次拜同一师父的冷却天数",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_graduate_apprentice_stone_reward": {
        "name": "出师徒弟灵石",
        "description": "徒弟正常出师获得的灵石",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_graduate_mentor_stone_reward": {
        "name": "出师师父灵石",
        "description": "师父培养徒弟出师获得的灵石",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_history_limit": {
        "name": "师徒记录条数",
        "description": "师徒记录保留条数",
        "type": "int",
        "category": "师徒设置"
    },
    "mentor_breakthrough_reward_base_rate": {
        "name": "突破返修基础比例",
        "description": "徒弟突破时师父返修基础比例",
        "type": "float",
        "category": "师徒设置"
    },
    "mentor_breakthrough_reward_min_rate": {
        "name": "突破返修最低比例",
        "description": "徒弟低境界突破时师父返修最低比例",
        "type": "float",
        "category": "师徒设置"
    },
    "mentor_breakthrough_reward_max_rate": {
        "name": "突破返修最高比例",
        "description": "徒弟高境界突破时师父返修最高比例",
        "type": "float",
        "category": "师徒设置"
    },
    "banned_unseal_ids": {
        "name": "禁解封用户",
        "description": "禁止解封的用户 ID 列表",
        "type": "list[str]",
        "category": "基础设置"
    },
    "level_punishment_floor": {
        "name": "突破失败惩罚下限",
        "description": "突破失败扣除修为惩罚下限（百分比）",
        "type": "int",
        "category": "修炼设置"
    },
    "level_punishment_limit": {
        "name": "突破失败惩罚上限",
        "description": "突破失败扣除修为惩罚上限（百分比）",
        "type": "int",
        "category": "修炼设置"
    },
    "level_up_probability": {
        "name": "失败增加概率",
        "description": "突破失败增加当前境界突破概率的比例",
        "type": "float",
        "category": "修炼设置"
    },
    "max_goods_num": {
        "name": "物品上限",
        "description": "背包单样物品最高上限",
        "type": "int",
        "category": "资源设置"
    },
    "sign_in_lingshi_lower_limit": {
        "name": "签到灵石下限",
        "description": "每日签到灵石下限",
        "type": "int",
        "category": "资源设置"
    },
    "sign_in_lingshi_upper_limit": {
        "name": "签到灵石上限",
        "description": "每日签到灵石上限",
        "type": "int",
        "category": "资源设置"
    },
    "beg_max_level": {
        "name": "奇缘最高境界",
        "description": "仙途奇缘能领灵石最高境界",
        "type": "select",
        "options": LEVELS,
        "category": "资源设置"
    },
    "beg_max_days": {
        "name": "奇缘最多天数",
        "description": "仙途奇缘能领灵石最多天数",
        "type": "int",
        "category": "资源设置"
    },
    "beg_lingshi_lower_limit": {
        "name": "奇缘灵石下限",
        "description": "仙途奇缘灵石下限",
        "type": "int",
        "category": "资源设置"
    },
    "beg_lingshi_upper_limit": {
        "name": "奇缘灵石上限",
        "description": "仙途奇缘灵石上限",
        "type": "int",
        "category": "资源设置"
    },
    "tou": {
        "name": "偷灵石惩罚",
        "description": "偷灵石惩罚金额",
        "type": "int",
        "category": "资源设置"
    },
    "tou_lower_limit": {
        "name": "偷灵石下限",
        "description": "偷灵石下限（百分比）",
        "type": "float",
        "category": "资源设置"
    },
    "tou_upper_limit": {
        "name": "偷灵石上限",
        "description": "偷灵石上限（百分比）",
        "type": "float",
        "category": "资源设置"
    },
    "remake": {
        "name": "重入仙途消费",
        "description": "重入仙途的消费灵石",
        "type": "int",
        "category": "资源设置"
    },
    "remaname": {
        "name": "修仙改名消费",
        "description": "修仙改名的消费灵石",
        "type": "int",
        "category": "资源设置"
    },
    "max_stamina": {
        "name": "体力上限",
        "description": "体力上限值",
        "type": "int",
        "category": "体力设置"
    },
    "stamina_recovery_points": {
        "name": "体力恢复",
        "description": "体力恢复点数/分钟",
        "type": "int",
        "category": "体力设置"
    },
    "lunhui_min_level": {
        "name": "千世轮回境界",
        "description": "千世轮回最低境界",
        "type": "select",
        "options": LEVELS,
        "category": "轮回设置"
    },
    "twolun_min_level": {
        "name": "万世轮回境界",
        "description": "万世轮回最低境界",
        "type": "select",
        "options": LEVELS,
        "category": "轮回设置"
    },
    "threelun_min_level": {
        "name": "永恒轮回境界",
        "description": "永恒轮回最低境界",
        "type": "select",
        "options": LEVELS,
        "category": "轮回设置"
    },
    "Infinite_reincarnation_min_level": {
        "name": "无限轮回境界",
        "description": "无限轮回最低境界",
        "type": "select",
        "options": LEVELS,
        "category": "轮回设置"
    },
    "markdown_status": {
        "name": "markdown模板",
        "description": "是否发送模板信息（野机请勿打开）",
        "type": "bool",
        "category": "MD设置"
    },
    "markdown_id": {
        "name": "模板ID1",
        "description": "用于发送markdown文本",
        "type": "str",
        "category": "MD设置"
    },
    "markdown_id2": {
        "name": "模板ID2",
        "description": "用于发送markdown蓝字",
        "type": "str",
        "category": "MD设置"
    },
    "button_id": {
        "name": "按钮ID1",
        "description": "用于发送修炼按钮",
        "type": "str",
        "category": "MD设置"
    },
    "button_id2": {
        "name": "按钮ID2",
        "description": "用于发送修仙帮助按钮",
        "type": "str",
        "category": "MD设置"
    },
    "markdown_button_status": {
        "name": "Markdown按钮",
        "description": "开启后将原生Markdown蓝字命令转为QQ自定义键盘按钮",
        "type": "bool",
        "category": "MD设置"
    },
    "gsk_link": {
        "name": "gsk地址",
        "description": "用于发送md模板艾特",
        "type": "str",
        "category": "MD设置"
    },
    "web_link": {
        "name": "修仙管理面板地址",
        "description": "用于发送md图片",
        "type": "str",
        "category": "MD设置"
    },
    "update_image_web": {
        "name": "频道图床上传接口",
        "description": "用于上传图片",
        "type": "str",
        "category": "MD设置"
    },
    "channel_id": {
        "name": "频道图床ID",
        "description": "用于上传图片的频道",
        "type": "str",
        "category": "MD设置"
    },
    "merge_forward_send": {
        "name": "消息发送方式",
        "description": "1=长文本,2=合并转发,3=合并转长图,4=长文本合并转发",
        "type": "int",
        "category": "消息设置"
    },
    "message_optimization": {
        "name": "消息优化",
        "description": "是否开启信息优化",
        "type": "bool",
        "category": "消息设置"
    },
    "img_compression_limit": {
        "name": "图片压缩率",
        "description": "图片压缩率（0-100）",
        "type": "int",
        "category": "消息设置"
    },
    "img_type": {
        "name": "图片类型",
        "description": "webp或者jpeg",
        "type": "str",
        "category": "消息设置"
    },
    "img_send_type": {
        "name": "图片发送类型",
        "description": "io或base64",
        "type": "str",
        "category": "消息设置"
    },
    "cloud_backup_enabled": {
        "name": "开启自动云备份",
        "description": "手动备份或更新插件时，是否自动上传到云端",
        "type": "bool",
        "category": "云备份设置"
    },
    "webdav_url": {
        "name": "WebDAV 服务器地址",
        "description": "例如：http://192.168.1.10:5244/dav",
        "type": "str",
        "category": "云备份设置"
    },
    "webdav_user": {
        "name": "WebDAV 账号",
        "description": "云存储的登录用户名",
        "type": "str",
        "category": "云备份设置"
    },
    "webdav_pass": {
        "name": "WebDAV 密码",
        "description": "云存储的登录密码或授权码",
        "type": "str",
        "category": "云备份设置"
    },
    "webdav_target_subdir": {
        "name": "云端存储根目录",
        "description": "WebDAV 路径下的存放目录，如：backup/xiuxian",
        "type": "str",
        "category": "云备份设置"
    },
    "webdav_backup_folder": {
        "name": "备份二级目录",
        "description": "根目录下再套一层的目录名，默认：backups",
        "type": "str",
        "category": "云备份设置"
    },
    "webdav_delete_days": {
        "name": "云端自动清理天数",
        "description": "删除云端多少天之前的旧备份。0 表示永不删除",
        "type": "int",
        "category": "云备份设置"
    },
    "local_backup_keep_days": {
        "name": "本地备份保留天数",
        "description": "本地 backups 下数据库/插件/配置备份超过该天数在下次备份时清理。0 表示不自动删，默认 10",
        "type": "int",
        "category": "云备份设置"
    },
    "custom_proxy_enabled": {
        "name": "启用自定义代理",
        "description": "开启后，番剧等需代理的 HTTP 请求经下方地址转发",
        "type": "bool",
        "category": "网络代理"
    },
    "custom_proxy": {
        "name": "自定义代理地址",
        "description": "HTTP/SOCKS 代理地址，如 socks5://127.0.0.1:1080",
        "type": "str",
        "category": "网络代理"
    }
}

# 排除数据库相关的配置字段
EXCLUDED_CONFIG_FIELDS = [
    'sql_table', 'sql_user_xiuxian', 'sql_user_cd', 'sql_sects',
    'sql_buff', 'sql_back', 'level', 'version',
    # 内部/复杂结构：不进面板编辑与配置备份字段表
    'config_jsonpath', '_cache_mtime_ns', 'layout_bot_dict', 'qqq',
]

__all__ = ["CONFIG_EDITABLE_FIELDS", "EXCLUDED_CONFIG_FIELDS", "LEVELS"]
