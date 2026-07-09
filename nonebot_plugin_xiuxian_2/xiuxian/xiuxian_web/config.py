from .core import (
    LEVELS,
    XiuConfig,
    Xiu_Plugin,
    app,
    get_csrf_token,
    get_user_by_id,
    jsondata,
    jsonify,
    re,
    redirect,
    render_template,
    request,
    session,
    url_for,
    web_feature_enabled,
)

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
    "web_port": {
        "name": "管理面板端口",
        "description": "修仙管理面板端口号",
        "type": "int",
        "category": "Web设置"
    },
    "web_host": {
        "name": "管理面板IP",
        "description": "修仙管理面板IP地址",
        "type": "str",
        "category": "Web设置"
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
    "web_enable_terminal": {
        "name": "Web终端",
        "description": "是否启用 Web 终端入口",
        "type": "bool",
        "category": "Web安全"
    },
    "web_enable_update": {
        "name": "在线更新",
        "description": "是否启用在线检测更新和执行更新",
        "type": "bool",
        "category": "Web安全"
    },
    "web_enable_database_write": {
        "name": "数据库写入",
        "description": "是否允许 Web 数据库编辑、指令中心、活动数据和发放记录写入",
        "type": "bool",
        "category": "Web安全"
    },
    "web_enable_backup_restore": {
        "name": "备份恢复删除",
        "description": "是否允许 Web 执行备份、同步、恢复、下载和删除操作",
        "type": "bool",
        "category": "Web安全"
    },
    "web_enable_message_send": {
        "name": "消息主动发送",
        "description": "是否允许 Web 消息面板主动发送、广播和撤回消息",
        "type": "bool",
        "category": "Web安全"
    },
    "web_allow_local_upload": {
        "name": "本机免登上传",
        "description": "是否允许本机免登录调用 /upload_image",
        "type": "bool",
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
    'sql_buff', 'sql_back', 'level', 'version'
]

def get_config_values():
    """获取当前配置值"""
    config = XiuConfig()
    values = {}
    
    for field_name, field_info in CONFIG_EDITABLE_FIELDS.items():
        if hasattr(config, field_name):
            value = getattr(config, field_name)
            values[field_name] = value
    
    return values

def save_config_values(new_values):
    """
    保存配置到文件。
    支持自动格式化布尔值、数字、列表以及包含特殊字符的 WebDAV 字符串。
    """
    config_file_path = Xiu_Plugin / "xiuxian" / "xiuxian_config.py"
    
    if not config_file_path.exists():
        return False, "配置文件不存在"
    
    try:
        # 读取原文件内容
        with open(config_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        for field_name, new_value in new_values.items():
            # 只有在可编辑字段列表中的项才允许处理
            if field_name in CONFIG_EDITABLE_FIELDS:
                field_type = CONFIG_EDITABLE_FIELDS[field_name]["type"]
                
                # --- 1. 布尔类型转换 ---
                if field_type == "bool":
                    # 处理来自 Web 的 'true'/'false' 字符串或 checkbox 的 'on'
                    if str(new_value).lower() in ('true', '1', 'yes', 'on'):
                        formatted_value = "True"
                    else:
                        formatted_value = "False"
                
                # --- 2. 整数列表转换 [1, 2, 3] ---
                elif field_type == "list[int]":
                    if isinstance(new_value, str):
                        # 移除所有非数字和非逗号字符
                        cleaned = re.sub(r'[^0-9,]', '', new_value)
                        items = [i.strip() for i in cleaned.split(',') if i.strip()]
                        formatted_value = f"[{', '.join(items)}]"
                    else:
                        formatted_value = str(new_value)
                
                # --- 3. 字符串列表转换 ["a", "b"] ---
                elif field_type == "list[str]":
                    if isinstance(new_value, str):
                        # 移除外层方括号，按逗号分割，并去除每个元素两端的引号和空格
                        cleaned = new_value.strip().replace('[', '').replace(']', '')
                        items = [i.strip().strip("'").strip('"') for i in cleaned.split(',') if i.strip()]
                        # 统一使用双引号包裹每个元素
                        formatted_value = "[" + ", ".join([f'"{i}"' for i in items]) + "]"
                    else:
                        formatted_value = str(new_value)
                
                # --- 4. 数字类型转换 ---
                elif field_type == "int":
                    try:
                        formatted_value = str(int(float(new_value)))
                    except (ValueError, TypeError):
                        formatted_value = "0"
                
                elif field_type == "float":
                    try:
                        formatted_value = str(float(new_value))
                    except (ValueError, TypeError):
                        formatted_value = "0.0"
                
                # --- 5. 字符串/选择类型 (最关键：处理 URL、路径和密码) ---
                else:
                    # 确保是字符串并去除首尾空格
                    val_str = str(new_value).strip()
                    # 避免重复包裹：如果用户输入的字符串本身带了引号，先去掉
                    if (val_str.startswith('"') and val_str.endswith('"')) or \
                       (val_str.startswith("'") and val_str.endswith("'")):
                        val_str = val_str[1:-1]
                    
                    # 统一使用双引号包裹，这样即使字符串里有单引号（如密码）也不会崩
                    formatted_value = f'"{val_str}"'
                
                # --- 6. 执行正则替换 ---
                # 匹配模式：捕获 self.变量名 = 这一部分，然后替换掉后面直到行尾的内容
                # 能够处理 self.xxx=yyy, self.xxx = yyy, self.xxx   =   yyy 等各种写法
                pattern = rf"(self\.{re.escape(field_name)}\s*=\s*).+"
                # 检查文件中是否存在该配置项
                if re.search(pattern, content):
                    # \1 代表保留第一个捕获组 (即 self.变量名 = )
                        content = re.sub(
                            pattern,
                            lambda m: f"{m.group(1)}{formatted_value}",
                            content
                        )
                else:
                    # 如果配置项在文件中不存在，可能是手动删除了，这里记录日志但不中断
                    from nonebot.log import logger
                    logger.warning(f"[Web管理] 配置项 {field_name} 在 xiuxian_config.py 中未找到匹配行，跳过修改。")
        
        # 写入更新后的内容
        with open(config_file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return True, "配置保存成功，重启机器人后生效。"
    
    except Exception as e:
        return False, f"保存配置时出错: {str(e)}"

# 配置管理路由
@app.route('/config')
def config_management():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    
    current_config = get_config_values()
    
    # 预处理列表值用于显示
    for field_name, value in current_config.items():
        if field_name in CONFIG_EDITABLE_FIELDS:
            field_type = CONFIG_EDITABLE_FIELDS[field_name]["type"]
            if field_type in ['list[int]', 'list[str]']:
                # 格式化列表值用于显示
                current_config[field_name] = format_list_value_for_display(value, field_type)
    
    # 按分类分组配置项
    config_by_category = {}
    for field_name, field_info in CONFIG_EDITABLE_FIELDS.items():
        category = field_info["category"]
        if category not in config_by_category:
            config_by_category[category] = []
        
        config_item = {
            "field_name": field_name,
            "name": field_info["name"],
            "description": field_info["description"],
            "type": field_info["type"],
            "value": current_config.get(field_name, "")
        }
        
        if field_info["type"] == "select" and "options" in field_info:
            config_item["options"] = field_info["options"]
        
        config_by_category[category].append(config_item)
    
    return render_template('config.html', config_by_category=config_by_category)

def format_list_value_for_display(value, field_type):
    """格式化列表值用于显示"""
    if not value:
        return ''
    
    try:
        if isinstance(value, str):
            import ast
            value = ast.literal_eval(value)
        
        if isinstance(value, (list, tuple)):
            if field_type == 'list[int]':
                return ', '.join(str(x) for x in value)
            else:
                return ', '.join(str(x).strip('"\'') for x in value)
        else:
            return str(value)
    except (ValueError, SyntaxError):
        # 如果解析失败，返回清理后的值
        cleaned = str(value).replace('[', '').replace(']', '').replace('"', '').replace("'", '')
        return cleaned

@app.route('/save_config', methods=['POST'])
def save_config():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        config_data = request.get_json()
        if not config_data:
            return jsonify({"success": False, "error": "无效的配置数据"})
        
        success, message = save_config_values(config_data)
        return jsonify({"success": success, "message": message})
    
    except Exception as e:
        return jsonify({"success": False, "error": f"保存配置时出错: {str(e)}"})

@app.context_processor
def inject_navigation():
    """注入导航栏状态和辅助函数到所有模板"""
    def is_active(endpoint):
        """检查当前路由是否匹配给定的端点"""
        if isinstance(endpoint, (list, tuple)):
            return request.endpoint in endpoint
        return request.endpoint == endpoint
    
    return dict(
        get_command_icon=get_command_icon,
        get_config_category_icon=get_config_category_icon,
        is_active=is_active,
        csrf_token=get_csrf_token,
        web_feature_enabled=web_feature_enabled
    )

def get_root_rate(root_type, user_id):
    """获取灵根倍率（完整版本，参考原版实现）"""
    # 获取灵根数据
    root_data = jsondata.root_data()
    
    # 特殊处理命运道果
    if root_type == '命运道果':
        # 获取用户信息
        user_info = get_user_by_id(user_id)
        if not user_info:
            return 1.0
            
        root_level = user_info.get('root_level', 0)
        
        # 获取永恒道果和命运道果的倍率
        eternal_rate = root_data['永恒道果']['type_speeds']
        fate_rate = root_data['命运道果']['type_speeds']
        
        decay_steps = int(root_level) // 5
        fate_bonus = max(0.5, fate_rate - decay_steps * 0.3)
        return eternal_rate + (root_level * fate_bonus)
    else:
        # 普通灵根，直接从数据中获取倍率
        if root_type in root_data:
            return root_data[root_type]['type_speeds']
        else:
            # 如果找不到对应的灵根类型，返回默认值
            return 1.0

def get_command_icon(command_name):
    """获取命令对应的图标"""
    icon_map = {
        "gm_command": "fas fa-gem",
        "adjust_exp_command": "fas fa-fire",
        "gmm_command": "fas fa-recycle",
        "zaohua_xiuxian": "fas fa-mountain",
        "cz": "fas fa-gift",
        "hmll": "fas fa-trash",
        "ccll_command": "fas fa-history"
    }
    return icon_map.get(command_name, "fas fa-cog")

def get_config_category_icon(category):
    """获取配置分类对应的图标"""
    icon_map = {
        "基础设置": "fas fa-cube",
        "MD设置": "fas fa-palette",
        "调试设置": "fas fa-bug",
        "消息设置": "fas fa-comment",
        "Web设置": "fas fa-globe",
        "修炼设置": "fas fa-medal",
        "渡劫设置": "fas fa-bolt",
        "宗门设置": "fas fa-landmark",
        "资源设置": "fas fa-coins",
        "灵根设置": "fas fa-seedling",
        "体力设置": "fas fa-heart",
        "轮回设置": "fas fa-infinity",
        "限流设置": "fas fa-tachometer-alt",
        "云备份设置": "fas fa-cloud-upload-alt",
        "Web安全": "fas fa-shield-halved"
    }
    return icon_map.get(category, "fas fa-cog")
