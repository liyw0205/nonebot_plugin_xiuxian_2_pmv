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
)

from ...features.plugin_config.runtime import plugin_config_application
from ...features.plugin_config.schema import (
    CONFIG_EDITABLE_FIELDS,
    EXCLUDED_CONFIG_FIELDS,
    LEVELS,
)


def get_config_values():
    return plugin_config_application.get_values()


def save_config_values(new_values):
    return plugin_config_application.save_values(
        new_values, include_restart_notice=True
    )


def format_list_value_for_display(value, field_type):
    return plugin_config_application.format_list_value_for_display(value, field_type)

# 配置管理路由
@app.route('/config')
def config_management():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template(
        'config.html',
        config_by_category=plugin_config_application.config_by_category(),
    )

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
        csrf_token=get_csrf_token
    )

def get_root_rate(root_type, user_id):
    """获取灵根倍率（与 XiuxianDateManage.get_root_rate / compute_fate_root_rate 一致）"""
    from ..xiuxian_utils.numeric_bind import compute_fate_root_rate

    root_data = jsondata.root_data()
    if root_type == '命运道果':
        user_info = get_user_by_id(user_id)
        if not user_info:
            return 1.0
        root_level = user_info.get('root_level', 0)
        eternal_rate = root_data['永恒道果']['type_speeds']
        fate_step = root_data['命运道果']['type_speeds']
        return compute_fate_root_rate(root_level, eternal_rate, fate_step)
    if root_type in root_data:
        return root_data[root_type]['type_speeds']
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
