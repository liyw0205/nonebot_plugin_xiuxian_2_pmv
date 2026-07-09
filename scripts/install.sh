#!/bin/bash

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m'

# 全局变量
REPO_OWNER="liyw0205"
REPO_NAME="nonebot_plugin_xiuxian_2_pmv"
RELEASE_TAG="latest" # 默认获取最新版本
RELEASE_ASSET="project.tar.gz" # GitHub Release打包的文件名

# 备份相关
TEMP_OLD_CONFIG_DATA="" # 临时文件用于存储旧配置
CURRENT_PROJECT_VERSION="unknown" # 用于备份文件名的版本号

# xiuxian_config.py 的正确路径 (相对路径)
XIUXIAN_CONFIG_PATH_REL="src/plugins/nonebot_plugin_xiuxian_2/xiuxian/xiuxian_config.py"
XIUXIAN_CONFIG_PATH_ABS="" # 运行时会根据 $DIR 赋值

# Python 虚拟环境路径
VENV_PATH="/root/myenv" # 默认虚拟环境路径

# 需要直接通过 pip 拉取当前索引最新版本的核心依赖。
# 不通过 nb-cli 安装适配器，避免 nb-cli 的适配器版本映射落后于 PyPI。
CORE_PIP_PACKAGES=(
    "nb-cli"
    "nonebot2[fastapi,httpx,websockets,aiohttp]"
    "nonebot-adapter-onebot"
    "nonebot-adapter-qq"
    "nonebot_plugin_apscheduler"
)

# 带颜色的输出函数
ui_print() {
    local color=$1
    shift
    case $color in
        red) echo -e "${RED}$*${NC}" ;;
        green) echo -e "${GREEN}$*${NC}" ;;
        yellow) echo -e "${YELLOW}$*${NC}" ;;
        blue) echo -e "${BLUE}$*${NC}" ;;
        purple) echo -e "${PURPLE}$*${NC}" ;;
        cyan) echo -e "${CYAN}$*${NC}" ;;
        white) echo -e "${WHITE}$*${NC}" ;;
        *) echo -e "$*" ;;
    esac
}

# 显示操作状态
show_status() {
    local operation=$1
    local status=$2
    if [ "$status" = "success" ]; then
        ui_print "green" "✓ $operation 成功"
    else
        ui_print "red" "✗ $operation 失败"
    fi
}

# 显示进度
show_progress() {
    ui_print "blue" "正在 $1..."
}

# 读取输入（带默认值）
read_or() {
    local var_name="$1"
    local prompt="$2"
    local default_value="$3"
    local input

    if [[ -r /dev/tty ]] && { true < /dev/tty; } 2>/dev/null; then
        printf "%s (默认: %s): " "$prompt" "$default_value" > /dev/tty
        read -r input < /dev/tty || input=""
    else
        printf "%s (默认: %s): " "$prompt" "$default_value"
        read -r input || input=""
    fi
    if [[ -z "$input" ]]; then
        input="$default_value"
    fi
    eval "$var_name=\"\$input\""
}

# 安全创建目录
ensure_dir() {
    local d="$1"
    mkdir -p "$d" || return 1
    return 0
}

ensure_default_env_files() {
    ensure_dir "$DIR" || return 1

    if [[ ! -f "$DIR/.env" ]]; then
        cat <<'EOF' > "$DIR/.env"
ENVIRONMENT=dev
DRIVER=~fastapi+~httpx+~websockets+~aiohttp
EOF
        show_status "创建默认 .env" "success"
    fi

    if [[ ! -f "$DIR/.env.dev" ]]; then
        cat <<'EOF' > "$DIR/.env.dev"
LOG_LEVEL=INFO

SUPERUSERS = ["123456"]
COMMAND_START = [""]
NICKNAME = ["堂堂"]
DEBUG = False
HOST = 0.0.0.0
PORT = 8080
EOF
        show_status "创建默认 .env.dev" "success"
    fi
}

print_installed_dependency_versions() {
    local python_cmd="$1"
    "$python_cmd" - <<'PY' 2>/dev/null || true
import importlib.util
from importlib import metadata

packages = {
    "nb-cli": "nb_cli",
    "nonebot2": "nonebot",
    "nonebot-adapter-onebot": "nonebot.adapters.onebot.v11",
    "nonebot-adapter-qq": "nonebot.adapters.qq",
    "nonebot-plugin-apscheduler": "nonebot_plugin_apscheduler",
}

def module_location(module_name: str) -> str:
    spec = importlib.util.find_spec(module_name)
    if spec is None:
        return "未找到模块"
    if spec.submodule_search_locations:
        return next(iter(spec.submodule_search_locations))
    return spec.origin or "未知路径"

print("当前核心依赖版本和安装路径：")
for package, module_name in packages.items():
    try:
        version = metadata.version(package)
    except metadata.PackageNotFoundError:
        continue
    print(f"  {package}: {version}")
    print(f"    {module_name}: {module_location(module_name)}")
PY
}

# 直接使用 pip 更新核心依赖和 requirements.txt。
# pip 会从当前配置的索引源拉取最新可用版本，例如 nonebot-adapter-qq 1.7.1。
upgrade_python_dependencies() {
    local python_cmd="$VENV_PATH/bin/python"

    if [[ ! -x "$python_cmd" ]]; then
        ui_print "red" "未检测到虚拟环境 Python：$python_cmd"
        ui_print "yellow" "请先完成安装，或确认 VENV_PATH 指向正确虚拟环境。"
        return 1
    fi
    show_status "检测虚拟环境 $VENV_PATH" "success"

    "$python_cmd" -m pip --version > /dev/null 2>&1 || {
        ui_print "red" "当前 Python 未安装 pip：$python_cmd"
        return 1
    }

    "$python_cmd" -m pip config set global.index-url https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple > /dev/null 2>&1 || true
    show_status "设置 pip 镜像源为清华源" "success"

    show_progress "升级 pip"
    "$python_cmd" -m pip install -U pip > /dev/null 2>&1 || {
        show_status "升级 pip" "failure"
        return 1
    }
    show_status "升级 pip" "success"

    show_progress "升级 NoneBot 核心、驱动、适配器和常用插件"
    "$python_cmd" -m pip install -U --upgrade-strategy eager "${CORE_PIP_PACKAGES[@]}" > /dev/null 2>&1 || {
        show_status "升级核心依赖" "failure"
        return 1
    }
    show_status "升级核心依赖" "success"

    if [[ -f "$DIR/requirements.txt" ]]; then
        show_progress "升级项目依赖（requirements.txt）"
        "$python_cmd" -m pip install -U --upgrade-strategy eager -r "$DIR/requirements.txt" > /dev/null 2>&1 || {
            show_status "升级项目依赖（requirements.txt）" "failure"
            return 1
        }
        show_status "升级项目依赖（requirements.txt）" "success"
    else
        ui_print "yellow" "未找到 requirements.txt，跳过项目依赖更新"
    fi

    print_installed_dependency_versions "$python_cmd"
    return 0
}

# 检查 bc，如果不存在则退回 awk
check_bc_command() {
    if ! command -v bc &> /dev/null; then
        ui_print "yellow" "检测到bc命令不可用，正在尝试安装..."
        if command -v apt &> /dev/null; then
            apt update > /dev/null 2>&1 && apt install -y bc > /dev/null 2>&1
        elif command -v yum &> /dev/null; then
            yum install -y bc > /dev/null 2>&1
        elif command -v dnf &> /dev/null; then
            dnf install -y bc > /dev/null 2>&1
        else
            ui_print "yellow" "无法自动安装bc，将使用awk进行数值比较"
            return 1
        fi

        if command -v bc &> /dev/null; then
            ui_print "green" "✓ bc安装成功"
            return 0
        else
            ui_print "yellow" "bc安装失败，将使用awk进行数值比较"
            return 1
        fi
    fi
    return 0
}

# 数值比较（兼容无bc）
compare_numbers() {
    local num1=$1
    local op=$2
    local num2=$3
    if command -v bc &> /dev/null; then
        echo "$num1 $op $num2" | bc -l
    else
        awk "BEGIN {print ($num1 $op $num2)}"
    fi
}

# 测试代理
test_proxy() {
    local proxy_url="$1"
    local test_url="${2:-https://github.com/$REPO_OWNER/$REPO_NAME/releases/$RELEASE_TAG/download/$RELEASE_ASSET}"
    local full_url="${proxy_url}${test_url}"
    local temp_file
    local cost
    local magic=""

    if ! command -v curl &> /dev/null; then
        echo "999999"
        return 1
    fi

    temp_file="$(mktemp)" || {
        echo "999999"
        return 1
    }

    cost="$(
        curl -fL -r 0-4095 --connect-timeout 5 --max-time 12 -o "$temp_file" -s -w '%{time_total}' "$full_url" 2>/dev/null || true
    )"

    if [[ -s "$temp_file" ]]; then
        magic="$(head -c 2 "$temp_file" | od -An -tx1 | tr -d ' \n')"
    fi
    rm -f "$temp_file"

    if [[ -n "$cost" && "$magic" == "1f8b" ]]; then
        awk -v n="$cost" 'BEGIN { printf "%d", n * 1000 }'
        return 0
    fi

    echo "999999"
    return 1
}

# 获取可用代理列表
get_available_proxies() {
    local proxies=(
        "https://gh-proxy.com/"
        "https://gh.jasonzeng.dev/"
        "https://git.yylx.win/"
        "https://wget.la/"
        "https://github.dpik.top/"
        "https://ghproxy.imciel.com/"
    )

    local proxy_latency=()
    local total=${#proxies[@]}
    local current=0

    ui_print "yellow" "正在测试代理服务器拼接下载可用性和延迟..."
    for proxy in "${proxies[@]}"; do
        current=$((current + 1))
        printf "\r${BLUE}进度: %d/%d - 测试 %s${NC}" "$current" "$total" "$proxy"
        latency=$(test_proxy "$proxy")
        proxy_latency+=("$latency:$proxy")

        if (( $(compare_numbers "$latency" "<" "999999") )); then
            ui_print "green" "\n✓ $proxy 可达 (延迟: ${latency}ms)"
        else
            ui_print "red" "\n✗ $proxy 不可达"
        fi
    done
    echo # 确保最后有一个换行

    local available_proxies_with_latency=()
    for item in "${proxy_latency[@]}"; do
        latency=$(echo "$item" | cut -d':' -f1)
        if (( $(compare_numbers "$latency" "<" "999999") )); then
            available_proxies_with_latency+=("$item")
        fi
    done

    # 按延迟排序
    IFS=$'\n' sorted_proxies_with_latency=($(sort -t: -k1 -n <<< "${available_proxies_with_latency[*]}"))
    unset IFS

    if [ ${#sorted_proxies_with_latency[@]} -eq 0 ]; then
        ui_print "red" "没有找到可用代理服务器"
        return 1
    fi

    ui_print "green" "找到 ${#sorted_proxies_with_latency[@]} 个可用代理（按延迟排序）："
    for i in "${!sorted_proxies_with_latency[@]}"; do
        latency=$(echo "${sorted_proxies_with_latency[$i]}" | cut -d':' -f1)
        proxy=$(echo "${sorted_proxies_with_latency[$i]}" | cut -d':' -f2-)
        ui_print "cyan" "  $((i+1)). $proxy (延迟: ${latency}ms)"
    done
    echo

    AVAILABLE_PROXIES=()
    for item in "${sorted_proxies_with_latency[@]}"; do
        proxy=$(echo "$item" | cut -d':' -f2-)
        AVAILABLE_PROXIES+=("$proxy")
    done

    return 0
}

# 选择代理
get_proxy_choice() {
    ui_print "yellow" "请选择是否使用下载代理："
    ui_print "white" "1：自动选择最低延迟可用代理（默认）"
    ui_print "white" "2：手动选择可用代理"
    ui_print "white" "3：不使用代理"
    ui_print "white" "4：自定义代理"
    echo

    read_or PROXY_CHOICE "请选择代理选项 (1-4)" "1"

    PROXY="" # 最终选择的代理URL
    case "$PROXY_CHOICE" in
        1)
            check_bc_command # 确保bc或awk可用
            if get_available_proxies; then
                PROXY="${AVAILABLE_PROXIES[0]}"
                latency=$(echo "${sorted_proxies_with_latency[0]}" | cut -d':' -f1)
                ui_print "green" "✓ 自动选择代理：$PROXY (延迟: ${latency}ms)"
            else
                ui_print "yellow" "未找到可用代理，将直连"
            fi
            ;;
        2)
            check_bc_command
            if get_available_proxies; then
                read_or PROXY_INDEX "请选择代理编号 (1-${#AVAILABLE_PROXIES[@]})" "1"
                if [[ "$PROXY_INDEX" =~ ^[0-9]+$ ]] && [ "$PROXY_INDEX" -ge 1 ] && [ "$PROXY_INDEX" -le "${#AVAILABLE_PROXIES[@]}" ]; then
                    PROXY="${AVAILABLE_PROXIES[$((PROXY_INDEX-1))]}"
                    ui_print "green" "✓ 已选择代理：$PROXY"
                else
                    ui_print "yellow" "无效编号，将直连"
                fi
            else
                ui_print "yellow" "未找到可用代理，将直连"
            fi
            ;;
        3)
            ui_print "green" "✓ 不使用代理"
            ;;
        4)
            read_or CUSTOM_PROXY "请输入自定义代理地址(如 https://xxx/)" ""
            if [[ -n "$CUSTOM_PROXY" ]]; then
                check_bc_command
                latency=$(test_proxy "$CUSTOM_PROXY")
                if (( $(compare_numbers "$latency" "<" "999999") )); then
                    PROXY="$CUSTOM_PROXY"
                    ui_print "green" "✓ 自定义代理可用：$PROXY"
                else
                    ui_print "yellow" "自定义代理不可达，将直连"
                fi
            else
                ui_print "yellow" "未输入代理地址，将直连"
            fi
            ;;
        *)
            ui_print "yellow" "无效选项，将直连"
            ;;
    esac
}

# 下载release资源（支持代理重试）
download_release_resource() {
    local release_url="$1"
    local download_path="$2"
    local proxy_urls="$3"

    show_progress "下载release资源文件"

    local proxy_array=()
    if [[ -z "$proxy_urls" || "$proxy_urls" == " " ]]; then
        proxy_array=("") # 直连
    elif [[ "$proxy_urls" == "("*")" ]]; then
        # shellcheck disable=SC2206
        proxy_array=(${proxy_urls//[()]/})
    else
        proxy_array=("$proxy_urls")
    fi

    for proxy_url in "${proxy_array[@]}"; do
        local download_full_url
        if [[ -n "$proxy_url" ]]; then
            download_full_url="${proxy_url}${release_url}"
            ui_print "cyan" "尝试代理下载: $download_full_url"
        else
            download_full_url="$release_url"
            ui_print "cyan" "尝试直连下载: $download_full_url"
        fi

        if command -v wget &> /dev/null; then
            if wget -q -O "$download_path" "$download_full_url"; then
                return 0
            fi
        elif command -v curl &> /dev/null; then
            if curl -s -L -o "$download_path" "$download_full_url"; then
                return 0
            fi
        else
            ui_print "red" "错误: 未找到wget或curl命令用于下载"
            return 1
        fi

        rm -f "$download_path" 2>/dev/null
        ui_print "yellow" "下载失败，尝试下一个方式..."
    done

    return 1 # 所有方式都失败
}

# 解压资源
extract_release_resource() {
    local archive_path="$1"
    local extract_dir="$2"
    local wrapper_dir
    local top_level_dirs=()

    show_progress "解压release资源文件"
    rm -rf "$extract_dir"
    ensure_dir "$extract_dir" || return 1

    if [[ "$archive_path" == *.tar.gz ]]; then
        tar -xzf "$archive_path" -C "$extract_dir" || return 1
    elif [[ "$archive_path" == *.zip ]]; then
        if command -v unzip &> /dev/null; then
            unzip -q "$archive_path" -d "$extract_dir" || return 1
        else
            ui_print "red" "错误: 未找到unzip命令用于解压zip文件"
            return 1
        fi
    else
        ui_print "red" "错误: 不支持的压缩格式（仅支持 .tar.gz 或 .zip）"
        return 1
    fi

    # Release 可能是仓库根目录，也可能额外包了一层目录，统一成固定布局。
    if [[ ! -d "$extract_dir/nonebot_plugin_xiuxian_2" ]]; then
        while IFS= read -r -d "" wrapper_dir; do
            top_level_dirs+=("$wrapper_dir")
        done < <(find "$extract_dir" -mindepth 1 -maxdepth 1 -type d -print0)

        if [[ ${#top_level_dirs[@]} -eq 1 && -d "${top_level_dirs[0]}/nonebot_plugin_xiuxian_2" ]]; then
            wrapper_dir="${top_level_dirs[0]}"
            cp -a "$wrapper_dir/." "$extract_dir/" || return 1
            rm -rf "$wrapper_dir"
        fi
    fi

    if [[ ! -f "$extract_dir/nonebot_plugin_xiuxian_2/__init__.py" ]]; then
        ui_print "red" "错误: Release 包缺少 nonebot_plugin_xiuxian_2/__init__.py，拒绝覆盖现有安装。"
        return 1
    fi

    return 0
}

replace_plugin_source() {
    local source_dir="$1"
    local target_dir="$DIR/src/plugins/nonebot_plugin_xiuxian_2"
    local staging_dir="$DIR/src/plugins/.nonebot_plugin_xiuxian_2.new.$$"
    local previous_dir="$DIR/src/plugins/.nonebot_plugin_xiuxian_2.old.$$"

    rm -rf "$staging_dir" "$previous_dir"
    ensure_dir "$staging_dir" || return 1
    cp -a "$source_dir/." "$staging_dir/" || { rm -rf "$staging_dir"; return 1; }

    if [[ -d "$target_dir" ]]; then
        mv "$target_dir" "$previous_dir" || { rm -rf "$staging_dir"; return 1; }
    fi
    if ! mv "$staging_dir" "$target_dir"; then
        [[ -d "$previous_dir" ]] && mv "$previous_dir" "$target_dir"
        return 1
    fi
    rm -rf "$previous_dir"
}

validate_deployment() {
    local plugin_dir="$DIR/src/plugins/nonebot_plugin_xiuxian_2"

    [[ -f "$plugin_dir/__init__.py" ]] || {
        ui_print "red" "错误: 部署后缺少插件入口文件。"
        return 1
    }
    if grep -R -n -E "(^|[[:space:]])(from|import)[[:space:]]+nonebot_plugin_xiuxian_2([[:space:].]|$)" \
        --include="*.py" "$plugin_dir" > /dev/null; then
        ui_print "red" "错误: 部署代码仍包含不兼容 src.plugins 命名空间的绝对导入。"
        return 1
    fi
    return 0
}

# 写入/覆盖日志轮转配置 + cron任务
setup_logrotate_and_cron() {
    ensure_dir "$DIR/logs" || return 1
    ensure_dir "/etc/logrotate.d" || { show_status "创建目录 /etc/logrotate.d" "failure"; return 1; }
    ensure_dir "/etc/cron.d" || { show_status "创建目录 /etc/cron.d" "failure"; return 1; }
    ensure_dir "/var/lib/logrotate" || true

    # logrotate 配置文件
    cat <<EOF > "/etc/logrotate.d/$PROJECT_NAME"
"$DIR/${PROJECT_NAME}.log" {
    size 10M
    rotate 10
    compress
    missingok
    notifempty
    copytruncate
    dateext
    dateformat -%Y%m%d-%H%M%S
    olddir $DIR/logs
}
EOF
    if [[ $? -eq 0 ]]; then
        show_status "创建 logrotate 配置 /etc/logrotate.d/$PROJECT_NAME" "success"
    else
        show_status "创建 logrotate 配置 /etc/logrotate.d/$PROJECT_NAME" "failure"
        return 1
    fi

    # cron 配置：每10分钟检查一次日志轮转
    cat <<EOF > "/etc/cron.d/${PROJECT_NAME}_logrotate"
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
*/10 * * * * root /usr/sbin/logrotate -s /var/lib/logrotate/status /etc/logrotate.d/$PROJECT_NAME >/dev/null 2>&1
EOF
    chmod 644 "/etc/cron.d/${PROJECT_NAME}_logrotate"

    if [[ $? -eq 0 ]]; then
        show_status "创建定时轮转任务 /etc/cron.d/${PROJECT_NAME}_logrotate" "success"
    else
        show_status "创建定时轮转任务 /etc/cron.d/${PROJECT_NAME}_logrotate" "failure"
        return 1
    fi

    return 0
}

# --- 备份/恢复相关函数 ---

backup_before_update() {
    ui_print "yellow" "正在执行更新前自动备份..."

    local TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    local BACKUP_ROOT_DIR="$DIR/data/xiuxian/backups"
    local CONFIG_BACKUP_DIR="$BACKUP_ROOT_DIR/config_backups"

    ensure_dir "$BACKUP_ROOT_DIR" || { ui_print "red" "无法创建备份目录 $BACKUP_ROOT_DIR"; return 1; }
    ensure_dir "$CONFIG_BACKUP_DIR" || { ui_print "red" "无法创建备份目录 $CONFIG_BACKUP_DIR"; return 1; }

    if [ -f "$DIR/data/xiuxian/version.txt" ]; then
        CURRENT_PROJECT_VERSION=$(cat "$DIR/data/xiuxian/version.txt" | tr -d '[:space:]')
    fi

    # 1. 插件文件备份 (模仿 Python enhanced_backup_current_version)
    local PLUGIN_BACKUP_NAME="backup_${TIMESTAMP}_${CURRENT_PROJECT_VERSION}.zip"
    show_progress "创建插件文件备份: ${PLUGIN_BACKUP_NAME}"

    # 排除项 (路径相对于 $DIR)
    local EXCLUDES_ZIP=(
        ".git/*"           # Git 版本控制文件
        ".mypy_cache/*"    # MyPy 类型检查缓存
        ".pytest_cache/*"  # Pytest 测试缓存
        "*/__pycache__/*"  # 所有 __pycache__ 目录
        "*.pyc"            # 编译后的 Python 文件
        "*.bak"            # 备份文件
        "*.tmp"            # 临时文件
        "*.temp"           # 临时文件
        "logs/*"           # 日志目录

        # data/xiuxian 目录下的特定排除项
        "data/xiuxian/backups/*"
        "data/xiuxian/backups/config_backups/*"
        "data/xiuxian/backups/db_backup/*"
        "data/xiuxian/cache/*"
        "data/xiuxian/boss_img/*"
        "data/xiuxian/font/*"
        "data/xiuxian/卡图/*"
        "data/xiuxian/message.db*"
        "data/xiuxian/activity/activity.db*"
    )

    # 切换到项目根目录执行 zip 命令，确保备份的相对路径正确
    # -x 参数的模式是文件路径相对于 zip 命令执行的当前目录
    ( cd "$DIR" && zip -r -q "${BACKUP_ROOT_DIR}/${PLUGIN_BACKUP_NAME}" \
        "data/xiuxian" \
        "src/plugins/nonebot_plugin_xiuxian_2" \
        -x "${EXCLUDES_ZIP[@]}"
    )
    if [ $? -eq 0 ]; then
        show_status "插件文件备份" "success"
    else
        ui_print "yellow" "警告: 插件文件备份过程中可能出现异常，请检查！"
    fi

    # 2. xiuxian_config.py 配置备份 (模仿 Python backup_all_configs)
    local CONFIG_FILE_PATH="$XIUXIAN_CONFIG_PATH_ABS"
    local CONFIG_BACKUP_NAME="config_backup_${TIMESTAMP}.json"
    show_progress "创建 xiuxian_config.py 配置快照: ${CONFIG_BACKUP_NAME}"

    if [ -f "$CONFIG_FILE_PATH" ]; then
        echo "{" > "${CONFIG_BACKUP_DIR}/${CONFIG_BACKUP_NAME}"
        grep -E "^[[:space:]]+self\.[a-zA-Z0-9_]+[[:space:]]*=" "$CONFIG_FILE_PATH" | \
        while IFS= read -r line; do
            local var_name=$(echo "$line" | sed -E 's/^[[:space:]]*self\.([a-zA-Z0-9_]+)[[:space:]]*=.*/\1/')
            local var_value_raw=$(echo "$line" | sed -E 's/^[[:space:]]*self\.[a-zA-Z0-9_]+[[:space:]]*=(.*)/\1/')
            # 去除首尾空格
            local var_value=$(echo "$var_value_raw" | sed -E 's/^[[:space:]]*//;s/[[:space:]]*$//')

            # 对值进行 JSON 格式转换和转义
            if [[ "$var_value" =~ ^(True|False)$ ]]; then
                var_value=$(echo "$var_value" | tr '[:upper:]' '[:lower:]')
            elif [[ "$var_value" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                : # 数字，保持不变
            elif [[ "$var_value" =~ ^\[.*\]$ ]]; then
                # 对于列表，确保内部引号和转义
                var_value=$(echo "$var_value" | sed -E 's/\"/\\\"/g') # 转义内部双引号
            elif [[ "$var_value" =~ ^\".*\"$ ]] || [[ "$var_value" =~ ^\'.*\'$ ]]; then
                var_value=$(echo "$var_value" | sed -E 's/^[\x27"]|[\x27"]$//g') # 去掉原有引号
                var_value="\"$(echo "$var_value" | sed -E 's/\"/\\\"/g')\"" # 加上双引号并转义
            else
                var_value="\"$(echo "$var_value" | sed -E 's/\"/\\\"/g')\""
            fi
            echo "  \"$var_name\": $var_value,"
        done | sed '$ s/,$//' >> "${CONFIG_BACKUP_DIR}/${CONFIG_BACKUP_NAME}"
        echo "}" >> "${CONFIG_BACKUP_DIR}/${CONFIG_BACKUP_NAME}"

        show_status "xiuxian_config.py 配置快照" "success"
    else
        ui_print "yellow" "警告: 未找到 xiuxian_config.py，跳过配置快照。"
    fi
}

# 提取当前 xiuxian_config.py 中的配置项，以便更新后恢复
extract_old_config_values() {
    local config_file="$XIUXIAN_CONFIG_PATH_ABS"
    if [ -f "$config_file" ]; then
        TEMP_OLD_CONFIG_DATA=$(mktemp)
        # 提取整个 `self.var = value` 行，包括缩进和等号周围的空格
        grep -E "^[[:space:]]+self\.[a-zA-Z0-9_]+[[:space:]]*=" "$config_file" > "$TEMP_OLD_CONFIG_DATA"
        ui_print "green" "✓ 旧配置值已提取到临时文件。"
    else
        ui_print "yellow" "警告: 未找到 xiuxian_config.py，跳过旧配置提取。"
    fi
}

# 将之前提取的配置项写入新的 xiuxian_config.py 文件
apply_old_config_values() {
    local new_config_file="$XIUXIAN_CONFIG_PATH_ABS"
    if [ -f "$TEMP_OLD_CONFIG_DATA" ] && [ -f "$new_config_file" ]; then
        show_progress "应用旧配置到新的 xiuxian_config.py"

        while IFS= read -r old_config_line; do
            # 从旧配置行中提取变量名和原始值（包含空格）
            local var_name=$(echo "$old_config_line" | sed -E 's/^[[:space:]]*self\.([a-zA-Z0-9_]+)[[:space:]]*=.*/\1/')
            local var_value_raw=$(echo "$old_config_line" | sed -E 's/^[[:space:]]*self\.[a-zA-Z0-9_]+[[:space:]]*=(.*)/\1/')

            # 去除值两端的空格，确保插入时不会有冗余空格
            local var_value=$(echo "$var_value_raw" | sed -E 's/^[[:space:]]*//;s/[[:space:]]*$//')

            # 对最终值进行转义，以防止 sed 命令中的特殊字符问题
            local escaped_var_value=$(echo "$var_value" | sed 's/[\/&]/\\&/g')

            # 使用 sed 替换新文件中的对应行
            # 模式解释：
            # (^[[:space:]]*self\.${var_name})        -> 捕获行首到变量名的部分 (包括缩进)
            # [[:space:]]*=[[:space:]]*(.*)          -> 匹配等号和其周围的任意空格，以及等号后的所有内容
            # \1 = ${escaped_var_value}              -> 用捕获的前缀、一个空格、等号、一个空格和处理后的值进行替换
            sed -i -E "s|(^[[:space:]]*self\.${var_name})[[:space:]]*=[[:space:]]*(.*)|\1 = ${escaped_var_value}|" "$new_config_file"
        done < "$TEMP_OLD_CONFIG_DATA"

        rm -f "$TEMP_OLD_CONFIG_DATA"
        show_status "旧配置应用" "success"
    else
        ui_print "yellow" "警告: 无法应用旧配置，可能是临时文件或新配置文件不存在。"
    fi
}

# 测试可只加载函数，不执行安装主流程。
if [[ "${XIUXIAN_INSTALLER_LIBRARY_ONLY:-0}" == "1" ]]; then
    return 0 2>/dev/null || exit 0
fi

# --- 主流程开始 ---

# ---------------- 参数解析 ----------------
DEFAULT_PROJECT_NAME="xiu2"
ACTION="install"
TARGET_INPUT="$DEFAULT_PROJECT_NAME"

show_main_menu() {
    ui_print "green" "========================================"
    ui_print "green" "请选择要执行的操作"
    ui_print "white" "1. 安装"
    ui_print "white" "2. 重装（删除目标安装目录后重新安装）"
    ui_print "white" "3. 更新"
    ui_print "white" "4. 更新依赖"
    ui_print "white" "5. 退出"
    ui_print "green" "========================================"

    local choice
    read_or choice "请输入编号" "1"
    case "$choice" in
        1)
            ACTION="install"
            ;;
        2)
            ACTION="reinstall"
            ;;
        3)
            ACTION="update"
            ;;
        4)
            ACTION="update-deps"
            ;;
        5)
            ui_print "yellow" "已退出。"
            exit 0
            ;;
        *)
            ui_print "red" "无效选择：$choice"
            exit 127
            ;;
    esac

    read_or TARGET_INPUT "请输入项目名或绝对路径" "$DEFAULT_PROJECT_NAME"
}

# 根据参数判断动作和目标
if [ $# -eq 0 ]; then
    show_main_menu
elif [ $# -eq 1 ]; then
    case "$1" in
        install)
            ACTION="install"
            TARGET_INPUT="$DEFAULT_PROJECT_NAME"
            ;;
        reinstall)
            ACTION="reinstall"
            TARGET_INPUT="$DEFAULT_PROJECT_NAME"
            ;;
        update)
            ACTION="update"
            TARGET_INPUT="$DEFAULT_PROJECT_NAME"
            ;;
        update-deps|deps|upgrade-deps)
            ACTION="update-deps"
            TARGET_INPUT="$DEFAULT_PROJECT_NAME"
            ;;
        *)
            ACTION="install"
            TARGET_INPUT="$1"
            ;;
    esac
else
    case "$1" in
        install|reinstall|update|update-deps|deps|upgrade-deps)
            ACTION="$1"
            if [[ "$ACTION" == "deps" || "$ACTION" == "upgrade-deps" ]]; then
                ACTION="update-deps"
            fi
            TARGET_INPUT="$2"
            ;;
        *)
            ui_print "red" "参数错误: 第一个参数只能是 install、reinstall、update 或 update-deps"
            ui_print "yellow" "用法: $0 [install|reinstall|update|update-deps] [project_name|/abs/path]"
            ui_print "yellow" "兼容用法: $0 [project_name|/abs/path]"
            exit 127
            ;;
    esac
fi

# 确定安装目录和项目名称
if [[ "$TARGET_INPUT" == /* ]]; then # 如果是绝对路径
    DIR="$TARGET_INPUT"
    PROJECT_NAME="$(basename "$DIR")"
else # 如果是项目名
    PROJECT_NAME="$TARGET_INPUT"
    DIR="/root/$PROJECT_NAME"
fi

# 如果项目名为空，使用默认值
if [[ -z "$PROJECT_NAME" ]]; then
    PROJECT_NAME="$DEFAULT_PROJECT_NAME"
fi

# 设置 xiuxian_config.py 的绝对路径
XIUXIAN_CONFIG_PATH_ABS="$DIR/$XIUXIAN_CONFIG_PATH_REL"

ui_print "green" "执行模式: $ACTION"
ui_print "green" "项目名称: $PROJECT_NAME"
ui_print "green" "安装目录: $DIR"
ui_print "green" "修仙配置路径: $XIUXIAN_CONFIG_PATH_ABS"

if [[ "$ACTION" == "reinstall" ]]; then
    if [[ -d "$DIR" ]]; then
        ui_print "red" "重装会删除目标安装目录：$DIR"
        ui_print "yellow" "如需保留数据，请先手动备份 data/xiuxian。"
        read_or REINSTALL_CONFIRM "确认重装请输入 YES" "NO"
        if [[ "$REINSTALL_CONFIRM" != "YES" ]]; then
            ui_print "yellow" "已取消重装。"
            exit 0
        fi
        rm -rf "$DIR" || { show_status "删除旧安装目录 $DIR" "failure"; exit 127; }
        show_status "删除旧安装目录 $DIR" "success"
    fi
    ACTION="install"
fi

if [[ "$ACTION" == "update-deps" ]]; then
    upgrade_python_dependencies || exit 127
    ui_print "green" "========================================"
    ui_print "green" "✓ 依赖更新完成！"
    ui_print "green" "项目名称: $PROJECT_NAME"
    ui_print "green" "安装目录: $DIR"
    ui_print "green" "========================================"
    exit 0
fi

# 检查目录状态并决定下一步操作
if [[ "$ACTION" == "install" ]]; then
    if [[ -d "$DIR" ]]; then
        ui_print "red" "安装目录已存在，请使用 'update' 命令或先删除旧目录：$DIR"
        exit 127
    fi
    # 创建所有必要的目录
    ensure_dir "$DIR" || { show_status "创建安装主目录 $DIR" "failure"; exit 127; }
    show_status "创建安装主目录 $DIR" "success"
    ensure_dir "$DIR/src/plugins" || { show_status "创建插件目录 $DIR/src/plugins" "failure"; exit 127; }
    show_status "创建插件目录 $DIR/src/plugins" "success"
    ensure_dir "$DIR/data/xiuxian" || { show_status "创建修仙数据目录 $DIR/data/xiuxian" "failure"; exit 127; }
    show_status "创建修仙数据目录 $DIR/data/xiuxian" "success"
    ensure_dir "$DIR/logs" || { show_status "创建日志目录 $DIR/logs" "failure"; exit 127; }
    show_status "创建日志目录 $DIR/logs" "success"
else # ACTION == "update"
    if [[ ! -d "$DIR" ]]; then
        ui_print "yellow" "目录 $DIR 不存在，自动切换到 install 模式。"
        ACTION="install"
        # 切换到 install 模式后，再次创建目录
        ensure_dir "$DIR" || { show_status "创建安装主目录 $DIR" "failure"; exit 127; }
        ensure_dir "$DIR/src/plugins" || { show_status "创建插件目录 $DIR/src/plugins" "failure"; exit 127; }
        ensure_dir "$DIR/data/xiuxian" || { show_status "创建修仙数据目录 $DIR/data/xiuxian" "failure"; exit 127; }
        ensure_dir "$DIR/logs" || { show_status "创建日志目录 $DIR/logs" "failure"; exit 127; }
    else
        ui_print "green" "检测到安装目录，执行更新：$DIR"
        ensure_dir "$DIR/src/plugins" || { show_status "创建插件目录 $DIR/src/plugins" "failure"; }
        ensure_dir "$DIR/data/xiuxian" || { show_status "创建修仙数据目录 $DIR/data/xiuxian" "failure"; }
        ensure_dir "$DIR/logs" || { show_status "创建日志目录 $DIR/logs" "failure"; }
    fi
fi


if [[ "$ACTION" == "install" ]]; then
    show_progress "更新系统及安装依赖 (screen, curl, wget, git, python3, pip, venv, bc, tar, unzip, logrotate, zip)"
    if command -v apt &> /dev/null; then
        apt update > /dev/null 2>&1 && apt upgrade -y > /dev/null 2>&1 && apt install -y screen curl wget git python3 python3-pip python3-venv bc tar unzip logrotate cron zip > /dev/null 2>&1
    elif command -v yum &> /dev/null; then
        yum update -y > /dev/null 2>&1 && yum install -y screen curl wget git python3 python3-pip python3-virtualenv bc tar unzip logrotate cronie zip > /dev/null 2>&1
    elif command -v dnf &> /dev/null; then
        dnf update -y > /dev/null 2>&1 && dnf install -y screen curl wget git python3 python3-pip python3-virtualenv bc tar unzip logrotate cronie zip > /dev/null 2>&1
    else
        ui_print "red" "不支持的包管理器，请手动安装依赖: screen, curl, wget, git, python3, python3-pip, python3-venv (或 python3-virtualenv), bc, tar, unzip, logrotate, cron/cronie, zip"
        exit 127
    fi
    [ $? -eq 0 ] && show_status "系统更新及依赖安装" "success" || { show_status "系统更新及依赖安装" "failure"; exit 127; }

    # 生成 pyproject.toml 文件
    cat <<EOF > "$DIR/pyproject.toml"
[project]
name = "$PROJECT_NAME"
version = "0.1.0"
description = "$PROJECT_NAME"
readme = "README.md"
requires-python = ">=3.9, <4.0"
dependencies = [
    "nonebot2[fastapi]>=2.4.4",
    "nonebot2[httpx]>=2.4.4",
    "nonebot2[websockets]>=2.4.4",
    "nonebot2[aiohttp]>=2.4.4",
    "nonebot-adapter-onebot>=2.4.6",
    "nonebot-adapter-qq>=1.7.1"
]

[project.optional-dependencies]
dev = []

[tool.nonebot]
plugin_dirs = ["src/plugins"]
builtin_plugins = []

[tool.nonebot.adapters]
nonebot-adapter-onebot = [
    { name = "OneBot V11", module_name = "nonebot.adapters.onebot.v11" }
]
"@local" = []
nonebot-adapter-qq = [{name = "QQ", module_name = "nonebot.adapters.qq"}]

[tool.nonebot.plugins]
"@local" = []
EOF

    # 设置时区
    ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
    show_status "设置系统时区为 Asia/Shanghai" "success"
fi

# 获取代理选择
get_proxy_choice

# 如果是更新模式，先进行备份和旧配置提取
if [[ "$ACTION" == "update" ]]; then
    backup_before_update || ui_print "yellow" "警告: 更新前备份可能不完整。"
    # 只有在配置文件存在时才尝试提取旧配置
    if [ -f "$XIUXIAN_CONFIG_PATH_ABS" ]; then
        extract_old_config_values || ui_print "yellow" "警告: 旧配置提取失败，可能无法完全恢复配置。"
    else
        ui_print "yellow" "警告: xiuxian_config.py 不存在，跳过旧配置提取。"
    fi
fi

# --- 下载和解压 Release ---
LOCAL_RELEASE_TEMP_PATH="$DIR/${REPO_NAME}_${RELEASE_ASSET}"
TEMP_EXTRACT_DIR="$DIR/temp_extract"

# PROXY 变量在 get_proxy_choice 中设置
if download_release_resource "https://github.com/$REPO_OWNER/$REPO_NAME/releases/$RELEASE_TAG/download/$RELEASE_ASSET" "$LOCAL_RELEASE_TEMP_PATH" "$PROXY"; then
    show_status "下载 release 资源文件" "success"
else
    show_status "下载 release 资源文件" "failure"
    exit 127
fi

if extract_release_resource "$LOCAL_RELEASE_TEMP_PATH" "$TEMP_EXTRACT_DIR"; then
    show_status "解压 release 资源文件" "success"
else
    show_status "解压 release 资源文件" "failure"
    exit 127
fi

# --- 移动文件到安装目录 (覆盖安装或更新) ---
show_progress "移动文件到安装目录"

# 完整替换插件源码，避免上游已删除的旧文件残留；用户数据仍按目录合并保留。
replace_plugin_source "$TEMP_EXTRACT_DIR/nonebot_plugin_xiuxian_2" || { show_status "替换插件文件" "failure"; exit 127; }
show_status "替换插件文件" "success"

# 移动 data 目录 (例如字体、图片等)
if [[ -d "$TEMP_EXTRACT_DIR/data" ]]; then
    ensure_dir "$DIR/data"
    cp -a "$TEMP_EXTRACT_DIR/data/." "$DIR/data/" || { show_status "移动data目录" "failure"; exit 127; }
    show_status "移动data目录" "success"
fi

# 移动 requirements.txt
if [[ -f "$TEMP_EXTRACT_DIR/requirements.txt" ]]; then
    mv "$TEMP_EXTRACT_DIR/requirements.txt" "$DIR/" || { show_status "移动requirements.txt" "failure"; exit 127; }
    show_status "移动requirements.txt" "success"
fi

# 更新 version.txt
if [[ -f "$TEMP_EXTRACT_DIR/version.txt" ]]; then
    ensure_dir "$DIR/data/xiuxian"
    mv "$TEMP_EXTRACT_DIR/version.txt" "$DIR/data/xiuxian/" || { show_status "更新版本文件" "failure"; exit 127; }
    show_status "更新版本文件" "success"
    CURRENT_PROJECT_VERSION=$(cat "$DIR/data/xiuxian/version.txt" | tr -d '[:space:]')
fi

# --- 清理临时文件 ---
rm -rf "$LOCAL_RELEASE_TEMP_PATH" "$TEMP_EXTRACT_DIR" > /dev/null 2>&1
show_status "清理临时文件" "success"

# 如果是更新模式，应用之前提取的旧配置
if [[ "$ACTION" == "update" ]]; then
    apply_old_config_values || { ui_print "red" "错误: 恢复旧配置失败，请手动检查 $XIUXIAN_CONFIG_PATH_ABS"; exit 127; }
fi

validate_deployment || { show_status "验证部署代码" "failure"; exit 127; }
show_status "验证部署代码" "success"


# --- 虚拟环境和 Bot 配置 (仅限 install 模式) ---
if [[ "$ACTION" == "install" ]]; then
    show_progress "创建 Python 虚拟环境"
    python3 -m venv "$VENV_PATH" > /dev/null 2>&1
    [ $? -eq 0 ] && show_status "创建 Python 虚拟环境" "success" || { show_status "创建 Python 虚拟环境" "failure"; exit 127; }

    # 创建启动脚本
    cat <<EOF > "/bin/${PROJECT_NAME}_start"
#!/bin/bash
export TZ=Asia/Shanghai

# 启动时自动确保 logrotate + cron 存在
if [ ! -d "$DIR/logs" ]; then
    mkdir -p "$DIR/logs"
fi
mkdir -p /etc/logrotate.d /etc/cron.d /var/lib/logrotate

# 如果 logrotate 配置不存在，则创建
if [ ! -f "/etc/logrotate.d/$PROJECT_NAME" ]; then
cat <<'LR_EOF' > "/etc/logrotate.d/$PROJECT_NAME"
"$DIR/${PROJECT_NAME}.log" {
    size 10M
    rotate 10
    compress
    missingok
    notifempty
    copytruncate
    dateext
    dateformat -%Y%m%d-%H%M%S
    olddir $DIR/logs
}
LR_EOF
fi

# 如果 cron 任务不存在，则创建
if [ ! -f "/etc/cron.d/${PROJECT_NAME}_logrotate" ]; then
cat <<'CRON_EOF' > "/etc/cron.d/${PROJECT_NAME}_logrotate"
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
*/10 * * * * root /usr/sbin/logrotate -s /var/lib/logrotate/status /etc/logrotate.d/$PROJECT_NAME >/dev/null 2>&1
CRON_EOF
chmod 644 "/etc/cron.d/${PROJECT_NAME}_logrotate"
fi

source "$VENV_PATH/bin/activate"
cd "$DIR" || exit 1
nb run --reload
EOF

    # 激活虚拟环境并配置 pip 镜像
    source "$VENV_PATH/bin/activate" > /dev/null 2>&1
    [ $? -eq 0 ] && show_status "激活 Python 虚拟环境" "success" || { show_status "激活 Python 虚拟环境" "failure"; exit 127; }

    cd "$DIR" || { show_status "进入安装目录 $DIR" "failure"; exit 127; }
    show_status "进入安装目录 $DIR" "success"

    upgrade_python_dependencies || { show_status "安装/升级 Python 依赖" "failure"; exit 127; }

    show_progress "获取用户配置信息"
    read_or SUPERUSERS "请输入主人QQ号（SUPERUSERS），多个用英文逗号分隔" "123456"
    read_or NICKNAME "请输入机器人昵称（NICKNAME），多个用英文逗号分隔" "堂堂"
    read_or PORT "请输入NoneBot监听端口号（PORT）" "8080"

    # 生成 .env 文件
    cat <<EOF > "$DIR/.env"
ENVIRONMENT=dev
DRIVER=~fastapi+~httpx+~websockets+~aiohttp
EOF

    # 生成 .env.dev 文件
    SUPERUSERS_LIST=$(echo "$SUPERUSERS" | sed -E 's/, */", "/g' | sed -E 's/^/"/' | sed -E 's/$/"/' | sed 's/"",""/","/g')
    NICKNAME_LIST=$(echo "$NICKNAME" | sed -E 's/, */", "/g' | sed -E 's/^/"/' | sed -E 's/$/"/' | sed 's/"",""/","/g')

    cat <<EOF > "$DIR/.env.dev"
LOG_LEVEL=INFO

SUPERUSERS = [$SUPERUSERS_LIST]
COMMAND_START = [""]
NICKNAME = [$NICKNAME_LIST]
DEBUG = False
HOST = 0.0.0.0
PORT = $PORT
EOF
    show_status "生成 NoneBot2 配置文件 (.env, .env.dev)" "success"

    # 创建管理脚本
    cat <<EOF > "/bin/$PROJECT_NAME"
formatlog() {
    local LOG_FILE="\$1"
    awk '{
        gsub(/\033\[[0-9;]*m/, "")
        if (/#[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}/ || /[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}/) {
            print "\n" \$0
        } else {
            print "  " \$0
        }
    }' "\$LOG_FILE" > "\$LOG_FILE.format.log"
}

if [ "\$#" -eq 0 ]; then
    set -- start
fi

case "\$1" in
    start)
        if [ ! -d "$DIR/logs" ]; then
            mkdir -p "$DIR/logs"
        fi
        mkdir -p /etc/logrotate.d /etc/cron.d /var/lib/logrotate
        if [ ! -f "/etc/logrotate.d/$PROJECT_NAME" ]; then
cat <<'LR_EOF' > "/etc/logrotate.d/$PROJECT_NAME"
"$DIR/${PROJECT_NAME}.log" {
    size 10M
    rotate 10
    compress
    missingok
    notifempty
    copytruncate
    dateext
    dateformat -%Y%m%d-%H%M%S
    olddir $DIR/logs
}
LR_EOF
        fi
        if [ ! -f "/etc/cron.d/${PROJECT_NAME}_logrotate" ]; then
cat <<'CRON_EOF' > "/etc/cron.d/${PROJECT_NAME}_logrotate"
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
*/10 * * * * root /usr/sbin/logrotate -s /var/lib/logrotate/status /etc/logrotate.d/$PROJECT_NAME >/dev/null 2>&1
CRON_EOF
            chmod 644 "/etc/cron.d/${PROJECT_NAME}_logrotate"
        fi

        if screen -list | grep -q "\b$PROJECT_NAME\b"; then
            echo "$PROJECT_NAME 已在后台运行"
        else
            echo "正在后台启动 $PROJECT_NAME..."
            screen -U -dmS "$PROJECT_NAME" -L -Logfile "$DIR/${PROJECT_NAME}.log" bash -c '${PROJECT_NAME}_start'
            echo "已后台启动，使用 '$PROJECT_NAME status' 查看日志或管理"
        fi
        ;;
    stop)
        if screen -list | grep -q "\b$PROJECT_NAME\b"; then
            screen -X -S "$PROJECT_NAME" quit
            echo "$PROJECT_NAME 已停止"
        else
            echo "$PROJECT_NAME 未在运行"
        fi
        ;;
    status)
        if screen -list | grep -q "\b$PROJECT_NAME\b"; then
            screen -U -r "$PROJECT_NAME"
        else
            echo "$PROJECT_NAME 未在运行"
        fi
        ;;
    update-deps|deps|upgrade-deps)
        PYTHON_BIN="$VENV_PATH/bin/python"
        if [ ! -x "\$PYTHON_BIN" ]; then
            echo "错误：未找到虚拟环境 Python: \$PYTHON_BIN"
            exit 1
        fi
        "\$PYTHON_BIN" -m pip config set global.index-url https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple >/dev/null 2>&1 || true
        "\$PYTHON_BIN" -m pip install -U pip || exit 1
        "\$PYTHON_BIN" -m pip install -U --upgrade-strategy eager \
            "nb-cli" \
            "nonebot2[fastapi,httpx,websockets,aiohttp]" \
            "nonebot-adapter-onebot" \
            "nonebot-adapter-qq" \
            "nonebot_plugin_apscheduler" || exit 1
        if [ -f "$DIR/requirements.txt" ]; then
            "\$PYTHON_BIN" -m pip install -U --upgrade-strategy eager -r "$DIR/requirements.txt" || exit 1
        fi
        "\$PYTHON_BIN" - <<'PY'
import importlib.util
from importlib import metadata

packages = {
    "nb-cli": "nb_cli",
    "nonebot2": "nonebot",
    "nonebot-adapter-onebot": "nonebot.adapters.onebot.v11",
    "nonebot-adapter-qq": "nonebot.adapters.qq",
    "nonebot-plugin-apscheduler": "nonebot_plugin_apscheduler",
}

def module_location(module_name: str) -> str:
    spec = importlib.util.find_spec(module_name)
    if spec is None:
        return "未找到模块"
    if spec.submodule_search_locations:
        return next(iter(spec.submodule_search_locations))
    return spec.origin or "未知路径"

print("当前核心依赖版本和安装路径：")
for package, module_name in packages.items():
    try:
        version = metadata.version(package)
    except metadata.PackageNotFoundError:
        continue
    print(f"  {package}: {version}")
    print(f"    {module_name}: {module_location(module_name)}")
PY
        ;;
    format)
        if [ -n "\$2" ]; then
            if [ -f "\$2" ]; then
                formatlog "\$2"
                echo "已输出格式化日志到: \$2.format.log"
            else
                echo "错误：日志文件不存在: \$2"
            fi
        else
            LOG_FILE="$DIR/${PROJECT_NAME}.log"
            if [ -f "\$LOG_FILE" ]; then
                formatlog "\$LOG_FILE"
                echo "已输出格式化日志到: \$LOG_FILE.format.log"
            else
                echo "错误：默认日志文件不存在: \$LOG_FILE"
            fi
        fi
        ;;
    *)
        echo "用法: $PROJECT_NAME [start|stop|status|update-deps|format [log_file]]"
        ;;
esac
EOF

    # 赋予脚本执行权限
    chmod +x "/bin/${PROJECT_NAME}_start" "/bin/$PROJECT_NAME"
    show_status "创建启动脚本 /bin/${PROJECT_NAME}_start 和管理脚本 /bin/$PROJECT_NAME" "success"

    # 在 install 模式下也设置 logrotate 和 cron
    setup_logrotate_and_cron || exit 127

else # ACTION == "update" 模式下
    upgrade_python_dependencies || show_status "更新 Python 依赖" "failure"

    if [[ ! -f "$DIR/.env.dev" ]]; then
        ui_print "yellow" "未找到 $DIR/.env.dev，将创建默认配置。"
        ensure_default_env_files || { show_status "创建默认配置文件" "failure"; exit 127; }
    fi

    # 强制覆盖 logrotate 和 cron 任务，确保最新
    setup_logrotate_and_cron || exit 127
fi


# --- 最终信息输出 ---
IPV4=$(curl -s ifconfig.me 2>/dev/null)
PORT_SHOW=$(grep -E '^PORT *= *' "$DIR/.env.dev" 2>/dev/null | sed -E 's/.*= *//')
[[ -z "$PORT_SHOW" ]] && PORT_SHOW="8080"

ui_print "green" "========================================"
ui_print "green" "✓ ${ACTION} 完成！"
ui_print "green" "项目名称: $PROJECT_NAME"
ui_print "green" "安装目录: $DIR"
ui_print "green" "日志文件:"
ui_print "white" "    当前日志: $DIR/${PROJECT_NAME}.log"
ui_print "white" "    历史日志: $DIR/logs (日志轮转后移入)"
ui_print "green" "数据库: SQLite（默认本地 data/xiuxian/*.db）"
ui_print "green" "OneBot V11 协议地址（用于连接 go-cqhttp 或其他适配器）："
ui_print "white" "    ws://${IPV4}:${PORT_SHOW}/onebot/v11/ws"
ui_print "white" "    ws://127.0.0.1:${PORT_SHOW}/onebot/v11/ws"
ui_print "green" "可用管理命令（直接在命令行输入）："
ui_print "white" "    ${PROJECT_NAME} start   -> 后台启动机器人"
ui_print "white" "    ${PROJECT_NAME} stop    -> 停止机器人"
ui_print "white" "    ${PROJECT_NAME} status  -> 查看机器人运行日志 (按 Ctrl+A+D 退出)"
ui_print "white" "    ${PROJECT_NAME} update-deps -> 更新 Python 依赖到当前索引最新版本"
ui_print "white" "    ${PROJECT_NAME} format [log_file] -> 格式化机器人日志文件，输出到 .format.log"
ui_print "green" "========================================"
