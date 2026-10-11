#!/data/data/com.termux/files/usr/bin/bash

set -u

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m'

REPO_OWNER="liyw0205"
REPO_NAME="nonebot_plugin_xiuxian_2_pmv"
RELEASE_TAG="${XIUXIAN_RELEASE_TAG:-latest}"
RELEASE_ASSET="project.tar.gz"
ACCELERATED_PROXIES=(
    "https://gh-proxy.com/"
    "https://ghfast.top/"
    "https://ghproxy.vip/"
    "https://gh-proxy.org/"
)
DEFAULT_PROJECT_NAME="xiu2"

TERMUX_HOME="${HOME:-/data/data/com.termux/files/home}"
TERMUX_PREFIX="${PREFIX:-/data/data/com.termux/files/usr}"
VENV_PATH="${VENV_PATH:-$TERMUX_HOME/myenv}"
SOURCE_MODE="release"
SOURCE_DIR=""
MIRROR_MODE="auto"
MIRROR_URL=""
INSTALL_YES=0
LOG_LINES=80

CORE_PIP_PACKAGES=(
    "nb-cli"
    "nonebot2[fastapi,httpx,websockets,aiohttp]"
    "nonebot-adapter-onebot"
    "nonebot-adapter-qq"
    "nonebot_plugin_apscheduler"
)

EXTRA_PIP_PACKAGES=(
    "wget"
    "ujson"
    "wcwidth"
    "aiohttp"
    "pydantic"
    "aiofiles"
    "flask"
    "requests"
)

ui_print() {
    local color="$1"
    shift
    case "$color" in
        red) echo -e "${RED}$*${NC}" ;;
        green) echo -e "${GREEN}$*${NC}" ;;
        yellow) echo -e "${YELLOW}$*${NC}" ;;
        blue) echo -e "${BLUE}$*${NC}" ;;
        cyan) echo -e "${CYAN}$*${NC}" ;;
        white) echo -e "${WHITE}$*${NC}" ;;
        *) echo -e "$*" ;;
    esac
}

show_status() {
    local operation="$1"
    local status="$2"
    if [[ "$status" == "success" ]]; then
        ui_print green "✓ $operation 成功"
    else
        ui_print red "✗ $operation 失败"
    fi
}

show_progress() {
    ui_print blue "正在 $1..."
}

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
    printf -v "$var_name" '%s' "$input"
}

ensure_dir() {
    mkdir -p "$1"
}

ensure_default_env_files() {
    ensure_dir "$DIR" || return 1

    if [[ ! -f "$DIR/.env" ]]; then
        cat > "$DIR/.env" <<'EOF'
ENVIRONMENT=dev
DRIVER=~fastapi+~httpx+~websockets+~aiohttp
EOF
        show_status "创建默认 .env" "success"
    fi

    if [[ ! -f "$DIR/.env.dev" ]]; then
        cat > "$DIR/.env.dev" <<'EOF'
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

detect_termux() {
    if [[ ! -d "$TERMUX_PREFIX" || ! -x "$TERMUX_PREFIX/bin/pkg" ]]; then
        ui_print red "未检测到 Termux 环境。Debian/Linux 请使用 install.sh。"
        exit 127
    fi
}

usage() {
    cat <<EOF
用法: $0 [install|update|uninstall|start|stop|restart|status|logs|login] [--target PATH] [选项]

默认安装到: $TERMUX_HOME/$DEFAULT_PROJECT_NAME
默认虚拟环境: $VENV_PATH
选项: --source-mode release|source --source PATH --mirror auto|direct|accelerated|custom
      --mirror-url URL --target PATH --venv PATH --yes
EOF
}

show_main_menu() {
    ui_print green "========================================"
    ui_print green "请选择要执行的操作"
    ui_print white "1. 安装"
    ui_print white "2. 重装（删除目标安装目录后重新安装）"
    ui_print white "3. 更新"
    ui_print white "4. 更新依赖"
    ui_print white "5. 退出"
    ui_print green "========================================"

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
            ui_print yellow "已退出。"
            exit 0
            ;;
        *)
            ui_print red "无效选择：$choice"
            exit 127
            ;;
    esac

    read_or TARGET_INPUT "请输入项目名或绝对路径" "$DEFAULT_PROJECT_NAME"
}

parse_args() {
    ACTION="install"
    TARGET_INPUT="$DEFAULT_PROJECT_NAME"

    if [[ $# -eq 0 ]]; then
        show_main_menu
        return 0
    fi

    local action_set=0
    while (($#)); do
        case "$1" in
            install|reinstall|update|update-deps|deps|upgrade-deps|uninstall|start|stop|restart|status|logs|login)
                ((action_set == 0)) || { ui_print red "只能指定一个动作"; exit 2; }
                ACTION="$1"
                action_set=1
                shift
                ;;
            --target|--directory)
                (($# >= 2)) || { ui_print red "$1 需要路径"; exit 2; }
                TARGET_INPUT="$2"
                shift 2
                ;;
            --venv)
                (($# >= 2)) || { ui_print red "--venv 需要路径"; exit 2; }
                VENV_PATH="$2"
                shift 2
                ;;
            --source-mode)
                (($# >= 2)) || { ui_print red "--source-mode 需要值"; exit 2; }
                SOURCE_MODE="$2"
                shift 2
                ;;
            --source)
                (($# >= 2)) || { ui_print red "--source 需要路径"; exit 2; }
                SOURCE_DIR="$2"
                shift 2
                ;;
            --mirror)
                (($# >= 2)) || { ui_print red "--mirror 需要值"; exit 2; }
                MIRROR_MODE="$2"
                shift 2
                ;;
            --mirror-url)
                (($# >= 2)) || { ui_print red "--mirror-url 需要 URL"; exit 2; }
                MIRROR_URL="$2"
                shift 2
                ;;
            --lines)
                (($# >= 2)) && [[ "$2" =~ ^[1-9][0-9]*$ ]] || { ui_print red "--lines 需要正整数"; exit 2; }
                LOG_LINES="$2"
                shift 2
                ;;
            --yes) INSTALL_YES=1; shift ;;
            -h|--help|help) usage; exit 0 ;;
            -*) ui_print red "未知选项: $1"; usage; exit 2 ;;
            *)
                [[ "$TARGET_INPUT" == "$DEFAULT_PROJECT_NAME" ]] || { ui_print red "只能指定一个目标目录"; exit 2; }
                TARGET_INPUT="$1"
                shift
                ;;
        esac
    done

    case "$SOURCE_MODE" in release|source) ;; *) ui_print red "--source-mode 必须是 release 或 source"; exit 2 ;; esac
    case "$MIRROR_MODE" in auto|direct|accelerated|custom) ;; *) ui_print red "无效 --mirror: $MIRROR_MODE"; exit 2 ;; esac
    if [[ "$MIRROR_MODE" == custom && -z "$MIRROR_URL" ]]; then
        ui_print red "--mirror custom 必须同时指定 --mirror-url"
        exit 2
    fi
    if [[ "$SOURCE_MODE" == source && -z "$SOURCE_DIR" ]]; then
        ui_print red "--source-mode source 必须同时指定 --source PATH"
        exit 2
    fi
}

resolve_paths() {
    if [[ "$TARGET_INPUT" == /* ]]; then
        DIR="$TARGET_INPUT"
        PROJECT_NAME="$(basename "$DIR")"
    else
        PROJECT_NAME="$TARGET_INPUT"
        DIR="$TERMUX_HOME/$PROJECT_NAME"
    fi

    if [[ -z "$PROJECT_NAME" ]]; then
        PROJECT_NAME="$DEFAULT_PROJECT_NAME"
        DIR="$TERMUX_HOME/$PROJECT_NAME"
    fi

    COMMAND_PATH="$TERMUX_PREFIX/bin/$PROJECT_NAME"
    START_PATH="$TERMUX_PREFIX/bin/${PROJECT_NAME}_start"
}

install_termux_packages() {
    show_progress "安装 Termux 依赖"
    pkg update -y || return 1
    pkg install -y \
        bash curl wget git python screen tar unzip zip coreutils sed grep findutils procps termux-api \
        clang rust binutils libffi openssl zlib libjpeg-turbo freetype libpng \
        python-numpy python-pillow python-psutil || return 1
    show_status "安装 Termux 依赖" "success"
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
    "numpy": "numpy",
    "Pillow": "PIL",
    "psutil": "psutil",
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

upgrade_python_dependencies() {
    local python_cmd="$VENV_PATH/bin/python"

    if [[ ! -x "$python_cmd" ]]; then
        ui_print red "未检测到虚拟环境 Python：$python_cmd"
        ui_print yellow "请先完成安装，或确认 VENV_PATH 指向正确虚拟环境。"
        return 1
    fi
    show_status "检测虚拟环境 $VENV_PATH" "success"

    "$python_cmd" -m pip --version >/dev/null 2>&1 || {
        ui_print red "当前 Python 未安装 pip：$python_cmd"
        return 1
    }

    "$python_cmd" -m pip config set global.index-url https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple >/dev/null 2>&1 || true
    show_status "设置 pip 镜像源为清华源" "success"

    show_progress "升级 pip"
    "$python_cmd" -m pip install -U pip || return 1
    show_status "升级 pip" "success"

    show_progress "安装/升级 NoneBot 核心、驱动、适配器和常用插件"
    "$python_cmd" -m pip install -U --upgrade-strategy eager "${CORE_PIP_PACKAGES[@]}" || return 1
    show_status "安装/升级核心依赖" "success"

    show_progress "安装/升级项目常用依赖"
    "$python_cmd" -m pip install -U --upgrade-strategy eager "${EXTRA_PIP_PACKAGES[@]}" || return 1
    show_status "安装/升级项目常用依赖" "success"

    if [[ -f "$DIR/requirements.txt" ]]; then
        show_progress "安装/升级 requirements.txt（跳过 Termux 系统包已处理项）"
        local req_tmp
        req_tmp="$(mktemp)"
        grep -Ev '^[[:space:]]*(numpy|Pillow|pillow|psutil|pathlib|asyncio)([[:space:]]|[<=>!~].*)?$' "$DIR/requirements.txt" > "$req_tmp" || true
        if [[ -s "$req_tmp" ]]; then
            "$python_cmd" -m pip install -U --upgrade-strategy eager -r "$req_tmp" || {
                rm -f "$req_tmp"
                return 1
            }
        fi
        rm -f "$req_tmp"
        show_status "安装/升级 requirements.txt" "success"
    else
        ui_print yellow "未找到 requirements.txt，跳过项目依赖更新"
    fi

    print_installed_dependency_versions "$python_cmd"
}

test_proxy_url() {
    local proxy="$1"
    local url="$2"
    local full_url="${proxy}${url}"
    local temp_file
    local cost
    local magic=""

    if ! command -v curl >/dev/null 2>&1; then
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
    else
        echo "999999"
    fi
}

select_proxy() {
    local release_url="https://github.com/$REPO_OWNER/$REPO_NAME/releases/$RELEASE_TAG/download/$RELEASE_ASSET"
    local proxies=("${ACCELERATED_PROXIES[@]}")
    local best_proxy=""
    local best_time=999999
    local cost

    case "$MIRROR_MODE" in
        direct) PROXY=""; return 0 ;;
        accelerated) PROXY="${ACCELERATED_PROXIES[0]}"; return 0 ;;
        custom) PROXY="$MIRROR_URL"; return 0 ;;
        auto) ;;
    esac

    ui_print yellow "正在自动选择可用代理，请稍候..."
    for proxy in "${proxies[@]}"; do
        cost="$(test_proxy_url "$proxy" "$release_url")"
        if [[ "$cost" =~ ^[0-9]+$ && "$cost" -lt "$best_time" ]]; then
            best_time="$cost"
            best_proxy="$proxy"
        fi
    done

    if [[ -n "$best_proxy" && "$best_time" -lt 999999 ]]; then
        PROXY="$best_proxy"
        ui_print green "✓ 自动选择代理: $PROXY 延迟约 ${best_time}ms"
    else
        PROXY=""
        ui_print yellow "未找到可用代理，使用直连下载。"
    fi
}

download_release_resource() {
    local release_url="https://github.com/$REPO_OWNER/$REPO_NAME/releases/$RELEASE_TAG/download/$RELEASE_ASSET"
    local download_path="$1"
    local urls=()

    if [[ -n "${PROXY:-}" ]]; then
        urls+=("${PROXY}${release_url}")
    fi
    urls+=("$release_url")

    show_progress "下载 release 资源文件"
    for url in "${urls[@]}"; do
        ui_print cyan "尝试下载: $url"
        if curl -fL --connect-timeout 15 --retry 2 -o "$download_path" "$url" \
            && tar -tzf "$download_path" >/dev/null 2>&1; then
            return 0
        fi
        rm -f "$download_path"
    done
    return 1
}

validate_release_reference() {
    if [[ "$RELEASE_TAG" != "latest" && ! "$RELEASE_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        ui_print red "无效 release tag：$RELEASE_TAG"
        return 1
    fi
    [[ "$RELEASE_ASSET" == "project.tar.gz" ]]
}

extract_release_resource() {
    local archive_path="$1"
    local extract_dir="$2"
    local wrapper_dir
    local top_level_dirs=()

    show_progress "解压 release 资源文件"
    rm -rf "$extract_dir"
    ensure_dir "$extract_dir"
    tar -xzf "$archive_path" -C "$extract_dir" || return 1

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

    [[ -f "$extract_dir/nonebot_plugin_xiuxian_2/__init__.py" ]] || {
        ui_print red "错误: Release 包缺少 nonebot_plugin_xiuxian_2/__init__.py，拒绝覆盖现有安装。"
        return 1
    }
}

replace_plugin_source() {
    local source_dir="$1"
    local target_dir="$DIR/src/plugins/nonebot_plugin_xiuxian_2"
    local staging_dir="$DIR/src/plugins/.nonebot_plugin_xiuxian_2.new.$$"
    local previous_dir="$DIR/src/plugins/.nonebot_plugin_xiuxian_2.old.$$"

    rm -rf "$staging_dir" "$previous_dir"
    ensure_dir "$staging_dir"
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

    [[ -f "$plugin_dir/__init__.py" ]] || return 1
    ! grep -R -n -E "(^|[[:space:]])(from|import)[[:space:]]+nonebot_plugin_xiuxian_2([[:space:].]|$)" \
        --include="*.py" "$plugin_dir" > /dev/null
}

write_pyproject() {
    cat > "$DIR/pyproject.toml" <<EOF
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
}

backup_before_update() {
    local timestamp
    timestamp="$(date +%Y%m%d_%H%M%S)"
    local backup_root="$DIR/data/xiuxian/backups"
    local backup_file="$backup_root/termux_backup_${timestamp}.zip"

    ensure_dir "$backup_root"
    if [[ -d "$DIR/src/plugins/nonebot_plugin_xiuxian_2" || -d "$DIR/data/xiuxian" ]]; then
        show_progress "更新前备份"
        (
            cd "$DIR" && zip -r -q "$backup_file" \
                "data/xiuxian" \
                "src/plugins/nonebot_plugin_xiuxian_2" \
                -x "data/xiuxian/backups/*" "*/__pycache__/*" "*.pyc" "logs/*" \
                   "data/xiuxian/message.db*" "data/xiuxian/.message.db.*.migrating" \
                   "data/xiuxian/activity/activity.db*"
        ) || ui_print yellow "警告: 更新前备份可能不完整。"
        ui_print green "备份位置: $backup_file"
    fi
}

extract_old_config_values() {
    local config_file="$DIR/src/plugins/nonebot_plugin_xiuxian_2/xiuxian/xiuxian_config.py"
    TEMP_OLD_CONFIG_DATA=""
    if [[ -f "$config_file" ]]; then
        TEMP_OLD_CONFIG_DATA="$(mktemp)"
        grep -E '^[[:space:]]+self\.[a-zA-Z0-9_]+[[:space:]]*=' "$config_file" > "$TEMP_OLD_CONFIG_DATA" || true
        show_status "提取旧 xiuxian_config.py 配置" "success"
    fi
}

apply_old_config_values() {
    local new_config_file="$DIR/src/plugins/nonebot_plugin_xiuxian_2/xiuxian/xiuxian_config.py"
    if [[ -n "${TEMP_OLD_CONFIG_DATA:-}" && -f "$TEMP_OLD_CONFIG_DATA" && -f "$new_config_file" ]]; then
        show_progress "恢复旧 xiuxian_config.py 配置"
        while IFS= read -r old_config_line; do
            local var_name
            local var_value_raw
            local var_value
            local escaped_var_value
            var_name="$(echo "$old_config_line" | sed -E 's/^[[:space:]]*self\.([a-zA-Z0-9_]+)[[:space:]]*=.*/\1/')"
            var_value_raw="$(echo "$old_config_line" | sed -E 's/^[[:space:]]*self\.[a-zA-Z0-9_]+[[:space:]]*=(.*)/\1/')"
            var_value="$(echo "$var_value_raw" | sed -E 's/^[[:space:]]*//;s/[[:space:]]*$//')"
            escaped_var_value="$(echo "$var_value" | sed 's/[\/&]/\\&/g')"
            sed -i -E "s|(^[[:space:]]*self\.${var_name})[[:space:]]*=[[:space:]]*(.*)|\1 = ${escaped_var_value}|" "$new_config_file"
        done < "$TEMP_OLD_CONFIG_DATA"
        rm -f "$TEMP_OLD_CONFIG_DATA"
        show_status "恢复旧 xiuxian_config.py 配置" "success"
    fi
}

install_release_files() {
    local temp_extract_dir="$DIR/temp_extract"
    local local_release_temp_path="$DIR/${REPO_NAME}_${RELEASE_ASSET}"

    if [[ "$SOURCE_MODE" == source ]]; then
        [[ -d "$SOURCE_DIR/.git" ]] || { ui_print red "--source 必须指向 Git checkout: $SOURCE_DIR"; exit 2; }
        [[ -z "$(git -C "$SOURCE_DIR" status --porcelain --untracked-files=no)" ]] || {
            ui_print red "源码 checkout 存在未提交的跟踪文件修改，拒绝打包: $SOURCE_DIR"
            exit 2
        }
        rm -rf -- "$temp_extract_dir"
        ensure_dir "$temp_extract_dir"
        git -C "$SOURCE_DIR" archive --format=tar HEAD | tar -xf - -C "$temp_extract_dir" || {
            ui_print red "无法从源码 checkout 构建安装文件: $SOURCE_DIR"
            exit 127
        }
        [[ -f "$temp_extract_dir/nonebot_plugin_xiuxian_2/__init__.py" ]] || {
            ui_print red "源码 checkout 缺少 nonebot_plugin_xiuxian_2/__init__.py"
            exit 127
        }
        show_status "从本地 checkout 准备源码" "success"
    else
        select_proxy
        download_release_resource "$local_release_temp_path" || {
            show_status "下载 release 资源文件" "failure"
            exit 127
        }
        show_status "下载 release 资源文件" "success"

        extract_release_resource "$local_release_temp_path" "$temp_extract_dir" || {
            show_status "解压 release 资源文件" "failure"
            exit 127
        }
        show_status "解压 release 资源文件" "success"
    fi

    show_progress "移动文件到安装目录"
    replace_plugin_source "$temp_extract_dir/nonebot_plugin_xiuxian_2" || {
        show_status "替换插件文件" "failure"
        exit 127
    }
    show_status "替换插件文件" "success"

    if [[ -d "$temp_extract_dir/data" ]]; then
        ensure_dir "$DIR/data"
        cp -a "$temp_extract_dir/data/." "$DIR/data/"
        show_status "移动 data 目录" "success"
    fi

    if [[ -f "$temp_extract_dir/requirements.txt" ]]; then
        cp -f "$temp_extract_dir/requirements.txt" "$DIR/requirements.txt"
        show_status "移动 requirements.txt" "success"
    fi

    if [[ -f "$temp_extract_dir/version.txt" ]]; then
        ensure_dir "$DIR/data/xiuxian"
        cp -f "$temp_extract_dir/version.txt" "$DIR/data/xiuxian/version.txt"
        show_status "更新版本文件" "success"
    fi

    rm -rf "$local_release_temp_path" "$temp_extract_dir"
    show_status "清理临时文件" "success"
}

create_venv() {
    if [[ ! -x "$VENV_PATH/bin/python" ]]; then
        show_progress "创建 Python 虚拟环境"
        python -m venv --system-site-packages "$VENV_PATH" || return 1
        show_status "创建 Python 虚拟环境" "success"
    else
        show_status "检测已有 Python 虚拟环境" "success"
    fi
}

write_env_files() {
    show_progress "获取用户配置信息"
    read_or SUPERUSERS "请输入主人 QQ 号（SUPERUSERS），多个用英文逗号分隔" "123456"
    read_or NICKNAME "请输入机器人昵称（NICKNAME），多个用英文逗号分隔" "堂堂"
    read_or PORT "请输入 NoneBot 监听端口号（PORT）" "8080"

    local superusers_list
    local nickname_list
    superusers_list="$(echo "$SUPERUSERS" | sed -E 's/, */", "/g' | sed -E 's/^/"/' | sed -E 's/$/"/' | sed 's/"",""/","/g')"
    nickname_list="$(echo "$NICKNAME" | sed -E 's/, */", "/g' | sed -E 's/^/"/' | sed -E 's/$/"/' | sed 's/"",""/","/g')"

    cat > "$DIR/.env" <<'EOF'
ENVIRONMENT=dev
DRIVER=~fastapi+~httpx+~websockets+~aiohttp
EOF

    cat > "$DIR/.env.dev" <<EOF
LOG_LEVEL=INFO

SUPERUSERS = [$superusers_list]
COMMAND_START = [""]
NICKNAME = [$nickname_list]
DEBUG = False
HOST = 0.0.0.0
PORT = $PORT
EOF
    if [[ ! -f "$DIR/runtime.env" ]]; then
        cat > "$DIR/runtime.env" <<'EOF'
XIUXIAN_WEB_STATUS=true
XIUXIAN_WEB_PORT=5888
EOF
    fi
    show_status "生成 NoneBot2 配置文件和插件运行环境文件" "success"
}

write_command_scripts() {
    cat > "$START_PATH" <<EOF
#!$TERMUX_PREFIX/bin/bash
export TZ=Asia/Shanghai
if [[ -f "$DIR/runtime.env" ]]; then
    set -a
    source "$DIR/runtime.env"
    set +a
fi
source "$VENV_PATH/bin/activate"
cd "$DIR" || exit 1
exec nb run
EOF

    cat > "$COMMAND_PATH" <<EOF
#!$TERMUX_PREFIX/bin/bash

PROJECT_NAME="$PROJECT_NAME"
DIR="$DIR"
VENV_PATH="$VENV_PATH"
START_PATH="$START_PATH"
PROJECT_DIR="\$DIR"
VENV_DIR="\$VENV_PATH"
SOURCE_MODE=release
SOURCE_DIR=
MIRROR_MODE=auto
MIRROR_URL=
YES=0
LOG_LINES=80
FORMAT_LOG=
MIRROR_ARGS=()

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

ACTION=\${1:-start}
if [[ "\$#" -gt 0 ]]; then shift; fi
while [[ "\$#" -gt 0 ]]; do
    case "\$1" in
        --target|--directory) PROJECT_DIR=\${2:?--target 需要路径}; shift 2 ;;
        --venv) VENV_DIR=\${2:?--venv 需要路径}; shift 2 ;;
        --source-mode) SOURCE_MODE=\${2:?--source-mode 需要值}; shift 2 ;;
        --source) SOURCE_DIR=\${2:?--source 需要路径}; shift 2 ;;
        --mirror) MIRROR_MODE=\${2:?--mirror 需要值}; shift 2 ;;
        --mirror-url) MIRROR_URL=\${2:?--mirror-url 需要 URL}; shift 2 ;;
        --yes) YES=1; shift ;;
        --lines)
            LOG_LINES=\${2:?--lines 需要正整数}
            [[ "\$LOG_LINES" =~ ^[1-9][0-9]*\$ ]] || { echo "--lines 需要正整数" >&2; exit 2; }
            shift 2
            ;;
        *) FORMAT_LOG=\$1; shift ;;
    esac
done
LOG_FILE="\$PROJECT_DIR/\${PROJECT_NAME}.log"
case "\$SOURCE_MODE" in release|source) ;; *) echo "invalid --source-mode: \$SOURCE_MODE" >&2; exit 2 ;; esac
case "\$MIRROR_MODE" in auto|direct|accelerated|custom) ;; *) echo "invalid --mirror: \$MIRROR_MODE" >&2; exit 2 ;; esac
if [ "\$MIRROR_MODE" = custom ] && [ -z "\$MIRROR_URL" ]; then
    echo "--mirror custom requires --mirror-url" >&2
    exit 2
fi
[ "\$MIRROR_MODE" != custom ] || MIRROR_ARGS+=(--mirror-url "\$MIRROR_URL")

run_installer() {
    local action="\$1" installer temporary result
    shift
    if [ "\$SOURCE_MODE" = source ]; then
        [ -n "\$SOURCE_DIR" ] || { echo "--source-mode source requires --source PATH" >&2; return 2; }
        installer="\$SOURCE_DIR/scripts/install_termux.sh"
        [ -f "\$installer" ] || { echo "installer not found: \$installer" >&2; return 2; }
        bash "\$installer" "\$action" --target "\$PROJECT_DIR" --venv "\$VENV_DIR" \\
            --source-mode source --source "\$SOURCE_DIR" --mirror "\$MIRROR_MODE" "\${MIRROR_ARGS[@]}" "\$@"
    else
        temporary=\$(mktemp)
        if ! curl -fsSL "https://raw.githubusercontent.com/$REPO_OWNER/$REPO_NAME/main/scripts/install_termux.sh" -o "\$temporary"; then
            rm -f "\$temporary"
            echo "无法获取官方 Termux 安装器" >&2
            return 1
        fi
        bash "\$temporary" "\$action" --target "\$PROJECT_DIR" --venv "\$VENV_DIR" \\
            --source-mode release --mirror "\$MIRROR_MODE" "\${MIRROR_ARGS[@]}" "\$@"
        result=\$?
        rm -f "\$temporary"
        return "\$result"
    fi
}

case "\$ACTION" in
    start)
        mkdir -p "\$DIR/logs"
        if screen -list | grep -q "\\b\${PROJECT_NAME}\\b"; then
            echo "\$PROJECT_NAME 已在后台运行"
        else
            termux-wake-lock >/dev/null 2>&1 || true
            echo "正在后台启动 \$PROJECT_NAME..."
            screen -U -dmS "\$PROJECT_NAME" -L -Logfile "\$DIR/\${PROJECT_NAME}.log" "$TERMUX_PREFIX/bin/bash" "\$START_PATH"
            echo "已后台启动，使用 '\$PROJECT_NAME status' 查看日志或管理"
        fi
        ;;
    stop)
        if screen -list | grep -q "\\b\${PROJECT_NAME}\\b"; then
            screen -X -S "\$PROJECT_NAME" quit
            echo "\$PROJECT_NAME 已停止"
        else
            echo "\$PROJECT_NAME 未在运行"
        fi
        ;;
    restart)
        "\$0" stop || true
        sleep 1
        "\$0" start
        ;;
    status)
        if screen -list | grep -q "\\b\${PROJECT_NAME}\\b"; then
            echo "\$PROJECT_NAME 正在运行"
            echo "日志: \$LOG_FILE"
        else
            echo "\$PROJECT_NAME 未在运行"
        fi
        ;;
    logs)
        [[ "\$LOG_LINES" =~ ^[1-9][0-9]*\$ ]] || { echo "--lines 必须是正整数" >&2; exit 2; }
        if [[ -f "\$LOG_FILE" ]]; then tail -n "\$LOG_LINES" "\$LOG_FILE"; else echo "日志尚不存在: \$LOG_FILE"; fi
        ;;
    login)
        echo "QQ 官方机器人配置入口: http://127.0.0.1:5888/config"
        echo "请在登录后的 Web 管理面板完成扫码绑定；OneBot 登录仍由 NapCat 客户端处理。"
        ;;
    install|update|uninstall)
        if [[ "\$ACTION" == uninstall && "\$YES" != 1 ]]; then
            echo "uninstall 会删除安装目录和其中的数据；请确认备份后添加 --yes。" >&2
            exit 2
        fi
        if [[ "\$YES" == 1 ]]; then run_installer "\$ACTION" --yes; else run_installer "\$ACTION"; fi
        ;;
    update-deps|deps|upgrade-deps)
        curl -fsSL https://raw.githubusercontent.com/$REPO_OWNER/$REPO_NAME/main/scripts/install_termux.sh | bash -s -- update-deps --target "\$PROJECT_DIR" --venv "\$VENV_DIR"
        ;;
    format)
        if [[ -n "\$FORMAT_LOG" ]]; then
            if [[ -f "\$FORMAT_LOG" ]]; then
                formatlog "\$FORMAT_LOG"
                echo "已输出格式化日志到: \$FORMAT_LOG.format.log"
            else
                echo "错误：日志文件不存在: \$FORMAT_LOG"
            fi
        else
            if [[ -f "\$LOG_FILE" ]]; then
                formatlog "\$LOG_FILE"
                echo "已输出格式化日志到: \$LOG_FILE.format.log"
            else
                echo "错误：默认日志文件不存在: \$LOG_FILE"
            fi
        fi
        ;;
    *)
        echo "用法: \$PROJECT_NAME [install|update|uninstall|start|stop|restart|status|logs|login|update-deps|format [log_file]] [--source-mode release|source] [--source PATH] [--mirror MODE] [--target PATH] [--venv PATH]"
        ;;
esac
EOF

    chmod +x "$START_PATH" "$COMMAND_PATH"
    show_status "创建管理命令 $COMMAND_PATH" "success"
}

final_message() {
    local port_show
    port_show="$(grep -E '^PORT *= *' "$DIR/.env.dev" 2>/dev/null | sed -E 's/.*= *//')"
    [[ -z "$port_show" ]] && port_show="8080"

    ui_print green "========================================"
    ui_print green "✓ ${ACTION} 完成！"
    ui_print green "项目名称: $PROJECT_NAME"
    ui_print green "安装目录: $DIR"
    ui_print green "虚拟环境: $VENV_PATH"
    ui_print green "日志文件:"
    ui_print white "    当前日志: $DIR/${PROJECT_NAME}.log"
    ui_print green "数据库: SQLite（默认本地 data/xiuxian/*.db）"
    ui_print green "OneBot V11 协议地址："
    ui_print white "    ws://127.0.0.1:${port_show}/onebot/v11/ws"
    ui_print green "可用管理命令："
    ui_print white "    ${PROJECT_NAME} start"
    ui_print white "    ${PROJECT_NAME} stop"
    ui_print white "    ${PROJECT_NAME} status"
    ui_print white "    ${PROJECT_NAME} update"
    ui_print white "    ${PROJECT_NAME} update-deps"
    ui_print white "    ${PROJECT_NAME} format [log_file]"
    ui_print green "========================================"
}

main() {
    detect_termux
    parse_args "$@"
    resolve_paths

    case "$ACTION" in
        start|stop|restart|status|logs|login)
            [[ -x "$COMMAND_PATH" ]] || { ui_print red "未找到管理命令: $COMMAND_PATH"; exit 1; }
            manager_args=(--target "$DIR" --venv "$VENV_PATH" --source-mode "$SOURCE_MODE" --mirror "$MIRROR_MODE" --lines "$LOG_LINES")
            [[ -z "$SOURCE_DIR" ]] || manager_args+=(--source "$SOURCE_DIR")
            [[ "$MIRROR_MODE" != custom ]] || manager_args+=(--mirror-url "$MIRROR_URL")
            ((INSTALL_YES == 0)) || manager_args+=(--yes)
            exec "$COMMAND_PATH" "$ACTION" "${manager_args[@]}"
            ;;
        uninstall)
            [[ "$DIR" != / && "$DIR" != "$TERMUX_HOME" ]] || { ui_print red "拒绝卸载危险目录: $DIR"; exit 2; }
            [[ -d "$DIR/src/plugins/nonebot_plugin_xiuxian_2" ]] || { ui_print red "未找到修仙2安装: $DIR"; exit 1; }
            if ((INSTALL_YES == 0)); then
                [[ -t 0 ]] || { ui_print red "非交互卸载必须指定 --yes"; exit 2; }
                read_or uninstall_confirm "卸载将删除安装目录及其中的数据；输入 YES 确认" "NO"
                [[ "$uninstall_confirm" == YES ]] || { ui_print yellow "已取消卸载"; exit 0; }
            fi
            "$COMMAND_PATH" stop >/dev/null 2>&1 || true
            rm -f "$COMMAND_PATH" "$START_PATH"
            rm -rf -- "$DIR"
            ui_print green "已卸载: $DIR"
            exit 0
            ;;
    esac

    ui_print green "执行模式: $ACTION"
    ui_print green "项目名称: $PROJECT_NAME"
    ui_print green "安装目录: $DIR"
    ui_print green "虚拟环境: $VENV_PATH"

    if [[ "$ACTION" == "reinstall" ]]; then
        if [[ -d "$DIR" ]]; then
            ui_print red "重装会删除目标安装目录：$DIR"
            ui_print yellow "如需保留数据，请先手动备份 data/xiuxian。"
            read_or REINSTALL_CONFIRM "确认重装请输入 YES" "NO"
            if [[ "$REINSTALL_CONFIRM" != "YES" ]]; then
                ui_print yellow "已取消重装。"
                exit 0
            fi
            rm -rf "$DIR" || {
                show_status "删除旧安装目录 $DIR" "failure"
                exit 127
            }
            show_status "删除旧安装目录 $DIR" "success"
        fi
        ACTION="install"
    fi

    if [[ "$ACTION" == "update-deps" ]]; then
        upgrade_python_dependencies || exit 127
        final_message
        exit 0
    fi

    if [[ "$ACTION" == "install" && -d "$DIR" ]]; then
        ui_print red "安装目录已存在，请使用 update 命令或先删除旧目录：$DIR"
        exit 127
    fi

    if [[ "$ACTION" == "update" && ! -d "$DIR" ]]; then
        ui_print yellow "目录 $DIR 不存在，自动切换到 install 模式。"
        ACTION="install"
    fi

    install_termux_packages || {
        show_status "安装 Termux 依赖" "failure"
        exit 127
    }

    ensure_dir "$DIR/src/plugins"
    ensure_dir "$DIR/data/xiuxian"
    ensure_dir "$DIR/logs"

    if [[ "$ACTION" == "install" ]]; then
        write_pyproject
    else
        backup_before_update
        extract_old_config_values
    fi

    install_release_files

    if [[ "$ACTION" == "update" ]]; then
        apply_old_config_values || exit 127
    fi

    validate_deployment || {
        show_status "验证部署代码" "failure"
        exit 127
    }
    show_status "验证部署代码" "success"

    create_venv || {
        show_status "创建 Python 虚拟环境" "failure"
        exit 127
    }

    upgrade_python_dependencies || {
        show_status "安装/升级 Python 依赖" "failure"
        exit 127
    }

    if [[ "$ACTION" == "install" ]]; then
        write_env_files
    elif [[ "$ACTION" == "update" ]]; then
        if [[ ! -f "$DIR/.env.dev" ]]; then
            ui_print yellow "未找到 $DIR/.env.dev，将创建默认配置。"
            ensure_default_env_files || {
                show_status "创建默认配置文件" "failure"
                exit 127
            }
        fi
    fi

    write_command_scripts
    final_message
}

if [[ "${XIUXIAN_INSTALLER_LIBRARY_ONLY:-0}" != "1" ]]; then
    validate_release_reference || exit 2
    main "$@"
fi
