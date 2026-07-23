#!/usr/bin/env bash
# 修仙2 Docker 一键脚本（使用 Release 分片镜像）
# 用法:
#   curl -fsSL .../scripts/install_docker.sh | bash
#   bash install_docker.sh install [DIR]
#   bash install_docker.sh update  [DIR]
#   bash install_docker.sh start|stop|status|logs [DIR]
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

FILE_REPO_OWNER="liyw0205"
FILE_REPO_NAME="nonebot_plugin_xiuxian_2_pmv_file"
RELEASE_TAG="${XIUXIAN_DOCKER_RELEASE_TAG:-docker-d0a3379}"
# 分片前缀：xiuxian2-docker-d0a3379-amd64.tar.gz.part00 ...
ASSET_PREFIX="${XIUXIAN_DOCKER_ASSET_PREFIX:-xiuxian2-docker-d0a3379-amd64.tar.gz}"
# 兼容旧变量：若设置了完整单文件名，则取其主名作为前缀
if [[ -n "${XIUXIAN_DOCKER_ASSET:-}" ]]; then
  ASSET_PREFIX="${XIUXIAN_DOCKER_ASSET%.part*}"
fi
MERGED_NAME="${ASSET_PREFIX}"
IMAGE_TAG="${XIUXIAN_DOCKER_IMAGE:-xiuxian2:latest}"
CONTAINER_NAME="${XIUXIAN_DOCKER_NAME:-xiuxian2}"
DEFAULT_DIR="${HOME}/xiuxian2-docker"
HOST_PORT="${XIUXIAN_DOCKER_PORT:-8080}"
# 分片序号，默认 00-04（当前 Release）
PART_FROM="${XIUXIAN_DOCKER_PART_FROM:-0}"
PART_TO="${XIUXIAN_DOCKER_PART_TO:-4}"

ui() { local c=$1; shift; case $c in red) echo -e "${RED}$*${NC}" >&2;; green) echo -e "${GREEN}$*${NC}" >&2;; yellow) echo -e "${YELLOW}$*${NC}" >&2;; blue) echo -e "${BLUE}$*${NC}" >&2;; *) echo "$*" >&2;; esac; }
ok() { ui green "✓ $*"; }
fail() { ui red "✗ $*"; exit 1; }
info() { ui blue "→ $*"; }
warn() { ui yellow "! $*"; }

need_cmd() { command -v "$1" >/dev/null 2>&1 || fail "缺少命令: $1"; }

resolve_dir() {
  local d="${1:-$DEFAULT_DIR}"
  d="${d/#\~/$HOME}"
  mkdir -p "$d"
  (cd "$d" && pwd -P)
}

ensure_docker() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    ok "Docker 已就绪"
    return 0
  fi
  if ! command -v docker >/dev/null 2>&1; then
    info "安装 Docker（需要 root / sudo）"
    if command -v apt-get >/dev/null 2>&1; then
      sudo apt-get update -y
      sudo apt-get install -y docker.io
      sudo systemctl enable --now docker || true
    elif command -v yum >/dev/null 2>&1; then
      sudo yum install -y docker
      sudo systemctl enable --now docker || true
    else
      fail "请先手动安装 Docker，再重试"
    fi
  fi
  if ! docker info >/dev/null 2>&1; then
    if command -v sudo >/dev/null 2>&1; then
      sudo systemctl start docker || true
    fi
  fi
  docker info >/dev/null 2>&1 || fail "Docker 无法访问，请检查服务/权限（可将用户加入 docker 组）"
  ok "Docker 可用"
}

download_one() {
  local url="$1" dest="$2"
  local url_proxy="https://ghproxy.net/${url}"
  if command -v curl >/dev/null 2>&1; then
    if ! curl -fL --retry 3 --retry-delay 2 --connect-timeout 20 -o "$dest" "$url"; then
      warn "直连失败，尝试代理: $(basename "$dest")"
      curl -fL --retry 3 --retry-delay 2 --connect-timeout 20 -o "$dest" "$url_proxy" || return 1
    fi
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$dest" "$url" || wget -O "$dest" "$url_proxy" || return 1
  else
    fail "需要 curl 或 wget"
  fi
  [[ -s "$dest" ]] || return 1
  return 0
}

download_parts_and_merge() {
  local dir="$1"
  local merged="$dir/$MERGED_NAME"
  mkdir -p "$dir/parts"

  # 已有完整包则复用
  if [[ -f "$merged" && -s "$merged" ]]; then
    ok "已存在合并镜像包，跳过下载: $merged"
    echo "$merged"
    return 0
  fi

  info "下载分片镜像 ${ASSET_PREFIX}.part${PART_FROM}..part${PART_TO}"
  local i part_name part_path url
  for ((i=PART_FROM; i<=PART_TO; i++)); do
    part_name=$(printf '%s.part%02d' "$ASSET_PREFIX" "$i")
    part_path="$dir/parts/$part_name"
    if [[ -f "$part_path" && -s "$part_path" ]]; then
      ok "已有分片: $part_name"
      continue
    fi
    url="https://github.com/${FILE_REPO_OWNER}/${FILE_REPO_NAME}/releases/download/${RELEASE_TAG}/${part_name}"
    info "下载 $part_name"
    download_one "$url" "$part_path" || fail "下载失败: $part_name"
    ok "完成 $part_name ($(du -h "$part_path" | awk '{print $1}'))"
  done

  info "合并分片 -> $MERGED_NAME"
  : >"$merged"
  for ((i=PART_FROM; i<=PART_TO; i++)); do
    part_path=$(printf '%s/parts/%s.part%02d' "$dir" "$ASSET_PREFIX" "$i")
    [[ -f "$part_path" ]] || fail "缺少分片: $part_path"
    cat "$part_path" >>"$merged"
  done
  [[ -s "$merged" ]] || fail "合并结果为空"
  ok "合并完成: $(du -h "$merged" | awk '{print $1}')"
  echo "$merged"
}

write_default_config() {
  local dir="$1"
  mkdir -p "$dir/config" "$dir/data" "$dir/logs"
  if [[ ! -f "$dir/config/.env" ]]; then
    cat >"$dir/config/.env" <<'EOF'
ENVIRONMENT=dev
DRIVER=~fastapi+~httpx+~websockets+~aiohttp
EOF
    ok "已生成 config/.env"
  fi
  if [[ ! -f "$dir/config/.env.dev" ]]; then
    cat >"$dir/config/.env.dev" <<'EOF'
LOG_LEVEL=INFO
SUPERUSERS = ["123456"]
COMMAND_START = [""]
NICKNAME = ["修仙"]
DEBUG = false
HOST = 0.0.0.0
PORT = 8080
EOF
    ok "已生成 config/.env.dev（请修改 SUPERUSERS）"
  fi
}

load_image() {
  local tar="$1"
  info "docker load 导入镜像"
  docker load -i "$tar"
  if ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
    if docker image inspect "xiuxian2:d0a3379" >/dev/null 2>&1; then
      docker tag xiuxian2:d0a3379 "$IMAGE_TAG"
    else
      fail "导入后未找到镜像 $IMAGE_TAG"
    fi
  fi
  ok "镜像已就绪: $IMAGE_TAG"
}

container_running() {
  docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"
}

container_exists() {
  docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"
}

start_container() {
  local dir="$1"
  write_default_config "$dir"
  if container_running; then
    ok "容器已在运行: $CONTAINER_NAME"
    return 0
  fi
  if container_exists; then
    info "启动已有容器"
    docker start "$CONTAINER_NAME" >/dev/null
  else
    info "创建并启动容器"
    docker run -d --name "$CONTAINER_NAME" --restart unless-stopped \
      -p "${HOST_PORT}:8080" \
      -v "$dir/data:/app/data" \
      -v "$dir/logs:/app/logs" \
      -v "$dir/config/.env:/app/.env:ro" \
      -v "$dir/config/.env.dev:/app/.env.dev:ro" \
      -e TZ=Asia/Shanghai \
      "$IMAGE_TAG" >/dev/null
  fi
  ok "已启动 $CONTAINER_NAME"
  echo
  ui green "NapCat / OneBot 请连接:"
  echo "  ws://宿主机IP:${HOST_PORT}/onebot/v11/ws"
  echo
  ui yellow "请编辑: $dir/config/.env.dev 中的 SUPERUSERS"
}

stop_container() {
  if container_running; then
    docker stop "$CONTAINER_NAME" >/dev/null
    ok "已停止 $CONTAINER_NAME"
  else
    warn "容器未运行"
  fi
}

status_container() {
  if container_exists; then
    docker ps -a --filter "name=^/${CONTAINER_NAME}$" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
  else
    warn "容器不存在"
  fi
}

cmd_install() {
  local dir tar
  dir="$(resolve_dir "${1:-}")"
  info "安装目录: $dir"
  ensure_docker
  need_cmd docker
  write_default_config "$dir"
  # 进度信息走 stderr；stdout 只返回 tar 路径
  tar="$(download_parts_and_merge "$dir" | tail -n 1)"
  [[ -f "$tar" ]] || fail "合并镜像包不存在: $tar"
  load_image "$tar"
  start_container "$dir"
  ok "安装完成"
}

cmd_update() {
  local dir tar
  dir="$(resolve_dir "${1:-}")"
  ensure_docker
  # 强制重新下载分片
  rm -f "$dir/$MERGED_NAME"
  rm -rf "$dir/parts"
  tar="$(download_parts_and_merge "$dir" | tail -n 1)"
  [[ -f "$tar" ]] || fail "合并镜像包不存在: $tar"
  load_image "$tar"
  if container_exists; then
    info "重建容器以使用新镜像"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
  start_container "$dir"
  ok "更新完成"
}

usage() {
  cat <<EOF
用法: $(basename "$0") <命令> [目录]

命令:
  install [DIR]   下载分片镜像、合并、导入并启动（默认）
  update  [DIR]   重新下载分片并重建容器
  start   [DIR]   启动容器
  stop            停止容器
  status          查看容器状态
  logs            查看日志
  help            帮助

环境变量:
  XIUXIAN_DOCKER_RELEASE_TAG   Release 标签（默认 docker-d0a3379）
  XIUXIAN_DOCKER_ASSET_PREFIX  分片前缀（默认 xiuxian2-docker-d0a3379-amd64.tar.gz）
  XIUXIAN_DOCKER_PART_FROM     起始分片号（默认 0）
  XIUXIAN_DOCKER_PART_TO       结束分片号（默认 4）
  XIUXIAN_DOCKER_IMAGE         镜像标签（默认 xiuxian2:latest）
  XIUXIAN_DOCKER_NAME          容器名（默认 xiuxian2）
  XIUXIAN_DOCKER_PORT          宿主机端口（默认 8080）

默认目录: $DEFAULT_DIR
Release: https://github.com/${FILE_REPO_OWNER}/${FILE_REPO_NAME}/releases/tag/${RELEASE_TAG}
EOF
}

main() {
  local cmd="${1:-install}"
  shift || true
  case "$cmd" in
    install|"") cmd_install "${1:-}" ;;
    update) cmd_update "${1:-}" ;;
    start)
      dir="$(resolve_dir "${1:-}")"
      ensure_docker
      start_container "$dir"
      ;;
    stop) ensure_docker; stop_container ;;
    status) ensure_docker; status_container ;;
    logs) ensure_docker; docker logs -f --tail 200 "$CONTAINER_NAME" ;;
    help|-h|--help) usage ;;
    *) usage; fail "未知命令: $cmd" ;;
  esac
}

main "$@"
