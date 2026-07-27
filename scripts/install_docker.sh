#!/usr/bin/env bash
# 修仙2 Docker 一键脚本（base 分片 + plugin 单包 + manifest md5）
# 用法:
#   bash install_docker.sh install [DIR]
#   bash install_docker.sh update [DIR]              # smart
#   bash install_docker.sh update --plugin [DIR]
#   bash install_docker.sh update --full [DIR]
#   bash install_docker.sh start|stop|status|logs [DIR]
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

FILE_REPO_OWNER="liyw0205"
FILE_REPO_NAME="nonebot_plugin_xiuxian_2_pmv_file"
RELEASE_TAG="${XIUXIAN_DOCKER_RELEASE_TAG:-docker-latest}"
IMAGE_TAG="${XIUXIAN_DOCKER_IMAGE:-xiuxian2:latest}"
BASE_IMAGE_TAG="${XIUXIAN_DOCKER_BASE_IMAGE:-xiuxian2-base:latest}"
CONTAINER_NAME="${XIUXIAN_DOCKER_NAME:-xiuxian2}"
DEFAULT_DIR="${HOME}/xiuxian2-docker"
HOST_PORT="${XIUXIAN_DOCKER_PORT:-8080}"
PLUGIN_MOUNT_REL="plugin/nonebot_plugin_xiuxian_2"

# 旧整包兼容（无 manifest 时回退）
LEGACY_ASSET_PREFIX="${XIUXIAN_DOCKER_ASSET_PREFIX:-xiuxian2-docker-latest-amd64.tar.gz}"
LEGACY_PART_FROM="${XIUXIAN_DOCKER_PART_FROM:-0}"
LEGACY_PART_TO="${XIUXIAN_DOCKER_PART_TO:-5}"

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
  docker info >/dev/null 2>&1 || fail "Docker 无法访问，请检查服务/权限"
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

file_md5() {
  md5sum "$1" 2>/dev/null | awk '{print $1}'
}

verify_md5() {
  local file="$1" expect="$2"
  [[ -n "$expect" ]] || return 0
  local got
  got="$(file_md5 "$file")"
  [[ "$got" == "$expect" ]] || return 1
  return 0
}

release_url() {
  local name="$1"
  echo "https://github.com/${FILE_REPO_OWNER}/${FILE_REPO_NAME}/releases/download/${RELEASE_TAG}/${name}"
}

state_path() { echo "$1/state.json"; }

read_state() {
  local dir="$1" key="$2"
  python3 - "$dir" "$key" <<'PY'
import json,sys
from pathlib import Path
p=Path(sys.argv[1])/"state.json"
key=sys.argv[2]
if not p.exists():
    print("")
    raise SystemExit
try:
    d=json.loads(p.read_text(encoding="utf-8"))
except Exception:
    print("")
    raise SystemExit
print(d.get(key,""))
PY
}

write_state() {
  local dir="$1"
  shift
  python3 - "$dir" "$@" <<'PY'
import json,sys
from pathlib import Path
from datetime import datetime
dirp=Path(sys.argv[1])
pairs=sys.argv[2:]
p=dirp/"state.json"
data={}
if p.exists():
    try: data=json.loads(p.read_text(encoding="utf-8"))
    except Exception: data={}
it=iter(pairs)
for k in it:
    v=next(it, "")
    data[k]=v
data["updated_at"]=datetime.now().astimezone().isoformat(timespec="seconds")
p.write_text(json.dumps(data, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
PY
}

fetch_manifest() {
  local dir="$1"
  local mf="$dir/manifest.json"
  local url
  url="$(release_url manifest.json)"
  info "下载 manifest.json"
  if download_one "$url" "$mf"; then
    python3 - "$mf" <<'PY'
import json,sys
from pathlib import Path
p=Path(sys.argv[1])
d=json.loads(p.read_text(encoding="utf-8"))
assert int(d.get("version",0)) >= 2
assert d.get("format")=="base+plugin"
assert "base" in d and "plugin" in d
print("ok")
PY
    ok "manifest 有效"
    echo "$mf"
    return 0
  fi
  warn "未找到新版 manifest，将尝试旧整包分片"
  return 1
}

download_base_from_manifest() {
  local dir="$1" mf="$2" force="${3:-0}"
  python3 - "$dir" "$mf" "$force" <<'PY' || exit 1
import json, hashlib, subprocess, sys
from pathlib import Path

dirp=Path(sys.argv[1]); mf=Path(sys.argv[2]); force=sys.argv[3]=="1"
man=json.loads(mf.read_text(encoding="utf-8"))
base=man["base"]
parts_dir=dirp/"parts"
parts_dir.mkdir(parents=True, exist_ok=True)
merged=dirp/base["name"]

def md5(p: Path)->str:
    h=hashlib.md5()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def sh(cmd):
    print("→", " ".join(cmd), file=sys.stderr)
    r=subprocess.run(cmd, check=False)
    if r.returncode!=0:
        raise SystemExit(r.returncode)

owner="liyw0205"; repo="nonebot_plugin_xiuxian_2_pmv_file"; tag=man.get("release_tag") or "docker-latest"
# tag fixed by env externally; URL constructed in bash helper via download_one — call curl here
import os
tag=os.environ.get("XIUXIAN_DOCKER_RELEASE_TAG","docker-latest")

def download(name, dest: Path):
    url=f"https://github.com/{owner}/{repo}/releases/download/{tag}/{name}"
    proxy="https://ghproxy.net/"+url
    for u in (url, proxy):
        r=subprocess.run(["curl","-fL","--retry","3","--retry-delay","2","--connect-timeout","20","-o",str(dest),u])
        if r.returncode==0 and dest.exists() and dest.stat().st_size>0:
            return
    raise SystemExit(f"download failed: {name}")

# skip whole base if merged ok and not force
if merged.exists() and not force and md5(merged)==base.get("md5"):
    print(f"✓ base 整包 md5 命中，跳过: {merged.name}", file=sys.stderr)
    print(merged)
    raise SystemExit(0)

need_merge=False
for part in base.get("parts") or []:
    name=part["name"]; expect=part.get("md5",""); path=parts_dir/name
    if path.exists() and expect and md5(path)==expect and not force:
        print(f"✓ 已有分片: {name}", file=sys.stderr)
        continue
    if force and path.exists():
        path.unlink()
    print(f"→ 下载 {name}", file=sys.stderr)
    download(name, path)
    if expect and md5(path)!=expect:
        path.unlink(missing_ok=True)
        raise SystemExit(f"md5 mismatch: {name}")
    print(f"✓ {name} md5 ok", file=sys.stderr)
    need_merge=True

if force or need_merge or not merged.exists() or md5(merged)!=base.get("md5"):
    print(f"→ 合并 base -> {merged.name}", file=sys.stderr)
    with merged.open("wb") as out:
        for part in base.get("parts") or []:
            p=parts_dir/part["name"]
            assert p.exists(), p
            out.write(p.read_bytes())
    got=md5(merged)
    if base.get("md5") and got!=base["md5"]:
        raise SystemExit(f"merged md5 mismatch: got {got} expect {base['md5']}")
    print(f"✓ 合并校验通过 ({merged.stat().st_size} bytes)", file=sys.stderr)
else:
    print(f"✓ base 无需重下", file=sys.stderr)
print(merged)
PY
}

download_plugin_from_manifest() {
  local dir="$1" mf="$2" force="${3:-0}"
  python3 - "$dir" "$mf" "$force" <<'PY' || exit 1
import json, hashlib, subprocess, sys, os, tarfile, shutil
from pathlib import Path
dirp=Path(sys.argv[1]); mf=Path(sys.argv[2]); force=sys.argv[3]=="1"
man=json.loads(mf.read_text(encoding="utf-8"))
pl=man["plugin"]
name=pl["name"]; expect=pl.get("md5","")
pkg=dirp/name
plugin_root=dirp/"plugin"
target=plugin_root/"nonebot_plugin_xiuxian_2"
tag=os.environ.get("XIUXIAN_DOCKER_RELEASE_TAG","docker-latest")
owner="liyw0205"; repo="nonebot_plugin_xiuxian_2_pmv_file"

def md5(p: Path)->str:
    h=hashlib.md5()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def download(name, dest: Path):
    url=f"https://github.com/{owner}/{repo}/releases/download/{tag}/{name}"
    proxy="https://ghproxy.net/"+url
    for u in (url, proxy):
        r=subprocess.run(["curl","-fL","--retry","3","--retry-delay","2","--connect-timeout","20","-o",str(dest),u])
        if r.returncode==0 and dest.exists() and dest.stat().st_size>0:
            return
    raise SystemExit(f"download failed: {name}")

if pkg.exists() and expect and md5(pkg)==expect and target.exists() and not force:
    print(f"✓ plugin 包与目录已是最新: {name}", file=sys.stderr)
    print(target)
    raise SystemExit(0)

if force and pkg.exists():
    pkg.unlink()
if not pkg.exists() or (expect and md5(pkg)!=expect):
    print(f"→ 下载 {name}", file=sys.stderr)
    download(name, pkg)
if expect and md5(pkg)!=expect:
    raise SystemExit(f"plugin md5 mismatch")
print(f"✓ plugin md5 ok ({pkg.stat().st_size} bytes)", file=sys.stderr)

# extract
tmp=dirp/"_plugin_extract"
if tmp.exists(): shutil.rmtree(tmp)
tmp.mkdir(parents=True)
with tarfile.open(pkg, "r:gz") as tf:
    tf.extractall(tmp)
src=tmp/"nonebot_plugin_xiuxian_2"
if not src.exists():
    # allow tarball root = package contents
    cands=list(tmp.iterdir())
    if len(cands)==1 and cands[0].is_dir():
        src=cands[0]
    else:
        raise SystemExit("plugin tarball missing nonebot_plugin_xiuxian_2/")
plugin_root.mkdir(parents=True, exist_ok=True)
if target.exists():
    shutil.rmtree(target)
shutil.move(str(src), str(target))
shutil.rmtree(tmp, ignore_errors=True)
# version stamp for web UpdateManager data path is separate; stamp plugin
(target/"VERSION").write_text(str(pl.get("version",""))+"\n", encoding="utf-8")
print(f"✓ 插件已解压: {target}", file=sys.stderr)
print(target)
PY
}

load_base_image() {
  local tar="$1"
  info "docker load 导入 base 镜像"
  docker load -i "$tar"
  if docker image inspect "$BASE_IMAGE_TAG" >/dev/null 2>&1; then
    docker tag "$BASE_IMAGE_TAG" "$IMAGE_TAG" || true
  fi
  if ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
    if docker image inspect "xiuxian2:latest" >/dev/null 2>&1; then
      docker tag xiuxian2:latest "$IMAGE_TAG"
    elif docker image inspect "xiuxian2-base:latest" >/dev/null 2>&1; then
      docker tag xiuxian2-base:latest "$IMAGE_TAG"
    else
      fail "导入后未找到镜像 $IMAGE_TAG / $BASE_IMAGE_TAG"
    fi
  fi
  ok "镜像已就绪: $IMAGE_TAG"
}

write_default_config() {
  local dir="$1"
  mkdir -p "$dir/config" "$dir/data" "$dir/logs" "$dir/plugin"
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

container_running() { docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; }
container_exists() { docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; }

start_container() {
  local dir="$1"
  write_default_config "$dir"
  local plugin_path="$dir/$PLUGIN_MOUNT_REL"
  [[ -d "$plugin_path" ]] || fail "插件目录不存在: $plugin_path（请先 install/update）"

  if container_running; then
    ok "容器已在运行: $CONTAINER_NAME"
    return 0
  fi
  if container_exists; then
    info "启动已有容器"
    docker start "$CONTAINER_NAME" >/dev/null
  else
    info "创建并启动容器（挂载插件目录，Web 更新可写）"
    docker run -d --name "$CONTAINER_NAME" --restart unless-stopped \
      -p "${HOST_PORT}:8080" \
      -v "$dir/data:/app/data" \
      -v "$dir/logs:/app/logs" \
      -v "$dir/config/.env:/app/.env:ro" \
      -v "$dir/config/.env.dev:/app/.env.dev:ro" \
      -v "$plugin_path:/app/src/plugins/nonebot_plugin_xiuxian_2" \
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

recreate_container() {
  local dir="$1"
  if container_exists; then
    info "重建容器以应用镜像/挂载"
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  fi
  start_container "$dir"
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

# -------- legacy fallback --------
legacy_download_merge() {
  local dir="$1"
  local merged="$dir/$LEGACY_ASSET_PREFIX"
  mkdir -p "$dir/parts"
  if [[ -f "$merged" && -s "$merged" ]]; then
    ok "已存在旧整包: $merged"
    echo "$merged"; return 0
  fi
  local i part_name part_path url
  for ((i=LEGACY_PART_FROM; i<=LEGACY_PART_TO; i++)); do
    part_name=$(printf '%s.part%02d' "$LEGACY_ASSET_PREFIX" "$i")
    part_path="$dir/parts/$part_name"
    if [[ -f "$part_path" && -s "$part_path" ]]; then
      ok "已有分片: $part_name"; continue
    fi
    url="$(release_url "$part_name")"
    info "下载 $part_name"
    download_one "$url" "$part_path" || fail "下载失败: $part_name"
  done
  info "合并旧整包"
  : >"$merged"
  for ((i=LEGACY_PART_FROM; i<=LEGACY_PART_TO; i++)); do
    part_path=$(printf '%s/parts/%s.part%02d' "$dir" "$LEGACY_ASSET_PREFIX" "$i")
    cat "$part_path" >>"$merged"
  done
  echo "$merged"
}

apply_from_manifest() {
  local dir="$1" mode="$2" # full|plugin|smart
  local mf base_tar
  export XIUXIAN_DOCKER_RELEASE_TAG="$RELEASE_TAG"
  if ! mf="$(fetch_manifest "$dir")"; then
    warn "回退旧整包流程"
    local tar
    tar="$(legacy_download_merge "$dir" | tail -n 1)"
    load_base_image "$tar"
    # 旧镜像自带插件；仍创建空挂载点避免路径缺失——若镜像内已有插件，挂载会覆盖
    mkdir -p "$dir/$PLUGIN_MOUNT_REL"
    if [[ ! -f "$dir/$PLUGIN_MOUNT_REL/__init__.py" ]]; then
      info "从镜像复制内置插件到挂载目录"
      local cid
      cid="$(docker create "$IMAGE_TAG")"
      docker cp "$cid:/app/src/plugins/nonebot_plugin_xiuxian_2/." "$dir/$PLUGIN_MOUNT_REL/" || true
      docker rm "$cid" >/dev/null
    fi
    recreate_container "$dir"
    return 0
  fi

  local base_md5 plugin_md5 req_md5 local_base local_plugin local_req
  base_md5="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["base"]["md5"])' "$mf")"
  plugin_md5="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["plugin"]["md5"])' "$mf")"
  req_md5="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["base"].get("requirements_md5",""))' "$mf")"
  local_base="$(read_state "$dir" base_md5)"
  local_plugin="$(read_state "$dir" plugin_md5)"
  local_req="$(read_state "$dir" requirements_md5)"

  local need_base=0 need_plugin=0
  case "$mode" in
    full) need_base=1; need_plugin=1 ;;
    plugin) need_plugin=1 ;;
    smart|*)
      if [[ "$local_base" != "$base_md5" || "$local_req" != "$req_md5" ]]; then need_base=1; fi
      if [[ "$local_plugin" != "$plugin_md5" ]]; then need_plugin=1; fi
      # 首次无插件目录
      if [[ ! -d "$dir/$PLUGIN_MOUNT_REL" ]]; then need_plugin=1; fi
      if ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then need_base=1; fi
      ;;
  esac

  if [[ $need_base -eq 0 && $need_plugin -eq 0 ]]; then
    ok "base/plugin 均已是最新，仅确保容器运行"
    start_container "$dir"
    return 0
  fi

  if [[ $need_base -eq 1 ]]; then
    info "更新 base 镜像"
    base_tar="$(download_base_from_manifest "$dir" "$mf" 0 | tail -n 1)"
    [[ -f "$base_tar" ]] || fail "base 包不存在"
    load_base_image "$base_tar"
    write_state "$dir" base_md5 "$base_md5" requirements_md5 "$req_md5"
  else
    ok "base 未变化，跳过"
  fi

  if [[ $need_plugin -eq 1 ]]; then
    info "更新 plugin 包"
    download_plugin_from_manifest "$dir" "$mf" 0 >/dev/null
    write_state "$dir" plugin_md5 "$plugin_md5" plugin_version "$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["plugin"].get("version",""))' "$mf")"
  else
    ok "plugin 未变化，跳过"
  fi

  # base 变了或容器挂载参数可能旧 → 重建；仅 plugin 也可 restart
  if [[ $need_base -eq 1 ]] || ! container_exists; then
    recreate_container "$dir"
  else
    if container_running; then
      info "重启容器以加载新插件"
      docker restart "$CONTAINER_NAME" >/dev/null
    else
      start_container "$dir"
    fi
  fi
  ok "完成（mode=$mode need_base=$need_base need_plugin=$need_plugin）"
}

cmd_install() {
  local dir
  dir="$(resolve_dir "${1:-}")"
  info "安装目录: $dir"
  ensure_docker
  need_cmd docker
  need_cmd curl
  need_cmd md5sum
  write_default_config "$dir"
  apply_from_manifest "$dir" full
  ok "安装完成"
}

cmd_update() {
  local mode="smart" dir_arg=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --full|full) mode="full"; shift ;;
      --plugin|plugin|--plugin-only) mode="plugin"; shift ;;
      --smart|smart) mode="smart"; shift ;;
      -*) fail "未知参数: $1" ;;
      *) dir_arg="$1"; shift ;;
    esac
  done
  local dir
  dir="$(resolve_dir "$dir_arg")"
  ensure_docker
  need_cmd docker
  need_cmd curl
  need_cmd md5sum
  write_default_config "$dir"
  info "更新模式: $mode  目录: $dir"
  apply_from_manifest "$dir" "$mode"
  ok "更新完成"
}

usage() {
  cat <<EOF
用法: $(basename "$0") <命令> [选项] [目录]

命令:
  install [DIR]              下载 base 分片 + plugin，导入并启动
  update  [DIR]              smart 更新（默认：只下变化层）
  update  --plugin [DIR]     仅更新插件包
  update  --full [DIR]       强制更新 base + plugin
  start   [DIR]              启动容器
  stop                       停止容器
  status                     查看状态
  logs                       查看日志
  help                       帮助

环境变量:
  XIUXIAN_DOCKER_RELEASE_TAG   Release 标签（默认 docker-latest）
  XIUXIAN_DOCKER_IMAGE         运行镜像标签（默认 xiuxian2:latest）
  XIUXIAN_DOCKER_BASE_IMAGE    底座镜像标签（默认 xiuxian2-base:latest）
  XIUXIAN_DOCKER_NAME          容器名（默认 xiuxian2）
  XIUXIAN_DOCKER_PORT          宿主机端口（默认 8080）

目录结构:
  DIR/parts/     base 分片缓存
  DIR/plugin/    插件目录（挂载进容器，Web 更新写这里）
  DIR/config/ data/ logs/
  DIR/manifest.json  DIR/state.json

Release: https://github.com/${FILE_REPO_OWNER}/${FILE_REPO_NAME}/releases/tag/${RELEASE_TAG}
默认目录: $DEFAULT_DIR
EOF
}

main() {
  local cmd="${1:-install}"
  shift || true
  case "$cmd" in
    install|"") cmd_install "${1:-}" ;;
    update) cmd_update "$@" ;;
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
