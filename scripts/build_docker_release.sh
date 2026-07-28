#!/usr/bin/env bash
# 构建 base 分片 + plugin 单包 + manifest（输出到目录，不自动上传）
# 用法:
#   XIUXIAN_DOCKER_REQUIRES_BASE_BUMP=true bash scripts/build_docker_release.sh [OUT_DIR]
# 当 requirements.txt 变更时必须设置 true，manifest 会要求 full 更新。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-/tmp/xiuxian2-docker-split-release}"
PART_SIZE="${XIUXIAN_DOCKER_PART_SIZE:-95M}"
BASE_IMAGE_TAG="${XIUXIAN_DOCKER_BASE_TAG:-xiuxian2-base:latest}"
RUNTIME_IMAGE_TAG="${XIUXIAN_DOCKER_IMAGE:-xiuxian2:latest}"
PLUGIN_NAME="xiuxian2-plugin-latest.tar.gz"
BASE_NAME="xiuxian2-base-amd64.tar.gz"
REQUIRES_BASE_BUMP="${XIUXIAN_DOCKER_REQUIRES_BASE_BUMP:-false}"
case "${REQUIRES_BASE_BUMP,,}" in
  1|true|yes|on) REQUIRES_BASE_BUMP=True ;;
  0|false|no|off) REQUIRES_BASE_BUMP=False ;;
  *) echo "XIUXIAN_DOCKER_REQUIRES_BASE_BUMP must be true/false" >&2; exit 2 ;;
esac

cd "$ROOT"
SHA="$(git rev-parse --short=7 HEAD 2>/dev/null || echo unknown)"
REQ_HASH="$(md5sum requirements.txt | awk '{print $1}')"
mkdir -p "$OUT/parts"
rm -f "$OUT"/parts/* "$OUT"/*.tar.gz "$OUT"/manifest.json "$OUT"/*.md5 2>/dev/null || true

echo "==> build base image: $BASE_IMAGE_TAG"
DOCKER_BUILDKIT=1 docker build --network=host \
  -f docker/Dockerfile.base \
  -t "$BASE_IMAGE_TAG" \
  -t "xiuxian2-base:${SHA}" \
  .

# 运行标签：base 也可直接当 runtime（挂载插件后）
docker tag "$BASE_IMAGE_TAG" "$RUNTIME_IMAGE_TAG"

echo "==> export base image"
BASE_TAR="$OUT/$BASE_NAME"
docker save "$BASE_IMAGE_TAG" "$RUNTIME_IMAGE_TAG" | gzip -1 >"$BASE_TAR"
BASE_MD5="$(md5sum "$BASE_TAR" | awk '{print $1}')"
BASE_SHA256="$(sha256sum "$BASE_TAR" | awk '{print $1}')"
BASE_SIZE="$(stat -c%s "$BASE_TAR")"
echo "$BASE_MD5  $BASE_NAME" >"$OUT/${BASE_NAME}.md5"

echo "==> split base ($PART_SIZE)"
split -b "$PART_SIZE" -d -a 2 --additional-suffix=.part "$BASE_TAR" "$OUT/parts/${BASE_NAME}."
# rename *.part -> .part00 style: split makes .00.part with additional-suffix; normalize
# With: prefix.00.part -> want prefix.part00
python3 - <<PY
from pathlib import Path
import re
parts_dir=Path("$OUT/parts")
for p in sorted(parts_dir.glob("${BASE_NAME}.*")):
    m=re.search(r"\.(\d{2})\.part$", p.name)
    if not m:
        # already partNN?
        continue
    n=m.group(1)
    dest=parts_dir/f"${BASE_NAME}.part{n}"
    p.rename(dest)
    print("part", dest.name, dest.stat().st_size)
PY

PARTS_JSON="["
first=1
for part in $(ls "$OUT/parts/${BASE_NAME}.part"* | sort); do
  bn="$(basename "$part")"
  md5="$(md5sum "$part" | awk '{print $1}')"
  sz="$(stat -c%s "$part")"
  echo "$md5  $bn" >"$OUT/parts/${bn}.md5"
  if [[ $first -eq 1 ]]; then first=0; else PARTS_JSON+=","; fi
  PARTS_JSON+=$(printf '{"name":"%s","md5":"%s","size":%s}' "$bn" "$md5" "$sz")
done
PARTS_JSON+="]"

echo "==> pack plugin"
PLUGIN_TAR="$OUT/$PLUGIN_NAME"
tar -czf "$PLUGIN_TAR" \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='.git' \
  -C "$ROOT" nonebot_plugin_xiuxian_2
# embed version marker inside tree for runtime
TMP_PLUGIN="$(mktemp -d)"
tar -xzf "$PLUGIN_TAR" -C "$TMP_PLUGIN"
printf '%s\n' "$SHA" >"$TMP_PLUGIN/nonebot_plugin_xiuxian_2/VERSION"
printf '%s\n' "$SHA" >"$TMP_PLUGIN/nonebot_plugin_xiuxian_2/data_version_hint.txt" 2>/dev/null || true
# version for UpdateManager lives in data at runtime; keep plugin VERSION file
tar -czf "$PLUGIN_TAR" -C "$TMP_PLUGIN" nonebot_plugin_xiuxian_2
rm -rf "$TMP_PLUGIN"
PLUGIN_MD5="$(md5sum "$PLUGIN_TAR" | awk '{print $1}')"
PLUGIN_SHA256="$(sha256sum "$PLUGIN_TAR" | awk '{print $1}')"
PLUGIN_SIZE="$(stat -c%s "$PLUGIN_TAR")"
echo "$PLUGIN_MD5  $PLUGIN_NAME" >"$OUT/${PLUGIN_NAME}.md5"

echo "==> write manifest"
python3 - <<PY
import json, datetime
from pathlib import Path
out=Path("$OUT")
parts=[]
for p in sorted((out/"parts").glob("${BASE_NAME}.part*")):
    if p.name.endswith(".md5"):
        continue
    md5=(out/"parts"/f"{p.name}.md5").read_text().split()[0]
    parts.append({"name": p.name, "md5": md5, "size": p.stat().st_size})
manifest={
  "version": 2,
  "format": "base+plugin",
  "updated_at": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
  "git_sha": "$SHA",
  "image": "$RUNTIME_IMAGE_TAG",
  "base": {
    "name": "$BASE_NAME",
    "tag": "$BASE_IMAGE_TAG",
    "md5": "$BASE_MD5",
    "sha256": "$BASE_SHA256",
    "size": int("$BASE_SIZE"),
    "requirements_md5": "$REQ_HASH",
    "parts": parts,
  },
  "plugin": {
    "name": "$PLUGIN_NAME",
    "version": "$SHA",
    "md5": "$PLUGIN_MD5",
    "sha256": "$PLUGIN_SHA256",
    "size": int("$PLUGIN_SIZE"),
    "install_path": "/app/src/plugins/nonebot_plugin_xiuxian_2",
    "requires_base_bump": $REQUIRES_BASE_BUMP,
  },
  "compat": {
    "min_base_requirements_md5": "$REQ_HASH",
    "requires_full_if_base_mismatch": True,
  },
}
(out/"manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
print(out/"manifest.json")
print("base_parts", len(parts), "base_mb", round(int("$BASE_SIZE")/1024/1024,1), "plugin_mb", round(int("$PLUGIN_SIZE")/1024/1024,1))
PY

echo "==> done: $OUT"
ls -lh "$OUT" "$OUT/parts" | sed -n '1,80p'
