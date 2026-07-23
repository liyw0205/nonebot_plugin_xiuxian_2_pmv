# 修仙2 Docker 镜像（Debian + Python + nonebot + 插件）
# 数据目录 /app/data 请挂载卷，避免升级丢档。
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Shanghai \
    PATH="/opt/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
    XIUXIAN_HOME=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        python3 \
        python3-pip \
        python3-venv \
        tzdata \
        build-essential \
        libjpeg62-turbo-dev \
        zlib1g-dev \
        libffi-dev \
        libssl-dev \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

# 独立 venv，避免 Debian PEP 668 限制
RUN python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install -U pip setuptools wheel

# 核心运行时（与一键安装脚本一致：直接 pip，不走 nb-cli 适配器映射）
RUN /opt/venv/bin/pip install \
      "nb-cli" \
      "nonebot2[fastapi,httpx,websockets,aiohttp]" \
      "nonebot-adapter-onebot" \
      "nonebot-adapter-qq" \
      "nonebot_plugin_apscheduler"

# 插件本体 + 依赖（构建上下文为仓库根目录）
COPY requirements.txt /tmp/requirements.txt
RUN /opt/venv/bin/pip install -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

COPY nonebot_plugin_xiuxian_2 /app/src/plugins/nonebot_plugin_xiuxian_2

# 最小 bot 工程骨架（plugin_dirs 自动发现插件）
COPY docker/bot.py /app/bot.py
COPY docker/pyproject.toml /app/pyproject.toml
COPY docker/env.example /app/.env
COPY docker/env.dev.example /app/.env.dev
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh \
    && mkdir -p /app/data /app/logs

VOLUME ["/app/data", "/app/logs"]
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8080/ || exit 1

ENTRYPOINT ["/entrypoint.sh"]
CMD ["nb", "run"]
