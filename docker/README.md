# 修仙2 Docker

预构建镜像（文件仓库 Release）：  
https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/releases/tag/docker-d0a3379

镜像标签：`xiuxian2:d0a3379` / `xiuxian2:latest`（amd64）  
资产文件：`xiuxian2-docker-d0a3379-amd64.tar.gz`（单文件）

> 安装教程以主仓库为准：  
> https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv

## 一键安装

```bash
# 默认目录 ~/xiuxian2-docker
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh | bash

# 自定义目录
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh | bash -s -- install /root/xiuxian2-docker
```

脚本会：检测/安装 Docker → 下载单文件镜像 → `docker load` → 生成配置 → 启动容器。

常用：

```bash
# 更新镜像并重建容器
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh | bash -s -- update

# 启停 / 状态 / 日志
bash scripts/install_docker.sh start
bash scripts/install_docker.sh stop
bash scripts/install_docker.sh status
bash scripts/install_docker.sh logs
```

## 手动安装

```bash
# 1) 下载单文件镜像（Release 资产）
# https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/releases/tag/docker-d0a3379
docker load -i xiuxian2-docker-d0a3379-amd64.tar.gz

# 2) 配置
mkdir -p config data logs
cp docker/env.example config/.env            # 或自行创建
cp docker/env.dev.example config/.env.dev
# 编辑 config/.env.dev 中 SUPERUSERS

# 3) 启动
docker run -d --name xiuxian2 --restart unless-stopped \
  -p 8080:8080 \
  -v "$PWD/data:/app/data" \
  -v "$PWD/logs:/app/logs" \
  -v "$PWD/config/.env:/app/.env:ro" \
  -v "$PWD/config/.env.dev:/app/.env.dev:ro" \
  xiuxian2:latest
```

也可用 `docker/docker-compose.yml`。

OneBot 反向 WS：

```text
ws://宿主机IP:8080/onebot/v11/ws
```

## 本地构建

在仓库根目录：

```bash
docker build -f docker/Dockerfile -t xiuxian2:latest .
```

## 说明

- 数据挂载 `/app/data`，日志 `/app/logs`
- 容器内 venv：`/opt/venv`
- 插件路径：`/app/src/plugins/nonebot_plugin_xiuxian_2`
