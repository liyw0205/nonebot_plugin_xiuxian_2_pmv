# 修仙2 Docker

预构建镜像（文件仓库 Release，**分片**）：  
https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/releases/tag/docker-d0a3379

- 镜像标签：`xiuxian2:d0a3379` / `xiuxian2:latest`（amd64）
- 资产：`xiuxian2-docker-d0a3379-amd64.tar.gz.part00` ~ `part04`
- 合并后：`xiuxian2-docker-d0a3379-amd64.tar.gz`

> 教程与脚本以主仓库为准：  
> https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv

## 一键安装

```bash
# 默认目录 ~/xiuxian2-docker
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh | bash

# 自定义目录
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh | bash -s -- install /root/xiuxian2-docker
```

脚本会：检测/安装 Docker → 下载全部分片 → 合并 → `docker load` → 生成配置 → 启动容器。

```bash
# 更新
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh | bash -s -- update

# 管理
bash scripts/install_docker.sh start|stop|status|logs
```

## 手动安装

```bash
# 1) 下载 part00~part04 到同一目录后合并
cat xiuxian2-docker-d0a3379-amd64.tar.gz.part* > xiuxian2-docker-d0a3379-amd64.tar.gz
docker load -i xiuxian2-docker-d0a3379-amd64.tar.gz

# 2) 配置
mkdir -p config data logs
# 参考 docker/env.example 与 docker/env.dev.example

# 3) 启动
docker run -d --name xiuxian2 --restart unless-stopped \
  -p 8080:8080 \
  -v "$PWD/data:/app/data" \
  -v "$PWD/logs:/app/logs" \
  -v "$PWD/config/.env:/app/.env:ro" \
  -v "$PWD/config/.env.dev:/app/.env.dev:ro" \
  xiuxian2:latest
```

OneBot：

```text
ws://宿主机IP:8080/onebot/v11/ws
```

## 本地构建

```bash
docker build -f docker/Dockerfile -t xiuxian2:latest .
```

也可用 `docker/docker-compose.yml`。
