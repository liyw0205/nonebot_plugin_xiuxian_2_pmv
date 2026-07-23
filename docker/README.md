# 修仙2 Docker

预构建镜像：  
https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/releases/tag/docker-d0a3379

镜像标签：`xiuxian2:d0a3379` / `xiuxian2:latest`（amd64）

## 使用预构建镜像

Release 中因体积限制使用分片上传：

- `xiuxian2-docker-d0a3379-amd64.tar.gz.part00` ~ `part04`
- `DOCKER_IMAGE_README.md`

```bash
# 1) 下载全部分片到同一目录后合并
cat xiuxian2-docker-d0a3379-amd64.tar.gz.part* > xiuxian2-docker-d0a3379-amd64.tar.gz
docker load -i xiuxian2-docker-d0a3379-amd64.tar.gz

# 2) 配置与数据
mkdir -p config data logs
# 编辑 config/.env 与 config/.env.dev（可参考 docker/env*.example）

# 3) 启动
docker run -d --name xiuxian2 --restart unless-stopped \
  -p 8080:8080 \
  -v "$PWD/data:/app/data" \
  -v "$PWD/logs:/app/logs" \
  -v "$PWD/config/.env:/app/.env:ro" \
  -v "$PWD/config/.env.dev:/app/.env.dev:ro" \
  xiuxian2:latest
```

OneBot 反向 WS：

```text
ws://宿主机IP:8080/onebot/v11/ws
```

## 本地构建

在仓库根目录：

```bash
docker build -f docker/Dockerfile -t xiuxian2:latest .
```

也可用 `docker/docker-compose.yml`。

## 说明

- 数据请挂载 `/app/data`，日志可挂载 `/app/logs`
- 容器内 Python venv：`/opt/venv`
- 插件路径：`/app/src/plugins/nonebot_plugin_xiuxian_2`
