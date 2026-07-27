# 修仙2 Docker

预构建资产（文件仓库 Release）：  
https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/releases/tag/docker-latest

## 资产结构（v2：base + plugin）

| 文件 | 说明 |
|------|------|
| `manifest.json` | 总清单（md5 / 版本 / 分片列表） |
| `xiuxian2-base-amd64.tar.gz.part00..N` | 底座镜像分片（系统+依赖，变更少） |
| `xiuxian2-plugin-latest.tar.gz` | **仅插件**单包（日常更新只下这个） |

运行时：

- 镜像：`xiuxian2:latest`（由 base 导入）
- 插件目录挂载到容器：`DIR/plugin/nonebot_plugin_xiuxian_2` → `/app/src/plugins/nonebot_plugin_xiuxian_2`（可写，**Web 更新可直接写插件**）

> 教程与脚本以主仓库为准：  
> https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv

## 一键安装

```bash
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh | bash

# 自定义目录
curl -fsSL https://raw.githubusercontent.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/main/scripts/install_docker.sh | bash -s -- install /root/xiuxian2-docker
```

## 更新

```bash
# smart：对比 manifest md5，base 不变则只下 plugin
bash install_docker.sh update

# 仅插件
bash install_docker.sh update --plugin

# 强制 base + plugin
bash install_docker.sh update --full
```

## 管理

```bash
bash install_docker.sh start|stop|status|logs
```

## 本地构建发布物

```bash
# 产出 manifest + base 分片 + plugin 包
bash scripts/build_docker_release.sh /tmp/xiuxian2-docker-split-release

# 上传到文件仓库 docker-latest（需 gh 登录）
# gh release upload docker-latest -R liyw0205/nonebot_plugin_xiuxian_2_pmv_file --clobber ...
```

## 手动（结构说明）

```bash
# base
cat xiuxian2-base-amd64.tar.gz.part* > xiuxian2-base-amd64.tar.gz
md5sum -c xiuxian2-base-amd64.tar.gz.md5
docker load -i xiuxian2-base-amd64.tar.gz

# plugin
mkdir -p plugin
tar -xzf xiuxian2-plugin-latest.tar.gz -C plugin

docker run -d --name xiuxian2 --restart unless-stopped \
  -p 8080:8080 \
  -v "$PWD/data:/app/data" \
  -v "$PWD/logs:/app/logs" \
  -v "$PWD/config/.env:/app/.env:ro" \
  -v "$PWD/config/.env.dev:/app/.env.dev:ro" \
  -v "$PWD/plugin/nonebot_plugin_xiuxian_2:/app/src/plugins/nonebot_plugin_xiuxian_2" \
  xiuxian2:latest
```

OneBot：`ws://宿主机IP:8080/onebot/v11/ws`
