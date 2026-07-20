from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import mimetypes
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Literal
from urllib.parse import urljoin, urlparse

import httpx

from ...paths import get_paths
from ..adapter_compat import MessageSegment
from ..infrastructure import runtime_metrics


MediaType = Literal["image", "audio", "video", "file"]
MediaSource = str | Path | bytes | bytearray | memoryview | BinaryIO

# 网易云 outer/url 等会 302 到 CDN；部分源站会校验 UA
_DEFAULT_DOWNLOAD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Mobile) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
    "Accept": "*/*",
}


@dataclass(frozen=True)
class MediaInput:
    source: MediaSource
    media_type: MediaType
    filename: str | None = None
    content_type: str | None = None


@dataclass(frozen=True)
class ResolvedMedia:
    media_type: MediaType
    content: bytes
    filename: str
    content_type: str
    cache_key: str
    from_cache: bool = False


class MediaResolver:
    def __init__(
        self,
        cache_dir: str | Path | None = None,
        *,
        max_bytes: int = 20 * 1024 * 1024,
        timeout: float = 20,
        cache_ttl: float = 24 * 60 * 60,
        allow_private_urls: bool = False,
        download_retries: int = 2,
    ) -> None:
        self.cache_dir = Path(cache_dir or (get_paths().cache / "media")).resolve()
        self.max_bytes = int(max_bytes)
        self.timeout = float(timeout)
        self.cache_ttl = float(cache_ttl)
        self.allow_private_urls = allow_private_urls
        self.download_retries = int(download_retries)
        if (
            self.max_bytes <= 0
            or self.timeout <= 0
            or self.cache_ttl < 0
            or self.download_retries < 0
        ):
            raise ValueError("媒体大小、超时和缓存 TTL 配置不合法")

    async def resolve(self, media: MediaInput) -> ResolvedMedia:
        self._validate_type(media.media_type)
        source = media.source
        if isinstance(source, str) and source.startswith(("http://", "https://")):
            content, inferred_name, inferred_type = await self._download(source)
        elif isinstance(source, Path) or isinstance(source, str):
            path = Path(source).expanduser().resolve()
            stat = await asyncio.to_thread(path.stat)
            if stat.st_size > self.max_bytes:
                raise ValueError(f"媒体文件超过大小限制: {stat.st_size} > {self.max_bytes}")
            content = await asyncio.to_thread(path.read_bytes)
            inferred_name = path.name
            inferred_type = mimetypes.guess_type(path.name)[0]
        elif isinstance(source, (bytes, bytearray, memoryview)):
            content = bytes(source)
            inferred_name = "media.bin"
            inferred_type = None
        elif hasattr(source, "read"):
            content = await asyncio.to_thread(self._read_stream, source)
            inferred_name = str(getattr(source, "name", "media.bin") or "media.bin")
            inferred_type = mimetypes.guess_type(inferred_name)[0]
        else:
            raise TypeError(f"不支持的媒体输入: {type(source)!r}")

        self._validate_size(content)
        cache_key = hashlib.sha256(content).hexdigest()
        cached = await asyncio.to_thread(self._load_cache, cache_key)
        from_cache = cached is not None
        if cached is None:
            await asyncio.to_thread(self._store_cache, cache_key, content)
        else:
            content = cached
            runtime_metrics.increment("media.cache.hit")
        return ResolvedMedia(
            media_type=media.media_type,
            content=content,
            filename=Path(media.filename or inferred_name).name,
            content_type=media.content_type or inferred_type or "application/octet-stream",
            cache_key=cache_key,
            from_cache=from_cache,
        )

    async def build_segment(self, bot: Any, media: MediaInput):
        value = (await self.resolve(media)).content
        builder = getattr(MessageSegment, media.media_type)
        return builder(bot, value)

    async def cleanup(self) -> int:
        return await asyncio.to_thread(self._cleanup_sync)

    async def _download(self, url: str) -> tuple[bytes, str, str | None]:
        await self._validate_public_url(url)
        for attempt in range(self.download_retries + 1):
            try:
                return await self._download_once(url)
            except (httpx.TimeoutException, httpx.TransportError):
                if attempt >= self.download_retries:
                    raise
                runtime_metrics.increment("media.download.retry")
                await asyncio.sleep(0)
        raise RuntimeError("媒体下载重试流程异常结束")

    async def _download_once(self, url: str) -> tuple[bytes, str, str | None]:
        """下载远程媒体；手动跟随重定向并对每一跳做公网校验（防 SSRF）。

        网易云 ``music.163.com/song/media/outer/url`` 常返回 302 到 CDN。
        旧逻辑 ``follow_redirects=False`` 会直接抛 RedirectResponse / 302。
        """
        max_redirects = 8
        current = str(url).strip()
        headers = dict(_DEFAULT_DOWNLOAD_HEADERS)
        # 网易 outer 链接带 Referer 更稳
        host = (urlparse(current).hostname or "").lower()
        if host.endswith("music.163.com") or host.endswith("126.net"):
            headers["Referer"] = "https://music.163.com/"

        async with httpx.AsyncClient(follow_redirects=False, timeout=self.timeout) as client:
            for hop in range(max_redirects + 1):
                await self._validate_public_url(current)
                async with client.stream("GET", current, headers=headers) as response:
                    # 显式处理 3xx：校验 Location 后再跳，避免 httpx 自动跟到内网
                    if response.status_code in {301, 302, 303, 307, 308}:
                        location = (response.headers.get("location") or "").strip()
                        if not location:
                            raise ValueError(f"媒体重定向缺少 Location: {current}")
                        nxt = urljoin(current, location)
                        if hop >= max_redirects:
                            raise ValueError(f"媒体重定向次数过多: {url}")
                        runtime_metrics.increment("media.download.redirect")
                        current = nxt
                        # 更新 CDN 域 Referer
                        nhost = (urlparse(current).hostname or "").lower()
                        if nhost.endswith("music.163.com") or nhost.endswith("126.net"):
                            headers["Referer"] = "https://music.163.com/"
                        continue

                    response.raise_for_status()
                    length = int(response.headers.get("content-length", 0) or 0)
                    if length > self.max_bytes:
                        raise ValueError(f"远程媒体超过大小限制: {length} > {self.max_bytes}")
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        content.extend(chunk)
                        if len(content) > self.max_bytes:
                            raise ValueError("远程媒体下载过程中超过大小限制")
                    path_name = Path(urlparse(current).path).name or "media.bin"
                    # 去掉查询串伪扩展名：id=xxx.mp3 路径可能无 .mp3
                    if "." not in path_name and "mp3" in (response.headers.get("content-type") or "").lower():
                        path_name = "audio.mp3"
                    content_type = response.headers.get("content-type", "").split(";", 1)[0] or None
                    return bytes(content), path_name, content_type

        raise RuntimeError("媒体下载重定向流程异常结束")

    async def _validate_public_url(self, url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("媒体 URL 必须是有效的 HTTP(S) 地址")
        if parsed.username or parsed.password:
            raise ValueError("媒体 URL 不允许携带认证信息")
        if self.allow_private_urls:
            return
        addresses = await asyncio.to_thread(
            socket.getaddrinfo,
            parsed.hostname,
            parsed.port or (443 if parsed.scheme == "https" else 80),
            type=socket.SOCK_STREAM,
        )
        for address in addresses:
            ip = ipaddress.ip_address(address[4][0])
            if not ip.is_global:
                raise ValueError(f"媒体 URL 不允许访问非公网地址: {ip}")

    def _read_stream(self, stream: BinaryIO) -> bytes:
        data = stream.read(self.max_bytes + 1)
        if isinstance(data, str):
            raise TypeError("媒体 stream 必须返回 bytes")
        return bytes(data)

    def _cache_path(self, cache_key: str) -> Path:
        return self.cache_dir / cache_key[:2] / cache_key

    def _load_cache(self, cache_key: str) -> bytes | None:
        path = self._cache_path(cache_key)
        if not path.is_file():
            runtime_metrics.increment("media.cache.miss")
            return None
        if self.cache_ttl and time.time() - path.stat().st_mtime > self.cache_ttl:
            path.unlink(missing_ok=True)
            runtime_metrics.increment("media.cache.expired")
            return None
        return path.read_bytes()

    def _store_cache(self, cache_key: str, content: bytes) -> None:
        path = self._cache_path(cache_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        temp.write_bytes(content)
        temp.replace(path)

    def _cleanup_sync(self) -> int:
        if not self.cache_dir.is_dir():
            return 0
        removed = 0
        threshold = time.time() - self.cache_ttl
        for path in self.cache_dir.glob("*/*"):
            if path.is_file() and (self.cache_ttl == 0 or path.stat().st_mtime < threshold):
                path.unlink(missing_ok=True)
                removed += 1
        runtime_metrics.increment("media.cache.cleaned", removed)
        return removed

    def _validate_size(self, content: bytes) -> None:
        if len(content) > self.max_bytes:
            raise ValueError(f"媒体内容超过大小限制: {len(content)} > {self.max_bytes}")

    @staticmethod
    def _validate_type(media_type: str) -> None:
        if media_type not in {"image", "audio", "video", "file"}:
            raise ValueError(f"不支持的媒体类型: {media_type}")


media_resolver = MediaResolver()


__all__ = ["MediaInput", "MediaResolver", "MediaType", "ResolvedMedia", "media_resolver"]
