import asyncio
import os
import time
from io import BytesIO
from pathlib import Path

from nonebot.log import logger

from ..xiuxian_config import XiuConfig
from .http_proxy import http_client

_REAL_ID_CACHE_TTL = 600
_REAL_ID_NEGATIVE_CACHE_TTL = 30
_real_id_cache: dict[str, tuple[float, str | None]] = {}


def get_real_id(id_str, timeout: float = 1.5):
    """
    调用API接口获取真实ID

    :param id_str: 要查询的ID字符串
    :return: 真实ID (str) 或 None
    """

    base_url = str(getattr(XiuConfig(), "gsk_link", "") or "").strip().rstrip("/")
    if not base_url or not id_str:
        return None

    cache_key = f"{base_url}:{id_str}"
    now = time.monotonic()
    cached = _real_id_cache.get(cache_key)
    if cached:
        expires_at, cached_id = cached
        if now < expires_at:
            return cached_id

    url = f"{base_url}/getid"
    try:
        data = http_client.get_json(
            url,
            params={"type": 2, "id": id_str},
            timeout=timeout,
        )
        real_id = data.get("id")
        real_id = str(real_id) if real_id else None
        ttl = _REAL_ID_CACHE_TTL if real_id else _REAL_ID_NEGATIVE_CACHE_TTL
        _real_id_cache[cache_key] = (now + ttl, real_id)
        return real_id
    except Exception:
        _real_id_cache[cache_key] = (now + _REAL_ID_NEGATIVE_CACHE_TTL, None)
        return None


async def get_real_id_async(id_str, timeout: float = 1.5):
    return await asyncio.to_thread(get_real_id, id_str, timeout)


def call_upload_api(image_data):
    """
    调用接口上传图片
    :param image_data: 可以是文件路径(str/Path)、BytesIO对象或bytes数据
    :return: 成功返回图片URL(str)，失败返回False
    """
    url = XiuConfig().update_image_web
    files = None
    file_obj = None

    try:
        if isinstance(image_data, (str, Path)):
            file_obj = open(image_data, "rb")
            files = {"image": (os.path.basename(str(image_data)), file_obj, "image/png")}
        elif isinstance(image_data, BytesIO):
            image_data.seek(0)
            files = {"image": ("image.png", image_data, "image/png")}
        elif isinstance(image_data, bytes):
            files = {"image": ("image.png", BytesIO(image_data), "image/png")}
        else:
            logger.error(f"call_upload_api: 不支持的数据类型 {type(image_data)}")
            return False

        data = {"channel_id": XiuConfig().channel_id}
        response = http_client.request(
            "POST",
            url,
            files=files,
            data=data,
            timeout=10,
            check_status=False,
        )

        if response.status_code == 200:
            res_json = response.json()
            if res_json.get("success"):
                return res_json.get("url")
            logger.error(f"图片上传接口返回失败: {res_json.get('error')}")
            return False

        logger.error(f"图片上传请求失败，状态码: {response.status_code}")
        return False

    except Exception as e:
        logger.error(f"调用图片上传接口异常: {e}")
        return False
    finally:
        if file_obj:
            file_obj.close()


async def call_upload_api_async(image_data):
    return await asyncio.to_thread(call_upload_api, image_data)
