"""HTTP 客户端：按采集器的代理设置创建 httpx 客户端"""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)


def make_client(proxy_mode: str, proxy_url: str | None, *, timeout: float = 20.0) -> httpx.AsyncClient:
    """proxy_mode 为 global 且配置了代理地址时走代理，否则直连；不读取系统环境代理

    proxy_mode 为 global 却没有配置代理地址时，请求实际会直连：记一条 warning，避免被墙的站点静默连不上。
    """
    if proxy_mode == "global" and not proxy_url:
        logger.warning("该采集器要求走全局代理（proxy: global），但未配置代理地址，直连")
    proxy = proxy_url if proxy_mode == "global" and proxy_url else None
    return httpx.AsyncClient(
        proxy=proxy,
        timeout=timeout,
        trust_env=False,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    )
