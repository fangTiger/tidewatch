"""外部 HTTP 链接的统一安全校验。"""

from __future__ import annotations

import unicodedata
from urllib.parse import urlsplit


def safe_http_url(url: str | None) -> str | None:
    """返回可安全展示的 HTTP(S) 地址，无效地址返回 ``None``。

    先去除首尾空白；地址必须包含主机名和合法端口，内部不得包含空白，
    原始输入不得包含控制字符或 Unicode 格式字符。解析过程中的任何
    ``ValueError`` 都按无效处理。
    """
    raw = url or ""
    if any(unicodedata.category(ch) in ("Cc", "Cf") for ch in raw):
        return None
    cleaned = raw.strip()
    if not cleaned or any(ch.isspace() for ch in cleaned):
        return None
    try:
        parts = urlsplit(cleaned)
        _ = parts.port
        hostname = parts.hostname
    except ValueError:
        return None
    if parts.scheme not in ("http", "https") or not parts.netloc or not hostname:
        return None
    return cleaned
