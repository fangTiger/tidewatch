"""外部 HTTP 链接的统一安全校验。"""

from __future__ import annotations

import unicodedata
from urllib.parse import urlsplit


def safe_http_url(url: str | None) -> str | None:
    """返回可安全展示的 HTTP(S) 地址，无效地址返回 ``None``。

    地址必须包含主机名和合法端口，并且不得包含空白、控制字符或
    Unicode 格式字符；解析过程中的任何 ``ValueError`` 都按无效处理。
    """
    cleaned = url or ""
    if not cleaned or any(ch.isspace() or unicodedata.category(ch) in ("Cc", "Cf") for ch in cleaned):
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
