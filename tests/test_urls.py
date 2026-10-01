import pytest

from tidewatch.urls import safe_http_url


@pytest.mark.parametrize(
    "url",
    [
        "javascript:alert(1)",
        "JavaScript:alert(1)",
        " javascript:alert(1)",
        "java\nscript:alert(1)",
        "data:text/html,<script>alert(1)</script>",
        "vbscript:msgbox(1)",
        "ftp://example.com/a",
        "flash.jin10.com/1",
        "//example.com/a",
        "example.com/a",
        "",
        "   ",
        None,
        "https://",
        "http://",
        "https:///path",
        "https://@/x",
        "https://a b/c",
        "https://example.com/a b",
        " https://example.com/a",
        "https://example.com/a ",
        "https://example.com/a\nb",
        "https://example.com/a\r\nb",
        "https://example.com/a\tb",
        "https://example.com/\x00",
        "https://exa\x1bmple.com/",
        "https://example.com/\u3000a",
        "http://[::1/a",
        "https://[::1/a",
        "https://exa\u200bmple.com/a",
        "https://example.com/a\u200b",
        "\ufeffhttps://example.com/a",
        "https://example.com\u2060/a",
        "https://example.com/\u202etxt.exe",
        "https://example.com:99999/a",
        "https://example.com:99999/",
        "https://example.com:abc/a",
        "https://example.com:-1/a",
    ],
)
def test_safe_http_url_rejects_unsafe_values(url):
    assert safe_http_url(url) is None


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/a",
        "https://example.com/a",
        "https://example.com/a?b=1",
        "HTTPS://Example.com/A",
        "https://example.com:8443/a?b=c&d=e#f",
        "https://example.com/新闻/1",
        "https://example.com:65535/a",
        "https://user:pw@example.com:8080/a",
    ],
)
def test_safe_http_url_preserves_valid_values(url):
    assert safe_http_url(url) == url
