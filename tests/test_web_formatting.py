"""网页数字格式器的跨语言边界样例。"""

from tidewatch.web.formatting import duration, leverage, money, number, percent, price


def test_formatters_handle_rounding_missing_zero_and_negative_values():
    assert money("1.005") == "$1.01"
    assert money("-1.005") == "-$1.01"
    assert money("-0.001") == "-$0.00"
    assert money(None) == "—"
    assert percent("0") == "0.00%"
    assert percent("-0.000001") == "-0.00%"
    assert duration("1583.844") == "0.44 小时"
    assert number("12345.6789012") == "12,345.678901"
    assert leverage("6.59") == "6.59×"


def test_formatters_keep_decimal_precision_for_tiny_and_large_values():
    assert price("0.00001234567") == "0.0000123457"
    assert money("9007199254740992.01") == "$9,007,199,254,740,992.01"
    assert money("1e27") == "$1,000,000,000,000,000,000,000,000,000.00"
