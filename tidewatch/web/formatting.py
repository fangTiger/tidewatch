"""网页数字展示格式。"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
from typing import Any

MISSING = "—"


def _decimal(value: Any) -> Decimal | None:
    """把外部数字安全转换为有限 Decimal。"""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or abs(parsed.adjusted()) > 1000:
        return None
    return parsed


def _fixed(value: Decimal, places: int, *, trim: bool = False) -> str:
    quantum = Decimal(1).scaleb(-places)
    integer_places = max(1, value.adjusted() + 1)
    with localcontext() as context:
        context.prec = max(28, len(value.as_tuple().digits), integer_places + places + 2)
        rounded = value.quantize(quantum, rounding=ROUND_HALF_UP)
    result = f"{rounded:,.{places}f}"
    if trim and "." in result:
        result = result.rstrip("0").rstrip(".")
    return result


def money(value: Any) -> str:
    """金额：美元符号、千分位和两位小数。"""
    parsed = _decimal(value)
    if parsed is None:
        return MISSING
    sign = "-" if parsed < 0 else ""
    return f"{sign}${_fixed(abs(parsed), 2)}"


def percent(value: Any) -> str:
    """0–1 比率：百分比两位小数。"""
    parsed = _decimal(value)
    return MISSING if parsed is None else f"{_fixed(parsed * 100, 2)}%"


def duration(value: Any) -> str:
    """秒数：不足一天显示小时，否则显示天。"""
    parsed = _decimal(value)
    if parsed is None:
        return MISSING
    if abs(parsed) < Decimal(86400):
        return f"{_fixed(parsed / Decimal(3600), 2)} 小时"
    return f"{_fixed(parsed / Decimal(86400), 2)} 天"


def number(value: Any, places: int = 6, fixed: bool = False) -> str:
    """普通数量：千分位并限制小数位。"""
    parsed = _decimal(value)
    if parsed is None:
        return MISSING
    return _fixed(parsed, max(0, min(int(places), 12)), trim=not fixed)


def price(value: Any) -> str:
    """价格：大额两位小数，小额按量级保留约六位有效数字。"""
    parsed = _decimal(value)
    if parsed is None:
        return MISSING
    absolute = abs(parsed)
    if absolute >= 1000:
        return _fixed(parsed, 2)
    if absolute >= 1:
        return _fixed(parsed, 4)
    if absolute == 0:
        return "0"
    places = max(4, min(12, 5 - absolute.adjusted()))
    return _fixed(parsed, places, trim=True)


def leverage(value: Any) -> str:
    """杠杆：两位小数及乘号。"""
    parsed = _decimal(value)
    return MISSING if parsed is None else f"{_fixed(parsed, 2)}×"


FILTERS = {
    "money": money,
    "percent": percent,
    "duration": duration,
    "number": number,
    "price": price,
    "leverage": leverage,
}
