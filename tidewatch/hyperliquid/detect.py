"""Hyperliquid 仓位快照事件检测与强平风险滞回状态。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection

from tidewatch.hyperliquid.registry import normalize_address
from tidewatch.models import Event
from tidewatch.store.tables import hl_state

if TYPE_CHECKING:
    from tidewatch.config import HyperliquidCfg


_ZERO = Decimal("0")
_VALID_RISK_STATES = frozenset({"B", "A", "S"})


@dataclass(frozen=True)
class PositionSnapshot:
    """一个地址在单个 dex、单个币种上的确定性仓位快照。"""

    address: str
    dex: str
    coin: str
    szi: Decimal
    notional_usd: Decimal
    updated_at: datetime
    entry_px: Decimal | None = None
    liquidation_px: Decimal | None = None
    leverage: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    account_value: Decimal | None = None
    account_notional_usd: Decimal | None = None

    def __post_init__(self) -> None:
        if self.updated_at.tzinfo is None:
            raise ValueError("仓位快照时间必须带时区")
        if not self.coin:
            raise ValueError("仓位币种不能为空")
        normalize_address(self.address)


@dataclass(frozen=True)
class DetectionResult:
    """事件检测结果，以及调用方应持久化的强平风险状态。"""

    events: list[Event]
    liquidation_risk_state: str | None


@dataclass(frozen=True)
class _RiskTransition:
    event_level: str | None
    next_state: str | None
    distance: Decimal | None


def detect_events(
    previous: PositionSnapshot,
    current: PositionSnapshot,
    *,
    cfg: HyperliquidCfg,
    fill: Mapping[str, Any] | None = None,
    mark_px: Decimal | None = None,
    liquidation_risk_state: str | None = None,
    wallet: Mapping[str, Any] | None = None,
) -> list[Event]:
    """比较相邻仓位快照并返回一期 ``Event`` 列表。"""
    return detect_events_with_state(
        previous,
        current,
        cfg=cfg,
        fill=fill,
        mark_px=mark_px,
        liquidation_risk_state=liquidation_risk_state,
        wallet=wallet,
    ).events


def detect_events_with_state(
    previous: PositionSnapshot,
    current: PositionSnapshot,
    *,
    cfg: HyperliquidCfg,
    fill: Mapping[str, Any] | None = None,
    mark_px: Decimal | None = None,
    liquidation_risk_state: str | None = None,
    wallet: Mapping[str, Any] | None = None,
) -> DetectionResult:
    """检测仓位、强平风险与清算事件，并返回下一份滞回状态。"""
    address, dex, coin = _validate_pair(previous, current)
    if liquidation_risk_state is not None and liquidation_risk_state not in _VALID_RISK_STATES:
        raise ValueError("强平风险状态必须是 B、A、S 或 None")
    evidence = fill if _fill_matches(fill, coin) else None
    timestamp = _fill_time(evidence) if evidence is not None else current.updated_at
    suffix = str(evidence["tid"]) if evidence is not None and evidence.get("tid") is not None else _millis(current.updated_at)
    events: list[Event] = []

    old_szi, new_szi = previous.szi, current.szi
    old_abs, new_abs = abs(old_szi), abs(new_szi)
    threshold = _coin_threshold(coin, cfg)
    same_side = old_szi * new_szi > 0

    if old_szi == 0 and new_szi != 0:
        if current.notional_usd >= threshold:
            events.append(
                _position_event(
                    "hl_open",
                    current,
                    amount=current.notional_usd,
                    quantity=new_abs,
                    occurred_at=timestamp,
                    suffix=suffix,
                    cfg=cfg,
                    fill=evidence,
                    mark_px=mark_px,
                    wallet=wallet,
                )
            )
    elif old_szi != 0 and new_szi == 0:
        if previous.notional_usd >= threshold:
            events.append(
                _position_event(
                    "hl_close",
                    previous,
                    amount=previous.notional_usd,
                    quantity=old_abs,
                    occurred_at=timestamp,
                    suffix=suffix,
                    cfg=cfg,
                    fill=evidence,
                    mark_px=mark_px,
                    wallet=wallet,
                )
            )
    elif old_szi * new_szi < 0:
        if previous.notional_usd >= threshold:
            events.append(
                _position_event(
                    "hl_close",
                    previous,
                    amount=previous.notional_usd,
                    quantity=old_abs,
                    occurred_at=timestamp,
                    suffix=suffix,
                    cfg=cfg,
                    fill=evidence,
                    mark_px=mark_px,
                    wallet=wallet,
                )
            )
        if current.notional_usd >= threshold:
            events.append(
                _position_event(
                    "hl_open",
                    current,
                    amount=current.notional_usd,
                    quantity=new_abs,
                    occurred_at=timestamp,
                    suffix=suffix,
                    cfg=cfg,
                    fill=evidence,
                    mark_px=mark_px,
                    wallet=wallet,
                )
            )
    elif same_side and old_abs != new_abs:
        quantity = abs(new_abs - old_abs)
        ratio = quantity / old_abs
        change_notional = quantity * _reference_price(previous, current, evidence, mark_px)
        if ratio >= _decimal(cfg.events.position_change_ratio) and change_notional >= threshold / 2:
            event_type = "hl_increase" if new_abs > old_abs else "hl_decrease"
            events.append(
                _position_event(
                    event_type,
                    current,
                    amount=change_notional,
                    quantity=quantity,
                    occurred_at=timestamp,
                    suffix=suffix,
                    cfg=cfg,
                    fill=evidence,
                    mark_px=mark_px,
                    wallet=wallet,
                )
            )

    risk = _liquidation_risk(current, mark_px, liquidation_risk_state, cfg)
    if risk.event_level is not None:
        events.append(
            _risk_event(
                current,
                risk,
                cfg=cfg,
                wallet=wallet,
                mark_px=mark_px,
            )
        )

    liquidation = evidence.get("liquidation") if evidence is not None else None
    if isinstance(liquidation, Mapping) and _same_address(liquidation.get("liquidatedUser"), address):
        amount = max(previous.notional_usd, current.notional_usd, _fill_notional(evidence))
        events.append(
            _liquidated_event(
                current if current.szi != 0 else previous,
                amount=amount,
                occurred_at=_fill_time(evidence),
                suffix=str(evidence.get("tid") or _millis(_fill_time(evidence))),
                fill=evidence,
                cfg=cfg,
                wallet=wallet,
                mark_px=mark_px,
            )
        )

    return DetectionResult(events, risk.next_state)


def _validate_pair(previous: PositionSnapshot, current: PositionSnapshot) -> tuple[str, str, str]:
    old_key = (normalize_address(previous.address), previous.dex, previous.coin)
    new_key = (normalize_address(current.address), current.dex, current.coin)
    if old_key != new_key:
        raise ValueError("前后仓位快照必须属于同一地址、dex 与币种")
    return new_key


def _coin_threshold(coin: str, cfg: HyperliquidCfg) -> Decimal:
    symbol = coin.rsplit(":", 1)[-1]
    if ":" not in coin and symbol in {"BTC", "ETH"}:
        return _decimal(cfg.events.btc_eth_min_notional_usd)
    if ":" not in coin and symbol in set(cfg.top_altcoins):
        return _decimal(cfg.events.top_alt_min_notional_usd)
    return _decimal(cfg.events.other_min_notional_usd)


def _reference_price(
    previous: PositionSnapshot,
    current: PositionSnapshot,
    fill: Mapping[str, Any] | None,
    mark_px: Decimal | None,
) -> Decimal:
    candidates: tuple[Any, ...] = (
        mark_px,
        fill.get("px") if fill is not None else None,
        current.notional_usd / abs(current.szi) if current.szi != 0 else None,
        previous.notional_usd / abs(previous.szi) if previous.szi != 0 else None,
    )
    for candidate in candidates:
        if candidate is not None:
            price = _decimal(candidate)
            if price > 0:
                return price
    raise ValueError("仓位变化检测缺少有效价格")


def _position_event(
    event_type: str,
    snapshot: PositionSnapshot,
    *,
    amount: Decimal,
    quantity: Decimal,
    occurred_at: datetime,
    suffix: str,
    cfg: HyperliquidCfg,
    fill: Mapping[str, Any] | None,
    mark_px: Decimal | None,
    wallet: Mapping[str, Any] | None,
) -> Event:
    direction = "long" if snapshot.szi > 0 else "short"
    labels = {
        "hl_open": "开仓",
        "hl_increase": "加仓",
        "hl_decrease": "减仓",
        "hl_close": "平仓",
    }
    raw = _raw_details(
        snapshot,
        quantity=quantity,
        amount=amount,
        direction=direction,
        cfg=cfg,
        fill=fill,
        mark_px=mark_px,
        wallet=wallet,
        include_closed_pnl=event_type in {"hl_decrease", "hl_close"},
    )
    return _event(
        snapshot,
        event_type,
        occurred_at,
        suffix,
        amount,
        f"Hyperliquid 巨鲸{labels[event_type]} {snapshot.coin}",
        raw,
    )


def _risk_event(
    snapshot: PositionSnapshot,
    transition: _RiskTransition,
    *,
    cfg: HyperliquidCfg,
    wallet: Mapping[str, Any] | None,
    mark_px: Decimal | None,
) -> Event:
    raw = _raw_details(
        snapshot,
        quantity=abs(snapshot.szi),
        amount=snapshot.notional_usd,
        direction="long" if snapshot.szi > 0 else "short",
        cfg=cfg,
        fill=None,
        mark_px=mark_px,
        wallet=wallet,
        include_closed_pnl=False,
    )
    raw["liquidation_risk_level"] = transition.event_level
    raw["liquidation_distance"] = _text(transition.distance)
    return _event(
        snapshot,
        "hl_liq_risk",
        snapshot.updated_at,
        _millis(snapshot.updated_at),
        snapshot.notional_usd,
        f"Hyperliquid 巨鲸接近强平 {snapshot.coin}",
        raw,
    )


def _liquidated_event(
    snapshot: PositionSnapshot,
    *,
    amount: Decimal,
    occurred_at: datetime,
    suffix: str,
    fill: Mapping[str, Any],
    cfg: HyperliquidCfg,
    wallet: Mapping[str, Any] | None,
    mark_px: Decimal | None,
) -> Event:
    direction = "long" if snapshot.szi > 0 else "short"
    quantity = _optional_decimal(fill.get("sz")) or abs(snapshot.szi)
    raw = _raw_details(
        snapshot,
        quantity=quantity,
        amount=amount,
        direction=direction,
        cfg=cfg,
        fill=fill,
        mark_px=mark_px,
        wallet=wallet,
        include_closed_pnl=True,
    )
    raw["liquidation"] = dict(fill["liquidation"])
    return _event(
        snapshot,
        "hl_liquidated",
        occurred_at,
        suffix,
        amount,
        f"Hyperliquid 巨鲸被清算 {snapshot.coin}",
        raw,
    )


def _event(
    snapshot: PositionSnapshot,
    event_type: str,
    occurred_at: datetime,
    suffix: str,
    amount: Decimal,
    title: str,
    raw: dict[str, Any],
) -> Event:
    address = normalize_address(snapshot.address)
    return Event(
        source="hl_monitor",
        category="hyperliquid",
        event_type=event_type,
        occurred_at=occurred_at,
        title=title,
        dedupe_key=f"hl:{address}:{snapshot.coin}:{event_type}:{suffix}",
        assets=[snapshot.coin],
        amount_usd=amount,
        raw=raw,
    )


def _raw_details(
    snapshot: PositionSnapshot,
    *,
    quantity: Decimal,
    amount: Decimal,
    direction: str,
    cfg: HyperliquidCfg,
    fill: Mapping[str, Any] | None,
    mark_px: Decimal | None,
    wallet: Mapping[str, Any] | None,
    include_closed_pnl: bool,
) -> dict[str, Any]:
    actual_leverage = None
    if snapshot.account_value is not None and snapshot.account_value > 0:
        account_notional = snapshot.account_notional_usd or snapshot.notional_usd
        actual_leverage = account_notional / snapshot.account_value
    high_leverage = (
        snapshot.leverage is not None
        and snapshot.leverage >= _decimal(cfg.events.configured_leverage_threshold)
    ) or (
        actual_leverage is not None
        and actual_leverage >= _decimal(cfg.events.actual_leverage_threshold)
    )
    distance = _distance(mark_px, snapshot.liquidation_px)
    metadata = dict(wallet or {})
    closed_pnl = (
        _optional_decimal(fill.get("closedPnl"))
        if include_closed_pnl and fill is not None
        else None
    )
    tags = metadata.get("tags")
    if not isinstance(tags, (list, tuple)):
        tags = []
    raw: dict[str, Any] = {
        "address": normalize_address(snapshot.address),
        "dex": snapshot.dex,
        "coin": snapshot.coin,
        "direction": direction,
        "quantity": _text(quantity),
        "notional_usd": _text(amount),
        "entry_px": _text(snapshot.entry_px),
        "liquidation_px": _text(snapshot.liquidation_px),
        "liquidation_distance": _text(distance),
        "mark_px": _text(mark_px),
        "configured_leverage": _text(snapshot.leverage),
        "actual_leverage": _text(actual_leverage),
        "high_leverage": bool(high_leverage),
        "closed_pnl": _text(closed_pnl),
        "unrealized_pnl": _text(snapshot.unrealized_pnl),
        "tier": metadata.get("tier"),
        "style": metadata.get("style"),
        "note": metadata.get("note"),
        "tags": [str(tag) for tag in tags if str(tag)],
    }
    if fill is not None and fill.get("tid") is not None:
        raw["tid"] = fill["tid"]
    return raw


def _liquidation_risk(
    snapshot: PositionSnapshot,
    mark_px: Decimal | None,
    previous_state: str | None,
    cfg: HyperliquidCfg,
) -> _RiskTransition:
    distance = _distance(mark_px, snapshot.liquidation_px)
    if snapshot.szi == 0 or snapshot.notional_usd <= 0 or distance is None:
        return _RiskTransition(None, None, distance)

    warning = _decimal(cfg.events.liquidation_risk_ratio)
    critical = _decimal(cfg.events.liquidation_critical_ratio)
    hysteresis = _decimal(cfg.events.liquidation_hysteresis_ratio)
    large = snapshot.notional_usd >= _decimal(cfg.events.hot_position_notional_usd)

    if previous_state == "S":
        if distance <= critical + hysteresis:
            return _RiskTransition(None, "S", distance)
        if distance <= warning + hysteresis:
            return _RiskTransition(None, "A" if large else "B", distance)
        return _RiskTransition(None, None, distance)
    if previous_state == "A":
        if large and distance <= critical:
            return _RiskTransition("S", "S", distance)
        if distance <= warning + hysteresis:
            return _RiskTransition(None, "A", distance)
        return _RiskTransition(None, None, distance)
    if previous_state == "B":
        if large and distance <= critical:
            return _RiskTransition("S", "S", distance)
        if large and distance <= warning:
            return _RiskTransition("A", "A", distance)
        if distance <= warning + hysteresis:
            return _RiskTransition(None, "B", distance)
        return _RiskTransition(None, None, distance)
    if large and distance <= critical:
        return _RiskTransition("S", "S", distance)
    if distance <= warning:
        level = "A" if large else "B"
        return _RiskTransition(level, level, distance)
    return _RiskTransition(None, None, distance)


def _distance(mark_px: Decimal | None, liquidation_px: Decimal | None) -> Decimal | None:
    if mark_px is None or liquidation_px is None:
        return None
    mark = _decimal(mark_px)
    if mark <= 0:
        return None
    return abs(mark - liquidation_px) / mark


def _fill_matches(fill: Mapping[str, Any] | None, coin: str) -> bool:
    return isinstance(fill, Mapping) and fill.get("coin") == coin


def _fill_time(fill: Mapping[str, Any]) -> datetime:
    try:
        milliseconds = int(fill["time"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("成交证据缺少有效 time") from exc
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)


def _fill_notional(fill: Mapping[str, Any]) -> Decimal:
    px = _optional_decimal(fill.get("px")) or _ZERO
    size = _optional_decimal(fill.get("sz")) or _ZERO
    return abs(px * size)


def _same_address(candidate: Any, address: str) -> bool:
    try:
        return normalize_address(candidate) == address
    except ValueError:
        return False


def _millis(value: datetime) -> str:
    return str(int(value.timestamp() * 1000))


def _decimal(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"数值无效：{value}") from exc
    if not parsed.is_finite():
        raise ValueError(f"数值无效：{value}")
    return parsed


def _optional_decimal(value: Any) -> Decimal | None:
    return None if value is None else _decimal(value)


def _text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _state_name(address: str, dex: str, coin: str) -> str:
    return f"liq_risk:{normalize_address(address)}:{dex}:{coin}"


async def load_liquidation_risk_state(
    conn: AsyncConnection,
    address: str,
    dex: str,
    coin: str,
) -> str | None:
    """从 ``hl_state`` 读取单仓位的强平风险滞回状态。"""
    cursor = (
        await conn.execute(select(hl_state.c.cursor).where(hl_state.c.name == _state_name(address, dex, coin)))
    ).scalar_one_or_none()
    if not isinstance(cursor, Mapping):
        return None
    level = cursor.get("level")
    return level if level in _VALID_RISK_STATES else None


async def save_liquidation_risk_state(
    conn: AsyncConnection,
    address: str,
    dex: str,
    coin: str,
    level: str | None,
    updated_at: datetime,
) -> None:
    """把单仓位的强平风险滞回状态幂等写入 ``hl_state``。"""
    if level is not None and level not in _VALID_RISK_STATES:
        raise ValueError("强平风险状态必须是 B、A、S 或 None")
    stmt = pg_insert(hl_state).values(
        name=_state_name(address, dex, coin),
        cursor={"level": level},
        updated_at=updated_at,
    )
    await conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["name"],
            set_={"cursor": stmt.excluded.cursor, "updated_at": stmt.excluded.updated_at},
        )
    )


async def delete_liquidation_risk_state(
    conn: AsyncConnection,
    address: str,
    dex: str,
    coin: str,
) -> None:
    """删除已平仓仓位对应的强平风险滞回状态。"""
    await conn.execute(
        delete(hl_state).where(
            hl_state.c.name == _state_name(address, dex, coin)
        )
    )
