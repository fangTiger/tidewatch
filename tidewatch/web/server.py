"""网站：FastAPI 应用（时间线、状态页、SSE、行片段）"""

from __future__ import annotations

from pathlib import Path
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncEngine
from sse_starlette.sse import EventSourceResponse

from tidewatch.clock import Clock, utcnow
from tidewatch.hyperliquid.registry import WalletRegistry
from tidewatch.pipeline.decision import freshness_from_settings
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.queries import (
    LEVELS,
    cluster_rows,
    collector_status,
    market_tickers,
    push_stats,
    timeline,
    whale_detail,
    whale_net_positions,
    whale_rows,
)

WEB_DIR = Path(__file__).resolve().parent


def create_app(
    engine: AsyncEngine,
    broadcaster: Broadcaster,
    settings: Any,
    *,
    clock: Clock = utcnow,
    hyperliquid: Any | None = None,
) -> FastAPI:
    app = FastAPI(title="观潮", docs_url=None, redoc_url=None, openapi_url=None)
    templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))
    app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")
    freshness = freshness_from_settings(settings)
    assets = [asset.symbol for asset in settings.watchlist.assets]
    registry = getattr(hyperliquid, "registry", None) or WalletRegistry(
        engine,
        configured_core=settings.hyperliquid.core_wallets,
        core_limit=settings.hyperliquid.tiers.core_limit,
        focus_limit=settings.hyperliquid.tiers.focus_limit,
        clock=clock,
    )

    def _optional_decimal(value: str | None) -> Decimal | None:
        if value in (None, ""):
            return None
        try:
            parsed = Decimal(value)
        except (InvalidOperation, ValueError):
            return None
        return parsed if parsed.is_finite() else None

    def _optional_bool(value: str | None) -> bool | None:
        normalized = (value or "").strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
        return None

    async def _hl_summary() -> dict[str, Any]:
        enabled = hyperliquid is not None
        mids = dict(getattr(getattr(hyperliquid, "monitor", None), "mids", {})) if enabled else {}
        statuses = await collector_status(engine, now=clock()) if enabled else []
        price_sources = [
            item for item in statuses if item["name"] in {"hl_trades_ws", "hl_core_ws"}
        ]
        connected = enabled and all(item["status"] == "normal" for item in price_sources)
        tickers = market_tickers(
            settings.hyperliquid.ticker_symbols,
            mids,
            enabled=enabled,
            connected=connected,
        )
        net_positions = []
        if enabled:
            net_positions = await whale_net_positions(
                engine,
                mids=mids,
                now=clock(),
                stale_after_s=settings.hyperliquid.health.hl_rest.stale_after_s,
            )
        updated = max((row["updated_at"] for row in net_positions), default=None)
        return {
            "enabled": enabled,
            "tickers": tickers,
            "net_positions": net_positions,
            "net_updated_at": updated,
        }

    def _filters(category: str | None, level: str | None, asset: str | None, show_c: str | None) -> dict[str, Any]:
        return {
            "category": category if category in ("news", "exchange") else None,
            "level": level if level in LEVELS else None,
            "asset": asset or None,
            "show_c": (show_c or "").strip().lower() in {"1", "true", "yes", "on"},
        }

    async def _rows(filters: dict[str, Any]) -> list[dict[str, Any]]:
        return await timeline(
            engine,
            freshness=freshness,
            category=filters["category"],
            level=filters["level"],
            asset=filters["asset"],
            include_c=filters["show_c"],
            limit=settings.web.timeline_limit,
        )

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request, category: str | None = None, level: str | None = None,
                    asset: str | None = None, show_c: str | None = None):
        filters = _filters(category, level, asset, show_c)
        context = {
            "rows": await _rows(filters),
            "filters": filters,
            "assets": assets,
            "hyperliquid": await _hl_summary(),
            "active": "home",
        }
        return templates.TemplateResponse(request, "index.html", context)

    @app.get("/api/timeline")
    async def api_timeline(category: str | None = None, level: str | None = None,
                           asset: str | None = None, show_c: str | None = None):
        return await _rows(_filters(category, level, asset, show_c))

    @app.get("/api/hyperliquid")
    async def api_hyperliquid():
        return await _hl_summary()

    def _whale_filters(
        account_value_min: str | None,
        pnl_30d_min: str | None,
        win_rate_min: str | None,
        style: str | None,
        tier: str | None,
        has_position: str | None,
        sort: str | None,
        order: str | None,
    ) -> dict[str, Any]:
        return {
            "account_value_min": _optional_decimal(account_value_min),
            "pnl_30d_min": _optional_decimal(pnl_30d_min),
            "win_rate_min": _optional_decimal(win_rate_min),
            "style": style or None,
            "tier": tier or None,
            "has_position": _optional_bool(has_position),
            "sort_by": sort or "account_value",
            "sort_order": "asc" if order == "asc" else "desc",
        }

    @app.get("/whales", response_class=HTMLResponse)
    async def whales_page(
        request: Request,
        account_value_min: str | None = None,
        pnl_30d_min: str | None = None,
        win_rate_min: str | None = None,
        style: str | None = None,
        tier: str | None = None,
        has_position: str | None = None,
        sort: str | None = None,
        order: str | None = None,
        message: str | None = None,
        kind: str | None = None,
    ):
        filters = _whale_filters(
            account_value_min, pnl_30d_min, win_rate_min, style, tier, has_position, sort, order
        )
        context = {
            "wallets": await whale_rows(engine, **filters, limit=200),
            "filters": filters,
            "message": message,
            "message_kind": kind,
            "active": "whales",
        }
        return templates.TemplateResponse(request, "whales.html", context)

    @app.get("/api/whales")
    async def api_whales(
        account_value_min: str | None = None,
        pnl_30d_min: str | None = None,
        win_rate_min: str | None = None,
        style: str | None = None,
        tier: str | None = None,
        has_position: str | None = None,
        sort: str | None = None,
        order: str | None = None,
    ):
        filters = _whale_filters(
            account_value_min, pnl_30d_min, win_rate_min, style, tier, has_position, sort, order
        )
        return await whale_rows(engine, **filters, limit=200)

    def _same_origin(request: Request) -> bool:
        supplied = request.headers.get("origin") or request.headers.get("referer")
        if not supplied:
            return False
        candidate = urlsplit(supplied)
        expected = request.url
        return candidate.scheme == expected.scheme and candidate.netloc == expected.netloc

    @app.post("/whales/actions")
    async def whale_action(request: Request):
        if not _same_origin(request):
            raise HTTPException(status_code=403, detail="拒绝跨站请求")
        values = {
            key: items[-1]
            for key, items in parse_qs((await request.body()).decode("utf-8"), keep_blank_values=True).items()
        }
        action = values.get("action", "")
        address = values.get("address", "")
        changed = False
        try:
            if action == "add":
                changed = await registry.add(address)
            elif action == "core":
                changed = await registry.set_core(address, True)
            elif action == "un_core":
                changed = await registry.set_core(address, False)
            elif action == "exclude":
                changed = await registry.set_excluded(address, True)
            elif action == "un_exclude":
                changed = await registry.set_excluded(address, False)
            elif action == "note":
                changed = await registry.set_label(address, values.get("note"))
            else:
                raise ValueError("未知的名单操作")
        except ValueError as exc:
            query = urlencode({"message": str(exc), "kind": "error"})
            return RedirectResponse(f"/whales?{query}", status_code=303)
        if changed:
            broadcaster.publish("wallet.updated", {"address": address.lower(), "action": action})
        message = "操作成功" if changed else "名单无需更改"
        return RedirectResponse(f"/whales?{urlencode({'message': message, 'kind': 'success'})}", status_code=303)

    @app.get("/whales/{address}", response_class=HTMLResponse)
    async def whale_detail_page(request: Request, address: str):
        try:
            detail = await whale_detail(
                engine,
                address,
                mids=dict(getattr(getattr(hyperliquid, "monitor", None), "mids", {})),
            )
        except ValueError:
            detail = None
        if detail is None:
            raise HTTPException(status_code=404, detail="钱包不存在")
        return templates.TemplateResponse(
            request,
            "whale_detail.html",
            {"detail": detail, "active": "whales"},
        )

    @app.get("/api/whales/{address}")
    async def api_whale_detail(address: str):
        try:
            detail = await whale_detail(
                engine,
                address,
                mids=dict(getattr(getattr(hyperliquid, "monitor", None), "mids", {})),
            )
        except ValueError:
            detail = None
        if detail is None:
            raise HTTPException(status_code=404, detail="钱包不存在")
        return detail

    @app.get("/fragment/cluster/{cluster_id}", response_class=HTMLResponse)
    async def cluster_fragment(request: Request, cluster_id: int):
        rows = await cluster_rows(engine, [cluster_id], freshness=freshness)
        if not rows:
            raise HTTPException(status_code=404, detail="事件簇不存在")
        return templates.TemplateResponse(request, "_row.html", {"row": rows[0]})

    @app.get("/status", response_class=HTMLResponse)
    async def status_page(request: Request):
        now = clock()
        context = {
            "collectors": await collector_status(engine, now=now),
            "stats": await push_stats(engine, now),
            "active": "status",
        }
        return templates.TemplateResponse(request, "status.html", context)

    @app.get("/api/status")
    async def api_status():
        now = clock()
        return {"collectors": await collector_status(engine, now=now), "push_stats": await push_stats(engine, now)}

    @app.get("/stream")
    async def stream():
        return EventSourceResponse(broadcaster.subscribe(), ping=15, send_timeout=10)

    return app
