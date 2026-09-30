"""网站：FastAPI 应用（时间线、状态页、SSE、行片段）"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncEngine
from sse_starlette.sse import EventSourceResponse

from tidewatch.clock import Clock, utcnow
from tidewatch.pipeline.decision import freshness_from_settings
from tidewatch.web.broadcaster import Broadcaster
from tidewatch.web.queries import LEVELS, cluster_rows, collector_status, push_stats, timeline

WEB_DIR = Path(__file__).resolve().parent


def create_app(engine: AsyncEngine, broadcaster: Broadcaster, settings: Any, *, clock: Clock = utcnow) -> FastAPI:
    app = FastAPI(title="观潮", docs_url=None, redoc_url=None, openapi_url=None)
    templates = Jinja2Templates(directory=str(WEB_DIR / "templates"))
    app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")
    freshness = freshness_from_settings(settings)
    assets = [asset.symbol for asset in settings.watchlist.assets]

    def _filters(category: str | None, level: str | None, asset: str | None, show_c: int) -> dict[str, Any]:
        return {
            "category": category if category in ("news", "exchange") else None,
            "level": level if level in LEVELS else None,
            "asset": asset or None,
            "show_c": bool(show_c),
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
                    asset: str | None = None, show_c: int = 0):
        filters = _filters(category, level, asset, show_c)
        context = {"rows": await _rows(filters), "filters": filters, "assets": assets, "active": "home"}
        return templates.TemplateResponse(request, "index.html", context)

    @app.get("/api/timeline")
    async def api_timeline(category: str | None = None, level: str | None = None,
                           asset: str | None = None, show_c: int = 0):
        return await _rows(_filters(category, level, asset, show_c))

    @app.get("/fragment/cluster/{cluster_id}", response_class=HTMLResponse)
    async def cluster_fragment(request: Request, cluster_id: int):
        rows = await cluster_rows(engine, [cluster_id], freshness=freshness)
        if not rows:
            raise HTTPException(status_code=404, detail="事件簇不存在")
        return templates.TemplateResponse(request, "_row.html", {"row": rows[0]})

    @app.get("/status", response_class=HTMLResponse)
    async def status_page(request: Request):
        context = {
            "collectors": await collector_status(engine),
            "stats": await push_stats(engine, clock()),
            "active": "status",
        }
        return templates.TemplateResponse(request, "status.html", context)

    @app.get("/api/status")
    async def api_status():
        return {"collectors": await collector_status(engine), "push_stats": await push_stats(engine, clock())}

    @app.get("/stream")
    async def stream():
        return EventSourceResponse(broadcaster.subscribe(), ping=15)

    return app
