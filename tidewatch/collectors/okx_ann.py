"""OKX 公告采集器（官方公开接口）"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from tidewatch.collectors.announcements import classify, extract_symbols
from tidewatch.collectors.base import PollCollector, parse_context, sort_events
from tidewatch.models import Event, FetchResult

OKX_URL = "https://www.okx.com/api/v5/support/announcements"
MAX_PAGES = 5
# annType → 分类大类
_HINTS = {
    "announcements-new-listings": "listing",
    "announcements-delistings": "delisting",
    "announcements-deposit-withdrawal-suspension-resumption": "suspension",
}


def _label(detail: dict[str, Any]) -> str:
    return f"OKX 公告 {detail.get('url') or detail.get('title') or detail.get('pTime')}"


def _ptime(detail: dict[str, Any]) -> int:
    with parse_context(_label(detail)):
        return int(detail["pTime"])


def parse_detail(detail: dict[str, Any]) -> Event:
    """一条公告 → 事件；解析失败时异常消息带上公告地址"""
    with parse_context(_label(detail)):
        title = detail["title"].strip()
        ann_type = detail.get("annType", "")
        return Event(
            source="okx_ann",
            category="exchange",
            event_type=classify(title, _HINTS.get(ann_type)),
            occurred_at=datetime.fromtimestamp(int(detail["pTime"]) / 1000, tz=timezone.utc),
            title=title,
            url=detail["url"],
            dedupe_key=f"okx_ann:{detail['url']}",
            assets=extract_symbols(title),
            raw={"exchange": "okx", "annType": ann_type, "pTime": detail["pTime"]},
        )


class OkxAnnCollector(PollCollector):
    name = "okx_ann"
    group = "news"

    async def fetch(self, cursor: dict[str, Any] | None) -> FetchResult:
        last = int((cursor or {}).get("last_ptime", 0))
        floor_ms = int((self.clock() - timedelta(seconds=self.cfg.backfill_max_s)).timestamp() * 1000)
        collected: list[dict[str, Any]] = []
        for page in range(1, MAX_PAGES + 1):
            items = await self._page(page)
            collected.extend(items)
            # 本页全部比游标新、且仍在补抓窗口内时才继续翻页；首次运行只取第一页
            if (
                not items
                or cursor is None
                or any(_ptime(item) <= last for item in items)
                or _ptime(items[-1]) < floor_ms
            ):
                break
        # 只按补抓窗口过滤、不按游标过滤：延迟出现的旧公告也能采到，重复由去重键处理
        events = [parse_detail(item) for item in collected if _ptime(item) >= floor_ms]
        # 游标只前进不后退：接口暂时返回比游标更旧的数据时，保持原游标
        newest = max([last, *(_ptime(item) for item in collected)])
        return FetchResult(sort_events(events), {"last_ptime": newest} if newest else None)

    async def _page(self, page: int) -> list[dict[str, Any]]:
        await self.limiter.acquire()
        response = await self.client.get(OKX_URL, params={"page": page})
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != "0":
            raise RuntimeError(f"OKX 公告接口返回错误：{payload.get('code')} {payload.get('msg')}")
        data = payload.get("data") or []
        return (data[0].get("details") if data else None) or []
