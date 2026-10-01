"""金十快讯采集器

金十没有公开 API，这里调用其网页使用的快讯接口（请求头与参数参照 RSSHub / newsnow 的实现），
个人低频自用。接口变化会导致解析失败，由健康监控发出告警。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from tidewatch.clock import LOCAL_TZ
from tidewatch.collectors.base import PollCollector, parse_context, sort_events
from tidewatch.models import Event, FetchResult
from tidewatch.pipeline.normalize import clean_text

JIN10_URL = "https://flash-api.jin10.com/get_flash_list"
JIN10_HEADERS = {"x-app-id": "bVBF4FyRTn5NJF5n", "x-version": "1.0.0"}
MAX_PAGES = 60

logger = logging.getLogger(__name__)


def _parse_time(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=LOCAL_TZ)


def _label(item: dict[str, Any]) -> str:
    if "id" not in item:  # 没有 id 就用时间与内容开头定位条目
        data = item.get("data")
        content = clean_text(data.get("content"))[:30] if isinstance(data, dict) else ""
        return f"金十条目（缺少 id，time={item.get('time')}，内容：{content}）"
    return f"金十条目 {item['id']}"


def _item_id(item: dict[str, Any]) -> str:
    with parse_context(_label(item)):
        return item["id"]


def _item_time(item: dict[str, Any]) -> datetime:
    with parse_context(_label(item)):
        return _parse_time(item["time"])


def parse_item(item: dict[str, Any]) -> Event | None:
    """把一条快讯转成事件；没有标题的条目返回 None；解析失败时异常消息带上条目 id"""
    with parse_context(_label(item)):
        return _parse_item(item)


def _parse_item(item: dict[str, Any]) -> Event | None:
    data = item.get("data") or {}
    locked = bool(data.get("lock"))
    if locked:  # VIP 锁定条目只有标题
        title, body = clean_text(data.get("vip_title")), ""
    else:
        body = clean_text(data.get("content"))
        title = clean_text(data.get("title")) or body
    if not title:
        return None
    return Event(
        source="jin10",
        category="news",
        event_type="article" if item.get("type") == 2 else "flash",
        occurred_at=_parse_time(item["time"]),
        title=title[:300],
        body=body,
        url=data.get("link") or f"https://flash.jin10.com/detail/{item['id']}",
        dedupe_key=f"jin10:{item['id']}",
        raw={"id": item["id"], "important": int(item.get("important") or 0), "type": item.get("type"), "locked": locked},
    )


class Jin10Collector(PollCollector):
    name = "jin10"
    group = "news"

    async def fetch(self, cursor: dict[str, Any] | None) -> FetchResult:
        last_id = (cursor or {}).get("last_id")
        floor = self.clock() - timedelta(seconds=self.cfg.backfill_max_s)
        collected: list[dict[str, Any]] = []
        seen: set[str] = set()  # 翻页时 max_time 多取 1 秒，相邻两页会有重叠条目，按 id 去重
        max_time: str | None = None
        for _ in range(MAX_PAGES):
            page = await self._page(max_time)
            if not page:
                break
            # 缺少 id 的条目与缺少 time 的处理一致：整轮失败，由健康监控告警（接口格式变化的信号），而不是悄悄跳过
            ids = [_item_id(item) for item in page]
            fresh = [item for item, item_id in zip(page, ids) if last_id is None or item_id > last_id]
            collected.extend(item for item in fresh if item["id"] not in seen)
            seen.update(item["id"] for item in fresh)
            # 首次运行只取第一页；本页出现已采集过的条目说明已追上游标
            if last_id is None or len(fresh) < len(page):
                break
            oldest_time = _item_time(page[-1])
            if oldest_time < floor:  # 超出补抓窗口
                break
            # 多取 1 秒：避免与上一页最后一条同一秒的快讯被跳过（重复由 id 去重与去重键处理）
            max_time = (oldest_time + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        else:
            logger.warning("金十翻页达到上限 %d 页仍未追上游标或补抓窗口，可能有更早的快讯未采集", MAX_PAGES)
        events = [e for e in (parse_item(item) for item in collected) if e is not None and e.occurred_at >= floor]
        newest = max((item["id"] for item in collected), default=last_id)
        return FetchResult(sort_events(events), {"last_id": newest} if newest else None)

    async def _page(self, max_time: str | None) -> list[dict[str, Any]]:
        await self.limiter.acquire()
        params = {"channel": "-8200", "vip": "1"}
        if max_time:
            params["max_time"] = max_time
        response = await self.client.get(JIN10_URL, params=params, headers=JIN10_HEADERS)
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != 200:
            raise RuntimeError(f"金十接口返回异常状态：{payload.get('status')} {payload.get('message')}")
        return payload.get("data") or []
