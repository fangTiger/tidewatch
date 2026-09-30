import pytest

from tidewatch.app import Application, parse_args
from tidewatch.collectors.binance_ann import BinanceAnnPoll, BinanceAnnStream
from tidewatch.collectors.jin10 import Jin10Collector
from tidewatch.collectors.okx_ann import OkxAnnCollector
from tidewatch.config import ConfigError
from tidewatch.notify.dryrun import DryRunNotifier


def test_parse_args():
    assert parse_args([]).only is None and parse_args([]).dry_run is False
    args = parse_args(["--only", "web", "--dry-run"])
    assert args.only == ["web"] and args.dry_run is True
    with pytest.raises(SystemExit):
        parse_args(["--only", "unknown"])


async def test_requires_feishu_unless_dry_run(settings):
    with pytest.raises(ConfigError) as exc:
        Application(settings, groups={"news"}, dry_run=False)
    assert "FEISHU_WEBHOOK" in str(exc.value)
    web_only = Application(settings, groups={"web"}, dry_run=False)  # 只开网站不需要飞书
    assert isinstance(web_only.notifier, DryRunNotifier) and web_only.collectors == []
    await web_only.stop()


async def test_collectors_and_binance_mode(settings):
    app = Application(settings, groups={"news"}, dry_run=True)
    assert [type(c) for c in app.collectors] == [Jin10Collector, OkxAnnCollector, BinanceAnnPoll]
    assert app.binance_degraded is True
    await app.stop()
    settings.secrets.binance_api_key, settings.secrets.binance_api_secret = "k", "s"
    app = Application(settings, groups={"news"}, dry_run=True)
    assert isinstance(app.collectors[-1], BinanceAnnStream) and app.binance_degraded is False
    await app.stop()


async def test_start_and_stop_cleanly(engine, settings):
    app = Application(settings, groups={"news"}, dry_run=True)
    for collector in app.collectors:  # 测试中不访问外部接口
        await collector.client.aclose()
    app.collectors = []
    await app.start()
    await app.stop()
