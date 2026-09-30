from datetime import datetime, timezone

import pytest

from tidewatch.collectors.announcements import binance_dedupe_key, classify, extract_symbols


@pytest.mark.parametrize(
    ("title", "hint", "expected"),
    [
        ("Binance Will List Hyperliquid (HYPE) with Seed Tag Applied", "listing", "spot_listing"),
        ("Binance Futures Will Launch Multiple TradFi USDⓈ-Margined Perpetual Contracts (2026-09-29)", "listing", "perp_listing"),
        ("Binance Will Add Hyperliquid (HYPE) on Earn, Buy Crypto, Convert, VIP Loan & Margin", "listing", "other"),
        ("Notice of Removal of Spot Trading Pairs - 2026-10-02", "delisting", "delisting"),
        ("OKX to list XDPUSD X-Perp", "listing", "perp_listing"),
        ("OKX to list XDP/USDT (Doppler Finance) for spot trading", "listing", "spot_listing"),
        ("OKX to Adjust KIOXIA Equity Perpetual Futures Due to Corporate Action", "listing", "other"),
        ("OKX to delist DORA, ICX, STORJ, ZEUS and ELF spot trading pairs", "delisting", "delisting"),
        ("OKX to cease deposits and withdrawals on the Lisk network", "suspension", "deposit_withdraw_suspend"),
        ("OKX to resume deposits and withdrawals on the Lisk network", "suspension", "other"),
        ("USDC on Spark (Ethereum) delisting from Onchain Earn", None, "other"),
        ("Binance Will Suspend Deposits and Withdrawals on the Solana Network", None, "deposit_withdraw_suspend"),
        ("Binance Futures Will Delist USDⓈ-M ABCUSDT Perpetual Contract", "delisting", "perp_delisting"),
    ],
)
def test_classify(title, hint, expected):
    assert classify(title, hint) == expected


@pytest.mark.parametrize(
    ("title", "symbols"),
    [
        ("Binance Will List Hyperliquid (HYPE) with Seed Tag Applied", ["HYPE"]),
        ("OKX to list XDPUSD X-Perp", ["XDP"]),
        ("OKX to list DATAUSD, SOONUSD, EIGENUSD, APEUSD X-Perps", ["DATA", "SOON", "EIGEN", "APE"]),
        ("OKX to delist DORA, ICX, STORJ, ZEUS and ELF spot trading pairs", ["DORA", "ICX", "STORJ", "ZEUS", "ELF"]),
        ("Binance Futures Will Launch OURAUSDT USDⓈ-Margined Perpetual Contract Pre-IPO Trading", ["OURA"]),
        ("Notice of Removal of Spot Trading Pairs - 2026-10-02", []),
        ("OKX to cease deposits and withdrawals on the Lisk network", []),
    ],
)
def test_extract_symbols(title, symbols):
    assert extract_symbols(title) == symbols


def test_binance_dedupe_key_same_day_same_title():
    a = binance_dedupe_key("Binance Will List XYZ (XYZ)", datetime(2026, 9, 30, 1, 0, tzinfo=timezone.utc))
    b = binance_dedupe_key("Binance Will List XYZ (XYZ) ", datetime(2026, 9, 30, 1, 0, 0, 500000, tzinfo=timezone.utc))
    c = binance_dedupe_key("Binance Will List XYZ (XYZ)", datetime(2026, 10, 1, 1, 0, tzinfo=timezone.utc))
    assert a == b and a != c and a.startswith("binance_ann:")
