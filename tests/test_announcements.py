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


@pytest.mark.parametrize(
    ("title", "hint", "expected"),
    [
        # 币名中含 swap 的现货项目不是合约（单词边界）
        ("Binance Will List Uniswap (UNI) with Seed Tag Applied", "listing", "spot_listing"),
        ("Binance Will List PancakeSwap (CAKE) with Seed Tag Applied", "listing", "spot_listing"),
        ("OKX to list SushiSwap (SUSHI) for spot trading", "listing", "spot_listing"),
        ("Binance Will Delist Uniswap (UNI) on 2026-10-02", "delisting", "delisting"),
        ("OKX to delist PancakeSwap (CAKE) and SushiSwap (SUSHI) spot trading pairs", "delisting", "delisting"),
        # 真正的合约写法仍判为合约（不区分大小写）
        ("OKX to list XDPUSD X-Perp", "listing", "perp_listing"),
        ("okx to list xdpusd x-perp", "listing", "perp_listing"),
        ("OKX to list DATAUSD, SOONUSD X-Perps", "listing", "perp_listing"),
        ("OKX to list DOGE-USDT SWAP", "listing", "perp_listing"),
        ("OKX to list ETH perpetual futures", "listing", "perp_listing"),
        ("Binance Futures Will Launch XYZUSDT USDⓈ-Margined Perpetual Contract", "listing", "perp_listing"),
        ("Binance Futures Will Delist USDⓈ-M ABCUSDT Perpetual Contract", "delisting", "perp_delisting"),
        ("OKX to delist ABCUSDT X-Perp", "delisting", "perp_delisting"),
    ],
)
def test_classify_perp_word_boundary(title, hint, expected):
    assert classify(title, hint) == expected


@pytest.mark.parametrize(
    ("title", "hint", "expected"),
    [
        ("OKX to cease deposits and withdrawals on the Lisk network", "suspension", "deposit_withdraw_suspend"),
        ("OKX to suspend deposits and withdrawals on the Lisk network", "suspension", "deposit_withdraw_suspend"),
        ("OKX to resume deposits and withdrawals on the Lisk network", "suspension", "other"),
        ("OKX to reopen deposits and withdrawals on the Lisk network", "suspension", "other"),
        ("OKX has enabled deposits and withdrawals on the Lisk network", "suspension", "other"),
        ("OKX has restored deposits and withdrawals on the Lisk network", "suspension", "other"),
        ("OKX completes the Lisk network upgrade, deposits and withdrawals are open", "suspension", "other"),
        # 没有栏目提示时，恢复类公告同样不算暂停
        ("Binance Has Restored Deposits and Withdrawals on the Solana Network After Suspension", None, "other"),
        ("Binance Will Suspend Deposits and Withdrawals on the Solana Network", None, "deposit_withdraw_suspend"),
    ],
)
def test_classify_suspension_recovery_is_other(title, hint, expected):
    assert classify(title, hint) == expected


@pytest.mark.parametrize(
    ("title", "symbols"),
    [
        # 数字开头的代码
        ("Binance Futures Will Launch 1000PEPEUSDT USDⓈ-Margined Perpetual Contract", ["1000PEPE"]),
        ("OKX to list 1000SATSUSD X-Perp", ["1000SATS"]),
        ("OKX to delist 1INCH and 1000BONK spot trading pairs", ["1INCH", "1000BONK"]),
        ("Binance Will List Example (1MBABYDOGE) with Seed Tag", ["1MBABYDOGE"]),
        # 纯数字（日期等）不是币种
        ("Notice of Removal of Spot Trading Pairs - 2026-10-02 10:00", []),
        ("OKX to adjust 1000USDT limits", []),
        # 稳定币与术语停用词：去后缀之前就要排除（FDUSD、PYUSD 以 USD 结尾，先去后缀会得到 FD、PY）
        ("OKX to list PYUSD and FDUSD spot trading pairs", []),
        ("Binance Will Add FDUSD Trading Pairs for ABC (ABC)", ["ABC"]),
        ("OKX to list DOGE-USDT SWAP", ["DOGE"]),
    ],
)
def test_extract_symbols_digit_leading_and_stopwords(title, symbols):
    assert extract_symbols(title) == symbols


def test_binance_dedupe_key_uses_utc_date_across_timezones():
    # 北京时间 10-01 07:00 与 UTC 09-30 23:00 是同一时刻，按 UTC 日期归一后键必须相同
    cn = datetime.fromisoformat("2026-10-01 07:00+08:00")
    utc = datetime.fromisoformat("2026-09-30 23:00+00:00")
    assert binance_dedupe_key("Binance Will List XYZ (XYZ)", cn) == binance_dedupe_key("Binance Will List XYZ (XYZ)", utc)
    # 同一个北京时间日期、但 UTC 已跨日的两个时刻不同键
    later = datetime.fromisoformat("2026-10-01 09:00+08:00")  # UTC 10-01 01:00
    assert binance_dedupe_key("Binance Will List XYZ (XYZ)", cn) != binance_dedupe_key("Binance Will List XYZ (XYZ)", later)


@pytest.mark.parametrize(
    ("title", "symbols"),
    [
        # 数字开头的代码去掉数字后至少要有 2 个字母，否则不是币种（杠杆倍数、时间周期等）
        ("Binance Futures Will Adjust Leverage to 20X for ABCUSDT Perpetual Contract", ["ABC"]),
        ("OKX to update 24H volume rules for BTC", ["BTC"]),
        ("OKX to delist BTC 3L and 3S leveraged tokens", ["BTC"]),
        ("OKX to list 24HUSDT X-Perp", []),
        ("OKX to list 20XUSD and 3LUSDT X-Perps", []),
        # 满足条件的数字开头代码照常提取，带后缀的先去后缀再判断
        ("OKX to list 1INCH spot trading pair", ["1INCH"]),
        ("OKX to list 1000PEPE spot trading pair", ["1000PEPE"]),
        ("Binance Futures Will Launch 1000PEPEUSDT Perpetual Contract", ["1000PEPE"]),
        ("OKX to list 1000PEPEUSD X-Perp", ["1000PEPE"]),
    ],
)
def test_extract_symbols_digit_leading_needs_two_letters(title, symbols):
    assert extract_symbols(title) == symbols
