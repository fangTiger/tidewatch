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
        # 带报价后缀的词不受 2 字母限制（2ZUSDT 与 24HUSDT 结构相同，无法区分）：这类写法不是裸词
        ("OKX to list 24HUSDT X-Perp", ["24H"]),
        ("OKX to list 20XUSD and 3LUSDT X-Perps", ["20X", "3L"]),
        # 满足条件的数字开头代码照常提取，带后缀的先去后缀再判断
        ("OKX to list 1INCH spot trading pair", ["1INCH"]),
        ("OKX to list 1000PEPE spot trading pair", ["1000PEPE"]),
        ("Binance Futures Will Launch 1000PEPEUSDT Perpetual Contract", ["1000PEPE"]),
        ("OKX to list 1000PEPEUSD X-Perp", ["1000PEPE"]),
    ],
)
def test_extract_symbols_digit_leading_needs_two_letters(title, symbols):
    assert extract_symbols(title) == symbols


# ---------------------------------------------------------------------------
# 暂停/恢复判定：先出现的动词优先
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("hint", [None, "suspension"])
@pytest.mark.parametrize(
    ("title", "expected"),
    [
        # 暂停类先出现：后面的 Completed / resume 只是说明何时恢复，整体仍是暂停公告
        ("Binance Will Suspend Deposits and Withdrawals on X Network Until the Upgrade Is Completed", "deposit_withdraw_suspend"),
        ("OKX will suspend deposits and resume after upgrade", "deposit_withdraw_suspend"),
        ("Binance Will Pause Deposits and Withdrawals on X Network and Reopen After the Upgrade", "deposit_withdraw_suspend"),
        ("OKX 将暂停 X 网络充值和提现 deposits withdrawals，升级完成后 resume", "deposit_withdraw_suspend"),
        # 恢复类先出现（或只出现恢复类）：不是暂停公告
        ("Binance Has Restored Deposits After Suspension", "other"),
        ("OKX to resume deposits and withdrawals on the Lisk network", "other"),
        ("OKX to enable deposits and withdrawals on the Lisk network", "other"),
        ("OKX to re-enable deposits and withdrawals on the Lisk network", "other"),
        ("Binance Has Completed the Network Upgrade and Enabled Deposits", "other"),
        ("Binance Has Completed the Network Upgrade and Enabled Deposits and Withdrawals Suspended Earlier", "other"),
        ("Binance Will Resume Deposits and Withdrawals on X After the Suspension", "other"),
    ],
)
def test_classify_first_verb_wins(title, hint, expected):
    assert classify(title, hint) == expected


@pytest.mark.parametrize("hint", [None, "suspension"])
def test_classify_completes_the_upgrade_phrase_is_restore(hint):
    # 窄化后的完成类短语仍要识别「completes the ... upgrade」这类恢复公告
    title = "OKX completes the Lisk network upgrade, deposits and withdrawals are open"
    assert classify(title, hint) == "other"


def test_classify_suspension_hint_without_any_verb_defaults_to_suspend():
    # 栏目提示是暂停时，标题里既没有暂停动词也没有恢复动词，沿用栏目判定
    assert classify("OKX adjusts deposit confirmations on the Lisk network", "suspension") == "deposit_withdraw_suspend"
    assert classify("OKX adjusts deposit confirmations on the Lisk network", None) == "other"


# ---------------------------------------------------------------------------
# 数字开头代码：2 字母规则只对裸词生效
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("title", "symbols"),
    [
        # 后面紧跟 / 的交易对写法不受限制
        ("OKX to list 2Z/USDT (Double Zero) for spot trading", ["2Z"]),
        ("OKX to list 0G/USDT spot trading pair", ["0G"]),
        ("OKX to list 2Z/USDT and 0G/USDT spot trading pairs", ["2Z", "0G"]),
        # 带报价后缀的词先去后缀再判断，不受 2 字母限制
        ("Binance Futures Will Launch 2ZUSDT Perpetual Contract", ["2Z"]),
        ("OKX to list 0GUSD X-Perp", ["0G"]),
        # 括号内的代码是标题明示的
        ("Binance Will List Double Zero (2Z) with Seed Tag", ["2Z"]),
        ("Double Zero (2Z)", ["2Z"]),
        # 裸词仍受 2 字母限制
        ("OKX to update 24H volume rules for BTC", ["BTC"]),
        ("Binance Futures Will Adjust Leverage to 20X for ABCUSDT Perpetual Contract", ["ABC"]),
        ("OKX to delist BTC 3L and 3S leveraged tokens", ["BTC"]),
        # 边界：数字开头且恰好 2 个字母的裸词可以提取
        ("OKX to list 1AB spot trading pair", ["1AB"]),
        ("OKX to list 12AB spot trading pair", ["12AB"]),
        # 带后缀但去后缀后是纯数字：仍不是币种
        ("OKX to adjust 1000USDT and 24/USDT limits", []),
    ],
)
def test_extract_symbols_two_letter_rule_only_for_bare_words(title, symbols):
    assert extract_symbols(title) == symbols


# ---------------------------------------------------------------------------
# 停用词
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("title", "symbols"),
    [
        ("OKX to require 2FA for DOGE withdrawals", ["DOGE"]),
        ("Binance Will Add USD1 Trading Pairs for ABC (ABC)", ["ABC"]),
        ("OKX to list USD1 and DOGE spot trading pairs", ["DOGE"]),
        ("OKX to enable 2FA and KYB checks for USD1 deposits", []),
        ("OKX to list ABC TGE for KRW and VND pairs", ["ABC"]),
    ],
)
def test_extract_symbols_new_stopwords(title, symbols):
    assert extract_symbols(title) == symbols


# ---------------------------------------------------------------------------
# 括号内结果先过滤停用词，过滤后为空再回退到正文大写词
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("title", "symbols"),
    [
        ("OKX Flash Earn Lite (USDT) is now live, Subscribe ETH to earn rewards", ["ETH"]),
        ("Binance Will Delist ABC, DEF at 2026-10-01 03:00 (UTC)", ["ABC", "DEF"]),
        ("OKX to delist XYZ at 10:00 (UTC) on 2026-10-01 (2026)", ["XYZ"]),
        # 括号内有有效代码时，不再看正文
        ("Binance Will List Hyperliquid (HYPE) at 10:00 (UTC) with ABC", ["HYPE"]),
        # 括号内全是停用词、正文也没有币种
        ("Notice of Maintenance at 10:00 (UTC)", []),
    ],
)
def test_extract_symbols_paren_stopwords_fall_back_to_body(title, symbols):
    assert extract_symbols(title) == symbols


# ---------------------------------------------------------------------------
# 去重键：发布时间必须带时区
# ---------------------------------------------------------------------------


def test_binance_dedupe_key_rejects_naive_datetime():
    with pytest.raises(ValueError, match="时区"):
        binance_dedupe_key("Binance Will List XYZ (XYZ)", datetime(2026, 9, 30, 1, 0))
