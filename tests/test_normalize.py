from tidewatch.config import AssetCfg
from tidewatch.pipeline.normalize import AssetMatcher, clean_text, contains_symbol, normalize_title, title_similarity

NFP = "美国9月非农就业人口增加8.2万人，预期增加12万人，前值增加14.2万人"


def test_clean_text_strips_html():
    assert clean_text("<b>美联储</b>&amp;加息  <br/>测试") == "美联储 &加息 测试"
    assert clean_text("") == ""


def test_normalize_title():
    assert normalize_title("Binance Will List ＸＹＺ (XYZ)!") == "binancewilllistxyzxyz"
    assert normalize_title("【港股】 恒指、翻红！") == "港股恒指翻红"


def test_title_similarity():
    assert title_similarity(NFP, f"【{NFP}】") == 1.0
    near = title_similarity(NFP, "美国9月非农就业人口增加8.2万人，预期12万人，前值增加14.2万人")
    assert near >= 0.9
    assert title_similarity("港股收盘：恒指涨1.2%，科指涨2%", "美股开盘：道指涨0.3%，纳指涨0.5%") < 0.1
    assert title_similarity("", "任意") == 0.0


def test_asset_matcher():
    matcher = AssetMatcher(
        [
            AssetCfg(symbol="BTC", aliases=["比特币", "Bitcoin"]),
            AssetCfg(symbol="ETH", aliases=["以太坊"]),
            AssetCfg(symbol="SOL", aliases=["Solana"]),
            AssetCfg(symbol="MSTR", aliases=["微策略"]),
        ]
    )
    assert matcher.find("微策略再次增持比特币，ETH 跟涨") == ["BTC", "ETH", "MSTR"]
    assert matcher.find("bitcoin 与 solana 齐涨") == ["BTC", "SOL"]
    # 全大写代码区分大小写且要求前后不是字母数字
    assert matcher.find("SOLUTION 与 ETHUSDT、eth") == []
    assert matcher.find("") == []
    assert matcher.find("ＢＴＣ 突破") == ["BTC"]  # 全角先转半角


def test_contains_symbol():
    assert contains_symbol("币安将上线XYZ", "XYZ")
    assert contains_symbol("Binance Will List XYZ (XYZ)", "XYZ")
    assert not contains_symbol("XYZW 上线", "XYZ")
    assert not contains_symbol("任意", "")
