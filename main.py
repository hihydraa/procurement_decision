import html
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

SHEET_ID = "1JNrYTJtmgpOGjdfhIdJKz15i8Jxp-u8r1UFAXuQRs8s"
GIDS = {
    "ENTRY_NYMEX": "72275920",
    "ENTRY_WTI": "833844178",
    "ENTRY_MOPS": "732137330",
    "ENTRY_INVENTORY": "1430708402",
    "SETTING": "1886059221",
    "ENTRY_EPPO": "953448993",
    "NATIONAL_CONSUMPTION_REF": "197066448",
    "ENTRY_OILFUND_SUSTAINABILITY": "799116470",
    "ENTRY_JOBBER": "24077118",
    "RECOMMENDATION": "528700139",
}

TZ = ZoneInfo("Asia/Bangkok")
DASHBOARD_TITLE = "Fuel Procurement Decision Dashboard"

FUEL_CONFIG = {
    "G95": {
        "label_th": "แก๊สโซฮอล์ 95",
        "label_full_th": "แก๊สโซฮอล์ 95 (GASOHOL95 E10)",
        "eppo_aliases": ["GASOHOL95 E10", "GASOHOL 95 E10", "GSH95 E10"],
        "mops_aliases": ["MOGAS 95", "GASOLINE 95", "UNLEADED 95", "MOGAS95"],
        "margin_default": 2.20,
    },
    "DIESEL": {
        "label_th": "ดีเซล",
        "label_full_th": "ดีเซล",
        "eppo_aliases": ["H-DIESEL", "DIESEL", "B7", "B10"],
        "mops_aliases": ["GASOIL", "DIESEL", "10 PPM GASOIL", "ULSD", "GASOIL 10PPM"],
        "margin_default": 1.60,
    },
}


def write_text(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def load_sheet(gid: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    df = pd.read_csv(url)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def norm(text) -> str:
    text = "" if text is None else str(text)
    text = text.strip().lower()
    text = re.sub(r"[\n\r\t]+", " ", text)
    text = re.sub(r"[^a-z0-9ก-๙]+", "", text)
    return text


def parse_number_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace("−", "-", regex=False)
        .str.replace("–", "-", regex=False)
        .str.replace("บาท/ลิตร", "", regex=False)
        .str.replace("บาท/กิโลกรัม", "", regex=False)
        .str.replace("ล้านบาท/วัน", "", regex=False)
        .str.replace("ล้านบาท", "", regex=False)
        .str.strip()
        .replace({"": None, "-": None, "nan": None, "None": None})
    )
    return pd.to_numeric(cleaned, errors="coerce")


def parse_date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def find_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    norm_map = {norm(c): c for c in df.columns}
    for cand in candidates:
        key = norm(cand)
        if key in norm_map:
            return norm_map[key]
    if required:
        raise KeyError(f"ไม่พบคอลัมน์ที่ต้องการ: {candidates}")
    return None


def maybe_find_column(df: pd.DataFrame, keyword_groups: list[list[str]]) -> str | None:
    columns = list(df.columns)
    ncols = {c: norm(c) for c in columns}
    for group in keyword_groups:
        for c, nc in ncols.items():
            if all(k in nc for k in group):
                return c
    return None


def safe_float(v):
    if v is None or pd.isna(v):
        return None
    try:
        return float(v)
    except Exception:
        return None


def fmt_num(v, digits=2):
    if v is None or pd.isna(v):
        return "-"
    return f"{float(v):,.{digits}f}"


def fmt_change(v, digits=2, signed=True):
    if v is None or pd.isna(v):
        return "-"
    n = float(v)
    if signed:
        sign = "+" if n > 0 else ""
        return f"{sign}{n:.{digits}f}"
    return f"{n:.{digits}f}"


def pct_change(curr, prev):
    if curr is None or prev in (None, 0) or pd.isna(curr) or pd.isna(prev):
        return None
    return ((curr - prev) / abs(prev)) * 100.0


def sign_badge_class(v: float | None) -> str:
    if v is None or pd.isna(v) or abs(v) < 1e-9:
        return "secondary"
    return "danger" if v > 0 else "success"


def prep_market_sheet(df: pd.DataFrame) -> pd.DataFrame:
    date_col = find_column(df, ["Date"])
    price_col = maybe_find_column(df, [["price"]])
    if not price_col:
        raise KeyError("ไม่พบคอลัมน์ Price ในชีตตลาดโลก")

    out = df.copy()
    out[date_col] = parse_date_series(out[date_col])
    out[price_col] = parse_number_series(out[price_col])
    out = out.dropna(subset=[date_col]).sort_values(date_col, ascending=False).reset_index(drop=True)
    return out.rename(columns={date_col: "Date", price_col: "Price"})



def prep_mops_sheet(df: pd.DataFrame) -> pd.DataFrame:
    date_col = find_column(df, ["Date"])
    oil_col = find_column(df, ["Oil Type", "Product", "Fuel"])
    price_col = maybe_find_column(df, [["price", "usdbbl"], ["priceusdbbl"], ["price"]])
    if not price_col:
        raise KeyError("ไม่พบคอลัมน์ราคาของ MOPS")

    out = df.copy()
    out[date_col] = parse_date_series(out[date_col])
    out[price_col] = parse_number_series(out[price_col])
    out[oil_col] = out[oil_col].astype(str)
    out = out.dropna(subset=[date_col]).sort_values(date_col, ascending=False).reset_index(drop=True)
    return out.rename(columns={date_col: "Date", oil_col: "Oil Type", price_col: "Price"})



def prep_eppo_sheet(df: pd.DataFrame) -> pd.DataFrame:
    date_col = find_column(df, ["Date"])
    oil_col = find_column(df, ["Oil Type", "Product", "Fuel"])

    ex_col = maybe_find_column(df, [["exrefineryprice"], ["ex", "refinery"], ["exref"]])
    mm_col = maybe_find_column(df, [["marketingmargin"], ["margin", "marketing"]])
    fund_col = maybe_find_column(df, [["oilfund"], ["fund"]])
    retail_col = maybe_find_column(df, [["retail"]])
    ws_col = maybe_find_column(df, [["wholesale"], ["ws"]])

    out = df.copy()
    out[date_col] = parse_date_series(out[date_col])
    out[oil_col] = out[oil_col].astype(str)
    for col in [c for c in [ex_col, mm_col, fund_col, retail_col, ws_col] if c]:
        out[col] = parse_number_series(out[col])
    out = out.dropna(subset=[date_col]).sort_values(date_col, ascending=False).reset_index(drop=True)

    rename_map = {date_col: "Date", oil_col: "Oil Type"}
    if ex_col:
        rename_map[ex_col] = "ExRefinery"
    if mm_col:
        rename_map[mm_col] = "MarketingMargin"
    if fund_col:
        rename_map[fund_col] = "OilFund"
    if retail_col:
        rename_map[retail_col] = "Retail"
    if ws_col:
        rename_map[ws_col] = "Wholesale"
    return out.rename(columns=rename_map)



def prep_oilfund_sheet(df: pd.DataFrame) -> pd.DataFrame:
    date_col = find_column(df, ["Date"])
    balance_col = maybe_find_column(df, [["totalbalance"], ["balance"]])
    cash_col = maybe_find_column(df, [["cashremaining"], ["cash"]])
    subsidy_col = maybe_find_column(df, [["dailysubsidy"], ["subsidy"]])
    collection_col = maybe_find_column(df, [["dailycollection"], ["collection"]])
    net_col = maybe_find_column(df, [["netfundimpact"], ["netimpact"]])
    runway_col = maybe_find_column(df, [["runwaydays"], ["runway"]])
    status_col = maybe_find_column(df, [["status"]])

    out = df.copy()
    out[date_col] = parse_date_series(out[date_col])
    for col in [balance_col, cash_col, subsidy_col, collection_col, net_col, runway_col]:
        if col:
            out[col] = parse_number_series(out[col])
    out = out.dropna(subset=[date_col]).sort_values(date_col, ascending=False).reset_index(drop=True)

    rename_map = {date_col: "Date"}
    for old, new in {
        balance_col: "TotalBalance",
        cash_col: "CashRemaining",
        subsidy_col: "DailySubsidy",
        collection_col: "DailyCollection",
        net_col: "NetFundImpact",
        runway_col: "RunwayDays",
        status_col: "Status",
    }.items():
        if old:
            rename_map[old] = new
    return out.rename(columns=rename_map)



def read_settings(df: pd.DataFrame) -> dict:
    settings = {
        "gasohol95_margin_threshold": 2.20,
        "diesel_margin_threshold": 1.60,
        "buy_score_high": 65.0,
        "buy_score_medium": 52.0,
        "buy_score_normal": 40.0,
        "runway_danger_days": 21.0,
        "runway_warning_days": 35.0,
        "mops_weight": 0.45,
        "nymex_weight": 0.35,
        "wti_weight": 0.20,
    }
    if df is None or df.empty:
        return settings

    work = df.copy()
    key_col = work.columns[0]
    val_col = work.columns[1] if len(work.columns) > 1 else work.columns[0]
    work[key_col] = work[key_col].astype(str)
    work[val_col] = parse_number_series(work[val_col])

    key_map = {
        "gasohol95marginthreshold": "gasohol95_margin_threshold",
        "dieselmarginthreshold": "diesel_margin_threshold",
        "buyscorehigh": "buy_score_high",
        "buyscoremedium": "buy_score_medium",
        "buyscorenormal": "buy_score_normal",
        "runwaydangerdays": "runway_danger_days",
        "runwaywarningdays": "runway_warning_days",
        "mopsweight": "mops_weight",
        "nymexweight": "nymex_weight",
        "wtiweight": "wti_weight",
    }
    for _, row in work.iterrows():
        key = norm(row[key_col])
        val = safe_float(row[val_col])
        if key in key_map and val is not None:
            settings[key_map[key]] = val
    return settings



def compute_market_snapshot(df: pd.DataFrame, label: str) -> dict:
    if df.empty:
        return {"label": label, "latest": None, "chg_1d": None, "chg_3d": None, "pct_3d": None, "date": None}
    latest = safe_float(df.iloc[0]["Price"])
    prev_1 = safe_float(df.iloc[1]["Price"]) if len(df) > 1 else None
    prev_3 = safe_float(df.iloc[min(3, len(df) - 1)]["Price"]) if len(df) > 3 else prev_1
    return {
        "label": label,
        "latest": latest,
        "chg_1d": None if latest is None or prev_1 is None else latest - prev_1,
        "pct_1d": pct_change(latest, prev_1),
        "chg_3d": None if latest is None or prev_3 is None else latest - prev_3,
        "pct_3d": pct_change(latest, prev_3),
        "date": df.iloc[0]["Date"],
    }



def filter_by_aliases(df: pd.DataFrame, aliases: list[str], exact_first: bool = True) -> pd.DataFrame:
    if df.empty:
        return df
    alias_norms = [norm(a) for a in aliases]
    oil_norm = df["Oil Type"].astype(str).map(norm)
    if exact_first:
        exact_mask = oil_norm.isin(alias_norms)
        exact = df.loc[exact_mask].copy().sort_values("Date", ascending=False).reset_index(drop=True)
        if not exact.empty:
            return exact
    contains_mask = oil_norm.apply(lambda x: any(a in x for a in alias_norms))
    return df.loc[contains_mask].copy().sort_values("Date", ascending=False).reset_index(drop=True)



def fuel_eppo_snapshot(df: pd.DataFrame, aliases: list[str]) -> dict:
    sub = filter_by_aliases(df, aliases, exact_first=True)
    if sub.empty:
        return {"date": None, "latest": {}}

    def pack(col):
        if col not in sub.columns:
            return {"latest": None, "chg_1d": None, "chg_3d": None}
        curr = safe_float(sub.iloc[0][col])
        prev1 = safe_float(sub.iloc[1][col]) if len(sub) > 1 else None
        prev3 = safe_float(sub.iloc[min(3, len(sub) - 1)][col]) if len(sub) > 3 else prev1
        return {
            "latest": curr,
            "chg_1d": None if curr is None or prev1 is None else curr - prev1,
            "chg_3d": None if curr is None or prev3 is None else curr - prev3,
        }

    return {
        "date": sub.iloc[0]["Date"],
        "latest": {
            "MarketingMargin": pack("MarketingMargin"),
            "ExRefinery": pack("ExRefinery"),
            "OilFund": pack("OilFund"),
            "Retail": pack("Retail"),
            "Wholesale": pack("Wholesale"),
        },
    }



def fuel_mops_snapshot(df: pd.DataFrame, aliases: list[str]) -> dict:
    sub = filter_by_aliases(df, aliases, exact_first=True)
    if sub.empty:
        return {"latest": None, "chg_1d": None, "chg_3d": None, "date": None}
    curr = safe_float(sub.iloc[0]["Price"])
    prev1 = safe_float(sub.iloc[1]["Price"]) if len(sub) > 1 else None
    prev3 = safe_float(sub.iloc[min(3, len(sub) - 1)]["Price"]) if len(sub) > 3 else prev1
    return {
        "latest": curr,
        "chg_1d": None if curr is None or prev1 is None else curr - prev1,
        "chg_3d": None if curr is None or prev3 is None else curr - prev3,
        "date": sub.iloc[0]["Date"],
    }



def oilfund_snapshot(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    latest = df.iloc[0]
    prev = df.iloc[1] if len(df) > 1 else None

    runway = safe_float(latest.get("RunwayDays"))
    cash = safe_float(latest.get("CashRemaining"))
    subsidy = safe_float(latest.get("DailySubsidy"))
    collection = safe_float(latest.get("DailyCollection"))
    net_impact = safe_float(latest.get("NetFundImpact"))
    balance = safe_float(latest.get("TotalBalance"))

    if runway is None and cash is not None and subsidy not in (None, 0):
        runway = cash / subsidy
    if net_impact is None and subsidy is not None and collection is not None:
        net_impact = collection - subsidy

    balance_prev = safe_float(prev.get("TotalBalance")) if prev is not None else None
    runway_prev = safe_float(prev.get("RunwayDays")) if prev is not None else None

    return {
        "date": latest.get("Date"),
        "balance": balance,
        "balance_chg_1d": None if balance is None or balance_prev is None else balance - balance_prev,
        "cash": cash,
        "subsidy": subsidy,
        "collection": collection,
        "net_impact": net_impact,
        "runway": runway,
        "runway_chg_1d": None if runway is None or runway_prev is None else runway - runway_prev,
        "status": latest.get("Status", "-") if "Status" in latest.index else "-",
    }



def clamp(v, lo, hi):
    return max(lo, min(hi, v))



def compute_market_score(mops, nymex, wti, settings):
    score = 50.0
    signals = []
    mops_pct = 0.0 if mops.get("chg_3d") is None or mops.get("latest") in (None, 0) else (mops["chg_3d"] / mops["latest"]) * 100
    nymex_pct = 0.0 if nymex.get("pct_3d") is None else nymex["pct_3d"]
    wti_pct = 0.0 if wti.get("pct_3d") is None else wti["pct_3d"]

    blended = (
        mops_pct * settings["mops_weight"]
        + nymex_pct * settings["nymex_weight"]
        + wti_pct * settings["wti_weight"]
    )
    score += blended * 7.5

    if blended > 1.2:
        signals.append("ตลาดโลกและ MOPS ขึ้นต่อเนื่อง")
    elif blended < -1.2:
        signals.append("ตลาดโลกและ MOPS อ่อนตัวต่อเนื่อง")
    else:
        signals.append("ตลาดโลกแกว่งในกรอบ")
    return clamp(score, 0, 100), signals



def compute_margin_score(mm_value, mm_change, threshold):
    score = 50.0
    signals = []
    if mm_value is None:
        signals.append("ไม่มีข้อมูลค่าการตลาดล่าสุด")
        return score, signals

    gap = mm_value - threshold
    score -= gap * 22
    if mm_value < threshold - 0.20:
        signals.append(f"ค่าการตลาดต่ำกว่าจุดเฝ้าระวัง ({mm_value:.2f} < {threshold:.2f})")
    elif mm_value > threshold + 0.30:
        signals.append(f"ค่าการตลาดสูงกว่าจุดเฝ้าระวัง ({mm_value:.2f} > {threshold:.2f})")
    else:
        signals.append("ค่าการตลาดอยู่ใกล้ค่ากลางที่กำหนด")

    if mm_change is not None:
        score -= mm_change * 18
        if mm_change < -0.10:
            signals.append("ค่าการตลาดลดลงจากวันก่อน ทำให้แรงกดดันด้านต้นทุนเพิ่ม")
        elif mm_change > 0.10:
            signals.append("ค่าการตลาดปรับขึ้นจากวันก่อน")

    return clamp(score, 0, 100), signals



def compute_oilfund_score(fund: dict, oilfund_per_litre: float | None, settings: dict):
    score = 50.0
    signals = []
    runway = fund.get("runway")
    balance = fund.get("balance")
    subsidy = fund.get("subsidy")

    if runway is not None:
        if runway <= settings["runway_danger_days"]:
            score += 20
            signals.append(f"Runway กองทุนน้ำมันอยู่ในโซนตึงตัว ({runway:.1f} วัน)")
        elif runway <= settings["runway_warning_days"]:
            score += 10
            signals.append(f"Runway กองทุนน้ำมันอยู่ในโซนเฝ้าระวัง ({runway:.1f} วัน)")
        else:
            score -= 8
            signals.append(f"Runway กองทุนน้ำมันยังไม่ตึงมาก ({runway:.1f} วัน)")

    if balance is not None:
        if balance < 0:
            score += 8
            signals.append("ฐานะกองทุนน้ำมันติดลบ")
        else:
            score -= 4
            signals.append("ฐานะกองทุนน้ำมันยังเป็นบวก")

    if subsidy is not None and subsidy > 0:
        if subsidy > 1000:
            score += 8
            signals.append("ภาระชดเชยรายวันอยู่ในระดับสูง")
        elif subsidy < 300:
            score -= 4
            signals.append("ภาระชดเชยรายวันไม่สูงมาก")

    if oilfund_per_litre is not None:
        if oilfund_per_litre < -5:
            score += 10
            signals.append("รัฐกำลังกดราคาผ่านกองทุนในระดับสูง")
        elif oilfund_per_litre > 0:
            score -= 5
            signals.append("กองทุนกลับมาเก็บเงินเข้าระบบ")

    return clamp(score, 0, 100), signals



def decide_action(final_score, market_score, margin_score, settings):
    if final_score >= settings["buy_score_high"]:
        return "เร่งซื้อ", "success", "แรงกดดันด้านต้นทุนมีแนวโน้มเพิ่ม ควรล็อคราคาหรือเร่งจัดซื้อเร็วขึ้น"
    if final_score >= settings["buy_score_medium"]:
        return "ทยอยซื้อ", "warning", "ต้นทุนมีความเสี่ยงขาขึ้น แต่ยังไม่ใช่จุดซื้อทั้งหมดในครั้งเดียว ควรทยอยซื้อ"
    if final_score >= settings["buy_score_normal"]:
        return "ซื้อปกติ", "primary", "ภาพรวมยังสมดุล จัดซื้อตามรอบปกติและติดตามรายวัน"
    if market_score < 45 and margin_score > 55:
        return "รอราคา", "secondary", "ตลาดต้นน้ำอ่อนตัวและค่าการตลาดไม่ตึงมาก จึงมีโอกาสรอจังหวะได้"
    return "ติดตามใกล้ชิด", "danger", "ข้อมูลยังผสมกันหลายด้าน ควรติดตามอย่างใกล้ชิดก่อนตัดสินใจ"



def fuel_analysis(fuel_key, config, eppo_df, mops_df, nymex_snap, wti_snap, fund_snap, settings):
    eppo = fuel_eppo_snapshot(eppo_df, config["eppo_aliases"])
    mops = fuel_mops_snapshot(mops_df, config["mops_aliases"])

    mm = eppo["latest"].get("MarketingMargin", {})
    exr = eppo["latest"].get("ExRefinery", {})
    oilfund_metric = eppo["latest"].get("OilFund", {})
    retail = eppo["latest"].get("Retail", {})
    ws = eppo["latest"].get("Wholesale", {})

    threshold = settings.get(
        f"{fuel_key.lower()}_margin_threshold",
        config["margin_default"],
    )

    market_score, market_signals = compute_market_score(mops, nymex_snap, wti_snap, settings)
    margin_score, margin_signals = compute_margin_score(mm.get("latest"), mm.get("chg_1d"), threshold)
    oilfund_score, fund_signals = compute_oilfund_score(fund_snap, oilfund_metric.get("latest"), settings)

    final_score = clamp(market_score * 0.45 + margin_score * 0.35 + oilfund_score * 0.20, 0, 100)
    action, action_class, executive_note = decide_action(final_score, market_score, margin_score, settings)

    reasons = []
    for bucket in [market_signals, margin_signals, fund_signals]:
        for item in bucket:
            if item not in reasons:
                reasons.append(item)
    reasons = reasons[:4]

    return {
        "fuel_key": fuel_key,
        "fuel_label": config["label_th"],
        "fuel_label_full": config["label_full_th"],
        "date": eppo.get("date") or mops.get("date") or nymex_snap.get("date"),
        "action": action,
        "action_class": action_class,
        "executive_note": executive_note,
        "scores": {
            "final": final_score,
            "market": market_score,
            "margin": margin_score,
            "oilfund": oilfund_score,
        },
        "reasons": reasons,
        "metrics": {
            "ค่าการตลาด": {"value": mm.get("latest"), "delta": mm.get("chg_1d"), "unit": "บาท/ลิตร", "tone": "primary"},
            "จุดเฝ้าระวัง": {"value": threshold, "delta": None, "unit": "บาท/ลิตร", "tone": None},
            "ราคาหน้าโรงกลั่น": {"value": exr.get("latest"), "delta": exr.get("chg_1d"), "unit": "บาท/ลิตร", "tone": None},
            "เงินกองทุนน้ำมัน": {"value": oilfund_metric.get("latest"), "delta": oilfund_metric.get("chg_1d"), "unit": "บาท/ลิตร", "tone": None},
            "ราคาขายปลีก": {"value": retail.get("latest"), "delta": retail.get("chg_1d"), "unit": "บาท/ลิตร", "tone": "success"},
            "ราคาขายส่ง": {"value": ws.get("latest"), "delta": ws.get("chg_1d"), "unit": "บาท/ลิตร", "tone": None},
            "MOPS": {"value": mops.get("latest"), "delta": mops.get("chg_1d"), "unit": "USD/BBL", "tone": None},
            "MOPS 3 วัน": {"value": mops.get("chg_3d"), "delta": None, "unit": "USD/BBL", "tone": None, "signed_value": True},
            "NYMEX": {"value": nymex_snap.get("latest"), "delta": nymex_snap.get("chg_1d"), "unit": "USD", "tone": None},
            "WTI": {"value": wti_snap.get("latest"), "delta": wti_snap.get("chg_1d"), "unit": "USD", "tone": None},
        },
    }



def esc(s: str) -> str:
    return html.escape(str(s))



def render_metric_card(title: str, metric: dict) -> str:
    tone = metric.get("tone")
    extra = f" {tone}" if tone else ""
    value = metric.get("value")
    value_text = fmt_change(value, 2) if metric.get("signed_value") else fmt_num(value, 2)
    badge = ""
    delta = metric.get("delta")
    if delta is not None:
        badge = f'<div class="metric-badge"><span class="badge text-bg-{sign_badge_class(delta)}">Δ {fmt_change(delta, 2)}</span></div>'
    return f'''
    <div class="metric-card{extra}">
      <div class="metric-title">{esc(title)}</div>
      <div class="metric-value">{value_text}</div>
      <div class="metric-unit">{esc(metric.get("unit", ""))}</div>
      {badge}
    </div>
    '''



def build_section(result: dict) -> str:
    date_text = result["date"].strftime("%d/%m/%Y") if result.get("date") else "-"
    score = result["scores"]
    pills = [
        ("คะแนนรวม", score["final"]),
        ("ตลาดโลก", score["market"]),
        ("ค่าการตลาด", score["margin"]),
        ("กองทุนน้ำมัน", score["oilfund"]),
    ]
    pills_html = "".join(
        f'<div class="score-pill"><span>{esc(label)}</span><strong>{fmt_num(value, 1)}</strong></div>'
        for label, value in pills
    )
    reasons_html = "".join(f"<li>{esc(r)}</li>" for r in result["reasons"])
    metrics_html = "".join(render_metric_card(title, metric) for title, metric in result["metrics"].items())
    return f'''
    <section class="fuel-panel">
      <div class="fuel-head">
        <div>
          <div class="eyebrow">คำแนะนำการจัดซื้อ</div>
          <h2>{esc(result["fuel_label_full"])}</h2>
          <div class="muted">ข้อมูลอ้างอิงล่าสุด: {date_text}</div>
        </div>
        <div class="action-box action-{esc(result["action_class"])}">
          <div class="action-label">Action</div>
          <div class="action-value">{esc(result["action"])}</div>
        </div>
      </div>

      <div class="summary-box">
        <div class="summary-title">บทสรุปเชิงวิเคราะห์</div>
        <div>{esc(result["executive_note"])}</div>
        <ul>{reasons_html}</ul>
      </div>

      <div class="score-row">{pills_html}</div>
      <div class="metric-grid">{metrics_html}</div>
    </section>
    '''



def build_html(results, nymex_snap, wti_snap, fund_snap, warnings):
    updated_at = datetime.now(TZ).strftime("%d/%m/%Y %H:%M น.")
    fund_date_text = fund_snap.get("date").strftime("%d/%m/%Y") if fund_snap.get("date") else "-"
    sections = "".join(build_section(r) for r in results)
    warnings_html = ""
    if warnings:
        items = "".join(f"<li>{esc(w)}</li>" for w in warnings)
        warnings_html = f'<div class="warning-box"><strong>คำเตือนด้านข้อมูล</strong><ul>{items}</ul></div>'

    return f'''<!DOCTYPE html>
<html lang="th">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <title>{esc(DASHBOARD_TITLE)}</title>
  <style>
    :root {{
      --bg:#f4f7fb;
      --card:#ffffff;
      --line:#dbe3ef;
      --text:#0f172a;
      --muted:#5b6473;
      --success:#2f9e44;
      --warning:#d39e00;
      --danger:#d64545;
      --primary:#2563eb;
      --pill:#eef4ff;
      --pill-border:#c9d8ff;
    }}
    * {{ box-sizing:border-box; }}
    html, body {{ margin:0; padding:0; font-family:Arial, Helvetica, sans-serif; background:var(--bg); color:var(--text); }}
    body {{ -webkit-text-size-adjust:100%; }}
    .container {{ max-width:1120px; margin:0 auto; padding:12px; }}
    .hero {{ background:linear-gradient(135deg, #0f172a, #1d4ed8); color:#fff; border-radius:22px; padding:18px; margin-bottom:16px; }}
    .hero-grid {{ display:grid; grid-template-columns:1fr; gap:14px; }}
    .hero h1 {{ margin:0 0 8px; font-size:1.4rem; line-height:1.3; }}
    .hero p {{ margin:0; line-height:1.6; font-size:.96rem; }}
    .snapshot-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
    .snapshot-card {{ background:rgba(255,255,255,.12); border:1px solid rgba(255,255,255,.15); border-radius:16px; padding:12px; min-width:0; }}
    .snapshot-card .k {{ font-size:.72rem; text-transform:uppercase; opacity:.82; }}
    .snapshot-card .v {{ font-size:1.25rem; font-weight:800; margin-top:4px; word-break:break-word; }}
    .snapshot-card .s {{ font-size:.8rem; margin-top:6px; line-height:1.45; }}
    .top-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:16px; }}
    .top-card {{ background:var(--card); border-radius:18px; padding:14px; border:1px solid var(--line); box-shadow:0 8px 20px rgba(15,23,42,.04); }}
    .top-card .label {{ font-size:.78rem; color:var(--muted); }}
    .top-card .value {{ font-size:1.25rem; font-weight:800; margin-top:6px; word-break:break-word; }}
    .top-card .muted {{ font-size:.82rem; color:var(--muted); margin-top:4px; }}
    .fuel-panel {{ background:var(--card); border-radius:20px; padding:14px; border:1px solid var(--line); box-shadow:0 8px 20px rgba(15,23,42,.04); margin-bottom:16px; }}
    .fuel-head {{ display:grid; grid-template-columns:1fr; gap:12px; }}
    .eyebrow {{ font-size:.75rem; color:var(--muted); letter-spacing:.04em; text-transform:uppercase; }}
    .fuel-head h2 {{ margin:2px 0 4px; font-size:1.5rem; line-height:1.3; }}
    .muted {{ color:var(--muted); }}
    .action-box {{ width:100%; border-radius:18px; padding:14px 16px; color:#fff; text-align:left; }}
    .action-success {{ background:var(--success); }}
    .action-warning {{ background:var(--warning); }}
    .action-danger {{ background:var(--danger); }}
    .action-primary {{ background:var(--primary); }}
    .action-secondary {{ background:#64748b; }}
    .action-label {{ font-size:.76rem; opacity:.9; text-transform:uppercase; }}
    .action-value {{ font-size:1.45rem; font-weight:800; margin-top:2px; }}
    .summary-box {{ background:#f8fafc; border:1px solid var(--line); border-radius:16px; padding:14px; margin:14px 0; }}
    .summary-title {{ font-weight:800; margin-bottom:8px; }}
    .summary-box ul {{ margin:8px 0 0 18px; padding:0; line-height:1.6; }}
    .score-row {{ display:flex; gap:8px; flex-wrap:wrap; margin-bottom:14px; }}
    .score-pill {{ background:var(--pill); border:1px solid var(--pill-border); border-radius:999px; padding:10px 12px; display:flex; gap:8px; align-items:center; justify-content:space-between; width:100%; font-size:.95rem; }}
    .metric-grid {{ display:grid; grid-template-columns:1fr; gap:10px; }}
    .metric-card {{ background:#fff; border:1px solid var(--line); border-radius:16px; padding:14px; min-width:0; }}
    .metric-card.primary {{ background:#eff6ff; }}
    .metric-card.success {{ background:#eefbf2; }}
    .metric-card.danger {{ background:#fff1f2; }}
    .metric-title {{ font-size:.78rem; color:var(--muted); min-height:0; }}
    .metric-value {{ font-size:1.35rem; font-weight:800; margin-top:8px; line-height:1.25; word-break:break-word; }}
    .metric-unit {{ color:var(--muted); font-size:.82rem; margin-top:4px; }}
    .metric-badge {{ margin-top:10px; }}
    .badge {{ display:inline-block; padding:6px 10px; border-radius:999px; color:#fff; font-size:.8rem; }}
    .text-bg-success {{ background:var(--success); }}
    .text-bg-danger {{ background:var(--danger); }}
    .text-bg-secondary {{ background:#64748b; }}
    .warning-box {{ background:#fff7ed; border:1px solid #fdba74; border-radius:16px; padding:14px; margin-bottom:16px; }}
    .warning-box ul {{ margin:8px 0 0 18px; padding:0; line-height:1.6; }}
    .footer {{ color:var(--muted); text-align:center; padding:6px 0 22px; font-size:.9rem; line-height:1.6; }}
    @media (min-width: 680px) {{
      .container {{ padding:18px; }}
      .hero {{ padding:22px; }}
      .hero-grid {{ grid-template-columns:1.3fr 1fr; }}
      .snapshot-grid {{ grid-template-columns:1fr 1fr; }}
      .top-grid {{ grid-template-columns:repeat(4,1fr); }}
      .fuel-head {{ grid-template-columns:1fr auto; align-items:start; }}
      .action-box {{ min-width:200px; width:auto; }}
      .score-pill {{ width:auto; min-width:160px; }}
      .metric-grid {{ grid-template-columns:repeat(2,1fr); }}
    }}
    @media (min-width: 980px) {{
      .container {{ padding:20px; }}
      .hero h1 {{ font-size:2rem; }}
      .metric-grid {{ grid-template-columns:repeat(5,1fr); }}
      .fuel-panel {{ padding:20px; }}
      .top-card .value, .snapshot-card .v {{ font-size:1.45rem; }}
      .metric-value {{ font-size:1.45rem; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <div class="hero-grid">
        <div>
          <h1>{esc(DASHBOARD_TITLE)}</h1>
          <p>ระบบช่วยตัดสินใจจัดซื้อน้ำมัน โดยผสานข้อมูลจาก NYMEX, WTI, MOPS, EPPO และฐานะกองทุนน้ำมัน เพื่อสรุปเป็นคำแนะนำที่ใช้งานได้จริงสำหรับผู้บริหารและทีมจัดซื้อ</p>
          <p style="margin-top:10px;">อัปเดตล่าสุด: {updated_at}</p>
        </div>
        <div class="snapshot-grid">
          <div class="snapshot-card">
            <div class="k">NYMEX</div>
            <div class="v">{fmt_num(nymex_snap.get('latest'), 2)}</div>
            <div class="s">Δ1D {fmt_change(nymex_snap.get('chg_1d'), 2)} | Δ3D {fmt_change(nymex_snap.get('chg_3d'), 2)}</div>
          </div>
          <div class="snapshot-card">
            <div class="k">WTI</div>
            <div class="v">{fmt_num(wti_snap.get('latest'), 2)}</div>
            <div class="s">Δ1D {fmt_change(wti_snap.get('chg_1d'), 2)} | Δ3D {fmt_change(wti_snap.get('chg_3d'), 2)}</div>
          </div>
          <div class="snapshot-card">
            <div class="k">ฐานะกองทุน</div>
            <div class="v">{fmt_num(fund_snap.get('balance'), 0)}</div>
            <div class="s">ล้านบาท | วันที่ {fund_date_text}</div>
          </div>
          <div class="snapshot-card">
            <div class="k">กองทุนพยุงราคาได้อีกกี่วัน</div>
            <div class="v">{fmt_num(fund_snap.get('runway'), 1)}</div>
            <div class="s">วัน | ชดเชย {fmt_num(fund_snap.get('subsidy'), 0)} ล้านบาท/วัน</div>
          </div>
        </div>
      </div>
    </section>

    {warnings_html}

    <section class="top-grid">
      <div class="top-card">
        <div class="label">เงินสดคงเหลือ (กองทุน)</div>
        <div class="value">{fmt_num(fund_snap.get('cash'), 0)}</div>
        <div class="muted">ล้านบาท</div>
      </div>
      <div class="top-card">
        <div class="label">ภาระชดเชยต่อวัน (กองทุน)</div>
        <div class="value">{fmt_num(fund_snap.get('subsidy'), 0)}</div>
        <div class="muted">ล้านบาท/วัน</div>
      </div>
      <div class="top-card">
        <div class="label">การจัดเก็บต่อวัน (กองทุน)</div>
        <div class="value">{fmt_num(fund_snap.get('collection'), 0)}</div>
        <div class="muted">ล้านบาท/วัน</div>
      </div>
      <div class="top-card">
        <div class="label">ผลกระทบสุทธิต่อวัน (กองทุน)</div>
        <div class="value">{fmt_num(fund_snap.get('net_impact'), 0)}</div>
        <div class="muted">ล้านบาท/วัน</div>
      </div>
    </section>

    {sections}

    <div class="footer">Fuel Procurement Decision Dashboard Vol.1</div>
  </div>
</body>
</html>'''



def main():
    warnings = []
    try:
        df_nymex = prep_market_sheet(load_sheet(GIDS["ENTRY_NYMEX"]))
        df_wti = prep_market_sheet(load_sheet(GIDS["ENTRY_WTI"]))
        df_mops = prep_mops_sheet(load_sheet(GIDS["ENTRY_MOPS"]))
        df_eppo = prep_eppo_sheet(load_sheet(GIDS["ENTRY_EPPO"]))
        df_oilfund = prep_oilfund_sheet(load_sheet(GIDS["ENTRY_OILFUND_SUSTAINABILITY"]))
        df_setting = load_sheet(GIDS["SETTING"])
    except Exception as e:
        write_text("index.html", f"<html><body style='font-family:Arial;padding:24px'><h1>Dashboard Error</h1><p>{esc(str(e))}</p></body></html>")
        raise

    settings = read_settings(df_setting)
    if df_oilfund.empty:
        warnings.append("ไม่มีข้อมูลใน Entry_OilFund_Sustainability")
    if df_eppo.empty:
        warnings.append("ไม่มีข้อมูลใน Entry_Eppo")
    if df_mops.empty:
        warnings.append("ไม่มีข้อมูลใน Entry_MOPS")

    nymex_snap = compute_market_snapshot(df_nymex, "NYMEX")
    wti_snap = compute_market_snapshot(df_wti, "WTI")
    fund_snap = oilfund_snapshot(df_oilfund)

    results = []
    for fuel_key, config in FUEL_CONFIG.items():
        result = fuel_analysis(
            fuel_key=fuel_key,
            config=config,
            eppo_df=df_eppo,
            mops_df=df_mops,
            nymex_snap=nymex_snap,
            wti_snap=wti_snap,
            fund_snap=fund_snap,
            settings=settings,
        )
        if result["date"] is None:
            warnings.append(f"ไม่พบข้อมูลล่าสุดของ {config['label_th']} ใน EPPO/MOPS")
        results.append(result)

    html_out = build_html(results, nymex_snap, wti_snap, fund_snap, warnings)
    write_text("index.html", html_out)
    print("Dashboard created successfully")


if __name__ == "__main__":
    main()
