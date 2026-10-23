import json
import os
import re
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import engine
import model as M
import signals as S

# ============================================================
# CONFIG
# ============================================================
SHEET_ID = "1JNrYTJtmgpOGjdfhIdJKz15i8Jxp-u8r1UFAXuQRs8s"
GIDS = {
    "ENTRY_WTI": "833844178",
    "ENTRY_MOPS": "732137330",
    "ENTRY_INVENTORY": "1430708402",  # reserved for next phase
    "SETTING": "1886059221",
    "ENTRY_EPPO": "953448993",
    "NATIONAL_CONSUMPTION_REF": "197066448",
    "ENTRY_OILFUND_SUSTAINABILITY": "799116470",
    "ENTRY_JOBBER": "24077118",       # reserved for next phase
    "RECOMMENDATION": "528700139",
    "NEWS": "1714884655",
}

FUEL_CONFIG = {
    "GASOHOL95": {
        "label_th": "แก๊สโซฮอล์ 95",
        "aliases": ["GASOHOL95 E10"],
        "margin_default": 3.37,
        "mops_aliases": ["mogas 95", "gasoline 95", "unleaded 95", "mogas95", "g95", "95"],
    },
    "DIESEL": {
        "label_th": "ดีเซล",
        "aliases": ["h-diesel"],
        "margin_default": 1.94,
        "mops_aliases": ["gasoil", "diesel", "10 ppm gasoil", "ulsd", "gasoil 10ppm", "ds"],
    },
}

# ชื่อแท็บฟีดสัญญาณ (ตรงกับ common.TABS — ไม่ import common เพราะ common ต้องใช้ jwt/credential ซึ่ง main.py ไม่ต้องการ)
TABS_FEED = {
    "intraday": "Entry_Intraday", "headsup": "Entry_HeadsUp", "fund": "Entry_FundNotice",
    "mops_close": "Entry_MopsClose", "decision_log": "DecisionLog", "dispatch_log": "DispatchLog", "outlook_log": "OutlookLog",
}

DASHBOARD_TITLE = "Fuel Procurement Decision Dashboard"
TZ = ZoneInfo("Asia/Bangkok")


# ============================================================
# BASIC UTILITIES
# ============================================================
def write_text(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def load_sheet(gid: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    df = pd.read_csv(url)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def load_sheet_by_name(name: str) -> pd.DataFrame:
    """อ่านแท็บตามชื่อ (แท็บฟีดที่ ingest.py สร้างเอง ไม่มี gid คงที่) — ชีทเปิดเป็นสาธารณะแบบเดียวกับ load_sheet"""
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(name)}"
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
        .str.replace("ล้านบาท", "", regex=False)
        .str.strip()
        .replace({"": None, "-": None, "nan": None, "None": None})
    )
    return pd.to_numeric(cleaned, errors="coerce")


def parse_date_series(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()

    parsed = pd.to_datetime(s, errors="coerce")

    missing = parsed.isna()
    if missing.any():
        parsed2 = pd.to_datetime(s[missing], errors="coerce", dayfirst=False)
        parsed.loc[missing] = parsed2

    missing = parsed.isna()
    if missing.any():
        parsed3 = pd.to_datetime(s[missing], errors="coerce", dayfirst=True)
        parsed.loc[missing] = parsed3

    return parsed.dt.date


def parse_datetime_series(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()

    parsed = pd.to_datetime(s, errors="coerce")

    missing = parsed.isna()
    if missing.any():
        parsed2 = pd.to_datetime(s[missing], errors="coerce", dayfirst=False)
        parsed.loc[missing] = parsed2

    missing = parsed.isna()
    if missing.any():
        parsed3 = pd.to_datetime(s[missing], errors="coerce", dayfirst=True)
        parsed.loc[missing] = parsed3

    return parsed


def find_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    norm_map = {norm(c): c for c in df.columns}
    for cand in candidates:
        key = norm(cand)
        if key in norm_map:
            return norm_map[key]
    if not required:
        return None
    raise KeyError(f"ไม่พบคอลัมน์ที่ต้องการในชีต: {candidates}")


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


def fmt_change(v, digits=2):
    if v is None or pd.isna(v):
        return "-"
    sign = "+" if float(v) > 0 else ""
    return f"{sign}{float(v):.{digits}f}"


def pct_change(curr, prev):
    if curr is None or prev in (None, 0) or pd.isna(curr) or pd.isna(prev):
        return None
    return ((curr - prev) / abs(prev)) * 100.0


def sign_badge_class(v: float | None) -> str:
    if v is None or pd.isna(v) or abs(v) < 1e-9:
        return "secondary"
    return "danger" if v > 0 else "success"


def html_escape(text) -> str:
    text = "" if text is None else str(text)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def nl2br(text) -> str:
    return html_escape(text).replace("\n", "<br>")


# ============================================================
# VISUAL TONES
# ============================================================
def classify_market_tone(change_1d, change_3d):
    vals = [v for v in [change_1d, change_3d] if v is not None and not pd.isna(v)]
    if not vals:
        return {"tone": "neutral", "label": "ข้อมูลไม่พอ"}
    score = sum(vals)
    if score >= 0.75:
        return {"tone": "danger", "label": "แรงกดดันสูง"}
    if score <= -0.75:
        return {"tone": "success", "label": "ผ่อนคลาย"}
    return {"tone": "warning", "label": "แกว่งตัว"}


TREND_KIND_TH = {"TREND": "Trend", "HALF": "ครึ่งวัน", "HALF_IMG": "ครึ่งวัน (จากรูป)"}


def trend_snapshot(tables, fuel: str, asof):
    """Trend Mops ล่าสุดของชนิดน้ำมัน (ดีเซล B7 = คอลัมน์ d, เบนซิน G95 = คอลัมน์ b) ที่โพสต์ก่อน asof ; None ถ้าไม่เคยมี"""
    df = tables.intraday
    col = S.TREND_COL[fuel]
    if df.empty or col not in df.columns:
        return None
    w = df[df.ts <= asof]
    w = w[w[col].map(S._num).notna()]
    if w.empty:
        return None
    last = w.iloc[-1]
    return {"value": S._num(last[col]), "ts": last.ts, "kind": str(last.get("kind", "")),
            "today": last.ts.normalize() == asof.normalize(), "n_today": int((w.ts >= asof.normalize()).sum())}


def classify_trend_tone(snap):
    if not snap:
        return {"tone": "neutral", "label": "ยังไม่มีข้อมูล"}
    if not snap["today"]:
        return {"tone": "neutral", "label": "ยังไม่มีของวันนี้"}
    if snap["value"] >= 1:
        return {"tone": "danger", "label": "ดันราคาขึ้น"}
    if snap["value"] <= -1:
        return {"tone": "success", "label": "กดราคาลง"}
    return {"tone": "warning", "label": "ใกล้ศูนย์"}


def trend_card_html(title, snap):
    tone = classify_trend_tone(snap)
    if not snap:
        return snapshot_card(title, "-", "ยังไม่เคยมี Trend Mops ในระบบ", tone["tone"], tone["label"])
    kind = TREND_KIND_TH.get(snap["kind"], snap["kind"] or "Trend")
    when = snap["ts"].strftime("%d/%m %H:%M")
    sub = f"{kind} {when}" + (f" · วันนี้ {snap['n_today']} ครั้ง" if snap["today"] else " · ของวันก่อน")
    return snapshot_card(title, fmt_change(snap["value"], 2), sub, tone["tone"], tone["label"])


def classify_balance_tone(balance):
    if balance is None or pd.isna(balance):
        return {"tone": "neutral", "label": "ข้อมูลไม่พอ"}
    if balance >= 0:
        return {"tone": "success", "label": "ฐานะบวก"}
    if balance <= -15000:
        return {"tone": "danger", "label": "ติดลบหนัก"}
    return {"tone": "warning", "label": "ติดลบ"}


def classify_runway_tone(runway):
    if runway is None or pd.isna(runway):
        return {"tone": "neutral", "label": "ข้อมูลไม่พอ"}
    if runway <= 20:
        return {"tone": "danger", "label": "ตึงมาก"}
    if runway <= 35:
        return {"tone": "warning", "label": "เฝ้าระวัง"}
    return {"tone": "success", "label": "ยังพอมีเวลา"}


# ============================================================
# SHEET PREP
# ============================================================
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
    src_col = next((c for c in df.columns if str(c).strip().lower() == "source"), None)

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
    if src_col:
        rename_map[src_col] = "Source"
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

    out = out.dropna(subset=[date_col]).copy()

    # สร้าง datetime สำหรับ sort ให้ชัวร์
    out["_sort_date"] = pd.to_datetime(out[date_col].astype(str), errors="coerce")
    out = out.sort_values("_sort_date", ascending=False).drop(columns=["_sort_date"]).reset_index(drop=True)

    rename_map = {date_col: "Date"}
    opt_map = {
        balance_col: "TotalBalance",
        cash_col: "CashRemaining",
        subsidy_col: "DailySubsidy",
        collection_col: "DailyCollection",
        net_col: "NetFundImpact",
        runway_col: "RunwayDays",
        status_col: "Status",
    }
    for old, new in opt_map.items():
        if old:
            rename_map[old] = new
    return out.rename(columns=rename_map)


def prep_news_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Timestamp", "Context"])

    ts_col = find_column(df, ["Timestamp"], required=False) or maybe_find_column(df, [["timestamp"]])
    ctx_col = find_column(df, ["Context"], required=False) or maybe_find_column(df, [["context"]])

    if not ts_col or not ctx_col:
        raise KeyError("ไม่พบคอลัมน์ Timestamp หรือ Context ในชีต NEWS")

    out = df.copy()
    out[ts_col] = parse_datetime_series(out[ts_col])
    out[ctx_col] = out[ctx_col].astype(str).fillna("").str.strip()
    out = out.dropna(subset=[ts_col])
    out = out[out[ctx_col] != ""]
    out = out.sort_values(ts_col, ascending=False).reset_index(drop=True)
    return out.rename(columns={ts_col: "Timestamp", ctx_col: "Context"})


# ============================================================
# LOOKUPS / S
# ============================================================
def read_settings(df: pd.DataFrame) -> dict:
    """ค่าตั้งต้นที่ยังใช้แสดงผล: จุดเฝ้าระวังค่าการตลาด (การ์ด) — rule ตัดสินใจเดิมที่ใช้ค่าพวกนี้ถูกแทนที่ด้วย engine.py แล้ว
    ชีท Setting อ่านได้ 2 รูปแบบ: (key, value) ต่อแถว หรือ Date/Exchange Rate (ใช้เก็บอัตราแลกเปลี่ยนล่าสุด)"""
    settings = {
        "gasohol95_margin_threshold": 3.37,
        "diesel_margin_threshold": 1.94,
        "exchange_rate": None,
    }
    if df is None or df.empty:
        return settings

    work = df.copy()
    work.columns = [str(c).strip() for c in work.columns]
    if "Exchange Rate" in work.columns:
        fx = parse_number_series(work["Exchange Rate"]).dropna()
        if len(fx):
            settings["exchange_rate"] = float(fx.iloc[-1])
        return settings

    key_col = work.columns[0]
    val_col = work.columns[1] if len(work.columns) > 1 else work.columns[0]
    work[key_col] = work[key_col].astype(str)
    work[val_col] = parse_number_series(work[val_col])
    key_map = {
        "gasohol95marginthreshold": "gasohol95_margin_threshold",
        "dieselmarginthreshold": "diesel_margin_threshold",
    }
    for _, row in work.iterrows():
        key = norm(row[key_col])
        val = safe_float(row[val_col])
        if key in key_map and val is not None:
            settings[key_map[key]] = val
    return settings


# ============================================================
# SERIES EXTRACTORS
# ============================================================
def compute_market_snapshot(df: pd.DataFrame, label: str) -> dict:
    if df.empty:
        return {"label": label, "latest": None, "chg_1d": None, "chg_3d": None, "date": None}
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


def filter_by_aliases(df: pd.DataFrame, aliases: list[str], exact: bool = False) -> pd.DataFrame:
    if df.empty:
        return df

    alias_norms = [norm(a) for a in aliases]
    oil_norm = df["Oil Type"].astype(str).map(norm)

    if exact:
        mask = oil_norm.isin(alias_norms)
    else:
        mask = oil_norm.apply(lambda x: any(a == x or a in x for a in alias_norms))

    return df.loc[mask].copy().sort_values("Date", ascending=False).reset_index(drop=True)


def fuel_eppo_snapshot(df: pd.DataFrame, aliases: list[str]) -> dict:
    sub = filter_by_aliases(df, aliases, exact=True)

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
    sub = filter_by_aliases(df, aliases)
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


# ============================================================
# DECISION ENGINE
# ============================================================
def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def build_reason_bullets(signals: list[str], max_items=4) -> list[str]:
    uniq = []
    for sig in signals:
        if sig and sig not in uniq:
            uniq.append(sig)
    return uniq[:max_items]


def fuel_analysis(fuel_key, config, eppo_df, mops_df, wti_snap, fund_snap, settings, decision):
    """สรุปต่อชนิดน้ำมัน: ตัวเลขอ้างอิง (EPPO/MOPS) สำหรับการ์ด + คำแนะนำจาก engine.decide (โมเดลที่ฝึกจากข้อมูลย้อนหลัง)"""
    eppo = fuel_eppo_snapshot(eppo_df, config["aliases"])
    mops = fuel_mops_snapshot(mops_df, config["mops_aliases"] + config["aliases"])
    mm = eppo["latest"].get("MarketingMargin", {})
    ex = eppo["latest"].get("ExRefinery", {})
    oilfund = eppo["latest"].get("OilFund", {})
    retail = eppo["latest"].get("Retail", {})
    wholesale = eppo["latest"].get("Wholesale", {})
    threshold = settings["gasohol95_margin_threshold"] if fuel_key == "GASOHOL95" else settings["diesel_margin_threshold"]

    return {
        "fuel_key": fuel_key,
        "label_th": config["label_th"],
        "date": eppo.get("date") or mops.get("date"),
        "threshold": threshold,
        "final_score": None if decision["p_rise"] is None else round(decision["p_rise"] * 100, 1),
        "action": decision["action"],
        "color": decision["color"],
        "summary": decision["summary"],
        "reasons": decision["reasons"],
        "decision": decision,
        "mm": mm, "ex": ex, "oilfund": oilfund, "retail": retail, "wholesale": wholesale, "mops": mops,
    }


# ============================================================
# DATA FRESHNESS / ACCURACY
# ============================================================
STALE_DAYS = 3


def eppo_fresh_date(eppo_df: pd.DataFrame):
    """วันที่ล่าสุดที่ค่า EPPO 'ขยับจริง' — ชีทเดิมลากค่าเดิมมาเติมเมื่อไม่มีไฟล์ใหม่ ทำให้ดูเหมือนมีข้อมูลสดทั้งที่เป็นค่าค้าง
    ใช้คอลัมน์ Source (FRESH/CARRIED) ถ้ามี ไม่งั้นดูว่า ExRefinery และ OilFund ต่างจากวันก่อนของชนิดเดียวกันหรือไม่"""
    if eppo_df is None or eppo_df.empty:
        return None
    d = eppo_df.copy()
    if "Source" in d.columns and d["Source"].astype(str).str.upper().isin(["FRESH", "CARRIED"]).any():
        fresh = d[d["Source"].astype(str).str.upper() == "FRESH"]
        legacy = d[~d["Source"].astype(str).str.upper().isin(["FRESH", "CARRIED"])]
    else:
        fresh, legacy = d.iloc[0:0], d
    dates = []
    if not fresh.empty:
        dates.append(fresh["Date"].max())
    if not legacy.empty and "ExRefinery" in legacy.columns:
        for _, g in legacy.groupby("Oil Type"):
            g = g.sort_values("Date")
            cols = [c for c in ("ExRefinery", "OilFund") if c in g.columns]
            changed = g[(g[cols].diff().abs() > 1e-6).any(axis=1)]
            if not changed.empty:
                dates.append(changed["Date"].max())
    return max(dates) if dates else None


def freshness_warnings(today, df_eppo, df_mops, df_wti, df_oilfund) -> list[str]:
    out = []

    def age(d):
        return (today - pd.Timestamp(d).date()).days if d is not None and not pd.isna(d) else None

    fresh = eppo_fresh_date(df_eppo)
    a = age(fresh)
    if a is None or a >= STALE_DAYS:
        when = fresh.strftime("%d/%m/%Y") if fresh is not None and not pd.isna(fresh) else "ไม่ทราบ"
        out.append(f"ข้อมูล EPPO (ราคาหน้าโรงกลั่น/กองทุน/ค่าการตลาด) ที่เป็นค่าจริงล่าสุดคือ {when} — วันหลังจากนั้นเป็นค่าลากมา การ์ดด้านล่างอาจไม่ตรงปัจจุบัน")
    for label, df in (("MOPS", df_mops), ("WTI", df_wti), ("ฐานะกองทุน (Oil Fund Sustainability)", df_oilfund)):
        if df is None or df.empty or "Date" not in df.columns:
            continue
        d = pd.to_datetime(df["Date"], errors="coerce").max()
        a = age(d)
        if a is not None and a >= STALE_DAYS:
            out.append(f"{label} ล่าสุด {d.strftime('%d/%m/%Y')} (เก่า {a} วัน)")
    return out


def fund_risk_level(fund_snap: dict):
    """ระดับความเสี่ยงของกองทุน: danger = เงินสดหมด/ติดลบ, สถานะ CRITICAL หรือพยุงได้ไม่เกิน 20 วัน ; warning = ไม่เกิน 35 วัน ; อื่น ๆ ไม่แสดงแถบ"""
    if not fund_snap:
        return None
    cash, runway = fund_snap.get("cash"), fund_snap.get("runway")
    status = str(fund_snap.get("status") or "").upper()
    if (cash is not None and cash < 0) or (runway is not None and runway <= 20) or status == "CRITICAL":
        return "danger"
    if runway is not None and runway <= 35:
        return "warning"
    return None


def _notice_change(label, old, new):
    old, new = safe_float(old), safe_float(new)
    if old is None or new is None:
        return None
    d = new - old
    if abs(d) < 0.005:
        return f"{label} {new:+.2f} บาท/ลิตร (ไม่เปลี่ยน)"
    hint = "เก็บเข้ากองทุนเพิ่ม / ลดเงินอุดหนุน → แรงดันให้ราคาหน้าปั๊มขึ้น" if d > 0 else "ลดการเก็บ / เพิ่มเงินอุดหนุน → แรงดันให้ราคาหน้าปั๊มลง"
    return f"{label} {old:+.2f} → {new:+.2f} บาท/ลิตร ({d:+.2f}: {hint})"


def build_fund_risk_banner(fund_snap: dict, fund_feed: pd.DataFrame) -> str:
    """แถบเตือนความเสี่ยงกองทุนบนหน้าแรก: เงินสด/จำนวนวันที่พยุงได้ + ประกาศ กบน. ล่าสุดจากกลุ่ม Jobber
    เป็นข้อมูลประกอบการตัดสินใจเท่านั้น ไม่ได้ใส่ในโมเดลความน่าจะเป็น (ทดสอบแล้วไม่ช่วยให้แม่นขึ้น)"""
    level = fund_risk_level(fund_snap)
    if level is None:
        return ""
    cash, runway = fund_snap.get("cash"), fund_snap.get("runway")
    if cash is not None and cash < 0:
        headline = "กองทุนน้ำมันเงินสดหมดแล้ว (ติดลบ) — ความเสี่ยงที่รัฐต้องปรับอัตราเก็บ/ลดเงินอุดหนุนสูง"
    elif runway is not None:
        headline = f"กองทุนน้ำมันพยุงราคาได้อีกประมาณ {runway:.1f} วัน — ความเสี่ยงปรับอัตราสูงขึ้น"
    else:
        headline = "กองทุนน้ำมันอยู่ในสถานะเสี่ยง"
    facts = [f"เงินสดคงเหลือ {fmt_num(cash, 0)} ล้านบาท", f"เงินอุดหนุน {fmt_num(fund_snap.get('subsidy'), 0)} ล้านบาท/วัน",
             f"ฐานะสุทธิ {fmt_num(fund_snap.get('balance'), 0)} ล้านบาท"]
    notice_html = "<div class='muted'>ยังไม่พบประกาศ กบน. จากกลุ่ม Jobber</div>"
    t = S.add_ts(fund_feed) if fund_feed is not None else pd.DataFrame()
    if not t.empty and "effective_date" in t.columns:
        last = t.iloc[-1]
        eff = pd.to_datetime(last.get("effective_date"), errors="coerce")
        parts = [_notice_change("ดีเซล", last.get("diesel_old"), last.get("diesel_new")),
                 _notice_change("เบนซิน/แก๊สโซฮอล์", last.get("benzene_old"), last.get("benzene_new"))]
        parts = [p for p in parts if p]
        head = f"ประกาศ กบน. ล่าสุด (โพสต์ {last['ts'].strftime('%d/%m %H:%M')}"
        head += f", มีผล {eff.strftime('%d/%m/%Y')})" if not pd.isna(eff) else ")"
        if parts:
            notice_html = f"<div><strong>{html_escape(head)}</strong><ul>" + "".join(f"<li>{html_escape(p)}</li>" for p in parts) + "</ul></div>"
    return (f"<div class='fund-risk fund-risk-{level}'><strong>{html_escape(headline)}</strong>"
            f"<div class='muted' style='margin:6px 0'>{html_escape(' | '.join(facts))}</div>{notice_html}</div>")


def _risk_bar(p_rise, p_fall):
    """แท่งสีสรุป "ถ้ารอ 1 วัน ราคา ณ เวลาถึงคลังจะ": แพงขึ้น / ใกล้เดิม / ถูกลง (ปรับให้รวมไม่เกิน 100%)"""
    r, f = max(0.0, min(1.0, p_rise)), max(0.0, min(1.0, p_fall))
    if r + f > 1:
        r, f = r / (r + f), f / (r + f)
    mid = 1 - r - f
    return (f"<div class='pbar'><span class='pbar-up' style='width:{r * 100:.0f}%'>{r:.0%}</span>"
            f"<span class='pbar-mid' style='width:{mid * 100:.0f}%'>{mid:.0%}</span>"
            f"<span class='pbar-dn' style='width:{f * 100:.0f}%'>{f:.0%}</span></div>"
            "<div class='pbar-legend'><span class='lg-up'>แพงขึ้น</span><span>ใกล้เดิม</span><span class='lg-dn'>ถูกลง</span></div>")


def _dispatch_card(label_th, d):
    color = d.get("color", "secondary")
    q = d.get("quality") or {}
    quality = ""
    if q.get("grade"):
        quality = f"<span class='qtag qtag-{ {'ดี': 'good', 'พอใช้': 'ok', 'อ่อน': 'weak'}[q['grade']] }'>ความเชื่อมั่น: {q['grade']}</span>"
    body = ""
    if d.get("p_rise") is not None:
        body += "<div class='muted' style='margin-top:10px'>ถ้ารอไปอีก 1 วัน ราคา ณ เวลาที่รถถึงคลังจะ…</div>" + _risk_bar(d["p_rise"], d["p_fall"])
    details = "".join(f"<li>{html_escape(x)}</li>" for x in d.get("detail", []))
    reasons = "".join(f"<li>{html_escape(x)}</li>" for x in d.get("reasons", []))
    ev = d.get("evidence")
    evidence = ""
    if ev and ev.get("source") == "backtest":
        evidence = (f"<div class='muted'>ย้อนหลัง {ev['n_days']} วัน (ช่วงที่ไม่เคยใช้ปรับเกณฑ์): เมื่อระบบแนะนำแบบนี้ ราคาแพงขึ้นจริง {ev['rise_rate']:.0%} "
                    f"ถูกลงจริง {ev['fall_rate']:.0%} เฉลี่ย {ev['avg_move']:+.2f} บาท/ลิตร</div>")
    note = f"<div class='muted'>{html_escape(d['arrival_note'])}</div>" if d.get("arrival_note") else ""
    nxt = f"<div class='muted'>{html_escape(d['next_check'])}</div>" if d.get("next_check") else ""
    return f"""<div class="dispatch-card">
      <div class="dispatch-head"><span class="dispatch-fuel">{html_escape(label_th)}</span><span class="badge text-bg-{color}">{html_escape(d.get('action', ''))}</span>{quality}</div>
      <div class="dispatch-headline">{html_escape(d.get('headline', ''))}</div>
      <ul>{details}</ul>{body}
      <details><summary>เหตุผลจากสัญญาณ</summary><ul>{reasons}</ul></details>
      {evidence}{note}{nxt}
    </div>"""


def build_alert_readiness(log: pd.DataFrame, summary, lead: int) -> str:
    """ความพร้อมแจ้งเตือนต่อชนิด/ช่วง (เกณฑ์ใน engine.alert_status) — แสดงเฉพาะสถานะ ไม่มีการส่งข้อความใด ๆ"""
    names = {"D15": "บ่าย 15:30", "D20": "ค่ำ 20:30"}
    items = []
    for fk in FUEL_CONFIG:
        fuel = engine.FUEL_KEY[fk]
        for win, label in names.items():
            s = engine.alert_status(log, summary, fuel, win, lead)
            ok = [{"URGENT": "เร่ง", "WAIT": "ชะลอ"}[c] for c, v in s["eligible"].items() if v]
            state = ("พร้อมแจ้งเตือน: " + "/".join(ok)) if ok else ("ยังไม่พร้อม: " + (s["reasons"][0] if s["reasons"] else "-"))
            items.append(f"<li>{html_escape(FUEL_CONFIG[fk]['label_th'])} ช่วง{label} — {html_escape(state)}</li>")
    return ("<div class='muted' style='margin-top:8px'><strong>ความพร้อมแจ้งเตือน</strong> (ตอนนี้ยังไม่ส่งข้อความใด ๆ เข้ากลุ่ม ; "
            f"ต้องมีผลจริง ≥ {engine.ALERT_MIN_DAYS} วันและผ่านเกณฑ์ความแม่นยำก่อน)<ul>{''.join(items)}</ul></div>")


def build_dispatch_live(log: pd.DataFrame, lead: int, summary=None) -> str:
    """ผลทดสอบเดินหน้าของแผนส่งรถ (นับจากวันที่เริ่มบันทึก) — ยังไม่ครบ MIN_DISPATCH_LIVE วันจะบอกว่ากำลังสะสม ไม่สรุปว่าแม่น"""
    names = {"URGENT": "เร่งส่ง", "GRADUAL": "ทยอยส่ง", "NORMAL": "ส่งปกติ", "WAIT": "ชะลอ"}
    rows, any_data = [], False
    for fk in FUEL_CONFIG:
        fuel = engine.FUEL_KEY[fk]
        st = engine.dispatch_live_stats(log, fuel, lead)
        if not st:
            rows.append(f"<tr><td>{html_escape(FUEL_CONFIG[fk]['label_th'])}</td><td colspan='4'>ยังไม่มีบันทึก</td></tr>")
            continue
        any_data = True
        if st["n"] < engine.MIN_DISPATCH_LIVE:
            rows.append(f"<tr><td>{html_escape(FUEL_CONFIG[fk]['label_th'])}</td><td colspan='4'>กำลังสะสมผล: มีผลลัพธ์แล้ว {st['n']} วัน "
                        f"(รอผลอีก {st['pending']}) ต้องครบ {engine.MIN_DISPATCH_LIVE} วันก่อนสรุป</td></tr>")
            continue
        acts = " ".join(f"{names[c]} {a['n']} ครั้ง (แพงขึ้นจริง {a['rise_rate']:.0%}/ถูกลงจริง {a['fall_rate']:.0%})"
                        for c, a in st["by_action"].items() if c in names)
        rows.append(f"<tr><td>{html_escape(FUEL_CONFIG[fk]['label_th'])}</td><td>{st['n']} วัน</td><td>{html_escape(acts)}</td>"
                    f"<td>{st['avg_gain_per_day']:+.3f}</td><td>{st['total_gain']:+.1f}</td></tr>")
    note = "" if any_data else "<div class='muted'>เริ่มบันทึกคำแนะนำจริงแล้ว ผลลัพธ์แรกจะออกหลังเวลาถึง+24 ชม. ของคำแนะนำแรก</div>"
    return f"""<div class='dispatch-live'><h3>ผลทดสอบจริง (เริ่มนับตั้งแต่เริ่มบันทึก, ระยะเดินทาง {lead} ชม.)</h3>{note}
      <div style='overflow-x:auto'><table class='acc-table'><thead><tr><th>ชนิด</th><th>จำนวนวันที่มีผล</th><th>คำแนะนำ → ผลจริง</th>
      <th>กำไรเฉลี่ย บาท/ลิตร/วัน</th><th>กำไรรวม บาท/ลิตร</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
      <div class='muted'>กำไร = ถ้าเร่งส่งแล้วราคาที่เวลาถึงถูกกว่าถ้ารอ 1 วัน หรือชะลอแล้วราคาถูกลง ; ส่งปกติ = 0 (เทียบกับทำตามแผนเดิมทุกวัน)</div>
      {build_alert_readiness(log, summary, lead)}</div>"""


def build_dispatch_section(by_lead: dict, default_lead: int, asof, live_html: str = "") -> str:
    """สรุปสำหรับพนักงานจัดซื้อ: ส่งรถตอนนี้หรือรอ — เลือกระยะเดินทางได้ (ค่าตั้งต้น default_lead ชม.)
    by_lead = {lead: {fuel_key: decision}}"""
    if not by_lead:
        return ""
    leads = sorted(by_lead)
    radios = "".join(
        f"<label class='lead-opt'><input type='radio' name='lead' value='{l}'{' checked' if l == default_lead else ''}"
        f" onchange=\"document.querySelectorAll('.lead-panel').forEach(p=>p.hidden=p.dataset.lead!==this.value)\"> {l} ชม.</label>"
        for l in leads)
    panels = ""
    for l in leads:
        cards = "".join(_dispatch_card(FUEL_CONFIG[fk]["label_th"], by_lead[l][fk]) for fk in FUEL_CONFIG if fk in by_lead[l])
        panels += f"<div class='lead-panel' data-lead='{l}'{'' if l == default_lead else ' hidden'}><div class='dispatch-grid'>{cards}</div></div>"
    return f"""<section class="dispatch">
      <h2>สรุปสำหรับทีมจัดซื้อ: ส่งรถตอนนี้ดีไหม?</h2>
      <p class="muted">ราคาที่ซื้อได้จริงคือราคา ณ เวลารถถึงคลัง (เวลาออก + ระยะเดินทาง) ไม่ใช่ราคาตอนนี้ — ระบบจึงประเมินราคาที่เวลาถึง
      เทียบกับถ้าออกรถช้าไปอีก 1 วัน &nbsp;|&nbsp; ประเมิน ณ {asof.strftime('%d/%m %H:%M')}</p>
      <div class="lead-pick">ระยะเดินทางสาขา → คลัง: {radios}</div>
      {panels}
      {live_html}
      <p class="muted">ข้อควรรู้: ความแม่นยำสูงสุดช่วงหลังประกาศกองทุนและ MOPS ปิดตลาด (~20:00) ; ช่วงเช้า–บ่ายสัญญาณน้อย ระบบจะบอกตรง ๆ ว่าไม่มีสัญญาณ
      ตัวเลขย้อนหลังมาจากการทดสอบกับข้อมูลในอดีต ส่วน "ผลทดสอบจริง" ด้านบนนับจากคำแนะนำที่ออกจริงตั้งแต่เริ่มบันทึก ห้ามใช้แทนการตัดสินใจของผู้จัดการ</p>
    </section>"""


def _outlook_horizon_html(h):
    gmap = {"สูง": "good", "กลาง": "ok", "ต่ำ": "weak"}
    color = {"UP_STRONG": "danger", "UP": "danger", "DOWN": "success", "DOWN_STRONG": "success", "UNCLEAR": "secondary"}[h["tier"]]
    size = f"<div class='muted'>{html_escape(h['size'])}</div>" if h.get("size") else ""
    ev = engine.outlook_evidence_text(h)
    evidence = f"<div class='muted'>{html_escape(ev)}</div>" if ev else ""
    cls = "outlook-main" if h["hours"] == 24 else "outlook-sub"
    return (f"<div class='{cls}'><div class='dispatch-head'><span class='badge text-bg-{color}'>{html_escape(h['label'])}</span>"
            + ("" if h["tier"] == "UNCLEAR" else f"<span class='qtag qtag-{gmap[h['grade']]}'>ความเชื่อมั่น: {h['grade']}</span>") + "</div>"
            f"<div class='dispatch-headline'>{html_escape(h['headline'])}</div>{size}{evidence}"
            f"<div class='muted'>ครอบคลุมถึง {h['until'].strftime('%d/%m %H:%M')}</div></div>")


def _outlook_card(label_th, d):
    if not d.get("horizons"):
        return (f"<div class='dispatch-card'><div class='dispatch-head'><span class='dispatch-fuel'>{html_escape(label_th)}</span></div>"
                f"<div class='dispatch-headline'>{html_escape(d.get('note') or 'ยังไม่มีข้อมูลพอ')}</div></div>")
    note = f"<div class='muted'>* {html_escape(d['note'])}</div>" if d.get("note") else ""
    return (f"<div class='dispatch-card'><div class='dispatch-head'><span class='dispatch-fuel'>{html_escape(label_th)}</span></div>"
            + "".join(_outlook_horizon_html(h) for h in d["horizons"]) + note + "</div>")


def build_outlook_live(log: pd.DataFrame) -> str:
    rows = []
    for fk, cfg in FUEL_CONFIG.items():
        for hz in (24, 48):
            st = engine.outlook_live_stats(log, engine.FUEL_KEY[fk], hz)
            if not st:
                continue
            parts = []
            for tier, x in st["tiers"].items():
                lab = M.OUTLOOK_TIERS[tier]
                parts.append(f"{lab} {x['n']} ครั้ง" + (f" (ถูกทิศ {x['right10']:.0%} ผิดทิศ {x['wrong10']:.0%})" if "right10" in x else ""))
            rows.append(f"<tr><td>{html_escape(cfg['label_th'])}</td><td>{hz} ชม.</td><td>{st['n_days']}</td><td>{html_escape(' / '.join(parts))}</td></tr>")
    if not rows:
        return ("<div class='dispatch-live'><h3>ผลทดสอบจริง</h3><div class='muted'>เริ่มบันทึกแนวโน้มรอบ 20:30 แล้ว "
                "ผลแรกออกหลังครบ 24 ชม. ของรอบแรก — ยังไม่ส่งข้อความใด ๆ เข้ากลุ่ม</div></div>")
    return ("<div class='dispatch-live'><h3>ผลทดสอบจริง (นับจากแนวโน้มที่ออกจริง)</h3><div style='overflow-x:auto'><table class='acc-table'>"
            "<thead><tr><th>ชนิด</th><th>ขอบฟ้า</th><th>วันที่มีผล</th><th>ระดับคำแนะนำ → ผลจริง</th></tr></thead><tbody>"
            + "".join(rows) + "</tbody></table></div><div class='muted'>ยังไม่ส่งข้อความใด ๆ เข้ากลุ่ม ต้องผ่านเกณฑ์ที่ตกลงกันก่อน</div></div>")


def build_outlook_section(outlook: dict, asof, live_html: str = "") -> str:
    if not outlook:
        return ""
    cards = "".join(_outlook_card(FUEL_CONFIG[fk]["label_th"], d) for fk, d in outlook.items())
    return f"""<section class="dispatch">
      <h2>แนวโน้มราคาตลาด 1–2 วันข้างหน้า</h2>
      <p class="muted">ข้อความกลางสำหรับทุกสาขา ไม่ระบุเวลาออกรถ — แต่ละสาขาจัดสรรเองตามระยะทาง &nbsp;|&nbsp; ประเมินจากข้อมูล 20:30 น. (หลังประกาศกองทุนและ MOPS ปิด)
      &nbsp;|&nbsp; ดู ณ {asof.strftime('%d/%m %H:%M')}</p>
      <div class="dispatch-grid">{cards}</div>
      {live_html}
      <p class="muted">อ่านอย่างไร: "ขึ้น/ลง" = ราคาต่ำสุดของตลาดขยับตั้งแต่ 10 สต./ลิตร ; "แรง" = มีโอกาสขยับตั้งแต่ 40 สต./ลิตร ;
      "ยังไม่ชัดเจน" ไม่ได้แปลว่าทรงตัว แต่ระบบไม่ฟันธง (ย้อนหลังทายทรงตัวไม่แม่น) ; ความเชื่อมั่นมาจากผลทดสอบย้อนหลังที่ไม่เคยใช้จูน
      และยังไม่ได้พิสูจน์กับข้อมูลจริงล่วงหน้า — ใช้ประกอบการตัดสินใจ ไม่ใช่คำสั่ง</p>
    </section>"""


def build_outlook_mini(outlook: dict) -> str:
    """แนวโน้ม 24/48 ชม. แบบย่อ สำหรับมุมซ้ายของส่วนหัว (รายละเอียดเต็มอยู่ในการ์ดแนวโน้มด้านล่าง)"""
    rows = []
    for fk, d in (outlook or {}).items():
        if not d.get("horizons"):
            continue
        cells = []
        for h in d["horizons"]:
            conf = "" if h["tier"] == "UNCLEAR" else f" {engine.GRADE_EMOJI[h['grade']]}"
            cells.append(f"<span>{h['hours']} ชม. {engine.TIER_EMOJI[h['tier']]} {html_escape(h['label'])}{conf}</span>")
        rows.append(f"<div class='om-row'><span class='om-fuel'>{html_escape(FUEL_CONFIG[fk]['label_th'])}</span>{''.join(cells)}</div>")
    if not rows:
        return ""
    ref = next(d["ref"] for d in outlook.values() if d.get("horizons"))
    return (f"<div class='om'><div class='om-title'>แนวโน้มราคาตลาด (ประเมินจากข้อมูล {ref.strftime('%d/%m %H:%M')})</div>{''.join(rows)}"
            "<div class='om-note'>🟢 สูง 🟡 กลาง 🔴 ต่ำ = ความเชื่อมั่น · ยังอยู่ระหว่างทดสอบความแม่นจริง · รายละเอียดด้านล่าง</div></div>")


def write_outlook_files(outlook: dict, version: str = "", snap_path: str = "outlook_snapshot.json", pub_path: str = "outlook.json") -> None:
    """snapshot ให้ log_outlook.py บันทึกลง OutlookLog ; outlook.json = ข้อความ + ระดับ (ไม่มีข้อมูลดิบ) ให้ส่งต่อเข้าไลน์/หน้าอื่น"""
    clean = lambda v: None if v is None or (isinstance(v, float) and np.isnan(v)) else round(float(v), 4)
    snap, pub = [], {}
    for fk, d in outlook.items():
        for h in d.get("horizons", []):
            snap.append({"asof": d["asof"].strftime("%Y-%m-%d %H:%M"), "ref": d["ref"].strftime("%Y-%m-%d %H:%M"), "fuel": d["fuel"],
                         "hours": h["hours"], "tier": h["tier"], "p_rise10": clean(h["p"]["r10"]), "p_fall10": clean(h["p"]["f10"]),
                         "p_rise40": clean(h["p"]["r40"]), "p_fall40": clean(h["p"]["f40"]), "grade": h["grade"], "model_version": version})
        pub[d["fuel"]] = {"ref": d["ref"].strftime("%Y-%m-%d %H:%M"), "note": d.get("note"),
                          "horizons": [{"hours": h["hours"], "tier": h["tier"], "label": h["label"], "grade": h["grade"],
                                        "until": h["until"].strftime("%Y-%m-%d %H:%M")} for h in d.get("horizons", [])]}
    write_text(snap_path, json.dumps(snap, ensure_ascii=False, indent=1))
    write_text(pub_path, json.dumps({"generated": datetime.now().strftime("%Y-%m-%d %H:%M"), "text": engine.outlook_text(outlook), "fuels": pub},
                                    ensure_ascii=False, indent=1))


def build_accuracy_section(summary, decision_log) -> str:
    """ตารางความแม่นยำของคำแนะนำ: สถิติ backtest (ช่วงที่ไม่เคยใช้จูนเกณฑ์) และสถิติสดจาก DecisionLog เมื่อมีพอ"""
    if not summary:
        return ""
    names = {"URGENT": "เร่งซื้อ", "GRADUAL": "ทยอยซื้อ", "NORMAL": "ซื้อปกติ", "WAIT": "ชะลอ"}
    rows = []
    for key, r in summary.get("results", {}).items():
        fuel, phase = key.split("_")
        for code, a in r["holdout_policy"]["actions"].items():
            live = engine.live_stats(decision_log, fuel, phase, code)
            live_txt = f"{live['rise_rate']:.0%} / {live['fall_rate']:.0%} (n={live['n_days']})" if live else "-"
            rows.append(
                f"<tr><td>{engine.FUEL_TH[fuel]}</td><td>{'บ่าย 15:30' if phase == 'PM' else 'ค่ำ 20:30'}</td><td>{names[code]}</td>"
                f"<td>{a['share']:.0%}</td><td>{a['rise_rate']:.0%} / {a['fall_rate']:.0%}</td><td>{a['avg_next_minus_today']:+.2f}</td><td>{live_txt}</td></tr>")
    return f"""
    <section class="news-panel">
      <div class="news-head"><div class="eyebrow">ACCURACY</div><h2>ความแม่นยำของคำแนะนำ</h2></div>
      <div class="muted" style="margin-bottom:8px;">ราคาต่ำสุดของตลาดพรุ่งนี้ 15:30 เทียบวันนี้ 15:30 — ขึ้น/ลง หมายถึงขยับเกิน {M.RISE_THB:.2f} บาท/ลิตร
        (backtest เมื่อ {html_escape(summary.get('generated', '-'))} ช่วงทดสอบ {next(iter(summary['results'].values()))['holdout_policy']['n_days']} วันที่ไม่เคยใช้จูนเกณฑ์)</div>
      <div style="overflow-x:auto"><table class="acc-table">
        <thead><tr><th>ชนิด</th><th>ช่วง</th><th>คำแนะนำ</th><th>สัดส่วนวัน</th><th>ราคาขึ้นจริง / ลงจริง (backtest)</th><th>ขยับเฉลี่ย</th><th>ขึ้น / ลง (ใช้งานจริง)</th></tr></thead>
        <tbody>{''.join(rows)}</tbody></table></div>
    </section>
    """


# ============================================================
# HTML BUILDERS
# ============================================================
def metric_card(title, value, unit, delta=None, theme="default"):
    badge = ""
    if delta is not None:
        badge = (
            f'<span class="badge text-bg-{sign_badge_class(delta)}">Δ {fmt_change(delta)}</span>'
            if isinstance(delta, (int, float)) and not pd.isna(delta)
            else f'<span class="badge text-bg-secondary">{delta}</span>'
        )
    return f"""
    <div class="metric-card {theme}">
        <div class="metric-title">{title}</div>
        <div class="metric-value">{value}</div>
        <div class="metric-unit">{unit}</div>
        <div class="metric-badge">{badge}</div>
    </div>
    """


def snapshot_card(title, value, subtext, tone="neutral", badge_text=""):
    badge_html = f'<div class="tone-badge badge-{tone}">{html_escape(badge_text)}</div>' if badge_text else ""
    return f"""
    <div class="snapshot-card tone-{tone}">
      <div class="snapshot-top">
        <div class="k">{html_escape(title)}</div>
        {badge_html}
      </div>
      <div class="v">{value}</div>
      <div class="sub">{html_escape(subtext)}</div>
    </div>
    """


def build_fuel_section(result: dict) -> str:
    color_map = {
        "success": "#198754",
        "warning": "#ffc107",
        "danger": "#dc3545",
        "primary": "#0d6efd",
        "secondary": "#6c757d",
    }
    border = color_map.get(result["color"], "#0d6efd")

    reasons_html = "".join(f"<li>{html_escape(r)}</li>" for r in result["reasons"])
    date_text = result["date"].strftime("%d/%m/%Y") if result["date"] else "-"

    d = result["decision"]
    if d["p_rise"] is not None:
        decision_line = (f"ช่วงตัดสินใจ: {'ค่ำ (หลังรู้ MOPS ปิด/กองทุน)' if d['phase'] == 'EVE' else 'บ่าย (ก่อน MOPS ปิด)'}"
                         f" | โอกาสราคาขึ้นข้ามคืน {d['p_rise']:.0%} | โอกาสราคาลง {d['p_fall']:.0%}"
                         f" | ณ {d['asof'].strftime('%d/%m %H:%M')}")
    else:
        decision_line = f"ณ {d['asof'].strftime('%d/%m %H:%M')}"
    ev = d.get("evidence")
    if ev:
        src = "ใช้งานจริง" if ev["source"] == "live" else "backtest"
        evidence_line = (f"หลักฐาน ({src}, {ev['n_days']} วัน): เมื่อระบบแนะนำแบบนี้ ราคาขึ้นจริง {ev['rise_rate']:.0%} "
                         f"ลงจริง {ev['fall_rate']:.0%} ขยับเฉลี่ย {ev['avg_move']:+.2f} บาท/ลิตร")
    else:
        evidence_line = ""
    oilfund_latest = safe_float(result["oilfund"].get("latest"))
    oilfund_theme = "danger" if oilfund_latest is not None and oilfund_latest < 0 else "default"

    return f"""
    <section class="fuel-panel" style="border-top: 6px solid {border};">
        <div class="fuel-head">
            <div>
                <div class="eyebrow">คำแนะนำการจัดซื้อ</div>
                <h2>{html_escape(result['label_th'])}</h2>
                <div class="muted">ข้อมูลอ้างอิงล่าสุด: {date_text}</div>
            </div>
            <div class="action-box action-{result['color']}">
                <div class="action-label">Action</div>
                <div class="action-value">{html_escape(result['action'])}</div>
            </div>
        </div>

            <div class="summary-box">
            <div class="summary-title">บทสรุปเชิงวิเคราะห์</div>
            <div class="summary-text">{html_escape(result['summary'])}</div>
            <div class="muted" style="margin-top:8px;">{decision_line}</div>
            <ul>{reasons_html}</ul>
            <div class="muted">{evidence_line}</div>
        </div>

        <div class="metric-grid">
            {metric_card('ค่าการตลาด', fmt_num(result['mm'].get('latest'), 2), 'บาท/ลิตร', result['mm'].get('chg_1d'), 'primary')}
            {metric_card('จุดเฝ้าระวัง', fmt_num(result['threshold'], 2), 'บาท/ลิตร')}
            {metric_card('ราคาหน้าโรงกลั่น', fmt_num(result['ex'].get('latest'), 4), 'บาท/ลิตร', result['ex'].get('chg_1d'))}
            {metric_card('เงินกองทุนน้ำมัน', fmt_num(result['oilfund'].get('latest'), 2), 'บาท/ลิตร', result['oilfund'].get('chg_1d'), oilfund_theme)}
            {metric_card('ราคาขายปลีก', fmt_num(result['retail'].get('latest'), 2), 'บาท/ลิตร', result['retail'].get('chg_1d'), 'success')}
            {metric_card('ราคาขายส่ง', fmt_num(result['wholesale'].get('latest'), 2), 'บาท/ลิตร', result['wholesale'].get('chg_1d'))}
        </div>
    </section>
    """


def build_news_section(news_df: pd.DataFrame, max_items: int = 1) -> str:
    if news_df is None or news_df.empty:
        return """
        <section class="news-panel">
          <div class="news-head">
            <div class="eyebrow">NEWS</div>
            <h2>ข่าวและประเด็นสำคัญ</h2>
          </div>
          <div class="news-empty">ยังไม่มีข้อมูลข่าวในชีต NEWS</div>
        </section>
        """

    rows = []
    for _, row in news_df.head(max_items).iterrows():
        ts = row.get("Timestamp")
        ctx = row.get("Context", "")
        ts_text = ts.strftime("%d/%m/%Y %H:%M") if pd.notna(ts) else "-"
        rows.append(f"""
        <div class="news-item">
          <div class="news-time">{ts_text}</div>
          <div class="news-content">{nl2br(ctx)}</div>
        </div>
        """)

    items_html = "\n".join(rows)
    return f"""
    <section class="news-panel">
      <div class="news-head">
        <div class="eyebrow">NEWS</div>
        <h2>ข่าวและประเด็นสำคัญ</h2>
      </div>
      <div class="news-list">
        {items_html}
      </div>
    </section>
    """




def build_html(results: list[dict], wti_snap: dict, fund_snap: dict, news_df: pd.DataFrame, warnings: list[str], accuracy_html: str = "", fund_feed: pd.DataFrame = None, dispatch_html: str = "", trend_snaps: dict = None, outlook_mini_html: str = "") -> str:
    updated_at = datetime.now(TZ).strftime("%d/%m/%Y %H:%M")
    sections = "\n".join(build_fuel_section(r) for r in results)
    news_html = build_news_section(news_df)

    fund_date = fund_snap.get("date")
    fund_date_text = fund_date.strftime("%d/%m/%Y") if fund_date else "-"

    g95_result = next((r for r in results if r.get("fuel_key") == "GASOHOL95"), {})
    ds_result = next((r for r in results if r.get("fuel_key") == "DIESEL"), {})

    g95_mops = g95_result.get("mops", {})
    ds_mops = ds_result.get("mops", {})

    wti_tone = classify_market_tone(wti_snap.get("chg_1d"), wti_snap.get("chg_3d"))
    g95_mops_tone = classify_market_tone(g95_mops.get("chg_1d"), g95_mops.get("chg_3d"))
    ds_mops_tone = classify_market_tone(ds_mops.get("chg_1d"), ds_mops.get("chg_3d"))
    balance_tone = classify_balance_tone(fund_snap.get("balance"))
    runway_tone = classify_runway_tone(fund_snap.get("runway"))

    trend_snaps = trend_snaps or {}
    snapshot_html = f"""
    <div class="snapshot-grid">
      {snapshot_card(
          "WTI",
          fmt_num(wti_snap.get('latest'), 2),
          f"Δ1D {fmt_change(wti_snap.get('chg_1d'), 2)} | Δ3D {fmt_change(wti_snap.get('chg_3d'), 2)}",
          wti_tone["tone"],
          wti_tone["label"]
      )}
      {snapshot_card(
          "MOPS G95",
          fmt_num(g95_mops.get('latest'), 2),
          f"USD/BBL | Δ1D {fmt_change(g95_mops.get('chg_1d'), 2)} | Δ3D {fmt_change(g95_mops.get('chg_3d'), 2)}",
          g95_mops_tone["tone"],
          g95_mops_tone["label"]
      )}
      {snapshot_card(
          "MOPS ดีเซล",
          fmt_num(ds_mops.get('latest'), 2),
          f"USD/BBL | Δ1D {fmt_change(ds_mops.get('chg_1d'), 2)} | Δ3D {fmt_change(ds_mops.get('chg_3d'), 2)}",
          ds_mops_tone["tone"],
          ds_mops_tone["label"]
      )}
      {trend_card_html("Trend Mops ดีเซล", trend_snaps.get("B7"))}
      {trend_card_html("Trend Mops เบนซิน (G95)", trend_snaps.get("G95"))}
      {snapshot_card(
          "ฐานะกองทุนน้ำมันสุทธิ",
          fmt_num(fund_snap.get('balance'), 0),
          f"ล้านบาท | วันที่ {fund_date_text}",
          balance_tone["tone"],
          balance_tone["label"]
      )}
      {snapshot_card(
          "กองทุนน้ำมันจะพยุงราคาได้อีก",
          fmt_num(fund_snap.get('runway'), 1),
          f"วัน | เงินอุดหนุน {fmt_num(fund_snap.get('subsidy'), 0)} ล้านบาท/วัน",
          runway_tone["tone"],
          runway_tone["label"]
      )}
    </div>
    """

    warnings_html = ""
    if warnings:
        items = "".join(f"<li>{html_escape(w)}</li>" for w in warnings)
        warnings_html = f"<div class='warning-box'><strong>Data warnings</strong><ul>{items}</ul></div>"

    fund_risk_html = build_fund_risk_banner(fund_snap, fund_feed)

    return f"""<!DOCTYPE html>
<html lang="th">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{DASHBOARD_TITLE}</title>
  <style>
    :root {{
      --bg: #f3f6fb;
      --card: #ffffff;
      --text: #14213d;
      --muted: #6b7280;
      --line: #e5e7eb;
      --success: #198754;
      --warning: #ffc107;
      --danger: #dc3545;
      --primary: #0d6efd;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: Arial, Helvetica, sans-serif; background: var(--bg); color: var(--text); }}
    .container {{ max-width: 1260px; margin: 0 auto; padding: 24px; }}
    .hero {{ background: linear-gradient(135deg, #0f172a, #1d4ed8); color: #fff; border-radius: 20px; padding: 18px 20px; margin-bottom: 16px; }}
    .hero-grid {{ display: grid; grid-template-columns: minmax(300px, .95fr) 1.7fr; gap: 16px; align-items: start; }}
    .hero h1 {{ margin: 0 0 6px; font-size: 1.5rem; }}
    .hero p {{ margin: 0; opacity: 0.92; line-height: 1.5; font-size: .9rem; }}
    .om {{ margin-top: 12px; background: rgba(255,255,255,.1); border: 1px solid rgba(255,255,255,.18); border-radius: 14px; padding: 10px 12px; }}
    .om-title {{ font-weight: 700; font-size: .85rem; margin-bottom: 6px; }}
    .om-row {{ display: grid; grid-template-columns: 88px 1fr 1fr; gap: 8px; font-size: .88rem; padding: 4px 0; border-top: 1px solid rgba(255,255,255,.14); }}
    .om-fuel {{ font-weight: 700; }}
    .om-note {{ opacity: .8; font-size: .75rem; margin-top: 6px; }}

    .snapshot-grid {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 10px;
      align-content: start;
    }}


    .snapshot-card {{
      border-radius: 14px;
      padding: 11px 12px;
      border: 1px solid rgba(255,255,255,0.18);
      position: relative;
      overflow: hidden;
      color: #fff;
    }}

    .snapshot-card::before {{
      content: "";
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 5px;
      opacity: 0.95;
    }}

    .snapshot-top {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: flex-start;
    }}

    .snapshot-card .k {{
      font-size: .78rem;
      opacity: .92;
      font-weight: 700;
    }}

    .snapshot-card .v {{
      font-size: 1.35rem;
      font-weight: 800;
      margin-top: 6px;
      line-height: 1.1;
    }}

    .snapshot-card .sub {{
      margin-top: 4px;
      line-height: 1.4;
      font-size: .8rem;
      opacity: .96;
      white-space: pre-line;
    }}

    .tone-badge {{
      display: inline-block;
      padding: 3px 8px;
      border-radius: 999px;
      font-size: .68rem;
      font-weight: 700;
      white-space: nowrap;
    }}

    .tone-danger {{ background: linear-gradient(180deg, rgba(220,53,69,.28), rgba(220,53,69,.18)); }}
    .tone-danger::before {{ background: #ff6b6b; }}

    .tone-warning {{ background: linear-gradient(180deg, rgba(255,193,7,.26), rgba(255,193,7,.16)); }}
    .tone-warning::before {{ background: #ffd43b; }}

    .tone-success {{ background: linear-gradient(180deg, rgba(25,135,84,.28), rgba(25,135,84,.18)); }}
    .tone-success::before {{ background: #51cf66; }}

    .tone-neutral {{ background: rgba(255,255,255,0.12); }}
    .tone-neutral::before {{ background: rgba(255,255,255,0.45); }}

    .badge-danger {{ background: rgba(220,53,69,.22); color: #ffd7dc; border: 1px solid rgba(255,255,255,.16); }}
    .badge-warning {{ background: rgba(255,193,7,.22); color: #fff3bf; border: 1px solid rgba(255,255,255,.16); }}
    .badge-success {{ background: rgba(25,135,84,.22); color: #d3f9d8; border: 1px solid rgba(255,255,255,.16); }}
    .badge-neutral {{ background: rgba(255,255,255,.14); color: #eef2ff; border: 1px solid rgba(255,255,255,.16); }}

    .top-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 20px; }}
    .top-card {{ background: var(--card); border-radius: 20px; padding: 18px; box-shadow: 0 10px 24px rgba(15,23,42,.06); }}
    .top-card .label {{ font-size: .8rem; color: var(--muted); text-transform: uppercase; }}
    .top-card .value {{ font-size: 1.6rem; font-weight: 800; margin-top: 8px; }}

    .fuel-panel, .news-panel {{ background: var(--card); border-radius: 22px; padding: 24px; box-shadow: 0 10px 24px rgba(15,23,42,.06); margin-bottom: 24px; }}
    .fuel-panel {{ border-top: 6px solid var(--primary); }}
    .fuel-head, .news-head {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; flex-wrap: wrap; }}
    .fuel-head h2, .news-head h2 {{ margin: 4px 0 4px; }}
    .eyebrow {{ font-size: .78rem; color: var(--muted); letter-spacing: .06em; text-transform: uppercase; }}
    .muted {{ color: var(--muted); }}

    .action-box {{ min-width: 180px; border-radius: 18px; padding: 14px 16px; color: #fff; }}
    .action-success {{ background: var(--success); }}
    .action-warning {{ background: #d39e00; color: #1f2937; }}
    .action-danger {{ background: var(--danger); }}
    .action-primary {{ background: var(--primary); }}
    .action-secondary {{ background: #6c757d; }}
    .action-label {{ font-size: .78rem; opacity: .9; text-transform: uppercase; }}
    .action-value {{ font-size: 1.4rem; font-weight: 800; margin-top: 4px; }}

    .summary-box {{ background: #f8fafc; border: 1px solid var(--line); border-radius: 18px; padding: 16px; margin: 18px 0; }}
    .summary-title {{ font-weight: 800; margin-bottom: 8px; }}
    .summary-box ul {{ margin: 10px 0 0 18px; padding: 0; line-height: 1.6; }}
    .summary-text {{ line-height: 1.6; }}

    .metric-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }}
    .metric-card {{ background: #fff; border: 1px solid var(--line); border-radius: 18px; padding: 16px; min-height: 130px; }}
    .metric-card.primary {{ background: #eef5ff; }}
    .metric-card.success {{ background: #eefaf3; }}
    .metric-card.danger {{ background: #fff1f2; }}
    .metric-title {{ font-size: .8rem; color: var(--muted); text-transform: uppercase; min-height: 32px; }}
    .metric-value {{ font-size: 1.4rem; font-weight: 800; margin-top: 10px; }}
    .metric-unit {{ color: var(--muted); font-size: .85rem; margin-top: 4px; }}
    .metric-badge {{ margin-top: 12px; }}

    .badge {{ display: inline-block; padding: 6px 10px; border-radius: 999px; color: #fff; font-size: .8rem; }}
    .text-bg-success {{ background: var(--success); }}
    .text-bg-danger {{ background: var(--danger); }}
    .text-bg-secondary {{ background: #6c757d; }}

    .warning-box {{ background: #fff7ed; border: 1px solid #fdba74; border-radius: 18px; padding: 16px; margin-bottom: 20px; }}
    .fund-risk {{ border-radius: 18px; padding: 16px; margin-bottom: 20px; border: 1px solid; }}
    .fund-risk ul {{ margin: 6px 0 0 18px; padding: 0; }}
    .fund-risk-danger {{ background: #fef2f2; border-color: #fca5a5; }}
    .fund-risk-warning {{ background: #fffbeb; border-color: #fcd34d; }}
    .dispatch {{ background: var(--card); border: 1px solid var(--line); border-radius: 22px; padding: 20px; margin-bottom: 20px; }}
    .dispatch h2 {{ margin: 0 0 6px; }}
    .lead-pick {{ margin: 10px 0 14px; }}
    .lead-opt {{ margin-right: 16px; cursor: pointer; }}
    .dispatch-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; }}
    .dispatch-card {{ border: 1px solid var(--line); border-radius: 18px; padding: 16px; background: #fbfdff; }}
    .dispatch-head {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }}
    .dispatch-fuel {{ font-weight: 700; font-size: 1.1rem; }}
    .dispatch-headline {{ font-size: 1.2rem; font-weight: 700; margin: 10px 0 4px; line-height: 1.4; }}
    .dispatch-card ul {{ margin: 6px 0 0 18px; padding: 0; }}
    .qtag {{ font-size: .8rem; padding: 4px 8px; border-radius: 999px; border: 1px solid; }}
    .qtag-good {{ color: #15803d; border-color: #86efac; }}
    .qtag-ok {{ color: #a16207; border-color: #fcd34d; }}
    .qtag-weak {{ color: #b91c1c; border-color: #fca5a5; }}
    .outlook-main {{ margin-top: 10px; }}
    .outlook-sub {{ margin-top: 12px; padding-top: 10px; border-top: 1px dashed var(--line); opacity: .92; }}
    .outlook-sub .dispatch-headline {{ font-size: 1rem; }}
    .dispatch-old {{ margin-bottom: 20px; }}
    .dispatch-old > summary {{ cursor: pointer; font-weight: 600; margin: 8px 0; }}
    .pbar {{ display: flex; height: 26px; border-radius: 8px; overflow: hidden; margin-top: 6px; font-size: .8rem; color: #fff; }}
    .pbar span {{ display: flex; align-items: center; justify-content: center; min-width: 0; overflow: hidden; }}
    .pbar-up {{ background: #dc3545; }}
    .pbar-mid {{ background: #9ca3af; }}
    .pbar-dn {{ background: #198754; }}
    .pbar-legend {{ display: flex; justify-content: space-between; font-size: .75rem; color: var(--muted); margin-bottom: 8px; }}
    .lg-up {{ color: #dc3545; }}
    .lg-dn {{ color: #198754; }}
    .footer {{ color: var(--muted); text-align: center; padding: 16px 0 28px; }}

    .news-list {{ display: grid; gap: 14px; margin-top: 10px; }}
    .news-item {{ border: 1px solid var(--line); border-radius: 18px; padding: 16px; background: #fbfdff; }}
    .news-time {{ font-size: .9rem; font-weight: 700; color: var(--primary); margin-bottom: 10px; }}
    .news-content {{ line-height: 1.7; white-space: normal; word-break: break-word; }}
    .news-empty {{ color: var(--muted); padding-top: 10px; }}

    @media (max-width: 1100px) {{
      .metric-grid {{ grid-template-columns: repeat(2, 1fr); }}
      .top-grid {{ grid-template-columns: repeat(2, 1fr); }}
      .hero-grid {{ grid-template-columns: 1fr; }}
      .snapshot-grid {{ grid-template-columns: repeat(4, 1fr); }}
    }}
    @media (max-width: 860px) {{
      .snapshot-grid {{ grid-template-columns: repeat(3, 1fr); }}
    }}

    @media (max-width: 680px) {{
      .metric-grid {{ grid-template-columns: 1fr; }}
      .top-grid {{ grid-template-columns: 1fr; }}
      .snapshot-grid {{ grid-template-columns: repeat(2, 1fr); }}
      .om-row {{ grid-template-columns: 1fr; gap: 2px; }}
      .container {{ padding: 16px; }}
      .hero {{ padding: 20px; border-radius: 20px; }}
      .hero h1 {{ font-size: 1.5rem; }}
      .fuel-panel, .news-panel {{ padding: 18px; border-radius: 18px; }}
      .action-box {{ width: 100%; min-width: auto; }}
      .metric-card {{ min-height: auto; }}
      .news-content {{ font-size: .95rem; }}
    }}
    .acc-table {{ width:100%; border-collapse:collapse; font-size:.9rem; }}
    .acc-table th, .acc-table td {{ padding:6px 8px; border-bottom:1px solid #e5e7eb; text-align:right; white-space:nowrap; }}
    .acc-table th:nth-child(-n+3), .acc-table td:nth-child(-n+3) {{ text-align:left; }}
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <div class="hero-grid">
        <div>
          <h1>{DASHBOARD_TITLE}</h1>
          <p>ผสานสัญญาณจากกลุ่ม Jobber (Trend Mops, ประกาศปรับราคา, ประกาศกองทุน), MOPS ปิด และข้อมูลตลาด เพื่อพยากรณ์แนวโน้มราคาและสรุปให้ทีมจัดซื้อ</p>
          <p style="margin-top:4px;">อัปเดตล่าสุด: {updated_at}</p>
          {outlook_mini_html}
        </div>
        {snapshot_html}
      </div>
    </section>

    {warnings_html}

    {fund_risk_html}

    {dispatch_html}

    <section class="top-grid">
      <div class="top-card">
        <div class="label">เงินสดคงเหลือ</div>
        <div class="value">{fmt_num(fund_snap.get('cash'), 0)}</div>
        <div class="muted">ล้านบาท</div>
      </div>
      <div class="top-card">
        <div class="label">เงินอุดหนุนต่อวัน</div>
        <div class="value">{fmt_num(fund_snap.get('subsidy'), 0)}</div>
        <div class="muted">ล้านบาท/วัน</div>
      </div>
      <div class="top-card">
        <div class="label">เงินไหลเข้าต่อวัน</div>
        <div class="value">{fmt_num(fund_snap.get('collection'), 0)}</div>
        <div class="muted">ล้านบาท/วัน</div>
      </div>
      <div class="top-card">
        <div class="label">ผลกระทบสุทธิต่อวัน</div>
        <div class="value">{fmt_num(fund_snap.get('net_impact'), 0)}</div>
        <div class="muted">ล้านบาท/วัน</div>
      </div>
    </section>

    {sections}

    {accuracy_html}

    {news_html}

    <div class="footer">Fuel Procurement Decision Dashboard Vol.1</div>
  </div>
</body>
</html>
"""


# ============================================================
# MAIN
# ============================================================
FEED_REQUIRED = {
    "Entry_Intraday": ["msg_date", "msg_time", "kind", "b", "d"],
    "Entry_HeadsUp": ["msg_date", "msg_time", "provider", "direction", "scope"],
    "Entry_MopsClose": ["msg_date", "msg_time", "g95_chg", "diesel_chg"],
    "Entry_FundNotice": ["msg_date", "msg_time", "effective_date", "diesel_old", "diesel_new"],
}


def load_feed(name: str, warnings: list) -> pd.DataFrame:
    """โหลดแท็บฟีดสัญญาณ ถ้าอ่านไม่ได้/คอลัมน์ไม่ครบให้คืนตารางว่าง + แจ้งเตือน (engine จะตอบ 'ข้อมูลไม่พอ' แทนการเดา)
    หมายเหตุ: gviz ถ้าชื่อแท็บไม่มีอยู่จะส่ง 'ชีทแรก' มาแทนโดยไม่แจ้ง error จึงต้องตรวจคอลัมน์เสมอ"""
    try:
        df = load_sheet_by_name(name)
    except Exception as e:
        warnings.append(f"โหลดแท็บฟีด {name} ไม่สำเร็จ: {e}")
        return pd.DataFrame()
    missing = [c for c in FEED_REQUIRED.get(name, []) if c not in df.columns]
    if missing:
        warnings.append(f"แท็บฟีด {name} ไม่มีคอลัมน์ {', '.join(missing)} (แท็บยังไม่ถูกสร้างหรือยังไม่มีข้อมูลจาก worker) — ถือว่าฟีดนี้ว่าง")
        return pd.DataFrame()
    return df


def now_bangkok() -> pd.Timestamp:
    """เวลาตัดสินใจ — ตั้ง ASOF=YYYY-MM-DDTHH:MM เพื่อทดสอบย้อนหลังได้"""
    override = os.environ.get("ASOF")
    return pd.Timestamp(override) if override else pd.Timestamp(datetime.now(TZ).replace(tzinfo=None))


def main():
    warnings = []

    try:
        df_wti = prep_market_sheet(load_sheet(GIDS["ENTRY_WTI"]))
        df_mops = prep_mops_sheet(load_sheet(GIDS["ENTRY_MOPS"]))
        df_eppo = prep_eppo_sheet(load_sheet(GIDS["ENTRY_EPPO"]))
        df_oilfund = prep_oilfund_sheet(load_sheet(GIDS["ENTRY_OILFUND_SUSTAINABILITY"]))
        df_setting = load_sheet(GIDS["SETTING"])

        try:
            df_news = prep_news_sheet(load_sheet(GIDS["NEWS"]))
        except Exception as news_err:
            df_news = pd.DataFrame(columns=["Timestamp", "Context"])
            warnings.append(f"โหลด NEWS ไม่สำเร็จ: {news_err}")

    except Exception as e:
        write_text(
            "index.html",
            f"<html><body style='font-family:Arial;padding:24px'><h1>Dashboard Error</h1><p>{html_escape(str(e))}</p></body></html>",
        )
        raise

    settings = read_settings(df_setting)
    asof = now_bangkok()

    if df_oilfund.empty:
        warnings.append("ไม่มีข้อมูลใน Entry_OilFund_Sustainability")
    if df_eppo.empty:
        warnings.append("ไม่มีข้อมูลใน Entry_Eppo")
    if df_mops.empty:
        warnings.append("ไม่มีข้อมูลใน Entry_MOPS")
    warnings += freshness_warnings(asof.date(), df_eppo, df_mops, df_wti, df_oilfund)

    wti_snap = compute_market_snapshot(df_wti, "WTI")
    fund_snap = oilfund_snapshot(df_oilfund)

    # ---- สัญญาณจากกลุ่ม Jobber (ฟีดที่ ingest.py คัดลอกมาจาก OIL BOT) + โมเดลที่ฝึกจากข้อมูลย้อนหลัง ----
    tables = S.Tables(
        intraday=load_feed(TABS_FEED["intraday"], warnings),
        headsup=load_feed(TABS_FEED["headsup"], warnings),
        mops_close=load_feed(TABS_FEED["mops_close"], warnings),
        fund=load_feed(TABS_FEED["fund"], warnings),
    )
    try:
        decision_log = load_sheet_by_name(TABS_FEED["decision_log"])
    except Exception:
        decision_log = pd.DataFrame()            # ยังไม่มีบันทึกคำแนะนำ — ไม่ใช่ข้อผิดพลาด
    params = M.load_params()
    summary = engine.load_backtest_summary()
    if params is None:
        warnings.append("ไม่พบ model_params.json — ระบบจะไม่ให้คำแนะนำจนกว่าจะฝึกโมเดล (fit_model.py)")

    results = []
    for fuel_key, config in FUEL_CONFIG.items():
        decision = engine.decide(fuel_key, asof, tables, params, summary, decision_log)
        result = fuel_analysis(
            fuel_key=fuel_key,
            config=config,
            eppo_df=df_eppo,
            mops_df=df_mops,
            wti_snap=wti_snap,
            fund_snap=fund_snap,
            settings=settings,
            decision=decision,
        )
        if result["date"] is None:
            warnings.append(f"ไม่พบข้อมูลล่าสุดของ {config['label_th']} ใน EPPO/MOPS")
        results.append(result)

    disp_cfg = (params or {}).get("dispatch") or {}
    by_lead = {}
    for lead in disp_cfg.get("leads", []):
        by_lead[lead] = {fk: engine.decide_dispatch(fk, asof, tables, params, summary, lead) for fk in FUEL_CONFIG}
    try:
        dispatch_log = load_sheet_by_name(TABS_FEED["dispatch_log"])
        if "outcome_delta" not in dispatch_log.columns:            # gviz ส่งชีทแรกมาแทนเมื่อไม่มีแท็บ
            dispatch_log = pd.DataFrame()
    except Exception:
        dispatch_log = pd.DataFrame()
    write_dispatch_snapshot(by_lead, version=M.model_version(params))
    default_lead = disp_cfg.get("default_lead_hours", 10)
    dispatch_html = build_dispatch_section(by_lead, default_lead, asof, build_dispatch_live(dispatch_log, default_lead, summary) if by_lead else "")
    if dispatch_html:
        dispatch_html = ("<details class='dispatch-old'><summary>แผนส่งรถตามระยะเดินทางของแต่ละสาขา (แบบเดิม — ยังบันทึกผลทดสอบอยู่)</summary>"
                         + dispatch_html + "</details>")
    outlook = {fk: engine.decide_outlook(fk, asof, tables, params, summary) for fk in FUEL_CONFIG}
    try:
        outlook_log = load_sheet_by_name(TABS_FEED["outlook_log"])
        if "outcome_delta" not in outlook_log.columns:
            outlook_log = pd.DataFrame()
    except Exception:
        outlook_log = pd.DataFrame()
    write_outlook_files(outlook, M.model_version(params))
    dispatch_html = build_outlook_section(outlook, asof, build_outlook_live(outlook_log)) + dispatch_html

    trend_snaps = {fuel: trend_snapshot(tables, fuel, asof) for fuel in S.FUELS}
    html = build_html(results, wti_snap, fund_snap, df_news, warnings, build_accuracy_section(summary, decision_log), tables.fund, dispatch_html, trend_snaps, build_outlook_mini(outlook))
    write_text("index.html", html)
    write_decision_snapshot(results)
    print("Dashboard created successfully")


def write_dispatch_snapshot(by_lead: dict, path: str = "dispatch_snapshot.json", version: str = "") -> None:
    """คำแนะนำแผนส่งรถรอบนี้ (ทุกระยะเดินทาง) ให้ log_dispatch.py บันทึกลง DispatchLog"""
    clean = lambda v: None if v is None or (isinstance(v, float) and np.isnan(v)) else round(float(v), 4)
    snap = []
    for lead, per_fuel in by_lead.items():
        for fk, d in per_fuel.items():
            if d.get("action_code") == "NO_DATA":
                continue
            snap.append({"asof": d["asof"].strftime("%Y-%m-%d %H:%M"), "window": d["window"], "fuel": d["fuel"], "lead": int(lead),
                         "action_code": d["action_code"], "raw_action_code": d.get("raw_action_code"),
                         "grade": (d.get("quality") or {}).get("grade"), "p_rise": clean(d["p_rise"]), "p_fall": clean(d["p_fall"]),
                         "arrive": d["arrive"].strftime("%Y-%m-%d %H:%M"), "model_version": version})
    write_text(path, json.dumps(snap, ensure_ascii=False, indent=1))


def write_decision_snapshot(results: list[dict], path: str = "decision_snapshot.json") -> None:
    """คำแนะนำรอบนี้ในรูปที่ log_decisions.py นำไปบันทึกลง DecisionLog (main.py ไม่มี credential เขียนชีท)"""
    snap = []
    for r in results:
        d = r["decision"]
        f = d.get("features") or {}
        clean = lambda v: None if v is None or (isinstance(v, float) and np.isnan(v)) else (round(v, 4) if isinstance(v, float) else v)
        snap.append({
            "asof": d["asof"].strftime("%Y-%m-%d %H:%M"), "fuel": d["fuel"], "phase": d["phase"], "action_code": d["action_code"],
            "action": d["action"], "p_rise": clean(d["p_rise"]), "p_fall": clean(d["p_fall"]),
            "trend": clean(f.get("trend")), "up_n": f.get("up_n"), "dn_n": f.get("dn_n"),
            "close_last": clean(f.get("close_last")), "fund_next": clean(f.get("fund_next")),
        })
    write_text(path, json.dumps(snap, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
