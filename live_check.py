"""
ทวนสอบแบบสดกับราคาจริงที่กำลังเกิดขึ้น — คำพยากรณ์ที่ยังไม่ครบกำหนด (OutlookLog) เทียบกับราคาต่ำสุดของตลาดตอนนี้ (Entry_Prices)
และเวลาที่ราคาปรับจริงวันนี้เทียบกับช่วงเวลาที่การ์ดทาย

ไม่เขียนลงชีท ไม่ใช่ผลทดสอบทางการ (ผลทางการคือผลที่เติมเมื่อครบกำหนดใน OutlookLog/ForecastCardLog) — ใช้ดูว่า "ตอนนี้ราคากำลังไปทางที่ทายหรือยัง"
ฟังก์ชันเป็น pure function (รับ DataFrame/เวลาเข้ามา) เพื่อทดสอบได้
"""
from __future__ import annotations

import re

import pandas as pd

import signals as S

SMALL = 0.10          # บาท/ลิตร: ขยับ >= นี้ถือเป็นขึ้น/ลง (ตรงกับ model.OUTLOOK_SMALL)
SLOT_RE = re.compile(r"(\d{2}:\d{2})\s*[–-]\s*(\d{2}:\d{2})")


def make_book(feed: pd.DataFrame):
    need = {"provider", "warehouse", "fuel", "price", "effective"}
    if feed is None or feed.empty or not need.issubset(feed.columns):
        return None
    d = feed.copy()
    d["effective"] = pd.to_datetime(d["effective"], errors="coerce")
    try:
        return S.PriceBook(d)
    except Exception:
        return None


def tier_direction(tier: str):
    if tier and tier.startswith("UP"):
        return "ขึ้น"
    if tier and tier.startswith("DOWN"):
        return "ลง"
    return None


def active_rows(log: pd.DataFrame, now: pd.Timestamp) -> list:
    """คำพยากรณ์ที่ยังไม่ครบกำหนด : ออกแล้ว (ref <= now) และ ref+ขอบฟ้า ยังไม่ผ่าน ; 1 แถวต่อ (ชนิด, ขอบฟ้า) เอาจุดอ้างอิงล่าสุด"""
    need = {"fuel", "hours", "tier", "ref"}
    if log is None or log.empty or not need.issubset(log.columns):
        return []
    d = log.copy()
    if "backfilled" in d.columns:
        d = d[pd.to_numeric(d["backfilled"], errors="coerce").fillna(0) == 0]
    if "outcome_delta" in d.columns:
        d = d[pd.to_numeric(d["outcome_delta"], errors="coerce").isna()]
    d["ref_dt"] = pd.to_datetime(d["ref"], errors="coerce")
    d["hours_n"] = pd.to_numeric(d["hours"], errors="coerce")
    d = d.dropna(subset=["ref_dt", "hours_n"])
    d["end"] = d["ref_dt"] + pd.to_timedelta(d["hours_n"], unit="h")
    d = d[(d["ref_dt"] <= now) & (d["end"] > now)]
    if d.empty:
        return []
    d = d.sort_values("ref_dt").groupby(["fuel", "hours_n"], as_index=False).tail(1)
    return [{"fuel": str(r["fuel"]), "hours": int(r["hours_n"]), "tier": str(r["tier"]), "ref": r["ref_dt"], "end": r["end"],
             "grade": str(r.get("grade", ""))} for _, r in d.iterrows()]


def check_one(book, row: dict, now: pd.Timestamp) -> dict:
    """เทียบคำพยากรณ์หนึ่งรายการกับราคาตลาดตอนนี้ ; status: right = ไปตามที่ทายแล้ว, wrong = สวนทิศ, wait = ยังไม่ขยับถึง 10 สต., none = ไม่ฟันธง/ไม่มีราคา"""
    direction = tier_direction(row["tier"])
    p0 = book.min_at(row["fuel"], [row["ref"]]).iloc[0]
    p1 = book.min_at(row["fuel"], [now]).iloc[0]
    total_h = (row["end"] - row["ref"]).total_seconds() / 3600
    elapsed = min(max((now - row["ref"]).total_seconds() / 3600 / total_h, 0.0), 1.0)
    out = {**row, "direction": direction, "price_ref": None if pd.isna(p0) else float(p0), "price_now": None if pd.isna(p1) else float(p1),
           "elapsed": elapsed, "left_h": max((row["end"] - now).total_seconds() / 3600, 0.0), "delta": None, "status": "none"}
    if pd.isna(p0) or pd.isna(p1):
        return out
    out["delta"] = round(float(p1 - p0), 2)
    if direction is None:
        return out
    signed = out["delta"] * (1 if direction == "ขึ้น" else -1)
    out["status"] = "right" if signed >= SMALL else "wrong" if signed <= -SMALL else "wait"
    return out


def moves_today(book, fuel: str, day_start: pd.Timestamp, now: pd.Timestamp) -> list:
    """เหตุการณ์ราคาต่ำสุดของตลาดปรับตั้งแต่ต้นวัน (ขยับสะสม >= SMALL จากระดับล่าสุด) : [(เวลา, ขยับ)]"""
    times = set()
    for g in book.groups.get(fuel, []):
        t = g["effective"]
        times.update(t[(t >= day_start) & (t <= now)])
    if not times:
        return []
    times = sorted(times)
    level = book.min_at(fuel, [day_start - pd.Timedelta(minutes=1)]).iloc[0]
    series = book.min_at(fuel, times)
    out = []
    for t, v in zip(times, series.to_numpy()):
        if pd.isna(v):
            continue
        if pd.isna(level):
            level = v
            continue
        if abs(v - level) >= SMALL:
            out.append((pd.Timestamp(t), round(float(v - level), 2)))
            level = v
    return out


def top_slot(timing_text: str):
    """ช่วงเวลาแรก (ที่เป็นไปได้สูงสุด) จากข้อความ timing ของการ์ด เช่น '09:00–15:59 (80%) / ...' -> ('09:00','15:59')"""
    m = SLOT_RE.search(str(timing_text or ""))
    return (m.group(1), m.group(2)) if m else None


def timing_verdict(events: list, call: str, slot) -> dict:
    """เทียบเวลาปรับจริงในทิศที่การ์ดทายกับช่วงเวลาที่ทาย ; ยังไม่มีเหตุการณ์ = รอ"""
    if slot is None or call not in ("ลง", "ขึ้น"):
        return {"status": "none"}
    sign = -1 if call == "ลง" else 1
    ev = [(t, d) for t, d in events if d * sign > 0]
    if not ev:
        return {"status": "wait"}
    t = ev[0][0]
    hhmm = t.strftime("%H:%M")
    return {"status": "right" if slot[0] <= hhmm <= slot[1] else "wrong", "time": hhmm, "delta": ev[0][1]}
