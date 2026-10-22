"""
บันทึกคำแนะนำที่ main.py ออกจริงลงแท็บ DecisionLog แล้วเติมผลลัพธ์ย้อนหลังเมื่อรู้แล้วว่าราคาจริงเป็นอย่างไร

เป้าหมายคือวัดความแม่นยำ "ของจริง" ต่อเนื่อง (main.py แสดงสถิตินี้เมื่อมีผลลัพธ์พอ) และให้ fit_model.py ฝึกซ้ำได้
ด้วยข้อมูลที่สะสมเอง — ไม่มีการแจ้งเตือนใด ๆ

บันทึก 1 แถวต่อ (วัน, ชนิดน้ำมัน, ช่วง): ช่วง PM บันทึกรอบที่รันในเวลา 15:00-18:59 , EVE รอบตั้งแต่ 20:00 (รอบหลังทับรอบก่อนเพราะรู้ข้อมูลมากกว่า)
ผลลัพธ์ = ราคาต่ำสุดของตลาด 15:30 วันถัดไป − 15:30 ของวันตัดสินใจ (นิยามเดียวกับ backtest)

ใช้: python log_decisions.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY และ decision_snapshot.json จาก main.py)
"""
import json
import os
from datetime import datetime, timedelta

import pandas as pd

import signals as S
from common import SHEET_ID, TABS, TZ_NAME, ensure_tab, get_token, sheets_get, sheets_append, sheets_update, to_ts
from zoneinfo import ZoneInfo

COLUMNS = ["asof", "date", "fuel", "phase", "action_code", "action", "p_rise", "p_fall", "trend", "up_n", "dn_n",
           "close_last", "fund_next", "price_asof", "price_next", "outcome_delta", "outcome_filled_at"]
WINDOWS = {"PM": (15.0, 19.0), "EVE": (20.0, 24.0)}


def in_window(asof: pd.Timestamp, phase: str) -> bool:
    h = asof.hour + asof.minute / 60
    lo, hi = WINDOWS[phase]
    return lo <= h < hi


def canonical_time(date: pd.Timestamp) -> pd.Timestamp:
    return date.normalize() + pd.Timedelta(hours=15.5)


def compute_outcome(book: S.PriceBook, fuel: str, date: pd.Timestamp, now: pd.Timestamp):
    """(price_asof, price_next, delta) หรือ None ถ้ายังไม่ถึงเวลา/ไม่มีราคา"""
    t0, t1 = canonical_time(date), canonical_time(date) + pd.Timedelta(days=1)
    if now < t1 + pd.Timedelta(minutes=30):
        return None
    a, b = book.min_at(fuel, [t0]).iloc[0], book.min_at(fuel, [t1]).iloc[0]
    if pd.isna(a) or pd.isna(b):
        return None
    return float(a), float(b), round(float(b - a), 2)


def _day(v) -> str:
    """วันที่เป็นข้อความ YYYY-MM-DD (ชีทคืนเลข serial เมื่ออ่านแบบ UNFORMATTED จึงเทียบกับข้อความตรง ๆ ไม่ได้ — เดิมทำให้ทุกรอบเพิ่มแถวซ้ำ)"""
    t = to_ts(v)
    return str(v) if pd.isna(t) else t.strftime("%Y-%m-%d")


def _stamp(v) -> str:
    t = to_ts(v)
    return str(v) if pd.isna(t) else t.strftime("%Y-%m-%d %H:%M")


def premature(row: dict) -> bool:
    """มีผลลัพธ์แล้วแต่เติมก่อนถึง 15:30 วันถัดไป (+30 นาที) = ผลผิดจากบั๊กอ่านเวลาเป็นปี 1970 → ต้องล้างแล้วคำนวณใหม่"""
    if str(row.get("outcome_delta", "")) == "":
        return False
    day, filled = to_ts(row.get("date")), to_ts(row.get("outcome_filled_at"))
    return pd.isna(day) or pd.isna(filled) or filled < canonical_time(day) + pd.Timedelta(days=1, minutes=30)


def merge_snapshot(rows: list, snapshot: list) -> tuple:
    """รวม snapshot ใหม่เข้ากับแถวเดิม: คืน (แถวที่ต้องเพิ่ม, {เลขแถว(0-based): แถวใหม่ที่ต้องเขียนทับ})"""
    index = {(_day(r.get("date")), r.get("fuel"), r.get("phase")): i for i, r in enumerate(rows)}
    appends, updates = [], {}
    for s in snapshot:
        asof = pd.Timestamp(s["asof"])
        if s["phase"] not in WINDOWS or not in_window(asof, s["phase"]) or s["action_code"] == "NO_DATA":
            continue
        row = {"asof": s["asof"], "date": asof.strftime("%Y-%m-%d"), "fuel": s["fuel"], "phase": s["phase"], "action_code": s["action_code"],
               "action": s["action"], "p_rise": s["p_rise"], "p_fall": s["p_fall"], "trend": s.get("trend"), "up_n": s.get("up_n"),
               "dn_n": s.get("dn_n"), "close_last": s.get("close_last"), "fund_next": s.get("fund_next"),
               "price_asof": "", "price_next": "", "outcome_delta": "", "outcome_filled_at": ""}
        k = (row["date"], row["fuel"], row["phase"])
        if k in index:
            old = rows[index[k]]
            if str(old.get("outcome_delta", "")) == "" and str(row["asof"]) >= _stamp(old.get("asof", "")):
                updates[index[k]] = row
        else:
            appends.append(row)
            index[k] = len(rows) + len(appends) - 1
    return appends, updates


def main():
    tz = ZoneInfo(TZ_NAME)
    now = pd.Timestamp(datetime.now(tz).replace(tzinfo=None))
    snapshot = json.load(open("decision_snapshot.json", encoding="utf-8")) if os.path.exists("decision_snapshot.json") else []
    tok = get_token()
    tab = TABS["DECISION_LOG"]
    if ensure_tab(tok, SHEET_ID, tab):
        sheets_update(tok, SHEET_ID, f"{tab}!A1", [COLUMNS])
        print(f"[LOG] created tab {tab}")
    values = sheets_get(tok, SHEET_ID, f"{tab}!A1:Q20000")
    head = values[0] if values else COLUMNS
    rows = [dict(zip(head, r + [""] * (len(head) - len(r)))) for r in values[1:]]

    appends, updates = merge_snapshot(rows, snapshot)
    for i, row in updates.items():
        sheets_update(tok, SHEET_ID, f"{tab}!A{i + 2}", [[row.get(c, "") for c in COLUMNS]])
        rows[i] = row
    if appends:
        sheets_append(tok, SHEET_ID, f"{tab}!A1:Q1", [[r.get(c, "") for c in COLUMNS] for r in appends])
        rows += appends
    print(f"[LOG] snapshot rows: +{len(appends)} new, {len(updates)} updated")

    # เติมผลลัพธ์ของแถวที่ถึงเวลาแล้ว
    prices = pd.DataFrame(sheets_get(tok, SHEET_ID, f"{TABS['FEED_PRICES']}!A1:E200000"))
    if len(prices) > 1:
        prices.columns = prices.iloc[0]
        prices = prices.iloc[1:].reset_index(drop=True)
        prices["effective"] = prices["effective"].map(to_ts)           # serial → วันเวลาจริง (ดู common.to_ts)
        book = S.PriceBook(prices)
        filled = reset = 0
        for i, r in enumerate(rows):
            if premature(r):
                for c in ("price_asof", "price_next", "outcome_delta", "outcome_filled_at"):
                    r[c] = ""
                sheets_update(tok, SHEET_ID, f"{tab}!N{i + 2}:Q{i + 2}", [["", "", "", ""]])
                reset += 1
            if str(r.get("outcome_delta", "")) != "" or not r.get("date"):
                continue
            day = to_ts(r["date"])
            if pd.isna(day):
                continue
            res = compute_outcome(book, r["fuel"], day, now)
            if res:
                r["price_asof"], r["price_next"], r["outcome_delta"] = res
                r["outcome_filled_at"] = now.strftime("%Y-%m-%d %H:%M")
                sheets_update(tok, SHEET_ID, f"{tab}!N{i + 2}:Q{i + 2}", [[r["price_asof"], r["price_next"], r["outcome_delta"], r["outcome_filled_at"]]])
                filled += 1
        print(f"[LOG] outcomes filled: {filled}, reset (filled too early): {reset}")
    else:
        print("[LOG] no price feed yet — outcomes not filled")


if __name__ == "__main__":
    main()
