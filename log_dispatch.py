"""
ทดสอบแผนส่งรถแบบ "เดินหน้า" (forward test) — บันทึกคำแนะนำที่ main.py ออกจริงทุกรอบลงแท็บ DispatchLog
แล้วเติมผลลัพธ์เมื่อรู้ราคาจริง ไม่มีการย้อนแก้ ไม่มีการแจ้งเตือน

หนึ่งแถว = คำแนะนำหนึ่งรอบ x ชนิดน้ำมัน x ระยะเดินทาง (บันทึกเฉพาะรอบที่ประเมินได้ ไม่ใช่ NO_DATA)
ผลลัพธ์ outcome_delta = ราคาต่ำสุดของตลาด ณ เวลาถึง+24 ชม. − ณ เวลาถึง (บวก = รอแล้วแพงขึ้น ซึ่งการเร่งส่งจะได้ประโยชน์)
เติมได้เมื่อเลย เวลาถึง+24 ชม. ไปแล้ว ~30 นาที — คำแนะนำของวันนี้จึงเริ่มมีผลเร็วสุดพรุ่งนี้ช่วงเย็น/ค่ำ

ใช้: python log_dispatch.py            (ต้องมี SA_EMAIL / SA_PRIVATE_KEY และ dispatch_snapshot.json จาก main.py)
"""
import json
import os
from datetime import datetime

import pandas as pd

import signals as S
from common import SHEET_ID, TABS, TZ_NAME, ensure_tab, get_token, sheets_get, sheets_append, sheets_update, to_ts
from zoneinfo import ZoneInfo

COLUMNS = ["asof", "date", "window", "fuel", "lead", "action_code", "raw_action_code", "grade", "p_rise", "p_fall", "arrive",
           "price_arrive", "price_next", "outcome_delta", "outcome_filled_at", "model_version"]
OUTCOME_FROM = COLUMNS.index("price_arrive")      # คอลัมน์ L..O


def _asof_key(v) -> str:
    """เวลารอบเป็นข้อความ YYYY-MM-DD HH:MM เสมอ (ชีทคืนเป็นเลข serial เมื่ออ่านแบบ UNFORMATTED จึงต้องแปลงก่อนเทียบ)"""
    t = to_ts(v)
    return str(v) if pd.isna(t) else t.strftime("%Y-%m-%d %H:%M")


def premature(row: dict) -> bool:
    """แถวที่มีผลลัพธ์แล้วแต่ถูกเติมก่อนเวลา (เวลาถึง+24 ชม.) = ผลผิดจากบั๊กอ่านวันที่เป็นปี 1970 → ต้องล้างแล้วคำนวณใหม่"""
    if str(row.get("outcome_delta", "")) == "":
        return False
    arrive, filled = to_ts(row.get("arrive")), to_ts(row.get("outcome_filled_at"))
    return pd.isna(arrive) or pd.isna(filled) or filled < arrive + pd.Timedelta(days=1)


def new_rows(existing: list, snapshot: list) -> list:
    """แถวใหม่ที่ยังไม่เคยบันทึก (คีย์ = asof + ชนิด + ระยะเดินทาง) ; ข้ามรอบที่ไม่มีคำแนะนำ"""
    seen = {(_asof_key(r.get("asof")), str(r.get("fuel")), str(r.get("lead"))) for r in existing}
    out = []
    for s in snapshot:
        if s.get("action_code") in (None, "NO_DATA"):
            continue
        k = (_asof_key(s["asof"]), str(s["fuel"]), str(s["lead"]))
        if k in seen:
            continue
        seen.add(k)
        row = {c: s.get(c, "") for c in COLUMNS}
        row["date"] = pd.Timestamp(s["asof"]).strftime("%Y-%m-%d")
        for c in COLUMNS[OUTCOME_FROM:]:
            row[c] = ""
        out.append(row)
    return out


def compute_outcome(book: S.PriceBook, fuel: str, arrive, now: pd.Timestamp):
    """(ราคา ณ เวลาถึง, ราคา ณ เวลาถึง+24ชม., ผลต่าง) หรือ None ถ้ายังไม่ถึงเวลา/ไม่มีราคา"""
    t0 = pd.Timestamp(arrive)
    t1 = t0 + pd.Timedelta(days=1)
    if now < t1 + pd.Timedelta(minutes=30):
        return None
    a, b = book.min_at(fuel, [t0]).iloc[0], book.min_at(fuel, [t1]).iloc[0]
    if pd.isna(a) or pd.isna(b):
        return None
    return float(a), float(b), round(float(b - a), 2)


def main():
    tz = ZoneInfo(TZ_NAME)
    now = pd.Timestamp(datetime.now(tz).replace(tzinfo=None))
    snapshot = json.load(open("dispatch_snapshot.json", encoding="utf-8")) if os.path.exists("dispatch_snapshot.json") else []
    tok = get_token()
    tab = TABS["DISPATCH_LOG"]
    if ensure_tab(tok, SHEET_ID, tab):
        sheets_update(tok, SHEET_ID, f"{tab}!A1", [COLUMNS])
        print(f"[DISPATCH] created tab {tab}")
    values = sheets_get(tok, SHEET_ID, f"{tab}!A1:P50000")
    head = values[0] if values else COLUMNS
    if head != COLUMNS:                                               # แท็บที่สร้างก่อนมีคอลัมน์ model_version: เติมหัวคอลัมน์ใหม่ (แถวเก่าเว้นว่าง = ไม่ทราบเวอร์ชัน)
        sheets_update(tok, SHEET_ID, f"{tab}!A1", [COLUMNS])
        head = COLUMNS
    rows = [dict(zip(head, r + [""] * (len(head) - len(r)))) for r in values[1:]]

    add = new_rows(rows, snapshot)
    if add:
        sheets_append(tok, SHEET_ID, f"{tab}!A1:P1", [[r.get(c, "") for c in COLUMNS] for r in add])
        rows += add
    print(f"[DISPATCH] snapshot rows: +{len(add)} new")

    prices = pd.DataFrame(sheets_get(tok, SHEET_ID, f"{TABS['FEED_PRICES']}!A1:E200000"))
    if len(prices) <= 1:
        print("[DISPATCH] no price feed yet — outcomes not filled")
        return
    prices.columns = prices.iloc[0]
    prices = prices.iloc[1:].reset_index(drop=True)
    prices["effective"] = prices["effective"].map(to_ts)           # serial → วันเวลาจริง (ดู common.to_ts)
    book = S.PriceBook(prices)
    filled = reset = 0
    for i, r in enumerate(rows):
        if premature(r):
            for c in COLUMNS[OUTCOME_FROM:]:
                r[c] = ""
            sheets_update(tok, SHEET_ID, f"{tab}!L{i + 2}:O{i + 2}", [["", "", "", ""]])
            reset += 1
        if str(r.get("outcome_delta", "")) != "" or not r.get("arrive"):
            continue
        arrive_ts = to_ts(r["arrive"])
        if pd.isna(arrive_ts):
            continue
        res = compute_outcome(book, r["fuel"], arrive_ts, now)
        if res:
            r["price_arrive"], r["price_next"], r["outcome_delta"] = res
            r["outcome_filled_at"] = now.strftime("%Y-%m-%d %H:%M")
            sheets_update(tok, SHEET_ID, f"{tab}!L{i + 2}:O{i + 2}",
                          [[r["price_arrive"], r["price_next"], r["outcome_delta"], r["outcome_filled_at"]]])
            filled += 1
    print(f"[DISPATCH] outcomes filled: {filled}, reset (filled too early): {reset}")


if __name__ == "__main__":
    main()
