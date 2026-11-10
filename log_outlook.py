"""
ทดสอบแนวโน้มราคา 24/48 ชม. แบบ "เดินหน้า" (forward test) — บันทึกแนวโน้มรอบ 20:30 ของแต่ละวันลงแท็บ OutlookLog
แล้วเติมผลจริงเมื่อครบ 24/48 ชม. ไม่มีการย้อนแก้ ไม่มีการแจ้งเตือน

หนึ่งแถว = วัน x ชนิดน้ำมัน x ขอบฟ้า (บันทึกรอบแรกที่ประเมินรอบ 20:30 ของวันนั้นได้ ซ้ำไม่ได้)
outcome_delta = ราคาต่ำสุดของตลาด ณ (20:30 + ขอบฟ้า) − ณ 20:30 ; เติมได้เมื่อเลยเวลานั้นไป ~30 นาที

ใช้: python log_outlook.py            (ต้องมี SA_EMAIL / SA_PRIVATE_KEY และ outlook_snapshot.json จาก main.py)
"""
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

import signals as S
from common import SHEET_ID, TABS, TZ_NAME, ensure_tab, get_token, sheets_get, sheets_append, sheets_update, to_ts

# asof = เวลาที่ระบบออกคำพยากรณ์ (created_at) ; ref = จุดอ้างอิงที่ตั้งใจ (as_of: ฟีเจอร์ใช้เฉพาะข้อมูลที่โพสต์ก่อน/เท่ากับเวลานี้)
# lag_h = asof - ref (ชม.) ; backfilled = 1 ถ้าสร้างย้อนหลัง (ไม่นับในเกณฑ์) — สองคอลัมน์ท้ายเพิ่มทีหลัง แถวเก่าเป็นค่าว่าง
COLUMNS = ["asof", "date", "ref", "fuel", "hours", "tier", "p_rise10", "p_fall10", "p_rise40", "p_fall40", "grade",
           "price_ref", "price_target", "outcome_delta", "outcome_filled_at", "model_version", "lag_h", "backfilled"]
LAST_COL = "R"
OUTCOME_FROM = COLUMNS.index("price_ref")
OUTCOME_COLS = COLUMNS[OUTCOME_FROM:OUTCOME_FROM + 4]
ON_TIME_LAG_H = 3      # "ตรงเวลา" = ออกภายใน 3 ชม. หลังจุดอ้างอิง (นับในเกณฑ์) ; ช้ากว่านี้บันทึกไว้แต่แยกรายงาน
MAX_LOG_LAG_H = 12     # ช้าเกินนี้ไม่บันทึก (ตรงกับ stale ของ decide_outlook)


def new_rows(existing: list, snapshot: list) -> list:
    """แถวใหม่ที่ยังไม่เคยบันทึก (คีย์ = ref + ชนิด + ขอบฟ้า ; ref = รอบ 20:30/15:30 ที่ใช้ประเมิน) — รอบแรกของ ref นั้นเท่านั้น
    ออกช้าหลัง ref ไม่เกิน MAX_LOG_LAG_H ก็บันทึก (ฟีเจอร์ใช้เฉพาะข้อมูลที่โพสต์ก่อน ref จึงไม่รั่วข้อมูลอนาคต) พร้อม lag_h"""
    def k(v):
        t = to_ts(v)
        return str(v) if pd.isna(t) else t.strftime("%Y-%m-%d %H:%M")
    seen = {(k(r.get("ref")), str(r.get("fuel")), str(r.get("hours"))) for r in existing}
    out = []
    for s in snapshot:
        lag = pd.Timestamp(s["asof"]) - pd.Timestamp(s["ref"])
        if lag > pd.Timedelta(hours=MAX_LOG_LAG_H):
            continue                                   # ข้อมูลเก่าเกินไป (เช่นเช้าวันถัดไป): ไม่ใช่คำพยากรณ์ของรอบนั้น
        key = (k(s["ref"]), str(s["fuel"]), str(s["hours"]))
        if key in seen:
            continue
        seen.add(key)
        row = {c: s.get(c, "") for c in COLUMNS}
        row["date"] = pd.Timestamp(s["ref"]).strftime("%Y-%m-%d")
        row["lag_h"] = round(lag.total_seconds() / 3600, 2)
        row["backfilled"] = int(bool(s.get("backfilled", 0)))
        for c in OUTCOME_COLS:
            row[c] = ""
        out.append(row)
    return out


def compute_outcome(book: S.PriceBook, fuel: str, ref, hours: int, now: pd.Timestamp):
    """(ราคา ณ ref, ราคา ณ ref+ขอบฟ้า, ผลต่าง) หรือ None ถ้ายังไม่ถึงเวลา/ไม่มีราคา"""
    t0 = pd.Timestamp(ref)
    t1 = t0 + pd.Timedelta(hours=int(hours))
    if now < t1 + pd.Timedelta(minutes=30):
        return None
    a, b = book.min_at(fuel, [t0]).iloc[0], book.min_at(fuel, [t1]).iloc[0]
    if pd.isna(a) or pd.isna(b):
        return None
    return float(a), float(b), round(float(b - a), 2)


def main():
    now = pd.Timestamp(datetime.now(ZoneInfo(TZ_NAME)).replace(tzinfo=None))
    snapshot = json.load(open("outlook_snapshot.json", encoding="utf-8")) if os.path.exists("outlook_snapshot.json") else []
    tok = get_token()
    tab = TABS["OUTLOOK_LOG"]
    if ensure_tab(tok, SHEET_ID, tab):
        sheets_update(tok, SHEET_ID, f"{tab}!A1", [COLUMNS])
        print(f"[OUTLOOK] created tab {tab}")
    values = sheets_get(tok, SHEET_ID, f"{tab}!A1:{LAST_COL}50000")
    if values and len(values[0]) < len(COLUMNS):         # แท็บเดิมมี 16 คอลัมน์ : เติมหัวคอลัมน์ใหม่ต่อท้าย (ไม่แตะแถวข้อมูล)
        sheets_update(tok, SHEET_ID, f"{tab}!A1", [COLUMNS])
        values[0] = COLUMNS
        print("[OUTLOOK] header upgraded (lag_h, backfilled)")
    head = values[0] if values else COLUMNS
    rows = [dict(zip(head, r + [""] * (len(head) - len(r)))) for r in values[1:]]
    add = new_rows(rows, snapshot)
    if add:
        sheets_append(tok, SHEET_ID, f"{tab}!A1:{LAST_COL}1", [[r.get(c, "") for c in COLUMNS] for r in add])
        rows += add
    print(f"[OUTLOOK] snapshot rows: +{len(add)} new")

    prices = pd.DataFrame(sheets_get(tok, SHEET_ID, f"{TABS['FEED_PRICES']}!A1:E200000"))
    if len(prices) <= 1:
        print("[OUTLOOK] no price feed yet — outcomes not filled")
        return
    prices.columns = prices.iloc[0]
    prices = prices.iloc[1:].reset_index(drop=True)
    prices["effective"] = prices["effective"].map(to_ts)
    book = S.PriceBook(prices)
    filled = 0
    for i, r in enumerate(rows):
        if str(r.get("outcome_delta", "")) != "" or not r.get("ref"):
            continue
        ref_ts = to_ts(r["ref"])
        if pd.isna(ref_ts):
            continue
        res = compute_outcome(book, r["fuel"], ref_ts, int(float(r["hours"])), now)
        if res:
            r["price_ref"], r["price_target"], r["outcome_delta"] = res
            r["outcome_filled_at"] = now.strftime("%Y-%m-%d %H:%M")
            sheets_update(tok, SHEET_ID, f"{tab}!L{i + 2}:O{i + 2}",
                          [[r["price_ref"], r["price_target"], r["outcome_delta"], r["outcome_filled_at"]]])
            filled += 1
    print(f"[OUTLOOK] outcomes filled: {filled}")


if __name__ == "__main__":
    main()
