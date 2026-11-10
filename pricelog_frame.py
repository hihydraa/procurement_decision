"""
แปลง PriceLog ของ OIL BOT (ชีทราคาจากกลุ่มจ๊อบเบอร์) เป็น DataFrame พร้อมเวลามีผลตามกติกาของฟีด (ingest.effective_datetime)
ใช้ร่วมกันระหว่างตัวตรวจ (diag_*) และตัวบันทึกเงา (log_taprec.py) — ไม่เขียนอะไรลงชีท
"""
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from ingest import cell_date, cell_time, effective_datetime

TBP, SPL = "ต้องบุญ", "ศรีพลัง"


def provider_group(name: str) -> str:
    s = str(name)
    for key, grp in (("ไออาร์", "IRPC"), ("สตาร์ฟูเอลส์", "ดาว"), ("ดาว", "ดาว"), ("บางจาก", "บางจาก"), ("ปตท", "ปตท")):
        if key in s:
            return grp
    return "อื่น ๆ"


def to_num(v):
    if isinstance(v, bool):
        return np.nan
    try:
        return float(v)
    except (TypeError, ValueError):
        return np.nan


def effective_old(date_s: str, time_s: str, eff_s: str) -> datetime:
    """กติกาเดิมของฟีด (ก่อนแก้) : ระบุเวลาน้อยกว่าเวลาโพสต์เกิน 2 ชม. = วันถัดไปเสมอ — ไว้เทียบก่อน/หลัง"""
    posted = datetime.strptime(f"{date_s} {time_s or '00:00'}", "%Y-%m-%d %H:%M")
    if not re.match(r"^\d{1,2}:\d{2}$", eff_s):
        return posted
    cand = datetime.strptime(f"{date_s} {eff_s.zfill(5)}", "%Y-%m-%d %H:%M")
    return cand + timedelta(days=1) if cand < posted - timedelta(hours=2) else cand


def effective_parts(date_s: str, time_s: str, eff_s: str):
    """(เวลามีผลตามกติกาปัจจุบันของฟีด, ถูกเลื่อนเป็นวันถัดไปหรือไม่ (ตามกติกาปัจจุบัน), เวลาโพสต์, เวลามีผลตามกติกาเดิม)"""
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_s):
        return pd.NaT, False, pd.NaT, pd.NaT
    posted = datetime.strptime(f"{date_s} {time_s or '00:00'}", "%Y-%m-%d %H:%M")
    eff = effective_datetime(date_s, time_s, eff_s)
    return pd.Timestamp(eff), eff.date() > posted.date(), pd.Timestamp(posted), pd.Timestamp(effective_old(date_s, time_s, eff_s))


def effective_ts(date_s: str, time_s: str, eff_s: str):
    return effective_parts(date_s, time_s, eff_s)[0]


def first_line(text) -> str:
    for ln in str(text or "").split("\n"):
        if ln.strip():
            return ln.strip()
    return ""


def stated(line: str) -> tuple:
    """บรรทัดแรกของประกาศ → (ทิศ 'ขึ้น'/'ลง'/'?', เวลาที่ระบุ 'HH:MM' หรือ 'เที่ยงคืน' หรือ '') เช่น '9/11/69 : ปรับลง 10.25 น.' → ('ลง', '10:25')"""
    dirn = "ขึ้น" if "ปรับขึ้น" in line else "ลง" if "ปรับลง" in line else "?"
    m = re.search(r"(\d{1,2})[.:](\d{2})\s*น", line)
    if m:
        return dirn, f"{int(m.group(1)):02d}:{m.group(2)}"
    return dirn, "เที่ยงคืน" if "เที่ยงคืน" in line else ""


def build_frame(values: list) -> pd.DataFrame:
    head = [str(h).strip() for h in values[0]]
    idx = {c: head.index(c) for c in ("message_id", "jobber", "provider", "warehouse_name", "fuel_type", "price", "update_date", "update_time",
                                      "effective_time", "announced_price", "raw_text")}
    rows = []
    for r in values[1:]:
        g = lambda c: r[idx[c]] if idx[c] < len(r) else ""
        price, ann = to_num(g("price")), to_num(g("announced_price"))
        if np.isnan(price):
            continue
        d, t = cell_date(g("update_date")), cell_time(g("update_time"))
        eff_t = cell_time(g("effective_time"))
        eff, shifted, posted, eff_old = effective_parts(d, t, eff_t)
        if pd.isna(eff):
            continue
        dirn, when = stated(first_line(g("raw_text")))
        rows.append((str(g("message_id")), str(g("jobber")), str(g("provider")), str(g("warehouse_name")), str(g("fuel_type")), price, ann, eff, d,
                     bool(re.match(r"^\d{1,2}:\d{2}$", eff_t)), dirn, when, shifted, posted, eff_old))
    df = pd.DataFrame(rows, columns=["msg", "jobber", "provider", "warehouse", "fuel", "price", "announced", "effective", "date", "has_eff", "dir", "stated",
                                     "shifted", "posted", "eff_old"])
    df["group"] = df["provider"].map(provider_group)
    df["discount"] = (df["announced"] - df["price"]).round(2)
    return df
