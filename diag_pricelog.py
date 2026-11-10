"""
S2 ขั้นแรก (อ่านอย่างเดียว) : ตรวจ PriceLog ของ OIL BOT — (1) ส่วนลดของต้องบุญ (ราคาประกาศ − ราคาที่เก็บ) ต่อกลุ่มผู้ขายและช่วงเวลา
(2) ส่วนต่างราคาระหว่างต้องบุญกับศรีพลังสำหรับคลัง/ชนิด/เวลามีผลเดียวกัน (3) เวลาที่ราคาเปลี่ยนจริง (ขึ้น/ลง ขนาดใหญ่) ตามช่วงเวลาของวัน

ใช้: python diag_pricelog.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY ; อ่านชีท OIL BOT ที่ ingest.py ใช้อยู่แล้ว ไม่เขียนอะไร)
"""
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from common import OILBOT_SHEET_ID, get_token, sheets_get
from ingest import cell_date, cell_time

TBP, SPL = "ต้องบุญ", "ศรีพลัง"
BIG = 0.30                                   # การเปลี่ยนราคา ≥ 30 สต. นับว่า "ใหญ่"
BUCKETS = [("22:00-23:29", 22 * 60, 23 * 60 + 30), ("23:30-23:59", 23 * 60 + 30, 24 * 60), ("00:00-00:59", 0, 60), ("01:00-04:59", 60, 5 * 60),
           ("05:00-07:59", 5 * 60, 8 * 60), ("08:00-08:59", 8 * 60, 9 * 60), ("09:00-12:59", 9 * 60, 13 * 60),
           ("13:00-17:59", 13 * 60, 18 * 60), ("18:00-21:59", 18 * 60, 22 * 60)]


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


def effective_ts(date_s: str, time_s: str, eff_s: str):
    """เวลามีผลของราคา — กติกาเดียวกับ ingest._prices_feed : ใช้ effective_time ถ้าระบุ ; ถ้าน้อยกว่าเวลาโพสต์เกิน 2 ชม. ถือเป็นของวันถัดไป"""
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_s):
        return pd.NaT
    posted = datetime.strptime(f"{date_s} {time_s or '00:00'}", "%Y-%m-%d %H:%M")
    eff = posted
    if re.match(r"^\d{1,2}:\d{2}$", eff_s):
        cand = datetime.strptime(f"{date_s} {eff_s.zfill(5)}", "%Y-%m-%d %H:%M")
        eff = cand + timedelta(days=1) if cand < posted - timedelta(hours=2) else cand
    return pd.Timestamp(eff)


def build_frame(values: list) -> pd.DataFrame:
    head = [str(h).strip() for h in values[0]]
    idx = {c: head.index(c) for c in ("jobber", "provider", "warehouse_name", "fuel_type", "price", "update_date", "update_time", "effective_time", "announced_price")}
    rows = []
    for r in values[1:]:
        g = lambda c: r[idx[c]] if idx[c] < len(r) else ""
        price, ann = to_num(g("price")), to_num(g("announced_price"))
        if np.isnan(price):
            continue
        d, t = cell_date(g("update_date")), cell_time(g("update_time"))
        eff = effective_ts(d, t, cell_time(g("effective_time")))
        if pd.isna(eff):
            continue
        rows.append((str(g("jobber")), str(g("provider")), str(g("warehouse_name")), str(g("fuel_type")), price, ann, eff, d))
    df = pd.DataFrame(rows, columns=["jobber", "provider", "warehouse", "fuel", "price", "announced", "effective", "date"])
    df["group"] = df["provider"].map(provider_group)
    df["discount"] = (df["announced"] - df["price"]).round(2)
    return df


def discount_report(df: pd.DataFrame, jobbers=(TBP, SPL)) -> list:
    """ส่วนลด = ราคาประกาศ − ราคาที่เก็บ ต่อจ๊อบเบอร์ × กลุ่มผู้ขาย : ค่าที่พบ (จำนวน, วันแรก–วันสุดท้าย)"""
    out = []
    for jb in jobbers:
        sub = df[df.jobber == jb]
        out.append(f"[PRICELOG] {jb}: {len(sub)} แถว ; ไม่มี announced_price {int(sub.announced.isna().sum())} แถว")
        for grp, g in sub.groupby("group"):
            g = g.dropna(subset=["announced"])
            if g.empty:
                continue
            parts = []
            for val, x in sorted(g.groupby("discount"), key=lambda kv: -len(kv[1]))[:4]:
                parts.append(f"{val:.2f}: {len(x)} ({len(x) / len(g):.0%}, {x.date.min()}→{x.date.max()})")
            out.append(f"   {grp:<7} ลด " + " | ".join(parts))
    return out


def jobber_gap_report(df: pd.DataFrame) -> list:
    """ราคาที่เก็บของศรีพลัง − ต้องบุญ ที่ คลัง+ชนิด+เวลามีผล เดียวกัน : ต่อกลุ่มผู้ขาย"""
    d = df.drop_duplicates(["jobber", "provider", "warehouse", "fuel", "effective"], keep="last")
    a, b = d[d.jobber == TBP], d[d.jobber == SPL]
    m = a.merge(b, on=["provider", "warehouse", "fuel", "effective"], suffixes=("_t", "_s"))
    out = [f"[PRICELOG] คู่ราคาต้องบุญ/ศรีพลัง (คลัง+ชนิด+เวลามีผลเดียวกัน) = {len(m)} คู่ ; ส่วนต่าง = ศรีพลัง − ต้องบุญ (ราคาที่เก็บ) และ − ราคาประกาศของต้องบุญ"]
    if m.empty:
        return out
    m["gap"] = (m.price_s - m.price_t).round(2)
    m["gap_ann"] = (m.price_s - m.announced_t).round(2)
    for grp, g in m.groupby("group_t"):
        out.append(f"   {grp:<7} n={len(g):>4} | ศรีพลังแพงกว่า(เก็บ) เฉลี่ย {g.gap.mean():+.3f} ; ต้องบุญถูกกว่า {float((g.gap > 0.004).mean()):.0%} เท่ากัน {float((g.gap.abs() < 0.005).mean()):.0%} "
                   f"ศรีพลังถูกกว่า {float((g.gap < -0.004).mean()):.0%} | เทียบประกาศต้องบุญ เฉลี่ย {g.gap_ann.mean():+.3f}")
    return out


def change_timing_report(df: pd.DataFrame, jobber: str = TBP) -> list:
    """เวลามีผลของการเปลี่ยนราคา (ราคาที่เก็บของต้องบุญ) ต่อช่วงเวลาของวัน : ขึ้น/ลงทั้งหมด และที่ใหญ่ (≥ BIG)"""
    d = df[df.jobber == jobber].drop_duplicates(["provider", "warehouse", "fuel", "effective"], keep="last").sort_values("effective")
    d = d.assign(delta=d.groupby(["provider", "warehouse", "fuel"]).price.diff()).dropna(subset=["delta"])
    d = d[d.delta.abs() > 0.004]
    d["minute"] = d.effective.dt.hour * 60 + d.effective.dt.minute
    out = [f"[PRICELOG] เวลามีผลของการเปลี่ยนราคา ({jobber}, {len(d)} ครั้ง ; ใหญ่ = ≥{BIG:.2f} บาท) — ต่อกลุ่มผู้ขาย : ช่วงเวลา → ขึ้นทั้งหมด/ลงทั้งหมด (ใหญ่: ขึ้น/ลง)"]
    for grp, g in d.groupby("group"):
        if len(g) < 20:
            continue
        cells = []
        for name, lo, hi in BUCKETS:
            x = g[(g.minute >= lo) & (g.minute < hi)]
            up, dn = int((x.delta > 0).sum()), int((x.delta < 0).sum())
            bu, bd = int((x.delta >= BIG).sum()), int((x.delta <= -BIG).sum())
            cells.append(f"{name} {up}/{dn} ({bu}/{bd})")
        out.append(f"   {grp:<7} n={len(g):>5} | " + " ; ".join(cells))
    return out


def main():
    tok = get_token()
    values = sheets_get(tok, OILBOT_SHEET_ID, "PriceLog!A1:Q200000")
    df = build_frame(values)
    print(f"[PRICELOG] rows={len(df)} {df.date.min()}→{df.date.max()} jobber: " + ", ".join(f"{k}={v}" for k, v in df.jobber.value_counts().head(6).items()))
    for line in discount_report(df) + jobber_gap_report(df) + change_timing_report(df):
        print(line)


if __name__ == "__main__":
    main()
