"""
ตรวจช่องว่างของ PriceLog : ช่วงที่ไม่มีข้อความราคาใหม่นาน ๆ เกิดจากจ๊อบเบอร์เงียบ หรือจากบอท/ไปป์ไลน์หยุด — อ่านอย่างเดียว ไม่เขียนชีท
ช่องว่างวัดจากเวลาที่ล็อกลงชีท แยกด้วย "ล็อกชดเชย" : ถ้าหลังช่องว่างมีข้อความจำนวนมากถูกล็อกพร้อมกันและเวลาโพสต์เก่ากว่าช่องว่าง แปลว่าไปป์ไลน์หยุดแล้วดึงย้อนหลังทีหลัง (ข้อความไม่ได้หายแต่ล็อกช้า)
ถ้าไม่มีข้อความเก่ามาชดเชย แปลว่าไม่มีข้อความโพสต์ในช่วงนั้น (เงียบจริง) หรือข้อความหายไป (แยกจากข้อมูลนี้อย่างเดียวไม่ได้)

ใช้: python diag_pricelog_gaps.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY)
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from common import OILBOT_SHEET_ID, TZ_NAME, get_token, sheets_get, to_ts
from ingest import cell_date, cell_time

GAP_H = 4.0                  # ช่องว่างตั้งแต่กี่ชม.ถึงรายงาน
DAY_FROM, DAY_TO = 6, 22     # ช่องว่างที่เริ่มในช่วงนี้ถือเป็น "กลางวัน" (กลางคืนเงียบเป็นปกติ)
BACKLOG_WINDOW_MIN = 15      # ข้อความที่ล็อกภายในกี่นาทีหลังช่องว่างนับเป็นชุดชดเชย
LOOKBACK_DAYS = 30
WD = ["จ", "อ", "พ", "พฤ", "ศ", "ส", "อา"]


def messages(values: list) -> pd.DataFrame:
    """PriceLog → 1 แถวต่อข้อความ (message_id) : posted (วัน+เวลาโพสต์), logged (เวลาที่ล็อกลงชีท)"""
    head = [str(h).strip() for h in values[0]]
    ix = {c: head.index(c) for c in ("timestamp_logged", "message_id", "update_date", "update_time")}
    rows = {}
    for r in values[1:]:
        g = lambda c: r[ix[c]] if ix[c] < len(r) else ""
        mid, d, t = str(g("message_id")), cell_date(g("update_date")), cell_time(g("update_time"))
        posted = pd.to_datetime(f"{d} {t}", errors="coerce") if d and t else pd.NaT
        logged = to_ts(g("timestamp_logged"))
        if mid and not pd.isna(posted) and mid not in rows:
            rows[mid] = (mid, posted, logged)
    return pd.DataFrame(rows.values(), columns=["msg", "posted", "logged"]).sort_values("posted").reset_index(drop=True)


def lag_report(m: pd.DataFrame) -> list:
    d = m.dropna(subset=["logged"]).copy()
    d["lag_min"] = (d["logged"] - d["posted"]).dt.total_seconds() / 60
    d = d[(d["lag_min"] > -60) & (d["lag_min"] < 60 * 24 * 3)]
    if d.empty:
        return ["[GAP-LAG] ไม่มีข้อมูลเวลาล็อก"]
    big = d[d["lag_min"] > 30]
    return [f"[GAP-LAG] {len(d)} ข้อความ ; ล็อกหลังโพสต์ มัธยฐาน {d['lag_min'].median():.0f} นาที | p90 {d['lag_min'].quantile(.9):.0f} | สูงสุด {d['lag_min'].max():.0f} ; "
            f"ช้ากว่า 30 นาที {len(big)} ข้อความ ({len(big) / len(d):.1%})"]


def gaps(m: pd.DataFrame, since: pd.Timestamp) -> pd.DataFrame:
    """ช่องว่างระหว่างเวลาที่ล็อกข้อความติดกัน (เห็นการหยุดของไปป์ไลน์) ตั้งแต่ GAP_H ชม. ;
    backlog = หลังช่องว่างมีข้อความที่โพสต์ "ในช่วงช่องว่าง" ถูกล็อกชดเชยตอนกลับมา (ไปป์ไลน์หยุดจริง ไม่ใช่เงียบ)"""
    p = m.dropna(subset=["logged"])
    p = p[p["logged"] >= since].sort_values("logged").reset_index(drop=True)
    out = []
    for i in range(1, len(p)):
        a, b = p.loc[i - 1, "logged"], p.loc[i, "logged"]
        hrs = (b - a).total_seconds() / 3600
        if hrs < GAP_H:
            continue
        batch = p[(p["logged"] >= b) & (p["logged"] <= b + pd.Timedelta(minutes=BACKLOG_WINDOW_MIN))]
        backlog = bool(((batch["posted"] >= a) & (batch["posted"] < b - pd.Timedelta(minutes=30))).any())
        out.append({"start": a, "end": b, "hours": round(hrs, 1), "day": DAY_FROM <= a.hour < DAY_TO and a.hour + a.minute / 60 + hrs < 24 + DAY_FROM, "backlog": backlog})
    return pd.DataFrame(out, columns=["start", "end", "hours", "day", "backlog"])


def gap_report(m: pd.DataFrame, now: pd.Timestamp) -> list:
    since = now.normalize() - pd.Timedelta(days=LOOKBACK_DAYS)
    last = m["posted"].max()
    out = [f"[GAP] ข้อความ {len(m)} ({m.posted.min():%d/%m %H:%M}–{last:%d/%m %H:%M}) ; ข้อความล่าสุดเก่า {(now - last).total_seconds() / 3600:.1f} ชม."]
    g = gaps(m, since)
    if g.empty:
        return out + [f"[GAP] ไม่มีช่องว่าง ≥ {GAP_H:.0f} ชม. ใน {LOOKBACK_DAYS} วันที่ผ่านมา"]
    daytime = g[g["day"]]
    out.append(f"[GAP] ช่องว่าง ≥ {GAP_H:.0f} ชม. ใน {LOOKBACK_DAYS} วัน : {len(g)} ครั้ง ; เริ่มกลางวัน ({DAY_FROM}:00–{DAY_TO}:00 และไม่ข้ามไปถึงเช้าวันถัดไป) {len(daytime)} ครั้ง ; "
               f"มีล็อกชดเชย (ไปป์ไลน์น่าจะหยุด) {int(g.backlog.sum())} ครั้ง ; ≥ 10 ชม. {int((g.hours >= 10).sum())} ครั้ง")
    wd = daytime.assign(w=daytime["start"].dt.dayofweek).groupby("w").size().reindex(range(7), fill_value=0)
    out.append("   ช่องว่างกลางวันตามวัน จ.–อา. : " + " ".join(str(int(v)) for v in wd))
    for _, r in g.sort_values("hours", ascending=False).head(8).iterrows():
        out.append(f"   {r['start']:%d/%m %H:%M} → {r['end']:%d/%m %H:%M} ({WD[r['start'].dayofweek]}) {r['hours']:.1f} ชม. {'กลางวัน' if r['day'] else 'ข้ามคืน'}"
                   f"{' · ล็อกชดเชย' if r['backlog'] else ''}")
    return out


def main():
    now = pd.Timestamp(datetime.now(ZoneInfo(TZ_NAME)).replace(tzinfo=None))
    m = messages(sheets_get(get_token(), OILBOT_SHEET_ID, "PriceLog!A1:Q200000"))
    if m.empty:
        print("[GAP] ไม่พบข้อความ")
        return
    for line in lag_report(m) + gap_report(m, now):
        print(line)


if __name__ == "__main__":
    main()
