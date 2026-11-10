"""
S2 โปรไฟล์ราคาตามเวลาแตะ (อ่านอย่างเดียว) : ต่อคลัง ถ้าแตะ ณ เวลาหนึ่ง ราคาที่ได้สูงกว่าราคาต่ำสุดที่เคยมีในช่วงเดียวกันเท่าไร (regret)
และถ้ารู้ทิศการเปลี่ยนราคา 24 ชม. (ขึ้น/ลง/ทรง) ล่วงหน้า จะเลือกเวลาแตะดีขึ้นแค่ไหน (เพดานเมื่อทายทิศถูกทุกครั้ง)

ใช้ PriceLog ของ OIL BOT (ตั้งแต่ 22 ก.ย.) ผ่านกติกาเวลามีผลของฟีดที่แก้แล้ว (ingest.effective_datetime) ; ราคาของคลัง = ต่ำสุดข้ามจ๊อบเบอร์ที่ซื้ออยู่
(ราคาที่เก็บ = หลังส่วนลดต้องบุญ) ; ราคาที่เก่ากว่า MAX_AGE_H ไม่นับ (กันราคาค้างของคีย์ที่หยุดโพสต์)

ช่วงของแต่ละวัน D : จาก 20:30 ของวันก่อน (จุดที่โมเดลแนวโน้มออกคำพยากรณ์) ถึง 22:00 ของ D ; จุดแตะที่ทดสอบ : TAPS
ไม่ได้ใส่เงื่อนไขคลัง (เวลาเปิดปิด โควต้า เวลาตัดรับออเดอร์ ระยะเดินทาง) — ผลนี้เป็นขอบบนของที่ปรับปรุงได้ ไม่ใช่เงินจริง

ใช้: python diag_tap_profile.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY)
"""
import warnings

import numpy as np
import pandas as pd

from common import OILBOT_SHEET_ID, get_token, sheets_get
from diag_pricelog import build_frame

MAX_AGE_H = 30                       # ราคาเก่ากว่านี้ไม่นับ
FLAT = 0.10                          # ขยับ 24 ชม. < 10 สต. = ทรง (เท่ากับเกณฑ์ขึ้น/ลงของแนวโน้ม)
DEPOTS = {"ไออาร์ ระยอง": ("ไออาร์", "ระยอง"), "ไออาร์ อยุธยา": ("ไออาร์", "อยุธยา"), "ดาว สระบุรี": ("สตาร์ฟูเอลส์", "สระบุรี"),
          "ดาว ระยอง": ("สตาร์ฟูเอลส์", "ระยอง"), "ปตท สระบุรี": ("ปตท", "สระบุรี"), "บางจาก บางปะอิน": ("บางจาก", "บางปะอิน")}
# จุดแตะ = ชั่วโมงนับจาก 00:00 ของวัน D (ติดลบ = เย็นวันก่อน)
TAPS = {"ก่อน20:30": -3.5, "ก่อน22:30": -1.5, "01:30": 1.5, "04:30": 4.5, "07:00": 7.0, "09:00": 9.0, "10:45": 10.75, "14:00": 14.0, "16:30": 16.5}
GRID_FROM, GRID_TO, GRID_STEP = -3.5, 22.0, 0.5


def depot_series(df: pd.DataFrame, depot: str, fuel: str) -> list:
    """รายการ (เวลามีผล, ราคา) ต่อจ๊อบเบอร์/คีย์คลังของคลังนี้ — ราคาที่โพสต์เวลาเดียวกันหลายค่าเลือกแถวหลังสุดในชีท"""
    pv, wh = DEPOTS[depot]
    d = df[(df.fuel == fuel) & df.provider.str.contains(pv) & df.warehouse.str.contains(wh)].copy()
    d["_ord"] = d.index
    out = []
    for _, g in d.groupby(["jobber", "provider", "warehouse"]):
        out.append(g.sort_values(["effective", "_ord"], kind="stable").drop_duplicates("effective", keep="last")[["effective", "price"]])
    return out


def price_at(series: list, times: pd.DatetimeIndex, max_age_h: float = MAX_AGE_H) -> np.ndarray:
    """ราคาต่ำสุดข้ามชุดราคา ณ แต่ละเวลา (ใช้ราคาล่าสุดของแต่ละชุดที่ไม่เก่าเกิน max_age_h) ; NaN ถ้าไม่มี"""
    if not series:
        return np.full(len(times), np.nan)
    grid = pd.DataFrame({"t": times})
    cols = []
    for s in series:
        m = pd.merge_asof(grid.sort_values("t"), s, left_on="t", right_on="effective", tolerance=pd.Timedelta(hours=max_age_h))
        cols.append(m.set_index(grid.sort_values("t").index).price.reindex(grid.index).to_numpy())
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)             # ไม่มีราคาสักชุด ณ เวลานั้น → NaN
        return np.nanmin(np.vstack(cols), axis=0)


def day_rows(df: pd.DataFrame) -> pd.DataFrame:
    """แถวต่อ (คลัง, ชนิด, วัน D) : regret ที่แต่ละจุดแตะ + ทิศการเปลี่ยน 24 ชม. (20:30 วันก่อน → 20:30 วัน D)"""
    days = pd.date_range(df.effective.min().normalize() + pd.Timedelta(days=2), df.effective.max().normalize() - pd.Timedelta(days=1), freq="D")
    grid_h = np.arange(GRID_FROM, GRID_TO + 1e-9, GRID_STEP)
    rows = []
    for depot in DEPOTS:
        for fuel in ("B7", "G95"):
            series = depot_series(df, depot, fuel)
            if not series:
                continue
            for day in days:
                grid = price_at(series, pd.DatetimeIndex([day + pd.Timedelta(hours=h) for h in grid_h]))
                taps = price_at(series, pd.DatetimeIndex([day + pd.Timedelta(hours=h) for h in TAPS.values()]))
                end = price_at(series, pd.DatetimeIndex([day + pd.Timedelta(hours=20.5)]))[0]
                if np.isnan(grid).any() or np.isnan(taps).any() or np.isnan(end):
                    continue
                ref = float(grid.min())
                delta = float(end - taps[0])                      # taps[0] = 20:30 วันก่อน
                cls = "ขึ้น" if delta >= FLAT else "ลง" if delta <= -FLAT else "ทรง"
                rows.append({"depot": depot, "fuel": fuel, "day": day, "cls": cls, "delta": delta,
                             **{k: float(v - ref) for k, v in zip(TAPS, taps)}})
    return pd.DataFrame(rows)


def _line(label: str, g: pd.DataFrame) -> str:
    best = min(TAPS, key=lambda k: g[k].mean())
    return f"   {label:<16} n={len(g):>3} | " + " | ".join(f"{k} {g[k].mean():.2f}{'*' if k == best else ''}" for k in TAPS)


def profile_report(rows: pd.DataFrame) -> list:
    if rows.empty:
        return ["[TAP] ไม่มีข้อมูลพอ"]
    days = rows.day.nunique()
    out = [f"[TAP] regret เฉลี่ยของแต่ละจุดแตะ (บาท/ลิตร สูงกว่าราคาต่ำสุดของช่วง 20:30 วันก่อน–22:00 วัน D) ; {rows.day.min():%d/%m}–{rows.day.max():%d/%m} {days} วัน ; * = ต่ำสุดของแถว"]
    for depot, g in rows.groupby("depot"):
        out.append(_line(depot, g))
    out.append(_line("ทุกคลัง", rows))
    at_min = " | ".join(f"{k} {float((rows[k] < 0.02).mean()):.0%}" for k in TAPS)
    out.append(f"   ทุกคลัง: สัดส่วนวันที่ราคา ณ จุดแตะ = ต่ำสุดของช่วง (<0.02) | {at_min}")
    out.append(f"[TAP] แยกตามทิศการเปลี่ยนราคา 24 ชม. (20:30 วันก่อน → 20:30 วัน D, ขึ้น/ลง = ขยับ ≥{FLAT:.2f}) ทุกคลัง :")
    for cls in ("ขึ้น", "ทรง", "ลง"):
        g = rows[rows.cls == cls]
        if len(g):
            out.append(_line(f"วัน{cls}", g))
    fixed = min(TAPS, key=lambda k: rows[k].mean())
    r_fixed = float(rows[fixed].mean())
    r_oracle = float(sum(len(g) * g[min(TAPS, key=lambda k: g[k].mean())].mean() for _, g in rows.groupby("cls")) / len(rows))
    out.append(f"[TAP] จุดแตะคงที่ที่ดีที่สุด = {fixed} regret เฉลี่ย {r_fixed:.3f} ; ถ้าเลือกจุดแตะตามทิศที่ถูกต้องทุกครั้ง (เพดาน ไม่ใช่ผลจริง) {r_oracle:.3f} "
               f"→ ลดได้ {r_fixed - r_oracle:.3f} บาท/ลิตร ({(r_fixed - r_oracle) / r_fixed if r_fixed else 0.0:.0%}) ; ยังไม่หักเงื่อนไขคลัง/เวลาเดินทาง และไม่ใช่ผลของแนวโน้มที่ทายผิดบ้าง")
    return out


def main():
    tok = get_token()
    df = build_frame(sheets_get(tok, OILBOT_SHEET_ID, "PriceLog!A1:Q200000")).reset_index(drop=True)
    print(f"[TAP] PriceLog rows={len(df)} {df.date.min()}→{df.date.max()}")
    for line in profile_report(day_rows(df)):
        print(line)


if __name__ == "__main__":
    main()
