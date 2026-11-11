"""
นโยบายจุดแตะตามแนวโน้ม + การคำนวณ regret ต่อ (คลัง, ชนิด, วัน) จากราคา PriceLog — ฟังก์ชันล้วน ไม่เขียนชีท
ใช้ร่วมกันระหว่างตัวตรวจ (diag_tap_profile.py) และตัวบันทึกเงา (log_taprec.py)

regret = ราคาที่แตะ ณ จุดเวลาหนึ่ง สูงกว่าราคาต่ำสุดของช่วง 20:30 วันก่อน–22:00 วัน D เท่าไร (บาท/ลิตร)
ไม่ได้หักเงื่อนไขคลัง (เวลาเปิดปิด โควต้า เวลาตัดรับออเดอร์ ระยะเดินทาง) และเวลามีผลของการขึ้นราคาบางส่วนไม่ทราบ — ใช้เทียบนโยบายด้วยกัน ไม่ใช่เงินจริง
"""
import warnings

import numpy as np
import pandas as pd

from common import to_ts

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


# นโยบายเลือกจุดแตะจากแนวโน้ม 24 ชม. ที่ออกตอน 20:30 วันก่อน (ขึ้น/ลง/ไม่ชัด → จุดแตะ) — กำหนดไว้ก่อนเริ่มติดตามแบบเงา (10 พ.ย. 69) ห้ามปรับตามผลที่เห็น
POLICY_MAPS = {
    "A": {"ขึ้น": "ก่อน22:30", "ลง": "16:30", "ไม่ชัด": "ก่อน22:30"},
    "B": {"ขึ้น": "07:00", "ลง": "16:30", "ไม่ชัด": "07:00"},
    "C": {"ขึ้น": "ก่อน22:30", "ลง": "16:30", "ไม่ชัด": "16:30"},
}
FIXED_TAPS = ("ก่อน22:30", "07:00", "16:30")             # ตัวเทียบ : แตะเวลาคงที่


def window_regrets(series: list, day: pd.Timestamp):
    """regret ที่ทุกจุดแตะ + การเปลี่ยนราคา 24 ชม. (20:30 วันก่อน → 20:30 วัน D) ของคลัง/ชนิดหนึ่งในวัน D ; None ถ้าราคาไม่ครบ"""
    grid_h = np.arange(GRID_FROM, GRID_TO + 1e-9, GRID_STEP)
    grid = price_at(series, pd.DatetimeIndex([day + pd.Timedelta(hours=h) for h in grid_h]))
    taps = price_at(series, pd.DatetimeIndex([day + pd.Timedelta(hours=h) for h in TAPS.values()]))
    end = price_at(series, pd.DatetimeIndex([day + pd.Timedelta(hours=20.5)]))[0]
    if np.isnan(grid).any() or np.isnan(taps).any() or np.isnan(end):
        return None
    ref = float(grid.min())
    delta = float(end - taps[0])                                   # taps[0] = 20:30 วันก่อน
    cls = "ขึ้น" if delta >= FLAT else "ลง" if delta <= -FLAT else "ทรง"
    return {"delta": delta, "cls": cls, **{k: float(v - ref) for k, v in zip(TAPS, taps)}}


def pred_of(tier) -> str:
    t = str(tier)
    return "ขึ้น" if t.startswith("UP") else "ลง" if t.startswith("DOWN") else "ไม่ชัด"


def load_outlook(values: list, max_lag_h: float = 3.0) -> pd.DataFrame:
    """แท็บ OutlookLog จาก Sheets API (อ่านแบบ UNFORMATTED จึงได้เลข serial สำหรับวันเวลา) → แถวรอบ 20:30 ที่ไม่ใช่ backfill
    ออกภายใน max_lag_h ชม. หลังจุดอ้างอิง (ค่าเดิม 3 = "ตรงเวลา") ; คอลัมน์ lag_h = asof − ref (ชม.)
    1 แถวต่อ (วัน, ชนิด, ขอบฟ้า) รอบแรกเท่านั้น"""
    head = values[0]
    d = pd.DataFrame([r + [""] * (len(head) - len(r)) for r in values[1:]], columns=head)
    d["asof"], d["ref"] = d["asof"].map(to_ts), d["ref"].map(to_ts)
    d["hours"] = pd.to_numeric(d["hours"], errors="coerce")
    d = d.dropna(subset=["asof", "ref", "hours"])
    d["lag_h"] = ((d["asof"] - d["ref"]).dt.total_seconds() / 3600).round(2)
    d = d[(d["ref"].dt.strftime("%H:%M") == "20:30") & (d["lag_h"] <= max_lag_h)]
    if "backfilled" in d.columns:
        d = d[pd.to_numeric(d["backfilled"], errors="coerce").fillna(0) == 0]
    d = d.assign(date=d["ref"].dt.strftime("%Y-%m-%d")).sort_values("asof").drop_duplicates(["date", "fuel", "hours"], keep="first")
    return d


def diff_ci(per_day: np.ndarray, reps: int = 2000, seed: int = 7):
    """ค่าเฉลี่ยของผลต่างรายวัน และช่วงเชื่อมั่น 95% แบบ bootstrap รายวัน (หน่วยอิสระ = วัน เพราะทุกคลังขยับตามกัน)"""
    per_day = np.asarray(per_day, dtype=float)
    rng = np.random.default_rng(seed)
    boots = rng.choice(per_day, size=(reps, len(per_day)), replace=True).mean(axis=1)
    return float(per_day.mean()), float(np.percentile(boots, 2.5)), float(np.percentile(boots, 97.5))
