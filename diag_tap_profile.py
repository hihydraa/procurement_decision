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

from common import OILBOT_SHEET_ID, SHEET_ID, TABS, get_token, sheets_get
from pricelog_frame import build_frame
from tap_policy import (DEPOTS, FIXED_TAPS, FLAT, GRID_FROM, GRID_STEP, GRID_TO, MAX_AGE_H, POLICY_MAPS, TAPS, depot_series,  # noqa: F401  (re-export สำหรับเทสต์เดิม)
                        diff_ci as _diff_ci, load_outlook, pred_of, price_at, window_regrets)

def day_rows(df: pd.DataFrame) -> pd.DataFrame:
    """แถวต่อ (คลัง, ชนิด, วัน D) : regret ที่แต่ละจุดแตะ + ทิศการเปลี่ยน 24 ชม. (20:30 วันก่อน → 20:30 วัน D)"""
    days = pd.date_range(df.effective.min().normalize() + pd.Timedelta(days=2), df.effective.max().normalize() - pd.Timedelta(days=1), freq="D")
    rows = []
    for depot in DEPOTS:
        for fuel in ("B7", "G95"):
            series = depot_series(df, depot, fuel)
            if not series:
                continue
            for day in days:
                w = window_regrets(series, day)
                if w is not None:
                    rows.append({"depot": depot, "fuel": fuel, "day": day, **w})
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


POLICIES = {
    "แนวโน้ม A (ขึ้น→ก่อน22:30, ลง→16:30, ไม่ชัด→ก่อน22:30)": POLICY_MAPS["A"],
    "แนวโน้ม B (ขึ้น→07:00, ลง→16:30, ไม่ชัด→07:00)": POLICY_MAPS["B"],
    "แนวโน้ม C (ขึ้น→ก่อน22:30, ลง→16:30, ไม่ชัด→16:30)": POLICY_MAPS["C"],
}
ORACLE = {"ขึ้น": "ก่อน22:30", "ทรง": "ก่อน22:30", "ลง": "16:30"}      # ทายทิศถูกทุกครั้ง (ใช้ทิศจริงของวัน)


def join_outlook(rows: pd.DataFrame, ol: pd.DataFrame, hours: int = 24) -> pd.DataFrame:
    """ผูกแนวโน้มที่ออก 20:30 ของวัน r (ขอบฟ้า hours) กับวัน D = r+1 ของ day_rows (ช่วง 20:30 r → 20:30 D) ต่อชนิดน้ำมัน"""
    o = ol[ol.hours == hours].copy()
    o["day"] = o.ref.dt.normalize() + pd.Timedelta(days=1)
    m = rows.merge(o[["fuel", "day", "tier"]], on=["fuel", "day"])
    m["pred"] = m.tier.map(pred_of)
    return m


def policy_regret(m: pd.DataFrame, mapping: dict, key: str = "pred") -> pd.Series:
    """regret ของแต่ละแถว (คลัง-ชนิด-วัน) เมื่อเลือกจุดแตะตาม mapping[key]"""
    return m.apply(lambda r: r[mapping[r[key]]], axis=1)


def diff_ci(m: pd.DataFrame, a: pd.Series, b: pd.Series, reps: int = 2000, seed: int = 7):
    """ผลต่างเฉลี่ย (a − b) และช่วงเชื่อมั่น 95% แบบ bootstrap รายวัน (หน่วยอิสระ = วันที่ ไม่ใช่แถว เพราะทุกคลังขยับตามกัน)"""
    return _diff_ci((a - b).groupby(m.day).mean().to_numpy(), reps, seed)


def policy_report(rows: pd.DataFrame, ol: pd.DataFrame) -> list:
    m = join_outlook(rows, ol)
    if m.empty:
        return ["[POLICY] ไม่มีวันที่ทั้งมีแนวโน้มและมีราคาครบ"]
    nd = m.day.nunique()
    out = [f"[POLICY] ประเมินนโยบายด้วยแนวโน้ม 24 ชม. ที่ออกจริง (OutlookLog รอบ 20:30 ตรงเวลา) {nd} วัน ({m.day.min():%d/%m}–{m.day.max():%d/%m}) × {m[['depot', 'fuel']].drop_duplicates().shape[0]} คลัง-ชนิด = {len(m)} แถว ; regret = บาท/ลิตรที่แพงกว่าต่ำสุดของช่วง"]
    conf = pd.crosstab(m.pred, m.cls).reindex(index=["ขึ้น", "ลง", "ไม่ชัด"], columns=["ขึ้น", "ทรง", "ลง"], fill_value=0)
    out.append("   แนวโน้มที่ทาย → ทิศจริงของวัน (ขึ้น/ทรง/ลง ต่อแถว): " + " ; ".join(f"{p} → {int(conf.loc[p, 'ขึ้น'])}/{int(conf.loc[p, 'ทรง'])}/{int(conf.loc[p, 'ลง'])}" for p in conf.index))
    fixed = {k: m[k] for k in FIXED_TAPS}
    best = min(TAPS, key=lambda k: m[k].mean())
    out.append("   แตะเวลาคงที่ : " + " | ".join(f"{k} {float(v.mean()):.3f}" for k, v in fixed.items()) + f" | ที่ดีที่สุดในตัวอย่างนี้ {best} {float(m[best].mean()):.3f}")
    base = m[best]
    for name, mapping in POLICIES.items():
        r = policy_regret(m, mapping)
        d, lo, hi = diff_ci(m, r, base)
        out.append(f"   {name}: regret {float(r.mean()):.3f} ; เทียบ {best} คงที่ {d:+.3f} (95% {lo:+.3f}..{hi:+.3f})")
    ro = policy_regret(m, ORACLE, key="cls")
    out.append(f"   ทายทิศถูกทุกครั้ง (เพดาน): regret {float(ro.mean()):.3f}")
    out.append("   ตัวเลขลบ = ทำตามแนวโน้มแล้ว regret ต่ำลง ; ถ้าช่วงเชื่อมั่นคร่อม 0 แปลว่ายังแยกจากการแตะเวลาคงที่ไม่ได้ ; ยังไม่หักเงื่อนไขคลัง/เวลาเดินทาง/เวลามีผลที่ไม่ทราบ")
    return out


def main():
    tok = get_token()
    df = build_frame(sheets_get(tok, OILBOT_SHEET_ID, "PriceLog!A1:Q200000")).reset_index(drop=True)
    print(f"[TAP] PriceLog rows={len(df)} {df.date.min()}→{df.date.max()}")
    rows = day_rows(df)
    for line in profile_report(rows):
        print(line)
    ol = load_outlook(sheets_get(tok, SHEET_ID, f"{TABS['OUTLOOK_LOG']}!A1:R50000"))
    for line in policy_report(rows, ol):
        print(line)


if __name__ == "__main__":
    main()
