"""
ราคาปลีกพยากรณ์ได้แม่นแค่ไหน — ทดสอบจากข้อมูลจริงใน Entry_Eppo (อ่านอย่างเดียว ไม่เขียนชีท)

กลไก (กำหนดล่วงหน้าก่อนดูผล ไม่ได้จูนจากข้อมูล) : EPPO คำนวณ ค่าการตลาด = ราคาปลีก/1.07 − ราคาขายส่ง
  ราคาส่งลงแต่ปลีกยังไม่ลง → ค่าการตลาดบวมเกินค่ากลาง (G95 3.37, ดีเซล 1.94) → แรงกดให้ปลีกลดตาม ; กลับกันถ้าค่าการตลาดต่ำกว่าค่ากลาง → ปลีกมีแรงขึ้น
  คะแนน = ค่าการตลาดเทียบค่ากลาง (บวก = แรงลง, ลบ = แรงขึ้น) ; ไม่ต้องฝึกโมเดล จึงไม่มีการรั่วของข้อมูลอนาคต

เป้าหมาย : ปลีกที่ t+k วัน (k = 1..3, วันปฏิทิน) เทียบ t : ลง (<= -0.10) / ขึ้น (>= +0.10) / นิ่ง
รายงาน : อัตราฐาน, ความแม่น (AUC ของคะแนน), อัตราลง/ขึ้นจริงแยกตามช่วงค่าการตลาด, ขนาดการปรับ, วันในสัปดาห์ที่ปลีกปรับ, ผลแยกครึ่งแรก/ครึ่งหลังของข้อมูล

ใช้: python diag_retail_forecast.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY)
"""
import numpy as np
import pandas as pd

from common import SHEET_ID, get_token, sheets_get
from diag_rule_backtest import MEDIAN_MM, eppo_daily

MOVE = 0.10
KS = (1, 2, 3)
FUEL_TH = {"B7": "ดีเซล", "G95": "G95"}


def build_table(eppo: pd.DataFrame) -> pd.DataFrame:
    """วัน × ชนิด : score (ค่าการตลาดเทียบค่ากลาง), dws (ΔWS เทียบวันก่อน), และ d{k} = ปลีก(t+k) − ปลีก(t) เมื่อมีข้อมูลวัน t+k จริง"""
    rows = []
    for fuel, g in eppo.groupby("fuel"):
        s = g.set_index("day").sort_index()
        for day, r in s.iterrows():
            prev = day - pd.Timedelta(days=1)
            row = {"day": day, "fuel": fuel, "score": float(r.mm - MEDIAN_MM[fuel]), "retail": float(r.retail), "ws": float(r.ws),
                   "dws": float(r.ws - s.ws[prev]) if prev in s.index else np.nan}
            for k in KS:
                t = day + pd.Timedelta(days=k)
                row[f"d{k}"] = float(s.retail[t] - r.retail) if t in s.index else np.nan
            rows.append(row)
    return pd.DataFrame(rows)


def auc(score: np.ndarray, positive: np.ndarray) -> float:
    """AUC แบบอันดับ (Mann-Whitney) ; nan ถ้ามีคลาสเดียว"""
    score, positive = np.asarray(score, float), np.asarray(positive, bool)
    n1, n0 = int(positive.sum()), int((~positive).sum())
    if n1 == 0 or n0 == 0:
        return float("nan")
    ranks = pd.Series(score).rank().to_numpy()
    return float((ranks[positive].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def skill_lines(t: pd.DataFrame) -> list:
    out = []
    for fuel in ("B7", "G95"):
        g = t[t.fuel == fuel]
        if g.empty:
            continue
        out.append(f"[RFC] {FUEL_TH[fuel]}: {len(g)} วัน ({g.day.min():%d/%m/%y}–{g.day.max():%d/%m/%y})")
        for k in KS:
            d = g.dropna(subset=[f"d{k}"])
            if len(d) < 20:
                out.append(f"   ปลีก +{k} วัน: ข้อมูลน้อย ({len(d)} วัน)")
                continue
            dn, up = d[f"d{k}"] <= -MOVE, d[f"d{k}"] >= MOVE
            a_dn, a_up = auc(d.score, dn), auc(-d.score, up)
            out.append(f"   ปลีก +{k} วัน (n={len(d)}) : ลงจริง {dn.mean():.0%} ขึ้นจริง {up.mean():.0%} นิ่ง {(~dn & ~up).mean():.0%} | AUC ทายลง {a_dn:.2f} ทายขึ้น {a_up:.2f}")
        d3 = g.dropna(subset=["d3"])
        if len(d3) >= 30:
            q = pd.qcut(d3.score, 3, labels=["ค่าการตลาดต่ำ", "กลาง", "สูง"], duplicates="drop")
            for lab, x in d3.groupby(q, observed=True):
                out.append(f"      ค่าการตลาด{lab} ({x.score.min():+.2f}..{x.score.max():+.2f}, n={len(x)}) : ปลีก +3 วัน ลง {(x.d3 <= -MOVE).mean():.0%} ขึ้น {(x.d3 >= MOVE).mean():.0%}")
            half = len(d3) // 2
            for name, x in (("ครึ่งแรก", d3.iloc[:half]), ("ครึ่งหลัง", d3.iloc[half:])):
                out.append(f"      {name} (n={len(x)}) AUC ทายลง {auc(x.score, x.d3 <= -MOVE):.2f} ทายขึ้น {auc(-x.score, x.d3 >= MOVE):.2f}")
    return out


def move_lines(t: pd.DataFrame) -> list:
    """ขนาดและวันในสัปดาห์ที่ปลีกปรับ (เทียบวันก่อนหน้า)"""
    out = []
    for fuel in ("B7", "G95"):
        g = t[t.fuel == fuel].sort_values("day")
        d = g.retail.diff()
        gap = g.day.diff().dt.days
        d = d[gap == 1]
        mv = d[d.abs() >= MOVE]
        if mv.empty:
            continue
        days = g.loc[mv.index, "day"].dt.dayofweek.value_counts().reindex(range(7), fill_value=0)
        out.append(f"[RFC-MOVE] {FUEL_TH[fuel]}: ปลีกปรับ {len(mv)}/{len(d)} วัน ({len(mv) / len(d):.0%}) ; ขนาด |Δ| มัธยฐาน {mv.abs().median():.2f} p25 {mv.abs().quantile(.25):.2f} p75 {mv.abs().quantile(.75):.2f}")
        out.append("   ปรับตามวัน จ.–อา. : " + " ".join(str(int(x)) for x in days.to_numpy()))
    return out


def passthrough_lines(t: pd.DataFrame) -> list:
    """เมื่อราคาส่งขยับแรง (|ΔWS| >= 0.30) ปลีกตามทิศเดียวกันภายใน k วันกี่ %"""
    out = []
    for fuel in ("B7", "G95"):
        g = t[(t.fuel == fuel)].dropna(subset=["dws"])
        for name, ev in (("ลง", g.dws <= -0.30), ("ขึ้น", g.dws >= 0.30)):
            x = g[ev]
            if len(x) < 5:
                continue
            sign = -1 if name == "ลง" else 1
            parts = []
            for k in KS:
                y = x.dropna(subset=[f"d{k}"])
                parts.append(f"+{k}วัน {int((sign * y[f'd{k}'] >= MOVE).sum())}/{len(y)}" if len(y) else f"+{k}วัน -")
            out.append(f"[RFC-PASS] {FUEL_TH[fuel]}: ส่งขยับ{name} >= 0.30 จำนวน {len(x)} วัน ; ปลีกตามทิศเดียวกัน " + " | ".join(parts))
    return out


def main():
    tok = get_token()
    eppo = eppo_daily(sheets_get(tok, SHEET_ID, "Entry_Eppo!A1:S6000"))
    t = build_table(eppo)
    print(f"[RFC] Entry_Eppo แถวจริง {len(eppo)} (ไม่นับ CARRIED)")
    for line in skill_lines(t) + move_lines(t) + passthrough_lines(t):
        print(line)


if __name__ == "__main__":
    main()
