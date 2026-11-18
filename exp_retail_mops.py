"""
การทดลอง: โมเดลราคาขายปลีกวันถัดไป (สัญญาณ Jobber) — ขยายเพดาน MOPS ปิด และเพิ่ม "MOPS สะสมตั้งแต่ราคาปลีกปรับครั้งล่าสุด"

ที่มา (18/11/2026) : ราคาปลีก ปตท. ลด G95 −0.50 ดีเซล −0.80 (มีผล 05:00) แต่คำทายคืน 17/11 = "นิ่ง" (โอกาสลง ดีเซล 17% G95 23%)
   - MOPS ปิดดีเซล 17/11 = −10.82 แต่ SIGNAL_CLIPS ตัดที่ −8 ; ถ้าไม่ตัด โอกาสลงของดีเซล = 34%
   - ย้อนหลัง (มิ.ย. 2025–) วันที่ MOPS ดีเซลร่วง ≥ 8 : ปลีกลงวันถัดไป 31% (16 วัน) ; G95 ร่วง 4–8 : ลง 42% (12 วัน) แต่โมเดลให้ ~22%
   - โมเดลเห็นเฉพาะ MOPS วันล่าสุด ไม่เห็นต้นทุนที่ลดสะสมตั้งแต่ราคาปลีกปรับครั้งก่อน (ดีเซล 11/11 → 17/11 ร่วงรวม ~−7.4 USD/bbl)
เทมเพลต (ตั้งก่อนรัน 2026-11-18)
  ชุดที่ทดสอบ (เทียบกับ "ที่ใช้จริง" = SIG 4 ตัว ; ฝั่งลงของ G95 มีระดับกองทุนตาม retail_forecast.DOWN_FUND_FUELS) :
     CLIP15 : เพดาน close_last จาก ±8 เป็น ±15 USD/bbl (ตัวแปรอื่นเหมือนเดิม)
     CUM    : เพิ่ม cum_mops = ผลรวม MOPS ปิด (การเปลี่ยนรายวันของชนิดนั้น) ที่ mops_date ≥ วันที่ราคาปลีกปรับครั้งล่าสุด และโพสต์ ≤ 20:30 วัน t ; เพดาน ±40
              วันปรับราคาปลีก = วันที่ RETAIL ใน data/eppo_daily_long.csv ต่างจากวันก่อนหน้า (ปลีกปรับตอนเช้า MOPS ของวันนั้นจึงยังไม่ถูกนับในราคา)
     BOTH   : CLIP15 + CUM
     ตัวแปรใหม่ใส่ทั้งโมเดลขึ้นและลง
  walk-forward ขยายหน้าต่าง ฝึกขั้นต่ำ 50 วัน ก้าว 10 วัน (เหมือน exp_fund.py / exp_retail_search.py)
  เกณฑ์ผ่าน (ต่อชนิด) : Brier 3 ชั้นต่ำกว่าที่ใช้จริง โดยช่วงเชื่อมั่น 95% ของส่วนต่าง (bootstrap รายวัน) ไม่คร่อมศูนย์ ; Brier ปี 2026 ไม่แย่ลง
     ผ่านหลายชุด เลือกชุดที่ Brier ทั้งช่วงต่ำสุด
  รายงานประกอบ (ไม่ใช่เกณฑ์) : AUC ลง/ขึ้น , โอกาสลงเฉลี่ยในวันที่ MOPS ร่วงแรง (ดีเซล ≤ −8 , G95 ≤ −4) เทียบอัตราที่ลงจริง , คืน 17/11/2026 (ถ้ามีในข้อมูล)
  ข้อควรระวัง : 3 ชุด x 2 ชนิด = เลือกหลายชุด ; ปี 2024–2025 ราคาปลีกถูกตรึงด้วยกองทุนเป็นช่วง ๆ (ดีเซลแทบไม่ขยับ) ; วันที่ MOPS ร่วงแรงมีไม่กี่สิบวัน

ผล (รัน 2026-11-18 ; oil-bot/training_data ถึง 21/10/2026 จึงไม่รวมคืน 17/11 ; 502 วันทดสอบ 04/2024–10/2026 ; ปี 2026 182 วัน) : ไม่มีชุดใดผ่าน → คงโมเดลเดิม
  ดีเซล (ลงจริง 4% ของวัน ; วัน MOPS ร่วง ≤ −8 มี 18 วัน ลงจริง 22%) :
     CLIP15 Brier 0.1420 → 0.1504 แย่ลงชัด (+0.003 ถึง +0.015) ; AUC ขึ้น 0.846 → 0.815 — ค่าสุดโต่งทำให้มั่นใจเกินจริง
     CUM    0.1429 (−0.004 ถึง +0.006) ไม่ต่าง ; BOTH แย่ลง
     วัน MOPS ร่วงแรง โอกาสลงเฉลี่ย 15–16% ทุกชุด เทียบลงจริง 22% (ตัวเลข 31% ในที่มามาจากช่วง มิ.ย. 2025– ที่นับต่างกัน)
  G95 (ลงจริง 10% ; วัน MOPS ร่วง ≤ −4 มี 19 วัน ลงจริง 32%) :
     CUM    Brier 0.3453 → 0.3303 (−0.041 ถึง +0.009 คร่อมศูนย์) ; ปี 2026 แย่ลง +0.020 ; แต่ AUC ลง 0.684 → 0.785 และวันร่วงแรงโอกาสลง 28% → 42%
     CLIP15 แย่ลงชัด (+0.002 ถึง +0.011) ; BOTH คร่อมศูนย์และปี 2026 แย่ลง +0.047
  การตีความ : การตัดเพดาน ±8 เป็นประโยชน์ (กันค่าสุดโต่งจากวันที่ตลาดผันผวน) ไม่ใช่สาเหตุหลัก ;
     MOPS สะสมช่วยจัดอันดับวันที่ G95 จะลง (AUC ลงดีขึ้นชัด) แต่ทำให้ความน่าจะเป็นโดยรวมแย่ลงในปี 2026 — อาจเพราะใส่ทั้งฝั่งขึ้นและลง
     ทดสอบเฉพาะฝั่งลงของ G95 ต้องตั้งเป็นการทดลองใหม่ (ไม่ได้ตั้งไว้ก่อนรันครั้งนี้)

ใช้: python exp_retail_mops.py <data_dir ของ jobber> [data/eppo_daily_long.csv]
"""
import sys
import warnings

import numpy as np
import pandas as pd

import exp_retail_kcg_features as K
import fit_model as F
import model as M
import retail_forecast as RF
import signals as S

warnings.filterwarnings("ignore")
CLOSE_COL = {"B7": "diesel", "G95": "g95"}
BIG_DROP = {"B7": -8.0, "G95": -4.0}
CUM_CLIP = 40.0


def change_days(g: pd.Series) -> list:
    """วันที่ราคาปลีกเปลี่ยนจากวันก่อนหน้า (ขยับ ≥ 1 สต.)"""
    d = g.diff().abs()
    return list(g.index[d >= 0.01])


def cum_mops(tables, fuel: str, day: pd.Timestamp, last_change: pd.Timestamp) -> float:
    """ผลรวม MOPS ปิดรายวันของชนิดนี้ ที่ mops_date ≥ last_change และโพสต์ ≤ 20:30 วัน day"""
    mc = tables.mops_close
    if mc.empty or last_change is None:
        return 0.0
    asof = day.normalize() + pd.Timedelta(hours=RF.SIGNAL_HOUR)
    w = mc[(mc.ts <= asof) & (pd.to_datetime(mc.mops_date) >= last_change)]
    w = w.drop_duplicates("mops_date", keep="last")
    v = pd.to_numeric(w[CLOSE_COL[fuel] + "_chg"], errors="coerce").dropna()
    return float(np.clip(v.sum(), -CUM_CLIP, CUM_CLIP))


def dataset(tables, eppo, fuel, last):
    g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
    chg = change_days(g.retail)
    rows = []
    for day, r in g.iterrows():
        nxt = day + pd.Timedelta(days=1)
        if nxt not in g.index or day > last:
            continue
        lc = max((c for c in chg if c <= day), default=None)
        f = RF.signal_features(tables, fuel, day)
        rows.append({"day": day, "y": float(g.retail[nxt] - r.retail), "fund": r.get("fund", np.nan),
                     "close_raw": f.get("close_last"), "cum": cum_mops(tables, fuel, day, lc), **f})
    return pd.DataFrame(rows)


def vectors(df: pd.DataFrame, clip: float, cum: bool) -> np.ndarray:
    base = np.vstack([RF.signal_vector(r) for r in df.to_dict("records")])
    raw = pd.to_numeric(df.close_raw, errors="coerce").fillna(0.0).to_numpy()
    base[:, RF.SIGNAL_FEATURES.index("close_last")] = np.clip(raw, -clip, clip)
    return np.column_stack([base, df.cum.to_numpy()]) if cum else base


def run_fuel(tables, eppo, fuel, last):
    df = dataset(tables, eppo, fuel, last)
    fund_ok = df.fund.notna().to_numpy()
    if fuel in RF.DOWN_FUND_FUELS:
        df = df[fund_ok].reset_index(drop=True)
    y = df.y.to_numpy()
    yu, yd = (y >= RF.MOVE).astype(float), (y <= -RF.MOVE).astype(float)
    fund = np.clip(df.fund.to_numpy(dtype=float), *RF.FUND_CLIP) if fuel in RF.DOWN_FUND_FUELS else None

    def probs(X):
        pu, pdn = K.wf_logit(X, yu, yd)
        if fund is not None:                                      # ฝั่งลงของ G95 ใช้ระดับกองทุนเพิ่ม (ตามที่ใช้จริง)
            _, pdn = K.wf_logit(np.column_stack([X, fund]), yu, yd)
        return pu, pdn

    variants = {"ที่ใช้จริง": probs(vectors(df, 8.0, False)), "CLIP15": probs(vectors(df, 15.0, False)),
                "CUM": probs(vectors(df, 8.0, True)), "BOTH": probs(vectors(df, 15.0, True))}
    ok = ~np.isnan(variants["ที่ใช้จริง"][0])
    y26 = (df.day >= pd.Timestamp("2026-01-01")).to_numpy()
    raw = pd.to_numeric(df.close_raw, errors="coerce").to_numpy()
    big = ok & (raw <= BIG_DROP[fuel])
    print(f"\n== {fuel} : {int(ok.sum())} วันทดสอบ ({df.day[ok].min():%d/%m/%Y}–{df.day[ok].max():%d/%m/%Y}) ; ลง {yd[ok].mean():.0%} ขึ้น {yu[ok].mean():.0%} ;"
          f" ปี 2026 {int((ok & y26).sum())} วัน ; MOPS ร่วง ≤ {BIG_DROP[fuel]:g} {int(big.sum())} วัน (ลงจริง {yd[big].mean():.0%})")
    base_all = K.brier_days(yu, yd, *variants["ที่ใช้จริง"])
    res = {}
    for name, (pu, pdn) in variants.items():
        b = K.brier_days(yu, yd, pu, pdn)
        d_all, d26 = (b - base_all)[ok], (b - base_all)[ok & y26]
        lo, hi = K.ci(d_all) if name != "ที่ใช้จริง" else (0.0, 0.0)
        passed = name != "ที่ใช้จริง" and hi < 0 and d26.mean() <= 0
        res[name] = (b[ok].mean(), passed)
        print(f"   {name:<10} Brier {b[ok].mean():.4f} (2026 {b[ok & y26].mean():.4f})"
              + ("" if name == "ที่ใช้จริง" else f" ต่าง {d_all.mean():+.4f} ({lo:+.4f} ถึง {hi:+.4f}) ; 2026 {d26.mean():+.4f}")
              + f" | AUC ลง {F.auc(yd[ok], pdn[ok]):.3f} ขึ้น {F.auc(yu[ok], pu[ok]):.3f}"
              + f" | วัน MOPS ร่วงแรง โอกาสลงเฉลี่ย {np.nanmean(pdn[big]):.0%}"
              + ("" if name == "ที่ใช้จริง" else f"  → {'ผ่าน' if passed else 'ไม่ผ่าน'}"))
    winners = [n for n, (_, p) in res.items() if p]
    best = min(winners, key=lambda n: res[n][0]) if winners else None
    print(f"   สรุป {fuel}: " + (f"ใช้ {best}" if best else "ไม่มีชุดใดผ่าน → คงโมเดลเดิม"))
    return best


def main():
    _, tables = F.load(sys.argv[1])
    eppo = pd.read_csv(sys.argv[2] if len(sys.argv) > 2 else RF.LONG_HISTORY_FILE, parse_dates=["day"])
    last = tables.headsup.ts.max().normalize() - pd.Timedelta(days=1)
    for fuel in ("B7", "G95"):
        run_fuel(tables, eppo, fuel, last)


if __name__ == "__main__":
    main()
