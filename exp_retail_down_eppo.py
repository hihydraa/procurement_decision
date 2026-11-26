"""
การทดลอง: ใส่ข้อมูลโครงสร้างราคา EPPO (ค่าการตลาด / ราคาหน้าโรงกลั่น) เฉพาะในโมเดล "ฝั่งลง" ของราคาปลีก + ฝึกด้วยข้อมูลถึงปัจจุบัน

ที่มา (26/11/2026) : โมเดลราคาปลีกยังไม่เคยทาย "ลง" ตั้งแต่เริ่มบันทึก ทั้งที่ปลีกลดจริง 18/11 (ดีเซล/G95) , 19/11 (G95) , 25/11 (ดีเซล) ,
   26/11 (G95 −0.70 ตามประกาศ SFL ; โมเดลให้โอกาสลง 32% + คำเตือน แต่คำทายหลัก "ไม่ปรับ")
   เบาะแสจาก exp_retail_kcg_features.py (17/11) : ฟีเจอร์โครงสร้างราคาช่วยฝั่ง "ลง" ของ G95 (AUC ลง 0.66–0.75 → 0.82–0.88) แต่ทำให้ฝั่งขึ้นแย่ลง
   → บันทึกไว้ว่า "ถ้าจะใช้ ต้องทดสอบแบบใส่เฉพาะในโมเดลฝั่งลง ด้วยเกณฑ์ที่ตั้งใหม่ก่อนรัน"

เทมเพลต (ตั้งก่อนรัน 2026-11-26 ; เจ้าของระบบสั่งให้เริ่ม)
  ข้อมูล : สัญญาณ = oil-bot/training_data + แท็บ Entry_* ในชีท (ถึง 25/11/2026 ; แบบเดียวกับ retrain.merge_increment)
           ราคา EPPO = data/eppo_daily_long.csv + Entry_Eppo (ไม่นับแถว CARRIED ; ถึง 25/11/2026)
  เป้าหมาย : ปลีก EPPO วัน t+1 − วัน t (คู่วันติดกัน) ; ลง = ลด >= 10 สต. ; ทายจากข้อมูลคืนวัน t (20:30) และไฟล์ EPPO ของวัน t
  เดิม (BASE) : โมเดลที่ใช้จริง — ฝั่งลง ดีเซล = up_n, dn_n, close_last, fund_next ; G95 = ชุดเดียวกัน + ระดับกองทุน (down_fund)
  ใหม่ (E) : ฝั่งลง = BASE + 2 ตัวแปรจาก EPPO ของวัน t
     mm_dev   = ค่าการตลาด − มัธยฐาน 30 ไฟล์ก่อนหน้า (ค่าการตลาดสูงผิดปกติ = มีช่องให้ลดปลีก)   ตัดที่ ±3
     exref_d5 = ราคาหน้าโรงกลั่นวันนี้ − 5 ไฟล์ก่อน (ต้นทุนลดสะสม)                           ตัดที่ ±5
  การทดสอบ : walk-forward ขยายหน้าต่าง ฝึกขั้นต่ำ 150 คู่วัน ก้าว 30 ; ฝั่งขึ้นไม่เปลี่ยน
  ผ่าน (ต่อชนิด ทุกข้อ) :
     1) AUC ฝั่งลง เพิ่ม >= 0.02 ทั้งช่วง และไม่แย่ลงในครึ่งแรก ครึ่งหลัง และปี 2026
     2) Brier ฝั่งลง (ลง/ไม่ลง) ต่ำลงทั้งช่วงและปี 2026
     3) ทาย "ลง" ผิด (โอกาสลง >= 0.35 แต่ไม่ลงจริง) เพิ่มไม่เกิน 2 จุดของจำนวนวัน
  รายงานเพิ่ม : โอกาสลงที่โมเดลให้ในวันที่ปลีกลดจริงช่วง พ.ย. 2026 (เดิม vs ใหม่) — ข้อมูลรายวันที่เป็นที่มา (ไม่ใช่เกณฑ์)
  ข้อควรระวัง : ข้อมูลปีปัจจุบันรวมอยู่ในการทดสอบ (มองดีเกินจริงได้สำหรับวันที่เป็นที่มา) ; ราคาปลีกดีเซลถูกตรึงช่วง 2024–2025 ;
     ไฟล์ EPPO ของวัน t อาจออกหลัง 20:30 ในบางวัน (ใช้ได้กับการ์ด 21:30 ที่เป็นผู้ใช้จริง)

ผล (รัน 2026-11-26 ; สัญญาณ + EPPO ถึง 25/11/2026 ; ดีเซล 403 / G95 404 วันทดสอบ) : ไม่ผ่านทั้งสองชนิด → คงโมเดลเดิม
   ดีเซล : AUC ลง 0.811 → 0.794 (ปี 2026 0.834 → 0.800) ✗ ; Brier 0.0540 → 0.0538 ✓ ; ทายลงผิด 0.7% → 0.5% ✓
   G95   : AUC ลง 0.630 → 0.738 ✓ (แต่มาจากครึ่งแรก 0.561 → 0.816 ; ครึ่งหลัง 0.650 → 0.674) ;
           Brier 0.1160 → 0.1111 แต่ปี 2026 แย่ลง 0.1453 → 0.1525 ✗ ; ทายลงผิด 3.2% → 5.4% ✗
   mm_dev แทบเป็น 0 ทุกวัน (ค่าการตลาดในไฟล์ EPPO แทบไม่เปลี่ยนรายวัน) ; สัญญาณมาจาก exref_d5 (ต้นทุนหน้าโรงกลั่นลดสะสม)
   วันที่ปลีกลดจริง พ.ย. 2026 : โอกาสลงเพิ่มขึ้นบ้าง (เช่น ดีเซล 25/11 17% → 33%) แต่ไม่ถึงเกณฑ์ทาย "ลง" (35%) ส่วนใหญ่
   ฝึกใหม่ด้วยข้อมูลถึง 24/11 (ชุดตัวแปรเดิม) — ดูผลในข้อความ PR ; ยังไม่นำไปใช้ เพราะน้ำหนักกองทุนในโมเดลฝั่งลงของดีเซลกลับทิศ
     (fund_next −0.06 → +0.39 : กองทุนเก็บเพิ่มกลับทำให้โอกาส "ลง" สูงขึ้น) รอเจ้าของระบบตัดสินใจ

ใช้: python exp_retail_down_eppo.py <data_dir สัญญาณที่รวมแล้ว> <eppo_long.csv ที่รวมแล้ว>
"""
import argparse
import warnings

import numpy as np
import pandas as pd

import fit_model as F
import model as M
import retail_forecast as RF

warnings.filterwarnings("ignore")
MIN_TRAIN, STEP = 150, 30
FALSE_DOWN_TOL = 0.02


def dataset(tables, eppo: pd.DataFrame, fuel: str) -> pd.DataFrame:
    g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
    mm, ex = g.mm.astype(float), g.exref.astype(float)
    mm_dev = mm - mm.shift(1).rolling(30, min_periods=10).median()
    ex_d5 = ex - ex.shift(5)
    rows = []
    for day, r in g.iterrows():
        nxt = day + pd.Timedelta(days=1)
        if nxt not in g.index:
            continue
        f = RF.signal_features(tables, fuel, day)
        rows.append({"day": day, "y": float(g.retail[nxt] - r.retail), "x": RF.signal_vector(f), "fund": float(r.fund) if pd.notna(r.fund) else np.nan,
                     "mm_dev": float(mm_dev[day]) if pd.notna(mm_dev[day]) else 0.0, "exref_d5": float(ex_d5[day]) if pd.notna(ex_d5[day]) else 0.0})
    return pd.DataFrame(rows)


def wf(X, y):
    p = np.full(len(y), np.nan)
    i = MIN_TRAIN
    while i < len(y):
        j = min(i + STEP, len(y))
        p[i:j] = M.predict_proba(M.fit_logistic(X[:i], y[:i]), X[i:j])
        i = j
    return p


def metrics(yd, p, mask):
    m = mask & ~np.isnan(p)
    if m.sum() < 10:
        return {"n": int(m.sum()), "auc": np.nan, "brier": np.nan, "false": np.nan}
    return {"n": int(m.sum()), "auc": F.auc(yd[m], p[m]), "brier": float(np.mean((p[m] - yd[m]) ** 2)),
            "false": float(((p[m] >= RF.CALL_P) & (yd[m] == 0)).mean()), "hit": float(((p[m] >= RF.CALL_P) & (yd[m] == 1)).sum()),
            "downs": int(yd[m].sum())}


def run(tables, eppo, fuel):
    d = dataset(tables, eppo, fuel)
    y = d.y.to_numpy()
    yd = (y <= -RF.MOVE).astype(float)
    Xb = np.vstack(d.x)
    if fuel in RF.DOWN_FUND_FUELS:
        Xb = np.column_stack([Xb, np.clip(np.nan_to_num(d.fund.to_numpy()), *RF.FUND_CLIP)])
    Xe = np.column_stack([Xb, np.clip(d.mm_dev, -3, 3), np.clip(d.exref_d5, -5, 5)])
    pb, pe = wf(Xb, yd), wf(Xe, yd)
    ok = ~np.isnan(pb)
    idx = np.flatnonzero(ok)
    h = idx[len(idx) // 2]
    days = pd.to_datetime(d.day)
    masks = {"ทั้งช่วง": ok, "ครึ่งแรก": ok & (np.arange(len(y)) < h), "ครึ่งหลัง": ok & (np.arange(len(y)) >= h), "ปี 2026": ok & (days.dt.year == 2026).to_numpy()}
    print(f"\n== {fuel} : {len(y)} คู่วัน ({days.min():%Y-%m-%d} ถึง {days.max():%Y-%m-%d}) ; ทดสอบ {int(ok.sum())} วัน ; ปลีกลดจริง {int(yd[ok].sum())} วัน")
    res = {}
    for name, m in masks.items():
        a, b = metrics(yd, pb, m), metrics(yd, pe, m)
        res[name] = (a, b)
        print(f"   {name:<8} n={a['n']:>4} ลดจริง {a.get('downs', 0):>3} | AUC ลง {a['auc']:.3f} → {b['auc']:.3f} | Brier {a['brier']:.4f} → {b['brier']:.4f} | "
              f"ทายลง(>={RF.CALL_P:.2f}) ถูก {a.get('hit', 0):.0f} → {b.get('hit', 0):.0f} ผิด {a['false']:.1%} → {b['false']:.1%}")
    all_, = [res["ทั้งช่วง"]]
    c1 = (all_[1]["auc"] - all_[0]["auc"] >= 0.02) and all(res[k][1]["auc"] >= res[k][0]["auc"] for k in ("ครึ่งแรก", "ครึ่งหลัง", "ปี 2026"))
    c2 = all_[1]["brier"] < all_[0]["brier"] and res["ปี 2026"][1]["brier"] < res["ปี 2026"][0]["brier"]
    c3 = all_[1]["false"] - all_[0]["false"] <= FALSE_DOWN_TOL
    print(f"   เกณฑ์ 1 {'✓' if c1 else '✗'}  เกณฑ์ 2 {'✓' if c2 else '✗'}  เกณฑ์ 3 {'✓' if c3 else '✗'}  → {'ผ่าน' if c1 and c2 and c3 else 'ไม่ผ่าน'}")
    nov = ok & (days >= "2026-11-01").to_numpy() & (yd == 1)
    for i in np.flatnonzero(nov):
        print(f"      ปลีกลดจริง {days[i] + pd.Timedelta(days=1):%d/%m} ({y[i]:+.2f}) : โอกาสลง เดิม {pb[i]:.0%} → ใหม่ {pe[i]:.0%} "
              f"(mm_dev {d.mm_dev[i]:+.2f} , exref_d5 {d.exref_d5[i]:+.2f})")
    return c1 and c2 and c3


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data_dir")
    ap.add_argument("eppo")
    a = ap.parse_args()
    _, tables = F.load(a.data_dir)
    eppo = pd.read_csv(a.eppo, parse_dates=["day"])
    out = {fuel: run(tables, eppo, fuel) for fuel in ("B7", "G95")}
    print("\n== สรุป : " + " ; ".join(f"{k} {'ผ่าน → ใช้ E ในโมเดลฝั่งลง' if v else 'ไม่ผ่าน → คงเดิม'}" for k, v in out.items()))


if __name__ == "__main__":
    main()
