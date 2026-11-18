"""
การทดลอง (ต่อจาก exp_retail_mops.py): ใส่ "MOPS สะสมตั้งแต่ราคาปลีกปรับครั้งล่าสุด" เฉพาะโมเดลฝั่ง "ลง" ของ G95

ที่มา : ใน exp_retail_mops.py ชุด CUM (ใส่ทั้งฝั่งขึ้นและลง) ไม่ผ่าน แต่ AUC ฝั่งลงของ G95 ดีขึ้น 0.684 → 0.785
   สมมติฐานนี้จึงมาจากการดูผลบนข้อมูลชุดเดียวกัน (เสี่ยงเลือกชุดที่บังเอิญดี) — จึงตั้งเกณฑ์เข้มกว่าเดิมก่อนรัน
เทมเพลต (ตั้งก่อนรัน 2026-11-18)
  ชุดที่ทดสอบ (G95 เท่านั้น) : DOWN_CUM = ฝั่งลง = SIG 4 ตัว + ระดับกองทุน (เหมือนที่ใช้จริง) + cum_mops ; ฝั่งขึ้น = SIG 4 ตัว (เหมือนที่ใช้จริง ไม่เปลี่ยน)
     cum_mops นิยามเดียวกับ exp_retail_mops.cum_mops (ผลรวม MOPS ปิด G95 ที่ mops_date ≥ วันปลีกปรับครั้งล่าสุด โพสต์ ≤ 20:30 ; เพดาน ±40)
  ฐานเปรียบเทียบ : ที่ใช้จริง (SIG ; ฝั่งลง + กองทุน) บนวันทดสอบชุดเดียวกัน ; walk-forward ฝึกขั้นต่ำ 50 วัน ก้าว 10 วัน
  เกณฑ์ผ่าน (ต้องครบทุกข้อ) :
     1) Brier 3 ชั้นทั้งช่วงต่ำกว่า โดยช่วงเชื่อมั่น 95% ของส่วนต่าง (bootstrap รายวัน) ต่ำกว่าศูนย์ทั้งช่วง
     2) Brier ปี 2026 ต่ำกว่า (ไม่ใช่แค่ไม่แย่ลง — ช่วงที่กลไกราคาตรงกับปัจจุบัน)
     3) Brier ไม่แย่ลงทั้งครึ่งแรกและครึ่งหลังของวันทดสอบ
     4) วันที่ MOPS G95 ร่วง ≤ −4 : โอกาสลงเฉลี่ยห่างจากอัตราที่ลงจริงไม่เกิน 10 จุด (ไม่ทายเกินจริง)
  ข้อควรระวัง : ไม่มีช่วงข้อมูลที่ไม่เคยเห็นเหลือให้ทดสอบ (ข้อมูลฝึกถึง 21/10/2026) ; ผ่านแล้วต้องติดป้ายทดลองและเก็บผลจริงคู่ขนานก่อนใช้แทน

ผล (รัน 2026-11-18 ; 502 วันทดสอบ 04/2024–10/2026 ; ปี 2026 182 วัน ; วัน MOPS G95 ร่วง ≤ −4 มี 19 วัน ลงจริง 32%) : ไม่ผ่าน (ผ่าน 1 จาก 4 ข้อ) → คงโมเดลเดิม
  1) Brier ทั้งช่วง 0.3453 → 0.3250 (−0.048 ถึง +0.006 คร่อมศูนย์) ไม่ผ่าน
  2) Brier ปี 2026 0.3600 → 0.3746 (+0.015 แย่ลง) ไม่ผ่าน
  3) ครึ่งแรก −0.034 ครึ่งหลัง −0.007 ผ่าน
  4) วันร่วงแรง โอกาสลง 28% → 42% เทียบลงจริง 32% (ห่างเกิน 10 จุดเล็กน้อย) ไม่ผ่าน — ทายลงเกินจริง
  ข้อมูลประกอบ : AUC ลง 0.684 → 0.785 (ปี 2026 0.842 → 0.866) ; ฟันธงลง 17 → 43 ครั้ง ถูก 5 → 17 ครั้ง ผิดทิศ 1 → 2
  การตีความ : ตัวแปรสะสมช่วยจัดอันดับวันที่จะลง แต่ทำให้โอกาสลงสูงเกินจริงโดยเฉพาะปี 2026 (MOPS ร่วงต่อเนื่องหลายสัปดาห์ แต่ปลีกไม่ได้ลดทุกวัน)
     ยังไม่ควรใช้แทนโมเดลเดิม ; ทดสอบซ้ำได้เมื่อมีข้อมูลหลัง 21/10/2026 สะสมพอ (ช่วงที่ไม่เคยเห็น) โดยใช้เกณฑ์ชุดนี้

ใช้: python exp_retail_cum_down.py <data_dir ของ jobber> [data/eppo_daily_long.csv]
"""
import sys
import warnings

import numpy as np
import pandas as pd

import exp_retail_kcg_features as K
import exp_retail_mops as X
import fit_model as F
import retail_forecast as RF

warnings.filterwarnings("ignore")
FUEL = "G95"


def main():
    _, tables = F.load(sys.argv[1])
    eppo = pd.read_csv(sys.argv[2] if len(sys.argv) > 2 else RF.LONG_HISTORY_FILE, parse_dates=["day"])
    last = tables.headsup.ts.max().normalize() - pd.Timedelta(days=1)
    df = X.dataset(tables, eppo, FUEL, last)
    df = df[df.fund.notna()].reset_index(drop=True)
    y = df.y.to_numpy()
    yu, yd = (y >= RF.MOVE).astype(float), (y <= -RF.MOVE).astype(float)
    sig = X.vectors(df, 8.0, False)
    fund = np.clip(df.fund.to_numpy(dtype=float), *RF.FUND_CLIP)
    cum = df.cum.to_numpy()

    pu, _ = K.wf_logit(sig, yu, yd)                                        # ฝั่งขึ้น : เหมือนกันทั้งสองชุด
    _, pd0 = K.wf_logit(np.column_stack([sig, fund]), yu, yd)               # ที่ใช้จริง
    _, pd1 = K.wf_logit(np.column_stack([sig, fund, cum]), yu, yd)          # DOWN_CUM
    ok = ~np.isnan(pu) & ~np.isnan(pd0) & ~np.isnan(pd1)
    b0, b1 = K.brier_days(yu, yd, pu, pd0), K.brier_days(yu, yd, pu, pd1)
    d = (b1 - b0)
    idx = np.flatnonzero(ok)
    h1, h2 = idx[: len(idx) // 2], idx[len(idx) // 2:]
    y26 = ok & (df.day >= pd.Timestamp("2026-01-01")).to_numpy()
    raw = pd.to_numeric(df.close_raw, errors="coerce").to_numpy()
    big = ok & (raw <= X.BIG_DROP[FUEL])
    lo, hi = K.ci(d[ok])

    c1 = hi < 0
    c2 = d[y26].mean() < 0
    c3 = d[h1].mean() <= 0 and d[h2].mean() <= 0
    gap = float(np.mean(pd1[big]) - yd[big].mean())
    c4 = abs(gap) <= 0.10
    print(f"G95 : {int(ok.sum())} วันทดสอบ ({df.day[ok].min():%d/%m/%Y}–{df.day[ok].max():%d/%m/%Y}) ; ลง {yd[ok].mean():.0%} ; ปี 2026 {int(y26.sum())} วัน ; MOPS ร่วง ≤ −4 {int(big.sum())} วัน (ลงจริง {yd[big].mean():.0%})")
    print(f"   Brier ทั้งช่วง {b0[ok].mean():.4f} → {b1[ok].mean():.4f} ต่าง {d[ok].mean():+.4f} (95% {lo:+.4f} ถึง {hi:+.4f}) → เกณฑ์ 1 {'ผ่าน' if c1 else 'ไม่ผ่าน'}")
    print(f"   Brier ปี 2026 {b0[y26].mean():.4f} → {b1[y26].mean():.4f} ({d[y26].mean():+.4f}) → เกณฑ์ 2 {'ผ่าน' if c2 else 'ไม่ผ่าน'}")
    print(f"   ครึ่งแรก {d[h1].mean():+.4f} ครึ่งหลัง {d[h2].mean():+.4f} → เกณฑ์ 3 {'ผ่าน' if c3 else 'ไม่ผ่าน'}")
    print(f"   วัน MOPS ร่วงแรง โอกาสลงเฉลี่ย {np.mean(pd0[big]):.0%} → {np.mean(pd1[big]):.0%} (ลงจริง {yd[big].mean():.0%} ; ห่าง {gap:+.0%}) → เกณฑ์ 4 {'ผ่าน' if c4 else 'ไม่ผ่าน'}")
    print(f"   AUC ลง {F.auc(yd[ok], pd0[ok]):.3f} → {F.auc(yd[ok], pd1[ok]):.3f} ; ปี 2026 {F.auc(yd[y26], pd0[y26]):.3f} → {F.auc(yd[y26], pd1[y26]):.3f}")
    # ระดับคำทายตามเกณฑ์จริง (retail_forecast.decide) : ฟันธงลงกี่ครั้ง ถูกกี่ครั้ง
    for name, pdn in (("ที่ใช้จริง", pd0), ("DOWN_CUM", pd1)):
        calls = [RF.decide({"ลง": a, "นิ่ง": 1 - a - b, "ขึ้น": b}) for a, b in zip(pdn[ok], pu[ok])]
        dn = np.array([c == "ลง" for c in calls])
        print(f"   {name:<10} ฟันธงลง {int(dn.sum())} ครั้ง ถูก {int(yd[ok][dn].sum())} ; ผิดทิศ (ขึ้นจริง) {int(yu[ok][dn].sum())}")
    print(f"   สรุป : {'ผ่านครบทุกข้อ' if (c1 and c2 and c3 and c4) else 'ไม่ผ่าน → คงโมเดลเดิม'}")


if __name__ == "__main__":
    main()
