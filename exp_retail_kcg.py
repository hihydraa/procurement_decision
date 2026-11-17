"""
การทดลอง: เทียบโมเดลราคาขายปลีกของเรา (สัญญาณ Jobber) กับโมเดลที่เคยจ้างทำ (โครงการ iTAP "KCG Oil Price Forecasting" มิ.ย. 2569) ด้วยมาตรวัดเดียวกัน
            และลองขอบฟ้า T+2 ตามขอบเขตของโครงการนั้น

ที่มา : เอกสารนำเสนอของโครงการ (ไม่มีโค้ด/โมเดล/ผลรายวันให้ใช้) รายงานว่า ทำนายทิศ UP/DOWN/STABLE ของราคาปลีก T+1 และ T+2 (เกณฑ์ ±0.05 บาท) 7 ชนิด
        ข้อมูลเข้า : PTT, โครงสร้างราคา EPPO 24 ปี, NYMEX (Brent/WTI/RBOB/HO), USDTHB ; ทำนาย 06:30 น. ; ผลที่รายงาน : F1 macro 0.50 , Cost-weighted F1 0.495 ,
        Kappa 0.05–0.08 , Balanced accuracy 0.556 ; ต่อชนิด F1 ดีเซล 0.59 , แก๊สโซฮอล์ 95 0.49 ; วันที่ราคาไม่เปลี่ยน 86%
เทมเพลต (ตั้งก่อนรัน 2026-11-17)
  คำถาม 1 : โมเดลสัญญาณของเรา (up_n, dn_n, close_last, fund_next ณ 20:30 ของวัน t) ได้ Kappa / F1 macro / Cost-weighted F1 / Balanced accuracy เท่าไร
            เมื่อวัดแบบเดียวกัน (3 ชั้น ; ชั้นที่ทาย = ความน่าจะเป็นสูงสุด) ที่เกณฑ์ ±0.05 (ของโครงการ) และ ±0.10 (ของเรา)
  คำถาม 2 : สัญญาณชุดเดียวกันทาย T+2 (ราคาปลีกวัน t+2 เทียบวัน t) ได้หรือไม่
  ข้อมูล : ราคาปลีกจาก data/eppo_daily_long.csv ; สัญญาณจากประวัติ Jobber ; walk-forward ขยายหน้าต่าง ฝึกขั้นต่ำ 50 วัน ก้าว 10 วัน (เท่า exp_retail_search.py)
  มาตรวัด : Kappa (Cohen) , F1 macro , Cost-weighted F1 = (2·F1ขึ้น + 2·F1ลง + F1นิ่ง)/5 ตามนิยามในเอกสาร , Balanced accuracy , AUC ขึ้น/ลง ; รายงานแยกทั้งช่วงและปี 2026
  ฐานเปรียบเทียบ : "ทายนิ่งทุกวัน" (Kappa = 0)
  การตีความ (ตั้งไว้ก่อน) : ช่วงเวลา จำนวนวัน และเวลาที่ทำนายต่างกัน (ของโครงการ 06:30 ล่วงหน้า ~22 ชม. ; ของเรา 20:30 ล่วงหน้า ~9 ชม. และรู้ MOPS ปิด/ประกาศของผู้ค้าแล้ว)
       จึงเทียบได้เพียงระดับ ไม่ใช่การแข่งบนข้อมูลชุดเดียวกัน ; ไม่เปลี่ยนระบบจากผลนี้
  ไม่ได้ทดสอบ : NYMEX / USDTHB เป็นข้อมูลเข้า — ณ 20:30 เรารู้ MOPS ปิดของวันเดียวกัน (ราคาผลิตภัณฑ์ที่ไทยใช้อ้างอิงโดยตรง) ซึ่งใหม่กว่า NYMEX ของคืนก่อน ; และยังไม่มีประวัติ NYMEX/FX ในระบบ

ผล (รัน 2026-11-17 ; T+1 502 วันทดสอบ 04/2024–10/2026 ซึ่ง 182 วันอยู่ในปี 2026 ; T+2 375 วัน ; เกณฑ์ 0.05 กับ 0.10 ให้ผลเท่ากันเพราะปลีกปรับครั้งละ >= 10 สต. เสมอ)
  คำถาม 1 (ชั้นที่ทาย = ความน่าจะเป็นสูงสุด) :
    ดีเซล T+1 : Kappa 0.08 , F1 macro 0.37 , Cost-weighted F1 0.25 , Balanced acc 0.36 , AUC ขึ้น/ลง 0.85/0.75 ; ฟันธงเพียง 3 ครั้ง (ถูก 2 ผิดทิศ 0)
    G95   T+1 : Kappa 0.05 (ปี 2026 : 0.14) , F1 macro 0.36 (0.40) , CW-F1 0.25 (0.31) , AUC 0.70/0.63 (0.79/0.75) ; ฟันธง 18 ครั้ง ถูก 5 ผิดทิศ 1
    → Kappa ระดับเดียวกับที่โครงการรายงาน (0.05–0.08) แต่ F1 ต่ำกว่า (0.50) เพราะโมเดลของเราแทบไม่ฟันธง — AUC สูงแปลว่าจัดลำดับวันได้ดี แต่เกณฑ์ฟันธง 0.50 สูงเกินไปสำหรับเหตุการณ์ที่เกิด 8–20% ของวัน
  คำถาม 2 (T+2) : ดีเซล Kappa 0.02 ; G95 Kappa 0.08–0.09 , ฟันธง 50 ครั้ง ถูก 20 → ไม่ดีพอจะเพิ่มขอบฟ้า T+2
  การสำรวจเพิ่ม (หลังเห็นผลข้างบน ไม่อยู่ในเทมเพลต — เกณฑ์ถูกเลือกโดยดูผล จึงมองดีเกินจริง) : ลดเกณฑ์ "ส่งสัญญาณ" เป็น 0.20 ตามแนวคิด cost-weighted ของโครงการ
    ดีเซล T+1 : Kappa 0.25 , F1 macro 0.49 , CW-F1 0.40 ; ส่งสัญญาณ 22 ครั้ง ราคาปรับตามทิศ 9 ไม่ปรับ 13 ผิดทิศ 0
    G95 T+1 ปี 2026 : Kappa 0.25 , F1 macro 0.51 , CW-F1 0.45 ; ส่งสัญญาณ 47 ครั้ง ตามทิศ 15 ไม่ปรับ 30 ผิดทิศ 2 (ทั้งช่วง Kappa 0.16 ; 110 ครั้ง ตามทิศ 27 ไม่ปรับ 78)
    → ใช้เป็นระดับ "เฝ้าระวัง" ได้ (ราว 4 ใน 10 ครั้งราคาปรับจริง แทบไม่ผิดทิศ) ไม่ใช่คำฟันธง ; ต้องบันทึกผลจริงก่อนเชื่อ
  สิ่งที่นำมาใช้ได้จากโครงการ : (ก) มาตรวัด Kappa / Cost-weighted F1 (ข) แนวคิดให้น้ำหนักการพลาดวันที่ราคาปรับมากกว่าการเตือนผิด → ระดับเฝ้าระวัง
  สิ่งที่ไม่นำมาใช้ : T+2 , โมเดลแยก 14 ตัว / deep learning (Kappa ที่โครงการรายงานเองอยู่ที่ 0.05–0.08) , NYMEX/USDTHB (ดูหัวข้อ "ไม่ได้ทดสอบ")
  ยังขาด : โค้ด เอกสารเทคนิค (model-findings.md, data_and_features.md) และคำทายรายวันของระบบนั้น — มีแล้วจึงเทียบบนวันเดียวกันได้ และขยายชนิดน้ำมัน (G91/B20/E20) ได้

ใช้: python exp_retail_kcg.py <data_dir ของ jobber> [data/eppo_daily_long.csv]
"""
import sys
import warnings

import numpy as np
import pandas as pd

import fit_model as F
import model as M
import retail_forecast as RF

warnings.filterwarnings("ignore")
MIN_TRAIN, STEP = 50, 10


def dataset(tables, eppo, fuel, last_signal_day, horizon):
    g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
    rows = []
    for day, r in g.iterrows():
        tgt = day + pd.Timedelta(days=horizon)
        if tgt not in g.index or day > last_signal_day:
            continue
        rows.append({"day": day, "y": float(g.retail[tgt] - r.retail), **RF.signal_features(tables, fuel, day)})
    return pd.DataFrame(rows)


def walk(df, thr):
    X = np.vstack([RF.signal_vector(r) for r in df.to_dict("records")])
    y = df.y.to_numpy()
    yu, yd = (y >= thr).astype(float), (y <= -thr).astype(float)
    pu, pdn = np.full(len(y), np.nan), np.full(len(y), np.nan)
    i = MIN_TRAIN
    while i < len(y):
        j = min(i + STEP, len(y))
        for yy, out in ((yu, pu), (yd, pdn)):
            out[i:j] = M.predict_proba(M.fit_logistic(X[:i], yy[:i]), X[i:j]) if 0 < yy[:i].sum() < i else yy[:i].mean()
        i = j
    return yu, yd, pu, pdn


def metrics(yu, yd, pu, pdn, mask):
    yu, yd, pu, pdn = yu[mask], yd[mask], pu[mask], pdn[mask]
    true = np.where(yu == 1, 2, np.where(yd == 1, 0, 1))                    # 0 ลง 1 นิ่ง 2 ขึ้น
    pred = np.argmax(np.column_stack([pdn, np.clip(1 - pu - pdn, 0, 1), pu]), axis=1)
    f1, rec = [], []
    for c in (0, 1, 2):
        tp = float(((pred == c) & (true == c)).sum())
        p, r = tp / max((pred == c).sum(), 1), tp / max((true == c).sum(), 1)
        f1.append(0.0 if p + r == 0 else 2 * p * r / (p + r))
        rec.append(r if (true == c).any() else np.nan)
    po = float((pred == true).mean())
    pe = sum(float((pred == c).mean()) * float((true == c).mean()) for c in (0, 1, 2))
    return {"n": int(len(true)), "stable": float((true == 1).mean()), "acc": po, "kappa": 0.0 if pe >= 1 else (po - pe) / (1 - pe), "f1_macro": float(np.mean(f1)),
            "cw_f1": (2 * f1[0] + 2 * f1[2] + f1[1]) / 5, "bal_acc": float(np.nanmean(rec)), "auc_up": F.auc(yu, pu), "auc_dn": F.auc(yd, pdn),
            "calls": int((pred != 1).sum()), "calls_right": int(((pred != 1) & (pred == true)).sum()), "calls_opp": int(((pred != 1) & (true != 1) & (pred != true)).sum())}


def main():
    _, tables = F.load(sys.argv[1])
    eppo = pd.read_csv(sys.argv[2] if len(sys.argv) > 2 else RF.LONG_HISTORY_FILE, parse_dates=["day"])
    last = tables.headsup.ts.max().normalize() - pd.Timedelta(days=1)
    print(f"{'ชนิด':<5}{'ขอบฟ้า':<7}{'เกณฑ์':<6}{'ช่วง':<10}{'n':>5}{'นิ่ง':>6}{'Acc':>6}{'Kappa':>7}{'F1mac':>7}{'CW-F1':>7}{'BalAcc':>8}{'AUCขึ้น':>8}{'AUCลง':>7}{'ฟันธง':>7}{'ถูก':>5}{'ผิดทิศ':>7}")
    for fuel in ("B7", "G95"):
        for horizon in (1, 2):
            df = dataset(tables, eppo, fuel, last, horizon)
            for thr in (0.05, 0.10):
                yu, yd, pu, pdn = walk(df, thr)
                ok = ~np.isnan(pu)
                for label, mask in (("ทั้งช่วง", ok), ("ปี 2026", ok & (df.day >= pd.Timestamp("2026-01-01")).to_numpy())):
                    m = metrics(yu, yd, pu, pdn, mask)
                    print(f"{fuel:<5}{'T+' + str(horizon):<7}{thr:<6}{label:<10}{m['n']:>5}{m['stable']:>6.0%}{m['acc']:>6.2f}{m['kappa']:>7.2f}{m['f1_macro']:>7.2f}{m['cw_f1']:>7.2f}{m['bal_acc']:>8.2f}"
                          f"{m['auc_up']:>8.2f}{m['auc_dn']:>7.2f}{m['calls']:>7d}{m['calls_right']:>5d}{m['calls_opp']:>7d}", flush=True)


if __name__ == "__main__":
    main()
