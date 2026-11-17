"""
การทดลอง: ใช้ระบบของผู้พัฒนาเดิม (โครงการ KCG iTAP) แล้วโมเดลราคาขายปลีกของเราแม่นขึ้นหรือไม่

ที่มา : ได้สคริปต์ ข้อมูล และ log ของระบบนั้นมาแล้ว (oil-bot สาขา kcg-export*) — ระบบจริงหยุดรับข้อมูลตั้งแต่ 24/06/2026 จึงเทียบคำทายรายวันไม่ได้
        ทดสอบได้เฉพาะ "วิธีการ" ของระบบนั้นบนข้อมูลของเรา : ชุดฟีเจอร์ใน train_per_fuel_custom.py และข้อมูลตลาดโลกใน external_features.csv
เทมเพลต (ตั้งก่อนรัน 2026-11-17)
  เป้าหมาย : ราคาปลีก EPPO ของวัน t+1 เทียบวัน t ; ขึ้น/ลง = ขยับ >= 10 สต. (เหมือนโมเดลที่ใช้อยู่) ; ทำนาย ณ 20:30 ของวัน t
  ชุดที่เทียบ (walk-forward ขยายหน้าต่าง ฝึกขั้นต่ำ 50 วัน ก้าว 10 วัน บนวันชุดเดียวกัน) :
     SIG      โมเดลที่ใช้อยู่ : up_n, dn_n, close_last, fund_next (โลจิสติกส์ของเรา)
     KCG      ฟีเจอร์ของผู้พัฒนา 20 ตัว ตามสคริปต์ : ราคาปลีกย้อนหลัง 1/7/30/90 แถว , ค่าเฉลี่ย 7/30/90 , ส่วนเบี่ยงเบน 7/30 , การเปลี่ยน 1/7/30/90 ,
              ราคาหน้าโรงกลั่นย้อนหลัง 1/7 , ค่าเฉลี่ย 7/30 , การเปลี่ยน 1/7 , ราคาปลีก − หน้าโรงกลั่น (โลจิสติกส์ของเรา)
     KCG-XGB  ฟีเจอร์ชุดเดียวกัน ด้วย XGBoost 3 ชั้น (อัลกอริทึมที่ระบบนั้นใช้กับแก๊สโซฮอล์ 95 T+1 ; ค่าพารามิเตอร์ที่จูนไว้ไม่ได้ส่งออกมา จึงใช้ 200 ต้น ลึก 4 อัตรา 0.1)
     SIG+KCG  ฟีเจอร์ของเรา + ของผู้พัฒนา (โลจิสติกส์ของเรา)
     SIG+EXT  ฟีเจอร์ของเรา + ตลาดโลก : การเปลี่ยน 1 และ 3 วันของ Brent , HO (ดีเซล) หรือ RBOB (G95) , USDTHB — ใช้ราคาปิดของวัน t−1 (ตลาดสหรัฐปิด ~02:30 น. ของวัน t จึงรู้ก่อน 20:30)
              ข้อมูลตลาดโลกมีถึง 15/05/2026 จึงเทียบ SIG กับ SIG+EXT เฉพาะวันที่มีข้อมูลครบ
  ตัวชี้วัด : AUC ขึ้น/ลง , Brier 3 ชั้น (ต่ำ = ดี) , Kappa (ชั้นที่ทาย = ความน่าจะเป็นสูงสุด) ; ทั้งช่วงและปี 2026
  เกณฑ์ "ช่วยให้แม่นขึ้น" (ต่อชนิด ครบทุกข้อ) : Brier ต่ำกว่า SIG โดยช่วงเชื่อมั่น 95% ของส่วนต่าง (bootstrap รายวัน 2000 รอบ) ไม่คร่อมศูนย์ และ AUC เฉลี่ยไม่ต่ำกว่า SIG
  ข้อควรระวัง : 4 ชุด x 2 ชนิด ; ราคาปลีกดีเซลถูกตรึงช่วงปี 2024–2025 ; ผลย้อนหลัง ไม่เปลี่ยนระบบจากผลนี้โดยไม่มีผลจริง

ผล (รัน 2026-11-17 ; 432 วันทดสอบ 21/08/2024–20/10/2026 ซึ่ง 182 วันอยู่ในปี 2026) : ไม่มีชุดใดผ่านเกณฑ์ ทั้งสองชนิด → ไม่นำมาใช้
  ดีเซล : SIG AUC ขึ้น/ลง 0.91/0.70 Brier 0.157 ; KCG 0.78/0.63 Brier 0.233 (แย่ลง +0.077 , 95% +0.036 ถึง +0.119) ; KCG-XGB 0.74/0.68 Brier 0.183 (แย่ลง)
          SIG+KCG 0.83/0.65 Brier 0.201 (แย่ลง +0.044 , +0.008 ถึง +0.080) ; ปี 2026 แย่ลงทุกชุดเช่นกัน (KCG Brier 0.509 เทียบ SIG 0.328)
  G95   : SIG 0.72/0.66 Brier 0.345 ; KCG 0.65/0.82 Brier 0.384 ; KCG-XGB 0.71/0.76 Brier 0.380 ; SIG+KCG 0.71/0.84 Brier 0.345 (ต่าง +0.000 , −0.040 ถึง +0.039)
          ปี 2026 : SIG 0.77/0.75 Brier 0.384 ; KCG 0.51/0.83 Brier 0.543 (แย่ลง) ; SIG+KCG 0.63/0.88 Brier 0.455
  ตลาดโลก (337 วันทดสอบ ถึง 19/05/2026 ; ปี 2026 87 วัน) : ดีเซล SIG+EXT Brier +0.005 (−0.006 ถึง +0.018) AUC ลดลง ; G95 +0.015 (−0.018 ถึง +0.046) → ไม่ช่วย
  ข้อสังเกตเดียวที่น่าตามต่อ (ยังไม่ใช่ข้อสรุป) : ฟีเจอร์โครงสร้างราคาช่วยฝั่ง "ลง" ของ G95 (AUC ลง 0.82–0.88 เทียบ 0.66–0.75) แต่ทำให้ฝั่ง "ขึ้น" และ Brier แย่ลง
     สอดคล้องกับเบาะแสระดับกองทุนของ G95 ที่พบ 16/11 — ถ้าจะใช้ ต้องทดสอบแบบใส่เฉพาะในโมเดลฝั่งลง ด้วยเกณฑ์ที่ตั้งใหม่ก่อนรัน
  สรุป : วิธีการของระบบผู้พัฒนา (ราคาย้อนหลัง + ราคาหน้าโรงกลั่นย้อนหลัง , XGBoost , ตลาดโลก) ไม่ทำให้โมเดลราคาปลีกของเราแม่นขึ้น และทำให้ดีเซลแย่ลงอย่างมีนัยสำคัญ

ใช้: python exp_retail_kcg_features.py <data_dir ของ jobber> <external_features.csv ของผู้พัฒนา> [data/eppo_daily_long.csv]
     (ต้องติดตั้ง scikit-learn และ xgboost เฉพาะสำหรับการทดลองนี้ — ไม่อยู่ใน requirements.txt ของระบบจริง)
"""
import sys
import warnings

import numpy as np
import pandas as pd

import fit_model as F
import model as M
import retail_forecast as RF

warnings.filterwarnings("ignore")
MIN_TRAIN, STEP, MOVE = 50, 10, RF.MOVE
EXT_COL = {"B7": "heating_oil_usd", "G95": "rbob_usd"}


def kcg_features(g: pd.DataFrame) -> pd.DataFrame:
    """ฟีเจอร์ตาม build_features ของ train_per_fuel_custom.py (เลื่อนตามแถว ไม่ใช่ตามวันปฏิทิน เหมือนต้นฉบับ)"""
    p, x = g.retail, g.exref
    f = pd.DataFrame(index=g.index)
    for k in (1, 7, 30, 90):
        f[f"price_lag{k}"] = p.shift(k)
        f[f"price_change_{k}d"] = p.diff(k)
    for k in (7, 30, 90):
        f[f"price_roll_mean_{k}"] = p.rolling(k).mean()
    for k in (7, 30):
        f[f"price_roll_std_{k}"] = p.rolling(k).std()
        f[f"exrefin_roll_mean_{k}"] = x.rolling(k).mean()
    for k in (1, 7):
        f[f"exrefin_lag{k}"] = x.shift(k)
        f[f"exrefin_change_{k}d"] = x.diff(k)
    f["price_minus_exrefin"] = p - x
    return f


def ext_features(ext: pd.DataFrame, fuel: str, days) -> pd.DataFrame:
    e = ext.set_index("date").sort_index()[["brent_usd", EXT_COL[fuel], "usdthb"]].ffill(limit=3)
    out = {}
    for c in e.columns:
        for k in (1, 3):
            out[f"{c}_d{k}"] = (e[c].pct_change(k) * 100).shift(1)        # shift(1) : ใช้ราคาปิดของวัน t−1 สำหรับการตัดสินใจวัน t
    return pd.DataFrame(out).reindex(e.index.union(pd.DatetimeIndex(days))).ffill(limit=3).reindex(days)


def build(tables, eppo, ext, fuel, last_signal_day):
    g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
    kf, rows = kcg_features(g), []
    for day, r in g.iterrows():
        nxt = day + pd.Timedelta(days=1)
        if nxt not in g.index or day > last_signal_day:
            continue
        rows.append({"day": day, "y": float(g.retail[nxt] - r.retail), **RF.signal_features(tables, fuel, day)})
    df = pd.DataFrame(rows)
    df = df.join(kf, on="day").join(ext_features(ext, fuel, df.day).add_prefix("x_"), on="day")
    return df.dropna(subset=list(kf.columns)).reset_index(drop=True)


def wf_logit(X, yu, yd):
    pu, pdn = np.full(len(yu), np.nan), np.full(len(yu), np.nan)
    i = MIN_TRAIN
    while i < len(yu):
        j = min(i + STEP, len(yu))
        for yy, out in ((yu, pu), (yd, pdn)):
            out[i:j] = M.predict_proba(M.fit_logistic(X[:i], yy[:i]), X[i:j]) if 0 < yy[:i].sum() < i else yy[:i].mean()
        i = j
    return pu, pdn


def wf_xgb(X, yu, yd):
    from xgboost import XGBClassifier
    cls = np.where(yu == 1, 2, np.where(yd == 1, 0, 1))
    pu, pdn = np.full(len(yu), np.nan), np.full(len(yu), np.nan)
    i = MIN_TRAIN
    while i < len(yu):
        j = min(i + STEP, len(yu))
        present = np.unique(cls[:i])
        if len(present) < 2:
            pu[i:j], pdn[i:j] = yu[:i].mean(), yd[:i].mean()
        else:
            remap = {c: k for k, c in enumerate(present)}
            m = XGBClassifier(n_estimators=200, max_depth=4, learning_rate=0.1, subsample=0.8, colsample_bytree=0.8, random_state=42, n_jobs=2, eval_metric="mlogloss")
            m.fit(X[:i], np.array([remap[c] for c in cls[:i]]))
            pr = m.predict_proba(X[i:j])
            pu[i:j] = pr[:, remap[2]] if 2 in remap else 0.0
            pdn[i:j] = pr[:, remap[0]] if 0 in remap else 0.0
        i = j
    return pu, pdn


def brier_days(yu, yd, pu, pdn):
    return (pu - yu) ** 2 + (pdn - yd) ** 2 + ((1 - pu - pdn) - (1 - yu - yd)) ** 2


def kappa(yu, yd, pu, pdn):
    true = np.where(yu == 1, 2, np.where(yd == 1, 0, 1))
    pred = np.argmax(np.column_stack([pdn, np.clip(1 - pu - pdn, 0, 1), pu]), axis=1)
    po = float((pred == true).mean())
    pe = sum(float((pred == c).mean()) * float((true == c).mean()) for c in (0, 1, 2))
    return 0.0 if pe >= 1 else (po - pe) / (1 - pe)


def ci(diff, reps=2000, seed=7):
    rng = np.random.default_rng(seed)
    b = [diff[rng.integers(0, len(diff), len(diff))].mean() for _ in range(reps)]
    return float(np.percentile(b, 2.5)), float(np.percentile(b, 97.5))


def main():
    _, tables = F.load(sys.argv[1])
    ext = pd.read_csv(sys.argv[2], parse_dates=["date"])
    eppo = pd.read_csv(sys.argv[3] if len(sys.argv) > 3 else RF.LONG_HISTORY_FILE, parse_dates=["day"])
    last = tables.headsup.ts.max().normalize() - pd.Timedelta(days=1)
    for fuel in ("B7", "G95"):
        df = build(tables, eppo, ext, fuel, last)
        y = df.y.to_numpy()
        yu, yd = (y >= MOVE).astype(float), (y <= -MOVE).astype(float)
        sig = np.vstack([RF.signal_vector(r) for r in df.to_dict("records")])
        kcols = [c for c in df.columns if c.startswith(("price_", "exrefin_"))]
        xcols = [c for c in df.columns if c.startswith("x_")]
        K = df[kcols].to_numpy(float)
        has_ext = df[xcols].notna().all(axis=1).to_numpy()
        res = {"SIG": wf_logit(sig, yu, yd), "KCG": wf_logit(K, yu, yd), "KCG-XGB": wf_xgb(K, yu, yd), "SIG+KCG": wf_logit(np.column_stack([sig, K]), yu, yd)}
        ok = ~np.isnan(res["SIG"][0])
        y26 = (df.day >= pd.Timestamp("2026-01-01")).to_numpy()
        print(f"\n== {fuel} : {int(ok.sum())} วันทดสอบ ({df.day[ok].min():%d/%m/%y}–{df.day[ok].max():%d/%m/%y}) ; ขึ้น {yu[ok].mean():.0%} ลง {yd[ok].mean():.0%} ; ปี 2026 {int((ok & y26).sum())} วัน")
        print(f"{'ชุด':<10}{'ช่วง':<10}{'AUCขึ้น':>8}{'AUCลง':>7}{'Brier':>8}{'Kappa':>7}{'ΔBrier เทียบ SIG (95%)':>30}")

        def show(name, pu, pdn, mask, label, base):
            b = brier_days(yu, yd, pu, pdn)[mask]
            d = b - base
            lo, hi = ci(d) if name != "SIG" else (0.0, 0.0)
            tag = "" if name == "SIG" else f"{d.mean():+.3f} ({lo:+.3f} ถึง {hi:+.3f})" + (" ดีขึ้น" if hi < 0 else " แย่ลง" if lo > 0 else "")
            print(f"{name:<10}{label:<10}{F.auc(yu[mask], pu[mask]):>8.2f}{F.auc(yd[mask], pdn[mask]):>7.2f}{b.mean():>8.3f}{kappa(yu[mask], yd[mask], pu[mask], pdn[mask]):>7.2f}{tag:>30}")

        for label, mask in (("ทั้งช่วง", ok), ("ปี 2026", ok & y26)):
            base = brier_days(yu, yd, *res["SIG"])[mask]
            for name, (pu, pdn) in res.items():
                show(name, pu, pdn, mask, label, base)
        # ตลาดโลก : ฝึกและทดสอบเฉพาะวันที่มีข้อมูลครบ
        sub = np.where(has_ext)[0]
        Xs, Xe = sig[sub], np.column_stack([sig[sub], df[xcols].to_numpy(float)[sub]])
        a, b2 = wf_logit(Xs, yu[sub], yd[sub]), wf_logit(Xe, yu[sub], yd[sub])
        m = ~np.isnan(a[0])
        d26 = y26[sub]
        print(f"   ตลาดโลก (เฉพาะวันที่มีข้อมูล ถึง {df.day[sub].max():%d/%m/%y}) : {int(m.sum())} วันทดสอบ ; ปี 2026 {int((m & d26).sum())} วัน")
        for label, mask in (("ทั้งช่วง", m), ("ปี 2026", m & d26)):
            if mask.sum() < 20:
                continue
            base = brier_days(yu[sub], yd[sub], *a)[mask]
            for name, (pu, pdn) in (("SIG", a), ("SIG+EXT", b2)):
                bb = brier_days(yu[sub], yd[sub], pu, pdn)[mask]
                d = bb - base
                lo, hi = ci(d) if name != "SIG" else (0.0, 0.0)
                tag = "" if name == "SIG" else f"{d.mean():+.3f} ({lo:+.3f} ถึง {hi:+.3f})" + (" ดีขึ้น" if hi < 0 else " แย่ลง" if lo > 0 else "")
                print(f"{name:<10}{label:<10}{F.auc(yu[sub][mask], pu[mask]):>8.2f}{F.auc(yd[sub][mask], pdn[mask]):>7.2f}{bb.mean():>8.3f}{kappa(yu[sub][mask], yd[sub][mask], pu[mask], pdn[mask]):>7.2f}{tag:>30}")


if __name__ == "__main__":
    main()
