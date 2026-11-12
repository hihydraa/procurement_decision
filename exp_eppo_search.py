"""
การทดลอง: ราคาขายปลีกและราคาขายส่ง EPPO ของวันพรุ่งนี้ พยากรณ์ ณ 20:30 ของวันนี้ ได้แม่นแค่ไหน และฟีเจอร์ไหนช่วย (อ่านอย่างเดียว)

เทมเพลต (ตั้งก่อนรัน)
  สมมติฐาน : ปลีก/ขายส่ง EPPO ของวัน t+1 ถูกกำหนดคืนวัน t พร้อมกับที่ผู้ค้าส่งประกาศราคาเที่ยงคืน
     ดังนั้น "ราคาที่ผู้ค้าประกาศล่วงหน้าแล้ว" (PEND) และประกาศ "มีปรับขึ้น/ลง" ของวัน t ควรทายปลีก t+1 ได้ดีกว่าค่าการตลาดหรือ MOPS อย่างเดียว
  เป้าหมาย : ปลีก(t+1) − ปลีก(t) และ WS(t+1) − WS(t) จาก Entry_Eppo (ไม่นับแถว CARRIED ; ใช้เฉพาะวันติดกัน) ; ขึ้น/ลง = ขยับ >= 10 สต.
  ตัวชี้วัด : AUC (walk-forward ฝึกขั้นต่ำ 50 วัน ก้าว 10 วัน) , ทิศถูกเมื่อฟันธง , MAE เทียบทายไม่เปลี่ยน , ครึ่งแรก/ครึ่งหลัง
  เกณฑ์ผ่าน (ตั้งก่อนรัน) : AUC >= 0.70 ทั้งขึ้นและลง และไม่ต่ำกว่า 0.60 ในครึ่งใดครึ่งหนึ่ง ; n น้อย (<150 วัน) ถือเป็นหลักฐานเบื้องต้นเท่านั้น
  เวลาที่รู้จริง : ฟีเจอร์ใช้ข้อมูลที่โพสต์ <= 20:30 วัน t ; ค่า EPPO ของวัน t (ค่าการตลาด, ΔWS วันนี้) ออกตอนเช้าวัน t จึงใช้ได้

ใช้: python exp_eppo_search.py <data_dir ของ jobber> <eppo_daily.csv>
"""
import sys
import warnings

import numpy as np
import pandas as pd

import exp_model_search as X
import fit_model as F
import model as M
import signals as S
from diag_rule_backtest import MEDIAN_MM

warnings.filterwarnings("ignore")
MIN_TRAIN, STEP, MOVE = 50, 10, 0.10


def build(prices, tables, eppo, fuel):
    kb = X.KnownBook(prices, fuel)
    g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
    last_price_day = pd.to_datetime(prices.msg_date).max()
    rows = []
    for day, r in g.iterrows():
        nxt, prev = day + pd.Timedelta(days=1), day - pd.Timedelta(days=1)
        if nxt not in g.index or prev not in g.index or day > last_price_day - pd.Timedelta(days=1):
            continue
        asof = day + pd.Timedelta(hours=20.5)
        f = S.build_features(asof, tables, fuel)
        f.update(X.extra_features(kb, asof, 24))
        f.update({"date": day, "dret": float(g.retail[nxt] - r.retail), "dws": float(g.ws[nxt] - r.ws), "mm_gap": float(r.mm - MEDIAN_MM[fuel]),
                  "dws_today": float(r.ws - g.ws[prev]), "dret_today": float(r.retail - g.retail[prev]), "dow": day.dayofweek})
        rows.append(f)
    return pd.DataFrame(rows)


def c(v, lo, hi):
    return np.clip(np.nan_to_num(np.asarray(v, float), nan=0.0), lo, hi)


def feature_sets(df):
    pend = c(df.pend, -3, 3)
    sig = np.column_stack([c(df.up_n, 0, 4), c(df.dn_n, 0, 4), c(df.close_last, -8, 8), c(df.fund_next, -3, 3)])
    pnd = np.column_stack([pend, c(df.pend_up_n, 0, 8), c(df.pend_dn_n, 0, 8)])
    mm = np.column_stack([c(df.mm_gap, -3, 3)])
    eppo = np.column_stack([c(df.mm_gap, -3, 3), c(df.dws_today, -3, 3), c(df.dret_today, -3, 3)])
    sat = np.column_stack([(df.dow == 5).to_numpy(float), (df.dow == 6).to_numpy(float)])
    return {"MM": mm, "SIG": sig, "PEND": pnd, "SIG+PEND": np.column_stack([sig, pnd]), "SIG+PEND+MM": np.column_stack([sig, pnd, mm]),
            "ALL": np.column_stack([sig, pnd, eppo, sat])}


def wf(Xm, y, kind="logit"):
    p = np.full(len(y), np.nan)
    i = MIN_TRAIN
    while i < len(y):
        j = min(i + STEP, len(y))
        if kind == "logit":
            p[i:j] = M.predict_proba(M.fit_logistic(Xm[:i], y[:i]), Xm[i:j]) if 0 < y[:i].sum() < i else y[:i].mean()
        else:
            mu, sd = Xm[:i].mean(0), Xm[:i].std(0)
            sd[sd < 1e-9] = 1
            A = np.column_stack([(Xm[:i] - mu) / sd, np.ones(i)])
            R = np.eye(A.shape[1]) * 5.0
            R[-1, -1] = 0
            p[i:j] = np.column_stack([(Xm[i:j] - mu) / sd, np.ones(j - i)]) @ np.linalg.solve(A.T @ A + R, A.T @ y[:i])
        i = j
    return p


def report(fuel, df, target, label):
    y = df[target].to_numpy()
    yu, yd = (y >= MOVE).astype(float), (y <= -MOVE).astype(float)
    sets = feature_sets(df)
    print(f"\n== {fuel} {label} : {len(df)} วัน ({df.date.min():%d/%m/%y}–{df.date.max():%d/%m/%y}) ; ขึ้น {yu.mean():.0%} ลง {yd.mean():.0%} นิ่ง {1 - yu.mean() - yd.mean():.0%}")
    print(f"{'ชุด':<13}{'AUCขึ้น':>8}{'AUCลง':>8}{'ครึ่ง1':>8}{'ครึ่ง2':>8}{'ฟันธง%':>8}{'ถูกทิศ':>8}{'ผิดทิศ':>8}{'MAE':>7}{'MAE0':>7}")
    for name, Xm in sets.items():
        pu, pd_ = wf(Xm, yu), wf(Xm, yd)
        ok = ~np.isnan(pu)
        half = ok.sum() // 2
        au, ad = F.auc(yu[ok], pu[ok]), F.auc(yd[ok], pd_[ok])
        hh = lambda sl: np.nanmean([F.auc(yu[ok][sl], pu[ok][sl]), F.auc(yd[ok][sl], pd_[ok][sl])])
        up = (pu[ok] >= 0.5) & (pu[ok] > pd_[ok])
        dn = (pd_[ok] >= 0.5) & (pd_[ok] > pu[ok])
        call = up | dn
        signed = np.where(up, y[ok], -y[ok])
        ph = wf(Xm, y, "ridge")[ok]
        print(f"{name:<13}{au:>8.2f}{ad:>8.2f}{hh(slice(0, half)):>8.2f}{hh(slice(half, None)):>8.2f}{call.mean():>8.0%}"
              f"{((signed >= MOVE) & call).sum() / max(call.sum(), 1):>8.0%}{((signed <= -MOVE) & call).sum() / max(call.sum(), 1):>8.0%}"
              f"{np.abs(y[ok] - ph).mean():>7.3f}{np.abs(y[ok]).mean():>7.3f}")
    ok_n = int((~np.isnan(wf(sets['MM'], yu))).sum())
    print(f"   วันทดสอบ {ok_n} วัน")


def main():
    prices, tables = F.load(sys.argv[1])
    eppo = pd.read_csv(sys.argv[2], parse_dates=["day"])
    for fuel in ("B7", "G95"):
        df = build(prices, tables, eppo, fuel)
        pend = df.pend.fillna(0)
        nz = pend.abs() >= 0.05
        print(f"\n#### {fuel}: ความสัมพันธ์ดิบ (n={len(df)}) : PEND≠0 {nz.mean():.0%} ของวัน ; corr(PEND, Δปลีกพรุ่งนี้) {np.corrcoef(pend, df.dret)[0, 1]:.2f} ; corr(PEND, ΔWS พรุ่งนี้) {np.corrcoef(pend, df.dws)[0, 1]:.2f}"
              f" ; corr(Δปลีก, ΔWS วันเดียวกัน) {np.corrcoef(df.dret, df.dws)[0, 1]:.2f}")
        if nz.sum():
            print(f"   เมื่อ PEND≠0 ({int(nz.sum())} วัน) : ปลีกขยับทิศเดียวกัน {(np.sign(pend[nz]) * df.dret[nz] >= MOVE).mean():.0%} ; ปลีกนิ่ง {(df.dret[nz].abs() < MOVE).mean():.0%} ; สวนทิศ {(np.sign(pend[nz]) * df.dret[nz] <= -MOVE).mean():.0%}")
            print(f"   เมื่อ PEND=0 ({int((~nz).sum())} วัน) : ปลีกขยับ {(df.dret[~nz].abs() >= MOVE).mean():.0%}")
        report(fuel, df, "dret", "ราคาขายปลีก พรุ่งนี้")
        report(fuel, df, "dws", "ราคาขายส่ง EPPO พรุ่งนี้")


if __name__ == "__main__":
    main()
