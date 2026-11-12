"""
การทดลอง: พยากรณ์ราคาขายปลีกของวันพรุ่งนี้ — โมเดลจากสัญญาณ Jobber (ใหม่) เทียบวิธีที่ใช้อยู่ (ความน่าจะเป็น WS ของการ์ด × ความถี่ที่ปลีกตาม)

เทมเพลต (ตั้งก่อนรัน 2026-11-12)
  สมมติฐาน : สัญญาณของคืนวัน t (จำนวนผู้ค้าที่ประกาศ "มีปรับขึ้น/ลง", MOPS ปิดล่าสุด, ประกาศกองทุนที่มีผลพรุ่งนี้) ทายปลีกของวัน t+1 ได้ดีกว่าวิธีเดิม
  ชุดฟีเจอร์ : SIG = up_n, dn_n, close_last, fund_next — ตรึงตามผลของ exp_eppo_search.py ไม่เลือกใหม่ ; โมเดลโลจิสติกส์ 2 ตัว (ขึ้น >= 10 สต. / ลง >= 10 สต.)
  ฐานเปรียบเทียบ : (ก) ความถี่ในอดีต (ไม่ใช้สัญญาณ)  (ข) วิธีเดิม จำลองตามโค้ดจริง (forecast_card.probabilities + retail_forecast.forecast) ด้วยข้อมูลถึงวัน t เท่านั้น
     หมายเหตุ : วิธีเดิมของดีเซลใช้โมเดล C ซึ่งได้ Δกองทุน/ภาษีจริงของวันถัดไป = ได้เปรียบเล็กน้อยในการจำลองนี้ (เอนเอียงเข้าข้างวิธีเดิม)
  เวลาที่รู้จริง : ฟีเจอร์ใช้เฉพาะข้อความที่โพสต์ <= 20:30 วัน t ; ทดสอบแบบขยายหน้าต่าง ฝึกขั้นต่ำ 50 วัน ก้าว 10 วัน
  ตัวชี้วัด : AUC ขึ้น/ลง , Brier 3 ชั้น (ต่ำ = ดี) , อัตราฟันธง/ถูกทิศ/ผิดทิศ ด้วยเกณฑ์ฟันธงเดียวกับที่ใช้จริง (retail_forecast.decide) , ครึ่งแรก/ครึ่งหลัง
  เกณฑ์ผ่าน (ต่อชนิดน้ำมัน ต้องครบทุกข้อ จึงใช้โมเดลใหม่แทนวิธีเดิมบนหน้าเว็บ) :
     1) AUC เฉลี่ย (ขึ้น, ลง) >= 0.70 และไม่ต่ำกว่า 0.60 ในครึ่งใดครึ่งหนึ่ง
     2) Brier ต่ำกว่าทั้งวิธีเดิมและความถี่ในอดีต บนวันทดสอบชุดเดียวกัน
     3) เมื่อฟันธง อัตราผิดทิศไม่สูงกว่าวิธีเดิม และฟันธงอย่างน้อย 5 ครั้ง
  ข้อควรระวัง : ชุดฟีเจอร์ SIG ถูกเลือกจาก 6 ชุดบนช่วงวันเดียวกันนี้มาก่อน (เอนเอียงจากการเลือก) และวันทดสอบมีราว 70 วัน
     จึงเป็นหลักฐานขั้นต้นเท่านั้น ผ่านเกณฑ์แล้วก็ยังติดป้าย "ทดลอง" จนกว่าจะมีผลจริงเดินหน้า >= 40 วัน

ผล (รัน 2026-11-12 ; วันทดสอบร่วม ดีเซล 50 วัน G95 51 วัน ช่วง 08/06–20/10/2026) : โมเดลใหม่ไม่ผ่านเกณฑ์ทั้งสองชนิด → คงวิธีเดิม
  ดีเซล : AUC ขึ้น/ลง ใหม่ 0.79/0.74 (ครึ่งแรก 0.54 ไม่ผ่านเกณฑ์ 1) เดิม 0.61/0.83 ; Brier ใหม่ 0.445 เดิม 0.665 ความถี่ 0.472 ; ใหม่ไม่ฟันธงเลย (ไม่ผ่านเกณฑ์ 3)
  G95   : AUC ใหม่ 0.67/0.59 (ไม่ผ่านเกณฑ์ 1) เดิม 0.66/0.69 ; Brier ใหม่ 0.535 เดิม 0.645 ความถี่ 0.539 ; ใหม่ฟันธง 1 ครั้ง
  ข้อค้นพบข้างเคียง : วิธีเดิมมั่นใจเกินจริง — Brier แย่กว่าความถี่ในอดีต และเมื่อฟันธงทิศ ถูกเพียง 33% (ดีเซล 21 ครั้ง, G95 27 ครั้ง) ส่วนใหญ่เพราะปลีกไม่ปรับ

ใช้: python exp_retail_search.py <data_dir ของ jobber> <eppo_daily.csv> <mops.json> <wti.json>
"""
import json
import sys
import warnings

import numpy as np
import pandas as pd

import fit_model as F
import forecast_card as FC
import model as M
import retail_forecast as RF
import signals as S
from diag_rule_backtest import mops_daily, wti_daily
from diag_wf_components import build_dataset, walk_forward

warnings.filterwarnings("ignore")
MIN_TRAIN, STEP = 50, 10
OLD_COL = {"B7": "C", "G95": "DIRECT"}
CLS = RF.CLASSES


def dataset(tables, eppo_d, fuel, last_signal_day):
    g = eppo_d[eppo_d.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
    rows = []
    for day, r in g.iterrows():
        nxt = day + pd.Timedelta(days=1)
        if nxt not in g.index or day > last_signal_day:
            continue
        rows.append({"day": day, "dret": float(g.retail[nxt] - r.retail), **RF.signal_features(tables, fuel, day)})
    return pd.DataFrame(rows)


def new_probs(df):
    """walk-forward ขยายหน้าต่าง : คืน p_up, p_down (NaN ช่วงฝึกขั้นต่ำ) และความถี่ในอดีต"""
    X = np.vstack([RF.signal_vector(r) for r in df.to_dict("records")])
    y = df.dret.to_numpy()
    yu, yd = (y >= RF.MOVE).astype(float), (y <= -RF.MOVE).astype(float)
    pu, pdn, cu, cd = (np.full(len(y), np.nan) for _ in range(4))
    i = MIN_TRAIN
    while i < len(y):
        j = min(i + STEP, len(y))
        for yy, out in ((yu, pu), (yd, pdn)):
            out[i:j] = M.predict_proba(M.fit_logistic(X[:i], yy[:i]), X[i:j]) if 0 < yy[:i].sum() < i else yy[:i].mean()
        cu[i:j], cd[i:j] = (yu[:i].sum() + 1) / (i + 3), (yd[:i].sum() + 1) / (i + 3)
        i = j
    return pu, pdn, cu, cd


def old_probs(ds, wfr, eppo_d, fuel, days):
    w = wfr[wfr.fuel == fuel].sort_values("day")
    col = OLD_COL[fuel]
    out = {}
    for t in days:
        row = w[w.day == t]
        resid = (w[w.day < t].actual - w[w.day < t][col]).to_numpy(float)
        if row.empty or len(resid) < FC.MIN_RESID:
            continue
        p = FC.probabilities(float(row[col].iloc[0]), resid)
        f = RF.forecast(eppo_d[eppo_d.day <= t], fuel, {"ลง": p["p_down"], "นิ่ง": p["p_flat"], "ขึ้น": p["p_up"]}, t + pd.Timedelta(days=1), None)
        if f:
            out[t] = (f["p_up"], f["p_down"])
    return out


def score(name, y, pu, pdn):
    yu, yd = (y >= RF.MOVE).astype(float), (y <= -RF.MOVE).astype(float)
    pf = np.clip(1 - pu - pdn, 0, 1)
    brier_i = (pu - yu) ** 2 + (pdn - yd) ** 2 + (pf - (1 - yu - yd)) ** 2
    calls = [RF.decide({"ลง": d, "นิ่ง": f, "ขึ้น": u}) for u, d, f in zip(pu, pdn, pf)]
    act = [RF.classify(v) for v in y]
    dirc = [(c, a) for c, a in zip(calls, act) if c in ("ขึ้น", "ลง")]
    right = sum(c == a for c, a in dirc)
    wrong = sum(a in ("ขึ้น", "ลง") and c != a for c, a in dirc)
    h = len(y) // 2
    half = lambda sl: np.nanmean([F.auc(yu[sl], pu[sl]), F.auc(yd[sl], pdn[sl])])
    res = {"auc_up": F.auc(yu, pu), "auc_dn": F.auc(yd, pdn), "h1": half(slice(0, h)), "h2": half(slice(h, None)), "brier": float(brier_i.mean()),
           "n_call": len(dirc), "right": right, "wrong": wrong, "brier_i": brier_i}
    print(f"{name:<22}{res['auc_up']:>7.2f}{res['auc_dn']:>7.2f}{res['h1']:>8.2f}{res['h2']:>8.2f}{res['brier']:>8.3f}{len(dirc):>7}"
          f"{(right / len(dirc) if dirc else float('nan')):>8.0%}{(wrong / len(dirc) if dirc else float('nan')):>8.0%}")
    return res


def boot(a, b, reps=4000, seed=7):
    rng = np.random.default_rng(seed)
    d = a - b
    m = np.array([d[rng.integers(0, len(d), len(d))].mean() for _ in range(reps)])
    return float(d.mean()), float(np.percentile(m, 2.5)), float(np.percentile(m, 97.5))


def main():
    _, tables = F.load(sys.argv[1])
    eppo_d = pd.read_csv(sys.argv[2], parse_dates=["day"])           # day,fuel,ws,mm,retail,exref,fund (สกัดจาก Entry_Eppo ไม่นับแถว CARRIED)
    eppo_c = eppo_d.assign(other=eppo_d.ws - eppo_d.exref - eppo_d.fund, resid=0.0)
    mops, wti = mops_daily(json.load(open(sys.argv[3]))), wti_daily(json.load(open(sys.argv[4])))
    ds = build_dataset(mops, wti, eppo_c)
    wfr = walk_forward(ds)
    last = tables.headsup.ts.max().normalize() - pd.Timedelta(days=1)
    verdict = {}
    for fuel in ("B7", "G95"):
        df = dataset(tables, eppo_d, fuel, last)
        pu, pdn, cu, cd = new_probs(df)
        old = old_probs(ds, wfr, eppo_d, fuel, list(df.day))
        ok = ~np.isnan(pu) & df.day.isin(list(old)).to_numpy()
        d = df[ok]
        y = d.dret.to_numpy()
        ou, od = np.array([old[t][0] for t in d.day]), np.array([old[t][1] for t in d.day])
        print(f"\n== {fuel} ราคาขายปลีกพรุ่งนี้ : วันทั้งหมด {len(df)} ; วันทดสอบร่วม {len(d)} ({d.day.min():%d/%m/%y}–{d.day.max():%d/%m/%y}) ; ขึ้น {(y >= .1).mean():.0%} ลง {(y <= -.1).mean():.0%}")
        print(f"{'วิธี':<22}{'AUCขึ้น':>7}{'AUCลง':>7}{'ครึ่ง1':>8}{'ครึ่ง2':>8}{'Brier':>8}{'ฟันธง':>7}{'ถูกทิศ':>8}{'ผิดทิศ':>8}")
        c = score("ความถี่ในอดีต", y, cu[ok], cd[ok])
        o = score("วิธีเดิม (WS×ความถี่)", y, ou, od)
        n = score("ใหม่ (สัญญาณ Jobber)", y, pu[ok], pdn[ok])
        m, lo, hi = boot(n["brier_i"], o["brier_i"])
        print(f"   Brier ใหม่ − เดิม = {m:+.3f} (ช่วงเชื่อมั่น 95% {lo:+.3f} ถึง {hi:+.3f})")
        c1 = np.nanmean([n["auc_up"], n["auc_dn"]]) >= 0.70 and min(n["h1"], n["h2"]) >= 0.60
        c2 = n["brier"] < o["brier"] and n["brier"] < c["brier"]
        wr = lambda r: r["wrong"] / r["n_call"] if r["n_call"] else 0.0
        c3 = n["n_call"] >= 5 and wr(n) <= wr(o)
        verdict[fuel] = (c1, c2, c3)
        print(f"   เกณฑ์ 1 (AUC) {'ผ่าน' if c1 else 'ไม่ผ่าน'} ; เกณฑ์ 2 (Brier) {'ผ่าน' if c2 else 'ไม่ผ่าน'} ; เกณฑ์ 3 (ฟันธง) {'ผ่าน' if c3 else 'ไม่ผ่าน'} → {'ใช้โมเดลใหม่' if all((c1, c2, c3)) else 'คงวิธีเดิม'}")
    return verdict


if __name__ == "__main__":
    main()
