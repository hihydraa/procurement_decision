"""
การทดลองหาโมเดลที่แม่นขึ้น (อ่านอย่างเดียว ไม่แตะโมเดลที่ใช้งานจริง) — ราคาหน้าคลังจาก Jobber (ราคาต่ำสุดของตลาด)

เทมเพลตการทดลอง (ตั้งก่อนรัน)
  สมมติฐาน : โมเดลปัจจุบัน (โลจิสติกส์ 8 ฟีเจอร์) ไม่ได้ใช้ข้อมูลที่ "รู้แล้ว ณ เวลาตัดสินใจ" อีกหลายอย่าง
     H1 PEND  ราคาที่ประกาศล่วงหน้าแล้ว (โพสต์ก่อนเวลาตัดสินใจ มีผลหลังจากนั้น เช่น โพสต์ 17:30 มีผล 23:59) บอกราคาพรุ่งนี้ได้โดยตรง
     H2 MOM   โมเมนตัมของราคาเอง (เทียบ 24/72 ชม. ก่อน, เทียบเช้าวันนี้ : ราคาเป็นฟันเลื่อย)
     H3 ECM   ส่วนที่ราคายังไม่ส่งผ่านจาก MOPS (ผลรวม MOPS 5/10 ครั้ง เทียบราคาที่ขยับ 5/10 วัน)
     H4 DISP  ราคาต่ำสุดต่ำกว่าค่ากลางของตลาดมากแค่ไหน (เจ้าที่ถูกสุดมักตามขึ้น)
     H5 DOW   วันในสัปดาห์ (MOPS ไม่ออกเสาร์–อาทิตย์)
     H6 GBM   โมเดลไม่เชิงเส้น (HistGradientBoosting) ดีกว่าโลจิสติกส์
  ชั้นที่แก้ : ทิศ (แนวโน้ม 24/48 ชม.) และขนาด (บาท/ลิตร)
  ตัวชี้วัดหลัก : AUC ของ ขึ้น>=10 สต. และ ลง>=10 สต. (walk-forward เดียวกับของจริง : ฝึกขั้นต่ำ 150 วัน, ก้าว 30 วัน)
                 + MAE ของขนาด (บาท/ลิตร) เทียบ "ทายไม่เปลี่ยน" + กำไรของนโยบายตามทิศ (บาท/ลิตร/วัน) บนครึ่งหลังที่ไม่เคยใช้ตั้งเกณฑ์
  เกณฑ์ผ่าน (ตั้งก่อนรัน) : AUC เฉลี่ย (ขึ้น,ลง) ดีขึ้น >= 0.02 เทียบ BASE บนวันทดสอบชุดเดียวกัน , ดีขึ้นทั้งครึ่งแรกและครึ่งหลัง ,
                          ช่วงเชื่อมั่น 90% ของส่วนต่าง (bootstrap รายวัน) ไม่คร่อม 0
  baseline : BASE = ฟีเจอร์และโลจิสติกส์เดิมของ model.py (ต้องได้ AUC ใกล้ backtest_summary.json)
  เวลาที่ข้อมูลรู้ได้จริง : ทุกฟีเจอร์ใหม่ใช้เฉพาะแถวราคาที่ "โพสต์" (msg_date+msg_time) ก่อน/เท่ากับเวลาตัดสินใจ — ไม่ใช้เวลามีผลอย่างเดียว
                          เพราะราคาบางส่วนถูกโพสต์ย้อนหลัง (รายการเช้า) ; เป้าหมายยังใช้ราคาจริง ณ เวลามีผลเหมือนเดิม

ใช้: python exp_model_search.py <data_dir> [--keys B7_H24,G95_H24,...] [--boot 400]
"""
import argparse
import sys
import warnings

import numpy as np
import pandas as pd

import fit_model as F
import model as M
import signals as S

warnings.filterwarnings("ignore")
MIN_TRAIN, STEP = F.MIN_TRAIN, F.STEP
MAX_AGE = pd.Timedelta(days=S.MARKET_MAX_AGE_DAYS)


# ---------- สมุดราคาแบบ "รู้ ณ เวลาโพสต์" ----------
class KnownBook:
    """ราคาที่รู้ ณ เวลา asof : ใช้เฉพาะแถวที่โพสต์ (msg) <= asof ; ถามราคาต่ำสุดของตลาด ณ เวลา t ใด ๆ (รวมอนาคตที่ประกาศแล้ว)"""

    def __init__(self, prices: pd.DataFrame, fuel: str):
        p = prices[prices.fuel == fuel].copy()
        p["msg"] = pd.to_datetime(p.msg_date.astype(str) + " " + p.msg_time.astype(str), errors="coerce")
        p["eff"] = pd.to_datetime(p.effective, errors="coerce")
        p["price"] = pd.to_numeric(p.price, errors="coerce")
        p = p.dropna(subset=["msg", "eff", "price"])
        p = p[p.price.between(15, 70)]
        p["key"] = p.provider.astype(str) + "|" + p.warehouse.astype(str)
        self.p = p.sort_values("msg").reset_index(drop=True)
        self.msg = self.p.msg.to_numpy()

    def snapshot(self, asof: pd.Timestamp) -> pd.DataFrame:
        n = int(np.searchsorted(self.msg, np.datetime64(asof), side="right"))
        lo = int(np.searchsorted(self.msg, np.datetime64(asof - pd.Timedelta(days=14)), side="left"))
        return self.p.iloc[lo:n]

    @staticmethod
    def latest(snap: pd.DataFrame, t: pd.Timestamp) -> pd.Series:
        """ราคาล่าสุดของแต่ละ (ผู้ขาย, คลัง) ที่มีผล <= t และไม่เก่าเกิน MAX_AGE"""
        w = snap[(snap.eff <= t) & (snap.eff >= t - MAX_AGE)]
        if w.empty:
            return pd.Series(dtype=float)
        return w.sort_values(["eff", "msg"]).groupby("key").price.last()


def extra_features(kb: KnownBook, asof: pd.Timestamp, hz: int) -> dict:
    snap = kb.snapshot(asof)
    now = kb.latest(snap, asof)
    out = {k: np.nan for k in ("pend", "pend_up_n", "pend_dn_n", "mom1", "mom3", "mom5", "mom10", "intra", "disp", "n_series")}
    if now.empty:
        return out
    p0 = float(now.min())
    fut = kb.latest(snap, asof + pd.Timedelta(hours=hz))
    out["pend"] = float(fut.min()) - p0 if len(fut) else 0.0
    both = pd.concat([now.rename("a"), fut.rename("b")], axis=1).dropna()
    d = both.b - both.a
    out["pend_up_n"], out["pend_dn_n"] = int((d >= 0.05).sum()), int((d <= -0.05).sum())
    for name, days in (("mom1", 1), ("mom3", 3), ("mom5", 5), ("mom10", 10)):
        prev = kb.latest(snap, asof - pd.Timedelta(days=days))
        out[name] = p0 - float(prev.min()) if len(prev) else np.nan
    morn = kb.latest(snap, asof.normalize() + pd.Timedelta(hours=6))
    out["intra"] = p0 - float(morn.min()) if len(morn) else np.nan
    out["disp"] = float(now.median()) - p0
    out["n_series"] = int(len(now))
    return out


def mops_sums(tables: S.Tables, asof: pd.Timestamp, fuel: str) -> dict:
    w = tables.mops_close[tables.mops_close.ts <= asof]
    v = pd.to_numeric(w[S.CLOSE_COL[fuel]], errors="coerce").dropna()
    return {"close_5": float(v.iloc[-5:].sum()) if len(v) else np.nan, "close_10": float(v.iloc[-10:].sum()) if len(v) else np.nan}


def build(prices, book, tables, fuel, hz, ref_hour):
    base = F.build_outlook_dataset(book, tables, fuel, hz, ref_hour)
    kb = KnownBook(prices, fuel)
    rows = []
    for r in base.to_dict("records"):
        asof = r["date"] + pd.Timedelta(hours=ref_hour)
        e = extra_features(kb, asof, hz)
        e.update(mops_sums(tables, asof, fuel))
        e["dow"] = asof.dayofweek
        rows.append({**r, **e})
    return pd.DataFrame(rows)


# ---------- เมทริกซ์ฟีเจอร์ ----------
def _c(v, lo, hi):
    return np.clip(np.nan_to_num(np.asarray(v, float), nan=0.0), lo, hi)


def blocks(df: pd.DataFrame) -> dict:
    base = np.vstack([M.vectorize(r) for r in df.to_dict("records")])
    pend = _c(df.pend, -3, 3)
    b = {
        "BASE": base,
        "PEND": np.column_stack([pend, np.maximum(pend, 0), np.minimum(pend, 0), _c(df.pend_up_n, 0, 8), _c(df.pend_dn_n, 0, 8)]),
        "MOM": np.column_stack([_c(df.mom1, -3, 3), _c(df.mom3, -5, 5), _c(df.intra, -2, 2)]),
        "ECM": np.column_stack([_c(df.close_5, -25, 25), _c(df.close_10, -40, 40), _c(df.mom5, -6, 6), _c(df.mom10, -8, 8)]),
        "DISP": np.column_stack([_c(df.disp, 0, 3)]),
        "DOW": np.column_stack([(df.dow == k).to_numpy(float) for k in (0, 4, 5, 6)]),
    }
    return b


SETS = {
    "BASE": ["BASE"],
    "+PEND": ["BASE", "PEND"],
    "+MOM": ["BASE", "MOM"],
    "+ECM": ["BASE", "ECM"],
    "+DISP": ["BASE", "DISP"],
    "+DOW": ["BASE", "DOW"],
    "ALL": ["BASE", "PEND", "MOM", "ECM", "DISP", "DOW"],
    "ALL-PEND": ["BASE", "MOM", "ECM", "DISP", "DOW"],
}


def X_of(b, names):
    return np.column_stack([b[n] for n in names])


# ---------- โมเดล ----------
def wf(X, y, kind="logit"):
    p = np.full(len(y), np.nan)
    i = MIN_TRAIN
    while i < len(y):
        j = min(i + STEP, len(y))
        if kind == "logit":
            p[i:j] = M.predict_proba(M.fit_logistic(X[:i], y[:i]), X[i:j])
        elif kind == "gbm":
            from sklearn.ensemble import HistGradientBoostingClassifier
            m = HistGradientBoostingClassifier(max_depth=3, max_iter=120, learning_rate=0.06, min_samples_leaf=20, l2_regularization=1.0, random_state=0)
            m.fit(X[:i], y[:i])
            p[i:j] = m.predict_proba(X[i:j])[:, 1]
        elif kind == "ridge":
            mu, sd = X[:i].mean(0), X[:i].std(0)
            sd[sd < 1e-9] = 1
            A = np.column_stack([(X[:i] - mu) / sd, np.ones(i)])
            R = np.eye(A.shape[1]) * 5.0
            R[-1, -1] = 0
            w = np.linalg.solve(A.T @ A + R, A.T @ y[:i])
            p[i:j] = np.column_stack([(X[i:j] - mu) / sd, np.ones(j - i)]) @ w
        elif kind == "gbr":
            from sklearn.ensemble import HistGradientBoostingRegressor
            m = HistGradientBoostingRegressor(max_depth=3, max_iter=150, learning_rate=0.05, min_samples_leaf=20, l2_regularization=1.0, loss="absolute_error", random_state=0)
            m.fit(X[:i], y[:i])
            p[i:j] = m.predict(X[i:j])
        i = j
    return p


def boot_diff(y, pa, pb, n=400, seed=0):
    """ช่วงเชื่อมั่น 90% ของ AUC(pb) - AUC(pa) (สุ่มซ้ำรายวัน)"""
    rng = np.random.default_rng(seed)
    idx = np.arange(len(y))
    d = []
    for _ in range(n):
        s = rng.choice(idx, len(idx))
        a, b = F.auc(y[s], pa[s]), F.auc(y[s], pb[s])
        if not (np.isnan(a) or np.isnan(b)):
            d.append(b - a)
    return (float(np.percentile(d, 5)), float(np.percentile(d, 95))) if d else (np.nan, np.nan)


def policy_gain(y, pr, pf, half):
    """นโยบายตามทิศ : ทายขึ้น (pr>=thr และ pr>pf) ได้ +y ; ทายลง ได้ -y ; ไม่ทาย = 0 — ตั้ง thr บนครึ่งแรก รายงานครึ่งหลัง"""
    def gain(th, sl):
        up = (pr[sl] >= th) & (pr[sl] > pf[sl])
        dn = (pf[sl] >= th) & (pf[sl] > pr[sl])
        g = np.where(up, y[sl], 0.0) + np.where(dn, -y[sl], 0.0)
        return float(g.mean()), float((up | dn).mean()), float(((np.where(up, y[sl], -y[sl]) >= 0.10) & (up | dn)).sum() / max((up | dn).sum(), 1))
    best = max((0.4, 0.45, 0.5, 0.55, 0.6, 0.65), key=lambda th: gain(th, slice(0, half))[0])
    g, share, right = gain(best, slice(half, None))
    return best, g, share, right


def evaluate(key, df, nboot):
    y = df.y.to_numpy()
    yr, yf = (y >= M.OUTLOOK_SMALL).astype(float), (y <= -M.OUTLOOK_SMALL).astype(float)
    b = blocks(df)
    res = {}
    for name, parts in SETS.items():
        X = X_of(b, parts)
        for kind in (("logit", "gbm") if name in ("BASE", "ALL", "+PEND") else ("logit",)):
            res[(name, kind)] = (wf(X, yr, kind), wf(X, yf, kind))
    ok = ~np.isnan(res[("BASE", "logit")][0])
    yo, yro, yfo = y[ok], yr[ok], yf[ok]
    half = len(yo) // 2
    print(f"\n== {key} : n={len(df)} ทดสอบ {ok.sum()} วัน ({df.date[ok].min():%d/%m/%y}–{df.date[ok].max():%d/%m/%y}) ; ฐาน ขึ้น {yro.mean():.0%} ลง {yfo.mean():.0%}")
    bpr, bpf = res[("BASE", "logit")][0][ok], res[("BASE", "logit")][1][ok]
    base_auc = (F.auc(yro, bpr) + F.auc(yfo, bpf)) / 2
    print(f"{'ชุด':<10}{'โมเดล':<7}{'AUCขึ้น':>8}{'AUCลง':>8}{'เฉลี่ย':>8}{'Δ':>7}{'Δครึ่ง1':>8}{'Δครึ่ง2':>8}{'CI90 Δขึ้น':>16}{'CI90 Δลง':>16}{'กำไร/วัน':>9}{'ทาย%':>6}{'ถูก%':>6}")
    out = {}
    for (name, kind), (pr, pf) in res.items():
        pr, pf = pr[ok], pf[ok]
        ar, af = F.auc(yro, pr), F.auc(yfo, pf)
        h = lambda sl: (F.auc(yro[sl], pr[sl]) + F.auc(yfo[sl], pf[sl])) / 2 - (F.auc(yro[sl], bpr[sl]) + F.auc(yfo[sl], bpf[sl])) / 2
        ci_r = boot_diff(yro, bpr, pr, nboot) if (name, kind) != ("BASE", "logit") else (0, 0)
        ci_f = boot_diff(yfo, bpf, pf, nboot) if (name, kind) != ("BASE", "logit") else (0, 0)
        th, g, share, right = policy_gain(yo, pr, pf, half)
        print(f"{name:<10}{kind:<7}{ar:>8.3f}{af:>8.3f}{(ar + af) / 2:>8.3f}{(ar + af) / 2 - base_auc:>+7.3f}{h(slice(0, half)):>+8.3f}{h(slice(half, None)):>+8.3f}"
              f"{f'[{ci_r[0]:+.3f},{ci_r[1]:+.3f}]':>16}{f'[{ci_f[0]:+.3f},{ci_f[1]:+.3f}]':>16}{g:>9.3f}{share:>6.0%}{right:>6.0%}")
        out[(name, kind)] = {"auc": (ar + af) / 2, "gain": g}
    # ขนาด (บาท/ลิตร)
    print("   ขนาด : MAE (บาท/ลิตร) เทียบทายไม่เปลี่ยน ; ทิศถูกเมื่อ |ทาย| >= 0.10")
    for name in ("BASE", "+PEND", "ALL"):
        X = X_of(b, SETS[name])
        for kind in ("ridge", "gbr"):
            ph = wf(X, y, kind)[ok]
            call = np.abs(ph) >= 0.10
            right = (np.sign(ph[call]) * yo[call] >= 0.10).mean() if call.sum() else np.nan
            print(f"   {name:<8}{kind:<6} MAE {np.abs(yo - ph).mean():.3f} (ไม่เปลี่ยน {np.abs(yo).mean():.3f}) ; ครึ่งหลัง {np.abs(yo[half:] - ph[half:]).mean():.3f} vs {np.abs(yo[half:]).mean():.3f}"
                  f" ; ฟันธง {call.mean():.0%} ถูกทิศ {right:.0%}")
    # ตรวจ PEND ตรง ๆ
    pend = df.pend.to_numpy()[ok]
    nz = np.abs(pend) >= 0.05
    if nz.sum():
        print(f"   PEND ตรง ๆ : มีประกาศล่วงหน้า {nz.mean():.0%} ของวัน ; สหสัมพันธ์กับผลจริง {np.corrcoef(pend[nz], yo[nz])[0, 1]:.2f} ; ทิศตรง {(np.sign(pend[nz]) == np.sign(yo[nz])).mean():.0%}"
              f" ; MAE ถ้าทาย y=pend {np.abs(yo - np.where(nz, pend, 0)).mean():.3f}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data_dir")
    ap.add_argument("--keys", default="B7_H24,G95_H24,B7_P_H24,G95_P_H24,B7_H48,G95_H48")
    ap.add_argument("--boot", type=int, default=400)
    a = ap.parse_args()
    prices, tables = F.load(a.data_dir)
    book = S.PriceBook(prices)
    for key in a.keys.split(","):
        parts = key.split("_")
        fuel, hz = parts[0], int(parts[-1][1:])
        ref_hour = M.OUTLOOK_REF_PM if "P" in parts[1:-1] else M.OUTLOOK_REF_HOUR
        df = build(prices, book, tables, fuel, hz, ref_hour)
        evaluate(key, df, a.boot)
        sys.stdout.flush()


if __name__ == "__main__":
    main()
