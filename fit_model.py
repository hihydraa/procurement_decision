"""
ฝึกและวัดโมเดลจากข้อมูลย้อนหลัง (walk-forward) แล้วเขียน model_params.json + backtest_summary.json

ใช้: python3 fit_model.py <data_dir> [--out model_params.json] [--summary backtest_summary.json]
data_dir ต้องมี prices.csv trend.csv headsup.csv mops.csv fund.csv (สร้างจาก tools/backtest/parse_line_export.py
ใน repo oil-bot) — ข้อมูลดิบมาจากแชท จึงไม่เก็บใน repo; เก็บเฉพาะสัมประสิทธิ์และตัวเลขสรุปที่ไม่ระบุตัวบุคคล

หลักการวัด: ทุกฟีเจอร์รู้ ณ เวลาตัดสินใจ (PM 15:30 / EVE 20:30) เป้าหมายคือราคาต่ำสุดของตลาดที่ 15:30 วันถัดไป
ลบ 15:30 วันนี้ (เวลาเดียวกันของวัน เพราะราคาเป็นฟันเลื่อย: ขึ้นข้ามคืนแล้วค่อย ๆ ลดทั้งวัน)
"""
import argparse
import json
import os
from datetime import datetime

import numpy as np
import pandas as pd

import model as M
import signals as S

PHASES = {"PM": 15.5, "EVE": 20.5}
MIN_TRAIN = 150
STEP = 30


def load(data_dir):
    rd = lambda n: pd.read_csv(os.path.join(data_dir, n))
    return (rd("prices.csv"), S.Tables(intraday=rd("trend.csv"), headsup=rd("headsup.csv"),
                                       mops_close=rd("mops.csv"), fund=rd("fund.csv")))


def auc(y, p):
    y, p = np.asarray(y), np.asarray(p)
    pos, neg = p[y == 1], p[y == 0]
    if len(pos) == 0 or len(neg) == 0:
        return float("nan")
    ranks = pd.Series(np.concatenate([pos, neg])).rank().to_numpy()
    return float((ranks[:len(pos)].sum() - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg)))


def build_dataset(book, tables, fuel, hour):
    d0 = tables.headsup.ts.min().normalize() + pd.Timedelta(days=10)
    d1 = tables.headsup.ts.max().normalize() - pd.Timedelta(days=1)
    days = pd.date_range(d0, d1)
    p_today = book.min_at(fuel, [d + pd.Timedelta(hours=15.5) for d in days])
    p_next = book.min_at(fuel, [d + pd.Timedelta(days=1, hours=15.5) for d in days])
    rows = []
    for d, a, b in zip(days, p_today.to_numpy(), p_next.to_numpy()):
        if np.isnan(a) or np.isnan(b):
            continue
        f = S.build_features(d + pd.Timedelta(hours=hour), tables, fuel)
        f.update({"date": d, "p_today": a, "p_next": b, "y": b - a})
        rows.append(f)
    return pd.DataFrame(rows)


def walk_forward(df):
    X = np.vstack([M.vectorize(r) for r in df.to_dict("records")])
    y_rise, y_fall = (df.y > M.RISE_THB).to_numpy(float), (df.y < -M.RISE_THB).to_numpy(float)
    pr, pf = np.full(len(df), np.nan), np.full(len(df), np.nan)
    i = MIN_TRAIN
    while i < len(df):
        j = min(i + STEP, len(df))
        pr[i:j] = M.predict_proba(M.fit_logistic(X[:i], y_rise[:i]), X[i:j])
        pf[i:j] = M.predict_proba(M.fit_logistic(X[:i], y_fall[:i]), X[i:j])
        i = j
    return X, y_rise, y_fall, pr, pf


def summarize_policy(df, pr, pf, th, label):
    acts = np.array([M.choose_action(a, b, th) for a, b in zip(pr, pf)])
    gain = df.y.to_numpy()
    u = np.array([M.utility(a, g) for a, g in zip(acts, gain)])
    out = {"label": label, "n_days": int(len(df)), "total_gain_thb_per_litre": float(u.sum()),
           "avg_gain_per_day": float(u.mean()), "actions": {}}
    for a in ("URGENT", "GRADUAL", "NORMAL", "WAIT"):
        m = acts == a
        if m.sum():
            out["actions"][a] = {"n": int(m.sum()), "share": float(m.mean()),
                                 "rise_rate": float((gain[m] > M.RISE_THB).mean()), "fall_rate": float((gain[m] < -M.RISE_THB).mean()),
                                 "avg_next_minus_today": float(gain[m].mean())}
    return out


def old_rule_policy(df):
    """ตัวเทียบ: state MOPS แบบ rule เดิม (ดูแค่ MOPS ปิดล่าสุด) UP -> ทยอยซื้อ , DOWN -> ชะลอ"""
    acts = []
    for r in df.to_dict("records"):
        c1, c3 = r.get("close_last"), r.get("close_3")
        if c1 is not None and not np.isnan(c1) and c1 > 0.01:
            st = "UP"
        elif c1 is not None and not np.isnan(c1) and c1 < -0.01:
            st = "DOWN"
        else:
            st = "UP" if (c3 or 0) > 0 else "DOWN" if (c3 or 0) < 0 else "FLAT"
        acts.append({"UP": "GRADUAL", "DOWN": "WAIT"}.get(st, "NORMAL"))
    gain = df.y.to_numpy()
    u = np.array([M.utility(a, g) for a, g in zip(acts, gain)])
    return {"label": "rule เดิม (MOPS state)", "n_days": int(len(df)), "total_gain_thb_per_litre": float(u.sum()),
            "avg_gain_per_day": float(u.mean())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data_dir")
    ap.add_argument("--out", default=M.PARAMS_PATH)
    ap.add_argument("--summary", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest_summary.json"))
    a = ap.parse_args()

    prices, tables = load(a.data_dir)
    book = S.PriceBook(prices)
    params = {"features": M.FEATURES, "trained_at": datetime.now().strftime("%Y-%m-%d"), "models": {}, "thresholds": {}}
    summary = {"generated": datetime.now().strftime("%Y-%m-%d"), "target": "ราคาต่ำสุดของตลาด 15:30 พรุ่งนี้ เทียบ 15:30 วันนี้ (บาท/ลิตร)",
               "rise_threshold_thb": M.RISE_THB, "results": {}}
    for fuel in S.FUELS:
        for phase, hour in PHASES.items():
            key = f"{fuel}_{phase}"
            df = build_dataset(book, tables, fuel, hour)
            X, y_rise, y_fall, pr, pf = walk_forward(df)
            ok = ~np.isnan(pr)
            d, pr_o, pf_o = df[ok].reset_index(drop=True), pr[ok], pf[ok]
            yr, yf = y_rise[ok], y_fall[ok]
            half = len(d) // 2
            th, _ = M.tune_thresholds(pr_o[:half], pf_o[:half], d.y.to_numpy()[:half])     # จูนเกณฑ์ด้วยครึ่งแรกของช่วงทดสอบ
            res = {
                "n_train_days": int(len(df)), "n_oos_days": int(ok.sum()),
                "base_rise": float(yr.mean()), "base_fall": float(yf.mean()),
                "auc_rise": auc(yr, pr_o), "auc_fall": auc(yf, pf_o),
                "brier_rise": float(np.mean((pr_o - yr) ** 2)), "brier_rise_baseline": float(np.mean((yr.mean() - yr) ** 2)),
                "thresholds": th,
                "holdout_policy": summarize_policy(d.iloc[half:], pr_o[half:], pf_o[half:], th, "นโยบายใหม่ (ช่วงที่ไม่เคยใช้จูน)"),
                "holdout_old_rule": old_rule_policy(d.iloc[half:]),
                "holdout_always_buy_today": {"total_gain_thb_per_litre": 0.0},
            }
            summary["results"][key] = res
            # โมเดลสุดท้ายฝึกด้วยข้อมูลทั้งหมด ; เกณฑ์จูนด้วยทุกวันที่ทดสอบ OOS
            params["models"][key] = {"rise": M.fit_logistic(X, y_rise), "fall": M.fit_logistic(X, y_fall)}
            params["thresholds"][key], _ = M.tune_thresholds(pr_o, pf_o, d.y.to_numpy())
            print(f"[{key}] oos={ok.sum()} AUC rise={res['auc_rise']:.2f} fall={res['auc_fall']:.2f} | "
                  f"holdout gain new={res['holdout_policy']['total_gain_thb_per_litre']:+.1f} old={res['holdout_old_rule']['total_gain_thb_per_litre']:+.1f} "
                  f"(บาท/ลิตรรวมทุกวัน, n={res['holdout_policy']['n_days']})")
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(params, fh, ensure_ascii=False, indent=1)
    with open(a.summary, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, ensure_ascii=False, indent=1, default=float)
    print("wrote", a.out, a.summary)


if __name__ == "__main__":
    main()
