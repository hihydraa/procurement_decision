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
# ช่วงเวลาตัดสินใจ "ส่งรถออก" (ชั่วโมงของวัน) และระยะเดินทางสาขา→คลังที่รองรับ (ชั่วโมง) — ราคาที่ได้จริงคือราคา ณ เวลารถถึง
DISPATCH_WINDOWS = {"D06": 6.0, "D12": 12.0, "D15": 15.5, "D20": 20.5}
DISPATCH_LEADS = (8, 10, 12)
DEFAULT_LEAD = 10
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


def build_dispatch_dataset(book, tables, fuel, hour, lead):
    """ออกรถ ณ เวลา hour ของวัน d ถึงคลังเวลา A = hour + lead ; เทียบราคาที่เวลาถึง A กับถ้าออกช้าไปอีก 24 ชม. (ถึง A+24)
    y = ราคา(A+24ชม.) - ราคา(A) : บวก = รอแล้วแพงขึ้น (ควรส่งตอนนี้) , ลบ = รอแล้วถูกลง"""
    d0 = tables.headsup.ts.min().normalize() + pd.Timedelta(days=10)
    d1 = tables.headsup.ts.max().normalize() - pd.Timedelta(days=1)
    days = pd.date_range(d0, d1)
    arr = [d + pd.Timedelta(hours=hour + lead) for d in days]
    p_now = book.min_at(fuel, arr)
    p_next = book.min_at(fuel, [t + pd.Timedelta(days=1) for t in arr])
    rows = []
    for d, a, b in zip(days, p_now.to_numpy(), p_next.to_numpy()):
        if np.isnan(a) or np.isnan(b):
            continue
        f = S.build_features(d + pd.Timedelta(hours=hour), tables, fuel)
        f.update({"date": d, "p_today": a, "p_next": b, "y": b - a})
        rows.append(f)
    return pd.DataFrame(rows)


def hour_profile(book, fuel, days):
    """ราคาต่ำสุดของตลาดเฉลี่ยแต่ละชั่วโมงของวัน เทียบ 15:30 วันเดียวกัน (บาท/ลิตร) — บอกว่าเวลาถึงคลังมีผลต่อราคาแค่ไหน"""
    base = book.min_at(fuel, [d + pd.Timedelta(hours=15.5) for d in days]).to_numpy()
    out = {}
    for h in range(24):
        v = book.min_at(fuel, [d + pd.Timedelta(hours=h + 0.5) for d in days]).to_numpy()
        m = np.nanmean(v - base) if np.isfinite(v - base).any() else None
        out[str(h)] = None if m is None else round(float(m), 3)
    return out


def fit_key(df):
    """walk-forward + จูนเกณฑ์ + โมเดลสุดท้ายของชุดข้อมูลเดียว — คืน (summary_dict, models, thresholds)"""
    X, y_rise, y_fall, pr, pf = walk_forward(df)
    ok = ~np.isnan(pr)
    d, pr_o, pf_o = df[ok].reset_index(drop=True), pr[ok], pf[ok]
    yr, yf = y_rise[ok], y_fall[ok]
    half = len(d) // 2
    th, _ = M.tune_thresholds(pr_o[:half], pf_o[:half], d.y.to_numpy()[:half])
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
    models = {"rise": M.fit_logistic(X, y_rise), "fall": M.fit_logistic(X, y_fall)}
    final_th, _ = M.tune_thresholds(pr_o, pf_o, d.y.to_numpy())
    return res, models, final_th


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


def fit_legacy(book, tables, params, summary):
    """โมเดลเดิม PM 15:30 / EVE 20:30 (เป้าหมาย: ราคา 15:30 พรุ่งนี้ − 15:30 วันนี้)"""
    for fuel in S.FUELS:
        for phase, hour in PHASES.items():
            key = f"{fuel}_{phase}"
            df = build_dataset(book, tables, fuel, hour)
            res, models, final_th = fit_key(df)
            summary["results"][key] = res
            params["models"][key] = models
            params["thresholds"][key] = final_th
            print(f"[{key}] oos={res['n_oos_days']} AUC rise={res['auc_rise']:.2f} fall={res['auc_fall']:.2f} | "
                  f"holdout gain new={res['holdout_policy']['total_gain_thb_per_litre']:+.1f} old={res['holdout_old_rule']['total_gain_thb_per_litre']:+.1f} "
                  f"(บาท/ลิตรรวมทุกวัน, n={res['holdout_policy']['n_days']})")


def fit_dispatch_into(book, tables, params, summary):
    """โมเดลแผนส่งรถ: ชนิด x ช่วงเวลาออกรถ x ระยะเดินทาง"""
    days = pd.date_range(tables.headsup.ts.min().normalize() + pd.Timedelta(days=10), tables.headsup.ts.max().normalize() - pd.Timedelta(days=1))
    params["dispatch"] = {"default_lead_hours": DEFAULT_LEAD, "leads": list(DISPATCH_LEADS), "windows": DISPATCH_WINDOWS,
                          "hour_profile": {f: hour_profile(book, f, days) for f in S.FUELS}, "trained_at": datetime.now().strftime("%Y-%m-%d")}
    summary.setdefault("dispatch_results", {})
    for fuel in S.FUELS:
        for win, hour in DISPATCH_WINDOWS.items():
            for lead in DISPATCH_LEADS:
                key = f"{fuel}_{win}_L{lead}"
                df = build_dispatch_dataset(book, tables, fuel, hour, lead)
                res, models, th = fit_key(df)
                summary["dispatch_results"][key] = res
                params["models"][key] = models
                params["thresholds"][key] = th
                print(f"[{key}] n={len(df)} oos={res['n_oos_days']} AUC rise={res['auc_rise']:.2f} fall={res['auc_fall']:.2f} "
                      f"holdout gain={res['holdout_policy']['total_gain_thb_per_litre']:+.1f} (n={res['holdout_policy']['n_days']})")


def build_outlook_dataset(book, tables, fuel, hz, ref_hour=M.OUTLOOK_REF_HOUR):
    """ตัดสิน ณ ref_hour ของวัน d ; y = ราคาต่ำสุดตลาด(ref + hz ชม.) - ราคา(ref) — ไม่ผูกกับเวลาถึงคลังของสาขาใด"""
    days = pd.date_range(tables.headsup.ts.min().normalize() + pd.Timedelta(days=10),
                         tables.headsup.ts.max().normalize() - pd.Timedelta(days=max(M.OUTLOOK_HORIZONS) // 24 + 1))
    ref = [d + pd.Timedelta(hours=ref_hour) for d in days]
    p0 = book.min_at(fuel, ref).to_numpy()
    p1 = book.min_at(fuel, [t + pd.Timedelta(hours=hz) for t in ref]).to_numpy()
    rows = []
    for d, a, b in zip(days, p0, p1):
        if np.isnan(a) or np.isnan(b):
            continue
        f = S.build_features(d + pd.Timedelta(hours=ref_hour), tables, fuel)
        f.update({"date": d, "y": b - a})
        rows.append(f)
    return pd.DataFrame(rows)


def _wf_proba(X, y):
    p = np.full(len(y), np.nan)
    i = MIN_TRAIN
    while i < len(y):
        j = min(i + STEP, len(y))
        p[i:j] = M.predict_proba(M.fit_logistic(X[:i], y[:i]), X[i:j])
        i = j
    return p


def outlook_tier_stats(y, tiers):
    """สถิติต่อระดับคำแนะนำ (ใช้ผล walk-forward เท่านั้น) : ถูกทิศ/ผิดทิศ (ขยับ >= 10 สต.) , ขยับ >= 40 สต. , ขยับเฉลี่ยในทิศที่ทาย , ความนิ่งครึ่งแรก/หลัง"""
    out, half = {}, len(y) // 2
    for code in M.OUTLOOK_TIERS:
        m = tiers == code
        st = {"n": int(m.sum()), "share": float(m.mean()) if len(m) else 0.0}
        if code != "UNCLEAR" and m.sum():
            sgn = 1.0 if code.startswith("UP") else -1.0
            mv = y[m] * sgn
            st.update({"right10": float((mv >= M.OUTLOOK_SMALL).mean()), "right40": float((mv >= M.OUTLOOK_BIG).mean()),
                       "wrong10": float((mv <= -M.OUTLOOK_SMALL).mean()), "avg_move": float(mv.mean())})
            for tag, sl in (("h1", slice(0, half)), ("h2", slice(half, None))):
                mm = (tiers[sl] == code)
                st["right_" + tag] = float(((y[sl][mm] * sgn) >= M.OUTLOOK_SMALL).mean()) if mm.sum() >= 8 else None
            st["grade"] = M.outlook_grade(st)
        out[code] = st
    return out


def fit_outlook_into(book, tables, params, summary):
    """โมเดลแนวโน้ม 24/48 ชม. ณ 20:30 (key {ชนิด}_H{ชม.}) และ ณ 15:30 (key {ชนิด}_P_H{ชม.}) : x {ขึ้น,ลง} x {>=10 สต., >=40 สต.}"""
    params["outlook"] = {"ref_hour": M.OUTLOOK_REF_HOUR, "ref_hour_pm": M.OUTLOOK_REF_PM, "horizons": list(M.OUTLOOK_HORIZONS),
                         "small": M.OUTLOOK_SMALL, "big": M.OUTLOOK_BIG, "trained_at": datetime.now().strftime("%Y-%m-%d"), "models": {}}
    summary["outlook_results"] = {}
    for fuel, hz, tag, ref_hour in [(f_, h_, t_, r_) for f_ in S.FUELS for t_, r_ in (("", M.OUTLOOK_REF_HOUR), ("P_", M.OUTLOOK_REF_PM)) for h_ in M.OUTLOOK_HORIZONS]:
        if True:
            key = f"{fuel}_{tag}H{hz}"
            df = build_outlook_dataset(book, tables, fuel, hz, ref_hour)
            X = np.vstack([M.vectorize(r) for r in df.to_dict("records")])
            y = df.y.to_numpy()
            tg = {"r10": (y >= M.OUTLOOK_SMALL), "f10": (y <= -M.OUTLOOK_SMALL), "r40": (y >= M.OUTLOOK_BIG), "f40": (y <= -M.OUTLOOK_BIG)}
            tg = {k: v.astype(float) for k, v in tg.items()}
            P = {k: _wf_proba(X, v) for k, v in tg.items()}
            ok = ~np.isnan(P["r10"])
            tiers = np.array([M.classify_outlook(P["r10"][i], P["f10"][i], P["r40"][i], P["f40"][i]) for i in np.where(ok)[0]], dtype=object)
            yo = y[ok]
            summary["outlook_results"][key] = {
                "n_train_days": int(len(df)), "n_oos_days": int(ok.sum()),
                "base_up10": float(tg["r10"][ok].mean()), "base_down10": float(tg["f10"][ok].mean()),
                "auc_r10": auc(tg["r10"][ok], P["r10"][ok]), "auc_f10": auc(tg["f10"][ok], P["f10"][ok]),
                "tiers": outlook_tier_stats(yo, tiers)}
            params["outlook"]["models"][key] = {k: M.fit_logistic(X, v) for k, v in tg.items()}
            t = summary["outlook_results"][key]["tiers"]
            print(f"[outlook {key}] n={len(df)} oos={ok.sum()} AUC r10={summary['outlook_results'][key]['auc_r10']:.2f} f10={summary['outlook_results'][key]['auc_f10']:.2f} | "
                  + " ".join(f"{M.OUTLOOK_TIERS[c]}:{t[c]['n']}" + (f"({t[c]['right10']:.0%}/{t[c]['wrong10']:.0%},{t[c]['grade']})" if t[c].get('grade') else "") for c in M.OUTLOOK_TIERS))


def fit_everything(book, tables):
    """ฝึกทุกโมเดล (เดิม + แผนส่งรถ) จากข้อมูลชุดเดียว คืน (params, summary)"""
    today = datetime.now().strftime("%Y-%m-%d")
    params = {"features": M.FEATURES, "trained_at": today, "data_through": tables.headsup.ts.max().strftime("%Y-%m-%d"), "models": {}, "thresholds": {}}
    summary = {"generated": today, "target": "ราคาต่ำสุดของตลาด 15:30 พรุ่งนี้ เทียบ 15:30 วันนี้ (บาท/ลิตร)", "rise_threshold_thb": M.RISE_THB, "results": {}}
    fit_legacy(book, tables, params, summary)
    fit_dispatch_into(book, tables, params, summary)
    fit_outlook_into(book, tables, params, summary)
    return params, summary


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=1, default=float)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data_dir")
    ap.add_argument("--out", default=M.PARAMS_PATH)
    ap.add_argument("--summary", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest_summary.json"))
    ap.add_argument("--outlook-only", action="store_true", help="ฝึกเฉพาะโมเดลแนวโน้ม 24/48 ชม. แล้วรวมเข้าไฟล์เดิม ไม่แตะโมเดลอื่น")
    ap.add_argument("--dispatch-only", action="store_true",
                    help="ฝึกเฉพาะโมเดลแผนส่งรถ (ช่วงเวลาออกรถ x ระยะเดินทาง) แล้วรวมเข้าไฟล์เดิม ไม่แตะโมเดลเดิม")
    a = ap.parse_args()

    prices, tables = load(a.data_dir)
    book = S.PriceBook(prices)
    if a.outlook_only:
        with open(a.out, encoding="utf-8") as fh:
            params = json.load(fh)
        with open(a.summary, encoding="utf-8") as fh:
            summary = json.load(fh)
        fit_outlook_into(book, tables, params, summary)
    elif a.dispatch_only:
        with open(a.out, encoding="utf-8") as fh:
            params = json.load(fh)
        with open(a.summary, encoding="utf-8") as fh:
            summary = json.load(fh)
        fit_dispatch_into(book, tables, params, summary)
    else:
        params, summary = fit_everything(book, tables)
    write_json(a.out, params)
    write_json(a.summary, summary)
    print("wrote", a.out, a.summary)


if __name__ == "__main__":
    main()
