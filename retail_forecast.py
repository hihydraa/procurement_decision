"""
พยากรณ์ราคาขายปลีก (ทดลอง ยังไม่พิสูจน์) — ต่อยอดจากการ์ดราคาขายส่ง (WS) ของ EPPO

หลักการ : ปลีกไม่ได้ตามราคาส่งตรง ๆ ปรับเป็นก้อนเอง (ประมาณ 28% ของวัน ขนาดมัธยฐาน ~0.75–0.80 บาท/ลิตร ไม่ปรับวันอาทิตย์)
  1) เอาความน่าจะเป็นของ WS วันเป้าหมาย (ลง/นิ่ง/ขึ้น จากการ์ด) คูณกับ "ความถี่ที่ปลีกปรับในวันเดียวกันเมื่อ WS เป็นสถานะนั้น" จากข้อมูล EPPO ย้อนหลัง (smoothing)
  2) ปรับด้วยอัตราการปรับของปลีกตามวันในสัปดาห์ (เมื่อมีข้อมูลวันนั้น >= MIN_DOW_N วัน)
  ไม่มีตัวแปรที่จูนเอง : ทุกตัวเลขมาจากความถี่ในข้อมูล จึงรายงาน n เสมอ ; ความแม่นของ WS เองยังไม่พิสูจน์ ปลีกจึงไม่ดีกว่านั้น

ข้อมูลเข้า : DataFrame จาก diag_rule_backtest.eppo_daily (day, fuel, ws, mm, retail, ...) ไม่นับแถว CARRIED
"""
from __future__ import annotations

import numpy as np
import pandas as pd

MOVE = 0.10                 # ขยับ >= นี้ถือเป็นขึ้น/ลง (เกณฑ์เดียวกับ WS ในการ์ด)
ALPHA = 1.0                 # Laplace smoothing
MIN_DOW_N = 10
CALL_P, CALL_GAP, FLAT_P = 0.35, 0.15, 0.60
CLASSES = ("ลง", "นิ่ง", "ขึ้น")


def classify(d: float, thr: float = MOVE) -> str:
    return "ลง" if d <= -thr else "ขึ้น" if d >= thr else "นิ่ง"


def history(eppo: pd.DataFrame, fuel: str) -> pd.DataFrame:
    """คู่วันติดกัน (t-1, t) : s = สถานะ WS ของวัน t, r = ชนิดการเปลี่ยนของปลีก, dr = ปลีกเปลี่ยนกี่บาท, dow"""
    g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
    rows = []
    for day, r in g.iterrows():
        prev = day - pd.Timedelta(days=1)
        if prev not in g.index:
            continue
        dws, dr = float(r.ws - g.ws[prev]), float(r.retail - g.retail[prev])
        rows.append((day, classify(dws), classify(dr), dr, day.dayofweek))
    return pd.DataFrame(rows, columns=["day", "s", "r", "dr", "dow"])


def transition(h: pd.DataFrame) -> dict:
    """P(ปลีกของวันเดียวกัน | สถานะ WS) แบบ smoothing"""
    out = {}
    for s in CLASSES:
        x = h[h.s == s]
        n = len(x)
        out[s] = {r: (int((x.r == r).sum()) + ALPHA) / (n + ALPHA * len(CLASSES)) for r in CLASSES}
        out[s]["n"] = n
    return out


def dow_scale(h: pd.DataFrame, weekday: int) -> tuple:
    """(ตัวคูณโอกาสปรับ, n วันนั้น) : อัตราปรับของวันนั้น (smoothing) / อัตราปรับรวม ; ข้อมูลน้อย = 1.0"""
    x = h[h.dow == weekday]
    if len(x) < MIN_DOW_N or h.empty:
        return 1.0, len(x)
    rate_all = ((h.r != "นิ่ง").sum() + 1) / (len(h) + 2)
    rate_dow = ((x.r != "นิ่ง").sum() + 1) / (len(x) + 2)
    return float(np.clip(rate_dow / rate_all, 0.1, 2.0)), len(x)


def move_sizes(h: pd.DataFrame) -> dict:
    """ขนาดการปรับของปลีกเมื่อปรับ (บาท/ลิตร, ค่าสัมบูรณ์) แยกทิศ : มัธยฐาน, p25, p75, n"""
    out = {}
    for name in ("ลง", "ขึ้น"):
        v = h[h.r == name].dr.abs()
        out[name] = {"med": float(v.median()), "p25": float(v.quantile(.25)), "p75": float(v.quantile(.75)), "n": int(len(v))} if len(v) >= 5 else None
    return out


def retail_probs(p_ws: dict, trans: dict, scale: float = 1.0) -> dict:
    mix = {r: sum(p_ws[s] * trans[s][r] for s in CLASSES) for r in CLASSES}
    pd_, pu = min(mix["ลง"] * scale, 0.95), min(mix["ขึ้น"] * scale, 0.95)
    if pd_ + pu > 0.98:
        k = 0.98 / (pd_ + pu)
        pd_, pu = pd_ * k, pu * k
    return {"ลง": pd_, "นิ่ง": 1.0 - pd_ - pu, "ขึ้น": pu}


def decide(p: dict) -> str:
    if p["นิ่ง"] >= FLAT_P:
        return "นิ่ง"
    d, u = p["ลง"], p["ขึ้น"]
    if d >= CALL_P and d - u >= CALL_GAP:
        return "ลง"
    if u >= CALL_P and u - d >= CALL_GAP:
        return "ขึ้น"
    return "ไม่ชัด"


def forecast(eppo: pd.DataFrame, fuel: str, p_ws: dict, target_day: pd.Timestamp, retail_base: float | None) -> dict | None:
    """p_ws = {'ลง','นิ่ง','ขึ้น'} ของ WS วันเป้าหมาย (จากการ์ด) ; None ถ้าประวัติไม่พอ"""
    h = history(eppo, fuel)
    if len(h) < 40 or any(k not in p_ws for k in CLASSES):
        return None
    scale, n_dow = dow_scale(h, pd.Timestamp(target_day).dayofweek)
    p = retail_probs(p_ws, transition(h), scale)
    sizes = move_sizes(h)
    call = decide(p)
    sz = sizes.get(call) if call in ("ลง", "ขึ้น") else None
    size_text = f"ถ้าปรับ มัธยฐาน {sz['med']:.2f} บาท/ลิตร (ช่วงที่พบบ่อย {sz['p25']:.2f}–{sz['p75']:.2f})" if sz else ""
    return {"p_down": p["ลง"], "p_flat": p["นิ่ง"], "p_up": p["ขึ้น"], "call": call, "size_text": size_text,
            "size_med": sz["med"] if sz else "", "retail_base": "" if retail_base is None else float(retail_base),
            "n_hist": int(len(h)), "dow_scale": round(scale, 2), "dow_n": n_dow}


# ---------- โมเดลจากสัญญาณ Jobber (ดู exp_retail_search.py) ----------
SIGNAL_FEATURES = ("up_n", "dn_n", "close_last", "fund_next")
SIGNAL_CLIPS = {"up_n": (0, 4), "dn_n": (0, 4), "close_last": (-8.0, 8.0), "fund_next": (-3.0, 3.0)}
SIGNAL_HOUR = 20.5          # สัญญาณของคืนก่อนวันเป้าหมาย ตัดที่ 20:30


def signal_features(tables, fuel: str, day: pd.Timestamp) -> dict:
    """สัญญาณ ณ 20:30 ของวัน day (วันก่อนวันเป้าหมาย) — ใช้เฉพาะข้อความที่โพสต์ก่อนเวลานั้น"""
    import signals as S
    f = S.build_features(pd.Timestamp(day).normalize() + pd.Timedelta(hours=SIGNAL_HOUR), tables, fuel)
    return {k: f.get(k) for k in SIGNAL_FEATURES}


def signal_vector(f: dict) -> np.ndarray:
    out = []
    for k in SIGNAL_FEATURES:
        v = f.get(k)
        v = 0.0 if v is None or (isinstance(v, float) and np.isnan(v)) else float(v)
        out.append(min(max(v, SIGNAL_CLIPS[k][0]), SIGNAL_CLIPS[k][1]))
    return np.array(out)


SIGNAL_MODEL_FILE = "retail_signal_model.json"
WS_SIGNAL_MODEL_FILE = "ws_signal_model.json"       # เป้า = ราคาขายส่ง EPPO ; ฝึกเฉพาะปี 2026 (กลไกกองทุนต่างจากปีก่อน ดู exp_ws_search.py)
WS_TRAIN_FROM = "2026-01-01"
LONG_HISTORY_FILE = "data/eppo_daily_long.csv"
SIGNAL_MAX_AGE_H = 12.0     # ฟีดสัญญาณต้องมีข้อความใหม่ภายในกี่ชั่วโมงก่อนเวลาตัด จึงถือว่าใช้ได้


def long_history(eppo: pd.DataFrame, path: str = None) -> pd.DataFrame:
    """ต่อประวัติราคาจากไฟล์โครงสร้างราคา EPPO (ตั้งแต่ปลายปี 2023) ไว้หน้าข้อมูลในชีท — ใช้เฉพาะวันที่ก่อนวันแรกของชีท จึงไม่ทับค่าในชีท
    เหตุผล : ประวัติสั้น (ตั้งแต่ มี.ค. 2026) ทำให้ความถี่ที่ปลีกตาม WS สูงเกินจริง (ดูผลรอบ 2 ใน exp_retail_search.py)"""
    import os
    path = path or os.path.join(os.path.dirname(os.path.abspath(__file__)), LONG_HISTORY_FILE)
    try:
        old = pd.read_csv(path, parse_dates=["day"])
    except (OSError, ValueError):
        return eppo
    if eppo is None or eppo.empty:
        return old
    old = old[old.day < eppo.day.min()]
    cols = [c for c in eppo.columns if c in old.columns]
    return pd.concat([old[cols], eppo], ignore_index=True).sort_values(["fuel", "day"]).reset_index(drop=True)


def fit_signal_model(tables, eppo: pd.DataFrame, fuel: str, last_signal_day: pd.Timestamp, col: str = "retail", start=None) -> dict | None:
    """ฝึกโลจิสติกส์ 2 ตัว (ขึ้น/ลง >= MOVE) จากคู่วันติดกันของราคา (col = retail หรือ ws) + สัญญาณ 20:30 ของวันก่อน ; start = ใช้ข้อมูลตั้งแต่วันใด"""
    import model as M
    g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
    if start is not None:
        g = g[g.index >= pd.Timestamp(start)]
    X, y, days = [], [], []
    for day, r in g.iterrows():
        nxt = day + pd.Timedelta(days=1)
        if nxt not in g.index or day > last_signal_day:
            continue
        X.append(signal_vector(signal_features(tables, fuel, day)))
        y.append(float(g[col][nxt] - r[col]))
        days.append(day)
    if len(y) < 100:
        return None
    X, y = np.vstack(X), np.array(y)
    yu, yd = (y >= MOVE).astype(float), (y <= -MOVE).astype(float)
    if not (0 < yu.sum() < len(y) and 0 < yd.sum() < len(y)):
        return None
    return {"up": M.fit_logistic(X, yu), "down": M.fit_logistic(X, yd), "n": int(len(y)), "n_up": int(yu.sum()), "n_down": int(yd.sum()),
            "from": str(min(days).date()), "to": str(max(days).date())}


def signals_fresh(tables, asof: pd.Timestamp) -> bool:
    """ฟีดสัญญาณ (ประกาศปรับราคา / MOPS ปิด / กองทุน) มีข้อความภายใน SIGNAL_MAX_AGE_H ก่อน asof หรือไม่"""
    newest = [df.ts[df.ts <= asof].max() for df in (tables.headsup, tables.mops_close, tables.fund) if not df.empty and (df.ts <= asof).any()]
    return bool(newest) and (asof - max(newest)) <= pd.Timedelta(hours=SIGNAL_MAX_AGE_H)


def signal_forecast(model: dict, tables, fuel: str, target_day: pd.Timestamp) -> dict | None:
    """ความน่าจะเป็นของปลีกวันเป้าหมายจากสัญญาณคืนก่อน (บันทึกคู่ขนาน ยังไม่ใช้แสดงผล) ; None ถ้าไม่มีโมเดล/ฟีดไม่สด"""
    import model as M
    m = (model or {}).get(fuel)
    eve = pd.Timestamp(target_day).normalize() - pd.Timedelta(days=1)
    asof = eve + pd.Timedelta(hours=SIGNAL_HOUR)
    if not m or not signals_fresh(tables, asof):
        return None
    x = signal_vector(signal_features(tables, fuel, eve))
    pu, pd_ = float(M.predict_proba(m["up"], x)[0]), float(M.predict_proba(m["down"], x)[0])
    if pu + pd_ > 0.98:
        k = 0.98 / (pu + pd_)
        pu, pd_ = pu * k, pd_ * k
    p = {"ลง": pd_, "นิ่ง": 1.0 - pu - pd_, "ขึ้น": pu}
    return {"p_down": pd_, "p_flat": p["นิ่ง"], "p_up": pu, "call": decide(p)}


def main(argv):
    """python retail_forecast.py fit <data_dir ของ jobber> [eppo_daily.csv] → เขียน retail_signal_model.json"""
    import json
    import fit_model as F
    _, tables = F.load(argv[1])
    eppo = pd.read_csv(argv[2] if len(argv) > 2 else LONG_HISTORY_FILE, parse_dates=["day"])
    last = tables.headsup.ts.max().normalize() - pd.Timedelta(days=1)
    out = {"features": list(SIGNAL_FEATURES), "move": MOVE, "signal_hour": SIGNAL_HOUR}
    for fuel in ("B7", "G95"):
        m = fit_signal_model(tables, eppo, fuel, last)
        if m:
            out[fuel] = m
            print(f"{fuel}: n={m['n']} ({m['from']} ถึง {m['to']}) ขึ้น {m['n_up']} ลง {m['n_down']} ; w ขึ้น {np.round(m['up']['w'], 2).tolist()} ; w ลง {np.round(m['down']['w'], 2).tolist()}")
    with open(SIGNAL_MODEL_FILE, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=1)
    ws = {"features": list(SIGNAL_FEATURES), "move": MOVE, "signal_hour": SIGNAL_HOUR, "target": "ws", "train_from": WS_TRAIN_FROM}
    for fuel in ("B7", "G95"):
        m = fit_signal_model(tables, eppo, fuel, last, col="ws", start=WS_TRAIN_FROM)
        if m:
            ws[fuel] = m
            print(f"WS {fuel}: n={m['n']} ({m['from']} ถึง {m['to']}) ขึ้น {m['n_up']} ลง {m['n_down']} ; w ขึ้น {np.round(m['up']['w'], 2).tolist()} ; w ลง {np.round(m['down']['w'], 2).tolist()}")
    with open(WS_SIGNAL_MODEL_FILE, "w", encoding="utf-8") as fh:
        json.dump(ws, fh, ensure_ascii=False, indent=1)


def outcome(eppo: pd.DataFrame, fuel: str, target_day: pd.Timestamp):
    """(ปลีกเปลี่ยนกี่บาท, ชนิด) ของวันเป้าหมายเทียบวันก่อน ; None ถ้ายังไม่มีข้อมูลวันนั้น"""
    g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").retail
    t = pd.Timestamp(target_day).normalize()
    if t not in g.index or (t - pd.Timedelta(days=1)) not in g.index:
        return None
    d = float(g[t] - g[t - pd.Timedelta(days=1)])
    return round(d, 2), classify(d)


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3 and sys.argv[1] == "fit":
        main(sys.argv[1:])
