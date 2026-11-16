"""
รอบเที่ยงของดีเซล แบบเงา (shadow) — บันทึกคำทายลงแท็บ NoonShadowLog แล้วเติมผลจริง ; ไม่แสดงในข้อความ/หน้าเว็บ ไม่กระทบการตัดสินใจ

ที่มา : exp_two_rounds.py (16/11/2026) — ณ 11:00–13:00 โมเดลดีเซลชุด v2 ทายได้ว่า "ราคาต่ำสุดของตลาด ณ 06:00 วันถัดไป ต่ำกว่าตอนนี้ >= 10 สต."
        (ย้อนหลังฝั่งลงถูก 71–78%) ; ฝั่ง "ขึ้น" ถูกเพียง 42–57% และ G95 ไม่ผ่านทุกเวลา — ผลมาจากการเลือกจากหลายชุดทดสอบ จึงต้องดูผลจริงก่อนใช้
คำทาย  : ณ 12:00 ของวัน d (ฟีเจอร์ใช้เฉพาะข้อความ/ราคาที่โพสต์ <= 12:00) → ราคา ณ 06:00 ของวัน d+1 เทียบ 12:00 : DOWN / UP / ว่าง (ไม่ฟันธง ; เกณฑ์ 0.50 เท่าโมเดลจริง)
บันทึก : หนึ่งแถวต่อวัน รอบแรกที่แดชบอร์ดรันหลัง 12:00 ; ออกช้าเกิน ON_TIME_LAG_H = late (นับแยก) ; ช้าเกิน MAX_LAG_H ไม่บันทึก
เกณฑ์ที่ตั้งไว้ก่อนเริ่ม (ห้ามปรับตามผล) : จะพิจารณาแสดงผลได้เมื่อมีผลจริงตรงเวลา >= 40 วัน และฝั่ง DOWN ถูกทิศ >= 60% ผิดทิศ <= 12% ; ฝั่ง UP ไม่ใช้แม้ผลจริงจะดี

ใช้: python log_noon.py                    (ต้องมี SA_EMAIL / SA_PRIVATE_KEY และ noon_snapshot.json จาก main.py)
     python log_noon.py fit <data_dir>     (ฝึกโมเดลจากประวัติ Jobber → noon_shadow_model.json)
"""
import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import model as M
import signals as S

FUEL = "B7"
REF_HOUR, TARGET_HOUR = 12.0, 6.0
HZ = 24.0 + TARGET_HOUR - REF_HOUR                 # 18 ชม. : 12:00 วัน d → 06:00 วัน d+1
MODEL_FILE = "noon_shadow_model.json"
SNAPSHOT_FILE = "noon_snapshot.json"
COLUMNS = ["asof", "date", "ref", "fuel", "target", "call", "p_up", "p_down", "price_ref", "price_target", "outcome_delta", "outcome_filled_at",
           "lag_h", "late", "model_version"]
LAST_COL = "O"
OUTCOME_FROM = COLUMNS.index("price_ref")
ON_TIME_LAG_H, MAX_LAG_H = 3.0, 12.0
READY_DAYS = 40


def _here(name: str) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), name)


def load_model(path: str = None) -> dict:
    try:
        with open(path or _here(MODEL_FILE), encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


# ---------- คำทาย (เรียกจาก main.py ซึ่งโหลดฟีดไว้แล้ว) ----------
def decide(tables, asof, model: dict = None):
    """คำทายรอบ 12:00 ของวันเดียวกับ asof ; None ถ้ายังไม่ถึง 12:00 / ไม่มีโมเดล / ฟีดราคาไม่สด (ไม่ฝืนทายด้วยข้อมูลค้าง)"""
    asof = pd.Timestamp(asof)
    model = load_model() if model is None else model
    ref = asof.normalize() + pd.Timedelta(hours=REF_HOUR)
    if asof < ref or not model.get("models") or not tables.has_prices(FUEL, ref):
        return None
    x = M.vectorize_v2(S.build_features_v2(ref, tables, FUEL, HZ))
    pu, pd_ = float(M.predict_proba(model["models"]["r10"], x)[0]), float(M.predict_proba(model["models"]["f10"], x)[0])
    call = "UP" if (pu >= M.OUTLOOK_MIN_P and pu > pd_) else "DOWN" if (pd_ >= M.OUTLOOK_MIN_P and pd_ > pu) else ""
    return {"asof": asof.strftime("%Y-%m-%d %H:%M"), "ref": ref.strftime("%Y-%m-%d %H:%M"), "fuel": FUEL,
            "target": (ref + pd.Timedelta(hours=HZ)).strftime("%Y-%m-%d %H:%M"), "call": call, "p_up": round(pu, 4), "p_down": round(pd_, 4),
            "model_version": str(model.get("trained_to", ""))}


def write_snapshot(tables, asof, path: str = SNAPSHOT_FILE) -> None:
    d = decide(tables, asof)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump([d] if d else [], fh, ensure_ascii=False, indent=1)


# ---------- บันทึก + เติมผล ----------
def _key(v) -> str:
    from common import to_ts
    t = to_ts(v)
    return str(v) if pd.isna(t) else t.strftime("%Y-%m-%d %H:%M")


def new_rows(existing: list, snapshot: list) -> list:
    """แถวใหม่ (คีย์ = ref + ชนิด) : รอบแรกของวันนั้นเท่านั้น ; ช้าเกิน MAX_LAG_H ไม่บันทึก"""
    seen = {(_key(r.get("ref")), str(r.get("fuel"))) for r in existing}
    out = []
    for s in snapshot:
        lag = (pd.Timestamp(s["asof"]) - pd.Timestamp(s["ref"])).total_seconds() / 3600
        key = (_key(s["ref"]), str(s["fuel"]))
        if lag > MAX_LAG_H or lag < 0 or key in seen:
            continue
        seen.add(key)
        row = {c: s.get(c, "") for c in COLUMNS}
        row.update({"date": pd.Timestamp(s["ref"]).strftime("%Y-%m-%d"), "lag_h": round(lag, 2), "late": int(lag > ON_TIME_LAG_H)})
        for c in COLUMNS[OUTCOME_FROM:OUTCOME_FROM + 4]:
            row[c] = ""
        out.append(row)
    return out


def compute_outcome(book, ref, target, now):
    """(ราคา ณ ref, ราคา ณ target, ผลต่าง) หรือ None ถ้ายังไม่เลยเวลาเป้าหมาย 30 นาที / ไม่มีราคา"""
    t0, t1 = pd.Timestamp(ref), pd.Timestamp(target)
    if now < t1 + pd.Timedelta(minutes=30):
        return None
    a, b = book.min_at(FUEL, [t0]).iloc[0], book.min_at(FUEL, [t1]).iloc[0]
    if pd.isna(a) or pd.isna(b):
        return None
    return float(a), float(b), round(float(b - a), 2)


def summary(rows: list) -> list:
    """สรุปผลจริงของแถวตรงเวลา (late = 0) แยกทิศ ; ใช้พิมพ์ใน log ของ workflow"""
    num = lambda v: pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    done = [(str(r.get("call", "")), float(num(r.get("outcome_delta")))) for r in rows
            if str(r.get("outcome_delta", "")) != "" and str(r.get("late", "0")) in ("0", "0.0", "")]
    out = [f"[NOON] ผลจริงตรงเวลา {len(done)}/{READY_DAYS} วัน"]
    for call, sign in (("DOWN", -1.0), ("UP", 1.0)):
        mv = [sign * d for c, d in done if c == call]
        if mv:
            out.append(f"[NOON] ทาย{'ลง' if call == 'DOWN' else 'ขึ้น'} {len(mv)} ครั้ง · ถูก {sum(m >= M.OUTLOOK_SMALL for m in mv)} · ผิดทิศ {sum(m <= -M.OUTLOOK_SMALL for m in mv)}"
                       f" · เฉลี่ยตามทิศ {np.mean(mv):+.2f} บาท/ลิตร")
    return out


def main() -> int:
    from common import SHEET_ID, TABS, TZ_NAME, ensure_tab, get_token, sheets_append, sheets_get, sheets_update, to_ts
    now = pd.Timestamp(datetime.now(ZoneInfo(TZ_NAME)).replace(tzinfo=None))
    snapshot = json.load(open(SNAPSHOT_FILE, encoding="utf-8")) if os.path.exists(SNAPSHOT_FILE) else []
    tok = get_token()
    tab = TABS["NOON_SHADOW_LOG"]
    if ensure_tab(tok, SHEET_ID, tab):
        sheets_update(tok, SHEET_ID, f"{tab}!A1", [COLUMNS])
        print(f"[NOON] created tab {tab}")
    values = sheets_get(tok, SHEET_ID, f"{tab}!A1:{LAST_COL}5000")
    head = values[0] if values else COLUMNS
    rows = [dict(zip(head, r + [""] * (len(head) - len(r)))) for r in values[1:]]
    add = new_rows(rows, snapshot)
    if add:
        sheets_append(tok, SHEET_ID, f"{tab}!A1:{LAST_COL}1", [[r.get(c, "") for c in COLUMNS] for r in add])
        rows += add
    print(f"[NOON] snapshot rows: +{len(add)} new")
    prices = pd.DataFrame(sheets_get(tok, SHEET_ID, f"{TABS['FEED_PRICES']}!A1:E200000"))
    if len(prices) <= 1:
        print("[NOON] no price feed yet — outcomes not filled")
        return 0
    prices.columns = prices.iloc[0]
    prices = prices.iloc[1:].reset_index(drop=True)
    prices["effective"] = prices["effective"].map(to_ts)
    book = S.PriceBook(prices)
    a, b = chr(ord("A") + OUTCOME_FROM), chr(ord("A") + OUTCOME_FROM + 3)
    filled = 0
    for i, r in enumerate(rows):
        if str(r.get("outcome_delta", "")) != "" or not r.get("ref") or not r.get("target"):
            continue
        ref, tgt = to_ts(r["ref"]), to_ts(r["target"])
        if pd.isna(ref) or pd.isna(tgt):
            continue
        res = compute_outcome(book, ref, tgt, now)
        if res:
            r["price_ref"], r["price_target"], r["outcome_delta"] = res
            r["outcome_filled_at"] = now.strftime("%Y-%m-%d %H:%M")
            sheets_update(tok, SHEET_ID, f"{tab}!{a}{i + 2}:{b}{i + 2}", [[r["price_ref"], r["price_target"], r["outcome_delta"], r["outcome_filled_at"]]])
            filled += 1
    print(f"[NOON] outcomes filled: {filled}")
    print("\n".join(summary(rows)))
    return 0


# ---------- ฝึก ----------
def fit(data_dir: str, path: str = None) -> dict:
    """ฝึกจากประวัติ Jobber ทั้งหมด (โลจิสติกส์เดียวกับของจริง ชุด v2) + แนบสถิติ walk-forward จาก exp_two_rounds.evaluate"""
    import exp_two_rounds as X
    import fit_model as F
    prices, tables = F.load(data_dir)
    book = S.PriceBook(prices)
    df = F.build_outlook_dataset(book, tables, FUEL, HZ, REF_HOUR, v2=True)
    Xm = np.vstack([M.vectorize_v2(r) for r in df.to_dict("records")])
    y = df.y.to_numpy()
    e = X.evaluate(book, tables, FUEL, REF_HOUR, HZ, True)
    out = {"fuel": FUEL, "ref_hour": REF_HOUR, "target_hour": TARGET_HOUR, "hz": HZ, "feature_set": "v2",
           "models": {"r10": M.fit_logistic(Xm, (y >= M.OUTLOOK_SMALL).astype(float)), "f10": M.fit_logistic(Xm, (y <= -M.OUTLOOK_SMALL).astype(float))},
           "n": int(len(y)), "trained_from": str(df.date.min().date()), "trained_to": str(df.date.max().date()),
           "walk_forward": {k: (round(float(v), 4) if isinstance(v, float) else v) for k, v in e.items()}}
    with open(path or _here(MODEL_FILE), "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=1)
    return out


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "fit":
        m = fit(sys.argv[2])
        print(f"{FUEL} 12:00 → 06:00 : n={m['n']} ({m['trained_from']} ถึง {m['trained_to']}) ; walk-forward {m['walk_forward']}")
    else:
        sys.exit(main())
