"""
ประเมินแผนวันถัดไปจากชีท "OIL BOT - Forecast Log" (แท็บ Plan/Backtest เขียนโดยงานตั้งเวลาอีกชุด ไม่ใช่โค้ดในรีโปนี้) กับราคาจริงใน PriceLog — อ่านอย่างเดียว
(1) Plan : แต่ละแถวมี tap_time (เวลาที่ควรแตะราคา) → regret = ราคา ณ เวลานั้นของคลังที่ระบุ − ราคาต่ำสุดของช่วง ; เทียบกับแตะเวลาคงที่
    ความหมายของ tap_time ยังไม่ยืนยัน จึงคำนวณ 2 แบบ : (a) แตะเวลานั้นของวัน plan_date  (b) แตะเวลานั้นของวันก่อน plan_date (ช่วงเทียบเริ่ม 06:00 วันก่อน)
    นับเป็น 1 หน่วยต่อ (วัน, คลัง, ชนิด, tap_time) ไม่นับซ้ำตามสาขา
(2) Backtest : ความแม่นของทิศ (ถูกทิศ/ผิดทิศ) ที่งานนั้นบันทึกไว้

ใช้: python diag_plan_eval.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY ; บัญชีบริการต้องเปิดชีท Forecast Log ได้)
"""
import re

import numpy as np
import pandas as pd

from common import OILBOT_SHEET_ID, get_token, sheets_get, to_ts
from diag_po import po_depot
from pricelog_frame import build_frame
from tap_policy import DEPOTS, TAPS, depot_series, price_at, window_regrets

FORECAST_LOG_ID = "1WNaW8o9DXn4Tei4V779BPVanUyVjx0Tpt9hLUcQft8k"
LONG_START_H = -18.0                 # แบบ (b) : ช่วงเริ่ม 06:00 วันก่อน (D 00:00 − 18 ชม.)
STD_START_H, END_H, STEP_H = -3.5, 22.0, 0.5


def tap_hours(v):
    """'16:30' → 16.5 ; อื่น ๆ → None"""
    m = re.match(r"^\s*(\d{1,2}):(\d{2})", str(v))
    return int(m.group(1)) + int(m.group(2)) / 60 if m else None


def plan_units(values: list) -> pd.DataFrame:
    """Plan → หน่วยประเมิน (day, depot_key, fuel, hours) ไม่ซ้ำ ; depot ที่ไม่ใช่คลังหลัก/อ่านเวลาไม่ได้ ตัดออก"""
    head = [str(h).strip() for h in values[0]]
    rows = []
    for r in values[1:]:
        d = dict(zip(head, r + [""] * (len(head) - len(r))))
        day, key, hrs = to_ts(d.get("plan_date")), po_depot(str(d.get("depot", ""))), tap_hours(d.get("tap_time", ""))
        if pd.isna(day) or key is None or hrs is None or not str(d.get("fuel", "")).strip():
            continue
        rows.append((day.normalize(), key, str(d["fuel"]).strip(), hrs))
    return pd.DataFrame(rows, columns=["day", "depot", "fuel", "hours"]).drop_duplicates().reset_index(drop=True)


def regret_at(series, tap: pd.Timestamp, win_start: pd.Timestamp, win_end: pd.Timestamp):
    """ราคา ณ เวลา tap − ต่ำสุดของช่วง [win_start, win_end] ; NaN ถ้าไม่มีราคา"""
    grid = pd.date_range(win_start, win_end, freq=f"{int(STEP_H * 60)}min")
    g = price_at(series, grid)
    p = price_at(series, pd.DatetimeIndex([tap]))[0]
    return float(p - np.nanmin(g)) if (~np.isnan(g)).any() and not np.isnan(p) else float("nan")


def h(x):
    return pd.Timedelta(hours=float(x))


def evaluate(units: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, u in units.iterrows():
        s = depot_series(prices, u["depot"], u["fuel"])
        w = window_regrets(s, u["day"])
        rows.append({**u.to_dict(), "plan_a": regret_at(s, u["day"] + h(u["hours"]), u["day"] + h(STD_START_H), u["day"] + h(END_H)),
                     "plan_b": regret_at(s, u["day"] - pd.Timedelta(days=1) + h(u["hours"]), u["day"] + h(LONG_START_H), u["day"] + h(END_H)),
                     **{f"fix_{k}": (w[k] if w else float("nan")) for k in TAPS}})
    return pd.DataFrame(rows)


def plan_report(units: pd.DataFrame, ev: pd.DataFrame) -> list:
    if units.empty:
        return ["[PLAN] ไม่พบแถวที่ประเมินได้"]
    out = [f"[PLAN] หน่วยประเมิน {len(units)} (วัน {units.day.nunique()} : {units.day.min():%d/%m}–{units.day.max():%d/%m}) ; tap_time ที่ใช้ : "
           + ", ".join(f"{int(h)}:{int(round((h % 1) * 60)):02d}×{n}" for h, n in units.hours.value_counts().items())]
    ok = ev.dropna(subset=["plan_a"])
    if ok.empty:
        return out + ["[PLAN-EVAL] ราคายังไม่ครบถึงสิ้นวันของแผนใด ๆ"]
    fixed = " ".join(f"{k} {ok[f'fix_{k}'].mean():.3f}" for k in ("ก่อน22:30", "07:00", "16:30"))
    out.append(f"[PLAN-EVAL] (a) แตะเวลา tap_time ของวัน plan_date : {len(ok)} หน่วย {ok.day.nunique()} วัน ; regret เฉลี่ยของแผน {ok.plan_a.mean():.3f} บาท/ลิตร | แตะคงที่ (ช่วงเดียวกัน) {fixed}")
    okb = ev.dropna(subset=["plan_b"])
    out.append(f"   (b) แตะเวลา tap_time ของวันก่อน plan_date (ช่วงเทียบเริ่ม 06:00 วันก่อน) : {len(okb)} หน่วย ; regret เฉลี่ย {okb.plan_b.mean():.3f}" if len(okb) else "   (b) ไม่มีหน่วยที่ราคาครบ")
    for day, g in ok.groupby("day"):
        out.append(f"   {day:%d/%m}: แผน(a) {g.plan_a.mean():.3f} ; ต่ำสุดของวัน (oracle) 0.000 ; คลัง-ชนิดที่เทียบ {len(g)}")
    out.append("   ข้อจำกัด : แผนใช้ 16:30 ค่าเริ่มต้นแทบทุกแถว จึงเท่ากับนโยบายแตะ 16:30 คงที่ ; จำนวนวันยังน้อยมาก ไม่ใช่ข้อสรุป")
    return out


def backtest_report(values: list) -> list:
    if len(values) < 2:
        return ["[PLAN-BT] ไม่มีข้อมูล Backtest"]
    head = [str(h).strip() for h in values[0]]
    d = pd.DataFrame([r + [""] * (len(head) - len(r)) for r in values[1:]], columns=head)
    d["day"] = d["target_date"].map(to_ts)
    d = d.dropna(subset=["day"])
    d["right"] = d["direction"].astype(str).str.contains("ถูก")
    d["wrong"] = d["direction"].astype(str).str.contains("ผิด")
    out = [f"[PLAN-BT] Backtest {len(d)} แถว {d.day.min():%d/%m}–{d.day.max():%d/%m} ({d.day.nunique()} วัน) ; ถูกทิศ {int(d.right.sum())} ผิดทิศ {int(d.wrong.sum())}"
           f" ({d.right.sum() / max(d.right.sum() + d.wrong.sum(), 1):.0%} ของที่ตัดสินได้) — อยู่ในตัวอย่างย้อนหลัง ไม่ใช่ผลเดินหน้า"]
    for fuel, g in d.groupby("fuel"):
        out.append(f"   {fuel}: ถูก {int(g.right.sum())} ผิด {int(g.wrong.sum())}")
    return out


def main():
    tok = get_token()
    plan = sheets_get(tok, FORECAST_LOG_ID, "Plan!A1:L5000")
    units = plan_units(plan)
    prices = build_frame(sheets_get(tok, OILBOT_SHEET_ID, "PriceLog!A1:Q200000")).reset_index(drop=True)
    for line in plan_report(units, evaluate(units, prices)):
        print(line)
    for line in backtest_report(sheets_get(tok, FORECAST_LOG_ID, "Backtest!A1:G5000")):
        print(line)


if __name__ == "__main__":
    main()
