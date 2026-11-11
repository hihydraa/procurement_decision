"""
ติดตามแบบเงา (shadow) : บันทึก "จุดแตะที่นโยบาย A/B/C แนะนำ" ควบคู่แนวโน้ม 24 ชม. รอบ 20:30 ของแต่ละวันลงแท็บ TapRecLog
แล้วเติม regret ที่เกิดจริงของแต่ละนโยบายและของการแตะเวลาคงที่ เมื่อวันนั้นผ่านไป (หลัง 22:30) — ไม่ส่งข้อความใด ๆ ไม่กระทบการตัดสินใจจริง

หนึ่งแถว = รอบ 20:30 ของวัน r × ชนิดน้ำมัน (วัน D = r+1) ; นโยบายกำหนดไว้ใน tap_policy.POLICY_MAPS ก่อนเริ่มติดตาม (ห้ามปรับตามผล)
ติดตามเฉพาะรอบตั้งแต่ SHADOW_START เพื่อไม่ปนกับช่วงที่เห็นผลแล้ว (นโยบายถูกกำหนดหลังดูข้อมูลถึง 9 พ.ย.)

regret = บาท/ลิตรที่ราคา ณ จุดแตะ สูงกว่าราคาต่ำสุดของช่วง 20:30 วันก่อน–22:00 วัน D เฉลี่ยข้ามคลัง ; ไม่หักเงื่อนไขคลัง (ดู tap_policy)

ใช้: python log_taprec.py    (ต้องมี SA_EMAIL / SA_PRIVATE_KEY)
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from common import OILBOT_SHEET_ID, SHEET_ID, TABS, TZ_NAME, ensure_tab, get_token, sheets_append, sheets_get, sheets_update, to_ts
from pricelog_frame import build_frame
from tap_policy import DEPOTS, FIXED_TAPS, FLAT, POLICY_MAPS, depot_series, diff_ci, load_outlook, pred_of, window_regrets

COLUMNS = ["ref", "day", "fuel", "tier", "pred", "rec_A", "rec_B", "rec_C", "model_version", "logged_at",
           "n_depots", "delta", "regret_A", "regret_B", "regret_C", "fixed_2230", "fixed_0700", "fixed_1630", "actual_cls", "filled_at",
           "lag_h", "late"]                                  # lag_h = ชม. ที่แนวโน้มออกช้ากว่า 20:30 ; late = 1 ถ้าเกิน ON_TIME_LAG_H (ไม่ทันใช้จริง)
OUTCOME_FROM = COLUMNS.index("n_depots")
OUTCOME_TO = COLUMNS.index("filled_at")
LAST_COL = "V"
MAX_LAG_H = 12.0                    # รับแนวโน้มที่ออกช้าได้ถึงกี่ชม. (เท่ากับ MAX_LOG_LAG_H ของ OutlookLog)
ON_TIME_LAG_H = 3.0                 # ภายในเกณฑ์นี้ = ตรงเวลา (นับในผลหลัก) ; ช้ากว่านี้นับแยก
SHADOW_START = pd.Timestamp("2026-11-10")          # รอบ 20:30 ตั้งแต่วันนี้เป็นต้นไป
MIN_DEPOTS = 3                                      # ต้องมีราคาอย่างน้อยกี่คลังจึงเติมผล
FILL_AFTER_H = 22.5                                 # เติมผลเมื่อเลยเวลานี้ของวัน D (ช่วงวัดสิ้นสุด 22:00)
FIXED_COLS = {"ก่อน22:30": "fixed_2230", "07:00": "fixed_0700", "16:30": "fixed_1630"}


def _ts(v):
    t = to_ts(v)
    return None if pd.isna(t) else t


def new_rows(existing: set, outlook: pd.DataFrame, now: pd.Timestamp) -> list:
    """แถวใหม่จากแนวโน้ม 24 ชม. รอบ 20:30 ใน OutlookLog (ออกช้าได้ถึง MAX_LAG_H) ที่ยังไม่เคยบันทึก (คีย์ = ref + ชนิด) เฉพาะ ref >= SHADOW_START
    แนวโน้มคำนวณจากข้อมูลถึง ref เสมอ ล็อกช้าจึงไม่รั่วข้อมูล แต่ถ้าช้าเกิน ON_TIME_LAG_H จะติด late=1 (จุดแตะที่แนะนำอาจพ้นเวลาไปแล้ว)"""
    out = []
    for _, r in outlook[outlook.hours == 24].sort_values("ref").iterrows():
        if r["ref"] < SHADOW_START:
            continue
        key = (r["ref"].strftime("%Y-%m-%d %H:%M"), str(r["fuel"]))
        if key in existing:
            continue
        existing.add(key)
        pred = pred_of(r["tier"])
        row = {c: "" for c in COLUMNS}
        row.update({"ref": key[0], "day": (r["ref"].normalize() + pd.Timedelta(days=1)).strftime("%Y-%m-%d"), "fuel": key[1],
                    "tier": r["tier"], "pred": pred, "model_version": r.get("model_version", ""), "logged_at": now.strftime("%Y-%m-%d %H:%M"),
                    "lag_h": float(r["lag_h"]), "late": int(float(r["lag_h"]) > ON_TIME_LAG_H),
                    **{f"rec_{k}": m[pred] for k, m in POLICY_MAPS.items()}})
        out.append(row)
    return out


def fill_outcome(row: dict, prices: pd.DataFrame, now: pd.Timestamp):
    """regret ที่เกิดจริงของแต่ละนโยบายและของเวลาคงที่ เฉลี่ยข้ามคลัง ; None ถ้ายังไม่ครบกำหนด/ราคายังไม่พอ"""
    day = pd.Timestamp(row["day"])
    if now < day + pd.Timedelta(hours=FILL_AFTER_H) or prices.effective.max() < day + pd.Timedelta(hours=21):
        return None
    regs = [w for depot in DEPOTS if (w := window_regrets(depot_series(prices, depot, row["fuel"]), day)) is not None]
    if len(regs) < MIN_DEPOTS:
        return None
    mean = lambda k: float(np.mean([w[k] for w in regs]))
    delta = mean("delta")
    out = {"n_depots": len(regs), "delta": round(delta, 3),
           **{f"regret_{k}": round(mean(row[f"rec_{k}"]), 3) for k in POLICY_MAPS},
           **{col: round(mean(tap), 3) for tap, col in FIXED_COLS.items()},
           "actual_cls": "ขึ้น" if delta >= FLAT else "ลง" if delta <= -FLAT else "ทรง", "filled_at": now.strftime("%Y-%m-%d %H:%M")}
    return out


def _group_lines(d: pd.DataFrame, label: str, with_ci: bool) -> list:
    num = ["regret_A", "regret_B", "regret_C", *FIXED_COLS.values()]
    d = d.copy()
    d[num] = d[num].apply(pd.to_numeric, errors="coerce")
    days = d.day.map(lambda v: str(_ts(v).date()))
    best = min(FIXED_COLS.values(), key=lambda c: d[c].mean())
    out = [f"[TAPREC] {label}: เติมผลแล้ว {len(d)} แถว ({days.nunique()} วัน) ; regret เฉลี่ย : "
           + " | ".join(f"{k} {d[c].mean():.3f}" for k, c in (("A", "regret_A"), ("B", "regret_B"), ("C", "regret_C"))) + " | "
           + " ".join(f"คงที่{t} {d[c].mean():.3f}" for t, c in FIXED_COLS.items())]
    if not with_ci:
        return out
    for k in POLICY_MAPS:
        diff = (d[f"regret_{k}"] - d[best]).groupby(days).mean().to_numpy()
        if len(diff) >= 8:
            m, lo, hi = diff_ci(diff)
            out.append(f"   นโยบาย {k} เทียบ {best}: {m:+.3f} (95% {lo:+.3f}..{hi:+.3f}, {len(diff)} วัน)")
        else:
            out.append(f"   นโยบาย {k} เทียบ {best}: {float(diff.mean()):+.3f} ({len(diff)} วัน — ยังไม่พอคำนวณช่วงเชื่อมั่น ต้อง ≥ 8)")
    return out


def shadow_summary(rows: list) -> list:
    """สรุปผลสะสมของแถวที่เติมผลแล้ว แยก "ตรงเวลา" (ผลหลัก พร้อมช่วงเชื่อมั่นเมื่อ ≥ 8 วัน) กับ "ล็อกช้า" (ไม่ทันใช้จริง แสดงแยกเพื่อเทียบ ไม่ปนผลหลัก)"""
    d = pd.DataFrame([r for r in rows if str(r.get("filled_at", "")) != ""])
    if d.empty:
        return ["[TAPREC] ยังไม่มีแถวที่เติมผลแล้ว"]
    late = pd.to_numeric(d.get("late", 0), errors="coerce").fillna(0) if "late" in d.columns else pd.Series(0, index=d.index)
    on_time, late_d = d[late == 0], d[late == 1]
    out = _group_lines(on_time, "ตรงเวลา", True) if len(on_time) else ["[TAPREC] ตรงเวลา: ยังไม่มีแถวที่เติมผลแล้ว"]
    if len(late_d):
        out += _group_lines(late_d, "ล็อกช้า (ไม่ทันใช้จริง)", False)
    return out


def main():
    now = pd.Timestamp(datetime.now(ZoneInfo(TZ_NAME)).replace(tzinfo=None))
    tok = get_token()
    outlook = load_outlook(sheets_get(tok, SHEET_ID, f"{TABS['OUTLOOK_LOG']}!A1:R50000"), MAX_LAG_H)
    tab = TABS["TAPREC_LOG"]
    if ensure_tab(tok, SHEET_ID, tab):
        sheets_update(tok, SHEET_ID, f"{tab}!A1", [COLUMNS])
        print(f"[TAPREC] created tab {tab}")
    values = sheets_get(tok, SHEET_ID, f"{tab}!A1:{LAST_COL}50000")
    if values and [str(h) for h in values[0]] != COLUMNS and COLUMNS[:len(values[0])] == [str(h) for h in values[0]]:
        sheets_update(tok, SHEET_ID, f"{tab}!A1", [COLUMNS])          # หัวตารางเก่า (ไม่มี lag_h/late) → ต่อท้ายให้ครบ
        values[0] = list(COLUMNS)
        print(f"[TAPREC] header upgraded ({tab})")
    head = values[0] if values else COLUMNS
    rows = [dict(zip(head, r + [""] * (len(head) - len(r)))) for r in values[1:]]
    existing = {(_ts(r["ref"]).strftime("%Y-%m-%d %H:%M"), str(r["fuel"])) for r in rows if _ts(r["ref"]) is not None}
    add = new_rows(existing, outlook, now)
    if add:
        sheets_append(tok, SHEET_ID, f"{tab}!A1:{LAST_COL}1", [[r[c] for c in COLUMNS] for r in add])
        rows += add
    print(f"[TAPREC] new rows: +{len(add)}")

    def due(r):
        day = _ts(r["day"])
        return str(r.get("filled_at", "")) == "" and day is not None and now >= day + pd.Timedelta(hours=FILL_AFTER_H)
    pending = [(i, r) for i, r in enumerate(rows) if due(r)]
    filled = 0
    if pending:
        prices = build_frame(sheets_get(tok, OILBOT_SHEET_ID, "PriceLog!A1:Q200000")).reset_index(drop=True)
        for i, r in pending:
            r = {**r, "day": _ts(r["day"]).strftime("%Y-%m-%d")}
            res = fill_outcome(r, prices, now)
            if res:
                rows[i].update(res)
                sheets_update(tok, SHEET_ID, f"{tab}!{chr(65 + OUTCOME_FROM)}{i + 2}:{chr(65 + OUTCOME_TO)}{i + 2}", [[res.get(c, rows[i].get(c, "")) for c in COLUMNS[OUTCOME_FROM:OUTCOME_TO + 1]]])
                filled += 1
    print(f"[TAPREC] outcomes filled: {filled} (รอผลอีก {len(pending) - filled})")
    for line in shadow_summary(rows):
        print(line)


if __name__ == "__main__":
    main()
