"""
เอนจินตัดสินใจซื้อ (แทน rule เดิมใน main.py) — ใช้โมเดลที่ฝึกใน fit_model.py กับสัญญาณที่รู้ ณ เวลานี้

หลักการ:
  * ไม่เดา: ถ้าฟีดสัญญาณหาย/เก่า หรือไม่มีพารามิเตอร์โมเดล ให้ตอบ "ข้อมูลไม่พอ" ไม่ใช้ค่าค้างมาตัดสินใจ
  * ทุกคำแนะนำมีตัวเลขหลักฐาน: ความน่าจะเป็นที่โมเดลให้ + สถิติย้อนหลังของคำแนะนำชนิดเดียวกัน
  * เหตุผลมาจากสัญญาณที่ขับ logit จริง (ไม่ใช่ข้อความ template)
"""
import json
import os
from datetime import datetime

import numpy as np
import pandas as pd

import model as M
import signals as S

HERE = os.path.dirname(os.path.abspath(__file__))
FUEL_KEY = {"GASOHOL95": "G95", "DIESEL": "B7"}
FUEL_TH = {"G95": "แก๊สโซฮอล์ 95", "B7": "ดีเซล"}
FEED_MAX_AGE_H = 72          # ฟีดสัญญาณทุกชนิดไม่มีแถวใหม่เกินนี้ = ฟีดน่าจะหยุด (ครอบคลุมวันหยุดยาวปกติ)
MIN_LIVE_OUTCOMES = 20       # สถิติสดจาก DecisionLog ต้องมีผลลัพธ์อย่างน้อยเท่านี้ถึงจะนำมาแสดงแทนสถิติ backtest


def phase_of(asof: pd.Timestamp) -> str:
    """หลัง ~19:00 ถือว่ารู้ MOPS ปิดและประกาศกองทุนแล้ว (EVE) ก่อนหน้านั้นใช้โมเดลช่วงบ่าย (PM)"""
    return "EVE" if asof.hour + asof.minute / 60 >= 19.0 else "PM"


def load_backtest_summary(path: str = None):
    path = path or os.path.join(HERE, "backtest_summary.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def feed_status(tables: S.Tables, asof: pd.Timestamp) -> dict:
    """ฟีดแต่ละชนิดล่าสุดเมื่อไร และโดยรวมยังเดินอยู่ไหม"""
    last = {}
    for name, df in (("trend", tables.intraday), ("headsup", tables.headsup), ("mops_close", tables.mops_close), ("fund", tables.fund)):
        last[name] = df.ts.max() if not df.empty else None
    newest = max((v for v in last.values() if v is not None), default=None)
    age_h = (asof - newest).total_seconds() / 3600 if newest is not None else None
    return {"last": last, "newest": newest, "age_h": age_h, "alive": age_h is not None and age_h <= FEED_MAX_AGE_H}


def _reason_lines(fuel: str, f: dict, model_rise: dict, model_fall: dict) -> list:
    """เหตุผลที่แปลงจากฟีเจอร์ที่ active จริง เรียงตามแรงที่ดัน logit (ขึ้น/ลง)"""
    x = M.vectorize(f)
    mu, sd = np.array(model_rise["mu"]), np.array(model_rise["sd"])
    z = (x - mu) / sd
    c_rise = np.array(model_rise["w"]) * z
    c_fall = np.array(model_fall["w"]) * ((x - np.array(model_fall["mu"])) / np.array(model_fall["sd"]))
    name = FUEL_TH[fuel]
    desc = {
        "trend": lambda: f"Trend Mops {name} ระหว่างวัน {f['trend']:+.2f} USD/bbl",
        "up_n": lambda: f"{f['up_n']} ราย ประกาศ 'มีปรับขึ้น' วันนี้",
        "dn_n": lambda: f"{f['dn_n']} ราย ประกาศ 'มีปรับลง' วันนี้",
        "close_last": lambda: f"MOPS ปิดล่าสุด {f['close_last']:+.2f} USD/bbl",
        "close_sign": lambda: f"ทิศทาง MOPS ปิดล่าสุด {'ขึ้น' if f['close_last'] > 0 else 'ลง'}",
        "close_3": lambda: f"MOPS 3 ครั้งล่าสุดรวม {f['close_3']:+.2f} USD/bbl",
        "fund_next": lambda: f"กองทุนประกาศปรับ {f['fund_next']:+.2f} บาท/ลิตร มีผลพรุ่งนี้",
    }
    active = {
        "trend": f.get("trend") is not None and not np.isnan(f.get("trend", np.nan)),
        "up_n": f.get("up_n", 0) > 0, "dn_n": f.get("dn_n", 0) > 0,
        "close_last": f.get("close_last") is not None and not np.isnan(f.get("close_last", np.nan)) and abs(f.get("close_last", 0)) >= 0.3,
        "close_sign": False,
        "close_3": f.get("close_3") is not None and not np.isnan(f.get("close_3", np.nan)) and abs(f.get("close_3", 0)) >= 1.0,
        "fund_next": abs(f.get("fund_next", 0.0)) >= 0.05,
    }
    items = []
    for i, feat in enumerate(M.FEATURES):
        if feat not in desc or not active.get(feat):
            continue
        push = c_rise[i] - c_fall[i]            # บวก = ดันไปทางราคาขึ้น , ลบ = ดันไปทางราคาลง
        if abs(push) < 0.05:
            continue
        # แสดงเฉพาะเหตุผลที่ทิศทางสมเหตุสมผลต่อผู้อ่าน (ประกาศขึ้น/MOPS ขึ้น ต้องดันไปทางขึ้น ฯลฯ) — ตัวที่สวนสามัญสำนึก
        # (เช่น "มีปรับลง → เสี่ยงขึ้น" ซึ่งเป็นผลของวันที่ราคาผันผวนทั้งสองทาง) ยังรวมอยู่ในความน่าจะเป็นแต่ไม่นำมาอธิบาย
        expected = {"trend": f.get("trend"), "close_last": f.get("close_last"), "close_3": f.get("close_3"), "fund_next": f.get("fund_next")}
        if feat == "up_n" and push < 0:
            continue
        if feat == "dn_n" and push > 0:
            continue
        if feat in expected and expected[feat] is not None and not np.isnan(expected[feat]) and expected[feat] != 0 and (expected[feat] > 0) != (push > 0):
            continue
        items.append((abs(push), f"{desc[feat]()} → {'เสี่ยงราคาขึ้น' if push > 0 else 'โน้มราคาลง'}"))
    items.sort(reverse=True)
    return [t for _, t in items[:4]]


def _evidence(summary, key: str, action: str, group: str = "results"):
    """สถิติย้อนหลังของคำแนะนำชนิดนี้ในช่วงทดสอบที่ไม่เคยใช้จูนเกณฑ์"""
    try:
        a = summary[group][key]["holdout_policy"]["actions"][action]
        n = summary[group][key]["holdout_policy"]["n_days"]
        return {"share": a["share"], "rise_rate": a["rise_rate"], "fall_rate": a["fall_rate"],
                "avg_move": a["avg_next_minus_today"], "n_days": n, "source": "backtest"}
    except (KeyError, TypeError):
        return None


def live_stats(decision_log: pd.DataFrame, fuel: str, phase: str, action: str, days: int = 90):
    """สถิติสดจาก DecisionLog (คำแนะนำจริงที่เคยออก + ผลที่เกิดขึ้น) — ใช้ได้เมื่อมีผลลัพธ์พอ"""
    if decision_log is None or decision_log.empty or "outcome_delta" not in decision_log.columns:
        return None
    d = decision_log.copy()
    d["outcome_delta"] = pd.to_numeric(d["outcome_delta"], errors="coerce")
    d = d[(d.fuel == fuel) & (d.phase == phase) & (d.action_code == action)].dropna(subset=["outcome_delta"])
    d["asof"] = pd.to_datetime(d["asof"], errors="coerce")
    d["_day"] = d["asof"].dt.strftime("%Y-%m-%d")
    d = d.sort_values("asof").drop_duplicates(["_day", "fuel", "phase"], keep="last")   # 1 วัน/ชนิด/ช่วง ต่อ 1 ผลลัพธ์ (รอบหลังทับรอบก่อน)
    # ต้องใช้ d["asof"] ไม่ใช่ d.asof — DataFrame.asof เป็นเมธอดของ pandas ไม่ใช่คอลัมน์
    d = d[d["asof"] >= d["asof"].max() - pd.Timedelta(days=days)] if len(d) else d
    if len(d) < MIN_LIVE_OUTCOMES:
        return None
    return {"n_days": int(len(d)), "rise_rate": float((d.outcome_delta > M.RISE_THB).mean()),
            "fall_rate": float((d.outcome_delta < -M.RISE_THB).mean()), "avg_move": float(d.outcome_delta.mean()), "source": "live"}


def decide(fuel_key: str, asof: pd.Timestamp, tables: S.Tables, params, summary=None, decision_log=None) -> dict:
    """คืน dict พร้อมใช้ในหน้าเว็บ: action, color, summary, reasons, p_rise, p_fall, features, evidence, quality"""
    fuel = FUEL_KEY[fuel_key]
    phase = phase_of(asof)
    key = f"{fuel}_{phase}"
    st = feed_status(tables, asof)
    out = {"fuel": fuel, "phase": phase, "asof": asof, "feed": st, "p_rise": None, "p_fall": None, "action_code": "NO_DATA",
           "features": None, "evidence": None, "reasons": [], "model_key": key}

    def no_data(msg):
        label, color = M.ACTIONS["NO_DATA"]
        out.update({"action": label, "color": color, "summary": msg})
        return out

    if params is None or key not in params.get("models", {}):
        return no_data("ยังไม่มีพารามิเตอร์โมเดล (model_params.json) — ยังไม่ให้คำแนะนำ")
    if not st["alive"]:
        newest = st["newest"].strftime("%d/%m %H:%M") if st["newest"] is not None else "ไม่เคยมีข้อมูล"
        return no_data(f"ฟีดสัญญาณจากไลน์ไม่อัปเดต (ล่าสุด {newest}) — ตรวจ worker/แท็บ Entry_* ก่อนใช้คำแนะนำ")

    f = S.build_features(asof, tables, fuel)
    mm = params["models"][key]
    x = M.vectorize(f)
    p_rise = float(M.predict_proba(mm["rise"], x)[0])
    p_fall = float(M.predict_proba(mm["fall"], x)[0])
    th = params.get("thresholds", {}).get(key, M.DEFAULT_THRESHOLDS)
    code = M.choose_action(p_rise, p_fall, th)
    label, color = M.ACTIONS[code]

    base = None
    if summary and key in summary.get("results", {}):
        base = summary["results"][key]
    base_rise = base["base_rise"] if base else None
    lead = {
        "URGENT": f"โอกาสราคาขึ้นภายในคืนนี้ {p_rise:.0%}" + (f" (ปกติ {base_rise:.0%})" if base_rise else "") + " — ควรสั่งซื้อวันนี้",
        "GRADUAL": f"โอกาสราคาขึ้น {p_rise:.0%} สูงกว่าปกติเล็กน้อย — ทยอยสั่งซื้อวันนี้บางส่วน",
        "NORMAL": f"ไม่มีสัญญาณชัดเจน (ขึ้น {p_rise:.0%} / ลง {p_fall:.0%}) — ซื้อตามแผนปกติ",
        "WAIT": f"โอกาสราคาลง {p_fall:.0%} และโอกาสขึ้นต่ำ {p_rise:.0%} — ชะลอการซื้อได้",
    }[code]

    reasons = _reason_lines(fuel, f, mm["rise"], mm["fall"])
    if f["trend"] is None or (isinstance(f["trend"], float) and np.isnan(f["trend"])):
        reasons.append("ยังไม่มี Trend Mops ของวันนี้ (ปกติโพสต์ ~11:30 และ ~15:00) — ความมั่นใจต่ำกว่าปกติ")
    if not reasons:
        reasons.append("ไม่มีสัญญาณขึ้น/ลงที่เด่นพอในตอนนี้")

    ev = live_stats(decision_log, fuel, phase, code) or _evidence(summary, key, code)
    out.update({"action_code": code, "action": label, "color": color, "summary": lead, "reasons": reasons,
                "p_rise": p_rise, "p_fall": p_fall, "features": f, "evidence": ev, "thresholds": th})
    return out


# ------------------------------------------------------------------ แผนส่งรถ (ระยะเดินทางสาขา -> คลัง)
# ราคาที่ได้จริงคือราคา ณ เวลารถถึงคลัง (เวลาออก + lead) ; โมเดลตอบว่า "ถ้ารออีก 1 วัน ราคา ณ เวลาถึงจะแพงขึ้น/ถูกลงไหม"
DISPATCH_FIRST_HOUR = 6.0          # ก่อนเวลานี้ยังไม่มีสัญญาณของวัน (ประกาศล่วงหน้านับตั้งแต่ 06:00) → ยังไม่ประเมิน
GOOD_AUC, OK_AUC = 0.62, 0.55


def dispatch_window(asof: pd.Timestamp, windows: dict):
    """ช่วงเวลาตัดสินใจล่าสุดที่ผ่านมาแล้ว (เช่น 13:40 → D12) ; ก่อนช่วงแรกคืน None"""
    h = asof.hour + asof.minute / 60
    passed = [(hour, name) for name, hour in windows.items() if hour <= h]
    return max(passed)[1] if passed else None


def dispatch_quality(summary, key: str) -> dict:
    """เกรดความเชื่อมั่นของโมเดลช่วงนี้ จากผลทดสอบย้อนหลังที่ไม่เคยใช้จูน: ดี / พอใช้ / อ่อน (อ่อน = ไม่ใช้แนะนำเร่ง/ชะลอ)"""
    try:
        r = summary["dispatch_results"][key]
        ar, af = r["auc_rise"], r["auc_fall"]
        gain = r["holdout_policy"]["total_gain_thb_per_litre"]
    except (KeyError, TypeError):
        return {"grade": "อ่อน", "auc_rise": None, "auc_fall": None, "gain": None, "n_days": None}
    if min(ar, af) >= GOOD_AUC and gain > 0:
        grade = "ดี"
    elif min(ar, af) >= OK_AUC and gain > 0:
        grade = "พอใช้"
    else:
        grade = "อ่อน"
    return {"grade": grade, "auc_rise": ar, "auc_fall": af, "gain": gain, "n_days": r["holdout_policy"]["n_days"]}


def _next_check(asof: pd.Timestamp, windows: dict) -> str:
    h = asof.hour + asof.minute / 60
    later = sorted(hour for hour in windows.values() if hour > h)
    if later:
        t = later[0]
        return f"ประเมินรอบถัดไปหลัง {int(t):02d}:{int(round((t % 1) * 60)):02d}"
    return "ประเมินรอบถัดไปพรุ่งนี้หลัง 06:00"


def decide_dispatch(fuel_key: str, asof: pd.Timestamp, tables: S.Tables, params, summary=None, lead_hours: int = None) -> dict:
    """คำแนะนำ "ส่งรถตอนนี้หรือรอ" สำหรับพนักงาน — ถ้าส่งรถตอนนี้จะถึงคลังที่ asof + lead_hours"""
    fuel = FUEL_KEY[fuel_key]
    disp = (params or {}).get("dispatch")
    lead = lead_hours or (disp or {}).get("default_lead_hours", 10)
    arrive = asof + pd.Timedelta(hours=lead)
    out = {"fuel": fuel, "lead": lead, "asof": asof, "arrive": arrive, "p_rise": None, "p_fall": None, "action_code": "NO_DATA",
           "reasons": [], "quality": None, "evidence": None, "features": None, "window": None,
           "next_check": None, "arrival_note": None}

    def no_data(msg):
        label, color = M.ACTIONS["NO_DATA"]
        out.update({"action": label, "color": color, "headline": msg, "detail": []})
        return out

    if not disp:
        return no_data("ยังไม่มีโมเดลแผนส่งรถ (ต้องฝึกด้วย fit_model.py --dispatch-only)")
    windows = disp["windows"]
    win = dispatch_window(asof, windows)
    out["next_check"] = _next_check(asof, windows)
    if win is None:
        return no_data(f"ยังไม่ถึงเวลาประเมิน (เริ่มประเมินตั้งแต่ {int(DISPATCH_FIRST_HOUR):02d}:00 เมื่อมีประกาศล่วงหน้าของวัน)")
    key = f"{fuel}_{win}_L{lead}"
    out["window"] = win
    if key not in params.get("models", {}):
        return no_data(f"ยังไม่มีโมเดลสำหรับระยะเดินทาง {lead} ชม.")
    st = feed_status(tables, asof)
    if not st["alive"]:
        newest = st["newest"].strftime("%d/%m %H:%M") if st["newest"] is not None else "ไม่เคยมีข้อมูล"
        return no_data(f"ฟีดสัญญาณจากไลน์ไม่อัปเดต (ล่าสุด {newest}) — ยังไม่ให้คำแนะนำ")

    f = S.build_features(asof, tables, fuel)
    mm = params["models"][key]
    x = M.vectorize(f)
    p_rise = float(M.predict_proba(mm["rise"], x)[0])
    p_fall = float(M.predict_proba(mm["fall"], x)[0])
    th = params.get("thresholds", {}).get(key, M.DEFAULT_THRESHOLDS)
    code = M.choose_action(p_rise, p_fall, th)
    q = dispatch_quality(summary, key)
    base = (summary or {}).get("dispatch_results", {}).get(key) or {}
    base_rise = base.get("base_rise")
    raw_code = code
    if q["grade"] == "อ่อน":
        code = "NORMAL"                                   # โมเดลช่วงนี้ยังไม่แม่นพอ ไม่แนะนำเร่ง/ชะลอ
    label, color = M.ACTIONS[code]
    at = arrive.strftime("%H:%M")
    day = "" if arrive.date() == asof.date() else " (วันถัดไป)"
    wait_cost = f"{p_rise:.0%}" + (f" (ปกติ {base_rise:.0%})" if base_rise else "")
    headline = {
        "URGENT": f"ส่งรถออกเลยตอนนี้ → ถึงคลังประมาณ {at}{day}",
        "GRADUAL": f"ส่งรถออกบางส่วนตอนนี้ (ประมาณครึ่งหนึ่งของแผน) → ถึงคลังประมาณ {at}{day}",
        "NORMAL": f"ส่งรถตามแผนปกติ → ถึงคลังประมาณ {at}{day}",
        "WAIT": f"ชะลอการส่งรถได้ → ถ้าออกตอนนี้จะถึงคลังประมาณ {at}{day}",
    }[code]
    detail = {
        "URGENT": [f"ถ้ารอไปอีก 1 วัน มีโอกาส {wait_cost} ที่ราคา ณ เวลาถึงคลังจะแพงขึ้นเกิน {M.RISE_THB:.2f} บาท/ลิตร"],
        "GRADUAL": [f"โอกาสที่ราคา ณ เวลาถึงจะแพงขึ้นถ้ารอ 1 วัน {wait_cost}"
                    + (" — สูงกว่าปกติเล็กน้อย ทยอยออกรถดีกว่ารอทั้งหมด" if not base_rise or p_rise > base_rise
                       else " — ไม่สูงกว่าปกติ แต่ไม่มีสัญญาณให้ชะลอ จึงทยอยออกรถเพื่อลดความเสี่ยง")],
        "NORMAL": ["ไม่มีสัญญาณชัดเจนให้เร่งหรือชะลอ"] if raw_code == "NORMAL" else
                  [f"ช่วงเวลานี้โมเดลยังไม่แม่นพอที่จะแนะนำให้เร่งหรือชะลอ (ตัวเลขดิบ: แพงขึ้น {p_rise:.0%} / ถูกลง {p_fall:.0%})"],
        "WAIT": [f"โอกาส {p_fall:.0%} ที่ราคา ณ เวลาถึงคลังจะถูกลงเกิน {M.RISE_THB:.2f} บาท/ลิตร ถ้ารอไปอีก 1 วัน และโอกาสแพงขึ้นต่ำ ({p_rise:.0%})"],
    }[code]

    prof = (disp.get("hour_profile") or {}).get(fuel, {})
    v = prof.get(str(arrive.hour))
    if v is not None and abs(v) >= 0.03:
        out["arrival_note"] = (f"ถึงคลังช่วง {arrive.hour:02d}:00 ราคาตลาดเฉลี่ยย้อนหลัง{'สูง' if v > 0 else 'ต่ำ'}กว่าช่วง 15:30 "
                               f"ประมาณ {abs(v):.2f} บาท/ลิตร")
    reasons = _reason_lines(fuel, f, mm["rise"], mm["fall"])
    if f["trend"] is None or (isinstance(f["trend"], float) and np.isnan(f["trend"])):
        reasons.append("ยังไม่มี Trend Mops ของวันนี้ — ความมั่นใจต่ำกว่าปกติ")
    if not reasons:
        reasons.append("ไม่มีสัญญาณขึ้น/ลงที่เด่นพอในตอนนี้")
    out.update({"action_code": code, "action": label, "color": color, "headline": headline, "detail": detail, "reasons": reasons,
                "p_rise": p_rise, "p_fall": p_fall, "base_rise": base_rise, "base_fall": base.get("base_fall"), "features": f,
                "quality": q, "evidence": _evidence(summary, key, code, group="dispatch_results"), "thresholds": th, "raw_action_code": raw_code})
    return out


MIN_DISPATCH_LIVE = 20     # จำนวนวัน (ที่มีผลลัพธ์แล้ว) ขั้นต่ำต่อชนิด/ระยะเดินทาง ก่อนสรุปว่าแม่นหรือไม่


def dispatch_live_stats(log: pd.DataFrame, fuel: str, lead: int, window: str = None):
    """ผลทดสอบเดินหน้าของแผนส่งรถจาก DispatchLog — นับ 1 รอบต่อ (วัน, ช่วง) คือรอบแรกที่ออกคำแนะนำในช่วงนั้น
    คืน {n, by_action:{code:{n,rise_rate,fall_rate,avg}}, total_gain, avg_gain_per_day, pending} หรือ None ถ้าไม่มีข้อมูลเลย
    gain ต่อแถว = M.utility(action, outcome_delta) ; ซื้อปกติ = 0 (เทียบกับ "ทำตามแผนเดิมทุกวัน")"""
    need = {"date", "window", "fuel", "lead", "action_code", "outcome_delta", "asof"}
    if log is None or log.empty or not need.issubset(log.columns):
        return None
    d = log.copy()
    d["lead"] = pd.to_numeric(d["lead"], errors="coerce")
    d = d[(d["fuel"] == fuel) & (d["lead"] == lead)]
    if window:
        d = d[d["window"] == window]
    if d.empty:
        return None
    d["asof"] = pd.to_datetime(d["asof"], errors="coerce")
    d = d.dropna(subset=["asof"]).sort_values("asof").drop_duplicates(["date", "window"], keep="first")
    d["outcome_delta"] = pd.to_numeric(d["outcome_delta"], errors="coerce")
    done, pending = d.dropna(subset=["outcome_delta"]), int(d["outcome_delta"].isna().sum())
    by_action, total = {}, 0.0
    for code, g in done.groupby("action_code"):
        by_action[code] = {"n": int(len(g)), "rise_rate": float((g.outcome_delta > M.RISE_THB).mean()),
                           "fall_rate": float((g.outcome_delta < -M.RISE_THB).mean()), "avg": float(g.outcome_delta.mean())}
    total = float(sum(M.utility(c, g) for c, g in zip(done.action_code, done.outcome_delta)))
    return {"n": int(len(done)), "pending": pending, "by_action": by_action, "total_gain": total,
            "avg_gain_per_day": total / len(done) if len(done) else None,
            "base_rise": float((done.outcome_delta > M.RISE_THB).mean()) if len(done) else None,
            "base_fall": float((done.outcome_delta < -M.RISE_THB).mean()) if len(done) else None}


# เกณฑ์ก่อนอนุญาตให้ "แจ้งเตือน" (ผู้ใช้อนุมัติแล้ว): ต้องผ่านทั้งหมดต่อ ชนิด/ช่วง/ระยะเดินทาง
ALERT_MIN_DAYS = 40            # วันที่มีผลจริงขั้นต่ำ
ALERT_MIN_LIFT = 0.15          # อัตราถูกจริงของ "เร่ง/ชะลอ" ต้องสูงกว่าอัตราปกติอย่างน้อยเท่านี้ (15 จุดเปอร์เซ็นต์)
ALERT_MIN_ACTION_N = 8         # ต้องมีคำแนะนำชนิดนั้นอย่างน้อยเท่านี้ครั้ง ถึงจะประเมินชนิดนั้นได้
ALERT_MIN_GAIN = 0.05          # กำไรเฉลี่ย บาท/ลิตร/วัน
ALERT_BACKTEST_KEEP = 2 / 3    # ผลจริงต้องไม่แย่กว่า backtest เกินหนึ่งในสาม


def alert_status(log: pd.DataFrame, summary, fuel: str, window: str, lead: int) -> dict:
    """ช่วง (fuel, window, lead) นี้ผ่านเกณฑ์แจ้งเตือนหรือยัง — คืน {eligible:{URGENT:bool,WAIT:bool}, n, reasons[]}
    ไม่ส่งข้อความใด ๆ เป็นแค่ตัวตัดสินว่า 'อนุญาตหรือยัง' ให้ขั้นแจ้งเตือนใช้ภายหลัง"""
    out = {"fuel": fuel, "window": window, "lead": lead, "n": 0, "eligible": {"URGENT": False, "WAIT": False}, "reasons": []}
    st = dispatch_live_stats(log, fuel, lead, window)
    if not st:
        out["reasons"].append("ยังไม่มีบันทึกคำแนะนำจริง")
        return out
    out["n"] = st["n"]
    if st["n"] < ALERT_MIN_DAYS:
        out["reasons"].append(f"มีผลจริง {st['n']}/{ALERT_MIN_DAYS} วัน")
        return out
    avg = st["avg_gain_per_day"]
    if avg is None or avg < ALERT_MIN_GAIN:
        out["reasons"].append(f"กำไรเฉลี่ยจริง {avg if avg is None else round(avg, 3)} ต่ำกว่า {ALERT_MIN_GAIN} บาท/ลิตร/วัน")
        return out
    try:
        bt = summary["dispatch_results"][f"{fuel}_{window}_L{lead}"]["holdout_policy"]
        bt_avg = bt["total_gain_thb_per_litre"] / bt["n_days"]
    except (KeyError, TypeError, ZeroDivisionError):
        bt_avg = None
    if bt_avg is not None and bt_avg > 0 and avg < bt_avg * ALERT_BACKTEST_KEEP:
        out["reasons"].append(f"กำไรจริง {avg:.3f} แย่กว่า backtest ({bt_avg:.3f}) เกินหนึ่งในสาม")
        return out
    for code, rate_key, base_key, label in (("URGENT", "rise_rate", "base_rise", "เร่ง"), ("WAIT", "fall_rate", "base_fall", "ชะลอ")):
        a = st["by_action"].get(code)
        if not a or a["n"] < ALERT_MIN_ACTION_N:
            out["reasons"].append(f"คำแนะนำ{label}มีน้อยกว่า {ALERT_MIN_ACTION_N} ครั้ง ยังประเมินไม่ได้")
            continue
        lift = a[rate_key] - st[base_key]
        if lift >= ALERT_MIN_LIFT:
            out["eligible"][code] = True
        else:
            out["reasons"].append(f"{label}: ถูกจริง {a[rate_key]:.0%} สูงกว่าปกติ {st[base_key]:.0%} แค่ {lift * 100:.0f} จุด (ต้อง ≥ {ALERT_MIN_LIFT * 100:.0f})")
    return out


# ------------------------------------------------------------------ แนวโน้มราคา 24/48 ชม. (ข้อความกลางสำหรับทุกสาขา)
OUTLOOK_HEADLINE = {
    "UP_STRONG": "ราคามีแนวโน้มขึ้นแรง",
    "UP": "ราคามีแนวโน้มขึ้น",
    "DOWN": "ราคามีแนวโน้มลง",
    "DOWN_STRONG": "ราคามีแนวโน้มลงแรง",
    "UNCLEAR": "ยังไม่ชัดเจน — ระบบไม่ฟันธงทิศทาง",
}
OUTLOOK_SIZE = {"UP_STRONG": "มักขยับตั้งแต่ 40 สต./ลิตรขึ้นไป", "UP": "มักขยับราว 10–40 สต./ลิตร",
                "DOWN": "มักขยับราว 10–40 สต./ลิตร", "DOWN_STRONG": "มักขยับตั้งแต่ 40 สต./ลิตรขึ้นไป"}


def outlook_ref(asof: pd.Timestamp) -> pd.Timestamp:
    """รอบประเมิน = 20:30 ล่าสุดที่ผ่านมาแล้ว (ก่อน 20:30 ใช้ของเมื่อคืน ซึ่งยังครอบคลุมช่วงเวลาที่เหลือของ 24 ชม.)"""
    ref = asof.normalize() + pd.Timedelta(hours=M.OUTLOOK_REF_HOUR)
    return ref if asof >= ref else ref - pd.Timedelta(days=1)


def _outlook_evidence(summary, key, tier):
    st = ((summary or {}).get("outlook_results", {}).get(key) or {}).get("tiers", {}).get(tier) or {}
    if not st.get("n"):
        return None
    if tier == "UNCLEAR":
        return {"n": st["n"], "share": st["share"], "grade": "ต่ำ"}
    return {"n": st["n"], "share": st["share"], "right10": st["right10"], "right40": st["right40"], "wrong10": st["wrong10"],
            "avg_move": st["avg_move"], "grade": st.get("grade", "ต่ำ")}


def decide_outlook(fuel_key: str, asof: pd.Timestamp, tables: S.Tables, params, summary=None) -> dict:
    """แนวโน้มราคาตลาดข้างหน้า 24/48 ชม. จากรอบ 20:30 — ไม่ระบุเวลาออกรถ ให้แต่ละสาขาจัดสรรเอง"""
    fuel = FUEL_KEY[fuel_key]
    ref = outlook_ref(asof)
    out = {"fuel": fuel, "asof": asof, "ref": ref, "horizons": [], "note": None}
    cfg = (params or {}).get("outlook")
    if not cfg:
        out["note"] = "ยังไม่มีโมเดลแนวโน้ม (ต้องฝึกด้วย fit_model.py --outlook-only)"
        return out
    st = feed_status(tables, asof)
    if not st["alive"]:
        newest = st["newest"].strftime("%d/%m %H:%M") if st["newest"] is not None else "ไม่เคยมีข้อมูล"
        out["note"] = f"ฟีดสัญญาณจากไลน์ไม่อัปเดต (ล่าสุด {newest}) — ยังไม่ให้แนวโน้ม"
        return out
    f = S.build_features(ref, tables, fuel)
    x = M.vectorize(f)
    out["features"] = f
    for hz in cfg["horizons"]:
        key = f"{fuel}_H{hz}"
        mm = cfg["models"].get(key)
        if not mm:
            continue
        p = {k: float(M.predict_proba(mm[k], x)[0]) for k in ("r10", "f10", "r40", "f40")}
        tier = M.classify_outlook(p["r10"], p["f10"], p["r40"], p["f40"])
        ev = _outlook_evidence(summary, key, tier)
        until = ref + pd.Timedelta(hours=hz)
        out["horizons"].append({
            "hours": hz, "tier": tier, "label": M.OUTLOOK_TIERS[tier], "headline": f"{OUTLOOK_HEADLINE[tier]} ภายใน {hz} ชม.",
            "size": OUTLOOK_SIZE.get(tier), "until": until, "p": p, "grade": (ev or {}).get("grade", "ต่ำ"), "evidence": ev,
            "base_up": ((summary or {}).get("outlook_results", {}).get(key) or {}).get("base_up10"),
            "base_down": ((summary or {}).get("outlook_results", {}).get(key) or {}).get("base_down10")})
    return out


def outlook_evidence_text(h: dict) -> str:
    ev = h.get("evidence")
    if not ev:
        return ""
    if h["tier"] == "UNCLEAR":
        return f"ย้อนหลังพบสถานการณ์แบบนี้ {ev['share']:.0%} ของวัน (ไม่ทายทิศทาง)"
    return (f"ย้อนหลัง {ev['n']} ครั้งที่ระบบบอกแบบนี้: ถูกทิศ {ev['right10']:.0%} ผิดทิศ {ev['wrong10']:.0%} "
            f"ขยับเฉลี่ย {ev['avg_move']:.2f} บาท/ลิตร")


TIER_EMOJI = {"UP_STRONG": "📈", "UP": "📈", "UNCLEAR": "❓", "DOWN": "📉", "DOWN_STRONG": "📉"}   # เฉพาะทิศทาง ; "แรง" สื่อด้วยคำ ไม่เพิ่ม emoji
GRADE_EMOJI = {"สูง": "🟢", "กลาง": "🟡", "ต่ำ": "🔴"}


def outlook_text(per_fuel: dict, now: pd.Timestamp = None) -> str:
    """ข้อความสั้นสำหรับส่งเข้าไลน์ (มี emoji สื่อทิศทาง/ความเชื่อมั่น) per_fuel = {fuel_key: decide_outlook(...)}"""
    items = [d for d in per_fuel.values() if d.get("horizons")]
    if not items:
        return ""
    ref = items[0]["ref"]
    lines = ["พยากรณ์แนวโน้มราคาน้ำมัน", f"ประเมินจากข้อมูล {ref.strftime('%H:%M')} น. วันที่ {ref.strftime('%d/%m')}"]
    for d in items:
        lines += ["", f"{d['fuel']}"]
        for h in d["horizons"]:
            size = f" ({h['size']})" if h.get("size") else ""
            conf = "" if h["tier"] == "UNCLEAR" else f" {GRADE_EMOJI[h['grade']]} ความเชื่อมั่น: {h['grade']}"
            lines.append(f"{h['hours']} ชม.: {TIER_EMOJI[h['tier']]} {h['label']}{size}{conf}")
            ev = outlook_evidence_text(h)
            if ev and h["hours"] == 24:
                lines.append(f"   {ev}")
        if d.get("note"):
            lines.append(f"   {d['note']}")
    lines += ["", "⚠️ ใช้ประกอบการตัดสินใจ แต่ละสาขาจัดสรรเองตามระยะทาง — ยังอยู่ระหว่างทดสอบความแม่นจริง"]
    return "\n".join(lines)


def outlook_live_stats(log: pd.DataFrame, fuel: str, hours: int):
    """ผลทดสอบเดินหน้าของแนวโน้มจาก OutlookLog : {tier: {n, right10, wrong10, avg_move}} เฉพาะวันที่มีผลแล้ว ; None ถ้ายังไม่มี"""
    if log is None or log.empty or "outcome_delta" not in log.columns:
        return None
    d = log[(log["fuel"].astype(str) == fuel) & (pd.to_numeric(log["hours"], errors="coerce") == hours)].copy()
    if "asof" in d.columns and "ref" in d.columns:                    # นับเฉพาะแนวโน้มที่ออกภายใน 3 ชม. หลังรอบ 20:30 (แถวที่ออกช้ากว่านั้นไม่ใช่การทดสอบเดินหน้า)
        lag = pd.to_datetime(d["asof"], errors="coerce") - pd.to_datetime(d["ref"], errors="coerce")
        d = d[lag.isna() | (lag <= pd.Timedelta(hours=3))]
    d["outcome_delta"] = pd.to_numeric(d["outcome_delta"], errors="coerce")
    d = d.dropna(subset=["outcome_delta"]).drop_duplicates(subset=["date"], keep="first")
    if d.empty:
        return None
    out = {}
    for tier in M.OUTLOOK_TIERS:
        t = d[d["tier"] == tier]
        if t.empty:
            continue
        if tier == "UNCLEAR":
            out[tier] = {"n": int(len(t))}
            continue
        mv = t["outcome_delta"] * (1.0 if tier.startswith("UP") else -1.0)
        out[tier] = {"n": int(len(t)), "right10": float((mv >= M.OUTLOOK_SMALL).mean()), "wrong10": float((mv <= -M.OUTLOOK_SMALL).mean()),
                     "avg_move": float(mv.mean())}
    return {"n_days": int(len(d)), "tiers": out}
