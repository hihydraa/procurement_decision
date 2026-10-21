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


def _evidence(summary, key: str, action: str):
    """สถิติย้อนหลังของคำแนะนำชนิดนี้ในช่วงทดสอบที่ไม่เคยใช้จูนเกณฑ์"""
    try:
        a = summary["results"][key]["holdout_policy"]["actions"][action]
        n = summary["results"][key]["holdout_policy"]["n_days"]
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
