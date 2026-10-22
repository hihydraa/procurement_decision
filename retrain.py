"""
ฝึกโมเดลซ้ำแบบมีประตูตรวจ (รันรายสัปดาห์จาก repo oil-bot ที่เป็น private) — ไม่เปลี่ยนโมเดลที่ใช้งานเองโดยไม่ผ่านประตู

ขั้นตอน:
  1) ชุดฐาน = ข้อมูลที่ export จากแชท (prices/trend/headsup/mops/fund .csv) + ข้อมูลที่ระบบเก็บเองในชีท (Entry_*) ที่ใหม่กว่าฐาน
     ข้อยกเว้น: Trend ที่อ่านจากรูป (kind=HALF_IMG) ยังไม่นำมาฝึก เพราะสเกลต่างจาก Trend ข้อความเดิมและมีข้อมูลน้อย (ดู trend_scale)
  2) ฝึกทุกโมเดลใหม่ด้วย walk-forward แบบเดียวกับ fit_model.py
  3) ประตูต่อโมเดล (key): AUC ขึ้น/ลง ไม่แย่กว่าเดิมเกิน AUC_TOL , กำไรเฉลี่ยต่อวันบนช่วงทดสอบไม่ต่ำกว่าเดิมเกิน GAIN_TOL ,
     และทิศทางสัมประสิทธิ์สมเหตุสมผล (sign_issues) — key ที่ไม่ผ่านคงโมเดลเดิมไว้
  4) เขียน model_params.json / backtest_summary.json ฉบับเสนอลง --out-dir ถ้ามี key ผ่านและมีข้อมูลใหม่ + รายงาน report.md / report.json

ใช้: python retrain.py --data-dir <โฟลเดอร์ csv ฐาน> --out-dir out   (รันในโฟลเดอร์ของ repo นี้ เพื่ออ่านโมเดลปัจจุบัน)
"""
import argparse
import copy
import json
import os
import urllib.parse
from datetime import datetime

import numpy as np
import pandas as pd

import engine
import fit_model as FM
import model as M
import signals as S
from common import SHEET_ID

MIN_NEW_DAYS = 14  # ข้อมูลใหม่น้อยกว่านี้ ผลต่างของโมเดลเป็นแค่สัญญาณรบกวน ไม่เปิด PR

TABLES = {"prices": "Entry_Prices", "trend": "Entry_Intraday", "headsup": "Entry_HeadsUp", "mops": "Entry_MopsClose", "fund": "Entry_FundNotice"}
FILES = {k: f"{k}.csv" for k in TABLES}
REQUIRED = {"prices": ["provider", "warehouse", "fuel", "price", "effective"], "trend": ["msg_date", "msg_time", "kind", "b", "d"],
            "headsup": ["msg_date", "msg_time", "provider", "direction", "scope"], "mops": ["msg_date", "msg_time", "g95_chg", "diesel_chg"],
            "fund": ["msg_date", "msg_time", "effective_date", "diesel_old", "diesel_new"]}
AUC_TOL = 0.02
GAIN_TOL = 0.02          # บาท/ลิตร/วัน
SIGN_TOL = 0.05
SCALE_RATIO_FLAG = 1.5


# ------------------------------------------------------------------ ข้อมูล
def _times(df: pd.DataFrame, kind: str) -> pd.Series:
    if kind == "prices":
        return pd.to_datetime(df["effective"], errors="coerce")
    return pd.to_datetime(df["msg_date"].astype(str) + " " + df["msg_time"].astype(str), errors="coerce")


def merge_increment(kind: str, base: pd.DataFrame, inc):
    """ต่อแถวใหม่จากชีทที่ 'ใหม่กว่า' ข้อมูลฐานเข้ากับฐาน คืน (ตารางรวม, จำนวนแถวที่เพิ่ม)"""
    if inc is None or len(inc) == 0:
        return base, 0
    inc = inc.copy()
    if kind == "trend" and "kind" in inc.columns:
        inc = inc[inc["kind"].astype(str) != "HALF_IMG"]              # นโยบาย: ยังไม่ฝึกด้วย Trend จากรูป
    if inc.empty:
        return base, 0
    cut = _times(base, kind).max()
    new = inc[_times(inc, kind) > cut]
    if new.empty:
        return base, 0
    cols = [c for c in base.columns if c in new.columns]
    return pd.concat([base, new[cols]], ignore_index=True), len(new)


def fetch_tab(name: str, kind: str = None) -> pd.DataFrame:
    """อ่านแท็บของชีทระบบแนะนำ (เปิดสาธารณะ) ; gviz ส่ง 'ชีทแรก' มาแทนเมื่อไม่มีแท็บ จึงตรวจคอลัมน์เสมอ"""
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(name)}"
    try:
        df = pd.read_csv(url)
    except Exception as e:                                           # noqa: BLE001
        print(f"[RETRAIN] อ่านแท็บ {name} ไม่ได้: {e}")
        return pd.DataFrame()
    df.columns = [str(c).strip() for c in df.columns]
    if kind and any(c not in df.columns for c in REQUIRED[kind]):
        print(f"[RETRAIN] แท็บ {name} คอลัมน์ไม่ครบ — ข้าม")
        return pd.DataFrame()
    return df


def trend_scale(base_trend: pd.DataFrame, sheet_trend) -> dict:
    """เทียบสเกลของ Trend จากรูป (HALF_IMG) กับ Trend ข้อความเดิม 1 ปีล่าสุด — ถ้าต่างกันมาก ห้ามเอามาผสมโดยไม่ตรวจ"""
    out = {"img_n": 0, "ratio_b": None, "ratio_d": None, "flag": False}
    if sheet_trend is None or len(sheet_trend) == 0 or "kind" not in sheet_trend.columns:
        return out
    img = sheet_trend[sheet_trend["kind"].astype(str) == "HALF_IMG"]
    out["img_n"] = int(len(img))
    t = _times(base_trend, "trend")
    text = base_trend[t >= t.max() - pd.Timedelta(days=365)]
    for col in ("b", "d"):
        a, b = pd.to_numeric(text[col], errors="coerce").std(), pd.to_numeric(img[col], errors="coerce").std()
        out[f"text_std_{col}"], out[f"img_std_{col}"] = (None if pd.isna(a) else float(a)), (None if pd.isna(b) else float(b))
        if out["img_n"] >= 10 and a and not pd.isna(b):
            out[f"ratio_{col}"] = float(b / a)
    ratios = [r for r in (out["ratio_b"], out["ratio_d"]) if r is not None]
    out["flag"] = any(r > SCALE_RATIO_FLAG or r < 1 / SCALE_RATIO_FLAG for r in ratios)
    return out


# ------------------------------------------------------------------ ประตูตรวจ
def _metrics(res: dict) -> dict:
    h = res.get("holdout_policy", {})
    n = h.get("n_days") or 0
    return {"auc_rise": res.get("auc_rise"), "auc_fall": res.get("auc_fall"),
            "gain_per_day": (h.get("total_gain_thb_per_litre", 0.0) / n) if n else None}


def sign_issues(models: dict, key: str) -> list:
    """ทิศทางสัมประสิทธิ์ที่ต้องสมเหตุสมผล (หน่วยมาตรฐาน): Trend ทุกช่วง ; ช่วงค่ำ (EVE/D20) เพิ่ม up_n และ MOPS ปิดล่าสุด
    ช่วงบ่ายและเช้าไม่ตรวจเพิ่ม เพราะสัญญาณเหล่านั้นอ่อนและสัมประสิทธิ์สลับทิศได้ตามธรรมชาติ"""
    idx = {f: i for i, f in enumerate(M.FEATURES)}
    r, f = models["rise"]["w"], models["fall"]["w"]
    issues = []
    if r[idx["trend"]] < -SIGN_TOL:
        issues.append("Trend ดันโอกาสราคาขึ้นสวนทาง")
    if f[idx["trend"]] > SIGN_TOL:
        issues.append("Trend ดันโอกาสราคาลงสวนทาง")
    if key.endswith("_EVE") or "_D20_" in key:
        if r[idx["up_n"]] < -SIGN_TOL:
            issues.append("จำนวนผู้ประกาศขึ้นดันโอกาสขึ้นสวนทาง")
        if r[idx["close_last"]] < -SIGN_TOL:
            issues.append("MOPS ปิดล่าสุดดันโอกาสขึ้นสวนทาง")
        if f[idx["close_last"]] > SIGN_TOL:
            issues.append("MOPS ปิดล่าสุดดันโอกาสลงสวนทาง")
    return issues


def gate(old_summary: dict, new_summary: dict, new_params: dict) -> dict:
    """คืน {'group:key': {ok, reasons, old, new}} — key ใหม่ที่ไม่เคยมีผ่านได้ถ้าทิศทางสัมประสิทธิ์ผ่าน"""
    out = {}
    for group in ("results", "dispatch_results"):
        for key, new in new_summary.get(group, {}).items():
            old = (old_summary or {}).get(group, {}).get(key)
            reasons = [f"สัมประสิทธิ์: {x}" for x in sign_issues(new_params["models"][key], key)]
            mn, mo = _metrics(new), (_metrics(old) if old else None)
            if mo:
                for k, tol, label in (("auc_rise", AUC_TOL, "AUC ขึ้น"), ("auc_fall", AUC_TOL, "AUC ลง"), ("gain_per_day", GAIN_TOL, "กำไรเฉลี่ยต่อวัน")):
                    a, b = mn[k], mo[k]
                    if a is None or pd.isna(a):
                        if b is not None and not pd.isna(b):
                            reasons.append(f"{label}: ใหม่คำนวณไม่ได้")
                    elif b is not None and not pd.isna(b) and a < b - tol:
                        reasons.append(f"{label} แย่ลง {b:.3f} → {a:.3f} (เกิน {tol})")
            out[f"{group}:{key}"] = {"ok": not reasons, "reasons": reasons, "old": mo, "new": mn}
    return out


def merge_models(old_params: dict, old_summary: dict, new_params: dict, new_summary: dict, accepted: list):
    """เอาเฉพาะ key ที่ผ่านประตูจากชุดใหม่ ที่เหลือคงของเดิม"""
    params, summary = copy.deepcopy(old_params), copy.deepcopy(old_summary)
    dispatch_changed = False
    for ident in accepted:
        group, key = ident.split(":", 1)
        params["models"][key] = new_params["models"][key]
        params["thresholds"][key] = new_params["thresholds"][key]
        summary.setdefault(group, {})[key] = new_summary[group][key]
        dispatch_changed = dispatch_changed or group == "dispatch_results"
    if accepted:
        params["trained_at"], params["data_through"] = new_params["trained_at"], new_params.get("data_through")
        summary["generated"] = new_summary["generated"]
        if dispatch_changed and "dispatch" in new_params:
            params["dispatch"] = {**params.get("dispatch", {}), "hour_profile": new_params["dispatch"]["hour_profile"],
                                  "trained_at": new_params["dispatch"]["trained_at"]}
    return params, summary


# ------------------------------------------------------------------ รายงาน
def render_report(info: dict) -> str:
    L = ["# รายงานฝึกโมเดลซ้ำ", "", f"ฝึกเมื่อ {info['when']} | ข้อมูลถึง {info.get('data_through')} (โมเดลเดิมถึง {info.get('old_data_through') or 'ไม่ทราบ'})", ""]
    L += ["## ข้อมูลที่เพิ่มจากชีท", "", "| ตาราง | แถวใหม่ |", "|---|---|"] + [f"| {k} | {v} |" for k, v in info["added"].items()]
    sc = info["trend_scale"]
    L += ["", "## สเกล Trend จากรูปเทียบ Trend ข้อความเดิม", "",
          f"แถวจากรูป {sc['img_n']} แถว" + ("" if sc["ratio_b"] is None and sc["ratio_d"] is None else
          f" | อัตราส่วนสเกล เบนซิน {sc['ratio_b']:.2f} ดีเซล {sc['ratio_d']:.2f}" + ("  **⚠ ต่างกันมาก ห้ามผสมโดยไม่ตรวจ**" if sc["flag"] else "")),
          "(ต้องมีแถวจากรูป ≥ 10 แถวถึงจะคำนวณอัตราส่วน ; Trend จากรูปยังไม่ถูกนำมาฝึก)"]
    L += ["", "## ผลต่อโมเดล (ประตูตรวจ)", "", f"ผ่าน {info['n_ok']} / ไม่ผ่าน {info['n_bad']} / ทั้งหมด {info['n_ok'] + info['n_bad']}", "",
          "| โมเดล | AUC ขึ้น เดิม→ใหม่ | AUC ลง เดิม→ใหม่ | กำไร/วัน เดิม→ใหม่ | ผล |", "|---|---|---|---|---|"]
    f3 = lambda v: "-" if v is None or (isinstance(v, float) and np.isnan(v)) else f"{v:.3f}"
    for ident, g in info["gate"].items():
        o = g["old"] or {}
        L.append(f"| {ident} | {f3(o.get('auc_rise'))}→{f3(g['new']['auc_rise'])} | {f3(o.get('auc_fall'))}→{f3(g['new']['auc_fall'])} | "
                 f"{f3(o.get('gain_per_day'))}→{f3(g['new']['gain_per_day'])} | {'ใช้ใหม่' if g['ok'] else 'คงเดิม: ' + '; '.join(g['reasons'])} |")
    if info.get("alert"):
        L += ["", "## ความพร้อมแจ้งเตือน (ผลทดสอบเดินหน้า, ระยะเดินทาง 10 ชม.)", "", "| ชนิด | ช่วง | วันที่มีผล | เร่ง | ชะลอ | หมายเหตุ |", "|---|---|---|---|---|---|"]
        for a in info["alert"]:
            L.append(f"| {a['fuel']} | {a['window']} | {a['n']} | {'ผ่าน' if a['eligible']['URGENT'] else 'ยัง'} | {'ผ่าน' if a['eligible']['WAIT'] else 'ยัง'} | {'; '.join(a['reasons'])} |")
    L += ["", "**ข้อสรุป:** " + info["verdict"]]
    return "\n".join(L) + "\n"


def alert_overview(summary) -> list:
    log = fetch_tab("DispatchLog")
    if log.empty or "outcome_delta" not in log.columns:
        return []
    return [engine.alert_status(log, summary, fuel, win, 10) for fuel in S.FUELS for win in FM.DISPATCH_WINDOWS]


# ------------------------------------------------------------------ ตัวรัน
def new_days(old_through, new_through) -> int:
    """จำนวนวันที่ข้อมูลใหม่กว่าโมเดลเดิม (ไม่ทราบวันเดิม = นับว่ามากพอ)"""
    if old_through is None:
        return 10 ** 6
    return (pd.Timestamp(new_through) - pd.Timestamp(old_through)).days


def run(data_dir: str, out_dir: str, use_sheet: bool = True, fetch=fetch_tab) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    train_dir = os.path.join(out_dir, "train")
    os.makedirs(train_dir, exist_ok=True)
    base = {k: pd.read_csv(os.path.join(data_dir, FILES[k]), encoding="utf-8-sig") for k in TABLES}
    added, sheet_trend = {}, None
    for k, tab in TABLES.items():
        inc = fetch(tab, k) if use_sheet else None
        if k == "trend":
            sheet_trend = inc
        base[k], added[k] = merge_increment(k, base[k], inc)
        base[k].to_csv(os.path.join(train_dir, FILES[k]), index=False, encoding="utf-8")
    scale = trend_scale(pd.read_csv(os.path.join(data_dir, FILES["trend"]), encoding="utf-8-sig"), sheet_trend)

    prices, tables = FM.load(train_dir)
    new_params, new_summary = FM.fit_everything(S.PriceBook(prices), tables)
    old_params, old_summary = M.load_params(), engine.load_backtest_summary()
    old_through = (old_params or {}).get("data_through")
    g = gate(old_summary or {}, new_summary, new_params)
    accepted = [i for i, x in g.items() if x["ok"]]
    gap = new_days(old_through, new_params["data_through"])
    has_new_data = gap >= MIN_NEW_DAYS
    if not has_new_data:
        accepted = []
    n_ok, n_bad = sum(1 for x in g.values() if x["ok"]), sum(1 for x in g.values() if not x["ok"])
    changed = bool(accepted)
    if changed:
        merged_p, merged_s = (new_params, new_summary) if old_params is None else merge_models(old_params, old_summary, new_params, new_summary, accepted)
        FM.write_json(os.path.join(out_dir, "model_params.json"), merged_p)
        FM.write_json(os.path.join(out_dir, "backtest_summary.json"), merged_s)
    verdict = (f"ข้อมูลใหม่กว่าโมเดลเดิมแค่ {gap} วัน (ต้อง ≥ {MIN_NEW_DAYS} วัน) ไม่เปลี่ยนโมเดล" if not has_new_data else
               f"เสนอโมเดลใหม่ {len(accepted)} จาก {len(g)} โมเดล (ที่เหลือคงเดิม) — ต้องให้คนอนุมัติ" if changed else "ไม่มีโมเดลใดผ่านประตู คงโมเดลเดิมทั้งหมด")
    info = {"when": datetime.now().strftime("%Y-%m-%d %H:%M"), "data_through": new_params["data_through"], "old_data_through": old_through, "added": added,
            "trend_scale": scale, "gate": g, "n_ok": n_ok, "n_bad": n_bad, "accepted": accepted, "changed": changed, "verdict": verdict,
            "alert": alert_overview(old_summary) if use_sheet else []}
    with open(os.path.join(out_dir, "report.md"), "w", encoding="utf-8") as fh:
        fh.write(render_report(info))
    with open(os.path.join(out_dir, "report.json"), "w", encoding="utf-8") as fh:
        json.dump(info, fh, ensure_ascii=False, indent=1, default=float)
    return info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out-dir", default="retrain_out")
    ap.add_argument("--no-sheet", action="store_true", help="ไม่ดึงข้อมูลเพิ่มจากชีท (ทดสอบ)")
    a = ap.parse_args()
    info = run(a.data_dir, a.out_dir, use_sheet=not a.no_sheet)
    print(info["verdict"])
    print(f"CHANGED={'true' if info['changed'] else 'false'}")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as fh:
            fh.write(f"changed={'true' if info['changed'] else 'false'}\n")


if __name__ == "__main__":
    main()
