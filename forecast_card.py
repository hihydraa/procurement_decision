"""
การ์ดพยากรณ์ประจำรอบ (โหมดเงา) — สร้างรายงาน "ต้นทุนหน้าคลัง / ราคาปลีก / เหตุผล / คำแนะนำ" ต่อชนิดน้ำมัน บันทึกลงแท็บ ForecastCardLog
แล้วเติมผลจริงของวันถัดไปเพื่อวัดว่าที่เขียนไว้ถูกแค่ไหน — ไม่ส่งข้อความใด ๆ ไม่กระทบการตัดสินใจจริง

รอบ : 16:00 (หลัง MOPS ปิด ก่อนจุดแตะ 16:30 — ประกาศกองทุนของวันถัดไปมักยังไม่ออก) และ 21:30 (หลังประกาศกองทุนที่ออก ~19:18–21:14)
หลักการเขียน : ไม่ฟันธงตัวเลขเดียว — ให้ "ช่วง + โอกาส" จากการกระจายของความคลาดเคลื่อน walk-forward (diag_wf_components) ;
              โอกาสลง/ขึ้น (≥ 10 สต.) < 50% หรือสองฝั่งต่างกันน้อย = "ไม่ชัดเจน" ไม่ฟันธง ; ปลีกยังไม่พยากรณ์ (ไม่มีหลักฐานย้อนหลังเพียงพอ)
ข้อจำกัด : ต้นทุนหน้าคลังอ้างอิง WHOLESALE ของ EPPO (ขยับเป็นขั้น) ; เวลาปรับมาจากราคารายชั่วโมงของ PriceLog ~50 วัน (เวลามีผลของราคาบางส่วนไม่ทราบ) ;
          ประกาศกองทุนเบนซินยังจับคู่กับ G95 ไม่ได้ (ใช้เฉพาะดีเซล) ; คำแนะนำเวลาซื้อยังไม่ผ่านการพิสูจน์เดินหน้า (TapRecLog กำลังวัด)

ใช้: python forecast_card.py [16:00|21:30] [--dry]    (ต้องมี SA_EMAIL / SA_PRIVATE_KEY ; ไม่ระบุรอบ → เลือกตามเวลาไทย ; --dry = พิมพ์การ์ดเท่านั้น ไม่เขียนชีท)
"""
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from common import OILBOT_SHEET_ID, SHEET_ID, TABS, TZ_NAME, ensure_tab, get_token, sheets_append, sheets_get, sheets_update, to_ts
from diag_rule_backtest import MEDIAN_MM, add_deltas, mops_daily, wti_daily
from diag_wf_components import FEATS, build_dataset, eppo_components, ridge_fit, walk_forward
from ingest import cell_time
from pricelog_frame import build_frame
from tap_policy import DEPOTS, GRID_STEP, depot_series, price_at

FUEL_TH = {"B7": "ดีเซล", "G95": "แก๊สโซฮอล์ 95"}
RETAIL_NAME = {"B7": "ดีเซล B7", "G95": "แก๊สโซฮอล์ 95"}
ROUNDS = ("16:00", "21:30")
UP = 0.10                              # ขยับ >= 10 สต. = ขึ้น/ลง
MIN_RESID = 30                         # ต้องมีความคลาดเคลื่อน walk-forward อย่างน้อยกี่ค่าจึงให้ความน่าจะเป็น
CALL_P, CALL_GAP = 0.50, 0.25          # ฟันธงทิศเมื่อโอกาสฝั่งนั้น >= 50% และนำอีกฝั่ง >= 25 จุด
SLOTS = (("ก่อนเที่ยงคืน (หลัง 20:30)", -3.5, 0.0), ("00:00–01:59", 0.0, 2.0), ("02:00–04:59", 2.0, 5.0), ("05:00–06:59", 5.0, 7.0),
         ("07:00–08:59", 7.0, 9.0), ("09:00–15:59", 9.0, 16.0), ("16:00–22:00", 16.0, 22.1))
SLOT_END_TXT = {"05:00–06:59": "07:00", "07:00–08:59": "09:00", "02:00–04:59": "05:00", "00:00–01:59": "02:00", "09:00–15:59": "16:00"}
SLOT_START_TXT = {"ก่อนเที่ยงคืน (หลัง 20:30)": "22:30", "00:00–01:59": "00:00", "02:00–04:59": "02:00", "05:00–06:59": "05:00", "07:00–08:59": "07:00"}
COLUMNS = ["card_id", "asof", "round", "fuel", "target_day", "mops_day", "m1", "m3", "w1", "w3", "mm_gap", "known_fund", "pred", "lo", "hi",
           "p_down", "p_flat", "p_up", "call", "confidence", "timing", "retail_now", "reasons", "recommendation", "model", "card_text",
           "outcome_dws", "outcome_cls", "right", "filled_at"]
OUT_FROM, OUT_TO = COLUMNS.index("outcome_dws"), COLUMNS.index("filled_at")
LAST_COL = "AD"


def col_letter(i: int) -> str:
    return chr(65 + i) if i < 26 else "A" + chr(65 + i - 26)


# ---------- ข้อมูลเข้า ----------
def load_notices_full(values: list) -> pd.DataFrame:
    h = [str(x).strip() for x in values[0]]
    ix = {c: h.index(c) for c in ("msg_date", "msg_time", "effective_date", "diesel_old", "diesel_new")}
    rows = []
    for r in values[1:]:
        g = lambda c: r[ix[c]] if ix[c] < len(r) else ""
        md, ed = to_ts(g("msg_date")), to_ts(g("effective_date"))
        try:
            do, dn = float(g("diesel_old")), float(g("diesel_new"))
        except (TypeError, ValueError):
            continue
        if not pd.isna(md) and not pd.isna(ed):
            rows.append((md.normalize(), str(cell_time(g("msg_time"))), ed.normalize(), do, dn))
    return pd.DataFrame(rows, columns=["msg_day", "msg_time", "eff_day", "old", "new"]).sort_values(["msg_day", "msg_time"]).reset_index(drop=True)


def known_fund_change(notices: pd.DataFrame, fuel: str, target: pd.Timestamp):
    """การเปลี่ยนของกองทุน (บาท/ลิตร) ที่มีผลวัน target ตามประกาศล่าสุด ; None = ไม่ทราบ/ไม่มีประกาศ (เบนซินจับคู่ไม่ได้ จึงคืน None เสมอ)"""
    if fuel != "B7" or notices.empty:
        return None
    n = notices[notices.eff_day == target]
    return float(n.iloc[-1].new - n.iloc[-1].old) if len(n) else None


def current_features(mops: pd.DataFrame, wti: pd.DataFrame, eppo: pd.DataFrame, fuel: str):
    m = mops[mops.fuel == fuel].set_index("day")["mops"].sort_index()
    if len(m) < 5:
        return None
    carried = False
    if len(m) >= 2 and float(m.iloc[-1]) == float(m.iloc[-2]):          # ราคาปิดซ้ำเท่าเดิมเป๊ะ = แถวที่ลากค่าเดิมมา/ยังไม่อัปเดต → ไม่ใช้
        m, carried = m.iloc[:-1], True
    f = add_deltas(m).dropna()
    t = f.index[-1]
    w = wti.set_index("day")["wti"].sort_index()
    w = w[w.index <= t]
    e = eppo[eppo.fuel == fuel].sort_values("day")
    if len(w) < 4 or e.empty:
        return None
    last = e.iloc[-1]
    return {"carried": carried, "mops_day": t, "m1": float(f.loc[t, "d1"]), "m3": float(f.loc[t, "d3"]), "w1": float(w.diff(1).iloc[-1]), "w3": float(w.diff(3).iloc[-1]),
            "mm_gap": float(last.mm - MEDIAN_MM[fuel]), "eppo_day": last.day, "ws": float(last.ws), "fund": float(last.fund)}


def fit_predict(ds: pd.DataFrame, fuel: str, feats: dict, known):
    """(pred, residuals, ชื่อโมเดล) — มีประกาศกองทุนที่ทราบ → โมเดล C (ฟีเจอร์ + Δกองทุน/ภาษีที่ประกาศ) ; ไม่ทราบ → DIRECT (ฟีเจอร์อย่างเดียว)"""
    g = ds[ds.fuel == fuel].sort_values("day")
    wf = walk_forward(ds)
    wf = wf[wf.fuel == fuel]
    x = np.array([feats[c] for c in FEATS], dtype=float)
    X = g[FEATS].to_numpy(float)
    if known is None:
        return float(ridge_fit(X, g.dws.to_numpy(float))(x)[0]), (wf.actual - wf.DIRECT).to_numpy(float), "DIRECT"
    pred = ridge_fit(np.c_[X, g.dknown.to_numpy(float)], g.dws.to_numpy(float))(np.r_[x, known])[0]
    return float(pred), (wf.actual - wf.C).to_numpy(float), "C"


def out_of_range(ds: pd.DataFrame, fuel: str, feats: dict) -> list:
    """ฟีเจอร์ที่อยู่นอกช่วงที่โมเดลเคยเห็น (|ค่า| เกินค่าสูงสุดในข้อมูลฝึก) — โมเดลเชิงเส้นจะขยายคำทายเกินจริงและความน่าจะเป็นมั่นใจเกิน"""
    g = ds[ds.fuel == fuel]
    return [c for c in ("m1", "m3", "w1", "w3") if len(g) and abs(feats[c]) > g[c].abs().max()]


def cap_confidence(conf: str, oor: list) -> str:
    """ยังไม่ผ่านการทดสอบเดินหน้า → สูงสุด 'กลาง' ; ฟีเจอร์นอกช่วงที่เคยเห็น → 'ต่ำ'"""
    if oor:
        return "ต่ำ"
    return "กลาง" if conf == "สูง" else conf


def probabilities(pred: float, resid: np.ndarray) -> dict:
    if len(resid) < MIN_RESID:
        return {}
    s = pred + resid
    return {"p_down": float((s <= -UP).mean()), "p_up": float((s >= UP).mean()), "p_flat": float(((s > -UP) & (s < UP)).mean()),
            "lo": float(np.percentile(s, 20)), "hi": float(np.percentile(s, 80))}


def decide(p: dict) -> tuple:
    """(ทิศ 'ลง'/'ขึ้น'/'ไม่ชัด', ความเชื่อมั่น 'สูง'/'กลาง'/'ต่ำ')"""
    if not p:
        return "ไม่ชัด", "ต่ำ"
    d, u = p["p_down"], p["p_up"]
    if d >= CALL_P and d - u >= CALL_GAP:
        return "ลง", ("สูง" if d >= 0.75 else "กลาง" if d >= 0.60 else "ต่ำ")
    if u >= CALL_P and u - d >= CALL_GAP:
        return "ขึ้น", ("สูง" if u >= 0.75 else "กลาง" if u >= 0.60 else "ต่ำ")
    return "ไม่ชัด", "ต่ำ"


# ---------- เวลาที่ราคาปรับ (จาก PriceLog) ----------
def change_time_profile(prices: pd.DataFrame, fuel: str, days: pd.DatetimeIndex, step_h: float = GRID_STEP) -> dict:
    """สัดส่วนวัน (ที่มีการขยับ) ที่ 'การขยับแรกในหน้าต่าง 20:30 วันก่อน → 22:00 วัน D' ตกช่วงเวลาไหน แยกตามทิศ ; ราคา = เฉลี่ยข้ามคลังหลักของค่าต่ำสุดข้ามจ๊อบเบอร์"""
    out = {"ลง": {}, "ขึ้น": {}, "n": {"ลง": 0, "ขึ้น": 0}}
    series = {d: depot_series(prices, d, fuel) for d in DEPOTS}
    for day in days:
        hrs = np.arange(-3.5, 22.0 + 1e-9, step_h)
        grid = pd.DatetimeIndex([day + pd.Timedelta(hours=h) for h in hrs])
        mat = np.vstack([price_at(s, grid) for s in series.values()])
        if (~np.isnan(mat)).sum(axis=0).min() < 3:
            continue
        with np.errstate(all="ignore"):
            avg = np.nanmean(mat, axis=0)
        diff = np.diff(avg)
        for sign, label in ((-1, "ลง"), (1, "ขึ้น")):
            idx = np.where(sign * diff >= UP)[0]
            if len(idx):
                h = hrs[idx[0] + 1]
                slot = next(n for n, a, b in SLOTS if a <= h < b)
                out[label][slot] = out[label].get(slot, 0) + 1
                out["n"][label] += 1
    return out


def timing_text(profile: dict, call: str) -> tuple:
    """('ช่วงเวลา: 05:00–06:59 (60%) / 07:00–08:59 (25%)', ช่วงเด่นสุด)"""
    if call not in ("ลง", "ขึ้น") or profile["n"][call] < 5:
        return "ยังไม่มีข้อมูลเวลาปรับเพียงพอ", None
    items = sorted(profile[call].items(), key=lambda kv: -kv[1])[:3]
    n = profile["n"][call]
    return "เวลาที่ผ่านมามักปรับ: " + " / ".join(f"{k} ({v / n:.0%})" for k, v in items) + f" (จาก {n} วัน ~50 วันล่าสุด ; เวลามีผลบางส่วนไม่ทราบ)", items[0][0]


def recommend(call: str, conf: str, top_slot, fuel: str) -> str:
    cav = " ข้อควรระวัง: คลังอาจจำกัดโควต้า/เวลาตัดรับออเดอร์ (ยังไม่รวมในการคำนวณ)"
    if call == "ลง":
        after = SLOT_END_TXT.get(top_slot, "08:30") if top_slot else "08:30"
        return f"ชะลอการสั่งซื้อ รอซื้อหลัง {after}" + ("" if conf != "ต่ำ" else " (ความเชื่อมั่นต่ำ)") + cav
    if call == "ขึ้น":
        before = SLOT_START_TXT.get(top_slot, "22:30") if top_slot else "22:30"
        return f"ควรซื้อก่อน {before}" + ("" if conf != "ต่ำ" else " (ความเชื่อมั่นต่ำ)") + cav
    return "ไม่มีสัญญาณชัด ใช้แตะ 16:30 ตามค่าเริ่มต้น (ยังไม่พิสูจน์ว่าดีกว่าเวลาอื่น)" + cav


def reasons_text(fuel: str, f: dict, known, notice_msg: str, rnd: str) -> str:
    mname = "MOPS " + ("ดีเซล" if fuel == "B7" else "G95")
    parts = [f"{mname} {f['m1'] + 0.0:+.2f} (Δ1D) {f['m3'] + 0.0:+.2f} (Δ3D) ณ {f['mops_day']:%d/%m}", f"WTI Δ1D {f['w1'] + 0.0:+.2f}"]
    if known is not None:
        parts.append(f"ประกาศกองทุนดีเซลมีผลพรุ่งนี้ {known:+.2f} บาท/ลิตร ({'ลดอุดหนุน → ดันต้นทุนขึ้น' if known > 0 else 'เพิ่มอุดหนุน → กดต้นทุน' if known < 0 else 'ไม่เปลี่ยน'})")
    elif fuel == "B7":
        parts.append(notice_msg)
    else:
        parts.append("ประกาศกองทุนเบนซินยังจับคู่กับ G95 ไม่ได้")
    parts.append(f"ค่าการตลาดเทียบค่ากลาง {(round(f['mm_gap'], 2) + 0.0):+.2f} (EPPO {f['eppo_day']:%d/%m})")
    return " · ".join(parts)


def build_card(fuel: str, rnd: str, now: pd.Timestamp, f: dict, known, notice_msg: str, pred: float, resid: np.ndarray, model: str, profile: dict, retail_now: str, oor: list = None) -> dict:
    p = probabilities(pred, resid)
    call, conf = decide(p)
    oor = oor or []
    conf = cap_confidence(conf, oor)
    timing, top = timing_text(profile, call)
    rec = recommend(call, conf, top, fuel)
    target = f["mops_day"].normalize() + pd.Timedelta(days=1)          # วันที่คำทายใช้ (สอดคล้องกับการฝึก : MOPS วัน t → ราคาหน้าคลัง t+1)
    if p:
        rng = f"{p['lo']:+.2f} ถึง {p['hi']:+.2f}"
        head = (f"ต้นทุน{FUEL_TH[fuel]}: " + (f"มีแนวโน้มปรับ{call} ช่วงประมาณ {rng} บาท/ลิตร" if call != "ไม่ชัด" else f"ไม่ชัดเจน (ช่วงที่เป็นไปได้ {rng} บาท/ลิตร)")
                + f" · โอกาสลง ≥10 สต. {p['p_down']:.0%} / นิ่ง {p['p_flat']:.0%} / ขึ้น ≥10 สต. {p['p_up']:.0%}")
    else:
        head = f"ต้นทุน{FUEL_TH[fuel]}: ไม่ชัดเจน (ข้อมูลย้อนหลังไม่พอสำหรับให้ความน่าจะเป็น)"
    reasons = reasons_text(fuel, f, known, notice_msg, rnd)
    if f.get("carried"):
        reasons += " · หมายเหตุ: MOPS ล่าสุดในชีทซ้ำค่าเดิม (ยังไม่อัปเดต) จึงใช้ของวันก่อนหน้า"
    if oor:
        reasons += f" · ⚠ สัญญาณ {', '.join(oor)} อยู่นอกช่วงที่โมเดลเคยเห็น จึงจำกัดความเชื่อมั่นเป็นต่ำ"
    text = "\n".join([f"รอบ {rnd} น. · แผนสำหรับ {target:%d/%m/%Y} · ความเชื่อมั่น: {conf}", head, f"เวลาที่ปรับ: {timing}",
                      f"ราคาปลีก: {retail_now} ; ยังไม่มีหลักฐานย้อนหลังเพียงพอที่จะพยากรณ์ (โดยทั่วไปปลีกตามราคาส่งช้ากว่า)", f"เหตุผล: {reasons}", f"คำแนะนำ: {rec}",
                      "(โหมดเงา — ยังไม่ผ่านการทดสอบเดินหน้า ไม่ใช่คำสั่งซื้อ)"])
    return {"card_id": f"{target:%Y-%m-%d} {rnd} {fuel}", "asof": now.strftime("%Y-%m-%d %H:%M"), "round": rnd, "fuel": fuel, "target_day": target.strftime("%Y-%m-%d"),
            "mops_day": f["mops_day"].strftime("%Y-%m-%d"), "m1": round(f["m1"], 3), "m3": round(f["m3"], 3), "w1": round(f["w1"], 3), "w3": round(f["w3"], 3),
            "mm_gap": round(f["mm_gap"], 3), "known_fund": "" if known is None else round(known, 3), "pred": round(pred, 3), "lo": round(p.get("lo", float("nan")), 3) if p else "",
            "hi": round(p.get("hi", float("nan")), 3) if p else "", "p_down": round(p["p_down"], 3) if p else "", "p_flat": round(p["p_flat"], 3) if p else "",
            "p_up": round(p["p_up"], 3) if p else "", "call": call, "confidence": conf, "timing": timing, "retail_now": retail_now, "reasons": reasons,
            "recommendation": rec, "model": model, "card_text": text, **{c: "" for c in COLUMNS[OUT_FROM:]}}


# ---------- ผลจริง ----------
def fill_outcome(row: dict, eppo: pd.DataFrame):
    """ΔWS จากวันก่อนวันเป้าหมายถึงวันเป้าหมาย (แถว EPPO ไม่ใช่ CARRIED) ; None ถ้ายังไม่มีข้อมูลวันเป้าหมาย"""
    e = eppo[eppo.fuel == row["fuel"]].set_index("day")["ws"]
    tgt = pd.Timestamp(row["target_day"])
    base = tgt - pd.Timedelta(days=1)
    if tgt not in e.index or base not in e.index:
        return None
    d = float(e[tgt] - e[base])
    cls = "ลง" if d <= -UP else "ขึ้น" if d >= UP else "นิ่ง"
    call = row.get("call", "ไม่ชัด")
    right = "" if call == "ไม่ชัด" else int(call == cls)
    return {"outcome_dws": round(d, 3), "outcome_cls": cls, "right": right, "filled_at": datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y-%m-%d %H:%M")}


def summary_lines(rows: list) -> list:
    d = pd.DataFrame([r for r in rows if str(r.get("filled_at", "")) != ""])
    if d.empty:
        return ["[CARD] ยังไม่มีการ์ดที่เติมผลแล้ว"]
    d["right_n"] = pd.to_numeric(d["right"], errors="coerce")
    called = d[d["call"].isin(["ลง", "ขึ้น"])]
    base = d["outcome_cls"].value_counts(normalize=True)
    out = [f"[CARD] เติมผลแล้ว {len(d)} ใบ ({d.card_id.str[:10].nunique()} วัน) ; ฟันธงทิศ {len(called)} ใบ ถูก {int(called.right_n.sum())} ({called.right_n.mean():.0%})" if len(called)
           else f"[CARD] เติมผลแล้ว {len(d)} ใบ ; ยังไม่เคยฟันธงทิศ",
           "   สัดส่วนจริง: " + ", ".join(f"{k} {v:.0%}" for k, v in base.items())]
    for call in ("ลง", "ขึ้น"):
        c = called[called.call == call]
        if len(c):
            out.append(f"   ฟันธง{call}: {len(c)} ใบ ถูก {int(c.right_n.sum())} ; ค่าเฉลี่ย ΔWS จริง {pd.to_numeric(c.outcome_dws).mean():+.3f}")
    return out


# ---------- ชีท ----------
def retail_now_text(values: list, fuel: str) -> str:
    h = [str(x).strip() for x in values[0]] if values else []
    if not h or "product" not in h:
        return "ไม่มีข้อมูล RetailLog"
    ix = {c: h.index(c) for c in ("last_seen_at", "product", "price")}
    best = None
    for r in values[1:]:
        if len(r) > ix["price"] and str(r[ix["product"]]).strip() == RETAIL_NAME[fuel]:
            best = r
    if not best:
        return "ไม่มีข้อมูล RetailLog"
    seen = to_ts(best[ix["last_seen_at"]])
    return f"ปตท. {float(best[ix['price']]):.2f} บาท/ลิตร (ตรวจล่าสุด {seen:%d/%m %H:%M})" if not pd.isna(seen) else f"ปตท. {float(best[ix['price']]):.2f} บาท/ลิตร"


def pick_round(now: pd.Timestamp) -> str:
    return "16:00" if now.hour < 18 else "21:30"


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    now = pd.Timestamp(datetime.now(ZoneInfo(TZ_NAME)).replace(tzinfo=None))
    dry = "--dry" in argv
    rnd = next((a for a in argv if a in ROUNDS), pick_round(now))
    tok = get_token()
    mops, wti = mops_daily(sheets_get(tok, SHEET_ID, "Entry_MOPS!A1:F6000")), wti_daily(sheets_get(tok, SHEET_ID, "Entry_WTI!A1:H6000"))
    eppo = eppo_components(sheets_get(tok, SHEET_ID, "Entry_Eppo!A1:S6000"))
    notices = load_notices_full(sheets_get(tok, SHEET_ID, "Entry_FundNotice!A1:H3000"))
    ds = build_dataset(mops, wti, eppo)
    prices = build_frame(sheets_get(tok, OILBOT_SHEET_ID, "PriceLog!A1:Q200000")).reset_index(drop=True)
    retail_vals = sheets_get(tok, SHEET_ID, f"{TABS['RETAIL_LOG']}!A1:G5000")
    days = pd.date_range(prices.effective.max().normalize() - pd.Timedelta(days=50), prices.effective.max().normalize(), freq="D")
    profile_cache = {}
    cards = []
    for fuel in ("B7", "G95"):
        f = current_features(mops, wti, eppo, fuel)
        if f is None:
            print(f"[CARD] {fuel}: ข้อมูลไม่พอสำหรับสร้างการ์ด")
            continue
        target = f["mops_day"].normalize() + pd.Timedelta(days=1)
        known = known_fund_change(notices, fuel, target)
        # รอบ 21:30 : ประกาศของพรุ่งนี้ออกครบแล้ว (ถึง ~21:15) ไม่มีประกาศ = กองทุนไม่เปลี่ยน ; รอบ 16:00 : ยังไม่ทราบ
        if known is None and fuel == "B7" and rnd == "21:30":
            known, notice_msg = 0.0, "ไม่มีประกาศกองทุนดีเซลที่มีผลพรุ่งนี้ (ถือว่าไม่เปลี่ยน)"
        else:
            notice_msg = "ประกาศกองทุนดีเซลของพรุ่งนี้ยังไม่ออก (ปกติออก ~19:18–21:14)"
        pred, resid, model = fit_predict(ds, fuel, f, known)
        profile = profile_cache.setdefault(fuel, change_time_profile(prices, fuel, days))
        cards.append(build_card(fuel, rnd, now, f, known, notice_msg, pred, resid, model, profile, retail_now_text(retail_vals, fuel), out_of_range(ds, fuel, f)))
    for c in cards:
        print(c["card_text"])
        print("-" * 60)
    if dry:
        print("[CARD] --dry : ไม่เขียนชีท")
        return
    tab = TABS["FORECAST_CARD"]
    if ensure_tab(tok, SHEET_ID, tab):
        sheets_update(tok, SHEET_ID, f"{tab}!A1", [COLUMNS])
        print(f"[CARD] created tab {tab}")
    values = sheets_get(tok, SHEET_ID, f"{tab}!A1:{LAST_COL}20000")
    head = values[0] if values else COLUMNS
    rows = [dict(zip(head, r + [""] * (len(head) - len(r)))) for r in values[1:]]
    have = {str(r.get("card_id", "")) for r in rows}
    add = [c for c in cards if c["card_id"] not in have]
    if add:
        sheets_append(tok, SHEET_ID, f"{tab}!A1:{LAST_COL}1", [[c[k] for k in COLUMNS] for c in add])
        rows += add
    print(f"[CARD] new cards: +{len(add)}")
    filled = 0
    for i, r in enumerate(rows):
        if str(r.get("filled_at", "")) == "" and str(r.get("target_day", "")):
            res = fill_outcome({**r, "target_day": str(to_ts(r["target_day"]).date())}, eppo)
            if res:
                rows[i].update(res)
                sheets_update(tok, SHEET_ID, f"{tab}!{col_letter(OUT_FROM)}{i + 2}:{col_letter(OUT_TO)}{i + 2}", [[res.get(c, "") for c in COLUMNS[OUT_FROM:OUT_TO + 1]]])
                filled += 1
    print(f"[CARD] outcomes filled: {filled}")
    for line in summary_lines(rows):
        print(line)


if __name__ == "__main__":
    main()
