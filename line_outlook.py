"""
ส่งข้อความ "แนวโน้มราคาส่ง (Jobber) 24 ชม." (รอบ 20:30) เข้าไลน์ส่วนตัวของผู้ดูแลระบบ — ช่วงทดสอบ ยังไม่ส่งเข้ากลุ่มใด ๆ

เนื้อหา : ใช้โมเดลเดียว คือแนวโน้ม Jobber 24 ชม. จากข้อมูล 20:30 (ตัวเดียวที่ผลจริงเริ่มเข้ากับผลทดสอบย้อนหลัง)
          + เหตุผลสั้น ๆ + ผลของคำทายล่าสุดที่รู้ผลแล้ว + สถิติผลจริงสะสมจาก OutlookLog
          ไม่รวมแนวโน้ม 48 ชม. และการ์ดต้นทุนขายส่ง (ยังไม่มีหลักฐานพอ)
          ข้อความตอบเมื่อพิมพ์ "forecast" (on_demand) มีพยากรณ์ราคาขายปลีกจาก RetailFcLog ต่อท้ายด้วย ติดป้าย [ทดลอง] — ข้อความประจำคืนไม่มี
          เพราะส่งก่อนการ์ดรอบ 21:30 ซึ่งเป็นรอบที่ออกพยากรณ์ปลีก
ความปลอดภัย : ปลายทางมาจาก Secret LINE_TEST_USER_ID เท่านั้น และต้องเป็นรหัสผู้ใช้ (ขึ้นต้น U) — รหัสกลุ่ม/ห้อง (C/R) ถูกปฏิเสธ
              ไม่มี Secret = ไม่ส่ง (ไม่ถือเป็นข้อผิดพลาด) ; ส่งครั้งเดียวต่อจุดอ้างอิง (บันทึกในแท็บ LineTestLog)

ขั้นตอน : main.py เขียน line_outlook_input.json → log_outlook.py เติมผลจริง → python line_outlook.py [--dry]
"""
import json
import os
import re
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

import engine as E
import model as M
from common import SHEET_ID, TABS, TZ_NAME, ensure_tab, get_token, sheets_append, sheets_get, sheets_update, to_ts

INPUT_FILE = "line_outlook_input.json"
LOG_TAB = TABS.get("LINE_TEST_LOG", "LineTestLog")
LOG_COLUMNS = ["ref", "sent_at", "status", "text"]
FUELS = (("B7", "ดีเซล"), ("G95", "แก๊สโซฮอล์ 95"))
TIER_TEXT = {"UP_STRONG": "📈📈 ขึ้นแรง (คาดขยับตั้งแต่ 40 สต./ลิตร)", "UP": "📈 ขึ้น (คาดขยับ 10–40 สต./ลิตร)",
             "DOWN": "📉 ลง (คาดขยับ 10–40 สต./ลิตร)", "DOWN_STRONG": "📉📉 ลงแรง (คาดขยับตั้งแต่ 40 สต./ลิตร)"}
CALL_WORD = {"UP_STRONG": "ขึ้นแรง", "UP": "ขึ้น", "DOWN": "ลง", "DOWN_STRONG": "ลงแรง"}
UP_MARK, DOWN_MARK = "เสี่ยงราคาขึ้น", "โน้มราคาลง"
USER_ID_RE = re.compile(r"^U[0-9a-f]{32}$")
READY_DAYS = 40
FOOTER = "[ทดสอบ] ยังไม่ผ่านเกณฑ์ผลจริง 40 วัน ไม่ใช่คำสั่งซื้อ"


# ---------- ข้อมูลเข้าจาก main.py ----------
def make_input(outlook: dict, asof) -> dict:
    """ย่อผลของ engine.decide_outlook ให้เหลือเฉพาะที่ข้อความใช้ (ช่วง 24 ชม.) — เขียนเป็นไฟล์ให้ขั้นส่งอ่าน"""
    fuels = {}
    for d in outlook.values():
        h = next((x for x in d.get("horizons", []) if int(x["hours"]) == 24), None)
        ev = (h or {}).get("evidence") or {}
        fuels[d["fuel"]] = {
            "ref": d["ref"].strftime("%Y-%m-%d %H:%M"), "source": d.get("source"), "stale": bool(d.get("stale")), "note": d.get("note"),
            "profile": [{"hours": int(pf["hours"]), "at": pd.Timestamp(pf["at"]).strftime("%H:%M"), "call": pf["call"], "ok": bool(pf["ok"]),
                         "n": pf.get("n"), "right": pf.get("right")} for pf in (d.get("profile") or [])],
            "h24": None if h is None else {"tier": h["tier"], "grade": h.get("grade"), "reasons": list(h.get("reasons") or []),
                                           "n": ev.get("n"), "right10": ev.get("right10"), "wrong10": ev.get("wrong10")}}
    return {"asof": pd.Timestamp(asof).strftime("%Y-%m-%d %H:%M"), "fuels": fuels}


def write_input(outlook: dict, asof, path: str = INPUT_FILE) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(make_input(outlook, asof), fh, ensure_ascii=False, indent=1)


# ---------- ข้อความ ----------
def _strip(reason: str) -> str:
    return reason.split(" → ")[0].strip()


def _reasons(reasons: list, mark: str, k: int) -> list:
    return [_strip(r) for r in reasons if mark in r][:k]


def usable(f: dict, asof: pd.Timestamp, on_demand: bool = False) -> str:
    """'' ถ้าใช้ส่งได้ ; ไม่งั้นคืนเหตุผล — ข้อความประจำคืน : ต้องเป็นคำทายจากข้อมูล 20:30 ของวันนี้ ไม่ค้าง และมีผลช่วง 24 ชม.
    on_demand (ตอบเมื่อพิมพ์ถาม) : ใช้คำทายล่าสุดที่ยังไม่หมดอายุ 24 ชม. ได้ (เช่น ของเมื่อคืน หรือรอบบ่าย 15:30)"""
    if not f:
        return "ไม่มีผลของชนิดนี้"
    ref = pd.Timestamp(f["ref"])
    if f.get("h24") is None:
        return f.get("note") or "ระบบยังไม่ให้แนวโน้ม (ข้อมูลไม่พร้อม)"
    if on_demand:
        return "" if pd.Timedelta(0) <= asof - ref < pd.Timedelta(hours=24) else f"คำทายล่าสุด ({ref:%d/%m %H:%M}) หมดอายุแล้ว"
    if ref.strftime("%H:%M") != "20:30" or ref.normalize() != asof.normalize() or f.get("stale"):
        return f"ยังไม่มีข้อมูลรอบ 20:30 ของวันนี้ (ล่าสุด {ref:%d/%m %H:%M})"
    return ""


def fuel_block(name: str, f: dict) -> list:
    h = f["h24"]
    tier = h["tier"]
    if tier == "UNCLEAR":
        return [f"{name}: ❓ ไม่มีสัญญาณชัด"]
    out = [f"{name}: {TIER_TEXT[tier]}", f"ความเชื่อมั่น: {h.get('grade') or 'ต่ำ'}"]
    out += [f"• {r}" for r in _reasons(h["reasons"], UP_MARK if tier.startswith("UP") else DOWN_MARK, 2)]
    if h.get("n") and h.get("right10") is not None:
        out.append(f"ย้อนหลังเมื่อระบบบอกแบบนี้ ({int(h['n'])} ครั้ง): ถูก {h['right10']:.0%} ผิดทิศ {h['wrong10']:.0%}")
    return out


AVG_PM_DISCOUNT = "โดยเฉลี่ยราคาช่วง 16:00 ถูกกว่า 08:00 ราว 0.10 บาท/ลิตร"


def timing_line(f: dict) -> str:
    """คำแนะนำช่วงเวลาซื้อของวันถัดจากจุดอ้างอิง จากโมเดลรายช่วงเวลา (12:30 / 16:30) — ใช้เฉพาะทิศที่ผ่านเกณฑ์ walk-forward (ok)
    ลำดับ (ตรึงก่อนเห็นผล) : ขึ้นภายในเที่ยง → ซื้อก่อนเช้า ; ลงภายใน 16:30 → รอบ่าย ; ยังสูงถึง 16:30 → การรอไม่ช่วย ; ไม่เข้าข้อใด = '' """
    pf = {p["hours"]: p for p in (f.get("profile") or []) if p.get("call") and p.get("ok")}
    pct = lambda p: f" (ย้อนหลังถูก {p['right']:.0%} จาก {int(p['n'])} ครั้ง)" if p.get("right") is not None and p.get("n") else ""
    day = f"{pd.Timestamp(f['ref']) + pd.Timedelta(days=1):%d/%m}"
    noon, pm = pf.get(16), pf.get(20)
    if noon and noon["call"] == "UP":
        return f"ราคามักขึ้นภายในเที่ยงวันที่ {day} → ถ้าต้องซื้อ ซื้อก่อนเช้าวันนั้น{pct(noon)}"
    if pm and pm["call"] == "DOWN":
        return f"ราคามักลงภายใน 16:30 น. วันที่ {day} → ถ้ารอได้ รอซื้อช่วงบ่ายแก่{pct(pm)}"
    if pm and pm["call"] == "UP":
        return f"ราคามักยังสูงกว่าคืนก่อนถึง 16:30 น. วันที่ {day} → การรอไม่ช่วย{pct(pm)}"
    return ""


def _mark(tier: str, delta: float) -> str:
    if tier == "UNCLEAR":
        return ""
    mv = delta * (1.0 if tier.startswith("UP") else -1.0)
    return " ✅" if mv >= M.OUTLOOK_SMALL else " ❌" if mv <= -M.OUTLOOK_SMALL else " ➖ ราคานิ่ง"


def last_results(log: pd.DataFrame) -> list:
    """ผลของคำทายล่าสุดที่รู้ผลแล้ว (รอบ 20:30 ช่วง 24 ชม. ที่ออกตรงเวลา)"""
    rows = []
    for fuel, name in FUELS:
        d = E._outlook_scored(log, fuel, 24)
        if d is None:
            continue
        r = d.assign(_ref=pd.to_datetime(d["ref"], errors="coerce")).sort_values("_ref").iloc[-1]
        call = "ไม่ได้ทาย" if r["tier"] == "UNCLEAR" else f"ทาย{CALL_WORD.get(r['tier'], r['tier'])}"
        rows.append((r["_ref"], f"{name}: {call} → จริง {float(r['outcome_delta']):+.2f}{_mark(r['tier'], float(r['outcome_delta']))}"))
    if not rows:
        return ["ผลของคำทายก่อนหน้า: ยังไม่มีผลจริง"]
    latest = max(t for t, _ in rows)
    return [f"ผลของคำทายคืน {latest:%d/%m}"] + [line for t, line in rows if t == latest]


def live_summary(log: pd.DataFrame) -> list:
    calls = right = wrong = flat = 0
    days, first = 0, None
    for fuel, _ in FUELS:
        d = E._outlook_scored(log, fuel, 24)
        if d is None:
            continue
        days = max(days, len(d))
        t0 = pd.to_datetime(d["ref"], errors="coerce").min()
        first = t0 if first is None else min(first, t0)
        c = d[d["tier"] != "UNCLEAR"]
        mv = c["outcome_delta"] * c["tier"].map(lambda t: 1.0 if str(t).startswith("UP") else -1.0)
        calls, right, wrong = calls + len(c), right + int((mv >= M.OUTLOOK_SMALL).sum()), wrong + int((mv <= -M.OUTLOOK_SMALL).sum())
        flat += int((mv.abs() < M.OUTLOOK_SMALL).sum())
    if first is None:
        return ["สถิติผลจริง: ยังไม่มี"]
    return [f"สถิติผลจริงตั้งแต่ {first:%d/%m} ({days}/{READY_DAYS} วัน)", f"ฟันธง {calls} ครั้ง · ถูก {right} · ผิดทิศ {wrong} · ราคานิ่ง {flat}"]


# ---------- ราคาขายปลีก (ทดลอง) : เฉพาะข้อความตอบเมื่อพิมพ์ถาม ----------
RETAIL_CALL = {"ลง": "📉 น่าจะปรับลง", "ขึ้น": "📈 น่าจะปรับขึ้น", "นิ่ง": "➖ น่าจะไม่ปรับ"}
RETAIL_NONE = "ยังไม่มีพยากรณ์ของวันถัดไป (ออกหลัง 21:30 น. คืนจันทร์–พฤหัสบดี)"


def _f(v):
    try:
        x = float(v)
        return None if x != x else x
    except (TypeError, ValueError):
        return None


def _probs(r, keys) -> str:
    p = [_f(r.get(k)) for k in keys]
    return "" if None in p else f"ลง {p[0]:.0%} / ไม่ปรับ {p[1]:.0%} / ขึ้น {p[2]:.0%}"


def retail_lines(retail: pd.DataFrame, asof: pd.Timestamp) -> list:
    """ส่วนราคาขายปลีกของข้อความ จาก RetailFcLog ที่ผ่าน procurement_summary.prep_retail (มี asof_dt, target_dt)
    ใช้แถวล่าสุดของแต่ละชนิด เฉพาะที่วันเป้าหมายยังไม่ผ่าน (วันนี้หรือพรุ่งนี้) ; วันเป้าหมาย = วันนี้และรู้ผลแล้ว จะบอกผลจริงด้วย
    บรรทัดหลัก = คำทายหลักของแถว (p_*/call) : model = SIG คือโมเดลสัญญาณ Jobber และวิธีเดิม (old_*) เป็นบรรทัดคู่ขนาน
    แถวเก่า/แถวที่โมเดลสัญญาณใช้ไม่ได้ (model ≠ SIG) : วิธีเดิมเป็นหลัก และโมเดลสัญญาณ (sig_*) เป็นบรรทัดคู่ขนานถ้ามี"""
    import procurement_summary as PS
    out = ["⛽ ราคาขายปลีก วันถัดไป [ทดลอง ยังไม่ผ่านการทดสอบเดินหน้า]"]
    today, stats = pd.Timestamp(asof).normalize(), []
    for fuel, name in FUELS:
        r = PS.latest_retail(retail, fuel)
        if not r or pd.Timestamp(r["target_dt"]).normalize() < today:
            last = f" ; ล่าสุดเป็นของวันที่ {pd.Timestamp(r['target_dt']):%d/%m}" if r else ""
            out.append(f"{name}: {RETAIL_NONE}{last}")
        else:
            call = str(r.get("call") or "").strip()
            out.append(f"{name} (วันที่ {pd.Timestamp(r['target_dt']):%d/%m}): {RETAIL_CALL.get(call, '❓ ยังไม่ชัดเจน')}")
            p, base = _probs(r, ("p_down", "p_flat", "p_up")), _f(r.get("retail_base"))
            if p:
                out.append(f"• โอกาส {p}")
            is_sig = str(r.get("model") or "").strip() == "SIG"
            alt = _probs(r, ("old_p_down", "old_p_flat", "old_p_up") if is_sig else ("sig_p_down", "sig_p_flat", "sig_p_up"))
            if alt:
                out.append(f"• {'วิธีเดิม' if is_sig else 'โมเดลสัญญาณ'} (คู่ขนาน): {alt}")
            if not is_sig:
                out.append("• คืนนี้โมเดลสัญญาณใช้ไม่ได้ จึงใช้วิธีเดิมเป็นคำทายหลัก")
            size = str(r.get("size_text") or "").strip()
            detail = ([f"ปลีกล่าสุดใน EPPO {base:.2f}"] if base is not None else []) + ([size] if size and size != "nan" else [])
            if detail:
                out.append("• " + " · ".join(detail))
            d = _f(r.get("outcome_dretail"))
            if d is not None:
                ok = str(r.get("outcome_cls") or "").strip()
                mark = "" if call not in RETAIL_CALL else " ✅" if ok == call else " ❌"
                out.append(f"• ผลจริง: {'ไม่ปรับ' if abs(d) < 0.005 else f'ปรับ {d:+.2f} บาท/ลิตร'}{mark}")
        s = PS.retail_live_stats(retail, fuel)
        if s["n"]:
            stats.append(f"{name} รู้ผล {s['n']} วัน ฟันธง {s['n_called']} ถูก {s['n_right']}")
    return out + ([f"ผลจริงสะสม: {' · '.join(stats)}"] if stats else [])


def build_message(inp: dict, log: pd.DataFrame, on_demand: bool = False, retail: pd.DataFrame = None) -> tuple:
    """(ข้อความ, รหัสจุดอ้างอิงสำหรับกันส่งซ้ำ) ; on_demand = ข้อความสำหรับตอบเมื่อพิมพ์ถาม (ใช้คำทายล่าสุดที่ยังไม่หมดอายุ)
    retail = RetailFcLog ที่ผ่าน prep_retail แล้ว (None = ไม่ใส่ส่วนราคาขายปลีก เช่น ข้อความประจำคืน)"""
    asof = pd.Timestamp(inp["asof"])
    fuels = inp.get("fuels", {})
    bad = {fuel: usable(fuels.get(fuel), asof, on_demand) for fuel, _ in FUELS}
    ok = [(fuel, name) for fuel, name in FUELS if not bad[fuel]]
    ref_id = f"{asof:%Y-%m-%d} 20:30"
    lines = ["🛢 แนวโน้มราคาส่ง (Jobber) 24 ชม."]
    if ok:
        ref = pd.Timestamp(fuels[ok[0][0]]["ref"])
        lines += [f"ข้อมูล {ref:%H:%M} น. {ref:%d/%m} · ใช้ได้ถึง {ref:%H:%M} น. {ref + pd.Timedelta(days=1):%d/%m}"]
        if ref.strftime("%H:%M") != "20:30":
            lines.append("(รอบบ่าย: มีผลจริงเพียงไม่กี่วัน เชื่อถือได้น้อยกว่ารอบ 20:30)")
        lines.append("")
    else:
        lines += [("ตอนนี้" if on_demand else f"คืนนี้ ({asof:%d/%m})") + " ไม่มีคำทาย", ""]
    unclear, timing = [], []
    for fuel, name in FUELS:
        if bad[fuel]:
            lines += [f"{name}: ไม่มีคำทาย — {bad[fuel]}", ""]
            continue
        f = fuels[fuel]
        lines += fuel_block(name, f)
        tl = timing_line(f)
        if tl:
            timing.append(f"⏰ {name}: {tl}")
        if f["h24"]["tier"] == "UNCLEAR":
            unclear.append(f)
        else:
            lines.append("")
    if unclear:
        if lines[-1] != "":
            lines.append("ราคาอาจขยับได้ทั้งสองทาง")
        ups = list(dict.fromkeys(r for f in unclear for r in _reasons(f["h24"]["reasons"], UP_MARK, 1)))
        downs = list(dict.fromkeys(r for f in unclear for r in _reasons(f["h24"]["reasons"], DOWN_MARK, 1)))
        if ups or downs:
            lines += ["", "สัญญาณคืนนี้"] + ([f"• ชี้ขึ้น: {', '.join(ups[:2])}"] if ups else []) + ([f"• ชี้ลง: {', '.join(downs[:2])}"] if downs else [])
        lines.append("")
    if ok:                                                # ช่วงเวลาซื้อของวันพรุ่งนี้ (เฉพาะคำทายรอบ 20:30 ; ไม่มีสัญญาณก็บอกข้อเท็จจริงเฉลี่ย)
        is_eve = pd.Timestamp(fuels[ok[0][0]]["ref"]).strftime("%H:%M") == "20:30"
        if is_eve:
            day = pd.Timestamp(fuels[ok[0][0]]["ref"]) + pd.Timedelta(days=1)
            lines += [f"เวลาซื้อวันที่ {day:%d/%m} [ทดลอง ยังไม่มีผลจริง]"] + (timing if timing else ["ไม่มีสัญญาณเรื่องเวลา"]) + [f"({AVG_PM_DISCOUNT})", ""]
    if retail is not None:
        lines += retail_lines(retail, asof) + [""]
    lines += last_results(log) + [""] + live_summary(log) + ["", FOOTER]
    text = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", text), ref_id


# ---------- ส่ง ----------
def valid_user_id(uid: str) -> bool:
    """รับเฉพาะรหัสผู้ใช้ของไลน์ (U + 32 ตัวอักษรฐานสิบหก) — รหัสกลุ่ม (C...) และห้อง (R...) ไม่ผ่าน"""
    return bool(USER_ID_RE.match(uid or ""))


SEND_FROM = "20:30"


def in_send_window(now: pd.Timestamp, asof: pd.Timestamp) -> bool:
    """ส่งเฉพาะคืนเดียวกับข้อมูล ตั้งแต่ 20:30 ถึงก่อนเที่ยงคืน — กันรอบที่ถูกเลื่อนมารันตอนเช้ามืดส่งข้อความผิดเวลา"""
    return now.normalize() == asof.normalize() and now.strftime("%H:%M") >= SEND_FROM


def push(text: str, token: str, uid: str) -> str:
    """คืนสถานะ 'sent' หรือ 'failed:<รหัส HTTP>' ; ไม่ throw และไม่พิมพ์ token/รหัสผู้ใช้"""
    try:
        r = requests.post("https://api.line.me/v2/bot/message/push", timeout=20,
                          headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                          json={"to": uid, "messages": [{"type": "text", "text": text[:4900]}]})
        return "sent" if r.status_code == 200 else f"failed:{r.status_code}"
    except requests.RequestException as e:
        return f"failed:{type(e).__name__}"


def log_frame(values: list) -> pd.DataFrame:
    if not values or len(values) < 2:
        return pd.DataFrame()
    head = [str(h).strip() for h in values[0]]
    df = pd.DataFrame([[(r[i] if i < len(r) else "") for i in range(len(head))] for r in values[1:]], columns=head)
    # ชีทอ่านแบบ UNFORMATTED_VALUE : วันที่/เวลามาเป็นเลขลำดับวัน (เช่น 46338.854) ต้องแปลงก่อน ไม่งั้นทุกแถวถูกมองว่าไม่ใช่รอบ 20:30
    for c, fmt in (("asof", "%Y-%m-%d %H:%M"), ("ref", "%Y-%m-%d %H:%M"), ("date", "%Y-%m-%d")):
        if c in df.columns:
            ts = pd.to_datetime(df[c].map(to_ts), errors="coerce").dt.round("min")
            df[c] = ts.dt.strftime(fmt).fillna("")
    return df


def already_sent(rows: list, ref_id: str) -> bool:
    return any(len(r) >= 3 and str(r[0]) == ref_id and str(r[2]) == "sent" for r in rows[1:])


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    dry = "--dry" in argv
    if not os.path.exists(INPUT_FILE):
        print(f"[LINE] ไม่มี {INPUT_FILE} (main.py ยังไม่ได้รัน) — ไม่ส่ง")
        return 0
    with open(INPUT_FILE, encoding="utf-8") as fh:
        inp = json.load(fh)
    tok = get_token()
    log = log_frame(sheets_get(tok, SHEET_ID, "OutlookLog!A1:R20000"))
    text, ref_id = build_message(inp, log)
    print(text)
    now = pd.Timestamp(datetime.now(ZoneInfo(TZ_NAME)).replace(tzinfo=None))
    if not dry and not in_send_window(now, pd.Timestamp(inp["asof"])):
        print(f"[LINE] นอกช่วงเวลาส่ง ({SEND_FROM}–23:59 น. ของวันเดียวกับข้อมูล) — ไม่ส่ง (เช่น รอบที่ GitHub เลื่อนมารันกลางดึก)")
        return 0
    token, uid = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", ""), os.environ.get("LINE_TEST_USER_ID", "").strip()
    if dry:
        print("[LINE] --dry : ไม่ส่ง")
        return 0
    if not token or not uid:
        print("[LINE] ยังไม่ได้ตั้ง Secret LINE_CHANNEL_ACCESS_TOKEN / LINE_TEST_USER_ID — ไม่ส่ง")
        return 0
    if not valid_user_id(uid):
        print("[LINE] LINE_TEST_USER_ID ไม่ใช่รหัสผู้ใช้ส่วนตัว (ต้องขึ้นต้นด้วย U) — ไม่ส่ง")
        return 0
    if ensure_tab(tok, SHEET_ID, LOG_TAB):
        sheets_update(tok, SHEET_ID, f"{LOG_TAB}!A1", [LOG_COLUMNS])
    sent_rows = sheets_get(tok, SHEET_ID, f"{LOG_TAB}!A1:C5000")
    if already_sent(sent_rows, ref_id):
        print(f"[LINE] ส่งของ {ref_id} ไปแล้ว — ไม่ส่งซ้ำ")
        return 0
    status = push(text, token, uid)
    sheets_append(tok, SHEET_ID, f"{LOG_TAB}!A1:D1", [[ref_id, datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y-%m-%d %H:%M"), status, text]])
    print(f"[LINE] {status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
