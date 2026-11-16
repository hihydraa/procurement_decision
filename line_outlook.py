"""
ส่งข้อความ "แนวโน้มราคาหน้าคลัง 24 ชม." (รอบ 20:30) เข้าไลน์ส่วนตัวของผู้ดูแลระบบ — ช่วงทดสอบ ยังไม่ส่งเข้ากลุ่มใด ๆ

เนื้อหา : ใช้โมเดลเดียว คือแนวโน้ม Jobber 24 ชม. จากข้อมูล 20:30 (ตัวเดียวที่ผลจริงเริ่มเข้ากับผลทดสอบย้อนหลัง)
          + เหตุผลสั้น ๆ + ผลของคำทายล่าสุดที่รู้ผลแล้ว + สถิติผลจริงสะสมจาก OutlookLog
          ไม่รวมแนวโน้ม 48 ชม. การ์ดต้นทุนขายส่ง และราคาขายปลีก (ยังไม่มีหลักฐานพอ)
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


def build_message(inp: dict, log: pd.DataFrame, on_demand: bool = False) -> tuple:
    """(ข้อความ, รหัสจุดอ้างอิงสำหรับกันส่งซ้ำ) ; on_demand = ข้อความสำหรับตอบเมื่อพิมพ์ถาม (ใช้คำทายล่าสุดที่ยังไม่หมดอายุ)"""
    asof = pd.Timestamp(inp["asof"])
    fuels = inp.get("fuels", {})
    bad = {fuel: usable(fuels.get(fuel), asof, on_demand) for fuel, _ in FUELS}
    ok = [(fuel, name) for fuel, name in FUELS if not bad[fuel]]
    ref_id = f"{asof:%Y-%m-%d} 20:30"
    lines = ["🛢 แนวโน้มราคาหน้าคลัง 24 ชม."]
    if ok:
        ref = pd.Timestamp(fuels[ok[0][0]]["ref"])
        lines += [f"ข้อมูล {ref:%H:%M} น. {ref:%d/%m} · ใช้ได้ถึง {ref:%H:%M} น. {ref + pd.Timedelta(days=1):%d/%m}"]
        if ref.strftime("%H:%M") != "20:30":
            lines.append("(รอบบ่าย: มีผลจริงเพียงไม่กี่วัน เชื่อถือได้น้อยกว่ารอบ 20:30)")
        lines.append("")
    else:
        lines += [("ตอนนี้" if on_demand else f"คืนนี้ ({asof:%d/%m})") + " ไม่มีคำทาย", ""]
    unclear = []
    for fuel, name in FUELS:
        if bad[fuel]:
            lines += [f"{name}: ไม่มีคำทาย — {bad[fuel]}", ""]
            continue
        f = fuels[fuel]
        lines += fuel_block(name, f)
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
