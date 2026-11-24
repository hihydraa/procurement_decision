"""
ส่งข้อความ "แนวโน้มราคาส่ง (Jobber) 24 ชม." (รอบ 20:30) เข้าไลน์ส่วนตัวของผู้ดูแลระบบ — ช่วงทดสอบ ยังไม่ส่งเข้ากลุ่มใด ๆ

เนื้อหา : ใช้โมเดลเดียว คือแนวโน้ม Jobber 24 ชม. จากข้อมูล 20:30 (ตัวเดียวที่ผลจริงเริ่มเข้ากับผลทดสอบย้อนหลัง)
          + เหตุผลสั้น ๆ + ผลของคำทายล่าสุดที่รู้ผลแล้ว + สถิติผลจริงสะสมจาก OutlookLog
ลำดับ : 🧭 ควรซื้อเมื่อไร (ซื้อก่อนขึ้นคืนนี้ หรือรอบ่ายพรุ่งนี้ ; ใช้ประกาศของผู้ค้าส่งเป็นเส้นตาย ไม่แสดงรายการประกาศซ้ำ) → รายละเอียด
รูปแบบ : แบ่งเป็นส่วนมีหัวข้อคั่น — ① ราคาส่ง (Jobber) พร้อมผลงานที่ผ่านมาของตัวเอง ② ราคาขายปลีก (เฉพาะข้อความตอบเมื่อพิมพ์ถาม)
          ความน่าจะเป็นแสดงเป็นแถบข้อความ ▰▱ (ช่องข้อความของไลน์ส่งรูปไม่ได้)
          ไม่รวมแนวโน้ม 48 ชม. และการ์ดต้นทุนขายส่ง (ยังไม่มีหลักฐานพอ)
          ข้อความตอบเมื่อพิมพ์ "forecast" (on_demand) มีพยากรณ์ราคาขายปลีกจาก RetailFcLog ต่อท้ายด้วย ติดป้าย [ทดลอง] — ข้อความประจำคืนไม่มี
          เพราะส่งก่อนการ์ดรอบ 21:30 ซึ่งเป็นรอบที่ออกพยากรณ์ปลีก
ความปลอดภัย : ปลายทางมาจาก Secret LINE_TEST_USER_ID เท่านั้น และต้องเป็นรหัสผู้ใช้ (ขึ้นต้น U) — รหัสกลุ่ม/ห้อง (C/R) ถูกปฏิเสธ
              ไม่มี Secret = ไม่ส่ง (ไม่ถือเป็นข้อผิดพลาด) ; ส่งครั้งเดียวต่อจุดอ้างอิง (บันทึกในแท็บ LineTestLog)

การส่งเข้าไลน์ส่วนตัว (ตั้งแต่ 20/11/2026) : ส่งเฉพาะ "แจ้งเตือนประกาศกองทุน" สั้น ๆ (fund_alert) ครั้งเดียวต่อฉบับ เมื่อคำทายรอบคืนนี้นับประกาศนั้นแล้ว (วันเดียวกับข้อมูล ไม่จำกัดเวลา)
          แทนข้อความเต็มประจำคืน — เจ้าของพิมพ์ forecast เพื่อดูการ์ด Flex เอง ; ข้อความเต็มยังพิมพ์ไว้ใน log ของ workflow

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
import retail_forecast as RF
from common import OILBOT_SHEET_ID, SHEET_ID, TABS, TZ_NAME, ensure_tab, get_token, sheets_append, sheets_get, sheets_update, to_ts

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
def make_input(outlook: dict, asof, headsup: pd.DataFrame = None, fund: pd.DataFrame = None, dispatch: dict = None) -> dict:
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
                                           "n": ev.get("n"), "right10": ev.get("right10"), "wrong10": ev.get("wrong10"), "avg_move": ev.get("avg_move"),
                                           "p_up": (h.get("p") or {}).get("r10"), "p_down": (h.get("p") or {}).get("f10")}}
    out = {"asof": pd.Timestamp(asof).strftime("%Y-%m-%d %H:%M"), "fuels": fuels}
    if headsup is not None:
        out["notices"] = notices(headsup, asof)
    if fund is not None:
        out["fund"] = fund_notice(fund, outlook, asof)
    if dispatch is not None:
        out["dispatch"] = dispatch
    return out


def write_input(outlook: dict, asof, path: str = INPUT_FILE, headsup: pd.DataFrame = None, fund: pd.DataFrame = None,
                dispatch: dict = None) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(make_input(outlook, asof, headsup, fund, dispatch), fh, ensure_ascii=False, indent=1)


# ---------- แผนส่งรถช่วงบ่าย (engine.decide_dispatch) : ตรงกับเวลาที่หน้างานตัดสินใจจริง (ดูราคา/PO 10:00–18:00 , สั่งคนขับ 15:00–20:00) ----------
DISPATCH_PM_WINDOWS = ("D12", "D15")          # 12:00–15:30 และ 15:30–20:30 ; หลัง 20:30 ใช้การ์ด "ควรโหลดเมื่อไร" แทน
DISPATCH_WINDOW_NAME = {"D06": "เช้า", "D12": "12:00–15:30", "D15": "15:30–20:30", "D20": "หลัง 20:30"}
DISPATCH_VERDICT = {"URGENT": "⚡ เร่งส่งรถ", "GRADUAL": "↗ ทยอยส่ง", "NORMAL": "➖ ส่งตามแผน", "WAIT": "⏸ ชะลอได้"}
DISPATCH_ALERT_WINDOW = "D15"                 # แจ้งเตือนเข้าไลน์ส่วนตัวราว 15:45 (Worker สั่งรันแดชบอร์ด 15:45) ครั้งเดียวต่อวัน
DISPATCH_ALERT_UNTIL = "19:00"                # หลังจากนี้รถส่วนใหญ่ออกแล้ว ไม่ส่ง (รอบที่ GitHub เลื่อนมาช้า)


def dispatch_input(decisions: dict, log: pd.DataFrame, lead: int) -> dict:
    """ย่อผล engine.decide_dispatch (ระยะเดินทาง lead) + ผลจริงของช่วงเดียวกันจาก DispatchLog → {fuel: {...}} ; ไม่มีคำแนะนำ = ไม่ใส่"""
    out = {}
    for d in (decisions or {}).values():
        if not d or d.get("action_code") in (None, "NO_DATA") or not d.get("window"):
            continue
        st = E.dispatch_live_stats(log, d["fuel"], lead, d["window"])
        live = None
        if st:
            act = lambda c, k: {"n": st["by_action"][c]["n"], "rate": st["by_action"][c][k]} if c in st["by_action"] else None
            live = {"n": st["n"], "gain": st["avg_gain_per_day"], "urgent": act("URGENT", "rise_rate"), "wait": act("WAIT", "fall_rate")}
        q = d.get("quality") or {}
        out[d["fuel"]] = {"window": d["window"], "lead": int(d.get("lead") or lead), "asof": pd.Timestamp(d["asof"]).strftime("%Y-%m-%d %H:%M"),
                          "arrive": pd.Timestamp(d["arrive"]).strftime("%Y-%m-%d %H:%M"), "action_code": d["action_code"],
                          "raw_action_code": d.get("raw_action_code"), "grade": q.get("grade"), "p_rise": d.get("p_rise"), "p_fall": d.get("p_fall"),
                          "base_rise": d.get("base_rise"), "base_fall": d.get("base_fall"), "live": live}
    return out


def dispatch_view(x: dict) -> dict:
    """ข้อความของคำแนะนำส่งรถ 1 ชนิด : {verdict, tone, why, arrive, live} — ใช้ทั้งข้อความตัวอักษร การ์ด Flex และแจ้งเตือน"""
    code, pr, pf = x["action_code"], x.get("p_rise"), x.get("p_fall")
    pct = lambda v: "-" if v is None else f"{v:.0%}"
    base = f" (ปกติ {pct(x['base_rise'])})" if x.get("base_rise") is not None else ""
    why = {"URGENT": f"ถ้ารอ 1 วัน โอกาสราคา ณ เวลาถึงคลังแพงขึ้น {pct(pr)}{base}",
           "GRADUAL": f"ถ้ารอ 1 วัน โอกาสแพงขึ้น {pct(pr)}{base} — ทยอยออกราวครึ่งหนึ่งของแผน",
           "WAIT": f"ถ้ารอ 1 วัน โอกาสถูกลง {pct(pf)} · แพงขึ้น {pct(pr)}",
           "NORMAL": "ช่วงนี้โมเดลยังไม่แม่นพอจะแนะนำให้เร่งหรือชะลอ" if x.get("grade") == "อ่อน" else "ไม่มีสัญญาณให้เร่งหรือชะลอ"}[code]
    asof, arrive = pd.Timestamp(x["asof"]), pd.Timestamp(x["arrive"])
    day = "" if arrive.normalize() == asof.normalize() else f" ({arrive:%d/%m})"
    lv = x.get("live")
    if not lv:
        live = "ผลจริงช่วงนี้: ยังไม่มี"
    else:
        parts = [f"ผลจริงช่วง {DISPATCH_WINDOW_NAME.get(x['window'], x['window'])}: {lv['n']} วัน"]
        if lv.get("gain") is not None:
            parts.append(f"ได้เปรียบเฉลี่ย {lv['gain']:+.2f} บาท/ลิตร/วัน")
        a = lv.get({"URGENT": "urgent", "WAIT": "wait"}.get(code, ""))
        if a:
            parts.append(f"{'เร่งส่ง' if code == 'URGENT' else 'ชะลอ'} {a['n']} ครั้ง ราคา{'ขึ้น' if code == 'URGENT' else 'ลง'}จริง {a['rate']:.0%}")
        live = " · ".join(parts)
    return {"verdict": DISPATCH_VERDICT[code], "tone": {"URGENT": "up", "GRADUAL": "up", "WAIT": "down"}.get(code, "flat"), "why": why,
            "arrive": f"ออก {asof:%H:%M} ถึงคลังราว {arrive:%H:%M}{day} (เดินทาง {x['lead']} ชม.)", "live": live}


def dispatch_pm(inp: dict) -> list:
    """[(fuel, name, x)] ของคำแนะนำส่งรถช่วงบ่ายที่ออก ณ ข้อมูลนี้"""
    dp = inp.get("dispatch") or {}
    return [(fuel, name, dp[fuel]) for fuel, name in FUELS if fuel in dp and dp[fuel].get("window") in DISPATCH_PM_WINDOWS]


def dispatch_lines(inp: dict) -> list:
    rows = dispatch_pm(inp)
    if not rows:
        return []
    out = [f"🚚 ส่งรถตอนนี้ดีไหม [ทดลอง] · ประเมิน {pd.Timestamp(rows[0][2]['asof']):%H:%M} น."]
    for _, name, x in rows:
        v = dispatch_view(x)
        out += [f"{name}: {v['verdict']}", f"   • {v['why']}", f"   • {v['arrive']}", f"   • {v['live']}"]
    return out + ["(ราคาที่ได้จริง = ราคา ณ เวลารถถึงคลัง เทียบกับออกรถช้าไป 1 วัน)", ""]


def dispatch_alert(inp: dict, now: pd.Timestamp) -> tuple:
    """(ข้อความสั้น, ref_id) เมื่อช่วง 15:30–20:30 ของวันนี้มีชนิดที่แนะนำเร่งส่ง/ชะลอ ; ไม่มี = (None, None) — ส่งครั้งเดียวต่อวัน"""
    rows = [(fuel, name, x) for fuel, name, x in dispatch_pm(inp)
            if x["window"] == DISPATCH_ALERT_WINDOW and x["action_code"] in ("URGENT", "WAIT")]
    if not rows:
        return None, None
    asof = pd.Timestamp(rows[0][2]["asof"])
    if now.normalize() != asof.normalize() or now.strftime("%H:%M") >= DISPATCH_ALERT_UNTIL:
        return None, None
    lines = [f"🚚 ส่งรถตอนนี้ดีไหม (ข้อมูล {asof:%H:%M} น. · ทดลอง)"]
    for _, name, x in rows:
        v = dispatch_view(x)
        lines += [f"{name}: {v['verdict']} — {v['why']}", f"   {v['live']}"]
    lines.append("พิมพ์ forecast เพื่อดูรายละเอียด")
    return "\n".join(lines), f"dispatch-{asof:%Y%m%d}-{DISPATCH_ALERT_WINDOW}"


# ---------- แจ้งเตือนเมื่อมีประกาศกองทุน (เจ้าของระบบ 20/11/2026) ----------
# flow : ประกาศ กบน. เข้ากลุ่มต้องบุญ → Worker บันทึก FundNotice และสั่งรันแดชบอร์ดทันที (ไม่ว่ากี่โมง ; ครั้งเดียวต่อฉบับ)
#        → พยากรณ์รอบคืนนี้ (อ้างอิง 20:30) ที่นับประกาศนั้นแล้ว (ก่อน 20:30 ใช้ข้อมูลเท่าที่มี ณ เวลารัน ดู engine.outlook_source)
#        → ส่งข้อความสั้นเข้าแชทส่วนตัวของเจ้าของ (ครั้งเดียวต่อฉบับ) → เจ้าของพิมพ์ forecast เอง
def fund_notice(fund: pd.DataFrame, outlook: dict, asof) -> dict | None:
    """ประกาศกองทุนที่มีผลวันถัดจากจุดอ้างอิง 20:30 และโพสต์ก่อนเวลารัน (คำทายรอบนี้นับแล้ว) ; ไม่มี/ไม่ใช่รอบ 20:30 = None"""
    asof = pd.Timestamp(asof)
    refs = [pd.Timestamp(d["ref"]) for d in (outlook or {}).values() if d.get("horizons")]
    if fund is None or fund.empty or not refs or "ts" not in fund.columns:
        return None
    ref = max(refs)
    if ref.strftime("%H:%M") != "20:30" or asof.normalize() != ref.normalize():
        return None
    eff = pd.to_datetime(fund["effective_date"], errors="coerce")
    w = fund[(eff == ref.normalize() + pd.Timedelta(days=1)) & (fund["ts"] <= asof)]
    if w.empty:
        return None
    r = w.iloc[-1]
    num = lambda k: None if pd.isna(pd.to_numeric(r.get(k), errors="coerce")) else float(pd.to_numeric(r.get(k), errors="coerce"))
    return {"issue": str(r.get("issue_no") or "").split(".")[0], "posted": pd.Timestamp(r["ts"]).strftime("%Y-%m-%d %H:%M"),
            "effective": (ref.normalize() + pd.Timedelta(days=1)).strftime("%Y-%m-%d"), "late": bool(pd.Timestamp(r["ts"]) > ref),
            "diesel_old": num("diesel_old"), "diesel_new": num("diesel_new"), "benzene_old": num("benzene_old"), "benzene_new": num("benzene_new")}


def _fund_change(name: str, old, new) -> str:
    if old is None or new is None:
        return ""
    d = new - old
    effect = "ดันราคาขึ้น" if d > 0.004 else "ดันราคาลง" if d < -0.004 else "ไม่เปลี่ยน"
    return f"{name} {d:+.2f} ({effect})"


def fund_alert(inp: dict) -> tuple:
    """(ข้อความสั้น, ref_id สำหรับกันส่งซ้ำ) หรือ (None, None) ถ้ารอบนี้ไม่มีประกาศกองทุนที่คำทายนับแล้ว"""
    f = inp.get("fund")
    if not f or not f.get("issue"):
        return None, None
    eff = pd.Timestamp(f["effective"])
    chg = [c for c in (_fund_change("ดีเซล", f.get("diesel_old"), f.get("diesel_new")),
                       _fund_change("เบนซิน", f.get("benzene_old"), f.get("benzene_new"))) if c]
    calls = []
    for fuel, name in FUELS:
        h = ((inp.get("fuels") or {}).get(fuel) or {}).get("h24")
        calls.append(f"{name} " + ("ไม่มีคำทาย" if not h else line_flex_tier(h["tier"])))
    lines = [f"📢 กองทุนฉบับที่ {f['issue']} (มีผล {eff:%d/%m})"]
    if chg:
        lines.append("กองทุน: " + " · ".join(chg))
    lines += ["ผลพยากรณ์อัปเดตแล้ว: " + " / ".join(calls), "พิมพ์ forecast เพื่อดูรายละเอียด"]
    return "\n".join(lines), f"fund-{f['issue']}-{eff:%Y%m%d}"


def latest_fund(values: list) -> dict | None:
    """ประกาศล่าสุดจากแท็บ FundNotice ของ OIL BOT (อ่านแบบ UNFORMATTED) → รูปแบบเดียวกับ fund_notice ; ใช้กับโหมดทดสอบ"""
    df = log_frame(values)
    if df.empty or "issue_no" not in df.columns:
        return None
    r = df.iloc[-1]
    num = lambda k: None if pd.isna(pd.to_numeric(r.get(k), errors="coerce")) else float(pd.to_numeric(r.get(k), errors="coerce"))
    eff = pd.to_datetime(to_ts(r.get("effective_date")), errors="coerce")
    if pd.isna(eff):
        return None
    return {"issue": str(r.get("issue_no") or "").split(".")[0], "effective": eff.strftime("%Y-%m-%d"), "late": False,
            "diesel_old": num("diesel_old"), "diesel_new": num("diesel_new"), "benzene_old": num("benzene_old"), "benzene_new": num("benzene_new")}


def test_alert(fund: dict, now: pd.Timestamp) -> str:
    """ข้อความทดสอบการส่ง (ไม่ได้พยากรณ์ใหม่) : ใช้ประกาศล่าสุดให้เห็นรูปแบบจริง"""
    text, _ = fund_alert({"fund": fund, "fuels": {}})
    body = [ln for ln in text.split("\n") if not ln.startswith("ผลพยากรณ์อัปเดตแล้ว")]
    return "\n".join([f"🧪 ทดสอบการแจ้งเตือน ({now:%d/%m %H:%M}) — ไม่ใช่ประกาศใหม่"] + body[:-1]
                     + ["ของจริงจะมีบรรทัดผลพยากรณ์ดีเซล / แก๊สโซฮอล์ 95 ด้วย", "ได้รับข้อความนี้ = ตั้งค่าส่งไลน์ถูกต้อง · พิมพ์ forecast เพื่อดูการ์ด"])


def line_flex_tier(tier: str) -> str:
    return {"UP_STRONG": "📈📈 ขึ้นแรง", "UP": "📈 ขึ้น", "DOWN": "📉 ลง", "DOWN_STRONG": "📉📉 ลงแรง"}.get(tier, "❓ ไม่ชัด")


# ---------- ประกาศปรับราคาของผู้ค้าส่งที่รู้แล้ววันนี้ (ฟีด Entry_HeadsUp) ----------
# ไม่แสดงรายการประกาศในข้อความ (ผู้ซื้อเห็นจากกลุ่มไลน์ของผู้ค้าอยู่แล้ว — เจ้าของระบบ 16/11/2026) ; ใช้เฉพาะเป็นเส้นตาย/ขนาดการขึ้นในคำแนะนำ
FUEL_GROUP = {"B7": "DIESEL", "G95": "GASOHOL"}


def _txt(v) -> str:
    s = "" if v is None else str(v).strip()
    return "" if s.lower() in ("", "nan", "none", "nat") else s


def _hhmm(v) -> str:
    """'9:00' / '23:30:00' → '09:00' / '23:30' ; อ่านไม่ได้ = ''"""
    m = re.match(r"^(\d{1,2}):(\d{2})", _txt(v))
    return f"{int(m.group(1)):02d}:{m.group(2)}" if m else ""


def parse_rates(text: str) -> dict:
    """'DIESEL:+1.00 GASOHOL:+1.00' → {'DIESEL': 1.0, 'GASOHOL': 1.0}"""
    out = {}
    for k, v in re.findall(r"([A-Z0-9]+):([+-]?\d+(?:\.\d+)?)", _txt(text)):
        out[k] = float(v)
    return out


def notices(headsup: pd.DataFrame, asof) -> list:
    """ประกาศ 'มีปรับขึ้น/ลง' ของวันเดียวกับ asof ที่โพสต์ก่อน/เท่ากับ asof รวมเป็นหนึ่งรายการต่อ (ผู้ค้า, ทิศ)
    ผู้ค้ามักโพสต์หลายข้อความต่อการปรับหนึ่งครั้ง (แจ้งก่อน → เรท → เวลา) จึงใช้เรท/เวลาล่าสุดที่ระบุ ; n = จำนวนข้อความ
    pending = ยังไม่ถึงเวลามีผล ณ asof (ไม่ระบุเวลา = ถือว่ายังไม่มีผล ; เวลาที่ระบุน้อยกว่าเวลาโพสต์ = ของวันถัดไป เช่น 00:00)"""
    asof = pd.Timestamp(asof)
    need = {"ts", "provider", "direction"}
    if headsup is None or headsup.empty or not need.issubset(headsup.columns):
        return []
    h = headsup[(headsup.ts >= asof.normalize()) & (headsup.ts <= asof) & headsup.direction.isin(["UP", "DOWN"])].sort_values("ts")
    out = []
    for (prov, direction), g in h.assign(_p=h.provider.map(_txt)).groupby(["_p", "direction"], sort=False):
        col = lambda c: [x for x in (g[c].map(_txt) if c in g.columns else []) if x]
        stated = ([_hhmm(x) for x in col("stated_time") if _hhmm(x)] or [""])[-1]
        posted = g.ts.iloc[-1]
        pending = True
        if stated:
            eff = posted.normalize() + pd.Timedelta(hours=int(stated[:2]), minutes=int(stated[3:]))
            if eff < posted - pd.Timedelta(minutes=30):          # เวลาที่ระบุอยู่ก่อนเวลาโพสต์มาก = ของวันถัดไป
                eff += pd.Timedelta(days=1)
            pending = eff > asof
        out.append({"provider": prov, "dir": direction, "at": f"{posted:%H:%M}", "n": int(len(g)), "rates": (col("rates") or [""])[-1],
                    "stated": stated, "scope": (col("scope") or ["ALL"])[-1], "pending": bool(pending)})
    return out


def pending_rise(inp: dict, fuel: str) -> dict | None:
    """ประกาศขึ้นที่ยังไม่มีผลและครอบคลุมชนิดนี้ : {'amt': เรทสูงสุดที่ประกาศ หรือ None, 'when': เวลามีผลที่เร็วที่สุด หรือ ''} ; ไม่มี = None"""
    grp = FUEL_GROUP[fuel]
    hits = [n for n in (inp.get("notices") or []) if n["dir"] == "UP" and n["pending"]
            and (n["scope"] in ("ALL", grp) or grp in parse_rates(n["rates"]))]
    if not hits:
        return None
    amts = [parse_rates(n["rates"]).get(grp) for n in hits if parse_rates(n["rates"]).get(grp) is not None]
    whens = sorted((n["stated"] for n in hits if n["stated"]), key=lambda t: (t < "12:00", t))      # 23:30 มาก่อน 00:00 ของวันถัดไป
    return {"amt": max(amts) if amts else None, "when": whens[0] if whens else ""}


# ---------- ข้อความ ----------
def _strip(reason: str) -> str:
    return reason.split(" → ")[0].strip()


def _reasons(reasons: list, mark: str, k: int) -> list:
    return [_strip(r) for r in reasons if mark in r][:k]


def early_note(inp: dict, ref: pd.Timestamp) -> str:
    """คำทายรอบ 20:30 ที่ออกก่อน 20:30 (ประกาศกองทุนออกแล้ว) : บอกว่าใช้ข้อมูลถึงเวลาไหน และจะอัปเดตเมื่อไร ; ไม่ใช่กรณีนี้ = ''"""
    asof = pd.Timestamp(inp["asof"])
    if asof >= ref or asof.normalize() != ref.normalize():
        return ""
    return f"ออกล่วงหน้าเพราะประกาศกองทุนออกแล้ว · ใช้ข้อมูลถึง {asof:%H:%M} น. · อัปเดตอีกครั้งหลัง 20:30 น."


def usable(f: dict, asof: pd.Timestamp, on_demand: bool = False) -> str:
    """'' ถ้าใช้ส่งได้ ; ไม่งั้นคืนเหตุผล — ข้อความประจำคืน : ต้องเป็นคำทายจากข้อมูล 20:30 ของวันนี้ ไม่ค้าง และมีผลช่วง 24 ชม.
    on_demand (ตอบเมื่อพิมพ์ถาม) : ใช้คำทายล่าสุดที่ยังไม่หมดอายุ 24 ชม. ได้ (เช่น ของเมื่อคืน หรือรอบบ่าย 15:30)"""
    if not f:
        return "ไม่มีผลของชนิดนี้"
    ref = pd.Timestamp(f["ref"])
    if f.get("h24") is None:
        return f.get("note") or "ระบบยังไม่ให้แนวโน้ม (ข้อมูลไม่พร้อม)"
    if on_demand:
        early = asof < ref and asof.normalize() == ref.normalize()   # ประกาศกองทุนออกก่อน 20:30 → ทายรอบคืนนี้ล่วงหน้า (engine.outlook_source)
        return "" if early or pd.Timedelta(0) <= asof - ref < pd.Timedelta(hours=24) else f"คำทายล่าสุด ({ref:%d/%m %H:%M}) หมดอายุแล้ว"
    if ref.strftime("%H:%M") != "20:30" or ref.normalize() != asof.normalize() or f.get("stale"):
        return f"ยังไม่มีข้อมูลรอบ 20:30 ของวันนี้ (ล่าสุด {ref:%d/%m %H:%M})"
    return ""


SEP = "━━━━━━━━━━━━━━"
BAR_N = 10


def bar(p: float, n: int = BAR_N) -> str:
    """แถบสัดส่วนแบบข้อความ (ไลน์ส่งรูปไม่ได้ในช่องข้อความ) : 0.62 → ▰▰▰▰▰▰▱▱▱▱ ; ค่าที่มากกว่า 0 ได้อย่างน้อย 1 ช่อง"""
    p = min(max(float(p), 0.0), 1.0)
    k = min(n, max(int(round(p * n)), 1 if p > 0 else 0))
    return "▰" * k + "▱" * (n - k)


def section(title: str, sub: str) -> list:
    return [SEP, title, sub, SEP]


def jobber_bars(h: dict) -> list:
    """โอกาสตามโมเดลที่ราคาส่งจะขยับเกิน 10 สต./ลิตร ใน 24 ชม. (ขึ้น / ลง) — ไม่มีค่าในข้อมูลเข้า = ไม่แสดง"""
    pu, pd_ = h.get("p_up"), h.get("p_down")
    if pu is None or pd_ is None:
        return []
    return [f"⬆ ขึ้น {bar(pu)} {pu:.0%}", f"⬇ ลง  {bar(pd_)} {pd_:.0%}"]


def fuel_block(name: str, f: dict) -> list:
    h = f["h24"]
    tier = h["tier"]
    if tier == "UNCLEAR":
        return [f"{name}: ❓ ไม่มีสัญญาณชัด"] + jobber_bars(h)
    out = [f"{name}: {TIER_TEXT[tier]}"] + jobber_bars(h) + [f"ความเชื่อมั่น: {h.get('grade') or 'ต่ำ'}"]
    out += [f"• {r}" for r in _reasons(h["reasons"], UP_MARK if tier.startswith("UP") else DOWN_MARK, 2)]
    if h.get("n") and h.get("right10") is not None:
        out.append(f"ย้อนหลังเมื่อระบบบอกแบบนี้ ({int(h['n'])} ครั้ง): ถูก {h['right10']:.0%} ผิดทิศ {h['wrong10']:.0%}")
    return out


# ค่าเริ่มต้นเมื่อไม่มีสัญญาณ = โหลดคืนนี้ : exp_lead_time.py (17/11/2026 ; walk-forward 645 วัน) — ในวันที่โมเดลไม่ฟันธง
# ราคา 16:30 วันถัดไปสูงกว่า 22:00 คืนนั้นเฉลี่ย +0.08 (ดีเซล) / +0.05 (G95) บาท/ลิตร ; นโยบาย "รอเฉพาะเมื่อทายลง นอกนั้นโหลดคืนนี้"
# ถูกกว่าโหลดคืนนี้ทุกวัน 0.10 / 0.06 บาท/ลิตร และถูกกว่ารอบ่ายทุกวัน 0.13 / 0.07
DEFAULT_NOTE = {"B7": "ไม่มีสัญญาณว่าราคาจะลง — ย้อนหลังวันแบบนี้ราคาบ่ายวันถัดไปแพงกว่าคืนนี้เฉลี่ย 0.08 บาท/ลิตร",
                "G95": "ไม่มีสัญญาณว่าราคาจะลง — ย้อนหลังวันแบบนี้ราคาบ่ายวันถัดไปแพงกว่าคืนนี้เฉลี่ย 0.05 บาท/ลิตร"}


def advice_line(f: dict, rise: dict | None) -> str:
    """คำตอบของคำถามที่ต้องตัดสินใจจริง : ซื้อก่อนราคาขึ้นคืนนี้ หรือรอช่วงบ่ายพรุ่งนี้ — เทียบกับราคา ณ จุดอ้างอิง 20:30 (ก่อนการขึ้นคืนนี้)
    หลักฐานฝั่ง "รอ"   : แนวโน้ม 24 ชม. ลง/ลงแรง หรือโมเดลรายช่วงเวลาบอกว่าราคา 16:30 พรุ่งนี้ต่ำกว่าคืนนี้ (เฉพาะทิศที่ผ่านเกณฑ์ walk-forward)
    หลักฐานฝั่ง "รีบ" : แนวโน้ม 24 ชม. ขึ้น/ขึ้นแรง หรือราคา 16:30 พรุ่งนี้ยังสูงกว่าคืนนี้ ; ขึ้นภายในเที่ยงอย่างเดียว = รีบแบบอ่อน
    กติกา (16/11/2026 ; ปรับ 17/11 หลังรู้ว่ารถใช้เวลา 10–12 ชม. แต่รอที่คลังข้ามคืนได้ → คำแนะนำนี้สำหรับรถที่อยู่คลังช่วงค่ำ) :
       ฝั่งเดียว = แนะนำฝั่งนั้น ; สองฝั่ง = บอกว่าขัดกัน ; ไม่มีเลย = โหลดคืนนี้ (ค่าเริ่มต้น ดู DEFAULT_NOTE)
    rise = ประกาศขึ้นที่ยังไม่มีผล (pending_rise) ใช้บอกเส้นตายและเตือนไม่ให้ซื้อช่วงเช้าหลังราคาขึ้น ไม่ใช้เป็นหลักฐานของทิศสุทธิ"""
    pf = {p["hours"]: p for p in (f.get("profile") or []) if p.get("call") and p.get("ok")}
    pct = lambda p: f"ย้อนหลังถูก {p['right']:.0%} จาก {int(p['n'])} ครั้ง" if p.get("right") is not None and p.get("n") else "ผ่านเกณฑ์ย้อนหลัง"
    noon, pm = pf.get(16), pf.get(20)
    tier = (f.get("h24") or {}).get("tier") or "UNCLEAR"
    grade = (f.get("h24") or {}).get("grade") or "ต่ำ"
    mv = (f.get("h24") or {}).get("avg_move")              # ขยับเฉลี่ยตามทิศที่ทาย จาก walk-forward (รวมครั้งที่ทายพลาด) = ส่วนต่างที่คาดได้
    gain = f" · ย้อนหลังราคาต่างจากคืนนี้เฉลี่ย {abs(mv):.2f} บาท/ลิตร" if mv is not None and mv == mv and mv > 0 else ""
    wait, buy = [], []
    if tier.startswith("DOWN"):
        wait.append(f"แนวโน้ม 24 ชม. {CALL_WORD[tier]} (ความเชื่อมั่น {grade}){gain}")
    if tier.startswith("UP"):
        buy.append(f"แนวโน้ม 24 ชม. {CALL_WORD[tier]} (ความเชื่อมั่น {grade}){gain}")
    if pm and pm["call"] == "DOWN":
        wait.append(f"ราคา 16:30 น. พรุ่งนี้มักต่ำกว่าคืนนี้ ({pct(pm)})")
    if pm and pm["call"] == "UP":
        buy.append(f"ราคา 16:30 น. พรุ่งนี้มักยังสูงกว่าคืนนี้ ({pct(pm)})")
    if noon and noon["call"] == "UP" and not wait and not buy:
        buy.append(f"ราคามักขึ้นภายในเที่ยงพรุ่งนี้ ({pct(noon)})")
    amt = f" {rise['amt']:+.2f}" if rise and rise["amt"] is not None else ""
    at = f" เวลา {rise['when']} น." if rise and rise["when"] else ""
    before = f"ก่อน {rise['when']} น. คืนนี้" if rise and rise["when"] else "คืนนี้"
    why = lambda xs: [f"   • {x}" for x in xs]
    known = [f"   ⚠ คืนนี้ราคาจะขึ้น{amt}{at} ตามประกาศ"] if rise else []
    load = f"🛒 โหลด{before}" + (f" ก่อนราคาขึ้น{amt}" if rise else "")
    if wait and not buy:
        out = ["⏳ รอโหลดบ่ายพรุ่งนี้ (หลัง 16:00 น.)"] + why(wait)
        if rise:
            out.append(known[0] + " — คำแนะนำนี้มองราคาสุทธิถึงบ่ายพรุ่งนี้ จึงให้เลี่ยงโหลดช่วงเช้าหลังราคาขึ้น")
    elif buy and not wait:
        out = [load] + why(buy)
    elif buy and wait:
        out = ["⚖ สัญญาณขัดกัน ยังไม่แนะนำ"] + why([f"ชี้ให้โหลดคืนนี้: {x}" for x in buy] + [f"ชี้ให้รอ: {x}" for x in wait]) + known
    else:                                                  # ไม่มีสัญญาณ : ค่าเริ่มต้น = โหลดคืนนี้ (ดู DEFAULT_NOTE)
        out = [load, f"   • {DEFAULT_NOTE[f.get('fuel_code', '')]}" if f.get("fuel_code") in DEFAULT_NOTE else "   • ไม่มีสัญญาณว่าราคาจะลง"]
    # ข้อความนี้ถูกอ่านได้ถึง 24 ชม. หลังออก (เช่น พิมพ์ถามเช้าวันถัดไป) จึงระบุวันที่แทนคำว่า คืนนี้/พรุ่งนี้
    ref = pd.Timestamp(f["ref"])
    text = re.sub(r"พรุ่งนี้(?=[^\s:,.)])", f"วันที่ {ref + pd.Timedelta(days=1):%d/%m} ", "\n".join(out)).replace("พรุ่งนี้", f"วันที่ {ref + pd.Timedelta(days=1):%d/%m}")
    return re.sub(r"คืนนี้(?=[^\s:,.)])", f"คืน {ref:%d/%m} ", text).replace("คืนนี้", f"คืน {ref:%d/%m}")


def advice_lines(inp: dict, ok: list) -> list:
    """ส่วน 'ควรซื้อเมื่อไร' : เฉพาะคำทายรอบ 20:30 (โมเดลรายช่วงเวลามีเฉพาะรอบนี้)"""
    fuels = inp.get("fuels", {})
    if not ok or pd.Timestamp(fuels[ok[0][0]]["ref"]).strftime("%H:%M") != "20:30":
        return []
    ref = pd.Timestamp(fuels[ok[0][0]]["ref"])
    day = ref + pd.Timedelta(days=1)
    same_night = pd.Timestamp(inp["asof"]).normalize() == ref.normalize()      # อ่านวันถัดไป : ประกาศในข้อมูลเข้าเป็นของวันใหม่ และช่วงซื้อก่อนขึ้นผ่านไปแล้ว
    out = [f"🧭 รถที่อยู่คลัง: โหลดคืน {ref:%d/%m} หรือรอบ่าย {day:%d/%m} [ทดลอง ยังไม่มีผลจริง]"]
    if not same_night:
        out.append(f"(ออกเมื่อ 20:30 น. {ref:%d/%m} — ช่วงโหลดก่อนราคาขึ้นของคืนนั้นผ่านไปแล้ว)")
    for fuel, name in ok:
        out.append(f"{name}: {advice_line(dict(fuels[fuel], fuel_code=fuel), pending_rise(inp, fuel) if same_night else None)}")
    return out + [f"(เทียบกับราคา ณ 20:30 น. {ref:%d/%m} · สำหรับรถที่ถึงคลังแล้ว ไม่รวมโควต้าและเวลาตัดรับออเดอร์ของคลัง)", ""]


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
    return ([f"สถิติผลจริงตั้งแต่ {first:%d/%m} ({days}/{READY_DAYS} วัน)", f"ฟันธง {calls} ครั้ง · ถูก {right} · ผิดทิศ {wrong} · ราคานิ่ง {flat}"]
            + ([f"ถูก {bar(right / calls)} {right}/{calls}"] if calls else []))


def outlook_counts(log: pd.DataFrame, ref_hhmm: str) -> dict | None:
    """สรุปผลจริงของแนวโน้ม 24 ชม. รอบ ref_hhmm รวมสองชนิด : {days, calls, right, wrong, flat, first} ; ไม่มีผล = None"""
    calls = right = wrong = flat = days = 0
    first = None
    for fuel, _ in FUELS:
        d = E._outlook_scored(log, fuel, 24, ref_hhmm)
        if d is None:
            continue
        days = max(days, len(d))
        t0 = pd.to_datetime(d["ref"], errors="coerce").min()
        first = t0 if first is None else min(first, t0)
        c = d[d["tier"] != "UNCLEAR"]
        mv = c["outcome_delta"] * c["tier"].map(lambda t: 1.0 if str(t).startswith("UP") else -1.0)
        calls, right, wrong = calls + len(c), right + int((mv >= M.OUTLOOK_SMALL).sum()), wrong + int((mv <= -M.OUTLOOK_SMALL).sum())
        flat += int((mv.abs() < M.OUTLOOK_SMALL).sum())
    return None if first is None else {"days": days, "calls": calls, "right": right, "wrong": wrong, "flat": flat, "first": first}


def pm_summary(log: pd.DataFrame) -> list:
    """ผลจริงรอบ 15:30 แยกจากรอบ 20:30 (คนละโมเดล) — รอบนี้ตรงกับเวลาตัดสินใจของหน้างาน"""
    c = outlook_counts(log, "15:30")
    if not c:
        return ["รอบ 15:30: ยังไม่มีผลจริง"]
    return [f"รอบ 15:30 ({c['days']}/{READY_DAYS} วัน): ฟันธง {c['calls']} · ถูก {c['right']} · ผิดทิศ {c['wrong']} · ราคานิ่ง {c['flat']}"]


# ---------- ราคาขายปลีก (ทดลอง) : เฉพาะข้อความตอบเมื่อพิมพ์ถาม ----------
RETAIL_CALL = {"ลง": "📉 น่าจะปรับลง", "ขึ้น": "📈 น่าจะปรับขึ้น", "นิ่ง": "➖ น่าจะไม่ปรับ"}
RETAIL_NONE = "ยังไม่มีพยากรณ์ของวันถัดไป (ออกหลัง 21:30 น. คืนจันทร์–พฤหัสบดี)"


def _f(v):
    try:
        x = float(v)
        return None if x != x else x
    except (TypeError, ValueError):
        return None


def retail_lines(retail: pd.DataFrame, asof: pd.Timestamp) -> list:
    """ส่วนราคาขายปลีกของข้อความ จาก RetailFcLog ที่ผ่าน procurement_summary.prep_retail (มี asof_dt, target_dt)
    ใช้แถวล่าสุดของแต่ละชนิด เฉพาะที่วันเป้าหมายยังไม่ผ่าน (วันนี้หรือพรุ่งนี้) ; วันเป้าหมาย = วันนี้และรู้ผลแล้ว จะบอกผลจริงด้วย
    บรรทัดหลัก = คำทายหลักของแถว (p_*/call) : model = SIG คือโมเดลสัญญาณ Jobber และวิธีเดิม (old_*) เป็นบรรทัดคู่ขนาน
    แถวเก่า/แถวที่โมเดลสัญญาณใช้ไม่ได้ (model ≠ SIG) : วิธีเดิมเป็นหลัก และโมเดลสัญญาณ (sig_*) เป็นบรรทัดคู่ขนานถ้ามี"""
    import procurement_summary as PS
    today, stats = pd.Timestamp(asof).normalize(), []
    # หัวข้อบอกวันที่พยากรณ์จริง : พยากรณ์ออกคืนก่อนวันเป้าหมาย จึงเป็น "พรุ่งนี้" เฉพาะหลังการ์ดรอบ 21:30 ; ถามตอนกลางวันจะเป็นของ "วันนี้"
    tgts = [pd.Timestamp(r["target_dt"]).normalize() for r in (PS.latest_retail(retail, fuel) for fuel, _ in FUELS) if r]
    tgt = max([t for t in tgts if t >= today], default=None)
    when = ("รอพยากรณ์ของวันถัดไป" if tgt is None else f"วันนี้ ({tgt:%d/%m})" if tgt == today
            else f"พรุ่งนี้ ({tgt:%d/%m})" if tgt == today + pd.Timedelta(days=1) else f"วันที่ {tgt:%d/%m}")
    out = section(f"② ราคาขายปลีก (หน้าปั๊ม) · {when}", "ราคาหน้าปั๊มจะปรับหรือไม่ [ทดลอง ยังไม่ผ่านการทดสอบเดินหน้า]")
    for fuel, name in FUELS:
        r = PS.latest_retail(retail, fuel)
        if not r or pd.Timestamp(r["target_dt"]).normalize() < today:
            last = f" ; ล่าสุดเป็นของวันที่ {pd.Timestamp(r['target_dt']):%d/%m}" if r else ""
            out.append(f"{name}: {RETAIL_NONE}{last}")
        else:
            call = str(r.get("call") or "").strip()
            out.append(f"{name} (วันที่ {pd.Timestamp(r['target_dt']):%d/%m}): {RETAIL_CALL.get(call, '❓ ยังไม่ชัดเจน')}")
            p, base = [_f(r.get(k)) for k in ("p_down", "p_flat", "p_up")], _f(r.get("retail_base"))
            ev = RF.elevated(fuel, call, p[0], p[2], r.get("model"))
            if ev:
                out.append(RF.elevated_text(ev))
            if None not in p:
                out += [f"⬇ ลง      {bar(p[0])} {p[0]:.0%}", f"➖ ไม่ปรับ {bar(p[1])} {p[1]:.0%}", f"⬆ ขึ้น     {bar(p[2])} {p[2]:.0%}"]
            model = str(r.get("model") or "").strip()
            if model == "OLD":                               # แถวก่อนการสลับโมเดล (model ว่าง) ไม่ใส่หมายเหตุนี้
                out.append("• คืนนี้โมเดลสัญญาณใช้ไม่ได้ จึงใช้วิธีเดิมเป็นคำทายหลัก")
            why = str(r.get("sig_reason") or "").strip()
            if why and why != "nan" and model == "SIG":
                out.append(f"• เหตุผล: {why}")
            size = str(r.get("size_text") or "").strip()
            detail = ([f"ปลีกล่าสุดใน EPPO {base:.2f}"] if base is not None else []) + ([size] if size and size != "nan" else [])
            if detail:
                out.append("• " + " · ".join(detail))
            d = _f(r.get("outcome_dretail"))
            if d is not None:
                ok = str(r.get("outcome_cls") or "").strip()
                mark = "" if call not in RETAIL_CALL else " ✅" if ok == call else " ❌"
                out.append(f"• ผลจริง: {'ไม่ปรับ' if abs(d) < 0.005 else f'ปรับ {d:+.2f} บาท/ลิตร'}{mark}")
            out.append("")
        s = PS.retail_live_stats(retail, fuel)
        if s["n"]:
            stats.append(f"{name} รู้ผล {s['n']} วัน ฟันธง {s['n_called']} ถูก {s['n_right']}")
    return out + ([f"ผลจริงสะสม (ราคาปลีก): {' · '.join(stats)}"] if stats else [])


def build_message(inp: dict, log: pd.DataFrame, on_demand: bool = False, retail: pd.DataFrame = None) -> tuple:
    """(ข้อความ, รหัสจุดอ้างอิงสำหรับกันส่งซ้ำ) ; on_demand = ข้อความสำหรับตอบเมื่อพิมพ์ถาม (ใช้คำทายล่าสุดที่ยังไม่หมดอายุ)
    retail = RetailFcLog ที่ผ่าน prep_retail แล้ว (None = ไม่ใส่ส่วนราคาขายปลีก เช่น ข้อความประจำคืน)"""
    asof = pd.Timestamp(inp["asof"])
    fuels = inp.get("fuels", {})
    bad = {fuel: usable(fuels.get(fuel), asof, on_demand) for fuel, _ in FUELS}
    ok = [(fuel, name) for fuel, name in FUELS if not bad[fuel]]
    ref_id = f"{asof:%Y-%m-%d} 20:30"
    adv = advice_lines(inp, ok)
    lines = ["🛢 พยากรณ์ราคาน้ำมัน"] + (adv or (dispatch_lines(inp) if on_demand else [])) + section(("① " if retail is not None else "") + "ราคาส่ง (Jobber) · 24 ชม.ข้างหน้า", "ราคาต่ำสุดที่ผู้ค้าส่งประกาศหน้าคลัง จะขึ้นหรือลง")
    if ok:
        ref = pd.Timestamp(fuels[ok[0][0]]["ref"])
        lines += [f"ข้อมูล {ref:%H:%M} น. {ref:%d/%m} · ใช้ได้ถึง {ref:%H:%M} น. {ref + pd.Timedelta(days=1):%d/%m}"]
        if ref.strftime("%H:%M") != "20:30":
            lines.append("(รอบบ่าย: มีผลจริงเพียงไม่กี่วัน เชื่อถือได้น้อยกว่ารอบ 20:30)")
        if early_note(inp, ref):
            lines.append(f"({early_note(inp, ref)})")
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
    lines += ["ผลงานที่ผ่านมา (ราคาส่ง Jobber)"] + last_results(log) + [""] + live_summary(log) + pm_summary(log) + [""]
    if retail is not None:
        lines += retail_lines(retail, asof) + [""]
    lines += [FOOTER]
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


def retail_frame(values: list) -> pd.DataFrame:
    """RetailFcLog จาก sheets_get (UNFORMATTED_VALUE : วันที่เป็นเลขลำดับวัน) → รูปแบบเดียวกับ procurement_summary.prep_retail"""
    import procurement_summary as PS
    df = log_frame(values)
    if df.empty or "target_day" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["target_day"] = pd.to_datetime(df["target_day"].map(to_ts), errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    return PS.prep_retail(df)


def already_sent(rows: list, ref_id: str) -> bool:
    return any(len(r) >= 3 and str(r[0]) == ref_id and str(r[2]) == "sent" for r in rows[1:])


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    dry = "--dry" in argv
    if "--test" in argv:
        return main_test(dry)
    if not os.path.exists(INPUT_FILE):
        print(f"[LINE] ไม่มี {INPUT_FILE} (main.py ยังไม่ได้รัน) — ไม่ส่ง")
        return 0
    with open(INPUT_FILE, encoding="utf-8") as fh:
        inp = json.load(fh)
    tok = get_token()
    log = log_frame(sheets_get(tok, SHEET_ID, "OutlookLog!A1:R20000"))
    retail = None
    try:                                       # ราคาขายปลีก : การ์ดรอบ 21:30 เขียน RetailFcLog ก่อนขั้นนี้ ; อ่านไม่ได้ = ข้อความไม่มีส่วนนี้
        retail = retail_frame(sheets_get(tok, SHEET_ID, f"{TABS['RETAIL_FC']}!A1:AF5000"))
    except Exception as e:                     # noqa: BLE001
        print(f"[LINE] อ่าน RetailFcLog ไม่สำเร็จ: {e}")
    print(build_message(inp, log, retail=retail)[0])                 # ข้อความเต็มพิมพ์ไว้ใน log (คำสั่ง forecast ใช้ outlook.json)
    now = pd.Timestamp(datetime.now(ZoneInfo(TZ_NAME)).replace(tzinfo=None))
    alerts = []
    text, ref_id = fund_alert(inp)                                   # ประกาศกองทุน (เจ้าของระบบ 20/11/2026)
    if not text:
        print("[LINE] รอบนี้ไม่มีประกาศกองทุนที่คำทายนับแล้ว")
    elif not dry and now.normalize() != pd.Timestamp(inp["asof"]).normalize():
        print("[LINE] ประกาศกองทุน: ไม่ใช่วันเดียวกับข้อมูล — ไม่ส่ง (เช่น รอบที่ GitHub เลื่อนมารันหลังเที่ยงคืน)")
    else:
        alerts.append((text, ref_id))
    text, ref_id = dispatch_alert(inp, now)                          # แผนส่งรถช่วงบ่าย : เร่งส่ง/ชะลอ (เจ้าของระบบ 24/11/2026)
    if text:
        alerts.append((text, ref_id))
    else:
        print("[LINE] แผนส่งรถช่วง 15:30–19:00: ไม่มีคำแนะนำเร่งส่ง/ชะลอ (หรือนอกเวลา)")
    if not alerts:
        return 0
    for text, _ in alerts:
        print(text)
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
    for text, ref_id in alerts:
        if already_sent(sent_rows, ref_id):
            print(f"[LINE] ส่งของ {ref_id} ไปแล้ว — ไม่ส่งซ้ำ")
            continue
        status = push(text, token, uid)
        sheets_append(tok, SHEET_ID, f"{LOG_TAB}!A1:D1", [[ref_id, datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y-%m-%d %H:%M"), status, text]])
        print(f"[LINE] {ref_id}: {status}")
    return 0


def main_test(dry: bool = False) -> int:
    """python line_outlook.py --test : ส่งข้อความทดสอบจากประกาศกองทุนล่าสุดเข้าไลน์ส่วนตัว (workflow line_test.yml สั่งด้วยมือ)"""
    tok = get_token()
    fund = latest_fund(sheets_get(tok, OILBOT_SHEET_ID, "FundNotice!A1:K5000"))
    if not fund:
        print("[LINE] ไม่พบประกาศกองทุนในแท็บ FundNotice — ไม่ส่ง")
        return 1
    now = pd.Timestamp(datetime.now(ZoneInfo(TZ_NAME)).replace(tzinfo=None))
    text = test_alert(fund, now)
    print(text)
    token, uid = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", ""), os.environ.get("LINE_TEST_USER_ID", "").strip()
    if dry:
        print("[LINE] --dry : ไม่ส่ง")
        return 0
    if not token or not uid:
        print("[LINE] ยังไม่ได้ตั้ง Secret LINE_CHANNEL_ACCESS_TOKEN / LINE_TEST_USER_ID")
        return 1
    if not valid_user_id(uid):
        print("[LINE] LINE_TEST_USER_ID ไม่ใช่รหัสผู้ใช้ส่วนตัว (ต้องขึ้นต้นด้วย U ตามด้วย 32 ตัวอักษร)")
        return 1
    status = push(text, token, uid)
    print(f"[LINE] {status}")
    return 0 if status == "sent" else 1


if __name__ == "__main__":
    sys.exit(main())
