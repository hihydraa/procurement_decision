"""
Generates the daily "Executive Summary" NEWS entry via the Claude API, replacing the manual
step of pasting numbers into ChatGPT/Claude by hand and copying the reply back into the sheet.

Feeds the model the same computed snapshot main.py itself would show (NYMEX/WTI/MOPS deltas,
EPPO retail changes, Oil Fund status) so the summary is grounded in real numbers already in
the sheet, not invented. Matches the existing house style seen in past manual entries: a
"🛢️ Executive Summary (24 ชม. ล่าสุด)" heading, short bullet points each tagged
Bullish/Bearish/Neutral, and a one-line closing verdict.
"""
import os
from datetime import datetime, date

import requests

from common import TABS, SHEET_ID, get_token, sheets_get, sheets_append, col_to_dicts, excel_serial_to_date, TZ_NAME
from zoneinfo import ZoneInfo

TZ = ZoneInfo(TZ_NAME)
ANTHROPIC_MODEL = "claude-sonnet-5"


def last_n(tok, tab_key, n=4, date_col_idx=0):
    rows = sheets_get(tok, SHEET_ID, f"{TABS[tab_key]}!A1:Z5000")
    dicts = col_to_dicts(rows)
    return dicts[-n:] if len(dicts) >= n else dicts


def snapshot_lines(tok):
    lines = []

    nymex = last_n(tok, "ENTRY_NYMEX", 4)
    if nymex:
        latest, prev = nymex[-1], (nymex[-2] if len(nymex) > 1 else None)
        chg = (latest["Price"] - prev["Price"]) if prev else None
        lines.append(f"NYMEX Singapore Gasoil: {latest['Price']}" + (f" (เปลี่ยนจากวันก่อนหน้า {chg:+.2f})" if chg is not None else ""))

    wti = last_n(tok, "ENTRY_WTI", 4)
    if wti:
        latest, prev = wti[-1], (wti[-2] if len(wti) > 1 else None)
        chg = (latest["Price"] - prev["Price"]) if prev else None
        lines.append(f"WTI Crude: {latest['Price']}" + (f" (เปลี่ยนจากวันก่อนหน้า {chg:+.2f})" if chg is not None else ""))

    mops = last_n(tok, "ENTRY_MOPS", 6)
    for fuel in ("G95", "DS"):
        fr = [r for r in mops if r.get("Oil Type") == fuel]
        if fr:
            latest = fr[-1]
            prev = fr[-2] if len(fr) > 1 else None
            chg = (latest["Price (USD/BBL)"] - prev["Price (USD/BBL)"]) if prev else None
            lines.append(f"MOPS {fuel}: {latest['Price (USD/BBL)']} USD/BBL" + (f" (เปลี่ยน {chg:+.2f})" if chg is not None else ""))

    eppo = last_n(tok, "ENTRY_EPPO", 20)
    for fuel in ("GASOHOL95 E10", "H-DIESEL"):
        fr = [r for r in eppo if r.get("oil type") == fuel]
        if fr:
            latest = fr[-1]
            mm = latest.get("MARKETING MARGIN")
            mm_str = f"{mm:.2f}" if isinstance(mm, (int, float)) else mm
            lines.append(f"ราคาขายปลีก {fuel}: {latest.get('RETAIL')} บาท/ลิตร, ค่าการตลาด {mm_str} บาท/ลิตร")

    fund = last_n(tok, "ENTRY_OILFUND_SUSTAINABILITY", 2)
    if fund:
        latest = fund[-1]
        lines.append(
            f"ฐานะกองทุนน้ำมันสุทธิ: {latest.get('Total_Balance (ล้านบาท)')} ล้านบาท, "
            f"เงินสดคงเหลือ {latest.get('Cash_Remaining (ล้านบาท)')} ล้านบาท, "
            f"Runway {latest.get('Runway_Days')} วัน, สถานะ {latest.get('Status')}"
        )

    return lines


def build_prompt(lines):
    facts = "\n".join(f"- {l}" for l in lines)
    return f"""คุณคือนักวิเคราะห์ตลาดน้ำมันที่เขียนสรุปสถานการณ์ประจำวันให้ทีมจัดซื้อขององค์กร ใช้ตัวเลขจริงต่อไปนี้เท่านั้น ห้ามสมมติตัวเลขหรือเหตุการณ์ข่าวที่ไม่ได้ให้มา:

{facts}

เขียนสรุปรูปแบบเดียวกับตัวอย่างนี้ (หัวข้อ, บูลเลตพอยต์สั้นๆ ระบุ Bullish/Bearish/Neutral ท้ายแต่ละข้อ, จบด้วยสรุป 1 บรรทัด):

🛢️ Executive Summary (24 ชม. ล่าสุด)
[บูลเลตพอยต์วิเคราะห์จากตัวเลขข้างต้น 3-5 ข้อ พร้อมระบุ (Bullish)/(Bearish)/(Neutral) ท้ายแต่ละข้อ]

สรุป:
👉 [ทิศทางรวม 1 บรรทัด]

ตอบเป็นข้อความสรุปเท่านั้น ไม่ต้องมีคำอธิบายอื่น"""


def call_claude(prompt):
    api_key = os.environ["ANTHROPIC_API_KEY"]
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json={"model": ANTHROPIC_MODEL, "max_tokens": 600, "messages": [{"role": "user", "content": prompt}]},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["content"][0]["text"].strip()


def main():
    tok = get_token()
    lines = snapshot_lines(tok)
    if not lines:
        print("[NEWS] no data available yet to summarize, skipping")
        return

    summary = call_claude(build_prompt(lines))
    ts = datetime.now(TZ).strftime("%-m/%-d/%Y %H:%M:%S") if os.name != "nt" else datetime.now(TZ).strftime("%#m/%#d/%Y %H:%M:%S")
    sheets_append(tok, SHEET_ID, f"{TABS['NEWS']}!A1:B1", [[ts, summary]])
    print(f"[NEWS] appended summary at {ts}")


if __name__ == "__main__":
    main()
