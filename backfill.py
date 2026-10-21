"""
เติมช่องว่างย้อนหลังในแท็บ Entry_MOPS และ Entry_WTI ของชีท procurement_decision (ครั้งเดียว, รันซ้ำได้ — ข้ามวันที่ที่มีอยู่แล้ว)

ข้อมูลอยู่ใน backfill/*.csv:
  entry_mops_gap.csv   MOPS ปิดตลาด 9 ก.ค.–22 ก.ย. 2026: ค่าจริงจากกลุ่มไลน์ (Source=LINE) ; วันที่ไม่มีโพสต์ใช้ค่าก่อนหน้า (CARRIED)
                       ตามหลักเดิมของระบบ ค่าจากแชทตรงกับค่าเดิมในชีทเป๊ะในช่วงที่ทั้งสองฝั่งมีข้อมูล
  entry_wti_gap.csv    WTI 9 ก.ค.–30 ส.ค. 2026: **ค่าประมาณ** จาก Nymex น้ำมันดิบในไลน์ (ต่างจาก WTI จริงได้ถึง ~1 ดอลลาร์) ติดป้าย Note

วิธีทำงาน: แทรกแถวใหม่ตรงตำแหน่งตามวันที่ (ไม่ทับข้อมูลเดิม) , สำรองแท็บเดิมเป็นแท็บ Backup_* ก่อน , เรียงเลข Obs Rank ใหม่ (MOPS)
ใช้: python backfill.py            # ทดลองรัน: แสดงแผนอย่างเดียว ไม่เขียนอะไร
     python backfill.py --apply    # เขียนจริง (ต้องมี SA_EMAIL / SA_PRIVATE_KEY ที่เป็น Editor ของชีท)
"""
import argparse
import csv
import os
import re
from datetime import date, datetime, timedelta

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
EPOCH = date(1899, 12, 30)


def to_date(v):
    """เลขลำดับของ Sheets หรือข้อความ M/D/YYYY / YYYY-MM-DD -> date"""
    if isinstance(v, (int, float)):
        return EPOCH + timedelta(days=int(v))
    s = str(v).strip()
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    return datetime.strptime(s[:10], "%Y-%m-%d").date()


def plan_inserts(existing_dates, new_rows):
    """existing_dates = วันที่ของแถวข้อมูลเดิมตามลำดับในชีท (ไม่รวมหัวตาราง) ; new_rows = แถวใหม่ (คอลัมน์แรกคือวันที่)
    คืนรายการ (ตำแหน่งที่แทรก 0-based นับจากแถวข้อมูลแรก, [แถวที่แทรก]) เรียงจากท้ายไปหน้า (แทรกจากท้ายก่อน ตำแหน่งจะไม่เลื่อน)
    ข้ามวันที่ที่มีอยู่แล้ว"""
    have = set(existing_dates)
    todo = [r for r in new_rows if to_date(r[0]) not in have]
    groups = {}
    for r in todo:
        d = to_date(r[0])
        pos = 0                                   # แทรกต่อท้ายแถวสุดท้ายที่วันที่ <= d
        for i, ed in enumerate(existing_dates):
            if ed <= d:
                pos = i + 1
        groups.setdefault(pos, []).append(r)
    return sorted(((p, rs) for p, rs in groups.items()), key=lambda x: -x[0])


def read_csv(name, ncols):
    with open(os.path.join(HERE, "backfill", name), encoding="utf-8") as fh:
        rows = list(csv.reader(fh))[1:]
    out = []
    for r in rows:
        r = r[:ncols] + [""] * (ncols - len(r))
        out.append([float(c) if re.match(r"^-?\d+(\.\d+)?$", c) else c for c in r])
    return out


# ------------------------------------------------------------------ Sheets API
def api(tok, method, url, **kw):
    r = requests.request(method, url, headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}, timeout=60, **kw)
    r.raise_for_status()
    return r.json() if r.content else {}


def sheet_ids(tok, ssid):
    meta = api(tok, "GET", f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}?fields=sheets.properties")
    return {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta["sheets"]}


def backup_tab(tok, ssid, title, sid):
    res = api(tok, "POST", f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}/sheets/{sid}:copyTo", json={"destinationSpreadsheetId": ssid})
    name = f"Backup_{title}_{datetime.now().strftime('%Y%m%d_%H%M')}"
    api(tok, "POST", f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}:batchUpdate",
        json={"requests": [{"updateSheetProperties": {"properties": {"sheetId": res["sheetId"], "title": name}, "fields": "title"}}]})
    return name


def apply_tab(tok, ssid, title, sid, new_rows, ncols, renumber_col=None, dry=True, note_header=None):
    col_letter = chr(ord("A") + ncols - 1)
    vals = api(tok, "GET", f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}/values/{requests.utils.quote(title)}!A2:{col_letter}20000?valueRenderOption=UNFORMATTED_VALUE").get("values", [])
    existing = [to_date(r[0]) for r in vals if r]
    plan = plan_inserts(existing, new_rows)
    total = sum(len(rs) for _, rs in plan)
    print(f"[{title}] แถวเดิม {len(vals)} | แถวที่จะแทรก {total} ใน {len(plan)} ตำแหน่ง", [(p + 2, len(rs)) for p, rs in plan])
    if not total or dry:
        return total
    print(f"[{title}] สำรองแท็บ -> {backup_tab(tok, ssid, title, sid)}")
    if note_header:                               # เพิ่มหัวคอลัมน์ Note (ป้ายบอกว่าแถวไหนเป็นค่าประมาณ/ลากมา) ถ้ายังไม่มี
        cell = f"{title}!{note_header[0]}1"
        cur = api(tok, "GET", f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}/values/{requests.utils.quote(cell)}").get("values", [])
        if not cur or not cur[0] or not cur[0][0]:
            api(tok, "PUT", f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}/values/{requests.utils.quote(cell)}?valueInputOption=USER_ENTERED", json={"values": [[note_header[1]]]})
    for pos, rs in plan:                          # จากท้ายไปหน้า
        start = pos + 1                           # 0-based ของแถวชีท (แถว 1 = หัวตาราง)
        api(tok, "POST", f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}:batchUpdate",
            json={"requests": [{"insertDimension": {"range": {"sheetId": sid, "dimension": "ROWS", "startIndex": start, "endIndex": start + len(rs)}, "inheritFromBefore": True}}]})
        api(tok, "PUT", f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}/values/{requests.utils.quote(title)}!A{start + 1}:{col_letter}{start + len(rs)}?valueInputOption=USER_ENTERED",
            json={"values": rs})
    if renumber_col is not None:                  # Obs Rank: นับต่อเนื่องแยกตามชนิด (คอลัมน์ Normalized Key)
        allv = api(tok, "GET", f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}/values/{requests.utils.quote(title)}!A2:{col_letter}20000?valueRenderOption=UNFORMATTED_VALUE").get("values", [])
        count, ranks = {}, []
        for r in allv:
            k = str(r[4]) if len(r) > 4 else ""
            count[k] = count.get(k, 0) + 1
            ranks.append([count[k]])
        api(tok, "PUT", f"https://sheets.googleapis.com/v4/spreadsheets/{ssid}/values/{requests.utils.quote(title)}!{col_letter}2:{col_letter}{len(ranks) + 1}?valueInputOption=USER_ENTERED",
            json={"values": ranks})
        print(f"[{title}] เรียง Obs Rank ใหม่ {len(ranks)} แถว")
    return total


def main():
    from common import SHEET_ID, TABS, get_token
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="เขียนจริง (ไม่ใส่ = ทดลองรัน)")
    a = ap.parse_args()
    tok = get_token()
    ids = sheet_ids(tok, SHEET_ID)
    mops = [r[:5] + [""] for r in read_csv("entry_mops_gap.csv", 6)]          # คอลัมน์ F (Obs Rank) ถูกเรียงใหม่ทีหลัง
    wti = read_csv("entry_wti_gap.csv", 8)                                      # Date, Price, Open, High, Low, Vol., Change %, Note
    apply_tab(tok, SHEET_ID, TABS["ENTRY_MOPS"], ids[TABS["ENTRY_MOPS"]], mops, 6, renumber_col=5, dry=not a.apply)
    apply_tab(tok, SHEET_ID, TABS["ENTRY_WTI"], ids[TABS["ENTRY_WTI"]], wti, 8, dry=not a.apply, note_header=("H", "Note"))
    if not a.apply:
        print("ทดลองรันเท่านั้น — ไม่ได้เขียนอะไร ใส่ --apply เพื่อเขียนจริง")


if __name__ == "__main__":
    main()
