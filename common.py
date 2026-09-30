"""Shared Google Sheets auth/IO helpers used by ingest.py, scrape_futures.py and news.py."""
import os
import time
from datetime import datetime, timedelta

import jwt
import requests

SHEET_ID = "1JNrYTJtmgpOGjdfhIdJKz15i8Jxp-u8r1UFAXuQRs8s"
OILBOT_SHEET_ID = "1rxd2vQJWZ0pfE1gpFGRJb3Bie0ubdEdQZg241sr6A4U"

# แผนที่ชื่อภายในสคริปต์ -> ชื่อแท็บจริงในชีท (ชื่อจริงมีตัวพิมพ์เล็ก/ใหญ่ปนกันและเว้นวรรค ไม่ตรงกับ
# ชื่อ GID ที่ main.py ตั้งไว้ภายใน อย่าลืมเช็คตรงนี้ก่อนถ้าเพิ่มแท็บใหม่)
TABS = {
    "ENTRY_NYMEX": "Entry_NYMEX",
    "ENTRY_WTI": "Entry_WTI",
    "ENTRY_MOPS": "Entry_MOPS",
    "ENTRY_EPPO": "Entry_Eppo",
    "SETTING": "Setting",
    "NATIONAL_CONSUMPTION_REF": "National Consumption Ref",
    "ENTRY_OILFUND_SUSTAINABILITY": "Entry_OilFund_Sustainability",
    "NEWS": "NEWS",
}

TZ_NAME = "Asia/Bangkok"
SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
EXCEL_EPOCH = datetime(1899, 12, 30)


def get_token(scope=SHEETS_SCOPE):
    raw = os.environ["SA_PRIVATE_KEY"].strip()
    # SA_PRIVATE_KEY อาจเป็น PEM ดิบ หรือ JSON ทั้งไฟล์ของ service account key (ถ้าตั้ง secret
    # ด้วย `gh secret set ... < key.json` จะได้ JSON ทั้งก้อนมา) รองรับทั้งสองแบบ
    if raw.startswith("{"):
        import json as _json
        key_obj = _json.loads(raw)
        private_key = key_obj["private_key"]
        email = os.environ.get("SA_EMAIL") or key_obj["client_email"]
    else:
        private_key = raw.replace("\\n", "\n")
        email = os.environ["SA_EMAIL"]
    now = int(time.time())
    token_payload = {
        "iss": email, "scope": scope, "aud": "https://oauth2.googleapis.com/token",
        "iat": now, "exp": now + 3600,
    }
    assertion = jwt.encode(token_payload, private_key, algorithm="RS256")
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion,
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def sheets_get(tok, spreadsheet_id, rng):
    r = requests.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{requests.utils.quote(rng)}",
        params={"valueRenderOption": "UNFORMATTED_VALUE"},
        headers={"Authorization": f"Bearer {tok}"},
    )
    r.raise_for_status()
    return r.json().get("values", [])


def sheets_append(tok, spreadsheet_id, rng, rows):
    if not rows:
        return
    r = requests.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{requests.utils.quote(rng)}:append",
        params={"valueInputOption": "USER_ENTERED", "insertDataOption": "INSERT_ROWS"},
        headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
        json={"values": rows},
    )
    r.raise_for_status()
    return r.json()


def col_to_dicts(rows):
    header = rows[0] if rows else []
    return [dict(zip(header, r + [None] * (len(header) - len(r)))) for r in rows[1:]]


def excel_serial_to_date(v):
    if isinstance(v, (int, float)):
        return (EXCEL_EPOCH + timedelta(days=int(v))).date()
    return None


def fmt_date_us(d):
    """เช่น 9/30/2026 — รูปแบบเดียวกับที่ใช้เขียนคอลัมน์ Date ในแท็บ Entry_* ทั้งหมด"""
    return d.strftime("%-m/%-d/%Y") if os.name != "nt" else d.strftime("%#m/%#d/%Y")
