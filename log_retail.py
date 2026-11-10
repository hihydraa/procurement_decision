"""
บันทึกราคาปลีก ปตท. จากหน้า Kapook (http://gasprice.kapook.com/gasprice.php — ใช้ตัวเลือกเดียวกับแพ็กเกจ thaioil-api) ลงแท็บ RetailLog
เก็บเฉพาะ "ตอนที่ราคาเปลี่ยน" : 1 แถวต่อ (แบรนด์, ชนิด, ราคา) พร้อม first_seen_at (ครั้งแรกที่เห็นราคานี้) และ last_seen_at (ครั้งล่าสุดที่ยังเห็นราคานี้)
→ ราคาเปลี่ยนจริงอยู่ในช่วง (last_seen_at ของแถวก่อนหน้า, first_seen_at ของแถวนี้] ; ความละเอียดเท่าความถี่ที่รัน และเป็นเวลาที่ Kapook อัปเดต ไม่ใช่เวลา ปตท. ประกาศ
ไม่ส่งข้อความใด ๆ ไม่กระทบการตัดสินใจ ; หน้าเว็บเปลี่ยนโครงสร้าง/เข้าไม่ได้ → พิมพ์เตือนและจบโดยไม่เขียนชีท

ใช้: python log_retail.py    (ต้องมี SA_EMAIL / SA_PRIVATE_KEY)
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from common import SHEET_ID, TABS, TZ_NAME, ensure_tab, get_token, sheets_append, sheets_get, sheets_update

SOURCE_URL = "http://gasprice.kapook.com/gasprice.php"
BRANDS = ("ptt",)
COLUMNS = ["first_seen_at", "last_seen_at", "brand", "product", "price", "page_date", "source"]
LAST_COL = "G"
PRICE_RANGE = (15.0, 70.0)             # ตัวเลขนอกช่วงนี้ถือว่า parse ผิด (ราคาน้ำมัน ฿/ลิตร)


def parse_page(html: str, brand: str):
    """(รายการ (ชนิด, ราคา), ข้อความวันที่ของหน้า) ของแบรนด์ ; ว่างถ้าโครงสร้างหน้าไม่ตรง"""
    soup = BeautifulSoup(html, "html.parser")
    date_el = soup.select_one("h2.text-lg")
    page_date = date_el.get_text(strip=True).replace("อัปเดตราคาน้ำมันล่าสุด", "").strip() if date_el else ""
    items = []
    for li in soup.select(f"#brand-{brand} ul > li"):
        name, price = li.select_one(".min-w-0.flex-1 > p.truncate"), li.select_one(".shrink-0.text-right > p.text-xl")
        if not name or not price:
            continue
        try:
            p = float(price.get_text(strip=True))
        except ValueError:
            continue
        if PRICE_RANGE[0] <= p <= PRICE_RANGE[1]:
            items.append((name.get_text(strip=True), p))
    return items, page_date


def fetch_html() -> str:
    r = requests.get(SOURCE_URL, timeout=30, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml"})
    r.raise_for_status()
    r.encoding = r.encoding if r.encoding and r.encoding.lower() != "iso-8859-1" else "utf-8"
    return r.text


def plan(rows: list, brand: str, items: list, page_date: str, now_s: str):
    """เทียบราคาที่เห็นกับแถวล่าสุดของแต่ละ (แบรนด์, ชนิด) → (แถวใหม่ที่ต้องเพิ่ม, [(เลขแถวในชีทที่ต้องอัปเดต last_seen_at)])
    rows = แถวข้อมูล (ไม่รวมหัวตาราง) ตามลำดับในชีท ; เลขแถวในชีท = index + 2"""
    last = {}
    for i, r in enumerate(rows):
        if len(r) > 4 and str(r[2]) == brand:
            last[str(r[3])] = (i, r[4])
    add, touch = [], []
    for product, price in items:
        prev = last.get(product)
        if prev is not None and abs(float(prev[1]) - price) < 1e-9:
            touch.append(prev[0] + 2)
        else:
            add.append([now_s, now_s, brand, product, price, page_date, "kapook"])
    return add, touch


def main():
    now_s = datetime.now(ZoneInfo(TZ_NAME)).strftime("%Y-%m-%d %H:%M")
    tok = get_token()
    tab = TABS["RETAIL_LOG"]
    html = fetch_html()
    parsed = {b: parse_page(html, b) for b in BRANDS}
    if not any(items for items, _ in parsed.values()):
        print("[RETAIL] ไม่พบราคาในหน้า Kapook (โครงสร้างหน้าอาจเปลี่ยน) — ไม่เขียนชีท")
        return
    if ensure_tab(tok, SHEET_ID, tab):
        sheets_update(tok, SHEET_ID, f"{tab}!A1", [COLUMNS])
        print(f"[RETAIL] created tab {tab}")
    rows = sheets_get(tok, SHEET_ID, f"{tab}!A2:{LAST_COL}50000")
    for brand, (items, page_date) in parsed.items():
        add, touch = plan(rows, brand, items, page_date, now_s)
        for n in touch:
            sheets_update(tok, SHEET_ID, f"{tab}!B{n}", [[now_s]])
        if add:
            sheets_append(tok, SHEET_ID, f"{tab}!A1:{LAST_COL}1", add)
        print(f"[RETAIL] {brand}: {len(items)} ชนิด ({page_date}) ; ราคาใหม่/เปลี่ยน +{len(add)} ; ราคาเดิม {len(touch)}")
        for a in add:
            print(f"   {a[3]} = {a[4]:.2f}")


if __name__ == "__main__":
    main()
