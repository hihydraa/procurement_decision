"""
Scrapes NYMEX (Singapore Gasoil Platts C1) and WTI Crude historical data from Investing.com
and appends new rows to Entry_NYMEX / Entry_WTI.

NOTE ON RISK: scraping Investing.com is against its Terms of Service. This is run at low
frequency (a few times/day, alongside the existing dashboard refresh schedule) for internal
decision-support use only — never re-published or redistributed. The user explicitly accepted
this risk before this script was built.

Investing.com blocks plain HTTP requests and even headless Chromium (403) — it only lets
through a real Chrome browser (channel="chrome"), so `playwright install chrome` must run in
CI, not just the default bundled Chromium.
"""
import re
import sys
from datetime import datetime, date

from playwright.sync_api import sync_playwright

from common import TABS, SHEET_ID, get_token, sheets_get, sheets_append, excel_serial_to_date, fmt_date_us

SOURCES = {
    "ENTRY_NYMEX": "https://www.investing.com/commodities/nymex-singapore-gasoil-platts-c1-futures-historical-data",
    "ENTRY_WTI": "https://www.investing.com/commodities/crude-oil-historical-data",
}
EXPECTED_HEADER = ["Date", "Price", "Open", "High", "Low", "Vol.", "Change %"]
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"


def scrape_historical_table(page, url):
    resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    tables = page.eval_on_selector_all(
        "table",
        """tables => tables.map(t => Array.from(t.querySelectorAll('tr')).map(
            tr => Array.from(tr.querySelectorAll('th,td')).map(td => td.innerText.trim())
        ))""",
    )
    for t in tables:
        if t and t[0] == EXPECTED_HEADER:
            return t[1:]
    # เผื่อวินิจฉัย: บันทึก status code + ตัวอย่างเนื้อหาหน้าเว็บที่ได้จริงไว้ในข้อความ error
    body_snippet = page.evaluate("() => document.body ? document.body.innerText.slice(0, 300) : '(no body)'")
    raise RuntimeError(
        f"{url}: no matching table (found {len(tables)} tables). "
        f"HTTP {resp.status if resp else '?'}. Body starts: {body_snippet!r}"
    )


def parse_num(s):
    s = str(s).replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def ingest_source(tok, tab_key, rows):
    existing = sheets_get(tok, SHEET_ID, f"{TABS[tab_key]}!A1:A5000")
    seen = {excel_serial_to_date(r[0]) for r in existing[1:] if r}
    new_rows = []
    for r in rows:
        try:
            d = datetime.strptime(r[0], "%b %d, %Y").date()
        except ValueError:
            continue
        if d in seen:
            continue
        new_rows.append([fmt_date_us(d), parse_num(r[1]), parse_num(r[2]), parse_num(r[3]), parse_num(r[4]), r[5], r[6]])
        seen.add(d)
    new_rows.sort(key=lambda r: datetime.strptime(r[0], "%m/%d/%Y"))
    sheets_append(tok, SHEET_ID, f"{TABS[tab_key]}!A1:G1", new_rows)
    print(f"[{tab_key}] appended {len(new_rows)} rows")
    return len(new_rows)


def main():
    tok = get_token()
    failures = []

    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=True, args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(user_agent=UA, viewport={"width": 1366, "height": 768}, locale="en-US")
        context.add_init_script('Object.defineProperty(navigator, "webdriver", {get: () => undefined})')
        page = context.new_page()

        for tab_key, url in SOURCES.items():
            try:
                rows = scrape_historical_table(page, url)
                ingest_source(tok, tab_key, rows)
            except Exception as e:
                print(f"[{tab_key}] FAILED: {e}")
                failures.append(tab_key)

        browser.close()

    if failures:
        raise SystemExit(f"scrape_futures.py: {', '.join(failures)} failed — see logs above")


if __name__ == "__main__":
    main()
