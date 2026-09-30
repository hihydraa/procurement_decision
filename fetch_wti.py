"""
Fetches today's WTI Crude price from OilPriceAPI (free tier, no scraping) and appends it to
Entry_WTI. Replaces the earlier attempt to scrape Investing.com's historical-data page via
Playwright — that worked from a residential IP but was 403-blocked from GitHub Actions'
datacenter IP range, which the bot-protection blocklists outright regardless of browser
fingerprint quality.

NYMEX Singapore Gasoil (the other series originally planned here) has no equivalent free API
anywhere — dropped by the user's decision; MOPS Diesel (already captured from OIL BOT) already
serves as a similar leading-indicator signal.

OilPriceAPI's /prices/latest only returns a single price, not OHLC — Open/High/Low/Vol. are
left blank and Change % is computed against our own previously recorded row instead of relying
on the source for it.
"""
import os
from datetime import datetime

import requests

from common import TABS, SHEET_ID, get_token, sheets_get, sheets_append, excel_serial_to_date, fmt_date_us, TZ_NAME
from zoneinfo import ZoneInfo

TZ = ZoneInfo(TZ_NAME)


def fetch_wti_price():
    api_key = os.environ["OILPRICEAPI_KEY"]
    r = requests.get(
        "https://api.oilpriceapi.com/v1/prices/latest",
        params={"by_code": "WTI_USD"},
        headers={"Authorization": f"Token {api_key}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["data"]["price"]


def main():
    tok = get_token()
    today = datetime.now(TZ).date()

    existing = sheets_get(tok, SHEET_ID, f"{TABS['ENTRY_WTI']}!A1:B5000")
    seen = {excel_serial_to_date(r[0]): r[1] for r in existing[1:] if r}
    if today in seen:
        print(f"[WTI] {today} already recorded, skipping")
        return

    price = fetch_wti_price()
    prev_dates = sorted(d for d in seen if d < today)
    prev_price = seen[prev_dates[-1]] if prev_dates else None
    change_pct = f"{(price - prev_price) / prev_price:+.2%}" if prev_price else ""

    sheets_append(tok, SHEET_ID, f"{TABS['ENTRY_WTI']}!A1:G1", [[fmt_date_us(today), price, "", "", "", "", change_pct]])
    print(f"[WTI] appended {price} for {today}")


if __name__ == "__main__":
    main()
