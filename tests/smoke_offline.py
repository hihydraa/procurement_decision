"""
รัน main.py ทั้งตัวแบบออฟไลน์ (ไม่แตะเน็ต/ชีท) เพื่อตรวจว่าทุกส่วนต่อกันได้ — ใช้ข้อมูลจริงที่ส่งเข้ามาทางพารามิเตอร์ (ไม่เก็บใน repo)

ใช้: python3 tests/smoke_offline.py <data_dir จาก parse_line_export.py> <eppo_raw.json ที่ดึงจากชีท Entry_Eppo> "2026-10-20 20:35"
"""
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import main as MAIN  # noqa: E402

data_dir, eppo_json, asof = sys.argv[1], sys.argv[2], sys.argv[3]
os.environ["ASOF"] = asof.replace(" ", "T")
rd = lambda n: pd.read_csv(os.path.join(data_dir, n))

# --- ตารางที่ main.py ต้องการ ---
eppo_raw = json.load(open(eppo_json, encoding="utf-8"))["values"]
width = max(len(r) for r in eppo_raw)
eppo = pd.DataFrame([r + [""] * (width - len(r)) for r in eppo_raw[1:]], columns=eppo_raw[0])

mops = rd("mops.csv")
mops_rows = []
for i, r in mops.drop_duplicates("mops_date").reset_index(drop=True).iterrows():
    d = pd.Timestamp(r.mops_date).strftime("%m/%d/%Y")
    mops_rows += [[d, "G95", r.g95, "", "95", i + 1], [d, "DS", r.diesel, "", "DS", i + 1]]
entry_mops = pd.DataFrame(mops_rows, columns=["Date", "Oil Type", "Price (USD/BBL)", "Type", "Normalized Key", "Obs Rank"])

crude = rd("crude.csv").drop_duplicates("mops_date")
mk = lambda col: pd.DataFrame({"Date": pd.to_datetime(crude.mops_date).dt.strftime("%m/%d/%Y"), "Price": crude[col], "Change %": ""}).dropna(subset=["Price"])
fund_sus = pd.DataFrame([["2026-10-19", "-102700.11", "236.42", "345.18", "0.00", "-345.18", "0.68", "CRITICAL"],
                         ["2026-10-20", "-103045.29", "-108.76", "345.18", "0.00", "-345.18", "-0.32", "CRITICAL"]],
                        columns=["Date", "Total_Balance (ล้านบาท)", "Cash_Remaining (ล้านบาท)", "Daily_Subsidy (ล้านบาท/วัน)",
                                 "Daily_Collection (ล้านบาท/วัน)", "Net_Fund_Impact (ล้านบาท/วัน)", "Runway_Days", "Status"])
by_gid = {
    MAIN.GIDS["ENTRY_WTI"]: mk("brent"), MAIN.GIDS["ENTRY_MOPS"]: entry_mops,
    MAIN.GIDS["ENTRY_EPPO"]: eppo, MAIN.GIDS["ENTRY_OILFUND_SUSTAINABILITY"]: fund_sus,
    MAIN.GIDS["SETTING"]: pd.DataFrame({"Date": ["3/17/2026"], "Exchange Rate": ["32.3347"]}),
    MAIN.GIDS["NEWS"]: pd.DataFrame({"Timestamp": [], "Context": []}),
}
mc = rd("mops.csv")[["msg_date", "msg_time", "mops_date", "g95", "g95_chg", "diesel", "diesel_chg"]]
feeds = {
    "Entry_Intraday": rd("trend.csv"), "Entry_HeadsUp": rd("headsup.csv"), "Entry_MopsClose": mc,
    "Entry_FundNotice": rd("fund.csv"),
}
MAIN.load_sheet = lambda gid: by_gid[gid].copy()
def by_name(name):
    if name in feeds:
        return feeds[name].copy()
    raise RuntimeError("no such tab (offline): " + name)
MAIN.load_sheet_by_name = by_name
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MAIN.main()
print(open("decision_snapshot.json", encoding="utf-8").read())
