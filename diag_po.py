"""
ตรวจการซื้อจริงจากไฟล์ PO รายเดือน (Google Sheet) เทียบกับราคาตลาด (PriceLog) — อ่านอย่างเดียว ไม่เขียนชีท
ตอบ : ซื้อจากคลังไหนบ้าง ราคาซื้อต่างจากราคาประกาศเท่าไร และต้นทุนส่งถึงปลายทาง (ราคา + ค่าขนส่ง) แพงกว่าต้นทุนต่ำสุดที่เป็นไปได้ในช่วงเดียวกันเท่าไร
แยก 2 ส่วน : (1) จังหวะเวลา = ราคาที่จ่าย − ราคาต่ำสุดของคลังเดียวกันในช่วง  (2) การเลือกคลัง = ต้นทุนส่งถึงของคลังที่ซื้อ − ต้นทุนส่งถึงต่ำสุดข้ามคลังหลัก

ช่วงเทียบ (วัน D = วันที่ของแท็บ) : 20:30 วัน D-1 → 22:00 วัน D ; ถ้าหมายเหตุเขียน "โหลดน้ำมันหลังเที่ยงคืน" (ใช้ราคาของวันก่อน) ขยายเริ่มที่ 06:00 วัน D-1
ข้อจำกัด : ไม่รู้เวลาสั่งจริง/โควต้า/เวลาตัดรับออเดอร์/เวลาเปิดคลัง จึงเป็น "ส่วนเกินเทียบกับค่าต่ำสุดที่ไม่มีเงื่อนไข" ไม่ใช่เงินที่ประหยัดได้จริงทั้งหมด ;
          PriceLog เริ่มปลาย ก.ย. 2569 และเวลามีผลของราคาบางส่วนไม่ทราบ ; อัตราค่าขนส่งมาจากตารางในไฟล์ PO (บางจาก/ไออาร์ อยุธยา ใช้อัตราเดียวกับ สระบุรี/นครหลวง และกระนวน = ยางตลาด เป็นสมมติฐาน)

ใช้: python diag_po.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY ; บัญชีบริการต้องเปิดไฟล์ PO ได้)
"""
import re
import time
from collections import defaultdict

import numpy as np
import pandas as pd
import requests

from common import OILBOT_SHEET_ID, get_token, sheets_get
from po_parse import parse_tab
from pricelog_frame import build_frame
from tap_policy import DEPOTS, depot_series, price_at

# ไฟล์ PO รายเดือนที่อ่าน (พ.ศ.-เดือน → spreadsheetId) ; เดือนก่อน ก.ย. 2569 ไม่มี PriceLog ให้เทียบ
PO_FILES = {"2569-09": "1mXK8f8xTJy_dDBQzFxP8HZw2y_E78bTef70c6J30qTA",
            "2569-10": "1Y7xIO-QzCLTgHsNQJz6HNnHaufXLZDgRQ-CQhvTBWoA",
            "2569-11": "1ks4_bwWBpF-2G6aPRTS5NIB34ADS28sNRArgxQrQzAs"}
ORIGIN = {"ไออาร์ ระยอง": "ระยอง", "ดาว ระยอง": "ระยอง", "ไออาร์ อยุธยา": "สระบุรี/นครหลวง", "ดาว สระบุรี": "สระบุรี/นครหลวง",
          "ปตท สระบุรี": "สระบุรี/นครหลวง", "บางจาก บางปะอิน": "บางปะอิน"}
# ค่าขนส่ง บาท/ลิตร ตามตารางในไฟล์ PO : (ต้นทาง, ปลายทาง) ; ปลายทาง ยางตลาด=กระนวน ; สระบุรี/ระยอง มุกดาหาร=วานร
TRANSPORT = {("สระบุรี/นครหลวง", "ยางตลาด"): 0.63, ("สระบุรี/นครหลวง", "มุกดาหาร"): 0.75, ("สระบุรี/นครหลวง", "วานรนิวาส"): 0.75,
             ("ระยอง", "ยางตลาด"): 0.80, ("ระยอง", "มุกดาหาร"): 0.95, ("ระยอง", "วานรนิวาส"): 0.95,
             ("บางปะอิน", "ยางตลาด"): 0.67, ("บางปะอิน", "มุกดาหาร"): 0.80, ("บางปะอิน", "วานรนิวาส"): 0.85}
PROV = (("ไออาร์", "ไออาร์"), ("iRPC", "ไออาร์"), ("IR", "ไออาร์"), ("SFL", "ดาว"), ("สตาร์", "ดาว"), ("ดาว", "ดาว"), ("ปตท", "ปตท"), ("PTT", "ปตท"),
        ("บางจาก", "บางจาก"), ("BCP", "บางจาก"))
PLACES = ("ระยอง", "สระบุรี", "อยุธยา", "บางปะอิน")
STD_START_H, LONG_START_H, END_H, STEP_H = -3.5, -18.0, 22.0, 0.5
MISMATCH = -0.10               # จ่ายต่ำกว่าราคาต่ำสุดใน PriceLog เกินนี้ = ข้อมูลไม่ตรง/ไม่ครบ ไม่นับเป็นผลลัพธ์


def po_depot(depot: str):
    """'IR ระยอง' → 'ไออาร์ ระยอง' (ชื่อคลังหลักใน tap_policy.DEPOTS) ; ไม่ใช่คลังหลัก → None"""
    prov = next((p for k, p in PROV if k in depot), None)
    place = next((pl for pl in PLACES if pl in depot), None)
    key = f"{prov} {place}" if prov and place else None
    return key if key in DEPOTS else None


def transport(origin_key, dest: str):
    d = "ยางตลาด" if dest in ("กระนวน", "ยางตลาด") else dest
    return TRANSPORT.get((ORIGIN.get(origin_key), d))


def window_times(day: pd.Timestamp, long_start: bool) -> pd.DatetimeIndex:
    start = LONG_START_H if long_start else STD_START_H
    return pd.DatetimeIndex([day + pd.Timedelta(hours=h) for h in np.arange(start, END_H + 1e-9, STEP_H)])


def window_min(series, times) -> float:
    p = price_at(series, times)
    return float(np.nanmin(p)) if (~np.isnan(p)).any() else float("nan")


def benchmark(po: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """เติมคอลัมน์ต้นทุนส่งถึง/ส่วนเกิน ให้แต่ละแถว PO ที่เทียบได้ (คลังหลัก + ปลายทางระบุ + มีราคาตลาดครบ) ; แถวที่เทียบไม่ได้ได้ NaN"""
    cache, rows = {}, []
    for _, r in po.iterrows():
        out = {"timing": np.nan, "depot_sel": np.nan, "excess": np.nan, "why": ""}
        key = po_depot(r["depot"])
        dest = str(r["dest"]).split("+")[0]
        if key is None:
            out["why"] = "นอกคลังหลัก"
        elif not dest:
            out["why"] = "ไม่ระบุปลายทาง"
        elif r["price"] is None or pd.isna(r["price"]):
            out["why"] = "ไม่มีราคา"
        else:
            day = pd.Timestamp(r["date"])
            times = window_times(day, "หลังเที่ยงคืน" in str(r["price_note"]))
            per = {}
            for dep in DEPOTS:
                ck = (dep, r["fuel"])
                if ck not in cache:
                    cache[ck] = depot_series(prices, dep, r["fuel"])
                mn, t = window_min(cache[ck], times), transport(dep, dest)
                if not np.isnan(mn) and t is not None:
                    per[dep] = (mn, t)
            t_paid = r["transport"] if r["transport"] else transport(key, dest)
            if key not in per or t_paid is None:
                out["why"] = "ไม่มีราคาตลาดของคลังที่ซื้อ"
            else:
                ideal = min(mn + t for mn, t in per.values())
                paid = r["price"] + t_paid
                timing = r["price"] - per[key][0]
                out.update({"timing": round(timing, 4), "excess": round(paid - ideal, 4), "depot_sel": round(paid - ideal - timing, 4),
                            "why": "ราคาจ่ายต่ำกว่า PriceLog" if timing < MISMATCH else ""})
        rows.append(out)
    return pd.concat([po.reset_index(drop=True), pd.DataFrame(rows)], axis=1)


def wmean(d: pd.DataFrame, col: str) -> float:
    return float((d[col] * d["liters"]).sum() / d["liters"].sum()) if len(d) and d["liters"].sum() else float("nan")


def coverage_report(po: pd.DataFrame) -> list:
    out = [f"[PO] {len(po)} แถว (ใบสั่ง × ชนิด) {po['date'].nunique()} วัน {po['date'].min()}–{po['date'].max()} ; รวม {po['liters'].sum():,.0f} ลิตร"]
    by = po.groupby("fuel")["liters"].sum().sort_values(ascending=False)
    out.append("   ตามชนิด : " + " ".join(f"{k} {v:,.0f}" for k, v in by.items()))
    dep = po.groupby("depot")["liters"].sum().sort_values(ascending=False)
    out.append("   ตามคลัง : " + " | ".join(f"{k or '?'} {v / po['liters'].sum():.0%}" for k, v in dep.head(8).items()))
    jb = po.groupby("seller")["liters"].sum().sort_values(ascending=False)
    out.append("   ตามผู้ขาย : " + " | ".join(f"{k or '?'} {v / po['liters'].sum():.0%}" for k, v in jb.head(5).items()))
    dst = po.groupby(po["dest"].replace("", "?"))["liters"].sum().sort_values(ascending=False)
    out.append("   ตามปลายทาง : " + " | ".join(f"{k} {v / po['liters'].sum():.0%}" for k, v in dst.head(6).items()))
    return out


def discount_report(po: pd.DataFrame) -> list:
    d = po.dropna(subset=["price", "list_price"]).copy()
    d["disc"] = (d["list_price"] - d["price"]).round(2)
    out = [f"[PO-DISC] ส่วนต่าง ราคาประกาศคลัง − ราคาซื้อ (ใบที่มีราคาประกาศ {len(d)}/{len(po)} แถว)"]
    for dep, g in d.groupby("depot"):
        vc = g["disc"].value_counts().head(3)
        out.append(f"   {dep or '?'}: " + ", ".join(f"{k:.2f}×{v}" for k, v in vc.items()))
    return out


def cost_report(b: pd.DataFrame) -> list:
    ok = b.dropna(subset=["excess"])
    ok = ok[ok["why"] == ""]
    out = [f"[PO-COST] เทียบได้ {len(ok)}/{len(b)} แถว ({ok['liters'].sum():,.0f}/{b['liters'].sum():,.0f} ลิตร) ; ไม่ได้เทียบเพราะ : "
           + ", ".join(f"{k} {v}" for k, v in b.loc[b['why'] != '', 'why'].value_counts().items())]
    if ok.empty:
        return out
    for fuel, g in list(ok.groupby("fuel")) + [("รวม", ok)]:
        near = g.loc[g["timing"] <= 0.05, "liters"].sum() / g["liters"].sum()
        out.append(f"   {fuel}: {g['liters'].sum():,.0f} ลิตร ; จังหวะเวลา +{wmean(g, 'timing'):.3f} | เลือกคลัง {wmean(g, 'depot_sel'):+.3f} | รวมส่วนเกิน +{wmean(g, 'excess'):.3f} บาท/ลิตร"
                   f" (≈ {(g['excess'] * g['liters']).sum():,.0f} บาท) ; ซื้อที่ราคาต่ำสุดของคลัง (≤5 สต.) {near:.0%} ของลิตร")
    return out


def batch_get(tok, sid, ranges, tries=5):
    """อ่านหลายช่วงในคำขอเดียว (Sheets API จำกัดจำนวนคำขออ่านต่อนาที) ; 429 → รอแล้วลองใหม่"""
    for k in range(tries):
        r = requests.get(f"https://sheets.googleapis.com/v4/spreadsheets/{sid}/values:batchGet",
                         params={"ranges": ranges, "valueRenderOption": "UNFORMATTED_VALUE"}, headers={"Authorization": f"Bearer {tok}"})
        if r.status_code == 429 and k < tries - 1:
            time.sleep(20 * (k + 1))
            continue
        r.raise_for_status()
        return [v.get("values", []) for v in r.json()["valueRanges"]]


def load_po() -> pd.DataFrame:
    tok = get_token()
    rows = []
    for label, sid in PO_FILES.items():
        meta = requests.get(f"https://sheets.googleapis.com/v4/spreadsheets/{sid}", params={"fields": "sheets.properties.title"},
                            headers={"Authorization": f"Bearer {tok}"})
        meta.raise_for_status()
        tabs = [s["properties"]["title"] for s in meta.json()["sheets"] if re.match(r"^\d{1,2}", s["properties"]["title"])]
        for i in range(0, len(tabs), 15):
            chunk = tabs[i:i + 15]
            for tab, vals in zip(chunk, batch_get(tok, sid, [f"'{t}'!A1:Z120" for t in chunk])):
                rows.extend(parse_tab(vals, label, tab))
        print(f"[PO] {label}: {len(tabs)} แท็บวัน")
    return pd.DataFrame(rows)


def main():
    po = load_po()
    if po.empty:
        print("[PO] ไม่พบรายการซื้อ")
        return
    for line in coverage_report(po) + discount_report(po):
        print(line)
    tok = get_token()
    prices = build_frame(sheets_get(tok, OILBOT_SHEET_ID, "PriceLog!A1:Q200000")).reset_index(drop=True)
    po = po[pd.to_datetime(po["date"]) >= prices["effective"].min().normalize() + pd.Timedelta(days=1)]
    print(f"[PO] ช่วงที่ซ้อนกับ PriceLog ({prices['effective'].min():%d/%m} →): {len(po)} แถว")
    for line in cost_report(benchmark(po, prices)):
        print(line)


if __name__ == "__main__":
    main()
