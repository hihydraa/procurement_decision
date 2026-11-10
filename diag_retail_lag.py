"""
ตรวจราคาปลีก (Entry_Eppo คอลัมน์ RETAIL รายวัน) เทียบกับราคาส่งหน้าคลัง (PriceLog) — อ่านอย่างเดียว ไม่เขียนชีท
คำถาม : (1) ปลีกปรับบ่อยแค่ไหน ขนาดเท่าไร (2) ราคาส่งขยับแล้วปลีกตามภายในกี่วัน กี่เปอร์เซ็นต์ของส่วนที่ส่งขยับ
        (3) แนวโน้ม 24 ชม. ที่ออกจริง (OutlookLog) บอกทิศทางปลีกใน 1-3 วันถัดไปได้แค่ไหน

ข้อจำกัด : EPPO มีราคาปลีกรายวัน ไม่มีเวลาที่ปรับ ; แถวที่ Source = CARRIED คือลากค่าเดิมมาเพราะยังไม่มีไฟล์ จึงไม่นับเป็นวันที่มีข้อมูลจริง
          ยังไม่ได้ยืนยันว่า RETAIL ใน EPPO ตรงกับราคาป้ายของ ปตท. ทุกวัน (ให้ดูตัวบันทึก RetailLog เทียบต่อไป)

ใช้: python diag_retail_lag.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY)
"""
import numpy as np
import pandas as pd

from common import OILBOT_SHEET_ID, SHEET_ID, TABS, get_token, sheets_get, to_ts
from pricelog_frame import build_frame
from tap_policy import DEPOTS, depot_series, load_outlook, pred_of, price_at

EPPO_FUEL = {"H-DIESEL": "B7", "GASOHOL95 E10": "G95"}
MOVE = 0.10                    # ปลีกขยับ >= 10 สต. นับว่าปรับ
WS_EVENT = 0.30                # ราคาส่งขยับ >= 30 สต./วัน นับเป็นเหตุการณ์
LAGS = (0, 1, 2, 3)
WS_HOUR = 12.0                 # อ่านราคาส่งที่เที่ยงของแต่ละวัน


def retail_daily(values: list) -> pd.DataFrame:
    """Entry_Eppo → (day, fuel, retail) เฉพาะ 2 ชนิดหลักและแถวที่ไม่ใช่ CARRIED ; แถวซ้ำวัน/ชนิดเก็บแถวหลังสุด"""
    head = [str(h).strip() for h in values[0]]
    ix = {c: head.index(c) for c in ("Date", "oil type", "RETAIL")}
    src = head.index("Source") if "Source" in head else None
    rows = []
    for r in values[1:]:
        g = lambda i: r[i] if i < len(r) else ""
        fuel = EPPO_FUEL.get(str(g(ix["oil type"])).strip())
        day, price = to_ts(g(ix["Date"])), g(ix["RETAIL"])
        if fuel is None or pd.isna(day) or isinstance(price, str) or price in ("", None):
            continue
        if src is not None and str(g(src)).strip().upper() == "CARRIED":
            continue
        rows.append((day.normalize(), fuel, float(price)))
    d = pd.DataFrame(rows, columns=["day", "fuel", "retail"])
    return d.drop_duplicates(["day", "fuel"], keep="last").sort_values(["fuel", "day"]).reset_index(drop=True)


def retail_changes(retail: pd.DataFrame) -> pd.DataFrame:
    """การเปลี่ยนแปลงจากวันข้อมูลจริงก่อนหน้า (gap_days = ห่างกี่วัน ; > 1 แปลว่ามีวันขาด ขนาดอาจรวมหลายวัน)"""
    out = []
    for fuel, g in retail.groupby("fuel"):
        g = g.sort_values("day")
        out.append(pd.DataFrame({"fuel": fuel, "day": g.day, "retail": g.retail, "chg": g.retail.diff().round(4),
                                 "gap_days": g.day.diff().dt.days}).iloc[1:])
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=["fuel", "day", "retail", "chg", "gap_days"])


def wholesale_daily(prices: pd.DataFrame, fuel: str, days: pd.DatetimeIndex, hour: float = WS_HOUR) -> pd.Series:
    """ราคาส่งเฉลี่ยข้ามคลังหลัก (ต่ำสุดข้ามจ๊อบเบอร์ในแต่ละคลัง) ที่เวลา hour ของแต่ละวัน ; NaN ถ้ามีคลังไม่ถึง 3 แห่ง"""
    times = pd.DatetimeIndex([d + pd.Timedelta(hours=hour) for d in days])
    cols = [price_at(depot_series(prices, depot, fuel), times) for depot in DEPOTS]
    mat = np.vstack(cols)
    ok = (~np.isnan(mat)).sum(axis=0) >= 3
    with np.errstate(all="ignore"):
        mean = np.where(ok, np.nanmean(np.where(np.isnan(mat), np.nan, mat), axis=0), np.nan)
    return pd.Series(mean, index=days)


def pass_through(ws: pd.Series, retail: pd.DataFrame) -> list:
    """ทุกวันที่ราคาส่งขยับ >= WS_EVENT : ปลีกขยับทิศเดียวกัน (>= MOVE) ภายในกี่วัน และสัดส่วนที่ปลีกตามภายใน 3 วัน"""
    r = retail.set_index("day").retail
    dws = ws.diff()
    res = {k: 0 for k in LAGS}
    n, ratios = 0, []
    for day, dv in dws[dws.abs() >= WS_EVENT].items():
        base = r.get(day - pd.Timedelta(days=1))
        if base is None or np.isnan(dv):
            continue
        n += 1
        sign = np.sign(dv)
        for k in LAGS:
            cur = r.get(day + pd.Timedelta(days=k))
            if cur is not None and np.sign(cur - base) == sign and abs(cur - base) >= MOVE:
                res[k] += 1
        end = r.get(day + pd.Timedelta(days=3))
        if end is not None:
            ratios.append((end - base) / dv)
    return [n, res, float(np.median(ratios)) if ratios else float("nan")]


def retail_report(changes: pd.DataFrame) -> list:
    out = []
    for fuel, g in changes.groupby("fuel"):
        real = g[g.gap_days == 1]
        up, dn = (real.chg >= MOVE).sum(), (real.chg <= -MOVE).sum()
        big = real.chg[real.chg.abs() >= MOVE].abs()
        out.append(f"[RETAIL] {fuel}: {len(real)} วันที่เทียบวันก่อนหน้าได้ ({g.day.min():%d/%m/%y}–{g.day.max():%d/%m/%y}) ; ปรับขึ้น {up} ลง {dn} "
                   f"({(up + dn) / max(len(real), 1):.0%} ของวัน) ; ขนาดกลาง {big.median() if len(big) else float('nan'):.2f} สูงสุด {big.max() if len(big) else float('nan'):.2f} บาท/ลิตร"
                   f" ; ข้อมูลขาดวัน {int((g.gap_days > 1).sum())} ช่วง")
        wd = real[real.chg.abs() >= MOVE].day.dt.dayofweek.value_counts().reindex(range(7), fill_value=0)
        out.append("   ปรับตามวัน จ.–อา. : " + " ".join(str(int(v)) for v in wd))
    return out


def lag_report(prices: pd.DataFrame, retail: pd.DataFrame) -> list:
    out = []
    for fuel, g in retail.groupby("fuel"):
        days = pd.date_range(g.day.min() - pd.Timedelta(days=1), g.day.max(), freq="D")
        ws = wholesale_daily(prices, fuel, days)
        n, res, ratio = pass_through(ws, g)
        if n == 0:
            out.append(f"[LAG] {fuel}: ไม่มีเหตุการณ์ราคาส่งขยับ >= {WS_EVENT:.2f} ที่เทียบได้")
            continue
        out.append(f"[LAG] {fuel}: ราคาส่งขยับ >= {WS_EVENT:.2f} บาท/ลิตร {n} วัน ; ปลีกตามทิศเดียวกัน (>= {MOVE:.2f}) เมื่อผ่านไป "
                   + " | ".join(f"{k} วัน {res[k]}/{n} ({res[k] / n:.0%})" for k in LAGS) + f" ; ปลีกขยับ/ส่งขยับ (ที่ 3 วัน, มัธยฐาน) = {ratio:.2f}")
    return out


def forecast_report(retail: pd.DataFrame, outlook: pd.DataFrame) -> list:
    """แนวโน้ม 24 ชม. ที่ออก ณ 20:30 ของวัน r เทียบกับการขยับปลีก r → r+1, r+2, r+3 (วัน r = วันที่ออกคำพยากรณ์)"""
    out = []
    o = outlook[outlook.hours == 24].assign(day=lambda d: d.ref.dt.normalize(), pred=lambda d: d.tier.map(pred_of))
    for fuel, g in retail.groupby("fuel"):
        r = g.set_index("day").retail
        rows = []
        for _, x in o[o.fuel == fuel].iterrows():
            base = r.get(x.day)
            if base is None:
                continue
            row = {"pred": x.pred, "day": x.day}
            for k in (1, 2, 3):
                cur = r.get(x.day + pd.Timedelta(days=k))
                row[k] = None if cur is None else round(cur - base, 4)
            rows.append(row)
        d = pd.DataFrame(rows)
        if d.empty:
            out.append(f"[FC→RETAIL] {fuel}: ไม่มีแถวคำพยากรณ์ที่จับคู่กับราคาปลีกได้")
            continue
        out.append(f"[FC→RETAIL] {fuel}: คำพยากรณ์ 24 ชม. {len(d)} วัน เทียบปลีกที่ขยับสะสมใน 1/2/3 วัน (ขึ้น/ทรง/ลง นับที่เกณฑ์ {MOVE:.2f})")
        for pred, gg in d.groupby("pred"):
            cells = []
            for k in (1, 2, 3):
                v = gg[k].dropna()
                cells.append(f"{k}วัน ↑{int((v >= MOVE).sum())} -{int((v.abs() < MOVE).sum())} ↓{int((v <= -MOVE).sum())} (n={len(v)})")
            out.append(f"   ทำนาย {pred} {len(gg)} วัน : " + " ; ".join(cells))
    return out


def main():
    tok = get_token()
    retail = retail_daily(sheets_get(tok, SHEET_ID, "Entry_Eppo!A1:S6000"))
    print(f"[RETAIL] Entry_Eppo แถวจริง (ไม่ใช่ CARRIED) {len(retail)} แถว")
    for line in retail_report(retail_changes(retail)):
        print(line)
    prices = build_frame(sheets_get(tok, OILBOT_SHEET_ID, "PriceLog!A1:Q200000")).reset_index(drop=True)
    for line in lag_report(prices, retail):
        print(line)
    ol = load_outlook(sheets_get(tok, SHEET_ID, f"{TABS['OUTLOOK_LOG']}!A1:R50000"))
    for line in forecast_report(retail, ol):
        print(line)


if __name__ == "__main__":
    main()
