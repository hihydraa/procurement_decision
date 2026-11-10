"""
ตรวจว่า "ราคาต่ำสุดของตลาด" (ที่ OutlookLog ใช้ตัดสินผลจริง) ถูกถือโดยใคร และตามราคาคลังที่ทีมซื้อจริงหรือไม่ — อ่านอย่างเดียว ไม่เขียนชีท
เทียบ 2 เกณฑ์บนจุดอ้างอิงเดียวกัน : ทุกผู้ขาย (เหมือน PriceBook) กับ "คลังหลัก" (ไออาร์ ระยอง/อยุธยา, ดาว สระบุรี/ระยอง ตามเอกสาร 07)
แล้วให้คะแนนคำพยากรณ์ใน OutlookLog (รอบ 20:30 ที่ออกตรงเวลา) ใหม่ด้วยทั้งสองเกณฑ์

ใช้: python diag_price_min.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY)
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from common import SHEET_ID, TABS, TZ_NAME, get_token, sheets_get, to_ts

MAX_AGE_DAYS = 3                      # เท่ากับ signals.MARKET_MAX_AGE_DAYS
CORE = [("ไออาร์", "ระยอง"), ("ไออาร์", "อยุธยา"), ("สตาร์ฟูเอลส์", "สระบุรี"), ("สตาร์ฟูเอลส์", "ระยอง")]   # คลังหลักตามเอกสาร 07 (สมมติฐาน ต้องยืนยัน)
SMALL = 0.10                          # ขยับ >= 10 สต. นับว่าขึ้น/ลง (เท่ากับ model.OUTLOOK_SMALL)


def is_core(key: str) -> bool:
    provider, _, warehouse = key.partition("|")
    return any(p in provider and w in warehouse for p, w in CORE)


def clean_prices(prices: pd.DataFrame) -> pd.DataFrame:
    p = prices.copy()
    p["effective"] = pd.to_datetime(p["effective"], errors="coerce")
    p["price"] = pd.to_numeric(p["price"], errors="coerce")
    p = p.dropna(subset=["effective", "price"])
    p = p[p.price.between(15, 70)]
    p["key"] = p.provider.astype(str) + "|" + p.warehouse.astype(str)
    return p


def quote_table(p: pd.DataFrame, fuel: str, times, only_core: bool = False) -> pd.DataFrame:
    """ต่อเวลา t : ราคาต่ำสุด, ผู้ถือราคา (provider|warehouse), เวลาโพสต์ของราคานั้น, อายุ (ชม.) — วิธีเดียวกับ PriceBook.min_at"""
    times = pd.DatetimeIndex(times)
    grid = pd.DataFrame({"t": times}).sort_values("t")
    cols, keys = [], []
    for (f, key), g in p[p.fuel == fuel].groupby(["fuel", "key"]):
        if only_core and not is_core(key):
            continue
        g = g.sort_values("effective")[["effective", "price"]].drop_duplicates("effective", keep="last")
        m = pd.merge_asof(grid, g, left_on="t", right_on="effective", tolerance=pd.Timedelta(days=MAX_AGE_DAYS))
        cols.append(m[["price", "effective"]].to_numpy())
        keys.append(key)
    out = pd.DataFrame({"t": grid.t.to_numpy(), "price": np.nan, "holder": "", "quote_time": pd.NaT, "age_h": np.nan})
    if cols:
        price = np.stack([c[:, 0].astype(float) for c in cols])
        qt = np.stack([pd.to_datetime(c[:, 1]).to_numpy() for c in cols])
        filled = np.where(np.isnan(price), np.inf, price)
        idx = filled.argmin(axis=0)
        ok = np.isfinite(filled[idx, np.arange(len(grid))])
        out.loc[ok, "price"] = price[idx, np.arange(len(grid))][ok]
        out.loc[ok, "holder"] = np.array(keys)[idx][ok]
        out.loc[ok, "quote_time"] = pd.to_datetime(qt[idx, np.arange(len(grid))])[ok]
        out["age_h"] = (out["t"] - pd.to_datetime(out["quote_time"])).dt.total_seconds() / 3600
    return out.set_index("t").reindex(times)


def signed(tier: str) -> int:
    return 1 if str(tier).startswith("UP") else -1 if str(tier).startswith("DOWN") else 0


def rescore(rows: pd.DataFrame, p: pd.DataFrame, now: pd.Timestamp) -> pd.DataFrame:
    """ให้คะแนนแถว OutlookLog ใหม่ด้วยราคาต่ำสุดทุกผู้ขาย (all) และคลังหลัก (core) : คืนแถวละ (วัน, ชนิด, ขอบฟ้า)"""
    out = []
    for fuel in rows["fuel"].unique():
        sub = rows[rows["fuel"] == fuel]
        t0 = pd.DatetimeIndex(sub["ref"])
        for hz in sorted(sub["hours"].unique()):
            s = sub[sub["hours"] == hz]
            r0, r1 = pd.DatetimeIndex(s["ref"]), pd.DatetimeIndex(s["ref"]) + pd.Timedelta(hours=int(hz))
            for name, only_core in (("all", False), ("core", True)):
                a, b = quote_table(p, fuel, r0, only_core)["price"].to_numpy(), quote_table(p, fuel, r1, only_core)["price"].to_numpy()
                ready = (r1 + pd.Timedelta(minutes=30)) <= now
                d = np.where(ready, b - a, np.nan)
                for i, (_, row) in enumerate(s.iterrows()):
                    out.append({"date": row["date"], "fuel": fuel, "hours": int(hz), "tier": row["tier"], "basis": name,
                                "logged": row.get("outcome_delta"), "delta": d[i]})
    return pd.DataFrame(out)


def summarize(df: pd.DataFrame) -> str:
    lines = []
    for (fuel, hz, basis), g in df.groupby(["fuel", "hours", "basis"]):
        g = g.dropna(subset=["delta"])
        if g.empty:
            continue
        sg = g["tier"].map(signed)
        mv = g["delta"] * sg
        calls = sg != 0
        right, wrong = int((mv[calls] >= SMALL).sum()), int((mv[calls] <= -SMALL).sum())
        unc_moved = int(((sg == 0) & (g["delta"].abs() >= SMALL)).sum())
        lines.append(f"  {fuel:>3} H{hz:<2} {basis:<4} วัน={len(g):>2} ฟันธง={int(calls.sum()):>2} ถูก={right:>2} ผิด={wrong:>2} ใกล้เดิม={int(calls.sum()) - right - wrong:>2}"
                     f" กำไร/วัน={mv.sum() / len(g):+.3f} | ไม่ชัดแต่ขยับ≥10สต={unc_moved}/{int((sg == 0).sum())} | ราคาไม่ขยับเลย(0.00)={int((g['delta'].abs() < 0.005).sum())}")
    return "\n".join(lines)


def main():
    now = pd.Timestamp(datetime.now(ZoneInfo(TZ_NAME)).replace(tzinfo=None))
    tok = get_token()
    raw = sheets_get(tok, SHEET_ID, f"{TABS['FEED_PRICES']}!A1:E200000")
    prices = pd.DataFrame(raw[1:], columns=raw[0])
    prices["effective"] = prices["effective"].map(to_ts)
    p = clean_prices(prices)
    print(f"[DIAG] price rows={len(p)} last={p.effective.max()} now={now:%Y-%m-%d %H:%M}")

    days = pd.date_range("2026-10-23", now.normalize(), freq="D")
    refs = pd.DatetimeIndex([d + pd.Timedelta(hours=20.5) for d in days if d + pd.Timedelta(hours=20.5) <= now])
    for fuel in ("G95", "B7"):
        a, c = quote_table(p, fuel, refs), quote_table(p, fuel, refs, only_core=True)
        print(f"[DIAG] {fuel} 20:30 : date | ทุกผู้ขาย (ราคา, ผู้ถือ, อายุชม.) | คลังหลัก (ราคา, ผู้ถือ) | ส่วนต่าง all-core")
        for t in refs:
            x, y = a.loc[t], c.loc[t]
            print(f"  {t:%m-%d} | {x.price:6.2f} {str(x.holder)[:28]:<28} {x.age_h:5.1f}h {'(คลังหลัก)' if is_core(str(x.holder)) else ''}"
                  f" | {y.price:6.2f} {str(y.holder)[:24]:<24} | {x.price - y.price:+.2f}")
        outside = int((~a["holder"].map(lambda k: is_core(str(k)))).sum())
        old = int((a["age_h"] > 24).sum())
        print(f"[DIAG] {fuel}: ผู้ถือราคาต่ำสุดไม่ใช่คลังหลัก {outside}/{len(refs)} วัน ; ราคาที่ถือเก่ากว่า 24 ชม. {old}/{len(refs)} วัน ; "
              f"ส่วนต่าง all-core เฉลี่ย {float((a['price'] - c['price']).mean()):+.2f} บาท")
        for hz in (24,):
            tgt = refs + pd.Timedelta(hours=hz)
            ok = (tgt + pd.Timedelta(minutes=30)) <= now
            da = quote_table(p, fuel, tgt[ok])["price"].to_numpy() - a.loc[refs[ok], "price"].to_numpy()
            dc = quote_table(p, fuel, tgt[ok], only_core=True)["price"].to_numpy() - c.loc[refs[ok], "price"].to_numpy()
            both = ~np.isnan(da) & ~np.isnan(dc)
            same = int((np.sign(da[both]) == np.sign(dc[both])).sum())
            print(f"[DIAG] {fuel} H{hz}: ขยับของ all vs core วันที่ทิศตรงกัน {same}/{int(both.sum())} ; all ไม่ขยับ(0.00) แต่ core ขยับ≥10สต "
                  f"{int(((np.abs(da[both]) < 0.005) & (np.abs(dc[both]) >= SMALL)).sum())} วัน ; correlation {float(np.corrcoef(da[both], dc[both])[0, 1]):.2f}")

    ol = sheets_get(tok, SHEET_ID, f"{TABS['OUTLOOK_LOG']}!A1:R50000")
    d = pd.DataFrame([r + [""] * (len(ol[0]) - len(r)) for r in ol[1:]], columns=ol[0])
    d["asof"], d["ref"] = pd.to_datetime(d["asof"], errors="coerce"), pd.to_datetime(d["ref"], errors="coerce")
    d["hours"] = pd.to_numeric(d["hours"], errors="coerce")
    d = d[(d["ref"].dt.strftime("%H:%M") == "20:30") & ((d["asof"] - d["ref"]) <= pd.Timedelta(hours=3))]
    if "backfilled" in d.columns:
        d = d[pd.to_numeric(d["backfilled"], errors="coerce").fillna(0) == 0]
    d = d.sort_values("asof").drop_duplicates(["date", "fuel", "hours"], keep="first")
    res = rescore(d[["date", "ref", "fuel", "hours", "tier", "outcome_delta"]], p, now)
    mism = res[(res.basis == "all") & res.delta.notna()]
    lg = pd.to_numeric(mism["logged"], errors="coerce")
    print(f"[DIAG] ตรวจความตรงกับค่าที่บันทึกไว้เดิม: ต่างจาก outcome_delta ใน OutlookLog เกิน 0.02 บาท {int(((mism.delta - lg).abs() > 0.02).sum())}/{len(mism)} แถว")
    print("[DIAG] ให้คะแนนใหม่ (รอบ 20:30 ตรงเวลา, ถูก/ผิดนับเมื่อขยับ ≥10 สต.) — all = เกณฑ์เดิม, core = เฉพาะคลังหลัก:")
    print(summarize(res))
    print("[DIAG] คลังหลักที่ใช้ (สมมติฐานจากเอกสาร 07):", "; ".join(f"{p_}/{w_}" for p_, w_ in CORE))


if __name__ == "__main__":
    main()
