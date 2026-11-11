"""
backtest กฎตัดสินใจของเจ้าของระบบ (เร่งซื้อ / ชะลอ / รอดูเทรนด์) กับข้อมูลรายวันย้อนหลัง — อ่านอย่างเดียว ไม่เขียนชีท
ตัวแปร (จากเอกสารของเจ้าของระบบ) : MOPS Δ1D/Δ3D (ตัวกำหนดทิศ) · WTI (ยืนยันเทรนด์) · ค่าการตลาดเทียบค่ากลาง (G95 3.37, ดีเซล 1.94 ; ใช้คอลัมน์ MARKETING MARGIN ของ EPPO)
         · สถานะกองทุน (ติดลบ/วิกฤติ = แรงขึ้นราคา , บวก = แรงกดราคา ; ใช้ Entry_OilFund_Sustainability)
กฎ :  BUY  = MOPS ขึ้น (Δ1D>0 และ Δ3D>0) และ WTI ไม่ขัดแย้ง (ไม่ลง) และ ค่าการตลาด < ค่ากลาง และ กองทุนติดลบ/วิกฤติ
      WAIT = MOPS ลง (Δ1D<0 และ Δ3D<0) และ WTI ลง และ กองทุนไม่วิกฤติ(ไม่ติดลบ)
      อื่น ๆ = UNCLEAR (รอดูเทรนด์ → ชะลอ)
ผลลัพธ์ที่วัด : ต้นทุนหน้าคลัง = WHOLESALE (WS) ใน Entry_Eppo เปลี่ยนจากวัน t ไป t+1 และ t+3 (เฉพาะแถวที่ไม่ใช่ CARRIED) ; รองลงมา RETAIL
แยกส่วนประกอบ (ablation) : MOPS อย่างเดียว / MOPS+WTI / MOPS+WTI+ค่าการตลาด / ทั้งหมด (รวมกองทุน) เพื่อดูว่าตัวแปรไหนเพิ่มข้อมูลจริง
ข้อจำกัด : EPPO อัปเดตเป็นช่วง (บางวันค่าเท่าเดิมหลายวัน) จึงเป็นตัวแทนของการปรับราคาหน้าคลังที่หยาบ ; ช่วงฤดูข้อมูล มี.ค.–พ.ย. 2569 กองทุนติดลบ/วิกฤติเกือบทั้งช่วง กฎ WAIT ที่ต้องกองทุนไม่วิกฤติจึงแทบไม่ทำงาน

ใช้: python diag_rule_backtest.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY)
"""
import numpy as np
import pandas as pd

from common import SHEET_ID, get_token, sheets_get, to_ts

MEDIAN_MM = {"G95": 3.37, "B7": 1.94}
EPPO_FUEL = {"GASOHOL95 E10": "G95", "H-DIESEL": "B7"}
MOPS_FUEL = {"G95": "G95", "DS": "B7"}
UP = 0.10                       # ขยับ >= 10 สต. = ขึ้น/ลง
VARIANTS = ("MOPS", "MOPS+WTI", "MOPS+WTI+margin", "ทั้งหมด(+กองทุน:ยอดติดลบ)", "ทั้งหมด(+กองทุน:Status≠OK)", "ทั้งหมด(+กองทุน:CRITICAL)")


def _hdr(values):
    return [str(h).strip() for h in values[0]]


def mops_daily(values: list) -> pd.DataFrame:
    h = _hdr(values)
    ix = {c: h.index(c) for c in ("Date", "Oil Type", "Price (USD/BBL)")}
    rows = []
    for r in values[1:]:
        g = lambda i: r[i] if i < len(r) else ""
        f, day, p = MOPS_FUEL.get(str(g(ix["Oil Type"])).strip()), to_ts(g(ix["Date"])), g(ix["Price (USD/BBL)"])
        try:
            p = float(p)
        except (TypeError, ValueError):
            continue
        if f and not pd.isna(day):
            rows.append((day.normalize(), f, p))
    d = pd.DataFrame(rows, columns=["day", "fuel", "mops"]).drop_duplicates(["day", "fuel"], keep="last")
    return d.sort_values(["fuel", "day"]).reset_index(drop=True)


def wti_daily(values: list) -> pd.DataFrame:
    h = _hdr(values)
    ix = {c: h.index(c) for c in ("Date", "Price")}
    rows = []
    for r in values[1:]:
        try:
            p = float(r[ix["Price"]])
        except (IndexError, TypeError, ValueError):
            continue
        day = to_ts(r[ix["Date"]]) if len(r) > ix["Date"] else pd.NaT
        if not pd.isna(day):
            rows.append((day.normalize(), p))
    return pd.DataFrame(rows, columns=["day", "wti"]).drop_duplicates("day", keep="last").sort_values("day").reset_index(drop=True)


def eppo_daily(values: list) -> pd.DataFrame:
    h = _hdr(values)
    ix = {c: h.index(c) for c in ("Date", "oil type", "WHOLESALE (WS)", "MARKETING MARGIN", "RETAIL")}
    src = h.index("Source") if "Source" in h else None
    rows = []
    for r in values[1:]:
        g = lambda i: r[i] if i < len(r) else ""
        f, day = EPPO_FUEL.get(str(g(ix["oil type"])).strip()), to_ts(g(ix["Date"]))
        if f is None or pd.isna(day) or (src is not None and str(g(src)).strip().upper() == "CARRIED"):
            continue
        try:
            rows.append((day.normalize(), f, float(g(ix["WHOLESALE (WS)"])), float(g(ix["MARKETING MARGIN"])), float(g(ix["RETAIL"]))))
        except (TypeError, ValueError):
            continue
    d = pd.DataFrame(rows, columns=["day", "fuel", "ws", "mm", "retail"]).drop_duplicates(["day", "fuel"], keep="last")
    return d.sort_values(["fuel", "day"]).reset_index(drop=True)


def fund_daily(values: list) -> pd.DataFrame:
    """วัน → กองทุนติดลบ/วิกฤติ (True) หรือไม่ ; ใช้ Total_Balance < 0 หรือ Status = CRITICAL"""
    h = _hdr(values)
    ix = {c: h.index(c) for c in h if c in ("Date", "Total_Balance (ล้านบาท)", "Status")}
    rows = []
    for r in values[1:]:
        g = lambda c: r[ix[c]] if c in ix and ix[c] < len(r) else ""
        day = to_ts(g("Date"))
        try:
            bal = float(g("Total_Balance (ล้านบาท)"))
        except (TypeError, ValueError):
            bal = np.nan
        if not pd.isna(day):
            rows.append((day.normalize(), bool((not np.isnan(bal) and bal < 0) or str(g("Status")).strip().upper() == "CRITICAL"), str(g("Status")).strip()))
    return pd.DataFrame(rows, columns=["day", "crisis", "status"]).drop_duplicates("day", keep="last").sort_values("day").reset_index(drop=True)


def add_deltas(s: pd.Series) -> pd.DataFrame:
    """Δ1D, Δ3D ตามวันทำการที่มีข้อมูล (ไม่ใช่วันปฏิทิน)"""
    return pd.DataFrame({"d1": s.diff(1), "d3": s.diff(3)})


def classify(f: dict, variant: str) -> str:
    """f: m1,m3 (MOPS), w1 (WTI Δ1D), mm_gap (ค่าการตลาด − ค่ากลาง), crisis (bool) → BUY / WAIT / UNCLEAR"""
    m_up, m_dn = f["m1"] > 0 and f["m3"] > 0, f["m1"] < 0 and f["m3"] < 0
    w_ok_up, w_dn = not (f["w1"] < 0), f["w1"] < 0
    low = f["mm_gap"] < 0
    if variant == "MOPS":
        return "BUY" if m_up else "WAIT" if m_dn else "UNCLEAR"
    if variant == "MOPS+WTI":
        return "BUY" if m_up and w_ok_up else "WAIT" if m_dn and w_dn else "UNCLEAR"
    if variant == "MOPS+WTI+margin":
        return "BUY" if m_up and w_ok_up and low else "WAIT" if m_dn and w_dn else "UNCLEAR"
    key = {"ทั้งหมด(+กองทุน:ยอดติดลบ)": "crisis", "ทั้งหมด(+กองทุน)": "crisis", "ทั้งหมด(+กองทุน:Status≠OK)": "fund_any", "ทั้งหมด(+กองทุน:CRITICAL)": "fund_crit"}[variant]
    return "BUY" if m_up and w_ok_up and low and f[key] else "WAIT" if m_dn and w_dn and not f[key] else "UNCLEAR"


def build_table(mops: pd.DataFrame, wti: pd.DataFrame, eppo: pd.DataFrame, fund: pd.DataFrame) -> pd.DataFrame:
    """1 แถวต่อ (วัน, ชนิด) ที่มีฟีเจอร์ครบ : ฟีเจอร์ ณ วัน t และผล ΔWS/ΔRetail ไป t+1 / t+3 (ตามแถว EPPO ที่ไม่ใช่ CARRIED ถัดไป)"""
    w = wti.assign(**{"w1": wti.wti.diff(1), "w3": wti.wti.diff(3)}).set_index("day")
    fd = fund.set_index("day")["crisis"]
    fs = fund.set_index("day")["status"].str.upper()
    out = []
    for fuel in ("G95", "B7"):
        m = mops[mops.fuel == fuel].set_index("day")["mops"]
        feats = add_deltas(m).rename(columns={"d1": "m1", "d3": "m3"})
        e = eppo[eppo.fuel == fuel].set_index("day")
        for day, r in feats.dropna().iterrows():
            wv = w.reindex([day]).iloc[0]
            crisis = fd.reindex(fd.index.union([day])).sort_index().ffill().get(day, np.nan)
            stat = fs.reindex(fs.index.union([day])).sort_index().ffill().get(day, np.nan)
            if wv.isna().any() or pd.isna(crisis) or pd.isna(stat):
                continue
            # ราคาหน้าคลังที่ใช้เทียบ : แถว EPPO ล่าสุดที่ไม่เกินวัน t (ฐาน) และ แถวที่ t+1, t+3 วันปฏิทิน (ต้องมีจริง)
            base = e.loc[:day]
            if base.empty or (day - base.index[-1]).days > 1:
                continue
            row = {"day": day, "fuel": fuel, "m1": r.m1, "m3": r.m3, "w1": wv.w1, "w3": wv.w3, "crisis": bool(crisis),
                   "fund_any": stat != "OK", "fund_crit": stat == "CRITICAL", "mm_gap": float(base.mm.iloc[-1] - MEDIAN_MM[fuel]), "ws0": float(base.ws.iloc[-1]), "rt0": float(base.retail.iloc[-1])}
            for k in (1, 3):
                nxt = e.reindex([day + pd.Timedelta(days=k)])
                row[f"dws{k}"] = float(nxt.ws.iloc[0] - row["ws0"]) if not nxt.ws.isna().all() else np.nan
                row[f"drt{k}"] = float(nxt.retail.iloc[0] - row["rt0"]) if not nxt.retail.isna().all() else np.nan
            out.append(row)
    return pd.DataFrame(out)


def lag_report(t: pd.DataFrame) -> list:
    out = []
    np.seterr(all="ignore")
    for fuel, g in t.groupby("fuel"):
        parts = []
        for k in (1, 3):
            x = g.dropna(subset=[f"dws{k}"])
            parts.append(f"t→t+{k}: n={len(x)} สหสัมพันธ์ MOPSΔ1D กับ ΔWS {np.corrcoef(x.m1, x[f'dws{k}'])[0, 1]:+.2f}" if len(x) > 5 else f"t→t+{k}: n={len(x)} น้อยเกินไป")
        out.append(f"[RULE-LAG] {fuel}: " + " ; ".join(parts))
    return out


def class_report(t: pd.DataFrame) -> list:
    out = []
    base = t.dropna(subset=["dws3"])
    out.append(f"[RULE] วัน×ชนิดที่ใช้ {len(t)} (มีผล t+3 {len(base)}) {t.day.min():%d/%m}–{t.day.max():%d/%m} ; ไม่มีเงื่อนไข : ΔWS 3 วัน เฉลี่ย {base.dws3.mean():+.3f} ขึ้น≥{UP:.2f} {(base.dws3 >= UP).mean():.0%} ลง≤-{UP:.2f} {(base.dws3 <= -UP).mean():.0%}")
    for v in VARIANTS:
        t = t.assign(cls=[classify(r, v) for r in t.to_dict("records")])
        out.append(f"[RULE-CLASS] {v}")
        for c in ("BUY", "WAIT", "UNCLEAR"):
            g = t[(t.cls == c)].dropna(subset=["dws3"])
            if g.empty:
                out.append(f"   {c}: ไม่มีวันที่เข้าเงื่อนไข")
                continue
            out.append(f"   {c}: n={len(g)} ; ΔWS 1 วัน {g.dws1.mean():+.3f} 3 วัน {g.dws3.mean():+.3f} ; ขึ้น≥{UP:.2f} {(g.dws3 >= UP).mean():.0%} ลง≤-{UP:.2f} {(g.dws3 <= -UP).mean():.0%} ; "
                       f"ปลีก 3 วัน {g.drt3.mean():+.3f}")
        buy, wait = t[t.cls == "BUY"].dropna(subset=["dws3"]), t[t.cls.isin(["WAIT", "UNCLEAR"])].dropna(subset=["dws3"])
        if len(buy) >= 5 and len(wait) >= 5:
            diff = buy.dws3.mean() - wait.dws3.mean()
            lo, hi = block_ci(buy, wait)
            out.append(f"   BUY − (WAIT+UNCLEAR) ต่างกัน {diff:+.3f} บาท/ลิตร (95% {lo:+.3f}..{hi:+.3f} bootstrap ตามสัปดาห์)")
    return out


def block_ci(a: pd.DataFrame, b: pd.DataFrame, reps: int = 2000, seed: int = 7):
    """bootstrap เป็นบล็อกตามสัปดาห์ (วันติดกันสัมพันธ์กัน)"""
    rng = np.random.default_rng(seed)
    wa, wb = a.day.dt.to_period("W").astype(str), b.day.dt.to_period("W").astype(str)
    weeks = sorted(set(wa) | set(wb))
    sa, sb = a.groupby(wa).dws3.agg(["sum", "count"]), b.groupby(wb).dws3.agg(["sum", "count"])
    diffs = []
    for _ in range(reps):
        pick = rng.choice(weeks, size=len(weeks), replace=True)
        na = sum(sa.loc[w, "count"] for w in pick if w in sa.index)
        nb = sum(sb.loc[w, "count"] for w in pick if w in sb.index)
        if na == 0 or nb == 0:
            continue
        diffs.append(sum(sa.loc[w, "sum"] for w in pick if w in sa.index) / na - sum(sb.loc[w, "sum"] for w in pick if w in sb.index) / nb)
    return (float(np.percentile(diffs, 2.5)), float(np.percentile(diffs, 97.5))) if diffs else (float("nan"), float("nan"))


def _diff(t: pd.DataFrame, variant: str):
    t = t.assign(cls=[classify(r, variant) for r in t.to_dict("records")]).dropna(subset=["dws3"])
    buy, rest = t[t.cls == "BUY"], t[t.cls != "BUY"]
    wait = t[t.cls == "WAIT"]
    return len(buy), len(wait), (buy.dws3.mean() if len(buy) else np.nan), (wait.dws3.mean() if len(wait) else np.nan), (buy.dws3.mean() - rest.dws3.mean() if len(buy) and len(rest) else np.nan)


def period_report(t: pd.DataFrame) -> list:
    """ความนิ่ง : แบ่งครึ่งตามเวลา (จำนวนวันเท่ากัน) และตัดสัปดาห์ที่ราคาขยับแรงสุด 3 สัปดาห์ออก"""
    t = t.dropna(subset=["dws3"]).sort_values("day")
    cut = t.day.sort_values().iloc[len(t) // 2]
    wk = t.day.dt.to_period("W").astype(str)
    big = t.assign(w=wk).groupby("w").dws3.apply(lambda x: x.abs().sum()).sort_values(ascending=False).head(3).index
    parts = {"ครึ่งแรก": t[t.day < cut], "ครึ่งหลัง": t[t.day >= cut], "ตัด 3 สัปดาห์ที่ขยับแรงสุด": t[~wk.isin(big)]}
    out = [f"[RULE-PERIOD] แบ่งที่ {cut:%d/%m/%y} ; สัปดาห์ที่ตัดออก: {', '.join(big)}"]
    for name, g in parts.items():
        for v in ("MOPS", "MOPS+WTI"):
            nb, nw, mb, mw, d = _diff(g, v)
            out.append(f"   {name} {v}: n={len(g)} ; BUY n={nb} ΔWS3 {mb:+.3f} | WAIT n={nw} {mw:+.3f} | BUY−ที่เหลือ {d:+.3f}")
    return out


def margin_report(t: pd.DataFrame) -> list:
    """ค่าการตลาดแบบต่อเนื่อง : ในวันที่ MOPS ขึ้น (Δ1D>0 และ Δ3D>0) ต้นทุน 3 วันต่างกันตามระดับค่าการตลาด (3 กลุ่มเท่ากัน) หรือไม่"""
    x = t.dropna(subset=["dws3"])
    up = x[(x.m1 > 0) & (x.m3 > 0)]
    out = [f"[RULE-MARGIN] ทุกวัน สหสัมพันธ์ ค่าการตลาด−ค่ากลาง กับ ΔWS 3 วัน {np.corrcoef(x.mm_gap, x.dws3)[0, 1]:+.2f} (n={len(x)}) ; เฉพาะวัน MOPS ขึ้น {np.corrcoef(up.mm_gap, up.dws3)[0, 1]:+.2f} (n={len(up)})"]
    if len(up) >= 9:
        up = up.assign(tier=pd.qcut(up.mm_gap, 3, labels=["ต่ำ", "กลาง", "สูง"], duplicates="drop"))
        for tier, g in up.groupby("tier", observed=True):
            out.append(f"   ค่าการตลาด{tier} ({g.mm_gap.min():+.2f}..{g.mm_gap.max():+.2f}) : n={len(g)} ΔWS3 {g.dws3.mean():+.3f} ขึ้น≥{UP:.2f} {(g.dws3 >= UP).mean():.0%}")
    return out


def main():
    tok = get_token()
    mops = mops_daily(sheets_get(tok, SHEET_ID, "Entry_MOPS!A1:F6000"))
    wti = wti_daily(sheets_get(tok, SHEET_ID, "Entry_WTI!A1:H6000"))
    eppo = eppo_daily(sheets_get(tok, SHEET_ID, "Entry_Eppo!A1:S6000"))
    fund = fund_daily(sheets_get(tok, SHEET_ID, "Entry_OilFund_Sustainability!A1:H3000"))
    print(f"[RULE] MOPS {len(mops)} แถว ({mops.day.min():%d/%m/%y}–{mops.day.max():%d/%m/%y}) WTI {len(wti)} EPPO {len(eppo)} (G95 {int((eppo.fuel == 'G95').sum())}, B7 {int((eppo.fuel == 'B7').sum())})"
          f" กองทุน {len(fund)} วัน ติดลบ/วิกฤติ {fund.crisis.mean():.0%} ; สถานะ: " + ", ".join(f"{k} {v}" for k, v in fund.status.value_counts().items()))
    t = build_table(mops, wti, eppo, fund)
    if t.empty:
        print("[RULE] ไม่มีวันที่ฟีเจอร์ครบ")
        return
    for line in lag_report(t) + class_report(t) + period_report(t) + margin_report(t):
        print(line)


if __name__ == "__main__":
    main()
