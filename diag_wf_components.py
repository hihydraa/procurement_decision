"""
walk-forward : ทายการเปลี่ยนของราคาหน้าคลัง (WS) วันถัดไปด้วยโมเดล "แยกส่วนประกอบ" เทียบกับตัวเทียบอื่น — อ่านอย่างเดียว ไม่เขียนชีท
WS = EX-REFIN. + (EXCISE TAX + M. TAX + CONSV. FUND) + OIL FUND  (คอลัมน์ของ Entry_Eppo ; ตรวจแล้วตรงกับตัวเลขในชีท)
ส่วนประกอบ : ราคาหน้าโรงกลั่น (ขึ้นกับ MOPS/WTI → ต้องทาย) · กองทุนและภาษี (ประกาศล่วงหน้า → ถ้ารู้ตั้งแต่เย็นก็ไม่ต้องทาย)
โมเดลที่เทียบ (วัน t ฟีเจอร์ MOPS Δ1D/Δ3D, WTI Δ1D/Δ3D, ค่าการตลาด−ค่ากลาง ; เป้า ΔWS จาก t ไป t+1) :
  ZERO   = ไม่เปลี่ยน
  RULE   = MOPS Δ1D × (0.12 ดีเซล / 0.11 G95)  (กฎของงานตั้งเวลา)
  DIRECT = ridge ทาย ΔWS ตรงจากฟีเจอร์
  A      = ridge ทาย ΔEX-REFIN แล้วถือว่ากองทุน/ภาษีไม่เปลี่ยน (ไม่รู้ประกาศ)
  B      = ridge ทาย ΔEX-REFIN + (Δกองทุนและภาษีจริงที่ประกาศแล้ว)  — ขอบบนของประโยชน์จากการรู้ประกาศล่วงหน้า
  C      = ridge ทาย ΔWS จากฟีเจอร์ + Δกองทุน/ภาษีที่ประกาศแล้ว "พร้อมกัน" (ให้ข้อมูลกำหนดน้ำหนักเอง ไม่บวกตรง ๆ)
  CR     = C + การเปลี่ยนของส่วนที่ไม่อยู่ในคอลัมน์ (resid = WS − EX-REFIN − กองทุน − ภาษีในชีท เช่น ลดภาษีชั่วคราว) — ขอบบนถ้ารู้ส่วนนั้นล่วงหน้า
  ข้อสมมติของ B : ประกาศกองทุน/ภาษีของวัน t+1 ทราบแล้วตั้งแต่เย็นวัน t (ต้องตรวจจากเวลาประกาศใน Entry_FundNotice ต่อ) ; ถ้าไม่จริง B จะดีเกินจริง
walk-forward : ฝึกด้วยวันที่ outcome ทราบแล้ว (วัน ≤ t−2) แบบขยายหน้าต่าง ฝึกซ้ำทุกวัน ต้องมีอย่างน้อย MIN_TRAIN วัน/ชนิด
ข้อจำกัด : EPPO อัปเดตเป็นช่วง ; ตัวอย่างน้อย ; ช่วงต้นปี (วิกฤติ) ผันผวนแรงมากและครอบงำ MAE จึงรายงานแยกครึ่งแรก/ครึ่งหลังของวันที่ทดสอบ

ใช้: python diag_wf_components.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY)
"""
import numpy as np
import pandas as pd

from common import SHEET_ID, get_token, sheets_get, to_ts
from ingest import cell_time
from diag_rule_backtest import EPPO_FUEL, MEDIAN_MM, add_deltas, mops_daily, wti_daily

MIN_TRAIN = 40
RIDGE = 3.0
UP = 0.10
K_RULE = {"G95": 0.11, "B7": 0.12}
MODELS = ("ZERO", "RULE", "DIRECT", "A", "B", "C", "CR")
FEATS = ["m1", "m3", "w1", "w3", "mm_gap"]


def eppo_components(values: list) -> pd.DataFrame:
    """Entry_Eppo → วัน × ชนิด : ws, exref, fund, other(= EXCISE + M.TAX + CONSV.), mm  (ไม่นับแถว CARRIED)"""
    h = [str(x).strip() for x in values[0]]
    need = ["Date", "oil type", "WHOLESALE (WS)", "EX-REFIN.", "EXCISE TAX", "M. TAX", "OIL FUND", "CONSV. FUND", "MARKETING MARGIN"]
    ix = {c: h.index(c) for c in need}
    src = h.index("Source") if "Source" in h else None
    rows = []
    for r in values[1:]:
        g = lambda i: r[i] if i < len(r) else ""
        f, day = EPPO_FUEL.get(str(g(ix["oil type"])).strip()), to_ts(g(ix["Date"]))
        if f is None or pd.isna(day) or (src is not None and str(g(src)).strip().upper() == "CARRIED"):
            continue
        try:
            v = {c: float(g(ix[c])) for c in need[2:]}
        except (TypeError, ValueError):
            continue
        rows.append((day.normalize(), f, v["WHOLESALE (WS)"], v["EX-REFIN."], v["OIL FUND"], v["EXCISE TAX"] + v["M. TAX"] + v["CONSV. FUND"], v["MARKETING MARGIN"],
                     v["EXCISE TAX"]))
    d = pd.DataFrame(rows, columns=["day", "fuel", "ws", "exref", "fund", "other", "mm", "excise"]).drop_duplicates(["day", "fuel"], keep="last")
    d["resid"] = (d.ws - d.exref - d.fund - d.other).round(4)
    return d.sort_values(["fuel", "day"]).reset_index(drop=True)


def load_notices(values: list) -> pd.DataFrame:
    h = [str(x).strip() for x in values[0]]
    ix = {c: h.index(c) for c in ("msg_date", "msg_time", "effective_date", "diesel_new")}
    rows = []
    for r in values[1:]:
        g = lambda c: r[ix[c]] if ix[c] < len(r) else ""
        md, ed = to_ts(g("msg_date")), to_ts(g("effective_date"))
        t = str(cell_time(g("msg_time")))
        try:
            hh = int(t[:2]) + int(t[3:5]) / 60
        except ValueError:
            continue
        try:
            dn = float(g("diesel_new"))
        except (TypeError, ValueError):
            dn = np.nan
        if not pd.isna(md) and not pd.isna(ed):
            rows.append((md.normalize(), ed.normalize(), hh, dn))
    return pd.DataFrame(rows, columns=["msg_day", "eff_day", "msg_hour", "diesel_new"])


def build_dataset(mops: pd.DataFrame, wti: pd.DataFrame, eppo: pd.DataFrame) -> pd.DataFrame:
    """วัน t ที่มีทั้ง EPPO วัน t และ t+1 (ปฏิทิน) + ฟีเจอร์ MOPS/WTI/ค่าการตลาด ณ วัน t ; เป้า ΔWS และส่วนประกอบจาก t ไป t+1"""
    w = pd.DataFrame({"w1": wti.wti.diff(1).to_numpy(), "w3": wti.wti.diff(3).to_numpy()}, index=wti.day)
    out = []
    for fuel in ("G95", "B7"):
        m = mops[mops.fuel == fuel].set_index("day")["mops"]
        f = add_deltas(m).rename(columns={"d1": "m1", "d3": "m3"})
        e = eppo[eppo.fuel == fuel].set_index("day")
        for day, r in f.dropna().iterrows():
            nxt = day + pd.Timedelta(days=1)
            if day not in e.index or nxt not in e.index or day not in w.index or w.loc[day].isna().any():
                continue
            a, b = e.loc[day], e.loc[nxt]
            out.append({"day": day, "fuel": fuel, "m1": r.m1, "m3": r.m3, "w1": w.loc[day, "w1"], "w3": w.loc[day, "w3"], "mm_gap": a.mm - MEDIAN_MM[fuel],
                        "dws": b.ws - a.ws, "dex": b.exref - a.exref, "dknown": (b.fund - a.fund) + (b.other - a.other), "dres": b.resid - a.resid})
    return pd.DataFrame(out)


def ridge_fit(X: np.ndarray, y: np.ndarray, lam: float = RIDGE):
    mu, sd = X.mean(0), X.std(0)
    sd[sd == 0] = 1.0
    Z = (X - mu) / sd
    A = Z.T @ Z + lam * np.eye(Z.shape[1])
    beta = np.linalg.solve(A, Z.T @ (y - y.mean()))
    return lambda Xn: ((np.atleast_2d(Xn) - mu) / sd) @ beta + y.mean()


def walk_forward(ds: pd.DataFrame, min_train: int = MIN_TRAIN) -> pd.DataFrame:
    rows = []
    for fuel, g in ds.groupby("fuel"):
        g = g.sort_values("day").reset_index(drop=True)
        for i in range(len(g)):
            test = g.loc[i]
            tr = g[g.day <= test.day - pd.Timedelta(days=2)]
            if len(tr) < min_train:
                continue
            X, x = tr[FEATS].to_numpy(float), test[FEATS].to_numpy(float)
            p_ex, p_ws = ridge_fit(X, tr.dex.to_numpy(float))(x)[0], ridge_fit(X, tr.dws.to_numpy(float))(x)[0]
            Xc, xc = np.c_[X, tr.dknown.to_numpy(float)], np.r_[x, test.dknown]
            Xr, xr = np.c_[Xc, tr.dres.to_numpy(float)], np.r_[xc, test.dres]
            rows.append({"day": test.day, "fuel": fuel, "actual": test.dws, "ZERO": 0.0, "RULE": K_RULE[fuel] * test.m1, "DIRECT": p_ws, "A": p_ex,
                         "B": p_ex + test.dknown, "C": ridge_fit(Xc, tr.dws.to_numpy(float))(xc)[0], "CR": ridge_fit(Xr, tr.dws.to_numpy(float))(xr)[0]})
    return pd.DataFrame(rows)


def metrics(df: pd.DataFrame) -> list:
    out = []
    for m in MODELS:
        e = df[m] - df.actual
        big = df[df.actual.abs() >= UP]
        hit = (np.sign(big[m]) == np.sign(big.actual)).mean() if len(big) and m != "ZERO" else float("nan")
        called = df[df[m].abs() >= UP]
        chit = (np.sign(called[m]) == np.sign(called.actual))[called.actual.abs() >= UP].mean() if len(called) else float("nan")
        corr = np.corrcoef(df[m], df.actual)[0, 1] if m != "ZERO" and df[m].std() > 0 else float("nan")
        out.append(f"   {m:6s} MAE {e.abs().mean():.3f}  RMSE {np.sqrt((e ** 2).mean()):.3f}  corr {corr:+.2f}  ทิศถูก(วันที่ขยับ≥{UP:.2f}) {hit:.0%}  ทิศถูกเมื่อโมเดลฟันธง(|ทาย|≥{UP:.2f}) {chit:.0%} (n={len(called)})")
    return out


def variance_report(ds: pd.DataFrame) -> list:
    out = []
    for fuel, g in ds.groupby("fuel"):
        v = g[["dws", "dex", "dknown"]].var()
        cov = np.cov(g.dex, g.dknown)[0, 1]
        out.append(f"[WF-VAR] {fuel}: n={len(g)} ; ความแปรปรวนของ ΔWS รายวัน {v.dws:.3f} = โรงกลั่น {v.dex:.3f} + กองทุน/ภาษี {v.dknown:.3f} + 2×ความแปรปรวนร่วม {2 * cov:+.3f}"
                   f" ; วันที่กองทุน/ภาษีเปลี่ยน ≥{UP:.2f}: {(g.dknown.abs() >= UP).mean():.0%} ของวัน")
    return out


def identity_report(eppo: pd.DataFrame) -> list:
    """แถวที่ WS ไม่เท่า EX-REFIN + กองทุน + ภาษีในชีท : รวมเป็นช่วงวันที่ residual คงที่ต่อชนิด (น่าจะเป็นส่วนลดภาษี/รายการที่ไม่อยู่ในคอลัมน์)"""
    out = []
    bad = eppo[eppo.resid.abs() > 0.01]
    out.append(f"[WF-IDENT] แถวที่สมการไม่ตรง {len(bad)}/{len(eppo)} ; ต่างสูงสุด {eppo.resid.abs().max():.3f}")
    for fuel, g in eppo.groupby("fuel"):
        g = g.sort_values("day")
        run_start, prev, cur = None, None, None
        runs = []
        for _, r in g.iterrows():
            v = round(float(r.resid), 2)
            if cur is None or v != cur:
                if cur is not None and abs(cur) > 0.01:
                    runs.append((run_start, prev, cur))
                run_start, cur = r.day, v
            prev = r.day
        if cur is not None and abs(cur) > 0.01:
            runs.append((run_start, prev, cur))
        out.append(f"   {fuel}: " + (" ; ".join(f"{a:%d/%m}–{b:%d/%m} {v:+.2f}" for a, b, v in runs[:12]) if runs else "ตรงทุกแถว") + (f" ... (+{len(runs) - 12} ช่วง)" if len(runs) > 12 else ""))
    return out


def notice_report(notices: pd.DataFrame, eppo: pd.DataFrame) -> list:
    """Entry_FundNotice : ประกาศกองทุนออกกี่โมง มีผลวันไหน และตรงกับ OIL FUND ใน EPPO ของวันมีผลหรือไม่ (ดีเซล)"""
    if notices.empty:
        return ["[WF-NOTICE] ไม่มีข้อมูลประกาศกองทุน"]
    n = notices.copy()
    n["hour"] = n.msg_hour
    lead = (n.eff_day - n.msg_day).dt.days
    out = [f"[WF-NOTICE] ประกาศ {len(n)} ฉบับ ({n.msg_day.min():%d/%m/%y}–{n.msg_day.max():%d/%m/%y}) ; ออกเวลา {n.hour.min():.1f}–{n.hour.max():.1f} น. (มัธยฐาน {n.hour.median():.1f}) ; "
           f"ออกก่อน 20:30 {(n.hour <= 20.5).mean():.0%} ; มีผลล่วงหน้า {int(lead.min())}–{int(lead.max())} วัน (มัธยฐาน {lead.median():.0f})"]
    e = eppo[eppo.fuel == "B7"].set_index("day")["fund"]
    m = n.dropna(subset=["diesel_new"])
    both = [(d, v) for d, v in zip(m.eff_day, m.diesel_new) if d in e.index]
    if both:
        ok = sum(abs(e[d] - v) <= 0.01 for d, v in both)
        out.append(f"   ดีเซล: ค่าใหม่ในประกาศตรงกับ OIL FUND ของ EPPO วันมีผล {ok}/{len(both)} วัน ({ok / len(both):.0%})")
    return out


def class_label(x: float) -> str:
    return "ขึ้น" if x >= UP else "ลง" if x <= -UP else "นิ่ง"


def class_report(wf: pd.DataFrame) -> list:
    """ประเมินเป็น "ขยับหรือไม่ + ทิศ" (ขึ้น/ลง/นิ่ง ที่เกณฑ์ UP) : ความถูกต้อง 3 กลุ่ม + precision/recall ของ ขึ้น และ ลง"""
    act = wf.actual.map(class_label)
    out = [f"[WF-CLASS] n={len(wf)} ; จริง: ขึ้น {int((act == 'ขึ้น').sum())} ลง {int((act == 'ลง').sum())} นิ่ง {int((act == 'นิ่ง').sum())} ; ตอบ 'นิ่ง' ทุกวัน ถูก {float((act == 'นิ่ง').mean()):.0%}"]
    for m in MODELS[1:]:
        pr = wf[m].map(class_label)
        acc = float((pr == act).mean())
        cells = []
        for c in ("ขึ้น", "ลง"):
            tp, called, real = int(((pr == c) & (act == c)).sum()), int((pr == c).sum()), int((act == c).sum())
            cells.append(f"{c}: precision {tp}/{called}" + (f" ({tp / called:.0%})" if called else "") + f" recall {tp}/{real}" + (f" ({tp / real:.0%})" if real else ""))
        out.append(f"   {m:6s} ถูก 3 กลุ่ม {acc:.0%} ; " + " | ".join(cells))
    return out


def report(ds: pd.DataFrame, wf: pd.DataFrame) -> list:
    out = variance_report(ds)
    if wf.empty:
        return out + [f"[WF] ข้อมูลไม่พอ (ต้องมีอย่างน้อย {MIN_TRAIN} วัน/ชนิดก่อนเริ่มทดสอบ)"]
    cut = wf.day.sort_values().iloc[len(wf) // 2]
    out.append(f"[WF] ทดสอบ {len(wf)} วัน×ชนิด ({wf.day.min():%d/%m/%y}–{wf.day.max():%d/%m/%y}) ฝึกแบบขยายหน้าต่าง ridge={RIDGE} ; แบ่งครึ่งที่ {cut:%d/%m/%y}")
    for name, g in (("ทั้งหมด", wf), ("ครึ่งแรกของช่วงทดสอบ", wf[wf.day < cut]), ("ครึ่งหลัง", wf[wf.day >= cut])):
        if len(g) < 10:
            continue
        out.append(f"[WF] {name}: n={len(g)}")
        out += metrics(g)
    out += class_report(wf)
    second = wf[wf.day >= cut]
    if len(second) >= 10:
        out.append("[WF-CLASS] เฉพาะครึ่งหลังของช่วงทดสอบ")
        out += class_report(second)[1:]
    for fuel, g in wf.groupby("fuel"):
        out.append(f"[WF] {fuel}: n={len(g)}")
        out += metrics(g)
    return out


def main():
    tok = get_token()
    mops = mops_daily(sheets_get(tok, SHEET_ID, "Entry_MOPS!A1:F6000"))
    wti = wti_daily(sheets_get(tok, SHEET_ID, "Entry_WTI!A1:H6000"))
    eppo = eppo_components(sheets_get(tok, SHEET_ID, "Entry_Eppo!A1:S6000"))
    chk = (eppo.ws - eppo.exref - eppo.fund - eppo.other).abs()
    print(f"[WF] EPPO {len(eppo)} แถว ; ตรวจ WS = EX-REFIN + กองทุน + ภาษี : ต่างเกิน 0.01 ใน {(chk > 0.01).sum()} แถว (สูงสุด {chk.max():.3f})")
    ds = build_dataset(mops, wti, eppo)
    for line in identity_report(eppo) + notice_report(load_notices(sheets_get(tok, SHEET_ID, "Entry_FundNotice!A1:H3000")), eppo) + report(ds, walk_forward(ds)):
        print(line)


if __name__ == "__main__":
    main()
