"""
การทดลอง: เพิ่มข้อมูลโครงสร้างราคา EPPO (ราคาขายส่ง/กองทุน/ค่าการตลาด ของวันนี้) ให้โมเดลแนวโน้มราคาหน้าคลัง Jobber 24 ชม. รอบ 20:30

ที่มา : โมเดลตอบ "ไม่ชัด" ราวครึ่งหนึ่งของวัน และในวันเหล่านั้นราคาขยับจริงเกินครึ่ง ; ลดเกณฑ์ฟันธงไม่ช่วย (วันที่ได้เพิ่มถูกทิศ ~41%)
        ปี 2026 ราคาทางการ (WS/ปลีก) ขยับเป็นขั้นตามที่รัฐปรับ และกองทุนถูกใช้หักล้างราคาหน้าโรงกลั่น (exp_ws_search.py) — โมเดล Jobber ยังไม่เคยเห็นข้อมูลนี้

เทมเพลต (ตั้งก่อนรัน 2026-11-16)
  สมมติฐาน : สถานะของราคาทางการ ณ วันนี้ ช่วยบอกทิศราคา Jobber ใน 24 ชม. ข้างหน้า เพิ่มจากสัญญาณที่มีอยู่
  ฟีเจอร์ใหม่ 6 ตัว (ตรึง ไม่เลือกทีหลัง ; ใช้ไฟล์ EPPO ล่าสุดที่วันที่ <= วันตัดสินใจ ซึ่งออกช่วงเช้า จึงรู้ก่อน 20:30) :
     e_dws1   WS วันนี้ − WS ของไฟล์ก่อนหน้า (วันนี้ราคาทางการขยับไหม)
     e_dws3   WS วันนี้ − WS ของ 3 ไฟล์ก่อน
     e_mm     ค่าการตลาดวันนี้ − มัธยฐาน 60 ไฟล์ก่อนหน้า
     e_fund   ระดับกองทุนวันนี้ (บาท/ลิตร ; ลบ = อุดหนุน)
     e_spread ราคาต่ำสุด Jobber ตอนนี้ − WS×1.07 เทียบมัธยฐานของตัวเองใน 30 วันก่อนหน้า (Jobber แพง/ถูกกว่าปกติเทียบราคาทางการ)
     e_since  จำนวนวันตั้งแต่ WS ขยับ >= 10 สต. ครั้งล่าสุด (ตัดที่ 10)
  baseline : ชุดฟีเจอร์ที่ใช้งานจริงของแต่ละ key (B7_H24 = ชุดเดิม 8 ตัว ; G95_H24 = ชุด v2 25 ตัว) โลจิสติกส์เดียวกัน
  การทดสอบ : walk-forward เดียวกับของจริง (fit_model._wf_proba : ฝึกขั้นต่ำ 150 วัน ก้าว 30 วัน) วันทดสอบชุดเดียวกัน
  เกณฑ์ผ่าน (ต่อ key ครบทุกข้อ) :
     1) AUC เฉลี่ย (ขึ้น, ลง >= 10 สต.) ดีขึ้น >= 0.01 และช่วงเชื่อมั่น 90% ของส่วนต่างของแต่ละฝั่งไม่ติดลบทั้งช่วง (อย่างน้อยฝั่งหนึ่งไม่คร่อม 0 อีกฝั่งไม่แย่ลงเกิน 0.01)
     2) ที่เกณฑ์ฟันธงเดิม (0.50) : สัดส่วนวันที่ฟันธงไม่ลดลง , ถูกทิศไม่ลดลงเกิน 1 จุด , ผิดทิศไม่เพิ่มเกิน 1 จุด
     3) ผ่านการตรวจระดับแนวโน้มของประตูฝึกรายสัปดาห์ (retrain.tier_regressions) เทียบชุดที่ใช้งานจริง
     4) AUC เฉลี่ยไม่แย่ลงทั้งครึ่งแรกและครึ่งหลังของช่วงทดสอบ
  ข้อควรระวัง : ไฟล์ EPPO ไม่ออกวันหยุด (ใช้ไฟล์ล่าสุด) ; ปี 2024–2025 กลไกราคาทางการต่างจากปี 2026 ฟีเจอร์อาจมีความหมายไม่คงที่

ผล (รัน 2026-11-16 ; 645 วันทดสอบ) : ไม่ผ่านทั้งสอง key → ไม่นำเข้าโมเดลจริง
  B7_H24  : AUC ขึ้น 0.768 → 0.768 , ลง 0.751 → 0.774 (ส่วนต่าง 90% −0.000 ถึง +0.049 คร่อมศูนย์) ; ฟันธง 52% → 52% ถูกทิศ 64% → 66% ผิดทิศ 11% → 10%
            ไม่ผ่านเกณฑ์ 1 (ช่วงเชื่อมั่นคร่อมศูนย์ทั้งสองฝั่ง) และเกณฑ์ 3 (ขึ้นแรง ถูกทิศ 82% → 69%) ; ครึ่งหลังของช่วงทดสอบไม่ดีขึ้นเลย (0.726 → 0.726)
  G95_H24 : AUC ขึ้น 0.748 → 0.753 , ลง 0.794 → 0.775 (−0.032 ถึง −0.005 แย่ลงจริง) ; ฟันธง 42% → 47% แต่ถูกทิศ 62% → 58% ผิดทิศ 7% → 9%
            ไม่ผ่านทั้ง 4 เกณฑ์ (ขึ้นแรง ถูกทิศ 81% → 64% ผิดทิศ 0% → 8%)
  ข้อสังเกต (ยังไม่ใช่ข้อสรุป) : e_spread ของดีเซลมีน้ำหนักทิศสอดคล้อง (Jobber แพงกว่าปกติเทียบราคาทางการ → โน้มลง) และ AUC ลงของดีเซลดีขึ้นเฉพาะครึ่งแรก
     น้ำหนักของ G95 ไม่คงเส้นคงวา (e_spread ลบทั้งฝั่งขึ้นและลง) สอดคล้องกับที่กลไกราคาทางการปี 2024–2025 ต่างจากปี 2026

ใช้: python exp_eppo_features.py <data_dir ของ jobber> [data/eppo_comp_long.csv]
"""
import sys
import warnings

import numpy as np
import pandas as pd

import exp_model_search as X
import fit_model as F
import model as M
import retrain as R
import signals as S

warnings.filterwarnings("ignore")
E_FEATS = ["e_dws1", "e_dws3", "e_mm", "e_fund", "e_spread", "e_since"]
E_CLIPS = {"e_dws1": (-2, 2), "e_dws3": (-4, 4), "e_mm": (-3, 3), "e_fund": (-25, 12), "e_spread": (-3, 3), "e_since": (0, 10)}


def eppo_table(e: pd.DataFrame, fuel: str) -> pd.DataFrame:
    g = e[e.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
    out = pd.DataFrame(index=g.index)
    out["ws"] = g.ws
    out["e_dws1"] = g.ws.diff(1)
    out["e_dws3"] = g.ws.diff(3)
    out["e_mm"] = g.mm - g.mm.shift(1).rolling(60, min_periods=20).median()
    out["e_fund"] = g.fund
    step = (g.ws.diff().abs() >= 0.10).to_numpy()
    since, n = [], 10
    for s_ in step:
        n = 0 if s_ else min(n + 1, 10)
        since.append(n)
    out["e_since"] = since
    return out


def add_eppo(df: pd.DataFrame, et: pd.DataFrame, p0: np.ndarray) -> pd.DataFrame:
    """ต่อฟีเจอร์ EPPO ของ "ไฟล์ล่าสุดที่วันที่ <= วันตัดสินใจ" ; e_spread ใช้มัธยฐานของตัวเอง 30 วันก่อนหน้า (ไม่รวมวันนี้)"""
    idx = et.index.searchsorted(df.date.to_numpy(), side="right") - 1
    ok = idx >= 0
    rows = et.iloc[np.where(ok, idx, 0)].reset_index(drop=True)
    out = df.reset_index(drop=True).copy()
    for c in ("e_dws1", "e_dws3", "e_mm", "e_fund", "e_since"):
        out[c] = np.where(ok, rows[c].to_numpy(), np.nan)
    raw = pd.Series(np.where(ok, p0 - 1.07 * rows.ws.to_numpy(), np.nan))
    out["e_spread"] = (raw - raw.shift(1).rolling(30, min_periods=10).median()).to_numpy()
    return out


def e_matrix(df):
    return np.column_stack([np.clip(np.nan_to_num(df[c].to_numpy(float), nan=0.0), *E_CLIPS[c]) for c in E_FEATS])


def call_stats(y, pr, pf, thr=M.OUTLOOK_MIN_P):
    up, dn = (pr >= thr) & (pr > pf), (pf >= thr) & (pf > pr)
    c, s = up | dn, np.where(up, y, -y)
    return float(c.mean()), float((s[c] >= .1).mean()), float((s[c] <= -.1).mean())


def run(key, fuel, v2, book, tables, e):
    df = F.build_outlook_dataset(book, tables, fuel, 24, M.OUTLOOK_REF_HOUR, v2=v2)
    p0 = book.min_at(fuel, [d + pd.Timedelta(hours=M.OUTLOOK_REF_HOUR) for d in df.date]).to_numpy()
    df = add_eppo(df, eppo_table(e, fuel), p0)
    recs = df.to_dict("records")
    Xb = np.vstack([(M.vectorize_v2 if v2 else M.vectorize)(r) for r in recs])
    Xn = np.column_stack([Xb, e_matrix(df)])
    y = df.y.to_numpy()
    tg = {"r10": y >= M.OUTLOOK_SMALL, "f10": y <= -M.OUTLOOK_SMALL, "r40": y >= M.OUTLOOK_BIG, "f40": y <= -M.OUTLOOK_BIG}
    tg = {k: v.astype(float) for k, v in tg.items()}
    Pb, Pn = ({k: F._wf_proba(Xm, v) for k, v in tg.items()} for Xm in (Xb, Xn))
    ok = ~np.isnan(Pb["r10"])
    yo, h = y[ok], int(ok.sum()) // 2
    A = lambda P, k, sl=slice(None): F.auc(tg[k][ok][sl], P[k][ok][sl])
    mean_auc = lambda P, sl=slice(None): np.nanmean([A(P, "r10", sl), A(P, "f10", sl)])
    ci = {k: X.boot_diff(tg[k][ok], Pb[k][ok], Pn[k][ok]) for k in ("r10", "f10")}
    cb, cn = call_stats(yo, Pb["r10"][ok], Pb["f10"][ok]), call_stats(yo, Pn["r10"][ok], Pn["f10"][ok])
    tiers = lambda P: F.outlook_tier_stats(yo, np.array([M.classify_outlook(*(P[k][ok][i] for k in ("r10", "f10", "r40", "f40"))) for i in range(int(ok.sum()))], dtype=object))
    tb, tn = tiers(Pb), tiers(Pn)
    reg = R.tier_regressions({"tiers": tb}, {"tiers": tn})
    print(f"\n== {key} ({int(ok.sum())} วันทดสอบ ; ฟีเจอร์ EPPO มีค่า {df[E_FEATS].notna().all(axis=1).mean():.0%} ของวัน)")
    print(f"   AUC ขึ้น {A(Pb, 'r10'):.3f} → {A(Pn, 'r10'):.3f} (ส่วนต่าง 90% {ci['r10'][0]:+.3f} ถึง {ci['r10'][1]:+.3f}) ; AUC ลง {A(Pb, 'f10'):.3f} → {A(Pn, 'f10'):.3f} ({ci['f10'][0]:+.3f} ถึง {ci['f10'][1]:+.3f})")
    print(f"   AUC เฉลี่ย {mean_auc(Pb):.3f} → {mean_auc(Pn):.3f} ; ครึ่งแรก {mean_auc(Pb, slice(0, h)):.3f} → {mean_auc(Pn, slice(0, h)):.3f} ; ครึ่งหลัง {mean_auc(Pb, slice(h, None)):.3f} → {mean_auc(Pn, slice(h, None)):.3f}")
    print(f"   ฟันธง {cb[0]:.0%} → {cn[0]:.0%} ของวัน ; ถูกทิศ {cb[1]:.0%} → {cn[1]:.0%} ; ผิดทิศ {cb[2]:.0%} → {cn[2]:.0%}")
    for code in M.OUTLOOK_TIERS:
        a, b = tb[code], tn[code]
        if code != "UNCLEAR":
            print(f"      {M.OUTLOOK_TIERS[code]:<8} n {a['n']:>3} → {b['n']:>3} ; ถูก {a.get('right10', float('nan')):.0%} → {b.get('right10', float('nan')):.0%} ; ผิด {a.get('wrong10', float('nan')):.0%} → {b.get('wrong10', float('nan')):.0%}")
    w = M.fit_logistic(Xn, tg["r10"])["w"][-len(E_FEATS):], M.fit_logistic(Xn, tg["f10"])["w"][-len(E_FEATS):]
    print("   น้ำหนักฟีเจอร์ EPPO (ขึ้น / ลง) : " + " ; ".join(f"{n} {a:+.2f}/{b:+.2f}" for n, a, b in zip(E_FEATS, *w)))
    c1 = (mean_auc(Pn) - mean_auc(Pb) >= 0.01) and max(ci["r10"][0], ci["f10"][0]) > 0 and min(ci["r10"][1], ci["f10"][1]) > -0.01 \
        and min(A(Pn, "r10") - A(Pb, "r10"), A(Pn, "f10") - A(Pb, "f10")) > -0.01
    c2 = cn[0] >= cb[0] and cn[1] >= cb[1] - 0.01 and cn[2] <= cb[2] + 0.01
    c3 = not reg
    c4 = all(mean_auc(Pn, sl) >= mean_auc(Pb, sl) for sl in (slice(0, h), slice(h, None)))
    print(f"   เกณฑ์ 1 AUC {'ผ่าน' if c1 else 'ไม่ผ่าน'} ; 2 ฟันธง {'ผ่าน' if c2 else 'ไม่ผ่าน'} ; 3 ประตูระดับ {'ผ่าน' if c3 else 'ไม่ผ่าน: ' + ' | '.join(reg)} ; 4 สองครึ่ง {'ผ่าน' if c4 else 'ไม่ผ่าน'}"
          f" → {'ผ่าน' if all((c1, c2, c3, c4)) else 'ไม่ผ่าน'}")


def main():
    prices, tables = F.load(sys.argv[1])
    e = pd.read_csv(sys.argv[2] if len(sys.argv) > 2 else "data/eppo_comp_long.csv", parse_dates=["day"])
    book = S.PriceBook(prices)
    for key, fuel in (("B7_H24", "B7"), ("G95_H24", "G95")):
        run(key, fuel, key in M.OUTLOOK_V2_KEYS, book, tables, e)


if __name__ == "__main__":
    main()
