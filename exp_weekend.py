"""
การทดลอง: แนวโน้มราคาส่ง (Jobber) 24 ชม. ทายผิดทิศบ่อยขึ้นในวันที่ MOPS ไม่อัปเดต (เสาร์–อาทิตย์/วันหยุดสิงคโปร์) หรือไม่ ถ้าใช่ แก้อย่างไร

ที่มา (17/11/2026) : ผลจริงใน OutlookLog ของ G95 24 ชม. ทายผิดทิศ 4/14 ครั้ง (backtest ~6–12%) ; 2 ครั้งเป็นคืนเสาร์ 31/10 และอาทิตย์ 1/11
   ที่โมเดลยังใช้ MOPS ปิดวันศุกร์ (−7.46) ทั้งที่กองทุนลดการอุดหนุน (+0.48 มีผล 1/11) และผู้ค้าประกาศขึ้น 10 เจ้า
เทมเพลต (ตั้งก่อนรัน 2026-11-17)
  สมมติฐาน : เมื่อค่า MOPS ปิดล่าสุด "ค้าง" (ไม่มีค่าใหม่ของวันนั้น) โมเดลใช้ close_last/close_3 เหมือนเป็นข้อมูลสด จึงฟันธงตาม MOPS เก่าและผิดทิศบ่อยขึ้น
  นิยาม "ค้าง" : อายุของ MOPS ปิดล่าสุด ณ เวลาตัดสินใจ (close_age_h) > 26 ชม. ที่ 20:30 / > 31 ชม. ที่ 15:30 (วันปกติ ≈ 1–2 / ≈ 20 ชม.)
     รายงานแยก เสาร์ / อาทิตย์ / วันธรรมดาที่ค้าง (วันหยุดสิงคโปร์) ด้วย
  ขอบเขต : โมเดลแนวโน้ม 24 ชม. ที่ใช้จริงทั้ง 4 ตัว : B7_H24 (v1), G95_H24 (v2), B7_P_H24 (v1), G95_P_H24 (v1)
     walk-forward เดียวกับของจริง (fit_model : ฝึกขั้นต่ำ 150 วัน ก้าว 30 วัน) ; ระดับคำทายด้วย model.classify_outlook (4 โมเดล : ขึ้น/ลง 10 และ 40 สต.)
  ตอนที่ 1 วินิจฉัย : อัตราถูกทิศ/ผิดทิศเมื่อฟันธง ของวันค้าง เทียบวันสด ; bootstrap รายวัน 90%
     "มีปัญหาจริง" = ผิดทิศวันค้าง − วันสด >= 5 จุด และช่วงเชื่อมั่น 90% ไม่คร่อม 0
  ตอนที่ 2 ทางแก้ (ทดสอบเฉพาะ key ที่ "มีปัญหาจริง") :
     A DEMOTE : วันค้าง → "ยังไม่ชัดเจน" ทุกครั้ง (กฎ ไม่ฝึกใหม่)
        ผ่าน = วันค้างที่ฟันธง ไม่ผ่านเกณฑ์ที่ใช้กับ "ช่วงที่ควรซื้อ" (ถูก >= 60% และผิด <= 12% ; model.PROFILE_MIN_RIGHT/MAX_WRONG)
     B STALE : เพิ่มฟีเจอร์ stale (0/1) และ close_last × stale ให้โมเดลเรียนเองว่าควรเชื่อ MOPS ค้างแค่ไหน
        ผ่าน (ตามแบบ exp_fund.py) = AUC เฉลี่ย (ขึ้น,ลง) เพิ่ม >= 0.01 ; ทั้งช่วง ฟันธงแล้วถูกไม่ลด และผิดไม่เพิ่มเกิน 1 จุด ;
              AUC ไม่แย่ลงทั้งครึ่งแรกและครึ่งหลัง ; ผิดทิศวันค้างลดลง
     ถ้าผ่านทั้งคู่ เลือก B (ยังทายได้ในวันค้างที่สัญญาณชัด) ; ไม่ผ่านทั้งคู่ = คงโมเดลเดิม
  ข้อควรระวัง : วันค้างมีราว 2/7 ของวัน (≈180 วันทดสอบต่อ key) ฟันธงจริงอาจเหลือไม่กี่สิบครั้ง ; ทดสอบ 4 key x 2 ทางแก้ = เลือกจากหลายชุด
     ข้อมูลฝึกถึง 21/10/2026 จึงไม่รวม 31/10 และ 1/11 ที่เป็นที่มา (ใช้เป็นหลักฐานยืนยันไม่ได้)

หมายเหตุการเปลี่ยนเกณฑ์หลังรัน : เกณฑ์ "ค้าง" ที่ 20:30 ตั้งไว้ 26 ชม. ผิด — วันเสาร์ 20:30 ห่างจาก MOPS ปิดวันศุกร์เพียง ~25 ชม. (มัธยฐาน 25.1)
   วันเสาร์ส่วนใหญ่จึงถูกนับเป็นวันสด ; แก้เป็น 12 ชม. (วันสดมัธยฐาน 1.1 ชม.) ; ผลด้วยเกณฑ์เดิมรายงานไว้ด้วย (--stale-h-eve 26) ข้อสรุปไม่เปลี่ยน

ผล (รัน 2026-11-17 ; oil-bot/training_data ถึง 21/10/2026 ; 645 วันทดสอบต่อ key)
  ตอนที่ 1 : ผิดทิศเมื่อฟันธง วันสด → วันค้าง (ส่วนต่าง , 90%)
     B7_H24    8% → 20%  (+12.5% , +5.3 ถึง +20.4)  มีปัญหาจริง  [เกณฑ์เดิม 26 ชม. : 8% → 25% , +17.0% (+6.7 ถึง +27.9)]
               วันค้างที่ฟันธง ถูกเพียง 37% (เสาร์ 12% , อาทิตย์ 42%) เทียบวันสด 74%
     G95_H24   8% → 7%   (−0.5% , −6.5 ถึง +6.5)   ไม่มีปัญหา — โมเดล v2 มีตัวแปรวันเสาร์/อาทิตย์อยู่แล้ว และฟันธงวันเสาร์เพียง 4/91 วัน
     B7_P_H24  15% → 8% ; G95_P_H24 15% → 12%      ไม่มีปัญหา (รอบ 15:30 วันธรรมดาก็ใช้ MOPS ของเมื่อวานอยู่แล้ว)
  ตอนที่ 2 (B7_H24 เท่านั้น) :
     A DEMOTE : ผ่าน — ทั้งช่วง ฟันธง 52% → 38% ถูก 64% → 74% ผิดทิศ 11% → 8%
     B STALE  : ผ่าน — AUC 0.760 → 0.793 (ครึ่ง 0.802/0.726 → 0.810/0.777) ; ทั้งช่วง ฟันธง 50% ถูก 70% ผิดทิศ 8% ; วันค้าง ผิดทิศ 20% → 11%
               [เกณฑ์เดิม : AUC 0.760 → 0.771 ผ่านเช่นกัน]
     → ตามเกณฑ์ที่ตั้งไว้ เลือก B สำหรับ B7_H24
  การตัดสินใจ (2026-11-17) : เจ้าของระบบอนุมัติ B สำหรับ B7_H24 (model.OUTLOOK_STALE_KEYS , ชุดฟีเจอร์ v1s) ติดป้าย "ทดลอง"
     โมเดลเดิมทายคู่ขนาน (OutlookLog old_*) ; ทบทวนเมื่อมีผลจริงของวันค้าง ≈ 16 วัน
     ฝึกเฉพาะ B7_H24 ด้วย oil-bot/training_data (ถึง 21/10/2026 ; ข้อมูลชุดเดียวกับโมเดลเดิม — v1 ที่ฝึกซ้ำได้ AUC 0.769/0.752 เท่าของเดิม)
     ผล walk-forward ใหม่ : AUC ขึ้น/ลง 0.768/0.751 → 0.788/0.798 ; ทุกระดับคำทายผิดทิศลดลง (ลง 13% → 9% , ลงแรง 8% → 4%) ; ประตูตรวจของ retrain ผ่าน
  การตีความ : ความผิดพลาดของ G95 ในผลจริง 31/10–1/11 ไม่ได้มาจากปัญหาวันหยุดแบบเป็นระบบ (backtest ของ G95 ไม่พบ)
     น่าจะมาจากช่วงตลาดผันผวนผิดปกติ (MOPS G95 −28% ใน 2 สัปดาห์) และกองทุนที่ลดการอุดหนุนในวันหยุด ;
     ปัญหาวันหยุดจริงอยู่ที่ดีเซล (B7_H24 ใช้ฟีเจอร์ชุด v1 ไม่มีตัวแปรวันในสัปดาห์)

ใช้: python exp_weekend.py <data_dir ของ jobber> [--boot 1000] [--stale-h-eve 12]
"""
import argparse
import warnings

import numpy as np
import pandas as pd

import fit_model as F
import model as M
import signals as S

warnings.filterwarnings("ignore")
KEYS = (("B7_H24", "B7", M.OUTLOOK_REF_HOUR, False), ("G95_H24", "G95", M.OUTLOOK_REF_HOUR, True),
        ("B7_P_H24", "B7", M.OUTLOOK_REF_PM, False), ("G95_P_H24", "G95", M.OUTLOOK_REF_PM, False))
STALE_H = {M.OUTLOOK_REF_HOUR: 12.0, M.OUTLOOK_REF_PM: 31.0}     # 20:30 เดิมตั้ง 26 (ผิด ดูหมายเหตุใน docstring) ; --stale-h-eve ใช้ทดสอบค่าเดิม
GAP_PROBLEM = 0.05


def tiers_wf(X, y):
    """walk-forward 4 โมเดล → (ระดับคำทาย, p ขึ้น10, p ลง10) ; NaN/'' ช่วงฝึกขั้นต่ำ"""
    ps = [F._wf_proba(X, t.astype(float)) for t in (y >= M.OUTLOOK_SMALL, y <= -M.OUTLOOK_SMALL, y >= M.OUTLOOK_BIG, y <= -M.OUTLOOK_BIG)]
    tiers = np.array(["" if np.isnan(a) else M.classify_outlook(a, b, c, d) for a, b, c, d in zip(*ps)], dtype=object)
    return tiers, ps[0], ps[1]


def call_stats(y, tiers, mask):
    c = mask & (tiers != "UNCLEAR") & (tiers != "")
    s = np.where(np.char.startswith(tiers.astype(str), "UP"), y, -y)[c]
    n = int(c.sum())
    return {"days": int((mask & (tiers != "")).sum()), "n": n, "share": n / max(int((mask & (tiers != "")).sum()), 1),
            "right": float((s >= M.OUTLOOK_SMALL).mean()) if n else np.nan, "wrong": float((s <= -M.OUTLOOK_SMALL).mean()) if n else np.nan}


def boot_gap(y, tiers, stale, n_boot, seed=7):
    """ช่วงเชื่อมั่น 90% ของ (ผิดทิศวันค้าง − วันสด) : สุ่มวันแบบคืนที่ ; คิดเฉพาะวันที่ฟันธง"""
    ok = tiers != ""
    idx = np.flatnonzero(ok)
    rng = np.random.default_rng(seed)
    out = []
    for _ in range(n_boot):
        b = rng.choice(idx, len(idx))
        a, f = call_stats(y[b], tiers[b], stale[b]), call_stats(y[b], tiers[b], ~stale[b])
        if a["n"] and f["n"]:
            out.append(a["wrong"] - f["wrong"])
    return np.percentile(out, [5, 95]) if out else (np.nan, np.nan)


def auc_pack(y, pr, pf):
    ok = ~np.isnan(pr)
    yu, yd = (y >= M.OUTLOOK_SMALL).astype(float), (y <= -M.OUTLOOK_SMALL).astype(float)
    h = int(ok.sum()) // 2
    yu, yd, pr, pf = yu[ok], yd[ok], pr[ok], pf[ok]
    a = lambda sl: float(np.mean([F.auc(yu[sl], pr[sl]), F.auc(yd[sl], pf[sl])]))
    return a(slice(None)), a(slice(0, h)), a(slice(h, None))


def fmt(st):
    if not st["n"]:
        return f"{st['days']:>4} วัน ฟันธง   0"
    return f"{st['days']:>4} วัน ฟันธง {st['n']:>3} ({st['share']:.0%}) ถูก {st['right']:.0%} ผิดทิศ {st['wrong']:.0%}"


def run_key(book, tables, key, fuel, ref_hour, v2, n_boot):
    df = F.build_outlook_dataset(book, tables, fuel, 24, ref_hour, v2=v2)
    vec = M.vectorize_v2 if v2 else M.vectorize
    recs = df.to_dict("records")
    X = np.vstack([vec(r) for r in recs])
    y = df.y.to_numpy()
    age = pd.to_numeric(df.close_age_h, errors="coerce").to_numpy()
    stale = np.nan_to_num(age, nan=999.0) > STALE_H[ref_hour]
    dow = pd.to_datetime(df.date).dt.dayofweek.to_numpy()

    tiers, pr, pf = tiers_wf(X, y)
    print(f"\n== {key} ({'v2' if v2 else 'v1'}, อ้างอิง {ref_hour:g} น.) : {len(y)} วัน ; วันค้าง {int(stale.sum())} วัน")
    print(f"   อายุ MOPS ปิด (มัธยฐาน ชม.) : วันสด {np.nanmedian(age[~stale]):.1f} ; เสาร์ {np.nanmedian(age[dow == 5]):.1f} ; อาทิตย์ {np.nanmedian(age[dow == 6]):.1f}")
    groups = (("วันสด", ~stale), ("วันค้าง (รวม)", stale), ("  เสาร์", stale & (dow == 5)), ("  อาทิตย์", stale & (dow == 6)),
              ("  วันธรรมดาที่ค้าง", stale & (dow < 5)), ("เสาร์-อาทิตย์ที่ไม่ค้าง", ~stale & (dow >= 5)))
    for name, m in groups:
        print(f"   {name:<22}{fmt(call_stats(y, tiers, m))}")
    fr, st = call_stats(y, tiers, ~stale), call_stats(y, tiers, stale)
    lo, hi = boot_gap(y, tiers, stale, n_boot)
    gap = st["wrong"] - fr["wrong"] if st["n"] and fr["n"] else np.nan
    problem = bool(gap >= GAP_PROBLEM and lo > 0)
    print(f"   ผิดทิศ วันค้าง − วันสด : {gap:+.1%} (90% {lo:+.1%} ถึง {hi:+.1%}) → {'มีปัญหาจริง' if problem else 'ไม่ถึงเกณฑ์'}")
    # ระดับละเอียดในวันค้าง (ดูว่ากระจุกที่ "ลงแรง/ขึ้นแรง" หรือไม่)
    for code in ("UP_STRONG", "UP", "DOWN", "DOWN_STRONG"):
        m = stale & (tiers == code)
        if m.sum():
            s = y[m] * (1 if code.startswith("UP") else -1)
            print(f"      วันค้าง {M.OUTLOOK_TIERS[code]:<6} {int(m.sum()):>3} ครั้ง ถูก {(s >= 0.1).mean():.0%} ผิด {(s <= -0.1).mean():.0%}")

    res = {"key": key, "problem": problem, "gap": gap, "lo": lo, "hi": hi, "fresh": fr, "stale": st}
    if not problem:
        return res

    # A : DEMOTE
    a_ok = not (st["n"] and st["right"] >= M.PROFILE_MIN_RIGHT and st["wrong"] <= M.PROFILE_MAX_WRONG)
    t_a = tiers.copy()
    t_a[stale & (t_a != "")] = "UNCLEAR"
    all0, all_a = call_stats(y, tiers, np.ones(len(y), bool)), call_stats(y, t_a, np.ones(len(y), bool))
    print(f"   A DEMOTE : ทั้งช่วง {fmt(all0)}  →  {fmt(all_a)}  ; {'ผ่าน' if a_ok else 'ไม่ผ่าน'} (วันค้างที่ฟันธง{'ไม่' if a_ok else ''}ผ่านเกณฑ์ ถูก>=60% ผิด<=12%)")

    # B : STALE features
    clip = M.CLIPS["close_last"]
    cl = np.clip(np.nan_to_num(pd.to_numeric(df.close_last, errors="coerce").to_numpy()), *clip)
    Xb = np.column_stack([X, stale.astype(float), cl * stale])
    t_b, pr_b, pf_b = tiers_wf(Xb, y)
    a0, a1 = auc_pack(y, pr, pf), auc_pack(y, pr_b, pf_b)
    all_b, st_b = call_stats(y, t_b, np.ones(len(y), bool)), call_stats(y, t_b, stale)
    b_ok = (a1[0] - a0[0] >= 0.01 and all_b["right"] >= all0["right"] and all_b["wrong"] <= all0["wrong"] + 0.01
            and a1[1] >= a0[1] and a1[2] >= a0[2] and (st_b["wrong"] if st_b["n"] else 0.0) < st["wrong"])
    print(f"   B STALE  : AUC {a0[0]:.3f} → {a1[0]:.3f} (ครึ่ง {a0[1]:.3f}/{a0[2]:.3f} → {a1[1]:.3f}/{a1[2]:.3f})")
    print(f"              ทั้งช่วง {fmt(all_b)} ; วันค้าง {fmt(st_b)} ; {'ผ่าน' if b_ok else 'ไม่ผ่าน'}")
    res.update({"A": a_ok, "B": b_ok})
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data_dir")
    ap.add_argument("--boot", type=int, default=1000)
    ap.add_argument("--stale-h-eve", type=float, default=STALE_H[M.OUTLOOK_REF_HOUR])
    a = ap.parse_args()
    STALE_H[M.OUTLOOK_REF_HOUR] = a.stale_h_eve
    prices, tables = F.load(a.data_dir)
    book = S.PriceBook(prices)
    out = [run_key(book, tables, *k, a.boot) for k in KEYS]
    print("\n== สรุป")
    for r in out:
        verdict = "ไม่ถึงเกณฑ์ปัญหา → คงเดิม" if not r["problem"] else (
            "ใช้ B (ฟีเจอร์ค้าง)" if r.get("B") else "ใช้ A (วันค้าง = ยังไม่ชัดเจน)" if r.get("A") else "มีปัญหาแต่ทางแก้ไม่ผ่าน → คงเดิม")
        print(f"   {r['key']:<10} ผิดทิศ สด {r['fresh']['wrong']:.0%} ค้าง {r['stale']['wrong']:.0%} ({r['gap']:+.1%}) → {verdict}")


if __name__ == "__main__":
    main()
