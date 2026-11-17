"""
การทดลอง: เพิ่ม "ระดับกองทุนน้ำมัน" เป็นตัวแปรหนึ่งในโมเดลพยากรณ์ทุกตัวที่ใช้งานจริง แล้วแม่นขึ้นหรือไม่

ที่มา : โมเดลปัจจุบันใช้เฉพาะ "การเปลี่ยนกองทุนที่ประกาศแล้วและมีผลพรุ่งนี้" (fund_next) ยังไม่ใช้ "ระดับ" ของกองทุน
        ระดับกองทุนช่วยโมเดลราคาปลีกฝั่งลงของ G95 (exp_leads.py ; ใช้งานแล้ว) — เจ้าของระบบขอให้ลองกับโมเดลอื่นด้วย (17/11/2026)
เทมเพลต (ตั้งก่อนรัน 2026-11-17)
  ตัวแปรใหม่ (จากโครงสร้างราคา EPPO ของวัน t ซึ่งออกช่วงเช้า จึงรู้ก่อน 20:30 ; data/eppo_daily_long.csv) :
     FUND     = ระดับกองทุนของวัน t (บาท/ลิตร ; บวก = เก็บเข้ากองทุน ลบ = อุดหนุน)
     FUND_T1  = FUND + fund_next (ระดับที่จะมีผลพรุ่งนี้ตามประกาศ) — ทดสอบเป็นชุดแยก ไม่ใส่พร้อมกัน
  1) ราคาส่ง Jobber : (ก) แนวโน้ม 24 ชม. รอบ 20:30 (ดีเซล v1 , G95 v2 ตามที่ใช้จริง)  (ข) โหลดคืนนี้หรือรอบ่าย : ราคา 16:30 พรุ่งนี้ เทียบ 22:00 คืนนี้ (v2 ตาม exp_lead_time.py)
     walk-forward เดียวกับของจริง (ฝึกขั้นต่ำ 150 วัน ก้าว 30 วัน) ; เกณฑ์ฟันธง 0.50
     เกณฑ์ผ่าน (ต่อชนิด ต่อเป้าหมาย ครบทุกข้อ) : AUC เฉลี่ย (ขึ้น,ลง) เพิ่ม >= 0.01 ; ฟันธงแล้วถูกทิศไม่ลดลงและผิดทิศไม่เพิ่มเกิน 1 จุด ; AUC เฉลี่ยไม่แย่ลงทั้งครึ่งแรกและครึ่งหลัง
  2) ราคาปลีก T+1 : ใส่กองทุนในโมเดลฝั่งขึ้น (UP+FUND) และทั้งสองฝั่ง (BOTH+FUND) เทียบกับที่ใช้จริง (ดีเซล = SIG ; G95 = SIG ที่ฝั่งลงมีกองทุนแล้ว)
     walk-forward ฝึกขั้นต่ำ 50 วัน ก้าว 10 วัน
     เกณฑ์ผ่าน (ต่อชนิด) : Brier 3 ชั้นต่ำกว่าที่ใช้จริง โดยช่วงเชื่อมั่น 95% ของส่วนต่างไม่คร่อมศูนย์ ; Brier ปี 2026 ไม่แย่ลง
  ข้อควรระวัง : 2 ตัวแปร x หลายโมเดล = เลือกจากหลายชุด ; ปี 2024–2025 กองทุนถูกใช้ตรึงราคาปลีก กลไกต่างจากปี 2026 ; exp_eppo_features.py เคยใส่กองทุนพร้อมตัวแปร EPPO อีก 5 ตัวแล้วไม่ผ่าน

ผล (รัน 2026-11-17) : ไม่มีชุดใดผ่านเกณฑ์ → ไม่เพิ่มกองทุนในโมเดลอื่น (คงไว้เฉพาะฝั่งลงของราคาปลีก G95 ที่ผ่านใน exp_leads.py)
  1) ราคาส่ง Jobber (642 วันทดสอบ) — AUC ลดลงเล็กน้อยทุกชุด ; FUND กับ FUND_T1 ให้ผลแทบเท่ากัน
     ดีเซล 24 ชม. : AUC 0.762 → 0.759 ; ฟันธง/ถูก/ผิดทิศเท่าเดิม (51% / 65% / 10%)
     ดีเซล โหลดคืนนี้หรือรอบ่าย : AUC 0.818 → 0.809 (ครึ่งหลัง 0.780 → 0.763) ; ถูก 68% → 69% ผิด 10% → 9%
     G95 24 ชม. : AUC 0.767 → 0.762 ; ฟันธง 42% → 45% ถูก 61% → 60% ผิด 9% → 8%
     G95 โหลดคืนนี้หรือรอบ่าย : AUC 0.777 → 0.773 ; ฟันธง 42% → 48% ถูก 63% → 60%
  2) ราคาปลีก (502 วันทดสอบ) — Brier ดีขึ้นเล็กน้อยแต่ช่วงเชื่อมั่นคร่อมศูนย์ทุกชุด ; AUC ฝั่งขึ้นไม่ดีขึ้น
     ดีเซล : UP+FUND / BOTH+FUND Brier 0.142 → 0.141 (−0.007 ถึง +0.005) ; ปี 2026 0.326 → 0.319 (−0.022 ถึง +0.010)
     G95  : UP+FUND Brier 0.345 → 0.341 (−0.013 ถึง +0.003) ; ปี 2026 0.360 → 0.343 (−0.041 ถึง +0.002) ใกล้เกณฑ์แต่ไม่ผ่าน ; AUC ขึ้น 0.70 → 0.68
  การตีความ : ระดับกองทุนบอกว่า "รัฐมีช่องให้ลดราคาปลีกทางการหรือไม่" จึงช่วยเฉพาะฝั่งลงของราคาปลีก G95 ;
     ราคาส่งของ Jobber ขยับตาม MOPS/ประกาศรายวัน ซึ่งโมเดลมีอยู่แล้ว ระดับกองทุนไม่เพิ่มข้อมูล

ใช้: python exp_fund.py <data_dir ของ jobber> [data/eppo_daily_long.csv]
"""
import sys
import warnings

import numpy as np
import pandas as pd

import exp_lead_time as LT
import exp_retail_kcg_features as K
import fit_model as F
import model as M
import retail_forecast as RF
import signals as S

warnings.filterwarnings("ignore")


def fund_series(eppo, fuel):
    g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index().fund
    return g.reindex(pd.date_range(g.index.min(), g.index.max())).ffill(limit=4)      # วันหยุดใช้ไฟล์ล่าสุด (ไม่เกิน 4 วัน)


def jobber_eval(X, y):
    yu, yd = (y >= M.OUTLOOK_SMALL).astype(float), (y <= -M.OUTLOOK_SMALL).astype(float)
    pr, pf = F._wf_proba(X, yu), F._wf_proba(X, yd)
    ok = ~np.isnan(pr)
    yy, yu, yd, pr, pf = y[ok], yu[ok], yd[ok], pr[ok], pf[ok]
    up, dn = (pr >= 0.5) & (pr > pf), (pf >= 0.5) & (pf > pr)
    c, s = up | dn, np.where(up, yy, -yy)
    h = len(yy) // 2
    a = lambda sl: float(np.mean([F.auc(yu[sl], pr[sl]), F.auc(yd[sl], pf[sl])]))
    return {"n": len(yy), "auc": a(slice(None)), "auc_up": F.auc(yu, pr), "auc_dn": F.auc(yd, pf), "h1": a(slice(0, h)), "h2": a(slice(h, None)),
            "share": float(c.mean()), "right": float((s[c] >= 0.1).mean()), "wrong": float((s[c] <= -0.1).mean()), "gain": float(yy[up].sum() / len(yy))}


def part1(book, tables, eppo):
    for fuel, v2 in (("B7", False), ("G95", True)):
        fs = fund_series(eppo, fuel)
        for title, df, vec in ((f"24 ชม. รอบ 20:30 ({'v2' if v2 else 'v1'})", F.build_outlook_dataset(book, tables, fuel, 24, M.OUTLOOK_REF_HOUR, v2=v2), M.vectorize_v2 if v2 else M.vectorize),
                               ("โหลดคืนนี้หรือรอบ่าย (v2)", LT.dataset(book, tables, fuel, 20.5, True), M.vectorize_v2)):
            fund = fs.reindex(pd.to_datetime(df.date)).to_numpy()
            keep = ~np.isnan(fund)
            recs = df[keep].to_dict("records")
            base = np.vstack([vec(r) for r in recs])
            y = df.y.to_numpy()[keep]
            fnext = np.array([0.0 if r.get("fund_next") is None or pd.isna(r.get("fund_next")) else float(r["fund_next"]) for r in recs])
            f0 = np.clip(fund[keep], -25, 25)
            print(f"\n== 1) {fuel} : {title} ; {keep.sum()} วัน (ตัดวันที่ไม่มีค่ากองทุน {int((~keep).sum())})")
            b = None
            for name, X in (("ฐาน", base), ("+FUND", np.column_stack([base, f0])), ("+FUND_T1", np.column_stack([base, np.clip(f0 + fnext, -25, 25)]))):
                e = jobber_eval(X, y)
                b = b or e
                ok = name != "ฐาน" and e["auc"] - b["auc"] >= 0.01 and e["right"] >= b["right"] - 0.01 and e["wrong"] <= b["wrong"] + 0.01 and e["h1"] >= b["h1"] and e["h2"] >= b["h2"]
                print(f"   {name:<9} n {e['n']} AUC {e['auc']:.3f} (ขึ้น {e['auc_up']:.3f} ลง {e['auc_dn']:.3f} ; ครึ่ง {e['h1']:.3f}/{e['h2']:.3f}) ฟันธง {e['share']:.0%} ถูก {e['right']:.0%} ผิดทิศ {e['wrong']:.0%}"
                      f" กำไรเมื่อทายขึ้น {e['gain']:+.3f}" + ("" if name == "ฐาน" else f"  ΔAUC {e['auc'] - b['auc']:+.3f} {'ผ่าน' if ok else '-'}"))


def part2(tables, eppo, last):
    for fuel in ("B7", "G95"):
        g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
        rows = []
        for day, r in g.iterrows():
            nxt = day + pd.Timedelta(days=1)
            if nxt not in g.index or day > last or pd.isna(r.fund):
                continue
            rows.append({"day": day, "y": float(g.retail[nxt] - r.retail), "fund": float(r.fund), **RF.signal_features(tables, fuel, day)})
        df = pd.DataFrame(rows)
        y = df.y.to_numpy()
        yu, yd = (y >= RF.MOVE).astype(float), (y <= -RF.MOVE).astype(float)
        sig = np.vstack([RF.signal_vector(r) for r in df.to_dict("records")])
        sf = np.column_stack([sig, df.fund])
        pu0, pd0 = K.wf_logit(sig, yu, yd)
        pu1, pd1 = K.wf_logit(sf, yu, yd)
        cur_dn = pd1 if fuel in RF.DOWN_FUND_FUELS else pd0
        variants = {"ที่ใช้จริง": (pu0, cur_dn), "UP+FUND": (pu1, cur_dn), "BOTH+FUND": (pu1, pd1)}
        ok = ~np.isnan(pu0)
        y26 = (df.day >= pd.Timestamp("2026-01-01")).to_numpy()
        print(f"\n== 2) ราคาปลีก {fuel} : {int(ok.sum())} วันทดสอบ ; ขึ้น {yu[ok].mean():.0%} ลง {yd[ok].mean():.0%} ; ปี 2026 {int((ok & y26).sum())} วัน")
        for label, mask in (("ทั้งช่วง", ok), ("ปี 2026", ok & y26)):
            base = K.brier_days(yu, yd, *variants["ที่ใช้จริง"])[mask]
            for name, (pu, pdn) in variants.items():
                b = K.brier_days(yu, yd, pu, pdn)[mask]
                lo, hi = K.ci(b - base) if name != "ที่ใช้จริง" else (0, 0)
                print(f"   {name:<11}{label:<9} AUC ขึ้น {F.auc(yu[mask], pu[mask]):.2f} ลง {F.auc(yd[mask], pdn[mask]):.2f} Brier {b.mean():.3f}"
                      + ("" if name == "ที่ใช้จริง" else f" ต่าง {(b - base).mean():+.3f} ({lo:+.3f} ถึง {hi:+.3f})"))


def main():
    prices, tables = F.load(sys.argv[1])
    eppo = pd.read_csv(sys.argv[2] if len(sys.argv) > 2 else RF.LONG_HISTORY_FILE, parse_dates=["day"])
    part1(S.PriceBook(prices), tables, eppo)
    part2(tables, eppo, tables.headsup.ts.max().normalize() - pd.Timedelta(days=1))


if __name__ == "__main__":
    main()
