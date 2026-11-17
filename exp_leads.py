"""
การทดลอง: เบาะแสที่ค้างไว้ 3 เรื่อง (17/11/2026) — ไม่เปลี่ยนระบบจากผลนี้โดยไม่มีผลจริง

เทมเพลต (ตั้งก่อนรัน 2026-11-17)
  ก) ราคาปลีก ฝั่ง "ลง" : เพิ่มข้อมูลโครงสร้างราคาเฉพาะในโมเดลฝั่งลง (โมเดลฝั่งขึ้นคงเดิม)
     ที่มา : ระดับกองทุน G95 ทายฝั่งลงได้ AUC ~0.81 (สำรวจ 16/11) ; ฟีเจอร์โครงสร้างราคาของระบบ KCG ช่วย AUC ฝั่งลงของ G95 แต่ทำฝั่งขึ้นแย่ลง (exp_retail_kcg_features.py)
     ชุด : SIG (ที่ใช้อยู่) / DN+FUND = SIG + ระดับกองทุนวัน t / DN+FUND+GAP = SIG + ระดับกองทุน + (ราคาปลีก − ราคาหน้าโรงกลั่น) วัน t
     ทดสอบ : walk-forward ฝึกขั้นต่ำ 50 วัน ก้าว 10 วัน ; ราคาปลีก EPPO t+1 เทียบ t ; ขึ้น/ลง >= 10 สต.
     เกณฑ์ผ่าน (ต่อชนิด ครบทุกข้อ) : Brier 3 ชั้นต่ำกว่า SIG โดยช่วงเชื่อมั่น 95% ของส่วนต่างไม่คร่อมศูนย์ ; AUC ฝั่งลงเพิ่ม >= 0.05 ; Brier ของปี 2026 ไม่แย่ลง
  ข) ราคาส่ง Jobber 24 ชม. รอบ 20:30 : เพิ่ม "ขนาดที่ผู้ค้าประกาศ" (บาท/ลิตร) จากคอลัมน์ rates ของประกาศวันนี้ที่โพสต์ <= 20:30
     ฟีเจอร์ใหม่ 2 ตัว : rate_up = ขนาดการขึ้นสูงสุดที่ประกาศสำหรับชนิดนี้ (0 ถ้าไม่มี) , rate_dn = ขนาดการลงสูงสุด ; ตัดที่ 0–2 บาท
     ฐาน : ชุดฟีเจอร์ที่ใช้งานจริงของแต่ละชนิด (ดีเซล v1 , G95 v2) ; walk-forward เดียวกับของจริง (ฝึกขั้นต่ำ 150 วัน ก้าว 30 วัน) ; เกณฑ์ฟันธง 0.50
     เกณฑ์ผ่าน (ต่อชนิด) : AUC เฉลี่ยเพิ่ม >= 0.01 ; ฟันธงแล้วถูกทิศไม่ลดลงและผิดทิศไม่เพิ่มเกิน 1 จุด ; เป็นจริงทั้งช่วงรวมและปี 2026
     ข้อควรระวัง : ประกาศที่ระบุเรทมี 1–3% ของข้อความในปี 2024–2025 และ 17% ในปี 2026 → ฟีเจอร์แทบเป็นศูนย์ในช่วงฝึกส่วนใหญ่
  ค) ราคาปลีก ปตท. กาฬสินธุ์ (ข้อมูลจากระบบ KCG ถึง 14/05/2026) ต่างจากกรุงเทพฯ อย่างไร — เชิงพรรณนา ไม่มีเกณฑ์ผ่าน
     คำถาม : ส่วนต่างคงที่หรือไม่ และวันที่ปรับตรงกันหรือไม่ (ถ้าตรงกัน พยากรณ์จากราคา EPPO/กรุงเทพฯ ใช้กับหน้าปั๊มกาฬสินธุ์ได้เลย)

ผล (รัน 2026-11-17)
  ก) ราคาปลีก ฝั่งลง (502 วันทดสอบ ; ปี 2026 182 วัน)
     G95 DN+FUND ผ่านครบ 3 ข้อ : AUC ฝั่งลง 0.626 → 0.684 (+0.059 ; ครึ่งแรก 0.52 ครึ่งหลัง 0.82) ; Brier 0.346 → 0.339 ต่าง −0.015 (95% −0.029 ถึง −0.004)
         ปี 2026 : AUC 0.75 → 0.84 , Brier 0.361 → 0.342 ต่าง −0.043 (−0.081 ถึง −0.011) ; น้ำหนักกองทุนเป็นบวก (เก็บเข้ากองทุนสูง → โอกาสปลีกลงมากขึ้น) สมเหตุผล
         ผลเชิงปฏิบัติเล็ก : ที่เกณฑ์เตือน 0.20 จับวันที่ลงจริงได้ 19 จาก 52 วัน (เดิม 16) เตือน 78 ครั้ง (เดิม 66) ; ครึ่งแรกของช่วงทดสอบแทบไม่ช่วย
     G95 DN+FUND+GAP ไม่ผ่าน : AUC 0.82 แต่ Brier ต่าง −0.023 (−0.052 ถึง +0.005) คร่อมศูนย์
     ดีเซล ไม่ผ่านทั้งสองชุด (AUC ลดลง 0.75 → 0.72–0.73 , Brier เท่าเดิม)
  ข) ขนาดที่ผู้ค้าประกาศ ในโมเดลราคาส่ง 24 ชม. (645 วันทดสอบ ; ปี 2026 273 วัน ; วันที่มีเรทขึ้น 34% ทั้งช่วง 77% ในปี 2026 ; ไม่มีประกาศลงที่ระบุเรทเลย) : ไม่ผ่านทั้งสองชนิด
     ดีเซล : AUC ขึ้น/ลง 0.768/0.751 → 0.762/0.754 ; ถูกทิศ 64% → 62% ผิดทิศ 11% → 12% (ปี 2026 : 60% → 57%) = แย่ลง
     G95  : AUC 0.748/0.794 → 0.761/0.789 (เฉลี่ย +0.004 ไม่ถึง 0.01) ; ถูกทิศ 62% → 64% ผิดทิศ 7% → 6% ; ปี 2026 ถูก 66% → 70% ผิด 10% → 8% (AUC เฉลี่ย +0.0095)
            → ทิศทางดีแต่ไม่ถึงเกณฑ์ ; ประวัติที่มีเรทยังสั้น ควรทดสอบซ้ำเมื่อข้อมูลปี 2026–2027 มากขึ้น
  ค) ราคาปลีก ปตท. อ.เมืองกาฬสินธุ์ เทียบกรุงเทพฯ (173 วันที่มีราคา 02/01/2022–14/05/2026)
     ดีเซล / แก๊สโซฮอล์ 95 / 91 : ส่วนต่างเป็นค่าคงที่เป็นช่วง ๆ (+0.00 ถึง +0.52 ; ตลอดปี 2026 = +0.52 บาท/ลิตร) ; B20 เท่ากรุงเทพฯ ; ขนาดการปรับเท่ากัน 99% ของแถว
     18 อำเภอของกาฬสินธุ์ราคาเฉลี่ยต่างกันไม่เกิน 0.04 บาท → พยากรณ์จากราคา EPPO/กรุงเทพฯ ใช้กับหน้าปั๊มกาฬสินธุ์ได้โดยบวกส่วนต่างคงที่ ไม่ต้องมีโมเดลแยก

ใช้: python exp_leads.py <data_dir ของ jobber> <โฟลเดอร์ข้อมูล KCG> [data/eppo_daily_long.csv]
"""
import os
import sys
import warnings

import numpy as np
import pandas as pd

import exp_retail_kcg_features as K
import fit_model as F
import line_outlook as L
import model as M
import retail_forecast as RF
import signals as S

warnings.filterwarnings("ignore")


# ---------- ก) ----------
def part_a(tables, eppo, last):
    for fuel in ("B7", "G95"):
        g = eppo[eppo.fuel == fuel].drop_duplicates("day", keep="last").set_index("day").sort_index()
        rows = []
        for day, r in g.iterrows():
            nxt = day + pd.Timedelta(days=1)
            if nxt not in g.index or day > last or pd.isna(r.fund) or pd.isna(r.exref):
                continue
            rows.append({"day": day, "y": float(g.retail[nxt] - r.retail), "fund": float(r.fund), "gap": float(r.retail - r.exref), **RF.signal_features(tables, fuel, day)})
        df = pd.DataFrame(rows)
        y = df.y.to_numpy()
        yu, yd = (y >= RF.MOVE).astype(float), (y <= -RF.MOVE).astype(float)
        sig = np.vstack([RF.signal_vector(r) for r in df.to_dict("records")])
        pu, pd0 = K.wf_logit(sig, yu, yd)
        variants = {"SIG": pd0, "DN+FUND": K.wf_logit(np.column_stack([sig, df.fund]), yu, yd)[1],
                    "DN+FUND+GAP": K.wf_logit(np.column_stack([sig, df.fund, df.gap]), yu, yd)[1]}
        ok = ~np.isnan(pu)
        y26 = (df.day >= pd.Timestamp("2026-01-01")).to_numpy()
        print(f"\n== ก) {fuel} : {int(ok.sum())} วันทดสอบ ; ลงจริง {yd[ok].mean():.0%} ; ปี 2026 {int((ok & y26).sum())} วัน")
        for label, mask in (("ทั้งช่วง", ok), ("ปี 2026", ok & y26)):
            base = K.brier_days(yu, yd, pu, pd0)[mask]
            for name, pdn in variants.items():
                pd2 = np.minimum(pdn, 0.98 - np.minimum(pu, 0.98))
                b = K.brier_days(yu, yd, pu, pd2)[mask]
                lo, hi = K.ci(b - base) if name != "SIG" else (0, 0)
                h = mask.sum() // 2
                idx = np.where(mask)[0]
                print(f"   {name:<13}{label:<9} AUC ลง {F.auc(yd[mask], pdn[mask]):.2f} (ครึ่ง {F.auc(yd[idx[:h]], pdn[idx[:h]]):.2f}/{F.auc(yd[idx[h:]], pdn[idx[h:]]):.2f})"
                      f" Brier {b.mean():.3f}" + ("" if name == "SIG" else f" ต่าง {(b - base).mean():+.3f} ({lo:+.3f} ถึง {hi:+.3f})"))


# ---------- ข) ----------
def announced(tables, fuel, asof):
    grp = L.FUEL_GROUP[fuel]
    h = tables.headsup
    w = h[(h.ts >= asof.normalize()) & (h.ts <= asof)]
    up = dn = 0.0
    for d, r in zip(w.direction, w.rates if "rates" in w.columns else []):
        v = L.parse_rates(r).get(grp)
        if v is None:
            continue
        if d == "UP":
            up = max(up, abs(v))
        elif d == "DOWN":
            dn = max(dn, abs(v))
    return min(up, 2.0), min(dn, 2.0)


def part_b(book, tables):
    for fuel, v2 in (("B7", False), ("G95", True)):
        df = F.build_outlook_dataset(book, tables, fuel, 24, M.OUTLOOK_REF_HOUR, v2=v2)
        base = np.vstack([(M.vectorize_v2 if v2 else M.vectorize)(r) for r in df.to_dict("records")])
        extra = np.array([announced(tables, fuel, d + pd.Timedelta(hours=M.OUTLOOK_REF_HOUR)) for d in df.date])
        y = df.y.to_numpy()
        yu, yd = (y >= M.OUTLOOK_SMALL).astype(float), (y <= -M.OUTLOOK_SMALL).astype(float)
        y26 = (pd.to_datetime(df.date) >= pd.Timestamp("2026-01-01")).to_numpy()
        print(f"\n== ข) {fuel} ({'v2' if v2 else 'v1'}) : วันที่มีเรทขึ้น {(extra[:, 0] > 0).mean():.0%} (ปี 2026 {(extra[y26, 0] > 0).mean():.0%}) ; เรทลง {(extra[:, 1] > 0).mean():.0%}")
        for name, X in (("ฐาน", base), ("ฐาน+เรท", np.column_stack([base, extra]))):
            pr, pf = F._wf_proba(X, yu), F._wf_proba(X, yd)
            ok = ~np.isnan(pr)
            for label, mask in (("ทั้งช่วง", ok), ("ปี 2026", ok & y26)):
                up, dn = (pr >= 0.5) & (pr > pf) & mask, (pf >= 0.5) & (pf > pr) & mask
                c, s = up | dn, np.where(up, y, -y)
                print(f"   {name:<9}{label:<9} n {int(mask.sum())} AUC ขึ้น {F.auc(yu[mask], pr[mask]):.3f} ลง {F.auc(yd[mask], pf[mask]):.3f} ; ฟันธง {c.sum() / mask.sum():.0%}"
                      f" ถูก {(s[c] >= 0.1).mean():.0%} ผิดทิศ {(s[c] <= -0.1).mean():.0%}")


# ---------- ค) ----------
def part_c(kcg_dir):
    k = pd.read_csv(os.path.join(kcg_dir, "pttor_history_kalasin.csv"), parse_dates=["price_date"])
    b = pd.read_csv(os.path.join(kcg_dir, "pttor_history_bkk.csv"), parse_dates=["price_date"])
    print(f"\n== ค) ราคาปลีก ปตท. กาฬสินธุ์ เทียบกรุงเทพฯ ({b.price_date.min():%d/%m/%Y}–{b.price_date.max():%d/%m/%Y} ; {b.price_date.nunique()} วันที่มีราคา)")
    for prod in ("Diesel", "Gasohol 95", "Gasohol 91", "Diesel B20"):
        bb = b[b["product"] == prod].set_index("price_date").price.sort_index()
        bb = bb[~bb.index.duplicated()]
        for loc in ("Mueang Kalasin",):
            kk = k[(k["product"] == prod) & (k.location == loc)].set_index("price_date").price.sort_index()
            kk = kk[~kk.index.duplicated()]
            j = pd.concat([bb.rename("bkk"), kk.rename("kal")], axis=1).dropna()
            d = j.kal - j.bkk
            y26 = d[d.index >= "2026-01-01"]
            cb, ck = j.bkk.diff().abs() >= 0.05, j.kal.diff().abs() >= 0.05
            same = (j.bkk.diff() - j.kal.diff()).abs() <= 0.011
            print(f"   {prod:<11} {loc}: {len(j)} วัน ; ส่วนต่าง (กาฬสินธุ์ − กทม.) เฉลี่ย {d.mean():+.2f} ต่ำสุด {d.min():+.2f} สูงสุด {d.max():+.2f}"
                  f" ; ปี 2026 เฉลี่ย {y26.mean():+.2f} ({y26.min():+.2f} ถึง {y26.max():+.2f}) ; วันที่ กทม. ปรับ {int(cb.sum())} กาฬสินธุ์ปรับ {int(ck.sum())} ปรับเท่ากันทุกแถว {same.mean():.0%}")
    locs = k[k["product"] == "Diesel"].groupby("location").price.mean()
    print(f"   ดีเซล เฉลี่ยรายอำเภอ : ต่ำสุด {locs.min():.2f} ({locs.idxmin()}) สูงสุด {locs.max():.2f} ({locs.idxmax()}) ; {len(locs)} อำเภอ")


def main():
    prices, tables = F.load(sys.argv[1])
    eppo = pd.read_csv(sys.argv[3] if len(sys.argv) > 3 else RF.LONG_HISTORY_FILE, parse_dates=["day"])
    last = tables.headsup.ts.max().normalize() - pd.Timedelta(days=1)
    part_a(tables, eppo, last)
    part_b(S.PriceBook(prices), tables)
    part_c(sys.argv[2])


if __name__ == "__main__":
    main()
