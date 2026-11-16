"""
การทดลอง: จะมีรอบพยากรณ์ 2 รอบ (บ่าย ก่อนผู้ค้าประกาศปรับราคา + กลางคืน) ได้หรือไม่ และเวลาใดแม่นที่สุดในแต่ละช่วง

ที่มา : ผู้ค้าส่งประกาศ "มีปรับขึ้น/ลง คืนนี้" ช่วง 16:00–18:00 เป็นส่วนใหญ่ ก่อนรอบพยากรณ์ 20:30 — คนซื้อเห็นประกาศก่อนพยากรณ์
        exp_ref_hours.py ทดสอบแล้วว่ารอบกลางวันทาย "ราคาอีก 24 ชม." ไม่แม่น แต่ยังไม่เคยทดสอบเป้าหมายที่รอบบ่ายควรตอบจริง ๆ คือ "คืนนี้จะขึ้นไหม"
เทมเพลต (ตั้งก่อนรัน 2026-11-16)
  คำถาม ก (รอบบ่าย) : ณ เวลา h ของวัน d ทายว่าราคาต่ำสุดของตลาด ณ 06:00 ของวัน d+1 (หลังการปรับข้ามคืน ก่อนราคาทยอยลงระหว่างวัน)
                      จะสูง/ต่ำกว่าราคา ณ เวลา h >= 10 สต. หรือไม่ = "ซื้อวันนี้ก่อนขึ้น หรือไม่ต้องรีบ"
     h (ตรึง) = 11:00 12:00 13:00 14:00 15:00 15:30 16:00 17:00 18:00  (ประกาศของผู้ค้ามาช่วง 16:00–18:00 ; 17:00–18:00 ใส่ไว้เทียบว่าประกาศช่วยเท่าไร)
  คำถาม ข (รอบกลางคืน) : เป้าหมายเดิม ราคา ณ h + 24 ชม. เทียบ h ; h (ตรึง) = 19:00 19:30 20:00 20:30 21:00 21:30 22:00 22:30 23:00
  โมเดล : โลจิสติกส์เดียวกับของจริง ฝึกแยกต่อเวลา/เป้าหมาย ; ชุดฟีเจอร์ v1 และ v2 ; ใช้เฉพาะข้อความ/ราคาที่โพสต์ก่อนเวลา h (fit_model.build_outlook_dataset)
  การทดสอบ : walk-forward เดียวกับของจริง (ฝึกขั้นต่ำ 150 วัน ก้าว 30 วัน) ; เกณฑ์ฟันธง 0.50 เท่าเดิม ไม่จูน
  เกณฑ์ "ใช้เป็นรอบพยากรณ์ได้" (เหมือน exp_ref_hours.py ; ต่อชนิดน้ำมัน ต่อเวลา ครบทุกข้อ ; ใช้ชุดฟีเจอร์ที่ดีกว่าได้ แต่รายงานทั้งคู่) :
     1) AUC เฉลี่ย (ขึ้น, ลง) >= 0.70   2) ฟันธงแล้วถูกทิศ >= 60% ผิดทิศ <= 12%   3) ฟันธง >= 25% ของวัน   4) ถูกทิศ >= 55% ทั้งสองครึ่งของช่วงทดสอบ
  ฐานเปรียบเทียบของคำถาม ก : "ทายขึ้นทุกวัน" (ราคาเป็นฟันเลื่อย ขึ้นข้ามคืนเกือบทุกวัน) — โมเดลต้องถูกทิศสูงกว่าอัตราที่ราคาขึ้นจริง จึงถือว่าให้ข้อมูลเพิ่ม
  ข้อควรระวัง : 18 เวลา x 2 ชนิด x 2 ชุด = เลือกจากหลายตัว ; ไม่นับเวลาที่ผ่านแบบเฉียดหรือผ่านโดด ๆ ไม่ต่อเนื่องกับเวลาข้างเคียง
               ราคา 06:00 = ต่ำสุดของตลาดเท่าที่มีประกาศ ณ เวลานั้น (บางเจ้ายังไม่ประกาศราคาเช้า) ; ผลย้อนหลังต้องยืนยันด้วยผลจริงก่อนใช้ตัดสินใจ

ใช้: python exp_two_rounds.py <data_dir ของ jobber> [A|B]
"""
import sys
import warnings

import numpy as np

import exp_ref_hours as X
import fit_model as F
import model as M
import signals as S

warnings.filterwarnings("ignore")
PM_HOURS = (11.0, 12.0, 13.0, 14.0, 15.0, 15.5, 16.0, 17.0, 18.0)
EVE_HOURS = (19.0, 19.5, 20.0, 20.5, 21.0, 21.5, 22.0, 22.5, 23.0)
MORNING = 6.0          # เป้าหมายของคำถาม ก : 06:00 ของวันถัดไป


def evaluate(book, tables, fuel, hour, hz, v2):
    """เหมือน exp_ref_hours.evaluate แต่กำหนดระยะเป้าหมาย (ชม.) ได้ และคืนอัตราที่ราคาขึ้น/ลงจริง (ฐานเปรียบเทียบ)"""
    df = F.build_outlook_dataset(book, tables, fuel, hz, hour, v2=v2)
    recs = df.to_dict("records")
    Xm = np.vstack([(M.vectorize_v2 if v2 else M.vectorize)(r) for r in recs])
    y = df.y.to_numpy()
    yu, yd = (y >= M.OUTLOOK_SMALL).astype(float), (y <= -M.OUTLOOK_SMALL).astype(float)
    pr, pf = F._wf_proba(Xm, yu), F._wf_proba(Xm, yd)
    ok = ~np.isnan(pr)
    y, yu, yd, pr, pf = y[ok], yu[ok], yd[ok], pr[ok], pf[ok]
    up, dn = (pr >= M.OUTLOOK_MIN_P) & (pr > pf), (pf >= M.OUTLOOK_MIN_P) & (pf > pr)
    c, s = up | dn, np.where(up, y, -y)
    first = np.arange(len(y)) < len(y) // 2
    r = lambda m: float((s[m] >= M.OUTLOOK_SMALL).mean()) if m.sum() else float("nan")
    out = {"n": len(y), "auc": float(np.nanmean([F.auc(yu, pr), F.auc(yd, pf)])), "auc_up": F.auc(yu, pr), "auc_dn": F.auc(yd, pf),
           "share": float(c.mean()), "right": r(c), "wrong": float((s[c] <= -M.OUTLOOK_SMALL).mean()) if c.sum() else float("nan"),
           "h1": r(c & first), "h2": r(c & ~first), "base_up": float(yu.mean()), "base_dn": float(yd.mean()),
           "n_up": int(up.sum()), "right_up": r(up), "n_dn": int(dn.sum()), "right_dn": r(dn), "avg": float(y.mean())}
    out["ok"] = bool(out["auc"] >= 0.70 and out["right"] >= 0.60 and out["wrong"] <= 0.12 and out["share"] >= 0.25 and min(out["h1"], out["h2"]) >= 0.55)
    return out


def run(book, tables, hours, hz_of, title):
    for fuel in S.FUELS:
        print(f"\n== {fuel} : {title}")
        print(f"{'เวลา':<7}{'ชุด':<4}{'AUC':>6}{'ขึ้น':>6}{'ลง':>6}{'ฟันธง':>7}{'ถูก':>6}{'ผิด':>6}{'ครึ่ง1':>7}{'ครึ่ง2':>7}{'ขึ้นจริง':>9}{'ลงจริง':>8}{'ทายขึ้น(ถูก)':>14}{'ทายลง(ถูก)':>13}{'เฉลี่ย':>8}  ผล")
        for hour in hours:
            for v2 in (False, True):
                e = evaluate(book, tables, fuel, hour, hz_of(hour), v2)
                print(f"{int(hour):02d}:{int(round(hour % 1 * 60)):02d}  {'v2' if v2 else 'v1':<4}{e['auc']:>6.3f}{e['auc_up']:>6.2f}{e['auc_dn']:>6.2f}{e['share']:>7.0%}{e['right']:>6.0%}{e['wrong']:>6.0%}"
                      f"{e['h1']:>7.0%}{e['h2']:>7.0%}{e['base_up']:>9.0%}{e['base_dn']:>8.0%}{e['n_up']:>8d} ({e['right_up']:.0%}){e['n_dn']:>7d} ({e['right_dn']:.0%}){e['avg']:>+8.2f}"
                      f"  {'ผ่าน' if e['ok'] else '-'}", flush=True)


def main():
    prices, tables = F.load(sys.argv[1])
    book = S.PriceBook(prices)
    which = sys.argv[2] if len(sys.argv) > 2 else "AB"
    if "A" in which:
        run(book, tables, PM_HOURS, lambda h: 24.0 + MORNING - h, "ก) ราคา 06:00 ของวันถัดไป เทียบราคา ณ เวลาตัดสินใจ")
    if "B" in which:
        run(book, tables, EVE_HOURS, lambda h: 24.0, "ข) ราคาอีก 24 ชม. เทียบราคา ณ เวลาตัดสินใจ")


if __name__ == "__main__":
    main()
