"""
การทดลอง: จัดซื้อต้องรู้ล่วงหน้า 10–12 ชม. — ตอนสาย/เที่ยง ทายได้ไหมว่า "ส่งรถตอนนี้ให้ถึงคลังคืนนี้ (ก่อนราคาปรับ)" ถูกกว่า "รอไปถึงบ่ายพรุ่งนี้"

ที่มา : เทียบ PO จริง 46 วัน (23/9–17/11/2026) กับคำแนะนำ "ควรซื้อเมื่อไร" รอบ 20:30 พบว่าส่วนต่างที่ยังเก็บไม่ได้อยู่ที่วัน "ซื้อก่อนขึ้น"
        (จ่ายจริงแพงกว่าราคาคืนก่อน ~0.15 บาท/ลิตร) แต่คำแนะนำออก 20:45 ขณะที่รถใช้เวลา 10–12 ชม. (เจ้าของระบบ 17/11/2026) จึงใช้ไม่ทัน
เทมเพลต (ตั้งก่อนรัน 2026-11-17)
  คำถาม : ณ เวลา h ของวัน d ทายว่า y = ราคาต่ำสุดของตลาด ณ 16:30 ของวัน d+1 − ราคาต่ำสุดของตลาด ณ 22:00 ของวัน d
          y >= +10 สต. = ควรส่งรถตอนนี้ (คืนนี้ถูกกว่าบ่ายพรุ่งนี้) ; y <= −10 สต. = ควรรอ
          หมายเหตุ : เป้าหมายนี้ไม่ได้เทียบกับราคา ณ เวลา h แต่เทียบสองจุดในอนาคตที่จัดซื้อเลือกได้จริง
  h (ตรึง) = 10:00 11:00 12:00 13:00 (ถึงคลัง 22:00 ด้วยระยะ 9–12 ชม.) และ 20:30 ไว้เทียบ (รู้ข้อมูลเย็นแล้ว แต่ไม่ทันรถ)
  โมเดล : โลจิสติกส์เดียวกับของจริง ; ชุดฟีเจอร์ v1 และ v2 (signals.build_features / build_features_v2 ณ เวลา h ; ใช้เฉพาะข้อมูลที่โพสต์ <= h)
  การทดสอบ : walk-forward เดียวกับของจริง (ฝึกขั้นต่ำ 150 วัน ก้าว 30 วัน) ; เกณฑ์ฟันธง 0.50
  เกณฑ์ "ใช้ได้" (เหมือน exp_ref_hours.py) : AUC เฉลี่ย >= 0.70 ; ฟันธงแล้วถูกทิศ >= 60% ผิดทิศ <= 12% ; ฟันธง >= 25% ของวัน ; ถูกทิศ >= 55% ทั้งสองครึ่ง
  ตัววัดเสริม (ไม่ใช่เกณฑ์) : กำไรของนโยบาย บาท/ลิตร/วัน เทียบ "รอบ่ายทุกวัน" = เฉลี่ยของ y ในวันที่ทายขึ้น (ส่งรถคืนนี้) คูณสัดส่วนวันที่ทายขึ้น
  ข้อควรระวัง : 5 เวลา x 2 ชนิด x 2 ชุด ; ราคา = ต่ำสุดของตลาด ไม่ใช่คลังที่ซื้อจริง ; ไม่หักโควต้า/เวลาตัดรับออเดอร์ ; ผลย้อนหลังต้องยืนยันด้วยผลจริง

ผล (รัน 2026-11-17 ; oil-bot/training_data ถึง 21/10/2026 ; ~647 วันทดสอบ ; ราคา 16:30 พรุ่งนี้สูงกว่า 22:00 คืนนี้ >= 10 สต. 38% ของวัน (ดีเซล) 31% (G95) ต่ำกว่า 30% / 28%)
  ไม่มีเวลาใดที่ทันรถ (10:00–13:00) ผ่านเกณฑ์ ทั้งสองชนิด ทั้งสองชุดฟีเจอร์
    ดีเซล v2 : AUC 0.63–0.66 ฟันธง 32–36% ถูก 45–50% ผิดทิศ 26–31% ; ทาย "ส่งรถคืนนี้" ถูก 42–50% ผิด 25–33% ; กำไรเทียบรอบ่ายทุกวัน +0.006 ถึง +0.022 บาท/ลิตร/วัน
    G95  v2 : AUC 0.60–0.62 ถูก 29–34% ผิดทิศ 23–29% (แย่กว่าเดา) ; v1 แทบไม่ฟันธง (3%)
  20:30 (เทียบ) : ดีเซลผ่าน (v2 AUC 0.82 ถูก 68% ผิด 9% กำไร +0.090 บาท/ลิตร/วัน ; ครึ่งหลัง +0.126) ; G95 v2 ผ่าน (64%/7% กำไร +0.046)
    เพดาน (รู้อนาคต) : ดีเซล 0.178 G95 0.109 บาท/ลิตร/วัน → รอบ 20:30 เก็บได้ราวครึ่งหนึ่ง ; รอบเที่ยงเก็บได้ ~1/10
  ส่วนขยาย (เพิ่มหลังเห็นผลข้างบน จึงเป็นการสำรวจ ไม่ใช่การทดสอบตามเทมเพลต) :
    20:30 ของคืนก่อนหน้า (รู้ล่วงหน้า ~25 ชม.) , 06:00 , 08:00 : ไม่ผ่านเช่นกัน (ดีเซล AUC 0.61–0.63 ถูก 44–47% ; G95 ถูก 33–39%)
    15:30 : ดีเซล v2 AUC 0.73 ถูก 62% ผิด 17% (ไม่ผ่าน) ; 18:00 : ดีเซล v1 ผ่านเฉียด (64%/12%) v2 64%/16% ; G95 ไม่ผ่านทั้งคู่
  สรุป : การตัดสินใจ "ให้รถถึงคลังคืนนี้ หรือบ่ายพรุ่งนี้" ทายไม่ได้ในเวลาที่ต้องปล่อยรถ (ล่วงหน้า 10–12 ชม.) — ข้อมูลที่ใช้ทายออกช่วง 15:30–20:30
         โมเดลช่วยได้เฉพาะเมื่อการตัดสินใจเกิดหลังรถถึงคลังแล้ว (โหลดคืนนี้ หรือรอโหลดบ่ายพรุ่งนี้) ; ไม่เปลี่ยนระบบ

มูลค่าของนโยบายเมื่อรถอยู่ที่คลังช่วงค่ำแล้ว (เจ้าของระบบ 17/11 : รถรอที่คลังข้ามคืนได้) — walk-forward รอบ 20:30 ชุด v2 645 วัน ; บาท/ลิตร เทียบ "โหลดคืนนี้ทุกวัน"
    ดีเซล : รอบ่ายทุกวัน +0.029 (ปี 2026 +0.073) · รอเฉพาะเมื่อทายลง นอกนั้นโหลดคืนนี้ −0.097 (ปี 2026 −0.130) · ตามคำทาย ไม่ชัด = รอบ่าย −0.060 · รู้อนาคต −0.149
    G95  : รอบ่ายทุกวัน +0.012 (ปี 2026 +0.045) · รอเฉพาะเมื่อทายลง −0.059 (ปี 2026 −0.085) · ไม่ชัด = รอบ่าย −0.033 · รู้อนาคต −0.097
    วันที่ไม่ฟันธง (47% ดีเซล 58% G95) : ราคา 16:30 วันถัดไปสูงกว่า 22:00 เฉลี่ย +0.077 / +0.045 → ค่าเริ่มต้นที่ดีกว่าคือ "โหลดคืนนี้" (ใช้ใน line_outlook.DEFAULT_NOTE)
    หมายเหตุ : โมเดลของการทดลองนี้ฝึกกับเป้าหมาย 22:00→16:30 โดยตรง ; ระบบจริงใช้แนวโน้ม 24 ชม. + โมเดลรายช่วงเวลา (20:30→16:30) ซึ่งใกล้เคียงแต่ไม่ใช่ตัวเดียวกัน
    PO จริง 46 วัน (23/9–17/11/2026 ; คำแนะนำคำนวณย้อนหลังด้วยโมเดลจริง) : ทำตามคำแนะนำถูกกว่าที่จ่ายจริง 0.04–0.06 บาท/ลิตร (ช่วงที่โมเดลไม่เคยเห็น 0.06–0.07)

ใช้: python exp_lead_time.py <data_dir ของ jobber>
"""
import sys
import warnings

import numpy as np
import pandas as pd

import fit_model as F
import model as M
import signals as S

warnings.filterwarnings("ignore")
HOURS = (10.0, 11.0, 12.0, 13.0, 20.5)
TONIGHT, NEXT_PM = 22.0, 24.0 + 16.5


def dataset(book, tables, fuel, hour, v2):
    days = pd.date_range(tables.headsup.ts.min().normalize() + pd.Timedelta(days=10), tables.headsup.ts.max().normalize() - pd.Timedelta(days=3))
    p0 = book.min_at(fuel, [d + pd.Timedelta(hours=TONIGHT) for d in days]).to_numpy()
    p1 = book.min_at(fuel, [d + pd.Timedelta(hours=NEXT_PM) for d in days]).to_numpy()
    rows = []
    for d, a, b in zip(days, p0, p1):
        if np.isnan(a) or np.isnan(b):
            continue
        asof = d + pd.Timedelta(hours=hour)
        f = S.build_features_v2(asof, tables, fuel, NEXT_PM - hour) if v2 else S.build_features(asof, tables, fuel)
        f.update({"date": d, "y": b - a})
        rows.append(f)
    return pd.DataFrame(rows)


def evaluate(book, tables, fuel, hour, v2):
    df = dataset(book, tables, fuel, hour, v2)
    Xm = np.vstack([(M.vectorize_v2 if v2 else M.vectorize)(r) for r in df.to_dict("records")])
    y = df.y.to_numpy()
    yu, yd = (y >= M.OUTLOOK_SMALL).astype(float), (y <= -M.OUTLOOK_SMALL).astype(float)
    pr, pf = F._wf_proba(Xm, yu), F._wf_proba(Xm, yd)
    ok = ~np.isnan(pr)
    y, yu, yd, pr, pf = y[ok], yu[ok], yd[ok], pr[ok], pf[ok]
    up, dn = (pr >= M.OUTLOOK_MIN_P) & (pr > pf), (pf >= M.OUTLOOK_MIN_P) & (pf > pr)
    c, s = up | dn, np.where(up, y, -y)
    first = np.arange(len(y)) < len(y) // 2
    r = lambda m: float((s[m] >= M.OUTLOOK_SMALL).mean()) if m.sum() else float("nan")
    out = {"n": len(y), "auc": float(np.nanmean([F.auc(yu, pr), F.auc(yd, pf)])), "share": float(c.mean()), "right": r(c),
           "wrong": float((s[c] <= -M.OUTLOOK_SMALL).mean()) if c.sum() else float("nan"), "h1": r(c & first), "h2": r(c & ~first),
           "base_up": float(yu.mean()), "base_dn": float(yd.mean()), "n_up": int(up.sum()), "right_up": r(up), "wrong_up": float((y[up] <= -M.OUTLOOK_SMALL).mean()) if up.sum() else float("nan"),
           "n_dn": int(dn.sum()), "right_dn": r(dn), "avg": float(y.mean()), "gain": float(y[up].sum() / len(y)) if len(y) else float("nan"),
           "gain_h2": float(y[up & ~first].sum() / max((~first).sum(), 1)), "oracle": float(np.clip(y, 0, None).mean())}
    out["ok"] = bool(out["auc"] >= 0.70 and out["right"] >= 0.60 and out["wrong"] <= 0.12 and out["share"] >= 0.25 and min(out["h1"], out["h2"]) >= 0.55)
    return out


def main():
    prices, tables = F.load(sys.argv[1])
    book = S.PriceBook(prices)
    for fuel in S.FUELS:
        print(f"\n== {fuel} : ราคา 16:30 พรุ่งนี้ เทียบ 22:00 คืนนี้ (บวก = ควรส่งรถให้ถึงคืนนี้)")
        print(f"{'เวลา':<7}{'ชุด':<4}{'AUC':>6}{'ฟันธง':>7}{'ถูก':>6}{'ผิด':>6}{'ครึ่ง1':>7}{'ครึ่ง2':>7}{'ขึ้นจริง':>9}{'ลงจริง':>8}{'ทายขึ้น(ถูก/ผิด)':>18}{'ทายลง(ถูก)':>13}{'กำไร':>8}{'ครึ่งหลัง':>10}{'เพดาน':>7}  ผล")
        for hour in HOURS:
            for v2 in (False, True):
                e = evaluate(book, tables, fuel, hour, v2)
                print(f"{int(hour):02d}:{int(round(hour % 1 * 60)):02d}  {'v2' if v2 else 'v1':<4}{e['auc']:>6.3f}{e['share']:>7.0%}{e['right']:>6.0%}{e['wrong']:>6.0%}{e['h1']:>7.0%}{e['h2']:>7.0%}"
                      f"{e['base_up']:>9.0%}{e['base_dn']:>8.0%}{e['n_up']:>8d} ({e['right_up']:.0%}/{e['wrong_up']:.0%}){e['n_dn']:>7d} ({e['right_dn']:.0%}){e['gain']:>+8.3f}{e['gain_h2']:>+10.3f}{e['oracle']:>7.3f}"
                      f"  {'ผ่าน' if e['ok'] else '-'}", flush=True)


if __name__ == "__main__":
    main()
