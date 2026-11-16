"""
การทดลอง: แยกระดับ "ยังไม่ชัดเจน" ของโมเดลแนวโน้มเป็น "น่าจะทรงตัว" กับ "อาจขยับแต่บอกทิศไม่ได้"

ที่มา : "ไม่ชัด" เกิดราวครึ่งหนึ่งของวัน และในวันเหล่านั้นราคาขยับจริงเกินครึ่ง (ดีเซล 59% G95 50%) ผู้อ่านจึงแยกไม่ออกว่าวันไหนพักได้ วันไหนต้องระวัง
เทมเพลต (ตั้งก่อนรัน 2026-11-16)
  กติกา (ตรึงก่อนดูผล ไม่จูน) : ในวันที่ระดับเป็น "ไม่ชัด" ให้เรียก "น่าจะทรงตัว" เมื่อ โอกาสขึ้น>=10 สต. + โอกาสลง>=10 สต. <= 0.50
     (= โมเดลเองให้โอกาสทรงตัวอย่างน้อยครึ่งหนึ่ง ; คู่ขนานกับเกณฑ์ฟันธง 0.50 ที่ใช้อยู่) ที่เหลือ = "อาจขยับ บอกทิศไม่ได้"
  ไม่มีการฝึกโมเดลใหม่ : ใช้ความน่าจะเป็น walk-forward ของโมเดลที่ใช้งานจริง (ชุดฟีเจอร์ตาม key)
  ตัวชี้วัด : สัดส่วนที่ราคาทรงตัวจริง (|ขยับ| < 10 สต.) ของแต่ละกลุ่ม , ครึ่งแรก/ครึ่งหลัง , จำนวนวัน
  เกณฑ์ผ่าน (ต่อ key ครบทุกข้อ จึงแสดงคำว่า "น่าจะทรงตัว" ของ key นั้น) :
     1) กลุ่ม "น่าจะทรงตัว" มีอย่างน้อย 40 วัน
     2) ทรงตัวจริง >= 60% และสูงกว่ากลุ่ม "อาจขยับ" อย่างน้อย 15 จุด
     3) ทรงตัวจริง >= 50% ทั้งครึ่งแรกและครึ่งหลังของช่วงทดสอบ
  ข้อควรระวัง : ไม่ได้ทำให้ระบบฟันธงทิศมากขึ้น เป็นการแยกข้อความเท่านั้น ; ระดับทั้ง 5 และสถิติเดิมไม่เปลี่ยน

ผล (รัน 2026-11-16 ; 645 วันทดสอบต่อ key) : ไม่ผ่านทุก key → ไม่แสดงคำว่า "น่าจะทรงตัว"
  B7_H24  : ไม่ชัด 310 วัน ; กลุ่ม "น่าจะทรงตัว" 163 วัน ทรงตัวจริง 46% (ครึ่งแรก 50% ครึ่งหลัง 36%) ; กลุ่ม "อาจขยับ" 147 วัน ทรงตัวจริง 35%
  G95_H24 : ไม่ชัด 375 วัน ; กลุ่ม "น่าจะทรงตัว" 273 วัน ทรงตัวจริง 52% (ครึ่งแรก 60% ครึ่งหลัง 44%) ; กลุ่ม "อาจขยับ" 102 วัน ทรงตัวจริง 44%
  รอบ 15:30 : ทรงตัวจริง 41% (ดีเซล) 47% (G95) ; ช่วง 48 ชม. แทบไม่มีวันเข้ากลุ่ม
  สรุป : ต่างกันราว 10 จุด (เกณฑ์ 15) และไม่ถึง 60% ; ครึ่งหลังของช่วงทดสอบต่ำกว่า 50% ทุก key — เรียก "น่าจะทรงตัว" จะผิดราวครึ่งหนึ่ง
  ข้อสังเกต (เห็นหลังรัน ยังไม่ใช่ข้อสรุป) : ในกลุ่มนี้ราคาขยับแรง (>= 40 สต.) เกิดน้อย : G95 6% ดีเซล 11% — ถ้าจะใช้ต้องตั้งการทดลองใหม่พร้อมเกณฑ์ก่อนรัน

ใช้: python exp_unclear_split.py <data_dir ของ jobber>
"""
import sys
import warnings

import numpy as np
import pandas as pd

import fit_model as F
import model as M
import signals as S

warnings.filterwarnings("ignore")
QUIET_MAX_MOVE_P = 0.50


def run(key, fuel, hz, ref_hour, book, tables):
    v2 = key in M.OUTLOOK_V2_KEYS
    df = F.build_outlook_dataset(book, tables, fuel, hz, ref_hour, v2=v2)
    recs = df.to_dict("records")
    X = np.vstack([(M.vectorize_v2 if v2 else M.vectorize)(r) for r in recs])
    y = df.y.to_numpy()
    tg = {"r10": y >= M.OUTLOOK_SMALL, "f10": y <= -M.OUTLOOK_SMALL, "r40": y >= M.OUTLOOK_BIG, "f40": y <= -M.OUTLOOK_BIG}
    P = {k: F._wf_proba(X, v.astype(float)) for k, v in tg.items()}
    ok = ~np.isnan(P["r10"])
    y, P = y[ok], {k: v[ok] for k, v in P.items()}
    unclear = np.array([M.classify_outlook(P["r10"][i], P["f10"][i], P["r40"][i], P["f40"][i]) == "UNCLEAR" for i in range(len(y))])
    quiet = unclear & (P["r10"] + P["f10"] <= QUIET_MAX_MOVE_P)
    mixed = unclear & ~quiet
    flat = np.abs(y) < M.OUTLOOK_SMALL
    h = len(y) // 2
    first = np.arange(len(y)) < h
    r = lambda m: float(flat[m].mean()) if m.sum() else float("nan")
    q, mx = r(quiet), r(mixed)
    q1, q2 = r(quiet & first), r(quiet & ~first)
    c1, c2, c3 = quiet.sum() >= 40, (q >= 0.60 and q - mx >= 0.15), (min(q1, q2) >= 0.50 if quiet.sum() else False)
    print(f"{key:<10} วันทดสอบ {len(y)} ทรงตัวจริงทั้งหมด {flat.mean():.0%} | ไม่ชัด {unclear.sum()} วัน ทรงตัวจริง {r(unclear):.0%} | "
          f"น่าจะทรงตัว {quiet.sum()} วัน ทรงตัวจริง {q:.0%} (ครึ่งแรก {q1:.0%} ครึ่งหลัง {q2:.0%} ; ขยับ>=40 สต. {(np.abs(y[quiet]) >= .4).mean() if quiet.sum() else float('nan'):.0%}) | "
          f"อาจขยับ {mixed.sum()} วัน ทรงตัวจริง {mx:.0%} | เกณฑ์ {'ผ่าน' if c1 else 'ไม่ผ่าน'}/{'ผ่าน' if c2 else 'ไม่ผ่าน'}/{'ผ่าน' if c3 else 'ไม่ผ่าน'} → {'ผ่าน' if c1 and c2 and c3 else 'ไม่ผ่าน'}")


def main():
    prices, tables = F.load(sys.argv[1])
    book = S.PriceBook(prices)
    for fuel in S.FUELS:
        for tag, ref in (("", M.OUTLOOK_REF_HOUR), ("P_", M.OUTLOOK_REF_PM)):
            for hz in M.OUTLOOK_HORIZONS:
                run(f"{fuel}_{tag}H{hz}", fuel, hz, ref, book, tables)


if __name__ == "__main__":
    main()
