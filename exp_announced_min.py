"""
การทดลอง: ให้โมเดลรอบ 20:30 รู้ "ราคาต่ำสุดตลาดหลังการปรับที่ผู้ค้าประกาศ" (ไม่ใช่เรทสูงสุดที่ประกาศ)

ที่มา (25/11/2026) : exp_headsup_rate.py ใช้ "เรทขึ้นสูงสุด" แล้วไม่ช่วย ; คืน 25/11 ไออาร์ประกาศดีเซล +2.00
   แต่ราคาต่ำสุดตลาดหลังปรับ (ประมาณ) ขึ้นเพียง +0.06 เพราะเจ้าที่ถูกสุดไม่ได้ประกาศเรทดีเซล
   → โมเดลทายราคาต่ำสุดของตลาด จึงควรได้ตัวแปรที่วัดผลต่อ "ราคาต่ำสุด" โดยตรง

เทมเพลต (ตั้งก่อนรัน 2026-11-25 ; เจ้าของระบบสั่งให้เริ่ม)
  ตัวแปรใหม่ (คำนวณแบบเดียวกับกล่อง "คืนนี้ตามประกาศ" ในไลน์ : line_outlook.notices + tonight_change) ณ 20:30 :
     ann_d      = ราคาต่ำสุดตลาดหลังปรับตามประกาศที่ยังไม่มีผล − ราคาต่ำสุดตอนนี้ (บาท/ลิตร , 0 ถ้าไม่มีประกาศ , ตัดที่ ±3)
                  ราคาหลังปรับ = ราคาล่าสุดของแต่ละ (ผู้ค้า, คลัง) + เรทที่ผู้ค้านั้นประกาศ ; ผู้ค้าที่ไม่ได้ประกาศ/ไม่แจ้งเรท = ราคาเดิม
     ann_norate = จำนวนผู้ค้าที่ประกาศปรับคืนนี้แต่ไม่แจ้งเรทของชนิดนี้ (0–4)
     นับเฉพาะประกาศที่ระบุเวลามีผลหรือเรท (เหมือนของจริง)
  ทางเลือก : A1 = ชุดเดิม + ann_d ; A2 = ชุดเดิม + ann_d + ann_norate
  ขอบเขตและเกณฑ์ : เหมือน exp_headsup_rate.py ทุกข้อ (6 key รอบ 20:30 ; walk-forward 150/30 ; คืนประกาศขึ้น = เรท >= 0.30)
     ผ่าน = AUC เฉลี่ย +0.01 และไม่แย่ลงทั้งสองครึ่ง ; ทั้งช่วงฟันธงถูกไม่ลด ผิดทิศไม่เพิ่มเกิน 1 จุด ; คืนประกาศขึ้น "ทายลงแต่ขึ้นจริง" ลดลง
     ผ่านทั้งคู่ → AUC สูงกว่า (ห่าง < 0.005 เลือก A1)
  รายงานเพิ่ม : ความครอบคลุม (กี่คืนที่ ann_d ≠ 0) และ ann_d เทียบการขยับจริงของราคาต่ำสุด ณ 00:30 (ตัวแปรวัดสิ่งที่ตั้งใจหรือไม่)
  ข้อควรระวัง : มีเรทในประกาศเพียง ~8% ของข้อความ ; ข้อมูลฝึกถึง 21/10/2026 ; ทดสอบหลายชุดจึงต้องผ่านทุกข้อพร้อมกัน

ผล (รัน 2026-11-25 ; oil-bot/training_data ถึง 21/10/2026) : ไม่ผ่านทุก key → คงโมเดลเดิม
   AUC เดิม → A1 / A2 : B7_H24 0.793 → 0.788/0.791 ; G95_H24 0.771 → 0.776/0.777 ; B7_T16 0.791 → 0.787/0.787
                        B7_T20 0.819 → 0.812/0.811 ; G95_T16 0.758 → 0.763/0.763 ; G95_T20 0.780 → 0.785/0.788
   G95 ดีขึ้นเล็กน้อยทุก key (+0.005–0.008 ทั้งสองครึ่ง) แต่ไม่ถึง 0.01 และฟันธงแล้วถูกลดลง ; ดีเซลแย่ลงทุก key
   ความครอบคลุม : ann_d ≠ 0 เพียง ~120 จาก ~795 คืน ; คืนที่มีประกาศแต่ไม่แจ้งเรท ~475 คืน
   ความแม่นของตัวเลข "ราคาต่ำสุดหลังปรับ" (คืนที่ ann_d ≠ 0 ; เทียบราคาต่ำสุดจริง 08:30 วันถัดไป เพราะราคาใหม่มักโพสต์ตอนเช้า) :
     ดีเซล ประมาณ +0.40 จริง +0.29 ; สหสัมพันธ์ 0.19 ; คลาดเฉลี่ย 0.36 ; ห่างไม่เกิน 0.10 เพียง 30% ของคืน
     G95   ประมาณ +0.31 จริง +0.27 ; สหสัมพันธ์ 0.58 ; คลาดเฉลี่ย 0.28 ; ห่างไม่เกิน 0.10 34%
   การตีความ : เรทในประกาศเป็น "โดยประมาณ" และมักไม่ครบทุกเจ้า (ดีเซลแย่กว่า G95) ตัวเลขจึงบอกทิศได้แต่คลาดรายคืนมาก
     ต้องรอข้อมูลเรทรายคลัง (PriceNotice เริ่ม 21/10/2026) สะสม 6–12 เดือนก่อนทดลองซ้ำ
   ผลต่อการแสดงผล : กล่อง "คืนนี้ตามประกาศ" บอกความคลาดเคลื่อนย้อนหลัง (ดีเซล ±0.36 / G95 ±0.28) ต่อท้ายตัวเลขประมาณ

ใช้: python exp_announced_min.py <data_dir ของ jobber>   (เช่น ../oil-bot/training_data)
"""
import argparse
import warnings

import numpy as np
import pandas as pd

import exp_headsup_rate as X
import fit_model as F
import line_outlook as L
import model as M
import signals as S

warnings.filterwarnings("ignore")
VARIANTS = ("A1", "A2")


def ann_features(tables: S.Tables, asof: pd.Timestamp, fuel: str) -> dict:
    kp = tables.known(fuel)
    prices = kp.latest(kp.snapshot(asof), asof).to_dict() if not kp.empty else {}
    inp = {"asof": asof.strftime("%Y-%m-%d %H:%M"), "notices": L.notices(tables.headsup, asof), "prices_now": {fuel: prices}}
    c = L.tonight_change(inp, fuel)
    if not c or c["now"] is None:
        return {"ann_d": 0.0, "ann_norate": float(c["n_norate"]) if c else 0.0}
    return {"ann_d": float(np.clip(c["after"] - c["now"], -3, 3)), "ann_norate": float(min(c["n_norate"], 4))}


def run_key(book, tables, key, fuel, hz, kind):
    df = F.build_outlook_dataset(book, tables, fuel, hz, M.OUTLOOK_REF_HOUR, v2=(kind != "v1s"))
    asofs = [pd.Timestamp(d) + pd.Timedelta(hours=M.OUTLOOK_REF_HOUR) for d in df.date]
    ann = pd.DataFrame([ann_features(tables, a, fuel) for a in asofs])
    hu = pd.DataFrame([X.hu_features(tables, a, fuel) for a in asofs])
    y = df.y.to_numpy()
    X0 = X.base_matrix(df, kind)
    target = hu.rate_up.to_numpy() >= X.TARGET_RATE
    allm = np.ones(len(y), bool)
    h24 = hz == 24
    c0, pr0, pf0 = X.wf(X0, y, h24)
    a0 = X.auc3(y, pr0, pf0)
    s0, t0 = X.stats(y, c0, allm), X.stats(y, c0, target)
    nz = ann.ann_d.to_numpy() != 0
    night = book.min_at(fuel, [a + pd.Timedelta(hours=4) for a in asofs]).to_numpy() - book.min_at(fuel, asofs).to_numpy()
    ok = nz & ~np.isnan(night)
    corr = float(np.corrcoef(ann.ann_d.to_numpy()[ok], night[ok])[0, 1]) if ok.sum() > 5 else np.nan
    print(f"\n== {key} ({kind}) : {len(y)} วัน ; ann_d ≠ 0 {int(nz.sum())} คืน (ขึ้น {int((ann.ann_d > 0).sum())} / ลง {int((ann.ann_d < 0).sum())}) ; "
          f"มีประกาศไม่แจ้งเรท {int((ann.ann_norate > 0).sum())} คืน")
    print(f"   ann_d เทียบราคาต่ำสุดที่ขยับจริงถึง 00:30 (คืนที่ ann_d ≠ 0) : สหสัมพันธ์ {corr:.2f} ; "
          f"เฉลี่ย ann_d {np.nanmean(ann.ann_d.to_numpy()[ok]):+.2f} จริง {np.nanmean(night[ok]):+.2f}")
    print(f"   เดิม : AUC {a0[0]:.3f} (ครึ่ง {a0[1]:.3f}/{a0[2]:.3f})")
    print(f"          ทั้งช่วง {X.fmt(s0)}")
    print(f"          คืนประกาศขึ้น {X.fmt(t0)}")
    res = {"key": key, "base": a0[0], "pass": {}}
    for var in VARIANTS:
        cols = [ann.ann_d.to_numpy()] + ([ann.ann_norate.to_numpy()] if var == "A2" else [])
        c1, pr1, pf1 = X.wf(np.column_stack([X0] + cols), y, h24)
        a1 = X.auc3(y, pr1, pf1)
        s1, t1 = X.stats(y, c1, allm), X.stats(y, c1, target)
        ok1 = a1[0] - a0[0] >= 0.01 and a1[1] >= a0[1] and a1[2] >= a0[2]
        ok2 = (not s1["n"] or not s0["n"]) or (s1["right"] >= s0["right"] and s1["wrong"] <= s0["wrong"] + 0.01)
        ok3 = t1["down_but_up"] < t0["down_but_up"] or (t0["down_but_up"] == 0 and t1["down_but_up"] == 0)
        res["pass"][var] = (ok1 and ok2 and ok3, a1[0])
        print(f"   {var}: AUC {a1[0]:.3f} (ครึ่ง {a1[1]:.3f}/{a1[2]:.3f}) [{'✓' if ok1 else '✗'}]")
        print(f"          ทั้งช่วง {X.fmt(s1)} [{'✓' if ok2 else '✗'}]")
        print(f"          คืนประกาศขึ้น {X.fmt(t1)} [{'✓' if ok3 else '✗'}]  → {'ผ่าน' if res['pass'][var][0] else 'ไม่ผ่าน'}")
    return res


def choose(res):
    passed = {v: a for v, (ok, a) in res["pass"].items() if ok}
    if not passed:
        return "ไม่ผ่าน → คงเดิม"
    if "A1" in passed and max(passed.values()) - passed["A1"] < 0.005:
        return "ใช้ A1"
    return f"ใช้ {max(passed, key=passed.get)}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data_dir")
    a = ap.parse_args()
    prices, tables = F.load(a.data_dir)
    book = S.PriceBook(prices)
    out = [run_key(book, tables, *k) for k in X.KEYS]
    print("\n== สรุป")
    for r in out:
        print(f"   {r['key']:<8} เดิม AUC {r['base']:.3f} ; " + " ".join(f"{v} {a:.3f}{'✓' if ok else ''}" for v, (ok, a) in r["pass"].items())
              + f" → {choose(r)}")


if __name__ == "__main__":
    main()
