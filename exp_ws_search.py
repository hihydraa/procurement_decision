"""
การทดลอง: การ์ดต้นทุนขายส่ง (WS ของ EPPO วันพรุ่งนี้) — ทำไมดีเซลผิด และโมเดลจากสัญญาณ Jobber ดีกว่าวิธีที่ใช้อยู่หรือไม่

ที่มา : การ์ดดีเซลฟันธง "ลง" 2 วันติด (12 และ 13/11/2026) ผลจริงนิ่งทั้งคู่ ; คืน 12/11 มีประกาศกองทุน −0.46 บาท/ลิตร
ข้อเท็จจริงจากไฟล์โครงสร้างราคา EPPO (สำรวจก่อนตั้งการทดลอง ; คู่วันติดกัน) :
  ปี 2024–2025 : กองทุนขยับ ~25% ของวัน และ WS ขยับตามกองทุน (ทิศเดียวกัน 85–93%) — ราคาปลีกถูกตรึง
  ปี 2026      : กองทุนดีเซลขยับ 63% ของวัน แต่ WS นิ่ง 54% ของวันเหล่านั้น (ทิศเดียวกับกองทุน 27% สวนทาง 19%) ; corr(Δหน้าโรงกลั่น, Δกองทุน) −0.40 (ดีเซล) −0.52 (G95)
     = กองทุนถูกตั้งเพื่อหักล้างราคาหน้าโรงกลั่น WS จึงขยับเฉพาะวันที่ราคาปลีกถูกปรับ (ΔWS ≈ Δปลีก/1.07)
  Δหน้าโรงกลั่นของพรุ่งนี้ทายจาก ΔMOPS วันนี้ได้หยาบ (คลาดเคลื่อนมัธยฐาน 0.33 บาท ดีเซล) จึงบวก "Δหน้าโรงกลั่นที่ทาย + Δกองทุนที่ประกาศ" แล้วแยก 0 กับการปรับจริงไม่ได้

เทมเพลต (ตั้งก่อนรัน 2026-11-13)
  สมมติฐาน : ทิศของ WS พรุ่งนี้ทายจากสัญญาณคืนนี้ (จำนวนผู้ค้าที่ประกาศขึ้น/ลง, MOPS ปิดล่าสุด, ประกาศกองทุน) ด้วยโลจิสติกส์ ได้ดีกว่าวิธีปัจจุบัน (ridge + การกระจายของความคลาดเคลื่อน)
  ชุดฟีเจอร์ : SIG ชุดเดียวกับการทดลองปลีก (ตรึง ไม่เลือกใหม่) ; ฝึกเฉพาะข้อมูลปี 2026 เพราะกลไกเปลี่ยนจากปี 2024–2025 (ข้างบน)
  ฐานเปรียบเทียบ : (ก) ความถี่ในอดีต  (ข) วิธีปัจจุบัน จำลองตามโค้ดจริง : ดีเซล = โมเดล C + Δกองทุน/ภาษีจริงของวันถัดไป (แทนประกาศ), G95 = DIRECT ; ความน่าจะเป็นจาก forecast_card.probabilities (รวมเพดาน 75%)
  เวลาที่รู้จริง : สัญญาณตัดที่ 20:30 วัน t ; walk-forward ขยายหน้าต่าง ฝึกขั้นต่ำ 50 วัน ก้าว 10 วัน
  ตัวชี้วัด : AUC ขึ้น/ลง (>= 10 สต.) , Brier 3 ชั้น , ฟันธงด้วยเกณฑ์ของการ์ด (forecast_card.decide) : จำนวนครั้ง ถูกทิศ ผิดทิศ ฟันธงแต่ราคานิ่ง
  เกณฑ์ผ่าน (ต่อชนิดน้ำมัน ครบทุกข้อ) :
     1) AUC เฉลี่ย >= 0.70 และไม่ต่ำกว่า 0.60 ในครึ่งใดครึ่งหนึ่ง
     2) Brier ต่ำกว่าวิธีปัจจุบัน โดยช่วงเชื่อมั่น 95% ของส่วนต่างไม่คร่อมศูนย์ และต่ำกว่าความถี่ในอดีต
     3) จำนวนครั้งที่ฟันธงแล้วไม่ถูก (ผิดทิศ + ราคานิ่ง) ไม่มากกว่าวิธีปัจจุบัน
  ข้อควรระวัง : ช่วงทดสอบร่วมราว 80 วันจากปีเดียว ; ข้อเท็จจริงข้างบนสำรวจจากข้อมูลชุดเดียวกัน ; ผ่านแล้วก็ยังเป็นหลักฐานย้อนหลัง

ผล (รัน 2026-11-13 ; วันทดสอบร่วม 78 วัน 06/06–20/10/2026 ; ขึ้น ~18% ลง 14% นิ่ง ~68%)
  ดีเซล : ปัจจุบัน AUC 0.78/0.59 Brier 0.939 ฟันธง 64 ครั้ง ถูก 16 ผิดทิศ 6 ราคานิ่ง 42 ; ใหม่ AUC 0.85/0.61 (ครึ่งหลัง 0.55) Brier 0.455 ฟันธง 9 ถูก 7 ผิดทิศ 0 นิ่ง 2
          ความถี่ในอดีต Brier 0.532 ; ส่วนต่าง Brier −0.484 (95% −0.615 ถึง −0.340) ; เกณฑ์ 1 ไม่ผ่าน (ครึ่งหลัง < 0.60) เกณฑ์ 2, 3 ผ่าน → ยังไม่ใช้เป็นคำทายหลัก
  G95   : ปัจจุบัน AUC 0.74/0.70 Brier 0.870 ฟันธง 52 ครั้ง ถูก 13 ผิดทิศ 2 นิ่ง 37 ; ใหม่ AUC 0.82/0.77 (ครึ่ง 0.87/0.66) Brier 0.431 ฟันธง 9 ถูก 7 ผิดทิศ 0 นิ่ง 2
          ความถี่ในอดีต Brier 0.537 ; ส่วนต่าง Brier −0.439 (95% −0.568 ถึง −0.302) ; ผ่านครบ 3 ข้อ → ใช้โมเดลใหม่
  การตัดสินใจ : วิธีปัจจุบัน Brier แย่กว่าความถี่ในอดีตเกือบเท่าตัวและฟันธงถูกราว 1 ใน 4 ทั้งสองชนิด → เลิกให้ฟันธงทิศ (ทั้งสองชนิด)
     G95 ใช้โมเดลสัญญาณเป็นคำทายหลัก ; ดีเซลบันทึกโมเดลสัญญาณคู่ขนาน (forecast_card.WS_SIGNAL_FUELS)

ใช้: python exp_ws_search.py <data_dir ของ jobber> <eppo_comp_long.csv (day,fuel,ws,exref,fund,other,mm,retail,resid)> <mops.json> <wti.json>
"""
import json
import sys
import warnings

import numpy as np
import pandas as pd

import exp_retail_search as X
import fit_model as F
import forecast_card as FC
import model as M
import retail_forecast as RF
from diag_rule_backtest import mops_daily, wti_daily
from diag_wf_components import build_dataset, walk_forward

warnings.filterwarnings("ignore")
MIN_TRAIN, STEP = 50, 10
TRAIN_FROM = pd.Timestamp("2026-01-01")
OLD_COL = {"B7": "C", "G95": "DIRECT"}


def dataset(tables, e, fuel, last):
    g = e[(e.fuel == fuel) & (e.day >= TRAIN_FROM)].drop_duplicates("day", keep="last").set_index("day").sort_index()
    rows = []
    for day, r in g.iterrows():
        nxt = day + pd.Timedelta(days=1)
        if nxt in g.index and day <= last:
            rows.append({"day": day, "dret": float(g.ws[nxt] - r.ws), **RF.signal_features(tables, fuel, day)})     # ใช้ชื่อ dret เพื่อใช้ X.new_probs ร่วม (ค่า = ΔWS)
    return pd.DataFrame(rows)


def old_probs(wfr, fuel, days):
    w = wfr[wfr.fuel == fuel].sort_values("day")
    col, out = OLD_COL[fuel], {}
    for t in days:
        row, past = w[w.day == t], w[w.day < t]
        if row.empty or len(past) < FC.MIN_RESID:
            continue
        p = FC.probabilities(float(row[col].iloc[0]), (past.actual - past[col]).to_numpy(float))
        out[t] = (p["p_up"], p["p_down"])
    return out


def score(name, y, pu, pdn):
    yu, yd = (y >= FC.UP).astype(float), (y <= -FC.UP).astype(float)
    pf = np.clip(1 - pu - pdn, 0, 1)
    bi = (pu - yu) ** 2 + (pdn - yd) ** 2 + (pf - (1 - yu - yd)) ** 2
    calls = [FC.decide({"p_down": d, "p_up": u})[0] for u, d in zip(pu, pdn)]
    n = right = wrong = flat = 0
    for c, v in zip(calls, y):
        if c in ("ขึ้น", "ลง"):
            s = v if c == "ขึ้น" else -v
            n, right, wrong, flat = n + 1, right + (s >= FC.UP), wrong + (s <= -FC.UP), flat + (abs(v) < FC.UP)
    h = len(y) // 2
    half = lambda sl: np.nanmean([F.auc(yu[sl], pu[sl]), F.auc(yd[sl], pdn[sl])])
    r = {"auc": np.nanmean([F.auc(yu, pu), F.auc(yd, pdn)]), "h1": half(slice(0, h)), "h2": half(slice(h, None)), "brier": float(bi.mean()), "bi": bi,
         "n": n, "right": int(right), "miss": int(wrong + flat)}
    print(f"{name:<24}{F.auc(yu, pu):>7.2f}{F.auc(yd, pdn):>7.2f}{r['h1']:>8.2f}{r['h2']:>8.2f}{r['brier']:>8.3f}{n:>7}{int(right):>6}{int(wrong):>8}{int(flat):>8}")
    return r


def main():
    _, tables = F.load(sys.argv[1])
    e = pd.read_csv(sys.argv[2], parse_dates=["day"])
    mops, wti = mops_daily(json.load(open(sys.argv[3]))), wti_daily(json.load(open(sys.argv[4])))
    wfr = walk_forward(build_dataset(mops, wti, e))
    last = tables.headsup.ts.max().normalize() - pd.Timedelta(days=1)
    for fuel in ("B7", "G95"):
        df = dataset(tables, e, fuel, last)
        pu, pdn, cu, cd = X.new_probs(df)
        old = old_probs(wfr, fuel, list(df.day))
        ok = ~np.isnan(pu) & df.day.isin(list(old)).to_numpy()
        d = df[ok]
        y = d.dret.to_numpy()
        ou, od = np.array([old[t][0] for t in d.day]), np.array([old[t][1] for t in d.day])
        print(f"\n== {fuel} WS พรุ่งนี้ : วันปี 2026 ที่ใช้ได้ {len(df)} ; วันทดสอบร่วม {len(d)} ({d.day.min():%d/%m/%y}–{d.day.max():%d/%m/%y}) ; ขึ้น {(y >= .1).mean():.0%} ลง {(y <= -.1).mean():.0%} นิ่ง {(abs(y) < .1).mean():.0%}")
        print(f"{'วิธี':<24}{'AUCขึ้น':>7}{'AUCลง':>7}{'ครึ่ง1':>8}{'ครึ่ง2':>8}{'Brier':>8}{'ฟันธง':>7}{'ถูก':>6}{'ผิดทิศ':>8}{'นิ่ง':>8}")
        c = score("ความถี่ในอดีต", y, cu[ok], cd[ok])
        o = score("ปัจจุบัน (ridge+กองทุน)", y, ou, od)
        n = score("ใหม่ (สัญญาณ Jobber)", y, pu[ok], pdn[ok])
        m, lo, hi = X.boot(n["bi"], o["bi"])
        print(f"   Brier ใหม่ − ปัจจุบัน = {m:+.3f} (ช่วงเชื่อมั่น 95% {lo:+.3f} ถึง {hi:+.3f})")
        c1 = n["auc"] >= 0.70 and min(n["h1"], n["h2"]) >= 0.60
        c2 = hi < 0 and n["brier"] < c["brier"]
        c3 = n["miss"] <= o["miss"]
        print(f"   เกณฑ์ 1 (AUC) {'ผ่าน' if c1 else 'ไม่ผ่าน'} ; เกณฑ์ 2 (Brier) {'ผ่าน' if c2 else 'ไม่ผ่าน'} ; เกณฑ์ 3 (ฟันธงพลาด {n['miss']} ต่อ {o['miss']}) {'ผ่าน' if c3 else 'ไม่ผ่าน'}"
              f" → {'ใช้โมเดลใหม่' if all((c1, c2, c3)) else 'ยังไม่เปลี่ยน'}")


if __name__ == "__main__":
    main()
