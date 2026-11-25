"""
การทดลอง: ให้โมเดลรอบ 20:30 รู้ "ขนาด" ของการปรับราคาที่ผู้ค้าประกาศล่วงหน้า และไม่นับประกาศ "มีลง" ที่มีผลไปแล้วเป็นสัญญาณลง

ที่มา (25/11/2026) : 3 คืนติด (23–25/11) ผู้ค้าประกาศขึ้นคืนนั้น (ไออาร์ +1.00 , ดีเซล +2.00 ในคืน 25/11) แต่คำทายรอบ 20:30 เอียงลง
   คืน 23/11 : ดีเซล 16:30 วันถัดไป ทาย "ลง" (77%) → จริง +0.39 ; 24 ชม. เอียงลง → จริง ดีเซล +0.39 / G95 +0.37
   สาเหตุที่สงสัย (ดูโค้ด signals.build_features / price_features) :
     1) up_n / dn_n นับแค่ "จำนวนเจ้า" ที่ประกาศขึ้น/ลงวันนี้ ไม่รู้ขนาดเรท (+1.00 ฯลฯ) ;
        pend (ชุด v2) รู้ขนาดเฉพาะเมื่อผู้ค้าโพสต์ "ราคาเต็ม" ล่วงหน้า ซึ่งตอนเย็นมักเป็นแค่เรทโดยประมาณ ("รอแจ้งราคาพรุ่งนี้")
     2) dn_n นับประกาศ "มีลง" ระหว่างวันที่มีผลไปแล้วก่อน 20:30 (ราคาอ้างอิงรวมการลดนั้นแล้ว) เป็นสัญญาณลงของคืนนี้

เทมเพลต (ตั้งก่อนรัน 2026-11-25 ; เจ้าของระบบสั่งให้เริ่มโดยไม่รอผลคืน 24/11)
  ฟีเจอร์ใหม่ (จาก HeadsUp ของวันนั้น 06:00 ถึงเวลาตัดสินใจ , ขอบเขตชนิดเดียวกับ up_n/dn_n) :
     "ค้าง (pending)" = เวลาที่ระบุ >= เวลาตัดสินใจ หรือก่อน 06:00 (หลังเที่ยงคืน) หรือไม่ระบุเวลา
     rate_up  = เรทขึ้นสูงสุดที่ประกาศ (บาท/ลิตร , 0 ถ้าไม่มี , ตัดที่ 3) จากประกาศที่ยังค้าง
     rate_dn  = เรทลงมากสุด (ค่าลบ , 0 ถ้าไม่มี , ตัดที่ −3) จากประกาศที่ยังค้าง
     up_p / dn_p = จำนวนเจ้าที่ประกาศขึ้น/ลงและยังค้าง
  ทางเลือก :
     R  RATE : ชุดเดิม + rate_up , rate_dn
     P  PEND : ชุดเดิมแต่ up_n/dn_n แทนด้วย up_p/dn_p
     RP      : ทั้งสองอย่าง
  ขอบเขต : โมเดลรอบ 20:30 ที่ใช้จริง — B7_H24 (v1s) , G95_H24 (v2) , {B7,G95}_T16 และ _T20 (ราคาเที่ยง / 16:30 วันถัดไป , v2)
     walk-forward เดียวกับของจริง (ฝึกขั้นต่ำ 150 วัน ก้าว 30 วัน) ; ระดับคำทาย H24 ด้วย model.classify_outlook ; T16/T20 ด้วยเกณฑ์ OUTLOOK_MIN_P
  กลุ่มเป้าหมาย : คืนที่มีประกาศขึ้นพร้อมเรท >= 0.30 สำหรับชนิดนั้นและยังค้าง ("คืนประกาศขึ้น")
  ผ่าน (ต่อ key ; ทุกข้อ) :
     1) AUC เฉลี่ย (ขึ้น,ลง) เพิ่ม >= 0.01 และไม่แย่ลงทั้งครึ่งแรกและครึ่งหลัง
     2) ทั้งช่วง : เมื่อฟันธงแล้วถูกไม่ลด และผิดทิศไม่เพิ่มเกิน 1 จุด
     3) คืนประกาศขึ้น : สัดส่วนที่ทาย "ลง" แต่ราคาขึ้นจริง >= 10 สต. ลดลง (หรือเป็น 0 อยู่แล้วและไม่เพิ่ม)
  เลือก : ผ่านหลายทาง → AUC สูงสุด ; ถ้าห่างกัน < 0.005 เลือกทางที่ง่ายกว่า (P < R < RP)
  ข้อควรระวัง : ข้อมูลฝึกถึง 21/10/2026 จึงไม่รวม 23–25/11 ที่เป็นที่มา ; เรทในประกาศเป็นค่าโดยประมาณ (มี has_rate เพียง ~8% ของแถว)
     ทดสอบ 6 key × 3 ทาง = เลือกจากหลายชุด จึงต้องผ่านทุกข้อพร้อมกัน

ผล (รัน 2026-11-25 ; oil-bot/training_data ถึง 21/10/2026 ; ~645 วันทดสอบต่อ key) : ไม่ผ่านทุก key ทุกทาง → คงโมเดลเดิม
   AUC เดิม → P / R / RP : B7_H24 0.793 → 0.788/0.790/0.786 ; G95_H24 0.771 → 0.769/0.772/0.770
                          B7_T16 0.791 → 0.788/0.786/0.784 ; B7_T20 0.819 → 0.817/0.814/0.812
                          G95_T16 0.758 → 0.754/0.761/0.758 ; G95_T20 0.780 → 0.776/0.782/0.779
   ไม่มีทางใดเพิ่ม AUC ถึง 0.01 ; R ของ G95 ดีขึ้นเล็กน้อย (+0.001–0.003) แต่ไม่ถึงเกณฑ์
   คืนประกาศขึ้น (เรท >= 0.30 , ~230 คืนต่อชนิด) ในอดีต : 24 ชม. ต่อมา ดีเซลขึ้นจริง 44% ลงจริง 34% , G95 42% / 29%
     → การประกาศขึ้นคืนนั้นไม่ได้แปลว่าราคา 20:30 วันถัดไปจะสูงกว่า (ผู้ค้ามักลดกลับระหว่างวัน) โมเดลเดิมรับรู้ผ่านตัวแปรอื่นแล้ว
     และ "ทายลงแต่ขึ้นจริง" ในคืนเหล่านี้เกิดเพียง 2–6% ของวัน
   ข้อมูลเพิ่มเติม (ดูหลังรัน ไม่ใช่เกณฑ์ที่ตั้งไว้ก่อน) — คำทาย "16:30 วันถัดไป ลง" (T20) แยกตามเรทขึ้นที่ประกาศ :
     B7  ไม่มีเรทขึ้น 115 ครั้ง ลงจริง 73% ขึ้นจริง 4% เฉลี่ย −0.39 ; เรท >= 0.30 70 ครั้ง 63% / 20% / −0.24 ; เรท >= 1.00 45 ครั้ง 58% / 22% / −0.22
     G95 ไม่มีเรทขึ้น 104 ครั้ง 61% / 5% / −0.19 ; เรท >= 0.30 55 ครั้ง 73% / 13% / −0.31 ; เรท >= 1.00 26 ครั้ง 65% / 15% / −0.41
     → ดีเซลในคืนที่ประกาศขึ้น คำแนะนำ "รอบ่าย" เสี่ยงขึ้นชัด (ผิดทิศ 4% → 20–22% ต่ำกว่าเกณฑ์ PROFILE_MAX_WRONG 12%) แต่เฉลี่ยยังประหยัดได้ ~0.22 บาท/ลิตร
   ยังไม่เปลี่ยนระบบ ; ทางเลือกที่เสนอเจ้าของระบบ : แสดงความเสี่ยงนี้ในคำแนะนำ / ฝึกใหม่ด้วยข้อมูลถึงปัจจุบัน / เฝ้าดูผลจริงต่อ

ใช้: python exp_headsup_rate.py <data_dir ของ jobber>   (เช่น ../oil-bot/training_data)
"""
import argparse
import warnings

import numpy as np
import pandas as pd

import fit_model as F
import model as M
import signals as S

warnings.filterwarnings("ignore")
KEYS = (("B7_H24", "B7", 24, "v1s"), ("G95_H24", "G95", 24, "v2"),
        ("B7_T16", "B7", 16, "v2p"), ("B7_T20", "B7", 20, "v2p"), ("G95_T16", "G95", 16, "v2p"), ("G95_T20", "G95", 20, "v2p"))
RATE_TAG = {"B7": ("DIESEL", "ALL"), "G95": ("GASOHOL", "ALL")}
TARGET_RATE = 0.30
VARIANTS = ("P", "R", "RP")


def _hm(s):
    s = str(s or "").strip()
    if not s or s.lower() == "nan" or ":" not in s:
        return None
    h, m = s.split(":")[:2]
    try:
        return int(h) * 60 + int(m)
    except ValueError:
        return None


def hu_features(t: S.Tables, asof: pd.Timestamp, fuel: str) -> dict:
    """rate_up / rate_dn / up_p / dn_p จากประกาศของวันนั้นที่ยังไม่มีผล ณ asof"""
    out = {"rate_up": 0.0, "rate_dn": 0.0, "up_p": 0, "dn_p": 0}
    h = t.headsup
    if h.empty:
        return out
    day = asof.normalize()
    w = h[(h.ts >= day + pd.Timedelta(hours=S.HEADSUP_FROM_HOUR)) & (h.ts <= asof) & h.scope.isin(S.HEADS_SCOPES[fuel])]
    if w.empty:
        return out
    now = asof.hour * 60 + asof.minute
    pend = w.stated_time.map(lambda s: _hm(s) is None or _hm(s) >= now or _hm(s) < 6 * 60)
    w = w[pend.astype(bool).to_numpy()]
    named = w[w.provider.astype(str).str.strip().ne("") & w.provider.notna()]
    out["up_p"] = int(named[named.direction == "UP"].provider.nunique())
    out["dn_p"] = int(named[named.direction == "DOWN"].provider.nunique())
    for rates in w.rates.dropna().astype(str):
        for tok in rates.split():
            k, _, v = tok.partition(":")
            if k not in RATE_TAG[fuel]:
                continue
            try:
                x = float(v.split("!")[0])
            except ValueError:
                continue
            out["rate_up"] = max(out["rate_up"], min(x, 3.0))
            out["rate_dn"] = min(out["rate_dn"], max(x, -3.0))
    return out


def base_matrix(df: pd.DataFrame, kind: str) -> np.ndarray:
    recs = df.to_dict("records")
    vec = {"v1s": M.vectorize_stale, "v2": M.vectorize_v2, "v2p": M.vectorize_v2}[kind]
    return np.vstack([vec(r) for r in recs])


def variant_matrix(X: np.ndarray, hu: pd.DataFrame, kind: str, var: str) -> np.ndarray:
    X = X.copy()
    if "P" in var:                                   # up_n/dn_n อยู่ตำแหน่งเดียวกันทั้ง v1s และ v2 (FEATURES 8 ตัวแรก)
        X[:, M.FEATURES.index("up_n")] = np.clip(hu.up_p.to_numpy(), *M.CLIPS.get("up_n", (0, 8)))
        X[:, M.FEATURES.index("dn_n")] = np.clip(hu.dn_p.to_numpy(), *M.CLIPS.get("dn_n", (0, 8)))
    if "R" in var:
        X = np.column_stack([X, hu.rate_up.to_numpy(), hu.rate_dn.to_numpy()])
    return X


def wf(X, y, h24):
    """walk-forward : (ระดับ/คำทาย, p ขึ้น10, p ลง10) ; H24 = 4 โมเดล + classify_outlook ; ช่วงเวลา = 2 โมเดล + OUTLOOK_MIN_P"""
    yu, yd = (y >= M.OUTLOOK_SMALL).astype(float), (y <= -M.OUTLOOK_SMALL).astype(float)
    pr, pf = F._wf_proba(X, yu), F._wf_proba(X, yd)
    if h24:
        p40r, p40f = F._wf_proba(X, (y >= M.OUTLOOK_BIG).astype(float)), F._wf_proba(X, (y <= -M.OUTLOOK_BIG).astype(float))
        calls = np.array(["" if np.isnan(a) else M.classify_outlook(a, b, c, d) for a, b, c, d in zip(pr, pf, p40r, p40f)], dtype=object)
    else:
        calls = np.array(["" if np.isnan(a) else "UP" if (a >= M.OUTLOOK_MIN_P and a > b) else "DOWN" if (b >= M.OUTLOOK_MIN_P and b > a)
                          else "UNCLEAR" for a, b in zip(pr, pf)], dtype=object)
    return calls, pr, pf


def stats(y, calls, mask):
    ok = mask & (calls != "")
    c = ok & (calls != "UNCLEAR")
    up = np.char.startswith(calls.astype(str), "UP")
    s = np.where(up, y, -y)
    n = int(c.sum())
    dn_call = ok & np.char.startswith(calls.astype(str), "DOWN")
    return {"days": int(ok.sum()), "n": n, "right": float((s[c] >= M.OUTLOOK_SMALL).mean()) if n else np.nan,
            "wrong": float((s[c] <= -M.OUTLOOK_SMALL).mean()) if n else np.nan,
            "down_but_up": float((dn_call & (y >= M.OUTLOOK_SMALL)).sum() / max(int(ok.sum()), 1))}


def auc3(y, pr, pf):
    ok = ~np.isnan(pr)
    yu, yd = (y[ok] >= M.OUTLOOK_SMALL).astype(float), (y[ok] <= -M.OUTLOOK_SMALL).astype(float)
    a, b, h = pr[ok], pf[ok], int(ok.sum()) // 2
    f = lambda sl: float(np.nanmean([F.auc(yu[sl], a[sl]), F.auc(yd[sl], b[sl])]))
    return f(slice(None)), f(slice(0, h)), f(slice(h, None))


def fmt(s):
    if not s["n"]:
        return f"{s['days']:>4} วัน ฟันธง   0 ; ทายลงแต่ขึ้นจริง {s['down_but_up']:.1%}"
    return (f"{s['days']:>4} วัน ฟันธง {s['n']:>3} ถูก {s['right']:.0%} ผิดทิศ {s['wrong']:.0%} ; "
            f"ทายลงแต่ขึ้นจริง {s['down_but_up']:.1%}")


def run_key(book, tables, key, fuel, hz, kind):
    df = F.build_outlook_dataset(book, tables, fuel, hz, M.OUTLOOK_REF_HOUR, v2=(kind != "v1s"))
    asofs = [pd.Timestamp(d) + pd.Timedelta(hours=M.OUTLOOK_REF_HOUR) for d in df.date]
    hu = pd.DataFrame([hu_features(tables, a, fuel) for a in asofs])
    y = df.y.to_numpy()
    X0 = base_matrix(df, kind)
    target = (hu.rate_up.to_numpy() >= TARGET_RATE)
    allm = np.ones(len(y), bool)
    h24 = hz == 24
    c0, pr0, pf0 = wf(X0, y, h24)
    a0 = auc3(y, pr0, pf0)
    s0, t0 = stats(y, c0, allm), stats(y, c0, target)
    print(f"\n== {key} ({kind}) : {len(y)} วัน ; คืนประกาศขึ้น (เรท >= {TARGET_RATE:.2f}) {int(target.sum())} วัน ; "
          f"ในคืนเหล่านี้ราคาขึ้นจริง {np.mean(y[target] >= M.OUTLOOK_SMALL):.0%} ลงจริง {np.mean(y[target] <= -M.OUTLOOK_SMALL):.0%}")
    print(f"   เดิม : AUC {a0[0]:.3f} (ครึ่ง {a0[1]:.3f}/{a0[2]:.3f})")
    print(f"          ทั้งช่วง {fmt(s0)}")
    print(f"          คืนประกาศขึ้น {fmt(t0)}")
    res = {"key": key, "base": a0[0], "pass": {}}
    for var in VARIANTS:
        c1, pr1, pf1 = wf(variant_matrix(X0, hu, kind, var), y, h24)
        a1 = auc3(y, pr1, pf1)
        s1, t1 = stats(y, c1, allm), stats(y, c1, target)
        ok1 = a1[0] - a0[0] >= 0.01 and a1[1] >= a0[1] and a1[2] >= a0[2]
        ok2 = (not s1["n"] or not s0["n"]) or (s1["right"] >= s0["right"] and s1["wrong"] <= s0["wrong"] + 0.01)
        ok3 = t1["down_but_up"] < t0["down_but_up"] or (t0["down_but_up"] == 0 and t1["down_but_up"] == 0)
        ok = ok1 and ok2 and ok3
        res["pass"][var] = (ok, a1[0])
        print(f"   {var:<3}: AUC {a1[0]:.3f} (ครึ่ง {a1[1]:.3f}/{a1[2]:.3f}) [{'✓' if ok1 else '✗'}]")
        print(f"          ทั้งช่วง {fmt(s1)} [{'✓' if ok2 else '✗'}]")
        print(f"          คืนประกาศขึ้น {fmt(t1)} [{'✓' if ok3 else '✗'}]  → {'ผ่าน' if ok else 'ไม่ผ่าน'}")
    return res


def choose(res):
    passed = [(v, a) for v, (ok, a) in res["pass"].items() if ok]
    if not passed:
        return "ไม่ผ่าน → คงเดิม"
    best = max(a for _, a in passed)
    for v in VARIANTS:                                # ง่ายก่อน : P < R < RP
        if any(v == p and best - a < 0.005 for p, a in passed):
            return f"ใช้ {v}"
    return "ไม่ผ่าน → คงเดิม"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data_dir")
    a = ap.parse_args()
    prices, tables = F.load(a.data_dir)
    book = S.PriceBook(prices)
    h = tables.headsup
    print(f"HeadsUp {len(h)} แถว ({h.ts.min():%Y-%m-%d} ถึง {h.ts.max():%Y-%m-%d}) ; มีเรท {int((h.has_rate == 'Y').sum())} แถว")
    out = [run_key(book, tables, *k) for k in KEYS]
    print("\n== สรุป")
    for r in out:
        print(f"   {r['key']:<8} เดิม AUC {r['base']:.3f} ; " + " ".join(f"{v} {a:.3f}{'✓' if ok else ''}" for v, (ok, a) in r["pass"].items())
              + f" → {choose(r)}")


if __name__ == "__main__":
    main()
