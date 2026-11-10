"""
S2 ขั้นแรก (อ่านอย่างเดียว) : ตรวจ PriceLog ของ OIL BOT — (1) ส่วนลดของต้องบุญ (ราคาประกาศ − ราคาที่เก็บ) ต่อกลุ่มผู้ขายและช่วงเวลา
(2) ส่วนต่างราคาระหว่างต้องบุญกับศรีพลังสำหรับคลัง/ชนิด/เวลามีผลเดียวกัน (3) เวลาที่ราคาเปลี่ยนจริง (ขึ้น/ลง ขนาดใหญ่) ตามช่วงเวลาของวัน

ใช้: python diag_pricelog.py   (ต้องมี SA_EMAIL / SA_PRIVATE_KEY ; อ่านชีท OIL BOT ที่ ingest.py ใช้อยู่แล้ว ไม่เขียนอะไร)
"""
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from common import OILBOT_SHEET_ID, get_token, sheets_get
from pricelog_frame import (build_frame, effective_old, effective_parts, effective_ts, first_line, provider_group, stated,  # noqa: F401  (re-export สำหรับเทสต์/สคริปต์เดิม)
                            to_num)

TBP, SPL = "ต้องบุญ", "ศรีพลัง"
BIG = 0.30                                   # การเปลี่ยนราคา ≥ 30 สต. นับว่า "ใหญ่"
BUCKETS = [("22:00-23:29", 22 * 60, 23 * 60 + 30), ("23:30-23:59", 23 * 60 + 30, 24 * 60), ("00:00-00:59", 0, 60), ("01:00-04:59", 60, 5 * 60),
           ("05:00-07:59", 5 * 60, 8 * 60), ("08:00-08:59", 8 * 60, 9 * 60), ("09:00-12:59", 9 * 60, 13 * 60),
           ("13:00-17:59", 13 * 60, 18 * 60), ("18:00-21:59", 18 * 60, 22 * 60)]


def discount_report(df: pd.DataFrame, jobbers=(TBP, SPL)) -> list:
    """ส่วนลด = ราคาประกาศ − ราคาที่เก็บ ต่อจ๊อบเบอร์ × กลุ่มผู้ขาย : ค่าที่พบ (จำนวน, วันแรก–วันสุดท้าย)"""
    out = []
    for jb in jobbers:
        sub = df[df.jobber == jb]
        out.append(f"[PRICELOG] {jb}: {len(sub)} แถว ; ไม่มี announced_price {int(sub.announced.isna().sum())} แถว")
        for grp, g in sub.groupby("group"):
            g = g.dropna(subset=["announced"])
            if g.empty:
                continue
            parts = []
            for val, x in sorted(g.groupby("discount"), key=lambda kv: -len(kv[1]))[:4]:
                parts.append(f"{val:.2f}: {len(x)} ({len(x) / len(g):.0%}, {x.date.min()}→{x.date.max()})")
            out.append(f"   {grp:<7} ลด " + " | ".join(parts))
    return out


def jobber_gap_report(df: pd.DataFrame) -> list:
    """ราคาที่เก็บของศรีพลัง − ต้องบุญ ที่ คลัง+ชนิด+เวลามีผล เดียวกัน : ต่อกลุ่มผู้ขาย"""
    d = df.drop_duplicates(["jobber", "provider", "warehouse", "fuel", "effective"], keep="last")
    a, b = d[d.jobber == TBP], d[d.jobber == SPL]
    m = a.merge(b, on=["provider", "warehouse", "fuel", "effective"], suffixes=("_t", "_s"))
    out = [f"[PRICELOG] คู่ราคาต้องบุญ/ศรีพลัง (คลัง+ชนิด+เวลามีผลเดียวกัน) = {len(m)} คู่ ; ส่วนต่าง = ศรีพลัง − ต้องบุญ (ราคาที่เก็บ) และ − ราคาประกาศของต้องบุญ"]
    if m.empty:
        return out
    m["gap"] = (m.price_s - m.price_t).round(2)
    m["gap_ann"] = (m.price_s - m.announced_t).round(2)
    for grp, g in m.groupby("group_t"):
        out.append(f"   {grp:<7} n={len(g):>4} | ศรีพลังแพงกว่า(เก็บ) เฉลี่ย {g.gap.mean():+.3f} ; ต้องบุญถูกกว่า {float((g.gap > 0.004).mean()):.0%} เท่ากัน {float((g.gap.abs() < 0.005).mean()):.0%} "
                   f"ศรีพลังถูกกว่า {float((g.gap < -0.004).mean()):.0%} | เทียบประกาศต้องบุญ เฉลี่ย {g.gap_ann.mean():+.3f}")
    return out


def change_timing_report(df: pd.DataFrame, jobber: str = TBP, explicit=None) -> list:
    """เวลามีผลของการเปลี่ยนราคา (ราคาที่เก็บของต้องบุญ) ต่อช่วงเวลาของวัน : ขึ้น/ลงทั้งหมด และที่ใหญ่ (≥ BIG)
    explicit=True นับเฉพาะแถวที่ระบุ effective_time จริง ; False = แถวที่ไม่ระบุ (ใช้เวลาโพสต์แทน) ; None = ทั้งหมด"""
    d = df[df.jobber == jobber].drop_duplicates(["provider", "warehouse", "fuel", "effective"], keep="last").sort_values("effective")
    d = d.assign(delta=d.groupby(["provider", "warehouse", "fuel"]).price.diff()).dropna(subset=["delta"])
    d = d[d.delta.abs() > 0.004]
    if explicit is not None:
        d = d[d.has_eff == explicit]
    d["minute"] = d.effective.dt.hour * 60 + d.effective.dt.minute
    label = {None: "ทั้งหมด", True: "ระบุ effective_time จริง", False: "ไม่ระบุ (ใช้เวลาโพสต์แทน)"}[explicit]
    out = [f"[PRICELOG] เวลาของการเปลี่ยนราคา ({jobber}, {label}, {len(d)} ครั้ง ; ใหญ่ = ≥{BIG:.2f} บาท) — ช่วงเวลา → ขึ้น/ลง (ใหญ่: ขึ้น/ลง)"]
    for grp, g in d.groupby("group"):
        if len(g) < 10:
            continue
        cells = []
        for name, lo, hi in BUCKETS:
            x = g[(g.minute >= lo) & (g.minute < hi)]
            up, dn = int((x.delta > 0).sum()), int((x.delta < 0).sum())
            bu, bd = int((x.delta >= BIG).sum()), int((x.delta <= -BIG).sum())
            cells.append(f"{name} {up}/{dn} ({bu}/{bd})")
        out.append(f"   {grp:<7} n={len(g):>5} | " + " ; ".join(cells))
    return out


def coverage_report(df: pd.DataFrame) -> list:
    """ต่อกลุ่มผู้ขาย : สัดส่วนแถวที่ระบุ effective_time จริง / ข้อความที่บรรทัดแรกระบุเวลา / พูดว่าเที่ยงคืน (นับต่อข้อความ ไม่ซ้ำ)"""
    out = ["[PRICELOG] ความครบของเวลามีผล ต่อกลุ่มผู้ขาย : แถวที่ระบุ effective_time | ข้อความที่บรรทัดแรกระบุเวลา | ข้อความที่บอก 'เที่ยงคืน'"]
    for grp, g in df.groupby("group"):
        m = g.drop_duplicates("msg")
        out.append(f"   {grp:<7} แถว={len(g):>5} มี effective_time {float(g.has_eff.mean()):.0%} | ข้อความ={len(m):>4} ระบุเวลา {float(m.stated.str.contains(':').mean()):.0%} "
                   f"เที่ยงคืน {float((m.stated == 'เที่ยงคืน').mean()):.0%}")
    return out


def announce_time_report(df: pd.DataFrame, top: int = 10) -> list:
    """เวลาที่ประกาศระบุไว้ในบรรทัดแรกของข้อความ (ทิศ + เวลา) ต่อจ๊อบเบอร์ นับต่อข้อความ — เห็นว่าการปรับตอนกลางคืนประกาศไว้กี่โมง"""
    out = []
    m = df.drop_duplicates("msg")
    m = m[m.stated != ""]
    for jb in (TBP, SPL):
        g = m[m.jobber == jb]
        top_items = g.groupby(["dir", "stated"]).size().sort_values(ascending=False).head(top)
        out.append(f"[PRICELOG] เวลาที่ประกาศระบุ ({jb}, {len(g)} ข้อความ) ทิศ+เวลา:จำนวน : " + "  ".join(f"{d}{t}:{n}" for (d, t), n in top_items.items()))
        night = g[g.stated.isin(["เที่ยงคืน"]) | g.stated.str.match(r"^(2[2-3]|0[0-6]):")]
        by = night.groupby(["dir", "stated"]).size().sort_values(ascending=False).head(top)
        out.append(f"   กลางคืน (22:00-06:59 หรือ เที่ยงคืน) {len(night)} ข้อความ : " + "  ".join(f"{d}{t}:{n}" for (d, t), n in by.items()))
    return out


def _hist(x: pd.DataFrame, col: str = "time") -> str:
    parts = []
    for name, lo, hi in BUCKETS:
        n = int(((x[col].dt.hour * 60 + x[col].dt.minute >= lo) & (x[col].dt.hour * 60 + x[col].dt.minute < hi)).sum())
        if n:
            parts.append(f"{name}:{n}")
    return ", ".join(parts) or "-"


def episodes(df: pd.DataFrame, sign: int = 1, gap_h: float = 14) -> pd.DataFrame:
    """เหตุการณ์เปลี่ยนราคาใหญ่ (ขึ้น sign=+1 / ลง sign=-1, ≥ BIG) รวมสองจ๊อบเบอร์ : 1 เหตุการณ์ต่อ (ผู้ขาย, คลัง, ชนิด) ที่เกิดห่างกันไม่เกิน gap_h ชม.
    เวลา = เวลามีผลที่ระบุเร็วที่สุดจากจ๊อบเบอร์ใดก็ได้ ถ้าไม่มีเลยใช้เวลาที่เห็นราคาใหม่ครั้งแรก (explicit=False)"""
    d = df.drop_duplicates(["jobber", "provider", "warehouse", "fuel", "effective"], keep="last").sort_values("effective")
    d = d.assign(delta=d.groupby(["jobber", "provider", "warehouse", "fuel"]).price.diff()).dropna(subset=["delta"])
    d = d[d.delta * sign >= BIG].sort_values(["provider", "warehouse", "fuel", "effective"])
    if d.empty:
        return pd.DataFrame(columns=["group", "time", "explicit", "size"])
    key = ["provider", "warehouse", "fuel"]
    step = d.groupby(key).effective.diff()
    d["ep"] = ((step > pd.Timedelta(hours=gap_h)) | step.isna()).cumsum()
    rows = []
    for _, g in d.groupby("ep"):
        ex = g[g.has_eff]
        rows.append((g.group.iloc[0], ex.effective.min() if len(ex) else g.effective.min(), bool(len(ex)), float(g.delta.abs().max())))
    return pd.DataFrame(rows, columns=["group", "time", "explicit", "size"])


def episode_timing_report(df: pd.DataFrame) -> list:
    out = []
    for sign, name in ((1, "ขึ้นใหญ่"), (-1, "ลงใหญ่")):
        e = episodes(df, sign)
        out.append(f"[PRICELOG] {name} ≥{BIG:.2f} บาท รวมสองจ๊อบเบอร์ ({len(e)} เหตุการณ์; 1 เหตุการณ์ = 1 คลัง+ชนิด) — ระบุเวลามีผลจริง: ช่วงเวลา:จำนวน | ไม่ระบุเลย: ช่วงที่เห็นราคาใหม่ครั้งแรก | เวลาที่ระบุบ่อย")
        for grp, g in e.groupby("group"):
            if len(g) < 5:
                continue
            ex, un = g[g.explicit], g[~g.explicit]
            exact = ex.time.dt.strftime("%H:%M").value_counts().head(6)
            out.append(f"   {grp:<7} n={len(g):>4} ระบุ {len(ex):>3} ({len(ex) / len(g):.0%}) : {_hist(ex)} | ไม่ระบุ {len(un):>3}: {_hist(un)} | " + " ".join(f"{k}:{v}" for k, v in exact.items()))
    return out


def shift_report(df: pd.DataFrame) -> list:
    """กติกาวันของ effective_time : เทียบกติกาเดิม (เลื่อนเป็นวันถัดไปเสมอเมื่อเวลาที่ระบุน้อยกว่าเวลาโพสต์เกิน 2 ชม.) กับกติกาปัจจุบัน
    (เลื่อนเฉพาะที่โพสต์ตั้งแต่ 12:00) — นับรายการที่เวลามีผลเปลี่ยน ต่อกลุ่มผู้ขาย พร้อมชุด 'ชั่วโมงโพสต์→เวลาที่ระบุ'"""
    e = df[df.has_eff].drop_duplicates(["msg", "provider", "warehouse", "fuel", "effective"])
    changed = e[e.effective != e.eff_old]
    out = [f"[PRICELOG] ราคาที่ระบุ effective_time {len(e)} รายการ : เลื่อนเป็นวันถัดไปตามกติกาเดิม {int((e.eff_old.dt.date > e.posted.dt.date).sum())} ; "
           f"ตามกติกาปัจจุบัน {int(e.shifted.sum())} ; เวลามีผลต่างกันระหว่างกติกา {len(changed)} รายการ ({len(changed) / max(len(e), 1):.1%})"]
    for grp, g in changed.groupby("group"):
        combos = (g.posted.dt.strftime("%H") + "น.→" + g.effective.dt.strftime("%H:%M")).value_counts().head(5)
        out.append(f"   {grp:<7} {len(g):>4} รายการ ย้ายจากวันถัดไปมาวันเดียวกัน โพสต์→เวลาที่ระบุ: " + " ".join(f"{k}:{v}" for k, v in combos.items()))
    return out


def rule_impact_report(df: pd.DataFrame) -> list:
    """ราคาต่ำสุดของตลาด (ทุกผู้ขาย) ณ จุดเวลาต่าง ๆ ต่างกันหรือไม่เมื่อใช้กติกาเดิมเทียบกับกติกาปัจจุบัน — ยืนยันว่าจุดวัด 20:30/15:30 ไม่เปลี่ยน"""
    import diag_price_min as M

    def frame(col):
        p = df[["provider", "warehouse", "fuel", "price"]].copy()
        p["effective"] = df[col]
        p["key"] = p.provider + "|" + p.warehouse
        p["_ord"] = range(len(p))
        return p
    old_p, new_p = frame("eff_old"), frame("effective")
    days = pd.date_range(df.effective.min().normalize() + pd.Timedelta(days=4), df.effective.max().normalize() - pd.Timedelta(days=3), freq="D")
    spots = {"20:30": 20.5, "15:30": 15.5, "20:30+24ชม.": 44.5, "20:30+48ชม.": 68.5, "02:00": 2.0, "05:00": 5.0, "07:00": 7.0, "09:00": 9.0, "16:30": 16.5}
    out = [f"[PRICELOG] ราคาต่ำสุดของตลาด เทียบกติกาเดิม/ปัจจุบัน ({len(days)} วัน × G95, B7) : จุดเวลา → จำนวนที่ต่างกัน (≥0.01 บาท) / ทั้งหมด ; ส่วนต่างเฉลี่ยที่ต่าง"]
    for name, hr in spots.items():
        diffs, n = [], 0
        for fuel in ("G95", "B7"):
            t = pd.DatetimeIndex([d + pd.Timedelta(hours=hr) for d in days])
            a, b = M.quote_table(old_p, fuel, t).price.to_numpy(), M.quote_table(new_p, fuel, t).price.to_numpy()
            ok = ~(np.isnan(a) | np.isnan(b))
            n += int(ok.sum())
            diffs += list((b[ok] - a[ok])[np.abs(b[ok] - a[ok]) >= 0.01])
        out.append(f"   {name:<12} {len(diffs):>3}/{n}" + (f" ; เฉลี่ย {float(np.mean(diffs)):+.2f} (ช่วง {min(diffs):+.2f}..{max(diffs):+.2f})" if diffs else ""))
    return out


def main():
    tok = get_token()
    values = sheets_get(tok, OILBOT_SHEET_ID, "PriceLog!A1:Q200000")
    df = build_frame(values)
    print(f"[PRICELOG] rows={len(df)} {df.date.min()}→{df.date.max()} jobber: " + ", ".join(f"{k}={v}" for k, v in df.jobber.value_counts().head(6).items()))
    for line in (discount_report(df) + jobber_gap_report(df) + coverage_report(df) + announce_time_report(df)
                 + episode_timing_report(df) + shift_report(df) + rule_impact_report(df)):
        print(line)


if __name__ == "__main__":
    main()
