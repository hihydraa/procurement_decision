"""
หน้าสรุปสำหรับทีมจัดซื้อ — 4 ส่วนเท่านั้น: ผลพยากรณ์ · คำแนะนำ · Backtest · ความแม่นยำจริง

ใช้ข้อมูลที่ main.py มีอยู่แล้ว (แนวโน้ม 24/48 ชม. จาก engine, backtest_summary.json, OutlookLog) และการ์ดพยากรณ์จากแท็บ ForecastCardLog
ฟังก์ชันทั้งหมดเป็น pure function (ไม่อ่านชีทเอง) เพื่อทดสอบได้ ; main.py เป็นผู้โหลดข้อมูลแล้วส่งเข้ามา

หลักการ: ไม่ตกแต่งตัวเลข — ถ้าข้อมูลยังไม่พอให้บอกว่ายังไม่พอ ; การ์ดที่ผลทราบแล้วก่อนออก (ย้อนหลัง) ไม่ถูกนับเป็นผลทดสอบเดินหน้า
"""
from __future__ import annotations

from html import escape

import pandas as pd

FUELS = [("DIESEL", "B7", "ดีเซล"), ("GASOHOL95", "G95", "แก๊สโซฮอล์ 95")]
TIER_TH = {"UP_STRONG": "ขึ้นแรง", "UP": "ขึ้น", "UNCLEAR": "ยังไม่ชัดเจน", "DOWN": "ลง", "DOWN_STRONG": "ลงแรง"}
MIN_LIVE_DAYS = 30          # ต่ำกว่านี้ถือว่ายังน้อยเกินไปจะสรุปความแม่นยำ


def _e(x) -> str:
    return escape(str(x))


# ---------- การ์ดพยากรณ์ (ForecastCardLog) ----------
def _parse_dt(v):
    s = str(v).strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return pd.to_datetime(s, format=fmt)
        except (ValueError, TypeError):
            pass
    try:
        return pd.to_datetime(s, dayfirst=True)
    except (ValueError, TypeError):
        return pd.NaT


def prep_cards(df: pd.DataFrame) -> pd.DataFrame:
    """ตารางการ์ดที่ใช้ได้ : เพิ่ม asof_dt, target_dt, valid_forward (ออกก่อนวันเป้าหมายเริ่ม = พยากรณ์ล่วงหน้าจริง)"""
    need = {"card_id", "asof", "fuel", "target_day"}
    if df is None or df.empty or not need.issubset(df.columns):
        return pd.DataFrame()
    d = df.copy()
    d["asof_dt"] = d["asof"].map(_parse_dt)
    d["target_dt"] = d["target_day"].map(_parse_dt)
    d = d.dropna(subset=["asof_dt", "target_dt"])
    d["valid_forward"] = d["asof_dt"].dt.normalize() < d["target_dt"].dt.normalize()
    return d.sort_values("asof_dt").reset_index(drop=True)


def latest_card(cards: pd.DataFrame, fuel: str):
    """การ์ดล่าสุดที่เป็นการพยากรณ์ล่วงหน้าจริงของเชื้อเพลิงนี้ (None ถ้าไม่มี)"""
    if cards is None or cards.empty:
        return None
    c = cards[(cards["fuel"] == fuel) & cards["valid_forward"]]
    return None if c.empty else c.iloc[-1].to_dict()


def _num(v):
    try:
        f = float(v)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def card_live_stats(cards: pd.DataFrame, fuel: str) -> dict:
    """สถิติสดของการ์ดที่เป็นพยากรณ์ล่วงหน้าและมีผลจริงแล้ว"""
    out = {"n_cards": 0, "n_called": 0, "n_right": 0, "n_hind": 0}
    if cards is None or cards.empty:
        return out
    c = cards[cards["fuel"] == fuel]
    out["n_hind"] = int((~c["valid_forward"]).sum())
    c = c[c["valid_forward"] & (c.get("outcome_cls", pd.Series("", index=c.index)).astype(str).str.strip() != "")]
    out["n_cards"] = int(len(c))
    called = c[c["call"].isin(["ลง", "ขึ้น"])]
    out["n_called"] = int(len(called))
    out["n_right"] = int((called["call"] == called["outcome_cls"]).sum())
    return out


# ---------- ส่วนที่ 1-2: ผลพยากรณ์ + คำแนะนำ ----------
def outlook_direction(tier: str):
    if tier.startswith("UP"):
        return "ขึ้น"
    if tier.startswith("DOWN"):
        return "ลง"
    return None


def combine(tier24: str | None, grade: str | None, card: dict | None) -> dict:
    """รวมสัญญาณ 2 แหล่ง (แนวโน้มตลาด 24 ชม. + การ์ดต้นทุน) เป็นคำแนะนำเดียว ; ไม่ตรงกัน = ไม่ฟันธง"""
    d_o = outlook_direction(tier24) if tier24 else None
    c_call = (card or {}).get("call")
    d_c = c_call if c_call in ("ลง", "ขึ้น") else None
    strong = bool(tier24 and tier24.endswith("STRONG"))
    if d_o and d_c and d_o != d_c:
        return {"dir": None, "action": "ยังไม่ฟันธง: สัญญาณสองแหล่งขัดกัน", "level": "ไม่ชัด",
                "why": f"แนวโน้มตลาดชี้ {d_o} แต่การ์ดต้นทุนชี้ {d_c}"}
    d = d_o or d_c
    if d is None:
        return {"dir": None, "action": "สั่งซื้อตามแผนปกติ ไม่ต้องเร่งหรือรอ", "level": "ไม่ชัด",
                "why": "ทั้งสองแหล่งไม่ให้สัญญาณชัด (ย้อนหลังระบบทายทรงตัวไม่แม่น จึงไม่ฟันธง)"}
    both = d_o == d_c
    level = "สูง" if both else ("กลาง" if (strong or (card or {}).get("confidence") == "กลาง") else "ต่ำ")
    srcs = "สัญญาณ 2 แหล่งตรงกัน" if both else ("จากแนวโน้มตลาดอย่างเดียว" if d_o else "จากการ์ดต้นทุนอย่างเดียว")
    if d == "ลง":
        timing = (card or {}).get("recommendation", "")
        action = "ชะลอการสั่งซื้อ รอราคาลง" if "ชะลอ" not in timing else timing.split(" ข้อควรระวัง")[0]
    else:
        action = "เร่งซื้อก่อนราคาขึ้น" if strong else "ทยอยซื้อก่อน ราคามีแนวโน้มขึ้น"
    return {"dir": d, "action": action, "level": level, "why": srcs}


def _forecast_rows(outlook: dict, cards: pd.DataFrame, asof) -> str:
    out = []
    for fk, code, label in FUELS:
        d = (outlook or {}).get(fk) or {}
        hz = {h["hours"]: h for h in d.get("horizons", [])}
        def cell(h):
            if not h:
                return "<span class='muted'>ยังไม่มีข้อมูล</span>"
            g = "" if h["tier"] == "UNCLEAR" else f" <span class='muted'>(ความเชื่อมั่น {_e(h['grade'])})</span>"
            return f"<strong>{TIER_TH[h['tier']]}</strong>{g}"
        c = latest_card(cards, code)
        if c:
            lo, hi = _num(c.get("lo")), _num(c.get("hi"))
            rng = f"{lo:+.2f} ถึง {hi:+.2f} บาท/ลิตร" if lo is not None and hi is not None else "-"
            pd_, pf, pu = (_num(c.get(k)) for k in ("p_down", "p_flat", "p_up"))
            prob = (f"ลง {pd_:.0%} / นิ่ง {pf:.0%} / ขึ้น {pu:.0%}" if None not in (pd_, pf, pu) else "-")
            tgt = pd.to_datetime(c["target_dt"]).strftime("%d/%m")
            card_txt = (f"<strong>{_e(c.get('call', '-'))}</strong> ช่วง {rng}<div class='muted'>{prob} · สำหรับ {tgt} "
                        f"· ออกเมื่อ {pd.to_datetime(c['asof_dt']).strftime('%d/%m %H:%M')}</div>")
            timing = f"<div class='muted'>เวลาที่มักปรับ: {_e(c.get('timing', '-'))}</div>" if c.get("timing") else ""
        else:
            card_txt, timing = "<span class='muted'>ยังไม่มีการ์ดพยากรณ์ล่วงหน้า (ออกรอบ 21:30)</span>", ""
        out.append(f"<tr><td><strong>{label}</strong></td><td>{cell(hz.get(24))}</td><td>{cell(hz.get(48))}</td><td>{card_txt}{timing}</td></tr>")
    return "".join(out)


def build_forecast_and_advice(outlook: dict, cards: pd.DataFrame, asof) -> str:
    adv = []
    for fk, code, label in FUELS:
        d = (outlook or {}).get(fk) or {}
        h24 = next((h for h in d.get("horizons", []) if h["hours"] == 24), None)
        res = combine(h24["tier"] if h24 else None, h24["grade"] if h24 else None, latest_card(cards, code))
        color = {"ลง": "#0f766e", "ขึ้น": "#b91c1c", None: "#475569"}[res["dir"]]
        adv.append(f"<div class='ps-advice' style='border-left:6px solid {color}'><div class='ps-adv-fuel'>{label}</div>"
                   f"<div class='ps-adv-action'>{_e(res['action'])}</div>"
                   f"<div class='muted'>ระดับสัญญาณ: {_e(res['level'])} · {_e(res['why'])}</div></div>")
    return f"""
    <section class="ps-block">
      <h2>1. ผลพยากรณ์</h2>
      <div class="muted">ดู ณ {asof.strftime('%d/%m/%Y %H:%M')} · "ขึ้น/ลง" = ราคาตลาดขยับตั้งแต่ 10 สต./ลิตร</div>
      <div style="overflow-x:auto"><table class="acc-table ps-table">
        <thead><tr><th>ชนิด</th><th>แนวโน้มตลาด 24 ชม.</th><th>แนวโน้มตลาด 48 ชม.</th><th>ต้นทุนส่งวันถัดไป (การ์ด)</th></tr></thead>
        <tbody>{_forecast_rows(outlook, cards, asof)}</tbody></table></div>
    </section>
    <section class="ps-block">
      <h2>2. คำแนะนำ</h2>
      <div class="ps-advice-grid">{''.join(adv)}</div>
      <div class="muted">สรุปจากสัญญาณ 2 แหล่ง ถ้าขัดกันจะไม่ฟันธง · ยังไม่รวมข้อจำกัดโควต้าและเวลาตัดรับออเดอร์ของแต่ละคลัง
      · อยู่ระหว่างทดสอบ ใช้ประกอบการตัดสินใจ ไม่ใช่คำสั่งซื้อ</div>
    </section>"""


# ---------- ส่วนที่ 3: Backtest ----------
def _bt_key(code: str, hours: int, ref_code: str) -> str:
    return f"{code}_{'P_' if ref_code == 'P' else ''}H{hours}"


def build_backtest(summary: dict, outlook: dict) -> str:
    res = (summary or {}).get("outlook_results") or {}
    if not res:
        return "<section class='ps-block'><h2>3. Backtest (ทดสอบย้อนหลัง)</h2><div class='muted'>ยังไม่มีผลทดสอบย้อนหลัง</div></section>"
    rows = []
    n_days = None
    for fk, code, label in FUELS:
        d = (outlook or {}).get(fk) or {}
        r = res.get(_bt_key(code, 24, d.get("source", "")))
        if not r:
            continue
        n_days = r.get("n_oos_days", n_days)
        for tier in ("DOWN_STRONG", "DOWN", "UP", "UP_STRONG"):
            t = r["tiers"].get(tier)
            if not t:
                continue
            rows.append(f"<tr><td>{label}</td><td>{TIER_TH[tier]}</td><td>{t['n']} วัน ({t['share']:.0%})</td>"
                        f"<td>{t['right10']:.0%}</td><td>{t['wrong10']:.0%}</td><td>{t['avg_move']:.2f}</td></tr>")
        u = r["tiers"].get("UNCLEAR")
        if u:
            rows.append(f"<tr><td>{label}</td><td>ยังไม่ชัดเจน</td><td>{u['n']} วัน ({u['share']:.0%})</td><td colspan='3' class='muted'>ไม่ฟันธง</td></tr>")
    gen = _e((summary or {}).get("generated", "-"))
    return f"""
    <section class="ps-block">
      <h2>3. Backtest (ทดสอบย้อนหลัง)</h2>
      <div class="muted">แนวโน้มตลาด 24 ชม. ทดสอบแบบเดินหน้า (ฝึกจากอดีตเท่านั้น) กับ {n_days or '-'} วันที่ไม่เคยใช้ฝึก · ผลเมื่อ {gen}<br>
      อ่านว่า: เมื่อระบบทาย "ลง" ในวันที่ผ่านมา ราคาลงจริงตั้งแต่ 10 สต. กี่ % และกลับขึ้นผิดทิศกี่ %</div>
      <div style="overflow-x:auto"><table class="acc-table ps-table">
        <thead><tr><th>ชนิด</th><th>ระบบทายว่า</th><th>จำนวนครั้ง</th><th>ถูกทิศ</th><th>ผิดทิศ</th><th>ขยับเฉลี่ยตามทิศที่ทาย (บาท/ลิตร)</th></tr></thead>
        <tbody>{''.join(rows)}</tbody></table></div>
      <div class="muted">การ์ดต้นทุนส่งที่เพิ่งเริ่มใช้ยังไม่มีผลทดสอบเดินหน้าเพียงพอ จึงยังไม่แสดงในตารางนี้</div>
    </section>"""


# ---------- ส่วนที่ 4: ความแม่นยำจริง ----------
def build_live_accuracy(outlook_log: pd.DataFrame, cards: pd.DataFrame, engine_mod) -> str:
    rows = []
    for fk, code, label in FUELS:
        fuel_key = engine_mod.FUEL_KEY[fk]
        st = engine_mod.outlook_live_stats(outlook_log, fuel_key, 24, "20:30") if outlook_log is not None and not outlook_log.empty else None
        if st:
            parts = []
            for tier in ("DOWN_STRONG", "DOWN", "UP", "UP_STRONG"):
                x = st["tiers"].get(tier)
                if x and "right10" in x:
                    parts.append(f"{TIER_TH[tier]} {x['n']} ครั้ง (ถูก {x['right10']:.0%} / ผิด {x['wrong10']:.0%})")
            enough = "" if st["n_days"] >= MIN_LIVE_DAYS else f" <span class='muted'>· ยังน้อยเกินไป (ต้องการ ≥{MIN_LIVE_DAYS} วัน)</span>"
            rows.append(f"<tr><td>{label}</td><td>แนวโน้มตลาด 24 ชม.</td><td>{st['n_days']} วัน{enough}</td><td>{_e(' · '.join(parts)) or '-'}</td></tr>")
        else:
            rows.append(f"<tr><td>{label}</td><td>แนวโน้มตลาด 24 ชม.</td><td>0 วัน</td><td class='muted'>ยังไม่มีผลที่ครบ 24 ชม.</td></tr>")
        cs = card_live_stats(cards, code)
        if cs["n_cards"]:
            enough = "" if cs["n_cards"] >= MIN_LIVE_DAYS else f" <span class='muted'>· ยังน้อยเกินไป (ต้องการ ≥{MIN_LIVE_DAYS})</span>"
            hit = f"ฟันธง {cs['n_called']} ครั้ง ถูก {cs['n_right']}" + (f" ({cs['n_right'] / cs['n_called']:.0%})" if cs["n_called"] else "")
            rows.append(f"<tr><td>{label}</td><td>การ์ดต้นทุนส่ง</td><td>{cs['n_cards']} ใบ{enough}</td><td>{hit}</td></tr>")
        else:
            rows.append(f"<tr><td>{label}</td><td>การ์ดต้นทุนส่ง</td><td>0 ใบ</td><td class='muted'>ยังไม่มีการ์ดล่วงหน้าที่รู้ผลแล้ว</td></tr>")
    hind = sum(card_live_stats(cards, c)["n_hind"] for _, c, _ in FUELS)
    note = (f"<div class='muted'>ไม่นับการ์ด {hind} ใบที่ออกหลังผลวันนั้นทราบแล้ว (ย้อนหลัง ไม่ใช่การพยากรณ์ล่วงหน้า)</div>" if hind else "")
    return f"""
    <section class="ps-block">
      <h2>4. ความแม่นยำจริง (ที่ระบบพยากรณ์ล่วงหน้าแล้วรู้ผล)</h2>
      <div class="muted">นับเฉพาะที่ออกคำพยากรณ์ก่อนเหตุการณ์จริง · ตัวเลขจะน่าเชื่อถือเมื่อสะสมอย่างน้อย {MIN_LIVE_DAYS} วัน</div>
      <div style="overflow-x:auto"><table class="acc-table ps-table">
        <thead><tr><th>ชนิด</th><th>ระบบ</th><th>จำนวนที่รู้ผลแล้ว</th><th>ผล</th></tr></thead>
        <tbody>{''.join(rows)}</tbody></table></div>{note}
    </section>"""


CSS = """
    .ps-block { background:#fff; border:1px solid #e5e7eb; border-radius:16px; padding:18px 20px; margin:14px 0; }
    .ps-block h2 { margin:0 0 6px; font-size:1.15rem; }
    .acc-table.ps-table td, .acc-table.ps-table th { white-space:normal; text-align:left; vertical-align:top; }
    .ps-advice-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin:8px 0; }
    .ps-advice { background:#f8fafc; border-radius:10px; padding:12px 14px; }
    .ps-adv-fuel { font-weight:600; color:#334155; }
    .ps-adv-action { font-size:1.2rem; font-weight:700; margin:4px 0; }
    details.ps-more > summary { cursor:pointer; font-weight:600; padding:12px 4px; }
    @media (max-width:680px) { .ps-advice-grid { grid-template-columns:1fr; } }
"""


def build_summary(outlook: dict, summary: dict, outlook_log: pd.DataFrame, cards_raw: pd.DataFrame, asof, engine_mod) -> str:
    cards = prep_cards(cards_raw)
    return (build_forecast_and_advice(outlook, cards, asof) + build_backtest(summary, outlook)
            + build_live_accuracy(outlook_log, cards, engine_mod))
