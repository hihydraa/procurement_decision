"""
หน้าสรุปสำหรับทีมจัดซื้อ — 4 ส่วนเท่านั้น: ผลพยากรณ์ · คำแนะนำ · Backtest · ความแม่นยำจริง

ใช้ข้อมูลที่ main.py มีอยู่แล้ว (แนวโน้ม 24/48 ชม. จาก engine, backtest_summary.json, OutlookLog) และการ์ดพยากรณ์จากแท็บ ForecastCardLog
ฟังก์ชันทั้งหมดเป็น pure function (ไม่อ่านชีทเอง) เพื่อทดสอบได้ ; main.py เป็นผู้โหลดข้อมูลแล้วส่งเข้ามา

หลักการ: ไม่ตกแต่งตัวเลข — ถ้าข้อมูลยังไม่พอให้บอกว่ายังไม่พอ ; การ์ดที่ผลทราบแล้วก่อนออก (ย้อนหลัง) ไม่ถูกนับเป็นผลทดสอบเดินหน้า (ดู prep_cards)
"""
from __future__ import annotations

from html import escape

import pandas as pd

FUELS = [("DIESEL", "B7", "ดีเซล"), ("GASOHOL95", "G95", "แก๊สโซฮอล์ 95")]
TIER_TH = {"UP_STRONG": "ขึ้นแรง", "UP": "ขึ้น", "UNCLEAR": "ยังไม่ชัดเจน", "DOWN": "ลง", "DOWN_STRONG": "ลงแรง"}
HIND_MIN = 15              # ผลถูกเติมภายในกี่นาทีหลังออกการ์ด = ย้อนหลัง (ตรงกับ forecast_card.HIND_MIN)
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
    """ตารางการ์ดที่ใช้ได้ : เพิ่ม asof_dt, target_dt, valid_forward
    valid_forward = ผลยังไม่ทราบตอนออกการ์ด ; ถ้าผลถูกเติมภายใน HIND_MIN นาทีหลังออก (ทราบแล้วตอนออก) ถือเป็นย้อนหลัง"""
    need = {"card_id", "asof", "fuel", "target_day"}
    if df is None or df.empty or not need.issubset(df.columns):
        return pd.DataFrame()
    d = df.copy()
    d["asof_dt"] = d["asof"].map(_parse_dt)
    d["target_dt"] = d["target_day"].map(_parse_dt)
    d["filled_dt"] = d["filled_at"].map(_parse_dt) if "filled_at" in d.columns else pd.NaT
    d = d.dropna(subset=["asof_dt", "target_dt"])
    hind = d["filled_dt"].notna() & (d["filled_dt"] <= d["asof_dt"] + pd.Timedelta(minutes=HIND_MIN))
    d["valid_forward"] = ~hind
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


def combine(tier24: str | None, grade: str | None, card: dict | None, tier48: str | None = None, grade48: str | None = None) -> dict:
    """รวมสัญญาณ (แนวโน้มตลาด 24 ชม. + การ์ดต้นทุน) เป็นคำแนะนำเดียว ; ไม่ตรงกัน = ไม่ฟันธง
    แล้วปรับด้วยแนวโน้ม 48 ชม.: ถ้า 24 ชม./การ์ดไม่ชัดแต่ 48 ชม. ชี้ทิศ ให้บอกเรื่อง 48 ชม. ; ถ้า 48 ชม. สวนทิศคำแนะนำ ให้เตือนและลดระดับ"""
    r = _combine_base(tier24, grade, card)
    d48 = outlook_direction(tier48) if tier48 else None
    if d48 is None:
        return r
    g48 = grade48 or "ต่ำ"
    if r["dir"] is None and r["action"].startswith("สั่งซื้อตามแผนปกติ"):
        if d48 == "ขึ้น":
            return {"dir": "ขึ้น", "level": g48, "why": f"จากแนวโน้มตลาด 48 ชม. (24 ชม. ยังไม่ชัด) ความเชื่อมั่น {g48}",
                    "action": "วันนี้ยังไม่ต้องเร่ง แต่ควรซื้อให้เสร็จก่อนครบ 48 ชม. (ราคามีแนวโน้มขึ้น)"}
        return {"dir": "ลง", "level": g48, "why": f"จากแนวโน้มตลาด 48 ชม. (24 ชม. ยังไม่ชัด) ความเชื่อมั่น {g48}",
                "action": "ไม่ต้องรีบซื้อ ราคามีแนวโน้มลงใน 48 ชม."}
    if r["dir"] and d48 != r["dir"]:
        r = dict(r)
        r["why"] += f" · แต่แนวโน้ม 48 ชม. ชี้{d48} (ความเชื่อมั่น {g48})"
        if r["level"] == "สูง":
            r["level"] = "กลาง"
        r["action"] += " (อย่ารอเกิน 24 ชม.)" if r["dir"] == "ลง" else " (ซื้อเฉพาะที่จำเป็นก่อน)"
    return r


def _combine_base(tier24: str | None, grade: str | None, card: dict | None) -> dict:
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


# ---------- ภาพประกอบ (ไอคอน/แถบสี) ----------
# สี: ราคาลง = เขียว (ดีต่อจัดซื้อ), ราคาขึ้น = แดง, นิ่ง/ไม่ชัด = เทา ; ทุกสีมีไอคอนและตัวอักษรกำกับเสมอ ไม่พึ่งสีอย่างเดียว
DIR_META = {
    "ลง": ("▼", "ps-down", "ราคาลง"),
    "ขึ้น": ("▲", "ps-up", "ราคาขึ้น"),
    "นิ่ง": ("▬", "ps-flat", "ทรงตัว"),
    None: ("◆", "ps-none", "ไม่ชัดเจน"),
}


def _tier_dir(tier: str):
    return outlook_direction(tier) if tier and tier != "UNCLEAR" else None


def _badge(direction, text: str, strong: bool = False, sub: str = "") -> str:
    icon, cls, _ = DIR_META[direction]
    icon = icon * 2 if strong else icon
    sub_html = f"<span class='ps-badge-sub'>{_e(sub)}</span>" if sub else ""
    return f"<span class='ps-badge {cls}'><span class='ps-ico'>{icon}</span><span>{_e(text)}{sub_html}</span></span>"


def _prob_bar(pd_, pf, pu) -> str:
    segs = []
    for v, cls, name in ((pd_, "ps-down", "ลง"), (pf, "ps-flat", "นิ่ง"), (pu, "ps-up", "ขึ้น")):
        lab = f"{name} {v:.0%}" if v >= 0.14 else ""
        segs.append(f"<div class='ps-seg {cls}' style='flex:{max(v, 0.001):.4f}' title='{name} {v:.0%}'>{lab}</div>")
    return f"<div class='ps-prob'>{''.join(segs)}</div>"


def _range_bar(lo: float, hi: float) -> str:
    """แถบช่วงการเปลี่ยนต้นทุนบนเส้นตัวเลข มีเส้น 0 กลาง : ซ้าย = ลง, ขวา = ขึ้น"""
    half = max(abs(lo), abs(hi), 0.5) * 1.15
    l = (lo + half) / (2 * half) * 100
    r = (hi + half) / (2 * half) * 100
    cls = "ps-down" if hi < 0 else "ps-up" if lo > 0 else "ps-none"
    return (f"<div class='ps-range' title='ช่วงที่เป็นไปได้ {lo:+.2f} ถึง {hi:+.2f} บาท/ลิตร'><div class='ps-range-zero'></div>"
            f"<div class='ps-range-fill {cls}' style='left:{l:.1f}%;width:{max(r - l, 1.5):.1f}%'></div></div>"
            f"<div class='ps-range-axis'><span>ลง {half:.1f}</span><span>0</span><span>ขึ้น {half:.1f}</span></div>")


def _forecast_card(label: str, d: dict, card: dict | None) -> str:
    hz = {h["hours"]: h for h in d.get("horizons", [])}

    def badge(h):
        if not h:
            return "<span class='ps-badge ps-none'><span class='ps-ico'>◆</span><span>ไม่มีข้อมูล</span></span>"
        dr = _tier_dir(h["tier"])
        sub = "" if dr is None else f"ความเชื่อมั่น {h['grade']}"
        return _badge(dr, TIER_TH[h["tier"]], strong=h["tier"].endswith("STRONG"), sub=sub)

    market = (f"<div class='ps-row'><div class='ps-key'>ตลาด 24 ชม.</div>{badge(hz.get(24))}</div>"
              f"<div class='ps-row'><div class='ps-key'>ตลาด 48 ชม.</div>{badge(hz.get(48))}</div>")
    if card:
        lo, hi = _num(card.get("lo")), _num(card.get("hi"))
        pd_, pf, pu = (_num(card.get(k)) for k in ("p_down", "p_flat", "p_up"))
        call = card.get("call")
        cd = call if call in ("ลง", "ขึ้น") else None
        body = _badge(cd, f"ต้นทุนส่ง {'ไม่ชัดเจน' if cd is None else call}", sub=f"ความเชื่อมั่น {card.get('confidence', '-')}")
        if None not in (pd_, pf, pu):
            body += f"<div class='ps-cap'>โอกาส ลง / นิ่ง / ขึ้น</div>{_prob_bar(pd_, pf, pu)}"
        if lo is not None and hi is not None:
            body += f"<div class='ps-cap'>ต้นทุนเปลี่ยนประมาณ <strong>{lo:+.2f} ถึง {hi:+.2f}</strong> บาท/ลิตร</div>{_range_bar(lo, hi)}"
        if card.get("timing"):
            body += f"<div class='ps-cap'>🕒 เวลาที่มักปรับ: {_e(card['timing'])}</div>"
        body += (f"<div class='ps-cap muted'>สำหรับ {pd.to_datetime(card['target_dt']).strftime('%d/%m')} · ออกเมื่อ "
                 f"{pd.to_datetime(card['asof_dt']).strftime('%d/%m %H:%M')}</div>")
    else:
        body = "<div class='ps-cap muted'>ยังไม่มีการ์ดต้นทุนส่งล่วงหน้า (ออกรอบ 21:30)</div>"
    return (f"<div class='ps-fcard'><div class='ps-fuel'>{label}</div>{market}<div class='ps-sep'></div>{body}</div>")


def build_forecast_and_advice(outlook: dict, cards: pd.DataFrame, asof) -> str:
    fc, adv = [], []
    for fk, code, label in FUELS:
        d = (outlook or {}).get(fk) or {}
        card = latest_card(cards, code)
        fc.append(_forecast_card(label, d, card))
        h24 = next((h for h in d.get("horizons", []) if h["hours"] == 24), None)
        h48 = next((h for h in d.get("horizons", []) if h["hours"] == 48), None)
        res = combine(h24["tier"] if h24 else None, h24["grade"] if h24 else None, card, h48["tier"] if h48 else None, h48["grade"] if h48 else None)
        icon, cls, _ = DIR_META[res["dir"]]
        dots = {"สูง": "●●●", "กลาง": "●●○", "ต่ำ": "●○○", "ไม่ชัด": "○○○"}[res["level"]]
        adv.append(f"<div class='ps-advice {cls}-b'><div class='ps-adv-ico {cls}'>{icon}</div><div>"
                   f"<div class='ps-adv-fuel'>{label}</div><div class='ps-adv-action'>{_e(res['action'])}</div>"
                   f"<div class='muted'><span class='ps-dots'>{dots}</span> ระดับสัญญาณ: {_e(res['level'])} · {_e(res['why'])}</div></div></div>")
    legend = ("<div class='ps-legend'><span class='ps-down-t'>▼ ราคาลง (ดีต่อการซื้อ: รอได้)</span>"
              "<span class='ps-up-t'>▲ ราคาขึ้น (ควรซื้อก่อน)</span><span class='ps-none-t'>◆ ไม่ชัดเจน (ซื้อตามปกติ)</span>"
              "<span class='muted'>▼▼ ▲▲ = แรง (ขยับ ≥ 40 สต.)</span></div>")
    return f"""
    <section class="ps-block">
      <h2>1. ผลพยากรณ์</h2>
      <div class="muted">ดู ณ {asof.strftime('%d/%m/%Y %H:%M')} · "ขึ้น/ลง" = ราคาขยับตั้งแต่ 10 สต./ลิตร</div>
      {legend}
      <div class="ps-fgrid">{''.join(fc)}</div>
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


def _pair_bars(right: float, wrong: float) -> str:
    return (f"<div class='ps-pair'><div class='ps-pbar'><div class='ps-right' style='width:{right * 100:.0f}%'></div></div>"
            f"<span class='ps-pval'>ถูกทิศ <strong>{right:.0%}</strong></span></div>"
            f"<div class='ps-pair'><div class='ps-pbar'><div class='ps-wrong' style='width:{wrong * 100:.0f}%'></div></div>"
            f"<span class='ps-pval'>ผิดทิศ <strong>{wrong:.0%}</strong></span></div>")


def build_backtest(summary: dict, outlook: dict) -> str:
    res = (summary or {}).get("outlook_results") or {}
    if not res:
        return "<section class='ps-block'><h2>3. Backtest (ทดสอบย้อนหลัง)</h2><div class='muted'>ยังไม่มีผลทดสอบย้อนหลัง</div></section>"
    cols = []
    n_days = None
    for fk, code, label in FUELS:
        d = (outlook or {}).get(fk) or {}
        r = res.get(_bt_key(code, 24, d.get("source", "")))
        if not r:
            continue
        n_days = r.get("n_oos_days", n_days)
        rows = []
        for tier in ("DOWN_STRONG", "DOWN", "UP", "UP_STRONG"):
            t = r["tiers"].get(tier)
            if not t:
                continue
            dr = _tier_dir(tier)
            rows.append(f"<div class='ps-btrow'><div class='ps-bthead'>{_badge(dr, 'ระบบทาย' + TIER_TH[tier], strong=tier.endswith('STRONG'))}"
                        f"<span class='muted'>{t['n']} ครั้ง ({t['share']:.0%} ของวัน)</span></div>{_pair_bars(t['right10'], t['wrong10'])}</div>")
        u = r["tiers"].get("UNCLEAR")
        if u:
            rows.append(f"<div class='ps-btrow muted'>◆ ไม่ฟันธง {u['n']} วัน ({u['share']:.0%} ของวัน)</div>")
        cols.append(f"<div class='ps-fcard'><div class='ps-fuel'>{label}</div>{''.join(rows)}</div>")
    gen = _e((summary or {}).get("generated", "-"))
    return f"""
    <section class="ps-block">
      <h2>3. Backtest (ทดสอบย้อนหลัง)</h2>
      <div class="muted">แนวโน้มตลาด 24 ชม. ทดสอบแบบเดินหน้า (ฝึกจากอดีตเท่านั้น) กับ {n_days or '-'} วันที่ไม่เคยใช้ฝึก · ผลเมื่อ {gen}<br>
      อ่านว่า: เมื่อระบบทายทิศนั้นในอดีต ราคาไปตามที่ทายตั้งแต่ 10 สต. กี่ % และไปผิดทิศกี่ % (แถบน้ำเงิน = ถูกทิศ, แถบส้ม = ผิดทิศ)</div>
      <div class="ps-fgrid">{''.join(cols)}</div>
      <div class="muted">การ์ดต้นทุนส่งที่เพิ่งเริ่มใช้ยังไม่มีผลทดสอบเดินหน้าเพียงพอ จึงยังไม่แสดงในส่วนนี้</div>
    </section>"""


# ---------- ส่วนที่ 4: ความแม่นยำจริง ----------
def _progress(n: int, goal: int = MIN_LIVE_DAYS) -> str:
    pct = min(n / goal, 1.0) * 100
    done = n >= goal
    return (f"<div class='ps-prog' title='สะสม {n} จาก {goal}'><div class='ps-pbar'><div class='{'ps-right' if done else 'ps-none'}' style='width:{pct:.0f}%'></div></div>"
            f"<span class='ps-pval'>{n}/{goal} {'✔ พอสรุปได้' if done else 'ยังน้อยเกินไป'}</span></div>")


def build_live_accuracy(outlook_log: pd.DataFrame, cards: pd.DataFrame, engine_mod) -> str:
    cols = []
    for fk, code, label in FUELS:
        rows = []
        fuel_key = engine_mod.FUEL_KEY[fk]
        st = engine_mod.outlook_live_stats(outlook_log, fuel_key, 24, "20:30") if outlook_log is not None and not outlook_log.empty else None
        rows.append("<div class='ps-btrow'><div class='ps-bthead'><strong>แนวโน้มตลาด 24 ชม.</strong></div>")
        if st:
            rows.append(_progress(st["n_days"]))
            for tier in ("DOWN_STRONG", "DOWN", "UP", "UP_STRONG"):
                x = st["tiers"].get(tier)
                if x and "right10" in x:
                    rows.append(f"<div class='ps-bthead'>{_badge(_tier_dir(tier), TIER_TH[tier], strong=tier.endswith('STRONG'))}"
                                f"<span class='muted'>{x['n']} ครั้ง</span></div>{_pair_bars(x['right10'], x['wrong10'])}")
        else:
            rows.append("<div class='muted'>ยังไม่มีผลที่ครบ 24 ชม.</div>")
        rows.append("</div>")
        cs = card_live_stats(cards, code)
        rows.append("<div class='ps-btrow'><div class='ps-bthead'><strong>การ์ดต้นทุนส่ง</strong></div>")
        if cs["n_cards"]:
            rows.append(_progress(cs["n_cards"]))
            if cs["n_called"]:
                rows.append(_pair_bars(cs["n_right"] / cs["n_called"], 1 - cs["n_right"] / cs["n_called"])
                            + f"<div class='muted'>ฟันธง {cs['n_called']} ครั้ง ถูก {cs['n_right']}</div>")
            else:
                rows.append("<div class='muted'>ยังไม่เคยฟันธงทิศ</div>")
        else:
            rows.append("<div class='muted'>ยังไม่มีการ์ดล่วงหน้าที่รู้ผลแล้ว</div>")
        rows.append("</div>")
        cols.append(f"<div class='ps-fcard'><div class='ps-fuel'>{label}</div>{''.join(rows)}</div>")
    hind = sum(card_live_stats(cards, c)["n_hind"] for _, c, _ in FUELS)
    note = (f"<div class='muted'>ไม่นับการ์ด {hind} ใบที่ออกหลังผลวันนั้นทราบแล้ว (ย้อนหลัง ไม่ใช่การพยากรณ์ล่วงหน้า)</div>" if hind else "")
    return f"""
    <section class="ps-block">
      <h2>4. ความแม่นยำจริง (ที่ระบบพยากรณ์ล่วงหน้าแล้วรู้ผล)</h2>
      <div class="muted">นับเฉพาะที่ออกคำพยากรณ์ก่อนเหตุการณ์จริง · ตัวเลขจะน่าเชื่อถือเมื่อสะสมอย่างน้อย {MIN_LIVE_DAYS} วัน (แถบ "สะสม" ยังไม่เต็ม = ยังสรุปไม่ได้)</div>
      <div class="ps-fgrid">{''.join(cols)}</div>{note}
    </section>"""


CSS = """
    .ps-block { background:#fff; border:1px solid #e5e7eb; border-radius:16px; padding:18px 20px; margin:14px 0; }
    .ps-block h2 { margin:0 0 6px; font-size:1.15rem; }
    .ps-down { background:#0ca30c; } .ps-up { background:#d03b3b; } .ps-flat { background:#9a9a94; } .ps-none { background:#9a9a94; }
    .ps-right { background:#2a78d6; } .ps-wrong { background:#eb6834; }
    .ps-fgrid { display:grid; grid-template-columns:1fr 1fr; gap:14px; margin:10px 0; }
    .ps-fcard { background:#f8fafc; border-radius:12px; padding:14px 16px; }
    .ps-fuel { font-weight:700; font-size:1.05rem; margin-bottom:8px; }
    .ps-row { display:flex; align-items:center; gap:10px; margin:6px 0; }
    .ps-key { width:92px; color:#475569; font-size:.9rem; flex:none; }
    .ps-sep { border-top:1px dashed #cbd5e1; margin:10px 0; }
    .ps-cap { font-size:.88rem; color:#334155; margin:8px 0 4px; }
    .ps-badge { display:inline-flex; align-items:center; gap:8px; padding:5px 12px 5px 8px; border-radius:999px; font-weight:600; color:#fff; font-size:.95rem; }
    .ps-badge.ps-down { background:#006300; } .ps-badge.ps-up { background:#b02a2a; } .ps-badge.ps-flat, .ps-badge.ps-none { background:#64645e; }
    .ps-ico { font-size:.95rem; letter-spacing:-2px; }
    .ps-badge-sub { display:block; font-weight:400; font-size:.75rem; opacity:.9; }
    .ps-prob { display:flex; gap:2px; height:26px; border-radius:6px; overflow:hidden; }
    .ps-seg { color:#fff; font-size:.78rem; font-weight:600; display:flex; align-items:center; justify-content:center; white-space:nowrap; min-width:3px; }
    .ps-seg.ps-down { background:#006300; } .ps-seg.ps-up { background:#b02a2a; } .ps-seg.ps-flat { background:#64645e; }
    .ps-range { position:relative; height:18px; background:#e2e8f0; border-radius:9px; }
    .ps-range-zero { position:absolute; left:50%; top:-3px; bottom:-3px; width:2px; background:#334155; }
    .ps-range-fill { position:absolute; top:3px; bottom:3px; border-radius:6px; }
    .ps-range-axis { display:flex; justify-content:space-between; font-size:.72rem; color:#64748b; margin-top:2px; }
    .ps-legend { display:flex; flex-wrap:wrap; gap:6px 16px; margin:8px 0 4px; font-size:.88rem; font-weight:600; }
    .ps-down-t { color:#006300; } .ps-up-t { color:#b02a2a; } .ps-none-t { color:#52514e; }
    .ps-advice-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin:8px 0; }
    .ps-advice { display:flex; gap:14px; align-items:center; background:#f8fafc; border-radius:12px; padding:14px 16px; border-left:6px solid #9a9a94; }
    .ps-down-b { border-left-color:#0ca30c; } .ps-up-b { border-left-color:#d03b3b; }
    .ps-adv-ico { width:48px; height:48px; flex:none; border-radius:50%; color:#fff; display:flex; align-items:center; justify-content:center; font-size:1.4rem; }
    .ps-adv-ico.ps-down { background:#006300; } .ps-adv-ico.ps-up { background:#b02a2a; } .ps-adv-ico.ps-none, .ps-adv-ico.ps-flat { background:#64645e; }
    .ps-adv-fuel { font-weight:600; color:#334155; }
    .ps-adv-action { font-size:1.2rem; font-weight:700; margin:2px 0 4px; }
    .ps-dots { letter-spacing:2px; color:#334155; }
    .ps-btrow { margin:10px 0 14px; }
    .ps-bthead { display:flex; justify-content:space-between; align-items:center; gap:8px; margin-bottom:4px; }
    .ps-pair, .ps-prog { display:flex; align-items:center; gap:8px; margin:3px 0; }
    .ps-pbar { flex:1; height:12px; background:#e2e8f0; border-radius:6px; overflow:hidden; }
    .ps-pbar > div { height:100%; border-radius:6px; }
    .ps-pval { width:104px; flex:none; font-size:.85rem; }
    details.ps-more > summary { cursor:pointer; font-weight:600; padding:12px 4px; }
    @media (max-width:680px) { .ps-advice-grid, .ps-fgrid { grid-template-columns:1fr; } .ps-key { width:76px; } }
"""


def build_summary(outlook: dict, summary: dict, outlook_log: pd.DataFrame, cards_raw: pd.DataFrame, asof, engine_mod) -> str:
    cards = prep_cards(cards_raw)
    return (build_forecast_and_advice(outlook, cards, asof) + build_backtest(summary, outlook)
            + build_live_accuracy(outlook_log, cards, engine_mod))
