"""
การ์ด Flex ของไลน์ (carousel 4 ใบ) สร้างจากข้อมูลชุดเดียวกับข้อความตัวอักษรใน line_outlook.py — ตัวเลขทุกตัวมาจากข้อมูลจริง ไม่มีการสร้างภาพ
  ใบ 1 🧭 ควรโหลดเมื่อไร (เฉพาะคำทายรอบ 20:30) — ช่วง 12:00–20:30 ใช้ 🚚 ส่งรถตอนนี้ดีไหม (แผนส่งรถช่วงบ่าย) แทน
  ใบ 2 🏭 ราคาหน้าคลัง Jobber 24 ชม.
  ใบ 3 ⛽ ราคาขายปลีก (ทดลอง)                      ใบ 4 📊 ผลงานที่ผ่านมา
สีหัวการ์ดแยกหมวด (หน้าคลัง = น้ำเงิน , ขายปลีก = ม่วง) ; สีในกล่องบอกทิศเหมือนกันทุกหมวด (ส้ม = ขึ้น/รีบ , เขียว = ลง/รอ , เทา = ไม่ชัด)
ใช้ใน main.py : outlook.json ช่อง line_flex = {"altText", "contents"} ให้ Worker ตอบคำสั่ง forecast (ข้อความตัวอักษร line_text เป็นทางสำรอง)
"""
import json

import pandas as pd

import engine as E
import line_outlook as L
import model as M

C = {"up": "#E8590C", "down": "#2B8A3E", "flat": "#868E96", "ink": "#212529", "sub": "#6C757D", "track": "#E9ECEF",
     "bg_up": "#FFF4E6", "bg_down": "#EBFBEE", "bg_flat": "#F1F3F5"}
THEME = {"advice": ("#343A40", "#FFFFFF"), "dispatch": ("#343A40", "#FFFFFF"), "jobber": ("#1864AB", "#F1F7FD"), "retail": ("#6741D9", "#F6F3FF"), "perf": ("#495057", "#FFFFFF")}
TIER_SHORT = {"UP_STRONG": "📈📈 ขึ้นแรง ≥40 สต.", "UP": "📈 ขึ้น 10–40 สต.", "DOWN": "📉 ลง 10–40 สต.", "DOWN_STRONG": "📉📉 ลงแรง ≥40 สต.", "UNCLEAR": "❓ ไม่ชัด"}
RETAIL_TONE = {"ขึ้น": "up", "ลง": "down", "นิ่ง": "flat"}
MAX_BYTES = 45_000          # เพดานของ LINE ต่อ carousel คือ 50 KB : เกินนี้ไม่ส่งการ์ด (Worker ใช้ข้อความตัวอักษรแทน)
NOTE = "[ทดลอง] ใช้ประกอบการตัดสินใจ ไม่ใช่คำสั่งซื้อ"


# ---------- ส่วนประกอบ ----------
def txt(t, size="sm", color=None, bold=False, align=None, flex=None, margin=None, wrap=True):
    d = {"type": "text", "text": str(t)[:2000] or " ", "size": size, "color": color or C["ink"], "wrap": wrap}
    if bold:
        d["weight"] = "bold"
    if align:
        d["align"] = align
    if flex is not None:
        d["flex"] = flex
    if margin:
        d["margin"] = margin
    return d


def bar(label, p, color):
    p = min(max(float(p), 0.0), 1.0)
    pct = max(1, round(p * 100))
    return {"type": "box", "layout": "horizontal", "spacing": "sm", "margin": "sm", "alignItems": "center", "contents": [
        txt(label, "xs", C["sub"], flex=2, wrap=False),
        {"type": "box", "layout": "vertical", "flex": 6, "height": "8px", "cornerRadius": "4px", "backgroundColor": C["track"], "contents": [
            {"type": "box", "layout": "vertical", "width": f"{pct}%", "height": "8px", "cornerRadius": "4px", "backgroundColor": color,
             "contents": [{"type": "filler"}]}]},
        txt(f"{p:.0%}", "xs", C["ink"], bold=True, align="end", flex=2, wrap=False)]}


def tone_box(tone, rows, margin="md"):
    return {"type": "box", "layout": "vertical", "backgroundColor": C["bg_" + tone], "cornerRadius": "10px", "paddingAll": "12px",
            "margin": margin, "contents": rows}


def title_row(name, verdict, tone):
    return {"type": "box", "layout": "horizontal", "contents": [txt(name, "sm", C["sub"], bold=True, flex=5),
                                                                txt(verdict, "sm", C[tone], bold=True, align="end", flex=5)]}


def bubble(theme, title, sub, body, note=NOTE):
    head, bg = THEME[theme]
    return {"type": "bubble", "size": "mega",
            "header": {"type": "box", "layout": "vertical", "backgroundColor": head, "paddingAll": "14px",
                       "contents": [txt(title, "md", "#FFFFFF", bold=True), txt(sub, "xxs", "#E7E7F0")]},
            "body": {"type": "box", "layout": "vertical", "paddingAll": "12px", "backgroundColor": bg, "contents": body},
            "footer": {"type": "box", "layout": "vertical", "paddingAll": "10px", "backgroundColor": bg,
                       "contents": [txt(note, "xxs", C["sub"], align="center")]}}


# ---------- การ์ดแต่ละใบ ----------
def advice_bubble(inp: dict, ok: list):
    """ใช้ข้อความของ line_outlook.advice_line ตรง ๆ (บรรทัดแรก = คำแนะนำ , บรรทัดถัดไป = เหตุผล) เพื่อให้การ์ดกับข้อความตัวอักษรตรงกันเสมอ"""
    fuels = inp.get("fuels", {})
    if not ok or pd.Timestamp(fuels[ok[0][0]]["ref"]).strftime("%H:%M") != "20:30":
        return None
    ref = pd.Timestamp(fuels[ok[0][0]]["ref"])
    same_night = pd.Timestamp(inp["asof"]).normalize() == ref.normalize()
    body = []
    for i, (fuel, name) in enumerate(ok):
        lines = L.advice_line(dict(fuels[fuel], fuel_code=fuel), L.pending_rise(inp, fuel) if same_night else None).split("\n")
        head = lines[0].strip()
        tone = "up" if head.startswith("🛒") else "down" if head.startswith("⏳") else "flat"
        rows = [txt(name, "sm", C["sub"], bold=True), txt(head, "lg", C[tone], bold=True)]
        rows += [txt(x.strip().lstrip("•").strip(), "xxs", C["sub"], margin="sm") for x in lines[1:] if x.strip()]
        body.append(tone_box(tone, rows, "none" if i == 0 else "md"))
    body.append(txt(f"เทียบกับราคา ณ 20:30 น. {ref:%d/%m} · ไม่รวมโควตาและเวลาตัดรับออเดอร์ของคลัง", "xxs", C["sub"], margin="md"))
    if not same_night:
        body.append(txt(f"ออกเมื่อ 20:30 น. {ref:%d/%m} — ช่วงโหลดก่อนราคาขึ้นของคืนนั้นผ่านไปแล้ว", "xxs", C["up"], margin="sm"))
    day = ref + pd.Timedelta(days=1)
    return bubble("advice", "🧭 ควรโหลดเมื่อไร", f"รถที่อยู่คลัง · คืน {ref:%d/%m} หรือบ่าย {day:%d/%m}", body)


def dispatch_bubble(inp: dict):
    """แผนส่งรถช่วงบ่าย (engine.decide_dispatch) — ช่วงที่หน้างานตัดสินใจออกรถจริง ; ใช้ข้อความชุดเดียวกับ line_outlook.dispatch_view"""
    rows = L.dispatch_pm(inp)
    if not rows:
        return None
    body = []
    for i, (_, name, x) in enumerate(rows):
        v = L.dispatch_view(x)
        body.append(tone_box(v["tone"], [title_row(name, v["verdict"], v["tone"]), txt(v["why"], "xxs", C["sub"], margin="sm"),
                                         txt(v["arrive"], "xxs", C["sub"]), txt(v["live"], "xxs", C["ink"], margin="sm")],
                             "none" if i == 0 else "md"))
    body.append(txt("ราคาที่ได้จริง = ราคา ณ เวลารถถึงคลัง เทียบกับออกรถช้าไป 1 วัน · ไม่รวมโควตาและเวลาตัดรับออเดอร์ของคลัง",
                    "xxs", C["sub"], margin="md"))
    asof = pd.Timestamp(rows[0][2]["asof"])
    return bubble("dispatch", "🚚 ส่งรถตอนนี้ดีไหม", f"ประเมิน {asof:%H:%M} น. {asof:%d/%m} · ระยะเดินทาง {rows[0][2]['lead']} ชม.", body)


def tonight_box(inp: dict):
    """กล่อง "คืนนี้ตามประกาศ" (แน่นอน) ด้านบนของการ์ดราคาหน้าคลัง ; ไม่มีประกาศ = None"""
    rows = []
    for fuel, name in L.FUELS:
        c = L.tonight_change(inp, fuel)
        if not c:
            continue
        v = L.tonight_view(name, c)
        tone = "up" if c["dir"] == "UP" else "down" if c["dir"] == "DOWN" else "flat"
        rows.append(txt(v[0], "sm", C[tone], bold=True, margin="sm"))
        rows += [txt(x, "xxs", C["sub"]) for x in v[1:]]
    if not rows:
        return None
    return {"type": "box", "layout": "vertical", "backgroundColor": "#FFFFFF", "cornerRadius": "10px", "paddingAll": "12px",
            "borderWidth": "1px", "borderColor": C["track"], "contents": [txt("คืนนี้ตามประกาศของผู้ค้า (ยังไม่มีผล)", "xs", C["ink"], bold=True)] + rows}


def jobber_bubble(inp: dict, bad: dict, on_demand: bool):
    fuels, body, ref = inp.get("fuels", {}), [], None
    tb = tonight_box(inp)
    if tb:
        body += [tb, txt("พยากรณ์ 24 ชม. (หลังการปรับข้างบน ผู้ค้าอาจปรับต่อระหว่างวัน)", "xs", C["sub"], bold=True, margin="lg")]
    for i, (fuel, name) in enumerate(L.FUELS):
        margin = "none" if i == 0 and not tb else "md"
        if bad[fuel]:
            body.append(tone_box("flat", [title_row(name, "ไม่มีคำทาย", "flat"), txt(bad[fuel], "xxs", C["sub"])], margin))
            continue
        f = fuels[fuel]
        h, ref = f["h24"], pd.Timestamp(f["ref"])
        tier = h["tier"]
        tone = "up" if tier.startswith("UP") else "down" if tier.startswith("DOWN") else "flat"
        rows = [title_row(name, TIER_SHORT.get(tier, tier), tone)]
        rows.append(txt(f"ความเชื่อมั่น: {h.get('grade') or 'ต่ำ'}" if tone != "flat" else "ราคาอาจขยับได้ทั้งสองทาง", "xxs", C["sub"]))
        if h.get("p_up") is not None and h.get("p_down") is not None:
            rows += [bar("ขึ้น", h["p_up"], C["up"]), bar("ลง", h["p_down"], C["down"])]
        if tone == "flat":
            ups, downs = L._reasons(h["reasons"], L.UP_MARK, 1), L._reasons(h["reasons"], L.DOWN_MARK, 1)
            why = [f"ชี้ขึ้น: {u}" for u in ups] + [f"ชี้ลง: {d}" for d in downs]     # ฝั่งใดไม่มีเหตุผล = ข้าม (เดิม ups[0] ล้มเมื่อว่าง)
        else:
            why = L._reasons(h["reasons"], L.UP_MARK if tone == "up" else L.DOWN_MARK, 2)
        rows += [txt("• " + w, "xxs", C["sub"], margin="sm") for w in why]
        if tone != "flat" and h.get("n") and h.get("right10") is not None:
            rows.append(txt(f"ย้อนหลังระบบบอกแบบนี้ {int(h['n'])} ครั้ง: ถูก {h['right10']:.0%} · ผิดทิศ {h['wrong10']:.0%}", "xxs", C["ink"], margin="sm"))
        body.append(tone_box(tone, rows, margin))
    sub = (f"ข้อมูล {ref:%H:%M} น. {ref:%d/%m} · ใช้ได้ถึง {ref:%H:%M} น. {ref + pd.Timedelta(days=1):%d/%m}" if ref is not None
           else ("ตอนนี้" if on_demand else f"คืนนี้ ({pd.Timestamp(inp['asof']):%d/%m})") + " ไม่มีคำทาย")
    if ref is not None and ref.strftime("%H:%M") != "20:30":
        body.append(txt("รอบบ่าย: มีผลจริงเพียงไม่กี่วัน เชื่อถือได้น้อยกว่ารอบ 20:30", "xxs", C["up"], margin="md"))
    if ref is not None and L.early_note(inp, ref):
        body.append(txt(L.early_note(inp, ref), "xxs", C["up"], margin="md"))
    return bubble("jobber", "🏭 ราคาหน้าคลัง (Jobber) · 24 ชม.", sub, body)


def retail_bubble(retail: pd.DataFrame, asof: pd.Timestamp):
    import procurement_summary as PS
    import retail_forecast as RF
    if retail is None or retail.empty:
        return None
    today, body, tgts = pd.Timestamp(asof).normalize(), [], []
    for i, (fuel, name) in enumerate(L.FUELS):
        margin = "none" if i == 0 else "md"
        r = PS.latest_retail(retail, fuel)
        if not r or pd.Timestamp(r["target_dt"]).normalize() < today:
            body.append(tone_box("flat", [title_row(name, "ยังไม่มีพยากรณ์", "flat"), txt(L.RETAIL_NONE, "xxs", C["sub"])], margin))
            continue
        tgt = pd.Timestamp(r["target_dt"]).normalize()
        tgts.append(tgt)
        call = str(r.get("call") or "").strip()
        tone = RETAIL_TONE.get(call, "flat")
        rows = [title_row(f"{name} ({tgt:%d/%m})", L.RETAIL_CALL.get(call, "❓ ยังไม่ชัดเจน"), tone)]
        p = [L._f(r.get(k)) for k in ("p_down", "p_flat", "p_up")]
        if None not in p:
            rows += [bar("ลง", p[0], C["down"]), bar("ไม่ปรับ", p[1], C["flat"]), bar("ขึ้น", p[2], C["up"])]
        ev = RF.elevated(fuel, call, p[0], p[2], r.get("model"))
        if ev:
            rows.append(txt(RF.elevated_text(ev), "xxs", C["up"], margin="sm"))
        why = str(r.get("sig_reason") or "").strip()
        if why and why != "nan" and str(r.get("model") or "").strip() == "SIG":
            rows.append(txt("• เหตุผล: " + why, "xxs", C["sub"], margin="sm"))
        base, size = L._f(r.get("retail_base")), str(r.get("size_text") or "").strip()
        detail = ([f"ปลีกล่าสุด {base:.2f}"] if base is not None else []) + ([size] if size and size != "nan" else [])
        if detail:
            rows.append(txt("• " + " · ".join(detail), "xxs", C["sub"]))
        d = L._f(r.get("outcome_dretail"))
        if d is not None:
            hit = str(r.get("outcome_cls") or "").strip()
            mark = "" if call not in L.RETAIL_CALL else " ✅" if hit == call else " ❌"
            rows.append(txt(f"• ผลจริง: {'ไม่ปรับ' if abs(d) < 0.005 else f'ปรับ {d:+.2f} บาท/ลิตร'}{mark}", "xxs", C["ink"]))
        body.append(tone_box(tone, rows, margin))
    tgt = max(tgts) if tgts else None
    when = "รอพยากรณ์ของวันถัดไป" if tgt is None else f"วันนี้ ({tgt:%d/%m})" if tgt == today else f"พรุ่งนี้ ({tgt:%d/%m})" if tgt == today + pd.Timedelta(days=1) else f"วันที่ {tgt:%d/%m}"
    return bubble("retail", "⛽ ราคาขายปลีก (หน้าปั๊ม)", f"พยากรณ์ของ{when}", body, "[ทดลอง] ยังไม่ผ่านการทดสอบเดินหน้า")


def perf_bubble(log: pd.DataFrame):
    last, latest = [], None
    calls = right = wrong = flat = days = 0
    first = None
    for fuel, name in L.FUELS:
        d = E._outlook_scored(log, fuel, 24)
        if d is None:
            continue
        d = d.assign(_ref=pd.to_datetime(d["ref"], errors="coerce")).sort_values("_ref")
        r = d.iloc[-1]
        last.append((r["_ref"], name, r["tier"], float(r["outcome_delta"])))
        days, first = max(days, len(d)), d["_ref"].min() if first is None else min(first, d["_ref"].min())
        c = d[d["tier"] != "UNCLEAR"]
        mv = c["outcome_delta"] * c["tier"].map(lambda t: 1.0 if str(t).startswith("UP") else -1.0)
        calls, right, wrong = calls + len(c), right + int((mv >= M.OUTLOOK_SMALL).sum()), wrong + int((mv <= -M.OUTLOOK_SMALL).sum())
        flat += int((mv.abs() < M.OUTLOOK_SMALL).sum())
    if not last:
        return None
    latest = max(t for t, *_ in last)
    body = [txt(f"คำทายคืน {latest:%d/%m}", "xs", C["sub"], bold=True)]
    for t, name, tier, delta in last:
        if t != latest:
            continue
        call = "ไม่ได้ทาย" if tier == "UNCLEAR" else "ทาย" + L.CALL_WORD.get(tier, tier)
        mark = L._mark(tier, delta).strip() or "–"
        col = C["down"] if "✅" in mark else C["up"] if "❌" in mark else C["sub"]
        body.append({"type": "box", "layout": "horizontal", "margin": "sm", "contents": [
            txt(name, "sm", flex=4), txt(call, "sm", C["sub"], flex=3), txt(f"{delta:+.2f}", "sm", bold=True, align="end", flex=2),
            txt(mark.split()[0], "sm", col, align="end", flex=1)]})
    stat = lambda v, label, col: {"type": "box", "layout": "vertical", "contents": [txt(v, "xxl", col, bold=True, align="center"),
                                                                                     txt(label, "xxs", C["sub"], align="center")]}
    body += [{"type": "separator", "margin": "lg"},
             txt(f"รอบ 20:30 · ผลจริงสะสมตั้งแต่ {first:%d/%m} ({days}/{L.READY_DAYS} วัน)", "xs", C["sub"], bold=True, margin="lg"),
             {"type": "box", "layout": "horizontal", "margin": "md", "contents": [
                 stat(str(calls), "ฟันธง", C["ink"]), stat(str(right), "ถูก", C["down"]), stat(str(wrong), "ผิดทิศ", C["up"]), stat(str(flat), "ราคานิ่ง", C["flat"])]}]
    if calls:
        body.append(bar("ถูก", right / calls, C["down"]))
    body += [{"type": "separator", "margin": "lg"}, txt(L.pm_summary(log)[0], "xs", C["sub"], bold=True, margin="lg")]
    body.append(txt(f"เกณฑ์ก่อนใช้จริง: ผลจริงครบ {L.READY_DAYS} วัน", "xxs", C["sub"], margin="md"))
    return bubble("perf", "📊 ผลงานที่ผ่านมา", "ราคาหน้าคลัง Jobber · 24 ชม. · ตัวเลขหลัก = รอบ 20:30", body)


# ---------- รวม ----------
def alt_text(inp: dict, bad: dict) -> str:
    parts = []
    for fuel, name in L.FUELS:
        h = (inp.get("fuels", {}).get(fuel) or {}).get("h24")
        parts.append(f"{name} " + ("ไม่มีคำทาย" if bad[fuel] or not h else TIER_SHORT.get(h["tier"], h["tier"]).split(" ", 1)[-1]))
    return ("พยากรณ์ราคาน้ำมัน: " + " · ".join(parts))[:390]


def build_flex(inp: dict, log: pd.DataFrame, retail: pd.DataFrame = None, on_demand: bool = True):
    """{"altText", "contents": carousel} หรือ None ถ้าไม่มีอะไรจะแสดง / ใหญ่เกินเพดาน"""
    asof = pd.Timestamp(inp["asof"])
    bad = {fuel: L.usable((inp.get("fuels") or {}).get(fuel), asof, on_demand) for fuel, _ in L.FUELS}
    ok = [(fuel, name) for fuel, name in L.FUELS if not bad[fuel]]
    first = advice_bubble(inp, ok) or dispatch_bubble(inp)
    bubbles = [b for b in (first, jobber_bubble(inp, bad, on_demand), retail_bubble(retail, asof), perf_bubble(log)) if b]
    out = {"altText": alt_text(inp, bad), "contents": {"type": "carousel", "contents": bubbles}}
    return out if len(json.dumps(out, ensure_ascii=False).encode("utf-8")) <= MAX_BYTES else None
