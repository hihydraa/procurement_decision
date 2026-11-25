"""ข้อความไลน์ส่วนตัว (ช่วงทดสอบ) : เนื้อหาถูกต้อง ส่งได้เฉพาะรหัสผู้ใช้ส่วนตัว ส่งครั้งเดียวต่อคืน และไม่ส่งผิดเวลา"""
import os
import sys
import unittest
from unittest import mock

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import line_outlook as L  # noqa: E402

UID = "U" + "0123456789abcdef" * 2


def _inp(b7="UP", g95="UNCLEAR", asof="2026-11-12 21:35", ref="2026-11-12 20:30", **kw):
    def fuel(tier):
        return {"ref": ref, "source": "E", "stale": False, "note": None,
                "h24": {"tier": tier, "grade": "กลาง", "n": 105, "right10": 0.61, "wrong10": 0.11,
                        "reasons": ["MOPS ปิดล่าสุด +2.20 USD/bbl → เสี่ยงราคาขึ้น", "2 ราย ประกาศ 'มีปรับขึ้น' วันนี้ → เสี่ยงราคาขึ้น",
                                    "ราคาดีเซล ขยับ -0.24 บาท/ลิตร ใน 3 วัน → โน้มราคาลง"]}}
    d = {"asof": asof, "fuels": {"B7": fuel(b7), "G95": fuel(g95)}}
    d["fuels"].update(kw)
    return d


def _log():
    rows = [["2026-11-10 20:45", "2026-11-10", "2026-11-10 20:30", "B7", 24, "UP_STRONG", 0.34, 0],
            ["2026-11-10 20:45", "2026-11-10", "2026-11-10 20:30", "G95", 24, "DOWN", -0.02, 0],
            ["2026-11-11 20:45", "2026-11-11", "2026-11-11 20:30", "B7", 24, "UP", 0.86, 0],
            ["2026-11-11 20:45", "2026-11-11", "2026-11-11 20:30", "G95", 24, "UNCLEAR", 0.63, 0],
            ["2026-11-11 20:45", "2026-11-11", "2026-11-11 20:30", "B7", 48, "DOWN", 0.90, 0],          # 48 ชม. ไม่นับ
            ["2026-11-11 15:45", "2026-11-11", "2026-11-11 15:30", "B7", 24, "DOWN", 0.90, 0],          # รอบ 15:30 ไม่นับ
            ["2026-11-12 20:45", "2026-11-12", "2026-11-12 20:30", "B7", 24, "UP", "", 0]]              # ยังไม่มีผล
    return pd.DataFrame(rows, columns=["asof", "date", "ref", "fuel", "hours", "tier", "outcome_delta", "backfilled"])


class MessageTest(unittest.TestCase):
    def test_call_and_unclear(self):
        text, ref_id = L.build_message(_inp(), _log())
        self.assertEqual(ref_id, "2026-11-12 20:30")
        self.assertIn("ข้อมูล 20:30 น. 12/11 · ใช้ได้ถึง 20:30 น. 13/11", text)
        self.assertIn("ดีเซล: 📈 ขึ้น (คาดขยับ 10–40 สต./ลิตร)", text)
        self.assertIn("• MOPS ปิดล่าสุด +2.20 USD/bbl", text)
        self.assertNotIn("→", text.split("ผลของคำทาย")[0])                        # ตัดคำลงท้ายของเหตุผล และไม่ใส่เหตุผลทิศตรงข้าม
        self.assertNotIn("ราคาดีเซล ขยับ -0.24", text.split("แก๊สโซฮอล์ 95")[0])
        self.assertIn("ย้อนหลังเมื่อระบบบอกแบบนี้ (105 ครั้ง): ถูก 61% ผิดทิศ 11%", text)
        self.assertIn("แก๊สโซฮอล์ 95: ❓ ไม่มีสัญญาณชัด", text)
        self.assertIn("ราคาอาจขยับได้ทั้งสองทาง", text)
        self.assertIn("ผลของคำทายคืน 11/11", text)
        self.assertIn("ดีเซล: ทายขึ้น → จริง +0.86 ✅", text)
        self.assertIn("แก๊สโซฮอล์ 95: ไม่ได้ทาย → จริง +0.63", text)
        self.assertIn("สถิติผลจริงตั้งแต่ 10/11 (2/40 วัน)", text)
        self.assertIn("ฟันธง 3 ครั้ง · ถูก 2 · ผิดทิศ 0 · ราคานิ่ง 1", text)       # 48 ชม. และรอบ 15:30 ไม่ถูกนับ
        self.assertTrue(text.endswith(L.FOOTER))
        self.assertNotIn("48", text)

    def test_strong_wrong_and_both_unclear(self):
        text, _ = L.build_message(_inp(b7="DOWN_STRONG", g95="UNCLEAR"), _log())
        self.assertIn("ดีเซล: 📉📉 ลงแรง (คาดขยับตั้งแต่ 40 สต./ลิตร)", text)
        self.assertIn("• ราคาดีเซล ขยับ -0.24 บาท/ลิตร ใน 3 วัน", text)
        log = _log()
        log.loc[2, ["tier", "outcome_delta"]] = ["DOWN", 0.86]
        self.assertIn("ทายลง → จริง +0.86 ❌", L.build_message(_inp(), log)[0])
        both, _ = L.build_message(_inp(b7="UNCLEAR", g95="UNCLEAR"), _log())
        self.assertEqual(both.count("❓ ไม่มีสัญญาณชัด"), 2)
        self.assertIn("สัญญาณคืนนี้", both)
        self.assertIn("• ชี้ขึ้น: MOPS ปิดล่าสุด +2.20 USD/bbl", both)
        self.assertIn("• ชี้ลง:", both)

    def test_no_forecast_cases(self):
        stale, _ = L.build_message(_inp(ref="2026-11-11 20:30"), _log())                  # ยังเป็นข้อมูลของเมื่อคืน
        self.assertIn("คืนนี้ (12/11) ไม่มีคำทาย", stale)
        self.assertIn("ยังไม่มีข้อมูลรอบ 20:30 ของวันนี้", stale)
        self.assertNotIn("📈", stale)
        pm, _ = L.build_message(_inp(ref="2026-11-12 15:30"), _log())
        self.assertNotIn("📈", pm)
        none, _ = L.build_message(_inp(B7={"ref": "2026-11-12 20:30", "stale": False, "note": "ฟีดสัญญาณจากไลน์ไม่อัปเดต", "h24": None}), _log())
        self.assertIn("ดีเซล: ไม่มีคำทาย — ฟีดสัญญาณจากไลน์ไม่อัปเดต", none)
        empty, _ = L.build_message(_inp(), pd.DataFrame())
        self.assertIn("ยังไม่มีผลจริง", empty)

    def test_on_demand_uses_latest_unexpired_forecast(self):
        day = _inp(asof="2026-11-13 09:10", ref="2026-11-12 20:30")                       # ถามตอนเช้า : ใช้คำทายของเมื่อคืน
        self.assertIn("ไม่มีคำทาย", L.build_message(day, _log())[0])                       # ข้อความประจำคืนยังเข้มเหมือนเดิม
        text, _ = L.build_message(day, _log(), on_demand=True)
        self.assertIn("ข้อมูล 20:30 น. 12/11 · ใช้ได้ถึง 20:30 น. 13/11", text)
        self.assertIn("ดีเซล: 📈 ขึ้น", text)
        pm, _ = L.build_message(_inp(asof="2026-11-13 16:00", ref="2026-11-13 15:30"), _log(), on_demand=True)
        self.assertIn("ข้อมูล 15:30 น. 13/11", pm)
        self.assertIn("รอบบ่าย", pm)
        old, _ = L.build_message(_inp(asof="2026-11-14 21:00", ref="2026-11-12 20:30"), _log(), on_demand=True)
        self.assertIn("ตอนนี้ ไม่มีคำทาย", old)
        self.assertIn("หมดอายุแล้ว", old)

    def test_make_input_from_engine_shape(self):
        d = {"GASOHOL95": {"fuel": "G95", "ref": pd.Timestamp("2026-11-12 20:30"), "source": "E", "stale": False, "note": None,
                           "horizons": [{"hours": 48, "tier": "UP", "reasons": []},
                                        {"hours": 24, "tier": "DOWN", "grade": "สูง", "reasons": ["x → โน้มราคาลง"], "evidence": {"n": 129, "right10": 0.61, "wrong10": 0.06}}]},
             "DIESEL": {"fuel": "B7", "ref": pd.Timestamp("2026-11-12 20:30"), "note": "ยังไม่มีโมเดล", "horizons": []}}
        inp = L.make_input(d, pd.Timestamp("2026-11-12 21:35"))
        self.assertEqual(inp["fuels"]["G95"]["h24"]["tier"], "DOWN")
        self.assertIsNone(inp["fuels"]["B7"]["h24"])
        text, _ = L.build_message(inp, pd.DataFrame())
        self.assertIn("แก๊สโซฮอล์ 95: 📉 ลง", text)
        self.assertIn("ดีเซล: ไม่มีคำทาย — ยังไม่มีโมเดล", text)


def _retail(target="2026-11-13", **kw):
    import procurement_summary as PS
    def row(fuel, **o):
        r = {"card_id": f"{target} 21:30 {fuel}", "asof": "2026-11-12 21:40", "round": "21:30", "fuel": fuel, "target_day": target, "retail_base": 40.79,
             "p_down": 0.12, "p_flat": 0.70, "p_up": 0.18, "call": "นิ่ง", "size_text": float("nan"), "outcome_dretail": float("nan"),
             "outcome_cls": float("nan"), "filled_at": float("nan"), "sig_p_down": 0.05, "sig_p_flat": 0.60, "sig_p_up": 0.35}
        r.update(o)
        return r
    old = {"card_id": "2026-11-11 21:30 B7", "asof": "2026-11-10 21:40", "target_day": "2026-11-11", "call": "ลง", "outcome_dretail": -1.0,
           "outcome_cls": "ลง", "filled_at": "2026-11-11 11:05"}
    return PS.prep_retail(pd.DataFrame([row("B7", **old), row("B7", **kw), row("G95", call="ขึ้น", p_down=0.1, p_flat=0.4, p_up=0.5,
                                                                              size_text="ถ้าปรับ มัธยฐาน 0.60 บาท/ลิตร (ช่วงที่พบบ่อย 0.50–1.00)")]))


class RetailTest(unittest.TestCase):
    """ข้อความตอบคำสั่ง forecast มีพยากรณ์ราคาขายปลีก ; ข้อความประจำคืนไม่มี"""

    def test_on_demand_includes_retail(self):
        text, _ = L.build_message(_inp(), _log(), on_demand=True, retail=_retail())
        self.assertTrue(text.startswith("🛢 พยากรณ์ราคาน้ำมัน\n🧭 รถที่อยู่คลัง: โหลดคืน 12/11 หรือรอบ่าย 13/11"))
        self.assertIn(L.SEP + "\n① ราคาส่ง (Jobber) · 24 ชม.ข้างหน้า", text)
        self.assertIn(L.SEP + "\n② ราคาขายปลีก (หน้าปั๊ม) · พรุ่งนี้ (13/11)", text)                 # ถามคืน 12/11 หลังการ์ดออก
        self.assertIn("ดีเซล (วันที่ 13/11): ➖ น่าจะไม่ปรับ\n⬇ ลง      ▰▱▱▱▱▱▱▱▱▱ 12%\n➖ ไม่ปรับ ▰▰▰▰▰▰▰▱▱▱ 70%\n⬆ ขึ้น     ▰▰▱▱▱▱▱▱▱▱ 18%", text)
        self.assertNotIn("คู่ขนาน", text)                                                    # ข้อความไลน์แสดงเฉพาะคำทายหลัก
        self.assertNotIn("โมเดลสัญญาณใช้ไม่ได้", text)                                        # แถวก่อนการสลับ ไม่ใช่กรณีโมเดลสัญญาณล้ม
        old = L.build_message(_inp(), _log(), on_demand=True, retail=_retail(model="OLD"))[0]
        self.assertIn("• คืนนี้โมเดลสัญญาณใช้ไม่ได้ จึงใช้วิธีเดิมเป็นคำทายหลัก", old)
        self.assertIn("• ปลีกล่าสุดใน EPPO 40.79\n", text)                                  # size_text ว่าง (NaN จากชีท) ไม่ถูกพิมพ์
        self.assertIn("แก๊สโซฮอล์ 95 (วันที่ 13/11): 📈 น่าจะปรับขึ้น", text)
        self.assertIn("• ปลีกล่าสุดใน EPPO 40.79 · ถ้าปรับ มัธยฐาน 0.60 บาท/ลิตร", text)
        self.assertIn("ผลจริงสะสม (ราคาปลีก): ดีเซล รู้ผล 1 วัน ฟันธง 1 ถูก 1", text)          # แถวที่ยังไม่มีผล (NaN) ไม่ถูกนับ
        self.assertNotIn("nan", text)
        # ผลงานของราคาส่งอยู่ในส่วนราคาส่ง ก่อนส่วนราคาปลีก — ไม่ปนกัน
        self.assertLess(text.index("ผลงานที่ผ่านมา (ราคาส่ง Jobber)"), text.index("② ราคาขายปลีก"))
        self.assertLess(text.index("ถูก ▰▰▰▰▰▰▰▱▱▱ 2/3"), text.index("② ราคาขายปลีก"))
        self.assertTrue(text.endswith(L.FOOTER))

    def test_signal_model_is_primary_when_logged(self):
        sig = _retail(model="SIG", call="ขึ้น", p_down=0.05, p_flat=0.60, p_up=0.35, old_p_down=0.12, old_p_flat=0.70, old_p_up=0.18, old_call="นิ่ง")
        text, _ = L.build_message(_inp(), _log(), on_demand=True, retail=sig)
        self.assertIn("ดีเซล (วันที่ 13/11): 📈 น่าจะปรับขึ้น\n⬇ ลง      ▰▱▱▱▱▱▱▱▱▱ 5%\n➖ ไม่ปรับ ▰▰▰▰▰▰▱▱▱▱ 60%\n⬆ ขึ้น     ▰▰▰▰▱▱▱▱▱▱ 35%", text)
        self.assertNotIn("โมเดลสัญญาณใช้ไม่ได้", text)

    def test_bars_and_jobber_probabilities(self):
        self.assertEqual((L.bar(0), L.bar(0.004), L.bar(0.62), L.bar(1.7)), ("▱" * 10, "▰" + "▱" * 9, "▰" * 6 + "▱" * 4, "▰" * 10))
        inp = _inp()
        inp["fuels"]["B7"]["h24"].update(p_up=0.62, p_down=0.08)
        text, _ = L.build_message(inp, _log())
        self.assertIn("ดีเซล: 📈 ขึ้น (คาดขยับ 10–40 สต./ลิตร)\n⬆ ขึ้น ▰▰▰▰▰▰▱▱▱▱ 62%\n⬇ ลง  ▰▱▱▱▱▱▱▱▱▱ 8%\nความเชื่อมั่น: กลาง", text)
        self.assertNotIn("①", text)                                                          # ข้อความประจำคืนมีส่วนเดียว ไม่ใส่เลขลำดับ
        self.assertIn("แก๊สโซฮอล์ 95: ❓ ไม่มีสัญญาณชัด\nราคาอาจขยับ", text)                  # ไม่มีค่าความน่าจะเป็น = ไม่มีแถบ
        d = {"DIESEL": {"fuel": "B7", "ref": pd.Timestamp("2026-11-12 20:30"), "horizons": [
            {"hours": 24, "tier": "UNCLEAR", "reasons": [], "p": {"r10": 0.31, "f10": 0.36, "r40": 0.1, "f40": 0.1}}]}}
        h = L.make_input(d, pd.Timestamp("2026-11-12 21:35"))["fuels"]["B7"]["h24"]
        self.assertEqual((h["p_up"], h["p_down"]), (0.31, 0.36))

    def test_nightly_message_has_no_retail(self):
        self.assertNotIn("ราคาขายปลีก", L.build_message(_inp(), _log())[0])
        self.assertNotIn("ราคาขายปลีก", L.build_message(_inp(), _log(), on_demand=True)[0])

    def test_stale_empty_and_outcome(self):
        late, _ = L.build_message(_inp(asof="2026-11-14 10:00", ref="2026-11-13 20:30"), _log(), on_demand=True, retail=_retail())
        self.assertIn("ดีเซล: ยังไม่มีพยากรณ์ของวันถัดไป (ออกหลัง 21:30 น. คืนจันทร์–พฤหัสบดี) ; ล่าสุดเป็นของวันที่ 13/11", late)
        self.assertNotIn("น่าจะ", late)
        self.assertIn("② ราคาขายปลีก (หน้าปั๊ม) · รอพยากรณ์ของวันถัดไป", late)
        empty, _ = L.build_message(_inp(), _log(), on_demand=True, retail=pd.DataFrame())
        self.assertIn("ดีเซล: ยังไม่มีพยากรณ์ของวันถัดไป (ออกหลัง 21:30 น. คืนจันทร์–พฤหัสบดี)\n", empty)
        today = _retail(call="ลง", outcome_dretail=-0.5, outcome_cls="ลง", filled_at="2026-11-13 11:05")
        done, _ = L.build_message(_inp(asof="2026-11-13 12:00", ref="2026-11-12 20:30"), _log(), on_demand=True, retail=today)
        self.assertIn("ดีเซล (วันที่ 13/11): 📉 น่าจะปรับลง", done)
        self.assertIn("② ราคาขายปลีก (หน้าปั๊ม) · วันนี้ (13/11)", done)                          # ถามกลางวัน 13/11 : เป็นพยากรณ์ของวันนี้ ไม่ใช่วันถัดไป
        self.assertIn("• ผลจริง: ปรับ -0.50 บาท/ลิตร ✅", done)
        miss = _retail(call="ขึ้น", outcome_dretail=0.0, outcome_cls="นิ่ง", filled_at="2026-11-13 11:05")
        self.assertIn("• ผลจริง: ไม่ปรับ ❌", L.build_message(_inp(asof="2026-11-13 12:00", ref="2026-11-12 20:30"), _log(), on_demand=True, retail=miss)[0])


class SheetReadTest(unittest.TestCase):
    """OutlookLog อ่านจากชีทแบบ UNFORMATTED_VALUE : วันที่/เวลาเป็นเลขลำดับวัน (พบจริงตอน dry-run 16/11/2026 : สถิติว่างทั้งที่มีผล)"""

    def test_serial_dates_are_converted(self):
        from datetime import datetime
        from common import EXCEL_EPOCH
        ser = lambda s: (datetime.strptime(s, "%Y-%m-%d %H:%M") - EXCEL_EPOCH).total_seconds() / 86400
        head = ["asof", "date", "ref", "fuel", "hours", "tier", "outcome_delta", "backfilled"]
        values = [head,
                  [ser("2026-11-11 20:45"), ser("2026-11-11 00:00"), ser("2026-11-11 20:30"), "B7", 24, "UP", 0.86, 0],
                  ["2026-11-11 20:45", "2026-11-11", "2026-11-11 20:30", "G95", 24, "UNCLEAR", 0.63],          # แถวสั้น + เป็นข้อความ
                  [ser("2026-11-12 20:45"), ser("2026-11-12 00:00"), ser("2026-11-12 20:30"), "B7", 24, "UP", "", 0]]
        df = L.log_frame(values)
        self.assertEqual(list(df.ref), ["2026-11-11 20:30", "2026-11-11 20:30", "2026-11-12 20:30"])
        self.assertEqual(df.date[0], "2026-11-11")
        text, _ = L.build_message(_inp(), df)
        self.assertIn("ดีเซล: ทายขึ้น → จริง +0.86 ✅", text)
        self.assertIn("ฟันธง 1 ครั้ง · ถูก 1", text)
        self.assertTrue(L.log_frame([]).empty)


class SendTest(unittest.TestCase):
    def test_only_personal_user_id(self):
        self.assertTrue(L.valid_user_id(UID))
        for bad in ("C" + UID[1:], "R" + UID[1:], UID[:-1], "", None, UID.upper()):
            self.assertFalse(L.valid_user_id(bad), bad)

    def test_window_and_dedupe(self):
        asof = pd.Timestamp("2026-11-12 21:35")
        self.assertTrue(L.in_send_window(pd.Timestamp("2026-11-12 21:36"), asof))
        self.assertFalse(L.in_send_window(pd.Timestamp("2026-11-13 04:30"), pd.Timestamp("2026-11-13 04:29")))   # รอบที่ GitHub เลื่อนมาเช้ามืด
        self.assertFalse(L.in_send_window(pd.Timestamp("2026-11-12 11:00"), pd.Timestamp("2026-11-12 11:00")))
        rows = [L.LOG_COLUMNS, ["2026-11-11 20:30", "x", "sent", "t"], ["2026-11-12 20:30", "x", "failed:500", "t"]]
        self.assertTrue(L.already_sent(rows, "2026-11-11 20:30"))
        self.assertFalse(L.already_sent(rows, "2026-11-12 20:30"))                 # ส่งไม่สำเร็จ → ลองใหม่ได้

    def test_push_payload_and_failure(self):
        with mock.patch("line_outlook.requests.post", return_value=mock.Mock(status_code=200)) as post:
            self.assertEqual(L.push("hi", "tok", UID), "sent")
        self.assertEqual(post.call_args.kwargs["json"], {"to": UID, "messages": [{"type": "text", "text": "hi"}]})
        self.assertTrue(post.call_args.args[0].endswith("/v2/bot/message/push"))
        with mock.patch("line_outlook.requests.post", return_value=mock.Mock(status_code=401)):
            self.assertEqual(L.push("hi", "tok", UID), "failed:401")
        with mock.patch("line_outlook.requests.post", side_effect=L.requests.ConnectionError("x")):
            self.assertEqual(L.push("hi", "tok", UID), "failed:ConnectionError")

    FUND = {"issue": "195", "posted": "2026-11-12 19:42", "effective": "2026-11-13", "late": False,
            "diesel_old": -1.28, "diesel_new": -0.87, "benzene_old": 3.30, "benzene_new": 3.37}

    def _run(self, env, now="2026-11-12 21:36", sent_rows=None, argv=(), fund=FUND):
        import json
        import tempfile
        calls = {"append": [], "post": 0}
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "in.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump({**_inp(), **({"fund": fund} if fund else {})}, fh, ensure_ascii=False)
            def post(*a, **k):
                calls["post"] += 1
                return mock.Mock(status_code=200)
            fake_now = mock.Mock(wraps=L.datetime)
            fake_now.now.return_value = pd.Timestamp(now).to_pydatetime()
            with mock.patch.object(L, "INPUT_FILE", path), mock.patch.object(L, "get_token", return_value="t"), \
                    mock.patch.object(L, "sheets_get", side_effect=lambda tok, sid, rng: (sent_rows or [L.LOG_COLUMNS]) if rng.startswith(L.LOG_TAB) else []), \
                    mock.patch.object(L, "ensure_tab", return_value=False), mock.patch.object(L, "sheets_update"), \
                    mock.patch.object(L, "sheets_append", side_effect=lambda *a: calls["append"].append(a[3])), \
                    mock.patch.object(L, "datetime", fake_now), mock.patch("line_outlook.requests.post", side_effect=post), \
                    mock.patch.dict(os.environ, env, clear=False):
                for k in ("LINE_CHANNEL_ACCESS_TOKEN", "LINE_TEST_USER_ID"):
                    if k not in env:
                        os.environ.pop(k, None)
                self.assertEqual(L.main(list(argv)), 0)
        return calls

    def test_main_gates(self):
        ok = {"LINE_CHANNEL_ACCESS_TOKEN": "tok", "LINE_TEST_USER_ID": UID}
        c = self._run(ok)
        self.assertEqual(c["post"], 1)
        self.assertEqual(c["append"][0][0][0], "fund-195-20261113")                                           # ครั้งเดียวต่อฉบับ
        self.assertEqual(c["append"][0][0][2], "sent")
        self.assertIn("กองทุนฉบับที่ 195 (มีผล 13/11)", c["append"][0][0][3])
        self.assertIn("พิมพ์ forecast", c["append"][0][0][3])
        self.assertEqual(self._run(ok, fund=None)["post"], 0)                                                 # ไม่มีประกาศกองทุน = ไม่ส่ง
        self.assertEqual(self._run({})["post"], 0)                                                            # ไม่มี Secret
        self.assertEqual(self._run({**ok, "LINE_TEST_USER_ID": "C" + UID[1:]})["post"], 0)                    # รหัสกลุ่ม
        self.assertEqual(self._run(ok, argv=("--dry",))["post"], 0)
        self.assertEqual(self._run(ok, now="2026-11-13 04:30")["post"], 0)                                    # หลังเที่ยงคืน (คนละวัน)
        self.assertEqual(self._run(ok, now="2026-11-12 19:50")["post"], 1)                                    # ประกาศก่อน 20:30 ส่งได้ทันที
        self.assertEqual(self._run(ok, sent_rows=[L.LOG_COLUMNS, ["fund-195-20261113", "x", "sent"]])["post"], 0)  # ส่งแล้ว


if __name__ == "__main__":
    unittest.main()


class GeneratedTimeTest(unittest.TestCase):
    """outlook.json ช่อง generated ต้องเป็นเวลาไทย (เครื่องรันของ GitHub เป็นเวลาสากล : เคยแสดงช้าไป 7 ชม.)"""

    def test_generated_is_bangkok_time(self):
        import json
        import tempfile
        from datetime import datetime, timezone
        import main
        with tempfile.TemporaryDirectory() as tmp:
            pub = os.path.join(tmp, "o.json")
            with mock.patch.dict(os.environ, {"TZ": "UTC"}):
                main.write_outlook_files({}, snap_path=os.path.join(tmp, "s.json"), pub_path=pub)
            with open(pub, encoding="utf-8") as fh:
                gen = pd.Timestamp(json.load(fh)["generated"])
        bkk = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None)) + pd.Timedelta(hours=7)
        self.assertLess(abs((gen - bkk).total_seconds()), 180)


def _headsup(*rows):
    import signals as S
    cols = ["msg_date", "msg_time", "provider", "direction", "scope", "stated_time", "has_rate", "rates"]
    return S.add_ts(pd.DataFrame([dict(zip(cols, ("2026-11-12",) + r)) for r in rows], columns=cols))


# ประกาศจริงของ 16/11/2026 (ย่อ) : ผู้ค้าโพสต์หลายข้อความต่อการปรับหนึ่งครั้ง
HEADSUP = (("9:12", "SFL", "DOWN", "ALL", "9:00", "N", None), ("12:18", "SFL", "DOWN", "ALL", "12:20", "N", None),
           ("15:21", "PTT", "UP", "ALL", None, "N", None), ("16:48", "PTT", "UP", "ALL", None, "Y", "DIESEL:+0.75"),
           ("17:30", None, "UP", "ALL", "0:00", "N", None), ("17:39", "IRPC", "UP", "ALL", None, "N", None),
           ("17:44", "IRPC", "UP", "ALL", None, "Y", "DIESEL:+1.00 GASOHOL:+1.00"), ("17:44", "IRPC", "UP", "ALL", "23:30", "N", None),
           ("22:10", "BC", "UP", "ALL", None, "N", None))                                   # โพสต์หลัง asof ต้องไม่ถูกใช้


class NoticeTest(unittest.TestCase):
    """ประกาศของผู้ค้าส่ง : ใช้เป็นเส้นตายและขนาดการขึ้นในคำแนะนำ ไม่แสดงรายการซ้ำในข้อความ"""

    def test_grouping_pending_and_lookahead(self):
        ns = {(n["provider"], n["dir"]): n for n in L.notices(_headsup(*HEADSUP), pd.Timestamp("2026-11-12 20:47"))}
        self.assertEqual(set(ns), {("SFL", "DOWN"), ("PTT", "UP"), ("", "UP"), ("IRPC", "UP")})            # BC 22:10 ยังไม่โพสต์ ณ 20:47
        ir = ns[("IRPC", "UP")]
        self.assertEqual((ir["n"], ir["rates"], ir["stated"], ir["at"], ir["pending"]), (3, "DIESEL:+1.00 GASOHOL:+1.00", "23:30", "17:44", True))
        self.assertEqual((ns[("SFL", "DOWN")]["n"], ns[("SFL", "DOWN")]["pending"]), (2, False))          # มีผลไปแล้วตอน 12:20
        self.assertTrue(ns[("", "UP")]["pending"])                                                        # 00:00 = เที่ยงคืนนี้ ยังไม่ถึง
        late = {(n["provider"], n["dir"]): n for n in L.notices(_headsup(*HEADSUP), pd.Timestamp("2026-11-12 23:45"))}
        self.assertFalse(late[("IRPC", "UP")]["pending"])                                                 # เลย 23:30 แล้ว
        self.assertEqual(L.notices(pd.DataFrame(), pd.Timestamp("2026-11-12 20:47")), [])
        self.assertEqual(L.parse_rates("DIESEL:+1.00 GASOHOL:-0.50"), {"DIESEL": 1.0, "GASOHOL": -0.5})

    def test_pending_rise(self):
        d = {"DIESEL": {"fuel": "B7", "ref": pd.Timestamp("2026-11-12 20:30"), "horizons": []}}
        inp = L.make_input(d, pd.Timestamp("2026-11-12 20:47"), _headsup(*HEADSUP))
        self.assertEqual(L.pending_rise(inp, "B7"), {"amt": 1.0, "when": "23:30"})                         # เรทสูงสุด และเวลามีผลที่เร็วที่สุด
        self.assertEqual(L.pending_rise(inp, "G95"), {"amt": 1.0, "when": "23:30"})
        self.assertIsNone(L.pending_rise({"notices": []}, "B7"))
        self.assertIsNone(L.pending_rise(_inp(), "B7"))                                                    # ไม่มีช่อง notices


class AdviceTest(unittest.TestCase):
    """ควรซื้อเมื่อไร : ซื้อก่อนขึ้นคืนนี้ หรือรอบ่ายพรุ่งนี้ — ใช้เฉพาะหลักฐานที่ผ่านเกณฑ์ และไม่แนะนำเมื่อสัญญาณขัดกัน"""
    RISE = {"amt": 1.0, "when": "23:30"}

    def _f(self, tier="UNCLEAR", *profile):
        return {"ref": "2026-11-12 20:30", "h24": {"tier": tier, "grade": "กลาง"},
                "profile": [dict(zip(("hours", "at", "call", "ok", "n", "right"), p)) for p in profile]}

    def test_wait_buy_conflict_and_none(self):
        wait = L.advice_line(self._f("DOWN_STRONG", (20, "16:30", "DOWN", True, 163, 0.64)), self.RISE)
        self.assertTrue(wait.startswith("⏳ รอโหลดบ่ายวันที่ 13/11 (หลัง 16:00 น.)"))
        self.assertIn("• แนวโน้ม 24 ชม. ลงแรง (ความเชื่อมั่น กลาง)\n", wait)
        f = self._f("DOWN_STRONG")
        f["h24"]["avg_move"] = 0.496
        self.assertIn("• แนวโน้ม 24 ชม. ลงแรง (ความเชื่อมั่น กลาง) · ย้อนหลังราคาต่างจากคืน 12/11 เฉลี่ย 0.50 บาท/ลิตร", L.advice_line(f, None))
        self.assertIn("• ราคา 16:30 น. วันที่ 13/11 มักต่ำกว่าคืน 12/11 (ย้อนหลังถูก 64% จาก 163 ครั้ง)", wait)
        self.assertIn("⚠ คืน 12/11 ราคาจะขึ้น +1.00 เวลา 23:30 น. ตามประกาศ", wait)
        self.assertIn("เลี่ยงโหลดช่วงเช้าหลังราคาขึ้น", wait)
        buy = L.advice_line(self._f("UP", (20, "16:30", "UP", True, 154, 0.68)), self.RISE)
        self.assertTrue(buy.startswith("🛒 โหลดก่อน 23:30 น. คืน 12/11 ก่อนราคาขึ้น +1.00"))
        self.assertTrue(L.advice_line(self._f("UP"), None).startswith("🛒 โหลดคืน 12/11\n   • แนวโน้ม 24 ชม. ขึ้น"))
        both = L.advice_line(self._f("UP_STRONG", (20, "16:30", "DOWN", True, 186, 0.69)), self.RISE)
        self.assertTrue(both.startswith("⚖ สัญญาณขัดกัน ยังไม่แนะนำ"))
        self.assertIn("ชี้ให้โหลดคืน 12/11: แนวโน้ม 24 ชม. ขึ้นแรง", both)
        self.assertIn("ชี้ให้รอ: ราคา 16:30 น.", both)
        none = L.advice_line(self._f("UNCLEAR", (20, "16:30", "DOWN", False, 110, 0.56)), self.RISE)      # ทิศที่ไม่ผ่านเกณฑ์ไม่ถูกใช้
        self.assertTrue(none.startswith("🛒 โหลดก่อน 23:30 น. คืน 12/11 ก่อนราคาขึ้น +1.00\n   • ไม่มีสัญญาณว่าราคาจะลง"))   # ไม่มีสัญญาณ = ค่าเริ่มต้น โหลดคืนนั้น
        self.assertEqual(L.advice_line(self._f(), None), "🛒 โหลดคืน 12/11\n   • ไม่มีสัญญาณว่าราคาจะลง")
        self.assertIn("ย้อนหลังวันแบบนี้ราคาบ่ายวันถัดไปแพงกว่าคืน 12/11 เฉลี่ย 0.08 บาท/ลิตร", L.advice_line(dict(self._f(), fuel_code="B7"), None))
        self.assertIn("เฉลี่ย 0.05 บาท/ลิตร", L.advice_line(dict(self._f(), fuel_code="G95"), None))
        noon = L.advice_line(self._f("UNCLEAR", (16, "12:30", "UP", True, 221, 0.75)), None)              # ขึ้นภายในเที่ยงอย่างเดียว = รีบแบบอ่อน
        self.assertIn("ราคามักขึ้นภายในเที่ยงวันที่ 13/11 (ย้อนหลังถูก 75% จาก 221 ครั้ง)", noon)
        noon_then_down = L.advice_line(self._f("UNCLEAR", (16, "12:30", "UP", True, 221, 0.75), (20, "16:30", "DOWN", True, 186, 0.69)), None)
        self.assertTrue(noon_then_down.startswith("⏳ รอโหลดบ่ายวันที่ 13/11"))                              # ขึ้นเช้าแล้วลงบ่าย = รอ

    def test_message_section(self):
        inp = _inp(b7="UNCLEAR", g95="DOWN_STRONG")
        inp["fuels"]["G95"]["profile"] = self._f("x", (20, "16:30", "DOWN", True, 163, 0.64))["profile"]
        inp["notices"] = L.notices(_headsup(*HEADSUP), pd.Timestamp(inp["asof"]))
        text, _ = L.build_message(inp, _log())
        self.assertTrue(text.startswith("🛢 พยากรณ์ราคาน้ำมัน\n🧭 รถที่อยู่คลัง: โหลดคืน 12/11 หรือรอบ่าย 13/11 [ทดลอง ยังไม่มีผลจริง]"))
        self.assertNotIn("📢", text)                                                      # ไม่แสดงรายการประกาศซ้ำ (ผู้ซื้อเห็นอยู่แล้ว)
        self.assertNotIn("ไออาร์", text)
        self.assertLess(text.index("🧭"), text.index("ราคาส่ง (Jobber) · 24 ชม.ข้างหน้า"))
        self.assertIn("ดีเซล: 🛒 โหลดก่อน 23:30 น. คืน 12/11 ก่อนราคาขึ้น +1.00\n   • ไม่มีสัญญาณว่าราคาจะลง — ย้อนหลังวันแบบนี้ราคาบ่ายวันถัดไปแพงกว่าคืน 12/11 เฉลี่ย 0.08 บาท/ลิตร", text)
        self.assertIn("แก๊สโซฮอล์ 95: ⏳ รอโหลดบ่ายวันที่ 13/11 (หลัง 16:00 น.)", text)
        self.assertNotIn("เวลาซื้อวันที่", text)                                           # ส่วนเดิมถูกแทนด้วยส่วนนี้
        self.assertNotIn("คืนนี้", text.split(L.SEP)[0])                                  # ระบุวันที่เสมอ ไม่ใช้คำว่า คืนนี้/พรุ่งนี้
        self.assertNotIn("พรุ่งนี้", text.split(L.SEP)[0])
        morning = dict(inp, asof="2026-11-13 09:10")                                     # พิมพ์ถามเช้าวันถัดไป : เส้นตายคืนก่อนผ่านไปแล้ว
        morning["notices"] = [{"provider": "SFL", "dir": "UP", "at": "08:30", "n": 1, "rates": "DIESEL:+0.50", "stated": "", "scope": "ALL", "pending": True}]
        late, _ = L.build_message(morning, _log(), on_demand=True)
        self.assertIn("(ออกเมื่อ 20:30 น. 12/11 — ช่วงโหลดก่อนราคาขึ้นของคืนนั้นผ่านไปแล้ว)", late)
        self.assertNotIn("ตามประกาศ", late)                                              # ประกาศของวันใหม่ไม่ถูกนำมาอ้างเป็นของคืนก่อน
        self.assertIn("แก๊สโซฮอล์ 95: ⏳ รอโหลดบ่ายวันที่ 13/11 (หลัง 16:00 น.)", late)
        pm, _ = L.build_message(_inp(asof="2026-11-13 16:00", ref="2026-11-13 15:30"), _log(), on_demand=True)
        self.assertNotIn("🧭", pm)                                                       # รอบบ่ายไม่มีโมเดลรายช่วงเวลา
        none, _ = L.build_message(_inp(ref="2026-11-11 20:30"), _log())
        self.assertNotIn("🧭", none)                                                     # ไม่มีคำทาย → ไม่มีส่วนนี้


class ProfileModelTest(unittest.TestCase):
    """โมเดลรายช่วงเวลาที่ใช้งานจริง + engine.decide_profile"""

    @classmethod
    def setUpClass(cls):
        import json
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "model_params.json"), encoding="utf-8") as fh:
            cls.params = json.load(fh)
        with open(os.path.join(root, "backtest_summary.json"), encoding="utf-8") as fh:
            cls.summary = json.load(fh)

    def test_shipped_profile_models_and_gates(self):
        import model as M
        prof, res = self.params["outlook"]["profile"], self.summary["profile_results"]
        self.assertEqual(set(prof), {f"{f}_T{h}" for f in ("B7", "G95") for h in M.OUTLOOK_PROFILE_HOURS})
        for key, mm in prof.items():
            self.assertEqual(len(mm["r10"]["w"]), len(M.FEATURES_V2))
            for d in ("up", "down"):
                st = res[key][d]
                want = st["n"] >= M.PROFILE_MIN_N and st["right"] >= M.PROFILE_MIN_RIGHT and st["wrong"] <= M.PROFILE_MAX_WRONG and min(st["right_h1"], st["right_h2"]) >= 0.55
                self.assertEqual(st["ok"], bool(want), key + d)
        self.assertFalse(res["B7_T16"]["down"]["ok"])            # "ลงภายในเที่ยง" ไม่ผ่าน (ถูก 56%) ต้องไม่ถูกใช้
        self.assertTrue(res["B7_T16"]["up"]["ok"] and res["B7_T20"]["down"]["ok"] and res["G95_T20"]["down"]["ok"])

    def test_decide_profile_gating(self):
        import engine as E
        import signals as S
        prices = pd.DataFrame([["A", "x", "B7", 39.0, "2026-11-10 05:00", "2026-11-09 17:00"], ["A", "x", "B7", 39.6, "2026-11-11 05:00", "2026-11-10 18:00"],
                               ["B", "y", "B7", 39.2, "2026-11-10 05:00", "2026-11-09 17:10"]], columns=["provider", "warehouse", "fuel", "price", "effective", "posted"])
        ref = pd.Timestamp("2026-11-10 20:30")
        cfg = self.params["outlook"]
        got = E.decide_profile("B7", ref, "E", S.Tables().set_prices(prices), cfg, self.summary)
        self.assertEqual([g["hours"] for g in got], [16, 20])
        self.assertEqual(str(got[0]["at"]), "2026-11-11 12:30:00")
        for g in got:
            self.assertTrue(0 <= g["p_up"] <= 1 and 0 <= g["p_down"] <= 1)
            self.assertEqual(g["ok"], bool(g["call"]) and self.summary["profile_results"][f"B7_T{g['hours']}"][{"UP": "up", "DOWN": "down"}[g["call"]]]["ok"] if g["call"] else False)
        self.assertEqual(E.decide_profile("B7", ref, "P", S.Tables().set_prices(prices), cfg, self.summary), [])     # รอบบ่ายไม่ใช้
        self.assertEqual(E.decide_profile("B7", ref, "E", S.Tables(), cfg, self.summary), [])                        # ไม่มีฟีดราคา
        self.assertEqual(E.decide_profile("B7", ref, "E", S.Tables().set_prices(prices), {}, self.summary), [])      # ไม่มีโมเดล

    def test_snapshot_rows_for_log(self):
        import json
        import tempfile
        import main
        d = {"fuel": "B7", "asof": pd.Timestamp("2026-11-10 21:35"), "ref": pd.Timestamp("2026-11-10 20:30"), "horizons": [],
             "profile": [{"hours": 16, "call": "UP", "p_up": 0.7, "p_down": 0.1, "ok": True}, {"hours": 20, "call": None, "p_up": 0.3, "p_down": 0.3, "ok": False}]}
        with tempfile.TemporaryDirectory() as tmp:
            snap = os.path.join(tmp, "s.json")
            main.write_outlook_files({"DIESEL": d}, "v", snap_path=snap, pub_path=os.path.join(tmp, "o.json"))
            rows = json.load(open(snap, encoding="utf-8"))
        self.assertEqual([(r["hours"], r["tier"], r["p_rise10"]) for r in rows], [(16, "UP", 0.7), (20, "UNCLEAR", 0.3)])


class NightlyRetailTest(unittest.TestCase):
    """ข้อความประจำคืนมีส่วนราคาขายปลีกพร้อมเหตุผล (อ่าน RetailFcLog จากชีทแบบ UNFORMATTED_VALUE)"""

    def test_retail_frame_serial_dates_and_reason(self):
        from datetime import datetime
        from common import EXCEL_EPOCH
        ser = lambda s: (datetime.strptime(s, "%Y-%m-%d %H:%M") - EXCEL_EPOCH).total_seconds() / 86400
        head = ["card_id", "asof", "round", "fuel", "target_day", "retail_base", "p_down", "p_flat", "p_up", "call", "size_text", "model", "sig_reason"]
        values = [head,
                  ["2026-11-13 21:30 B7", ser("2026-11-12 21:40"), "21:30", "B7", ser("2026-11-13 00:00"), 40.79, 0.05, 0.6, 0.35, "ขึ้น", "", "SIG",
                   "ผู้ค้าประกาศปรับขึ้น 3 ราย / ลง 0 ราย · MOPS ปิดล่าสุด +2.20 USD/bbl"],
                  ["2026-11-13 21:30 G95", ser("2026-11-12 21:40"), "21:30", "G95", ser("2026-11-13 00:00"), 39.34, 0.1, 0.8, 0.1, "นิ่ง", "", "OLD", ""]]
        r = L.retail_frame(values)
        self.assertEqual(list(r["target_day"]), ["2026-11-13", "2026-11-13"])
        text, _ = L.build_message(_inp(), _log(), retail=r)                     # ข้อความประจำคืน (ไม่ใช่ on_demand)
        self.assertIn("ราคาขายปลีก", text)
        self.assertIn("• เหตุผล: ผู้ค้าประกาศปรับขึ้น 3 ราย", text)
        self.assertEqual(text.count("• เหตุผล:"), 1)                             # แถว OLD ไม่มีเหตุผลจากโมเดลสัญญาณ
        self.assertTrue(L.retail_frame([]).empty)

    def test_signal_reason(self):
        import retail_forecast as RF
        self.assertEqual(RF.signal_reason({"up_n": 2, "dn_n": 0, "close_last": 1.02, "fund_next": -0.46}),
                         "ผู้ค้าประกาศปรับขึ้น 2 ราย / ลง 0 ราย · MOPS ปิดล่าสุด +1.02 USD/bbl · กองทุนมีผลพรุ่งนี้ -0.46 บาท/ลิตร")
        self.assertEqual(RF.signal_reason({"up_n": 0, "dn_n": 0, "close_last": 0.2, "fund_next": 0.0}), "คืนนี้ไม่มีประกาศปรับราคา และ MOPS ขยับน้อย")
        self.assertIn("MOPS", RF.signal_reason({"close_last": float("nan"), "up_n": None}) + "MOPS")


class FundAlertTest(unittest.TestCase):
    """แจ้งเตือนประกาศกองทุน (flow เจ้าของระบบ 20/11/2026) + การนับประกาศที่ออกหลัง 20:30"""

    def fund_table(self, posted="2026-11-19 19:42", eff="2026-11-20", issue=195):
        import signals as S
        d, t = posted.split(" ")
        df = pd.DataFrame([{"msg_date": d, "msg_time": t, "issue_no": issue, "effective_date": eff,
                            "diesel_old": -1.28, "diesel_new": -0.87, "benzene_old": 3.30, "benzene_new": 3.37}])
        return S.Tables(fund=df).fund

    def outlook(self, ref="2026-11-19 20:30"):
        return {"DIESEL": {"ref": pd.Timestamp(ref), "horizons": [{"hours": 24}], "fuel": "B7"}}

    def test_notice_used_by_tonight_forecast(self):
        f = L.fund_notice(self.fund_table(), self.outlook(), "2026-11-19 20:47")
        self.assertEqual((f["issue"], f["effective"], f["late"]), ("195", "2026-11-20", False))
        self.assertAlmostEqual(f["diesel_new"] - f["diesel_old"], 0.41)
        late = L.fund_notice(self.fund_table("2026-11-19 21:03"), self.outlook(), "2026-11-19 21:10")
        self.assertTrue(late["late"])

    def test_no_alert_cases(self):
        self.assertIsNone(L.fund_notice(self.fund_table("2026-11-19 21:03"), self.outlook(), "2026-11-19 20:47"))   # ยังไม่ออก ณ เวลารัน
        self.assertIsNone(L.fund_notice(self.fund_table(eff="2026-11-21"), self.outlook(), "2026-11-19 20:47"))     # ไม่ใช่ของพรุ่งนี้
        self.assertIsNone(L.fund_notice(self.fund_table(), self.outlook("2026-11-19 15:30"), "2026-11-19 16:00"))  # รอบบ่าย
        self.assertIsNone(L.fund_notice(pd.DataFrame(), self.outlook(), "2026-11-19 20:47"))
        self.assertEqual(L.fund_alert({"fuels": {}}), (None, None))

    def test_alert_text(self):
        inp = {"fuels": {"B7": {"h24": {"tier": "DOWN"}}, "G95": {"h24": {"tier": "UNCLEAR"}}},
               "fund": L.fund_notice(self.fund_table(), self.outlook(), "2026-11-19 20:47")}
        text, ref_id = L.fund_alert(inp)
        self.assertEqual(ref_id, "fund-195-20261120")
        self.assertEqual(text.split("\n"), ["📢 กองทุนฉบับที่ 195 (มีผล 20/11)",
                                            "กองทุน: ดีเซล +0.41 (ดันราคาขึ้น) · เบนซิน +0.07 (ดันราคาขึ้น)",
                                            "ผลพยากรณ์อัปเดตแล้ว: ดีเซล 📉 ลง / แก๊สโซฮอล์ 95 ❓ ไม่ชัด",
                                            "พิมพ์ forecast เพื่อดูรายละเอียด"])

    def test_late_notice_counted_only_after_ref(self):
        import signals as S
        t = S.Tables(fund=pd.DataFrame([{"msg_date": "2026-11-19", "msg_time": "21:03", "issue_no": 195, "effective_date": "2026-11-20",
                                          "diesel_old": -1.28, "diesel_new": -0.87, "benzene_old": 3.30, "benzene_new": 3.37}]))
        ref = pd.Timestamp("2026-11-19 20:30")
        base = S.build_features(ref, t, "B7")
        self.assertEqual((base["fund_next"], base["fund_late"]), (0.0, False))                         # แบบเดิม : ไม่เห็น
        late = S.build_features(ref, t, "B7", fund_asof=pd.Timestamp("2026-11-19 21:10"))
        self.assertAlmostEqual(late["fund_next"], 0.41)
        self.assertTrue(late["fund_late"])
        early = S.build_features(ref, t, "B7", fund_asof=pd.Timestamp("2026-11-19 20:50"))           # ยังไม่ออก ณ เวลารัน
        self.assertEqual(early["fund_next"], 0.0)


class TestAlertModeTest(unittest.TestCase):
    def test_latest_fund_and_text(self):
        head = ["msg_date", "msg_time", "issue_no", "effective_date", "diesel_old", "diesel_new", "diesel_sign", "benzene_old", "benzene_new", "benzene_sign", "message_id"]
        values = [head, ["2026-11-18", "19:45", 194, "2026-11-19", -2.06, -1.28, "SUBSIDY", 3.45, 3.3, "LEVY", "x"],
                  [46345, 0.795833, 195, 46346, -1.28, -0.87, "SUBSIDY", 3.3, 3.37, "LEVY", "y"]]   # UNFORMATTED : วันที่เป็นเลขลำดับวัน
        f = L.latest_fund(values)
        self.assertEqual((f["issue"], f["effective"]), ("195", "2026-11-20"))
        text = L.test_alert(f, pd.Timestamp("2026-11-20 11:50"))
        lines = text.split("\n")
        self.assertTrue(lines[0].startswith("🧪 ทดสอบการแจ้งเตือน (20/11 11:50)"))
        self.assertIn("📢 กองทุนฉบับที่ 195 (มีผล 20/11)", lines)
        self.assertIn("กองทุน: ดีเซล +0.41 (ดันราคาขึ้น) · เบนซิน +0.07 (ดันราคาขึ้น)", lines)
        self.assertNotIn("ผลพยากรณ์อัปเดตแล้ว", text)
        self.assertIsNone(L.latest_fund([]))


def _texts(node, out=None):
    out = [] if out is None else out
    if isinstance(node, dict):
        if node.get("type") == "text":
            out.append(node)
        for v in node.values():
            _texts(v, out)
    elif isinstance(node, list):
        for v in node:
            _texts(v, out)
    return out


class EarlyFundRoundTest(unittest.TestCase):
    """24/11/2026 : ประกาศกองทุน 19:20 → คำทายรอบคืนนี้ (อ้างอิง 20:30) ออก 19:22 แต่คำสั่ง forecast ขึ้นว่า "หมดอายุแล้ว" """
    def test_on_demand_shows_early_round(self):
        inp = _inp(asof="2026-11-24 19:22", ref="2026-11-24 20:30")
        self.assertEqual(L.usable(inp["fuels"]["B7"], pd.Timestamp("2026-11-24 19:22"), on_demand=True), "")
        text = L.build_message(inp, _log(), on_demand=True)[0]
        self.assertIn("ดีเซล: 📈 ขึ้น", text)
        self.assertIn("(ออกล่วงหน้าเพราะประกาศกองทุนออกแล้ว · ใช้ข้อมูลถึง 19:22 น. · อัปเดตอีกครั้งหลัง 20:30 น.)", text)
        self.assertNotIn("หมดอายุ", text)
        # วันก่อนหน้า / หลัง 20:30 ไม่มีหมายเหตุ ; คำทายของวันพรุ่งนี้ (ไม่ควรเกิด) ไม่ถือว่าใช้ได้
        self.assertEqual(L.early_note(_inp(), pd.Timestamp("2026-11-12 20:30")), "")
        self.assertNotEqual(L.usable(inp["fuels"]["B7"], pd.Timestamp("2026-11-23 19:22"), on_demand=True), "")

    def test_flex_shows_early_round(self):
        import line_flex as F
        flex = F.build_flex(_inp(asof="2026-11-24 19:22", ref="2026-11-24 20:30"), _log(), None)
        car = flex["contents"]["contents"]
        self.assertEqual(car[0]["header"]["contents"][0]["text"], "🧭 ควรโหลดเมื่อไร")
        texts = [n["text"] for n in _texts(flex)]
        self.assertIn("ออกล่วงหน้าเพราะประกาศกองทุนออกแล้ว · ใช้ข้อมูลถึง 19:22 น. · อัปเดตอีกครั้งหลัง 20:30 น.", texts)


class WaitRiskTest(unittest.TestCase):
    """ดีเซล "รอบ่าย" ในคืนที่ผู้ค้าประกาศขึ้น (exp_headsup_rate.py ; 23/11/2026 ทายลง → จริง +0.39)"""
    def _f(self, fuel="B7"):
        return {"ref": "2026-11-25 20:30", "fuel_code": fuel, "h24": {"tier": "UNCLEAR", "grade": "ต่ำ"},
                "profile": [{"hours": 20, "call": "DOWN", "ok": True, "right": 0.69, "n": 186}]}

    def test_block_when_big_rise(self):
        text = L.advice_line(self._f(), {"amt": 2.00, "when": "00:00"})
        self.assertTrue(text.startswith("⚖ ไม่ฟันธง — ผู้ค้าประกาศขึ้นแรงคืน 25/11"))
        self.assertIn("คืนที่ประกาศขึ้น ≥1.00 ย้อนหลังรอบ่ายแล้วถูกกว่า 58% แพงกว่า 22% (เฉลี่ยประหยัด 0.22 บาท/ลิตร จาก 45 ครั้ง)", text)
        self.assertIn("ถ้ารับความเสี่ยงไม่ได้ ให้โหลดก่อน 00:00 น. คืน 25/11", text)
        self.assertNotIn("⏳", text)

    def test_warn_when_moderate_rise(self):
        text = L.advice_line(self._f(), {"amt": 0.43, "when": "23:30"})
        self.assertTrue(text.startswith("⏳ รอโหลดบ่ายวันที่ 26/11"))
        self.assertIn("⚠ คืนที่ประกาศขึ้น ≥0.30 ย้อนหลังรอบ่ายแล้วถูกกว่า 63% แพงกว่า 20% (เฉลี่ยประหยัด 0.24 บาท/ลิตร)", text)

    def test_unchanged_cases(self):
        self.assertNotIn("ย้อนหลังรอบ่าย", L.advice_line(self._f(), {"amt": 0.20, "when": "23:30"}))   # เรทเล็ก
        self.assertNotIn("ย้อนหลังรอบ่าย", L.advice_line(self._f(), {"amt": None, "when": "23:30"}))  # ไม่รู้เรท
        self.assertNotIn("ย้อนหลังรอบ่าย", L.advice_line(self._f(), None))
        self.assertTrue(L.advice_line(self._f("G95"), {"amt": 2.00, "when": "00:00"}).startswith("⏳"))   # G95 ไม่ใช้กฎนี้


class TonightChangeTest(unittest.TestCase):
    """คืน 25/11/2026 : ผู้ค้าประกาศขึ้นแต่การ์ดแสดงแค่คำทาย 24 ชม. → แยก "คืนนี้ตามประกาศ (แน่นอน)" พร้อมตัวเลข"""
    def _inp(self):
        inp = _inp(asof="2026-11-25 20:18", ref="2026-11-25 20:30")
        rows = [["2026-11-25", "16:31", "IRPC", "UP", "ALL", "23:30", "DIESEL:+1.00 GASOHOL:+1.00"],
                ["2026-11-25", "16:37", "PTT", "UP", "ALL", "23:30", "DIESEL:+0.43 GASOHOL:+0.54"],
                ["2026-11-25", "17:07", "SFL", "UP", "ALL", "23:59", "DIESEL:+2.00"],
                ["2026-11-25", "17:10", "BC", "UP", "ALL", "00:00", ""],
                ["2026-11-25", "10:05", "IRPC", "DOWN", "ALL", "10:05", ""],                         # ลดไปแล้วตอนเช้า ไม่นับ
                ["2026-11-25", "16:32", "", "DOWN", "ALL", "", ""]]                                  # ไม่มีเวลาและเรท ไม่นับ
        h = pd.DataFrame(rows, columns=["msg_date", "msg_time", "provider", "direction", "scope", "stated_time", "rates"])
        h["ts"] = pd.to_datetime(h.msg_date + " " + h.msg_time)
        inp["notices"] = L.notices(h, pd.Timestamp("2026-11-25 20:18"))
        inp["prices_now"] = {"B7": {"ไออาร์|ระยอง": 38.20, "ปตท|สระบุรี-ใหญ่/นอก": 38.30, "สตาร์ฟูเอลส์ (ดาว)|ขอนแก่น": 38.90,
                                    "บางจาก|บางปะอิน": 38.60, "บางจาก (BSRC)|สระบุรี": 38.70}}
        return inp

    def test_numbers(self):
        c = L.tonight_change(self._inp(), "B7")
        self.assertEqual((c["dir"], c["lo"], c["hi"], c["first"], c["last"], c["n"], c["n_norate"]), ("UP", 0.43, 2.0, "23:30", "00:00", 4, 1))
        # ต่ำสุดหลังปรับ : ไออาร์ 39.20 , ปตท 38.73 , SFL 40.90 , บางจาก (ไม่แจ้งเรท ใช้ราคาเดิม) 38.60 , BSRC ไม่ได้ประกาศ 38.70 → 38.60
        self.assertEqual((c["now"], c["after"]), (38.2, 38.6))
        self.assertIn("ไม่รวมเรทของ BC ที่ยังไม่แจ้ง", c["after_note"])
        self.assertEqual(c["after_by"], "ต่ำสุดหลังปรับมาจาก บางจาก บางปะอิน ซึ่งยังไม่แจ้งเรท")
        self.assertEqual(L.provider_code("บางจาก (BSRC)"), "BSRC")
        self.assertEqual(L.provider_code("สตาร์ฟูเอลส์ (ดาว)"), "SFL")

    def test_text_and_flex(self):
        text = L.build_message(self._inp(), _log(), on_demand=True)[0]
        self.assertIn("คืนนี้ตามประกาศของผู้ค้า (ยังไม่มีผล)\nดีเซล ⬆ ขึ้น +0.43 ถึง +2.00 · 23:30–00:00 น. (4 เจ้า)", text)
        self.assertIn("≈ ราคาต่ำสุดตลาดหลังปรับ 38.20 → 38.60 (+0.40) [ประมาณจากเรทที่ประกาศ · ไม่รวมเรทของ BC ที่ยังไม่แจ้ง]", text)
        self.assertIn("แก๊สโซฮอล์ 95 ⬆ ขึ้น +0.54 ถึง +1.00", text)
        self.assertIn("พยากรณ์ถึง 20:30 น. 26/11 (หลังการปรับข้างบน", text)
        import line_flex as F
        texts = [n["text"] for n in _texts(F.build_flex(self._inp(), _log(), None))]
        self.assertIn("คืนนี้ตามประกาศของผู้ค้า (ยังไม่มีผล)", texts)
        self.assertIn("ดีเซล ⬆ ขึ้น +0.43 ถึง +2.00 · 23:30–00:00 น. (4 เจ้า)", texts)

    def test_no_notice(self):
        self.assertIsNone(L.tonight_change(_inp(), "B7"))
        self.assertNotIn("คืนนี้ตามประกาศ", L.build_message(_inp(), _log(), on_demand=True)[0])
